#!/usr/bin/env python3
"""Utilities for building gate-coordinate files from BUFR via FTP.

This module provides `create_gate_coords_file` which locates BUFR files for a
specified radar/strategy/vol_nr in a recent time window, downloads a few of
them, checks that independent files agree on the scan shape (sweeps, rays,
gates, range axis) and writes the gate coordinates (x/y/z) of an agreeing file
as a compressed `.npz` file in `output_dir`.

The function reads FTP credentials from arguments or environment variables
(`FTP_HOST`, `FTP_USER`, `FTP_PASS`) if not provided.
"""
from __future__ import annotations

import gc
import logging
import os
import random
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart
from radarlib.io.ftp.ftp_client import RadarFTPClient
from radarlib.radar_grid import get_gate_coordinates
from radarlib.radar_grid.utils import infer_blind_range_m

logger = logging.getLogger(__name__)


def _scan_signature(radar) -> tuple:
    """Return the scan shape of a Py-ART radar as a hashable tuple.

    The signature is ``(nsweeps, nrays, ngates, first_gate_m, gate_spacing_m)``.
    Elevation angles are left out on purpose: they jitter by +-0.1 deg between
    volumes of the same strategy and do not change the array shapes.
    """
    rng = np.asarray(radar.range["data"], dtype=float)
    first_gate = int(round(float(rng[0]))) if rng.size else 0
    spacing = int(round(float(rng[1] - rng[0]))) if rng.size > 1 else 0
    return (int(radar.nsweeps), int(radar.nrays), int(radar.ngates), first_gate, spacing)


def _extract_gate_payload(radar) -> dict:
    """Gate coordinates and the scalars saved next to them in the ``.npz``."""
    gate_x, gate_y, gate_z = get_gate_coordinates(radar)
    return {
        "gate_x": gate_x,
        "gate_y": gate_y,
        "gate_z": gate_z,
        "blind_range_m": infer_blind_range_m(radar),
        # lowest elevation, for the below-beam mask
        "lowest_elev_deg": float(np.min(radar.fixed_angle["data"])),
    }


def create_gate_coords_file(
    radar_name: str,
    strategy_name: str,
    vol_nr: str,
    output_dir: str,
    field_names: Optional[List[str]] = None,
    ftp_host: Optional[str] = None,
    ftp_user: Optional[str] = None,
    ftp_pass: Optional[str] = None,
    lookback_hours: int = 72,
    max_download_attempts: int = 3,
    reference_dt: Optional[datetime] = None,
    min_agreement: int = 2,
):
    """Fetch BUFR files and save gate coordinates as compressed .npz.

    The gate coordinates (and the geometry built from them) are cached on disk
    and reused indefinitely, so a single atypical scan (for example a volume
    with a missing sweep) would corrupt every later product of that volume
    type. To avoid that, the file is only written once ``min_agreement``
    independent BUFR files share the same scan shape.

    Steps:
    - connect to FTP (credentials from args or env)
    - search last `lookback_hours` hours for BUFR files matching the pattern
      ``_*_{strategy_name}_{vol_nr}_{field}_*.BUFR`` for the given `radar_name`
    - if `field_names` is provided (e.g. ['VRAD', 'WRAD']), only BUFR files
      for those specific fields are considered — this ensures the gate
      coordinates match the scan geometry of the fields that will actually
      be interpolated later
    - download candidates in random order, one at a time, to a temporary directory
    - convert each to a pyart Radar object and compute its scan signature
      (sweeps, rays, gates, range axis)
    - stop as soon as `min_agreement` files share a signature; if the
      candidates disagree and none reaches that count, raise instead of
      guessing which one is the typical scan
    - extract gate coordinates with `radar_grid.get_gate_coordinates`
    - save gate coordinates as `{radar_name}_{strategy_name}_{vol_nr}_gate_coordinates.npz`

    Parameters
    ----------
    radar_name : str
        Radar identifier (e.g. 'RMA1')
    strategy_name : str
        Strategy code (e.g. '0315')
    vol_nr : str
        Volume number (e.g. '01', '02')
    output_dir : str
        Directory to write the .npz file
    field_names : list of str, optional
        Restrict to BUFR files for these fields (e.g. ['VRAD', 'WRAD']).
        If None, any field for the strategy/vol is accepted (legacy behavior).
    ftp_host, ftp_user, ftp_pass : str, optional
        FTP credentials. Falls back to env vars FTP_HOST/FTP_USER/FTP_PASS.
    lookback_hours : int
        How many hours back to search for BUFR files (default: 72).
    max_download_attempts : int
        The candidate pool holds up to ``2 * max_download_attempts`` files and
        each one may be downloaded at most once (default pool: 6).
    min_agreement : int
        Number of BUFR files that must share the same scan signature
        (default: 2). If every readable file agrees but fewer than
        ``min_agreement`` could be read (a radar with very few recent
        volumes), the file is still written, with a warning.

    Returns
    -------
    Path
        Path to the written .npz file.
    """
    ftp_host = ftp_host or os.environ.get("FTP_HOST")
    ftp_user = ftp_user or os.environ.get("FTP_USER")
    ftp_pass = ftp_pass or os.environ.get("FTP_PASS")

    if not ftp_host or not ftp_user or not ftp_pass:
        raise ValueError("FTP credentials required (args or FTP_HOST/FTP_USER/FTP_PASS)")

    dt_end = reference_dt if reference_dt is not None else datetime.now(timezone.utc)
    dt_start = dt_end - timedelta(hours=lookback_hours)

    # build filename regex — restrict to specific fields if provided
    if field_names:
        if "KDP" in field_names:
            logger.warning("KDP is not well suited for building gate coordinates files, excluding it from field filter")
            field_names = [f for f in field_names if f.upper() != "KDP"]
        fields_alt = "|".join(re.escape(f) for f in field_names)
        vol_nr_pat = r"0?" + re.escape(vol_nr.lstrip("0") or "0")
        pattern = re.compile(
            rf"^.*_{re.escape(strategy_name)}_{vol_nr_pat}_({fields_alt})_.*\.BUFR$",
            re.IGNORECASE,
        )
        logger.info(
            "Searching for BUFR files matching fields: %s (strategy=%s, vol=%s)",
            field_names,
            strategy_name,
            vol_nr,
        )
    else:
        vol_nr_pat = r"0?" + re.escape(vol_nr.lstrip("0") or "0")
        pattern = re.compile(
            rf"^.*_{re.escape(strategy_name)}_{vol_nr_pat}_.*\.BUFR$",
            re.IGNORECASE,
        )

    with RadarFTPClient(ftp_host, ftp_user, ftp_pass) as client:
        candidates = []
        for dt, fname, full_remote in client.traverse_radar(
            radar_name, dt_start=dt_start, dt_end=dt_end, vol_types=pattern
        ):
            candidates.append((dt, fname, full_remote))
            if len(candidates) >= max_download_attempts * 2:
                break

        if not candidates:
            raise FileNotFoundError(
                f"No BUFR files found for {radar_name} {strategy_name} {vol_nr} in last {lookback_hours}h"
            )

        # Shuffle so successive retries try different files
        random.shuffle(candidates)
        last_error: Optional[Exception] = None

        # signature -> number of files that produced it / coordinates of the first one
        votes: Dict[tuple, int] = {}
        payloads: Dict[tuple, dict] = {}
        pool = len(candidates)

        for attempt, (_, fname, remote_path) in enumerate(candidates, start=1):
            try:
                with tempfile.TemporaryDirectory(prefix=f"bufr_{radar_name}_{vol_nr}_") as tmpdir:
                    tmpdir = Path(tmpdir)
                    local_bufr = tmpdir / Path(remote_path).name
                    logger.info(
                        "Attempt %d/%d: downloading %s to %s",
                        attempt,
                        pool,
                        remote_path,
                        local_bufr,
                    )
                    client.download_file(remote_path.as_posix(), local_bufr)

                    # convert to pyart Radar (in-memory)
                    radar = bufr_paths_to_pyart([str(local_bufr)], save_path=None)
                    signature = _scan_signature(radar)
                    if signature not in payloads:
                        payloads[signature] = _extract_gate_payload(radar)
                    del radar
                    gc.collect()
            except Exception as e:
                last_error = e
                logger.warning(
                    "Attempt %d/%d failed for %s: %s. %s",
                    attempt,
                    pool,
                    fname,
                    e,
                    "Trying next candidate..." if attempt < pool else "No more candidates.",
                )
                continue

            votes[signature] = votes.get(signature, 0) + 1
            logger.info(
                "%s %s-%s: %s has scan signature %s (nsweeps, nrays, ngates, first_gate_m, spacing_m); "
                "%d/%d agreeing file(s)",
                radar_name,
                strategy_name,
                vol_nr,
                fname,
                signature,
                votes[signature],
                min_agreement,
            )
            if votes[signature] >= min_agreement:
                return _write_gate_coords(output_dir, radar_name, strategy_name, vol_nr, payloads[signature])

        readable = sum(votes.values())
        if len(votes) == 1 and 0 < readable < min_agreement:
            # Everything we could read agrees, there just are too few files (quiet radar).
            (signature,) = votes
            logger.warning(
                "%s %s-%s: only %d readable BUFR file(s) found, fewer than min_agreement=%d; "
                "writing gate coordinates from them anyway (signature %s)",
                radar_name,
                strategy_name,
                vol_nr,
                readable,
                min_agreement,
                signature,
            )
            return _write_gate_coords(output_dir, radar_name, strategy_name, vol_nr, payloads[signature])

        if votes:
            raise RuntimeError(
                f"No agreement on the scan shape for {radar_name} {strategy_name} {vol_nr}: "
                f"{readable} readable file(s), signatures={dict(votes)} "
                f"(need {min_agreement} matching). Not writing gate coordinates; will retry later."
            )

        # Nothing could be read at all
        raise RuntimeError(
            f"Failed to create gate coordinates after {pool} "
            f"attempts for {radar_name} {strategy_name} {vol_nr}: {last_error}"
        ) from last_error


def _write_gate_coords(output_dir: str, radar_name: str, strategy_name: str, vol_nr: str, payload: dict) -> Path:
    """Write the gate coordinates ``.npz`` and return its path."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    out_path = out / f"{radar_name}_{strategy_name}_{vol_nr}_gate_coordinates.npz"
    np.savez_compressed(out_path, **payload)
    logger.info("Wrote gate coordinates to %s", out_path)
    return out_path


if __name__ == "__main__":
    radar_name = "RMA4"
    strategy_name = "0315"
    output_dir = "data/gate_coordinates"
    for radar_name in ["RMA5", "RMA6", "RMA7", "RMA8", "RMA9", "RMA10", "RMA11", "RMA12"]:
        for vol_nr in ["01", "02"]:
            create_gate_coords_file(radar_name, strategy_name, vol_nr, output_dir)
