#!/usr/bin/env python3
"""Utilities for building gate-coordinate files from BUFR via FTP.

This module provides `create_gate_coords_file` which locates a random
BUFR file for a specified radar/strategy/vol_nr in a recent time window,
downloads it, converts it to a small CFRadial NetCDF and writes the gate
coordinates (x/y/z) as a compressed `.npz` file in `output_dir`.

The function reads FTP credentials from arguments or environment variables
(`FTP_HOST`, `FTP_USER`, `FTP_PASS`) if not provided.
"""
from __future__ import annotations

import logging
import os
import random
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import numpy as np

from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart
from radarlib.io.ftp.ftp_client import RadarFTPClient
from radarlib.radar_grid import get_gate_coordinates
from radarlib.radar_grid.utils import infer_blind_range_m

logger = logging.getLogger(__name__)


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
):
    """Fetch one random BUFR file and save gate coordinates as compressed .npz.

    Steps:
    - connect to FTP (credentials from args or env)
    - search last `lookback_hours` hours for BUFR files matching the pattern
      ``_*_{strategy_name}_{vol_nr}_{field}_*.BUFR`` for the given `radar_name`
    - if `field_names` is provided (e.g. ['VRAD', 'WRAD']), only BUFR files
      for those specific fields are considered — this ensures the gate
      coordinates match the scan geometry of the fields that will actually
      be interpolated later
    - pick one random BUFR file from those found
    - download it to a temporary directory
    - convert it to a pyart Radar object and write a small NetCDF file
      into `output_dir` (so the generated netcdf is available)
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
        How many hours back to search for BUFR files (default: 24).

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

    dt_end = datetime.now(timezone.utc)
    dt_start = dt_end - timedelta(hours=lookback_hours)

    # build filename regex — restrict to specific fields if provided
    if field_names:
        if "KDP" in field_names:
            logger.warning("KDP is not well suited for building gate coordinates files, excluding it from field filter")
            field_names = [f for f in field_names if f.upper() != "KDP"]
        if not field_names:
        fields_alt = "|".join(re.escape(f) for f in field_names)
        pattern = re.compile(
            rf"^.*_{re.escape(strategy_name)}_{re.escape(vol_nr)}_({fields_alt})_.*\.BUFR$",
            re.IGNORECASE,
        )
        logger.info(
            "Searching for BUFR files matching fields: %s (strategy=%s, vol=%s)",
            field_names,
            strategy_name,
            vol_nr,
        )
    else:
        pattern = re.compile(
            rf"^.*_{re.escape(strategy_name)}_{re.escape(vol_nr)}_.*\.BUFR$",
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

        for attempt, (_, fname, remote_path) in enumerate(candidates[:max_download_attempts], start=1):
            try:
                with tempfile.TemporaryDirectory(prefix=f"bufr_{radar_name}_{vol_nr}_") as tmpdir:
                    tmpdir = Path(tmpdir)
                    local_bufr = tmpdir / Path(remote_path).name
                    logger.info(
                        "Attempt %d/%d: downloading %s to %s",
                        attempt,
                        min(max_download_attempts, len(candidates)),
                        remote_path,
                        local_bufr,
                    )
                    client.download_file(remote_path.as_posix(), local_bufr)

                    # convert to pyart Radar (in-memory)
                    radar = bufr_paths_to_pyart([str(local_bufr)], save_path=None)

                    # ensure output dir exists
                    out = Path(output_dir)
                    out.mkdir(parents=True, exist_ok=True)

                    # extract gate coordinates and save
                    gate_x, gate_y, gate_z = get_gate_coordinates(radar)
                    out_fname = f"{radar_name}_{strategy_name}_{vol_nr}_gate_coordinates.npz"
                    out_path = out / out_fname

                    # blind range
                    blind_range_m = infer_blind_range_m(radar)

                    # Extraer elevación mínima para below-beam mask
                    lowest_elev_deg = float(np.min(radar.fixed_angle["data"]))

                    np.savez_compressed(
                        out_path,
                        gate_x=gate_x,
                        gate_y=gate_y,
                        gate_z=gate_z,
                        blind_range_m=blind_range_m,
                        lowest_elev_deg=lowest_elev_deg,
                    )

                    logger.info("Wrote gate coordinates to %s", out_path)
                    return out_path
            except Exception as e:
                last_error = e
                logger.warning(
                    "Attempt %d/%d failed for %s: %s. %s",
                    attempt,
                    min(max_download_attempts, len(candidates)),
                    fname,
                    e,
                    (
                        "Trying next candidate..."
                        if attempt < min(max_download_attempts, len(candidates))
                        else "No more candidates."
                    ),
                )

        # All attempts exhausted
        raise RuntimeError(
            f"Failed to create gate coordinates after {min(max_download_attempts, len(candidates))} "
            f"attempts for {radar_name} {strategy_name} {vol_nr}: {last_error}"
        ) from last_error


if __name__ == "__main__":
    radar_name = "RMA4"
    strategy_name = "0315"
    output_dir = "data/gate_coordinates"
    for radar_name in ["RMA5", "RMA6", "RMA7", "RMA8", "RMA9", "RMA10", "RMA11", "RMA12"]:
        for vol_nr in ["01", "02"]:
            create_gate_coords_file(radar_name, strategy_name, vol_nr, output_dir)
