#!/usr/bin/env python3
"""
calculate_cores_and_tops.py — Standalone script for convective cores & storm tops detection.

Replicates the full detection pipeline used by ProductGenerationDaemon.

INPUT — radar volume identification (identical to how genpro25 tracks volumes):
    --radar-name  Radar station code, e.g. RMA1
    --strategy    Volume strategy code, e.g. 0315
    --vol-nr      Volume number within the strategy, e.g. 01
    --datetime    Observation time in ISO 8601 UTC, e.g. 2026-04-23T07:07:21Z

VOLUME RESOLUTION:
    1. The script builds the canonical NetCDF filename:
           {RADAR}_{strategy}_{vol_nr}_{TIMESTAMP}.nc
    2. It checks ROOT_RADAR_FILES_PATH/{radar_name}/netcdf/ for that file.
    3. If the file is NOT there, it fetches the DBZH and RHOHV BUFR files
       from the FTP server using the credentials from app/config (or env vars).
       - If no files exactly match the requested datetime, the script searches
         a ±SEARCH_WINDOW_HOURS window and picks the closest datetime where
         BOTH DBZH and RHOHV are available.
    4. The two BUFR files are decoded, merged into a single PyART Radar object,
       and written to a temporary NetCDF for the rest of the pipeline to consume.

GEOMETRY:
    The script searches ROOT_GEOMETRY_PATH for a file matching the pattern:
        {RADAR}_{strategy}_{vol_nr}_*.npz
    and uses the first match (same discovery logic as the daemon).

PIPELINE (once radar volume is available):
    NetCDF  →  read_radar_netcdf + estandarizar_campos_RMA
            →  get_field_data (DBZH + optional RhoHV)
            →  apply_geometry  (polar → Cartesian 3D)
            →  column_max      (3D → COLMAX 2D)
            →  generate_cores_and_tops
            →  GeoJSON file
            →  (optional) PNG visualisation

Usage — run inside the genpro25-rmaX container:

    python3 /workspace/scripts/calculate_cores_and_tops.py \\
        --radar-name RMA1 \\
        --strategy 0315 \\
        --vol-nr 01 \\
        --datetime 2026-04-23T07:07:21Z

    # With PNG:
    python3 /workspace/scripts/calculate_cores_and_tops.py \\
        --radar-name RMA1 --strategy 0315 --vol-nr 01 \\
        --datetime 2026-04-23T07:07:21Z \\
        --output-dir /tmp/tops_and_cores --with-png

Exit codes:
    0  Success (GeoJSON written or no detections — both are valid outcomes)
    1  Fatal error (missing inputs, FTP failure, decode failure, etc.)
"""

from __future__ import annotations

import argparse
import gc
import glob
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Repository root resolution — allows running as  python3 scripts/...  from
# the repo root WITHOUT having radarlib installed in the active environment.
# ---------------------------------------------------------------------------
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parent
sys.path.insert(0, str(_repo_root / "src"))
# Also add app/ so that  import config  resolves to app/config.py inside the
# container (same pattern used by check_rma_bufr_ftp.py and plot_bufr_pyart.py).
sys.path.insert(0, str(_repo_root / "app"))

logger = logging.getLogger(__name__)

# How many hours either side of the requested datetime to search for BUFR files
# when no exact match is available.
_DEFAULT_SEARCH_WINDOW_HOURS = 6

# Fields required to run the full pipeline.
_REQUIRED_BUFR_FIELDS = ("DBZH", "RHOHV")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Detect convective cores and storm tops from a radar volume. "
            "The volume is identified by radar code + strategy + volume number + "
            "datetime.  If the pre-processed NetCDF is not found locally the "
            "corresponding BUFR files are fetched from the FTP server."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ------------------------------------------------------------------
    # Volume identification (all required)
    # ------------------------------------------------------------------
    parser.add_argument(
        "--radar-name",
        required=True,
        metavar="CODE",
        help="Radar station identifier (e.g. RMA1).",
    )
    parser.add_argument(
        "--strategy",
        required=True,
        metavar="CODE",
        help="Volume strategy code (e.g. 0315).",
    )
    parser.add_argument(
        "--vol-nr",
        required=True,
        metavar="NR",
        help="Volume number within the strategy (e.g. 01).",
    )
    parser.add_argument(
        "--datetime",
        required=True,
        metavar="ISO8601",
        dest="obs_datetime",
        help=(
            "Observation datetime in ISO 8601 UTC (e.g. 2026-04-23T07:07:21Z). "
            "If no BUFR files exist at this exact time the script searches a "
            f"±{_DEFAULT_SEARCH_WINDOW_HOURS}h window and uses the closest "
            "available pair of DBZH + RHOHV files."
        ),
    )

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------
    parser.add_argument(
        "--output-dir",
        default="./output",
        metavar="DIR",
        help=("Root directory for GeoJSON output. " "Files are written into YYYY/MM/DD/ subdirectories."),
    )

    # ------------------------------------------------------------------
    # Optional PNG visualisation
    # ------------------------------------------------------------------
    parser.add_argument(
        "--with-png",
        action="store_true",
        default=False,
        help=(
            "Generate a PNG visualisation of the COLMAX grid with detected "
            "cores (blue circles) and tops (red triangles) overlaid."
        ),
    )
    parser.add_argument(
        "--png-output",
        default=None,
        metavar="PATH",
        help=(
            "Full path for the PNG file. "
            "When omitted the file is saved as "
            "<output-dir>/<RADAR>_<STRATEGY>_<VOLNR>_<TIMESTAMP>_cores_tops.png."
        ),
    )
    parser.add_argument(
        "--with-html",
        action="store_true",
        default=False,
        help=(
            "Generate an interactive Plotly HTML visualisation of the COLMAX grid. "
            "Hover tooltips show core intensity (dBZ) and top altitude (km). "
            "Can be used alongside --with-png."
        ),
    )
    parser.add_argument(
        "--html-output",
        default=None,
        metavar="PATH",
        help=(
            "Full path for the HTML file. "
            "When omitted the file is saved as "
            "<output-dir>/<RADAR>_<STRATEGY>_<VOLNR>_<TIMESTAMP>_cores_tops.html."
        ),
    )

    # ------------------------------------------------------------------
    # FTP search window override
    # ------------------------------------------------------------------
    parser.add_argument(
        "--search-window-hours",
        type=float,
        default=_DEFAULT_SEARCH_WINDOW_HOURS,
        metavar="HOURS",
        help=(
            "Hours to search either side of the requested datetime when the "
            "exact BUFR files are not available on the FTP server."
        ),
    )

    # ------------------------------------------------------------------
    # Detection algorithm parameter overrides
    # When not provided, values are taken from app/config (genpro25.yml)
    # or from radarlib.config defaults as a last fallback.
    # ------------------------------------------------------------------
    detect = parser.add_argument_group(
        "detection overrides",
        "Override algorithm thresholds. Defaults come from app/config (genpro25.yml).",
    )
    detect.add_argument(
        "--min-z-core",
        type=float,
        default=None,
        metavar="DBZ",
        help="Reflectivity threshold for core detection (dBZ). app/config: MIN_Z_CORE.",
    )
    detect.add_argument(
        "--min-z-updraft",
        type=float,
        default=None,
        metavar="DBZ",
        help="Violent-updraft intensity gate for core detection (dBZ). app/config: MIN_Z_UP.",
    )
    detect.add_argument(
        "--min-range",
        type=float,
        default=None,
        metavar="M",
        help="Minimum range from radar to accept a detection (m). app/config: MIN_RANGE.",
    )
    detect.add_argument(
        "--dedup-radius-cores",
        type=float,
        default=None,
        metavar="M",
        help="Deduplication merge radius for cores (m). app/config: R_NUCLEOS.",
    )
    detect.add_argument(
        "--dedup-radius-tops",
        type=float,
        default=None,
        metavar="M",
        help="Cylindrical search radius around each core for top detection (m). app/config: R_TOPES.",
    )
    detect.add_argument(
        "--rhohv-threshold-cores",
        type=float,
        default=None,
        metavar="RATIO",
        help="Minimum mean RhoHV for the meteorological echo quality gate "
        "in core detection (0–1). radarlib default: 0.85.",
    )
    detect.add_argument(
        "--min-pixels",
        type=int,
        default=None,
        metavar="N",
        help="Minimum blob pixel count accepted through the RhoHV gate path in core detection. radarlib default: 4.",
    )
    detect.add_argument(
        "--min-pixels-updraft",
        type=int,
        default=None,
        metavar="N",
        help="Minimum blob pixel count for the violent-updraft quality gate in core detection. radarlib default: 6.",
    )

    return parser


# ---------------------------------------------------------------------------
# App config / credential loading
# ---------------------------------------------------------------------------


def _load_app_config() -> dict:
    """
    Load service-layer config from app/config.py (genpro25.yml + env vars).

    Falls back to bare OS environment variables if the module cannot be loaded
    (e.g. when genpro25.yml is missing or the YAML parser is not installed).

    Returns
    -------
    dict with keys: FTP_HOST, FTP_USER, FTP_PASS,
                    ROOT_RADAR_FILES_PATH, ROOT_GEOMETRY_PATH
    """
    try:
        import config as app_config  # app/ is already on sys.path

        cfg = {
            "FTP_HOST": getattr(app_config, "FTP_HOST", None),
            "FTP_USER": getattr(app_config, "FTP_USER", None),
            "FTP_PASS": getattr(app_config, "FTP_PASS", None),
            "ROOT_RADAR_FILES_PATH": getattr(app_config, "ROOT_RADAR_FILES_PATH", "data/radares"),
            "ROOT_GEOMETRY_PATH": getattr(app_config, "ROOT_GEOMETRY_PATH", "data/geometries"),
            # Detection algorithm thresholds (app/config layer, may differ from radarlib.config defaults)
            "MIN_Z_CORE": getattr(app_config, "MIN_Z_CORE", None),
            "MIN_Z_UP": getattr(app_config, "MIN_Z_UP", None),
            "MIN_RANGE": getattr(app_config, "MIN_RANGE", None),
            "R_NUCLEOS": getattr(app_config, "R_NUCLEOS", None),
            "R_TOPES": getattr(app_config, "R_TOPES", None),
        }
        logger.info("Loaded service config from app/config.py")
        return cfg
    except Exception as exc:
        logger.debug("Could not load app/config.py: %s — falling back to env vars", exc)

    cfg = {
        "FTP_HOST": os.environ.get("FTP_HOST"),
        "FTP_USER": os.environ.get("FTP_USER"),
        "FTP_PASS": os.environ.get("FTP_PASS"),
        "ROOT_RADAR_FILES_PATH": os.environ.get("ROOT_RADAR_FILES_PATH", "data/radares"),
        "ROOT_GEOMETRY_PATH": os.environ.get("ROOT_GEOMETRY_PATH", "data/geometries"),
        "MIN_Z_CORE": float(os.environ["MIN_Z_CORE"]) if "MIN_Z_CORE" in os.environ else None,
        "MIN_Z_UP": float(os.environ["MIN_Z_UP"]) if "MIN_Z_UP" in os.environ else None,
        "MIN_RANGE": float(os.environ["MIN_RANGE"]) if "MIN_RANGE" in os.environ else None,
        "R_NUCLEOS": float(os.environ["R_NUCLEOS"]) if "R_NUCLEOS" in os.environ else None,
        "R_TOPES": float(os.environ["R_TOPES"]) if "R_TOPES" in os.environ else None,
    }
    logger.info("Loaded service config from environment variables")
    return cfg


# ---------------------------------------------------------------------------
# NetCDF filename / path helpers
# ---------------------------------------------------------------------------


def _build_netcdf_filename(radar_name: str, strategy: str, vol_nr: str, obs_time: datetime) -> str:
    """Return the canonical NetCDF filename for a volume.

    Format matches  get_netcdf_filename_from_bufr_filename:
        {RADAR}_{strategy}_{vol_nr}_{YYYYMMDDTHHMMSSZ}.nc
    """
    timestamp = obs_time.strftime("%Y%m%dT%H%M%SZ")
    return f"{radar_name}_{strategy}_{vol_nr}_{timestamp}.nc"


def _find_netcdf(netcdf_dir: Path, filename: str) -> Optional[Path]:
    """Return the full path to the NetCDF if it exists, else None."""
    candidate = netcdf_dir / filename
    if candidate.exists():
        logger.info("Found existing NetCDF: %s", candidate)
        return candidate
    logger.info("NetCDF not found locally: %s", candidate)
    return None


# ---------------------------------------------------------------------------
# Geometry discovery
# ---------------------------------------------------------------------------


def _find_geometry_file(radar_name: str, strategy: str, vol_nr: str, geometry_dir: Path) -> Optional[Path]:
    """
    Search geometry_dir for a .npz file matching {radar_name}_{strategy}_{vol_nr}_*.

    Uses the same naming convention as build_geometry_filename — the parameters
    that differ between builds (resolution, TOA, etc.) are part of the filename
    suffix so we glob on the fixed prefix.
    """
    pattern = str(geometry_dir / f"{radar_name}_{strategy}_{vol_nr}_*.npz")
    matches = sorted(glob.glob(pattern))
    if matches:
        logger.info("Found geometry file: %s", matches[0])
        return Path(matches[0])
    logger.warning("No geometry file found matching pattern: %s", pattern)
    return None


# ---------------------------------------------------------------------------
# FTP BUFR retrieval
# ---------------------------------------------------------------------------


def _build_bufr_filename(radar_name: str, strategy: str, vol_nr: str, field: str, obs_time: datetime) -> str:
    """Return the canonical BUFR filename for a single field observation.

    Format: {RADAR}_{strategy}_{vol_nr}_{FIELD}_{YYYYMMDDTHHMMSSZ}.BUFR
    """
    timestamp = obs_time.strftime("%Y%m%dT%H%M%SZ")
    return f"{radar_name}_{strategy}_{vol_nr}_{field}_{timestamp}.BUFR"


def _traverse_and_collect(
    ftp_client,
    radar_name: str,
    strategy: str,
    vol_nr: str,
    obs_time: datetime,
    window_hours: float,
) -> List[Tuple[datetime, str, str]]:
    """
    Traverse the FTP server around obs_time and return all BUFR files that
    match the given radar/strategy/vol_nr for DBZH or RHOHV fields.

    Returns
    -------
    list of (datetime, filename, remote_path), one entry per matching file.
    """
    dt_start = obs_time - timedelta(hours=window_hours)
    dt_end = obs_time + timedelta(hours=window_hours)

    # Pre-build the filename prefix that must match for this volume.
    # Pattern: {RADAR}_{strategy}_{vol_nr}_{FIELD}_{TIMESTAMP}.BUFR
    prefix = f"{radar_name}_{strategy}_{vol_nr}_"

    results = []
    try:
        for dt, fname, remote_path in ftp_client.traverse_radar(
            radar_name, dt_start, dt_end, include_start=True, include_end=True
        ):
            if not fname.upper().endswith(".BUFR"):
                continue
            if not fname.startswith(prefix):
                continue
            # Extract field token (4th underscore-separated component)
            parts = fname.split("_")
            if len(parts) < 5:
                continue
            field_token = parts[3].upper()
            if field_token in ("DBZH", "RHOHV"):
                results.append((dt, fname, str(remote_path)))
    except Exception as exc:
        logger.error("FTP traversal error: %s", exc)

    return results


def _find_closest_bufr_pair(
    ftp_client,
    radar_name: str,
    strategy: str,
    vol_nr: str,
    obs_time: datetime,
    window_hours: float,
) -> Optional[Tuple[datetime, str, str, str, str]]:
    """
    Find the closest datetime where BOTH DBZH and RHOHV BUFR files are
    available on the FTP server.

    Strategy:
    1. Collect all DBZH and RHOHV files within the search window.
    2. For each DBZH file, look for an RHOHV file at the exact same datetime.
    3. Among pairs found, return the pair whose datetime is closest to obs_time.
    4. If no exact-dt pairs exist, fall back to nearest pair within 30 seconds.

    Returns
    -------
    None if no matching pair is found, otherwise a tuple:
        (best_dt, dbzh_remote_path, dbzh_filename, rhohv_remote_path, rhohv_filename)
    """
    logger.info(
        "Searching FTP for DBZH+RHOHV pair within ±%.1fh of %s …",
        window_hours,
        obs_time.isoformat(),
    )

    files = _traverse_and_collect(ftp_client, radar_name, strategy, vol_nr, obs_time, window_hours)
    if not files:
        logger.warning("No matching BUFR files found in search window.")
        return None

    # Group by (datetime, field)
    dbzh_by_dt: dict = {}
    rhohv_by_dt: dict = {}
    for dt, fname, remote in files:
        parts = fname.split("_")
        field = parts[3].upper()
        if field == "DBZH":
            dbzh_by_dt[dt] = (fname, remote)
        elif field == "RHOHV":
            rhohv_by_dt[dt] = (fname, remote)

    # Find datetimes where both fields are present
    common_dts = set(dbzh_by_dt.keys()) & set(rhohv_by_dt.keys())
    if not common_dts:
        logger.warning(
            "Found %d DBZH and %d RHOHV files but no datetime with BOTH present — " "trying 30-second proximity match.",
            len(dbzh_by_dt),
            len(rhohv_by_dt),
        )
        # Fallback: pair DBZH and RHOHV files that are within 30 seconds of each other.
        pairs = []
        for dbzh_dt, (dbzh_fname, dbzh_remote) in dbzh_by_dt.items():
            for rhohv_dt, (rhohv_fname, rhohv_remote) in rhohv_by_dt.items():
                delta_s = abs((dbzh_dt - rhohv_dt).total_seconds())
                if delta_s <= 30:
                    mid_dt = dbzh_dt
                    dist = abs((mid_dt - obs_time).total_seconds())
                    pairs.append((dist, mid_dt, dbzh_remote, dbzh_fname, rhohv_remote, rhohv_fname))
        if not pairs:
            logger.error("No DBZH+RHOHV pair found within ±30s across the entire search window.")
            return None
        pairs.sort(key=lambda x: x[0])
        _, best_dt, dbzh_r, dbzh_f, rhohv_r, rhohv_f = pairs[0]
        logger.info(
            "Proximity pair selected at %s (Δ=%.0fs from requested): %s + %s",
            best_dt.isoformat(),
            abs((best_dt - obs_time).total_seconds()),
            dbzh_f,
            rhohv_f,
        )
        return best_dt, dbzh_r, dbzh_f, rhohv_r, rhohv_f

    # Pick the closest datetime to obs_time
    best_dt = min(common_dts, key=lambda dt: abs((dt - obs_time).total_seconds()))
    dbzh_fname, dbzh_remote = dbzh_by_dt[best_dt]
    rhohv_fname, rhohv_remote = rhohv_by_dt[best_dt]

    delta_s = abs((best_dt - obs_time).total_seconds())
    logger.info(
        "Best BUFR pair at %s (Δ=%.0fs from requested): %s + %s",
        best_dt.isoformat(),
        delta_s,
        dbzh_fname,
        rhohv_fname,
    )
    return best_dt, dbzh_remote, dbzh_fname, rhohv_remote, rhohv_fname


def _fetch_and_decode_bufr(
    radar_name: str,
    strategy: str,
    vol_nr: str,
    obs_time: datetime,
    ftp_cfg: dict,
    window_hours: float,
) -> Tuple[Path, datetime]:
    """
    Fetch DBZH + RHOHV BUFR files from FTP, decode them, and write a
    temporary NetCDF file.

    The function first tries the exact requested datetime.  If those files
    are not found it uses _find_closest_bufr_pair to locate the nearest pair
    within the search window.

    Returns
    -------
    (netcdf_tmp_path, actual_obs_time)

    Raises
    ------
    RuntimeError on FTP, decode, or write failures.
    """
    from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart, save_radar_to_cfradial
    from radarlib.io.ftp.ftp_client import RadarFTPClientAsync

    host = ftp_cfg.get("FTP_HOST")
    user = ftp_cfg.get("FTP_USER")
    password = ftp_cfg.get("FTP_PASS")

    if not all([host, user, password]):
        raise RuntimeError(
            "FTP credentials not configured. "
            "Set FTP_HOST, FTP_USER, FTP_PASS in the app config or as environment variables."
        )

    with RadarFTPClientAsync(host, user, password) as ftp_client:
        # ------------------------------------------------------------------
        # Try exact datetime first — build the FTP directory path from obs_time
        # Path structure: /L2/{RADAR}/{YYYY}/{MM}/{DD}/{HH}/{MMSS}/
        # ------------------------------------------------------------------
        mmss = f"{obs_time.minute:02d}{obs_time.second:02d}"
        exact_dir = (
            f"/L2/{radar_name}"
            f"/{obs_time.year}"
            f"/{obs_time.month:02d}"
            f"/{obs_time.day:02d}"
            f"/{obs_time.hour:02d}"
            f"/{mmss}"
        )
        dbzh_fname_exact = _build_bufr_filename(radar_name, strategy, vol_nr, "DBZH", obs_time)
        rhohv_fname_exact = _build_bufr_filename(radar_name, strategy, vol_nr, "RHOHV", obs_time)

        dbzh_remote: Optional[str] = None
        dbzh_fname: Optional[str] = None
        rhohv_remote: Optional[str] = None
        rhohv_fname: Optional[str] = None
        actual_obs_time = obs_time

        logger.info("Looking for exact BUFR files in %s …", exact_dir)
        try:
            dir_files = ftp_client.list_dir(exact_dir)
            if dbzh_fname_exact in dir_files and rhohv_fname_exact in dir_files:
                logger.info("Exact BUFR files found.")
                dbzh_remote = f"{exact_dir}/{dbzh_fname_exact}"
                dbzh_fname = dbzh_fname_exact
                rhohv_remote = f"{exact_dir}/{rhohv_fname_exact}"
                rhohv_fname = rhohv_fname_exact
        except Exception as exc:
            logger.debug("Exact datetime FTP dir not accessible (%s) — will search.", exc)

        # ------------------------------------------------------------------
        # Fall back to closest pair search
        # ------------------------------------------------------------------
        if dbzh_remote is None:
            logger.info(
                "Exact BUFR files not found at %s. Searching ±%.1fh window …",
                exact_dir,
                window_hours,
            )
            result = _find_closest_bufr_pair(ftp_client, radar_name, strategy, vol_nr, obs_time, window_hours)
            if result is None:
                raise RuntimeError(
                    f"No BUFR pair (DBZH + RHOHV) found for "
                    f"{radar_name} {strategy}-{vol_nr} within ±{window_hours}h "
                    f"of {obs_time.isoformat()}."
                )
            actual_obs_time, dbzh_remote, dbzh_fname, rhohv_remote, rhohv_fname = result

        # ------------------------------------------------------------------
        # Download both files to a temporary directory
        # ------------------------------------------------------------------
        with tempfile.TemporaryDirectory(prefix="radarlib_bufr_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            dbzh_local = tmp_path / dbzh_fname
            rhohv_local = tmp_path / rhohv_fname

            logger.info("Downloading %s …", dbzh_fname)
            ftp_client.download_file(dbzh_remote, dbzh_local)

            logger.info("Downloading %s …", rhohv_fname)
            ftp_client.download_file(rhohv_remote, rhohv_local)

            # ------------------------------------------------------------------
            # Decode BUFR files and build PyART Radar object
            # ------------------------------------------------------------------
            logger.info("Decoding BUFR files and building PyART Radar object …")
            radar = bufr_paths_to_pyart([str(dbzh_local), str(rhohv_local)])

            if radar is None:
                raise RuntimeError("bufr_paths_to_pyart returned None — decode failed.")

            # ------------------------------------------------------------------
            # Save to a persistent temp NetCDF (outside the BUFR tmpdir so it
            # survives after that context manager exits).
            # ------------------------------------------------------------------
            netcdf_tmp = Path(tempfile.mktemp(suffix=".nc", prefix="radarlib_nc_"))
            logger.info("Writing temporary NetCDF: %s", netcdf_tmp)
            save_radar_to_cfradial(radar, netcdf_tmp)
            del radar
            gc.collect()

            return netcdf_tmp, actual_obs_time


# ---------------------------------------------------------------------------
# Detection orchestration (with explicit config overrides)
# ---------------------------------------------------------------------------


def _run_detection(
    colmax_2d: np.ndarray,
    dbzh_3d: np.ndarray,
    xx: np.ndarray,
    yy: np.ndarray,
    z_1d: np.ndarray,
    radar_lat: float,
    radar_lon: float,
    observation_time: datetime,
    radar_name: str,
    strategy: str,
    vol_nr: str,
    output_dir: Path,
    rhohv_3d: Optional[np.ndarray],
    rhohv_2d: Optional[np.ndarray],
    # detection param overrides — None means "use radarlib.config default"
    min_z_core: Optional[float] = None,
    min_z_updraft: Optional[float] = None,
    min_range_m: Optional[float] = None,
    dedup_radius_cores: Optional[float] = None,
    dedup_radius_tops: Optional[float] = None,
    rhohv_threshold_cores: Optional[float] = None,
    min_pixels: Optional[int] = None,
    min_pixels_updraft: Optional[int] = None,
) -> Optional[Path]:
    """
    Run core and top detection with explicit parameter control, then write GeoJSON.

    Parameters left as ``None`` fall back to the defaults defined in
    ``radarlib.config`` (the same values used by ``generate_cores_and_tops``).

    Returns the GeoJSON path, or None if no features were detected.
    """
    import time

    from pyart.core.transforms import cartesian_to_geographic_aeqd

    from radarlib.radar_grid import detect_cores_from_colmax, detect_tops_from_cores

    # ------------------------------------------------------------------
    # Build kwargs dicts — only pass overrides that were explicitly set;
    # omitted kwargs use the function's own defaults from radarlib.config.
    # ------------------------------------------------------------------
    core_kwargs: dict = {}
    if min_z_core is not None:
        core_kwargs["min_dbz"] = min_z_core
    if min_z_updraft is not None:
        core_kwargs["min_dbz_updraft"] = min_z_updraft
    if min_range_m is not None:
        core_kwargs["min_range_m"] = min_range_m
    if dedup_radius_cores is not None:
        core_kwargs["dedup_radius_m"] = dedup_radius_cores
    if rhohv_threshold_cores is not None:
        core_kwargs["rhohv_threshold"] = rhohv_threshold_cores
    if min_pixels is not None:
        core_kwargs["min_pixels"] = min_pixels
    if min_pixels_updraft is not None:
        core_kwargs["min_pixels_updraft"] = min_pixels_updraft

    tops_kwargs: dict = {}
    if dedup_radius_tops is not None:
        tops_kwargs["radius_m"] = dedup_radius_tops

    t0 = time.monotonic()

    # ------------------------------------------------------------------
    # Core detection
    # ------------------------------------------------------------------
    cores: list = []
    try:
        cores = detect_cores_from_colmax(
            colmax=colmax_2d,
            x_coords=xx,
            y_coords=yy,
            rhohv=rhohv_2d,
            **core_kwargs,
        )
        logger.info("Core detection complete: %d core(s) found.", len(cores))
        if core_kwargs:
            logger.info("  Core overrides applied: %s", core_kwargs)
    except Exception as exc:
        logger.warning("Core detection failed: %s — no tops will be detected.", exc)

    # ------------------------------------------------------------------
    # Top detection (only when cores were found)
    # ------------------------------------------------------------------
    tops: list = []
    if cores:
        try:
            tops = detect_tops_from_cores(
                cores=cores,
                grid_3d=dbzh_3d,
                x_coords=xx,
                y_coords=yy,
                z_coords=z_1d,
                **tops_kwargs,
            )
            logger.info("Top detection complete: %d top(s) found.", len(tops))
            if tops_kwargs:
                logger.info("  Tops overrides applied: %s", tops_kwargs)
        except Exception as exc:
            logger.warning("Top detection failed: %s", exc)
    else:
        logger.debug("No cores found — skipping top detection.")

    # ------------------------------------------------------------------
    # Nothing detected — skip file write
    # ------------------------------------------------------------------
    if not cores and not tops:
        logger.info("No cores or tops detected — GeoJSON not written.")
        return None

    # ------------------------------------------------------------------
    # Build GeoJSON features (same schema as generate_cores_and_tops)
    # ------------------------------------------------------------------
    obs_time_str = observation_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    features: list = []

    for core in cores:
        lon_arr, lat_arr = cartesian_to_geographic_aeqd(
            np.array([core["x_m"]], dtype=np.float64),
            np.array([core["y_m"]], dtype=np.float64),
            radar_lon,
            radar_lat,
        )
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon_arr[0]), float(lat_arr[0])]},
                "properties": {
                    "type": "core",
                    "intensity_dbz": int(core["mean_dbz"]),
                    "radar_code": radar_name,
                    "observation_time": obs_time_str,
                },
            }
        )

    for top in tops:
        lon_arr, lat_arr = cartesian_to_geographic_aeqd(
            np.array([top["x_m"]], dtype=np.float64),
            np.array([top["y_m"]], dtype=np.float64),
            radar_lon,
            radar_lat,
        )
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon_arr[0]), float(lat_arr[0])]},
                "properties": {
                    "type": "top",
                    "altitude_m": int(top["altitude_m"]),
                    "dbz": float(top["dbz"]),
                    "radar_code": radar_name,
                    "observation_time": obs_time_str,
                    "parent_core_dbz": int(top["core_mean_dbz"]),
                },
            }
        )

    geojson: dict = {"type": "FeatureCollection", "features": features}

    # ------------------------------------------------------------------
    # Output path: {output_dir}/YYYY/MM/DD/{radar}_{strategy}_{vol_nr}_{ts}_TOPS_CORES.geojson
    # ------------------------------------------------------------------
    timestamp_str = observation_time.strftime("%Y%m%dT%H%M%SZ")
    subdir = (
        output_dir / f"{observation_time.year:04d}" / f"{observation_time.month:02d}" / f"{observation_time.day:02d}"
    )
    filename = f"{radar_name}_{strategy}_{vol_nr}_{timestamp_str}_TOPS_CORES.geojson"
    output_path = subdir / filename

    try:
        subdir.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(geojson, fh, separators=(",", ":"))
    except Exception as exc:
        logger.error("Failed to write GeoJSON %s: %s", output_path, exc, exc_info=True)
        return None

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "CORES_TOPS radar=%s time=%s cores=%d tops=%d elapsed=%dms -> %s",
        radar_name,
        obs_time_str,
        len(cores),
        len(tops),
        elapsed_ms,
        output_path.name,
    )
    return output_path


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------


def _extract_coordinates(geometry) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return ``(xx, yy, z_1d)`` arrays from a GridGeometry object."""
    _nz, ny, nx = geometry.grid_shape
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    x_1d = np.linspace(x_min, x_max, nx, dtype=np.float32)
    y_1d = np.linspace(y_min, y_max, ny, dtype=np.float32)
    yy, xx = np.meshgrid(y_1d, x_1d, indexing="ij")  # shape (ny, nx)

    z_1d = geometry.z_levels().astype(np.float32)
    return xx, yy, z_1d


# ---------------------------------------------------------------------------
# RhoHV field discovery
# ---------------------------------------------------------------------------

_RHOHV_FIELD_CANDIDATES = ("RHOHV", "RhoHV", "cross_correlation_ratio")


def _find_rhohv_field(radar) -> Optional[str]:
    for name in _RHOHV_FIELD_CANDIDATES:
        if name in radar.fields:
            return name
    return None


# ---------------------------------------------------------------------------
# PNG visualisation
# ---------------------------------------------------------------------------


def _save_png(
    colmax: np.ndarray,
    xx: np.ndarray,
    yy: np.ndarray,
    geojson_path: Optional[Path],
    radar_lat: float,
    radar_lon: float,
    radar_name: str,
    timestamp_str: str,
    strategy: str,
    vol_nr: str,
    output_path: Path,
) -> None:
    """Save a COLMAX map overlaid with detected cores/tops to a PNG file."""
    import matplotlib.pyplot as plt

    try:
        from pyart.core.transforms import geographic_to_cartesian_aeqd
    except ImportError as exc:
        logger.warning("pyart not available for PNG coordinate transform: %s", exc)
        return

    try:
        import radarlib.visualization  # noqa: F401 — registers custom cmaps

        cmap_refl = "grc_th"
    except Exception:
        cmap_refl = "NWSRef"

    cores: list = []
    tops: list = []
    if geojson_path is not None and geojson_path.exists():
        try:
            with open(geojson_path) as fh:
                fc = json.load(fh)
            for feature in fc.get("features", []):
                lon, lat = feature["geometry"]["coordinates"]
                props = feature["properties"]
                x_arr, y_arr = geographic_to_cartesian_aeqd(
                    np.array([lon], dtype=np.float64),
                    np.array([lat], dtype=np.float64),
                    radar_lon,
                    radar_lat,
                )
                entry = {"x_m": float(x_arr[0]), "y_m": float(y_arr[0]), **props}
                if props.get("type") == "core":
                    cores.append(entry)
                elif props.get("type") == "top":
                    tops.append(entry)
        except Exception as exc:
            logger.warning("Could not parse GeoJSON for PNG overlay: %s", exc)

    fig, ax = plt.subplots(figsize=(9, 8))
    x_km = xx / 1000.0
    y_km = yy / 1000.0

    pcm = ax.pcolormesh(
        x_km,
        y_km,
        np.ma.masked_invalid(colmax),
        cmap=cmap_refl,
        vmin=-10.0,
        vmax=65.0,
        shading="auto",
    )
    cbar = fig.colorbar(pcm, ax=ax, pad=0.02)
    cbar.set_label("COLMAX reflectivity (dBZ)", fontsize=11)

    for core in cores:
        cx_km = core["x_m"] / 1000.0
        cy_km = core["y_m"] / 1000.0
        ax.plot(
            cx_km,
            cy_km,
            marker="o",
            markersize=5,
            markerfacecolor="#3b82f6",
            markeredgecolor="black",
            markeredgewidth=0.7,
            zorder=5,
        )
        ax.annotate(
            f"{core.get('intensity_dbz', '?')} dBZ",
            xy=(cx_km, cy_km),
            xytext=(5, 4),
            textcoords="offset points",
            color="black",
            fontsize=6,
            fontweight="bold",
            zorder=6,
        )

    for top in tops:
        tx_km = top["x_m"] / 1000.0
        ty_km = top["y_m"] / 1000.0
        ax.plot(
            tx_km,
            ty_km,
            marker="^",
            markersize=5,
            markerfacecolor="#ef4444",
            markeredgecolor="black",
            markeredgewidth=0.7,
            zorder=5,
        )
        alt_m = top.get("altitude_m", "?")
        label = f"{alt_m / 1000:.1f} km" if isinstance(alt_m, (int, float)) else str(alt_m)
        ax.annotate(
            label,
            xy=(tx_km, -ty_km),
            xytext=(5, 4),
            textcoords="offset points",
            color="black",
            fontsize=6,
            fontweight="bold",
            zorder=6,
        )

    ax.set_xlabel("Range East (km)", fontsize=11)
    ax.set_ylabel("Range North (km)", fontsize=11)
    ax.set_title(
        f"{radar_name} — COLMAX · cores ({len(cores)}) · tops ({len(tops)})\n{timestamp_str}",
        fontsize=12,
    )
    ax.set_aspect("equal")
    ax.grid(color="gray", linestyle="--", linewidth=0.4, alpha=0.5)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    logger.info("PNG saved to %s", output_path)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plotly HTML visualisation
# ---------------------------------------------------------------------------


def _save_html(
    colmax: np.ndarray,
    xx: np.ndarray,
    yy: np.ndarray,
    geojson_path: Optional[Path],
    radar_lat: float,
    radar_lon: float,
    radar_name: str,
    timestamp_str: str,
    strategy: str,
    vol_nr: str,
    output_path: Path,
) -> None:
    """Save an interactive Plotly HTML map of the COLMAX grid with cores/tops overlaid."""
    try:
        import plotly.graph_objects as go
    except ImportError as exc:
        logger.warning("plotly is not installed — cannot generate HTML output: %s", exc)
        return

    try:
        from pyart.core.transforms import geographic_to_cartesian_aeqd
    except ImportError as exc:
        logger.warning("pyart not available for HTML coordinate transform: %s", exc)
        return

    # ------------------------------------------------------------------
    # Load detection results from GeoJSON
    # ------------------------------------------------------------------
    cores: list = []
    tops: list = []
    if geojson_path is not None and geojson_path.exists():
        try:
            with open(geojson_path) as fh:
                fc = json.load(fh)
            for feature in fc.get("features", []):
                lon, lat = feature["geometry"]["coordinates"]
                props = feature["properties"]
                x_arr, y_arr = geographic_to_cartesian_aeqd(
                    np.array([lon], dtype=np.float64),
                    np.array([lat], dtype=np.float64),
                    radar_lon,
                    radar_lat,
                )
                entry = {"x_m": float(x_arr[0]), "y_m": float(y_arr[0]), **props}
                if props.get("type") == "core":
                    cores.append(entry)
                elif props.get("type") == "top":
                    tops.append(entry)
        except Exception as exc:
            logger.warning("Could not parse GeoJSON for HTML overlay: %s", exc)

    # ------------------------------------------------------------------
    # COLMAX heatmap — downsample to a reasonable resolution for the browser
    # ------------------------------------------------------------------
    x_km = (xx / 1000.0).astype(np.float32)
    y_km = (yy / 1000.0).astype(np.float32)
    colmax_f = np.ma.filled(np.ma.masked_invalid(colmax.astype(np.float32)), fill_value=np.nan)

    # Use the 1D axes (constant per row/col since meshgrid indexing="ij")
    x_axis_km = x_km[0, :]  # shape (NX,)
    y_axis_km = y_km[:, 0]  # shape (NY,)

    traces: list = []

    # Reflectivity heatmap
    traces.append(
        go.Heatmap(
            z=colmax_f,
            x=x_axis_km,
            y=y_axis_km,
            colorscale="RdYlGn_r",
            zmin=-10.0,
            zmax=65.0,
            colorbar=dict(title="COLMAX (dBZ)", thickness=14, len=0.7),
            hovertemplate="x: %{x:.1f} km<br>y: %{y:.1f} km<br>dBZ: %{z:.1f}<extra></extra>",
            name="COLMAX",
        )
    )

    # Cores scatter
    if cores:
        traces.append(
            go.Scatter(
                x=[c["x_m"] / 1000.0 for c in cores],
                y=[c["y_m"] / 1000.0 for c in cores],
                mode="markers",
                marker=dict(
                    symbol="circle",
                    size=14,
                    color="#3b82f6",
                    line=dict(color="black", width=1),
                ),
                name=f"Cores ({len(cores)})",
                customdata=[[c.get("intensity_dbz", "?"), c.get("observation_time", "")] for c in cores],
                hovertemplate=(
                    "<b>Core</b><br>"
                    "x: %{x:.1f} km  y: %{y:.1f} km<br>"
                    "Intensity: %{customdata[0]} dBZ<br>"
                    "Time: %{customdata[1]}<extra></extra>"
                ),
            )
        )

    # Tops scatter
    if tops:
        alt_km = [
            round(t["altitude_m"] / 1000.0, 1) if isinstance(t.get("altitude_m"), (int, float)) else "?" for t in tops
        ]
        traces.append(
            go.Scatter(
                x=[t["x_m"] / 1000.0 for t in tops],
                y=[t["y_m"] / 1000.0 for t in tops],
                mode="markers",
                marker=dict(
                    symbol="triangle-up",
                    size=14,
                    color="#ef4444",
                    line=dict(color="black", width=1),
                ),
                name=f"Tops ({len(tops)})",
                customdata=[
                    [
                        ak,
                        round(t.get("dbz", float("nan")), 1) if isinstance(t.get("dbz"), float) else "?",
                        t.get("observation_time", ""),
                    ]
                    for t, ak in zip(tops, alt_km)
                ],
                hovertemplate=(
                    "<b>Top</b><br>"
                    "x: %{x:.1f} km  y: %{y:.1f} km<br>"
                    "Altitude: %{customdata[0]} km<br>"
                    "dBZ: %{customdata[1]}<br>"
                    "Time: %{customdata[2]}<extra></extra>"
                ),
            )
        )

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=dict(
            text=(
                f"{radar_name} — COLMAX · " f"cores: {len(cores)} · tops: {len(tops)}<br>" f"<sup>{timestamp_str}</sup>"
            ),
            x=0.5,
        ),
        xaxis=dict(title="Range East (km)", scaleanchor="y", scaleratio=1),
        yaxis=dict(title="Range North (km)"),
        legend=dict(x=1.12, y=1.0),
        hovermode="closest",
        width=900,
        height=820,
        margin=dict(l=60, r=160, t=80, b=60),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_path), include_plotlyjs="cdn")
    logger.info("HTML saved to %s", output_path)


# ---------------------------------------------------------------------------
# Detection report helper
# ---------------------------------------------------------------------------


def _print_report(geojson_path: Path) -> None:
    """Print a human-readable summary of the detected features."""
    try:
        with open(geojson_path) as fh:
            fc = json.load(fh)
    except Exception as exc:
        logger.warning("Could not read GeoJSON for report: %s", exc)
        return

    features = fc.get("features", [])
    cores = [f for f in features if f["properties"].get("type") == "core"]
    tops = [f for f in features if f["properties"].get("type") == "top"]

    print(f"\nDetection summary — {geojson_path.name}")
    print(f"  Cores detected: {len(cores)}")
    print(f"  Tops  detected: {len(tops)}")
    print()

    if cores:
        print("  Cores:")
        for i, f in enumerate(cores, start=1):
            lon, lat = f["geometry"]["coordinates"]
            dbz = f["properties"].get("intensity_dbz", "?")
            print(f"    [{i}]  lon={lon:.4f}  lat={lat:.4f}  intensity={dbz} dBZ")
        print()

    if tops:
        print("  Tops:")
        for i, f in enumerate(tops, start=1):
            lon, lat = f["geometry"]["coordinates"]
            alt_m = f["properties"].get("altitude_m", "?")
            alt_str = f"{alt_m / 1000:.1f} km" if isinstance(alt_m, (int, float)) else str(alt_m)
            dbz = f["properties"].get("dbz", "?")
            dbz_str = f"{dbz:.1f}" if isinstance(dbz, float) else str(dbz)
            print(f"    [{i}]  lon={lon:.4f}  lat={lat:.4f}  altitude={alt_str}  dbz={dbz_str}")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _build_parser().parse_args()

    # ------------------------------------------------------------------
    # 1. Parse the requested observation datetime
    # ------------------------------------------------------------------
    try:
        obs_str = args.obs_datetime
        if obs_str.endswith("Z"):
            obs_str = obs_str[:-1] + "+00:00"
        obs_time: datetime = datetime.fromisoformat(obs_str)
        if obs_time.tzinfo is None:
            obs_time = obs_time.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        print(f"ERROR: Could not parse --datetime '{args.obs_datetime}': {exc}", file=sys.stderr)
        sys.exit(1)

    radar_name: str = args.radar_name
    strategy: str = args.strategy
    vol_nr: str = args.vol_nr
    timestamp_str: str = obs_time.strftime("%Y%m%dT%H%M%SZ")

    logger.info(
        "Volume: radar=%s  strategy=%s  vol_nr=%s  time=%s",
        radar_name,
        strategy,
        vol_nr,
        timestamp_str,
    )

    # ------------------------------------------------------------------
    # 2. Load service config (FTP creds + filesystem paths)
    # ------------------------------------------------------------------
    app_cfg = _load_app_config()
    netcdf_dir = Path(app_cfg["ROOT_RADAR_FILES_PATH"]) / radar_name / "netcdf"
    geometry_dir = Path(app_cfg["ROOT_GEOMETRY_PATH"])

    # ------------------------------------------------------------------
    # 3. Import radarlib (deferred to allow sys.path setup above)
    # ------------------------------------------------------------------
    try:
        from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf
        from radarlib.radar_grid.geometry import load_geometry
        from radarlib.radar_grid.interpolate import apply_geometry
        from radarlib.radar_grid.products import column_max
        from radarlib.radar_grid.utils import get_field_data
        from radarlib.utils.fields_utils import determine_reflectivity_fields
    except ImportError as exc:
        print(
            f"ERROR: Could not import radarlib. Ensure the 'src/' directory is on "
            f"PYTHONPATH or run from the repository root.\n  ImportError: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # 4. Resolve the NetCDF (local cache or FTP fetch)
    # ------------------------------------------------------------------
    # Track whether we created a temp file that must be cleaned up.
    _temp_netcdf: Optional[Path] = None
    actual_obs_time = obs_time  # may be updated if we fetch a nearby datetime

    netcdf_filename = _build_netcdf_filename(radar_name, strategy, vol_nr, obs_time)
    netcdf_path = _find_netcdf(netcdf_dir, netcdf_filename)

    if netcdf_path is None:
        logger.info(
            "NetCDF not found locally. Fetching BUFR files from FTP for %s %s-%s @ %s …",
            radar_name,
            strategy,
            vol_nr,
            timestamp_str,
        )
        try:
            netcdf_path, actual_obs_time = _fetch_and_decode_bufr(
                radar_name=radar_name,
                strategy=strategy,
                vol_nr=vol_nr,
                obs_time=obs_time,
                ftp_cfg=app_cfg,
                window_hours=args.search_window_hours,
            )
        except RuntimeError as exc:
            print(f"ERROR: BUFR fetch/decode failed: {exc}", file=sys.stderr)
            sys.exit(1)

        _temp_netcdf = netcdf_path  # remember to clean up at the end

        # Update the display timestamp if we ended up using a nearby datetime.
        if actual_obs_time != obs_time:
            timestamp_str = actual_obs_time.strftime("%Y%m%dT%H%M%SZ")
            logger.info(
                "Using actual observation time %s (Δ=%.0fs from requested)",
                timestamp_str,
                abs((actual_obs_time - obs_time).total_seconds()),
            )

    # ------------------------------------------------------------------
    # 5. Find geometry file
    # ------------------------------------------------------------------
    geometry_file = _find_geometry_file(radar_name, strategy, vol_nr, geometry_dir)
    if geometry_file is None:
        print(
            f"ERROR: No geometry file found in {geometry_dir} for "
            f"{radar_name} {strategy}-{vol_nr}.\n"
            f"  Expected pattern: {radar_name}_{strategy}_{vol_nr}_*.npz",
            file=sys.stderr,
        )
        if _temp_netcdf and _temp_netcdf.exists():
            _temp_netcdf.unlink(missing_ok=True)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 6. Load and standardise radar volume
    # ------------------------------------------------------------------
    logger.info("Loading radar volume: %s", netcdf_path)
    try:
        radar = read_radar_netcdf(str(netcdf_path))
        radar = estandarizar_campos_RMA(radar)
    except Exception as exc:
        print(f"ERROR: Failed to load radar volume: {exc}", file=sys.stderr)
        if _temp_netcdf and _temp_netcdf.exists():
            _temp_netcdf.unlink(missing_ok=True)
        sys.exit(1)

    fields = determine_reflectivity_fields(radar)
    hrefl_field: str = fields["hrefl_field"]
    logger.info("Horizontal reflectivity field: %s", hrefl_field)

    if hrefl_field not in radar.fields:
        print(
            f"ERROR: Required reflectivity field '{hrefl_field}' not present in the radar volume.",
            file=sys.stderr,
        )
        if _temp_netcdf and _temp_netcdf.exists():
            _temp_netcdf.unlink(missing_ok=True)
        sys.exit(1)

    radar_lat: float = float(radar.latitude["data"].data[0])
    radar_lon: float = float(radar.longitude["data"].data[0])
    coverage_radius_m: float = float(radar.range["data"][-1])
    logger.info("Radar location: lat=%.4f  lon=%.4f", radar_lat, radar_lon)

    # ------------------------------------------------------------------
    # 7. Load precomputed geometry
    # ------------------------------------------------------------------
    logger.info("Loading geometry: %s", geometry_file)
    try:
        geometry = load_geometry(str(geometry_file))
    except Exception as exc:
        print(f"ERROR: Failed to load geometry: {exc}", file=sys.stderr)
        if _temp_netcdf and _temp_netcdf.exists():
            _temp_netcdf.unlink(missing_ok=True)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 8. Build Cartesian grids
    # ------------------------------------------------------------------
    logger.info("Applying geometry (polar → Cartesian) …")
    try:
        dbzh_field_data = get_field_data(radar, hrefl_field)
        dbzh_3d = apply_geometry(geometry, dbzh_field_data)
    except Exception as exc:
        print(f"ERROR: Failed to apply geometry to DBZH: {exc}", file=sys.stderr)
        if _temp_netcdf and _temp_netcdf.exists():
            _temp_netcdf.unlink(missing_ok=True)
        sys.exit(1)
    finally:
        try:
            del dbzh_field_data
        except NameError:
            pass
        gc.collect()

    logger.info("DBZH 3D grid shape: %s", dbzh_3d.shape)
    colmax_2d = column_max(dbzh_3d, geometry=geometry)
    from radarlib.daemons.field_processor import apply_coverage_radius_mask
    colmax_2d = apply_coverage_radius_mask(colmax_2d, geometry, coverage_radius_m)
    logger.info("COLMAX 2D grid shape: %s", colmax_2d.shape)

    valid_colmax = colmax_2d[~np.isnan(colmax_2d)]
    if len(valid_colmax) > 0:
        logger.info(
            "COLMAX stats — min=%.1f  max=%.1f  mean=%.1f  valid_px=%d",
            float(np.min(valid_colmax)),
            float(np.max(valid_colmax)),
            float(np.mean(valid_colmax)),
            len(valid_colmax),
        )
    else:
        logger.warning("COLMAX grid contains only NaN values — nothing will be detected.")

    xx, yy, z_1d = _extract_coordinates(geometry)
    logger.info(
        "Grid: nz=%d  ny=%d  nx=%d  z_range=[%.0f, %.0f] m",
        len(z_1d),
        xx.shape[0],
        xx.shape[1],
        float(z_1d[0]),
        float(z_1d[-1]),
    )

    # ------------------------------------------------------------------
    # 9. Optional RhoHV 3D grid + 2D constant-elevation PPI
    #    Matches the product daemon: rhohv_2d is derived from the lowest
    #    sweep elevation, not from a raw z=0 slice of the 3D grid.
    # ------------------------------------------------------------------
    from radarlib.radar_grid.products import constant_elevation_ppi
    from radarlib.utils.fields_utils import get_lowest_nsweep

    rhohv_3d: Optional[np.ndarray] = None
    rhohv_2d: Optional[np.ndarray] = None
    rhohv_field_name = _find_rhohv_field(radar)
    if rhohv_field_name is not None:
        logger.info("Extracting RhoHV field '%s' …", rhohv_field_name)
        try:
            rhohv_field_data = get_field_data(radar, rhohv_field_name)
            rhohv_3d = apply_geometry(geometry, rhohv_field_data)
            logger.info("RhoHV 3D grid shape: %s", rhohv_3d.shape)

            # Derive 2D RhoHV as a constant-elevation PPI at the lowest sweep
            # (identical to the product daemon: sweep = get_lowest_nsweep(radar))
            lowest_sweep = get_lowest_nsweep(radar)
            elevation_angle = float(np.unique(radar.get_elevation(lowest_sweep))[0])
            logger.info(
                "Computing RhoHV PPI at lowest sweep %d (elevation %.2f°) …",
                lowest_sweep,
                elevation_angle,
            )
            rhohv_2d = constant_elevation_ppi(
                rhohv_3d, geometry, elevation_angle=elevation_angle, interpolation="linear"
            )
            logger.info("RhoHV 2D PPI shape: %s", rhohv_2d.shape)
        except Exception as exc:
            logger.warning("Could not extract RhoHV; proceeding without quality gate: %s", exc)
            rhohv_3d = None
            rhohv_2d = None
        finally:
            try:
                del rhohv_field_data
            except NameError:
                pass
            gc.collect()
    else:
        logger.warning(
            "No RhoHV field found (%s). Quality gate will use updraft intensity only.",
            _RHOHV_FIELD_CANDIDATES,
        )

    # We no longer need the PyART radar object.
    del radar
    gc.collect()

    # ------------------------------------------------------------------
    # 10. Resolve detection config params
    #     Priority: CLI arg > app/config > radarlib.config default
    # ------------------------------------------------------------------
    import radarlib.config as _rlib_cfg  # type: ignore[import]

    def _resolve(cli_val, cfg_key, rlib_attr):
        """Return the first non-None value in: CLI → app_cfg → radarlib.config."""
        if cli_val is not None:
            return cli_val
        if cfg_key is not None:
            v = app_cfg.get(cfg_key)
            if v is not None:
                return float(v)
        return float(getattr(_rlib_cfg, rlib_attr))

    eff_min_z_core = _resolve(args.min_z_core, "MIN_Z_CORE", "CORES_MIN_Z")
    eff_min_z_updraft = _resolve(args.min_z_updraft, "MIN_Z_UP", "CORES_MIN_Z_UPDRAFT")
    eff_min_range = _resolve(args.min_range, "MIN_RANGE", "CORES_MIN_RANGE")
    eff_dedup_cores = _resolve(args.dedup_radius_cores, "R_NUCLEOS", "CORES_DEDUP_RADIUS")
    eff_dedup_tops = _resolve(args.dedup_radius_tops, "R_TOPES", "TOPS_DEDUP_RADIUS_M")
    # These three have no app/config equivalents — CLI overrides radarlib.config directly.
    eff_rhohv_threshold = _resolve(args.rhohv_threshold_cores, None, "CORES_RHOHV_THRESHOLD")
    eff_min_pixels = int(_resolve(args.min_pixels, None, "CORES_MIN_PIXELS"))
    eff_min_pixels_updraft = int(_resolve(args.min_pixels_updraft, None, "CORES_MIN_PIXELS_UPDRAFT"))

    logger.info(
        "Detection params — min_z_core=%.1f  min_z_updraft=%.1f  "
        "min_range=%.0fm  dedup_cores=%.0fm  dedup_tops=%.0fm  "
        "rhohv_threshold=%.2f  min_pixels=%d  min_pixels_updraft=%d",
        eff_min_z_core,
        eff_min_z_updraft,
        eff_min_range,
        eff_dedup_cores,
        eff_dedup_tops,
        eff_rhohv_threshold,
        eff_min_pixels,
        eff_min_pixels_updraft,
    )

    # ------------------------------------------------------------------
    # 10b. Run detection
    # ------------------------------------------------------------------
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Running cores & tops detection …")
    try:
        geojson_path: Optional[Path] = _run_detection(
            colmax_2d=colmax_2d,
            dbzh_3d=dbzh_3d,
            xx=xx,
            yy=yy,
            z_1d=z_1d,
            radar_lat=radar_lat,
            radar_lon=radar_lon,
            observation_time=actual_obs_time,
            radar_name=radar_name,
            strategy=strategy,
            vol_nr=vol_nr,
            output_dir=output_dir,
            rhohv_3d=rhohv_3d,
            rhohv_2d=rhohv_2d,
            min_z_core=eff_min_z_core,
            min_z_updraft=eff_min_z_updraft,
            min_range_m=eff_min_range,
            dedup_radius_cores=eff_dedup_cores,
            dedup_radius_tops=eff_dedup_tops,
            rhohv_threshold_cores=eff_rhohv_threshold,
            min_pixels=eff_min_pixels,
            min_pixels_updraft=eff_min_pixels_updraft,
        )
    except Exception as exc:
        print(f"ERROR: Detection pipeline failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        del dbzh_3d
        if rhohv_3d is not None:
            del rhohv_3d
        if rhohv_2d is not None:
            del rhohv_2d
        gc.collect()

    if geojson_path is not None:
        logger.info("GeoJSON written: %s", geojson_path)
        _print_report(geojson_path)
    else:
        logger.info("No convective cores or tops detected — GeoJSON file not written.")

    # ------------------------------------------------------------------
    # 11. Optional PNG
    # ------------------------------------------------------------------
    if args.with_png:
        png_path = (
            Path(args.png_output).resolve()
            if args.png_output
            else output_dir / f"{radar_name}_{strategy}_{vol_nr}_{timestamp_str}_cores_tops.png"
        )
        _save_png(
            colmax=colmax_2d,
            xx=xx,
            yy=yy,
            geojson_path=geojson_path,
            radar_lat=radar_lat,
            radar_lon=radar_lon,
            radar_name=radar_name,
            timestamp_str=timestamp_str,
            strategy=strategy,
            vol_nr=vol_nr,
            output_path=png_path,
        )

    # ------------------------------------------------------------------
    # 11b. Optional interactive HTML (Plotly)
    # ------------------------------------------------------------------
    if args.with_html:
        html_path = (
            Path(args.html_output).resolve()
            if args.html_output
            else output_dir / f"{radar_name}_{strategy}_{vol_nr}_{timestamp_str}_cores_tops.html"
        )
        _save_html(
            colmax=colmax_2d,
            xx=xx,
            yy=yy,
            geojson_path=geojson_path,
            radar_lat=radar_lat,
            radar_lon=radar_lon,
            radar_name=radar_name,
            timestamp_str=timestamp_str,
            strategy=strategy,
            vol_nr=vol_nr,
            output_path=html_path,
        )

    del colmax_2d, xx, yy
    gc.collect()

    # ------------------------------------------------------------------
    # 12. Cleanup temp NetCDF (if we created one)
    # ------------------------------------------------------------------
    if _temp_netcdf is not None and _temp_netcdf.exists():
        try:
            _temp_netcdf.unlink()
            logger.debug("Removed temporary NetCDF: %s", _temp_netcdf)
        except OSError as exc:
            logger.warning("Could not remove temporary NetCDF %s: %s", _temp_netcdf, exc)


if __name__ == "__main__":
    main()
