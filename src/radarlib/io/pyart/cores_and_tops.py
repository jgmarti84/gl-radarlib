"""
cores_and_tops.py — Integration function for convective core and storm top detection.

Provides :func:`generate_cores_and_tops`, the single call-site used inside
:meth:`~radarlib.daemons.product_daemon.ProductGenerationDaemon._generate_raw_cog_products_sync`
to detect convective cores and storm tops from a processed radar volume and write
the results as a GeoJSON FeatureCollection.

This is the **only** module that imports both ``radarlib.radar_grid`` detection
functions and PyART coordinate utilities.  It is imported lazily at the call-site
in ``product_daemon.py`` (local import after the guard block) to keep daemon
startup cost low.

GeoJSON schema reference
------------------------
::

    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [lon, lat]},
          "properties": {
            "type": "core",
            "intensity_dbz": 54,
            "radar_code": "RMA1",
            "observation_time": "2026-04-28T15:00:00Z"
          }
        },
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [lon, lat]},
          "properties": {
            "type": "top",
            "altitude_m": 12500,
            "radar_code": "RMA1",
            "observation_time": "2026-04-28T15:00:00Z"
          }
        }
      ]
    }

Output file path
----------------
``{output_dir}/{radar_code}/YYYY/MM/DD/{radar_code}_{strategy}_{vol_nr}_{timestamp}_TOPS_CORES.geojson``

where ``{timestamp}`` follows the same ``%Y%m%dT%H%M%SZ`` format used for COG
filenames throughout this codebase.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


def generate_cores_and_tops(
    colmax_2d: np.ndarray,
    dbzh_3d: np.ndarray,
    x_coords: np.ndarray,
    y_coords: np.ndarray,
    z_coords: np.ndarray,
    radar_lat: float,
    radar_lon: float,
    observation_time: datetime,
    radar_code: str,
    strategy: str,
    vol_nr: str,
    output_dir: Path,
    rhohv_3d: Optional[np.ndarray] = None,
    rhohv_2d: Optional[np.ndarray] = None,
) -> Optional[Path]:
    """
    Detect convective cores and storm tops and write one GeoJSON file per volume.

    This function wraps the core implementation with an outermost ``try/except``
    so that **any** unexpected failure is logged but never re-raised.  The calling
    product-generation pipeline must not be aborted by this secondary product.

    Parameters
    ----------
    colmax_2d : np.ndarray, shape (NY, NX)
        2D column-maximum reflectivity grid in dBZ.
    dbzh_3d : np.ndarray, shape (NZ, NY, NX)
        3D Cartesian reflectivity grid in dBZ produced by ``apply_geometry``.
    x_coords : np.ndarray, shape (NY, NX)
        Cartesian x coordinates in metres, radar-relative.
    y_coords : np.ndarray, shape (NY, NX)
        Cartesian y coordinates in metres, radar-relative.
    z_coords : np.ndarray, shape (NZ,)
        1D array of altitude values in metres (from ``geometry.z_levels()``).
    radar_lat : float
        Radar latitude in decimal degrees.
    radar_lon : float
        Radar longitude in decimal degrees.
    observation_time : datetime
        Timezone-aware UTC datetime of the observation.
    radar_code : str
        Radar station identifier (e.g. ``"RMA1"``).
    strategy : str
        Volume strategy code (e.g. ``"0315"``).
    vol_nr : str
        Volume number string (e.g. ``"01"``).
    output_dir : Path
        Root directory for GeoJSON output.  Subdirectories are created
        automatically following the pattern
        ``{output_dir}/{radar_code}/YYYY/MM/DD/``.
    rhohv_3d : np.ndarray (NZ, NY, NX) or None, optional
        Co-registered 3D cross-correlation ratio grid for quality gating.
        Pass ``None`` when the RhoHV field is absent from the volume; detection
        will run with the quality gate disabled.
    rhohv_2d : np.ndarray (NY, NX) or None, optional
        2D RhoHV grid, used for core detection and quality gating.

    Returns
    -------
    Path or None
        Absolute path of the written GeoJSON file, or ``None`` when:

        * both ``cores`` and ``tops`` detection results are empty, **or**
        * the output directory cannot be created, **or**
        * writing the GeoJSON file fails, **or**
        * any unexpected exception is raised.
    """
    t0 = time.monotonic()
    try:
        return _run(
            colmax_2d=colmax_2d,
            dbzh_3d=dbzh_3d,
            x_coords=x_coords,
            y_coords=y_coords,
            z_coords=z_coords,
            radar_lat=radar_lat,
            radar_lon=radar_lon,
            observation_time=observation_time,
            radar_code=radar_code,
            strategy=strategy,
            vol_nr=vol_nr,
            output_dir=output_dir,
            rhohv_3d=rhohv_3d,
            rhohv_2d=rhohv_2d,
            t0=t0,
        )
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.error(
            "CORES_TOPS radar=%s time=%s elapsed=%dms unexpected error: %s",
            radar_code,
            observation_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            elapsed_ms,
            exc,
            exc_info=True,
        )
        return None


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _run(
    colmax_2d: np.ndarray,
    dbzh_3d: np.ndarray,
    x_coords: np.ndarray,
    y_coords: np.ndarray,
    z_coords: np.ndarray,
    radar_lat: float,
    radar_lon: float,
    observation_time: datetime,
    radar_code: str,
    strategy: str,
    vol_nr: str,
    output_dir: Path,
    rhohv_3d: Optional[np.ndarray],
    rhohv_2d: Optional[np.ndarray],
    t0: float,
) -> Optional[Path]:
    """Internal worker — called exclusively from :func:`generate_cores_and_tops`."""
    # Lazy imports: only pulled in when actually called, keeping daemon startup fast.
    from pyart.core.transforms import cartesian_to_geographic_aeqd

    from radarlib.radar_grid import detect_cores_from_colmax, detect_tops_from_3d_grid

    obs_time_str = observation_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ------------------------------------------------------------------
    # Derive 2D RhoHV for core detection: lowest level of the 3D array.
    # The cores function works on a 2D COLMAX grid, so only one level
    # of RhoHV is needed as a spatial quality gate.
    # ------------------------------------------------------------------
    if rhohv_3d is not None:
        if rhohv_2d is None:
            rhohv_2d = rhohv_3d[0]

    # ------------------------------------------------------------------
    # Convective core detection
    # ------------------------------------------------------------------
    cores: list = []
    try:
        cores = detect_cores_from_colmax(
            colmax=colmax_2d,
            x_coords=x_coords,
            y_coords=y_coords,
            rhohv=rhohv_2d,
        )
    except Exception as exc:
        logger.warning(
            "CORES_TOPS radar=%s: core detection raised %s: %s — " "skipping cores, still attempting tops.",
            radar_code,
            type(exc).__name__,
            exc,
        )

    # ------------------------------------------------------------------
    # Storm top detection
    # ------------------------------------------------------------------
    tops: list = []
    try:
        tops = detect_tops_from_3d_grid(
            grid_3d=dbzh_3d,
            x_coords=x_coords,
            y_coords=y_coords,
            z_coords=z_coords,
            rhohv_3d=rhohv_3d,
        )
    except Exception as exc:
        logger.warning(
            "CORES_TOPS radar=%s: tops detection raised %s: %s",
            radar_code,
            type(exc).__name__,
            exc,
        )

    # ------------------------------------------------------------------
    # Nothing detected — skip file write
    # ------------------------------------------------------------------
    if not cores and not tops:
        logger.debug(
            "CORES_TOPS radar=%s time=%s: no features detected — GeoJSON not written.",
            radar_code,
            obs_time_str,
        )
        return None

    # ------------------------------------------------------------------
    # Convert radar-relative Cartesian (m) → geographic (lon, lat)
    # and build GeoJSON features.
    # pyart.core.transforms.cartesian_to_geographic_aeqd returns (lon, lat).
    # ------------------------------------------------------------------
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
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon_arr[0]), float(lat_arr[0])],
                },
                "properties": {
                    "type": "core",
                    "intensity_dbz": int(core["mean_dbz"]),
                    "radar_code": radar_code,
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
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon_arr[0]), float(lat_arr[0])],
                },
                "properties": {
                    "type": "top",
                    "altitude_m": int(top["altitude_m"]),
                    "radar_code": radar_code,
                    "observation_time": obs_time_str,
                },
            }
        )

    geojson: dict = {"type": "FeatureCollection", "features": features}

    # ------------------------------------------------------------------
    # Build output path:
    # {output_dir}/{radar_code}/YYYY/MM/DD/
    # {radar_code}_{strategy}_{vol_nr}_{timestamp}_TOPS_CORES.geojson
    # The timestamp format matches the COG filename convention: %Y%m%dT%H%M%SZ
    # ------------------------------------------------------------------
    timestamp_str = observation_time.strftime("%Y%m%dT%H%M%SZ")
    subdir = (
        Path(output_dir)
        / radar_code
        / f"{observation_time.year:04d}"
        / f"{observation_time.month:02d}"
        / f"{observation_time.day:02d}"
    )
    filename = f"{radar_code}_{strategy}_{vol_nr}_{timestamp_str}_TOPS_CORES.geojson"
    output_path = subdir / filename

    # ------------------------------------------------------------------
    # Create output directory
    # ------------------------------------------------------------------
    try:
        subdir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.error(
            "CORES_TOPS radar=%s: cannot create output directory %s: %s",
            radar_code,
            subdir,
            exc,
        )
        return None

    # ------------------------------------------------------------------
    # Write GeoJSON — compact encoding, no trailing newline
    # ------------------------------------------------------------------
    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(geojson, fh, separators=(",", ":"))
    except Exception as exc:
        logger.error(
            "CORES_TOPS radar=%s: failed to write GeoJSON %s: %s",
            radar_code,
            output_path,
            exc,
            exc_info=True,
        )
        return None

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "CORES_TOPS radar=%s time=%s cores=%d tops=%d elapsed=%dms -> %s",
        radar_code,
        obs_time_str,
        len(cores),
        len(tops),
        elapsed_ms,
        output_path.name,
    )
    return output_path
