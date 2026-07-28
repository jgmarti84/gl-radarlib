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

Algorithm Overview
-------------------

1. **Core Detection** (from COLMAX 2D):
   - Connected-component labelling on column-maximum reflectivity grid
   - Quality gates: RhoHV (mean > 0.85) OR violent updraft (max > 56 dBZ)
   - Deduplication: keep strongest core's peak-pixel location
   - Output: List of core dicts with centroid (x_m, y_m) and intensity metrics

2. **Top Detection** (from DBZH 3D, relative to cores):
   - For each detected core, search in cylindrical column (radius from TOPS_DEDUP_RADIUS_M config)
   - Find the **highest altitude with valid DBZH** within that cylinder
   - No thresholds applied: accepts first valid DBZH found at upper levels
   - Output: List of top dicts with altitude and parent core reference

This two-stage design ensures tops are only detected where cores exist, and uses
a simplified (parameterless) algorithm focused on finding tower peaks.

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
            "observation_time": "2026-04-28T15:10:00Z"
          }
        },
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [lon, lat]},
          "properties": {
            "type": "top",
            "altitude_m": 12500,
            "dbz": 25.5,
            "parent_core_dbz": 54,
            "radar_code": "RMA1",
            "observation_time": "2026-04-28T15:10:00Z"
          }
        }
      ]
    }

Output file path
----------------
``{output_dir}/YYYY/MM/DD/{radar_code}_{strategy}_{vol_nr}_{timestamp}_TOPS_CORES.geojson``

where ``{timestamp}`` follows the same ``%Y%m%dT%H%M%SZ`` format used for COG
filenames throughout this codebase.

The caller is responsible for passing a **ceiled** ``observation_time``
(``floor(obs_time + 10 min, 10 min)``) so that the GeoJSON timestamp aligns
with the corresponding COG products.  When the ceiled and rounded timestamps
differ, the caller also writes a second copy of the file using the rounded
timestamp — the same dual-write pattern used for COGs.  This module always
writes exactly one file at whatever ``observation_time`` it receives.

When this function returns ``None`` (no detections) and the ceiled and rounded
timestamps differ, the caller must also **delete** any pre-existing
rounded-timestamp GeoJSON from an earlier scan that landed in the same bucket,
because the COG rounded copy has already been overwritten unconditionally.
Failing to do this leaves a stale tops/cores file paired with a newer COG that
contains no convective cells — the adjacent-frame mismatch visible in the
frontend.
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
        ``{output_dir}/YYYY/MM/DD/``.
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
    import pyproj

    from radarlib.radar_grid import detect_cores_from_colmax, detect_tops_from_cores

    obs_time_str = observation_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ------------------------------------------------------------------
    # Compute the COG affine transform (EPSG:3857) using exactly the same
    # algorithm as _compute_crs_bounds in geotiff.py, so that each dot's
    # lon/lat resolves to the geographic CENTER of its corresponding COG
    # pixel rather than to the AEQD cell's theoretical geographic position.
    # The two differ by up to 3–4 km at long range (non-linear AEQD→EPSG:3857
    # distortion inside an affine approximation), which is enough to place a
    # dot several pixels away from the high-dBZ pixel it represents.
    # ------------------------------------------------------------------
    ny, nx = x_coords.shape
    x_min = float(x_coords[0, 0])
    x_max = float(x_coords[0, -1])
    y_min = float(y_coords[0, 0])
    y_max = float(y_coords[-1, 0])
    dx_aeqd = (x_max - x_min) / (nx - 1) if nx > 1 else 1.0
    dy_aeqd = (y_max - y_min) / (ny - 1) if ny > 1 else 1.0

    _local_proj = pyproj.Proj(proj="aeqd", lat_0=radar_lat, lon_0=radar_lon, x_0=0, y_0=0, datum="WGS84")
    _to_wgs84 = pyproj.Transformer.from_proj(_local_proj, pyproj.CRS("EPSG:4326"), always_xy=True)
    _corner_lons, _corner_lats = [], []
    for _x in [x_min, x_max]:
        for _y in [y_min, y_max]:
            _lon, _lat = _to_wgs84.transform(_x, _y)
            _corner_lons.append(_lon)
            _corner_lats.append(_lat)
    _west, _east = min(_corner_lons), max(_corner_lons)
    _south, _north = min(_corner_lats), max(_corner_lats)

    _to_3857 = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    _from_3857 = pyproj.Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    _merc_pts = [_to_3857.transform(_lon, _lat) for _lon in [_west, _east] for _lat in [_south, _north]]
    _west_m = min(p[0] for p in _merc_pts)
    _east_m = max(p[0] for p in _merc_pts)
    _south_m = min(p[1] for p in _merc_pts)
    _north_m = max(p[1] for p in _merc_pts)
    _dx_m = (_east_m - _west_m) / nx
    _dy_m = (_north_m - _south_m) / ny

    def _cog_pixel_lonlat(aeqd_x: float, aeqd_y: float) -> tuple:
        """Return (lon, lat) of the geographic centre of the COG pixel for (aeqd_x, aeqd_y)."""
        col = int(round((aeqd_x - x_min) / dx_aeqd))
        row = int(round((aeqd_y - y_min) / dy_aeqd))
        col = max(0, min(nx - 1, col))
        row = max(0, min(ny - 1, row))
        cog_col = col
        cog_row = (ny - 1) - row  # flipud: row 0 of COG = northernmost AEQD row
        px_merc_x = _west_m + (cog_col + 0.5) * _dx_m
        px_merc_y = _north_m - (cog_row + 0.5) * _dy_m
        return _from_3857.transform(px_merc_x, px_merc_y)

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
            "CORES_TOPS radar=%s: core detection raised %s: %s — " "skipping cores, and thus no tops will be detected.",
            radar_code,
            type(exc).__name__,
            exc,
        )

    # ------------------------------------------------------------------
    # Storm top detection (only if cores were found)
    # ------------------------------------------------------------------
    tops: list = []
    if cores:
        try:
            tops = detect_tops_from_cores(
                cores=cores,
                grid_3d=dbzh_3d,
                x_coords=x_coords,
                y_coords=y_coords,
                z_coords=z_coords,
            )
        except Exception as exc:
            logger.warning(
                "CORES_TOPS radar=%s: tops detection raised %s: %s",
                radar_code,
                type(exc).__name__,
                exc,
            )
    else:
        logger.debug(
            "CORES_TOPS radar=%s time=%s: no cores detected — skipping tops detection.",
            radar_code,
            obs_time_str,
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
    # Build GeoJSON features.
    # Each dot is placed at the geographic centre of its corresponding COG
    # pixel (using the inverse of the same affine transform the COG was
    # created with), so the dot always lands exactly on that pixel in the
    # frontend display.
    # ------------------------------------------------------------------
    features: list = []

    for core in cores:
        lon, lat = _cog_pixel_lonlat(core["x_m"], core["y_m"])
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)],
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
        lon, lat = _cog_pixel_lonlat(top["x_m"], top["y_m"])
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)],
                },
                "properties": {
                    "type": "top",
                    "altitude_m": int(top["altitude_m"]),
                    "dbz": float(top["dbz"]),
                    "radar_code": radar_code,
                    "observation_time": obs_time_str,
                    "parent_core_dbz": int(top["core_mean_dbz"]),
                },
            }
        )

    geojson: dict = {"type": "FeatureCollection", "features": features}

    # ------------------------------------------------------------------
    # Build output path:
    # {output_dir}/YYYY/MM/DD/
    # {radar_code}_{strategy}_{vol_nr}_{timestamp}_TOPS_CORES.geojson
    # The timestamp format matches the COG filename convention: %Y%m%dT%H%M%SZ
    # ------------------------------------------------------------------
    timestamp_str = observation_time.strftime("%Y%m%dT%H%M%SZ")
    subdir = (
        Path(output_dir)
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
