"""
Storm top detection from 3D Cartesian reflectivity grids.

This module provides :func:`detect_tops_from_3d_grid`, which analyses a 3D
post-interpolation Cartesian reflectivity grid (NZ × NY × NX) and returns the
centroids of detected storm tops as a list of dictionaries, ordered by
descending altitude.

Unlike the legacy sweep-by-sweep approach, this function operates entirely on
the regular Cartesian grid produced by :func:`~radarlib.radar_grid.apply_geometry`.
Each horizontal level is processed independently with connected-component
labelling, and then candidates are aggregated and deduplicated across levels.

No PyART objects are used — the function is pure NumPy + SciPy and is fully
unit-testable in isolation from the radar I/O stack.
"""

import logging
import math
from typing import List, Optional

import numpy as np
from scipy.ndimage import label as ndimage_label

from radarlib import config

logger = logging.getLogger(__name__)


def detect_tops_from_3d_grid(
    grid_3d: np.ndarray,
    x_coords: np.ndarray,
    y_coords: np.ndarray,
    z_coords: np.ndarray,
    rhohv_3d: Optional[np.ndarray] = None,
    min_dbz: float = config.TOPS_MIN_Z,
    min_dev_m: float = config.TOPS_MIN_DEV_M,
    min_range_m: float = config.TOPS_MIN_RANGE_M,
    dedup_radius_m: float = config.TOPS_DEDUP_RADIUS_M,
    rhohv_threshold: float = config.TOPS_RHOHV_THRESHOLD,
    min_pixels: int = config.TOPS_MIN_PIXELS,
) -> list:
    """
    Detect storm top centroids from a 3D Cartesian reflectivity grid.

    Each horizontal level of the 3D grid is labelled independently.  Contiguous
    blobs at each level are evaluated against minimum-altitude, minimum-range,
    and RhoHV quality gates.  After processing all levels the surviving
    candidates are deduplicated horizontally, keeping the highest top when two
    are nearby.

    .. note::
        **RhoHV availability**

        The 3D reflectivity grid carries no RhoHV information.  The caller
        must supply a co-registered 3D ``rhohv_3d`` array of identical shape
        (NZ, NY, NX) for the quality gate to be active.  When ``rhohv_3d`` is
        ``None`` the RhoHV check is skipped entirely and blobs are accepted
        based on altitude and pixel count alone.  This increases recall but
        also increases the risk of accepting non-meteorological echoes at
        upper levels (e.g. aircraft, anomalous propagation).

    Parameters
    ----------
    grid_3d : np.ndarray, shape (NZ, NY, NX)
        3D Cartesian reflectivity grid in dBZ, produced by
        :func:`~radarlib.radar_grid.apply_geometry`.  May be a numpy masked
        array; masked pixels are excluded from all blobs.
    x_coords : np.ndarray, shape (NY, NX)
        Cartesian x coordinates in metres, radar-relative, same horizontal
        grid as ``grid_3d``.
    y_coords : np.ndarray, shape (NY, NX)
        Cartesian y coordinates in metres, radar-relative.
    z_coords : np.ndarray, shape (NZ,) or (NZ, NY, NX)
        Altitude of each grid cell in metres.  A 1D array of level altitudes
        (e.g. from :meth:`~radarlib.radar_grid.GridGeometry.z_levels`) is
        broadcast automatically.  A full 3D array can also be passed to
        account for beam-height variation across the horizontal extent.
    rhohv_3d : np.ndarray, shape (NZ, NY, NX) or None, optional
        Co-registered 3D RhoHV grid.  When supplied, blobs whose mean RhoHV
        does not exceed ``rhohv_threshold`` are rejected.  When ``None``,
        the quality gate is skipped (see note above).
    min_dbz : float, optional
        Reflectivity threshold (dBZ) used to build the binary echo mask at
        each level.  Defaults to ``config.TOPS_MIN_Z`` (20 dBZ).
    min_dev_m : float, optional
        Minimum mean altitude (m) for a blob to qualify as a storm top.
        Blobs at low levels are discarded as they represent shallow echoes,
        not true tops.  Defaults to ``config.TOPS_MIN_DEV_M`` (9 000 m).
    min_range_m : float, optional
        Minimum horizontal distance from the radar origin (m).  Pixels and
        blob centroids closer than this are excluded to suppress near-range
        artefacts, which are especially problematic at upper levels.
        Defaults to ``config.TOPS_MIN_RANGE_M`` (25 000 m).
    dedup_radius_m : float, optional
        Horizontal merge radius (m).  When two accepted tops have centroids
        within this distance of each other the lower one is discarded.
        Defaults to ``config.TOPS_DEDUP_RADIUS_M`` (17 000 m).
    rhohv_threshold : float, optional
        Minimum mean RhoHV required for a blob to pass the meteorological
        quality gate (default ``config.TOPS_RHOHV_THRESHOLD`` = 0.94).
        This threshold is intentionally stricter than the cores threshold
        because non-meteorological echoes are more common at upper levels.
    min_pixels : int, optional
        Minimum number of pixels in a blob for it to be considered (default
        2).  Single-pixel artefacts are always discarded.

    Returns
    -------
    list of dict
        One dict per accepted storm top, sorted by descending
        ``altitude_m``.  Each dict contains:

        * ``"x_m"`` – centroid x in metres (radar-relative, *float*)
        * ``"y_m"`` – centroid y in metres (radar-relative, *float*)
        * ``"altitude_m"`` – mean altitude of the blob in metres (*float*)
        * ``"altitude_km"`` – same value rounded to 1 decimal, in km (*float*)
        * ``"pixel_count"`` – number of pixels in the blob (*int*)
        * ``"range_m"`` – horizontal distance of centroid from radar (*float*)
        * ``"level_index"`` – level index ``k`` where this top was found (*int*)

        Returns an empty list if no tops are detected.

    Notes
    -----
    Connected-component labelling uses :func:`scipy.ndimage.label` with the
    default 4-neighbour connectivity, which is intentionally conservative to
    avoid merging adjacent but distinct convective cells.

    This function never raises.  Internal exceptions are caught, logged at
    ``ERROR`` level, and the partial result accumulated before the failure is
    returned.

    Examples
    --------
    >>> import numpy as np
    >>> from radarlib.radar_grid import detect_tops_from_3d_grid
    >>> nz, ny, nx = 4, 50, 50
    >>> x = np.linspace(-150_000, 150_000, nx, dtype="float32")
    >>> y = np.linspace(-150_000, 150_000, ny, dtype="float32")
    >>> yy, xx = np.meshgrid(y, x, indexing="ij")
    >>> z_1d = np.array([3000., 6000., 9000., 12000.], dtype="float32")
    >>> grid = np.zeros((nz, ny, nx), dtype="float32")
    >>> grid[3, 30:35, 30:35] = 30.0   # echo at upper level
    >>> tops = detect_tops_from_3d_grid(grid, xx, yy, z_1d, rhohv_3d=None,
    ...                                 min_dev_m=9000., min_range_m=0.)
    >>> len(tops)
    1
    """
    candidates: list = []

    try:
        nz = grid_3d.shape[0]

        # ------------------------------------------------------------------
        # Normalise z_coords to (NZ, NY, NX) — support both 1D and 3D input
        # ------------------------------------------------------------------
        z_arr = np.asarray(z_coords)
        if z_arr.ndim == 1:
            # Level altitudes are the same for every (y, x) — broadcast later
            _z_1d = z_arr.astype(np.float32)
            _z_3d: Optional[np.ndarray] = None  # use 1D indexing: z[k] is scalar
        elif z_arr.ndim == 3:
            _z_3d = z_arr.astype(np.float32)
            _z_1d = None  # type: ignore[assignment]
        else:
            raise ValueError(f"z_coords must be 1D (NZ,) or 3D (NZ, NY, NX); got shape {z_arr.shape}")

        # ------------------------------------------------------------------
        # Step 1 — Pre-compute horizontal range mask (same for all levels)
        # ------------------------------------------------------------------
        x2d = np.asarray(x_coords, dtype=np.float32)
        y2d = np.asarray(y_coords, dtype=np.float32)
        range_2d = np.sqrt(x2d * x2d + y2d * y2d)
        range_ok: np.ndarray = range_2d >= min_range_m

        # ------------------------------------------------------------------
        # Step 2 — Determine which pixels are valid (not masked) in the 3D grid
        # ------------------------------------------------------------------
        mask_3d_invalid = np.ma.getmaskarray(grid_3d)  # (NZ, NY, NX) bool
        data_3d = np.ma.getdata(grid_3d).astype(np.float32, copy=False)

        # Optional RhoHV data
        rhohv_data: Optional[np.ndarray] = None
        if rhohv_3d is not None:
            rhohv_data = np.ma.getdata(rhohv_3d).astype(np.float32, copy=False)

        # ------------------------------------------------------------------
        # Step 3 — Process each level independently
        # ------------------------------------------------------------------
        for k in range(nz):
            slice_2d: np.ndarray = data_3d[k]  # (NY, NX)
            invalid_2d: np.ndarray = mask_3d_invalid[k]  # (NY, NX)

            # z-value for this level (scalar or 2D)
            if _z_3d is not None:
                z_slice: np.ndarray = _z_3d[k]  # (NY, NX)
            else:
                z_slice = np.full_like(slice_2d, _z_1d[k])  # scalar → (NY, NX)

            # Build valid mask
            valid: np.ndarray = (slice_2d >= min_dbz) & range_ok & (~invalid_2d)

            label_array, n_labels = ndimage_label(valid)
            if n_labels == 0:
                continue

            logger.debug(
                "detect_tops_from_3d_grid: level %d — %d raw blob(s) above %.1f dBZ",
                k,
                n_labels,
                min_dbz,
            )

            for label_id in range(1, n_labels + 1):
                blob_mask: np.ndarray = label_array == label_id
                pixel_count = int(blob_mask.sum())

                # a) minimum pixel count (skip single-pixel spikes)
                if pixel_count <= min_pixels:
                    continue

                # b) mean altitude filter
                mean_alt = float(z_slice[blob_mask].mean())
                if mean_alt <= min_dev_m:
                    logger.debug(
                        "Level %d blob %d rejected: mean_alt %.0f m <= min_dev_m %.0f m",
                        k,
                        label_id,
                        mean_alt,
                        min_dev_m,
                    )
                    continue

                # c) centroid and range check
                x_c = float(x2d[blob_mask].mean())
                y_c = float(y2d[blob_mask].mean())
                range_c = math.sqrt(x_c * x_c + y_c * y_c)
                if range_c < min_range_m:
                    logger.debug(
                        "Level %d blob %d rejected: centroid range %.0f m < min_range %.0f m",
                        k,
                        label_id,
                        range_c,
                        min_range_m,
                    )
                    continue

                # d) RhoHV quality gate
                if rhohv_data is not None:
                    rhv_slice: np.ndarray = rhohv_data[k]
                    mean_rhohv = float(rhv_slice[blob_mask].mean())
                    if mean_rhohv <= rhohv_threshold:
                        logger.debug(
                            "Level %d blob %d rejected: mean_rhohv %.3f <= threshold %.3f",
                            k,
                            label_id,
                            mean_rhohv,
                            rhohv_threshold,
                        )
                        continue

                altitude_km = round(mean_alt / 1000.0, 1)
                candidates.append(
                    {
                        "x_m": x_c,
                        "y_m": y_c,
                        "altitude_m": mean_alt,
                        "altitude_km": altitude_km,
                        "pixel_count": pixel_count,
                        "range_m": range_c,
                        "level_index": k,
                    }
                )

        # ------------------------------------------------------------------
        # Step 4 — Deduplicate: keep the highest top when two are nearby
        # ------------------------------------------------------------------
        candidates.sort(key=lambda c: c["altitude_m"], reverse=True)

        accepted: list = []
        for candidate in candidates:
            too_close = False
            for kept in accepted:
                dx = candidate["x_m"] - kept["x_m"]
                dy = candidate["y_m"] - kept["y_m"]
                dist = math.sqrt(dx * dx + dy * dy)
                if dist < dedup_radius_m:
                    too_close = True
                    break
            if not too_close:
                accepted.append(candidate)

        logger.debug(
            "detect_tops_from_3d_grid: %d top(s) accepted after deduplication " "(%d candidates before)",
            len(accepted),
            len(candidates),
        )
        # Already sorted descending by altitude_m
        return accepted

    except Exception as exc:
        logger.error("detect_tops_from_3d_grid failed: %s", exc, exc_info=True)
        return candidates  # return whatever was accumulated before the failure


def detect_tops_from_cores(
    cores: List,
    grid_3d: np.ndarray,
    x_coords: np.ndarray,
    y_coords: np.ndarray,
    z_coords: np.ndarray,
    radius_m: float = config.TOPS_DEDUP_RADIUS_M,
) -> list:
    """
    Detect storm tops as the highest altitude with valid DBZH within a cylindrical
    column around each detected convective core centroid.

    This function implements a core-relative tops detection algorithm: for each
    detected core, it finds the maximum altitude point in a cylindrical column
    (radius = ``radius_m``) centered on the core's (x, y) centroid within the 3D
    reflectivity grid.

    Unlike :func:`detect_tops_from_3d_grid`, which performs independent 3D grid
    analysis with multiple threshold parameters, this function:

    - Is always tethered to core detections (requires non-empty cores list)
    - Uses only one parameter: the cylindrical search radius
    - Accepts the **first valid DBZH value encountered** at upper levels (per-core)
    - Does NOT apply MIN_Z, MIN_DEV_M, MIN_RANGE_M, or RhoHV thresholds

    Parameters
    ----------
    cores : list of dict
        Output from :func:`detect_cores_from_colmax`. Each dict must contain:
        * ``"x_m"`` – centroid x in metres (radar-relative)
        * ``"y_m"`` – centroid y in metres (radar-relative)
        * ``"mean_dbz"`` – mean dBZ of the core blob
    grid_3d : np.ndarray, shape (NZ, NY, NX)
        3D Cartesian reflectivity grid in dBZ. May be a numpy masked array;
        masked pixels are skipped when searching for peaks.
    x_coords : np.ndarray, shape (NY, NX)
        Cartesian x coordinates in metres, radar-relative.
    y_coords : np.ndarray, shape (NY, NX)
        Cartesian y coordinates in metres, radar-relative.
    z_coords : np.ndarray, shape (NZ,) or (NZ, NY, NX)
        Altitude values in metres. A 1D array of level altitudes is broadcast
        automatically; a 3D array can represent beam height variation.
    radius_m : float, optional
        Cylindrical search radius around each core centroid (metres).
        Defaults to ``config.TOPS_DEDUP_RADIUS_M`` (17 000 m).

    Returns
    -------
    list of dict
        One dict per detected top. Each dict contains:

        * ``"x_m"`` – location x in metres (of highest DBZH point, *float*)
        * ``"y_m"`` – location y in metres (of highest DBZH point, *float*)
        * ``"altitude_m"`` – altitude of the point in metres (*float*)
        * ``"altitude_km"`` – same value rounded to 1 decimal, in km (*float*)
        * ``"dbz"`` – reflectivity value at the top (*float*)
        * ``"core_x_m"`` – parent core centroid x (*float*)
        * ``"core_y_m"`` – parent core centroid y (*float*)
        * ``"core_mean_dbz"`` – parent core mean dBZ (*float*)

        Returns an empty list if:
        * ``cores`` is empty, or
        * no valid DBZH found within any cylinder.
    """
    tops: list = []

    try:
        if not cores:
            logger.debug("detect_tops_from_cores: no cores provided; returning empty list")
            return []

        # Normalize z_coords to (NZ, NY, NX) — support both 1D and 3D input
        z_arr = np.asarray(z_coords)
        if z_arr.ndim == 1:
            _z_1d = z_arr.astype(np.float32)
            _z_3d = None
        elif z_arr.ndim == 3:
            _z_3d = z_arr.astype(np.float32)
            _z_1d = None
        else:
            raise ValueError(f"z_coords must be 1D (NZ,) or 3D (NZ, NY, NX); got shape {z_arr.shape}")

        # Grid dimensions and data extraction
        nz = grid_3d.shape[0]
        data_3d = np.ma.getdata(grid_3d).astype(np.float32, copy=False)
        mask_3d_invalid = np.ma.getmaskarray(grid_3d)

        x2d = np.asarray(x_coords, dtype=np.float32)
        y2d = np.asarray(y_coords, dtype=np.float32)

        # Process each core
        for core in cores:
            core_x = float(core["x_m"])
            core_y = float(core["y_m"])
            core_mean_dbz = float(core["mean_dbz"])

            logger.debug(
                "detect_tops_from_cores: searching for top near core at (%.1f, %.1f) m within radius %.0f m",
                core_x,
                core_y,
                radius_m,
            )

            # Find grid points within cylindrical radius of core centroid
            dx = x2d - core_x
            dy = y2d - core_y
            horizontal_distance = np.sqrt(dx * dx + dy * dy)
            in_cylinder = horizontal_distance <= radius_m

            if not in_cylinder.any():
                logger.debug(
                    "detect_tops_from_cores: no grid points within cylinder (%.0f m) of core (%.1f, %.1f) m",
                    radius_m,
                    core_x,
                    core_y,
                )
                continue

            # Search all altitude levels for the highest valid DBZH
            highest_top = None
            highest_altitude = -1.0
            highest_dbz = None

            for k in range(nz):
                level_data = data_3d[k]
                level_invalid = mask_3d_invalid[k]

                # Get z value for this level
                if _z_3d is not None:
                    z_level = _z_3d[k]
                else:
                    z_level = _z_1d[k]

                # Extract values within cylinder at this level
                valid_in_cylinder = in_cylinder & (~level_invalid)

                if not valid_in_cylinder.any():
                    continue

                # Find the grid point with maximum reflectivity
                dbz_in_cylinder = level_data[valid_in_cylinder]
                max_dbz = float(np.nanmax(dbz_in_cylinder))

                # If this level has a higher altitude with valid echo, use it
                # (prefer higher altitudes when available)
                z_at_level = float(z_level[valid_in_cylinder].max()) if z_level.ndim > 1 else float(z_level)

                if z_at_level > highest_altitude and max_dbz > 0:  # Guard against NaN/extreme values
                    highest_altitude = z_at_level
                    highest_dbz = max_dbz

                    # Find the (x, y) location of the max dBZ point within cylinder
                    max_idx = np.argmax(dbz_in_cylinder)
                    valid_points_idx = np.where(valid_in_cylinder)
                    point_y_idx = valid_points_idx[0][max_idx]
                    point_x_idx = valid_points_idx[1][max_idx]

                    highest_top = {
                        "x_m": float(x2d[point_y_idx, point_x_idx]),
                        "y_m": float(y2d[point_y_idx, point_x_idx]),
                        "altitude_m": highest_altitude,
                        "altitude_km": round(highest_altitude / 1000.0, 1),
                        "dbz": highest_dbz,
                        "core_x_m": core_x,
                        "core_y_m": core_y,
                        "core_mean_dbz": core_mean_dbz,
                    }

            if highest_top is not None:
                tops.append(highest_top)
                logger.debug(
                    "detect_tops_from_cores: found top for core (%.1f, %.1f) m at altitude %.0f m, dBZ=%.1f",
                    core_x,
                    core_y,
                    highest_top["altitude_m"],
                    highest_top["dbz"],
                )
            else:
                logger.debug(
                    "detect_tops_from_cores: no valid DBZH found within cylinder around core (%.1f, %.1f) m",
                    core_x,
                    core_y,
                )

        logger.debug("detect_tops_from_cores: %d top(s) detected for %d core(s)", len(tops), len(cores))
        return tops

    except Exception as exc:
        logger.error("detect_tops_from_cores failed: %s", exc, exc_info=True)
        return tops  # return whatever was accumulated before the failure
