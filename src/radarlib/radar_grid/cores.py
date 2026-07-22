"""
Convective core detection from COLMAX reflectivity grids.

This module provides :func:`detect_cores_from_colmax`, which analyses a 2D
column-maximum reflectivity (COLMAX) grid and returns the centroids of
detected convective cores as a list of dictionaries.

No PyART objects are used here — the function operates purely on NumPy arrays
and SciPy's connected-component labeller so it can be unit-tested in complete
isolation from the radar I/O stack.
"""

import logging
import math
from typing import Optional

import numpy as np
from scipy.ndimage import label as ndimage_label

from radarlib import config

logger = logging.getLogger(__name__)


def detect_cores_from_colmax(
    colmax: np.ndarray,
    x_coords: np.ndarray,
    y_coords: np.ndarray,
    rhohv: Optional[np.ndarray] = None,
    min_dbz: float = config.CORES_MIN_Z,
    min_dbz_updraft: float = config.CORES_MIN_Z_UPDRAFT,
    min_range_m: float = config.CORES_MIN_RANGE,
    dedup_radius_m: float = config.CORES_DEDUP_RADIUS,
    rhohv_threshold: float = config.CORES_RHOHV_THRESHOLD,
    min_pixels: int = config.CORES_MIN_PIXELS,
    min_pixels_updraft: int = config.CORES_MIN_PIXELS_UPDRAFT,
) -> list:
    """
    Detect convective core centroids from a 2D COLMAX reflectivity grid.

    A core is a spatially contiguous region of high reflectivity (≥ ``min_dbz``)
    that passes a minimum-size check, a minimum-range check, and a quality gate
    based on either RhoHV or extreme reflectivity intensity.

    .. note::
        **RhoHV availability**

        This function receives a 2D COLMAX grid which carries no RhoHV
        information per se.  The caller must supply a co-registered 2D
        ``rhohv`` grid of identical shape (e.g. derived from the same
        geometry-mapped volume at the lowest sweep).  When ``rhohv`` is
        ``None``, the meteorological echo gate is skipped entirely and only
        the violent-updraft intensity gate (``max_dbz > min_dbz_updraft``
        with sufficient pixel count) can qualify a blob.  Expect a lower
        recall of weaker convective cells in that mode.

    Parameters
    ----------
    colmax : np.ndarray, shape (NY, NX)
        2D COLMAX reflectivity grid in dBZ.  May be a numpy masked array;
        masked pixels are excluded from all blobs.
    x_coords : np.ndarray, shape (NY, NX)
        Cartesian x coordinates in metres, radar-relative, same grid shape
        as ``colmax``.
    y_coords : np.ndarray, shape (NY, NX)
        Cartesian y coordinates in metres, radar-relative, same grid shape
        as ``colmax``.
    rhohv : np.ndarray (NY, NX) or None, optional
        Co-registered 2D RhoHV grid.  When supplied it enables the
        meteorological echo quality gate. When ``None`` only the updraft
        gate applies (see note above).
    min_dbz : float, optional
        Reflectivity threshold (dBZ) for initial blob detection.
        Defaults to ``config.CORES_MIN_Z`` (52 dBZ).
    min_dbz_updraft : float, optional
        Maximum dBZ required for the violent-updraft quality gate.
        Defaults to ``config.CORES_MIN_Z_UPDRAFT`` (56 dBZ).
    min_range_m : float, optional
        Minimum distance of a blob centroid from the radar origin (metres).
        Blobs with centroids closer than this are rejected as near-range
        artefacts.  Defaults to ``config.CORES_MIN_RANGE`` (12 000 m).
    dedup_radius_m : float, optional
        Merge radius for nearby centroids (metres).  When two cores are within
        this distance of each other, their centroids are merged using a weighted
        mean (weight = mean dBZ), and the merged core retains the intensity
        metrics of the stronger core.  Defaults to ``config.CORES_DEDUP_RADIUS``
        (8 000 m).
    rhohv_threshold : float, optional
        Minimum mean RhoHV to pass the meteorological echo gate (default
        0.85).
    min_pixels : int, optional
        Minimum blob pixel count for the RhoHV gate path (default 2).
    min_pixels_updraft : int, optional
        Minimum blob pixel count for the updraft gate path (default 5).

    Returns
    -------
    list of dict
        One dict per accepted convective core, sorted by descending
        ``mean_dbz``.  Each dict has the following keys:

        * ``"x_m"`` – centroid x in metres (radar-relative *float*)
        * ``"y_m"`` – centroid y in metres (radar-relative *float*)
        * ``"mean_dbz"`` – mean dBZ of all pixels in the blob (*float*)
        * ``"max_dbz"`` – maximum dBZ in the blob (*float*)
        * ``"pixel_count"`` – number of pixels in the blob (*int*)
        * ``"range_m"`` – distance of centroid from radar origin (*float*)

        Returns an empty list if no cores are detected.

    Notes
    -----
    **Connected-component labelling:**
    :func:`scipy.ndimage.label` on a boolean threshold mask with 4-neighbours
    only (up/down/left/right); diagonal pixels are NOT treated as connected.
    Conservative to avoid merging adjacent but distinct cells.

    **Deduplication strategy:**
    When two accepted cores within ``dedup_radius_m`` are identified, their
    centroids are merged using a weighted mean where the weight is the core's
    mean dBZ. The merged core retains the intensity statistics (mean_dbz,
    max_dbz, pixel_count) of the stronger core. This approach reduces fragmentation
    from wind shear while respecting both cells' spatial contributions.

    **Error handling:**
    This function never raises.  Exceptions encountered during processing
    are caught, logged at ``ERROR`` level, and an empty list (or whatever
    was accumulated up to the failure point) is returned.

    Examples
    --------
    >>> import numpy as np
    >>> from radarlib.radar_grid import detect_cores_from_colmax
    >>> ny, nx = 100, 100
    >>> x = np.linspace(-100_000, 100_000, nx)
    >>> y = np.linspace(-100_000, 100_000, ny)
    >>> yy, xx = np.meshgrid(y, x, indexing="ij")
    >>> colmax = np.full((ny, nx), 20.0)
    >>> colmax[50:55, 50:55] = 58.0   # a strong blob
    >>> cores = detect_cores_from_colmax(colmax, xx, yy)
    >>> len(cores)
    1
    """
    accepted: list = []

    try:
        # ------------------------------------------------------------------
        # Step 1 — build threshold mask, excluding masked pixels
        # ------------------------------------------------------------------
        colmax_data = np.ma.getdata(colmax).astype(np.float32, copy=False)
        mask_invalid = np.ma.getmaskarray(colmax)

        valid: np.ndarray = (colmax_data >= min_dbz) & (~mask_invalid)

        # ------------------------------------------------------------------
        # Step 2 — connected-component labelling
        # ------------------------------------------------------------------
        label_array, n_labels = ndimage_label(valid)

        if n_labels == 0:
            logger.debug("detect_cores_from_colmax: no blobs found above threshold %.1f dBZ", min_dbz)
            return []

        logger.debug("detect_cores_from_colmax: %d raw blob(s) found above %.1f dBZ", n_labels, min_dbz)

        # ------------------------------------------------------------------
        # Step 3 — evaluate each blob
        # ------------------------------------------------------------------
        rhohv_data: Optional[np.ndarray] = None
        if rhohv is not None:
            rhohv_data = np.ma.getdata(rhohv).astype(np.float32, copy=False)

        for label_id in range(1, n_labels + 1):
            blob_mask = label_array == label_id
            pixel_count = int(blob_mask.sum())

            # a) minimum blob size (skip single-pixel spikes)
            if pixel_count <= 1:
                continue

            # b) centroid — dBZ-weighted to avoid geometric bias on L-shaped blobs
            weights = colmax_data[blob_mask]
            x_c = float(np.average(x_coords[blob_mask], weights=weights))
            y_c = float(np.average(y_coords[blob_mask], weights=weights))

            # c) range from radar origin
            range_m = math.sqrt(x_c * x_c + y_c * y_c)
            if range_m < min_range_m:
                logger.debug(
                    "Blob %d rejected: centroid range %.0f m < min_range %.0f m",
                    label_id,
                    range_m,
                    min_range_m,
                )
                continue

            # d) dBZ statistics
            blob_dbz = colmax_data[blob_mask]
            mean_dbz = float(blob_dbz.mean())
            max_dbz = float(blob_dbz.max())

            # e) quality gate
            rhohv_ok = False
            if rhohv_data is not None and pixel_count > min_pixels:
                mean_rhohv = float(rhohv_data[blob_mask].mean())
                rhohv_ok = mean_rhohv > rhohv_threshold

            updraft_ok = (max_dbz > min_dbz_updraft) and (pixel_count > min_pixels_updraft)

            if not (rhohv_ok or updraft_ok):
                logger.debug(
                    "Blob %d rejected: quality gate failed " "(rhohv_ok=%s, updraft_ok=%s, max_dbz=%.1f, pixels=%d)",
                    label_id,
                    rhohv_ok,
                    updraft_ok,
                    max_dbz,
                    pixel_count,
                )
                continue

            accepted.append(
                {
                    "x_m": x_c,
                    "y_m": y_c,
                    "mean_dbz": mean_dbz,
                    "max_dbz": max_dbz,
                    "pixel_count": pixel_count,
                    "range_m": range_m,
                }
            )

        # ------------------------------------------------------------------
        # Step 4 — deduplicate by proximity with weighted mean centroids
        # ------------------------------------------------------------------
        accepted.sort(key=lambda c: c["mean_dbz"], reverse=True)

        deduplicated: list = []
        merged_flags: set = set()  # Track which candidates have been merged

        for i, candidate in enumerate(accepted):
            if i in merged_flags:
                continue  # Already merged into a previous core

            # Find all candidates close to this one
            close_candidates = [candidate]
            close_indices = [i]

            for j in range(i + 1, len(accepted)):
                if j in merged_flags:
                    continue
                other = accepted[j]
                dx = candidate["x_m"] - other["x_m"]
                dy = candidate["y_m"] - other["y_m"]
                dist = math.sqrt(dx * dx + dy * dy)
                if dist < dedup_radius_m:
                    close_candidates.append(other)
                    close_indices.append(j)

            # If no close candidates, keep this core as-is
            if len(close_candidates) == 1:
                deduplicated.append(candidate)
            else:
                # Compute weighted mean centroid using mean_dbz as weight
                total_weight = sum(c["mean_dbz"] for c in close_candidates)
                weighted_x = sum(c["x_m"] * c["mean_dbz"] for c in close_candidates) / total_weight
                weighted_y = sum(c["y_m"] * c["mean_dbz"] for c in close_candidates) / total_weight

                # Merge: keep strongest core's dBZ stats, update centroid
                merged_core = close_candidates[0].copy()
                merged_core["x_m"] = float(weighted_x)
                merged_core["y_m"] = float(weighted_y)
                merged_core["range_m"] = math.sqrt(weighted_x * weighted_x + weighted_y * weighted_y)

                deduplicated.append(merged_core)

                # Mark all merged candidates
                for j in close_indices[1:]:
                    merged_flags.add(j)

                logger.debug(
                    "detect_cores_from_colmax: merged %d core(s) within %.0f m; "
                    "weighted centroid: (%.1f, %.1f) m, mean_dbz=%.1f dBZ",
                    len(close_candidates),
                    dedup_radius_m,
                    weighted_x,
                    weighted_y,
                    close_candidates[0]["mean_dbz"],
                )

        logger.debug(
            "detect_cores_from_colmax: %d core(s) accepted after deduplication " "(%d before)",
            len(deduplicated),
            len(accepted),
        )
        return deduplicated  # already sorted descending by mean_dbz

    except Exception as exc:
        logger.error("detect_cores_from_colmax failed: %s", exc, exc_info=True)
        return accepted  # return whatever was accumulated before the failure
