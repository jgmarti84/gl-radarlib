"""
Geometry computation with optional parallel processing.
"""

import gc
import logging
import os
from multiprocessing import Pool, cpu_count
from typing import Optional, Tuple

import numpy as np
from scipy.spatial import cKDTree

from .geometry import GridGeometry
from .utils import calculate_roi_dist_beam, compute_beam_height

logger = logging.getLogger(__name__)


def compute_weights(distances, roi, method="nearest"):
    """
    Calcula pesos de interpolación según la distancia y el método.

    Args:
        distances: np.ndarray de distancias en metros
        roi: Radio de influencia en metros (puede ser escalar o array del mismo tamaño que distances)
        method: 'Barnes', 'Cressman', o 'nearest'

    Returns:
        np.ndarray: Pesos (sin normalizar)
    """
    distances = np.asarray(distances, dtype=np.float32)
    roi = np.asarray(roi, dtype=np.float32)

    if distances.size == 0:
        return distances  # array vacío

    # Manejar ROI escalar o array
    if roi.ndim == 0:  # escalar
        roi_val = float(roi)
        if roi_val <= 0:
            return np.zeros_like(distances, dtype=np.float32)
    else:  # array
        if np.any(roi <= 0):
            result = np.zeros_like(distances, dtype=np.float32)
            valid_roi = roi > 0
            if not valid_roi.any():
                return result

    if method == "Barnes":
        # Barnes: w = exp(-d²/(2σ²)) con σ = ROI/2
        sigma = roi / 2.0
        # sigma > 0 ya garantizado por roi > 0
        return np.exp(-(distances * distances) / (2.0 * sigma * sigma)).astype(np.float32)

    elif method == "Barnes2":
        # Py-ART: weights = exp(-dist2 / (r2/4)) + 1e-5
        r2 = roi * roi
        dist2 = distances * distances
        w = np.exp(-dist2 / (r2 / 4.0)) + 1e-5
        return w.astype(np.float32)

    elif method == "Cressman":
        # Cressman: w = (ROI² - d²) / (ROI² + d²)
        r2 = roi * roi
        dist2 = distances * distances
        w = (r2 - dist2) / (r2 + dist2 + 1e-12)  # epsilon para estabilidad
        w = np.clip(w, 0.0, 1.0)  # evita negativos
        return w.astype(np.float32)

    elif method == "nearest":
        # Nearest: solo el más cercano tiene peso 1
        weights = np.zeros_like(distances, dtype=np.float32)
        weights[int(np.argmin(distances))] = 1.0
        return weights

    else:
        raise ValueError(f"Método desconocido: {method}")


def _process_single_level(args) -> Tuple[int, int, str]:
    """
    Worker function to process a single z-level.

    This function is designed to be called by multiprocessing.Pool.
    It builds a KD-tree for the valid gates and finds all gate-to-grid
    mappings for one horizontal slice of the grid.
    """
    (
        iz,
        gz,
        grid_y_2d,
        grid_x_2d,
        gate_x,
        gate_y,
        gate_z,
        gate_valid_mask,
        min_radius,
        h_factor,
        nb,
        bsp,
        weighting,
        max_neighbors,
        temp_dir,
        dtype_val,
        dtype_idx,
        min_beam_h,
        blind_range_m,
    ) = args

    n_points = grid_y_2d.shape[0]

    # Filter gates by toa mask
    valid_indices = np.where(gate_valid_mask)[0]
    gate_x_valid = gate_x[gate_valid_mask]
    gate_y_valid = gate_y[gate_valid_mask]
    gate_z_valid = gate_z[gate_valid_mask]

    # Build KD-tree from valid gates only
    gate_coords = np.column_stack([gate_x_valid, gate_y_valid, gate_z_valid]).astype("float32")
    tree = cKDTree(gate_coords)
    del gate_coords
    gc.collect()

    grid_z_level = np.full(n_points, gz, dtype="float32")

    # ROI for this level
    # dist_from_radar = np.sqrt(grid_x_2d**2 + grid_y_2d**2 + grid_z_level**2)
    # roi = np.maximum(min_radius, dist_from_radar * beam_factor)
    # ROI para este nivel usando dist_beam
    roi = calculate_roi_dist_beam(
        z_coords=grid_z_level,
        y_coords=grid_y_2d,
        x_coords=grid_x_2d,
        h_factor=h_factor,
        nb=nb,
        bsp=bsp,
        min_radius=min_radius,
        radar_offset=(0, 0, 0),
    )
    # Máscara de radio ciego: si el voxel está más cerca que el primer gate,
    # se salta para mantener "sin dato" en la zona no observada por el radar.
    if blind_range_m is not None and blind_range_m > 0.0:
        slant_dist = np.sqrt(grid_x_2d**2 + grid_y_2d**2 + grid_z_level**2)
        in_blind_zone = slant_dist <= float(blind_range_m)
    else:
        in_blind_zone = None

    # Máscara below-beam: si este nivel Z está debajo del haz más bajo,
    # se salta el voxel directamente (fila vacía en W → NaN al interpolar)
    if min_beam_h is not None and gz < np.max(min_beam_h):
        below_beam = gz < min_beam_h  # bool array (n_points,)
    else:
        below_beam = None  # Ningún voxel de este nivel está debajo del haz
    # Storage for this level
    level_indices = []
    level_weights = []
    level_indptr = [0]

    for i in range(n_points):
        if in_blind_zone is not None and in_blind_zone[i]:
            level_indptr.append(level_indptr[-1])
            continue

        # Saltar voxels debajo del haz más bajo del radar
        if below_beam is not None and below_beam[i]:
            level_indptr.append(level_indptr[-1])
            continue

        gx, gy, gz_pt = grid_x_2d[i], grid_y_2d[i], grid_z_level[i]
        r = roi[i]
        r2 = r * r

        point = np.array([gx, gy, gz_pt])
        candidate_indices_local = tree.query_ball_point(point, r)

        if not candidate_indices_local:
            level_indptr.append(level_indptr[-1])
            continue

        candidate_indices_local = np.array(candidate_indices_local, dtype="int32")
        candidate_indices_global = valid_indices[candidate_indices_local]

        dx = gate_x_valid[candidate_indices_local] - gx
        dy = gate_y_valid[candidate_indices_local] - gy
        dz = gate_z_valid[candidate_indices_local] - gz_pt
        d2 = dx * dx + dy * dy + dz * dz

        mask = d2 < r2
        if not np.any(mask):
            level_indptr.append(level_indptr[-1])
            continue

        final_indices = candidate_indices_global[mask]
        final_d2 = d2[mask]

        # Limitar a max_neighbors si está especificado
        if max_neighbors is not None and len(final_indices) > max_neighbors:
            # Usar argpartition para seleccionar k más cercanos eficientemente: O(n) + O(k log k)
            # 1. Partition: encuentra los k menores sin ordenar (O(n))
            kth_indices = np.argpartition(final_d2, max_neighbors)[:max_neighbors]
            # 2. Ordenar los k seleccionados por distancia (O(k log k), k << n típicamente)
            #    Esto mejora: cache locality, CSR matrix ops, y consistencia con ROI completo
            kth_indices = kth_indices[np.argsort(final_d2[kth_indices])]
            final_indices = final_indices[kth_indices]
            final_d2 = final_d2[kth_indices]

        # Calcular pesos
        d = np.sqrt(final_d2).astype("float32")
        w = compute_weights(d, r, weighting).astype(dtype_val)

        level_indices.extend(final_indices)
        level_weights.extend(w)
        level_indptr.append(level_indptr[-1] + final_indices.shape[0])

    # Save this level to temp file
    temp_file = os.path.join(temp_dir, f"geometry_level_{iz}.npz")
    np.savez(
        temp_file,
        indptr=np.array(level_indptr, dtype="int32"),
        gate_indices=np.array(level_indices, dtype="int32"),
        weights=np.array(level_weights, dtype="float32"),
    )

    n_pairs = len(level_indices)
    # Limpiar memoria del worker
    del tree, level_indices, level_weights, level_indptr
    gc.collect()

    return iz, n_pairs, temp_file


def compute_grid_geometry(
    gate_x: np.ndarray,
    gate_y: np.ndarray,
    gate_z: np.ndarray,
    grid_shape: Tuple[int, int, int],
    grid_limits: Tuple[Tuple[float, float], ...],
    temp_dir: str,
    radar_altitude: float = 0.0,
    min_radius: float = 250.0,
    h_factor: float = 0.01746,
    nb: float = 1.5,
    bsp: float = 1.2,
    weighting: str = "barnes2",
    max_neighbors: Optional[int] = None,
    toa: float = 17000.0,
    min_beam_h: Optional[np.ndarray] = None,
    blind_range_m: Optional[float] = None,
    n_workers: Optional[int] = None,
    lowest_elev_deg=None,  # Elevación mínima para máscara below-beam
    dtype_val=np.float32,
    dtype_idx=np.int32,
) -> GridGeometry:
    """
    Compute the sparse mapping from grid points to radar gates.

    This function precomputes which radar gates contribute to each grid point
    and with what weight. The result can be saved and reused for fast
    interpolation of any field from the same radar.

    Parameters
    ----------
    gate_x, gate_y, gate_z : np.ndarray
        Flattened gate coordinates in meters, shape (n_gates,)
    grid_shape : tuple
        (nz, ny, nx) grid dimensions
    grid_limits : tuple
        ((z_min, z_max), (y_min, y_max), (x_min, x_max)) in meters
    temp_dir : str
        Directory for temporary files during computation
    radar_altitude : float, optional
        Altitude of radar in meters (default: 0.0)
    min_radius : float, optional
        Minimum radius of influence in meters (default: 250.0)
    beam_factor : float, optional
        ROI = max(min_radius, distance * beam_factor)
        Default: 0.01746 = tan(1°), matching PyART's default
    weighting : str, optional
        Weighting function: 'barnes2' (default), 'cressman', or 'nearest'
    toa : float, optional
        Top of atmosphere - gates above this altitude are excluded
        (default: 17000.0 meters)
    n_workers : int, optional
        Number of parallel workers for computation.
        Default: cpu_count() - 1
        Set to 1 for sequential processing (lower peak memory usage)

    Returns
    -------
    GridGeometry
        Precomputed sparse mapping ready for fast field interpolation

    Notes
    -----
    Memory usage during computation is managed by:
    - Processing one z-level at a time
    - Writing intermediate results to temp files
    - Using multiprocessing to parallelize across levels

    For a typical radar volume with ~3.5M gates and ~900k grid points,
    expect computation time of 2-5 minutes with 4 workers.
    """
    if n_workers is None:
        n_workers = max(1, cpu_count() - 1)

    # Validate inputs
    if not os.path.isdir(temp_dir):
        raise ValueError(f"temp_dir does not exist: {temp_dir}")

    if weighting not in ("barnes2", "cressman", "nearest"):
        raise ValueError(f"Unknown weighting function: {weighting}")

    nz, ny, nx = grid_shape

    # Convert gate_z from absolute altitude to height above radar
    gate_z_relative = gate_z - radar_altitude

    z_coords = np.linspace(grid_limits[0][0], grid_limits[0][1], nz, dtype="float32")
    y_coords = np.linspace(grid_limits[1][0], grid_limits[1][1], ny, dtype="float32")
    x_coords = np.linspace(grid_limits[2][0], grid_limits[2][1], nx, dtype="float32")

    # Preparar grid 2D (Y-X plane)
    yy, xx = np.meshgrid(y_coords, x_coords, indexing="ij")
    grid_y_2d = yy.ravel().astype("float32")
    grid_x_2d = xx.ravel().astype("float32")

    # Precomputar altura mínima del haz para el plano XY (una sola vez)
    # Se pasa a cada worker para saltar voxels debajo del haz más bajo.
    # Se resta medio z_step como margen para no enmascarar niveles que
    # collapse_ppi necesitará interpolar (evita NaN en el nivel z_low).
    if lowest_elev_deg is not None:
        z_step = float(z_coords[1] - z_coords[0]) if len(z_coords) > 1 else 500.0
        below_beam_margin = 0.8 * z_step
        horiz_dist = np.sqrt(grid_x_2d**2 + grid_y_2d**2)
        min_beam_h = (compute_beam_height(horiz_dist, lowest_elev_deg, radar_altitude=0.0) - below_beam_margin).astype(
            "float32"
        )
        logger.info(
            f"  Below-beam mask: elev_min={lowest_elev_deg:.2f}°, margin={below_beam_margin:.0f}m, "
            f"beam_h range=[{min_beam_h.min():.0f}, {min_beam_h.max():.0f}]m"
        )
    else:
        min_beam_h = None

    yy, xx = np.meshgrid(y_coords, x_coords, indexing="ij")
    grid_y_2d = yy.ravel().astype("float64")
    grid_x_2d = xx.ravel().astype("float64")

    # Create mask for gates below toa
    gate_valid_mask = gate_z_relative <= toa
    n_valid_gates = gate_valid_mask.sum()
    n_excluded = gate_z_relative.shape[0] - n_valid_gates

    logger.info(f"Radar altitude: {radar_altitude:.1f} m")
    logger.info(f"Gate Z range (relative to radar): {gate_z_relative.min():.1f} to {gate_z_relative.max():.1f} m")
    logger.info(f"TOA filter: {n_valid_gates:,} gates below {toa}m (above radar), {n_excluded:,} excluded")
    logger.info(f"Processing {nz} z-levels with {n_workers} worker(s)...")

    # Use gate_z_relative for geometry computation
    args_list = [
        (
            iz,
            gz,
            grid_y_2d,
            grid_x_2d,
            gate_x,
            gate_y,
            gate_z_relative,
            gate_valid_mask,
            min_radius,
            h_factor,
            nb,
            bsp,
            weighting,
            max_neighbors,
            temp_dir,
            dtype_val,
            dtype_idx,
            min_beam_h,
            blind_range_m,
        )
        for iz, gz in enumerate(z_coords)
    ]

    if n_workers == 1:
        # Sequential processing
        results = []
        for args in args_list:
            result = _process_single_level(args)
            logger.debug(f"  Level {result[0]}: {result[1]:,} pairs")
            results.append(result)
    else:
        # Parallel processing
        with Pool(n_workers) as pool:
            results = []
            for result in pool.imap_unordered(_process_single_level, args_list):
                logger.debug(f"  Level {result[0]}: {result[1]:,} pairs")
                results.append(result)

    # Sort results by level index
    results.sort(key=lambda x: x[0])

    total_pairs = sum(r[1] for r in results)
    total_grid_points = ny * nx * nz
    logger.info(f"Merging {nz} levels ({total_pairs:,} total pairs)...")

    # Pre-allocate arrays with known sizes - much more memory efficient!
    final_indptr = np.zeros(total_grid_points + 1, dtype="int32")
    final_indices = np.empty(total_pairs, dtype="int32")
    final_weights = np.empty(total_pairs, dtype="float32")

    # Fill pre-allocated arrays level by level
    pair_offset = 0
    point_offset = 0

    for _iz, _n_pairs, temp_file in results:
        # Load level data
        data = np.load(temp_file)
        level_indptr = data["indptr"]
        level_indices = data["gate_indices"]
        level_weights = data["weights"]

        # Number of grid points at this level
        n_level_points = len(level_indptr) - 1

        # Copy indices and weights directly into pre-allocated arrays
        n_level_pairs = len(level_indices)
        if n_level_pairs > 0:
            final_indices[pair_offset : pair_offset + n_level_pairs] = level_indices
            final_weights[pair_offset : pair_offset + n_level_pairs] = level_weights

        # Build indptr for this level with proper offset
        for j in range(n_level_points):
            final_indptr[point_offset + j + 1] = pair_offset + level_indptr[j + 1]

        # Update offsets
        pair_offset += n_level_pairs
        point_offset += n_level_points

        # Explicitly close the npz file and delete references
        data.close()
        del data, level_indptr, level_indices, level_weights

        # Clean up temp file immediately
        os.remove(temp_file)

        # Force garbage collection after each level to keep memory low
        gc.collect()

    logger.info("Merge complete.")

    # Return using pre-allocated arrays directly (no copying needed!)
    return GridGeometry(
        grid_shape=grid_shape,
        grid_limits=grid_limits,
        indptr=final_indptr,
        gate_indices=final_indices,
        weights=final_weights,
        toa=toa,
    )
