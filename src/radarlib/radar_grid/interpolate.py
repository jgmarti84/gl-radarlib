"""
Fast field interpolation using precomputed geometry.
"""

import logging
from typing import Dict, List, Optional

import numpy as np

from .filters import GateFilter
from .geometry import GridGeometry

logger = logging.getLogger(__name__)


def apply_geometry(
    geometry: GridGeometry,
    field_data: np.ndarray,
    # field_mask: Optional[np.ndarray] = None,
    additional_filters: Optional[List[GateFilter]] = None,
    fill_value: float = np.nan,
) -> np.ndarray:
    """
    Apply precomputed geometry to interpolate a field onto the grid.

    This function uses vectorized NumPy operations to efficiently
    compute weighted averages for each grid point.

    Parameters
    ----------
    geometry : GridGeometry
        Precomputed sparse mapping from compute_grid_geometry
    field_data : np.ndarray
        Flattened field values at each gate, shape (n_gates,)
    # field_mask : np.ndarray, optional
    #     Boolean mask where True = invalid data, shape (n_gates,)
    #     If None, automatically masks NaN and Inf values
    additional_filters : List[GateFilter], optional
        List of gatefilters to apply before interpolation
    fill_value : float, optional
        Value for grid points with no valid data (default: np.nan)

    Returns
    -------
    np.ndarray
        Interpolated field on grid, shape = geometry.grid_shape
    """
    n_grid = np.prod(geometry.grid_shape)

    # Build mask if not provided
    if not isinstance(additional_filters, List):
        if additional_filters is None:
            additional_filters = []
        elif isinstance(additional_filters, GateFilter):
            additional_filters = [additional_filters]
        else:
            raise ValueError("additional_filters must be a list of GateFilter objects")

    # field_mask = field_data.mask
    field_mask = np.ma.getmask(field_data)
    for gf in additional_filters:
        field_mask = field_mask | gf.gate_excluded

    # field_data = field_data.data
    field_data = np.ma.getdata(field_data)

    # if additional_filters is None:
    #     field_mask = np.isnan(field_data) | np.isinf(field_data)

    indptr = geometry.indptr
    gate_indices = geometry.gate_indices
    weights = geometry.weights

    # Validate gate_indices against field_data size
    n_gates = field_data.shape[0]
    max_gate_index = gate_indices.max() if len(gate_indices) > 0 else 0
    if max_gate_index >= n_gates:
        raise IndexError(
            f"Geometry gate_indices (max={max_gate_index}) exceed field_data size ({n_gates}). "
            f"The geometry was likely built from a radar with different dimensions "
            f"(e.g., different nrays or ngates). "
            f"Delete the geometry .npz file and gate_coordinates .npz file for this "
            f"volume type and let them be regenerated from a matching radar file."
        )

    # Get all values and masks at once
    all_values = field_data[gate_indices]
    all_masks = field_mask[gate_indices]

    # Replace masked values with 0 BEFORE arithmetic to avoid inf*0=nan issues
    safe_values = np.where(all_masks, 0.0, all_values)
    effective_weights = np.where(all_masks, 0.0, weights)

    # Compute weighted values
    weighted_values = effective_weights * safe_values

    # Find non-empty segments
    segment_lengths = np.diff(indptr)
    non_empty = segment_lengths > 0
    non_empty_indices = np.where(non_empty)[0]
    non_empty_starts = indptr[:-1][non_empty]

    # Segmented sums using reduceat
    if len(non_empty_starts) > 0:
        val_sums = np.add.reduceat(weighted_values, non_empty_starts)
        weight_sums = np.add.reduceat(effective_weights, non_empty_starts)
    else:
        val_sums = np.array([])
        weight_sums = np.array([])

    # Build result array
    result = np.full(n_grid, fill_value, dtype="float32")
    valid_sums = weight_sums > 0
    valid_indices = non_empty_indices[valid_sums]
    result[valid_indices] = val_sums[valid_sums] / weight_sums[valid_sums]

    return result.reshape(geometry.grid_shape)


def apply_geometry_multi(
    geometry: GridGeometry,
    fields: Dict[str, np.ndarray],
    additional_filters: Optional[Dict[str, List[GateFilter]]] = None,
    fill_value: float = np.nan,
) -> Dict[str, np.ndarray]:
    """
    Apply precomputed geometry to multiple fields at once.

    Parameters
    ----------
    geometry : GridGeometry
        Precomputed sparse mapping
    fields : dict
        Dictionary of {field_name: field_data} where field_data is
        flattened array of shape (n_gates,)
    field_masks : dict, optional
        Dictionary of {field_name: mask} for each field.
        If None or if a field is not in the dict, auto-masking is used.
    fill_value : float, optional
        Value for grid points with no valid data (default: np.nan)

    Returns
    -------
    dict
        Dictionary of {field_name: gridded_data}
    """
    if additional_filters is None:
        additional_filters = {}

    results = {}
    for name, data in fields.items():
        filters = additional_filters.get(name, None)
        results[name] = apply_geometry(geometry, data, additional_filters=filters, fill_value=fill_value)

    return results
