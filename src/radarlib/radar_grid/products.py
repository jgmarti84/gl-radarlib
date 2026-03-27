"""
Radar products derived from gridded data.

Includes:
- Constant Elevation PPI (Plan Position Indicator)
- Column Maximum (COLMAX)
- Earth curvature corrections
"""

import logging
from typing import Optional

import numpy as np

from .geometry import GridGeometry

logger = logging.getLogger(__name__)


# Constants for Earth curvature calculations
EARTH_RADIUS = 6371000.0  # Earth's radius in meters
EFFECTIVE_RADIUS_FACTOR = 4.0 / 3.0  # Standard refraction (4/3 Earth model)


def compute_beam_height(
    horizontal_distance: np.ndarray,
    elevation_angle: float,
    radar_altitude: float = 0.0,
    ke: float = EFFECTIVE_RADIUS_FACTOR,
    re: float = EARTH_RADIUS,
) -> np.ndarray:
    """
    Compute radar beam height accounting for Earth's curvature.

    Uses the standard 4/3 effective Earth radius model to account for
    atmospheric refraction.

    Parameters
    ----------
    horizontal_distance : np.ndarray
        Horizontal distance from radar in meters (ground range)
    elevation_angle : float
        Elevation angle in degrees
    radar_altitude : float, optional
        Radar altitude above sea level in meters (default: 0.0)
    ke : float, optional
        Effective Earth radius factor (default: 4/3 for standard refraction)
    re : float, optional
        Earth's radius in meters (default: 6371000.0)

    Returns
    -------
    np.ndarray
        Beam height in meters above sea level

    Notes
    -----
    The formula used is:
        h = sqrt(r² + (ke*Re)² + 2*r*ke*Re*sin(θ)) - ke*Re + h0

    Where r (slant range) is approximated from horizontal distance as:
        r ≈ s / cos(θ)  for small elevation angles

    For a more accurate calculation at high elevation angles, an iterative
    approach could be used.

    References
    ----------
    Doviak, R. J., and D. S. Zrnić, 1993: Doppler Radar and Weather
    Observations. Academic Press, 562 pp.
    """
    elevation_rad = np.radians(elevation_angle)
    ke_re = ke * re

    # Approximate slant range from horizontal distance
    # For small angles: r ≈ s / cos(θ)
    # For better accuracy at higher angles, we iterate
    cos_elev = np.cos(elevation_rad)
    sin_elev = np.sin(elevation_rad)

    # Initial estimate of slant range
    slant_range = horizontal_distance / np.maximum(cos_elev, 0.01)

    # Compute height using the radar equation with Earth curvature
    height = np.sqrt(slant_range**2 + ke_re**2 + 2 * slant_range * ke_re * sin_elev) - ke_re + radar_altitude

    return height


def compute_beam_height_simple(
    horizontal_distance: np.ndarray,
    elevation_angle: float,
    radar_altitude: float = 0.0,
    ke: float = EFFECTIVE_RADIUS_FACTOR,
    re: float = EARTH_RADIUS,
) -> np.ndarray:
    """
    Compute radar beam height using simplified formula with Earth curvature.

    This uses the commonly-used approximation:
        h = r*sin(θ) + r²/(2*ke*Re) + h0

    Parameters
    ----------
    horizontal_distance : np.ndarray
        Horizontal distance from radar in meters
    elevation_angle : float
        Elevation angle in degrees
    radar_altitude : float, optional
        Radar altitude in meters (default: 0.0)
    ke : float, optional
        Effective Earth radius factor (default: 4/3)
    re : float, optional
        Earth's radius in meters (default: 6371000.0)

    Returns
    -------
    np.ndarray
        Beam height in meters
    """
    elevation_rad = np.radians(elevation_angle)
    ke_re = ke * re

    # Approximate slant range
    slant_range = horizontal_distance / np.maximum(np.cos(elevation_rad), 0.01)

    # Simplified formula: h = r*sin(θ) + r²/(2*ke*Re) + h0
    height = slant_range * np.sin(elevation_rad) + (slant_range**2) / (2 * ke_re) + radar_altitude

    return height


def compute_beam_height_flat(
    horizontal_distance: np.ndarray, elevation_angle: float, radar_altitude: float = 0.0
) -> np.ndarray:
    """
    Compute radar beam height assuming flat Earth (no curvature correction).

    Simple trigonometric calculation:
        h = horizontal_distance * tan(θ) + h0

    Parameters
    ----------
    horizontal_distance : np.ndarray
        Horizontal distance from radar in meters
    elevation_angle : float
        Elevation angle in degrees
    radar_altitude : float, optional
        Radar altitude in meters (default: 0.0)

    Returns
    -------
    np.ndarray
        Beam height in meters
    """
    elevation_rad = np.radians(elevation_angle)
    return horizontal_distance * np.tan(elevation_rad) + radar_altitude


def constant_elevation_ppi(
    grid: np.ndarray,
    geometry: GridGeometry,
    elevation_angle: float,
    # radar_altitude: float = 0.0,
    interpolation: str = "linear",
    earth_curvature: bool = True,
    ke: float = EFFECTIVE_RADIUS_FACTOR,
) -> np.ndarray:
    """
    Extract a constant elevation PPI from a 3D gridded field.

    For each (x, y) point, computes the altitude corresponding to the
    given elevation angle (optionally accounting for Earth's curvature),
    then interpolates vertically in the 3D grid.

    Parameters
    ----------
    grid : np.ndarray
        3D gridded field data, shape (nz, ny, nx)
    geometry : GridGeometry
        Grid geometry containing grid_shape and grid_limits
    elevation_angle : float
        Elevation angle in degrees
    radar_altitude : float, optional
        Radar altitude in meters (default: 0.0)
    interpolation : str, optional
        Interpolation method: 'linear' or 'nearest' (default: 'linear')
    earth_curvature : bool, optional
        If True, account for Earth's curvature using 4/3 Earth model.
        If False, use flat Earth assumption. (default: True)
    ke : float, optional
        Effective Earth radius factor for curvature correction (default: 4/3)

    Returns
    -------
    np.ndarray
        2D array of shape (ny, nx) containing the PPI at the given elevation

    Notes
    -----
    When earth_curvature=True, uses the standard 4/3 effective Earth radius
    model which accounts for both geometric curvature and standard atmospheric
    refraction. This is important for ranges > 50 km where the difference
    becomes significant.

    Examples
    --------
    >>> # With Earth curvature (recommended)
    >>> ppi = constant_elevation_ppi(grid, geometry, elevation_angle=0.5,
    ...                               radar_altitude=100, earth_curvature=True)

    >>> # Without Earth curvature (flat Earth)
    >>> ppi = constant_elevation_ppi(grid, geometry, elevation_angle=0.5,
    ...                               radar_altitude=100, earth_curvature=False)
    """
    nz, ny, nx = geometry.grid_shape

    # Extract grid limits
    z_min, z_max = geometry.grid_limits[0]
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    # Create 2D coordinate arrays
    y_coords = np.linspace(y_min, y_max, ny, dtype="float32")
    x_coords = np.linspace(x_min, x_max, nx, dtype="float32")
    # z_coords = np.linspace(z_min, z_max, nz, dtype="float32")

    # Meshgrid for (y, x)
    yy, xx = np.meshgrid(y_coords, x_coords, indexing="ij")

    # Compute horizontal distance from radar (assumed at origin)
    horizontal_dist = np.sqrt(xx**2 + yy**2)

    # Compute altitude at each (x, y) for the given elevation angle
    if earth_curvature:
        target_z = compute_beam_height(horizontal_dist, elevation_angle, radar_altitude=0.0, ke=ke)
    else:
        target_z = compute_beam_height_flat(horizontal_dist, elevation_angle, radar_altitude=0.0)

    # Initialize output
    result = np.full((ny, nx), np.nan, dtype="float32")

    if interpolation == "nearest":
        # Find nearest z-level for each point
        z_step = (z_max - z_min) / (nz - 1) if nz > 1 else 1.0
        z_indices = np.round((target_z - z_min) / z_step).astype(int)

        # Mark out-of-bounds as invalid
        valid = (z_indices >= 0) & (z_indices < nz)
        z_indices_safe = np.clip(z_indices, 0, nz - 1)

        # Extract values using advanced indexing
        y_indices, x_indices = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
        result = grid[z_indices_safe, y_indices, x_indices]
        result[~valid] = np.nan

    elif interpolation == "linear":
        # Linear interpolation between z-levels
        z_step = (z_max - z_min) / (nz - 1) if nz > 1 else 1.0

        # Compute fractional z-index
        z_frac = (target_z - z_min) / z_step
        z_low = np.floor(z_frac).astype(int)
        z_high = z_low + 1

        # Compute interpolation weight
        weight_high = z_frac - z_low
        weight_low = 1.0 - weight_high

        # Handle boundaries
        below_grid = target_z < z_min
        above_grid = target_z > z_max

        # Clip indices for safe indexing
        z_low_safe = np.clip(z_low, 0, nz - 1)
        z_high_safe = np.clip(z_high, 0, nz - 1)

        y_indices, x_indices = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")

        # Get values at lower and upper z-levels
        val_low = grid[z_low_safe, y_indices, x_indices]
        val_high = grid[z_high_safe, y_indices, x_indices]

        # NaN-aware interpolation: when only one of the two bracketing levels has
        # valid data (e.g. the lower level is masked by the below-beam filter),
        # fall back to the single valid level instead of propagating NaN.
        # Only output NaN when both levels are NaN (genuinely unobserved).
        nan_low = np.isnan(val_low)
        nan_high = np.isnan(val_high)
        both_valid = ~nan_low & ~nan_high
        only_low = ~nan_low & nan_high
        only_high = nan_low & ~nan_high
        neither = nan_low & nan_high

        result_data = np.zeros((ny, nx), dtype="float32")
        result_data[both_valid] = (
            weight_low[both_valid] * val_low[both_valid] + weight_high[both_valid] * val_high[both_valid]
        )
        result_data[only_low] = val_low[only_low]
        result_data[only_high] = val_high[only_high]

        result = np.where(neither, np.nan, result_data).astype("float32")

        # For points outside the grid extent, force NaN
        result[below_grid] = np.nan
        result[above_grid] = np.nan

    else:
        raise ValueError(f"Unknown interpolation method: {interpolation}")

    return result


def constant_altitude_ppi(
    grid: np.ndarray, geometry: GridGeometry, altitude: float, interpolation: str = "linear"
) -> np.ndarray:
    """
    Extract a constant altitude PPI from a 3D gridded field.

    For a given altitude (z value), extracts a horizontal slice from the 3D grid.
    If the altitude doesn't correspond exactly to a grid z-level, interpolates
    between the surrounding levels.

    Parameters
    ----------
    grid : np.ndarray
        3D gridded field data, shape (nz, ny, nx)
    geometry : GridGeometry
        Grid geometry containing grid_shape and grid_limits
    altitude : float
        Target altitude in meters
    interpolation : str, optional
        Interpolation method: 'linear' or 'nearest' (default: 'linear')

    Returns
    -------
    np.ndarray
        2D array of shape (ny, nx) containing the PPI at the given altitude

    Notes
    -----
    This function works with the uniform cartesian grid coordinates. The altitude
    parameter is compared against the z-coordinates of the grid to determine
    the appropriate slice or interpolation.

    Examples
    --------
    >>> # Extract slice at 3000m altitude
    >>> ppi = constant_altitude_ppi(grid, geometry, altitude=3000.0)

    >>> # Use nearest neighbor interpolation
    >>> ppi = constant_altitude_ppi(grid, geometry, altitude=3000.0,
    ...                             interpolation='nearest')
    """
    nz, ny, nx = geometry.grid_shape

    # Extract grid limits
    z_min, z_max = geometry.grid_limits[0]

    # Create z-coordinate array
    z_coords = np.linspace(z_min, z_max, nz, dtype="float32")

    # Check if altitude is within grid bounds
    if altitude < z_min or altitude > z_max:
        logger.warning(f"Altitude {altitude}m is outside grid range [{z_min}, {z_max}]m")
        return np.full((ny, nx), np.nan, dtype="float32")

    # Find the z-index corresponding to the target altitude
    if interpolation == "nearest":
        # Find nearest z-level
        z_index = np.argmin(np.abs(z_coords - altitude))
        return grid[z_index, :, :]

    elif interpolation == "linear":
        # Check if altitude exactly matches a grid level
        z_matches = np.isclose(z_coords, altitude, rtol=1e-6)
        if np.any(z_matches):
            # Exact match found
            z_index = np.where(z_matches)[0][0]
            return grid[z_index, :, :]

        # Linear interpolation between surrounding levels
        z_step = (z_max - z_min) / (nz - 1) if nz > 1 else 1.0

        # Compute fractional z-index
        z_frac = (altitude - z_min) / z_step
        z_low = int(np.floor(z_frac))
        z_high = z_low + 1

        # Ensure indices are within bounds
        if z_low < 0:
            return grid[0, :, :]
        if z_high >= nz:
            return grid[nz - 1, :, :]

        # Compute interpolation weights
        weight_high = z_frac - z_low
        weight_low = 1.0 - weight_high

        # Get values at lower and upper z-levels
        val_low = grid[z_low, :, :]
        val_high = grid[z_high, :, :]

        # Linear interpolation
        result = weight_low * val_low + weight_high * val_high
        return result.astype("float32")

    else:
        raise ValueError(f"Unknown interpolation method: {interpolation}")


def column_max(
    grid: np.ndarray,
    z_min_idx: Optional[int] = None,
    z_max_idx: Optional[int] = None,
    z_min_alt: Optional[float] = None,
    z_max_alt: Optional[float] = None,
    geometry: Optional[GridGeometry] = None,
) -> np.ndarray:
    """
    Compute column maximum (COLMAX) - the maximum value in each vertical column.

    Parameters
    ----------
    grid : np.ndarray
        3D gridded field data, shape (nz, ny, nx)
    z_min_idx : int, optional
        Minimum z-index to include (inclusive)
    z_max_idx : int, optional
        Maximum z-index to include (inclusive)
    z_min_alt : float, optional
        Minimum altitude in meters. Requires geometry parameter.
    z_max_alt : float, optional
        Maximum altitude in meters. Requires geometry parameter.
    geometry : GridGeometry, optional
        Grid geometry, required if using altitude-based limits

    Returns
    -------
    np.ndarray
        2D array of shape (ny, nx) containing the column maximum values

    Examples
    --------
    >>> # Full column max
    >>> cmax = column_max(grid)

    >>> # Column max between z-indices 2 and 6
    >>> cmax = column_max(grid, z_min_idx=2, z_max_idx=6)

    >>> # Column max between 1000m and 8000m altitude
    >>> cmax = column_max(grid, z_min_alt=1000, z_max_alt=8000, geometry=geometry)
    """
    nz = grid.shape[0]

    # Convert altitude limits to indices if provided
    if z_min_alt is not None or z_max_alt is not None:
        if geometry is None:
            raise ValueError("geometry is required when using altitude-based limits")

        z_min_grid, z_max_grid = geometry.grid_limits[0]
        z_coords = np.linspace(z_min_grid, z_max_grid, nz)

        if z_min_alt is not None:
            z_min_idx = np.searchsorted(z_coords, z_min_alt)
        if z_max_alt is not None:
            z_max_idx = np.searchsorted(z_coords, z_max_alt, side="right") - 1

    # Apply default limits
    if z_min_idx is None:
        z_min_idx = 0
    if z_max_idx is None:
        z_max_idx = nz - 1

    # Clip to valid range
    z_min_idx = max(0, z_min_idx)
    z_max_idx = min(nz - 1, z_max_idx)

    # Slice and compute max
    grid_slice = grid[z_min_idx : z_max_idx + 1, :, :]

    return np.nanmax(grid_slice, axis=0)


def column_min(
    grid: np.ndarray,
    z_min_idx: Optional[int] = None,
    z_max_idx: Optional[int] = None,
    z_min_alt: Optional[float] = None,
    z_max_alt: Optional[float] = None,
    geometry: Optional[GridGeometry] = None,
) -> np.ndarray:
    """
    Compute column minimum - the minimum value in each vertical column.

    Parameters are the same as column_max().

    Returns
    -------
    np.ndarray
        2D array of shape (ny, nx) containing the column minimum values
    """
    nz = grid.shape[0]

    if z_min_alt is not None or z_max_alt is not None:
        if geometry is None:
            raise ValueError("geometry is required when using altitude-based limits")

        z_min_grid, z_max_grid = geometry.grid_limits[0]
        z_coords = np.linspace(z_min_grid, z_max_grid, nz)

        if z_min_alt is not None:
            z_min_idx = np.searchsorted(z_coords, z_min_alt)
        if z_max_alt is not None:
            z_max_idx = np.searchsorted(z_coords, z_max_alt, side="right") - 1

    if z_min_idx is None:
        z_min_idx = 0
    if z_max_idx is None:
        z_max_idx = nz - 1

    z_min_idx = max(0, z_min_idx)
    z_max_idx = min(nz - 1, z_max_idx)

    grid_slice = grid[z_min_idx : z_max_idx + 1, :, :]

    return np.nanmin(grid_slice, axis=0)


def column_mean(
    grid: np.ndarray,
    z_min_idx: Optional[int] = None,
    z_max_idx: Optional[int] = None,
    z_min_alt: Optional[float] = None,
    z_max_alt: Optional[float] = None,
    geometry: Optional[GridGeometry] = None,
) -> np.ndarray:
    """
    Compute column mean - the mean value in each vertical column.

    Parameters are the same as column_max().

    Returns
    -------
    np.ndarray
        2D array of shape (ny, nx) containing the column mean values
    """
    nz = grid.shape[0]

    if z_min_alt is not None or z_max_alt is not None:
        if geometry is None:
            raise ValueError("geometry is required when using altitude-based limits")

        z_min_grid, z_max_grid = geometry.grid_limits[0]
        z_coords = np.linspace(z_min_grid, z_max_grid, nz)

        if z_min_alt is not None:
            z_min_idx = np.searchsorted(z_coords, z_min_alt)
        if z_max_alt is not None:
            z_max_idx = np.searchsorted(z_coords, z_max_alt, side="right") - 1

    if z_min_idx is None:
        z_min_idx = 0
    if z_max_idx is None:
        z_max_idx = nz - 1

    z_min_idx = max(0, z_min_idx)
    z_max_idx = min(nz - 1, z_max_idx)

    grid_slice = grid[z_min_idx : z_max_idx + 1, :, :]

    return np.nanmean(grid_slice, axis=0)


def get_beam_height_difference(
    geometry: GridGeometry, elevation_angle: float, radar_altitude: float = 0.0, ke: float = EFFECTIVE_RADIUS_FACTOR
) -> np.ndarray:
    """
    Compute the difference in beam height between curved and flat Earth models.

    Useful for understanding where Earth curvature correction is significant.

    Parameters
    ----------
    geometry : GridGeometry
        Grid geometry
    elevation_angle : float
        Elevation angle in degrees
    radar_altitude : float, optional
        Radar altitude in meters (default: 0.0)
    ke : float, optional
        Effective Earth radius factor (default: 4/3)

    Returns
    -------
    np.ndarray
        2D array of shape (ny, nx) containing height difference in meters
        (curved - flat)
    """
    nz, ny, nx = geometry.grid_shape
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    y_coords = np.linspace(y_min, y_max, ny)
    x_coords = np.linspace(x_min, x_max, nx)

    yy, xx = np.meshgrid(y_coords, x_coords, indexing="ij")
    horizontal_dist = np.sqrt(xx**2 + yy**2)

    height_curved = compute_beam_height(horizontal_dist, elevation_angle, radar_altitude, ke=ke)
    height_flat = compute_beam_height_flat(horizontal_dist, elevation_angle, radar_altitude)

    return height_curved - height_flat


def get_elevation_from_z_level(
    z_level: float,
    geometry: GridGeometry,
    radar_altitude: float = 0.0,
    earth_curvature: bool = True,
    ke: float = EFFECTIVE_RADIUS_FACTOR,
) -> np.ndarray:
    """
    Compute the elevation angle at each (x, y) point for a given z-level.

    This is useful for understanding what elevation angles correspond to
    a given altitude at different ranges.

    Parameters
    ----------
    z_level : float
        Altitude in meters
    geometry : GridGeometry
        Grid geometry
    radar_altitude : float, optional
        Radar altitude in meters (default: 0.0)
    earth_curvature : bool, optional
        If True, account for Earth's curvature (default: True)
    ke : float, optional
        Effective Earth radius factor (default: 4/3)

    Returns
    -------
    np.ndarray
        2D array of shape (ny, nx) containing elevation angles in degrees
    """
    nz, ny, nx = geometry.grid_shape
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    y_coords = np.linspace(y_min, y_max, ny)
    x_coords = np.linspace(x_min, x_max, nx)

    yy, xx = np.meshgrid(y_coords, x_coords, indexing="ij")
    horizontal_dist = np.sqrt(xx**2 + yy**2)

    # Avoid division by zero at radar location
    horizontal_dist = np.maximum(horizontal_dist, 1.0)

    height_above_radar = z_level - radar_altitude

    if earth_curvature:
        # With Earth curvature, we need to solve for elevation angle
        # Using the inverse of the beam height formula (iterative approach)
        ke_re = ke * EARTH_RADIUS

        # Initial guess using flat Earth
        elevation_rad = np.arctan(height_above_radar / horizontal_dist)

        # Iterate to refine (usually converges in 2-3 iterations)
        for _ in range(5):
            slant_range = horizontal_dist / np.maximum(np.cos(elevation_rad), 0.01)
            computed_height = (
                np.sqrt(slant_range**2 + ke_re**2 + 2 * slant_range * ke_re * np.sin(elevation_rad))
                - ke_re
                + radar_altitude
            )
            height_error = z_level - computed_height
            # Adjust elevation angle based on error
            elevation_rad += height_error / (slant_range + 1)
            elevation_rad = np.clip(elevation_rad, -np.pi / 2, np.pi / 2)
    else:
        elevation_rad = np.arctan(height_above_radar / horizontal_dist)

    return np.degrees(elevation_rad)


# """
# Radar products derived from gridded data.

# Includes:
# - Constant Elevation PPI (Plan Position Indicator)
# - Column Maximum (COLMAX)
# """

# import numpy as np
# from typing import Tuple, Optional, Union
# from .geometry import GridGeometry


# def constant_elevation_ppi(
#     grid: np.ndarray,
#     geometry: GridGeometry,
#     elevation_angle: float,
#     radar_altitude: float = 0.0,
#     interpolation: str = 'linear'
# ) -> np.ndarray:
#     """
#     Extract a constant elevation PPI from a 3D gridded field.

#     For each (x, y) point, computes the altitude corresponding to the
#     given elevation angle, then interpolates vertically in the 3D grid.

#     Parameters
#     ----------
#     grid : np.ndarray
#         3D gridded field data, shape (nz, ny, nx)
#     geometry : GridGeometry
#         Grid geometry containing grid_shape and grid_limits
#     elevation_angle : float
#         Elevation angle in degrees
#     radar_altitude : float, optional
#         Radar altitude in meters (default: 0.0)
#     interpolation : str, optional
#         Interpolation method: 'linear' or 'nearest' (default: 'linear')

#     Returns
#     -------
#     np.ndarray
#         2D array of shape (ny, nx) containing the PPI at the given elevation

#     Notes
#     -----
#     The altitude at each (x, y) point is computed as:
#         z = sqrt(x² + y²) * tan(elevation_angle) + radar_altitude

#     This is a simplified formula that ignores earth curvature effects,
#     which is acceptable for typical radar ranges (< 250 km).
#     """
#     nz, ny, nx = geometry.grid_shape

#     # Extract grid limits
#     z_min, z_max = geometry.grid_limits[0]
#     y_min, y_max = geometry.grid_limits[1]
#     x_min, x_max = geometry.grid_limits[2]

#     # Create 2D coordinate arrays
#     y_coords = np.linspace(y_min, y_max, ny, dtype='float32')
#     x_coords = np.linspace(x_min, x_max, nx, dtype='float32')
#     z_coords = np.linspace(z_min, z_max, nz, dtype='float32')

#     # Meshgrid for (y, x)
#     yy, xx = np.meshgrid(y_coords, x_coords, indexing='ij')

#     # Compute horizontal distance from radar (assumed at origin)
#     horizontal_dist = np.sqrt(xx**2 + yy**2)

#     # Compute altitude at each (x, y) for the given elevation angle
#     elevation_rad = np.radians(elevation_angle)
#     target_z = horizontal_dist * np.tan(elevation_rad) + radar_altitude

#     # Initialize output
#     result = np.full((ny, nx), np.nan, dtype='float32')

#     if interpolation == 'nearest':
#         # Find nearest z-level for each point
#         z_step = (z_max - z_min) / (nz - 1) if nz > 1 else 1.0
#         z_indices = np.round((target_z - z_min) / z_step).astype(int)

#         # Clip to valid range
#         z_indices = np.clip(z_indices, 0, nz - 1)

#         # Extract values using advanced indexing
#         y_indices, x_indices = np.meshgrid(np.arange(ny), np.arange(nx), indexing='ij')
#         result = grid[z_indices, y_indices, x_indices]

#     elif interpolation == 'linear':
#         # Linear interpolation between z-levels
#         z_step = (z_max - z_min) / (nz - 1) if nz > 1 else 1.0

#         # Compute fractional z-index
#         z_frac = (target_z - z_min) / z_step
#         z_low = np.floor(z_frac).astype(int)
#         z_high = z_low + 1

#         # Compute interpolation weight
#         weight_high = z_frac - z_low
#         weight_low = 1.0 - weight_high

#         # Handle boundaries
#         valid_interp = (z_low >= 0) & (z_high < nz)
#         below_grid = z_high <= 0
#         above_grid = z_low >= nz - 1

#         # Clip indices for safe indexing
#         z_low_safe = np.clip(z_low, 0, nz - 1)
#         z_high_safe = np.clip(z_high, 0, nz - 1)

#         y_indices, x_indices = np.meshgrid(np.arange(ny), np.arange(nx), indexing='ij')

#         # Get values at lower and upper z-levels
#         val_low = grid[z_low_safe, y_indices, x_indices]
#         val_high = grid[z_high_safe, y_indices, x_indices]

#         # Interpolate
#         result = weight_low * val_low + weight_high * val_high

#         # For points below grid, use lowest level
#         result[below_grid] = grid[0, y_indices[below_grid], x_indices[below_grid]]

#         # For points above grid, use highest level
#         result[above_grid] = grid[nz-1, y_indices[above_grid], x_indices[above_grid]]

#     else:
#         raise ValueError(f"Unknown interpolation method: {interpolation}")

#     return result


# def column_max(
#     grid: np.ndarray,
#     z_min_idx: Optional[int] = None,
#     z_max_idx: Optional[int] = None,
#     z_min_alt: Optional[float] = None,
#     z_max_alt: Optional[float] = None,
#     geometry: Optional[GridGeometry] = None
# ) -> np.ndarray:
#     """
#     Compute column maximum (COLMAX) - the maximum value in each vertical column.

#     Parameters
#     ----------
#     grid : np.ndarray
#         3D gridded field data, shape (nz, ny, nx)
#     z_min_idx : int, optional
#         Minimum z-index to include (inclusive)
#     z_max_idx : int, optional
#         Maximum z-index to include (inclusive)
#     z_min_alt : float, optional
#         Minimum altitude in meters. Requires geometry parameter.
#     z_max_alt : float, optional
#         Maximum altitude in meters. Requires geometry parameter.
#     geometry : GridGeometry, optional
#         Grid geometry, required if using altitude-based limits

#     Returns
#     -------
#     np.ndarray
#         2D array of shape (ny, nx) containing the column maximum values

#     Examples
#     --------
#     >>> # Full column max
#     >>> cmax = column_max(grid)

#     >>> # Column max between z-indices 2 and 6
#     >>> cmax = column_max(grid, z_min_idx=2, z_max_idx=6)

#     >>> # Column max between 1000m and 8000m altitude
#     >>> cmax = column_max(grid, z_min_alt=1000, z_max_alt=8000, geometry=geometry)
#     """
#     nz = grid.shape[0]

#     # Convert altitude limits to indices if provided
#     if z_min_alt is not None or z_max_alt is not None:
#         if geometry is None:
#             raise ValueError("geometry is required when using altitude-based limits")

#         z_min_grid, z_max_grid = geometry.grid_limits[0]
#         z_coords = np.linspace(z_min_grid, z_max_grid, nz)

#         if z_min_alt is not None:
#             z_min_idx = np.searchsorted(z_coords, z_min_alt)
#         if z_max_alt is not None:
#             z_max_idx = np.searchsorted(z_coords, z_max_alt, side='right') - 1

#     # Apply default limits
#     if z_min_idx is None:
#         z_min_idx = 0
#     if z_max_idx is None:
#         z_max_idx = nz - 1

#     # Clip to valid range
#     z_min_idx = max(0, z_min_idx)
#     z_max_idx = min(nz - 1, z_max_idx)

#     # Slice and compute max
#     grid_slice = grid[z_min_idx:z_max_idx + 1, :, :]

#     return np.nanmax(grid_slice, axis=0)


# def column_min(
#     grid: np.ndarray,
#     z_min_idx: Optional[int] = None,
#     z_max_idx: Optional[int] = None,
#     z_min_alt: Optional[float] = None,
#     z_max_alt: Optional[float] = None,
#     geometry: Optional[GridGeometry] = None
# ) -> np.ndarray:
#     """
#     Compute column minimum - the minimum value in each vertical column.

#     Parameters are the same as column_max().

#     Returns
#     -------
#     np.ndarray
#         2D array of shape (ny, nx) containing the column minimum values
#     """
#     nz = grid.shape[0]

#     if z_min_alt is not None or z_max_alt is not None:
#         if geometry is None:
#             raise ValueError("geometry is required when using altitude-based limits")

#         z_min_grid, z_max_grid = geometry.grid_limits[0]
#         z_coords = np.linspace(z_min_grid, z_max_grid, nz)

#         if z_min_alt is not None:
#             z_min_idx = np.searchsorted(z_coords, z_min_alt)
#         if z_max_alt is not None:
#             z_max_idx = np.searchsorted(z_coords, z_max_alt, side='right') - 1

#     if z_min_idx is None:
#         z_min_idx = 0
#     if z_max_idx is None:
#         z_max_idx = nz - 1

#     z_min_idx = max(0, z_min_idx)
#     z_max_idx = min(nz - 1, z_max_idx)

#     grid_slice = grid[z_min_idx:z_max_idx + 1, :, :]

#     return np.nanmin(grid_slice, axis=0)


# def column_mean(
#     grid: np.ndarray,
#     z_min_idx: Optional[int] = None,
#     z_max_idx: Optional[int] = None,
#     z_min_alt: Optional[float] = None,
#     z_max_alt: Optional[float] = None,
#     geometry: Optional[GridGeometry] = None
# ) -> np.ndarray:
#     """
#     Compute column mean - the mean value in each vertical column.

#     Parameters are the same as column_max().

#     Returns
#     -------
#     np.ndarray
#         2D array of shape (ny, nx) containing the column mean values
#     """
#     nz = grid.shape[0]

#     if z_min_alt is not None or z_max_alt is not None:
#         if geometry is None:
#             raise ValueError("geometry is required when using altitude-based limits")

#         z_min_grid, z_max_grid = geometry.grid_limits[0]
#         z_coords = np.linspace(z_min_grid, z_max_grid, nz)

#         if z_min_alt is not None:
#             z_min_idx = np.searchsorted(z_coords, z_min_alt)
#         if z_max_alt is not None:
#             z_max_idx = np.searchsorted(z_coords, z_max_alt, side='right') - 1

#     if z_min_idx is None:
#         z_min_idx = 0
#     if z_max_idx is None:
#         z_max_idx = nz - 1

#     z_min_idx = max(0, z_min_idx)
#     z_max_idx = min(nz - 1, z_max_idx)

#     grid_slice = grid[z_min_idx:z_max_idx + 1, :, :]

#     return np.nanmean(grid_slice, axis=0)


# def get_elevation_from_z_level(
#     z_level: float,
#     geometry: GridGeometry,
#     radar_altitude: float = 0.0
# ) -> np.ndarray:
#     """
#     Compute the elevation angle at each (x, y) point for a given z-level.

#     This is useful for understanding what elevation angles correspond to
#     a given altitude at different ranges.

#     Parameters
#     ----------
#     z_level : float
#         Altitude in meters
#     geometry : GridGeometry
#         Grid geometry
#     radar_altitude : float, optional
#         Radar altitude in meters (default: 0.0)

#     Returns
#     -------
#     np.ndarray
#         2D array of shape (ny, nx) containing elevation angles in degrees
#     """
#     nz, ny, nx = geometry.grid_shape
#     y_min, y_max = geometry.grid_limits[1]
#     x_min, x_max = geometry.grid_limits[2]

#     y_coords = np.linspace(y_min, y_max, ny)
#     x_coords = np.linspace(x_min, x_max, nx)

#     yy, xx = np.meshgrid(y_coords, x_coords, indexing='ij')
#     horizontal_dist = np.sqrt(xx**2 + yy**2)

#     # Avoid division by zero at radar location
#     horizontal_dist = np.maximum(horizontal_dist, 1.0)

#     height_above_radar = z_level - radar_altitude
#     elevation_rad = np.arctan(height_above_radar / horizontal_dist)

#     return np.degrees(elevation_rad)
