"""
GeoTIFF generation for radar grid products.

This module provides functions to convert 2D radar products (CAPPI, PPI, COLMAX)
into georeferenced GeoTIFF images with optional colormap application and
Cloud-Optimized GeoTIFF (COG) format.
"""

import logging
from pathlib import Path
from typing import Optional, Union

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pyproj
import rasterio
from matplotlib.colors import Normalize
from rasterio.enums import ColorInterp, Resampling
from rasterio.transform import from_bounds

from .geometry import GridGeometry

logger = logging.getLogger(__name__)


def _string_to_resampling(method: str) -> Resampling:
    """
    Convert resampling method string to rasterio Resampling enum.

    Parameters
    ----------
    method : str
        Resampling method name

    Returns
    -------
    Resampling
        Rasterio resampling enum

    Raises
    ------
    ValueError
        If method is not valid
    """
    method_map = {
        "nearest": Resampling.nearest,
        "bilinear": Resampling.bilinear,
        "cubic": Resampling.cubic,
        "average": Resampling.average,
        "mode": Resampling.mode,
        "gauss": Resampling.gauss,
        "cubic_spline": Resampling.cubic_spline,
        "lanczos": Resampling.lanczos,
        # 'max': Resampling.max,
        # 'min': Resampling.min,
        # 'med': Resampling.med,
        # 'q1': Resampling.q1,
        # 'q3': Resampling.q3,
        "rms": Resampling.rms,
    }

    if method not in method_map:
        valid = ", ".join(method_map.keys())
        raise ValueError(f"Invalid resampling method '{method}'. Valid options: {valid}")

    return method_map[method]


def apply_colormap_to_array(
    data: np.ndarray,
    cmap: Union[str, matplotlib.colors.Colormap],
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    fill_value: Optional[float] = None,
) -> np.ndarray:
    """
    Apply a colormap to a 2D data array, converting to RGBA.

    Parameters
    ----------
    data : np.ndarray
        2D array of data values, shape (ny, nx)
    cmap : str or matplotlib.colors.Colormap
        Colormap to apply. Can be a matplotlib colormap name or object.
    vmin : float, optional
        Minimum value for colormap normalization. If None, uses data minimum.
    vmax : float, optional
        Maximum value for colormap normalization. If None, uses data maximum.
    fill_value : float, optional
        Value to treat as no-data. These pixels will be transparent.
        If None, NaN values are treated as no-data.

    Returns
    -------
    np.ndarray
        RGBA image array, shape (ny, nx, 4), dtype uint8

    Notes
    -----
    The returned array has values in range [0, 255] with:
    - RGB channels contain the colormap values
    - Alpha channel is 0 (transparent) for no-data, 255 otherwise
    """
    # Get colormap
    if isinstance(cmap, str):
        cmap = plt.get_cmap(cmap)

    # Create a copy to avoid modifying original
    data_copy = data.copy()

    # Identify no-data pixels
    if fill_value is not None:
        nodata_mask = data_copy == fill_value
    else:
        nodata_mask = np.isnan(data_copy)

    # Set vmin and vmax if not provided
    valid_data = data_copy[~nodata_mask]
    if len(valid_data) > 0:
        if vmin is None:
            vmin = np.nanmin(valid_data)
        if vmax is None:
            vmax = np.nanmax(valid_data)
    else:
        # All data is no-data
        if vmin is None:
            vmin = 0.0
        if vmax is None:
            vmax = 1.0

    # Normalize data
    norm = Normalize(vmin=vmin, vmax=vmax, clip=True)
    normalized = norm(data_copy)

    # Apply colormap
    rgba = cmap(normalized)

    # Convert to uint8 (0-255 range)
    rgba_uint8 = (rgba * 255).astype(np.uint8)

    # Set alpha channel to 0 for no-data pixels
    rgba_uint8[nodata_mask, 3] = 0

    return rgba_uint8


def create_geotiff(
    data: np.ndarray,
    geometry: GridGeometry,
    radar_lat: float,
    radar_lon: float,
    output_path: Union[str, Path],
    cmap: Union[str, matplotlib.colors.Colormap] = "viridis",
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    projection: str = "EPSG:3857",
    nodata_value: Optional[float] = None,
) -> Path:
    """
    Create a GeoTIFF from a 2D radar product array.

    Parameters
    ----------
    data : np.ndarray
        2D array of radar data, shape (ny, nx)
    geometry : GridGeometry
        Grid geometry containing spatial extent and grid dimensions
    radar_lat : float
        Radar latitude in degrees
    radar_lon : float
        Radar longitude in degrees
    output_path : str or Path
        Output path for the GeoTIFF file
    cmap : str or matplotlib.colors.Colormap, optional
        Colormap to apply (default: 'viridis')
    vmin : float, optional
        Minimum value for colormap scaling (default: data minimum)
    vmax : float, optional
        Maximum value for colormap scaling (default: data maximum)
    projection : str, optional
        Target projection as EPSG code or proj4 string (default: 'EPSG:3857' - Web Mercator)
    nodata_value : float, optional
        Value to treat as no-data (default: None, treats NaN as no-data)

    Returns
    -------
    Path
        Path to the created GeoTIFF file

    Notes
    -----
    The function:
    1. Applies the specified colormap to convert data to RGBA
    2. Transforms coordinates from radar-relative Cartesian to geographic
    3. Optionally projects to Web Mercator or other projection
    4. Writes the georeferenced RGBA image as a GeoTIFF

    Examples
    --------
    >>> # Create a simple GeoTIFF from CAPPI data
    >>> cappi = constant_altitude_ppi(grid, geometry, altitude=3000)
    >>> create_geotiff(cappi, geometry, radar_lat=40.5, radar_lon=-105.2,
    ...                output_path='cappi_3km.tif', cmap='pyart_NWSRef')

    >>> # With custom value range
    >>> create_geotiff(cappi, geometry, radar_lat=40.5, radar_lon=-105.2,
    ...                output_path='cappi_3km.tif', cmap='pyart_NWSRef',
    ...                vmin=-10, vmax=70)
    """
    output_path = Path(output_path)

    # Validate data shape matches geometry
    ny, nx = data.shape
    _, geom_ny, geom_nx = geometry.grid_shape
    if ny != geom_ny or nx != geom_nx:
        raise ValueError(f"Data shape {data.shape} does not match geometry grid shape " f"({geom_ny}, {geom_nx})")

    # Apply colormap to get RGBA image
    rgba_image = apply_colormap_to_array(data, cmap, vmin, vmax, nodata_value)

    # Flip the data vertically because rasterio stores images top-to-bottom,
    # while our grid coordinates increase from bottom to top (south to north).
    # Without this flip, the image appears upside down.
    rgba_image = np.flipud(rgba_image)

    # Get grid limits in meters (relative to radar)
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    # # Create coordinate arrays in meters
    # x_coords_m = np.linspace(x_min, x_max, nx)
    # y_coords_m = np.linspace(y_min, y_max, ny)

    # Convert from radar-relative Cartesian (meters) to geographic coordinates
    # First create a local projection centered on the radar
    # Use Azimuthal Equidistant projection centered on radar
    local_proj = pyproj.Proj(proj="aeqd", lat_0=radar_lat, lon_0=radar_lon, x_0=0, y_0=0, datum="WGS84")
    wgs84_proj = pyproj.CRS("EPSG:4326")

    # Get corner coordinates in geographic space (WGS84)
    transformer_to_wgs84 = pyproj.Transformer.from_proj(local_proj, wgs84_proj, always_xy=True)

    # Transform corners (x_min, y_min), (x_max, y_min), (x_max, y_max), (x_min, y_max)
    corner_lons = []
    corner_lats = []
    for x in [x_min, x_max]:
        for y in [y_min, y_max]:
            lon, lat = transformer_to_wgs84.transform(x, y)
            corner_lons.append(lon)
            corner_lats.append(lat)

    # Get bounds in WGS84
    west = min(corner_lons)
    east = max(corner_lons)
    south = min(corner_lats)
    north = max(corner_lats)

    # Transform to target projection if not WGS84
    target_proj = pyproj.CRS(projection)

    if target_proj.to_epsg() != 4326:  # Not WGS84
        transformer_to_target = pyproj.Transformer.from_crs("EPSG:4326", target_proj, always_xy=True)

        # Transform all corners to get proper bounds in target projection
        target_corners_x = []
        target_corners_y = []
        for lon in [west, east]:
            for lat in [south, north]:
                tx, ty = transformer_to_target.transform(lon, lat)
                target_corners_x.append(tx)
                target_corners_y.append(ty)

        west_proj = min(target_corners_x)
        east_proj = max(target_corners_x)
        south_proj = min(target_corners_y)
        north_proj = max(target_corners_y)

        # Create transform for target projection
        transform = from_bounds(west_proj, south_proj, east_proj, north_proj, nx, ny)
        crs = target_proj
    else:
        # Use WGS84
        transform = from_bounds(west, south, east, north, nx, ny)
        crs = "EPSG:4326"

    # Write GeoTIFF
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=ny,
        width=nx,
        count=4,  # RGBA
        dtype=np.uint8,
        crs=crs,
        transform=transform,
        compress="DEFLATE",
        tiled=True,
    ) as dst:
        # Write each band
        dst.write(rgba_image[:, :, 0], 1)  # Red
        dst.write(rgba_image[:, :, 1], 2)  # Green
        dst.write(rgba_image[:, :, 2], 3)  # Blue
        dst.write(rgba_image[:, :, 3], 4)  # Alpha

        # Set color interpretation
        dst.colorinterp = (ColorInterp.red, ColorInterp.green, ColorInterp.blue, ColorInterp.alpha)

    return output_path


def create_cog(
    data: np.ndarray,
    geometry: GridGeometry,
    radar_lat: float,
    radar_lon: float,
    output_path: Union[str, Path],
    cmap: Union[str, matplotlib.colors.Colormap] = "viridis",
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    projection: str = "EPSG:3857",
    nodata_value: Optional[float] = None,
    overview_factors: Optional[list] = None,
    resampling_method: str = "nearest",
) -> Path:
    """
    Create a Cloud-Optimized GeoTIFF (COG) from a 2D radar product array.

    This function creates a COG with pyramid overviews for efficient multi-scale
    display and tiling in web applications.

    Parameters
    ----------
    data : np.ndarray
        2D array of radar data, shape (ny, nx)
    geometry : GridGeometry
        Grid geometry containing spatial extent and grid dimensions
    radar_lat : float
        Radar latitude in degrees
    radar_lon : float
        Radar longitude in degrees
    output_path : str or Path
        Output path for the COG file
    cmap : str or matplotlib.colors.Colormap, optional
        Colormap to apply (default: 'viridis')
    vmin : float, optional
        Minimum value for colormap scaling (default: data minimum)
    vmax : float, optional
        Maximum value for colormap scaling (default: data maximum)
    projection : str, optional
        Target projection as EPSG code (default: 'EPSG:3857' - Web Mercator)
    nodata_value : float, optional
        Value to treat as no-data (default: None, treats NaN as no-data)
    overview_factors : list of int, optional
        Downsampling factors for overview levels. Default [2, 4, 8, 16] creates
        4 overview levels. Set to [] to disable overviews.
    resampling_method : str, optional
        Resampling method for overviews: 'nearest', 'bilinear', 'cubic',
        'average', 'mode', 'max', 'min', etc. (default: 'nearest')

    Returns
    -------
    Path
        Path to the created COG file

    Notes
    -----
    COG benefits:
    - Tiled structure for efficient partial reads
    - Pyramid overviews for fast multi-scale display
    - HTTP range request support for cloud storage
    - Optimized for web mapping applications

    Examples
    --------
    >>> # Create COG from COLMAX data
    >>> colmax = column_max(grid)
    >>> create_cog(colmax, geometry, radar_lat=40.5, radar_lon=-105.2,
    ...            output_path='colmax.cog', cmap='pyart_NWSRef',
    ...            vmin=0, vmax=70)

    >>> # High-quality COG with custom overviews
    >>> create_cog(colmax, geometry, radar_lat=40.5, radar_lon=-105.2,
    ...            output_path='colmax.cog', cmap='pyart_NWSRef',
    ...            overview_factors=[2, 4, 8, 16, 32],
    ...            resampling_method='average')
    """
    output_path = Path(output_path)

    # Set default overview factors
    if overview_factors is None:
        overview_factors = [2, 4, 8, 16]

    # Validate overview_factors
    if not isinstance(overview_factors, list):
        raise TypeError(f"overview_factors must be a list, got {type(overview_factors).__name__}")

    # Validate data shape matches geometry
    ny, nx = data.shape
    _, geom_ny, geom_nx = geometry.grid_shape
    if ny != geom_ny or nx != geom_nx:
        raise ValueError(f"Data shape {data.shape} does not match geometry grid shape " f"({geom_ny}, {geom_nx})")

    # Apply colormap to get RGBA image
    rgba_image = apply_colormap_to_array(data, cmap, vmin, vmax, nodata_value)

    # Flip the data vertically because rasterio stores images top-to-bottom,
    # while our grid coordinates increase from bottom to top (south to north).
    # Without this flip, the image appears upside down.
    rgba_image = np.flipud(rgba_image)

    # Get grid limits in meters (relative to radar)
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    # Convert from radar-relative Cartesian (meters) to geographic coordinates
    local_proj = pyproj.Proj(proj="aeqd", lat_0=radar_lat, lon_0=radar_lon, x_0=0, y_0=0, datum="WGS84")
    wgs84_proj = pyproj.CRS("EPSG:4326")

    # Transform corners to WGS84
    transformer_to_wgs84 = pyproj.Transformer.from_proj(local_proj, wgs84_proj, always_xy=True)

    corner_lons = []
    corner_lats = []
    for x in [x_min, x_max]:
        for y in [y_min, y_max]:
            lon, lat = transformer_to_wgs84.transform(x, y)
            corner_lons.append(lon)
            corner_lats.append(lat)

    west = min(corner_lons)
    east = max(corner_lons)
    south = min(corner_lats)
    north = max(corner_lats)

    # Transform to target projection
    target_proj = pyproj.CRS(projection)

    if target_proj.to_epsg() != 4326:
        transformer_to_target = pyproj.Transformer.from_crs("EPSG:4326", target_proj, always_xy=True)

        target_corners_x = []
        target_corners_y = []
        for lon in [west, east]:
            for lat in [south, north]:
                tx, ty = transformer_to_target.transform(lon, lat)
                target_corners_x.append(tx)
                target_corners_y.append(ty)

        west_proj = min(target_corners_x)
        east_proj = max(target_corners_x)
        south_proj = min(target_corners_y)
        north_proj = max(target_corners_y)

        transform = from_bounds(west_proj, south_proj, east_proj, north_proj, nx, ny)
        crs = target_proj
    else:
        transform = from_bounds(west, south, east, north, nx, ny)
        crs = "EPSG:4326"

    # Convert resampling method string to enum
    resampling_enum = _string_to_resampling(resampling_method)

    # Write COG with overviews
    with rasterio.open(
        output_path,
        "w",
        driver="COG",
        height=ny,
        width=nx,
        count=4,  # RGBA
        dtype=np.uint8,
        crs=crs,
        transform=transform,
        compress="DEFLATE",
        predictor=2,
        BIGTIFF="IF_NEEDED",
        photometric="RGB",
        tiled=True,
    ) as dst:
        # Write each band
        dst.write(rgba_image[:, :, 0], 1)  # Red
        dst.write(rgba_image[:, :, 1], 2)  # Green
        dst.write(rgba_image[:, :, 2], 3)  # Blue
        dst.write(rgba_image[:, :, 3], 4)  # Alpha

        # Set color interpretation
        dst.colorinterp = (ColorInterp.red, ColorInterp.green, ColorInterp.blue, ColorInterp.alpha)

        # Build overviews
        if overview_factors:
            dst.build_overviews(overview_factors, resampling_enum)
            dst.update_tags(ns="rio_overview", resampling=resampling_method)

    return output_path


def save_product_as_geotiff(
    product_data: np.ndarray,
    geometry: GridGeometry,
    radar_lat: float,
    radar_lon: float,
    output_path: Union[str, Path],
    product_type: str = "CAPPI",
    cmap: Union[str, matplotlib.colors.Colormap] = "viridis",
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    projection: str = "EPSG:3857",
    as_cog: bool = True,
    overview_factors: Optional[list] = None,
    resampling_method: str = "nearest",
) -> Path:
    """
    Convenience function to save any radar product (CAPPI, PPI, COLMAX) as GeoTIFF.

    This is a unified interface that automatically creates either a standard GeoTIFF
    or a Cloud-Optimized GeoTIFF based on the `as_cog` parameter.

    Parameters
    ----------
    product_data : np.ndarray
        2D array of product data (e.g., from constant_altitude_ppi,
        constant_elevation_ppi, or column_max)
    geometry : GridGeometry
        Grid geometry
    radar_lat : float
        Radar latitude in degrees
    radar_lon : float
        Radar longitude in degrees
    output_path : str or Path
        Output file path
    product_type : str, optional
        Product type name for metadata (default: 'CAPPI')
    cmap : str or matplotlib.colors.Colormap, optional
        Colormap to apply (default: 'viridis')
    vmin : float, optional
        Minimum value for colormap scaling
    vmax : float, optional
        Maximum value for colormap scaling
    projection : str, optional
        Target projection (default: 'EPSG:3857' - Web Mercator)
    as_cog : bool, optional
        If True, create COG with overviews. If False, create standard GeoTIFF.
        (default: True)
    overview_factors : list of int, optional
        Overview levels for COG (default: [2, 4, 8, 16])
    resampling_method : str, optional
        Resampling method for COG overviews (default: 'nearest')

    Returns
    -------
    Path
        Path to created file

    Examples
    --------
    >>> # Save CAPPI as COG
    >>> cappi = constant_altitude_ppi(grid, geometry, altitude=3000)
    >>> save_product_as_geotiff(cappi, geometry, 40.5, -105.2,
    ...                          'cappi_3km.cog', product_type='CAPPI',
    ...                          cmap='pyart_NWSRef', vmin=-10, vmax=70)

    >>> # Save PPI as standard GeoTIFF
    >>> ppi = constant_elevation_ppi(grid, geometry, elevation_angle=2.0)
    >>> save_product_as_geotiff(ppi, geometry, 40.5, -105.2,
    ...                          'ppi_2deg.tif', product_type='PPI',
    ...                          as_cog=False)

    >>> # Save COLMAX as COG with custom overviews
    >>> colmax = column_max(grid)
    >>> save_product_as_geotiff(colmax, geometry, 40.5, -105.2,
    ...                          'colmax.cog', product_type='COLMAX',
    ...                          overview_factors=[2, 4, 8],
    ...                          resampling_method='average')
    """
    if as_cog:
        return create_cog(
            product_data,
            geometry,
            radar_lat,
            radar_lon,
            output_path,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            projection=projection,
            overview_factors=overview_factors,
            resampling_method=resampling_method,
        )
    else:
        return create_geotiff(
            product_data,
            geometry,
            radar_lat,
            radar_lon,
            output_path,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            projection=projection,
        )
