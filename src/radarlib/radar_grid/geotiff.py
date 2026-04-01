"""
GeoTIFF generation for radar grid products.

This module provides functions to convert 2D radar products (CAPPI, PPI, COLMAX)
into georeferenced GeoTIFF images with optional colormap application and
Cloud-Optimized GeoTIFF (COG) format.

It also provides utilities to re-render existing raw float COGs with new colormaps
without needing to reprocess the original radar data, and to upgrade legacy RGBA
COG files (created before the raw float format was introduced) into the raw float
format so they too can benefit from dynamic colormap changes.
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

# Metadata tag keys written into COG/GeoTIFF files by this library
_TAG_CMAP = "radarlib_cmap"
_TAG_VMIN = "radarlib_vmin"
_TAG_VMAX = "radarlib_vmax"
_TAG_NODATA = "radarlib_nodata"
_TAG_DATA_TYPE = "radarlib_data_type"
_DATA_TYPE_RGBA = "rgba"
_DATA_TYPE_RAW = "raw_float"


def _get_cmap_name(cmap: Union[str, matplotlib.colors.Colormap]) -> str:
    """Return the string name of a colormap object or pass-through a string."""
    if isinstance(cmap, str):
        return cmap
    return getattr(cmap, "name", repr(cmap))


def _resolve_vmin_vmax(
    data: np.ndarray,
    vmin: Optional[float],
    vmax: Optional[float],
    nodata_value: Optional[float],
) -> tuple:
    """
    Resolve vmin/vmax, falling back to data extremes when None is supplied.

    Returns
    -------
    tuple
        (resolved_vmin, resolved_vmax) as Python floats
    """
    if nodata_value is not None:
        valid = data[data != nodata_value]
    else:
        valid = data[~np.isnan(data)]

    if len(valid) > 0:
        resolved_vmin = float(vmin) if vmin is not None else float(np.nanmin(valid))
        resolved_vmax = float(vmax) if vmax is not None else float(np.nanmax(valid))
    else:
        resolved_vmin = float(vmin) if vmin is not None else 0.0
        resolved_vmax = float(vmax) if vmax is not None else 1.0
    return resolved_vmin, resolved_vmax


def _compute_crs_bounds(
    geometry: GridGeometry,
    radar_lat: float,
    radar_lon: float,
    projection: str,
) -> tuple:
    """
    Convert radar-relative Cartesian grid limits to bounds in the target CRS.

    Returns
    -------
    tuple
        (west_proj, south_proj, east_proj, north_proj, crs)
        where the first four values are in the target projection units.
    """
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    local_proj = pyproj.Proj(proj="aeqd", lat_0=radar_lat, lon_0=radar_lon, x_0=0, y_0=0, datum="WGS84")
    wgs84_proj = pyproj.CRS("EPSG:4326")
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
        crs = target_proj
    else:
        west_proj, south_proj, east_proj, north_proj = west, south, east, north
        crs = pyproj.CRS("EPSG:4326")

    return west_proj, south_proj, east_proj, north_proj, crs


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

    # Resolve vmin/vmax so we can store them in the file metadata
    actual_vmin, actual_vmax = _resolve_vmin_vmax(data, vmin, vmax, nodata_value)

    # Apply colormap to get RGBA image
    rgba_image = apply_colormap_to_array(data, cmap, actual_vmin, actual_vmax, nodata_value)

    # Flip the data vertically because rasterio stores images top-to-bottom,
    # while our grid coordinates increase from bottom to top (south to north).
    # Without this flip, the image appears upside down.
    rgba_image = np.flipud(rgba_image)

    west_proj, south_proj, east_proj, north_proj, crs = _compute_crs_bounds(geometry, radar_lat, radar_lon, projection)
    transform = from_bounds(west_proj, south_proj, east_proj, north_proj, nx, ny)

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

        # Store colormap metadata so the file can be re-rendered later
        dst.update_tags(
            **{
                _TAG_CMAP: _get_cmap_name(cmap),
                _TAG_VMIN: str(actual_vmin),
                _TAG_VMAX: str(actual_vmax),
                _TAG_NODATA: str(nodata_value) if nodata_value is not None else "",
                _TAG_DATA_TYPE: _DATA_TYPE_RGBA,
            }
        )

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

    # Resolve vmin/vmax so we can store them in the file metadata
    actual_vmin, actual_vmax = _resolve_vmin_vmax(data, vmin, vmax, nodata_value)

    # Apply colormap to get RGBA image
    rgba_image = apply_colormap_to_array(data, cmap, actual_vmin, actual_vmax, nodata_value)

    # Flip the data vertically because rasterio stores images top-to-bottom,
    # while our grid coordinates increase from bottom to top (south to north).
    # Without this flip, the image appears upside down.
    rgba_image = np.flipud(rgba_image)

    west_proj, south_proj, east_proj, north_proj, crs = _compute_crs_bounds(geometry, radar_lat, radar_lon, projection)
    transform = from_bounds(west_proj, south_proj, east_proj, north_proj, nx, ny)

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

        # Store colormap metadata so the file can be re-rendered later
        dst.update_tags(
            **{
                _TAG_CMAP: _get_cmap_name(cmap),
                _TAG_VMIN: str(actual_vmin),
                _TAG_VMAX: str(actual_vmax),
                _TAG_NODATA: str(nodata_value) if nodata_value is not None else "",
                _TAG_DATA_TYPE: _DATA_TYPE_RGBA,
            }
        )

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


def create_raw_cog(
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
    Create a Cloud-Optimized GeoTIFF (COG) that stores the raw float32 data values.

    Unlike :func:`create_cog`, which converts data to RGBA immediately, this function
    preserves the original floating-point values so the file can later be re-rendered
    with any colormap via :func:`remap_cog_colormap` or :func:`read_cog_tile_as_rgba`.

    The colormap, vmin, and vmax are stored as file metadata and used as defaults when
    rendering, but can always be overridden at render time.

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
        Default colormap stored as metadata (default: 'viridis')
    vmin : float, optional
        Default minimum value for colormap scaling stored as metadata.
        If None, the data minimum is computed and stored.
    vmax : float, optional
        Default maximum value for colormap scaling stored as metadata.
        If None, the data maximum is computed and stored.
    projection : str, optional
        Target projection as EPSG code (default: 'EPSG:3857' - Web Mercator)
    nodata_value : float, optional
        Value to treat as no-data (default: None, NaN values become nodata)
    overview_factors : list of int, optional
        Downsampling factors for overview levels (default: [2, 4, 8, 16])
    resampling_method : str, optional
        Resampling method for overviews (default: 'nearest')

    Returns
    -------
    Path
        Path to the created raw float COG file

    Examples
    --------
    >>> colmax = column_max(grid)
    >>> raw_path = create_raw_cog(
    ...     colmax, geometry, radar_lat=40.5, radar_lon=-105.2,
    ...     output_path="colmax_raw.cog",
    ...     cmap="viridis", vmin=0, vmax=70,
    ... )
    >>> # Later, re-render with a different colormap
    >>> remap_cog_colormap(raw_path, "colmax_hot.cog", new_cmap="hot")
    """
    output_path = Path(output_path)

    if overview_factors is None:
        overview_factors = [2, 4, 8, 16]

    if not isinstance(overview_factors, list):
        raise TypeError(f"overview_factors must be a list, got {type(overview_factors).__name__}")

    ny, nx = data.shape
    _, geom_ny, geom_nx = geometry.grid_shape
    if ny != geom_ny or nx != geom_nx:
        raise ValueError(f"Data shape {data.shape} does not match geometry grid shape ({geom_ny}, {geom_nx})")

    # Resolve vmin/vmax so they can be stored as metadata defaults
    actual_vmin, actual_vmax = _resolve_vmin_vmax(data, vmin, vmax, nodata_value)

    # Flip vertically: rasterio top-to-bottom vs. grid south-to-north
    data_flipped = np.flipud(data.astype(np.float32))

    west_proj, south_proj, east_proj, north_proj, crs = _compute_crs_bounds(geometry, radar_lat, radar_lon, projection)
    transform = from_bounds(west_proj, south_proj, east_proj, north_proj, nx, ny)

    resampling_enum = _string_to_resampling(resampling_method)

    # Determine rasterio nodata value
    rio_nodata = nodata_value if nodata_value is not None else float("nan")

    with rasterio.open(
        output_path,
        "w",
        driver="COG",
        height=ny,
        width=nx,
        count=1,
        dtype=np.float32,
        crs=crs,
        transform=transform,
        nodata=rio_nodata,
        compress="DEFLATE",
        BIGTIFF="IF_NEEDED",
        tiled=True,
    ) as dst:
        dst.write(data_flipped, 1)

        dst.update_tags(
            **{
                _TAG_CMAP: _get_cmap_name(cmap),
                _TAG_VMIN: str(actual_vmin),
                _TAG_VMAX: str(actual_vmax),
                _TAG_NODATA: str(nodata_value) if nodata_value is not None else "",
                _TAG_DATA_TYPE: _DATA_TYPE_RAW,
            }
        )

        if overview_factors:
            dst.build_overviews(overview_factors, resampling_enum)
            dst.update_tags(ns="rio_overview", resampling=resampling_method)

    logger.debug("Raw float COG written to %s", output_path)
    return output_path


def read_cog_metadata(cog_path: Union[str, Path]) -> dict:
    """
    Read the radarlib metadata tags from a COG created by this library.

    Parameters
    ----------
    cog_path : str or Path
        Path to a COG file created by :func:`create_cog`, :func:`create_raw_cog`,
        or :func:`remap_cog_colormap`.

    Returns
    -------
    dict
        Dictionary with keys ``cmap``, ``vmin``, ``vmax``, ``nodata``, and
        ``data_type``.  ``vmin`` / ``vmax`` are returned as ``float`` when
        present, otherwise ``None``.  ``data_type`` is either ``"rgba"`` or
        ``"raw_float"`` (or ``None`` for files not written by radarlib).

    Examples
    --------
    >>> meta = read_cog_metadata("colmax_raw.cog")
    >>> print(meta["cmap"], meta["vmin"], meta["vmax"])
    viridis 0.0 70.0
    """
    cog_path = Path(cog_path)
    with rasterio.open(cog_path) as src:
        tags = src.tags()

    def _parse_float(val: str) -> Optional[float]:
        if not val:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    return {
        "cmap": tags.get(_TAG_CMAP) or None,
        "vmin": _parse_float(tags.get(_TAG_VMIN, "")),
        "vmax": _parse_float(tags.get(_TAG_VMAX, "")),
        "nodata": _parse_float(tags.get(_TAG_NODATA, "")),
        "data_type": tags.get(_TAG_DATA_TYPE) or None,
    }


def remap_cog_colormap(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    new_cmap: Union[str, matplotlib.colors.Colormap],
    new_vmin: Optional[float] = None,
    new_vmax: Optional[float] = None,
    overview_factors: Optional[list] = None,
    resampling_method: str = "nearest",
) -> Path:
    """
    Re-render a raw float COG with a new colormap and produce a new RGBA COG.

    This is the primary function for **changing the colormap** of an existing
    radar product.  It requires the input file to be a raw float COG created
    by :func:`create_raw_cog` (``data_type = "raw_float"`` in its metadata).

    Parameters
    ----------
    input_path : str or Path
        Path to a raw float COG (created with :func:`create_raw_cog`).
    output_path : str or Path
        Path for the new RGBA COG.
    new_cmap : str or matplotlib.colors.Colormap
        New colormap to apply.
    new_vmin : float, optional
        Minimum value for the new colormap range.  If None, the value stored
        in the source file's metadata is used.
    new_vmax : float, optional
        Maximum value for the new colormap range.  If None, the value stored
        in the source file's metadata is used.
    overview_factors : list of int, optional
        Overview levels for the output COG (default: [2, 4, 8, 16]).
    resampling_method : str, optional
        Resampling method for overviews (default: 'nearest').

    Returns
    -------
    Path
        Path to the new RGBA COG.

    Raises
    ------
    ValueError
        If the input file is not a raw float COG (i.e. already RGBA).

    Examples
    --------
    >>> raw_path = create_raw_cog(colmax, geometry, 40.5, -105.2,
    ...                            "colmax_raw.cog", cmap="viridis",
    ...                            vmin=0, vmax=70)
    >>> remap_cog_colormap(raw_path, "colmax_hot.cog", new_cmap="hot")
    >>> remap_cog_colormap(raw_path, "colmax_jet.cog", new_cmap="jet",
    ...                     new_vmin=10, new_vmax=60)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if overview_factors is None:
        overview_factors = [2, 4, 8, 16]

    if not isinstance(overview_factors, list):
        raise TypeError(f"overview_factors must be a list, got {type(overview_factors).__name__}")

    meta = read_cog_metadata(input_path)

    with rasterio.open(input_path) as src:
        file_band_count = src.count
        file_dtype = src.dtypes[0]

    if _is_rgba_cog(meta, file_band_count, file_dtype):
        raise ValueError(
            f"'{input_path}' is an RGBA COG and cannot be re-rendered directly. "
            "Convert it to a raw float COG first using convert_rgba_cog_to_raw(), "
            "then call remap_cog_colormap() on the converted file."
        )

    # Fall back to metadata defaults when the caller does not supply them
    vmin = new_vmin if new_vmin is not None else meta.get("vmin")
    vmax = new_vmax if new_vmax is not None else meta.get("vmax")
    nodata = meta.get("nodata")

    with rasterio.open(input_path) as src:
        raw_data = src.read(1)
        file_crs = src.crs
        file_transform = src.transform
        ny, nx = raw_data.shape

    # Convert to float64 for processing; restore NaN for nodata pixels
    raw_data = raw_data.astype(np.float64)
    if nodata is not None:
        raw_data[raw_data == nodata] = np.nan

    # Resolve vmin/vmax from actual data if still unknown
    vmin, vmax = _resolve_vmin_vmax(raw_data, vmin, vmax, None)

    rgba_image = apply_colormap_to_array(raw_data, new_cmap, vmin, vmax)

    resampling_enum = _string_to_resampling(resampling_method)

    with rasterio.open(
        output_path,
        "w",
        driver="COG",
        height=ny,
        width=nx,
        count=4,
        dtype=np.uint8,
        crs=file_crs,
        transform=file_transform,
        compress="DEFLATE",
        predictor=2,
        BIGTIFF="IF_NEEDED",
        photometric="RGB",
        tiled=True,
    ) as dst:
        dst.write(rgba_image[:, :, 0], 1)
        dst.write(rgba_image[:, :, 1], 2)
        dst.write(rgba_image[:, :, 2], 3)
        dst.write(rgba_image[:, :, 3], 4)

        dst.colorinterp = (ColorInterp.red, ColorInterp.green, ColorInterp.blue, ColorInterp.alpha)

        dst.update_tags(
            **{
                _TAG_CMAP: _get_cmap_name(new_cmap),
                _TAG_VMIN: str(vmin),
                _TAG_VMAX: str(vmax),
                _TAG_NODATA: str(nodata) if nodata is not None else "",
                _TAG_DATA_TYPE: _DATA_TYPE_RGBA,
            }
        )

        if overview_factors:
            dst.build_overviews(overview_factors, resampling_enum)
            dst.update_tags(ns="rio_overview", resampling=resampling_method)

    logger.debug("Remapped COG written to %s (cmap=%s)", output_path, _get_cmap_name(new_cmap))
    return output_path


def read_cog_tile_as_rgba(
    cog_path: Union[str, Path],
    cmap: Optional[Union[str, matplotlib.colors.Colormap]] = None,
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    overview_level: int = 0,
    window: Optional[rasterio.windows.Window] = None,
) -> np.ndarray:
    """
    Read a tile (or the full image) from a COG and return an RGBA numpy array.

    For **raw float COGs** (created with :func:`create_raw_cog`) the colormap
    is applied on-the-fly.  You can supply any colormap at read time without
    modifying the file.

    For **RGBA COGs** (created with :func:`create_cog` or
    :func:`remap_cog_colormap`) the existing RGBA data is returned directly
    (no colormap is applied unless a ``cmap`` is explicitly provided, in which
    case the caller's intent is respected and the colormap is applied to band 1
    interpreted as raw data — this is mainly useful for RGBA single-band files).

    Parameters
    ----------
    cog_path : str or Path
        Path to a COG file.
    cmap : str or matplotlib.colors.Colormap, optional
        Colormap to apply when rendering raw float data.  If None, the
        colormap stored in the file's metadata is used (defaulting to
        ``'viridis'`` if no metadata is found).
    vmin : float, optional
        Minimum value for colormap scaling.  If None, uses file metadata or
        data minimum.
    vmax : float, optional
        Maximum value for colormap scaling.  If None, uses file metadata or
        data maximum.
    overview_level : int, optional
        Overview (pyramid) level to read.  0 means full resolution.
        1 means first downsampled level, etc. (default: 0)
    window : rasterio.windows.Window, optional
        Spatial window to read.  If None, reads the full extent at the
        requested overview level.

    Returns
    -------
    np.ndarray
        RGBA image array, shape (height, width, 4), dtype uint8.

    Examples
    --------
    >>> # Read the full raw COG with a new colormap
    >>> rgba = read_cog_tile_as_rgba("colmax_raw.cog", cmap="hot")
    >>> from PIL import Image
    >>> Image.fromarray(rgba).save("tile.png")

    >>> # Read a spatial window at overview level 1
    >>> import rasterio
    >>> win = rasterio.windows.Window(col_off=100, row_off=100, width=256, height=256)
    >>> rgba = read_cog_tile_as_rgba("colmax_raw.cog", cmap="jet", overview_level=1,
    ...                               window=win)
    """
    cog_path = Path(cog_path)
    meta = read_cog_metadata(cog_path)
    data_type = meta.get("data_type")

    with rasterio.open(cog_path) as src:
        # Select overview dataset if requested
        if overview_level > 0:
            if overview_level > len(src.overviews(1)):
                raise ValueError(
                    f"overview_level={overview_level} requested but the file only has "
                    f"{len(src.overviews(1))} overview level(s)."
                )
            ovr_dataset = src.overviews(1)[overview_level - 1]
            ovr_transform = src.transform * src.transform.scale(src.width / ovr_dataset, src.height / ovr_dataset)
            _ = ovr_transform  # kept for reference; rasterio handles internally

        if data_type == _DATA_TYPE_RAW or src.count == 1:
            # Single-band float COG: apply colormap
            band_data = src.read(1, out_shape=_overview_shape(src, overview_level), window=window).astype(np.float64)

            # Restore NaN for nodata
            src_nodata = src.nodata
            if src_nodata is not None and not np.isnan(src_nodata):
                band_data[band_data == src_nodata] = np.nan

            # Resolve colormap and value range
            effective_cmap = cmap if cmap is not None else (meta.get("cmap") or "viridis")
            effective_vmin = vmin if vmin is not None else meta.get("vmin")
            effective_vmax = vmax if vmax is not None else meta.get("vmax")

            return apply_colormap_to_array(band_data, effective_cmap, effective_vmin, effective_vmax)

        else:
            # Multi-band (RGBA) COG: return bands directly
            bands = src.read(out_shape=(src.count, *_overview_shape(src, overview_level)), window=window)
            # bands shape: (4, H, W) → transpose to (H, W, 4)
            return np.moveaxis(bands, 0, -1).astype(np.uint8)


def _overview_shape(src: rasterio.DatasetReader, overview_level: int) -> tuple:
    """Return (height, width) for the requested overview level of *src*."""
    if overview_level == 0:
        return (src.height, src.width)
    overviews = src.overviews(1)
    if not overviews or overview_level > len(overviews):
        return (src.height, src.width)
    factor = overviews[overview_level - 1]
    return (max(1, src.height // factor), max(1, src.width // factor))


def _is_rgba_cog(meta: dict, band_count: int, dtype: str) -> bool:
    """
    Return True when a rasterio dataset is an RGBA-encoded COG.

    Checks both the radarlib metadata tag (present on files created by this
    library) and the band count / dtype combination (for legacy files that
    pre-date the metadata tag).
    """
    if meta.get("data_type") == _DATA_TYPE_RGBA:
        return True
    # Heuristic for legacy RGBA files: 4 uint8 bands and NOT tagged as raw
    if band_count == 4 and dtype in ("uint8",) and meta.get("data_type") != _DATA_TYPE_RAW:
        return True
    return False


def _build_colormap_lut(
    cmap: Union[str, matplotlib.colors.Colormap],
    vmin: float,
    vmax: float,
    lut_size: int,
) -> tuple:
    """
    Build a lookup table mapping uint8 RGB tuples to float data values.

    Parameters
    ----------
    cmap : str or Colormap
        Colormap to invert.
    vmin, vmax : float
        Data range used when the colormap was applied.
    lut_size : int
        Number of uniformly spaced sample points across [vmin, vmax].

    Returns
    -------
    lut_float : np.ndarray, shape (lut_size,)
        Float values at each LUT entry.
    lut_rgb : np.ndarray, shape (lut_size, 3), dtype uint8
        Corresponding RGB values (0-255) for each float entry.
    """
    if isinstance(cmap, str):
        cmap = plt.get_cmap(cmap)

    lut_float = np.linspace(vmin, vmax, lut_size)
    norm = Normalize(vmin=vmin, vmax=vmax, clip=True)
    normalized = norm(lut_float)
    # cmap returns float RGBA in [0, 1]; convert RGB to uint8
    lut_rgba_f = cmap(normalized)  # shape (lut_size, 4)
    lut_rgb = (lut_rgba_f[:, :3] * 255).astype(np.uint8)
    return lut_float, lut_rgb


def _invert_colormap_to_float(
    rgba_image: np.ndarray,
    cmap: Union[str, matplotlib.colors.Colormap],
    vmin: float,
    vmax: float,
    lut_size: int = 1024,
    chunk_size: int = 4096,
) -> np.ndarray:
    """
    Approximate the original float values by inverting the colormap mapping.

    Each opaque pixel's RGB values are matched against a dense lookup table
    (LUT) and the nearest entry (in RGB Euclidean distance) is used to
    reconstruct an approximate float value.  Transparent pixels (alpha == 0)
    become ``NaN``.

    .. note::
        The reconstruction is an **approximation**.  Because the colormap was
        applied and the result was quantized to uint8 before writing to disk,
        each pixel can represent at most one of ~256 distinct float values
        within [vmin, vmax].  Increasing ``lut_size`` does not recover
        additional precision beyond the uint8 quantization limit, but it does
        improve coverage for colormaps with non-monotone colour channels.

    Parameters
    ----------
    rgba_image : np.ndarray, shape (H, W, 4), dtype uint8
        RGBA image as read from the COG.
    cmap : str or Colormap
        The colormap that was originally used to create the RGBA data.
    vmin, vmax : float
        Value range that was originally used for normalisation.
    lut_size : int, optional
        Number of sample points in the lookup table (default: 1024).
    chunk_size : int, optional
        Number of pixels to process at once when doing nearest-neighbour
        lookup (controls peak memory use, default: 4096).

    Returns
    -------
    np.ndarray, shape (H, W), dtype float32
        Reconstructed float values; NaN where alpha was 0.
    """
    lut_float, lut_rgb = _build_colormap_lut(cmap, vmin, vmax, lut_size)
    lut_rgb_f = lut_rgb.astype(np.float32)  # (lut_size, 3)

    H, W = rgba_image.shape[:2]
    flat_rgba = rgba_image.reshape(-1, 4)
    flat_rgb = flat_rgba[:, :3].astype(np.float32)
    alpha = flat_rgba[:, 3]

    result_flat = np.full(H * W, np.nan, dtype=np.float32)
    opaque_indices = np.where(alpha > 0)[0]

    if opaque_indices.size == 0:
        return result_flat.reshape(H, W)

    # Process opaque pixels in chunks to limit peak memory usage
    for start in range(0, opaque_indices.size, chunk_size):
        end = min(start + chunk_size, opaque_indices.size)
        chunk_idx = opaque_indices[start:end]
        chunk_rgb = flat_rgb[chunk_idx]  # (chunk, 3)

        # Squared Euclidean distance between each chunk pixel and each LUT entry
        # diff shape: (chunk, lut_size, 3)
        diff = chunk_rgb[:, np.newaxis, :] - lut_rgb_f[np.newaxis, :, :]
        dists = (diff * diff).sum(axis=-1)  # (chunk, lut_size)

        nearest = np.argmin(dists, axis=-1)  # (chunk,)
        result_flat[chunk_idx] = lut_float[nearest].astype(np.float32)

    return result_flat.reshape(H, W)


def convert_rgba_cog_to_raw(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    cmap: Optional[Union[str, matplotlib.colors.Colormap]] = None,
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    lut_size: int = 1024,
    overview_factors: Optional[list] = None,
    resampling_method: str = "nearest",
) -> Path:
    """
    Convert a legacy RGBA COG into a raw float32 COG suitable for colormap changes.

    The original RGBA COG stores data with the colormap already baked in as
    uint8 pixel values.  This function **approximately** recovers the original
    floating-point values by performing an inverse colormap lookup: for each
    opaque pixel, the RGB value is matched against a dense lookup table (LUT)
    built from the supplied (or file-embedded) colormap and value range.

    The resulting raw float COG can subsequently be re-rendered with any
    colormap via :func:`remap_cog_colormap` or :func:`read_cog_tile_as_rgba`,
    just like a file originally created by :func:`create_raw_cog`.

    .. warning::
        **Approximate conversion** — because the original data were quantized
        to uint8 when the RGBA COG was written, the reconstructed float values
        have a precision of at most ``(vmax − vmin) / 255``.  The conversion
        cannot recover sub-quantisation detail that was already lost.

    Parameters
    ----------
    input_path : str or Path
        Path to the existing RGBA COG (single or multi-band, uint8).
        May be a legacy file without radarlib metadata, or a newer file
        written by :func:`create_cog`.
    output_path : str or Path
        Destination path for the new raw float32 COG.
    cmap : str or matplotlib.colors.Colormap, optional
        Colormap that was used when the RGBA COG was created.  If ``None``,
        the value stored in the file's ``radarlib_cmap`` metadata tag is used.
        **Required** when the input file has no radarlib metadata.
    vmin : float, optional
        Minimum data value that was used for colormap normalisation.  If
        ``None``, uses the ``radarlib_vmin`` metadata tag.
        **Required** when the input file has no radarlib metadata.
    vmax : float, optional
        Maximum data value that was used for colormap normalisation.  If
        ``None``, uses the ``radarlib_vmax`` metadata tag.
        **Required** when the input file has no radarlib metadata.
    lut_size : int, optional
        Number of uniformly spaced sample points used to build the inverse
        lookup table.  Higher values increase the resolution of the
        reconstructed float values at the cost of slightly more computation
        (default: 1024).
    overview_factors : list of int, optional
        Overview levels written into the output COG (default: [2, 4, 8, 16]).
    resampling_method : str, optional
        Resampling method for overviews (default: 'nearest').

    Returns
    -------
    Path
        Path to the newly created raw float32 COG.

    Raises
    ------
    ValueError
        If the input file is already a raw float COG (no conversion needed).
    ValueError
        If ``cmap``, ``vmin``, or ``vmax`` cannot be determined from the
        input arguments or from the file's embedded metadata.

    Examples
    --------
    Convert a legacy RGBA COG (no metadata) to raw float — you must supply
    the original colormap and value range:

    >>> convert_rgba_cog_to_raw(
    ...     "colmax_legacy.cog", "colmax_raw.cog",
    ...     cmap="viridis", vmin=0, vmax=70,
    ... )

    Convert a newer RGBA COG that already contains radarlib metadata — the
    colormap/vmin/vmax are read automatically from the file:

    >>> convert_rgba_cog_to_raw("colmax_new_rgba.cog", "colmax_raw.cog")

    After conversion, freely re-render with any colormap:

    >>> remap_cog_colormap("colmax_raw.cog", "colmax_hot.cog", new_cmap="hot")
    >>> rgba = read_cog_tile_as_rgba("colmax_raw.cog", cmap="plasma")
    """

    input_path = Path(input_path)
    output_path = Path(output_path)

    if overview_factors is None:
        overview_factors = [2, 4, 8, 16]

    if not isinstance(overview_factors, list):
        raise TypeError(f"overview_factors must be a list, got {type(overview_factors).__name__}")

    meta = read_cog_metadata(input_path)

    # Reject raw float input — nothing to convert
    if meta.get("data_type") == _DATA_TYPE_RAW:
        raise ValueError(
            f"'{input_path}' is already a raw float COG (data_type='raw_float'). " "No conversion is needed."
        )

    # Resolve colormap and value range: prefer caller args, fall back to metadata
    effective_cmap = cmap if cmap is not None else meta.get("cmap")
    effective_vmin = vmin if vmin is not None else meta.get("vmin")
    effective_vmax = vmax if vmax is not None else meta.get("vmax")

    missing = []
    if effective_cmap is None:
        missing.append("cmap")
    if effective_vmin is None:
        missing.append("vmin")
    if effective_vmax is None:
        missing.append("vmax")
    if missing:
        raise ValueError(
            f"Cannot determine {', '.join(missing)} for '{input_path}'. "
            "The file has no radarlib metadata. "
            "Please supply the original colormap and value range explicitly:\n"
            "  convert_rgba_cog_to_raw(input, output, cmap='viridis', vmin=0, vmax=70)"
        )

    effective_vmin = float(effective_vmin)
    effective_vmax = float(effective_vmax)

    with rasterio.open(input_path) as src:
        band_count = src.count
        file_crs = src.crs
        file_transform = src.transform
        ny, nx = src.height, src.width
        nodata_meta = meta.get("nodata")

        # Read RGBA bands; handle both 4-band and (unusual) single-band uint8 files
        if band_count >= 4:
            r = src.read(1)
            g = src.read(2)
            b = src.read(3)
            a = src.read(4)
            rgba_image = np.stack([r, g, b, a], axis=-1).astype(np.uint8)
        elif band_count == 3:
            r = src.read(1)
            g = src.read(2)
            b = src.read(3)
            a = np.full((ny, nx), 255, dtype=np.uint8)
            rgba_image = np.stack([r, g, b, a], axis=-1).astype(np.uint8)
        else:
            raise ValueError(
                f"'{input_path}' has {band_count} band(s); expected an RGBA or RGB COG " "(3 or 4 uint8 bands)."
            )

    logger.debug(
        "Inverting colormap '%s' (vmin=%s, vmax=%s) for %s",
        _get_cmap_name(effective_cmap),
        effective_vmin,
        effective_vmax,
        input_path,
    )

    float_data = _invert_colormap_to_float(
        rgba_image,
        effective_cmap,
        effective_vmin,
        effective_vmax,
        lut_size=lut_size,
    )

    resampling_enum = _string_to_resampling(resampling_method)

    with rasterio.open(
        output_path,
        "w",
        driver="COG",
        height=ny,
        width=nx,
        count=1,
        dtype=np.float32,
        crs=file_crs,
        transform=file_transform,
        nodata=float("nan"),
        compress="DEFLATE",
        BIGTIFF="IF_NEEDED",
        tiled=True,
    ) as dst:
        dst.write(float_data, 1)

        dst.update_tags(
            **{
                _TAG_CMAP: _get_cmap_name(effective_cmap),
                _TAG_VMIN: str(effective_vmin),
                _TAG_VMAX: str(effective_vmax),
                _TAG_NODATA: str(nodata_meta) if nodata_meta is not None else "",
                _TAG_DATA_TYPE: _DATA_TYPE_RAW,
            }
        )

        if overview_factors:
            dst.build_overviews(overview_factors, resampling_enum)
            dst.update_tags(ns="rio_overview", resampling=resampling_method)

    logger.debug("Converted RGBA COG to raw float COG at %s", output_path)
    return output_path
