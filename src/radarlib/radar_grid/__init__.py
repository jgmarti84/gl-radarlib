"""
radar_grid - Fast radar gridding with precomputed geometry
"""

from .compute import compute_grid_geometry
from .filters import GateFilter, GridFilter, create_mask_from_filter
from .geometry import GridGeometry, build_geometry_filename, load_geometry, peek_geometry_metadata, save_geometry
from .geotiff import (
    apply_colormap_to_array,
    create_cog,
    create_geotiff,
    create_raw_cog,
    read_cog_metadata,
    read_cog_tile_as_rgba,
    remap_cog_colormap,
    save_product_as_geotiff,
)
from .interpolate import apply_geometry, apply_geometry_multi
from .mpl_visualization import (
    FIELD_CONFIGS,
    plot_all_fields,
    plot_grid_multi_level,
    plot_grid_slice,
    plot_vertical_cross_section,
)
from .products import (
    EARTH_RADIUS,
    EFFECTIVE_RADIUS_FACTOR,
    column_max,
    column_mean,
    column_min,
    compute_beam_height,
    compute_beam_height_flat,
    constant_altitude_ppi,
    constant_elevation_ppi,
    get_beam_height_difference,
    get_elevation_from_z_level,
)
from .utils import get_available_fields, get_field_data, get_gate_coordinates, get_radar_info

__version__ = "0.1.0"

__all__ = [
    # Core classes
    "GridGeometry",
    # Geometry I/O
    "save_geometry",
    "load_geometry",
    "peek_geometry_metadata",
    "build_geometry_filename",
    # Computation
    "compute_grid_geometry",
    # Interpolation
    "apply_geometry",
    "apply_geometry_multi",
    # Utilities
    "get_gate_coordinates",
    "get_field_data",
    "get_available_fields",
    "get_radar_info",
    # Visualization
    "plot_grid_slice",
    "plot_grid_multi_level",
    "plot_all_fields",
    "plot_vertical_cross_section",
    "FIELD_CONFIGS",
    # Filters
    "GateFilter",
    "GridFilter",
    "create_mask_from_filter",
    # Products
    "constant_altitude_ppi",
    "constant_elevation_ppi",
    "column_max",
    "column_min",
    "column_mean",
    "get_elevation_from_z_level",
    "get_beam_height_difference",
    "compute_beam_height",
    "compute_beam_height_flat",
    "EARTH_RADIUS",
    "EFFECTIVE_RADIUS_FACTOR",
    # GeoTIFF generation
    "create_geotiff",
    "create_cog",
    "create_raw_cog",
    "save_product_as_geotiff",
    "apply_colormap_to_array",
    # Colormap modification
    "remap_cog_colormap",
    "read_cog_metadata",
    "read_cog_tile_as_rgba",
]
