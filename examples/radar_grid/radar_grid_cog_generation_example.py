#!/usr/bin/env python
"""
Example: Creating GeoTIFF and COG files from radar_grid products

Demonstrates how to:
1. Generate radar products (CAPPI, PPI, COLMAX)
2. Save them as GeoTIFF files
3. Save them as Cloud-Optimized GeoTIFF (COG) files
4. Apply custom colormaps and value ranges
5. Use Web Mercator projection

IMPORTANT: Before running these examples, update the file paths in each
example function to match your environment, or modify the examples to accept
paths as command-line arguments.

Example paths to update:
- radar_file: Path to your radar NetCDF file
- geometry_file: Path to your precomputed geometry file
"""

import os
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pyart
from radar_grid import (
    GridGeometry,
    apply_geometry,
    column_max,
    constant_altitude_ppi,
    constant_elevation_ppi,
    create_cog,
    create_geotiff,
    get_field_data,
    get_radar_info,
    load_geometry,
    save_product_as_geotiff,
)

# ============================================================================
# CONFIGURATION - Update these paths to match your environment
# ============================================================================
DEFAULT_RADAR_FILE = "data/radares/RMA1/netcdf/RMA1_0315_01_20251208T191648Z.nc"
DEFAULT_GEOMETRY_FILE = "data/geometry/RMA1/RMA1_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz"
DEFAULT_OUTPUT_DIR = "outputs/radar_grid/geotiff_generation"  # Change to your preferred output directory
# ============================================================================


def load_test_data(
    radar_file: Optional[str] = None, geometry_file: Optional[str] = None
) -> Tuple[pyart.core.Radar, GridGeometry, float, float]:
    """
    Helper function to load radar and geometry data.

    Returns (radar, geometry, radar_lat, radar_lon) or (None, None, None, None) on error.
    """
    radar_file = radar_file or DEFAULT_RADAR_FILE
    geometry_file = geometry_file or DEFAULT_GEOMETRY_FILE

    try:
        radar = pyart.io.read(radar_file)
    except Exception as e:
        print(f"  ⚠ Error loading radar file '{radar_file}': {e}")
        print("  Please update DEFAULT_RADAR_FILE in the script or pass a valid path.")
        raise Exception("Radar file loading failed.")

    try:
        geometry = load_geometry(geometry_file)
    except Exception as e:
        print(f"  ⚠ Error loading geometry file '{geometry_file}': {e}")
        print("  Please update DEFAULT_GEOMETRY_FILE in the script or generate geometry first.")
        raise Exception("Geometry file loading failed.")

    info = get_radar_info(radar)
    radar_lat = info["latitude"]
    radar_lon = info["longitude"]

    return radar, geometry, radar_lat, radar_lon


def example_1_basic_cappi_geotiff():
    """
    Example 1: Create a basic CAPPI and save as GeoTIFF
    """
    print("=" * 60)
    print("EXAMPLE 1: Basic CAPPI to GeoTIFF")
    print("=" * 60)

    # Load test data
    radar, geometry, radar_lat, radar_lon = load_test_data()
    if radar is None:
        return

    print(f"Radar location: {radar_lat:.4f}°N, {radar_lon:.4f}°E")
    print(f"Grid geometry: {geometry}")

    # Get field data and interpolate
    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)  # type: ignore

    # Generate CAPPI at 3000m altitude
    cappi = constant_altitude_ppi(grid_dbzh, geometry, altitude=3000.0)  # type: ignore
    print(f"CAPPI shape: {cappi.shape}")
    print(f"CAPPI value range: [{np.nanmin(cappi):.2f}, {np.nanmax(cappi):.2f}] dBZ")

    # Save as GeoTIFF (standard, not COG)
    output_file = Path(DEFAULT_OUTPUT_DIR) / "cappi_3km.tif"
    os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
    create_geotiff(
        cappi,
        geometry,
        radar_lat,
        radar_lon,
        output_file,
        cmap="viridis",
        vmin=-10,
        vmax=70,
        projection="EPSG:3857",  # Web Mercator
    )
    print(f"✓ Saved GeoTIFF: {output_file}")
    print()


def example_2_cappi_as_cog():
    """
    Example 2: Create CAPPI as Cloud-Optimized GeoTIFF with overviews
    """
    print("=" * 60)
    print("EXAMPLE 2: CAPPI as Cloud-Optimized GeoTIFF")
    print("=" * 60)

    # Load test data
    radar, geometry, radar_lat, radar_lon = load_test_data()
    if radar is None:
        return

    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    # Generate CAPPI
    cappi = constant_altitude_ppi(grid_dbzh, geometry, altitude=1500.0)

    # Save as COG with overviews
    output_file = Path(DEFAULT_OUTPUT_DIR) / "cappi_1.5km.cog"
    create_cog(
        cappi,
        geometry,
        radar_lat,
        radar_lon,
        output_file,
        cmap="viridis",
        vmin=-10,
        vmax=70,
        projection="EPSG:3857",
        overview_factors=[2, 4, 8, 16],  # 4 pyramid levels
        resampling_method="nearest",
    )
    print(f"✓ Saved COG with overviews: {output_file}")
    print()


def example_3_ppi_geotiff():
    """
    Example 3: Create PPI (constant elevation) and save as GeoTIFF
    """
    print("=" * 60)
    print("EXAMPLE 3: PPI to GeoTIFF")
    print("=" * 60)

    # Load test data
    radar, geometry, radar_lat, radar_lon = load_test_data()
    if radar is None:
        return

    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    # Generate PPI at 2.0° elevation
    ppi = constant_elevation_ppi(grid_dbzh, geometry, elevation_angle=1.0)
    print(f"PPI shape: {ppi.shape}")
    print(f"PPI value range: [{np.nanmin(ppi):.2f}, {np.nanmax(ppi):.2f}] dBZ")

    # Save as COG using convenience function
    output_file = Path(DEFAULT_OUTPUT_DIR) / "ppi_1deg.cog"
    save_product_as_geotiff(
        ppi,
        geometry,
        radar_lat,
        radar_lon,
        output_file,
        product_type="PPI",
        cmap="viridis",
        vmin=-10,
        vmax=70,
        as_cog=True,
        overview_factors=[2, 4, 8],
        resampling_method="average",  # Better for intensity data
    )
    print(f"✓ Saved PPI as COG: {output_file}")
    print()


def example_4_colmax_geotiff():
    """
    Example 4: Create COLMAX (column maximum) and save as GeoTIFF
    """
    print("=" * 60)
    print("EXAMPLE 4: COLMAX to GeoTIFF")
    print("=" * 60)

    # Load test data
    radar, geometry, radar_lat, radar_lon = load_test_data()
    if radar is None:
        return

    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    # Generate COLMAX (full column)
    colmax = column_max(grid_dbzh)
    print(f"COLMAX shape: {colmax.shape}")
    print(f"COLMAX value range: [{np.nanmin(colmax):.2f}, {np.nanmax(colmax):.2f}] dBZ")

    # Save as COG with custom colormap
    output_file = Path(DEFAULT_OUTPUT_DIR) / "colmax.cog"
    save_product_as_geotiff(
        colmax,
        geometry,
        radar_lat,
        radar_lon,
        output_file,
        product_type="COLMAX",
        cmap="jet",  # You could use 'pyart_NWSRef' if available
        vmin=0,
        vmax=70,
        projection="EPSG:3857",
        as_cog=True,
        overview_factors=[2, 4, 8, 16],
        resampling_method="cubic_spline",  # Preserve max values in overviews
    )
    print(f"✓ Saved COLMAX as COG: {output_file}")
    print()


def example_5_wgs84_projection():
    """
    Example 5: Save product in WGS84 instead of Web Mercator
    """
    print("=" * 60)
    print("EXAMPLE 5: GeoTIFF with WGS84 projection")
    print("=" * 60)

    # Load test data
    radar, geometry, radar_lat, radar_lon = load_test_data()
    if radar is None:
        return

    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    cappi = constant_altitude_ppi(grid_dbzh, geometry, altitude=1500.0)

    # Save with WGS84 projection (EPSG:4326)
    output_file = Path(DEFAULT_OUTPUT_DIR) / "cappi_1.5km_wgs84.cog"
    save_product_as_geotiff(
        cappi,
        geometry,
        radar_lat,
        radar_lon,
        output_file,
        product_type="CAPPI",
        cmap="viridis",
        vmin=-10,
        vmax=70,
        projection="EPSG:4326",  # WGS84
        as_cog=True,
    )
    print(f"✓ Saved with WGS84 projection: {output_file}")
    print()


def example_6_multiple_altitudes():
    """
    Example 6: Generate CAPPIs at multiple altitudes
    """
    print("=" * 60)
    print("EXAMPLE 6: Multiple CAPPI altitudes")
    print("=" * 60)

    # Load test data
    radar, geometry, radar_lat, radar_lon = load_test_data()
    if radar is None:
        return

    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    # Generate CAPPIs at different altitudes
    altitudes = [1000, 2000, 3000, 5000, 8000]  # meters

    for altitude in altitudes:
        cappi = constant_altitude_ppi(grid_dbzh, geometry, altitude=altitude)

        if np.all(np.isnan(cappi)):
            print(f"  Skipping {altitude}m: all NaN")
            continue

        output_file = Path(DEFAULT_OUTPUT_DIR) / f"cappi_{altitude}m.cog"
        save_product_as_geotiff(
            cappi,
            geometry,
            radar_lat,
            radar_lon,
            output_file,
            product_type=f"CAPPI_{altitude}m",
            cmap="viridis",
            vmin=-10,
            vmax=70,
            as_cog=True,
            overview_factors=[2, 4],  # Fewer overviews for speed
            resampling_method="nearest",
        )
        print(f"  ✓ Saved: {output_file}")

    print()


def main():
    """Run all examples"""
    print()
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + "  Radar Grid GeoTIFF Generation Examples".center(58) + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "=" * 58 + "╝")
    print()

    print("NOTE: These examples require valid radar data and geometry files.")
    print("      Uncomment the examples you want to run.")
    print()

    # Uncomment to run examples:
    # example_1_basic_cappi_geotiff()
    # example_2_cappi_as_cog()
    # example_3_ppi_geotiff()
    # example_4_colmax_geotiff()
    example_5_wgs84_projection()
    # example_6_multiple_altitudes()

    print("=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
