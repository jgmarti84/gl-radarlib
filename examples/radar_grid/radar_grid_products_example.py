import numpy as np
import pyart

from radarlib.radar_grid import (
    apply_geometry,
    column_max,
    constant_altitude_ppi,
    constant_elevation_ppi,
    get_field_data,
    get_radar_info,
    load_geometry,
)

# ============================================================================
# CONFIGURATION - Update these paths to match your environment
# ============================================================================
DEFAULT_RADAR_FILE = "./app/data/radares/RMA1/netcdf/RMA1_0315_01_20260318T141818Z.nc"
geometry_file = "RMA1_0315_01_RES1000x600_TOA15000_HF1p1000_MR900_MN1_NB1p30_BSP1p40_nearest_geometry.npz"
DEFAULT_GEOMETRY_FILE = f"/workspaces/radarlib/data/geometry/RMA1/{geometry_file}"
DEFAULT_OUTPUT_DIR = "outputs/radar_grid/geotiff_generation"  # Change to your preferred output directory
# ============================================================================


def main_cappi():
    print("=" * 60)
    print("RADAR_GRID MODULE - CAPPI GENERATION EXAMPLE")
    print("=" * 60)

    # Load radar
    file = DEFAULT_RADAR_FILE
    radar = pyart.io.read(file)

    # 1. Radar info
    print("\n1. RADAR INFO")
    print("-" * 40)
    info = get_radar_info(radar)
    for k, v in info.items():
        print(f"   {k}: {v}")

    # 2. Load pre-computed geometry
    print("\n2. LOAD GEOMETRY")
    print("-" * 40)
    geometry = load_geometry(DEFAULT_GEOMETRY_FILE)
    print(geometry)

    # Interpolate DBZH
    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    print(f"3D Grid shape: {grid_dbzh.shape}")
    print(f"3D Grid DBZH range: [{np.nanmin(grid_dbzh):.2f}, {np.nanmax(grid_dbzh):.2f}]")

    # --- Test Constant Altitude PPI ---
    print("\n=== Constant Altitude PPI (CAPPI) ===")
    for altitude in [1345.0, 2000.0, 3000.0]:  # meters
        print(f"Generating CAPPI at {altitude} m")
        cappi = constant_altitude_ppi(grid_dbzh, geometry, altitude=altitude, interpolation="linear")
        valid = np.sum(~np.isnan(cappi))
        message = f"  Altitude {altitude} m: shape={cappi.shape}, valid={valid:,}, range"
        message += f"=[{np.nanmin(cappi):.2f}, {np.nanmax(cappi):.2f}]"
        print(message)

    print("=" * 60)
    print("FINISHED RADAR_GRID CAPPI GENERATION EXAMPLE!")
    print("=" * 60)


def main_ppi():
    print("=" * 60)
    print("RADAR_GRID MODULE - PPI GENERATION EXAMPLE")
    print("=" * 60)

    # Load radar
    file = DEFAULT_RADAR_FILE
    radar = pyart.io.read(file)

    # 1. Radar info
    print("\n1. RADAR INFO")
    print("-" * 40)
    info = get_radar_info(radar)
    for k, v in info.items():
        print(f"   {k}: {v}")

    # 2. Load pre-computed geometry
    print("\n2. LOAD GEOMETRY")
    print("-" * 40)
    geometry = load_geometry(DEFAULT_GEOMETRY_FILE)
    print(geometry)

    # Interpolate DBZH
    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    print(f"3D Grid shape: {grid_dbzh.shape}")
    print(f"3D Grid DBZH range: [{np.nanmin(grid_dbzh):.2f}, {np.nanmax(grid_dbzh):.2f}]")

    # --- Test Constant Elevation PPI ---
    print("\n=== Constant Elevation PPI (CAPPI) ===")
    for elev in [0.6, 1.0, 2.0, 3.0, 5.0]:
        ppi = constant_elevation_ppi(grid_dbzh, geometry, elevation_angle=elev, interpolation="linear")
        valid = np.sum(~np.isnan(ppi))
        print(
            f"  Elevation {elev}°: shape={ppi.shape}, valid={valid:,}, "
            f"range=[{np.nanmin(ppi):.2f}, {np.nanmax(ppi):.2f}]"
        )

    print("=" * 60)
    print("FINISHED RADAR_GRID PPI GENERATION EXAMPLE!")
    print("=" * 60)


def main_colmax():
    print("=" * 60)
    print("RADAR_GRID MODULE - COLMAX GENERATION EXAMPLE")
    print("=" * 60)

    # Load radar
    file = DEFAULT_RADAR_FILE
    radar = pyart.io.read(file)

    # 1. Radar info
    print("\n1. RADAR INFO")
    print("-" * 40)
    info = get_radar_info(radar)
    for k, v in info.items():
        print(f"   {k}: {v}")

    # 2. Load pre-computed geometry
    print("\n2. LOAD GEOMETRY")
    print("-" * 40)
    geometry = load_geometry(DEFAULT_GEOMETRY_FILE)
    print(geometry)

    # Interpolate DBZH
    dbzh_data = get_field_data(radar, "DBZH")
    grid_dbzh = apply_geometry(geometry, dbzh_data)

    # --- Test COLMAX ---
    print("\n=== Column Maximum (COLMAX) ===")

    # Full column max
    cmax_full = column_max(grid_dbzh)
    print(f"Full COLMAX: shape={cmax_full.shape}, range=[{np.nanmin(cmax_full):.2f}, {np.nanmax(cmax_full):.2f}]")

    # Column max with altitude limits
    cmax_limited = column_max(grid_dbzh, z_min_alt=1000, z_max_alt=8000, geometry=geometry)
    message = f"COLMAX 1-8km: shape={cmax_limited.shape}, "
    message += f"range=[{np.nanmin(cmax_limited):.2f}, {np.nanmax(cmax_limited):.2f}]"
    print(message)

    print("=" * 60)
    print("FINISHED RADAR_GRID COLMAX GENERATION EXAMPLE!")
    print("=" * 60)


if __name__ == "__main__":
    main_cappi()
    main_ppi()
    main_colmax()
