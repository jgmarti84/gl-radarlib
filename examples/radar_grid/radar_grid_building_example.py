import logging
import os
import time

import numpy as np
import pyart

# Import our module
from radar_grid import compute_grid_geometry, get_gate_coordinates, get_radar_info, save_geometry

# Configurar logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    print("=" * 60)
    print("RADAR_GRID MODULE - BUILDING GEOMETRY EXAMPLE")
    print("=" * 60)

    # Load radar
    file = "data/radares/RMA1/netcdf/RMA1_0315_02_20251208T191243Z.nc"
    radar = pyart.io.read(file)

    # Print radar info
    print("Radar info:")
    info = get_radar_info(radar)
    for k, v in info.items():
        print(f"  {k}: {v}")

    # Grid configuration
    grid_resolution = 1000
    cap_z = 12000.0
    min_radius = 400.0
    beam_factor = 0.022
    range_max_m = radar.range["data"].max()
    print("Grid configuration:")
    print(f"  Grid resolution: {grid_resolution} m")
    print(f"  Cap Z: {cap_z} m")
    print(f"  Min radius: {min_radius} m")
    print(f"  Beam factor: {beam_factor}")

    # Define grid shape and limits
    z_grid_limits = (0.0, cap_z if cap_z is not None else 15000.0)
    y_grid_limits = (-range_max_m, range_max_m)
    x_grid_limits = (-range_max_m, range_max_m)

    z_points = int(np.ceil(z_grid_limits[1] / grid_resolution)) + 1
    y_points = int((y_grid_limits[1] - y_grid_limits[0]) / grid_resolution)
    x_points = int((x_grid_limits[1] - x_grid_limits[0]) / grid_resolution)

    grid_shape = (z_points, y_points, x_points)
    grid_limits = (z_grid_limits, y_grid_limits, x_grid_limits)

    print("Grid shape and limits:")
    print(f"  Grid shape (z, y, x): {grid_shape}")
    print(f"  Z limits: {grid_limits[0]}")
    print(f"  Y limits: {grid_limits[1]}")
    print(f"  X limits: {grid_limits[2]}")

    # Directories
    # temporary directory for intermediate files, should be removed after processing
    temp_dir = "/workspaces/radarlib/data/temp"
    # this is the directory where final geometry file will be saved
    geometry_dir = "/workspaces/radarlib/data/geometry/RMA1"
    os.makedirs(temp_dir, exist_ok=True)

    # Test 1: Compute geometry
    print("\n--- Computing geometry ---")
    gate_x, gate_y, gate_z = get_gate_coordinates(radar)
    print(f"Gate coordinates: {len(gate_x):,} gates")

    file_name = f"{info['radar_name']}_{info['strategy']}_{info['volume_nr']}"
    file_name += f"_RES{int(grid_resolution)}_TOA{int(cap_z)}"
    file_name += f"_FAC{int(str(beam_factor).replace('0.', '')[:3]):03d}_MR{int(min_radius)}_geometry"

    start = time.time()
    geometry = compute_grid_geometry(
        gate_x,
        gate_y,
        gate_z,
        grid_shape,
        grid_limits,
        temp_dir=temp_dir,
        toa=cap_z,
        min_radius=min_radius,
        radar_altitude=info["altitude"],
        beam_factor=beam_factor,
        n_workers=3,
    )
    print(f"Completed in {time.time() - start:.1f} seconds")
    print(geometry)

    # Test 2: Save and load
    print("\n--- Save/Load geometry ---")
    os.makedirs(geometry_dir, exist_ok=True)
    save_geometry(geometry, f"{geometry_dir}/{file_name}.npz")

    print("\n" + "=" * 60)
    print("FINISHED BUILDING GEOMETRY EXAMPLE!")
    print("=" * 60)


if __name__ == "__main__":
    """
    Example script to build a radar grid with building data.
    """
    main()
