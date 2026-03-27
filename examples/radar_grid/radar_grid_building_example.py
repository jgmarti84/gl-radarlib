import logging
import os
import time

import pyart

# Import our module
from radarlib.radar_grid import (
    build_geometry_filename,
    compute_grid_geometry,
    get_gate_coordinates,
    get_radar_info,
    load_geometry,
    peek_geometry_metadata,
    save_geometry,
)
from radarlib.radar_grid.utils import calculate_grid_points, safe_range_max_m

# Configurar logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    print("=" * 60)
    print("RADAR_GRID MODULE - BUILDING GEOMETRY EXAMPLE")
    print("=" * 60)

    # Load radar
    file = "./app/data/radares/RMA1/netcdf/RMA1_0315_01_20260318T141818Z.nc"
    # file = "data/radares/RMA1/netcdf/RMA1_0315_02_20251208T191243Z.nc"
    radar = pyart.io.read(file)

    # Print radar info
    print("Radar info:")
    info = get_radar_info(radar)
    for k, v in info.items():
        print(f"  {k}: {v}")

    # Grid configuration
    grid_resolution_xy = 1000
    grid_resolution_z = 600
    cap_z = 15000.0
    min_radius = 900.0
    beam_factor = 1.1
    nb = 1.3
    bsp = 1.4
    weighting = "nearest"
    # range_max_m = radar.range["data"].max()
    # Calcular límites Z según producto ANTES de prepare_radar_for_product
    range_max_m = safe_range_max_m(radar, round_to_km=20)

    print("Grid configuration:")
    print(f"  Grid resolution xy: {grid_resolution_xy} m")
    print(f"  Grid resolution z: {grid_resolution_z} m")
    print(f"  Cap Z: {cap_z} m")
    print(f"  Min radius: {min_radius} m")
    print(f"  Beam factor: {beam_factor}")

    # Define grid shape and limits
    z_grid_limits = (0.0, cap_z if cap_z is not None else 15000.0)
    y_grid_limits = (-range_max_m, range_max_m)
    x_grid_limits = (-range_max_m, range_max_m)

    # z_points = int(np.ceil(z_grid_limits[1] / grid_resolution_z)) + 1
    # y_points = int((y_grid_limits[1] - y_grid_limits[0]) / grid_resolution_xy)
    # x_points = int((x_grid_limits[1] - x_grid_limits[0]) / grid_resolution_xy)
    z_points, y_points, x_points = calculate_grid_points(
        z_grid_limits, y_grid_limits, x_grid_limits, grid_resolution_xy, grid_resolution_z
    )

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
        h_factor=beam_factor,
        nb=nb,
        bsp=bsp,
        weighting=weighting,
        max_neighbors=1,
        n_workers=8,
    )
    print(f"Completed in {time.time() - start:.1f} seconds")

    # Attach build parameters as metadata so the file is self-describing
    geometry.metadata = {
        "radar_name": info["radar_name"],
        "strategy": info["strategy"],
        "volume_nr": info["volume_nr"],
        "grid_resolution_xy": grid_resolution_xy,
        "grid_resolution_z": grid_resolution_z,
        "toa": cap_z,
        "h_factor": beam_factor,
        "min_radius": min_radius,
        "max_neighbors": 1,
        "nb": nb,
        "bsp": bsp,
        "weighting": weighting,
    }
    print(geometry)

    # Test 2: Save and load
    print("\n--- Save/Load geometry ---")
    # Derive the canonical filename from the build parameters
    file_name = build_geometry_filename(geometry.metadata)
    os.makedirs(geometry_dir, exist_ok=True)
    save_path = f"{geometry_dir}/{file_name}.npz"
    save_geometry(geometry, save_path)
    print(f"Saved to: {save_path}")

    # Peek at parameters WITHOUT loading the heavy arrays
    print("\n--- Peeking at metadata before full load ---")
    meta = peek_geometry_metadata(save_path)
    for k, v in meta.items():
        print(f"  {k}: {v}")

    # Now do the full load
    print("\n--- Loading full geometry ---")
    loaded = load_geometry(save_path)
    print(loaded)

    print("\n" + "=" * 60)
    print("FINISHED BUILDING GEOMETRY EXAMPLE!")
    print("=" * 60)


if __name__ == "__main__":
    """
    Example script to build a radar grid with building data.
    """
    main()
