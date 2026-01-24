import time

import numpy as np
import pyart
from radar_grid import (
    apply_geometry,
    apply_geometry_multi,
    get_available_fields,
    get_field_data,
    get_radar_info,
    load_geometry,
)
from radar_grid.filters import GateFilter


def main_nofilters():
    print("=" * 60)
    print("RADAR_GRID MODULE - INTERPOLATION EXAMPLE")
    print("=" * 60)

    # Load radar
    file = "data/radares/RMA1/netcdf/RMA1_0315_01_20251208T191648Z.nc"
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
    geometry_dir = "data/geometry/RMA1"
    geometry = load_geometry(f"{geometry_dir}/RMA1_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz")
    print(geometry)

    # 3. Single field interpolation with timing
    print("\n3. SINGLE FIELD INTERPOLATION")
    print("-" * 40)

    dbzh_data = get_field_data(radar, "DBZH")
    start = time.time()
    # Here is where the actual interpolation happens
    grid_dbzh = apply_geometry(geometry, dbzh_data, additional_filters=None)
    elapsed = time.time() - start
    print(f"   DBZH interpolation: {elapsed:.2f} seconds")
    print(f"   Shape: {grid_dbzh.shape}")
    print(f"   Range: [{np.nanmin(grid_dbzh):.2f}, {np.nanmax(grid_dbzh):.2f}] dBZ")
    print(f"   Valid points: {np.sum(~np.isnan(grid_dbzh)):,} / {grid_dbzh.size:,}")

    # 4. Multi-field interpolation
    print("\n4. MULTI-FIELD INTERPOLATION")
    print("-" * 40)
    fields = {}
    # field_masks = {}
    for name in get_available_fields(radar):
        data = get_field_data(radar, name)
        fields[name] = data

    start = time.time()
    grids = apply_geometry_multi(geometry, fields)
    elapsed = time.time() - start
    print(f"   {len(fields)} fields interpolated in {elapsed:.2f} seconds")
    for name, grid in grids.items():
        print(f"   {name}: [{np.nanmin(grid):.2f}, {np.nanmax(grid):.2f}]")

    print("\n" + "=" * 60)
    print("FINISHED RADAR_GRID INTERPOLATION EXAMPLE!")
    print("=" * 60)


def main_withfilters():
    print("=" * 60)
    print("RADAR_GRID MODULE - INTERPOLATION EXAMPLE WITH FILTERS")
    print("=" * 60)

    # Load radar
    file = "data/radares/RMA1/netcdf/RMA1_0315_01_20251208T191648Z.nc"
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
    geometry_dir = "data/geometry/RMA1"
    geometry = load_geometry(f"{geometry_dir}/RMA1_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz")
    print(geometry)

    # 3. Single field interpolation with timing
    print("\n3. SINGLE FIELD INTERPOLATION WITH FILTERS")
    print("-" * 40)

    dbzh_data = get_field_data(radar, "DBZH")

    gf = GateFilter(radar)
    gf.exclude_below("DBZH", 5.0)
    gf.exclude_below("RHOHV", 0.7)
    print(gf.summary())

    start = time.time()
    # Here is where the actual interpolation happens
    grid_dbzh = apply_geometry(geometry, dbzh_data, additional_filters=[gf])
    elapsed = time.time() - start
    print(f"   DBZH interpolation: {elapsed:.2f} seconds")
    print(f"   Shape: {grid_dbzh.shape}")
    print(f"   Range: [{np.nanmin(grid_dbzh):.2f}, {np.nanmax(grid_dbzh):.2f}] dBZ")
    print(f"   Valid points: {np.sum(~np.isnan(grid_dbzh)):,} / {grid_dbzh.size:,}")

    # 4. Multi-field interpolation
    print("\n4. MULTI-FIELD INTERPOLATION WITH FILTERS")
    print("-" * 40)
    fields = {}
    for name in get_available_fields(radar):
        data = get_field_data(radar, name)
        fields[name] = data

    additional_filters = {}
    additional_filters["DBZH"] = [gf]

    gf_kdp = GateFilter(radar)
    gf_kdp.exclude_below("DBZH", 5)
    gf_kdp.exclude_above("DBZH", 60)
    gf_kdp.exclude_above_range(200000)
    gf_kdp.exclude_below_altitude(500)
    additional_filters["KDP"] = [gf_kdp]

    start = time.time()
    grids = apply_geometry_multi(geometry, fields, additional_filters=additional_filters)
    elapsed = time.time() - start
    print(f"   {len(fields)} fields interpolated in {elapsed:.2f} seconds")
    for name, grid in grids.items():
        print(f"   {name}: [{np.nanmin(grid):.2f}, {np.nanmax(grid):.2f}]")

    print("\n" + "=" * 60)
    print("FINISHED RADAR_GRID INTERPOLATION EXAMPLE WITH FILTERS!")
    print("=" * 60)


if __name__ == "__main__":
    main_nofilters()
    # main_withfilters()
