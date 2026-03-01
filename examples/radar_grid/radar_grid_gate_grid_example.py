import os
from pathlib import Path

import numpy as np
import pyart

from radarlib.radar_grid import get_gate_coordinates


def main_gate_coords():
    netcdf_file = "data/radares/RMA1/netcdf/RMA1_0315_02_20251208T191243Z.nc"
    radar = pyart.io.read(netcdf_file)

    gate_coords = get_gate_coordinates(radar)

    netcdf_path = Path(netcdf_file)
    gate_coords_name = "_".join(netcdf_path.stem.split("_")[:-1])
    gate_coords_fname = f"{gate_coords_name}_gate_coordinates.npz"
    gate_coords_dir = "data/gate_coordinates/"
    os.makedirs(gate_coords_dir, exist_ok=True)
    gate_coords_path = gate_coords_dir + gate_coords_fname
    np.savez_compressed(gate_coords_path, gate_x=gate_coords[0], gate_y=gate_coords[1], gate_z=gate_coords[2])


if __name__ == "__main__":
    main_gate_coords()
    # main_use_gate_coords()
