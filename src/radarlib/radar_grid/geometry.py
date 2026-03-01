"""
GridGeometry class and serialization functions.
"""

import logging
import os
from dataclasses import dataclass
from typing import Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class GridGeometry:
    """
    Stores the precomputed gate-to-grid mapping.

    This class holds a sparse representation of which radar gates
    contribute to each grid point, along with their interpolation weights.

    Attributes
    ----------
    grid_shape : tuple of int
        (nz, ny, nx) dimensions of the output grid
    grid_limits : tuple of tuples
        ((z_min, z_max), (y_min, y_max), (x_min, x_max)) in meters
    indptr : np.ndarray
        CSR-format index pointers, shape (n_grid_points + 1,)
    gate_indices : np.ndarray
        Indices of contributing gates, shape (n_total_pairs,)
    weights : np.ndarray
        Interpolation weights, shape (n_total_pairs,)
    toa : float
        Top of atmosphere used during computation (meters)

    Notes
    -----
    The sparse mapping uses CSR-like format. For grid point i,
    the contributing gates are:
        gate_indices[indptr[i]:indptr[i+1]]
    with corresponding weights:
        weights[indptr[i]:indptr[i+1]]
    """

    grid_shape: Tuple[int, int, int]
    grid_limits: Tuple[Tuple[float, float], ...]
    indptr: np.ndarray  # index pointers
    gate_indices: np.ndarray  # indices of contributing gates
    weights: np.ndarray  # interpolation weights
    toa: float
    radar_altitude: float = 0.0

    def memory_usage_mb(self) -> float:
        """Return memory usage in megabytes."""
        return (self.indptr.nbytes + self.gate_indices.nbytes + self.weights.nbytes) / 1e6

    def n_grid_points(self) -> int:
        """Return total number of grid points."""
        return int(np.prod(self.grid_shape))

    def n_pairs(self) -> int:
        """Return total number of (grid_point, gate) pairs."""
        return len(self.gate_indices)

    def avg_neighbors(self) -> float:
        """Return average number of gates per grid point."""
        return self.n_pairs() / self.n_grid_points()

    def z_levels(self) -> np.ndarray:
        """Return the Z coordinates of each level (meters above radar)."""
        nz = self.grid_shape[0]
        z_min, z_max = self.grid_limits[0]
        return np.linspace(z_min, z_max, nz)

    def z_levels_absolute(self) -> np.ndarray:
        """Return the Z coordinates as absolute altitude (meters above sea level)."""
        return self.z_levels() + self.radar_altitude

    def __repr__(self) -> str:
        return (
            f"GridGeometry(\n"
            f"  grid_shape={self.grid_shape},\n"
            f"  grid_limits={self.grid_limits},\n"
            f"  toa={self.toa}m,\n"
            f"  radar_altitude={self.radar_altitude}m,\n"
            f"  n_pairs={self.n_pairs():,},\n"
            f"  avg_neighbors={self.avg_neighbors():.1f},\n"
            f"  memory={self.memory_usage_mb():.1f} MB\n"
            f")"
        )


def save_geometry(geometry: GridGeometry, filepath: str) -> None:
    """
    Save geometry to disk using numpy's compressed format.

    Parameters
    ----------
    geometry : GridGeometry
        The geometry object to save
    filepath : str
        Output file path (should end in .npz)
    """
    np.savez_compressed(
        filepath,
        grid_shape=np.array(geometry.grid_shape),
        grid_limits_z=np.array(geometry.grid_limits[0]),
        grid_limits_y=np.array(geometry.grid_limits[1]),
        grid_limits_x=np.array(geometry.grid_limits[2]),
        indptr=geometry.indptr,
        gate_indices=geometry.gate_indices,
        weights=geometry.weights,
        toa=np.array([geometry.toa]),
        radar_altitude=np.array([geometry.radar_altitude]),
    )
    file_size_mb = os.path.getsize(filepath) / 1e6
    logger.info(f"Saved geometry to {filepath} ({file_size_mb:.1f} MB on disk)")


def load_geometry(filepath: str) -> GridGeometry:
    """
    Load geometry from disk.

    Parameters
    ----------
    filepath : str
        Path to the .npz file

    Returns
    -------
    GridGeometry
        The loaded geometry object
    """
    data = np.load(filepath)
    geometry = GridGeometry(
        grid_shape=tuple(data["grid_shape"]),
        grid_limits=(tuple(data["grid_limits_z"]), tuple(data["grid_limits_y"]), tuple(data["grid_limits_x"])),
        indptr=data["indptr"],
        gate_indices=data["gate_indices"],
        weights=data["weights"],
        toa=float(data["toa"][0]) if "toa" in data else np.inf,
        radar_altitude=float(data["radar_altitude"][0]) if "radar_altitude" in data else 0.0,
    )
    logger.info(f"Loaded geometry: {geometry.memory_usage_mb():.1f} MB in memory, toa={geometry.toa}m")
    return geometry
