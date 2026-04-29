"""
GridGeometry class and serialization functions.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple
from zipfile import BadZipFile

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
    metadata: Dict[str, Any] = field(default_factory=dict)

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
        meta_str = ""
        if self.metadata:
            meta_str = (
                f"  radar_name={self.metadata.get('radar_name', '?')},\n"
                f"  strategy={self.metadata.get('strategy', '?')},\n"
                f"  volume_nr={self.metadata.get('volume_nr', '?')},\n"
                f"  grid_resolution={self.metadata.get('grid_resolution', '?')}m,\n"
                f"  h_factor={self.metadata.get('h_factor', '?')},\n"
                f"  min_radius={self.metadata.get('min_radius', '?')}m,\n"
                f"  max_neighbors={self.metadata.get('max_neighbors', '?')},\n"
                f"  nb={self.metadata.get('nb', '?')},\n"
                f"  bsp={self.metadata.get('bsp', '?')},\n"
                f"  weighting={self.metadata.get('weighting', '?')},\n"
            )
        return (
            f"GridGeometry(\n"
            f"  grid_shape={self.grid_shape},\n"
            f"  grid_limits={self.grid_limits},\n"
            f"  toa={self.toa}m,\n"
            f"  radar_altitude={self.radar_altitude}m,\n"
            f"{meta_str}"
            f"  n_pairs={self.n_pairs():,},\n"
            f"  avg_neighbors={self.avg_neighbors():.1f},\n"
            f"  memory={self.memory_usage_mb():.1f} MB\n"
            f")"
        )


def build_geometry_filename(metadata: Dict[str, Any]) -> str:
    """
    Build a canonical geometry filename from build parameters.

    The filename encodes every parameter that affects the geometry so that
    two geometries with different settings always produce different filenames.

    Parameters
    ----------
    metadata : dict
        Must contain the keys used during geometry construction:
        radar_name, strategy, volume_nr, grid_resolution, toa, h_factor,
        min_radius, max_neighbors, nb, bsp, weighting.

    Returns
    -------
    str
        Filename stem (no extension). Append ``.npz`` for the full path.

    Examples
    --------
    >>> build_geometry_filename(meta)
    'RMA1_0315_01_RES1000_TOA12000_HF0p0175_MR900_MN1_NB1p40_BSP1p20_barnes2_geometry'
    """

    def _fmt(value: float, decimals: int = 2) -> str:
        """Format a float replacing '.' with 'p', e.g. 1.4 -> '1p40'."""
        return f"{float(value):.{decimals}f}".replace(".", "p")

    radar_name = metadata.get("radar_name", "UNKNOWN")
    strategy = metadata.get("strategy", "XX")
    volume_nr = metadata.get("volume_nr", "00")
    res_xy = int(metadata.get("grid_resolution_xy", 0))
    res_z = int(metadata.get("grid_resolution_z", 0))
    toa = int(metadata.get("toa", 0))
    h_factor = metadata.get("h_factor", 0.0)
    min_radius = int(metadata.get("min_radius", 0))
    max_neighbors = metadata.get("max_neighbors", 1)
    nb = metadata.get("nb", 0.0)
    bsp = metadata.get("bsp", 0.0)
    weighting = metadata.get("weighting", "nearest")

    return (
        f"{radar_name}_{strategy}_{volume_nr}"
        f"_RES{res_xy}x{res_z}"
        f"_TOA{toa}"
        f"_HF{_fmt(h_factor, 4)}"
        f"_MR{min_radius}"
        f"_MN{max_neighbors}"
        f"_NB{_fmt(nb, 2)}"
        f"_BSP{_fmt(bsp, 2)}"
        f"_{weighting}"
        f"_geometry"
    )


def peek_geometry_metadata(filepath: str) -> Dict[str, Any]:
    """
    Read only the build-parameter metadata from a saved geometry file.

    This is intentionally cheap: numpy's NpzFile is lazy, so only the tiny
    ``metadata`` entry is decompressed — the large ``indptr``,
    ``gate_indices``, and ``weights`` arrays are never touched.

    Parameters
    ----------
    filepath : str
        Path to an ``.npz`` geometry file.

    Returns
    -------
    dict
        The metadata dict that was stored when the geometry was saved.
        Returns an empty dict if the file has no metadata entry (legacy file).

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    BadZipFile
        If the file is not a valid numpy .npz archive.

    Examples
    --------
    >>> meta = peek_geometry_metadata("RMA1_geometry.npz")
    >>> print(meta["weighting"], meta["nb"])
    barnes2 1.4
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Geometry file not found: {filepath}")

    try:
        with np.load(filepath, allow_pickle=False) as data:
            if "metadata" not in data:
                logger.warning("No metadata found in %s (legacy file)", filepath)
                return {}
            return json.loads(str(data["metadata"]))
    except (BadZipFile, ValueError, OSError) as e:
        raise BadZipFile(
            f"Cannot read geometry file {filepath}: file is not a valid numpy archive. "
            f"The file may be corrupted, incomplete, or not a valid .npz file. Original error: {e}"
        ) from e


def save_geometry(geometry: GridGeometry, filepath: str) -> None:
    """
    Save geometry to disk using numpy's compressed format.

    The ``geometry.metadata`` dict (if populated) is serialised as a JSON
    string and stored alongside the arrays so it can be inspected later with
    :func:`peek_geometry_metadata` without loading the full geometry.

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
        metadata=np.array(json.dumps(geometry.metadata)),
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

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    BadZipFile
        If the file is not a valid numpy .npz archive.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Geometry file not found: {filepath}")

    try:
        data = np.load(filepath, allow_pickle=False)
        metadata = json.loads(str(data["metadata"])) if "metadata" in data else {}
        geometry = GridGeometry(
            grid_shape=tuple(data["grid_shape"]),
            grid_limits=(tuple(data["grid_limits_z"]), tuple(data["grid_limits_y"]), tuple(data["grid_limits_x"])),
            indptr=data["indptr"],
            gate_indices=data["gate_indices"],
            weights=data["weights"],
            toa=float(data["toa"][0]) if "toa" in data else np.inf,
            radar_altitude=float(data["radar_altitude"][0]) if "radar_altitude" in data else 0.0,
            metadata=metadata,
        )
        logger.info(f"Loaded geometry: {geometry.memory_usage_mb():.1f} MB in memory, toa={geometry.toa}m")
        return geometry
    except (BadZipFile, ValueError, OSError) as e:
        raise BadZipFile(
            f"Cannot load geometry from {filepath}: file is not a valid numpy archive. "
            f"The file may be corrupted, incomplete, or not a valid .npz file. Original error: {e}"
        ) from e
