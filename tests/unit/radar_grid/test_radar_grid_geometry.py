"""
Unit tests for radar_grid.geometry module.

Tests the GridGeometry dataclass and serialization functions.
"""

import tempfile
from pathlib import Path

import numpy as np

from radarlib.radar_grid.geometry import GridGeometry, load_geometry, save_geometry

# import pytest


class TestGridGeometry:
    """Test GridGeometry dataclass."""

    def test_create_geometry(self):
        """Test creating a GridGeometry object."""
        nz, ny, nx = 10, 50, 50
        grid_shape = (nz, ny, nx)
        grid_limits = ((0.0, 10000.0), (-25000.0, 25000.0), (-25000.0, 25000.0))

        n_points = nz * ny * nx
        indptr = np.arange(n_points + 1, dtype=np.int32)
        gate_indices = np.zeros(n_points, dtype=np.int32)
        weights = np.ones(n_points, dtype=np.float32)

        geometry = GridGeometry(
            grid_shape=grid_shape,
            grid_limits=grid_limits,
            indptr=indptr,
            gate_indices=gate_indices,
            weights=weights,
            toa=12000.0,
            radar_altitude=100.0,
        )

        assert geometry.grid_shape == grid_shape
        assert geometry.grid_limits == grid_limits
        assert geometry.toa == 12000.0
        assert geometry.radar_altitude == 100.0

    def test_memory_usage_mb(self):
        """Test memory usage calculation."""
        n_points = 1000
        geometry = GridGeometry(
            grid_shape=(10, 10, 10),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.arange(n_points + 1, dtype=np.int32),
            gate_indices=np.zeros(n_points, dtype=np.int32),
            weights=np.ones(n_points, dtype=np.float32),
            toa=12000.0,
        )

        memory = geometry.memory_usage_mb()
        assert memory > 0
        # Should be approximately (n_points+1 + n_points + n_points) * 4 bytes / 1e6
        expected = ((n_points + 1) * 4 + n_points * 4 + n_points * 4) / 1e6
        assert abs(memory - expected) < 0.001

    def test_n_grid_points(self):
        """Test grid points count."""
        geometry = GridGeometry(
            grid_shape=(5, 10, 20),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.arange(1001, dtype=np.int32),
            gate_indices=np.zeros(1000, dtype=np.int32),
            weights=np.ones(1000, dtype=np.float32),
            toa=12000.0,
        )

        assert geometry.n_grid_points() == 5 * 10 * 20

    def test_n_pairs(self):
        """Test pairs count."""
        n_pairs = 5000
        geometry = GridGeometry(
            grid_shape=(10, 10, 10),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.arange(1001, dtype=np.int32),
            gate_indices=np.zeros(n_pairs, dtype=np.int32),
            weights=np.ones(n_pairs, dtype=np.float32),
            toa=12000.0,
        )

        assert geometry.n_pairs() == n_pairs

    def test_avg_neighbors(self):
        """Test average neighbors calculation."""
        n_points = 1000
        n_pairs = 5000
        geometry = GridGeometry(
            grid_shape=(10, 10, 10),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.arange(n_points + 1, dtype=np.int32),
            gate_indices=np.zeros(n_pairs, dtype=np.int32),
            weights=np.ones(n_pairs, dtype=np.float32),
            toa=12000.0,
        )

        assert geometry.avg_neighbors() == n_pairs / n_points

    def test_z_levels(self):
        """Test Z levels calculation."""
        nz = 10
        z_min, z_max = 0.0, 10000.0
        geometry = GridGeometry(
            grid_shape=(nz, 20, 20),
            grid_limits=((z_min, z_max), (-500, 500), (-500, 500)),
            indptr=np.arange(4001, dtype=np.int32),
            gate_indices=np.zeros(4000, dtype=np.int32),
            weights=np.ones(4000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=100.0,
        )

        z_levels = geometry.z_levels()
        assert len(z_levels) == nz
        assert z_levels[0] == z_min
        assert z_levels[-1] == z_max

    def test_z_levels_absolute(self):
        """Test absolute Z levels calculation."""
        nz = 10
        z_min, z_max = 0.0, 10000.0
        radar_alt = 500.0
        geometry = GridGeometry(
            grid_shape=(nz, 20, 20),
            grid_limits=((z_min, z_max), (-500, 500), (-500, 500)),
            indptr=np.arange(4001, dtype=np.int32),
            gate_indices=np.zeros(4000, dtype=np.int32),
            weights=np.ones(4000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=radar_alt,
        )

        z_levels_abs = geometry.z_levels_absolute()
        z_levels_rel = geometry.z_levels()

        np.testing.assert_array_almost_equal(z_levels_abs, z_levels_rel + radar_alt)


class TestGeometrySerialization:
    """Test geometry save and load functions."""

    def test_save_and_load_geometry(self):
        """Test saving and loading geometry."""
        # Create geometry
        nz, ny, nx = 5, 10, 10
        grid_shape = (nz, ny, nx)
        grid_limits = ((0.0, 5000.0), (-10000.0, 10000.0), (-10000.0, 10000.0))

        n_points = nz * ny * nx
        indptr = np.arange(n_points + 1, dtype=np.int32)
        gate_indices = np.random.randint(0, 1000, size=n_points, dtype=np.int32)
        weights = np.random.rand(n_points).astype(np.float32)

        original_geometry = GridGeometry(
            grid_shape=grid_shape,
            grid_limits=grid_limits,
            indptr=indptr,
            gate_indices=gate_indices,
            weights=weights,
            toa=10000.0,
            radar_altitude=200.0,
        )

        # Save and load
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_geometry.npz"
            save_geometry(original_geometry, str(filepath))

            assert filepath.exists()

            loaded_geometry = load_geometry(str(filepath))

            # Verify all attributes match
            assert loaded_geometry.grid_shape == original_geometry.grid_shape
            assert loaded_geometry.grid_limits == original_geometry.grid_limits
            assert loaded_geometry.toa == original_geometry.toa
            assert loaded_geometry.radar_altitude == original_geometry.radar_altitude

            np.testing.assert_array_equal(loaded_geometry.indptr, original_geometry.indptr)
            np.testing.assert_array_equal(loaded_geometry.gate_indices, original_geometry.gate_indices)
            np.testing.assert_array_almost_equal(loaded_geometry.weights, original_geometry.weights)

    def test_load_geometry_backward_compatibility(self):
        """Test loading geometry without radar_altitude field (backward compatibility)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_geometry.npz"

            # Manually create a geometry file without radar_altitude
            grid_shape = (5, 10, 10)
            n_points = 500
            np.savez_compressed(
                filepath,
                grid_shape=np.array(grid_shape),
                grid_limits_z=np.array([0.0, 5000.0]),
                grid_limits_y=np.array([-10000.0, 10000.0]),
                grid_limits_x=np.array([-10000.0, 10000.0]),
                indptr=np.arange(n_points + 1, dtype=np.int32),
                gate_indices=np.zeros(n_points, dtype=np.int32),
                weights=np.ones(n_points, dtype=np.float32),
                toa=np.array([10000.0]),
                # Note: no radar_altitude field
            )

            # Should load without error and default to 0.0
            geometry = load_geometry(str(filepath))
            assert geometry.radar_altitude == 0.0


class TestGridGeometryRepresentation:
    """Test GridGeometry string representation."""

    def test_repr(self):
        """Test __repr__ method."""
        geometry = GridGeometry(
            grid_shape=(10, 20, 20),
            grid_limits=((0, 10000), (-25000, 25000), (-25000, 25000)),
            indptr=np.arange(4001, dtype=np.int32),
            gate_indices=np.zeros(4000, dtype=np.int32),
            weights=np.ones(4000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=150.0,
        )

        repr_str = repr(geometry)
        assert "GridGeometry" in repr_str
        assert "grid_shape=(10, 20, 20)" in repr_str
        assert "toa=12000" in repr_str
        assert "radar_altitude=150" in repr_str
