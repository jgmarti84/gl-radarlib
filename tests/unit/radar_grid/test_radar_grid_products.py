"""
Unit tests for radar_grid.products module.

Tests radar products like CAPPI, PPI, COLMAX and beam height calculations.
"""

import numpy as np
import pytest

from radarlib.radar_grid.geometry import GridGeometry
from radarlib.radar_grid.products import (
    EARTH_RADIUS,
    EFFECTIVE_RADIUS_FACTOR,
    column_max,
    column_mean,
    column_min,
    compute_beam_height,
    compute_beam_height_flat,
    compute_beam_height_simple,
    constant_altitude_ppi,
    constant_elevation_ppi,
    get_beam_height_difference,
    get_elevation_from_z_level,
)


class TestBeamHeightCalculations:
    """Test beam height calculation functions."""

    def test_compute_beam_height_basic(self):
        """Test basic beam height calculation."""
        horizontal_distance = np.array([10000.0, 20000.0, 50000.0])  # meters
        elevation_angle = 2.0  # degrees
        radar_altitude = 100.0  # meters

        heights = compute_beam_height(horizontal_distance, elevation_angle, radar_altitude)

        assert heights.shape == horizontal_distance.shape
        assert np.all(heights > radar_altitude)  # Heights should be above radar
        assert np.all(np.isfinite(heights))

        # Heights should increase with distance
        assert heights[1] > heights[0]
        assert heights[2] > heights[1]

    def test_compute_beam_height_zero_elevation(self):
        """Test beam height with zero elevation angle."""
        horizontal_distance = np.array([10000.0, 20000.0])
        elevation_angle = 0.0

        heights = compute_beam_height(horizontal_distance, elevation_angle, 0.0)

        # At zero elevation, height should increase due to Earth curvature
        assert heights[1] > heights[0]

    def test_compute_beam_height_high_elevation(self):
        """Test beam height with high elevation angle."""
        horizontal_distance = np.array([10000.0])
        elevation_angle = 45.0  # degrees

        heights = compute_beam_height(horizontal_distance, elevation_angle, 0.0)

        # At 45 degrees, beam should be quite high
        assert heights[0] > 7000.0  # Should be above 7km

    def test_compute_beam_height_simple(self):
        """Test simplified beam height calculation."""
        horizontal_distance = np.array([10000.0, 20000.0, 50000.0])
        elevation_angle = 2.0

        heights = compute_beam_height_simple(horizontal_distance, elevation_angle, 100.0)

        assert heights.shape == horizontal_distance.shape
        assert np.all(heights > 100.0)
        assert np.all(np.isfinite(heights))

    def test_compute_beam_height_flat(self):
        """Test flat Earth beam height calculation."""
        horizontal_distance = np.array([10000.0, 20000.0])
        elevation_angle = 2.0

        heights = compute_beam_height_flat(horizontal_distance, elevation_angle, 100.0)

        assert heights.shape == horizontal_distance.shape
        assert np.all(heights > 100.0)

        # For flat Earth, height should be linear with distance at constant elevation
        # h = h0 + d * tan(elevation)
        expected_diff = 10000.0 * np.tan(np.radians(2.0))
        actual_diff = heights[1] - heights[0]
        np.testing.assert_almost_equal(actual_diff, expected_diff, decimal=1)


class TestGetElevationFromZLevel:
    """Test get_elevation_from_z_level function."""

    @pytest.fixture
    def sample_geometry(self):
        """Create sample geometry for testing."""
        return GridGeometry(
            grid_shape=(10, 50, 50),
            grid_limits=((0.0, 10000.0), (-25000.0, 25000.0), (-25000.0, 25000.0)),
            indptr=np.arange(25001, dtype=np.int32),
            gate_indices=np.zeros(25000, dtype=np.int32),
            weights=np.ones(25000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=100.0,
        )

    def test_get_elevation_zero_distance(self, sample_geometry):
        """Test elevation with zero horizontal distance."""
        z_level_idx = 5

        # Should handle zero distance gracefully
        elevation = get_elevation_from_z_level(z_level_idx, sample_geometry)

        assert np.isfinite(elevation).all()


class TestGetBeamHeightDifference:
    """Test get_beam_height_difference function."""

    @pytest.fixture
    def sample_geometry(self):
        """Create sample geometry."""
        return GridGeometry(
            grid_shape=(10, 50, 50),
            grid_limits=((0.0, 10000.0), (-25000.0, 25000.0), (-25000.0, 25000.0)),
            indptr=np.arange(25001, dtype=np.int32),
            gate_indices=np.zeros(25000, dtype=np.int32),
            weights=np.ones(25000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=100.0,
        )

    def test_get_beam_height_difference(self, sample_geometry):
        """Test beam height difference calculation."""
        # z_level_idx = 5
        # horizontal_distance = 25000.0
        elevation_angle = 2.0

        diff = get_beam_height_difference(sample_geometry, elevation_angle)

        assert isinstance(diff, np.ndarray)
        assert diff.dtype == np.float64
        assert np.isfinite(diff).all()


class TestConstantAltitudePPI:
    """Test constant_altitude_ppi function (CAPPI)."""

    @pytest.fixture
    def sample_grid_data(self):
        """Create sample 3D grid data."""
        nz, ny, nx = 10, 50, 50
        data = np.random.rand(nz, ny, nx).astype(np.float32) * 50
        # Add some NaN values
        data[0, 0:10, 0:10] = np.nan
        return data

    @pytest.fixture
    def sample_geometry(self):
        """Create sample geometry."""
        return GridGeometry(
            grid_shape=(10, 50, 50),
            grid_limits=((0.0, 10000.0), (-25000.0, 25000.0), (-25000.0, 25000.0)),
            indptr=np.arange(25001, dtype=np.int32),
            gate_indices=np.zeros(25000, dtype=np.int32),
            weights=np.ones(25000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=100.0,
        )

    def test_cappi_basic(self, sample_grid_data, sample_geometry):
        """Test basic CAPPI generation."""
        altitude = 5000.0  # meters

        cappi = constant_altitude_ppi(sample_grid_data, sample_geometry, altitude)

        assert cappi.shape == (50, 50)
        assert cappi.dtype == np.float32

    def test_cappi_at_grid_level(self, sample_grid_data, sample_geometry):
        """Test CAPPI at exact grid level."""
        # Grid levels are at 0, 1111, 2222, ..., 10000 meters
        altitude = 5000.0

        cappi = constant_altitude_ppi(sample_grid_data, sample_geometry, altitude)

        assert cappi.shape == (50, 50)
        # Should have valid data (not all NaN)
        assert not np.all(np.isnan(cappi))

    def test_cappi_above_grid(self, sample_grid_data, sample_geometry):
        """Test CAPPI above grid top."""
        altitude = 15000.0  # Above max altitude

        cappi = constant_altitude_ppi(sample_grid_data, sample_geometry, altitude)

        # Should return all NaN
        assert np.all(np.isnan(cappi))

    def test_cappi_below_grid(self, sample_grid_data, sample_geometry):
        """Test CAPPI below grid bottom."""
        altitude = -1000.0  # Below min altitude

        cappi = constant_altitude_ppi(sample_grid_data, sample_geometry, altitude)

        # Should handle gracefully
        assert cappi.shape == (50, 50)


class TestConstantElevationPPI:
    """Test constant_elevation_ppi function."""

    @pytest.fixture
    def sample_grid_data(self):
        """Create sample 3D grid data."""
        nz, ny, nx = 10, 50, 50
        return np.random.rand(nz, ny, nx).astype(np.float32) * 50

    @pytest.fixture
    def sample_geometry(self):
        """Create sample geometry."""
        return GridGeometry(
            grid_shape=(10, 50, 50),
            grid_limits=((0.0, 10000.0), (-25000.0, 25000.0), (-25000.0, 25000.0)),
            indptr=np.arange(25001, dtype=np.int32),
            gate_indices=np.zeros(25000, dtype=np.int32),
            weights=np.ones(25000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=100.0,
        )

    def test_ppi_basic(self, sample_grid_data, sample_geometry):
        """Test basic PPI generation."""
        elevation_angle = 2.0  # degrees

        ppi = constant_elevation_ppi(sample_grid_data, sample_geometry, elevation_angle)

        assert ppi.shape == (50, 50)
        assert ppi.dtype == np.float32

    def test_ppi_zero_elevation(self, sample_grid_data, sample_geometry):
        """Test PPI with zero elevation."""
        elevation_angle = 0.0

        ppi = constant_elevation_ppi(sample_grid_data, sample_geometry, elevation_angle)

        assert ppi.shape == (50, 50)

    def test_ppi_high_elevation(self, sample_grid_data, sample_geometry):
        """Test PPI with high elevation angle."""
        elevation_angle = 45.0

        ppi = constant_elevation_ppi(sample_grid_data, sample_geometry, elevation_angle)

        assert ppi.shape == (50, 50)


class TestColumnAggregations:
    """Test column aggregation functions (max, min, mean)."""

    @pytest.fixture
    def sample_grid_data(self):
        """Create sample 3D grid data with known values."""
        nz, ny, nx = 10, 50, 50
        data = np.zeros((nz, ny, nx), dtype=np.float32)

        # Set specific values for testing
        for z in range(nz):
            data[z, :, :] = z * 10.0

        # Add some NaN values
        data[:, 0:5, 0:5] = np.nan

        return data

    def test_column_max(self, sample_grid_data):
        """Test column maximum."""
        colmax = column_max(sample_grid_data)

        assert colmax.shape == (50, 50)

        # Max should be 90 (from z=9) except where all NaN
        assert colmax[10, 10] == 90.0
        assert np.isnan(colmax[0, 0])

    def test_column_min(self, sample_grid_data):
        """Test column minimum."""
        colmin = column_min(sample_grid_data)

        assert colmin.shape == (50, 50)

        # Min should be 0 (from z=0) except where all NaN
        assert colmin[10, 10] == 0.0
        assert np.isnan(colmin[0, 0])

    def test_column_mean(self, sample_grid_data):
        """Test column mean."""
        colmean = column_mean(sample_grid_data)

        assert colmean.shape == (50, 50)

        # Mean should be 45 (average of 0, 10, 20, ..., 90) except where NaN
        expected_mean = np.mean([0, 10, 20, 30, 40, 50, 60, 70, 80, 90])
        np.testing.assert_almost_equal(colmean[10, 10], expected_mean, decimal=1)
        assert np.isnan(colmean[0, 0])

    def test_column_aggregation_all_nan(self):
        """Test column aggregations with all NaN data."""
        data = np.full((10, 50, 50), np.nan, dtype=np.float32)

        colmax = column_max(data)
        colmin = column_min(data)
        colmean = column_mean(data)

        assert np.all(np.isnan(colmax))
        assert np.all(np.isnan(colmin))
        assert np.all(np.isnan(colmean))

    def test_column_aggregation_partial_nan(self):
        """Test column aggregations with partial NaN data."""
        nz, ny, nx = 10, 50, 50
        data = np.ones((nz, ny, nx), dtype=np.float32) * 10.0

        # Set some layers to NaN
        data[0:3, :, :] = np.nan

        colmax = column_max(data)
        colmin = column_min(data)
        colmean = column_mean(data)

        # Should compute over valid data only
        assert np.all(colmax[10:20, 10:20] == 10.0)
        assert np.all(colmin[10:20, 10:20] == 10.0)
        assert np.all(colmean[10:20, 10:20] == 10.0)


class TestProductsWithMaskedArrays:
    """Test products with masked arrays."""

    @pytest.fixture
    def masked_grid_data(self):
        """Create sample masked 3D grid data."""
        nz, ny, nx = 10, 50, 50
        data = np.ma.array(np.random.rand(nz, ny, nx).astype(np.float32) * 50, mask=np.zeros((nz, ny, nx), dtype=bool))
        # Mask some data
        data.mask[0, 0:10, 0:10] = True
        return data

    @pytest.fixture
    def sample_geometry(self):
        """Create sample geometry."""
        return GridGeometry(
            grid_shape=(10, 50, 50),
            grid_limits=((0.0, 10000.0), (-25000.0, 25000.0), (-25000.0, 25000.0)),
            indptr=np.arange(25001, dtype=np.int32),
            gate_indices=np.zeros(25000, dtype=np.int32),
            weights=np.ones(25000, dtype=np.float32),
            toa=12000.0,
            radar_altitude=100.0,
        )

    def test_cappi_with_masked_array(self, masked_grid_data, sample_geometry):
        """Test CAPPI with masked array input."""
        cappi = constant_altitude_ppi(masked_grid_data, sample_geometry, 5000.0)

        assert cappi.shape == (50, 50)
        # Should work without errors


class TestProductsConstants:
    """Test module constants."""

    def test_earth_radius_constant(self):
        """Test EARTH_RADIUS constant."""
        assert EARTH_RADIUS == 6371000.0
        assert isinstance(EARTH_RADIUS, float)

    def test_effective_radius_factor_constant(self):
        """Test EFFECTIVE_RADIUS_FACTOR constant."""
        assert EFFECTIVE_RADIUS_FACTOR == 4.0 / 3.0
        assert isinstance(EFFECTIVE_RADIUS_FACTOR, float)
