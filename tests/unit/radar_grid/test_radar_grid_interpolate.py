"""
Unit tests for radar_grid.interpolate module.

Tests the interpolation functions that use precomputed geometry.
"""

from unittest.mock import Mock

import numpy as np
import pytest

from radarlib.radar_grid.filters import GateFilter
from radarlib.radar_grid.geometry import GridGeometry
from radarlib.radar_grid.interpolate import apply_geometry, apply_geometry_multi


class TestApplyGeometry:
    """Test apply_geometry function."""

    @pytest.fixture
    def simple_geometry(self):
        """Create a simple geometry for testing."""
        # 2x2x2 grid with simple mapping
        nz, ny, nx = 2, 2, 2
        n_grid = nz * ny * nx  # 8 grid points

        # Each grid point gets 2 gates
        indptr = np.arange(0, n_grid * 2 + 1, 2, dtype=np.int32)
        gate_indices = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], dtype=np.int32)
        weights = np.ones(16, dtype=np.float32)

        return GridGeometry(
            grid_shape=(nz, ny, nx),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=indptr,
            gate_indices=gate_indices,
            weights=weights,
            toa=2000.0,
        )

    def test_apply_geometry_basic(self, simple_geometry):
        """Test basic interpolation."""
        # Create field data for 16 gates
        field_data = np.ma.array(np.arange(16, dtype=np.float32))

        result = apply_geometry(simple_geometry, np.ma.masked_invalid(field_data))

        assert result.shape == (2, 2, 2)
        assert result.dtype == np.float32

    def test_apply_geometry_with_nans(self, simple_geometry):
        """Test interpolation with NaN values."""
        field_data = np.ma.array(np.ones(16, dtype=np.float32) * 10.0)
        field_data[0:4] = np.nan

        result = apply_geometry(simple_geometry, np.ma.masked_invalid(field_data))

        assert result.shape == (2, 2, 2)
        # First grid point should be NaN since its gates are NaN
        assert np.isnan(result.ravel()[0])

    def test_apply_geometry_with_mask(self, simple_geometry):
        """Test interpolation with masked array."""
        field_data = np.ma.array(np.ones(16, dtype=np.float32) * 10.0, mask=np.zeros(16, dtype=bool))
        field_data.mask[0:4] = True

        result = apply_geometry(simple_geometry, field_data)

        assert result.shape == (2, 2, 2)
        # First grid point should be NaN due to masked data
        assert np.isnan(result.ravel()[0])

    def test_apply_geometry_weighted_average(self):
        """Test that weighted average is computed correctly."""
        # Simple case: 1 grid point, 2 gates
        geometry = GridGeometry(
            grid_shape=(1, 1, 1),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.array([0, 2], dtype=np.int32),
            gate_indices=np.array([0, 1], dtype=np.int32),
            weights=np.array([0.3, 0.7], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.array([10.0, 20.0], dtype=np.float32)

        result = apply_geometry(geometry, np.ma.masked_invalid(field_data))

        # Expected: (10*0.3 + 20*0.7) / (0.3 + 0.7) = (3 + 14) / 1.0 = 17.0
        expected = 17.0
        np.testing.assert_almost_equal(result[0, 0, 0], expected, decimal=5)

    def test_apply_geometry_with_gate_filter(self, simple_geometry):
        """Test interpolation with gate filter."""
        # Create mock radar for filter
        radar = Mock()
        radar.nrays = 4
        radar.ngates = 4
        radar.fields = {"DBZH": {"data": np.ones((4, 4), dtype=np.float32) * 10.0}}

        # Create filter that excludes first 4 gates
        gf = GateFilter(radar)
        radar.fields["DBZH"]["data"][0, 0:4] = -999.0
        gf.exclude_below("DBZH", 0.0)

        field_data = np.ma.array(np.ones(16, dtype=np.float32) * 10.0)

        result = apply_geometry(simple_geometry, field_data, additional_filters=gf)

        assert result.shape == (2, 2, 2)

    def test_apply_geometry_fill_value(self):
        """Test custom fill value."""
        geometry = GridGeometry(
            grid_shape=(1, 1, 1),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.array([0, 0], dtype=np.int32),  # No gates for this point
            gate_indices=np.array([], dtype=np.int32),
            weights=np.array([], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.array([10.0], dtype=np.float32)

        result = apply_geometry(geometry, np.ma.masked_invalid(field_data), fill_value=-9999.0)

        # Should use fill_value for point with no gates
        assert result[0, 0, 0] == -9999.0

    def test_apply_geometry_all_masked(self):
        """Test when all contributing gates are masked."""
        geometry = GridGeometry(
            grid_shape=(1, 1, 1),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.array([0, 2], dtype=np.int32),
            gate_indices=np.array([0, 1], dtype=np.int32),
            weights=np.array([0.5, 0.5], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.array([10.0, 20.0], mask=[True, True])  # Both masked

        result = apply_geometry(geometry, field_data)

        # Should be fill_value (NaN by default) when all gates masked
        assert np.isnan(result[0, 0, 0])


class TestApplyGeometryMulti:
    """Test apply_geometry_multi function for multiple fields."""

    @pytest.fixture
    def simple_geometry(self):
        """Create a simple geometry."""
        return GridGeometry(
            grid_shape=(2, 2, 2),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.arange(0, 17, 2, dtype=np.int32),
            gate_indices=np.arange(16, dtype=np.int32),
            weights=np.ones(16, dtype=np.float32),
            toa=2000.0,
        )

    def test_apply_geometry_multi_basic(self, simple_geometry):
        """Test interpolating multiple fields."""
        fields = {
            "DBZH": np.ma.array(np.ones(16, dtype=np.float32) * 10.0),
            "ZDR": np.ma.array(np.ones(16, dtype=np.float32) * 2.0),
            "RHOHV": np.ma.array(np.ones(16, dtype=np.float32) * 0.95),
        }
        for k, v in fields.items():
            fields[k] = np.ma.masked_invalid(v)
        results = apply_geometry_multi(simple_geometry, fields)

        assert isinstance(results, dict)
        assert len(results) == 3
        assert "DBZH" in results
        assert "ZDR" in results
        assert "RHOHV" in results

        for _field_name, grid in results.items():
            assert grid.shape == (2, 2, 2)

    def test_apply_geometry_multi_with_filters(self, simple_geometry):
        """Test multi-field interpolation with filters."""
        fields = {
            "DBZH": np.ma.array(np.ones(16, dtype=np.float32) * 10.0),
            "ZDR": np.ma.array(np.ones(16, dtype=np.float32) * 2.0),
        }
        for k, v in fields.items():
            fields[k] = np.ma.masked_invalid(v)
        # Create mock filter
        radar = Mock()
        radar.nrays = 4
        radar.ngates = 4
        radar.fields = {"DBZH": {"data": np.ones((4, 4), dtype=np.float32) * 10.0}}

        gf = GateFilter(radar)
        gf.exclude_below("DBZH", 5.0)

        results = apply_geometry_multi(simple_geometry, fields, additional_filters={"DBZH": gf})

        assert len(results) == 2


class TestApplyGeometryEdgeCases:
    """Test edge cases for apply_geometry."""

    def test_empty_geometry(self):
        """Test with geometry that has no gate mappings."""
        geometry = GridGeometry(
            grid_shape=(2, 2, 2),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.zeros(9, dtype=np.int32),  # All zeros = no gates
            gate_indices=np.array([], dtype=np.int32),
            weights=np.array([], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.masked_invalid(np.ma.array([10.0], dtype=np.float32))

        result = apply_geometry(geometry, field_data)

        # All grid points should be fill_value
        assert np.all(np.isnan(result))

    def test_single_grid_point(self):
        """Test with single grid point."""
        geometry = GridGeometry(
            grid_shape=(1, 1, 1),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.array([0, 3], dtype=np.int32),
            gate_indices=np.array([0, 1, 2], dtype=np.int32),
            weights=np.array([0.2, 0.5, 0.3], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.masked_invalid(np.ma.array([10.0, 20.0, 30.0], dtype=np.float32))

        result = apply_geometry(geometry, field_data)

        assert result.shape == (1, 1, 1)
        # Expected: (10*0.2 + 20*0.5 + 30*0.3) / (0.2+0.5+0.3) = 21.0
        expected = 21.0
        np.testing.assert_almost_equal(result[0, 0, 0], expected, decimal=5)

    def test_mixed_valid_invalid_gates(self):
        """Test grid point with mix of valid and invalid gates."""
        geometry = GridGeometry(
            grid_shape=(1, 1, 1),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.array([0, 3], dtype=np.int32),
            gate_indices=np.array([0, 1, 2], dtype=np.int32),
            weights=np.array([0.3, 0.4, 0.3], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.masked_invalid(np.ma.array([10.0, np.nan, 30.0], dtype=np.float32))

        result = apply_geometry(geometry, field_data)

        # Should compute average using only valid gates
        # Expected: (10*0.3 + 30*0.3) / (0.3+0.3) = 20.0
        expected = 20.0
        np.testing.assert_almost_equal(result[0, 0, 0], expected, decimal=5)


class TestApplyGeometryWithInfValues:
    """Test handling of infinity values."""

    def test_inf_values_excluded(self):
        """Test that infinity values are excluded."""
        geometry = GridGeometry(
            grid_shape=(1, 1, 1),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.array([0, 3], dtype=np.int32),
            gate_indices=np.array([0, 1, 2], dtype=np.int32),
            weights=np.array([0.3, 0.4, 0.3], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.masked_invalid(np.ma.array([10.0, np.inf, 30.0], dtype=np.float32))

        result = apply_geometry(geometry, field_data)

        # Should exclude inf value
        assert np.isfinite(result[0, 0, 0])

    def test_negative_inf_values_excluded(self):
        """Test that negative infinity values are excluded."""
        geometry = GridGeometry(
            grid_shape=(1, 1, 1),
            grid_limits=((0, 1000), (-500, 500), (-500, 500)),
            indptr=np.array([0, 2], dtype=np.int32),
            gate_indices=np.array([0, 1], dtype=np.int32),
            weights=np.array([0.5, 0.5], dtype=np.float32),
            toa=2000.0,
        )

        field_data = np.ma.masked_invalid(np.ma.array([10.0, -np.inf], dtype=np.float32))

        result = apply_geometry(geometry, field_data)

        # Should exclude -inf value and use only valid gate
        np.testing.assert_almost_equal(result[0, 0, 0], 10.0, decimal=5)
