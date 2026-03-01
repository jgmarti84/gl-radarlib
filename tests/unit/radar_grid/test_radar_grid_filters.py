"""
Unit tests for radar_grid.filters module.

Tests the GateFilter class for filtering radar gates.
"""

from unittest.mock import Mock

import numpy as np
import pytest

from radarlib.radar_grid.filters import GateFilter, create_mask_from_filter


class TestGateFilter:
    """Test GateFilter class."""

    @pytest.fixture
    def mock_radar(self):
        """Create a mock radar object for testing."""
        radar = Mock()
        radar.nrays = 100
        radar.ngates = 500
        radar.fields = {
            "DBZH": {"data": np.random.randn(100, 500).astype(np.float32) * 20 + 10},
            "RHOHV": {"data": np.random.rand(100, 500).astype(np.float32) * 0.3 + 0.7},
            "ZDR": {"data": np.random.randn(100, 500).astype(np.float32) * 2 + 1},
        }
        return radar

    def test_create_gate_filter(self, mock_radar):
        """Test creating a GateFilter."""
        gf = GateFilter(mock_radar)

        assert gf.radar == mock_radar
        assert gf.n_gates == 100 * 500
        assert gf.n_excluded() == 0
        assert gf.n_included() == 50000

    def test_gate_excluded_property(self, mock_radar):
        """Test gate_excluded property."""
        gf = GateFilter(mock_radar)
        mask = gf.gate_excluded

        assert isinstance(mask, np.ndarray)
        assert mask.dtype == bool
        assert mask.shape == (50000,)
        assert not np.any(mask)  # All False initially

    def test_gate_included_property(self, mock_radar):
        """Test gate_included property."""
        gf = GateFilter(mock_radar)
        mask = gf.gate_included

        assert isinstance(mask, np.ndarray)
        assert mask.dtype == bool
        assert np.all(mask)  # All True initially

        # Should be inverse of gate_excluded
        np.testing.assert_array_equal(mask, ~gf.gate_excluded)

    def test_exclude_below(self, mock_radar):
        """Test exclude_below method."""
        gf = GateFilter(mock_radar)
        gf.exclude_below("DBZH", -10.0)

        # Some gates should be excluded
        assert gf.n_excluded() > 0

        # Check that excluded gates have values below threshold
        field_data = mock_radar.fields["DBZH"]["data"].ravel()
        excluded_values = field_data[gf.gate_excluded]

        # All excluded values should be below threshold
        assert np.all(excluded_values < -10.0)

    def test_exclude_above(self, mock_radar):
        """Test exclude_above method."""
        gf = GateFilter(mock_radar)
        gf.exclude_above("RHOHV", 0.95)

        # Some gates should be excluded
        assert gf.n_excluded() > 0

        # Check that excluded gates have values above threshold
        field_data = mock_radar.fields["RHOHV"]["data"].ravel()
        excluded_values = field_data[gf.gate_excluded]

        # All excluded values should be above threshold
        assert np.all(excluded_values > 0.95)

    def test_exclude_outside(self, mock_radar):
        """Test exclude_outside method."""
        gf = GateFilter(mock_radar)
        gf.exclude_outside("ZDR", -1.0, 5.0)

        # Check that excluded gates are outside range
        field_data = mock_radar.fields["ZDR"]["data"].ravel()
        excluded_values = field_data[gf.gate_excluded]

        # All excluded values should be outside range
        assert np.all((excluded_values < -1.0) | (excluded_values > 5.0))

    def test_exclude_equal(self, mock_radar):
        """Test exclude_equal method."""
        # Add some exact values
        mock_radar.fields["DBZH"]["data"][0, 0:10] = 0.0

        gf = GateFilter(mock_radar)
        gf.exclude_equal("DBZH", 0.0)

        # At least those 10 gates should be excluded
        assert gf.n_excluded() >= 10

    def test_exclude_invalid(self, mock_radar):
        """Test exclude_invalid method."""
        # Add some NaN and Inf values
        mock_radar.fields["DBZH"]["data"][0, 0:5] = np.nan
        mock_radar.fields["DBZH"]["data"][0, 5:10] = np.inf
        mock_radar.fields["DBZH"]["data"][0, 10:15] = -np.inf

        gf = GateFilter(mock_radar)
        gf.exclude_invalid("DBZH")

        # At least 15 gates should be excluded
        assert gf.n_excluded() >= 15

        # Check that excluded gates include the NaN/Inf ones
        # field_data = mock_radar.fields["DBZH"]["data"].ravel()
        # first_15 = field_data[0:15]
        excluded_first_15 = gf.gate_excluded[0:15]

        # All first 15 should be excluded
        assert np.all(excluded_first_15)

    def test_multiple_filters_combined(self, mock_radar):
        """Test that multiple filters are combined with OR logic."""
        gf = GateFilter(mock_radar)

        initial_excluded = gf.n_excluded()

        gf.exclude_below("DBZH", -10.0)
        after_first = gf.n_excluded()

        gf.exclude_above("RHOHV", 0.95)
        after_second = gf.n_excluded()

        # Number of excluded gates should increase or stay same
        assert after_first >= initial_excluded
        assert after_second >= after_first

    def test_summary(self, mock_radar):
        """Test summary method."""
        gf = GateFilter(mock_radar)
        gf.exclude_below("DBZH", 0.0)

        summary = gf.summary()

        assert "GateFilter Summary" in summary
        assert "Total gates: 50,000" in summary
        assert "Excluded:" in summary
        assert "Included:" in summary

    def test_reset(self, mock_radar):
        """Test reset method."""
        gf = GateFilter(mock_radar)
        gf.exclude_below("DBZH", 0.0)

        # Should have some excluded gates
        assert gf.n_excluded() > 0

        # Reset
        gf.reset()

        # Should be back to all included
        assert gf.n_excluded() == 0
        assert gf.n_included() == 50000


class TestCreateMaskFromFilter:
    """Test create_mask_from_filter helper function."""

    @pytest.fixture
    def mock_radar(self):
        """Create a mock radar object."""
        radar = Mock()
        radar.nrays = 100
        radar.ngates = 500
        radar.fields = {"DBZH": {"data": np.random.randn(100, 500).astype(np.float32) * 20 + 10}}
        return radar

    def test_create_mask_from_filter(self, mock_radar):
        """Test creating mask from filter."""
        gf = GateFilter(mock_radar)
        gf.exclude_below("DBZH", 0.0)

        field_data, mask = create_mask_from_filter(mock_radar, "DBZH", gf)

        assert isinstance(field_data, np.ndarray)
        assert isinstance(mask, np.ndarray)
        assert field_data.dtype == np.float32
        assert mask.dtype == bool
        assert field_data.shape == (50000,)
        assert mask.shape == (50000,)

        # Should match gate_excluded
        np.testing.assert_array_equal(mask, gf.gate_excluded)


class TestGateFilterEdgeCases:
    """Test edge cases for GateFilter."""

    def test_empty_radar(self):
        """Test with a radar that has no gates."""
        radar = Mock()
        radar.nrays = 0
        radar.ngates = 0
        radar.fields = {}

        gf = GateFilter(radar)
        assert gf.n_gates == 0
        assert gf.n_excluded() == 0
        assert gf.n_included() == 0

    def test_exclude_all_gates(self):
        """Test excluding all gates."""
        radar = Mock()
        radar.nrays = 10
        radar.ngates = 10
        radar.fields = {"DBZH": {"data": np.ones((10, 10), dtype=np.float32) * 10}}

        gf = GateFilter(radar)
        gf.exclude_below("DBZH", 100.0)  # All values are below 100

        assert gf.n_excluded() == 100
        assert gf.n_included() == 0


class TestGateFilterMaskedArrays:
    """Test GateFilter with masked arrays."""

    def test_exclude_with_masked_data(self):
        """Test that filter works with masked arrays."""
        radar = Mock()
        radar.nrays = 10
        radar.ngates = 10

        data = np.ma.array(np.random.randn(10, 10).astype(np.float32) * 10, mask=np.zeros((10, 10), dtype=bool))
        data.mask[0:2, :] = True  # Mask first 2 rows

        radar.fields = {"DBZH": {"data": data}}

        gf = GateFilter(radar)
        gf.exclude_below("DBZH", 0.0)

        # Should work without errors
        assert gf.n_excluded() >= 0
