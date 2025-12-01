"""Integration tests for BUFR file decoding.

Tests the BUFR decoding functionality using radarlib.io.bufr module.
"""

from pathlib import Path

import numpy as np
import pytest

from radarlib.io.bufr import bufr_to_dict


@pytest.mark.integration
class TestBUFRDecodingConsistency:
    """Test consistency of BUFR decoding between implementations."""

    @pytest.fixture
    def sample_bufr_file(self):
        """Get sample BUFR file path."""
        bufr_path = Path("tests/data/bufr")
        if not bufr_path.exists():
            pytest.skip("No BUFR test data available")

        bufr_files = list(bufr_path.glob("*.BUFR"))
        if not bufr_files:
            pytest.skip("No BUFR test files found")

        return str(bufr_files[0])

    def test_decoded_dict_structure_matches(self, sample_bufr_file):
        """Test that decoded BUFR dict has all expected keys."""
        decoded = bufr_to_dict(sample_bufr_file, root_resources=None)

        # Current API returns a single field dict with 'data' and 'info' keys
        assert decoded is not None, "Decoded result is None"
        assert "data" in decoded, "Missing 'data' key"
        assert "info" in decoded, "Missing 'info' key"
        assert isinstance(decoded["info"], dict), "'info' is not a dict"

        # Check info structure
        info = decoded["info"]
        expected_info_keys = [
            "nombre_radar",
            "tipo_producto",
            "lat",
            "lon",
            "altura",
            "nsweeps",
        ]
        for key in expected_info_keys:
            assert key in info, f"Missing info key: {key}"

    def test_decoded_field_data_is_array(self, sample_bufr_file):
        """Test that decoded field contains data array."""
        decoded = bufr_to_dict(sample_bufr_file, root_resources=None)

        assert decoded is not None
        assert "data" in decoded

        data = decoded["data"]
        assert isinstance(data, (np.ndarray, np.ma.MaskedArray)), "Data is not ndarray"
        assert data.ndim == 2, f"Data should be 2D, got {data.ndim}D"
        assert data.shape[0] > 0 and data.shape[1] > 0, "Data has zero dimension"

    def test_decoded_field_values_valid(self, sample_bufr_file):
        """Test that decoded field values are physically reasonable."""
        decoded = bufr_to_dict(sample_bufr_file, root_resources=None)

        data = decoded["data"]  # type: ignore

        # Should have some non-masked/non-NaN values
        if isinstance(data, np.ma.MaskedArray):
            assert data.count() > 0, "Data is entirely masked"
            valid_data = data.compressed()
        else:
            assert np.isfinite(data).any(), "Data has no finite values"
            valid_data = data[np.isfinite(data)]

        if len(valid_data) > 0:
            # Filter out sentinel values used by BUFR decoder for missing data
            # The BUFR decoder uses -1.797693134862315708e+308 as a missing value marker
            valid_data = valid_data[valid_data > -1.7e308]

            if len(valid_data) > 0:
                # Check value ranges - meteorological radar reflectivity is typically -20 to 80 dBZ
                assert valid_data.min() > -100, f"Unreasonably low values: {valid_data.min()}"
                assert valid_data.max() < 200, f"Unreasonably high values: {valid_data.max()}"

    def test_decoded_metadata_values_reasonable(self, sample_bufr_file):
        """Test that decoded metadata values are reasonable."""
        decoded = bufr_to_dict(sample_bufr_file, root_resources=None)
        info = decoded["info"]  # type: ignore

        # Date/time checks from 'ano_vol', 'mes_vol', etc.
        assert 1900 <= info.get("ano_vol", 2024) <= 2100, "Year out of range"
        assert 1 <= info.get("mes_vol", 1) <= 12, "Month out of range"
        assert 1 <= info.get("dia_vol", 1) <= 31, "Day out of range"
        assert 0 <= info.get("hora_vol", 0) <= 23, "Hour out of range"
        assert 0 <= info.get("min_vol", 0) <= 59, "Minute out of range"

        # Geographic checks
        lat = info.get("lat")
        lon = info.get("lon")
        alt = info.get("altura")

        assert -90 <= lat <= 90, f"Latitude out of range: {lat}"
        assert -180 <= lon <= 180, f"Longitude out of range: {lon}"
        assert alt >= -500, f"Altitude too low: {alt}"
        assert alt <= 5000, f"Altitude too high: {alt}"

        # Sweep count
        nsweeps = info.get("nsweeps", 1)
        assert nsweeps >= 1, "Must have at least 1 sweep"
        assert nsweeps <= 30, f"Unusually high sweep count: {nsweeps}"
