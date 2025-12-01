"""Integration tests for PyART radar conversion.

Tests the BUFR to PyART conversion using the new module structure.
"""

from pathlib import Path

import numpy as np
import pytest

from radarlib.io.bufr import bufr_to_dict
from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar


@pytest.mark.integration
class TestPyARTConversionConsistency:
    """Test consistency of PyART conversion between implementations."""

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

    @pytest.fixture
    def decoded_field(self, sample_bufr_file):
        """Get decoded field for testing."""
        decoded = bufr_to_dict(sample_bufr_file, root_resources=None)
        return decoded

    def test_radar_object_created(self, decoded_field):
        """Test that PyART radar objects are created successfully."""
        if decoded_field is None:
            pytest.skip("Could not decode BUFR file")

        # Use the decoded field directly with the pyart_writer
        radar = bufr_fields_to_pyart_radar([decoded_field])
        assert radar is not None
        assert hasattr(radar, "fields")

    def test_radar_object_structure_complete(self, decoded_field):
        """Test that radar object has all required components."""
        if decoded_field is None:
            pytest.skip("Could not decode BUFR file")

        radar = bufr_fields_to_pyart_radar([decoded_field])

        # Check required attributes
        required_attrs = [
            "fields",
            "metadata",
            "latitude",
            "longitude",
            "altitude",
            "azimuth",
            "elevation",
            "range",
            "time",
        ]
        for attr in required_attrs:
            assert hasattr(radar, attr), f"Radar missing attribute: {attr}"

    def test_radar_coordinates_reasonable(self, decoded_field):
        """Test that radar coordinate values are reasonable."""
        if decoded_field is None:
            pytest.skip("Could not decode BUFR file")

        radar = bufr_fields_to_pyart_radar([decoded_field])

        # Check latitude
        if hasattr(radar.latitude["data"], "__len__"):
            lat = radar.latitude["data"][0]
            assert -90 <= lat <= 90, f"Invalid latitude: {lat}"

        # Check longitude
        if hasattr(radar.longitude["data"], "__len__"):
            lon = radar.longitude["data"][0]
            assert -180 <= lon <= 180, f"Invalid longitude: {lon}"

        # Check altitude
        if hasattr(radar.altitude["data"], "__len__"):
            alt = radar.altitude["data"][0]
            assert -500 <= alt <= 5000, f"Invalid altitude: {alt}"

        # Check range values
        range_data = radar.range["data"]
        assert len(range_data) > 0, "Range has no data"
        assert np.all(range_data >= 0), "Range values should be non-negative"

    def test_field_dimensions_match_sweep_info(self, decoded_field):
        """Test that field dimensions match declared sweep information."""
        if decoded_field is None:
            pytest.skip("Could not decode BUFR file")

        radar = bufr_fields_to_pyart_radar([decoded_field])

        for field_name, field_dict in radar.fields.items():
            field_data = field_dict["data"]
            assert field_data.shape[0] == radar.nrays, (
                f"Field {field_name} has {field_data.shape[0]} rays " f"but radar has {radar.nrays} rays"
            )
            assert field_data.shape[1] == radar.ngates, (
                f"Field {field_name} has {field_data.shape[1]} gates " f"but radar has {radar.ngates} gates"
            )

    def test_field_data_masked_appropriately(self, decoded_field):
        """Test that field data is properly masked."""
        if decoded_field is None:
            pytest.skip("Could not decode BUFR file")

        radar = bufr_fields_to_pyart_radar([decoded_field])

        for field_name, field_dict in radar.fields.items():
            field_data = field_dict["data"]

            # If masked array, some values should be valid
            if isinstance(field_data, np.ma.MaskedArray):
                # Should have some unmasked data
                assert field_data.count() > 0, f"Field {field_name} is entirely masked"
            else:
                # Should have some finite values
                assert np.isfinite(field_data).any(), f"Field {field_name} has no finite values"

    def test_time_array_monotonic_per_sweep(self, decoded_field):
        """Test that time increases monotonically within each sweep."""
        if decoded_field is None:
            pytest.skip("Could not decode BUFR file")

        radar = bufr_fields_to_pyart_radar([decoded_field])

        if hasattr(radar, "time") and "data" in radar.time:
            time_data = radar.time["data"]

            for sweep_idx in range(radar.nsweeps):
                start_ray = radar.sweep_start_ray_index["data"][sweep_idx]
                end_ray = radar.sweep_end_ray_index["data"][sweep_idx] + 1

                sweep_times = time_data[start_ray:end_ray]
                # Times should be non-decreasing within sweep
                assert np.all(np.diff(sweep_times) >= 0), f"Times not monotonic in sweep {sweep_idx}"
