"""End-to-end tests for the new BUFR to PyART conversion API.

This module contains tests for the modern implementation using:
- bufr_to_dict() from radarlib.io.bufr (new API with legacy=False)
- bufr_to_pyart() and bufr_fields_to_pyart_radar() from radarlib.io.bufr

Note: The old import path radarlib.io.bufr.pyart_writer is deprecated.
      Use radarlib.io.bufr.bufr_to_pyart or radarlib.io.bufr instead.
"""

from pathlib import Path

import pytest


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:numpy.ndarray size changed.*:RuntimeWarning")
def test_end_to_end_bufr_to_pyart(tmp_save_path: Path):
    # Locate sample BUFR files under tests/data/bufr
    data_dir = Path("tests/data/bufr")
    if not data_dir.exists():
        pytest.skip("No BUFR test data available")

    bufr_files = list(data_dir.glob("*.BUFR"))
    if not bufr_files:
        pytest.skip("No BUFR test files found")

    # Use new import path
    from radarlib.io.bufr.bufr_to_pyart import bufr_paths_to_pyart
    from radarlib.utils.names_utils import get_netcdf_filename_from_bufr_filename

    results = bufr_paths_to_pyart([str(bufr_files[0])], root_resources=None, save_path=tmp_save_path)
    assert results
    # Use canonical naming function to compute expected NetCDF filename
    netcdf_name = get_netcdf_filename_from_bufr_filename(bufr_files[0].name)
    out_file = tmp_save_path / netcdf_name
    assert out_file.exists()


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:numpy.ndarray size changed.*:RuntimeWarning")
def test_end_to_end_bufr_multiple_files_to_pyart_radar(tmp_path: Path):
    """Test decoding multiple BUFR files and combining them into a single Radar object.

    This test:
    1. Locates all BUFR files in tests/data/bufr/RMA5/
    2. Decodes each BUFR file using bufr_to_dict()
    3. Collects the decoded dicts into a list
    4. Passes the list to bufr_fields_to_pyart_radar() to create a combined Radar object
    5. Validates the resulting Radar object
    """
    # Locate BUFR files in RMA5 subdirectory
    data_dir = Path("tests/data/bufr/RMA5")
    if not data_dir.exists():
        pytest.skip("No BUFR RMA5 test data available")

    bufr_files = sorted(list(data_dir.glob("*.BUFR")))
    if not bufr_files:
        pytest.skip("No BUFR test files found in RMA5 directory")

    # Use new import paths
    from radarlib.io.bufr import bufr_to_dict
    from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar

    # try:
    #     import pyart
    # except ImportError:
    #     pytest.skip("pyart not installed")
    # Decode all BUFR files into a list of field dicts
    decoded_fields = []
    for bufr_file in bufr_files:
        decoded = bufr_to_dict(str(bufr_file), root_resources=None)
        if decoded is not None:
            decoded_fields.append(decoded)

    if not decoded_fields:
        pytest.skip("Could not decode any BUFR files from RMA5 directory")

    # Convert the list of decoded fields to a PyART Radar object
    radar = bufr_fields_to_pyart_radar(decoded_fields)

    # Validate the resulting Radar object
    assert radar is not None, "Radar object is None"
    assert hasattr(radar, "fields"), "Radar object missing 'fields' attribute"
    assert len(radar.fields) > 0, "Radar object has no fields"

    # Check that radar has all required components
    required_attrs = [
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

    # Validate field data
    for field_name, field_dict in radar.fields.items():
        assert "data" in field_dict, f"Field '{field_name}' missing 'data' key"
        field_data = field_dict["data"]
        assert field_data is not None, f"Field '{field_name}' data is None"
        assert field_data.size > 0, f"Field '{field_name}' data is empty"
        # Check dimensions match radar geometry
        assert (
            field_data.shape[0] == radar.nrays
        ), f"Field '{field_name}' rays {field_data.shape[0]} != radar rays {radar.nrays}"
        assert (
            field_data.shape[1] == radar.ngates
        ), f"Field '{field_name}' gates {field_data.shape[1]} != radar gates {radar.ngates}"

    # Validate coordinate data
    assert radar.latitude["data"] is not None, "Latitude data is None"
    assert radar.longitude["data"] is not None, "Longitude data is None"
    assert radar.altitude["data"] is not None, "Altitude data is None"
    assert radar.range["data"] is not None, "Range data is None"
    assert radar.azimuth["data"] is not None, "Azimuth data is None"
    assert radar.elevation["data"] is not None, "Elevation data is None"

    # Validate ranges
    if hasattr(radar.latitude["data"], "__len__"):
        lat = radar.latitude["data"][0]
        assert -90 <= lat <= 90, f"Invalid latitude: {lat}"

    if hasattr(radar.longitude["data"], "__len__"):
        lon = radar.longitude["data"][0]
        assert -180 <= lon <= 180, f"Invalid longitude: {lon}"

    # Validate number of fields matches number of decoded files
    assert len(radar.fields) == len(decoded_fields), (
        f"Number of fields ({len(radar.fields)}) doesn't match " f"number of decoded files ({len(decoded_fields)})"
    )

    print(f"✓ Successfully combined {len(decoded_fields)} BUFR files into single Radar object")
    print(f"  Fields: {list(radar.fields.keys())}")
    print(f"  Radar geometry: {radar.nrays} rays × {radar.ngates} gates × {radar.nsweeps} sweeps")
