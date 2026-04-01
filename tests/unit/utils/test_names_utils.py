"""Unit tests for radarlib.utils.names_utils module."""

import datetime
import os
from datetime import timezone

from radarlib.utils import names_utils


class TestGetTimeFromRMAFilename:
    """Test get_time_from_RMA_filename() function.

    Note: The function expects format RADAR_ELEV_SWEEP_TIMESTAMP.ext (4 parts),
    but actual BUFR files use RADAR_ELEV_SWEEP_FIELD_TIMESTAMP.ext (5 parts).
    """

    def test_basic_filename_parsing_4_parts(self):
        """Test parsing RMA filename with 4-part format (without field name)."""
        # This is the format the function currently expects
        filename = "RMA5_0315_1_20240101T120000Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename)

        assert isinstance(result, datetime.datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1
        assert result.hour == 12
        assert result.minute == 0
        assert result.second == 0
        assert result.tzinfo == timezone.utc

    def test_different_datetime_values_4_parts(self):
        """Test parsing with different datetime values (4-part format)."""
        filename = "AR5_1000_2_20231225T235959Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename)

        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59

    def test_timezone_utc_true(self):
        """Test that tz_UTC=True returns UTC timezone."""
        filename = "RMA1_0315_1_20240615T143022Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename, tz_UTC=True)

        assert result.tzinfo == timezone.utc

    def test_timezone_utc_false(self):
        """Test that tz_UTC=False returns Argentina timezone."""
        filename = "RMA1_0315_1_20240615T143022Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename, tz_UTC=False)

        # Should be converted to Argentina timezone
        assert result.tzinfo is not None
        assert result.tzinfo != timezone.utc

    def test_filename_without_extension(self):
        """Test parsing filename without .bufr extension."""
        filename = "RMA5_0315_1_20240101T120000Z"
        result = names_utils.get_time_from_RMA_filename(filename)

        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1


class TestGetPathFromRMAFilename:
    """Test get_path_from_RMA_filename() function.

    Note: This function expects 4-part format: RADAR_ELEV_SWEEP_TIMESTAMP.ext
    (without field name). The actual BUFR files have 5 parts but this function
    is not actively used in the codebase (all usage is commented out).
    """

    def test_basic_path_generation_4_parts(self):
        """Test generating path from RMA filename with 4-part format."""
        filename = "RMA5_0315_1_20240615T143022Z.bufr"
        result = names_utils.get_path_from_RMA_filename(filename)

        # Should contain radar name
        assert "RMA5" in result
        # Should contain year
        assert "2024" in result
        # Should contain month
        assert "06" in result
        # Should contain day
        assert "15" in result
        # Should contain hour
        assert "14" in result

    def test_path_structure_4_parts(self):
        """Test that path has correct structure."""
        filename = "AR5_1000_2_20231225T235959Z.bufr"
        result = names_utils.get_path_from_RMA_filename(filename)

        # Path should follow pattern: .../radar/year/month/day/hour
        parts = result.split(os.sep)
        assert "AR5" in parts
        assert "2023" in parts
        assert "12" in parts
        assert "25" in parts
        assert "23" in parts

    def test_custom_root_radar_files_4_parts(self):
        """Test using custom root_radar_files parameter."""
        filename = "RMA1_0315_1_20240101T120000Z.bufr"
        custom_root = "/custom/radar/root"
        result = names_utils.get_path_from_RMA_filename(filename, root_radar_files=custom_root)

        assert result.startswith(custom_root)

    def test_different_radars_4_parts(self):
        """Test path generation for different radar names."""
        filenames = [
            "RMA1_0315_1_20240101T120000Z.bufr",
            "RMA5_0315_1_20240101T120000Z.bufr",
            "AR5_1000_1_20240101T120000Z.bufr",
        ]

        for filename in filenames:
            result = names_utils.get_path_from_RMA_filename(filename)
            radar_name = filename.split("_")[0]
            assert radar_name in result


class TestGetNetcdfFilenameFromBufrFilename:
    """Test get_netcdf_filename_from_bufr_filename() function.

    The function creates format: RADAR_ELEV_SWEEP_TIMESTAMP.nc from
    input format: RADAR_ELEV_SWEEP_FIELD_TIMESTAMP.ext
    (i.e., it skips the field name in the middle)
    """

    def test_basic_conversion(self):
        """Test basic BUFR to NetCDF filename conversion."""
        bufr_filename = "RMA5_0315_1_DBZH_20240101T120000Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)

        # Function skips field name and creates: RMA5_0315_1_20240101T120000Z.nc
        assert result == "RMA5_0315_1_20240101T120000Z.nc"
        assert result.endswith(".nc")
        assert "RMA5" in result
        assert "0315" in result
        assert "1" in result
        assert "20240101T120000Z" in result

    def test_removes_extension(self):
        """Test that .bufr extension is removed."""
        bufr_filename = "AR5_1000_2_VRAD_20231225T235959Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)

        assert not result.endswith(".bufr")
        assert result.endswith(".nc")
        assert result == "AR5_1000_2_20231225T235959Z.nc"

    def test_field_name_not_in_output(self):
        """Test that field name is not included in output (intentional behavior)."""
        bufr_filename = "RMA1_0315_3_ZDR_20240615T143022Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)

        # Field name "ZDR" should NOT be in the output
        # (the function skips parts[3] which is the field name)
        assert "ZDR" not in result
        assert result == "RMA1_0315_3_20240615T143022Z.nc"

    def test_timestamp_preserved(self):
        """Test that timestamp is preserved in output."""
        bufr_filename = "RMA5_0315_1_DBZH_20240101T120000Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)

        assert "20240101T120000Z" in result
        assert result.endswith(".nc")


class TestTimezones:
    """Test timezone handling."""

    def test_utc_timezone_constant(self):
        """Test that UTC timezone constant is defined."""
        assert hasattr(names_utils, "tz_utc")
        assert names_utils.tz_utc is not None

    def test_argentina_timezone_constant(self):
        """Test that Argentina timezone constant is defined."""
        assert hasattr(names_utils, "tz_arg")
        assert names_utils.tz_arg is not None


class TestExtractCogFilenameComponents:
    """Tests for extract_cog_filename_components() function.

    COG filename format: RADAR_TIMESTAMP_FIELD[o]_SWEEP.tif
    The 'o' suffix marks a NON-filtered product; its absence marks a filtered one.
    """

    # ------------------------------------------------------------------
    # Happy-path: filtered products (no 'o' suffix)
    # ------------------------------------------------------------------

    def test_filtered_filename_basic(self):
        """Filtered filename (no 'o') should yield filtered=True."""
        result = names_utils.extract_cog_filename_components("RMA1_20260326T200000Z_VRAD_00.tif")

        assert result["radar_name"] == "RMA1"
        assert result["timestamp"] == "20260326T200000Z"
        assert result["field_type"] == "VRAD"
        assert result["sweep"] == "00"
        assert result["filtered"] is True

    def test_nonfiltered_filename_basic(self):
        """Non-filtered filename (with 'o' suffix) should yield filtered=False."""
        result = names_utils.extract_cog_filename_components("RMA1_20260326T200000Z_VRADo_00.tif")

        assert result["radar_name"] == "RMA1"
        assert result["timestamp"] == "20260326T200000Z"
        # field_type should NOT include the trailing 'o'
        assert result["field_type"] == "VRAD"
        assert result["sweep"] == "00"
        assert result["filtered"] is False

    def test_filtered_dbzh(self):
        """DBZH filtered product."""
        result = names_utils.extract_cog_filename_components("RMA1_20260326T200000Z_DBZH_01.tif")

        assert result["field_type"] == "DBZH"
        assert result["filtered"] is True
        assert result["sweep"] == "01"

    def test_nonfiltered_dbzh(self):
        """DBZHo non-filtered product."""
        result = names_utils.extract_cog_filename_components("RMA1_20260326T200000Z_DBZHo_01.tif")

        assert result["field_type"] == "DBZH"
        assert result["filtered"] is False
        assert result["sweep"] == "01"

    def test_various_field_types(self):
        """Should parse all common field types correctly."""
        field_cases = ["DBZH", "DBZV", "VRAD", "WRAD", "ZDR", "RHOHV", "KDP", "PHIDP", "COLMAX"]
        for field in field_cases:
            filename = f"RMA1_20260326T200000Z_{field}_00.tif"
            result = names_utils.extract_cog_filename_components(filename)
            assert result["field_type"] == field, f"Failed for field '{field}'"
            assert result["filtered"] is True

    def test_various_nonfiltered_field_types(self):
        """Should parse all common non-filtered field types (with 'o') correctly."""
        field_cases = ["DBZH", "VRAD", "ZDR", "RHOHV"]
        for field in field_cases:
            filename = f"RMA1_20260326T200000Z_{field}o_00.tif"
            result = names_utils.extract_cog_filename_components(filename)
            assert result["field_type"] == field, f"Failed for field '{field}'"
            assert result["filtered"] is False

    def test_different_radar_names(self):
        """Should extract different radar name prefixes."""
        cases = [
            ("RMA1_20260326T200000Z_DBZH_00.tif", "RMA1"),
            ("RMA5_20260326T200000Z_DBZH_00.tif", "RMA5"),
            ("RMA11_20260326T200000Z_DBZH_00.tif", "RMA11"),
        ]
        for filename, expected_radar in cases:
            result = names_utils.extract_cog_filename_components(filename)
            assert result["radar_name"] == expected_radar, f"Failed for {filename}"

    def test_timestamp_extracted_correctly(self):
        """Timestamp should be extracted verbatim."""
        result = names_utils.extract_cog_filename_components("RMA1_20260326T123456Z_DBZH_00.tif")
        assert result["timestamp"] == "20260326T123456Z"

    def test_different_sweep_numbers(self):
        """Sweep numbers 00-99 should be extracted as zero-padded strings."""
        for _, sweep_str in [(0, "00"), (1, "01"), (9, "09"), (10, "10"), (99, "99")]:
            filename = f"RMA1_20260326T200000Z_DBZH_{sweep_str}.tif"
            result = names_utils.extract_cog_filename_components(filename)
            assert result["sweep"] == sweep_str, f"Failed for sweep {sweep_str}"

    # ------------------------------------------------------------------
    # Edge cases: invalid / malformed filenames
    # ------------------------------------------------------------------

    def test_invalid_filename_returns_all_nones(self):
        """Unrecognised filename should return a dict with all None values."""
        result = names_utils.extract_cog_filename_components("not_a_valid_cog_filename.txt")

        assert result["radar_name"] is None
        assert result["timestamp"] is None
        assert result["field_type"] is None
        assert result["sweep"] is None
        assert result["filtered"] is None

    def test_empty_string_returns_all_nones(self):
        """Empty string should return all None values."""
        result = names_utils.extract_cog_filename_components("")

        assert all(v is None for v in result.values())

    def test_missing_extension_returns_nones(self):
        """Filename without .tif extension should not match."""
        result = names_utils.extract_cog_filename_components("RMA1_20260326T200000Z_DBZH_00")

        assert result["radar_name"] is None

    def test_wrong_extension_returns_nones(self):
        """Filename with .nc extension should not match COG pattern."""
        result = names_utils.extract_cog_filename_components("RMA1_20260326T200000Z_DBZH_00.nc")

        assert result["radar_name"] is None

    def test_returns_dict_with_expected_keys(self):
        """Return value should always be a dict with the 5 expected keys."""
        for filename in [
            "RMA1_20260326T200000Z_DBZH_00.tif",
            "bad_filename.txt",
            "",
        ]:
            result = names_utils.extract_cog_filename_components(filename)
            assert isinstance(result, dict)
            assert set(result.keys()) == {"radar_name", "timestamp", "field_type", "sweep", "filtered"}
