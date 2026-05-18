# -*- coding: utf-8 -*-
"""Unit tests for the v2 filename utilities in names_utils.py.

Tests cover:
- product_path_and_filename() with the new signature
- extract_cog_filename_components_v2()

Test bodies are intentionally left as stubs (pass) to be filled in as part
of the TDD cycle.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from radarlib.utils.names_utils import extract_cog_filename_components_v2, product_path_and_filename

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def ts_utc() -> datetime:
    """A known UTC datetime with second precision."""
    return datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# product_path_and_filename()
# ---------------------------------------------------------------------------


class TestProductPathAndFilename:
    def test_returns_path_object(self, ts_utc: datetime, tmp_path: Path) -> None:
        result = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert isinstance(result, Path)

    def test_filename_contains_radar_name(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert p.name.startswith("RMA1_")

    def test_filename_contains_strategy(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert "_0315_" in p.name

    def test_filename_contains_vol_nr(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert "_01_" in p.name

    def test_exact_timestamp_in_filename_by_default(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert "20260401T205000Z" in p.name

    def test_rounded_timestamp_when_round_filename_true(self, tmp_path: Path) -> None:
        # 20:53 rounded to nearest 10 min = 20:50 → "20260401T205000Z"
        ts = datetime(2026, 4, 1, 20, 53, 0, tzinfo=timezone.utc)
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_path, round_filename=True)
        assert "T205000Z" in p.name

    def test_filtered_field_has_no_o_suffix(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path, filtered=True)
        assert "DBZHo" not in p.name
        assert "DBZH.tif" in p.name

    def test_unfiltered_field_has_o_suffix(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path, filtered=False)
        assert "DBZHo.tif" in p.name

    def test_extension_is_always_tif(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert p.suffix == ".tif"

    def test_directory_structure_yyyy_mm_dd(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        # Path should include 2026/04/01
        assert "2026" in str(p)
        assert "04" in str(p)
        assert "01" in str(p)

    def test_radar_name_subdirectory_under_base_dir(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert p.parts[len(tmp_path.parts)] == "RMA1"

    def test_parent_directories_are_created(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        assert p.parent.is_dir()

    def test_full_path_example(self, ts_utc: datetime, tmp_path: Path) -> None:
        # ts_utc = 2026-04-01 20:50:00 UTC
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path)
        expected = tmp_path / "RMA1" / "2026" / "04" / "01" / "RMA1_0315_01_20260401T205000Z_DBZH.tif"
        assert p == expected

    def test_unfiltered_full_path_example(self, ts_utc: datetime, tmp_path: Path) -> None:
        p = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_utc, tmp_path, filtered=False)
        expected = tmp_path / "RMA1" / "2026" / "04" / "01" / "RMA1_0315_01_20260401T205000Z_DBZHo.tif"
        assert p == expected


# ---------------------------------------------------------------------------
# extract_cog_filename_components_v2()
# ---------------------------------------------------------------------------


class TestExtractCogFilenameComponentsV2:
    @pytest.mark.parametrize(
        "filename,expected",
        [
            (
                "RMA1_0315_01_20260401T205000Z_DBZH.tif",
                {
                    "radar_name": "RMA1",
                    "strategy": "0315",
                    "vol_nr": "01",
                    "timestamp": "20260401T205000Z",
                    "field_name": "DBZH",
                    "filtered": True,
                },
            ),
            (
                "RMA1_0315_01_20260401T205000Z_DBZHo.tif",
                {
                    "radar_name": "RMA1",
                    "strategy": "0315",
                    "vol_nr": "01",
                    "timestamp": "20260401T205000Z",
                    "field_name": "DBZH",
                    "filtered": False,
                },
            ),
            (
                "RMA6_0315_02_20260422T021351Z_COLMAX.tif",
                {
                    "radar_name": "RMA6",
                    "strategy": "0315",
                    "vol_nr": "02",
                    "timestamp": "20260422T021351Z",
                    "field_name": "COLMAX",
                    "filtered": True,
                },
            ),
            (
                "RMA6_0315_02_20260422T021351Z_COLMAXo.tif",
                {
                    "radar_name": "RMA6",
                    "strategy": "0315",
                    "vol_nr": "02",
                    "timestamp": "20260422T021351Z",
                    "field_name": "COLMAX",
                    "filtered": False,
                },
            ),
        ],
    )
    def test_parametrized_valid_filenames(self, filename: str, expected: dict) -> None:
        assert extract_cog_filename_components_v2(filename) == expected

    def test_radar_name_extracted(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZH.tif")
        assert result["radar_name"] == "RMA1"

    def test_strategy_extracted(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZH.tif")
        assert result["strategy"] == "0315"

    def test_vol_nr_extracted(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZH.tif")
        assert result["vol_nr"] == "01"

    def test_timestamp_extracted(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZH.tif")
        assert result["timestamp"] == "20260401T205000Z"

    def test_field_name_extracted_without_o_suffix(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZHo.tif")
        assert result["field_name"] == "DBZH"

    def test_filtered_true_when_no_o_suffix(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZH.tif")
        assert result["filtered"] is True

    def test_filtered_false_when_o_suffix_present(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZHo.tif")
        assert result["filtered"] is False

    def test_invalid_filename_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            extract_cog_filename_components_v2("not_a_valid_filename.tif")

    def test_old_v1_format_raises_value_error(self) -> None:
        # v1 format: RMA1_20260401T205000Z_DBZH_00.tif (no strategy/vol_nr tokens)
        with pytest.raises(ValueError):
            extract_cog_filename_components_v2("RMA1_20260401T205000Z_DBZH_00.tif")

    def test_error_message_includes_filename(self) -> None:
        bad_name = "bad_format_file.tif"
        with pytest.raises(ValueError, match=bad_name):
            extract_cog_filename_components_v2(bad_name)

    def test_round_trip_filtered(self, tmp_path: Path) -> None:
        """Generate a v2 filename then parse it back — components must be identical."""
        ts = datetime(2026, 4, 22, 2, 13, 51, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA6", "0315", "02", "ZDR", ts, tmp_path, filtered=True)
        components = extract_cog_filename_components_v2(path.name)
        assert components["radar_name"] == "RMA6"
        assert components["strategy"] == "0315"
        assert components["vol_nr"] == "02"
        assert components["field_name"] == "ZDR"
        assert components["filtered"] is True

    def test_round_trip_unfiltered(self, tmp_path: Path) -> None:
        """Generate an unfiltered v2 filename then parse it back."""
        ts = datetime(2026, 4, 22, 2, 13, 51, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA6", "0315", "02", "ZDR", ts, tmp_path, filtered=False)
        components = extract_cog_filename_components_v2(path.name)
        assert components["field_name"] == "ZDR"
        assert components["filtered"] is False
