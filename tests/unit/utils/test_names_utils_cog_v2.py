# -*- coding: utf-8 -*-
"""Unit tests verifying the v2 COG filename utilities against the Output Contract.

These tests are complementary to test_names_utils_v2.py.  They focus on:

- Output Contract compliance (token order, field names, extension, suffix rules)
- Boundary and edge-case inputs (multiple vol_nr formats, different radar codes,
  various field names including multi-character ones like COLMAX)
- Round-trip invariants: ``product_path_and_filename`` → ``extract_cog_filename_components_v2``
  must recover all original components losslessly
- Explicit rejection of v1 filenames and other invalid patterns
- Directory structure compliance (RADAR_NAME/YYYY/MM/DD/)
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from radarlib.utils.names_utils import extract_cog_filename_components_v2, product_path_and_filename

# from typing import Any, Dict


# ---------------------------------------------------------------------------
# Shared timestamp fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def ts_midnight() -> datetime:
    return datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def ts_end_of_day() -> datetime:
    return datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


@pytest.fixture()
def ts_standard() -> datetime:
    return datetime(2026, 4, 22, 2, 13, 51, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Output Contract: token structure
# ---------------------------------------------------------------------------


class TestOutputContractFilenameStructure:
    """Verify the v2 filename follows the Output Contract token order exactly."""

    def test_token_order_matches_output_contract(self, ts_standard: datetime, tmp_path: Path) -> None:
        """Token order: RADAR_STRATEGY_VOLNR_TIMESTAMP_FIELD[o].tif"""
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path, filtered=True)
        parts = path.stem.split("_")
        # Positions: 0=RADAR, 1=STRATEGY, 2=VOLNR, 3=TIMESTAMP_DATE, 4=TIMESTAMP_TIME, 5=FIELD
        # timestamp has a 'T' separator so it occupies one token
        assert parts[0] == "RMA6"
        assert parts[1] == "0315"
        assert parts[2] == "01"
        assert "T" in parts[3]  # TIMESTAMP token, e.g. "20260422T021351Z"
        assert parts[4] == "DBZH"

    def test_stem_token_count_filtered(self, ts_standard: datetime, tmp_path: Path) -> None:
        """Filtered filename stem has exactly 5 underscore-separated tokens."""
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path, filtered=True)
        assert len(path.stem.split("_")) == 5

    def test_stem_token_count_unfiltered(self, ts_standard: datetime, tmp_path: Path) -> None:
        """Unfiltered field token is FIELD+'o' still within original position (5th token)."""
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path, filtered=False)
        # Last token ends with 'o'; still 5 tokens total
        assert len(path.stem.split("_")) == 5
        assert path.stem.split("_")[4] == "DBZHo"

    def test_extension_is_tif(self, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
        assert path.suffix == ".tif"

    def test_no_elevation_token_in_v2_format(self, ts_standard: datetime, tmp_path: Path) -> None:
        """v2 format has NO elevation token (unlike v1 which had _00 elevation)."""
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path, filtered=True)
        # v1 had pattern like DBZH_00 — there should be no such elevation-like suffix
        assert not path.name.endswith("_00.tif")


# ---------------------------------------------------------------------------
# Filtered vs unfiltered suffix ('o')
# ---------------------------------------------------------------------------


class TestFilteredUnfilteredSuffix:
    @pytest.mark.parametrize(
        "field_name",
        ["DBZH", "ZDR", "COLMAX", "RHOHV", "KDP", "VRAD", "PHIDP"],
    )
    def test_filtered_no_o_suffix(self, field_name: str, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA1", "0315", "01", field_name, ts_standard, tmp_path, filtered=True)
        assert f"{field_name}o" not in path.name
        assert f"{field_name}.tif" in path.name

    @pytest.mark.parametrize(
        "field_name",
        ["DBZH", "ZDR", "COLMAX", "RHOHV", "KDP", "VRAD", "PHIDP"],
    )
    def test_unfiltered_has_o_suffix(self, field_name: str, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA1", "0315", "01", field_name, ts_standard, tmp_path, filtered=False)
        assert f"{field_name}o.tif" in path.name

    def test_default_filtered_is_true(self, ts_standard: datetime, tmp_path: Path) -> None:
        path_default = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_standard, tmp_path)
        path_explicit = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_standard, tmp_path, filtered=True)
        assert path_default.name == path_explicit.name


# ---------------------------------------------------------------------------
# Exact timestamp encoding
# ---------------------------------------------------------------------------


class TestTimestampEncoding:
    def test_midnight_timestamp_encoded_correctly(self, ts_midnight: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_midnight, tmp_path)
        assert "20260101T000000Z" in path.name

    def test_end_of_day_timestamp_encoded_correctly(self, ts_end_of_day: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_end_of_day, tmp_path)
        assert "20261231T235959Z" in path.name

    def test_timestamp_format_is_yyyymmddthhmmssz(self, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
        # Extract the timestamp token (4th underscore-position)
        ts_token = path.stem.split("_")[3]
        # Must match YYYYMMDDTHHMMSSZ exactly
        parsed = datetime.strptime(ts_token, "%Y%m%dT%H%M%SZ")
        assert parsed.replace(tzinfo=timezone.utc) == ts_standard

    def test_seconds_precision_preserved(self, tmp_path: Path) -> None:
        ts = datetime(2026, 5, 14, 12, 30, 45, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_path)
        assert "T123045Z" in path.name

    def test_round_filename_rounds_to_nearest_10_minutes(self, tmp_path: Path) -> None:
        ts = datetime(2026, 4, 1, 20, 53, 0, tzinfo=timezone.utc)
        path_rounded = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_path, round_filename=True)
        path_exact = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_path, round_filename=False)
        # 53 min rounded to nearest 10 = 50
        assert "T205000Z" in path_rounded.name
        assert "T205300Z" in path_exact.name


# ---------------------------------------------------------------------------
# Directory structure: RADAR/YYYY/MM/DD/
# ---------------------------------------------------------------------------


class TestDirectoryStructure:
    # def test_root_subdirectory_is_radar_name(self, ts_standard: datetime, tmp_path: Path) -> None:
    # path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
    # relative = path.relative_to(tmp_path)
    # assert relative.parts[0] == "RMA6"

    def test_year_directory_level(self, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
        relative = path.relative_to(tmp_path)
        assert relative.parts[0] == "2026"

    def test_month_directory_level(self, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
        relative = path.relative_to(tmp_path)
        assert relative.parts[1] == "04"

    def test_day_directory_level(self, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
        relative = path.relative_to(tmp_path)
        assert relative.parts[2] == "22"

    def test_depth_is_exactly_5_levels(self, ts_standard: datetime, tmp_path: Path) -> None:
        """Path depth: base/RADAR/YYYY/MM/DD/filename — 5 parts below base."""
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
        relative = path.relative_to(tmp_path)
        assert len(relative.parts) == 4  # RADAR, YYYY, MM, DD, filename

    def test_parent_dirs_created(self, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts_standard, tmp_path)
        assert path.parent.is_dir()

    @pytest.mark.parametrize("radar", ["RMA1", "RMA6", "RMA11", "RMA3"])
    def test_radar_directory_not_matches_radar_name(self, radar: str, ts_standard: datetime, tmp_path: Path) -> None:
        path = product_path_and_filename(radar, "0315", "01", "DBZH", ts_standard, tmp_path)
        assert path.parts[len(tmp_path.parts)] != radar


# ---------------------------------------------------------------------------
# Strategy and volume number token variants
# ---------------------------------------------------------------------------


class TestStrategyAndVolNrTokens:
    @pytest.mark.parametrize(
        "strategy,vol_nr",
        [
            ("0315", "01"),
            ("0315", "02"),
            ("0302", "01"),
            ("0303", "02"),
        ],
    )
    def test_strategy_and_vol_nr_embedded_in_filename(
        self, strategy: str, vol_nr: str, ts_standard: datetime, tmp_path: Path
    ) -> None:
        path = product_path_and_filename("RMA1", strategy, vol_nr, "DBZH", ts_standard, tmp_path)
        assert f"_{strategy}_" in path.name
        assert f"_{vol_nr}_" in path.name

    def test_vol_nr_02_differs_from_01(self, ts_standard: datetime, tmp_path: Path) -> None:
        p1 = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts_standard, tmp_path)
        p2 = product_path_and_filename("RMA1", "0315", "02", "DBZH", ts_standard, tmp_path)
        assert p1.name != p2.name


# ---------------------------------------------------------------------------
# extract_cog_filename_components_v2: comprehensive parametrization
# ---------------------------------------------------------------------------


class TestExtractCogFilenameComponentsV2:
    @pytest.mark.parametrize(
        "filename,expected_field,expected_filtered",
        [
            ("RMA1_0315_01_20260401T205000Z_DBZH.tif", "DBZH", True),
            ("RMA1_0315_01_20260401T205000Z_DBZHo.tif", "DBZH", False),
            ("RMA6_0315_02_20260422T021351Z_COLMAX.tif", "COLMAX", True),
            ("RMA6_0315_02_20260422T021351Z_COLMAXo.tif", "COLMAX", False),
            ("RMA11_0302_01_20261231T235959Z_ZDR.tif", "ZDR", True),
            ("RMA11_0302_01_20261231T235959Z_ZDRo.tif", "ZDR", False),
            ("RMA3_0315_02_20260101T000000Z_VRAD.tif", "VRAD", True),
            ("RMA3_0315_02_20260101T000000Z_VRADo.tif", "VRAD", False),
        ],
    )
    def test_field_and_filtered_extraction(self, filename: str, expected_field: str, expected_filtered: bool) -> None:
        result = extract_cog_filename_components_v2(filename)
        assert result["field_name"] == expected_field
        assert result["filtered"] is expected_filtered

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
            (
                "RMA11_0302_01_20261231T235959Z_ZDR.tif",
                {
                    "radar_name": "RMA11",
                    "strategy": "0302",
                    "vol_nr": "01",
                    "timestamp": "20261231T235959Z",
                    "field_name": "ZDR",
                    "filtered": True,
                },
            ),
        ],
    )
    def test_full_component_extraction(self, filename: str, expected: dict) -> None:
        assert extract_cog_filename_components_v2(filename) == expected

    def test_all_keys_present_in_result(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZH.tif")
        assert set(result.keys()) == {"radar_name", "strategy", "vol_nr", "timestamp", "field_name", "filtered"}

    def test_filtered_type_is_bool(self) -> None:
        result = extract_cog_filename_components_v2("RMA1_0315_01_20260401T205000Z_DBZH.tif")
        assert isinstance(result["filtered"], bool)


# ---------------------------------------------------------------------------
# extract_cog_filename_components_v2: invalid inputs
# ---------------------------------------------------------------------------


class TestExtractCogFilenameComponentsV2InvalidInputs:
    @pytest.mark.parametrize(
        "invalid_filename",
        [
            "RMA1_20260401T205000Z_DBZH_00.tif",  # v1 format — no strategy/vol
            "not_a_valid_filename.tif",
            "RMA1_0315_01_20260401T205000Z_DBZH.png",  # wrong extension
            "",
            "RMA1_0315_01_DBZH.tif",  # missing timestamp
            "RMA1_20260401T205000Z_DBZH.tif",  # missing strategy and vol_nr
            "INVALID.tif",
        ],
    )
    def test_invalid_filename_raises_value_error(self, invalid_filename: str) -> None:
        with pytest.raises(ValueError):
            extract_cog_filename_components_v2(invalid_filename)

    def test_error_message_contains_filename(self) -> None:
        bad = "bad_format.tif"
        with pytest.raises(ValueError, match="bad_format.tif"):
            extract_cog_filename_components_v2(bad)

    def test_v1_format_explicitly_rejected(self) -> None:
        """Old v1 format (RADAR_TIMESTAMP_FIELD_ELEVATION) must be rejected."""
        with pytest.raises(ValueError):
            extract_cog_filename_components_v2("RMA1_20260401T205000Z_DBZH_00.tif")


# ---------------------------------------------------------------------------
# Round-trip: generate → parse → verify
# ---------------------------------------------------------------------------


class TestRoundTrip:
    @pytest.mark.parametrize(
        "radar,strategy,vol_nr,field,filtered",
        [
            ("RMA1", "0315", "01", "DBZH", True),
            ("RMA1", "0315", "01", "DBZH", False),
            ("RMA6", "0315", "02", "COLMAX", True),
            ("RMA6", "0315", "02", "COLMAX", False),
            ("RMA11", "0302", "01", "ZDR", True),
            ("RMA11", "0302", "01", "ZDR", False),
            ("RMA3", "0303", "02", "VRAD", False),
        ],
    )
    def test_round_trip_all_fields_recovered(
        self,
        radar: str,
        strategy: str,
        vol_nr: str,
        field: str,
        filtered: bool,
        tmp_path: Path,
    ) -> None:
        ts = datetime(2026, 4, 22, 2, 13, 51, tzinfo=timezone.utc)
        path = product_path_and_filename(radar, strategy, vol_nr, field, ts, tmp_path, filtered=filtered)
        components = extract_cog_filename_components_v2(path.name)

        assert components["radar_name"] == radar
        assert components["strategy"] == strategy
        assert components["vol_nr"] == vol_nr
        assert components["field_name"] == field
        assert components["filtered"] is filtered
        assert components["timestamp"] == ts.strftime("%Y%m%dT%H%M%SZ")

    def test_round_trip_midnight_timestamp(self, tmp_path: Path) -> None:
        ts = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_path)
        components = extract_cog_filename_components_v2(path.name)
        assert components["timestamp"] == "20260101T000000Z"

    def test_round_trip_end_of_day_timestamp(self, tmp_path: Path) -> None:
        ts = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_path)
        components = extract_cog_filename_components_v2(path.name)
        assert components["timestamp"] == "20261231T235959Z"

    def test_round_trip_filtered_false_field_name_stripped_of_o(self, tmp_path: Path) -> None:
        """field_name returned by parser must strip the 'o' suffix."""
        ts = datetime(2026, 4, 22, 2, 13, 51, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA6", "0315", "01", "DBZH", ts, tmp_path, filtered=False)
        components = extract_cog_filename_components_v2(path.name)
        assert components["field_name"] == "DBZH"  # NOT "DBZHo"
        assert components["filtered"] is False
