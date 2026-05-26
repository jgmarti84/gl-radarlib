# -*- coding: utf-8 -*-
"""Unit tests for ProductMetadata dataclass and helpers in product_metadata.py.

Test outline — implementations are intentionally left as stubs (pass) to be
filled in as part of the TDD cycle.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from radarlib.daemons.product_metadata import ProductMetadata, get_radar_coverage_km, parse_observation_timestamp

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def minimal_metadata() -> ProductMetadata:
    """Return a ProductMetadata instance with only the required fields set."""
    return ProductMetadata(
        volume_number="01",
        strategy="0315",
        field_name="DBZH",
        radar_name="RMA1",
        radar_coverage_m=240000.0,
        observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
    )


@pytest.fixture()
def mock_radar() -> MagicMock:
    """Return a mock PyART Radar object with a range array."""
    radar = MagicMock()
    radar.range = {"data": [1000.0, 2000.0, 240000.0]}
    return radar


# ---------------------------------------------------------------------------
# ProductMetadata.__init__
# ---------------------------------------------------------------------------


class TestProductMetadataInit:
    def test_required_fields_are_set(self, minimal_metadata: ProductMetadata) -> None:
        assert minimal_metadata.volume_number == "01"
        assert minimal_metadata.strategy == "0315"
        assert minimal_metadata.field_name == "DBZH"
        assert minimal_metadata.radar_name == "RMA1"
        assert minimal_metadata.radar_coverage_m == 240000.0
        assert minimal_metadata.observation_timestamp == datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc)

    def test_defaults_are_applied(self, minimal_metadata: ProductMetadata) -> None:
        assert minimal_metadata.filtered is False
        assert minimal_metadata.processing_version == "1.0"
        assert minimal_metadata.additional_metadata == {}
        assert isinstance(minimal_metadata.processing_timestamp, datetime)

    def test_filtered_defaults_to_false(self, minimal_metadata: ProductMetadata) -> None:
        assert minimal_metadata.filtered is False

    def test_processing_version_defaults_to_1_0(self, minimal_metadata: ProductMetadata) -> None:
        assert minimal_metadata.processing_version == "1.0"

    def test_additional_metadata_defaults_to_empty_dict(self, minimal_metadata: ProductMetadata) -> None:
        m1 = ProductMetadata(
            volume_number=1,
            strategy="0315",
            field_name="DBZH",
            radar_name="RMA1",
            radar_coverage_m=240000.0,
            observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
        )
        m2 = ProductMetadata(
            volume_number=1,
            strategy="0315",
            field_name="DBZH",
            radar_name="RMA1",
            radar_coverage_m=240000.0,
            observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
        )
        m1.additional_metadata["sentinel"] = 1
        assert "sentinel" not in m2.additional_metadata

    def test_missing_required_field_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            ProductMetadata(  # type: ignore[call-arg]
                volume_number=1,
                strategy="0315",
                radar_name="RMA1",
                radar_coverage_m=240000.0,
                observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
            )

    def test_filtered_true_is_stored_correctly(self) -> None:
        meta = ProductMetadata(
            volume_number=2,
            strategy="0302",
            field_name="ZDR",
            radar_name="RMA6",
            radar_coverage_m=150000.0,
            observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
            filtered=True,
        )
        assert meta.filtered is True


# ---------------------------------------------------------------------------
# ProductMetadata.to_dict()
# ---------------------------------------------------------------------------


class TestProductMetadataToDict:
    def test_returns_dict(self, minimal_metadata: ProductMetadata) -> None:
        assert isinstance(minimal_metadata.to_dict(), dict)

    def test_datetime_fields_are_iso_strings(self, minimal_metadata: ProductMetadata) -> None:
        d = minimal_metadata.to_dict()
        assert isinstance(d["observation_timestamp"], str)
        assert isinstance(d["processing_timestamp"], str)
        datetime.fromisoformat(d["observation_timestamp"])
        datetime.fromisoformat(d["processing_timestamp"])

    def test_all_required_keys_present(self, minimal_metadata: ProductMetadata) -> None:
        d = minimal_metadata.to_dict()
        for key in (
            "volume_number",
            "strategy",
            "field_name",
            "radar_name",
            "radar_coverage_m",
            "observation_timestamp",
            "processing_timestamp",
            "processing_version",
            "filtered",
        ):
            assert key in d, f"Missing key: {key!r}"

    def test_additional_metadata_merged_at_top_level(self) -> None:
        meta = ProductMetadata(
            volume_number=1,
            strategy="0315",
            field_name="DBZH",
            radar_name="RMA1",
            radar_coverage_m=240000.0,
            observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
            additional_metadata={"extra_info": "value123"},
        )
        assert meta.to_dict()["extra_info"] == "value123"

    def test_filtered_true_reflected_in_dict(self) -> None:
        meta = ProductMetadata(
            volume_number=1,
            strategy="0315",
            field_name="DBZH",
            radar_name="RMA1",
            radar_coverage_m=240000.0,
            observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
            filtered=True,
        )
        assert meta.to_dict()["filtered"] is True

    def test_filtered_false_reflected_in_dict(self, minimal_metadata: ProductMetadata) -> None:
        assert minimal_metadata.to_dict()["filtered"] is False

    def test_volume_number_is_str(self, minimal_metadata: ProductMetadata) -> None:
        assert isinstance(minimal_metadata.to_dict()["volume_number"], str)

    def test_observation_timestamp_year_correct(self, minimal_metadata: ProductMetadata) -> None:
        parsed = datetime.fromisoformat(minimal_metadata.to_dict()["observation_timestamp"])
        assert parsed.year == 2026 and parsed.month == 4


# ---------------------------------------------------------------------------
# ProductMetadata.to_geotiff_tags()
# ---------------------------------------------------------------------------


class TestProductMetadataToGeotiffTags:
    def test_returns_dict_of_strings(self, minimal_metadata: ProductMetadata) -> None:
        for key, value in minimal_metadata.to_geotiff_tags().items():
            assert isinstance(value, str), f"Tag {key!r} value is not str: {type(value)}"

    def test_all_radarlib_keys_present(self, minimal_metadata: ProductMetadata) -> None:
        tags = minimal_metadata.to_geotiff_tags()
        for expected in (
            "radarlib_volume_number",
            "radarlib_strategy",
            "radarlib_field_name",
            "radarlib_radar_name",
            "radarlib_radar_coverage_m",
            "radarlib_observation_timestamp",
            "radarlib_processing_timestamp",
            "radarlib_processing_version",
            "radarlib_filtered",
        ):
            assert expected in tags, f"Missing tag: {expected!r}"

    def test_all_keys_have_radarlib_prefix(self, minimal_metadata: ProductMetadata) -> None:
        for key in minimal_metadata.to_geotiff_tags():
            assert key.startswith("radarlib_"), f"Key {key!r} lacks 'radarlib_' prefix"

    def test_observation_timestamp_is_iso_string(self, minimal_metadata: ProductMetadata) -> None:
        obs_str = minimal_metadata.to_geotiff_tags()["radarlib_observation_timestamp"]
        assert isinstance(obs_str, str)
        parsed = datetime.fromisoformat(obs_str)
        assert parsed.year == 2026

    def test_filtered_tag_is_string_true_or_false(self) -> None:
        ts = datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc)
        meta_t = ProductMetadata(
            volume_number=1,
            strategy="0315",
            field_name="DBZH",
            radar_name="RMA1",
            radar_coverage_m=240000.0,
            observation_timestamp=ts,
            filtered=True,
        )
        meta_f = ProductMetadata(
            volume_number=1,
            strategy="0315",
            field_name="DBZH",
            radar_name="RMA1",
            radar_coverage_m=240000.0,
            observation_timestamp=ts,
            filtered=False,
        )
        assert meta_t.to_geotiff_tags()["radarlib_filtered"] == "True"
        assert meta_f.to_geotiff_tags()["radarlib_filtered"] == "False"

    def test_coverage_is_string_representation_of_float(self, minimal_metadata: ProductMetadata) -> None:
        cov_str = minimal_metadata.to_geotiff_tags()["radarlib_radar_coverage_m"]
        assert isinstance(cov_str, str)
        assert float(cov_str) == pytest.approx(240000.0)

    def test_field_name_tag_value(self, minimal_metadata: ProductMetadata) -> None:
        assert minimal_metadata.to_geotiff_tags()["radarlib_field_name"] == "DBZH"

    def test_radar_name_tag_value(self, minimal_metadata: ProductMetadata) -> None:
        assert minimal_metadata.to_geotiff_tags()["radarlib_radar_name"] == "RMA1"


# ---------------------------------------------------------------------------
# parse_observation_timestamp()
# ---------------------------------------------------------------------------


class TestParseObservationTimestamp:
    def test_valid_timestamp_returns_datetime_utc(self) -> None:
        dt = parse_observation_timestamp("20260401T205000Z")
        assert isinstance(dt, datetime)
        assert dt.tzinfo is timezone.utc

    def test_parsed_year_month_day_are_correct(self) -> None:
        dt = parse_observation_timestamp("20260401T205000Z")
        assert dt.year == 2026
        assert dt.month == 4
        assert dt.day == 1

    def test_parsed_hour_minute_second_are_correct(self) -> None:
        dt = parse_observation_timestamp("20260401T205000Z")
        assert dt.hour == 20
        assert dt.minute == 50
        assert dt.second == 0

    def test_invalid_format_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            parse_observation_timestamp("not_a_timestamp")

    def test_empty_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            parse_observation_timestamp("")

    def test_midnight_parses_correctly(self) -> None:
        dt = parse_observation_timestamp("20260101T000000Z")
        assert dt.hour == 0 and dt.minute == 0 and dt.second == 0

    @pytest.mark.parametrize(
        "timestamp_str,expected",
        [
            (
                "20260401T205000Z",
                datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
            ),
            (
                "20240101T000000Z",
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
        ],
    )
    def test_parametrized_valid_inputs(self, timestamp_str: str, expected: datetime) -> None:
        assert parse_observation_timestamp(timestamp_str) == expected


# ---------------------------------------------------------------------------
# get_radar_coverage_km()
# ---------------------------------------------------------------------------


class TestGetRadarCoverageKm:
    def test_returns_float(self, mock_radar: MagicMock) -> None:
        assert isinstance(get_radar_coverage_km(mock_radar), float)

    def test_converts_metres_to_km(self, mock_radar: MagicMock) -> None:
        # mock_radar.range['data'] = [1000.0, 2000.0, 240000.0]; last = 240000 m
        assert get_radar_coverage_km(mock_radar) == pytest.approx(240.0)

    def test_uses_last_element_of_range_array(self, mock_radar: MagicMock) -> None:
        # The fixture ends at 240000; a radar ending at 300000 must return 300.0
        import numpy as np

        mock_radar.range = {"data": np.array([1000.0, 100_000.0, 300_000.0])}
        assert get_radar_coverage_km(mock_radar) == pytest.approx(300.0)

    def test_single_element_range_array(self) -> None:
        import numpy as np

        radar = MagicMock()
        radar.range = {"data": np.array([60_000.0])}
        result = get_radar_coverage_km(radar)
        assert isinstance(result, float)
        assert result == pytest.approx(60.0)
