# -*- coding: utf-8 -*-
"""Unit tests for metadata_utils.py.

Test bodies are intentionally left as stubs (pass) to be filled in as part
of the TDD cycle.  The test outline covers all public functions and the key
error paths.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from radarlib.daemons.metadata_utils import apply_metadata_to_cog, build_product_metadata
from radarlib.daemons.product_metadata import ProductMetadata

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_radar() -> MagicMock:
    """PyART Radar mock with a 240 km range array."""
    radar = MagicMock()
    radar.range = {"data": [1000.0, 120000.0, 240000.0]}
    return radar


@pytest.fixture()
def valid_volume_info() -> dict:
    """Minimal volume_info dict as returned by SQLiteStateTracker."""
    return {
        "vol_nr": "01",
        "strategy": "0315",
        "observation_datetime": "20260401T205000Z",
    }


@pytest.fixture()
def sample_metadata() -> ProductMetadata:
    """A ready-made ProductMetadata for use in apply_metadata_to_cog tests."""
    return ProductMetadata(
        volume_number=1,
        strategy="0315",
        field_name="DBZH",
        radar_name="RMA1",
        radar_coverage_m=240000.0,
        observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# build_product_metadata()
# ---------------------------------------------------------------------------


class TestBuildProductMetadata:
    def test_returns_product_metadata_instance(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1")
        assert isinstance(result, ProductMetadata)

    def test_volume_number_parsed_as_int(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1")
        assert result.volume_number == 1
        assert isinstance(result.volume_number, int)

    def test_strategy_passed_through(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1")
        assert result.strategy == "0315"

    def test_field_name_passed_through(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "ZDR", "RMA1")
        assert result.field_name == "ZDR"

    def test_radar_name_passed_through(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA6")
        assert result.radar_name == "RMA6"

    def test_radar_coverage_converted_to_metres(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        # mock_radar.range['data'][-1] = 240000 m = 240 km → stored as 240 * 1000 = 240000 m
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1")
        assert result.radar_coverage_m == pytest.approx(240000.0)

    def test_observation_timestamp_is_utc_aware(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1")
        assert result.observation_timestamp.tzinfo is timezone.utc

    def test_observation_timestamp_values(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1")
        dt = result.observation_timestamp
        assert (dt.year, dt.month, dt.day, dt.hour, dt.minute) == (2026, 4, 1, 20, 50)

    def test_filtered_default_is_false(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1")
        assert result.filtered is False

    def test_filtered_true_propagated(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        result = build_product_metadata(mock_radar, valid_volume_info, "DBZH", "RMA1", filtered=True)
        assert result.filtered is True

    def test_missing_observation_datetime_raises_value_error(self, mock_radar: MagicMock) -> None:
        bad_info = {"vol_nr": "01", "strategy": "0315"}
        with pytest.raises(ValueError, match="observation_datetime"):
            build_product_metadata(mock_radar, bad_info, "DBZH", "RMA1")

    def test_empty_observation_datetime_raises_value_error(
        self, mock_radar: MagicMock, valid_volume_info: dict
    ) -> None:
        info = {**valid_volume_info, "observation_datetime": ""}
        with pytest.raises(ValueError):
            build_product_metadata(mock_radar, info, "DBZH", "RMA1")

    def test_missing_vol_nr_raises_key_error(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        info = {k: v for k, v in valid_volume_info.items() if k != "vol_nr"}
        with pytest.raises(KeyError, match="vol_nr"):
            build_product_metadata(mock_radar, info, "DBZH", "RMA1")

    def test_missing_strategy_raises_key_error(self, mock_radar: MagicMock, valid_volume_info: dict) -> None:
        info = {k: v for k, v in valid_volume_info.items() if k != "strategy"}
        with pytest.raises(KeyError, match="strategy"):
            build_product_metadata(mock_radar, info, "DBZH", "RMA1")

    def test_invalid_observation_datetime_format_raises_value_error(
        self, mock_radar: MagicMock, valid_volume_info: dict
    ) -> None:
        info = {**valid_volume_info, "observation_datetime": "2026-04-01T20:50:00Z"}
        with pytest.raises(ValueError):
            build_product_metadata(mock_radar, info, "DBZH", "RMA1")


# ---------------------------------------------------------------------------
# apply_metadata_to_cog()
# ---------------------------------------------------------------------------


class TestApplyMetadataToCog:
    def test_calls_rasterio_open_with_r_plus_mode(self, sample_metadata: ProductMetadata, tmp_path: Path) -> None:
        cog_path = tmp_path / "test.tif"
        mock_dst = MagicMock()
        # mock_ctx = MagicMock().__enter__.return_value = mock_dst
        with patch("rasterio.open") as mock_open:
            mock_open.return_value.__enter__ = MagicMock(return_value=mock_dst)
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            apply_metadata_to_cog(cog_path, sample_metadata)
        mock_open.assert_called_once_with(cog_path, "r+")

    def test_calls_update_tags_with_correct_keys(self, sample_metadata: ProductMetadata, tmp_path: Path) -> None:
        cog_path = tmp_path / "test.tif"
        mock_dst = MagicMock()
        expected_tags = sample_metadata.to_geotiff_tags()
        with patch("rasterio.open") as mock_open:
            mock_open.return_value.__enter__ = MagicMock(return_value=mock_dst)
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            apply_metadata_to_cog(cog_path, sample_metadata)
        mock_dst.update_tags.assert_called_once_with(**expected_tags)

    def test_rasterio_exception_is_wrapped_as_io_error(self, sample_metadata: ProductMetadata, tmp_path: Path) -> None:
        cog_path = tmp_path / "missing.tif"
        with patch("rasterio.open", side_effect=Exception("rasterio error")):
            with pytest.raises(IOError):
                apply_metadata_to_cog(cog_path, sample_metadata)

    def test_io_error_message_includes_path(self, sample_metadata: ProductMetadata, tmp_path: Path) -> None:
        cog_path = tmp_path / "myfile.tif"
        with patch("rasterio.open", side_effect=Exception("boom")):
            with pytest.raises(IOError, match="myfile.tif"):
                apply_metadata_to_cog(cog_path, sample_metadata)

    @pytest.mark.integration
    def test_tags_written_and_readable_from_real_cog(self, tmp_path: Path) -> None:
        """Integration: tags written by apply_metadata_to_cog must be readable
        back via rasterio on a real minimal COG file."""
        from datetime import datetime, timezone

        import numpy as np
        import rasterio
        from rasterio.transform import from_bounds

        cog_path = tmp_path / "test.tif"
        data = np.zeros((1, 10, 10), dtype=np.float32)
        transform = from_bounds(-65, -35, -60, -30, 10, 10)
        with rasterio.open(
            cog_path,
            "w",
            driver="GTiff",
            height=10,
            width=10,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            dst.write(data)

        meta = ProductMetadata(
            volume_number=1,
            strategy="0315",
            field_name="DBZH",
            radar_name="RMA1",
            radar_coverage_m=240000.0,
            observation_timestamp=datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc),
        )
        apply_metadata_to_cog(cog_path, meta)

        with rasterio.open(cog_path) as src:
            tags = src.tags()
        assert tags.get("radarlib_field_name") == "DBZH"
        assert tags.get("radarlib_radar_name") == "RMA1"
        assert tags.get("radarlib_filtered") == "False"
