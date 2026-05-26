# -*- coding: utf-8 -*-
"""
Unit tests for src/radarlib/daemons/field_processor.py

Tests cover:
- FieldProcessor cannot be instantiated directly (ABC)
- RawCogFieldProcessor initialisation
- process_and_save happy path: correct output path, file existence
- Metadata tags are applied to the generated COG
- Output filename follows the naming convention
- Error handling: missing field in radar
- Gate filter is applied when provided (filtered=True path)
- Unfiltered path uses _NOFILTERS config keys
"""
# import shutil
# import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from radarlib.daemons.field_processor import FieldProcessor, RawCogFieldProcessor, get_field_data_safe

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def minimal_volume_info() -> Dict[str, Any]:
    """Minimal volume_info dict as produced by SQLiteStateTracker."""
    return {
        "volume_id": "test-vol-001",
        "strategy": "0315",
        "vol_nr": "01",
        "observation_datetime": "20260514T120000Z",  # YYYYMMDDTHHMMSSZ format
        "netcdf_path": "/data/RMA1_0315_01_20260514T120000Z.nc",
    }


@pytest.fixture()
def mock_config(tmp_path: Path) -> MagicMock:
    """Minimal mock of ProductGenerationDaemonConfig."""
    cfg = MagicMock()
    cfg.local_product_dir = tmp_path / "products"
    cfg.local_product_dir.mkdir(parents=True, exist_ok=True)
    cfg.field_value_masks = {}
    return cfg


@pytest.fixture()
def mock_radar() -> MagicMock:
    """Minimal mock of a PyART Radar object with one DBZH field."""

    class SimpleData:
        """Simple wrapper to mimic PyART's data structures."""

        def __init__(self, value):
            self.data = np.array([value])

    radar = MagicMock()
    radar.fields = {"DBZH": {"data": np.ma.array(np.random.rand(10, 10), mask=False)}}
    radar.latitude = {"data": SimpleData(-34.6)}
    radar.longitude = {"data": SimpleData(-58.5)}
    radar.range = {"data": np.array([1000.0, 50_000.0, 240_000.0])}
    radar.get_elevation = MagicMock(return_value=np.array([0.5, 0.5, 0.5]))
    radar.metadata = {"instrument_name": "RMA1", "filename": "RMA1_0315_01_20260514T120000Z.nc"}
    return radar


@pytest.fixture()
def mock_geometry() -> MagicMock:
    """Minimal mock of GridGeometry."""
    geom = MagicMock(spec_set=False)
    return geom


# ---------------------------------------------------------------------------
# get_field_data_safe
# ---------------------------------------------------------------------------


class TestGetFieldDataSafe:
    def test_returns_data_for_existing_field(self, mock_radar: MagicMock) -> None:
        data = get_field_data_safe(mock_radar, "DBZH")
        assert data is not None

    def test_raises_key_error_for_missing_field(self, mock_radar: MagicMock) -> None:
        with pytest.raises(KeyError, match="VRAD"):
            get_field_data_safe(mock_radar, "VRAD")

    def test_raises_value_error_when_data_is_none(self) -> None:
        radar = MagicMock()
        radar.fields = {"DBZH": {"data": None}}
        with pytest.raises(ValueError, match="DBZH"):
            get_field_data_safe(radar, "DBZH")


# ---------------------------------------------------------------------------
# FieldProcessor (ABC)
# ---------------------------------------------------------------------------


class TestFieldProcessorIsAbstract:
    def test_cannot_instantiate_base_class(self, mock_config: MagicMock, minimal_volume_info: Dict[str, Any]) -> None:
        with pytest.raises(TypeError):
            FieldProcessor(
                config=mock_config, volume_info=minimal_volume_info, radar_name="RMA1"
            )  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# RawCogFieldProcessor — initialisation
# ---------------------------------------------------------------------------


class TestRawCogFieldProcessorInit:
    def test_stores_config(self, mock_config: MagicMock, minimal_volume_info: Dict[str, Any]) -> None:
        proc = RawCogFieldProcessor(config=mock_config, volume_info=minimal_volume_info, radar_name="RMA1")
        assert proc.config is mock_config

    def test_stores_volume_info(self, mock_config: MagicMock, minimal_volume_info: Dict[str, Any]) -> None:
        proc = RawCogFieldProcessor(config=mock_config, volume_info=minimal_volume_info, radar_name="RMA1")
        assert proc.volume_info == minimal_volume_info

    def test_stores_radar_name(self, mock_config: MagicMock, minimal_volume_info: Dict[str, Any]) -> None:
        proc = RawCogFieldProcessor(config=mock_config, volume_info=minimal_volume_info, radar_name="RMA1")
        assert proc.radar_name == "RMA1"


# ---------------------------------------------------------------------------
# Helpers for process_and_save tests
# ---------------------------------------------------------------------------


def _build_happy_path_patches(
    tmp_path: Path,
    with_gate_filter: bool = False,
):
    """Return a stack of patches that makes process_and_save succeed.

    Because ``apply_geometry``, ``constant_elevation_ppi``, ``create_raw_cog``,
    and ``product_path_and_filename`` are DEFERRED imports inside
    ``process_and_save`` they must be patched at their source modules (not at
    ``radarlib.daemons.field_processor.*``).

    ``product_path_and_filename`` is now called TWICE (ceiled + rounded).
    The mock returns two distinct Paths so both the target and the rounded
    variant can be exercised without a real filesystem write.
    """
    fake_grid = np.zeros((5, 10, 10), dtype=np.float32)
    fake_ppi = np.zeros((10, 10), dtype=np.float32)

    # Two distinct paths — ceiled and rounded
    ceiled_path = tmp_path / "RMA1" / "2026" / "05" / "14" / "RMA1_0315_01_20260514T130000Z_DBZHo.tif"
    rounded_path = tmp_path / "RMA1" / "2026" / "05" / "14" / "RMA1_0315_01_20260514T120000Z_DBZHo.tif"

    def _fake_create_cog(ppi, geom, lat, lon, path, **kwargs):
        """Mimic create_raw_cog by touching the output file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    # product_path_and_filename is called twice; return ceiled first, then rounded
    ceiled_path.parent.mkdir(parents=True, exist_ok=True)
    rounded_path.parent.mkdir(parents=True, exist_ok=True)

    patches = [
        patch("radarlib.radar_grid.apply_geometry", return_value=fake_grid),
        patch("radarlib.radar_grid.constant_elevation_ppi", return_value=fake_ppi),
        patch("radarlib.radar_grid.create_raw_cog", side_effect=_fake_create_cog),
        patch(
            "radarlib.utils.names_utils.product_path_and_filename",
            side_effect=[ceiled_path, rounded_path],
        ),
        patch("radarlib.utils.memory_profiling.log_memory_usage"),
        patch("radarlib.daemons.metadata_utils.build_product_metadata", return_value=MagicMock()),
    ]
    return patches, ceiled_path, rounded_path


# ---------------------------------------------------------------------------
# RawCogFieldProcessor — process_and_save happy path
# ---------------------------------------------------------------------------


class TestRawCogFieldProcessorProcessAndSave:
    def _make_processor(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
    ) -> RawCogFieldProcessor:
        return RawCogFieldProcessor(
            config=mock_config,
            volume_info=minimal_volume_info,
            radar_name="RMA1",
        )

    def test_returns_path_on_success(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
        mock_radar: MagicMock,
        mock_geometry: MagicMock,
        tmp_path: Path,
    ) -> None:
        """process_and_save returns a Path when create_raw_cog succeeds."""
        proc = self._make_processor(mock_config, minimal_volume_info)
        field_data = np.ma.array(np.zeros((10, 10)), mask=False)

        patches, ceiled_path, _ = _build_happy_path_patches(tmp_path)
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            result = proc.process_and_save(
                field_data=field_data,
                field_name="DBZH",
                radar=mock_radar,
                geometry=mock_geometry,
                output_dir=tmp_path,
            )

        assert result is not None
        assert isinstance(result, Path)

    def test_metadata_is_applied_to_cog(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
        mock_radar: MagicMock,
        mock_geometry: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Metadata is passed to create_raw_cog via extra_tags kwarg."""
        proc = self._make_processor(mock_config, minimal_volume_info)
        field_data = np.ma.array(np.zeros((10, 10)), mask=False)

        patches, _, __ = _build_happy_path_patches(tmp_path)
        from contextlib import ExitStack

        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            create_cog_mock = mocks[2]  # radarlib.radar_grid.create_raw_cog
            proc.process_and_save(
                field_data=field_data,
                field_name="DBZH",
                radar=mock_radar,
                geometry=mock_geometry,
                output_dir=tmp_path,
            )

        create_cog_mock.assert_called_once()
        # Check that extra_tags kwarg was passed
        call_kwargs = create_cog_mock.call_args.kwargs
        assert "extra_tags" in call_kwargs

    def test_output_filename_follows_naming_convention(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
        mock_radar: MagicMock,
        mock_geometry: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Output filename from process_and_save matches the legacy RADAR_*_FIELD_*.tif pattern."""
        proc = self._make_processor(mock_config, minimal_volume_info)
        field_data = np.ma.array(np.zeros((10, 10)), mask=False)

        patches, ceiled_path, _ = _build_happy_path_patches(tmp_path)
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            result = proc.process_and_save(
                field_data=field_data,
                field_name="DBZH",
                radar=mock_radar,
                geometry=mock_geometry,
                output_dir=tmp_path,
            )

        assert result is not None
        # The result path is the ceiled-timestamp path returned by product_path_and_filename
        assert result == ceiled_path
        assert result.suffix == ".tif"

    def test_returns_none_on_exception(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
        mock_radar: MagicMock,
        mock_geometry: MagicMock,
    ) -> None:
        """process_and_save returns None (no re-raise) when an internal step fails."""
        proc = self._make_processor(mock_config, minimal_volume_info)
        field_data = np.ma.array(np.zeros((10, 10)))

        # apply_geometry is a deferred import — patch at its source module
        with patch("radarlib.radar_grid.apply_geometry", side_effect=RuntimeError("boom")):
            result = proc.process_and_save(
                field_data=field_data,
                field_name="DBZH",
                radar=mock_radar,
                geometry=mock_geometry,
            )

        assert result is None

    def test_gate_filter_passed_to_apply_geometry(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
        mock_radar: MagicMock,
        mock_geometry: MagicMock,
        tmp_path: Path,
    ) -> None:
        """When gate_filter is provided, apply_geometry receives it in additional_filters."""
        proc = self._make_processor(mock_config, minimal_volume_info)
        field_data = np.ma.array(np.zeros((10, 10)), mask=False)
        fake_gate_filter = MagicMock(name="gate_filter")

        patches, _, _ = _build_happy_path_patches(tmp_path, with_gate_filter=True)
        from contextlib import ExitStack

        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            apply_geom_mock = mocks[0]  # radarlib.radar_grid.apply_geometry
            proc.process_and_save(
                field_data=field_data,
                field_name="DBZH",
                radar=mock_radar,
                geometry=mock_geometry,
                gate_filter=fake_gate_filter,
                output_dir=tmp_path,
            )

        apply_geom_mock.assert_called_once()
        call_kwargs = apply_geom_mock.call_args
        # additional_filters must include the gate_filter
        additional_filters = call_kwargs.kwargs.get(
            "additional_filters",
            call_kwargs.args[2] if len(call_kwargs.args) > 2 else [],
        )
        assert fake_gate_filter in additional_filters

    def test_unfiltered_uses_nofilters_config_keys(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
        mock_radar: MagicMock,
        mock_geometry: MagicMock,
        tmp_path: Path,
    ) -> None:
        """When gate_filter=None, VMIN/CMAP/VMAX keys read with _NOFILTERS suffix."""
        proc = self._make_processor(mock_config, minimal_volume_info)
        field_data = np.ma.array(np.zeros((10, 10)), mask=False)

        # Inject sentinel values into radarlib.config for the NOFILTERS keys
        import radarlib.config as rc_module

        rc_module.__dict__["CMAP_DBZH_NOFILTERS"] = "grc_th"
        rc_module.__dict__["VMIN_DBZH_NOFILTERS"] = -10.0
        rc_module.__dict__["VMAX_DBZH_NOFILTERS"] = 60.0

        patches, _, _ = _build_happy_path_patches(tmp_path)
        from contextlib import ExitStack

        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            create_cog_mock = mocks[2]  # radarlib.radar_grid.create_raw_cog
            proc.process_and_save(
                field_data=field_data,
                field_name="DBZH",
                radar=mock_radar,
                geometry=mock_geometry,
                gate_filter=None,  # unfiltered
                config_key_field="DBZH",
                output_dir=tmp_path,
            )

        create_cog_mock.assert_called_once()
        call_kwargs = create_cog_mock.call_args
        assert call_kwargs.kwargs.get("cmap") == "grc_th"

        # Cleanup injected sentinels
        for k in ("CMAP_DBZH_NOFILTERS", "VMIN_DBZH_NOFILTERS", "VMAX_DBZH_NOFILTERS"):
            rc_module.__dict__.pop(k, None)

    def test_filtered_uses_standard_config_keys(
        self,
        mock_config: MagicMock,
        minimal_volume_info: Dict[str, Any],
        mock_radar: MagicMock,
        mock_geometry: MagicMock,
        tmp_path: Path,
    ) -> None:
        """When gate_filter is provided, CMAP/VMIN/VMAX keys WITHOUT _NOFILTERS are used."""
        proc = self._make_processor(mock_config, minimal_volume_info)
        field_data = np.ma.array(np.zeros((10, 10)), mask=False)
        fake_gate_filter = MagicMock(name="gate_filter")

        import radarlib.config as rc_module

        rc_module.__dict__["CMAP_DBZH"] = "rainbow"
        rc_module.__dict__["VMIN_DBZH"] = -20.0
        rc_module.__dict__["VMAX_DBZH"] = 70.0

        patches, _, _ = _build_happy_path_patches(tmp_path, with_gate_filter=True)
        from contextlib import ExitStack

        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            create_cog_mock = mocks[2]  # radarlib.radar_grid.create_raw_cog
            proc.process_and_save(
                field_data=field_data,
                field_name="DBZH",
                radar=mock_radar,
                geometry=mock_geometry,
                gate_filter=fake_gate_filter,  # filtered=True
                config_key_field="DBZH",
                output_dir=tmp_path,
            )

        create_cog_mock.assert_called_once()
        call_kwargs = create_cog_mock.call_args
        assert call_kwargs.kwargs.get("cmap") == "rainbow"

        # Cleanup injected sentinels
        for k in ("CMAP_DBZH", "VMIN_DBZH", "VMAX_DBZH"):
            rc_module.__dict__.pop(k, None)
