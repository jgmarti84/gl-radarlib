"""
Unit tests for radar_grid.geotiff module.

Tests cover:
- apply_colormap_to_array
- create_geotiff
- create_cog  (metadata round-trip)
- create_raw_cog
- read_cog_metadata
- remap_cog_colormap
- read_cog_tile_as_rgba
- helper functions (_resolve_vmin_vmax, _get_cmap_name, _compute_crs_bounds)
"""

import tempfile
from pathlib import Path

import numpy as np
import pytest
import rasterio

from radarlib.radar_grid.geotiff import (
    _DATA_TYPE_RAW,
    _DATA_TYPE_RGBA,
    _TAG_CMAP,
    _TAG_DATA_TYPE,
    _TAG_VMAX,
    _TAG_VMIN,
    _compute_crs_bounds,
    _get_cmap_name,
    _resolve_vmin_vmax,
    _string_to_resampling,
    apply_colormap_to_array,
    convert_rgba_cog_to_raw,
    create_cog,
    create_geotiff,
    create_raw_cog,
    read_cog_metadata,
    read_cog_tile_as_rgba,
    remap_cog_colormap,
)
from radarlib.radar_grid.geometry import GridGeometry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_geometry(ny: int = 30, nx: int = 30, nz: int = 5) -> GridGeometry:
    """Return a minimal GridGeometry for testing."""
    n_points = nz * ny * nx
    return GridGeometry(
        grid_shape=(nz, ny, nx),
        grid_limits=((0.0, 10000.0), (-50000.0, 50000.0), (-50000.0, 50000.0)),
        indptr=np.arange(n_points + 1, dtype=np.int32),
        gate_indices=np.zeros(n_points, dtype=np.int32),
        weights=np.ones(n_points, dtype=np.float32),
        toa=12000.0,
        radar_altitude=100.0,
    )


def _make_data(ny: int = 30, nx: int = 30, with_nan: bool = True) -> np.ndarray:
    """Return a synthetic 2-D data array."""
    rng = np.random.default_rng(42)
    data = rng.uniform(0.0, 70.0, (ny, nx)).astype(np.float32)
    if with_nan:
        data[0:3, 0:3] = np.nan
    return data


@pytest.fixture
def geometry():
    return _make_geometry()


@pytest.fixture
def data(geometry):
    _, ny, nx = geometry.grid_shape
    return _make_data(ny, nx)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestGetCmapName:
    def test_string_passthrough(self):
        assert _get_cmap_name("viridis") == "viridis"

    def test_colormap_object(self):
        import matplotlib.pyplot as plt

        cmap = plt.get_cmap("plasma")
        assert _get_cmap_name(cmap) == "plasma"


class TestResolveVminVmax:
    def test_with_none_infers_from_data(self):
        data = np.array([[0.0, 10.0], [20.0, 30.0]])
        vmin, vmax = _resolve_vmin_vmax(data, None, None, None)
        assert vmin == pytest.approx(0.0)
        assert vmax == pytest.approx(30.0)

    def test_explicit_values_respected(self):
        data = np.array([[0.0, 10.0], [20.0, 30.0]])
        vmin, vmax = _resolve_vmin_vmax(data, -5.0, 50.0, None)
        assert vmin == pytest.approx(-5.0)
        assert vmax == pytest.approx(50.0)

    def test_nan_values_ignored(self):
        data = np.array([[0.0, np.nan], [20.0, 30.0]])
        vmin, vmax = _resolve_vmin_vmax(data, None, None, None)
        assert vmin == pytest.approx(0.0)
        assert vmax == pytest.approx(30.0)

    def test_nodata_values_excluded(self):
        data = np.array([[0.0, -9999.0], [20.0, 30.0]])
        vmin, vmax = _resolve_vmin_vmax(data, None, None, -9999.0)
        assert vmin == pytest.approx(0.0)
        assert vmax == pytest.approx(30.0)

    def test_all_nodata_returns_defaults(self):
        data = np.full((3, 3), np.nan)
        vmin, vmax = _resolve_vmin_vmax(data, None, None, None)
        assert vmin == pytest.approx(0.0)
        assert vmax == pytest.approx(1.0)


class TestStringToResampling:
    def test_valid_methods(self):
        from rasterio.enums import Resampling

        assert _string_to_resampling("nearest") == Resampling.nearest
        assert _string_to_resampling("bilinear") == Resampling.bilinear

    def test_invalid_method_raises(self):
        with pytest.raises(ValueError, match="Invalid resampling method"):
            _string_to_resampling("unknown_method")


class TestComputeCrsBounds:
    def test_returns_five_values(self, geometry):
        result = _compute_crs_bounds(geometry, 40.0, -105.0, "EPSG:3857")
        assert len(result) == 5

    def test_wgs84_projection(self, geometry):
        west, south, east, north, crs = _compute_crs_bounds(geometry, 40.0, -105.0, "EPSG:4326")
        assert west < east
        assert south < north

    def test_web_mercator_projection(self, geometry):
        west, south, east, north, crs = _compute_crs_bounds(geometry, 40.0, -105.0, "EPSG:3857")
        assert west < east
        assert south < north


# ---------------------------------------------------------------------------
# apply_colormap_to_array
# ---------------------------------------------------------------------------


class TestApplyColormapToArray:
    def test_output_shape(self):
        data = np.random.rand(20, 20).astype(np.float32)
        rgba = apply_colormap_to_array(data, "viridis")
        assert rgba.shape == (20, 20, 4)

    def test_output_dtype(self):
        data = np.random.rand(10, 10).astype(np.float32)
        rgba = apply_colormap_to_array(data, "viridis")
        assert rgba.dtype == np.uint8

    def test_nan_produces_transparent_alpha(self):
        data = np.full((10, 10), np.nan)
        rgba = apply_colormap_to_array(data, "viridis")
        assert np.all(rgba[:, :, 3] == 0)

    def test_nodata_mask_by_fill_value(self):
        data = np.ones((10, 10), dtype=np.float32)
        data[2, 2] = -9999.0
        rgba = apply_colormap_to_array(data, "viridis", fill_value=-9999.0)
        assert rgba[2, 2, 3] == 0
        assert rgba[0, 0, 3] == 255

    def test_vmin_vmax_clip(self):
        # Data outside [0, 1] should be clipped, not cause errors
        data = np.array([[-10.0, 0.5, 10.0]], dtype=np.float32)
        rgba = apply_colormap_to_array(data, "viridis", vmin=0.0, vmax=1.0)
        assert rgba.shape == (1, 3, 4)

    def test_colormap_object_accepted(self):
        import matplotlib.pyplot as plt

        data = np.random.rand(5, 5).astype(np.float32)
        cmap = plt.get_cmap("hot")
        rgba = apply_colormap_to_array(data, cmap)
        assert rgba.shape == (5, 5, 4)


# ---------------------------------------------------------------------------
# create_geotiff
# ---------------------------------------------------------------------------


class TestCreateGeotiff:
    def test_creates_file(self, data, geometry, tmp_path):
        out = tmp_path / "test.tif"
        result = create_geotiff(data, geometry, 40.0, -105.0, out, cmap="viridis", vmin=0, vmax=70)
        assert result == out
        assert out.exists()

    def test_file_has_four_bands(self, data, geometry, tmp_path):
        out = tmp_path / "test.tif"
        create_geotiff(data, geometry, 40.0, -105.0, out)
        with rasterio.open(out) as src:
            assert src.count == 4

    def test_metadata_stored(self, data, geometry, tmp_path):
        out = tmp_path / "test.tif"
        create_geotiff(data, geometry, 40.0, -105.0, out, cmap="plasma", vmin=5.0, vmax=65.0)
        with rasterio.open(out) as src:
            tags = src.tags()
        assert tags[_TAG_CMAP] == "plasma"
        assert float(tags[_TAG_VMIN]) == pytest.approx(5.0)
        assert float(tags[_TAG_VMAX]) == pytest.approx(65.0)
        assert tags[_TAG_DATA_TYPE] == _DATA_TYPE_RGBA

    def test_shape_mismatch_raises(self, geometry, tmp_path):
        bad_data = np.zeros((5, 5))
        out = tmp_path / "bad.tif"
        with pytest.raises(ValueError, match="does not match geometry"):
            create_geotiff(bad_data, geometry, 40.0, -105.0, out)


# ---------------------------------------------------------------------------
# create_cog
# ---------------------------------------------------------------------------


class TestCreateCog:
    def test_creates_file(self, data, geometry, tmp_path):
        out = tmp_path / "test.cog"
        result = create_cog(data, geometry, 40.0, -105.0, out, cmap="viridis", vmin=0, vmax=70)
        assert result == out
        assert out.exists()

    def test_file_has_four_bands(self, data, geometry, tmp_path):
        out = tmp_path / "test.cog"
        create_cog(data, geometry, 40.0, -105.0, out)
        with rasterio.open(out) as src:
            assert src.count == 4

    def test_metadata_stored(self, data, geometry, tmp_path):
        out = tmp_path / "test.cog"
        create_cog(data, geometry, 40.0, -105.0, out, cmap="hot", vmin=0.0, vmax=70.0)
        with rasterio.open(out) as src:
            tags = src.tags()
        assert tags[_TAG_CMAP] == "hot"
        assert float(tags[_TAG_VMIN]) == pytest.approx(0.0)
        assert float(tags[_TAG_VMAX]) == pytest.approx(70.0)
        assert tags[_TAG_DATA_TYPE] == _DATA_TYPE_RGBA

    def test_vmin_vmax_inferred_when_none(self, data, geometry, tmp_path):
        out = tmp_path / "test.cog"
        create_cog(data, geometry, 40.0, -105.0, out, cmap="viridis")
        meta = read_cog_metadata(out)
        assert meta["vmin"] is not None
        assert meta["vmax"] is not None

    def test_overview_factors_invalid_type_raises(self, data, geometry, tmp_path):
        out = tmp_path / "test.cog"
        with pytest.raises(TypeError):
            create_cog(data, geometry, 40.0, -105.0, out, overview_factors=(2, 4))

    def test_shape_mismatch_raises(self, geometry, tmp_path):
        bad_data = np.zeros((5, 5))
        out = tmp_path / "bad.cog"
        with pytest.raises(ValueError, match="does not match geometry"):
            create_cog(bad_data, geometry, 40.0, -105.0, out)


# ---------------------------------------------------------------------------
# create_raw_cog
# ---------------------------------------------------------------------------


class TestCreateRawCog:
    def test_creates_file(self, data, geometry, tmp_path):
        out = tmp_path / "test_raw.cog"
        result = create_raw_cog(data, geometry, 40.0, -105.0, out, cmap="viridis", vmin=0, vmax=70)
        assert result == out
        assert out.exists()

    def test_single_float_band(self, data, geometry, tmp_path):
        out = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, out)
        with rasterio.open(out) as src:
            assert src.count == 1
            assert np.issubdtype(src.dtypes[0], np.floating)

    def test_metadata_stored(self, data, geometry, tmp_path):
        out = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, out, cmap="jet", vmin=5.0, vmax=60.0)
        with rasterio.open(out) as src:
            tags = src.tags()
        assert tags[_TAG_CMAP] == "jet"
        assert float(tags[_TAG_VMIN]) == pytest.approx(5.0)
        assert float(tags[_TAG_VMAX]) == pytest.approx(60.0)
        assert tags[_TAG_DATA_TYPE] == _DATA_TYPE_RAW

    def test_data_values_preserved(self, geometry, tmp_path):
        _, ny, nx = geometry.grid_shape
        data = np.linspace(0, 100, ny * nx).reshape(ny, nx).astype(np.float32)
        out = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, out)
        with rasterio.open(out) as src:
            stored = src.read(1)
        # After flipud and re-read the values should still span [0, 100]
        assert stored.min() == pytest.approx(0.0, abs=1e-3)
        assert stored.max() == pytest.approx(100.0, abs=1e-3)

    def test_shape_mismatch_raises(self, geometry, tmp_path):
        bad_data = np.zeros((5, 5))
        out = tmp_path / "bad.cog"
        with pytest.raises(ValueError, match="does not match geometry"):
            create_raw_cog(bad_data, geometry, 40.0, -105.0, out)


# ---------------------------------------------------------------------------
# read_cog_metadata
# ---------------------------------------------------------------------------


class TestReadCogMetadata:
    def test_returns_dict_for_rgba_cog(self, data, geometry, tmp_path):
        out = tmp_path / "cog.cog"
        create_cog(data, geometry, 40.0, -105.0, out, cmap="plasma", vmin=0.0, vmax=70.0)
        meta = read_cog_metadata(out)
        assert meta["cmap"] == "plasma"
        assert meta["vmin"] == pytest.approx(0.0)
        assert meta["vmax"] == pytest.approx(70.0)
        assert meta["data_type"] == _DATA_TYPE_RGBA

    def test_returns_dict_for_raw_cog(self, data, geometry, tmp_path):
        out = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, out, cmap="hot", vmin=10.0, vmax=60.0)
        meta = read_cog_metadata(out)
        assert meta["cmap"] == "hot"
        assert meta["vmin"] == pytest.approx(10.0)
        assert meta["vmax"] == pytest.approx(60.0)
        assert meta["data_type"] == _DATA_TYPE_RAW

    def test_missing_tags_return_none(self, tmp_path):
        # Create a minimal rasterio file without radarlib tags
        out = tmp_path / "plain.tif"
        with rasterio.open(
            out, "w", driver="GTiff", height=4, width=4, count=1, dtype=np.float32
        ) as dst:
            dst.write(np.zeros((1, 4, 4), dtype=np.float32))
        meta = read_cog_metadata(out)
        assert meta["cmap"] is None
        assert meta["vmin"] is None
        assert meta["data_type"] is None


# ---------------------------------------------------------------------------
# remap_cog_colormap
# ---------------------------------------------------------------------------


class TestRemapCogColormap:
    def test_creates_new_rgba_cog(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        out = tmp_path / "remapped.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, cmap="viridis", vmin=0, vmax=70)
        result = remap_cog_colormap(raw, out, new_cmap="hot")
        assert result == out
        assert out.exists()

    def test_output_has_four_bands(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        out = tmp_path / "remapped.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw)
        remap_cog_colormap(raw, out, new_cmap="plasma")
        with rasterio.open(out) as src:
            assert src.count == 4

    def test_metadata_updated_with_new_cmap(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        out = tmp_path / "remapped.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, cmap="viridis", vmin=0, vmax=70)
        remap_cog_colormap(raw, out, new_cmap="hot")
        meta = read_cog_metadata(out)
        assert meta["cmap"] == "hot"
        assert meta["data_type"] == _DATA_TYPE_RGBA

    def test_new_vmin_vmax_override_metadata(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        out = tmp_path / "remapped.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, cmap="viridis", vmin=0, vmax=70)
        remap_cog_colormap(raw, out, new_cmap="hot", new_vmin=10.0, new_vmax=60.0)
        meta = read_cog_metadata(out)
        assert meta["vmin"] == pytest.approx(10.0)
        assert meta["vmax"] == pytest.approx(60.0)

    def test_fallback_to_metadata_vmin_vmax(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        out = tmp_path / "remapped.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, cmap="viridis", vmin=5.0, vmax=65.0)
        remap_cog_colormap(raw, out, new_cmap="jet")
        meta = read_cog_metadata(out)
        assert meta["vmin"] == pytest.approx(5.0)
        assert meta["vmax"] == pytest.approx(65.0)

    def test_crs_preserved(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        out = tmp_path / "remapped.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, projection="EPSG:3857")
        remap_cog_colormap(raw, out, new_cmap="viridis")
        with rasterio.open(raw) as src_raw, rasterio.open(out) as src_out:
            assert src_raw.crs == src_out.crs

    def test_raises_on_rgba_input(self, data, geometry, tmp_path):
        rgba_cog = tmp_path / "rgba.cog"
        out = tmp_path / "out.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba_cog)
        with pytest.raises(ValueError, match="RGBA COG"):
            remap_cog_colormap(rgba_cog, out, new_cmap="hot")

    def test_different_colormaps_produce_different_pixels(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        out1 = tmp_path / "viridis.cog"
        out2 = tmp_path / "hot.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, vmin=0, vmax=70)
        remap_cog_colormap(raw, out1, new_cmap="viridis")
        remap_cog_colormap(raw, out2, new_cmap="hot")
        with rasterio.open(out1) as s1, rasterio.open(out2) as s2:
            r1 = s1.read(1)
            r2 = s2.read(1)
        # Red channel should differ between colormaps
        assert not np.array_equal(r1, r2)


# ---------------------------------------------------------------------------
# read_cog_tile_as_rgba
# ---------------------------------------------------------------------------


class TestReadCogTileAsRgba:
    def test_returns_rgba_from_raw_cog(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, cmap="viridis", vmin=0, vmax=70)
        rgba = read_cog_tile_as_rgba(raw)
        assert rgba.ndim == 3
        assert rgba.shape[2] == 4
        assert rgba.dtype == np.uint8

    def test_custom_cmap_applied(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, cmap="viridis", vmin=0, vmax=70)
        rgba_viridis = read_cog_tile_as_rgba(raw, cmap="viridis")
        rgba_hot = read_cog_tile_as_rgba(raw, cmap="hot")
        # Different colormaps → different pixel values
        assert not np.array_equal(rgba_viridis, rgba_hot)

    def test_returns_rgba_from_rgba_cog(self, data, geometry, tmp_path):
        cog = tmp_path / "rgba.cog"
        create_cog(data, geometry, 40.0, -105.0, cog, cmap="viridis", vmin=0, vmax=70)
        rgba = read_cog_tile_as_rgba(cog)
        assert rgba.ndim == 3
        assert rgba.shape[2] == 4
        assert rgba.dtype == np.uint8

    def test_overview_level_zero_is_full_resolution(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        _, ny, nx = geometry.grid_shape
        create_raw_cog(data, geometry, 40.0, -105.0, raw, overview_factors=[2])
        rgba = read_cog_tile_as_rgba(raw, overview_level=0)
        assert rgba.shape[0] == ny
        assert rgba.shape[1] == nx

    def test_invalid_overview_level_raises(self, data, geometry, tmp_path):
        raw = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw, overview_factors=[2])
        with pytest.raises(ValueError, match="overview_level"):
            read_cog_tile_as_rgba(raw, overview_level=99)


# ---------------------------------------------------------------------------
# _is_rgba_cog helper
# ---------------------------------------------------------------------------


class TestIsRgbaCog:
    def test_tagged_rgba_returns_true(self, data, geometry, tmp_path):
        from radarlib.radar_grid.geotiff import _is_rgba_cog

        out = tmp_path / "rgba.cog"
        create_cog(data, geometry, 40.0, -105.0, out)
        meta = read_cog_metadata(out)
        with rasterio.open(out) as src:
            assert _is_rgba_cog(meta, src.count, src.dtypes[0]) is True

    def test_tagged_raw_returns_false(self, data, geometry, tmp_path):
        from radarlib.radar_grid.geotiff import _is_rgba_cog

        out = tmp_path / "raw.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, out)
        meta = read_cog_metadata(out)
        with rasterio.open(out) as src:
            assert _is_rgba_cog(meta, src.count, src.dtypes[0]) is False

    def test_untagged_4band_uint8_returns_true(self, tmp_path):
        """Legacy file: 4 bands, uint8, no radarlib tag → treated as RGBA."""
        from radarlib.radar_grid.geotiff import _is_rgba_cog

        untagged = tmp_path / "legacy.tif"
        with rasterio.open(
            untagged,
            "w",
            driver="GTiff",
            height=4,
            width=4,
            count=4,
            dtype=np.uint8,
        ) as dst:
            for b in range(1, 5):
                dst.write(np.zeros((4, 4), dtype=np.uint8), b)
        meta = read_cog_metadata(untagged)
        with rasterio.open(untagged) as src:
            assert _is_rgba_cog(meta, src.count, src.dtypes[0]) is True

    def test_untagged_1band_float_returns_false(self, tmp_path):
        from radarlib.radar_grid.geotiff import _is_rgba_cog

        f = tmp_path / "float.tif"
        with rasterio.open(
            f,
            "w",
            driver="GTiff",
            height=4,
            width=4,
            count=1,
            dtype=np.float32,
        ) as dst:
            dst.write(np.zeros((1, 4, 4), dtype=np.float32))
        meta = read_cog_metadata(f)
        with rasterio.open(f) as src:
            assert _is_rgba_cog(meta, src.count, src.dtypes[0]) is False


# ---------------------------------------------------------------------------
# remap_cog_colormap — updated error detection for legacy RGBA files
# ---------------------------------------------------------------------------


class TestRemapCogColormapLegacyDetection:
    def test_raises_on_legacy_untagged_rgba_cog(self, tmp_path):
        """remap_cog_colormap must reject legacy 4-band uint8 files even without tags."""
        legacy = tmp_path / "legacy.cog"
        with rasterio.open(
            legacy,
            "w",
            driver="GTiff",
            height=4,
            width=4,
            count=4,
            dtype=np.uint8,
            crs="EPSG:3857",
        ) as dst:
            for b in range(1, 5):
                dst.write(np.full((4, 4), 128, dtype=np.uint8), b)
        out = tmp_path / "out.cog"
        with pytest.raises(ValueError, match="RGBA COG"):
            remap_cog_colormap(legacy, out, new_cmap="hot")

    def test_error_message_mentions_convert(self, data, geometry, tmp_path):
        """The error message for RGBA input should mention convert_rgba_cog_to_raw."""
        rgba_cog = tmp_path / "rgba.cog"
        out = tmp_path / "out.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba_cog)
        with pytest.raises(ValueError, match="convert_rgba_cog_to_raw"):
            remap_cog_colormap(rgba_cog, out, new_cmap="hot")


# ---------------------------------------------------------------------------
# _build_colormap_lut helper
# ---------------------------------------------------------------------------


class TestBuildColormapLut:
    def test_shapes(self):
        from radarlib.radar_grid.geotiff import _build_colormap_lut

        lut_float, lut_rgb = _build_colormap_lut("viridis", 0.0, 70.0, 256)
        assert lut_float.shape == (256,)
        assert lut_rgb.shape == (256, 3)
        assert lut_rgb.dtype == np.uint8

    def test_float_range(self):
        from radarlib.radar_grid.geotiff import _build_colormap_lut

        lut_float, _ = _build_colormap_lut("viridis", 10.0, 60.0, 100)
        assert lut_float[0] == pytest.approx(10.0)
        assert lut_float[-1] == pytest.approx(60.0)

    def test_accepts_colormap_object(self):
        import matplotlib.pyplot as plt

        from radarlib.radar_grid.geotiff import _build_colormap_lut

        cmap = plt.get_cmap("plasma")
        lut_float, lut_rgb = _build_colormap_lut(cmap, 0.0, 1.0, 64)
        assert lut_rgb.shape == (64, 3)


# ---------------------------------------------------------------------------
# _invert_colormap_to_float helper
# ---------------------------------------------------------------------------


class TestInvertColormapToFloat:
    def test_transparent_pixels_become_nan(self):
        from radarlib.radar_grid.geotiff import _invert_colormap_to_float

        rgba = np.zeros((10, 10, 4), dtype=np.uint8)
        result = _invert_colormap_to_float(rgba, "viridis", 0.0, 70.0)
        assert np.all(np.isnan(result))

    def test_round_trip_approximate(self):
        """Apply colormap, then invert — should recover values within uint8 precision."""
        from radarlib.radar_grid.geotiff import _invert_colormap_to_float

        original = np.linspace(10.0, 60.0, 25, dtype=np.float32).reshape(5, 5)
        rgba = apply_colormap_to_array(original, "viridis", vmin=0.0, vmax=70.0)
        # Mark a few pixels as transparent (no-data)
        rgba[0, 0, 3] = 0
        rgba[4, 4, 3] = 0

        recovered = _invert_colormap_to_float(rgba, "viridis", 0.0, vmax=70.0)

        assert np.isnan(recovered[0, 0])
        assert np.isnan(recovered[4, 4])

        opaque_mask = rgba[:, :, 3] > 0
        # Max error should be within uint8 quantisation range
        tolerance = (70.0 - 0.0) / 254.0 + 0.5  # generous for colourmap non-linearity
        err = np.abs(original[opaque_mask] - recovered[opaque_mask])
        assert err.max() < tolerance

    def test_output_dtype_float32(self):
        from radarlib.radar_grid.geotiff import _invert_colormap_to_float

        original = np.full((4, 4), 35.0, dtype=np.float32)
        rgba = apply_colormap_to_array(original, "hot", vmin=0.0, vmax=70.0)
        result = _invert_colormap_to_float(rgba, "hot", 0.0, 70.0)
        assert result.dtype == np.float32


# ---------------------------------------------------------------------------
# convert_rgba_cog_to_raw
# ---------------------------------------------------------------------------


def _make_legacy_rgba_cog(path, data, geometry, cmap="viridis", vmin=0.0, vmax=70.0):
    """Create an RGBA COG that simulates a file from the OLD radarlib (no metadata tags)."""
    # Manually write 4-band uint8 tiff with geo-referencing but without radarlib tags
    rgba = apply_colormap_to_array(data, cmap, vmin, vmax)
    rgba = np.flipud(rgba)

    import pyproj
    from rasterio.transform import from_bounds as _from_bounds

    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]
    local_proj = pyproj.Proj(proj="aeqd", lat_0=40.0, lon_0=-105.0, x_0=0, y_0=0, datum="WGS84")
    wgs84 = pyproj.CRS("EPSG:4326")
    t = pyproj.Transformer.from_proj(local_proj, wgs84, always_xy=True)
    lons, lats = [], []
    for x in [x_min, x_max]:
        for y in [y_min, y_max]:
            lon, lat = t.transform(x, y)
            lons.append(lon)
            lats.append(lat)

    ny, nx = data.shape
    transform = _from_bounds(min(lons), min(lats), max(lons), max(lats), nx, ny)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=ny,
        width=nx,
        count=4,
        dtype=np.uint8,
        crs="EPSG:4326",
        transform=transform,
        compress="DEFLATE",
        tiled=True,
    ) as dst:
        dst.write(rgba[:, :, 0], 1)
        dst.write(rgba[:, :, 1], 2)
        dst.write(rgba[:, :, 2], 3)
        dst.write(rgba[:, :, 3], 4)


class TestConvertRgbaCogToRaw:
    def test_creates_file(self, data, geometry, tmp_path):
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba, cmap="viridis", vmin=0.0, vmax=70.0)
        result = convert_rgba_cog_to_raw(rgba, raw)
        assert result == raw
        assert raw.exists()

    def test_output_is_single_float_band(self, data, geometry, tmp_path):
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba, cmap="viridis", vmin=0.0, vmax=70.0)
        convert_rgba_cog_to_raw(rgba, raw)
        with rasterio.open(raw) as src:
            assert src.count == 1
            assert np.issubdtype(src.dtypes[0], np.floating)

    def test_metadata_stored_correctly(self, data, geometry, tmp_path):
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba, cmap="plasma", vmin=5.0, vmax=65.0)
        convert_rgba_cog_to_raw(rgba, raw)
        meta = read_cog_metadata(raw)
        assert meta["cmap"] == "plasma"
        assert meta["vmin"] == pytest.approx(5.0)
        assert meta["vmax"] == pytest.approx(65.0)
        assert meta["data_type"] == _DATA_TYPE_RAW

    def test_crs_and_transform_preserved(self, data, geometry, tmp_path):
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba)
        convert_rgba_cog_to_raw(rgba, raw)
        with rasterio.open(rgba) as src_rgba, rasterio.open(raw) as src_raw:
            assert src_rgba.crs == src_raw.crs
            assert src_rgba.transform == pytest.approx(src_raw.transform, abs=1e-6)

    def test_transparent_pixels_become_nan(self, geometry, tmp_path):
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        _, ny, nx = geometry.grid_shape
        # All-NaN data → all transparent pixels → all NaN after conversion
        all_nan = np.full((ny, nx), np.nan, dtype=np.float32)
        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        create_cog(all_nan, geometry, 40.0, -105.0, rgba, cmap="viridis", vmin=0.0, vmax=70.0)
        convert_rgba_cog_to_raw(rgba, raw, cmap="viridis", vmin=0.0, vmax=70.0)
        with rasterio.open(raw) as src:
            stored = src.read(1)
        assert np.all(np.isnan(stored))

    def test_uses_file_metadata_when_args_omitted(self, data, geometry, tmp_path):
        """When cmap/vmin/vmax are not supplied, file metadata is used."""
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba, cmap="hot", vmin=10.0, vmax=60.0)
        # No explicit cmap/vmin/vmax — should be read from file metadata
        convert_rgba_cog_to_raw(rgba, raw)
        meta = read_cog_metadata(raw)
        assert meta["cmap"] == "hot"
        assert meta["vmin"] == pytest.approx(10.0)
        assert meta["vmax"] == pytest.approx(60.0)

    def test_caller_args_override_file_metadata(self, data, geometry, tmp_path):
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba, cmap="viridis", vmin=0.0, vmax=70.0)
        # Override with explicit values
        convert_rgba_cog_to_raw(rgba, raw, cmap="plasma", vmin=5.0, vmax=65.0)
        meta = read_cog_metadata(raw)
        assert meta["cmap"] == "plasma"
        assert meta["vmin"] == pytest.approx(5.0)
        assert meta["vmax"] == pytest.approx(65.0)

    def test_raises_when_no_metadata_and_no_args(self, tmp_path):
        """Legacy file with no metadata and no caller args must raise a helpful error."""
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        legacy = tmp_path / "legacy.tif"
        with rasterio.open(
            legacy,
            "w",
            driver="GTiff",
            height=4,
            width=4,
            count=4,
            dtype=np.uint8,
        ) as dst:
            for b in range(1, 5):
                dst.write(np.full((4, 4), 100, dtype=np.uint8), b)
        out = tmp_path / "out.cog"
        with pytest.raises(ValueError, match="cmap"):
            convert_rgba_cog_to_raw(legacy, out)

    def test_raises_on_raw_float_input(self, data, geometry, tmp_path):
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        raw_in = tmp_path / "raw_in.cog"
        raw_out = tmp_path / "raw_out.cog"
        create_raw_cog(data, geometry, 40.0, -105.0, raw_in)
        with pytest.raises(ValueError, match="already a raw float COG"):
            convert_rgba_cog_to_raw(raw_in, raw_out)

    def test_legacy_file_without_metadata(self, data, geometry, tmp_path):
        """Convert a legacy RGBA COG (no radarlib tags) — user supplies cmap/vmin/vmax."""
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        _, ny, nx = geometry.grid_shape
        legacy = tmp_path / "legacy.tif"
        _make_legacy_rgba_cog(legacy, data, geometry, cmap="viridis", vmin=0.0, vmax=70.0)

        raw = tmp_path / "converted.cog"
        convert_rgba_cog_to_raw(legacy, raw, cmap="viridis", vmin=0.0, vmax=70.0)

        meta = read_cog_metadata(raw)
        assert meta["data_type"] == _DATA_TYPE_RAW
        assert meta["cmap"] == "viridis"

        with rasterio.open(raw) as src:
            assert src.count == 1
            stored = src.read(1)
        # Some non-NaN values should exist (non-NaN data was written)
        assert not np.all(np.isnan(stored))

    def test_approximate_values_within_quantisation_tolerance(self, geometry, tmp_path):
        """After round-trip (raw→RGBA COG→convert back), values should be close."""
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        _, ny, nx = geometry.grid_shape
        original = np.linspace(10.0, 60.0, ny * nx, dtype=np.float32).reshape(ny, nx)

        rgba_cog = tmp_path / "rgba.cog"
        raw_out = tmp_path / "recovered.cog"
        create_cog(original, geometry, 40.0, -105.0, rgba_cog, cmap="viridis", vmin=0.0, vmax=70.0)
        convert_rgba_cog_to_raw(rgba_cog, raw_out)

        with rasterio.open(raw_out) as src:
            recovered = src.read(1)  # stored top-to-bottom (rasterio convention)

        # create_cog flips the array before writing; the stored float data is also
        # top-to-bottom, so we must flip it back to the same orientation as `original`
        # before comparing values pixel-by-pixel.
        recovered_same_orientation = np.flipud(recovered)

        # Quantisation step for uint8 over [0, 70]
        quant_step = 70.0 / 254.0
        # Allow a generous multiple to account for colormap non-linearity
        tolerance = quant_step * 2 + 0.5
        diff = np.abs(original - recovered_same_orientation)
        assert diff.max() < tolerance

    def test_result_can_be_remapped(self, data, geometry, tmp_path):
        """After conversion, the raw COG should work with remap_cog_colormap."""
        from radarlib.radar_grid import convert_rgba_cog_to_raw

        rgba = tmp_path / "rgba.cog"
        raw = tmp_path / "raw.cog"
        out = tmp_path / "remapped.cog"
        create_cog(data, geometry, 40.0, -105.0, rgba, cmap="viridis", vmin=0.0, vmax=70.0)
        convert_rgba_cog_to_raw(rgba, raw)
        result = remap_cog_colormap(raw, out, new_cmap="hot")
        assert result.exists()
        meta = read_cog_metadata(out)
        assert meta["cmap"] == "hot"
        assert meta["data_type"] == _DATA_TYPE_RGBA
