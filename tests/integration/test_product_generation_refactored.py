# -*- coding: utf-8 -*-
"""Integration tests for the refactored COG product generation pipeline.

These tests use **real sample NetCDF files** from ``outputs/`` and pre-computed
geometry files from ``app/data/geometries/`` to validate the full pipeline
end-to-end:

    NetCDF load → estandarizar_campos_RMA
                → build_product_metadata
                → apply_metadata_to_cog
                → GeoTIFF with radarlib tags

The tests are marked with ``@pytest.mark.integration`` and are skipped
automatically when the required sample data files are absent.

Run only integration tests::

    pytest tests/integration/ -m integration -v

Skip integration tests::

    pytest tests/ -m "not integration"
"""

# import shutil
# import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

# from typing import Generator


# ---------------------------------------------------------------------------
# Paths to sample data (relative to repo root, resolved at module level)
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent.parent
_OUTPUTS_DIR = _REPO_ROOT / "outputs"
_GEOMETRIES_DIR = _REPO_ROOT / "app" / "data" / "geometries"

# Known sample NetCDF volume with matching geometry
_SAMPLE_NC = _OUTPUTS_DIR / "RMA1_0315_01_20260423T070721Z.nc"
_SAMPLE_GEOMETRY = next(iter(sorted(_GEOMETRIES_DIR.glob("RMA1_0315_01_*.npz"))), None)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def radar_object():
    """Load and standardise the RMA1 vol-01 NetCDF into a PyART Radar object.

    Skipped when the sample file is absent.
    """
    if not _SAMPLE_NC.exists():
        pytest.skip(f"Sample NetCDF not found: {_SAMPLE_NC}")

    from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf

    radar = read_radar_netcdf(str(_SAMPLE_NC))
    radar = estandarizar_campos_RMA(radar)
    return radar


@pytest.fixture(scope="module")
def geometry():
    """Load the precomputed GridGeometry for RMA1 vol-01.

    Skipped when no matching geometry file is found.
    """
    if _SAMPLE_GEOMETRY is None or not _SAMPLE_GEOMETRY.exists():
        pytest.skip(f"No geometry file found in {_GEOMETRIES_DIR} for RMA1_0315_01")

    from radarlib.radar_grid.geometry import load_geometry

    return load_geometry(str(_SAMPLE_GEOMETRY))


@pytest.fixture()
def tmp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for COG output files."""
    out_dir = tmp_path / "products"
    out_dir.mkdir()
    return out_dir


# ---------------------------------------------------------------------------
# ProductMetadata — real radar object
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBuildProductMetadataWithRealRadar:
    def test_build_metadata_returns_product_metadata(self, radar_object: object) -> None:
        from radarlib.daemons.metadata_utils import build_product_metadata

        volume_info = {
            "vol_nr": "01",
            "strategy": "0315",
            "observation_datetime": "20260423T070721Z",
        }
        meta = build_product_metadata(radar_object, volume_info, "DBZH", "RMA1")
        from radarlib.daemons.product_metadata import ProductMetadata

        assert isinstance(meta, ProductMetadata)

    def test_radar_coverage_is_positive(self, radar_object: object) -> None:
        from radarlib.daemons.metadata_utils import build_product_metadata

        volume_info = {
            "vol_nr": "01",
            "strategy": "0315",
            "observation_datetime": "20260423T070721Z",
        }
        meta = build_product_metadata(radar_object, volume_info, "DBZH", "RMA1")
        assert meta.radar_coverage_m > 0

    def test_radar_coverage_reasonable_range(self, radar_object: object) -> None:
        """Radar coverage should be between 10 km and 600 km."""
        from radarlib.daemons.metadata_utils import build_product_metadata

        volume_info = {
            "vol_nr": "01",
            "strategy": "0315",
            "observation_datetime": "20260423T070721Z",
        }
        meta = build_product_metadata(radar_object, volume_info, "DBZH", "RMA1")
        assert 10_000.0 <= meta.radar_coverage_m <= 600_000.0

    def test_observation_timestamp_is_utc(self, radar_object: object) -> None:
        from radarlib.daemons.metadata_utils import build_product_metadata

        volume_info = {
            "vol_nr": "01",
            "strategy": "0315",
            "observation_datetime": "20260423T070721Z",
        }
        meta = build_product_metadata(radar_object, volume_info, "DBZH", "RMA1")
        assert meta.observation_timestamp.tzinfo is timezone.utc

    def test_observation_timestamp_matches_filename(self, radar_object: object) -> None:
        from radarlib.daemons.metadata_utils import build_product_metadata

        volume_info = {
            "vol_nr": "01",
            "strategy": "0315",
            "observation_datetime": "20260423T070721Z",
        }
        meta = build_product_metadata(radar_object, volume_info, "DBZH", "RMA1")
        assert meta.observation_timestamp == datetime(2026, 4, 23, 7, 7, 21, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# apply_metadata_to_cog — real GeoTIFF write/read round-trip
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestApplyMetadataToCogRealFile:
    def test_tags_written_and_readable(self, radar_object: object, tmp_path: Path) -> None:
        """Tags written by apply_metadata_to_cog are readable back from the file."""
        import rasterio
        from rasterio.transform import from_bounds

        from radarlib.daemons.metadata_utils import apply_metadata_to_cog, build_product_metadata

        cog_path = tmp_path / "test.tif"
        data = np.zeros((1, 20, 20), dtype=np.float32)
        transform = from_bounds(-65, -35, -60, -30, 20, 20)
        with rasterio.open(
            cog_path,
            "w",
            driver="GTiff",
            height=20,
            width=20,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            dst.write(data)

        volume_info = {
            "vol_nr": "01",
            "strategy": "0315",
            "observation_datetime": "20260423T070721Z",
        }
        meta = build_product_metadata(radar_object, volume_info, "DBZH", "RMA1")
        apply_metadata_to_cog(cog_path, meta)

        with rasterio.open(cog_path) as src:
            tags = src.tags()

        assert tags["radarlib_field_name"] == "DBZH"
        assert tags["radarlib_radar_name"] == "RMA1"
        assert tags["radarlib_strategy"] == "0315"
        assert tags["radarlib_volume_number"] == "1"

    def test_all_required_tag_keys_present(self, radar_object: object, tmp_path: Path) -> None:
        """All expected radarlib tag keys are present in the output GeoTIFF."""
        import rasterio
        from rasterio.transform import from_bounds

        from radarlib.daemons.metadata_utils import apply_metadata_to_cog, build_product_metadata

        cog_path = tmp_path / "check_keys.tif"
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

        volume_info = {
            "vol_nr": "01",
            "strategy": "0315",
            "observation_datetime": "20260423T070721Z",
        }
        meta = build_product_metadata(radar_object, volume_info, "DBZH", "RMA1")
        apply_metadata_to_cog(cog_path, meta)

        with rasterio.open(cog_path) as src:
            tags = src.tags()

        expected_keys = {
            "radarlib_volume_number",
            "radarlib_strategy",
            "radarlib_field_name",
            "radarlib_radar_name",
            "radarlib_radar_coverage_m",
            "radarlib_observation_timestamp",
            "radarlib_processing_timestamp",
            "radarlib_processing_version",
            "radarlib_filtered",
        }
        assert expected_keys.issubset(set(tags.keys()))


# ---------------------------------------------------------------------------
# GridGeometry: load from disk
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLoadGeometry:
    def test_load_geometry_returns_grid_geometry(self, geometry: object) -> None:
        from radarlib.radar_grid.geometry import GridGeometry

        assert isinstance(geometry, GridGeometry)

    def test_geometry_has_non_zero_shape(self, geometry: object) -> None:
        from radarlib.radar_grid.geometry import GridGeometry

        assert isinstance(geometry, GridGeometry)
        nz, ny, nx = geometry.grid_shape
        assert nz > 0 and ny > 0 and nx > 0

    def test_geometry_has_gate_indices(self, geometry: object) -> None:
        assert len(geometry.gate_indices) > 0

    def test_geometry_has_weights(self, geometry: object) -> None:
        assert len(geometry.weights) > 0

    def test_geometry_indptr_length(self, geometry: object) -> None:
        expected_len = int(np.prod(geometry.grid_shape)) + 1
        assert len(geometry.indptr) == expected_len


# ---------------------------------------------------------------------------
# Radar fields: validate loaded radar has expected fields
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLoadedRadarFields:
    def test_radar_has_fields(self, radar_object: object) -> None:
        assert len(radar_object.fields) > 0

    def test_radar_has_dbzh_or_equivalent(self, radar_object: object) -> None:
        """The standardised radar should have a reflectivity-like field."""
        known_reflectivity_names = {"DBZH", "DBZ", "TH", "reflectivity"}
        present = set(radar_object.fields.keys())
        assert len(present & known_reflectivity_names) > 0, f"No reflectivity field found — available: {list(present)}"

    def test_radar_range_array_non_empty(self, radar_object: object) -> None:
        assert len(radar_object.range["data"]) > 0

    def test_radar_has_latitude(self, radar_object: object) -> None:
        assert radar_object.latitude is not None

    def test_radar_has_longitude(self, radar_object: object) -> None:
        assert radar_object.longitude is not None

    def test_radar_field_data_is_masked_array(self, radar_object: object) -> None:
        first_field = next(iter(radar_object.fields.keys()))
        assert np.ma.isMaskedArray(radar_object.fields[first_field]["data"])


# ---------------------------------------------------------------------------
# v2 filename: generate path and verify directory is created
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestProductPathAndFilenameIntegration:
    def test_generates_tif_path(self, tmp_output_dir: Path) -> None:
        from radarlib.utils.names_utils import product_path_and_filename

        ts = datetime(2026, 4, 23, 7, 7, 21, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_output_dir)
        assert path.suffix == ".tif"

    def test_directory_created_on_disk(self, tmp_output_dir: Path) -> None:
        from radarlib.utils.names_utils import product_path_and_filename

        ts = datetime(2026, 4, 23, 7, 7, 21, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_output_dir)
        assert path.parent.is_dir()

    def test_extract_round_trips_correctly(self, tmp_output_dir: Path) -> None:
        from radarlib.utils.names_utils import extract_cog_filename_components_v2, product_path_and_filename

        ts = datetime(2026, 4, 23, 7, 7, 21, tzinfo=timezone.utc)
        path = product_path_and_filename("RMA1", "0315", "01", "DBZH", ts, tmp_output_dir, filtered=True)
        components = extract_cog_filename_components_v2(path.name)
        assert components["radar_name"] == "RMA1"
        assert components["strategy"] == "0315"
        assert components["vol_nr"] == "01"
        assert components["field_name"] == "DBZH"
        assert components["filtered"] is True
        assert components["timestamp"] == "20260423T070721Z"
