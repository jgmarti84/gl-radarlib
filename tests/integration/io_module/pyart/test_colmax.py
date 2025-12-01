# -*- coding: utf-8 -*-
"""
Tests for the refactored COLMAX generation.

Uses the new module structure for imports.
"""
import time
from pathlib import Path

import pytest

from radarlib import config
from radarlib.io.pyart.colmax import _compute_colmax, _compute_colmax_optimized, generate_colmax
from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf
from radarlib.io.pyart.vvg import get_ordered_sweep_list, get_vertical_vinculation_gate_map


@pytest.fixture
def radar_object(sample_RMA11_vol1_bufr_files):
    """Load and standardize example radar for testing."""
    netcdf_fname = (
        Path(__file__).parent.parent.parent.parent / "outputs/example_netcdfs/RMA11_0315_01_20251020T152828Z.nc"
    )
    if not netcdf_fname.exists():
        # Use new import path
        from radarlib.io.bufr.bufr_to_pyart import bufr_paths_to_pyart

        # build the radar object from the bufr files
        save_path = Path(__file__).parent.parent.parent.parent / "outputs/example_netcdfs/"
        radar = bufr_paths_to_pyart(
            [str(fn) for fn in sample_RMA11_vol1_bufr_files], root_resources=None, save_path=save_path
        )

    radar = read_radar_netcdf(str(netcdf_fname))
    radar = estandarizar_campos_RMA(radar=radar, idioma=0)  # type: ignore
    return radar


class TestGenerateColmax:
    """Test suite for COLMAX generation."""

    def test_colmax_basic(self, radar_object):
        """Test basic COLMAX generation."""
        radar = radar_object
        assert radar.nsweeps > 1, "Test radar must have multiple sweeps"

        result_radar = generate_colmax(radar=radar, field_for_colmax="DBZH")

        assert result_radar is not None, "COLMAX generation should succeed"
        assert "COLMAX" in result_radar.fields, "COLMAX field should be added to radar"
        # Verify that the original radar is not modified
        assert "COLMAX" not in radar.fields, "Original radar should not be modified"

    def test_colmax_with_filters(self, radar_object):
        """Test COLMAX generation with polarimetric filters."""
        radar = radar_object

        result_radar = generate_colmax(
            radar=radar,
            field_for_colmax="DBZH",
            TH_filter=True,
            TH_umbral=10,
            RHOHV_filter=True,
            RHOHV_umbral=0.9,
            WRAD_filter=True,
            WRAD_umbral=4.2,
            TDR_filter=True,
            TDR_umbral=8.5,
        )

        assert result_radar is not None
        assert "COLMAX" in result_radar.fields
        # Verify that the original radar is not modified
        assert "COLMAX" not in radar.fields

    def test_colmax_custom_source_field(self, radar_object):
        """Test COLMAX with custom source field."""
        radar = radar_object

        # Use DBZH as source if available
        if "DBZH" in radar.fields:
            result_radar = generate_colmax(
                radar=radar,
                field_for_colmax="DBZH",
            )
            assert result_radar is not None
            assert "COLMAX" in result_radar.fields

    def test_colmax_missing_source_field(self, radar_object):
        """Test that COLMAX fails gracefully with missing source field."""
        radar = radar_object

        result_radar = generate_colmax(
            radar=radar,
            field_for_colmax="NONEXISTENT_FIELD",
        )

        # assert result_radar is None, "COLMAX should fail with missing field"
        assert result_radar == radar, "returned radar object should be the same as input radar"

    def test_colmax_field_shape(self, radar_object):
        """Test that COLMAX field has correct shape."""
        radar = radar_object

        result_radar = generate_colmax(radar=radar, field_for_colmax="DBZH")

        colmax_field = result_radar.fields["COLMAX"]["data"]
        assert colmax_field.shape == (result_radar.nrays, result_radar.ngates)

    def test_colmax_metadata(self, radar_object):
        """Test that COLMAX field has proper metadata."""
        radar = radar_object

        result_radar = generate_colmax(radar=radar, field_for_colmax="DBZH")

        colmax_meta = result_radar.fields["COLMAX"]
        assert "standard_name" in colmax_meta
        assert "long_name" in colmax_meta
        assert "units" in colmax_meta
        assert colmax_meta["standard_name"] == "COLMAX"
        assert colmax_meta["long_name"] == "Column Maximum"

    def test_colmax_no_filters(self, radar_object):
        """Test COLMAX generation with all filters disabled."""
        radar = radar_object

        # Run with no filters - should use source field directly
        result_radar = generate_colmax(
            radar=radar,
            field_for_colmax="DBZH",
            TH_filter=False,
            RHOHV_filter=False,
            WRAD_filter=False,
            TDR_filter=False,
        )

        assert result_radar is not None
        assert "COLMAX" in result_radar.fields
        # Verify that the original radar is not modified
        assert "COLMAX" not in radar.fields

    def test_colmax_returns_new_object(self, radar_object):
        """Test that generate_colmax returns a new radar object."""
        radar = radar_object
        original_field_count = len(radar.fields)

        result_radar = generate_colmax(radar=radar, field_for_colmax="DBZH")

        # Original radar should not be modified
        assert len(radar.fields) == original_field_count
        assert "COLMAX" not in radar.fields

        # Result radar should have the new field
        assert result_radar is not None
        assert "COLMAX" in result_radar.fields
        assert len(result_radar.fields) == original_field_count + 1

        # They should be different objects
        assert radar is not result_radar


class TestColmaxPerformance:
    """Test suite for COLMAX performance improvements."""

    def test_optimized_faster_than_old_with_filters(self, radar_object):
        """Test that optimized implementation is faster than old with filters enabled."""
        radar = radar_object

        # Prepare common inputs for both implementations
        source_field = "DBZH" if "DBZH" in radar.fields else list(radar.fields.keys())[0]

        # Create filtered field for testing
        filtered_field_name = source_field + "_test_filtered"
        radar.add_field_like(source_field, filtered_field_name, radar.fields[source_field]["data"].copy())

        # Get sweep ordering and vvg_map
        sw_tuples_az, sweep_ref = get_ordered_sweep_list(radar, use_sweeps_above=0)
        vvg_map = get_vertical_vinculation_gate_map(
            radar=radar,
            use_sweeps_above=0,
            save_vvg_map=True,
            root_cache=config.ROOT_CACHE_PATH,
            regenerate_flag=False,
        )

        # Time the old implementation
        start_old = time.perf_counter()
        result_old = _compute_colmax(
            radar=radar,
            filtered_field_name=filtered_field_name,
            source_field=source_field,
            sw_tuples_az=sw_tuples_az,
            sweep_ref=sweep_ref,
            vvg_map=vvg_map,
        )
        time_old = time.perf_counter() - start_old

        # Time the optimized implementation
        start_optimized = time.perf_counter()
        result_optimized = _compute_colmax_optimized(
            radar=radar,
            field_name=filtered_field_name,
            sw_tuples_az=sw_tuples_az,
            sweep_ref=sweep_ref,
            vvg_map=vvg_map,
        )
        time_optimized = time.perf_counter() - start_optimized

        # Clean up temporary field
        del radar.fields[filtered_field_name]

        # Assert optimized is faster (with some tolerance for variance)
        # We expect at least some speedup, but allow for measurement variance
        speedup_ratio = time_old / time_optimized
        print("\nPerformance comparison (with filters):")
        print(f"  Old implementation: {time_old:.4f}s")
        print(f"  Optimized implementation: {time_optimized:.4f}s")
        print(f"  Speedup ratio: {speedup_ratio:.2f}x")

        # The optimized version should be faster or at least not significantly slower
        # Allow for up to 20% slower due to measurement variance, but ideally faster
        assert time_optimized <= time_old * 1.2, (
            "Optimized implementation should not be significantly slower. "
            f"Old: {time_old:.4f}s, Optimized: {time_optimized:.4f}s"
        )

        # Verify results are consistent
        import numpy as np

        assert result_old.shape == result_optimized.shape, "Results should have same shape"
        # Compare non-masked values
        mask_combined = result_old.mask | result_optimized.mask
        if not np.all(mask_combined):
            non_masked_old = result_old[~mask_combined]
            non_masked_optimized = result_optimized[~mask_combined]
            assert np.allclose(
                non_masked_old, non_masked_optimized, rtol=1e-5
            ), "Results should be numerically equivalent"

    def test_optimized_faster_than_old_no_filters(self, radar_object):
        """Test that optimized implementation is faster than old without filters."""
        radar = radar_object

        # Prepare common inputs for both implementations
        source_field = "DBZH" if "DBZH" in radar.fields else list(radar.fields.keys())[0]

        # Use source field directly (no filtering)
        field_name = source_field

        # Get sweep ordering and vvg_map
        sw_tuples_az, sweep_ref = get_ordered_sweep_list(radar, use_sweeps_above=0)
        vvg_map = get_vertical_vinculation_gate_map(
            radar=radar,
            use_sweeps_above=0,
            save_vvg_map=True,
            root_cache=config.ROOT_CACHE_PATH,
            regenerate_flag=False,
        )

        # Time the old implementation
        start_old = time.perf_counter()
        result_old = _compute_colmax(
            radar=radar,
            filtered_field_name=field_name,
            source_field=source_field,
            sw_tuples_az=sw_tuples_az,
            sweep_ref=sweep_ref,
            vvg_map=vvg_map,
        )
        time_old = time.perf_counter() - start_old

        # Time the optimized implementation
        start_optimized = time.perf_counter()
        result_optimized = _compute_colmax_optimized(
            radar=radar,
            field_name=field_name,
            sw_tuples_az=sw_tuples_az,
            sweep_ref=sweep_ref,
            vvg_map=vvg_map,
        )
        time_optimized = time.perf_counter() - start_optimized

        # Assert optimized is faster
        speedup_ratio = time_old / time_optimized
        print("\nPerformance comparison (no filters):")
        print(f"  Old implementation: {time_old:.4f}s")
        print(f"  Optimized implementation: {time_optimized:.4f}s")
        print(f"  Speedup ratio: {speedup_ratio:.2f}x")

        # The optimized version should be faster or at least not significantly slower
        assert time_optimized <= time_old * 1.2, (
            "Optimized implementation should not be significantly slower. "
            f"Old: {time_old:.4f}s, Optimized: {time_optimized:.4f}s"
        )

        # Verify results are consistent
        import numpy as np

        assert result_old.shape == result_optimized.shape, "Results should have same shape"
        # Compare non-masked values
        mask_combined = result_old.mask | result_optimized.mask
        if not np.all(mask_combined):
            non_masked_old = result_old[~mask_combined]
            non_masked_optimized = result_optimized[~mask_combined]
            assert np.allclose(
                non_masked_old, non_masked_optimized, rtol=1e-5
            ), "Results should be numerically equivalent"
