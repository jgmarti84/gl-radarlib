"""
Unit tests for radarlib.radar_grid.tops.detect_tops_from_3d_grid.

All tests use small synthetic (4, 20, 20) NumPy arrays — no real radar data.
The grid has 4 vertical levels at 3 000, 6 000, 9 000 and 12 000 m.
Horizontal coordinates span ±150 000 m so centroids at the grid edge are
easily beyond any reasonable min_range threshold.
"""

import numpy as np

from radarlib.radar_grid.tops import detect_tops_from_3d_grid

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_NZ, _NY, _NX = 4, 20, 20
_Z_1D = np.array([3000.0, 6000.0, 9000.0, 12000.0], dtype=np.float32)

_XLIN = np.linspace(-150_000.0, 150_000.0, _NX, dtype=np.float32)
_YLIN = np.linspace(-150_000.0, 150_000.0, _NY, dtype=np.float32)
_YY, _XX = np.meshgrid(_YLIN, _XLIN, indexing="ij")  # shape (NY, NX)


def _make_grid(fill: float = 0.0) -> np.ndarray:
    """Return a (NZ, NY, NX) float32 array filled with *fill*."""
    return np.full((_NZ, _NY, _NX), fill, dtype=np.float32)


def _high_rhohv() -> np.ndarray:
    """Return a 3D RhoHV array with value 0.97 everywhere."""
    return np.full((_NZ, _NY, _NX), 0.97, dtype=np.float32)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDetectTopsFrom3dGrid:
    """Full test suite for detect_tops_from_3d_grid."""

    def test_empty_grid(self):
        """All-zeros grid yields no tops."""
        grid = _make_grid(0.0)
        result = detect_tops_from_3d_grid(
            grid, _XX, _YY, _Z_1D, rhohv_3d=None, min_dbz=20.0, min_range_m=0.0, min_dev_m=0.0
        )
        assert result == []

    def test_single_blob_above_threshold(self):
        """One blob at level 3 (12 000 m) passes all gates → exactly one top.

        Because rhohv_3d is None, the RhoHV gate is skipped.  The blob exceeds
        min_dev_m=9 000 m and min_range_m=0 m.
        """
        grid = _make_grid(0.0)
        row_sl, col_sl = slice(14, 18), slice(14, 18)  # 4×4 = 16 pixels
        grid[3, row_sl, col_sl] = 30.0  # level 3 = 12 000 m

        result = detect_tops_from_3d_grid(
            grid,
            _XX,
            _YY,
            _Z_1D,
            rhohv_3d=None,
            min_dbz=20.0,
            min_range_m=0.0,
            min_dev_m=9000.0,
            dedup_radius_m=1.0,
            min_pixels=2,
        )

        assert len(result) == 1, f"Expected 1 top, got {len(result)}: {result}"
        top = result[0]
        assert top["level_index"] == 3
        assert abs(top["altitude_m"] - 12000.0) < 1e-3
        assert top["altitude_km"] == 12.0
        assert top["pixel_count"] == 16

        # Centroid must match the mean coordinates of the blob pixels
        expected_x = float(_XX[row_sl, col_sl].mean())
        expected_y = float(_YY[row_sl, col_sl].mean())
        assert abs(top["x_m"] - expected_x) < 1e-2
        assert abs(top["y_m"] - expected_y) < 1e-2

    def test_blob_below_min_dev(self):
        """Blob at a low level (altitude < min_dev_m) is rejected."""
        grid = _make_grid(0.0)
        # Level 0 = 3 000 m, which is below min_dev_m=9 000 m
        grid[0, 14:18, 14:18] = 30.0

        result = detect_tops_from_3d_grid(
            grid, _XX, _YY, _Z_1D, rhohv_3d=None, min_dbz=20.0, min_range_m=0.0, min_dev_m=9000.0
        )
        assert result == [], f"Expected no tops (blob below min_dev_m), got {result}"

    def test_blob_below_min_range(self):
        """Blob whose centroid is inside the range exclusion zone is rejected.

        The blob is placed at the centre of the grid (range ≈ 0) and
        min_range_m is set large enough to exclude it.
        """
        grid = _make_grid(0.0)
        # Centre 4×4 block — straddles the grid origin
        grid[3, 8:12, 8:12] = 30.0

        result = detect_tops_from_3d_grid(
            grid, _XX, _YY, _Z_1D, rhohv_3d=None, min_dbz=20.0, min_range_m=100_000.0, min_dev_m=0.0
        )
        assert result == [], f"Expected no tops (inside min_range), got {result}"

    def test_deduplication_keeps_higher(self):
        """Two blobs within dedup_radius_m → only the higher-altitude one returned.

        Blob A is at level 3 (12 000 m) and Blob B is at level 2 (9 000 m).
        Both are placed near the same horizontal location so the dedup radius
        covers them.  The returned top must be Blob A.
        """
        grid = _make_grid(0.0)

        # Blob A — level 3, upper-right quadrant, 4×4 pixels
        row_sl, col_sl = slice(13, 17), slice(13, 17)
        grid[3, row_sl, col_sl] = 30.0

        # Blob B — level 2, same horizontal position (non-contiguous — different level)
        grid[2, row_sl, col_sl] = 25.0

        # Centroid of both blobs is nearly identical horizontally
        # Compute approximate horizontal distance: ~0 m → use a large radius
        result = detect_tops_from_3d_grid(
            grid,
            _XX,
            _YY,
            _Z_1D,
            rhohv_3d=None,
            min_dbz=20.0,
            min_range_m=0.0,
            min_dev_m=0.0,
            dedup_radius_m=30_000.0,  # large enough to merge them
            min_pixels=2,
        )

        assert len(result) == 1, f"Expected 1 top after dedup, got {len(result)}: {result}"
        assert (
            result[0]["level_index"] == 3
        ), f"Expected surviving top to be at level 3, got level {result[0]['level_index']}"
        assert abs(result[0]["altitude_m"] - 12000.0) < 1e-3

    def test_two_distinct_tops(self):
        """Two blobs far apart horizontally → two tops returned."""
        grid = _make_grid(0.0)

        # Blob A — level 3, upper-right
        grid[3, 14:18, 14:18] = 30.0
        # Blob B — level 3, lower-left (far away)
        grid[3, 2:6, 2:6] = 25.0

        result = detect_tops_from_3d_grid(
            grid,
            _XX,
            _YY,
            _Z_1D,
            rhohv_3d=None,
            min_dbz=20.0,
            min_range_m=0.0,
            min_dev_m=9000.0,
            dedup_radius_m=1.0,  # tiny — no merging
            min_pixels=2,
        )

        assert len(result) == 2, f"Expected 2 distinct tops, got {len(result)}: {result}"
        # Sorted descending by altitude — both are level 3 so order may vary,
        # but there must be exactly 2.

    def test_no_rhohv_skips_quality_gate(self):
        """With rhohv_3d=None the quality gate is skipped; blob is accepted."""
        grid = _make_grid(0.0)
        grid[3, 14:18, 14:18] = 25.0  # above min_dbz, at upper level

        result = detect_tops_from_3d_grid(
            grid, _XX, _YY, _Z_1D, rhohv_3d=None, min_dbz=20.0, min_range_m=0.0, min_dev_m=9000.0, min_pixels=2
        )

        assert len(result) == 1, f"Expected blob accepted without RhoHV gate, got {result}"

    def test_rhohv_gate_rejects_low_rhohv(self):
        """Blob with mean RhoHV below threshold is rejected."""
        grid = _make_grid(0.0)
        grid[3, 14:18, 14:18] = 25.0  # valid echo at upper level

        # Set RhoHV to 0.80, which is below the default threshold of 0.94
        rhohv = np.full((_NZ, _NY, _NX), 0.80, dtype=np.float32)

        result = detect_tops_from_3d_grid(
            grid,
            _XX,
            _YY,
            _Z_1D,
            rhohv_3d=rhohv,
            min_dbz=20.0,
            min_range_m=0.0,
            min_dev_m=9000.0,
            rhohv_threshold=0.94,
            min_pixels=2,
        )

        assert result == [], f"Expected blob rejected by RhoHV gate (mean=0.80 < 0.94), got {result}"

    def test_rhohv_gate_accepts_high_rhohv(self):
        """Blob with mean RhoHV above threshold is accepted."""
        grid = _make_grid(0.0)
        grid[3, 14:18, 14:18] = 25.0

        rhohv = np.full((_NZ, _NY, _NX), 0.97, dtype=np.float32)

        result = detect_tops_from_3d_grid(
            grid,
            _XX,
            _YY,
            _Z_1D,
            rhohv_3d=rhohv,
            min_dbz=20.0,
            min_range_m=0.0,
            min_dev_m=9000.0,
            rhohv_threshold=0.94,
            min_pixels=2,
        )

        assert len(result) == 1, f"Expected RhoHV gate to accept blob, got {result}"

    def test_masked_array_input(self):
        """Masked pixels in grid_3d are excluded from blobs.

        A 4×4 blob at level 3 has its centre pixel masked.  The resulting blob
        has 15 valid pixels (not 16) and the centroid should not include
        the masked pixel.
        """
        grid = _make_grid(0.0)
        row_sl, col_sl = slice(14, 18), slice(14, 18)
        grid[3, row_sl, col_sl] = 30.0

        # Mask the central pixel of the blob at level 3
        mask = np.zeros((_NZ, _NY, _NX), dtype=bool)
        mask[3, 15, 15] = True  # one pixel inside the 4×4 blob

        ma_grid = np.ma.array(grid, mask=mask)

        result = detect_tops_from_3d_grid(
            ma_grid, _XX, _YY, _Z_1D, rhohv_3d=None, min_dbz=20.0, min_range_m=0.0, min_dev_m=9000.0, min_pixels=2
        )

        assert len(result) == 1, f"Expected 1 top from masked array input, got {result}"
        # Pixel count must be 15, not 16
        assert (
            result[0]["pixel_count"] == 15
        ), f"Expected pixel_count=15 (masked 1 pixel), got {result[0]['pixel_count']}"

        # Centroid must exclude the masked pixel
        unmasked_x = _XX[row_sl, col_sl][~mask[3, row_sl, col_sl]]
        unmasked_y = _YY[row_sl, col_sl][~mask[3, row_sl, col_sl]]
        expected_x = float(unmasked_x.mean())
        expected_y = float(unmasked_y.mean())
        assert abs(result[0]["x_m"] - expected_x) < 1e-2
        assert abs(result[0]["y_m"] - expected_y) < 1e-2

    def test_output_sorted_by_altitude(self):
        """Multiple tops are returned in descending altitude order."""
        grid = _make_grid(0.0)

        # Three blobs at different levels and horizontal positions
        grid[3, 14:18, 14:18] = 30.0  # level 3 = 12 000 m
        grid[2, 2:6, 2:6] = 25.0  # level 2 =  9 000 m  (far from level-3 blob)
        grid[1, 2:6, 14:18] = 22.0  # level 1 =  6 000 m  (different location)

        result = detect_tops_from_3d_grid(
            grid,
            _XX,
            _YY,
            _Z_1D,
            rhohv_3d=None,
            min_dbz=20.0,
            min_range_m=0.0,
            min_dev_m=0.0,
            dedup_radius_m=1.0,
            min_pixels=2,
        )

        assert len(result) == 3, f"Expected 3 tops, got {len(result)}: {result}"

        altitudes = [t["altitude_m"] for t in result]
        assert altitudes == sorted(altitudes, reverse=True), f"Tops not sorted by descending altitude: {altitudes}"

    def test_z_coords_3d_input(self):
        """Function handles 3D z_coords (NZ, NY, NX) correctly."""
        grid = _make_grid(0.0)
        row_sl, col_sl = slice(14, 18), slice(14, 18)
        grid[3, row_sl, col_sl] = 30.0

        # Build 3D z_coords by broadcasting 1D values
        z_3d = np.zeros((_NZ, _NY, _NX), dtype=np.float32)
        for k, z_val in enumerate(_Z_1D):
            z_3d[k, :, :] = z_val

        result = detect_tops_from_3d_grid(
            grid, _XX, _YY, z_3d, rhohv_3d=None, min_dbz=20.0, min_range_m=0.0, min_dev_m=9000.0, min_pixels=2
        )

        assert len(result) == 1
        assert abs(result[0]["altitude_m"] - 12000.0) < 1e-3


# ---------------------------------------------------------------------------
# detect_tops_from_cores — footprint search (blob + dilation)
# ---------------------------------------------------------------------------

from functools import partial  # noqa: E402

from radarlib import config  # noqa: E402
from radarlib.radar_grid.tops import detect_tops_from_cores  # noqa: E402

_CZ = np.array([3000.0, 6000.0, 9000.0, 12000.0, 15000.0], dtype=np.float32)
_CNZ = len(_CZ)
_CNY = _CNX = 21
_CLIN = np.linspace(-100_000.0, 100_000.0, _CNX, dtype=np.float32)
_CYY, _CXX = np.meshgrid(_CLIN, _CLIN, indexing="ij")
_PIX = 10_000.0  # metres per pixel on this grid


def _core_at(row: int, col: int, blob_rows=None, blob_cols=None, mean_dbz: float = 50.0) -> dict:
    """Core whose blob is the given pixel(s); centroid = (row, col)."""
    mask = np.zeros((_CNY, _CNX), dtype=bool)
    if blob_rows is None:
        mask[row, col] = True
    else:
        mask[np.ix_(blob_rows, blob_cols)] = True
    return {
        "x_m": float(_CXX[row, col]),
        "y_m": float(_CYY[row, col]),
        "mean_dbz": mean_dbz,
        "blob_mask": mask,
    }


def _grid_with_echo(*voxels) -> np.ndarray:
    """Grid of -20 dBZ with given (k, row, col, dbz) voxels set."""
    g = np.full((_CNZ, _CNY, _CNX), -20.0, dtype=np.float32)
    for k, r, c, dbz in voxels:
        g[k, r, c] = dbz
    return g


# Footprint tests use weak echoes too; pass 0 dBZ so they test the footprint, not the default.
_tops = partial(detect_tops_from_cores, min_dbz=0.0)


class TestDetectTopsFromCores:
    def test_top_found_inside_blob(self):
        grid = _grid_with_echo((4, 10, 10, 30.0))
        tops = _tops([_core_at(10, 10)], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0)
        assert len(tops) == 1
        assert tops[0]["altitude_m"] == 15000.0
        assert tops[0]["dbz"] == 30.0
        assert tops[0]["x_m"] == float(_CXX[10, 10])

    def test_dilation_reaches_tower_just_outside_blob(self):
        core = _core_at(10, 10)  # one-pixel blob
        # Tower two pixels away: not reached by a 1-px dilation
        far = _grid_with_echo((3, 10, 12, 25.0))
        assert _tops([core], far, _CXX, _CYY, _CZ, footprint_dilation_px=1) == []
        # Tower one pixel away: reached by a 1-px dilation, not by none
        near = _grid_with_echo((3, 10, 11, 25.0))
        assert _tops([core], near, _CXX, _CYY, _CZ, footprint_dilation_px=0) == []
        tops = _tops([core], near, _CXX, _CYY, _CZ, footprint_dilation_px=1)
        assert len(tops) == 1 and tops[0]["x_m"] == float(_CXX[10, 11])

    def test_highest_altitude_wins_over_stronger_lower_echo(self):
        grid = _grid_with_echo((1, 10, 10, 55.0), (4, 11, 10, 22.0))
        core = _core_at(10, 10, blob_rows=[10, 11], blob_cols=[10])
        tops = _tops([core], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0)
        assert tops[0]["altitude_m"] == 15000.0
        assert tops[0]["x_m"] == float(_CXX[11, 10])  # location of the highest echo, not the peak

    def test_tie_resolved_by_stronger_echo(self):
        grid = _grid_with_echo((2, 10, 10, 20.0), (2, 10, 11, 35.0))
        core = _core_at(10, 10, blob_rows=[10], blob_cols=[10, 11])
        tops = _tops([core], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0)
        assert tops[0]["dbz"] == 35.0
        assert tops[0]["x_m"] == float(_CXX[10, 11])

    def test_threshold_changes_top(self):
        grid = _grid_with_echo((4, 10, 10, 12.0), (2, 10, 10, 30.0))
        core = _core_at(10, 10)
        t0 = _tops([core], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0, min_dbz=0.0)
        t18 = _tops([core], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0, min_dbz=18.0)
        assert t0[0]["altitude_m"] == 15000.0  # 12 dBZ counts as echo at 0 dBZ
        assert t18[0]["altitude_m"] == 9000.0  # 12 dBZ ignored at 18 dBZ; top drops to the 30 dBZ echo

    def test_nan_voxels_are_not_echo_and_do_not_hide_real_max(self):
        grid = _grid_with_echo((2, 10, 10, 28.0))
        grid[4, 10, 10] = np.nan
        grid[4, 10, 11] = np.nan
        core = _core_at(10, 10, blob_rows=[10], blob_cols=[10, 11])
        tops = _tops([core], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0)
        assert len(tops) == 1
        assert tops[0]["altitude_m"] == 9000.0
        assert tops[0]["dbz"] == 28.0

    def test_masked_voxels_are_not_echo(self):
        grid = np.ma.masked_array(_grid_with_echo((2, 10, 10, 28.0), (4, 10, 10, 50.0)))
        grid.mask = np.zeros(grid.shape, dtype=bool)
        grid.mask[4, 10, 10] = True
        tops = _tops([_core_at(10, 10)], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0)
        assert tops[0]["altitude_m"] == 9000.0

    def test_echo_at_highest_level_returns_that_level(self):
        grid = _grid_with_echo((4, 10, 10, 40.0))
        tops = _tops([_core_at(10, 10)], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0)
        assert tops[0]["altitude_m"] == float(_CZ[-1])  # no truncation flag, the top is the top

    def test_no_echo_in_footprint_gives_no_top(self):
        grid = _grid_with_echo((4, 0, 0, 40.0))  # echo far from the core
        assert _tops([_core_at(10, 10)], grid, _CXX, _CYY, _CZ, footprint_dilation_px=1) == []

    def test_no_cores_returns_empty(self):
        grid = _grid_with_echo((4, 10, 10, 40.0))
        assert _tops([], grid, _CXX, _CYY, _CZ) == []

    def test_cylinder_fallback_without_blob_mask(self):
        grid = _grid_with_echo((3, 10, 11, 30.0))
        core = {"x_m": float(_CXX[10, 10]), "y_m": float(_CYY[10, 10]), "mean_dbz": 45.0}
        tops = _tops([core], grid, _CXX, _CYY, _CZ, radius_m=20_000.0)
        assert len(tops) == 1 and tops[0]["x_m"] == float(_CXX[10, 11])

    def test_3d_z_coords(self):
        z3d = np.broadcast_to(_CZ[:, None, None], (_CNZ, _CNY, _CNX)).astype(np.float32)
        grid = _grid_with_echo((2, 10, 10, 30.0))
        tops = _tops([_core_at(10, 10)], grid, _CXX, _CYY, z3d, footprint_dilation_px=0)
        assert tops[0]["altitude_m"] == 9000.0

    def test_bad_z_shape_is_logged_not_raised(self):
        grid = _grid_with_echo((2, 10, 10, 30.0))
        assert _tops([_core_at(10, 10)], grid, _CXX, _CYY, np.zeros((2, 2))) == []

    def test_default_threshold_is_30_dbz(self):
        assert config.TOPS_MIN_Z == 30.0
        grid = _grid_with_echo((2, 10, 10, 25.0), (4, 10, 11, 35.0))  # 25 dBZ ignored by default
        core = _core_at(10, 10, blob_rows=[10], blob_cols=[10, 11])
        tops = detect_tops_from_cores([core], grid, _CXX, _CYY, _CZ, footprint_dilation_px=0)
        assert tops[0]["altitude_m"] == 15000.0
        assert tops[0]["dbz"] == 35.0
        weak_only = _grid_with_echo((2, 10, 10, 25.0))
        assert detect_tops_from_cores([_core_at(10, 10)], weak_only, _CXX, _CYY, _CZ, footprint_dilation_px=0) == []
