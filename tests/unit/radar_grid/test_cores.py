"""
Unit tests for radarlib.radar_grid.cores.detect_cores_from_colmax.

All tests use small synthetic (20, 20) NumPy arrays — no real radar data.
Coordinates span ±100 000 m so that centroids far from the origin can easily
satisfy the default minimum-range requirement.
"""

import numpy as np

from radarlib.radar_grid.cores import detect_cores_from_colmax

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_NY, _NX = 20, 20
_XLIN = np.linspace(-100_000.0, 100_000.0, _NX)
_YLIN = np.linspace(-100_000.0, 100_000.0, _NY)
_YY, _XX = np.meshgrid(_YLIN, _XLIN, indexing="ij")  # shape (NY, NX)
_SPACING = float(_XLIN[1] - _XLIN[0])  # metres between adjacent pixel centres


def _make_colmax(fill: float = 0.0) -> np.ndarray:
    """Return a (NY, NX) float32 array filled with *fill*."""
    return np.full((_NY, _NX), fill, dtype=np.float32)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDetectCoresFromColmax:
    """Full test suite for detect_cores_from_colmax."""

    def test_empty_grid(self):
        """All-zeros grid yields no cores."""
        colmax = _make_colmax(0.0)
        result = detect_cores_from_colmax(colmax, _XX, _YY, rhohv=None, min_dbz=52.0, min_range_m=0.0)
        assert result == []

    def test_single_blob_above_threshold(self):
        """One clear blob above threshold passes range, size and updraft gate.

        The blob is a 4×4 region (16 pixels) at the far corner of the grid.
        Because max_dbz=65 > min_dbz_updraft=56 and pixel_count=16 > 5,
        the updraft gate must accept it even without RhoHV.
        The centroid must be placed at the peak-dBZ pixel, not the geometric
        centre.  The peak is forced to (row=17, col=17) — the last pixel in
        row-major order — so the test would fail with a mean-centroid algorithm.
        """
        colmax = _make_colmax(0.0)
        row_sl, col_sl = slice(14, 18), slice(14, 18)  # 4×4 = 16 pixels
        colmax[row_sl, col_sl] = 54.0   # above min_dbz, below updraft threshold
        colmax[17, 17] = 65.0           # clear peak — last pixel in the blob

        result = detect_cores_from_colmax(
            colmax,
            _XX,
            _YY,
            rhohv=None,
            min_dbz=52.0,
            min_range_m=0.0,
            dedup_radius_m=0.0,
            min_dbz_updraft=56.0,
            min_pixels_updraft=5,
        )

        assert len(result) == 1, f"Expected 1 core, got {len(result)}"
        core = result[0]

        # Centroid must be at the peak-dBZ pixel (17, 17), not the blob mean
        expected_x = float(_XX[17, 17])
        expected_y = float(_YY[17, 17])
        assert abs(core["x_m"] - expected_x) < 1e-3, (
            f"Centroid x should be peak-pixel {expected_x:.1f}, got {core['x_m']:.1f}"
        )
        assert abs(core["y_m"] - expected_y) < 1e-3, (
            f"Centroid y should be peak-pixel {expected_y:.1f}, got {core['y_m']:.1f}"
        )

        assert core["pixel_count"] == 16
        assert abs(core["max_dbz"] - 65.0) < 1e-4
        assert core["range_m"] > 0.0

    def test_blob_below_min_range(self):
        """Blob whose centroid is too close to the radar origin is excluded.

        The blob is placed at the centre of the grid (range ≈ 0 m), with
        min_range_m=50 000 m to guarantee rejection.
        """
        colmax = _make_colmax(0.0)
        # Centre region — both indices 9 and 10 straddle the grid centre
        row_sl, col_sl = slice(8, 12), slice(8, 12)  # 4×4 = 16 pixels
        colmax[row_sl, col_sl] = 60.0

        result = detect_cores_from_colmax(
            colmax,
            _XX,
            _YY,
            rhohv=None,
            min_dbz=52.0,
            min_range_m=50_000.0,
            min_dbz_updraft=56.0,
            min_pixels_updraft=5,
        )

        assert result == [], f"Expected no cores, got {result}"

    def test_deduplication(self):
        """Two blobs within dedup_radius_m are merged; the stronger one survives.

        Blob A (stronger, mean_dbz=60) is placed at the upper-right quadrant.
        Blob B (weaker, mean_dbz=54) is placed close to Blob A.
        A large dedup_radius_m ensures they are merged.
        """
        colmax = _make_colmax(0.0)

        # Blob A — stronger, 4×4 at far upper-right
        row_a, col_a = slice(13, 17), slice(13, 17)
        colmax[row_a, col_a] = 62.0

        # Blob B — weaker, adjacent block separated by one pixel gap (non-contiguous)
        row_b, col_b = slice(13, 17), slice(4, 8)
        colmax[row_b, col_b] = 54.5

        centroid_ax = float(_XX[row_a, col_a].mean())
        centroid_ay = float(_YY[row_a, col_a].mean())
        centroid_bx = float(_XX[row_b, col_b].mean())
        centroid_by = float(_YY[row_b, col_b].mean())
        dist = float(np.sqrt((centroid_ax - centroid_bx) ** 2 + (centroid_ay - centroid_by) ** 2))

        # Merge radius larger than actual distance → both merge
        result = detect_cores_from_colmax(
            colmax,
            _XX,
            _YY,
            rhohv=None,
            min_dbz=52.0,
            min_range_m=0.0,
            dedup_radius_m=dist + 10_000.0,
            min_dbz_updraft=54.0,  # low enough that blob B also qualifies
            min_pixels_updraft=5,
        )

        assert len(result) == 1, f"Expected 1 core after dedup, got {len(result)}: {result}"
        # The surviving core should be the stronger one (Blob A)
        assert (
            abs(result[0]["mean_dbz"] - 62.0) < 1e-4
        ), f"Expected surviving core to be Blob A (mean_dbz=62.0), got {result[0]['mean_dbz']}"

    def test_two_distinct_cores(self):
        """Two blobs far apart are both returned when dedup_radius_m is small."""
        colmax = _make_colmax(0.0)

        # Blob A — top-right quadrant
        row_a, col_a = slice(14, 18), slice(14, 18)
        colmax[row_a, col_a] = 59.0

        # Blob B — bottom-left quadrant
        row_b, col_b = slice(2, 6), slice(2, 6)
        colmax[row_b, col_b] = 57.0

        result = detect_cores_from_colmax(
            colmax,
            _XX,
            _YY,
            rhohv=None,
            min_dbz=52.0,
            min_range_m=0.0,
            dedup_radius_m=1.0,  # tiny — no merging
            min_dbz_updraft=56.0,
            min_pixels_updraft=5,
        )

        assert len(result) == 2, f"Expected 2 distinct cores, got {len(result)}: {result}"
        # Sorted descending by mean_dbz
        assert result[0]["mean_dbz"] >= result[1]["mean_dbz"]

    def test_no_rhohv_falls_back_to_updraft(self):
        """With rhohv=None the updraft gate alone can accept a strong blob."""
        colmax = _make_colmax(0.0)
        row_sl, col_sl = slice(14, 20), slice(14, 20)  # 6×6 = 36 pixels
        colmax[row_sl, col_sl] = 58.0  # max_dbz=58 > min_dbz_updraft=56, pixels=36 > 5

        result = detect_cores_from_colmax(
            colmax,
            _XX,
            _YY,
            rhohv=None,  # no RhoHV supplied
            min_dbz=52.0,
            min_range_m=0.0,
            min_dbz_updraft=56.0,
            min_pixels_updraft=5,
        )

        assert len(result) == 1, f"Expected updraft gate to accept blob, got {result}"

    def test_rhohv_gate_rejects_low_rhohv(self):
        """Blob with sufficient dBZ but low RhoHV AND insufficient intensity is rejected.

        To test this, max_dbz must be BELOW min_dbz_updraft (so the updraft gate
        fails) and mean RhoHV must be BELOW rhohv_threshold (so the met gate
        fails).  The blob should be rejected entirely.
        """
        colmax = _make_colmax(0.0)
        row_sl, col_sl = slice(14, 18), slice(14, 18)  # 4×4 = 16 pixels
        colmax[row_sl, col_sl] = 53.0  # above min_dbz=52 but below min_dbz_updraft=56

        rhohv = np.full((_NY, _NX), 0.70, dtype=np.float32)  # mean RhoHV=0.70 < 0.85

        result = detect_cores_from_colmax(
            colmax,
            _XX,
            _YY,
            rhohv=rhohv,
            min_dbz=52.0,
            min_range_m=0.0,
            min_dbz_updraft=56.0,
            min_pixels_updraft=5,
            rhohv_threshold=0.85,
            min_pixels=2,
        )

        assert result == [], f"Expected blob to be rejected (low RhoHV, below updraft threshold), got {result}"

    def test_rhohv_gate_accepts_high_rhohv(self):
        """Blob with sufficient dBZ and high RhoHV passes the met gate."""
        colmax = _make_colmax(0.0)
        row_sl, col_sl = slice(14, 18), slice(14, 18)  # 4×4 = 16 pixels, pixel_count=16 > min_pixels=2
        colmax[row_sl, col_sl] = 53.0  # above 52, below updraft threshold of 56

        rhohv = np.full((_NY, _NX), 0.92, dtype=np.float32)  # well above 0.85

        result = detect_cores_from_colmax(
            colmax,
            _XX,
            _YY,
            rhohv=rhohv,
            min_dbz=52.0,
            min_range_m=0.0,
            min_dbz_updraft=56.0,
            min_pixels_updraft=5,
            rhohv_threshold=0.85,
            min_pixels=2,
        )

        assert len(result) == 1, f"Expected met gate to accept blob, got {result}"

    def test_masked_array_input(self):
        """Masked pixels in colmax are excluded from blobs.

        A 5×5 blob is created with most pixels at 54 dBZ and a distinct peak
        at (17, 17) = 65 dBZ.  The central pixel (16, 16) is masked out.
        The centroid must land at (17, 17) — the unmasked peak-dBZ pixel —
        confirming that masking is respected and centroid = peak, not mean.
        """
        colmax = _make_colmax(0.0)
        row_sl, col_sl = slice(14, 19), slice(14, 19)  # 5×5 = 25 pixels
        colmax[row_sl, col_sl] = 54.0
        colmax[17, 17] = 65.0   # clear peak, away from masked pixel at (16, 16)

        # Mask the central pixel of the blob
        mask = np.zeros((_NY, _NX), dtype=bool)
        mask[16, 16] = True  # centre of the 5×5 blob

        ma_colmax = np.ma.array(colmax, mask=mask)

        result = detect_cores_from_colmax(
            ma_colmax,
            _XX,
            _YY,
            rhohv=None,
            min_dbz=52.0,
            min_range_m=0.0,
            min_dbz_updraft=56.0,
            min_pixels_updraft=5,
        )

        # The blob is still large enough (24 unmasked pixels) to be detected
        assert len(result) == 1, f"Expected 1 core from masked array input, got {result}"

        # Centroid must be at the peak-dBZ pixel (17, 17), not the blob mean
        core = result[0]
        expected_x = float(_XX[17, 17])
        expected_y = float(_YY[17, 17])
        assert (
            abs(core["x_m"] - expected_x) < 1e-3
        ), f"Centroid x should be peak-pixel {expected_x:.2f}, got {core['x_m']:.2f}"
        assert (
            abs(core["y_m"] - expected_y) < 1e-3
        ), f"Centroid y should be peak-pixel {expected_y:.2f}, got {core['y_m']:.2f}"
        assert core["pixel_count"] == 24  # 25 - 1 masked pixel
