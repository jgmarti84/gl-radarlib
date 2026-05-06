"""
Unit tests for radarlib.io.pyart.cores_and_tops.generate_cores_and_tops.

These tests use:
  - Synthetic NumPy arrays — no real radar data required.
  - ``unittest.mock.patch`` to replace the detection functions and the PyART
    coordinate transform so that neither scipy nor pyart need to execute
    real code.  The patches target the *source* module attributes so they are
    picked up by the lazy ``from X import Y`` inside ``_run()``.

The entire module is skipped when scipy or pyart are not importable (e.g. local
dev environments without the full dependency stack).  In Docker (requirements.txt
includes scipy and arm-pyart) all tests run.
"""

import json
from datetime import datetime, timezone
from unittest.mock import patch

import numpy as np
import pytest

# Skip the whole module if heavyweight deps are absent.
# scipy is required because ``radarlib.radar_grid`` (imported transitively when
# the patch context managers open) pulls in scipy.ndimage at module load time.
pytest.importorskip("scipy")
pytest.importorskip("pyart")

# Import the module under test *after* the importorskip guards so that the test
# session fails fast (rather than with a cryptic ImportError) when deps are absent.
from radarlib.io.pyart.cores_and_tops import generate_cores_and_tops  # noqa: E402

# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------
# Patch at the *source* module so the lazy ``from X import Y`` inside _run()
# picks up the mock when the module attribute has been replaced.
PATCH_CORES = "radarlib.radar_grid.detect_cores_from_colmax"
PATCH_TOPS = "radarlib.radar_grid.detect_tops_from_3d_grid"
PATCH_GEO = "pyart.core.transforms.cartesian_to_geographic_aeqd"

# ---------------------------------------------------------------------------
# Synthetic detection results
# ---------------------------------------------------------------------------
CORE = {
    "x_m": 10_000.0,
    "y_m": 20_000.0,
    "mean_dbz": 55.3,
    "max_dbz": 60.0,
    "pixel_count": 10,
    "range_m": 25_000.0,
}
TOP = {
    "x_m": 15_000.0,
    "y_m": 25_000.0,
    "altitude_m": 12_500.0,
    "altitude_km": 12.5,
    "pixel_count": 8,
    "range_m": 30_000.0,
    "level_index": 3,
}


def _mock_geo(x_arr, y_arr, lon_0, lat_0, R=6370997.0):
    """Fake coordinate transform: return constant lon/lat offsets."""
    return (
        np.full_like(x_arr, lon_0 + 0.01, dtype=np.float64),
        np.full_like(x_arr, lat_0 + 0.01, dtype=np.float64),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def arrays():
    ny, nx, nz = 10, 10, 4
    return {
        "colmax_2d": np.zeros((ny, nx), dtype=np.float32),
        "dbzh_3d": np.zeros((nz, ny, nx), dtype=np.float32),
        "x_coords": np.zeros((ny, nx), dtype=np.float32),
        "y_coords": np.zeros((ny, nx), dtype=np.float32),
        "z_coords": np.array([1000.0, 3000.0, 6000.0, 9000.0], dtype=np.float32),
    }


@pytest.fixture()
def obs_time():
    return datetime(2026, 4, 28, 15, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def kwargs(arrays, obs_time, tmp_path):
    return {
        **arrays,
        "radar_lat": -31.0,
        "radar_lon": -64.0,
        "observation_time": obs_time,
        "radar_code": "RMA1",
        "strategy": "0315",
        "vol_nr": "01",
        "output_dir": tmp_path,
        "rhohv_3d": None,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_empty_results_no_file_written(kwargs, tmp_path):
    """Both detectors return [] → None returned, nothing written to disk."""
    with (
        patch(PATCH_CORES, return_value=[]),
        patch(PATCH_TOPS, return_value=[]),
        patch(PATCH_GEO, side_effect=_mock_geo),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is None
    assert list(tmp_path.rglob("*.geojson")) == []


def test_geojson_written_with_cores(kwargs):
    """One core detected → file written with correct GeoJSON schema."""
    with (
        patch(PATCH_CORES, return_value=[CORE]),
        patch(PATCH_TOPS, return_value=[]),
        patch(PATCH_GEO, side_effect=_mock_geo),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is not None
    assert result.exists()

    data = json.loads(result.read_text())
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 1
    feat = data["features"][0]
    assert feat["type"] == "Feature"
    assert feat["geometry"]["type"] == "Point"
    assert len(feat["geometry"]["coordinates"]) == 2
    props = feat["properties"]
    assert props["type"] == "core"
    assert props["intensity_dbz"] == int(CORE["mean_dbz"])
    assert props["radar_code"] == "RMA1"


def test_geojson_written_with_tops(kwargs):
    """One top detected → file written with correct GeoJSON schema."""
    with (
        patch(PATCH_CORES, return_value=[]),
        patch(PATCH_TOPS, return_value=[TOP]),
        patch(PATCH_GEO, side_effect=_mock_geo),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is not None
    data = json.loads(result.read_text())
    assert len(data["features"]) == 1
    feat = data["features"][0]
    props = feat["properties"]
    assert props["type"] == "top"
    assert props["altitude_m"] == int(TOP["altitude_m"])
    assert props["radar_code"] == "RMA1"


def test_geojson_contains_both_types(kwargs):
    """One core and one top → FeatureCollection has exactly 2 features."""
    with (
        patch(PATCH_CORES, return_value=[CORE]),
        patch(PATCH_TOPS, return_value=[TOP]),
        patch(PATCH_GEO, side_effect=_mock_geo),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is not None
    data = json.loads(result.read_text())
    types = {f["properties"]["type"] for f in data["features"]}
    assert types == {"core", "top"}
    assert len(data["features"]) == 2


def test_output_path_format(kwargs, obs_time):
    """Filename and directory structure match the documented convention."""
    with (
        patch(PATCH_CORES, return_value=[CORE]),
        patch(PATCH_TOPS, return_value=[]),
        patch(PATCH_GEO, side_effect=_mock_geo),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is not None
    assert result.name == "RMA1_0315_01_20260428T150000Z_TOPS_CORES.geojson"
    # Check directory hierarchy: …/RMA1/2026/04/28/
    parts = result.parts
    assert parts[-2] == "28"
    assert parts[-3] == "04"
    assert parts[-4] == "2026"
    assert parts[-5] == "RMA1"


def test_write_failure_does_not_raise(kwargs):
    """IOError during file write → returns None, does NOT re-raise."""
    with (
        patch(PATCH_CORES, return_value=[CORE]),
        patch(PATCH_TOPS, return_value=[]),
        patch(PATCH_GEO, side_effect=_mock_geo),
        patch("builtins.open", side_effect=IOError("disk full")),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is None


def test_missing_rhohv_passes_none(kwargs):
    """rhohv_3d=None → detect_cores_from_colmax called with rhohv=None."""
    kwargs["rhohv_3d"] = None
    with (
        patch(PATCH_CORES, return_value=[CORE]) as mock_cores,
        patch(PATCH_TOPS, return_value=[TOP]),
        patch(PATCH_GEO, side_effect=_mock_geo),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is not None
    # Verify the cores function received rhohv=None
    called_kwargs = mock_cores.call_args.kwargs
    assert called_kwargs.get("rhohv") is None


def test_observation_time_format(kwargs):
    """observation_time value in output JSON is a valid ISO 8601 UTC string."""
    with (
        patch(PATCH_CORES, return_value=[CORE]),
        patch(PATCH_TOPS, return_value=[]),
        patch(PATCH_GEO, side_effect=_mock_geo),
    ):
        result = generate_cores_and_tops(**kwargs)

    assert result is not None
    data = json.loads(result.read_text())
    obs_str = data["features"][0]["properties"]["observation_time"]
    # Must parse as ISO 8601 UTC with seconds precision
    parsed = datetime.strptime(obs_str, "%Y-%m-%dT%H:%M:%SZ")
    assert parsed.year == 2026
    assert parsed.month == 4
    assert parsed.day == 28
    assert parsed.hour == 15
    assert parsed.minute == 0
    assert parsed.second == 0
