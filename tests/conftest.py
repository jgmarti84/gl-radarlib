import pathlib
import shutil
from collections import defaultdict

import pytest

HERE = pathlib.Path(__file__).parent


@pytest.fixture(scope="session")
def sample_RMA11_vol1_bufr_files():
    """Get sample RMA11 VOL1 BUFR file paths."""
    bufr_path = HERE / "data" / "bufr" / "RMA11"
    if not bufr_path.exists():
        pytest.skip("No BUFR test data available")

    bufr_files = list(bufr_path.glob("RMA11_0315_01_*.BUFR"))

    if not bufr_files:
        pytest.skip("No RMA11 VOL1 BUFR test files found")

    groups = defaultdict(list)
    for p in bufr_files:
        stem = p.stem
        timestamp = stem.split("_")[-1]
        groups[timestamp].append(p)

    return list(groups.values())[0]


@pytest.fixture(scope="session")
def sample_RMA5_vol2_bufr_files():
    """Get sample RMA5 VOL2 BUFR file paths."""
    bufr_path = HERE / "data" / "bufr" / "RMA5"
    if not bufr_path.exists():
        pytest.skip("No BUFR test data available")

    bufr_files = list(bufr_path.glob("*.BUFR"))
    if not bufr_files:
        pytest.skip("No RMA5 VOL2 BUFR test files found")

    return [str(f) for f in bufr_files]


@pytest.fixture(scope="session")
def sample_AR_bufr_file():
    """Get sample BUFR file path."""
    bufr_path = pathlib.Path("tests/data/bufr")
    if not bufr_path.exists():
        pytest.skip("No BUFR test data available")

    bufr_files = list(bufr_path.glob("*.BUFR"))
    if not bufr_files:
        pytest.skip("No BUFR test files found")

    return str(bufr_files[0])


@pytest.fixture(scope="session")
def tmp_save_path():
    """Temporary directory for saving test output files within the workspace."""
    tmp_dir = HERE.parent / "tmp"  # Creates /workspaces/radarlib/tmp
    tmp_dir.mkdir(exist_ok=True)
    yield tmp_dir
    # Clean up after session
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def bufr_test_dir():
    """Directory where real BUFR test files live (optional)."""
    return HERE / "data" / "bufr"


@pytest.fixture
def sample_sweep_bytes():
    """Return a small synthetic sweep bytes compressed with zlib."""
    import zlib

    import numpy as np

    nrays, ngates = 4, 3
    arr = (np.arange(nrays * ngates, dtype=np.float64) + 0.5).reshape((nrays, ngates))
    raw = arr.tobytes()
    comp = zlib.compress(raw)
    return {
        "nrays": nrays,
        "ngates": ngates,
        "compress_data": comp,
    }
