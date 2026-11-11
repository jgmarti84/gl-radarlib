"""Unit tests for radarlib.utils.grid_utils.create_gate_coords_file.

The FTP client and the BUFR decoder are replaced by fakes, so no network and no
real BUFR files are needed.
"""

import logging
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from radarlib.utils import grid_utils

NORMAL = dict(nsweeps=15, nrays=5400, ngates=653)  # 15 sweeps x 360 rays
ODD = dict(nsweeps=14, nrays=5040, ngates=653)  # one sweep missing
OTHER = dict(nsweeps=15, nrays=5400, ngates=700)


def _fake_radar(nsweeps: int, nrays: int, ngates: int) -> SimpleNamespace:
    return SimpleNamespace(
        nsweeps=nsweeps,
        nrays=nrays,
        ngates=ngates,
        range={"data": 1380.0 + 360.0 * np.arange(ngates)},
        fixed_angle={"data": np.array([0.5, 0.9])},
    )


class _FakeClient:
    """Stands in for RadarFTPClient; ``plan`` lists the shape of each candidate."""

    downloads: list = []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def traverse_radar(self, radar_name, dt_start=None, dt_end=None, vol_types=None):
        for i, _ in enumerate(self.plan):
            yield (None, f"{radar_name}_0315_01_DBZH_{i}.BUFR", Path(f"/remote/{i}.BUFR"))


@pytest.fixture
def run(monkeypatch, tmp_path):
    """Return ``run(plan, **kwargs)``: plan is a list of shape dicts, or None for a decode failure."""

    def _run(plan, **kwargs):
        _FakeClient.plan = plan
        _FakeClient.downloads = []
        order = {f"/remote/{i}.BUFR": shape for i, shape in enumerate(plan)}
        current = {}

        def fake_download(self, remote, local):
            _FakeClient.downloads.append(remote)
            current["shape"] = order[remote]
            Path(local).write_bytes(b"x")

        def fake_decode(paths, save_path=None):
            if current["shape"] is None:
                raise ValueError("corrupt BUFR")
            return _fake_radar(**current["shape"])

        monkeypatch.setattr(_FakeClient, "download_file", fake_download, raising=False)
        monkeypatch.setattr(grid_utils, "RadarFTPClient", _FakeClient)
        monkeypatch.setattr(grid_utils, "bufr_paths_to_pyart", fake_decode)
        monkeypatch.setattr(
            grid_utils,
            "get_gate_coordinates",
            lambda r: tuple(np.zeros(r.nrays * r.ngates) for _ in range(3)),
        )
        monkeypatch.setattr(grid_utils, "infer_blind_range_m", lambda r: 1380.0)
        monkeypatch.setattr(grid_utils.random, "shuffle", lambda seq: None)  # keep the plan order
        return grid_utils.create_gate_coords_file(
            "RMA5",
            "0315",
            "01",
            str(tmp_path),
            ftp_host="h",
            ftp_user="u",
            ftp_pass="p",
            **kwargs,
        )

    return _run


def _gates_in(path: Path) -> int:
    return int(np.load(path)["gate_x"].size)


def test_scan_signature_ignores_elevation_jitter():
    a = _fake_radar(**NORMAL)
    b = _fake_radar(**NORMAL)
    b.fixed_angle = {"data": np.array([0.6, 1.0])}
    assert grid_utils._scan_signature(a) == grid_utils._scan_signature(b)
    assert grid_utils._scan_signature(a) != grid_utils._scan_signature(_fake_radar(**ODD))


def test_agreeing_files_outvote_an_atypical_one(run):
    """An odd scan drawn first must not become the cached geometry."""
    path = run([ODD, NORMAL, NORMAL])
    assert _gates_in(path) == NORMAL["nrays"] * NORMAL["ngates"]
    assert len(_FakeClient.downloads) == 3


def test_stops_downloading_once_two_files_agree(run):
    path = run([NORMAL, NORMAL, ODD])
    assert _gates_in(path) == NORMAL["nrays"] * NORMAL["ngates"]
    assert len(_FakeClient.downloads) == 2


def test_disagreement_raises_and_writes_nothing(run, tmp_path):
    with pytest.raises(RuntimeError, match="No agreement on the scan shape"):
        run([NORMAL, ODD, OTHER])
    assert list(tmp_path.glob("*.npz")) == []


def test_single_candidate_falls_back_with_warning(run, caplog):
    with caplog.at_level(logging.WARNING, logger=grid_utils.logger.name):
        path = run([NORMAL])
    assert _gates_in(path) == NORMAL["nrays"] * NORMAL["ngates"]
    assert "fewer than min_agreement" in caplog.text


def test_unreadable_files_are_skipped(run):
    path = run([None, NORMAL, NORMAL])
    assert _gates_in(path) == NORMAL["nrays"] * NORMAL["ngates"]


def test_all_unreadable_raises(run):
    with pytest.raises(RuntimeError, match="Failed to create gate coordinates"):
        run([None, None])


def test_min_agreement_one_keeps_the_old_behaviour(run):
    path = run([ODD, NORMAL], min_agreement=1)
    assert _gates_in(path) == ODD["nrays"] * ODD["ngates"]
    assert len(_FakeClient.downloads) == 1


def test_output_keeps_the_npz_contract(run):
    data = np.load(run([NORMAL, NORMAL]))
    assert set(data.files) == {"gate_x", "gate_y", "gate_z", "blind_range_m", "lowest_elev_deg"}
    assert float(data["lowest_elev_deg"]) == 0.5
