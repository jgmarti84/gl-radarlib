# -*- coding: utf-8 -*-
"""Unit tests for the NC cube writer.

The cube is consumed by code that lives outside this repo, so the tests here
pin the parts of the file format that are contractual: the south->north flip,
the NaN <-> fill-value mapping, and the global attributes a reader needs in
order to place the grid vertically and geographically.
"""

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest
from netCDF4 import Dataset

from radarlib.io.nccube_writer import write_nccube

NZ, NY, NX = 3, 4, 5


@pytest.fixture()
def ts_utc() -> datetime:
    return datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc)


@pytest.fixture()
def coords():
    return {
        "x_km": np.linspace(-2.0, 2.0, NX, dtype="float32"),
        "y_km": np.linspace(-1.5, 1.5, NY, dtype="float32"),
        "z_m": np.linspace(0.0, 1200.0, NZ, dtype="float32"),
    }


def _fields(**over):
    base = {
        "DBZH": np.arange(NZ * NY * NX, dtype="float32").reshape(NZ, NY, NX),
        "RHOHV": np.full((NZ, NY, NX), 0.95, dtype="float32"),
    }
    base.update(over)
    return base


def _write(tmp_path: Path, ts, coords, fields=None, **kw) -> Path:
    return write_nccube(
        path=tmp_path / "cube.nc",
        fields=_fields() if fields is None else fields,
        radar_lat=-31.44133,
        radar_lon=-64.19199,
        radar_alt_m=484.0,
        timestamp=ts,
        **coords,
        **kw,
    )


class TestRoundTrip:
    def test_returns_path(self, tmp_path, ts_utc, coords):
        assert isinstance(_write(tmp_path, ts_utc, coords), Path)

    def test_creates_parent_directories(self, tmp_path, ts_utc, coords):
        out = write_nccube(
            path=tmp_path / "a" / "b" / "cube.nc",
            fields=_fields(),
            radar_lat=0.0,
            radar_lon=0.0,
            radar_alt_m=0.0,
            timestamp=ts_utc,
            **coords,
        )
        assert out.is_file()

    def test_writes_every_configured_field(self, tmp_path, ts_utc, coords):
        _write(tmp_path, ts_utc, coords, fields=_fields(ZDR=np.zeros((NZ, NY, NX), "float32")))
        with Dataset(tmp_path / "cube.nc") as nc:
            assert {"DBZH", "RHOHV", "ZDR"} <= set(nc.variables)

    def test_fields_attribute_lists_what_was_written(self, tmp_path, ts_utc, coords):
        _write(tmp_path, ts_utc, coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            assert nc.fields == "DBZH,RHOHV"


class TestRowOrder:
    """Row 0 is SOUTH on input and NORTH on disk. Getting this wrong mirrors
    the cube with no shape or range change to give it away."""

    def test_data_is_flipped_north_first(self, tmp_path, ts_utc, coords):
        src = _fields()["DBZH"]
        _write(tmp_path, ts_utc, coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            np.testing.assert_array_equal(np.asarray(nc["DBZH"][:]), src[:, ::-1, :])

    def test_y_coordinate_is_not_flipped(self, tmp_path, ts_utc, coords):
        _write(tmp_path, ts_utc, coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            np.testing.assert_allclose(np.asarray(nc["y_km"][:]), coords["y_km"])

    def test_row_order_attribute_is_declared(self, tmp_path, ts_utc, coords):
        _write(tmp_path, ts_utc, coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            assert nc.row_order == "north_first"


class TestNoData:
    def test_nan_round_trips_as_masked(self, tmp_path, ts_utc, coords):
        data = _fields()["DBZH"].copy()
        data[0, 0, 0] = np.nan
        _write(tmp_path, ts_utc, coords, fields={"DBZH": data, "RHOHV": _fields()["RHOHV"]})
        with Dataset(tmp_path / "cube.nc") as nc:
            # row 0 south -> last row on disk
            assert np.ma.is_masked(nc["DBZH"][0, -1, 0])


class TestGlobalAttributes:
    def test_records_real_antenna_altitude(self, tmp_path, ts_utc, coords):
        """A consumer converts heights into the antenna-relative frame with
        this value; a wrong one shifts every echo-height test silently."""
        _write(tmp_path, ts_utc, coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            assert nc.radar_alt_m == pytest.approx(484.0)

    def test_declares_vertical_reference(self, tmp_path, ts_utc, coords):
        _write(tmp_path, ts_utc, coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            assert nc.referencia_z == "radar"

    def test_timestamp_is_iso_utc(self, tmp_path, ts_utc, coords):
        _write(tmp_path, ts_utc, coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            assert nc.timestamp == "2026-04-01T20:50:00Z"

    def test_naive_timestamp_is_read_as_utc(self, tmp_path, coords):
        _write(tmp_path, datetime(2026, 4, 1, 20, 50, 0), coords)
        with Dataset(tmp_path / "cube.nc") as nc:
            assert nc.timestamp == "2026-04-01T20:50:00Z"

    def test_extra_attrs_are_written(self, tmp_path, ts_utc, coords):
        _write(tmp_path, ts_utc, coords, extra_attrs={"radar_name": "RMA1"})
        with Dataset(tmp_path / "cube.nc") as nc:
            assert nc.radar_name == "RMA1"


class TestValidation:
    def test_rejects_unknown_vertical_reference(self, tmp_path, ts_utc, coords):
        with pytest.raises(ValueError, match="referencia_z"):
            _write(tmp_path, ts_utc, coords, referencia_z="agl")

    def test_rejects_empty_field_mapping(self, tmp_path, ts_utc, coords):
        with pytest.raises(ValueError, match="empty"):
            _write(tmp_path, ts_utc, coords, fields={})

    def test_rejects_mismatched_field_shapes(self, tmp_path, ts_utc, coords):
        bad = {"DBZH": _fields()["DBZH"], "RHOHV": np.zeros((NZ, NY, NX + 1), "float32")}
        with pytest.raises(ValueError, match="shape mismatch"):
            _write(tmp_path, ts_utc, coords, fields=bad)

    def test_rejects_non_3d_field(self, tmp_path, ts_utc, coords):
        with pytest.raises(ValueError, match=r"nz, ny, nx"):
            _write(tmp_path, ts_utc, coords, fields={"DBZH": np.zeros((NY, NX), "float32")})

    def test_rejects_coordinates_that_do_not_match_data(self, tmp_path, ts_utc, coords):
        coords = dict(coords, z_m=np.linspace(0.0, 1.0, NZ + 1, dtype="float32"))
        with pytest.raises(ValueError, match="coordinate lengths"):
            _write(tmp_path, ts_utc, coords)
