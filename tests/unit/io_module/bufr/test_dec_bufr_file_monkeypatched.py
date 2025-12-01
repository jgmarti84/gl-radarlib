import numpy as np

from radarlib.io.bufr import bufr as bufr_mod


def make_fake_meta():
    return {
        "radar_name": "RMA11",
        "estrategia_nombre": "0315",
        "estrategia_nvol": "01",
        "tipo_producto": "KDP",
        "filename": "RMA11_0315_01_KDP_20251020T151109Z.BUFR",
        "year": 2025,
        "month": 10,
        "day": 20,
        "hour": 15,
        "min": 11,
        "lat": -31.4,
        "lon": -64.2,
        "radar_height": 100.0,
        "nsweeps": 1,
    }


def test_dec_bufr_file_monkeypatched(monkeypatch, sample_sweep_bytes):
    # Patch low-level functions used by dec_bufr_file
    monkeypatch.setattr(bufr_mod, "get_metadata", lambda lib, path, root_resources=None: make_fake_meta())
    monkeypatch.setattr(bufr_mod, "get_size_data", lambda lib, path, root_resources=None: 0)
    monkeypatch.setattr(
        bufr_mod,
        "get_raw_volume",
        lambda lib, path, size, root_resources=None: np.array([1], dtype=int),
    )
    monkeypatch.setattr(bufr_mod, "get_elevations", lambda lib, path, max_elev=30, root_resources=None: np.array([0.5]))

    sweep = dict(sample_sweep_bytes)
    sweep.update(
        {
            "year": 2025,
            "month": 10,
            "day": 20,
            "hour": 15,
            "min": 11,
            "sec": 0,
            "product_type": 1,
            "elevation": 0.5,
            "range_size": 1,
            "range_offset": 0,
            "nrays": sweep["nrays"],
            "ngates": sweep["ngates"],
            "antenna_beam_az": 0,
        }
    )

    monkeypatch.setattr(bufr_mod, "parse_sweeps", lambda vol, nsweeps, elevs: [sweep])

    # Use a filename that matches the expected pattern:
    # <RADAR>_<ESTRATEGIA>_<NVOL>_<TIPO>_<TIMESTAMP>.BUFR
    test_filename = "RMA11_0315_01_KDP_20251020T151109Z.BUFR"
    meta_vol, sweeps, vol_data, run_log = bufr_mod.dec_bufr_file(
        test_filename, root_resources=None, parallel=False  # type: ignore[arg-type]
    )
    assert isinstance(meta_vol, dict)
    assert vol_data.ndim == 2
    assert len(sweeps) == meta_vol["nsweeps"]
