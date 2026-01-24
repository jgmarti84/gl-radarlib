import radarlib.io.bufr.bufr as bufr_mod


def main_decode():
    test_filename = "data/radares/RMA1/bufr/RMA1_0315_01_DBZV_20251208T191648Z.BUFR"
    meta_vol, sweeps, vol_data, run_log = bufr_mod.dec_bufr_file(
        test_filename, root_resources=None, parallel=False  # type: ignore[arg-type]
    )
    assert isinstance(meta_vol, dict)
    assert vol_data.ndim == 2
    assert len(sweeps) == meta_vol["nsweeps"]


if __name__ == "__main__":
    main_decode()
