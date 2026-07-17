from importlib import import_module

import numpy as np
import pandas as pd

from radarlib.io.bufr import pyart_writer  # noqa: F401, E402

# Import the module directly without going through __init__.py
# This avoids the name collision between the module and the function
bufr_to_pyart_module = import_module("radarlib.io.bufr.bufr_to_pyart")


def make_field(filename: str, ngates: int, nrays: int, gate_offset: int, gate_size: int, elev: float):
    sweeps = pd.DataFrame(
        {
            "ngates": [ngates],
            "nrayos": [nrays],
            "gate_offset": [gate_offset],
            "gate_size": [gate_size],
            "elevaciones": [elev],
        }
    )
    info = {"sweeps": sweeps, "metadata": {"instrument_name": "TEST_RADAR"}, "filename": filename, "nsweeps": 1}
    data = np.ma.ones((nrays, ngates), dtype=np.float32) * 5.0
    return {"info": info, "data": data, "tipo_producto": filename.split("_")[2]}


def test_find_reference_field():
    f1 = make_field("R_1_DBZH_1.BUFR", 100, 10, 0, 100, 1.0)
    f2 = make_field("R_1_VRAD_1.BUFR", 150, 10, 0, 100, 1.0)
    idx = bufr_to_pyart_module._find_reference_field([f1, f2])
    assert idx == 1


def test_find_reference_field_prefers_min_offset():
    # Reproduces the RMA20 case: PHIDP/RHOHV have a slightly larger gate_offset
    # than DBZH/DBZV (half-gate shift), which gives them a marginally larger
    # last_gate. The old logic chose PHIDP as reference, causing init=-1 for
    # all 1440m-offset fields. The new logic picks the minimum-offset field.
    dbzh = make_field("R_1_DBZH_1.BUFR", 653, 360, 1440, 360, 1.0)  # last_gate=236,520m
    phidp = make_field("R_1_PHIDP_1.BUFR", 653, 360, 1620, 360, 1.0)  # last_gate=236,700m
    # PHIDP has a larger last_gate but a larger offset — must NOT be chosen as reference.
    idx = bufr_to_pyart_module._find_reference_field([dbzh, phidp])
    assert idx == 0


def test_align_field_to_reference():
    # reference 200 gates, field has offset 100 and 50 gates
    ref_ngates = 200
    field = make_field("R_1_DBZH_1.BUFR", 50, 5, 100, 100, 1.0)
    aligned = bufr_to_pyart_module._align_field_to_reference(
        field, ref_gate_offset=0, ref_gate_size=100, ref_ngates=ref_ngates
    )
    assert aligned["data"].shape == (5, ref_ngates)
    # check that values were placed starting at gate 1 (index 1 because offset 100 / gate_size 100)
    assert aligned["data"][0, 1] == 5.0
