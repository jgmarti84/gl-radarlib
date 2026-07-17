#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inspect_netcdf.py — Structural summary of a CFRadial NetCDF file.

Loads the file via PyART and prints: radar metadata, gate dimensions,
per-sweep geometry, and per-field statistics (shape, units, data range,
masked-gate fraction). Useful to verify a generated NetCDF before
feeding it into the product pipeline.

Usage:
    python3 scripts/inspect_netcdf.py /path/to/file.nc

Docker execution:
    docker exec genpro25-rma20 python3 /workspace/scripts/inspect_netcdf.py \\
        /workspace/app/data/radares/RMA20/netcdf/RMA20_0315_01_20260716T154443Z.nc
"""

import argparse
import logging
import sys
from pathlib import Path

import numpy as np

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("inspect_netcdf")

SEP = "-" * 64


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fmt_m(v: float) -> str:
    return f"{v:>10,.0f} m"


def _pct(n: int, total: int) -> str:
    return f"{100 * n / total:.1f}%" if total > 0 else "—"


# ---------------------------------------------------------------------------
# Core inspection
# ---------------------------------------------------------------------------


def inspect(nc_path: str) -> None:
    import pyart

    p = Path(nc_path)
    if not p.exists():
        print(f"[ERROR] File not found: {nc_path}", file=sys.stderr)
        sys.exit(1)

    size_mb = p.stat().st_size / (1024 ** 2)

    print(f"\n{SEP}")
    print(f"  NETCDF FILE: {p.name}")
    print(SEP)

    print(f"\n  File size: {size_mb:.2f} MB")

    radar = pyart.io.read_cfradial(str(p))

    # --- Metadata ---
    meta = radar.metadata
    print("\n[Radar metadata]")
    for key in ("instrument_name", "source", "history", "references", "comment"):
        val = meta.get(key)
        if val:
            print(f"  {key:<20}: {str(val).strip()[:80]}")

    lat = float(radar.latitude["data"][0])
    lon = float(radar.longitude["data"][0])
    alt = float(radar.altitude["data"][0])
    print(f"  {'latitude':<20}: {lat:.5f}°")
    print(f"  {'longitude':<20}: {lon:.5f}°")
    print(f"  {'altitude':<20}: {alt:.1f} m")

    time_units = radar.time.get("units", "")
    time_start = radar.time["data"][0] if len(radar.time["data"]) > 0 else None
    time_end = radar.time["data"][-1] if len(radar.time["data"]) > 0 else None
    print(f"  {'time units':<20}: {time_units}")
    if time_start is not None:
        print(f"  {'time start':<20}: {time_start:.1f} s")
        print(f"  {'time end':<20}: {time_end:.1f} s")

    # --- Gate dimensions ---
    range_data = radar.range["data"]
    gate_size = float(range_data[1] - range_data[0]) if len(range_data) > 1 else float("nan")
    print("\n[Gate dimensions]")
    print(f"  n_sweeps       : {radar.nsweeps}")
    print(f"  n_rays (total) : {radar.nrays}")
    print(f"  n_gates        : {radar.ngates}")
    print(f"  total gates    : {radar.nrays * radar.ngates:,}")
    print(f"  range first    :{_fmt_m(float(range_data[0]))}")
    print(f"  range last     :{_fmt_m(float(range_data[-1]))}")
    print(f"  gate size      :{_fmt_m(gate_size)}")

    # --- Per-sweep table ---
    print("\n[Per-sweep geometry]")
    header = f"  {'sweep':>5}  {'fixed_ang':>9}  {'n_rays':>7}  {'ray_start':>9}  {'ray_end':>7}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    for i in range(radar.nsweeps):
        sl = radar.get_slice(i)
        n_rays_sweep = sl.stop - sl.start
        fixed = float(radar.fixed_angle["data"][i])
        print(f"  {i:>5}  {fixed:>9.3f}°  {n_rays_sweep:>7}  {sl.start:>9}  {sl.stop - 1:>7}")

    # --- Fields table ---
    print("\n[Fields]")
    col_fn = 10
    col_sh = 16
    col_un = 14
    col_mn = 12
    col_mx = 12
    col_ms = 10

    hdr = (
        f"  {'FIELD':<{col_fn}}  "
        f"{'SHAPE':<{col_sh}}  "
        f"{'UNITS':<{col_un}}  "
        f"{'DATA MIN':>{col_mn}}  "
        f"{'DATA MAX':>{col_mx}}  "
        f"{'MASKED%':>{col_ms}}"
    )
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    for name in sorted(radar.fields.keys()):
        fld = radar.fields[name]
        data = fld["data"]
        units = fld.get("units", "—")
        shape_str = f"({data.shape[0]}, {data.shape[1]})"

        if np.ma.is_masked(data):
            valid = data.compressed()
            n_masked = data.mask.sum() if data.mask.shape else 0
            total = data.size
        else:
            valid = data.ravel()
            n_masked = 0
            total = data.size

        if len(valid) > 0:
            d_min = f"{float(valid.min()):.3f}"
            d_max = f"{float(valid.max()):.3f}"
        else:
            d_min = d_max = "all masked"

        masked_pct = _pct(n_masked, total)

        print(
            f"  {name:<{col_fn}}  "
            f"{shape_str:<{col_sh}}  "
            f"{str(units):<{col_un}}  "
            f"{d_min:>{col_mn}}  "
            f"{d_max:>{col_mx}}  "
            f"{masked_pct:>{col_ms}}"
        )

    print(f"\n{SEP}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print structural summary of a CFRadial NetCDF file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("netcdf", help="Path to the local NetCDF file.")
    args = parser.parse_args()
    inspect(args.netcdf)


if __name__ == "__main__":
    main()
