#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inspect_bufr_scan.py — Structural summary of a single BUFR file.

Decodes the BUFR, prints per-sweep geometry (elevation, n_rays, n_gates,
range max, max beam height), field list, blind range, and gate-coordinate
extents. Useful as the first step before tuning geometry params.

Usage:
    python3 scripts/inspect_bufr_scan.py /path/to/file.BUFR

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/inspect_bufr_scan.py \\
        /workspace/app/product_output/RMA1/vol04/RMA1_0315_04_DBZH_20260526T181241Z.BUFR
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
logger = logging.getLogger("inspect_bufr_scan")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SEP = "-" * 64


def _fmt_m(v: float) -> str:
    return f"{v:>10,.0f} m"


def _fmt_km(v: float) -> str:
    return f"{v / 1000:>8.1f} km"


# ---------------------------------------------------------------------------
# Core inspection
# ---------------------------------------------------------------------------


def inspect(bufr_path: str) -> None:
    from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart
    from radarlib.radar_grid import get_gate_coordinates, get_radar_info
    from radarlib.radar_grid.utils import beam_height_max_km, infer_blind_range_m, safe_range_max_m

    p = Path(bufr_path)
    if not p.exists():
        print(f"[ERROR] File not found: {bufr_path}", file=sys.stderr)
        sys.exit(1)

    print(f"\n{SEP}")
    print(f"  BUFR FILE: {p.name}")
    print(SEP)

    radar = bufr_paths_to_pyart([str(p)], save_path=None)
    info = get_radar_info(radar)

    # --- Basic metadata ---
    print("\n[Radar metadata]")
    print(f"  Radar name     : {info['radar_name']}")
    print(f"  Strategy       : {info['strategy']}")
    print(f"  Volume nr      : {info['volume_nr']}")
    print(f"  Latitude       : {info['latitude']:.4f}°")
    print(f"  Longitude      : {info['longitude']:.4f}°")
    print(f"  Altitude       : {info['altitude']:.1f} m")
    print(f"  Fields         : {', '.join(info['fields'])}")

    # --- Global gate stats ---
    blind_m = infer_blind_range_m(radar)
    range_max_m = safe_range_max_m(radar, round_to_km=20)
    print("\n[Gate dimensions]")
    print(f"  n_sweeps       : {info['nsweeps']}")
    print(f"  n_rays (total) : {info['nrays']}")
    print(f"  n_gates        : {info['ngates']}")
    print(f"  total gates    : {info['total_gates']:,}")
    print(f"  range first    :{_fmt_m(info['range_min'])}")
    print(f"  range last     :{_fmt_m(info['range_max'])}")
    print(f"  range max (safe):{_fmt_m(range_max_m)}")
    print(f"  blind range    :{_fmt_m(blind_m)}")

    # --- Per-sweep table ---
    print("\n[Per-sweep geometry]")
    header = (
        f"  {'sweep':>5}  {'elev_deg':>9}  {'n_rays':>7}  {'range_max':>12}  {'beam_top_km':>11}  {'rays_uniform':>12}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))

    sweep_elevations = radar.fixed_angle["data"]
    for i in range(radar.nsweeps):
        sl = radar.get_slice(i)
        n_rays_sweep = sl.stop - sl.start
        elev = float(sweep_elevations[i])
        r_max = float(radar.range["data"][-1])
        beam_top = beam_height_max_km(r_max, elev, info["altitude"])

        # Check ray uniformity within sweep
        elev_in_sweep = radar.elevation["data"][sl]
        elev_std = float(np.std(elev_in_sweep))
        uniform = "yes" if elev_std < 0.05 else f"no (σ={elev_std:.3f}°)"

        print(f"  {i:>5}  {elev:>9.3f}°  {n_rays_sweep:>7}  {_fmt_km(r_max):>12}  {beam_top:>10.2f} km  {uniform:>12}")

    # --- Gate coordinate extents ---
    print("\n[Gate coordinate extents]")
    gate_x, gate_y, gate_z = get_gate_coordinates(radar)
    print(
        f"  x range  : {gate_x.min():>10,.0f} m  →  {gate_x.max():>10,.0f} m "
        + f"  (span {gate_x.max()-gate_x.min():,.0f} m)"
    )
    print(
        f"  y range  : {gate_y.min():>10,.0f} m  →  {gate_y.max():>10,.0f} m "
        + f"  (span {gate_y.max()-gate_y.min():,.0f} m)"
    )
    print(
        f"  z range  : {gate_z.min():>10,.0f} m  →  {gate_z.max():>10,.0f} m "
        + f"  (span {gate_z.max()-gate_z.min():,.0f} m)"
    )

    print("\n[Recommended toa hint]")
    max_z = float(gate_z.max())
    suggested_toa = max(1000.0 * round(max_z / 1000.0), 3000.0)
    print(f"  Max gate z     : {max_z:,.0f} m")
    print(f"  Suggested toa  : {suggested_toa:,.0f} m  (rounded to nearest km, ≥ 3000 m)")
    print("  (Compare with current default toa=15000 m for multi-elevation volumes)")

    print(f"\n{SEP}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print structural summary of a BUFR file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("bufr", help="Path to the local BUFR file.")
    args = parser.parse_args()
    inspect(args.bufr)


if __name__ == "__main__":
    main()
