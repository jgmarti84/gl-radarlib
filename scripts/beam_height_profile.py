#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
beam_height_profile.py — Beam height table for every elevation in a BUFR file.

For each sweep, prints beam height (m and km) at regular range steps up to the
radar's maximum range. Helps decide the appropriate `toa` and understand where
data actually exists vertically — critical for single-elevation volumes (e.g.
vol04 vigilant) that should NOT use the same toa as multi-elevation scans.

Usage:
    python3 scripts/beam_height_profile.py /path/to/file.BUFR
    python3 scripts/beam_height_profile.py /path/to/file.BUFR --step-km 10
    python3 scripts/beam_height_profile.py /path/to/file.BUFR --step-km 25 --max-range-km 240

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/beam_height_profile.py \\
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
logger = logging.getLogger("beam_height_profile")

SEP = "-" * 72


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def beam_height_profile(bufr_path: str, step_km: float, max_range_km: float) -> None:
    from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart
    from radarlib.radar_grid import get_radar_info
    from radarlib.radar_grid.utils import compute_beam_height, safe_range_max_m

    p = Path(bufr_path)
    if not p.exists():
        print(f"[ERROR] File not found: {bufr_path}", file=sys.stderr)
        sys.exit(1)

    radar = bufr_paths_to_pyart([str(p)], save_path=None)
    info = get_radar_info(radar)
    antenna_alt = info["altitude"]
    data_range_max_km = safe_range_max_m(radar, round_to_km=20) / 1000.0
    effective_max_km = min(max_range_km, data_range_max_km) if max_range_km else data_range_max_km

    print(f"\n{SEP}")
    print(f"  BEAM HEIGHT PROFILE: {p.name}")
    print(f"  Radar: {info['radar_name']}  strategy={info['strategy']}  vol={info['volume_nr']}")
    print(f"  Antenna altitude: {antenna_alt:.1f} m  |  Range max (data): {data_range_max_km:.0f} km")
    print(f"  Step: {step_km} km  |  Table range: 0 – {effective_max_km:.0f} km")
    print(SEP)

    # Range array for the profile table (km → m)
    range_steps_km = np.arange(step_km, effective_max_km + 1, step_km)
    range_steps_m = range_steps_km * 1000.0

    # Build column header
    col_w = 9
    header_ranges = "".join(f"{int(r):>{col_w}} km" for r in range_steps_km)
    print(f"\n  {'elev':>7}  {header_ranges}")
    print("  " + "-" * (9 + len(header_ranges)))

    elevations = radar.fixed_angle["data"]
    for elev_deg in sorted(set(float(e) for e in elevations)):
        heights_m = compute_beam_height(range_steps_m, elev_deg, radar_altitude=antenna_alt)
        heights_km = heights_m / 1000.0
        row = "".join(f"{h:>{col_w+3}.2f} km" for h in heights_km)
        print(f"  {elev_deg:>6.2f}°  {row}")

    # Summary: per-elevation max height at data range max
    print(f"\n[Maximum beam height at data range ({data_range_max_km:.0f} km)]")
    for elev_deg in sorted(set(float(e) for e in elevations)):
        h_max = compute_beam_height(np.array([data_range_max_km * 1000.0]), elev_deg, radar_altitude=antenna_alt)
        print(f"  elev {elev_deg:>6.2f}°  →  {float(h_max[0]):>8,.0f} m  ({float(h_max[0])/1000:.2f} km)")

    print("\n[Guidance]")
    max_elev = max(float(e) for e in elevations)
    h_at_range_max = compute_beam_height(np.array([data_range_max_km * 1000.0]), max_elev, radar_altitude=antenna_alt)
    suggested_toa = max(1000.0 * round(float(h_at_range_max[0]) / 1000.0), 3000.0)
    print(f"  Highest beam at range max : {float(h_at_range_max[0]):,.0f} m")
    print(f"  Suggested toa             : {suggested_toa:,.0f} m")
    print("  (Default multi-elev toa   : 15000 m — likely wasteful for single-elevation volumes)")
    print(f"\n{SEP}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print beam height profile for every elevation in a BUFR file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("bufr", help="Path to the local BUFR file.")
    parser.add_argument(
        "--step-km",
        type=float,
        default=20.0,
        metavar="KM",
        help="Range step for the table in km (default: 20).",
    )
    parser.add_argument(
        "--max-range-km",
        type=float,
        default=None,
        metavar="KM",
        help="Truncate table at this range in km (default: file's range max).",
    )
    args = parser.parse_args()
    beam_height_profile(args.bufr, args.step_km, args.max_range_km)


if __name__ == "__main__":
    main()
