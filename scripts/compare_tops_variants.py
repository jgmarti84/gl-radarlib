#!/usr/bin/env python3
"""Compare storm-top variants on one NC cube (validation harness, read-only).

Reads a 3D volume written by ``radarlib.io.nccube_writer.write_nccube`` (DBZH,
north-first rows), derives COLMAX, detects the convective cores exactly as the
product daemon does, then reports the top of every core under:

  * the current footprint search at each ``--thresholds`` value (dBZ) and
    ``--dilation`` value (pixels), and
  * optionally the legacy cylinder search, loaded from a copy of the old
    ``tops.py`` given with ``--legacy-tops``.

The point is to choose the echo threshold with data: a large share of tops at
the highest grid level means the top is censored by the grid, not measured.

Get the legacy module without touching the working tree::

    git show origin/dev:src/radarlib/radar_grid/tops.py > /path/outside/repo/tops_legacy.py

Usage::

    python scripts/compare_tops_variants.py --cube RMA1_..._NCCUBE.nc \\
        --thresholds 0 10 18 --dilation 0 1 [--legacy-tops /path/tops_legacy.py] [--csv out.csv]
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import statistics
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import numpy.ma as ma


def load_cube(path: Path):
    """Return (dbzh[z,y,x] south-first with NaN for no data, x_m, y_m, z_m)."""
    import netCDF4

    with netCDF4.Dataset(path, "r") as nc:
        if nc.getncattr("row_order") != "north_first":
            raise SystemExit(f"{path}: unexpected row_order {nc.getncattr('row_order')!r}")
        dbzh = ma.filled(nc.variables["DBZH"][:].astype(np.float32), np.nan)
        x_m = np.asarray(nc.variables["x_km"][:], dtype=np.float64) * 1000.0
        y_m = np.asarray(nc.variables["y_km"][:], dtype=np.float64) * 1000.0
        z_m = np.asarray(nc.variables["z_m"][:], dtype=np.float32)
    dbzh = dbzh[:, ::-1, :]  # back to south-first, the convention apply_geometry uses
    return dbzh, x_m, y_m, z_m


def load_legacy(path: Path):
    spec = importlib.util.spec_from_file_location("tops_legacy", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.detect_tops_from_cores


def summarise(name: str, tops: list, z_top: float, n_cores: int) -> dict:
    alts = [t["altitude_m"] for t in tops]
    at_top = sum(1 for a in alts if a >= z_top - 1e-3)
    row = {
        "variant": name,
        "cores": n_cores,
        "tops": len(tops),
        "at_grid_top": at_top,
        "share_at_grid_top": (at_top / len(tops)) if tops else float("nan"),
        "median_alt_m": statistics.median(alts) if alts else float("nan"),
        "min_alt_m": min(alts) if alts else float("nan"),
    }
    return row


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cube", required=True, type=Path)
    ap.add_argument("--thresholds", nargs="+", type=float, default=[0.0, 10.0, 18.0])
    ap.add_argument("--dilation", nargs="+", type=int, default=[0, 1])
    ap.add_argument("--legacy-tops", type=Path, help="path to the old tops.py (git show origin/dev:...)")
    ap.add_argument("--csv", type=Path, help="write one row per core and variant")
    args = ap.parse_args()

    from radarlib.radar_grid.cores import detect_cores_from_colmax
    from radarlib.radar_grid.tops import detect_tops_from_cores

    dbzh, x_m, y_m, z_m = load_cube(args.cube)
    nz, ny, nx = dbzh.shape
    print(f"cube {args.cube.name}: grid {nz}x{ny}x{nx}, z {z_m[0]:.0f}..{z_m[-1]:.0f} m")

    colmax = np.nanmax(dbzh, axis=0)
    colmax_m = ma.masked_invalid(colmax)
    xx, yy = np.meshgrid(x_m, y_m)
    cores = detect_cores_from_colmax(colmax_m, xx.astype(np.float32), yy.astype(np.float32))
    print(f"cores detected: {len(cores)}")
    if not cores:
        print("no cores: nothing to compare")
        return

    z_top = float(z_m[-1])
    dbz_masked = ma.masked_invalid(dbzh)
    rows_out: list[dict] = []
    summaries: list[dict] = []
    variants: list[tuple[str, list]] = []

    if args.legacy_tops:
        legacy = load_legacy(args.legacy_tops)
        variants.append(("legacy_cylinder", legacy(cores, dbz_masked, xx, yy, z_m)))

    for thr in args.thresholds:
        for dil in args.dilation:
            tops = detect_tops_from_cores(cores, dbz_masked, xx, yy, z_m, min_dbz=thr, footprint_dilation_px=dil)
            variants.append((f"footprint_thr{thr:g}_dil{dil}", tops))

    for name, tops in variants:
        summaries.append(summarise(name, tops, z_top, len(cores)))
        for i, t in enumerate(tops):
            rows_out.append({"variant": name, "core": i, **{k: t[k] for k in ("x_m", "y_m", "altitude_m", "dbz")}})

    print(f"\n{'variant':24s} {'tops':>5s} {'at grid top':>12s} {'share':>7s} {'median alt':>11s} {'min alt':>9s}")
    for s in summaries:
        share = "-" if s["share_at_grid_top"] != s["share_at_grid_top"] else f"{100 * s['share_at_grid_top']:.0f}%"
        print(
            f"{s['variant']:24s} {s['tops']:5d} {s['at_grid_top']:12d} {share:>7s} "
            f"{s['median_alt_m']:10.0f}m {s['min_alt_m']:8.0f}m"
        )

    print("\naltitude histogram (m) per variant:")
    for name, tops in variants:
        hist = Counter(int(round(t["altitude_m"])) for t in tops)
        top = ", ".join(f"{a}:{n}" for a, n in sorted(hist.items(), key=lambda kv: -kv[0])[:6])
        print(f"  {name:24s} {top or '-'}")

    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["variant", "core", "x_m", "y_m", "altitude_m", "dbz"])
            w.writeheader()
            w.writerows(rows_out)
        print(f"\nCSV written: {args.csv} ({len(rows_out)} rows)")


if __name__ == "__main__":
    sys.exit(main())
