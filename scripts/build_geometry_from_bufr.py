#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_geometry_from_bufr.py — Build a GridGeometry from a local BUFR file.

Decodes the BUFR, extracts gate coordinates, builds the grid geometry with
the given ROI parameters, and prints detailed stats. Optionally saves the
resulting .npz file to a directory.

ROI parameters can be supplied as a JSON string via --roi-params. Any key
not provided falls back to the radarlib config defaults. This makes it easy
to experiment with different values without touching config files.

Usage:
    # Use defaults from radarlib config
    python3 scripts/build_geometry_from_bufr.py /path/to/file.BUFR

    # Override specific params
    python3 scripts/build_geometry_from_bufr.py /path/to/file.BUFR \\
        --roi-params '{"toa": 4000, "res_z": 400, "hfac": 1.5, "nb": 1.5, "bsp": 1.5}'

    # Build and save the geometry
    python3 scripts/build_geometry_from_bufr.py /path/to/file.BUFR \\
        --roi-params '{"toa": 4000}' \\
        --save-dir /workspace/app/data/geometries

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/build_geometry_from_bufr.py \\
        /workspace/app/product_output/RMA1/vol04/RMA1_0315_04_DBZH_20260526T181241Z.BUFR \\
        --roi-params '{"toa": 4000, "res_z": 400}' \\
        --save-dir /workspace/app/data/geometries

ROI parameter reference:
    res_xy          Grid horizontal resolution in metres (default: 1000)
    res_z           Grid vertical resolution in metres (default: 600)
    toa             Top of atmosphere / max grid height in metres (default: 15000)
    hfac            Vertical beam-width factor for ROI (default: 1.02)
    nb              Nominal beam-width in degrees (default: 1.45)
    bsp             Beam-spacing multiplier (default: 1.2)
    min_radius      Minimum ROI radius in metres near the radar (default: 900)
    max_neighbors   Max gates per grid point (default: 1)
    weight_function Interpolation method: 'nearest' or 'barnes2' (default: 'nearest')
"""

import argparse
import json
import logging
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("build_geometry_from_bufr")

SEP = "-" * 68


# ---------------------------------------------------------------------------
# Defaults (mirror radarlib config so this script can run standalone)
# ---------------------------------------------------------------------------


def _radarlib_defaults() -> Dict[str, Any]:
    """Pull defaults from radarlib.config, falling back to hardcoded values."""
    try:
        from radarlib import config as rc

        return {
            "res_xy": rc.GEOMETRY_RES_XY,
            "res_z": rc.GEOMETRY_RES_Z,
            "toa": rc.GEOMETRY_TOA,
            "hfac": rc.GEOMETRY_HFAC,
            "nb": rc.GEOMETRY_NB,
            "bsp": rc.GEOMETRY_BSP,
            "min_radius": rc.GEOMETRY_MIN_RADIUS,
            "max_neighbors": rc.MAX_NEIGHBORS,
            "weight_function": rc.WEIGHT_FUNCTION,
        }
    except Exception:
        return {
            "res_xy": 1000.0,
            "res_z": 600.0,
            "toa": 15000.0,
            "hfac": 1.02,
            "nb": 1.45,
            "bsp": 1.2,
            "min_radius": 900.0,
            "max_neighbors": 1,
            "weight_function": "nearest",
        }


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def build_geometry(bufr_path: str, roi_overrides: Dict[str, Any], save_dir: str | None) -> None:
    from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart
    from radarlib.radar_grid import (
        build_geometry_filename,
        compute_grid_geometry,
        get_gate_coordinates,
        get_radar_info,
        save_geometry,
    )
    from radarlib.radar_grid.utils import calculate_grid_points, infer_blind_range_m

    p = Path(bufr_path)
    if not p.exists():
        print(f"[ERROR] File not found: {bufr_path}", file=sys.stderr)
        sys.exit(1)

    roi_params = dict(_radarlib_defaults(), **roi_overrides)

    print(f"\n{SEP}")
    print(f"  BUILD GEOMETRY FROM BUFR: {p.name}")
    print(SEP)

    # --- Load radar ---
    logger.info("Loading BUFR file…")
    t0 = time.time()
    radar = bufr_paths_to_pyart([str(p)], save_path=None)
    info = get_radar_info(radar)
    print("\n[Radar]")
    print(f"  Name     : {info['radar_name']}  strategy={info['strategy']}  vol={info['volume_nr']}")

    print(
        f"  Sweeps   : {info['nsweeps']}  elevations: "
        + f"{sorted(set(round(float(e), 3) for e in radar.fixed_angle['data']))}"
    )
    print(f"  Fields   : {', '.join(info['fields'])}")

    # --- ROI params in use ---
    print("\n[ROI parameters]")
    defaults = _radarlib_defaults()
    for k, v in roi_params.items():
        tag = " ← overridden" if k in roi_overrides else ""
        default_v = defaults.get(k, "?")
        print(f"  {k:<20}: {v}   (default: {default_v}){tag}")

    # --- Gate coordinates ---
    logger.info("Extracting gate coordinates…")
    gate_x, gate_y, gate_z = get_gate_coordinates(radar)
    blind_m = infer_blind_range_m(radar)
    lowest_elev = float(np.min(radar.fixed_angle["data"]))

    print("\n[Gate coordinates]")
    print(f"  Total gates : {len(gate_x):,}")
    print(f"  x extent    : {gate_x.min():>10,.0f} m  →  {gate_x.max():>10,.0f} m")
    print(f"  y extent    : {gate_y.min():>10,.0f} m  →  {gate_y.max():>10,.0f} m")
    print(f"  z extent    : {gate_z.min():>10,.0f} m  →  {gate_z.max():>10,.0f} m")
    print(f"  blind range : {blind_m:,.0f} m")
    print(f"  lowest elev : {lowest_elev:.3f}°")

    # --- Grid shape ---
    z_limits = (0.0, float(roi_params["toa"]))
    y_limits = (float(gate_y.min()), float(gate_y.max()))
    x_limits = (float(gate_x.min()), float(gate_x.max()))
    nz, ny, nx = calculate_grid_points(z_limits, y_limits, x_limits, roi_params["res_xy"], roi_params["res_z"])
    grid_shape = (nz, ny, nx)
    grid_limits = (z_limits, y_limits, x_limits)

    print("\n[Grid shape]")
    print(f"  shape (z, y, x) : {grid_shape}   →  {nz*ny*nx:,} voxels")
    print(f"  z limits        : {z_limits[0]:,.0f} m  →  {z_limits[1]:,.0f} m")
    print(f"  y limits        : {y_limits[0]:,.0f} m  →  {y_limits[1]:,.0f} m")
    print(f"  x limits        : {x_limits[0]:,.0f} m  →  {x_limits[1]:,.0f} m")

    # --- Compute geometry ---
    print("\n[Computing geometry…]")
    t1 = time.time()
    with tempfile.TemporaryDirectory(prefix="radarlib_geo_") as tmp:
        geometry = compute_grid_geometry(
            gate_x,
            gate_y,
            gate_z,
            grid_shape,
            grid_limits,
            temp_dir=tmp,
            toa=float(roi_params["toa"]),
            min_radius=float(roi_params["min_radius"]),
            radar_altitude=info["altitude"],
            h_factor=float(roi_params["hfac"]),
            nb=float(roi_params["nb"]),
            bsp=float(roi_params["bsp"]),
            weighting=str(roi_params["weight_function"]),
            max_neighbors=int(roi_params["max_neighbors"]),
            blind_range_m=blind_m,
            lowest_elev_deg=lowest_elev,
            n_workers=8,
        )
    elapsed = time.time() - t1

    # --- Stats ---
    z_levels_m = geometry.z_levels()
    coverage = geometry.n_pairs() / geometry.n_grid_points() * 100.0
    empty_voxels = geometry.n_grid_points() - np.diff(geometry.indptr).astype(bool).sum()

    print(f"  Elapsed         : {elapsed:.1f} s")
    print("\n[Geometry stats]")
    print(f"  grid shape      : {geometry.grid_shape}")
    print(f"  total voxels    : {geometry.n_grid_points():,}")
    print(f"  total pairs     : {geometry.n_pairs():,}")
    print(f"  avg neighbors   : {geometry.avg_neighbors():.3f}")
    print(f"  voxel coverage  : {coverage:.1f}%  ({empty_voxels:,} empty voxels)")
    print(f"  memory usage    : {geometry.memory_usage_mb():.1f} MB")
    print(f"\n[Z levels (toa={roi_params['toa']:.0f} m, res_z={roi_params['res_z']:.0f} m)]")
    for iz, z in enumerate(z_levels_m):
        n_filled = int((np.diff(geometry.indptr)[iz * ny * nx : (iz + 1) * ny * nx] > 0).sum())
        pct = n_filled / (ny * nx) * 100.0
        bar = "█" * int(pct / 5)
        print(f"  level {iz:>2}  {z:>7,.0f} m  {n_filled:>6,} filled  ({pct:>5.1f}%)  {bar}")

    # --- Save ---
    metadata = {
        "radar_name": info["radar_name"],
        "strategy": info["strategy"],
        "volume_nr": info["volume_nr"],
        "grid_resolution_xy": roi_params["res_xy"],
        "grid_resolution_z": roi_params["res_z"],
        "toa": roi_params["toa"],
        "h_factor": roi_params["hfac"],
        "min_radius": roi_params["min_radius"],
        "max_neighbors": roi_params["max_neighbors"],
        "nb": roi_params["nb"],
        "bsp": roi_params["bsp"],
        "weighting": roi_params["weight_function"],
    }
    geometry.metadata = metadata
    filename = build_geometry_filename(metadata) + ".npz"

    if save_dir:
        out_dir = Path(save_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        save_path = out_dir / filename
        save_geometry(geometry, str(save_path))
        print("\n[Saved]")
        print(f"  {save_path}")
    else:
        print("\n[Not saved — pass --save-dir to persist the geometry]")
        print(f"  Would save as: {filename}")

    print(f"\n  Total elapsed: {time.time() - t0:.1f} s")
    print(f"\n{SEP}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a GridGeometry from a BUFR file and print stats.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("bufr", help="Path to the local BUFR file.")
    parser.add_argument(
        "--roi-params",
        type=str,
        default="{}",
        metavar="JSON",
        help=(
            "JSON dict of ROI parameter overrides. Keys not provided fall back "
            "to radarlib config defaults. Example: "
            '\'{"toa": 4000, "res_z": 400, "hfac": 1.5}\''
        ),
    )
    parser.add_argument(
        "--save-dir",
        type=str,
        default=None,
        metavar="DIR",
        help="Directory to save the resulting .npz geometry file. Skipped if not set.",
    )
    args = parser.parse_args()

    try:
        roi_overrides = json.loads(args.roi_params)
    except json.JSONDecodeError as exc:
        print(f"[ERROR] --roi-params is not valid JSON: {exc}", file=sys.stderr)
        sys.exit(1)

    build_geometry(args.bufr, roi_overrides, args.save_dir)


if __name__ == "__main__":
    main()
