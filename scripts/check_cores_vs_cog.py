#!/usr/bin/env python
"""Check that tops/cores GeoJSON markers coincide with the COLMAX raw COG.

For every ``core`` point the script samples the COG (base resolution, band 1,
float dBZ) at the marker, then looks for the true local maximum in a window
around it and reports the pixel/metre offset.  For every ``blob`` polygon it
compares the stored ``max_dbz`` with the COG maximum inside the polygon, which
tells *wrong location* (marker displaced) apart from *wrong data* (COG does not
contain the echo the detection saw, e.g. filtered vs unfiltered COLMAX, or a
different volume/timestamp).

Usage:
    python scripts/check_cores_vs_cog.py COLMAX.tif TOPS_CORES.geojson [--window 6] [--min-dbz 40]
"""

import argparse
import json
import sys

import numpy as np
import rasterio
from pyproj import Transformer
from rasterio.features import geometry_mask
from rasterio.windows import Window


def read_base(path):
    src = rasterio.open(path)
    if src.count != 1 or src.dtypes[0] not in ("float32", "float64"):
        sys.exit(f"{path}: expected a single-band float 'raw' COG (got count={src.count}, dtype={src.dtypes[0]}). "
                 "Use the raw COLMAX COG, not the RGBA one.")
    return src


def sample(src, data, lon, lat, tr):
    x, y = tr.transform(lon, lat)
    row, col = src.index(x, y)
    inside = 0 <= row < src.height and 0 <= col < src.width
    return row, col, inside


def window_max(src, row, col, w):
    r0, c0 = max(0, row - w), max(0, col - w)
    r1, c1 = min(src.height, row + w + 1), min(src.width, col + w + 1)
    arr = src.read(1, window=Window(c0, r0, c1 - c0, r1 - r0), masked=True).astype("float64")
    arr = np.ma.filled(arr, np.nan)
    if np.isnan(arr).all():
        return None, None, arr, (r0, c0)
    i = np.unravel_index(np.nanargmax(arr), arr.shape)
    return float(arr[i]), (r0 + i[0], c0 + i[1]), arr, (r0, c0)


def fmt(v):
    return "NaN/nodata" if v is None or not np.isfinite(v) else f"{v:.1f}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("tif")
    ap.add_argument("geojson")
    ap.add_argument("--window", type=int, default=6, help="search half-width in pixels (default 6)")
    ap.add_argument("--min-dbz", type=float, default=40.0, help="CORES_MIN_Z used by the detector (default 40)")
    args = ap.parse_args()

    src = read_base(args.tif)
    tags = src.tags()
    print(f"COG: {args.tif}\n  size={src.width}x{src.height} crs={src.crs} "
          f"px=({src.res[0]:.1f},{src.res[1]:.1f}) m  tags: "
          f"{ {k: v for k, v in tags.items() if k.startswith(('radar', 'filter', 'obs', 'vol', 'strategy', 'time'))} }")
    with open(args.geojson) as fh:
        feats = json.load(fh)["features"]
    tr = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
    px_m = abs(src.res[0])

    cores = [f for f in feats if f["properties"].get("type") == "core"]
    blobs = [f for f in feats if f["properties"].get("type") == "blob"]
    tops = [f for f in feats if f["properties"].get("type") == "top"]
    print(f"GeoJSON: {args.geojson}\n  cores={len(cores)} blobs={len(blobs)} tops={len(tops)}  "
          f"obs_time={(feats[0]['properties'].get('observation_time') if feats else None)}\n")

    verdicts = []
    for i, f in enumerate(cores):
        lon, lat = f["geometry"]["coordinates"]
        row, col, inside = sample(src, None, lon, lat, tr)
        p = f["properties"]
        print(f"CORE #{i}  lon={lon:.5f} lat={lat:.5f}  mean_dbz={p.get('intensity_dbz')}  pixel=(row {row}, col {col})")
        if not inside:
            print("  marker is OUTSIDE the COG extent\n")
            verdicts.append("outside")
            continue
        here = src.read(1, window=Window(col, row, 1, 1), masked=True)[0, 0]
        here = None if np.ma.is_masked(here) else float(here)
        vmax, (mr, mc), arr, _ = window_max(src, row, col, args.window)
        print(f"  COG value at marker pixel : {fmt(here)} dBZ")
        if vmax is None:
            print(f"  window ±{args.window}px is all NaN -> data absent around marker\n")
            verdicts.append("nan")
            continue
        d_px = float(np.hypot(mr - row, mc - col))
        print(f"  max in ±{args.window}px window    : {vmax:.1f} dBZ at (row {mr}, col {mc}) -> "
              f"offset drow={mr - row:+d} dcol={mc - col:+d} = {d_px:.1f} px ≈ {d_px * px_m / 1000:.1f} km")
        n_ge = int(np.nansum(arr >= args.min_dbz))
        print(f"  pixels >= {args.min_dbz:.0f} dBZ in window : {n_ge}   NaN pixels in window: {int(np.isnan(arr).sum())}")
        if here is not None and here >= args.min_dbz:
            v = "OK (marker on a >= threshold pixel)"
        elif n_ge == 0:
            v = "DATA MISMATCH (no >= threshold echo in the COG near marker: filtered/other volume/other time?)"
        else:
            v = "LOCATION OFFSET (echo exists nearby but marker is not on it)"
        print(f"  verdict: {v}\n")
        verdicts.append(v.split(" ")[0])

    for i, f in enumerate(blobs):
        ring = f["geometry"]["coordinates"][0]
        xs, ys = zip(*[tr.transform(lo, la) for lo, la in ring])
        poly = {"type": "Polygon", "coordinates": [list(zip(xs, ys))]}
        m = geometry_mask([poly], out_shape=(src.height, src.width), transform=src.transform, invert=True)
        data = src.read(1, masked=True).astype("float64").filled(np.nan)
        vals = data[m]
        p = f["properties"]
        vm = np.nanmax(vals) if vals.size and not np.isnan(vals).all() else None
        print(f"BLOB #{i}  stored max_dbz={p.get('max_dbz')} mean={p.get('intensity_dbz')} pixels={p.get('pixel_count')}"
              f"  | COG inside hull: max={fmt(vm)} mean={fmt(np.nanmean(vals) if vals.size else None)} "
              f"NaN={int(np.isnan(vals).sum())}/{vals.size}")
        if vm is not None and abs(vm - p.get('max_dbz', vm)) > 3:
            print("  -> COG max differs from detection max by >3 dBZ: the COG is NOT the grid the detector saw "
                  "(filtered vs unfiltered, or different volume/time)")

    for i, f in enumerate(tops):
        lon, lat = f["geometry"]["coordinates"]
        row, col, inside = sample(src, None, lon, lat, tr)
        v = None
        if inside:
            a = src.read(1, window=Window(col, row, 1, 1), masked=True)[0, 0]
            v = None if np.ma.is_masked(a) else float(a)
        print(f"TOP #{i}  alt={f['properties'].get('altitude_m')} m  COLMAX at top pixel={fmt(v)} "
              f"(top is the max-dBZ point at the highest level, so it need NOT sit on the COLMAX peak)")

    print("\nSummary:", {k: verdicts.count(k) for k in set(verdicts)} or "no cores")


if __name__ == "__main__":
    main()
