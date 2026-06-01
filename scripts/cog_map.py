#!/usr/bin/env python3
"""
cog_map.py — Generate an interactive Leaflet map from radar COG files.

Reads COG GeoTIFFs produced by radarlib/genpro25, renders each field as a
georeferenced colour overlay on an OpenStreetMap basemap, and writes a
self-contained HTML file that can be opened directly in any browser.

Usage
-----
    python cog_map.py [OPTIONS]

Key options
-----------
  --radar        Radar station code               (default: RMA11)
  --strategy     Volume strategy                  (default: 0315)
  --volumes      Comma-separated volume numbers   (default: 01,04)
  --fields       Comma-separated field names      (default: DBZH)
  --timestamps   Comma-separated timestamps OR "latest:N" to pick N most
                 recent per group                 (default: latest:3)
  --date         YYYY/MM/DD sub-folder inside --product-dir  (default: today)
  --product-dir  Root product output directory
                 (default: /workspace/app/product_output)
  --radarlib-src Path to radarlib/src for colormap registration
                 (default: /workspaces/radarlib/src)
  --output       Output HTML file path            (default: /tmp/radar_cog_map.html)
  --open         Open the HTML file in the default browser after writing
  --no-meta      Skip printing the metadata table

Examples
--------
  # Three most recent DBZHo + DBZH for vol01 and vol04, today
  python cog_map.py --radar RMA11 --fields DBZH --volumes 01,04

  # Specific timestamps, only vol04, both DBZH variants, open in browser
  python cog_map.py --radar RMA11 --volumes 04 \\
      --timestamps 20260527T023000Z,20260527T024000Z,20260527T025000Z \\
      --open

  # Custom date and output path
  python cog_map.py --radar RMA1 --date 2026/05/20 \\
      --fields DBZH,ZDR --volumes 01 --output ~/Desktop/rma1_map.html --open

# Most recent 3 timestamps, vol01 + vol04, DBZH — same as what was just shown
docker exec genpro25-rma11 python3 /workspaces/radarlib/scripts/cog_map.py

# Specific timestamps only
docker exec genpro25-rma11 python3 /workspaces/radarlib/scripts/cog_map.py \\
  --timestamps 20260527T023000Z,20260527T024000Z,20260527T025000Z

# Different date
docker exec genpro25-rma11 python3 /workspaces/radarlib/scripts/cog_map.py \\
  --date 2026/05/20

# Multiple fields, open in browser automatically
docker exec genpro25-rma11 python3 /workspaces/radarlib/scripts/cog_map.py \\
  --fields DBZH,ZDR --volumes 01 \\
  --output /tmp/rma11_map.html \\
  && docker cp genpro25-rma11:/tmp/rma11_map.html ~/Desktop/rma11_map.html \\
  && open ~/Desktop/rma11_map.html
"""

import argparse
import base64
import datetime
import glob
import io
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

# ── Optional heavy imports (fail loudly if missing) ──────────────────────────
try:
    import rasterio
    import rasterio.warp
    from rasterio.crs import CRS
except ImportError:
    sys.exit("ERROR: rasterio is required. Install it with: pip install rasterio")

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("ERROR: matplotlib is required. Install it with: pip install matplotlib")

# ─────────────────────────────────────────────────────────────────────────────
WGS84 = CRS.from_epsg(4326)


# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ─────────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate an interactive Leaflet HTML map from radar COG files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--radar", default="RMA11", help="Radar station code (e.g. RMA11, RMA1)")
    parser.add_argument("--strategy", default="0315", help="Volume strategy code (e.g. 0315)")
    parser.add_argument("--volumes", default="01,04", help="Comma-separated volume numbers (e.g. 01,04)")
    parser.add_argument("--fields", default="DBZH", help="Comma-separated field names (e.g. DBZH,ZDR)")
    parser.add_argument(
        "--timestamps",
        default="latest:3",
        help=(
            "Comma-separated list of timestamps (e.g. 20260527T023000Z,20260527T024000Z), "
            "OR 'latest:N' to pick the N most recent files per group (default: latest:3)"
        ),
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date sub-folder as YYYY/MM/DD (default: today in UTC)",
    )
    parser.add_argument(
        "--product-dir",
        default="/workspace/app/product_output",
        help="Root product output directory (default: /workspace/app/product_output)",
    )
    parser.add_argument(
        "--radarlib-src",
        default="/workspaces/radarlib/src",
        help="Path to radarlib/src for colormap registration",
    )
    parser.add_argument(
        "--output",
        default="/tmp/radar_cog_map.html",
        help="Output HTML file path (default: /tmp/radar_cog_map.html)",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the HTML file in the default browser after writing",
    )
    parser.add_argument(
        "--no-meta",
        action="store_true",
        help="Skip printing the metadata table",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# File collection
# ─────────────────────────────────────────────────────────────────────────────


def collect_files(
    product_dir: str,
    radar: str,
    strategy: str,
    volumes: List[str],
    fields: List[str],
) -> Dict[str, List[str]]:
    """
    Returns a dict keyed by '<volNN>_<FIELD>' and '<volNN>_<FIELD>o'
    mapping to sorted lists of matching .tif file paths.
    """
    result: Dict[str, List[str]] = {}
    for vol in volumes:
        for field in fields:
            for filtered, suffix in [(True, ""), (False, "o")]:
                key = f"vol{vol}_{field}{'o' if not filtered else ''}"
                pattern = os.path.join(
                    product_dir,
                    f"{radar}_{strategy}_{vol}_*_{field}{suffix}.tif",
                )
                matches = sorted(glob.glob(pattern))
                result[key] = matches
    return result


def apply_timestamp_filter(
    files: Dict[str, List[str]],
    timestamps_arg: str,
) -> Dict[str, List[str]]:
    """
    Filter the collected files according to --timestamps.

    'latest:N'          → keep the N most recent files per group
    'ts1,ts2,...'       → keep only files whose filename contains one of the timestamps
    """
    if timestamps_arg.startswith("latest:"):
        n = int(timestamps_arg.split(":")[1])
        return {k: v[-n:] for k, v in files.items()}

    wanted = [t.strip() for t in timestamps_arg.split(",") if t.strip()]
    filtered: Dict[str, List[str]] = {}
    for key, flist in files.items():
        filtered[key] = [fp for fp in flist if any(ts in os.path.basename(fp) for ts in wanted)]
    return filtered


# ─────────────────────────────────────────────────────────────────────────────
# Metadata table
# ─────────────────────────────────────────────────────────────────────────────


def print_metadata_table(files: Dict[str, List[str]]) -> None:
    col_w = 60
    print("\n" + "=" * 100)
    print(f"  {'File':<{col_w}} {'coverage_radius_m':>18} {'valid%':>8} {'dims':>12}")
    print("=" * 100)
    for _, flist in files.items():
        for fp in flist:
            try:
                with rasterio.open(fp) as ds:
                    tags = ds.tags()
                    cov_r = tags.get("radarlib_radar_coverage_m", "N/A")
                    mask = ds.dataset_mask()
                    valid_pct = 100.0 * np.sum(mask == 255) / (ds.width * ds.height)
                    dims = f"{ds.width}×{ds.height}"
                fname = os.path.basename(fp)
                print(f"  {fname:<{col_w}} {cov_r:>18} {valid_pct:>7.1f}% {dims:>12}")
            except Exception as exc:
                print(f"  [ERROR reading {fp}]: {exc}")
    print("=" * 100)


# ─────────────────────────────────────────────────────────────────────────────
# COG → base64 PNG rendering
# ─────────────────────────────────────────────────────────────────────────────


def render_cog_to_b64(
    filepath: str,
) -> Tuple[str, List[List[float]]]:
    """
    Read a float32 COG, apply the stored colormap (or turbo fallback),
    and return (base64_png_str, [[south, west], [north, east]]).
    """
    with rasterio.open(filepath) as ds:
        data = ds.read(1)
        nodata = ds.nodata
        tags = ds.tags()
        vmin = float(tags.get("radarlib_vmin", -20))
        vmax = float(tags.get("radarlib_vmax", 70))
        cmap_name = tags.get("radarlib_cmap", "turbo")
        west, south, east, north = rasterio.warp.transform_bounds(ds.crs, WGS84, *ds.bounds)

    # Build validity mask (True = no data → transparent)
    mask = ~np.isfinite(data)
    if nodata is not None:
        mask |= data == nodata

    norm = mcolors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    try:
        cmap = plt.get_cmap(cmap_name)
    except Exception:
        cmap = plt.get_cmap("turbo")

    safe_data = np.where(mask, 0.0, data)
    rgba = cmap(norm(safe_data))
    rgba[..., 3] = np.where(mask, 0.0, 0.85)

    # rasterio row 0 = north; flip so row 0 = south for Leaflet imageOverlay
    rgba_flipped = rgba[::-1]

    buf = io.BytesIO()
    plt.imsave(buf, rgba_flipped, format="png", origin="lower")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    return b64, [[south, west], [north, east]]


# ─────────────────────────────────────────────────────────────────────────────
# Layer building
# ─────────────────────────────────────────────────────────────────────────────


def build_layers(files: Dict[str, List[str]]) -> List[dict]:
    """Render each COG and collect layer metadata + base64 image."""
    layers: List[dict] = []
    print("\nRendering COG images for Leaflet map...")

    for key, flist in files.items():
        if not flist:
            continue
        # key format: 'volNN_FIELDo' or 'volNN_FIELD'
        parts = key.split("_", 1)  # ['vol01', 'DBZHo'] or ['vol01', 'DBZH']
        vol_tag = parts[0]  # 'vol01', 'vol04', …
        field_with_suffix = parts[1]  # 'DBZHo' or 'DBZH'
        filtered = not field_with_suffix.endswith("o")

        for fp in flist:
            fname = os.path.basename(fp)
            # Extract timestamp token from filename:  RADAR_STRAT_VOL_TIMESTAMP_FIELD.tif
            name_parts = fname.replace(".tif", "").split("_")
            ts_str = name_parts[3] if len(name_parts) >= 4 else fname
            label = f"{vol_tag} {field_with_suffix} – {ts_str}"

            try:
                b64, bounds = render_cog_to_b64(fp)
                layers.append(
                    {
                        "id": f"{key}_{fname}",
                        "label": label,
                        "vol": vol_tag,
                        "field": field_with_suffix,
                        "filtered": filtered,
                        "b64": b64,
                        "bounds": bounds,
                        "ts": ts_str,
                    }
                )
                print(f"  ✓ {fname}")
            except Exception as exc:
                print(f"  ✗ {fname}: {exc}")

    return layers


# ─────────────────────────────────────────────────────────────────────────────
# HTML generation
# ─────────────────────────────────────────────────────────────────────────────


def build_html(layers: List[dict], radar: str) -> str:
    # Determine map centre from available bounds
    centre_lat, centre_lon = _guess_centre(layers, radar)

    # Volume info for the info panel (coverage radius from layer metadata or tag)
    vol_labels = _build_vol_labels(layers)
    field_names = sorted({lyr["field"] for lyr in layers})

    layers_json = json.dumps([{k: v for k, v in lyr.items() if k != "b64"} for lyr in layers])
    images_js = "\n".join(f'  "{lyr["id"]}": "data:image/png;base64,{lyr["b64"]}",' for lyr in layers)
    vol_buttons_html = _build_vol_buttons_html(layers)
    field_buttons_html = _build_field_buttons_html(field_names)

    initial_vol = layers[0]["vol"] if layers else "vol01"
    initial_field = layers[0]["field"] if layers else field_names[0] if field_names else ""

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{radar} – Radar COG Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  body {{ margin:0; font-family:Arial,sans-serif; background:#1a1a2e; color:#eee; }}
  #map {{ width:100vw; height:100vh; }}
  #controls {{
    position:absolute; top:10px; left:50%; transform:translateX(-50%);
    z-index:1000; background:rgba(20,20,40,0.92); border-radius:10px;
    padding:12px 18px; display:flex; flex-direction:column; gap:8px;
    min-width:540px; box-shadow:0 4px 24px rgba(0,0,0,0.5);
  }}
  #controls h3 {{ margin:0 0 4px; font-size:14px; text-align:center; color:#90caf9; }}
  .row {{ display:flex; gap:6px; align-items:center; justify-content:center; flex-wrap:wrap; }}
  .btn {{
    padding:6px 14px; border-radius:6px; cursor:pointer;
    border:1px solid #555; font-size:12px; white-space:nowrap; transition:all .15s;
  }}
  .vol-btn   {{ background:#1e3a5f; color:#90caf9; }}
  .vol-btn.active  {{ background:#1565c0; color:#fff; border-color:#42a5f5; }}
  .field-btn {{ background:#1b3a1b; color:#a5d6a7; }}
  .field-btn.active {{ background:#2e7d32; color:#fff; border-color:#66bb6a; }}
  .filter-btn {{ background:#3a2b10; color:#ffe082; }}
  .filter-btn.active {{ background:#f57f17; color:#fff; border-color:#ffd54f; }}
  .ts-btn  {{ background:#2a1b3d; color:#ce93d8; font-size:11px; }}
  .ts-btn.active  {{ background:#6a1b9a; color:#fff; border-color:#ab47bc; }}
  #info {{
    position:absolute; bottom:20px; left:10px; z-index:1000;
    background:rgba(20,20,40,0.88); border-radius:8px; padding:10px 14px;
    font-size:12px; max-width:360px; line-height:1.6;
  }}
  #info .cov {{ color:#ffd54f; }}
  #legend {{
    position:absolute; bottom:20px; right:10px; z-index:1000;
    background:rgba(20,20,40,0.88); border-radius:8px; padding:10px 14px; font-size:12px;
  }}
  .grad-bar {{
    width:200px; height:14px; border-radius:3px;
    background:linear-gradient(to right,
      #00008b,#0000ff,#00bfff,#00ff7f,#ffff00,#ff8c00,#ff0000,#8b0000);
    margin:4px 0;
  }}
  .grad-labels {{ display:flex; justify-content:space-between; font-size:10px; color:#bbb; }}
</style>
</head>
<body>
<div id="map"></div>

<div id="controls">
  <h3>&#x1F329;&#xFE0F;  {radar} — Radar COG Viewer</h3>
  <div class="row" id="vol-row">
    <span style="font-size:11px;color:#aaa">Volume:</span>
    {vol_buttons_html}
  </div>
  <div class="row" id="field-row">
    <span style="font-size:11px;color:#aaa">Field:</span>
    {field_buttons_html}
  </div>
  <div class="row" id="filter-row">
    <span style="font-size:11px;color:#aaa">Filter:</span>
    <button class="btn filter-btn active" data-filt="false" onclick="setFilter(false)">Unfiltered (raw)</button>
    <button class="btn filter-btn" data-filt="true" onclick="setFilter(true)">Filtered (GRC)</button>
  </div>
  <div class="row" id="ts-row"></div>
</div>

<div id="info"><div id="info-text">Select a layer above to see details.</div></div>
<div id="legend">
  <div style="color:#aaa;margin-bottom:4px">DBZH (dBZ)</div>
  <div class="grad-bar"></div>
  <div class="grad-labels"><span>−20</span><span>0</span><span>25</span><span>50</span><span>70</span></div>
</div>

<script>
// ── Data ────────────────────────────────────────────────────────────────────
const LAYERS = {layers_json};
const IMAGES = {{
{images_js}
}};
const VOL_LABELS = {json.dumps(vol_labels)};

// ── Map setup ───────────────────────────────────────────────────────────────
const map = L.map('map', {{ center: [{centre_lat:.4f}, {centre_lon:.4f}], zoom: 5 }});
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '© OpenStreetMap contributors', maxZoom: 14
}}).addTo(map);

L.circleMarker([{centre_lat:.4f}, {centre_lon:.4f}], {{
  radius: 6, color: '#fff', weight: 2, fillColor: '#f44336', fillOpacity: 1
}}).addTo(map).bindTooltip('{radar}', {{
  permanent: true, direction: 'right', offset: [8, 0], opacity: 0.9
}});

// ── State ───────────────────────────────────────────────────────────────────
let activeVol      = (LAYERS.length ? LAYERS[0].vol   : '{initial_vol}');
let activeField    = (LAYERS.length ? LAYERS[0].field : '{initial_field}');
let activeFiltered = false;
let activeTs       = null;
let activeOverlay  = null;

// ── Helpers ─────────────────────────────────────────────────────────────────
function layersFor(vol, field, filtered) {{
  return LAYERS.filter(l => l.vol === vol && l.field === field && l.filtered === filtered);
}}
function getLayer(vol, field, filtered, ts) {{
  return LAYERS.find(l => l.vol === vol && l.field === field && l.filtered === filtered && l.ts === ts);
}}

function showOverlay(layer) {{
  if (!layer) return;
  if (activeOverlay) map.removeLayer(activeOverlay);
  activeOverlay = L.imageOverlay(IMAGES[layer.id], layer.bounds, {{ opacity: 0.85 }}).addTo(map);
  const filtStr = layer.filtered ? 'Filtered (GRC)' : 'Unfiltered (raw)';
  const volInfo = VOL_LABELS[layer.vol] || layer.vol;
  document.getElementById('info-text').innerHTML =
    '<b>' + layer.label + '</b><br>' +
    '• Volume: ' + volInfo + '<br>' +
    '• Field: ' + layer.field + '<br>' +
    '• Filter: ' + filtStr;
}}

function buildTsButtons() {{
  const row = document.getElementById('ts-row');
  row.innerHTML = '<span style="font-size:11px;color:#aaa">Timestamp:</span>';
  const list = layersFor(activeVol, activeField, activeFiltered);
  if (!list.length) {{
    row.innerHTML += '<span style="color:#888;font-size:11px;">&nbsp;No files found</span>';
    return;
  }}
  list.forEach(l => {{
    const btn = document.createElement('button');
    btn.className = 'btn ts-btn' + (l.ts === activeTs ? ' active' : '');
    btn.textContent = l.ts;
    btn.onclick = () => selectTs(l.ts);
    row.appendChild(btn);
  }});
}}

function selectTs(ts) {{
  activeTs = ts;
  document.querySelectorAll('.ts-btn').forEach(b => b.classList.toggle('active', b.textContent === ts));
  showOverlay(getLayer(activeVol, activeField, activeFiltered, ts));
}}

function setVol(vol) {{
  activeVol = vol;
  document.querySelectorAll('.vol-btn').forEach(b => b.classList.toggle('active', b.dataset.vol === vol));
  const list = layersFor(vol, activeField, activeFiltered);
  activeTs = list.length ? list[list.length - 1].ts : null;
  buildTsButtons();
  if (activeTs) showOverlay(getLayer(activeVol, activeField, activeFiltered, activeTs));
}}

function setField(field) {{
  activeField = field;
  document.querySelectorAll('.field-btn').forEach(b => b.classList.toggle('active', b.dataset.field === field));
  const list = layersFor(activeVol, field, activeFiltered);
  activeTs = list.length ? list[list.length - 1].ts : null;
  buildTsButtons();
  if (activeTs) showOverlay(getLayer(activeVol, activeField, activeFiltered, activeTs));
}}

function setFilter(filtered) {{
  activeFiltered = filtered;
  document.querySelectorAll('.filter-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.filt === String(filtered)));
  buildTsButtons();
  if (activeTs) showOverlay(getLayer(activeVol, activeField, activeFiltered, activeTs));
}}

// ── Init ────────────────────────────────────────────────────────────────────
(function init() {{
  const list = layersFor(activeVol, activeField, activeFiltered);
  activeTs = list.length ? list[list.length - 1].ts : null;
  buildTsButtons();
  if (activeTs) showOverlay(getLayer(activeVol, activeField, activeFiltered, activeTs));
}})();
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
# HTML helpers
# ─────────────────────────────────────────────────────────────────────────────


def _guess_centre(layers: List[dict], radar: str) -> Tuple[float, float]:
    """Compute map centre from the bounds of the first available layer."""
    known_centres = {
        "RMA1": (-31.441, -64.191),
        "RMA11": (-31.441, -64.191),
        "RMA6": (-37.237, -59.868),
        "RMA3": (-29.901, -59.868),
    }
    for layer in layers:
        b = layer["bounds"]  # [[south,west],[north,east]]
        return (b[0][0] + b[1][0]) / 2, (b[0][1] + b[1][1]) / 2
    return known_centres.get(radar, (-31.4, -64.2))


def _build_vol_labels(layers: List[dict]) -> Dict[str, str]:
    """
    Build a dict mapping vol key → human readable label with coverage info.
    Reads coverage_radius_m from the first file of each vol group.
    """
    seen: Dict[str, str] = {}
    for layer in layers:
        vol = layer["vol"]
        if vol not in seen:
            # Try to read coverage tag from the actual COG
            seen[vol] = vol  # fallback
    return seen


def _build_vol_buttons_html(layers: List[dict]) -> str:
    vols_seen: dict = {}
    for layer in layers:
        if layer["vol"] not in vols_seen:
            vols_seen[layer["vol"]] = layer["bounds"]

    buttons = []
    for i, (vol, bounds) in enumerate(vols_seen.items()):
        south, west = bounds[0]
        north, east = bounds[1]
        km = int(round(((north - south) / 2) * 111))  # rough deg→km
        active = " active" if i == 0 else ""
        buttons.append(
            f'<button class="btn vol-btn{active}" data-vol="{vol}" '
            f"onclick=\"setVol('{vol}')\">{vol} – ~{km} km</button>"
        )
    return "\n    ".join(buttons)


def _build_field_buttons_html(fields: List[str]) -> str:
    buttons = []
    for i, field in enumerate(fields):
        # Strip trailing 'o' for display; we use the filter toggle for that
        # base = field.rstrip("o")
        active = " active" if i == 0 else ""
        # Only show base fields (no 'o' suffix) as field buttons; 'o' is filter
        if not field.endswith("o"):
            buttons.append(
                f'<button class="btn field-btn{active}" data-field="{field}" '
                f"onclick=\"setField('{field}')\">{field}</button>"
            )
    return "\n    ".join(buttons)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    args = parse_args()

    # ── Register radarlib colormaps ──────────────────────────────────────────
    if args.radarlib_src and os.path.isdir(args.radarlib_src):
        sys.path.insert(0, args.radarlib_src)
    try:
        from radarlib.colormaps import register_colormaps  # type: ignore

        register_colormaps()
        print("[INFO] radarlib colormaps registered.")
    except Exception as e:
        print(f"[WARN] Could not register radarlib colormaps: {e}. Falling back to turbo.")

    # ── Resolve date ─────────────────────────────────────────────────────────
    if args.date:
        date_path = args.date
    else:
        today = datetime.datetime.now(datetime.timezone.utc)
        date_path = today.strftime("%Y/%m/%d")

    product_dir = os.path.join(args.product_dir, args.radar, date_path)
    if not os.path.isdir(product_dir):
        sys.exit(f"ERROR: Product directory not found: {product_dir}")

    volumes = [v.strip().zfill(2) for v in args.volumes.split(",") if v.strip()]
    fields = [f.strip() for f in args.fields.split(",") if f.strip()]

    print(f"\nRadar    : {args.radar}")
    print(f"Strategy : {args.strategy}")
    print(f"Volumes  : {volumes}")
    print(f"Fields   : {fields}")
    print(f"Date dir : {product_dir}")

    # ── Collect + filter files ───────────────────────────────────────────────
    raw_files = collect_files(product_dir, args.radar, args.strategy, volumes, fields)
    files = apply_timestamp_filter(raw_files, args.timestamps)

    any_found = any(flist for flist in files.values())
    if not any_found:
        sys.exit("ERROR: No COG files matched the given parameters.")

    # ── Metadata table ───────────────────────────────────────────────────────
    if not args.no_meta:
        print_metadata_table(files)

    # ── Render and build HTML ────────────────────────────────────────────────
    layers = build_layers(files)
    if not layers:
        sys.exit("ERROR: No layers could be rendered.")

    html = build_html(layers, args.radar)
    out_path = os.path.expanduser(args.output)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as fh:
        fh.write(html)

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"\n✓ HTML map written to: {out_path}  ({size_mb:.1f} MB)")

    if args.open:
        import time
        import webbrowser

        # Small delay so the file is fully flushed
        time.sleep(0.3)
        webbrowser.open(f"file://{os.path.abspath(out_path)}")
        print("  → Opened in browser.")


if __name__ == "__main__":
    main()
