#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
plot_bufr_pyart.py — Quick PyART PPI plotter for a single BUFR file.

Decodes one BUFR file directly to a PyART Radar object and plots a PPI
using PyART's built-in RadarDisplay.  No radar_grid interpolation is
involved — this is the fastest way to verify that a BUFR file contains
real data and what fields it holds.

The input path can be:
  • A full FTP remote path  (e.g. /L2/RMA2/2026/05/01/…/file.BUFR)
  • A local file path       (e.g. /tmp/RMA2_0315_01_DBZH_20260501T113521Z.BUFR)

When an FTP path is given the script downloads the file to a temporary
directory, decodes it, plots it, and cleans up.

Usage:
    python3 scripts/plot_bufr_pyart.py \\
        --path /L2/RMA2/2026/05/01/11/3521/RMA2_0315_01_DBZH_20260501T113521Z.BUFR \\
        --field DBZH

    python3 scripts/plot_bufr_pyart.py \\
        --path /tmp/RMA2_0315_01_DBZH_20260501T113521Z.BUFR \\
        --field DBZH --local

    python3 scripts/plot_bufr_pyart.py \\
        --path /L2/RMA2/… \\
        --field DBZH --sweep 1 --output-dir /tmp/plots

Docker execution (save to container /tmp/bufr_plots):
    docker exec genpro25-rma2 python3 /workspace/scripts/plot_bufr_pyart.py \\
        --path /L2/RMA2/2026/05/01/11/3521/RMA2_0315_01_DBZH_20260501T113521Z.BUFR \\
        --field DBZH

Docker execution (output to host via base64):
    docker exec genpro25-rma2 python3 /workspace/scripts/plot_bufr_pyart.py \\
        --path /L2/RMA2/2026/05/01/11/3521/RMA2_0315_01_DBZH_20260501T113521Z.BUFR \\
        --field DBZH --to-stdout | base64 -d > output.png
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use("Agg")  # headless-safe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("plot_bufr_pyart")


# ---------------------------------------------------------------------------
# Credential resolution — identical to check_rma_bufr_ftp.py
# ---------------------------------------------------------------------------


def resolve_ftp_credentials() -> Tuple[str, str, str]:
    """Resolve FTP credentials from app.config or environment variables."""
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
        import config as app_config

        host = getattr(app_config, "FTP_HOST", None)
        user = getattr(app_config, "FTP_USER", None)
        password = getattr(app_config, "FTP_PASS", None)

        if host and user and password:
            logger.info(f"Loaded FTP credentials from app.config: {host}")
            return host, user, password
    except Exception as e:
        logger.debug(f"Could not load from app.config: {e}")

    host = os.environ.get("FTP_HOST")
    user = os.environ.get("FTP_USER")
    password = os.environ.get("FTP_PASS")

    if not host or not user or not password:
        raise RuntimeError(
            "FTP credentials not found. Set FTP_HOST, FTP_USER, FTP_PASS "
            "environment variables or ensure app/config.py is available."
        )

    logger.info(f"Loaded FTP credentials from environment: {host}")
    return host, user, password


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decode a BUFR file to PyART and plot a PPI directly (no grid interpolation)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # FTP path (auto-downloaded):
  python3 scripts/plot_bufr_pyart.py \\
    --path /L2/RMA2/2026/05/01/11/3521/RMA2_0315_01_DBZH_20260501T113521Z.BUFR \\
    --field DBZH

  # Local file:
  python3 scripts/plot_bufr_pyart.py \\
    --path /tmp/RMA2_0315_01_DBZH_20260501T113521Z.BUFR \\
    --field DBZH --local

  # Plot sweep 1 with custom range ring:
  python3 scripts/plot_bufr_pyart.py \\
    --path /L2/RMA2/… --field DBZH --sweep 1 --max-range 200
        """,
    )
    parser.add_argument(
        "--path",
        required=True,
        help="FTP remote path OR local file path to the .BUFR file",
    )
    parser.add_argument(
        "--field",
        default="DBZH",
        help="Radar field to plot (default: DBZH)",
    )
    parser.add_argument(
        "--sweep",
        type=int,
        default=0,
        help="Sweep index to plot (default: 0)",
    )
    parser.add_argument(
        "--max-range",
        type=float,
        default=None,
        help="Max range ring radius in km (default: auto from radar)",
    )
    parser.add_argument(
        "--vmin",
        type=float,
        default=None,
        help="Colorscale min (default: auto per field)",
    )
    parser.add_argument(
        "--vmax",
        type=float,
        default=None,
        help="Colorscale max (default: auto per field)",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Treat --path as a local file (skip FTP download)",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/bufr_plots",
        help="Directory to save the PNG (default: /tmp/bufr_plots)",
    )
    parser.add_argument(
        "--to-stdout",
        action="store_true",
        help="Output PNG as base64 to stdout instead of file (useful for Docker; pipe to 'base64 -d > output.png')",
    )
    parser.add_argument(
        "--save-netcdf",
        default=None,
        metavar="PATH",
        help="Also save the decoded PyART radar as CFRadial NetCDF to this path.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING"],
        help="Log verbosity (default: INFO)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Field defaults
# ---------------------------------------------------------------------------

_FIELD_DEFAULTS = {
    "DBZH": (-5.0, 75.0, "gist_ncar"),
    "ZDR": (-2.0, 6.0, "RdYlBu_r"),
    "RHOHV": (0.6, 1.05, "plasma"),
    "KDP": (-1.0, 6.0, "RdYlBu_r"),
    "VRADH": (-30.0, 30.0, "RdBu_r"),
    "WRADH": (0.0, 10.0, "viridis"),
    "PHIDP": (0.0, 360.0, "hsv"),
}

_DEFAULT_VMIN_VMAX_CMAP = (-10.0, 75.0, "viridis")


def field_display_params(field: str, vmin_arg: Optional[float], vmax_arg: Optional[float]):
    """Return (vmin, vmax, cmap) for the given field name."""
    defaults = _FIELD_DEFAULTS.get(field.upper(), _DEFAULT_VMIN_VMAX_CMAP)
    vmin = vmin_arg if vmin_arg is not None else defaults[0]
    vmax = vmax_arg if vmax_arg is not None else defaults[1]
    cmap = defaults[2]

    # Prefer grc_ colormaps if registered
    try:
        from radarlib.colormaps import REGISTERED_COLORMAP_NAMES

        grc_cand = f"grc_{field.lower()}"
        if grc_cand in REGISTERED_COLORMAP_NAMES:
            cmap = grc_cand
        elif "grc_ref" in REGISTERED_COLORMAP_NAMES and field.upper() == "DBZH":
            cmap = "grc_ref"
    except Exception:
        pass

    return vmin, vmax, cmap


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = Path(args.path).name
    local_bufr: Optional[Path] = None
    _tmp_dir_obj = None  # keep reference so it is not garbage-collected

    try:
        if args.local:
            local_path = Path(args.path)
            if not local_path.exists():
                print(f"✗ Local file not found: {local_path}", file=sys.stderr)
                sys.exit(1)
            logger.info(f"Using local file: {local_path}")
            local_bufr = local_path
        else:
            # Download from FTP into a temp subdir that preserves original filename
            try:
                ftp_host, ftp_user, ftp_pass = resolve_ftp_credentials()
            except RuntimeError as e:
                print(f"✗ {e}", file=sys.stderr)
                sys.exit(1)

            _tmp_dir_obj = tempfile.TemporaryDirectory(prefix="plot_bufr_")
            tmp_dir = Path(_tmp_dir_obj.name) / "bufr"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            local_bufr = tmp_dir / filename

            from radarlib.io.ftp.ftp_client import RadarFTPClient

            with RadarFTPClient(host=ftp_host, user=ftp_user, password=ftp_pass) as client:
                logger.info(f"Downloading: {args.path}")
                client.download_file(str(args.path), local_bufr)
                logger.info(f"Downloaded: {filename}")

        # ------------------------------------------------------------------
        # Decode to PyART
        # ------------------------------------------------------------------
        from radarlib.io.bufr.bufr_to_pyart import bufr_paths_to_pyart

        logger.info(f"Decoding {filename} to PyART ...")
        radar = bufr_paths_to_pyart(
            [str(local_bufr)],
            root_resources=None,
            root_scan_config_files=None,
        )

        logger.info(f"PyART radar: ngates={radar.ngates}, nrays={radar.nrays}, " f"nsweeps={radar.nsweeps}")
        logger.info(f"Available fields: {list(radar.fields.keys())}")

        if args.save_netcdf:
            import pyart as _pyart

            nc_path = Path(args.save_netcdf)
            nc_path.parent.mkdir(parents=True, exist_ok=True)
            _pyart.io.write_cfradial(str(nc_path), radar)
            logger.info(f"Saved NetCDF: {nc_path}")
            print(f"NetCDF  : {nc_path}")

        # Validate sweep index
        if args.sweep >= radar.nsweeps:
            print(
                f"✗ --sweep {args.sweep} is out of range. "
                f"This radar has {radar.nsweeps} sweep(s) (0–{radar.nsweeps - 1}).",
                file=sys.stderr,
            )
            sys.exit(1)

        # Validate field
        if args.field not in radar.fields:
            print(
                f"✗ Field '{args.field}' not found. " f"Available: {list(radar.fields.keys())}",
                file=sys.stderr,
            )
            sys.exit(1)

        # Field data stats
        fdata = np.ma.masked_invalid(radar.fields[args.field]["data"])
        total = fdata.size
        valid = int(np.ma.count(fdata))
        pct = 100.0 * valid / total if total > 0 else 0.0
        logger.info(f"Field '{args.field}': total gates={total:,}, " f"valid={valid:,} ({pct:.1f}%)")
        if pct < 1.0:
            logger.warning(
                f"Only {pct:.1f}% of gates are valid — this may be a clear-sky scan. "
                f"The PPI will appear mostly blank."
            )

        # Sweep elevation
        elev = float(radar.fixed_angle["data"][args.sweep])
        logger.info(f"Plotting sweep {args.sweep} at elevation {elev:.1f}°")

        # Display params
        vmin, vmax, cmap = field_display_params(args.field, args.vmin, args.vmax)
        logger.info(f"Color scale: vmin={vmin}, vmax={vmax}, cmap={cmap}")

        # ------------------------------------------------------------------
        # Plot with PyART RadarDisplay
        # ------------------------------------------------------------------
        import pyart

        display = pyart.graph.RadarDisplay(radar)

        fig, ax = plt.subplots(1, 1, figsize=(9, 8))

        display.plot(
            args.field,
            sweep=args.sweep,
            ax=ax,
            vmin=vmin,
            vmax=vmax,
            cmap=cmap,
            colorbar_label=args.field,
            title_flag=False,  # we set our own title below
            axislabels_flag=True,
        )

        if args.max_range is not None:
            display.plot_range_ring(args.max_range, ax=ax, lw=0.8, col="white")
        else:
            # Draw a range ring at ~80% of max range for reference
            try:
                r_max_km = float(radar.range["data"][-1]) / 1e3
                display.plot_range_ring(round(r_max_km * 0.8 / 25) * 25, ax=ax, lw=0.8, col="white")
            except Exception:
                pass

        display.plot_cross_hair(0.5, ax=ax)

        # Timestamp from filename if parseable, else from radar metadata
        ts_str = ""
        try:
            from radarlib.io.bufr.bufr import BUFRFilename

            parsed = BUFRFilename(filename)
            ts_str = parsed.datetime.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            try:
                ts_str = radar.time["units"].replace("seconds since ", "")
            except Exception:
                ts_str = filename

        ax.set_title(
            f"{filename}\n" f"Field: {args.field}  |  Sweep {args.sweep}  |  Elev {elev:.1f}°  |  {ts_str}",
            fontsize=10,
        )

        ax.set_aspect("equal")

        fig.tight_layout()

        stem = filename.replace(".BUFR", "").replace(".bufr", "")
        out_filename = f"{stem}_sw{args.sweep:02d}_{args.field}.png"

        if args.to_stdout:
            # Output PNG as base64 to stdout (useful for Docker exec)
            import base64
            import io

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
            buf.seek(0)
            png_bytes = buf.read()
            plt.close(fig)

            # Print metadata as JSON on stderr, base64 PNG on stdout
            import json

            info = {
                "filename": out_filename,
                "file": filename,
                "field": args.field,
                "sweep": args.sweep,
                "elevation": elev,
                "gates_valid": valid,
                "gates_total": total,
            }
            print(json.dumps(info), file=sys.stderr)

            # Output base64 PNG to stdout
            b64_png = base64.b64encode(png_bytes).decode("ascii")
            print(b64_png)

            logger.info(f"PNG ({len(png_bytes):,} bytes) output to stdout as base64")
        else:
            out_path = output_dir / out_filename
            fig.savefig(str(out_path), dpi=120, bbox_inches="tight")
            plt.close(fig)

            logger.info(f"Figure saved: {out_path}")

            # Summary to stdout
            print(f"File    : {filename}")
            print(f"Field   : {args.field}")
            print(f"Sweep   : {args.sweep}  (elev {elev:.1f}°)")
            print(f"Gates   : {valid:,}/{total:,} valid ({pct:.1f}%)")
            print(f"Figure  : {out_path}")

    finally:
        # TemporaryDirectory cleans itself up when _tmp_dir_obj is deleted / GC'd.
        # Explicitly clean up here for clarity.
        if _tmp_dir_obj is not None:
            try:
                _tmp_dir_obj.cleanup()
            except Exception:
                pass


if __name__ == "__main__":
    main()
