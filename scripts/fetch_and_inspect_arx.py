#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_and_inspect_arx.py — Download the most recent BUFR file for a given radar
and run a full structural inspection on it.

Designed for evaluating new radar types (e.g., ARX) to determine whether
the BUFR decoder can handle them and what geometry they expose.

Steps performed:
  1. Find the most recent BUFR file on FTP for the given radar
  2. Download it to --output-dir (default: ./arx_samples/)
  3. Run inspect_bufr_scan.py on it (fields, sweeps, geometry, etc.)
  4. Print the FTP path so it can be used with get_bufr_geometry.py later

Credential resolution order (first match wins):
  1. --host / --user / --password CLI arguments
  2. app/config.py  (FTP_HOST, FTP_USER, FTP_PASS)
  3. Environment variables FTP_HOST, FTP_USER, FTP_PASS

Usage:
    python3 scripts/fetch_and_inspect_arx.py --radar AR5
    python3 scripts/fetch_and_inspect_arx.py --radar AR5 --vol 1
    python3 scripts/fetch_and_inspect_arx.py --radar AR8 --output-dir /tmp/arx

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/fetch_and_inspect_arx.py --radar AR5
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("fetch_and_inspect_arx")

SEP = "=" * 72


# ---------------------------------------------------------------------------
# Credential resolution  (same pattern as other scripts in this repo)
# ---------------------------------------------------------------------------


def resolve_credentials(
    host: Optional[str], user: Optional[str], password: Optional[str]
) -> Tuple[str, str, str]:
    if host and user and password:
        return host, user, password

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
        import config as app_config  # type: ignore

        h = getattr(app_config, "FTP_HOST", None)
        u = getattr(app_config, "FTP_USER", None)
        p = getattr(app_config, "FTP_PASS", None)
        if h and u and p:
            return h, u, p
    except Exception as e:
        logger.debug(f"Could not load app/config.py: {e}")

    h = os.environ.get("FTP_HOST") or host
    u = os.environ.get("FTP_USER") or user
    p = os.environ.get("FTP_PASS") or password

    if not h or not u or not p:
        raise RuntimeError(
            "FTP credentials not found.\n"
            "  Provide --host / --user / --password, or set FTP_HOST / FTP_USER / FTP_PASS."
        )
    return h, u, p  # type: ignore


# ---------------------------------------------------------------------------
# FTP: find most recent file for radar (optionally filtered by volume number)
# ---------------------------------------------------------------------------


def find_latest_file(
    client,
    radar: str,
    vol_filter: Optional[str] = None,
) -> Optional[Tuple[str, str]]:
    """
    Walk /L2/{radar}/YYYY/MM/DD/HH/MMSS/ newest-first and return
    (ftp_path, filename) for the first BUFR file found.

    vol_filter: if given (e.g. "1"), only return files whose volume
                component in the filename matches.
    """
    base = f"/L2/{radar}"

    def latest(entries: List[str]) -> List[str]:
        return sorted(entries, reverse=True)

    try:
        for y in latest(client.list_dir(base)):
            for m in latest(client.list_dir(f"{base}/{y}")):
                for d in latest(client.list_dir(f"{base}/{y}/{m}")):
                    for h in latest(client.list_dir(f"{base}/{y}/{m}/{d}")):
                        for ms in latest(client.list_dir(f"{base}/{y}/{m}/{d}/{h}")):
                            minute_path = f"{base}/{y}/{m}/{d}/{h}/{ms}"
                            files = [
                                f for f in client.list_dir(minute_path)
                                if f.upper().endswith(".BUFR")
                            ]
                            for fname in sorted(files, reverse=True):
                                if vol_filter:
                                    # Filename: {radar}_{strategy}_{vol}_{field}_{ts}.BUFR
                                    parts = fname.split("_")
                                    if len(parts) >= 3 and parts[2] != vol_filter:
                                        continue
                                ftp_path = f"{minute_path}/{fname}"
                                return ftp_path, fname
    except Exception as e:
        logger.debug(f"find_latest_file({radar}): {e}")

    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and inspect the most recent BUFR file for a radar.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--radar", required=True, help="Radar folder name (e.g. AR5, AR8)")
    parser.add_argument(
        "--vol",
        default=None,
        metavar="N",
        help="Optional: filter to a specific volume number (e.g. 1, 3)",
    )
    parser.add_argument(
        "--output-dir",
        default="./arx_samples",
        help="Local directory to save downloaded BUFR file (default: ./arx_samples/)",
    )
    parser.add_argument("--host", default=None, help="FTP host")
    parser.add_argument("--user", default=None, help="FTP username")
    parser.add_argument("--password", default=None, help="FTP password")
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    try:
        host, user, password = resolve_credentials(args.host, args.user, args.password)
    except RuntimeError as e:
        print(f"\n✗ {e}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from radarlib.io.ftp.ftp_client import RadarFTPClient

    print(f"\n{SEP}")
    print(f"  Radar: {args.radar}  |  FTP: {host}")
    print(SEP)

    with RadarFTPClient(host=host, user=user, password=password) as client:

        # --- Step 1: find most recent file ---
        print(f"\n[1/3] Searching for most recent BUFR file under /L2/{args.radar}/ ...")
        if args.vol:
            print(f"      Volume filter: vol={args.vol}")

        result = find_latest_file(client, args.radar, vol_filter=args.vol)
        if result is None:
            print(f"\n✗ No BUFR files found for radar '{args.radar}'")
            if args.vol:
                print(f"  (with vol filter '{args.vol}' — try without --vol)")
            sys.exit(1)

        ftp_path, fname = result
        local_path = output_dir / fname

        print(f"\n  Found : {ftp_path}")
        print(f"  Save  : {local_path}")

        # --- Step 2: download ---
        print(f"\n[2/3] Downloading ...")
        try:
            client.download_file(ftp_path, local_path)
            size_kb = local_path.stat().st_size / 1024
            print(f"  ✓ Downloaded ({size_kb:.1f} KB)")
        except Exception as e:
            print(f"\n✗ Download failed: {e}", file=sys.stderr)
            sys.exit(1)

    # --- Step 3: inspect (runs outside FTP context) ---
    print(f"\n[3/3] Structural inspection:")
    print(SEP)

    try:
        # Reuse the existing inspect logic directly
        scripts_dir = Path(__file__).parent
        sys.path.insert(0, str(scripts_dir))
        from inspect_bufr_scan import inspect  # type: ignore

        inspect(str(local_path))
    except Exception as e:
        print(f"\n✗ Inspection failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        print(
            f"\n  The file is saved at: {local_path}\n"
            f"  You can inspect it manually with:\n"
            f"    python3 scripts/inspect_bufr_scan.py {local_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\n{SEP}")
    print(f"  File saved at: {local_path}")
    print(f"  FTP path     : {ftp_path}")
    print(f"\n  To re-inspect later:")
    print(f"    python3 scripts/inspect_bufr_scan.py {local_path}")
    print(f"\n  To get geometry detail:")
    print(f"    python3 scripts/get_bufr_geometry.py {local_path}")
    print(SEP + "\n")


if __name__ == "__main__":
    main()
