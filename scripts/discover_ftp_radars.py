#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
discover_ftp_radars.py — List all radar folders on the FTP server and peek at
the most recent BUFR file in each one.

Useful as the first step when evaluating a new radar type (e.g., ARX) to check:
  - What folder names exist under /L2/
  - Whether the naming convention matches the expected pattern
  - What strategy, volume, and field codes are being used

Credential resolution order (first match wins):
  1. --host / --user / --password CLI arguments
  2. app/config.py  (FTP_HOST, FTP_USER, FTP_PASS)
  3. Environment variables FTP_HOST, FTP_USER, FTP_PASS

Usage:
    # From inside a container (credentials already in app/config.py):
    python3 scripts/discover_ftp_radars.py

    # From outside, passing creds explicitly:
    python3 scripts/discover_ftp_radars.py --host ftp.example.com --user USER --password PASS

    # Filter to a specific folder prefix:
    python3 scripts/discover_ftp_radars.py --filter AR

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/discover_ftp_radars.py
    docker exec genpro25-rma1 python3 /workspace/scripts/discover_ftp_radars.py --filter AR
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
logger = logging.getLogger("discover_ftp_radars")

SEP = "=" * 72
SUB = "-" * 72


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------


def resolve_credentials(
    host: Optional[str], user: Optional[str], password: Optional[str]
) -> Tuple[str, str, str]:
    """Resolve FTP credentials: CLI args > app/config.py > env vars."""

    if host and user and password:
        logger.debug("Using credentials from CLI arguments")
        return host, user, password

    # Try app/config.py
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
        import config as app_config  # type: ignore

        h = getattr(app_config, "FTP_HOST", None)
        u = getattr(app_config, "FTP_USER", None)
        p = getattr(app_config, "FTP_PASS", None)
        if h and u and p:
            logger.debug(f"Using credentials from app/config.py (host={h})")
            return h, u, p
    except Exception as e:
        logger.debug(f"Could not load app/config.py: {e}")

    # Fall back to env vars
    h = os.environ.get("FTP_HOST") or host
    u = os.environ.get("FTP_USER") or user
    p = os.environ.get("FTP_PASS") or password

    if not h or not u or not p:
        raise RuntimeError(
            "FTP credentials not found.\n"
            "  Provide --host / --user / --password, or set FTP_HOST / FTP_USER / FTP_PASS,\n"
            "  or run from inside a container where app/config.py is available."
        )

    logger.debug(f"Using credentials from environment variables (host={h})")
    return h, u, p  # type: ignore


# ---------------------------------------------------------------------------
# FTP helpers
# ---------------------------------------------------------------------------


def _peek_latest_file(client, folder: str) -> Optional[str]:
    """
    Walk the YYYY/MM/DD/HH/MMSS tree under /L2/{folder} and return the
    filename of the most recent BUFR file found (or None).

    Traverses at most one level deep at each step to keep it fast.
    """
    base = f"/L2/{folder}"

    def latest(entries: List[str]) -> Optional[str]:
        s = sorted(entries, reverse=True)
        return s[0] if s else None

    try:
        years = client.list_dir(base)
        y = latest(years)
        if not y:
            return None
        months = client.list_dir(f"{base}/{y}")
        m = latest(months)
        if not m:
            return None
        days = client.list_dir(f"{base}/{y}/{m}")
        d = latest(days)
        if not d:
            return None
        hours = client.list_dir(f"{base}/{y}/{m}/{d}")
        h = latest(hours)
        if not h:
            return None
        minutes = client.list_dir(f"{base}/{y}/{m}/{d}/{h}")
        ms = latest(minutes)
        if not ms:
            return None
        files = client.list_dir(f"{base}/{y}/{m}/{d}/{h}/{ms}")
        bufr_files = [f for f in files if f.upper().endswith(".BUFR")]
        return sorted(bufr_files, reverse=True)[0] if bufr_files else None
    except Exception as e:
        logger.debug(f"peek_latest_file({folder}): {e}")
        return None


def _count_years(client, folder: str) -> int:
    try:
        return len(client.list_dir(f"/L2/{folder}"))
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List all radar folders on the FTP server and peek at the most recent BUFR file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--host", default=None, help="FTP host")
    parser.add_argument("--user", default=None, help="FTP username")
    parser.add_argument("--password", default=None, help="FTP password")
    parser.add_argument(
        "--filter",
        default=None,
        metavar="PREFIX",
        help="Only show folders whose name starts with PREFIX (e.g. AR, RMA)",
    )
    parser.add_argument(
        "--no-peek",
        action="store_true",
        help="Skip fetching the most recent file (faster, less informative)",
    )
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

    print(f"\n{SEP}")
    print(f"  FTP Radar Discovery — {host}")
    print(SEP)

    # Use existing RadarFTPClient so connection/retry logic is shared
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from radarlib.io.ftp.ftp_client import RadarFTPClient

    with RadarFTPClient(host=host, user=user, password=password) as client:

        # List /L2/ root
        try:
            all_folders = sorted(client.list_dir("/L2"))
        except Exception as e:
            print(f"\n✗ Cannot list /L2/: {e}", file=sys.stderr)
            sys.exit(1)

        if args.filter:
            folders = [f for f in all_folders if f.upper().startswith(args.filter.upper())]
            print(f"\n  Filter: '{args.filter}' → {len(folders)} of {len(all_folders)} folders shown\n")
        else:
            folders = all_folders
            print(f"\n  {len(folders)} radar folder(s) found under /L2/\n")

        if not folders:
            print("  (no folders match filter)")
            return

        # Header
        col_name = 14
        col_years = 7
        col_file = 52
        print(
            f"  {'FOLDER':<{col_name}}  {'YEARS':>{col_years}}  {'MOST RECENT FILE':<{col_file}}"
        )
        print(f"  {SUB}")

        for folder in folders:
            n_years = _count_years(client, folder)
            years_str = str(n_years) if n_years > 0 else "—"

            if args.no_peek:
                latest_file = "(skipped)"
            else:
                latest_file = _peek_latest_file(client, folder) or "(no BUFR files found)"

            print(f"  {folder:<{col_name}}  {years_str:>{col_years}}  {latest_file:<{col_file}}")

        print(f"\n{SEP}\n")

        # Summary: flag folders that don't match the known RMA pattern
        unknown = [f for f in folders if not f.upper().startswith("RMA")]
        if unknown:
            print("  Non-RMA folders detected:")
            for f in unknown:
                print(f"    → {f}")
            print()
        else:
            print("  All folders match the RMA pattern — no ARX or other radars found yet.\n")


if __name__ == "__main__":
    main()
