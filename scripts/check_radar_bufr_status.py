#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_radar_bufr_status.py — FTP BUFR file status checker

Connects to the FTP server, discovers BUFR files for a given radar within a
lookback window, and prints a structured table (or JSON) of the most recent
N files per (strategy, vol, field) combination.

No BUFR files are downloaded — only FTP directory traversal and SIZE commands.

Usage:
    # All combinations found in the last 24 h, most recent 1 per combination:
    python3 scripts/check_radar_bufr_status.py --radar RMA2

    # Filter to a specific strategy/vol/field:
    python3 scripts/check_radar_bufr_status.py \\
        --radar RMA2 --strategy 0315 --vol 01 --field DBZH --last-n 3

    # JSON output for shell parsing:
    python3 scripts/check_radar_bufr_status.py \\
        --radar RMA2 --output-format json --lookback-days 0.5

Docker execution:
    docker exec genpro25-rma2 python3 /workspace/scripts/check_radar_bufr_status.py \\
        --radar RMA2 \\
        --lookback-days 1 \\
        --last-n 1 \\
        --output-format table
"""

import argparse
import ftplib
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("check_radar_bufr_status")

# ---------------------------------------------------------------------------
# FTP size threshold
# ---------------------------------------------------------------------------
SIZE_OK_THRESHOLD_BYTES = 10_240  # >10 KB = OK


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
# FTP SIZE helper
# ---------------------------------------------------------------------------


def get_ftp_size(ftp: ftplib.FTP, remote_path: str) -> Optional[int]:
    """Return file size in bytes using the FTP SIZE command, or None on failure."""
    try:
        response = ftp.sendcmd(f"SIZE {remote_path}")
        if response.startswith("213"):
            return int(response.split()[-1])
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Human-readable size
# ---------------------------------------------------------------------------


def human_size(size_bytes: Optional[int]) -> str:
    if size_bytes is None:
        return "—"
    if size_bytes >= 1_048_576:
        return f"{size_bytes / 1_048_576:.1f} MB"
    if size_bytes >= 1_024:
        return f"{size_bytes / 1_024:.1f} KB"
    return f"{size_bytes} B"


def size_status(size_bytes: Optional[int]) -> str:
    if size_bytes is None:
        return "UNKNOWN"
    return "OK" if size_bytes > SIZE_OK_THRESHOLD_BYTES else "SMALL"


# ---------------------------------------------------------------------------
# Result record
# ---------------------------------------------------------------------------


class FileRecord:
    __slots__ = (
        "radar",
        "strategy",
        "vol",
        "field",
        "obs_dt",
        "remote_path",
        "size_bytes",
    )

    def __init__(
        self,
        radar: str,
        strategy: str,
        vol: str,
        field: str,
        obs_dt: datetime,
        remote_path: str,
        size_bytes: Optional[int],
    ):
        self.radar = radar
        self.strategy = strategy
        self.vol = vol
        self.field = field
        self.obs_dt = obs_dt
        self.remote_path = remote_path
        self.size_bytes = size_bytes

    def to_dict(self) -> dict:
        return {
            "radar": self.radar,
            "strategy": self.strategy,
            "vol": self.vol,
            "field": self.field,
            "obs_timestamp": self.obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "remote_path": self.remote_path,
            "size_bytes": self.size_bytes,
            "size_human": human_size(self.size_bytes),
            "size_status": size_status(self.size_bytes),
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check FTP BUFR file availability and sizes for a radar",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # All combinations, last 24 h:
  python3 scripts/check_radar_bufr_status.py --radar RMA2

  # Specific strategy/vol/field, last 3 files:
  python3 scripts/check_radar_bufr_status.py \\
    --radar RMA2 --strategy 0315 --vol 01 --field DBZH --last-n 3

  # JSON output:
  python3 scripts/check_radar_bufr_status.py \\
    --radar RMA2 --output-format json --lookback-days 0.5

  # Docker:
  docker exec genpro25-rma2 python3 /workspace/scripts/check_radar_bufr_status.py \\
    --radar RMA2 --lookback-days 1 --last-n 1 --output-format table
        """,
    )
    parser.add_argument(
        "--radar",
        required=True,
        help="Radar code(s), comma-separated (e.g. RMA2 or RMA2,RMA3,RMA6)",
    )
    parser.add_argument(
        "--strategy",
        default=None,
        help="Filter by strategy, comma-separated (e.g. 0315 or 0315,0316)",
    )
    parser.add_argument(
        "--vol",
        default=None,
        help="Filter by volume number, comma-separated (e.g. 01 or 01,02)",
    )
    parser.add_argument(
        "--field",
        default=None,
        help="Filter by field name, comma-separated (e.g. DBZH or DBZH,ZDR,RHOHV)",
    )
    parser.add_argument(
        "--last-n",
        type=int,
        default=1,
        metavar="N",
        help="Number of most recent files to report per (strategy,vol,field). Default: 1",
    )
    parser.add_argument(
        "--lookback-days",
        type=float,
        default=1.0,
        metavar="DAYS",
        help="How many days back to search. Supports fractions (e.g. 0.5 = 12h). Default: 1.0",
    )
    parser.add_argument(
        "--output-format",
        choices=["table", "json"],
        default="table",
        help="Output format: 'table' (human-readable) or 'json' (machine-readable). Default: table",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING"],
        help="Log verbosity (default: WARNING)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Build vol_types dict for traverse_radar
# ---------------------------------------------------------------------------


def build_vol_types(
    strategies: Optional[List[str]],
    vols: Optional[List[str]],
    fields: Optional[List[str]],
) -> Optional[dict]:
    """
    Construct the vol_types dict consumed by RadarFTPClient.traverse_radar().

    Accepts lists of strategies/vols/fields and builds a combined filter dict.

    vol_types controls filename filtering inside traversal:
      {strategy: {vol: [field, ...]}} → exact match
      {strategy: {vol: []}}           → any field for that vol
      {strategy: {}}                  → any vol/field for that strategy
      None                            → no filtering (return everything)
    """
    if not strategies:
        return None  # discovery mode — let everything through
    result: dict = {}
    for strategy in strategies:
        if not vols:
            result[strategy] = {}
        else:
            result[strategy] = {}
            for vol in vols:
                if not fields:
                    result[strategy][vol] = []
                else:
                    result[strategy][vol] = list(fields)
    return result


# ---------------------------------------------------------------------------
# Output: table
# ---------------------------------------------------------------------------


def _col(text: str, width: int, align: str = "<") -> str:
    """Format a column cell, truncating if necessary."""
    text = str(text)
    if len(text) > width:
        text = text[: width - 1] + "…"
    return f"{text:{align}{width}}"


def print_table(records: List[FileRecord]) -> None:
    if not records:
        print("No files found.")
        return

    # Group by (radar, strategy, vol)
    groups: Dict[Tuple[str, str, str], List[FileRecord]] = defaultdict(list)
    for r in records:
        groups[(r.radar, r.strategy, r.vol)].append(r)

    # Sort groups
    for key in sorted(groups.keys()):
        radar, strategy, vol = key
        # Sort: field ascending, then obs_dt descending within each field
        final: List[FileRecord] = sorted(
            groups[key],
            key=lambda r: (r.field, -r.obs_dt.timestamp()),
        )

        radar = final[0].radar
        print()
        print(f"=== {radar} | strategy={strategy} | vol={vol} ===")

        # Column widths
        W_FIELD = 10
        W_TS = 22
        W_SIZE = 12
        W_STAT = 9

        header = (
            _col("FIELD", W_FIELD)
            + "  "
            + _col("TIMESTAMP (UTC)", W_TS)
            + "  "
            + _col("SIZE", W_SIZE, ">")
            + "  "
            + _col("SIZE_STATUS", W_STAT)
            + "  "
            + "REMOTE_PATH"
        )
        print(header)
        print("-" * len(header))

        for r in final:
            ts_str = r.obs_dt.strftime("%Y-%m-%d %H:%M:%S")
            print(
                _col(r.field, W_FIELD)
                + "  "
                + _col(ts_str, W_TS)
                + "  "
                + _col(human_size(r.size_bytes), W_SIZE, ">")
                + "  "
                + _col(size_status(r.size_bytes), W_STAT)
                + "  "
                + r.remote_path
            )

    print()


# ---------------------------------------------------------------------------
# Output: JSON
# ---------------------------------------------------------------------------


def print_json(records: List[FileRecord]) -> None:
    data = [r.to_dict() for r in records]
    print(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    if args.last_n < 1:
        print("--last-n must be a positive integer.", file=sys.stderr)
        sys.exit(1)

    # Parse comma-separated list arguments
    radars: List[str] = [r.strip() for r in args.radar.split(",") if r.strip()]
    strategies: Optional[List[str]] = (
        [s.strip() for s in args.strategy.split(",") if s.strip()] if args.strategy else None
    )
    vols: Optional[List[str]] = [v.strip().zfill(2) for v in args.vol.split(",") if v.strip()] if args.vol else None
    fields: Optional[List[str]] = [f.strip() for f in args.field.split(",") if f.strip()] if args.field else None

    try:
        ftp_host, ftp_user, ftp_pass = resolve_ftp_credentials()
    except RuntimeError as e:
        print(f"✗ {e}", file=sys.stderr)
        sys.exit(1)

    from radarlib.io.bufr.bufr import BUFRFilename
    from radarlib.io.ftp.ftp_client import RadarFTPClient

    dt_end = datetime.now(tz=timezone.utc)
    dt_start = dt_end - timedelta(days=args.lookback_days)

    logger.info(
        f"Searching radars={radars} from {dt_start.strftime('%Y-%m-%d %H:%M')} "
        f"to {dt_end.strftime('%Y-%m-%d %H:%M')} UTC"
    )

    vol_types = build_vol_types(strategies, vols, fields)
    logger.debug(f"vol_types filter: {vol_types}")

    # ------------------------------------------------------------------
    # Step 1 — Traverse FTP for every radar and collect raw hits
    # ------------------------------------------------------------------
    # Each hit: (radar, obs_dt, fname, full_remote)
    raw_hits: List[Tuple[str, datetime, str, str]] = []

    with RadarFTPClient(host=ftp_host, user=ftp_user, password=ftp_pass) as client:
        logger.info(f"Connected to FTP {ftp_host}")

        for radar in radars:
            logger.info(f"Traversing radar {radar} ...")
            count_before = len(raw_hits)
            for obs_dt, fname, full_remote in client.traverse_radar(
                radar_name=radar,
                dt_start=dt_start,
                dt_end=dt_end,
                vol_types=vol_types,
            ):
                raw_hits.append((radar, obs_dt, str(fname), str(full_remote)))
            logger.info(f"  {radar}: {len(raw_hits) - count_before} files found")

    logger.info(f"Traversal complete: {len(raw_hits)} total files across {len(radars)} radar(s)")

    if not raw_hits:
        if args.output_format == "json":
            print("[]")
        else:
            print(f"No BUFR files found for radar(s) {', '.join(radars)} in the last " f"{args.lookback_days} day(s).")
        return

    # ------------------------------------------------------------------
    # Step 2 — Parse filenames and apply any remaining post-filters
    # ------------------------------------------------------------------
    # Group by (radar, strategy, vol, field) → list of (obs_dt, remote_path)
    combo_hits: Dict[Tuple[str, str, str, str], List[Tuple[datetime, str]]] = defaultdict(list)

    for radar, obs_dt, fname, full_remote in raw_hits:
        try:
            parsed = BUFRFilename(fname)
        except ValueError:
            logger.debug(f"Skipping unparseable filename: {fname}")
            continue

        # Post-filter: strategy
        if strategies is not None and parsed.strategy not in strategies:
            continue
        # Post-filter: vol
        if vols is not None and parsed.volume not in vols:
            continue
        # Post-filter: field
        if fields is not None and parsed.field not in fields:
            continue

        key = (radar, parsed.strategy, parsed.volume, parsed.field)
        combo_hits[key].append((obs_dt, full_remote))

    if not combo_hits:
        if args.output_format == "json":
            print("[]")
        else:
            filters = ", ".join(
                filter(
                    None,
                    [
                        f"strategy={strategies}" if strategies else None,
                        f"vol={vols}" if vols else None,
                        f"field={fields}" if fields else None,
                    ],
                )
            )
            print(
                f"No matching BUFR files found for radar(s) {', '.join(radars)}"
                + (f" with filters: {filters}" if filters else "")
                + f" in the last {args.lookback_days} day(s)."
            )
        return

    logger.info(f"Unique (radar,strategy,vol,field) combinations: {len(combo_hits)}")

    # ------------------------------------------------------------------
    # Step 3 — Select top N per combination
    # ------------------------------------------------------------------
    selected: List[Tuple[Tuple[str, str, str, str], datetime, str]] = []
    for key, hits in combo_hits.items():
        top_n = sorted(hits, key=lambda h: h[0], reverse=True)[: args.last_n]
        for obs_dt, remote_path in top_n:
            selected.append((key, obs_dt, remote_path))

    logger.info(f"Selected {len(selected)} file(s) for reporting")

    # ------------------------------------------------------------------
    # Step 4 — Fetch file sizes via a single raw ftplib connection
    # ------------------------------------------------------------------
    sizes: Dict[str, Optional[int]] = {}
    try:
        ftp_raw = ftplib.FTP(ftp_host, timeout=30)
        ftp_raw.login(ftp_user, ftp_pass)
        for _key, _dt, remote_path in selected:
            sizes[remote_path] = get_ftp_size(ftp_raw, remote_path)
            logger.debug(f"SIZE {remote_path} → {sizes[remote_path]}")
        ftp_raw.quit()
    except Exception as e:
        logger.warning(f"Could not fetch file sizes: {e}")
        for _key, _dt, remote_path in selected:
            if remote_path not in sizes:
                sizes[remote_path] = None

    # ------------------------------------------------------------------
    # Step 5 — Build result records and output
    # ------------------------------------------------------------------
    records: List[FileRecord] = []
    for (radar, strategy, vol, field), obs_dt, remote_path in selected:
        records.append(
            FileRecord(
                radar=radar,
                strategy=strategy,
                vol=vol,
                field=field,
                obs_dt=obs_dt,
                remote_path=remote_path,
                size_bytes=sizes.get(remote_path),
            )
        )

    # Sort: radar → strategy → vol → field → timestamp descending
    records.sort(key=lambda r: (r.radar, r.strategy, r.vol, r.field, -r.obs_dt.timestamp()))

    if args.output_format == "json":
        print_json(records)
    else:
        print_table(records)


if __name__ == "__main__":
    main()
