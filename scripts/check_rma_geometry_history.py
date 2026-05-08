#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Geometry History Checker - Diagnostic Tool

Connects to the live FTP server and samples .BUFR files for a given radar
over a specified time window. Decodes each sampled file just enough to
extract radar geometry dimensions (ngates, nrays, gate_size, gate_offset),
then prints a timeline showing whether the geometry changed over that window.

Primary use case: diagnosing geometry mismatches between cached .npz files
and currently arriving data (e.g. after a radar scan strategy change).

Usage:
    python3 scripts/check_rma_geometry_history.py \\
        --radar RMA2 \\
        --strategy 0315 \\
        --start "2026-05-01 00:00" \\
        --end "2026-05-04 12:00" \\
        [--vol 01] \\
        [--field DBZH] \\
        [--sample-interval 60] \\
        [--download-dir /tmp/rma_geometry_check] \\
        [--log-level INFO]

Docker execution:
    docker exec genpro25-rma2 python3 /workspace/scripts/check_rma_geometry_history.py \\
        --radar RMA2 --strategy 0315 \\
        --start "2026-05-01 00:00" --end "2026-05-04 12:00" \\
        --vol 01 --sample-interval 60
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Setup logging to stderr so stdout stays clean for the report
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("check_rma_geometry_history")


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class GeometryRecord:
    """Result of decoding one BUFR file for geometry extraction."""

    timestamp: datetime  # Observation timestamp of the file
    filename: str
    ngates: Optional[int] = None
    nrays: Optional[int] = None
    gate_size: Optional[int] = None
    gate_offset: Optional[int] = None
    status: str = "OK"  # OK | DECODE_ERROR | FTP_ERROR
    error_msg: str = ""
    changed: bool = False  # True if geometry differs from previous OK record


@dataclass
class GeometryVariant:
    """A unique combination of geometry values seen across samples."""

    ngates: int
    nrays: int
    gate_size: int
    gate_offset: int
    first_seen: datetime
    last_seen: datetime
    count: int = 0

    def matches(self, rec: GeometryRecord) -> bool:
        return (
            rec.ngates == self.ngates
            and rec.nrays == self.nrays
            and rec.gate_size == self.gate_size
            and rec.gate_offset == self.gate_offset
        )

    def key(self) -> Tuple[int, int, int, int]:
        return (self.ngates, self.nrays, self.gate_size, self.gate_offset)


# ---------------------------------------------------------------------------
# Credential resolution — identical to check_rma_bufr_ftp.py
# ---------------------------------------------------------------------------


def resolve_ftp_credentials() -> Tuple[str, str, str]:
    """Resolve FTP credentials from app.config or environment variables."""
    try:
        # Try to import from app.config (genpro25.yml)
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

    # Fallback to environment variables
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
# Config helpers
# ---------------------------------------------------------------------------


def load_app_config():
    """Import app.config (genpro25.yml merged config). Returns module or None."""
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
        import config as app_config

        return app_config
    except Exception as e:
        logger.debug(f"app.config not available: {e}")
        return None


def get_geometry_path(app_config) -> str:
    """Return ROOT_GEOMETRY_PATH from app config, or a sensible default."""
    if app_config is not None:
        val = getattr(app_config, "ROOT_GEOMETRY_PATH", None)
        if val:
            return str(val)
    return os.environ.get("ROOT_GEOMETRY_PATH", "data/geometries")


def get_volume_types(app_config, strategy: str) -> Dict[str, List[str]]:
    """
    Return the vol_nr -> [fields] mapping for a given strategy from VOLUME_TYPES.

    Returns {} if strategy not found.
    """
    if app_config is not None:
        volume_types = getattr(app_config, "VOLUME_TYPES", None)
        if volume_types and isinstance(volume_types, dict):
            return volume_types.get(strategy, {})

    # Fallback default
    defaults = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        }
    }
    return defaults.get(strategy, {})


# ---------------------------------------------------------------------------
# Sampling logic
# ---------------------------------------------------------------------------


def collect_samples(
    client,
    radar: str,
    strategy: str,
    vol_nr: str,
    fields_priority: List[str],
    dt_start: datetime,
    dt_end: datetime,
    sample_interval_minutes: int,
) -> List[Tuple[datetime, str, str]]:
    """
    Use RadarFTPClient.traverse_radar() to enumerate available files, then
    apply the sample interval, keeping at most one file per sampled time slot.

    Returns a list of (datetime, filename, remote_path) tuples to be decoded.

    The vol_types dict passed to traverse_radar uses the strategy and vol_nr
    to filter file names, selecting only the preferred fields.
    """
    # Build a vol_types dict that traverse_radar can convert to a regex.
    # Format: {strategy: {vol_nr: [fields]}}
    vol_types_filter: Dict[str, Dict[str, List[str]]] = {
        strategy: {vol_nr: fields_priority}
    }

    # Collect all matching (dt, fname, remote_path) from the traversal
    all_slots: List[Tuple[datetime, str, str]] = []
    logger.debug(f"Starting traverse_radar for {radar}, strategy={strategy}, vol={vol_nr}, "
                 f"window=[{dt_start.isoformat()}, {dt_end.isoformat()}]")

    for dt, fname, full_remote in client.traverse_radar(
        radar_name=radar,
        dt_start=dt_start,
        dt_end=dt_end,
        vol_types=vol_types_filter,
    ):
        logger.debug(f"FTP slot: {dt.isoformat()} -> {fname}")
        all_slots.append((dt, fname, str(full_remote)))

    if not all_slots:
        return []

    # Sort chronologically (traverse_radar already does this, but be explicit)
    all_slots.sort(key=lambda x: x[0])

    # Group by time slot (same dt can have multiple field files)
    # We want one representative file per time slot, preferring fields in priority order.
    # Build a dict: dt -> {field_name: (fname, remote_path)}
    slot_map: Dict[datetime, Dict[str, Tuple[str, str]]] = {}
    for dt, fname, remote_path in all_slots:
        if dt not in slot_map:
            slot_map[dt] = {}
        # Extract field from filename: {RADAR}_{STRATEGY}_{VOL}_{FIELD}_{TIMESTAMP}.BUFR
        parts = fname.split("_")
        if len(parts) >= 4:
            field_name = parts[3]
            slot_map[dt][field_name] = (fname, remote_path)

    # Now apply sample interval: walk sorted time slots and keep every
    # slot that is >= sample_interval_minutes after the last kept slot.
    sorted_dts = sorted(slot_map.keys())
    sampled: List[Tuple[datetime, str, str]] = []
    last_kept_dt: Optional[datetime] = None

    interval = timedelta(minutes=sample_interval_minutes)

    for dt in sorted_dts:
        if last_kept_dt is None or (dt - last_kept_dt) >= interval:
            # Pick the best available field for this slot
            available = slot_map[dt]
            chosen_fname: Optional[str] = None
            chosen_remote: Optional[str] = None
            for field in fields_priority:
                if field in available:
                    chosen_fname, chosen_remote = available[field]
                    break
            # If preferred fields not found, take the first available
            if chosen_fname is None and available:
                chosen_fname, chosen_remote = next(iter(available.values()))

            if chosen_fname is not None and chosen_remote is not None:
                sampled.append((dt, chosen_fname, chosen_remote))
                last_kept_dt = dt

    return sampled


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def decode_geometry(local_path: str) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """
    Decode a locally downloaded BUFR file and extract geometry from the
    sweeps DataFrame. Does NOT build a PyART object.

    Returns (ngates, nrays, gate_size, gate_offset) or raises on failure.
    """
    from radarlib.io.bufr.bufr import bufr_to_dict

    field_dict = bufr_to_dict(local_path, root_resources=None)
    if field_dict is None:
        raise ValueError(f"bufr_to_dict returned None for {local_path}")

    sweeps = field_dict["info"]["sweeps"]
    ngates = int(sweeps["ngates"].iloc[0])
    nrays = int(sweeps["nrayos"].iloc[0])
    gate_size = int(sweeps["gate_size"].iloc[0])
    gate_offset = int(sweeps["gate_offset"].iloc[0])

    # Warn if geometry varies across sweeps within this file
    if len(sweeps) > 1:
        if (
            sweeps["ngates"].nunique() > 1
            or sweeps["nrayos"].nunique() > 1
            or sweeps["gate_size"].nunique() > 1
            or sweeps["gate_offset"].nunique() > 1
        ):
            logger.debug(
                f"  Note: file has {len(sweeps)} sweeps with varying geometry — "
                "using iloc[0] values"
            )

    return ngates, nrays, gate_size, gate_offset


def process_vol(
    client,
    radar: str,
    strategy: str,
    vol_nr: str,
    fields_priority: List[str],
    dt_start: datetime,
    dt_end: datetime,
    sample_interval_minutes: int,
    download_dir: Path,
) -> List[GeometryRecord]:
    """
    Sample, download, decode and extract geometry for a single vol_nr.

    Returns a list of GeometryRecord, one per sampled slot.
    """
    logger.info(f"Sampling vol={vol_nr}: collecting FTP slots...")
    sampled = collect_samples(
        client=client,
        radar=radar,
        strategy=strategy,
        vol_nr=vol_nr,
        fields_priority=fields_priority,
        dt_start=dt_start,
        dt_end=dt_end,
        sample_interval_minutes=sample_interval_minutes,
    )

    logger.info(f"Sampling vol={vol_nr}: {len(sampled)} files to decode")

    records: List[GeometryRecord] = []

    for i, (dt, fname, remote_path) in enumerate(sampled, 1):
        local_path = download_dir / fname

        if (i % 10 == 0) or i == 1 or i == len(sampled):
            logger.info(f"Decoded {i}/{len(sampled)} for vol={vol_nr}...")

        # Download
        try:
            logger.debug(f"Downloading {remote_path}")
            client.download_file(str(remote_path), local_path)
        except Exception as e:
            logger.warning(f"FTP_ERROR downloading {fname}: {e}")
            records.append(
                GeometryRecord(
                    timestamp=dt,
                    filename=fname,
                    status="FTP_ERROR",
                    error_msg=str(e),
                )
            )
            continue

        # Decode and extract geometry, always clean up local file
        try:
            logger.debug(f"Decoding {fname}")
            ngates, nrays, gate_size, gate_offset = decode_geometry(str(local_path))
            records.append(
                GeometryRecord(
                    timestamp=dt,
                    filename=fname,
                    ngates=ngates,
                    nrays=nrays,
                    gate_size=gate_size,
                    gate_offset=gate_offset,
                    status="OK",
                )
            )
        except Exception as e:
            logger.warning(f"DECODE_ERROR for {fname}: {e}")
            records.append(
                GeometryRecord(
                    timestamp=dt,
                    filename=fname,
                    status="DECODE_ERROR",
                    error_msg=str(e),
                )
            )
        finally:
            if local_path.exists():
                try:
                    local_path.unlink()
                except Exception:
                    pass

    # Mark geometry changes: compare each OK record against the most recent
    # prior OK record
    prev_ok: Optional[GeometryRecord] = None
    for rec in records:
        if rec.status != "OK":
            continue
        if prev_ok is not None:
            if (
                rec.ngates != prev_ok.ngates
                or rec.nrays != prev_ok.nrays
                or rec.gate_size != prev_ok.gate_size
                or rec.gate_offset != prev_ok.gate_offset
            ):
                rec.changed = True
        prev_ok = rec

    return records


# ---------------------------------------------------------------------------
# Report printing
# ---------------------------------------------------------------------------


def build_variants(records: List[GeometryRecord]) -> List[GeometryVariant]:
    """Summarise geometry variants seen across OK records."""
    variants: List[GeometryVariant] = []
    for rec in records:
        if rec.status != "OK":
            continue
        assert rec.ngates is not None
        assert rec.nrays is not None
        assert rec.gate_size is not None
        assert rec.gate_offset is not None
        matched = False
        for v in variants:
            if v.matches(rec):
                v.count += 1
                if rec.timestamp < v.first_seen:
                    v.first_seen = rec.timestamp
                if rec.timestamp > v.last_seen:
                    v.last_seen = rec.timestamp
                matched = True
                break
        if not matched:
            variants.append(
                GeometryVariant(
                    ngates=rec.ngates,
                    nrays=rec.nrays,
                    gate_size=rec.gate_size,
                    gate_offset=rec.gate_offset,
                    first_seen=rec.timestamp,
                    last_seen=rec.timestamp,
                    count=1,
                )
            )
    return variants


def print_vol_report(
    radar: str,
    strategy: str,
    vol_nr: str,
    fields_priority: List[str],
    field_label: str,
    dt_start: datetime,
    dt_end: datetime,
    sample_interval_minutes: int,
    total_available: int,
    records: List[GeometryRecord],
    geometry_path: str,
) -> None:
    ok_records = [r for r in records if r.status == "OK"]
    decode_errors = sum(1 for r in records if r.status == "DECODE_ERROR")
    ftp_errors = sum(1 for r in records if r.status == "FTP_ERROR")
    variants = build_variants(records)

    start_str = dt_start.strftime("%Y-%m-%d %H:%M")
    end_str = dt_end.strftime("%Y-%m-%d %H:%M")

    print()
    print(f"=== GEOMETRY HISTORY: {radar} | strategy={strategy} | vol={vol_nr} ===")
    print(f"Window  : {start_str} UTC → {end_str} UTC")
    print(f"Sampled : {len(records)} / {total_available} available slots (interval={sample_interval_minutes}min)")
    print(f"Field   : {field_label}")
    print()

    # Table header
    col_ts = 23
    col_ng = 8
    col_nr = 7
    col_gs = 11
    col_go = 12
    col_st = 10
    header = (
        f"{'TIMESTAMP (UTC)':<{col_ts}} "
        f"{'NGATES':>{col_ng}} "
        f"{'NRAYS':>{col_nr}} "
        f"{'GATE_SIZE':>{col_gs}} "
        f"{'GATE_OFFSET':>{col_go}} "
        f"{'STATUS':<{col_st}}"
    )
    print(header)
    print("-" * len(header))

    for rec in records:
        ts_str = rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        if rec.status == "OK":
            assert rec.ngates is not None
            change_marker = "  ← CHANGE" if rec.changed else ""
            print(
                f"{ts_str:<{col_ts}} "
                f"{rec.ngates:>{col_ng}} "
                f"{rec.nrays:>{col_nr}} "
                f"{rec.gate_size:>{col_gs}} "
                f"{rec.gate_offset:>{col_go}} "
                f"{'OK':<{col_st}}"
                f"{change_marker}"
            )
        else:
            print(
                f"{ts_str:<{col_ts}} "
                f"{'—':>{col_ng}} "
                f"{'—':>{col_nr}} "
                f"{'—':>{col_gs}} "
                f"{'—':>{col_go}} "
                f"{rec.status:<{col_st}}"
            )

    print()
    print("--- GEOMETRY CHANGE SUMMARY ---")

    if not variants:
        print("  No OK records — cannot determine geometry.")
    elif len(variants) == 1:
        v = variants[0]
        print(
            f"\n✓ No geometry change detected across {v.count} samples."
        )
        print(
            f"Geometry: ngates={v.ngates}, nrays={v.nrays}, "
            f"gate_size={v.gate_size}, gate_offset={v.gate_offset}"
        )
        print("Cached geometry .npz files are likely valid.")
    else:
        for idx, v in enumerate(variants, 1):
            print(
                f"\nGeometry variant {idx}: ngates={v.ngates}, nrays={v.nrays}, "
                f"gate_size={v.gate_size}, gate_offset={v.gate_offset}"
            )
            print(f"  First seen  : {v.first_seen.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"  Last seen   : {v.last_seen.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"  Sample count: {v.count}")

        # First record where changed=True
        change_point = next((r for r in ok_records if r.changed), None)
        if change_point:
            print()
            print(
                f"*** GEOMETRY CHANGE DETECTED at approx. "
                f"{change_point.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC ***"
            )
            print(
                "Cached geometry .npz files may be stale. "
                "Delete them to force regeneration."
            )
            print(f"Geometry cache path (from config): {geometry_path}")

    if decode_errors > 0 or ftp_errors > 0:
        print()
        print("--- ERRORS ---")
        if decode_errors:
            print(f"  {decode_errors} slot(s) had decode errors (see log for details).")
        if ftp_errors:
            print(f"  {ftp_errors} slot(s) had FTP errors.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile radar geometry history from live FTP BUFR files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check RMA2 geometry over a 3-day window
  python3 scripts/check_rma_geometry_history.py \\
    --radar RMA2 --strategy 0315 \\
    --start "2026-05-01 00:00" --end "2026-05-04 12:00"

  # Restrict to vol 01, sample every 30 minutes
  python3 scripts/check_rma_geometry_history.py \\
    --radar RMA1 --strategy 0315 \\
    --start "2026-04-28 00:00" --end "2026-05-01 00:00" \\
    --vol 01 --sample-interval 30

  # Docker execution (from host):
  docker exec genpro25-rma2 python3 /workspace/scripts/check_rma_geometry_history.py \\
    --radar RMA2 --strategy 0315 \\
    --start "2026-05-01 00:00" --end "2026-05-04 12:00"
        """,
    )

    parser.add_argument("--radar", required=True, help="Radar code (e.g., RMA1, RMA2)")
    parser.add_argument("--strategy", required=True, help="Strategy code (e.g., 0315)")
    parser.add_argument(
        "--start",
        required=True,
        help="Start datetime in UTC, format: YYYY-MM-DD HH:MM",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End datetime in UTC, format: YYYY-MM-DD HH:MM",
    )
    parser.add_argument(
        "--vol",
        default=None,
        help="Optional: restrict to a single vol_nr (e.g., 01). Default: all vols.",
    )
    parser.add_argument(
        "--field",
        default=None,
        help="Optional: force a specific field (e.g., DBZH). Default: auto-select.",
    )
    parser.add_argument(
        "--sample-interval",
        type=int,
        default=60,
        help="Minutes between sampled time slots (default: 60).",
    )
    parser.add_argument(
        "--download-dir",
        default="/tmp/rma_geometry_check",
        help="Temp directory for downloaded BUFR files (default: /tmp/rma_geometry_check).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING"],
        help="Log verbosity (default: INFO).",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    # Apply log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    # Parse datetimes — treat as UTC
    try:
        dt_start = datetime.strptime(args.start, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"✗ Invalid --start format: '{args.start}' (use YYYY-MM-DD HH:MM)", file=sys.stderr)
        sys.exit(1)
    try:
        dt_end = datetime.strptime(args.end, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"✗ Invalid --end format: '{args.end}' (use YYYY-MM-DD HH:MM)", file=sys.stderr)
        sys.exit(1)

    if dt_end <= dt_start:
        print("✗ --end must be after --start", file=sys.stderr)
        sys.exit(1)

    # Ensure download dir exists
    download_dir = Path(args.download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    # Load app config
    app_config = load_app_config()
    geometry_path = get_geometry_path(app_config)
    vol_types_for_strategy = get_volume_types(app_config, args.strategy)

    if not vol_types_for_strategy:
        logger.warning(
            f"No VOLUME_TYPES found for strategy {args.strategy}. "
            "Using empty field list — consider passing --field explicitly."
        )

    # Determine which vol_nrs to process
    if args.vol is not None:
        vol_nrs = [args.vol.zfill(2)]
    else:
        vol_nrs = sorted(vol_types_for_strategy.keys()) if vol_types_for_strategy else []
        if not vol_nrs:
            print(
                f"✗ No vol_nrs found for strategy {args.strategy} in VOLUME_TYPES. "
                "Pass --vol explicitly.",
                file=sys.stderr,
            )
            sys.exit(1)

    # Resolve FTP credentials
    try:
        ftp_host, ftp_user, ftp_pass = resolve_ftp_credentials()
    except RuntimeError as e:
        print(f"✗ {e}", file=sys.stderr)
        sys.exit(1)

    # Import RadarFTPClient
    from radarlib.io.ftp.ftp_client import FTPError, RadarFTPClient

    with RadarFTPClient(host=ftp_host, user=ftp_user, password=ftp_pass) as client:
        logger.info(f"Connected to FTP {ftp_host}")

        for vol_nr in vol_nrs:
            # Determine field priority list for this vol_nr
            if args.field:
                fields_priority = [args.field]
                field_label = f"{args.field} (forced)"
            else:
                fields_priority = vol_types_for_strategy.get(vol_nr, [])
                if not fields_priority:
                    logger.warning(
                        f"No fields configured for vol={vol_nr} in strategy={args.strategy}. "
                        "Skipping."
                    )
                    continue
                field_label = f"{fields_priority[0]} (auto-selected)"

            # Count available slots (we traverse once for sampling — total_available
            # is derived from the un-sampled traversal, but that would double the
            # traversal cost. Instead, report "sampled / all sampled" since we only
            # traverse once and apply the interval in collect_samples).
            records = process_vol(
                client=client,
                radar=args.radar,
                strategy=args.strategy,
                vol_nr=vol_nr,
                fields_priority=fields_priority,
                dt_start=dt_start,
                dt_end=dt_end,
                sample_interval_minutes=args.sample_interval,
                download_dir=download_dir,
            )

            # For total_available we use len(records) since we already applied
            # sampling during collection.  The sampling count equals len(records).
            # Distinguish it as the count of sampled slots, not total FTP slots,
            # since a second traversal solely for counting would be expensive.
            print_vol_report(
                radar=args.radar,
                strategy=args.strategy,
                vol_nr=vol_nr,
                fields_priority=fields_priority,
                field_label=field_label,
                dt_start=dt_start,
                dt_end=dt_end,
                sample_interval_minutes=args.sample_interval,
                total_available=len(records),
                records=records,
                geometry_path=geometry_path,
            )

    logger.info("Done.")


if __name__ == "__main__":
    main()
