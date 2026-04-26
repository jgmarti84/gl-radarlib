#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
State DB Processing Timeline Profiler - Diagnostic Tool

Connects to a radar's local SQLite state database and extracts the exact timeline
of when BUFR files were downloaded and processed by the daemon.

This helps identify if files were downloaded out of chronological order,
which could explain missing products or incomplete scan cycles.

Usage:
    python3 scripts/profile_state_db.py \\
        --radar RMA1 \\
        --date 2026-04-17 \\
        --strategy 0315 \\
        --vol 01 \\
        --field DBZH \\
        [--hour 12]  # optional

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/profile_state_db.py \\
        --radar RMA1 --date 2026-04-17 --strategy 0315 --vol 01 --field DBZH
"""

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


class StateDBRecord:
    """Represents a single download record from state.db."""

    def __init__(
        self,
        filename: str,
        observation_datetime: str,
        created_at: str,
        updated_at: Optional[str] = None,
        file_size: Optional[int] = None,
        status: str = "unknown",
    ):
        self.filename = filename
        self.observation_datetime = observation_datetime
        self.created_at = created_at
        self.updated_at = updated_at
        self.file_size = file_size
        self.status = status

        # Parse datetimes
        try:
            self.obs_dt = datetime.fromisoformat(
                observation_datetime.replace("Z", "+00:00")
                if "Z" in observation_datetime
                else observation_datetime
            )
        except Exception as e:
            logger.warning(f"Failed to parse obs datetime '{observation_datetime}': {e}")
            self.obs_dt = None

        try:
            self.created_dt = datetime.fromisoformat(
                created_at.replace("Z", "+00:00")
                if "Z" in created_at
                else created_at
            )
        except Exception as e:
            logger.warning(f"Failed to parse created_at '{created_at}': {e}")
            self.created_dt = None

        # Calculate delay in seconds
        if self.obs_dt and self.created_dt:
            self.delay_seconds = (self.created_dt - self.obs_dt).total_seconds()
        else:
            self.delay_seconds = None

    def is_out_of_order(self, previous_record: Optional["StateDBRecord"]) -> bool:
        """Check if this record arrived before a previous one (by obs time)."""
        if not previous_record or not self.obs_dt or not previous_record.obs_dt:
            return False
        return self.obs_dt < previous_record.obs_dt


def get_state_db_path(radar_name: str) -> Path:
    """
    Resolve the path to the state.db file for a given radar.

    Uses ROOT_RADAR_FILES_PATH environment variable or defaults to standard location.

    Args:
        radar_name: Radar code (e.g., "RMA1")

    Returns:
        Path to state.db file

    Raises:
        FileNotFoundError: If state.db doesn't exist
    """
    # First try ROOT_RADAR_FILES_PATH env var
    root_path = os.environ.get("ROOT_RADAR_FILES_PATH")

    if not root_path:
        # Try standard Docker path
        root_path = "/workspace/app/data/radares"

    db_path = Path(root_path) / radar_name / "state.db"

    if not db_path.exists():
        raise FileNotFoundError(
            f"State database not found: {db_path}\n"
            f"Set ROOT_RADAR_FILES_PATH environment variable if using non-standard path."
        )

    return db_path


def query_downloads(
    db_path: Path,
    date_str: str,
    strategy: str,
    vol_nr: str,
    field_type: str,
    hour: Optional[str] = None,
) -> List[StateDBRecord]:
    """
    Query the downloads table for matching records.

    Args:
        db_path: Path to state.db
        date_str: Date in YYYY-MM-DD format
        strategy: Strategy code (e.g., "0315")
        vol_nr: Volume number (e.g., "01")
        field_type: Field name (e.g., "DBZH")
        hour: Optional hour filter (00-23)

    Returns:
        List of StateDBRecord objects sorted by creation time
    """
    records: List[StateDBRecord] = []

    try:
        # Use read-only URI parameter to prevent locking the DB
        # sqlite3 in Python 3.10+ supports URI filenames
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row

        cursor = conn.cursor()

        # Build query with optional hour filtering
        query = """
            SELECT 
                filename,
                observation_datetime,
                created_at,
                updated_at,
                file_size,
                status
            FROM downloads
            WHERE DATE(observation_datetime) = ?
                AND strategy = ?
                AND vol_nr = ?
                AND field_type = ?
        """
        params = [date_str, strategy, vol_nr.zfill(2), field_type]

        # Add hour filter if specified
        if hour:
            query += " AND strftime('%H', observation_datetime) = ?"
            params.append(hour)

        query += " ORDER BY created_at ASC"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        for row in rows:
            record = StateDBRecord(
                filename=row["filename"],
                observation_datetime=row["observation_datetime"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                file_size=row["file_size"],
                status=row["status"],
            )
            records.append(record)

        conn.close()
        logger.info(f"Successfully queried {len(records)} records from state.db")

    except sqlite3.OperationalError as e:
        # Fallback to regular (non-URI) connection if URI not supported
        if "mode=ro" in str(e) or "URI" in str(e):
            logger.debug(
                "URI mode not supported, falling back to regular connection",
            )
            conn = sqlite3.connect(str(db_path), timeout=5.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row

            cursor = conn.cursor()

            # Same query with fallback connection
            query = """
                SELECT 
                    filename,
                    observation_datetime,
                    created_at,
                    updated_at,
                    file_size,
                    status
                FROM downloads
                WHERE DATE(observation_datetime) = ?
                    AND strategy = ?
                    AND vol_nr = ?
                    AND field_type = ?
            """
            params = [date_str, strategy, vol_nr.zfill(2), field_type]

            if hour:
                query += " AND strftime('%H', observation_datetime) = ?"
                params.append(hour)

            query += " ORDER BY created_at ASC"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            for row in rows:
                record = StateDBRecord(
                    filename=row["filename"],
                    observation_datetime=row["observation_datetime"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    file_size=row["file_size"],
                    status=row["status"],
                )
                records.append(record)

            conn.close()
            logger.info(f"Successfully queried {len(records)} records from state.db")
        else:
            raise

    except Exception as e:
        logger.error(f"Failed to query state.db: {e}")
        raise

    return records


def format_size(bytes_val: Optional[int]) -> str:
    """Format file size in human-readable format."""
    if bytes_val is None:
        return "—"
    if bytes_val < 1024:
        return f"{bytes_val}B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f}KB"
    else:
        return f"{bytes_val / (1024 * 1024):.1f}MB"


def format_delay(seconds: Optional[float]) -> str:
    """Format delay in seconds to human-readable string."""
    if seconds is None:
        return "—"

    if seconds < 0:
        # Early arrival (shouldn't happen but possible)
        minutes = abs(seconds) / 60
        if minutes < 1:
            return f"−{seconds:.1f}s"
        else:
            return f"−{minutes:.1f}m"
    else:
        minutes = seconds / 60
        if minutes < 1:
            return f"{seconds:.1f}s"
        elif minutes < 60:
            return f"{minutes:.1f}m"
        else:
            hours = minutes / 60
            return f"{hours:.2f}h"


def print_results(
    records: List[StateDBRecord],
    radar: str,
    date_str: str,
    strategy: str,
    vol_nr: str,
    field_type: str,
    hour: Optional[str] = None,
) -> None:
    """Print formatted results table."""

    print("\n" + "=" * 180)
    print(f"State DB Processing Timeline - {radar} ({date_str})")
    print(f"Strategy: {strategy}, Volume: {vol_nr}, Field: {field_type}", end="")
    if hour:
        print(f", Hour: {hour}:00-{hour}:59")
    else:
        print()
    print("=" * 180)

    if not records:
        print("\n✗ No records found matching criteria.")
        return

    print(
        f"\nFound {len(records)} downloads (sorted by download time / created_at):\n"
    )

    # Header
    print(
        f"{'#':<4} "
        f"{'Filename':<50} "
        f"{'Observation Time':<22} "
        f"{'Downloaded At':<22} "
        f"{'Delay':<12} "
        f"{'Size':<10} "
        f"{'Order':<10}"
    )
    print("-" * 180)

    out_of_order_count = 0
    prev_record: Optional[StateDBRecord] = None

    for i, record in enumerate(records, 1):
        obs_str = (
            record.obs_dt.strftime("%Y-%m-%d %H:%M:%S")
            if record.obs_dt
            else "—"
        )
        created_str = (
            record.created_dt.strftime("%Y-%m-%d %H:%M:%S")
            if record.created_dt
            else "—"
        )
        delay_str = format_delay(record.delay_seconds)
        size_str = format_size(record.file_size)

        # Check for out-of-order
        is_ooo = record.is_out_of_order(prev_record)
        if is_ooo:
            out_of_order_count += 1
            order_status = "✗ OUT-OF-ORDER"
        else:
            order_status = "✓ OK"

        print(
            f"{i:<4} "
            f"{record.filename:<50} "
            f"{obs_str:<22} "
            f"{created_str:<22} "
            f"{delay_str:<12} "
            f"{size_str:<10} "
            f"{order_status:<10}"
        )

        prev_record = record

    # Summary statistics
    print("\n" + "=" * 180)
    print("Summary Statistics:")
    print("=" * 180)

    if records:
        delays = [r.delay_seconds for r in records if r.delay_seconds is not None]
        obs_times = [r.obs_dt for r in records if r.obs_dt is not None]
        created_times = [r.created_dt for r in records if r.created_dt is not None]

        print(f"\n  Total files:          {len(records)}")
        print(f"  Out-of-order files:   {out_of_order_count} ({100*out_of_order_count/len(records):.1f}%)")

        if delays:
            min_delay = min(delays)
            max_delay = max(delays)
            avg_delay = sum(delays) / len(delays)

            print(f"\n  Min download delay:   {format_delay(min_delay)}")
            print(f"  Max download delay:   {format_delay(max_delay)}")
            print(f"  Avg download delay:   {format_delay(avg_delay)}")

        if obs_times:
            time_span = obs_times[-1] - obs_times[0]
            print(f"\n  Observation time span: {time_span}")

        if created_times:
            download_span = created_times[-1] - created_times[0]
            print(f"  Download time span:   {download_span}")

        # Alerts
        if out_of_order_count > 0:
            print(
                f"\n  ✗ ALERT: {out_of_order_count} file(s) downloaded out of chronological order!"
            )
            print(
                f"     This indicates temporal aliasing or FTP timing irregularities."
            )
            print(
                f"     Files with older observation times were downloaded AFTER files with newer times."
            )

        if delays and max(delays) - min(delays) > 600:  # More than 10 minutes variance
            variance = max(delays) - min(delays)
            print(
                f"\n  ⚠ WARNING: High variance in download delays ({format_delay(variance)})"
            )
            print(
                f"     Download times are irregular. This could impact volume completeness detection."
            )

    print()


def main():
    parser = argparse.ArgumentParser(
        description="Profile state database download timeline for out-of-order arrivals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Profile vol01 DBZH downloads for 2026-04-17 with strategy 0315
  python3 scripts/profile_state_db.py \\
    --radar RMA1 --date 2026-04-17 --strategy 0315 --vol 01 --field DBZH
  
  # Profile vol02 VRAD downloads only during hour 12 (noon)
  python3 scripts/profile_state_db.py \\
    --radar RMA2 --date 2026-04-17 --strategy 0315 --vol 02 --field VRAD --hour 12
  
  # Docker execution (from host):
  docker exec genpro25-rma1 python3 /workspace/scripts/profile_state_db.py \\
    --radar RMA1 --date 2026-04-17 --strategy 0315 --vol 01 --field DBZH
        """,
    )

    parser.add_argument(
        "--radar",
        required=True,
        help="Radar code (e.g., RMA1, RMA2, RMA16)",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date in YYYY-MM-DD format (e.g., 2026-04-17)",
    )
    parser.add_argument(
        "--strategy",
        required=True,
        help="Strategy code (e.g., 0315)",
    )
    parser.add_argument(
        "--vol",
        required=True,
        help="Volume number (e.g., 01, 02)",
    )
    parser.add_argument(
        "--field",
        required=True,
        help="Field name (e.g., DBZH, VRAD, ZDR)",
    )
    parser.add_argument(
        "--hour",
        default=None,
        help="Optional: filter to specific hour (00-23)",
    )

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"✗ Invalid date format: {args.date} (use YYYY-MM-DD)")
        sys.exit(1)

    # Validate strategy
    if not args.strategy.isdigit() or len(args.strategy) != 4:
        print(f"✗ Invalid strategy: {args.strategy} (use 4-digit code like 0315)")
        sys.exit(1)

    # Validate volume
    try:
        vol_num = int(args.vol)
        if not (1 <= vol_num <= 99):
            raise ValueError()
    except (ValueError, TypeError):
        print(f"✗ Invalid volume: {args.vol} (use 01, 02, etc.)")
        sys.exit(1)

    # Validate hour if provided
    if args.hour:
        if not args.hour.isdigit() or not (0 <= int(args.hour) <= 23):
            print(f"✗ Invalid hour: {args.hour} (use 00-23)")
            sys.exit(1)
        args.hour = f"{int(args.hour):02d}"

    try:
        # Resolve state.db path
        db_path = get_state_db_path(args.radar)
        logger.info(f"Using state database: {db_path}")

        # Query records
        records = query_downloads(
            db_path,
            args.date,
            args.strategy,
            args.vol,
            args.field,
            args.hour,
        )

        # Print results
        print_results(
            records,
            radar=args.radar,
            date_str=args.date,
            strategy=args.strategy,
            vol_nr=args.vol,
            field_type=args.field,
            hour=args.hour,
        )

    except FileNotFoundError as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
