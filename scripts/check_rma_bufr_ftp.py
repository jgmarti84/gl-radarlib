#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FTP Arrival Time Profiler - Diagnostic Tool

Connects to the live FTP server and analyzes the arrival timestamps of BUFR files,
comparing observation times (from filename) against FTP modification times (when files appeared on server).

This helps identify if files are arriving out of chronological order, which could explain
missing products or incomplete scan cycles.

Usage:
    python3 scripts/profile_ftp_arrivals.py \\
        --radar RMA1 \\
        --date 2026-04-17 \\
        --strategy 0315 \\
        --vol 01 \\
        --field DBZH \\
        [--hour 12]  # optional: filter to specific hour

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/profile_ftp_arrivals.py \\
        --radar RMA1 --date 2026-04-17 --strategy 0315 --vol 01 --field DBZH
"""

import argparse
import ftplib
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from dataclasses import dataclass
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class FTPFileMetadata:
    """Metadata for a single BUFR file on FTP."""
    filename: str
    obs_datetime: datetime  # Extracted from filename
    ftp_mtime: datetime     # FTP modification time
    arrival_delay_sec: float  # FTP mtime - obs datetime, in seconds
    is_out_of_order: bool = False


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


def parse_bufr_filename(filename: str) -> Optional[datetime]:
    """
    Extract observation datetime from BUFR filename.
    
    Format: {radar}_{strategy}_{vol}_{field}_YYYYMMDDTHHMMSSZ.BUFR
    Example: RMA1_0315_01_DBZH_20260417T212900Z.BUFR
    
    Returns datetime or None if parsing fails.
    """
    try:
        # Match the datetime part: YYYYMMDDTHHMMSSZ
        match = re.search(r'(\d{8}T\d{6}Z)', filename)
        if not match:
            return None
        
        dt_str = match.group(1)  # e.g., "20260417T212900Z"
        obs_dt = datetime.strptime(dt_str, "%Y%m%dT%H%M%SZ")
        return obs_dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.warning(f"Failed to parse datetime from {filename}: {e}")
        return None


def get_ftp_mtime(ftp: ftplib.FTP, filename: str) -> Optional[datetime]:
    """
    Get FTP modification time for a file using MDTM command.
    
    MDTM response format: "213 YYYYMMDDHHMMSS"
    
    Returns datetime or None if MDTM fails.
    """
    try:
        response = ftp.sendcmd(f"MDTM {filename}")
        # Parse: "213 YYYYMMDDHHMMSS"
        if response.startswith("213"):
            mtime_str = response.split()[-1]
            mtime = datetime.strptime(mtime_str, "%Y%m%d%H%M%S")
            return mtime.replace(tzinfo=timezone.utc)
    except ftplib.error_perm:
        logger.debug(f"MDTM failed for {filename} (permission denied)")
    except Exception as e:
        logger.debug(f"MDTM failed for {filename}: {e}")
    
    return None


def scan_ftp_directory(
    ftp: ftplib.FTP,
    radar: str,
    date_str: str,
    strategy: str,
    vol: str,
    field: str,
    hour: Optional[str] = None,
) -> List[FTPFileMetadata]:
    """
    Scan FTP directory for matching BUFR files and collect metadata.
    
    Args:
        ftp: Connected FTP client
        radar: Radar code (e.g., "RMA1")
        date_str: Date in YYYY-MM-DD format
        strategy: Strategy code (e.g., "0315")
        vol: Volume number (e.g., "01")
        field: Field name (e.g., "DBZH")
        hour: Optional hour filter (00-23)
    
    Returns:
        List of FTPFileMetadata sorted by FTP arrival time
    """
    # Parse date
    year, month, day = date_str.split("-")
    
    # Build pattern to match: {radar}_{strategy}_{vol}_{field}_*.BUFR
    pattern = re.compile(
        rf"^{re.escape(radar)}_{re.escape(strategy)}_{re.escape(vol):0>2}_{re.escape(field)}_\d{{8}}T\d{{6}}Z\.BUFR$",
        re.IGNORECASE,
    )
    
    files_metadata: List[FTPFileMetadata] = []
    
    try:
        # Navigate to base directory for this date
        base_path = f"/L2/{radar}/{year}/{month}/{day}"
        logger.info(f"Navigating to {base_path}")
        ftp.cwd(base_path)
        
        # List hour directories
        try:
            hours = ftp.nlst()
        except ftplib.error_perm:
            logger.error(f"Permission denied accessing {base_path}")
            return []
        
        for h in sorted(hours):
            # Filter by hour if specified
            if hour and h != hour:
                continue
            
            hour_path = f"{base_path}/{h}"
            
            try:
                # List minute directories under this hour
                ftp.cwd(hour_path)
                minutes = ftp.nlst()
            except ftplib.all_errors as e:
                logger.debug(f"Cannot access {hour_path}: {e}")
                continue
            
            for mm in sorted(minutes):
                minute_path = f"{hour_path}/{mm}"
                
                try:
                    # List files in minute directory
                    ftp.cwd(minute_path)
                    file_list = ftp.nlst()
                except ftplib.all_errors as e:
                    logger.debug(f"Cannot access {minute_path}: {e}")
                    continue
                
                for filename in sorted(file_list):
                    # Check if filename matches pattern
                    if not pattern.match(filename):
                        continue
                    
                    # Parse observation datetime from filename
                    obs_dt = parse_bufr_filename(filename)
                    if not obs_dt:
                        logger.warning(f"Could not parse datetime from {filename}")
                        continue
                    
                    # Get FTP modification time
                    ftp_mtime = get_ftp_mtime(ftp, filename)
                    if not ftp_mtime:
                        logger.warning(f"Could not get MDTM for {filename}")
                        continue
                    
                    # Calculate arrival delay
                    delay_sec = (ftp_mtime - obs_dt).total_seconds()
                    
                    files_metadata.append(
                        FTPFileMetadata(
                            filename=filename,
                            obs_datetime=obs_dt,
                            ftp_mtime=ftp_mtime,
                            arrival_delay_sec=delay_sec,
                        )
                    )
        
        # Sort by FTP arrival time
        files_metadata.sort(key=lambda x: x.ftp_mtime)
        
        # Flag out-of-order files
        if files_metadata:
            last_obs_dt = None
            for i, meta in enumerate(files_metadata):
                if last_obs_dt and meta.obs_datetime < last_obs_dt:
                    meta.is_out_of_order = True
                last_obs_dt = meta.obs_datetime
    
    except Exception as e:
        logger.error(f"Error scanning FTP: {e}")
        import traceback
        traceback.print_exc()
    
    return files_metadata


def format_delay(seconds: float) -> str:
    """Format delay in seconds to human-readable string."""
    if seconds < 0:
        return f"−{abs(seconds):.1f}s"  # Early arrival
    else:
        minutes = seconds / 60
        if minutes < 1:
            return f"{seconds:.1f}s"
        else:
            return f"{minutes:.1f}m"


def print_results(
    files_metadata: List[FTPFileMetadata],
    radar: str,
    date_str: str,
    strategy: str,
    vol: str,
    field: str,
) -> None:
    """Print formatted results table."""
    
    print("\n" + "=" * 140)
    print(f"FTP Arrival Time Profiler - {radar} ({date_str})")
    print(f"Strategy: {strategy}, Volume: {vol}, Field: {field}")
    print("=" * 140)
    
    if not files_metadata:
        print("\n✗ No files found matching criteria.")
        return
    
    print(f"\nFound {len(files_metadata)} files (sorted by FTP arrival time):\n")
    
    # Header
    print(
        f"{'#':<4} "
        f"{'Filename':<50} "
        f"{'Observation Time':<20} "
        f"{'FTP Arrival Time':<20} "
        f"{'Delay':<10} "
        f"{'Status':<10}"
    )
    print("-" * 140)
    
    out_of_order_count = 0
    
    for i, meta in enumerate(files_metadata, 1):
        obs_str = meta.obs_datetime.strftime("%Y-%m-%d %H:%M:%S")
        arr_str = meta.ftp_mtime.strftime("%Y-%m-%d %H:%M:%S")
        delay_str = format_delay(meta.arrival_delay_sec)
        
        status = "✓ OK" if not meta.is_out_of_order else "✗ OUT-OF-ORDER"
        if meta.is_out_of_order:
            out_of_order_count += 1
        
        print(
            f"{i:<4} "
            f"{meta.filename:<50} "
            f"{obs_str:<20} "
            f"{arr_str:<20} "
            f"{delay_str:<10} "
            f"{status:<10}"
        )
    
    # Summary statistics
    print("\n" + "=" * 140)
    print("Summary Statistics:")
    print("=" * 140)
    
    delays = [m.arrival_delay_sec for m in files_metadata]
    if delays:
        min_delay = min(delays)
        max_delay = max(delays)
        avg_delay = sum(delays) / len(delays)
        
        print(f"\n  Total files:          {len(files_metadata)}")
        print(f"  Out-of-order files:   {out_of_order_count} ({100*out_of_order_count/len(files_metadata):.1f}%)")
        print(f"\n  Min arrival delay:    {format_delay(min_delay)}")
        print(f"  Max arrival delay:    {format_delay(max_delay)}")
        print(f"  Avg arrival delay:    {format_delay(avg_delay)}")
        
        # Check for significant variability
        if max_delay - min_delay > 300:  # More than 5 minutes variance
            print(f"\n  ⚠ WARNING: High variance in arrival times ({format_delay(max_delay - min_delay)})")
            print(f"     This could indicate files arriving out of order or FTP timing issues.")
        
        if out_of_order_count > 0:
            print(f"\n  ✗ ALERT: {out_of_order_count} file(s) arrived out of chronological order!")
            print(f"     Files with older observation times arrived AFTER files with newer times.")
    
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Profile FTP arrival times for BUFR files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Profile vol01 DBZH files for 2026-04-17
  python3 scripts/profile_ftp_arrivals.py \\
    --radar RMA1 --date 2026-04-17 --strategy 0315 --vol 01 --field DBZH
  
  # Profile only hour 12 (noon)
  python3 scripts/profile_ftp_arrivals.py \\
    --radar RMA1 --date 2026-04-17 --strategy 0315 --vol 02 --field VRAD --hour 12
  
  # Docker execution (from host):
  docker exec genpro25-rma1 python3 /workspace/scripts/profile_ftp_arrivals.py \\
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
    
    # Validate volume
    if not args.vol.isdigit() or not (1 <= int(args.vol) <= 99):
        print(f"✗ Invalid volume: {args.vol} (use 01, 02, etc.)")
        sys.exit(1)
    
    # Validate hour if provided
    if args.hour:
        if not args.hour.isdigit() or not (0 <= int(args.hour) <= 23):
            print(f"✗ Invalid hour: {args.hour} (use 00-23)")
            sys.exit(1)
        args.hour = f"{int(args.hour):02d}"
    
    try:
        # Resolve credentials
        ftp_host, ftp_user, ftp_pass = resolve_ftp_credentials()
        
        # Connect to FTP
        logger.info(f"Connecting to FTP server {ftp_host}...")
        ftp = ftplib.FTP(ftp_host, timeout=30)
        ftp.login(ftp_user, ftp_pass)
        logger.info("Connected successfully")
        
        try:
            # Scan FTP directory
            files_metadata = scan_ftp_directory(
                ftp,
                radar=args.radar,
                date_str=args.date,
                strategy=args.strategy,
                vol=args.vol.zfill(2),
                field=args.field,
                hour=args.hour,
            )
            
            # Print results
            print_results(
                files_metadata,
                radar=args.radar,
                date_str=args.date,
                strategy=args.strategy,
                vol=args.vol,
                field=args.field,
            )
        
        finally:
            ftp.quit()
            logger.info("Disconnected from FTP server")
    
    except RuntimeError as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    import os
    main()