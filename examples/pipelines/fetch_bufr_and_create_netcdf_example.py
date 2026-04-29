"""
fetch_bufr_and_create_netcdf_example.py — End-to-end BUFR to NetCDF conversion.

This example demonstrates how to fetch BUFR files from an FTP server for a
specific radar, strategy, and volume number, then convert them to a NetCDF file.

It reuses the same functions and utilities as the download and processing daemons,
but as a straightforward synchronous workflow without running the full daemons.

Usage
-----
python examples/fetch_bufr_and_create_netcdf_example.py \\
    --radar RMA1 \\
    --strategy 0315 \\
    --vol-nr 01 \\
    --fields DBZH DBZV \\
    --output-dir ./generated_netcdf/ \\
    --timestamp 2026-04-28T14:00:00Z

Or use defaults to find the latest available volume:
python examples/fetch_bufr_and_create_netcdf_example.py \\
    --radar RMA1 \\
    --strategy 0315 \\
    --vol-nr 01 \\
    --fields DBZH
"""

import argparse
import asyncio
import gc
import logging
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pyart

# ---------------------------------------------------------------------------
# Make sure the package root is importable when run directly from the repo
# ---------------------------------------------------------------------------
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root / "src"))

from radarlib import config
from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.pyart_writer import bufr_fields_to_pyart_radar
from radarlib.io.ftp.ftp_client import RadarFTPClientAsync
from radarlib.utils.memory_profiling import log_memory_usage
from radarlib.utils.names_utils import (
    build_vol_types_regex,
    extract_bufr_filename_components,
    get_netcdf_filename_from_bufr_filename,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration and CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Fetch BUFR files from FTP and convert to NetCDF.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--radar",
        type=str,
        required=True,
        help="Radar name (e.g., 'RMA1')",
    )
    p.add_argument(
        "--strategy",
        type=str,
        required=True,
        help="Strategy code (e.g., '0315')",
    )
    p.add_argument(
        "--vol-nr",
        type=str,
        required=True,
        help="Volume number (e.g., '01')",
    )
    p.add_argument(
        "--fields",
        type=str,
        nargs="+",
        required=True,
        help="Field names to fetch (e.g., DBZH DBZV)",
    )
    p.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help=(
            "ISO 8601 timestamp to search for (e.g., '2026-04-28T14:00:00Z'). "
            "If not provided, uses the latest available timestamp with all fields."
        ),
    )
    p.add_argument(
        "--output-dir",
        type=str,
        default="./netcdf_output",
        help="Directory to save the generated NetCDF file.",
    )
    p.add_argument(
        "--ftp-host",
        type=str,
        default=config.FTP_HOST,
        help="FTP server hostname.",
    )
    p.add_argument(
        "--ftp-user",
        type=str,
        default=config.FTP_USER,
        help="FTP username.",
    )
    p.add_argument(
        "--ftp-password",
        type=str,
        default=config.FTP_PASS,
        help="FTP password.",
    )
    p.add_argument(
        "--bufr-resources",
        type=str,
        default=None,
        help="Path to BUFR resources directory (for decoding tables).",
    )
    p.add_argument(
        "--lookback-hours",
        type=int,
        default=24,
        help=(
            "Number of hours to look back from now when searching for files "
            "(if --timestamp is not provided)."
        ),
    )
    p.add_argument(
        "--max-age-minutes",
        type=int,
        default=None,
        help=(
            "Maximum age of files in minutes. Used to find latest timestamp. "
            "If None, no age restriction."
        ),
    )
    p.add_argument(
        "--ftp-base-path",
        type=str,
        default="L2",
        help=(
            "Base path on FTP server for BUFR files. "
            "Can be 'L2', 'R2', or a full path like '/L2' or '/data/bufr'. "
            "Default: 'L2' (results in /L2/RADAR/YYYY/MM/DD/HH/MMSS)"
        ),
    )
    return p


# ---------------------------------------------------------------------------
# Fetching logic
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Utility functions for timestamp handling and FTP traversal
# ---------------------------------------------------------------------------

def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO 8601 timestamp string to datetime.

    Parameters
    ----------
    timestamp_str : str or None
        ISO 8601 timestamp string (e.g., '2026-04-28T14:00:00Z')

    Returns
    -------
    datetime or None
        Parsed datetime object, or None if input is None
    """
    if timestamp_str is None:
        return None

    try:
        # Try parsing with timezone
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        # Try parsing without timezone
        dt = datetime.fromisoformat(timestamp_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


def format_timestamp_for_ftp(dt: datetime) -> str:
    """
    Format a datetime object into the FTP folder structure: YYYY/MM/DD/HH/MMSS

    Parameters
    ----------
    dt : datetime
        Datetime object to format

    Returns
    -------
    str
        Formatted path segment (YYYY/MM/DD/HH/MMSS)
    """
    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"
    hour = f"{dt.hour:02d}"
    minute = f"{dt.minute:02d}"
    second = f"{dt.second:02d}"
    return f"{year}/{month}/{day}/{hour}/{minute}{second}"


async def try_timestamp_directory(
    ftp_client: RadarFTPClientAsync,
    radar: str,
    strategy: str,
    vol_nr: str,
    fields: List[str],
    timestamp: datetime,
    ftp_base_path: str = "L2",
) -> Optional[Dict[str, str]]:
    """
    Try to find BUFR files for a specific timestamp by constructing the direct path.

    Parameters
    ----------
    ftp_client : RadarFTPClientAsync
        Connected FTP client
    radar : str
        Radar name
    strategy : str
        Strategy code
    vol_nr : str
        Volume number
    fields : list of str
        Required field names
    timestamp : datetime
        Target timestamp
    ftp_base_path : str
        Base path on FTP server (default: 'L2', e.g., '/L2/RMA1/...')
        Can be 'L2', 'R2', or a full path.

    Returns
    -------
    dict or None
        Mapping of field_name -> remote_path if all fields found, None otherwise
    """
    ftp_path_segment = format_timestamp_for_ftp(timestamp)
    # Construct FTP directory path, handling various base path formats
    if ftp_base_path.startswith("/"):
        # Absolute path provided
        ftp_dir = f"{ftp_base_path.rstrip('/')}/{radar}/{ftp_path_segment}"
    else:
        # Relative to root
        ftp_dir = f"/{ftp_base_path.rstrip('/')}/{radar}/{ftp_path_segment}"

    logger.debug(f"Trying timestamp directory: {ftp_dir}")

    try:
        # List files in the directory
        def list_dir_sync():
            return ftp_client.list_dir(ftp_dir)

        files_in_dir = await asyncio.to_thread(list_dir_sync)

        if not files_in_dir:
            logger.debug(f"Directory empty or not found: {ftp_dir}")
            return None

        # Build vol_types filter for this directory
        vol_types = {strategy: {vol_nr: fields}}
        vol_types_regex = build_vol_types_regex(vol_types)

        found_files = {field: None for field in fields}

        for fname in files_in_dir:
            # Check if filename matches our vol_types filter
            if not vol_types_regex.match(fname):
                continue

            # Parse filename to extract field
            bufr_meta = extract_bufr_filename_components(fname)
            field = bufr_meta.get("field_type")

            if field in found_files and found_files[field] is None:
                ftp_full_path = f"{ftp_dir}/{fname}"
                logger.debug(f"Found {field} at {timestamp.isoformat()}: {ftp_full_path}")
                found_files[field] = ftp_full_path

        # Check if all fields were found
        if all(path is not None for path in found_files.values()):
            logger.info(f"Found all fields at timestamp {timestamp.isoformat()}")
            return found_files

        # Log which fields are missing
        missing = [f for f, p in found_files.items() if p is None]
        logger.debug(f"Missing fields at {timestamp.isoformat()}: {missing}")
        return None

    except Exception as e:
        logger.debug(f"Error checking {ftp_dir}: {e}")
        return None


async def find_nearby_timestamps(
    ftp_client: RadarFTPClientAsync,
    radar: str,
    strategy: str,
    vol_nr: str,
    fields: List[str],
    target_timestamp: datetime,
    max_offset_minutes: int = 30,
    step_minutes: int = 5,
    ftp_base_path: str = "L2",
) -> Optional[Dict[str, str]]:
    """
    Search for BUFR files at timestamps near the target timestamp.

    Tries progressively larger offsets (±5min, ±10min, ±15min, ±20min, etc.)
    until all fields are found or max offset is exceeded.

    Parameters
    ----------
    ftp_client : RadarFTPClientAsync
        Connected FTP client
    radar : str
        Radar name
    strategy : str
        Strategy code
    vol_nr : str
        Volume number
    fields : list of str
        Required field names
    target_timestamp : datetime
        Target observation timestamp
    max_offset_minutes : int
        Maximum minutes to search away from target
    step_minutes : int
        Increment for search (try ±step_minutes, ±2*step_minutes, etc.)
    ftp_base_path : str
        Base path on FTP server (default: 'L2')

    Returns
    -------
    dict or None
        Found files, or None if not found within offset
    """
    logger.info(
        f"Searching nearby timestamps (±{max_offset_minutes} min) for {radar} {strategy}-{vol_nr}"
    )

    # Try offsets in both directions: 0, ±5, ±10, ±15, ±20, ±25, ±30
    offsets_to_try = [0]
    for offset in range(step_minutes, max_offset_minutes + 1, step_minutes):
        offsets_to_try.append(offset)
        offsets_to_try.append(-offset)

    for offset_minutes in offsets_to_try:
        search_timestamp = target_timestamp + timedelta(minutes=offset_minutes)

        logger.debug(
            f"Searching offset {offset_minutes:+3d} min: {search_timestamp.isoformat()}"
        )

        result = await try_timestamp_directory(
            ftp_client, radar, strategy, vol_nr, fields, search_timestamp, ftp_base_path
        )

        if result is not None:
            logger.info(
                f"Found all fields at offset {offset_minutes:+d} min: "
                f"{search_timestamp.isoformat()}"
            )
            return result

    logger.warning(
        f"Could not find all fields within ±{max_offset_minutes} minutes of "
        f"{target_timestamp.isoformat()}"
    )
    return None


async def find_bufr_files_for_timestamp(
    ftp_client: RadarFTPClientAsync,
    radar: str,
    strategy: str,
    vol_nr: str,
    fields: List[str],
    target_timestamp: datetime,
    lookback_hours: int = 24,
    max_fallback_minutes: int = 30,
    ftp_base_path: str = "L2",
) -> Dict[str, str]:
    """
    Find BUFR files for all specified fields at a given timestamp.

    First tries the exact timestamp directory. If some fields are missing,
    searches nearby timestamps (±5, ±10, ±15, ±20, ±25, ±30 minutes).

    Parameters
    ----------
    ftp_client : RadarFTPClientAsync
        Connected FTP client
    radar : str
        Radar name (e.g., 'RMA1')
    strategy : str
        Strategy code (e.g., '0315')
    vol_nr : str
        Volume number (e.g., '01')
    fields : list of str
        Field names to find (e.g., ['DBZH', 'DBZV'])
    target_timestamp : datetime or None
        Target observation timestamp. If None, searches from now.
    lookback_hours : int
        Unused (kept for backward compatibility)
    max_fallback_minutes : int
        Maximum minutes to search away from target if exact not found
    ftp_base_path : str
        Base path on FTP server (default: 'L2', e.g., '/L2/RMA1/...')

    Returns
    -------
    dict
        Mapping of field_name -> remote_path for all found files.

    Raises
    ------
    ValueError
        If not all fields can be found at target timestamp or nearby
    """
    logger.info(
        f"Searching for BUFR files: {radar} {strategy}-{vol_nr} "
        f"fields={fields} timestamp={target_timestamp} ftp_base={ftp_base_path}"
    )

    # First, try the exact timestamp directory
    exact_result = await try_timestamp_directory(
        ftp_client, radar, strategy, vol_nr, fields, target_timestamp, ftp_base_path
    )

    if exact_result is not None:
        logger.info(f"Found all fields at exact timestamp {target_timestamp.isoformat()}")
        return exact_result

    logger.info(
        f"Could not find all fields at exact timestamp {target_timestamp.isoformat()}. "
        f"Searching nearby timestamps..."
    )

    # Fall back to searching nearby timestamps
    fallback_result = await find_nearby_timestamps(
        ftp_client,
        radar,
        strategy,
        vol_nr,
        fields,
        target_timestamp,
        max_offset_minutes=max_fallback_minutes,
        ftp_base_path=ftp_base_path,
    )

    if fallback_result is not None:
        return fallback_result

    # If still not found, raise an error
    raise ValueError(
        f"Could not find all required fields ({fields}) for {radar} {strategy}-{vol_nr} "
        f"at timestamp {target_timestamp.isoformat()} or nearby (±{max_fallback_minutes} min)"
    )


async def find_latest_bufr_timestamp(
    ftp_client: RadarFTPClientAsync,
    radar: str,
    strategy: str,
    vol_nr: str,
    fields: List[str],
    lookback_hours: int = 24,
) -> datetime:
    """
    Find the latest timestamp with all required fields available.

    Parameters
    ----------
    ftp_client : RadarFTPClientAsync
        Connected FTP client
    radar : str
        Radar name (e.g., 'RMA1')
    strategy : str
        Strategy code (e.g., '0315')
    vol_nr : str
        Volume number (e.g., '01')
    fields : list of str
        Required field names
    lookback_hours : int
        Number of hours to look back from now

    Returns
    -------
    datetime
        Timestamp of latest volume with all fields
    """
    logger.info(
        f"Searching for latest timestamp with all fields: {radar} {strategy}-{vol_nr}"
    )

    # Use find_bufr_files_at_closest_time which handles this efficiently
    result = await find_bufr_files_for_timestamp(
        ftp_client, radar, strategy, vol_nr, fields,
        target_timestamp=None,  # Searches from now
        lookback_hours=lookback_hours
    )
    
    # The function will raise if not found, so if we get here we found it
    # We just need the timestamp from the filename
    # For now, return the time we found it at
    if result:
        # Extract timestamp from any of the remote paths
        first_path = list(result.values())[0]
        logger.info(f"Found latest timestamp from path: {first_path}")
        # Parse timestamp from path: /L2/RADAR/YYYY/MM/DD/HH/MMSS/FILE.BUFR
        parts = first_path.split('/')
        if len(parts) >= 8:
            year, month, day, hour, minumsec = parts[-7], parts[-6], parts[-5], parts[-4], parts[-3]
            minute = int(minumsec[:2])
            second = int(minumsec[2:]) if len(minumsec) > 2 else 0
            dt = datetime(int(year), int(month), int(day), int(hour), minute, second, tzinfo=timezone.utc)
            return dt
    
    raise ValueError(f"Could not determine timestamp for {radar} {strategy}-{vol_nr}")


async def download_bufr_files(
    ftp_client: RadarFTPClientAsync,
    bufr_paths: Dict[str, str],
    local_dir: Path,
) -> Dict[str, Path]:
    """
    Download BUFR files from FTP to local directory.

    Parameters
    ----------
    ftp_client : RadarFTPClientAsync
        Connected FTP client
    bufr_paths : dict
        Mapping of field_name -> remote_path
    local_dir : Path
        Local directory to download to

    Returns
    -------
    dict
        Mapping of field_name -> local_path
    """
    local_paths = {}

    for field, remote_path in bufr_paths.items():
        remote_path_obj = Path(remote_path)
        local_path = local_dir / remote_path_obj.name

        logger.info(f"Downloading {field}: {remote_path} -> {local_path}")
        try:
            await ftp_client.download_file_async(
                Path(remote_path), local_path
            )
            local_paths[field] = local_path
        except Exception as e:
            logger.error(f"Failed to download {field}: {e}")
            raise

    return local_paths


async def decode_bufr_files(
    bufr_paths: Dict[str, Path],
    bufr_resources: Optional[str] = None,
) -> List[Dict]:
    """
    Decode BUFR files from disk.

    Parameters
    ----------
    bufr_paths : dict
        Mapping of field_name -> local_path
    bufr_resources : str or None
        Path to BUFR resources directory

    Returns
    -------
    list of dict
        Decoded BUFR data for each file
    """
    decoded_fields = []

    log_memory_usage("Before BUFR decoding")
    logger.info(f"Decoding {len(bufr_paths)} BUFR files...")

    for field, bufr_path in bufr_paths.items():
        try:
            logger.debug(f"Decoding {field} from {bufr_path}")
            decoded = bufr_to_dict(
                str(bufr_path),
                root_resources=bufr_resources,
            )
            if decoded:
                decoded_fields.append(decoded)
                logger.info(f"Successfully decoded {field}")
            else:
                logger.warning(f"Failed to decode {bufr_path}")
        except Exception as e:
            logger.error(f"Error decoding {bufr_path}: {e}")
            raise

    if not decoded_fields:
        raise ValueError("No BUFR files could be decoded")

    logger.info(f"Successfully decoded {len(decoded_fields)} fields")
    log_memory_usage("After all BUFR files decoded")

    return decoded_fields


def create_radar_from_decoded_bufr(decoded_fields: List[Dict]) -> pyart.core.Radar:
    """
    Create a PyART Radar object from decoded BUFR data.

    Parameters
    ----------
    decoded_fields : list of dict
        Decoded BUFR data from bufr_to_dict

    Returns
    -------
    pyart.core.Radar
        Radar object with all decoded fields
    """
    logger.info("Creating PyART Radar object from decoded BUFR data...")
    logger.debug(f"Building radar from {len(decoded_fields)} field(s)")

    radar = bufr_fields_to_pyart_radar(decoded_fields)

    if radar is None:
        raise ValueError("Failed to create Radar object from decoded BUFR fields")

    # Clean up memory
    del decoded_fields
    gc.collect()
    log_memory_usage("After PyART Radar object creation (decoded_fields freed)")

    return radar


def save_radar_to_netcdf(radar: pyart.core.Radar, output_path: Path) -> Path:
    """
    Save PyART Radar object to NetCDF file.

    Parameters
    ----------
    radar : pyart.core.Radar
        Radar object to save
    output_path : Path
        Output file path

    Returns
    -------
    Path
        Path to saved NetCDF file
    """
    logger.info(f"Saving radar to NetCDF: {output_path}")

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pyart.io.write_cfradial(str(output_path), radar)

    # Clean up memory
    del radar
    gc.collect()
    log_memory_usage("After NetCDF write and radar cleanup")

    logger.info(f"Successfully saved NetCDF file: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

async def fetch_bufr_and_create_netcdf(
    radar: str,
    strategy: str,
    vol_nr: str,
    fields: List[str],
    output_dir: Path,
    timestamp: Optional[datetime] = None,
    ftp_host: str = config.FTP_HOST,
    ftp_user: str = config.FTP_USER,
    ftp_password: str = config.FTP_PASS,
    bufr_resources: Optional[str] = None,
    lookback_hours: int = 24,
    max_age_minutes: Optional[int] = None,
    ftp_base_path: str = "L2",
) -> Path:
    """
    Complete end-to-end workflow: fetch BUFR from FTP and create NetCDF.

    Parameters
    ----------
    radar : str
        Radar name (e.g., 'RMA1')
    strategy : str
        Strategy code (e.g., '0315')
    vol_nr : str
        Volume number (e.g., '01')
    fields : list of str
        Field names to fetch
    output_dir : Path
        Directory to save NetCDF file
    timestamp : datetime or None
        Target observation timestamp. If None, finds the latest.
    ftp_host, ftp_user, ftp_password : str
        FTP connection credentials
    bufr_resources : str or None
        Path to BUFR resources directory
    lookback_hours : int
        Hours to look back from now
    max_age_minutes : int or None
        Maximum age of files
    ftp_base_path : str
        Base path on FTP server (default: 'L2')

    Returns
    -------
    Path
        Path to the generated NetCDF file
    """
    # ------------------------------------------------------------------
    # Step 0 — Setup temporary directory for BUFR files
    # ------------------------------------------------------------------
    with tempfile.TemporaryDirectory(prefix="radarlib_bufr_") as temp_dir:
        temp_dir_path = Path(temp_dir)

        # ------------------------------------------------------------------
        # Step 1 — Connect to FTP and find files
        # ------------------------------------------------------------------
        logger.info(f"Connecting to FTP server: {ftp_host}")
        async with RadarFTPClientAsync(
            host=ftp_host,
            user=ftp_user,
            password=ftp_password,
            base_dir=ftp_base_path,
        ) as ftp_client:

            # Determine target timestamp
            if timestamp is None:
                logger.info("No timestamp provided, finding latest with all fields...")
                for field in fields:
                    last_bufr = ftp_client.find_last_bufr_file(radar=radar, strategy=strategy, volume_nr=vol_nr, field=field, search_from_time=None)
                    if last_bufr:
                        break
                if not last_bufr:
                    raise ValueError(f"No BUFR files found for {radar} {strategy}-{vol_nr} with fields {fields}")
                timestamp = last_bufr.datetime

            logger.info(f"Target timestamp: {timestamp.isoformat()}")

            # Find BUFR files for all fields
            bufr_remote_paths = await find_bufr_files_for_timestamp(
                ftp_client,
                radar,
                strategy,
                vol_nr,
                fields,
                timestamp,
                lookback_hours=lookback_hours,
            )
            logger.info(f"Found BUFR files: {len(bufr_remote_paths)} files")
            for field, path in bufr_remote_paths.items():
                logger.info(f"  {field}: {path}")

            # ------------------------------------------------------------------
            # Step 2 — Download BUFR files
            # ------------------------------------------------------------------
            bufr_local_paths = await download_bufr_files(
                ftp_client, bufr_remote_paths, temp_dir_path
            )
            logger.info(f"Downloaded {len(bufr_local_paths)} BUFR files")

        # ------------------------------------------------------------------
        # Step 3 — Decode BUFR files
        # ------------------------------------------------------------------
        decoded_fields = await decode_bufr_files(bufr_local_paths, bufr_resources)

        # ------------------------------------------------------------------
        # Step 4 — Create PyART Radar object
        # ------------------------------------------------------------------
        radar_obj = create_radar_from_decoded_bufr(decoded_fields)

        # ------------------------------------------------------------------
        # Step 5 — Generate output filename
        # ------------------------------------------------------------------
        # Use the first BUFR filename to generate the NetCDF filename
        first_bufr_name = list(bufr_local_paths.values())[0].name
        netcdf_filename = get_netcdf_filename_from_bufr_filename(first_bufr_name)
        output_path = Path(output_dir) / netcdf_filename

        # ------------------------------------------------------------------
        # Step 6 — Save to NetCDF
        # ------------------------------------------------------------------
        save_radar_to_netcdf(radar_obj, output_path)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"SUCCESS: Created NetCDF file")
        logger.info(f"  Location: {output_path}")
        logger.info(f"  Radar: {radar} {strategy}-{vol_nr}")
        logger.info(f"  Timestamp: {timestamp.isoformat()}")
        logger.info(f"  Fields: {fields}")
        logger.info(f"{'=' * 60}\n")

        return output_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Main entry point."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _build_parser().parse_args()

    try:
        # Parse timestamp if provided
        target_timestamp = parse_timestamp(args.timestamp)

        # Run the workflow
        output_path = await fetch_bufr_and_create_netcdf(
            radar=args.radar,
            strategy=args.strategy,
            vol_nr=args.vol_nr,
            fields=args.fields,
            output_dir=Path(args.output_dir),
            timestamp=target_timestamp,
            ftp_host=args.ftp_host,
            ftp_user=args.ftp_user,
            ftp_password=args.ftp_password,
            bufr_resources=args.bufr_resources,
            lookback_hours=args.lookback_hours,
            max_age_minutes=args.max_age_minutes,
            ftp_base_path=args.ftp_base_path,
        )

        logger.info(f"Output saved to: {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
