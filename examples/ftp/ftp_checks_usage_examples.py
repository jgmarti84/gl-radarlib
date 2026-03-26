#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Usage Examples for Updated FTP BUFR File Availability Checker

This module demonstrates the new features and improvements in ftp_checks.py,
including proper handling of the date-based FTP folder structure and filtering
capabilities.

FTP Folder Structure:
    L2 / {radar_name} / YYYY / MM / DD / HH / MMSS / *.BUFR

BUFR Filename Format:
    {radar_name}_{strategy}_{volume}_{field}_YYYYMMDDTHHMMSSZ.BUFR
    Example: RMA1_0315_01_DBZV_20251020T152828Z.BUFR
"""

import asyncio

from radarlib.io.bufr.bufr import BUFRFilename
from radarlib.io.ftp.ftp_client import RadarFTPClient, RadarFTPClientAsync


# ==============================================================================
# Example 1: Parsing BUFR Filenames
# ==============================================================================
def example_parse_bufr_filename():
    """Demonstrate BUFR filename parsing."""
    print("\n" + "=" * 70)
    print("Example 1: Parsing BUFR Filenames")
    print("=" * 70)

    filename = "RMA1_0315_01_DBZV_20251020T152828Z.BUFR"

    try:
        parsed = BUFRFilename(filename)
        print(f"✓ Parsed: {filename}")
        print(f"  Radar Name:  {parsed.radar_name}")
        print(f"  Strategy:    {parsed.strategy}")
        print(f"  Volume:      {parsed.volume}")
        print(f"  Field:       {parsed.field}")
        print(f"  Datetime:    {parsed.datetime}")
    except ValueError as e:
        print(f"✗ Error: {e}")


# ==============================================================================
# Example 2: Find Last Available BUFR File
# ==============================================================================
def example_find_last_file(host: str, user: str, password: str):
    """Find the most recent BUFR file with optional filters."""
    base_path = "L2"

    print("\n" + "=" * 70)
    print("Example 2: Find Last Available BUFR File")
    print("=" * 70)

    client = RadarFTPClient(host, user, password, base_dir=base_path)
    # checker = BUFRAvailabilityChecker(host, user, password)

    # Find the last file for a specific radar and field
    print("\n2a. Find last BUFR file for RMA1 with DBZV field:")
    file_info = client.find_last_bufr_file(radar="RMA1", field="DBZV")

    if file_info:
        print("✓ Last file found:")
        print(f"  Filename:    {file_info.filename}")
        print(f"  Datetime:    {file_info.datetime}")
        print(f"  Remote path: {file_info.remote_path}")
        print(f"  Strategy:    {file_info.strategy}")
        print(f"  Volume:      {file_info.volume}")
        print(f"  Field:       {file_info.field}")
    else:
        print("✗ No file found")

    # Find last file with specific strategy, volume, and field
    print("\n2b. Find last BUFR file with specific strategy, volume, and field:")
    file_info = client.find_last_bufr_file(radar="RMA1", strategy="0315", volume_nr="01", field="DBZV")

    if file_info:
        print("✓ Last file found:")
        print(f"  Filename: {file_info.filename}")
        print(f"  Datetime: {file_info.datetime}")
    else:
        print("✗ No file found")


# ==============================================================================
# Example 9: File Existence Check
# ==============================================================================
def example_file_exists(host: str, user: str, password: str):
    """Check if specific files exist on the FTP server."""
    print("\n" + "=" * 70)
    print("Example 9: Check File Existence")
    print("=" * 70)
    base_path = "L2"
    client = RadarFTPClient(host, user, password, base_dir=base_path)
    # checker = BUFRAvailabilityChecker(host, user, password)

    # Check single file
    remote_path = "/L2/RMA1/2025/10/20/15/3045/RMA1_0315_01_DBZV_20251020T153045Z.BUFR"
    print(f"\nChecking file: {remote_path}")
    exists = client.file_exists(remote_path)
    status = "✓ Exists" if exists else "✗ Does not exist"
    print(f"Status: {status}")


async def example_files_exist_async(host: str, user: str, password: str):
    """Check multiple files exist in parallel with fresh connections."""
    base_path = "L2"
    async with RadarFTPClientAsync(host, user, password, base_dir=base_path) as client:
        files_to_check = [
            "/L2/RMA1/2025/10/20/15/3045/RMA1_0315_01_DBZV_20251020T153045Z.BUFR",
            "/L2/RMA1/2025/10/20/15/1807/RMA1_0315_03_DBZH_20251020T151807Z.BUFR",
            "/L2/RMA1/2025/10/20/15/3242/RMA1_0315_02_DBZV_20251020T153242Z.BUFR",
        ]
        print("\nExample: Parallel File Existence Check")
        print("-" * 75)
        results = await client.files_exist_parallel(files_to_check)

        exist_count = sum(1 for _, exists in results if exists)
        print(f"Checked {len(results)} files, {exist_count} exist:")
        print()
        for path, exists in results:
            status = "✓ Exists   " if exists else "✗ Missing  "
            print(f"  {status} {path}")

        return results


async def find_latest_files_for_multiple_radars(host: str, user: str, password: str):
    """Find latest BUFR files for multiple radars in parallel with fresh connections."""
    base_path = "L2"
    radars = ["RMA1", "RMA2", "RMA3"]

    async with RadarFTPClientAsync(host, user, password, base_dir=base_path) as client:
        print("\nExample: Parallel Last File Search Across Multiple Radars")
        print("-" * 75)
        results = await client.find_last_bufr_files_parallel(radars=radars, strategy="0315", field="DBZV")

        found_count = sum(1 for _, file_info in results if file_info is not None)
        print(f"Searched {len(radars)} radars, found {found_count} files:")
        print()
        for radar, file_info in results:
            if file_info:
                print(f"  ✓ {radar}: {file_info.filename}")
                print(f"           └─ DateTime: {file_info.datetime}")
                print(f"               Strategy: {file_info.strategy}, Volume: {file_info.volume}")
            else:
                print(f"  ✗ {radar}: No file found")

        return results


# ==============================================================================
# Async Runner Helper
# ==============================================================================
def run_async_example(async_func, *args):
    """Helper to run async functions in synchronous context."""
    try:
        asyncio.run(async_func(*args))
    except Exception as e:
        print(f"✗ Error: {e}")


if __name__ == "__main__":
    import app.config as config  # type: ignore

    # Configuration
    HOST = config.FTP_HOST  # type: ignore
    USER = config.FTP_USER  # type: ignore
    PASSWORD = config.FTP_PASS  # type: ignore

    # Note: Replace with actual FTP credentials and uncomment examples to run

    print("=" * 70)
    print("FTP BUFR Availability Checker - Usage Examples")
    print("=" * 70)
    print("\nNote: These examples demonstrate the API usage.")
    print("To run examples, replace FTP credentials with actual values.")

    # # Run example 1 (doesn't require FTP connection)
    # example_parse_bufr_filename()

    # # # To run other examples, uncomment below and provide valid credentials:
    # example_find_last_file(HOST, USER, PASSWORD)
    # example_file_exists(HOST, USER, PASSWORD)

    # ====== ASYNC EXAMPLES ======
    # Uncomment to run async examples (require valid FTP credentials and async environment)
    # These examples demonstrate significant performance improvements for multi-hour range checks
    run_async_example(example_files_exist_async, HOST, USER, PASSWORD)
    run_async_example(find_latest_files_for_multiple_radars, HOST, USER, PASSWORD)
    # run_async_example(example_check_availability_range_async, HOST, USER, PASSWORD)
    # run_async_example(example_check_availability_range_async_filtered, HOST, USER, PASSWORD)
