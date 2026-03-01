#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example: Integration with BUFR Processing

This example demonstrates how to integrate the FTP client/daemon with
BUFR file processing to create a complete workflow:
1. Download BUFR files from FTP server
2. Process them with the BUFR decoder
3. Track processed files to avoid reprocessing
"""
import asyncio
import os
from pathlib import Path

from radarlib import config
from radarlib.daemons.legacy import FTPDaemon, FTPDaemonConfig
from radarlib.io.ftp import FTPClient
from radarlib.state import FileStateTracker

# Configuration
radar_name = "RMA1"
FTP_HOST = config.FTP_HOST
FTP_USER = config.FTP_USER
FTP_PASSWORD = config.FTP_PASS
REMOTE_DIR = f"/L2/{radar_name}/2025/11/18/12/0239"
LOCAL_DIR = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name, "bufr"))


def simple_download_and_process():
    """
    Simple example: Download files and process them.

    This demonstrates a basic workflow without the daemon.
    """
    print("=" * 60)
    print("Simple Download and Process Example")
    print("=" * 60)

    # Setup
    client = FTPClient(host=FTP_HOST, user=FTP_USER, password=FTP_PASSWORD)
    local_dir = LOCAL_DIR
    local_dir.mkdir(parents=True, exist_ok=True)

    # Download files
    remote_dir = REMOTE_DIR
    print(f"\nListing files in {remote_dir}...")

    try:
        files = client.list_files(remote_dir)
        bufr_files = [f for f in files if f.endswith(".BUFR")]

        print(f"Found {len(bufr_files)} BUFR files")

        # Download first file as example
        if bufr_files:
            filename = bufr_files[0]
            remote_path = f"{remote_dir}/{filename}"
            local_path = local_dir / filename

            print(f"\nDownloading: {filename}")
            client.download_file(remote_path, local_path)
            print(f"Saved to: {local_path}")

            # Process the file (example placeholder)
            print(f"\nProcessing {filename}...")
            process_bufr_file(local_path)

    except Exception as e:
        print(f"Error: {e}")

    print("\n" + "=" * 60)


def process_bufr_file(filepath: Path):
    """
    Process a BUFR file.

    This is a placeholder - in a real application, you would:
    1. Use radarlib.io.bufr.bufr_to_dict() to decode the file
    2. Use radarlib.io.bufr.bufr_to_pyart() to convert to radar object
    3. Generate products (PNG, GeoTIFF, etc.)
    4. Store results
    """
    print(f"  [Placeholder] Would process: {filepath.name}")
    print(f"  File size: {filepath.stat().st_size} bytes")

    # Example of what you might do:
    # from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart
    # bufr_dict = bufr_to_dict(str(filepath))
    # radar = bufr_to_pyart([bufr_dict])
    # ... process radar data ...


class BUFRProcessingDaemon(FTPDaemon):
    """
    Custom daemon that processes BUFR files after downloading.

    This extends the base FTPDaemon to add processing logic.
    """

    def __init__(self, config: FTPDaemonConfig, processing_callback=None):
        super().__init__(config)
        self.processing_callback = processing_callback

    async def _download_file_async(self, remote_path: str) -> bool:
        """Override to add processing after download."""
        # Download the file
        success = await super()._download_file_async(remote_path)

        if success and self.processing_callback:
            # Process the file
            filename = Path(remote_path).name
            local_path = self.config.local_download_dir / filename

            try:
                print(f"Processing {filename}...")
                await asyncio.get_event_loop().run_in_executor(None, self.processing_callback, local_path)
                print(f"Processed {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

        return success


def daemon_with_processing():
    """
    Example: Run daemon with automatic processing.

    This shows how to extend the daemon to process files as they're downloaded.
    """
    print("=" * 60)
    print("FTP Daemon with BUFR Processing")
    print("=" * 60)

    # Configure daemon
    daemon_config = FTPDaemonConfig(
        host=FTP_HOST,
        username=FTP_USER,
        password=FTP_PASSWORD,
        remote_base_path=f"/L2/{radar_name}",
        local_download_dir=LOCAL_DIR,
        state_file=Path(config.ROOT_RADAR_FILES_PATH) / radar_name / "state.db",
        poll_interval=60,
        max_concurrent_downloads=2,
    )

    # Create processing daemon
    daemon = BUFRProcessingDaemon(daemon_config, processing_callback=process_bufr_file)

    print("\nDaemon configured with automatic BUFR processing")
    print("Press Ctrl+C to stop\n")
    print("-" * 60)

    # Run daemon
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped")


def batch_process_from_state():
    """
    Example: Process files that were downloaded but not yet processed.

    This is useful for reprocessing or if processing failed previously.
    """
    print("=" * 60)
    print("Batch Process from State File")
    print("=" * 60)

    # Load state
    state_file = Path(config.ROOT_RADAR_FILES_PATH) / radar_name / "state.db"
    if not state_file.exists():
        print("\nNo state file found. Run the daemon first.")
        return

    tracker = FileStateTracker(state_file)
    local_dir = Path("./daemon_downloads")

    print(f"\nFound {tracker.count()} downloaded files")

    # Process each downloaded file
    processed = 0
    for filename in tracker.get_downloaded_files():
        local_path = local_dir / filename
        if local_path.exists():
            print(f"\nProcessing: {filename}")
            process_bufr_file(local_path)
            processed += 1
        else:
            print(f"\nWarning: {filename} in state but not found locally")

    print(f"\nProcessed {processed} files")
    print("\n" + "=" * 60)


def selective_download():
    """
    Example: Download only specific field types or radars.

    This shows how to filter files before downloading.
    """
    print("=" * 60)
    print("Selective Download Example")
    print("=" * 60)

    from radarlib.io.ftp.ftp import parse_ftp_path

    client = FTPClient(host=FTP_HOST, user=FTP_USER, password=FTP_PASSWORD)
    local_dir = LOCAL_DIR
    local_dir.mkdir(parents=True, exist_ok=True)
    tracker = FileStateTracker(Path(config.ROOT_RADAR_FILES_PATH) / radar_name / "state.db")

    # Target specific radar and field
    target_radar = radar_name
    target_field = "DBZH"  # Reflectivity
    remote_dir = "/L2"

    print(f"\nLooking for {target_field} files from radar {target_radar}")

    try:
        files = client.list_files(remote_dir)

        for item in files:
            # Handle both nlst and mlsd
            filename = item if isinstance(item, str) else item[0]

            # Skip if already downloaded
            if tracker.is_downloaded(filename):
                continue

            # Check if it's a BUFR file
            if not filename.endswith(".BUFR"):
                continue

            # Parse file info
            try:
                # Build full path for parsing
                full_path = f"{remote_dir}/{filename}"
                file_info = parse_ftp_path(full_path)

                # Filter by radar and field
                if file_info["radar_code"] == target_radar and file_info["field_type"] == target_field:

                    print(f"\nDownloading: {filename}")
                    print(f"  Radar: {file_info['radar_code']}")
                    print(f"  Field: {file_info['field_type']}")
                    print(f"  Time: {file_info['datetime']}")

                    local_path = local_dir / filename
                    client.download_file(full_path, local_path)
                    tracker.mark_downloaded(filename, full_path, metadata=file_info)

            except Exception as e:
                print(f"Error processing {filename}: {e}")

    except Exception as e:
        print(f"Error: {e}")

    print(f"\nDownloaded {tracker.count()} matching files")
    print("\n" + "=" * 60)


if __name__ == "__main__":
    # Uncomment the example you want to run:

    # Simple download and process
    simple_download_and_process()

    # Daemon with automatic processing
    # daemon_with_processing()

    # Batch process from state
    # batch_process_from_state()

    # Selective download by radar/field
    # selective_download()
