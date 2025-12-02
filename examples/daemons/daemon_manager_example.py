#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example: Simple Daemon Manager

This example demonstrates how to use the DaemonManager for easy control
of both download and processing daemons.
"""

import asyncio
import logging
import os
from pathlib import Path

from radarlib import config
from radarlib.daemons import DaemonManager, DaemonManagerConfig

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def example_basic_daemon_manager():
    """
    Example: Basic daemon manager usage.

    This shows the simplest way to start both daemons together.
    """
    print("=" * 60)
    print("Basic Daemon Manager Example")
    print("=" * 60)

    # Define volume types
    volume_types = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        },
        "0200": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP", "CM"]},
    }

    radar_name = "RMA3"
    base_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name))

    # Create manager configuration
    manager_config = DaemonManagerConfig(
        radar_name=radar_name,
        base_path=base_path,
        ftp_host=config.FTP_HOST,
        ftp_user=config.FTP_USER,
        ftp_password=config.FTP_PASS,
        ftp_base_path="/L2",
        volume_types=volume_types,
        # start_date=datetime(2025, 11, 25, 10, 0, 0, tzinfo=timezone.utc),
        download_poll_interval=60,
        processing_poll_interval=30,
        product_poll_interval=30,
        enable_download_daemon=True,
        enable_processing_daemon=True,
        enable_product_daemon=True,
        # product_dir=Path(os.path.join(config.ROOT_RADAR_PRODUCTS_PATH, radar_name)),
        product_type="image",
        add_colmax=True,
        enable_cleanup_daemon=True,
        netcdf_retention_days=2 / 24,
        bufr_retention_days=2 / 24,
        cleanup_poll_interval=1800,
    )

    # Create manager
    manager = DaemonManager(manager_config)

    print("\nStarting daemon manager...")
    print("  Both download and processing daemons will start")
    print("  Press Ctrl+C to stop all daemons\n")

    try:
        asyncio.run(manager.start())
    except KeyboardInterrupt:
        print("\n\nStopping daemons...")
        manager.stop()
        print("All daemons stopped")

    # Show final status
    status = manager.get_status()
    print("\n" + "=" * 60)
    print("Final Status:")
    print(f"  Radar: {status['radar_code']}")
    print(f"  Base path: {status['base_path']}")
    print("\n  Download daemon:")
    print(f"    Enabled: {status['download_daemon']['enabled']}")
    print(f"    Running: {status['download_daemon']['running']}")
    if status["download_daemon"]["stats"]:
        print(f"    Files downloaded: {status['download_daemon']['stats']['total_downloaded']}")
    print("\n  Processing daemon:")
    print(f"    Enabled: {status['processing_daemon']['enabled']}")
    print(f"    Running: {status['processing_daemon']['running']}")
    if status["processing_daemon"]["stats"]:
        print(f"    Volumes processed: {status['processing_daemon']['stats']['volumes_processed']}")
    print("=" * 60)


if __name__ == "__main__":
    # Uncomment the example you want to run:

    # Basic usage - start both daemons
    example_basic_daemon_manager()
