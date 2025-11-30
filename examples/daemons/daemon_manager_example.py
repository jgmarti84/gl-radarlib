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

    radar_name = "RMA2"
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
        # end_date=None,  # Continuous
        download_poll_interval=60,
        processing_poll_interval=30,
        product_poll_interval=30,
        enable_download_daemon=True,
        enable_processing_daemon=True,
        enable_product_daemon=True,
        product_dir=Path(os.path.join(config.ROOT_RADAR_PRODUCTS_PATH, radar_name)),
        product_type="image",
        add_colmax=True,
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


# def example_selective_daemons():
#     """
#     Example: Start only specific daemons.

#     This shows how to enable only download or only processing.
#     """
#     print("=" * 60)
#     print("Selective Daemon Example")
#     print("=" * 60)

#     volume_types = {
#         "0315": {
#             "01": ["DBZH", "DBZV"],
#             "02": ["VRAD", "WRAD"],
#         },
#     }

#     radar_name = "RMA1"
#     base_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name))

#     # Configure to run ONLY processing daemon
#     manager_config = DaemonManagerConfig(
#         radar_code=radar_name,
#         base_path=base_path,
#         ftp_host=config.FTP_HOST,
#         ftp_user=config.FTP_USER,
#         ftp_password=config.FTP_PASSWORD,
#         ftp_base_path="/L2",
#         volume_types=volume_types,
#         start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
#         enable_download_daemon=False,  # Disabled
#         enable_processing_daemon=True,  # Enabled
#     )

#     manager = DaemonManager(manager_config)

#     print("\nStarting manager with only processing daemon enabled...")
#     print("  Download daemon will NOT start")
#     print("  Processing daemon will process existing downloads\n")

#     try:
#         asyncio.run(manager.start())
#     except KeyboardInterrupt:
#         print("\n\nStopping...")
#         manager.stop()


# async def example_restart_daemon():
#     """
#     Example: Restart individual daemons.

#     This shows how to restart a daemon with new configuration.
#     """
#     print("=" * 60)
#     print("Daemon Restart Example")
#     print("=" * 60)

#     volume_types = {
#         "0315": {
#             "01": ["DBZH", "DBZV"],
#             "02": ["VRAD", "WRAD"],
#         },
#     }

#     radar_name = "RMA1"
#     base_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name))

#     manager_config = DaemonManagerConfig(
#         radar_code=radar_name,
#         base_path=base_path,
#         ftp_host=config.FTP_HOST,
#         ftp_user=config.FTP_USER,
#         ftp_password=config.FTP_PASSWORD,
#         ftp_base_path="/L2",
#         volume_types=volume_types,
#         start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
#         download_poll_interval=60,
#         processing_poll_interval=30,
#     )

#     manager = DaemonManager(manager_config)

#     print("\nStarting daemons...")

#     # Start in background
#     start_task = asyncio.create_task(manager.start())

#     # Let them run for a bit
#     await asyncio.sleep(5)

#     print("\nRestarting processing daemon with new poll interval...")
#     # Restart processing daemon with faster poll interval
#     await manager.restart_processing_daemon(new_config={"processing_poll_interval": 15})

#     print("Processing daemon restarted with 15s poll interval")

#     # Let it run a bit more
#     await asyncio.sleep(5)

#     print("\nStopping all daemons...")
#     manager.stop()

#     # Wait for clean shutdown
#     try:
#         await start_task
#     except asyncio.CancelledError:
#         pass

#     print("All daemons stopped")


# async def example_status_monitoring():
#     """
#     Example: Monitor daemon status.

#     This shows how to check status while daemons are running.
#     """
#     print("=" * 60)
#     print("Status Monitoring Example")
#     print("=" * 60)

#     volume_types = {
#         "0315": {
#             "01": ["DBZH", "DBZV"],
#             "02": ["VRAD", "WRAD"],
#         },
#     }

#     radar_name = "RMA1"
#     base_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name))

#     manager_config = DaemonManagerConfig(
#         radar_code=radar_name,
#         base_path=base_path,
#         ftp_host=config.FTP_HOST,
#         ftp_user=config.FTP_USER,
#         ftp_password=config.FTP_PASSWORD,
#         ftp_base_path="/L2",
#         volume_types=volume_types,
#         start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
#     )

#     manager = DaemonManager(manager_config)

#     # Start in background
#     start_task = asyncio.create_task(manager.start())

#     try:
#         # Monitor status periodically
#         for i in range(5):
#             await asyncio.sleep(10)

#             status = manager.get_status()
#             print(f"\nStatus check #{i+1}:")
#             print(f"  Manager running: {status['manager_running']}")

#             if status["download_daemon"]["stats"]:
#                 stats = status["download_daemon"]["stats"]
#                 print(f"  Download: {stats['total_downloaded']} files")

#             if status["processing_daemon"]["stats"]:
#                 stats = status["processing_daemon"]["stats"]
#                 print(
#                     f"  Processing: {stats['volumes_processed']} volumes processed, "
#                     f"{stats['volumes_failed']} failed"
#                 )

#     except KeyboardInterrupt:
#         print("\nInterrupted")
#     finally:
#         print("\nStopping daemons...")
#         manager.stop()
#         try:
#             await start_task
#         except asyncio.CancelledError:
#             pass


if __name__ == "__main__":
    # Uncomment the example you want to run:

    # Basic usage - start both daemons
    example_basic_daemon_manager()

    # Start only specific daemons
    # example_selective_daemons()

    # Restart individual daemons
    # asyncio.run(example_restart_daemon())

    # Monitor status while running
    # asyncio.run(example_status_monitoring())
