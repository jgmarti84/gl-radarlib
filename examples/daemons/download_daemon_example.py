#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example: Download Daemon

This example demonstrates how to use the DownloadDaemon (formerly ContinuousDaemon)
to monitor an FTP server and automatically download new BUFR files.
"""

import asyncio
import datetime
import logging
from pathlib import Path

from radarlib import config
from radarlib.daemons import DownloadDaemon, DownloadDaemonConfig

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

if __name__ == "__main__":
    # Example configuration
    radar_name = "RMA1"
    vol_types = {}
    vol_types["0315"] = {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}
    vol_types["9202"] = {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}

    # Use configured paths (or override with custom paths if needed)
    radar_bufr_dir = Path(config.ROOT_RADAR_FILES_PATH) / radar_name / "bufr"
    radar_state_db = Path(config.ROOT_RADAR_FILES_PATH) / radar_name / "state.db"

    dconfig = DownloadDaemonConfig(
        host=config.DEFAULTS["FTP_HOST"],
        username=config.DEFAULTS["FTP_USER"],
        password=config.DEFAULTS["FTP_PASS"],
        radar_name=radar_name,
        remote_base_path=f"/L2/{radar_name}",
        start_date=datetime.datetime(2025, 11, 23, 20, 0, 0, tzinfo=datetime.timezone.utc),
        local_bufr_dir=radar_bufr_dir,
        state_db=radar_state_db,
        poll_interval=30,  # Check every 30 seconds
        vol_types=vol_types,
    )

    daemon = DownloadDaemon(dconfig)

    try:
        asyncio.run(daemon.run_service())
    except KeyboardInterrupt:
        print("Daemon stopped by user.")
