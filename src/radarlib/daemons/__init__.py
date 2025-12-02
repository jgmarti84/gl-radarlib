# -*- coding: utf-8 -*-
"""
Daemons module for radar data processing pipeline.

This module provides the core daemons for the radar data processing pipeline:

- **DownloadDaemon**: Monitors FTP server and downloads BUFR files
- **ProcessingDaemon**: Processes BUFR files and creates NetCDF volumes
- **ProductGenerationDaemon**: Generates visualization products (PNG, GeoTIFF) from NetCDF
- **CleanupDaemon**: Manages disk space by cleaning up processed files
- **DaemonManager**: Orchestrates all daemons for a complete pipeline

Example:
    >>> from radarlib.daemons import DaemonManager, DaemonManagerConfig
    >>> config = DaemonManagerConfig(...)
    >>> manager = DaemonManager(config)
    >>> await manager.start()
"""

# Main daemons
from radarlib.daemons.cleanup_daemon import CleanupDaemon, CleanupDaemonConfig
from radarlib.daemons.download_daemon import DownloadDaemon, DownloadDaemonConfig, DownloadDaemonError

# Legacy daemons (for backward compatibility)
from radarlib.daemons.legacy import DateBasedDaemonConfig, DateBasedFTPDaemon, FTPDaemon, FTPDaemonConfig
from radarlib.daemons.manager import DaemonManager, DaemonManagerConfig
from radarlib.daemons.processing_daemon import ProcessingDaemon, ProcessingDaemonConfig
from radarlib.daemons.product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig

__all__ = [
    # Main daemons (new names)
    "DownloadDaemon",
    "DownloadDaemonConfig",
    "DownloadDaemonError",
    "ProcessingDaemon",
    "ProcessingDaemonConfig",
    "ProductGenerationDaemon",
    "ProductGenerationDaemonConfig",
    "CleanupDaemon",
    "CleanupDaemonConfig",
    "DaemonManager",
    "DaemonManagerConfig",
    # Legacy daemons
    "FTPDaemon",
    "FTPDaemonConfig",
    "DateBasedFTPDaemon",
    "DateBasedDaemonConfig",
]
