# -*- coding: utf-8 -*-
"""Daemon manager for Download, Processing, Product Generation, and Cleanup daemons."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from radarlib.daemons.cleanup_daemon import CleanupDaemon, CleanupDaemonConfig
from radarlib.daemons.download_daemon import DownloadDaemon, DownloadDaemonConfig
from radarlib.daemons.processing_daemon import ProcessingDaemon, ProcessingDaemonConfig
from radarlib.daemons.product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig

# Watchdog constants
_WATCHDOG_INTERVAL_S: int = 120  # Check daemon health every 2 minutes
_HEARTBEAT_STALE_THRESHOLD_S: int = 300  # Daemon is considered hung after 5 minutes without heartbeat

logger = logging.getLogger(__name__)


@dataclass
class DaemonManagerConfig:
    """
    Configuration for the daemon manager.

    Attributes:
        radar_code: Radar code (e.g., "RMA1")
        base_path: Base path for all files
        ftp_host: FTP server hostname
        ftp_user: FTP username
        ftp_password: FTP password
        ftp_base_path: Remote FTP base path
        volume_types: Volume type configuration
        start_date: Start date for downloads (UTC)
        download_poll_interval: Seconds between download checks
        processing_poll_interval: Seconds between processing checks
        product_poll_interval: Seconds between product generation checks
        cleanup_poll_interval: Seconds between cleanup checks (default: 1800 = 30 min)
        enable_download_daemon: Whether to start download daemon
        enable_processing_daemon: Whether to start processing daemon
        enable_product_daemon: Whether to start product generation daemon
        enable_cleanup_daemon: Whether to start cleanup daemon
        product_type: Type of product to generate:
                      - ``'image'``: PNG visualization (default)
                      - ``'geotiff'``: Legacy multi-band RGBA Cloud-Optimized GeoTIFF
                      - ``'raw_cog'``: Single-band float32 COG with embedded colormap metadata,
                        enabling dynamic colormap changes via ``remap_cog_colormap`` or
                        ``read_cog_tile_as_rgba``
        add_colmax: Whether to generate COLMAX field in product daemon
        bufr_retention_days: Days to keep BUFR files before cleanup (default: 7)
        netcdf_retention_days: Days to keep NetCDF files before cleanup (default: 7)
        cleanup_product_types: Product types that must be completed before cleanup
    """

    radar_name: str
    base_path: Path
    ftp_host: str
    ftp_user: str
    ftp_password: str
    ftp_base_path: str
    volume_types: Dict
    start_date: Optional[datetime] = None
    # end_date: Optional[datetime] = None
    download_poll_interval: int = 60
    processing_poll_interval: int = 30
    product_poll_interval: int = 30
    cleanup_poll_interval: int = 1800  # 30 minutes
    enable_download_daemon: bool = True
    bufr_dir: Optional[Path] = None
    enable_processing_daemon: bool = True
    netcdf_dir: Optional[Path] = None
    enable_product_daemon: bool = True
    product_dir: Optional[Path] = None
    enable_cleanup_daemon: bool = False  # Disabled by default for safety
    product_type: str = "image"
    add_colmax: bool = True
    geometry_types: Optional[Dict[str, Dict[str, Any]]] = None
    bufr_retention_days: int = 7
    netcdf_retention_days: int = 7
    cleanup_product_types: Optional[List[str]] = None

    def __post_init__(self):
        """Post-initialization checks."""
        if isinstance(self.start_date, str):
            try:
                self.start_date = datetime.fromisoformat(self.start_date)
            except ValueError:
                self.start_date = None

        if self.start_date and self.start_date.tzinfo is None:
            raise ValueError("start_date must be timezone-aware (UTC)")

        if self.start_date is None:
            self.start_date = datetime.now().replace(tzinfo=timezone.utc)

        # Default cleanup_product_types to the configured product_type so that cleanup
        # waits for the same product type the product daemon actually generates.
        if self.cleanup_product_types is None:
            self.cleanup_product_types = [self.product_type]


class DaemonManager:
    """
    Simple manager for FTP download, BUFR processing, product generation, and cleanup daemons.

    Provides easy start/stop control and configuration management for all daemons.

    Example:
        >>> manager = DaemonManager(config)
        >>> await manager.start()  # Starts enabled daemons
        >>> # ... later ...
        >>> manager.stop()  # Stops all running daemons
    """

    def __init__(self, config: DaemonManagerConfig):
        """
        Initialize the daemon manager.

        Args:
            config: Manager configuration
        """
        self.config = config
        self.download_daemon: Optional[DownloadDaemon] = None
        self.processing_daemon: Optional[ProcessingDaemon] = None
        self.product_daemon: Optional[ProductGenerationDaemon] = None
        self.cleanup_daemon: Optional[CleanupDaemon] = None
        self._tasks = []
        self._running = False

        # Setup paths
        self.bufr_dir = config.base_path / "bufr"
        if self.config.bufr_dir is not None:
            self.bufr_dir = self.config.bufr_dir
        self.netcdf_dir = config.base_path / "netcdf"
        if self.config.netcdf_dir is not None:
            self.netcdf_dir = self.config.netcdf_dir
        self.product_dir = config.base_path / "products"
        if self.config.product_dir is not None:
            self.product_dir = self.config.product_dir
        self.state_db = config.base_path / "state.db"

        # Ensure directories exist
        self.bufr_dir.mkdir(parents=True, exist_ok=True)
        self.netcdf_dir.mkdir(parents=True, exist_ok=True)
        self.product_dir.mkdir(parents=True, exist_ok=True)

    def _create_download_daemon(self) -> DownloadDaemon:
        """Create download daemon with current configuration."""
        download_config = DownloadDaemonConfig(
            host=self.config.ftp_host,
            username=self.config.ftp_user,
            password=self.config.ftp_password,
            radar_name=self.config.radar_name,
            remote_base_path=self.config.ftp_base_path,
            local_bufr_dir=self.bufr_dir,
            state_db=self.state_db,
            start_date=self.config.start_date,
            # end_date=self.config.end_date,
            poll_interval=self.config.download_poll_interval,
            vol_types=self.config.volume_types,
        )
        return DownloadDaemon(download_config)

    def _create_processing_daemon(self) -> ProcessingDaemon:
        """Create processing daemon with current configuration."""
        processing_config = ProcessingDaemonConfig(
            local_bufr_dir=self.bufr_dir,
            local_netcdf_dir=self.netcdf_dir,
            state_db=self.state_db,
            start_date=self.config.start_date,
            volume_types=self.config.volume_types,
            radar_name=self.config.radar_name,
            poll_interval=self.config.processing_poll_interval,
            ftp_host=self.config.ftp_host,
            ftp_user=self.config.ftp_user,
            ftp_password=self.config.ftp_password,
        )
        return ProcessingDaemon(processing_config)

    def _create_product_daemon(self) -> ProductGenerationDaemon:
        """Create product generation daemon with current configuration."""
        product_config = ProductGenerationDaemonConfig(
            local_netcdf_dir=self.netcdf_dir,
            local_product_dir=self.product_dir,
            state_db=self.state_db,
            volume_types=self.config.volume_types,
            radar_name=self.config.radar_name,
            poll_interval=self.config.product_poll_interval,
            product_type=self.config.product_type,
            add_colmax=self.config.add_colmax,
            geometry_types=self.config.geometry_types,
            ftp_host=self.config.ftp_host,
            ftp_user=self.config.ftp_user,
            ftp_password=self.config.ftp_password,
        )
        return ProductGenerationDaemon(product_config)

    def _create_cleanup_daemon(self) -> CleanupDaemon:
        """Create cleanup daemon with current configuration."""
        cleanup_config = CleanupDaemonConfig(
            state_db=self.state_db,
            radar_name=self.config.radar_name,
            poll_interval=self.config.cleanup_poll_interval,
            bufr_retention_days=self.config.bufr_retention_days,
            netcdf_retention_days=self.config.netcdf_retention_days,
            product_types=self.config.cleanup_product_types,
        )
        return CleanupDaemon(cleanup_config)

    async def start(self) -> None:
        """
        Start enabled daemons.

        Starts the download, processing, product generation, and/or cleanup daemons
        based on configuration. Runs until stopped or cancelled.
        """
        if self._running:
            logger.warning("Daemons are already running")
            return

        self._running = True
        self._tasks = []

        logger.info(f"Starting daemon manager for radar '{self.config.radar_name}'")

        try:
            from radarlib.utils.memory_profiling import log_memory_usage

            log_memory_usage("DaemonManager startup")
        except ImportError:
            pass

        # Create and start download daemon
        if self.config.enable_download_daemon:
            self.download_daemon = self._create_download_daemon()
            task = asyncio.create_task(self.download_daemon.start())
            self._tasks.append(("download", task))
            logger.info("Started download daemon")

        # Create and start processing daemon
        if self.config.enable_processing_daemon:
            self.processing_daemon = self._create_processing_daemon()
            task = asyncio.create_task(self.processing_daemon.run())
            self._tasks.append(("processing", task))
            logger.info("Started processing daemon")

        # Create and start product generation daemon
        if self.config.enable_product_daemon:
            try:
                self.product_daemon = self._create_product_daemon()
            except Exception as e:
                logger.error(
                    f"Failed to create product daemon (geometry init may have failed): {e}. "
                    f"Product daemon will NOT start. Other daemons continue.",
                    exc_info=True,
                )
                self.product_daemon = None

            if self.product_daemon is not None:
                task = asyncio.create_task(self.product_daemon.run())
                self._tasks.append(("product", task))
                logger.info("Started product generation daemon")

        # Create and start cleanup daemon
        if self.config.enable_cleanup_daemon:
            self.cleanup_daemon = self._create_cleanup_daemon()
            task = asyncio.create_task(self.cleanup_daemon.run())
            self._tasks.append(("cleanup", task))
            logger.info("Started cleanup daemon")

        if not self._tasks:
            logger.warning("No daemons enabled in configuration")
            return

        # Start watchdog to monitor daemon health
        watchdog_task = asyncio.create_task(self._watchdog())
        self._tasks.append(("watchdog", watchdog_task))
        logger.info("Started daemon watchdog")

        # Wait for all tasks to complete
        try:
            await asyncio.gather(*[task for _, task in self._tasks])
        except asyncio.CancelledError:
            logger.info("Daemon manager cancelled")
            self.stop()
        except Exception as e:
            logger.error(f"Error in daemon manager: {e}", exc_info=True)
            self.stop()
        finally:
            self._running = False
            try:
                from radarlib.utils.memory_profiling import aggressive_cleanup, log_memory_usage

                log_memory_usage("DaemonManager shutdown")
                aggressive_cleanup("DaemonManager shutdown")
            except ImportError:
                pass

    def stop(self) -> None:
        """Stop all running daemons."""
        logger.info("Stopping all daemons")

        if self.download_daemon:
            self.download_daemon.stop()
            logger.info("Stopped download daemon")

        if self.processing_daemon:
            self.processing_daemon.stop()
            logger.info("Stopped processing daemon")

        if self.product_daemon:
            self.product_daemon.stop()
            logger.info("Stopped product generation daemon")

        if self.cleanup_daemon:
            self.cleanup_daemon.stop()
            logger.info("Stopped cleanup daemon")

        # Cancel any running tasks
        for name, task in self._tasks:
            if not task.done():
                task.cancel()
                logger.debug(f"Cancelled {name} task")

        self._running = False

    async def _watchdog(self) -> None:
        """
        Periodically check daemon health via heartbeat timestamps.

        If a daemon's heartbeat is stale beyond the threshold, it is considered
        hung (e.g., blocked on a stale FTP socket) and will be restarted.
        Currently monitors the download daemon only, since it is the daemon
        most susceptible to FTP connection hangs.
        """
        logger.info(
            f"Watchdog started: checking every {_WATCHDOG_INTERVAL_S}s, "
            f"stale threshold {_HEARTBEAT_STALE_THRESHOLD_S}s"
        )
        try:
            while self._running:
                await asyncio.sleep(_WATCHDOG_INTERVAL_S)

                # --- Download daemon health check ---
                if self.download_daemon and self.download_daemon._last_heartbeat:
                    elapsed = (datetime.now(timezone.utc) - self.download_daemon._last_heartbeat).total_seconds()

                    if elapsed > _HEARTBEAT_STALE_THRESHOLD_S:
                        logger.warning(
                            f"[{self.config.radar_name}] Watchdog: download daemon heartbeat "
                            f"stale for {elapsed:.0f}s (threshold {_HEARTBEAT_STALE_THRESHOLD_S}s). "
                            f"Restarting download daemon..."
                        )
                        try:
                            await self.restart_download_daemon()
                            logger.info(f"[{self.config.radar_name}] Watchdog: download daemon restarted successfully")
                        except Exception as e:
                            logger.error(
                                f"[{self.config.radar_name}] Watchdog: failed to restart download daemon: {e}",
                                exc_info=True,
                            )
                    else:
                        logger.debug(
                            f"[{self.config.radar_name}] Watchdog: download daemon healthy "
                            f"(last heartbeat {elapsed:.0f}s ago)"
                        )

                # --- Check if download daemon task has died silently ---
                # Also detect other crashed daemon tasks for visibility
                for name, task in list(self._tasks):
                    if name == "watchdog" or not task.done():
                        continue
                    exc = task.exception() if not task.cancelled() else None
                    logger.warning(
                        f"[{self.config.radar_name}] Watchdog: '{name}' daemon task "
                        f"unexpectedly finished (exception={exc})."
                    )
                    if name == "download":
                        logger.info(f"[{self.config.radar_name}] Watchdog: restarting download daemon...")
                        try:
                            await self.restart_download_daemon()
                        except Exception as e:
                            logger.error(
                                f"[{self.config.radar_name}] Watchdog: restart download daemon failed: {e}",
                                exc_info=True,
                            )
                    # For other daemons, just log — they don't have the FTP hang issue
                    # and their crashes need investigation rather than blind restart

        except asyncio.CancelledError:
            logger.info("Watchdog cancelled, shutting down...")
        except Exception as e:
            logger.error(f"Watchdog error: {e}", exc_info=True)

    async def restart_download_daemon(self, new_config: Optional[Dict] = None) -> None:
        """
        Restart download daemon with optional new configuration.

        Args:
            new_config: Optional dict with config parameters to update
        """
        logger.info("Restarting download daemon")

        # Stop existing download daemon
        if self.download_daemon:
            self.download_daemon.stop()
            # Find and cancel its task
            for i, (name, task) in enumerate(self._tasks):
                if name == "download" and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self._tasks.pop(i)
                    break

        # Apply new configuration if provided
        if new_config:
            for key, value in new_config.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
                    logger.debug(f"Updated config: {key} = {value}")

        # Create and start new download daemon
        self.download_daemon = self._create_download_daemon()
        task = asyncio.create_task(self.download_daemon.start())
        self._tasks.append(("download", task))
        logger.info("Download daemon restarted")

    async def restart_processing_daemon(self, new_config: Optional[Dict] = None) -> None:
        """
        Restart processing daemon with optional new configuration.

        Args:
            new_config: Optional dict with config parameters to update
        """
        logger.info("Restarting processing daemon")

        # Stop existing processing daemon
        if self.processing_daemon:
            self.processing_daemon.stop()
            # Find and cancel its task
            for i, (name, task) in enumerate(self._tasks):
                if name == "processing" and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self._tasks.pop(i)
                    break

        # Apply new configuration if provided
        if new_config:
            for key, value in new_config.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
                    logger.debug(f"Updated config: {key} = {value}")

        # Create and start new processing daemon
        self.processing_daemon = self._create_processing_daemon()
        task = asyncio.create_task(self.processing_daemon.run())
        self._tasks.append(("processing", task))
        logger.info("Processing daemon restarted")

    def get_status(self) -> Dict:
        """
        Get status of all daemons.

        Returns:
            Dictionary with daemon status information
        """
        status = {
            "manager_running": self._running,
            "radar_code": self.config.radar_name,
            "base_path": str(self.config.base_path),
            "download_daemon": {
                "enabled": self.config.enable_download_daemon,
                "running": self.download_daemon is not None and self.download_daemon._running,
                "stats": self.download_daemon.get_stats() if self.download_daemon else None,
            },
            "processing_daemon": {
                "enabled": self.config.enable_processing_daemon,
                "running": self.processing_daemon is not None and self.processing_daemon._running,
                "stats": self.processing_daemon.get_stats() if self.processing_daemon else None,
            },
            "product_daemon": {
                "enabled": self.config.enable_product_daemon,
                "running": self.product_daemon is not None and self.product_daemon._running,
                "stats": self.product_daemon.get_stats() if self.product_daemon else None,
            },
            "cleanup_daemon": {
                "enabled": self.config.enable_cleanup_daemon,
                "running": self.cleanup_daemon is not None and self.cleanup_daemon._running,
                "stats": self.cleanup_daemon.get_stats() if self.cleanup_daemon else None,
            },
        }
        return status

    def update_config(self, **kwargs) -> None:
        """
        Update configuration parameters.

        Args:
            **kwargs: Configuration parameters to update
        """
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                logger.info(f"Updated config: {key} = {value}")
            else:
                logger.warning(f"Unknown config parameter: {key}")
