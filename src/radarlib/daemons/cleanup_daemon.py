# -*- coding: utf-8 -*-
"""
Cleanup Daemon for managing disk space by removing processed files.

This daemon implements a metadata-only retention policy where files are deleted
from disk but detailed metadata is preserved in the database, allowing for lazy
re-download from FTP when needed.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from radarlib.state.sqlite_tracker import SQLiteStateTracker

logger = logging.getLogger(__name__)


@dataclass
class CleanupDaemonConfig:
    """
    Configuration for Cleanup Daemon service.

    Attributes:
        state_db: Path to SQLite database for tracking state
        radar_name: Optional radar name to filter cleanup (if None, cleans all radars)
        poll_interval: Seconds between cleanup cycles (default: 1800 = 30 minutes)
        bufr_retention_days: Days to keep BUFR files before cleanup (default: 7)
        netcdf_retention_days: Days to keep NetCDF files before cleanup (default: 7)
        enable_bufr_cleanup: Whether to cleanup BUFR files (default: True)
        enable_netcdf_cleanup: Whether to cleanup NetCDF files (default: True)
        product_types: List of product types that must be completed before cleanup
                      (default: ['image']). Files won't be cleaned until all
                      specified product types are successfully generated.
        max_files_per_cycle: Maximum number of files to cleanup per cycle (default: 100).
                            This prevents long-running cleanup operations.
        dry_run: If True, only logs what would be deleted without actually deleting
    """

    state_db: Path
    radar_name: Optional[str] = None
    poll_interval: int = 1800  # 30 minutes
    bufr_retention_days: int = 7
    netcdf_retention_days: int = 7
    enable_bufr_cleanup: bool = True
    enable_netcdf_cleanup: bool = True
    product_types: List[str] = field(default_factory=lambda: ["image"])
    max_files_per_cycle: int = 100
    dry_run: bool = False


class CleanupDaemon:
    """
    Daemon for cleaning up processed BUFR and NetCDF files from disk.

    This daemon monitors the state database for files that have been fully processed
    (all products generated) and are older than the configured retention period.
    It deletes the files from disk while preserving all metadata in the database,
    enabling lazy re-download from FTP when needed.

    Key features:
    - Database-driven: Queries for completed volumes/products ready for cleanup
    - Atomic operations: Marks files for cleanup, then deletes with rollback on failure
    - Safety checks: Verifies downstream dependencies before cleanup
    - Configurable retention: Different retention periods for BUFR vs NetCDF
    - Dry-run mode: Test cleanup without actually deleting files

    Example:
        >>> from pathlib import Path
        >>> config = CleanupDaemonConfig(
        ...     state_db=Path("./state.db"),
        ...     radar_name="RMA1",
        ...     bufr_retention_days=7,
        ...     netcdf_retention_days=14,
        ... )
        >>> daemon = CleanupDaemon(config)
        >>> asyncio.run(daemon.run())
    """

    def __init__(self, config: CleanupDaemonConfig):
        """
        Initialize the cleanup daemon.

        Args:
            config: Daemon configuration
        """
        self.config = config
        self.state_tracker = SQLiteStateTracker(config.state_db)
        self._running = False

        # Statistics
        self._stats = {
            "bufr_files_cleaned": 0,
            "bufr_files_failed": 0,
            "netcdf_files_cleaned": 0,
            "netcdf_files_failed": 0,
            "last_cleanup_at": None,
            "cycles_completed": 0,
        }

    async def run(self) -> None:
        """
        Run the daemon to periodically clean up processed files.

        Continuously checks for files ready for cleanup based on retention policy.
        """
        self._running = True

        logger.info("Starting Cleanup daemon")
        if self.config.radar_name:
            logger.info(f"Filtering cleanup for radar: {self.config.radar_name}")
        logger.info(
            f"Configuration: poll_interval={self.config.poll_interval}s, "
            f"bufr_retention={self.config.bufr_retention_days} days, "
            f"netcdf_retention={self.config.netcdf_retention_days} days, "
            f"product_types={self.config.product_types}, "
            f"dry_run={self.config.dry_run}"
        )

        try:
            while self._running:
                try:
                    await self._run_cleanup_cycle()
                    self._stats["cycles_completed"] += 1
                    self._stats["last_cleanup_at"] = asyncio.get_event_loop().time()

                    # Wait before next cleanup cycle
                    await asyncio.sleep(self.config.poll_interval)

                except Exception as e:
                    logger.error(f"Error during cleanup cycle: {e}", exc_info=True)
                    await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            logger.info("Cleanup daemon cancelled, shutting down...")
        except KeyboardInterrupt:
            logger.info("Cleanup daemon interrupted, shutting down...")
        finally:
            self._running = False
            # Log final statistics
            logger.info(
                f"Cleanup daemon shutting down. Statistics: "
                f"bufr_cleaned={self._stats['bufr_files_cleaned']}, "
                f"bufr_failed={self._stats['bufr_files_failed']}, "
                f"netcdf_cleaned={self._stats['netcdf_files_cleaned']}, "
                f"netcdf_failed={self._stats['netcdf_files_failed']}, "
                f"cycles={self._stats['cycles_completed']}"
            )
            self.state_tracker.close()
            logger.info("Cleanup daemon stopped")

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info("Cleanup daemon stop requested")

    async def _run_cleanup_cycle(self) -> None:
        """
        Run a single cleanup cycle.

        Cleans up BUFR and NetCDF files that meet the retention criteria.
        """
        logger.debug("Starting cleanup cycle...")

        bufr_cleaned = 0
        bufr_failed = 0
        netcdf_cleaned = 0
        netcdf_failed = 0

        # Cleanup BUFR files
        if self.config.enable_bufr_cleanup:
            bufr_cleaned, bufr_failed = await self._cleanup_bufr_files()

        # Cleanup NetCDF files
        if self.config.enable_netcdf_cleanup:
            netcdf_cleaned, netcdf_failed = await self._cleanup_netcdf_files()

        # Log summary
        total_cleaned = bufr_cleaned + netcdf_cleaned
        total_failed = bufr_failed + netcdf_failed

        if total_cleaned > 0 or total_failed > 0:
            logger.info(
                f"Cleanup cycle complete: "
                f"BUFR({bufr_cleaned} cleaned, {bufr_failed} failed), "
                f"NetCDF({netcdf_cleaned} cleaned, {netcdf_failed} failed)"
            )
        else:
            logger.debug("Cleanup cycle complete: no files to clean")

    async def _cleanup_bufr_files(self) -> tuple:
        """
        Clean up BUFR files that meet retention criteria.

        Returns:
            Tuple of (cleaned_count, failed_count)
        """
        files = self.state_tracker.get_bufr_files_for_cleanup(
            retention_days=self.config.bufr_retention_days,
            radar_name=self.config.radar_name,
            product_types=self.config.product_types,
        )

        if not files:
            logger.debug("No BUFR files ready for cleanup")
            return 0, 0

        # Limit to max_files_per_cycle
        files_to_process = files[: self.config.max_files_per_cycle]
        logger.info(f"Found {len(files)} BUFR files ready for cleanup, processing {len(files_to_process)}")

        cleaned = 0
        failed = 0

        for file_info in files_to_process:
            filename = file_info["filename"]
            local_path = file_info.get("local_path")

            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would cleanup BUFR file: {filename} ({local_path})")
                cleaned += 1
                continue

            if self.state_tracker.cleanup_bufr_file(filename):
                cleaned += 1
                self._stats["bufr_files_cleaned"] += 1
            else:
                failed += 1
                self._stats["bufr_files_failed"] += 1

            # Yield control to event loop periodically
            await asyncio.sleep(0)

        return cleaned, failed

    async def _cleanup_netcdf_files(self) -> tuple:
        """
        Clean up NetCDF files that meet retention criteria.

        Returns:
            Tuple of (cleaned_count, failed_count)
        """
        volumes = self.state_tracker.get_netcdf_files_for_cleanup(
            retention_days=self.config.netcdf_retention_days,
            radar_name=self.config.radar_name,
            product_types=self.config.product_types,
        )

        if not volumes:
            logger.debug("No NetCDF files ready for cleanup")
            return 0, 0

        # Limit to max_files_per_cycle
        volumes_to_process = volumes[: self.config.max_files_per_cycle]
        logger.info(f"Found {len(volumes)} NetCDF files ready for cleanup, processing {len(volumes_to_process)}")

        cleaned = 0
        failed = 0

        for volume_info in volumes_to_process:
            volume_id = volume_info["volume_id"]
            netcdf_path = volume_info.get("netcdf_path")

            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would cleanup NetCDF volume: {volume_id} ({netcdf_path})")
                cleaned += 1
                continue

            if self.state_tracker.cleanup_netcdf_file(volume_id):
                cleaned += 1
                self._stats["netcdf_files_cleaned"] += 1
            else:
                failed += 1
                self._stats["netcdf_files_failed"] += 1

            # Yield control to event loop periodically
            await asyncio.sleep(0)

        return cleaned, failed

    def get_stats(self) -> Dict:
        """
        Get daemon statistics.

        Returns:
            Dictionary with daemon stats
        """
        cleanup_stats = self.state_tracker.get_cleanup_stats(self.config.radar_name)

        return {
            "running": self._running,
            "bufr_files_cleaned": self._stats["bufr_files_cleaned"],
            "bufr_files_failed": self._stats["bufr_files_failed"],
            "netcdf_files_cleaned": self._stats["netcdf_files_cleaned"],
            "netcdf_files_failed": self._stats["netcdf_files_failed"],
            "cycles_completed": self._stats["cycles_completed"],
            "last_cleanup_at": self._stats["last_cleanup_at"],
            "db_stats": cleanup_stats,
        }

    async def run_once(self) -> Dict:
        """
        Run a single cleanup cycle (useful for testing or manual invocation).

        Returns:
            Dictionary with cleanup results
        """
        logger.info("Running single cleanup cycle...")

        bufr_cleaned = 0
        bufr_failed = 0
        netcdf_cleaned = 0
        netcdf_failed = 0

        if self.config.enable_bufr_cleanup:
            bufr_cleaned, bufr_failed = await self._cleanup_bufr_files()

        if self.config.enable_netcdf_cleanup:
            netcdf_cleaned, netcdf_failed = await self._cleanup_netcdf_files()

        return {
            "bufr_cleaned": bufr_cleaned,
            "bufr_failed": bufr_failed,
            "netcdf_cleaned": netcdf_cleaned,
            "netcdf_failed": netcdf_failed,
            "total_cleaned": bufr_cleaned + netcdf_cleaned,
            "total_failed": bufr_failed + netcdf_failed,
        }
