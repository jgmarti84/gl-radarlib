"""
Download Daemon for monitoring FTP server and downloading BUFR files.

This daemon continuously checks the FTP server for the latest minute/second folder
and downloads new files, similar to the process_new_files pattern in FTPRadarDaemon.
"""

import asyncio
import gc
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from radarlib.io.ftp.ftp import exponential_backoff_retry
from radarlib.io.ftp.ftp_client import FTPError, RadarFTPClientAsync
from radarlib.state.sqlite_tracker import SQLiteStateTracker
from radarlib.utils.memory_profiling import aggressive_cleanup, log_memory_usage
from radarlib.utils.names_utils import build_vol_types_regex, extract_bufr_filename_components

logger = logging.getLogger(__name__)


class DownloadDaemonError(Exception):
    """Base class for Download Daemon errors."""

    pass


@dataclass
class DownloadDaemonConfig:
    """Configuration for DownloadDaemon."""

    host: str
    username: str
    password: str
    radar_name: str
    remote_base_path: str
    local_bufr_dir: Path
    state_db: Path
    poll_interval: int = 60
    start_date: Optional[datetime] = None
    vol_types: Optional[Dict] = None
    max_concurrent_downloads: int = 5
    bufr_download_max_retries: int = 3
    bufr_download_base_delay: float = 1
    bufr_download_max_delay: float = 30

    def __post_init__(self):
        """Set default start_date to now UTC rounded to nearest hour if not provided."""
        if self.start_date is None:
            # Round to nearest hour
            now = datetime.now(timezone.utc)
            now = now.replace(minute=0, second=0, microsecond=0)
            self.start_date = now


class DownloadDaemon:
    """
    A daemon that continuously monitors the FTP server for new files.

    It checks for the latest minute/second folder in the FTP directory
    and logs it periodically.
    """

    def __init__(self, daemon_config: DownloadDaemonConfig):
        """
        Initialize the DownloadDaemon.

        Args:
            daemon_config: Configuration for the daemon.

        Raises:
            DownloadDaemonError: If initialization fails.
        """
        self.config = daemon_config
        self.radar_name = daemon_config.radar_name
        self.local_dir = Path(daemon_config.local_bufr_dir)
        self.poll_interval = daemon_config.poll_interval
        self.start_date = daemon_config.start_date
        self.vol_types = daemon_config.vol_types

        try:
            self.state_tracker = SQLiteStateTracker(daemon_config.state_db)
            logger.info("[%s] State tracker initialized with database: %s", self.radar_name, daemon_config.state_db)
        except Exception as e:
            logger.exception("[%s] Failed to initialize state tracker", self.radar_name)
            raise DownloadDaemonError(f"Failed to initialize state tracker: {e}") from e
        self._stats = {
            "bufr_files_downloaded": 0,
            "bufr_files_failed": 0,
            "bufr_files_pending": 0,
            "last_downloaded": None,
            "total_bytes": 0,
        }
        self._running = False

    @property
    def vol_types(self):
        return self._vol_types

    @vol_types.setter
    def vol_types(self, value):
        if isinstance(value, dict):
            self._vol_types = build_vol_types_regex(value)
        elif isinstance(value, re.Pattern):
            self._vol_types = value
        else:
            self._vol_types = None

    async def start(self, interval: int = 60):
        """Run indefinitely, polling new files every `interval` seconds."""
        while True:
            try:
                await self.run_service()
            except Exception as e:
                logger.exception("Radar process error: %s", e)
            await asyncio.sleep(interval)

    async def run_service(self):
        """
        Run the daemon indefinitely, checking for new files every poll_interval seconds.
        """
        logger.info(f"[{self.radar_name}] Starting continuous daemon with poll interval: {self.poll_interval} seconds")

        # Memory monitoring setup (per copilot-instructions.md Rule 5)
        _cycle_count = 0
        log_memory_usage(f"[{self.radar_name}] DownloadDaemon startup")

        while True:
            try:
                # Determine resume date: use latest downloaded file if available and newer than start_date
                resume_date: datetime = self.start_date  # type: ignore
                latest_bufr_file = self.state_tracker.get_latest_downloaded_file(self.radar_name)
                if latest_bufr_file and latest_bufr_file.get("observation_datetime"):
                    latest_bufr_date_str = latest_bufr_file["observation_datetime"]
                    # Parse ISO format datetime string if needed
                    if isinstance(latest_bufr_date_str, str):
                        latest_bufr_date = datetime.fromisoformat(latest_bufr_date_str.replace("Z", "+00:00"))
                    else:
                        latest_bufr_date = latest_bufr_date_str

                    if resume_date and latest_bufr_date > resume_date:
                        resume_date = latest_bufr_date
                        logger.info(f"[{self.radar_name}] Resuming from last bufr file date: {resume_date.isoformat()}")
                    else:
                        logger.info(
                            f"[{self.radar_name}] Starting from configured start date: {resume_date.isoformat()}"
                        )
                else:
                    if resume_date:
                        logger.info(
                            f"[{self.radar_name}] No previous downloads found, starting from: {resume_date.isoformat()}"
                        )
                    else:
                        logger.warning(f"[{self.radar_name}] No start date configured")

                async with RadarFTPClientAsync(
                    self.config.host,
                    self.config.username,
                    self.config.password,
                    max_workers=self.config.max_concurrent_downloads,
                ) as client:
                    logger.debug(f"[{self.radar_name}] Connected to FTP server. Checking for new files...")
                    files = self.new_bufr_files(
                        ftp_client=client, start_date=resume_date, end_date=None, vol_types=self.vol_types
                    )
                    if files:
                        tasks = []
                        for remote, local, fname, dt, status in files:

                            async def download_one(
                                remote_path=remote, local_path=local, fname=fname, dt=dt, status=status
                            ):
                                components = extract_bufr_filename_components(fname)
                                try:
                                    await exponential_backoff_retry(
                                        lambda: client.download_file_async(remote_path, local_path),
                                        max_retries=self.config.bufr_download_max_retries,
                                        base_delay=self.config.bufr_download_base_delay,
                                        max_delay=self.config.bufr_download_max_delay,
                                    )
                                    # success → update DB
                                    # Calculate checksum if enabled
                                    checksum = None
                                    # TODO: implement checksum calculation asynchronously
                                    # Get file size
                                    file_size = local_path.stat().st_size

                                    self.state_tracker.mark_downloaded(
                                        fname,
                                        str(remote_path),
                                        str(local_path),
                                        file_size=file_size,
                                        checksum=checksum,
                                        radar_name=self.radar_name,
                                        strategy=components["strategy"],
                                        vol_nr=components["vol_nr"],
                                        field_type=components["field_type"],
                                        observation_datetime=dt.isoformat(),
                                    )
                                    logger.info(f"[{self.radar_name}] Downloaded {fname}")
                                except FTPError as e:
                                    self.state_tracker.mark_failed(
                                        fname,
                                        str(remote_path),
                                        str(local_path),
                                        radar_name=self.radar_name,
                                        strategy=components["strategy"],
                                        vol_nr=components["vol_nr"],
                                        field_type=components["field_type"],
                                        observation_datetime=dt.isoformat(),
                                    )
                                    logger.error(f"[{self.radar_name}] FTPError for {fname}: {e}")
                                finally:
                                    # Explicit cleanup (per copilot-instructions.md Rules 1, 4)
                                    if "components" in locals():
                                        del components
                                    gc.collect()

                            tasks.append(asyncio.create_task(download_one()))

                        await asyncio.gather(*tasks)
                        logger.info(f"[{self.radar_name}] Processed {len(files)} files.")

                        # Cleanup task list to release closure references (per copilot-instructions.md Rules 1, 3)
                        tasks = []
                        gc.collect()

                        _cycle_count += 1
                        if _cycle_count % 5 == 0:  # Every 5 cycles, same cadence as other daemons
                            log_memory_usage(f"[{self.radar_name}] DownloadDaemon cycle {_cycle_count}")
                            aggressive_cleanup(f"DownloadDaemon cycle {_cycle_count}")
                    else:
                        logger.info(f"[{self.radar_name}] No new files.")

            except Exception as e:
                logger.exception(f"[{self.radar_name}] Error during check_latest_folder: {e}")

            await asyncio.sleep(self.poll_interval)

    def new_bufr_files(
        self,
        ftp_client: RadarFTPClientAsync,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        vol_types: Optional[re.Pattern] = None,
    ) -> list:
        """
        Get new BUFR files from FTP server within the specified date range.

        Args:
            ftp_client: RadarFTPClientAsync instance.
            start_date: Start date for searching files.
            end_date: End date for searching files.
            vol_types: Optional dictionary to filter volume types.

        Returns:
            List of tuples (remote_path, local_path, filename, datetime, status).
        """
        candidates = []
        for dt, fname, remote in ftp_client.traverse_radar(
            self.radar_name, start_date, end_date, include_start=False, vol_types=vol_types
        ):
            local_path = self.local_dir / fname
            candidates.append((remote, local_path, fname, dt, "new"))
        return candidates

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info("Daemon stop requested")

    def get_stats(self) -> Dict[str, Optional[object]]:
        """
        Retrieve basic statistics for this daemon's radar from the state tracker.

        """
        return {
            "running": self._running,
            "bufr_files_downloaded": self._stats["bufr_files_downloaded"],
            "bufr_files_failed": self._stats["bufr_files_failed"],
        }
