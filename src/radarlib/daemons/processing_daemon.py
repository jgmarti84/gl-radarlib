# -*- coding: utf-8 -*-
"""Processing daemon for monitoring and processing complete BUFR volumes."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from radarlib import config
from radarlib.io.ftp import RadarFTPClientAsync
from radarlib.state.sqlite_tracker import SQLiteStateTracker

logger = logging.getLogger(__name__)


@dataclass
class ProcessingDaemonConfig:
    """
    Configuration for BUFR processing daemon service.

    Attributes:
        local_bufr_dir: Directory containing downloaded BUFR files
        local_netcdf_dir: Directory to save processed NetCDF files
        state_db: Path to SQLite database for tracking state
        volume_types: Dict mapping volume codes to valid volume numbers and field types.
                     Format: {'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}}
        radar_name: Radar name to process (e.g., "RMA1")
        poll_interval: Seconds between checks for new complete volumes
        max_concurrent_processing: Maximum simultaneous volume processing tasks
        root_resources: Path to BUFR resources directory (for decoding)
        allow_incomplete: Whether to process incomplete volumes (missing some fields)
        incomplete_timeout_hours: Hours to wait before processing incomplete volumes
        stuck_volume_timeout_minutes: Minutes to wait before resetting a stuck volume from
                                      'processing' status back to 'pending' for retry
        start_date: Minimum observation datetime to process. Only volumes with
                   observation_datetime >= start_date will be considered. If None, all dates
                   are processed. Format: ISO 8601 string or datetime object.
    """

    local_bufr_dir: Path
    local_netcdf_dir: Path
    state_db: Path
    volume_types: Dict[str, Dict[str, List[str]]]
    radar_name: str
    poll_interval: int = 30
    max_concurrent_processing: int = 2
    root_resources: Optional[Path] = None
    allow_incomplete: bool = False
    incomplete_timeout_hours: int = 24
    stuck_volume_timeout_minutes: int = 60
    start_date: Optional[datetime] = None
    ftp_host: Optional[str] = config.FTP_HOST
    ftp_user: Optional[str] = config.FTP_USER
    ftp_password: Optional[str] = config.FTP_PASS

    def __post_init__(self):
        """Set default start_date to now UTC rounded to nearest hour if not provided."""
        if self.start_date is None:
            # Round to nearest hour
            now = datetime.now(timezone.utc)
            now = now.replace(minute=0, second=0, microsecond=0)
            self.start_date = now


class ProcessingDaemon:
    """
    Daemon for monitoring and processing complete BUFR volumes.

    This daemon monitors downloaded BUFR files tracked in the SQLite database,
    detects when complete volumes are available (all required field types for
    a volume code/number combination), decodes the BUFR files, creates a PyART
    Radar object, and saves it as a NetCDF file.

    Example:
        >>> from pathlib import Path
        >>> config = ProcessingDaemonConfig(
        ...     local_bufr_dir=Path("./downloads"),
        ...     local_netcdf_dir=Path("./netcdf"),
        ...     state_db=Path("./state.db"),
        ...     volume_types={'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}},
        ...     radar_name="RMA1"
        ... )
        >>> daemon = ProcessingDaemon(config)
        >>> asyncio.run(daemon.run())
    """

    def __init__(self, config: ProcessingDaemonConfig):
        """
        Initialize the processing daemon.

        Args:
            config: Daemon configuration
        """
        self.config = config
        self.state_tracker = SQLiteStateTracker(config.state_db)
        self._running = False
        self._processing_semaphore: Optional[asyncio.Semaphore] = None
        # Lock for serializing C library calls (BUFR library is not thread-safe)
        self._c_library_lock: Optional[asyncio.Lock] = None

        # Ensure output directory exists
        self.config.local_netcdf_dir.mkdir(parents=True, exist_ok=True)

        # Statistics
        self._stats = {
            "volumes_processed": 0,
            "volumes_failed": 0,
            "incomplete_volumes_detected": 0,
        }

    async def run(self) -> None:
        """
        Run the daemon to monitor and process complete volumes.

        Continuously checks for complete volumes and processes them.
        """
        self._running = True
        self._processing_semaphore = asyncio.Semaphore(self.config.max_concurrent_processing)
        # Create lock for C library serialization (must be created in async context)
        self._c_library_lock = asyncio.Lock()

        logger.info(f"Starting BUFR processing daemon for radar '{self.config.radar_name}'")
        logger.info(f"Monitoring BUFR files in '{self.config.local_bufr_dir}'")
        logger.info(f"Saving NetCDF files to '{self.config.local_netcdf_dir}'")
        logger.info(
            f"Configuration: poll_interval={self.config.poll_interval}s, "
            f"max_concurrent={self.config.max_concurrent_processing}, "
            f"stuck_timeout={self.config.stuck_volume_timeout_minutes}min, "
            f"allow_incomplete={self.config.allow_incomplete}, "
            f"start_date={self.config.start_date or 'None (all dates)'}"
        )

        try:
            from radarlib.utils.memory_profiling import aggressive_cleanup, log_memory_usage

            _memory_monitoring = True
        except ImportError:
            _memory_monitoring = False

        _cycle_count = 0

        try:
            while self._running:
                try:
                    # Check for and reset stuck volumes
                    await self._check_and_reset_stuck_volumes()

                    # Retry incomplete volumes if enabled
                    await self._retry_incomplete_volumes()

                    # Update volume completion status
                    await self._check_volume_completeness()

                    # Process complete volumes
                    await self._process_complete_volumes()

                    _cycle_count += 1

                    # Every 5 cycles: log memory and run aggressive GC
                    if _memory_monitoring and _cycle_count % 5 == 0:
                        log_memory_usage(f"Processing daemon cycle {_cycle_count}")
                        aggressive_cleanup(f"Processing daemon cycle {_cycle_count}")

                    # Wait before next check
                    await asyncio.sleep(self.config.poll_interval)

                except Exception as e:
                    logger.error(f"Error during processing cycle: {e}", exc_info=True)
                    await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            logger.info("Daemon cancelled, shutting down...")
        except KeyboardInterrupt:
            logger.info("Daemon interrupted, shutting down...")
        finally:
            self._running = False
            # Log final statistics
            logger.info(
                f"Daemon shutting down. Statistics: "
                f"processed={self._stats['volumes_processed']}, "
                f"failed={self._stats['volumes_failed']}, "
                f"incomplete_detected={self._stats['incomplete_volumes_detected']}"
            )
            self.state_tracker.close()
            logger.info(f"Daemon for '{self.config.radar_name}' stopped")

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info("Daemon stop requested")

    async def _retry_incomplete_volumes(self) -> None:
        try:
            incomplete_volumes = self.state_tracker.get_incomplete_volumes()
            for inc_vol in incomplete_volumes:

                fields_downloaded = self.state_tracker.get_incomplete_volumes_fields(inc_vol)

                fields_to_download = list(
                    set(inc_vol["expected_fields"].split(",")) - set([f["field_type"] for f in fields_downloaded])
                )
                remote_dir = str(Path(fields_downloaded[0]["remote_path"]).parent)
                local_dir = Path(fields_downloaded[0]["local_path"]).parent

                with RadarFTPClientAsync(
                    self.config.ftp_host, self.config.ftp_user, self.config.ftp_password
                ) as client:
                    for field in fields_to_download:
                        file_name = (
                            Path(fields_downloaded[0]["remote_path"]).stem.replace(
                                fields_downloaded[0]["field_type"], field
                            )
                            + ".BUFR"
                        )
                        remote_path = str(Path(remote_dir) / file_name)
                        local_path = local_dir / file_name
                        logger.info(
                            f"Retrying download of missing field '{file_name}' "
                            f"for incomplete volume {inc_vol['volume_id']}"
                        )
                        client.download_file(remote_path, local_path)

                        # After downloading, we should update the state tracker with the new file information
                        checksum = None
                        file_size = local_path.stat().st_size

                        self.state_tracker.mark_downloaded(
                            file_name,
                            str(remote_path),
                            str(local_path),
                            file_size=file_size,
                            checksum=checksum,
                            radar_name=self.config.radar_name,
                            strategy=inc_vol["strategy"],
                            vol_nr=inc_vol["vol_nr"],
                            field_type=field,
                            observation_datetime=inc_vol["observation_datetime"],
                        )
                        logger.info(f"[{self.config.radar_name}] Downloaded {file_name}")

                conn = self.state_tracker._get_connection()
                cursor = conn.cursor()

                now = datetime.now(timezone.utc).isoformat()
                cursor.execute(
                    """
                    UPDATE volume_processing
                    SET status = 'pending', is_complete = 1, updated_at = ?
                    WHERE volume_id = ?
                    """,
                    (now, inc_vol["volume_id"]),
                )
                conn.commit()

        except Exception as e:
            logger.error(f"Error retrying incomplete volumes: {e}", exc_info=True)

    async def _check_and_reset_stuck_volumes(self) -> None:
        """
        Check for volumes stuck in 'processing' status and reset them to 'pending'.

        Volumes that have been in 'processing' status for longer than the configured
        timeout will be reset to 'pending' and logged for retry.
        """
        try:
            num_reset = self.state_tracker.reset_stuck_volumes(self.config.stuck_volume_timeout_minutes)
            if num_reset > 0:
                logger.warning(
                    f"Reset {num_reset} stuck volume(s) from 'processing' to 'pending' "
                    f"(timeout: {self.config.stuck_volume_timeout_minutes} minutes)"
                )
        except Exception as e:
            logger.error(f"Error checking for stuck volumes: {e}", exc_info=True)

    async def _check_volume_completeness(self) -> None:
        """
        Check downloaded files and update volume completion status.

        Scans the downloads table for files belonging to volumes and determines
        if each volume is complete based on the expected field types.
        Only fetches new files since the last registered volume to improve efficiency.
        Filters out observations before the configured start_date.
        """
        logger.debug(f"Checking volume completeness for radar '{self.config.radar_name}'")

        # Get all downloaded files for this radar
        conn = self.state_tracker._get_connection()
        cursor = conn.cursor()

        # Get the latest registered volume's observation datetime to avoid re-processing
        latest_volume_datetime = self.state_tracker.get_latest_registered_volume_datetime(self.config.radar_name)
        # Parse ISO format datetime string if needed
        if isinstance(latest_volume_datetime, str):
            min_datetime = datetime.fromisoformat(latest_volume_datetime.replace("Z", "+00:00"))
        else:
            min_datetime = latest_volume_datetime

        if self.config.start_date:
            # If start_date is configured, use it as minimum threshold
            if min_datetime is None or self.config.start_date > min_datetime:
                min_datetime = self.config.start_date

        # Fetch files with observation_datetime >= min_datetime
        cursor.execute(
            """
            SELECT filename, radar_name, strategy, vol_nr, field_type, observation_datetime, local_path
            FROM downloads
            WHERE radar_name = ? AND status = 'completed'
                AND observation_datetime >= ?
            ORDER BY observation_datetime
        """,
            (self.config.radar_name, min_datetime.isoformat().replace("T", " ")),
        )

        files = cursor.fetchall()
        logger.info(f"Retrieved {len(files)} downloaded files for completeness check")

        # Group files by volume (radar + strategy + vol_nr + timestamp)
        volumes: Dict[str, Dict] = {}

        for row in files:
            filename = row[0]
            radar_name = row[1]
            strategy = row[2]
            vol_nr = row[3]
            field_type = row[4]
            observation_datetime = row[5]
            local_path = row[6]

            # Skip if strategy or vol_nr is None (shouldn't happen but defensive)
            if not strategy or not vol_nr:
                logger.warning(f"Skipping file {filename} with missing strategy or vol_nr")
                continue

            # Check if this volume type is configured
            if strategy not in self.config.volume_types:
                continue

            if vol_nr not in self.config.volume_types[strategy]:
                continue

            # Create volume key
            volume_id = self.state_tracker.get_volume_id(radar_name, strategy, vol_nr, observation_datetime)

            if volume_id not in volumes:
                volumes[volume_id] = {
                    "radar_name": radar_name,
                    "strategy": strategy,
                    "vol_nr": vol_nr,
                    "observation_datetime": observation_datetime,
                    "expected_fields": self.config.volume_types[strategy][vol_nr],
                    "downloaded_fields": set(),
                    "files": {},
                }

            volumes[volume_id]["downloaded_fields"].add(field_type)
            volumes[volume_id]["files"][field_type] = local_path

        # Update volume status in database
        num_new_complete = 0
        num_new_incomplete = 0
        num_updated_complete = 0

        for volume_id, vol_info in volumes.items():
            expected = set(vol_info["expected_fields"])
            downloaded = vol_info["downloaded_fields"]
            is_complete = expected == downloaded

            # Check if volume already exists in database
            existing = self.state_tracker.get_volume_info(volume_id)

            if existing:
                # Update if status changed
                if existing["is_complete"] != (1 if is_complete else 0):
                    self.state_tracker.update_volume_fields(volume_id, list(downloaded), is_complete)
                    if is_complete:
                        logger.info(f"Volume {volume_id} is now complete")
                        num_updated_complete += 1
                    else:
                        missing = expected - downloaded
                        logger.info(f"Volume {volume_id} incomplete, missing: {missing}")
            else:
                # Register new volume
                self.state_tracker.register_volume(
                    volume_id,
                    vol_info["radar_name"],
                    vol_info["strategy"],
                    vol_info["vol_nr"],
                    vol_info["observation_datetime"],
                    vol_info["expected_fields"],
                    is_complete,
                )
                if is_complete:
                    logger.info(f"New complete volume detected: {volume_id}")
                    num_new_complete += 1
                else:
                    missing = expected - downloaded
                    logger.info(f"New incomplete volume detected: {volume_id}, missing: {missing}")
                    num_new_incomplete += 1
                    self._stats["incomplete_volumes_detected"] += 1

        # Log summary of completeness check
        if num_new_complete > 0 or num_updated_complete > 0 or num_new_incomplete > 0:
            complete_total = num_new_complete + num_updated_complete
            logger.info(
                f"Volume completeness check completed: "
                f"{complete_total} complete "
                f"({num_new_complete} new, {num_updated_complete} updated), "
                f"{num_new_incomplete} incomplete"
            )
        else:
            logger.info("Volume completeness check completed: no new or updated volumes")

    async def _process_complete_volumes(self) -> None:
        """
        Process all volumes that haven't been processed yet.

        This includes both complete and incomplete volumes.
        Incomplete volumes are processed with available fields and marked as incomplete.
        """
        # Get all unprocessed volumes (complete and incomplete)
        unprocessed_volumes = self.state_tracker.get_unprocessed_volumes()

        if not unprocessed_volumes:
            logger.info(f"No unprocessed volumes found for {self.config.radar_name}")
            return

        # Separate by completeness
        complete_volumes = [v for v in unprocessed_volumes if v.get("is_complete", 0) == 1]
        incomplete_volumes = [v for v in unprocessed_volumes if v.get("is_complete", 0) == 0]

        logger.info(
            f"Processing volumes: {len(complete_volumes)} complete, "
            f"{len(incomplete_volumes)} incomplete "
            f"(total: {len(unprocessed_volumes)})"
        )

        # Process volumes concurrently
        tasks = []
        for volume_info in unprocessed_volumes:
            task = self._process_volume_async(volume_info)
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Count successes and failures
            num_success = sum(1 for r in results if r is True)
            num_failed = sum(1 for r in results if r is False or isinstance(r, Exception))
            if num_failed > 0:
                logger.warning(f"Processing complete: {num_success} succeeded, {num_failed} failed")

    async def _process_volume_async(self, volume_info: Dict) -> bool:
        """
        Process a single volume asynchronously.

        Processes both complete and incomplete volumes.
        Incomplete volumes are processed with available fields.

        Args:
            volume_info: Dictionary with volume information from database

        Returns:
            True if successful, False otherwise
        """
        if self._processing_semaphore is None:
            raise RuntimeError("Processing semaphore not initialized. Call run() first.")

        async with self._processing_semaphore:
            volume_id = volume_info["volume_id"]
            radar_name = volume_info["radar_name"]
            strategy = volume_info["strategy"]
            vol_nr = volume_info["vol_nr"]
            observation_datetime = volume_info["observation_datetime"]
            is_complete = volume_info.get("is_complete", 0) == 1

            completeness_str = "complete" if is_complete else "incomplete"
            logger.info(f"Processing {completeness_str} volume {volume_id}...")

            # Mark as processing
            self.state_tracker.mark_volume_processing(volume_id, "processing")

            try:
                # Get all files for this volume with their local paths
                files = self.state_tracker.get_volume_files(radar_name, strategy, vol_nr, observation_datetime)

                if not files:
                    raise ValueError(f"No files found for volume {volume_id}")

                # Extract local paths from file information
                bufr_paths = [file_info["local_path"] for file_info in files if file_info.get("local_path")]

                if not bufr_paths:
                    raise ValueError(f"No local paths found for volume {volume_id}")

                logger.info(f"Decoding {len(bufr_paths)} BUFR files for {completeness_str} volume {volume_id}")

                # Decode and save volume
                netcdf_path = await self._decode_and_save_volume(bufr_paths, volume_id, radar_name)

                # Mark as completed
                self.state_tracker.mark_volume_processing(volume_id, "completed", str(netcdf_path))
                logger.info(f"Successfully processed {completeness_str} volume {volume_id} -> {netcdf_path}")
                self._stats["volumes_processed"] += 1
                return True

            except Exception as e:
                error_msg = f"Failed to process {completeness_str} volume {volume_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                self.state_tracker.mark_volume_processing(volume_id, "failed", error_message=error_msg)
                self._stats["volumes_failed"] += 1
                return False

    async def _decode_and_save_volume(self, bufr_paths: List[str], volume_id: str, radar_name: str) -> Path:
        """
        Decode BUFR files and save as NetCDF.

        Args:
            bufr_paths: List of paths to BUFR files
            volume_id: Unique volume identifier
            radar_name: Radar name

        Returns:
            Path to saved NetCDF file

        Raises:
            Exception if decoding or saving fails
        """
        # Import here to avoid issues if pyart not available
        import gc

        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.pyart_writer import bufr_fields_to_pyart_radar
        from radarlib.utils.memory_profiling import log_memory_usage
        from radarlib.utils.names_utils import get_netcdf_filename_from_bufr_filename

        # Decode all BUFR files
        log_memory_usage("Before BUFR decoding")
        logger.debug(f"Decoding {len(bufr_paths)} BUFR files...")
        decoded_fields = []

        for bufr_path in bufr_paths:
            try:
                # Serialize BUFR decoding to avoid C library thread-safety issues
                # The BUFR C library uses global state and is not thread-safe
                async with self._c_library_lock:  # type: ignore
                    decoded = bufr_to_dict(
                        bufr_path,
                        root_resources=str(self.config.root_resources) if self.config.root_resources else None,
                    )
                if decoded:
                    decoded_fields.append(decoded)
                else:
                    logger.warning(f"Failed to decode {bufr_path}")
            except Exception as e:
                logger.error(f"Error decoding {bufr_path}: {e}")
                # Continue with other files

        if not decoded_fields:
            raise ValueError(f"No BUFR files could be decoded for volume {volume_id}")

        logger.debug(f"Successfully decoded {len(decoded_fields)} fields")
        log_memory_usage("After all BUFR files decoded")

        # Free decoded BUFR dicts before building the large PyART Radar object
        logger.debug("Creating PyART Radar object...")
        radar = bufr_fields_to_pyart_radar(decoded_fields)
        del decoded_fields
        gc.collect()
        log_memory_usage("After PyART Radar object creation (decoded_fields freed)")

        if radar is None:
            raise ValueError(f"Failed to create Radar object for volume {volume_id}")

        # Generate NetCDF filename from first BUFR file
        first_bufr_name = Path(bufr_paths[0]).name
        netcdf_filename = get_netcdf_filename_from_bufr_filename(first_bufr_name)
        netcdf_path = self.config.local_netcdf_dir / netcdf_filename

        # Save to NetCDF
        logger.debug(f"Saving to NetCDF: {netcdf_path}")
        import pyart

        pyart.io.write_cfradial(str(netcdf_path), radar)

        # Explicit cleanup of PyART Radar object (100-200 MB)
        del radar
        gc.collect()
        log_memory_usage("After NetCDF write and radar cleanup")

        logger.info(f"Successfully saved NetCDF file: {netcdf_path}")
        return netcdf_path

    def get_stats(self) -> Dict:
        """
        Get daemon statistics.

        Returns:
            Dictionary with daemon stats
        """
        return {
            "running": self._running,
            "volumes_processed": self._stats["volumes_processed"],
            "volumes_failed": self._stats["volumes_failed"],
            "incomplete_volumes_detected": self._stats["incomplete_volumes_detected"],
            "pending_volumes": len(self.state_tracker.get_volumes_by_status("pending")),
            "complete_unprocessed": len(self.state_tracker.get_complete_unprocessed_volumes()),
        }
