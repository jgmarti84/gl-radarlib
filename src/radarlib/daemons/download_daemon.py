"""
Download Daemon for monitoring FTP server and downloading BUFR files.

This daemon continuously checks the FTP server for the latest minute/second folder
and downloads new files, similar to the process_new_files pattern in FTPRadarDaemon.
"""

import asyncio
import gc
import logging
import random
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

from radarlib.io.ftp.ftp import exponential_backoff_retry
from radarlib.io.ftp.ftp_client import FTPError, RadarFTPClientAsync
from radarlib.state.sqlite_tracker import SQLiteStateTracker
from radarlib.utils.memory_profiling import aggressive_cleanup, log_memory_usage
from radarlib.utils.names_utils import build_vol_types_regex, extract_bufr_filename_components

logger = logging.getLogger(__name__)


def compute_scan_window(
    latest_by_vol: Dict[str, datetime],
    scan_cursor: Optional[datetime],
    now: datetime,
    start_date: Optional[datetime],
    window_minutes: int,
    backfill_minutes: int = 15,
):
    """Decide the FTP traversal window for one poll cycle.

    Returns ``(resume_date, scan_end)``:

    * ``resume_date`` — inclusive-exclusive lower bound to resume scanning from.
    * ``scan_end`` — upper bound, or ``None`` meaning "scan to the present".

    The window is always capped to ``window_minutes`` so a single cycle never
    scans an unbounded range (this is what prevents the old ``min + 60`` design's
    runaway window / zombie-thread problem).

    The key property — and the fix for the "gap ratchet" that froze RMA12/RMA17 —
    is ``scan_cursor``. Anchoring purely to the newest *downloaded* file means the
    window cannot advance across a data gap longer than ``window - backfill``
    minutes: the next scan lands beyond the window and nothing downloads, so the
    anchor never moves. The cursor decouples *scan progress* from *download
    progress*: whenever a capped (backlog) window is fully scanned, the caller
    advances the cursor to ``scan_end``, so the next cycle steps forward by one
    window — marching through gaps and backlog until it reaches the present, at
    which point the cursor is cleared and normal ``max - backfill`` tracking (which
    re-scans the last ``backfill_minutes`` for late-published files) resumes.

    Args:
        latest_by_vol: Newest downloaded observation datetime per volume number.
        scan_cursor: Where the previous cycle left off while catching up, or None
            when tracking the live frontier.
        now: Current UTC time.
        start_date: Configured fallback start when nothing has been downloaded yet.
        window_minutes: Maximum directory span scanned per cycle.
        backfill_minutes: How far behind the newest download to re-scan, to catch
            files published slightly late.
    """
    if latest_by_vol:
        base_floor: Optional[datetime] = max(latest_by_vol.values()) - timedelta(minutes=backfill_minutes)
    else:
        base_floor = start_date

    if base_floor is not None and scan_cursor is not None:
        resume_date: Optional[datetime] = max(base_floor, scan_cursor)
    elif scan_cursor is not None:
        resume_date = scan_cursor
    else:
        resume_date = base_floor

    if resume_date is None:
        return None, None

    scan_end: Optional[datetime] = resume_date + timedelta(minutes=window_minutes)
    if scan_end > now:
        scan_end = None  # window reaches the present; scan to now

    return resume_date, scan_end


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
    max_concurrent_downloads: int = 2
    bufr_download_max_retries: int = 3
    bufr_download_base_delay: float = 1
    bufr_download_max_delay: float = 30
    failed_file_retry_interval: int = 600  # Retry failed files every 10 minutes (in seconds)
    failed_file_retention_days: int = 1  # Keep retrying for up to 1 day
    ftp_cycle_timeout: int = 3600  # Max seconds for a single FTP poll cycle before timeout
    max_traversal_window_minutes: int = 30  # Max FTP directory window scanned per cycle
    ftp_timeout: int = 120  # Socket timeout (seconds) for each FTP control/data connection

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
        # Store original vol_types dict for extracting volume numbers
        self._vol_types_config = daemon_config.vol_types if isinstance(daemon_config.vol_types, dict) else None

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
            "failed_files_retried": 0,
        }
        self._running = False
        self._last_failed_retry_time: Optional[datetime] = None
        self._last_heartbeat: Optional[datetime] = None
        self._cancel_traversal = threading.Event()
        # Where the last backlog/catch-up cycle finished scanning. None means we
        # are tracking the live frontier. Lets the traversal step forward through
        # data gaps instead of freezing (see compute_scan_window).
        self._scan_cursor: Optional[datetime] = None

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
            except asyncio.CancelledError:
                logger.info(f"[{self.radar_name}] Download daemon cancelled, shutting down...")
                break
            except Exception as e:
                logger.exception("Radar process error: %s", e)
            await asyncio.sleep(interval)

    async def run_service(self):
        """
        Run the daemon indefinitely, checking for new files every poll_interval seconds.
        """
        self._running = True
        logger.info(f"[{self.radar_name}] Starting continuous daemon with poll interval: {self.poll_interval} seconds")

        # Memory monitoring setup (per copilot-instructions.md Rule 5)
        _cycle_count = 0
        log_memory_usage(f"[{self.radar_name}] DownloadDaemon startup")

        await self._validate_stuck_volume_downloads()

        try:
            while self._running:
                try:
                    # Update heartbeat at start of each cycle
                    self._last_heartbeat = datetime.now(timezone.utc)

                    await self._run_ftp_poll_cycle(_cycle_count)
                    _cycle_count += 1

                    # Wait before next check — INSIDE try/except so CancelledError is caught
                    jitter = random.uniform(-30, 30)
                    await asyncio.sleep(max(10, self.poll_interval + jitter))

                except asyncio.CancelledError:
                    logger.info(f"[{self.radar_name}] Download daemon cancelled during cycle")
                    raise  # Re-raise to be caught by outer CancelledError handler
                except Exception as e:
                    logger.exception(f"[{self.radar_name}] Error during FTP poll cycle: {e}")
                    jitter = random.uniform(-30, 30)
                    await asyncio.sleep(max(10, self.poll_interval + jitter))

        except asyncio.CancelledError:
            logger.info(f"[{self.radar_name}] Download daemon cancelled, shutting down...")
        finally:
            self._running = False
            logger.info(f"[{self.radar_name}] Download daemon stopped")

    async def _run_ftp_poll_cycle(self, _cycle_count: int) -> None:
        """
        Execute a single FTP poll cycle with a timeout guard.

        Wraps the entire FTP connection + traversal + download in an
        asyncio.wait_for to prevent indefinite hangs on stale connections.
        The cancel event is signalled on timeout so the traversal thread
        exits cleanly at the next directory boundary instead of lingering.
        """
        self._cancel_traversal.clear()
        try:
            await asyncio.wait_for(
                self._ftp_poll_cycle_inner(_cycle_count),
                timeout=self.config.ftp_cycle_timeout,
            )
        except asyncio.TimeoutError:
            self._cancel_traversal.set()
            logger.error(
                f"[{self.radar_name}] FTP poll cycle timed out after {self.config.ftp_cycle_timeout}s. "
                f"This likely indicates a hung FTP connection. Will retry next cycle."
            )

    async def _ftp_poll_cycle_inner(self, _cycle_count: int) -> None:
        """Inner FTP poll cycle — connection, traversal, download, retry."""
        try:
            # Multi-volume resume logic: newest downloaded observation per volume.
            latest_by_vol: Dict[str, datetime] = {}
            if self._vol_types_config and isinstance(self._vol_types_config, dict):
                # Extract all unique volume numbers from all strategies
                all_volumes = set()
                for strategy_dict in self._vol_types_config.values():
                    if isinstance(strategy_dict, dict):
                        all_volumes.update(strategy_dict.keys())

                logger.debug(f"[{self.radar_name}] Checking latest downloads for volumes: {sorted(all_volumes)}")

                for vol_nr in sorted(all_volumes):
                    latest = self.state_tracker.get_latest_downloaded_file_by_volume(self.radar_name, vol_nr)
                    if latest and latest.get("observation_datetime"):
                        try:
                            latest_str = latest["observation_datetime"]
                            if isinstance(latest_str, str):
                                latest_date = datetime.fromisoformat(latest_str.replace("Z", "+00:00"))
                            else:
                                latest_date = latest_str
                            latest_by_vol[vol_nr] = latest_date
                            logger.debug(f"[{self.radar_name}]   vol{vol_nr}: {latest_date.isoformat()}")
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"[{self.radar_name}] Failed to parse observation_datetime for vol{vol_nr}: {e}"
                            )

            # Resume from the newest download minus a small backfill buffer, but keep a
            # scan cursor so a capped (backlog) window steps forward each cycle instead
            # of re-scanning the same range forever. Anchoring only to the newest
            # download froze RMA12/RMA17: a data gap longer than the forward reach
            # (window - backfill) left the anchor pinned behind the gap permanently.
            # The cursor marches the window through gaps/backlog until it reaches the
            # present. See compute_scan_window.
            now_utc = datetime.now(timezone.utc)
            resume_date, scan_end = compute_scan_window(
                latest_by_vol=latest_by_vol,
                scan_cursor=self._scan_cursor,
                now=now_utc,
                start_date=self.start_date,
                window_minutes=self.config.max_traversal_window_minutes,
            )

            if resume_date is None:
                # No downloads yet and no configured start date — nothing to scan.
                logger.warning(f"[{self.radar_name}] No start date configured; skipping cycle")
                return
            elif scan_end is None:
                logger.info(
                    f"[{self.radar_name}] Scanning from {resume_date.isoformat()} to present"
                    + (" (caught up from backlog)" if self._scan_cursor is not None else "")
                )
            else:
                logger.info(
                    f"[{self.radar_name}] Backlog detected — scanning capped {self.config.max_traversal_window_minutes}"
                    f" min window {resume_date.isoformat()} .. {scan_end.isoformat()}; advancing next cycle"
                )

            async with RadarFTPClientAsync(
                self.config.host,
                self.config.username,
                self.config.password,
                max_workers=self.config.max_concurrent_downloads,
                timeout=self.config.ftp_timeout,
            ) as client:
                logger.debug(f"[{self.radar_name}] Connected to FTP server. Checking for new files...")

                # Heartbeat refresh covers the entire cycle (traversal + downloads + retry)
                # so the watchdog only fires when the daemon is truly unresponsive, not
                # just busy with a large batch of retrying downloads.
                async def _refresh_heartbeat():
                    while True:
                        self._last_heartbeat = datetime.now(timezone.utc)
                        await asyncio.sleep(30)

                self._last_heartbeat = datetime.now(timezone.utc)
                _heartbeat_task = asyncio.create_task(_refresh_heartbeat())
                try:
                    files = await asyncio.to_thread(
                        self.new_bufr_files,
                        ftp_client=client,
                        start_date=resume_date,
                        end_date=scan_end,
                        vol_types=self.vol_types,
                        cancel_event=self._cancel_traversal,
                    )
                    # Traversal complete — release the control connection now.
                    # Parallel downloads use fresh per-file connections and don't need it.
                    # This keeps the concurrent connection count low across all containers.
                    client.disconnect()

                    # Advance the scan cursor now that this window is fully scanned.
                    # When capped (backlog), step to scan_end so the next cycle moves
                    # forward even if this window was empty (a data gap) — this is what
                    # unfreezes a stuck radar. When we reached the present (scan_end is
                    # None), clear the cursor to resume live-frontier tracking.
                    self._scan_cursor = scan_end

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
                                except FileNotFoundError as e:
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
                                    logger.warning(
                                        f"[{self.radar_name}] BUFR file missing during download"
                                        f" — marked as failed: {fname}: {e}"
                                    )
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

                    # Periodically retry failed downloads
                    await self._retry_failed_downloads_async()
                finally:
                    _heartbeat_task.cancel()
                    try:
                        await _heartbeat_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logger.exception(f"[{self.radar_name}] Error during FTP poll cycle: {e}")

    def new_bufr_files(
        self,
        ftp_client: RadarFTPClientAsync,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        vol_types: Optional[re.Pattern] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> list:
        """
        Get new BUFR files from FTP server within the specified date range.

        Args:
            ftp_client: RadarFTPClientAsync instance.
            start_date: Start date for searching files.
            end_date: End date for searching files.
            vol_types: Optional dictionary to filter volume types.
            cancel_event: If set mid-traversal, exits early at the next directory boundary.

        Returns:
            List of tuples (remote_path, local_path, filename, datetime, status).
        """
        candidates = []
        for dt, fname, remote in ftp_client.traverse_radar(
            self.radar_name,
            start_date,
            end_date,
            include_start=False,
            vol_types=vol_types,
            vol_types_dict=self._vol_types_config,
            cancel_event=cancel_event,
        ):
            # Skip already-downloaded files (deduplication for multi-volume race condition fix)
            if self.state_tracker.is_file_downloaded(fname, self.radar_name):
                logger.debug(f"[{self.radar_name}] Already downloaded, skipping: {fname}")
                continue

            local_path = self.local_dir / fname
            candidates.append((remote, local_path, fname, dt, "new"))
        return candidates

    async def _validate_stuck_volume_downloads(self) -> None:
        """
        One-shot startup check: find BUFR files whose local copy is corrupt.

        Queries all volumes currently in 'pending' or 'processing' state (i.e. stuck),
        looks up their completed downloads, and compares each local file size against
        the FTP SIZE command.  Any mismatch means the local file was corrupted during
        a previous download (partial transfer, dropped connection, etc.).

        For each corrupt file:
          - The local file is deleted so the processing daemon cannot attempt to decode it.
          - The downloads row is reset to 'failed' so the download daemon re-fetches it
            on the very next poll cycle via the normal retry path.

        Files on volumes that are no longer on the FTP server (old data purged) are
        skipped with a warning — the SIZE command returns None and no action is taken.

        This runs once at startup on a single persistent FTP connection (no extra
        connections are opened; SIZE is a control-channel command).
        """
        pending_volumes = self.state_tracker.get_volumes_by_status(
            "pending"
        ) + self.state_tracker.get_volumes_by_status("processing")

        if not pending_volumes:
            logger.debug(f"[{self.radar_name}] Startup validation: no stuck volumes found, skipping.")
            return

        logger.info(
            f"[{self.radar_name}] Startup validation: checking {len(pending_volumes)} stuck volume(s) "
            f"for corrupt local BUFR files..."
        )

        ftp_client = RadarFTPClientAsync(
            host=self.config.host,
            user=self.config.username,
            password=self.config.password,
            timeout=self.config.ftp_timeout,
        )

        volumes_checked = 0
        files_reset = 0

        try:
            for vol in pending_volumes:
                radar_name = vol.get("radar_name", self.radar_name)
                strategy = vol.get("strategy", "")
                vol_nr = vol.get("vol_nr", "")
                obs_dt = vol.get("observation_datetime", "")

                files = self.state_tracker.get_volume_files(radar_name, strategy, vol_nr, obs_dt)
                if not files:
                    continue

                volumes_checked += 1

                for f in files:
                    local_path = Path(f["local_path"])
                    remote_path = f.get("remote_path", "")
                    filename = f["filename"]

                    # File missing from disk entirely — reset so it gets re-downloaded
                    if not local_path.exists():
                        logger.warning(
                            f"[{self.radar_name}] Startup validation: {filename} missing from disk, "
                            f"resetting to failed for re-download."
                        )
                        self.state_tracker.reset_corrupt_download(filename)
                        files_reset += 1
                        continue

                    if not remote_path:
                        continue

                    remote_size = await asyncio.to_thread(ftp_client.get_remote_size, remote_path)

                    if remote_size is None:
                        logger.warning(
                            f"[{self.radar_name}] Startup validation: cannot get FTP size for "
                            f"{filename} (file purged or FTP error) — skipping."
                        )
                        continue

                    local_size = local_path.stat().st_size
                    if local_size != remote_size:
                        logger.warning(
                            f"[{self.radar_name}] Startup validation: corrupt local file detected — "
                            f"{filename} (local={local_size}B, ftp={remote_size}B). "
                            f"Deleting and resetting for re-download."
                        )
                        local_path.unlink(missing_ok=True)
                        self.state_tracker.reset_corrupt_download(filename)
                        files_reset += 1

        except Exception as e:
            logger.warning(
                f"[{self.radar_name}] Startup validation failed unexpectedly: {e}. "
                f"Continuing with normal operation."
            )
        finally:
            try:
                ftp_client.disconnect()
            except Exception:
                pass

        if files_reset:
            logger.info(
                f"[{self.radar_name}] Startup validation complete: checked {volumes_checked} stuck volume(s), "
                f"reset {files_reset} corrupt file(s) for re-download."
            )
        else:
            logger.info(
                f"[{self.radar_name}] Startup validation complete: checked {volumes_checked} stuck volume(s), "
                f"all local files are intact."
            )

    async def _retry_failed_downloads_async(self) -> None:
        """
        Periodically retry failed BUFR file downloads.

        This method runs every `failed_file_retry_interval` seconds and attempts
        to re-download files that previously failed due to FTP errors. Files that
        have been in 'failed' status for more than `failed_file_retention_days` are
        abandoned.
        """
        now = datetime.now(timezone.utc)

        # Check if enough time has passed since last retry attempt
        if self._last_failed_retry_time is not None:
            elapsed = (now - self._last_failed_retry_time).total_seconds()
            if elapsed < self.config.failed_file_retry_interval:
                # Not yet time to retry
                return

        # Get all failed files for this radar from the database
        try:
            conn = self.state_tracker._get_connection()
            cursor = conn.cursor()
            cutoff_datetime = now.timestamp() - (self.config.failed_file_retention_days * 86400)

            cursor.execute(
                """
                SELECT filename, remote_path, local_path, field_type, observation_datetime, updated_at
                FROM downloads
                WHERE radar_name = ? AND status = 'failed' AND updated_at > ?
                  AND (permanently_failed IS NULL OR permanently_failed = 0)
                ORDER BY updated_at DESC
                LIMIT 50
            """,
                (self.radar_name, cutoff_datetime),
            )
            failed_files = cursor.fetchall()

            if not failed_files:
                logger.debug(f"[{self.radar_name}] No failed files to retry")
                return

            logger.info(f"[{self.radar_name}] Retrying {len(failed_files)} failed downloads...")
            self._last_failed_retry_time = now

            # Retry each failed file.
            # No context manager — downloads use fresh per-file connections so no
            # persistent control connection is needed (avoids holding an idle socket).
            retry_count = 0
            client = RadarFTPClientAsync(
                self.config.host,
                self.config.username,
                self.config.password,
                max_workers=self.config.max_concurrent_downloads,
                timeout=self.config.ftp_timeout,
            )
            for failed_file in failed_files:
                filename = failed_file[0]
                remote_path = failed_file[1]
                local_path = Path(failed_file[2])
                field_type = failed_file[3]
                observation_datetime = failed_file[4]

                try:
                    logger.debug(f"[{self.radar_name}] Retrying failed download: {filename} " f"from {remote_path}")

                    # Remove local file if it partially exists
                    if local_path.exists():
                        try:
                            local_path.unlink()
                        except OSError:
                            pass

                    current_remote = Path(remote_path)
                    current_local = local_path

                    await exponential_backoff_retry(
                        lambda cr=current_remote, cl=current_local: client.download_file_async(cr, cl),
                        max_retries=self.config.bufr_download_max_retries,
                        base_delay=self.config.bufr_download_base_delay,
                        max_delay=self.config.bufr_download_max_delay,
                    )

                    # Mark as successfully downloaded
                    file_size = local_path.stat().st_size
                    components = extract_bufr_filename_components(filename)

                    self.state_tracker.mark_downloaded(
                        filename,
                        remote_path,
                        str(local_path),
                        file_size=file_size,
                        checksum=None,
                        radar_name=self.radar_name,
                        strategy=components["strategy"],
                        vol_nr=components["vol_nr"],
                        field_type=field_type,
                        observation_datetime=observation_datetime,
                    )

                    logger.info(f"[{self.radar_name}] Successfully retried: {filename}")
                    retry_count += 1
                    self._stats["failed_files_retried"] += 1

                except FTPError as e:
                    logger.warning(f"[{self.radar_name}] Retry still failing for {filename}: {e}")
                except FileNotFoundError as e:
                    self.state_tracker.mark_download_permanently_failed(filename)
                    logger.warning(
                        f"[{self.radar_name}] BUFR file removed before retry completed"
                        f" — marked permanently failed: {filename}: {e}"
                    )
                except Exception as e:
                    logger.error(f"[{self.radar_name}] Unexpected error retrying {filename}: {e}")
                finally:
                    gc.collect()

            if retry_count > 0:
                logger.info(
                    f"[{self.radar_name}] Retry attempt complete: {retry_count}/{len(failed_files)} "
                    f"files successfully recovered"
                )

        except Exception as e:
            logger.error(f"[{self.radar_name}] Error during failed file retry: {e}", exc_info=True)

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
