# -*- coding: utf-8 -*-
"""Unit tests for radarlib.daemons.download_daemon module."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datetime import timedelta

from radarlib.daemons.download_daemon import DownloadDaemon, DownloadDaemonConfig, compute_scan_window


def _make_config(tmp_path):
    return DownloadDaemonConfig(
        host="ftp.example.com",
        username="user",
        password="pass",
        radar_name="AR8",
        remote_base_path="L2",
        local_bufr_dir=tmp_path / "bufr",
        state_db=tmp_path / "state.db",
        start_date=datetime(2026, 4, 26, tzinfo=timezone.utc),
        max_concurrent_downloads=5,
    )


def _make_daemon(config):
    """Construct a DownloadDaemon with a fully mocked SQLiteStateTracker."""
    with patch("radarlib.daemons.download_daemon.SQLiteStateTracker") as MockTracker:
        tracker_instance = MagicMock()
        tracker_instance.get_latest_downloaded_file_by_volume.return_value = None
        tracker_instance.is_file_downloaded.return_value = False
        tracker_instance.mark_downloaded.return_value = None
        tracker_instance.mark_failed.return_value = None
        tracker_instance.get_failed_downloads_for_retry.return_value = []
        MockTracker.return_value = tracker_instance
        daemon = DownloadDaemon(config)
    return daemon


def _make_files(n, tmp_path):
    """Build N fake (remote, local, fname, dt, status) tuples."""
    files = []
    for i in range(n):
        dt = datetime(2026, 4, 28, 19, i, 0, tzinfo=timezone.utc)
        fname = f"AR8_1000_1_DBZH_20260428T19{i:02d}00Z.BUFR"
        remote = Path(f"/L2/AR8/2026/04/28/19/{i:02d}00/{fname}")
        local = tmp_path / "bufr" / fname
        files.append((remote, local, fname, dt, "pending"))
    return files


async def _passthrough_retry(fn, **kw):
    """Drop-in replacement for exponential_backoff_retry that just calls fn() once."""
    return await fn()


class TestDownloadTaskCreation:
    """Verify that a task is created for every file returned by new_bufr_files."""

    @pytest.mark.asyncio
    async def test_all_files_get_a_task(self, tmp_path):
        """Every file in new_bufr_files must produce one download task."""
        n_files = 10
        config = _make_config(tmp_path)
        daemon = _make_daemon(config)
        fake_files = _make_files(n_files, tmp_path)
        downloaded = []

        async def fake_download(remote_path, local_path):
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(b"BUFR")
            downloaded.append(local_path.name)
            return local_path

        mock_client = MagicMock()
        mock_client.download_file_async = fake_download
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.object(daemon, "new_bufr_files", return_value=fake_files),
            patch("radarlib.daemons.download_daemon.RadarFTPClientAsync", return_value=mock_client),
            patch("radarlib.daemons.download_daemon.exponential_backoff_retry", new=_passthrough_retry),
            patch.object(daemon, "_retry_failed_downloads_async", new_callable=AsyncMock),
        ):
            await daemon._ftp_poll_cycle_inner(_cycle_count=0)

        assert len(downloaded) == n_files, (
            f"Expected {n_files} downloads but only {len(downloaded)} were triggered. "
            f"Off-by-one indentation bug: tasks.append() may still be outside the for-loop. "
            f"Downloaded: {downloaded}"
        )

    @pytest.mark.asyncio
    async def test_regression_all_files_not_just_last(self, tmp_path):
        """Regression: the off-by-one indentation bug only downloaded the LAST file per cycle."""
        config = _make_config(tmp_path)
        daemon = _make_daemon(config)
        fake_files = _make_files(3, tmp_path)
        downloaded_names = []

        async def fake_download(remote_path, local_path):
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(b"BUFR")
            downloaded_names.append(local_path.name)
            return local_path

        mock_client = MagicMock()
        mock_client.download_file_async = fake_download
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.object(daemon, "new_bufr_files", return_value=fake_files),
            patch("radarlib.daemons.download_daemon.RadarFTPClientAsync", return_value=mock_client),
            patch("radarlib.daemons.download_daemon.exponential_backoff_retry", new=_passthrough_retry),
            patch.object(daemon, "_retry_failed_downloads_async", new_callable=AsyncMock),
        ):
            await daemon._ftp_poll_cycle_inner(_cycle_count=0)

        expected = {fname for _, _, fname, _, _ in fake_files}
        assert set(downloaded_names) == expected, (
            f"Not all files were downloaded.\n"
            f"  Expected : {sorted(expected)}\n"
            f"  Got      : {sorted(downloaded_names)}"
        )


class TestComputeScanWindow:
    """Tests for the gap-tolerant scan window used by the download daemon.

    These lock in the fix for the 'gap ratchet' that froze RMA12/RMA17: a data
    gap longer than (window - backfill) must not pin the scan window in the past.
    """

    WIN = 30  # window_minutes
    BACKFILL = 15

    def _win(self, latest_by_vol, cursor, now):
        return compute_scan_window(
            latest_by_vol=latest_by_vol,
            scan_cursor=cursor,
            now=now,
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            window_minutes=self.WIN,
            backfill_minutes=self.BACKFILL,
        )

    def test_live_frontier_scans_to_present(self):
        """Caught up (recent download): resume = max-backfill, scan to present."""
        now = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)
        latest = {"01": now - timedelta(minutes=9)}  # 9-min lag, well within reach
        resume, scan_end = self._win(latest, None, now)
        assert resume == latest["01"] - timedelta(minutes=self.BACKFILL)
        assert scan_end is None  # scans to present

    def test_backlog_window_is_capped(self):
        """When behind, the window is capped to window_minutes (no runaway scan)."""
        now = datetime(2026, 10, 10, 20, 0, 0, tzinfo=timezone.utc)
        latest = {"01": datetime(2026, 10, 10, 17, 11, 59, tzinfo=timezone.utc)}
        resume, scan_end = self._win(latest, None, now)
        assert resume == datetime(2026, 10, 10, 16, 56, 59, tzinfo=timezone.utc)
        assert scan_end == resume + timedelta(minutes=self.WIN)  # capped, not to present

    def test_cursor_steps_window_forward(self):
        """A cursor ahead of the download anchor moves the window forward."""
        now = datetime(2026, 10, 10, 20, 0, 0, tzinfo=timezone.utc)
        latest = {"01": datetime(2026, 10, 10, 17, 11, 59, tzinfo=timezone.utc)}
        cursor = datetime(2026, 10, 10, 17, 26, 59, tzinfo=timezone.utc)
        resume, scan_end = self._win(latest, cursor, now)
        assert resume == cursor  # steps forward past the stale anchor
        assert scan_end == cursor + timedelta(minutes=self.WIN)

    def test_gap_is_escaped_by_stepping_to_present(self):
        """RMA12-style replay: a >15-min data gap must not pin the window.

        Starting frozen (anchor 17:11:59, gap until 17:38), drive the cursor the
        way the daemon does (advance to scan_end after each capped cycle) and
        confirm it reaches the present in a bounded number of steps."""
        now = datetime(2026, 10, 10, 20, 0, 0, tzinfo=timezone.utc)
        latest = {"01": datetime(2026, 10, 10, 17, 11, 59, tzinfo=timezone.utc)}
        cursor = None
        reached_present = False
        for _ in range(12):  # generous bound; ~6 windows needed for ~3h backlog
            resume, scan_end = self._win(latest, cursor, now)
            assert resume is not None
            if scan_end is None:
                reached_present = True
                break
            # daemon advances the cursor to scan_end after scanning a capped window
            cursor = scan_end
        assert reached_present, "window never caught up to the present (still frozen)"

    def test_caught_up_clears_cursor_via_none_scan_end(self):
        """Once the stepped window reaches the present, scan_end is None so the
        daemon clears the cursor and returns to live-frontier tracking."""
        now = datetime(2026, 10, 10, 20, 0, 0, tzinfo=timezone.utc)
        latest = {"01": datetime(2026, 10, 10, 17, 11, 59, tzinfo=timezone.utc)}
        cursor = now - timedelta(minutes=10)  # cursor near present
        resume, scan_end = self._win(latest, cursor, now)
        assert scan_end is None  # resume + 30min > now → caught up

    def test_no_downloads_no_start_date_returns_none(self):
        now = datetime(2026, 10, 10, 20, 0, 0, tzinfo=timezone.utc)
        resume, scan_end = compute_scan_window(
            latest_by_vol={}, scan_cursor=None, now=now,
            start_date=None, window_minutes=self.WIN,
        )
        assert resume is None and scan_end is None

    def test_lagging_volume_does_not_pin_window(self):
        """Anchoring to max (not min) means one stale volume can't drag the
        window back (the old min+60 zombie-growth failure mode)."""
        now = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)
        latest = {
            "01": now - timedelta(minutes=9),    # fresh
            "02": now - timedelta(hours=3),      # badly lagging / infrequent
        }
        resume, scan_end = self._win(latest, None, now)
        assert resume == (now - timedelta(minutes=9)) - timedelta(minutes=self.BACKFILL)
        assert scan_end is None
