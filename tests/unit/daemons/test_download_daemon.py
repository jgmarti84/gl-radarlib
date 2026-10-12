# -*- coding: utf-8 -*-
"""Unit tests for radarlib.daemons.download_daemon module."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
        # Backfill cursor already caught up to the live window → these tests exercise
        # only the live stream (one scan), so download counts stay deterministic.
        tracker_instance.get_cursor.return_value = datetime.now(timezone.utc).isoformat()
        tracker_instance.set_cursor.return_value = None
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
            latest_by_vol={},
            scan_cursor=None,
            now=now,
            start_date=None,
            window_minutes=self.WIN,
        )
        assert resume is None and scan_end is None

    def test_lagging_volume_does_not_pin_window(self):
        """Anchoring to max (not min) means one stale volume can't drag the
        window back (the old min+60 zombie-growth failure mode)."""
        now = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)
        latest = {
            "01": now - timedelta(minutes=9),  # fresh
            "02": now - timedelta(hours=3),  # badly lagging / infrequent
        }
        resume, scan_end = self._win(latest, None, now)
        assert resume == (now - timedelta(minutes=9)) - timedelta(minutes=self.BACKFILL)
        assert scan_end is None


class TestLiveFirstAndBackfill:
    """Live-first download: current window scanned first every cycle, old data
    backfilled as a trickle via a persisted cursor (Option A)."""

    def _daemon(self, tmp_path, *, initial_cursor=None, latest_download=None):
        config = _make_config(tmp_path)
        config.live_window_minutes = 180
        config.max_traversal_window_minutes = 30
        config.backfill_windows_per_cycle = 1
        config.enable_backfill = True
        with patch("radarlib.daemons.download_daemon.SQLiteStateTracker") as MockTracker:
            t = MagicMock()
            store = {}
            if initial_cursor is not None:
                store["backfill"] = initial_cursor
            t.get_cursor.side_effect = lambda radar, name: store.get(name)
            t.set_cursor.side_effect = lambda radar, name, value: store.__setitem__(name, value)
            t.clear_cursor.side_effect = lambda radar, name: store.pop(name, None)
            t.get_latest_downloaded_file_by_volume.return_value = (
                {"observation_datetime": latest_download} if latest_download else None
            )
            t.get_failed_downloads_for_retry.return_value = []
            MockTracker.return_value = t
            daemon = DownloadDaemon(config)
        daemon._store = store
        # single-volume config so _initial_backfill_cursor has a volume to look up
        daemon._vol_types_config = {"0315": {"01": ["DBZH"]}}
        return daemon

    async def _run_cycle(self, daemon):
        calls = []

        async def fake_scan(client, resume, scan_end, label):
            calls.append((label, resume, scan_end))
            return 0

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        with (
            patch.object(daemon, "_scan_and_download", side_effect=fake_scan),
            patch("radarlib.daemons.download_daemon.RadarFTPClientAsync", return_value=mock_client),
            patch.object(daemon, "_retry_failed_downloads_async", new_callable=AsyncMock),
        ):
            await daemon._ftp_poll_cycle_inner(_cycle_count=0)
        return calls

    @pytest.mark.asyncio
    async def test_live_runs_first_scanning_recent_window_to_present(self, tmp_path):
        now = datetime.now(timezone.utc)
        stall = (now - timedelta(days=2)).isoformat()
        daemon = self._daemon(tmp_path, latest_download=stall)
        calls = await self._run_cycle(daemon)
        assert calls[0][0] == "live"
        assert calls[0][2] is None  # scan to present
        assert abs((calls[0][1] - (now - timedelta(minutes=180))).total_seconds()) < 180

    @pytest.mark.asyncio
    async def test_backfill_trickles_one_bounded_window_from_the_hole(self, tmp_path):
        now = datetime.now(timezone.utc)
        stall = (now - timedelta(days=2)).isoformat()
        daemon = self._daemon(tmp_path, latest_download=stall)
        calls = await self._run_cycle(daemon)
        backfill = [c for c in calls if c[0] == "backfill"]
        assert len(backfill) == 1  # trickle = 1 window/cycle
        _, resume, end = backfill[0]
        assert end is not None and (end - resume) == timedelta(minutes=30)  # bounded
        # started at the pre-live frontier (the stall point)
        assert abs((resume - datetime.fromisoformat(stall)).total_seconds()) < 1

    @pytest.mark.asyncio
    async def test_backfill_cursor_advances_and_persists(self, tmp_path):
        now = datetime.now(timezone.utc)
        stall = (now - timedelta(days=2)).isoformat()
        daemon = self._daemon(tmp_path, latest_download=stall)
        await self._run_cycle(daemon)
        advanced = datetime.fromisoformat(daemon._store["backfill"])
        assert advanced == datetime.fromisoformat(stall) + timedelta(minutes=30)

    @pytest.mark.asyncio
    async def test_no_backfill_when_caught_up(self, tmp_path):
        now = datetime.now(timezone.utc)
        # cursor already at the live edge → nothing older to fill
        daemon = self._daemon(tmp_path, initial_cursor=now.isoformat(), latest_download=now.isoformat())
        calls = await self._run_cycle(daemon)
        assert [c[0] for c in calls] == ["live"]

    @pytest.mark.asyncio
    async def test_persisted_cursor_is_not_reinitialised(self, tmp_path):
        now = datetime.now(timezone.utc)
        mid = (now - timedelta(days=1)).isoformat()
        daemon = self._daemon(tmp_path, initial_cursor=mid, latest_download=(now - timedelta(days=5)).isoformat())
        with patch.object(daemon, "_initial_backfill_cursor", side_effect=AssertionError("should not re-init")):
            calls = await self._run_cycle(daemon)
        backfill = [c for c in calls if c[0] == "backfill"]
        # backfill resumes from the persisted cursor (1 day ago), not the 5-day frontier
        assert abs((backfill[0][1] - datetime.fromisoformat(mid)).total_seconds()) < 1


class TestDaemonCursorStore:
    """Durable backfill cursor persistence in SQLiteStateTracker."""

    def test_cursor_roundtrip_and_upsert(self, tmp_path):
        from radarlib.state.sqlite_tracker import SQLiteStateTracker

        t = SQLiteStateTracker(tmp_path / "state.db")
        assert t.get_cursor("RMA10", "backfill") is None
        t.set_cursor("RMA10", "backfill", "2026-09-29T00:00:00+00:00")
        assert t.get_cursor("RMA10", "backfill") == "2026-09-29T00:00:00+00:00"
        t.set_cursor("RMA10", "backfill", "2026-09-29T00:30:00+00:00")  # upsert
        assert t.get_cursor("RMA10", "backfill") == "2026-09-29T00:30:00+00:00"
        assert t.get_cursor("RMA11", "backfill") is None  # per-radar isolation
        t.clear_cursor("RMA10", "backfill")
        assert t.get_cursor("RMA10", "backfill") is None
        t.clear_cursor("RMA10", "backfill")  # idempotent (no error if absent)


class TestBackfillOriginFloor:
    """The backfill origin is published for the processing daemon's completeness
    floor (so backward-filled old data gets registered/processed), and cleared
    once the backfill catches up."""

    def _daemon(self, tmp_path, *, initial_cursor=None, latest_download=None):
        return TestLiveFirstAndBackfill._daemon(
            TestLiveFirstAndBackfill(), tmp_path, initial_cursor=initial_cursor, latest_download=latest_download
        )

    async def _run_cycle(self, daemon):
        return await TestLiveFirstAndBackfill._run_cycle(TestLiveFirstAndBackfill(), daemon)

    @pytest.mark.asyncio
    async def test_origin_published_when_hole_exists(self, tmp_path):
        now = datetime.now(timezone.utc)
        stall = (now - timedelta(days=2)).isoformat()
        daemon = self._daemon(tmp_path, latest_download=stall)
        await self._run_cycle(daemon)
        assert "backfill_origin" in daemon._store
        # origin == the pre-live frontier (stall point), so processing scans from there
        assert (
            abs(
                (
                    datetime.fromisoformat(daemon._store["backfill_origin"]) - datetime.fromisoformat(stall)
                ).total_seconds()
            )
            < 1
        )

    @pytest.mark.asyncio
    async def test_origin_cleared_when_caught_up(self, tmp_path):
        now = datetime.now(timezone.utc)
        daemon = self._daemon(tmp_path, initial_cursor=now.isoformat(), latest_download=now.isoformat())
        # pretend a stale origin is lingering
        daemon._store["backfill_origin"] = (now - timedelta(days=5)).isoformat()
        await self._run_cycle(daemon)
        assert "backfill_origin" not in daemon._store
