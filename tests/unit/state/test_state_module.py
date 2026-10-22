# -*- coding: utf-8 -*-
"""Tests for the state module."""


class TestStateImport:
    """Test that state tracking classes can be imported from the new location."""

    def test_import_sqlite_tracker(self):
        """Test importing SQLiteStateTracker from new location."""
        from radarlib.state import SQLiteStateTracker

        assert SQLiteStateTracker is not None

    def test_import_file_tracker(self):
        """Test importing FileStateTracker from new location."""
        from radarlib.state import FileStateTracker

        assert FileStateTracker is not None


class TestSQLiteTrackerNewLocation:
    """Tests for SQLiteStateTracker using the new location."""

    def test_sqlite_tracker_init(self, tmp_path):
        """Test SQLiteStateTracker initialization from new location."""
        from radarlib.state import SQLiteStateTracker

        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        assert tracker.count() == 0
        assert tracker.db_path == db_file
        assert db_file.exists()

        tracker.close()

    def test_sqlite_tracker_mark_downloaded(self, tmp_path):
        """Test marking a file as downloaded using new location import."""
        from radarlib.state import SQLiteStateTracker

        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/RMA1/file1.BUFR")

        assert tracker.is_downloaded("file1.BUFR")
        assert tracker.count() == 1

        tracker.close()


class TestFileTrackerNewLocation:
    """Tests for FileStateTracker using the new location."""

    def test_file_tracker_init(self, tmp_path):
        """Test FileStateTracker initialization from new location."""
        from radarlib.state import FileStateTracker

        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        assert tracker.count() == 0

    def test_file_tracker_mark_downloaded(self, tmp_path):
        """Test marking a file as downloaded using new location import."""
        from radarlib.state import FileStateTracker

        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")

        assert tracker.is_downloaded("file1.BUFR")
        assert tracker.count() == 1


class TestRetryCounterAndAbandon:
    """Regression tests for the incomplete-volume retry-counter fix.

    A missing field that was never downloaded has no `downloads` row, so the old
    UPDATE-only counter stayed at 0 forever and the field (and its volume) were
    retried indefinitely. The counter must now persist (upsert) and the volume must
    be abandonable.
    """

    def _tracker(self, tmp_path):
        from radarlib.state import SQLiteStateTracker

        return SQLiteStateTracker(tmp_path / "state.db")

    def test_retry_counter_persists_for_never_downloaded_field(self, tmp_path):
        t = self._tracker(tmp_path)
        f = "RMA8_0315_02_WRAD_20261015T071206Z.BUFR"
        assert t.get_retry_count_for_failed_file(f) == 0  # no row yet
        for expected in (1, 2, 3):
            t.increment_retry_count(
                f,
                "550",
                remote_path="/L2/x/" + f,
                radar_name="RMA8",
                strategy="0315",
                vol_nr="02",
                field_type="WRAD",
                observation_datetime="2026-10-15T07:12:06+00:00",
            )
            assert t.get_retry_count_for_failed_file(f) == expected

    def test_permanently_failed_applies_once_row_exists(self, tmp_path):
        t = self._tracker(tmp_path)
        f = "RMA8_0315_02_WRAD_20261015T071206Z.BUFR"
        t.increment_retry_count(f, "550", remote_path="/L2/x/" + f)  # creates the row
        t.mark_download_permanently_failed(f)
        import sqlite3

        con = sqlite3.connect(str(tmp_path / "state.db"))
        assert con.execute("SELECT permanently_failed FROM downloads WHERE filename=?", (f,)).fetchone()[0] == 1

    def test_abandon_after_max_retries_sequence(self, tmp_path):
        """Simulate the daemon loop: 3 attempts, then abandon on the 4th."""
        t = self._tracker(tmp_path)
        f = "RMA8_0315_02_WRAD_20261015T071206Z.BUFR"
        max_retries, abandoned_on = 3, None
        for cycle in range(1, 6):
            if t.get_retry_count_for_failed_file(f) >= max_retries:
                t.mark_download_permanently_failed(f)
                abandoned_on = cycle
                break
            t.increment_retry_count(f, "550", remote_path="/L2/x/" + f)
        assert abandoned_on == 4

    def test_mark_volume_abandoned_excluded_from_incomplete(self, tmp_path):
        t = self._tracker(tmp_path)
        vid = "RMA8_0315_02_2026-10-15T07:12:06+00:00"
        t.register_volume(vid, "RMA8", "0315", "02", "2026-10-15T07:12:06+00:00", "VRAD,WRAD", False)
        assert any(v["volume_id"] == vid for v in t.get_incomplete_volumes())
        t.mark_volume_abandoned(vid)
        assert not any(v["volume_id"] == vid for v in t.get_incomplete_volumes())
