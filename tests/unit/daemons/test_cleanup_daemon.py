# -*- coding: utf-8 -*-
"""Tests for the CleanupDaemon and cleanup-related functionality."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from radarlib.daemons import CleanupDaemon, CleanupDaemonConfig
from radarlib.state import SQLiteStateTracker


class TestCleanupDaemonImport:
    """Test that CleanupDaemon can be imported."""

    def test_import_cleanup_daemon(self):
        """Test importing CleanupDaemon from radarlib.daemons."""
        from radarlib.daemons import CleanupDaemon, CleanupDaemonConfig

        assert CleanupDaemon is not None
        assert CleanupDaemonConfig is not None


class TestCleanupDaemonConfig:
    """Tests for CleanupDaemonConfig."""

    def test_config_defaults(self, tmp_path):
        """Test default configuration values."""
        config = CleanupDaemonConfig(state_db=tmp_path / "state.db")

        assert config.poll_interval == 1800  # 30 minutes
        assert config.bufr_retention_days == 7
        assert config.netcdf_retention_days == 7
        assert config.enable_bufr_cleanup is True
        assert config.enable_netcdf_cleanup is True
        assert config.product_types == ["image"]
        assert config.max_files_per_cycle == 100
        assert config.dry_run is False
        assert config.radar_name is None

    def test_config_custom_values(self, tmp_path):
        """Test custom configuration values."""
        config = CleanupDaemonConfig(
            state_db=tmp_path / "state.db",
            radar_name="RMA1",
            poll_interval=3600,
            bufr_retention_days=14,
            netcdf_retention_days=30,
            enable_bufr_cleanup=False,
            enable_netcdf_cleanup=True,
            product_types=["image", "geotiff"],
            max_files_per_cycle=50,
            dry_run=True,
        )

        assert config.radar_name == "RMA1"
        assert config.poll_interval == 3600
        assert config.bufr_retention_days == 14
        assert config.netcdf_retention_days == 30
        assert config.enable_bufr_cleanup is False
        assert config.enable_netcdf_cleanup is True
        assert config.product_types == ["image", "geotiff"]
        assert config.max_files_per_cycle == 50
        assert config.dry_run is True


class TestCleanupDaemon:
    """Tests for CleanupDaemon."""

    @pytest.fixture
    def temp_dirs(self, tmp_path):
        """Create temporary directories for testing."""
        state_db = tmp_path / "state.db"
        bufr_dir = tmp_path / "bufr"
        bufr_dir.mkdir()
        netcdf_dir = tmp_path / "netcdf"
        netcdf_dir.mkdir()
        return state_db, bufr_dir, netcdf_dir

    def test_cleanup_daemon_init(self, temp_dirs):
        """Test CleanupDaemon initialization."""
        state_db, bufr_dir, netcdf_dir = temp_dirs
        config = CleanupDaemonConfig(
            state_db=state_db,
            radar_name="RMA1",
            bufr_retention_days=7,
        )

        daemon = CleanupDaemon(config)

        assert daemon.config == config
        assert daemon._running is False
        assert daemon._stats["bufr_files_cleaned"] == 0
        assert daemon._stats["netcdf_files_cleaned"] == 0

    def test_cleanup_daemon_get_stats(self, temp_dirs):
        """Test getting daemon statistics."""
        state_db, bufr_dir, netcdf_dir = temp_dirs
        config = CleanupDaemonConfig(state_db=state_db)

        daemon = CleanupDaemon(config)
        stats = daemon.get_stats()

        assert "running" in stats
        assert "bufr_files_cleaned" in stats
        assert "netcdf_files_cleaned" in stats
        assert "cycles_completed" in stats
        assert "db_stats" in stats

    def test_cleanup_daemon_stop(self, temp_dirs):
        """Test stopping the daemon."""
        state_db, bufr_dir, netcdf_dir = temp_dirs
        config = CleanupDaemonConfig(state_db=state_db)

        daemon = CleanupDaemon(config)
        daemon._running = True
        daemon.stop()

        assert daemon._running is False


class TestSQLiteTrackerCleanupMethods:
    """Tests for SQLiteStateTracker cleanup methods."""

    @pytest.fixture
    def tracker(self, tmp_path):
        """Create a SQLiteStateTracker for testing."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)
        yield tracker
        tracker.close()

    def test_cleanup_status_column_exists(self, tracker):
        """Test that cleanup_status column exists in downloads table."""
        conn = tracker._get_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(downloads)")
        columns = {row[1] for row in cursor.fetchall()}

        assert "cleanup_status" in columns
        assert "cleaned_at" in columns

    def test_volume_cleanup_status_column_exists(self, tracker):
        """Test that cleanup_status column exists in volume_processing table."""
        conn = tracker._get_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(volume_processing)")
        columns = {row[1] for row in cursor.fetchall()}

        assert "cleanup_status" in columns
        assert "cleaned_at" in columns

    def test_mark_bufr_cleanup_status(self, tracker):
        """Test marking a BUFR file's cleanup status."""
        # First add a file
        tracker.mark_downloaded(
            "test.BUFR",
            "/remote/test.BUFR",
            "/local/test.BUFR",
            radar_name="RMA1",
        )

        # Mark cleanup status
        result = tracker.mark_bufr_cleanup_status("test.BUFR", "pending_cleanup")
        assert result is True

        # Verify status was updated
        file_info = tracker.get_file_info("test.BUFR")
        assert file_info["cleanup_status"] == "pending_cleanup"

    def test_mark_bufr_cleaned_sets_timestamp(self, tracker):
        """Test that marking as 'cleaned' sets the cleaned_at timestamp."""
        # Add a file
        tracker.mark_downloaded(
            "test.BUFR",
            "/remote/test.BUFR",
            "/local/test.BUFR",
            radar_name="RMA1",
        )

        # Mark as cleaned
        tracker.mark_bufr_cleanup_status("test.BUFR", "cleaned")

        # Verify cleaned_at was set
        file_info = tracker.get_file_info("test.BUFR")
        assert file_info["cleanup_status"] == "cleaned"
        assert file_info["cleaned_at"] is not None

    def test_get_cleanup_stats(self, tracker):
        """Test getting cleanup statistics."""
        # Add some files
        tracker.mark_downloaded("file1.BUFR", "/remote/file1.BUFR", radar_name="RMA1")
        tracker.mark_downloaded("file2.BUFR", "/remote/file2.BUFR", radar_name="RMA1")

        # Mark one as cleaned
        tracker.mark_bufr_cleanup_status("file1.BUFR", "cleaned")

        stats = tracker.get_cleanup_stats()

        assert "bufr_active" in stats
        assert "bufr_cleaned" in stats
        assert stats["bufr_cleaned"] >= 1

    def test_can_redownload_bufr(self, tracker):
        """Test checking if a cleaned file can be re-downloaded."""
        # Add and clean a file
        tracker.mark_downloaded(
            "test.BUFR",
            "/remote/path/test.BUFR",
            "/local/test.BUFR",
            radar_name="RMA1",
            observation_datetime="2025-01-01T12:00:00+00:00",
        )
        tracker.mark_bufr_cleanup_status("test.BUFR", "cleaned")

        # Check if it can be re-downloaded
        redownload_info = tracker.can_redownload_bufr("test.BUFR")

        assert redownload_info is not None
        assert redownload_info["remote_path"] == "/remote/path/test.BUFR"
        assert redownload_info["radar_name"] == "RMA1"

    def test_can_redownload_bufr_not_cleaned(self, tracker):
        """Test that active files cannot be flagged for re-download."""
        # Add a file (not cleaned)
        tracker.mark_downloaded("test.BUFR", "/remote/test.BUFR", radar_name="RMA1")

        # Should return None since file is not cleaned
        redownload_info = tracker.can_redownload_bufr("test.BUFR")
        assert redownload_info is None

    def test_delete_file_safely_nonexistent(self, tracker):
        """Test that deleting a non-existent file returns True."""
        result = tracker.delete_file_safely("/nonexistent/path/file.BUFR")
        assert result is True


class TestCleanupDaemonIntegration:
    """Integration tests for CleanupDaemon."""

    @pytest.fixture
    def setup_test_data(self, tmp_path):
        """Set up test data with files and database entries."""
        state_db = tmp_path / "state.db"
        bufr_dir = tmp_path / "bufr"
        bufr_dir.mkdir()
        netcdf_dir = tmp_path / "netcdf"
        netcdf_dir.mkdir()

        tracker = SQLiteStateTracker(state_db)

        # Create old observation datetime (older than retention period)
        old_datetime = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()

        # Create BUFR files
        bufr_file1 = bufr_dir / "RMA1_0315_01_DBZH_20250101T120000Z.BUFR"
        bufr_file1.write_bytes(b"test data")

        # Add to database
        tracker.mark_downloaded(
            bufr_file1.name,
            f"/remote/{bufr_file1.name}",
            str(bufr_file1),
            radar_name="RMA1",
            strategy="0315",
            vol_nr="01",
            field_type="DBZH",
            observation_datetime=old_datetime,
        )

        # Register and complete volume processing
        volume_id = tracker.get_volume_id("RMA1", "0315", "01", old_datetime)
        tracker.register_volume(volume_id, "RMA1", "0315", "01", old_datetime, ["DBZH"], True)

        # Create NetCDF file
        netcdf_file = netcdf_dir / "RMA1_0315_01_20250101T120000Z.nc"
        netcdf_file.write_bytes(b"netcdf data")
        tracker.mark_volume_processing(volume_id, "completed", str(netcdf_file))

        # Register product generation as completed
        tracker.register_product_generation(volume_id, "image")
        tracker.mark_product_status(volume_id, "image", "completed")

        return {
            "state_db": state_db,
            "bufr_dir": bufr_dir,
            "netcdf_dir": netcdf_dir,
            "tracker": tracker,
            "bufr_file": bufr_file1,
            "netcdf_file": netcdf_file,
            "volume_id": volume_id,
        }

    @pytest.mark.asyncio
    async def test_dry_run_does_not_delete(self, setup_test_data):
        """Test that dry_run mode does not actually delete files."""
        data = setup_test_data

        config = CleanupDaemonConfig(
            state_db=data["state_db"],
            radar_name="RMA1",
            bufr_retention_days=7,
            netcdf_retention_days=7,
            dry_run=True,
        )

        daemon = CleanupDaemon(config)
        result = await daemon.run_once()

        # Files should still exist
        assert data["bufr_file"].exists()
        assert data["netcdf_file"].exists()

        # But cleanup should have been "successful" in dry run
        assert result["total_cleaned"] >= 0

        data["tracker"].close()

    @pytest.mark.asyncio
    async def test_cleanup_respects_retention_period(self, tmp_path):
        """Test that files newer than retention period are not cleaned."""
        state_db = tmp_path / "state.db"
        bufr_dir = tmp_path / "bufr"
        bufr_dir.mkdir()

        tracker = SQLiteStateTracker(state_db)

        # Create recent observation datetime (within retention period)
        recent_datetime = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        # Create BUFR file
        bufr_file = bufr_dir / "recent.BUFR"
        bufr_file.write_bytes(b"test data")

        # Add to database
        tracker.mark_downloaded(
            bufr_file.name,
            f"/remote/{bufr_file.name}",
            str(bufr_file),
            radar_name="RMA1",
            strategy="0315",
            vol_nr="01",
            field_type="DBZH",
            observation_datetime=recent_datetime,
        )

        # Set up volume and product generation
        volume_id = tracker.get_volume_id("RMA1", "0315", "01", recent_datetime)
        tracker.register_volume(volume_id, "RMA1", "0315", "01", recent_datetime, ["DBZH"], True)
        tracker.mark_volume_processing(volume_id, "completed", str(tmp_path / "test.nc"))
        tracker.register_product_generation(volume_id, "image")
        tracker.mark_product_status(volume_id, "image", "completed")

        # Get files ready for cleanup
        files = tracker.get_bufr_files_for_cleanup(7, "RMA1", ["image"])

        # Should be empty since file is only 1 day old
        assert len(files) == 0

        tracker.close()


class TestDaemonManagerWithCleanup:
    """Tests for DaemonManager with CleanupDaemon."""

    def test_daemon_manager_config_has_cleanup_options(self):
        """Test that DaemonManagerConfig includes cleanup options."""
        from radarlib.daemons import DaemonManagerConfig

        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=Path("/tmp/test"),
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={},
        )

        # Check cleanup-related attributes exist
        assert hasattr(config, "enable_cleanup_daemon")
        assert hasattr(config, "cleanup_poll_interval")
        assert hasattr(config, "bufr_retention_days")
        assert hasattr(config, "netcdf_retention_days")
        assert hasattr(config, "cleanup_product_types")

        # Check defaults
        assert config.enable_cleanup_daemon is False  # Disabled by default
        assert config.cleanup_poll_interval == 1800
        assert config.bufr_retention_days == 7
        assert config.netcdf_retention_days == 7
