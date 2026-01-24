# -*- coding: utf-8 -*-
"""Unit tests for radarlib.daemons.processing_daemon module.

Tests the ProcessingDaemon and ProcessingDaemonConfig classes.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from radarlib.daemons.processing_daemon import ProcessingDaemon, ProcessingDaemonConfig


class TestProcessingDaemonConfig:
    """Tests for ProcessingDaemonConfig class."""

    def test_init_with_required_params(self, tmp_path):
        """Should initialize with required parameters."""
        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH", "DBZV"]}},
            radar_name="RMA1",
        )

        assert config.local_bufr_dir == tmp_path / "bufr"
        assert config.local_netcdf_dir == tmp_path / "netcdf"
        assert config.radar_name == "RMA1"

    def test_init_default_values(self, tmp_path):
        """Should use default values for optional parameters."""
        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        assert config.poll_interval == 30
        assert config.max_concurrent_processing == 2
        assert config.root_resources is None
        assert config.allow_incomplete is False
        assert config.incomplete_timeout_hours == 24
        assert config.stuck_volume_timeout_minutes == 60

    def test_init_custom_values(self, tmp_path):
        """Should accept custom values."""
        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
            poll_interval=60,
            max_concurrent_processing=4,
            allow_incomplete=True,
            stuck_volume_timeout_minutes=120,
        )

        assert config.poll_interval == 60
        assert config.max_concurrent_processing == 4
        assert config.allow_incomplete is True
        assert config.stuck_volume_timeout_minutes == 120

    def test_post_init_sets_start_date(self, tmp_path):
        """Should set start_date to now UTC rounded to hour if not provided."""
        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        assert config.start_date is not None
        assert config.start_date.minute == 0
        assert config.start_date.second == 0
        assert config.start_date.microsecond == 0

    def test_custom_start_date(self, tmp_path):
        """Should use provided start_date."""
        custom_date = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
            start_date=custom_date,
        )

        assert config.start_date == custom_date


class TestProcessingDaemon:
    """Tests for ProcessingDaemon class."""

    @pytest.fixture
    def daemon_config(self, tmp_path):
        """Create a daemon configuration for testing."""
        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH", "DBZV"], "02": ["VRAD"]}},
            radar_name="RMA1",
            poll_interval=1,  # Short for testing
        )
        return config

    @patch("radarlib.daemons.processing_daemon.SQLiteStateTracker")
    def test_init_creates_output_dir(self, mock_tracker, daemon_config):
        """Should create output directory on init."""

        assert daemon_config.local_netcdf_dir.exists()

    @patch("radarlib.daemons.processing_daemon.SQLiteStateTracker")
    def test_init_initializes_stats(self, mock_tracker, daemon_config):
        """Should initialize statistics counters."""
        daemon = ProcessingDaemon(daemon_config)

        assert daemon._stats["volumes_processed"] == 0
        assert daemon._stats["volumes_failed"] == 0
        assert daemon._stats["incomplete_volumes_detected"] == 0

    @patch("radarlib.daemons.processing_daemon.SQLiteStateTracker")
    def test_stop_sets_running_to_false(self, mock_tracker, daemon_config):
        """Should set _running to False when stop is called."""
        daemon = ProcessingDaemon(daemon_config)
        daemon._running = True

        daemon.stop()

        assert daemon._running is False

    @patch("radarlib.daemons.processing_daemon.SQLiteStateTracker")
    @pytest.mark.asyncio
    async def test_check_and_reset_stuck_volumes(self, mock_tracker_class, daemon_config):
        """Should call state_tracker.reset_stuck_volumes."""
        mock_tracker = MagicMock()
        mock_tracker.reset_stuck_volumes.return_value = 2
        mock_tracker_class.return_value = mock_tracker

        daemon = ProcessingDaemon(daemon_config)

        await daemon._check_and_reset_stuck_volumes()

        mock_tracker.reset_stuck_volumes.assert_called_once_with(daemon_config.stuck_volume_timeout_minutes)


class TestProcessingDaemonIntegration:
    """Integration tests for ProcessingDaemon."""

    @patch("radarlib.daemons.processing_daemon.SQLiteStateTracker")
    def test_daemon_config_volume_types_structure(self, mock_tracker, tmp_path):
        """Volume types should have correct structure."""
        volume_types = {
            "0315": {
                "01": ["DBZH", "DBZV", "ZDR"],
                "02": ["VRAD", "WRAD"],
            },
            "0320": {
                "01": ["RHOHV", "KDP"],
            },
        }

        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types=volume_types,
            radar_name="RMA1",
        )

        daemon = ProcessingDaemon(config)

        assert "0315" in daemon.config.volume_types
        assert "01" in daemon.config.volume_types["0315"]
        assert "DBZH" in daemon.config.volume_types["0315"]["01"]

    @patch("radarlib.daemons.processing_daemon.SQLiteStateTracker")
    def test_daemon_uses_state_tracker(self, mock_tracker_class, tmp_path):
        """Daemon should use SQLiteStateTracker."""
        mock_tracker = MagicMock()
        mock_tracker_class.return_value = mock_tracker

        config = ProcessingDaemonConfig(
            local_bufr_dir=tmp_path / "bufr",
            local_netcdf_dir=tmp_path / "netcdf",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        daemon = ProcessingDaemon(config)

        mock_tracker_class.assert_called_once_with(tmp_path / "state.db")
        assert daemon.state_tracker == mock_tracker
