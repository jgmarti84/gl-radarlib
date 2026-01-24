# -*- coding: utf-8 -*-
"""Unit tests for radarlib.daemons.manager module.

Tests the DaemonManager and DaemonManagerConfig classes.
"""

from datetime import datetime, timezone

import pytest

from radarlib.daemons.manager import DaemonManager, DaemonManagerConfig


class TestDaemonManagerConfig:
    """Tests for DaemonManagerConfig class."""

    def test_init_with_required_params(self, tmp_path):
        """Should initialize with required parameters."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="testuser",
            ftp_password="testpass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH", "DBZV"]}},
        )

        assert config.radar_name == "RMA1"
        assert config.base_path == tmp_path
        assert config.ftp_host == "ftp.example.com"
        assert config.ftp_user == "testuser"

    def test_init_default_values(self, tmp_path):
        """Should use default values for optional parameters."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="testuser",
            ftp_password="testpass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
        )

        assert config.download_poll_interval == 60
        assert config.processing_poll_interval == 30
        assert config.product_poll_interval == 30
        assert config.cleanup_poll_interval == 1800
        assert config.enable_download_daemon is True
        assert config.enable_processing_daemon is True
        assert config.enable_product_daemon is True
        assert config.enable_cleanup_daemon is False  # Disabled by default
        assert config.product_type == "image"
        assert config.add_colmax is True
        assert config.bufr_retention_days == 7
        assert config.netcdf_retention_days == 7

    def test_init_custom_values(self, tmp_path):
        """Should accept custom values."""
        start_date = datetime.now(timezone.utc)
        config = DaemonManagerConfig(
            radar_name="RMA2",
            base_path=tmp_path,
            ftp_host="radar.server.com",
            ftp_user="admin",
            ftp_password="secret",
            ftp_base_path="/RADAR",
            volume_types={"0315": {"01": ["DBZH"]}},
            start_date=start_date,
            download_poll_interval=120,
            enable_download_daemon=False,
            enable_cleanup_daemon=True,
            product_type="geotiff",
        )

        assert config.radar_name == "RMA2"
        assert config.start_date == start_date
        assert config.download_poll_interval == 120
        assert config.enable_download_daemon is False
        assert config.enable_cleanup_daemon is True
        assert config.product_type == "geotiff"

    def test_post_init_sets_start_date_if_none(self, tmp_path):
        """Should set start_date to now UTC if not provided."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
        )

        assert config.start_date is not None
        assert config.start_date.tzinfo is not None

    def test_post_init_raises_for_naive_datetime(self, tmp_path):
        """Should raise ValueError for timezone-naive start_date."""
        with pytest.raises(ValueError, match="timezone-aware"):
            DaemonManagerConfig(
                radar_name="RMA1",
                base_path=tmp_path,
                ftp_host="ftp.example.com",
                ftp_user="user",
                ftp_password="pass",
                ftp_base_path="/L2",
                volume_types={"0315": {"01": ["DBZH"]}},
                start_date=datetime.now(),  # Naive datetime
            )


class TestDaemonManager:
    """Tests for DaemonManager class."""

    @pytest.fixture
    def manager_config(self, tmp_path):
        """Create a manager configuration for testing."""
        return DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH", "DBZV"], "02": ["VRAD"]}},
        )

    def test_init_creates_directories(self, manager_config):
        """Should create bufr, netcdf, and product directories."""
        manager = DaemonManager(manager_config)

        assert manager.bufr_dir.exists()
        assert manager.netcdf_dir.exists()
        assert manager.product_dir.exists()

    def test_init_sets_paths(self, manager_config):
        """Should set correct paths based on base_path."""
        manager = DaemonManager(manager_config)

        assert manager.bufr_dir == manager_config.base_path / "bufr"
        assert manager.netcdf_dir == manager_config.base_path / "netcdf"
        assert manager.product_dir == manager_config.base_path / "products"
        assert manager.state_db == manager_config.base_path / "state.db"

    def test_init_custom_dirs(self, tmp_path):
        """Should use custom directories if provided."""
        custom_bufr = tmp_path / "custom_bufr"
        custom_netcdf = tmp_path / "custom_netcdf"
        custom_product = tmp_path / "custom_product"

        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
            bufr_dir=custom_bufr,
            netcdf_dir=custom_netcdf,
            product_dir=custom_product,
        )

        manager = DaemonManager(config)

        assert manager.bufr_dir == custom_bufr
        assert manager.netcdf_dir == custom_netcdf
        assert manager.product_dir == custom_product

    def test_init_running_false(self, manager_config):
        """Should initialize _running to False."""
        manager = DaemonManager(manager_config)

        assert manager._running is False

    def test_init_daemons_none(self, manager_config):
        """Should initialize daemons to None."""
        manager = DaemonManager(manager_config)

        assert manager.download_daemon is None
        assert manager.processing_daemon is None
        assert manager.product_daemon is None
        assert manager.cleanup_daemon is None

    def test_stop_sets_running_false(self, manager_config):
        """Should set _running to False."""
        manager = DaemonManager(manager_config)
        manager._running = True

        manager.stop()

        assert manager._running is False

    def test_update_config_valid_key(self, manager_config):
        """Should update valid configuration parameters."""
        manager = DaemonManager(manager_config)

        manager.update_config(product_type="geotiff")

        assert manager.config.product_type == "geotiff"

    def test_update_config_invalid_key(self, manager_config, caplog):
        """Should warn for invalid configuration parameters."""
        import logging

        manager = DaemonManager(manager_config)

        with caplog.at_level(logging.WARNING):
            manager.update_config(invalid_param="value")

        assert "Unknown config parameter" in caplog.text

    def test_get_status_structure(self, manager_config):
        """Should return status dictionary with expected structure."""
        manager = DaemonManager(manager_config)

        status = manager.get_status()

        assert "manager_running" in status
        assert "radar_code" in status
        assert "base_path" in status
        assert "download_daemon" in status
        assert "processing_daemon" in status
        assert "product_daemon" in status
        assert "cleanup_daemon" in status

    def test_get_status_values(self, manager_config):
        """Should return correct status values."""
        manager = DaemonManager(manager_config)

        status = manager.get_status()

        assert status["manager_running"] is False
        assert status["radar_code"] == "RMA1"
        assert status["download_daemon"]["enabled"] is True
        assert status["download_daemon"]["running"] is False


class TestDaemonManagerIntegration:
    """Integration tests for DaemonManager."""

    def test_volume_types_propagate(self, tmp_path):
        """Volume types should be properly configured."""
        volume_types = {
            "0315": {
                "01": ["DBZH", "DBZV", "ZDR"],
                "02": ["VRAD", "WRAD"],
            },
        }

        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types=volume_types,
        )

        manager = DaemonManager(config)

        assert manager.config.volume_types == volume_types

    def test_cleanup_product_types_default(self, tmp_path):
        """Cleanup product types should default to ['image']."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
        )

        assert config.cleanup_product_types == ["image"]

    def test_cleanup_product_types_custom(self, tmp_path):
        """Should accept custom cleanup product types."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=tmp_path,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
            cleanup_product_types=["image", "geotiff"],
        )

        assert config.cleanup_product_types == ["image", "geotiff"]
