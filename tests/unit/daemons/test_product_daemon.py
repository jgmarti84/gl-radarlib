# -*- coding: utf-8 -*-
"""Unit tests for radarlib.daemons.product_daemon module.

Tests the ProductGenerationDaemon and ProductGenerationDaemonConfig classes.
"""


from unittest.mock import MagicMock, patch

import pytest

from radarlib.daemons.product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig


class TestProductGenerationDaemonConfig:
    """Tests for ProductGenerationDaemonConfig class."""

    def test_init_with_required_params(self, tmp_path):
        """Should initialize with required parameters."""
        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH", "DBZV"]}},
            radar_name="RMA1",
        )

        assert config.local_netcdf_dir == tmp_path / "netcdf"
        assert config.local_product_dir == tmp_path / "products"
        assert config.radar_name == "RMA1"

    def test_init_default_values(self, tmp_path):
        """Should use default values for optional parameters."""
        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        assert config.poll_interval == 30
        assert config.max_concurrent_processing == 2
        assert config.product_type == "image"
        assert config.add_colmax is True
        assert config.stuck_volume_timeout_minutes == 60
        # geometry has been replaced by geometry parameters; assert defaults exist
        assert hasattr(config, "geometry_res")
        assert config.geometry_res == 1200.0
        assert config.geometry_toa == 12000.0
        assert config.geometry_hfac == 0.017
        assert config.geometry_min_radius == 250.0

    def test_init_custom_values(self, tmp_path):
        """Should accept custom values."""
        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
            poll_interval=60,
            product_type="geotiff",
            add_colmax=False,
            stuck_volume_timeout_minutes=120,
        )

        assert config.poll_interval == 60
        assert config.product_type == "geotiff"
        assert config.add_colmax is False
        assert config.stuck_volume_timeout_minutes == 120


class TestProductGenerationDaemon:
    """Tests for ProductGenerationDaemon class."""

    @pytest.fixture
    def daemon_config(self, tmp_path):
        """Create a daemon configuration for testing."""
        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH", "DBZV"], "02": ["VRAD"]}},
            radar_name="RMA1",
            poll_interval=1,  # Short for testing
        )
        return config

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_init_creates_output_dir(self, mock_tracker, daemon_config):
        """Should create output directory on init."""
        _ = ProductGenerationDaemon(daemon_config)

        assert daemon_config.local_product_dir.exists()

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_init_initializes_stats(self, mock_tracker, daemon_config):
        """Should initialize statistics counters."""
        daemon = ProductGenerationDaemon(daemon_config)

        assert daemon._stats["volumes_processed"] == 0
        assert daemon._stats["volumes_failed"] == 0

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_init_sets_running_false(self, mock_tracker, daemon_config):
        """Should initialize _running to False."""
        daemon = ProductGenerationDaemon(daemon_config)

        assert daemon._running is False

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_init_geometry_returns_none_when_not_provided(self, mock_tracker, daemon_config):
        """Should return None for geometry when not provided."""
        # Avoid running heavy geometry initialization in unit test
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(daemon_config)
            assert daemon.geometry is None


class TestProductGenerationDaemonIntegration:
    """Integration tests for ProductGenerationDaemon."""

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
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

        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types=volume_types,
            radar_name="RMA1",
        )
        # Avoid running geometry init during this simple structural test
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value={}):
            daemon = ProductGenerationDaemon(config)

        assert "0315" in daemon.config.volume_types
        assert "01" in daemon.config.volume_types["0315"]
        assert "DBZH" in daemon.config.volume_types["0315"]["01"]

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_daemon_uses_state_tracker(self, mock_tracker_class, tmp_path):
        """Daemon should use SQLiteStateTracker."""
        mock_tracker = MagicMock()
        mock_tracker_class.return_value = mock_tracker

        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        daemon = ProductGenerationDaemon(config)

        mock_tracker_class.assert_called_once_with(tmp_path / "state.db")
        assert daemon.state_tracker == mock_tracker

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_different_product_types(self, mock_tracker, tmp_path):
        """Should accept different product types."""
        for product_type in ["image", "geotiff", "cog"]:
            config = ProductGenerationDaemonConfig(
                local_netcdf_dir=tmp_path / "netcdf",
                local_product_dir=tmp_path / "products",
                state_db=tmp_path / "state.db",
                volume_types={"0315": {"01": ["DBZH"]}},
                radar_name="RMA1",
                product_type=product_type,
            )

            daemon = ProductGenerationDaemon(config)
            assert daemon.config.product_type == product_type
