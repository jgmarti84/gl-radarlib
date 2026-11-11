# -*- coding: utf-8 -*-
"""Unit tests for radarlib.daemons.product_daemon module.

Tests the ProductGenerationDaemon and ProductGenerationDaemonConfig classes.
"""


from unittest.mock import MagicMock, patch

import pytest

from radarlib.daemons.product_daemon import ProductGenerationDaemonConfig


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
        assert hasattr(config, "geometry_types")
        assert config.geometry_types == {}

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
        # avoid heavy geometry initialization that can require FTP
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            _ = ProductGenerationDaemon(daemon_config)

        assert daemon_config.local_product_dir.exists()

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_init_initializes_stats(self, mock_tracker, daemon_config):
        """Should initialize statistics counters."""
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(daemon_config)

        assert daemon._stats["volumes_processed"] == 0
        assert daemon._stats["volumes_failed"] == 0

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_init_sets_running_false(self, mock_tracker, daemon_config):
        """Should initialize _running to False."""
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
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

        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(config)

        mock_tracker_class.assert_called_once_with(tmp_path / "state.db")
        assert daemon.state_tracker == mock_tracker

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_different_product_types(self, mock_tracker, tmp_path):
        """Should accept different product types."""
        for product_type in ["image", "geotiff", "cog", "raw_cog"]:
            config = ProductGenerationDaemonConfig(
                local_netcdf_dir=tmp_path / "netcdf",
                local_product_dir=tmp_path / "products",
                state_db=tmp_path / "state.db",
                volume_types={"0315": {"01": ["DBZH"]}},
                radar_name="RMA1",
                product_type=product_type,
            )

            from unittest.mock import patch as _patch

            from radarlib.daemons.product_daemon import ProductGenerationDaemon

            with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
                daemon = ProductGenerationDaemon(config)

            assert daemon.config.product_type == product_type


class TestProductGenerationDaemonRawCog:
    """Tests for raw_cog product type routing in ProductGenerationDaemon."""

    @pytest.fixture
    def raw_cog_config(self, tmp_path):
        """Create a daemon configuration with raw_cog product type."""
        return ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
            poll_interval=1,
            product_type="raw_cog",
        )

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_raw_cog_product_type_stored(self, mock_tracker, raw_cog_config):
        """Daemon should store raw_cog product type."""
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(raw_cog_config)

        assert daemon.config.product_type == "raw_cog"

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_raw_cog_routes_to_correct_method(self, mock_tracker, raw_cog_config, tmp_path):
        """_generate_product_async should call _generate_raw_cog_products_sync for raw_cog."""
        import asyncio
        from unittest.mock import MagicMock
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(raw_cog_config)

        # Create a fake NetCDF file so the path-exists check passes
        netcdf_file = tmp_path / "netcdf" / "fake.nc"
        netcdf_file.parent.mkdir(parents=True, exist_ok=True)
        netcdf_file.write_bytes(b"fake")

        volume_info = {
            "volume_id": "vol_001",
            "netcdf_path": str(netcdf_file),
            "is_complete": 1,
            "strategy": "0315",
            "vol_nr": "01",
        }

        # Patch the state tracker methods and the generation method itself
        daemon.state_tracker.register_product_generation = MagicMock()
        daemon.state_tracker.mark_product_status = MagicMock()

        with _patch.object(daemon, "_ensure_geometry", return_value=MagicMock()):
            daemon.geometry = {"0315-01": object()}  # or MagicMock(), or whatever fits
            with _patch.object(daemon, "_generate_raw_cog_products_sync") as mock_raw_cog:
                with _patch.object(daemon, "_generate_cog_products_sync") as mock_legacy_cog:
                    with _patch.object(daemon, "_generate_products_sync") as mock_image:
                        result = asyncio.run(daemon._generate_product_async(volume_info))

        mock_raw_cog.assert_called_once()
        mock_legacy_cog.assert_not_called()
        mock_image.assert_not_called()
        assert result is True

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_geotiff_still_routes_to_legacy_method(self, mock_tracker, tmp_path):
        """product_type='geotiff' must still use _generate_cog_products_sync (no regression)."""
        import asyncio
        from unittest.mock import MagicMock
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
            product_type="geotiff",
        )

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(config)

        netcdf_file = tmp_path / "netcdf" / "fake.nc"
        netcdf_file.parent.mkdir(parents=True, exist_ok=True)
        netcdf_file.write_bytes(b"fake")

        volume_info = {
            "volume_id": "vol_001",
            "netcdf_path": str(netcdf_file),
            "is_complete": 1,
            "strategy": "0315",
            "vol_nr": "01",
        }

        daemon.state_tracker.register_product_generation = MagicMock()
        daemon.state_tracker.mark_product_status = MagicMock()

        with _patch.object(daemon, "_ensure_geometry", return_value=MagicMock()):
            with _patch.object(daemon, "_generate_cog_products_sync") as mock_legacy_cog:
                with _patch.object(daemon, "_generate_raw_cog_products_sync") as mock_raw_cog:
                    with _patch.object(daemon, "_generate_products_sync") as mock_image:
                        result = asyncio.run(daemon._generate_product_async(volume_info))

        mock_legacy_cog.assert_called_once()
        mock_raw_cog.assert_not_called()
        mock_image.assert_not_called()
        assert result is True

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_image_still_routes_to_legacy_method(self, mock_tracker, tmp_path):
        """product_type='image' must still use _generate_products_sync (no regression)."""
        import asyncio
        from unittest.mock import MagicMock
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
            product_type="image",
        )

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(config)

        netcdf_file = tmp_path / "netcdf" / "fake.nc"
        netcdf_file.parent.mkdir(parents=True, exist_ok=True)
        netcdf_file.write_bytes(b"fake")

        volume_info = {
            "volume_id": "vol_001",
            "netcdf_path": str(netcdf_file),
            "is_complete": 1,
            "strategy": "0315",
            "vol_nr": "01",
        }

        daemon.state_tracker.register_product_generation = MagicMock()
        daemon.state_tracker.mark_product_status = MagicMock()

        with _patch.object(daemon, "_generate_products_sync") as mock_image:
            with _patch.object(daemon, "_generate_cog_products_sync") as mock_legacy_cog:
                with _patch.object(daemon, "_generate_raw_cog_products_sync") as mock_raw_cog:
                    result = asyncio.run(daemon._generate_product_async(volume_info))

        mock_image.assert_called_once()
        mock_legacy_cog.assert_not_called()
        mock_raw_cog.assert_not_called()
        assert result is True

    @patch("radarlib.daemons.product_daemon.SQLiteStateTracker")
    def test_generate_raw_cog_products_sync_method_exists(self, mock_tracker, raw_cog_config):
        """ProductGenerationDaemon must expose _generate_raw_cog_products_sync."""
        from unittest.mock import patch as _patch

        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        with _patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
            daemon = ProductGenerationDaemon(raw_cog_config)

        assert callable(getattr(daemon, "_generate_raw_cog_products_sync", None))


class TestProductGenerationNoOutputVisibility:
    """A volume that writes no product must not be logged as a plain success, and a volume
    type that keeps producing nothing must raise a searchable ``NO OUTPUT`` error."""

    @pytest.fixture
    def daemon(self, tmp_path):
        from radarlib.daemons.product_daemon import ProductGenerationDaemon

        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "netcdf",
            local_product_dir=tmp_path / "products",
            state_db=tmp_path / "state.db",
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA5",
            product_type="raw_cog",
            no_output_alert_threshold=3,
        )
        with patch("radarlib.daemons.product_daemon.SQLiteStateTracker"):
            with patch.object(ProductGenerationDaemon, "_init_geometry", return_value=None):
                d = ProductGenerationDaemon(config)
        d.state_tracker = MagicMock()
        netcdf = tmp_path / "netcdf" / "fake.nc"
        netcdf.parent.mkdir(parents=True, exist_ok=True)
        netcdf.write_bytes(b"fake")
        d._netcdf = netcdf
        return d

    def _volume(self, daemon, vol_nr="01", complete=1, vid="v1"):
        return {
            "volume_id": vid,
            "netcdf_path": str(daemon._netcdf),
            "is_complete": complete,
            "strategy": "0315",
            "vol_nr": vol_nr,
        }

    def _run(self, daemon, volume, **patch_kwargs):
        import asyncio

        with patch.object(daemon, "_ensure_geometry", return_value=MagicMock()):
            with patch.object(daemon, "_generate_raw_cog_products_sync", **patch_kwargs):
                return asyncio.run(daemon._generate_product_async(volume))

    def test_default_threshold(self, tmp_path):
        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=tmp_path / "n",
            local_product_dir=tmp_path / "p",
            state_db=tmp_path / "s.db",
            volume_types={},
            radar_name="RMA1",
        )
        assert config.no_output_alert_threshold == 5

    def test_zero_products_is_warned_not_reported_as_success(self, daemon, caplog):
        with caplog.at_level("INFO"):
            result = self._run(daemon, self._volume(daemon), return_value=0)
        assert result is True  # status handling is unchanged: the volume is still marked completed
        assert "Generated 0 raw_cog products" in caplog.text
        assert "Successfully generated" not in caplog.text

    def test_success_reports_the_count(self, daemon, caplog):
        with caplog.at_level("INFO"):
            self._run(daemon, self._volume(daemon), return_value=4)
        assert "Successfully generated raw_cog for complete volume v1 (4 product(s))" in caplog.text
        assert "Generated 0" not in caplog.text

    def test_unknown_count_keeps_the_old_message(self, daemon, caplog):
        """Callers that do not return a count (mocks, other product types) are treated as success."""
        with caplog.at_level("INFO"):
            self._run(daemon, self._volume(daemon))  # MagicMock return value
        assert "Successfully generated raw_cog for complete volume v1" in caplog.text
        assert "product(s))" not in caplog.text

    def test_alert_after_threshold_and_repeats(self, daemon, caplog):
        with caplog.at_level("INFO"):
            for i in range(1, 7):
                self._run(daemon, self._volume(daemon, vid=f"v{i}"), return_value=0)
                alerts = caplog.text.count("NO OUTPUT")
                assert alerts == (i // 3), f"after volume {i}"
        assert "[RMA5 0315-01] NO OUTPUT: 3 consecutive complete volume(s)" in caplog.text
        assert "NO OUTPUT: 6 consecutive" in caplog.text

    def test_a_good_volume_resets_the_streak(self, daemon, caplog):
        with caplog.at_level("INFO"):
            for i in range(3):
                self._run(daemon, self._volume(daemon, vid=f"bad{i}"), return_value=0)
            self._run(daemon, self._volume(daemon, vid="good"), return_value=2)
            assert "producing again after 3 consecutive volume(s) without output" in caplog.text
            caplog.clear()
            for i in range(2):
                self._run(daemon, self._volume(daemon, vid=f"bad2{i}"), return_value=0)
        assert "NO OUTPUT" not in caplog.text

    def test_incomplete_volumes_do_not_count(self, daemon, caplog):
        with caplog.at_level("INFO"):
            for i in range(6):
                self._run(daemon, self._volume(daemon, complete=0, vid=f"inc{i}"), return_value=0)
        assert "NO OUTPUT" not in caplog.text

    def test_volume_types_are_tracked_separately(self, daemon, caplog):
        with caplog.at_level("INFO"):
            for i in range(2):
                self._run(daemon, self._volume(daemon, vol_nr="01", vid=f"a{i}"), return_value=0)
            for i in range(2):
                self._run(daemon, self._volume(daemon, vol_nr="02", vid=f"b{i}"), return_value=0)
        assert "NO OUTPUT" not in caplog.text

    def test_exceptions_count_towards_the_streak(self, daemon, caplog):
        with caplog.at_level("INFO"):
            for i in range(3):
                result = self._run(daemon, self._volume(daemon, vid=f"e{i}"), side_effect=RuntimeError("boom"))
                assert result is False
        assert "NO OUTPUT: 3 consecutive" in caplog.text

    def test_threshold_zero_disables_the_alert(self, daemon, caplog):
        daemon.config.no_output_alert_threshold = 0
        with caplog.at_level("INFO"):
            for i in range(10):
                self._run(daemon, self._volume(daemon, vid=f"z{i}"), return_value=0)
        assert "NO OUTPUT" not in caplog.text
