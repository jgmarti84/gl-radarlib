"""Test configuration parsing from genpro25.yml"""

import sys
from pathlib import Path

import pytest

# Add app to path
APP_PATH = Path(__file__).parent.parent.parent / "app"
sys.path.insert(0, str(APP_PATH))


class TestRadiarlibConfigDefaults:
    """Test that radarlib.config has cleanup daemon defaults."""

    def test_cleanup_daemon_defaults_exist(self):
        """Verify radarlib.config defines cleanup daemon configuration."""
        from radarlib import config as radarlib_config

        # These MUST exist as module-level attributes
        required_attrs = [
            "ENABLE_CLEANUP_DAEMON",
            "CLEANUP_POLL_INTERVAL",
            "BUFR_RETENTION_DAYS",
            "NETCDF_RETENTION_DAYS",
        ]

        missing = []
        for attr in required_attrs:
            if not hasattr(radarlib_config, attr):
                missing.append(attr)
            else:
                value = getattr(radarlib_config, attr)
                print(f"✓ radarlib.config.{attr} = {value}")

        assert not missing, (
            f"Missing in radarlib.config: {missing}\n"
            f"These must be defined as module-level attributes for app/config.py to work"
        )


# class TestConfigFlattening:
#     """Test that app/config.py correctly flattens YAML structure."""

#     @pytest.fixture
#     def minimal_yaml_file(self, tmp_path):
#         """Create a minimal genpro25.yml so app/config.py can be imported."""
#         yml_file = tmp_path / "genpro25.yml"
#         yml_file.write_text("local:\n  DAEMON_PARAMS:\n    ENABLE_CLEANUP_DAEMON: true\n")
#         return yml_file

#     def test_flatten_config_with_daemon_params(self, minimal_yaml_file, monkeypatch):
#         """Verify _flatten_config removes DAEMON_PARAMS nesting."""
#         monkeypatch.setenv("GENPRO25_CONFIG", str(minimal_yaml_file))
#         sys.path.insert(0, str(APP_PATH))
#         if "config" in sys.modules:
#             del sys.modules["config"]
#         from config import _flatten_config

#         test_config = {
#             "DAEMON_PARAMS": {
#                 "ENABLE_CLEANUP_DAEMON": True,
#                 "CLEANUP_POLL_INTERVAL": 900,
#                 "BUFR_RETENTION_DAYS": 3,
#                 "NETCDF_RETENTION_DAYS": 5,
#             },
#             "COLMAX": {
#                 "COLMAX_THRESHOLD": -3,
#             },
#         }

#         flattened = _flatten_config(test_config)

#         # After flattening, DAEMON_PARAMS key should be GONE
#         assert "DAEMON_PARAMS" not in flattened, "DAEMON_PARAMS should be flattened away, but key still exists"

#         # Flattened values should have the DAEMON_PARAMS values
#         assert (
#             flattened.get("ENABLE_CLEANUP_DAEMON") is True
#         ), f"Expected True, got {flattened.get('ENABLE_CLEANUP_DAEMON')}"
#         assert (
#             flattened.get("CLEANUP_POLL_INTERVAL") == 900
#         ), f"Expected 900, got {flattened.get('CLEANUP_POLL_INTERVAL')}"

#         print("✓ Flattening works correctly")
#         print(f"  Keys in flattened result: {list(flattened.keys())[:5]}...")


class TestAppConfigLoading:
    """Test that app/config.py loads from genpro25.yml correctly."""

    @pytest.fixture
    def mock_yaml_file(self, tmp_path):
        """Create a test genpro25.yml file."""
        yml_file = tmp_path / "genpro25.yml"
        yml_content = """
local:
  DAEMON_PARAMS:
    ENABLE_CLEANUP_DAEMON: true
    CLEANUP_POLL_INTERVAL: 900
    BUFR_RETENTION_DAYS: 3
    NETCDF_RETENTION_DAYS: 5
  FTP_HOST: "ftp.example.com"
  FTP_USER: "testuser"
  FTP_PASS: "testpass"
"""
        yml_file.write_text(yml_content)
        return yml_file

    def test_app_config_loads_daemon_params(self, mock_yaml_file, monkeypatch):
        """Verify app/config.py loads DAEMON_PARAMS from YAML."""
        # Set env var to point to test YAML
        monkeypatch.setenv("GENPRO25_CONFIG", str(mock_yaml_file))

        # Remove app.config from sys.modules to force reload
        if "config" in sys.modules:
            del sys.modules["config"]

        # Add app to path and import
        sys.path.insert(0, str(APP_PATH))
        import config

        # Check if cleanup daemon settings were loaded
        try:
            assert hasattr(config, "ENABLE_CLEANUP_DAEMON"), "config.ENABLE_CLEANUP_DAEMON not found"
            assert (
                config.ENABLE_CLEANUP_DAEMON is True
            ), f"Expected ENABLE_CLEANUP_DAEMON=True, got {config.ENABLE_CLEANUP_DAEMON}"
            assert (
                config.CLEANUP_POLL_INTERVAL == 900
            ), f"Expected CLEANUP_POLL_INTERVAL=900, got {config.CLEANUP_POLL_INTERVAL}"

            print("✓ App config loaded DAEMON_PARAMS correctly")
            print(f"  ENABLE_CLEANUP_DAEMON: {config.ENABLE_CLEANUP_DAEMON}")
            print(f"  CLEANUP_POLL_INTERVAL: {config.CLEANUP_POLL_INTERVAL}")

        except AssertionError as e:
            print(f"✗ Config loading failed: {e}")
            print("  Available config attributes starting with 'CLEANUP' or 'ENABLE_CLEANUP':")
            for attr in dir(config):
                if "cleanup" in attr.lower() or "enable" in attr.lower():
                    print(f"    {attr} = {getattr(config, attr, 'N/A')}")
            raise


# class TestConfigMergingBug:
#     """Test for the potential KeyError bug at line 85."""

#     @pytest.fixture
#     def mock_yaml_file(self, tmp_path):
#         """Create a test genpro25.yml file."""
#         yml_file = tmp_path / "genpro25.yml"
#         yml_content = """
# local:
#   DAEMON_PARAMS:
#     ENABLE_CLEANUP_DAEMON: true
#     CLEANUP_POLL_INTERVAL: 900
# """
#         yml_file.write_text(yml_content)
#         return yml_file

#     def test_config_merge_does_not_crash(self, mock_yaml_file, monkeypatch):
#         """Verify _load_config_from_yaml doesn't crash on line 85."""
#         monkeypatch.setenv("GENPRO25_CONFIG", str(mock_yaml_file))

#         if "config" in sys.modules:
#             del sys.modules["config"]

#         sys.path.insert(0, str(APP_PATH))

#         # This should not crash with KeyError
#         try:
#             import config

#             print("✓ Config module imported without KeyError")

#         except KeyError as e:
#             pytest.fail(
#                 f"KeyError during config loading - line 85 bug confirmed:\n"
#                 f"  {e}\n"
#                 f"  flat_raw_config['DAEMON_PARAMS'] doesn't exist after flattening"
#             )
