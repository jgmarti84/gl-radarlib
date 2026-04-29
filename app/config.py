"""Genpro25 service configuration loader.

Configuration is merged from three sources in ascending priority order:
  1. Built-in defaults (_DEFAULTS dict below)
  2. YAML file (genpro25.yml, environment section defined by GENPRO25_ENV)
  3. OS environment variables

Usage from app/main.py::

    import config
    config.FTP_HOST   # str
    config.ENABLE_CLEANUP_DAEMON  # bool
    ...

Public API::

    get_config(key, default=None) -> Any
    get_all_config() -> Dict[str, Any]
"""

import json
import os
from pathlib import Path
from typing import Any, Dict

import yaml

# ---------------------------------------------------------------------------
# Defaults — all service-layer settings needed by Genpro25 (app/main.py).
# radarlib's internal settings (colormaps, GRC thresholds, etc.) are managed
# by radarlib.config and are NOT duplicated here; radarlib reads its own module.
# ---------------------------------------------------------------------------
_DEFAULTS: Dict[str, Any] = {
    # FTP connection
    "FTP_HOST": None,
    "FTP_USER": None,
    "FTP_PASS": None,
    # Daemon enable/disable toggles
    "ENABLE_DOWNLOAD_DAEMON": True,
    "ENABLE_PROCESSING_DAEMON": True,
    "ENABLE_PRODUCT_DAEMON": True,
    "ENABLE_CLEANUP_DAEMON": True,
    # Poll intervals (seconds)
    "DOWNLOAD_POLL_INTERVAL": 60,
    "PROCESSING_POLL_INTERVAL": 30,
    "PRODUCT_POLL_INTERVAL": 30,
    "CLEANUP_POLL_INTERVAL": 1800,
    # Processing parameters
    "START_DATE": None,
    "PRODUCT_TYPE": "raw_cog",
    "ADD_COLMAX": True,
    "ADD_TOPS_AND_CORES": False,
    "TOPS_AND_CORES_OUTPUT_DIR": None,
    "GEOMETRY_BUFR_LOOKBACK_HOURS": 72,
    # Data retention periods (days)
    "NETCDF_RETENTION_DAYS": 30.0,
    "BUFR_RETENTION_DAYS": 30.0,
    # File system paths (typically overridden by docker-compose.yml env vars)
    "ROOT_LOGS_PATH": "logs",
    "ROOT_RADAR_FILES_PATH": "data/radares",
    "ROOT_RADAR_PRODUCTS_PATH": "product_output",
    "ROOT_CACHE_PATH": "cache",
    "ROOT_GATE_COORDS_PATH": "data/gate_coordinates",
    "ROOT_GEOMETRY_PATH": "data/geometries",
    # ROI parameters per volume type (None = no ROI clipping)
    "ROI_PARAMS_VOL01": None,
    "ROI_PARAMS_VOL02": None,
    # Volume type definitions: {vol_code: {vol_nr: [fields]}}
    # Override this in genpro25.yml under VOLUME_TYPES.
    "VOLUME_TYPES": {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        }
    },
}

# Keys whose values are dicts and must NOT be flattened when encountered in YAML.
_KNOWN_DICT_KEYS: frozenset = frozenset(k for k, v in _DEFAULTS.items() if isinstance(v, dict) or v is None)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _flatten_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a nested YAML section into a flat key→value dict.

    Rules:
    - If a key is listed in ``_DEFAULTS``, its value is kept as-is (even when
      that value is itself a dict, e.g. ``VOLUME_TYPES``).
    - Any other dict-valued key is treated as a *structural grouping node*
      (e.g. ``DAEMON_PARAMS``, ``FTP``, ``COLMAX``) and is recursed into.
    - ``None`` and the string ``"None"`` are excluded from the output.

    Examples::

        {"DAEMON_PARAMS": {"ENABLE_CLEANUP_DAEMON": True}}
            → {"ENABLE_CLEANUP_DAEMON": True}

        {"VOLUME_TYPES": {"0315": {"01": [...]}}}
            → {"VOLUME_TYPES": {"0315": {"01": [...]}}}   # kept intact
    """
    result: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict) and k not in _DEFAULTS:
            # Structural grouping node — recurse, don't keep the parent key.
            result.update(_flatten_dict(v))
        elif v is not None and v != "None":
            result[k] = v
    return result


# Keep old name as alias so existing imports/tests continue to work.
_flatten_config = _flatten_dict


def _load_yaml_section() -> Dict[str, Any]:
    """Load and return the active environment section from genpro25.yml."""
    cfg_path = os.getenv("GENPRO25_CONFIG", "/workspace/app/genpro25.yml")
    p = Path(cfg_path)
    if not p.is_file():
        raise FileNotFoundError(
            f"Genpro25 configuration file not found: {cfg_path}\n"
            "Set the GENPRO25_CONFIG environment variable to the correct path."
        )
    with p.open("r", encoding="utf-8") as fh:
        all_envs: Dict[str, Any] = yaml.safe_load(fh) or {}
    env_name = os.getenv("GENPRO25_ENV", "local").lower()
    section = all_envs.get(env_name, {})
    return section if isinstance(section, dict) else {}


def _apply_env_overrides(config: Dict[str, Any]) -> None:
    """Override *config* values in-place with matching OS environment variables.

    Type coercion uses the type declared in ``_DEFAULTS`` for each key.
    """
    for key in list(config.keys()):
        env_val = os.environ.get(key)
        if env_val is None:
            continue
        default_val = _DEFAULTS.get(key)
        if isinstance(default_val, bool):
            config[key] = env_val.lower() in ("true", "1", "yes")
        elif isinstance(default_val, int) and not isinstance(default_val, bool):
            try:
                config[key] = int(env_val)
            except ValueError:
                pass
        elif isinstance(default_val, float):
            try:
                config[key] = float(env_val)
            except ValueError:
                pass
        elif isinstance(default_val, dict):
            try:
                config[key] = json.loads(env_val)
            except (ValueError, json.JSONDecodeError):
                pass
        else:
            config[key] = env_val


def _build_config() -> Dict[str, Any]:
    """Build and return the final merged configuration dict.

    Precedence (highest last):
      1. ``_DEFAULTS``
      2. Flattened YAML section from genpro25.yml
      3. Environment variable overrides
    """
    merged: Dict[str, Any] = _DEFAULTS.copy()
    yaml_section = _load_yaml_section()
    yaml_flat = _flatten_dict(yaml_section)
    merged.update(yaml_flat)
    _apply_env_overrides(merged)
    return merged


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_config(key: str, default: Any = None) -> Any:
    """Return a single configuration value, or *default* if not found."""
    return _config.get(key, default)


def get_all_config() -> Dict[str, Any]:
    """Return a copy of the complete merged configuration dict."""
    return _config.copy()


# ---------------------------------------------------------------------------
# Build config and expose every key as a module-level attribute so that
# ``import config; config.FTP_HOST`` works as expected by app/main.py.
# ---------------------------------------------------------------------------
_config: Dict[str, Any] = _build_config()

for _key, _value in _config.items():
    globals()[_key] = _value

del _key, _value
