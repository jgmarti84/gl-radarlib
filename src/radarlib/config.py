"""Simple configuration loader for radarlib.

Load order:
1. File pointed to by RADARLIB_CONFIG env var (if set).
2. <package>/radarlib.json (if present).
3. Built-in defaults.

Config file format: JSON dictionary, e.g. {"MAX_SAMPLES": 100, "DEFAULT_TIMEOUT": 5.0}
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

# Import resource resolver for package data
from radarlib.resources import resolve_bufr_resources_path

# Workspace/project paths (for development/testing only)
# When running as installed package, these fallback to reasonable defaults
_this_file = Path(__file__).resolve()
root_package = _this_file.parent
root_project = Path.home().parent / "workspaces" / "radarlib"
root_data = root_project / "data"
root_products = root_project / "product_output"

# Resolve BUFR resources path using importlib for wheel-safe access
try:
    _bufr_resources_path = str(resolve_bufr_resources_path())
except Exception as e:
    # Fallback for development: use direct path
    _bufr_resources_path = str(root_package / "io" / "bufr" / "bufr_resources")
    import warnings

    warnings.warn(f"Could not resolve BUFR resources via importlib, using fallback: {e}", stacklevel=2)

default_volume_types: Dict[str, Dict[str, list]] = {
    "0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}
}

DEFAULTS: Dict[str, Any] = {
    "BUFR_RESOURCES_PATH": _bufr_resources_path,
    "ROOT_CACHE_PATH": os.path.join(root_project, "cache"),
    "ROOT_RADAR_FILES_PATH": os.path.join(root_data, "radares"),
    "ROOT_LOGS_PATH": os.path.join(root_project, "logs"),
    "ROOT_RADAR_PRODUCTS_PATH": root_products,
    "ROOT_GATE_COORDS_PATH": os.path.join(root_data, "gate_coordinates"),
    "ROOT_GEOMETRY_PATH": os.path.join(root_data, "geometries"),
    "COLMAX_THRESHOLD": -3,
    "COLMAX_ELEV_LIMIT1": 0.65,
    "COLMAX_RHOHV_FILTER": True,
    "COLMAX_RHOHV_UMBRAL": 0.8,
    "COLMAX_WRAD_FILTER": True,
    "COLMAX_WRAD_UMBRAL": 4.6,
    "COLMAX_TDR_FILTER": True,
    "COLMAX_TDR_UMBRAL": 8.5,
    "FTP_HOST": "www.example.com",
    "FTP_USER": "example_user",
    "FTP_PASS": "secret",
    "VMIN_REFL_NOFILTERS": -20,
    "VMAX_REFL_NOFILTERS": 70,
    "CMAP_REFL_NOFILTERS": "grc_th",
    "VMIN_RHOHV_NOFILTERS": 0,
    "VMAX_RHOHV_NOFILTERS": 1,
    "CMAP_RHOHV_NOFILTERS": "grc_rho",
    "VMIN_PHIDP_NOFILTERS": -5,
    "VMAX_PHIDP_NOFILTERS": 360,
    "CMAP_PHIDP_NOFILTERS": "grc_th",
    "VMIN_KDP_NOFILTERS": -4,
    "VMAX_KDP_NOFILTERS": 8,
    "CMAP_KDP_NOFILTERS": "jet",
    "VMIN_ZDR_NOFILTERS": -7.5,
    "VMAX_ZDR_NOFILTERS": 7.5,
    "CMAP_ZDR_NOFILTERS": "grc_zdr",
    "VMIN_VRAD_NOFILTERS": -30,
    "VMAX_VRAD_NOFILTERS": 30,
    "CMAP_VRAD_NOFILTERS": "grc_vrad",
    "VMIN_WRAD_NOFILTERS": -2,
    "VMAX_WRAD_NOFILTERS": 6,
    "CMAP_WRAD_NOFILTERS": "grc_th",
    "VMIN_REFL": -20,
    "VMAX_REFL": 70,
    "CMAP_REFL": "grc_th",
    "VMIN_RHOHV": 0,
    "VMAX_RHOHV": 1,
    "CMAP_RHOHV": "grc_rho",
    "VMIN_PHIDP": -5,
    "VMAX_PHIDP": 360,
    "CMAP_PHIDP": "grc_th",
    "VMIN_KDP": -4,
    "VMAX_KDP": 8,
    "CMAP_KDP": "jet",
    "VMIN_ZDR": -2,
    "VMAX_ZDR": 7.5,
    "CMAP_ZDR": "grc_zdr",
    "VMIN_VRAD": -15,
    "VMAX_VRAD": 15,
    "CMAP_VRAD": "grc_vrad",
    "VMIN_WRAD": -2,
    "VMAX_WRAD": 6,
    "CMAP_WRAD": "grc_th",
    "FIELDS_TO_PLOT": [
        "DBZH",
        "ZDR",
        "RHOHV",
        "VRAD",
        "WRAD",
        "KDP",
        "COLMAX",
    ],  # COLMAX is treated differently in the COG context, left here for backwards compatibility with PNG products
    "FILTERED_FIELDS_TO_PLOT": [
        "DBZH",
        "ZDR",
        "COLMAX",
        "KDP",
    ],  # COLMAX is treated differently in the COG context, left here for backwards compatibility with PNG products
    "PNG_DPI": 72,
    "GRC_RHV_FILTER": True,
    "GRC_RHV_THRESHOLD": 0.55,
    "GRC_WRAD_FILTER": True,
    "GRC_WRAD_THRESHOLD": 4.6,
    "GRC_REFL_FILTER": True,
    "GRC_REFL_THRESHOLD": -3,
    "GRC_ZDR_FILTER": True,
    "GRC_ZDR_THRESHOLD": 8.5,
    "GRC_REFL_FILTER2": True,
    "GRC_REFL_THRESHOLD2": 25,
    "GRC_CM_FILTER": True,
    "GRC_RHOHV_THRESHOLD2": 0.85,
    "GRC_DESPECKLE_FILTER": True,
    "GRC_MEAN_FILTER": True,
    "GRC_MEAN_THRESHOLD": 0.85,
    "VOLUME_TYPES": default_volume_types,
    "GEOMETRY_RES_XY": 1000.0,
    "GEOMETRY_RES_Z": 600,
    "GEOMETRY_TOA": 15000.0,
    "GEOMETRY_HFAC": 1.02,
    "GEOMETRY_NB": 1.45,
    "GEOMETRY_BSP": 1.2,
    "GEOMETRY_MIN_RADIUS": 900.0,
    "MAX_NEIGHBORS": 1,
    "WEIGHT_FUNCTION": "nearest",
    "GEOMETRY_BUFR_LOOKBACK_HOURS": 72,
}

_config: Dict[str, Any] = DEFAULTS.copy()


def _try_load_file(path: str) -> bool:
    try:
        with open(path, "r", encoding="utf8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            _config.update(data)
            return True
    except FileNotFoundError:
        return False
    except Exception:
        # ignore parse errors / other I/O errors to keep loader robust
        return False
    return False


def _auto_load() -> None:
    # 1) explicit env var
    env_path = os.environ.get("RADARLIB_CONFIG")
    if env_path and _try_load_file(env_path):
        return

    # Override with environment variables if they exist
    for key in _config.keys():
        env_val = os.environ.get(key)
        if env_val is not None:
            # Try to preserve type from defaults
            default_val = DEFAULTS.get(key)
            if isinstance(default_val, bool):
                _config[key] = env_val.lower() in ("true", "1", "yes")
            elif isinstance(default_val, (int, float)):
                try:
                    _config[key] = type(default_val)(env_val)
                except ValueError:
                    pass
            elif isinstance(default_val, dict):
                try:
                    _config[key] = json.loads(env_val)
                except ValueError:
                    pass
            else:
                _config[key] = env_val


# 2) package config file
_auto_load()


def get(key: str, default: Any = None) -> Any:
    return _config.get(key, default)


# convenience attributes
BUFR_RESOURCES_PATH: str = get("BUFR_RESOURCES_PATH")
ROOT_CACHE_PATH: str = get("ROOT_CACHE_PATH")
ROOT_RADAR_FILES_PATH: str = get("ROOT_RADAR_FILES_PATH")
ROOT_RADAR_PRODUCTS_PATH: str = get("ROOT_RADAR_PRODUCTS_PATH")
ROOT_LOGS_PATH: str = get("ROOT_LOGS_PATH")
ROOT_GATE_COORDS_PATH: str = get("ROOT_GATE_COORDS_PATH")
ROOT_GEOMETRY_PATH: str = get("ROOT_GEOMETRY_PATH")
COLMAX_THRESHOLD: float = get("COLMAX_THRESHOLD")
COLMAX_ELEV_LIMIT1: float = get("COLMAX_ELEV_LIMIT1")
COLMAX_RHOHV_FILTER: bool = get("COLMAX_RHOHV_FILTER")
COLMAX_RHOHV_UMBRAL: float = get("COLMAX_RHOHV_UMBRAL")
COLMAX_WRAD_FILTER: bool = get("COLMAX_WRAD_FILTER")
COLMAX_WRAD_UMBRAL: float = get("COLMAX_WRAD_UMBRAL")
COLMAX_TDR_FILTER: bool = get("COLMAX_TDR_FILTER")
COLMAX_TDR_UMBRAL: float = get("COLMAX_TDR_UMBRAL")
FTP_HOST: str = get("FTP_HOST")
FTP_USER: str = get("FTP_USER")
FTP_PASS: str = get("FTP_PASS")
VMIN_REFL_NOFILTERS: int = get("VMIN_REFL_NOFILTERS")
VMAX_REFL_NOFILTERS: int = get("VMAX_REFL_NOFILTERS")
CMAP_REFL_NOFILTERS: str = get("CMAP_REFL_NOFILTERS")
VMIN_RHOHV_NOFILTERS: int = get("VMIN_RHOHV_NOFILTERS")
VMAX_RHOHV_NOFILTERS: int = get("VMAX_RHOHV_NOFILTERS")
CMAP_RHOHV_NOFILTERS: str = get("CMAP_RHOHV_NOFILTERS")
VMIN_PHIDP_NOFILTERS: int = get("VMIN_PHIDP_NOFILTERS")
VMAX_PHIDP_NOFILTERS: int = get("VMAX_PHIDP_NOFILTERS")
CMAP_PHIDP_NOFILTERS: str = get("CMAP_PHIDP_NOFILTERS")
VMIN_KDP_NOFILTERS: int = get("VMIN_KDP_NOFILTERS")
VMAX_KDP_NOFILTERS: int = get("VMAX_KDP_NOFILTERS")
CMAP_KDP_NOFILTERS: str = get("CMAP_KDP_NOFILTERS")
VMIN_ZDR_NOFILTERS: float = get("VMIN_ZDR_NOFILTERS")
VMAX_ZDR_NOFILTERS: float = get("VMAX_ZDR_NOFILTERS")
CMAP_ZDR_NOFILTERS: str = get("CMAP_ZDR_NOFILTERS")
VMIN_VRAD_NOFILTERS: int = get("VMIN_VRAD_NOFILTERS")
VMAX_VRAD_NOFILTERS: int = get("VMAX_VRAD_NOFILTERS")
CMAP_VRAD_NOFILTERS: str = get("CMAP_VRAD_NOFILTERS")
VMIN_WRAD_NOFILTERS: int = get("VMIN_WRAD_NOFILTERS")
VMAX_WRAD_NOFILTERS: int = get("VMAX_WRAD_NOFILTERS")
CMAP_WRAD_NOFILTERS: str = get("CMAP_WRAD_NOFILTERS")
VMIN_REFL: int = get("VMIN_REFL")
VMAX_REFL: int = get("VMAX_REFL")
CMAP_REFL: str = get("CMAP_REFL")
VMIN_RHOHV: int = get("VMIN_RHOHV")
VMAX_RHOHV: int = get("VMAX_RHOHV")
CMAP_RHOHV: str = get("CMAP_RHOHV")
VMIN_PHIDP: int = get("VMIN_PHIDP")
VMAX_PHIDP: int = get("VMAX_PHIDP")
CMAP_PHIDP: str = get("CMAP_PHIDP")
VMIN_KDP: int = get("VMIN_KDP")
VMAX_KDP: int = get("VMAX_KDP")
CMAP_KDP: str = get("CMAP_KDP")
VMIN_ZDR: int = get("VMIN_ZDR")
VMAX_ZDR: float = get("VMAX_ZDR")
CMAP_ZDR: str = get("CMAP_ZDR")
VMIN_VRAD: int = get("VMIN_VRAD")
VMAX_VRAD: int = get("VMAX_VRAD")
CMAP_VRAD: str = get("CMAP_VRAD")
VMIN_WRAD: int = get("VMIN_WRAD")
VMAX_WRAD: int = get("VMAX_WRAD")
CMAP_WRAD: str = get("CMAP_WRAD")
FIELDS_TO_PLOT: list = get("FIELDS_TO_PLOT")
FILTERED_FIELDS_TO_PLOT: list = get("FILTERED_FIELDS_TO_PLOT")
PNG_DPI: int = get("PNG_DPI")
GRC_RHV_FILTER: bool = get("GRC_RHV_FILTER")
GRC_RHV_THRESHOLD: float = get("GRC_RHV_THRESHOLD")
GRC_WRAD_FILTER: bool = get("GRC_WRAD_FILTER")
GRC_WRAD_THRESHOLD: float = get("GRC_WRAD_THRESHOLD")
GRC_REFL_FILTER: bool = get("GRC_REFL_FILTER")
GRC_REFL_THRESHOLD: float = get("GRC_REFL_THRESHOLD")
GRC_ZDR_FILTER: bool = get("GRC_ZDR_FILTER")
GRC_ZDR_THRESHOLD: float = get("GRC_ZDR_THRESHOLD")
GRC_REFL_FILTER2: bool = get("GRC_REFL_FILTER2")
GRC_REFL_THRESHOLD2: float = get("GRC_REFL_THRESHOLD2")
GRC_CM_FILTER: bool = get("GRC_CM_FILTER")
GRC_RHOHV_THRESHOLD2: float = get("GRC_RHOHV_THRESHOLD2")
GRC_DESPECKLE_FILTER: bool = get("GRC_DESPECKLE_FILTER")
GRC_MEAN_FILTER: bool = get("GRC_MEAN_FILTER")
GRC_MEAN_THRESHOLD: float = get("GRC_MEAN_THRESHOLD")
VOLUME_TYPES: Dict[str, Dict[str, list]] = get("VOLUME_TYPES")
GEOMETRY_RES_XY: float = get("GEOMETRY_RES_XY")
GEOMETRY_RES_Z: float = get("GEOMETRY_RES_Z")
GEOMETRY_TOA: float = get("GEOMETRY_TOA")
GEOMETRY_HFAC: float = get("GEOMETRY_HFAC")
GEOMETRY_NB: float = get("GEOMETRY_NB")
GEOMETRY_BSP: float = get("GEOMETRY_BSP")
GEOMETRY_MIN_RADIUS: float = get("GEOMETRY_MIN_RADIUS")
MAX_NEIGHBORS: int = get("MAX_NEIGHBORS")
WEIGHT_FUNCTION: str = get("WEIGHT_FUNCTION")
GEOMETRY_BUFR_LOOKBACK_HOURS: int = get("GEOMETRY_BUFR_LOOKBACK_HOURS")

# Pre-compiled regex pattern for BUFR filename parsing (efficiency optimization)
# Format: RADAR_VOLCODE_VOLNR_FIELD_TIMESTAMP.BUFR
_BUFR_FILENAME_PATTERN = re.compile(r"^([A-Z0-9]+)_(\d{4})_(\d{2})_([A-Z]{2,10})_(\d{8}T\d{6}Z)\.BUFR$", re.IGNORECASE)
_NETCDF_FILENAME_PATTERN = re.compile(r"^([A-Z0-9]+)_(\d{4})_(\d{2})_(\d{8}T\d{6}Z)\.nc$", re.IGNORECASE)


def reload(path: Optional[str] = None) -> None:
    """Force reload configuration. If path is provided, try it first."""
    global _config
    _config = DEFAULTS.copy()
    if path:
        _try_load_file(path)
    _auto_load()
