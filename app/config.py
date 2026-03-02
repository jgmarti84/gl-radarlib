import os
from pathlib import Path

import yaml

from radarlib import config as radarlib_config

cfg_path = os.getenv("GENPRO25_CONFIG", "/workspace/app/genpro25.yml")
p = Path(cfg_path)

if not p.is_file():
    raise FileNotFoundError(f"Configuration file not found: {cfg_path}")

try:
    with p.open("r", encoding="utf-8") as fh:
        _ALL_ENVS_CONFIG = yaml.safe_load(fh)

except Exception:
    _ALL_ENVS_CONFIG = {}

# Determine which environment to load (prd, stg, local, etc.)
# Default to 'local' if not specified
_GENPRO25_ENV = os.getenv("GENPRO25_ENV", "local").lower()

# Extract the environment-specific config
RAW_CONFIG = _ALL_ENVS_CONFIG.get(_GENPRO25_ENV, {})


def _flatten_config(cfg):
    """
    Flatten nested dict structure.
    - Leaf values (non-dict) are stored with their original key name only
    - Nested dicts are also included as key-value pairs in the result
    - None values and string 'None' are excluded from flattening

    E.g., {'FTP': {'FTP_HOST': 'example.com'}, 'COLMAX': {'THRESHOLD': 'None'}} ->
           {'FTP': {'FTP_HOST': 'example.com'}, 'FTP_HOST': 'example.com',
            'COLMAX': {...}}
    """
    items = {}
    if not isinstance(cfg, dict):
        return {}

    for k, v in cfg.items():
        if isinstance(v, dict):
            # Include the nested dict itself
            items[k] = v
            # Recursively flatten and add leaf values (excluding None and 'None')
            flattened_nested = _flatten_config(v)
            for nested_k, nested_v in flattened_nested.items():
                if nested_v is not None and nested_v != "None":
                    items[nested_k] = nested_v
        elif v is not None and v != "None":
            # Add leaf value with its key only if not None or 'None'
            items[k] = v

    return items


def _load_config_from_yaml():
    """
    Load and merge configuration from radarlib defaults and genpro25.yml overrides.

    Strategy:
    1. Get all public attributes from radarlib.config that are not None/callable
    2. Flatten RAW_CONFIG to handle nested structures
    3. For each radarlib attribute, check if it exists in RAW_CONFIG
    4. If found and not None in RAW_CONFIG, use that value; otherwise use radarlib default
    5. Set as module-level attribute
    """

    # Flatten the raw config for easier lookup
    flat_raw_config = _flatten_config(RAW_CONFIG)

    # Get all public, non-callable attributes from radarlib.config
    radarlib_attrs = {k: v for k, v in vars(radarlib_config).items() if not k.startswith("_") and not callable(v)}

    # Merge: radarlib defaults + yaml overrides
    merged_config = {}

    for attr_name, default_value in radarlib_attrs.items():
        # Skip special attributes that aren't configuration values
        if attr_name in (
            "DEFAULTS",
            "annotations",
            "json",
            "os",
            "re",
            "Path",
            "Any",
            "Dict",
            "Optional",
            "resolve_bufr_resources_path",
            "root_package",
            "root_project",
            "root_data",
            "root_products",
            "get",
            "reload",
            "default_volume_types",
        ):
            continue

        # Look up in flattened raw config (handles nested structures)
        yaml_value = flat_raw_config.get(attr_name)

        # Use yaml value if it exists and is not None, otherwise use default
        merged_config[attr_name] = yaml_value if yaml_value is not None else default_value

    # add daemon params
    merged_config = dict(merged_config, **flat_raw_config["DAEMON_PARAMS"])
    return merged_config


# Load merged configuration and set as module attributes
_merged = _load_config_from_yaml()
for key, value in _merged.items():
    globals()[key] = value
