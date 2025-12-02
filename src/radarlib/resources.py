"""Resource resolution utility for radarlib.

Handles locating package data (C libraries, tables, colormaps) whether
radarlib is installed as a wheel, editable install, or run from source.
"""

import os
import shutil
from importlib import resources
from pathlib import Path


def _default_cache_dir() -> Path:
    """Get the default cache directory for extracted resources."""
    xdg = os.getenv("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "radarlib" / "bufr_resources"


def resolve_bufr_resources_path(override: str | None = None) -> Path:
    """
    Resolve the path to bufr_resources directory.

    Priority:
      1) Explicit override parameter or RADARLIB_BUFR_RESOURCES env var
      2) Resources bundled in the package (importlib.resources) → extract to cache
      3) Relative path for editable/development installs

    Args:
        override: Optional explicit path to bufr_resources directory

    Returns:
        Path to bufr_resources directory

    Raises:
        RuntimeError: If bufr_resources cannot be found in any location
    """

    # 1) Explicit override (parameter or env var)
    override_path = override or os.getenv("RADARLIB_BUFR_RESOURCES")
    if override_path:
        p = Path(override_path)
        if p.exists() and p.is_dir():
            return p
        raise RuntimeError(f"Configured BUFR_RESOURCES_PATH does not exist or is not a directory: {p}")

    # 2) Try to extract from package resources (works for wheels)
    try:
        # Get package resource reference
        pkg_resources = resources.files("radarlib").joinpath("io").joinpath("bufr").joinpath("bufr_resources")

        # Use as_file context manager to extract if necessary
        from contextlib import contextmanager

        @contextmanager
        def resource_path():
            with resources.as_file(pkg_resources) as extracted:
                yield extracted

        with resource_path() as extracted:
            cache_dir = _default_cache_dir()
            cache_dir.mkdir(parents=True, exist_ok=True)

            # Copy resources to cache for persistent access
            if extracted.is_dir():
                for src_file in extracted.rglob("*"):
                    if src_file.is_file():
                        rel_path = src_file.relative_to(extracted)
                        dest_file = cache_dir / rel_path
                        dest_file.parent.mkdir(parents=True, exist_ok=True)

                        # Only copy if not already present (avoid repeated I/O)
                        if not dest_file.exists():
                            shutil.copy2(src_file, dest_file)

            return cache_dir
    except (ModuleNotFoundError, TypeError, AttributeError):
        pass  # Fall through to editable install path

    # 3) Fallback to editable/development install path
    editable_path = Path(__file__).resolve().parent / "io" / "bufr" / "bufr_resources"
    if editable_path.exists() and editable_path.is_dir():
        return editable_path

    # Nothing found
    raise RuntimeError(
        "bufr_resources directory not found. This can happen if:\n"
        "  1) Package is not properly installed (missing bufr_resources in wheel)\n"
        "  2) Running from source but bufr_resources directory is missing\n"
        "  3) Cache directory cannot be created\n"
        "\n"
        "Solutions:\n"
        "  - Reinstall: pip install --force-reinstall radarlib\n"
        "  - Set explicit path: export RADARLIB_BUFR_RESOURCES=/path/to/bufr_resources\n"
        "  - Check that bufr_resources/ exists in the radarlib source"
    )


def resolve_resource_path(resource_name: str, override: str | None = None) -> Path:
    """
    Generic resource path resolver for other package resources.

    Args:
        resource_name: Name of the resource subdirectory (e.g., 'colormaps', 'bufr_tables')
        override: Optional explicit path

    Returns:
        Path to the resource directory
    """
    override_path = override or os.getenv(f"RADARLIB_{resource_name.upper()}")
    if override_path:
        p = Path(override_path)
        if p.exists():
            return p

    # Try package resources
    try:
        pkg_resource = resources.files("radarlib").joinpath("resources").joinpath(resource_name)
        with resources.as_file(pkg_resource) as extracted:
            return extracted
    except Exception:
        pass

    # Fallback to editable path
    editable_path = Path(__file__).resolve().parent / "resources" / resource_name
    if editable_path.exists():
        return editable_path

    raise RuntimeError(f"Resource '{resource_name}' not found")
