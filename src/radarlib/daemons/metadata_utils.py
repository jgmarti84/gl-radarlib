# -*- coding: utf-8 -*-
"""Utilities for building and applying COG product metadata.

This module provides helpers that sit between the daemon pipeline and the
ProductMetadata dataclass, translating raw radar objects and volume_info dicts
into structured metadata and persisting it to GeoTIFF tags.
"""

from pathlib import Path
from typing import Any, Dict

from radarlib.daemons.product_metadata import ProductMetadata, get_radar_coverage_km, parse_observation_timestamp


def build_product_metadata(
    radar: Any,
    volume_info: Dict[str, Any],
    field_name: str,
    radar_name: str,
    filtered: bool = False,
) -> ProductMetadata:
    """Build a ProductMetadata object from a PyART radar and volume_info dict.

    Args:
        radar: PyART ``Radar`` object that has already been loaded and
            standardised (e.g. via ``read_radar_netcdf`` /
            ``estandarizar_campos_RMA``).
        volume_info: Dict returned by ``SQLiteStateTracker.get_volumes_for_product_generation()``.
            Must contain the keys ``vol_nr``, ``strategy``, and
            ``observation_datetime``.
        field_name: Name of the radar field being processed
            (e.g. ``"DBZH"``, ``"COLMAX"``).
        radar_name: Radar station identifier taken from daemon config
            (e.g. ``"RMA1"``).
        filtered: ``True`` if a ``GateFilter`` was applied to this field;
            ``False`` for the raw/unfiltered variant (``'o'`` suffix in the
            output filename).

    Returns:
        A fully populated :class:`ProductMetadata` instance.

    Raises:
        ValueError: If ``observation_datetime`` is missing from
            ``volume_info`` or has an invalid format.
        KeyError: If ``vol_nr`` or ``strategy`` are absent from
            ``volume_info``.
    """
    # Validate required volume_info keys early for a clear error message.
    for key in ("vol_nr", "strategy"):
        if key not in volume_info:
            raise KeyError(f"Required key '{key}' missing from volume_info")

    obs_ts_str = volume_info.get("observation_datetime")
    if not obs_ts_str:
        raise ValueError("'observation_datetime' missing or empty in volume_info")

    observation_timestamp = parse_observation_timestamp(obs_ts_str)
    coverage_km = get_radar_coverage_km(radar)

    return ProductMetadata(
        volume_number=volume_info["vol_nr"],
        strategy=volume_info["strategy"],
        field_name=field_name,
        radar_name=radar_name,
        radar_coverage_m=coverage_km * 1000.0,
        observation_timestamp=observation_timestamp,
        filtered=filtered,
    )


def apply_metadata_to_cog(
    cog_path: Path,
    metadata: ProductMetadata,
) -> None:
    """Apply ProductMetadata tags to an existing COG GeoTIFF file.

    Opens the file in read/write mode with rasterio and calls
    ``update_tags()`` with the serialised metadata dict produced by
    :meth:`ProductMetadata.to_geotiff_tags`.

    Args:
        cog_path: Absolute path to the existing ``.tif`` COG file.
        metadata: Populated :class:`ProductMetadata` instance whose tags
            should be written into the file.

    Raises:
        IOError: If the file cannot be opened, is not a valid GeoTIFF, or
            rasterio raises any error during the update.
    """
    import rasterio

    tags = metadata.to_geotiff_tags()

    try:
        with rasterio.open(cog_path, "r+") as dst:
            dst.update_tags(**tags)
    except Exception as exc:
        raise IOError(f"Failed to update COG metadata for {cog_path}: {exc}") from exc
