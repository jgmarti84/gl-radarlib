# -*- coding: utf-8 -*-
"""ProductMetadata dataclass and helpers for embedding metadata in COG GeoTIFF tags."""

from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import datetime, timezone
from typing import Any, Dict


@dataclass
class ProductMetadata:
    """Metadata to embed in COG GeoTIFF tags.

    Attributes:
        volume_number: Volume number within the strategy (from volume_info["vol_nr"], converted to int).
        strategy: Volume scan strategy code (e.g. "0315").
        field_name: Radar field name (e.g. "DBZH", "COLMAX").
        radar_name: Radar station identifier (e.g. "RMA1").
        radar_coverage_m: Maximum radar range in metres (from radar.range['data'][-1]).
        observation_timestamp: Exact observation time with UTC timezone.
        processing_timestamp: Time at which this product was generated (defaults to utcnow).
        processing_version: Version string for the processing pipeline.
        filtered: True if this is a filtered field; False if raw/unfiltered (suffix 'o').
        additional_metadata: Arbitrary extra key/value pairs included in to_dict() output.
    """

    # Required fields
    volume_number: str
    strategy: str
    field_name: str
    radar_name: str
    radar_coverage_m: float
    observation_timestamp: datetime

    # Optional fields with defaults
    processing_timestamp: datetime = dc_field(default_factory=datetime.utcnow)
    processing_version: str = "1.0"
    filtered: bool = False
    additional_metadata: Dict[str, Any] = dc_field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary suitable for JSON serialisation.

        Returns:
            Dict with all metadata fields. Datetime values are ISO 8601 strings.
            Keys from additional_metadata are merged at the top level.
        """
        return {
            "volume_number": self.volume_number,
            "strategy": self.strategy,
            "field_name": self.field_name,
            "radar_name": self.radar_name,
            "radar_coverage_m": self.radar_coverage_m,
            "observation_timestamp": self.observation_timestamp.isoformat(),
            "processing_timestamp": self.processing_timestamp.isoformat(),
            "processing_version": self.processing_version,
            "filtered": self.filtered,
            **self.additional_metadata,
        }

    def to_geotiff_tags(self) -> Dict[str, str]:
        """Convert to GeoTIFF tag dict suitable for rasterio's update_tags().

        All values are serialised as strings because GDAL stores tag values
        as strings internally.

        Returns:
            Dict mapping radarlib tag names to string values, compatible with
            the tag constants used in create_raw_cog().
        """
        return {
            "radarlib_volume_number": self.volume_number,
            "radarlib_strategy": self.strategy,
            "radarlib_field_name": self.field_name,
            "radarlib_radar_name": self.radar_name,
            "radarlib_radar_coverage_m": str(self.radar_coverage_m),
            "radarlib_observation_timestamp": self.observation_timestamp.isoformat(),
            "radarlib_processing_timestamp": self.processing_timestamp.isoformat(),
            "radarlib_processing_version": self.processing_version,
            "radarlib_filtered": str(self.filtered),
        }


def parse_observation_timestamp(timestamp_str: str) -> datetime:
    """Parse an observation timestamp string from volume_info format.

    Args:
        timestamp_str: String in format ``"YYYYMMDDTHHMMSSZ"``
            (e.g. ``"20260401T205000Z"``), as stored in
            ``volume_info["observation_datetime"]``.

    Returns:
        datetime.datetime with UTC timezone (``timezone.utc``).

    Raises:
        ValueError: If ``timestamp_str`` does not match the expected format.
    """
    try:
        # dt = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%SZ")
        dt = datetime.fromisoformat(timestamp_str)
        return dt.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(
            f"Invalid observation timestamp format: {timestamp_str!r}. " "Expected 'YYYYMMDDTHHMMSSZ'."
        ) from exc


def get_radar_coverage_km(radar: Any) -> float:
    """Extract the maximum radar coverage distance in kilometres.

    Args:
        radar: A ``pyart.core.Radar`` object. The coverage distance is read
            from ``radar.range['data'][-1]``, which PyART stores in metres.

    Returns:
        Radar coverage distance in kilometres as a float.

    Note:
        The value is derived from the last element of the range gate array,
        which represents the far edge of the outermost range gate.
    """
    coverage_m = float(radar.range["data"][-1])
    return coverage_m / 1000.0
