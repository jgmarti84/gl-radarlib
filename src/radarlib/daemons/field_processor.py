# -*- coding: utf-8 -*-
"""
Field processor classes for modular COG product generation.

Provides :class:`FieldProcessor` (ABC) and :class:`RawCogFieldProcessor` to
encapsulate the per-field pipeline:

    field_data ──► apply_geometry ──► constant_elevation_ppi ──► create_raw_cog
                                                                         │
                                                               apply_metadata_to_cog
                                                                         │
                                                                   final .tif path

Usage example::

    processor = RawCogFieldProcessor(config=daemon.config, volume_info=volume_info, radar_name="RMA1")
    output_path = processor.process_and_save(
        field_data=field_data,
        field_name="DBZH",
        radar=radar,
        geometry=geometry,
        gate_filter=gf,          # pass None for unfiltered
        sweep=0,
        config_key_field="REFL",  # maps DBZH → config keys like VMIN_REFL / CMAP_REFL
    )
"""

import logging
import shutil
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np

from radarlib.daemons.metadata_utils import build_product_metadata
from radarlib.radar_grid import GridGeometry

if TYPE_CHECKING:
    from radarlib.daemons.product_daemon import ProductGenerationDaemonConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def apply_coverage_radius_mask(
    data_2d: np.ndarray,
    geometry: "GridGeometry",
    coverage_radius_m: float,
) -> np.ndarray:
    """Mask Cartesian grid cells that lie outside the radar coverage radius.

    Computes the ground-range distance from the grid origin (radar position)
    for every (y, x) cell and sets cells beyond *coverage_radius_m* to masked.
    This prevents interpolation artefacts from appearing at the grid edges
    when the configured grid extent is larger than the actual radar sweep range.

    Args:
        data_2d: 2-D float array of shape ``(ny, nx)``.
        geometry: :class:`~radarlib.radar_grid.GridGeometry` describing the grid
            extent.  ``grid_limits`` must follow the convention
            ``((z_min, z_max), (y_min, y_max), (x_min, x_max))`` in metres.
        coverage_radius_m: Radar coverage radius in metres (typically
            ``float(radar.range["data"][-1])``).

    Returns:
        A :class:`numpy.ma.MaskedArray` equivalent to *data_2d* with all cells
        whose ground-range distance exceeds *coverage_radius_m* masked out.
    """
    import numpy.ma as ma

    (_, _), (y_min, y_max), (x_min, x_max) = geometry.grid_limits
    ny, nx = data_2d.shape[-2], data_2d.shape[-1]

    x = np.linspace(x_min, x_max, nx)
    y = np.linspace(y_min, y_max, ny)
    xx, yy = np.meshgrid(x, y)
    distance = np.sqrt(xx**2 + yy**2)
    outside_coverage = distance >= coverage_radius_m

    if isinstance(data_2d, ma.MaskedArray):
        combined_mask = np.ma.getmaskarray(data_2d) | outside_coverage
        return ma.array(data_2d.data, mask=combined_mask, fill_value=data_2d.fill_value)
    else:
        return ma.array(data_2d, mask=outside_coverage)


def get_field_data_safe(radar: Any, field_name: str) -> np.ndarray:
    """Safely retrieve field data from a PyART Radar object.

    Args:
        radar: PyART Radar object.
        field_name: Name of the radar field (e.g. ``"DBZH"``).

    Returns:
        Masked numpy array of the field data.

    Raises:
        KeyError: If *field_name* is not present in ``radar.fields``.
        ValueError: If the field entry has no ``"data"`` key / value.
    """
    if field_name not in radar.fields:
        raise KeyError(f"Field '{field_name}' not found in radar. " f"Available fields: {list(radar.fields.keys())}")

    field_dict = radar.fields[field_name]
    data = field_dict.get("data")
    if data is None:
        raise ValueError(f"Field '{field_name}' entry has no 'data' array.")

    return data


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------


class FieldProcessor(ABC):
    """Base class for processing and saving radar fields as COGs.

    Subclasses implement :meth:`process_and_save` to perform the field-specific
    processing pipeline (interpolation, colormap resolution, COG creation, etc.).
    """

    def __init__(
        self,
        config: "ProductGenerationDaemonConfig",
        volume_info: Dict[str, Any],
        radar_name: str,
    ) -> None:
        """Initialise the processor.

        Args:
            config: :class:`~radarlib.daemons.product_daemon.ProductGenerationDaemonConfig`
                instance holding daemon settings.
            volume_info: Volume metadata dict from the SQLite state tracker.  Must
                contain at least ``strategy``, ``vol_nr``, and ``observation_datetime``.
            radar_name: Radar station identifier (e.g. ``"RMA1"``).
        """
        self.config = config
        self.volume_info = volume_info
        self.radar_name = radar_name

    @abstractmethod
    def process_and_save(
        self,
        field_data: np.ndarray,
        field_name: str,
        radar: Any,
        geometry: GridGeometry,
        gate_filter: Optional[Any] = None,
        output_dir: Optional[Path] = None,
        sweep: int = 0,
        config_key_field: Optional[str] = None,
    ) -> Optional[Path]:
        """Process field data and save as a COG file.

        Args:
            field_data: Raw polar-coordinate field data array (masked numpy array
                directly from ``radar.fields[name]["data"]``).
            field_name: Radar field name used for output filename and metadata
                (e.g. ``"DBZH"``).
            radar: PyART Radar object — used for coordinate extraction and metadata.
            geometry: :class:`~radarlib.radar_grid.GridGeometry` describing the
                Cartesian output grid for Barnes interpolation.
            gate_filter: Optional PyART / radarlib ``GateFilter`` to apply before
                gridding.  Pass ``None`` for unfiltered output.
            output_dir: Root directory for output COG files.  Defaults to
                ``self.config.local_product_dir`` when *None*.
            sweep: Sweep index used for elevation angle extraction and for the
                legacy filename helper.
            config_key_field: Config key prefix used to look up colormap/vmin/vmax
                (e.g. ``"REFL"`` for reflectivity fields, or the field name itself
                for polarimetric fields).  Defaults to *field_name* when *None*.

        Returns:
            Absolute :class:`~pathlib.Path` to the generated COG file, or ``None``
            if processing failed (errors are logged; exceptions are not re-raised).
        """
        ...


# ---------------------------------------------------------------------------
# Concrete implementation
# ---------------------------------------------------------------------------


class RawCogFieldProcessor(FieldProcessor):
    """Processor that creates single-band float32 Cloud-Optimized GeoTIFFs.

    This is the production processor used when
    ``ProductGenerationDaemonConfig.product_type == 'raw_cog'``.

    Pipeline
    --------
    1. Resolve colormap / vmin / vmax from ``radarlib.config`` using
       *config_key_field* and a ``_NOFILTERS`` suffix for unfiltered data.
    2. Apply Barnes interpolation via :func:`~radarlib.radar_grid.apply_geometry`,
       optionally applying *gate_filter*.
    3. Extract constant-elevation PPI slice via
       :func:`~radarlib.radar_grid.constant_elevation_ppi`.
    4. Build :class:`~radarlib.daemons.product_metadata.ProductMetadata` via
       :func:`~radarlib.daemons.metadata_utils.build_product_metadata`.
    5. Write COG to a temp file via :func:`~radarlib.radar_grid.create_raw_cog`.
    6. Embed metadata tags via :func:`~radarlib.daemons.metadata_utils.apply_metadata_to_cog`.
    7. Move COG to final output path (both *ceiled* and *rounded* timestamp variants).
    8. Return the final path.
    """

    def process_and_save(
        self,
        field_data: np.ndarray,
        field_name: str,
        radar: Any,
        geometry: GridGeometry,
        gate_filter: Optional[Any] = None,
        output_dir: Optional[Path] = None,
        sweep: int = 0,
        config_key_field: Optional[str] = None,
    ) -> Optional[Path]:
        """Process and save a single field as a float32 COG.

        See :meth:`FieldProcessor.process_and_save` for parameter documentation.
        """
        # Deferred heavy imports to avoid slowing down module loads
        import datetime as _dt

        from radarlib import config as radarlib_config
        from radarlib.daemons.product_metadata import parse_observation_timestamp
        from radarlib.radar_grid import apply_geometry, constant_elevation_ppi, create_raw_cog
        from radarlib.utils.memory_profiling import log_memory_usage
        from radarlib.utils.names_utils import product_path_and_filename

        if output_dir is None:
            output_dir = self.config.local_product_dir

        filtered: bool = gate_filter is not None
        key_field: str = config_key_field if config_key_field is not None else field_name

        # Config key suffix convention:
        #   unfiltered → VMIN_<key>_NOFILTERS / CMAP_<key>_NOFILTERS
        #   filtered   → VMIN_<key>            / CMAP_<key>
        suffix = "" if filtered else "_NOFILTERS"
        cmap = radarlib_config.__dict__.get(f"CMAP_{key_field}{suffix}", None)
        vmin = radarlib_config.__dict__.get(f"VMIN_{key_field}{suffix}", None)
        vmax = radarlib_config.__dict__.get(f"VMAX_{key_field}{suffix}", None)

        grid_data: Optional[np.ndarray] = None
        ppi: Optional[np.ndarray] = None

        try:
            # --- Apply geometry (Barnes interpolation) -----------------------------------
            additional_filters = [gate_filter] if gate_filter is not None else []
            grid_data = apply_geometry(geometry, field_data, additional_filters=additional_filters)
            log_memory_usage(f"After apply_geometry for {'filtered' if filtered else 'unfiltered'} {field_name}")

            # --- Constant elevation PPI --------------------------------------------------
            elevation_array = radar.get_elevation(sweep)
            elevation_angle = float(np.unique(elevation_array)[0])
            ppi = constant_elevation_ppi(
                grid_data,
                geometry,
                elevation_angle=elevation_angle,
                interpolation="linear",
            )

            # --- Coverage radius mask ----------------------------------------------------
            # Mask out all Cartesian cells that fall outside the actual radar
            # sweep range to avoid Barnes interpolation artefacts at grid edges.
            try:
                coverage_radius_m = float(radar.range["data"][-1])
                ppi = apply_coverage_radius_mask(ppi, geometry, coverage_radius_m)
                log_memory_usage(f"After coverage mask for {'filtered' if filtered else 'unfiltered'} {field_name}")
            except Exception as _mask_err:
                logger.warning(
                    f"[RawCogFieldProcessor] Could not apply coverage radius mask for "
                    f"field '{field_name}': {_mask_err}. Proceeding without mask."
                )

            # --- Build structured metadata -----------------------------------------------
            metadata = build_product_metadata(
                radar=radar,
                volume_info=self.volume_info,
                field_name=field_name,
                radar_name=self.radar_name,
                filtered=filtered,
            )

            # --- Resolve output paths (v2 naming, ceiled + rounded variants) ----------
            # Reproduce pre-refactor dual-save behaviour:
            #   ceiled  = floor(obs_time + 10 min, 10 min)  → "next" 10-min boundary
            #   rounded = round(obs_time, 10 min)           → nearest 10-min boundary
            # Both paths are written so that consumers can find the product even
            # when the rounded and the ceiled timestamps differ.
            obs_dt = parse_observation_timestamp(self.volume_info["observation_datetime"])
            strategy = self.volume_info["strategy"]
            vol_nr = self.volume_info["vol_nr"]

            # Ceiled: add 10 min then floor to 10-min boundary
            ceiled_dt_raw = obs_dt + _dt.timedelta(minutes=10)
            ceiled_min = (ceiled_dt_raw.minute // 10) * 10
            ceiled_dt = ceiled_dt_raw.replace(minute=ceiled_min, second=0, microsecond=0)

            # Rounded: round obs_dt to nearest 10 min (handles roll-over to next hour)
            rounded_min_val = round(obs_dt.minute / 10) * 10
            if rounded_min_val == 60:
                rounded_dt = obs_dt.replace(minute=0, second=0, microsecond=0) + _dt.timedelta(hours=1)
            else:
                rounded_dt = obs_dt.replace(minute=rounded_min_val, second=0, microsecond=0)

            target_path = product_path_and_filename(
                self.radar_name,
                strategy,
                vol_nr,
                field_name,
                ceiled_dt,
                output_dir,
                filtered=filtered,
            )
            rounded_path = product_path_and_filename(
                self.radar_name,
                strategy,
                vol_nr,
                field_name,
                rounded_dt,
                output_dir,
                filtered=filtered,
            )

            # --- Write COG ---------------------------------------------------------------
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_cog_path = Path(temp_dir) / "ppi.cog"

                create_raw_cog(
                    ppi,
                    geometry,
                    float(radar.latitude["data"].data[0]),
                    float(radar.longitude["data"].data[0]),
                    temp_cog_path,
                    cmap=cmap,
                    vmin=vmin,
                    vmax=vmax,
                    overview_factors=[2, 4, 8, 16],
                    resampling_method="average",
                    extra_tags=metadata.to_geotiff_tags(),
                )
                log_memory_usage(f"After create_raw_cog for {'filtered' if filtered else 'unfiltered'} {field_name}")

                if not temp_cog_path.exists():
                    logger.error(
                        f"[RawCogFieldProcessor] COG file was not created for field '{field_name}'. "
                        f"Skipping metadata and move steps."
                    )
                    return None

                shutil.move(str(temp_cog_path), str(target_path))

            # --- Create rounded timestamp variant ----------------------------------------
            if target_path != rounded_path:
                shutil.copy2(str(target_path), str(rounded_path))
                logger.debug(f"Created rounded-timestamp variant: {rounded_path.name}")

            logger.info(
                f"Generated {'filtered' if filtered else 'unfiltered'} raw COG: "
                f"{field_name} sweep {sweep} -> {target_path.name}"
            )
            return target_path

        except Exception as e:
            logger.error(
                f"[RawCogFieldProcessor] Failed to process field '{field_name}': {e}",
                exc_info=True,
            )
            return None

        finally:
            if grid_data is not None:
                del grid_data
            if ppi is not None:
                del ppi
