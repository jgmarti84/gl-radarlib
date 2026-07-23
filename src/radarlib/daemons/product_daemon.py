# -*- coding: utf-8 -*-
"""Product generation daemon for monitoring and generating visualization products from processed NetCDF volumes."""

import asyncio
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from pyart.config import get_field_name

from radarlib import config
from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf
from radarlib.radar_grid import (
    GridGeometry,
    build_geometry_filename,
    compute_grid_geometry,
    load_geometry,
    save_geometry,
)
from radarlib.radar_grid.utils import calculate_grid_points

# from radarlib.radar_processing.grid_geometry import calculate_grid_points
from radarlib.state.sqlite_tracker import SQLiteStateTracker
from radarlib.utils.fields_utils import determine_reflectivity_fields, get_lowest_nsweep
from radarlib.utils.names_utils import product_path_and_filename

logger = logging.getLogger(__name__)


class FilterFieldsMissingError(Exception):
    """Raised when one or more gate-filter fields required by config are absent from the radar volume.

    This is a recoverable condition: the volume is incomplete and will be retried
    once the processing daemon detects that the missing fields have been downloaded.
    """


@dataclass
class ProductGenerationDaemonConfig:
    """
    Configuration for product generation daemon service.

    Attributes:
        local_netcdf_dir: Directory containing processed NetCDF files
        local_product_dir: Directory to save product output files (PNG, GeoTIFF, etc.)
        state_db: Path to SQLite database for tracking state
        volume_types: Dict mapping volume codes to valid volume numbers and field types.
                     Format: {'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}}
        radar_name: Radar name to process (e.g., "RMA1")
        poll_interval: Seconds between checks for new volumes to process
        max_concurrent_processing: (Deprecated - kept for compatibility) Processing is now sequential
        product_type: Type of product to generate:
                      - ``'image'``: PNG visualization files (default)
                      - ``'geotiff'``: Legacy multi-band RGBA Cloud-Optimized GeoTIFF (colormap
                        baked in as uint8 pixels)
                      - ``'raw_cog'``: Single-band float32 Cloud-Optimized GeoTIFF with colormap
                        and value-range stored as file metadata, enabling dynamic colormap changes
                        via :func:`~radarlib.radar_grid.remap_cog_colormap` or
                        :func:`~radarlib.radar_grid.read_cog_tile_as_rgba`
        add_colmax: Whether to generate COLMAX field (only for 'image' product type)
        add_tops_and_cores: Whether to generate convective tops & cores GeoJSON products.
        tops_and_cores_vol_nr: Volume number whose DBZH is used for tops & cores detection.
            Only volumes whose ``vol_nr`` matches this value will trigger detection.
            Defaults to ``"01"`` (the full multi-elevation polarimetric scan). Set this
            explicitly if your strategy uses a different volume number for the 3-D scan.
        tops_and_cores_output_dir: Directory to write tops & cores GeoJSON files.
        stuck_volume_timeout_minutes: Minutes to wait before resetting a stuck volume from
                                      'processing' status back to 'pending' for retry
    """

    local_netcdf_dir: Path
    local_product_dir: Path
    state_db: Path
    volume_types: Dict[str, Dict[str, List[str]]]
    radar_name: str
    poll_interval: int = 30
    max_concurrent_processing: int = 2  # Deprecated - processing is now sequential for stability
    product_type: str = "image"
    add_colmax: bool = True
    add_tops_and_cores: bool = False
    tops_and_cores_vol_nr: str = "01"
    tops_and_cores_output_dir: Optional[Path] = None
    stuck_volume_timeout_minutes: int = 60
    geometry_types: Optional[Dict[str, Dict[str, Any]]] = None
    field_value_masks: Optional[Dict[str, Dict[str, float]]] = None
    ftp_host: Optional[str] = config.FTP_HOST
    ftp_user: Optional[str] = config.FTP_USER
    ftp_password: Optional[str] = config.FTP_PASS
    start_date: Optional[datetime] = None

    def __post_init__(self):
        # Validate product type
        if self.geometry_types is None:
            self.geometry_types = {}
        if self.tops_and_cores_output_dir is None:
            self.tops_and_cores_output_dir = self.local_product_dir.parent / "tops_and_cores"

        if self.product_type != "raw_cog":
            logger.warning(
                f"Product type '{self.product_type}' is deprecated. "
                f"Please switch to product_type='raw_cog' as soon as possible. "
                f"Support for '{self.product_type}' will be removed in a future version."
            )


class ProductGenerationDaemon:
    """
    Daemon for monitoring and generating visualization products from processed NetCDF volumes.

    This daemon monitors the volume_processing table in the SQLite database,
    detects volumes with status='completed' (NetCDF files generated),
    reads the NetCDF file, generates visualization products (PNG plots, COLMAX),
    and tracks the generation status in a separate product_generation table.

    Volumes are processed sequentially to avoid threading issues with matplotlib and NetCDF
    libraries, ensuring reliable and stable product generation.

    Example:
        >>> from pathlib import Path
        >>> config = ProductGenerationDaemonConfig(
        ...     local_netcdf_dir=Path("./netcdf"),
        ...     local_product_dir=Path("./products"),
        ...     state_db=Path("./state.db"),
        ...     volume_types={'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}},
        ...     radar_name="RMA1"
        ... )
        >>> daemon = ProductGenerationDaemon(config)
        >>> asyncio.run(daemon.run())
    """

    def __init__(self, dconfig: ProductGenerationDaemonConfig):
        """
        Initialize the product generation daemon.

        Args:
            config: Daemon configuration
        """
        self.config = dconfig
        self.state_tracker = SQLiteStateTracker(dconfig.state_db)
        self._running = False

        # Ensure output directory exists
        self.config.local_product_dir.mkdir(parents=True, exist_ok=True)

        # Statistics
        self._stats = {
            "volumes_processed": 0,
            "volumes_failed": 0,
        }
        # gemoetry for Geotiff generation
        # self.geometry = self.init_geometry(config.geometry)
        self.geometry = self._init_geometry()

    def _init_geometry(self):
        """
        Initialize geometry structures from input dictionary.

        This method implements a cascading strategy to handle multiple input formats:
        1. If there is already a geometry file with the expected name based
            on radar and params, load it
        2. Check for gate coordinates file, if no file, then create it based on
            a sample radar NetCDF fetched from ftp, then build the geometry with
            corresponding params based on those gate coordinates

        Returns:
            Dictionary with structure {strategy: {vol_num: GridGeometry}} or None

        Raises:
            Exception: If all geometry initialization strategies fail
        """
        default_roi_params = {
            "res_xy": config.GEOMETRY_RES_XY,
            "res_z": config.GEOMETRY_RES_Z,
            "toa": config.GEOMETRY_TOA,
            "hfac": config.GEOMETRY_HFAC,
            "nb": config.GEOMETRY_NB,
            "bsp": config.GEOMETRY_BSP,
            "min_radius": config.GEOMETRY_MIN_RADIUS,
            "max_neighbors": config.MAX_NEIGHBORS,
            "weight_function": config.WEIGHT_FUNCTION,
        }

        vol_types_keys = set(self.config.volume_types.keys())

        result_geometry: Dict[str, Dict[str, GridGeometry]] = {}

        for strategy in vol_types_keys:
            vol_nums_keys = set(self.config.volume_types[strategy].keys())
            result_geometry[strategy] = {}

            for vol_num in vol_nums_keys:
                try:
                    roi_params_env = os.environ.get(f"ROI_PARAMS_VOL{vol_num}")
                    if roi_params_env is not None:
                        roi_params_overrides = json.loads(roi_params_env)
                    else:
                        roi_params_overrides = getattr(config, f"ROI_PARAMS_VOL{vol_num}", None) or {}

                    roi_params = dict(default_roi_params, **roi_params_overrides)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse ROI_PARAMS_VOL{vol_num} "
                        f"from environment variable: {e} or using the config yaml. Using default parameters."
                    )
                    roi_params = default_roi_params

                # Attach build parameters as metadata so the file is self-describing
                geometry_metadata = {
                    "radar_name": self.config.radar_name,
                    "strategy": strategy,
                    "volume_nr": vol_num,
                    "grid_resolution_xy": roi_params["res_xy"],
                    "grid_resolution_z": roi_params["res_z"],
                    "toa": roi_params["toa"],
                    "h_factor": roi_params["hfac"],
                    "min_radius": roi_params["min_radius"],
                    "max_neighbors": roi_params["max_neighbors"],
                    "nb": roi_params["nb"],
                    "bsp": roi_params["bsp"],
                    "weighting": roi_params["weight_function"],
                }

                # Derive the canonical filename from the build parameters
                file_name = build_geometry_filename(geometry_metadata)
                file_name = f"{file_name}.npz"
                file_path = os.path.join(config.ROOT_GEOMETRY_PATH, file_name)

                # Strategy 1: geometry file already exists - load geometry from file
                try:
                    loaded_geom = load_geometry(file_path)
                    logger.info(f"Loaded geometry from file: {file_path}")
                    result_geometry[strategy][vol_num] = loaded_geom
                    continue
                except Exception as e:
                    logger.warning(
                        f"Failed to load geometry from {file_path}: {e}. " "Will attempt alternative strategies."
                    )

                # Strategy 2: build geometry from gate coordinates file
                try:
                    geometry = self._build_geometry_for_vol(strategy, vol_num, roi_params, file_path)
                    result_geometry[strategy][vol_num] = geometry
                    continue
                except Exception as e:
                    logger.error(
                        f"Failed to build geometry for {self.config.radar_name} {strategy}-{vol_num}: {e}",
                        exc_info=True,
                    )
                    logger.warning(
                        f"Geometry unavailable for {self.config.radar_name} {strategy}-{vol_num}. "
                        f"Products requiring geometry will be skipped for this volume type "
                        f"until geometry can be built (e.g. after a successful FTP download)."
                    )
                    result_geometry[strategy][vol_num] = None
        return result_geometry

    def _build_geometry_for_vol(
        self,
        strategy: str,
        vol_num: str,
        roi_params: Dict[str, Any],
        geometry_save_path: str,
    ) -> GridGeometry:
        """
        Build geometry for a single strategy/vol_num from gate coordinates.

        Downloads a sample BUFR via FTP (with retry on different files), extracts
        gate coordinates, computes the grid geometry, and saves it to disk.

        Args:
            strategy: Strategy code (e.g. '0315')
            vol_num: Volume number (e.g. '01')
            roi_params: Region-of-interest / grid parameters dict
            geometry_save_path: Path to save the resulting .npz geometry file

        Returns:
            GridGeometry object

        Raises:
            Exception: If gate coordinate creation or geometry computation fails
        """
        gate_coords_filename = f"{self.config.radar_name}_{strategy}_{vol_num}_gate_coordinates.npz"
        gate_coords_file_path = os.path.join(config.ROOT_GATE_COORDS_PATH, gate_coords_filename)
        if Path(gate_coords_file_path).exists():
            logger.debug(f"Using existing gate coordinates file: {gate_coords_file_path}")
        else:
            from radarlib.utils.grid_utils import create_gate_coords_file

            # Pass the field names from vol_types so the FTP search
            # only considers BUFR files for fields that will actually
            # be interpolated (e.g. ['VRAD', 'WRAD'] for vol 02).
            vol_field_names = self.config.volume_types.get(strategy, {}).get(vol_num, [])

            created_coords_file_path = create_gate_coords_file(
                self.config.radar_name,
                strategy,
                vol_num,
                output_dir=config.ROOT_GATE_COORDS_PATH,
                field_names=vol_field_names or None,
                ftp_host=self.config.ftp_host,
                ftp_user=self.config.ftp_user,
                ftp_pass=self.config.ftp_password,
                lookback_hours=config.GEOMETRY_BUFR_LOOKBACK_HOURS,
                reference_dt=self.config.start_date,
            )
            gate_coords_file_path = str(created_coords_file_path)
            logger.info(f"Created gate coordinates file: {gate_coords_file_path}")

        gate_coords = np.load(gate_coords_file_path)
        logger.info(f"Loaded gate coordinates from file: {gate_coords_file_path}")

        gate_x = gate_coords["gate_x"]
        gate_y = gate_coords["gate_y"]
        gate_z = gate_coords["gate_z"]

        z_grid_limits = (0.0, roi_params["toa"])
        y_grid_limits = (gate_y.min(), gate_y.max())
        x_grid_limits = (gate_x.min(), gate_x.max())

        z_points, y_points, x_points = calculate_grid_points(
            z_grid_limits, y_grid_limits, x_grid_limits, roi_params["res_xy"], roi_params["res_z"]
        )

        grid_shape = (z_points, y_points, x_points)
        grid_limits = (z_grid_limits, y_grid_limits, x_grid_limits)

        with tempfile.TemporaryDirectory() as temp_dir:
            logger.debug("Computing grid geometry...")
            geometry = compute_grid_geometry(
                gate_x,
                gate_y,
                gate_z,
                grid_shape,
                grid_limits,
                temp_dir=temp_dir,
                toa=roi_params["toa"],
                min_radius=roi_params["min_radius"],
                radar_altitude=0,
                h_factor=roi_params["hfac"],
                nb=roi_params["nb"],
                bsp=roi_params["bsp"],
                weighting=roi_params["weight_function"],
                max_neighbors=roi_params["max_neighbors"],
                blind_range_m=gate_coords.get("blind_range_m", None),
                lowest_elev_deg=gate_coords.get("lowest_elev_deg", None),
                n_workers=8,
            )

        logger.info(f"Successfully built geometry for {self.config.radar_name} {strategy}-{vol_num}")
        os.makedirs(config.ROOT_GEOMETRY_PATH, exist_ok=True)
        save_geometry(geometry, geometry_save_path)
        return geometry

    def _ensure_geometry(self, strategy: str, vol_num: str) -> Optional[GridGeometry]:
        """
        Return geometry for the given strategy/vol, rebuilding lazily if it was None.

        Called before each product generation attempt. If the geometry was not
        available at daemon startup (e.g. FTP was down), this method retries
        the build. On success the geometry dict is updated so future calls are
        instant. On failure it returns None and logs a warning.
        """
        if self.geometry is None:
            self.geometry = {}
        if strategy not in self.geometry:
            self.geometry[strategy] = {}

        geom = self.geometry[strategy].get(vol_num)
        if geom is not None:
            return geom

        # Geometry is None — attempt lazy rebuild
        logger.info(
            f"[{self.config.radar_name}] Geometry for {strategy}-{vol_num} is not available. "
            f"Attempting lazy rebuild..."
        )
        try:
            roi_params = self._get_roi_params(vol_num)
            metadata = {
                "radar_name": self.config.radar_name,
                "strategy": strategy,
                "volume_nr": vol_num,
                "grid_resolution_xy": roi_params["res_xy"],
                "grid_resolution_z": roi_params["res_z"],
                "toa": roi_params["toa"],
                "h_factor": roi_params["hfac"],
                "min_radius": roi_params["min_radius"],
                "max_neighbors": roi_params["max_neighbors"],
                "nb": roi_params["nb"],
                "bsp": roi_params["bsp"],
                "weighting": roi_params["weight_function"],
            }
            file_name = f"{build_geometry_filename(metadata)}.npz"
            file_path = os.path.join(config.ROOT_GEOMETRY_PATH, file_name)

            # Try loading from disk first (another process may have built it)
            if Path(file_path).exists():
                geom = load_geometry(file_path)
                logger.info(f"[{self.config.radar_name}] Loaded geometry from file: {file_path}")
            else:
                geom = self._build_geometry_for_vol(strategy, vol_num, roi_params, file_path)

            self.geometry[strategy][vol_num] = geom
            logger.info(f"[{self.config.radar_name}] Geometry for {strategy}-{vol_num} rebuilt successfully")
            return geom
        except Exception as e:
            logger.warning(
                f"[{self.config.radar_name}] Lazy geometry rebuild failed for {strategy}-{vol_num}: {e}. "
                f"Will retry on next cycle."
            )
            return None

    def _get_roi_params(self, vol_num: str) -> Dict[str, Any]:
        """Return ROI params for a volume number, merging defaults with overrides."""
        default_roi_params = {
            "res_xy": config.GEOMETRY_RES_XY,
            "res_z": config.GEOMETRY_RES_Z,
            "toa": config.GEOMETRY_TOA,
            "hfac": config.GEOMETRY_HFAC,
            "nb": config.GEOMETRY_NB,
            "bsp": config.GEOMETRY_BSP,
            "min_radius": config.GEOMETRY_MIN_RADIUS,
            "max_neighbors": config.MAX_NEIGHBORS,
            "weight_function": config.WEIGHT_FUNCTION,
        }
        try:
            roi_params_env = os.environ.get(f"ROI_PARAMS_VOL{vol_num}")
            if roi_params_env is not None:
                overrides = json.loads(roi_params_env)
            else:
                overrides = getattr(config, f"ROI_PARAMS_VOL{vol_num}", None) or {}
            return dict(default_roi_params, **overrides)
        except Exception:
            return default_roi_params

    async def run(self) -> None:
        """
        Run the daemon to monitor and generate products for processed volumes.

        Continuously checks for volumes ready for product generation and processes them sequentially.
        """
        self._running = True

        logger.info(f"Starting {self.config.product_type} generation daemon for radar '{self.config.radar_name}'")
        logger.info(f"Monitoring NetCDF files in '{self.config.local_netcdf_dir}'")
        logger.info(f"Saving {self.config.product_type} files to '{self.config.local_product_dir}'")
        logger.info(
            f"Configuration: poll_interval={self.config.poll_interval}s, "
            f"stuck_timeout={self.config.stuck_volume_timeout_minutes}min, "
            f"processing_mode=sequential"
        )

        try:
            from radarlib.utils.memory_profiling import aggressive_cleanup, log_memory_usage

            _memory_monitoring = True
        except ImportError:
            _memory_monitoring = False

        _cycle_count = 0

        try:
            while self._running:
                try:
                    # Check for and reset stuck volumes
                    await self._check_and_reset_stuck_volumes()

                    # Process volumes ready for product generation
                    await self._process_volumes_for_products()

                    _cycle_count += 1

                    # Every 5 cycles: log memory and run aggressive GC
                    if _memory_monitoring and _cycle_count % 5 == 0:
                        log_memory_usage(f"Product daemon cycle {_cycle_count}")
                        aggressive_cleanup(f"Product daemon cycle {_cycle_count}")

                    # Wait before next check
                    await asyncio.sleep(self.config.poll_interval)

                except Exception as e:
                    logger.error(f"Error during {self.config.product_type} generation cycle: {e}", exc_info=True)
                    await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            logger.info(f"{self.config.product_type} daemon cancelled, shutting down...")
        except KeyboardInterrupt:
            logger.info(f"{self.config.product_type} daemon interrupted, shutting down...")
        finally:
            self._running = False
            # Log final statistics
            logger.info(
                f"{self.config.product_type} daemon shutting down. Statistics: "
                f"processed={self._stats['volumes_processed']}, "
                f"failed={self._stats['volumes_failed']}"
            )
            self.state_tracker.close()
            logger.info(f"{self.config.product_type} daemon for '{self.config.radar_name}' stopped")

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info(f"{self.config.product_type} daemon stop requested")

    async def _check_and_reset_stuck_volumes(self) -> None:
        """
        Check for volumes stuck in 'processing' status and reset them to 'pending'.

        Volumes that have been in 'processing' status for longer than the configured
        timeout will be reset to 'pending' and logged for retry.
        """
        try:
            num_reset = self.state_tracker.reset_stuck_product_generations(
                self.config.stuck_volume_timeout_minutes, self.config.product_type
            )
            if num_reset > 0:
                logger.warning(
                    f"Reset {num_reset} stuck {self.config.product_type} volume(s) from 'processing' to 'pending' "
                    f"(timeout: {self.config.stuck_volume_timeout_minutes} minutes)"
                )
        except Exception as e:
            logger.error(f"Error checking for stuck {self.config.product_type} volumes: {e}", exc_info=True)

    async def _process_volumes_for_products(self) -> None:
        """
        Process all volumes that are ready for product generation sequentially.

        Gets volumes with status='completed' and no product or product status='pending' or 'failed',
        and generates products for them one at a time to avoid threading issues.
        """
        # Get all volumes ready for product generation
        volumes = self.state_tracker.get_volumes_for_product_generation(self.config.product_type)

        if not volumes:
            logger.debug(f"No volumes ready for {self.config.product_type} generation for {self.config.radar_name}")
            return

        logger.info(f"Found {len(volumes)} volume(s) ready for {self.config.product_type} generation")

        # Process volumes sequentially to avoid threading issues with matplotlib/NetCDF
        num_success = 0
        num_failed = 0

        for volume_info in volumes:
            try:
                result = await self._generate_product_async(volume_info)
                if result:
                    num_success += 1
                else:
                    num_failed += 1
            except Exception as e:
                logger.error(f"Exception processing volume {volume_info.get('volume_id')}: {e}", exc_info=True)
                num_failed += 1

        if num_failed > 0:
            logger.warning(
                f"{self.config.product_type} generation complete: {num_success} succeeded, {num_failed} failed"
            )
        else:
            logger.info(f"{self.config.product_type} generation complete: {num_success} succeeded")

    async def _generate_product_async(self, volume_info: Dict) -> bool:
        """
        Generate products for a single volume.

        Args:
            volume_info: Dictionary with volume information from database

        Returns:
            True if successful, False otherwise
        """
        volume_id = volume_info["volume_id"]
        netcdf_path = volume_info.get("netcdf_path")
        is_complete = volume_info.get("is_complete", 0) == 1

        # Register product generation if not already registered
        self.state_tracker.register_product_generation(volume_id, self.config.product_type)

        if not netcdf_path:
            logger.error(f"No NetCDF path found for volume {volume_id}")
            self.state_tracker.mark_product_status(
                volume_id,
                self.config.product_type,
                "failed",
                error_message="No NetCDF path found",
                error_type="NO_NETCDF_PATH",
            )
            self._stats["volumes_failed"] += 1
            return False

        netcdf_file = Path(netcdf_path)
        if not netcdf_file.exists():
            logger.error(f"NetCDF file not found: {netcdf_file}")
            self.state_tracker.mark_product_status(
                volume_id,
                self.config.product_type,
                "failed",
                error_message=f"NetCDF file not found: {netcdf_file}",
                error_type="FILE_NOT_FOUND",
            )
            self._stats["volumes_failed"] += 1
            return False

        completeness_str = "complete" if is_complete else "incomplete"
        logger.info(f"Generating {self.config.product_type} for {completeness_str} volume {volume_id}...")

        # Mark as processing
        self.state_tracker.mark_product_status(volume_id, self.config.product_type, "processing")

        # For geometry-dependent product types, ensure geometry is available (lazy rebuild)
        if self.config.product_type in ("geotiff", "raw_cog"):
            strategy = volume_info.get("strategy")
            vol_nr = volume_info.get("vol_nr")
            if strategy and vol_nr:
                geom = self._ensure_geometry(strategy, vol_nr)
                if geom is None:
                    logger.warning(
                        f"Geometry not available for {strategy}-{vol_nr}. "
                        f"Marking volume {volume_id} as pending — will retry when geometry is built."
                    )
                    self.state_tracker.mark_product_status(
                        volume_id,
                        self.config.product_type,
                        "pending",
                        error_message="Geometry not yet available",
                        error_type="GEOMETRY_UNAVAILABLE",
                    )
                    return False

        try:
            # Generate products synchronously (no threading to avoid issues)
            # Route to appropriate generation method based on product_type
            if self.config.product_type == "geotiff":
                self._generate_cog_products_sync(netcdf_file, volume_info)
            elif self.config.product_type == "raw_cog":
                self._generate_raw_cog_products_sync(netcdf_file, volume_info)
            else:  # default to "image" (PNG)
                self._generate_products_sync(netcdf_file, volume_info)

            # Mark as completed
            self.state_tracker.mark_product_status(volume_id, self.config.product_type, "completed")
            logger.info(f"Successfully generated {self.config.product_type} for {completeness_str} volume {volume_id}")
            self._stats["volumes_processed"] += 1
            return True

        except FilterFieldsMissingError as e:
            logger.warning(str(e))
            self.state_tracker.mark_product_status(
                volume_id,
                self.config.product_type,
                "failed",
                error_message=str(e)[:500],
                error_type="FILTER_FIELDS_MISSING",
            )
            # Do not increment volumes_failed — this is an expected deferral, not an error.
            return False

        except Exception as e:
            error_msg = (
                f"Failed to generate {self.config.product_type} for {completeness_str} volume {volume_id}: {str(e)}"
            )
            logger.error(error_msg, exc_info=True)
            # Determine error type from exception
            error_type = type(e).__name__
            self.state_tracker.mark_product_status(
                volume_id,
                self.config.product_type,
                "failed",
                error_message=str(e)[:500],  # Limit error message length
                error_type=error_type,
            )
            self._stats["volumes_failed"] += 1
            return False

    def _generate_cog_products_sync(self, netcdf_path: Path, volume_info: Dict) -> None:
        """
        DEPRECATED: Legacy RGBA uint8 GeoTIFF generation.  Use raw_cog generation instead.

        Delegates to
        :func:`~radarlib.daemons.deprecated_generators.generate_geotiff_products_sync_deprecated`.
        """
        logger.warning(
            f"Product type 'geotiff' is deprecated. "
            f"Volume {volume_info.get('volume_id')} will be processed but GeoTIFF generation "
            f"is not recommended. Please switch to product_type='raw_cog' in your configuration."
        )
        from radarlib.daemons.deprecated_generators import generate_geotiff_products_sync_deprecated

        return generate_geotiff_products_sync_deprecated(self, netcdf_path, volume_info)

    def _generate_raw_cog_products_sync(self, netcdf_path: Path, volume_info: Dict) -> None:
        """
        Synchronous raw float COG product generation logic.

        Generates single-band float32 Cloud-Optimized GeoTIFF (COG) files for all fields
        in the radar volume.  The original floating-point values are preserved so the file
        can later be re-rendered with any colormap via
        :func:`~radarlib.radar_grid.remap_cog_colormap` or
        :func:`~radarlib.radar_grid.read_cog_tile_as_rgba`.

        Per-field processing is delegated to
        :class:`~radarlib.daemons.field_processor.RawCogFieldProcessor`.  COLMAX and
        Tops & Cores are handled by their own private helpers
        :meth:`_generate_colmax_cog` and :meth:`_generate_tops_and_cores`.

        This method is invoked when ``product_type == 'raw_cog'`` in the daemon
        configuration.

        Args:
            netcdf_path: Path to the NetCDF volume file to process.
            volume_info: Dictionary with volume metadata from the state database.
        """
        import datetime as _dt
        import gc

        from radarlib.daemons.field_processor import RawCogFieldProcessor
        from radarlib.daemons.product_metadata import parse_observation_timestamp
        from radarlib.radar_grid import get_field_data
        from radarlib.utils.memory_profiling import log_memory_usage

        filename = str(netcdf_path)
        filename_stem = Path(filename).stem
        vol_types = self.config.volume_types

        # Compute ceiled/rounded timestamps once here so every product for this
        # volume (COG, COLMAX, tops/cores) uses exactly the same values.
        obs_dt = parse_observation_timestamp(volume_info["observation_datetime"])
        ceiled_dt_raw = obs_dt + _dt.timedelta(minutes=10)
        ceiled_min = (ceiled_dt_raw.minute // 10) * 10
        ceiled_dt = ceiled_dt_raw.replace(minute=ceiled_min, second=0, microsecond=0)
        rounded_min_val = round(obs_dt.minute / 10) * 10
        if rounded_min_val == 60:
            rounded_dt = obs_dt.replace(minute=0, second=0, microsecond=0) + _dt.timedelta(hours=1)
        else:
            rounded_dt = obs_dt.replace(minute=rounded_min_val, second=0, microsecond=0)

        try:
            # --- Load and standardize volume -------------------------------------------
            log_memory_usage("Before loading radar")
            try:
                radar = read_radar_netcdf(filename)
                logger.debug(f"Volume {filename} loaded successfully for raw COG generation.")
            except Exception as e:
                error_msg = f"Reading volume: {e}"
                logger.error(f"Error reading volume {filename}: {e}")
                raise RuntimeError(error_msg)

            # --- Standardize fields ----------------------------------------------------------
            try:
                radar = estandarizar_campos_RMA(radar)
                logger.debug(f"Volume {filename} fields standardized successfully.")
            except Exception as e:
                error_msg = f"Standardizing fields: {e}"
                logger.error(f"Error standardizing fields {filename}: {e}")
                raise RuntimeError(error_msg)

            log_memory_usage("After loading and standardizing radar")

            # --- Determine field name aliases -------------------------------------------
            fields = determine_reflectivity_fields(radar)
            hrefl_field = fields["hrefl_field"]
            hrefl_field_raw = fields["hrefl_field_raw"]
            vrefl_field = fields["vrefl_field"]
            vrefl_field_raw = fields["vrefl_field_raw"]

            rhv_field = get_field_name("cross_correlation_ratio")
            zdr_field = get_field_name("differential_reflectivity")
            phidp_field = get_field_name("differential_phase")
            kdp_field = get_field_name("specific_differential_phase")
            vrad_field = get_field_name("velocity")
            wrad_field = get_field_name("spectrum_width")
            colmax_field = get_field_name("colmax")

            # --- Volume completeness check -----------------------------------------------
            try:
                fields_to_check = vol_types[filename_stem.split("_")[1]][filename_stem.split("_")[2]][:]
                missing_fields = set(fields_to_check) - set(radar.fields.keys())
                if missing_fields:
                    logger.info(
                        f"Incomplete volume {filename_stem}: missing {missing_fields}. "
                        f"Will generate raw COGs for available fields: "
                        f"{set(radar.fields.keys()) & set(fields_to_check)}"
                    )
                else:
                    logger.debug("Complete volume - all expected fields present.")
            except (IndexError, KeyError) as e:
                logger.debug(f"Could not check completeness for {filename_stem}: {e}")

            sweep = get_lowest_nsweep(radar)
            geom = self.geometry[volume_info["strategy"]][volume_info["vol_nr"]]

            # --- Create field processor --------------------------------------------------
            processor = RawCogFieldProcessor(
                config=self.config,
                volume_info=volume_info,
                radar_name=self.config.radar_name,
            )

            # --- COLMAX (uses column_max, not constant_elevation_ppi) -------------------
            if self.config.add_colmax:
                self._generate_colmax_cog(
                    radar=radar,
                    geom=geom,
                    volume_info=volume_info,
                    sweep=sweep,
                    hrefl_field=hrefl_field,
                    rhv_field=rhv_field,
                    wrad_field=wrad_field,
                    zdr_field=zdr_field,
                    colmax_field=colmax_field,
                    ceiled_dt=ceiled_dt,
                    rounded_dt=rounded_dt,
                )

            # --- Unfiltered fields -------------------------------------------------------
            raw_cog_generated = False
            logger.info(f"Generating unfiltered raw COG products for {filename_stem}")

            for field in list(config.FIELDS_TO_PLOT):
                if field == "COLMAX":
                    continue  # handled by _generate_colmax_cog above

                # Apply reflectivity alias: prefer the raw / non-renamed field name
                plot_field = field
                if field in (hrefl_field, hrefl_field_raw):
                    plot_field = hrefl_field_raw
                elif field in (vrefl_field, vrefl_field_raw):
                    plot_field = vrefl_field_raw

                if plot_field not in radar.fields:
                    continue

                config_key = "REFL" if field in (hrefl_field, vrefl_field, colmax_field) else plot_field

                field_data = None
                try:
                    field_data = get_field_data(radar, plot_field)
                    result = processor.process_and_save(
                        field_data=field_data,
                        field_name=plot_field,
                        radar=radar,
                        geometry=geom,
                        gate_filter=None,
                        sweep=sweep,
                        config_key_field=config_key,
                        output_dir=self.config.local_product_dir,
                    )
                    if result:
                        raw_cog_generated = True
                except Exception as e:
                    logger.error(f"Error generating unfiltered raw COG for {plot_field}: {e}")
                finally:
                    if field_data is not None:
                        del field_data
                    gc.collect()

                log_memory_usage(f"After unfiltered {plot_field}")

            # --- Tops & Cores (after unfiltered loop so large arrays are freed) ---------
            # NOTE: 3D arrays freed inside each iteration's finally block above.
            # Tops/cores grids are recomputed on demand — deliberate documented exception.
            if self.config.add_tops_and_cores and volume_info.get("vol_nr") == self.config.tops_and_cores_vol_nr:
                self._generate_tops_and_cores(
                    radar=radar,
                    geom=geom,
                    filename_stem=filename_stem,
                    volume_info=volume_info,
                    hrefl_field=hrefl_field,
                    rhv_field=rhv_field,
                    sweep=sweep,
                    ceiled_dt=ceiled_dt,
                    rounded_dt=rounded_dt,
                )

            # --- Filtered fields ---------------------------------------------------------
            logger.info(f"Generating filtered raw COG products for {filename_stem}")

            filtered_plotted_fields = [f for f in config.FILTERED_FIELDS_TO_PLOT if f in radar.fields and f != "COLMAX"]

            if filtered_plotted_fields:
                missing_filter_fields = self._get_missing_filter_fields(
                    radar=radar,
                    hrefl_field=hrefl_field,
                    rhv_field=rhv_field,
                    wrad_field=wrad_field,
                    zdr_field=zdr_field,
                )
                is_complete = volume_info.get("is_complete", 0) == 1
                if missing_filter_fields and not is_complete:
                    raise FilterFieldsMissingError(
                        f"Skipping filtered COGs for {filename_stem}: "
                        f"filter field(s) {missing_filter_fields} not yet present in incomplete volume. "
                        f"Will retry when volume is complete."
                    )
                elif missing_filter_fields and is_complete:
                    logger.error(
                        f"Filter field(s) {missing_filter_fields} missing from complete volume {filename_stem}. "
                        f"Generating best-effort filtered COG without those criteria."
                    )

                gf = self._build_gate_filter(
                    radar=radar,
                    hrefl_field=hrefl_field,
                    rhv_field=rhv_field,
                    wrad_field=wrad_field,
                    zdr_field=zdr_field,
                )

                for field in filtered_plotted_fields:
                    config_key = "REFL" if field in (hrefl_field, vrefl_field, colmax_field) else field

                    field_data = None
                    try:
                        field_data = get_field_data(radar, field)
                        result = processor.process_and_save(
                            field_data=field_data,
                            field_name=field,
                            radar=radar,
                            geometry=geom,
                            gate_filter=gf,
                            sweep=sweep,
                            config_key_field=config_key,
                            output_dir=self.config.local_product_dir,
                        )
                        if result:
                            raw_cog_generated = True
                    except Exception as e:
                        logger.error(f"Error generating filtered raw COG for {field}: {e}")
                    finally:
                        if field_data is not None:
                            del field_data
                        gc.collect()

                    log_memory_usage(f"After filtered {field}")

            if not raw_cog_generated:
                logger.warning(
                    f"No raw COG products were successfully generated for {filename_stem}. "
                    f"This may indicate an incomplete volume with missing fields. "
                    f"Will retry on next iteration if volume is being processed."
                )
            else:
                logger.info(f"Raw COG product generation completed successfully for {filename_stem}")

        finally:
            try:
                if "radar" in locals():
                    del radar
            except Exception:
                logger.debug("Failed to delete radar object during cleanup", exc_info=False)
            gc.collect()

    def _get_missing_filter_fields(
        self,
        radar: Any,
        hrefl_field: str,
        rhv_field: str,
        wrad_field: str,
        zdr_field: str,
    ) -> List[str]:
        """Return the names of filter fields that are enabled in config but absent from ``radar.fields``.

        A field is only considered *required* if its corresponding ``GRC_*_FILTER`` flag is True.
        Fields that are disabled in config are ignored regardless of availability.

        Returns:
            List of field name strings that are needed but missing.  Empty list means
            the gate filter can be built and applied correctly.
        """
        missing: List[str] = []
        if config.GRC_RHV_FILTER and rhv_field not in radar.fields:
            missing.append(rhv_field)
        if config.GRC_WRAD_FILTER and wrad_field not in radar.fields:
            missing.append(wrad_field)
        if config.GRC_REFL_FILTER and hrefl_field not in radar.fields:
            missing.append(hrefl_field)
        if config.GRC_ZDR_FILTER and zdr_field not in radar.fields:
            missing.append(zdr_field)
        return missing

    def _build_gate_filter(
        self,
        radar: Any,
        hrefl_field: str,
        rhv_field: str,
        wrad_field: str,
        zdr_field: str,
    ) -> Any:
        """Build a GRC-style GateFilter from config thresholds.

        Applies up to four configurable exclusion criteria (RHV, WRAD, reflectivity, ZDR)
        based on the ``GRC_*_FILTER`` / ``GRC_*_THRESHOLD`` config flags.

        Args:
            radar: PyART Radar object.
            hrefl_field: Horizontal reflectivity field name.
            rhv_field: Cross-correlation ratio field name.
            wrad_field: Spectral width field name.
            zdr_field: Differential reflectivity field name.

        Returns:
            Configured :class:`~radarlib.radar_grid.GateFilter` instance.
        """
        from radarlib.radar_grid import GateFilter

        # --- Diagnostic: log field availability vs. config flags -------------------
        def _field_status(enabled: bool, field: str) -> str:
            if not enabled:
                return f"{field}(disabled)"
            return f"{field}({'present' if field in radar.fields else 'MISSING'})"

        logger.info(
            "GateFilter field availability — "
            f"RHV={_field_status(config.GRC_RHV_FILTER, rhv_field)}  "
            f"WRAD={_field_status(config.GRC_WRAD_FILTER, wrad_field)}  "
            f"REFL={_field_status(config.GRC_REFL_FILTER, hrefl_field)}  "
            f"ZDR={_field_status(config.GRC_ZDR_FILTER, zdr_field)}"
        )
        # ---------------------------------------------------------------------------

        gf = GateFilter(radar)
        if config.GRC_RHV_FILTER:
            gf.exclude_below(rhv_field, config.GRC_RHV_THRESHOLD)
        if config.GRC_WRAD_FILTER:
            gf.exclude_above(wrad_field, config.GRC_WRAD_THRESHOLD)
        if config.GRC_REFL_FILTER:
            gf.exclude_below(hrefl_field, config.GRC_REFL_THRESHOLD)
        if config.GRC_ZDR_FILTER:
            gf.exclude_above(zdr_field, config.GRC_ZDR_THRESHOLD)

        # --- Diagnostic: log filter outcome ----------------------------------------
        n_criteria = len(gf._filter_history)
        pct = 100.0 * gf.n_excluded() / gf.n_gates if gf.n_gates else 0.0
        if n_criteria == 0:
            logger.warning(
                "GateFilter result: 0 criteria applied — no gates excluded. "
                "All enabled filter fields were missing from the radar object. "
                "Filtered COG will be identical to unfiltered COG."
            )
        else:
            logger.info(
                f"GateFilter result: {n_criteria} criteria applied — "
                f"excluded {gf.n_excluded():,}/{gf.n_gates:,} gates ({pct:.1f}%). "
                f"Criteria: {gf._filter_history}"
            )
        # ---------------------------------------------------------------------------

        return gf

    def _generate_colmax_cog(
        self,
        radar: Any,
        geom: GridGeometry,
        volume_info: Dict,
        sweep: int,
        hrefl_field: str,
        rhv_field: str,
        wrad_field: str,
        zdr_field: str,
        colmax_field: str,
        ceiled_dt: "datetime",
        rounded_dt: "datetime",
    ) -> None:
        """Generate unfiltered and/or filtered COLMAX raw COG files.

        COLMAX is derived from the horizontal reflectivity field via
        :func:`~radarlib.radar_grid.column_max` (not
        :func:`~radarlib.radar_grid.constant_elevation_ppi`), so it cannot use
        :class:`~radarlib.daemons.field_processor.RawCogFieldProcessor` directly.

        Generates the unfiltered variant when ``"COLMAX" in config.FIELDS_TO_PLOT``
        and the filtered variant when ``"COLMAX" in config.FILTERED_FIELDS_TO_PLOT``.
        Errors in either variant are logged but do not abort the overall volume.

        Args:
            radar: Loaded and standardised PyART Radar object.
            geom: Pre-built :class:`~radarlib.radar_grid.GridGeometry` for this strategy/vol.
            volume_info: Volume metadata dict from the state tracker.
            sweep: Lowest-elevation sweep index.
            hrefl_field, rhv_field, wrad_field, zdr_field, colmax_field:
                Resolved PyART field name strings.
        """
        import gc

        from radarlib.daemons.metadata_utils import build_product_metadata
        from radarlib.radar_grid import apply_geometry, column_max, create_raw_cog, get_field_data
        from radarlib.utils.memory_profiling import log_memory_usage

        strategy = volume_info["strategy"]
        vol_nr = volume_info["vol_nr"]

        for filtered in (False, True):
            list_key = config.FILTERED_FIELDS_TO_PLOT if filtered else config.FIELDS_TO_PLOT
            if "COLMAX" not in list_key:
                continue

            label = "filtered" if filtered else "unfiltered"
            colmax_data = None
            colmax_grid = None
            colmax_2d = None
            gf = None

            try:
                if hrefl_field not in radar.fields:
                    logger.warning(
                        f"Cannot generate {label} COLMAX: "
                        f"reflectivity field '{hrefl_field}' not found. "
                        f"Available: {set(radar.fields.keys())}"
                    )
                    continue

                suffix = "" if filtered else "_NOFILTERS"
                cmap = config.__dict__.get(f"CMAP_REFL{suffix}", None)
                vmin = config.__dict__.get(f"VMIN_REFL{suffix}", None)
                vmax = config.__dict__.get(f"VMAX_REFL{suffix}", None)

                colmax_data = get_field_data(radar, hrefl_field)

                # Gate filter: none for unfiltered, GRC-style (same as DBZH filtered) for filtered
                if filtered:
                    missing_filter_fields = self._get_missing_filter_fields(
                        radar=radar,
                        hrefl_field=hrefl_field,
                        rhv_field=rhv_field,
                        wrad_field=wrad_field,
                        zdr_field=zdr_field,
                    )
                    is_complete = volume_info.get("is_complete", 0) == 1
                    if missing_filter_fields and not is_complete:
                        raise FilterFieldsMissingError(
                            f"Skipping filtered COLMAX: "
                            f"filter field(s) {missing_filter_fields} not yet present in incomplete volume. "
                            f"Will retry when volume is complete."
                        )
                    elif missing_filter_fields and is_complete:
                        logger.error(
                            f"Filter field(s) {missing_filter_fields} missing from complete volume for COLMAX. "
                            f"Generating best-effort filtered COLMAX without those criteria."
                        )

                    gf = self._build_gate_filter(
                        radar=radar,
                        hrefl_field=hrefl_field,
                        rhv_field=rhv_field,
                        wrad_field=wrad_field,
                        zdr_field=zdr_field,
                    )
                    additional_filters = [gf]
                else:
                    additional_filters = []

                log_memory_usage(f"Before {label} COLMAX apply_geometry")
                colmax_grid = apply_geometry(geom, colmax_data, additional_filters=additional_filters)
                log_memory_usage(f"After {label} COLMAX apply_geometry")
                colmax_2d = column_max(colmax_grid, geometry=geom)

                # --- Coverage radius mask ------------------------------------------------
                # Mask Cartesian cells beyond the actual radar sweep range.
                try:
                    from radarlib.daemons.field_processor import apply_coverage_radius_mask

                    coverage_radius_m = float(radar.range["data"][-1])
                    colmax_2d = apply_coverage_radius_mask(colmax_2d, geom, coverage_radius_m)
                    log_memory_usage(f"After coverage mask for {label} COLMAX")
                except Exception as _mask_err:
                    logger.warning(
                        f"[_generate_colmax_cog] Could not apply coverage radius mask for "
                        f"{label} COLMAX: {_mask_err}. Proceeding without mask."
                    )

                # if filtered:
                #     gridf = GridFilter()
                #     colmax_2d = gridf.apply_below(colmax_2d, config.COLMAX_THRESHOLD)

                # Build metadata and output paths (v2 naming: ceiled + rounded variants)
                metadata = build_product_metadata(
                    radar=radar,
                    volume_info=volume_info,
                    field_name=colmax_field,
                    radar_name=self.config.radar_name,
                    filtered=filtered,
                )

                target_path = product_path_and_filename(
                    self.config.radar_name,
                    strategy,
                    vol_nr,
                    colmax_field,
                    ceiled_dt,
                    self.config.local_product_dir,
                    filtered=filtered,
                )
                rounded_path = product_path_and_filename(
                    self.config.radar_name,
                    strategy,
                    vol_nr,
                    colmax_field,
                    rounded_dt,
                    self.config.local_product_dir,
                    filtered=filtered,
                )

                with tempfile.TemporaryDirectory() as temp_dir:
                    output_file = Path(temp_dir) / "colmax.cog"
                    create_raw_cog(
                        colmax_2d,
                        geom,
                        float(radar.latitude["data"].data[0]),
                        float(radar.longitude["data"].data[0]),
                        output_file,
                        cmap=cmap,
                        vmin=vmin,
                        vmax=vmax,
                        overview_factors=[2, 4, 8, 16],
                        resampling_method="average",
                        extra_tags=metadata.to_geotiff_tags(),
                    )
                    log_memory_usage(f"After create_raw_cog for {label} COLMAX")

                    if not output_file.exists():
                        logger.error(f"[_generate_colmax_cog] COG file was not created for {label} COLMAX. Skipping.")
                        continue

                    shutil.move(str(output_file), str(target_path))
                    logger.info(f"Generated {label} raw COLMAX COG -> {target_path.name}")

                if target_path != rounded_path:
                    shutil.copy2(str(target_path), str(rounded_path))
                    logger.debug(f"Created rounded-timestamp COLMAX variant: {rounded_path.name}")

            except Exception as e:
                logger.error(f"Error generating {label} raw COLMAX: {e}", exc_info=True)
            finally:
                for obj in (colmax_data, colmax_grid, colmax_2d, gf):
                    if obj is not None:
                        del obj
                gc.collect()
            log_memory_usage(f"After {label} COLMAX saved")

    def _generate_tops_and_cores(
        self,
        radar: Any,
        geom: GridGeometry,
        filename_stem: str,
        volume_info: Dict,
        hrefl_field: str,
        rhv_field: str,
        sweep: int,
        ceiled_dt: "datetime",
        rounded_dt: "datetime",
    ) -> None:
        """Recompute Cartesian grids and run convective tops & cores detection.

        All large 3D arrays produced in the field loops are freed inside iteration
        finally blocks before this method is called.  The required DBZH 3D, COLMAX 2D,
        and RhoHV 3D grids are therefore recomputed here from scratch — this is a
        deliberate documented trade-off to keep memory usage bounded across the daemon's
        long lifecycle.

        Logs an ERROR on failure but never re-raises; tops/cores failure must not abort
        the volume's COG products.

        Args:
            radar: Loaded and standardised PyART Radar object.
            geom: Pre-built :class:`~radarlib.radar_grid.GridGeometry` for this strategy/vol.
            filename_stem: Volume filename without extension (used for logging).
            volume_info: Volume metadata dict from the state tracker.
            hrefl_field: Resolved horizontal reflectivity field name.
            rhv_field: Resolved cross-correlation ratio field name.
            sweep: Lowest-elevation sweep index.
            ceiled_dt: Pre-computed ceiled observation datetime (shared with COG step).
            rounded_dt: Pre-computed rounded observation datetime (shared with COG step).
        """
        import gc

        from radarlib.daemons.field_processor import apply_coverage_radius_mask
        from radarlib.io.pyart.cores_and_tops import generate_cores_and_tops
        from radarlib.radar_grid import apply_geometry, column_max, constant_elevation_ppi, get_field_data

        _ct_dbzh_3d = None
        _ct_colmax_2d = None
        _ct_rhohv_3d = None
        _ct_rhohv_2d = None

        try:
            # Recompute DBZH 3D + derive COLMAX 2D
            if hrefl_field in radar.fields:
                _ct_fd = get_field_data(radar, hrefl_field)
                _ct_dbzh_3d = apply_geometry(geom, _ct_fd)
                del _ct_fd
                _ct_colmax_2d = column_max(_ct_dbzh_3d, geometry=geom)
                coverage_radius_m = float(radar.range["data"][-1])
                _ct_colmax_2d = apply_coverage_radius_mask(_ct_colmax_2d, geom, coverage_radius_m)
            else:
                logger.warning(
                    f"[{self.config.radar_name}] Tops/cores: reflectivity field "
                    f"'{hrefl_field}' absent — skipping detection."
                )

            # Recompute RhoHV 3D (None when field is absent from this volume)
            if rhv_field in radar.fields:
                _ct_rhv_fd = get_field_data(radar, rhv_field)
                _ct_rhohv_3d = apply_geometry(geom, _ct_rhv_fd)
                elevation_angle = float(np.unique(radar.get_elevation(sweep))[0])
                _ct_rhohv_2d = constant_elevation_ppi(
                    _ct_rhohv_3d, geom, elevation_angle=elevation_angle, interpolation="linear"
                )
                del _ct_rhv_fd
            else:
                _ct_rhohv_2d = None
                logger.warning(
                    f"[{self.config.radar_name}] Tops/cores: RhoHV field "
                    f"'{rhv_field}' absent — proceeding without RhoHV quality gate."
                )

            if _ct_dbzh_3d is not None and _ct_colmax_2d is not None:
                _ct_nz, _ct_ny, _ct_nx = geom.grid_shape
                _ct_y_min, _ct_y_max = geom.grid_limits[1]
                _ct_x_min, _ct_x_max = geom.grid_limits[2]
                _ct_x_1d = np.linspace(_ct_x_min, _ct_x_max, _ct_nx, dtype=np.float32)
                _ct_y_1d = np.linspace(_ct_y_min, _ct_y_max, _ct_ny, dtype=np.float32)
                _ct_yy, _ct_xx = np.meshgrid(_ct_y_1d, _ct_x_1d, indexing="ij")
                _ct_z_1d = geom.z_levels().astype(np.float32)

                primary_path = generate_cores_and_tops(
                    colmax_2d=_ct_colmax_2d,
                    dbzh_3d=_ct_dbzh_3d,
                    x_coords=_ct_xx,
                    y_coords=_ct_yy,
                    z_coords=_ct_z_1d,
                    radar_lat=float(radar.latitude["data"].data[0]),
                    radar_lon=float(radar.longitude["data"].data[0]),
                    observation_time=ceiled_dt,
                    radar_code=self.config.radar_name,
                    strategy=volume_info["strategy"],
                    vol_nr=volume_info["vol_nr"],
                    output_dir=self.config.tops_and_cores_output_dir,
                    rhohv_3d=_ct_rhohv_3d,
                    rhohv_2d=_ct_rhohv_2d,
                )

                if ceiled_dt != rounded_dt:
                    rounded_ts = rounded_dt.strftime("%Y%m%dT%H%M%SZ")
                    rounded_subdir = (
                        Path(self.config.tops_and_cores_output_dir)
                        / f"{rounded_dt.year:04d}"
                        / f"{rounded_dt.month:02d}"
                        / f"{rounded_dt.day:02d}"
                    )
                    rounded_path = rounded_subdir / (
                        f"{self.config.radar_name}_{volume_info['strategy']}"
                        f"_{volume_info['vol_nr']}_{rounded_ts}_TOPS_CORES.geojson"
                    )
                    if primary_path is not None:
                        shutil.copy2(str(primary_path), str(rounded_path))
                        logger.debug(
                            f"[{self.config.radar_name}] Created rounded-timestamp TOPS_CORES variant: {rounded_path.name}"
                        )
                    else:
                        # No detections this scan, but the COG rounded copy already
                        # overwrote the COG at rounded_dt. Remove any stale tops/cores
                        # at that timestamp left by an earlier scan so COG and
                        # tops/cores stay in sync.
                        if rounded_path.exists():
                            rounded_path.unlink()
                            logger.debug(
                                f"[{self.config.radar_name}] Removed stale rounded-timestamp TOPS_CORES "
                                f"(no detections this scan): {rounded_path.name}"
                            )

        except Exception as _ct_exc:
            logger.error(
                f"[{self.config.radar_name}] Tops/cores detection failed " f"for {filename_stem}: {_ct_exc}",
                exc_info=True,
            )
        finally:
            for obj in (_ct_dbzh_3d, _ct_colmax_2d, _ct_rhohv_3d, _ct_rhohv_2d):
                if obj is not None:
                    del obj
            gc.collect()

    def _generate_products_sync(self, netcdf_path: Path, volume_info: Dict) -> None:
        """
        DEPRECATED: Legacy matplotlib PNG generation.  Use raw_cog generation instead.

        Delegates to
        :func:`~radarlib.daemons.deprecated_generators.generate_image_products_sync_deprecated`.
        """
        logger.warning(
            f"Product type 'image' (PNG) is deprecated. "
            f"Volume {volume_info.get('volume_id')} will be processed but PNG generation "
            f"is not recommended. Please switch to product_type='raw_cog' in your configuration."
        )
        from radarlib.daemons.deprecated_generators import generate_image_products_sync_deprecated

        return generate_image_products_sync_deprecated(self, netcdf_path, volume_info)

    def get_stats(self) -> Dict:
        """
        Get daemon statistics.

        Returns:
            Dictionary with daemon stats
        """
        return {
            "running": self._running,
            "volumes_processed": self._stats["volumes_processed"],
            "volumes_failed": self._stats["volumes_failed"],
            "pending_volumes": len(self.state_tracker.get_products_by_status("pending", self.config.product_type)),
            "completed_volumes": len(self.state_tracker.get_products_by_status("completed", self.config.product_type)),
        }
