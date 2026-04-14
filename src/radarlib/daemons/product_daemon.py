# -*- coding: utf-8 -*-
"""Product generation daemon for monitoring and generating visualization products from processed NetCDF volumes."""
import asyncio
import gc
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
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
    stuck_volume_timeout_minutes: int = 60
    geometry_types: Optional[Dict[str, Dict[str, Any]]] = None
    ftp_host: Optional[str] = config.FTP_HOST
    ftp_user: Optional[str] = config.FTP_USER
    ftp_password: Optional[str] = config.FTP_PASS

    def __post_init__(self):
        # Validate product type
        if self.geometry_types is None:
            self.geometry_types = {}


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

    def __init__(self, config: ProductGenerationDaemonConfig):
        """
        Initialize the product generation daemon.

        Args:
            config: Daemon configuration
        """
        self.config = config
        self.state_tracker = SQLiteStateTracker(config.state_db)
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
                    gate_coords_filename = f"{self.config.radar_name}_{strategy}_{vol_num}_gate_coordinates.npz"
                    gate_coords_file_path = os.path.join(config.ROOT_GATE_COORDS_PATH, gate_coords_filename)
                    if Path(gate_coords_file_path).exists():
                        logger.debug(f"Using gate coordinates file: {gate_coords_file_path}")
                    else:
                        from radarlib.utils.grid_utils import create_gate_coords_file

                        # Pass the field names from vol_types so the FTP search
                        # only considers BUFR files for fields that will actually
                        # be interpolated (e.g. ['VRAD', 'WRAD'] for vol 02).
                        # This prevents downloading a BUFR with different scan
                        # geometry than the fields used in product generation.
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
                        )
                        msg = "Created gate coordinates file path does not match expected path. "
                        msg += "Check create_gate_coords_file implementation."
                        assert str(created_coords_file_path) == gate_coords_file_path, logger.warning(msg)
                        gate_coords_file_path = str(created_coords_file_path)
                        logger.info(f"Created gate coordinates file: {gate_coords_file_path}")

                    try:
                        gate_coords = np.load(gate_coords_file_path)
                        logger.info(f"Loaded gate coordinates from file: {gate_coords_file_path}")

                    except Exception as e:
                        raise Exception(f"Failed to load gate coordinates from {gate_coords_file_path}: {e}")

                    # Get gate coordinates
                    gate_x = gate_coords["gate_x"]
                    gate_y = gate_coords["gate_y"]
                    gate_z = gate_coords["gate_z"]

                    z_grid_limits = (0.0, roi_params["toa"])
                    y_grid_limits = (gate_y.min(), gate_y.max())
                    x_grid_limits = (gate_x.min(), gate_x.max())

                    # z_points = int(np.ceil(z_grid_limits[1] / roi_params["res_z"])) + 1
                    # y_points = int((y_grid_limits[1] - y_grid_limits[0]) / roi_params["res_xy"])
                    # x_points = int((x_grid_limits[1] - x_grid_limits[0]) / roi_params["res_xy"])
                    z_points, y_points, x_points = calculate_grid_points(
                        z_grid_limits, y_grid_limits, x_grid_limits, roi_params["res_xy"], roi_params["res_z"]
                    )

                    grid_shape = (z_points, y_points, x_points)
                    grid_limits = (z_grid_limits, y_grid_limits, x_grid_limits)

                    # Create temporary directory for intermediate processing
                    with tempfile.TemporaryDirectory() as temp_dir:
                        # Compute geometry
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

                    save_geometry(geometry, file_path)
                    result_geometry[strategy][vol_num] = geometry
                    continue
                except Exception as e:
                    logger.error(
                        f"Failed to build geometry for {self.config.radar_name} {strategy}-{vol_num}: {e}",
                        exc_info=True,
                    )
                    msg = "Exhausted all strategies for geometry initialization  "
                    msg += f"{self.config.radar_name} ({strategy}-{vol_num}): {e}"
                    raise Exception(msg) from e
        return result_geometry

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
        Synchronous COG (Cloud Optimized GeoTIFF) product generation logic.

        Generates COG files using radar_processor library for all fields in the radar volume.
        Similar flow to PNG generation but outputs GeoTIFF files instead.
        """
        from radarlib.utils.memory_profiling import log_memory_usage

        filename = str(netcdf_path)
        vol_types = self.config.volume_types

        try:
            # --- Load volume -----------------------------------------------------------------
            log_memory_usage("Before loading radar")
            try:
                radar = read_radar_netcdf(filename)
                logger.debug(f"Volume {filename} loaded successfully for COG generation.")
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

            # --- Determine reflectivity fields (horizontal and vertical) ---
            fields = determine_reflectivity_fields(radar)
            hrefl_field = fields["hrefl_field"]
            hrefl_field_raw = fields["hrefl_field_raw"]
            vrefl_field = fields["vrefl_field"]
            vrefl_field_raw = fields["vrefl_field_raw"]

            # polarimetric and product field names
            rhv_field = get_field_name("cross_correlation_ratio")
            zdr_field = get_field_name("differential_reflectivity")
            phidp_field = get_field_name("differential_phase")
            kdp_field = get_field_name("specific_differential_phase")
            vrad_field = get_field_name("velocity")
            wrad_field = get_field_name("spectrum_width")
            colmax_field = get_field_name("colmax")

            filename_stem = Path(filename).stem

            # Verify volume completeness - log missing fields but don't reject volume
            try:
                strategy = filename_stem.split("_")[1]
                vol_nr = filename_stem.split("_")[2]
                fields_expected = vol_types[strategy][vol_nr][:]
                radar_fields = set(radar.fields.keys())
                missing_fields = set(fields_expected) - radar_fields

                if missing_fields:
                    logger.info(
                        f"Incomplete volume {filename_stem}: missing {missing_fields}. "
                        f"Will generate COGs for available fields: {radar_fields & set(fields_expected)}"
                    )
                else:
                    logger.debug("Complete volume - all expected fields present.")
            except (IndexError, KeyError) as e:
                logger.debug(f"Could not parse volume structure from {filename_stem}: {e}. Proceeding with available fields.")

            # Get lowest sweep for PPI products
            sweep = get_lowest_nsweep(radar)
            from radarlib.radar_grid import (
                GateFilter,
                GridFilter,
                apply_geometry,
                column_max,
                constant_elevation_ppi,
                get_field_data,
                save_product_as_geotiff,
            )

            # --- Generate COLMAX -----------------------------------------------------------
            if self.config.add_colmax:
                # Non filtered COLMAX
                if "COLMAX" in config.FIELDS_TO_PLOT:
                    logger.debug(f"Generating COLMAX for {filename_stem}")
                    try:
                        # Validate field exists before accessing
                        if hrefl_field not in radar.fields:
                            logger.warning(
                                f"Cannot generate COLMAX: Reflectivity field '{hrefl_field}' not found. "
                                f"Available fields: {set(radar.fields.keys())}. Skipping COLMAX."
                            )
                        else:
                            # COLMAX is generated from the reflectivity field
                            colmax_data = get_field_data(radar, hrefl_field)

                        temp_dir = tempfile.mkdtemp()
                        vmin_key = "VMIN_REFL_NOFILTERS"
                        vmax_key = "VMAX_REFL_NOFILTERS"
                        cmap_key = "CMAP_REFL_NOFILTERS"
                        vmin = config.__dict__.get(vmin_key, None)
                        vmax = config.__dict__.get(vmax_key, None)
                        cmap = config.__dict__.get(cmap_key, None)
                        log_memory_usage("Before unfiltered COLMAX generation")
                        colmax_data_unfiltered = apply_geometry(
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            colmax_data,
                            # additional_filters=[],
                        )
                        log_memory_usage("After apply_geometry for unfiltered COLMAX")
                        colmax = column_max(
                            colmax_data_unfiltered,
                            geometry=self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        )

                        # Save as COG using convenience function
                        output_file = Path(temp_dir) / "ppi.cog"
                        save_product_as_geotiff(
                            colmax,
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            float(radar.latitude["data"].data[0]),
                            float(radar.longitude["data"].data[0]),
                            output_file,
                            product_type="COLMAX",
                            cmap=cmap,
                            vmin=vmin,
                            vmax=vmax,
                            as_cog=True,
                            overview_factors=[2, 4, 8, 16],
                            resampling_method="average",  # Better for intensity data
                        )
                        log_memory_usage("After save_product_as_geotiff for unfiltered COLMAX generation")
                        if output_file.exists():
                            # Generate the proper filename using radarlib naming convention
                            output_dict = product_path_and_filename(
                                radar, colmax_field, sweep, round_filename=True, filtered=False, extension="tif"
                            )

                            # Ceiled version path
                            target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                            target_subdir.mkdir(parents=True, exist_ok=True)
                            target_path = target_subdir / output_dict["ceiled"][1]

                            shutil.move(str(output_file), str(target_path))
                            logger.info(f"Generated unfiltered COG: {colmax_field} sweep {sweep} -> {target_path.name}")

                            # Also create the "rounded" version if different from ceiled
                            rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                            rounded_subdir.mkdir(parents=True, exist_ok=True)
                            rounded_path = rounded_subdir / output_dict["rounded"][1]

                            if target_path != rounded_path:
                                shutil.copy2(target_path, rounded_path)
                                logger.debug(f"Created rounded version: {rounded_path.name}")

                            cog_generated = True

                        shutil.rmtree(temp_dir, ignore_errors=True)
                        logger.debug(f"COLMAX generated successfully for {filename_stem}.")

                    except Exception as e:
                        error_msg = f"Generating COLMAX: {e}"
                        logger.error(f"Error generating COLMAX for {filename_stem}: {e}")
                    log_memory_usage("After saving geotiff for unfiltered COLMAX")

                if "COLMAX" in config.FILTERED_FIELDS_TO_PLOT:
                    # filtered COLMAX
                    logger.debug(f"Generating Filtered COLMAX for {filename_stem}")
                    try:
                        # COLMAX is generated from the reflectivity field
                        colmax_data = get_field_data(radar, hrefl_field)

                        temp_dir = tempfile.mkdtemp()
                        vmin_key = "VMIN_REFL"
                        vmax_key = "VMAX_REFL"
                        cmap_key = "CMAP_REFL"
                        vmin = config.__dict__.get(vmin_key, None)
                        vmax = config.__dict__.get(vmax_key, None)
                        cmap = config.__dict__.get(cmap_key, None)

                        gf = GateFilter(radar)
                        gf.exclude_below_elevation_angle(config.COLMAX_ELEV_LIMIT1)
                        if config.COLMAX_RHOHV_FILTER:
                            gf.exclude_below(rhv_field, config.COLMAX_RHOHV_UMBRAL)
                        if config.COLMAX_WRAD_FILTER:
                            gf.exclude_above(wrad_field, config.COLMAX_WRAD_UMBRAL)
                        if config.COLMAX_TDR_FILTER:
                            gf.exclude_above(zdr_field, config.COLMAX_TDR_UMBRAL)

                        log_memory_usage("Before filtered COLMAX generation")
                        colmax_data_filtered = apply_geometry(
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            colmax_data,
                            additional_filters=[gf],
                        )
                        log_memory_usage("After apply_geometry for filtered COLMAX")
                        colmax = column_max(
                            colmax_data_filtered, geometry=self.geometry[volume_info["strategy"]][volume_info["vol_nr"]]
                        )
                        gridf = GridFilter()
                        colmax = gridf.apply_below(colmax, config.COLMAX_THRESHOLD)

                        # Save as COG using convenience function
                        output_file = Path(temp_dir) / "ppi.cog"
                        save_product_as_geotiff(
                            colmax,
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            float(radar.latitude["data"].data[0]),
                            float(radar.longitude["data"].data[0]),
                            output_file,
                            product_type="COLMAX",
                            cmap=cmap,
                            vmin=vmin,
                            vmax=vmax,
                            as_cog=True,
                            overview_factors=[2, 4, 8, 16],
                            resampling_method="average",  # Better for intensity data
                        )
                        log_memory_usage("After save_product_as_geotiff for COLMAX")

                        if output_file.exists():
                            # Generate the proper filename using radarlib naming convention
                            output_dict = product_path_and_filename(
                                radar, colmax_field, sweep, round_filename=True, filtered=True, extension="tif"
                            )

                            # Ceiled version path
                            target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                            target_subdir.mkdir(parents=True, exist_ok=True)
                            target_path = target_subdir / output_dict["ceiled"][1]

                            shutil.move(str(output_file), str(target_path))
                            logger.info(f"Generated unfiltered COG: {colmax_field} sweep {sweep} -> {target_path.name}")

                            # Also create the "rounded" version if different from ceiled
                            rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                            rounded_subdir.mkdir(parents=True, exist_ok=True)
                            rounded_path = rounded_subdir / output_dict["rounded"][1]

                            if target_path != rounded_path:
                                shutil.copy2(target_path, rounded_path)
                                logger.debug(f"Created rounded version: {rounded_path.name}")

                            cog_generated = True

                        shutil.rmtree(temp_dir, ignore_errors=True)
                        logger.debug(f"COLMAX generated successfully for {filename_stem}.")

                    except Exception as e:
                        error_msg = f"Generating COLMAX: {e}"
                        logger.error(f"Error generating COLMAX for {filename_stem}: {e}")
                        # Continue with plotting even if COLMAX fails
                    log_memory_usage("After saving geotiff for filtered COLMAX")

            # --- Prepare field lists ----------------------------------------------------
            cog_generated = False
            fields_to_plot = config.FIELDS_TO_PLOT
            plotted_fields = [f for f in fields_to_plot if f in radar.fields]

            # --- COG Generation block (unfiltered) ----------------------------------------------
            logger.info(f"Generating unfiltered COG products for {filename_stem}")

            for field in list(plotted_fields):
                # special mapping for reflectivity raw/renamed
                if field in (hrefl_field, hrefl_field_raw):
                    plot_field = hrefl_field_raw
                elif field in (vrefl_field, vrefl_field_raw):
                    plot_field = vrefl_field_raw
                else:
                    plot_field = field

                if plot_field not in radar.fields:
                    continue

                try:
                    # Get vmin/vmax/cmap from config (NOFILTERS version)
                    if field in [hrefl_field, vrefl_field, colmax_field]:
                        key_field = "REFL"
                    else:
                        key_field = plot_field

                    vmin_key = f"VMIN_{key_field}_NOFILTERS"
                    vmax_key = f"VMAX_{key_field}_NOFILTERS"
                    cmap_key = f"CMAP_{key_field}_NOFILTERS"
                    vmin = config.__dict__.get(vmin_key, None)
                    vmax = config.__dict__.get(vmax_key, None)
                    cmap = config.__dict__.get(cmap_key, None)

                    temp_dir = tempfile.mkdtemp()

                    # # Prepare overrides
                    field_data = get_field_data(radar, plot_field)
                    grid_data = apply_geometry(
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]], field_data
                    )
                    log_memory_usage(f"After apply_geometry for unfiltered {plot_field}")

                    # Generate PPI
                    elevation_angle = radar.get_elevation(sweep)
                    elevation_angle = float(np.unique(elevation_angle)[0])
                    ppi = constant_elevation_ppi(
                        grid_data,
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        elevation_angle=elevation_angle,
                        interpolation="linear",
                    )

                    # Save as COG using convenience function
                    output_file = Path(temp_dir) / "ppi.cog"
                    save_product_as_geotiff(
                        ppi,
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        float(radar.latitude["data"].data[0]),
                        float(radar.longitude["data"].data[0]),
                        output_file,
                        product_type="PPI",
                        cmap=cmap,
                        vmin=vmin,
                        vmax=vmax,
                        as_cog=True,
                        overview_factors=[2, 4, 8, 16],
                        resampling_method="average",  # Better for intensity data
                    )
                    log_memory_usage(f"After save_product_as_geotiff for unfiltered {plot_field}")

                    if output_file.exists():
                        # Generate the proper filename using radarlib naming convention
                        output_dict = product_path_and_filename(
                            radar, plot_field, sweep, round_filename=True, filtered=False, extension="tif"
                        )

                        # Ceiled version path
                        target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                        target_subdir.mkdir(parents=True, exist_ok=True)
                        target_path = target_subdir / output_dict["ceiled"][1]

                        shutil.move(str(output_file), str(target_path))
                        logger.info(f"Generated unfiltered COG: {plot_field} sweep {sweep} -> {target_path.name}")

                        # Also create the "rounded" version if different from ceiled
                        rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                        rounded_subdir.mkdir(parents=True, exist_ok=True)
                        rounded_path = rounded_subdir / output_dict["rounded"][1]

                        if target_path != rounded_path:
                            shutil.copy2(target_path, rounded_path)
                            logger.debug(f"Created rounded version: {rounded_path.name}")

                        cog_generated = True

                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"Generated unfiltered COG for {plot_field} successfully.")

                except Exception as e:
                    logger.error(f"Error generating unfiltered COG for {plot_field}: {e}")
                    continue
                log_memory_usage(f"After saved geotiff for unfiltered {plot_field}")

            # --- COG Generation block (filtered) ----------------------------------------------
            logger.info(f"Generating filtered COG products for {filename_stem}")

            filtered_fields_to_plot = config.FILTERED_FIELDS_TO_PLOT
            filtered_plotted_fields = [f for f in filtered_fields_to_plot if f in radar.fields]

            for field in list(filtered_plotted_fields):
                plot_field = field
                if plot_field not in radar.fields:
                    continue

                try:
                    if field in [
                        hrefl_field,
                        vrefl_field,
                        rhv_field,
                        phidp_field,
                        kdp_field,
                        zdr_field,
                        wrad_field,
                        vrad_field,
                    ]:
                        gf = GateFilter(radar)
                        # Standard QC filters
                        if config.GRC_RHV_FILTER:
                            gf.exclude_below(rhv_field, config.GRC_RHV_THRESHOLD)
                            # filters_list.append({"field": rhv_field, "min": config.GRC_RHV_THRESHOLD, "max": None})
                        if config.GRC_WRAD_FILTER:
                            gf.exclude_above(wrad_field, config.GRC_WRAD_THRESHOLD)
                            # filters_list.append({"field": wrad_field, "min": None, "max": config.GRC_WRAD_THRESHOLD})
                        if config.GRC_REFL_FILTER:
                            gf.exclude_below(hrefl_field, config.GRC_REFL_THRESHOLD)
                            # filters_list.append({"field": hrefl_field, "min": config.GRC_REFL_THRESHOLD, "max": None})
                        if config.GRC_ZDR_FILTER:
                            gf.exclude_above(zdr_field, config.GRC_ZDR_THRESHOLD)
                            # filters_list.append({"field": zdr_field, "min": None, "max": config.GRC_ZDR_THRESHOLD})

                    # Get vmin/vmax/cmap from config (filtered version, without NOFILTERS)
                    if field in [hrefl_field, vrefl_field, colmax_field]:
                        key_field = "REFL"
                    else:
                        key_field = plot_field

                    vmin_key = f"VMIN_{key_field}"
                    vmax_key = f"VMAX_{key_field}"
                    cmap_key = f"CMAP_{key_field}"
                    vmin = config.__dict__.get(vmin_key, None)
                    vmax = config.__dict__.get(vmax_key, None)
                    cmap = config.__dict__.get(cmap_key, None)

                    temp_dir = tempfile.mkdtemp()

                    field_data = get_field_data(radar, plot_field)
                    grid_data = apply_geometry(
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        field_data,
                        additional_filters=[gf],
                    )
                    log_memory_usage(f"After apply_geometry for filtered {plot_field}")

                    # Generate PPI
                    elevation_angle = radar.get_elevation(sweep)
                    elevation_angle = float(np.unique(elevation_angle)[0])
                    ppi = constant_elevation_ppi(
                        grid_data,
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        elevation_angle=elevation_angle,
                        interpolation="linear",
                    )

                    # Save as COG using convenience function
                    output_file = Path(temp_dir) / "ppi.cog"
                    save_product_as_geotiff(
                        ppi,
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        float(radar.latitude["data"].data[0]),
                        float(radar.longitude["data"].data[0]),
                        output_file,
                        product_type="PPI",
                        cmap=cmap,
                        vmin=vmin,
                        vmax=vmax,
                        as_cog=True,
                        overview_factors=[2, 4, 8, 16],
                        resampling_method="average",  # Better for intensity data
                    )
                    log_memory_usage(f"After save_product_as_geotiff for filtered {plot_field}")

                    if output_file.exists():
                        # Generate the proper filename using radarlib naming convention
                        output_dict = product_path_and_filename(
                            radar, plot_field, sweep, round_filename=True, filtered=True, extension="tif"
                        )
                        # Ceiled version path
                        target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                        target_subdir.mkdir(parents=True, exist_ok=True)
                        target_path = target_subdir / output_dict["ceiled"][1]

                        shutil.move(str(output_file), str(target_path))
                        logger.info(f"Generated filtered COG: {plot_field} sweep {sweep} -> {target_path.name}")

                        # Also create the "rounded" version if different from ceiled
                        rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                        rounded_subdir.mkdir(parents=True, exist_ok=True)
                        rounded_path = rounded_subdir / output_dict["rounded"][1]

                        if target_path != rounded_path:
                            shutil.copy2(target_path, rounded_path)
                            logger.debug(f"Created rounded version: {rounded_path.name}")

                        cog_generated = True

                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"Generated filtered COG for {plot_field} successfully.")

                except Exception as e:
                    logger.error(f"Error generating filtered COG for {plot_field}: {e}")
                    continue
                log_memory_usage(f"After saved geotiff for filtered {plot_field}")

            if not cog_generated:
                logger.warning(
                    f"No filtered COG products were successfully generated for {filename_stem}. "
                    f"This may indicate an incomplete volume with missing fields. "
                    f"Will retry on next iteration if volume is being processed."
                )
            else:
                logger.info(f"Filtered COG product generation completed successfully for {filename_stem}")

        finally:
            # Cleanup radar object if it was created
            try:
                if "radar" in locals():
                    del radar
            except Exception:
                logger.debug("Failed to delete radar object during cleanup", exc_info=False)
            gc.collect()

    def _generate_raw_cog_products_sync(self, netcdf_path: Path, volume_info: Dict) -> None:
        """
        Synchronous raw float COG product generation logic.

        Generates single-band float32 Cloud-Optimized GeoTIFF (COG) files for all fields
        in the radar volume.  Unlike :meth:`_generate_cog_products_sync`, which bakes the
        colormap into multi-band RGBA uint8 pixels, this method stores the original data
        values together with colormap/vmin/vmax as file-level metadata.

        The resulting files can later be re-rendered with any colormap using
        :func:`~radarlib.radar_grid.remap_cog_colormap` or read as RGBA tiles on-the-fly
        via :func:`~radarlib.radar_grid.read_cog_tile_as_rgba`.

        This method is invoked when ``product_type == 'raw_cog'`` in the daemon
        configuration.

        Args:
            netcdf_path: Path to the NetCDF volume file to process
            volume_info: Dictionary with volume metadata from the state database
        """
        import gc

        from radarlib.radar_grid import (
            GateFilter,
            GridFilter,
            apply_geometry,
            column_max,
            constant_elevation_ppi,
            create_raw_cog,
            get_field_data,
        )
        from radarlib.utils.memory_profiling import log_memory_usage

        filename = str(netcdf_path)
        vol_types = self.config.volume_types

        try:
            # --- Load volume -----------------------------------------------------------------
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

            # --- Determine reflectivity fields (horizontal and vertical) ---
            fields = determine_reflectivity_fields(radar)
            hrefl_field = fields["hrefl_field"]
            hrefl_field_raw = fields["hrefl_field_raw"]
            vrefl_field = fields["vrefl_field"]
            vrefl_field_raw = fields["vrefl_field_raw"]

            # polarimetric and product field names
            rhv_field = get_field_name("cross_correlation_ratio")
            zdr_field = get_field_name("differential_reflectivity")
            phidp_field = get_field_name("differential_phase")
            kdp_field = get_field_name("specific_differential_phase")
            vrad_field = get_field_name("velocity")
            wrad_field = get_field_name("spectrum_width")
            colmax_field = get_field_name("colmax")

            filename_stem = Path(filename).stem

            # Verify volume completeness and check for missing critical fields
            fields_to_check = vol_types[filename_stem.split("_")[1]][filename_stem.split("_")[2]][:]
            radar_fields = radar.fields.keys()
            missing_fields = set(fields_to_check) - set(radar_fields)

            if missing_fields:
                logger.warning(
                    f"Incomplete volume {filename_stem}, missing fields: {missing_fields}. "
                    f"Skipping product generation for this volume."
                )
                # Skip this volume but don't raise an error - mark it as having missing fields
                raise ValueError(f"Volume has missing required fields: {missing_fields}")
            else:
                logger.debug("Complete volume.")

            # Get lowest sweep for PPI products
            sweep = get_lowest_nsweep(radar)

            # --- Generate COLMAX -----------------------------------------------------------
            if self.config.add_colmax:
                # Non-filtered COLMAX
                if "COLMAX" in config.FIELDS_TO_PLOT:
                    logger.debug(f"Generating raw COLMAX for {filename_stem}")
                    try:
                        # Validate field exists before accessing
                        if hrefl_field not in radar.fields:
                            logger.error(
                                f"Reflectivity field '{hrefl_field}' not found in radar. "
                                f"Available fields: {set(radar.fields.keys())}"
                            )
                            raise KeyError(f"Reflectivity field '{hrefl_field}' not found")

                        colmax_data = get_field_data(radar, hrefl_field)

                        vmin_key = "VMIN_REFL_NOFILTERS"
                        vmax_key = "VMAX_REFL_NOFILTERS"
                        cmap_key = "CMAP_REFL_NOFILTERS"
                        vmin = config.__dict__.get(vmin_key, None)
                        vmax = config.__dict__.get(vmax_key, None)
                        cmap = config.__dict__.get(cmap_key, None)

                        gf = GateFilter(radar)
                        gf.exclude_below_elevation_angle(config.COLMAX_ELEV_LIMIT1)
                        if config.COLMAX_RHOHV_FILTER:
                            gf.exclude_below(rhv_field, config.COLMAX_RHOHV_UMBRAL)
                        if config.COLMAX_WRAD_FILTER:
                            gf.exclude_above(wrad_field, config.COLMAX_WRAD_UMBRAL)
                        if config.COLMAX_TDR_FILTER:
                            gf.exclude_above(zdr_field, config.COLMAX_TDR_UMBRAL)
                        log_memory_usage("Before unfiltered COLMAX generation")
                        colmax_data_filtered = apply_geometry(
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            colmax_data,
                            additional_filters=[gf],
                        )
                        log_memory_usage("After apply_geometry for unfiltered COLMAX")
                        colmax = column_max(
                            colmax_data_filtered,
                            geometry=self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        )

                        with tempfile.TemporaryDirectory() as temp_dir:
                            output_file = Path(temp_dir) / "ppi.cog"
                            create_raw_cog(
                                colmax,
                                self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                                float(radar.latitude["data"].data[0]),
                                float(radar.longitude["data"].data[0]),
                                output_file,
                                cmap=cmap,
                                vmin=vmin,
                                vmax=vmax,
                                overview_factors=[2, 4, 8, 16],
                                resampling_method="average",
                            )
                            log_memory_usage("After create_raw_cog for unfiltered COLMAX")
                            if output_file.exists():
                                output_dict = product_path_and_filename(
                                    radar, colmax_field, sweep, round_filename=True, filtered=False, extension="tif"
                                )

                                target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                                target_subdir.mkdir(parents=True, exist_ok=True)
                                target_path = target_subdir / output_dict["ceiled"][1]

                                shutil.move(str(output_file), str(target_path))
                                logger.info(
                                    f"Generated unfiltered raw COG: {colmax_field} sweep {sweep} -> {target_path.name}"
                                )

                                rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                                rounded_subdir.mkdir(parents=True, exist_ok=True)
                                rounded_path = rounded_subdir / output_dict["rounded"][1]

                                if target_path != rounded_path:
                                    shutil.copy2(target_path, rounded_path)
                                    logger.debug(f"Created rounded version: {rounded_path.name}")

                        logger.debug(f"Unfiltered raw COLMAX generated successfully for {filename_stem}.")

                        # Explicit cleanup of large arrays
                        del colmax_data, colmax_data_filtered, colmax, gf
                        gc.collect()

                    except Exception as e:
                        logger.error(f"Error generating unfiltered raw COLMAX for {filename_stem}: {e}")
                    finally:
                        # Ensure cleanup even if exception occurred
                        if "colmax_data" in locals():
                            del colmax_data
                        if "colmax_data_filtered" in locals():
                            del colmax_data_filtered
                        if "colmax" in locals():
                            del colmax
                    log_memory_usage("After saving geotiff for unfiltered COLMAX")

                # Filtered COLMAX
                if "COLMAX" in config.FILTERED_FIELDS_TO_PLOT:
                    logger.debug(f"Generating filtered raw COLMAX for {filename_stem}")
                    try:
                        colmax_data = get_field_data(radar, hrefl_field)

                        vmin_key = "VMIN_REFL"
                        vmax_key = "VMAX_REFL"
                        cmap_key = "CMAP_REFL"
                        vmin = config.__dict__.get(vmin_key, None)
                        vmax = config.__dict__.get(vmax_key, None)
                        cmap = config.__dict__.get(cmap_key, None)

                        gf = GateFilter(radar)
                        gf.exclude_below_elevation_angle(config.COLMAX_ELEV_LIMIT1)
                        if config.COLMAX_RHOHV_FILTER:
                            gf.exclude_below(rhv_field, config.COLMAX_RHOHV_UMBRAL)
                        if config.COLMAX_WRAD_FILTER:
                            gf.exclude_above(wrad_field, config.COLMAX_WRAD_UMBRAL)
                        if config.COLMAX_TDR_FILTER:
                            gf.exclude_above(zdr_field, config.COLMAX_TDR_UMBRAL)

                        log_memory_usage("Before filtered COLMAX generation")
                        colmax_data_filtered = apply_geometry(
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            colmax_data,
                            additional_filters=[gf],
                        )
                        log_memory_usage("After apply_geometry for filtered COLMAX")
                        colmax = column_max(
                            colmax_data_filtered,
                            geometry=self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        )
                        gridf = GridFilter()
                        colmax = gridf.apply_below(colmax, config.COLMAX_THRESHOLD)

                        with tempfile.TemporaryDirectory() as temp_dir:
                            output_file = Path(temp_dir) / "ppi.cog"
                            create_raw_cog(
                                colmax,
                                self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                                float(radar.latitude["data"].data[0]),
                                float(radar.longitude["data"].data[0]),
                                output_file,
                                cmap=cmap,
                                vmin=vmin,
                                vmax=vmax,
                                overview_factors=[2, 4, 8, 16],
                                resampling_method="average",
                            )
                            log_memory_usage("After create_raw_cog for filtered COLMAX")

                            if output_file.exists():
                                output_dict = product_path_and_filename(
                                    radar, colmax_field, sweep, round_filename=True, filtered=True, extension="tif"
                                )

                                target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                                target_subdir.mkdir(parents=True, exist_ok=True)
                                target_path = target_subdir / output_dict["ceiled"][1]

                                shutil.move(str(output_file), str(target_path))
                                logger.info(
                                    f"Generated filtered raw COG: {colmax_field} sweep {sweep} -> {target_path.name}"
                                )

                                rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                                rounded_subdir.mkdir(parents=True, exist_ok=True)
                                rounded_path = rounded_subdir / output_dict["rounded"][1]

                                if target_path != rounded_path:
                                    shutil.copy2(target_path, rounded_path)
                                    logger.debug(f"Created rounded version: {rounded_path.name}")

                        logger.debug(f"Filtered raw COLMAX generated successfully for {filename_stem}.")

                        # Explicit cleanup of large arrays
                        del colmax_data, colmax_data_filtered, colmax, gf, gridf
                        gc.collect()

                    except Exception as e:
                        logger.error(f"Error generating filtered raw COLMAX for {filename_stem}: {e}")
                    finally:
                        # Ensure cleanup even if exception occurred
                        if "colmax_data" in locals():
                            del colmax_data
                        if "colmax_data_filtered" in locals():
                            del colmax_data_filtered
                        if "colmax" in locals():
                            del colmax
                    log_memory_usage("After saving geotiff for filtered COLMAX")

            # --- Prepare field lists ----------------------------------------------------
            raw_cog_generated = False
            fields_to_plot = config.FIELDS_TO_PLOT
            plotted_fields = [f for f in fields_to_plot if f in radar.fields]

            # --- Raw COG generation block (unfiltered) ----------------------------------
            logger.info(f"Generating unfiltered raw COG products for {filename_stem}")

            for field in list(plotted_fields):
                # special mapping for reflectivity raw/renamed
                if field in (hrefl_field, hrefl_field_raw):
                    plot_field = hrefl_field_raw
                elif field in (vrefl_field, vrefl_field_raw):
                    plot_field = vrefl_field_raw
                else:
                    plot_field = field

                if plot_field not in radar.fields:
                    continue

                try:
                    # Get vmin/vmax/cmap from config (NOFILTERS version)
                    if field in [hrefl_field, vrefl_field, colmax_field]:
                        key_field = "REFL"
                    else:
                        key_field = plot_field

                    vmin_key = f"VMIN_{key_field}_NOFILTERS"
                    vmax_key = f"VMAX_{key_field}_NOFILTERS"
                    cmap_key = f"CMAP_{key_field}_NOFILTERS"
                    vmin = config.__dict__.get(vmin_key, None)
                    vmax = config.__dict__.get(vmax_key, None)
                    cmap = config.__dict__.get(cmap_key, None)

                    field_data = get_field_data(radar, plot_field)
                    grid_data = apply_geometry(
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]], field_data
                    )
                    log_memory_usage(f"After apply_geometry for unfiltered {plot_field}")

                    elevation_angle = radar.get_elevation(sweep)
                    elevation_angle = float(np.unique(elevation_angle)[0])
                    ppi = constant_elevation_ppi(
                        grid_data,
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        elevation_angle=elevation_angle,
                        interpolation="linear",
                    )

                    with tempfile.TemporaryDirectory() as temp_dir:
                        output_file = Path(temp_dir) / "ppi.cog"
                        create_raw_cog(
                            ppi,
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            float(radar.latitude["data"].data[0]),
                            float(radar.longitude["data"].data[0]),
                            output_file,
                            cmap=cmap,
                            vmin=vmin,
                            vmax=vmax,
                            overview_factors=[2, 4, 8, 16],
                            resampling_method="average",
                        )
                        log_memory_usage(f"After create_raw_cog for unfiltered {plot_field}")

                        if output_file.exists():
                            output_dict = product_path_and_filename(
                                radar, plot_field, sweep, round_filename=True, filtered=False, extension="tif"
                            )

                            target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                            target_subdir.mkdir(parents=True, exist_ok=True)
                            target_path = target_subdir / output_dict["ceiled"][1]

                            shutil.move(str(output_file), str(target_path))
                            logger.info(
                                f"Generated unfiltered raw COG: {plot_field} sweep {sweep} -> {target_path.name}"
                            )

                            rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                            rounded_subdir.mkdir(parents=True, exist_ok=True)
                            rounded_path = rounded_subdir / output_dict["rounded"][1]

                            if target_path != rounded_path:
                                shutil.copy2(target_path, rounded_path)
                                logger.debug(f"Created rounded version: {rounded_path.name}")

                            raw_cog_generated = True

                    logger.debug(f"Generated unfiltered raw COG for {plot_field} successfully.")

                    # Explicit cleanup of large arrays
                    del field_data, grid_data, ppi
                    gc.collect()

                except Exception as e:
                    logger.error(f"Error generating unfiltered raw COG for {plot_field}: {e}")
                finally:
                    # Ensure cleanup even if exception occurred
                    if "field_data" in locals():
                        del field_data
                    if "grid_data" in locals():
                        del grid_data
                    if "ppi" in locals():
                        del ppi

                log_memory_usage(f"After saved geotiff for unfiltered {plot_field}")

            # --- Raw COG generation block (filtered) ------------------------------------
            logger.info(f"Generating filtered raw COG products for {filename_stem}")

            filtered_fields_to_plot = config.FILTERED_FIELDS_TO_PLOT
            filtered_plotted_fields = [f for f in filtered_fields_to_plot if f in radar.fields]

            for field in list(filtered_plotted_fields):
                plot_field = field
                if plot_field not in radar.fields:
                    continue

                try:
                    if field in [
                        hrefl_field,
                        vrefl_field,
                        rhv_field,
                        phidp_field,
                        kdp_field,
                        zdr_field,
                        wrad_field,
                        vrad_field,
                    ]:
                        gf = GateFilter(radar)
                        if config.GRC_RHV_FILTER:
                            gf.exclude_below(rhv_field, config.GRC_RHV_THRESHOLD)
                        if config.GRC_WRAD_FILTER:
                            gf.exclude_above(wrad_field, config.GRC_WRAD_THRESHOLD)
                        if config.GRC_REFL_FILTER:
                            gf.exclude_below(hrefl_field, config.GRC_REFL_THRESHOLD)
                        if config.GRC_ZDR_FILTER:
                            gf.exclude_above(zdr_field, config.GRC_ZDR_THRESHOLD)

                    # Get vmin/vmax/cmap from config (filtered version, without NOFILTERS)
                    if field in [hrefl_field, vrefl_field, colmax_field]:
                        key_field = "REFL"
                    else:
                        key_field = plot_field

                    vmin_key = f"VMIN_{key_field}"
                    vmax_key = f"VMAX_{key_field}"
                    cmap_key = f"CMAP_{key_field}"
                    vmin = config.__dict__.get(vmin_key, None)
                    vmax = config.__dict__.get(vmax_key, None)
                    cmap = config.__dict__.get(cmap_key, None)

                    field_data = get_field_data(radar, plot_field)
                    grid_data = apply_geometry(
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        field_data,
                        additional_filters=[gf],
                    )
                    log_memory_usage(f"After apply_geometry for filtered {plot_field}")

                    elevation_angle = radar.get_elevation(sweep)
                    elevation_angle = float(np.unique(elevation_angle)[0])
                    ppi = constant_elevation_ppi(
                        grid_data,
                        self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        elevation_angle=elevation_angle,
                        interpolation="linear",
                    )

                    with tempfile.TemporaryDirectory() as temp_dir:
                        output_file = Path(temp_dir) / "ppi.cog"
                        create_raw_cog(
                            ppi,
                            self.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                            float(radar.latitude["data"].data[0]),
                            float(radar.longitude["data"].data[0]),
                            output_file,
                            cmap=cmap,
                            vmin=vmin,
                            vmax=vmax,
                            overview_factors=[2, 4, 8, 16],
                            resampling_method="average",
                        )
                        log_memory_usage(f"After create_raw_cog for filtered {plot_field}")

                        if output_file.exists():
                            output_dict = product_path_and_filename(
                                radar, plot_field, sweep, round_filename=True, filtered=True, extension="tif"
                            )

                            target_subdir = self.config.local_product_dir / output_dict["ceiled"][0]
                            target_subdir.mkdir(parents=True, exist_ok=True)
                            target_path = target_subdir / output_dict["ceiled"][1]

                            shutil.move(str(output_file), str(target_path))
                            logger.info(f"Generated filtered raw COG: {plot_field} sweep {sweep} -> {target_path.name}")

                            rounded_subdir = self.config.local_product_dir / output_dict["rounded"][0]
                            rounded_subdir.mkdir(parents=True, exist_ok=True)
                            rounded_path = rounded_subdir / output_dict["rounded"][1]

                            if target_path != rounded_path:
                                shutil.copy2(target_path, rounded_path)
                                logger.debug(f"Created rounded version: {rounded_path.name}")

                            raw_cog_generated = True

                    logger.debug(f"Generated filtered raw COG for {plot_field} successfully.")

                    # Explicit cleanup of large arrays
                    del field_data, grid_data, ppi
                    if "gf" in locals():
                        del gf
                    gc.collect()

                except Exception as e:
                    logger.error(f"Error generating filtered raw COG for {plot_field}: {e}")
                finally:
                    # Ensure cleanup even if exception occurred
                    if "field_data" in locals():
                        del field_data
                    if "grid_data" in locals():
                        del grid_data
                    if "ppi" in locals():
                        del ppi

                log_memory_usage(f"After saved geotiff for filtered {plot_field}")

            if not raw_cog_generated:
                logger.warning(
                    f"No raw COG products were successfully generated for {filename_stem}. "
                    f"This may indicate an incomplete volume with missing fields. "
                    f"Will retry on next iteration if volume is being processed."
                )
            else:
                logger.info(f"Raw COG product generation completed successfully for {filename_stem}")

        finally:
            # Cleanup radar object if it was created
            try:
                if "radar" in locals():
                    del radar
            except Exception:
                logger.debug("Failed to delete radar object during cleanup", exc_info=False)
            gc.collect()

    def _generate_products_sync(self, netcdf_path: Path, volume_info: Dict) -> None:
        """
        Synchronous product generation logic.

        This implements the process_volume logic with all TODOs resolved.
        Runs synchronously to avoid threading issues with matplotlib and NetCDF.
        """
        # Import dependencies
        import matplotlib

        # Set backend to Agg for non-interactive plotting
        matplotlib.use("Agg")

        import matplotlib.pyplot as plt
        import pyart

        from radarlib.io.pyart.colmax import generate_colmax
        from radarlib.io.pyart.filters import filter_fields_grc1
        from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig, RadarPlotConfig, plot_ppi_field, save_ppi_png

        filename = str(netcdf_path)
        vol_types = self.config.volume_types

        try:
            # --- Load volume -----------------------------------------------------------------
            try:
                radar = read_radar_netcdf(filename)
                logger.debug(f"Volume {filename} loaded successfully.")
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

            # --- Determine reflectivity fields (horizontal and vertical) ---
            fields = determine_reflectivity_fields(radar)
            hrefl_field = fields["hrefl_field"]
            hrefl_field_raw = fields["hrefl_field_raw"]
            vrefl_field = fields["vrefl_field"]
            vrefl_field_raw = fields["vrefl_field_raw"]

            # polarimetric and product field names: we use pyart.get_field_name
            rhv_field = get_field_name("cross_correlation_ratio")
            zdr_field = get_field_name("differential_reflectivity")
            cm_field = get_field_name("clutter_map")
            phidp_field = get_field_name("differential_phase")
            kdp_field = get_field_name("specific_differential_phase")
            vrad_field = get_field_name("velocity")
            wrad_field = get_field_name("spectrum_width")
            colmax_field = get_field_name("colmax")

            # eliminamos la extension .nc
            filename_stem = Path(filename).stem

            # Verificamos el volúmen and check for missing critical fields
            fields_to_check = vol_types[filename_stem.split("_")[1]][filename_stem.split("_")[2]][:]
            radar_fields = radar.fields.keys()
            missing_fields = set(fields_to_check) - set(radar_fields)

            if missing_fields:
                logger.warning(
                    f"Incomplete volume {filename_stem}, missing fields: {missing_fields}. "
                    f"Skipping product generation for this volume."
                )
                # Skip this volume but don't raise an error - mark it as having missing fields
                raise ValueError(f"Volume has missing required fields: {missing_fields}")
            else:
                logger.debug("Complete volume.")

            # --- Generate COLMAX -----------------------------------------------------------
            if self.config.add_colmax:
                logger.debug(f"Generating COLMAX for {filename_stem}")
                try:
                    radar = generate_colmax(
                        radar=radar,
                        elev_limit1=config.COLMAX_ELEV_LIMIT1,
                        field_for_colmax=hrefl_field,
                        RHOHV_filter=config.COLMAX_RHOHV_FILTER,
                        RHOHV_umbral=config.COLMAX_RHOHV_UMBRAL,
                        WRAD_filter=config.COLMAX_WRAD_FILTER,
                        WRAD_umbral=config.COLMAX_WRAD_UMBRAL,
                        TDR_filter=config.COLMAX_TDR_FILTER,
                        TDR_umbral=config.COLMAX_TDR_UMBRAL,
                        save_changes=True,
                    )
                    logger.debug(f"COLMAX generated successfully for {filename_stem}.")
                except Exception as e:
                    error_msg = f"Generating COLMAX: {e}"
                    logger.error(f"Error generating COLMAX for {filename_stem}: {e}")
                    # Continue with plotting even if COLMAX fails

            # --- Prepare plotting lists ----------------------------------------------------
            field_plotted = False
            fields_to_plot = config.FIELDS_TO_PLOT
            plotted_fields = [f for f in fields_to_plot if f in radar.fields]

            # --- Plotting block (unfiltered) ----------------------------------------------
            plot_config = RadarPlotConfig(figsize=(15, 15), dpi=config.PNG_DPI, transparent=True)
            plt.ioff()

            try:
                for field in list(plotted_fields):
                    # special mapping for reflectivity raw/renamed
                    if field in (hrefl_field, hrefl_field_raw):
                        plot_field = hrefl_field_raw
                    elif field in (vrefl_field, vrefl_field_raw):
                        plot_field = vrefl_field_raw
                    else:
                        plot_field = field

                    if plot_field not in radar.fields:
                        continue

                    try:
                        if field in [hrefl_field, vrefl_field, colmax_field]:
                            key_field = "REFL"
                        else:
                            key_field = plot_field
                        vmin_key = f"VMIN_{key_field}_NOFILTERS"
                        vmax_key = f"VMAX_{key_field}_NOFILTERS"
                        cmap_key = f"CMAP_{key_field}_NOFILTERS"
                        vmin = config.__dict__.get(vmin_key, None)
                        vmax = config.__dict__.get(vmax_key, None)
                        cmap = config.__dict__.get(cmap_key, None)

                        sweep = get_lowest_nsweep(radar)
                        field_config = FieldPlotConfig(plot_field, vmin=vmin, vmax=vmax, cmap=cmap, sweep=sweep)
                        fig, ax = plot_ppi_field(
                            radar, field, sweep=sweep, config=plot_config, field_config=field_config
                        )
                        try:
                            output_dict = product_path_and_filename(
                                radar, plot_field, sweep, round_filename=True, filtered=False
                            )
                            _ = save_ppi_png(
                                fig,
                                os.path.join(self.config.local_product_dir, output_dict["ceiled"][0]),
                                output_dict["ceiled"][1],
                                dpi=plot_config.dpi,
                                transparent=plot_config.transparent,
                            )
                            _ = save_ppi_png(
                                fig,
                                os.path.join(self.config.local_product_dir, output_dict["rounded"][0]),
                                output_dict["rounded"][1],
                                dpi=plot_config.dpi,
                                transparent=plot_config.transparent,
                            )

                            plt.close(fig)
                            field_plotted = True
                        except Exception as e:
                            logger.error(f"Generating path/filename for {plot_field}: {e}")
                            continue
                    except Exception as e:
                        logger.error(f"Plotting unfiltered {filename_stem} | {plot_field}: {e}")
                        continue

                    finally:
                        plt.clf()
                        gc.collect()
            except Exception as e:
                error_msg = f"General plotting error: {e}"
                logger.error(f"General error plotting unfiltered: {e}")

            # --- Plotting block (filtered) ----------------------------------------------
            filtered_fields_to_plot = config.FILTERED_FIELDS_TO_PLOT
            filtered_plotted_fields = [f for f in filtered_fields_to_plot if f in radar.fields]
            try:
                for field in list(filtered_plotted_fields):
                    plot_field = field
                    if plot_field not in radar.fields:
                        continue

                    try:
                        gatefilter = pyart.correct.GateFilter(radar)
                        if field in [colmax_field]:
                            gatefilter.exclude_below(colmax_field, config.COLMAX_THRESHOLD)
                        elif field in [
                            hrefl_field,
                            vrefl_field,
                            rhv_field,
                            phidp_field,
                            kdp_field,
                            zdr_field,
                            wrad_field,
                            vrad_field,
                        ]:
                            size = int(19000 / radar.range["meters_between_gates"])
                            gatefilter = filter_fields_grc1(
                                radar,
                                rhv_field=rhv_field,
                                rhv_filter1=config.GRC_RHV_FILTER,
                                rhv_threshold1=config.GRC_RHV_THRESHOLD,
                                wrad_field=wrad_field,
                                wrad_filter=config.GRC_WRAD_FILTER,
                                wrad_threshold=config.GRC_WRAD_THRESHOLD,
                                refl_field=hrefl_field,
                                refl_filter=config.GRC_REFL_FILTER,
                                refl_threshold=config.GRC_REFL_THRESHOLD,
                                zdr_field=zdr_field,
                                zdr_filter=config.GRC_ZDR_FILTER,
                                zdr_threshold=config.GRC_ZDR_THRESHOLD,
                                refl_filter2=config.GRC_REFL_FILTER2,
                                refl_threshold2=config.GRC_REFL_THRESHOLD2,
                                cm_field=cm_field,
                                cm_filter=config.GRC_CM_FILTER,
                                rhohv_threshold2=config.GRC_RHOHV_THRESHOLD2,
                                despeckle_filter=config.GRC_DESPECKLE_FILTER,
                                size=size,
                                mean_filter=config.GRC_MEAN_FILTER,
                                mean_threshold=config.GRC_MEAN_THRESHOLD,
                                target_fields=[hrefl_field],
                                overwrite_fields=False,
                            )

                        sweep = get_lowest_nsweep(radar)
                        if field in [hrefl_field, vrefl_field, colmax_field]:
                            key_field = "REFL"
                        else:
                            key_field = plot_field
                        vmin_key = f"VMIN_{key_field}"
                        vmax_key = f"VMAX_{key_field}"
                        cmap_key = f"CMAP_{key_field}"
                        vmin = config.__dict__.get(vmin_key, None)
                        vmax = config.__dict__.get(vmax_key, None)
                        cmap = config.__dict__.get(cmap_key, None)

                        field_config = FieldPlotConfig(plot_field, vmin=vmin, vmax=vmax, cmap=cmap, sweep=sweep)
                        fig, ax = plot_ppi_field(
                            radar, field, sweep=sweep, config=plot_config, field_config=field_config
                        )
                        try:
                            output_dict = product_path_and_filename(
                                radar, plot_field, sweep, round_filename=True, filtered=True
                            )
                            _ = save_ppi_png(
                                fig,
                                os.path.join(self.config.local_product_dir, output_dict["ceiled"][0]),
                                output_dict["ceiled"][1],
                                dpi=plot_config.dpi,
                                transparent=plot_config.transparent,
                            )
                            _ = save_ppi_png(
                                fig,
                                os.path.join(self.config.local_product_dir, output_dict["rounded"][0]),
                                output_dict["rounded"][1],
                                dpi=plot_config.dpi,
                                transparent=plot_config.transparent,
                            )

                            plt.close(fig)
                            field_plotted = True
                        except Exception as e:
                            logger.error(f"Generating path/filename for filtered {plot_field}: {e}")
                            continue
                    except Exception as e:
                        logger.error(f"Plotting filtered {filename_stem} | {plot_field}: {e}")
                        continue

                    finally:
                        plt.clf()
                        gc.collect()
            except Exception as e:
                error_msg = f"General filtered plotting error: {e}"
                logger.error(f"General error plotting filtered: {e}")
                plt.close("all")
                gc.collect()

            if not field_plotted:
                logger.warning(
                    f"No fields were successfully plotted for PNG generation for {filename_stem}. "
                    f"This may indicate an incomplete volume with missing fields. PNG output is deprecated anyway."
                )
            else:
                logger.info(f"PNG product generation completed successfully for {filename_stem}")

        finally:
            # Cleanup - ensure all matplotlib figures are closed
            try:
                import matplotlib.pyplot as plt

                plt.close("all")
            except Exception:
                # Non-critical: matplotlib cleanup may fail, don't let it block shutdown
                logger.debug("Failed to close matplotlib figures during cleanup", exc_info=False)

            # Cleanup radar object if it was created
            try:
                if "radar" in locals():
                    del radar
            except Exception:
                # Non-critical: radar cleanup may fail
                logger.debug("Failed to delete radar object during cleanup", exc_info=False)

            gc.collect()

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
