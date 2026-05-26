# -*- coding: utf-8 -*-
"""
DEPRECATED: Legacy product generators (PNG and RGBA GeoTIFF).

These generators are deprecated and will be removed in a future version.
Use ``product_type='raw_cog'`` (i.e. ``_generate_raw_cog_products_sync``) instead.

This module is kept for reference and backward-compatibility only.
Do **not** use it in new code.

Migration guide
---------------
Old configuration::

    ProductGenerationDaemonConfig(
        product_type='image',   # or 'geotiff'
        ...
    )

New configuration::

    ProductGenerationDaemonConfig(
        product_type='raw_cog',
        ...
    )

The ``raw_cog`` generator outputs single-band float32 Cloud-Optimized GeoTIFFs
with colormap/vmin/vmax stored as file-level metadata, enabling dynamic
colormap changes at serve time without re-processing the data.
"""

import gc
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict

import numpy as np
from pyart.config import get_field_name

from radarlib import config
from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf
from radarlib.utils.fields_utils import determine_reflectivity_fields, get_lowest_nsweep
from radarlib.utils.names_utils import product_path_and_filename_legacy

logger = logging.getLogger(__name__)


def generate_geotiff_products_sync_deprecated(
    daemon: Any,
    netcdf_path: Path,
    volume_info: Dict[str, Any],
) -> None:
    """Generate RGBA uint8 GeoTIFF products (deprecated).

    This function is no longer maintained.  Use ``raw_cog`` generation instead.
    Kept for backward compatibility with old ``product_type='geotiff'``
    configurations.  Will be removed in a future version.

    Args:
        daemon: The :class:`~radarlib.daemons.ProductGenerationDaemon` instance
            (provides ``daemon.config`` and ``daemon.geometry``).
        netcdf_path: Path to the source NetCDF volume file.
        volume_info: Volume metadata dict from ``SQLiteStateTracker``.
    """
    logger.warning(
        "GeoTIFF product generation is deprecated and will be removed. " "Please switch to product_type='raw_cog'."
    )

    from radarlib.radar_grid import (
        GateFilter,
        GridFilter,
        apply_geometry,
        column_max,
        constant_elevation_ppi,
        get_field_data,
        save_product_as_geotiff,
    )
    from radarlib.utils.memory_profiling import log_memory_usage

    filename = str(netcdf_path)
    vol_types = daemon.config.volume_types

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
            logger.debug(
                f"Could not parse volume structure from {filename_stem}: {e}. Proceeding with available fields."
            )

        # Get lowest sweep for PPI products
        sweep = get_lowest_nsweep(radar)

        # --- Generate COLMAX -----------------------------------------------------------
        if daemon.config.add_colmax:
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
                        daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        colmax_data,
                    )
                    log_memory_usage("After apply_geometry for unfiltered COLMAX")
                    colmax = column_max(
                        colmax_data_unfiltered,
                        geometry=daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                    )

                    # Save as COG using convenience function
                    output_file = Path(temp_dir) / "ppi.cog"
                    save_product_as_geotiff(
                        colmax,
                        daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        float(radar.latitude["data"].data[0]),
                        float(radar.longitude["data"].data[0]),
                        output_file,
                        product_type="COLMAX",
                        cmap=cmap,
                        vmin=vmin,
                        vmax=vmax,
                        as_cog=True,
                        overview_factors=[2, 4, 8, 16],
                        resampling_method="average",
                    )
                    log_memory_usage("After save_product_as_geotiff for unfiltered COLMAX generation")
                    if output_file.exists():
                        output_dict = product_path_and_filename_legacy(
                            radar, colmax_field, sweep, round_filename=True, filtered=False, extension="tif"
                        )

                        target_subdir = daemon.config.local_product_dir / output_dict["ceiled"][0]
                        target_subdir.mkdir(parents=True, exist_ok=True)
                        target_path = target_subdir / output_dict["ceiled"][1]
                        shutil.move(str(output_file), str(target_path))
                        logger.info(f"Generated unfiltered COG: {colmax_field} sweep {sweep} -> {target_path.name}")

                        rounded_subdir = daemon.config.local_product_dir / output_dict["rounded"][0]
                        rounded_subdir.mkdir(parents=True, exist_ok=True)
                        rounded_path = rounded_subdir / output_dict["rounded"][1]
                        if target_path != rounded_path:
                            shutil.copy2(target_path, rounded_path)
                            logger.debug(f"Created rounded version: {rounded_path.name}")

                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"COLMAX generated successfully for {filename_stem}.")

                except Exception as e:
                    logger.error(f"Error generating COLMAX for {filename_stem}: {e}")
                log_memory_usage("After saving geotiff for unfiltered COLMAX")

            if "COLMAX" in config.FILTERED_FIELDS_TO_PLOT:
                logger.debug(f"Generating Filtered COLMAX for {filename_stem}")
                try:
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
                        daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        colmax_data,
                        additional_filters=[gf],
                    )
                    log_memory_usage("After apply_geometry for filtered COLMAX")
                    colmax = column_max(
                        colmax_data_filtered,
                        geometry=daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                    )
                    gridf = GridFilter()
                    colmax = gridf.apply_below(colmax, config.COLMAX_THRESHOLD)

                    output_file = Path(temp_dir) / "ppi.cog"
                    save_product_as_geotiff(
                        colmax,
                        daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                        float(radar.latitude["data"].data[0]),
                        float(radar.longitude["data"].data[0]),
                        output_file,
                        product_type="COLMAX",
                        cmap=cmap,
                        vmin=vmin,
                        vmax=vmax,
                        as_cog=True,
                        overview_factors=[2, 4, 8, 16],
                        resampling_method="average",
                    )
                    log_memory_usage("After save_product_as_geotiff for COLMAX")

                    if output_file.exists():
                        output_dict = product_path_and_filename_legacy(
                            radar, colmax_field, sweep, round_filename=True, filtered=True, extension="tif"
                        )
                        target_subdir = daemon.config.local_product_dir / output_dict["ceiled"][0]
                        target_subdir.mkdir(parents=True, exist_ok=True)
                        target_path = target_subdir / output_dict["ceiled"][1]
                        shutil.move(str(output_file), str(target_path))
                        logger.info(f"Generated unfiltered COG: {colmax_field} sweep {sweep} -> {target_path.name}")

                        rounded_subdir = daemon.config.local_product_dir / output_dict["rounded"][0]
                        rounded_subdir.mkdir(parents=True, exist_ok=True)
                        rounded_path = rounded_subdir / output_dict["rounded"][1]
                        if target_path != rounded_path:
                            shutil.copy2(target_path, rounded_path)
                            logger.debug(f"Created rounded version: {rounded_path.name}")

                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"COLMAX generated successfully for {filename_stem}.")

                except Exception as e:
                    logger.error(f"Error generating COLMAX for {filename_stem}: {e}")
                log_memory_usage("After saving geotiff for filtered COLMAX")

        # --- Prepare field lists ----------------------------------------------------
        cog_generated = False
        fields_to_plot = config.FIELDS_TO_PLOT
        plotted_fields = [f for f in fields_to_plot if f in radar.fields]

        # --- COG Generation block (unfiltered) ----------------------------------------------
        logger.info(f"Generating unfiltered COG products for {filename_stem}")

        for field in list(plotted_fields):
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

                temp_dir = tempfile.mkdtemp()

                field_data = get_field_data(radar, plot_field)
                grid_data = apply_geometry(daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]], field_data)
                log_memory_usage(f"After apply_geometry for unfiltered {plot_field}")

                elevation_angle = radar.get_elevation(sweep)
                elevation_angle = float(np.unique(elevation_angle)[0])
                ppi = constant_elevation_ppi(
                    grid_data,
                    daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                    elevation_angle=elevation_angle,
                    interpolation="linear",
                )

                output_file = Path(temp_dir) / "ppi.cog"
                save_product_as_geotiff(
                    ppi,
                    daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                    float(radar.latitude["data"].data[0]),
                    float(radar.longitude["data"].data[0]),
                    output_file,
                    product_type="PPI",
                    cmap=cmap,
                    vmin=vmin,
                    vmax=vmax,
                    as_cog=True,
                    overview_factors=[2, 4, 8, 16],
                    resampling_method="average",
                )
                log_memory_usage(f"After save_product_as_geotiff for unfiltered {plot_field}")

                if output_file.exists():
                    output_dict = product_path_and_filename_legacy(
                        radar, plot_field, sweep, round_filename=True, filtered=False, extension="tif"
                    )
                    target_subdir = daemon.config.local_product_dir / output_dict["ceiled"][0]
                    target_subdir.mkdir(parents=True, exist_ok=True)
                    target_path = target_subdir / output_dict["ceiled"][1]
                    shutil.move(str(output_file), str(target_path))
                    logger.info(f"Generated unfiltered COG: {plot_field} sweep {sweep} -> {target_path.name}")

                    rounded_subdir = daemon.config.local_product_dir / output_dict["rounded"][0]
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
                    if config.GRC_RHV_FILTER:
                        gf.exclude_below(rhv_field, config.GRC_RHV_THRESHOLD)
                    if config.GRC_WRAD_FILTER:
                        gf.exclude_above(wrad_field, config.GRC_WRAD_THRESHOLD)
                    if config.GRC_REFL_FILTER:
                        gf.exclude_below(hrefl_field, config.GRC_REFL_THRESHOLD)
                    if config.GRC_ZDR_FILTER:
                        gf.exclude_above(zdr_field, config.GRC_ZDR_THRESHOLD)

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
                    daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                    field_data,
                    additional_filters=[gf],
                )
                log_memory_usage(f"After apply_geometry for filtered {plot_field}")

                elevation_angle = radar.get_elevation(sweep)
                elevation_angle = float(np.unique(elevation_angle)[0])
                ppi = constant_elevation_ppi(
                    grid_data,
                    daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                    elevation_angle=elevation_angle,
                    interpolation="linear",
                )

                output_file = Path(temp_dir) / "ppi.cog"
                save_product_as_geotiff(
                    ppi,
                    daemon.geometry[volume_info["strategy"]][volume_info["vol_nr"]],
                    float(radar.latitude["data"].data[0]),
                    float(radar.longitude["data"].data[0]),
                    output_file,
                    product_type="PPI",
                    cmap=cmap,
                    vmin=vmin,
                    vmax=vmax,
                    as_cog=True,
                    overview_factors=[2, 4, 8, 16],
                    resampling_method="average",
                )
                log_memory_usage(f"After save_product_as_geotiff for filtered {plot_field}")

                if output_file.exists():
                    output_dict = product_path_and_filename_legacy(
                        radar, plot_field, sweep, round_filename=True, filtered=True, extension="tif"
                    )
                    target_subdir = daemon.config.local_product_dir / output_dict["ceiled"][0]
                    target_subdir.mkdir(parents=True, exist_ok=True)
                    target_path = target_subdir / output_dict["ceiled"][1]
                    shutil.move(str(output_file), str(target_path))
                    logger.info(f"Generated filtered COG: {plot_field} sweep {sweep} -> {target_path.name}")

                    rounded_subdir = daemon.config.local_product_dir / output_dict["rounded"][0]
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
        try:
            if "radar" in locals():
                del radar
        except Exception:
            logger.debug("Failed to delete radar object during cleanup", exc_info=False)
        gc.collect()


def generate_image_products_sync_deprecated(
    daemon: Any,
    netcdf_path: Path,
    volume_info: Dict[str, Any],
) -> None:
    """Generate PNG products via matplotlib (deprecated).

    This function is no longer maintained.  Use ``raw_cog`` generation instead.
    Kept for backward compatibility with old ``product_type='image'``
    configurations.  Will be removed in a future version.

    Args:
        daemon: The :class:`~radarlib.daemons.ProductGenerationDaemon` instance
            (provides ``daemon.config`` and ``daemon.geometry``).
        netcdf_path: Path to the source NetCDF volume file.
        volume_info: Volume metadata dict from ``SQLiteStateTracker``.
    """
    logger.warning(
        "PNG product generation is deprecated and will be removed. " "Please switch to product_type='raw_cog'."
    )

    import matplotlib

    # Set backend to Agg for non-interactive plotting
    matplotlib.use("Agg")

    import matplotlib.pyplot as plt
    import pyart

    from radarlib.io.pyart.colmax import generate_colmax
    from radarlib.io.pyart.filters import filter_fields_grc1
    from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig, RadarPlotConfig, plot_ppi_field, save_ppi_png

    filename = str(netcdf_path)
    vol_types = daemon.config.volume_types

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

        rhv_field = get_field_name("cross_correlation_ratio")
        zdr_field = get_field_name("differential_reflectivity")
        cm_field = get_field_name("clutter_map")
        phidp_field = get_field_name("differential_phase")
        kdp_field = get_field_name("specific_differential_phase")
        vrad_field = get_field_name("velocity")
        wrad_field = get_field_name("spectrum_width")
        colmax_field = get_field_name("colmax")

        filename_stem = Path(filename).stem

        fields_to_check = vol_types[filename_stem.split("_")[1]][filename_stem.split("_")[2]][:]
        radar_fields = radar.fields.keys()
        missing_fields = set(fields_to_check) - set(radar_fields)

        if missing_fields:
            logger.info(
                f"Incomplete volume {filename_stem}: missing {missing_fields}. "
                f"Will generate products for available fields: {set(radar_fields) & set(fields_to_check)}"
            )
        else:
            logger.debug("Complete volume - all expected fields present.")

        # --- Generate COLMAX -----------------------------------------------------------
        if daemon.config.add_colmax:
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
                logger.error(f"Error generating COLMAX for {filename_stem}: {e}")

        # --- Prepare plotting lists ----------------------------------------------------
        field_plotted = False
        fields_to_plot = config.FIELDS_TO_PLOT
        plotted_fields = [f for f in fields_to_plot if f in radar.fields]

        # --- Plotting block (unfiltered) ----------------------------------------------
        plot_config = RadarPlotConfig(figsize=(15, 15), dpi=config.PNG_DPI, transparent=True)
        plt.ioff()

        try:
            for field in list(plotted_fields):
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
                    fig, ax = plot_ppi_field(radar, field, sweep=sweep, config=plot_config, field_config=field_config)
                    try:
                        output_dict = product_path_and_filename_legacy(
                            radar, plot_field, sweep, round_filename=True, filtered=False
                        )
                        _ = save_ppi_png(
                            fig,
                            os.path.join(daemon.config.local_product_dir, output_dict["ceiled"][0]),
                            output_dict["ceiled"][1],
                            dpi=plot_config.dpi,
                            transparent=plot_config.transparent,
                        )
                        _ = save_ppi_png(
                            fig,
                            os.path.join(daemon.config.local_product_dir, output_dict["rounded"][0]),
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
                    fig, ax = plot_ppi_field(radar, field, sweep=sweep, config=plot_config, field_config=field_config)
                    try:
                        output_dict = product_path_and_filename_legacy(
                            radar, plot_field, sweep, round_filename=True, filtered=True
                        )
                        _ = save_ppi_png(
                            fig,
                            os.path.join(daemon.config.local_product_dir, output_dict["ceiled"][0]),
                            output_dict["ceiled"][1],
                            dpi=plot_config.dpi,
                            transparent=plot_config.transparent,
                        )
                        _ = save_ppi_png(
                            fig,
                            os.path.join(daemon.config.local_product_dir, output_dict["rounded"][0]),
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
        try:
            import matplotlib.pyplot as plt

            plt.close("all")
        except Exception:
            logger.debug("Failed to close matplotlib figures during cleanup", exc_info=False)

        try:
            if "radar" in locals():
                del radar
        except Exception:
            logger.debug("Failed to delete radar object during cleanup", exc_info=False)

        gc.collect()
