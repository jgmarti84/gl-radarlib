"""
Example: End-to-end GeoTIFF generation from RMA5 BUFR files with georeferencing.

This example demonstrates:
1. Decoding BUFR files with bufr_to_dict
2. Creating PyART Radar objects with the BUFR to PyART converter
3. Generating GeoTIFF files with geographic referencing
4. Exporting to multiple formats (PNG + GeoTIFF + NetCDF) in one call
"""

import logging
from pathlib import Path

from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart
from radarlib.io.pyart.radar_png_plotter import export_fields_to_geotiff, export_fields_to_multi_format

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def example_single_field_geotiff():
    """
    Example 1: Export a single field to GeoTIFF with WGS84 georeferencing.

    This example demonstrates decoding multiple BUFR files and exporting
    a single field as a georeferenced GeoTIFF file.
    """
    logger.info("=" * 80)
    logger.info("Example 1: Single Field GeoTIFF Export (WGS84)")
    logger.info("=" * 80)

    # Path to BUFR files
    bufr_dir = Path("tests/data/bufr/RMA5")
    bufr_files = sorted(list(bufr_dir.glob("*.BUFR")))

    if not bufr_files:
        logger.error(f"No BUFR files found in {bufr_dir}")
        return

    # Decode first two BUFR files (or all available if less than 2)
    num_files = min(2, len(bufr_files))
    bufr_dicts = []

    for i in range(num_files):
        bufr_file = str(bufr_files[i])
        logger.info(f"Decoding file {i+1}/{num_files}: {Path(bufr_file).name}")

        # Decode BUFR file using bufr_to_dict
        bufr_dict = bufr_to_dict(bufr_file, legacy=False)

        if bufr_dict is None:
            logger.warning(f"  Failed to decode BUFR file: {bufr_file}")
            continue

        logger.info(f"  ✓ Decoded successfully. Data shape: {bufr_dict['data'].shape}")
        bufr_dicts.append(bufr_dict)

    if not bufr_dicts:
        logger.error("No BUFR files were successfully decoded")
        return

    logger.info(f"\n✓ Successfully decoded {len(bufr_dicts)} BUFR files")

    # Create radar object using bufr_to_pyart with list of field dicts
    logger.info("Creating PyART Radar object from decoded fields...")
    radar = bufr_to_pyart(fields=bufr_dicts, logger_name="example")

    logger.info(f"✓ Radar created with fields: {list(radar.fields.keys())}")

    # Output directory
    output_dir = Path("outputs/example_geotiff")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Export each field as GeoTIFF with WGS84 georeferencing
    for field in radar.fields.keys():
        logger.info(f"Exporting field to GeoTIFF: {field}")

        try:
            output_file = export_fields_to_geotiff(
                radar=radar,
                fields=[field],
                output_base_path=str(output_dir),
                sweep=0,
                crs="EPSG:4326",  # WGS84 lat/lon
            )

            logger.info(f"✓ GeoTIFF saved to: {output_file}")
            logger.info("")

        except Exception as e:
            logger.error(f"Error exporting {field}: {e}", exc_info=True)


def example_multi_field_utm_geotiff():
    """
    Example 2: Export multiple fields to GeoTIFF with UTM georeferencing.

    This example demonstrates exporting multiple fields as georeferenced
    GeoTIFF files using UTM coordinates (Zone 32N for RMA5 radar location).
    """
    logger.info("=" * 80)
    logger.info("Example 2: Multi-Field GeoTIFF Export (UTM Zone 32N)")
    logger.info("=" * 80)

    # Path to BUFR files
    bufr_dir = Path("tests/data/bufr/RMA5")
    bufr_files = sorted(list(bufr_dir.glob("*.BUFR")))

    if not bufr_files:
        logger.error(f"No BUFR files found in {bufr_dir}")
        return

    # Decode first two BUFR files
    num_files = min(2, len(bufr_files))
    bufr_dicts = []

    for i in range(num_files):
        bufr_file = str(bufr_files[i])
        logger.info(f"Decoding file {i+1}/{num_files}: {Path(bufr_file).name}")

        bufr_dict = bufr_to_dict(bufr_file, legacy=False)

        if bufr_dict is None:
            logger.warning(f"  Failed to decode BUFR file: {bufr_file}")
            continue

        bufr_dicts.append(bufr_dict)

    if not bufr_dicts:
        logger.error("No BUFR files were successfully decoded")
        return

    logger.info(f"\n✓ Successfully decoded {len(bufr_dicts)} BUFR files")

    # Create radar object
    logger.info("Creating PyART Radar object from decoded fields...")
    radar = bufr_to_pyart(fields=bufr_dicts, logger_name="example")

    logger.info(f"✓ Radar created with fields: {list(radar.fields.keys())}")

    # Output directory
    output_dir = Path("outputs/example_geotiff_utm")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Export all fields at once with UTM georeferencing
    logger.info("Exporting all fields to GeoTIFF with UTM Zone 32N...")

    try:
        fields_list = list(radar.fields.keys())
        output_files = export_fields_to_geotiff(
            radar=radar,
            fields=fields_list,
            output_base_path=str(output_dir),
            sweep=0,
            crs="EPSG:32632",  # UTM Zone 32N
        )

        logger.info(f"✓ Exported {len(fields_list)} fields")
        for output_file in output_files:
            logger.info(f"  ✓ {output_file}")

        logger.info("")

    except Exception as e:
        logger.error(f"Error exporting fields: {e}", exc_info=True)


def example_multi_format_export():
    """
    Example 3: Export to multiple formats (PNG + GeoTIFF + NetCDF) in one call.

    This example demonstrates the convenience of exporting radar fields to
    multiple formats simultaneously, creating organized subdirectories for
    each format.
    """
    logger.info("=" * 80)
    logger.info("Example 3: Multi-Format Export (PNG + GeoTIFF + NetCDF)")
    logger.info("=" * 80)

    # Path to BUFR files
    bufr_dir = Path("tests/data/bufr/RMA5")
    bufr_files = sorted(list(bufr_dir.glob("*.BUFR")))

    if not bufr_files:
        logger.error(f"No BUFR files found in {bufr_dir}")
        return

    # Decode first two BUFR files
    num_files = min(2, len(bufr_files))
    bufr_dicts = []

    for i in range(num_files):
        bufr_file = str(bufr_files[i])
        logger.info(f"Decoding file {i+1}/{num_files}: {Path(bufr_file).name}")

        bufr_dict = bufr_to_dict(bufr_file, legacy=False)

        if bufr_dict is None:
            logger.warning(f"  Failed to decode BUFR file: {bufr_file}")
            continue

        bufr_dicts.append(bufr_dict)

    if not bufr_dicts:
        logger.error("No BUFR files were successfully decoded")
        return

    logger.info(f"\n✓ Successfully decoded {len(bufr_dicts)} BUFR files")

    # Create radar object
    logger.info("Creating PyART Radar object from decoded fields...")
    radar = bufr_to_pyart(fields=bufr_dicts, logger_name="example")

    logger.info(f"✓ Radar created with fields: {list(radar.fields.keys())}")

    # Output directory
    output_dir = Path("outputs/example_multi_format")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Export to multiple formats with one call
    logger.info("Exporting to multiple formats (PNG, GeoTIFF, NetCDF)...")

    try:
        fields_list = list(radar.fields.keys())

        # This will create subdirectories: png/, geotiff/, netcdf/
        export_fields_to_multi_format(
            radar=radar,
            fields=fields_list,
            output_base_path=str(output_dir),
            formats=["png", "geotiff", "netcdf"],
            sweep=0,
        )

        logger.info("✓ Successfully exported to all formats")
        logger.info(f"  Output directory: {output_dir}")
        logger.info("  Subdirectories created:")
        logger.info(f"    - {output_dir}/png/")
        logger.info(f"    - {output_dir}/geotiff/")
        logger.info(f"    - {output_dir}/netcdf/")
        logger.info("")

    except Exception as e:
        logger.error(f"Error during multi-format export: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("\n")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 78 + "║")
    logger.info("║" + "  PyART Radar GeoTIFF Generation Examples".center(78) + "║")
    logger.info("║" + "  Using New BUFR Pipeline with Georeferencing".center(78) + "║")
    logger.info("║" + " " * 78 + "║")
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("")

    # Run examples
    try:
        example_single_field_geotiff()
        example_multi_field_utm_geotiff()
        example_multi_format_export()

        logger.info("╔" + "=" * 78 + "╗")
        logger.info("║" + "  All examples completed successfully! ✓".center(78) + "║")
        logger.info("╚" + "=" * 78 + "╝")

    except Exception as e:
        logger.error(f"Error during examples: {e}", exc_info=True)
