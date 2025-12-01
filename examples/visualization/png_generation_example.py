"""
Example: End-to-end PNG generation from RMA5 BUFR files using legacy pipeline.

This example demonstrates:
1. Decoding BUFR files with bufr_to_dict
2. Creating PyART Radar objects with the BUFR to PyART converter
3. Generating PPI visualizations as PNG files
"""

import logging
from pathlib import Path

from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart
from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig, RadarPlotConfig, plot_and_save_ppi

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def example_fields_png():
    """
    Example 1: Plot a single field combining multiple BUFR files.

    This example demonstrates decoding multiple BUFR files and combining
    their field data into a single PyART Radar object.
    """
    logger.info("=" * 80)
    logger.info("Example 1: Fields PNG Generation (Multiple BUFR Files)")
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

        # Decode BUFR file using bufr_to_dict with legacy=True to get format
        # compatible with bufr_to_pyart_legacy
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

    # Create radar object using legacy converter with list of field dicts
    logger.info("Creating PyART Radar object from decoded fields...")
    radar = bufr_to_pyart(fields=bufr_dicts, logger_name="example")

    logger.info(f"✓ Radar created with fields: {list(radar.fields.keys())}")

    # Plot configuration
    config = RadarPlotConfig(figsize=(12, 12), dpi=150, transparent=True)

    # Output directory
    output_dir = Path("outputs/example_fields")
    output_dir.mkdir(parents=True, exist_ok=True)

    # loop over all the fields in the radar and plot each one
    for field in radar.fields.keys():
        field_config = FieldPlotConfig(field_name=field)

        # field = list(radar.fields.keys())[0]
        logger.info(f"Plotting field: {field}")

        # Generate PNG
        output_file = plot_and_save_ppi(
            radar=radar,
            field=field,
            output_path=str(output_dir),
            filename=f"{field}_sweep00_new.png",
            # sweep=0,
            config=config,
            field_config=field_config,
        )

        logger.info(f"✓ PNG saved to: {output_file}")
        logger.info("")


if __name__ == "__main__":
    logger.info("\n")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 78 + "║")
    logger.info("║" + "  PyART Radar PNG Generation Examples".center(78) + "║")
    logger.info("║" + "  Using New BUFR Pipeline".center(78) + "║")
    logger.info("║" + " " * 78 + "║")
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("")

    # Run examples
    try:
        example_fields_png()

        logger.info("╔" + "=" * 78 + "╗")
        logger.info("║" + "  Example completed successfully! ✓".center(78) + "║")
        logger.info("╚" + "=" * 78 + "╝")

    except Exception as e:
        logger.error(f"Error during examples: {e}", exc_info=True)
