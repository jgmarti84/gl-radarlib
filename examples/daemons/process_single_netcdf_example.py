"""
Example: Process a single NetCDF file using ProductGenerationDaemon.

This example demonstrates how to use the ProductGenerationDaemon's
_generate_products_sync method to process a single NetCDF radar file
and generate PNG visualization products.

Usage:
    python process_single_netcdf_example.py --netcdf /path/to/file.nc --output /path/to/output
"""

import argparse
import logging
from pathlib import Path

from radarlib.daemons.product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def process_single_netcdf(netcdf_path: str, output_dir: str, state_db: str = None) -> bool:
    """
    Process a single NetCDF file and generate PNG products.

    Args:
        netcdf_path: Path to the NetCDF file to process
        output_dir: Directory where products will be saved
        state_db: Path to SQLite state database (optional)

    Returns:
        True if processing was successful, False otherwise

    Example:
        >>> process_single_netcdf(
        ...     netcdf_path="/data/volumes/RMA1_0315_20240101T000000Z.nc",
        ...     output_dir="/data/products",
        ...     state_db="/data/state.db"
        ... )
    """
    netcdf_path = Path(netcdf_path)
    output_dir = Path(output_dir)

    # Validate input file
    if not netcdf_path.exists():
        logger.error(f"NetCDF file not found: {netcdf_path}")
        return False

    if not netcdf_path.suffix == ".nc":
        logger.warning(f"File may not be a NetCDF: {netcdf_path}")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create state database path if not provided
    if state_db is None:
        state_db = output_dir / "state.db"

    # Extract radar info from filename
    # Expected format: RADAR_STRATEGY_VOLNUM_YYYYMMDDTHHMMSSZ.nc
    # Example: RMA1_0315_01_20240101T000000Z.nc
    filename_stem = netcdf_path.stem
    parts = filename_stem.split("_")

    if len(parts) < 3:
        logger.error(
            f"Could not parse filename. Expected format: RADAR_STRATEGY_VOLNUM_DATETIME.nc, got: {filename_stem}"
        )
        return False

    radar_name = parts[0]
    strategy = parts[1]
    vol_num = parts[2]

    logger.info(f"Processing {radar_name} {strategy}-{vol_num}")
    logger.info(f"Input file: {netcdf_path}")
    logger.info(f"Output directory: {output_dir}")

    try:
        # Create daemon configuration
        # Note: For single file processing, you can use minimal configuration
        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=netcdf_path.parent,
            local_product_dir=output_dir,
            state_db=Path(state_db),
            # Default fields, adjust as needed
            volume_types={strategy: {vol_num: ["WRAD", "VRAD"]}},
            radar_name=radar_name,
            product_type="geotiff",  # GeoTIFF products
            add_colmax=True,
        )

        # Create daemon instance
        logger.info("Initializing ProductGenerationDaemon...")
        daemon = ProductGenerationDaemon(config)

        # Minimal volume_info dict (contains metadata about the volume)
        # For PNG generation, only netcdf_path and basic info are needed
        volume_info = {
            "volume_id": f"{radar_name}_{strategy}_{vol_num}",
            "netcdf_path": str(netcdf_path),
            "is_complete": 1,  # Assume complete volume
            "strategy": strategy,
            "vol_nr": vol_num,
        }

        # Generate products synchronously
        logger.info("Starting product generation...")
        daemon._generate_cog_products_sync(netcdf_path, volume_info)

        logger.info(f"✓ Successfully processed {netcdf_path.name}")
        logger.info(f"✓ Products saved to {output_dir}")
        return True

    except Exception as e:
        logger.error(f"Error processing NetCDF file: {e}", exc_info=True)
        return False


def main():
    """Command-line interface for processing single NetCDF files."""
    parser = argparse.ArgumentParser(
        description="Process a single NetCDF radar file and generate PNG products",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a file and save products to the same directory
  python process_single_netcdf_example.py --netcdf /data/RMA1_0315_01_20240101T000000Z.nc

  # Specify custom output directory
  python process_single_netcdf_example.py \\
    --netcdf /data/RMA1_0315_01_20240101T000000Z.nc \\
    --output /data/products

  # Specify custom database location
  python process_single_netcdf_example.py \\
    --netcdf /data/RMA1_0315_01_20240101T000000Z.nc \\
    --output /data/products \\
    --state-db /data/state.db
        """,
    )

    parser.add_argument(
        "--netcdf",
        "-n",
        type=str,
        required=True,
        help="Path to the NetCDF file to process",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output directory for products (default: same as input file directory)",
    )

    parser.add_argument(
        "--state-db",
        "-s",
        type=str,
        default=None,
        help="Path to SQLite state database (default: output_dir/state.db)",
    )

    args = parser.parse_args()

    # Determine output directory
    output_dir = args.output
    if output_dir is None:
        output_dir = str(Path(args.netcdf).parent)

    # Process the file
    success = process_single_netcdf(
        netcdf_path=args.netcdf,
        output_dir=output_dir,
        state_db=args.state_db,
    )

    exit(0 if success else 1)


if __name__ == "__main__":
    main()
