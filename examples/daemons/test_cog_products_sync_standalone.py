"""
Standalone testing of ProductGenerationDaemon._generate_cog_products_sync() method.

This test file allows you to test the COG product generation method independently
without running the full daemon service. It uses the exact same volume_info structure
that comes from the SQLiteStateTracker database.

Usage:
    python test_cog_products_sync_standalone.py
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from radarlib.daemons import ProductGenerationDaemon, ProductGenerationDaemonConfig

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_volume_info_from_db_structure(
    volume_id: str,
    radar_name: str,
    strategy: str,
    vol_nr: str,
    observation_datetime: str,
    netcdf_path: str,
    is_complete: int = 1,
    expected_fields: str = "DBZH,DBZV,ZDR,RHOHV,PHIDP,KDP",
    downloaded_fields: str = "DBZH,DBZV,ZDR,RHOHV,PHIDP,KDP",
) -> dict:
    """
    Create a volume_info dictionary with the exact structure from the database.

    This mimics what SQLiteStateTracker.get_volumes_for_product_generation() returns.

    Args:
        volume_id: Unique volume identifier
        radar_name: Radar name (e.g., "RMA1")
        strategy: Volume strategy code (e.g., "0315")
        vol_nr: Volume number (e.g., "01")
        observation_datetime: Observation datetime in ISO format
        netcdf_path: Full path to NetCDF file
        is_complete: 1 if volume is complete, 0 if incomplete
        expected_fields: Comma-separated list of expected fields
        downloaded_fields: Comma-separated list of downloaded fields

    Returns:
        Dictionary with exact structure from volume_processing table
    """
    return {
        # From volume_processing table (all fields returned by get_volumes_for_product_generation)
        "id": 1,  # SQLite rowid
        "volume_id": volume_id,
        "radar_name": radar_name,
        "strategy": strategy,
        "vol_nr": vol_nr,
        "observation_datetime": observation_datetime,
        "status": "completed",  # Volume processing status (always "completed" when returned for product gen)
        "netcdf_path": netcdf_path,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "error_message": None,  # Will be None for completed volumes
        "is_complete": is_complete,  # 0 or 1
        "expected_fields": expected_fields,  # Comma-separated
        "downloaded_fields": downloaded_fields,  # Comma-separated
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        # From product_generation table (LEFT JOIN, may be NULL if not yet started)
        "product_status": None,  # Will be NULL if product_generation row doesn't exist
        "product_error_message": None,
        "product_error_type": None,
        # Volume number as integer (useful for some processing)
        "volume_number": int(vol_nr),
    }


def test_cog_generation_with_real_netcdf():
    """
    Test COG generation with a real NetCDF file from the data directory.

    This is the main test that uses actual radar data.
    """
    logger.info("=" * 70)
    logger.info("Test 1: COG Generation with Real NetCDF File")
    logger.info("=" * 70)

    # Configuration
    netcdf_dir = Path("data/radares/RMA2/netcdf")
    product_dir = Path("outputs/products_cog_test")
    state_db = Path("data/radares/RMA2/state.db")

    # Verify NetCDF directory exists and has files
    if not netcdf_dir.exists():
        logger.error(f"NetCDF directory not found: {netcdf_dir}")
        return False

    netcdf_files = list(netcdf_dir.glob("*.nc"))
    if not netcdf_files:
        logger.error(f"No NetCDF files found in {netcdf_dir}")
        return False

    # Use the first NetCDF file
    netcdf_file = netcdf_files[0]
    logger.info(f"Using NetCDF file: {netcdf_file.name}")

    # Extract volume information from filename
    # Expected format: RMA1_0315_01_20251208T191243Z.nc
    filename_parts = netcdf_file.stem.split("_")
    if len(filename_parts) < 4:
        logger.error(f"Unexpected NetCDF filename format: {netcdf_file.name}")
        return False

    radar_name = filename_parts[0]
    strategy = filename_parts[1]
    vol_nr = filename_parts[2]
    observation_datetime = filename_parts[3]
    volume_id = f"{radar_name}_{strategy}_{vol_nr}_{observation_datetime}"

    logger.info("Extracted volume info:")
    logger.info(f"  Radar: {radar_name}")
    logger.info(f"  Strategy: {strategy}")
    logger.info(f"  Volume #: {vol_nr}")
    logger.info(f"  DateTime: {observation_datetime}")
    geometries = {
        "0315": {
            "01": {
                "grid_resolution": 1200,
                "min_radius": 250,
                "rfactor": 0.017,
            },
            "02": {
                "grid_resolution": 1000,
                "min_radius": 400,
                "rfactor": 0.022,
            },
        },
    }
    # Create daemon configuration
    config = ProductGenerationDaemonConfig(
        local_netcdf_dir=netcdf_dir,
        local_product_dir=product_dir,
        state_db=state_db,
        volume_types={
            "0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]},
        },
        radar_name=radar_name,
        product_type="geotiff",
        add_colmax=True,  # Enable for testing
        geometry=geometries,
        # geometry={
        #     "0315": {
        #         "01": "data/radares/RMA2/geometry/RMA2_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz",
        #         "02": "data/radares/RMA2/geometry/RMA2_0315_02_RES1000_TOA12000_FAC022_MR400_geometry.npz"
        #     },
        # }  # Will try to load or build
    )

    # Create daemon instance
    try:
        daemon = ProductGenerationDaemon(config)
        logger.info("✓ Daemon initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize daemon: {e}", exc_info=True)
        return False

    # Create volume_info with exact database structure
    volume_info = create_volume_info_from_db_structure(
        volume_id=volume_id,
        radar_name=radar_name,
        strategy=strategy,
        vol_nr=vol_nr,
        observation_datetime=observation_datetime,
        netcdf_path=str(netcdf_file),
        is_complete=1,
        expected_fields="DBZH,DBZV,ZDR,RHOHV,PHIDP,KDP",
        downloaded_fields="DBZH,DBZV,ZDR,RHOHV,PHIDP,KDP",
    )

    logger.info("✓ Created volume_info structure")
    logger.info(f"  Volume ID: {volume_info['volume_id']}")
    logger.info(f"  NetCDF Path: {volume_info['netcdf_path']}")
    logger.info(f"  Is Complete: {volume_info['is_complete']}")

    # Call the method directly
    logger.info("\nCalling _generate_cog_products_sync()...")
    try:
        daemon._generate_cog_products_sync(netcdf_file, volume_info)
        logger.info("✓ COG generation completed successfully!")

        # Verify output files
        if product_dir.exists():
            cog_files = list(product_dir.rglob("*.tif"))
            logger.info(f"✓ Generated {len(cog_files)} COG files:")
            for cog_file in cog_files[:5]:  # Show first 5
                logger.info(f"  - {cog_file.relative_to(product_dir)}")
            if len(cog_files) > 5:
                logger.info(f"  ... and {len(cog_files) - 5} more files")

        return True

    except Exception as e:
        logger.error(f"✗ Failed to generate COG products: {e}", exc_info=True)
        return False


def test_cog_generation_with_mock_netcdf():
    """
    Test COG generation with mock data (for when real NetCDF is not available).

    This creates a minimal mock setup for testing the method independently.
    """
    logger.info("=" * 70)
    logger.info("Test 2: Mock Test (Shows volume_info structure)")
    logger.info("=" * 70)

    # Create volume_info with exact database structure
    volume_info = create_volume_info_from_db_structure(
        volume_id="RMA1_0315_01_20250116T120000Z",
        radar_name="RMA1",
        strategy="0315",
        vol_nr="01",
        observation_datetime="20250116T120000Z",
        netcdf_path="/data/radares/RMA1/netcdf/RMA1_0315_01_20250116T120000Z.nc",
        is_complete=1,
        expected_fields="DBZH,DBZV,ZDR,RHOHV,PHIDP,KDP",
        downloaded_fields="DBZH,DBZV,ZDR,RHOHV,PHIDP,KDP",
    )

    logger.info("✓ volume_info structure from database:")
    logger.info("")
    logger.info("  From volume_processing table:")
    for key in [
        "volume_id",
        "radar_name",
        "strategy",
        "vol_nr",
        "observation_datetime",
        "status",
        "netcdf_path",
        "is_complete",
        "expected_fields",
        "downloaded_fields",
    ]:
        logger.info(f"    {key:25} = {volume_info.get(key)}")

    logger.info("")
    logger.info("  From product_generation table (LEFT JOIN):")
    for key in ["product_status", "product_error_message", "product_error_type"]:
        logger.info(f"    {key:25} = {volume_info.get(key)}")

    logger.info("")
    logger.info("  Additional fields:")
    for key in ["id", "processed_at", "created_at", "updated_at", "volume_number"]:
        logger.info(f"    {key:25} = {volume_info.get(key)}")

    return True


def test_volume_info_structure():
    """
    Show the exact structure of volume_info as it comes from the database.
    """
    logger.info("=" * 70)
    logger.info("Test 3: Volume Info Structure Verification")
    logger.info("=" * 70)

    volume_info = create_volume_info_from_db_structure(
        volume_id="RMA1_0315_01_20250116T120000Z",
        radar_name="RMA1",
        strategy="0315",
        vol_nr="01",
        observation_datetime="20250116T120000Z",
        netcdf_path="/data/radares/RMA1/netcdf/RMA1_0315_01_20250116T120000Z.nc",
        is_complete=1,
    )

    logger.info("\nKeys available in volume_info:")
    for i, key in enumerate(sorted(volume_info.keys()), 1):
        value = volume_info[key]
        if isinstance(value, str) and len(value) > 50:
            value = value[:50] + "..."
        logger.info(f"  {i:2}. {key:30} : {value}")

    logger.info("\nKeys accessed by _generate_cog_products_sync():")
    logger.info(f"  - volume_info.get('volume_number')  = {volume_info.get('volume_number')}")

    logger.info("\nKeys accessed by _generate_product_async():")
    logger.info(f"  - volume_info['volume_id']           = {volume_info['volume_id']}")
    logger.info(f"  - volume_info.get('netcdf_path')    = {volume_info.get('netcdf_path')}")
    logger.info(f"  - volume_info.get('is_complete')    = {volume_info.get('is_complete')}")

    return True


def main():
    """Run all tests."""
    logger.info("\n")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║" + " " * 68 + "║")
    logger.info("║" + "  Standalone Testing: ProductGenerationDaemon._generate_cog_products_sync()".ljust(68) + "║")
    logger.info("║" + " " * 68 + "║")
    logger.info("╚" + "=" * 68 + "╝")
    logger.info("")

    # Test 1: Volume info structure
    test_volume_info_structure()
    logger.info("")

    # Test 2: Mock test
    test_cog_generation_with_mock_netcdf()
    logger.info("")

    # Test 3: Real test with NetCDF
    success = test_cog_generation_with_real_netcdf()

    logger.info("")
    logger.info("=" * 70)
    if success:
        logger.info("✓ All tests completed successfully!")
    else:
        logger.info("✗ Some tests failed - check logs above")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
