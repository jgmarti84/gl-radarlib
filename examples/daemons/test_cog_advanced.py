"""
Advanced test script for _generate_cog_products_sync with debugging options.

This script provides more detailed testing and debugging capabilities:
- Multiple test files
- Detailed logging and profiling
- Error handling and recovery
- Performance monitoring
- Result verification

Usage:
    python3 test_cog_advanced.py --file /path/to/netcdf.nc
    python3 test_cog_advanced.py --verbose
    python3 test_cog_advanced.py --profile
"""

import argparse
import logging
import sys
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional

from radarlib.daemons import ProductGenerationDaemon, ProductGenerationDaemonConfig

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class COGTestRunner:
    """Advanced test runner for COG product generation."""

    def __init__(self, verbose: bool = False, profile: bool = False):
        """
        Initialize test runner.

        Args:
            verbose: Enable verbose logging
            profile: Enable performance profiling
        """
        self.verbose = verbose
        self.profile = profile
        self.results = {"success": False, "files_processed": 0, "files_failed": 0, "execution_time": 0, "errors": []}

    def setup_configuration(
        self, netcdf_dir: Optional[Path] = None, product_dir: Optional[Path] = None, geometry_dir: Optional[Path] = None
    ) -> ProductGenerationDaemonConfig:
        """
        Create daemon configuration with optional custom paths.

        Args:
            netcdf_dir: Custom NetCDF directory
            product_dir: Custom product output directory
            geometry_dir: Custom geometry directory

        Returns:
            ProductGenerationDaemonConfig instance
        """
        # Default paths
        netcdf_dir = netcdf_dir or Path("data/radares/RMA1/netcdf")
        product_dir = product_dir or Path("outputs/cog_test_advanced")

        # Volume types
        volume_types = {
            "0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]},
            "0200": {"01": ["DBZH", "DBZV", "ZDR"]},
        }

        # Optional geometry
        geometry = None
        if geometry_dir and geometry_dir.exists():
            try:

                geom_files = list(geometry_dir.glob("*.npz"))
                if geom_files:
                    logger.info(f"Found {len(geom_files)} geometry files")
                    # Load first one as example
                    geometry = {
                        "0315": {
                            "01": str(geom_files[0]),
                            "02": str(geom_files[1]) if len(geom_files) > 1 else str(geom_files[0]),
                        }
                    }
            except Exception as e:
                logger.warning(f"Failed to load geometry: {e}")

        config_obj = ProductGenerationDaemonConfig(
            local_netcdf_dir=netcdf_dir,
            local_product_dir=product_dir,
            state_db=Path("data/radares/RMA1/state.db"),
            volume_types=volume_types,
            radar_name="RMA1",
            product_type="geotiff",
            add_colmax=True,
            geometry=geometry,
        )

        if self.verbose:
            logger.info("Configuration created:")
            logger.info(f"  NetCDF dir: {config_obj.local_netcdf_dir}")
            logger.info(f"  Product dir: {config_obj.local_product_dir}")
            logger.info(f"  Geometry: {'Provided' if geometry else 'None'}")

        return config_obj

    def find_netcdf_files(self, netcdf_dir: Path, max_files: Optional[int] = None) -> List[Path]:
        """
        Find NetCDF files to test.

        Args:
            netcdf_dir: Directory to search
            max_files: Maximum number of files to return

        Returns:
            List of NetCDF file paths
        """
        nc_files = sorted(list(netcdf_dir.glob("*.nc")))

        if not nc_files:
            raise FileNotFoundError(f"No NetCDF files found in {netcdf_dir}")

        if max_files:
            nc_files = nc_files[:max_files]

        if self.verbose:
            logger.info(f"Found {len(nc_files)} NetCDF files")
            for f in nc_files[:5]:
                logger.info(f"  - {f.name}")
            if len(nc_files) > 5:
                logger.info(f"  ... and {len(nc_files) - 5} more")

        return nc_files

    def parse_volume_info(self, netcdf_path: Path) -> Dict:
        """
        Parse volume information from NetCDF filename.

        Args:
            netcdf_path: Path to NetCDF file

        Returns:
            Dictionary with volume information
        """
        filename = netcdf_path.stem
        parts = filename.split("_")

        if len(parts) < 3:
            raise ValueError(f"Invalid filename format: {filename}")

        return {
            "radar_name": parts[0],
            "strategy": parts[1],
            "volume_number": parts[2],
            "datetime": "_".join(parts[3:]) if len(parts) > 3 else "unknown",
            "filename": netcdf_path.name,
            "filepath": str(netcdf_path),
        }

    def test_single_file(self, daemon: ProductGenerationDaemon, netcdf_path: Path) -> bool:
        """
        Test COG generation for a single file.

        Args:
            daemon: ProductGenerationDaemon instance
            netcdf_path: Path to NetCDF file

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"\nProcessing: {netcdf_path.name}")

            # Parse volume info
            volume_info = self.parse_volume_info(netcdf_path)

            # Timing
            start_time = time.time() if self.profile else None

            # Generate COG products
            daemon._generate_cog_products_sync(netcdf_path, volume_info)

            if self.profile:
                elapsed = time.time() - start_time
                logger.info(f"  Execution time: {elapsed:.2f} seconds")

            logger.info("  ✓ Success")
            self.results["files_processed"] += 1
            return True

        except Exception as e:
            logger.error(f"  ✗ Failed: {e}")
            if self.verbose:
                logger.error(traceback.format_exc())
            self.results["files_failed"] += 1
            self.results["errors"].append({"file": netcdf_path.name, "error": str(e)})
            return False

    def test_multiple_files(self, daemon: ProductGenerationDaemon, netcdf_files: List[Path]) -> bool:
        """
        Test COG generation for multiple files.

        Args:
            daemon: ProductGenerationDaemon instance
            netcdf_files: List of NetCDF file paths

        Returns:
            True if at least one succeeded, False if all failed
        """
        logger.info("=" * 70)
        logger.info(f"Testing {len(netcdf_files)} NetCDF files")
        logger.info("=" * 70)

        start_time = time.time() if self.profile else None

        for netcdf_path in netcdf_files:
            self.test_single_file(daemon, netcdf_path)

        if self.profile:
            total_time = time.time() - start_time
            logger.info(f"\nTotal execution time: {total_time:.2f} seconds")
            avg_time = total_time / len(netcdf_files)
            logger.info(f"Average per file: {avg_time:.2f} seconds")

        self.results["execution_time"] = time.time() - start_time if self.profile else 0
        self.results["success"] = self.results["files_processed"] > 0

        return self.results["success"]

    def verify_outputs(self, output_dir: Path) -> int:
        """
        Verify generated output files.

        Args:
            output_dir: Output directory to check

        Returns:
            Number of generated files
        """
        if not output_dir.exists():
            logger.warning(f"Output directory does not exist: {output_dir}")
            return 0

        tif_files = list(output_dir.rglob("*.tif"))

        logger.info(f"\n✓ Generated {len(tif_files)} GeoTIFF files:")

        # Group by directory
        by_dir = {}
        for f in tif_files:
            dir_name = f.parent.name
            by_dir.setdefault(dir_name, []).append(f)

        for dir_name in sorted(by_dir.keys()):
            files = by_dir[dir_name]
            logger.info(f"  {dir_name}: {len(files)} files")
            for f in files[:3]:
                logger.info(f"    - {f.name}")
            if len(files) > 3:
                logger.info(f"    ... and {len(files) - 3} more")

        return len(tif_files)

    def print_summary(self):
        """Print test summary."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Files processed: {self.results['files_processed']}")
        logger.info(f"Files failed: {self.results['files_failed']}")
        logger.info(f"Success: {'YES' if self.results['success'] else 'NO'}")

        if self.results["errors"]:
            logger.info("\nErrors:")
            for error in self.results["errors"]:
                logger.info(f"  - {error['file']}: {error['error']}")

        if self.profile and self.results["execution_time"] > 0:
            logger.info(f"Execution time: {self.results['execution_time']:.2f}s")

        logger.info("=" * 70 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Advanced test for _generate_cog_products_sync")
    parser.add_argument("--file", type=Path, help="Specific NetCDF file to test")
    parser.add_argument("--dir", type=Path, help="NetCDF directory (overrides default)")
    parser.add_argument("--output", type=Path, help="Output directory (overrides default)")
    parser.add_argument("--max-files", type=int, help="Maximum number of files to test")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--profile", action="store_true", help="Enable performance profiling")

    args = parser.parse_args()

    # Create test runner
    runner = COGTestRunner(verbose=args.verbose, profile=args.profile)

    try:
        # Setup
        logger.info("Setting up configuration...")
        test_config = runner.setup_configuration(netcdf_dir=args.dir, product_dir=args.output)

        # Create daemon
        logger.info("Creating daemon...")
        daemon = ProductGenerationDaemon(test_config)

        # Get files to test
        if args.file:
            netcdf_files = [args.file]
        else:
            netcdf_files = runner.find_netcdf_files(test_config.local_netcdf_dir, max_files=args.max_files)

        # Run tests
        runner.test_multiple_files(daemon, netcdf_files)

        # Verify outputs
        logger.info("Verifying generated files...")
        runner.verify_outputs(test_config.local_product_dir)

        # Print summary
        runner.print_summary()

        return 0 if runner.results["success"] else 1

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
