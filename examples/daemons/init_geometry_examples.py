"""
Example usage of the init_geometry method in ProductGenerationDaemon.

This file demonstrates all four strategies for initializing geometries:
1. Direct GridGeometry instances
2. File paths
3. Parameter dictionaries
4. Fallback building from sample radar
"""

import logging
from pathlib import Path

from radar_grid import load_geometry

from radarlib import config
from radarlib.daemons import ProductGenerationDaemon, ProductGenerationDaemonConfig

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# Strategy 1: Using Pre-loaded GridGeometry Objects
# =============================================================================


def example_with_gridgeometry_objects():
    """
    Use pre-loaded GridGeometry objects.

    Advantages:
    - Fastest (no I/O operations)
    - Geometry already in memory

    Disadvantages:
    - Requires pre-loading geometry before creating daemon
    """
    print("\n" + "=" * 70)
    print("Example 1: Using Pre-loaded GridGeometry Objects")
    print("=" * 70)

    # Pre-load geometries
    geometry_path = Path(config.ROOT_RADAR_FILES_PATH) / "RMA1" / "geometry"
    geom_01 = load_geometry(str(geometry_path / "/RMA1_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz"))
    geom_02 = load_geometry(str(geometry_path / "RMA1_0315_02_RES1000_TOA12000_FAC022_MR400_geometry.npz"))

    config_ = ProductGenerationDaemonConfig(
        local_netcdf_dir=Path("data/radares/RMA1/netcdf"),
        local_product_dir=Path("outputs/products"),
        state_db=Path("data/radares/RMA1/state.db"),
        volume_types={"0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}},
        radar_name="RMA1",
        geometry={"0315": {"01": geom_01, "02": geom_02}},
    )

    daemon = ProductGenerationDaemon(config_)
    print("✓ Daemon initialized with GridGeometry objects")
    print(f"  Geometry available: {daemon.geometry is not None}")


# =============================================================================
# Strategy 2: Using File Paths
# =============================================================================


def example_with_file_paths():
    """
    Use file paths to geometry files.

    Advantages:
    - Simple and clear
    - Files loaded on-demand

    Disadvantages:
    - Requires files to exist at specified paths
    """
    print("\n" + "=" * 70)
    print("Example 2: Using File Paths")
    print("=" * 70)

    geometry_path = Path(config.ROOT_RADAR_FILES_PATH) / "RMA1" / "geometry"

    config_ = ProductGenerationDaemonConfig(
        local_netcdf_dir=Path("data/radares/RMA1/netcdf"),
        local_product_dir=Path("outputs/products"),
        state_db=Path("data/radares/RMA1/state.db"),
        volume_types={"0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}},
        radar_name="RMA1",
        geometry={
            "0315": {
                "01": str(geometry_path / "RMA1_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz"),
                "02": str(geometry_path / "RMA1_0315_02_RES1000_TOA12000_FAC022_MR400_geometry.npz"),
            }
        },
    )

    daemon = ProductGenerationDaemon(config_)
    print("✓ Daemon initialized with file paths")
    print(f"  Geometry available: {daemon.geometry is not None}")


# =============================================================================
# Strategy 3: Using Parameter Dictionaries
# =============================================================================


def example_with_parameters():
    """
    Use parameter dictionaries to identify or build geometries.

    The method will:
    1. First try to find pre-built geometry files matching the parameters
    2. If not found, build geometry from a sample radar

    Advantages:
    - Flexible
    - Auto-builds if files don't exist

    Disadvantages:
    - Requires NetCDF files for fallback building
    - Takes longer if building new geometry
    """
    print("\n" + "=" * 70)
    print("Example 3: Using Parameter Dictionaries")
    print("=" * 70)

    config_ = ProductGenerationDaemonConfig(
        local_netcdf_dir=Path("data/radares/RMA1/netcdf"),
        local_product_dir=Path("outputs/products"),
        state_db=Path("data/radares/RMA1/state.db"),
        volume_types={"0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}},
        radar_name="RMA1",
        geometry={
            "0315": {
                "01": {"grid_resolution": 1500, "min_radius": 250.0, "toa": 12000.0, "rfactor": 0.017},
                "02": {"grid_resolution": 1000, "min_radius": 400.0, "toa": 12000.0, "rfactor": 0.022},
            }
        },
    )

    daemon = ProductGenerationDaemon(config_)
    print("✓ Daemon initialized with parameter dictionaries")
    print(f"  Geometry available: {daemon.geometry is not None}")
    print("  Parameters:")
    print("    Vol 01: 1500m resolution, 250m min_radius, 12000m TOA, 0.017 beam_factor")
    print("    Vol 02: 1000m resolution, 400m min_radius, 12000m TOA, 0.022 beam_factor")


# =============================================================================
# Strategy 4: Empty Parameters (Use Defaults)
# =============================================================================


def example_with_default_parameters():
    """
    Use empty dictionaries to trigger default parameters and auto-building.

    Defaults:
    - grid_resolution: 1500 m
    - min_radius: 250.0 m
    - toa: 12000.0 m
    - rfactor: 0.017

    Advantages:
    - Simplest configuration
    - Works with just NetCDF files available

    Disadvantages:
    - Uses default parameters for all geometries
    - Slowest (builds from scratch)
    """
    print("\n" + "=" * 70)
    print("Example 4: Using Default Parameters (Auto-build)")
    print("=" * 70)

    config = ProductGenerationDaemonConfig(
        local_netcdf_dir=Path("data/radares/RMA1/netcdf"),
        local_product_dir=Path("outputs/products"),
        state_db=Path("data/radares/RMA1/state.db"),
        volume_types={"0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}},
        radar_name="RMA1",
        geometry={"0315": {"01": {}, "02": {}}},  # Empty dict - uses defaults  # Empty dict - uses defaults
    )

    daemon = ProductGenerationDaemon(config)
    print("✓ Daemon initialized with default parameters")
    print(f"  Geometry available: {daemon.geometry is not None}")
    print("  Will use defaults if pre-built files not found:")
    print("    - grid_resolution: 1500m")
    print("    - min_radius: 250.0m")
    print("    - toa: 12000.0m")
    print("    - rfactor: 0.017")


# =============================================================================
# Strategy 5: Mixed Approach
# =============================================================================


def example_with_mixed_approach():
    """
    Mix different strategies for different volumes.

    This is the most realistic scenario where different volumes might have
    different requirements.

    Advantages:
    - Most flexible
    - Optimal for mixed configurations

    Disadvantages:
    - More complex configuration
    """
    print("\n" + "=" * 70)
    print("Example 5: Mixed Approach (Different Strategies)")
    print("=" * 70)

    # Pre-load one geometry
    geometry_path = Path(config.ROOT_RADAR_FILES_PATH) / "RMA1" / "geometry"
    geom_01 = load_geometry(str(geometry_path / "RMA1_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz"))

    config_ = ProductGenerationDaemonConfig(
        local_netcdf_dir=Path("data/radares/RMA1/netcdf"),
        local_product_dir=Path("outputs/products"),
        state_db=Path("data/radares/RMA1/state.db"),
        volume_types={"0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}},
        radar_name="RMA1",
        geometry={
            "0315": {
                "01": geom_01,  # Pre-loaded GridGeometry object
                "02": str(geometry_path / "RMA1_0315_02_RES1000_TOA12000_FAC022_MR400_geometry.npz"),  # File path
                # Could also add 03 with parameters if more volumes existed
            }
        },
    )

    daemon = ProductGenerationDaemon(config_)
    print("✓ Daemon initialized with mixed strategies:")
    print("  Vol 01: Pre-loaded GridGeometry object (Strategy 1)")
    print("  Vol 02: File path (Strategy 2)")
    print(f"  Geometry available: {daemon.geometry is not None}")


# =============================================================================
# Strategy 6: Multiple Radar Strategies
# =============================================================================


def example_with_multiple_strategies():
    """
    Handle multiple volume strategies (e.g., 0315, 0200, etc.)

    Each strategy can use different approaches.
    """
    print("\n" + "=" * 70)
    print("Example 6: Multiple Radar Strategies")
    print("=" * 70)

    config = ProductGenerationDaemonConfig(
        local_netcdf_dir=Path("data/radares/RMA1/netcdf"),
        local_product_dir=Path("outputs/products"),
        state_db=Path("data/radares/RMA1/state.db"),
        volume_types={
            "0315": {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]},
            "0200": {"01": ["DBZH", "DBZV", "ZDR"]},
        },
        radar_name="RMA1",
        geometry={
            "0315": {
                "01": "data/geometry/RMA1/RMA1_0315_01_RES1500_TOA12000_FAC017_MR250_geometry.npz",
                "02": {"grid_resolution": 1000, "min_radius": 400.0, "toa": 12000.0, "rfactor": 0.022},
            },
            "0200": {"01": {}},  # Use defaults if needed
        },
    )

    daemon = ProductGenerationDaemon(config)
    print("✓ Daemon initialized with multiple strategies:")
    print("  Strategy 0315:")
    print("    Vol 01: File path")
    print("    Vol 02: Parameters")
    print("  Strategy 0200:")
    print("    Vol 01: Defaults")
    print(f"  Geometry available: {daemon.geometry is not None}")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("ProductDaemon init_geometry() - Usage Examples")
    print("=" * 70)

    # # Uncomment to run examples (requires actual data files):
    # example_with_gridgeometry_objects()
    # example_with_file_paths()
    # example_with_parameters()
    # example_with_default_parameters()
    example_with_mixed_approach()
    # example_with_multiple_strategies()
