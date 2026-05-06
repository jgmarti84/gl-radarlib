"""
detect_cores_from_netcdf.py — Detect convective cores from a NetCDF radar volume.

This script loads a pre-processed NetCDF radar volume, computes the COLMAX
reflectivity grid using the precomputed grid geometry, and then detects
convective core centroids with :func:`radarlib.radar_grid.detect_cores_from_colmax`.

The processing pipeline mirrors the sequence used inside
:meth:`ProductGenerationDaemon._generate_raw_cog_products_sync` so the COLMAX
grid produced here is numerically identical to the one stored by the daemon.

Usage
-----
python examples/detect_cores_from_netcdf.py \\
    --netcdf /data/radares/RMA1/netcdf/RMA1_0315_01_20260417T160000Z.nc \\
    --radar-name RMA1 \\
    --strategy 0315 \\
    --vol-nr 01 \\
    --plot
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path

import numpy as np

from radarlib import config

# ---------------------------------------------------------------------------
# Make sure the package root is importable when run directly from the repo
# ---------------------------------------------------------------------------
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root / "src"))

from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf  # noqa: E402
from radarlib.radar_grid import (  # noqa: E402
    apply_geometry,
    column_max,
    compute_grid_geometry,
    detect_cores_from_colmax,
    get_gate_coordinates,
    get_radar_info,
    load_geometry,
    save_geometry,
)
from radarlib.radar_grid.geometry import build_geometry_filename  # noqa: E402
from radarlib.radar_grid.utils import calculate_grid_points, get_field_data, safe_range_max_m  # noqa: E402
from radarlib.utils.fields_utils import determine_reflectivity_fields  # noqa: E402
from radarlib.utils.names_utils import extract_netcdf_filename_components  # noqa: E402

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    filename = "RMA1_0315_01_RES1000x600_TOA15000_HF1p0000_MR900_MN1_NB1p30_BSP1p10_nearest_geometry.npz"
    p = argparse.ArgumentParser(
        description="Detect convective cores from a radar NetCDF volume.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--netcdf",
        type=str,
        default="tests/data/netcdf/RMA1_0315_01_20260428T145503Z.nc",
        help="Path to NetCDF radar volume file.",
    )
    p.add_argument(
        "--geometry",
        type=str,
        default=f"tests/data/geometry/{filename}",
        help="Path to precomputed geometry file. If not found, will be created from the NetCDF data.",
    )
    # p.add_argument(
    #     "--radar-name",
    #     type=str,
    #     default="UNKNOWN",
    #     help="Radar name for metadata (used when creating geometry from data).",
    # )
    # p.add_argument(
    #     "--strategy",
    #     type=str,
    #     default="UNKNOWN",
    #     help="Strategy code for metadata (used when creating geometry from data).",
    # )
    # p.add_argument(
    #     "--vol-nr",
    #     type=str,
    #     default="UNKNOWN",
    #     help="Volume number for metadata (used when creating geometry from data).",
    # )
    p.add_argument(
        "--min-dbz",
        type=float,
        default=None,
        help=(
            "Reflectivity threshold for core detection (dBZ). "
            f"Defaults to config.CORES_MIN_Z ({config.CORES_MIN_Z})."
        ),
    )
    p.add_argument(
        "--min-pixels",
        type=int,
        default=2,
        help=(
            "Minimum blob size in pixels for quality gate (default: 2). "
            "Must be exceeded: pixel_count > min_pixels. Use 1 to accept 2-pixel blobs."
        ),
    )
    p.add_argument(
        "--min-dbz-updraft",
        type=float,
        default=None,
        help=(
            "Reflectivity threshold for violent updraft gate (dBZ). "
            f"Defaults to config.CORES_MIN_Z_UPDRAFT ({config.CORES_MIN_Z_UPDRAFT}). "
            "Blobs with max_dbz > this AND sufficient pixels can pass without RhoHV."
        ),
    )
    p.add_argument(
        "--rhohv-threshold",
        type=float,
        default=0.85,
        help=(
            "RhoHV quality gate threshold (0-1, default: 0.85). "
            "Blobs with mean RhoHV > this can pass the meteorological gate."
        ),
    )
    p.add_argument(
        "--no-rhohv-quality-gate",
        action="store_true",
        default=False,
        help=(
            "Disable RhoHV quality gating entirely. " "Only updraft intensity gate applies. Useful for weak convection."
        ),
    )
    p.add_argument(
        "--plot",
        action="store_true",
        default=False,
        help="If set, save a matplotlib figure of the COLMAX grid with core centroids.",
    )
    p.add_argument(
        "--plot-output-dir",
        type=str,
        default="tests/data/outputs/",
        help="Directory where to save plot PNG files (default: tests/data/outputs/).",
    )
    return p


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

# def _load_geometry(geometry_path):
#     """
#     Load the precomputed GridGeometry for the given radar/strategy/volume.

#     The geometry file is located using the same naming convention as the
#     daemon: :func:`~radarlib.radar_grid.geometry.build_geometry_filename`
#     followed by ``.npz``, stored under ``config.ROOT_GEOMETRY_PATH``.

#     Parameters
#     ----------
#     radar_name : str
#         Radar station identifier (e.g. ``"RMA1"``).
#     strategy : str
#         Strategy key (e.g. ``"0315"``).
#     vol_nr : str
#         Volume number string (e.g. ``"01"``).

#     Returns
#     -------
#     GridGeometry
#         The loaded geometry object.

#     Raises
#     ------
#     FileNotFoundError
#         If no matching ``.npz`` file is found under
#         ``config.ROOT_GEOMETRY_PATH``.
#     """
# Build the canonical filename stem using the same helper as the daemon
# metadata = {
#     "radar_name": radar_name,
#     "strategy": strategy,
#     "volume_nr": vol_nr,
#     "grid_resolution_xy": config.GEOMETRY_RES_XY,
#     "grid_resolution_z": config.GEOMETRY_RES_Z,
#     "toa": config.GEOMETRY_TOA,
#     "h_factor": config.GEOMETRY_HFAC,
#     "min_radius": config.GEOMETRY_MIN_RADIUS,
#     "max_neighbors": config.MAX_NEIGHBORS,
#     "nb": config.GEOMETRY_NB,
#     "bsp": config.GEOMETRY_BSP,
#     "weighting": config.WEIGHT_FUNCTION,
# }
# filename_stem = build_geometry_filename(metadata)
# geometry_path = os.path.join(config.ROOT_GEOMETRY_PATH, f"{filename_stem}.npz")

# if not Path(geometry_path).exists():
#     raise FileNotFoundError(
#         f"Geometry file not found: {geometry_path}\n"
#         f"Run the daemon at least once so the geometry is built, or use "
#         f"examples/daemons/init_geometry_examples.py to pre-build it."
#     )

# logger.info("Loading geometry from %s", geometry_path)
# return load_geometry(geometry_path)


def _extract_xy_coords(geometry) -> tuple:
    """
    Derive 2D Cartesian x/y coordinate grids from a GridGeometry object.

    The geometry stores the grid extents in ``grid_limits`` (z, y, x) and the
    grid dimensions in ``grid_shape`` (nz, ny, nx).  A regular 1D linspace is
    built for each axis and then expanded into a 2D meshgrid of shape
    ``(ny, nx)`` using ``indexing="ij"``.

    Parameters
    ----------
    geometry : GridGeometry

    Returns
    -------
    tuple of np.ndarray
        ``(xx, yy)`` each with shape ``(ny, nx)``.
    """
    _nz, ny, nx = geometry.grid_shape
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    x_1d = np.linspace(x_min, x_max, nx, dtype=np.float32)
    y_1d = np.linspace(y_min, y_max, ny, dtype=np.float32)

    yy, xx = np.meshgrid(y_1d, x_1d, indexing="ij")  # shape (ny, nx)
    return xx, yy


def _print_report(cores: list, radar_name: str) -> None:
    """Print a human-readable detection report to stdout."""
    print(f"\nCores detected: {len(cores)}\n")
    if not cores:
        print("  (no convective cores found)\n")
        return

    for i, core in enumerate(cores, start=1):
        x_km = core["x_m"] / 1000.0
        y_km = core["y_m"] / 1000.0
        range_km = core["range_m"] / 1000.0
        print(f"Core {i}:")
        print(f"  x = {x_km:>10,.1f} km   y = {y_km:>10,.1f} km")
        print(f"  range = {range_km:>6.1f} km")
        print(f"  mean dBZ = {core['mean_dbz']:>5.1f}   max dBZ = {core['max_dbz']:>5.1f}")
        print(f"  pixels = {core['pixel_count']}")
        print()


def _plot_colmax_with_cores(
    colmax: np.ndarray,
    xx: np.ndarray,
    yy: np.ndarray,
    cores: list,
    radar_name: str,
    timestamp: str,
    strategy: str,
    vol_nr: str,
    output_dir: str = "tests/data/outputs/",
) -> None:
    """
    Save the COLMAX grid with detected core centroids overlaid to a PNG file.

    Parameters
    ----------
    colmax : np.ndarray
        2D COLMAX grid (ny, nx).
    xx, yy : np.ndarray
        2D coordinate grids in metres.
    cores : list of dict
        Detected core dicts from :func:`detect_cores_from_colmax`.
    radar_name : str
        Radar identifier for the figure title and filename.
    timestamp : str
        Volume timestamp string for the figure title and filename.
    strategy : str
        Strategy code for the filename.
    vol_nr : str
        Volume number for the filename.
    output_dir : str
        Directory where to save the PNG file (default: tests/data/outputs/).
    """
    import matplotlib.pyplot as plt

    # Try the project colourmap; fall back gracefully
    try:
        import radarlib.visualization  # noqa: F401 — registers custom cmaps

        cmap_name = "grc_th"
    except Exception:
        cmap_name = "NWSRef"

    fig, ax = plt.subplots(figsize=(9, 8))

    x_km = xx / 1000.0
    y_km = yy / 1000.0

    pcm = ax.pcolormesh(
        x_km,
        y_km,
        np.ma.masked_invalid(colmax),
        cmap=cmap_name,
        vmin=-10.0,
        vmax=65.0,
        shading="auto",
    )

    cbar = fig.colorbar(pcm, ax=ax, pad=0.02)
    cbar.set_label("COLMAX reflectivity (dBZ)", fontsize=11)

    for core in cores:
        cx_km = core["x_m"] / 1000.0
        cy_km = core["y_m"] / 1000.0
        ax.plot(
            cx_km,
            cy_km,
            marker="o",
            markersize=8,
            markerfacecolor="red",
            markeredgecolor="black",
            markeredgewidth=0.5,
            zorder=5,
        )
        ax.annotate(
            f"{core['mean_dbz']:.1f}",
            xy=(cx_km, cy_km),
            xytext=(4, 4),
            textcoords="offset points",
            color="black",
            fontsize=9,
            fontweight="bold",
            zorder=6,
        )

    ax.set_xlabel("Range East (km)", fontsize=11)
    ax.set_ylabel("Range North (km)", fontsize=11)
    ax.set_title(
        f"{radar_name} — COLMAX with convective cores\n{timestamp}",
        fontsize=12,
    )
    ax.set_aspect("equal")
    ax.grid(color="gray", linestyle="--", linewidth=0.4, alpha=0.5)

    plt.tight_layout()

    # Build output filename based on radar, strategy, volume, and timestamp
    # Format: RADAR_STRATEGY_VOLUME_TIMESTAMP_cores.png
    timestamp_for_file = timestamp.replace(":", "").replace("-", "").replace("T", "t").replace("Z", "")
    filename = f"{radar_name}_{strategy}_{vol_nr}_{timestamp_for_file}_cores.png"

    # Create output directory if it doesn't exist
    import os

    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, filename)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    logging.getLogger(__name__).info(f"Saved plot to {output_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entry point for the convective core detection example."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _build_parser().parse_args()
    netcdf_path = Path(args.netcdf)
    netcdf_filename = netcdf_path.name

    if not netcdf_path.exists():
        print(f"ERROR: NetCDF file not found: {netcdf_path}", file=sys.stderr)
        sys.exit(1)

    min_dbz = args.min_dbz if args.min_dbz is not None else config.CORES_MIN_Z

    # ------------------------------------------------------------------
    # Step 1 — Load and standardise the radar volume
    # ------------------------------------------------------------------
    logger.info("Loading radar volume: %s", netcdf_path)
    radar = read_radar_netcdf(str(netcdf_path))
    radar = estandarizar_campos_RMA(radar)

    fields = determine_reflectivity_fields(radar)
    hrefl_field = fields["hrefl_field"]
    logger.info("Horizontal reflectivity field: %s", hrefl_field)

    # Derive a timestamp string for display (from the radar object if possible)
    filename_components = extract_netcdf_filename_components(netcdf_filename)
    timestamp = filename_components.get("timestamp", netcdf_path.stem.split("_")[-1])
    radar_name = filename_components.get("radar_name", "UNKNOWN")
    strategy = filename_components.get("strategy", "UNKNOWN")
    vol_nr = filename_components.get("vol_nr", "UNKNOWN")
    # # Use command line metadata if provided, otherwise extract from file
    # if args.radar_name == "UNKNOWN":
    #     args.radar_name = filename_components.get("radar_name", "UNKNOWN")
    # if args.strategy == "UNKNOWN":
    #     args.strategy = filename_components.get("strategy", "UNKNOWN")
    # if args.vol_nr == "UNKNOWN":
    #     args.vol_nr = filename_components.get("volume_nr", "UNKNOWN")

    # ------------------------------------------------------------------
    # Step 2 — Load the precomputed grid geometry
    # ------------------------------------------------------------------
    try:
        geometry = load_geometry(args.geometry)
    except Exception as exc:
        logger.warning(f"Failed to load geometry from {args.geometry}: {exc}")
        logger.info("Attempting to create geometry from loaded radar data...")
        try:
            # Extract gate coordinates from the already-loaded radar
            gate_x, gate_y, gate_z = get_gate_coordinates(radar)
            logger.info(f"Extracted gate coordinates: {len(gate_x):,} gates")

            # Get radar info for metadata
            radar_info = get_radar_info(radar)

            # Define grid limits based on radar extents
            range_max_m = safe_range_max_m(radar, round_to_km=20)
            z_grid_limits = (0.0, config.GEOMETRY_TOA)
            y_grid_limits = (-range_max_m, range_max_m)
            x_grid_limits = (-range_max_m, range_max_m)

            # Calculate grid dimensions
            z_points, y_points, x_points = calculate_grid_points(
                z_grid_limits, y_grid_limits, x_grid_limits, config.GEOMETRY_RES_XY, config.GEOMETRY_RES_Z
            )
            grid_shape = (z_points, y_points, x_points)
            grid_limits = (z_grid_limits, y_grid_limits, x_grid_limits)

            logger.info(f"Computing geometry with grid shape {grid_shape}...")
            # Compute geometry from gate coordinates
            with tempfile.TemporaryDirectory(prefix="radarlib_geometry_") as temp_dir:
                geometry = compute_grid_geometry(
                    gate_x,
                    gate_y,
                    gate_z,
                    grid_shape,
                    grid_limits,
                    temp_dir=temp_dir,
                    toa=config.GEOMETRY_TOA,
                    min_radius=config.GEOMETRY_MIN_RADIUS,
                    radar_altitude=radar_info.get("altitude", 0.0),
                    h_factor=config.GEOMETRY_HFAC,
                    nb=config.GEOMETRY_NB,
                    bsp=config.GEOMETRY_BSP,
                    weighting=config.WEIGHT_FUNCTION,
                    max_neighbors=config.MAX_NEIGHBORS,
                    n_workers=4,
                )

            # Attach metadata
            geometry.metadata = {
                "radar_name": radar_name,
                "strategy": strategy,
                "volume_nr": vol_nr,
                "grid_resolution_xy": config.GEOMETRY_RES_XY,
                "grid_resolution_z": config.GEOMETRY_RES_Z,
                "toa": config.GEOMETRY_TOA,
                "h_factor": config.GEOMETRY_HFAC,
                "min_radius": config.GEOMETRY_MIN_RADIUS,
                "max_neighbors": config.MAX_NEIGHBORS,
                "nb": config.GEOMETRY_NB,
                "bsp": config.GEOMETRY_BSP,
                "weighting": config.WEIGHT_FUNCTION,
            }
            logger.info(f"Successfully created geometry: {geometry.memory_usage_mb():.1f} MB in memory")

            # Derive the canonical filename from the build parameters
            file_name = build_geometry_filename(geometry.metadata)
            file_name = f"{file_name}.npz"
            file_path = os.path.join("tests/data/geometry/", file_name)
            save_geometry(geometry, file_path)
            logger.info(f"Saved geometry to {file_path}")
        except Exception as build_exc:
            print(
                f"ERROR: Could not load or create geometry:\n"
                f"  Original error: {exc}\n"
                f"  Fallback error: {build_exc}",
                file=sys.stderr,
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # Step 3 — Compute the COLMAX grid
    # ------------------------------------------------------------------
    logger.info("Computing COLMAX from field '%s'", hrefl_field)
    field_data = get_field_data(radar, hrefl_field)
    colmax_data = apply_geometry(geometry, field_data)
    colmax = column_max(colmax_data, geometry=geometry)
    logger.info("COLMAX grid shape: %s", colmax.shape)

    # Print COLMAX statistics for debugging
    valid_colmax = colmax[~np.isnan(colmax)]
    if len(valid_colmax) > 0:
        logger.info("COLMAX statistics (valid values only):")
        logger.info(f"  Min: {np.min(valid_colmax):.2f} dBZ")
        logger.info(f"  Max: {np.max(valid_colmax):.2f} dBZ")
        logger.info(f"  Mean: {np.mean(valid_colmax):.2f} dBZ")
        logger.info(f"  Median: {np.median(valid_colmax):.2f} dBZ")
        logger.info(f"  Std: {np.std(valid_colmax):.2f} dBZ")
        logger.info(f"  Total valid pixels: {len(valid_colmax):,}")
        logger.info(f"  Pixels > {min_dbz} dBZ: {np.sum(valid_colmax > min_dbz):,}")
    else:
        logger.warning("COLMAX grid contains only NaN values!")

    # Extract RhoHV grid from the lowest sweep to enable meteorological quality gate
    rhohv = None
    rhv_field = None
    try:
        # Try RHOHV field first
        if "RHOHV" in radar.fields:
            rhv_field = "RHOHV"
            logger.info("Found RHOHV field for quality gating")
        elif "RhoHV" in radar.fields:
            rhv_field = "RhoHV"
            logger.info("Found RhoHV field for quality gating")

        if rhv_field is not None:
            # Get the lowest sweep data
            rhohv_field_data = get_field_data(radar, rhv_field)
            # Apply geometry mapping to RhoHV
            rhohv_data = apply_geometry(geometry, rhohv_field_data)
            # Extract a representative slice (e.g., at lowest elevation or median)
            # For simplicity, take the column maximum of RhoHV values
            rhohv = np.max(rhohv_data, axis=0)  # shape (ny, nx)
            logger.info(f"Extracted RhoHV grid for quality gating. Shape: {rhohv.shape}")
        else:
            logger.warning("No RhoHV field found. Core quality gate will rely only on updraft intensity.")
    except Exception as e:
        logger.warning(f"Could not extract RhoHV: {e}. Proceeding without RhoHV quality gate.")

    # ------------------------------------------------------------------
    # Step 4 — Extract x/y coordinate grids from the geometry object
    # ------------------------------------------------------------------
    xx, yy = _extract_xy_coords(geometry)

    # ------------------------------------------------------------------
    # Step 5 — Detect convective cores
    # ------------------------------------------------------------------
    logger.info("Running core detection with min_dbz=%.1f dBZ", min_dbz)

    # Use custom parameters from CLI, or defaults from config
    min_dbz_updraft = args.min_dbz_updraft if args.min_dbz_updraft is not None else config.CORES_MIN_Z_UPDRAFT
    rhohv_for_detection = None if args.no_rhohv_quality_gate else rhohv

    logger.info("Core detection parameters:")
    logger.info(f"  min_dbz: {min_dbz:.1f} dBZ")
    logger.info(f"  min_pixels: {args.min_pixels}")
    logger.info(f"  min_dbz_updraft: {min_dbz_updraft:.1f} dBZ")
    logger.info(f"  rhohv_threshold: {args.rhohv_threshold}")
    logger.info(f"  rhohv_quality_gate_enabled: {not args.no_rhohv_quality_gate}")

    cores = detect_cores_from_colmax(
        colmax=colmax,
        x_coords=xx,
        y_coords=yy,
        rhohv=rhohv_for_detection,
        min_dbz=min_dbz,
        min_dbz_updraft=min_dbz_updraft,
        min_pixels=args.min_pixels,
        rhohv_threshold=args.rhohv_threshold,
    )

    # ------------------------------------------------------------------
    # Step 6 — Print report
    # ------------------------------------------------------------------
    _print_report(cores, radar_name)

    # ------------------------------------------------------------------
    # Step 7 — Optional plot
    # ------------------------------------------------------------------
    if args.plot:
        _plot_colmax_with_cores(
            colmax,
            xx,
            yy,
            cores,
            radar_name,
            timestamp,
            strategy=strategy,
            vol_nr=vol_nr,
            output_dir=args.plot_output_dir,
        )


if __name__ == "__main__":
    main()
