"""
detect_tops_from_netcdf.py — Detect storm tops from a NetCDF radar volume.

This script loads a pre-processed NetCDF radar volume, obtains the full 3D
Cartesian reflectivity grid via the precomputed grid geometry, and then detects
storm top centroids with :func:`radarlib.radar_grid.detect_tops_from_3d_grid`.

The processing pipeline mirrors the sequence used inside
:meth:`ProductGenerationDaemon._generate_raw_cog_products_sync` so the 3D
interpolated grid produced here is numerically identical to the one used by
the daemon.  Unlike the cores example, this script does **not** call
``column_max`` — tops require the full 3D grid.

Usage
-----
python examples/detect_tops_from_netcdf.py \
    --netcdf /data/radares/RMA1/netcdf/RMA1_0315_01_20260417T160000Z.nc \
    --geometry /data/radares/RMA1/geometry/RMA1_0315_01_..._geometry.npz \
    --plot
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Make sure the package root is importable when run directly from the repo
# ---------------------------------------------------------------------------
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root / "src"))

from radarlib import config  # noqa: E402
from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf  # noqa: E402
from radarlib.radar_grid import (  # noqa: E402
    apply_geometry,
    compute_grid_geometry,
    detect_tops_from_3d_grid,
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
    p = argparse.ArgumentParser(
        description="Detect storm tops from a radar NetCDF volume.",
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
        default=(
            "tests/data/geometry/"
            "RMA1_0315_01_RES1000x600_TOA15000_HF1p0000_MR900_MN1_NB1p30_BSP1p10_nearest_geometry.npz"
        ),
        help=("Path to precomputed geometry file. " "If not found, will be created from the NetCDF data."),
    )
    p.add_argument(
        "--min-dbz",
        type=float,
        default=None,
        help=(
            "Reflectivity threshold for tops detection (dBZ). " f"Defaults to config.TOPS_MIN_Z ({config.TOPS_MIN_Z})."
        ),
    )
    p.add_argument(
        "--min-pixels",
        type=int,
        default=2,
        help=("Minimum blob size in pixels. " "Must be exceeded: pixel_count > min_pixels."),
    )
    p.add_argument(
        "--min-altitude",
        type=float,
        default=None,
        help=(
            "Minimum mean altitude of storm top blobs (metres). "
            f"Defaults to config.TOPS_MIN_DEV_M ({config.TOPS_MIN_DEV_M})."
        ),
    )
    p.add_argument(
        "--rhohv-threshold",
        type=float,
        default=config.TOPS_RHOHV_THRESHOLD,
        help=(
            "RhoHV quality gate threshold (0–1). "
            f"Defaults to config.TOPS_RHOHV_THRESHOLD ({config.TOPS_RHOHV_THRESHOLD})."
        ),
    )
    p.add_argument(
        "--no-rhohv-quality-gate",
        action="store_true",
        default=False,
        help=(
            "Disable RhoHV quality gating entirely. "
            "Blobs are accepted on altitude and pixel count alone. "
            "Increases recall but also accepts more non-meteorological echoes at upper levels."
        ),
    )
    p.add_argument(
        "--plot",
        action="store_true",
        default=False,
        help="If set, save matplotlib figures to --plot-output-dir.",
    )
    p.add_argument(
        "--plot-output-dir",
        type=str,
        default="tests/data/outputs/",
        help="Directory where to save plot PNG files.",
    )
    return p


# ---------------------------------------------------------------------------
# Coordinate extraction
# ---------------------------------------------------------------------------


def _extract_coordinates(geometry) -> tuple:
    """
    Extract 2D x/y and 1D z coordinate arrays from a GridGeometry object.

    The geometry stores the grid extents in ``grid_limits`` — a tuple of
    ((z_min, z_max), (y_min, y_max), (x_min, x_max)) — and the grid
    dimensions in ``grid_shape`` (nz, ny, nx).  Regular linspace arrays are
    built for each axis; x and y are expanded to 2D meshgrids of shape
    (ny, nx) and z is kept as a 1D array of level altitudes via
    :meth:`~radarlib.radar_grid.GridGeometry.z_levels`.

    Parameters
    ----------
    geometry : GridGeometry

    Returns
    -------
    tuple
        ``(xx, yy, z_1d)`` where ``xx`` and ``yy`` have shape (ny, nx) and
        ``z_1d`` has shape (nz,).
    """
    _nz, ny, nx = geometry.grid_shape
    y_min, y_max = geometry.grid_limits[1]
    x_min, x_max = geometry.grid_limits[2]

    x_1d = np.linspace(x_min, x_max, nx, dtype=np.float32)
    y_1d = np.linspace(y_min, y_max, ny, dtype=np.float32)
    yy, xx = np.meshgrid(y_1d, x_1d, indexing="ij")  # shape (ny, nx)

    # z_levels() returns np.linspace(z_min, z_max, nz) — the canonical 1D accessor
    z_1d = geometry.z_levels().astype(np.float32)

    return xx, yy, z_1d


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _print_report(tops: list, radar_name: str) -> None:
    """Print a human-readable detection report to stdout."""
    print(f"\nTops detected: {len(tops)}\n")
    if not tops:
        print("  (no storm tops found)\n")
        return

    for i, top in enumerate(tops, start=1):
        x_km = top["x_m"] / 1000.0
        y_km = top["y_m"] / 1000.0
        range_km = top["range_m"] / 1000.0
        print(f"Top {i}:")
        print(f"  x = {x_km:>10,.1f} km   y = {y_km:>10,.1f} km")
        print(f"  range = {range_km:>6.1f} km")
        print(f"  altitude = {top['altitude_m']:>7,.0f} m  ({top['altitude_km']} km)")
        print(f"  pixels = {top['pixel_count']}")
        print(f"  level index = {top['level_index']}")
        print()


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def _plot_grid_with_tops(
    grid_3d: np.ndarray,
    xx: np.ndarray,
    yy: np.ndarray,
    z_1d: np.ndarray,
    tops: list,
    radar_name: str,
    timestamp: str,
    strategy: str,
    vol_nr: str,
    output_dir: str = "tests/data/outputs/",
) -> None:
    """
    Save two side-by-side panels to a PNG file.

    Left  — COLMAX grid with detected top centroids as cyan triangles.
    Right — Horizontal bar chart of detected top altitudes coloured by ``turbo``.

    Parameters
    ----------
    grid_3d : np.ndarray
        3D grid (NZ, NY, NX) used to compute COLMAX for display.
    xx, yy : np.ndarray
        2D coordinate grids in metres.
    z_1d : np.ndarray
        1D level altitudes in metres.
    tops : list of dict
        Detected tops from :func:`detect_tops_from_3d_grid`.
    radar_name, timestamp, strategy, vol_nr : str
        Used for the figure title and output filename.
    output_dir : str
        Directory where the PNG is saved.
    """
    import matplotlib.cm as cm
    import matplotlib.pyplot as plt

    # Try the project colormap; fall back gracefully
    try:
        import radarlib.visualization  # noqa: F401 — registers custom cmaps

        cmap_refl = "grc_th"
    except Exception:
        cmap_refl = "NWSRef"

    colmax = np.nanmax(np.ma.filled(grid_3d.astype(np.float32), np.nan), axis=0)

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(14, 7))

    # ------------------------------------------------------------------
    # Left panel — COLMAX with top centroids
    # ------------------------------------------------------------------
    x_km = xx / 1000.0
    y_km = yy / 1000.0

    pcm = ax_left.pcolormesh(
        x_km,
        y_km,
        np.ma.masked_invalid(colmax),
        cmap=cmap_refl,
        vmin=-10.0,
        vmax=65.0,
        shading="auto",
    )
    cbar = fig.colorbar(pcm, ax=ax_left, pad=0.02)
    cbar.set_label("COLMAX reflectivity (dBZ)", fontsize=10)

    for top in tops:
        cx_km = top["x_m"] / 1000.0
        cy_km = top["y_m"] / 1000.0
        ax_left.plot(
            cx_km,
            cy_km,
            marker="^",
            markersize=8,
            markerfacecolor="cyan",
            markeredgecolor="black",
            markeredgewidth=0.5,
            zorder=5,
        )
        ax_left.annotate(
            f"{top['altitude_km']} km",
            xy=(cx_km, cy_km),
            xytext=(4, 4),
            textcoords="offset points",
            color="black",
            fontsize=8,
            fontweight="bold",
            zorder=6,
        )

    ax_left.set_xlabel("Range East (km)", fontsize=10)
    ax_left.set_ylabel("Range North (km)", fontsize=10)
    ax_left.set_title(f"{radar_name} — COLMAX with storm tops\n{timestamp}", fontsize=11)
    ax_left.set_aspect("equal")
    ax_left.grid(color="gray", linestyle="--", linewidth=0.4, alpha=0.5)

    # ------------------------------------------------------------------
    # Right panel — altitude horizontal bar chart
    # ------------------------------------------------------------------
    if tops:
        alts_km = [t["altitude_km"] for t in tops]
        _cmap = cm.get_cmap("turbo")
        alt_min = min(alts_km)
        alt_max = max(alts_km)
        alt_range = (alt_max - alt_min) if (alt_max - alt_min) > 0 else 1.0
        colors = [_cmap((a - alt_min) / alt_range) for a in alts_km]

        y_positions = list(range(len(tops)))
        bars = ax_right.barh(y_positions, alts_km, color=colors, edgecolor="white", height=0.6)

        for bar, top in zip(bars, tops):
            ax_right.text(
                bar.get_width() + 0.1,
                bar.get_y() + bar.get_height() / 2.0,
                f"Top {tops.index(top) + 1}  ({top['altitude_km']} km)",
                va="center",
                ha="left",
                fontsize=9,
            )

        ax_right.set_yticks([])
        ax_right.set_xlabel("Altitude (km)", fontsize=10)
        ax_right.set_title("Detected top altitudes", fontsize=11)
        ax_right.margins(x=0.25)
    else:
        ax_right.text(
            0.5,
            0.5,
            "No tops detected",
            transform=ax_right.transAxes,
            ha="center",
            va="center",
            fontsize=14,
            color="gray",
        )
        ax_right.set_axis_off()

    plt.tight_layout()

    timestamp_for_file = timestamp.replace(":", "").replace("-", "").replace("T", "t").replace("Z", "")
    filename = f"{radar_name}_{strategy}_{vol_nr}_{timestamp_for_file}_tops.png"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info("Saved plot to %s", output_path)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entry point for the storm tops detection example."""
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

    min_dbz = args.min_dbz if args.min_dbz is not None else config.TOPS_MIN_Z
    min_dev_m = args.min_altitude if args.min_altitude is not None else config.TOPS_MIN_DEV_M

    # ------------------------------------------------------------------
    # Step 1 — Load and standardise the radar volume
    # ------------------------------------------------------------------
    logger.info("Loading radar volume: %s", netcdf_path)
    radar = read_radar_netcdf(str(netcdf_path))
    radar = estandarizar_campos_RMA(radar)

    fields = determine_reflectivity_fields(radar)
    hrefl_field = fields["hrefl_field"]
    logger.info("Horizontal reflectivity field: %s", hrefl_field)

    # Derive metadata from the filename (same pattern as detect_cores_from_netcdf.py)
    filename_components = extract_netcdf_filename_components(netcdf_filename)
    timestamp = filename_components.get("timestamp", netcdf_path.stem.split("_")[-1])
    radar_name = filename_components.get("radar_name", "UNKNOWN")
    strategy = filename_components.get("strategy", "UNKNOWN")
    vol_nr = filename_components.get("vol_nr", "UNKNOWN")

    # ------------------------------------------------------------------
    # Step 2 — Load precomputed grid geometry (or build on the fly)
    # ------------------------------------------------------------------
    try:
        geometry = load_geometry(args.geometry)
    except Exception as exc:
        logger.warning("Failed to load geometry from %s: %s", args.geometry, exc)
        logger.info("Attempting to create geometry from loaded radar data...")
        try:
            gate_x, gate_y, gate_z = get_gate_coordinates(radar)
            logger.info("Extracted gate coordinates: %s gates", f"{len(gate_x):,}")

            radar_info = get_radar_info(radar)

            range_max_m = safe_range_max_m(radar, round_to_km=20)
            z_grid_limits = (0.0, config.GEOMETRY_TOA)
            y_grid_limits = (-range_max_m, range_max_m)
            x_grid_limits = (-range_max_m, range_max_m)

            z_points, y_points, x_points = calculate_grid_points(
                z_grid_limits,
                y_grid_limits,
                x_grid_limits,
                config.GEOMETRY_RES_XY,
                config.GEOMETRY_RES_Z,
            )
            grid_shape = (z_points, y_points, x_points)
            grid_limits = (z_grid_limits, y_grid_limits, x_grid_limits)

            logger.info("Computing geometry with grid shape %s...", grid_shape)
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
            logger.info(
                "Successfully created geometry: %.1f MB in memory",
                geometry.memory_usage_mb(),
            )

            file_name = f"{build_geometry_filename(geometry.metadata)}.npz"
            file_path = os.path.join("tests/data/geometry/", file_name)
            save_geometry(geometry, file_path)
            logger.info("Saved geometry to %s", file_path)

        except Exception as build_exc:
            print(
                f"ERROR: Could not load or create geometry:\n"
                f"  Original error: {exc}\n"
                f"  Fallback error: {build_exc}",
                file=sys.stderr,
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # Step 3 — Obtain the full 3D Cartesian grid
    # ------------------------------------------------------------------
    logger.info("Computing 3D Cartesian grid from field '%s'", hrefl_field)
    field_data = get_field_data(radar, hrefl_field)
    grid_3d = apply_geometry(geometry, field_data)
    logger.info("3D grid shape: %s", grid_3d.shape)

    # 3D grid statistics (analogous to COLMAX stats in cores example)
    valid_vals = grid_3d[~np.isnan(grid_3d)]
    if len(valid_vals) > 0:
        logger.info("3D grid statistics (valid values only):")
        logger.info("  Min:    %.2f dBZ", float(np.min(valid_vals)))
        logger.info("  Max:    %.2f dBZ", float(np.max(valid_vals)))
        logger.info("  Mean:   %.2f dBZ", float(np.mean(valid_vals)))
        logger.info("  Median: %.2f dBZ", float(np.median(valid_vals)))
        logger.info("  Std:    %.2f dBZ", float(np.std(valid_vals)))
        logger.info(
            "  Pixels > %.1f dBZ: %s",
            min_dbz,
            f"{int(np.sum(valid_vals > min_dbz)):,}",
        )
    else:
        logger.warning("3D grid contains only NaN values!")

    # ------------------------------------------------------------------
    # Step 4 — Extract coordinate arrays from the geometry object
    # ------------------------------------------------------------------
    xx, yy, z_1d = _extract_coordinates(geometry)
    logger.info("Grid: nz=%d levels from %.0f m to %.0f m", len(z_1d), z_1d[0], z_1d[-1])

    # ------------------------------------------------------------------
    # Step 5 — Optional: 3D RhoHV grid for quality gating
    # ------------------------------------------------------------------
    rhohv_3d = None
    if not args.no_rhohv_quality_gate:
        rhv_field = None
        if "RHOHV" in radar.fields:
            rhv_field = "RHOHV"
        elif "RhoHV" in radar.fields:
            rhv_field = "RhoHV"

        if rhv_field is not None:
            try:
                rhohv_field_data = get_field_data(radar, rhv_field)
                rhohv_3d = apply_geometry(geometry, rhohv_field_data)
                logger.info(
                    "Extracted 3D RhoHV grid for quality gating. Shape: %s",
                    rhohv_3d.shape,
                )
            except Exception as e:
                logger.warning(
                    "Could not extract 3D RhoHV: %s. Proceeding without RhoHV quality gate.",
                    e,
                )
        else:
            logger.warning(
                "No RhoHV field found in radar volume. "
                "Tops quality gate will rely on altitude and pixel count alone."
            )

    # ------------------------------------------------------------------
    # Step 6 — Detect storm tops
    # ------------------------------------------------------------------
    logger.info("Running tops detection:")
    logger.info("  min_dbz:                 %.1f dBZ", min_dbz)
    logger.info("  min_altitude:            %.0f m", min_dev_m)
    logger.info("  min_pixels:              %d", args.min_pixels)
    logger.info("  rhohv_threshold:         %.2f", args.rhohv_threshold)
    logger.info("  rhohv_quality_gate:      %s", not args.no_rhohv_quality_gate)

    tops = detect_tops_from_3d_grid(
        grid_3d=grid_3d,
        x_coords=xx,
        y_coords=yy,
        z_coords=z_1d,
        rhohv_3d=rhohv_3d,
        min_dbz=min_dbz,
        min_dev_m=min_dev_m,
        min_pixels=args.min_pixels,
        rhohv_threshold=args.rhohv_threshold,
    )

    # ------------------------------------------------------------------
    # Step 7 — Print report
    # ------------------------------------------------------------------
    _print_report(tops, radar_name)

    # ------------------------------------------------------------------
    # Step 8 — Optional plot
    # ------------------------------------------------------------------
    if args.plot:
        _plot_grid_with_tops(
            grid_3d,
            xx,
            yy,
            z_1d,
            tops,
            radar_name,
            timestamp,
            strategy,
            vol_nr,
            output_dir=args.plot_output_dir,
        )


if __name__ == "__main__":
    main()
