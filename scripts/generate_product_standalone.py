#!/usr/bin/env python3
"""
generate_product_standalone.py — Standalone COG product generation for a single field.

Useful for testing, debugging, and ad-hoc processing without running the daemon.

VOLUME RESOLUTION:
    The script expects the canonical NetCDF filename already present on disk:
        {ROOT_RADAR_FILES_PATH}/{RADAR}/netcdf/{RADAR}_{strategy}_{vol_nr}_{TIMESTAMP}.nc

    Pass --netcdf-path to point directly at the file, or use --netcdf-dir together
    with --radar-name / --strategy / --vol-nr / --timestamp to let the script
    construct the expected path.

GEOMETRY:
    Scans ROOT_GEOMETRY_PATH (from radarlib config or env var) for a file
    matching {RADAR}_{strategy}_{vol_nr}_*.npz and uses the first match.
    Override with --geometry-path.

PIPELINE:
    NetCDF  → read_radar_netcdf + estandarizar_campos_RMA
            → (optional) build GateFilter   [--filtered]
            → RawCogFieldProcessor.process_and_save
            → COG file at output-dir

Usage — run inside the genpro25-rmaX container or local venv:

    python3 scripts/generate_product_standalone.py \\
        --radar-name RMA1 \\
        --strategy 0315 \\
        --vol-nr 01 \\
        --field DBZH \\
        --timestamp 2026-05-14T10:30:45Z \\
        --output-dir /data/products

    # Generate filtered version:
    python3 scripts/generate_product_standalone.py \\
        --radar-name RMA1 --strategy 0315 --vol-nr 01 \\
        --field DBZH --timestamp 2026-05-14T10:30:45Z \\
        --output-dir /data/products --filtered

    # Point directly at a NetCDF file:
    python3 scripts/generate_product_standalone.py \\
        --netcdf-path /data/netcdf/RMA1_0315_01_20260514T103045Z.nc \\
        --field DBZH --output-dir /data/products

    # List all available fields in a volume:
    python3 scripts/generate_product_standalone.py \\
        --netcdf-path /data/netcdf/RMA1_0315_01_20260514T103045Z.nc \\
        --list-fields
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("generate_product_standalone")

# ---------------------------------------------------------------------------
# Minimal config shim  (RawCogFieldProcessor only reads local_product_dir)
# ---------------------------------------------------------------------------


@dataclass
class _MinimalConfig:
    """Minimal stand-in for ProductGenerationDaemonConfig."""

    local_product_dir: Path


# ---------------------------------------------------------------------------
# Geometry discovery
# ---------------------------------------------------------------------------


def _find_geometry(radar_name: str, strategy: str, vol_nr: str, geometry_dir: Path) -> Optional[Path]:
    """Return the first .npz geometry file that matches the volume pattern.

    Searches *geometry_dir* for files matching:
        ``{radar_name}_{strategy}_{vol_nr}_*.npz``

    Args:
        radar_name: Radar station identifier (e.g. ``"RMA1"``).
        strategy: Volume strategy code (e.g. ``"0315"``).
        vol_nr: Volume number (e.g. ``"01"``).
        geometry_dir: Directory to scan.

    Returns:
        Path to the first matching file, or ``None`` if none found.
    """
    pattern = str(geometry_dir / f"{radar_name}_{strategy}_{vol_nr}_*.npz")
    matches = sorted(glob.glob(pattern))
    if not matches:
        return None
    if len(matches) > 1:
        logger.warning(
            f"Multiple geometry files found for {radar_name}/{strategy}/{vol_nr} — "
            f"using the first match.  Use --geometry-path to select explicitly.\n"
            + "\n".join(f"  {m}" for m in matches)
        )
    return Path(matches[0])


# ---------------------------------------------------------------------------
# NetCDF resolution
# ---------------------------------------------------------------------------


def _resolve_netcdf(
    netcdf_path: Optional[str],
    radar_name: Optional[str],
    strategy: Optional[str],
    vol_nr: Optional[str],
    timestamp_str: Optional[str],
    netcdf_dir: Optional[str],
) -> Path:
    """Return the Path to the NetCDF volume file.

    Either *netcdf_path* must be given, or all of *radar_name*, *strategy*,
    *vol_nr*, *timestamp_str*, and *netcdf_dir* must be given.

    Raises:
        SystemExit: With a descriptive message if the file cannot be located.
    """
    if netcdf_path:
        p = Path(netcdf_path)
        if not p.exists():
            logger.error(f"NetCDF file not found: {p}")
            sys.exit(1)
        return p

    if not all([radar_name, strategy, vol_nr, timestamp_str, netcdf_dir]):
        logger.error(
            "Either --netcdf-path or all of "
            "--radar-name / --strategy / --vol-nr / --timestamp / --netcdf-dir must be provided."
        )
        sys.exit(1)

    # Parse timestamp — accept both "2026-05-14T10:30:45Z" and "20260514T103045Z"
    ts_str = timestamp_str.strip()
    try:
        if "T" in ts_str and "-" in ts_str:
            obs_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        else:
            obs_dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError as e:
        logger.error(f"Cannot parse timestamp '{timestamp_str}': {e}")
        sys.exit(1)

    formatted = obs_dt.strftime("%Y%m%dT%H%M%SZ")
    nc_filename = f"{radar_name}_{strategy}_{vol_nr}_{formatted}.nc"
    nc_path = Path(netcdf_dir) / nc_filename

    if not nc_path.exists():
        # Fall back: try the sub-directory layout used by ProcessingDaemon
        alt = Path(netcdf_dir) / radar_name / "netcdf" / nc_filename
        if alt.exists():
            nc_path = alt
        else:
            logger.error(
                f"NetCDF file not found.\n"
                f"  Tried: {nc_path}\n"
                f"  Tried: {alt}\n"
                "Ensure the volume has been processed by ProcessingDaemon before running this script."
            )
            sys.exit(1)

    return nc_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a single raw-float COG product from a radar NetCDF volume.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # --- Volume identification (alternative A: explicit path) ---
    parser.add_argument(
        "--netcdf-path",
        metavar="PATH",
        help="Direct path to the NetCDF volume file.  Overrides --netcdf-dir + --timestamp.",
    )

    # --- Volume identification (alternative B: components) ---
    id_group = parser.add_argument_group("Volume identification (used when --netcdf-path is not given)")
    id_group.add_argument("--radar-name", metavar="CODE", help="Radar station code, e.g. RMA1")
    id_group.add_argument("--strategy", metavar="CODE", help="Volume strategy code, e.g. 0315")
    id_group.add_argument("--vol-nr", metavar="NR", help="Volume number within the strategy, e.g. 01")
    id_group.add_argument(
        "--timestamp",
        metavar="ISO8601",
        help="Observation timestamp in ISO 8601 UTC, e.g. 2026-05-14T10:30:45Z",
    )
    id_group.add_argument(
        "--netcdf-dir",
        metavar="DIR",
        default=None,
        help="Directory that contains the NetCDF file (default: ROOT_RADAR_FILES_PATH from config)",
    )

    # --- Field selection ---
    parser.add_argument(
        "--field",
        metavar="NAME",
        help="Radar field to generate (e.g. DBZH, ZDR, RHOHV).  Required unless --list-fields is set.",
    )
    parser.add_argument(
        "--filtered",
        action="store_true",
        help="Apply GRC quality-control gate filter before interpolation (omits 'o' suffix in filename).",
    )

    # --- Output ---
    parser.add_argument(
        "--output-dir",
        metavar="DIR",
        default="./products",
        help="Root output directory for COG files (default: ./products).",
    )

    # --- Geometry ---
    parser.add_argument(
        "--geometry-path",
        metavar="PATH",
        help="Explicit .npz geometry file.  Auto-detected from ROOT_GEOMETRY_PATH when omitted.",
    )
    parser.add_argument(
        "--geometry-dir",
        metavar="DIR",
        default=None,
        help="Directory to scan for geometry files (default: ROOT_GEOMETRY_PATH from config).",
    )

    # --- Misc ---
    parser.add_argument("--list-fields", action="store_true", help="List available fields in the volume and exit.")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging.")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ------------------------------------------------------------------ #
    # 1. Resolve NetCDF path                                               #
    # ------------------------------------------------------------------ #
    # Determine netcdf_dir default from config if not supplied
    netcdf_dir = args.netcdf_dir
    if netcdf_dir is None and args.netcdf_path is None:
        try:
            from radarlib import config as radarlib_config

            netcdf_dir = getattr(radarlib_config, "ROOT_RADAR_FILES_PATH", None)
        except Exception:
            pass

    nc_path = _resolve_netcdf(
        netcdf_path=args.netcdf_path,
        radar_name=args.radar_name,
        strategy=args.strategy,
        vol_nr=args.vol_nr,
        timestamp_str=args.timestamp,
        netcdf_dir=netcdf_dir,
    )
    logger.info(f"NetCDF: {nc_path}")

    # ------------------------------------------------------------------ #
    # 2. Load and standardise radar                                        #
    # ------------------------------------------------------------------ #
    from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf
    from radarlib.utils.fields_utils import determine_reflectivity_fields, get_lowest_nsweep
    from radarlib.utils.names_utils import get_time_from_RMA_filename

    try:
        radar = read_radar_netcdf(str(nc_path))
    except Exception as e:
        logger.error(f"Failed to load NetCDF: {e}")
        sys.exit(1)

    try:
        radar = estandarizar_campos_RMA(radar)
    except Exception as e:
        logger.error(f"Failed to standardise fields: {e}")
        sys.exit(1)

    logger.info(f"Fields in volume: {sorted(radar.fields.keys())}")

    # ------------------------------------------------------------------ #
    # 3. --list-fields shortcut                                            #
    # ------------------------------------------------------------------ #
    if args.list_fields:
        print("\nFields available in this volume:")
        for name in sorted(radar.fields.keys()):
            arr = radar.fields[name].get("data")
            shape = arr.shape if arr is not None else "?"
            units = radar.fields[name].get("units", "—")
            print(f"  {name:<20}  shape={str(shape):<15}  units={units}")
        sys.exit(0)

    if not args.field:
        parser.error("--field is required (or use --list-fields to see what is available).")

    if args.field not in radar.fields:
        logger.error(f"Field '{args.field}' not found in volume.\n" f"Available: {sorted(radar.fields.keys())}")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 4. Derive volume metadata (radar_name, strategy, vol_nr from stem)   #
    # ------------------------------------------------------------------ #
    stem = nc_path.stem  # e.g. RMA1_0315_01_20260514T103045Z
    parts = stem.split("_")
    if len(parts) < 4:
        logger.error(f"Cannot parse NetCDF stem '{stem}': expected {{RADAR}}_{{strategy}}_{{vol_nr}}_{{timestamp}}")
        sys.exit(1)

    radar_name = args.radar_name or parts[0]
    strategy = args.strategy or parts[1]
    vol_nr = args.vol_nr or parts[2]

    try:
        obs_time = get_time_from_RMA_filename(stem)
    except Exception as e:
        logger.error(f"Cannot extract observation time from filename '{stem}': {e}")
        sys.exit(1)

    volume_info: Dict[str, Any] = {
        "volume_id": stem,
        "strategy": strategy,
        "vol_nr": vol_nr,
        "observation_datetime": obs_time.strftime("%Y%m%dT%H%M%SZ"),
        "netcdf_path": str(nc_path),
    }
    logger.debug(f"volume_info: {json.dumps(volume_info, indent=2)}")

    # ------------------------------------------------------------------ #
    # 5. Resolve and load geometry                                         #
    # ------------------------------------------------------------------ #
    from radarlib.radar_grid import load_geometry

    if args.geometry_path:
        geom_path = Path(args.geometry_path)
        if not geom_path.exists():
            logger.error(f"Geometry file not found: {geom_path}")
            sys.exit(1)
    else:
        geom_dir: Optional[Path] = None
        if args.geometry_dir:
            geom_dir = Path(args.geometry_dir)
        else:
            try:
                from radarlib import config as radarlib_config

                geom_dir = Path(getattr(radarlib_config, "ROOT_GEOMETRY_PATH", "."))
            except Exception:
                geom_dir = Path(".")

        geom_path = _find_geometry(radar_name, strategy, vol_nr, geom_dir)
        if geom_path is None:
            logger.error(
                f"No geometry file found for {radar_name}/{strategy}/{vol_nr} in {geom_dir}.\n"
                "Use --geometry-path to specify the file explicitly, or run the daemon once "
                "so it can build and cache the geometry."
            )
            sys.exit(1)

    logger.info(f"Geometry: {geom_path}")
    try:
        geometry = load_geometry(str(geom_path))
    except Exception as e:
        logger.error(f"Failed to load geometry: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 6. (Optional) build gate filter                                       #
    # ------------------------------------------------------------------ #
    gate_filter = None
    if args.filtered:
        from pyart.config import get_field_name

        from radarlib import config as radarlib_config
        from radarlib.radar_grid import GateFilter

        hrefl_field = determine_reflectivity_fields(radar)["hrefl_field"]
        rhv_field = get_field_name("cross_correlation_ratio")
        wrad_field = get_field_name("spectrum_width")
        zdr_field = get_field_name("differential_reflectivity")

        gate_filter = GateFilter(radar)
        if getattr(radarlib_config, "GRC_RHV_FILTER", False) and rhv_field in radar.fields:
            gate_filter.exclude_below(rhv_field, radarlib_config.GRC_RHV_THRESHOLD)
            logger.debug(f"GateFilter: exclude {rhv_field} < {radarlib_config.GRC_RHV_THRESHOLD}")
        if getattr(radarlib_config, "GRC_WRAD_FILTER", False) and wrad_field in radar.fields:
            gate_filter.exclude_above(wrad_field, radarlib_config.GRC_WRAD_THRESHOLD)
            logger.debug(f"GateFilter: exclude {wrad_field} > {radarlib_config.GRC_WRAD_THRESHOLD}")
        if getattr(radarlib_config, "GRC_REFL_FILTER", False) and hrefl_field in radar.fields:
            gate_filter.exclude_below(hrefl_field, radarlib_config.GRC_REFL_THRESHOLD)
            logger.debug(f"GateFilter: exclude {hrefl_field} < {radarlib_config.GRC_REFL_THRESHOLD}")
        if getattr(radarlib_config, "GRC_ZDR_FILTER", False) and zdr_field in radar.fields:
            gate_filter.exclude_above(zdr_field, radarlib_config.GRC_ZDR_THRESHOLD)
            logger.debug(f"GateFilter: exclude {zdr_field} > {radarlib_config.GRC_ZDR_THRESHOLD}")

        logger.info("Gate filter built — generating filtered COG.")

    # ------------------------------------------------------------------ #
    # 7. Resolve config_key_field for reflectivity aliases                  #
    # ------------------------------------------------------------------ #
    refl_fields = determine_reflectivity_fields(radar)
    if args.field in (refl_fields["hrefl_field"], refl_fields["vrefl_field"]):
        config_key_field: Optional[str] = "REFL"
    else:
        config_key_field = None  # FieldProcessor defaults to field_name

    sweep = get_lowest_nsweep(radar)

    # ------------------------------------------------------------------ #
    # 8. Run FieldProcessor                                                #
    # ------------------------------------------------------------------ #
    from radarlib.daemons.field_processor import RawCogFieldProcessor
    from radarlib.radar_grid import get_field_data

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stub_config = _MinimalConfig(local_product_dir=output_dir)

    processor = RawCogFieldProcessor(
        config=stub_config,
        volume_info=volume_info,
        radar_name=radar_name,
    )

    logger.info(
        f"Processing field '{args.field}' "
        f"({'filtered' if args.filtered else 'unfiltered'}) "
        f"for volume {stem} ..."
    )

    field_data = get_field_data(radar, args.field)
    result = processor.process_and_save(
        field_data=field_data,
        field_name=args.field,
        radar=radar,
        geometry=geometry,
        gate_filter=gate_filter,
        output_dir=output_dir,
        sweep=sweep,
        config_key_field=config_key_field,
    )

    # ------------------------------------------------------------------ #
    # 9. Report                                                            #
    # ------------------------------------------------------------------ #
    if result is None:
        logger.error("COG generation failed — see log output above for details.")
        sys.exit(1)

    # Read back metadata tags from the file for reporting
    try:
        import rasterio

        with rasterio.open(str(result)) as ds:
            tags = ds.tags()
    except Exception:
        tags = {}

    print()
    print("✓  COG generated successfully")
    print(f"   Output  : {result}")
    print(f"   Field   : {args.field}  ({'filtered' if args.filtered else 'unfiltered'})")
    print(f"   Radar   : {radar_name}")
    print(f"   Volume  : strategy={strategy}  vol_nr={vol_nr}")
    print(f"   Time    : {obs_time.isoformat()}")
    if tags:
        print("   Metadata tags:")
        for k, v in sorted(tags.items()):
            if k.startswith("radarlib"):
                print(f"     {k} = {v}")


if __name__ == "__main__":
    main()
