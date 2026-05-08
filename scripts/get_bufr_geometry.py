#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BUFR Geometry Summary Script

Given a single BUFR file (local path or FTP path), decodes it and prints
the geometry summary: per-sweep dimensions and total gate count.

Usage:
    # Local file
    python3 scripts/get_bufr_geometry.py /path/to/RMA1_0315_01_WRAD_20260501T120000Z.BUFR

    # FTP file (requires FTP credentials in app.config or environment)
    python3 scripts/get_bufr_geometry.py /L2/RMA1/2026/05/01/12/0000/RMA1_0315_01_WRAD_20260501T120000Z.BUFR

Docker execution:
    docker exec genpro25-rma1 python3 /workspace/scripts/get_bufr_geometry.py \\
        /L2/RMA1/2026/05/01/12/0000/RMA1_0315_01_WRAD_20260501T120000Z.BUFR
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("get_bufr_geometry")


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------


def resolve_ftp_credentials() -> Tuple[str, str, str]:
    """Resolve FTP credentials from app.config or environment variables."""
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
        import config as app_config

        host = getattr(app_config, "FTP_HOST", None)
        user = getattr(app_config, "FTP_USER", None)
        password = getattr(app_config, "FTP_PASS", None)

        if host and user and password:
            logger.info(f"Loaded FTP credentials from app.config: {host}")
            return host, user, password
    except Exception as e:
        logger.debug(f"Could not load from app.config: {e}")

    host = os.environ.get("FTP_HOST")
    user = os.environ.get("FTP_USER")
    password = os.environ.get("FTP_PASS")

    if not host or not user or not password:
        raise RuntimeError(
            "FTP credentials not found. Set FTP_HOST, FTP_USER, FTP_PASS "
            "environment variables or ensure app/config.py is available."
        )

    logger.info(f"Loaded FTP credentials from environment: {host}")
    return host, user, password


# ---------------------------------------------------------------------------
# Geometry data extracted from one BUFR file
# ---------------------------------------------------------------------------


@dataclass
class SweepGeometry:
    """Geometry for a single sweep."""

    sweep_idx: int
    ngates: int
    nrays: int
    gate_size: int
    gate_offset: int
    elevation: Optional[float] = None


@dataclass
class FileGeometry:
    """All sweep geometry extracted from one decoded BUFR file."""

    ftp_path: str
    filename: str
    obs_timestamp: Optional[str] = None
    sweeps: List[SweepGeometry] = field(default_factory=list)
    decode_error: Optional[str] = None

    @property
    def total_gates(self) -> int:
        """Total flat gate count: sum(ngates * nrays) across all sweeps."""
        return sum(s.ngates * s.nrays for s in self.sweeps)


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def decode_file_geometry(local_path: str, ftp_path: str) -> FileGeometry:
    """
    Decode a local BUFR file and extract per-sweep geometry.

    Raises ValueError if decoding fails.
    """
    from radarlib.io.bufr.bufr import bufr_to_dict

    result = FileGeometry(
        ftp_path=ftp_path,
        filename=Path(local_path).name,
    )

    field_dict = bufr_to_dict(local_path, root_resources=None)
    if field_dict is None:
        raise ValueError(f"bufr_to_dict returned None for {local_path}")

    sweeps_df = field_dict["info"]["sweeps"]

    for idx, row in sweeps_df.iterrows():
        elev = None
        if "elevaciones" in sweeps_df.columns:
            try:
                elev = float(row["elevaciones"])
            except (TypeError, ValueError):
                pass

        result.sweeps.append(
            SweepGeometry(
                sweep_idx=int(idx),
                ngates=int(row["ngates"]),
                nrays=int(row["nrayos"]),
                gate_size=int(row["gate_size"]),
                gate_offset=int(row["gate_offset"]),
                elevation=elev,
            )
        )

    # Attach obs timestamp from the filename
    try:
        from radarlib.io.bufr.bufr import BUFRFilename

        parsed = BUFRFilename(Path(local_path).name)
        result.obs_timestamp = parsed.datetime.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        pass

    return result


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_geometry_summary(geom: FileGeometry) -> None:
    """Print a clean geometry summary for a single file."""

    print()
    print(f"=== GEOMETRY SUMMARY: {geom.filename} ===")
    print()

    if geom.decode_error:
        print(f"✗ Decode failed: {geom.decode_error}")
        print()
        return

    ts = geom.obs_timestamp or "unknown"
    print(f"File         : {geom.filename}")
    print(f"Timestamp    : {ts}")
    print()

    # Per-sweep table
    col_sw = 6
    col_ng = 8
    col_nr = 7
    col_gs = 11
    col_go = 12
    col_el = 11

    header = (
        f"{'SWEEP':>{col_sw}}  "
        f"{'NGATES':>{col_ng}}  "
        f"{'NRAYS':>{col_nr}}  "
        f"{'GATE_SIZE':>{col_gs}}  "
        f"{'GATE_OFFSET':>{col_go}}  "
        f"{'ELEVATION':>{col_el}}"
    )
    print(header)
    print("-" * len(header))

    for sw in geom.sweeps:
        el_str = f"{sw.elevation:.1f}°" if sw.elevation is not None else "—"
        print(
            f"{sw.sweep_idx:>{col_sw}}  "
            f"{sw.ngates:>{col_ng}}  "
            f"{sw.nrays:>{col_nr}}  "
            f"{sw.gate_size:>{col_gs}}  "
            f"{sw.gate_offset:>{col_go}}  "
            f"{el_str:>{col_el}}"
        )

    print()
    print(f"Total gate count (field_data size): {geom.total_gates:,}")
    print()


# ---------------------------------------------------------------------------
# Detect whether path is local or FTP
# ---------------------------------------------------------------------------


def is_ftp_path(path: str) -> bool:
    """Heuristic: if path starts with '/', it's FTP. Otherwise check if local file exists."""
    if path.startswith("/"):
        return not Path(path).exists()  # FTP if path doesn't exist locally
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Decode a BUFR file and print its geometry summary",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local file
  python3 scripts/get_bufr_geometry.py /path/to/RMA1_0315_01_WRAD_20260501T120000Z.BUFR

  # FTP file (requires credentials)
  python3 scripts/get_bufr_geometry.py /L2/RMA1/2026/05/01/12/0000/RMA1_0315_01_WRAD_20260501T120000Z.BUFR

  # Docker
  docker exec genpro25-rma1 python3 /workspace/scripts/get_bufr_geometry.py \\
    /L2/RMA1/2026/05/01/12/0000/RMA1_0315_01_WRAD_20260501T120000Z.BUFR
        """,
    )

    parser.add_argument(
        "bufr_path",
        help="Path to BUFR file (local or FTP path starting with /)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )
    parser.add_argument(
        "--temp-dir",
        default="/tmp/get_bufr_geom",
        help="Temp directory for FTP downloads (default: /tmp/get_bufr_geom)",
    )

    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    bufr_path = args.bufr_path
    is_ftp = is_ftp_path(bufr_path)

    logger.info(f"Input path: {bufr_path}")
    logger.info(f"Is FTP: {is_ftp}")

    geom = FileGeometry(
        ftp_path=bufr_path,
        filename=Path(bufr_path).name,
    )

    local_path: Optional[Path] = None

    # --- Download if FTP ---
    if is_ftp:
        try:
            ftp_host, ftp_user, ftp_pass = resolve_ftp_credentials()
        except RuntimeError as e:
            print(f"✗ {e}", file=sys.stderr)
            sys.exit(1)

        from radarlib.io.ftp.ftp_client import RadarFTPClient

        temp_dir = Path(args.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        local_path = temp_dir / Path(bufr_path).name

        try:
            logger.info(f"Connecting to FTP {ftp_host}...")
            with RadarFTPClient(host=ftp_host, user=ftp_user, password=ftp_pass) as client:
                logger.info(f"Downloading: {bufr_path}")
                client.download_file(str(bufr_path), local_path)
                logger.info(f"Downloaded to: {local_path}")
        except Exception as e:
            geom.decode_error = f"FTP download failed: {e}"
            logger.error(f"Download failed: {e}")

    else:
        # Local file
        local_path = Path(bufr_path)
        if not local_path.exists():
            geom.decode_error = f"Local file not found: {bufr_path}"
            logger.error(geom.decode_error)

    # --- Decode ---
    if geom.decode_error is None and local_path:
        try:
            logger.info(f"Decoding: {local_path}")
            geom = decode_file_geometry(str(local_path), bufr_path)
            logger.info(f"Decode successful: {len(geom.sweeps)} sweeps")
        except Exception as e:
            geom.decode_error = str(e)
            logger.error(f"Decode failed: {e}")
        finally:
            # Clean up temp file if it was downloaded
            if is_ftp and local_path and local_path.exists():
                try:
                    local_path.unlink()
                    logger.debug(f"Cleaned up temp file: {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temp file: {e}")

    # --- Print report ---
    print_geometry_summary(geom)

    # Exit with error code if decode failed
    if geom.decode_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
