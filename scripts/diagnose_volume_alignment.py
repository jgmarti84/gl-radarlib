#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Volume Alignment Diagnostic Script

Decodes all BUFR fields for a complete radar volume and checks whether
each field can be aligned to the reference grid built by
bufr_fields_to_pyart_radar(). This reproduces the exact arithmetic used
in _find_reference_field() and _align_field_to_reference() so you can
see which field fails and why without having to run the full pipeline.

Usage — local files (e.g. already downloaded to the bufr spool):
    python3 scripts/diagnose_volume_alignment.py \\
        /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_DBZH_20260716T154443Z.BUFR \\
        /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_DBZV_20260716T154443Z.BUFR \\
        /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_ZDR_20260716T154443Z.BUFR \\
        /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_KDP_20260716T154443Z.BUFR \\
        /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_PHIDP_20260716T154443Z.BUFR \\
        /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_RHOHV_20260716T154443Z.BUFR

Usage — FTP fetch (downloads all fields for the given volume timestamp):
    python3 scripts/diagnose_volume_alignment.py \\
        --radar RMA20 --strategy 0315 --vol 01 \\
        --timestamp "2026-07-16 15:44:43"

Docker execution:
    docker exec genpro25-rma20 python3 /workspace/scripts/diagnose_volume_alignment.py \\
        --radar RMA20 --strategy 0315 --vol 01 \\
        --timestamp "2026-07-16 15:44:43"
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("diagnose_volume_alignment")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class FieldGeometry:
    """Geometry extracted from one decoded BUFR field."""

    filename: str
    field_name: str
    gate_offset: int
    gate_size: int
    ngates: int
    nrays: int
    last_gate: int  # gate_offset + gate_size * ngates
    decode_error: Optional[str] = None


@dataclass
class AlignmentResult:
    """Result of simulating _align_field_to_reference for one field."""

    field_name: str
    filename: str
    is_reference: bool
    init: Optional[int]  # starting gate index in reference grid; None on gate_size mismatch
    passes: bool
    failure_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Credential resolution (identical pattern to other scripts)
# ---------------------------------------------------------------------------


def resolve_ftp_credentials() -> Tuple[str, str, str]:
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
        import config as app_config

        host = getattr(app_config, "FTP_HOST", None)
        user = getattr(app_config, "FTP_USER", None)
        password = getattr(app_config, "FTP_PASS", None)
        if host and user and password:
            return host, user, password
    except Exception:
        pass

    host = os.environ.get("FTP_HOST")
    user = os.environ.get("FTP_USER")
    password = os.environ.get("FTP_PASS")
    if not host or not user or not password:
        raise RuntimeError(
            "FTP credentials not found. Set FTP_HOST, FTP_USER, FTP_PASS "
            "environment variables or ensure app/config.py is available."
        )
    return host, user, password


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def decode_field_geometry(local_path: str) -> FieldGeometry:
    """Decode a BUFR file and extract first-sweep geometry."""
    from radarlib.io.bufr.bufr import bufr_to_dict

    fname = Path(local_path).name
    parts = fname.split("_")
    field_name = parts[3] if len(parts) >= 4 else "UNKNOWN"

    field_dict = bufr_to_dict(local_path, root_resources=None)
    if field_dict is None:
        raise ValueError(f"bufr_to_dict returned None for {local_path}")

    sweeps = field_dict["info"]["sweeps"]

    if len(sweeps) > 1 and (
        sweeps["gate_offset"].nunique() > 1
        or sweeps["gate_size"].nunique() > 1
        or sweeps["ngates"].nunique() > 1
    ):
        logger.warning(
            "%s: geometry varies across sweeps — reporting sweep 0 values only", fname
        )

    gate_offset = int(sweeps["gate_offset"].iloc[0])
    gate_size = int(sweeps["gate_size"].iloc[0])
    ngates = int(sweeps["ngates"].iloc[0])
    nrays = int(sweeps["nrayos"].iloc[0])

    return FieldGeometry(
        filename=fname,
        field_name=field_name,
        gate_offset=gate_offset,
        gate_size=gate_size,
        ngates=ngates,
        nrays=nrays,
        last_gate=gate_offset + gate_size * ngates,
    )


# ---------------------------------------------------------------------------
# Alignment simulation (mirrors pyart_writer.py logic exactly)
# ---------------------------------------------------------------------------


def find_reference(fields: List[FieldGeometry]) -> int:
    """Return index of field with largest last_gate — mirrors _find_reference_field."""
    max_last = -1
    max_idx = 0
    for i, f in enumerate(fields):
        if f.last_gate > max_last:
            max_last = f.last_gate
            max_idx = i
    return max_idx


def check_alignment(
    field: FieldGeometry,
    ref: FieldGeometry,
    is_reference: bool,
) -> AlignmentResult:
    """Simulate _align_field_to_reference and return pass/fail with diagnostics."""

    if field.gate_size != ref.gate_size:
        return AlignmentResult(
            field_name=field.field_name,
            filename=field.filename,
            is_reference=is_reference,
            init=None,
            passes=False,
            failure_reason=(
                f"gate_size mismatch: field={field.gate_size}m vs ref={ref.gate_size}m"
            ),
        )

    if field.gate_offset == ref.gate_offset:
        # Simple case: same offset, data placed from gate 0
        init = 0
        passes = field.ngates <= ref.ngates
        reason = (
            f"ngates overflow: field has {field.ngates} gates but ref only has {ref.ngates}"
            if not passes
            else None
        )
    else:
        init = int((field.gate_offset - ref.gate_offset) // ref.gate_size)
        if init < 0:
            passes = False
            reason = (
                f"init={init} < 0: field starts {abs(init) * ref.gate_size}m "
                f"BEFORE the reference grid "
                f"(field_offset={field.gate_offset}m < ref_offset={ref.gate_offset}m)"
            )
        elif init + field.ngates > ref.ngates:
            passes = False
            reason = (
                f"init={init} + ngates={field.ngates} = {init + field.ngates} "
                f"> ref_ngates={ref.ngates}: field overflows reference grid"
            )
        else:
            passes = True
            reason = None

    return AlignmentResult(
        field_name=field.field_name,
        filename=field.filename,
        is_reference=is_reference,
        init=init,
        passes=passes,
        failure_reason=reason,
    )


# ---------------------------------------------------------------------------
# FTP fetch helpers
# ---------------------------------------------------------------------------


def fetch_volume_from_ftp(
    radar: str,
    strategy: str,
    vol_nr: str,
    timestamp: datetime,
    download_dir: Path,
) -> List[str]:
    """
    Download all BUFR field files for the given radar volume from FTP.

    Returns a list of local file paths.
    """
    ftp_host, ftp_user, ftp_pass = resolve_ftp_credentials()
    from radarlib.io.ftp.ftp_client import RadarFTPClient

    # FTP path: /L2/{RADAR}/{YYYY}/{MM}/{DD}/{HH}/{MMSS}/
    ts = timestamp
    ftp_dir = (
        f"/L2/{radar}/{ts.year:04d}/{ts.month:02d}/{ts.day:02d}/"
        f"{ts.hour:02d}/{ts.minute:02d}{ts.second:02d}"
    )
    logger.info("Searching FTP directory: %s", ftp_dir)

    local_paths: List[str] = []

    with RadarFTPClient(host=ftp_host, user=ftp_user, password=ftp_pass) as client:
        try:
            files = client.ftp.nlst(ftp_dir)
        except Exception as e:
            raise RuntimeError(f"Failed to list FTP directory {ftp_dir}: {e}") from e

        bufr_files = [f for f in files if f.endswith(".BUFR") and f"_{vol_nr}_" in f.split("/")[-1]]
        if not bufr_files:
            raise RuntimeError(
                f"No BUFR files found in {ftp_dir} matching vol={vol_nr}"
            )

        logger.info("Found %d BUFR files, downloading...", len(bufr_files))
        for remote in bufr_files:
            fname = Path(remote).name
            local = download_dir / fname
            logger.info("Downloading %s", fname)
            client.download_file(remote, local)
            local_paths.append(str(local))

    return local_paths


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_report(
    geometries: List[FieldGeometry],
    alignments: List[AlignmentResult],
    ref_idx: int,
) -> None:
    ref = geometries[ref_idx]

    print()
    print("=" * 70)
    print("VOLUME ALIGNMENT DIAGNOSTIC")
    print("=" * 70)
    print()
    print(f"Reference field : {ref.field_name}  ({ref.filename})")
    print(f"  gate_offset   : {ref.gate_offset} m")
    print(f"  gate_size     : {ref.gate_size} m")
    print(f"  ngates        : {ref.ngates}")
    print(f"  last_gate     : {ref.last_gate} m")
    print()

    # ---- Geometry table ----
    c_fn = 8
    c_go = 13
    c_gs = 11
    c_ng = 8
    c_nr = 7
    c_lg = 12

    hdr = (
        f"{'FIELD':<{c_fn}}  "
        f"{'GATE_OFFSET':>{c_go}}  "
        f"{'GATE_SIZE':>{c_gs}}  "
        f"{'NGATES':>{c_ng}}  "
        f"{'NRAYS':>{c_nr}}  "
        f"{'LAST_GATE':>{c_lg}}"
    )
    print("--- Per-field geometry ---")
    print(hdr)
    print("-" * len(hdr))
    for g in geometries:
        tag = " [REF]" if g.field_name == ref.field_name else ""
        print(
            f"{g.field_name:<{c_fn}}  "
            f"{g.gate_offset:>{c_go}}  "
            f"{g.gate_size:>{c_gs}}  "
            f"{g.ngates:>{c_ng}}  "
            f"{g.nrays:>{c_nr}}  "
            f"{g.last_gate:>{c_lg}}"
            f"{tag}"
        )

    print()

    # ---- Alignment table ----
    c_fn2 = 8
    c_init = 8
    c_end = 12
    c_res = 6
    hdr2 = (
        f"{'FIELD':<{c_fn2}}  "
        f"{'INIT_GATE':>{c_init}}  "
        f"{'INIT+NGATES':>{c_end}}  "
        f"{'RESULT':<{c_res}}  "
        f"DETAIL"
    )
    print("--- Alignment check (simulating _align_field_to_reference) ---")
    print(hdr2)
    print("-" * 72)

    any_fail = False
    for a in alignments:
        init_str = str(a.init) if a.init is not None else "N/A"
        g = next(g for g in geometries if g.field_name == a.field_name)
        end_str = str(a.init + g.ngates) if a.init is not None else "N/A"
        result_str = "PASS" if a.passes else "FAIL"
        ref_tag = " [REF]" if a.is_reference else ""
        detail = a.failure_reason or ("reference field — no alignment needed" if a.is_reference else "OK")
        print(
            f"{a.field_name:<{c_fn2}}  "
            f"{init_str:>{c_init}}  "
            f"{end_str:>{c_end}}  "
            f"{result_str:<{c_res}}  "
            f"{detail}{ref_tag}"
        )
        if not a.passes:
            any_fail = True

    print()
    if any_fail:
        print("✗ ALIGNMENT WILL FAIL — one or more fields cannot be placed in the reference grid.")
        print()
        print("Likely cause: the reference field has a larger gate_offset than another field,")
        print("meaning that field starts closer to the radar than the reference grid begins.")
        print()
        print("Fix options:")
        print("  A) In _align_field_to_reference (pyart_writer.py:82-84): clamp init to 0")
        print("     and skip leading gates when init < 0 (drops near-range data before")
        print("     the reference grid start — usually noise-only range).")
        print("  B) In bufr_fields_to_pyart_radar: compute reference grid from min(gate_offset)")
        print("     across all fields so the grid covers every field from its own start.")
    else:
        print("✓ All fields can be aligned to the reference grid.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Diagnose cross-field gate alignment for a BUFR volume",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local files (provide all field paths explicitly)
  python3 scripts/diagnose_volume_alignment.py \\
      /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_DBZH_20260716T154443Z.BUFR \\
      /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_DBZV_20260716T154443Z.BUFR \\
      /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_ZDR_20260716T154443Z.BUFR \\
      /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_KDP_20260716T154443Z.BUFR \\
      /workspace/app/data/radares/RMA20/bufr/RMA20_0315_01_PHIDP_20260716T154443Z.BUFR

  # FTP fetch (downloads the volume first)
  python3 scripts/diagnose_volume_alignment.py \\
      --radar RMA20 --strategy 0315 --vol 01 \\
      --timestamp "2026-07-16 15:44:43"

  # Docker
  docker exec genpro25-rma20 python3 /workspace/scripts/diagnose_volume_alignment.py \\
      --radar RMA20 --strategy 0315 --vol 01 \\
      --timestamp "2026-07-16 15:44:43"
        """,
    )

    parser.add_argument(
        "bufr_paths",
        nargs="*",
        help="Local BUFR file paths (all fields of one volume). "
        "Mutually exclusive with --radar/--timestamp.",
    )
    parser.add_argument("--radar", help="Radar code for FTP fetch (e.g. RMA20)")
    parser.add_argument("--strategy", help="Strategy code (e.g. 0315)")
    parser.add_argument("--vol", default="01", help="Vol number (default: 01)")
    parser.add_argument(
        "--timestamp",
        help="Volume obs timestamp UTC for FTP fetch, format: YYYY-MM-DD HH:MM:SS",
    )
    parser.add_argument(
        "--download-dir",
        default="/tmp/diagnose_volume_alignment",
        help="Temp dir for FTP downloads (default: /tmp/diagnose_volume_alignment)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    local_paths: List[str] = []

    if args.bufr_paths:
        # Local mode
        for p in args.bufr_paths:
            if not Path(p).exists():
                print(f"✗ File not found: {p}", file=sys.stderr)
                sys.exit(1)
        local_paths = list(args.bufr_paths)
    elif args.radar and args.timestamp:
        # FTP mode
        try:
            ts = datetime.strptime(args.timestamp, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            print(
                f"✗ Invalid --timestamp format '{args.timestamp}' (use YYYY-MM-DD HH:MM:SS)",
                file=sys.stderr,
            )
            sys.exit(1)

        download_dir = Path(args.download_dir)
        download_dir.mkdir(parents=True, exist_ok=True)
        strategy = args.strategy or "0315"
        vol_nr = args.vol.zfill(2)

        try:
            local_paths = fetch_volume_from_ftp(
                radar=args.radar,
                strategy=strategy,
                vol_nr=vol_nr,
                timestamp=ts,
                download_dir=download_dir,
            )
        except Exception as e:
            print(f"✗ FTP fetch failed: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(
            "✗ Provide either local BUFR file paths or --radar + --timestamp for FTP fetch.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not local_paths:
        print("✗ No BUFR files to process.", file=sys.stderr)
        sys.exit(1)

    # Decode all fields
    geometries: List[FieldGeometry] = []
    for p in local_paths:
        fname = Path(p).name
        logger.info("Decoding %s", fname)
        try:
            g = decode_field_geometry(p)
            geometries.append(g)
        except Exception as e:
            logger.error("Failed to decode %s: %s", fname, e)
            geometries.append(
                FieldGeometry(
                    filename=fname,
                    field_name=fname.split("_")[3] if len(fname.split("_")) >= 4 else "UNKNOWN",
                    gate_offset=0,
                    gate_size=0,
                    ngates=0,
                    nrays=0,
                    last_gate=0,
                    decode_error=str(e),
                )
            )

    ok_geometries = [g for g in geometries if g.decode_error is None]
    if not ok_geometries:
        print("✗ All files failed to decode.", file=sys.stderr)
        sys.exit(1)

    if len(ok_geometries) < len(geometries):
        failed = [g.filename for g in geometries if g.decode_error]
        logger.warning("Skipping failed files: %s", ", ".join(failed))

    # Find reference and check alignment
    ref_idx = find_reference(ok_geometries)
    ref = ok_geometries[ref_idx]

    alignments: List[AlignmentResult] = []
    for i, g in enumerate(ok_geometries):
        alignments.append(check_alignment(g, ref, is_reference=(i == ref_idx)))

    print_report(ok_geometries, alignments, ref_idx)


if __name__ == "__main__":
    main()
