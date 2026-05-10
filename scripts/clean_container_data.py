#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Container Data Cleaner - Maintenance Tool

Lists and optionally removes .BUFR and .nc files whose observation timestamp
(extracted from filename) is strictly before a given cutoff datetime.
Operates directly on the files inside the running genpro25-rmaX container,
targeting:

    /workspace/app/data/radares/<RADAR>/bufr/
    /workspace/app/data/radares/<RADAR>/netcdf/

Requires the target container to be running and `docker` to be available on
the host.

Usage:
    python3 scripts/clean_container_data.py \\
        --radar RMA1 \\
        --before "2026-05-07 12:00" \\
        [--dry-run]

    # --before accepts:
    #   "YYYY-MM-DD HH:MM"       (UTC assumed)
    #   "YYYY-MM-DD HH:MM:SS"
    #   "YYYY-MM-DDTHH:MM:SSZ"

    # All timestamps are treated as UTC.

Docker execution (from inside the devcontainer or host):
    python3 scripts/clean_container_data.py \\
        --radar RMA6 \\
        --before "2026-05-01 00:00" \\
        --dry-run

Examples:
    # Dry-run: preview what would be deleted for RMA1 before May 7 noon
    python3 scripts/clean_container_data.py --radar RMA1 --before "2026-05-07 12:00" --dry-run

    # Actually delete files for RMA6 before May 1
    python3 scripts/clean_container_data.py --radar RMA6 --before "2026-05-01 00:00"
"""

import argparse
import logging
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Logging — stderr so stdout stays clean
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("clean_container_data")

# ---------------------------------------------------------------------------
# Container context detection
# ---------------------------------------------------------------------------

_DOCKER_ENV_FILE = Path("/.dockerenv")
_CGROUP_FILE = Path("/proc/1/cgroup")


def _running_inside_container() -> bool:
    """
    Return True when this process is running inside a container
    (Docker or compatible runtime).

    Heuristics (any one is sufficient):
      - /.dockerenv exists  (Docker standard marker)
      - /proc/1/cgroup contains "docker" or "containerd"
    """
    if _DOCKER_ENV_FILE.exists():
        return True
    if _CGROUP_FILE.exists():
        try:
            content = _CGROUP_FILE.read_text(errors="replace")
            if "docker" in content or "containerd" in content:
                return True
        except OSError:
            pass
    return False


# ---------------------------------------------------------------------------
# Timestamp extraction
# ---------------------------------------------------------------------------

# Matches the standard radarlib timestamp token embedded in filenames:
#   20260423T070721Z  (YYYYMMDDTHHMMSSz)
_TIMESTAMP_RE = re.compile(r"(\d{8}T\d{6}Z)")

# Also handle the older / alternative format without the T/Z separators:
#   20260423070721   or  20260423_070721
_TIMESTAMP_RE_ALT = re.compile(r"(\d{8})[\s_T]?(\d{6})")


def extract_file_timestamp(filename: str) -> Optional[datetime]:
    """Return the UTC datetime encoded in a radarlib filename, or None."""
    m = _TIMESTAMP_RE.search(filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    # Fallback: two-group pattern
    m2 = _TIMESTAMP_RE_ALT.search(filename)
    if m2:
        try:
            raw = m2.group(1) + m2.group(2)
            return datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    return None


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class FileResult:
    """Represents one candidate file found inside the container."""

    container_path: str  # Full path inside the container
    filename: str
    file_type: str  # "bufr" | "netcdf"
    obs_time: Optional[datetime]  # None if timestamp could not be parsed
    will_delete: bool = False  # True if obs_time < cutoff
    deleted: bool = False
    error: str = ""


@dataclass
class ScanResult:
    """Aggregated outcome of a single folder scan."""

    folder: str
    file_type: str
    all_files: List[FileResult] = field(default_factory=list)
    skipped_no_ts: List[str] = field(default_factory=list)

    @property
    def candidates(self) -> List[FileResult]:
        return [f for f in self.all_files if f.will_delete]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def container_name(radar: str) -> str:
    """Derive container name from radar code, e.g. RMA1 → genpro25-rma1."""
    return f"genpro25-{radar.lower()}"


# — Local filesystem backend (used when running inside the container) --------


def _local_list_files(folder: str, extension: str) -> List[str]:
    """Return sorted full paths of *.{extension} files in folder (local fs)."""
    p = Path(folder)
    if not p.is_dir():
        logger.debug("Folder does not exist (local): %s", folder)
        return []
    return sorted(str(f) for f in p.iterdir() if f.is_file() and f.suffix.lower() == f".{extension.lower()}")


def _local_delete_file(path: str) -> Optional[str]:
    """Delete a file on the local filesystem. Returns error string or None."""
    try:
        Path(path).unlink()
        return None
    except OSError as exc:
        return str(exc)


# — docker exec backend (used when running on the host) ---------------------


def _docker_is_running(cname: str) -> bool:
    """Return True if the named container exists and is running."""
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", cname],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def _docker_list_files(cname: str, folder: str, extension: str) -> List[str]:
    """Return sorted full paths of matching files via docker exec find."""
    result = subprocess.run(
        ["docker", "exec", cname, "find", folder, "-maxdepth", "1", "-type", "f", "-name", f"*.{extension}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.debug(
            "find returned non-zero for %s:%s/*.%s — %s",
            cname,
            folder,
            extension,
            result.stderr.strip(),
        )
        return []
    return sorted(ln.strip() for ln in result.stdout.splitlines() if ln.strip())


def _docker_delete_file(cname: str, path: str) -> Optional[str]:
    """Delete a single file inside the container. Returns error string or None."""
    result = subprocess.run(
        ["docker", "exec", cname, "rm", "-f", path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return result.stderr.strip() or f"exit code {result.returncode}"
    return None


# — Unified dispatch ---------------------------------------------------------


def _list_files(inside: bool, cname: str, folder: str, extension: str) -> List[str]:
    return _local_list_files(folder, extension) if inside else _docker_list_files(cname, folder, extension)


def _delete_file(inside: bool, cname: str, path: str) -> Optional[str]:
    return _local_delete_file(path) if inside else _docker_delete_file(cname, path)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def scan_folder(
    inside: bool,
    cname: str,
    folder: str,
    file_type: str,
    extension: str,
    cutoff: datetime,
) -> ScanResult:
    """
    List all matching files in *folder*, filter by timestamp < cutoff,
    and return a ScanResult describing what was found.
    """
    result = ScanResult(folder=folder, file_type=file_type)
    paths = _list_files(inside, cname, folder, extension)

    for full_path in paths:
        filename = PurePosixPath(full_path).name
        obs_time = extract_file_timestamp(filename)

        if obs_time is None:
            logger.debug("Could not extract timestamp from '%s' — skipping", filename)
            result.skipped_no_ts.append(filename)
            continue

        fr = FileResult(
            container_path=full_path,
            filename=filename,
            file_type=file_type,
            obs_time=obs_time,
            will_delete=obs_time < cutoff,
        )
        result.all_files.append(fr)

    return result


def process_radar(
    radar: str,
    cutoff: datetime,
    dry_run: bool,
) -> Tuple[List[ScanResult], int, int]:
    """
    Scan both data folders for the radar, optionally delete candidate files.

    Auto-detects whether to use the local filesystem (inside container)
    or route through docker exec (running on host).

    Returns:
        (scan_results, total_candidates, total_deleted)
    """
    inside = _running_inside_container()
    cname = container_name(radar)

    if inside:
        logger.info("Running INSIDE container — using local filesystem directly.")
    else:
        logger.info("Running on HOST — routing operations through: docker exec %s", cname)
        if not _docker_is_running(cname):
            logger.error(
                "Container '%s' is not running. Start it before running this script.",
                cname,
            )
            sys.exit(1)

    base = f"/workspace/app/data/radares/{radar}"
    folders = [
        (f"{base}/bufr", "bufr", "BUFR"),
        (f"{base}/netcdf", "netcdf", "nc"),
    ]

    scan_results: List[ScanResult] = []
    total_candidates = 0
    total_deleted = 0

    for folder, file_type, ext in folders:
        if inside:
            logger.info("Scanning (local) %s  (ext=.%s)", folder, ext)
        else:
            logger.info("Scanning %s:%s  (ext=.%s)", cname, folder, ext)
        sr = scan_folder(inside, cname, folder, file_type, ext, cutoff)
        scan_results.append(sr)
        total_candidates += len(sr.candidates)

        if not dry_run:
            for fr in sr.candidates:
                err = _delete_file(inside, cname, fr.container_path)
                if err:
                    fr.error = err
                    logger.error("Failed to delete %s: %s", fr.container_path, err)
                else:
                    fr.deleted = True
                    total_deleted += 1

    return scan_results, total_candidates, total_deleted


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "???"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def print_report(
    radar: str,
    cutoff: datetime,
    scan_results: List[ScanResult],
    total_candidates: int,
    total_deleted: int,
    dry_run: bool,
) -> None:
    cname = container_name(radar)
    mode = "DRY-RUN" if dry_run else "LIVE"
    inside = _running_inside_container()
    context = "inside container (local fs)" if inside else f"host → docker exec {cname}"

    print()
    print("=" * 72)
    print(f"  Container Data Cleaner — {mode}")
    print("=" * 72)
    print(f"  Radar     : {radar}")
    print(f"  Container : {cname}")
    print(f"  Cutoff    : {_fmt_dt(cutoff)}  (files strictly BEFORE this time)")
    print(f"  Mode      : {'DRY-RUN  (no files were removed)' if dry_run else 'LIVE  (files were removed)'}")
    print(f"  Context   : {context}")
    print()

    for sr in scan_results:
        total_in_folder = len(sr.all_files) + len(sr.skipped_no_ts)
        print(f"  Folder : {sr.folder}")
        print(f"    Total files found    : {total_in_folder}")
        print(f"    Timestamp parseable  : {len(sr.all_files)}")
        print(f"    Skipped (no ts)      : {len(sr.skipped_no_ts)}")
        print(f"    Candidates (before cutoff): {len(sr.candidates)}")

        if sr.candidates:
            print()
            header = f"    {'Filename':<60} {'Obs time (UTC)':<22} {'Status'}"
            print(header)
            print("    " + "-" * (len(header) - 4))
            for fr in sr.candidates:
                if dry_run:
                    status = "would delete"
                elif fr.deleted:
                    status = "DELETED"
                else:
                    status = f"ERROR: {fr.error}"
                print(f"    {fr.filename:<60} {_fmt_dt(fr.obs_time):<22} {status}")

        if sr.skipped_no_ts:
            print()
            print("    Files skipped (no parseable timestamp):")
            for fname in sr.skipped_no_ts:
                print(f"      {fname}")

        print()

    print("-" * 72)
    if dry_run:
        print(f"  SUMMARY (DRY-RUN): {total_candidates} file(s) would be deleted.")
    else:
        errors = sum(1 for sr in scan_results for fr in sr.candidates if not fr.deleted)
        print(f"  SUMMARY: {total_candidates} candidate(s) found — " f"{total_deleted} deleted, {errors} error(s).")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
]


def parse_cutoff(value: str) -> datetime:
    """Parse the --before argument into an aware UTC datetime."""
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Cannot parse datetime '{value}'. "
        "Accepted formats: 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS', "
        "'YYYY-MM-DDTHH:MM:SSZ', 'YYYY-MM-DD'."
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Remove .BUFR and .nc files older than a given datetime from a " "running genpro25-rmaX container."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--radar",
        required=True,
        metavar="RADAR",
        help="Radar code, e.g. RMA1, RMA6, RMA17.",
    )
    parser.add_argument(
        "--before",
        required=True,
        type=parse_cutoff,
        metavar="DATETIME",
        help=(
            "Delete files whose timestamp is strictly before this UTC datetime. "
            "Formats: 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS', "
            "'YYYY-MM-DDTHH:MM:SSZ', 'YYYY-MM-DD'."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help=(
            "List candidate files and print a summary without deleting anything. "
            "Omit this flag (or set it to false) to perform actual deletion."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    # Apply log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    radar = args.radar.upper()
    cutoff: datetime = args.before
    dry_run: bool = args.dry_run

    logger.info(
        "Starting clean_container_data — radar=%s  cutoff=%s  dry_run=%s",
        radar,
        _fmt_dt(cutoff),
        dry_run,
    )

    scan_results, total_candidates, total_deleted = process_radar(
        radar=radar,
        cutoff=cutoff,
        dry_run=dry_run,
    )

    print_report(
        radar=radar,
        cutoff=cutoff,
        scan_results=scan_results,
        total_candidates=total_candidates,
        total_deleted=total_deleted,
        dry_run=dry_run,
    )

    # Exit 0 always — even in dry-run, a non-zero exit would be misleading
    sys.exit(0)


if __name__ == "__main__":
    main()
