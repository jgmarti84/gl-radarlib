#!/usr/bin/env python3
"""
browse_ftp.py — Interactive FTP browser for the SINARAME radar data server.

Starts at /L2/ and lets you navigate the directory tree with simple commands.
Shows file sizes and dates parsed from the server's LIST response.
Optionally download individual files.

Commands inside the browser:
  ls [path]        List current directory (or a sub-path)
  cd <path>        Change directory (supports .. and absolute paths)
  pwd              Print current path
  get <file>       Download a file to the current local directory
  get <file> <dst> Download a file to a specific local path
  info <file>      Show size and last-modified time for a file
  find <pattern>   List files matching a glob pattern in the current subtree
  q / quit / exit  Quit

Credential resolution order (first match wins):
  1. --host / --user / --password CLI arguments
  2. app/config.py  (FTP_HOST, FTP_USER, FTP_PASS)
  3. Environment variables FTP_HOST, FTP_USER, FTP_PASS

Usage:
    python3 scripts/browse_ftp.py
    python3 scripts/browse_ftp.py --start /L2/RMA3
    python3 scripts/browse_ftp.py --host 200.16.116.24 --user USER --password PASS

Docker execution:
    docker exec genpro25-rma3 python3 /workspace/scripts/browse_ftp.py
    docker exec genpro25-rma3 python3 /workspace/scripts/browse_ftp.py --start /L2/RMA3
"""

import argparse
import ftplib
import logging
import os
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("browse_ftp")


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------


def resolve_credentials(
    host: Optional[str], user: Optional[str], password: Optional[str]
) -> Tuple[str, str, str]:
    if host and user and password:
        return host, user, password

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
        import config as app_config  # type: ignore

        h = getattr(app_config, "FTP_HOST", None)
        u = getattr(app_config, "FTP_USER", None)
        p = getattr(app_config, "FTP_PASS", None)
        if h and u and p:
            return h, u, p
    except Exception as e:
        logger.debug(f"Could not load app/config.py: {e}")

    h = os.environ.get("FTP_HOST") or host
    u = os.environ.get("FTP_USER") or user
    p = os.environ.get("FTP_PASS") or password

    if not h or not u or not p:
        raise RuntimeError(
            "FTP credentials not found.\n"
            "  Provide --host / --user / --password, set FTP_HOST / FTP_USER / FTP_PASS,\n"
            "  or run from inside a container where app/config.py is available."
        )
    return h, u, p  # type: ignore


# ---------------------------------------------------------------------------
# LIST line parser
# ---------------------------------------------------------------------------

# Matches Unix-style LIST lines:
#   drwxr-xr-x  2 user group    4096 Jan 15 10:30 name
#   -rw-r--r--  1 user group 8388608 Jul 17 2024  name.bufr
_LIST_RE = re.compile(
    r"^([d\-lrwxst]{10})\s+"   # permissions
    r"\d+\s+"                   # links
    r"\S+\s+"                   # owner
    r"\S+\s+"                   # group
    r"(\d+)\s+"                 # size
    r"(\w{3}\s+\d+\s+[\d:]+)\s+"  # date/time
    r"(.+)$"                    # name (may contain spaces)
)


def _parse_list_line(line: str) -> Optional[Dict]:
    m = _LIST_RE.match(line.strip())
    if not m:
        return None
    perms, size_str, date_str, name = m.groups()
    return {
        "name": name,
        "is_dir": perms.startswith("d"),
        "size": int(size_str),
        "date": date_str.strip(),
    }


def _list_dir(ftp: ftplib.FTP, path: str) -> List[Dict]:
    """Return parsed entries for *path* using LIST, fallback to bare NLST."""
    lines: List[str] = []
    try:
        ftp.retrlines(f"LIST {path}", lines.append)
    except ftplib.all_errors as e:
        logger.debug(f"LIST failed for {path}: {e}")
        # Fallback: NLST gives only names, no metadata
        try:
            names = ftp.nlst(path)
            return [{"name": Path(n).name, "is_dir": False, "size": 0, "date": ""} for n in names]
        except ftplib.all_errors:
            return []

    entries = []
    for line in lines:
        parsed = _parse_list_line(line)
        if parsed and parsed["name"] not in (".", ".."):
            entries.append(parsed)
    return entries


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_UNITS = ["B", "KB", "MB", "GB"]


def _human_size(n: int) -> str:
    val = float(n)
    for unit in _UNITS[:-1]:
        if val < 1024:
            return f"{val:.1f} {unit}"
        val /= 1024
    return f"{val:.1f} {_UNITS[-1]}"


# ---------------------------------------------------------------------------
# FTP session wrapper
# ---------------------------------------------------------------------------


class FTPBrowser:
    def __init__(self, host: str, user: str, password: str, start: str = "/L2"):
        self.host = host
        self.user = user
        self.password = password
        self.ftp: Optional[ftplib.FTP] = None
        self.cwd = start

    def connect(self) -> None:
        self.ftp = ftplib.FTP(self.host, timeout=30)
        self.ftp.login(self.user, self.password)
        self.ftp.set_pasv(True)
        try:
            self.ftp.cwd(self.cwd)
            self.cwd = self.ftp.pwd()
        except ftplib.error_perm:
            print(f"[warn] Could not cd to {self.cwd}, starting at /")
            self.cwd = "/"
        print(f"Connected to {self.host}  (cwd: {self.cwd})")

    def _ensure_connected(self) -> None:
        try:
            if self.ftp:
                self.ftp.voidcmd("NOOP")
                return
        except Exception:
            pass
        print("[reconnecting...]")
        self.connect()

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return str(PurePosixPath(path))
        return str(PurePosixPath(self.cwd) / path)

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def cmd_pwd(self) -> None:
        print(self.cwd)

    def cmd_ls(self, path: str = "") -> None:
        self._ensure_connected()
        target = self._resolve(path) if path else self.cwd

        entries = _list_dir(self.ftp, target)  # type: ignore[arg-type]

        if not entries:
            print("(empty)")
            return

        dirs = sorted([e for e in entries if e["is_dir"]], key=lambda x: x["name"])
        files = sorted([e for e in entries if not e["is_dir"]], key=lambda x: x["name"])

        col_name = max((len(e["name"]) for e in entries), default=20)
        col_name = max(col_name, 20)

        print(f"\n  {'NAME':<{col_name}}  {'SIZE':>10}  {'DATE'}")
        print(f"  {'-'*col_name}  {'-'*10}  {'-'*16}")
        for e in dirs:
            label = "[" + e["name"] + "]"
            print(f"  \033[34m{label:<{col_name}}\033[0m  {'':>10}  {e['date']}")
        for e in files:
            size_str = _human_size(e["size"]) if e["size"] else ""
            print(f"  {e['name']:<{col_name}}  {size_str:>10}  {e['date']}")

        print(f"\n  {len(dirs)} dir(s), {len(files)} file(s)  —  {target}")

    def cmd_cd(self, path: str) -> None:
        self._ensure_connected()
        target = self._resolve(path)
        try:
            self.ftp.cwd(target)  # type: ignore[union-attr]
            self.cwd = self.ftp.pwd()  # type: ignore[union-attr]
        except ftplib.error_perm as e:
            print(f"[error] {e}")

    def cmd_info(self, path: str) -> None:
        self._ensure_connected()
        target = self._resolve(path)

        # Get size via SIZE command
        try:
            size = self.ftp.size(target)  # type: ignore[union-attr]
            size_str = _human_size(size) if size is not None else "unknown"
        except Exception:
            size_str = "unknown"

        # Get mtime via MDTM
        try:
            mdtm = self.ftp.voidcmd(f"MDTM {target}").split()[-1]  # type: ignore[union-attr]
            mod_str = f"{mdtm[:4]}-{mdtm[4:6]}-{mdtm[6:8]} {mdtm[8:10]}:{mdtm[10:12]}" if len(mdtm) >= 12 else mdtm
        except Exception:
            mod_str = "unknown"

        print(f"  path : {target}")
        print(f"  size : {size_str}")
        print(f"  mtime: {mod_str}")

    def cmd_get(self, remote: str, local_path: Optional[str] = None) -> None:
        self._ensure_connected()
        remote_full = self._resolve(remote)
        dest = Path(local_path) if local_path else Path(".") / Path(remote_full).name

        try:
            size = self.ftp.size(remote_full)  # type: ignore[union-attr]
        except Exception:
            size = None

        size_str = f" ({_human_size(size)})" if size else ""
        print(f"Downloading {remote_full}{size_str} → {dest} ...")

        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(dest, "wb") as f:
                self.ftp.retrbinary(f"RETR {remote_full}", f.write)  # type: ignore[union-attr]
            actual = dest.stat().st_size
            print(f"Done. {_human_size(actual)} written to {dest}")
        except ftplib.error_perm as e:
            print(f"[error] {e}")
        except OSError as e:
            print(f"[error] Could not write file: {e}")

    def cmd_find(self, pattern: str) -> None:
        """Recursively list files matching a glob pattern under cwd."""
        self._ensure_connected()
        print(f"Searching for '{pattern}' under {self.cwd} ...")
        found: List[str] = []

        def _walk(path: str, depth: int = 0) -> None:
            if depth > 8:
                return
            for entry in _list_dir(self.ftp, path):  # type: ignore[arg-type]
                full = f"{path.rstrip('/')}/{entry['name']}"
                if entry["is_dir"]:
                    _walk(full, depth + 1)
                elif Path(entry["name"]).match(pattern):
                    size_str = _human_size(entry["size"]) if entry["size"] else ""
                    found.append(f"  {full:<70}  {size_str:>10}")

        _walk(self.cwd)
        if found:
            print(f"\n  {'PATH':<70}  {'SIZE':>10}")
            print(f"  {'-'*82}")
            for line in found:
                print(line)
            print(f"\n  {len(found)} file(s) found.")
        else:
            print("  No matches found.")

    def disconnect(self) -> None:
        try:
            if self.ftp:
                self.ftp.quit()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# REPL
# ---------------------------------------------------------------------------

HELP_TEXT = """
Commands:
  ls [path]         List directory (dirs shown in blue)
  cd <path>         Change directory (supports .., absolute paths)
  pwd               Print current path
  get <file> [dst]  Download file (default dest: current local dir)
  info <file>       Show size and modification time
  find <pattern>    Recursively search for files matching glob (e.g. *.bufr)
  help              Show this help
  q / quit / exit   Quit
"""


def run_repl(browser: FTPBrowser) -> None:
    print(HELP_TEXT)
    browser.cmd_ls()

    while True:
        try:
            raw = input(f"\nftp:{browser.cwd}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue

        parts = raw.split(None, 2)
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in ("q", "quit", "exit"):
            break
        elif cmd == "help":
            print(HELP_TEXT)
        elif cmd == "pwd":
            browser.cmd_pwd()
        elif cmd == "ls":
            browser.cmd_ls(args[0] if args else "")
        elif cmd == "cd":
            if not args:
                print("Usage: cd <path>")
            else:
                browser.cmd_cd(args[0])
                browser.cmd_ls()
        elif cmd == "get":
            if not args:
                print("Usage: get <remote_file> [local_path]")
            else:
                browser.cmd_get(args[0], args[1] if len(args) > 1 else None)
        elif cmd == "info":
            if not args:
                print("Usage: info <file>")
            else:
                browser.cmd_info(args[0])
        elif cmd == "find":
            if not args:
                print("Usage: find <pattern>  (e.g. find *.bufr)")
            else:
                browser.cmd_find(args[0])
        else:
            print(f"Unknown command: {cmd!r}  (type 'help' for a list)")

    browser.disconnect()
    print("Disconnected.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactive FTP browser for the SINARAME radar data server.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host", default=None, help="FTP host")
    parser.add_argument("--user", default=None, help="FTP username")
    parser.add_argument("--password", default=None, help="FTP password")
    parser.add_argument(
        "--start",
        default="/L2",
        metavar="PATH",
        help="Initial remote directory (default: /L2)",
    )
    args = parser.parse_args()

    host, user, password = resolve_credentials(args.host, args.user, args.password)
    browser = FTPBrowser(host, user, password, start=args.start)
    browser.connect()
    run_repl(browser)


if __name__ == "__main__":
    main()
