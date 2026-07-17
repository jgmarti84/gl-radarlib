#!/usr/bin/env python3
"""
browse_ftp.py — Interactive FTP browser for the SINARAME radar data server.

Starts at /L2/ and lets you navigate the directory tree with simple commands.
Understands the SINARAME folder structure and shows file sizes in human-readable
form. Optionally download individual files.

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
import sys
from pathlib import Path, PurePosixPath
from typing import List, Optional, Tuple

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


def _parse_mlsd_entry(name: str, facts: dict) -> dict:
    return {
        "name": name,
        "type": facts.get("type", "?"),
        "size": int(facts["size"]) if "size" in facts else None,
        "modify": facts.get("modify", ""),
    }


def _fmt_modify(modify: str) -> str:
    # MLSD modify format: YYYYMMDDHHmmss
    if len(modify) >= 12:
        return f"{modify[:4]}-{modify[4:6]}-{modify[6:8]} {modify[8:10]}:{modify[10:12]}"
    return modify


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

        entries = []
        try:
            for name, facts in self.ftp.mlsd(target):  # type: ignore[union-attr]
                if name in (".", ".."):
                    continue
                entries.append(_parse_mlsd_entry(name, facts))
        except ftplib.error_perm as e:
            print(f"[error] {e}")
            return
        except Exception:
            # MLSD not supported — fall back to NLST
            try:
                names = self.ftp.nlst(target)  # type: ignore[union-attr]
                entries = [{"name": Path(n).name, "type": "?", "size": None, "modify": ""} for n in sorted(names)]
            except ftplib.error_perm as e:
                print(f"[error] {e}")
                return

        if not entries:
            print("(empty)")
            return

        dirs = sorted([e for e in entries if e["type"] == "dir"], key=lambda x: x["name"])
        files = sorted([e for e in entries if e["type"] != "dir"], key=lambda x: x["name"])

        col_name = max((len(e["name"]) for e in entries), default=20)
        col_name = max(col_name, 20)

        print(f"\n  {'NAME':<{col_name}}  {'SIZE':>10}  {'MODIFIED'}")
        print(f"  {'-'*col_name}  {'-'*10}  {'-'*16}")
        for e in dirs:
            print(f"  \033[34m{'[' + e['name'] + ']':<{col_name}}\033[0m  {'':>10}  {_fmt_modify(e['modify'])}")
        for e in files:
            size_str = _human_size(e["size"]) if e["size"] is not None else ""
            print(f"  {e['name']:<{col_name}}  {size_str:>10}  {_fmt_modify(e['modify'])}")

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
        try:
            size = self.ftp.size(target)  # type: ignore[union-attr]
            size_str = _human_size(size) if size is not None else "unknown"
        except Exception:
            size_str = "unknown"

        try:
            mdtm = self.ftp.voidcmd(f"MDTM {target}").split()[-1]  # type: ignore[union-attr]
            mod_str = _fmt_modify(mdtm)
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
            try:
                entries = list(self.ftp.mlsd(path))  # type: ignore[union-attr]
            except Exception:
                try:
                    names = self.ftp.nlst(path)  # type: ignore[union-attr]
                    entries = [(Path(n).name, {"type": "file"}) for n in names]
                except Exception:
                    return

            for name, facts in entries:
                if name in (".", ".."):
                    continue
                full = f"{path.rstrip('/')}/{name}"
                ftype = facts.get("type", "file")
                if ftype == "dir":
                    _walk(full, depth + 1)
                else:
                    if Path(name).match(pattern):
                        size = int(facts["size"]) if "size" in facts else None
                        size_str = _human_size(size) if size else ""
                        found.append(f"  {full}  {size_str}")

        _walk(self.cwd)
        if found:
            print(f"\n  {'PATH + SIZE'}")
            print(f"  {'-'*60}")
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
