#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ftp_check.py — what is actually on the radar FTP right now.

Answers: is the radar still producing, or did the source stop? Finds the newest
BUFR per volume, reports its age, and (with --reference) compares against another
radar to prove the FTP itself is alive. Stdlib only (ftplib) — no container, no
radarlib import. Credentials: --host/--user/--password, else env
FTP_HOST/FTP_USER/FTP_PASS, else parsed from genpro25.yml.

Usage:
    python3 ftp_check.py --radar RMA13 --reference RMA12
    python3 ftp_check.py --radar RMA13 --file RMA13_0315_02_VRAD_20261012T160638Z.BUFR
    python3 ftp_check.py --radar RMA13 --json

Exit: 0 current, 1 lagging, 2 stale/down.
"""
import argparse
import ftplib
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

FNAME_RE = re.compile(r"^([A-Z0-9]+)_(\d{4})_(\d{1,2})_([A-Z]+)_(\d{8}T\d{6}Z)\.BUFR$", re.I)


def load_creds(args):
    host, user, pw = args.host, args.user, args.password
    host = host or os.environ.get("FTP_HOST")
    user = user or os.environ.get("FTP_USER")
    pw = pw or os.environ.get("FTP_PASS")
    if (not host or not user or not pw) and os.path.exists(args.config):
        txt = open(args.config, "r", errors="replace").read()
        def grab(key):
            m = re.search(key + r'\s*:\s*"?([^"\n]+?)"?\s*(?:#.*)?$', txt, re.M)
            return m.group(1).strip() if m else None
        host = host or grab("FTP_HOST")
        user = user or grab("FTP_USER")
        pw = pw or grab("FTP_PASS")
    if not (host and user and pw):
        sys.exit("FTP credentials not found (use --host/--user/--password, env, or genpro25.yml)")
    return host, user, pw


class FTP:
    def __init__(self, host, user, pw, timeout=60):
        self.ftp = ftplib.FTP(host, timeout=timeout)
        self.ftp.login(user, pw)
        self.ftp.voidcmd("TYPE I")

    def ls(self, path):
        try:
            self.ftp.cwd(path)
            return self.ftp.nlst()
        except ftplib.all_errors:
            return []

    def size(self, path):
        try:
            return self.ftp.size(path)
        except ftplib.all_errors:
            return None

    def close(self):
        try:
            self.ftp.quit()
        except ftplib.all_errors:
            pass


def _obs_dt(ts):
    return datetime.strptime(ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def collect_recent(ftp, radar, now, days_back=3, max_folders=12):
    """Return (folders, files) for the most recent minute-folders that exist.

    folders: list of (dt, "HH:MM:SS", [vol_nrs]) newest-first.
    files:   list of (obs_dt, strategy, vol, field) across those folders.
    """
    folders, files = [], []
    day = now.date()
    for _ in range(days_back + 1):
        base = f"/L2/{radar}/{day.year:04d}/{day.month:02d}/{day.day:02d}"
        hours = sorted((h for h in ftp.ls(base) if h.isdigit()), reverse=True)
        for hh in hours:
            mins = sorted((m for m in ftp.ls(f"{base}/{hh}") if m.isdigit()), reverse=True)
            for ms in mins:
                mi, se = int(ms[:2]), int(ms[2:]) if len(ms) > 2 else 0
                fdt = datetime(day.year, day.month, day.day, int(hh), mi, se, tzinfo=timezone.utc)
                names = ftp.ls(f"{base}/{hh}/{ms}")
                vols = set()
                for n in names:
                    m = FNAME_RE.match(n.split("/")[-1])
                    if m:
                        vols.add(m.group(3))
                        files.append((_obs_dt(m.group(5)), m.group(2), m.group(3), m.group(4)))
                folders.append((fdt, f"{int(hh):02d}:{mi:02d}:{se:02d}", sorted(vols)))
                if len(folders) >= max_folders:
                    return folders, files
        day = day - timedelta(days=1)
    return folders, files


def analyze(ftp, radar, now, stale_min):
    folders, files = collect_recent(ftp, radar, now)
    if not files:
        return dict(radar=radar, newest=None, age_min=None, verdict="NO-DATA",
                    per_vol={}, folders=[])
    newest = max(f[0] for f in files)
    age_min = round((now - newest).total_seconds() / 60)
    per_vol = {}
    for obs, _strat, vol, _field in files:
        if vol not in per_vol or obs > per_vol[vol]:
            per_vol[vol] = obs
    if age_min <= stale_min:
        verdict = "CURRENT"
    elif age_min <= 60:
        verdict = "LAGGING"
    else:
        verdict = "STALE/DOWN"
    return dict(radar=radar, newest=newest, age_min=age_min, verdict=verdict,
                per_vol={k: v.isoformat() for k, v in sorted(per_vol.items())},
                folders=[(t, v) for _dt, t, v in folders])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--radar", required=True)
    ap.add_argument("--reference", default=None, help="Another radar to prove the FTP is alive.")
    ap.add_argument("--file", default=None, help="Confirm one BUFR filename exists (else 550).")
    ap.add_argument("--config", default="genpro25.yml")
    ap.add_argument("--host"); ap.add_argument("--user"); ap.add_argument("--password")
    ap.add_argument("--stale-min", type=int, default=20)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    host, user, pw = load_creds(a)
    now = datetime.now(timezone.utc)
    ftp = FTP(host, user, pw)
    try:
        res = analyze(ftp, a.radar, now, a.stale_min)
        ref = analyze(ftp, a.reference, now, a.stale_min) if a.reference else None
        file_hit = None
        if a.file:
            m = FNAME_RE.match(a.file)
            if m:
                ts = m.group(5)
                d = _obs_dt(ts)
                path = (f"/L2/{a.radar}/{d.year:04d}/{d.month:02d}/{d.day:02d}/"
                        f"{d.hour:02d}/{d.minute:02d}{d.second:02d}/{a.file}")
                file_hit = dict(file=a.file, path=path, exists=ftp.size(path) is not None)
    finally:
        ftp.close()

    code = {"CURRENT": 0, "LAGGING": 1, "NO-DATA": 2, "STALE/DOWN": 2}.get(res["verdict"], 2)

    if a.json:
        out = dict(now=now.isoformat(), target=res, reference=ref, file=file_hit)
        print(json.dumps(out, indent=2, default=str))
        return code

    def line(r):
        n = r["newest"].isoformat() if r["newest"] else "—"
        return f"{r['radar']:7s} {r['verdict']:11s} newest={n}  age={r['age_min']}min"
    print(f"{'='*72}\n  FTP CHECK @ {now.isoformat()}\n{'='*72}")
    print("  " + line(res))
    for v, t in res["per_vol"].items():
        print(f"      vol{v}: {t}")
    if res["folders"]:
        print("  recent folders (newest first): " +
              ", ".join(f"{t}[{'+'.join(v) or '-'}]" for t, v in res["folders"][:8]))
    if ref:
        print("  " + line(ref) + "   (reference)")
        if ref["verdict"] == "CURRENT" and res["verdict"] in ("STALE/DOWN", "NO-DATA"):
            print(f"  → FTP is alive ({a.reference} current) but {a.radar} stopped producing "
                  f"~{res['age_min']} min ago → RADAR/SOURCE outage, not a daemon stall.")
        elif res["verdict"] == "CURRENT":
            print(f"  → {a.radar} is producing; if the daemon isn't downloading, the stall is in the daemon.")
    if file_hit:
        print(f"  file {file_hit['file']}: {'EXISTS' if file_hit['exists'] else '550 / MISSING'}")
    print(f"{'='*72}")
    return code


if __name__ == "__main__":
    sys.exit(main())
