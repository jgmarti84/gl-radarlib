#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sweep.py — diagnose a whole fleet of radars from a directory of genpro25 logs.

Runs analyze_log.py on every matching log, ranks by severity, and (with --ftp)
cross-checks the stalled ones via ftp_check.py to separate "daemon stuck" from
"radar/source down". Stdlib only; shells out to the sibling scripts.

Usage:
    python3 sweep.py --logs-dir .
    python3 sweep.py --logs-dir /data/logs --ftp --reference RMA12
"""
import argparse
import glob
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RANK = {"STUCK": 3, "DEGRADED": 2, "CATCHING-UP / LAGGING": 2, "NO-DATA": 2, "HEALTHY": 0}
STALL_MODES = {"download-gap-ratchet", "ar-vol-padding-mismatch", "download-stalled"}


def run_json(script, *args):
    r = subprocess.run([sys.executable, "-I", os.path.join(HERE, script), *args],
                       capture_output=True, text=True)
    try:
        return json.loads(r.stdout)
    except (json.JSONDecodeError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs-dir", required=True)
    ap.add_argument("--pattern", default="genpro25-*.log*")
    ap.add_argument("--ftp", action="store_true", help="Cross-check stalled radars against the FTP.")
    ap.add_argument("--reference", default="RMA12")
    ap.add_argument("--config", default="genpro25.yml")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    logs = sorted(glob.glob(os.path.join(a.logs_dir, a.pattern)))
    if not logs:
        sys.exit(f"no logs matching {a.pattern} in {a.logs_dir}")

    fleet = []
    for log in logs:
        d = run_json("analyze_log.py", "--log", log, "--json")
        if not d:
            fleet.append(dict(radar="?", log=os.path.basename(log), status="PARSE-ERROR",
                              findings=[], lag=None, ftp=None))
            continue
        modes = [f["mode"] for f in d.get("findings", [])]
        stalled = any(m in STALL_MODES for m in modes)
        ftp = None
        if a.ftp and stalled and d.get("radar"):
            ref = None if d["radar"] == a.reference else a.reference
            fargs = ["--radar", d["radar"], "--config", a.config, "--json"]
            if ref:
                fargs += ["--reference", ref]
            fj = run_json("ftp_check.py", *fargs)
            if fj:
                t = fj.get("target", {})
                refr = fj.get("reference") or {}
                ftp = dict(radar_verdict=t.get("verdict"), age_min=t.get("age_min"),
                           source_down=(t.get("verdict") in ("STALE/DOWN", "NO-DATA")
                                        and (refr.get("verdict") == "CURRENT" if refr else None)))
        fleet.append(dict(radar=d.get("radar") or os.path.basename(log),
                          log=os.path.basename(log), status=d.get("status"),
                          lag=d.get("product_lag_minutes"),
                          modes=modes, ftp=ftp))

    fleet.sort(key=lambda r: (RANK.get(r["status"], 1), r.get("lag") or 0), reverse=True)

    if a.json:
        print(json.dumps(fleet, indent=2))
        return 0

    print(f"{'='*84}\n  RADAR FLEET SWEEP — {len(fleet)} radars from {a.logs_dir}\n{'='*84}")
    print(f"  {'RADAR':7} {'STATUS':14} {'LAG(min)':8}  FINDINGS / FTP")
    print(f"  {'-'*78}")
    for r in fleet:
        icon = {"STUCK": "🔴", "DEGRADED": "🟠", "CATCHING-UP / LAGGING": "🟡",
                "HEALTHY": "🟢"}.get(r["status"], "•")
        modes = ",".join(m for m in r.get("modes", []) if m != "benign-filter-fields") or "-"
        ftp = ""
        if r.get("ftp"):
            f = r["ftp"]
            if f.get("source_down"):
                ftp = f"  | FTP: radar DOWN ~{f['age_min']}min (source outage)"
            elif f.get("radar_verdict") == "CURRENT":
                ftp = "  | FTP: radar CURRENT (daemon stall)"
            elif f.get("radar_verdict"):
                ftp = f"  | FTP: {f['radar_verdict']}"
        lag = r["lag"] if r["lag"] is not None else "-"
        print(f"  {icon} {r['radar']:5} {r['status']:14} {str(lag):8}  {modes}{ftp}")
    print(f"{'='*84}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
