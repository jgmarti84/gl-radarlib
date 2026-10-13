#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""state_inspect.py — what the daemon thinks (from a radar's state.db).

Shows the per-volume download frontier, the backfill/backfill_origin cursors, and
the download/processing/product queue so you can see where the daemon is stuck or
how far a backfill has progressed. Stdlib only (sqlite3); opens read-only.

Usage:
    python3 state_inspect.py --db <.../RMAXX/state.db>
    python3 state_inspect.py --radar RMA12 --root <ROOT_RADAR_FILES_PATH> --json
"""
import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone


def q(cur, sql, params=()):
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except sqlite3.Error:
        return None


def table_exists(cur, name):
    return bool(q(cur, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)))


def inspect(db_path):
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = con.cursor()
    out = {"db": db_path, "tables": {}}

    # Per-volume download frontier (newest completed observation per vol).
    frontier = {}
    rows = q(cur, "SELECT vol_nr, MAX(observation_datetime) FROM downloads "
                  "WHERE status='completed' GROUP BY vol_nr ORDER BY vol_nr")
    if rows:
        frontier = {v: t for v, t in rows if v is not None}
    out["download_frontier"] = frontier

    # Download status counts + permanently-failed.
    out["downloads"] = {}
    rows = q(cur, "SELECT status, COUNT(*) FROM downloads GROUP BY status")
    if rows:
        out["downloads"]["by_status"] = {s: n for s, n in rows}
    pf = q(cur, "SELECT COUNT(*) FROM downloads WHERE permanently_failed=1")
    out["downloads"]["permanently_failed"] = pf[0][0] if pf else None

    # Daemon cursors (backfill / backfill_origin / anything else).
    cursors = {}
    if table_exists(cur, "daemon_cursor"):
        rows = q(cur, "SELECT radar_name, name, value FROM daemon_cursor ORDER BY name")
        if rows:
            cursors = {f"{r}:{n}": v for r, n, v in rows}
    out["daemon_cursors"] = cursors

    # Volume processing queue.
    vp = {}
    rows = q(cur, "SELECT status, COUNT(*) FROM volume_processing GROUP BY status")
    if rows:
        vp["by_status"] = {s: n for s, n in rows}
    r = q(cur, "SELECT MAX(observation_datetime) FROM volume_processing WHERE status='completed'")
    vp["latest_completed"] = r[0][0] if r else None
    r = q(cur, "SELECT COUNT(*) FROM volume_processing WHERE is_complete=0 AND status!='completed'")
    vp["incomplete_pending"] = r[0][0] if r else None
    out["volume_processing"] = vp

    # Product generation queue.
    pg = {}
    rows = q(cur, "SELECT status, COUNT(*) FROM product_generation GROUP BY status")
    if rows:
        pg["by_status"] = {s: n for s, n in rows}
    out["product_generation"] = pg

    con.close()
    return out


def _age(iso, now):
    try:
        d = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return round((now - d).total_seconds() / 60)
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--radar", default=None)
    ap.add_argument("--root", default=os.environ.get("ROOT_RADAR_FILES_PATH", ""))
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    db = a.db
    if not db and a.radar and a.root:
        db = os.path.join(a.root, a.radar, "state.db")
    if not db:
        sys.exit("Provide --db <path> (or --radar + --root).")
    if not os.path.exists(db):
        sys.exit(f"state.db not found: {db}")

    out = inspect(db)
    now = datetime.now(timezone.utc)

    if a.json:
        print(json.dumps(out, indent=2, default=str))
        return 0

    print(f"{'='*72}\n  RADAR STATE — {db}\n{'='*72}")
    print("  Download frontier (newest completed obs per volume):")
    if out["download_frontier"]:
        for v, t in out["download_frontier"].items():
            age = _age(t, now)
            print(f"      vol{v}: {t}" + (f"   ({age} min old)" if age is not None else ""))
    else:
        print("      (none)")
    print("  Daemon cursors:")
    if out["daemon_cursors"]:
        for k, v in out["daemon_cursors"].items():
            print(f"      {k} = {v}")
    else:
        print("      (none — older build, or no backfill active)")
    d = out["downloads"]
    print(f"  Downloads   : {d.get('by_status', {})}  permanently_failed={d.get('permanently_failed')}")
    vp = out["volume_processing"]
    print(f"  Processing  : {vp.get('by_status', {})}  latest_completed={vp.get('latest_completed')}"
          f"  incomplete_pending={vp.get('incomplete_pending')}")
    pg = out["product_generation"]
    print(f"  Products    : {pg.get('by_status', {})}")
    print(f"{'='*72}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
