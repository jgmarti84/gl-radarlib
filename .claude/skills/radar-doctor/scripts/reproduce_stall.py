#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""reproduce_stall.py — seed a SCRATCH state.db to simulate a stalled radar.

Sets the download frontier back in time and clears the daemon cursors, so when the
container restarts it must use both the live stream (current data) and the backfill
stream (old data) — the scenario to verify the live-first behaviour.

SAFETY: mutates the database in place. Refuses paths that don't look like scratch
(must contain 'scratch', 'tmp', or 'test') unless --force. Never point this at a
production state.db.

Usage:
    python3 reproduce_stall.py --db <scratch>/data/radares/RMA12/state.db \
        --radar RMA12 --back 2d
    python3 reproduce_stall.py --db ... --radar AR5 --strategy 1000 --vols 1,2,3 --back 2d
"""
import argparse
import re
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

DUR_RE = re.compile(r"^(\d+)([dhm])$")
DEFAULT_FIELDS = {"01": "DBZH", "02": "VRAD", "03": "DBZH", "04": "DBZH",
                  "1": "DBZH", "2": "VRAD", "3": "DBZH"}


def parse_back(s):
    m = DUR_RE.match(s.strip())
    if not m:
        sys.exit("--back must look like 2d / 6h / 90m")
    n, unit = int(m.group(1)), m.group(2)
    return timedelta(days=n) if unit == "d" else timedelta(hours=n) if unit == "h" else timedelta(minutes=n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--radar", required=True)
    ap.add_argument("--back", required=True, help="How far to set the frontier back: 2d / 6h / 90m")
    ap.add_argument("--strategy", default="0315")
    ap.add_argument("--vols", default="01,02,04", help="Comma list, e.g. 01,02,04 (RMA) or 1,2,3 (AR)")
    ap.add_argument("--no-wipe", action="store_true", help="Keep existing rows (default wipes state tables)")
    ap.add_argument("--force", action="store_true", help="Bypass the scratch-path safety check")
    a = ap.parse_args()

    low = a.db.lower()
    if not any(tok in low for tok in ("scratch", "tmp", "test", "temp")) and not a.force:
        sys.exit(f"Refusing to mutate '{a.db}' — path doesn't look like scratch. Use --force to override.")

    frontier = (datetime.now(timezone.utc) - parse_back(a.back)).replace(microsecond=0)
    iso = frontier.isoformat()
    ts = frontier.strftime("%Y%m%dT%H%M%SZ")
    vols = [v.strip() for v in a.vols.split(",") if v.strip()]

    con = sqlite3.connect(a.db)
    cur = con.cursor()
    if not a.no_wipe:
        for t in ("downloads", "volume_processing", "product_generation", "daemon_cursor"):
            try:
                cur.execute(f"DELETE FROM {t}")
            except sqlite3.Error:
                pass
    else:
        try:
            cur.execute("DELETE FROM daemon_cursor")  # always clear cursors so backfill re-inits
        except sqlite3.Error:
            pass

    for vol in vols:
        field = DEFAULT_FIELDS.get(vol, "DBZH")
        fname = f"{a.radar}_{a.strategy}_{vol}_{field}_{ts}.BUFR"
        try:
            cur.execute(
                """INSERT OR REPLACE INTO downloads
                (filename,remote_path,local_path,downloaded_at,file_size,checksum,radar_name,
                 strategy,vol_nr,field_type,observation_datetime,status,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (fname, f"/L2/{a.radar}/x/{fname}", f"/tmp/{fname}", iso, 100, None, a.radar,
                 a.strategy, vol.zfill(2) if vol.isdigit() and len(vol) == 1 else vol,
                 field, iso, "completed", iso, iso))
        except sqlite3.Error as e:
            sys.exit(f"insert failed (schema mismatch?): {e}")
    con.commit()

    print(f"Stall seeded in {a.db}")
    print(f"  frontier set to {iso}  ({a.back} ago)")
    rows = cur.execute("SELECT vol_nr, MAX(observation_datetime) FROM downloads "
                       "WHERE status='completed' GROUP BY vol_nr").fetchall()
    for v, t in rows:
        print(f"  vol{v}: {t}")
    print("  daemon_cursor cleared → backfill will re-init to this frontier on restart.")
    print("  Next: restart the container; watch for 'Live scan' (current) + 'Backfill scan' (old).")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
