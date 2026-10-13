---
description: Inspect a radar's state.db — download frontier, backfill cursors, and queues.
argument-hint: <--db <path> | RADAR --root <ROOT_RADAR_FILES_PATH>>
---

Inspect the radar state database using the `radar-doctor` skill.

Run `python3 .claude/skills/radar-doctor/scripts/state_inspect.py` with the `--db <path>`
from `$ARGUMENTS` (or `--radar $1 --root <ROOT_RADAR_FILES_PATH>` to construct the path
`<root>/<RADAR>/state.db`). Add `--json` if asked.

Report:
- the **download frontier** (newest completed observation per volume, with age) — where
  the daemon's download pointer sits;
- the **backfill / backfill_origin cursors** (live-first build) — how far a backfill has
  progressed and the processing floor;
- the **download / processing / product queue counts** and `permanently_failed` — where
  work is piling up or stalling.

Opens the DB read-only. Pair with `analyze_log.py` (what the log shows) and `ftp_check.py`
(what's on the FTP) for a full picture.
