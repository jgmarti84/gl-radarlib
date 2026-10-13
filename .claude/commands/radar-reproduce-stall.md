---
description: Seed a SCRATCH state.db to simulate a stalled radar (for testing live+backfill recovery).
argument-hint: --db <scratch state.db> --radar <RADAR> --back <2d|6h> [--strategy ...] [--vols ...]
---

Simulate a stall for testing, using the `radar-doctor` skill.

Run `python3 .claude/skills/radar-doctor/scripts/reproduce_stall.py` with the `--db`,
`--radar`, and `--back` from `$ARGUMENTS` (plus `--strategy`/`--vols` for AR radars, e.g.
`--strategy 1000 --vols 1,2,3`). It sets the download frontier back and clears the daemon
cursors so, on container restart, the daemon must use both the live stream (current data)
and the backfill stream (old data).

SAFETY: this mutates the DB in place and refuses non-scratch paths unless `--force` — only
ever point it at a `run_radar` scratch state.db, never production.

Typical flow:
1. Start a container once: `/radar-run $RADAR --no-wait` (note its scratch dir).
2. `docker stop <container>`; seed the stall on `<scratch>/data/radares/<RADAR>/state.db`.
3. `docker start <container>`; watch `Live scan` (current) + `Backfill scan` (old) and use
   `/radar-state` to confirm the backfill cursor advances.
