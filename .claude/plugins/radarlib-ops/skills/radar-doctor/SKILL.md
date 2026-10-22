---
name: radar-doctor
description: >-
  Diagnose and operate a radarlib (genpro25) radar. Use when asked why a radar is
  stuck / not producing GeoTIFFs/COGs, to analyze a genpro25 log, to check what
  data is actually on the radar FTP, to inspect a radar's state.db, or to spin up
  a genpro25 container to reproduce a stall or verify a change. Triggers: "why is
  RMAxx stuck", "radar not producing tiffs", "analyze genpro25 log", "check the
  radar ftp", "is the radar producing data", "reproduce the stall", "run a genpro25
  container".
---

# Radar Doctor

A toolkit for diagnosing and operating radarlib radars. Work in layers: classify
the **log**, confirm against the **FTP**, inspect the **state.db**, and when a fix
needs proving, reproduce/verify in a **container**. All scripts live in `scripts/`
and take `--json` for composition. FTP credentials are always read from
`genpro25.yml` — never hardcode them.

Read `references/failure_modes.md` for the fingerprint catalog and
`references/pipeline_map.md` for the daemons, state.db schema, cursors, config
knobs, and FTP layout.

## Decision tree

1. **Always start with the log** (offline, no creds):
   `python scripts/analyze_log.py --log <genpro25-rmaXX.log> [--json]`
   It reconstructs the timeline, traces the download pointer, and classifies the
   failure mode. Relay its verdict + findings; each finding names the fix/branch.

2. **If a download stall is reported** (`download-gap-ratchet`,
   `ar-vol-padding-mismatch`, `download-stalled`): confirm the radar itself is
   still producing (so it's the daemon, not the source):
   `python scripts/ftp_check.py --radar RMAXX --lookback 6h`
   - Fresh data after the freeze time ⇒ daemon stuck, not the radar.
   - Use `--gap <hourA-hourB>` to pinpoint an acquisition gap, `--file <name>` to
     confirm a specific 550.

3. **If the daemon's view is in question** (frozen pointer, backfill, stuck
   volumes): inspect state:
   `python scripts/state_inspect.py --db <.../RMAXX/state.db>`
   Shows per-volume frontier, the `backfill`/`backfill_origin` cursors, and the
   download/processing/product queue counts.

4. **To prove a fix or reproduce a stall** (uses Docker + live FTP, scratch dirs
   only — never touches a production state.db):
   - Live smoke test: `python scripts/run_radar.py --radar RMAXX --minutes 15`
   - Reproduce a stall: seed a scratch state.db with
     `python scripts/reproduce_stall.py --db <scratch state.db> --back 2d`
     then restart the container and watch live + backfill streams.

## Always review raised Python errors

`analyze_log.py` always prints a **Raised errors** census (traceback count, exception
types, and the top normalized ERROR messages) — and emits an `unclassified-errors`
finding when non-benign tracebacks aren't explained by a known fingerprint. Always read
it: a radar can look "stuck/stalled" at the pointer level while the real story is an
exception raised deep in processing/product generation. Report the top raised errors with
their counts, and say which are benign (e.g. `FilterFieldsMissingError`) vs real. If the
census shows an exception type the known modes don't cover, surface it explicitly rather
than forcing it into an existing mode.

## Output

Report the verdict (HEALTHY / CATCHING-UP / DEGRADED / STUCK), the matched
findings with their evidence, the **raised-error census**, and the recommended action.
Keep it tight; cite the concrete numbers (frozen-at time, lag minutes, 550 counts,
traceback/exception counts) the scripts return.

## Known failure modes (see references/failure_modes.md)

- `download-gap-ratchet` — resume pointer frozen after a >15-min data gap.
- `ar-vol-padding-mismatch` — AR radars: SIZE detector builds zero-padded names.
- `poison-incomplete-volume` — 550-missing field never abandoned (retry counter no-op).
- `downloading-not-producing` — data downloaded but not processed (processing floor).
- `pyart-shape-mismatch` — fields in a scan have inconsistent ray counts.
- `download-stalled` — generic stall (older builds that don't log the resume pointer).
- `unclassified-errors` — non-benign tracebacks no known fingerprint explains; investigate.
