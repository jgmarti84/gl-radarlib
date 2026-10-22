# Radar failure-mode catalog

Each mode: **log fingerprint** → **FTP confirmation** → **root cause** → **fix**.
All observed on 2026-10-10..12 (RMA12/RMA17/AR5/RMA10/RMA13).

## 1. download-gap-ratchet  (STUCK)
**Fingerprint:** `Resuming from newest ... minus 15 min: <T>` repeats with the *same*
`<T>` for many consecutive cycles; `Backlog detected — capping ...`; `No new files`.
Products stop shortly after the freeze; downloads stop at the frozen time.
**FTP check:** `ftp_check --radar RMAXX --lookback 6h` shows data *after* the freeze
time → radar healthy, daemon frozen. A `--gap` probe shows a >15-min acquisition gap
just after the last downloaded scan.
**Root cause:** resume = `max(downloaded)−15min`, window capped to `+30min`, so forward
reach is only ~15 min. A data gap longer than that lands the next scan beyond the
window; `max(downloaded)` can't advance → permanent freeze. Regressed 2026-09-04
(`max−15` replaced `min+60`) + 2026-09-11 (30-min cap).
**Fix:** `fix/download-gap-ratchet-scan-cursor` (stepping cursor) and the superset
`feat/live-first-download-with-backfill`.

## 2. ar-vol-padding-mismatch  (STUCK, AR radars only)
**Fingerprint:** AR* radar; persistent `No new files`, download stalled, often a frozen
pointer; **zero downloads after the stall** despite the radar producing.
**FTP check:** `ftp_check` finds files on the FTP but the SIZE-based traversal yields 0
(the detector builds `AR5_1000_03_...` while real files are `AR5_1000_3_...`).
**Root cause:** SIZE-path filename construction in `ftp_client.traverse_radar` uses the
config's zero-padded vol key; AR/Gematronik sites publish single-digit vol numbers.
Regressed 2026-09-24 (`5014cd4`, nlst→SIZE). RMA sites unaffected (both padded).
**Fix:** `fix/ar-radar-vol-padding-ftp-detection` (probe both paddings).

## 3. poison-incomplete-volume  (DEGRADED)
**Fingerprint:** repeated `Processing complete: 0 succeeded, N failed`; `incomplete,
missing: {...}`; `FTP 550 ... (attempt 1/3)` where the attempt number is *always 1*.
**FTP check:** the missing field file genuinely 550s (permanent per-scan gap); sibling
fields of the same scan exist.
**Root cause:** `increment_retry_count`/`mark_download_permanently_failed` update the
`downloads` row `WHERE filename=?`, but a never-downloaded missing field has no row →
UPDATE hits 0 rows → counter stuck at 0 → abandonment never fires → volume retried
forever. Also `_retry_incomplete_volumes` re-flagged the volume `complete+pending` and
reset product generation *every cycle* unconditionally, forcing a failed re-decode each
cycle. Compounded by ~2h BUFR retention deleting the volume's fields.
**Secondary symptom — `No BUFR files could be decoded` (ValueError, often the top raised
error):** does NOT mean incomplete volumes skip products. Partial generation IS the design
— a fresh incomplete volume is processed with its available fields (`Will generate raw
COGs for available fields: {..}`) and re-rendered when it completes. This error only means
the volume has *zero* decodable BUFRs left — the available-field BUFRs were cleaned up
(~2h retention) before this week-old, un-abandonable volume was (endlessly) reprocessed.
So: lots of `No BUFR files` + a week-old `incomplete, missing` volume = poison volume, not
a partial-generation defect.
**Fix:** `fix/incomplete-volume-retry-abandon` (commit 6c96886) — `increment_retry_count`
upserts so the counter persists → field abandoned after N; `mark_volume_abandoned()` +
`get_incomplete_volumes` excludes `status='abandoned'`; `_retry_incomplete_volumes` only
re-queues/resets products when a field was actually recovered (else abandons the volume).

## 4. downloading-not-producing  (DEGRADED)
**Fingerprint:** many `Downloaded` lines but ~0 `Successfully saved NetCDF` / `raw COG`
over a long span; high `Backlog detected`.
**Root cause:** backfilled/old data has `observation_datetime` below the processing
daemon's completeness floor (which defaults to the daemon's startup `start_date`), so
old volumes are never registered/processed. (Also: processing genuinely stalled.)
**Fix:** `feat/live-first-download-with-backfill` publishes `backfill_origin`; the
processing floor is lowered to it so backward-filled volumes get processed.

## 5. pyart-shape-mismatch  (DEGRADED)
**Fingerprint:** `'data' has invalid shape, should be (R, G)`.
**Root cause:** within one scan, fields have inconsistent ray counts (e.g. 14 vs 13
sweeps → 5040 vs 4680 rays). `_align_field_to_reference` aligns gates, not rays, so
`radar.add_field` rejects the mismatched field.
**Fix (pending):** align fields on rays (pad/trim to the reference ray count).

## 6. download-stalled  (STUCK, generic fallback)
**Fingerprint:** last `Downloaded` far before end-of-log + persistent `No new files`,
but the specific mode can't be pinned (e.g. an older build that doesn't log the resume
pointer). Treat as a download stall; confirm with ftp_check + state_inspect.

## Always census raised Python errors
Pointer/download signals don't tell the whole story — real faults often surface only as
exceptions raised deep in processing/product generation. `analyze_log.py` always reports a
**Raised errors** census (traceback count, exception classes, top normalized ERROR lines)
and an `unclassified-errors` finding for non-benign tracebacks no fingerprint explains.
Read it every time; a new/unexpected exception type is a signal to investigate, not to
force into an existing mode. The `No BUFR files could be decoded` case above is a good
example of why: it's a `ValueError` whose meaning is the opposite of what it looks like.

## Benign (not faults)
- `FilterFieldsMissingError: Skipping ... COLMAX: filter field(s) ['RHOHV'] not yet
  present in incomplete volume. Will retry when volume is complete.` — expected retry
  while a volume is still arriving. Appears as a traceback but is caught.
- `CPLE_NotSupported ... COG does not support creation option TILED` — GDAL warning.
- `Campo de reflectividad vertical inexistente`, `Field 'RHOHV' not found ... No gates
  excluded`, best-effort COLMAX/COG without RHOHV — handled degraded-mode warnings.
