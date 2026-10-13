# radarlib / genpro25 pipeline map

## Daemons (one process per radar, single asyncio loop; `app/main.py` → DaemonManager)
- **download_daemon** — FTP → BUFR. Resume logic in `_ftp_poll_cycle_inner` +
  `compute_scan_window`. Live-first build adds a live window `[now−live_window,now]` +
  a trickle `_run_backfill` using a persisted cursor.
- **processing_daemon** — BUFR → NetCDF. `_check_volume_completeness` registers volumes
  (scan floor = `start_date`, lowered to `backfill_origin` when backfilling);
  `_process_complete_volumes` processes pending (newest-first). Heavy decode runs *in
  the loop* (`bufr_to_dict` under `_c_library_lock`), so a big batch starves peers.
- **product_daemon** — NetCDF → COG/GeoTIFF (+ COLMAX, tops/cores).
- **cleanup_daemon** — deletes files whose products are done and are older than
  retention (BUFR/NetCDF retention ≈ 2h in genpro25.yml). Keeps the DB rows.

## state.db (per radar, `<ROOT_RADAR_FILES_PATH>/<RADAR>/state.db`)
- `downloads(filename UNIQUE, radar_name, strategy, vol_nr, field_type,
  observation_datetime, status, ...)` — `status='completed'` = downloaded. Frontier =
  `MAX(observation_datetime) WHERE status='completed'` per `vol_nr`.
- `volume_processing(volume_id, status, observation_datetime, is_complete, ...)` —
  `pending`→`completed`. Selection newest-first (`DESC`).
- `product_generation(volume_id, product_type, status, ...)`.
- `daemon_cursor(radar_name, name, value)` — durable cursors: `backfill` (how far the
  backfill has filled), `backfill_origin` (oldest point of the active hole = processing
  floor; cleared when caught up).

## Config (genpro25.yml, section = `GENPRO25_ENV`, default `local`)
- `FTP.FTP_HOST/FTP_USER/FTP_PASS` — credentials (read these).
- `VOLUME_TYPES."<strategy>"."<vol>"` = [fields]. RMA: strategy `0315`, vols `01/02/04`,
  zero-padded. AR: strategy `1000`, vols `01/02/03` in config but **single-digit on FTP**.
- `DAEMON_PARAMS`: poll intervals (~150-200s), retention days, PRODUCT_TYPE (`raw_cog`),
  ADD_COLMAX, ADD_TOPS_AND_CORES.
- DownloadDaemonConfig knobs (live-first): `live_window_minutes` (default 180),
  `enable_backfill`, `backfill_windows_per_cycle`, `max_traversal_window_minutes` (30).

## FTP layout
`/L2/<RADAR>/<YYYY>/<MM>/<DD>/<HH>/<MMSS>/<RADAR>_<strategy>_<vol>_<field>_<YYYYMMDD>T<HHMMSS>Z.BUFR`
Minute folder is `MMSS` (minute+second). SIZE on the control channel is the cheap
existence check; NLST lists a folder.

## Container run (see run-webmet25 pattern / docker-compose.yml)
Image from `app/Dockerfile`. Env: `RADAR_NAME`, `GENPRO25_ENV=local`, `GENPRO25_CONFIG`,
`ROOT_*` paths. Mount genpro25.yml, radarlib_overrides.json, and scratch dirs for
`data/radares`, `product_output`, `tops_and_cores`, `logs`, `data/geometries`, `cache`,
`data/gate_coordinates`. Geometry auto-builds on first use (slow, blocks the loop once).
