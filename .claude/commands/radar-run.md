---
description: Build + run a genpro25 radar container live against the FTP (scratch dirs), then report what flowed.
argument-hint: <RADAR> [--minutes N] [--config genpro25.yml] [--no-wait] [--keep]
---

Smoke-test radar **$1** end-to-end using the `radar-doctor` skill.

Run `python3 .claude/skills/radar-doctor/scripts/run_radar.py --radar $1` (pass through
`--minutes`, `--config` (use `genpro25_arX.yml` for AR radars), `--no-wait`, `--keep`,
`--image`, `--build`, `--json` from `$ARGUMENTS`). It builds the image from
`app/Dockerfile` if needed, runs the radar into a throwaway scratch dir, waits, and
reports BUFR downloads, NetCDF, and TIFFs per volume + live/backfill scan counts.

Because this is long-running (and the first run per scratch dir rebuilds geometry, which
blocks for a few minutes), **run it in the background** and report when it completes.
The state.db lands at `<scratch>/data/radares/$1/state.db` — pair with `/radar-state`.
To verify a code change, it uses the current working tree (editable install in the image).
