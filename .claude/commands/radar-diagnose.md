---
description: Diagnose a radarlib radar from its genpro25 log (and optionally cross-check the FTP).
argument-hint: <RADAR> [--log <path>] [--ftp]
---

Diagnose radar **$1** using the `radar-doctor` skill (`.claude/skills/radar-doctor/`).

Steps:
1. Locate the genpro25 log: use `--log <path>` if given in `$ARGUMENTS`, else look for
   `genpro25-<radar>.log*` in the repo root / cwd.
2. Run `python3 .claude/skills/radar-doctor/scripts/analyze_log.py --log <path>` and
   report the verdict + findings (each finding names the fix/branch).
3. If `--ftp` is in `$ARGUMENTS` and a download stall was found (`download-gap-ratchet`,
   `ar-vol-padding-mismatch`, or `download-stalled`), cross-check the live FTP:
   `python3 .claude/skills/radar-doctor/scripts/ftp_check.py --radar $1 --reference RMA12`
   to distinguish a daemon stall (radar current) from a radar/source outage (FTP alive,
   this radar stale).

Follow the decision tree in `.claude/skills/radar-doctor/SKILL.md`. Keep the summary
tight and cite concrete numbers (frozen-at time, product lag, 550 counts).
