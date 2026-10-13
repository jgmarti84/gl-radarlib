---
description: Check what data is actually on the radar FTP (is the radar still producing, or down?).
argument-hint: <RADAR> [--reference <RADAR>] [--file <name>]
---

Check the live FTP for radar **$1** using the `radar-doctor` skill.

Run `python3 .claude/skills/radar-doctor/scripts/ftp_check.py --radar $1 --reference RMA12`
(pass through any `--reference`, `--file`, or `--json` from `$ARGUMENTS`; default the
reference to a known-healthy radar such as RMA12 to prove the FTP is alive).

Report:
- the newest file on the FTP for $1 and its age → **CURRENT / LAGGING / STALE-DOWN**;
- the recent-folder cadence (reveals an acquisition gap);
- the reference comparison → distinguishes **radar/source outage** (FTP alive, this radar
  stale) from a **daemon stall** (radar current, daemon not downloading).

Credentials come from `genpro25.yml` automatically. This is the FTP step of the skill's
decision tree — use it after `analyze_log.py` reports any download stall.
