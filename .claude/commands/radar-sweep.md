---
description: Fleet health sweep — diagnose all radars from a directory of genpro25 logs, ranked by severity.
argument-hint: <logs-dir> [--ftp] [--reference RMA12]
---

Sweep the whole radar fleet using the `radar-doctor` skill.

Run `python3 .claude/skills/radar-doctor/scripts/sweep.py --logs-dir $1` (add `--ftp
--reference RMA12` to cross-check stalled radars against the live FTP, and `--json` if
asked; default the logs-dir to `.` if `$1` is empty).

Report the severity-ranked table and group the problems by root cause:
- **daemon stall** (radar CURRENT on FTP, daemon not downloading) → fixable pipeline bug;
- **radar/source outage** (radar STALE-DOWN on FTP) → escalate, daemon is fine;
- **degraded** (producing but with issues) and **healthy**.

For a deeper, autonomous per-radar investigation (drilling into gaps/cursors/550s), hand
off to the **radar-sweep** agent instead of running the script inline.
