#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""analyze_log.py — classify a genpro25 radar log and report its health.

Offline (stdlib only). Point it at a genpro25 log; it reconstructs the timeline,
traces the download resume pointer, and classifies the failure mode against the
known fingerprints documented in references/failure_modes.md.

Usage:
    python analyze_log.py --log genpro25-rma12.log.txt
    python analyze_log.py --log <path> --radar RMA12 --json

Exit code: 0 healthy/catching-up, 1 degraded, 2 stuck (no current products).
"""
import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime

TS = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
TS_RE = re.compile("^" + TS)
OBS_RE = re.compile(r"_(\d{8}T\d{6}Z)")


def _parse_ts(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _obs_to_dt(s):  # 20261012T173138Z
    try:
        return datetime.strptime(s, "%Y%m%dT%H%M%SZ")
    except ValueError:
        return None


def _normalize_err(msg):
    """Collapse variable tokens (files, volume ids, timestamps, numbers) so the same
    error from different volumes aggregates into one census row."""
    msg = msg.strip()
    msg = re.sub(r"[A-Z0-9]+_\d{3,4}_\d{1,2}_[A-Z]+_\d{8}T\d{6}Z(?:\.BUFR)?", "<FILE>", msg)
    msg = re.sub(r"[A-Z0-9]+_\d{3,4}_\d{1,2}_\d{4}-\d{2}-\d{2}T[0-9:+\-]+", "<VOL>", msg)
    msg = re.sub(r"\d{4}-\d{2}-\d{2}[T ][0-9:.+\-]+", "<TS>", msg)
    msg = re.sub(r"/\S+", "<PATH>", msg)
    msg = re.sub(r"\{[^}]*\}", "{..}", msg)
    msg = re.sub(r"\(\d+(?:, \d+)*\)", "(..)", msg)
    msg = re.sub(r"\b\d+\b", "N", msg)
    return msg.strip()


def scan(path):
    """Single streaming pass over the log collecting everything we need."""
    d = {
        "first_ts": None, "last_ts": None, "radar": None, "lines": 0,
        "resume_values": [],          # (ts, resume_iso)
        "backlog_detected": 0, "no_new_files": 0,
        "downloaded_obs": [],         # obs datetimes of downloaded files
        "live_scans": 0, "backfill_scans": 0,
        "netcdf_obs": [],             # obs of saved netcdf
        "cog_obs": [],                # obs of generated COG/tif
        "proc_complete": [],          # (ts, succeeded, failed)
        "incomplete_missing": Counter(),   # field -> count
        "ftp_550": 0, "retry_attempts": Counter(),  # "n/m" -> count
        "invalid_shape": 0,
        "tracebacks": 0, "benign_tb": 0,
        "restarts": 0,
        "last_download_ts": None, "last_netcdf_ts": None, "last_cog_ts": None,
        "error_msgs": Counter(),   # normalized ERROR-level log messages -> count
        "exc_types": Counter(),    # exception class names from tracebacks -> count
    }
    resume_re = re.compile(r"minus 15 min: ([0-9T:+\-]+)")
    proc_re = re.compile(r"Processing complete: (\d+) succeeded, (\d+) failed")
    attempt_re = re.compile(r"attempt (\d+/\d+)")
    missing_re = re.compile(r"incomplete, missing: \{([^}]*)\}")
    # An ERROR log line: "<ts> [RADAR|ERROR] module - <message>"
    err_re = re.compile(r"\|ERROR\]\s+\S+\s+-\s+(.*)$")
    # A traceback's final exception line (not timestamped): "pkg.mod.SomeError: msg"
    exc_re = re.compile(r"^([A-Za-z_][\w.]*(?:Error|Exception|Warning)):")

    with open(path, "r", errors="replace") as fh:
        for line in fh:
            d["lines"] += 1
            m = TS_RE.match(line)
            if m:
                ts = _parse_ts(m.group(1))
                if ts:
                    if d["first_ts"] is None:
                        d["first_ts"] = ts
                    d["last_ts"] = ts
            if d["radar"] is None:
                rm = re.search(r"\[([A-Z]{2,5}\d{1,2})\|", line)
                if rm:
                    d["radar"] = rm.group(1)

            if "Service Starting" in line or "DaemonManager startup" in line:
                d["restarts"] += 1
            if "Resuming from newest" in line:
                rv = resume_re.search(line)
                if rv:
                    d["resume_values"].append((m.group(1) if m else None, rv.group(1)))
            if "Backlog detected" in line:
                d["backlog_detected"] += 1
            if "No new files" in line:
                d["no_new_files"] += 1
            if "Live scan" in line:
                d["live_scans"] += 1
            if "Backfill scan" in line:
                d["backfill_scans"] += 1
            if "] Downloaded " in line:
                ob = OBS_RE.search(line)
                if ob:
                    d["downloaded_obs"].append(ob.group(1))
                if m:
                    d["last_download_ts"] = m.group(1)
            if "Successfully saved NetCDF" in line:
                ob = OBS_RE.search(line)
                if ob:
                    d["netcdf_obs"].append(ob.group(1))
                if m:
                    d["last_netcdf_ts"] = m.group(1)
            if "Generated" in line and "raw COG" in line:
                ob = OBS_RE.search(line)
                if ob:
                    d["cog_obs"].append(ob.group(1))
                if m:
                    d["last_cog_ts"] = m.group(1)
            pc = proc_re.search(line)
            if pc:
                d["proc_complete"].append((m.group(1) if m else None, int(pc.group(1)), int(pc.group(2))))
            mm = missing_re.search(line)
            if mm:
                for f in re.findall(r"'([^']+)'", mm.group(1)):
                    d["incomplete_missing"][f] += 1
            if "550" in line and ("file not found" in line or "Could not get file size" in line):
                d["ftp_550"] += 1
            am = attempt_re.search(line)
            if am and "Retrying download of missing field" in line:
                d["retry_attempts"][am.group(1)] += 1
            if "invalid shape" in line:
                d["invalid_shape"] += 1
            if "Traceback (most recent call last)" in line:
                d["tracebacks"] += 1
            if "FilterFieldsMissingError" in line:
                d["benign_tb"] += 1
            # Census every ERROR-level log line (normalized) so novel/unanticipated
            # Python errors surface even when no known fingerprint matches.
            if "|ERROR]" in line:
                em = err_re.search(line)
                if em:
                    d["error_msgs"][_normalize_err(em.group(1))] += 1
            # Capture the exception class from a traceback's final line.
            ex = exc_re.match(line)
            if ex:
                d["exc_types"][ex.group(1).split(".")[-1]] += 1
    return d


def classify(d):
    """Return list of findings: {mode, severity, confidence, evidence, action}."""
    findings = []
    rv = d["resume_values"]
    # Frozen download pointer: the same resume value repeated many consecutive times.
    frozen_run, frozen_val = 0, None
    if rv:
        cur, run = rv[0][1], 1
        best = 1
        for _, val in rv[1:]:
            if val == cur:
                run += 1
            else:
                cur, run = val, 1
            if run > best:
                best, frozen_val = run, val
        frozen_run = best
    tail_frozen = frozen_run >= 5 and rv and rv[-1][1] == frozen_val

    is_ar = bool(d["radar"]) and d["radar"].startswith("AR")

    # Download-stall gap: how long before end-of-log did the last file download?
    stall_min = None
    if d["last_download_ts"] and d["last_ts"]:
        ld = _parse_ts(d["last_download_ts"])
        if ld:
            stall_min = round((d["last_ts"] - ld).total_seconds() / 60)
    download_stalled = (stall_min is not None and stall_min > 45 and d["no_new_files"] >= 5)

    # 1) AR volume-number padding mismatch: AR radar whose download has stalled
    #    (frozen pointer and/or persistent No-new-files). The SIZE detector builds
    #    zero-padded names that never match single-digit AR filenames.
    if is_ar and (tail_frozen or download_stalled) and d["no_new_files"] >= 5:
        findings.append(dict(
            mode="ar-vol-padding-mismatch", severity="stuck", confidence="high",
            evidence=f"AR radar with stalled download ('No new files' x{d['no_new_files']}, "
                     f"{('pointer frozen, ' if tail_frozen else '')}last download {stall_min} min before "
                     f"end-of-log) — SIZE detector builds zero-padded names that don't match "
                     f"single-digit AR files.",
            action="Deploy fix/ar-radar-vol-padding-ftp-detection; confirm with radar-ftp "
                   "(files exist on FTP but the detector finds 0)."))

    # 2) Gap-ratchet: frozen resume pointer + backlog cap + no new files.
    if tail_frozen and d["backlog_detected"] >= 3 and d["no_new_files"] >= 3 and not is_ar:
        findings.append(dict(
            mode="download-gap-ratchet", severity="stuck", confidence="high",
            evidence=f"Resume pointer frozen at {frozen_val} for {frozen_run} consecutive cycles; "
                     f"Backlog-capped x{d['backlog_detected']}, 'No new files' x{d['no_new_files']}. "
                     f"A data gap > forward reach pinned the window.",
            action="Deploy live-first/stepping fix (branch feat/live-first-download-with-backfill or "
                   "fix/download-gap-ratchet-scan-cursor); confirm radar is still producing via radar-ftp."))

    # 3) Poison incomplete volume / retry-counter no-op.
    always_first = d["retry_attempts"] and set(a.split("/")[0] for a in d["retry_attempts"]) == {"1"}
    zero_succ = sum(1 for _, s, f in d["proc_complete"] if s == 0 and f > 0)
    if d["incomplete_missing"] and d["ftp_550"] and (always_first or zero_succ >= 5):
        findings.append(dict(
            mode="poison-incomplete-volume", severity="degraded", confidence="high" if always_first else "medium",
            evidence=f"Incomplete volumes missing {dict(d['incomplete_missing'])}, FTP 550 x{d['ftp_550']}, "
                     f"retry attempts stuck at '1/..' ({'always' if always_first else 'often'}); "
                     f"{zero_succ} cycles with 0 succeeded. Retry counter never persists (no downloads row).",
            action="Fix retry-counter persistence in sqlite_tracker so 550-missing fields are abandoned "
                   "after N attempts (still pending)."))

    # 4) Downloading but not producing: data is coming in but no products come out
    #    (e.g. backfilled old data invisible to the processing start_date floor).
    if len(d["downloaded_obs"]) >= 50 and len(d["netcdf_obs"]) == 0:
        findings.append(dict(
            mode="downloading-not-producing", severity="degraded", confidence="high",
            evidence=f"{len(d['downloaded_obs'])} downloads but 0 NetCDF/COG produced over the log — "
                     f"downloaded data is not being processed (old backfilled data sits below the "
                     f"processing start_date floor, or processing is stalled).",
            action="Check the processing completeness floor / backfill_origin (live-first fix); "
                   "inspect state with radar-state."))

    # 5) Generic download stall (fallback when no specific download mode fired),
    #    e.g. an older daemon build that does not log the resume pointer.
    specific_download = any(f["mode"] in ("download-gap-ratchet", "ar-vol-padding-mismatch")
                            for f in findings)
    if download_stalled and not specific_download:
        findings.append(dict(
            mode="download-stalled", severity="stuck", confidence="medium",
            evidence=f"Last download was {stall_min} min before end-of-log with 'No new files' "
                     f"x{d['no_new_files']} (Backlog x{d['backlog_detected']}). Download is not "
                     f"advancing; exact cause not pinned from the log.",
            action="Confirm the radar is still producing via radar-ftp; trace the resume pointer and "
                   "inspect cursors with radar-state; likely a gap-ratchet or padding stall."))

    # 6) pyart ray/gate mismatch.
    if d["invalid_shape"]:
        findings.append(dict(
            mode="pyart-shape-mismatch", severity="degraded", confidence="high",
            evidence=f"'data has invalid shape' x{d['invalid_shape']} — fields in a scan have "
                     f"inconsistent ray counts; _align_field_to_reference aligns gates not rays.",
            action="Align fields on rays (pad/trim to reference ray count) in pyart_writer."))

    # Benign traceback note
    real_tb = d["tracebacks"] - d["benign_tb"]
    if d["benign_tb"]:
        findings.append(dict(
            mode="benign-filter-fields", severity="info", confidence="high",
            evidence=f"{d['benign_tb']} FilterFieldsMissingError tracebacks — expected retry for "
                     f"incomplete volumes awaiting RHOHV; not a fault.",
            action="None."))

    # Always watch for raised Python errors. Flag any that the known modes above do
    # NOT already explain, so a novel/unanticipated error is never silently missed.
    explained = any(f["mode"] in ("poison-incomplete-volume", "pyart-shape-mismatch",
                                  "downloading-not-producing") for f in findings)
    top_errs = [m for m, _ in d["error_msgs"].most_common(3)]
    if real_tb > 0 and not explained:
        findings.append(dict(
            mode="unclassified-errors", severity="degraded", confidence="medium",
            evidence=f"{real_tb} non-benign traceback(s); top ERROR(s): " + "; ".join(top_errs) +
                     (f"  exc_types={dict(d['exc_types'])}" if d["exc_types"] else ""),
            action="Investigate these raised errors — no known failure fingerprint explains them."))
    return findings, dict(frozen_run=frozen_run, frozen_val=frozen_val, tail_frozen=tail_frozen,
                          real_tracebacks=real_tb)


def verdict(d, findings):
    sev = {f["severity"] for f in findings}
    # Age of newest product observation vs end of log.
    cog_dts = sorted(x for x in (_obs_to_dt(o) for o in d["cog_obs"]) if x)
    last_cog_obs = cog_dts[-1] if cog_dts else None
    behind_min = None
    if last_cog_obs and d["last_ts"]:
        behind_min = round((d["last_ts"] - last_cog_obs).total_seconds() / 60)
    if "stuck" in sev:
        return "STUCK", 2, behind_min, last_cog_obs
    if "degraded" in sev:
        return "DEGRADED", 1, behind_min, last_cog_obs
    # catching-up heuristic: many backfill/live scans but products lag
    if behind_min is not None and behind_min > 180:
        return "CATCHING-UP / LAGGING", 1, behind_min, last_cog_obs
    return "HEALTHY", 0, behind_min, last_cog_obs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--radar", default=None)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    d = scan(a.log)
    if a.radar:
        d["radar"] = a.radar
    findings, extra = classify(d)
    status, code, behind_min, last_cog_obs = verdict(d, findings)

    if a.json:
        out = dict(radar=d["radar"], status=status,
                   span=[str(d["first_ts"]), str(d["last_ts"])],
                   last_product_obs=str(last_cog_obs) if last_cog_obs else None,
                   product_lag_minutes=behind_min,
                   counts=dict(downloads=len(d["downloaded_obs"]), netcdf=len(d["netcdf_obs"]),
                               cog=len(d["cog_obs"]), no_new_files=d["no_new_files"],
                               backlog_detected=d["backlog_detected"], live_scans=d["live_scans"],
                               backfill_scans=d["backfill_scans"], ftp_550=d["ftp_550"],
                               restarts=d["restarts"], real_tracebacks=extra["real_tracebacks"]),
                   frozen_pointer=extra["frozen_val"] if extra["tail_frozen"] else None,
                   raised_errors=dict(tracebacks=d["tracebacks"], benign=d["benign_tb"],
                                      real=extra["real_tracebacks"],
                                      exc_types=dict(d["exc_types"]),
                                      top=d["error_msgs"].most_common(8)),
                   findings=findings)
        print(json.dumps(out, indent=2))
        return code

    R = d["radar"] or "?"
    print(f"{'='*72}\n  RADAR DOCTOR — {R}   [{status}]\n{'='*72}")
    print(f"  Log span      : {d['first_ts']}  ..  {d['last_ts']}  ({d['lines']} lines)")
    print(f"  Restarts      : {d['restarts']}")
    print(f"  Last download : {d['last_download_ts']}")
    print(f"  Last NetCDF   : {d['last_netcdf_ts']}")
    print(f"  Last product  : obs {last_cog_obs}" + (f"  ({behind_min} min behind end-of-log)" if behind_min is not None else ""))
    print(f"  Downloads={len(d['downloaded_obs'])}  NetCDF={len(d['netcdf_obs'])}  COG={len(d['cog_obs'])}  "
          f"NoNewFiles={d['no_new_files']}  Backlog={d['backlog_detected']}  550s={d['ftp_550']}")
    if extra["tail_frozen"]:
        print(f"  ⚠ download pointer FROZEN at {extra['frozen_val']} ({extra['frozen_run']} cycles)")
    print(f"{'-'*72}")
    if not findings:
        print("  No known failure fingerprints matched.")
    for f in findings:
        tag = {"stuck": "🔴", "degraded": "🟠", "info": "ℹ️"}.get(f["severity"], "•")
        print(f"  {tag} [{f['mode']}]  ({f['confidence']} confidence)")
        print(f"       {f['evidence']}")
        print(f"       → {f['action']}")
    # Always surface the raised-error census (even when a mode already explains them).
    print(f"{'-'*72}")
    print(f"  Raised errors : {d['tracebacks']} tracebacks ({extra['real_tracebacks']} non-benign)"
          + (f", exc_types={dict(d['exc_types'])}" if d["exc_types"] else ""))
    if d["error_msgs"]:
        for msg, n in d["error_msgs"].most_common(5):
            print(f"      {n:5d} x  {msg[:92]}")
    else:
        print("      (no ERROR-level lines)")
    print(f"{'='*72}")
    return code


if __name__ == "__main__":
    sys.exit(main())
