#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""run_radar.py — build + run a genpro25 radar container against the live FTP.

Spins up one radar into throwaway scratch dirs (never a production path), waits,
then reports what flowed end-to-end: BUFR downloads, NetCDF, and TIFFs per volume.
Use it to smoke-test a change or watch live/backfill behaviour. Shells out to
docker; the state.db lands in <scratch>/data/radares/<RADAR>/state.db so you can
pair it with reproduce_stall.py / state_inspect.py.

Usage:
    python3 run_radar.py --radar RMA12 --minutes 12
    python3 run_radar.py --radar AR5 --config genpro25_arX.yml --minutes 12
    python3 run_radar.py --radar RMA12 --no-wait        # start, return, check later
    python3 run_radar.py --radar RMA12 --dry-run        # print the docker command
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

OBS_RE = re.compile(r"_(\d{8}T\d{6}Z)")
VOL_RE = re.compile(r"_0?(\d{1,2})_[A-Z]+_\d{8}T")


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def repo_root():
    r = sh(["git", "rev-parse", "--show-toplevel"])
    return r.stdout.strip() if r.returncode == 0 else os.getcwd()


def ensure_image(image, repo, build):
    exists = sh(["docker", "image", "inspect", image]).returncode == 0
    if exists and not build:
        return True
    print(f"[run_radar] building image {image} (this can take ~1-2 min)...")
    r = subprocess.run(["docker", "build", "-f", "app/Dockerfile", "-t", image, "."], cwd=repo)
    return r.returncode == 0


def docker_run_cmd(radar, image, scratch, repo, config, name):
    def v(host, cont):
        return ["-v", f"{host}:{cont}"]
    mounts = []
    for sub, cont in [("data/radares", "/workspace/app/data/radares"),
                      ("product_output", "/workspace/app/product_output"),
                      ("tops_and_cores", "/workspace/app/tops_and_cores"),
                      ("logs", "/workspace/app/logs"),
                      ("geometries", "/workspace/app/data/geometries"),
                      ("cache", "/workspace/app/cache"),
                      ("data/gate_coordinates", "/workspace/app/data/gate_coordinates")]:
        (Path(scratch) / sub).mkdir(parents=True, exist_ok=True)
        mounts += v(str(Path(scratch) / sub), cont)
    mounts += ["-v", f"{repo}/{config}:/workspace/app/genpro25.yml:ro"]
    overrides = Path(repo) / "radarlib_overrides.json"
    if overrides.exists():
        mounts += ["-v", f"{overrides}:/workspace/app/radarlib_overrides.json:ro"]
    env = {
        "RADAR_NAME": radar, "GENPRO25_ENV": "local",
        "GENPRO25_CONFIG": "/workspace/app/genpro25.yml",
        "RADARLIB_CONFIG": "/workspace/app/radarlib_overrides.json",
        "ROOT_CACHE_PATH": "/workspace/app/cache",
        "ROOT_RADAR_FILES_PATH": "/workspace/app/data/radares",
        "ROOT_RADAR_PRODUCTS_PATH": "/workspace/app/product_output",
        "ROOT_TOPS_AND_CORES_PATH": "/workspace/app/tops_and_cores",
        "ROOT_GATE_COORDS_PATH": "/workspace/app/data/gate_coordinates",
        "ROOT_LOGS_PATH": "/workspace/app/logs",
        "ROOT_GEOMETRY_PATH": "/workspace/app/data/geometries",
        "STREAM_LOG_LEVEL": "INFO", "PYTHONUNBUFFERED": "1",
    }
    envargs = []
    for k, val in env.items():
        envargs += ["-e", f"{k}={val}"]
    return (["docker", "run", "-d", "--name", name, "--memory=3g"] + mounts + envargs +
            [image, "bash", "-c", "mkdir -p /workspace/app/product_output && python main.py"])


def report(name, scratch):
    logs = sh(["docker", "logs", name]).stdout + sh(["docker", "logs", name]).stderr
    dl = Counter_by_date(re.findall(r"\] Downloaded \S+_(\d{8})T", logs))
    nc = len(re.findall(r"Successfully saved NetCDF", logs))
    tifs = list((Path(scratch) / "product_output").rglob("*.tif"))
    by_vol, by_date = {}, {}
    for t in tifs:
        mv = VOL_RE.search(t.name)
        if mv:
            by_vol[mv.group(1).zfill(2)] = by_vol.get(mv.group(1).zfill(2), 0) + 1
        mo = OBS_RE.search(t.name)
        if mo:
            dd = mo.group(1)[:8]
            by_date[dd] = by_date.get(dd, 0) + 1
    live = len(re.findall(r"Live scan", logs))
    bf = len(re.findall(r"Backfill scan", logs))
    return dict(downloads_by_date=dl, netcdf=nc, tiffs=len(tifs),
                tiffs_by_vol=by_vol, tiffs_by_date=by_date, live_scans=live, backfill_scans=bf)


def Counter_by_date(dates):
    out = {}
    for d in dates:
        out[d] = out.get(d, 0) + 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--radar", required=True)
    ap.add_argument("--minutes", type=int, default=12)
    ap.add_argument("--scratch", default=None)
    ap.add_argument("--image", default="radarlib-genpro25:local")
    ap.add_argument("--config", default="genpro25.yml")
    ap.add_argument("--name", default=None)
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--no-wait", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    repo = repo_root()
    scratch = a.scratch or tempfile.mkdtemp(prefix=f"radar-run-{a.radar}-")
    name = a.name or f"genpro25-{a.radar.lower()}-doctor"
    if not (Path(repo) / a.config).exists():
        sys.exit(f"config not found: {repo}/{a.config}")

    cmd = docker_run_cmd(a.radar, a.image, scratch, repo, a.config, name)
    if a.dry_run:
        print("scratch:", scratch)
        print(" ".join(cmd))
        return 0

    if sh(["docker", "--version"]).returncode != 0:
        sys.exit("docker not available")
    if not ensure_image(a.image, repo, a.build):
        sys.exit("image build failed")

    sh(["docker", "rm", "-f", name])
    r = subprocess.run(cmd, cwd=repo)
    if r.returncode != 0:
        sys.exit("docker run failed")
    print(f"[run_radar] started {name}; scratch={scratch}")
    if a.no_wait:
        print(f"[run_radar] check later: docker logs {name} | state.db at {scratch}/data/radares/{a.radar}/state.db")
        return 0

    deadline = time.time() + a.minutes * 60
    while time.time() < deadline:
        time.sleep(30)
        tifs = len(list((Path(scratch) / "product_output").rglob("*.tif")))
        print(f"[run_radar] +{int((a.minutes*60-(deadline-time.time()))/60)}min  tiffs={tifs}")
        if tifs > 0 and time.time() < deadline - 60:
            pass  # keep going to gather a fuller picture

    rep = report(name, scratch)
    rep["container"] = name
    rep["scratch"] = scratch
    if not a.keep:
        sh(["docker", "stop", name])
        rep["stopped"] = True

    if a.json:
        print(json.dumps(rep, indent=2))
    else:
        print(f"{'='*72}\n  RUN REPORT — {a.radar}  ({name})\n{'='*72}")
        print(f"  scratch        : {scratch}")
        print(f"  downloads      : {rep['downloads_by_date']}")
        print(f"  live/backfill  : live={rep['live_scans']} backfill={rep['backfill_scans']}")
        print(f"  NetCDF         : {rep['netcdf']}")
        print(f"  TIFFs          : {rep['tiffs']}  by_vol={rep['tiffs_by_vol']}  by_date={rep['tiffs_by_date']}")
        print(f"  container {'kept running' if a.keep else 'stopped'}.")
        print(f"{'='*72}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
