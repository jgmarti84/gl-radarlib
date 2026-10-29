# AGENTS.md — gl-radarlib

Instructions for anyone working on this repo, human or AI agent (Claude, Codex,
Copilot, …). Short on purpose. The detailed reference is
[`.github/copilot-instructions.md`](.github/copilot-instructions.md) and
[`docs/radarlib_EN.md`](docs/radarlib_EN.md).

## What this repo is
The **producer** of the radar system. It downloads BUFR files from the SINARAME radar
FTP, decodes them, and writes Cloud-Optimized GeoTIFFs (COG), Tops & Cores GeoJSON and
3D "NC cube" grids. Two layers:
- `src/radarlib/`: the library;
- `app/`: the **genpro25** service, one container per radar (`docker-compose.yml`).

Its consumer is **gl-webmet25**, which indexes and serves these files.

## Working rules (the same in gl-radarlib, gl-webmet25 and gl-tracker)
1. **Branch from `origin/dev`; open PRs into `dev`.** Never push to `dev` or `main`
   directly. `dev → main` is a separate promotion done by the maintainer. Agents never
   merge PRs.
2. **Propose → approve → apply.** For anything non-trivial, describe the change and
   wait for approval before writing it.
3. **Secrets never go into git.** Credentials live in a gitignored `.env` (copy
   `.env.example`). Never print, log or paste a secret value; to compare two values,
   show only their length or SAME/DIFFERENT.
4. **Run tests in a container, not on your host** (commands below). Static checks
   (`docker compose config`, pre-commit) are fine on the host.
5. **Don't delete files to tidy up.** Move them into the gitignored `local/` folder.
6. **PR descriptions.** Agents always cover: what changed and why; where it was tested;
   deployment notes, in order; new `.env` keys; and whether it changes the **output
   contract** (below). For PRs written by people these are recommended, not required;
   the ones worth never skipping are new `.env` keys and output-contract changes.

## Rules specific to this repo
- **Output contract.** gl-webmet25 depends entirely on this repo's file formats and
  names: COG metadata (`radarlib_cmap`, `vmin`, `vmax`, `field_name`, `timestamp`,
  EPSG:4326) and file names (`<RADAR>_<YYYYMMDDTHHMMSSZ>_<FIELD>[o]_<EL>.tif`,
  `…_TOPS_CORES.geojson`, `…_NCCUBE.nc`). Any change must be flagged in the PR and
  coordinated with gl-webmet25. PNG output is deprecated; don't build on it.
- **Two independent config layers.** `src/radarlib/config.py` holds the library
  defaults. `app/config.py` is the service layer (defaults → `genpro25.yml` section →
  environment variables). Never import `app/config` from `src/radarlib`.
- **Memory.** Explicit `del` + `gc.collect()` after large arrays in loops;
  `tempfile.TemporaryDirectory()`; cleanup in `finally`. Examples in
  copilot-instructions.md.
- **Style.** black, isort and flake8 via pre-commit (CI runs
  `pre-commit run --all-files`). Type hints on all functions; no bare `except:`.
- **After a change**, say whether `docs/radarlib_EN.md`, copilot-instructions.md or
  the output contract need updating.

## How to test (in a container)
```bash
docker build -f tests/Dockerfile -t radarlib-tests .
docker run --rm -v "$PWD":/app -w /app -e PYTHONDONTWRITEBYTECODE=1 radarlib-tests \
  python -m pytest -q -m "not integration" -p no:cacheprovider
```
Integration tests (`-m integration`) need real BUFR files or external resources.

## Running the genpro25 services
`docker compose` needs a `.env` with `FTP_USER` / `FTP_PASS` next to
`docker-compose.yml`. `genpro25.yml` keeps them empty; environment variables override
it. Without `.env`, compose refuses to start.
