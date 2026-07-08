# radarlib — Copilot Instructions

## About This Repository
radarlib is a **data producer** library and one of two repositories
in the radar/meteorology system. It fetches raw radar data (BUFR format)
from FTP servers, processes it through a multi-stage pipeline, and
outputs Cloud-Optimized GeoTIFFs (COGs) and PNG images.

The second repository, **webmet25**, is the data consumer. It reads
the output files produced by this library and indexes them into a
database to serve them via an API and frontend.

> 📖 Full project documentation lives in `docs/radarlib_EN.md`
> Always read the relevant section before writing any code.

---

## Documentation Map
When you need context about a specific topic, read the
corresponding section in `docs/radarlib_EN.md`:

| Topic | Section to Read |
|-------|----------------|
| What radarlib does and why | Section 1: Overview & Quick Start |
| Installation and environment setup | Section 2: Installation & Setup |
| Configuration options and env variables | Section 3: Configuration Reference |
| Docker deployment and operations | Section 4: Deployment & Operations |
| System architecture and daemons | Section 5: Architecture Deep Dive |
| Core modules, functions, usage examples | Section 6: Module Reference |
| BUFR format and decoding pipeline | Section 7: BUFR Processing Guide |
| Advanced usage and integration examples | Section 8: Integration & Advanced Examples |
| Output files, naming, metadata (CRITICAL) | Section: Output Contract |
| Current bugs and limitations | Section: Known Gaps & Risks |
| Code style and contribution rules | Section: Contributing |

---

## Output Contract (Quick Reference)
> ⚠️ This is the most critical section for webmet25 compatibility.
> For full details always read the Output Contract section in
> `docs/radarlib_EN.md` before touching any output-related code.
> ⚠️ This contract is duplicated in webmet25's
> `copilot-instructions.md`. If you change this,
> update that file too.

### Output Types Overview

| Output | Format | Status | Description |
|---|---|---|---|
| COG | GeoTIFF Float32 | ✅ Primary | One per radar field per volume |
| GeoJSON | FeatureCollection | ✅ Active | Tops & cores, one per volume (gated by config) |
| PNG | RGBA uint8 | ⚠️ Deprecated | Backward compatibility only |

### Primary Output Format
- **GeoTIFF (COG):** This is the primary and current output format.
  Cloud-Optimized GeoTIFF is the production standard.
- **PNG:** Deprecated. Kept only for backward compatibility.
  Do not build new features around PNG output.

### File Naming Convention
`<RADAR_NAME>_<TIMESTAMP>_<FIELD>[o]_<ELEVATION>.<ext>`

| Token | Description | Example |
|-------|-------------|---------|
| `RADAR_NAME` | Radar station identifier | `RMA1` |
| `TIMESTAMP` | ISO 8601 format: `YYYYMMDDTHHMMSSZ` | `20260401T205000Z` |
| `FIELD` | Radar field/variable name | `ZDR`, `DBZH` |
| `[o]` | Letter `o` suffix = raw/non-filtered data. Absent = filtered data | `ZDRo` vs `ZDR` |
| `ELEVATION` | Elevation angle in degrees, zero-padded to 2 digits. Currently always `00`. Future versions will support other values | `00` |
| `ext` | File extension | `tif` (primary), `png` (deprecated) |

### Naming Examples
Filtered ZDR field, elevation 00 degrees → GeoTIFF
RMA1_20260401T205000Z_ZDR_00.tif

Non-filtered (raw) ZDR field, elevation 00 degrees → GeoTIFF
RMA1_20260401T205000Z_ZDRo_00.tif

PNG equivalent (deprecated, backward compat only)
RMA1_20260401T205000Z_ZDR_00.png

### Folder Structure
```text
ROOT_RADAR_PRODUCTS_PATH/
└── <RADAR_NAME>/
    └── /YYYY/
        └── /MM/
            └── /DD/
                ├── RMA1_20260401T205000Z_ZDR_00.tif
                ├── RMA1_20260401T205000Z_ZDRo_00.tif
                └── RMA1_20260401T205000Z_ZDR_00.png ← deprecated
```

### Tops & Cores GeoJSON Convention
```text
tops_and_cores/
└── {radar_code}/
    └── YYYY/
        └── MM/
            └── DD/
                └── {radar_code}_{strategy}_{vol_nr}_{timestamp}_TOPS_CORES.geojson
```
Example: `RMA6_A_00_20260505163854_TOPS_CORES.geojson`

GeoJSON schema per feature:
```json
// Core feature:
{
  "type": "Feature",
  "geometry": { "type": "Point", "coordinates": [lon, lat] },
  "properties": {
    "type": "core",
    "intensity_dbz": 52,           ← int, mean dBZ of the COLMAX blob
    "radar_code": "RMA6",
    "observation_time": "2026-05-05T16:38:54Z"
  }
}

// Top feature:
{
  "type": "Feature",
  "geometry": { "type": "Point", "coordinates": [lon, lat] },
  "properties": {
    "type": "top",
    "altitude_m": 11200,           ← int, altitude in metres of highest valid echo
    "dbz": 25.5,                   ← float, reflectivity at the top point
    "parent_core_dbz": 52,         ← int, mean_dbz of the parent core
    "radar_code": "RMA6",
    "observation_time": "2026-05-05T16:38:54Z"
  }
}
```

Critical rules:
- File is NOT written if both core list and top list are empty.
- `observation_time` is always ISO 8601 UTC.
- Coordinates are [lon, lat] — GeoJSON standard order.
- Timestamp format in filename is `YYYYMMDDTHHMMSSZ` — same as COG filenames.
- webmet25 indexer depends on this filename pattern — never change it without
updating TopsAndCoresFilenameParser in the webmet25 repo.

### GeoTIFF Metadata Fields

| Field | Value | Purpose |
|---|---|---|
| **CRS** | EPSG:4326 | Geographic coordinate system (WGS84 lat/lon) |
| **radarlib_cmap** | Colormap name string | Name of matplotlib colormap used (e.g., `"grc_th"`) |
| **vmin** | Float | Minimum data value for color scaling |
| **vmax** | Float | Maximum data value for color scaling |
| **field_name** | String | Radar field name (e.g., `"DBZH"`) |
| **timestamp** | ISO 8601 | Data acquisition timestamp |

### Critical Rules
- **Never change this contract without updating webmet25 indexer.**
- **Do not add new output formats without updating both repos.**
- When implementing multi-elevation support in the future, the
  `ELEVATION` token must remain zero-padded to 2 digits (e.g.,
  `05`, `10`) to preserve consistent file naming.
- PNG generation should not be extended or improved. If a task
  involves PNG output, flag it and ask for confirmation.

---

## Coding Conventions & Rules
> Always follow these when generating code. Full examples are in
> the Contributing section of `docs/radarlib_EN.md`.

- **Python version:** 3.11
- **Formatter:** black (run before every commit)
- **Linter:** flake8
- **Type hints:** Required on all functions (enforced by mypy)
- **Naming:**
  - Variables and functions: `snake_case`
  - Classes: `PascalCase`
  - Constants: `UPPER_SNAKE_CASE`
- **Error handling:** Always raise specific descriptive exceptions.
  Never use bare `except:` clauses.
- **Async:** FTP operations use `aioftp` and must be async/await.

---

## Memory Management Best Practices
> ✅ radarlib implements comprehensive memory cleanup to prevent
> leaks in long-running daemons. Always follow these patterns.

### Rule 1: Explicit Cleanup of Large Numpy Arrays
**Pattern:** Always delete large arrays after use in loops

```python
# ✅ CORRECT
for field in fields:
    field_data = get_field_data(radar, field)     # Large array
    grid_data = apply_geometry(geometry, field_data)  # Large array
    ppi = constant_elevation_ppi(grid_data)           # Large array

    # Use the data...
    save_product(ppi)

    # Explicit cleanup
    del field_data, grid_data, ppi
    gc.collect()

# ❌ WRONG - Arrays persist across loop iterations
for field in fields:
    field_data = get_field_data(radar, field)
    grid_data = apply_geometry(geometry, field_data)
    # No cleanup - memory accumulates!
```

### Rule 2: Use Context Managers for Temporary Directories
**Pattern:** Use `TemporaryDirectory()` instead of `mkdtemp()`

```python
# ✅ CORRECT - Automatic cleanup
import tempfile
with tempfile.TemporaryDirectory() as temp_dir:
    output_file = Path(temp_dir) / "output.tif"
    create_cog(data, output_file)
    shutil.move(output_file, target_path)
# temp_dir automatically deleted here

# ❌ WRONG - Manual cleanup required, often forgotten
temp_dir = tempfile.mkdtemp()
output_file = Path(temp_dir) / "output.tif"
create_cog(data, output_file)
shutil.move(output_file, target_path)
shutil.rmtree(temp_dir)  # Easy to forget or miss on error!
```

### Rule 3: Trigger GC After Heavy Operations
**Pattern:** Call `gc.collect()` after creating many intermediate objects

```python
# ✅ CORRECT
import gc

for field in radar.fields.keys():
    # Processing that creates intermediate masked arrays
    radar.fields[field]["data"] = ma.masked_invalid(radar.fields[field]["data"])
    radar.fields[field]["data"] = ma.masked_outside(radar.fields[field]["data"], -100000, 100000)

# Force GC to free intermediate copies
gc.collect()
```

### Rule 4: Comprehensive Finally Blocks
**Pattern:** Always cleanup in finally blocks, even for exceptions

```python
# ✅ CORRECT
try:
    radar = load_radar(filename)
    grid_data = process(radar)
    save_output(grid_data)
finally:
    # Cleanup even if exception occurs
    if 'radar' in locals():
        del radar
    if 'grid_data' in locals():
        del grid_data
    gc.collect()

# ❌ WRONG - No cleanup on exception
try:
    radar = load_radar(filename)
    grid_data = process(radar)
    save_output(grid_data)
except Exception:
    pass  # Memory leaked!
```

### Rule 5: Use Memory Monitoring in Long-Running Code
**Pattern:** Use `log_memory_usage()` to track memory at key points

```python
from radarlib.utils.memory_profiling import log_memory_usage, check_and_cleanup_memory

def process_volume(volume_path):
    log_memory_usage("Before loading volume")

    radar = load_volume(volume_path)
    log_memory_usage("After loading volume")

    # Process fields...
    for field in fields:
        grid_data = apply_geometry(radar, field)
        log_memory_usage(f"After apply_geometry for {field}")
        save_product(grid_data)
        del grid_data

    # Check memory and force GC if threshold exceeded
    check_and_cleanup_memory(threshold_mb=1100.0, label="After field processing")

    log_memory_usage("After volume processing")
```

### Memory Leak Investigation Checklist
If you suspect a memory leak:

1. ✅ Are large numpy arrays explicitly deleted in loops?
2. ✅ Are temporary directories using context managers?
3. ✅ Is `gc.collect()` called after heavy operations?
4. ✅ Do finally blocks cleanup all large objects?
5. ✅ Are intermediate copies in masking/filtering operations freed?
6. ✅ Are PyART radar objects deleted after use?
7. ✅ Is memory monitoring in place to track RSS growth?

---

### Tops & Cores Detection Module
File: `src/radarlib/io/pyart/cores_and_tops.py`
Entry point: `generate_cores_and_tops(colmax_2d, dbzh_3d, x_coords, y_coords, z_coords, radar_lat, radar_lon, observation_time, radar_code, strategy, vol_nr, output_dir, rhohv_3d, rhohv_2d)`
Called from: `ProductGenerationDaemon._generate_tops_and_cores()`, which is called inside
`_generate_raw_cog_products_sync()` after unfiltered field COG loop but before filtered field loop.
Gate: `config.ADD_TOPS_AND_CORES` must be True — otherwise `_generate_tops_and_cores()` is never called.

**Algorithm Summary**

Inputs:
- `colmax_2d` — 2D COLMAX array (NY, NX) from `column_max()`
- `dbzh_3d` — 3D Cartesian DBZH array (NZ, NY, NX) from `apply_geometry()`
- `x_coords`, `y_coords` — 2D Cartesian coordinate grids (NY, NX), radar-relative metres
- `z_coords` — 1D altitude array (NZ), from `geometry.z_levels()`
- `rhohv_3d` — optional 3D RhoHV (NZ, NY, NX), `None` when field absent
- `rhohv_2d` — optional 2D RhoHV (NY, NX) at lowest sweep, `None` when field absent

**Core detection** — `detect_cores_from_colmax()` in `src/radarlib/radar_grid/cores.py`:
1. Threshold COLMAX 2D ≥ `CORES_MIN_Z`, exclude masked pixels
2. `scipy.ndimage.label()` connected-component labelling (4-neighbour, conservative)
3. Per blob: reject if pixel_count ≤ 1; compute centroid via `x_coords[mask].mean()` / `y_coords[mask].mean()`
4. Reject blobs with centroid range < `CORES_MIN_RANGE`
5. Quality gate: `rhohv_ok = (mean_rhohv > CORES_RHOHV_THRESHOLD) AND (pixel_count > CORES_MIN_PIXELS)` OR
   `updraft_ok = (max_dbz > CORES_MIN_Z_UPDRAFT) AND (pixel_count > CORES_MIN_PIXELS_UPDRAFT)`
6. Deduplication: pairs within `CORES_DEDUP_RADIUS` → **weighted-mean centroid** (weight = mean_dBZ),
   keep strongest core's intensity stats

**Top detection** — `detect_tops_from_cores()` in `src/radarlib/radar_grid/tops.py`:
1. For each detected core: define a cylinder of radius `TOPS_DEDUP_RADIUS_M` around its (x_m, y_m)
2. Scan all NZ levels top-to-bottom; for each level find grid cells within cylinder that are not masked
3. Track the **highest altitude** where any valid (non-masked, > 0 dBZ) echo exists
4. Record: position of max-dBZ pixel at that highest level, altitude, dBZ value
5. Returns one top dict per core (or none if no valid echo found in cylinder)
> Note: No MIN_Z_TOP, MIN_DEV, or RhoHV threshold is applied in this function.
  The only parameter is the cylindrical search radius.

**Coordinate conversion:**
`pyart.core.transforms.cartesian_to_geographic_aeqd(x, y, origin_lon, origin_lat)` → (lon, lat)

**Important Implementation Notes**
- Uses `scipy.ndimage.label()` — NOT `pyart.correct.find_objects()` (which requires polar Radar objects)
- Core detection operates on the 2D COLMAX grid, not on 3D levels
- Top detection is core-anchored (cylindrical, per-core) — not independent per-level scanning
- RhoHV field name resolved via `pyart.config.get_field_name('cross_correlation_ratio')`
- Missing RhoHV → WARNING logged; detection continues with updraft-gate only for cores, no RhoHV gate for tops
- Missing DBZH → WARNING logged, tops/cores skipped entirely
- GeoJSON write failure → ERROR logged with traceback, never re-raised (must not abort COG generation)
- 3D arrays (dbzh_3d, rhohv_3d) for tops/cores are **recomputed from scratch** after field loops are freed —
  deliberate trade-off to keep memory bounded in the long-running daemon
- Log at INFO: `"CORES_TOPS radar={} time={} cores={} tops={} elapsed={}ms"`

---

## Known Gaps & Risks (Quick Reference)
> Do not replicate these patterns. Suggest fixes when touching
> these areas. Full details in `docs/radarlib_EN.md`.

- ✅ **Memory management:** FIXED - Comprehensive cleanup implemented
- ❌ Limited error handling in daemons
- ❌ No retry logic for failed processing steps
- ❌ Incomplete test coverage for `radar_grid`
- ❌ No integration tests for the full pipeline
- ❌ No GeoTIFF output validation
- ❌ SQLite state tracking may bottleneck at high frequency
- ✅ **Configuration system:** FIXED - Unified loader in `app/config.py`

---

## Configuration System

### Two-Layer Design
The configuration system has two distinct layers:

| Layer | File | Purpose |
|-------|------|---------|
| **radarlib core** | `src/radarlib/config.py` | Standalone library defaults (colormaps, GRC thresholds, geometry params, daemon defaults). Used by all radarlib internals. |
| **Genpro25 service** | `app/config.py` | Service-layer config. Merges `_DEFAULTS` + genpro25.yml YAML + env vars. Consumed by `app/main.py`. |

These two layers are **independent** — `app/config.py` does NOT import from `radarlib.config`.

### Precedence (app/config.py)
Lowest → Highest:
1. `_DEFAULTS` dict in `app/config.py`
2. Flattened leaf values from the active section in `genpro25.yml`
3. OS environment variables (type-coerced against `_DEFAULTS`)

### YAML Structure & Flattening
`genpro25.yml` uses nested grouping sections (`DAEMON_PARAMS`, `FTP`, `COLMAX`, etc.)
that are NOT themselves config keys. `_flatten_dict()` removes these structural nodes,
promoting their leaf children to the top level.

**Rule:** a YAML key whose name appears in `_DEFAULTS` is treated as a *config key*
(value kept as-is, even if the value is a dict, e.g. `VOLUME_TYPES`). Any other
dict-valued key is treated as a *grouping node* and recursed into.

```
# genpro25.yml (nested)       →   flattened result
DAEMON_PARAMS:                 →   ENABLE_CLEANUP_DAEMON: True
  ENABLE_CLEANUP_DAEMON: True  →   CLEANUP_POLL_INTERVAL: 1800
  CLEANUP_POLL_INTERVAL: 1800

VOLUME_TYPES:                  →   VOLUME_TYPES: {"0315": {...}}  # kept intact
  "0315":
    "01": [...]
```

### Tops & Cores Config Keys

Tops & cores settings are split across both config layers:

**Service layer** — `app/config.py` `_DEFAULTS` (rule 1: daemon toggles / paths):

| Key | Default | Type | Description |
|---|---|---|---|
| `ADD_TOPS_AND_CORES` | `False` | bool | Gate flag — enables GeoJSON generation |
| `ROOT_TOPS_AND_CORES_PATH` | `"tops_and_cores"` | str | Root directory for GeoJSON output |

**Library layer** — `src/radarlib/config.py` `DEFAULTS` (rule 2: algorithm thresholds):

| Key | Default | Type | Description |
|---|---|---|---|
| `CORES_MIN_Z` | `40.0` | float | COLMAX threshold for blob detection [dBZ] |
| `CORES_MIN_RANGE` | `10000.0` | float | Min centroid range from radar [m] |
| `CORES_MIN_Z_UPDRAFT` | `58.0` | float | Violent updraft max-dBZ gate [dBZ] |
| `CORES_DEDUP_RADIUS` | `5000.0` | float | Weighted-mean merge radius for cores [m] |
| `CORES_RHOHV_THRESHOLD` | `0.85` | float | Min mean RhoHV to pass quality gate |
| `CORES_MIN_PIXELS` | `4` | int | Min blob pixel count for RhoHV gate path |
| `CORES_MIN_PIXELS_UPDRAFT` | `6` | int | Min blob pixel count for updraft gate path |
| `TOPS_DEDUP_RADIUS_M` | `2000.0` | float | Cylindrical search radius around each core [m] |

> `TOPS_DEDUP_RADIUS_M` is the **cylindrical search radius** used by `detect_tops_from_cores`,
> not a deduplication parameter. There is no deduplication step for tops — one top is returned
> per core.

### Other Service-Layer Config Keys (app/config.py)

| Key | Default | Type | Description |
|---|---|---|---|
| `FIELD_VALUE_MASKS` | `{}` | dict | Per-field value threshold masks applied post-interpolation. `Dict[field, {"min": float, "max": float}]`. Values outside range are masked to NaN in the COG. Example: `{"RHOHV": {"min": 0.3}}` |
| `ROI_PARAMS_VOL01` | `None` | dict or None | Override geometry parameters for volume 01 |
| `ROI_PARAMS_VOL02` | `None` | dict or None | Override geometry parameters for volume 02 |

### Rules When Adding New Config Keys
1. **Service-layer settings** (daemon toggles, poll intervals, retention days, paths, FTP):
   → Add to `_DEFAULTS` in `app/config.py` only.
2. **radarlib internal settings** (colormaps, GRC thresholds, geometry params):
   → Add to `DEFAULTS` in `src/radarlib/config.py` only, plus a convenience attribute.
3. **Settings shared by both layers** (e.g. a new cleanup param):
   → Add to BOTH files with matching default values.
4. **Never** add `app/config.py` imports inside `src/radarlib/config.py` — radarlib
   must remain usable as a standalone library without the Genpro25 service layer.
5. After adding a key, add the matching entry in `genpro25.yml` (even if `None`).

### Public API (app/config.py)
```python
import config
config.FTP_HOST              # module-level attribute (any key in _DEFAULTS)
config.get_config(key)       # safe getter with optional default
config.get_all_config()      # returns copy of full merged dict
```

---

## SDD Workflow — Follow This Every Time
When I give you a task, strictly follow this cycle:

### 1. PROPOSAL
- Read this file fully
- Read the relevant section of `docs/radarlib_EN.md`
- Read the relevant source files
- Propose the code changes
- Flag any conflict with the Output Contract immediately
- Do not violate the coding conventions above

### 2. APPLY
- Wait for my approval or feedback
- Adjust based on my response
- Provide final code only after I confirm

### 3. ARCHIVE
- After code is applied, explicitly tell me:
  - Does `docs/radarlib_EN.md` need to be updated?
  - Does this `copilot-instructions.md` need to be updated?
  - Does the Output Contract need to be updated?
