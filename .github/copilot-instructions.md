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
- ❌ `genpro25.yml` configuration is poorly documented

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
