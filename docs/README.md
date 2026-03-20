# radarlib

![Version](https://img.shields.io/badge/version-0.1.0-blue)
![Python](https://img.shields.io/badge/python-%3E%3D3.11-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

## Overview

**radarlib** is a professional Python library for fetching, processing, and
visualizing meteorological radar data.  It is developed and maintained by
**Grupo Radar Córdoba (GRC)** and is designed to serve both operational
systems and research workflows.

### What radarlib solves

Weather radar networks continuously produce large volumes of binary data in
proprietary or specialized formats (BUFR, NetCDF, IRIS/SIGMET, etc.).
Turning raw radar scans into actionable products — geospatially referenced
PNG maps or Cloud-Optimized GeoTIFF rasters suitable for downstream
services — requires a reliable, automated pipeline.  `radarlib` provides
exactly that pipeline:

```
FTP server (BUFR)
      │
      ▼
DownloadDaemon          ← monitors remote FTP, checksums, retries
      │
      ▼  (SQLite state DB)
ProcessingDaemon        ← decodes BUFR → NetCDF volumes via PyART
      │
      ▼  (SQLite state DB)
ProductGenerationDaemon ← renders PNG images / Cloud-Optimized GeoTIFFs
      │
      ▼  (SQLite state DB)
CleanupDaemon           ← enforces data-retention policies
```

### Supported data formats and sources

| Category | Details |
|---|---|
| **Input format** | BUFR (Binary Universal Form for the Representation of meteorological data) |
| **Intermediate format** | NetCDF-4 / CF-Radial (via arm-pyart) |
| **Output formats** | PNG images, Cloud-Optimized GeoTIFF (COG) |
| **Data transport** | Asynchronous FTP (via `aioftp`) |
| **Radar network** | Argentina's SINARAME network (RMA* codes) and any BUFR-compatible radar |

### High-level architecture

| Module | Role |
|---|---|
| `radarlib.daemons` | Background async workers (download → process → product → cleanup) |
| `radarlib.io.bufr` | Low-level BUFR decoding using Fortran/C shared libraries |
| `radarlib.io.ftp` | Async FTP client with retry and checksum verification |
| `radarlib.io.pyart` | PyART integration: field filtering, COLMAX computation, PNG/GeoTIFF export |
| `radarlib.radar_grid` | Pre-computed polar-to-Cartesian grid engine |
| `radarlib.state` | SQLite and JSON file state tracking for pipeline coordination |
| `radarlib.colormaps` | Custom matplotlib colormaps tuned for dual-polarisation radar |
| `radarlib.config` | Centralised configuration with JSON/env-var/YAML override chain |
| `radarlib.utils` | File naming helpers, field-type utilities, grid utilities |

---

## Key Features

- **Complete end-to-end pipeline** — BUFR download through product delivery in
  a single orchestrated service.
- **Async architecture** — every daemon runs as a `asyncio` coroutine for
  concurrent, non-blocking I/O.
- **Fault-tolerant state management** — SQLite-backed `StateTracker` allows
  automatic resume after crashes; no file is processed twice.
- **BUFR decoding** — high-performance BUFR reader wrapping C/Fortran
  shared libraries bundled with the package.
- **PyART integration** — reads decoded data into `pyart.core.Radar` objects;
  supports arbitrary field filtering pipelines.
- **COLMAX computation** — column-maximum reflectivity with configurable
  elevation limit, RhoHV/WRAD/ZDR quality filters.
- **Dual output modes** — render radar sweeps as PNG images *or* as
  Cloud-Optimized GeoTIFFs with proper EPSG:4326 georeference.
- **Custom colormaps** — 8+ GRC-tuned colormaps for reflectivity, velocity,
  ZDR, RhoHV, PhiDP, KDP, etc.
- **Flexible configuration** — multi-level override chain (env vars → JSON
  file → YAML file → hard-coded defaults).
- **Docker-ready** — `Dockerfile` and `docker-compose.yml` allow multi-radar
  deployment without code changes.
- **Comprehensive tests** — 43 test files covering unit and integration
  scenarios with `pytest` + `pytest-asyncio`.

---

## Requirements and Dependencies

### Python version

Python **≥ 3.11**, **< 4.0** is required.

### Required runtime dependencies

| Package | Minimum version | Purpose |
|---|---|---|
| `arm-pyart` | ≥ 2.1.1 | Radar data model and I/O |
| `numpy` | ≥ 2.3.5 | Array computing |
| `pandas` | ≥ 2.3.3 | Data manipulation |
| `xarray` | ≥ 2024.0.0 | Labelled N-D arrays / NetCDF |
| `netcdf4` | ≥ 1.7.0 | NetCDF-4 read/write |
| `scipy` | ≥ 1.14.0 | Interpolation and signal processing |
| `matplotlib` | ≥ 3.9.0 | Plotting and colormap engine |
| `pillow` | ≥ 10.0.0 | PNG image encoding |
| `rasterio` | ≥ 1.3.0 | GeoTIFF read/write |
| `GDAL` | = 3.10.3 | Geospatial library (required by rasterio) |
| `pyproj` | ≥ 3.0.0 | Coordinate reference system transformations |
| `affine` | ≥ 2.0.0 | Affine transform helpers |
| `aioftp` | ≥ 0.22.0 | Async FTP client |
| `cachetools` | ≥ 5.0.0 | LRU caching utilities |
| `pytz` | ≥ 2025.2 | Timezone handling |

### Development / testing dependencies

| Package | Purpose |
|---|---|
| `pytest` | Test runner |
| `pytest-asyncio` | Async test support |
| `flake8` | Linting |
| `black` | Code formatting |
| `mypy` | Static type checking |
| `tox` | Test environment management |

### System dependencies (Linux)

GDAL native libraries must be installed:

```bash
apt-get install -y gdal-bin libgdal-dev build-essential
```

---

## Installation

### From source (development)

```bash
# 1. Clone the repository
git clone https://github.com/jgmarti84/gl-radarlib.git
cd gl-radarlib

# 2. Create and activate a virtual environment (Python ≥ 3.11)
python3 -m venv venv
source venv/bin/activate

# 3. Install system dependencies (Debian/Ubuntu)
sudo apt-get install -y gdal-bin libgdal-dev build-essential git

# 4. Install the package in editable mode with all dependencies
pip install -e .

# 5. (Optional) Install development dependencies
pip install -r requirements-dev.txt
```

### Using the Makefile

```bash
make venv       # Creates venv
make setup      # Installs all dependencies + pre-commit hooks
make test       # Runs the test suite
make lint       # Runs flake8
```

### Credentials and environment setup

Provide FTP credentials either as environment variables or via the
`genpro25.yml` configuration file (see
[Configuration Reference](#configuration-reference)):

```bash
export FTP_HOST="ftp.your-radar-server.example.com"
export FTP_USER="your_username"
export FTP_PASS="your_password"
```

---

## Quick Start

The following example starts the full pipeline for one radar site:

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

config = DaemonManagerConfig(
    radar_name="RMA1",
    base_path=Path("/data/radares/RMA1"),
    ftp_host="ftp.example.com",
    ftp_user="radar_user",
    ftp_password="radar_pass",
    ftp_base_path="/L2",
    volume_types={
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        }
    },
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    product_type="geotiff",
    add_colmax=True,
)

manager = DaemonManager(config)
asyncio.run(manager.start())
```

---

## Module Reference

### `radarlib.config`

Centralised configuration module.  Loads settings from (in priority order):

1. `RADARLIB_CONFIG` env var pointing to a JSON file
2. Individual environment variables matching config key names
3. Package defaults

#### Public attributes

All attributes listed below are available directly on the module after import:

```python
import radarlib.config as cfg
print(cfg.FTP_HOST)
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `BUFR_RESOURCES_PATH` | `str` | `<package>/io/bufr/bufr_resources` | Path to bundled BUFR decoding resources |
| `ROOT_CACHE_PATH` | `str` | `~/workspaces/radarlib/cache` | Cache directory |
| `ROOT_RADAR_FILES_PATH` | `str` | `~/workspaces/radarlib/data/radares` | Base path for raw radar (BUFR/NetCDF) files |
| `ROOT_RADAR_PRODUCTS_PATH` | `str` | `~/workspaces/radarlib/product_output` | Base path for generated products |
| `ROOT_LOGS_PATH` | `str` | `~/workspaces/radarlib/logs` | Log directory |
| `ROOT_GATE_COORDS_PATH` | `str` | `~/workspaces/radarlib/data/gate_coordinates` | Precomputed gate coordinates |
| `ROOT_GEOMETRY_PATH` | `str` | `~/workspaces/radarlib/data/geometries` | Geometry files directory |
| `FTP_HOST` | `str` | `"www.example.com"` | FTP server hostname |
| `FTP_USER` | `str` | `"example_user"` | FTP username |
| `FTP_PASS` | `str` | `"secret"` | FTP password |
| `VOLUME_TYPES` | `dict` | `{"0315": {"01": [...], "02": [...]}}` | BUFR volume type → field mapping |
| `COLMAX_THRESHOLD` | `float` | `-3` | Minimum reflectivity (dBZ) for COLMAX |
| `COLMAX_ELEV_LIMIT1` | `float` | `0.65` | Minimum elevation (°) included in COLMAX |
| `COLMAX_RHOHV_FILTER` | `bool` | `True` | Enable RhoHV quality filter for COLMAX |
| `COLMAX_RHOHV_UMBRAL` | `float` | `0.8` | RhoHV threshold (gates below this are masked) |
| `COLMAX_WRAD_FILTER` | `bool` | `True` | Enable spectrum-width quality filter |
| `COLMAX_WRAD_UMBRAL` | `float` | `4.6` | Spectrum-width threshold (m/s) |
| `COLMAX_TDR_FILTER` | `bool` | `True` | Enable ZDR quality filter |
| `COLMAX_TDR_UMBRAL` | `float` | `8.5` | ZDR threshold (dB) |
| `GRC_RHV_FILTER` | `bool` | `True` | Enable RhoHV-based ground-clutter filter |
| `GRC_RHV_THRESHOLD` | `float` | `0.55` | RhoHV clutter threshold |
| `GRC_WRAD_FILTER` | `bool` | `True` | Enable spectrum-width clutter filter |
| `GRC_WRAD_THRESHOLD` | `float` | `4.6` | Spectrum-width clutter threshold |
| `GRC_REFL_FILTER` | `bool` | `True` | Enable low-reflectivity filter |
| `GRC_REFL_THRESHOLD` | `float` | `-3` | Low-reflectivity threshold (dBZ) |
| `GRC_ZDR_FILTER` | `bool` | `True` | Enable ZDR outlier filter |
| `GRC_ZDR_THRESHOLD` | `float` | `8.5` | ZDR outlier threshold (dB) |
| `GRC_REFL_FILTER2` | `bool` | `True` | Enable second reflectivity filter |
| `GRC_REFL_THRESHOLD2` | `float` | `25` | Second reflectivity threshold (dBZ) |
| `GRC_CM_FILTER` | `bool` | `True` | Enable cross-moment consistency filter |
| `GRC_RHOHV_THRESHOLD2` | `float` | `0.85` | Second RhoHV threshold |
| `GRC_DESPECKLE_FILTER` | `bool` | `True` | Enable speckle removal filter |
| `GRC_MEAN_FILTER` | `bool` | `True` | Enable mean-field smoothing filter |
| `GRC_MEAN_THRESHOLD` | `float` | `0.85` | Mean-field smoothing threshold |
| `FIELDS_TO_PLOT` | `list` | `["DBZH", "ZDR", "RHOHV", "COLMAX"]` | Fields rendered in PNG products |
| `FILTERED_FIELDS_TO_PLOT` | `list` | `["DBZH", "ZDR", "COLMAX", ...]` | Fields rendered in filtered-PNG products |
| `PNG_DPI` | `int` | `72` | Output PNG resolution (dots per inch) |
| `GEOMETRY_RES` | `float` | `1200.0` | Grid resolution (metres) |
| `GEOMETRY_TOA` | `float` | `12000.0` | Top-of-atmosphere height for geometry (metres) |
| `GEOMETRY_HFAC` | `float` | `0.017` | Geometry height factor |
| `GEOMETRY_MIN_RADIUS` | `float` | `250.0` | Minimum range gate radius (metres) |
| `GEOMETRY_BUFR_LOOKBACK_HOURS` | `int` | `72` | Hours to look back when searching for geometry files |

#### Functions

```python
radarlib.config.get(key: str, default: Any = None) -> Any
```

Retrieve a single configuration value by name.

```python
radarlib.config.reload(path: Optional[str] = None) -> None
```

Force reload of all configuration.  If `path` is given it is tried first.

---

### `radarlib.daemons`

Background worker processes forming the processing pipeline.

#### `DaemonManagerConfig`

Dataclass that bundles all configuration for a single radar-instance deployment.

```python
from radarlib.daemons import DaemonManagerConfig

config = DaemonManagerConfig(
    radar_name="RMA1",           # Radar site code
    base_path=Path("/data/RMA1"),
    ftp_host="ftp.example.com",
    ftp_user="user",
    ftp_password="pass",
    ftp_base_path="/L2",
    volume_types={...},
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
)
```

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `radar_name` | `str` | required | Radar site code (e.g., `"RMA1"`) |
| `base_path` | `Path` | required | Root directory for all pipeline data |
| `ftp_host` | `str` | required | FTP server hostname |
| `ftp_user` | `str` | required | FTP username |
| `ftp_password` | `str` | required | FTP password |
| `ftp_base_path` | `str` | required | Remote base path on FTP server |
| `volume_types` | `dict` | required | BUFR volume code → sweep code → field list |
| `start_date` | `datetime` | `now()` UTC | Earliest timestamp to consider for downloads |
| `download_poll_interval` | `int` | `60` | Seconds between FTP polling cycles |
| `processing_poll_interval` | `int` | `30` | Seconds between BUFR→NetCDF processing cycles |
| `product_poll_interval` | `int` | `30` | Seconds between product-generation cycles |
| `cleanup_poll_interval` | `int` | `1800` | Seconds between cleanup cycles |
| `enable_download_daemon` | `bool` | `True` | Start the download daemon |
| `enable_processing_daemon` | `bool` | `True` | Start the processing daemon |
| `enable_product_daemon` | `bool` | `True` | Start the product-generation daemon |
| `enable_cleanup_daemon` | `bool` | `False` | Start the cleanup daemon (disabled by default) |
| `product_type` | `str` | `"image"` | Output product type: `"image"` or `"geotiff"` |
| `add_colmax` | `bool` | `True` | Include COLMAX field in product output |
| `bufr_retention_days` | `int` | `7` | Days before BUFR files are deleted |
| `netcdf_retention_days` | `int` | `7` | Days before NetCDF files are deleted |
| `product_dir` | `Path \| None` | `base_path/products` | Override for product output directory |

#### `DaemonManager`

Orchestrates all daemons.

```python
from radarlib.daemons import DaemonManager

manager = DaemonManager(config)

# Start all enabled daemons (async)
await manager.start()

# Stop all running daemons (sync)
manager.stop()

# Query status
status = manager.get_status()
# Returns dict: {"radar_code", "base_path", "download_daemon": {...}, ...}
```

**Methods**

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(config: DaemonManagerConfig)` | Create manager; creates required directories |
| `start` | `async () -> None` | Start all enabled daemons concurrently |
| `stop` | `() -> None` | Stop all running daemons |
| `get_status` | `() -> dict` | Return runtime status dictionary |

#### `DownloadDaemon`

Polls an FTP server, discovers new BUFR files, checksums them, and downloads
them to the local `bufr/` directory.  Uses `SQLiteStateTracker` to track
download state and avoid duplicates.

#### `ProcessingDaemon`

Watches the local BUFR directory for `downloaded` files, decodes them using
`radarlib.io.bufr`, assembles complete radar volumes, and writes NetCDF-4
files to the `netcdf/` directory.

#### `ProductGenerationDaemon`

Watches the `netcdf/` directory for `processed` volumes, loads them via PyART,
applies GRC quality filters, optionally computes COLMAX, and writes PNG images
or Cloud-Optimized GeoTIFFs to the product directory.

#### `CleanupDaemon`

Periodically scans the state database for files whose product generation is
complete and whose age exceeds configured retention thresholds.  Deletes
matching BUFR and NetCDF files to free disk space.

---

### `radarlib.io.bufr`

Low-level BUFR file decoder.  Wraps Fortran/C shared libraries bundled in the
`bufr_resources/` sub-package.

```python
from radarlib.io.bufr import BufrFile

with BufrFile("/data/RMA1_0315_01_DBZH_20250101T120000Z.BUFR") as bufr:
    data = bufr.read()          # Returns structured numpy arrays
    metadata = bufr.metadata    # Radar site metadata
```

**Key classes**

| Class | Description |
|---|---|
| `BufrFile` | Context-manager wrapper around a single BUFR file |
| `BufrDecoder` | Low-level decoder; called internally by `BufrFile` |
| `BufrVolume` | Aggregates multiple sweeps into a complete volume |

---

### `radarlib.io.ftp`

Async FTP client for radar data retrieval.

```python
from radarlib.io.ftp import FTPClient

async with FTPClient(host="ftp.example.com", user="u", password="p") as ftp:
    files = await ftp.list_files("/L2/RMA1/")
    await ftp.download("/L2/RMA1/file.BUFR", "/local/path/file.BUFR")
```

**Key classes**

| Class | Description |
|---|---|
| `FTPClient` | Async FTP client with retry and checksum support |
| `FTPClientConfig` | Configuration dataclass for `FTPClient` |

**Features**

- Exponential-back-off retry on connection errors
- MD5 checksum verification after download
- Directory listing with glob-pattern filtering

---

### `radarlib.io.pyart`

PyART integration layer: field filtering, COLMAX computation, and image export.

```python
from radarlib.io.pyart import (
    apply_grc_filters,
    compute_colmax,
    save_png,
    save_geotiff,
)
import pyart

radar = pyart.io.read_cfradial("/data/RMA1/netcdf/volume.nc")

# Apply quality filters
filtered = apply_grc_filters(radar)

# Compute column-maximum reflectivity
colmax = compute_colmax(filtered)

# Export PNG image
save_png(radar, field="DBZH", output_path="/products/DBZH.png")

# Export Cloud-Optimized GeoTIFF
save_geotiff(colmax, output_path="/products/COLMAX.tif")
```

**Key functions**

| Function | Description |
|---|---|
| `apply_grc_filters(radar)` | Apply RhoHV, WRAD, REFL, ZDR, despeckle, and mean filters |
| `compute_colmax(radar, ...)` | Compute column-maximum reflectivity sweep |
| `save_png(radar, field, ...)` | Render a sweep to a PNG image |
| `save_geotiff(grid, ...)` | Write a grid to a Cloud-Optimized GeoTIFF |
| `filter_by_rhohv(radar, ...)` | Mask gates where RhoHV < threshold |
| `filter_by_wrad(radar, ...)` | Mask gates where WRAD > threshold |

---

### `radarlib.radar_grid`

Pre-computes polar-to-Cartesian interpolation geometry for fast repeated
gridding.

```python
from radarlib.radar_grid import RadarGrid

grid = RadarGrid.from_radar(radar, resolution=1200.0)
cartesian = grid.interpolate(radar.fields["DBZH"]["data"])
```

**Key classes**

| Class | Description |
|---|---|
| `RadarGrid` | Holds precomputed geometry; provides `interpolate()` method |
| `GateGrid` | Low-level gate coordinate container |

---

### `radarlib.state`

Pipeline coordination via persistent state tracking.

```python
from radarlib.state import SQLiteStateTracker

tracker = SQLiteStateTracker("/data/state.db")

# Mark a file as downloaded
tracker.set_state("file.BUFR", "downloaded")

# Query state
state = tracker.get_state("file.BUFR")  # e.g. "downloaded", "processed", "product_ready"

# List all files in a given state
pending = tracker.list_by_state("downloaded")
```

**States**

| State | Meaning |
|---|---|
| `"discovered"` | File found on FTP |
| `"downloaded"` | File saved locally |
| `"processed"` | BUFR decoded to NetCDF |
| `"product_ready"` | PNG/GeoTIFF written |
| `"cleanup_done"` | Raw files deleted |

---

### `radarlib.colormaps`

Custom matplotlib colormaps for dual-polarisation radar fields.

```python
import matplotlib.pyplot as plt
import radarlib.colormaps  # registers colormaps on import

# Use colormaps by name in any matplotlib call
plt.pcolormesh(data, cmap="grc_th")
plt.pcolormesh(data, cmap="grc_vrad")
```

**Available colormaps**

| Name | Field | Description |
|---|---|---|
| `grc_th` | Reflectivity | GRC reflectivity colormap |
| `grc_vrad` | Radial velocity | Diverging velocity colormap |
| `grc_rho` | RhoHV | Co-polar correlation |
| `grc_zdr` | ZDR | Differential reflectivity |
| `grc_phidp` | PhiDP | Differential phase |
| `grc_kdp` | KDP | Specific differential phase |
| `grc_wrad` | WRAD | Spectrum width |
| `grc_cm` | Clutter mask | Boolean clutter field |

---

### `radarlib.utils`

Utility functions used across the library.

**`radarlib.utils.names_utils`**

```python
from radarlib.utils.names_utils import parse_bufr_filename, build_bufr_filename

# Parse a BUFR filename into components
parts = parse_bufr_filename("RMA1_0315_01_DBZH_20250101T120000Z.BUFR")
# Returns: {"radar": "RMA1", "vol_code": "0315", "sweep": "01",
#           "field": "DBZH", "timestamp": datetime(...)}

# Build a BUFR filename from components
name = build_bufr_filename(radar="RMA1", vol_code="0315", sweep="01",
                           field="DBZH", timestamp=dt)
```

**`radarlib.utils.fields_utils`**

```python
from radarlib.utils.fields_utils import get_field_type, is_dual_pol_field

field_type = get_field_type("DBZH")  # returns "reflectivity"
is_dp = is_dual_pol_field("ZDR")     # returns True
```

---

## Usage Examples

### Example 1: Running the full pipeline for a single radar

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

config = DaemonManagerConfig(
    radar_name="RMA1",
    base_path=Path("/data/radares/RMA1"),
    ftp_host="200.16.116.24",
    ftp_user="radar_user",
    ftp_password="secret",
    ftp_base_path="/L2",
    volume_types={
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        }
    },
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    product_type="geotiff",
    add_colmax=True,
    enable_cleanup_daemon=True,
    bufr_retention_days=1,
    netcdf_retention_days=1,
)

manager = DaemonManager(config)

try:
    asyncio.run(manager.start())
except KeyboardInterrupt:
    manager.stop()
```

---

### Example 2: Decoding a BUFR file and reading metadata

```python
from radarlib.io.bufr import BufrFile

path = "/data/RMA1/bufr/RMA1_0315_01_DBZH_20250101T120000Z.BUFR"

with BufrFile(path) as bufr:
    metadata = bufr.metadata
    print(f"Radar: {metadata['radar_name']}")
    print(f"Site lat/lon: {metadata['latitude']}, {metadata['longitude']}")
    print(f"Elevation: {metadata['elevation_angle']}°")

    data = bufr.read()
    print(f"Data shape: {data.shape}")
    print(f"Range gates: {data.shape[1]}, Azimuth bins: {data.shape[0]}")
```

---

### Example 3: Downloading files from FTP

```python
import asyncio
from radarlib.io.ftp import FTPClient

async def download_latest(radar_name: str):
    async with FTPClient(
        host="ftp.example.com",
        user="radar_user",
        password="secret",
    ) as ftp:
        remote_path = f"/L2/{radar_name}/"
        files = await ftp.list_files(remote_path)

        for remote_file in files[-5:]:   # last 5 files
            local_path = f"/data/{radar_name}/bufr/{remote_file.split('/')[-1]}"
            await ftp.download(remote_file, local_path)
            print(f"Downloaded: {local_path}")

asyncio.run(download_latest("RMA1"))
```

---

### Example 4: Applying filters and exporting a PNG image

```python
import pyart
from radarlib.io.pyart import apply_grc_filters, save_png

# Load a NetCDF volume produced by ProcessingDaemon
radar = pyart.io.read_cfradial("/data/RMA1/netcdf/20250101T120000Z.nc")

# Apply GRC quality filters (RhoHV, WRAD, REFL, ZDR, despeckle, mean)
filtered_radar = apply_grc_filters(radar)

# Render the first sweep of DBZH to PNG
save_png(
    filtered_radar,
    field="DBZH",
    sweep_index=0,
    output_path="/products/RMA1/DBZH_20250101T120000Z.png",
    vmin=-20,
    vmax=70,
    cmap="grc_th",
    dpi=72,
)
print("PNG exported successfully.")
```

---

### Example 5: Computing and exporting a COLMAX Cloud-Optimized GeoTIFF

```python
import pyart
from radarlib.io.pyart import apply_grc_filters, compute_colmax, save_geotiff

radar = pyart.io.read_cfradial("/data/RMA1/netcdf/20250101T120000Z.nc")
filtered_radar = apply_grc_filters(radar)

# Compute column-maximum reflectivity
colmax_grid = compute_colmax(
    filtered_radar,
    threshold=-3.0,       # minimum dBZ included
    elev_limit=0.65,      # minimum elevation (degrees)
)

# Write Cloud-Optimized GeoTIFF (EPSG:4326)
save_geotiff(
    colmax_grid,
    output_path="/products/RMA1/COLMAX_20250101T120000Z.tif",
    crs="EPSG:4326",
)
print("COG exported successfully.")
```

---

### Example 6: Using custom colormaps in matplotlib

```python
import numpy as np
import matplotlib.pyplot as plt
import radarlib.colormaps   # registers all GRC colormaps

# Simulated reflectivity data
data = np.random.uniform(-20, 70, (360, 240))

fig, ax = plt.subplots(figsize=(8, 8))
mesh = ax.pcolormesh(data, cmap="grc_th", vmin=-20, vmax=70)
plt.colorbar(mesh, ax=ax, label="Reflectivity (dBZ)")
ax.set_title("Simulated DBZH")
plt.savefig("reflectivity.png", dpi=150)
```

---

## Configuration Reference

### Environment variables

The following environment variables are recognized by `radarlib.config` and
can be used to override built-in defaults.  They are also used by
`app/config.py` when deploying via Docker Compose.

| Variable | Type | Default | Description |
|---|---|---|---|
| `RADARLIB_CONFIG` | path | — | Path to a JSON config file that overrides defaults |
| `ROOT_CACHE_PATH` | path | see above | Cache directory |
| `ROOT_RADAR_FILES_PATH` | path | see above | Root for raw radar files |
| `ROOT_RADAR_PRODUCTS_PATH` | path | see above | Root for generated products |
| `ROOT_LOGS_PATH` | path | see above | Log output directory |
| `ROOT_GATE_COORDS_PATH` | path | see above | Gate coordinate files |
| `FTP_HOST` | string | `"www.example.com"` | FTP server hostname |
| `FTP_USER` | string | `"example_user"` | FTP username |
| `FTP_PASS` | string | `"secret"` | FTP password |
| `COLMAX_THRESHOLD` | float | `-3` | COLMAX minimum reflectivity (dBZ) |
| `COLMAX_ELEV_LIMIT1` | float | `0.65` | COLMAX minimum elevation (°) |
| `GRC_RHV_THRESHOLD` | float | `0.55` | RhoHV filter threshold |
| `GRC_WRAD_THRESHOLD` | float | `4.6` | Spectrum-width filter threshold |
| `PNG_DPI` | int | `72` | PNG output resolution |

### JSON configuration file

Point `RADARLIB_CONFIG` at a JSON file for bulk overrides:

```json
{
  "FTP_HOST": "ftp.my-radar.example.com",
  "FTP_USER": "radar",
  "FTP_PASS": "my_secret",
  "PNG_DPI": 100,
  "COLMAX_THRESHOLD": -5
}
```

### YAML application configuration (`genpro25.yml`)

The application layer uses a multi-environment YAML file.  Select the active
environment with `GENPRO25_ENV` (default: `local`).

```yaml
local:
  FTP:
    FTP_HOST: "ftp.example.com"
    FTP_USER: "radar_user"
    FTP_PASS: "secret"
  DAEMON_PARAMS:
    START_DATE: "2025-01-01T00:00:00Z"
    ENABLE_DOWNLOAD_DAEMON: true
    ENABLE_PROCESSING_DAEMON: true
    ENABLE_PRODUCT_DAEMON: true
    ENABLE_CLEANUP_DAEMON: false
    DOWNLOAD_POLL_INTERVAL: 60
    PROCESSING_POLL_INTERVAL: 30
    PRODUCT_POLL_INTERVAL: 30
    CLEANUP_POLL_INTERVAL: 1800
    PRODUCT_TYPE: "geotiff"
    ADD_COLMAX: true
    NETCDF_RETENTION_DAYS: 7
    BUFR_RETENTION_DAYS: 7
```

---

## Error Handling

### Common exceptions

| Exception | Module | Cause | Handling |
|---|---|---|---|
| `FileNotFoundError` | `app/config.py` | `GENPRO25_CONFIG` path does not exist | Ensure the YAML config is mounted correctly |
| `DownloadDaemonError` | `radarlib.daemons` | Unrecoverable FTP error | Daemon logs the error and retries on next poll cycle |
| `ValueError` | `DaemonManagerConfig` | `start_date` is not timezone-aware | Always pass `tzinfo=timezone.utc` |
| `ConnectionRefusedError` | `radarlib.io.ftp` | FTP server unreachable | `FTPClient` retries with exponential back-off |
| `OSError` | `radarlib.io.bufr` | Corrupt or incomplete BUFR file | File is marked `"failed"` in state DB; pipeline continues |

### Best practices

```python
import asyncio
from radarlib.daemons import DaemonManager

manager = DaemonManager(config)
try:
    asyncio.run(manager.start())
except KeyboardInterrupt:
    # Graceful shutdown on Ctrl-C
    manager.stop()
except Exception as exc:
    # Log unexpected errors and stop cleanly
    import logging
    logging.exception("Fatal error: %s", exc)
    manager.stop()
    raise
```

---

---

## Deployment Guide

### Overview

`radarlib` is deployed as a **stateless Docker container per radar instance**.
Each container runs `app/main.py`, which:

1. Reads `genpro25.yml` (mounted as a read-only volume)
2. Merges YAML settings with `radarlib` defaults and environment variables
3. Constructs a `DaemonManager` and starts all enabled daemons asynchronously

A single Docker image can serve any number of radar sites by changing the
`RADAR_NAME` environment variable and the FTP credentials.

### Docker Compose Deployment

#### Prerequisites

- Docker Engine ≥ 24.0 and Docker Compose ≥ 2.20
- GDAL 3.10.3 native libraries (installed in the image)
- Valid FTP credentials for the target radar network
- A populated `genpro25.yml` configuration file

#### Running a radar instance

```bash
# Start all services defined in docker-compose.yml
docker compose up -d

# Follow logs for a specific radar instance
docker compose logs -f genpro25
```

#### Stopping and restarting

```bash
# Stop all containers (data is preserved in volumes)
docker compose down

# Restart a single service
docker compose restart genpro25

# Pull a new image and recreate containers
docker compose pull && docker compose up -d
```

---

### Complete Configuration Reference

The following variables are used by `app/main.py` and `app/config.py`.

---

#### `RADAR_NAME`

- **Description**: Identifies the radar site.  Used to construct file paths, log directories, and FTP remote paths.
- **Type**: string
- **Required**: No
- **Default**: `"RMA2"`
- **Example value**: `RADAR_NAME=RMA1`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `app/main.py`, `DaemonManagerConfig.radar_name`, log formatter
- **Notes**: Must match the radar code used in BUFR filenames and on the FTP server.

---

#### `GENPRO25_CONFIG`

- **Description**: Absolute path to the `genpro25.yml` YAML configuration file inside the container.
- **Type**: path
- **Required**: No
- **Default**: `"/workspace/app/genpro25.yml"`
- **Example value**: `GENPRO25_CONFIG=/workspace/app/genpro25.yml`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `app/config.py` on startup
- **Notes**: The file must be mounted as a volume (see [Volume and Mount Reference](#volume-and-mount-reference)).

---

#### `GENPRO25_ENV`

- **Description**: Selects the environment block (`local`, `stg`, `prd`) within `genpro25.yml`.
- **Type**: string
- **Required**: No
- **Default**: `"local"`
- **Example value**: `GENPRO25_ENV=prd`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `app/config.py` (`_GENPRO25_ENV`)
- **Notes**: The chosen key must exist as a top-level key in `genpro25.yml`.

---

#### `ROOT_CACHE_PATH`

- **Description**: Directory used for caching intermediate data.
- **Type**: path
- **Required**: No
- **Default**: `"/workspace/app/cache"` (via env override in docker-compose)
- **Example value**: `ROOT_CACHE_PATH=/workspace/app/cache`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `radarlib.config`, various daemons
- **Notes**: Created automatically by the container if it does not exist.

---

#### `ROOT_RADAR_FILES_PATH`

- **Description**: Root directory where raw radar files (BUFR, NetCDF) are stored, organised by radar name.
- **Type**: path
- **Required**: No
- **Default**: `"/workspace/app/data/radares"`
- **Example value**: `ROOT_RADAR_FILES_PATH=/workspace/app/data/radares`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `app/main.py` → `DaemonManagerConfig.base_path`
- **Notes**: Each radar gets a sub-directory: `<ROOT_RADAR_FILES_PATH>/<RADAR_NAME>/`.

---

#### `ROOT_RADAR_PRODUCTS_PATH`

- **Description**: Root directory where generated PNG / GeoTIFF products are written.
- **Type**: path
- **Required**: No
- **Default**: `"/workspace/app/product_output"`
- **Example value**: `ROOT_RADAR_PRODUCTS_PATH=/workspace/app/product_output`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `app/main.py` → `DaemonManagerConfig.product_dir`
- **Notes**: Should be bind-mounted to a host path to preserve products across container restarts (see docker-compose volumes).

---

#### `ROOT_GATE_COORDS_PATH`

- **Description**: Directory containing pre-computed gate coordinate files used for fast gridding.
- **Type**: path
- **Required**: No
- **Default**: `"/workspace/app/data/gate_coordinates"`
- **Example value**: `ROOT_GATE_COORDS_PATH=/workspace/app/data/gate_coordinates`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `radarlib.radar_grid`

---

#### `ROOT_LOGS_PATH`

- **Description**: Root log directory.  A sub-directory per radar is created automatically.
- **Type**: path
- **Required**: No
- **Default**: `"/workspace/app/logs"`
- **Example value**: `ROOT_LOGS_PATH=/workspace/app/logs`
- **Where to set it**: `docker-compose.yml` `environment:` section
- **Consumed by**: `app/main.py` (log file handler)
- **Notes**: The actual log file path is `<ROOT_LOGS_PATH>/<RADAR_NAME>/genpro25.log`.  Rotated daily; 7 days retained.

---

#### `PYTHONDONTWRITEBYTECODE`

- **Description**: Prevents Python from creating `.pyc` bytecode files.
- **Type**: string (`"1"` to enable)
- **Required**: No
- **Default**: `"1"` (set in Dockerfile and docker-compose)
- **Where to set it**: Dockerfile `ENV` or `docker-compose.yml` `environment:`

---

#### `PYTHONUNBUFFERED`

- **Description**: Forces Python stdout/stderr to be unbuffered, ensuring logs appear immediately in `docker logs`.
- **Type**: string (`"1"` to enable)
- **Required**: No
- **Default**: `"1"` (set in Dockerfile and docker-compose)
- **Where to set it**: Dockerfile `ENV` or `docker-compose.yml` `environment:`

---

#### Configuration variables sourced from `genpro25.yml` / `DAEMON_PARAMS`

These values are read from `genpro25.yml` under `DAEMON_PARAMS` and override
the `radarlib` defaults.

| Variable | Type | Default | Description |
|---|---|---|---|
| `START_DATE` | ISO-8601 string | current UTC time | Earliest timestamp for file download |
| `ENABLE_DOWNLOAD_DAEMON` | bool | `true` | Enable the download daemon |
| `ENABLE_PROCESSING_DAEMON` | bool | `true` | Enable the processing daemon |
| `ENABLE_PRODUCT_DAEMON` | bool | `true` | Enable the product-generation daemon |
| `ENABLE_CLEANUP_DAEMON` | bool | `true` | Enable the cleanup daemon |
| `DOWNLOAD_POLL_INTERVAL` | int (seconds) | `60` | FTP polling interval |
| `PROCESSING_POLL_INTERVAL` | int (seconds) | `30` | BUFR→NetCDF processing interval |
| `PRODUCT_POLL_INTERVAL` | int (seconds) | `30` | Product generation interval |
| `CLEANUP_POLL_INTERVAL` | int (seconds) | `1800` | Cleanup sweep interval |
| `PRODUCT_TYPE` | string | `"geotiff"` | `"image"` or `"geotiff"` |
| `ADD_COLMAX` | bool | `true` | Compute and include COLMAX |
| `NETCDF_RETENTION_DAYS` | float (days) | `0.0833` | NetCDF file retention period |
| `BUFR_RETENTION_DAYS` | float (days) | `0.0833` | BUFR file retention period |
| `GEOMETRY_BUFR_LOOKBACK_HOURS` | int | `72` | Hours to look back for geometry BUFR files |

---

### Configuration Precedence

Configuration values are resolved in the following order (highest → lowest
priority):

| Priority | Source | How to use |
|---|---|---|
| 1 | **`docker-compose.yml` `environment:`** | Set or override any `radarlib` config key directly as an environment variable. Takes effect immediately on container (re)start. |
| 2 | **`GENPRO25_ENV` environment block in `genpro25.yml`** | Override specific settings per environment (`local`, `stg`, `prd`). Mount the file and set `GENPRO25_ENV`. |
| 3 | **`genpro25.yml` `DAEMON_PARAMS` section** | Daemon-specific overrides (poll intervals, retention, product type). |
| 4 | **`RADARLIB_CONFIG` JSON file** | Provide a JSON dictionary with any `radarlib.config` key. Pointed to by the `RADARLIB_CONFIG` env var. |
| 5 | **`radarlib` built-in defaults** (`radarlib/config.py` `DEFAULTS` dict) | Hard-coded fall-back values. Change only if patching the library. |

**Recommendation**: Use `docker-compose.yml` environment variables for
deployment-specific values (`RADAR_NAME`, paths, FTP credentials) and
`genpro25.yml` for processing parameters (thresholds, intervals, product
type).

---

### Volume and Mount Reference

| Volume / Bind Mount | Host Path | Container Path | Mode | Required | Purpose |
|---|---|---|---|---|---|
| Product output | `../product_output` | `/workspace/app/product_output` | read-write | Yes | Persists generated PNG / GeoTIFF products |
| Logs | `../logs` | `/workspace/app/logs` | read-write | Yes | Persists daily-rotated log files |
| YAML config | `./genpro25.yml` | `/workspace/app/genpro25.yml` | read-only | Yes | Application configuration file |

> **Note**: BUFR and NetCDF intermediate files are stored inside the container
> at `ROOT_RADAR_FILES_PATH`.  They are not persisted to the host by default.
> If you need them for debugging, add a bind mount in `docker-compose.yml`.

---

### Secrets and Credentials Setup

FTP credentials are the only secrets required by the current deployment.

#### Development (local docker-compose)

For development it is acceptable to store credentials directly in
`genpro25.yml` under the `FTP:` section.  **Do not commit real credentials to
version control.**

```yaml
local:
  FTP:
    FTP_HOST: "ftp.example.com"
    FTP_USER: "radar_user"
    FTP_PASS: "my_secret"
```

#### Production (recommended)

In production, pass credentials via environment variables so they are not
stored on disk inside the image:

```yaml
# docker-compose.prod.yml
services:
  genpro25:
    environment:
      FTP_HOST: "${FTP_HOST}"
      FTP_USER: "${FTP_USER}"
      FTP_PASS: "${FTP_PASS}"
```

Then provide the values in a `.env` file (not committed) or via your CI/CD
secrets manager:

```bash
# .env  (git-ignored)
FTP_HOST=ftp.example.com
FTP_USER=radar_user
FTP_PASS=my_secret
```

> ⚠️ **Never hardcode credentials in `docker-compose.yml` or commit them to
> version control.**  Use `.env` files, Docker secrets, or a secrets manager
> (Vault, AWS Secrets Manager, etc.) in production.

---

### Deploying for a New Radar Site

#### Step-by-step

1. **Update `RADAR_NAME`** in `docker-compose.yml` to the new radar's code
   (e.g., `RMA5`).

2. **Update FTP credentials** if the new radar uses a different FTP server.

3. **Update `genpro25.yml`** `VOLUME_TYPES` section with the BUFR volume
   codes and field lists for the new radar.

4. **Set `START_DATE`** in `genpro25.yml` `DAEMON_PARAMS` to the earliest
   timestamp to process.

5. **Start the container**:

   ```bash
   docker compose up -d
   docker compose logs -f genpro25
   ```

#### Minimal `docker-compose.yml` snippet for a new radar

```yaml
services:
  genpro25-rma5:
    build:
      context: .
      dockerfile: ./app/Dockerfile
    container_name: genpro25-rma5
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA5"
      GENPRO25_CONFIG: "/workspace/app/genpro25.yml"
      ROOT_CACHE_PATH: "/workspace/app/cache"
      ROOT_RADAR_FILES_PATH: "/workspace/app/data/radares"
      ROOT_RADAR_PRODUCTS_PATH: "/workspace/app/product_output"
      ROOT_GATE_COORDS_PATH: "/workspace/app/data/gate_coordinates"
      ROOT_LOGS_PATH: "/workspace/app/logs"
      FTP_HOST: "ftp.rma5.example.com"
      FTP_USER: "rma5_user"
      FTP_PASS: "rma5_pass"
    working_dir: /workspace/app
    command: ["bash", "-c", "mkdir -p /workspace/app/product_output && python main.py"]
    restart: unless-stopped
```

#### Naming conventions

BUFR filenames follow the pattern:

```
<RADAR_NAME>_<VOL_CODE>_<SWEEP_NR>_<FIELD>_<TIMESTAMP>.BUFR
e.g.  RMA1_0315_01_DBZH_20250101T120000Z.BUFR
```

Products are written to:

```
<ROOT_RADAR_PRODUCTS_PATH>/<RADAR_NAME>/<FIELD>_<TIMESTAMP>.<ext>
```

#### Verifying the deployment

```bash
# Confirm the container is running
docker ps | grep genpro25

# Check startup logs
docker compose logs genpro25 | head -30

# Expected output includes:
#   Genpro25 Radar Data Processing Service Starting
#   Starting daemon manager...
#   Both download and processing daemons will start
```

---

### Running Multiple Radar Instances

Add one service block per radar in `docker-compose.yml`, varying
`RADAR_NAME`, `container_name`, and (if needed) `FTP_HOST`/`FTP_USER`/`FTP_PASS`.
All services can share the same image, log volume, and product volume.

```yaml
services:
  genpro25-rma1:
    build: { context: ., dockerfile: ./app/Dockerfile }
    container_name: genpro25-rma1
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA1"
      # ... other env vars ...
    restart: unless-stopped

  genpro25-rma3:
    build: { context: ., dockerfile: ./app/Dockerfile }
    container_name: genpro25-rma3
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA3"
      # ... other env vars ...
    restart: unless-stopped
```

Each service writes to its own sub-directory under `ROOT_RADAR_FILES_PATH`
and `ROOT_RADAR_PRODUCTS_PATH` (keyed by `RADAR_NAME`), so there are no
conflicts between instances.

---

### Build Arguments Reference

The `app/Dockerfile` currently has no `ARG` directives; all configuration is
supplied at runtime via environment variables.  The base image and GDAL
version are pinned in the `FROM` and `RUN` layers:

| Layer | Value | Description |
|---|---|---|
| Base image | `python:3.11-slim` | Minimal Debian-based Python 3.11 image |
| GDAL version | `3.10.3` (from `requirements.txt`) | Must match the native `libgdal-dev` installed in the image |

To rebuild after a dependency update:

```bash
docker compose build --no-cache
```

---

### Environment-Specific Deployment

#### Development (local)

```yaml
# docker-compose.yml — local development
services:
  genpro25:
    build:
      context: .
      dockerfile: ./app/Dockerfile
    container_name: genpro25
    volumes:
      - ../product_output:/workspace/app/product_output
      - ../logs:/workspace/app/logs
      - ./genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA1"
      GENPRO25_ENV: "local"
      GENPRO25_CONFIG: "/workspace/app/genpro25.yml"
      ROOT_CACHE_PATH: "/workspace/app/cache"
      ROOT_RADAR_FILES_PATH: "/workspace/app/data/radares"
      ROOT_RADAR_PRODUCTS_PATH: "/workspace/app/product_output"
      ROOT_GATE_COORDS_PATH: "/workspace/app/data/gate_coordinates"
      ROOT_LOGS_PATH: "/workspace/app/logs"
    working_dir: /workspace/app
    command: ["bash", "-c", "mkdir -p /workspace/app/product_output && python main.py"]
    restart: "no"
```

#### Production

```yaml
# docker-compose.prod.yml
services:
  genpro25:
    image: grc/radarlib:latest   # pre-built image from registry
    container_name: genpro25
    volumes:
      - /opt/radar/product_output:/workspace/app/product_output
      - /opt/radar/logs:/workspace/app/logs
      - /opt/radar/config/genpro25.yml:/workspace/app/genpro25.yml:ro
    environment:
      RADAR_NAME: "RMA1"
      GENPRO25_ENV: "prd"
      GENPRO25_CONFIG: "/workspace/app/genpro25.yml"
      ROOT_CACHE_PATH: "/workspace/app/cache"
      ROOT_RADAR_FILES_PATH: "/workspace/app/data/radares"
      ROOT_RADAR_PRODUCTS_PATH: "/workspace/app/product_output"
      ROOT_GATE_COORDS_PATH: "/workspace/app/data/gate_coordinates"
      ROOT_LOGS_PATH: "/workspace/app/logs"
      FTP_HOST: "${FTP_HOST}"
      FTP_USER: "${FTP_USER}"
      FTP_PASS: "${FTP_PASS}"
    restart: unless-stopped
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: "4G"
```

**Production security checklist**

- [ ] FTP credentials provided via `.env` file or secrets manager (not in YAML)
- [ ] `genpro25.yml` does not contain real credentials
- [ ] Product and log directories mounted from host or persistent volume
- [ ] Container runs as non-root user (add `user: "1000:1000"` if needed)
- [ ] Network access restricted to FTP server only

---

### Health Checks and Monitoring

The current `docker-compose.yml` does not define an explicit `healthcheck`.
To add one, append to the service definition:

```yaml
healthcheck:
  test: ["CMD", "python", "-c", "import radarlib; print('ok')"]
  interval: 60s
  timeout: 10s
  retries: 3
  start_period: 30s
```

#### Key log messages on startup

```
======================================================
Genpro25 Radar Data Processing Service Starting
======================================================
Starting daemon manager...
Both download and processing daemons will start
Press Ctrl+C to stop all daemons
```

If these lines appear, the service started successfully.

#### Checking container health

```bash
# Check running state
docker ps

# Stream logs
docker compose logs -f genpro25

# Inspect exit code of a stopped container
docker inspect genpro25 --format "{{.State.ExitCode}}"
```

---

### Troubleshooting

#### Container exits immediately

**Cause**: `genpro25.yml` not found at the path specified by `GENPRO25_CONFIG`.

**Fix**: Verify the volume mount in `docker-compose.yml`:
```yaml
volumes:
  - ./genpro25.yml:/workspace/app/genpro25.yml:ro
```
And that `./genpro25.yml` exists on the host.

---

#### `FileNotFoundError: Configuration file not found`

**Cause**: `GENPRO25_CONFIG` points to a non-existent path inside the container.

**Fix**: Check the volume mount and that `GENPRO25_CONFIG` matches the container path exactly.

---

#### FTP connection refused / timeout

**Cause**: Incorrect `FTP_HOST`, `FTP_USER`, or `FTP_PASS`; network firewall; FTP server down.

**Fix**:
1. Verify credentials in `genpro25.yml` or environment variables.
2. Test connectivity: `docker exec genpro25 nc -zv <FTP_HOST> 21`
3. Check firewall rules between the container and the FTP server.

---

#### `ValueError: start_date must be timezone-aware`

**Cause**: `START_DATE` in `genpro25.yml` is not a valid ISO-8601 UTC timestamp.

**Fix**: Use the format `"2025-01-01T00:00:00Z"` (trailing `Z` denotes UTC).

---

#### Products not appearing in output directory

**Cause**: `ENABLE_PRODUCT_DAEMON` is `false`, or the product volume mount is incorrect.

**Fix**:
1. Check `genpro25.yml` `DAEMON_PARAMS.ENABLE_PRODUCT_DAEMON: true`.
2. Verify the volume mount: `../product_output:/workspace/app/product_output`.
3. Inspect logs for `ProductGenerationDaemon` errors: `docker compose logs genpro25 | grep product`.

---

#### High disk usage

**Cause**: Cleanup daemon disabled or retention thresholds too large.

**Fix**: Enable the cleanup daemon and reduce retention:
```yaml
DAEMON_PARAMS:
  ENABLE_CLEANUP_DAEMON: true
  NETCDF_RETENTION_DAYS: 1
  BUFR_RETENTION_DAYS: 1
```

---

#### Inspecting logs

```bash
# Stream all logs
docker compose logs -f

# Stream logs for a single service
docker compose logs -f genpro25

# Last 100 lines
docker compose logs --tail=100 genpro25

# Log files on host (if mounted)
tail -f ../logs/RMA1/genpro25.log
```
