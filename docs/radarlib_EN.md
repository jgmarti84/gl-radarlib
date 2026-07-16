# radarlib — Comprehensive Documentation

---

## Table of Contents

### Operator & Deployer Guide
1. [Overview & Quick Start](#1-overview--quick-start)
2. [Installation & Setup](#2-installation--setup)
3. [Configuration Reference](#3-configuration-reference)
4. [Deployment & Operations](#4-deployment--operations)

### Developer Guide
5. [Architecture Deep Dive](#5-architecture-deep-dive)
6. [Module Reference](#6-module-reference)
7. [BUFR Processing Guide](#7-bufr-processing-guide)
8. [Integration & Advanced Examples](#8-integration--advanced-examples)

---

# 1. Overview & Quick Start {#1-overview--quick-start}

## What is radarlib?

**radarlib** is a professional Python library for fetching, processing, and visualizing meteorological radar data. It is developed and maintained by **Grupo Radar Córdoba (GRC)** and is designed to serve both operational systems and research workflows.

### The Problem It Solves

Weather radar networks continuously produce large volumes of binary data in proprietary or specialized formats (BUFR, NetCDF, IRIS/SIGMET, etc.). Turning raw radar scans into actionable products — geospatially referenced PNG maps or Cloud-Optimized GeoTIFF rasters suitable for downstream services — requires a reliable, automated pipeline. **radarlib** provides exactly that pipeline.

### Data Pipeline Overview



FTP Server (raw BUFR files)
    |
    | BUFR format
    |
    v
[DownloadDaemon]  -- Monitors, checksums, retries
    |
    | SQLite state DB
    |
    v
[ProcessingDaemon]  -- Decodes BUFR -> NetCDF volumes
    |
    | SQLite state DB
    |
    v
[ProductGenerationDaemon]  -- Renders PNG & GeoTIFF products
    |
    |
    v
[Output Products]
  PNG + GeoTIFF



### Supported Formats & Sources

| Category | Details |
|---|---|
| **Input format** | BUFR (Binary Universal Form for the Representation of meteorological data) |
| **Intermediate format** | NetCDF-4 / CF-Radial (via arm-pyart) |
| **Output formats** | PNG images, Cloud-Optimized GeoTIFF (COG) |
| **Data transport** | Asynchronous FTP (via `aioftp`) |
| **Radar networks** | Argentina's SINARAME network (RMA* codes) and any BUFR-compatible radar |

### Key Features

- [CHECK] **Complete end-to-end pipeline** — BUFR download through product delivery in a single orchestrated service
- [CHECK] **Async architecture** — every daemon runs as an `asyncio` coroutine for concurrent, non-blocking I/O
- [CHECK] **Fault-tolerant state management** — SQLite-backed `StateTracker` allows automatic resume after crashes
- [CHECK] **BUFR decoding** — high-performance BUFR reader wrapping C/Fortran shared libraries
- [CHECK] **PyART integration** — reads decoded data into `pyart.core.Radar` objects with arbitrary field filtering
- [CHECK] **COLMAX computation** — column-maximum reflectivity with configurable elevation limits and quality filters
- [CHECK] **Dual output modes** — PNG images **or** Cloud-Optimized GeoTIFFs with proper EPSG:4326 georeference
- [CHECK] **Custom colormaps** — 8+ GRC-tuned colormaps for reflectivity, velocity, ZDR, RhoHV, PhiDP, KDP
- [CHECK] **Flexible configuration** — multi-level override chain (env vars → JSON → YAML → defaults)
- [CHECK] **Docker-ready** — deploy one container per radar instance without code changes
- [CHECK] **Comprehensive tests** — 43+ test files covering unit and integration scenarios

### High-Level Architecture

| Module | Role |
|---|---|
| `radarlib.daemons` | Background async workers (download → process → product → cleanup) |
| `radarlib.io.bufr` | Low-level BUFR decoding using Fortran/C shared libraries |
| `radarlib.io.ftp` | Async FTP client with retry and checksum verification |
| `radarlib.io.pyart` | PyART integration: field filtering, COLMAX, PNG/GeoTIFF export |
| `radarlib.radar_grid` | Pre-computed polar-to-Cartesian grid engine |
| `radarlib.state` | SQLite and JSON file state tracking for pipeline coordination |
| `radarlib.colormaps` | Custom matplotlib colormaps tuned for dual-polarisation radar |
| `radarlib.config` | Centralised configuration with JSON/env-var/YAML override chain |
| `radarlib.utils` | File naming, field-type utilities, grid utilities |

### Quick Start (5 Minutes)

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

# Configure the pipeline for one radar
config = DaemonManagerConfig(
    radar_name="RMA1",
    base_path=Path("/data/radares/RMA1"),
    ftp_host="ftp.example.com",
    ftp_user="radar_user",
    ftp_password="radar_pass",
)

# Start the pipeline
async def main():
    manager = DaemonManager(config)
    await manager.start()

# Run it
asyncio.run(main())
```

---

# 2. Installation & Setup {#2-installation--setup}

## System Requirements

### Python Version
- **Required:** Python >= 3.11, < 4.0

### Operating System
- **Primary:** Linux (Ubuntu 20.04+ recommended)
- **Other:** macOS, Windows (via WSL2)

### System Dependencies (Linux)

GDAL native libraries are required:

```bash
sudo apt-get update
sudo apt-get install -y gdal-bin libgdal-dev build-essential git
```

### Hardware Requirements
- **CPU:** 2+ cores (for parallel BUFR decompression)
- **RAM:** 4GB minimum (8GB+ recommended for high-frequency data)
- **Disk:** Depends on retention policy; typically 100GB+ for active radar data

## Installation Methods

### 1. From Source (Development)

```bash
# Clone the repository
git clone https://github.com/jgmarti84/gl-radarlib.git
cd gl-radarlib

# Create and activate a virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install system dependencies (Debian/Ubuntu)
sudo apt-get install -y gdal-bin libgdal-dev build-essential git

# Install the package in editable mode with dependencies
pip install -e .

# (Optional) Install development dependencies
pip install -r requirements-dev.txt
```

### 2. Using the Makefile

```bash
# Create virtual environment
make venv

# Setup: install dependencies + pre-commit hooks
make setup

# Run tests
make test

# Run linting (flake8)
make lint

# Format code (black)
make format
```

### 3. Docker Setup

```bash
# Build the Docker image
docker build -t radarlib:latest .

# Run a container for a specific radar
docker run -d \
  -e FTP_HOST="ftp.example.com" \
  -e FTP_USER="user" \
  -e FTP_PASS="password" \
  -v /data/radares:/data/radares \
  -v /output/products:/output/products \
  radarlib:latest
```

## Credentials & Environment Setup

### Option 1: Environment Variables

```bash
export FTP_HOST="ftp.your-radar-server.com"
export FTP_USER="your_username"
export FTP_PASS="your_password"
export BUFR_RESOURCES_PATH="/path/to/bufr/resources"
export ROOT_RADAR_PRODUCTS_PATH="/path/to/product/output"
```

### Option 2: Configuration File

See [Configuration Reference](#3-configuration-reference) for details on using `genpro25.yml`.

---

# 3. Configuration Reference {#3-configuration-reference}

## Configuration System (Two-Layer Architecture)

radarlib has two independent configuration systems:

### Library Layer: `radarlib.config` (Standalone)
Controls core library settings: colormaps, COLMAX thresholds, geometry parameters.
- Accessible with `from radarlib import config`
- Priority order:
  1. Environment variables (highest priority)
  2. JSON file (via `RADARLIB_CONFIG` env var)
  3. Built-in defaults (lowest priority)

### Service Layer: `app.config` (Genpro25 Service)
Controls daemon behavior: FTP credentials, poll intervals, retention policies, output format.
- Accessible with `from config import ...` (from app/main.py)
- Used by Genpro25 service layer only
- Priority order:
  1. Environment variables (highest priority)
  2. YAML file (`genpro25.yml` — GENPRO25_ENV section)
  3. Built-in defaults in `_DEFAULTS` dict (lowest priority)

### Configuration Loading

```python
# Library layer example
from radarlib import config
value = config.get("COLMAX_THRESHOLD")
value = config.get("KEY", default="fallback")

# Service layer example (from app/main.py)
import config  # This imports app/config.py
ftp_host = config.FTP_HOST  # Direct attribute access
product_type = config.PRODUCT_TYPE
```

## Key Environment Variables

### General Paths

| Variable | Description | Type | Default |
|---|---|---|---|
| `RADARLIB_CONFIG` | Path to JSON configuration file | `str` | - |
| `BUFR_RESOURCES_PATH` | Path to BUFR tables and C library | `str` | `<package>/io/bufr/bufr_resources` |
| `ROOT_CACHE_PATH` | Cache directory | `str` | `<project>/cache` |
| `ROOT_RADAR_FILES_PATH` | Base radar files directory | `str` | `<project>/data/radares` |
| `ROOT_RADAR_PRODUCTS_PATH` | Product output directory | `str` | `<project>/product_output` |
| `ROOT_GATE_COORDS_PATH` | Gate coordinates cache | `str` | `<project>/data/gate_coordinates` |
| `ROOT_GEOMETRY_PATH` | Geometries cache | `str` | `<project>/data/geometries` |
| `ROOT_LOGS_PATH` | Logs directory | `str` | `<project>/logs` |

### FTP Configuration

| Variable | Description | Type | Default |
|---|---|---|---|
| `FTP_HOST` | FTP server hostname | `str` | `"www.example.com"` |
| `FTP_USER` | FTP username | `str` | `"example_user"` |
| `FTP_PASS` | FTP password | `str` | `"secret"` |

### Service-Layer Daemon Configuration

These variables control the Genpro25 service layer daemon behavior and are defined in `app/config.py`:

#### Daemon Toggles

| Variable | Description | Type | Default |
|---|---|---|---|
| `ENABLE_DOWNLOAD_DAEMON` | Start FTP download daemon | `bool` | `True` |
| `ENABLE_PROCESSING_DAEMON` | Start BUFR processing daemon | `bool` | `True` |
| `ENABLE_PRODUCT_DAEMON` | Start product generation daemon | `bool` | `True` |
| `ENABLE_CLEANUP_DAEMON` | Start cleanup daemon ⚠️ **Disabled by default** | `bool` | `False` |

#### Poll Intervals (seconds)

| Variable | Description | Type | Default |
|---|---|---|---|
| `DOWNLOAD_POLL_INTERVAL` | Seconds between FTP checks | `int` | `60` |
| `PROCESSING_POLL_INTERVAL` | Seconds between processing checks | `int` | `30` |
| `PRODUCT_POLL_INTERVAL` | Seconds between product generation checks | `int` | `30` |
| `CLEANUP_POLL_INTERVAL` | Seconds between cleanup cycles | `int` | `1800` (30 minutes) |

#### Processing & Product Generation

| Variable | Description | Type | Default |
|---|---|---|---|
| `PRODUCT_TYPE` | Output format (see below) | `str` | `"raw_cog"` |
| `ADD_COLMAX` | Generate COLMAX (column-max reflectivity) COG product | `bool` | `True` |
| `ADD_TOPS_AND_CORES` | Generate convective tops & cores GeoJSON product | `bool` | `False` |
| `ROOT_TOPS_AND_CORES_PATH` | Root directory for tops & cores GeoJSON output | `str` | `"tops_and_cores"` |
| `START_DATE` | Begin downloads from this date (UTC) | `datetime` or `None` | `None` |
| `FIELD_VALUE_MASKS` | Per-field value threshold masks applied post-interpolation before COG write. Values outside range are masked to NaN. Example: `{"RHOHV": {"min": 0.3}}` | `dict` | `{}` |
| `ROI_PARAMS_VOL01` | Override geometry build parameters for volume 01 (dict or None) | `dict` | `None` |
| `ROI_PARAMS_VOL02` | Override geometry build parameters for volume 02 (dict or None) | `dict` | `None` |

#### Data Retention

| Variable | Description | Type | Default |
|---|---|---|---|
| `BUFR_RETENTION_DAYS` | Keep BUFR files for N days | `float` | `30.0` |
| `NETCDF_RETENTION_DAYS` | Keep NetCDF files for N days | `float` | `30.0` |
| `GEOMETRY_BUFR_LOOKBACK_HOURS` | Look back N hours for geometry BUFR | `int` | `72` |

#### Output Product Type

The `PRODUCT_TYPE` variable controls what format is generated:

| Value | Format | Description |
|-------|--------|-------------|
| `"image"` | PNG | PNG visualization files (legacy, legacy mode) |
| `"geotiff"` | COG | Multi-band RGBA GeoTIFF (colormap baked into pixels as uint8) |
| `"raw_cog"` | COG | **Production standard.** Single-band float32 GeoTIFF with colormap and value-range as metadata, enabling dynamic colormap remapping via rio-tiler |

**Recommendation:** Use `"raw_cog"` for all new deployments.

**Note:** In `raw_cog` mode, COLMAX is generated as its own single-band float32 COG product (via `_generate_colmax_cog()`), separately from the per-field pipeline. The `ADD_COLMAX` flag controls whether COLMAX is generated regardless of product type. `ADD_COLMAX` in the legacy `image` PNG mode generates a matplotlib PNG instead.

### GRC Quality Filters

⚠️ **Important:** These parameters belong to the **library layer** (`radarlib.config`), not the service layer. They cannot be set via `genpro25.yml` — `product_daemon.py` reads them directly from `radarlib.config`. To override these values per radar, use **environment variables** (in `docker-compose.yml`) or a **JSON file** pointed to by the `RADARLIB_CONFIG` env var.

| Variable | Description | Type | Default |
|---|---|---|---|
| `GRC_RHV_FILTER` | Enable RhoHV gate filter | `bool` | `True` |
| `GRC_RHV_THRESHOLD` | RhoHV minimum — gates **below** this value are excluded | `float` | `0.87` |
| `GRC_WRAD_FILTER` | Enable spectral width gate filter | `bool` | `False` |
| `GRC_WRAD_THRESHOLD` | Spectral width maximum — gates **above** this value are excluded | `float` | `4.6` |
| `GRC_REFL_FILTER` | Enable reflectivity gate filter | `bool` | `True` |
| `GRC_REFL_THRESHOLD` | Reflectivity minimum (dBZ) — gates **below** this value are excluded | `float` | `30` |
| `GRC_ZDR_FILTER` | Enable ZDR gate filter | `bool` | `False` |
| `GRC_ZDR_THRESHOLD` | ZDR maximum — gates **above** this value are excluded | `float` | `8.5` |

**Active filters with default config:** RhoHV (`< 0.87`) and Reflectivity (`< 30 dBZ`). WRAD and ZDR filters are off by default.

**To override per radar (docker-compose.yml):**
```yaml
environment:
  - GRC_RHV_THRESHOLD=0.70
  - GRC_WRAD_FILTER=true
  - GRC_WRAD_THRESHOLD=3.5
  - GRC_REFL_THRESHOLD=20
```

**To override for all radars globally (shared JSON file):**

Create a JSON file with the values you want to change from the defaults (e.g. `radarlib_overrides.json`):
```json
{
  "GRC_RHV_THRESHOLD": 0.90,
  "GRC_REFL_THRESHOLD": 25
}
```

Then point every radar container to it via `RADARLIB_CONFIG`:
```yaml
environment:
  - RADARLIB_CONFIG=/config/radarlib_overrides.json
volumes:
  - ./radarlib_overrides.json:/config/radarlib_overrides.json:ro
```

Individual env vars always win over the JSON file (load order: defaults → JSON file → env vars), so you can use the JSON as a network-wide baseline and per-service env vars to deviate on specific radars.

## Example: genpro25.yml

```yaml
# Genpro25 Service Configuration Example
# Place in app/genpro25.yml or referenced via GENPRO25_ENV env var

local:
  # FTP Connection Settings
  FTP:
    FTP_HOST: "200.16.116.24"
    FTP_USER: "your_ftp_username"
    FTP_PASS: "your_ftp_password"

  # Daemon Toggle Switches
  DAEMON_PARAMS:
    ENABLE_DOWNLOAD_DAEMON: true
    ENABLE_PROCESSING_DAEMON: true
    ENABLE_PRODUCT_DAEMON: true
    ENABLE_CLEANUP_DAEMON: true        # WARNING: Disabled by default!

  # Poll Intervals (seconds)
  INTERVALS:
    DOWNLOAD_POLL_INTERVAL: 60         # Check FTP every 60 sec
    PROCESSING_POLL_INTERVAL: 30       # Check for NetCDF every 30 sec
    PRODUCT_POLL_INTERVAL: 30          # Generate products every 30 sec
    CLEANUP_POLL_INTERVAL: 1800        # Cleanup every 30 minutes

  # Product Generation Output
  PRODUCT_OUTPUT:
    PRODUCT_TYPE: "raw_cog"            # Options: "image" (PNG), "geotiff", "raw_cog" (recommended)
    ADD_COLMAX: true                   # Generate COLMAX COG product
    ADD_TOPS_AND_CORES: false          # Generate convective tops & cores GeoJSON
    ROOT_TOPS_AND_CORES_PATH: "tops_and_cores"  # Output root for GeoJSON

  # Per-field value masks (applied post-interpolation, before COG write)
  # Values outside the specified range are masked to NaN in the output COG.
  FIELD_VALUE_MASKS:
    RHOHV:
      min: 0.3                         # mask RHOHV values below 0.3

  # Data Retention (days)
  RETENTION:
    BUFR_RETENTION_DAYS: 30.0
    NETCDF_RETENTION_DAYS: 30.0
    GEOMETRY_BUFR_LOOKBACK_HOURS: 72

```

---

# 4. Deployment & Operations {#4-deployment--operations}

## Docker Deployment

### 1. Single Radar Deployment

```bash
# Build image
docker build -t radarlib:latest .

# Run container
docker run -d \
  --name radarlib-rma1 \
  -e FTP_HOST="ftp.example.com" \
  -e FTP_USER="user" \
  -e FTP_PASS="pass" \
  -v /data/radares/RMA1:/data/radares/RMA1 \
  -v /output/products:/output/products \
  radarlib:latest
```

### 2. Multi-Radar Deployment (Docker Compose)

```yaml
version: '3.8'
services:
  rma1:
    build: .
    environment:
      - FTP_HOST=ftp.example.com
      - FTP_USER=user1
      - FTP_PASS=pass1
      - RADAR_NAME=RMA1
    volumes:
      - /data/radares/RMA1:/data/radares/RMA1
      - /output/products:/output/products
    restart: unless-stopped

  rma11:
    build: .
    environment:
      - FTP_HOST=ftp.example.com
      - FTP_USER=user2
      - FTP_PASS=pass2
      - RADAR_NAME=RMA11
    volumes:
      - /data/radares/RMA11:/data/radares/RMA11
      - /output/products:/output/products
    restart: unless-stopped
```

### 3. Monitor Logs

```bash
# Docker logs for a specific container
docker logs -f radarlib-rma1

# Check health
docker ps | grep radarlib

# Restart an instance
docker restart radarlib-rma1
```

## Quick Troubleshooting

### Problem: FTP Connection Fails
- **Check:** FTP credentials are correct in env vars or config
- **Check:** FTP server is reachable: `ftp-ping ftp.example.com`
- **Solution:** Verify `FTP_HOST`, `FTP_USER`, `FTP_PASS`

### Problem: No Output Files Generated
- **Check:** Input BUFR files are being downloaded: `ls /data/radares/RADAR_NAME/`
- **Check:** Processing daemon logs: `docker logs radarlib-rma1`
- **Check:** Product output directory exists and has write permissions

### Problem: Out of Disk Space
- **Solution:** Configure cleanup daemon retention policy
- **Check:** `du -sh /output/products` to see current size
- **Solution:** Archive old products to external storage

### Problem: High Memory Usage
- **Cause:** Possibly parallel BUFR decompression
- **Solution:** Disable parallel mode or reduce parallelism in config

---

# 5. Architecture Deep Dive {#5-architecture-deep-dive}

## System Architecture

### Daemon Manager & Daemons

radarlib uses an async daemon-based architecture where independent workers process different stages of the pipeline:

```
+-----------------------------------------------------+
|            DAEMON MANAGER                          |
| Orchestrates all daemons for complete pipeline    |
+-----------------------------------------------------+
|
+-------------------+    +------------------+
| DOWNLOAD          |--> | PROCESSING       |
| DAEMON            |    | DAEMON           |
|                   |    |                  |
| - FTP monitor     |    | - BUFR decode    |
| - Download        |    | - PyART convert  |
| - Checksum verify |    | - Save NetCDF    |
+--------+----------+    +--------+---------+
         |                       |
         v                       v
+------------------------------------+
| PRODUCT GENERATION DAEMON          |
|                                    |
| - Read NetCDF                      |
| - Generate COLMAX                  |
| - Render PNG images                |
| - Export GeoTIFF (COG)             |
+--------+---------------------------+
         |
         v
+-------------------------------+
| CLEANUP DAEMON                 |
|                                |
| - Delete old products          |
| - Enforce retention policy     |
+-------------------------------+

+-------------------------------+
| SQLite State Database          |
|                                |
| - downloads table              |
| - volumes table                |
| - product_generation table     |
+-------------------------------+
```

### Data Flow

1. **Download Stage**
   - `DownloadDaemon` monitors FTP server every N seconds
   - Downloads new BUFR files to `ROOT_RADAR_FILES_PATH/<radar_name>/`
   - Verifies checksums
   - Records state in SQLite

2. **Processing Stage**
   - `ProcessingDaemon` polls for downloaded BUFR files
   - Uses `radarlib.io.bufr.bufr_to_dict()` to decode
   - Converts to PyART `Radar` object
   - Saves as CF-Radial NetCDF to `ROOT_RADAR_FILES_PATH/<radar_name>/`
   - Records state (resume-on-crash safe)

3. **Product Generation Stage**
   - `ProductGenerationDaemon` polls for processed NetCDF files
   - Generates COLMAX (column-maximum reflectivity)
   - Creates PPI (Plan Position Indicator) visualizations
   - Exports PNG images to `ROOT_RADAR_PRODUCTS_PATH/<radar_name>/`
   - Exports GeoTIFF (COG) to same directory
   - Records state

4. **Cleanup Stage**
   - `CleanupDaemon` enforces retention policies
   - Deletes files older than configured retention period
   - Manages disk space

## State Management

All daemons use **SQLite-backed state tracking** to ensure:
- [CHECK] Files are never processed twice
- [CHECK] Resume from exact point after crash
- [CHECK] Atomic transactions prevent partial state writes

```python
# Example: StateTracker usage in a daemon
from radarlib.state import StateTracker

tracker = StateTracker(db_path="/path/to/state.db")
tracker.record_download(filename, file_hash, timestamp)
tracker.record_volume(filename, volume_hash)
tracker.record_product(volume_id, product_type, output_path)
```

---

# 6. Module Reference {#6-module-reference}

## Core Modules

### radarlib.config

Centralized configuration management with multi-level override chain.

**Key Functions:**
- `config.get(key: str, default: Any = None) -> Any` — Get configuration value
- `config.reload(path: str | Path) -> None` — Reload from file
- `config.to_dict() -> dict` — Export all current config

**Example:**
```python
from radarlib import config

# Get configurable paths
products_path = config.get("ROOT_RADAR_PRODUCTS_PATH")
ftp_host = config.get("FTP_HOST")
```

### radarlib.io.bufr

BUFR file decoding and parsing. **Thin Python wrapper around C/Fortran libraries.**

**Key Functions:**
- `bufr_to_dict(filename: str) -> dict | None` — High-level decode interface
- `dec_bufr_file(filename: str, ...)` — Low-level decoding with full control
- `bufr_name_metadata(filename: str) -> dict` — Parse BUFR filename

**Example:**
```python
from radarlib.io.bufr import bufr_to_dict

result = bufr_to_dict("AR5_1000_1_DBZH_20240101T000746Z.BUFR")
if result:
    data = result['data']  # numpy ndarray (rays, gates)
    info = result['info']  # metadata dict
```

### radarlib.io.ftp

Asynchronous FTP client for reliable data download.

**Key Classes:**
- `FTPClient` — Async FTP connection with retry logic
- `FileAvailabilityChecker` — Check file existence on FTP
- `FTPDownloader` — High-level download with checksums

**Example:**
```python
import asyncio
from radarlib.io.ftp import FTPDownloader

async def download_radar_data():
    downloader = FTPDownloader(host="ftp.example.com", user="user", password="pass")
    await downloader.download("remote/path/file.BUFR", "local/path/file.BUFR")

asyncio.run(download_radar_data())
```

### radarlib.io.pyart

PyART integration for Radar object manipulation and product generation.

**Key Functions:**
- `radar_to_pyart(decoded_bufr: dict) -> pyart.core.Radar` — Convert decoded BUFR to Radar
- `apply_grc_filters(radar: Radar, ...) -> Radar` — Apply GRC quality filters
- `export_to_geotiff(radar: Radar, field: str, output_path: str, ...) -> None` — Save as GeoTIFF
- `export_to_png(radar: Radar, field: str, output_path: str, ...) -> None` — Save as PNG

### radarlib.radar_grid

Pre-computed polar-to-Cartesian Barnes interpolation engine. Uses pre-built
`GridGeometry` objects (`.npz` files) to avoid re-computing interpolation weights.

**Key Classes:**
- `GridGeometry` — Holds pre-computed Barnes interpolation weights, grid shape, and limits

**Key Functions:**
- `compute_grid_geometry(...)` — Build and cache interpolation geometry from gate coordinates
- `load_geometry(path) / save_geometry(geom, path)` — Load/save `.npz` geometry files
- `apply_geometry(geometry, field_data, additional_filters=[])` — Polar → 3D Cartesian interpolation
- `constant_elevation_ppi(grid_3d, geometry, elevation_angle, interpolation)` — Extract constant-elevation 2D slice
- `column_max(grid_3d, geometry)` — Compute 2D column-maximum reflectivity
- `create_raw_cog(data_2d, geometry, lat, lon, path, cmap, vmin, vmax, ...)` — Write float32 COG
- `detect_cores_from_colmax(colmax, x_coords, y_coords, rhohv)` — Convective core detection
- `detect_tops_from_cores(cores, grid_3d, x_coords, y_coords, z_coords)` — Storm top detection (core-anchored)
- `detect_tops_from_3d_grid(grid_3d, x_coords, y_coords, z_coords, rhohv_3d)` — Alternative independent top detection
- `GateFilter(radar)` — Polar gate quality filter (exclude_below, exclude_above)
- `get_field_data(radar, field_name)` — Extract masked numpy array from PyART Radar object

### radarlib.daemons

Main pipeline orchestration (download, process, product, cleanup).

**Key Classes:**

#### DownloadDaemon
```python
class DownloadDaemon:
    """Monitors FTP and downloads new BUFR files."""
    async def start()  # Start monitoring loop
    async def stop()   # Graceful shutdown
```

#### ProcessingDaemon
```python
class ProcessingDaemon:
    """Decodes BUFR → NetCDF volumes."""
    async def start()
    async def stop()
```

#### ProductGenerationDaemon
```python
class ProductGenerationDaemon:
    """Generates PNG/GeoTIFF from NetCDF."""
    async def start()
    async def stop()
```

#### DaemonManager
```python
class DaemonManager:
    """Orchestrates all daemons for complete pipeline."""
    async def start()  # Start all daemons
    async def stop()   # Stop all daemons
```

---

# 7. BUFR Processing Guide {#7-bufr-processing-guide}

## What is BUFR?

BUFR (Binary Universal Form for the Representation of meteorological data) is a standardized World Meteorological Organization (WMO) format for encoding meteorological data. It is widely used by national weather services for archiving and transmitting radar observations.

## BUFR File Structure

A BUFR radar file contains:
- **Message header** — timestamp, radar location, site metadata
- **Fixed angles** — elevation angles of each sweep
- **Volume data** — encoded reflectivity fields (compressed)

## Decoding Pipeline

radarlib's BUFR decoding pipeline:

```
1. Load C library (libdecbufr.so)
   ↓
2. Read volume size, elevations, raw integer buffer
   ↓
3. Parse integer buffer into per-sweep headers + compressed data chunks
   ↓
4. Decompress per-sweep data (zlib)
   ↓
5. Reshape into 2-D arrays (rays × gates per sweep)
   ↓
6. Uniformize gate counts across sweeps (pad with NaN)
   ↓
7. Vertically concatenate into single volume array
   ↓
8. Build metadata dictionary
```

## High-Level Usage

```python
from radarlib.io.bufr import bufr_to_dict

# Decode BUFR file
result = bufr_to_dict("AR5_1000_1_DBZH_20240101T000746Z.BUFR")

if result:
    # Access the decoded data
    volume_data = result['data']        # (total_rays, gates) ndarray
    metadata = result['info']           # dict with sweep info, radar location, etc.

    # Example: convert to PyART Radar
    from radarlib.io.pyart import radar_to_pyart
    radar = radar_to_pyart(result)
    print(f"Radar location: {radar.latitude['data'][0]}, {radar.longitude['data'][0]}")
else:
    print("BUFR decoding failed")
```

## Low-Level API

For fine-grained control over decoding:

```python
from radarlib.io.bufr.bufr import (
    dec_bufr_file, parse_sweeps, decompress_sweep, uniformize_sweeps
)

# Full decoding with control
meta_vol, sweeps, vol_data, run_log = dec_bufr_file(
    bufr_filename="file.BUFR",
    parallel=True,  # Use ThreadPoolExecutor for decompression
    logger_name="custom_logger"
)

# Access per-sweep data
for i, sweep in enumerate(sweeps):
    print(f"Sweep {i}: {sweep['data'].shape} (rays, gates)")
```

## Error Handling

Common exceptions:

```python
from radarlib.io.bufr.bufr import SweepConsistencyException

try:
    result = bufr_to_dict("file.BUFR")
except SweepConsistencyException as e:
    # Bad sweep was skipped
    print(f"Skipped sweep due to inconsistency: {e}")
except ValueError as e:
    # Decompression or data format error
    print(f"Error during decoding: {e}")
```

## Supported BUFR Templates

radarlib supports BUFR templates commonly used by Argentina's SINARAME network:
- **Template 0315** — Multi-sweep radar volumes
  - Subset 01: Full dual-pol fields (DBZH, DBZV, ZDR, RHOHV, PHIDP, KDP)
  - Subset 02: Velocity fields (VRAD, WRAD)
  - Subset 04: Surveillance reflectivity only (DBZH) — used for "vigilant mode" volumes

---

# 8. Integration & Advanced Examples {#8-integration--advanced-examples}

## Using radarlib as a Library

### Example 1: Process a Single BUFR File

```python
from pathlib import Path
from radarlib.io.bufr import bufr_to_dict
from radarlib.io.pyart import export_to_geotiff, export_to_png

# Decode BUFR
result = bufr_to_dict("radar_data.BUFR")
if not result:
    raise ValueError("Failed to decode BUFR")

# Convert to PyART
from radarlib.io.pyart import radar_to_pyart
radar = radar_to_pyart(result)

# Export as PNG and GeoTIFF
export_to_png(
    radar=radar,
    field="DBZH",
    output_path="reflected_power.png",
    vmin=-20, vmax=70, colormap="grc_th"
)

export_to_geotiff(
    radar=radar,
    field="DBZH",
    output_path="reflected_power.tif",
    vmin=-20, vmax=70, colormap="grc_th"
)
```

### Example 2: Multi-Radar Processing

```python
import asyncio
from pathlib import Path
from radarlib.daemons import DaemonManager, DaemonManagerConfig

async def process_multiple_radars():
    radars = ["RMA1", "RMA11", "RMA5"]
    managers = []

    for radar_name in radars:
        config = DaemonManagerConfig(
            radar_name=radar_name,
            base_path=Path(f"/data/radares/{radar_name}"),
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
        )
        manager = DaemonManager(config)
        managers.append(manager)

    # Start all pipelines concurrently
    await asyncio.gather(*[m.start() for m in managers])

asyncio.run(process_multiple_radars())
```

### Example 3: Custom Field Processing Pipeline

```python
from radarlib.io.pyart import apply_grc_filters, export_to_geotiff
from radarlib.io.bufr import bufr_to_dict
from radarlib.io.pyart import radar_to_pyart

# Decode and convert
result = bufr_to_dict("radar_data.BUFR")
radar = radar_to_pyart(result)

# Apply quality-control filters (GRC methodology)
filtered_radar = apply_grc_filters(
    radar=radar,
    rhohv_filter=True,
    rhohv_threshold=0.8,
    zdr_filter=True,
    zdr_threshold=8.5,
    wrad_filter=True,
    wrad_threshold=4.6
)

# Compute COLMAX (column-maximum reflectivity)
from radarlib.io.pyart import compute_colmax
colmax = compute_colmax(
    radar=filtered_radar,
    field="DBZH",
    elev_limit=0.65,
    threshold=-3
)

# Export result
export_to_geotiff(
    radar=colmax,
    field="COLMAX",
    output_path="colmax_product.tif",
    vmin=-3, vmax=70, colormap="grc_th"
)
```

## Output Contract (CRITICAL)

> [WARNING] **This section is critical.** The `webmet25` repository consumes the output files produced by radarlib. Never change this contract without updating webmet25 as well.

### File Types

- **GeoTIFF (COG): (.tif)** This is the primary and current output format.
  Cloud-Optimized GeoTIFF is the production standard.
- **PNG:(.png)** Deprecated. Kept only for backward compatibility.
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

**Examples:**
- `RMA1_20260401T205000Z_ZDR_00.tif` — filtered ZDR field, elevation 00°, GeoTIFF (primary)
- `RMA1_20260401T205000Z_ZDRo_00.tif` — raw/unfiltered ZDR field, elevation 00°, GeoTIFF
- `RMA1_20260401T205000Z_ZDR_00.png` — PNG equivalent (deprecated, backward compatibility only)

### Dual-Timestamp Write Behaviour

Each COG product is written at **two timestamps** to aid consumer lookup:

| Variant | Formula | Purpose |
|---------|---------|---------|
| **Ceiled** | `floor(obs_time + 10 min, 10 min)` | "Next" 10-minute boundary after observation |
| **Rounded** | `round(obs_time, 10 min)` | Nearest 10-minute boundary |

When the ceiled and rounded timestamps differ, both files are written (the rounded file is a copy of the ceiled one). When they are equal, only one file is written. This behaviour is intentional for `webmet25` compatibility and must not be changed.

### Folder Structure

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
{ROOT_TOPS_AND_CORES_PATH}/
└── {radar_code}/
    └── YYYY/
        └── MM/
            └── DD/
                └── {radar_code}_{strategy}_{vol_nr}_{timestamp}_TOPS_CORES.geojson
```
Example: `RMA6_0315_01_20260505T163854Z_TOPS_CORES.geojson`

**Critical rules:**
- File is **NOT written** if both core and top lists are empty.
- `observation_time` is always ISO 8601 UTC.
- Coordinates are `[lon, lat]` — GeoJSON standard order.
- Timestamp format in filename is `YYYYMMDDTHHMMSSZ` — same as COG filenames.
- `webmet25` indexer depends on this filename pattern — never change it without
  updating `TopsAndCoresFilenameParser` in the webmet25 repo.

**GeoJSON schema per feature:**
```json
// Core feature:
{
  "type": "Feature",
  "geometry": { "type": "Point", "coordinates": [lon, lat] },
  "properties": {
    "type": "core",
    "intensity_dbz": 52,
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
    "altitude_m": 11200,
    "dbz": 25.5,
    "parent_core_dbz": 52,
    "radar_code": "RMA6",
    "observation_time": "2026-05-05T16:38:54Z"
  }
}
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

### GeoTIFF Properties

- **Format:** Cloud-Optimized GeoTIFF (COG)
- **Compression:** Deflate or LZW
- **BlockSize:** 512×512 (optimized for tiling)
- **Overviews:** Generated for multi-scale access
- **Data Type:** Float32

### PNG Properties

- **Format:** RGBA PNG (with alpha channel for georeferencing)
- **DPI:** 100-150 (configurable)
- **Colormap:** Applied using matplotlib
- **Georeference:** Embedded in metadata (optional, for GIS software support)

---

## Known Gaps & Risks

### Error Handling
- [INCOMPLETE] Limited error handling in daemons (FTP failures, file corruption)
- [INCOMPLETE] No retry logic for failed processing steps
- [FIX] **Recommendation:** Implement exponential backoff + dead-letter queue

### Testing
- [INCOMPLETE] Incomplete test coverage for `radar_grid` module
- [INCOMPLETE] No integration tests for full end-to-end pipeline
- [INCOMPLETE] No tests for GeoTIFF output validation (CRS, metadata completeness)
- [FIX] **Recommendation:** Add pytest fixtures for realistic data

### Output Validation
- [INCOMPLETE] No validation that GeoTIFF has correct CRS
- [INCOMPLETE] No validation that metadata fields are present
- [FIX] **Recommendation:** Implement `validate_geotiff()` function in output stage

### Scalability
- [INCOMPLETE] SQLite state tracking may bottleneck with high-frequency (< 1 min intervals) data
- [FIX] **Recommendation:** Consider PostgreSQL for production systems

### Documentation
- [INCOMPLETE] Configuration options in `genpro25.yml` are poorly documented
- [INCOMPLETE] BUFR template support and limitations unclear
- [FIX] **Recommendation:** Maintain authoritative schema documentation

### Deployment
- [INCOMPLETE] No production-specific configurations (e.g., `docker-compose.prod.yml`)
- [INCOMPLETE] No Kubernetes manifests
- [FIX] **Recommendation:** Create `deploy/k8s/` folder with Helm charts

---

## Memory Management Best Practices

radarlib implements comprehensive memory cleanup to prevent leaks in long-running daemons.

### Core Rules

1. **Delete large numpy arrays after use in loops** — always `del field_data, grid_data, ppi` + `gc.collect()` in `finally` blocks
2. **Use `TemporaryDirectory()` context managers** — never `mkdtemp()` (auto-cleanup on scope exit)
3. **Call `gc.collect()` after heavy operations** — force collection of intermediate masked-array copies
4. **Comprehensive `finally` blocks** — clean up even on exception paths

### Key Utilities
```python
from radarlib.utils.memory_profiling import log_memory_usage, check_and_cleanup_memory

log_memory_usage("Before loading radar")          # logs current RSS
check_and_cleanup_memory(threshold_mb=1100.0)     # force GC if above threshold
```

### Memory Leak Investigation Checklist
1. Are large numpy arrays explicitly deleted in loops?
2. Are temporary directories using context managers?
3. Is `gc.collect()` called after heavy operations?
4. Do `finally` blocks clean up all large objects?
5. Are intermediate copies in masking/filtering operations freed?
6. Are PyART radar objects deleted after use?
7. Is memory monitoring in place to track RSS growth?

---

## Rules When Adding New Config Keys

| Setting type | Where to add |
|---|---|
| **Service-layer** (daemon toggles, poll intervals, paths, FTP) | `_DEFAULTS` in `app/config.py` only |
| **Library internal** (colormaps, GRC thresholds, geometry, algorithm params) | `DEFAULTS` in `src/radarlib/config.py` only + convenience attribute |
| **Both layers** (shared setting) | Both files with matching default values |

- **Never** add `app/config.py` imports inside `src/radarlib/` — radarlib must remain usable as a standalone library.
- After adding a key, add the matching entry in `genpro25.yml` (even if `None`).

---

## SDD Workflow — Follow This Every Time

When given a task, strictly follow this cycle:

### 1. PROPOSAL
- Read `copilot-instructions.md` fully
- Read the relevant section of `docs/radarlib_EN.md`
- Read the relevant source files
- Propose the code changes
- Flag any conflict with the Output Contract immediately

### 2. APPLY
- Wait for approval or feedback
- Adjust based on response
- Provide final code only after confirmation

### 3. ARCHIVE
After code is applied, explicitly state:
- Does `docs/radarlib_EN.md` need to be updated?
- Does `copilot-instructions.md` need to be updated?
- Does the Output Contract need to be updated?

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Code style:** black + flake8
2. **Type hints:** Required on all functions (enforced by mypy)
3. **Tests:** Add tests for new functionality
4. **Documentation:** Update docs/README sections

## License

This project is licensed under the MIT License. See the [LICENSE](../LICENSE) file for details.

## Acknowledgments

Developed by **Grupo Radar Córdoba (GRC)** — Universidad Nacional de Córdoba, Argentina.

---

**Last Updated:** April 2, 2026
**Version:** 0.1.0
