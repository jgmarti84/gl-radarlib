# radarlib

## Overview

My Python Library is a Python package designed to provide essential functionalities for managing radars and radar products.

## Features

- Easy-to-use API for common tasks
- Comprehensive unit tests to ensure reliability
- Built-in support for debugging and development
- **Async FTP Daemon Service**: Background service for continuous monitoring and downloading of BUFR files from FTP servers
- **BUFR Processing Daemon**: Automatic processing of downloaded BUFR files into NetCDF format
- BUFR file decoding and processing
- PyART radar data integration
- PNG and GeoTIFF export capabilities

## Installation

To install the library, you can use pip:

```bash
pip install radarlib
```

For development purposes, clone the repository and install the dependencies:

```bash
git clone https://github.com/yourusername/my-python-library.git
cd my-python-library
pip install -r requirements.txt
```

### FTP credentials (`.env`)

The genpro25 services read `FTP_USER` / `FTP_PASS` from a `.env` file next to
`docker-compose.yml`. `genpro25.yml` intentionally keeps these empty — environment
variables override YAML values (see `app/config.py`).

```bash
cp .env.example .env   # then fill in the real values
chmod 600 .env
```

`.env` is gitignored: **never commit it**, and share it out-of-band (never via git).
`docker compose` refuses to start the genpro25 services if `.env` is missing.

## Usage

### Date-Based FTP Daemon (NEW)

**For date-range based monitoring with SQLite state tracking and auto-resume:**

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.io.ftp import DateBasedFTPDaemon, DateBasedDaemonConfig

async def run_daemon():
    config = DateBasedDaemonConfig(
        host='ftp.example.com',
        username='user',
        password='pass',
        remote_base_path='/L2',
        radar_code='RMA1',  # Specific radar
        local_download_dir=Path('./downloads'),
        state_db=Path('./state.db'),  # SQLite database
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 1, 2, tzinfo=timezone.utc),  # Auto-stops when complete
        verify_checksums=True,  # SHA256 verification
        resume_partial=True,  # Resume interrupted downloads
    )

    daemon = DateBasedFTPDaemon(config)
    await daemon.run()  # Auto-resumes from last download

asyncio.run(run_daemon())
```

**Features:**
- Scans date hierarchy: `/L2/RADAR/YYYY/MM/DD/HH/MMSS/`
- SQLite database for high-performance state tracking
- Auto-resumes from last downloaded file
- Auto-stops when end date reached
- Checksum verification and partial download handling

See [`DATE_BASED_DAEMON.md`](DATE_BASED_DAEMON.md) for detailed documentation.

### BUFR Processing Daemon (NEW)

**Automatic processing of downloaded BUFR volumes into NetCDF files:**

```python
import asyncio
from pathlib import Path
from radarlib.io.ftp import ProcessingDaemon, ProcessingDaemonConfig

# Define volume types - must match download daemon config
volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

async def run_processing():
    config = ProcessingDaemonConfig(
        local_bufr_dir=Path('./downloads/bufr'),
        local_netcdf_dir=Path('./downloads/netcdf'),
        state_db=Path('./state.db'),  # Same database as download daemon
        volume_types=volume_types,
        radar_code='RMA1',
        poll_interval=30,  # Check every 30 seconds
    )

    daemon = ProcessingDaemon(config)
    await daemon.run()

asyncio.run(run_processing())
```

**Features:**
- Automatic detection of complete volumes (all required field types downloaded)
- BUFR decoding and PyART Radar object creation
- NetCDF file generation with CF/Radial convention
- Processing state tracking in SQLite database
- Error handling and recovery
- Concurrent volume processing

**Combined Pipeline** - Run download and processing together:

```python
async def run_complete_pipeline():
    """Complete pipeline from FTP to NetCDF."""
    download_daemon = DateBasedFTPDaemon(download_config)
    processing_daemon = ProcessingDaemon(processing_config)

    await asyncio.gather(
        download_daemon.run(),
        processing_daemon.run(),
    )
```

See [`PROCESSING_DAEMON.md`](PROCESSING_DAEMON.md) for detailed documentation.

### Simple Daemon Manager (NEW)

**Easy management of both daemons with a single interface:**

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.io.ftp import DaemonManager, DaemonManagerConfig

volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

config = DaemonManagerConfig(
    radar_code="RMA1",
    base_path=Path("./radar_data/RMA1"),
    ftp_host="ftp.example.com",
    ftp_user="user",
    ftp_password="pass",
    ftp_base_path="/L2",
    volume_types=volume_types,
    start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
)

# Start both daemons with one command
manager = DaemonManager(config)
await manager.start()

# Or control them individually:
# manager.stop()  # Stop all
# await manager.restart_processing_daemon()  # Restart just processing
# status = manager.get_status()  # Check status
```

**Features:**
- Single interface for both daemons
- Start/stop control for each daemon independently
- Update configuration on-the-fly
- Status monitoring
- Automatic directory setup

### FTP Daemon Service

The basic FTP daemon service for simple directory monitoring:

```python
import asyncio
from pathlib import Path
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig

async def run_daemon():
    config = FTPDaemonConfig(
        host='ftp.example.com',
        username='user',
        password='pass',
        remote_base_path='/L2/radar',
        local_download_dir=Path('./downloads'),
        state_file=Path('./download_state.json'),
        poll_interval=60,  # Check every 60 seconds
        max_concurrent_downloads=5
    )

    daemon = FTPDaemon(config)
    await daemon.run()  # Runs indefinitely

asyncio.run(run_daemon())
```

### Basic FTP Client

For simple one-time downloads or custom workflows:

```python
from pathlib import Path
from radarlib.io.ftp import FTPClient

client = FTPClient(host='ftp.example.com', user='user', password='pass')

# List files
files = client.list_files('/L2/RMA1/2024/01/01')

# Download a file
client.download_file('/L2/RMA1/file.BUFR', Path('./local.BUFR'))

# Download multiple files
client.download_files('/L2/RMA1', ['file1.BUFR', 'file2.BUFR'], Path('./downloads'))
```

For more examples, see:
- `examples/ftp_client_example.py` - Basic FTP client usage
- `examples/ftp_daemon_example.py` - Daemon service examples
- `examples/ftp_date_daemon_example.py` - Date-based daemon examples
- `examples/processing_daemon_example.py` - BUFR processing daemon examples
- `examples/daemon_manager_example.py` - Simple daemon manager (NEW)
- `examples/ftp_integration_example.py` - Complete integration with BUFR processing

### BUFR File Processing

Here is an example of how to process BUFR radar files:

```python
from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart

# Decode BUFR file
bufr_dict = bufr_to_dict('radar_file.BUFR')

# Convert to PyART radar object
radar = bufr_to_pyart([bufr_dict])
```

## Development

To contribute to the development of this library, please follow these steps:

1. Clone the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and ensure that all tests pass.
4. Submit a pull request.

## Testing

To run the tests, use the following command:

```bash
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
