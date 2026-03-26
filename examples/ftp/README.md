# FTP Examples

This folder contains examples for FTP client operations, availability checking, and workflows.

## Files

### Core Examples

- **ftp_client_example.py** - Basic FTP client usage for downloading BUFR files
- **ftp_integration_example.py** - Integration workflow with BUFR processing and state tracking
- **ftp_checks.py** - Suite of functions/classes for checking BUFR file availability without downloading
- **ftp_checks_example.py** - Comprehensive examples showing how to use the availability checker

## FTP File Availability Checking

The `ftp_checks.py` module provides a complete toolkit for checking radar BUFR file availability on FTP servers. This is useful for:

- Verifying files exist before attempting download
- Generating availability reports for auditing
- Monitoring multiple radars simultaneously
- Caching checks for efficiency
- Getting detailed statistics

### Quick Start

```python
from ftp_checks import BUFRAvailabilityChecker
from datetime import datetime, timezone
from radarlib import config

# Initialize checker
checker = BUFRAvailabilityChecker(
    host=config.FTP_HOST,
    user=config.FTP_USER,
    password=config.FTP_PASS
)

# Check if a file exists
exists = checker.file_exists("/L2/RMA1/2025/01/01/12/file.BUFR")

# List files in a directory
files = checker.list_bufr_files("/L2/RMA1/2025/01/01/12")

# Check availability for a time range
report = checker.check_availability_range(
    radar="RMA1",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2025, 1, 7, tzinfo=timezone.utc)
)

print(report.get_summary())
```

### Key Classes

#### `BUFRAvailabilityChecker`
Main class for checking file availability.

Methods:
- `file_exists(remote_path)` - Check if a specific file exists
- `list_bufr_files(remote_dir)` - List all BUFR files in a directory
- `check_hourly_directory(radar, date)` - Check files for a specific hour
- `check_daily_directory(radar, date)` - Check all hours in a day
- `check_availability_range(radar, start_date, end_date, hours)` - Check a date range
- `batch_file_check(file_paths)` - Check multiple files efficiently
- `get_statistics_for_date_range(...)` - Get availability statistics
- `clear_cache()` - Clear cached data
- `get_cache_info()` - Get cache statistics

#### `AvailabilityReport`
Container for availability check results.

Properties:
- `total_files_checked` - Total files checked
- `total_files_found` - Files that exist
- `total_files_missing` - Files that don't exist
- `availability_percentage` - Percentage of files found
- `total_size_mb` - Total size in MB

Methods:
- `get_summary()` - Formatted summary string
- `get_missing_files_summary()` - Summary of missing files
- `to_dict()` - Convert to dictionary

#### `MultiRadarAvailabilityChecker`
Check availability across multiple radars.

Methods:
- `check_all_radars(start_date, end_date, hours)` - Check all configured radars
- `get_comparison_summary(start_date, end_date, hours)` - Get comparison table

### Examples

#### Example 1: Check Single File
```python
checker = BUFRAvailabilityChecker(host, user, password)
exists = checker.file_exists("/L2/RMA1/2025/01/01/12/file.BUFR")
print(f"File exists: {exists}")
```

#### Example 2: List Directory
```python
files = checker.list_bufr_files("/L2/RMA1/2025/01/01/12")
print(f"Found {len(files)} files")
```

#### Example 3: Generate Report
```python
from datetime import datetime, timezone

report = checker.check_availability_range(
    radar="RMA1",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2025, 1, 7, tzinfo=timezone.utc)
)

print(report.get_summary())
if report.total_files_missing > 0:
    print(report.get_missing_files_summary())
```

#### Example 4: Monitor Multiple Radars
```python
from ftp_checks import MultiRadarAvailabilityChecker

checker = MultiRadarAvailabilityChecker(
    host, user, password,
    radars=["RMA1", "RMA2"]
)

summary = checker.get_comparison_summary(
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2025, 1, 7, tzinfo=timezone.utc)
)
print(summary)
```

### Performance Tips

1. **Use Caching**: Enable caching (default) to avoid repeated FTP queries
```python
checker = BUFRAvailabilityChecker(host, user, password, enable_cache=True)
```

2. **Clear Cache When Needed**: If you need fresh data
```python
checker.clear_cache()
```

3. **Check Specific Hours**: Limit checks to hours of interest
```python
report = checker.check_availability_range(
    radar="RMA1",
    start_date=start_date,
    end_date=end_date,
    hours=[6, 12, 18]  # Only check these hours
)
```

4. **Batch Operations**: Check multiple files in one operation
```python
paths = ["/L2/RMA1/2025/01/01/12/file1.BUFR", ...]
results = checker.batch_file_check(paths)
```

## Running Examples

```bash
# Run FTP client example
python ftp_client_example.py

# Run availability checker examples
python ftp_checks_example.py

# Run integration example
python ftp_integration_example.py
```

## Dependencies

- `radarlib.io.ftp` - FTP client functionality
- `radarlib.config` - Configuration management
- Python 3.8+
