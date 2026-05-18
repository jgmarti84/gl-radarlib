import datetime
import logging
import os
import re
from datetime import datetime as _datetime
from datetime import timezone
from pathlib import Path
from typing import Dict, Optional

import pytz

from radarlib import config

tz_utc = pytz.timezone("UTC")
tz_arg = pytz.timezone("America/Argentina/Cordoba")


logger = logging.getLogger(__name__)


def get_time_from_RMA_filename(filename, tz_UTC=True):
    """
    Extract datetime from RMA BUFR filename.
    """
    str_time = filename.split("_")[3].split(".")[0]
    date = datetime.datetime.strptime(str_time, "%Y%m%dT%H%M%SZ")

    # el huso horario de los vols rma es UTC
    date = date.replace(tzinfo=timezone.utc)

    if not tz_UTC:
        # trasladamos tiempo a huso horario argentino
        date = date.astimezone(tz_arg)

    return date


def get_path_from_RMA_filename(filename, **kwargs):
    root_radar_files = kwargs.get("root_radar_files")
    if root_radar_files is None:
        root_radar_files = config.ROOT_RADAR_FILES_PATH

    radar = filename.split("_")[0]
    ano = filename.split("_")[3].split("T")[0][0:4]
    mes = filename.split("_")[3].split("T")[0][4:6]
    dia = filename.split("_")[3].split("T")[0][6:8]
    hora = filename.split("_")[3].split("T")[1][0:2]

    path = os.path.join(root_radar_files, radar, ano, mes, dia, hora)
    return path


def get_netcdf_filename_from_bufr_filename(ref_filename: str) -> str:
    """Generate netCDF filename from BUFR filename for RMA radars."""
    # Elimino la extensión original del archivo leido y armo el
    # nombre final por partes.
    fichero = ref_filename.split(".")[0]
    fichero = (
        fichero.split("_")[0] + "_" + fichero.split("_")[1] + "_" + fichero.split("_")[2] + "_" + fichero.split("_")[4]
    )
    return fichero + ".nc"


def extract_netcdf_filename_components(filename: str) -> dict:
    """
    Extract radar_name, strategy, vol_nr, field_type, and timestamp from a netCDF filename.

    Uses pre-compiled regex for efficient repeated calls.

    netCDF filename format: RADAR_VOLCODE_VOLNR_TIMESTAMP.nc
    Example: RMA11_0302_01_20251120T120000Z.nc

    Args:
        filename: netCDF filename to parse
    Returns:
        Dictionary with keys: radar_name, vol_code, vol_nr, timestamp
        Returns None for any key if extraction fails.
    Example:
        >>> result = extract_netcdf_filename_components('RMA11_0302_01_20251120T120000Z.nc')
        >>> result
        {'radar_name': 'RMA11', 'vol_code': '0302', 'vol_nr': '01', 'timestamp': '20251120T120000Z'}
    """
    match = config._NETCDF_FILENAME_PATTERN.match(filename)

    if match:
        return {
            "radar_name": match.group(1),
            "strategy": match.group(2),
            "vol_nr": match.group(3),
            "timestamp": match.group(4),
        }
    else:
        return {
            "radar_name": None,
            "strategy": None,
            "vol_nr": None,
            "timestamp": None,
        }


def extract_cog_filename_components(filename: str) -> dict:
    """
    Extract radar_name, strategy, vol_nr, field_type, timestamp, and filtered status from a COG filename.

    Uses pre-compiled regex for efficient repeated calls.

    COG filename format: RADAR_TIMESTAMP_FIELD_SWEEP[_o].tif
    Example: RMA1_20260326T200000Z_VRAD_00.tif (filtered)
             RMA1_20260326T200000Z_VRADo_00.tif (non-filtered)

    Args:
        filename: COG filename to parse
    Returns:
        Dictionary with keys: radar_name, timestamp, field_type, sweep, filtered
        Returns None for any key if extraction fails.
        {'radar_name': 'RMA1', 'timestamp': '20260326T200000Z', 'field_type': 'VRAD', 'sweep': '00', 'filtered': False}
    """
    match = config._COG_FILENAME_PATTERN.match(filename)

    if match:
        return {
            "radar_name": match.group(1),
            "timestamp": match.group(2),
            "field_type": match.group(3),
            "filtered": not bool(match.group(4)),  # If group(4) is 'o', it's non-filtered
            "sweep": match.group(5),
        }
    else:
        return {
            "radar_name": None,
            "timestamp": None,
            "field_type": None,
            "sweep": None,
            "filtered": None,
        }


def extract_bufr_filename_components(filename: str) -> dict:
    """
    Extract radar_name, strategy, vol_nr, and field_type from a BUFR filename.

    Uses pre-compiled regex for efficient repeated calls.

    BUFR filename format: RADAR_VOLCODE_VOLNR_FIELD_TIMESTAMP.BUFR
    Example: RMA11_0302_01_TH_20251120T120000Z.BUFR

    Args:
        filename: BUFR filename to parse

    Returns:
        Dictionary with keys: radar_name, vol_code, vol_nr, field_type, timestamp
        Returns None for any key if extraction fails.

    Example:
        >>> result = extract_bufr_filename_components('RMA11_0302_01_TH_20251120T120000Z.BUFR')
        >>> result
        {'radar_name': 'RMA11', 'vol_code': '0302', 'vol_nr': '01', 'field_type': 'TH', 'timestamp': '20251120T120000Z'}
    """
    match = config._BUFR_FILENAME_PATTERN.match(filename)

    if match:
        return {
            "radar_name": match.group(1),
            "strategy": match.group(2),
            "vol_nr": match.group(3),
            "field_type": match.group(4),
            "timestamp": match.group(5),
        }
    else:
        return {
            "radar_name": None,
            "strategy": None,
            "vol_nr": None,
            "field_type": None,
            "timestamp": None,
        }


def build_vol_types_regex(vol_types: Dict[str, Dict[str, list]]) -> Optional[re.Pattern]:
    """
    Build a compiled regex pattern from vol_types dictionary to match BUFR filenames.

    The vol_types dictionary structure:
        vol_types['vol_code'] = {'vol_nr': ['FIELD1', 'FIELD2', ...], ...}

    BUFR filename format (assumed): RADAR_VOLCODE_VOLNR_FIELD_TIMESTAMP.BUFR

    The regex will match filenames where vol_code, vol_nr, and field are all present
    in the vol_types dictionary.

    Example:
        >>> vol_types = {
        ...     '0302': {'01': ['TH', 'TV', 'DBZH']},
        ...     '0303': {'01': ['RHOHV', 'KDP'], '02': ['VRAD']}
        ... }
        >>> regex = build_vol_types_regex(vol_types)
        >>> regex.match('RMA11_0302_01_TH_20251120T120000Z.BUFR')  # True
        >>> regex.match('RMA11_0302_01_ZDR_20251120T120000Z.BUFR')  # False (ZDR not in list)

    Args:
        vol_types: Dictionary mapping vol_code -> {vol_nr -> [field_names]}

    Returns:
        Compiled regex pattern, or None if vol_types is empty.
    """
    if not vol_types:
        return None

    # Build list of patterns: vol_code_vol_nr_field combinations
    patterns = []

    for vol_code, vol_numbers in vol_types.items():
        for vol_nr, fields in vol_numbers.items():
            for field in fields:
                # Create pattern: _VOLCODE_VOLNR_FIELD_
                # Using escaped characters to handle special regex chars
                pattern = f"_{re.escape(vol_code)}_{re.escape(vol_nr)}_{re.escape(field)}_"
                patterns.append(pattern)

    if not patterns:
        return None

    # Combine all patterns with OR (|)
    combined_pattern = "|".join(patterns)

    # Add anchors: match anywhere in filename and end with .BUFR
    full_pattern = f"^.*(?:{combined_pattern}).*\\.BUFR$"

    try:
        return re.compile(full_pattern, re.IGNORECASE)
    except re.error as e:
        logger.error("Failed to compile vol_types regex: %s", e)
        return None


def product_path_and_filename(
    radar_name: str,
    strategy: str,
    vol_nr: str,
    field_name: str,
    observation_timestamp: _datetime,
    base_dir: Path,
    filtered: bool = True,
    round_filename: bool = False,
) -> Path:
    """Generate a COG output path including strategy and volume number.

    Format::

        {base_dir}/{radar_name}/YYYY/MM/DD/
        {radar_name}_{strategy}_{vol_nr}_{timestamp}_{field_name}[o].tif

    The ``'o'`` suffix is appended when *filtered* is ``False`` (raw/unfiltered
    data), following the Output Contract convention.

    Args:
        radar_name: Radar station identifier (e.g. ``"RMA1"``).
        strategy: Volume scan strategy code (e.g. ``"0315"``).
        vol_nr: Volume number string (e.g. ``"01"``).
        field_name: Radar field name (e.g. ``"DBZH"``, ``"COLMAX"``).
        observation_timestamp: Observation datetime in UTC with second
            precision.
        base_dir: Root output directory.
        filtered: ``True`` (default) for filtered output — no suffix.
            ``False`` for raw/unfiltered output — appends ``'o'``.
        round_filename: If ``True``, round the timestamp to the nearest
            10 minutes (legacy behaviour). Defaults to ``False`` (exact
            seconds precision).

    Returns:
        :class:`~pathlib.Path` pointing to the full output file path.
        Parent directories are created automatically.

    Examples:
        >>> from datetime import datetime, timezone
        >>> ts = datetime(2026, 4, 1, 20, 50, 0, tzinfo=timezone.utc)
        >>> product_path_and_filename(
        ...     "RMA1", "0315", "01", "DBZH", ts, Path("/products"), filtered=True
        ... )
        PosixPath('/products/RMA1/2026/04/01/RMA1_0315_01_20260401T205000Z_DBZH.tif')
    """
    if round_filename:
        rounded_min = str(round(observation_timestamp.minute / 10) * 10).zfill(2)
        timestamp_str = f"{observation_timestamp.strftime('%Y%m%dT%H')}{rounded_min}00Z"
    else:
        timestamp_str = observation_timestamp.strftime("%Y%m%dT%H%M%SZ")

    field_suffix = "" if filtered else "o"
    filename = f"{radar_name}_{strategy}_{vol_nr}_{timestamp_str}_{field_name}{field_suffix}.tif"

    date_path = observation_timestamp.strftime("%Y/%m/%d")
    full_path = Path(base_dir) / date_path / filename
    full_path.parent.mkdir(parents=True, exist_ok=True)
    return full_path


def product_path_and_filename_legacy(radar, field, sweep, round_filename=True, filtered=True, extension="png"):
    """Legacy PNG/GeoTIFF filename generator that accepts a PyART Radar object.

    .. deprecated::
        Use :func:`product_path_and_filename` for new raw-COG output.
        This function is retained for PNG generation backward compatibility.
    """
    radar_name = radar.metadata["instrument_name"]
    # root_out = config.root_products

    # non-filtered fields have 'o' suffix
    if not filtered:
        field = f"{field}o"

    fnames_dict = {}
    try:
        if round_filename:
            date = get_time_from_RMA_filename(radar.metadata["filename"])
            cdate = date + datetime.timedelta(seconds=600)
            cdate = cdate.strftime("%Y%m%dT%H%M")[:-1] + "000Z"  # ceiled date
            rounded_min = str(round(date.minute / 10) * 10).zfill(2)
            rdate = f"{date.strftime('%Y%m%dT%H')}{rounded_min}00Z"  # rounded date

            filename_out = f"{radar_name}_{cdate}_{field}_{str(sweep).zfill(2)}.{extension}"
            full_path = os.path.join(rdate[:4], rdate[4:6], rdate[6:8])

            filename_out2 = f"{radar_name}_{rdate}_{field}_{str(sweep).zfill(2)}.{extension}"
            full_path2 = os.path.join(rdate[:4], rdate[4:6], rdate[6:8])

            # return full_path, filename_out, full_path2, filename_out2
            fnames_dict["ceiled"] = (full_path, filename_out)
            fnames_dict["rounded"] = (full_path2, filename_out2)
        else:
            elev = str(radar.get_elevation(sweep)[0])
            filename_out = f"{radar_name}_{elev}_{field}.{extension}"
            full_path = os.path.join(field)

            fnames_dict["non_rounded"] = (full_path, filename_out)
    except Exception as e:
        logger.error(f"Error generating product path and filename: {e}")

    return fnames_dict


def extract_cog_filename_components_v2(filename: str) -> Dict[str, object]:
    """Extract components from the v2 COG filename format.

    Pattern::

        {RADAR}_{STRATEGY}_{VOLNR}_{TIMESTAMP}_{FIELD}[o].tif

    Examples::

        RMA1_0315_01_20260401T205000Z_DBZH.tif    # filtered
        RMA1_0315_01_20260401T205000Z_DBZHo.tif   # unfiltered

    Args:
        filename: Bare filename to parse (no directory prefix).

    Returns:
        Dict with keys:

        - ``radar_name`` (str)
        - ``strategy`` (str)
        - ``vol_nr`` (str)
        - ``timestamp`` (str) — ``YYYYMMDDTHHMMSSZ``
        - ``field_name`` (str)
        - ``filtered`` (bool) — ``True`` when no ``'o'`` suffix is present

    Raises:
        ValueError: If *filename* does not match the v2 pattern.
    """
    pattern = r"^([A-Z0-9]+)_(\d{4})_(\d{2})_(\d{8}T\d{6}Z)_([A-Z0-9]+)(o)?\.tif$"
    match = re.match(pattern, filename)
    if not match:
        raise ValueError(f"Filename does not match v2 COG format: {filename!r}")

    radar_name, strategy, vol_nr, timestamp, field_name, raw_suffix = match.groups()
    return {
        "radar_name": radar_name,
        "strategy": strategy,
        "vol_nr": vol_nr,
        "timestamp": timestamp,
        "field_name": field_name,
        "filtered": raw_suffix is None,  # 'o' present → unfiltered → filtered=False
    }
