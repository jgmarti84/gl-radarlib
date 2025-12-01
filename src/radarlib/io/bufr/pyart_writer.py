from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from radarlib.utils.names_utils import get_netcdf_filename_from_bufr_filename

logger = logging.getLogger(__name__)


PRODUCT_UNITS = {
    "DBZH": "dBZ",
    "DBZV": "dBZ",
    "ZDR": "dB",
    "KDP": "deg/km",
    "VRAD": "m/s",
}


def _find_reference_field(fields: List[dict]) -> int:
    """Return the index of the field that has the farthest range.

    The input is expected to be a list where each element contains an
    `'info'` dict with a `'sweeps'` DataFrame including `gate_offset`,
    `gate_size` and `ngates`.
    """
    if not fields:
        raise ValueError("no fields provided")

    # Compute a per-field maximum last_gate distance and pick the field with the largest
    # last gate. This is robust even when the concatenated sweeps do not contain a
    # 'vol_id' column.
    max_last = -1
    max_idx = 0
    for i, f in enumerate(fields):
        sweeps = f.get("info", {}).get("sweeps")
        if sweeps is None or sweeps.empty:
            continue
        last_gate = (sweeps["gate_offset"] + sweeps["gate_size"] * sweeps["ngates"]).max()
        if last_gate > max_last:
            max_last = last_gate
            max_idx = i
    return max_idx


def _create_empty_radar(n_gates: int, n_rays: int, n_sweeps: int, radar_name: str):
    """Create an empty pyart PPI radar object using testing factory.

    This keeps the implementation testable without constructing full
    internal pyart Radar objects by hand.
    """
    import pyart

    radar = pyart.testing.make_empty_ppi_radar(n_gates, n_rays, n_sweeps)
    return radar


def _align_field_to_reference(field: dict, ref_gate_offset: int, ref_gate_size: int, ref_ngates: int):
    """Return a copy of `field` whose `data` array is aligned to the reference grid.

    The returned dict has the same keys as input but `data` will have shape
    (nrays, ref_ngates) and dtype float32.
    """
    # Defensive copy
    out = field.copy()
    data = np.array(field["data"], copy=True)
    nrays, ngates = data.shape
    out_data = np.ma.masked_all((nrays, ref_ngates), dtype=np.float32)

    field_offset = int(field["info"]["sweeps"]["gate_offset"].iloc[0])
    field_gate_size = int(field["info"]["sweeps"]["gate_size"].iloc[0])

    if field_gate_size != ref_gate_size:
        raise ValueError("gate_size mismatch not supported in align routine")

    if field_offset == ref_gate_offset:
        out_data[:, :ngates] = data
    else:
        init = int((field_offset - ref_gate_offset) // ref_gate_size)
        if init < 0 or init + ngates > ref_ngates:
            raise ValueError("field cannot be aligned to reference grid")
        out_data[:, init : init + ngates] = data

    out["data"] = out_data
    return out


def bufr_fields_to_pyart_radar(
    fields: List[dict],
    *,
    include_scan_metadata: bool = False,
    root_scan_config_files: Optional[Path] = None,
    config: Optional[Dict[str, Any]] = None,
    debug: bool = False,
) -> Any:
    """Convert a list of BUFR-parsed field dicts to a Py-ART Radar object.

    This function mirrors the previous `bufr_to_pyart_new` behavior but is
    modular and easier to test. It intentionally avoids writing files.

    This version works with field dicts output by bufr_to_dict() where each
    dict contains 'data' and 'info' keys. The 'info' dict has a 'sweeps'
    DataFrame with columns like gate_offset, gate_size, ngates, nrayos, elevaciones.
    """
    if not fields:
        raise ValueError("fields is empty")

    # find reference field (the one with farthest range)
    ref_idx = _find_reference_field(fields)
    ref_field = fields[ref_idx]

    # Get dimensions from reference field
    ref_ngates = int(ref_field["info"]["sweeps"]["ngates"].iloc[0])
    # NOTE: nrayos is per sweep; make_empty_ppi_radar will multiply by nsweeps
    ref_rays_per_sweep = int(ref_field["info"]["sweeps"]["nrayos"].iloc[0])
    ref_nsweeps = int(ref_field["info"].get("nsweeps", 1))

    radar_name = ref_field["info"]["metadata"].get("instrument_name", "RADAR")
    radar = _create_empty_radar(ref_ngates, ref_rays_per_sweep, ref_nsweeps, radar_name)

    # range axis
    gate_size = int(ref_field["info"]["sweeps"]["gate_size"].iloc[0])
    gate_offset = int(ref_field["info"]["sweeps"]["gate_offset"].iloc[0])
    range_data = gate_offset + gate_size * np.arange(radar.ngates)
    radar.range = radar.range  # keep existing structure
    radar.range["data"] = range_data
    radar.range["meters_between_gates"] = gate_size
    radar.range["meters_to_center_of_first_gate"] = gate_offset

    # elevation/azimuth/fixed_angle
    rays_per_sweep = ref_field["info"]["sweeps"]["nrayos"].to_numpy()
    elevs = np.array(ref_field["info"]["sweeps"]["elevaciones"], dtype=np.float32)
    radar.elevation["data"] = np.repeat(elevs, rays_per_sweep)
    radar.azimuth["data"] = np.concatenate([np.arange(n, dtype=np.float32) for n in rays_per_sweep])
    radar.fixed_angle["data"] = elevs

    # metadata
    radar.metadata = radar.metadata
    radar.metadata.update(ref_field["info"]["metadata"])

    # Load geographic coordinates
    radar.latitude["data"] = np.ndarray(1)
    radar.latitude["data"][0] = ref_field["info"].get("lat", 0)
    radar.latitude["units"] = "degrees"
    radar.latitude["long_name"] = "latitude"
    radar.latitude["_fillValue"] = -9999.0

    radar.longitude["data"] = np.ndarray(1)
    radar.longitude["data"][0] = ref_field["info"].get("lon", 0)
    radar.longitude["units"] = "degrees"
    radar.longitude["long_name"] = "longitude"
    radar.longitude["_fillValue"] = -9999.0

    radar.altitude["data"] = np.ndarray(1)
    radar.altitude["data"][0] = ref_field["info"].get("altura", 0)
    radar.altitude["units"] = "meters"
    radar.altitude["long_name"] = "altitude"
    radar.altitude["_fillValue"] = -9999.0

    # add fields aligned to reference
    for field in fields:
        aligned = _align_field_to_reference(field, gate_offset, gate_size, ref_ngates)
        name = field["info"].get("tipo_producto", "UNKNOWN")
        # remove non-serializable metadata before adding to radar
        if "info" in aligned:
            del aligned["info"]
        # units mapping
        units = PRODUCT_UNITS.get(name)
        if units:
            aligned["units"] = units
        radar.add_field(name, aligned, replace_existing=True)

    return radar


def bufr_paths_to_pyart(
    bufr_paths: List[str],
    *,
    root_resources: Optional[str] = None,
    include_scan_metadata: bool = False,
    root_scan_config_files: Optional[Path] = None,
    config: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    save_path: Optional[Path] = None,
) -> Any:
    """Decode one or more BUFR files and convert to Py-ART Radar object."""
    from radarlib.io.bufr.bufr import bufr_to_dict

    fields = []
    for p in bufr_paths:
        vol = bufr_to_dict(p, root_resources=root_resources, legacy=False)
        if vol is None:
            continue
        fields.append(vol)
    # the previous code expects a list of fields; we support passing list with a single volume
    radar = bufr_fields_to_pyart_radar(
        fields,
        include_scan_metadata=include_scan_metadata,
        root_scan_config_files=root_scan_config_files,
        config=config,
        debug=debug,
    )

    if save_path is not None:
        save_path = Path(save_path)
        save_path.mkdir(parents=True, exist_ok=True)
        # base = Path(p).stem
        netcdf_fname = get_netcdf_filename_from_bufr_filename(str(Path(p).stem))
        out_file = save_path / netcdf_fname
        save_radar_to_cfradial(radar, out_file)
        return radar
    return radar


def bufr_to_pyart(
    fields: List[dict],
    *,
    debug: bool = False,
    logger_name: str = __name__,
    include_scan_metadata: bool = False,
    root_scan_config_files: Optional[Path] = None,
) -> Any:
    """Convert a list of decoded BUFR field dicts to a PyART Radar object.

    This is the modern equivalent to bufr_to_pyart_legacy() and uses the new
    bufr_to_dict() output format (with 'sweeps' DataFrame in info).

    Parameters
    ----------
    fields : List[dict]
        List of field dicts from bufr_to_dict(bufr_filename, legacy=False).
        Each dict has 'data' array and 'info' dict with 'sweeps' DataFrame.
    debug : bool, optional
        Debug logging flag (default False)
    logger_name : str, optional
        Logger name (default __name__)
    include_scan_metadata : bool, optional
        Include RMA scan metadata from XML files (default False)
    root_scan_config_files : Path, optional
        Root directory for scan config XML files

    Returns
    -------
    pyart.Radar
        PyART Radar object with all fields combined
    """
    if not fields:
        raise ValueError("fields is empty")

    return bufr_fields_to_pyart_radar(
        fields,
        include_scan_metadata=include_scan_metadata,
        root_scan_config_files=root_scan_config_files,
        debug=debug,
    )


def save_radar_to_cfradial(radar: Any, out_file: Path, format: str = "NETCDF4") -> Path:
    """Save a Py-ART Radar object to a CFRadial NetCDF file using pyart.io.cfradial.write_cfradial.

    Returns the path to the written file.
    """
    import pyart

    try:
        pyart.io.cfradial.write_cfradial(str(out_file), radar, format=format)
    except Exception as exc:
        logger.error("Failed to write CFRadial file %s: %s", out_file, exc)
        raise
    return out_file
