"""
Utility functions for PyART integration.
"""

import logging
from typing import Tuple

import numpy as np

logger = logging.getLogger(__name__)


def get_gate_coordinates(radar) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Extract flattened gate coordinates from a PyART radar object.

    Parameters
    ----------
    radar : pyart.core.Radar
        PyART radar object

    Returns
    -------
    gate_x : np.ndarray
        Flattened x coordinates in meters, shape (n_gates,)
    gate_y : np.ndarray
        Flattened y coordinates in meters, shape (n_gates,)
    gate_z : np.ndarray
        Flattened z coordinates (altitude) in meters, shape (n_gates,)

    Notes
    -----
    Coordinates are relative to the radar location.
    The total number of gates is nrays * ngates_per_ray.
    """
    gate_x = radar.gate_x["data"].ravel().astype("float32")
    gate_y = radar.gate_y["data"].ravel().astype("float32")
    gate_z = radar.gate_z["data"].ravel().astype("float32")
    return gate_x, gate_y, gate_z


def get_field_data(radar, field_name: str) -> np.ndarray:
    """
    # Extract flattened field data and mask from a PyART radar object.
    Extract flattened field data from a PyART radar object as a masked array.

    Parameters
    ----------
    radar : pyart.core.Radar
        PyART radar object
    field_name : str
        Name of the field to extract (e.g., 'DBZH', 'ZDR', 'KDP')

    Returns
    -------
    field_data : np.ndarray
        Flattened masked field values, shape (n_gates,)
    # field_mask : np.ndarray
    #     Boolean mask where True = invalid, shape (n_gates,)

    Notes
    -----
    Uses np.ma.masked_invalid() to properly handle NaN, Inf, and
    existing masked values in the radar field data.
    """
    field = radar.fields[field_name]["data"]

    return np.ma.masked_invalid(field).ravel().astype("float32")


def get_available_fields(radar) -> list:
    """
    Get list of available field names in a radar object.

    Parameters
    ----------
    radar : pyart.core.Radar
        PyART radar object

    Returns
    -------
    list
        List of field names
    """
    return list(radar.fields.keys())


def get_radar_altitude(radar) -> float:
    """
    Get radar altitude in meters above sea level.

    Parameters
    ----------
    radar : pyart.core.Radar
        PyART radar object

    Returns
    -------
    float
        Radar altitude in meters
    """
    return float(radar.altitude["data"][0])


def get_radar_info(radar) -> dict:
    """
    Get basic information about a radar object.

    Parameters
    ----------
    radar : pyart.core.Radar
        PyART radar object

    Returns
    -------
    dict
        Dictionary with radar metadata
    """
    return {
        "radar_name": radar.metadata.get("instrument_name", "UNKNOWN"),
        "strategy": radar.metadata.get("scan_id", "UNKNOWN"),
        "volume_nr": f"{int(radar.metadata.get('volume_number', 0)):02d}",
        "nrays": radar.nrays,
        "ngates": radar.ngates,
        "nsweeps": radar.nsweeps,
        "total_gates": radar.nrays * radar.ngates,
        "fields": list(radar.fields.keys()),
        "range_min": float(radar.range["data"][0]),
        "range_max": float(radar.range["data"][-1]),
        "latitude": float(radar.latitude["data"][0]),
        "longitude": float(radar.longitude["data"][0]),
        "altitude": float(radar.altitude["data"][0]),
    }
