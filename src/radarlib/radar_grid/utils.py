"""
Utility functions for PyART integration.
"""

import logging
import math
from typing import Tuple

import numpy as np
import pyart

logger = logging.getLogger(__name__)


def beam_height_max_km(range_max_m: float, elev_deg: float, antenna_alt_m: float = 0.0) -> float:
    """
    Calcula la altura máxima del haz en km para un rango y elevación dados.

    Args:
        range_max_m: Rango máximo del radar en metros
        elev_deg: Ángulo de elevación en grados
        antenna_alt_m: Altura de la antena sobre el suelo en metros (default: 0)

    Returns:
        Altura máxima del haz en kilómetros
    """
    Re = 8.49e6  # Radio efectivo de la Tierra en metros
    r = float(range_max_m)
    th = math.radians(float(elev_deg))
    h = r * math.sin(th) + (r * r) / (2.0 * Re) + antenna_alt_m
    return h / 1000.0  # Convertir a km


def infer_blind_range_m(
    radar: pyart.core.Radar,
    default: float = 0.0,
    extra_margin_m: float = 0.0,
) -> float:
    """
    Infiere el radio ciego cercano al radar en metros.

    Estrategia de inferencia (en orden):
    1) ``radar.range['data'][0]`` (centro del primer gate)
    2) atributo ``meters_to_center_of_first_gate`` de ``radar.range``
    3) ``default``

    Args:
        radar: Objeto radar de PyART
        default: Valor de fallback en metros si no se puede inferir
        extra_margin_m: Margen adicional para ampliar el radio ciego

    Returns:
        Radio ciego en metros (>= 0)
    """
    first_gate_m = None

    try:
        r = radar.range.get("data", None)
        if r is not None:
            arr = np.asarray(getattr(r, "filled", lambda v: r)(np.nan), dtype=float)
            if arr.size > 0 and np.isfinite(arr[0]):
                first_gate_m = float(arr[0])
    except Exception:
        first_gate_m = None

    if first_gate_m is None:
        try:
            r = radar.range
            attr_val = getattr(r, "meters_to_center_of_first_gate", None)
            if attr_val is not None and np.isfinite(float(attr_val)):
                first_gate_m = float(attr_val)
        except Exception:
            first_gate_m = None

    if first_gate_m is None:
        first_gate_m = float(default)

    blind_range_m = max(0.0, float(first_gate_m) + float(extra_margin_m))
    return blind_range_m


def compute_beam_height(
    horizontal_distance: np.ndarray,
    elevation_deg: float,
    radar_altitude: float = 0.0,
    ke: float = 4.0 / 3.0,
    re: float = 6.371e6,
) -> np.ndarray:
    """
    Calcula la altura del haz del radar considerando curvatura terrestre.

    Usa el modelo estándar 4/3 de radio efectivo de la Tierra que considera
    tanto la curvatura geométrica como la refracción atmosférica estándar.

    Fórmula completa:
        h = sqrt(r² + (ke*Re)² + 2*r*ke*Re*sin(θ)) - ke*Re + h0

    Donde:
        - r: slant range (rango inclinado) ≈ distancia_horizontal / cos(θ)
        - ke: factor de radio efectivo (4/3 para refracción estándar)
        - Re: radio de la Tierra (6.371e6 m)
        - θ: ángulo de elevación
        - h0: altura del radar sobre el nivel del mar

    Args:
        horizontal_distance: Distancia horizontal desde el radar en metros (array o escalar)
        elevation_deg: Ángulo de elevación en grados
        radar_altitude: Altura del radar sobre el nivel del mar en metros (default: 0)
        ke: Factor de radio efectivo de la Tierra (default: 4/3)
        re: Radio de la Tierra en metros (default: 6.371e6)

    Returns:
        Altura del haz en metros sobre el nivel del mar (mismo shape que input)

    Notas:
        - Importante para rangos > 50 km donde la curvatura es significativa
        - Para rangos cortos, coincide aproximadamente con h = r*tan(θ)

    Referencias:
        Doviak, R. J., and D. S. Zrnić, 1993: Doppler Radar and Weather
        Observations. Academic Press, 562 pp.
    """
    # Convertir a radianes
    elev_rad = np.radians(elevation_deg)
    sin_elev = np.sin(elev_rad)
    cos_elev = np.cos(elev_rad)

    # Radio efectivo de la Tierra
    ke_re = ke * re

    # Aproximar slant range desde distancia horizontal
    # Para ángulos pequeños: r ≈ s / cos(θ)
    slant_range = horizontal_distance / np.maximum(cos_elev, 0.01)

    # Altura usando ecuación completa con curvatura terrestre
    height = np.sqrt(slant_range**2 + ke_re**2 + 2 * slant_range * ke_re * sin_elev) - ke_re + radar_altitude

    return height


def calculate_z_limits(
    range_max_m: float,
    elevation: int = 0,
    # cappi_height: float = 4000,
    radar_fixed_angles=None,
    round_to_km: int = 20,
    toa: float = 15000.0,
) -> tuple[float, float, float | None]:
    """
    Calcula límites verticales (z_min, z_max).

    Args:
        range_max_m: Rango máximo del radar en metros
        elevation: Índice de elevación (para PPI)
        cappi_height: Altura CAPPI en metros (para CAPPI)
        radar_fixed_angles: Array con ángulos de elevación fijos del radar
        round_to_km: Redondear z_max hacia arriba a múltiplos de este valor en km
                     (default 20). Estabiliza el caché: mediciones del mismo radar
                     con alturas máximas ligeramente distintas convergen al mismo
                     z_max y reutilizan la grilla 3D y el operador W cacheados.
                     Pasar 0 para desactivar el redondeo.

    Returns:
        Tupla (z_min, z_max, elev_deg) donde:
            - z_min: Altura mínima en metros (siempre 0.0)
            - z_max: Altura máxima en metros del último nivel (redondeada hacia arriba)
            - elev_deg: Ángulo de elevación usado
    """

    if radar_fixed_angles is None:
        raise ValueError("radar_fixed_angles requerido para PPI/COLMAX")

    elev_deg = float(radar_fixed_angles[elevation])
    hmax_km = beam_height_max_km(range_max_m, elev_deg)
    z_top_m = (hmax_km + 3) * 1000  # +3 km de margen

    if round_to_km > 0:
        step_m = round_to_km * 1000.0
        z_top_m = math.ceil(z_top_m / step_m) * step_m

    return (0.0, toa, elev_deg)


def calculate_grid_resolution(volume: str) -> tuple[float, float]:
    """
    Calcula resolución XY y Z para la grilla según el volumen del radar.

    Args:
        volume: Identificador del volumen del radar ('03' tiene mayor resolución)

    Returns:
        Tupla (grid_resolution_xy, grid_resolution_z) en metros:
            - grid_resolution_xy: Resolución horizontal (depende del volumen)
            - grid_resolution_z: Resolución vertical (siempre 600m para cross-sections)
    """
    # XY depende del volumen, pero Z siempre usa resolución fina para transectos suaves
    grid_resolution_xy = 300 if volume == "03" else 1000
    grid_resolution_z = 600

    return grid_resolution_xy, grid_resolution_z


def calculate_grid_points(
    z_limits: tuple[float, float],
    y_limits: tuple[float, float],
    x_limits: tuple[float, float],
    resolution_xy: float,
    resolution_z: float,
) -> tuple[int, int, int]:
    """
    Calcula número de puntos de la grilla en cada dimensión.

    Args:
        z_limits: Tupla (z_min, z_max) en metros
        xy_limits: Tupla (min, max) para X e Y en metros (asume cuadrado)
        resolution_xy: Resolución horizontal en metros
        resolution_z: Resolución vertical en metros

    Returns:
        Tupla (z_points, y_points, x_points) con cantidad de puntos en cada eje
    """
    z_points = int(np.ceil(z_limits[1] / resolution_z)) + 1
    y_points = int((y_limits[1] - y_limits[0]) / resolution_xy) + 1
    x_points = int((x_limits[1] - x_limits[0]) / resolution_xy) + 1

    return z_points, y_points, x_points


def calculate_roi_dist_beam(
    z_coords,
    y_coords,
    x_coords,
    h_factor: float = 0.8,
    nb: float = 1.0,
    bsp: float = 0.8,
    min_radius: float = 300.0,
    radar_offset: tuple[float, float, float] = (0.0, 0.0, 0.0),
):
    """
    Calcula el Radio de Influencia (ROI) usando el método dist_beam que simula
    el ensanchamiento geométrico del haz del radar con la distancia.

    El ROI crece con la distancia del radar basándose en:
    - Altura del voxel (componente vertical)
    - Distancia horizontal del voxel (componente que simula apertura del haz)

    Fórmula dist_beam (PyART):
        ROI = max(h_factor*(z/20) + distancia_xy*tan(nb*bsp*π/180), min_radius)

    Args:
        z_coords: Coordenada(s) Z en metros (escalar o array)
        y_coords: Coordenada(s) Y en metros (escalar o array)
        x_coords: Coordenada(s) X en metros (escalar o array)
        h_factor: Factor de escalado de altura (default 0.8)
            - Controla cuánto contribuye la altura al ROI
            - Dividido por 20 para evitar que domine sobre componente horizontal
        nb: Ancho de haz virtual en grados (default 1.0)
            - 0.5-1.0°: Radares de investigación alta resolución (recomendado)
            - 1.5°: Radares meteorológicos operacionales
            - 2.0-3.0°: Radares antiguos o baja resolución
        bsp: Espaciado entre haces (default 0.8)
            - Multiplica el ancho del haz para simular solapamiento
        min_radius: Radio mínimo en metros (default 300.0)
            - Garantiza ROI mínimo cerca del radar donde z y xy son pequeños
            - Previene ROI demasiado pequeño que causaría voxels sin datos
        radar_offset: Offset (z, y, x) del centro del radar en metros (default origen)

    Returns:
        ROI en metros (mismo shape que inputs: escalar o array)
    """
    # Convertir a arrays para operaciones vectorizadas
    z = np.asarray(z_coords, dtype=np.float32)
    y = np.asarray(y_coords, dtype=np.float32)
    x = np.asarray(x_coords, dtype=np.float32)

    # Aplicar offset del radar (normalmente en el origen)
    z_rel = z - radar_offset[0]
    y_rel = y - radar_offset[1]
    x_rel = x - radar_offset[2]

    # Componente vertical: altura dividida por 20 para evitar que domine
    vertical_component = h_factor * (z_rel / 20.0)

    # Componente horizontal: distancia XY multiplicada por tangente del ángulo del haz
    # tan(nb * bsp * π/180) simula el ensanchamiento del haz con la distancia
    xy_distance = np.sqrt(x_rel**2 + y_rel**2)
    beam_angle_rad = nb * bsp * np.pi / 180.0
    horizontal_component = xy_distance * np.tan(beam_angle_rad)

    # ROI total: suma de componentes con límite mínimo
    roi = np.maximum(vertical_component + horizontal_component, min_radius)

    return roi


def safe_range_max_m(radar: pyart.core.Radar, default: float = 240e3, round_to_km: int = 20) -> float:
    """
    Devuelve el alcance máximo (último gate) en metros, con fallback.
    Redondea hacia arriba al múltiplo de round_to_km km para alinear grids.

    Args:
        radar: Objeto radar de PyART
        default: Valor por defecto si no se puede determinar
        round_to_km: Redondear hacia arriba a múltiplos de este valor en km (default: 20km)
                     Ejemplos: 116580m → 120000m, 236460m → 240000m

    Returns:
        Rango máximo redondeado en metros
    """
    import math

    r = radar.range["data"]
    arr = np.asarray(getattr(r, "filled", lambda v: r)(np.nan), dtype=float)
    if arr.size == 0:
        range_m = float(default)
    else:
        last = float(arr[-1])
        if np.isfinite(last):
            range_m = last
        else:
            # fallback al máximo finito
            finite = arr[np.isfinite(arr)]
            range_m = float(finite.max()) if finite.size else float(default)

    # Redondear hacia arriba al múltiplo más cercano
    if round_to_km > 0:
        step_m = round_to_km * 1000.0
        range_m = math.ceil(range_m / step_m) * step_m

    return float(range_m)


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
