# -*- coding: utf-8 -*-

import logging
from math import asin, cos, radians, sin, sqrt
from typing import List, Optional

import numpy as np
from pyart.config import get_field_name, get_metadata
from pyart.core import Radar

from radarlib import config

LINE_LARGE = 90
LINE_FORMAT = "90.90"


def gps_to_distance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6367 * c

    return km


def indx_az_proximo(radar: Radar, az_target: float) -> int:
    """------------------------------------------------------------------------
    Identificamos Azimuth más próximo a la estación en el barrido actual.
    El rango puede variar dependiendo de la conf del radar y el azimuth
    producto del angulo de inicio del barrido).
    ------------------------------------------------------------------------"""
    b = az_target
    # Variable que contendrá el indice dentro de los azimuth que
    # coincide con el azimuth cargado de cada estación
    indx_az_proximo = -1

    if int(radar.azimuth["data"][0]) > b:
        indx_az_proximo = radar.nrays - (int(radar.azimuth["data"][0]) - b) - 1
    else:
        indx_az_proximo = b - int(radar.azimuth["data"][0])  # buscado-ray_ini
    indx_az_proximo = int(indx_az_proximo)

    # Elegimos el Azimuth mas próximo entre los vecinos.
    # Para optimizar se podría utilizar np.round sobre el index directamente,
    # solo hay que chequear si andaría bien en todos los casos como
    # rayos comenzados en 0.5°
    if np.absolute(radar.azimuth["data"][indx_az_proximo] - az_target) > np.absolute(
        radar.azimuth["data"][indx_az_proximo + 1] - az_target
    ):
        indx_az_proximo = indx_az_proximo + 1

    return indx_az_proximo


def indx_range_proximo(
    radar: Radar, range_target: Optional[float] = None, debug: bool = False, logger_name: str = __name__
) -> Optional[int]:
    """------------------------------------------------------------------------
    Identificamos Gate más próximo a la estación en el barrido actual.
    El rango puede variar dependiendo de la conf del radar y el azimuth
    producto del angulo de inicio del barrido.
    ------------------------------------------------------------------------"""

    logger = logging.getLogger(logger_name)

    if radar.range["spacing_is_constant"]:
        # Detecta index más próximo del rango buscado
        gate_width = radar.range["meters_between_gates"]
        gate_ini = radar.range["meters_to_center_of_first_gate"]
        indx_range_proximo = int((range_target - gate_ini) / gate_width)

        # si el objetivo está más cerca que el gate inicial
        # o mas lejos que el gate final retorna None
        if range_target - gate_ini < 0 or range_target > radar.range["data"][-1]:
            if debug:
                logger.debug("Target mas proximo que el gate inicial o más " + "lejano que el gate final del radar.")
            return None

        # Elegimos el Rango mas próximo entre los vecinos.
        if np.absolute(radar.range["data"][indx_range_proximo] - range_target) > np.absolute(
            radar.range["data"][indx_range_proximo + 1] - range_target
        ):
            indx_range_proximo = indx_range_proximo + 1

        return indx_range_proximo
    else:
        logger.error("Error: El espaciado de bins no es constante. Funcionalidad " + "aún no implementada.")


def get_radar_gate_dimensions(
    radar: Radar,
    gate: int,
    #    plot=False,
    debug: bool = False,
    verbose: bool = False,
    logger_name: str = __name__,
) -> list:
    """
    Devuelve las dimensiones físicas aproximadas de un gate.

    Returns :
    * Ancho de la sección transversal del haz más próxima al radar: op_corto
    * Ancho de la sección transversal del haz más lejana al radar: op_largo
    * Largo de la diagonal: diagonal
    """
    logger = logging.getLogger(logger_name)
    if verbose:
        logger.info("")
        logger.info("Calculando dimensiones de gate de radar ...")

    # rango al centro del gate + medio gate
    ady_largo = radar.range["data"][gate] + radar.range["meters_between_gates"] / 2
    ady_corto = radar.range["data"][gate] - radar.range["meters_between_gates"] / 2

    op_largo = np.tan(np.pi / 360) * ady_largo  # tan(0.5°)*cat_adyacente=cat_opuesto
    op_largo = op_largo * 2  # recien calculamos solo la mitad del lado deseado.
    op_corto = np.tan(np.pi / 360) * ady_corto  # tan(0.5°)*cat_adyacente=cat_opuesto
    op_corto = op_corto * 2  # recien calculamos solo la mitad del lado deseado.

    # hip = ancho_bin / cos 0.5
    diagonal = radar.range["meters_between_gates"] / np.cos(np.pi / 360)

    if debug:
        logger.debug("Opuesto Largo: ", op_largo)
        logger.debug("Opuesto Corto: ", op_corto)
        logger.debug("Diagonal: ", diagonal)

    # if plot:
    #     import matplotlib.pyplot as plt
    #     plt.plot([0,ady_corto,ady_largo], [0,op_corto/2,op_largo/2])
    #     plt.plot([0,ady_corto,ady_largo], [0,-op_corto/2,-op_largo/2])

    #     plt.plot([ady_corto,ady_corto], [-op_corto/2,op_corto/2])
    #     plt.plot([ady_largo,ady_largo], [-op_largo/2,op_largo/2])
    #     plt.axis([0, 40000, -18000, 18000])

    #     plt.show()

    return [op_largo, op_corto, diagonal]


def get_relative_polar_coord_from_two_geo_coords(lon_ref: float, lat_ref: float, lon_target: float, lat_target: float):
    """
    Devuelve azimuth y distancia de la estación relativa al radar [ray,gate]
    """
    # Aplicamos teor.del coseno para calcular el ang de la aws respet al radar
    # recta: aws a punto aux
    a = gps_to_distance(lon_target, lat_target, lon_ref, lat_ref + 4.5)
    # alt triangulo
    b = gps_to_distance(lon_ref, lat_ref, lon_ref, lat_ref + 4.5)
    # recta: radar a aws
    c = gps_to_distance(lon_ref, lat_ref, lon_target, lat_target)

    # Teorema del Coseno
    alfa = np.arccos((np.power(c, 2) + np.power(b, 2) - np.power(a, 2)) / (2 * b * c))
    alfa = alfa * 180 / np.pi

    # si la aws esta en el tercer o cuarto cuadrante vamos a estar
    # calculando el angulo externo y no el externo.
    if lon_ref > lon_target:
        alfa = 360 - alfa

    angle = alfa  # [°Deg]
    distance = c * 1000  # [m]
    return angle, distance


def get_radar_parameters_from_geo_coord(
    radar: Radar, point_lat: float, point_lon: float, verbose: bool = False, logger_name: str = __name__
) -> Optional[tuple]:
    """
    Devuelve ray y gate más próximos a un punto determinado especificado
    en coordenadas geográficas (lon y lat).
    """

    logger = logging.getLogger(logger_name)
    if verbose:
        logger.info("")
        logger.info("Determinando parametros de radar a partir de " + "coordenada geográfica ...")

    lon_radar = radar.longitude["data"]
    lat_radar = radar.latitude["data"]

    [ang, distance] = get_relative_polar_coord_from_two_geo_coords(lon_radar, lat_radar, point_lon, point_lat)

    if radar.ngates * radar.range["meters_between_gates"] > distance:
        ray = indx_az_proximo(radar, ang)
        gate = indx_range_proximo(radar, distance)
        return ray, gate

    else:
        if verbose:
            logger.info(
                "Error: La estacion seleccionada esta fuera del rango" + " de cobertura del radar de referencia."
            )
        return None, None


def add_field_to_radar_object(
    field,
    radar: Radar,
    field_name: str = "FH",
    units: str = "unitless",
    long_name: str = "Hidrometeoro ID",
    standard_name: str = "Hidrometeoro ID",
    dz_field: str = "dBZ",
):
    """
    Adds a newly created field to the Py-ART radar object.
    If reflectivity is a masked array,
    make the new field masked the same as reflectivity.
    """
    masked_field = np.ma.asanyarray(field)
    fill_value = -32768
    if hasattr(radar.fields[dz_field]["data"], "mask"):
        # setattr(masked_field, "mask", radar.fields[dz_field]["data"].mask)
        masked_field.mask = radar.fields[dz_field]["data"].mask
        fill_value = radar.fields[dz_field]["_FillValue"]
    field_dict = {
        "data": masked_field,
        "units": units,
        "long_name": long_name,
        "standard_name": standard_name,
        "_FillValue": fill_value,
    }
    radar.add_field(field_name, field_dict, replace_existing=True)
    return radar


def get_geo_coor(radar: Radar, rad_coor: List[tuple]) -> List[tuple]:
    """========================================================================
    Función para obtener las coordenadas geográficas de un conjunto de celdas
    a partir de coordenas las polares (azimut y rango)

    Attributes
    ----------
    radar : radar_object
        Objeto radar tipo PyART.
    rad_coor : list of tuples
        Lista de tuplas (az,gate)
    ========================================================================"""
    geo_coor = []
    for [ray, gate] in rad_coor:
        geo_coor.append([radar.gate_latitude["data"][ray, gate], radar.gate_longitude["data"][ray, gate]])
    return geo_coor


def extract_first_sweep_minor_to_elevation_limit(radar: Radar, elevation_limit: float = 0.6) -> Radar:
    for nsweep in range(radar.nsweeps):
        if radar.get_elevation(sweep=nsweep)[0] < elevation_limit:
            radar = radar.extract_sweeps([nsweep])
            return radar
    raise ValueError("No se encontro ningun barrido menor a " + " la elevacion fijdada.")


def get_first_nsweep_minor_to_elevation_limit(radar: Radar, elevation_limit: float = 0.6) -> int:
    for nsweep in range(radar.nsweeps):
        if radar.get_elevation(sweep=nsweep)[0] < elevation_limit:
            return nsweep
    raise ValueError("No se encontro ningun barrido menor a la " + "elevacion fijdada.")


def get_lowest_nsweep(radar: Radar) -> int:
    sweeps_list = []
    for nsweep in range(radar.nsweeps):
        sweeps_list.append(radar.get_elevation(sweep=nsweep)[0])

    return sweeps_list.index(min(sweeps_list))


def get_field_config_(field, filter: bool = True, logger_name: str = __name__):

    d = {
        "DBZH": {
            "nofilter": {
                "vmin": config.VMIN_REFL_NOFILTERS,
                "vmax": config.VMAX_REFL_NOFILTERS,
                "cmap": config.CMAP_REFL_NOFILTERS,
            },
            "filter": {"vmin": config.VMIN_REFL, "vmax": config.VMAX_REFL, "cmap": config.CMAP_REFL},
        },
        "TH": {
            "nofilter": {
                "vmin": config.VMIN_REFL_NOFILTERS,
                "vmax": config.VMAX_REFL_NOFILTERS,
                "cmap": config.CMAP_REFL_NOFILTERS,
            },
            "filter": {"vmin": config.VMIN_REFL, "vmax": config.VMAX_REFL, "cmap": config.CMAP_REFL},
        },
        "DBZV": {
            "nofilter": {
                "vmin": config.VMIN_REFL_NOFILTERS,
                "vmax": config.VMAX_REFL_NOFILTERS,
                "cmap": config.CMAP_REFL_NOFILTERS,
            },
            "filter": {"vmin": config.VMIN_REFL, "vmax": config.VMAX_REFL, "cmap": config.CMAP_REFL},
        },
        "TV": {
            "nofilter": {
                "vmin": config.VMIN_REFL_NOFILTERS,
                "vmax": config.VMAX_REFL_NOFILTERS,
                "cmap": config.CMAP_REFL_NOFILTERS,
            },
            "filter": {"vmin": config.VMIN_REFL, "vmax": config.VMAX_REFL, "cmap": config.CMAP_REFL},
        },
        "COLMAX": {
            "nofilter": {
                "vmin": config.VMIN_REFL_NOFILTERS,
                "vmax": config.VMAX_REFL_NOFILTERS,
                "cmap": config.CMAP_REFL_NOFILTERS,
            },
            "filter": {"vmin": config.VMIN_REFL, "vmax": config.VMAX_REFL, "cmap": config.CMAP_REFL},
        },
    }
    if field not in d.keys():
        return {"vmin": None, "vmax": None, "cmap": None}

    filter_key = "filter" if filter else "nofilter"
    return d[field][filter_key]


def calcular_zdr(radar: Radar, ref_vertical=None, ref_horizontal=None, zdr_out_field=None) -> Radar:
    """
    Genera el campo Zdr de radar.

    Parameters
    ----------
    radar : TYPE
        DESCRIPTION.
    ref_vertical : TYPE, optional
        DESCRIPTION. The default is None.
    ref_horizontal : TYPE, optional
        DESCRIPTION. The default is None.
    zdr_out_field : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    radar : TYPE
        DESCRIPTION.

    """
    import gc

    if ref_horizontal is None:
        ref_horizontal = get_field_name("reflectivity")
    if ref_vertical is None:
        ref_vertical = get_field_name("vertical_reflectivity")
    if zdr_out_field is None:
        zdr_out_field = get_field_name("differential_reflectivity")

    ZDR = np.zeros((radar.nrays, radar.ngates))
    ZDR = radar.fields[ref_horizontal]["data"] - radar.fields[ref_vertical]["data"]

    # Agregamos el campo a los datos del radar.
    radar.add_field_like(ref_horizontal, zdr_out_field, ZDR, replace_existing=True)

    # Explicitly delete temporary array to free memory
    del ZDR
    gc.collect()

    return radar


def calculate_zdr(radar: Radar, hrefl_field=None, vrefl_field=None, zdr_field=None) -> Radar:  # type: ignore
    """
    Genera el campo Zdr de radar.

    Parameters
    ----------
    radar : TYPE
        DESCRIPTION.
    ref_vertical : TYPE, optional
        DESCRIPTION. The default is None.
    ref_horizontal : TYPE, optional
        DESCRIPTION. The default is None.
    zdr_out_field : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    radar : TYPE
        DESCRIPTION.

    """
    if hrefl_field is None:
        hrefl_field = get_field_name("reflectivity")
    if vrefl_field is None:
        vrefl_field = get_field_name("vertical_reflectivity")
    if zdr_field is None:
        zdr_field = get_field_name("differential_reflectivity")

    if hrefl_field not in radar.fields.keys():
        raise ValueError(f"{hrefl_field} no se encuentra en radar.")

    if vrefl_field not in radar.fields.keys():
        raise ValueError(f"{vrefl_field} no se encuentra en radar.")

    zdr_data = np.zeros((radar.nrays, radar.ngates))
    zdr_data = radar.fields[hrefl_field]["data"] - radar.fields[vrefl_field]["data"]

    # Agregamos el campo a los datos del radar.
    new_field = get_metadata(zdr_field)
    new_field["data"] = zdr_data
    radar.add_field(zdr_field, new_field, replace_existing=True)
    return radar


def determine_reflectivity_fields(radar, logger_name: str = __name__) -> dict:
    """
    Determine the horizontal and vertical reflectivity fields from the radar object.

    Parameters
    ----------
    radar : Radar
        Py-ART Radar object containing fields.

    Returns
    -------
    dict
        A dictionary with the following keys:
        - 'hrefl_field': Horizontal reflectivity field (e.g., 'DBZH').
        - 'hrefl_field_raw': Raw horizontal reflectivity field (e.g., 'TH').
        - 'vrefl_field': Vertical reflectivity field (e.g., 'DBZV').
        - 'vrefl_field_raw': Raw vertical reflectivity field (e.g., 'TV').
    """
    logger = logging.getLogger(logger_name)

    # Initialize default values
    hrefl_field = hrefl_field_raw = "DBZH"
    vrefl_field = vrefl_field_raw = "DBZV"

    # Determine horizontal reflectivity fields
    if "DBZH" in radar.fields and "TH" in radar.fields:
        hrefl_field_raw = "TH"
    elif "DBZH" in radar.fields:
        hrefl_field = hrefl_field_raw = "DBZH"
    elif "TH" in radar.fields:
        hrefl_field = hrefl_field_raw = "TH"
    else:
        logger.warning("Campo de reflectividad horizontal inexistente.")

    # Determine vertical reflectivity fields
    if "DBZV" in radar.fields and "TV" in radar.fields:
        vrefl_field, vrefl_field_raw = "DBZV", "TV"
    elif "DBZV" in radar.fields:
        vrefl_field = vrefl_field_raw = "DBZV"
    elif "TV" in radar.fields:
        vrefl_field = vrefl_field_raw = "TV"
    else:
        logger.warning("Campo de reflectividad vertical inexistente.")

    return {
        "hrefl_field": hrefl_field,
        "hrefl_field_raw": hrefl_field_raw,
        "vrefl_field": vrefl_field,
        "vrefl_field_raw": vrefl_field_raw,
    }
