#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: jsaffe
"""
import logging
import os

import numpy as np

# import grc.global_parameters as cf
from radarlib import config

logger = logging.getLogger(__name__)


def get_vertical_vinculation_gate_map(
    radar,
    # logger_name=__name__,
    use_sweeps_above: float = 0,
    altitude_threshold=20000,
    save_vvg_map=True,
    root_cache=None,
    # verbose=False,
    regenerate_flag=False,
):
    """
    La función genera y devuelve el campo de vinculación vertical de celdas
    entre barridos.

    Parámetros
    ----------
    radar : Radar
        Radar object

    logger_name : str
        Nombre del logger utilizado para registrar los datos de salida.

    use_sweeps_above : int
        Elevación de antena a partir de la cual se empiezan a utilizar los
        sweeps en el proceamiento de vinculación vertical de celdas.

    altitude_threshold : int
        Umbral de altitud hasta el cual se limita el procesamiento de
        vinculación vertical de celas.

    save_vvg_map : Boolean
        Guarda el campo vvg map calculado en un archivo utilizando la ruta
        'root_cache'.

    root_cache : str or None
        Ruta a la carpeta cache donde se almacenarán los archivos de
        vinculación de los escaneos híbridos. None utiliza la ruta
        por defecto cargada en el archivo de configuración general.

    verbose : boolean, optional
        Habilita/Inhabilita los logs de nivel "INFO".

    regenerate_flag : Boolean
        Activa / Desactiva la regenarión del mapa de escaneo híbrido.
        Si existe el archivo lo reescribe.


    Returns
    -------
    vvh_map : masked array
        Campo de vinculación vertical de celdas entre barridos.


    """

    # ==========================================================================
    # Inicializamos Variables
    # ==========================================================================
    # logger = logging.getLogger(logger_name)

    if root_cache is None:
        root_cache = config.ROOT_CACHE_PATH

    root_cache_vvgmap = os.path.join(root_cache, "VVG_Map/")
    if not os.path.isdir(root_cache_vvgmap):
        os.makedirs(root_cache_vvgmap)

    sw_rays = int(radar.nrays / radar.nsweeps)

    # =========================================================================
    # buscamos vvg_map en cache
    # =========================================================================
    # generamos nombre del archivo a utilizar en la lectura/escritura:
    # estos incluyen el nombre del radar, estrategia de escaneo,
    # número de volumen, y las elevaciones utilizadas en el escaneo
    elevations = ""
    for sweep in range(radar.nsweeps):
        elevations += format(str(radar.get_elevation(sweep)[0]), ".5") + "_"

    vvgm_filename = (
        root_cache_vvgmap
        + "vvgm_of_"
        + radar.metadata["instrument_name"]
        + "_"
        + radar.metadata["scan_id"]
        + "_"
        + radar.metadata["filename"].split("_")[2]
        + "_["
        + elevations
        + "]_"
        + "[min_sw_el"
        + str(use_sweeps_above)
        + "]"
    )

    # buscamos en cache campo hscan
    if os.path.isfile(vvgm_filename + ".npy") and not regenerate_flag:

        # leyendo hscan_map_data
        vvg_map = np.load(vvgm_filename + ".npy")
        logger.debug("Leyendo vvgm de archivo ...")

        vvg_map = np.ma.masked_invalid(vvg_map)
        vvg_map = np.ma.masked_equal(vvg_map, vvg_map.fill_value)

        return vvg_map

    # =========================================================================
    # calculamos vvg_map
    # archivo no encontrado en cache, lo generamos
    # =========================================================================

    # =========================================================================
    # Orden de Sweeps y Sweep de Referencia
    # =========================================================================
    # Se ordenan los sweeps, descartando los sw inferiores a use_sweeps_above y
    # se determina el primer sweep ubicado por arriba de dicho umbral. Este sw
    # se establece como referencia (sw_ref) para el calculo de las
    # vinculaciones verticales sobre los otros sweeps. Los barridos inferiores
    # no son tenidos en cuenta, simplemente son ignorados en armado del VVG
    # map.
    [sw_tuples_az, sw_ref] = get_ordered_sweep_list(radar, use_sweeps_above)

    if not len(sw_tuples_az) > 1:
        raise ValueError("No hay barridos suficientes (2 o más) con el limite" + " de elevacion establecido.")

    # =========================================================================
    # Vinculación vertical entre gates
    # Se utiliza solo un rayo, la vinculación es la misma para todos.
    # =========================================================================
    # vvg_map [ngates x nsweeps]
    # La matriz devuelve para los ngates del barrido de referencia el índice
    # de gate a utilizar en los restantes barrido. Ej: vvg_map[50,11] = 58
    # indica que la celda 50 del barrido de referencia (el barrido más bajo
    # por arriba del umbral especificado) está relacionada con la celda 58 del
    # barrido número 11. Los barridos no utilizados simplemente son rellandos
    # con NaN.
    vvg_map = np.ma.array(np.zeros((radar.ngates, radar.nsweeps)) * np.nan, mask=False)

    ray_ref = sw_rays * sw_ref  # rayos por barrido

    # distancia entre celdas en grados de latitud
    # si bien en función de la proyección utilizada esta distancia varía
    # a medida que nos desplazamaos en rango, esta variación es cercana a
    # a 2x10^(-3) en latitud o equivalentemente del orden de 0.3m.
    lat_distance = np.abs(radar.gate_latitude["data"][ray_ref, 0] - radar.gate_latitude["data"][ray_ref, 1])
    lat_distance = lat_distance / 2

    for gate_ref in range(radar.ngates):
        # calculamos la latitud del borde inferior del gate de referencia
        # los gates de las elevaciones superiores se vinculan con la celda de
        # referencia si las latitudes de sus centros superan la latitud del
        # borde inferior. Se calcula solo para el rayo 0º (dirección norte), ya
        # que las vinculaciones son simétricas para el resto de los haces.
        lat1 = radar.gate_latitude["data"][ray_ref, gate_ref] - lat_distance

        last_gate = gate_ref  # variable para optimizar el calculo.
        for [_elev, sweep] in sw_tuples_az:
            for gate in range(last_gate, radar.ngates):

                # Limita el calculo a una altura de altitude_threshold
                if radar.gate_altitude["data"][sw_rays * sweep, gate] > altitude_threshold:
                    last_gate = gate
                    break

                # Chequeamos si el centro de la celda de prueba esta dentro del
                # cuadrado de la celda de referencia. Un chequeo exacto de si
                # determinada celda (o una parte de esta) estan dentro de otra
                # demandaria mucho tiempo de procesamiento ya que habria que
                # armar un poligono de referencia (con n puntos) para la celda
                # bajo test y un poligo para cada celda a comparar.
                if radar.gate_latitude["data"][sw_rays * sweep, gate] > lat1:
                    vvg_map[gate_ref, sweep] = gate
                    last_gate = gate
                    break

    # =========================================================================
    #  Guardamos vvg_map en cache
    # =========================================================================
    if save_vvg_map:
        # si el archivo no existe
        if not os.path.isfile(vvgm_filename + ".npy") or regenerate_flag:
            np.save(vvgm_filename, vvg_map.filled())
            logger.debug("vvg_map guardado en: " + vvgm_filename)

    # enmascaramos datos invalidos (np.nan)
    vvg_map = np.ma.masked_invalid(vvg_map)

    return vvg_map


def get_ordered_sweep_list(radar, use_sweeps_above: float = -5, sweeps_to_use=None):
    """
    Función devuelve lista ordenada de barridos (en tuplas) y sw de refencia.
    Las tuplas tienen el siguiente formato:
    (ángulo_de_elevacición,número_de_sweep_dentro_del_VCP).


    Parámetros
    ----------
    radar : Radar
        Radar object

    use_sweeps_above : float
        Las elevaciones por debajo de dicha elevación se descartan.

    Returns
    ----------
    sw_tuples_za : list of tuples
        Lista ordenada de tuplas con formato (elevetion_angle/sweep_number)

    sw_ref : int
        Número de sweep del primer barrido encontrado que supera el umbral
        "use_sweeps_above".

    sweeps_to_use : vector of int or None
        Lista con los índices de los sweeps a utilizar. Si es None se utilizan
        todos los sweeps disponibles en el objeto radar.
    """
    if sweeps_to_use is None:
        sweeps_to_use = np.arange(radar.nsweeps)

    sw_ref = None

    # lista de tuplas (elevation_angle,sw_number) ordenados por
    # elevacion de menor a mayor (az)
    sw_tuples_az = []
    for sweep in sweeps_to_use:
        el = radar.get_elevation(sweep=sweep)[0]
        sw_tuples_az.append([float(el), int(sweep)])
        sw_tuples_az.sort()

    # lista de tuplas ordenadas de mayor a menor (za)
    sw_tuples_za = sw_tuples_az.copy()
    sw_tuples_za.reverse()

    for [elev, sweep] in sw_tuples_az:
        if elev >= use_sweeps_above:
            sw_ref = sweep  # primer sw que supera el limite de altura fijado
            break
        else:
            sw_tuples_za.pop()  # elimina el ultimo elemento del vector

    sw_tuples_za.reverse()  # reordenamos las tuplas de menor
    # a mayor sin los sw descartados.

    if sw_ref is None:
        raise ValueError("No se encontraron barridos superiores al " + "limite fijado.")

    return sw_tuples_za, sw_ref
