import logging
import os
from copy import deepcopy
from typing import Optional

import numpy as np
import numpy.ma as ma
import pyart
from pyart.core import Radar

from radarlib.utils.names_utils import get_path_from_RMA_filename

logger = logging.getLogger(__name__)


class NetCDFError(Exception):
    pass


def normalize_fields_names(radar: Radar, idioma: int | bool | str = 1) -> Radar:
    # ==========================================================================
    # Renombramiento de las Variables Polarimétricas
    # ==========================================================================
    radar_norm = deepcopy(radar)

    if type(idioma) is bool or type(idioma) is int:
        if idioma == 0 or idioma is False:
            idioma = "ingles"
        elif idioma == 1 or idioma is True:
            idioma = "español"

    if idioma in ["español", "spanish"]:
        if "TH" in radar_norm.fields.keys():
            radar_norm.fields["TH"]["long_name"] = "Factor de Reflectividad Horizontal"
            radar_norm.fields["TH"]["standard_name"] = "Th"
            radar_norm.fields["TH"]["units"] = "[dBZ]"

        if "TV" in radar_norm.fields.keys():
            radar_norm.fields["TV"]["long_name"] = "Factor de Reflectividad Vertical"
            radar_norm.fields["TV"]["standard_name"] = "Tv"
            radar_norm.fields["TV"]["units"] = "[dBZ]"

        if "RHOHV" in radar_norm.fields.keys():
            radar_norm.fields["RHOHV"]["long_name"] = "Coeficiente de Correlacion Co-Polar"
            radar_norm.fields["RHOHV"]["standard_name"] = "RHOhv"
            radar_norm.fields["RHOHV"]["units"] = " "

        if "TDR" in radar_norm.fields.keys():
            radar_norm.fields["TDR"]["long_name"] = "Reflectividad Diferencial (o SNR nva vers INVAP)"
            radar_norm.fields["TDR"]["standard_name"] = "Tdr o SNR"
            radar_norm.fields["TDR"]["units"] = "[dBZ]"

        if "PHIDP" in radar_norm.fields.keys():
            radar_norm.fields["PHIDP"]["long_name"] = "Fase Diferencial"
            radar_norm.fields["PHIDP"]["standard_name"] = "PHIdp"
            radar_norm.fields["PHIDP"]["units"] = "[grados]"

        if "KDP" in radar_norm.fields.keys():
            radar_norm.fields["KDP"]["long_name"] = "Fase Diferencial Especifica"
            radar_norm.fields["KDP"]["standard_name"] = "Kdp"
            radar_norm.fields["KDP"]["units"] = "[grados/km]"

        if "CM" in radar_norm.fields.keys():
            radar_norm.fields["CM"]["long_name"] = "Mapa de Clutter"
            radar_norm.fields["CM"]["standard_name"] = "CM"
            radar_norm.fields["CM"]["units"] = " "

        if "WRAD" in radar_norm.fields.keys():
            radar_norm.fields["WRAD"]["long_name"] = "Ancho Espectral"
            radar_norm.fields["WRAD"]["standard_name"] = "Wrad"
            radar_norm.fields["WRAD"]["units"] = "[Hz]"

        if "VRAD" in radar_norm.fields.keys():
            radar_norm.fields["VRAD"]["long_name"] = "Velocidad Doppler Radial"
            radar_norm.fields["VRAD"]["standard_name"] = "Vrad"
            radar_norm.fields["VRAD"]["units"] = "[m/s]"

        if "DBZH" in radar_norm.fields.keys():
            radar_norm.fields["DBZH"]["long_name"] = "Factor de Reflectividad " + "Horizontal Filtrado"
            radar_norm.fields["DBZH"]["standard_name"] = "Zh"
            radar_norm.fields["DBZH"]["units"] = "[dBZ]"

        if "DBZV" in radar_norm.fields.keys():
            radar_norm.fields["DBZV"]["long_name"] = "Factor de Reflectividad " + "Vertical Filtrado"
            radar_norm.fields["DBZV"]["standard_name"] = "Zv"
            radar_norm.fields["DBZV"]["units"] = "[dBZ]"

        if "ZDR" in radar_norm.fields.keys():
            radar_norm.fields["ZDR"]["long_name"] = "Reflectividad Diferencial " + "Filtrada"
            radar_norm.fields["ZDR"]["standard_name"] = "Zdr"
            radar_norm.fields["ZDR"]["units"] = "[dBZ]"

        if "VRADV" in radar_norm.fields.keys():
            radar_norm.fields["VRADV"]["long_name"] = "SQI (nva vers INVAP)"
            radar_norm.fields["VRADV"]["standard_name"] = "SQI"
            radar_norm.fields["VRADV"]["units"] = ""

    elif idioma in ["ingles", "english"]:
        if "TH" in radar_norm.fields.keys():
            radar_norm.fields["TH"]["long_name"] = "Horizontal Reflectivity Factor"
            radar_norm.fields["TH"]["standard_name"] = "Th"
            radar_norm.fields["TH"]["units"] = "[dBZ]"

        if "TV" in radar_norm.fields.keys():
            radar_norm.fields["TV"]["long_name"] = "Vertical Reflectivity Factor"
            radar_norm.fields["TV"]["standard_name"] = "Tv"
            radar_norm.fields["TV"]["units"] = "[dBZ]"

        if "RHOHV" in radar_norm.fields.keys():
            radar_norm.fields["RHOHV"]["long_name"] = "Co-Polar Correlation " + "Coeficient"
            radar_norm.fields["RHOHV"]["standard_name"] = "RHOhv"
            radar_norm.fields["RHOHV"]["units"] = " "

        if "TDR" in radar_norm.fields.keys():
            radar_norm.fields["TDR"]["long_name"] = "Differential Reflectivity " + "(or SNR nva vers INVAP)"
            radar_norm.fields["TDR"]["standard_name"] = "Tdr or SNR"
            radar_norm.fields["TDR"]["units"] = "[dBZ]"

        if "PHIDP" in radar_norm.fields.keys():
            radar_norm.fields["PHIDP"]["long_name"] = "Differential Phase"
            radar_norm.fields["PHIDP"]["standard_name"] = "PHIdp"
            radar_norm.fields["PHIDP"]["units"] = "[degrees]"

        if "KDP" in radar_norm.fields.keys():
            radar_norm.fields["KDP"]["long_name"] = "Specific Differential Phase"
            radar_norm.fields["KDP"]["standard_name"] = "Kdp"
            radar_norm.fields["KDP"]["units"] = "[degrees/km]"

        if "CM" in radar_norm.fields.keys():
            radar_norm.fields["CM"]["long_name"] = "Clutter Map"
            radar_norm.fields["CM"]["standard_name"] = "CM"
            radar_norm.fields["CM"]["units"] = " "

        if "WRAD" in radar_norm.fields.keys():
            radar_norm.fields["WRAD"]["long_name"] = "Spectral Width"
            radar_norm.fields["WRAD"]["standard_name"] = "Wrad"
            radar_norm.fields["WRAD"]["units"] = "[Hz]"

        if "VRAD" in radar_norm.fields.keys():
            radar_norm.fields["VRAD"]["long_name"] = "Radial Doppler Velocity"
            radar_norm.fields["VRAD"]["standard_name"] = "Vrad"
            radar_norm.fields["VRAD"]["units"] = "[m/s]"

        if "DBZH" in radar_norm.fields.keys():
            radar_norm.fields["DBZH"]["long_name"] = "Horizontal Reflectivity " + "Factor Filtered"
            radar_norm.fields["DBZH"]["standard_name"] = "Zh"
            radar_norm.fields["DBZH"]["units"] = "[dBZ]"

        if "DBZV" in radar_norm.fields.keys():
            radar_norm.fields["DBZV"]["long_name"] = "Vertical Reflectivity " + "Factor Filtered"
            radar_norm.fields["DBZV"]["standard_name"] = "Zv"
            radar_norm.fields["DBZV"]["units"] = "[dBZ]"

        if "ZDR" in radar.fields.keys():
            radar_norm.fields["ZDR"]["long_name"] = "Differential Reflectivity " + "Filtered"
            radar_norm.fields["ZDR"]["standard_name"] = "Zdr"
            radar_norm.fields["ZDR"]["units"] = "[dBZ]"

        if "VRADV" in radar.fields.keys():
            radar_norm.fields["VRADV"]["long_name"] = "SQI (nva vers INVAP)"
            radar_norm.fields["VRADV"]["standard_name"] = "SQI"
            radar_norm.fields["VRADV"]["units"] = ""
    return radar_norm


def estandarizar_campos_RMA(radar: Radar, idioma: bool = True, replace_dbz_fields: bool = False):
    return normalize_RMA_fields(radar, idioma=idioma, replace_dbz_fields=replace_dbz_fields)


def normalize_RMA_fields(radar: Radar, idioma: bool | int | str = True, replace_dbz_fields=False):

    import gc

    from radarlib.utils.fields_utils import calcular_zdr

    # logger = logging.getLogger(logger_name)

    try:
        # ======================================================================
        # Renombramiento de Campo DBZH a TH ; DBZV a TV ; ZDR a TDR
        # ======================================================================
        if replace_dbz_fields:
            if "DBZH" in radar.fields.keys():
                # Agregamos el campo a los datos del radar.
                radar.add_field_like("DBZH", "TH", radar.fields["DBZH"]["data"], replace_existing=True)
                del radar.fields["DBZH"]

            if "DBZV" in radar.fields.keys():
                # Agregamos el campo a los datos del radar.
                radar.add_field_like("DBZV", "TV", radar.fields["DBZV"]["data"], replace_existing=True)
                del radar.fields["DBZV"]

            if "ZDR" in radar.fields.keys():
                # Agregamos el campo a los datos del radar.
                radar.add_field_like("ZDR", "TDR", radar.fields["ZDR"]["data"], replace_existing=True)
                del radar.fields["ZDR"]

        # ---------------------------------------------------------------------
        # Generación de Variables Polarimétricas No Incluidas:
        # ---------------------------------------------------------------------
        # Genera ZDR (en caso de no existir)
        if "TDR" not in radar.fields.keys() and "TH" in radar.fields.keys() and "TV" in radar.fields.keys():
            radar = calcular_zdr(radar, ref_vertical="TV", ref_horizontal="TH", zdr_out_field="TDR")
            logger.debug("TDR Inexistente en Volumen: se ha generado.")

        # Genera ZDR (en caso de no existir)
        if "ZDR" not in radar.fields.keys() and "DBZH" in radar.fields.keys() and "DBZV" in radar.fields.keys():
            radar = calcular_zdr(radar, ref_vertical="DBZV", ref_horizontal="DBZH", zdr_out_field="ZDR")
            logger.debug("ZDR Inexistente en Volumen: se ha generado.")

        # =====================================================================
        # Enmascaramiento de los Datos
        # El conversor de PyART a NETCDF tiene un error al grabar: se modifica
        # el tipo de arrays enmascarados a arrays simples.
        # =====================================================================
        for field in radar.fields.keys():
            if (
                type(radar.fields[field]["data"]) is not np.ma.core.MaskedArray
                or type(radar.fields[field]["data"].mask) is not np.ndarray
            ):
                # Enmascara solo los datos inválidos (NaN, INF, etc)
                radar.fields[field]["data"] = ma.masked_invalid(radar.fields[field]["data"])
                radar.fields[field]["data"] = ma.masked_outside(radar.fields[field]["data"], -100000, 100000)

        # Force garbage collection to free intermediate masking arrays
        gc.collect()

        # =====================================================================
        # Normalización de Nombres
        # =====================================================================
        radar = normalize_fields_names(radar, idioma)
        return radar

    except Exception as e:
        raise Exception(f"Error normalizing RMA fields: {e}") from e


def read_radar_netcdf(
    netcdf_fname: str,
    extract_sweeps: bool = False,
    sweep: Optional[int] = None,
    normalize_names: bool = True,
    idioma: int | str | bool = "español",
    # debug: bool = False,
    # logger_name: str = __name__,
):
    """
    Lee un volumen radar en formato NetCDF (CFRadial).
    Parámetros:
    - netcdf_fname: Ruta al archivo NetCDF.
    - extract_sweeps: Si es True, extrae solo las sweeps indicadas
    - sweep: Índice de la sweep a extraer (si extract_sweeps es True).
    - normalize_names: Si es True, normaliza los nombres de los campos.
    - idioma: Idioma para la normalización de nombres ('español' o 'ingles').
    - debug: Si es True, habilita mensajes de depuración.
    - logger_name: Nombre del logger a utilizar.
    Retorna:
    - radar: Objeto Radar cargado desde el archivo NetCDF.
    """

    if os.path.isfile(netcdf_fname):
        try:
            radar = pyart.io.read(netcdf_fname)
            logger.debug(f"Cargado NETCDF desde disco: {netcdf_fname}")
        except Exception as e:
            raise NetCDFError(f"Error reading NetCDF file {netcdf_fname}: {e}")

        if extract_sweeps and sweep is not None:
            logger.debug("Recortamos vol del NETCDF")
            logger.debug(f"ext_swps: {str(extract_sweeps)} sw: {str(sweep)}")
            radar = radar.extract_sweeps([sweep])
        if normalize_names:
            normalize_RMA_fields(radar, idioma=idioma)
        return radar
    else:
        raise NetCDFError(f"NetCDF file {netcdf_fname} does not exist.")


def save_radar_netcdf(radar, filename_out=None, path_out=None, **kwargs):
    """Guarda un objeto Py-ART Radar en formato NetCDF (CFRadial).
    Parámetros:
    - radar: Objeto Radar a guardar.
    - filename_out: Nombre del archivo de salida (opcional).
    - path_out: Ruta del directorio de salida (opcional).
    - logger_name: Nombre del logger a utilizar.
    Retorna:
    - fullname: Ruta completa del archivo guardado.
    """
    # logger = logging.getLogger(logger_name)

    if filename_out is None:
        filename_out = radar.metadata["filename"]
    if not filename_out.endswith(".nc"):
        filename_out = filename_out + ".nc"

    if path_out is None or path_out == "bbdd":
        path_out = get_path_from_RMA_filename(filename=filename_out, **kwargs)
    os.makedirs(path_out, exist_ok=True)

    try:
        fullname = os.path.join(path_out, filename_out)
        pyart.io.cfradial.write_cfradial(fullname, radar)
        return fullname
    except Exception as e:
        logger.error("Error al guardar NetCDF: " + str(e))
        raise
