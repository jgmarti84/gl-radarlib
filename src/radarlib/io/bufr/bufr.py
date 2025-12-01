import logging
import os
import tempfile
import time
import zlib
from contextlib import contextmanager
from ctypes import CDLL, POINTER, Structure, c_char_p, c_double, c_int, cdll
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from radarlib import config

logger = logging.getLogger(__name__)


class SweepConsistencyException(Exception):
    pass


class point_t(Structure):
    _fields_ = [("lat", c_double), ("lon", c_double)]


class meta_t(Structure):
    _fields_ = [
        ("year", c_int),
        ("month", c_int),
        ("day", c_int),
        ("hour", c_int),
        ("min", c_int),
        ("radar", point_t),
        ("radar_height", c_double),
    ]


@contextmanager
def decbufr_library_context(root_resources: str | None = None):
    """
    Context manager para cargar la librería C de decodificación BUFR.

    Args:
        root_resources: Ruta al directorio de recursos que contiene la
            librería dinámica.

    Yields:
        El objeto CDLL cargado.
    """
    if root_resources is None:
        root_resources = config.BUFR_RESOURCES_PATH
    lib = load_decbufr_library(root_resources)
    try:
        yield lib
    finally:
        # No explicit unload in ctypes, but could add cleanup if needed
        pass


@contextmanager
def safe_c_call():
    """
    Context manager that redirects C library stderr to capture error messages
    without terminating the Python process.

    This helps catch C library errors that would otherwise crash Python.

    Yields:
        A tuple of (stderr_file, temp_file) for logging purposes.
    """
    # Save original stderr file descriptor
    original_stderr = os.dup(2)

    try:
        # Create temporary file to capture stderr
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".err") as tmp:
            temp_file_path = tmp.name

        # Redirect stderr to temporary file
        stderr_file = open(temp_file_path, "w")
        os.dup2(stderr_file.fileno(), 2)

        yield stderr_file, temp_file_path

    finally:
        # Restore original stderr
        os.dup2(original_stderr, 2)
        os.close(original_stderr)

        # Close and clean up temp file
        try:
            stderr_file.close()
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        except Exception:
            pass


def bufr_name_metadata(bufr_filename: str) -> dict:
    """
    Extrae información estructural del nombre de archivo BUFR. Se espera el
    patrón: <RADAR>_<ESTRATEGIA>_<NVOL>_<TIPO>_<TIMESTAMP>.BUFR

    Args:
        bufr_filename: Nombre del archivo BUFR (puede incluir ruta).

    Returns:
        Diccionario con claves: 'radar_name', 'estrategia_nombre',
        'estrategia_nvol', 'tipo_producto' y 'filename'.

    Raises:
        ValueError: Si el nombre no cumple el patrón esperado.
    """
    filename = bufr_filename.split("/")[-1]
    base_name = filename.split(".")[0]
    parts = base_name.split("_")
    if len(parts) != 5:
        raise ValueError(f"Unexpected BUFR filename format: {bufr_filename}")

    return {
        "radar_name": parts[0],
        "estrategia_nombre": parts[1],
        "estrategia_nvol": parts[2],
        "tipo_producto": parts[3],
        "filename": filename,
    }


def load_decbufr_library(root_resources: str) -> CDLL:
    """
    Carga la librería dinámica compartida (libdecbufr.so) desde el directorio
    de recursos especificado.

    Args:
        root_resources: Ruta al directorio que contiene
            'dynamic_library/libdecbufr.so'.

    Returns:
        Un objeto CDLL cargado que expone las funciones de la librería C.
    """
    if root_resources is None:
        root_resources = config.BUFR_RESOURCES_PATH
    lib_path = os.path.join(root_resources, "dynamic_library/libdecbufr.so")
    return cdll.LoadLibrary(lib_path)


def get_metadata(lib: CDLL, bufr_path: str, root_resources: str | None = None) -> Dict[str, Any]:
    """
    Extrae metadatos básicos del archivo BUFR mediante la función
    `get_meta_data` de la librería C cargada.

    Args:
        lib: Objeto CDLL de la librería ya cargada.
        bufr_path: Ruta al archivo BUFR.
        root_resources: Ruta al directorio que contiene 'bufr_tables'.

    Returns:
        Diccionario con claves: 'year', 'month', 'day', 'hour', 'min',
        'lat', 'lon' y 'radar_height'.

    Raises:
        RuntimeError: Si el archivo no existe o la función C falla.
    """
    if root_resources is None:
        root_resources = config.BUFR_RESOURCES_PATH

    # Validate input file exists
    if not os.path.exists(bufr_path):
        raise FileNotFoundError(f"BUFR file not found: {bufr_path}")

    get_meta_data = lib.get_meta_data
    get_meta_data.argtypes = [c_char_p, c_char_p]
    get_meta_data.restype = POINTER(meta_t)

    tables_path = os.path.join(root_resources, "bufr_tables")

    # Validate tables path exists
    if not os.path.exists(tables_path):
        raise FileNotFoundError(f"BUFR tables directory not found: {tables_path}")

    try:
        with safe_c_call() as (stderr_file, temp_file_path):
            metadata = get_meta_data(bufr_path.encode("utf-8"), tables_path.encode("utf-8"))

        # Check if stderr captured any error messages
        try:
            with open(temp_file_path, "r") as f:
                stderr_content = f.read().strip()
                if stderr_content:
                    raise RuntimeError(f"C library error: {stderr_content}")
        except FileNotFoundError:
            pass

        # Validate returned pointer is not null
        if metadata is None:
            raise RuntimeError(f"get_meta_data returned NULL for {bufr_path}")

        # Validate metadata contents
        if metadata.contents.year < 1900 or metadata.contents.year > 2100:
            raise ValueError(f"Invalid year from BUFR file: {metadata.contents.year}")

        return {
            "year": metadata.contents.year,
            "month": metadata.contents.month,
            "day": metadata.contents.day,
            "hour": metadata.contents.hour,
            "min": metadata.contents.min,
            "lat": metadata.contents.radar.lat,
            "lon": metadata.contents.radar.lon,
            "radar_height": metadata.contents.radar_height,
        }
    except Exception as e:
        raise RuntimeError(f"C library error in get_meta_data: {e}") from e


def get_elevations(lib: CDLL, bufr_path: str, max_elev: int = 30, root_resources: str | None = None) -> np.ndarray:
    """
    Recupera las elevaciones de los barridos (fixed angles) desde la librería C.

    Args:
        lib: Objeto CDLL de la librería cargada.
        bufr_path: Ruta al archivo BUFR.
        root_resources: Ruta al directorio que contiene 'bufr_tables'.
        max_elev: Máximo número de elevaciones esperado (por defecto 30).

    Returns:
        Un numpy.ndarray con las elevaciones (float).

    Raises:
        RuntimeError: Si la función C falla.
        ValueError: Si los valores de elevación son inválidos.
    """
    if root_resources is None:
        root_resources = config.BUFR_RESOURCES_PATH

    get_elevation_data = lib.get_elevation_data
    get_elevation_data.argtypes = [c_char_p, c_char_p]
    array_shape = c_double * max_elev
    get_elevation_data.restype = POINTER(array_shape)
    tables_path = os.path.join(root_resources, "bufr_tables")

    try:
        arr = get_elevation_data(bufr_path.encode("utf-8"), tables_path.encode("utf-8"))

        if arr is None:
            raise RuntimeError(f"get_elevation_data returned NULL for {bufr_path}")

        result = np.asarray(list(arr.contents))

        # Validate elevations are reasonable (between -1 and 90 degrees)
        valid_elevs = result[result > 0]  # Filter out zeros/invalid values
        if len(valid_elevs) == 0:
            raise ValueError("No valid elevations found in BUFR file")

        if np.any((valid_elevs < -1) | (valid_elevs > 90)):
            raise ValueError(f"Invalid elevation values: {valid_elevs[valid_elevs < -1 or valid_elevs > 90]}")

        return result
    except Exception as e:
        raise RuntimeError(f"C library error in get_elevation_data: {e}") from e


def get_raw_volume(lib: CDLL, bufr_path: str, size: int, root_resources: str | None = None) -> np.ndarray:
    """
    Recupera el bloque de datos crudo (array de enteros) del archivo BUFR
    llamando a la función C correspondiente.

    Args:
        lib: Objeto CDLL de la librería cargada.
        bufr_path: Ruta al archivo BUFR.
        root_resources: Ruta al directorio que contiene 'bufr_tables'.
        size: Tamaño esperado del bloque de datos (número de enteros).

    Returns:
        numpy.ndarray de enteros con el volumen crudo.

    Raises:
        RuntimeError: Si la función C falla.
        ValueError: Si el tamaño es inválido.
    """
    if root_resources is None:
        root_resources = config.BUFR_RESOURCES_PATH

    if size <= 0:
        raise ValueError(f"Invalid size: {size}")

    get_data = lib.get_data
    get_data.argtypes = [c_char_p, c_char_p]
    array_shape = c_int * size
    get_data.restype = POINTER(array_shape)
    tables_path = os.path.join(root_resources, "bufr_tables")

    try:
        raw = get_data(bufr_path.encode("utf-8"), tables_path.encode("utf-8"))

        if raw is None:
            raise RuntimeError(f"get_data returned NULL for {bufr_path}")

        result = np.asarray(list(raw.contents))

        # Validate we got the expected size
        if len(result) != size:
            raise ValueError(f"Data size mismatch: expected {size}, got {len(result)}")

        return result
    except Exception as e:
        raise RuntimeError(f"C library error in get_raw_volume: {e}") from e


def get_size_data(lib: CDLL, bufr_path: str, root_resources: str | None = None) -> int:
    """
    Llama a la función C que devuelve el tamaño del bloque de datos
    bruto del archivo BUFR.

    Args:
        lib: Objeto CDLL de la librería cargada.
        bufr_path: Ruta al archivo BUFR.
        root_resources: Ruta al directorio que contiene 'bufr_tables'.

    Returns:
        Entero con el tamaño (número de elementos) del volumen codificado.

    Raises:
        RuntimeError: Si la función C falla o retorna un valor inválido.
    """
    if root_resources is None:
        root_resources = config.BUFR_RESOURCES_PATH

    get_size_data = lib.get_size_data
    get_size_data.argtypes = [c_char_p, c_char_p]
    get_size_data.restype = c_int
    tables_path = os.path.join(root_resources, "bufr_tables")

    try:
        size = get_size_data(bufr_path.encode("utf-8"), tables_path.encode("utf-8"))

        if size <= 0:
            raise ValueError(f"Invalid data size: {size}")

        # Sanity check: size should be reasonable (< 100MB = 26M ints)
        if size > 26_000_000:
            raise ValueError(f"Data size too large: {size} elements")

        return size
    except Exception as e:
        raise RuntimeError(f"C library error in get_size_data: {e}") from e


def parse_sweeps(vol: np.ndarray, nsweeps: int, elevs: np.ndarray) -> list[dict]:
    """
    Parsea el buffer de enteros devuelto por la librería C y extrae una
    lista de barridos (sweeps). Cada barrido es un diccionario que contiene
    metadatos y los chunks comprimidos.

    Args:
        vol: Array 1-D de enteros tal como lo devuelve la librería C.
        nsweeps: Número de barridos esperados en el volumen.
        elevs: Array de elevaciones disponibles.

    Returns:
        Lista de diccionarios con claves como 'year', 'ngates', 'nrays',
        'compress_data', etc.
    """
    sweeps = []
    u = 1
    for sweep_idx in range(nsweeps):
        # parse fixed header block (13–15 ints)
        (
            year_ini,
            month_ini,
            day_ini,
            hour_ini,
            min_ini,
            sec_ini,
            year,
            month,
            day,
            hour,
            minute,
            sec,
            product_type,
        ) = vol[u : u + 13]
        u += 13

        elevation = elevs[sweep_idx] if sweep_idx < len(elevs) else None
        u += 1
        ngates, range_size, range_offset, nrays, azimuth = vol[u : u + 5]
        u += 5

        # skip optional duplicated type/product etc.
        # Example from legacy: u = u+2
        u += 3

        multi_pri = vol[u]
        u += 1
        comp_chunks = []
        for _ in range(multi_pri):
            multi_sec = vol[u]
            u += 1
            data_chunk = vol[u : u + multi_sec]
            u += multi_sec
            # vectorized replace
            data_chunk = np.where(data_chunk == 99999, 255, data_chunk)
            comp_chunks.append(data_chunk)
        compress_data = bytearray(np.concatenate(comp_chunks).astype(np.uint8))

        sweeps.append(
            {
                "year_ini": year_ini,
                "month_ini": month_ini,
                "day_ini": day_ini,
                "hour_ini": hour_ini,
                "min_ini": min_ini,
                "sec_ini": sec_ini,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "min": minute,
                "sec": sec,
                "product_type": product_type,
                "elevation": elevation,
                "ngates": ngates,
                "range_size": range_size,
                "range_offset": range_offset,
                "nrays": nrays,
                "antenna_beam_az": azimuth,
                "compress_data": compress_data,
            }
        )
    return sweeps


def decompress_sweep(sweep: dict) -> np.ndarray:
    """
    Descomprime y reconstruye los datos de un solo barrido.

    Args:
        sweep: Diccionario con los metadatos del barrido y el campo
            'compress_data' con los bytes comprimidos.

    Returns:
        numpy.ndarray 2-D de tipo float64 con forma (nrays, ngates).

    Raises:
        SweepConsistencyException: Si 'ngates' excede el límite razonable.
        ValueError: Si el número de elementos descomprimidos no coincide
            con nrays*ngates.
    """
    # Descartamos barridos con ngates > 8400
    if sweep["ngates"] > 8400:
        raise SweepConsistencyException(f"Barrido con ngates > 8400: {sweep['ngates']}")

    dec_data = zlib.decompress(memoryview(sweep["compress_data"]))
    arr = np.frombuffer(dec_data, dtype=np.float64)

    # Enmascarado de valores faltantes
    arr = np.ma.masked_equal(arr, -1.797693134862315708e308)

    # Reordenar a 2D (nrays, ngates)
    expected = sweep["nrays"] * sweep["ngates"]
    if arr.size != expected:
        raise ValueError(f"Data de barrido inconsistente: obtenido {arr.size}, esperado {expected}")

    return arr.reshape((sweep["nrays"], sweep["ngates"]))


def uniformize_sweeps(sweeps: list[dict]) -> list[dict]:
    """
    Normaliza todos los barridos para que compartan el mismo número de
    gates (ngates). Si algún barrido tiene menos gates, se rellena con NaN
    hasta igualar el máximo.

    Args:
        sweeps: Lista de diccionarios de barridos ya descomprimidos, cada
            uno con 'data' como ndarray 2-D.

    Returns:
        La lista modificada de barridos con 'data' ampliada cuando fue
        necesario y 'ngates' actualizado.
    """
    max_gates = max(sweep["data"].shape[1] for sweep in sweeps)
    for sw in sweeps:
        nr, ng = sw["data"].shape
        if ng < max_gates:
            pad = np.full((nr, max_gates), np.nan, dtype=np.float64)
            pad[:, :ng] = sw["data"]
            sw["data"] = pad
            sw["ngates"] = max_gates
    return sweeps


def assemble_volume(sweeps: list[dict]) -> np.ndarray:
    """
    Concatena verticalmente (vstack) los arrays de cada barrido para formar
    el volumen final de forma (total_rays, ngates).

    Args:
        sweeps: Lista de diccionarios con clave 'data' conteniendo arrays 2-D.

    Returns:
        numpy.ndarray 2-D (float64) con la concatenación de todos los barridos.
    """
    return np.vstack([sw["data"] for sw in sweeps])


def validate_sweeps_df(sweeps_df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida consistencia básica entre los barridos del volumen usando un DataFrame:
    - mismo número de rayos por barrido (nrayos)
    - mismo gate_size
    - gate_offset dentro de tolerancia
    Args:
        sweeps_df: DataFrame con la información del volumen y listas por sweep.
    Raises:
        AssertionError: Si se detecta inconsistencia que hace el volumen no soportado.
    """
    assert sweeps_df["nrayos"].nunique() == 1, "Número de rayos inconsistente entre sweeps"
    assert sweeps_df["gate_size"].nunique() == 1, "Gate size inconsistente entre sweeps"
    max_offset = sweeps_df["gate_offset"].iloc[0] // 2
    assert all(
        abs(sweeps_df["gate_offset"] - sweeps_df["gate_offset"].iloc[0]) <= max_offset
    ), "Desplazamiento excesivo en gate_offset entre sweeps"

    return sweeps_df


def build_metadata(filename: str, info: dict) -> dict:
    """
    Construye un diccionario de metadatos estandarizados a partir del nombre
    de archivo y la estructura 'info' utilizada por el resto del pipeline.

    Args:
        filename: Nombre del archivo BUFR original (sin ruta).
        info: Diccionario con información del volumen y de los barridos.

    Returns:
        Diccionario con campos de metadatos listos para incluir en 'info'.
    """
    dia_sweep = int(info["sweeps"]["dia_sweep"].iloc[0])
    mes_sweep = int(info["sweeps"]["mes_sweep"].iloc[0])
    ano_sweep = int(info["sweeps"]["ano_sweep"].iloc[0])
    hora_sweep = int(info["sweeps"]["hora_sweep"].iloc[0])
    min_sweep = int(info["sweeps"]["min_sweep"].iloc[0])
    seg_sweep = int(info["sweeps"]["seg_sweep"].iloc[0])
    return {
        "comment": "-",
        "instrument_type": "Radar",
        "site_name": "-",
        "Sub_conventions": "-",
        "references": "-",
        "volume_number": info["estrategia"]["volume_number"],
        "scan_id": info["estrategia"]["nombre"],
        "title": "-",
        "source": "-",
        "version": "-",
        "instrument_name": info["nombre_radar"],
        "ray_times_increase": "-",
        "platform_is_mobile": "false",
        "driver": "-",
        "institution": "SiNaRaMe",
        "n_gates_vary": "-",
        "primary_axis": "-",
        "created": (
            f"Fecha:{dia_sweep}/" f"{mes_sweep}/" f"{ano_sweep} " f"Hora:{hora_sweep}:" f"{min_sweep}:" f"{seg_sweep}"
        ),
        "scan_name": "-",
        "author": "Grupo Radar Cordoba (GRC) - Extractor/Conversor de Datos de Radar ",
        "Conventions": "-",
        "platform_type": "Base Fija",
        "history": "-",
        "filename": (
            filename.split("_")[0]
            + "_"
            + filename.split("_")[1]
            + "_"
            + filename.split("_")[2]
            + "_"
            + filename.split("_")[4].split(".")[0]
            + ".nc"
        ),
    }


def build_info_dict(meta_vol: dict, meta_sweeps: list[dict]) -> dict:
    """
    Ensambla el diccionario 'info' que combina metadatos del volumen
    y de cada barrido. Devuelve la estructura utilizada por el resto
    del pipeline (validación, creación de Radar, etc.).

    Args:
        meta_vol: Metadatos generales del volumen tal como los devuelve la C API.
        meta_sweeps: Lista de diccionarios con metadatos por barrido.

    Returns:
        Diccionario 'info' con listas por campo para cada sweep y claves
        _tmp_df = pd.DataFrame.from_dict(meta_sweeps)
        sweeps_df = _tmp_df.drop(columns=drop_cols)  # type: ignore
    """
    nsweeps = meta_vol["nsweeps"]
    # =====================================================================
    # INFO VOLUMEN
    # =====================================================================
    info = {
        # .....................................................................
        # Info de Instrumento y Estrategia
        # .....................................................................
        "nombre_radar": meta_vol["radar_name"],
        "estrategia": {
            "nombre": meta_vol["estrategia_nombre"],
            "volume_number": meta_vol["estrategia_nvol"],
        },
        "tipo_producto": meta_vol["tipo_producto"],
        "filename": meta_vol["filename"],
        # .....................................................................
        # Carga de Info General del Volumen
        # .....................................................................
        "ano_vol": meta_vol["year"],
        "mes_vol": meta_vol["month"],
        "dia_vol": meta_vol["day"],
        "hora_vol": meta_vol["hour"],
        "min_vol": meta_vol["min"],
        "lat": meta_vol["lat"],
        "lon": meta_vol["lon"],
        "altura": meta_vol["radar_height"],
        "nsweeps": nsweeps,
    }

    # .....................................................................
    # Carga de Info de Barridos
    # .....................................................................
    drop_cols = ["data", "compress_data", "product_type"]
    sweeps_df = pd.DataFrame.from_dict(meta_sweeps).drop(columns=drop_cols)  # type: ignore
    sweeps_df = sweeps_df.rename(
        columns={
            "year_ini": "ano_sweep_ini",
            "year": "ano_sweep",
            "month_ini": "mes_sweep_ini",
            "month": "mes_sweep",
            "day_ini": "dia_sweep_ini",
            "day": "dia_sweep",
            "hour_ini": "hora_sweep_ini",
            "hour": "hora_sweep",
            "min_ini": "min_sweep_ini",
            "min": "min_sweep",
            "sec_ini": "seg_sweep_ini",
            "sec": "seg_sweep",
            "elevation": "elevaciones",
            "ngates": "ngates",
            "range_size": "gate_size",
            "range_offset": "gate_offset",
            "nrays": "nrayos",
            "antenna_beam_az": "rayo_inicial",
        }
    )

    # Validamos consistencia básica entre los barridos del volumen
    info["sweeps"] = validate_sweeps_df(sweeps_df)

    # Agrega metadatos estandarizados
    info["metadata"] = build_metadata(meta_vol["filename"], info)

    return info


def dec_bufr_file(
    bufr_filename: str,
    root_resources: str | None = None,
    # logger_name: Optional[str] = None,
    parallel: bool = True,
) -> Tuple[Dict[str, Any], List[dict], np.ndarray, List[List[Any]]]:
    """
    Decodifica un archivo BUFR usando la librería C y reconstruye:
    - metadatos del volumen (meta_vol),
    - una lista de barridos (meta_sweeps) con sus datos descomprimidos,
    - el volumen completo concatenado (vol_data) y
    - un registro de ejecución (run_log) con advertencias/errores.

    Args:
        bufr_filename: Ruta al archivo BUFR a procesar.
        root_resources: Ruta al directorio de recursos (tablas y librería C).
        logger_name: Nombre base para el logger.
        parallel: Si True, usa ThreadPoolExecutor para descomprimir sweeps
            en paralelo (por barrido). Si False, procesa en serie.

    Returns:
        Tupla (meta_vol, sweeps, vol_data, run_log):
                _tmp_df = pd.DataFrame.from_dict(meta_sweeps)
                sweeps_df = _tmp_df.drop(columns=drop_cols)  # type: ignore
                sweeps_df = _tmp_df.drop(columns=drop_cols)  # type: ignore
            - sweeps: lista de diccionarios por barrido (con 'data')
            - vol_data: ndarray 2-D con los datos concatenados
            - run_log: lista con entradas de log (niveles/mensajes)

    Nota:
        No se modifica la lógica de validación existente; las excepciones
        se registran en el logger y se propagan cuando corresponda.
    """
    # filename = bufr_filename.split("/")[-1]
    # logger = logging.getLogger((logger_name or __name__) + "." + filename.split("_")[0])

    run_log = []

    try:
        with decbufr_library_context(root_resources) as lib:
            # Extrae metadatos generales del volumen
            vol_metadata = get_metadata(lib, bufr_filename, root_resources)

            # Extrae tamaño del volumen, datos crudos y elevaciones
            size_data = get_size_data(lib, bufr_filename, root_resources)
            vol = get_raw_volume(lib, bufr_filename, size_data, root_resources=root_resources)
            elevs = get_elevations(lib, bufr_filename, max_elev=vol[0], root_resources=root_resources)

            # Encabezado de barridos + datos comprimidos
            nsweeps = int(vol[0])
            vol_metadata["nsweeps"] = nsweeps
            sweeps = parse_sweeps(vol, nsweeps, elevs)

            def decompress_wrapper(sw, idx):
                try:
                    sw["data"] = decompress_sweep(sw)
                    return sw, None
                except SweepConsistencyException:
                    vol_name = bufr_filename.split("/")[-1].split(".")[0][:-5]
                    product_type = sw.get("product_type", "N/A")
                    message = (
                        f"{vol_name}: Se descarta barrido inconsistente "
                        f"({product_type} / Sw: {idx}) (ngates fuera de limites)"
                    )
                    logger.warning(message)
                    return None, [2, message]
                except Exception as exc:
                    logger.warning(f"Descartado barrido inconsistente en sweep {idx}: {exc}")
                    return None, [
                        2,
                        f"Descartado barrido inconsistente en sweep {idx}: {exc}",
                    ]

            results = []
            if parallel:
                from concurrent.futures import ThreadPoolExecutor, as_completed

                with ThreadPoolExecutor() as executor:
                    futures = {executor.submit(decompress_wrapper, sw, idx): idx for idx, sw in enumerate(sweeps)}
                    for future in as_completed(futures):
                        sw, log_entry = future.result()
                        if sw is not None:
                            results.append(sw)
                        if log_entry:
                            run_log.append(log_entry)
                # To preserve sweep order, sort by original index if needed
                results.sort(key=lambda sw: sweeps.index(sw))
            else:
                for idx, sw in enumerate(sweeps):
                    sw_out, log_entry = decompress_wrapper(sw, idx)
                    if sw_out is not None:
                        results.append(sw_out)
                    if log_entry:
                        run_log.append(log_entry)
            sweeps = results

            # Actualiza número de barridos y metadatos desde nombre de archivo
            vol_metadata["nsweeps"] = len(sweeps)
            name_dict = bufr_name_metadata(bufr_filename)
            vol_metadata = dict(vol_metadata, **name_dict)

            # Uniformiza el número de ngates entre barridos (rellena con NaN si es menor)
            sweeps = uniformize_sweeps(sweeps)

            # Arma el volumen concatenando todos los barridos
            vol_data = assemble_volume(sweeps)

            return vol_metadata, sweeps, vol_data, run_log

    except Exception as exc:
        msg = f"Error en la decodificacion del archivo BUFR: {exc}"
        logger.error(msg, exc_info=True)
        run_log.append([3, str(exc)])
        raise ValueError(msg)


def bufr_to_dict(
    bufr_filename: str,
    root_resources: str | None = None,
    # logger_name: str | None = None,
    legacy: bool = False,
) -> Optional[dict]:
    """
    Procesa un archivo BUFR y devuelve una representación en forma de diccionario
    lista para uso por otras partes del pipeline.

    El diccionario resultante contiene al menos:
      - 'data': ndarray 2-D con todos los datos concatenados.
      - 'info': diccionario con metadatos y listas por barrido.

    Args:
        bufr_filename: Ruta al archivo BUFR a procesar.
        root_resources: Ruta al directorio de recursos (por defecto './bufr_resources').
        logger_name: Nombre del logger a utilizar (si se desea).

    Returns:
        Diccionario con 'data' e 'info' o None en caso de fallo
        (el error queda registrado en el logger).
    """
    # TODO: include a check for input type. bufr_filename should be str or Path
    # and not a list nor dict. Potentially include a fallback in case it's a
    # list of strings
    # filename = bufr_filename.split("/")[-1]
    # logger_local = logging.getLogger((logger_name or __name__) + "." + filename.split("_")[0])
    # Implement retry/backoff for transient failures (e.g., I/O, C-library transient errors)
    max_attempts = 3
    base_delay = 0.5
    for attempt in range(1, max_attempts + 1):
        try:
            meta_vol, meta_sweeps, vol_data, run_log = dec_bufr_file(
                bufr_filename=bufr_filename,
                root_resources=root_resources,
                # logger_name=logger_name,
            )

            vol: Dict[str, Any] = {"data": vol_data}

            vol["info"] = build_info_dict(meta_vol, meta_sweeps)
            if legacy:
                vol["info"] = dict(vol["info"], **vol["info"]["sweeps"].to_dict(orient="list"))
                del vol["info"]["sweeps"]

            return vol

        except Exception as e:
            # Log with local logger including attempt count
            logger.warning(f"Attempt {attempt}/{max_attempts} failed for {bufr_filename}: {e}")
            if attempt < max_attempts:
                # exponential backoff with jitter
                delay = base_delay * (2 ** (attempt - 1))
                delay = delay * (0.8 + 0.4 * np.random.random())
                time.sleep(delay)
                continue
            else:
                logger.error("Error en bufr_to_dict (final): %s", e, exc_info=True)
                # attach to run_log for compatibility (if exists)
                try:
                    run_log.append([3, str(e)])
                except Exception:
                    pass
                return None
