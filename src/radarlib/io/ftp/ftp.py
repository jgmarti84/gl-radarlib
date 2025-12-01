# -*- coding: utf-8 -*-
"""
Created on Tue Sep 02 11:05:20 2025

@author: Javier Marti
"""

import asyncio
import ftplib
import logging
import random
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Iterable, List

logger = logging.getLogger(__name__)


class FTPActionError(Exception):
    """Excepción personalizada para errores durante operaciones FTP después de una conexión exitosa."""

    pass


class FTP_IsADirectoryError(FTPActionError):
    """Excepción específica para cuando se intenta descargar un directorio en lugar de un archivo."""

    pass


@contextmanager
def ftp_connection_manager(host: str, user: str, password: str) -> Generator[ftplib.FTP, None, None]:
    """
    Gestiona una conexión FTP de forma segura, garantizando su cierre.

    Este es un gestor de contexto que se debe usar con una declaración 'with'.
    Se conecta, autentica, y cede el control al bloque 'with'. Al salir
    del bloque, cierra la conexión automáticamente.

    Args:
        host (str): La dirección IP o el nombre de dominio del servidor FTP.
        user (str): El nombre de usuario para la autenticación.
        password (str): La contraseña para la autenticación.

    Yields:
        ftplib.FTP: Un objeto de conexión FTP autenticado.

    Raises:
        ConnectionError: Si falla la conexión o el login.

    # --- CÓMO USARLO ---
    # try:
    #     with ftp_connection_manager("ftp.example.com", "user", "pass") as ftp:
    #         print("Conexión exitosa. Bienvenido:", ftp.getwelcome())
    #         print("Listando archivos:", ftp.nlst())
    #         # La conexión se cerrará automáticamente al salir de este bloque
    # except ConnectionError as e:
    #     print(f"No se pudo completar la operación FTP: {e}")
    """
    ftp_connection = None
    try:
        ftp_connection = _ftp_connection(host, user, password)

        # Devuelve la conexión al bloque 'with'
        yield ftp_connection

    except ftplib.all_errors as e:
        error_message = f"Error al conectar o autenticar en el servidor FTP '{host}': {e}"
        raise ConnectionError(error_message) from e

    finally:
        # Este bloque SIEMPRE se ejecuta, haya habido error o no.
        if ftp_connection:
            logger.info("Cerrando la conexión FTP.")
            ftp_connection.quit()


def _ftp_connection(host: str, user: str, password: str) -> ftplib.FTP:
    """
    Función auxiliar para establecer y devolver una conexión FTP autenticada.
    No maneja el cierre de la conexión; el llamador es responsable de ello.
    """
    try:
        ftp = ftplib.FTP(host)
        ftp.login(user, password)
        return ftp
    except ftplib.all_errors as e:
        error_message = f"Error al conectar o autenticar en el servidor FTP '{host}': {e}"
        raise ConnectionError(error_message) from e


def _download_single_file(ftp: ftplib.FTP, remote_path: Path, local_path: Path):
    """
    Función auxiliar que descarga un único archivo usando una conexión FTP activa.
    Gestiona el archivo local de forma segura con un gestor de contexto.
    """
    try:
        # Usamos 'with open' para garantizar que el archivo local se cierre siempre.
        with open(local_path, "wb") as archivo_local:
            # El comando RETR necesita una cadena, no un objeto Path.
            ftp.retrbinary(f"RETR {str(remote_path)}", archivo_local.write)
        logger.info(f"    [Archivo] Descargado: '{remote_path}'")
    except ftplib.all_errors as e:
        logger.error(f"No se pudo descargar el archivo '{remote_path}': {e}")
        # En un sistema robusto, podrías registrar este fallo en una lista y continuar.
    except IOError as e:
        logger.error(f"No se pudo descargar el archivo '{remote_path}': {e}")
        # En un sistema robusto, podrías registrar este fallo en una lista y continuar.


def _copy_folder_recursively(ftp: ftplib.FTP, current_remote_path: Path, current_local_path: Path):
    """
    Función auxiliar recursiva.
    Usa NLST y CWD para compatibilidad con servidores FTP antiguos que no soportan MLSD.
    """
    try:
        # 1. Obtenemos la lista de nombres. NLST es universal.
        names = ftp.nlst(str(current_remote_path))
        # nlst() puede devolver la ruta completa, así que extraemos solo el nombre base.
        # Esto hace el código más robusto.
        names = [Path(p).name for p in names]

    except ftplib.error_perm as e:
        logger.error(f"No se pudo listar el contenido del directorio remoto '{current_remote_path}': {e}")
        return

    for name in names:
        # Ignoramos las referencias al directorio actual y padre.
        if name in [".", ".."]:
            continue

        next_remote_path = current_remote_path / name
        next_local_path = current_local_path / name

        # 2. Verificamos el tipo de cada elemento intentando CWD.
        try:
            # Si podemos entrar en el directorio, es un directorio.
            ftp.cwd(str(next_remote_path))

            # Es un directorio, procedemos con la recursión.
            logger.info(f"[Directorio] Creando y explorando: '{next_local_path}'")
            next_local_path.mkdir(exist_ok=True)
            # Llamada recursiva. Es importante pasarle la ruta completa remota.
            _copy_folder_recursively(ftp, next_remote_path, next_local_path)

            # ¡Importante! Regresamos al directorio padre para continuar con el bucle.
            ftp.cwd(str(current_remote_path))

        except ftplib.error_perm:
            # Si CWD falla, asumimos que es un archivo.
            # Llamamos a nuestra función de descarga de bajo nivel.
            _download_single_file(ftp, next_remote_path, next_local_path)


def list_files_in_remote_dir(host: str, user: str, password: str, remote_dir: str, method="nlst") -> Iterable[Any]:
    """
    Obtiene una lista de nombres de archivo de un directorio en un servidor FTP.

    Utiliza un gestor de contexto para conectar de forma segura, navegar al
    directorio especificado y recuperar la lista de archivos. La conexión
    se cierra automáticamente al finalizar.

    Args:
        host (str): La dirección IP o el nombre de dominio del servidor FTP.
        user (str): El nombre de usuario para la autenticación.
        password (str): La contraseña para la autenticación.
        remote_dir (str): La ruta del directorio en el servidor FTP del cual
                          se listarán los archivos.
        method (str): El método para listar archivos. "nlst" devuelve solo
                      nombres de archivo, mientras que "mlsd" proporciona metadatos
                      adicionales. Por defecto es "nlst".

    Returns:
        List[str]: Una lista de cadenas, donde cada una es el nombre de un
                   archivo en el directorio remoto.

    Raises:
        ConnectionError: Si falla la conexión inicial o el login al servidor FTP.
        FTPActionError: Si ocurre un error durante una operación FTP después de
                        conectar (p. ej., el directorio no existe o no hay
                        permisos de lectura).
    """
    logger.info(f"Intentando listar archivos en '{remote_dir}' en el host '{host}'...")
    try:
        # El gestor contexto se encarga de conectar, autenticar y cerrar automáticamente.
        with ftp_connection_manager(host, user, password) as ftp:

            # Todas las operaciones que necesitan la conexión ocurren dentro de este bloque.
            logger.info("Conexión exitosa. Cambiando a directorio...")

            # Navegar al directorio objetivo en el ftp.
            ftp.cwd(remote_dir)

            # Busca la lista de archivos. NLST es generalmente preferido para solo nombres de archivo.
            if method == "mlsd":
                logger.info("Usando MLSD para listar archivos con metadatos...")
                # `mlsd()` returns a generator/iterator of (name, facts) pairs.
                # Convert to list so we can count and safely return the contents
                # (avoids returning an already-consumed iterator).
                file_list = list(ftp.mlsd())
                nfiles = len(file_list)
            else:
                logger.info("Usando NLST para listar solo nombres de archivo...")
                file_list = ftp.nlst()
                nfiles = len(file_list)

            logger.info(f"Se encontraron {nfiles} archivos. La conexión se cerrará automáticamente.")
            return file_list

    except ftplib.all_errors as e:
        # Aquí capturamos errores específicos de FTP que podrían ocurrir dentro del bloque 'with',
        # como ftp.cwd() fallando porque el directorio no existe.
        # Lo envolvemos en nuestra excepción personalizada, más informativa.
        error_message = f"Una operación FTP falló al intentar listar el directorio '{remote_dir}': {e}"
        raise FTPActionError(error_message) from e
    # Nota: No es necesario capturar ConnectionError aquí. Si ftp_connection_manager falla
    # al conectar, lanzará un ConnectionError, que es exactamente lo que queremos.
    # Lo dejamos propagarse al llamador.


def download_file_from_ftp(
    host: str, user: str, password: str, remote_dir: str, remote_filename: str, local_filepath: Path
) -> None:
    """
    Descarga un archivo específico de un servidor FTP a una ruta local.

    Esta función utiliza gestores de contexto anidados para garantizar que tanto
    la conexión FTP como el archivo local se cierren correctamente, incluso si
    ocurren errores durante la transferencia.

    Args:
        host (str): La dirección IP o el nombre de dominio del servidor FTP.
        user (str): El nombre de usuario para la autenticación.
        password (str): La contraseña para la autenticación.
        remote_dir (str): El directorio remoto donde se encuentra el archivo.
        remote_filename (str): El nombre del archivo a descargar.
        local_filepath (Path): La ruta completa (incluyendo el nombre del archivo)
                               donde se guardará el archivo localmente.

    Raises:
        ConnectionError: Si falla la conexión inicial o el login al servidor FTP.
        FTPActionError: Si ocurre un error durante una operación FTP después de
                        conectar (p. ej., el directorio o archivo no existe).
        IOError: Si ocurre un error al escribir el archivo en el disco local.
    """
    logger.info(f"Iniciando descarga de '{remote_filename}' desde '{host}/{remote_dir}'.")
    try:
        # 1. Gestor de contexto externo para la conexión FTP.
        #    Maneja la conexión, el login y garantiza el cierre (quit).
        with ftp_connection_manager(host, user, password) as ftp:
            logger.info("Conexión FTP establecida. Navegando al directorio...")
            ftp.cwd(remote_dir)

            # --- VERIFICACIÓN DE TIPO (NUEVO BLOQUE DE CÓDIGO) ---
            logger.info(f"Verificando que '{remote_filename}' no sea un directorio...")
            es_directorio = False
            try:
                # Intentamos entrar en el 'archivo'. Si funciona, es un directorio.
                ftp.cwd(remote_filename)
                es_directorio = True
                ftp.cwd("..")  # ¡Importante! Regresamos al directorio padre.
            except ftplib.error_perm:
                # Esperamos un error de permiso (ej. 550), lo que confirma que NO es un directorio.
                # Este es el comportamiento normal y esperado para un archivo.
                pass

            if es_directorio:
                # Si logramos entrar, lanzamos nuestro error específico.
                raise FTP_IsADirectoryError(
                    f"La ruta remota '{remote_filename}' es un directorio, no un archivo descargable."
                )
            # --- FIN DE LA VERIFICACIÓN ---

            logger.info(f"Verificación exitosa. '{remote_filename}' es un archivo. Procediendo a descargar.")
            with open(local_filepath, "wb") as archivo_local:

                # La operación principal de descarga
                comando = f"RETR {remote_filename}"
                logger.info(f"Ejecutando comando FTP: '{comando}'")

                # retrbinary escribe los datos binarios directamente en el
                # objeto de archivo local que le pasamos.
                ftp.retrbinary(comando, archivo_local.write)

    except ftplib.all_errors as e:
        # Captura errores específicos de FTP (archivo no encontrado, permisos denegados, etc.)
        mensaje_error = f"La operación FTP falló para el archivo '{remote_filename}' en '{remote_dir}': {e}"
        raise FTPActionError(mensaje_error) from e
    except IOError as e:
        # Captura errores del sistema de archivos (disco lleno, sin permisos de escritura, etc.)
        mensaje_error = f"No se pudo escribir el archivo en la ruta local '{local_filepath}': {e}"
        raise IOError(mensaje_error) from e

    logger.info(f"Descarga completada exitosamente. Archivo guardado en '{local_filepath}'.")


def download_multiple_files_from_ftp(
    host: str, user: str, password: str, remote_dir: str, remote_filenames: List[str], local_dir: Path
) -> None:
    """
    Descarga una lista de archivos de un servidor FTP a un directorio local.

    Abre una única conexión FTP para descargar todos los archivos de forma eficiente.
    Utiliza gestores de contexto para garantizar el cierre seguro tanto de la
    conexión FTP como de cada archivo local individualmente. Si la descarga de
    un archivo falla, la función se detiene y lanza una excepción.

    Args:
        host (str): La dirección IP o el nombre de dominio del servidor FTP.
        user (str): El nombre de usuario para la autenticación.
        password (str): La contraseña para la autenticación.
        remote_dir (str): El directorio remoto donde se encuentran los archivos.
        remote_filenames (List[str]): Una lista con los nombres de los archivos a descargar.
        local_dir (Path): El directorio local donde se guardarán los archivos.

    Raises:
        ConnectionError: Si falla la conexión inicial o el login al servidor FTP.
        FTPActionError: Si ocurre un error durante una operación FTP (ej. un archivo
                        no existe, no hay permisos).
        IOError: Si ocurre un error al escribir un archivo en el disco local.
    """
    logger.info(f"Iniciando descarga por lotes desde '{host}/{remote_dir}'.")
    try:
        # El gestor de contexto externo maneja la conexión FTP para toda la sesión.
        # Se conecta una vez, descarga todo y luego se cierra. Es muy eficiente.
        with ftp_connection_manager(host, user, password) as ftp:
            logger.info("Conexión FTP establecida. Navegando al directorio remoto...")
            ftp.cwd(remote_dir)

            # Iteramos sobre cada archivo que necesitamos descargar.
            for nombre_archivo in remote_filenames:
                # Construimos la ruta local completa de forma segura con pathlib.
                ruta_archivo_local = local_dir / nombre_archivo
                logger.info(f"Procesando descarga de '{nombre_archivo}' a '{ruta_archivo_local}'...")

                # --- VERIFICACIÓN DE TIPO (NUEVO BLOQUE DE CÓDIGO) ---
                logger.info(f"Verificando que '{nombre_archivo}' no sea un directorio...")
                es_directorio = False
                try:
                    # Intentamos entrar en el 'archivo'. Si funciona, es un directorio.
                    ftp.cwd(nombre_archivo)
                    es_directorio = True
                    ftp.cwd("..")  # ¡Importante! Regresamos al directorio padre.
                except ftplib.error_perm:
                    # Esperamos un error de permiso (ej. 550), lo que confirma que NO es un directorio.
                    # Este es el comportamiento normal y esperado para un archivo.
                    pass

                if es_directorio:
                    # Si logramos entrar, lanzamos nuestro error específico.
                    raise FTP_IsADirectoryError(
                        f"La ruta remota '{nombre_archivo}' es un directorio, no un archivo descargable."
                    )
                # --- FIN DE LA VERIFICACIÓN ---

                try:
                    # El gestor de contexto interno maneja cada archivo local.
                    # Garantiza que el archivo se cierre sin importar si la descarga tiene éxito o no.
                    with open(ruta_archivo_local, "wb") as archivo_local:
                        comando = f"RETR {nombre_archivo}"
                        ftp.retrbinary(comando, archivo_local.write)

                    logger.info(f"'{nombre_archivo}' descargado con éxito.")

                except ftplib.all_errors as e:
                    # Si un archivo falla, lo envolvemos en nuestro error y relanzamos.
                    # Esto detendrá toda la operación.
                    mensaje_error = f"Falló la descarga del archivo '{nombre_archivo}': {e}"
                    raise FTPActionError(mensaje_error) from e

    except IOError as e:
        # Captura errores de escritura en disco.
        mensaje_error = f"No se pudo escribir en el directorio local '{local_dir}': {e}"
        raise IOError(mensaje_error) from e

    logger.info("Todas las descargas solicitadas se completaron exitosamente.")


def download_ftp_folder(host: str, user: str, password: str, remote_path: Path, local_path: Path):
    """
    Descarga un directorio completo de forma recursiva desde un servidor FTP.

    Esta función establece una única conexión segura y la reutiliza para descargar
    toda la estructura de directorios y archivos de manera eficiente.

    Args:
        host (str): La dirección IP o el nombre de dominio del servidor FTP.
        user (str): El nombre de usuario para la autenticación.
        password (str): La contraseña para la autenticación.
        remote_path (Path): La ruta del directorio en el servidor FTP a descargar.
        local_path (Path): El directorio local donde se guardará el contenido.

    Lanza:
        ConnectionError: Si la conexión inicial o el login al servidor FTP fallan.
    """
    logger.info(f"Iniciando copia recursiva de FTP:'{remote_path}' a Local:'{local_path}'...")
    try:
        # El gestor de contexto establece la conexión que se usará para toda la operación.
        with ftp_connection_manager(host, user, password) as ftp:
            # Creamos el directorio raíz local donde se guardará todo.
            local_path.mkdir(parents=True, exist_ok=True)
            # Iniciamos el proceso recursivo.
            _copy_folder_recursively(ftp, remote_path, local_path)

        logger.info("Copia recursiva completada exitosamente.")
    except ConnectionError as e:
        logger.critical(f"No se pudo conectar al servidor FTP. Abortando operación. Error: {e}")
        raise  # Relanzamos el error para que el llamador sepa que la conexión falló.
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado durante la copia: {e}")
        raise


def build_ftp_path(fname: str, base_dir: str = "L2") -> Path:
    """
    Build the FTP full path from BUFR filename.

    Example:
        build_ftp_path("/L2", "RMA1", "RMA1_0315_03_DBZH_20250925T000534Z.BUFR")
        -> "/L2/RMA1/2025/09/25/00/0534/RMA1_0315_03_DBZH_20250925T000534Z.BUFR"
    """
    # extract datetime part of filename
    # Example fname: RMA1_0315_03_DBZH_20250925T000534Z.BUFR
    datetime_str = fname.split("_")[-1].replace("Z.BUFR", "")
    nombre_radar = fname.split("_")[0]
    dt = datetime.strptime(datetime_str, "%Y%m%dT%H%M%S")

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hour = dt.strftime("%H")
    minutesec = dt.strftime("%M%S")

    return Path(base_dir) / nombre_radar / year / month / day / hour / minutesec / fname


def parse_ftp_path(remote_path: str):
    """
    Extract radar_code, fname and datetime from a full FTP path.

    Example:
        parse_ftp_path("/L2/RMA1/2025/09/25/00/0534/RMA1_0315_03_DBZH_20250925T000534Z.BUFR")
        -> {
            "radar_code": "RMA1",
            "file_name": "RMA1_0315_03_DBZH_20250925T000534Z.BUFR",
            "datetime": datetime(2025,9,25,0,5,34),
            "field_type": "DBZH"
        }
    """
    path = Path(remote_path)
    fname = path.name
    radar_code = path.parts[-7]  # e.g. "RMA1"
    year, month, day, hour, minsec = path.parts[-6:-1]

    dt = datetime(int(year), int(month), int(day), int(hour), int(minsec[:2]), int(minsec[2:]))

    field_type = fname.split("_")[3]

    return {"radar_code": radar_code, "file_name": fname, "datetime": dt, "field_type": field_type}


async def exponential_backoff_retry(coro, max_retries: int = 5, base_delay: float = 1, max_delay: float = 60) -> None:
    """
    Retry an async callable (typically an FTP download) with exponential backoff.

    :param coro: async coroutine function (callable) with no args or bound via lambda
    :param max_retries: total number of attempts before raising
    :param base_delay: first delay in seconds (default=1)
    :param max_delay: maximum cap for delay
    """
    attempt = 0
    while True:
        try:
            return await coro()  # run the work
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                raise  # re-raise after too many failures

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            # add jitter so multiple workers don't retry in sync
            jitter = random.uniform(0, delay / 4)
            sleep_time = delay + jitter
            logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {sleep_time:.1f} seconds...")
            print(f"Retry attempt {attempt} after {sleep_time:.1f}s due to {e}")
            await asyncio.sleep(sleep_time)
