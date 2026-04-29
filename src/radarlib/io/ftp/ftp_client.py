import asyncio
import ftplib
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List, Optional, Tuple

from radarlib.io.bufr.bufr import BUFRFileInfo, BUFRFilename
from radarlib.utils.names_utils import build_vol_types_regex

logger = logging.getLogger(__name__)


class FTPError(Exception):
    """Base class for FTP errors."""


class RadarFTPClient:
    """
    Efficient FTP client for radar BUFR data retrieval.
    - Maintains a single FTP connection during operations
    - Supports traversal of nested YYYY/MM/DD/HH/MM folder structures
    - Provides methods to list, traverse, and download files
    """

    def __init__(self, host: str, user: str, password: str, base_dir: str = "L2", timeout: int = 30):
        self.host = host
        self.user = user
        self.password = password
        self.base_dir = base_dir
        self.timeout = timeout
        self.ftp: Optional[ftplib.FTP] = None

    def _connect(self) -> None:
        """Establece una nueva conexión FTP y hace login."""
        if self.ftp is not None:
            try:
                self.ftp.quit()
            except Exception:
                try:
                    self.ftp.close()
                except Exception:
                    pass
            self.ftp = None

        try:
            self.ftp = ftplib.FTP(timeout=self.timeout)
            self.ftp.connect(self.host)
            self.ftp.login(self.user, self.password)
            logger.info(f"Connected to FTP {self.host}")
        except ftplib.all_errors as e:
            self.ftp = None
            raise FTPError(f"Error connecting to FTP {self.host}: {e}")

    # ----------------------
    # Context Manager
    # ----------------------
    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.ftp:
            try:
                self.ftp.quit()
            except Exception:
                try:
                    self.ftp.close()
                except Exception:
                    pass
        logger.info("FTP connection closed")
        self.ftp = None

    def is_connected(self) -> bool:
        """
        Comprueba si la sesión FTP está viva realizando un NOOP.
        Devuelve True si la conexión responde, False en otro caso.
        """
        if self.ftp is None:
            return False
        try:
            # NOOP es ligero y seguro para comprobar la conexión.
            self.ftp.voidcmd("NOOP")
            return True
        except (ftplib.error_reply, ftplib.error_temp, ftplib.error_proto, ftplib.error_perm, EOFError, OSError):
            return False

    def _ensure_connection(self, retries: int = 3, backoff: float = 1.0) -> None:
        """
        Asegura que exista una conexión válida; intenta reconectar con backoff si es necesario.
        Lanza FTPError si no puede reconectar.
        """
        if self.is_connected():
            return

        last_exc: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                logger.debug(f"Attempting FTP connect to {self.host} (attempt {attempt}/{retries})")
                self._connect()
                if self.is_connected():
                    logger.info("FTP reconnection successful")
                    return
            except Exception as e:
                last_exc = e
                logger.warning(f"FTP connect attempt {attempt} failed: {e}")
            time.sleep(backoff * (2 ** (attempt - 1)))
        raise FTPError(f"Could not connect to FTP {self.host} after {retries} attempts: {last_exc}")

    # ----------------------
    # Low-level listing
    # ----------------------
    def list_dir(self, remote_path: str) -> List[str]:
        """
        List directory contents using single active connection.
        Reintenta la operación si detecta pérdida de conexión (EOFError).
        """
        self._ensure_connection()
        try:
            self.ftp.cwd(remote_path)  # type: ignore
            return self.ftp.nlst()  # type: ignore
        except EOFError as e:
            logger.warning(f"EOFError while listing {remote_path}: trying to reconnect and retry: {e}")
            # Intentar reconexión y una segunda pasada
            try:
                self._ensure_connection()
                self.ftp.cwd(remote_path)  # type: ignore
                return self.ftp.nlst()  # type: ignore
            except Exception as e2:
                raise FTPError(f"Error listing directory {remote_path} after reconnect: {e2}")
        except ftplib.all_errors as e:
            raise FTPError(f"Error listing directory {remote_path}: {e}")

    # ----------------------
    # File download
    # ----------------------
    def download_file(self, remote_path: str, local_path: Path) -> Path:
        """Download a single file efficiently using the current session."""
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(local_path, "wb") as f:
                self.ftp.retrbinary(f"RETR {remote_path}", f.write)  # type: ignore
            logger.info(f"Downloaded {remote_path} -> {local_path}")
            return local_path
        except ftplib.all_errors as e:
            raise FTPError(f"Error downloading {remote_path}: {e}")

    def file_exists(self, remote_path: str) -> bool:
        """
        Check if a file exists on the FTP server.

        Args:
            remote_path: Full path to remote file

        Returns:
            True if file exists, False otherwise
        """
        self._ensure_connection()

        remote_path_obj = Path(remote_path)
        remote_dir = str(remote_path_obj.parent)
        remote_filename = remote_path_obj.name

        try:
            files = self.list_dir(remote_dir)
            return remote_filename in files
        except ftplib.all_errors:
            return False
        except ConnectionError:
            return False
        except FTPError:
            return False

    def find_last_bufr_file(
        self,
        radar: str,
        strategy: Optional[str] = None,
        volume_nr: Optional[int] = None,
        field: Optional[str] = None,
        search_from_time: Optional[datetime] = None,
    ) -> Optional[BUFRFileInfo]:
        """
        Encuentra el último archivo BUFR para un radar dado, con opciones de filtrado.
        
        Busca hacia atrás desde `search_from_time` (por defecto ahora).
        
        Parameters
        ----------
        radar : str
            Identificador del radar
        strategy : str, optional
            Filtra por estrategia de radar si se proporciona
        volume_nr : int, optional
            Filtra por número de volumen si se proporciona
        field : str, optional
            Filtra por campo específico si se proporciona
        search_from_time : datetime, optional
            Hora desde la cual buscar hacia atrás (por defecto: ahora)
            
        Returns
        -------
        BUFRFileInfo or None
            Primer archivo encontrado (más reciente), o None si no hay coincidencias
        """
        if search_from_time is None:
            search_from_time = datetime.now(timezone.utc)

        base_path = f"/{self.base_dir}/{radar}"
        try:
            years = sorted(self.list_dir(base_path), reverse=True)
            for y in years:
                yi = int(y)
                if yi > search_from_time.year:
                    continue
                year_path = f"{base_path}/{y}"
                months = sorted(self.list_dir(year_path), reverse=True)
                for m in months:
                    mi = int(m)
                    if yi == search_from_time.year and mi > search_from_time.month:
                        continue
                    month_path = f"{year_path}/{m}"
                    days = sorted(self.list_dir(month_path), reverse=True)
                    for d in days:
                        di = int(d)
                        if yi == search_from_time.year and mi == search_from_time.month and di > search_from_time.day:
                            continue
                        day_path = f"{month_path}/{d}"
                        hours = sorted(self.list_dir(day_path), reverse=True)
                        for h in hours:
                            hi = int(h)
                            if (yi == search_from_time.year and mi == search_from_time.month and 
                                di == search_from_time.day and hi > search_from_time.hour):
                                continue
                            hour_path = f"{day_path}/{h}"
                            minutes = sorted(self.list_dir(hour_path), reverse=True)
                            for ms in minutes:
                                mi_val = int(ms[:2])
                                sec_val = int(ms[2:]) if len(ms) > 2 else 0
                                if (yi == search_from_time.year and mi == search_from_time.month and 
                                    di == search_from_time.day and hi == search_from_time.hour and
                                    mi_val > search_from_time.minute):
                                    continue
                                
                                minute_path = f"{hour_path}/{ms}"
                                files = self.list_dir(minute_path)
                                for fname in sorted(files, reverse=True):
                                    bufr_file = BUFRFilename(fname)
                                    if strategy and strategy != bufr_file.strategy:
                                        continue
                                    if volume_nr and volume_nr != bufr_file.volume:
                                        continue
                                    if field and field != bufr_file.field:
                                        continue
                                    return BUFRFileInfo(fname, minute_path, True, None, None)
        except FTPError as e:
            logger.error(f"Error finding last BUFR file for radar {radar}: {e}")
            return None

    # ----------------------
    # Recursive traversal for radar BUFR files
    # ----------------------
    def traverse_radar(
        self,
        radar_name: str,
        dt_start: datetime | None = None,
        dt_end: datetime | None = None,
        include_start: bool = True,
        include_end: bool = True,
        vol_types: Optional[dict] | re.Pattern = None,
    ) -> Generator[Tuple[datetime, str, str | Path], None, None]:
        """
        Traverse FTP folders for BUFR files, constrained to dt_start..dt_end.
        Correctly handles boundary pruning at each level.
        """
        if vol_types is not None and isinstance(vol_types, dict):
            vol_types = build_vol_types_regex(vol_types)

        base_path = f"/{self.base_dir}/{radar_name}"
        if dt_start is None:
            dt_start = datetime.min.replace(tzinfo=timezone.utc)
        if dt_end is None:
            dt_end = datetime.max.replace(tzinfo=timezone.utc)

        try:
            years = sorted(self.list_dir(base_path))
            for y in years:
                yi = int(y)
                if yi < dt_start.year or yi > dt_end.year:
                    continue
                year_path = f"{base_path}/{y}"

                months = sorted(self.list_dir(year_path))
                for m in months:
                    mi = int(m)
                    if yi == dt_start.year and mi < dt_start.month:
                        continue
                    if yi == dt_end.year and mi > dt_end.month:
                        continue
                    month_path = f"{year_path}/{m}"

                    days = sorted(self.list_dir(month_path))
                    for d in days:
                        di = int(d)
                        if yi == dt_start.year and mi == dt_start.month and di < dt_start.day:
                            continue
                        if yi == dt_end.year and mi == dt_end.month and di > dt_end.day:
                            continue
                        day_path = f"{month_path}/{d}"

                        hours = sorted(self.list_dir(day_path))
                        for h in hours:
                            hi = int(h)
                            if (
                                yi == dt_start.year
                                and mi == dt_start.month
                                and di == dt_start.day
                                and hi < dt_start.hour
                            ):
                                continue
                            if yi == dt_end.year and mi == dt_end.month and di == dt_end.day and hi > dt_end.hour:
                                continue
                            hour_path = f"{day_path}/{h}"

                            minutes = sorted(self.list_dir(hour_path))
                            for ms in minutes:
                                mi_val = int(ms[:2])
                                sec_val = int(ms[2:]) if len(ms) > 2 else 0
                                dt = datetime(yi, mi, di, hi, mi_val, sec_val, tzinfo=timezone.utc)

                                # ---------- INCLUSIVITY LOGIC ----------
                                if include_start:
                                    if dt < dt_start:
                                        continue
                                else:
                                    if dt <= dt_start:
                                        continue

                                if include_end:
                                    if dt > dt_end:
                                        continue
                                else:
                                    if dt >= dt_end:
                                        continue
                                # ---------------------------------------

                                minute_path = f"{hour_path}/{ms}"
                                files = self.list_dir(minute_path)
                                for fname in files:
                                    # Filtrado por vol_types si se proporciona
                                    if vol_types is not None:
                                        if not vol_types.match(fname):
                                            continue
                                    full_remote = Path(f"{minute_path}/{fname}")
                                    yield dt, fname, full_remote
        except FTPError as e:
            logger.error(f"Traversal failed for radar {radar_name}: {e}")

    @staticmethod
    def _path_to_datetime(year: str, month: str, day: str, hour: str, minute: str, second: str) -> datetime:
        return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))


class RadarFTPClientAsync(RadarFTPClient):
    """
    Async-enabled wrapper around RadarFTPClient.

    - Traversal & list_dir remain synchronous (from parent class).
    - Async context manager translates __enter__/__exit__ into async friendly version.
    - Download methods are wrapped with asyncio.to_thread so they run concurrently.
    """

    def __init__(self, host: str, user: str, password: str, base_dir: str = "L2", max_workers: Optional[int] = None):
        super().__init__(host, user, password, base_dir)
        self._max_workers = max_workers
        self._semaphore = asyncio.Semaphore(self.max_workers)

    @property
    def max_workers(self):
        if self._max_workers is None:
            self._max_workers = min(32, (os.cpu_count() or 1) + 4)
        return self._max_workers

    # ------------------------------
    # Async context manager
    # ------------------------------
    async def __aenter__(self):
        # Uses the parent sync __enter__
        return self.__enter__()

    async def __aexit__(self, exc_type, exc, tb):
        return self.__exit__(exc_type, exc, tb)

    # ------------------------------
    # Async parallel downloads
    # ------------------------------
    async def download_file_async(self, remote_path: Path, local_path: Path) -> Path:
        """
        Each download runs inside its own short-lived FTP connection,
        dispatched safely in a thread via asyncio.to_thread.
        """
        async with self._semaphore:
            return await asyncio.to_thread(self._download_with_fresh_connection, remote_path, local_path)

    def _download_with_fresh_connection(self, remote_path: Path, local_path: Path) -> Path:
        """This is blocking; run per-task in thread for safety."""
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with ftplib.FTP(self.host) as ftp:
                ftp.login(self.user, self.password)
                dir_path = remote_path.parent.as_posix()
                fname = remote_path.name
                ftp.cwd(dir_path)
                with open(local_path, "wb") as f:
                    ftp.retrbinary(f"RETR {fname}", f.write)

            logger.info(f"Downloaded {remote_path} -> {local_path}")
            return local_path

        except ftplib.all_errors as e:
            raise FTPError(f"Error downloading {remote_path}: {e}")

    async def download_files_parallel(self, files: List[Tuple[Path, Path]]) -> List[Path]:
        """Download multiple files asynchronously in parallel."""
        tasks = [asyncio.create_task(self.download_file_async(remote, local)) for remote, local in files]
        return await asyncio.gather(*tasks, return_exceptions=False)

    def _list_dir_with_fresh_connection(self, remote_path: str) -> List[str]:
        """
        List directory contents using a fresh FTP connection.
        This prevents thread safety issues when called from multiple threads.

        Args:
            remote_path: Remote directory path

        Returns:
            List of items in directory, empty list on error
        """
        try:
            with ftplib.FTP(self.host, timeout=self.timeout) as ftp:
                ftp.login(self.user, self.password)
                ftp.cwd(remote_path)
                return ftp.nlst()
        except ftplib.all_errors as e:
            logger.debug(f"Error listing {remote_path}: {e}")
            return []
        except Exception as e:
            logger.debug(f"Unexpected error listing {remote_path}: {e}")
            return []

    def _check_file_exists_with_fresh_connection(self, remote_path: str) -> bool:
        """
        Check if a file exists on the FTP server using a fresh connection.
        Each call creates its own FTP connection to avoid thread safety issues.

        Args:
            remote_path: Full path to remote file

        Returns:
            True if file exists, False otherwise
        """
        try:
            with ftplib.FTP(self.host, timeout=self.timeout) as ftp:
                ftp.login(self.user, self.password)
                remote_path_obj = Path(remote_path)
                remote_dir = str(remote_path_obj.parent)
                remote_filename = remote_path_obj.name
                ftp.cwd(remote_dir)
                files = ftp.nlst()
                return remote_filename in files
        except ftplib.all_errors:
            logger.debug(f"File existence check failed for {remote_path}")
            return False
        except Exception as e:
            logger.debug(f"Unexpected error checking file existence: {e}")
            return False

    def _find_last_bufr_file_with_fresh_connection(
        self,
        radar: str,
        strategy: Optional[str] = None,
        volume_nr: Optional[int] = None,
        field: Optional[str] = None,
    ) -> Optional[BUFRFileInfo]:
        """
        Find the last BUFR file for a radar using fresh FTP connections.
        Each directory listing creates its own connection to avoid thread safety issues.

        Args:
            radar: Radar name
            strategy: Optional filter by strategy number
            volume_nr: Optional filter by volume number
            field: Optional filter by field name

        Returns:
            BUFRFileInfo if found, None otherwise
        """
        base_path = f"/{self.base_dir}/{radar}"
        try:
            years = sorted(self._list_dir_with_fresh_connection(base_path), reverse=True)
            for y in years:
                year_path = f"{base_path}/{y}"
                months = sorted(self._list_dir_with_fresh_connection(year_path), reverse=True)
                for m in months:
                    month_path = f"{year_path}/{m}"
                    days = sorted(self._list_dir_with_fresh_connection(month_path), reverse=True)
                    for d in days:
                        day_path = f"{month_path}/{d}"
                        hours = sorted(self._list_dir_with_fresh_connection(day_path), reverse=True)
                        for h in hours:
                            hour_path = f"{day_path}/{h}"
                            minutes = sorted(self._list_dir_with_fresh_connection(hour_path), reverse=True)
                            for ms in minutes:
                                minute_path = f"{hour_path}/{ms}"
                                files = self._list_dir_with_fresh_connection(minute_path)
                                for fname in sorted(files, reverse=True):
                                    bufr_file = BUFRFilename(fname)
                                    if strategy and strategy != bufr_file.strategy:
                                        continue
                                    if volume_nr and volume_nr != bufr_file.volume:
                                        continue
                                    if field and field != bufr_file.field:
                                        continue
                                    return BUFRFileInfo(fname, minute_path, True, None, None)
        except Exception as e:
            logger.error(f"Error finding last BUFR file for radar {radar}: {e}")
            return None
        return None

    async def files_exist_parallel(self, remote_paths: List[str]) -> List[Tuple[str, bool]]:
        """
        Check if multiple files exist on the FTP server in parallel.

        Uses the semaphore to limit concurrent FTP operations. Each check uses a fresh
        FTP connection to avoid thread safety issues with shared connections.

        Args:
            remote_paths: List of remote file paths to check

        Returns:
            List of tuples (remote_path, exists) indicating existence of each file

        Example:
            >>> import asyncio
            >>>
            >>> async def check_multiple_files():
            ...     async with RadarFTPClientAsync('ftp.example.com', 'user', 'pass') as client:
            ...         files_to_check = [
            ...             '/L2/RMA1/2025/10/20/15/3045/RMA1_0315_01_DBZV_20251020T153045Z.BUFR',
            ...             '/L2/RMA1/2025/10/20/15/3046/RMA1_0315_01_DBZV_20251020T153046Z.BUFR',
            ...             '/L2/RMA1/2025/10/20/15/3047/RMA1_0315_01_DBZV_20251020T153047Z.BUFR',
            ...         ]
            ...         results = await client.files_exist_parallel(files_to_check)
            ...         for path, exists in results:
            ...             status = '✓' if exists else '✗'
            ...             print(f'{status} {path}')
            ...
            >>> # Run the async function
            >>> asyncio.run(check_multiple_files())
        """

        async def _check_file_exists(remote_path: str) -> Tuple[str, bool]:
            """Check if a single file exists using the semaphore and fresh connection."""
            async with self._semaphore:
                exists = await asyncio.to_thread(self._check_file_exists_with_fresh_connection, remote_path)
                return (remote_path, exists)

        tasks = [asyncio.create_task(_check_file_exists(path)) for path in remote_paths]
        return await asyncio.gather(*tasks, return_exceptions=False)

    async def find_last_bufr_files_parallel(
        self,
        radars: List[str],
        strategy: Optional[str] = None,
        volume_nr: Optional[int] = None,
        field: Optional[str] = None,
    ) -> List[Tuple[str, Optional[BUFRFileInfo]]]:
        """
        Find the last BUFR file for multiple radars in parallel.

        Uses the semaphore to limit concurrent FTP operations. Each search uses fresh
        FTP connections to avoid thread safety issues with shared connections.
        The same strategy, volume_nr, and field filters are applied to all radars.

        Args:
            radars: List of radar names to check (e.g., ['RMA1', 'RMA2', 'RMA3'])
            strategy: Optional filter by strategy number (applied to all radars)
            volume_nr: Optional filter by volume number (applied to all radars)
            field: Optional filter by field name (applied to all radars)

        Returns:
            List of tuples (radar_name, BUFRFileInfo or None) for each radar

        Example:
            >>> import asyncio
            >>>
            >>> async def find_latest_files_for_multiple_radars():
            ...     async with RadarFTPClientAsync('ftp.example.com', 'user', 'pass') as client:
            ...         radars = ['RMA1', 'RMA2', 'RMA3']
            ...         results = await client.find_last_bufr_files_parallel(
            ...             radars=radars,
            ...             strategy='0315',
            ...             field='DBZV'
            ...         )
            ...         for radar, file_info in results:
            ...             if file_info:
            ...                 print(f'{radar}: {file_info.filename} at {file_info.datetime}')
            ...             else:
            ...                 print(f'{radar}: No file found')
            ...
            >>> # Run the async function
            >>> asyncio.run(find_latest_files_for_multiple_radars())
        """

        async def _find_last_file(radar: str) -> Tuple[str, Optional[BUFRFileInfo]]:
            """Find last BUFR file for a single radar using semaphore and fresh connection."""
            async with self._semaphore:
                file_info = await asyncio.to_thread(
                    self._find_last_bufr_file_with_fresh_connection,
                    radar,
                    strategy,
                    volume_nr,
                    field,
                )
                return (radar, file_info)

        tasks = [asyncio.create_task(_find_last_file(radar)) for radar in radars]
        return await asyncio.gather(*tasks, return_exceptions=False)
