import asyncio
import ftplib
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List, Optional, Tuple

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
