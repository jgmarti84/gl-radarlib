# -*- coding: utf-8 -*-
"""FTP client for downloading BUFR files from radar data servers."""

import ftplib
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List

from .ftp import FTP_IsADirectoryError, FTPActionError, ftp_connection_manager

logger = logging.getLogger(__name__)


class FTPClient:
    """
    Client for interacting with FTP servers to download BUFR radar files.

    This class provides a high-level interface for common FTP operations,
    with built-in error handling and connection management.

    Example:
        >>> client = FTPClient(host="ftp.example.com", user="user", password="pass")
        >>> files = client.list_files("/L2/RMA1/2024/01/01")
        >>> client.download_file("/L2/RMA1/2024/01/01/file.BUFR", "./local.BUFR")
    """

    def __init__(self, host: str, user: str, password: str):
        """
        Initialize the FTP client.

        Args:
            host: FTP server hostname or IP address
            user: Username for authentication
            password: Password for authentication
        """
        self.host = host
        self.user = user
        self.password = password

    @contextmanager
    def connect(self) -> Generator[ftplib.FTP, None, None]:
        """
        Create a context-managed FTP connection.

        Yields:
            ftplib.FTP: Active FTP connection

        Raises:
            ConnectionError: If connection or authentication fails
        """
        with ftp_connection_manager(self.host, self.user, self.password) as ftp:
            yield ftp

    def list_files(self, remote_dir: str, method: str = "nlst") -> List[tuple[str, dict[str, str]]] | List[str]:
        """
        List files in a remote directory.

        Args:
            remote_dir: Path to remote directory
            method: Listing method ("nlst" for names only, "mlsd" for metadata)

        Returns:
            List of file names or tuples of (name, metadata) if using mlsd

        Raises:
            ConnectionError: If connection fails
            FTPActionError: If listing operation fails
        """
        logger.info(f"Listing files in '{remote_dir}' on '{self.host}'...")
        try:
            with self.connect() as ftp:
                ftp.cwd(remote_dir)

                if method == "mlsd":
                    file_list = list(ftp.mlsd())
                else:
                    file_list = ftp.nlst()

                logger.info(f"Found {len(file_list)} items")
                return file_list

        except ftplib.all_errors as e:
            error_message = f"Failed to list directory '{remote_dir}': {e}"
            raise FTPActionError(error_message) from e

    def download_file(self, remote_path: str, local_path: Path, verify_not_directory: bool = True) -> None:
        """
        Download a single file from the FTP server.

        Args:
            remote_path: Full path to remote file (including directory and filename)
            local_path: Local path where file will be saved
            verify_not_directory: If True, verify that remote path is not a directory

        Raises:
            ConnectionError: If connection fails
            FTPActionError: If download operation fails
            FTP_IsADirectoryError: If remote path is a directory (when verify_not_directory=True)
            IOError: If local file cannot be written
        """
        logger.info(f"Downloading '{remote_path}' to '{local_path}'")

        remote_path_obj = Path(remote_path)
        remote_dir = str(remote_path_obj.parent)
        remote_filename = remote_path_obj.name

        try:
            with self.connect() as ftp:
                ftp.cwd(remote_dir)

                # Verify it's not a directory
                if verify_not_directory:
                    try:
                        ftp.cwd(remote_filename)
                        ftp.cwd("..")
                        raise FTP_IsADirectoryError(f"Path '{remote_path}' is a directory, not a file")
                    except ftplib.error_perm:
                        # Expected error - it's a file, not a directory
                        pass

                # Download the file
                with open(local_path, "wb") as local_file:
                    ftp.retrbinary(f"RETR {remote_filename}", local_file.write)

                logger.info(f"Successfully downloaded to '{local_path}'")

        except ftplib.all_errors as e:
            error_message = f"Failed to download '{remote_path}': {e}"
            raise FTPActionError(error_message) from e
        except IOError as e:
            error_message = f"Failed to write to '{local_path}': {e}"
            raise IOError(error_message) from e

    def download_files(self, remote_dir: str, filenames: List[str], local_dir: Path) -> None:
        """
        Download multiple files from the same remote directory.

        Uses a single connection for efficiency.

        Args:
            remote_dir: Remote directory containing the files
            filenames: List of filenames to download
            local_dir: Local directory where files will be saved

        Raises:
            ConnectionError: If connection fails
            FTPActionError: If any download fails
            IOError: If local files cannot be written
        """
        logger.info(f"Downloading {len(filenames)} files from '{remote_dir}'")

        try:
            with self.connect() as ftp:
                ftp.cwd(remote_dir)

                for filename in filenames:
                    local_path = local_dir / filename

                    # Verify not a directory
                    try:
                        ftp.cwd(filename)
                        ftp.cwd("..")
                        raise FTP_IsADirectoryError(f"Path '{filename}' is a directory, not a file")
                    except ftplib.error_perm:
                        pass

                    # Download
                    with open(local_path, "wb") as local_file:
                        ftp.retrbinary(f"RETR {filename}", local_file.write)

                    logger.info(f"Downloaded '{filename}'")

            logger.info("All files downloaded successfully")

        except ftplib.all_errors as e:
            error_message = f"Failed during batch download: {e}"
            raise FTPActionError(error_message) from e
        except IOError as e:
            error_message = f"Failed to write to '{local_dir}': {e}"
            raise IOError(error_message) from e

    def file_exists(self, remote_path: str) -> bool:
        """
        Check if a file exists on the FTP server.

        Args:
            remote_path: Full path to remote file

        Returns:
            True if file exists, False otherwise
        """
        remote_path_obj = Path(remote_path)
        remote_dir = str(remote_path_obj.parent)
        remote_filename = remote_path_obj.name

        try:
            with self.connect() as ftp:
                ftp.cwd(remote_dir)
                files = ftp.nlst()
                return remote_filename in files
        except ftplib.all_errors:
            return False
        except ConnectionError:
            return False
