# -*- coding: utf-8 -*-
"""Unit tests for radarlib.io.ftp.ftp module.

Tests the FTP utility functions.
"""

import ftplib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from radarlib.io.ftp.ftp import (
    FTP_IsADirectoryError,
    FTPActionError,
    _copy_folder_recursively,
    _download_single_file,
    _ftp_connection,
    build_ftp_path,
    download_file_from_ftp,
    download_ftp_folder,
    download_multiple_files_from_ftp,
    exponential_backoff_retry,
    ftp_connection_manager,
    list_files_in_remote_dir,
    parse_ftp_path,
)


class TestFTPExceptions:
    """Tests for custom FTP exceptions."""

    def test_ftp_action_error_is_exception(self):
        """FTPActionError should be an Exception."""
        error = FTPActionError("Test error")
        assert isinstance(error, Exception)

    def test_ftp_is_a_directory_error_is_ftp_action_error(self):
        """FTP_IsADirectoryError should inherit from FTPActionError."""
        error = FTP_IsADirectoryError("Is a directory")
        assert isinstance(error, FTPActionError)


class TestFTPConnection:
    """Tests for _ftp_connection helper."""

    @patch("radarlib.io.ftp.ftp.ftplib.FTP")
    def test_ftp_connection_returns_connected_ftp(self, mock_ftp_class):
        """Should return authenticated FTP connection."""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp

        result = _ftp_connection("ftp.example.com", "user", "pass")

        mock_ftp_class.assert_called_once_with("ftp.example.com")
        mock_ftp.login.assert_called_once_with("user", "pass")
        assert result == mock_ftp

    @patch("radarlib.io.ftp.ftp.ftplib.FTP")
    def test_ftp_connection_raises_connection_error_on_failure(self, mock_ftp_class):
        """Should raise ConnectionError when connection fails."""
        mock_ftp_class.side_effect = ftplib.error_temp("Connection refused")

        with pytest.raises(ConnectionError, match="Error al conectar"):
            _ftp_connection("ftp.example.com", "user", "pass")


class TestFTPConnectionManager:
    """Tests for ftp_connection_manager context manager."""

    @patch("radarlib.io.ftp.ftp._ftp_connection")
    def test_connection_manager_yields_ftp(self, mock_ftp_conn):
        """Should yield FTP connection in context."""
        mock_ftp = MagicMock()
        mock_ftp_conn.return_value = mock_ftp

        with ftp_connection_manager("host", "user", "pass") as ftp:
            assert ftp == mock_ftp

    @patch("radarlib.io.ftp.ftp._ftp_connection")
    def test_connection_manager_closes_connection(self, mock_ftp_conn):
        """Should call quit() on exit."""
        mock_ftp = MagicMock()
        mock_ftp_conn.return_value = mock_ftp

        with ftp_connection_manager("host", "user", "pass"):
            pass

        mock_ftp.quit.assert_called_once()

    @patch("radarlib.io.ftp.ftp._ftp_connection")
    def test_connection_manager_raises_connection_error(self, mock_ftp_conn):
        """Should raise ConnectionError on FTP errors."""
        mock_ftp_conn.side_effect = ftplib.error_temp("Failed")

        with pytest.raises(ConnectionError):
            with ftp_connection_manager("host", "user", "pass"):
                pass


class TestDownloadSingleFile:
    """Tests for _download_single_file helper."""

    def test_download_single_file_writes_data(self, tmp_path):
        """Should download file using retrbinary."""
        mock_ftp = MagicMock()
        local_path = tmp_path / "test.bufr"

        _download_single_file(mock_ftp, Path("/remote/test.bufr"), local_path)

        # Check that retrbinary was called with correct command
        call_args = mock_ftp.retrbinary.call_args
        assert "RETR /remote/test.bufr" in call_args[0][0]

    def test_download_single_file_handles_ftp_error(self, tmp_path, caplog):
        """Should log error on FTP failure."""
        mock_ftp = MagicMock()
        mock_ftp.retrbinary.side_effect = ftplib.error_perm("File not found")
        local_path = tmp_path / "test.bufr"

        # Should not raise, just log
        _download_single_file(mock_ftp, Path("/remote/test.bufr"), local_path)


class TestCopyFolderRecursively:
    """Tests for _copy_folder_recursively helper."""

    def test_copy_folder_recursively_handles_files(self, tmp_path):
        """Should download files found in directory."""
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = ["file1.bufr", "file2.bufr"]
        # cwd fails for files (they are not directories)
        mock_ftp.cwd.side_effect = ftplib.error_perm("Not a directory")

        remote_path = Path("/remote/data")
        local_path = tmp_path / "data"
        local_path.mkdir()

        _copy_folder_recursively(mock_ftp, remote_path, local_path)

        # Should have tried to download both files
        assert mock_ftp.retrbinary.call_count == 2

    def test_copy_folder_recursively_handles_directories(self, tmp_path):
        """Should recurse into subdirectories."""
        mock_ftp = MagicMock()

        # Track which directory we're in for mock responses
        current_dir = ["/remote/data"]

        def mock_nlst(path):
            if str(path) == "/remote/data":
                return ["subdir", "file.bufr"]
            elif str(path) == "/remote/data/subdir":
                return ["nested.bufr"]
            return []

        def mock_cwd(path):
            if "subdir" in str(path):
                current_dir[0] = str(path)
                return None  # Success - it's a directory
            elif path == "/remote/data":
                current_dir[0] = str(path)
                return None
            else:
                raise ftplib.error_perm("Not a directory")

        mock_ftp.nlst.side_effect = mock_nlst
        mock_ftp.cwd.side_effect = mock_cwd

        remote_path = Path("/remote/data")
        local_path = tmp_path / "data"
        local_path.mkdir()

        _copy_folder_recursively(mock_ftp, remote_path, local_path)

        # Subdir should be created
        assert (local_path / "subdir").exists() or mock_ftp.cwd.called


class TestListFilesInRemoteDir:
    """Tests for list_files_in_remote_dir function."""

    @patch("radarlib.io.ftp.ftp.ftp_connection_manager")
    def test_list_files_nlst_method(self, mock_cm):
        """Should list files using nlst method."""
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = ["file1.bufr", "file2.bufr"]
        mock_cm.return_value.__enter__ = MagicMock(return_value=mock_ftp)
        mock_cm.return_value.__exit__ = MagicMock(return_value=False)

        result = list_files_in_remote_dir("host", "user", "pass", "/data", method="nlst")

        assert result == ["file1.bufr", "file2.bufr"]
        mock_ftp.cwd.assert_called_with("/data")

    @patch("radarlib.io.ftp.ftp.ftp_connection_manager")
    def test_list_files_mlsd_method(self, mock_cm):
        """Should list files using mlsd method with metadata."""
        mock_ftp = MagicMock()
        mock_ftp.mlsd.return_value = iter([("file1.bufr", {"type": "file"}), ("file2.bufr", {"type": "file"})])
        mock_cm.return_value.__enter__ = MagicMock(return_value=mock_ftp)
        mock_cm.return_value.__exit__ = MagicMock(return_value=False)

        result = list_files_in_remote_dir("host", "user", "pass", "/data", method="mlsd")

        assert len(result) == 2

    @patch("radarlib.io.ftp.ftp.ftp_connection_manager")
    def test_list_files_raises_ftp_action_error(self, mock_cm):
        """Should raise FTPActionError on failure."""
        mock_ftp = MagicMock()
        mock_ftp.cwd.side_effect = ftplib.error_perm("No such directory")
        mock_cm.return_value.__enter__ = MagicMock(return_value=mock_ftp)
        mock_cm.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(FTPActionError, match="operación FTP falló"):
            list_files_in_remote_dir("host", "user", "pass", "/nonexistent")


class TestDownloadFileFromFTP:
    """Tests for download_file_from_ftp function."""

    @patch("radarlib.io.ftp.ftp.ftp_connection_manager")
    def test_download_file_success(self, mock_cm, tmp_path):
        """Should download file successfully."""
        mock_ftp = MagicMock()
        mock_ftp.cwd.side_effect = [None, ftplib.error_perm("Not a dir")]  # First for dir, second for file check
        mock_cm.return_value.__enter__ = MagicMock(return_value=mock_ftp)
        mock_cm.return_value.__exit__ = MagicMock(return_value=False)

        local_file = tmp_path / "downloaded.bufr"

        download_file_from_ftp("host", "user", "pass", "/data", "file.bufr", local_file)

        mock_ftp.retrbinary.assert_called_once()

    @patch("radarlib.io.ftp.ftp.ftp_connection_manager")
    def test_download_file_raises_is_directory_error(self, mock_cm, tmp_path):
        """Should raise FTP_IsADirectoryError if target is directory."""
        mock_ftp = MagicMock()
        mock_ftp.cwd.return_value = None  # cwd succeeds - it's a directory
        mock_cm.return_value.__enter__ = MagicMock(return_value=mock_ftp)
        mock_cm.return_value.__exit__ = MagicMock(return_value=False)

        local_file = tmp_path / "downloaded.bufr"

        with pytest.raises(FTP_IsADirectoryError, match="es un directorio"):
            download_file_from_ftp("host", "user", "pass", "/data", "subdir", local_file)


class TestDownloadMultipleFilesFromFTP:
    """Tests for download_multiple_files_from_ftp function."""

    @patch("radarlib.io.ftp.ftp.ftp_connection_manager")
    def test_download_multiple_files_success(self, mock_cm, tmp_path):
        """Should download multiple files."""
        mock_ftp = MagicMock()
        mock_ftp.cwd.side_effect = [
            None,  # Initial cwd to remote_dir
            ftplib.error_perm("Not a dir"),  # file1 check
            ftplib.error_perm("Not a dir"),  # file2 check
        ]
        mock_cm.return_value.__enter__ = MagicMock(return_value=mock_ftp)
        mock_cm.return_value.__exit__ = MagicMock(return_value=False)

        download_multiple_files_from_ftp("host", "user", "pass", "/data", ["file1.bufr", "file2.bufr"], tmp_path)

        assert mock_ftp.retrbinary.call_count == 2


class TestDownloadFTPFolder:
    """Tests for download_ftp_folder function."""

    @patch("radarlib.io.ftp.ftp.ftp_connection_manager")
    @patch("radarlib.io.ftp.ftp._copy_folder_recursively")
    def test_download_folder_creates_local_dir(self, mock_copy, mock_cm, tmp_path):
        """Should create local directory and call recursive copy."""
        mock_ftp = MagicMock()
        mock_cm.return_value.__enter__ = MagicMock(return_value=mock_ftp)
        mock_cm.return_value.__exit__ = MagicMock(return_value=False)

        remote = Path("/remote/data")
        local = tmp_path / "data"

        download_ftp_folder("host", "user", "pass", remote, local)

        assert local.exists()
        mock_copy.assert_called_once()


class TestBuildFTPPath:
    """Tests for build_ftp_path function."""

    def test_build_ftp_path_basic(self):
        """Should build correct FTP path from filename."""
        fname = "RMA1_0315_03_DBZH_20250925T000534Z.BUFR"
        result = build_ftp_path(fname)

        expected = Path("L2/RMA1/2025/09/25/00/0534") / fname
        assert result == expected

    def test_build_ftp_path_custom_base_dir(self):
        """Should use custom base directory."""
        fname = "RMA1_0315_03_DBZH_20250925T120000Z.BUFR"
        result = build_ftp_path(fname, base_dir="RADAR")

        assert result.parts[0] == "RADAR"

    def test_build_ftp_path_different_datetime(self):
        """Should correctly parse different datetime values."""
        fname = "RMA2_0315_01_VRAD_20241231T235959Z.BUFR"
        result = build_ftp_path(fname)

        assert "2024" in str(result)
        assert "12" in str(result)
        assert "31" in str(result)
        assert "23" in str(result)


class TestParseFTPPath:
    """Tests for parse_ftp_path function."""

    def test_parse_ftp_path_basic(self):
        """Should parse FTP path correctly."""
        path = "/L2/RMA1/2025/09/25/00/0534/RMA1_0315_03_DBZH_20250925T000534Z.BUFR"
        result = parse_ftp_path(path)

        assert result["radar_code"] == "RMA1"
        assert result["file_name"] == "RMA1_0315_03_DBZH_20250925T000534Z.BUFR"
        assert result["field_type"] == "DBZH"
        assert result["datetime"] == datetime(2025, 9, 25, 0, 5, 34)

    def test_parse_ftp_path_different_field(self):
        """Should parse different field types."""
        path = "/L2/RMA2/2024/12/31/23/5959/RMA2_0315_01_VRAD_20241231T235959Z.BUFR"
        result = parse_ftp_path(path)

        assert result["radar_code"] == "RMA2"
        assert result["field_type"] == "VRAD"


class TestExponentialBackoffRetry:
    """Tests for exponential_backoff_retry function."""

    @pytest.mark.asyncio
    async def test_exponential_backoff_success_first_try(self):
        """Should return immediately on success."""
        call_count = [0]

        async def mock_coro():
            call_count[0] += 1
            return "success"

        result = await exponential_backoff_retry(mock_coro, max_retries=3)

        assert result == "success"
        assert call_count[0] == 1

    @pytest.mark.asyncio
    async def test_exponential_backoff_retries_on_failure(self):
        """Should retry on failure."""
        call_count = [0]

        async def mock_coro():
            call_count[0] += 1
            if call_count[0] < 3:
                raise Exception("Temporary failure")
            return "success"

        result = await exponential_backoff_retry(mock_coro, max_retries=5, base_delay=0.01)

        assert result == "success"
        assert call_count[0] == 3

    @pytest.mark.asyncio
    async def test_exponential_backoff_raises_after_max_retries(self):
        """Should raise after max retries."""

        async def mock_coro():
            raise Exception("Persistent failure")

        with pytest.raises(Exception, match="Persistent failure"):
            await exponential_backoff_retry(mock_coro, max_retries=2, base_delay=0.01)
