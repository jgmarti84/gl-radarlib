# -*- coding: utf-8 -*-
"""Unit tests for radarlib.io.ftp.ftp_client module.

Tests the RadarFTPClient class functionality.
"""
import ftplib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from radarlib.io.ftp.ftp_client import FTPError, RadarFTPClient


class TestRadarFTPClientInit:
    """Tests for RadarFTPClient initialization."""

    def test_init_with_defaults(self):
        """Should initialize with default parameters."""
        client = RadarFTPClient(host="ftp.example.com", user="testuser", password="testpass")

        assert client.host == "ftp.example.com"
        assert client.user == "testuser"
        assert client.password == "testpass"
        assert client.base_dir == "L2"
        assert client.timeout == 30
        assert client.ftp is None

    def test_init_with_custom_params(self):
        """Should initialize with custom parameters."""
        client = RadarFTPClient(
            host="radar.server.com", user="admin", password="secret", base_dir="RADAR_DATA", timeout=60
        )

        assert client.base_dir == "RADAR_DATA"
        assert client.timeout == 60


class TestRadarFTPClientConnection:
    """Tests for FTP connection management."""

    def test_is_connected_returns_false_when_no_connection(self):
        """Should return False when ftp is None."""
        client = RadarFTPClient("host", "user", "pass")
        assert client.is_connected() is False

    def test_is_connected_returns_true_when_noop_succeeds(self):
        """Should return True when NOOP command succeeds."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.voidcmd.return_value = None

        assert client.is_connected()
        client.ftp.voidcmd.assert_called_once_with("NOOP")

    def test_is_connected_returns_false_when_noop_fails(self):
        """Should return False when NOOP command fails."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.voidcmd.side_effect = EOFError("Connection lost")

        assert not client.is_connected()

    @patch("radarlib.io.ftp.ftp_client.ftplib.FTP")
    def test_connect_creates_ftp_connection(self, mock_ftp_class):
        """Should create FTP connection and login."""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp

        client = RadarFTPClient("ftp.example.com", "user", "pass", timeout=30)
        client._connect()

        mock_ftp_class.assert_called_once_with(timeout=30)
        mock_ftp.connect.assert_called_once_with("ftp.example.com")
        mock_ftp.login.assert_called_once_with("user", "pass")

    @patch("radarlib.io.ftp.ftp_client.ftplib.FTP")
    def test_connect_raises_ftp_error_on_failure(self, mock_ftp_class):
        """Should raise FTPError when connection fails."""
        mock_ftp = MagicMock()
        mock_ftp.connect.side_effect = ftplib.error_temp("Connection refused")
        mock_ftp_class.return_value = mock_ftp

        client = RadarFTPClient("ftp.example.com", "user", "pass")

        with pytest.raises(FTPError, match="Error connecting"):
            client._connect()

    @patch("radarlib.io.ftp.ftp_client.ftplib.FTP")
    def test_context_manager_connects_and_disconnects(self, mock_ftp_class):
        """Should connect on enter and disconnect on exit."""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp

        client = RadarFTPClient("ftp.example.com", "user", "pass")

        with client:
            assert client.ftp is not None
            mock_ftp.connect.assert_called_once()

        mock_ftp.quit.assert_called_once()


class TestRadarFTPClientListDir:
    """Tests for directory listing."""

    def test_list_dir_returns_directory_contents(self):
        """Should return list of directory contents."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.voidcmd.return_value = None  # is_connected check
        client.ftp.nlst.return_value = ["file1.bufr", "file2.bufr"]

        result = client.list_dir("/path/to/dir")

        assert result == ["file1.bufr", "file2.bufr"]
        client.ftp.cwd.assert_called_with("/path/to/dir")

    def test_list_dir_raises_ftp_error_on_failure(self):
        """Should raise FTPError when listing fails."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.voidcmd.return_value = None
        client.ftp.cwd.side_effect = ftplib.error_perm("No such directory")

        with pytest.raises(FTPError, match="Error listing directory"):
            client.list_dir("/nonexistent")


class TestRadarFTPClientDownload:
    """Tests for file download."""

    def test_download_file_creates_parent_directory(self, tmp_path):
        """Should create parent directory if it doesn't exist."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.retrbinary.return_value = None

        local_path = tmp_path / "subdir" / "file.bufr"

        client.download_file("/remote/file.bufr", local_path)

        assert local_path.parent.exists()

    def test_download_file_returns_local_path(self, tmp_path):
        """Should return the local path after download."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.retrbinary.return_value = None

        local_path = tmp_path / "file.bufr"
        result = client.download_file("/remote/file.bufr", local_path)

        assert result == local_path

    def test_download_file_raises_ftp_error_on_failure(self, tmp_path):
        """Should raise FTPError when download fails."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.retrbinary.side_effect = ftplib.error_perm("File not found")

        with pytest.raises(FTPError, match="Error downloading"):
            client.download_file("/remote/file.bufr", tmp_path / "file.bufr")


class TestRadarFTPClientTraverseRadar:
    """Tests for radar directory traversal."""

    def test_traverse_radar_filters_by_date_range(self):
        """Should only yield files within date range."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.voidcmd.return_value = None

        # Mock directory structure
        def mock_list_dir(path):
            if path == "/L2/RMA1":
                return ["2025"]
            elif path == "/L2/RMA1/2025":
                return ["01", "02"]
            elif "/2025/01" in path:
                if path.endswith("/01"):
                    return ["15"]  # Day 15
                elif path.endswith("/15"):
                    return ["10"]  # Hour 10
                elif path.endswith("/10"):
                    return ["30"]  # Minute 30
                elif path.endswith("/30"):
                    return ["RMA1_0315_01.bufr"]
            elif "/2025/02" in path:
                return []
            return []

        client.list_dir = MagicMock(side_effect=mock_list_dir)

        dt_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        dt_end = datetime(2025, 1, 31, tzinfo=timezone.utc)

        results = list(client.traverse_radar("RMA1", dt_start, dt_end))
        assert results
        # Should have traversed the January directory
        assert any("01" in str(r) for call in client.list_dir.call_args_list for r in call.args)


class TestFTPError:
    """Tests for FTPError exception."""

    def test_ftp_error_is_exception(self):
        """FTPError should be an Exception."""
        error = FTPError("Test error")
        assert isinstance(error, Exception)

    def test_ftp_error_message(self):
        """FTPError should store message."""
        error = FTPError("Connection failed")
        assert str(error) == "Connection failed"


class TestEnsureConnection:
    """Tests for _ensure_connection method."""

    @patch("time.sleep")
    def test_ensure_connection_retries_on_failure(self, mock_sleep):
        """Should retry connection with backoff."""
        client = RadarFTPClient("host", "user", "pass")

        # First two attempts fail, third succeeds
        connect_attempts = [0]

        def mock_connect():
            connect_attempts[0] += 1
            if connect_attempts[0] < 3:
                raise FTPError("Connection failed")
            client.ftp = MagicMock()
            client.ftp.voidcmd.return_value = None

        client._connect = MagicMock(side_effect=mock_connect)
        client._ensure_connection(retries=3, backoff=0.1)

        assert connect_attempts[0] == 3

    @patch("time.sleep")
    def test_ensure_connection_raises_after_max_retries(self, mock_sleep):
        """Should raise FTPError after max retries."""
        client = RadarFTPClient("host", "user", "pass")
        client._connect = MagicMock(side_effect=FTPError("Failed"))

        with pytest.raises(FTPError, match="Could not connect"):
            client._ensure_connection(retries=2, backoff=0.1)

    def test_ensure_connection_does_nothing_if_connected(self):
        """Should not reconnect if already connected."""
        client = RadarFTPClient("host", "user", "pass")
        client.ftp = MagicMock()
        client.ftp.voidcmd.return_value = None  # is_connected returns True

        client._connect = MagicMock()
        client._ensure_connection()

        client._connect.assert_not_called()
