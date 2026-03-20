# -*- coding: utf-8 -*-
"""SQLite-based state tracking for downloaded BUFR files."""

import hashlib
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SQLiteStateTracker:
    """
    Track downloaded BUFR files using SQLite database.

    Provides better performance and query capabilities compared to JSON,
    especially for large numbers of files. Tracks download progress,
    checksums, and file metadata.

    Example:
        >>> tracker = SQLiteStateTracker("./download_state.db")
        >>> tracker.mark_downloaded("file.BUFR", "/L2/RMA1/2025/01/01/18/3020/file.BUFR")
        >>> if not tracker.is_downloaded("file2.BUFR"):
        ...     # Download file2.BUFR
        ...     tracker.mark_downloaded("file2.BUFR", "/L2/RMA1/2025/01/01/18/3025/file2.BUFR")
    """

    def __init__(self, db_path: Path):
        """
        Initialize the SQLite state tracker.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._init_database()

    def _init_database(self) -> None:
        """Initialize database schema."""
        # Check if database file already exists
        db_exists = self.db_path.exists()

        conn = self._get_connection()
        cursor = conn.cursor()

        # Check if tables already exist by querying sqlite_master
        cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('downloads', 'volume_processing', 'product_generation')
        """
        )
        existing_tables = {row[0] for row in cursor.fetchall()}
        expected_tables = {"downloads", "volume_processing", "product_generation"}
        missing_tables = expected_tables - existing_tables

        # Main downloads table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                remote_path TEXT NOT NULL,
                local_path TEXT,
                downloaded_at TEXT NOT NULL,
                file_size INTEGER,
                checksum TEXT,
                radar_name TEXT,
                strategy TEXT,
                vol_nr TEXT,
                field_type TEXT,
                observation_datetime TEXT,
                status TEXT DEFAULT 'completed',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """
        )

        # Index for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_filename ON downloads(filename)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_radar_datetime ON downloads(radar_name, observation_datetime)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON downloads(status)")

        # Volume processing table for tracking processed volumes
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS volume_processing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                volume_id TEXT UNIQUE NOT NULL,
                radar_name TEXT NOT NULL,
                strategy TEXT NOT NULL,
                vol_nr TEXT NOT NULL,
                observation_datetime TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                netcdf_path TEXT,
                processed_at TEXT,
                error_message TEXT,
                is_complete INTEGER DEFAULT 0,
                expected_fields TEXT,
                downloaded_fields TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """
        )

        # Index for faster queries on volume processing
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_volume_id ON volume_processing(volume_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_volume_radar_datetime ON "
            "volume_processing(radar_name, observation_datetime)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_volume_status ON volume_processing(status)")

        # Product generation table for tracking generated products (PNG, GeoTIFF, etc.)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS product_generation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                volume_id TEXT NOT NULL,
                product_type TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                generated_at TEXT,
                error_message TEXT,
                error_type TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(volume_id, product_type),
                FOREIGN KEY (volume_id) REFERENCES volume_processing(volume_id)
            )
        """
        )

        # Index for faster queries on product generation
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_volume_id ON product_generation(volume_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_status ON product_generation(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_type ON product_generation(product_type)")

        conn.commit()

        # Run migrations to add new columns if needed
        self._migrate_schema()

        # Log appropriate message based on database state
        if not db_exists:
            logger.info(f"Initialized new SQLite database at {self.db_path}")
        elif missing_tables:
            tables_list = ", ".join(sorted(missing_tables))
            if len(missing_tables) == 1:
                logger.info(
                    f"Connected to existing SQLite database at {self.db_path} - initializing table {tables_list}"
                )
            else:
                logger.info(
                    f"Connected to existing SQLite database at {self.db_path} - initializing tables {tables_list}"
                )
        else:
            logger.info(f"Connected to existing SQLite database at {self.db_path} - no new tables need to be created")

    def _migrate_schema(self) -> None:
        """
        Apply schema migrations for new columns.

        Adds cleanup-related columns to existing tables if they don't exist.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Check and add cleanup_status column to downloads table
        cursor.execute("PRAGMA table_info(downloads)")
        download_columns = {row[1] for row in cursor.fetchall()}

        if "cleanup_status" not in download_columns:
            cursor.execute("ALTER TABLE downloads ADD COLUMN cleanup_status TEXT DEFAULT 'active'")
            logger.info("Added cleanup_status column to downloads table")

        if "cleaned_at" not in download_columns:
            cursor.execute("ALTER TABLE downloads ADD COLUMN cleaned_at TEXT")
            logger.info("Added cleaned_at column to downloads table")

        # Check and add cleanup_status column to volume_processing table
        cursor.execute("PRAGMA table_info(volume_processing)")
        volume_columns = {row[1] for row in cursor.fetchall()}

        if "cleanup_status" not in volume_columns:
            cursor.execute("ALTER TABLE volume_processing ADD COLUMN cleanup_status TEXT DEFAULT 'active'")
            logger.info("Added cleanup_status column to volume_processing table")

        if "cleaned_at" not in volume_columns:
            cursor.execute("ALTER TABLE volume_processing ADD COLUMN cleaned_at TEXT")
            logger.info("Added cleaned_at column to volume_processing table")

        # Add indexes for cleanup queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_download_cleanup_status ON downloads(cleanup_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_volume_cleanup_status ON volume_processing(cleanup_status)")

        conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def is_downloaded(self, filename: str) -> bool:
        """
        Check if a file has been successfully downloaded.

        Args:
            filename: Name of the file to check

        Returns:
            True if file has been downloaded, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM downloads WHERE filename = ? AND status = 'completed'",
            (filename,),
        )
        return cursor.fetchone() is not None

    def mark_downloaded(
        self,
        filename: str,
        remote_path: str,
        local_path: Optional[str] = None,
        file_size: Optional[int] = None,
        checksum: Optional[str] = None,
        radar_name: Optional[str] = None,
        strategy: Optional[str] = None,
        vol_nr: Optional[str] = None,
        field_type: Optional[str] = None,
        observation_datetime: Optional[str] = None,
    ) -> None:
        """
        Mark a file as successfully downloaded.

        Args:
            filename: Name of the downloaded file
            remote_path: Full remote path where file was located
            local_path: Local path where file was saved
            file_size: Size of the file in bytes
            checksum: SHA256 checksum of the file
            metadata: Optional metadata (radar, field, timestamp, etc.)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO downloads
            (filename, remote_path, local_path, downloaded_at, file_size, checksum,
             radar_name, strategy, vol_nr, field_type, observation_datetime, status,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?)
        """,
            (
                filename,
                remote_path,
                local_path,
                now,
                file_size,
                checksum,
                radar_name,
                strategy,
                vol_nr,
                field_type,
                observation_datetime,
                now,
                now,
            ),
        )

        conn.commit()
        logger.debug(f"Marked '{filename}' as downloaded")

    def mark_failed(
        self,
        filename: str,
        remote_path: str,
        local_path: Optional[str] = None,
        file_size: Optional[int] = None,
        checksum: Optional[str] = None,
        radar_name: Optional[str] = None,
        strategy: Optional[str] = None,
        vol_nr: Optional[str] = None,
        field_type: Optional[str] = None,
        observation_datetime: Optional[str] = None,
    ) -> None:
        """
        Mark a file as failed to download.

        Args:
            filename: Name of the failed file
            remote_path: Full remote path where file was located
            local_path: Local path where file was saved
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO downloads
            (filename, remote_path, local_path, downloaded_at, file_size, checksum,
             radar_name, strategy, vol_nr, field_type, observation_datetime, status,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, ?)
        """,
            (
                filename,
                remote_path,
                local_path,
                now,
                file_size,
                checksum,
                radar_name,
                strategy,
                vol_nr,
                field_type,
                observation_datetime,
                now,
                now,
            ),
        )

        conn.commit()
        logger.debug(f"Marked '{filename}' as failed")

    def get_downloaded_files(self) -> Set[str]:
        """
        Get set of all successfully downloaded filenames.

        Returns:
            Set of filenames that have been downloaded
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT filename FROM downloads WHERE status = 'completed'")
        return {row[0] for row in cursor.fetchall()}

    def get_file_info(self, filename: str) -> Optional[Dict]:
        """
        Get information about a downloaded file.

        Args:
            filename: Name of the file

        Returns:
            Dictionary with download info, or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM downloads WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_files_by_date_range(
        self, start_date: datetime, end_date: datetime, radar_name: Optional[str] = None
    ) -> List[str]:
        """
        Get files downloaded within a date range.

        Args:
            start_date: Start of date range
            end_date: End of date range
            radar_name: Optional radar name to filter by

        Returns:
            List of filenames in the range
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()

        if radar_name:
            cursor.execute(
                """
                SELECT filename FROM downloads
                WHERE observation_datetime >= ? AND observation_datetime <= ?
                AND radar_name = ? AND status = 'completed'
                ORDER BY observation_datetime
            """,
                (start_iso, end_iso, radar_name),
            )
        else:
            cursor.execute(
                """
                SELECT filename FROM downloads
                WHERE observation_datetime >= ? AND observation_datetime <= ?
                AND status = 'completed'
                ORDER BY observation_datetime
            """,
                (start_iso, end_iso),
            )

        return [row[0] for row in cursor.fetchall()]

    def count(self, status: str = "completed") -> int:
        """
        Get total number of downloaded files.

        Args:
            status: Filter by status (default: 'completed')

        Returns:
            Count of files
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM downloads WHERE status = ?", (status,))
        return cursor.fetchone()[0]

    def remove_file(self, filename: str) -> None:
        """
        Remove a file from the state.

        Args:
            filename: Name of the file to remove
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM downloads WHERE filename = ?", (filename,))
        # cursor.execute("DELETE FROM partial_downloads WHERE filename = ?", (filename,))
        conn.commit()
        logger.debug(f"Removed '{filename}' from state")

    def clear(self, include_partials: bool = True) -> None:
        """
        Clear all state.

        Args:
            include_partials: Also clear partial downloads
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM downloads")
        conn.commit()
        logger.info("Cleared all state")

    @staticmethod
    def calculate_checksum(file_path: Path) -> str:
        """
        Calculate SHA256 checksum of a file.

        Args:
            file_path: Path to file

        Returns:
            Hexadecimal checksum string
        """
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def get_latest_downloaded_file(self, radar_name: Optional[str] = None) -> Optional[Dict]:
        """
        Get the downloaded BUFR file with the latest observation time.

        Args:
            radar_name: Optional filter by radar name. If None, returns latest across all radars.

        Returns:
            Dictionary with file info (filename, remote_path, local_path, observation_datetime, etc.)
            or None if no files found.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if radar_name:
            cursor.execute(
                """
                SELECT * FROM downloads
                WHERE status = 'completed' AND radar_name = ?
                ORDER BY observation_datetime DESC
                LIMIT 1
            """,
                (radar_name,),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM downloads
                WHERE status = 'completed'
                ORDER BY observation_datetime DESC
                LIMIT 1
            """
            )

        row = cursor.fetchone()
        return dict(row) if row else None

    # Volume processing methods

    def get_volume_id(self, radar_name: str, strategy: str, vol_nr: str, observation_datetime: str) -> str:
        """
        Generate unique volume identifier.

        Args:
            radar_name: Radar name (e.g., "RMA1")
            strategy: Volume strategy/code (e.g., "0315")
            vol_nr: Volume number (e.g., "01")
            observation_datetime: Observation datetime (ISO format)

        Returns:
            Unique volume identifier string
        """
        return f"{radar_name}_{strategy}_{vol_nr}_{observation_datetime}"

    def get_volume_info(self, volume_id: str) -> Optional[Dict]:
        """
        Get information about a volume.

        Args:
            volume_id: Volume identifier

        Returns:
            Dictionary with volume info, or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM volume_processing WHERE volume_id = ?", (volume_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def register_volume(
        self,
        volume_id: str,
        radar_name: str,
        strategy: str,
        vol_nr: str,
        observation_datetime: str,
        expected_fields: List[str],
        is_complete: bool,
    ) -> None:
        """
        Register a new volume in the database.

        Args:
            volume_id: Unique volume identifier
            radar_name: Radar name
            strategy: Volume strategy/code
            vol_nr: Volume number
            observation_datetime: Observation datetime (ISO format)
            expected_fields: List of expected field types
            is_complete: Whether volume is complete
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        # Convert list to comma-separated string
        expected_fields_str = ",".join(expected_fields)

        cursor.execute(
            """
            INSERT INTO volume_processing
            (volume_id, radar_name, strategy, vol_nr, observation_datetime,
             status, is_complete, expected_fields, downloaded_fields, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, '', ?, ?)
        """,
            (
                volume_id,
                radar_name,
                strategy,
                vol_nr,
                observation_datetime,
                1 if is_complete else 0,
                expected_fields_str,
                now,
                now,
            ),
        )

        conn.commit()
        logger.debug(f"Registered volume '{volume_id}' (complete={is_complete})")

    def update_volume_fields(self, volume_id: str, downloaded_fields: List[str], is_complete: bool) -> None:
        """
        Update volume fields and completion status.

        Args:
            volume_id: Volume identifier
            downloaded_fields: List of downloaded field types
            is_complete: Whether volume is complete
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        # Convert list to comma-separated string
        downloaded_fields_str = ",".join(downloaded_fields)

        cursor.execute(
            """
            UPDATE volume_processing
            SET downloaded_fields = ?, is_complete = ?, updated_at = ?
            WHERE volume_id = ?
        """,
            (downloaded_fields_str, 1 if is_complete else 0, now, volume_id),
        )

        conn.commit()
        logger.debug(f"Updated volume '{volume_id}' fields (complete={is_complete})")

    def mark_volume_processing(
        self, volume_id: str, status: str, netcdf_path: Optional[str] = None, error_message: Optional[str] = None
    ) -> None:
        """
        Mark volume processing status.

        Args:
            volume_id: Volume identifier
            status: Processing status ('pending', 'processing', 'completed', 'failed')
            netcdf_path: Optional path to generated NetCDF file
            error_message: Optional error message if failed
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        updates = ["status = ?", "updated_at = ?"]
        params = [status, now]

        if netcdf_path:
            updates.append("netcdf_path = ?")
            params.append(netcdf_path)

        if error_message:
            updates.append("error_message = ?")
            params.append(error_message)

        if status == "completed":
            updates.append("processed_at = ?")
            params.append(now)

        params.append(volume_id)

        cursor.execute(
            f"""
            UPDATE volume_processing
            SET {', '.join(updates)}
            WHERE volume_id = ?
        """,
            tuple(params),
        )

        conn.commit()
        logger.debug(f"Marked volume '{volume_id}' as {status}")

    def get_complete_unprocessed_volumes(self) -> List[Dict]:
        """
        Get complete volumes that haven't been processed yet.

        Returns:
            List of volume info dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE is_complete = 1 AND status = 'pending'
            ORDER BY observation_datetime ASC
        """
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_volume_files(self, radar_name: str, strategy: str, vol_nr: str, observation_datetime: str) -> List[Dict]:
        """
        Get all BUFR file information for a specific volume.

        Args:
            radar_name: Radar name
            strategy: Volume strategy/code
            vol_nr: Volume number
            observation_datetime: Observation datetime (ISO format)

        Returns:
            List of dictionaries with file information (filename, local_path, etc.)
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT filename, local_path, file_size, field_type FROM downloads
            WHERE radar_name = ? AND strategy = ? AND vol_nr = ?
            AND observation_datetime = ? AND status = 'completed'
        """,
            (radar_name, strategy, vol_nr, observation_datetime),
        )

        return [
            {
                "filename": row[0],
                "local_path": row[1],
                "file_size": row[2],
                "field_type": row[3],
            }
            for row in cursor.fetchall()
        ]

    def get_volumes_by_status(self, status: str) -> List[Dict]:
        """
        Get volumes by processing status.

        Args:
            status: Processing status to filter by

        Returns:
            List of volume info dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE status = ?
            ORDER BY observation_datetime DESC
        """,
            (status,),
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_latest_registered_volume_datetime(self, radar_name: str) -> Optional[str]:
        """
        Get the observation datetime of the latest registered volume for a radar.

        Args:
            radar_name: Radar name to filter by

        Returns:
            ISO format datetime string of latest volume, or None if no volumes exist
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT observation_datetime FROM volume_processing
            WHERE radar_name = ?
            ORDER BY observation_datetime DESC
            LIMIT 1
        """,
            (radar_name,),
        )

        row = cursor.fetchone()
        return row[0] if row else None

    def get_incomplete_volumes(self) -> List[Dict]:
        """ """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE is_complete = 0
            """
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_unprocessed_volumes(self) -> List[Dict]:
        """
        Get all volumes that haven't been processed yet (both complete and incomplete).

        Returns:
            List of volume info dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE status = 'pending'
            ORDER BY observation_datetime ASC
        """
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_stuck_volumes(self, timeout_minutes: int) -> List[Dict]:
        """
        Get volumes that have been in 'processing' status for longer than the timeout.

        These volumes are considered stuck and should be reset back to 'pending' for retry.

        Args:
            timeout_minutes: Timeout in minutes - volumes in 'processing' status longer than
                           this will be considered stuck

        Returns:
            List of volume info dictionaries that are stuck
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Calculate the cutoff time (timeout_minutes ago)
        now = datetime.now(timezone.utc)
        cutoff_time = now - timedelta(minutes=timeout_minutes)
        cutoff_iso = cutoff_time.isoformat()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE status = 'processing' AND updated_at < ?
            ORDER BY updated_at ASC
        """,
            (cutoff_iso,),
        )

        return [dict(row) for row in cursor.fetchall()]

    def reset_stuck_volumes(self, timeout_minutes: int) -> int:
        """
        Reset volumes that have been stuck in 'processing' status back to 'pending'.

        This allows stuck volumes to be retried. Updates their status and updated_at timestamp.

        Args:
            timeout_minutes: Timeout in minutes - volumes in 'processing' status longer than
                           this will be reset

        Returns:
            Number of volumes that were reset
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        # Calculate the cutoff time (timeout_minutes ago)
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        cutoff_iso = cutoff_time.isoformat()

        cursor.execute(
            """
            UPDATE volume_processing
            SET status = 'pending', updated_at = ?
            WHERE status = 'processing' AND updated_at < ?
        """,
            (now, cutoff_iso),
        )

        conn.commit()
        num_reset = cursor.rowcount
        if num_reset > 0:
            logger.info(f"Reset {num_reset} stuck volumes from 'processing' back to 'pending'")

        return num_reset

    # ==================================================================================
    # Product Generation Methods
    # ==================================================================================

    def register_product_generation(self, volume_id: str, product_type: str = "image") -> None:
        """
        Register a product generation task for a volume.

        Args:
            volume_id: Volume identifier
            product_type: Type of product ('image' for PNG/visualization, 'geotiff', etc.)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        try:
            cursor.execute(
                """
                INSERT INTO product_generation (volume_id, product_type, status, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?)
            """,
                (volume_id, product_type, now, now),
            )
            conn.commit()
            logger.debug(f"Registered {product_type} generation for volume {volume_id}")
        except sqlite3.IntegrityError:
            # Already exists, skip
            logger.debug(f"Product generation already registered for {volume_id}/{product_type}")

    def mark_product_status(
        self,
        volume_id: str,
        product_type: str,
        status: str,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
    ) -> None:
        """
        Mark product generation status for a volume.

        Args:
            volume_id: Volume identifier
            product_type: Type of product ('image', 'geotiff', etc.)
            status: Generation status ('pending', 'processing', 'completed', 'failed')
            error_message: Optional detailed error message if failed
            error_type: Optional short error type for categorization
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        if status == "completed":
            cursor.execute(
                """
                UPDATE product_generation
                SET status = ?, generated_at = ?, error_message = NULL, error_type = NULL, updated_at = ?
                WHERE volume_id = ? AND product_type = ?
            """,
                (status, now, now, volume_id, product_type),
            )
        elif status == "failed":
            cursor.execute(
                """
                UPDATE product_generation
                SET status = ?, error_message = ?, error_type = ?, updated_at = ?
                WHERE volume_id = ? AND product_type = ?
            """,
                (status, error_message, error_type, now, volume_id, product_type),
            )
        else:
            cursor.execute(
                """
                UPDATE product_generation
                SET status = ?, updated_at = ?
                WHERE volume_id = ? AND product_type = ?
            """,
                (status, now, volume_id, product_type),
            )

        conn.commit()
        logger.debug(f"Marked {product_type} for {volume_id} with status: {status}")

    def get_volumes_for_product_generation(self, product_type: str = "image") -> List[Dict]:
        """
        Get volumes that are ready for product generation.

        Returns volumes that:
        - Have status='completed' in volume_processing (NetCDF file generated)
        - Don't have a product_generation entry OR have status='pending' or 'failed'

        Args:
            product_type: Type of product to check ('image', 'geotiff', etc.)

        Returns:
            List of dictionaries with volume and product generation info
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                vp.*,
                pg.status as product_status,
                pg.error_message as product_error_message,
                pg.error_type as product_error_type
            FROM volume_processing vp
            LEFT JOIN product_generation pg
                ON vp.volume_id = pg.volume_id AND pg.product_type = ?
            WHERE vp.status = 'completed'
              AND (pg.status IS NULL OR pg.status = 'pending' OR pg.status = 'failed')
            ORDER BY vp.observation_datetime ASC
        """,
            (product_type,),
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_products_by_status(self, status: str, product_type: Optional[str] = None) -> List[Dict]:
        """
        Get all products with a specific status.

        Args:
            status: Status to filter by ('pending', 'processing', 'completed', 'failed')
            product_type: Optional product type filter ('image', 'geotiff', etc.)

        Returns:
            List of dictionaries with product generation info
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if product_type:
            cursor.execute(
                """
                SELECT pg.*, vp.radar_name, vp.observation_datetime, vp.netcdf_path
                FROM product_generation pg
                JOIN volume_processing vp ON pg.volume_id = vp.volume_id
                WHERE pg.status = ? AND pg.product_type = ?
                ORDER BY pg.updated_at DESC
            """,
                (status, product_type),
            )
        else:
            cursor.execute(
                """
                SELECT pg.*, vp.radar_name, vp.observation_datetime, vp.netcdf_path
                FROM product_generation pg
                JOIN volume_processing vp ON pg.volume_id = vp.volume_id
                WHERE pg.status = ?
                ORDER BY pg.updated_at DESC
            """,
                (status,),
            )

        return [dict(row) for row in cursor.fetchall()]

    def get_stuck_product_generations(self, timeout_minutes: int, product_type: str = "image") -> List[Dict]:
        """
        Get product generations stuck in 'processing' state.

        Args:
            timeout_minutes: Timeout in minutes
            product_type: Type of product to check

        Returns:
            List of stuck product generation dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        now = datetime.now(timezone.utc)
        cutoff_time = now - timedelta(minutes=timeout_minutes)
        cutoff_iso = cutoff_time.isoformat()

        cursor.execute(
            """
            SELECT * FROM product_generation
            WHERE status = 'processing' AND product_type = ? AND updated_at < ?
            ORDER BY updated_at ASC
        """,
            (product_type, cutoff_iso),
        )

        return [dict(row) for row in cursor.fetchall()]

    def reset_stuck_product_generations(self, timeout_minutes: int, product_type: str = "image") -> int:
        """
        Reset product generations that have been stuck in 'processing' status back to 'pending'.

        Args:
            timeout_minutes: Timeout in minutes
            product_type: Type of product to check

        Returns:
            Number of product generations that were reset
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        cutoff_iso = cutoff_time.isoformat()

        cursor.execute(
            """
            UPDATE product_generation
            SET status = 'pending', updated_at = ?
            WHERE status = 'processing' AND product_type = ? AND updated_at < ?
        """,
            (now, product_type, cutoff_iso),
        )

        conn.commit()
        num_reset = cursor.rowcount
        if num_reset > 0:
            logger.info(f"Reset {num_reset} stuck {product_type} generations from 'processing' back to 'pending'")

        return num_reset

    # ==================================================================================
    # Cleanup Methods - Metadata-Only Retention Policy
    # ==================================================================================

    def get_bufr_files_for_cleanup(
        self, retention_days: int, radar_name: Optional[str] = None, product_types: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Get BUFR files that are eligible for cleanup based on retention policy.

        Files are eligible for cleanup when:
        - They belong to a completed volume (NetCDF generated)
        - All specified product types have been successfully generated
        - They are older than the retention period
        - They haven't been cleaned up yet (cleanup_status = 'active')

        Args:
            retention_days: Minimum age in days before a file can be cleaned up
            radar_name: Optional filter by radar name
            product_types: List of product types that must be completed before cleanup.
                          If None, defaults to ['image'].

        Returns:
            List of dictionaries with file information ready for cleanup
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        cutoff_iso = cutoff_date.isoformat()

        if product_types is None:
            product_types = ["image"]

        # Build query for product type checks
        product_type_placeholders = ", ".join("?" for _ in product_types)
        product_type_count = len(product_types)

        if radar_name:
            cursor.execute(
                f"""
                SELECT d.*, vp.volume_id, vp.netcdf_path, vp.status as volume_status
                FROM downloads d
                INNER JOIN volume_processing vp
                    ON d.radar_name = vp.radar_name
                    AND d.strategy = vp.strategy
                    AND d.vol_nr = vp.vol_nr
                    AND d.observation_datetime = vp.observation_datetime
                WHERE d.status = 'completed'
                    AND d.radar_name = ?
                    AND (d.cleanup_status = 'active' OR d.cleanup_status IS NULL)
                    AND d.observation_datetime < ?
                    AND vp.status = 'completed'
                    AND (
                        SELECT COUNT(*)
                        FROM product_generation pg
                        WHERE pg.volume_id = vp.volume_id
                            AND pg.product_type IN ({product_type_placeholders})
                            AND pg.status = 'completed'
                    ) = ?
                ORDER BY d.observation_datetime ASC
            """,
                (radar_name, cutoff_iso, *product_types, product_type_count),
            )
        else:
            cursor.execute(
                f"""
                SELECT d.*, vp.volume_id, vp.netcdf_path, vp.status as volume_status
                FROM downloads d
                INNER JOIN volume_processing vp
                    ON d.radar_name = vp.radar_name
                    AND d.strategy = vp.strategy
                    AND d.vol_nr = vp.vol_nr
                    AND d.observation_datetime = vp.observation_datetime
                WHERE d.status = 'completed'
                    AND (d.cleanup_status = 'active' OR d.cleanup_status IS NULL)
                    AND d.observation_datetime < ?
                    AND vp.status = 'completed'
                    AND (
                        SELECT COUNT(*)
                        FROM product_generation pg
                        WHERE pg.volume_id = vp.volume_id
                            AND pg.product_type IN ({product_type_placeholders})
                            AND pg.status = 'completed'
                    ) = ?
                ORDER BY d.observation_datetime ASC
            """,
                (cutoff_iso, *product_types, product_type_count),
            )

        return [dict(row) for row in cursor.fetchall()]

    def get_netcdf_files_for_cleanup(
        self, retention_days: int, radar_name: Optional[str] = None, product_types: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Get NetCDF files (volumes) that are eligible for cleanup based on retention policy.

        Files are eligible for cleanup when:
        - All specified product types have been successfully generated
        - They are older than the retention period
        - They haven't been cleaned up yet (cleanup_status = 'active')

        Args:
            retention_days: Minimum age in days before a file can be cleaned up
            radar_name: Optional filter by radar name
            product_types: List of product types that must be completed before cleanup.
                          If None, defaults to ['image'].

        Returns:
            List of dictionaries with volume information ready for cleanup
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        cutoff_iso = cutoff_date.isoformat()

        if product_types is None:
            product_types = ["image"]

        # Build query for product type checks
        product_type_placeholders = ", ".join("?" for _ in product_types)
        product_type_count = len(product_types)

        if radar_name:
            cursor.execute(
                f"""
                SELECT vp.*
                FROM volume_processing vp
                WHERE vp.status = 'completed'
                    AND vp.radar_name = ?
                    AND (vp.cleanup_status = 'active' OR vp.cleanup_status IS NULL)
                    AND vp.observation_datetime < ?
                    AND vp.netcdf_path IS NOT NULL
                    AND (
                        SELECT COUNT(*)
                        FROM product_generation pg
                        WHERE pg.volume_id = vp.volume_id
                            AND pg.product_type IN ({product_type_placeholders})
                            AND pg.status = 'completed'
                    ) = ?
                ORDER BY vp.observation_datetime ASC
            """,
                (radar_name, cutoff_iso, *product_types, product_type_count),
            )
        else:
            cursor.execute(
                f"""
                SELECT vp.*
                FROM volume_processing vp
                WHERE vp.status = 'completed'
                    AND (vp.cleanup_status = 'active' OR vp.cleanup_status IS NULL)
                    AND vp.observation_datetime < ?
                    AND vp.netcdf_path IS NOT NULL
                    AND (
                        SELECT COUNT(*)
                        FROM product_generation pg
                        WHERE pg.volume_id = vp.volume_id
                            AND pg.product_type IN ({product_type_placeholders})
                            AND pg.status = 'completed'
                    ) = ?
                ORDER BY vp.observation_datetime ASC
            """,
                (cutoff_iso, *product_types, product_type_count),
            )

        return [dict(row) for row in cursor.fetchall()]

    def get_incomplete_volumes_fields(self, volume_info) -> List[Dict]:
        """ """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT filename, local_path, remote_path, file_size, field_type FROM downloads
            WHERE radar_name = ? AND strategy = ? AND vol_nr = ?
            AND observation_datetime = ? AND status = 'completed'
        """,
            (
                volume_info["radar_name"],
                volume_info["strategy"],
                volume_info["vol_nr"],
                volume_info["observation_datetime"],
            ),
        )

        return [
            {
                "filename": row[0],
                "local_path": row[1],
                "remote_path": row[2],
                "file_size": row[3],
                "field_type": row[4],
            }
            for row in cursor.fetchall()
        ]

    def mark_bufr_cleanup_status(self, filename: str, cleanup_status: str, error_message: Optional[str] = None) -> bool:
        """
        Mark a BUFR file's cleanup status.

        Args:
            filename: Name of the file
            cleanup_status: New status ('active', 'pending_cleanup', 'cleaned', 'cleanup_failed')
            error_message: Optional error message if cleanup failed

        Returns:
            True if the update was successful, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        if cleanup_status == "cleaned":
            cursor.execute(
                """
                UPDATE downloads
                SET cleanup_status = ?, cleaned_at = ?, updated_at = ?
                WHERE filename = ?
            """,
                (cleanup_status, now, now, filename),
            )
        else:
            cursor.execute(
                """
                UPDATE downloads
                SET cleanup_status = ?, updated_at = ?
                WHERE filename = ?
            """,
                (cleanup_status, now, filename),
            )

        conn.commit()
        success = cursor.rowcount > 0
        if success:
            logger.debug(f"Marked BUFR file '{filename}' cleanup_status as {cleanup_status}")
        return success

    def mark_netcdf_cleanup_status(
        self, volume_id: str, cleanup_status: str, error_message: Optional[str] = None
    ) -> bool:
        """
        Mark a NetCDF volume's cleanup status.

        Args:
            volume_id: Volume identifier
            cleanup_status: New status ('active', 'pending_cleanup', 'cleaned', 'cleanup_failed')
            error_message: Optional error message if cleanup failed

        Returns:
            True if the update was successful, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        if cleanup_status == "cleaned":
            cursor.execute(
                """
                UPDATE volume_processing
                SET cleanup_status = ?, cleaned_at = ?, updated_at = ?
                WHERE volume_id = ?
            """,
                (cleanup_status, now, now, volume_id),
            )
        else:
            cursor.execute(
                """
                UPDATE volume_processing
                SET cleanup_status = ?, updated_at = ?
                WHERE volume_id = ?
            """,
                (cleanup_status, now, volume_id),
            )

        conn.commit()
        success = cursor.rowcount > 0
        if success:
            logger.debug(f"Marked NetCDF volume '{volume_id}' cleanup_status as {cleanup_status}")
        return success

    def delete_file_safely(self, file_path: str) -> bool:
        """
        Safely delete a file from disk.

        Args:
            file_path: Path to the file to delete

        Returns:
            True if file was deleted or doesn't exist, False on error
        """
        try:
            path = Path(file_path)
            if path.exists():
                path.unlink()
                logger.debug(f"Deleted file: {file_path}")
            else:
                logger.debug(f"File already deleted or doesn't exist: {file_path}")
            return True
        except OSError as e:
            logger.error(f"Failed to delete file {file_path}: {e}")
            return False

    def cleanup_bufr_file(self, filename: str) -> bool:
        """
        Clean up a single BUFR file (delete from disk, keep metadata).

        This is an atomic operation: marks file as pending, deletes it,
        then marks as cleaned. On failure, rolls back to active status.

        Args:
            filename: Name of the file to cleanup

        Returns:
            True if cleanup was successful, False otherwise
        """
        # Get file info first
        file_info = self.get_file_info(filename)
        if not file_info:
            logger.warning(f"Cannot cleanup BUFR file '{filename}': not found in database")
            return False

        local_path = file_info.get("local_path")
        if not local_path:
            logger.warning(f"Cannot cleanup BUFR file '{filename}': no local path recorded")
            return False

        # Mark as pending cleanup
        self.mark_bufr_cleanup_status(filename, "pending_cleanup")

        # Attempt to delete file
        if self.delete_file_safely(local_path):
            # Success - mark as cleaned
            self.mark_bufr_cleanup_status(filename, "cleaned")
            logger.info(f"Successfully cleaned up BUFR file '{filename}'")
            return True
        else:
            # Failure - rollback to active
            self.mark_bufr_cleanup_status(filename, "active", "Failed to delete file from disk")
            logger.error(f"Failed to cleanup BUFR file '{filename}'")
            return False

    def cleanup_netcdf_file(self, volume_id: str) -> bool:
        """
        Clean up a single NetCDF file (delete from disk, keep metadata).

        This is an atomic operation: marks volume as pending, deletes it,
        then marks as cleaned. On failure, rolls back to active status.

        Args:
            volume_id: Volume identifier

        Returns:
            True if cleanup was successful, False otherwise
        """
        # Get volume info first
        volume_info = self.get_volume_info(volume_id)
        if not volume_info:
            logger.warning(f"Cannot cleanup NetCDF volume '{volume_id}': not found in database")
            return False

        netcdf_path = volume_info.get("netcdf_path")
        if not netcdf_path:
            logger.warning(f"Cannot cleanup NetCDF volume '{volume_id}': no netcdf path recorded")
            return False

        # Mark as pending cleanup
        self.mark_netcdf_cleanup_status(volume_id, "pending_cleanup")

        # Attempt to delete file
        if self.delete_file_safely(netcdf_path):
            # Success - mark as cleaned
            self.mark_netcdf_cleanup_status(volume_id, "cleaned")
            logger.info(f"Successfully cleaned up NetCDF volume '{volume_id}'")
            return True
        else:
            # Failure - rollback to active
            self.mark_netcdf_cleanup_status(volume_id, "active", "Failed to delete file from disk")
            logger.error(f"Failed to cleanup NetCDF volume '{volume_id}'")
            return False

    def get_cleanup_stats(self, radar_name: Optional[str] = None) -> Dict:
        """
        Get cleanup statistics.

        Args:
            radar_name: Optional filter by radar name

        Returns:
            Dictionary with cleanup statistics
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        stats = {
            "bufr_active": 0,
            "bufr_cleaned": 0,
            "bufr_pending": 0,
            "netcdf_active": 0,
            "netcdf_cleaned": 0,
            "netcdf_pending": 0,
        }

        # BUFR stats
        if radar_name:
            cursor.execute(
                """
                SELECT cleanup_status, COUNT(*) as count
                FROM downloads
                WHERE radar_name = ?
                GROUP BY cleanup_status
            """,
                (radar_name,),
            )
        else:
            cursor.execute(
                """
                SELECT cleanup_status, COUNT(*) as count
                FROM downloads
                GROUP BY cleanup_status
            """
            )

        for row in cursor.fetchall():
            status = row[0] or "active"
            count = row[1]
            if status == "active":
                stats["bufr_active"] = count
            elif status == "cleaned":
                stats["bufr_cleaned"] = count
            elif status == "pending_cleanup":
                stats["bufr_pending"] = count

        # NetCDF stats
        if radar_name:
            cursor.execute(
                """
                SELECT cleanup_status, COUNT(*) as count
                FROM volume_processing
                WHERE radar_name = ?
                GROUP BY cleanup_status
            """,
                (radar_name,),
            )
        else:
            cursor.execute(
                """
                SELECT cleanup_status, COUNT(*) as count
                FROM volume_processing
                GROUP BY cleanup_status
            """
            )

        for row in cursor.fetchall():
            status = row[0] or "active"
            count = row[1]
            if status == "active":
                stats["netcdf_active"] = count
            elif status == "cleaned":
                stats["netcdf_cleaned"] = count
            elif status == "pending_cleanup":
                stats["netcdf_pending"] = count

        return stats

    def can_redownload_bufr(self, filename: str) -> Optional[Dict]:
        """
        Check if a cleaned BUFR file can be re-downloaded and get its remote path.

        This enables lazy re-download from FTP when needed.

        Args:
            filename: Name of the file to check

        Returns:
            Dictionary with remote_path and metadata if file can be re-downloaded,
            None otherwise
        """
        file_info = self.get_file_info(filename)
        if not file_info:
            return None

        if file_info.get("cleanup_status") != "cleaned":
            return None

        return {
            "filename": file_info["filename"],
            "remote_path": file_info["remote_path"],
            "radar_name": file_info.get("radar_name"),
            "observation_datetime": file_info.get("observation_datetime"),
            "checksum": file_info.get("checksum"),
        }
