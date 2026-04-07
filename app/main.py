#!/usr/bin/env python3
"""
Genpro25 - Radar Data Processing and Product Generation Service

This service handles:
- BUFR file processing
- NetCDF product generation
- Radar image creation
"""
import asyncio
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import config

from radarlib.daemons import DaemonManager, DaemonManagerConfig


def main():
    """
    Main entry point for the Genpro25 radar data processing service.
    """
    radar_name = os.getenv("RADAR_NAME", "RMA2")

    ################################################################
    # LOGGING SETUP
    # Ensure every LogRecord has a 'radar' attribute so "%(radar)s" in the format won't KeyError.
    # setLogRecordFactory must be registered BEFORE any handler or formatter is attached so that
    # all records — including those emitted during handler construction — carry the 'radar' field.
    _old_factory = logging.getLogRecordFactory()

    def _record_factory(*args, **kwargs):
        record = _old_factory(*args, **kwargs)
        record.radar = radar_name
        return record

    logging.setLogRecordFactory(_record_factory)

    # handlers
    stream_handler = logging.StreamHandler()
    # ensure log directory exists before creating a FileHandler to avoid "No such file or directory"
    log_dir = Path(config.ROOT_LOGS_PATH) / radar_name  # type: ignore
    log_dir.mkdir(parents=True, exist_ok=True)

    # Rotate daily at midnight, keep 7 days of logs
    timed_handler = TimedRotatingFileHandler(
        filename=str(log_dir / "genpro25.log"),
        when="midnight",  # When to rotate
        interval=1,  # Rotate every 1 unit (day in this case)
        backupCount=7,  # Keep 7 backup files
        encoding="utf-8",
        utc=True,  # Use local time (True for UTC)
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(radar)s|%(levelname)s] %(module)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler.setFormatter(formatter)
    timed_handler.setFormatter(formatter)
    # Explicitly set the handler level so no records are silently dropped by the handler itself
    stream_handler.setLevel(logging.INFO)
    timed_handler.setLevel(logging.INFO)

    # Configure the root logger explicitly instead of using logging.basicConfig() which is a
    # no-op when any handler has already been attached to the root logger (e.g. by a module-level
    # basicConfig call inside an imported library).
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    root_logger.addHandler(stream_handler)
    root_logger.addHandler(timed_handler)

    # Obtain the module logger AFTER the root logger is fully configured so the record factory
    # and formatter are in place for all subsequent calls.
    logger = logging.getLogger(__name__)
    ################################################################

    logger.info("=" * 60)
    logger.info("Genpro25 Radar Data Processing Service Starting")
    logger.info("=" * 60)

    # Log startup memory baseline
    try:
        from radarlib.utils.memory_profiling import log_memory_usage

        log_memory_usage("Genpro25 service startup")
    except ImportError:
        pass

    # Define volume types
    volume_types = config.VOLUME_TYPES  # type: ignore

    base_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name))  # type: ignore

    # Create manager configuration
    manager_config = DaemonManagerConfig(
        radar_name=radar_name,
        base_path=base_path,
        ftp_host=config.FTP_HOST,  # type: ignore
        ftp_user=config.FTP_USER,  # type: ignore
        ftp_password=config.FTP_PASS,  # type: ignore
        ftp_base_path="/L2",
        volume_types=volume_types,
        start_date=config.START_DATE,
        download_poll_interval=config.DOWNLOAD_POLL_INTERVAL,  # type: ignore
        processing_poll_interval=config.PROCESSING_POLL_INTERVAL,  # type: ignore
        product_poll_interval=config.PRODUCT_POLL_INTERVAL,  # type: ignore
        enable_download_daemon=config.ENABLE_DOWNLOAD_DAEMON,  # type: ignore
        enable_processing_daemon=config.ENABLE_PROCESSING_DAEMON,  # type: ignore
        enable_product_daemon=config.ENABLE_PRODUCT_DAEMON,  # type: ignore
        product_dir=Path(os.path.join(config.ROOT_RADAR_PRODUCTS_PATH, radar_name)),  # type: ignore
        product_type=config.PRODUCT_TYPE,  # type: ignore
        add_colmax=config.ADD_COLMAX,  # type: ignore
        enable_cleanup_daemon=config.ENABLE_CLEANUP_DAEMON,  # type: ignore
        netcdf_retention_days=config.NETCDF_RETENTION_DAYS,  # type: ignore
        bufr_retention_days=config.BUFR_RETENTION_DAYS,  # type: ignore
        cleanup_poll_interval=config.CLEANUP_POLL_INTERVAL,  # type: ignore
    )

    # Create manager
    manager = DaemonManager(manager_config)

    logger.info("Starting daemon manager...")
    logger.info("Both download and processing daemons will start")
    logger.info("Press Ctrl+C to stop all daemons")

    try:
        asyncio.run(manager.start())
    except KeyboardInterrupt:
        logger.info("\n\nStopping daemons...")
        manager.stop()
        logger.info("All daemons stopped")

    # Log final memory state
    try:
        from radarlib.utils.memory_profiling import log_memory_usage

        log_memory_usage("Genpro25 service shutdown")
    except ImportError:
        pass

    # Show final status
    status = manager.get_status()
    logger.info("\n" + "=" * 60)
    logger.info("Final Status:")
    logger.info(f"  Radar: {status['radar_code']}")
    logger.info(f"  Base path: {status['base_path']}")
    logger.info("\n  Download daemon:")
    logger.info(f"    Enabled: {status['download_daemon']['enabled']}")
    logger.info(f"    Running: {status['download_daemon']['running']}")
    if status["download_daemon"]["stats"]:
        logger.info(f"    Files downloaded: {status['download_daemon']['stats']['total_downloaded']}")
    logger.info("\n  Processing daemon:")
    logger.info(f"    Enabled: {status['processing_daemon']['enabled']}")
    logger.info(f"    Running: {status['processing_daemon']['running']}")
    if status["processing_daemon"]["stats"]:
        logger.info(f"    Volumes processed: {status['processing_daemon']['stats']['volumes_processed']}")
    logger.info("=" * 60)

    # Flush and close all log handlers before exit so that buffered records in the
    # TimedRotatingFileHandler are written to disk even on a clean shutdown.
    logging.shutdown()


if __name__ == "__main__":
    main()
