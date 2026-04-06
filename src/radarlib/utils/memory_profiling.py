"""
Memory profiling utilities for radarlib.

Provides lightweight memory tracking tools using psutil and tracemalloc
to help diagnose memory leaks in long-running daemons.
"""

import gc
import logging
import tracemalloc
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

try:
    if TYPE_CHECKING:
        import psutil
except ImportError:
    psutil = None  # type: ignore

logger = logging.getLogger(__name__)


def log_memory_usage(label: str, process: Optional[object] = None) -> None:
    """
    Log current memory usage (RSS) and tracked allocations.

    Args:
        label: Descriptive label for the measurement point
        process: psutil.Process instance (optional, will create if needed)
    """
    if psutil is None:
        logger.warning("psutil not available, skipping memory logging")
        return

    if process is None:
        process = psutil.Process()

    mem_info = process.memory_info()  # type: ignore
    rss_mb = mem_info.rss / (1024 * 1024)

    # Get tracemalloc stats if enabled
    if tracemalloc.is_tracing():
        current, peak = tracemalloc.get_traced_memory()
        current_mb = current / (1024 * 1024)
        peak_mb = peak / (1024 * 1024)
        logger.info(
            f"[MEMORY] {label} | RSS: {rss_mb:.1f} MB | " f"Traced: {current_mb:.1f} MB | Peak: {peak_mb:.1f} MB"
        )
    else:
        logger.info(f"[MEMORY] {label} | RSS: {rss_mb:.1f} MB")


@contextmanager
def track_memory(label: str, gc_collect: bool = False):
    """
    Context manager to track memory usage before/after a code block.

    Usage:
        with track_memory("processing volume"):
            process_radar_volume()

    Args:
        label: Descriptive label for the operation
        gc_collect: If True, run gc.collect() after the block
    """
    if psutil is None:
        logger.warning("psutil not available, skipping memory tracking")
        yield
        return

    process = psutil.Process()
    mem_before = process.memory_info().rss / (1024 * 1024)

    try:
        yield
    finally:
        if gc_collect:
            gc.collect()

        mem_after = process.memory_info().rss / (1024 * 1024)
        delta = mem_after - mem_before

        logger.info(
            f"[MEMORY DELTA] {label} | Before: {mem_before:.1f} MB | "
            f"After: {mem_after:.1f} MB | Delta: {delta:+.1f} MB"
        )


def check_and_cleanup_memory(threshold_mb: float = 1200.0, label: str = "") -> None:
    """
    Check current RSS memory usage and trigger garbage collection if threshold exceeded.

    This function helps prevent memory accumulation in long-running daemons by
    proactively triggering garbage collection when memory usage grows too high.

    Args:
        threshold_mb: Memory threshold in MB. If exceeded, force gc.collect().
        label: Description label for logging.

    Example:
        # After processing a batch of radar fields
        check_and_cleanup_memory(threshold_mb=1100.0, label="After field processing")
    """
    if psutil is None:
        logger.warning("psutil not available, skipping memory check")
        return

    process = psutil.Process()
    rss_memory_mb = process.memory_info().rss / (1024 * 1024)

    logger.info(f"[MEMORY_CHECK] {label}: RSS = {rss_memory_mb:.1f} MB")

    if rss_memory_mb > threshold_mb:
        logger.warning(f"[MEMORY_WARNING] Memory threshold exceeded: {rss_memory_mb:.1f} MB > {threshold_mb:.1f} MB")
        logger.debug("Triggering aggressive garbage collection...")
        collected = gc.collect()
        logger.debug(f"Garbage collection: {collected} objects freed")

        rss_after = process.memory_info().rss / (1024 * 1024)
        logger.info(f"[MEMORY_AFTER_GC] {label}: RSS = {rss_after:.1f} MB (freed {rss_memory_mb - rss_after:.1f} MB)")


def aggressive_cleanup(label: str = "") -> None:
    """
    Perform an aggressive, multi-pass garbage collection cycle.

    Runs gc.collect() three times to handle multi-generational reference cycles
    and logs RSS before/after. Use this at daemon cycle boundaries or after
    processing a complete radar volume.

    Three passes are needed because:
    - Pass 1: collects objects with no more references
    - Pass 2: collects objects freed by pass 1 (reference cycles)
    - Pass 3: final sweep to catch any stragglers

    Args:
        label: Descriptive label for the cleanup point (logged for traceability)

    Example:
        from radarlib.utils.memory_profiling import aggressive_cleanup

        # At end of each processing cycle
        aggressive_cleanup("End of volume processing cycle")
    """
    if psutil is None:
        gc.collect()
        gc.collect()
        gc.collect()
        return

    process = psutil.Process()
    rss_before = process.memory_info().rss / (1024 * 1024)

    prefix = f" [{label}]" if label else ""
    logger.debug(f"[AGGRESSIVE_GC]{prefix} Starting 3-pass gc — RSS before: {rss_before:.1f} MB")

    collected_total = 0
    for pass_num in range(1, 4):
        collected = gc.collect()
        collected_total += collected
        logger.debug(f"[AGGRESSIVE_GC]{prefix} Pass {pass_num}: freed {collected} objects")

    rss_after = process.memory_info().rss / (1024 * 1024)
    freed = rss_before - rss_after
    logger.info(
        f"[AGGRESSIVE_GC]{prefix} Done — RSS: {rss_before:.1f} MB → {rss_after:.1f} MB "
        f"({freed:+.1f} MB) | objects freed: {collected_total}"
    )
