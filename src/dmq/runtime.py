"""Runtime detection for execution mode selection."""

from __future__ import annotations

import os
import sys

from loguru import logger

from .execution_mode import ExecutionMode


def is_free_threaded_build() -> bool:
    if not hasattr(sys, "_is_gil_enabled"):
        return False

    try:
        return not sys._is_gil_enabled()
    except Exception as e:
        logger.warning("failed to check gil status: {}, assuming standard build", e)
        return False


def detect_execution_mode(force_mode: ExecutionMode | None = None) -> ExecutionMode:
    if force_mode is not None:
        logger.info("execution mode forced to: {}", force_mode)
        return force_mode

    if is_free_threaded_build():
        logger.info("free-threaded python detected, using threaded execution mode")
        return ExecutionMode.THREADED

    logger.info("standard python detected, using async_only execution mode")
    return ExecutionMode.ASYNC_ONLY


def get_recommended_worker_count(mode: ExecutionMode, requested: int | None = None) -> int:
    if requested is not None:
        return requested

    cpu_count = os.cpu_count() or 4

    if mode == ExecutionMode.THREADED:
        return cpu_count

    return min(cpu_count * 2, 8)
