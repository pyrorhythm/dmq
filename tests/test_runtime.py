from __future__ import annotations

from dmq.execution_mode import ExecutionMode
from dmq.runtime import detect_execution_mode, get_recommended_worker_count, is_free_threaded_build


def test_is_free_threaded_build_returns_bool():
    result = is_free_threaded_build()
    assert isinstance(result, bool)


def test_detect_execution_mode_default():
    mode = detect_execution_mode()
    assert mode in (ExecutionMode.ASYNC_ONLY, ExecutionMode.THREADED)


def test_detect_execution_mode_forced():
    mode = detect_execution_mode(force_mode=ExecutionMode.THREADED)
    assert mode == ExecutionMode.THREADED

    mode = detect_execution_mode(force_mode=ExecutionMode.ASYNC_ONLY)
    assert mode == ExecutionMode.ASYNC_ONLY


def test_get_recommended_worker_count_requested():
    assert get_recommended_worker_count(ExecutionMode.ASYNC_ONLY, requested=16) == 16


def test_get_recommended_worker_count_auto():
    count = get_recommended_worker_count(ExecutionMode.ASYNC_ONLY)
    assert count > 0

    count = get_recommended_worker_count(ExecutionMode.THREADED)
    assert count > 0


def test_execution_mode_values():
    assert ExecutionMode.ASYNC_ONLY == "async_only"
    assert ExecutionMode.THREADED == "threaded"
