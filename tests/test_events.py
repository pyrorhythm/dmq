from __future__ import annotations

from dmq.events.core import QEventType
from dmq.events.shortcuts import (
    _task_completed_event,
    _task_failure_event,
    _task_not_found_event,
    _task_retry_event,
    _task_started_event,
)
from dmq.types import TaskMessage


def _make_message(**overrides):
    defaults = {
        "task_id": "t1",
        "task_name": "test.task",
        "args": (),
        "kwargs": {},
        "options": {},
        "retry_count": 0,
        "max_retries": 3,
    }
    defaults.update(overrides)
    return TaskMessage(**defaults)


def test_task_started_event():
    msg = _make_message()
    event = _task_started_event(msg, 100.0, "w1")
    assert event.event_type == QEventType.TASK_STARTED
    assert event.task_id == "t1"
    assert event.worker_id == "w1"
    assert event.timestamp == 100.0


def test_task_completed_event():
    msg = _make_message()
    event = _task_completed_event(msg, 1.5, "result_val")
    assert event.event_type == QEventType.TASK_COMPLETED
    assert event.duration == 1.5
    assert event.result == "result_val"


def test_task_failure_event():
    msg = _make_message(retry_count=1)
    exc = ValueError("something broke")
    event = _task_failure_event(msg, exc)
    assert event.event_type == QEventType.TASK_FAILED
    assert "something broke" in event.exception
    assert event.retry_count == 1


def test_task_retry_event():
    msg = _make_message(retry_count=1, max_retries=5)
    event = _task_retry_event(msg)
    assert event.event_type == QEventType.TASK_RETRY
    assert event.retry_count == 2  # incremented
    assert event.max_retries == 5


def test_task_not_found_event():
    msg = _make_message()
    event = _task_not_found_event(msg)
    assert event.task_name == "test.task"


def test_event_types():
    assert QEventType.TASK_QUEUED == "task.queued"
    assert QEventType.TASK_STARTED == "task.started"
    assert QEventType.TASK_COMPLETED == "task.completed"
    assert QEventType.TASK_FAILED == "task.failed"
    assert QEventType.TASK_RETRY == "task.retry"
