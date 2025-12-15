from __future__ import annotations

import time
import traceback
from typing import Any

from dmq.events import QTaskCompleted, QTaskFailed, QTaskRetry, QTaskStarted
from dmq.events.core import QTaskNotFound
from dmq.types import TaskMessage


def _task_started_event(message: TaskMessage, start_time: float, worker_id: str) -> QTaskStarted:
    return QTaskStarted(task_id=message.task_id, task_name=message.task_name, timestamp=start_time, worker_id=worker_id)


def _task_completed_event(message: TaskMessage, duration: float, result: Any) -> QTaskCompleted:
    return QTaskCompleted(
        task_id=message.task_id, task_name=message.task_name, timestamp=time.time(), duration=duration, result=result
    )


def _task_failure_event(message: TaskMessage, exception: Exception) -> QTaskFailed:
    return QTaskFailed(
        task_id=message.task_id,
        task_name=message.task_name,
        timestamp=time.time(),
        exception=str(exception),
        traceback="\n".join(traceback.format_exception(exception.__class__, exception, exception.__traceback__)),
        retry_count=message.retry_count,
    )


def _task_retry_event(message: TaskMessage) -> QTaskRetry:
    return QTaskRetry(
        task_id=message.task_id,
        task_name=message.task_name,
        timestamp=time.time(),
        retry_count=message.retry_count + 1,
        max_retries=message.max_retries,
        delay=2.0**message.retry_count,
    )


def _task_not_found_event(message: TaskMessage) -> QTaskNotFound:
    return QTaskNotFound(task_id=message.task_id, task_name=message.task_name, timestamp=time.time())
