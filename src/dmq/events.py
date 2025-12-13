from __future__ import annotations

from enum import Enum
from typing import Any

import msgspec


class QEventType(str, Enum):
    TASK_QUEUED = "task.queued"
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_RETRY = "task.retry"
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_STEP_COMPLETED = "workflow.step_completed"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"


class QEventBase(msgspec.Struct, frozen=True):
    event_type: QEventType
    task_id: str
    task_name: str
    timestamp: float
    metadata: dict[str, Any] = {}


class QTaskQueued(msgspec.Struct, frozen=True):
    event_type: QEventType
    task_id: str
    task_name: str
    timestamp: float
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class QTaskStarted(msgspec.Struct, frozen=True):
    event_type: QEventType
    task_id: str
    task_name: str
    timestamp: float
    worker_id: str | None = None


class QTaskCompleted(msgspec.Struct, frozen=True):
    event_type: QEventType
    task_id: str
    task_name: str
    timestamp: float
    duration: float
    result: Any = None


class QTaskFailed(msgspec.Struct, frozen=True):
    event_type: QEventType
    task_id: str
    task_name: str
    timestamp: float
    exception: str
    traceback: str
    retry_count: int


class QTaskRetry(msgspec.Struct, frozen=True):
    event_type: QEventType
    task_id: str
    task_name: str
    timestamp: float
    retry_count: int
    max_retries: int
    delay: float


type QEvent = QEventBase | QTaskQueued | QTaskStarted | QTaskCompleted | QTaskFailed | QTaskRetry
