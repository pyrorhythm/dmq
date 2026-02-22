from __future__ import annotations

from enum import Enum
from typing import Any

import msgspec


class QEventType(str, Enum):
	UNKNOWN = "unknown"

	TASK_QUEUED = "task.queued"
	TASK_STARTED = "task.started"
	TASK_COMPLETED = "task.completed"
	TASK_FAILED = "task.failed"
	TASK_RETRY = "task.retry"
	TASK_NOT_FOUND = "task.not_found"
	WORKFLOW_STARTED = "workflow.started"
	WORKFLOW_STEP_COMPLETED = "workflow.step_completed"
	WORKFLOW_COMPLETED = "workflow.completed"
	WORKFLOW_FAILED = "workflow.failed"


class QEventBase(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	timestamp: float
	metadata: dict[str, Any] = {}

	event_type: QEventType = QEventType.UNKNOWN


class QTaskQueued(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	timestamp: float
	args: tuple[Any, ...]
	kwargs: dict[str, Any]

	event_type: QEventType = QEventType.TASK_QUEUED


class QTaskStarted(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	timestamp: float
	worker_id: str | None = None

	event_type: QEventType = QEventType.TASK_STARTED


class QTaskCompleted(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	timestamp: float
	duration: float
	result: Any = None

	event_type: QEventType = QEventType.TASK_COMPLETED


class QTaskFailed(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	timestamp: float
	exception: str
	traceback: str
	retry_count: int

	event_type: QEventType = QEventType.TASK_FAILED


class QTaskRetry(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	timestamp: float
	retry_count: int
	max_retries: int
	delay: float

	event_type: QEventType = QEventType.TASK_RETRY


class QTaskNotFound(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	timestamp: float

	event_type: QEventType = QEventType.TASK_NOT_FOUND


class QWorkflowStarted(msgspec.Struct, frozen=True):
	workflow_id: str
	workflow_name: str
	timestamp: float
	event_type: QEventType = QEventType.WORKFLOW_STARTED


class QWorkflowStepCompleted(msgspec.Struct, frozen=True):
	workflow_id: str
	workflow_name: str
	step_index: int
	task_id: str
	timestamp: float
	result: Any = None
	event_type: QEventType = QEventType.WORKFLOW_STEP_COMPLETED


class QWorkflowCompleted(msgspec.Struct, frozen=True):
	workflow_id: str
	workflow_name: str
	timestamp: float
	result: Any = None
	event_type: QEventType = QEventType.WORKFLOW_COMPLETED


class QWorkflowFailed(msgspec.Struct, frozen=True):
	workflow_id: str
	workflow_name: str
	timestamp: float
	error: str
	step_index: int
	event_type: QEventType = QEventType.WORKFLOW_FAILED


type QEvent = (
	QEventBase
	| QTaskQueued
	| QTaskStarted
	| QTaskCompleted
	| QTaskFailed
	| QTaskRetry
	| QTaskNotFound
	| QWorkflowStarted
	| QWorkflowStepCompleted
	| QWorkflowCompleted
	| QWorkflowFailed
)
