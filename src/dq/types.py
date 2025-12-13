from __future__ import annotations

from datetime import datetime
from typing import Any

import msgspec


class DelaySchedule(msgspec.Struct, frozen=True):
    delay_seconds: float


class ETASchedule(msgspec.Struct, frozen=True):
    eta: datetime


class CronSchedule(msgspec.Struct, frozen=True):
    cron_expr: str


type Schedule = DelaySchedule | ETASchedule | CronSchedule


class TaskMessage(msgspec.Struct, frozen=True):
    task_id: str
    task_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    options: dict[str, Any]
    workflow_context: dict[str, Any] | None = None
    retry_count: int = 0
    max_retries: int = 3
