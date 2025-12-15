from __future__ import annotations

import contextvars
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from loguru import logger
from ulid import ulid

from .events import QEventType, QTaskQueued
from .types import CronSchedule, DelaySchedule, ETASchedule, QInProgressTask
from .user_event import UserEventEmitter

if TYPE_CHECKING:
    from .manager import QManager

_current_emitter: contextvars.ContextVar[UserEventEmitter | None] = contextvars.ContextVar(
    "current_emitter", default=None
)


class QTask[**Param, ReturnType]:
    def __init__(
        self,
        original_func: Callable[Param, ReturnType | Awaitable[ReturnType]],
        task_name: str,
        task_kws: dict,
        manager: QManager,
        return_type: type[ReturnType] | None = None,
    ) -> None:
        self.original_func = original_func
        self.task_name: str = task_name
        self.task_labels: dict[str, Any] = task_kws
        self.manager: QManager = manager
        self.return_type: type[ReturnType] | None = return_type

        new_name = f"{self.original_func.__name__}_org_dq"  # ty: ignore
        self.original_func.__name__ = new_name  # ty: ignore
        if hasattr(self.original_func, "__qualname__"):
            original_qualname = self.original_func.__qualname__.rsplit(".")  # ty: ignore
            original_qualname[-1] = new_name
            new_qualname = ".".join(original_qualname)
            self.original_func.__qualname__ = new_qualname  # ty: ignore
        setattr(sys.modules[original_func.__module__], new_name, original_func)

    async def __call__(self, *args: Param.args, **kwargs: Param.kwargs) -> ReturnType:
        return await self.original_func(*args, **kwargs)  # type: ignore

    @staticmethod
    def e() -> UserEventEmitter:
        emitter = _current_emitter.get()
        if emitter is None:
            raise RuntimeError("emit() can only be called from within a task execution context")
        return emitter

    @staticmethod
    def set_emitter(emitter: UserEventEmitter | None) -> None:
        _current_emitter.set(emitter)

    async def q(self, *args: Param.args, **kwargs: Param.kwargs) -> QInProgressTask:
        task_id = str(ulid())
        options = self.task_labels.copy()

        event = QTaskQueued(
            event_type=QEventType.TASK_QUEUED,
            task_id=task_id,
            task_name=self.task_name,
            timestamp=time.time(),
            args=args,
            kwargs=kwargs,
        )
        await self.manager.event_router.emit(event)

        logger.info("{}", event)

        await self.manager.broker.send_task(
            task_name=self.task_name, args=args, kwargs=kwargs, options=options, task_id=task_id
        )

        logger.info("sent task")

        return QInProgressTask(task_id, self.manager)

    async def sched(
        self,
        delay: float | None = None,
        eta: datetime | None = None,
        cron: str | None = None,
        *args: Param.args,
        **kwargs: Param.kwargs,
    ) -> QInProgressTask:
        schedule_params = [delay, eta, cron]
        provided_count = sum(p is not None for p in schedule_params)

        if provided_count != 1:
            raise ValueError("exactly one schedule parameter (delay, eta, or cron) must be provided")

        task_id = str(ulid())
        options = self.task_labels.copy()

        if delay is not None:
            schedule = DelaySchedule(delay_seconds=delay)
        elif eta is not None:
            schedule = ETASchedule(eta=eta)
        else:  # cron is not None
            schedule = CronSchedule(cron_expr=cron)  # type: ignore

        await self.manager.broker.send_scheduled_task(
            task_name=self.task_name, args=args, kwargs=kwargs, schedule=schedule, options=options, task_id=task_id
        )

        return QInProgressTask(task_id, self.manager)
