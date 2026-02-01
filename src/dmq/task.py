from __future__ import annotations

import contextvars
import sys
import time
from collections.abc import Callable, Coroutine
from datetime import datetime
from typing import TYPE_CHECKING, Any

from loguru import logger
from ulid import ulid

from .events import QEventType, QTaskQueued
from .types import CronSchedule, DelaySchedule, ETASchedule, QInProgressTask
from .user_event import UserEventEmitter
from .utils import await_if_async

if TYPE_CHECKING:
    from .manager import QManager

_current_emitter: contextvars.ContextVar[UserEventEmitter | None] = contextvars.ContextVar(
    "current_emitter", default=None
)


class QTask[**P, T]:
    def __init__(
        self,
        original_func: Callable[P, T | Coroutine[Any, Any, T]],
        task_name: str,
        task_kws: dict,
        manager: QManager,
        return_type: type[T] | None = None,
    ) -> None:
        self.original_func = original_func
        self.task_name: str = task_name
        self.task_labels: dict[str, Any] = task_kws
        self.manager: QManager = manager
        self.return_type: type[T] | None = return_type

        new_name = f"{self.original_func.__name__}_org_dq"
        self.original_func.__name__ = new_name
        if hasattr(self.original_func, "__qualname__"):
            original_qualname = self.original_func.__qualname__.rsplit(".")
            original_qualname[-1] = new_name
            new_qualname = ".".join(original_qualname)
            self.original_func.__qualname__ = new_qualname
        setattr(sys.modules[original_func.__module__], new_name, original_func)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        return await await_if_async(self.original_func(*args, **kwargs))  # pyrefly: ignore[ignore-param-spec]

    @staticmethod
    def e() -> UserEventEmitter:
        emitter = _current_emitter.get()
        if emitter is None:
            raise RuntimeError("emit() can only be called from within a task execution context")
        return emitter

    @staticmethod
    def set_emitter(emitter: UserEventEmitter | None) -> None:
        _current_emitter.set(emitter)

    async def q(self, *args: P.args, **kwargs: P.kwargs) -> QInProgressTask:
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
        *args: P.args,
        **kwargs: P.kwargs,
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
