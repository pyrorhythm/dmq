from __future__ import annotations

import time
from typing import Any

import msgspec


class UserEvent(msgspec.Struct, frozen=True):
    event_name: str
    task_id: str
    task_name: str
    timestamp: float
    data: dict[str, Any] = {}


class UserEventEmitter:
    def __init__(self, task_id: str, task_name: str, event_router) -> None:
        self._task_id = task_id
        self._task_name = task_name
        self._event_router = event_router

    async def emit(self, event_name: str, **data: Any) -> None:
        event = UserEvent(
            event_name=event_name, task_id=self._task_id, task_name=self._task_name, timestamp=time.time(), data=data
        )
        await self._event_router.emit(event)
