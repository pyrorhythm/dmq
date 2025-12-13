from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .events import QEvent

if TYPE_CHECKING:
    from .callback import Callback


class EventRouter:
    def __init__(self) -> None:
        self._callbacks: list[Callback] = []
        self._event_queue: asyncio.Queue[QEvent] = asyncio.Queue()
        self._listener_task: asyncio.Task | None = None

    def register_callback(self, callback: Callback) -> None:
        self._callbacks.append(callback)

    def unregister_callback(self, callback: Callback) -> None:
        self._callbacks.remove(callback)

    async def emit(self, event: QEvent) -> None:
        await self._event_queue.put(event)

        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._process_events())

    async def _process_events(self) -> None:
        while not self._event_queue.empty():
            event = await self._event_queue.get()
            await self._route_event(event)

    async def _route_event(self, event: QEvent) -> None:
        tasks = []
        for callback in self._callbacks:
            if callback.should_handle(event):
                tasks.append(asyncio.create_task(callback.handle(event)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        if self._listener_task and not self._listener_task.done():
            await self._listener_task
