from __future__ import annotations

import asyncio
import weakref
from typing import TYPE_CHECKING

from .events import QEvent

if TYPE_CHECKING:
    from .callback import Callback


class EventRouter:
    def __init__(self) -> None:
        self._callbacks: list[Callback] = []
        self._event_queues: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Queue[QEvent]] = (
            weakref.WeakKeyDictionary()
        )
        self._listener_tasks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Task] = (
            weakref.WeakKeyDictionary()
        )

    def _get_queue(self) -> asyncio.Queue[QEvent]:
        """Get event queue for the current event loop."""
        loop = asyncio.get_running_loop()
        if loop not in self._event_queues:
            self._event_queues[loop] = asyncio.Queue()
        return self._event_queues[loop]

    def _get_listener_task(self) -> asyncio.Task | None:
        loop = asyncio.get_running_loop()
        return self._listener_tasks.get(loop)

    def _set_listener_task(self, task: asyncio.Task) -> None:
        loop = asyncio.get_running_loop()
        self._listener_tasks[loop] = task

    def register_callback(self, callback: Callback) -> None:
        self._callbacks.append(callback)

    def unregister_callback(self, callback: Callback) -> None:
        self._callbacks.remove(callback)

    async def emit(self, event: QEvent) -> None:
        queue = self._get_queue()
        await queue.put(event)

        listener_task = self._get_listener_task()
        if listener_task is None or listener_task.done():
            self._set_listener_task(asyncio.create_task(self._process_events()))

    async def _process_events(self) -> None:
        queue = self._get_queue()
        while not queue.empty():
            event = await queue.get()
            await self._route_event(event)

    async def _route_event(self, event: QEvent) -> None:
        tasks = []
        for callback in self._callbacks:
            if callback.should_handle(event):
                tasks.append(asyncio.create_task(callback.handle(event)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        _listener_task = self._get_listener_task()
        if _listener_task and not _listener_task.done():
            await _listener_task
