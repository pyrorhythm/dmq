from __future__ import annotations

import asyncio
import contextlib
import heapq
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True)
class ScheduledTask:
    execute_at: float
    task: Callable[[], Awaitable[Any]] = field(compare=False)
    task_id: str = field(compare=False)


class QScheduler:
    def __init__(self, max_workers: int = 100) -> None:
        self.max_workers = max_workers
        self._task_heap: list[ScheduledTask] = []
        self._active_tasks: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()
        self._scheduler_task: asyncio.Task | None = None
        self._running = False

    async def schedule(
        self, task: Callable[[], Awaitable[Any]], delay: float = 0.0, task_id: str | None = None
    ) -> None:
        execute_at = time.time() + delay
        scheduled = ScheduledTask(execute_at=execute_at, task=task, task_id=task_id or f"task-{id(task)}")

        async with self._lock:
            heapq.heappush(self._task_heap, scheduled)

    async def start(self) -> None:
        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task

        for task in self._active_tasks:
            task.cancel()

        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)

    async def _scheduler_loop(self) -> None:
        while self._running:
            await self._process_due_tasks()
            await asyncio.sleep(0.01)

    async def _process_due_tasks(self) -> None:
        now = time.time()

        async with self._lock:
            while self._task_heap and self._task_heap[0].execute_at <= now:
                if len(self._active_tasks) >= self.max_workers:
                    break

                scheduled = heapq.heappop(self._task_heap)
                task = asyncio.create_task(self._run_task(scheduled.task))
                self._active_tasks.add(task)
                task.add_done_callback(self._active_tasks.discard)

    async def _run_task(self, task: Callable[[], Awaitable[Any]]) -> Any:
        return await task()
