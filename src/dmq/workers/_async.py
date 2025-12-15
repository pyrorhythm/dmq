from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from loguru import logger

from ..guarantees import DeliveryConfig, IdempotencyStore
from ..types import TaskMessage
from ._base import QBaseWorker

if TYPE_CHECKING:
    from ..manager import QManager


class QAsyncWorker(QBaseWorker):
    def __init__(
        self,
        manager: QManager,
        worker_id: str | None = None,
        delivery_config: DeliveryConfig | None = None,
        max_concurrent_tasks: int = 10,
    ) -> None:
        super().__init__(
            manager=manager,
            worker_id=worker_id or f"async-worker-{id(self)}",
            delivery_config=delivery_config or DeliveryConfig(),
            max_concurrent_tasks=max_concurrent_tasks
        )

        self._process_task: asyncio.Task | None = None
        self._started = asyncio.Event()

    @property
    def active_task_count(self) -> int:
        return len([t for t in self._pending_tasks if not t.done()])

    @property
    def queue_size(self) -> int:
        return self._task_queue.qsize()

    @property
    def load(self) -> int:
        return self.active_task_count + self.queue_size

    async def submit(self, message: TaskMessage) -> None:
        await self._task_queue.put(message)

    async def start(self) -> None:
        self._running = True
        self._semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        self._pending_tasks = set()

        logger.info(
            "worker {} started",
            self.worker_id,
            self.max_concurrent_tasks,
        )

        self._process_task = asyncio.create_task(self._process_loop())
        self._started.set()

        with contextlib.suppress(asyncio.CancelledError):
            await self._process_task

    async def wait_started(self) -> None:
        await self._started.wait()

    async def stop(self) -> None:
        logger.info("worker {} stopping...", self.worker_id)
        self._running = False

        if self._process_task and not self._process_task.done():
            self._process_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._process_task

        if self._pending_tasks:
            active = [t for t in self._pending_tasks if not t.done()]
            if active:
                logger.info(
                    "worker {} waiting for {} pending tasks",
                    self.worker_id,
                    len(active),
                )
                await asyncio.gather(*active, return_exceptions=True)

        logger.info("worker {} stopped", self.worker_id)
