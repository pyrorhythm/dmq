from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from loguru import logger

from .guarantees import DeliveryConfig
from .types import TaskMessage
from .worker import QWorker

if TYPE_CHECKING:
    from .manager import QManager


class QWorkerPool:
    def __init__(
        self,
        manager: QManager,
        worker_count: int = 4,
        max_tasks_per_worker: int = 10,
        delivery_config: DeliveryConfig | None = None,
    ) -> None:
        self.manager = manager
        self.worker_count = worker_count
        self.max_tasks_per_worker = max_tasks_per_worker
        self.delivery_config = delivery_config

        self.workers: list[QWorker] = []
        self._worker_tasks: list[asyncio.Task] = []
        self._dispatcher_task: asyncio.Task | None = None
        self._running = False
        self._started = asyncio.Event()
        self._stopped = asyncio.Event()
        self._next_worker_idx = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def total_max_concurrent(self) -> int:
        return self.worker_count * self.max_tasks_per_worker

    def _get_next_worker(self) -> QWorker:
        worker = self.workers[self._next_worker_idx]
        self._next_worker_idx = (self._next_worker_idx + 1) % len(self.workers)
        return worker

    def _get_least_loaded_worker(self) -> QWorker:
        return min(self.workers, key=lambda w: w.load)

    async def _dispatch_task(self, message: TaskMessage) -> None:
        worker = self._get_least_loaded_worker()
        await worker.submit(message)
        logger.debug("dispatcher: task {} to {}", message.task_id[:8], worker.worker_id)

    async def _dispatcher_loop(self) -> None:
        logger.info("dispatcher started; pulling from broker")

        if hasattr(self.manager.broker, "consume"):
            async for message in self.manager.broker.consume():
                if not self._running:
                    break
                await self._dispatch_task(message)
        else:
            await self.manager.broker.consume_tasks(self._dispatch_task)

    async def start(self) -> None:
        if self._running:
            logger.warning("pool already running")
            return

        self._running = True
        self._stopped.clear()

        self.workers = [
            QWorker(
                manager=self.manager,
                worker_id=f"worker-{i}",
                delivery_config=self.delivery_config,
                max_concurrent_tasks=self.max_tasks_per_worker,
            )
            for i in range(self.worker_count)
        ]

        self._worker_tasks = [
            asyncio.create_task(worker.start(), name=f"worker-{worker.worker_id}") for worker in self.workers
        ]

        await asyncio.gather(*[worker.wait_started() for worker in self.workers])

        self._dispatcher_task = asyncio.create_task(self._dispatcher_loop(), name="dispatcher")

        self._started.set()
        logger.info("pool initiated; {} workers", self.worker_count)

    async def run_forever(self) -> None:
        await self.start()

        try:
            if self._dispatcher_task:
                await self._dispatcher_task
        except asyncio.CancelledError:
            pass
        finally:
            self._stopped.set()

    async def wait_started(self) -> None:
        await self._started.wait()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    async def shutdown(self, timeout: float = 30.0) -> None:
        if not self._running:
            return

        logger.info("shutting down worker pool...")
        self._running = False

        if hasattr(self.manager.broker, "shutdown"):
            await self.manager.broker.shutdown()

        if self._dispatcher_task and not self._dispatcher_task.done():
            self._dispatcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dispatcher_task

        try:
            await asyncio.wait_for(
                asyncio.gather(  # pyrefly: ignore
                    *[worker.stop() for worker in self.workers], return_exceptions=True
                ),
                timeout=timeout,
            )
        except TimeoutError:
            logger.warning("shutting down worker pool... timeout! cancelling remaining tasks")

        for task in self._worker_tasks:
            if not task.done():
                task.cancel()

        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)

        self._stopped.set()
        logger.info("shutting down worker pool... success!")

    async def __aenter__(self) -> QWorkerPool:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()
