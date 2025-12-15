from __future__ import annotations

import asyncio
import contextlib
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, cast

from loguru import logger

from .execution_mode import ExecutionMode
from .guarantees import DeliveryConfig
from .threaded_worker import QThreadedWorker
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
        execution_mode: ExecutionMode = ExecutionMode.ASYNC_ONLY,
    ) -> None:
        self.manager = manager
        self.worker_count = worker_count
        self.max_tasks_per_worker = max_tasks_per_worker
        self.delivery_config = delivery_config
        self.execution_mode = execution_mode

        self.workers: list[QWorker | QThreadedWorker] = []
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

    def _create_workers(self) -> list[QWorker | QThreadedWorker]:
        if self.execution_mode == ExecutionMode.THREADED:
            logger.info("creating {} threaded workers", self.worker_count)
            return [
                QThreadedWorker(
                    manager=self.manager,
                    worker_id=f"threaded-worker-{i}",
                    delivery_config=self.delivery_config,
                    max_concurrent_tasks=self.max_tasks_per_worker,
                )
                for i in range(self.worker_count)
            ]

        logger.info("creating {} async workers", self.worker_count)
        return [
            QWorker(
                manager=self.manager,
                worker_id=f"worker-{i}",
                delivery_config=self.delivery_config,
                max_concurrent_tasks=self.max_tasks_per_worker,
            )
            for i in range(self.worker_count)
        ]

    def _get_next_worker(self) -> QWorker | QThreadedWorker:
        worker = self.workers[self._next_worker_idx]
        self._next_worker_idx = (self._next_worker_idx + 1) % len(self.workers)
        return worker

    def _get_least_loaded_worker(self) -> QWorker | QThreadedWorker:
        return min(self.workers, key=lambda w: w.load)

    async def _dispatch_task(self, message: TaskMessage) -> None:
        worker = self._get_least_loaded_worker()

        if self.execution_mode == ExecutionMode.THREADED:
            logger.debug("submitting task to threaded worker...")
            worker = cast(QThreadedWorker, worker)
            # thread-safe synchronous submit for threaded workers
            worker.submit(message)  # pyrefly: ignore
            logger.debug("submitting task to threaded worker... success!")
        else:
            logger.debug("submitting task to async worker...")
            worker = cast(QWorker, worker)
            # async submit for async workers
            await worker.submit(message)  # pyrefly: ignore

        logger.debug("dispatcher: task {} to {}", message.task_id[:8], worker.worker_id)

    async def _dispatcher_loop(self) -> None:
        logger.info("dispatcher started; pulling from broker")

        if hasattr(self.manager.broker, "consume"):
            logger.debug("attaching to async iter, consuming...")
            async for message in self.manager.broker.consume():
                logger.debug("got message... {}", message)
                if not self._running:
                    logger.warning("_running is false; breaking...")
                    break
                logger.debug("dispatching message...")

                await self._dispatch_task(message)
        else:
            await self.manager.broker.consume_tasks(self._dispatch_task)

    async def start(self) -> None:
        if self._running:
            logger.warning("pool already running")
            return

        self._running = True
        self._stopped.clear()

        self.workers = self._create_workers()

        if self.execution_mode == ExecutionMode.THREADED:
            threaded_workers = cast(list[QThreadedWorker], self.workers)
            for worker in threaded_workers:
                worker.start()

            for worker in threaded_workers:
                worker.wait_started()
        else:
            async_workers = cast(list[QWorker], self.workers)
            self._worker_tasks = [
                asyncio.create_task(worker.start(), name=f"worker-{worker.worker_id}") for worker in async_workers
            ]

            await asyncio.gather(*[worker.wait_started() for worker in async_workers])

        # dispatcher remains async in both modes
        self._dispatcher_task = asyncio.create_task(self._dispatcher_loop(), name="dispatcher")

        self._started.set()
        logger.info("pool initiated; {} workers (mode: {})", self.worker_count, self.execution_mode)

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

        if self.execution_mode == ExecutionMode.THREADED:
            # stop threaded workers
            with ThreadPoolExecutor(max_workers=len(self.workers)) as executor:
                futures = [executor.submit(worker.stop, timeout) for worker in self.workers]  # pyrefly: ignore
                # wait for all futures to complete
                for future in futures:
                    try:
                        future.result(timeout=timeout)
                    except Exception as e:
                        logger.warning("error stopping worker: {}", e)
        else:
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

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # ANN001: ignore
        await self.shutdown()
