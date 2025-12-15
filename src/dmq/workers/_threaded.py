from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING

from loguru import logger

from ..guarantees import DeliveryConfig, IdempotencyStore
from ..types import TaskMessage
from ._base import QBaseWorker

if TYPE_CHECKING:
    from ..manager import QManager


class QThreadedWorker(QBaseWorker):
    def __init__(
        self,
        manager: QManager,
        worker_id: str | None = None,
        delivery_config: DeliveryConfig | None = None,
        max_concurrent_tasks: int = 10,
    ) -> None:
        super().__init__(
            manager=manager,
            worker_id=worker_id or f"threaded-worker-{id(self)}",
            delivery_config=delivery_config or DeliveryConfig(),
            max_concurrent_tasks=max_concurrent_tasks
        )

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        self._started_event = threading.Event()
        self._stopped_event = threading.Event()

    @property
    def active_task_count(self) -> int:
        if not self._pending_tasks:
            return 0
        return len([t for t in self._pending_tasks if not t.done()])

    @property
    def queue_size(self) -> int:
        if self._task_queue is None:
            return 0
        return self._task_queue.qsize()

    @property
    def load(self) -> int:
        return self.active_task_count + self.queue_size

    def submit(self, message: TaskMessage) -> None:
        logger.debug("got message {}", message)

        if not self._running:
            logger.error("_running is false")
            msg = f"Worker {self.worker_id} is not running"
            raise RuntimeError(msg)

        if self._loop is None or self._task_queue is None:
            logger.error("event loop not available")
            msg = f"Worker {self.worker_id} event loop not available"
            raise RuntimeError(msg)

        asyncio.run_coroutine_threadsafe(self._task_queue.put(message), self._loop)

    def start(self) -> None:
        if self._running:
            logger.warning("worker {} already running", self.worker_id)
            return

        self._running = True
        self._stopped_event.clear()

        self._thread = threading.Thread(target=self._thread_main, name=f"dmq-{self.worker_id}", daemon=True)
        self._thread.start()

        if not self._started_event.wait(timeout=5.0):
            logger.warning("worker {} start timed out", self.worker_id)

        logger.info(
            "threaded worker {} started (thread: {}, max_concurrent: {})",
            self.worker_id,
            self._thread.name,
            self.max_concurrent_tasks,
        )

    def wait_started(self) -> None:
        self._started_event.wait()

    def stop(self, timeout: float = 30.0) -> None:
        if not self._running:
            return

        logger.info("stopping threaded worker {}...", self.worker_id)
        self._running = False

        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._shutdown_loop(), self._loop)

        if self._thread:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning("worker {} thread did not stop in time", self.worker_id)

        self._stopped_event.set()
        logger.info("threaded worker {} stopped", self.worker_id)

    def _thread_main(self) -> None:
        loop = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            loop.run_until_complete(self._async_main())
        except Exception as e:
            logger.exception("worker {} thread crashed: {}", self.worker_id, e)
        finally:
            if loop:
                try:
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()

                    if pending:
                        loop.run_until_complete(
                            asyncio.gather(*pending, return_exceptions=True)  # type: ignore
                        )

                    loop.close()
                except Exception as e:
                    logger.warning("error closing loop for worker {}: {}", self.worker_id, e)

            self._stopped_event.set()

    async def _async_main(self) -> None:
        self._task_queue = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        self._pending_tasks = set()

        self._started_event.set()

        await self._process_loop()

    async def _shutdown_loop(self) -> None:
        if self._pending_tasks:
            active = [t for t in self._pending_tasks if not t.done()]
            if active:
                logger.info("worker {} waiting for {} pending tasks", self.worker_id, len(active))
                await asyncio.gather(*active, return_exceptions=True)
