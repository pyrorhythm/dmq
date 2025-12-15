from __future__ import annotations

import asyncio
import threading
import time
import traceback
from typing import TYPE_CHECKING

from loguru import logger

from dmq.events import (
    _task_completed_event,
    _task_failure_event,
    _task_not_found_event,
    _task_retry_event,
    _task_started_event,
)
from dmq.util.misc import await_if_async

from .guarantees import DeliveryConfig, IdempotencyStore
from .task import QTask
from .types import TaskMessage
from .user_event import UserEventEmitter

if TYPE_CHECKING:
    from .manager import QManager


class QThreadedWorker:
    def __init__(
        self,
        manager: QManager,
        worker_id: str | None = None,
        delivery_config: DeliveryConfig | None = None,
        max_concurrent_tasks: int = 10,
    ) -> None:
        self.manager = manager
        self.worker_id = worker_id or f"threaded-worker-{id(self)}"
        self.delivery_config = delivery_config or DeliveryConfig()
        self.idempotency_store = IdempotencyStore() if self.delivery_config.enable_idempotency else None
        self.max_concurrent_tasks = max_concurrent_tasks

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._running = False
        self._started_event = threading.Event()
        self._stopped_event = threading.Event()

        self._task_queue: asyncio.Queue[TaskMessage] | None = None
        self._pending_tasks: set[asyncio.Task] = set()
        self._semaphore: asyncio.Semaphore | None = None

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

        # Wait for thread to finish
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

    async def _process_loop(self) -> None:
        while self._running:
            try:
                message = await asyncio.wait_for(self._task_queue.get(), timeout=0.5)  # pyrefly: ignore
                task = asyncio.create_task(self._execute_with_semaphore(message))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
            except TimeoutError:
                done = {t for t in self._pending_tasks if t.done()}
                self._pending_tasks -= done
            except asyncio.CancelledError:
                break

    async def _execute_with_semaphore(self, message: TaskMessage) -> None:
        if self._semaphore is None:
            logger.warning("sema is None; executing task without semaphore")
            await self._handle_task(message)
            return

        async with self._semaphore:
            await self._handle_task(message)

    async def _handle_task(self, message: TaskMessage) -> None:
        if self.delivery_config.should_ack_before_processing():
            await self.manager.broker.ack_task(message.task_id)

        if self.delivery_config.should_check_idempotency():
            if await self.idempotency_store.is_processed(  # pyrefly: ignore
                message.task_id
            ):
                await self.manager.broker.ack_task(message.task_id)
                return

        try:
            task = self.manager.get_task(message.task_name)

            if task is None:
                nf_event = _task_not_found_event(message)
                await self.manager.event_router.emit(nf_event)
                logger.warning("task nf")
                return

            start_time = time.time()
            start_event = _task_started_event(message, start_time, self.worker_id)
            await self.manager.event_router.emit(start_event)

            emitter = UserEventEmitter(message.task_id, message.task_name, self.manager.event_router)
            QTask.set_emitter(emitter)

            try:
                result = await await_if_async(
                    task.original_func(*message.args, **message.kwargs)  # pyrefly: ignore[invalid-param-spec]
                )
            finally:
                QTask.set_emitter(None)

            await self.manager.result_backend.store_result(message.task_id, result)

            if self.delivery_config.should_check_idempotency():
                await self.idempotency_store.mark_processed(  # pyrefly: ignore
                    message.task_id, time.time()
                )

            duration = time.time() - start_time
            complete_event = _task_completed_event(message, duration, result)
            await self.manager.event_router.emit(complete_event)

            if self.delivery_config.should_ack_after_processing():
                await self.manager.broker.ack_task(message.task_id)

            logger.debug("worker {} completed task {} in {:.3f}s", self.worker_id, message.task_id, duration)

        except Exception as e:
            await self._handle_task_failure(message, e)

    async def _handle_task_failure(self, message: TaskMessage, exception: Exception) -> None:
        logger.warning("worker {} task {} failed: {}", self.worker_id, message.task_id, exception)
        logger.debug("{}", traceback.format_exception(type(exception), exception, exception.__traceback__))

        if message.retry_count < message.max_retries:
            retry_event = _task_retry_event(message)
            await self.manager.event_router.emit(retry_event)

            await self.manager.broker.neg_ack_task(message.task_id, requeue=True)
        else:
            failed_event = _task_failure_event(message, exception)
            await self.manager.event_router.emit(failed_event)

            await self.manager.result_backend.store_result(message.task_id, exception)
            await self.manager.broker.neg_ack_task(message.task_id, requeue=False)
