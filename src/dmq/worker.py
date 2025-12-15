from __future__ import annotations

import asyncio
import contextlib
import time
import traceback
from typing import TYPE_CHECKING

from loguru import logger

from dmq.events import _task_not_found_event

from .events import _task_completed_event, _task_failure_event, _task_retry_event, _task_started_event
from .guarantees import DeliveryConfig, IdempotencyStore
from .task import QTask
from .types import TaskMessage
from .user_event import UserEventEmitter
from .util.misc import await_if_async

if TYPE_CHECKING:
    from .manager import QManager


class QWorker:
    def __init__(
        self,
        manager: QManager,
        worker_id: str | None = None,
        delivery_config: DeliveryConfig | None = None,
        max_concurrent_tasks: int = 10,
    ) -> None:
        self.manager = manager
        self.worker_id = worker_id or f"worker-{id(self)}"
        self.delivery_config = delivery_config or DeliveryConfig()
        self.idempotency_store = IdempotencyStore() if self.delivery_config.enable_idempotency else None
        self.max_concurrent_tasks = max_concurrent_tasks

        self._running = False
        self._semaphore: asyncio.Semaphore | None = None
        self._pending_tasks: set[asyncio.Task] = set()
        self._task_queue: asyncio.Queue[TaskMessage] = asyncio.Queue()
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

        logger.info("worker {} started", self.worker_id, self.max_concurrent_tasks)

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
                logger.info("worker {} waiting for {} pending tasks", self.worker_id, len(active))
                await asyncio.gather(*active, return_exceptions=True)

        logger.info("worker {} stopped", self.worker_id)

    async def _process_loop(self) -> None:
        while self._running:
            try:
                message = await asyncio.wait_for(self._task_queue.get(), timeout=0.5)
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
            logger.warning("sema is none; executing task without semaphore")
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

        start_time = time.time()

        start_event = _task_started_event(message, start_time, self.worker_id)
        await self.manager.event_router.emit(start_event)

        try:
            task = self.manager.get_task(message.task_name)

            if task is None:
                nf_event = _task_not_found_event(message)
                await self.manager.event_router.emit(nf_event)
                logger.warning("task nf")
                return

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
            await self._handle_task_failure(message, e, start_time)

    async def _handle_task_failure(self, message: TaskMessage, exception: Exception, start_time: float) -> None:
        tb = traceback.format_exc()

        logger.warning("worker {} task {} failed: {}", self.worker_id, message.task_id, exception)

        if message.retry_count < message.max_retries:
            retry_event = _task_retry_event(message)
            await self.manager.event_router.emit(retry_event)

            await self.manager.broker.neg_ack_task(message.task_id, requeue=True)
        else:
            failed_event = _task_failure_event(message, exception)
            await self.manager.event_router.emit(failed_event)

            await self.manager.result_backend.store_result(message.task_id, exception)
            await self.manager.broker.neg_ack_task(message.task_id, requeue=False)
