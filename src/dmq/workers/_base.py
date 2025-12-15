from __future__ import annotations

import asyncio
import time
import traceback
from typing import TYPE_CHECKING

from loguru import logger

from ..events import (
    _task_completed_event,
    _task_failure_event,
    _task_not_found_event,
    _task_retry_event,
    _task_started_event,
)
from ..guarantees import DeliveryConfig
from ..task import QTask
from ..types import TaskMessage
from ..user_event import UserEventEmitter
from ..util.misc import await_if_async

if TYPE_CHECKING:
    from ..guarantees import IdempotencyStore
    from ..manager import QManager


class QBaseWorker:
    def __init__(
        self,
        manager: QManager,
        worker_id: str,
        delivery_config: DeliveryConfig | None = None,
        max_concurrent_tasks: int | None = None,
    ) -> None:
        self.manager = manager
        self.worker_id = worker_id
        self.delivery_config = delivery_config or DeliveryConfig()
        self.idempotency_store = IdempotencyStore() if self.delivery_config.enable_idempotency else None
        self.max_concurrent_tasks = max_concurrent_tasks or 10

        self._running = False
        self._pending_tasks = set[asyncio.Task]()
        self._task_queue: asyncio.Queue[TaskMessage] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(self.max_concurrent_tasks)

    async def _process_loop(
        self,
    ) -> None:
        while self._running:
            try:
                message = await asyncio.wait_for(self._task_queue.get(), timeout=0.5)
                task = asyncio.create_task(self._execute_with_semaphore(message))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
            except TimeoutError:
                self._pending_tasks -= {t for t in self._pending_tasks if t.done()}
            except asyncio.CancelledError:
                break

    async def _execute_with_semaphore(
        self,
        message: TaskMessage,
    ) -> None:
        if self._semaphore is None:
            logger.warning("sema is None; executing task without semaphore")
            await self._handle_task(message)
            return

        async with self._semaphore:
            await self._handle_task(message)

    async def _handle_task(self, message: TaskMessage) -> None:
        if self.delivery_config.should_ack_before_processing():
            await self.manager.broker.ack_task(message.task_id)

        if (
            self.delivery_config.should_check_idempotency()
            and self.idempotency_store
            and await self.idempotency_store.is_processed(message.task_id)
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
                logger.warning("task {} not found", message.task_name)
                return

            emitter = UserEventEmitter(message.task_id, message.task_name, self.manager.event_router)
            QTask.set_emitter(emitter)

            try:
                result = await await_if_async(task.original_func(*message.args, **message.kwargs))
            finally:
                QTask.set_emitter(None)

            await self.manager.result_backend.store_result(message.task_id, result, status="SUCCESS")

            if self.delivery_config.should_check_idempotency() and self.idempotency_store:
                await self.idempotency_store.mark_processed(message.task_id, time.time())

            duration = time.time() - start_time
            complete_event = _task_completed_event(message, duration, result)
            await self.manager.event_router.emit(complete_event)

            if self.delivery_config.should_ack_after_processing():
                await self.manager.broker.ack_task(message.task_id)

            logger.debug(
                "worker {} completed task {} in {:.3f}s",
                self.worker_id,
                message.task_id,
                duration,
            )

        except Exception as e:
            await self._handle_task_failure(message, e)

    async def _handle_task_failure(
        self, message: TaskMessage, exception: Exception
    ) -> None:
        logger.warning(
            "worker {} task {} failed: {}",
            self.worker_id,
            message.task_id,
            exception,
        )
        logger.debug("traceback: {}", traceback.format_exc())

        if message.retry_count < message.max_retries:
            retry_event = _task_retry_event(message)
            await self.manager.event_router.emit(retry_event)
            await self.manager.broker.neg_ack_task(message.task_id, requeue=True)
        else:
            failed_event = _task_failure_event(message, exception)
            await self.manager.event_router.emit(failed_event)
            await self.manager.result_backend.store_result(message.task_id, exception, status="FAILED")
            await self.manager.broker.neg_ack_task(message.task_id, requeue=False)
