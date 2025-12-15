import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime

from loguru import logger
from ulid import ulid

from ..types import CronSchedule, DelaySchedule, ETASchedule, Schedule, TaskMessage


class InMemoryBroker:
    def __init__(self, max_concurrent_per_consumer: int = 10) -> None:
        self._task_queue: asyncio.Queue[TaskMessage] = asyncio.Queue()
        self._scheduled_queue: asyncio.PriorityQueue[tuple[float, TaskMessage]] = asyncio.PriorityQueue()
        self._processing: set[str] = set()
        self._processing_lock = asyncio.Lock()
        self._scheduler_task: asyncio.Task | None = None
        self._running = True
        self._max_concurrent = max_concurrent_per_consumer

    async def send_task(
        self, task_name: str, args: tuple, kwargs: dict, options: dict, task_id: str | None = None
    ) -> str:
        if task_id is None:
            task_id = str(ulid())
        message = TaskMessage(task_id=task_id, task_name=task_name, args=args, kwargs=kwargs, options=options)
        await self._task_queue.put(message)
        logger.debug("task {} queued: {}", task_id, task_name)
        return task_id

    async def send_scheduled_task(
        self, task_name: str, args: tuple, kwargs: dict, schedule: Schedule, options: dict, task_id: str | None = None
    ) -> str:
        if task_id is None:
            task_id = str(ulid())
        message = TaskMessage(task_id=task_id, task_name=task_name, args=args, kwargs=kwargs, options=options)

        execute_at = self._calculate_execute_time(schedule)
        await self._scheduled_queue.put((execute_at, message))

        if self._scheduler_task is None or self._scheduler_task.done():
            self._scheduler_task = asyncio.create_task(self._process_scheduled_tasks())

        return task_id

    def _calculate_execute_time(self, schedule: Schedule) -> float:
        now = datetime.now(UTC).timestamp()

        match schedule:
            case DelaySchedule(delay_seconds=delay):
                return now + delay
            case ETASchedule(eta=eta):
                return eta.timestamp()
            case CronSchedule(cron_expr=_):
                raise NotImplementedError("cron scheduling not yet implemented in in-memory broker")
            case _:
                raise NotImplementedError("unknown schedule")

    async def _process_scheduled_tasks(self) -> None:
        while self._running:
            if self._scheduled_queue.empty():
                await asyncio.sleep(0.1)
                continue

            execute_at, message = await self._scheduled_queue.get()
            now = datetime.now(UTC).timestamp()

            if now >= execute_at:
                await self._task_queue.put(message)
                logger.debug("scheduled task {} ready for execution", message.task_id)
            else:
                await self._scheduled_queue.put((execute_at, message))
                await asyncio.sleep(min(execute_at - now, 1.0))

    async def consume(self) -> AsyncIterator[TaskMessage]:
        """
        asynchronously consume messages from the queue

        `return:` asynchronous iterator of TaskMessage
        """

        while self._running:
            try:
                message = await asyncio.wait_for(self._task_queue.get(), timeout=1.0)
                async with self._processing_lock:
                    self._processing.add(message.task_id)
                yield message
            except TimeoutError:
                continue

    async def consume_tasks(self, callback: Callable[[TaskMessage], Awaitable[None]]) -> None:
        semaphore = asyncio.Semaphore(self._max_concurrent)
        pending_tasks: set[asyncio.Task] = set()

        async def process_with_semaphore(msg: TaskMessage) -> None:
            async with semaphore:
                try:
                    await callback(msg)
                finally:
                    async with self._processing_lock:
                        self._processing.discard(msg.task_id)

        while self._running:
            try:
                message = await asyncio.wait_for(self._task_queue.get(), timeout=0.5)
            except TimeoutError:
                done = {t for t in pending_tasks if t.done()}
                pending_tasks -= done
                continue

            async with self._processing_lock:
                self._processing.add(message.task_id)

            task = asyncio.create_task(process_with_semaphore(message))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

            done = {t for t in pending_tasks if t.done()}
            pending_tasks -= done

        if pending_tasks:
            logger.info("waiting for {} pending tasks to complete", len(pending_tasks))
            await asyncio.gather(*pending_tasks, return_exceptions=True)

    async def ack_task(self, task_id: str) -> None:
        async with self._processing_lock:
            self._processing.discard(task_id)

    async def neg_ack_task(self, task_id: str, requeue: bool = True) -> None:
        async with self._processing_lock:
            self._processing.discard(task_id)

    async def shutdown(self) -> None:
        self._running = False
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task

    async def health_check(self) -> bool:
        return True
