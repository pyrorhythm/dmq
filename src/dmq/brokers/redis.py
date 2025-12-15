from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import redis.asyncio as redis
from loguru import logger
from ulid import ulid

from dmq.serializers.msgpack import MsgpackSerializer

from ..types import CronSchedule, DelaySchedule, ETASchedule, Schedule, TaskMessage

if TYPE_CHECKING:
    from ..abc.serializer import QSerializerProtocol


class RedisBroker:
    """

    broker based on redis' lists

    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "dmq:tasks",
        scheduled_queue: str = "dmq:scheduled",
        serializer: QSerializerProtocol | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        self._redis_url = redis_url
        self._clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, redis.Redis] = weakref.WeakKeyDictionary()
        self.queue_name = queue_name
        self.scheduled_queue = scheduled_queue
        self.poll_interval = poll_interval
        self._running = True
        self._scheduler_task: asyncio.Task | None = None
        self.serializer = serializer or MsgpackSerializer()

    @property
    def redis(self) -> redis.Redis:
        """Get Redis client for the current event loop."""
        loop = asyncio.get_running_loop()
        if loop not in self._clients:
            self._clients[loop] = redis.from_url(self._redis_url, decode_responses=False)
        return self._clients[loop]

    async def send_task(
        self, task_name: str, args: tuple, kwargs: dict, options: dict, task_id: str | None = None
    ) -> str:
        if task_id is None:
            task_id = str(ulid())

        message = TaskMessage(task_id=task_id, task_name=task_name, args=args, kwargs=kwargs, options=options)

        data = self.serializer.serialize(message)
        await self.redis.lpush(self.queue_name, data)  # pyrefly: ignore[not-async]
        logger.debug("task {} queued to redis: {}", task_id, task_name)

        return task_id

    async def send_scheduled_task(
        self, task_name: str, args: tuple, kwargs: dict, schedule: Schedule, options: dict, task_id: str | None = None
    ) -> str:
        if task_id is None:
            task_id = str(ulid())

        message = TaskMessage(task_id=task_id, task_name=task_name, args=args, kwargs=kwargs, options=options)

        execute_at = self._calculate_execute_time(schedule)
        data = self.serializer.serialize(message)

        await self.redis.zadd(self.scheduled_queue, {data: execute_at})
        logger.debug("scheduled task {} for {}", task_id, execute_at)

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
                raise NotImplementedError("cron scheduling not yet implemented")
            case _:
                raise NotImplementedError("unknown schedule")

    async def _process_scheduled_tasks(self) -> None:
        while self._running:
            try:
                now = datetime.now(UTC).timestamp()

                due_tasks = await self.redis.zrangebyscore(self.scheduled_queue, min=0, max=now)

                for task_data in due_tasks:
                    await self.redis.lpush(self.queue_name, task_data)  # pyrefly: ignore[not-async]
                    await self.redis.zrem(self.scheduled_queue, task_data)  # pyrefly: ignore[not-async]

                    message = self.serializer.deserialize(task_data, into=TaskMessage)
                    logger.debug("scheduled task {} ready for execution", message.task_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("scheduler error: {}", e)

            await asyncio.sleep(self.poll_interval)

    async def consume(self) -> AsyncIterator[TaskMessage]:
        while self._running:
            try:
                logger.debug("trying to consume message...")
                result = await self.redis.brpop([self.queue_name], timeout=1)  # pyrefly: ignore[not-async]

                if result is None:
                    logger.debug("trying to consume message... failure")
                    continue

                _, data = result
                logger.debug("trying to consume message... success; got {}", data)
                message = self.serializer.deserialize(data, into=TaskMessage)
                logger.debug("yielding message... {}", message)
                yield message
                logger.debug("yielding message; success!")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("redis consume error: {}", e)
                await asyncio.sleep(1.0)

    async def consume_tasks(self, callback: Callable[[TaskMessage], Awaitable[None]]) -> None:
        async for message in self.consume():
            if not self._running:
                break
            await callback(message)

    async def ack_task(self, task_id: str) -> None:
        pass

    async def neg_ack_task(self, task_id: str, requeue: bool = True) -> None:
        pass

    async def shutdown(self) -> None:
        logger.info("redis broker shutting down...")
        self._running = False

        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task

        await self.redis.aclose()

    async def health_check(self) -> bool:
        try:
            await self.redis.ping()  # pyrefly: ignore[not-async]
            return True
        except Exception:
            return False

    async def queue_length(self) -> int:
        return await self.redis.llen(self.queue_name)  # pyrefly: ignore[not-async]

    async def scheduled_count(self) -> int:
        return await self.redis.zcard(self.scheduled_queue)
