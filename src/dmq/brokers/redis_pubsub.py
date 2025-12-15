from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Never

import redis.asyncio as redis

from ..partitioning import HashPartitionStrategy, PartitionStrategy
from ..serializers import MsgpackSerializer
from ..topic import TopicConfig
from ..types import TaskMessage

if TYPE_CHECKING:
    from ..abc.serializer import QSerializerProtocol


class RedisPubSubBroker:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        topics: dict[str, TopicConfig] | None = None,
        partition_strategy: PartitionStrategy | None = None,
        serializer: QSerializerProtocol | None = None,
    ) -> None:
        self._redis_url = redis_url
        self._clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, redis.Redis] = weakref.WeakKeyDictionary()
        self.topics = topics or {}
        self.partition_strategy = partition_strategy or HashPartitionStrategy()
        self.serializer = serializer or MsgpackSerializer()

    @property
    def redis(self) -> redis.Redis:  # type: ignore[type-arg]
        """Get Redis client for the current event loop."""
        loop = asyncio.get_running_loop()
        if loop not in self._clients:
            self._clients[loop] = redis.from_url(self._redis_url)
        return self._clients[loop]

    async def create_topic(self, config: TopicConfig) -> None:
        self.topics[config.name] = config

    async def send_task(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> str:
        from ulid import ulid

        task_id = str(ulid())

        message = TaskMessage(task_id=task_id, task_name=task_name, args=args, kwargs=kwargs, options=options)

        topic = options.get("topic", "default")
        await self.send_to_topic(topic, message)

        return task_id

    async def send_to_topic(self, topic: str, message: TaskMessage, partition_key: str | None = None) -> None:
        if topic not in self.topics:
            raise ValueError(f"topic {topic} not found. create it first with create_topic()")

        topic_config = self.topics[topic]
        partition = await self.partition_strategy.get_partition(message, topic_config.partition_count)

        channel = f"sotq:topic:{topic}:partition:{partition}"
        data = self.serializer.serialize(message)
        await self.redis.publish(channel, data)

    async def consume_partitions(self, topic: str, partitions: list[int], group_id: str) -> AsyncIterator[TaskMessage]:
        pubsub = self.redis.pubsub()

        channels = [f"sotq:topic:{topic}:partition:{p}" for p in partitions]

        await pubsub.subscribe(*channels)

        async for message in pubsub.listen():
            if message["type"] == "message":
                task_message = self.serializer.deserialize(message["data"], into=TaskMessage)
                yield task_message

    async def consume_tasks(self, callback) -> Never:
        raise NotImplementedError(
            "redis_pubsub_broker uses consume_partitions. use the simple inmemory_broker for basic "
            "consume_tasks support."
        )

    async def ack_task(self, task_id: str) -> None:
        pass

    async def neg_ack_task(self, task_id: str, requeue: bool = True) -> None:
        pass

    async def health_check(self) -> bool:
        try:
            await self.redis.ping()  # pyrefly: ignore[not-async]
            return True
        except Exception:
            return False
