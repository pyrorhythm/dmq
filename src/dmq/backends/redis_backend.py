from __future__ import annotations

import asyncio
import weakref
from typing import TYPE_CHECKING, Any

import redis.asyncio as redis

from dmq.serializers import JsonSerializer

if TYPE_CHECKING:
    from ..abc.serializer import QSerializerProtocol


class RedisResultBackend:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        default_ttl: int = 3600,
        serializer: QSerializerProtocol | None = None,
    ) -> None:
        self._redis_url = redis_url
        self._clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, redis.Redis] = weakref.WeakKeyDictionary()
        self.default_ttl = default_ttl
        self.serializer: QSerializerProtocol = serializer or JsonSerializer()

    @property
    def redis(self) -> redis.Redis:
        """Get Redis client for the current event loop."""
        loop = asyncio.get_running_loop()
        if loop not in self._clients:
            self._clients[loop] = redis.from_url(self._redis_url)
        return self._clients[loop]

    async def store_result(self, task_id: str, result: Any, ttl: int | None = None) -> None:
        key = f"sotq:result:{task_id}"
        data: bytes = self.serializer.serialize(result)
        await self.redis.setex(key, ttl or self.default_ttl, data)

        await self.redis.publish(f"sotq:result:ready:{task_id}", "ready")

    async def get_result(self, task_id: str, timeout: float | None = None) -> Any:
        key = f"sotq:result:{task_id}"

        data = await self.redis.get(key)
        if data is not None:
            return self.serializer.deserialize(data)

        if timeout is None:
            raise KeyError(f"result not found: {task_id}")

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(f"sotq:result:ready:{task_id}")

        try:
            async with asyncio.timeout(timeout):
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        data: bytes | None = await self.redis.get(key)
                        if data is not None:
                            return self.serializer.deserialize(data)
        except TimeoutError:
            raise TimeoutError(f"result not available within {timeout}s") from None
        finally:
            await pubsub.unsubscribe()
            await pubsub.close()

    async def delete_result(self, task_id: str) -> None:
        key = f"sotq:result:{task_id}"
        await self.redis.delete(key)

    async def result_exists(self, task_id: str) -> bool:
        key = f"sotq:result:{task_id}"
        return await self.redis.exists(key) > 0
