from __future__ import annotations

import asyncio
import weakref

import redis.asyncio as redis


class RedisClientManager:
    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, redis.Redis] = weakref.WeakKeyDictionary()

    def client(self) -> redis.Redis:
        loop = asyncio.get_running_loop()
        if loop not in self._clients:
            self._clients[loop] = redis.from_url(self._redis_url)
        return self._clients[loop]

    async def close_all(self) -> None:
        """Close all Redis client connections."""
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
