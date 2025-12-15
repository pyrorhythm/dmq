from __future__ import annotations

import asyncio
import importlib
import weakref
from typing import TYPE_CHECKING, Any

import redis.asyncio as redis
from loguru import logger

from dmq.cli.utils import add_cwd_in_path
from dmq.serializers.msgpack import MsgpackSerializer
from dmq.util.misc import _get_type_fqn

if TYPE_CHECKING:
    from ..abc.serializer import QSerializerProtocol


def _get_type_from_fqn(_result: bytes | None) -> Any | None:
    _imported_type = None
    if _result is None:
        return _imported_type

    _type_module_fqn, _type_name = _result.decode().split("|")
    try:
        with add_cwd_in_path():
            _module = importlib.import_module(_type_module_fqn)
            _imported_type = getattr(_module, _type_name, None)
    except Exception as exc:
        logger.warning("{}", exc)

    return _imported_type


class RedisResultBackend:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        default_ttl: int = 3600,
        serializer: QSerializerProtocol | None = None,
        type_serialization: bool = False,
    ) -> None:
        """
        initialize redis result backend

        :param redis_url:
        :param default_ttl:
        :param serializer: implementation of SerializerProto
        :param type_serialization: (defaults to False) -- if to serialize types of results,
        requires worker and client to be in 1 working directory to correctly resolve FQN's

        """
        self._redis_url = redis_url
        self._clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, redis.Redis] = weakref.WeakKeyDictionary()
        self.default_ttl = default_ttl
        self.serializer: QSerializerProtocol = serializer or MsgpackSerializer()
        self.type_serialization = type_serialization

    @property
    def redis(self) -> redis.Redis:
        loop = asyncio.get_running_loop()
        if loop not in self._clients:
            self._clients[loop] = redis.from_url(self._redis_url)
        return self._clients[loop]

    async def store_result(self, task_id: str, result: Any, ttl: int | None = None) -> None:
        key = f"sotq:result:{task_id}"
        data: bytes = self.serializer.serialize(result)
        await self.redis.setex(key, ttl or self.default_ttl, data)

        if self.type_serialization:
            _type = _get_type_fqn(result)
            if _type is None:
                logger.warning(
                    "type_serialization is True but failed to get type FQN of {}, serializing as builtins.str", result
                )
                _type = "builtins|str"

            await self.redis.setex(key + ":type", ttl or self.default_ttl, _type)

        await self.redis.publish(f"sotq:result:ready:{task_id}", "ready")

    async def get_result(self, task_id: str, timeout: float | None = None) -> Any:
        key = f"sotq:result:{task_id}"

        if (_res := await self._fetch_result(key)) is not None:
            return _res

        if timeout is None:
            raise KeyError(f"result not found: {task_id}")

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(f"sotq:result:ready:{task_id}")

        try:
            async with asyncio.timeout(timeout):
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        return await self._fetch_result(key)

        except TimeoutError as exc:
            raise TimeoutError(f"result not available within {timeout}s") from exc
        finally:
            await pubsub.unsubscribe()
            await pubsub.close()

    async def _fetch_result(self, key: str) -> Any:
        _deser = None

        data = await self.redis.get(key)

        if data is not None:
            _result = await self.redis.get(key + ":type")
            _imported_type = _get_type_from_fqn(_result)
            _deser = self.serializer.deserialize(data, into=_imported_type)

        return _deser

    async def delete_result(self, task_id: str) -> None:
        key = f"sotq:result:{task_id}"
        await self.redis.delete(key)

    async def result_exists(self, task_id: str) -> bool:
        key = f"sotq:result:{task_id}"
        return await self.redis.exists(key) > 0
