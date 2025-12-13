from __future__ import annotations

import asyncio
import itertools
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from .types import TaskMessage


class PartitionStrategy(Protocol):
    async def get_partition(self, message: TaskMessage, partition_count: int) -> int: ...


class HashPartitionStrategy:
    async def get_partition(self, message: TaskMessage, partition_count: int) -> int:
        return hash(message.task_id) % partition_count


class KeyBasedPartitionStrategy:
    def __init__(self, key_extractor: Callable[[TaskMessage], str]) -> None:
        self.key_extractor = key_extractor

    async def get_partition(self, message: TaskMessage, partition_count: int) -> int:
        key = self.key_extractor(message)
        return hash(key) % partition_count


class RoundRobinPartitionStrategy:
    def __init__(self) -> None:
        self._counter = itertools.count()
        self._lock = asyncio.Lock()

    async def get_partition(self, message: TaskMessage, partition_count: int) -> int:
        async with self._lock:
            return next(self._counter) % partition_count
