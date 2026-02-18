from __future__ import annotations

from dmq.partitioning import HashPartitionStrategy, KeyBasedPartitionStrategy, RoundRobinPartitionStrategy
from dmq.types import TaskMessage


def _make_message(task_id="t1"):
	return TaskMessage(task_id=task_id, task_name="test", args=(), kwargs={}, options={})


async def test_hash_partition():
	strategy = HashPartitionStrategy()
	msg = _make_message("t1")
	partition = await strategy.get_partition(msg, 4)
	assert 0 <= partition < 4


async def test_hash_partition_deterministic():
	strategy = HashPartitionStrategy()
	msg = _make_message("t1")
	p1 = await strategy.get_partition(msg, 4)
	p2 = await strategy.get_partition(msg, 4)
	assert p1 == p2


async def test_key_based_partition():
	strategy = KeyBasedPartitionStrategy(key_extractor=lambda m: m.task_name)
	msg = _make_message()
	partition = await strategy.get_partition(msg, 4)
	assert 0 <= partition < 4


async def test_round_robin_partition():
	strategy = RoundRobinPartitionStrategy()
	msg = _make_message()
	results = [await strategy.get_partition(msg, 3) for _ in range(6)]
	assert results == [0, 1, 2, 0, 1, 2]
