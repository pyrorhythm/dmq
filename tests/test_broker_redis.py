from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest
from testcontainers.redis import RedisContainer

from dmq.brokers.redis import RedisBroker
from dmq.types import ETA, Cron, Delay, TaskMessage


@pytest.fixture
def redis_url(redis_container: RedisContainer) -> str:
	host = redis_container.get_container_host_ip()
	port = redis_container.get_exposed_port(6379)
	return f"redis://{host}:{port}"


@pytest.fixture
async def broker(redis_url: str) -> RedisBroker:
	b = RedisBroker(redis_url=redis_url, poll_interval=0.1)
	await b.redis.flushdb()
	yield b
	await b.shutdown()


async def test_send_task_returns_id(broker: RedisBroker) -> None:
	task_id = await broker.send_task("my.task", (1, 2), {"k": "v"}, {})
	assert isinstance(task_id, str)
	assert len(task_id) > 0


async def test_send_task_with_custom_id(broker: RedisBroker) -> None:
	task_id = await broker.send_task("my.task", (), {}, {}, task_id="custom-123")
	assert task_id == "custom-123"


async def test_consume_yields_sent_task(broker: RedisBroker) -> None:
	await broker.send_task("my.task", (1,), {"x": 2}, {}, task_id="t1")

	messages = []
	async for msg in broker.consume():
		messages.append(msg)
		break

	assert len(messages) == 1
	assert messages[0].task_id == "t1"
	assert messages[0].task_name == "my.task"
	assert messages[0].args == (1,)
	assert messages[0].kwargs == {"x": 2}


async def test_send_task_propagates_retry_count(broker: RedisBroker) -> None:
	await broker.send_task("t", (), {}, {"_retry_count": 2, "_max_retries": 5}, task_id="rt1")

	async for msg in broker.consume():
		assert msg.retry_count == 2
		assert msg.max_retries == 5
		break


async def test_health_check(broker: RedisBroker) -> None:
	assert await broker.health_check() is True


async def test_shutdown(broker: RedisBroker) -> None:
	b = RedisBroker(redis_url=broker._redis_manager._redis_url, poll_interval=0.1)
	await b.shutdown()
	assert b._running is False


async def test_consume_tasks_callback(broker: RedisBroker) -> None:
	await broker.send_task("t", (), {}, {}, task_id="cb1")

	received = []

	async def cb(msg: TaskMessage) -> None:
		received.append(msg)
		broker._running = False

	await broker.consume_tasks(cb)
	assert len(received) == 1
	assert received[0].task_id == "cb1"
	broker._running = True


async def test_queue_length(broker: RedisBroker) -> None:
	assert await broker.queue_length() == 0
	await broker.send_task("t", (), {}, {}, task_id="ql1")
	assert await broker.queue_length() == 1


async def test_send_scheduled_task_delay_zero(broker: RedisBroker) -> None:
	task_id = await broker.send_scheduled_task("t", (), {}, Delay(delay_seconds=0.0), {}, task_id="s1")
	assert task_id == "s1"
	await asyncio.sleep(0.5)
	assert await broker.queue_length() >= 1


async def test_send_scheduled_task_delay_future(broker: RedisBroker) -> None:
	await broker.send_scheduled_task("t", (), {}, Delay(delay_seconds=5.0), {}, task_id="s2")
	await asyncio.sleep(0.3)
	# Should still be in scheduled queue, not ready yet
	assert await broker.scheduled_count() >= 1
	assert await broker.queue_length() == 0


async def test_send_scheduled_task_eta_past(broker: RedisBroker) -> None:
	from datetime import UTC, datetime, timedelta

	past = datetime.now(UTC) - timedelta(seconds=10)
	await broker.send_scheduled_task("t", (), {}, ETA(eta=past), {}, task_id="s3")
	await asyncio.sleep(0.5)
	assert await broker.queue_length() >= 1


async def test_send_scheduled_task_eta_future(broker: RedisBroker) -> None:
	from datetime import UTC, datetime, timedelta

	future = datetime.now(UTC) + timedelta(seconds=60)
	await broker.send_scheduled_task("t", (), {}, ETA(eta=future), {}, task_id="s4")
	await asyncio.sleep(0.3)
	assert await broker.scheduled_count() >= 1
	assert await broker.queue_length() == 0


async def test_scheduled_task_consumed_after_delay(broker: RedisBroker) -> None:
	await broker.send_scheduled_task("t", (), {}, Delay(delay_seconds=0.2), {}, task_id="sd1")
	# Not ready yet
	assert await broker.queue_length() == 0
	await asyncio.sleep(0.8)
	assert await broker.queue_length() >= 1
	# Consume it and verify
	async for msg in broker.consume():
		assert msg.task_id == "sd1"
		break


async def test_multiple_tasks_ordering(broker: RedisBroker) -> None:
	for i in range(5):
		await broker.send_task("t", (), {}, {}, task_id=f"order-{i}")

	ids = []
	async for msg in broker.consume():
		ids.append(msg.task_id)
		if len(ids) == 5:
			break

	assert ids == [f"order-{i}" for i in range(5)]


async def test_send_scheduled_task_cron(broker: RedisBroker) -> None:
	sched = Cron(cron_expr="* * * * *")
	task_id = await broker.send_scheduled_task("t", (), {}, sched, {}, task_id="c1")
	assert task_id == "c1"
	assert await broker.scheduled_count() >= 1


async def test_send_scheduled_task_cron_with_delay(broker: RedisBroker) -> None:
	sched = Cron(cron_expr="* * * * *", base=datetime.now() + timedelta(hours=2))
	await broker.send_scheduled_task("t", (), {}, sched, {}, task_id="c2")
	assert await broker.scheduled_count() >= 1
	# Should not be ready (2h+ in the future)
	assert await broker.queue_length() == 0


async def test_send_scheduled_task_cron_invalid(broker: RedisBroker) -> None:
	sched = Cron(cron_expr="not a cron")
	with pytest.raises(ValueError, match="Exactly 5, 6 or 7 columns has to be specified for iterator expression."):
		await broker.send_scheduled_task("t", (), {}, sched, {}, task_id="bad")
