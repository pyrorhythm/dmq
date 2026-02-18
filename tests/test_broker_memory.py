from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from dmq.brokers.memory import InMemoryBroker


@pytest.fixture
def broker() -> InMemoryBroker:
	return InMemoryBroker()


async def test_send_task_returns_id(broker: InMemoryBroker):
	task_id = await broker.send_task("my.task", (1, 2), {"k": "v"}, {})
	assert isinstance(task_id, str)
	assert len(task_id) > 0


async def test_send_task_with_custom_id(broker: InMemoryBroker):
	task_id = await broker.send_task("my.task", (), {}, {}, task_id="custom-123")
	assert task_id == "custom-123"


async def test_consume_yields_sent_task(broker: InMemoryBroker):
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


async def test_ack_removes_from_processing(broker: InMemoryBroker):
	await broker.send_task("t", (), {}, {}, task_id="t1")

	async for _ in broker.consume():
		break

	assert "t1" in broker._processing
	await broker.ack_task("t1")
	assert "t1" not in broker._processing


async def test_neg_ack_removes_from_processing(broker: InMemoryBroker):
	await broker.send_task("t", (), {}, {}, task_id="t1")

	async for _ in broker.consume():
		break

	await broker.neg_ack_task("t1", requeue=False)
	assert "t1" not in broker._processing


async def test_send_task_propagates_retry_count(broker: InMemoryBroker):
	await broker.send_task("t", (), {}, {"_retry_count": 2, "_max_retries": 5}, task_id="t1")

	async for msg in broker.consume():
		assert msg.retry_count == 2
		assert msg.max_retries == 5
		break


async def test_health_check(broker: InMemoryBroker):
	assert await broker.health_check() is True


async def test_shutdown(broker: InMemoryBroker):
	await broker.shutdown()
	assert broker._running is False


async def test_consume_tasks_callback(broker: InMemoryBroker):
	await broker.send_task("t", (), {}, {}, task_id="t1")

	received = []

	async def cb(msg):
		received.append(msg)
		broker._running = False

	await broker.consume_tasks(cb)
	assert len(received) == 1
	assert received[0].task_id == "t1"


async def test_send_scheduled_task_delay_zero(broker: InMemoryBroker):
	from dmq.types import Delay

	task_id = await broker.send_scheduled_task("t", (), {}, Delay(delay_seconds=0.0), {}, task_id="s1")
	assert task_id == "s1"
	await asyncio.sleep(0.3)
	assert not broker._task_queue.empty()
	await broker.shutdown()


async def test_send_scheduled_task_delay_future(broker: InMemoryBroker):
	from dmq.types import Delay

	await broker.send_scheduled_task("t", (), {}, Delay(delay_seconds=5.0), {}, task_id="s2")
	await asyncio.sleep(0.2)
	assert broker._task_queue.empty()
	await broker.shutdown()


async def test_send_scheduled_task_eta_past(broker: InMemoryBroker):
	from datetime import UTC, datetime, timedelta

	from dmq.types import ETA

	past = datetime.now(UTC) - timedelta(seconds=10)
	await broker.send_scheduled_task("t", (), {}, ETA(eta=past), {}, task_id="s3")
	await asyncio.sleep(0.3)
	assert not broker._task_queue.empty()
	await broker.shutdown()


async def test_send_scheduled_task_eta_future(broker: InMemoryBroker):
	from datetime import UTC, datetime, timedelta

	from dmq.types import ETA

	future = datetime.now(UTC) + timedelta(seconds=60)
	await broker.send_scheduled_task("t", (), {}, ETA(eta=future), {}, task_id="s4")
	await asyncio.sleep(0.2)
	assert broker._task_queue.empty()
	await broker.shutdown()


async def test_scheduled_task_consumed_after_delay(broker: InMemoryBroker):
	from dmq.types import Delay

	await broker.send_scheduled_task("t", (), {}, Delay(delay_seconds=0.2), {}, task_id="sd1")
	assert broker._task_queue.empty()
	await asyncio.sleep(0.5)
	assert not broker._task_queue.empty()
	async for msg in broker.consume():
		assert msg.task_id == "sd1"
		break
	await broker.shutdown()


async def test_send_scheduled_task_cron(broker: InMemoryBroker):
	from datetime import UTC, datetime

	from dmq.types import Cron

	# Every minute — next fire should be within ~60s
	task_id = await broker.send_scheduled_task("t", (), {}, Cron(cron_expr="* * * * *"), {}, task_id="c1")
	assert task_id == "c1"
	# The task should be in the scheduled queue with a future timestamp
	execute_at, msg = await broker._scheduled_queue.get()
	assert execute_at > datetime.now(UTC).timestamp()
	assert execute_at <= datetime.now(UTC).timestamp() + 61
	assert broker._msgmap[(execute_at, msg)].task_id == "c1"
	await broker.shutdown()


async def test_send_scheduled_task_cron_with_delay(broker: InMemoryBroker):
	from datetime import UTC, datetime

	from dmq.types import Cron

	# Every minute, but delayed 2 hours — next fire is at least 2h from now
	sched = Cron(cron_expr="* * * * *", base=datetime.now() + timedelta(hours=2))
	await broker.send_scheduled_task("t", (), {}, sched, {}, task_id="c2")
	execute_at, _ = await broker._scheduled_queue.get()
	now = datetime.now(UTC).timestamp()
	assert execute_at >= now + 7200
	assert execute_at <= now + 7200 + 61
	await broker.shutdown()


async def test_send_scheduled_task_cron_every_4h_with_1h_delay(broker: InMemoryBroker):
	from datetime import UTC, datetime

	from dmq.types import Cron

	sched = Cron(cron_expr="0 */4 * * *", base=datetime.now() + timedelta(hours=2))
	await broker.send_scheduled_task("t", (), {}, sched, {}, task_id="c3")
	execute_at, _ = await broker._scheduled_queue.get()
	now = datetime.now(UTC).timestamp()
	# Must be at least 1h from now (the delay), and aligned to a 4h boundary
	assert execute_at >= now + 3600
	await broker.shutdown()


async def test_send_scheduled_task_cron_invalid(broker: InMemoryBroker):
	from dmq.types import Cron

	with pytest.raises(ValueError, match="Exactly 5, 6 or 7 columns has to be specified for iterator expression."):
		await broker.send_scheduled_task("t", (), {}, Cron(cron_expr="not a cron"), {}, task_id="bad")
