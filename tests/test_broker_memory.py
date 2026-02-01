from __future__ import annotations

import asyncio

import pytest

from dmq.brokers.memory import InMemoryBroker


@pytest.fixture
def broker():
    return InMemoryBroker()


async def test_send_task_returns_id(broker):
    task_id = await broker.send_task("my.task", (1, 2), {"k": "v"}, {})
    assert isinstance(task_id, str)
    assert len(task_id) > 0


async def test_send_task_with_custom_id(broker):
    task_id = await broker.send_task("my.task", (), {}, {}, task_id="custom-123")
    assert task_id == "custom-123"


async def test_consume_yields_sent_task(broker):
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


async def test_ack_removes_from_processing(broker):
    await broker.send_task("t", (), {}, {}, task_id="t1")

    async for _ in broker.consume():
        break

    assert "t1" in broker._processing
    await broker.ack_task("t1")
    assert "t1" not in broker._processing


async def test_neg_ack_removes_from_processing(broker):
    await broker.send_task("t", (), {}, {}, task_id="t1")

    async for _ in broker.consume():
        break

    await broker.neg_ack_task("t1", requeue=False)
    assert "t1" not in broker._processing


async def test_send_task_propagates_retry_count(broker):
    await broker.send_task("t", (), {}, {"_retry_count": 2, "_max_retries": 5}, task_id="t1")

    async for msg in broker.consume():
        assert msg.retry_count == 2
        assert msg.max_retries == 5
        break


async def test_health_check(broker):
    assert await broker.health_check() is True


async def test_shutdown(broker):
    await broker.shutdown()
    assert broker._running is False


async def test_consume_tasks_callback(broker):
    await broker.send_task("t", (), {}, {}, task_id="t1")

    received = []

    async def cb(msg):
        received.append(msg)
        broker._running = False

    await broker.consume_tasks(cb)
    assert len(received) == 1
    assert received[0].task_id == "t1"


async def test_send_scheduled_task(broker):
    from dmq.types import DelaySchedule

    task_id = await broker.send_scheduled_task("t", (), {}, DelaySchedule(delay_seconds=0.0), {}, task_id="s1")
    assert task_id == "s1"
    # Give scheduler a moment to move it to the ready queue
    await asyncio.sleep(0.3)
    assert not broker._task_queue.empty()
    await broker.shutdown()
