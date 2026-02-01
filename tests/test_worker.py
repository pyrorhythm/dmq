from __future__ import annotations

import asyncio

import pytest

from dmq.backends.memory import InMemoryResultBackend
from dmq.brokers.memory import InMemoryBroker
from dmq.manager import QManager
from dmq.serializers.msgpack import MsgpackSerializer
from dmq.workers._async import QAsyncWorker


@pytest.fixture
def manager():
    return QManager(broker=InMemoryBroker(), result_backend=InMemoryResultBackend(), serializer=MsgpackSerializer())


async def test_worker_executes_task(manager):
    @manager.register(qname="test.add")
    async def add(x: int, y: int) -> int:
        return x + y

    handle = await add.q(3, 4)

    worker = QAsyncWorker(manager=manager, worker_id="w1")
    worker_task = asyncio.create_task(worker.start())

    # Dispatch from broker to worker
    async for msg in manager.broker.consume():
        await worker.submit(msg)
        break

    result = await manager.result_backend.get_result(handle.id, timeout=2.0)
    assert result == 7

    await worker.stop()
    worker_task.cancel()


async def test_worker_handles_failure(manager):
    @manager.register(qname="test.fail")
    async def failing_task() -> None:
        raise ValueError("boom")

    handle = await failing_task.q()

    worker = QAsyncWorker(manager=manager, worker_id="w1")
    worker_task = asyncio.create_task(worker.start())

    # The task has max_retries=3 by default, so it will be re-sent.
    # Consume and submit until retries are exhausted (original + 3 retries = 4 attempts).
    for _ in range(4):
        async for msg in manager.broker.consume():
            await worker.submit(msg)
            break
        await asyncio.sleep(0.1)

    result = await manager.result_backend.get_result(handle.id, timeout=2.0)
    assert "boom" in result  # stored as str(exception)

    await worker.stop()
    worker_task.cancel()


async def test_worker_retries_on_failure(manager):
    call_count = 0

    @manager.register(qname="test.retry")
    async def flaky_task() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("not yet")
        return "success"

    handle = await flaky_task.q()

    worker = QAsyncWorker(manager=manager, worker_id="w1")
    worker_task = asyncio.create_task(worker.start())

    # Process messages: original + retries
    for _ in range(3):
        async for msg in manager.broker.consume():
            await worker.submit(msg)
            break
        await asyncio.sleep(0.2)

    result = await manager.result_backend.get_result(handle.id, timeout=2.0)
    assert result == "success"
    assert call_count == 3

    await worker.stop()
    worker_task.cancel()


async def test_worker_task_not_found(manager):
    from dmq.types import TaskMessage

    worker = QAsyncWorker(manager=manager, worker_id="w1")
    worker_task = asyncio.create_task(worker.start())

    msg = TaskMessage(task_id="t1", task_name="nonexistent.task", args=(), kwargs={}, options={})
    await worker.submit(msg)
    await asyncio.sleep(0.3)

    # Should not crash; task just not found
    assert await manager.result_backend.result_exists("t1") is False

    await worker.stop()
    worker_task.cancel()


async def test_worker_properties(manager):
    worker = QAsyncWorker(manager=manager, worker_id="w1")
    assert worker.active_task_count == 0
    assert worker.queue_size == 0
    assert worker.load == 0
