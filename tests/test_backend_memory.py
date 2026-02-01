from __future__ import annotations

import asyncio

import pytest

from dmq.backends.memory import InMemoryResultBackend


@pytest.fixture
def backend():
    return InMemoryResultBackend()


async def test_store_and_get_result(backend):
    await backend.store_result("t1", {"value": 42})
    result = await backend.get_result("t1")
    assert result == {"value": 42}


async def test_store_result_accepts_status(backend):
    # Should not raise — status is accepted but ignored
    await backend.store_result("t1", "ok", status="SUCCESS")
    await backend.store_result("t2", "fail", status="FAILED")
    assert await backend.get_result("t1") == "ok"
    assert await backend.get_result("t2") == "fail"


async def test_get_result_waits_for_store(backend):
    async def delayed_store():
        await asyncio.sleep(0.1)
        await backend.store_result("t1", "delayed")

    asyncio.create_task(delayed_store())
    result = await backend.get_result("t1", timeout=2.0)
    assert result == "delayed"


async def test_get_result_timeout(backend):
    with pytest.raises(TimeoutError, match="timeout"):
        await backend.get_result("nonexistent", timeout=0.1)


async def test_delete_result(backend):
    await backend.store_result("t1", "val")
    await backend.delete_result("t1")
    assert await backend.result_exists("t1") is False


async def test_result_exists(backend):
    assert await backend.result_exists("t1") is False
    await backend.store_result("t1", "val")
    assert await backend.result_exists("t1") is True


async def test_get_result_immediate_if_already_stored(backend):
    await backend.store_result("t1", 99)
    # Should return immediately without needing timeout
    result = await backend.get_result("t1")
    assert result == 99
