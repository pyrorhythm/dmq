from __future__ import annotations

import asyncio

import pytest

from dmq.backends.redis_backend import RedisResultBackend


@pytest.fixture
def redis_url(redis_container):
	host = redis_container.get_container_host_ip()
	port = redis_container.get_exposed_port(6379)
	return f"redis://{host}:{port}"


@pytest.fixture
async def backend(redis_url):
	b = RedisResultBackend(redis_url=redis_url, default_ttl=60)
	await b.redis.flushdb()
	yield b
	await b.redis.aclose()


async def test_store_and_get_result(backend):
	await backend.store_result("t1", {"value": 42})
	result = await backend.get_result("t1")
	assert result == {"value": 42}


async def test_store_result_accepts_status(backend):
	await backend.store_result("t2", "ok", status="SUCCESS")
	await backend.store_result("t3", "fail", status="FAILED")
	assert await backend.get_result("t2") == "ok"
	assert await backend.get_result("t3") == "fail"


async def test_get_result_waits_for_store(backend):
	async def delayed_store():
		await asyncio.sleep(0.2)
		await backend.store_result("t4", "delayed")

	asyncio.create_task(delayed_store())
	result = await backend.get_result("t4", timeout=5.0)
	assert result == "delayed"


async def test_get_result_timeout(backend):
	with pytest.raises(TimeoutError, match="not available"):
		await backend.get_result("nonexistent", timeout=0.2)


async def test_get_result_raises_without_timeout(backend):
	with pytest.raises(KeyError, match="result not found"):
		await backend.get_result("missing")


async def test_delete_result(backend):
	await backend.store_result("t5", "val")
	await backend.delete_result("t5")
	assert await backend.result_exists("t5") is False


async def test_result_exists(backend):
	assert await backend.result_exists("t6") is False
	await backend.store_result("t6", "val")
	assert await backend.result_exists("t6") is True


async def test_get_result_immediate_if_already_stored(backend):
	await backend.store_result("t7", 99)
	result = await backend.get_result("t7")
	assert result == 99


async def test_type_serialization_int(redis_url):
	b = RedisResultBackend(redis_url=redis_url, default_ttl=60, type_serialization=True)
	await b.redis.flushdb()
	await b.store_result("typed_int", 42)
	result = await b.get_result("typed_int")
	assert result == 42
	assert isinstance(result, int)
	await b.redis.aclose()


async def test_type_serialization_str(redis_url):
	b = RedisResultBackend(redis_url=redis_url, default_ttl=60, type_serialization=True)
	await b.redis.flushdb()
	await b.store_result("typed_str", "hello")
	result = await b.get_result("typed_str")
	assert result == "hello"
	assert isinstance(result, str)
	await b.redis.aclose()


async def test_type_serialization_float(redis_url):
	b = RedisResultBackend(redis_url=redis_url, default_ttl=60, type_serialization=True)
	await b.redis.flushdb()
	await b.store_result("typed_float", 3.14)
	result = await b.get_result("typed_float")
	assert result == 3.14
	assert isinstance(result, float)
	await b.redis.aclose()


async def test_type_serialization_list(redis_url):
	b = RedisResultBackend(redis_url=redis_url, default_ttl=60, type_serialization=True)
	await b.redis.flushdb()
	await b.store_result("typed_list", [1, "two", 3.0])
	result = await b.get_result("typed_list")
	assert result == [1, "two", 3.0]
	assert isinstance(result, list)
	await b.redis.aclose()


async def test_type_serialization_dict(redis_url):
	b = RedisResultBackend(redis_url=redis_url, default_ttl=60, type_serialization=True)
	await b.redis.flushdb()
	await b.store_result("typed_dict", {"a": 1, "b": [2, 3]})
	result = await b.get_result("typed_dict")
	assert result == {"a": 1, "b": [2, 3]}
	assert isinstance(result, dict)
	await b.redis.aclose()


async def test_type_serialization_custom_struct(redis_url):
	from tests.custom_types import UserResult

	b = RedisResultBackend(redis_url=redis_url, default_ttl=60, type_serialization=True)
	await b.redis.flushdb()
	user = UserResult(name="alice", age=30, email="alice@example.com")
	await b.store_result("typed_user", user)
	result = await b.get_result("typed_user")
	assert isinstance(result, UserResult)
	assert result.name == "alice"
	assert result.age == 30
	assert result.email == "alice@example.com"
	await b.redis.aclose()


async def test_type_serialization_custom_struct_nested(redis_url):
	from tests.custom_types import NestedResult, OrderResult, UserResult

	b = RedisResultBackend(redis_url=redis_url, default_ttl=60, type_serialization=True)
	await b.redis.flushdb()
	nested = NestedResult(
		user=UserResult(name="bob", age=25), order=OrderResult(order_id="o1", total=99.99, items=["widget"])
	)
	await b.store_result("typed_nested", nested)
	result = await b.get_result("typed_nested")
	assert isinstance(result, NestedResult)
	assert result.user.name == "bob"
	assert result.order.total == 99.99
	await b.redis.aclose()


async def test_store_various_types(backend):
	await backend.store_result("str1", "hello")
	assert await backend.get_result("str1") == "hello"

	await backend.store_result("int1", 123)
	assert await backend.get_result("int1") == 123

	await backend.store_result("list1", [1, 2, 3])
	assert await backend.get_result("list1") == [1, 2, 3]

	await backend.store_result("bool1", True)
	assert await backend.get_result("bool1") is True
