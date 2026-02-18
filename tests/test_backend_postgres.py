from __future__ import annotations

import asyncio

import pytest

from dmq.backends.postgres_backend import PostgresResultBackend, RetentionPolicy


@pytest.fixture
def dsn(postgres_container):
	host = postgres_container.get_container_host_ip()
	port = postgres_container.get_exposed_port(5432)
	user = postgres_container.username
	password = postgres_container.password
	dbname = postgres_container.dbname
	return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


@pytest.fixture
async def backend(dsn):
	b = PostgresResultBackend(dsn=dsn)
	await b.connect()
	yield b
	await b.close()


async def test_store_and_get_result(backend):
	await backend.store_result("t1", {"value": 42}, task_name="test")
	result = await backend.get_result("t1")
	assert result == {"value": 42}


async def test_store_result_statuses(backend):
	await backend.store_result("t2", "ok", status="SUCCESS", task_name="test")
	await backend.store_result("t3", "fail", status="FAILED", task_name="test")
	assert await backend.get_result("t2") == "ok"
	assert await backend.get_result("t3") == "fail"


async def test_get_result_waits_for_store(backend):
	async def delayed_store():
		await asyncio.sleep(0.3)
		await backend.store_result("t4", "delayed", task_name="test")

	asyncio.create_task(delayed_store())
	result = await backend.get_result("t4", timeout=5.0)
	assert result == "delayed"


async def test_get_result_timeout(backend):
	with pytest.raises(TimeoutError, match="not available"):
		await backend.get_result("nonexistent", timeout=0.3)


async def test_delete_result(backend):
	await backend.store_result("t5", "val", task_name="test")
	await backend.delete_result("t5")
	assert await backend.result_exists("t5") is False


async def test_result_exists(backend):
	assert await backend.result_exists("t6") is False
	await backend.store_result("t6", "val", task_name="test")
	assert await backend.result_exists("t6") is True


async def test_get_result_immediate_if_already_stored(backend):
	await backend.store_result("t7", 99, task_name="test")
	result = await backend.get_result("t7")
	assert result == 99


async def test_store_various_types(backend):
	await backend.store_result("str1", "hello", task_name="test")
	assert await backend.get_result("str1") == "hello"

	await backend.store_result("int1", 123, task_name="test")
	assert await backend.get_result("int1") == 123

	await backend.store_result("list1", [1, 2, 3], task_name="test")
	assert await backend.get_result("list1") == [1, 2, 3]

	await backend.store_result("none1", None, task_name="test")
	assert await backend.get_result("none1") is None


async def test_upsert_result(backend):
	await backend.store_result("upsert1", "first", task_name="test")
	assert await backend.get_result("upsert1") == "first"
	await backend.store_result("upsert1", "second", task_name="test")
	assert await backend.get_result("upsert1") == "second"


async def test_workflow_state(backend):
	class FakeState:
		workflow_name = "test_wf"
		current_nodes = ["a"]
		completed_nodes = ["b"]
		results = {"b": 1}

	await backend.store_workflow_state("wf1", FakeState())
	state = await backend.get_workflow_state("wf1")
	assert state["workflow_name"] == "test_wf"
	assert state["current_nodes"] == ["a"]
	assert state["completed_nodes"] == ["b"]
	assert state["results"] == {"b": 1}


async def test_get_workflow_state_missing(backend):
	with pytest.raises(KeyError, match="workflow state not found"):
		await backend.get_workflow_state("missing_wf")


async def test_cleanup_expired(backend):
	b = PostgresResultBackend(dsn=backend.dsn, retention_policy=RetentionPolicy(success_ttl=-1))
	await b.connect()
	await b.store_result("exp1", "val", task_name="test")
	deleted = await b.cleanup_expired()
	assert deleted >= 1
	assert await b.result_exists("exp1") is False
	await b.close()


async def test_type_serialization_int(dsn):
	b = PostgresResultBackend(dsn=dsn, type_serialization=True)
	await b.connect()
	await b.store_result("typed_int", 42, task_name="test")
	result = await b.get_result("typed_int")
	assert result == 42
	assert isinstance(result, int)
	await b.close()


async def test_type_serialization_str(dsn):
	b = PostgresResultBackend(dsn=dsn, type_serialization=True)
	await b.connect()
	await b.store_result("typed_str", "hello", task_name="test")
	result = await b.get_result("typed_str")
	assert result == "hello"
	assert isinstance(result, str)
	await b.close()


async def test_type_serialization_float(dsn):
	b = PostgresResultBackend(dsn=dsn, type_serialization=True)
	await b.connect()
	await b.store_result("typed_float", 3.14, task_name="test")
	result = await b.get_result("typed_float")
	assert result == 3.14
	assert isinstance(result, float)
	await b.close()


async def test_type_serialization_list(dsn):
	b = PostgresResultBackend(dsn=dsn, type_serialization=True)
	await b.connect()
	await b.store_result("typed_list", [1, "two", 3.0], task_name="test")
	result = await b.get_result("typed_list")
	assert result == [1, "two", 3.0]
	assert isinstance(result, list)
	await b.close()


async def test_type_serialization_dict(dsn):
	b = PostgresResultBackend(dsn=dsn, type_serialization=True)
	await b.connect()
	await b.store_result("typed_dict", {"a": 1, "b": [2, 3]}, task_name="test")
	result = await b.get_result("typed_dict")
	assert result == {"a": 1, "b": [2, 3]}
	assert isinstance(result, dict)
	await b.close()


async def test_type_serialization_custom_struct(dsn):
	from tests.custom_types import UserResult

	b = PostgresResultBackend(dsn=dsn, type_serialization=True)
	await b.connect()
	user = UserResult(name="alice", age=30, email="alice@example.com")
	await b.store_result("typed_user", user, task_name="test")
	result = await b.get_result("typed_user")
	assert isinstance(result, UserResult)
	assert result.name == "alice"
	assert result.age == 30
	assert result.email == "alice@example.com"
	await b.close()


async def test_type_serialization_custom_nested(dsn):
	from tests.custom_types import NestedResult, OrderResult, UserResult

	b = PostgresResultBackend(dsn=dsn, type_serialization=True)
	await b.connect()
	nested = NestedResult(
		user=UserResult(name="bob", age=25), order=OrderResult(order_id="o1", total=99.99, items=["widget"])
	)
	await b.store_result("typed_nested", nested, task_name="test")
	result = await b.get_result("typed_nested")
	assert isinstance(result, NestedResult)
	assert result.user.name == "bob"
	assert result.order.total == 99.99
	await b.close()
