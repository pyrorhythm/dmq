from __future__ import annotations

import pytest

from dmq.backends.memory import InMemoryResultBackend
from dmq.brokers.memory import InMemoryBroker
from dmq.manager import QManager
from dmq.serializers.msgpack import MsgpackSerializer
from dmq.task import QTask


@pytest.fixture
def manager():
	return QManager(broker=InMemoryBroker(), result_backend=InMemoryResultBackend(), serializer=MsgpackSerializer())


def test_register_sync_function(manager):
	@manager.register
	def my_task(x: int) -> int:
		return x * 2

	assert isinstance(my_task, QTask)
	assert "my_task" in str(list(manager.task_registry.keys()))


def test_register_async_function(manager):
	@manager.register
	async def my_async_task(x: int) -> int:
		return x * 2

	assert isinstance(my_async_task, QTask)


def test_register_with_custom_name(manager):
	@manager.register(qname="custom.name")
	async def my_task(x: int) -> int:
		return x

	assert "custom.name" in manager.task_registry


def test_get_task_found(manager):
	@manager.register(qname="test.task")
	async def my_task() -> None:
		pass

	assert manager.get_task("test.task") is not None


def test_get_task_not_found(manager):
	assert manager.get_task("nonexistent") is None


async def test_task_direct_call(manager):
	@manager.register
	async def add(x: int, y: int) -> int:
		return x + y

	result = await add(3, 4)
	assert result == 7


async def test_task_direct_call_sync(manager):
	@manager.register
	def multiply(x: int, y: int) -> int:
		return x * y

	result = await multiply(3, 4)
	assert result == 12


async def test_task_enqueue(manager):
	@manager.register(qname="enqueue.test")
	async def my_task(x: int) -> int:
		return x

	handle = await my_task.q(42)
	assert handle.id is not None
	assert isinstance(handle.id, str)


async def test_register_callback(manager):
	from dmq.callback import Callback
	from dmq.events import QEvent

	@manager.callback
	class MyCallback(Callback):
		async def handle(self, event: QEvent) -> None:
			pass

	assert len(manager.event_router._callbacks) == 1
