from __future__ import annotations

import asyncio

from dmq.callback import Callback, CallbackRule, on_event
from dmq.event_router import EventRouter
from dmq.events import QEventType
from dmq.events.core import QTaskCompleted, QTaskFailed, QTaskStarted


async def test_emit_and_route_to_callback():
	router = EventRouter()
	received = []

	class TestCallback(Callback):
		async def handle(self, event):
			received.append(event)

	router.register_callback(TestCallback())

	event = QTaskStarted(task_id="t1", task_name="test", timestamp=1.0, worker_id="w1")
	await router.emit(event)
	await asyncio.sleep(0.1)

	assert len(received) == 1
	assert received[0].task_id == "t1"


async def test_callback_with_rule_filters():
	router = EventRouter()
	received = []

	class FilteredCallback(Callback):
		def __init__(self):
			super().__init__()
			self.add_rule(CallbackRule(event_types={QEventType.TASK_COMPLETED}))

		async def handle(self, event):
			received.append(event)

	router.register_callback(FilteredCallback())

	# Should be filtered out
	started = QTaskStarted(task_id="t1", task_name="test", timestamp=1.0)
	await router.emit(started)
	await asyncio.sleep(0.1)
	assert len(received) == 0

	# Should pass filter
	completed = QTaskCompleted(task_id="t1", task_name="test", timestamp=1.0, duration=0.5)
	await router.emit(completed)
	await asyncio.sleep(0.1)
	assert len(received) == 1


async def test_callback_rule_matches():
	rule = CallbackRule(event_types={QEventType.TASK_FAILED})
	failed = QTaskFailed(task_id="t1", task_name="test", timestamp=1.0, exception="err", traceback="", retry_count=0)
	started = QTaskStarted(task_id="t1", task_name="test", timestamp=1.0)

	assert rule.matches(failed) is True
	assert rule.matches(started) is False


async def test_callback_rule_task_name_filter():
	rule = CallbackRule(task_names={"important.task"})
	event = QTaskStarted(task_id="t1", task_name="important.task", timestamp=1.0)
	other = QTaskStarted(task_id="t2", task_name="other.task", timestamp=1.0)

	assert rule.matches(event) is True
	assert rule.matches(other) is False


async def test_callback_rule_performance_threshold():
	rule = CallbackRule(performance_threshold=1.0)
	slow = QTaskCompleted(task_id="t1", task_name="test", timestamp=1.0, duration=2.0)
	fast = QTaskCompleted(task_id="t2", task_name="test", timestamp=1.0, duration=0.5)
	no_dur = QTaskStarted(task_id="t3", task_name="test", timestamp=1.0)

	assert rule.matches(slow) is True
	assert rule.matches(fast) is False
	assert rule.matches(no_dur) is False


async def test_on_event_decorator():
	@on_event(QEventType.TASK_COMPLETED)
	class MyCallback(Callback):
		async def handle(self, event):
			pass

	cb = MyCallback()
	assert len(cb.rules) == 1
	assert QEventType.TASK_COMPLETED in cb.rules[0].event_types


async def test_unregister_callback():
	router = EventRouter()

	class TestCallback(Callback):
		async def handle(self, event):
			pass

	cb = TestCallback()
	router.register_callback(cb)
	assert len(router._callbacks) == 1
	router.unregister_callback(cb)
	assert len(router._callbacks) == 0


async def test_callback_error_is_logged():
	router = EventRouter()

	class BrokenCallback(Callback):
		async def handle(self, event):
			raise RuntimeError("callback broke")

	router.register_callback(BrokenCallback())

	event = QTaskStarted(task_id="t1", task_name="test", timestamp=1.0)
	await router.emit(event)
	await asyncio.sleep(0.1)
	# Should not crash; error is logged


async def test_shutdown():
	router = EventRouter()
	await router.shutdown()
