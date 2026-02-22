from __future__ import annotations

import asyncio

import pytest

from dmq.backends.memory import InMemoryResultBackend
from dmq.brokers.memory import InMemoryBroker
from dmq.callback import Callback, CallbackRule
from dmq.events.core import QEventType
from dmq.manager import QManager
from dmq.serializers.msgpack import MsgpackSerializer
from dmq.workers._async import QAsyncWorker
from dmq.workflow import Workflow, _ConditionalBuilder


@pytest.fixture
def manager():
	return QManager(broker=InMemoryBroker(), result_backend=InMemoryResultBackend(), serializer=MsgpackSerializer())


async def _process_messages(manager, worker, count) -> None:
	"""Consume *count* messages from the broker and submit them to the worker,
	leaving time between each for event propagation."""
	for _ in range(count):
		async for msg in manager.broker.consume():
			await worker.submit(msg)
			break
		await asyncio.sleep(0.15)


# ---------------------------------------------------------------------------
# Builder API tests
# ---------------------------------------------------------------------------


def test_workflow_step_builds(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	wf = Workflow("simple").step(task_a)
	assert wf.name == "simple"
	assert len(wf._steps) == 1


def test_workflow_parallel_requires_two_tasks(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	with pytest.raises(ValueError, match="at least 2"):
		Workflow("bad").parallel(task_a)


def test_workflow_chain_builds(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	@manager.register(qname="t.b")
	async def task_b(x: int) -> int:
		return x

	@manager.register(qname="t.c")
	async def task_c(x: int) -> int:
		return x

	wf = Workflow("chain").step(task_a).step(task_b).step(task_c)
	assert len(wf._steps) == 3


def test_workflow_parallel_builds(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	@manager.register(qname="t.b")
	async def task_b(x: int) -> int:
		return x

	@manager.register(qname="t.c")
	async def task_c(x: int) -> int:
		return x

	wf = Workflow("par").step(task_a).parallel(task_b, task_c)
	assert len(wf._steps) == 2


def test_workflow_conditional_builds(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	@manager.register(qname="t.b")
	async def task_b(x: int) -> int:
		return x

	@manager.register(qname="t.c")
	async def task_c(x: int) -> int:
		return x

	wf = Workflow("cond").step(task_a).when(lambda r: r > 10).then(task_b).otherwise(task_c)
	assert len(wf._steps) == 2  # task_a step + conditional step


def test_workflow_conditional_multi_branch(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	@manager.register(qname="t.b")
	async def task_b(x: int) -> int:
		return x

	@manager.register(qname="t.c")
	async def task_c(x: int) -> int:
		return x

	@manager.register(qname="t.d")
	async def task_d(x: int) -> int:
		return x

	wf = (
		Workflow("multi_cond")
		.step(task_a)
		.when(lambda r: r == "premium")
		.then(task_b)
		.when(lambda r: r == "standard")
		.then(task_c)
		.otherwise(task_d)
	)
	assert len(wf._steps) == 2


def test_conditional_then_without_when_raises(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	builder = _ConditionalBuilder(Workflow("bad"))
	with pytest.raises(ValueError, match=".then\\(\\) must follow .when\\(\\)"):
		builder.then(task_a)


def test_conditional_when_without_then_raises(manager):
	builder = _ConditionalBuilder(Workflow("bad"))
	builder._pending_predicate = lambda _r: True
	with pytest.raises(ValueError, match=".when\\(\\) called without"):
		builder.when(lambda _r: False)


def test_conditional_finalize_without_then_raises(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	builder = _ConditionalBuilder(Workflow("bad"))
	builder._pending_predicate = lambda _r: True
	with pytest.raises(ValueError, match=".when\\(\\) without"):
		builder.otherwise(task_a)


def test_conditional_step_continues_workflow(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	@manager.register(qname="t.b")
	async def task_b(x: int) -> int:
		return x

	@manager.register(qname="t.final")
	async def task_final(x: int) -> int:
		return x

	wf = Workflow("cond_then_step").when(lambda r: r > 0).then(task_a).otherwise(task_b).step(task_final)
	# conditional + task_final
	assert len(wf._steps) == 2


def test_conditional_parallel_continues_workflow(manager):
	@manager.register(qname="t.a")
	async def task_a(x: int) -> int:
		return x

	@manager.register(qname="t.b")
	async def task_b(x: int) -> int:
		return x

	@manager.register(qname="t.c")
	async def task_c(x: int) -> int:
		return x

	wf = Workflow("cond_then_par").when(lambda r: r > 0).then(task_a).otherwise(task_b).parallel(task_a, task_c)
	# conditional + parallel
	assert len(wf._steps) == 2


# ---------------------------------------------------------------------------
# Execution tests
# ---------------------------------------------------------------------------


async def test_single_step_workflow(manager):
	@manager.register(qname="wf.double")
	async def double(x: int) -> int:
		return x * 2

	wf = Workflow("single").step(double)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 21)
	assert handle.id.startswith("wf-")

	await _process_messages(manager, worker, 1)

	result = await handle.result(timeout=2.0)
	assert result == 42

	await worker.stop()
	worker_task.cancel()


async def test_chain_workflow(manager):
	@manager.register(qname="wf.add_one")
	async def add_one(x: int) -> int:
		return x + 1

	@manager.register(qname="wf.triple")
	async def triple(x: int) -> int:
		return x * 3

	@manager.register(qname="wf.to_str")
	async def to_str(x: int) -> str:
		return f"result={x}"

	wf = Workflow("chain").step(add_one).step(triple).step(to_str)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 10)

	# 3 steps: add_one(10)=11, triple(11)=33, to_str(33)="result=33"
	await _process_messages(manager, worker, 3)

	result = await handle.result(timeout=2.0)
	assert result == "result=33"

	await worker.stop()
	worker_task.cancel()


async def test_parallel_workflow(manager):
	@manager.register(qname="wf.double")
	async def double(x: int) -> int:
		return x * 2

	@manager.register(qname="wf.square")
	async def square(x: int) -> int:
		return x * x

	@manager.register(qname="wf.negate")
	async def negate(x: int) -> int:
		return -x

	@manager.register(qname="wf.collect")
	async def collect(results: list) -> str:
		return ",".join(str(r) for r in results)

	wf = Workflow("par").step(double).parallel(double, square, negate).step(collect)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 5)

	# step 1: double(5)=10  -> 1 message
	# step 2: parallel(double(10), square(10), negate(10)) -> 3 messages
	# step 3: collect([20, 100, -10]) -> 1 message
	await _process_messages(manager, worker, 5)

	result = await handle.result(timeout=2.0)
	assert result == "20,100,-10"

	await worker.stop()
	worker_task.cancel()


async def test_conditional_first_branch(manager):
	@manager.register(qname="wf.classify")
	async def classify(x: int) -> str:
		return "big" if x > 100 else "small"

	@manager.register(qname="wf.big_handler")
	async def big_handler(label: str) -> str:
		return f"{label}:processed_as_big"

	@manager.register(qname="wf.small_handler")
	async def small_handler(label: str) -> str:
		return f"{label}:processed_as_small"

	wf = (
		Workflow("cond")
		.step(classify)
		.when(lambda r: r == "big")
		.then(big_handler)
		.when(lambda r: r == "small")
		.then(small_handler)
		.otherwise(small_handler)
	)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 200)

	# step 1: classify(200)="big"  -> 1 message
	# step 2: conditional picks big_handler("big") -> 1 message
	await _process_messages(manager, worker, 2)

	result = await handle.result(timeout=2.0)
	assert result == "big:processed_as_big"

	await worker.stop()
	worker_task.cancel()


async def test_conditional_otherwise_branch(manager):
	@manager.register(qname="wf.classify")
	async def classify(x: int) -> str:
		return "unknown"

	@manager.register(qname="wf.known")
	async def known(label: str) -> str:
		return f"known:{label}"

	@manager.register(qname="wf.fallback")
	async def fallback(label: str) -> str:
		return f"fallback:{label}"

	wf = Workflow("cond_otherwise").step(classify).when(lambda r: r == "big").then(known).otherwise(fallback)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 50)

	await _process_messages(manager, worker, 2)

	result = await handle.result(timeout=2.0)
	assert result == "fallback:unknown"

	await worker.stop()
	worker_task.cancel()


async def test_conditional_then_continues_chain(manager):
	@manager.register(qname="wf.first")
	async def first(x: int) -> int:
		return x + 1

	@manager.register(qname="wf.branch_a")
	async def branch_a(x: int) -> int:
		return x * 10

	@manager.register(qname="wf.branch_b")
	async def branch_b(x: int) -> int:
		return x * 100

	@manager.register(qname="wf.finalize")
	async def finalize(x: int) -> str:
		return f"done:{x}"

	wf = Workflow("cond_chain").step(first).when(lambda r: r > 5).then(branch_a).otherwise(branch_b).step(finalize)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	# x=10 -> first(10)=11 -> 11>5 -> branch_a(11)=110 -> finalize(110)="done:110"
	handle = await wf.run(manager, 10)
	await _process_messages(manager, worker, 3)

	result = await handle.result(timeout=2.0)
	assert result == "done:110"

	await worker.stop()
	worker_task.cancel()


async def test_workflow_with_kwargs(manager):
	@manager.register(qname="wf.greet")
	async def greet(name: str, greeting: str = "Hello") -> str:
		return f"{greeting}, {name}!"

	wf = Workflow("kwargs").step(greet)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, "World", greeting="Hi")
	await _process_messages(manager, worker, 1)

	result = await handle.result(timeout=2.0)
	assert result == "Hi, World!"

	await worker.stop()
	worker_task.cancel()


async def test_workflow_failure_propagates(manager):
	@manager.register(qname="wf.ok")
	async def ok_task(x: int) -> int:
		return x

	@manager.register(qname="wf.boom")
	async def boom_task(x: int) -> int:
		raise ValueError("workflow step exploded")

	@manager.register(qname="wf.never")
	async def never_task(x: int) -> int:
		return x

	wf = Workflow("failing").step(ok_task).step(boom_task).step(never_task)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 42)

	# step 1: ok_task(42) -> succeeds  (1 message)
	# step 2: boom_task(42) -> fails (retries 3 times, then final failure)
	# original + 3 retries = 4 executions for the failing step
	await _process_messages(manager, worker, 5)

	result = await handle.result(timeout=2.0)
	assert "workflow step exploded" in str(result)

	await worker.stop()
	worker_task.cancel()


async def test_workflow_events_emitted(manager):
	events_received: list = []

	@manager.callback
	class WorkflowEventCollector(Callback):
		def __init__(self) -> None:
			super().__init__()
			self.add_rule(
				CallbackRule(
					event_types={
						QEventType.WORKFLOW_STARTED,
						QEventType.WORKFLOW_STEP_COMPLETED,
						QEventType.WORKFLOW_COMPLETED,
					}
				)
			)

		async def handle(self, event) -> None:
			events_received.append(event)

	@manager.register(qname="wf.inc")
	async def inc(x: int) -> int:
		return x + 1

	@manager.register(qname="wf.double")
	async def double(x: int) -> int:
		return x * 2

	wf = Workflow("evented").step(inc).step(double)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 5)
	await _process_messages(manager, worker, 2)
	await handle.result(timeout=2.0)

	# Allow final events to propagate
	await asyncio.sleep(0.2)

	await worker.stop()
	worker_task.cancel()

	event_types = [e.event_type for e in events_received]
	assert QEventType.WORKFLOW_STARTED in event_types
	assert event_types.count(QEventType.WORKFLOW_STEP_COMPLETED) == 2
	assert QEventType.WORKFLOW_COMPLETED in event_types


async def test_multiple_workflows_independent(manager):
	@manager.register(qname="wf.add_ten")
	async def add_ten(x: int) -> int:
		return x + 10

	@manager.register(qname="wf.times_two")
	async def times_two(x: int) -> int:
		return x * 2

	wf1 = Workflow("wf1").step(add_ten)
	wf2 = Workflow("wf2").step(times_two)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle1 = await wf1.run(manager, 5)
	handle2 = await wf2.run(manager, 5)

	await _process_messages(manager, worker, 2)

	r1 = await handle1.result(timeout=2.0)
	r2 = await handle2.result(timeout=2.0)

	assert r1 == 15
	assert r2 == 10

	await worker.stop()
	worker_task.cancel()


async def test_workflow_reusable(manager):
	@manager.register(qname="wf.inc")
	async def inc(x: int) -> int:
		return x + 1

	wf = Workflow("reusable").step(inc)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle1 = await wf.run(manager, 1)
	await _process_messages(manager, worker, 1)
	assert await handle1.result(timeout=2.0) == 2

	handle2 = await wf.run(manager, 100)
	await _process_messages(manager, worker, 1)
	assert await handle2.result(timeout=2.0) == 101

	await worker.stop()
	worker_task.cancel()


async def test_parallel_preserves_order(manager):
	@manager.register(qname="wf.slow")
	async def slow(x: int) -> str:
		await asyncio.sleep(0.1)
		return f"slow:{x}"

	@manager.register(qname="wf.fast")
	async def fast(x: int) -> str:
		return f"fast:{x}"

	@manager.register(qname="wf.identity")
	async def identity(results: list) -> list:
		return results

	wf = Workflow("order").parallel(slow, fast).step(identity)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 42)
	await _process_messages(manager, worker, 3)

	result = await handle.result(timeout=2.0)
	# Results should be in declaration order: [slow, fast]
	assert result[0] == "slow:42"
	assert result[1] == "fast:42"

	await worker.stop()
	worker_task.cancel()


async def test_conditional_no_match_no_default_raises(manager):
	@manager.register(qname="wf.noop")
	async def noop(x: int) -> int:
		return x

	wf = (
		Workflow("no_match")
		.when(lambda r: r == "never")
		.then(noop)
		.step(noop)  # finalize conditional without default, then add step
	)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	with pytest.raises(RuntimeError, match="No matching branch"):
		await wf.run(manager, 42)

	await worker.stop()
	worker_task.cancel()


async def test_workflow_sync_task(manager):
	@manager.register(qname="wf.sync_double")
	def sync_double(x: int) -> int:
		return x * 2

	wf = Workflow("sync").step(sync_double)

	worker = QAsyncWorker(manager=manager, worker_id="w1")
	worker_task = asyncio.create_task(worker.start())

	handle = await wf.run(manager, 21)
	await _process_messages(manager, worker, 1)

	result = await handle.result(timeout=2.0)
	assert result == 42

	await worker.stop()
	worker_task.cancel()
