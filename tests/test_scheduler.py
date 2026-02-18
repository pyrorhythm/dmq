from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from dmq.backends.memory import InMemoryResultBackend
from dmq.brokers.memory import InMemoryBroker
from dmq.manager import QManager
from dmq.serializers.msgpack import MsgpackSerializer
from dmq.types import Cron, Delay


@pytest.fixture
def broker():
	return InMemoryBroker()


@pytest.fixture
def manager(broker):
	return QManager(broker=broker, result_backend=InMemoryResultBackend(), serializer=MsgpackSerializer())


async def test_periodic_delay_fires_multiple_times(manager, broker):
	@manager.periodic(Delay(delay_seconds=0.1))
	async def tick():
		pass

	await manager.scheduler.start()
	await asyncio.sleep(0.55)
	await manager.scheduler.stop()

	# Should have fired ~5 times (0.1, 0.2, 0.3, 0.4, 0.5)
	count = 0
	while not broker._task_queue.empty():
		await broker._task_queue.get()
		count += 1
	assert count >= 4


async def test_periodic_cron_fires(manager, broker):
	# Every minute — we can't wait a full minute, so test that the scheduler
	# registered and computed a future fire time
	@manager.periodic(Cron(cron_expr="* * * * *"))
	async def every_minute():
		pass

	assert len(manager.scheduler._periodic_tasks) == 1
	assert manager.scheduler._periodic_tasks[0][0] == "tests.test_scheduler.every_minute"


async def test_periodic_cron_with_delay_registers(manager):
	base = datetime.now() + timedelta(hours=1)

	@manager.periodic(Cron(cron_expr="0 */4 * * *", base=base))
	async def every_4h_delayed():
		pass

	task_name, schedule = manager.scheduler._periodic_tasks[0]
	assert isinstance(schedule, Cron)
	assert schedule.base == base
	assert schedule.cron_expr == "0 */4 * * *"


# async def test_periodic_eta_raises(manager):
# 	from datetime import UTC, datetime
#
# 	with pytest.raises(ValueError, match="ETASchedule cannot be used for periodic"):
#
# 		@manager.periodic(ETA(eta=datetime.now(UTC)))
# 		async def bad():
# 			pass


async def test_periodic_stop_cancels_cleanly(manager):
	@manager.periodic(Delay(delay_seconds=0.1))
	async def tick():
		pass

	await manager.scheduler.start()
	assert manager.scheduler._task is not None
	assert not manager.scheduler._task.done()

	await manager.scheduler.stop()
	assert manager.scheduler._task is None


async def test_periodic_no_tasks_skips_start(manager):
	# No periodic tasks registered — start should be a no-op
	await manager.scheduler.start()
	assert manager.scheduler._task is None


async def test_periodic_task_is_in_registry(manager):
	@manager.periodic(Delay(delay_seconds=60), qname="my.periodic.task")
	async def my_task():
		pass

	assert "my.periodic.task" in manager.task_registry


async def test_periodic_task_callable_directly(manager):
	results = []

	@manager.periodic(Delay(delay_seconds=60))
	async def accumulate():
		results.append(1)

	# Direct call still works
	await accumulate()
	assert results == [1]
