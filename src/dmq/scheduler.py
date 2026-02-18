from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from loguru import logger

from dmq.types import PeriodicScheduleProtocol

if TYPE_CHECKING:
	from .manager import QManager


class PeriodicScheduler:
	def __init__(self, manager: QManager) -> None:
		self.manager = manager
		self._periodic_tasks: list[tuple[str, PeriodicScheduleProtocol]] = []
		self._task: asyncio.Task | None = None
		self._running = False

	def register(self, task_name: str, schedule: PeriodicScheduleProtocol) -> None:
		self._periodic_tasks.append((task_name, schedule))
		logger.info("registered periodic task: {} with schedule {}", task_name, schedule)

	async def start(self) -> None:
		if not self._periodic_tasks:
			logger.debug("no periodic tasks registered, skipping scheduler start")
			return
		self._running = True
		self._task = asyncio.create_task(self._run(), name="periodic-scheduler")

	async def stop(self) -> None:
		self._running = False
		if self._task and not self._task.done():
			self._task.cancel()
			with contextlib.suppress(asyncio.CancelledError):
				await self._task
			self._task = None

	async def _run(self) -> None:
		next_fires: list[float] = [schedule.execute_at.timestamp() for _, schedule in self._periodic_tasks]

		logger.info("periodic scheduler started with {} tasks", len(self._periodic_tasks))

		while self._running:
			try:
				now = datetime.now(UTC).timestamp()

				earliest_idx = min(range(len(next_fires)), key=lambda i: next_fires[i])
				earliest_time = next_fires[earliest_idx]
				sleep_for = max(earliest_time - now, 0)

				if sleep_for > 0:
					await asyncio.sleep(sleep_for)

				if not self._running:
					break

				for i, (task_name, schedule) in enumerate(self._periodic_tasks):
					if next_fires[i] <= now:
						try:
							await self.manager.broker.send_task(task_name=task_name, args=(), kwargs={}, options={})
							logger.debug("periodic fire: {}", task_name)
						except Exception as e:
							logger.error("failed to fire periodic task {}: {}", task_name, e)

						next_fires[i] = schedule.execute_at.timestamp()

			except asyncio.CancelledError:
				break
			except Exception as e:
				logger.error("periodic scheduler error: {}", e)
				await asyncio.sleep(1.0)
