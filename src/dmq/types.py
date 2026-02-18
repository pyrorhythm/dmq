from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

import msgspec
from croniter import croniter

if TYPE_CHECKING:
	from .manager import QManager


class CannotRunError(BaseException): ...


class PeriodicScheduleProtocol(Protocol):
	@property
	def execute_in(self) -> float: ...
	@property
	def execute_at(self) -> datetime: ...


class BasePeriodicSchedule(PeriodicScheduleProtocol):
	base: datetime | None

	@property
	def _base(self) -> datetime:
		# If base is None or past now(), use now() instead
		return max(self.base or datetime(year=1, month=1, day=1), datetime.now())


@dataclass
class Delay(BasePeriodicSchedule):
	"""

	runs task with delay, with periodic task scheduling it works like :

		... -> 	creation - 5s -> execution -> reschedule AFTER execution -> ...
					delay = 5		 | dur = Any	  | delay = 5

	"""

	delay_seconds: float
	base: datetime | None = None

	@property
	def execute_in(self) -> float:
		bn = self._base
		ts = datetime.now()
		if bn > ts:
			return ((bn - ts) + timedelta(seconds=self.delay_seconds)).total_seconds()

		return self.delay_seconds

	@property
	def execute_at(self) -> datetime:
		return self._base + timedelta(seconds=self.delay_seconds)


@dataclass
class Cron(BasePeriodicSchedule):
	cron_expr: str
	base: datetime | None = None

	@property
	def execute_in(self) -> float:
		return croniter(self.cron_expr, self._base).get_next(float)

	@property
	def execute_at(self) -> datetime:
		return croniter(self.cron_expr, self._base).get_next(datetime)


@dataclass
class ETA:
	"""

	runs task at a certain point(s) of time
	cannot be periodically scheduled

	"""

	eta: datetime

	@property
	def execute_at(self) -> datetime:
		return self.eta

	@property
	def execute_in(self) -> float:
		dtn = datetime.now()
		return float("inf") if self.eta < dtn else (dtn - self.eta).total_seconds()


type PeriodicSchedule = Cron | Delay
type Schedule = PeriodicSchedule | ETA


class TaskMessage(msgspec.Struct, frozen=True):
	task_id: str
	task_name: str
	args: tuple[Any, ...]
	kwargs: dict[str, Any]
	options: dict[str, Any]
	workflow_context: dict[str, Any] | None = None
	retry_count: int = 0
	max_retries: int = 3


class QOngoingTask[T](msgspec.Struct, frozen=True):
	_id: str
	_manager: QManager

	async def result(self, timeout: float = 1.0, nonblocking: bool = False) -> T:
		return await self._manager.result_backend.get_result(self._id, timeout if nonblocking else None)

	@property
	def id(self) -> str:
		return self._id
