from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, Literal, get_type_hints, overload

from loguru import logger
from ulid import ulid

from dmq.types import PeriodicSchedule

from .abc.backend import QResultBackendProtocol
from .abc.broker import QBrokerProtocol
from .abc.serializer import QSerializerProtocol
from .callback import Callback
from .event_router import EventRouter
from .scheduler import PeriodicScheduler
from .task import QTask
from .utils import object_fqn

type _QTaskDecorator[**P, R] = Callable[[Callable[P, R | Coroutine[None, None, R]]], QTask[P, R]]


class QManager:
	def __init__(
		self, broker: QBrokerProtocol, result_backend: QResultBackendProtocol, serializer: QSerializerProtocol
	) -> None:
		"""

		initialize manager

		:rtype: None
		:param broker: qbroker protocol impl
		:param result_backend: qrbackend protocol impl
		:param serializer: qserializer protocol impl

		"""
		self.broker: QBrokerProtocol = broker
		self.result_backend: QResultBackendProtocol = result_backend
		self.serializer: QSerializerProtocol = serializer
		self.task_registry: dict[str, Any] = {}
		self.event_router = EventRouter()
		self.scheduler = PeriodicScheduler(self)
		self._callbacks: list[type[Callback]] = []

	def get_task(self, task_name: str) -> QTask | None:
		"""fetch task from registry by name"""
		task = self.task_registry.get(task_name)
		if task is None:
			logger.warning("task {} not found in task registry: {}", task_name, self.task_registry)
		return task

	def register_callback(self, callback_cls: type[Callback]) -> type[Callback]:
		"""register callback"""

		self._callbacks.append(callback_cls)
		callback_instance = callback_cls()
		callback_instance.bind_manager(self)
		self.event_router.register_callback(callback_instance)
		return callback_cls

	def callback(self, callback_cls: type[Callback]) -> type[Callback]:
		"""alias to register_callback"""

		return self.register_callback(callback_cls)

	@overload
	def register[**P, R](self, fn: Callable[P, R | Coroutine[None, None, R]]) -> QTask[P, R]: ...

	@overload
	def register[**P, R](
		self, fn: Literal[None] = None, qname: str | None = None, **kws: Any
	) -> _QTaskDecorator[P, R]: ...

	def register[**P, R](
		self,
		fn: Callable[P, R | Coroutine[None, None, R]] | None = None,
		qname: str | None = None,
		**kws: Any,
	) -> _QTaskDecorator[P, R] | QTask[P, R]:
		# print(huh, fn)

		"""
		register task and wrap it with QTask

		:param fn: function if you define task without parenthesis
		:param qname: task name
		:param kws: task keyword arguments (labels)

		"""

		def inner(func: Callable[P, R | Coroutine[None, None, R]]) -> QTask[P, R]:
			nonlocal qname
			if qname is None:
				try:
					qname = object_fqn(func)
				except Exception:
					qname = f"task_{ulid()}"
				else:
					if "<lambda>" in qname:
						qname = qname.replace("<lambda>", f"lambda_m{func.__module__}_{ulid()}")

			wrp = wraps(func)
			ret_type = get_type_hints(func).get("return")
			task_wrp = wrp(QTask(original_func=func, task_kws=kws, task_name=qname, manager=self, return_type=ret_type))

			self.task_registry[qname] = task_wrp

			return task_wrp  # type: ignore

		if fn is not None:
			return inner(fn)

		return inner

	def periodic[**P, R](
		self, schedule: PeriodicSchedule, *, qname: str | None = None, **kws: dict
	) -> _QTaskDecorator[P, R]:
		"""
		register a periodic task

		:param schedule: CronSchedule or DelaySchedule for repeat interval
		:param qname: task name
		:param kws: task keyword arguments (labels)

		"""

		def wrapper(func: Callable[P, R | Coroutine[None, None, R]]) -> QTask[P, R]:
			task = self.register(fn=None, qname=qname, **kws)(func)
			self.scheduler.register(task.task_name, schedule)
			return task

		return wrapper
