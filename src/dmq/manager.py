from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, get_type_hints

from loguru import logger
from ulid import ulid

from .abc.backend import QResultBackendProtocol
from .abc.broker import QBrokerProtocol
from .abc.serializer import QSerializerProtocol
from .callback import Callback
from .event_router import EventRouter
from .task import QTask
from .util.misc import _object_fqn


class QManager:
    def __init__(
        self, broker: QBrokerProtocol, result_backend: QResultBackendProtocol, serializer: QSerializerProtocol
    ) -> None:
        self.broker: QBrokerProtocol = broker
        self.result_backend: QResultBackendProtocol = result_backend
        self.serializer: QSerializerProtocol = serializer
        self.task_registry: dict[str, Any] = {}
        self.event_router = EventRouter()
        self._callbacks: list[type[Callback]] = []

    def get_task(self, task_name: str) -> QTask | None:
        task = self.task_registry.get(task_name)
        if task is None:
            logger.warning("task {} not found in task.registry: {}", task_name, self.task_registry)
        return task

    def register_callback(self, callback_cls: type[Callback]) -> type[Callback]:
        self._callbacks.append(callback_cls)
        callback_instance = callback_cls()
        callback_instance.bind_manager(self)
        self.event_router.register_callback(callback_instance)
        return callback_cls

    def callback(self, callback_cls: type[Callback]) -> type[Callback]:
        return self.register_callback(callback_cls)

    def register[**Param, Return](
        self, qname: str | None = None, **kws: dict
    ) -> QTask[Param, Return] | Callable[[Callable[Param, Return]], QTask[Param, Return]]:
        def _make(
            task_qname: str | None = None, task_kws: dict | None = None
        ) -> Callable[[Callable[Param, Return]], QTask[Param, Return]]:
            if task_kws is None:
                task_kws = {}

            def inner(func: Callable[Param, Return | Awaitable[Return]]) -> QTask[Param, Return]:
                nonlocal task_qname
                if task_qname is None:
                    try:
                        task_qname = _object_fqn(func)
                    except Exception:
                        task_qname = f"task_{ulid()}"
                    else:
                        if "<lambda>" in task_qname:
                            task_qname = task_qname.replace("<lambda>", f"lambda_m{func.__module__}_{ulid()}")
                wrp = wraps(func)
                ret_type = get_type_hints(func).get("return")
                task_wrp = wrp(
                    QTask(
                        original_func=func, task_kws=task_kws, task_name=task_qname, manager=self, return_type=ret_type
                    )
                )

                self.task_registry[task_qname] = task_wrp

                return task_wrp  # type: ignore

            return inner

        if callable(qname):
            return _make(task_kws=kws or {})(qname)

        return _make(task_qname=qname, task_kws=kws or {})
