from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .events import QEvent, QEventType

if TYPE_CHECKING:
    from .manager import QManager


class CallbackRule:
    def __init__(
        self,
        event_types: set[QEventType] | None = None,
        task_names: set[str] | None = None,
        on_exception: bool | None = None,
        performance_threshold: float | None = None,
    ) -> None:
        self.event_types = event_types
        self.task_names = task_names
        self.on_exception = on_exception
        self.performance_threshold = performance_threshold

    def matches(self, event: QEvent) -> bool:
        if self.event_types and event.event_type not in self.event_types:
            return False

        if self.task_names and event.task_name not in self.task_names:
            return False

        if self.on_exception is not None:
            is_exception = event.event_type == QEventType.TASK_FAILED
            if self.on_exception != is_exception:
                return False

        if self.performance_threshold is not None:
            if hasattr(event, "duration"):
                if event.duration < self.performance_threshold:
                    return False
            else:
                return False

        return True


class Callback(ABC):
    def __init__(self) -> None:
        self.rules: list[CallbackRule] = []
        self._manager: QManager | None = None

    def add_rule(self, rule: CallbackRule) -> None:
        self.rules.append(rule)

    def should_handle(self, event: QEvent) -> bool:
        if not self.rules:
            return True

        return any(rule.matches(event) for rule in self.rules)

    @abstractmethod
    async def handle(self, event: QEvent) -> None: ...

    def bind_manager(self, manager: QManager) -> None:
        self._manager = manager

    @property
    def manager(self) -> QManager:
        if self._manager is None:
            raise RuntimeError("callback not bound to manager")
        return self._manager


def on_event(
    *event_types: QEventType,
    task_names: set[str] | None = None,
    on_exception: bool | None = None,
    performance_threshold: float | None = None,
) -> Any:
    def decorator(callback_cls: type[Callback]) -> type[Callback]:
        original_init = callback_cls.__init__

        def new_init(self: Callback) -> None:
            original_init(self)
            rule = CallbackRule(
                event_types=set(event_types) if event_types else None,
                task_names=task_names,
                on_exception=on_exception,
                performance_threshold=performance_threshold,
            )
            self.add_rule(rule)

        callback_cls.__init__ = new_init
        return callback_cls

    return decorator
