from __future__ import annotations

from collections.abc import Awaitable, Coroutine
from inspect import iscoroutine
from types import CoroutineType
from typing import Any, cast


def _object_fqn(obj: object) -> str:
    if hasattr(obj, "__name__"):
        return f"{obj.__module__}.{obj.__name__}"
    return f"{obj.__module__}.{obj.__class__.__name__}"


async def await_if_async[T](arg: T | Awaitable[T] | Coroutine[Any, Any, T] | CoroutineType[Any, Any, T]) -> T:
    to_return: T

    if iscoroutine(arg):
        to_return = cast(T, await arg)
    else:
        to_return = cast(T, arg)

    return to_return


def _get_type_fqn(arg: Any) -> str | None:
    _resolved_module = ""
    try:
        _resolved_module = arg.__module__
    except AttributeError:
        if arg.__class__.__name__ in __builtins__:
            _resolved_module = "builtins"

    if _resolved_module == "":
        return None

    return _resolved_module + "|" + arg.__class__.__name__
