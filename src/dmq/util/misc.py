from __future__ import annotations

from collections.abc import Awaitable, Coroutine
from inspect import iscoroutine
from types import CoroutineType
from typing import Any, cast

from loguru import logger

from dmq.cli.utils import import_object


def _object_fqn(obj: object) -> str:
    if hasattr(obj, "__name__"):
        return f"{obj.__module__}.{obj.__name__}"
    return f"{obj.__module__}.{obj.__class__.__name__}"


def _get_type_fqn(arg: Any) -> str | None:
    _resolved_module = ""
    try:
        _resolved_module = arg.__module__
    except AttributeError:
        if arg.__class__.__name__ in __builtins__:
            _resolved_module = "builtins"

    if _resolved_module == "":
        return None

    return _resolved_module + ":" + arg.__qualname__


def _get_type_from_fqn(_result: str | bytes | None) -> Any:
    _imported_type = None
    if _result is None:
        return _imported_type

    _decoded_result = _result.decode() if isinstance(_result, bytes) else _result
    try:
        _imported_type = import_object(_decoded_result)
    except Exception as exc:
        logger.warning("{}", exc)

    return _imported_type


async def await_if_async[T](
    arg: T | Awaitable[T] | Coroutine[Any, Any, T] | CoroutineType[Any, Any, T],
) -> T:
    to_return: T

    to_return = cast(T, await arg) if iscoroutine(arg) else cast(T, arg)

    return to_return
