from collections.abc import Awaitable, Coroutine
from inspect import iscoroutine
from types import CoroutineType
from typing import Any, cast


def _object_fqn(obj: object) -> str:
	if hasattr(obj, "__name__"):
		return f"{obj.__module__}.{obj.__name__}"
	return f"{obj.__module__}.{obj.__class__.__name__}"


async def await_if_async[T](
	arg: T | Awaitable[T] | Coroutine[Any, Any, T] | CoroutineType[Any, Any, T],
) -> T:
	to_return: T

	if iscoroutine(arg):
		to_return = cast(T, await arg)
	else:
		to_return = cast(T, arg)

	return to_return