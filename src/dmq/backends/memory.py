import asyncio
from typing import Any


class InMemoryResultBackend:
    def __init__(self) -> None:
        self._results: dict[str, Any] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def store_result(self, task_id: str, result: Any) -> None:
        async with self._lock:
            self._results[task_id] = result
            if task_id in self._events:
                self._events[task_id].set()

    async def get_result(self, task_id: str, timeout: float | None = None) -> Any:
        if task_id in self._results:
            return self._results[task_id]

        async with self._lock:
            if task_id not in self._events:
                self._events[task_id] = asyncio.Event()
            event = self._events[task_id]

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError as e:
            raise TimeoutError(f"timeout waiting for result of task {task_id}") from e

        if task_id not in self._results:
            raise KeyError(f"task {task_id} not found")

        return self._results[task_id]

    async def delete_result(self, task_id: str) -> None:
        async with self._lock:
            self._results.pop(task_id, None)
            self._events.pop(task_id, None)

    async def result_exists(self, task_id: str) -> bool:
        return task_id in self._results
