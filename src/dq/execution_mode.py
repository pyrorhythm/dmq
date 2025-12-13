from enum import StrEnum


class ExecutionMode(StrEnum):
    ASYNC_ONLY = "async_only"
    THREADED = "threaded"
