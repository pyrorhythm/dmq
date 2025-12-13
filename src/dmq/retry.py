from __future__ import annotations

import random
from abc import ABC, abstractmethod

import msgspec


class BackoffStrategy(ABC):
    @abstractmethod
    def calculate_delay(self, retry_count: int) -> float: ...


class ExponentialBackoff(msgspec.Struct, frozen=True):
    base: float = 2.0
    max_delay: float = 3600.0
    jitter: bool = True

    def calculate_delay(self, retry_count: int) -> float:
        delay = min(self.base**retry_count, self.max_delay)
        if self.jitter:
            delay *= random.uniform(0.5, 1.5)
        return delay


class FixedDelayBackoff(msgspec.Struct, frozen=True):
    delay: float = 1.0

    def calculate_delay(self, retry_count: int) -> float:
        return self.delay


class LinearBackoff(msgspec.Struct, frozen=True):
    base: float = 1.0
    increment: float = 1.0
    max_delay: float = 60.0

    def calculate_delay(self, retry_count: int) -> float:
        return min(self.base + (self.increment * retry_count), self.max_delay)


class RetryPolicy(msgspec.Struct, frozen=True):
    max_retries: int = 3
    backoff: ExponentialBackoff | FixedDelayBackoff | LinearBackoff = ExponentialBackoff()
    retry_on: tuple[type[Exception], ...] = (Exception,)
    dont_retry_on: tuple[type[Exception], ...] = ()

    def should_retry(self, exception: Exception, retry_count: int) -> bool:
        if retry_count >= self.max_retries:
            return False

        if self.dont_retry_on and isinstance(exception, self.dont_retry_on):
            return False

        return bool(self.retry_on and isinstance(exception, self.retry_on))

    def get_delay(self, retry_count: int) -> float:
        return self.backoff.calculate_delay(retry_count)
