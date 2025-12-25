"""Shared scheduling utilities for brokers."""

from __future__ import annotations

from datetime import UTC, datetime

from dmq.types import CronSchedule, DelaySchedule, ETASchedule, Schedule


def calculate_execute_time(schedule: Schedule) -> float:
    now = datetime.now(UTC).timestamp()

    match schedule:
        case DelaySchedule(delay_seconds=delay):
            return now + delay
        case ETASchedule(eta=eta):
            return eta.timestamp()
        case CronSchedule(cron_expr=expr):
            raise NotImplementedError(f"cron scheduling not implemented: {expr}")
        case _:
            raise ValueError(f"unknown schedule type: {type(schedule)}")
