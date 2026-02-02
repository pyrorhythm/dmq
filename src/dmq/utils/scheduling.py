from __future__ import annotations

from datetime import UTC, datetime, timedelta

from croniter import croniter

from dmq.types import CronSchedule, DelaySchedule, ETASchedule, Schedule


def calculate_execute_time(schedule: Schedule) -> float:
    now = datetime.now(UTC)

    match schedule:
        case DelaySchedule(delay_seconds=delay):
            return now.timestamp() + delay
        case ETASchedule(eta=eta):
            return eta.timestamp()
        case CronSchedule(cron_expr=expr, delay_seconds=delay):
            if not croniter.is_valid(expr):
                raise ValueError(f"invalid cron expression: {expr}")
            base = now + timedelta(seconds=delay)
            return croniter(expr, base).get_next(float)
        case _:
            raise ValueError(f"unknown schedule type: {type(schedule)}")
