from __future__ import annotations

from dmq.types import CronSchedule, DelaySchedule, ETASchedule, TaskMessage


def test_task_message_defaults():
    msg = TaskMessage(task_id="t1", task_name="test", args=(), kwargs={}, options={})
    assert msg.retry_count == 0
    assert msg.max_retries == 3
    assert msg.workflow_context is None


def test_task_message_frozen():
    msg = TaskMessage(task_id="t1", task_name="test", args=(), kwargs={}, options={})
    try:
        msg.task_id = "other"
        assert False, "Should be frozen"
    except AttributeError:
        pass


def test_delay_schedule():
    s = DelaySchedule(delay_seconds=5.0)
    assert s.delay_seconds == 5.0


def test_eta_schedule():
    from datetime import UTC, datetime

    dt = datetime(2026, 1, 1, tzinfo=UTC)
    s = ETASchedule(eta=dt)
    assert s.eta == dt


def test_cron_schedule():
    s = CronSchedule(cron_expr="*/5 * * * *")
    assert s.cron_expr == "*/5 * * * *"
