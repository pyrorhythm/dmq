from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from argparse import Namespace

import loguru

from ..execution_mode import ExecutionMode
from ..guarantees import DeliveryConfig, DeliveryGuarantee
from ..manager import QManager
from ..runtime import detect_execution_mode, get_recommended_worker_count
from ..worker_pool import QWorkerPool
from .utils import import_object, import_tasks

logger = loguru.logger.bind(name="dmq.cli")


async def run_worker(
    manager: QManager,
    worker_count: int,
    max_tasks_per_worker: int,
    delivery_guarantee: str,
    execution_mode: ExecutionMode | None = None,
) -> None:
    # auto detect execution mode if not specified
    if execution_mode is None:
        execution_mode = detect_execution_mode()

    delivery_config = DeliveryConfig(
        guarantee=DeliveryGuarantee(delivery_guarantee), enable_idempotency=delivery_guarantee == "exactly_once"
    )

    pool = QWorkerPool(
        manager,
        worker_count=worker_count,
        max_tasks_per_worker=max_tasks_per_worker,
        delivery_config=delivery_config,
        execution_mode=execution_mode,
    )

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal(sig: signal.Signals) -> None:
        logger.info("rcvd signal {}, initiating graceful shutdown...", sig.name)
        shutdown_event.set()

    for _sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(_sig, handle_signal, _sig)

    logger.info("preset delivery guarantee: {}", delivery_guarantee)
    logger.info("execution mode: {}", execution_mode)

    await pool.start()

    logger.info("dmq worker running...")

    await shutdown_event.wait()

    logger.info("shutting down worker pool...")
    await pool.shutdown()
    logger.info("shutting down worker pool... success!")


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="dmq - python native distributed task queue worker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  dmq --broker myapp.broker:manager --tasks myapp.tasks
  dmq --broker myapp:manager --tasks tasks.email tasks.notifications --workers 8
  dmq --broker app:manager --tasks app.tasks --workers 4 --concurrency 20
  dmq --broker app:manager --tasks app.tasks --execution-mode threaded
        """,
    )

    parser.add_argument(
        "--broker", required=True, help="python path to qmanager instance (e g., 'myapp broker:manager')"
    )

    parser.add_argument("--tasks", nargs="+", required=True, help="task modules to import (space separated)")

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=None,
        help="number of parallel workers (default: auto detect based on execution mode)",
    )

    parser.add_argument(
        "--concurrency", "-c", type=int, default=10, help="max concurrent tasks per worker (default: 10)"
    )

    parser.add_argument(
        "--guarantee",
        "-g",
        choices=["at_most_once", "at_least_once", "exactly_once"],
        default="at_least_once",
        help="delivery guarantee (default: at least once)",
    )

    parser.add_argument(
        "--execution-mode",
        "-e",
        choices=["auto", "async", "threaded"],
        default="auto",
        help="execution mode: auto (detect), async (single loop), threaded (multi thread). default: auto",
    )

    parser.add_argument("--log-level", "-l", choices=["debug", "info", "warning"], default="info", help="log level")

    args: Namespace = parser.parse_args()

    # determine execution mode
    if args.execution_mode == "auto":
        execution_mode = None  # will be auto detected in run worker
    elif args.execution_mode == "async":
        execution_mode = ExecutionMode.ASYNC_ONLY
    else:
        execution_mode = ExecutionMode.THREADED

    logger.remove()
    logger.add(sys.stderr, level=args.log_level.upper())

    # determine worker count
    if args.workers is None:
        # auto select based on detected/forced mode
        detected_mode = execution_mode or detect_execution_mode()
        worker_count = get_recommended_worker_count(detected_mode, None)
    else:
        worker_count = args.workers

    logger.info("=" * 60 + "\n" + "dmq worker starting..." + "\n" + "=" * 60)
    logger.info("broker: {}", args.broker)
    logger.info("tasks: {}", ", ".join(args.tasks))
    logger.info("workers: {}", worker_count)
    logger.info("execution mode: {}", args.execution_mode)

    # Import manager and tasks
    try:
        manager = import_object(args.broker)
        if not isinstance(manager, QManager):
            logger.error("imported object is not a qmanager instance")
            sys.exit(1)
    except Exception as e:
        logger.error("failed to import broker: {}", e)
        sys.exit(1)

    try:
        import_tasks(args.tasks, pattern="", fs_discover=False)
    except Exception as e:
        logger.error("failed to import tasks: {}", e)
        sys.exit(1)

    logger.info("registered tasks: {}", list(manager.task_registry.keys()))

    # Run the async worker
    try:
        asyncio.run(run_worker(manager, worker_count, args.concurrency, args.guarantee, execution_mode))
    except KeyboardInterrupt:
        logger.info("interrupted by user")
    except Exception as e:
        logger.exception("worker failed: {}", e)
        sys.exit(1)
