from __future__ import annotations

import argparse
import asyncio
import signal
import sys

import loguru

from ..guarantees import DeliveryConfig, DeliveryGuarantee
from ..manager import QManager
from ..worker_pool import QWorkerPool
from .utils import import_object, import_tasks

logger = loguru.logger.bind(name="dmq.cli")


async def run_worker(
    manager: QManager,
    worker_count: int,
    max_tasks_per_worker: int,
    delivery_guarantee: str,
) -> None:
    delivery_config = DeliveryConfig(
        guarantee=DeliveryGuarantee(delivery_guarantee),
        enable_idempotency=delivery_guarantee == "exactly_once",
    )

    pool = QWorkerPool(
        manager,
        worker_count=worker_count,
        max_tasks_per_worker=max_tasks_per_worker,
        delivery_config=delivery_config,
    )

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal(sig: signal.Signals) -> None:
        logger.info("rcvd signal {}, initiating graceful shutdown...", sig.name)
        shutdown_event.set()

    for _sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(_sig, handle_signal, _sig)

    logger.info("preset delivery guarantee: {}", delivery_guarantee)

    await pool.start()

    logger.info("dmq worker running...")

    await shutdown_event.wait()

    logger.info("shutting down worker pool...")
    await pool.shutdown()
    logger.info("shutting down worker pool... success!")


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="dmq - Python-native distributed task queue worker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  dmq --broker myapp.broker:manager --tasks myapp.tasks
  dmq --broker myapp:manager --tasks tasks.email tasks.notifications --workers 8
  dmq --broker app:manager --tasks app.tasks --workers 4 --concurrency 20
        """,
    )

    parser.add_argument(
        "--broker",
        required=True,
        help="Python path to QManager instance (e.g., 'myapp.broker:manager')",
    )

    parser.add_argument(
        "--tasks",
        nargs="+",
        required=True,
        help="Task modules to import (space-separated)",
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )

    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=10,
        help="Max concurrent tasks per worker (default: 10)",
    )

    parser.add_argument(
        "--guarantee",
        "-g",
        choices=["at_most_once", "at_least_once", "exactly_once"],
        default="at_least_once",
        help="Delivery guarantee (default: at_least_once)",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("dmq worker starting...")
    logger.info("=" * 60)
    logger.info("Broker: {}", args.broker)
    logger.info("Tasks: {}", ", ".join(args.tasks))

    # Import manager and tasks
    try:
        manager = import_object(args.broker)
        if not isinstance(manager, QManager):
            logger.error("Imported object is not a QManager instance")
            sys.exit(1)
    except Exception as e:
        logger.error("Failed to import broker: {}", e)
        sys.exit(1)

    try:
        import_tasks(args.tasks, pattern="", fs_discover=False)
    except Exception as e:
        logger.error("Failed to import tasks: {}", e)
        sys.exit(1)

    logger.info("Registered tasks: {}", list(manager.task_registry.keys()))

    # Run the async worker
    try:
        asyncio.run(
            run_worker(
                manager,
                args.workers,
                args.concurrency,
                args.guarantee,
            )
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception("Worker failed: {}", e)
        sys.exit(1)
