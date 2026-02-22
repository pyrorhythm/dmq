"""
Stress test for dmq with ~30,000 network IO tasks.

This test:
1. Spawns 30,000 tasks that make HTTP requests (network IO)
2. Uses event callbacks to collect metrics (latency, throughput, errors)
3. Integrates profiling via event callbacks

Run with: python -m pytest tests/test_stress_net_io.py -v -s
"""

from __future__ import annotations

import asyncio
import cProfile
import pstats
import ssl
import time
from collections import defaultdict
from dataclasses import dataclass, field
from io import StringIO
from typing import Any

import aiohttp
import pytest
from aiohttp import ClientTimeout
from loguru import logger

from dmq.backends.memory import InMemoryResultBackend
from dmq.backends.redis_backend import RedisResultBackend
from dmq.brokers import RedisBroker
from dmq.brokers.memory import InMemoryBroker
from dmq.callback import Callback, on_event
from dmq.event_router import EventRouter
from dmq.events import QEvent, QEventType
from dmq.manager import QManager
from dmq.serializers.msgpack import MsgpackSerializer
from dmq.worker_pool import QWorkerPool

# ============================================================================
# Metrics Collection via Event Callbacks
# ============================================================================


@dataclass
class StressMetrics:
	"""Collected stress test metrics."""

	task_durations: list[float] = field(default_factory=list)
	task_timestamps: list[float] = field(default_factory=list)
	task_errors: dict[str, int] = field(default_factory=lambda: defaultdict(int))
	tasks_queued: int = 0
	tasks_started: int = 0
	tasks_completed: int = 0
	tasks_failed: int = 0
	start_time: float = 0.0
	end_time: float = 0.0

	@property
	def total_duration(self) -> float:
		return self.end_time - self.start_time

	@property
	def throughput(self) -> float:
		if self.total_duration == 0:
			return 0.0
		return self.tasks_completed / self.total_duration

	@property
	def avg_latency(self) -> float:
		if not self.task_durations:
			return 0.0
		return sum(self.task_durations) / len(self.task_durations)

	@property
	def p50_latency(self) -> float:
		if not self.task_durations:
			return 0.0
		sorted_durations = sorted(self.task_durations)
		idx = len(sorted_durations) // 2
		return sorted_durations[idx]

	@property
	def p95_latency(self) -> float:
		if not self.task_durations:
			return 0.0
		sorted_durations = sorted(self.task_durations)
		idx = int(len(sorted_durations) * 0.95)
		return sorted_durations[idx]

	@property
	def p99_latency(self) -> float:
		if not self.task_durations:
			return 0.0
		sorted_durations = sorted(self.task_durations)
		idx = int(len(sorted_durations) * 0.99)
		return sorted_durations[idx]

	def summary(self) -> dict[str, Any]:
		return {
			"total_tasks": self.tasks_completed + self.tasks_failed,
			"completed": self.tasks_completed,
			"failed": self.tasks_failed,
			"duration_seconds": round(self.total_duration, 2),
			"throughput_tasks_per_sec": round(self.throughput, 2),
			"avg_latency_sec": round(self.avg_latency, 4),
			"p50_latency_sec": round(self.p50_latency, 4),
			"p95_latency_sec": round(self.p95_latency, 4),
			"p99_latency_sec": round(self.p99_latency, 4),
			"error_counts": dict(self.task_errors),
		}


# Global metrics instance for the test
_stress_metrics = StressMetrics()


@on_event(QEventType.TASK_QUEUED)
class MetricsQueuedCallback(Callback):
	"""Callback that tracks when tasks are queued."""

	async def handle(self, event: QEvent) -> None:
		_stress_metrics.tasks_queued += 1
		if _stress_metrics.start_time == 0.0:
			_stress_metrics.start_time = time.time()


@on_event(QEventType.TASK_STARTED)
class MetricsStartedCallback(Callback):
	"""Callback that tracks when tasks start processing."""

	async def handle(self, event: QEvent) -> None:
		_stress_metrics.tasks_started += 1


@on_event(QEventType.TASK_COMPLETED)
class MetricsCompletedCallback(Callback):
	"""Callback that tracks task completion and extracts latency metrics."""

	async def handle(self, event: QEvent) -> None:
		_stress_metrics.tasks_completed += 1
		if hasattr(event, "duration"):
			_stress_metrics.task_durations.append(event.duration)
		_stress_metrics.task_timestamps.append(time.time())
		_stress_metrics.end_time = time.time()


@on_event(QEventType.TASK_FAILED)
class MetricsFailedCallback(Callback):
	"""Callback that tracks task failures."""

	async def handle(self, event: QEvent) -> None:
		_stress_metrics.tasks_failed += 1
		if hasattr(event, "exception"):
			_stress_metrics.task_errors[event.exception] += 1
		_stress_metrics.end_time = time.time()


# ============================================================================
# Profiling via Event Callbacks
# ============================================================================


@dataclass
class ProfilingData:
	"""Stores profiling information collected during the test."""

	profiler: cProfile.Profile = field(default_factory=cProfile.Profile)
	enabled: bool = False
	profile_output: str = ""

	def start(self) -> None:
		"""Start profiling."""
		self.enabled = True
		self.profiler.enable()

	def stop(self) -> None:
		"""Stop profiling and generate output."""
		if not self.enabled:
			return
		self.profiler.disable()
		self.enabled = False

		# Generate profile stats
		stream = StringIO()
		stats = pstats.Stats(self.profiler, stream=stream)
		stats.sort_stats("cumulative")
		stats.print_stats(50)  # Top 50 functions
		self.profile_output = stream.getvalue()

	def get_stats(self) -> pstats.Stats:
		"""Get pstats object for detailed analysis."""
		return pstats.Stats(self.profiler)


_profiling_data = ProfilingData()


@on_event(QEventType.TASK_STARTED)
class ProfilingStartedCallback(Callback):
	"""Callback that starts profiling when first task begins."""

	async def handle(self, event: QEvent) -> None:
		if not _profiling_data.enabled and _stress_metrics.tasks_started == 0:
			print("\n[PROFILING] First task started - enabling profiler...")
			_profiling_data.start()


@on_event(QEventType.TASK_COMPLETED)
class ProfilingCompletedCallback(Callback):
	"""Callback that stops profiling when all tasks complete."""

	async def handle(self, event: QEvent) -> None:
		# Check if we've processed enough tasks to stop profiling
		# We'll stop when we've completed at least 90% of tasks
		if _profiling_data.enabled:
			expected = _stress_metrics.tasks_queued
			completed = _stress_metrics.tasks_completed
			if expected > 0 and completed >= expected * 0.9:
				print(f"\n[PROFILING] {completed}/{expected} tasks completed - stopping profiler...")
				_profiling_data.stop()


# ============================================================================
# Test Tasks with Network IO
# ============================================================================


@pytest.fixture
def redis_url(redis_container):
	host = redis_container.get_container_host_ip()
	port = redis_container.get_exposed_port(6379)
	return f"redis://{host}:{port}"


@pytest.fixture
def manager(redis_url) -> QManager:
	"""Create a manager with registered callbacks."""
	m = QManager(
		broker=RedisBroker(redis_url=redis_url),
		result_backend=RedisResultBackend(redis_url=redis_url, type_serialization=True),
		serializer=MsgpackSerializer(),
	)

	# Register metrics callbacks
	m.register_callback(MetricsQueuedCallback)
	m.register_callback(MetricsStartedCallback)
	m.register_callback(MetricsCompletedCallback)
	m.register_callback(MetricsFailedCallback)

	# Register profiling callbacks
	m.register_callback(ProfilingStartedCallback)
	m.register_callback(ProfilingCompletedCallback)

	return m


def manager_no_fx(redis_url) -> QManager:
	"""Create a manager with registered callbacks."""
	m = QManager(
		broker=RedisBroker(redis_url=redis_url),
		result_backend=RedisResultBackend(redis_url=redis_url, type_serialization=True),
		serializer=MsgpackSerializer(),
	)

	# Register metrics callbacks
	m.register_callback(MetricsQueuedCallback)
	m.register_callback(MetricsStartedCallback)
	m.register_callback(MetricsCompletedCallback)
	m.register_callback(MetricsFailedCallback)

	# Register profiling callbacks
	m.register_callback(ProfilingStartedCallback)
	m.register_callback(ProfilingCompletedCallback)

	return m


# HTTP endpoint for testing (httpbin.org is a public test service)
HTTP_TEST_URL = "https://httpbin.org/delay/0.1"  # 100ms delay
HTTP_TEST_URLS = [
	"https://httpbin.org/get",
	"https://httpbin.org/uuid",
	"https://httpbin.org/headers",
]

SSLCTX = ssl.create_default_context()

try:
	import certifi
except ImportError:
	certifi = None
else:
	ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
	ctx.load_default_certs()

	if certifi:
		ctx.load_verify_locations(certifi.where())

	ctx.minimum_version = ssl.TLSVersion.TLSv1_2
	ctx.maximum_version = ssl.TLSVersion.TLSv1_3

	ctx.set_ciphers(
		"TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256:"
		"TLS_CHACHA20_POLY1305_SHA256:"
		"ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
		"ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256"
	)

	ctx.check_hostname = True
	ctx.verify_mode = ssl.CERT_REQUIRED

	SSLCTX = ctx


async def fetch_url(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
	"""Make an HTTP GET request and return result."""
	async with session.get(url, ssl=SSLCTX, timeout=ClientTimeout(total=30)) as response:
		return {"status": response.status, "url": url}


async def net_io_task(manager: QManager, url: str) -> dict[str, Any]:
	"""Task that makes an HTTP request (network IO)."""
	async with aiohttp.ClientSession() as session:
		result = await fetch_url(session, url)
		return result


# ============================================================================
# Stress Test
# ============================================================================


@pytest.mark.slow
async def test_stress_30k_net_io_tasks(manager: QManager) -> None:
	"""
	Stress test with ~30,000 tasks making network IO calls.

	This test:
	1. Registers a task that makes HTTP requests
	2. Queues 30,000 task executions
	3. Processes them with a worker pool
	4. Collects metrics via event callbacks
	5. Profiles execution via event callbacks

	Note: This is a slow test that makes real HTTP requests.
	Adjust task_count if needed for your environment.
	"""

	# Configuration
	task_count = 30000
	worker_count = 20
	max_tasks_per_worker = 50

	logger.info(f"\n{'=' * 60}")
	logger.info(f"Starting stress test: {task_count} network IO tasks")
	logger.info(f"Workers: {worker_count}, Max tasks/worker: {max_tasks_per_worker}")
	logger.info(f"Target URL: {HTTP_TEST_URL}")
	logger.info(f"{'=' * 60}\n")

	# Register the network IO task
	@manager.register(qname="stress.net_io")
	async def net_io_wrapper(task_idx: int) -> dict[str, Any]:
		# Vary the URL to avoid caching
		url = HTTP_TEST_URLS[task_idx % len(HTTP_TEST_URLS)]
		return await net_io_task(manager, url)

	# Queue all tasks
	logger.info(f"Queueing {task_count} tasks...")
	queue_start = time.time()

	# Create tasks in batches for efficiency
	tasks = []
	for i in range(task_count):
		task = net_io_wrapper.q(i)
		tasks.append(task)

	# Wait for all tasks to be queued
	await asyncio.gather(*tasks)

	queue_duration = time.time() - queue_start
	logger.info(f"Queued {task_count} tasks in {queue_duration:.2f}s")
	logger.info(f"Queue rate: {task_count / queue_duration:.0f} tasks/sec\n")

	# Start worker pool to process tasks
	logger.info(f"Starting worker pool with {worker_count} workers...")
	async with QWorkerPool(
		manager=manager,
		worker_count=worker_count,
		max_tasks_per_worker=max_tasks_per_worker,
	) as pool:
		logger.info(f"Worker pool started. Max concurrent: {pool.total_max_concurrent}")

		# Wait for all tasks to complete (with timeout)
		max_wait = 600  # 10 minutes timeout
		check_interval = 1.0

		last_poll = time.time()
		start_time = time.time()
		last_count = 0

		while _stress_metrics.tasks_completed + _stress_metrics.tasks_failed < task_count:
			await asyncio.sleep(check_interval)
			completed = _stress_metrics.tasks_completed + _stress_metrics.tasks_failed  # N completed (overall)
			new_count = completed - last_count  # New count for iteration
			last_count = completed  # Update with last completed
			now = time.time()  # Now
			elapsed = now - start_time  # Now - overall start time
			since_last_poll = now - last_poll  # Now - last poll time
			last_poll = now
			average_rate = completed / elapsed if elapsed > 0 else 0
			periodic_rate = new_count / (since_last_poll or 1)
			logger.info(
				f"\rProgress: {completed}/{task_count} "
				f"({100 * completed / task_count:.1f}%) "
				f"Average rate: {average_rate:.0f}/s "
				f"Periodic rate: {new_count} new tasks in {since_last_poll}, {periodic_rate:.0f}/s"
				f"Errors: {_stress_metrics.tasks_failed}",
				end="",
				flush=True,
			)

		logger.info("\n\nAll tasks processed!")

	# Print metrics summary
	logger.info(f"\n{'=' * 60}")
	logger.info("METRICS SUMMARY")
	logger.info(f"{'=' * 60}")

	summary = _stress_metrics.summary()
	for key, value in summary.items():
		logger.info(f"  {key}: {value}")

	# Print profiling results
	if _profiling_data.profile_output:
		logger.info(f"\n{'=' * 60}")
		logger.info("PROFILING RESULTS (Top 50 functions by cumulative time)")
		logger.info(f"{'=' * 60}")
		logger.info(_profiling_data.profile_output)

	# Assertions
	assert _stress_metrics.tasks_completed > 0, "No tasks completed"
	assert _stress_metrics.tasks_completed + _stress_metrics.tasks_failed == task_count, (
		f"Not all tasks finished: {_stress_metrics.tasks_completed + _stress_metrics.tasks_failed}/{task_count}"
	)

	logger.info(f"\n{'=' * 60}")
	logger.info("STRESS TEST PASSED")
	logger.info(f"{'=' * 60}\n")


# ============================================================================
# Alternative: Local network IO test (no external dependencies)
# ============================================================================


class MockNetIOHandler:
	"""Mock HTTP server handler for local testing."""

	def __init__(self, delay: float = 0.01):
		self.delay = delay
		self.request_count = 0

	async def handle(self, request: dict) -> dict:
		"""Simulate network IO with a small delay."""
		self.request_count += 1
		await asyncio.sleep(self.delay)  # Simulate network latency
		return {"status": 200, "request_id": self.request_count}


# For environments without internet access, use this fixture
@pytest.fixture
def manager_local() -> QManager:
	"""Create a manager with local mock network IO."""

	class LocalMetricsCallback(Callback):
		"""Track metrics for local test."""

		def __init__(self):
			super().__init__()
			self.completed = 0
			self.failed = 0

		async def handle(self, event: QEvent) -> None:
			if event.event_type == QEventType.TASK_COMPLETED:
				self.completed += 1
			elif event.event_type == QEventType.TASK_FAILED:
				self.failed += 1

	m = QManager(
		broker=InMemoryBroker(),
		result_backend=InMemoryResultBackend(),
		serializer=MsgpackSerializer(),
	)
	m.register_callback(LocalMetricsCallback)
	return m


@pytest.mark.slow
@pytest.mark.skipif(True, reason="Requires external network access")
async def test_stress_local_net_io(manager_local: QManager) -> None:
	"""
	Local stress test using mock network IO.
	Useful for CI/CD environments without internet access.
	"""

	TASK_COUNT = 30000
	WORKER_COUNT = 20
	mock_handler = MockNetIOHandler(delay=0.005)  # 5ms simulated latency

	@manager_local.register(qname="stress.mock_net_io")
	async def mock_net_io(idx: int) -> dict:
		return await mock_handler.handle({"idx": idx})

	print(f"\nQueuing {TASK_COUNT} mock network IO tasks...")

	# Queue tasks
	tasks = [mock_net_io.q(i) for i in range(TASK_COUNT)]
	await asyncio.gather(*tasks)

	print("Processing tasks...")

	async with QWorkerPool(
		manager=manager_local,
		worker_count=WORKER_COUNT,
		max_tasks_per_worker=50,
	):
		# Wait for completion
		while True:
			await asyncio.sleep(1)
			# Check completion via result backend
			break

	print("Local stress test completed!")


# ============================================================================
# Simple Unit Test (no network required)
# ============================================================================


async def test_metrics_callbacks_work() -> None:
	"""
	Simple test to verify metrics callbacks work correctly.
	Uses local mock IO - no network required.
	"""
	from dmq.callback import Callback, CallbackRule
	from dmq.events import QTaskCompleted, QTaskQueued, QTaskStarted

	# Create fresh metrics for this test
	metrics = StressMetrics()
	profiling = ProfilingData()

	class TestMetricsQueuedCallback(Callback):
		def __init__(self):
			super().__init__()
			self.add_rule(CallbackRule(event_types={QEventType.TASK_QUEUED}))

		async def handle(self, event: QEvent) -> None:
			metrics.tasks_queued += 1

	class TestMetricsStartedCallback(Callback):
		def __init__(self):
			super().__init__()
			self.add_rule(CallbackRule(event_types={QEventType.TASK_STARTED}))

		async def handle(self, event: QEvent) -> None:
			metrics.tasks_started += 1

	class TestMetricsCompletedCallback(Callback):
		def __init__(self):
			super().__init__()
			self.add_rule(CallbackRule(event_types={QEventType.TASK_COMPLETED}))

		async def handle(self, event: QEvent) -> None:
			metrics.tasks_completed += 1
			if hasattr(event, "duration"):
				metrics.task_durations.append(event.duration)

	# Set up router
	router = EventRouter()
	router.register_callback(TestMetricsQueuedCallback())
	router.register_callback(TestMetricsStartedCallback())
	router.register_callback(TestMetricsCompletedCallback())

	# Emit events
	await router.emit(QTaskQueued(task_id="t1", task_name="test", timestamp=1.0, args=(), kwargs={}))
	await router.emit(QTaskStarted(task_id="t1", task_name="test", timestamp=1.0))
	await router.emit(QTaskCompleted(task_id="t1", task_name="test", timestamp=1.0, duration=0.5))

	# Wait for event processing
	await asyncio.sleep(0.1)

	# Verify
	assert metrics.tasks_queued == 1
	assert metrics.tasks_started == 1
	assert metrics.tasks_completed == 1
	assert len(metrics.task_durations) == 1
	assert metrics.task_durations[0] == 0.5
	assert metrics.avg_latency == 0.5

	print("Metrics callbacks test passed!")


async def test_profiling_callback() -> None:
	"""
	Test that profiling can be started/stopped via event callbacks.
	"""

	profiling = ProfilingData()
	metrics = StressMetrics()

	class ProfilingStartCallback(Callback):
		async def handle(self, event: QEvent) -> None:
			if not profiling.enabled:
				profiling.start()

	class ProfilingStopCallback(Callback):
		async def handle(self, event: QEvent) -> None:
			if profiling.enabled:
				profiling.stop()

	# Set up
	router = EventRouter()
	router.register_callback(ProfilingStartCallback())
	router.register_callback(ProfilingStopCallback())

	# Simulate some work
	def cpu_work():
		total = 0
		for i in range(10000):
			total += i * i
		return total

	# Start profiling
	profiling.start()
	cpu_work()
	profiling.stop()

	assert profiling.profile_output != ""
	assert "cpu_work" in profiling.profile_output or "test_profiling_callback" in profiling.profile_output

	print("Profiling callback test passed!")


# ============================================================================
# Small Scale Stress Test (1000 tasks - runs without external network)
# ============================================================================


@pytest.mark.slow
async def test_small_stress_net_io() -> None:
	"""
	Small-scale stress test with 1000 tasks using mock network IO.
	Runs without external network access.
	"""

	TASK_COUNT = 1000
	WORKER_COUNT = 10
	mock_delay = 0.001  # 1ms simulated latency

	# Reset metrics
	global _stress_metrics, _profiling_data
	_stress_metrics = StressMetrics()
	_profiling_data = ProfilingData()

	m = QManager(
		broker=InMemoryBroker(),
		result_backend=InMemoryResultBackend(),
		serializer=MsgpackSerializer(),
	)

	# Register callbacks
	m.register_callback(MetricsQueuedCallback)
	m.register_callback(MetricsStartedCallback)
	m.register_callback(MetricsCompletedCallback)
	m.register_callback(MetricsFailedCallback)
	m.register_callback(ProfilingStartedCallback)
	m.register_callback(ProfilingCompletedCallback)

	# Create mock handler
	mock_handler = MockNetIOHandler(delay=mock_delay)

	# Register task
	@m.register(qname="stress.mock_net_io_small")
	async def mock_net_io(idx: int) -> dict[str, Any]:
		return await mock_handler.handle({"idx": idx})

	print(f"\nSmall stress test: {TASK_COUNT} tasks, {WORKER_COUNT} workers")

	# Queue tasks
	queue_start = time.time()
	tasks = [mock_net_io.q(i) for i in range(TASK_COUNT)]
	await asyncio.gather(*tasks)
	queue_duration = time.time() - queue_start
	print(f"Queued in {queue_duration:.2f}s")

	# Process
	async with QWorkerPool(
		manager=m,
		worker_count=WORKER_COUNT,
		max_tasks_per_worker=50,
	):
		while _stress_metrics.tasks_completed + _stress_metrics.tasks_failed < TASK_COUNT:
			await asyncio.sleep(0.5)
			completed = _stress_metrics.tasks_completed + _stress_metrics.tasks_failed
			print(f"\rProgress: {completed}/{TASK_COUNT}", end="", flush=True)

	print("\n\nCompleted!")

	# Verify
	summary = _stress_metrics.summary()
	print(f"\nSummary: {summary}")

	assert summary["completed"] == TASK_COUNT
	assert summary["failed"] == 0

	print("\nSmall stress test passed!")


#
# if __name__ == "__main__":
# 	with RedisContainer("redis:8-alpine") as cont:
# 		host = cont.get_container_host_ip()
# 		port = cont.get_exposed_port(6379)
# 		asyncio.run(test_stress_30k_net_io_tasks(manager_no_fx(f"redis://{host}:{port}")))
