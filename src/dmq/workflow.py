from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import msgspec
from loguru import logger
from ulid import ulid

from .callback import Callback, CallbackRule
from .events.core import (
	QEventType,
	QWorkflowCompleted,
	QWorkflowFailed,
	QWorkflowStarted,
	QWorkflowStepCompleted,
)

if TYPE_CHECKING:
	from .manager import QManager
	from .task import QTask


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _TaskStep:
	"""Execute a single task."""

	task: QTask


@dataclass(frozen=True)
class _ParallelStep:
	"""Execute multiple tasks concurrently; the next step receives a list of
	results in the same order the tasks were declared."""

	tasks: tuple[QTask, ...]


@dataclass(frozen=True)
class _ConditionalBranch:
	predicate: Callable[[Any], bool]
	task: QTask


@dataclass(frozen=True)
class _ConditionalStep:
	"""Route to a task chosen by predicate evaluation against the previous
	step's result."""

	branches: tuple[_ConditionalBranch, ...]
	default: QTask | None = None


type _Step = _TaskStep | _ParallelStep | _ConditionalStep


# ---------------------------------------------------------------------------
# Conditional builder  (returned by Workflow.when)
# ---------------------------------------------------------------------------


class _ConditionalBuilder:
	"""Fluent helper that collects ``.when(pred).then(task)`` clauses and
	returns control back to the parent :class:`Workflow`."""

	def __init__(self, workflow: Workflow) -> None:
		self._workflow = workflow
		self._branches: list[_ConditionalBranch] = []
		self._default: QTask | None = None
		self._pending_predicate: Callable[[Any], bool] | None = None

	# -- building branches ---------------------------------------------------

	def then(self, task: QTask) -> _ConditionalBuilder:
		if self._pending_predicate is None:
			raise ValueError(".then() must follow .when()")
		self._branches.append(_ConditionalBranch(self._pending_predicate, task))
		self._pending_predicate = None
		return self

	def when(self, predicate: Callable[[Any], bool]) -> _ConditionalBuilder:
		if self._pending_predicate is not None:
			raise ValueError(".when() called without a preceding .then()")
		self._pending_predicate = predicate
		return self

	# -- finalising ----------------------------------------------------------

	def otherwise(self, task: QTask) -> Workflow:
		"""Provide a fallback branch and return to the workflow."""
		self._default = task
		return self._finalize()

	def step(self, task: QTask) -> Workflow:
		"""Finalize the conditional (no default) and add the next step."""
		wf = self._finalize()
		return wf.step(task)

	def parallel(self, *tasks: QTask) -> Workflow:
		"""Finalize the conditional (no default) and add a parallel step."""
		wf = self._finalize()
		return wf.parallel(*tasks)

	def _finalize(self) -> Workflow:
		if self._pending_predicate is not None:
			raise ValueError(".when() without a matching .then()")
		step = _ConditionalStep(
			branches=tuple(self._branches),
			default=self._default,
		)
		self._workflow._steps.append(step)
		return self._workflow


# ---------------------------------------------------------------------------
# Workflow builder
# ---------------------------------------------------------------------------


class Workflow:
	"""Declarative, fluent workflow builder.

	Example::

	        wf = (
	            Workflow("order_pipeline")
	            .step(validate_order)
	            .parallel(send_email, send_sms)
	            .when(lambda r: r[0] == "ok")
	            .then(finalize)
	            .otherwise(compensate)
	        )

	        handle = await wf.run(manager, order_id)
	        result = await handle.result(timeout=30.0)
	"""

	def __init__(self, name: str) -> None:
		self.name = name
		self._steps: list[_Step] = []

	# -- fluent API ----------------------------------------------------------

	def step(self, task: QTask) -> Workflow:
		"""Append a single-task step."""
		self._steps.append(_TaskStep(task=task))
		return self

	def parallel(self, *tasks: QTask) -> Workflow:
		"""Append a parallel step (all tasks run concurrently)."""
		if len(tasks) < 2:
			raise ValueError("parallel() requires at least 2 tasks")
		self._steps.append(_ParallelStep(tasks=tasks))
		return self

	def when(self, predicate: Callable[[Any], bool]) -> _ConditionalBuilder:
		"""Start a conditional routing block."""
		builder = _ConditionalBuilder(self)
		builder._pending_predicate = predicate
		return builder

	# -- execution -----------------------------------------------------------

	async def run(self, manager: QManager, *args: Any, **kwargs: Any) -> QOngoingWorkflow:
		"""Execute this workflow on *manager* and return a result handle."""
		return await manager.workflow_engine.start(self, args, kwargs)


# ---------------------------------------------------------------------------
# Result handle
# ---------------------------------------------------------------------------


class QOngoingWorkflow(msgspec.Struct, frozen=True):
	"""Handle returned by :meth:`Workflow.run`, analogous to
	:class:`~dmq.types.QOngoingTask`."""

	_id: str
	_manager: QManager

	async def result(self, timeout: float = 30.0) -> Any:
		return await self._manager.result_backend.get_result(self._id, timeout)

	@property
	def id(self) -> str:
		return self._id


# ---------------------------------------------------------------------------
# Internal execution state
# ---------------------------------------------------------------------------

_UNSET = object()


class _WorkflowExecution:
	"""Tracks the progress of a single workflow run."""

	def __init__(self, workflow_id: str, workflow: Workflow, engine: WorkflowEngine) -> None:
		self.workflow_id = workflow_id
		self.workflow = workflow
		self.engine = engine
		self.current_step = 0
		self.last_result: Any = _UNSET
		self._initial_args: tuple[Any, ...] = ()
		self._initial_kwargs: dict[str, Any] = {}
		# parallel bookkeeping
		self._pending_task_ids: set[str] = set()
		self._parallel_results: dict[str, Any] = {}
		self._parallel_order: list[str] = []

	# -- lifecycle -----------------------------------------------------------

	async def start(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
		self._initial_args = args
		self._initial_kwargs = kwargs
		await self._execute_step(0)

	async def _execute_step(self, step_index: int) -> None:
		if step_index >= len(self.workflow._steps):
			await self._complete()
			return

		self.current_step = step_index
		step = self.workflow._steps[step_index]

		if isinstance(step, _TaskStep):
			handle = await self._enqueue(step.task)
			self._track(handle.id)

		elif isinstance(step, _ParallelStep):
			self._parallel_results = {}
			self._parallel_order = []
			for task in step.tasks:
				handle = await self._enqueue(task)
				self._track(handle.id)
				self._parallel_order.append(handle.id)

		elif isinstance(step, _ConditionalStep):
			value = self._resolve_input_value()
			selected: QTask | None = None
			for branch in step.branches:
				if branch.predicate(value):
					selected = branch.task
					break
			if selected is None:
				selected = step.default
			if selected is None:
				raise RuntimeError(
					f"No matching branch in conditional step {step_index} of workflow '{self.workflow.name}'"
				)
			handle = await selected.q(value)
			self._track(handle.id)

	# -- helpers -------------------------------------------------------------

	def _resolve_input_value(self) -> Any:
		"""Return the value to pass as input to the current step."""
		if self.last_result is _UNSET:
			if len(self._initial_args) == 1 and not self._initial_kwargs:
				return self._initial_args[0]
			return self._initial_args
		return self.last_result

	async def _enqueue(self, task: QTask) -> Any:  # noqa: ANN401
		"""Enqueue *task*, automatically choosing between initial args and
		the previous step's result."""
		if self.last_result is _UNSET:
			return await task.q(*self._initial_args, **self._initial_kwargs)
		return await task.q(self.last_result)

	def _track(self, task_id: str) -> None:
		self._pending_task_ids.add(task_id)
		self.engine._task_to_workflow[task_id] = self.workflow_id

	# -- event handlers ------------------------------------------------------

	async def on_task_completed(self, task_id: str, result: Any) -> None:
		self._pending_task_ids.discard(task_id)
		self.engine._task_to_workflow.pop(task_id, None)

		step = self.workflow._steps[self.current_step]

		# Emit step-completed event
		await self.engine.manager.event_router.emit(
			QWorkflowStepCompleted(
				workflow_id=self.workflow_id,
				workflow_name=self.workflow.name,
				step_index=self.current_step,
				task_id=task_id,
				timestamp=time.time(),
				result=result,
			)
		)

		if isinstance(step, _ParallelStep):
			self._parallel_results[task_id] = result
			if self._pending_task_ids:
				return  # still waiting for siblings
			self.last_result = [self._parallel_results[tid] for tid in self._parallel_order]
		else:
			self.last_result = result

		await self._execute_step(self.current_step + 1)

	async def on_task_failed(self, task_id: str, error: str) -> None:
		self._pending_task_ids.discard(task_id)
		self.engine._task_to_workflow.pop(task_id, None)

		# Cancel remaining parallel siblings (best-effort)
		for remaining_id in list(self._pending_task_ids):
			self.engine._task_to_workflow.pop(remaining_id, None)
		self._pending_task_ids.clear()

		await self.engine.manager.event_router.emit(
			QWorkflowFailed(
				workflow_id=self.workflow_id,
				workflow_name=self.workflow.name,
				timestamp=time.time(),
				error=error,
				step_index=self.current_step,
			)
		)

		await self.engine.manager.result_backend.store_result(self.workflow_id, error, status="FAILED")
		self.engine._active.pop(self.workflow_id, None)

	async def _complete(self) -> None:
		await self.engine.manager.event_router.emit(
			QWorkflowCompleted(
				workflow_id=self.workflow_id,
				workflow_name=self.workflow.name,
				timestamp=time.time(),
				result=self.last_result,
			)
		)
		await self.engine.manager.result_backend.store_result(self.workflow_id, self.last_result, status="SUCCESS")
		self.engine._active.pop(self.workflow_id, None)
		logger.info("workflow '{}' ({}) completed", self.workflow.name, self.workflow_id)


# ---------------------------------------------------------------------------
# Workflow engine
# ---------------------------------------------------------------------------


class _WorkflowCallback(Callback):
	"""Internal callback that drives workflow progression."""

	def __init__(self, engine: WorkflowEngine) -> None:
		super().__init__()
		self._engine = engine
		self.add_rule(CallbackRule(event_types={QEventType.TASK_COMPLETED, QEventType.TASK_FAILED}))

	async def handle(self, event) -> None:  # noqa: ANN001
		task_id: str = event.task_id
		if task_id not in self._engine._task_to_workflow:
			return
		if event.event_type == QEventType.TASK_COMPLETED:
			await self._engine._on_task_completed(task_id, event.result)
		elif event.event_type == QEventType.TASK_FAILED:
			await self._engine._on_task_failed(task_id, event.exception)


class WorkflowEngine:
	"""Orchestrates active workflow executions within a :class:`QManager`."""

	def __init__(self, manager: QManager) -> None:
		self.manager = manager
		self._active: dict[str, _WorkflowExecution] = {}
		self._task_to_workflow: dict[str, str] = {}
		self._callback_registered = False

	def _ensure_callback(self) -> None:
		if not self._callback_registered:
			callback = _WorkflowCallback(self)
			self.manager.event_router.register_callback(callback)
			self._callback_registered = True

	async def start(
		self,
		workflow: Workflow,
		args: tuple[Any, ...],
		kwargs: dict[str, Any],
	) -> QOngoingWorkflow:
		self._ensure_callback()

		workflow_id = f"wf-{ulid()}"
		execution = _WorkflowExecution(workflow_id, workflow, self)
		self._active[workflow_id] = execution

		await self.manager.event_router.emit(
			QWorkflowStarted(
				workflow_id=workflow_id,
				workflow_name=workflow.name,
				timestamp=time.time(),
			)
		)

		await execution.start(args, kwargs)

		return QOngoingWorkflow(workflow_id, self.manager)

	async def _on_task_completed(self, task_id: str, result: Any) -> None:
		workflow_id = self._task_to_workflow.get(task_id)
		if workflow_id is None:
			return
		execution = self._active.get(workflow_id)
		if execution is None:
			return
		await execution.on_task_completed(task_id, result)

	async def _on_task_failed(self, task_id: str, error: str) -> None:
		workflow_id = self._task_to_workflow.get(task_id)
		if workflow_id is None:
			return
		execution = self._active.get(workflow_id)
		if execution is None:
			return
		await execution.on_task_failed(task_id, error)
