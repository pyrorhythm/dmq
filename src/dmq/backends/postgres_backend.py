from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

import asyncpg
import msgspec

from ..utils import _get_type_fqn, _get_type_from_fqn

if TYPE_CHECKING:
    pass

import threading

SCHEMA_SQL = """
create schema if not exists dmq;

create table if not exists dmq.task_results (
    id serial primary key,
    task_id varchar(26) unique not null,
    task_name varchar(255) not null,
    result jsonb,
    type_fqn text,
    status varchar(50) not null check (status in ('PENDING', 'SUCCESS', 'FAILED', 'RETRY')),
    created_at timestamptz default now(),
    completed_at timestamptz,
    expires_at timestamptz,
    metadata jsonb
);

create index if not exists idx_task_results_task_id on dmq.task_results(task_id);
create index if not exists idx_task_results_expires_at on dmq.task_results(expires_at) where expires_at is not null;
create index if not exists idx_task_results_status on dmq.task_results(status);

create table if not exists dmq.workflow_states (
    workflow_id varchar(26) primary key,
    workflow_name varchar(255) not null,
    current_nodes jsonb not null,
    completed_nodes jsonb not null,
    results jsonb not null,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create or replace function dmq.notify_task_completion()
returns trigger as $$
begin
    if new.status in ('SUCCESS', 'FAILED') then
        perform pg_notify('dmq_task_completion', new.task_id);
    end if;
    return new;
end;
$$ language plpgsql;

do $$
begin
    if not exists (
        select 1 from pg_trigger
        where tgname = 'task_completion_trigger'
        and tgrelid = 'dmq.task_results'::regclass
    ) then
        create trigger task_completion_trigger
        after insert or update on dmq.task_results
        for each row
        execute function dmq.notify_task_completion();
    end if;
end $$;
"""


class RetentionPolicy(msgspec.Struct, frozen=True):
    success_ttl: int | None = 86400
    failed_ttl: int | None = 604800


class PostgresResultBackend:
    def __init__(
        self, dsn: str, retention_policy: RetentionPolicy | None = None, type_serialization: bool = False
    ) -> None:
        """
        initialize postgres result backend

        :param dsn: postgres connection string
        :param retention_policy: RetentionPolicy
        :param type_serialization: (defaults to False) -- if to serialize types of results,
        requires worker and client to be in 1 working directory to correctly resolve FQN's

        """
        self._type_memory: dict[str, type] = {}
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None
        self._pools: dict[int, asyncpg.Pool] = {}
        self._pools_lock = threading.Lock()
        self.retention_policy = retention_policy or RetentionPolicy()
        self._cleanup_task: asyncio.Task | None = None
        self._cleanup_tasks: dict[int, asyncio.Task] = {}
        self.type_serialization = type_serialization

    async def _get_pool(self) -> asyncpg.Pool:
        try:
            loop = asyncio.get_running_loop()
            loop_id = id(loop)
        except RuntimeError:
            if self.pool is None:
                await self.connect()
            return self.pool  # type: ignore

        if loop_id in self._pools:
            return self._pools[loop_id]

        with self._pools_lock:
            if loop_id in self._pools:
                return self._pools[loop_id]

            pool = await asyncpg.create_pool(self.dsn)
            self._pools[loop_id] = pool

            if not self.pool:
                await self._initialize_schema_with_pool(pool)
                self.pool = pool

            if loop_id not in self._cleanup_tasks:
                self._cleanup_tasks[loop_id] = asyncio.create_task(self._cleanup_loop())

            return pool

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self.dsn)
        await self._initialize_schema()
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def close(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task

        for task in self._cleanup_tasks.values():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        if self.pool:
            await self.pool.close()

        with self._pools_lock:
            for pool in self._pools.values():
                if pool != self.pool:
                    await pool.close()
            self._pools.clear()

    async def _initialize_schema(self) -> None:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")

        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)

    async def _initialize_schema_with_pool(self, pool: asyncpg.Pool) -> None:
        async with pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)

    async def store_result(
        self, task_id: str, result: Any, status: Literal["SUCCESS", "FAILED"] = "SUCCESS", task_name: str = ""
    ) -> None:
        pool = await self._get_pool()

        expires_at = None
        if self.retention_policy:
            ttl = self.retention_policy.success_ttl if status == "SUCCESS" else self.retention_policy.failed_ttl
            if ttl:
                expires_at = datetime.now(UTC) + timedelta(seconds=ttl)

        type_fqn = None

        if self.type_serialization:
            type_fqn = _get_type_fqn(result)

        result_data = msgspec.json.encode(result).decode()

        async with pool.acquire() as conn:
            await conn.execute(
                """
                insert into dmq.task_results
                (task_id, task_name, result, type_fqn, status, completed_at, expires_at)
                values ($1, $2, $3::jsonb, $4, $5, $6, $7)
                on conflict (task_id)
                do update set
                    result = excluded.result,
                    status = excluded.status,
                    completed_at = excluded.completed_at,
                    expires_at = excluded.expires_at
                """,
                task_id,
                task_name,
                result_data,
                type_fqn,
                status,
                datetime.now(UTC),
                expires_at,
            )

    async def get_result(self, task_id: str, timeout: float | None = None) -> Any:  # noqa
        pool = await self._get_pool()

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "select result, status, type_fqn from dmq.task_results where task_id = $1", task_id
            )

        if row and row["status"] in ("SUCCESS", "FAILED"):
            kws = {}
            if self.type_serialization and row["type_fqn"]:
                kws["type"] = _get_type_from_fqn(row["type_fqn"])
            return msgspec.json.decode(row["result"], **kws)

        notification_event = asyncio.Event()

        def notification_callback(connection, pid, channel, payload):  # noqa
            if payload == task_id:
                notification_event.set()

        async with pool.acquire() as conn:
            await conn.add_listener("dmq_task_completion", notification_callback)

            try:
                row = await conn.fetchrow(
                    "select result, status, type_fqn from dmq.task_results where task_id = $1", task_id
                )

                if row and row["status"] in ("SUCCESS", "FAILED"):
                    kws = {}
                    if self.type_serialization and row["type_fqn"]:
                        kws["type"] = _get_type_from_fqn(row["type_fqn"])
                    return msgspec.json.decode(row["result"], **kws)

                if timeout:
                    try:
                        await asyncio.wait_for(notification_event.wait(), timeout=timeout)
                    except TimeoutError as exc:
                        raise TimeoutError(f"result not available within {timeout}s") from exc
                else:
                    await notification_event.wait()

                row = await conn.fetchrow(
                    "select result, status, type_fqn from dmq.task_results where task_id = $1", task_id
                )

                if row and row["status"] in ("SUCCESS", "FAILED"):
                    kws = {}
                    if self.type_serialization and row["type_fqn"]:
                        kws["type"] = _get_type_from_fqn(row["type_fqn"])
                    return msgspec.json.decode(row["result"], **kws)

                raise KeyError(f"task result disappeared after notification: {task_id}")

            finally:
                await conn.remove_listener("dmq_task_completion", notification_callback)

    async def delete_result(self, task_id: str) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute("delete from dmq.task_results where task_id = $1", task_id)

    async def result_exists(self, task_id: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("select 1 from dmq.task_results where task_id = $1", task_id)
            return row is not None

    async def store_workflow_state(self, workflow_id: str, state: Any) -> None:
        msgspec.json.encode(state).decode()

        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                insert into dmq.workflow_states
                (workflow_id, workflow_name, current_nodes, completed_nodes, results, updated_at)
                values ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6)
                on conflict (workflow_id)
                do update set
                    current_nodes = excluded.current_nodes,
                    completed_nodes = excluded.completed_nodes,
                    results = excluded.results,
                    updated_at = excluded.updated_at
                """,
                workflow_id,
                getattr(state, "workflow_name", "unknown"),
                msgspec.json.encode(getattr(state, "current_nodes", [])).decode(),
                msgspec.json.encode(getattr(state, "completed_nodes", [])).decode(),
                msgspec.json.encode(getattr(state, "results", {})).decode(),
                datetime.now(UTC),
            )

    async def get_workflow_state(self, workflow_id: str) -> Any:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select workflow_name, current_nodes, completed_nodes, results
                from dmq.workflow_states
                where workflow_id = $1
                """,
                workflow_id,
            )

            if not row:
                raise KeyError(f"workflow state not found: {workflow_id}")

            return {
                "workflow_id": workflow_id,
                "workflow_name": row["workflow_name"],
                "current_nodes": msgspec.json.decode(row["current_nodes"]),
                "completed_nodes": msgspec.json.decode(row["completed_nodes"]),
                "results": msgspec.json.decode(row["results"]),
            }

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(3600)
            await self.cleanup_expired()

    async def cleanup_expired(self) -> int:
        pool = await self._get_pool()

        async with pool.acquire() as conn:
            result = await conn.execute(
                "delete from dmq.task_results where expires_at is not null and expires_at < now()"
            )
            return int(result.split()[-1])
