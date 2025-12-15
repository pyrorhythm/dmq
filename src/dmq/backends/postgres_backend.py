from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import msgspec

if TYPE_CHECKING:
    from ..abc.serializer import QSerializerProtocol


SCHEMA_SQL = """
create schema if not exists dmq;

create table if not exists dmq.task_results (
    id serial primary key,
    task_id varchar(26) unique not null,
    task_name varchar(255) not null,
    result jsonb,
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
"""


class RetentionPolicy(msgspec.Struct, frozen=True):
    success_ttl: int | None = 86400
    failed_ttl: int | None = 604800


class PostgresResultBackend:
    def __init__(
        self, dsn: str, serializer: QSerializerProtocol, retention_policy: RetentionPolicy | None = None
    ) -> None:
        self._type_memory: dict[str, type] = {}
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None
        self.retention_policy = retention_policy or RetentionPolicy()
        self.serializer = serializer
        self._cleanup_task: asyncio.Task | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self.dsn)
        await self._initialize_schema()
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def close(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task

        if self.pool:
            await self.pool.close()

    async def _initialize_schema(self) -> None:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")

        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)

    async def store_result(self, task_id: str, result: Any, status: str = "success", task_name: str = "") -> None:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")

        expires_at = None
        if self.retention_policy:
            ttl = self.retention_policy.success_ttl if status == "success" else self.retention_policy.failed_ttl
            if ttl:
                expires_at = datetime.now(UTC) + timedelta(seconds=ttl)

        result_data = msgspec.json.encode(result)
        result_json = msgspec.json.decode(result_data) if isinstance(result_data, bytes) else result_data

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                insert into dmq.task_results
                (task_id, task_name, result, status, completed_at, expires_at)
                values ($1, $2, $3::jsonb, $4, $5, $6)
                on conflict (task_id)
                do update set
                    result = excluded.result,
                    status = excluded.status,
                    completed_at = excluded.completed_at,
                    expires_at = excluded.expires_at
                """,
                task_id,
                task_name,
                result_json,
                status,
                datetime.now(UTC),
                expires_at,
            )

    async def get_result(self, task_id: str, timeout: float | None = None) -> Any:
        deadline = time.time() + timeout if timeout else None

        while True:
            if self.pool is None:
                raise ConnectionError("pg pool not initialized")
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("select result, status from dmq.task_results where task_id = $1", task_id)

            if row and row["status"] in ("success", "failed"):
                return msgspec.json.decode(row["result"])

            if deadline and time.time() >= deadline:
                raise TimeoutError(f"Result not available within {timeout}s")

            await asyncio.sleep(0.1)

    async def delete_result(self, task_id: str) -> None:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")
        async with self.pool.acquire() as conn:
            await conn.execute("delete from dmq.task_results where task_id = $1", task_id)

    async def result_exists(self, task_id: str) -> bool:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("select 1 from dmq.task_results where task_id = $1", task_id)
            return row is not None

    async def store_workflow_state(self, workflow_id: str, state: Any) -> None:
        msgspec.json.encode(state).decode()

        if self.pool is None:
            raise ConnectionError("pg pool not initialized")
        async with self.pool.acquire() as conn:
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
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select workflow_name, current_nodes, completed_nodes, results
                from dmq.workflow_states
                where workflow_id = $1
                """,
                workflow_id,
            )

            if not row:
                raise KeyError(f"Workflow state not found: {workflow_id}")

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
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "delete from dmq.task_results where expires_at is not null and expires_at < now()"
            )
            return int(result.split()[-1])
