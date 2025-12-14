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
CREATE SCHEMA IF NOT EXISTS sotq;

CREATE TABLE IF NOT EXISTS sotq.task_results (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(26) UNIQUE NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    result JSONB,
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'success', 'failed', 'retry')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_task_results_task_id ON sotq.task_results(task_id);
CREATE INDEX IF NOT EXISTS idx_task_results_expires_at ON sotq.task_results(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_results_status ON sotq.task_results(status);

CREATE TABLE IF NOT EXISTS sotq.workflow_states (
    workflow_id VARCHAR(26) PRIMARY KEY,
    workflow_name VARCHAR(255) NOT NULL,
    current_nodes JSONB NOT NULL,
    completed_nodes JSONB NOT NULL,
    results JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""


class RetentionPolicy(msgspec.Struct, frozen=True):
    success_ttl: int | None = 86400
    failed_ttl: int | None = 604800


class PostgresResultBackend:
    def __init__(
        self,
        dsn: str,
        serializer: QSerializerProtocol,
        retention_policy: RetentionPolicy | None = None,
    ) -> None:
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

    async def store_result(
        self,
        task_id: str,
        result: Any,
        status: str = "success",
        task_name: str = "",
    ) -> None:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")

        expires_at = None
        if self.retention_policy:
            ttl = self.retention_policy.success_ttl if status == "success" else self.retention_policy.failed_ttl
            if ttl:
                expires_at = datetime.now(UTC) + timedelta(seconds=ttl)

        result_data = self.serializer.serialize(result)
        result_json = msgspec.json.decode(result_data) if isinstance(result_data, bytes) else result_data

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO sotq.task_results
                (task_id, task_name, result, status, completed_at, expires_at)
                VALUES ($1, $2, $3::jsonb, $4, $5, $6)
                ON CONFLICT (task_id)
                DO UPDATE SET
                    result = EXCLUDED.result,
                    status = EXCLUDED.status,
                    completed_at = EXCLUDED.completed_at,
                    expires_at = EXCLUDED.expires_at
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
                row = await conn.fetchrow(
                    "SELECT result, status FROM sotq.task_results WHERE task_id = $1",
                    task_id,
                )

            if row and row["status"] in ("success", "failed"):
                return msgspec.json.decode(row["result"])

            if deadline and time.time() >= deadline:
                raise TimeoutError(f"Result not available within {timeout}s")

            await asyncio.sleep(0.1)

    async def delete_result(self, task_id: str) -> None:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM sotq.task_results WHERE task_id = $1",
                task_id,
            )

    async def result_exists(self, task_id: str) -> bool:
        if self.pool is None:
            raise ConnectionError("pg pool not initialized")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM sotq.task_results WHERE task_id = $1",
                task_id,
            )
            return row is not None

    async def store_workflow_state(
        self,
        workflow_id: str,
        state: Any,
    ) -> None:
        msgspec.json.encode(state).decode()

        if self.pool is None:
            raise ConnectionError("pg pool not initialized")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO sotq.workflow_states
                (workflow_id, workflow_name, current_nodes, completed_nodes, results, updated_at)
                VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6)
                ON CONFLICT (workflow_id)
                DO UPDATE SET
                    current_nodes = EXCLUDED.current_nodes,
                    completed_nodes = EXCLUDED.completed_nodes,
                    results = EXCLUDED.results,
                    updated_at = EXCLUDED.updated_at
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
                SELECT workflow_name, current_nodes, completed_nodes, results
                FROM sotq.workflow_states
                WHERE workflow_id = $1
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
                "DELETE FROM sotq.task_results WHERE expires_at IS NOT NULL AND expires_at < NOW()"
            )
            return int(result.split()[-1])
