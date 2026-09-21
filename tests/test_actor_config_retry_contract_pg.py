"""End-to-end pin for the actor retry contract on server-side fires.

Issue #405: `sync_actor_config`'s UPSERT never wrote
``actor_config.max_attempts``/``retry_kind``, so the row kept the DDL
defaults (3, 'transient') forever and every server-side fire (cron tick,
admin run-now) built its EnqueueArgs from those defaults instead of the
declared RetryPolicy. Producers were unaffected: a producer-side enqueue
carries the ActorRef's RetryPolicy literal in its EnqueueArgs directly.

These tests register through the real registration write path
(`sync_actor_config`, fed the way `worker/_bootstrap.py` builds the
carrier from the ActorRef), then fire a schedule server-side through the
real cron tick and inspect the created job row. Both the stored row and
the fired job must carry the declared contract.
"""

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.actor_config import ActorConfig
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import _create_worker
from taskq.worker.cron_loop import tick_cron
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.startup import sync_actor_config

pytestmark = pytest.mark.integration

# Deliberately not the DDL defaults (3, 'transient'): a rerun of the old
# binary would leave both columns at the defaults and pass nothing.
DECLARED_MAX_ATTEMPTS = 50
DECLARED_RETRY_KIND = "indefinite"

_HEARTBEAT_INTERVAL = 1.0
_LOCK_LEASE = 1250.0


def _build_settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema.lower(),
            "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
            "TASKQ_LOCK_LEASE": str(_LOCK_LEASE),
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "2.0",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
        }
    )


def _build_actor_config() -> ActorConfig:
    """Mirror _bootstrap.py's ActorConfig construction from the ActorRef literal."""
    retry = RetryPolicy(kind=DECLARED_RETRY_KIND, max_attempts=DECLARED_MAX_ATTEMPTS)
    return ActorConfig(
        actor="contract_actor",
        max_concurrent=None,
        max_pending=None,
        queue="default",
        result_ttl=None,
        metadata={},
        retry_base=retry.base,
        retry_cap=retry.cap,
        retry_backoff=retry.backoff,
        retry_jitter=retry.jitter,
        max_attempts=retry.max_attempts,
        retry_kind=retry.kind,
    )


@asynccontextmanager
async def _open_cron(
    pg_dsn: str, schema: str
) -> AsyncGenerator[tuple[str, AsyncExitStack, WorkerDeps, PostgresBackend, UUID], None]:
    settings = _build_settings(pg_dsn, schema)

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        backend: PostgresBackend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
    except BaseException:
        await stack.aclose()
        raise

    worker_id = new_uuid()
    async with deps.dispatcher_pool.acquire() as conn:
        await _create_worker(conn, settings.schema_name, worker_id)

    try:
        yield settings.schema_name, stack, deps, backend, worker_id
    finally:
        await stack.aclose()


@pytest.mark.asyncio
async def test_registration_seeds_declared_retry_contract(pg_dsn: str) -> None:
    """sync_actor_config writes the declared max_attempts/retry_kind, not
    the DDL defaults, on both first create and re-registration with a
    changed contract."""
    schema = f"test_contract_{new_base62()}"
    async with _open_cron(pg_dsn, schema) as (schema_name, _stack, deps, _backend, _wid):
        async with deps.dispatcher_pool.acquire() as conn:
            await sync_actor_config(conn, [_build_actor_config()], schema=schema_name)
            row = await conn.fetchrow(
                f"SELECT max_attempts, retry_kind FROM {schema_name}.actor_config "
                "WHERE actor = 'contract_actor'"
            )
            assert row is not None
            assert row["max_attempts"] == DECLARED_MAX_ATTEMPTS
            assert row["retry_kind"] == DECLARED_RETRY_KIND

            # Re-registration with a changed contract must move the row:
            # the columns are code-owned, the conflict arm rewrites them.
            changed = ActorConfig(
                actor="contract_actor",
                max_concurrent=None,
                queue="default",
                max_attempts=1,
                retry_kind="non_retryable",
            )
            await sync_actor_config(conn, [changed], schema=schema_name)
            row = await conn.fetchrow(
                f"SELECT max_attempts, retry_kind FROM {schema_name}.actor_config "
                "WHERE actor = 'contract_actor'"
            )
            assert row is not None
            assert row["max_attempts"] == 1
            assert row["retry_kind"] == "non_retryable"


@pytest.mark.asyncio
async def test_cron_fire_uses_declared_retry_contract(pg_dsn: str) -> None:
    """A server-side cron fire creates a job carrying the declared
    max_attempts/retry_kind, not the DDL defaults."""
    schema = f"test_contract_{new_base62()}"
    async with _open_cron(pg_dsn, schema) as (schema_name, _stack, deps, backend, wid):
        async with deps.dispatcher_pool.acquire() as conn:
            await sync_actor_config(conn, [_build_actor_config()], schema=schema_name)
            cron_expr = "* * * * *"
            await conn.execute(
                f'INSERT INTO "{schema_name}".cron_schedules '
                "(id, actor, cron_expr, timezone, dst_strategy, payload_factory, "
                "enabled, next_fire_at, metadata) "
                "VALUES ($1, $2, $3, $4, $5, NULL, TRUE, $6, $7::jsonb)",
                new_uuid(),
                "contract_actor",
                cron_expr,
                "UTC",
                "skip",
                datetime.now(UTC) - timedelta(hours=2),
                '{"static_payload": {"key": "value"}}',
            )

        async with deps.dispatcher_pool.acquire() as conn:
            async with conn.transaction():
                await tick_cron(conn, deps.settings, backend, schema_name, wid)

        async with deps.dispatcher_pool.acquire() as conn:
            job = await conn.fetchrow(
                f'SELECT max_attempts, retry_kind FROM "{schema_name}".jobs '
                "WHERE actor = 'contract_actor'"
            )
            assert job is not None
            assert job["max_attempts"] == DECLARED_MAX_ATTEMPTS, (
                "the cron-fired job must carry the declared max_attempts, "
                "not the actor_config DDL default of 3"
            )
            assert job["retry_kind"] == DECLARED_RETRY_KIND, (
                "the cron-fired job must carry the declared retry_kind, "
                "not the actor_config DDL default of 'transient'"
            )
