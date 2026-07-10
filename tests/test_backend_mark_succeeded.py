"""Tests for mark_succeeded_with_conn on both Backend implementations.

Covers:
- PG: conn-aware variant uses supplied connection and commits atomically
- PG: conn-aware variant rolls back when caller rolls back
- In-memory: delegation to mark_succeeded works
- Regression: autonomous mark_succeeded still works after helper extraction
"""

# ruff: noqa: S608 Why: schema name validated by WorkerSettings._post_load against _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches existing integration test pattern

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.pg import create_running_job, create_worker

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object


# ── PG: conn-aware variant uses supplied connection ─────────────────────


@pytest.mark.integration
async def test_pg_conn_aware_commits_on_supplied_connection(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(conn, schema, worker_id)

        async with conn.transaction():
            ok = await backend.mark_succeeded_with_conn(
                conn,
                JobId(job_id),
                worker_id,
                {"ok": True},
            )
            assert ok is True

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, result FROM "{schema}".jobs WHERE id = $1', job_id
        )
    assert row is not None
    assert row["status"] == "succeeded"


# ── PG: conn-aware variant rolls back when caller rolls back ────────────


@pytest.mark.integration
async def test_pg_conn_aware_rolls_back_with_caller(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(conn, schema, worker_id)

    async with deps.worker_pool.acquire() as conn:
        try:
            async with conn.transaction():
                ok = await backend.mark_succeeded_with_conn(
                    conn,
                    JobId(job_id),
                    worker_id,
                    {"ok": True},
                )
                assert ok is True
                raise RuntimeError("simulate caller rollback")
        except RuntimeError:
            pass

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    assert row["status"] == "running"


# ── In-memory: delegation works ─────────────────────────────────────────


async def test_in_memory_delegation() -> None:
    clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)

    args = make_enqueue_args(scheduled_at=clock.now())
    row = await backend.enqueue(args)
    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only access to in-memory backend internals
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    job_id = dispatched[0].id

    ok = await backend.mark_succeeded_with_conn(None, job_id, backend._worker_id, {"ok": True})  # type: ignore[reportPrivateUsage] # Why: test-only access to in-memory backend internals
    assert ok is True

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded"


# ── Regression: autonomous mark_succeeded still works ──────────────────


@pytest.mark.integration
async def test_pg_autonomous_mark_succeeded_still_works(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    deps, backend = clean_jobs_app
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(conn, schema, worker_id)

    ok = await backend.mark_succeeded(JobId(job_id), worker_id, {"ok": True})
    assert ok is True

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    assert row["status"] == "succeeded"


async def test_in_memory_autonomous_mark_succeeded_still_works() -> None:
    clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)

    args = make_enqueue_args(scheduled_at=clock.now())
    row = await backend.enqueue(args)
    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only access to in-memory backend internals
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    job_id = dispatched[0].id

    ok = await backend.mark_succeeded(job_id, backend._worker_id, {"ok": True})  # type: ignore[reportPrivateUsage] # Why: test-only access to in-memory backend internals
    assert ok is True

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded"
