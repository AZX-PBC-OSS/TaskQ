"""Integration tests for snooze/retry/reservation round-trips with shielding against real Postgres.

Covers:
- Snooze round-trip with scheduled-wake
- RetryAfter round-trip
- Reservation denial metadata observable
- asyncio.shield on snooze write
- Concurrent snooze and cancel
- Snooze back-off then succeed, attempt unchanged across snooze cycle
"""

# ruff: noqa: S608 Why: schema name validated by WorkerSettings._post_load against _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches existing integration test pattern

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.context import JobContext
from taskq.exceptions import ReservationUnavailable, RetryAfter, Snooze
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    from taskq.backend.postgres import PostgresBackend

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    WorkerDeps = PostgresBackend = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg and worker modules are TYPE_CHECKING-only to avoid transitive imports in test modules
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive imports in test modules

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=60)


_DEFAULT_CONFIG = StubActorConfig(
    retry=RetryPolicy(kind="transient", max_attempts=10, jitter=0.0),
)


async def _dispatch_job(conn: _Conn, schema: str, worker_id: UUID, job_id: UUID) -> None:
    await conn.execute(
        f"""UPDATE \"{schema}\".jobs
        SET status = 'running',
            attempt = attempt + 1,
            locked_by_worker = $1,
            lock_expires_at = now() + interval '60 seconds',
            started_at = now(),
            last_heartbeat_at = now()
        WHERE id = $2 AND status = 'pending'""",
        worker_id,
        job_id,
    )


async def _advance_scheduled_to_pending(conn: _Conn, schema: str, job_id: UUID) -> None:
    await conn.execute(
        f"""UPDATE \"{schema}\".jobs
        SET scheduled_at = now() - interval '1 second'
        WHERE id = $1""",
        job_id,
    )
    await conn.execute(
        f"""UPDATE \"{schema}\".jobs
        SET status = 'pending'
        WHERE id = $1 AND status = 'scheduled' AND scheduled_at <= now()""",
        job_id,
    )


async def _run_consume_with_actor(
    backend: PostgresBackend,
    job: JobRow,
    worker_id: UUID,
    actor: object,
    actor_config: StubActorConfig | None = None,
) -> None:
    """Run consume_one_job against the PG backend with a stub actor function.

    ``actor`` must be an ``async def (JobRow, JobContext) -> Awaitable[object]`` callable.
    """
    cfg = actor_config if actor_config is not None else _DEFAULT_CONFIG
    await consume_one_job(
        backend,
        job,
        worker_id,
        run_actor=actor,  # type: ignore[arg-type] # Why: actor is object-typed for pyright; at runtime it is an async callable with signature (JobRow, JobContext) -> Awaitable[object]
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=SystemClock(),
    )


# ── Snooze round-trip with scheduled-wake ──────────────────────


async def test_snooze_round_trip_with_scheduled_wake(
    clean_jobs_app: JobsApp,
) -> None:
    """snooze round-trip with scheduled-wake."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="snooze_actor",
            queue="default",
            payload={},
            max_attempts=10,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row = await backend.get(job_id)
    assert row is not None
    assert row.attempt == 1
    assert row.status == "running"

    async def snooze_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=5))

    await _run_consume_with_actor(backend, row, worker_id, snooze_actor)

    row_after = await backend.get(job_id)
    assert row_after is not None
    assert row_after.status == "scheduled"
    assert row_after.attempt == 1  # attempt unchanged by snooze

    # scheduled→pending wake + re-dispatch
    async with deps.worker_pool.acquire() as conn:
        await _advance_scheduled_to_pending(conn, schema, job_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row2 = await backend.get(job_id)
    assert row2 is not None
    assert row2.attempt == 2  # dispatch increments from 1 → 2
    assert row2.status == "running"

    async def success_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    await _run_consume_with_actor(backend, row2, worker_id, success_actor)

    final = await backend.get(job_id)
    assert final is not None
    assert final.status == "succeeded"
    assert final.attempt == 2

    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 2
    snoozed_attempt = next(a for a in attempts if a.outcome == "snoozed")
    assert snoozed_attempt.attempt == 1
    succeeded_attempt = next(a for a in attempts if a.outcome == "succeeded")
    assert succeeded_attempt.attempt == 2


# ── RetryAfter round-trip ─────────────────────────────────────


async def test_retry_after_round_trip(
    clean_jobs_app: JobsApp,
) -> None:
    """RetryAfter round-trip."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="retry_actor",
            queue="default",
            payload={},
            max_attempts=10,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row = await backend.get(job_id)
    assert row is not None
    assert row.attempt == 1

    async def retry_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=5), consume_budget=True)

    await _run_consume_with_actor(backend, row, worker_id, retry_actor)

    row_after = await backend.get(job_id)
    assert row_after is not None
    assert row_after.status == "scheduled"
    assert row_after.attempt == 1

    async with deps.worker_pool.acquire() as conn:
        await _advance_scheduled_to_pending(conn, schema, job_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row2 = await backend.get(job_id)
    assert row2 is not None
    assert row2.attempt == 2

    async def success_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    await _run_consume_with_actor(backend, row2, worker_id, success_actor)

    final = await backend.get(job_id)
    assert final is not None
    assert final.status == "succeeded"
    assert final.attempt == 2

    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 2
    outcomes = [a.outcome for a in attempts]
    assert "snoozed" in outcomes
    assert "succeeded" in outcomes


# ── Reservation denial metadata observable ──────────────────────


async def test_reservation_denial_metadata_observable(
    clean_jobs_app: JobsApp,
) -> None:
    """reservation denial metadata observable."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="reservation_actor",
            queue="default",
            payload={},
            max_attempts=10,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row = await backend.get(job_id)
    assert row is not None

    async def reservation_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise ReservationUnavailable("gpu_pool", timedelta(seconds=10))

    await _run_consume_with_actor(backend, row, worker_id, reservation_actor)

    async with deps.worker_pool.acquire() as conn:
        awaiting = await conn.fetchval(
            f"SELECT metadata->>'awaiting' FROM \"{schema}\".jobs WHERE id = $1",
            job_id,
        )
    assert awaiting == "reservation:gpu_pool"

    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "reservation_denied"
    assert attempts[0].attempt == 1

    row_after = await backend.get(job_id)
    assert row_after is not None
    assert row_after.status == "scheduled"

    async with deps.worker_pool.acquire() as conn:
        await _advance_scheduled_to_pending(conn, schema, job_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row2 = await backend.get(job_id)
    assert row2 is not None
    assert row2.attempt == 2  # dispatch increments from 1 (snooze preserves attempt)

    async def success_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    await _run_consume_with_actor(backend, row2, worker_id, success_actor)

    final = await backend.get(job_id)
    assert final is not None
    assert final.status == "succeeded"
    assert final.attempt == 2  # snooze did not consume budget; dispatch increment is expected


# ── asyncio.shield on snooze write ──────────────────────────────


async def test_shield_on_snooze_write(
    clean_jobs_app: JobsApp,
) -> None:
    """asyncio.shield on snooze write — shielded mark_snoozed completes under cancellation."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="shield_actor",
            queue="default",
            payload={},
            max_attempts=10,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row = await backend.get(job_id)
    assert row is not None

    cancel_window = asyncio.Event()
    from taskq.backend import _terminal as _terminal_mod

    original_insert = _terminal_mod._insert_attempt

    async def _delayed_insert(
        conn: _Conn,
        sql: object,
        jid: JobId,
        att: int,
        started: datetime | None,
        outcome: str,
        ec: str | None,
        em: str | None,
        et: str | None,
        dur: int | None,
        wid: UUID | None,
    ) -> None:
        cancel_window.set()
        await asyncio.sleep(0.1)
        await original_insert(conn, sql, jid, att, started, outcome, ec, em, et, dur, wid)

    _terminal_mod._insert_attempt = _delayed_insert  # type: ignore[assignment]  # Why: monkeypatching to inject cancellation window

    async def snooze_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=5))

    async def run() -> None:
        await _run_consume_with_actor(backend, row, worker_id, snooze_actor)

    task = asyncio.create_task(run())
    await asyncio.wait_for(cancel_window.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0.3)

    final = await backend.get(job_id)
    assert final is not None
    assert final.status == "scheduled"

    _terminal_mod._insert_attempt = original_insert  # type: ignore[assignment]  # Why: restoring after monkeypatch


# ── Concurrent snooze and cancel ────────────────────────────────


async def test_concurrent_snooze_and_cancel(
    clean_jobs_app: JobsApp,
) -> None:
    """concurrent snooze and cancel — exactly one transition wins."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="concurrent_actor",
            queue="default",
            payload={},
            max_attempts=10,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row = await backend.get(job_id)
    assert row is not None

    async def snooze_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=5))

    async def run_snooze() -> None:
        await _run_consume_with_actor(backend, row, worker_id, snooze_actor)

    async def run_cancel() -> bool:
        return await backend.mark_cancelled(job_id, worker_id)

    snooze_task = asyncio.create_task(run_snooze())
    await asyncio.sleep(0.001)
    cancel_task = asyncio.create_task(run_cancel())

    results = await asyncio.gather(snooze_task, cancel_task, return_exceptions=True)
    for i, r in enumerate(results):
        assert not isinstance(r, BaseException), f"unexpected exception in task {i}: {r}"

    final = await backend.get(job_id)
    assert final is not None
    assert final.status in ("scheduled", "cancelled")

    attempts = await backend.get_attempts(job_id)
    if final.status == "scheduled":
        assert any(a.outcome == "snoozed" for a in attempts)
        assert not any(a.outcome == "cancelled" for a in attempts)
    else:
        assert any(a.outcome == "cancelled" for a in attempts)
        assert not any(a.outcome == "snoozed" for a in attempts)


# ── snooze back-off then succeed round-trip ──────────────────────────────


async def test_snooze_backoff_then_succeed_round_trip(
    clean_jobs_app: JobsApp,
) -> None:
    """Snooze back-off then succeed, attempt unchanged across snooze cycle."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="notify_test_actor",
            queue="default",
            payload={},
            max_attempts=10,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row = await backend.get(job_id)
    assert row is not None

    async def snooze_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=5))

    await _run_consume_with_actor(backend, row, worker_id, snooze_actor)

    async with deps.worker_pool.acquire() as conn:
        await _advance_scheduled_to_pending(conn, schema, job_id)
        await _dispatch_job(conn, schema, worker_id, job_id)

    row2 = await backend.get(job_id)
    assert row2 is not None

    async def success_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    await _run_consume_with_actor(backend, row2, worker_id, success_actor)

    final = await backend.get(job_id)
    assert final is not None
    assert final.status == "succeeded"
    assert final.attempt == 2  # snooze preserves attempt ; dispatch increments normally
