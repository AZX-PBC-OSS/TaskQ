"""Equivalence tests for state transitions across in-memory and PG backends.

Covers For every (from, to) in
VALID_TRANSITIONS plus the Sweep 1 bypass entries (running→pending and
running→crashed via reclaim_expired_locks), run the transition on both
backends and assert the oracle tuple matches.

The isolate_self bypass paths are NOT included — they are exercised via
in test_state_transitions_pg.py (the isolate_self
function operates on a fresh asyncpg connection, not through the Backend
protocol, so it cannot be parametrized via backend_pair).
"""

from __future__ import annotations

# ruff: noqa: S608 Why: schema name validated by WorkerSettings.post_load against _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches existing integration test pattern
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId
from taskq.backend.statemachine import VALID_TRANSITIONS
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    from taskq.backend.postgres import PostgresBackend
    from taskq.testing.in_memory import InMemoryBackend
    from taskq.worker.deps import WorkerDeps

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=60)
_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)


# ── Helpers ────────────────────────────────────────────────────────────


type OracleTuple = tuple[str, bool, str | None, int, str | None]


def _extract_oracle(
    *,
    status: str,
    finished_at: object,
    error_class: str | None,
    attempt: int,
    first_event_kind: str | None,
) -> OracleTuple:
    return (status, finished_at is not None, error_class, attempt, first_event_kind)


def _parametrize_transitions() -> list[tuple[str, str]]:
    transitions: list[tuple[str, str]] = []
    for from_status, to_set in sorted(VALID_TRANSITIONS.items()):
        for to_status in sorted(to_set):
            transitions.append((from_status, to_status))
    transitions.append(("running", "pending"))
    return transitions


_TRANSITIONS = _parametrize_transitions()


# ── Parametrized equivalence test ────────────────────────────


@pytest.mark.parametrize(
    "from_status,to_status",
    _TRANSITIONS,
    ids=[f"{f}->{t}" for f, t in _TRANSITIONS],
)
async def test_equivalence(
    from_status: str,
    to_status: str,
    jobs_app: JobsApp,
    memory_jobs: InMemoryBackend,
) -> None:
    """For every transition, oracle tuple matches across backends."""

    deps = jobs_app.deps
    pg_backend = jobs_app.backend
    mem_backend = memory_jobs
    schema = deps.settings.schema_name

    pg_worker_id = new_uuid()
    async with deps.dispatcher_pool.acquire() as conn:
        await create_worker(conn, schema, pg_worker_id)

    mem_worker_id = mem_backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access for dispatch_batch

    pg_oracle = await _run_transition_on_pg(
        pg_backend, deps, schema, pg_worker_id, from_status, to_status
    )
    mem_oracle = await _run_transition_on_memory(mem_backend, mem_worker_id, from_status, to_status)

    assert pg_oracle == mem_oracle, (
        f"oracle mismatch for {from_status} → {to_status}: PG={pg_oracle}, mem={mem_oracle}"
    )


# ── Mark-failed-or-retry already-terminal: both raise ─────────────────


async def test_mark_failed_or_retry_already_terminal_equivalence(
    jobs_app: JobsApp,
    memory_jobs: InMemoryBackend,
) -> None:
    """Both backends raise WorkerOwnershipMismatch on already-terminal job."""
    from taskq.exceptions import WorkerOwnershipMismatch

    deps = jobs_app.deps
    pg_backend = jobs_app.backend
    mem_backend = memory_jobs
    schema = deps.settings.schema_name

    error_info = ErrorInfo(
        error_class="TestError",
        error_message="test",
        error_traceback=None,
    )

    pg_worker_id = new_uuid()
    async with deps.dispatcher_pool.acquire() as conn:
        await create_worker(conn, schema, pg_worker_id)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC),
    )
    pg_row = await pg_backend.enqueue(args)
    dispatched = await pg_backend.dispatch_batch(
        pg_worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
    )
    assert len(dispatched) == 1
    await pg_backend.mark_succeeded(pg_row.id, pg_worker_id, result={"ok": True})

    with pytest.raises(WorkerOwnershipMismatch):
        await pg_backend.mark_failed_or_retry(
            pg_row.id, pg_worker_id, error_info, next_scheduled_at=None
        )

    mem_worker_id = mem_backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access
    mem_args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=mem_backend._clock.now(),  # type: ignore[reportPrivateUsage] # Why: test-only private access
    )
    await mem_backend.enqueue(mem_args)
    mem_dispatched = await mem_backend.dispatch_batch(
        mem_worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
    )
    assert len(mem_dispatched) == 1
    mem_job_id = mem_dispatched[0].id
    await mem_backend.mark_succeeded(mem_job_id, mem_worker_id, result={"ok": True})

    with pytest.raises(WorkerOwnershipMismatch):
        await mem_backend.mark_failed_or_retry(
            mem_job_id, mem_worker_id, error_info, next_scheduled_at=None
        )


# ── Dispatch-from-scheduled exclusion on PG ───────────────────────────


async def test_dispatch_excludes_future_scheduled(
    jobs_app: JobsApp,
) -> None:
    """Dispatch does not select jobs with scheduled_at > now() (evidence on PG)."""

    _deps = jobs_app.deps
    backend = jobs_app.backend

    worker_id = new_uuid()

    future = datetime.now(UTC) + timedelta(hours=1)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=future,
    )
    await backend.enqueue(args)

    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=10, lock_lease=_LOCK_LEASE
    )
    assert len(dispatched) == 0


# ── PG transition runner ──────────────────────────────────────────────


async def _run_transition_on_pg(
    backend: PostgresBackend,
    deps: WorkerDeps,
    schema: str,
    worker_id: UUID,
    from_status: str,
    to_status: str,
) -> OracleTuple:
    """Set up a job in from_status on PG, perform the transition, return oracle."""

    job_id = await _setup_job_pg(backend, deps, schema, worker_id, from_status, to_status)

    await _perform_transition_pg(backend, deps, schema, job_id, worker_id, from_status, to_status)

    row = await backend.get(job_id)
    assert row is not None

    async with deps.worker_pool.acquire() as conn:
        event = await conn.fetchrow(
            f"SELECT kind FROM \"{schema}\".job_events WHERE job_id = $1 AND kind = 'state_change' ORDER BY occurred_at ASC LIMIT 1",
            job_id,
        )
    first_kind: str | None = event["kind"] if event else None

    return _extract_oracle(
        status=row.status,
        finished_at=row.finished_at,
        error_class=row.error_class,
        attempt=row.attempt,
        first_event_kind=first_kind,
    )


async def _setup_job_pg(
    backend: PostgresBackend,
    deps: WorkerDeps,
    schema: str,
    worker_id: UUID,
    from_status: str,
    to_status: str,
) -> JobId:
    need_deadline = to_status in ("failed",)
    schedule_to_close = datetime.now(UTC) + timedelta(hours=1) if need_deadline else None

    if from_status == "pending":
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
            schedule_to_close=schedule_to_close,
        )
        row = await backend.enqueue(args)
        return row.id

    if from_status == "scheduled":
        if to_status == "failed":
            past = datetime.now(UTC) - timedelta(seconds=10)
            args = EnqueueArgs(
                id=new_job_id(),
                actor="test_actor",
                queue="default",
                payload={"key": "value"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC) + timedelta(hours=1),
                schedule_to_close=past,
            )
            row = await backend.enqueue(args)
            async with deps.worker_pool.acquire() as conn:
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'scheduled' WHERE id = $1",
                    row.id,
                )
            return row.id
        snooze_close = datetime.now(UTC) + timedelta(hours=2) if need_deadline else None
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
            schedule_to_close=snooze_close,
        )
        row = await backend.enqueue(args)
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1
        result = await backend.mark_snoozed(row.id, worker_id, delay=timedelta(hours=1))
        assert result == "scheduled"
        return row.id

    if from_status == "running":
        max_attempts = 1 if to_status in ("crashed",) else 3
        retry_kind: str = "non_retryable" if to_status == "crashed" else "transient"
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=max_attempts,
            retry_kind=retry_kind,  # type: ignore[arg-type] # Why: str is known-valid RetryKind value at runtime
            scheduled_at=datetime.now(UTC),
        )
        row = await backend.enqueue(args)
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1
        return row.id

    raise ValueError(f"unsupported from_status: {from_status!r}")


async def _perform_transition_pg(
    backend: PostgresBackend,
    deps: WorkerDeps,
    schema: str,
    job_id: JobId,
    worker_id: UUID,
    from_status: str,
    to_status: str,
) -> None:
    if (from_status, to_status) == ("pending", "running"):
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) >= 1

    elif (from_status, to_status) == ("pending", "cancelled"):
        ok = await backend.write_cancel_request(job_id, reason="test")
        assert ok is True

    elif (from_status, to_status) == ("pending", "failed"):
        past = datetime.now(UTC) - timedelta(seconds=10)
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET schedule_to_close = $1 WHERE id = $2',
                past,
                job_id,
            )
        count = await backend.deadline_sweep(datetime.now(UTC))
        assert count >= 1

    elif (from_status, to_status) == ("scheduled", "pending"):
        async with deps.worker_pool.acquire() as conn:
            # 5s margin, not 1s — see test_backend_equivalence.py's identical
            # fix for why (PG-server-clock vs. Python-client-clock skew).
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET scheduled_at = now() - interval '5 seconds' WHERE id = $1",
                job_id,
            )
        count = await backend.scheduled_to_pending(datetime.now(UTC))
        assert count >= 1

    elif (from_status, to_status) == ("scheduled", "cancelled"):
        ok = await backend.write_cancel_request(job_id, reason="test")
        assert ok is True

    elif (from_status, to_status) == ("scheduled", "failed"):
        count = await backend.deadline_sweep(datetime.now(UTC))
        assert count >= 1

    elif (from_status, to_status) == ("running", "succeeded"):
        ok = await backend.mark_succeeded(job_id, worker_id, result={"ok": True})
        assert ok is True

    elif (from_status, to_status) == ("running", "cancelled"):
        ok = await backend.mark_cancelled(job_id, worker_id)
        assert ok is True

    elif (from_status, to_status) == ("running", "failed"):
        error_info = ErrorInfo(
            error_class="TestError",
            error_message="test",
            error_traceback=None,
        )
        await backend.mark_failed_or_retry(job_id, worker_id, error_info, next_scheduled_at=None)

    elif (from_status, to_status) == ("running", "crashed"):
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET lock_expires_at = now() - interval '60 seconds' WHERE id = $1",
                job_id,
            )
        count = await backend.reclaim_expired_locks(
            datetime.now(UTC), _CANCEL_GRACE, _CLEANUP_GRACE
        )
        assert count >= 1

    elif (from_status, to_status) == ("running", "abandoned"):
        await backend.write_cancel_request(job_id, reason="test")
        await backend.write_cancel_escalation(job_id, worker_id, phase=2)
        ok = await backend.mark_abandoned(job_id)
        assert ok is True

    elif (from_status, to_status) == ("running", "scheduled"):
        result = await backend.mark_snoozed(job_id, worker_id, delay=timedelta(seconds=30))
        assert result == "scheduled"

    elif (from_status, to_status) == ("running", "pending"):
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET lock_expires_at = now() - interval '60 seconds' WHERE id = $1",
                job_id,
            )
        count = await backend.reclaim_expired_locks(
            datetime.now(UTC), _CANCEL_GRACE, _CLEANUP_GRACE
        )
        assert count >= 1

    else:
        raise ValueError(f"unsupported transition: {from_status!r} → {to_status!r}")


# ── In-memory transition runner ──────────────────────────────────────


async def _run_transition_on_memory(
    backend: InMemoryBackend,
    worker_id: UUID,
    from_status: str,
    to_status: str,
) -> OracleTuple:
    """Set up a job in from_status on in-memory backend, perform the transition, return oracle."""

    job_id = await _setup_job_memory(backend, worker_id, from_status, to_status)

    await _perform_transition_memory(backend, worker_id, job_id, from_status, to_status)

    row = await backend.get(job_id)
    assert row is not None

    events = await backend.get_events(job_id)
    first_kind: str | None = None
    for e in events:
        if e.kind == "state_change":
            first_kind = e.kind
            break

    return _extract_oracle(
        status=row.status,
        finished_at=row.finished_at,
        error_class=row.error_class,
        attempt=row.attempt,
        first_event_kind=first_kind,
    )


async def _setup_job_memory(
    backend: InMemoryBackend,
    worker_id: UUID,
    from_status: str,
    to_status: str,
) -> JobId:
    now = backend._clock.now()  # type: ignore[reportPrivateUsage] # Why: test-only private access for clock time
    need_deadline = to_status in ("failed",)
    schedule_to_close = now + timedelta(hours=1) if need_deadline else None

    if from_status == "pending":
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=now,
            schedule_to_close=schedule_to_close,
        )
        row = await backend.enqueue(args)
        return row.id

    if from_status == "scheduled":
        if to_status == "failed":
            past = now - timedelta(seconds=10)
            args = EnqueueArgs(
                id=new_job_id(),
                actor="test_actor",
                queue="default",
                payload={"key": "value"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=now + timedelta(hours=1),
                schedule_to_close=past,
            )
            row = await backend.enqueue(args)
            assert row.status == "scheduled"
            return row.id
        snooze_close = now + timedelta(hours=2) if need_deadline else None
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=now,
            schedule_to_close=snooze_close,
        )
        row = await backend.enqueue(args)
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1
        result = await backend.mark_snoozed(row.id, worker_id, delay=timedelta(hours=1))
        assert result == "scheduled"
        return row.id

    if from_status == "running":
        max_attempts = 1 if to_status in ("crashed",) else 3
        retry_kind: str = "non_retryable" if to_status == "crashed" else "transient"
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=max_attempts,
            retry_kind=retry_kind,  # type: ignore[arg-type] # Why: str is known-valid RetryKind value at runtime
            scheduled_at=now,
        )
        row = await backend.enqueue(args)
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) == 1
        return row.id

    raise ValueError(f"unsupported from_status: {from_status!r}")


async def _perform_transition_memory(
    backend: InMemoryBackend,
    worker_id: UUID,
    job_id: JobId,
    from_status: str,
    to_status: str,
) -> None:
    if (from_status, to_status) == ("pending", "running"):
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert len(dispatched) >= 1

    elif (from_status, to_status) == ("pending", "cancelled"):
        ok = await backend.write_cancel_request(job_id, reason="test")
        assert ok is True

    elif (from_status, to_status) == ("pending", "failed"):
        from taskq.testing.clock import FakeClock

        assert isinstance(backend._clock, FakeClock)  # type: ignore[reportPrivateUsage] # Why: test-only private access to verify FakeClock
        backend.advance_clock_to(backend._clock.now() + timedelta(hours=2))  # type: ignore[reportPrivateUsage] # Why: test-only private access
        count = await backend.deadline_sweep(backend._clock.now())  # type: ignore[reportPrivateUsage] # Why: test-only private access
        assert count >= 1

    elif (from_status, to_status) == ("scheduled", "pending"):
        from taskq.testing.clock import FakeClock

        assert isinstance(backend._clock, FakeClock)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        backend.advance_clock_to(backend._clock.now() + timedelta(hours=2))  # type: ignore[reportPrivateUsage] # Why: test-only private access
        count = await backend.scheduled_to_pending(backend._clock.now())  # type: ignore[reportPrivateUsage] # Why: test-only private access
        assert count >= 1

    elif (from_status, to_status) == ("scheduled", "cancelled"):
        ok = await backend.write_cancel_request(job_id, reason="test")
        assert ok is True

    elif (from_status, to_status) == ("scheduled", "failed"):
        from taskq.testing.clock import FakeClock

        assert isinstance(backend._clock, FakeClock)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        backend.advance_clock_to(backend._clock.now() + timedelta(hours=2))  # type: ignore[reportPrivateUsage] # Why: test-only private access
        count = await backend.deadline_sweep(backend._clock.now())  # type: ignore[reportPrivateUsage] # Why: test-only private access
        assert count >= 1

    elif (from_status, to_status) == ("running", "succeeded"):
        ok = await backend.mark_succeeded(job_id, worker_id, result={"ok": True})
        assert ok is True

    elif (from_status, to_status) == ("running", "cancelled"):
        ok = await backend.mark_cancelled(job_id, worker_id)
        assert ok is True

    elif (from_status, to_status) == ("running", "failed"):
        error_info = ErrorInfo(
            error_class="TestError",
            error_message="test",
            error_traceback=None,
        )
        await backend.mark_failed_or_retry(job_id, worker_id, error_info, next_scheduled_at=None)

    elif (from_status, to_status) == ("running", "crashed"):
        from taskq.testing.clock import FakeClock

        assert isinstance(backend._clock, FakeClock)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        backend.advance_clock_to(backend._clock.now() + timedelta(minutes=2))  # type: ignore[reportPrivateUsage] # Why: test-only private access — lock_expires_at = now + 60s at dispatch, advance past it
        await backend.reclaim_expired_locks(
            backend._clock.now(),  # type: ignore[reportPrivateUsage] # Why: test-only private access
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
        )

    elif (from_status, to_status) == ("running", "abandoned"):
        await backend.write_cancel_request(job_id, reason="test")
        await backend.write_cancel_escalation(job_id, worker_id, phase=2)
        ok = await backend.mark_abandoned(job_id)
        assert ok is True

    elif (from_status, to_status) == ("running", "scheduled"):
        result = await backend.mark_snoozed(job_id, worker_id, delay=timedelta(seconds=30))
        assert result == "scheduled"

    elif (from_status, to_status) == ("running", "pending"):
        from taskq.testing.clock import FakeClock

        assert isinstance(backend._clock, FakeClock)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        backend.advance_clock_to(backend._clock.now() + timedelta(minutes=2))  # type: ignore[reportPrivateUsage] # Why: test-only private access — lock_expires_at = now + 60s at dispatch, advance past it
        await backend.reclaim_expired_locks(
            backend._clock.now(),  # type: ignore[reportPrivateUsage] # Why: test-only private access
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
        )

    else:
        raise ValueError(f"unsupported transition: {from_status!r} → {to_status!r}")
