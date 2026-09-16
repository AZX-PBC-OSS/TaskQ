"""Sweep 1's crashed terminal arm must stamp the job row's error fields.

The reclaim sweep has always written the attempt row with
``error_class='WorkerCrashed'`` and an ``error_message`` naming the
deadline that fired (lease or heartbeat), but the job row itself kept
``error_class = NULL``: an operator reading ``jobs`` had to join
``job_attempts`` to learn why the row crashed, while every other terminal
failure path (``DeadlineExceeded`` on the deadline sweep, the cancel-origin
markers on the cancel paths) self-describes on the row. The crashed arm now
stamps the same fields the attempt row carries, on both backends, mapped
from the arm that fired — never the sibling arm's message.

The cancel-in-flight exhausted arm is the deliberate boundary: it lands
``'cancelled'`` honouring the caller's in-flight request, and its row keeps
``error_class = NULL`` on both backends — none of the four cancel-origin
markers describes "the worker crashed mid-protocol" (the actor neither
yielded nor was interrupted), and the attempt row's ``WorkerCrashed`` plus
the event's ``cause`` carry the explanation instead. These tests pin both
sides of that line.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=30)
_EXPIRED_AGO = timedelta(seconds=10)
_START = datetime(2025, 6, 1, tzinfo=UTC)

# The two deadline messages, pinned verbatim as an operator reads them on
# the row — the same strings ``_ATTEMPT_MESSAGES`` feeds the attempt rows
# (test_sweep_expired_locks_bounded.py pins the attempt-row text; drift
# between the row and the attempt would make one record contradict the
# other).
_LOCK_MESSAGE = "lock expired before worker reported terminal state"
_HEARTBEAT_MESSAGE = "heartbeat timeout passed before worker reported terminal state"


# ── Postgres seeding ───────────────────────────────────────────────────


async def _seed_worker(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        "VALUES ($1, 'err-fields-host', 12345, ARRAY['default'])",
        worker_id,
    )


async def _seed_lease_expired_running_job(
    conn: asyncpg.Connection, schema: str, job_id: UUID, worker_id: UUID
) -> None:
    """Lease arm: running, lock expired, no heartbeat knob, no budget left."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs ('  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        " id, actor, queue, payload, max_attempts, retry_kind, status,"
        " priority, attempt, scheduled_at, started_at, last_heartbeat_at,"
        " locked_by_worker, lock_expires_at, cancel_phase"
        ") VALUES ("
        " $1, 'test_actor', 'default', '{}'::jsonb, 1, 'transient',"
        " 'running', 0, 1, clock_timestamp(), clock_timestamp(), clock_timestamp(),"
        " $2, clock_timestamp() - ($3::double precision * interval '1 second'), 0"
        ")",
        job_id,
        worker_id,
        _EXPIRED_AGO.total_seconds(),
    )


async def _seed_heartbeat_stale_running_job(
    conn: asyncpg.Connection, schema: str, job_id: UUID, worker_id: UUID
) -> None:
    """Heartbeat arm: lease still valid, per-job heartbeat deadline passed."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs ('  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        " id, actor, queue, payload, max_attempts, retry_kind, status,"
        " priority, attempt, scheduled_at, started_at, last_heartbeat_at,"
        " heartbeat_timeout, locked_by_worker, lock_expires_at, cancel_phase"
        ") VALUES ("
        " $1, 'test_actor', 'default', '{}'::jsonb, 1, 'transient',"
        " 'running', 0, 1, clock_timestamp(), clock_timestamp(),"
        " clock_timestamp() - interval '60 seconds', interval '5 seconds',"
        " $2, clock_timestamp() + interval '1 hour', 0"
        ")",
        job_id,
        worker_id,
    )


async def _pg_error_fields(
    conn: asyncpg.Connection, schema: str, job_id: UUID
) -> tuple[str, str | None, str | None]:
    row = await conn.fetchrow(
        f"SELECT status::text AS status, error_class, error_message "  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        f'FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert row is not None
    return row["status"], row["error_class"], row["error_message"]


# ── Postgres pins ──────────────────────────────────────────────────────


async def test_sweep1_crashed_row_stamps_error_fields_lease_arm(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The lease arm's crashed row self-describes: WorkerCrashed + the
    lock-expiry message, readable without joining job_attempts."""
    schema = module_pg_schema.schema_name
    job_id = new_uuid()
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    await _seed_lease_expired_running_job(clean_pg_conn, schema, job_id, worker_id)

    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _GRACE,
        _GRACE,
        schema=schema,
    )
    assert count == 1

    status, error_class, error_message = await _pg_error_fields(clean_pg_conn, schema, job_id)
    assert status == "crashed"
    assert error_class == "WorkerCrashed"
    assert error_message == _LOCK_MESSAGE


async def test_sweep1_crashed_row_stamps_error_fields_heartbeat_arm(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The heartbeat arm's crashed row names the heartbeat deadline, never
    the lease arm's — the same honesty standard the attempt rows carry."""
    schema = module_pg_schema.schema_name
    job_id = new_uuid()
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    await _seed_heartbeat_stale_running_job(clean_pg_conn, schema, job_id, worker_id)

    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _GRACE,
        _GRACE,
        schema=schema,
    )
    assert count == 1

    status, error_class, error_message = await _pg_error_fields(clean_pg_conn, schema, job_id)
    assert status == "crashed"
    assert error_class == "WorkerCrashed"
    assert error_message == _HEARTBEAT_MESSAGE


async def test_sweep1_cancelled_arm_keeps_error_fields_unset(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Boundary: the cancel-in-flight exhausted arm lands 'cancelled' with
    no error fields — the worker crashed mid-protocol, which no
    cancel-origin marker describes; the attempt row and event cause carry
    the explanation."""
    schema = module_pg_schema.schema_name
    job_id = new_uuid()
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    # max_attempts=1 (exhausted) with a cancel in-flight (phase 1), and the
    # lock expired past the deep cancel margin (grace + grace + 60s) so the
    # carve-out admits the row to crash-reclaim.
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs ('  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        " id, actor, queue, payload, max_attempts, retry_kind, status,"
        " priority, attempt, scheduled_at, started_at, last_heartbeat_at,"
        " locked_by_worker, lock_expires_at, cancel_phase, cancel_requested_at"
        ") VALUES ("
        " $1, 'test_actor', 'default', '{}'::jsonb, 1, 'transient',"
        " 'running', 0, 1, clock_timestamp(), clock_timestamp(), clock_timestamp(),"
        " $2, clock_timestamp() - interval '130 seconds', 1,"
        " clock_timestamp() - interval '130 seconds'"
        ")",
        job_id,
        worker_id,
    )

    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _GRACE,
        _GRACE,
        schema=schema,
    )
    assert count == 1

    status, error_class, error_message = await _pg_error_fields(clean_pg_conn, schema, job_id)
    assert status == "cancelled"
    assert error_class is None
    assert error_message is None


# ── In-memory twin pins ────────────────────────────────────────────────


def _make_memory_backend() -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


async def _seed_memory_running_job(
    memory: InMemoryBackend,
    *,
    heartbeat_timeout: timedelta | None = None,
    heartbeat_stale: bool = False,
) -> tuple[UUID, UUID]:
    """The twin's exhausted-crash corpus: running, attempt=max_attempts, and
    either an expired lease (default) or a stale heartbeat under a valid
    lease, mirroring the PG seeders above."""
    holder = new_uuid()
    args = make_enqueue_args(scheduled_at=_START, max_attempts=1)
    row = await memory.enqueue(args)
    if heartbeat_stale:
        # Lease valid an hour out; the last beat is 60s old against a 5s
        # per-job timeout — the heartbeat arm owns this row.
        memory._jobs[args.id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only seeding of the crash-reclaim shape, the established pattern from test_rt_sweeps_parity.py.
            row,
            status="running",
            attempt=1,
            started_at=_START - timedelta(seconds=60),
            last_heartbeat_at=_START - timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=5),
            locked_by_worker=holder,
            lock_expires_at=_START + timedelta(hours=1),
        )
    else:
        memory._jobs[args.id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only seeding of the crash-reclaim shape, the established pattern from test_rt_sweeps_parity.py.
            row,
            status="running",
            attempt=1,
            started_at=_START - _EXPIRED_AGO,
            locked_by_worker=holder,
            lock_expires_at=_START - _EXPIRED_AGO,
        )
    return args.id, holder


async def test_twin_crashed_row_stamps_error_fields_lease_arm() -> None:
    memory = _make_memory_backend()
    job_id, _ = await _seed_memory_running_job(memory)

    count = await memory.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 1

    job = await memory.get(job_id)
    assert job is not None
    assert job.status == "crashed"
    assert job.error_class == "WorkerCrashed"
    assert job.error_message == _LOCK_MESSAGE


async def test_twin_crashed_row_stamps_error_fields_heartbeat_arm() -> None:
    memory = _make_memory_backend()
    job_id, _ = await _seed_memory_running_job(memory, heartbeat_stale=True)

    count = await memory.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 1

    job = await memory.get(job_id)
    assert job is not None
    assert job.status == "crashed"
    assert job.error_class == "WorkerCrashed"
    assert job.error_message == _HEARTBEAT_MESSAGE


async def test_twin_cancelled_arm_keeps_error_fields_unset() -> None:
    """The twin's boundary half: the cancel-in-flight exhausted arm lands
    'cancelled' with the error fields untouched, exactly as PG's."""
    memory = _make_memory_backend()
    holder = new_uuid()
    args = make_enqueue_args(scheduled_at=_START, max_attempts=1)
    row = await memory.enqueue(args)
    memory._jobs[args.id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only seeding of the crash-reclaim shape, the established pattern from test_rt_sweeps_parity.py.
        row,
        status="running",
        attempt=1,
        started_at=_START - timedelta(seconds=130),
        locked_by_worker=holder,
        # Past the deep cancel margin (grace + grace + 60s) so the
        # carve-out admits the row despite the in-flight cancel.
        lock_expires_at=_START - timedelta(seconds=130),
        cancel_phase=1,
        cancel_requested_at=_START - timedelta(seconds=130),
    )

    count = await memory.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 1

    job = await memory.get(args.id)
    assert job is not None
    assert job.status == "cancelled"
    assert job.error_class is None
    assert job.error_message is None
