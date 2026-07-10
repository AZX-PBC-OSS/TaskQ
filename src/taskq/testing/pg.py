from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple
from uuid import UUID

from taskq._ids import new_uuid
from taskq._json import dumps_str
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.testing.assertions import parse_detail

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy

__all__ = [
    "DEFAULT_ACTORS",
    "JobTriple",
    "create_pending_job",
    "create_running_job",
    "create_worker",
    "create_workered_running_job",
    "get_job_triple",
    "parse_detail",
    "reset_schema",
    "seed_actors",
    "setup_running_job",
    "truncate_schema",
]


async def _create_worker(
    conn: _Conn,
    schema: str,
    worker_id: UUID,
) -> None:
    """Insert a worker row used by integration tests that exercise leader
    election or per-attempt history (both still FK to workers(id)).
    ``jobs.locked_by_worker`` is not an FK, so
    tests that only dispatch/lock jobs do not strictly need this — it is
    kept for tests that also write ``job_attempts`` or ``maintenance_leader``.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608
        worker_id,
        "test-host",
        12345,
        ["default"],
    )


create_worker = _create_worker

# Default actor_config rows seeded into every test schema.
# The dispatch CTE requires explicit rows in actor_config — even uncapped
# actors need an entry.  Test authors can override via seed_actors() or
# pass their own list to reset_schema().
DEFAULT_ACTORS: tuple[str, ...] = (
    "actor_a",
    "actor_b",
    "actor_c",
    "A",
    "C",
    "X",
    "test_actor",
    "_progress_redis_hundred",
    "_progress_redis_single",
    "_progress_redis_three",
)

# Tables truncated by truncate_schema() in FK-safe cascade order.
# schema_migrations is excluded — migration metadata is not test data.
_TRUNCATE_TABLES: tuple[str, ...] = (
    "reservation_slots",
    "rate_limit_window_entries",
    "rate_limit_buckets",
    "cron_schedules",
    "jobs_archive",
    "jobs",
    "workers",
    "actor_config",
    "queues",
)


async def truncate_schema(conn: _Conn, schema: str) -> None:
    """Truncate all dynamic tables in FK-safe order using CASCADE.

    Leaves ``schema_migrations`` intact.  Safe to call repeatedly.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    for table in _TRUNCATE_TABLES:
        await conn.execute(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')


async def seed_actors(
    conn: _Conn,
    schema: str,
    *,
    actors: Sequence[str] | None = None,
) -> None:
    """Insert actor_config rows for the given actors (or DEFAULT_ACTORS).

    ``ON CONFLICT (actor) DO NOTHING`` makes this safe to call
    alongside custom seed data — it never overwrites existing rows.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    target = actors if actors is not None else DEFAULT_ACTORS
    await conn.executemany(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',  # noqa: S608
        [(actor, "default") for actor in target],
    )


async def reset_schema(
    conn: _Conn,
    schema: str,
    *,
    actors: Sequence[str] | None = None,
) -> None:
    """Truncate all dynamic tables then seed default actor_config rows.

    Tests needing a custom actor set can pass ``actors=[...]``;
    tests that need an empty actor_config can pass ``actors=[]``.
    """
    await truncate_schema(conn, schema)
    await seed_actors(conn, schema, actors=actors)


class JobTriple(NamedTuple):
    row: asyncpg.Record
    attempts: list[asyncpg.Record]
    events: list[asyncpg.Record]


async def create_running_job(
    conn: _Conn,
    schema: str,
    worker_id: UUID,
    job_id: UUID | None = None,
    *,
    cancel_phase: int = 0,
    max_attempts: int = 3,
    retry_kind: str = "transient",
    attempt: int = 1,
    cancel_requested_at: datetime | None = None,
    lock_expires_at: datetime | None = None,
    schedule_to_close: datetime | None = None,
    with_events: bool = True,
) -> UUID:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    job_id = job_id or new_uuid()
    expires_at = lock_expires_at or (datetime.now(UTC) + timedelta(seconds=60))
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, attempt, scheduled_at,
            locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,
            cancel_phase, cancel_requested_at, schedule_to_close
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            'running', 0, $7, now(),
            $8, $9, now(), now(),
            $10, $11, $12
        )""",  # noqa: S608
        job_id,
        "test_actor",
        "default",
        '{"key": "value"}',
        max_attempts,
        retry_kind,
        attempt,
        worker_id,
        expires_at,
        cancel_phase,
        cancel_requested_at,
        schedule_to_close,
    )
    if with_events:
        detail = dumps_str(
            {"from_state": "pending", "to_state": "running", "worker_id": str(worker_id)}
        )
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail) '  # noqa: S608
            "VALUES ($1, $2, 'state_change', $3::jsonb)",
            job_id,
            now,
            detail,
        )
    return job_id


async def create_pending_job(
    conn: _Conn,
    schema: str,
    job_id: UUID | None = None,
    *,
    schedule_to_close: datetime | None = None,
    status: str = "pending",
    scheduled_at: datetime | None = None,
) -> UUID:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    job_id = job_id or new_uuid()
    stc = schedule_to_close or (datetime.now(UTC) + timedelta(seconds=60))
    sa = scheduled_at or datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            $7, 0, $8, $9
        )""",  # noqa: S608
        job_id,
        "test_actor",
        "default",
        '{"key": "value"}',
        3,
        "transient",
        status,
        sa,
        stc,
    )
    return job_id


async def get_job_triple(conn: _Conn, schema: str, job_id: UUID) -> JobTriple:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    row = await conn.fetchrow(
        f'SELECT * FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
        job_id,
    )
    assert row is not None
    attempts = await conn.fetch(
        f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1',  # noqa: S608
        job_id,
    )
    events = await conn.fetch(
        f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',  # noqa: S608
        job_id,
    )
    return JobTriple(row=row, attempts=list(attempts), events=list(events))


async def setup_running_job(
    conn: _Conn,
    schema: str,
    *,
    worker_id: UUID | None = None,
    job_id: UUID | None = None,
    attempt: int = 1,
    max_attempts: int = 3,
    retry_kind: str = "transient",
    cancel_phase: int = 0,
    cancel_requested_at: datetime | None = None,
    lock_expires_at: datetime | None = None,
    schedule_to_close: datetime | None = None,
    with_events: bool = True,
) -> tuple[UUID, UUID]:
    """Create a worker row and a running job row in one call.

    Returns ``(worker_id, job_id)``.  Delegates to
    :func:`create_workered_running_job`.
    """
    return await create_workered_running_job(
        conn,
        schema,
        worker_id=worker_id,
        job_id=job_id,
        cancel_phase=cancel_phase,
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        attempt=attempt,
        cancel_requested_at=cancel_requested_at,
        lock_expires_at=lock_expires_at,
        schedule_to_close=schedule_to_close,
        with_events=with_events,
    )


async def create_workered_running_job(
    conn: _Conn,
    schema: str,
    *,
    worker_id: UUID | None = None,
    **job_kwargs: Any,
) -> tuple[UUID, UUID]:
    """Create a worker row and a running job row, returning ``(worker_id, job_id)``.

    Passthrough wrapper: creates a worker (generating a UUID if none provided),
    then creates a running job belonging to that worker.  All extra keyword
    arguments are forwarded to :func:`create_running_job`.
    """
    wid = worker_id or new_uuid()
    await _create_worker(conn, schema, wid)
    jid = await create_running_job(conn, schema, wid, **job_kwargs)
    return wid, jid
