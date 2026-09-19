# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team attacks on the outbox slice's immortality (event retention exemption).

The crash-reclaim outbox - ``kind='state_change' AND detail.reason='lock_expired'``
- is exempt from the event-retention sweep at EVERY age
(src/taskq/backend/_sweeps.py:477-478), and ``poll_reclaim_events`` can only
reach ``id > $1`` (the trailing watermark cursor,
src/taskq/backend/_sql_templates.py:1138). Two consequences nothing bounds:

* (a) a fleet with NO watch_reclaims consumer retains every lock_expired
  event forever - unbounded growth (the orphaned-row class);
* (b) an event committed late, below a cursor that already passed it, is
  both unreachable (the cursor cannot go back) and immortal (the exemption
  has no age cap).

The contract: unconsumed outbox rows must be bounded by SOME mechanism - an
age cap beyond the visibility delay, a consumption watermark, or a
no-consumer detector.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_pending_job

pytestmark = pytest.mark.integration

_OUTBOX_DETAIL = '{"from_state": "running", "to_state": "pending", "reason": "lock_expired"}'
_ORDINARY_DETAIL = '{"from_state": "pending", "to_state": "running"}'


class _StubBackendDeps:
    """Minimal duck-typed BackendDeps: settings + pools only."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None


def _pool_backend(schema: str, pool: asyncpg.Pool) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema,
        },
        validate=False,
    )
    deps = _StubBackendDeps(settings)
    deps.worker_pool = pool
    deps.heartbeat_pool = pool
    deps.dispatcher_pool = pool
    return PostgresBackend(
        deps,  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; only settings + pools are read on the paths under test.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


async def _insert_events(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    detail: str,
    n: int,
) -> None:
    """Insert n back-dated state_change events with the given detail."""
    await conn.execute(
        f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail) '
        "SELECT $1, clock_timestamp() - interval '1 hour', 'state_change', $2::jsonb "
        "FROM generate_series(1, $3::int)",
        job_id,
        detail,
        n,
    )


async def _count_outbox(conn: asyncpg.Connection, schema: str) -> int:
    # The sweep's own carve-out predicate, verbatim.
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events '
            "WHERE kind = 'state_change' AND COALESCE(detail->>'reason', '') = 'lock_expired'"
        )
    )


async def _drain_retention(conn: asyncpg.Connection, schema: str) -> int:
    """Run the retention sweep to exhaustion in small batches; return total deleted."""
    deleted = 0
    while True:
        n = await PostgresBackend.sweep_expired_events(
            conn, schema=schema, retention=timedelta(seconds=1), batch_size=3
        )
        deleted += n
        if n == 0:
            return deleted


async def test_unconsumed_outbox_rows_survive_every_retention_batch(pg_dsn: str) -> None:
    """With no watch_reclaims consumer, age-expired unconsumed lock_expired
    events must be bounded by SOME mechanism. Today all of them survive a
    full retention drain (the exemption has no age cap, no watermark, and no
    no-consumer detector) - while the ordinary events next to them are
    deleted, proving the sweep ran."""
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        job_id = await create_pending_job(conn, schema)

        n_outbox, n_ordinary = 6, 2
        await _insert_events(conn, schema, job_id, _OUTBOX_DETAIL, n_outbox)
        await _insert_events(conn, schema, job_id, _ORDINARY_DETAIL, n_ordinary)
        before = await _count_outbox(conn, schema)
        assert before == n_outbox

        deleted = await _drain_retention(conn, schema)
        after = await _count_outbox(conn, schema)

        assert after < n_outbox, (
            "Contract: unconsumed outbox rows must be bounded by SOME mechanism - an age cap "
            "beyond the visibility delay, a consumption watermark, or a no-consumer detector; "
            "a fleet with NO watch_reclaims consumer cannot retain every lock_expired event "
            "forever. Current behavior violates it: the retention sweep deleted only the "
            f"ordinary events (deleted={deleted} of {n_ordinary}) while all {n_outbox} "
            f"age-expired unconsumed outbox rows survived every batch (before={before}, "
            f"after={after}) - the exemption at src/taskq/backend/_sweeps.py:478 has no bound "
            "of any kind."
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_below_cursor_late_event_is_unreachable_and_immortal(pg_dsn: str) -> None:
    """An event committed below a cursor that already passed it must not be
    BOTH undeliverable and undeletable forever. Today it is exactly that: the
    watermark poll cannot see it (``id > $1``) and the retention exemption
    keeps it at every age."""
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        job_id = await create_pending_job(conn, schema)

        # Ordinary events (invisible to the reclaim poll) occupy the low ids.
        await _insert_events(conn, schema, job_id, _ORDINARY_DETAIL, 8)
        # Free one low id: the late writer's transaction will commit into it.
        freed_id = await conn.fetchval(
            f'SELECT id FROM "{schema}".job_events '
            "WHERE kind = 'state_change' AND COALESCE(detail->>'reason', '') != 'lock_expired' "
            "ORDER BY id LIMIT 1"
        )
        assert freed_id is not None
        await conn.execute(f'DELETE FROM "{schema}".job_events WHERE id = $1', freed_id)
        # Outbox events land above every ordinary id.
        await _insert_events(conn, schema, job_id, _OUTBOX_DETAIL, 3)

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        backend = _pool_backend(schema, pool)
        delivered = await backend.poll_reclaim_events(0, limit=10, visibility_delay=timedelta(0))
        assert len(delivered) == 3, f"consumer must reach the 3 outbox events; got {delivered!r}"
        cursor = delivered[-1].event_id
        assert cursor > freed_id, "cursor must sit above the freed low id"

        # The late writer commits a back-dated outbox row below the cursor.
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (id, job_id, occurred_at, kind, detail) '
            "VALUES ($1, $2, clock_timestamp() - interval '1 hour', 'state_change', $3::jsonb)",
            freed_id,
            job_id,
            _OUTBOX_DETAIL,
        )

        # Protocol truth: the trailing watermark cannot re-deliver a below-cursor id.
        late_poll = await backend.poll_reclaim_events(
            cursor, limit=10, visibility_delay=timedelta(0)
        )
        assert late_poll == [], (
            "The trailing watermark cannot re-deliver a below-cursor id "
            "(src/taskq/backend/_sql_templates.py:1138, `id > $1`) - which is exactly why "
            "retention, not the poll, must bound this row."
        )

        deleted = await _drain_retention(conn, schema)
        late_surviving = int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_events WHERE id = $1', freed_id
            )
        )

        assert late_surviving == 0, (
            "Contract: an event the cursor already passed must not be both unreachable and "
            "immortal - some mechanism (an age cap beyond visibility, a backfill poll, or a "
            "detector) must bound it. Current behavior violates it: the late row (id="
            f"{freed_id}, below cursor {cursor}) is undeliverable (poll after cursor returned "
            f"{late_poll!r}) and undeletable (retention drain deleted {deleted} rows, all "
            "ordinary; the lock_expired exemption at src/taskq/backend/_sweeps.py:478 keeps it "
            "at every age) - permanently lost reclaim signal AND permanent storage."
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
