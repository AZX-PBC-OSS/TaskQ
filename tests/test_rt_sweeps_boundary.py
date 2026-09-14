"""Red-team attacks on the bounded-sweep parameter boundary.

The bounded sweeps take ``batch_size`` and ``statement_timeout_ms`` as
typed ints at a public boundary (``PostgresBackend.sweep_*`` staticmethods
and the in-memory twins).  Neither is validated today, and both feed
load-bearing SQL mechanics whose degenerate values are silently wrong:

* ``LIMIT $n`` with ``n = 0`` selects ZERO rows — the sweep returns 0
  forever while a full backlog sits eligible: a silent drain stall that
  looks exactly like "nothing to do" to every caller and metric.
* ``LIMIT $n`` with a negative ``n`` does not mean "unbounded" for a
  bound parameter (that reading holds only for a negative LIMIT
  *literal*) — the server rejects it with SQLSTATE 2201W
  (``InvalidRowCountInLimitClauseError``), a data error the leader's
  transient-error classification deliberately does NOT treat as
  transient, so it burns the unexpected-error budget and can tear the
  sweep loop down.
* ``SET LOCAL statement_timeout = 0`` DISABLES the server-side timeout —
  the safety net that aborts a batch which cannot finish inside the
  ``RECLAIM_EVENT_VISIBILITY_DELAY`` margin — while the call still
  succeeds normally.
* a negative ``statement_timeout`` dies with SQLSTATE 22023
  (``InvalidParameterValueError``), same non-transient class.

The boundary these functions present is typed and public; the contract
is a loud ``ValueError`` at the boundary, before any SQL runs.  The
per-test evidence below drives the real sweeps against real eligible
rows so the current silent behaviours are on record in the run output,
not just in this docstring.

``SweepBatchSizer`` mints the default tier for every backend sweep call,
so its constructor takes the same validation: a ``default_size`` of 0
would put ``LIMIT 0`` into every unlatched call (the same silent stall).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sweeps import SweepBatchSizer
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)


# ── Seeding: one eligible row per sweep kind, in one round trip each ─────


async def _seed_due_scheduled(conn: asyncpg.Connection, schema: str) -> int:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid()],
    )
    return 1


async def _seed_overdue_deadline(conn: asyncpg.Connection, schema: str) -> int:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, "
        " schedule_to_close) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp() - interval '60 seconds', clock_timestamp() - interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid()],
    )
    return 1


async def _seed_expired_lock(conn: asyncpg.Connection, schema: str) -> tuple[int, UUID]:
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, attempt, "
        " scheduled_at, locked_by_worker, lock_expires_at, started_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'running', 3, 'transient', 1, "
        "clock_timestamp(), $2, clock_timestamp() - interval '10 seconds', "
        "clock_timestamp() - interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid()],
        worker_id,
    )
    return 1, worker_id


async def _seed_expired_result(conn: asyncpg.Connection, schema: str) -> int:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, "
        " result, result_expires_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'succeeded', 3, 'transient', "
        "clock_timestamp(), '{\"k\": 1}'::jsonb, clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid()],
    )
    return 1


_SWEEP_CALLS: dict[str, Callable[..., Awaitable[int]]] = {
    "scheduled_to_pending": lambda conn, schema, **kw: PostgresBackend.sweep_scheduled_to_pending(
        conn, schema=schema, **kw
    ),
    "deadline_exceeded": lambda conn, schema, **kw: PostgresBackend.sweep_deadline_exceeded(
        conn, schema=schema, **kw
    ),
    "expired_locks": lambda conn, schema, **kw: PostgresBackend.sweep_expired_locks(
        conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema, **kw
    ),
    "expired_results": lambda conn, schema, **kw: PostgresBackend.sweep_expired_results(
        conn, schema=schema, **kw
    ),
}

_SEEDERS: dict[str, Callable[..., Awaitable[Any]]] = {
    "scheduled_to_pending": _seed_due_scheduled,
    "deadline_exceeded": _seed_overdue_deadline,
    "expired_locks": _seed_expired_lock,
    "expired_results": _seed_expired_result,
}


@pytest.mark.parametrize("sweep_name", sorted(_SWEEP_CALLS))
async def test_batch_size_zero_is_rejected_at_the_boundary(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    sweep_name: str,
) -> None:
    """``batch_size=0`` must raise ValueError, not silently no-op.

    Today the call succeeds and returns 0 while the seeded eligible row
    stays eligible: ``LIMIT 0`` is a legal, rowless query.  A drain loop
    keyed on "0 means done" terminates instantly and the backlog never
    drains, with no error anywhere.
    """
    schema = module_pg_schema.schema_name
    await _SEEDERS[sweep_name](clean_pg_conn, schema)

    with pytest.raises(ValueError, match="batch_size"):
        await _SWEEP_CALLS[sweep_name](clean_pg_conn, schema, batch_size=0)


@pytest.mark.parametrize("sweep_name", sorted(_SWEEP_CALLS))
async def test_negative_batch_size_is_rejected_at_the_boundary(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    sweep_name: str,
) -> None:
    """A negative ``batch_size`` must raise ValueError, not SQLSTATE 2201W.

    Today the bound parameter reaches ``LIMIT $n`` and the server rejects
    it with ``InvalidRowCountInLimitClauseError`` — a data error the
    leader's transient classification deliberately excludes, so it counts
    against the unexpected-error budget instead of being caught at the
    typed boundary it belongs to.
    """
    schema = module_pg_schema.schema_name
    await _SEEDERS[sweep_name](clean_pg_conn, schema)

    with pytest.raises(ValueError, match="batch_size"):
        await _SWEEP_CALLS[sweep_name](clean_pg_conn, schema, batch_size=-1)


@pytest.mark.parametrize(
    "sweep_name",
    ["scheduled_to_pending", "deadline_exceeded", "expired_locks"],
)
@pytest.mark.parametrize("timeout_ms", [0, -1])
async def test_statement_timeout_ms_out_of_range_is_rejected_at_the_boundary(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    sweep_name: str,
    timeout_ms: int,
) -> None:
    """``statement_timeout_ms`` must be >= 1, rejected before any SQL.

    ``0`` disables the server-side timeout — the mechanism that aborts a
    batch which cannot finish inside the RECLAIM_EVENT_VISIBILITY_DELAY
    margin — so today the call silently succeeds with its safety net
    removed.  A negative value dies with SQLSTATE 22023 from the server.
    Both belong at the typed boundary as ValueError.
    """
    schema = module_pg_schema.schema_name
    await _SEEDERS[sweep_name](clean_pg_conn, schema)

    with pytest.raises(ValueError, match="statement_timeout_ms"):
        await _SWEEP_CALLS[sweep_name](clean_pg_conn, schema, statement_timeout_ms=timeout_ms)


# ── SweepBatchSizer constructor takes the same boundary ──────────────────


@pytest.mark.parametrize(
    ("default_size", "divisor", "failure_threshold", "window_secs"),
    [
        (0, 4, 3, 600.0),
        (-5, 4, 3, 600.0),
        (100, 0, 3, 600.0),
        (100, -2, 3, 600.0),
        (100, 4, 0, 600.0),
        (100, 4, -1, 600.0),
        (100, 4, 3, 0.0),
        (100, 4, 3, -1.0),
    ],
)
def test_sizer_constructor_rejects_degenerate_configuration(
    default_size: int,
    divisor: int,
    failure_threshold: int,
    window_secs: float,
) -> None:
    """Degenerate sizer knobs must fail at construction, not at LIMIT time.

    ``default_size=0`` would put ``LIMIT 0`` into every unlatched call
    (the silent drain stall above); ``divisor=0`` would raise ZeroDivisionError
    only once the breaker latches; ``failure_threshold=0`` latches on the
    first success-adjacent accounting; ``window_secs=0`` expires every
    failure instantly, making the latch unreachable.  All are
    configuration bugs and belong at the constructor boundary.
    """
    with pytest.raises(ValueError):
        SweepBatchSizer(default_size, divisor, failure_threshold, window_secs)


# ── Edge drains that must keep working (pinned, expected green) ──────────


async def test_batch_size_one_drains_oldest_eligible_first(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """``batch_size=1`` drains every row in its own committed batch.

    The snap's ORDER BY makes the drain deterministic: with per-row
    distinct ``scheduled_at`` seeds, successive size-1 calls promote in
    ascending ``scheduled_at`` order (oldest-eligible first).  This pins
    both the size-1 edge (the ladder degenerates to ordinal 1 on every
    row) and the documented oldest-first drain determinism.
    """
    schema = module_pg_schema.schema_name
    rows = 6
    job_ids = [new_uuid() for _ in range(rows)]
    await _seed_scheduled_spread(clean_pg_conn, schema, job_ids)

    calls = 0
    for _ in range(rows + 2):
        n = await PostgresBackend.sweep_scheduled_to_pending(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            schema=schema,
            batch_size=1,
        )
        calls += 1
        if n == 0:
            break
        assert n == 1, f"a size-1 call must promote exactly one row, got {n}"

    assert calls == rows + 1, (
        f"a size-1 drain of {rows} rows must take {rows + 1} calls (rows + the "
        f"terminating zero), took {calls}"
    )
    # The drain order is observable through the per-call event rows: each
    # size-1 call writes exactly one state_change event, bigserial-ordered.
    promoted_order = [
        rec["job_id"]
        for rec in await clean_pg_conn.fetch(
            f'SELECT job_id FROM "{schema}".job_events ORDER BY id',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        )
    ]
    assert len(promoted_order) == rows, "one state_change event per promoted row"
    scheduled_order = [
        rec["id"]
        for rec in await clean_pg_conn.fetch(
            f'SELECT id FROM "{schema}".jobs ORDER BY scheduled_at',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        )
    ]
    assert promoted_order == scheduled_order, (
        "the drain must be deterministic oldest-eligible-first (the snap's ORDER BY scheduled_at)"
    )


async def _seed_scheduled_spread(
    conn: asyncpg.Connection, schema: str, job_ids: list[UUID]
) -> None:
    """Seed scheduled jobs with strictly distinct past due instants.

    ``unnest`` pairs each id with its own back-dated ``scheduled_at``
    (i * 10 s ago, i descending so array order ≠ scheduled_at order — a
    drain that followed insertion order instead of the ORDER BY fails the
    determinism assertion).
    """
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT t.id, 'test_actor', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
        "clock_timestamp() - (t.i * interval '10 seconds') "
        "FROM unnest($1::uuid[], $2::int[]) AS t(id, i)",
        job_ids,
        list(range(len(job_ids), 0, -1)),
    )


async def test_batch_size_above_the_backlog_drains_in_one_call(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A cap larger than the backlog is not an error: one call takes all.

    The cap is a ceiling, not a target — a caller tuning it up must not
    see truncated calls or extra round trips.
    """
    schema = module_pg_schema.schema_name
    await _seed_due_scheduled_bulk(clean_pg_conn, schema, 25)

    n = await PostgresBackend.sweep_scheduled_to_pending(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        batch_size=40,
    )
    assert n == 25, f"an oversized cap must still drain the whole backlog, got {n}"
    again = await PostgresBackend.sweep_scheduled_to_pending(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        batch_size=40,
    )
    assert again == 0, "the backlog is drained; a further call must return 0"


async def test_backlog_exactly_batch_size_terminates(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A backlog of exactly the cap drains in one full batch, then stops.

    The short-batch edge: a caller cannot distinguish "exactly one full
    batch remains" from "more remains" by the return value alone, so the
    drain contract is a subsequent 0 — the loop must terminate, not spin.
    """
    schema = module_pg_schema.schema_name
    await _seed_due_scheduled_bulk(clean_pg_conn, schema, 10)

    first = await PostgresBackend.sweep_scheduled_to_pending(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        batch_size=10,
    )
    assert first == 10, "a cap equal to the backlog takes the whole backlog"
    second = await PostgresBackend.sweep_scheduled_to_pending(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        schema=schema,
        batch_size=10,
    )
    assert second == 0, "the next call must see an empty eligible set"


async def _seed_due_scheduled_bulk(conn: asyncpg.Connection, schema: str, count: int) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT id, 'test_actor', 'default', '{}'::jsonb, 'scheduled', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        [new_uuid() for _ in range(count)],
    )
