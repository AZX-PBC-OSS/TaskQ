"""Red-team attacks on the occurred_at ladder and mixed-writer co-monotonicity.

``job_events.occurred_at`` must be non-decreasing in ``id`` order across
ALL rows — that is the property ``poll_reclaim_events``' trailing
watermark (``RECLAIM_EVENT_VISIBILITY_DELAY``) reads.  The bounded sweeps
write their batch with a microsecond ordinality ladder
(``clock_timestamp() + (ord - 1) * 1 microsecond``, see
``INSERT_EVENTS_DETAIL_BATCH_SQL``), while the OTHER writers in the same
database use a bare per-row ``clock_timestamp()`` (dispatch's
``INSERT_EVENTS_BATCH_SQL``) or the single-row ``INSERT_EVENT_SQL``.
This file attacks the invariant at the global level:

* interleave a bounded sweep DRAIN (three committed batches) with the
  dispatch form and the single-row form ON A DIFFERENT CONNECTION, then
  assert no inversion anywhere in id order;
* pin the ladder's exact shape — within one batch, consecutive ids step
  by exactly one microsecond, which is what proves the ladder (rather
  than a bare volatile stamp, which collapses ~26 rows per microsecond)
  is still there;
* pin the clock-domain agreement inside one reclaim: the job's
  ``finished_at`` (written by the driving UPDATE) must not postdate its
  event's ``occurred_at`` (written one statement later in the same
  transaction) — a regression to transaction-start ``now()`` on either
  side inverts this.

Honest skew analysis for the mixed-writer race this file cannot make
deterministic: the ladder can stamp a row up to ``batch_size - 1``
microseconds AHEAD of real time, so a concurrent writer that INSERTS a
higher-id row inside that window can produce a microsecond-scale
inversion.  That window is four-plus orders of magnitude inside the 2 s
visibility margin, which exists precisely to absorb commit-order skew
between concurrent writers (milliseconds to seconds in practice) — the
ladder's skew is strictly dominated by the skew class the margin already
tolerates, so it is not treated as a defect here; the deterministic
interleave below pins the invariant at the level that cannot flake.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sql import INSERT_EVENT_SQL, INSERT_EVENTS_BATCH_SQL
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)
_BATCH = 15


async def _seed_expired_locks(conn: asyncpg.Connection, schema: str, count: int) -> list[UUID]:
    """Seed *count* reclaim-eligible running jobs held by one worker."""
    worker_id = new_uuid()
    job_ids = [new_uuid() for _ in range(count)]
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, attempt, "
        " scheduled_at, locked_by_worker, lock_expires_at, started_at) "
        "SELECT t.id, 'test_actor', 'default', '{}'::jsonb, 'running', 3, 'transient', "
        "1, clock_timestamp(), $2, "
        "clock_timestamp() - interval '10 seconds', clock_timestamp() - interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
        worker_id,
    )
    return job_ids


async def test_occurred_at_stays_co_monotonic_across_mixed_writers(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Sweep-batch ladder writes and bare clock_timestamp() writers must
    not invert against each other in id order.

    Three committed sweep batches (ladder stamps) interleave with the
    dispatch form (bare per-row clock_timestamp(), no ladder) and the
    single-row form, the latter two on a separate connection, matching
    the production writer mix.  The assertion is the watermark's own
    invariant, evaluated over every event row in id order.
    """
    schema = module_pg_schema.schema_name
    sweep_job_ids = await _seed_expired_locks(clean_pg_conn, schema, 40)
    dispatch_ids = [new_uuid() for _ in range(3)]
    single_id = new_uuid()
    # job_events.job_id FK-references jobs: the interleaved writers' rows
    # need parent jobs rows (plain pending jobs, seeded in one round trip
    # so the seed itself is not the loop defect under test).
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at) "
        "SELECT t.id, 'test_actor', 'default', '{}'::jsonb, 'pending', 3, "
        "'transient', clock_timestamp() "
        "FROM unnest($1::uuid[]) AS t(id)",
        [*dispatch_ids, single_id],
    )

    other = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        # Batch 1: 15 ladder-stamped rows.
        n1 = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
            batch_size=_BATCH,
        )
        assert n1 == _BATCH

        # Dispatch-form writer (no ladder) on another connection.
        await other.execute(
            INSERT_EVENTS_BATCH_SQL.format(schema=schema),
            dispatch_ids,
            "state_change",
            '{"from_state": "pending", "to_state": "running"}',
        )

        # Batch 2: 15 more ladder-stamped rows.
        n2 = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
            batch_size=_BATCH,
        )
        assert n2 == _BATCH

        # Single-row writer on the other connection.
        await other.execute(
            INSERT_EVENT_SQL.format(schema=schema),
            single_id,
            "state_change",
            '{"from_state": "pending", "to_state": "running"}',
        )

        # Batch 3: the 10-row short batch that finishes the drain.
        n3 = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
            batch_size=_BATCH,
        )
        assert n3 == 10
    finally:
        await other.close()

    rows = await clean_pg_conn.fetch(
        f'SELECT id, job_id, occurred_at FROM "{schema}".job_events ORDER BY id',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
    )
    assert len(rows) == 40 + len(dispatch_ids) + 1, "every writer's rows are present"
    sweep_id_set = set(sweep_job_ids)
    sweep_event_rows = [row for row in rows if row["job_id"] in sweep_id_set]
    assert {row["job_id"] for row in sweep_event_rows} == sweep_id_set, (
        "every reclaimed row must carry its own event across the interleaved "
        "drain — the ladder rows are exactly the sweep's own writes"
    )
    assert len(sweep_event_rows) == 40, "exactly one event per reclaimed row"

    inversions = [
        (rows[i - 1]["id"], rows[i]["id"])
        for i in range(1, len(rows))
        if rows[i]["occurred_at"] < rows[i - 1]["occurred_at"]
    ]
    assert not inversions, (
        f"occurred_at inverted against ascending id at {inversions[:3]} — the "
        "poll_reclaim_events trailing watermark assumes the two are co-monotonic "
        "across ALL writers, ladder-stamped and bare alike"
    )


async def test_batch_ladder_keeps_stamps_strictly_increasing_and_inside_real_time(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Within one batch, consecutive ids must step occurred_at strictly
    forward, and no stamp may run ahead of real time past the ladder.

    The ladder's actual guarantee (see ``_sql.py``'s comment on
    ``INSERT_EVENTS_DETAIL_BATCH_SQL``): ``clock_timestamp()`` is VOLATILE,
    so PostgreSQL evaluates it once PER ROW, and each row's stamp is its
    own evaluation instant plus ``(ordinal - 1)`` microseconds — strictly
    increasing by construction (each step is at least the 1 µs ladder
    increment; the per-row evaluation instants can add more when they
    straddle a microsecond boundary), and at most ``batch_size - 1``
    microseconds ahead of real time.

    A bare volatile stamp without the ladder collapses tens of rows onto
    one microsecond (0 µs steps); a transaction-wide ``now()`` collapses
    the whole batch onto one stamp.  Either regression fails the
    strict-increase half.  A ladder whose span ran away (say milliseconds
    per row) would pass strict increase but fail the real-time half,
    measured against a ``clock_timestamp()`` fetched immediately after
    the sweep's commit.
    """
    schema = module_pg_schema.schema_name
    rows_n = 20
    await _seed_expired_locks(clean_pg_conn, schema, rows_n)

    n = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
        schema=schema,
        batch_size=rows_n,
    )
    assert n == rows_n, "the whole corpus must land in one batch"

    events = await clean_pg_conn.fetch(
        f"SELECT id, occurred_at, clock_timestamp() AS real_now "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        f'FROM "{schema}".job_events ORDER BY id'
    )
    assert len(events) == rows_n
    # The ladder's skew budget: (rows - 1) microseconds past real time.
    # real_now is evaluated one round trip AFTER the batch committed, so
    # every stamp must sit at or below it — a deterministic bound because
    # a round trip dwarfs 19 microseconds.
    max_ladder_skew = timedelta(microseconds=rows_n - 1)

    stamps = [row["occurred_at"] for row in events]
    steps = [stamps[i] - stamps[i - 1] for i in range(1, len(stamps))]
    assert all(step >= timedelta(microseconds=1) for step in steps), (
        f"occurred_at must step strictly forward (>= 1 microsecond, the ladder "
        f"increment) per consecutive id; saw steps as small as {min(steps)} — "
        "a bare clock_timestamp() collapse (0 µs) or a now() collapse (0 µs "
        "everywhere) destroys the ordering the watermark reads"
    )
    assert len(set(stamps)) == len(stamps), "stamps must be distinct per row"
    assert stamps[-1] <= events[-1]["real_now"] + max_ladder_skew, (
        f"the last row's stamp ran {stamps[-1] - events[-1]['real_now']} past "
        "real time measured after the commit — the ladder's whole span must "
        "stay inside (batch_size - 1) microseconds of real time"
    )


async def test_written_timestamps_agree_within_one_reclaim(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A reclaim's finished_at must not postdate its own event's occurred_at.

    Both are written inside one sweep transaction — ``finished_at`` by
    the driving UPDATE, ``occurred_at`` by the event INSERT one round
    trip later — and both must be ``clock_timestamp()``-domain so they
    agree with each other and with other writers.  A regression to
    transaction-start ``now()`` on the event side stamps it BEFORE the
    finished_at the same transaction wrote; on the job side it stamps
    finished_at before an earlier-committed event.  Either inversion is
    caught here per row.
    """
    schema = module_pg_schema.schema_name
    await _seed_expired_locks(clean_pg_conn, schema, 3)
    # Force the terminal branch so finished_at is actually stamped.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET max_attempts = 1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
    )

    n = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
        schema=schema,
    )
    assert n == 3

    rows = await clean_pg_conn.fetch(
        f"SELECT j.id, j.finished_at, e.occurred_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        f'FROM "{schema}".jobs j JOIN "{schema}".job_events e ON e.job_id = j.id'
    )
    assert len(rows) == 3
    for row in rows:
        assert row["finished_at"] is not None
        assert row["occurred_at"] >= row["finished_at"], (
            f"job {row['id']}: event occurred_at {row['occurred_at']} predates the "
            f"finished_at {row['finished_at']} written by the same reclaim — one "
            "of the two stamps left the clock_timestamp() domain"
        )
