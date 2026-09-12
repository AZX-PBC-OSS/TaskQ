"""Red-team attack on duration_ms at DRAIN granularity.

The bounded sweeps compute ``job_attempts.duration_ms`` from the driving
UPDATE's own ``RETURNING ... clock_timestamp() AS now_ts`` — per row, per
BATCH.  The existing pins (``test_sweep_expired_locks_bounded.py``'s
attempt-shape test) bound the value inside ONE batch.  This attack
drains a backlog across MULTIPLE committed batches with wall-clock
separation between them and asserts each batch's rows were measured
against THAT batch's statement clock, not the first batch's:

* rows reclaimed in later batches must show strictly later durations
  (the separation between batches shows up in duration_ms);
* the earliest batch's rows must still measure ~the seeded elapsed,
  proving the clock is the statement's, not a stale transaction-start
  or first-batch hoist (a ``now()`` regression pins every row to one
  instant; a hoisted first-batch clock pins every row to the FIRST
  batch's instant — both fail the spread assertion below).
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)

_JOBS = 9
_BATCH = 3
_STARTED_SECONDS_AGO = 30.0
# Separation between drain calls: two gaps of 1.1 s put the third batch's
# clock >= 2.2 s after the first's — far above ladder/round-trip noise,
# far below anything the 1750 ms statement_timeout could abort.
_GAP_SECONDS = 1.1


async def _seed_expired_locks(conn: asyncpg.Connection, schema: str, count: int) -> list[UUID]:
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
        "clock_timestamp() - interval '10 seconds', "
        f"clock_timestamp() - interval '{_STARTED_SECONDS_AGO} seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
        worker_id,
    )
    return job_ids


async def test_each_batchs_rows_are_measured_against_that_batchs_clock(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A multi-batch drain must measure each batch at its own instant.

    Three committed batches with 1.1 s of separation between them: if
    every row were measured against one instant (transaction-start
    ``now()``, or a first-batch clock hoisted across the drain), all
    durations would agree within microseconds and the spread assertion
    fails.
    """
    schema = module_pg_schema.schema_name
    await _seed_expired_locks(clean_pg_conn, schema, _JOBS)

    counts: list[int] = []
    for call in range(_JOBS // _BATCH + 1):
        n = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
            batch_size=_BATCH,
        )
        counts.append(n)
        if n == 0:
            break
        assert n == _BATCH, f"call {call} reclaimed {n}, expected the full batch"
        # Wall-clock separation between committed batches: everything in
        # the NEXT batch is measured strictly later than this one's rows.
        await clean_pg_conn.execute(f"SELECT pg_sleep({_GAP_SECONDS})")

    assert counts == [_BATCH, _BATCH, _BATCH, 0], (
        f"the drain must take exactly {_JOBS // _BATCH} full batches then terminate; saw {counts}"
    )

    # finished_at carries the per-batch statement clock (plus the
    # in-batch microsecond ladder), so ordering by it is ordering by
    # batch: rows 0-2 batch 1, 3-5 batch 2, 6-8 batch 3.
    attempts = await clean_pg_conn.fetch(
        f'SELECT duration_ms, finished_at FROM "{schema}".job_attempts '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "ORDER BY finished_at"
    )
    assert len(attempts) == _JOBS

    durations: list[int] = []
    for row in attempts:
        d = row["duration_ms"]
        assert d is not None, "started_at is seeded on every row — duration_ms must be computed"
        durations.append(d)
    seeded_ms = int(_STARTED_SECONDS_AGO * 1000)
    for d in durations:
        assert seeded_ms - 5_000 <= d <= seeded_ms + 90_000, (
            f"duration_ms {d} is not a sane database-clock span for a job "
            f"started {_STARTED_SECONDS_AGO} s ago"
        )

    # The load-bearing assertion, at batch granularity: the LAST batch's
    # rows were measured at least (batches - 1) * GAP later than the
    # FIRST batch's rows.  Per-row comparison inside a batch is not a
    # stable property — both started_at (seeded per row) and now_ts (the
    # driving statement's per-row clock_timestamp()) jitter by
    # microseconds-to-milliseconds under load, which the 2.2 s batch
    # separation dwarfs.
    min_spread_ms = (_JOBS // _BATCH - 1) * int(_GAP_SECONDS * 1000) - 100
    first_batch_durations = durations[:_BATCH]
    last_batch_durations = durations[-_BATCH:]
    separation = min(last_batch_durations) - max(first_batch_durations)
    assert separation >= min_spread_ms, (
        f"the last batch's durations sit only {separation} ms above the first "
        f"batch's (expected >= {min_spread_ms} from {(_JOBS // _BATCH - 1)} gaps of "
        f"{_GAP_SECONDS} s) — later batches' rows were measured against an "
        "earlier batch's clock, not each batch's own statement clock"
    )
