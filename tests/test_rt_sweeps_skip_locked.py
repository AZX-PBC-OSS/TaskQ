"""Red-team attacks on SKIP LOCKED semantics and batch atomicity.

The bounded snaps use ``FOR UPDATE SKIP LOCKED`` so a sweep running
against a worker that holds a row lock (an open transaction on one of
the eligible jobs) skips the contended row instead of blocking on it.
The observable contract, attacked here end to end:

* the contended row is SKIPPED — the other eligible rows are still
  transitioned by the same call (skip, not block, not abort);
* the skipped row gains NO attempts/events from the call that skipped
  it (nothing is written for a row the snap never returned);
* once the holder's transaction rolls back, a subsequent call reclaims
  the row with exactly one attempts row and one event — the
  ``(job_id, attempt)`` primary key cannot unique-violate because the
  skipping call never wrote anything for it.

The second attack is the atomicity half: the driving UPDATE, the batched
attempt INSERT and the batched event INSERT share ONE transaction, so a
constraint failure anywhere in the batch must roll back the ENTIRE batch
— no job transitions, no events.  The existing pin
(``test_postgres_sweeps.py``'s ``test_atomicity_event_insert_failure_rolls_back_state``)
simulates the failure in Python by wrapping the connection; this attack
makes the database itself reject a row — a pre-seeded duplicate
``job_attempts`` row unique-violates the batched attempt INSERT after
the driving UPDATE has already transitioned the rows — which is the
stronger, server-level version of the same property.
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
_JOBS = 5


async def _seed_worker_and_expired_locks(
    conn: asyncpg.Connection, schema: str, count: int
) -> tuple[UUID, list[UUID]]:
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
    return worker_id, job_ids


async def _counts(
    conn: asyncpg.Connection, schema: str, job_ids: list[UUID]
) -> tuple[int, int, int]:
    """(still-running, attempts, events) for *job_ids*."""
    running = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "WHERE id = ANY($1::uuid[]) AND status = 'running'",
        job_ids,
    )
    attempts = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        job_ids,
    )
    events = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        job_ids,
    )
    assert running is not None and attempts is not None and events is not None
    return running, attempts, events


async def test_locked_row_is_skipped_then_reclaimed_after_holder_rollback(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """An external row lock must be skipped, not blocked on — and the
    skipped row must still be reclaimable afterwards.

    A worker holding an open transaction on ONE of five eligible jobs is
    the production shape: a dispatcher mid-transaction, or a terminal
    write in flight.  The sweep must make progress on the other four
    without waiting, and the skipped row must come back cleanly on a
    later call once the holder rolls back — including the
    ``(job_id, attempt)`` PK never seeing a duplicate from the skipping
    call.
    """
    schema = module_pg_schema.schema_name
    _worker_id, job_ids = await _seed_worker_and_expired_locks(clean_pg_conn, schema, _JOBS)
    contended = job_ids[0]
    dsn = module_pg_schema.pg_dsn

    holder = await asyncpg.connect(dsn)
    try:
        async with holder.transaction():
            locked = await holder.execute(
                f'UPDATE "{schema}".jobs SET priority = priority '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
                "WHERE id = $1",
                contended,
            )
            assert locked == "UPDATE 1", "the holder must own the row lock"

            count = await PostgresBackend.sweep_expired_locks(
                clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
                batch_size=_JOBS,
            )
            assert count == _JOBS - 1, (
                f"a sweep against one contended row of {_JOBS} must reclaim the "
                f"other {_JOBS - 1}, got {count} — SKIP LOCKED must skip, not "
                "block or abort"
            )

            running, attempts, events = await _counts(clean_pg_conn, schema, job_ids)
            assert running == 1, "only the contended row may still be running"
            assert attempts == _JOBS - 1, "no attempt row for a skipped row"
            assert events == _JOBS - 1, "no event row for a skipped row"
        # Holder rolls back: the contended row is unlocked and untouched.
    finally:
        await holder.close()

    second = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
        schema=schema,
        batch_size=_JOBS,
    )
    assert second == 1, "the previously contended row must now be reclaimable"

    running, attempts, events = await _counts(clean_pg_conn, schema, job_ids)
    assert running == 0
    assert attempts == _JOBS, (
        "after the re-sweep every job has exactly one attempt row — the "
        "skipping call wrote nothing for the contended row, so the "
        "(job_id, attempt) PK cannot have collided"
    )
    assert events == _JOBS, "one event per job across the two calls"


async def test_mid_batch_constraint_failure_rolls_back_the_whole_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A DB-level constraint failure mid-batch must roll back everything.

    Pre-seeding a duplicate ``job_attempts`` row for one eligible job
    makes the BATCHED attempt INSERT unique-violate after the driving
    UPDATE has already transitioned rows — the failure lands between the
    sweep's statements, exactly where a partial write would be possible
    if the statements were not in one transaction.  The whole batch must
    roll back: every job still running, zero events, only the pre-seeded
    attempt row present.  A constraint violation is non-transient in the
    leader's error classification, so a partial write here would also be
    a permanently inconsistent corpus, not a retry away.
    """
    schema = module_pg_schema.schema_name
    _worker_id, job_ids = await _seed_worker_and_expired_locks(clean_pg_conn, schema, 3)
    # The duplicate the sweep's attempt INSERT will collide with: same
    # (job_id, attempt) as the row the sweep is about to write.
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".job_attempts '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(job_id, attempt, started_at, finished_at, outcome, error_class, "
        " error_message, metadata) "
        "VALUES ($1, 1, clock_timestamp(), clock_timestamp(), 'crashed', "
        "'WorkerCrashed', 'pre-seeded duplicate', '{}'::jsonb)",
        job_ids[1],
    )

    with pytest.raises(asyncpg.UniqueViolationError):
        await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )

    running, attempts, events = await _counts(clean_pg_conn, schema, job_ids)
    assert running == 3, (
        f"the whole batch must roll back — {3 - running} job(s) transitioned "
        "despite the batch failing on the attempt INSERT"
    )
    assert attempts == 1, "only the pre-seeded duplicate may exist — no partial writes"
    assert events == 0, "no event rows may survive a failed batch"

    # Removing the stray duplicate restores drainability: the sweep's
    # own writes were fully rolled back, so nothing is half-done.
    await clean_pg_conn.execute(
        f'DELETE FROM "{schema}".job_attempts WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        job_ids[1],
    )
    reclaimed = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
        schema=schema,
    )
    assert reclaimed == 3, "after the obstacle is gone the drain must complete"
    running, attempts, events = await _counts(clean_pg_conn, schema, job_ids)
    assert (running, attempts, events) == (0, 3, 3)
