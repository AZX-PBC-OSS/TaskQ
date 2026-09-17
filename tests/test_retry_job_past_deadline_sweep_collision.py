# ruff: noqa: S608  # Why: schema is the fixture's validated identifier throughout; every value is $-bound.

"""PIN: an admin-retried job whose preserved ``schedule_to_close`` later
elapses while it still waits to dispatch must leave the row resolvable by
the deadline sweep, not wedge the whole sweep batch on a spent
``job_attempts`` key.

``retry_job`` (``_sql_templates.py``) deliberately keeps ``attempt``
monotonic on a normal re-dispatch cycle (never reset — see
``test_retry_job_attempt_epoch_pk.py``): a fresh dispatch climbs the
counter past the spent value, so the next terminal write lands on a
fresh ``(job_id, attempt)`` key. That contract depends on the row
actually getting dispatched again.

``retry_job`` clears a ``schedule_to_close`` that has already elapsed at
retry time (the retried row must be genuinely dispatchable — pinned by
``test_retry_job_stale_deadline_operator_footgun.py``), but it preserves
a still-future deadline: the operator's original budget intent survives
an in-window retry. Dispatch excludes any row whose ``schedule_to_close``
has elapsed, so if that preserved deadline passes while the re-pended row
is still waiting, the row can never climb to a fresh attempt number — it
sits ``pending`` at its *spent* attempt. ``sweep_deadline_exceeded``
(``_sweeps.py``) then claims that row (``status IN ('pending','scheduled')
AND schedule_to_close < now``) and inserts its batched ``job_attempts``
row keyed at the job's current attempt — the same key the original failed
run already wrote. The whole sweep batch's ``job_attempts`` INSERT is one
statement over every swept row in that call, so without the
``ON CONFLICT (job_id, attempt) DO NOTHING`` guard this single collision
rolls back every row the sweep claimed in that call, not just the
retried one.

Operator impact if the guard regresses: the deadline sweep raises
``asyncpg.exceptions.UniqueViolationError`` on ``job_attempts_pkey``
every tick it encounters the wedged row. ``UniqueViolationError`` is not
one of the leader loop's ``TRANSIENT_PG_ERRORS``
(``taskq/worker/_transient.py``), so it is not the "log and retry next
tick" case — it is treated as an unexpected loop error. The admin UI's
retry endpoint returns success and redirects as if the retry worked;
the operator sees nothing wrong until the leader's unexpected-error
telemetry fires.

Distinct from the footgun pin: that file pins that a retry after the
deadline has already passed produces a row dispatch can actually claim
(the stale deadline is cleared). This file pins that a retry *inside*
the deadline window — where the deadline is deliberately preserved —
still cannot wedge the sweep when the window later closes before the
re-run dispatches.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.backend._sweeps import sweep_deadline_exceeded
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration


async def test_retry_past_deadline_does_not_wedge_the_deadline_sweep(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A job retried inside its deadline window whose preserved
    ``schedule_to_close`` then elapses before the re-run dispatches must
    not make ``sweep_deadline_exceeded`` raise a primary-key collision,
    and the retried row must eventually be resolved to a terminal state
    by the sweep rather than sitting wedged forever."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()

    error = ErrorInfo(error_class="TransientError", error_message="boom", error_traceback=None)

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the JobsApp shim, as throughout the suite.
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            max_attempts=1,
            attempt=1,
            lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            schedule_to_close=datetime.now(UTC) + timedelta(minutes=5),
            with_events=False,
        )

        # Epoch 1: the terminal write lands the attempt row at (job, 1).
        assert await backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=1)
        failed = await conn.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert failed is not None and failed["status"] == "failed" and failed["attempt"] == 1

    # The operator retries it through the escape hatch while the deadline
    # is still in the window — retry_job preserves a still-future
    # schedule_to_close (the operator's original budget intent); only an
    # already-elapsed one is cleared.
    assert await backend.retry_job(job_id)

    async with deps.worker_pool.acquire() as conn:
        repended = await conn.fetchrow(
            f'SELECT status, attempt, schedule_to_close FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert repended is not None and repended["status"] == "pending"
        assert repended["schedule_to_close"] is not None, (
            "a retry inside the deadline window must preserve the "
            "operator's original schedule_to_close — clearing a "
            "still-future deadline would silently extend a budget the "
            "operator set"
        )

        # The preserved deadline elapses while the re-pended row is still
        # waiting to be dispatched (no worker has capacity yet). The row
        # now sits pending at its spent attempt with an elapsed deadline —
        # the collision shape this file pins.
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET schedule_to_close = now() - interval '1 second' "
            "WHERE id = $1",
            job_id,
        )

        # The deadline sweep must be able to process this row (whatever
        # its final disposition) without the batch's job_attempts INSERT
        # colliding on the spent (job_id, 1) key. A raised
        # UniqueViolationError here means every row the sweep claimed in
        # this call was rolled back, not just this one.
        try:
            await sweep_deadline_exceeded(conn, schema=schema)
        except Exception as exc:
            pytest.fail(
                "sweep_deadline_exceeded raised on a retried past-deadline "
                f"row instead of resolving it: {exc!r}. A retried job whose "
                "preserved schedule_to_close elapsed before the re-run "
                "dispatched keeps its spent attempt number, so the sweep's "
                "batched job_attempts INSERT revisits the already-written "
                "(job_id, attempt) key and the whole sweep batch rolls back."
            )

        final = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
        assert final is not None
        assert final["status"] != "pending", (
            "the retried row must not be left permanently pending with an "
            "elapsed schedule_to_close and unable to dispatch (dispatch "
            "excludes rows whose deadline has already passed) — it must be "
            f"resolved to a terminal state by the sweep. observed: {final['status']!r}"
        )


async def test_retry_collision_does_not_wedge_sibling_rows_in_same_sweep_batch(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A collided ``job_attempts`` key from one retried-past-deadline row must
    not roll back the whole batched sweep transaction and strand every other
    overdue row the same call claimed.

    The sweep's driving UPDATE and its batched ``job_attempts``/``job_events``
    INSERTs all run inside one transaction (``sweep_deadline_exceeded``,
    ``_sweeps.py``). Before ``ON CONFLICT (job_id, attempt) DO NOTHING`` was
    added to the batched attempt INSERT, a single colliding key inside that
    one statement raised ``UniqueViolationError`` and rolled back every row
    the sweep had snapshotted in that call -- not just the colliding one.
    This pins that a collision on one row in a multi-row batch still lets
    every sibling row in the same call reach 'failed'.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()

    error = ErrorInfo(error_class="TransientError", error_message="boom", error_traceback=None)

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the JobsApp shim, as throughout the suite.
        await create_worker(conn, schema, worker_id)

        # Row A: failed at attempt 1, retried inside its deadline window
        # (the preserved deadline), then stranded past that deadline before
        # the re-run could dispatch -- collides on (job_id, attempt=1) the
        # way the single-row pin above proves.
        collide_job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            max_attempts=1,
            attempt=1,
            lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            schedule_to_close=datetime.now(UTC) + timedelta(minutes=5),
            with_events=False,
        )
        assert await backend.mark_failed_or_retry(collide_job_id, worker_id, error, None, attempt=1)

        # Row B: an ordinary never-dispatched job whose deadline has simply
        # passed -- no retry, no prior attempt row, nothing to collide with.
        # It must still resolve to 'failed' in the same sweep call.
        clean_job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            max_attempts=3,
            attempt=0,
            lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            schedule_to_close=datetime.now(UTC) - timedelta(seconds=1),
            with_events=False,
        )
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'pending' WHERE id = $1",
            clean_job_id,
        )

    # The operator retries row A while its deadline is still in the window
    # (preserved), then the window closes before the re-run dispatches.
    assert await backend.retry_job(collide_job_id)

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET schedule_to_close = now() - interval '1 second' "
            "WHERE id = $1",
            collide_job_id,
        )
        # Both rows are eligible for the same sweep call: sweep_deadline_exceeded
        # snapshots in schedule_to_close order with no per-row isolation, so a
        # generous batch_size claims both in one transaction.
        await sweep_deadline_exceeded(conn, schema=schema, batch_size=100)

        rows = await conn.fetch(
            f'SELECT id, status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
            [collide_job_id, clean_job_id],
        )
        statuses = {r["id"]: r["status"] for r in rows}

        assert statuses[clean_job_id] == "failed", (
            "a sibling row with no colliding attempt key must still be "
            f"resolved by the same sweep call. observed: {statuses[clean_job_id]!r}"
        )
        assert statuses[collide_job_id] != "pending", (
            "the colliding row itself must not be left permanently pending "
            f"either. observed: {statuses[collide_job_id]!r}"
        )
