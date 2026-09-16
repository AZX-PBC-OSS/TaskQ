# ruff: noqa: S608  # Why: schema is the fixture's validated identifier throughout; every value is $-bound.

"""PIN: an admin ``retry_job`` on a job whose ``schedule_to_close`` has
already elapsed must produce a job that can actually be re-executed —
not a row that silently re-fails via the deadline sweep on its very next
tick while the operator-facing surface reports success.

Background — what an operator expects. Sidekiq's ``SortedEntry#retry``
(``vendor/sidekiq/lib/sidekiq/api.rb:644-649``) re-pushes the job as a
fresh ``Client.push`` with a decremented ``retry_count`` — a genuinely
new unit of work with a fresh schedule. Oban's ``retry_job``
(``vendor/oban/lib/oban/engines/lite.ex:296-309``) keeps the same row
and the same monotonic ``attempt``/bumped ``max_attempts`` shape TaskQ
uses (``fragment("MAX(?, ? + 1)", j.max_attempts, j.attempt)`` mirrors
TaskQ's ``LEAST(GREATEST(max_attempts, attempt + 1), 32767)`` in
``_sql_templates.py``'s ``retry_job`` template) — but Oban's jobs have
no absolute-deadline column analogous to ``schedule_to_close``, so
Oban's shape cannot go stale the way TaskQ's can. Both vendors agree on
one thing neither of TaskQ's current behaviours honours: a manual retry
must actually be capable of running again. Whichever shape TaskQ keeps,
"the operator clicked Retry and it says success" must mean the job can
execute — not silently re-fail on the very next sweep tick with no
distinguishing signal.

Verified live against a real Postgres 16 container by hand before this
test was written: enqueuing an indefinite-retry actor with
``time_budget=2s``, letting it exhaust to ``failed`` / ``DeadlineExceeded``,
then calling ``backend.retry_job()`` on it, produces a row with
``status='pending'`` and ``schedule_to_close`` UNCHANGED — still in the
past. The admin UI's retry endpoint (``docs/guides/admin-ui.md`` — "POST
/admin/jobs/{job_id}/retry" — "Redirects to the job detail page on
success") gives no indication that the retried job cannot actually
dispatch-and-run again before its own stale deadline sweeps it back to
``failed``.

Distinct from ``tests/test_retry_job_past_deadline_sweep_collision.py``:
that file already pins that the deadline sweep does not WEDGE (no
job_attempts primary-key collision, no stuck batch) on this row shape —
and it passes. This file pins a different, still-open contract: that the
retried job is not merely un-wedged but genuinely EXECUTABLE — it must
reach a worker and run the actor body at least once before any deadline
can re-fail it. A retry that never dispatches because its deadline has
already elapsed has not given the operator what "retry" promises on
every vendor's surface, even though nothing crashes.

Fix direction: ``retry_job`` (the ``retry_job`` SQL template in
``taskq/backend/_sql_templates.py``) must clear a ``schedule_to_close``
that has already elapsed — the same way it already clears
``finished_at``, ``result``, and the error fields, on the reasoning that
an admin retry is a fresh run and the prior run's terminal-state
artifacts (deadline included) are stale for it. A `schedule_to_close`
still in the future should be left alone (the operator's original
deadline intent survives an in-window retry); only a deadline that has
already passed — which can never again permit a dispatch — must be
cleared or extended.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration


async def test_retry_job_past_deadline_produces_a_dispatchable_row(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A job retried after its own ``schedule_to_close`` has already
    elapsed must be dispatchable — i.e. actually able to reach a worker
    and execute the actor body — not merely resolvable-without-wedging.

    ``dispatch_batch``'s own WHERE clause excludes any row whose
    ``schedule_to_close`` has passed (this is the documented, correct
    behaviour for ordinary dispatch — see docs/guides/retries.md §6).
    The bug is that ``retry_job`` hands back exactly such a row: status
    'pending', but a `schedule_to_close` already in the past, so this
    freshly 'retried' row is dispatch-excluded from the moment the
    operator clicks Retry. It can only ever be swept straight back to
    'failed' — it is never actually re-run.
    """
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

        assert await backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=1)
        failed = await conn.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert failed is not None and failed["status"] == "failed"

        # The deadline elapses while the job sits failed — an operator
        # investigating an incident routinely takes longer than a tight
        # schedule_to_close window.
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET schedule_to_close = now() - interval '1 second' "
            "WHERE id = $1",
            job_id,
        )

    # The operator clicks Retry (backend.retry_job — the admin UI's
    # /admin/jobs/{job_id}/retry handler calls exactly this).
    assert await backend.retry_job(job_id)

    async with deps.worker_pool.acquire() as conn:
        repended = await conn.fetchrow(
            f'SELECT status, schedule_to_close FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert repended is not None and repended["status"] == "pending"

        # THE ACTUAL CONTRACT: the retried row must be able to reach a
        # worker. dispatch_batch is production dispatch's own claim
        # query — if it refuses this row, the row can never run,
        # regardless of what the admin UI's redirect implied.
        claimed = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=timedelta(minutes=5))

        assert claimed, (
            "an admin-retried job past its own schedule_to_close is never "
            "dispatchable: dispatch_batch's WHERE clause correctly excludes "
            "any row whose deadline has elapsed (this is right for ordinary "
            "jobs), but retry_job leaves the stale deadline in place "
            "instead of clearing it for the fresh run it is granting. "
            "observed schedule_to_close="
            f"{repended['schedule_to_close']!r} (already in the past) — "
            "the operator's Retry click can never actually re-execute this "
            "job's actor body; it can only be swept straight back to "
            "'failed' by the deadline sweep, with no distinguishing "
            "signal from an ordinary successful retry on the admin UI's "
            "redirect-to-success response."
        )
