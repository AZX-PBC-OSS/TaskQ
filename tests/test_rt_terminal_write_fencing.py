# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""Red-team pin: a stale attempt's terminal write must not terminate a
re-dispatched attempt on the same worker.

Our terminal writes carry ownership fencing — ``WHERE id = $1 AND
status = 'running' AND locked_by_worker = $2`` — which correctly no-ops
a *different* worker's late write (pinned by
``tests/test_postgres_terminal_writes.py::TestWrongWorkerIdPG``). But
the guard carries no attempt identity, so it cannot distinguish the
stale handler of attempt N from the live handler of attempt N+1 when
the job was reclaimed and re-dispatched **to the same worker**:

1. worker W's event loop stalls past ``lock_lease`` (heartbeat cannot
   renew — PG unreachable, so ``isolate_self`` cannot run either);
2. the leader's Sweep 1 re-pends the job (lock expired);
3. W recovers; its dispatch loop re-dispatches the job — to itself —
   at attempt N+1;
4. the stale handler from attempt N, suspended all along, resumes and
   calls ``mark_succeeded`` — the guard matches, and attempt N+1 is
   falsely terminalised with attempt N's result.

Fence this with an attempt-identity epoch on every terminal write, so
the guard matches ``attempted_at == job.attempted_at`` and a rescued-
and-refetched job makes the old worker's terminal call a silent no-op.
This pin holds the same contract: after a same-worker re-dispatch, a
write carrying the stale attempt's context must not apply — the job
stays running at its new attempt. Red today because the terminal-write
API cannot express attempt identity at all, which is the finding.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)


async def test_stale_attempt_write_does_not_apply_after_same_worker_redispatch(
    clean_jobs_app: JobsApp,
) -> None:
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn:
        worker_id = new_uuid()
        await create_worker(conn, schema, worker_id)
        # Attempt 1, running under W, lock already expired — the
        # stalled-worker state Sweep 1 exists to reclaim.
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
            max_attempts=3,
            retry_kind="transient",
        )

        count = await PostgresBackend.sweep_expired_locks(
            conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )
        assert count >= 1, "fixture broken: the expired lock was not reclaimed"

        status = await conn.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
        assert status == "pending", "fixture broken: the sweep did not re-pend the job"

        # Make the re-pended job due now — the sweep's pending branch adds
        # its retry backoff; resetting it is setup plumbing for the
        # re-dispatch, not part of the contract under test.
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() '
            f"- interval '1 second' WHERE id = $1",
            job_id,
        )

    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) == 1 and dispatched[0].id == job_id, (
        "fixture broken: the re-pended job was not re-dispatched"
    )

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, attempt, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert row is not None
        assert row["status"] == "running", "fixture broken: not running after re-dispatch"
        assert row["attempt"] == 2, f"fixture broken: expected attempt 2, got {row['attempt']}"
        assert row["locked_by_worker"] == worker_id

    # The stale attempt-1 handler's write, arriving late: suspended
    # through the stall, the sweep, and the re-dispatch, it now calls
    # the terminal API — indistinguishable from attempt 2's own write.
    # It must not apply.
    applied = await backend.mark_succeeded(job_id, worker_id, None)

    assert applied is False, (
        "a terminal write carrying the stale attempt's context was applied to "
        "attempt 2 of the same job on the same worker — the guard "
        "(id, status, locked_by_worker) cannot distinguish attempts, so a "
        "reclaimed-and-redispatched job is falsely terminalised with the old "
        "attempt's result. Fence this by matching attempted_at == job.attempted_at "
        "on every terminal write, so the old attempt's call becomes a silent no-op."
    )

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None
        assert row["status"] == "running", (
            "the re-dispatched attempt must still be running — the stale write must be a no-op"
        )
        assert row["attempt"] == 2
