"""Graceful hand-back must not spend the retry budget of a job that never ran.

A rolling Kubernetes deploy hands each pod a SIGTERM. Phase DRAINING re-pends
every row the pod claimed but never started so a surviving pod can run it.
The claim that put the row in that buffer already incremented ``attempt`` -
that increment is the cost of an execution, and the hand-back is the
statement that no execution happened. The two must cancel, exactly as the
snooze and admission-denial paths refund a claim whose actor never ran.

If they do not, ``attempt`` climbs once per deploy for a job that has never
executed, and the retry budget an operator configured to absorb real
failures is spent absorbing their own deployments instead. The observable
consequences an operator hits: a job's first genuine transient failure is
treated as its last, and a job that has never run at all reaches a terminal
state.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import dispatch_one_job
from taskq.worker.shutdown import drain_local_queue_to_pending
from tests._di_scopes import BootstrappedScopes

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=60)
_MAX_ATTEMPTS = 3
_GRACE = timedelta(seconds=30)


class _Payload(BaseModel):
    """Empty payload; this contract is about counters, not job input."""


def _now() -> datetime:
    return datetime.now(UTC) - timedelta(seconds=1)


async def _enqueue_job(backend: PostgresBackend) -> JobId:
    job_id = JobId(new_uuid())
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=_MAX_ATTEMPTS,
            retry_kind="transient",
            scheduled_at=_now(),
        )
    )
    return job_id


async def _fresh_worker(deps: WorkerDeps, schema: str) -> Any:
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    return worker_id


async def _deploy_cycle(backend: PostgresBackend, deps: WorkerDeps, schema: str) -> int:
    """One rolling-deploy step: a pod claims the backlog, then gets SIGTERM.

    The claim is the production ``dispatch_batch`` statement, so the row
    carries exactly the shape a real pod's local buffer holds, including
    whatever the claim did to ``attempt``. The hand-back is the production
    DRAINING helper. No job is ever started: this pod's consumer never got
    to the row before the signal arrived.
    """
    worker_id = await _fresh_worker(deps, schema)
    await backend.dispatch_batch(worker_id, ["default"], 10, _LOCK_LEASE)
    return await drain_local_queue_to_pending(deps, worker_id)


async def test_a_job_handed_back_by_shutdown_keeps_its_full_retry_budget(
    clean_jobs_app: JobsApp,
) -> None:
    """Rolling deploys that never run a job must leave its attempt count alone.

    The job sits in a pod's local buffer when SIGTERM lands, twice running.
    Each time it is handed back to pending untouched by any actor. An
    operator who deploys twice on a quiet afternoon has not used up two of
    the three attempts they budgeted for genuine failures, and the job's
    recorded attempt history - which is empty, because nothing ran - must
    agree with the attempt counter.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    baseline_attempt = enqueued.attempt

    for _ in range(2):
        handed_back = await _deploy_cycle(backend, deps, schema)
        assert handed_back == 1, (
            "the claimed-but-unstarted row must come back to pending on every "
            f"deploy so a surviving pod can run it; hand-back released {handed_back} rows"
        )

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "pending", (
        "a job that was only ever claimed and handed back must be waiting to "
        f"run, not parked in {row.status!r}"
    )

    async with deps.worker_pool.acquire() as conn:
        recorded = await conn.fetch(
            f'SELECT attempt FROM "{schema}".job_attempts WHERE job_id = $1',  # noqa: S608  # Why: schema is a fixture-owned identifier; the job id is $-bound.
            job_id,
        )
    assert recorded == [], (
        "no attempt ran, so the job must have no recorded attempt history; "
        f"found {len(recorded)} attempt row(s)"
    )

    assert row.attempt == baseline_attempt, (
        "shutdown hand-back must refund the claim's attempt increment the way "
        "every other release of an unexecuted job does - otherwise each "
        "rolling deploy silently spends one of the retries the operator "
        f"budgeted for real failures; attempt went {baseline_attempt} -> "
        f"{row.attempt} with the actor never invoked"
    )


async def test_first_real_failure_after_deploys_still_gets_its_retries(
    clean_jobs_app: JobsApp,
) -> None:
    """A job's first genuine failure must be retried, however many deploys preceded it.

    Same job, same actor, same single transient exception. The only
    difference from an undisturbed queue is that two pods were rolled while
    the job waited in their buffers. An operator would experience this as a
    job that dies on its first error during a deploy window and retries
    normally the rest of the week - a failure mode that looks like the
    actor's fault and is not.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    runs: list[JobId] = []

    async def failing_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        runs.append(ctx.job_id)
        raise ValueError("transient boom")

    actor_ref: Any = ActorRef(
        name="test_actor",
        queue="default",
        fn=failing_actor,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,  # type: ignore[arg-type]  # Why: result_adapter is unused on the failure path under test.
        retry=RetryPolicy(kind="transient", max_attempts=_MAX_ATTEMPTS, jitter=0.0),
        result_ttl=None,
    )

    rolled = await _enqueue_job(backend)
    for _ in range(2):
        assert await _deploy_cycle(backend, deps, schema) == 1, (
            "the job under test must be handed back on each deploy"
        )

    async with _scopes() as scopes:
        await _run_once(backend, deps, schema, scopes, actor_ref, rolled)

        # The control is enqueued only now, so the two jobs differ in exactly
        # one thing: whether a draining pod ever held it in its buffer.
        undisturbed = await _enqueue_job(backend)
        await _run_once(backend, deps, schema, scopes, actor_ref, undisturbed)

    assert runs.count(undisturbed) == 1, "control job must have run exactly once"
    assert runs.count(rolled) == 1, "rolled job must have run exactly once"

    control_row = await backend.get(undisturbed)
    rolled_row = await backend.get(rolled)
    assert control_row is not None
    assert rolled_row is not None

    assert control_row.status not in {"failed", "crashed", "cancelled"}, (
        "a transient failure on attempt one of three must be retried; the "
        f"control job landed in {control_row.status!r}, so the fixture is wrong"
    )
    assert rolled_row.status == control_row.status, (
        "two identical jobs failing identically must reach the same state; "
        "the rolled job differs only in having been handed back by shutdown "
        "without ever running, which must not cost it retries. Control landed "
        f"in {control_row.status!r}, the rolled job in {rolled_row.status!r} - "
        "an operator sees jobs dying on their first error during deploy "
        "windows and nowhere else"
    )


async def test_a_job_that_never_ran_cannot_reach_a_terminal_state(
    clean_jobs_app: JobsApp,
) -> None:
    """Deploys plus one crashed pod must not terminally kill unexecuted work.

    Three rolling deploys leave the job in a pod's buffer each time. A fourth
    pod claims it and is killed outright, so its lease expires and the
    reclaim sweep picks the row up. The actor has still never been invoked
    once, and the job's entire configured budget was spent on deployments.
    A terminal state here is silent data loss: work the operator enqueued is
    gone, and nothing in the record names a failure that explains it.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = await _enqueue_job(backend)
    for _ in range(_MAX_ATTEMPTS):
        assert await _deploy_cycle(backend, deps, schema) == 1

    crashed_worker = await _fresh_worker(deps, schema)
    await backend.dispatch_batch(crashed_worker, ["default"], 10, _LOCK_LEASE)
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET lock_expires_at = clock_timestamp() - interval '10 seconds' WHERE id = $1",  # noqa: S608  # Why: schema is a fixture-owned identifier; the job id is $-bound.
            job_id,
        )
        reclaimed = await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)
    assert reclaimed == 1, (
        "the dead pod's expired lease must be reclaimed so the row is not "
        f"stranded in running with no owner; sweep reclaimed {reclaimed} rows"
    )

    row = await backend.get(job_id)
    assert row is not None

    assert row.status not in {"failed", "crashed", "cancelled"}, (
        "a job whose actor has never been invoked must still be runnable: its "
        "budget was spent entirely on rolling deploys, none of which is an "
        f"execution. The job reached terminal {row.status!r} without the actor "
        "ever being called - work an operator enqueued is gone and nothing in "
        "the record says why"
    )


# ── DI scaffolding ──────────────────────────────────────────────────────


def _scopes() -> BootstrappedScopes:
    """The three DI scopes ``dispatch_one_job`` resolves an actor through."""
    return BootstrappedScopes(ProviderRegistry())


async def _run_once(
    backend: PostgresBackend,
    deps: WorkerDeps,
    schema: str,
    scopes: BootstrappedScopes,
    actor_ref: Any,
    job_id: JobId,
) -> str:
    """Claim *job_id* and run it through the production dispatch composition."""
    worker_id = await _fresh_worker(deps, schema)
    claimed = await backend.dispatch_batch(worker_id, ["default"], 1, _LOCK_LEASE)
    assert [row.id for row in claimed] == [job_id], (
        "the claim must pick up exactly the job under test so the run is "
        f"attributable; claimed {[r.id for r in claimed]}"
    )
    job = claimed[0]
    return await dispatch_one_job(
        backend=backend,
        deps=deps,
        job=job,
        worker_id=worker_id,
        registry=scopes.registry,
        process_scope=scopes.process_scope,
        thread_scope=scopes.thread_scope,
        loop_scope=scopes.loop_scope,
        actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] against the declared BaseModel generics; the runtime contract holds.
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=_MAX_ATTEMPTS, jitter=0.0)
        ),
        clock=SystemClock(),
        enqueuer=SubJobEnqueuer(backend=backend, loop_scope_resolved=None, worker_pool=None),
    )
