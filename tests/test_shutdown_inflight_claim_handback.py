"""SIGTERM must not leave rows claimed by an in-flight dispatch round behind.

The DRAINING phase sets the producer's stop flag and then issues one
hand-back statement that re-pends every row this worker holds. The producer
only observes that flag between rounds: a round already inside its claim
keeps going, commits, and buffers rows that the single hand-back pass has
already looked past.

Those rows are locked to a process that is on its way out. They are not in
the in-flight registry, so the CANCELLING, FORCING and RELEASING phases
never see them either, and the orchestration reports a clean exit with them
still marked running. For a Kubernetes rolling deploy that is up to one
concurrency window of work per pod that stops moving the moment the pod goes
away, invisible in the queue's own accounting until a lock lease expires far
later — and the reclaim that eventually frees it counts the wait as a
crashed attempt against a job that never ran.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import producer_loop
from taskq.worker.shutdown import ShutdownPhase, orchestrate_shutdown

pytestmark = pytest.mark.integration

_BACKLOG = 20


async def _seed_backlog(backend: PostgresBackend) -> None:
    for _ in range(_BACKLOG):
        await backend.enqueue(
            EnqueueArgs(
                id=JobId(new_uuid()),
                actor="test_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )


async def _rows_locked_by(deps: WorkerDeps, schema: str, worker_id: object) -> list[JobId]:
    async with deps.worker_pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT id FROM \"{schema}\".jobs WHERE status = 'running' AND locked_by_worker = $1",  # noqa: S608  # Why: schema is a fixture-owned identifier; the worker id is $-bound.
            worker_id,
        )
    return [JobId(row["id"]) for row in rows]


async def test_sigterm_during_a_claim_round_leaves_no_job_locked_to_the_dead_pod(
    clean_jobs_app: JobsApp,
) -> None:
    """A shutdown that begins mid-claim must still release everything it holds.

    The worker's producer is running against a backlog deeper than one claim
    round, exactly as a pod does under load. SIGTERM arrives once the
    producer has entered a round: the single yield below hands control to the
    producer, which is then parked inside its claim when the orchestration
    begins.

    When the orchestration reports its clean exit, every phase has run and
    the worker is gone. Nothing may still be marked running and locked to it.
    Such a row has no owner alive to finish it, no phase left to release it,
    and nothing to tell the operator it is stuck.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    await _seed_backlog(backend)

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=5)
    shutdown_event = asyncio.Event()
    deps.producer_stop_event = asyncio.Event()
    deps.shutdown_phase = ShutdownPhase.NONE
    deps.shutdown_started_at = None

    producer = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown_event,
            deps.producer_stop_event,
            backend=backend,
            worker_id=worker_id,
        )
    )
    # One yield, not a timed wait: it hands the loop to the producer, which
    # runs until it parks on its claim. The signal then lands with that round
    # in flight — the state a pod is in when a deploy rolls it under load.
    await asyncio.sleep(0)

    exit_code = await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )
    await producer

    assert exit_code == 0, (
        f"the orchestration must report a clean shutdown; got exit code {exit_code}"
    )
    assert shutdown_event.is_set(), "every phase must have completed before this assertion"

    stranded = await _rows_locked_by(deps, schema, worker_id)
    assert stranded == [], (
        "the pod is gone and every shutdown phase has run, yet "
        f"{len(stranded)} job(s) are still marked running and locked to it. "
        "A claim round that was in flight when the signal arrived commits "
        "after the one hand-back pass and is never released by any later "
        "phase, so the work stops dead until a lock lease expires — a rolling "
        "deploy quietly parks a concurrency window of jobs per pod"
    )


async def test_work_claimed_during_shutdown_returns_to_the_fleet(
    clean_jobs_app: JobsApp,
) -> None:
    """Whatever the draining pod claimed must be runnable by a surviving pod.

    The operator's contract for a rolling deploy is that work moves to the
    pods that are staying up. This asserts it from the surviving pod's side:
    after the first worker has shut down completely, a second worker polling
    the same queue must be able to claim the entire backlog. Anything the
    first worker still holds is work the fleet cannot reach.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    await _seed_backlog(backend)

    draining = new_uuid()
    surviving = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, draining)
        await create_worker(conn, schema, surviving)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=5)
    shutdown_event = asyncio.Event()
    deps.producer_stop_event = asyncio.Event()
    deps.shutdown_phase = ShutdownPhase.NONE
    deps.shutdown_started_at = None

    producer = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown_event,
            deps.producer_stop_event,
            backend=backend,
            worker_id=draining,
        )
    )
    await asyncio.sleep(0)
    await orchestrate_shutdown(deps, deps.settings, draining, shutdown_event, None, backend=backend)
    await producer

    claimed: set[JobId] = set()
    while True:
        batch = await backend.dispatch_batch(
            surviving, ["default"], _BACKLOG, timedelta(seconds=60)
        )
        if not batch:
            break
        claimed.update(row.id for row in batch)

    assert len(claimed) == _BACKLOG, (
        "every job the departing pod was holding must come back to the fleet "
        f"when it shuts down; the surviving worker could only claim "
        f"{len(claimed)} of {_BACKLOG} jobs. The rest are still locked to a "
        "pod that no longer exists, so that work simply stops until a lease "
        "expires"
    )
