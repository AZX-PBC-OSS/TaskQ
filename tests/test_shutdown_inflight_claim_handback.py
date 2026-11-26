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
later - and the reclaim that eventually frees it counts the wait as a
crashed attempt against a job that never ran.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.postgres import PostgresBackend
from taskq.obs import setup_logging
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import producer_loop
from taskq.worker.shutdown import ShutdownPhase, orchestrate_shutdown

pytestmark = pytest.mark.integration

_BACKLOG = 20


def _event_payload(record: logging.LogRecord) -> dict[str, object] | None:
    """The structlog event dict a captured record carries, if it has one.

    structlog's stdlib bridge hands the handler the event dict as the
    record's message; once a formatter has rendered it (the production
    JSON/console chain runs ahead of caplog's own handler in some runs) the
    message is the rendered string instead. Both shapes are read so the
    premise below cannot fail on handler ordering alone.
    """
    payload = record.msg
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    try:
        parsed = json.loads(record.getMessage())
    except (TypeError, ValueError):
        return None
    return cast("dict[str, object]", parsed) if isinstance(parsed, dict) else None


def _assert_a_claim_round_actually_happened(caplog: pytest.LogCaptureFixture) -> None:
    """The precondition every hand-back assertion below depends on: the
    producer's dispatch path worked, and a claim round committed rows into
    this worker's hands.

    ``producer_loop`` catches a dispatch error per round, logs
    ``dispatch-batch-error``, and retries on the next tick - deliberate
    resilience to transient faults, and never silent in the fleet: the
    dispatch internals bump the ``taskq.dispatch.failures`` counter and set
    an ERROR span status before the raise (``backend/_dispatch_sql.py``,
    ``backend/_dispatch.py``). But that swallow makes a shutdown test
    vacuous unless the premise is checked: with dispatch erroring every
    round, no row is ever claimed, the exit hand-back has nothing to do,
    ``stranded == []`` is trivially true, and the test passes over a
    producer that did nothing. So assert the observable premise: no
    dispatch error was logged, and a hand-back pass actually re-pended
    claimed rows.

    Why the hand-back record and not the local queue: a claim round still
    in flight when the stop lands is handed straight back by the producer's
    exit pass, so those rows never enter ``local_queue``. The
    ``drain-local-queue-completed`` record with ``rows_re_pended > 0`` is
    the shape that fires whichever pass wins the race (the orchestrator's
    DRAINING pass or the producer's exit pass), so an empty queue cannot
    make the premise vacuous.
    """
    dispatch_errors = [
        record for record in caplog.records if "dispatch-batch-error" in record.getMessage()
    ]
    assert not dispatch_errors, (
        "the producer's dispatch round errored, so the shutdown assertions "
        "below are vacuous - nothing was ever claimed, so nothing could be "
        f"left locked: {[r.getMessage() for r in dispatch_errors]}"
    )
    hand_backs: list[dict[str, object]] = []
    for record in caplog.records:
        payload = _event_payload(record)
        if payload is not None and payload.get("event") == "drain-local-queue-completed":
            hand_backs.append(payload)
    assert hand_backs, (
        "the producer never handed a claimed row back - with a backlog "
        "deeper than one claim round, a working dispatch leaves rows claimed "
        f"to this worker for the hand-back to re-pend; got {hand_backs!r}"
    )
    re_pended = sum(int(payload.get("rows_re_pended", 0) or 0) for payload in hand_backs)
    assert re_pended > 0, (
        "no hand-back pass re-pended a row: the scenario this test pins "
        "(SIGTERM with a claim in hand) never happened, and the stranded-row "
        f"assertion below would be vacuous. Hand-back records: {hand_backs!r}"
    )


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
    clean_jobs_app: JobsApp, caplog: pytest.LogCaptureFixture
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

    # Read the producer's failure signal where an operator reads it.
    # setup_logging is the production configurator and is idempotent, so
    # this pins the routing rather than depending on an earlier test having
    # configured it (the tests/test_dispatch_pg.py precedent).
    setup_logging(level="INFO", log_format="json")
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
    # in flight - the state a pod is in when a deploy rolls it under load.
    await asyncio.sleep(0)

    with caplog.at_level(logging.INFO):
        exit_code = await orchestrate_shutdown(
            deps, deps.settings, worker_id, shutdown_event, None, backend=backend
        )
        await producer

    assert exit_code == 0, (
        f"the orchestration must report a clean shutdown; got exit code {exit_code}"
    )
    assert shutdown_event.is_set(), "every phase must have completed before this assertion"

    _assert_a_claim_round_actually_happened(caplog)

    stranded = await _rows_locked_by(deps, schema, worker_id)
    assert stranded == [], (
        "the pod is gone and every shutdown phase has run, yet "
        f"{len(stranded)} job(s) are still marked running and locked to it. "
        "A claim round that was in flight when the signal arrived commits "
        "after the one hand-back pass and is never released by any later "
        "phase, so the work stops dead until a lock lease expires - a rolling "
        "deploy quietly parks a concurrency window of jobs per pod"
    )


async def test_work_claimed_during_shutdown_returns_to_the_fleet(
    clean_jobs_app: JobsApp, caplog: pytest.LogCaptureFixture
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

    setup_logging(level="INFO", log_format="json")
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
    with caplog.at_level(logging.INFO):
        await orchestrate_shutdown(
            deps, deps.settings, draining, shutdown_event, None, backend=backend
        )
        await producer

    # The premise, asserted before the fleet-side claim below: the departing
    # pod must actually have held work. A producer whose dispatch errored
    # every round hands the assertion an empty backlog to "recover" - the
    # surviving worker then claims all of it trivially, and the test would
    # pass having exercised nothing.
    _assert_a_claim_round_actually_happened(caplog)

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
        "pod that no longer exists, so that work stops until a lease "
        "expires"
    )
