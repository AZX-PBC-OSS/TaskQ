"""Red-team: DRAINING re-pends local_queue rows but di_consumer_loop keeps
dequeuing them, producing double-execution of actor side effects (#178).

Contract under attack: once DRAINING fires (``deps.producer_stop_event.set()``
at shutdown.py:187, immediately followed by ``drain_local_queue_to_pending``
re-pending every claimed-but-unstarted row back to 'pending' so another
worker can reclaim it), THIS worker's own consumer loop must stop pulling
jobs off ``local_queue`` — otherwise the re-pended row is claimable by a
second worker AND still eligible to run here, via a stale local copy.

Hypothesis (verified against the current tree): ``di_consumer_loop``'s outer
loop guard is ``while not shutdown_event.is_set()`` (src/taskq/worker/run.py,
di_consumer_loop) — it never reads ``deps.producer_stop_event`` or
``deps.shutdown_phase``. ``shutdown_event`` itself is only set at the very
end of the full 4-phase orchestration (shutdown.py:313), after DRAINING,
the entire CANCELLING grace period, FORCING, and ABANDONING have all run.
So a job already sitting in local_queue when DRAINING enters is re-pended in
the DB (claimable by another worker) while this loop happily dispatches its
own stale copy of the same job, because neither ``producer_stop_event`` nor
``shutdown_phase`` gates the dequeue.

This test drives ``di_consumer_loop`` directly (no Postgres): it seeds one
job into ``local_queue``, flips ``producer_stop_event`` (mirroring
shutdown.py:187's DRAINING entry) while leaving ``shutdown_event`` unset
(mirroring shutdown.py:313 firing only after all later phases complete),
and asserts the loop does NOT dispatch that job. Today it does — a fresh
green pin of the current, non-existent guard.
"""

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

import pytest

import taskq.worker.run as run_module
from taskq._di import ProviderRegistry
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import JobRow
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, as_backend
from taskq.testing.jobs import make_job_row
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.run import di_consumer_loop
from taskq.worker.shutdown import ShutdownPhase

_DOUBLE_EXEC_MSG = (
    "CONTRACT (#178): once DRAINING sets deps.producer_stop_event and "
    "re-pends this worker's local_queue rows back to 'pending' (claimable by "
    "another worker), THIS worker's di_consumer_loop must stop dequeuing "
    "them. VIOLATION: di_consumer_loop's outer guard is only "
    "`while not shutdown_event.is_set()` (src/taskq/worker/run.py) — it never "
    "checks deps.producer_stop_event or deps.shutdown_phase, and "
    "shutdown_event is set only after the full 4-phase sequence completes "
    "(shutdown.py:313). The re-pended job was dispatched here too, "
    "confirming a genuine double-execution path, not just a DB race."
)


class _DepsStub:
    """Duck-typed WorkerDeps carrying only what di_consumer_loop reads (plus
    the DRAINING-phase signals under test: producer_stop_event / shutdown_phase,
    set the same way orchestrate_shutdown sets them at DRAINING entry)."""

    def __init__(self) -> None:
        self.active_jobs = ActiveJobRegistry()
        self.settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
                "TASKQ_CANCELLATION_GRACE_PERIOD": "30.0",
                "TASKQ_CLEANUP_GRACE_PERIOD": "10.0",
                "TASKQ_TERMINATION_GRACE_PERIOD": "60.0",
                "TASKQ_LOCK_LEASE": "45.0",
                "TASKQ_HEARTBEAT_INTERVAL": "5.0",
            }
        )
        self.drain_failures = 0
        self.producer_stop_event = asyncio.Event()
        self.shutdown_phase = ShutdownPhase.NONE


class _ScopeStub:
    """Duck-typed DI scope: ``.get(Clock)`` returns a FakeClock."""

    def __init__(self) -> None:
        from taskq.testing.clock import FakeClock

        self._clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

    def get(self, key: object) -> object:
        return self._clock


async def test_di_consumer_loop_stops_dequeuing_once_draining_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job sitting in local_queue when DRAINING fires (producer_stop_event
    set, row re-pended to 'pending' in the real code path) must not also be
    dispatched by this worker's own consumer loop — that is double execution.
    """
    job = make_job_row(actor="rt_drain_stale_actor", status="running")
    fb = FakeBackend()
    backend = as_backend(fb)
    spy_job_ids: list[object] = []

    async def _actor_fn(payload: object, ctx: object) -> None:
        return None

    actor_ref: ActorRef[Any, Any] = ActorRef(
        name="rt_drain_stale_actor",
        queue="default",
        fn=_actor_fn,
        wants_ctx=True,
        dependencies={},
        payload_type=None,  # type: ignore[arg-type]  # Why: dispatch_one_job is monkeypatched below; payload_type is never read.
        result_adapter=None,  # type: ignore[arg-type]  # Why: dispatch_one_job is monkeypatched below; result_adapter is never read.
        retry=RetryPolicy(),
        result_ttl=None,
    )

    async def _spy_dispatch(**kwargs: object) -> str:
        spy_job_ids.append(cast("JobRow", kwargs["job"]).id)
        return "succeeded"

    # Why the spy: it is the execution observable for "this worker ran the
    # stale local_queue copy of a job that DRAINING already re-pended in the
    # DB" — di_consumer_loop calls the run-module global dispatch_one_job.
    monkeypatch.setattr(run_module, "dispatch_one_job", _spy_dispatch)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    shutdown_event = asyncio.Event()
    deps = _DepsStub()

    # Seed the job into local_queue BEFORE DRAINING — this mirrors a job
    # already claimed and sitting in this worker's queue when shutdown
    # begins (the exact row drain_local_queue_to_pending re-pends).
    local_queue.put_nowait(job)

    loop_task = asyncio.create_task(
        di_consumer_loop(
            deps,  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps stub carrying the real DRAINING signals under test.
            local_queue,
            shutdown_event,
            backend=backend,
            worker_id=new_uuid(),
            registry=ProviderRegistry(),
            process_scope=_ScopeStub(),  # type: ignore[arg-type]
            thread_scope=_ScopeStub(),  # type: ignore[arg-type]
            loop_scope=_ScopeStub(),  # type: ignore[arg-type]
            actor_registry={"rt_drain_stale_actor": actor_ref},
            enqueuer=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        )
    )

    # Simulate orchestrate_shutdown's DRAINING entry (shutdown.py:184-188):
    # shutdown_phase -> DRAINING, producer_stop_event.set() — WITHOUT
    # setting shutdown_event, exactly as the real orchestrator does (it only
    # sets shutdown_event at the very end, after CANCELLING/FORCING/
    # ABANDONING have all completed — shutdown.py:313).
    await asyncio.sleep(0)
    deps.shutdown_phase = ShutdownPhase.DRAINING
    deps.producer_stop_event.set()

    # Give the consumer loop a chance to act on (or ignore) the DRAINING
    # signal before we tear the test down. It must NOT dispatch the job.
    for _ in range(5):
        await asyncio.sleep(0)
        if spy_job_ids:
            break

    # Now end the test's event loop cleanly regardless of outcome.
    shutdown_event.set()
    try:
        await asyncio.wait_for(loop_task, timeout=1.0)
    except TimeoutError:
        loop_task.cancel()

    assert spy_job_ids == [], (
        _DOUBLE_EXEC_MSG + f" Evidence: dispatch spy saw {spy_job_ids!r} after "
        "producer_stop_event/shutdown_phase=DRAINING were set with "
        "shutdown_event still unset — the stale local_queue copy of a "
        "DRAINING-re-pended job was dispatched by this worker's own "
        "consumer loop."
    )
