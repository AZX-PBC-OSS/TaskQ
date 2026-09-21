"""A phase-2 forced cancel must kill the job, not the slot's consumer loop.

Issue #403: ``consume_one_job`` registers ``asyncio.current_task()`` as the
job's inflight attempt (src/taskq/worker/_consumer.py). ``di_consumer_loop``
awaits ``dispatch_one_job`` inline, so the registered task IS the consumer
loop task, and the cancel ladder's phase-2 escalation
(src/taskq/worker/cancel.py, ``active.task.cancel()``) cancels the whole
loop. ``CancelledError`` is a ``BaseException``; the loop's handlers catch
only ``SlotPoolAcquireError`` and ``Exception``, so the loop task dies, the
TaskGroup discards the cancelled sibling, nothing respawns it, and the slot
never serves another row: the worker strands ``max_concurrency`` claimed rows
and its dispatch capacity is zero for the process's lifetime.

The pinned contract here is the loop's survival: after a phase-2 forced
cancel of a mid-flight job, the SAME loop must pick up the next pending row,
and the registry bookkeeping (the active-jobs entry the producer's
availability subtracts) must come back to zero. The forced cancel is fired
exactly as the cancel ladder's phase-2 arm fires it: the registered entry's
phase stamped FORCED, then ``entry.task.cancel()``.
"""

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import JobRow
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.jobs import make_job_row
from taskq.worker.cancel import ActiveJobRegistry, CancelPhase
from taskq.worker.run import di_consumer_loop
from tests._di_scopes import BootstrappedScopes

pytestmark = pytest.mark.asyncio

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


class _FakeWorkerDeps:
    """The deps surface di_consumer_loop and dispatch_one_job touch, with a
    REAL ActiveJobRegistry: the registration identity and the entry phase the
    forced cancel reads are production objects, not doubles."""

    def __init__(self) -> None:
        self.active_jobs = ActiveJobRegistry()
        self.worker_pool: Any = None
        self.slot_pool: Any = None
        self.slot_pool_connection_init: Any = None
        self.settings = StubActorConfig  # replaced below; kept out of the way
        del self.settings
        from taskq.settings import WorkerSettings

        self.settings = WorkerSettings.load_from_dict(
            {
                "PG_DSN": "postgres://u:p@localhost:5432/db",
                "LOCK_LEASE": 60,
                "HEARTBEAT_INTERVAL": 10,
            }
        )
        self.settings.worker_group = "default"
        self.redis_client: Any = None
        self.progress_buffers: dict[Any, Any] = {}
        self.disowned_jobs: set[Any] = set()
        self.producer_stop_event = asyncio.Event()
        from taskq.worker.shutdown import ShutdownPhase

        self.shutdown_phase = ShutdownPhase.NONE
        self.drain_failures = 0


class _ActorRuns:
    """The two slots' observable behavior: which actor bodies ever ran."""

    def __init__(self) -> None:
        self.first = 0
        self.second = 0


async def _wait_until(predicate: Any) -> None:
    """Poll a condition to truth on the running loop, bounded."""
    async with asyncio.timeout(5.0):
        while not predicate():  # noqa: ASYNC110  # Why: the polled state (a stub's run count, the registry's entry map) has no asyncio.Event; a bounded poll is the seam.
            await asyncio.sleep(0.01)


async def test_phase2_force_cancel_kills_the_job_not_the_slot() -> None:
    """After a phase-2 forced cancel of a hung mid-flight job, the same
    consumer loop must serve the row behind it and the slot bookkeeping must
    return to zero."""
    runs = _ActorRuns()
    first_job = make_job_row(payload={"value": 1})
    second_job = make_job_row(payload={"value": 2})
    job_ids = {"first": first_job.id, "second": second_job.id}

    async def _branching_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        if ctx.job_id == job_ids["first"]:
            runs.first += 1
            # The phase-2 target: never polls ctx.cancel_event, never returns.
            await asyncio.Event().wait()
        else:
            runs.second += 1

    actor_ref = ActorRef(
        name="test_actor",
        queue="default",
        fn=_branching_actor,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,
        retry=RetryPolicy(),
        result_ttl=None,
        rate_limits=[],
        reservations=[],
    )

    backend = FakeBackend()
    deps = _FakeWorkerDeps()
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    local_queue.put_nowait(first_job)
    local_queue.put_nowait(second_job)
    shutdown_event = asyncio.Event()

    from taskq._di.scope import Scope
    from taskq.backend.clock import Clock, SystemClock

    registry = ProviderRegistry()
    # di_consumer_loop requires the ProcessScope to have a cached Clock after
    # bootstrap: the same registration the worker's own bootstrap does.
    registry.register_value(Clock, Scope.PROCESS, SystemClock())
    async with BootstrappedScopes(registry) as scopes:
        loop_task = asyncio.create_task(
            di_consumer_loop(
                deps,  # type: ignore[arg-type]  # Why: the same Any-typed fake-deps seam the depfail family's dispatch drives.
                local_queue,
                shutdown_event,
                backend=as_backend(backend),
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_registry={"test_actor": actor_ref},
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(backend), loop_scope_resolved=None, worker_pool=None
                ),
            )
        )

        # The hung job is registered mid-flight: the registry holds exactly
        # the entry the cancel ladder's phase-2 arm would read.
        await _wait_until(lambda: deps.active_jobs.get(first_job.id) is not None)
        await _wait_until(lambda: runs.first == 1)

        entry = deps.active_jobs.get(first_job.id)
        assert entry is not None
        # The phase-2 forced cancel, verbatim cancel.py's escalation arm:
        # stamp FORCED, then cancel the registered task.
        entry.cancel_phase = CancelPhase.FORCED
        entry.task.cancel()

        # The slot's subsequent behavior: the row behind the force-cancelled
        # one must still be served by this same loop.
        await _wait_until(lambda: runs.second == 1)

        # The active-jobs entry (the count the producer's availability
        # subtracts) must be released once the forced path unwinds.
        await _wait_until(lambda: deps.active_jobs.count() == 0)

        shutdown_event.set()
        results = await asyncio.wait_for(
            asyncio.gather(loop_task, return_exceptions=True), timeout=5.0
        )

    # The loop itself must not have died by cancellation: a CancelledError
    # here is the loop task's own death, the defect.
    assert not isinstance(results[0], asyncio.CancelledError), (
        "the consumer loop died with CancelledError after the phase-2 "
        "forced cancel; the slot never served the next row"
    )
    assert runs.second == 1
    # The force-cancelled job itself terminalises through mark_cancelled.
    assert any(call["job_id"] == first_job.id for call in backend.mark_cancelled_calls)
