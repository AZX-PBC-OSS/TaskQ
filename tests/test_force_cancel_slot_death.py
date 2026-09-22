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
from dataclasses import dataclass
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
from taskq.worker.shutdown import ShutdownPhase
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


# ── The router's discriminator under a same-batch shutdown cancel ────────
#
# The loop routes the CancelledError out of the child by who was cancelled:
# its own cancellation or ANY shutdown signal re-raises; only a child-ALONE
# cancellation is absorbed as the job's outcome. The dangerous interleaving
# is a shutdown's cancel and a phase-2 arm's cancel delivered in ONE
# event-loop batch: if the router read only the child's cancellation, the
# shutdown would be eaten as a job outcome, the loop would keep serving
# through teardown, and the worker would hang until a hard kill. These two
# pins hold the re-raise side of the discriminator, in the two ways a
# shutdown cancel can reach the loop's await.


@dataclass
class _LoopHandles:
    """The harness handles a discriminator pin needs to drive its cancel."""

    deps: "_FakeWorkerDeps"
    loop_task: asyncio.Task[Any]
    local_queue: asyncio.Queue[JobRow]
    shutdown_event: asyncio.Event
    scopes: Any


async def _start_hung_first_job(runs: "_ActorRuns") -> tuple[_LoopHandles, Any, Any]:
    """Start a di_consumer_loop on a hung first job with a second queued.

    Returns once the first job is registered mid-flight and its body is
    executing, the exact state a phase-2 escalation arm reads the registry
    at. The caller owns the returned scopes and must exit them.
    """
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
    scopes_cm = BootstrappedScopes(registry)
    scopes = await scopes_cm.__aenter__()
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
    handles = _LoopHandles(
        deps=deps,
        loop_task=loop_task,
        local_queue=local_queue,
        shutdown_event=shutdown_event,
        scopes=scopes_cm,
    )
    return handles, entry, backend


async def test_shutdown_cancel_and_phase2_cancel_in_one_batch_re_raises() -> None:
    """The highest-stakes interleaving: the shutdown orchestrator cancels the
    consumer loop task in the same event-loop batch that a phase-2 arm
    cancels the child. Both cancels are pending when the loop resumes at its
    ``await dispatch_task``. The discriminator must route on the loop's OWN
    cancellation (and the shutdown phase) and RE-RAISE: the loop dies with
    the worker. A router that read only the child's cancellation would
    absorb this as a job outcome and keep serving through teardown - the
    worker never dies during shutdown and hangs until a hard kill."""
    runs = _ActorRuns()
    handles, entry, _backend = await _start_hung_first_job(runs)
    try:
        # Both cancels, one tick: the phase stamp and both cancel() calls run
        # with no await between them, so neither the loop task nor the child
        # can observe a half-delivered state. This is the batch the prompt
        # scenario describes: shutdown initiates exactly as the escalation
        # arm fires.
        entry.cancel_phase = CancelPhase.FORCED
        handles.deps.shutdown_phase = ShutdownPhase.FORCING
        handles.loop_task.cancel()
        entry.task.cancel()

        results = await asyncio.wait_for(
            asyncio.gather(handles.loop_task, return_exceptions=True), timeout=5.0
        )
    finally:
        await handles.scopes.__aexit__(None, None, None)

    # The shutdown cancel won: the loop died by cancellation, it did NOT
    # absorb the batch as a job outcome and keep serving.
    assert isinstance(results[0], asyncio.CancelledError), (
        "a shutdown cancel delivered in the same batch as a phase-2 child "
        "cancel must re-raise out of the loop's await - the loop was "
        f"cancelled with the worker and must die with it; got {results[0]!r}"
    )
    # The absorbed-into-teardown shape: the queued second job must never be
    # dispatched by a loop that is on its way out with the shutdown.
    assert runs.second == 0, (
        "a consumer loop cancelled by the shutdown must not serve another "
        f"row from the local queue; the second job ran {runs.second} times"
    )
    # The child the batch cancelled must still have unwound its registry
    # entry: the attempt task's own finally runs during the child's
    # cancellation, however the router routed the parent's await.
    await _wait_until(lambda: handles.deps.active_jobs.count() == 0)


async def test_shutdown_phase_child_cancel_with_loop_alive_re_raises() -> None:
    """The shutdown orchestrator's FORCING arm cancels ONLY the registered
    attempt task - never the loop task itself. The loop therefore sees a
    CancelledError out of the child with its OWN cancel count at zero: the
    exact shape of an absorbable child-only cancel. The router must catch
    the shutdown through the PHASE flag and re-raise. A router that keyed
    only on ``cancelling()`` would absorb a shutdown-driven child cancel as
    a job outcome and keep claiming rows through teardown."""
    runs = _ActorRuns()
    handles, entry, _backend = await _start_hung_first_job(runs)
    try:
        # The FORCING arm verbatim: stamp the phase, cancel the registered
        # task. The loop task itself is NOT cancelled - the orchestrator has
        # no handle to it, the registry entry is all it holds.
        handles.deps.shutdown_phase = ShutdownPhase.FORCING
        entry.cancel_phase = CancelPhase.FORCED
        entry.task.cancel()

        results = await asyncio.wait_for(
            asyncio.gather(handles.loop_task, return_exceptions=True), timeout=5.0
        )
    finally:
        await handles.scopes.__aexit__(None, None, None)

    assert isinstance(results[0], asyncio.CancelledError), (
        "a shutdown-driven cancel of the registered attempt task must "
        "re-raise out of the loop even when the loop task itself was never "
        f"cancelled - the shutdown phase flag owns this route; got {results[0]!r}"
    )
    assert runs.second == 0, (
        "a loop on its way out with a shutdown must not absorb the "
        f"child's cancellation as a job outcome and serve the next row; "
        f"the second job ran {runs.second} times"
    )
    await _wait_until(lambda: handles.deps.active_jobs.count() == 0)


async def test_own_teardown_cancel_of_the_loop_re_raises() -> None:
    """The third route into the router: the loop task's OWN cancellation with
    no shutdown flag set - a TaskGroup teardown cancelling its siblings after
    one failed. Cancelling the loop while it awaits the child delivers the
    cancel INTO the child first (Task.cancel cancels the awaited future), so
    the loop sees a CancelledError out of the child with every shutdown flag
    clear: the exact shape of an absorbable child-only cancel. The router
    must key on its own ``cancelling()`` count and re-raise. A router that
    absorbed here would keep serving rows inside a TaskGroup that is already
    unwinding - the absorb would also leave the count elevated, misrouting
    the NEXT child-only operator cancel into a loop death."""
    runs = _ActorRuns()
    handles, entry, _backend = await _start_hung_first_job(runs)
    try:
        # The TaskGroup teardown shape: the loop task itself is cancelled,
        # no shutdown flag is set, the escalation arm did not fire. The
        # child's cancellation is a side effect of the loop's own cancel
        # propagating through the awaited future.
        entry.cancel_phase = CancelPhase.FORCED
        handles.loop_task.cancel()

        results = await asyncio.wait_for(
            asyncio.gather(handles.loop_task, return_exceptions=True), timeout=5.0
        )
    finally:
        await handles.scopes.__aexit__(None, None, None)

    assert isinstance(results[0], asyncio.CancelledError), (
        "the loop task's own cancellation must re-raise out of the await "
        f"even with every shutdown flag clear; got {results[0]!r}"
    )
    assert runs.second == 0, (
        "a loop cancelled by its own TaskGroup's teardown must not absorb "
        f"the cancellation as a job outcome and keep serving; the second "
        f"job ran {runs.second} times"
    )
    # The child the teardown cancelled unwinds its registry entry.
    await _wait_until(lambda: handles.deps.active_jobs.count() == 0)
