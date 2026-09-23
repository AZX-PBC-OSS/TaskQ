"""Seam-audit pins: shared-seam interleavings the regression review surfaced.

Companion to ``docs/design/seam-audit-matrix.md``. Each test here closes a
MISSING cell in that matrix: an invariant that held in the source but had no
test constructing the neighbor-caller or interleaved state, so a regression
at the seam would not have been caught. Every pin is deterministic (forced
interleave, stubbed clock or captured SQL; no wall-clock sleeps) and green
on the tree it was written against.

Pins and the seams they guard:

- the shutdown hand-back's cancel fence (``drain_local_queue_to_pending``):
  a row with an operator cancel in flight must never re-enter the fleet
  through the deploy path (double-run class);
- the heartbeat's post-tx drain contract on an acquire-failed tick: the
  pending-abandons deque must drain under a full budget even when the tick
  never got a connection (stuck-between-phases class), and a failing drain
  must not displace the tick's own failure (observability class);
- the abandon drain's detector-2 liveness renewal, once per drained entry
  (worker force-exit mid-drain class);
- an async actor's ``SystemExit`` on the non-transactional path is an
  attempt outcome, not worker death (the attempt-boundary capture contract;
  the task-boundary variants are issue 459's seam).
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._watchdog import LoopLiveness
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.shutdown import drain_local_queue_to_pending

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _worker_settings(schema_name: str = "taskq") -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema_name,
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "18.0",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "3",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "2.0",
            # Tier 1 must be able to fire before the terminal tier, and both
            # must sit inside lock_lease (the settings validator's coupling).
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
        },
    )


def _make_deps(
    *,
    heartbeat_pool: object = None,
    liveness: LoopLiveness | None = None,
) -> WorkerDeps:
    deps = WorkerDeps(
        settings=_worker_settings(),
        dispatcher_pool=MagicMock(),  # type: ignore[arg-type] # Why: not used by the paths under test; class stand-in prevents pyright error on WorkerDeps field type.
        heartbeat_pool=heartbeat_pool or MagicMock(),  # type: ignore[arg-type]
        worker_pool=MagicMock(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    if liveness is not None:
        deps.liveness = liveness
    return deps


def _make_task() -> asyncio.Task[object]:
    loop = asyncio.get_running_loop()
    return loop.create_task(asyncio.sleep(9999))


def _make_ctx(job_id: JobId) -> JobContext[BaseModel]:
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    return JobContext(
        job_id=job_id,
        actor="test",
        queue="default",
        attempt=1,
        claim_epoch=0,
        worker_id=new_uuid(),
        payload=EmptyPayload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=backend,
        ),
        log=bind_job_context(
            structlog.get_logger("taskq.seam-audit"),
            job_id=job_id,
            actor="test",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


# ── Seam: the shutdown hand-back's cancel fence ────────────────────────────


class _CaptureConn:
    """Records the statements the drain issues, no database behind it."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 0"


class _CapturePool:
    """Hands out one :class:`_CaptureConn`, mirroring asyncpg's acquire CM."""

    def __init__(self) -> None:
        self.conn = _CaptureConn()

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_CaptureConn, None]:  # noqa: ASYNC109 # Why: asyncpg.Pool.acquire signature takes `timeout`; the stand-in mirrors it.
        yield self.conn


async def test_drain_handback_keeps_the_cancel_fence_in_both_statement_shapes() -> None:
    """The DRAINING hand-back must never re-pend a row with a cancel in flight.

    The hand-back UPDATE clears the lock of every ``running`` row this
    worker owns that no live consumer holds. A row carrying an operator
    cancel (``cancel_phase != 0``) is exactly the row the cancel ladder
    still owns: re-pending it publishes the lock-clear to the fleet, the
    next holder claims and runs the body again before any ladder arm can
    terminalise it, a double execution of a job the operator asked to
    stop. The fence is the ``cancel_phase = 0`` conjunct, and it must hold
    in BOTH statement shapes the helper issues: the empty-registry
    single-parameter shape (the common drained-worker case) and the
    exclusion shape that binds the held ids. A regression that drops the
    conjunct from either shape re-opens the double-run.
    """
    for active_ids in ([], [JobId(new_uuid())]):
        deps = _make_deps()
        for jid in active_ids:
            await deps.active_jobs.register(
                jid,
                cast("asyncio.Task[object]", MagicMock()),
                cast("JobContext[Any]", MagicMock()),
            )
        pool = _CapturePool()
        deps.dispatcher_pool = pool  # type: ignore[assignment] # Why: the drain reads only the dispatcher role; the capture pool is the seam.
        rowcount = await drain_local_queue_to_pending(deps, new_uuid())
        assert rowcount == 0
        ((sql, _args),) = pool.conn.execute_calls
        assert "cancel_phase = 0" in sql, (
            "the hand-back UPDATE lost the cancel fence (cancel_phase = 0): a "
            "row with an operator cancel in flight would be re-pended to the "
            f"fleet and run a second time. Statement was: {sql!r} "
            f"(exclusion shape: {bool(active_ids)})"
        )


# ── Seam: the heartbeat tick's post-tx drain contract ──────────────────────


class _AcquireFailPool:
    """A pool whose acquire always times out: the worker never gets a conn."""

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[None, None]:  # noqa: ASYNC109 # Why: asyncpg.Pool.acquire signature takes `timeout`; the stand-in mirrors it.
        raise TimeoutError("pool exhausted")
        yield None  # pragma: no cover # Why: the raise above is the seam; the yield only satisfies the generator type.


class _PostTxRecorder:
    """CancelController stand-in recording run_post_tx under the loop."""

    def __init__(self, *, raise_from_post_tx: bool = False) -> None:
        self.post_tx_calls = 0
        self._raise = raise_from_post_tx

    async def run_in_tx(self, conn: object) -> None:
        raise AssertionError("an acquire-failed tick must never open a transaction")

    async def run_post_tx(self) -> None:
        self.post_tx_calls += 1
        if self._raise:
            raise RuntimeError("post-tx drain failed")


async def _run_one_tick(deps: WorkerDeps, controller: object) -> None:
    """Run heartbeat_loop until its first tick settles, then stop it.

    Synchronises on the tick-duration histogram's record hook, which fires
    on BOTH the success and the failure arm of the tick, so the wait is
    deterministic however the tick ends.
    """
    import taskq.worker.heartbeat as hb_mod

    shutdown = asyncio.Event()
    tick_done = asyncio.Event()
    prev_record = hb_mod._tick_duration.record  # pyright: ignore[reportPrivateUsage]  # Why: the harness syncs on the same module-global hook the heartbeat tests use.

    def _record_and_signal(value: float, *args: object, **kwargs: object) -> None:
        prev_record(value, *args, **kwargs)
        tick_done.set()

    hb_mod._tick_duration.record = _record_and_signal  # type: ignore[method-assign,reportPrivateUsage]
    saved_update = hb_mod.update_heartbeat_consecutive_failures  # pyright: ignore[reportPrivateImportUsage]  # Why: the loop's gauge writer is module-private; the harness pins its restore.
    hb_mod.update_heartbeat_consecutive_failures = lambda *a: None  # type: ignore[method-assign,reportPrivateUsage]  # pyright: ignore[reportPrivateImportUsage]
    try:
        task = asyncio.create_task(
            heartbeat_loop(
                deps,
                new_uuid(),
                shutdown,
                cancel_controller=cast("Any", controller),
            )
        )
        await asyncio.wait_for(tick_done.wait(), timeout=5.0)
        shutdown.set()
        await task
    finally:
        hb_mod._tick_duration.record = prev_record  # type: ignore[method-assign,reportPrivateUsage]
        hb_mod.update_heartbeat_consecutive_failures = saved_update  # type: ignore[method-assign,reportPrivateUsage]  # pyright: ignore[reportPrivateImportUsage]


async def test_an_acquire_failed_tick_still_drains_the_pending_abandons() -> None:
    """A tick that never got a connection must still drain the abandon queue.

    ``run_post_tx`` is the ONLY delivery point for an abandon queued by the
    same-tick fast path: the entry sits in ``_pending_abandons`` with the
    in-process ABANDON_PENDING sentinel, which matches no ladder arm in
    ``run_in_tx``. The drain runs in the tick's ``finally`` with a FULL
    command budget when the acquire itself failed (no budget was consumed),
    so a PG outage that starves the pool stalls the ladder's writes without
    ever stranding a queued abandon: the next connection, whatever tick
    lands it, carries the drain. A regression that moves the drain behind
    the success path leaves every queued abandon stuck between phases for
    as long as the outage lasts, and the handler keeps running with its
    slot gone.
    """
    controller = _PostTxRecorder()
    deps = _make_deps(heartbeat_pool=_AcquireFailPool())
    await _run_one_tick(deps, controller)

    assert controller.post_tx_calls == 1, (
        "the acquire-failed tick skipped the post-tx drain: a queued abandon "
        "would sit between phases until the pool recovers, its handler still "
        "running with no route back to cancellation"
    )
    assert deps.heartbeat_failures == 1, (
        "the failed tick must still count toward the isolate ledger"
    )


async def test_a_failing_post_tx_drain_does_not_displace_the_tick_failure() -> None:
    """A post-tx failure on an already-failed tick is logged, never raised over
    the tick's own root cause.

    The acquire failure is what the operators' alerts and the isolate
    ledger must see. If the drain's own error were allowed to displace it
    (a raise over the ``finally``'s in-flight exception), every alert and
    the failure classification would point at the drain while the real
    fault, the pool, hid behind it. The loop must still count exactly one
    failed tick and exit the loop task cleanly.
    """
    controller = _PostTxRecorder(raise_from_post_tx=True)
    deps = _make_deps(heartbeat_pool=_AcquireFailPool())

    # The observable root cause: the transient arm logs
    # "heartbeat-tick-failure", the unexpected arm
    # "heartbeat-tick-unexpected-error". The drain's displacement would
    # reclassify the tick (a RuntimeError is not transient), so the event
    # NAME is the assertion, not just the counter.
    import structlog.testing

    with structlog.testing.capture_logs() as logs:
        await _run_one_tick(deps, controller)

    assert controller.post_tx_calls == 1
    assert deps.heartbeat_failures == 1, "the drain's failure must not double-count the tick"
    events = [e.get("event") for e in logs]
    assert "heartbeat-tick-failure" in events, (
        "the tick's own acquire failure must remain the failure the ledger "
        f"and the alerts saw; log events were {events!r}"
    )
    assert "heartbeat-tick-unexpected-error" not in events, (
        "a drain failure on an already-failed tick displaced the tick's own "
        f"root cause; log events were {events!r}"
    )


# ── Seam: the abandon drain's detector-2 liveness renewal ──────────────────


class _RecordingLiveness(LoopLiveness):
    """LoopLiveness that records every tick's (name, period) pair."""

    def __init__(self) -> None:
        super().__init__()
        self.ticks: list[tuple[str, float]] = []

    def tick(self, name: str, *, period: float) -> None:
        self.ticks.append((name, period))
        super().tick(name, period=period)


class _AbandonOkBackend:
    """Backend stand-in whose mark_abandoned always applies."""

    def __init__(self) -> None:
        self.abandoned: list[JobId] = []

    async def mark_abandoned(self, job_id: JobId) -> bool:
        self.abandoned.append(job_id)
        return True


async def test_the_abandon_drain_renews_detector2_liveness_per_entry() -> None:
    """Every drain entry renews the heartbeat liveness stamp between round
    trips.

    A bulk cancel makes every active job's escalation due inside ONE tick:
    the drain's mark_abandoned + deregister cost a round trip each, and a
    multi-entry drain can outlast the staleness budget. Detector 2 reads
    the 'heartbeat' stamp and force-exits the worker mid-drain when it
    goes stale, after the tick's lease renewals already committed: the
    swept rows are not yet reclaimable, and the worker's own exit orphans
    them. The drain must therefore tick the liveness registry once per
    entry, with the heartbeat loop's own name and period, so a healthy
    drain of any length never reads as a dead loop.
    """
    liveness = _RecordingLiveness()
    deps = _make_deps(liveness=liveness)
    backend = _AbandonOkBackend()
    controller = make_cancel_controller(deps, new_uuid(), cast("Any", backend))
    interval = deps.settings.heartbeat_interval

    job_ids = [new_job_id() for _ in range(3)]
    tasks: list[asyncio.Task[object]] = []
    try:
        for jid in job_ids:
            task = _make_task()
            tasks.append(task)
            await deps.active_jobs.register(jid, task, _make_ctx(jid))
            entry = deps.active_jobs.get(jid)
            assert entry is not None
            cast("Any", controller)._pending_abandons.append((jid, entry))  # pyright: ignore[reportAttributeAccessIssue]  # Why: the deque is the concrete controller's own queue state; the test seeds it directly to isolate the drain's contract from the ladder's.

        await controller.run_post_tx()

        assert backend.abandoned == job_ids, "every queued abandon must be written, in queue order"
        assert liveness.ticks == [("heartbeat", interval)] * len(job_ids), (
            "the drain must renew detector-2's stamp once per entry with the "
            f"heartbeat loop's own name and period; got {liveness.ticks!r}"
        )
        assert deps.active_jobs.count() == 0, (
            "an applied abandon deregisters its entry; anything left behind "
            "keeps a dead handler registered against a terminal row"
        )
        for task in tasks:
            assert task.cancelling() >= 1 or task.done(), (
                "the drain owns each abandoned attempt's first cancellation"
            )
    finally:
        for task in tasks:
            task.cancel()
            with pytest.raises((asyncio.CancelledError, Exception)):
                await task


# ── Seam: the per-attempt capture boundary (async, non-tx path) ────────────


async def test_async_actor_system_exit_is_an_attempt_outcome_not_worker_death() -> None:
    """An async actor raising ``SystemExit`` is recorded as the attempt's
    outcome; the worker survives.

    ``SystemExit`` is the interpreter-shaped escape: an actor calling a
    CLI-derived helper that ends in ``sys.exit()`` must fail ITS attempt,
    truthfully labelled, not kill the consumer loop (which would cancel
    every in-flight sibling and strand the row until lease expiry relabels
    it ``WorkerCrashed``, a false audit trail). On the non-transactional
    path the actor coroutine is awaited inside the consumer's own task, so
    the boundary ``except BaseException`` is the capture point;
    ``KeyboardInterrupt`` stays the deliberate carve-out. The
    task-boundary variants (a sync actor's executor thread, the
    transactional path's actor task) are issue 459's seam and carry their
    own repro with that fix.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    async def exits(payload: object, ctx: object) -> None:
        raise SystemExit(3)

    backend.register_stub("exits", exits, payload_type=EmptyPayload)

    args = EnqueueArgs(
        id=new_job_id(),
        actor="exits",
        queue="default",
        payload={},
        max_attempts=1,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    # The drain must not raise: the consumer loop survives the actor's
    # SystemExit.
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "SystemExit", (
        "the attempt row must carry the actor's own exception class, never a "
        "relabelled WorkerCrashed from the later lease reclaim"
    )
