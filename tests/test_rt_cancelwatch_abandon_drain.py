"""Red-team: phase-3 abandonment drain failure modes in ``_CancelController``.

Two attacking interleavings around ``run_post_tx``'s drain of
``_pending_abandons``:

1. **Drain exception strands the job at ABANDON_PENDING.**
   ``run_in_tx`` sets ``active.cancel_phase = ABANDON_PENDING`` *before*
   the abandon write runs (cancel.py: the both-deadlines branch sets the
   sentinel so the consumer's CancelledError path skips ``mark_cancelled``).
   ``run_post_tx`` then pops the job and awaits ``mark_abandoned``.  If that
   write RAISES (a mere pool-acquire ``TimeoutError`` is enough), the entry
   keeps ``ABANDON_PENDING`` - and no arm in ``run_in_tx`` ever matches
   ``ABANDON_PENDING`` again: the phase-1 arm needs ``< COOPERATIVE``, the
   re-issue arm needs ``== COOPERATIVE`` or ``(>= FORCED AND db ==
   COOPERATIVE)``, and the phase-3 queueing arm needs ``== FORCED`` exactly.
   The not-applied fallback in ``run_post_tx`` (entry back to ``FORCED``)
   only covers the ``False`` return, never the exception.  One transient
   write failure therefore leaves a hung-actor job *permanently*
   un-abandonable while the heartbeat keeps renewing its lease - the sweep
   can never reclaim it either.

2. **The phase-3 queueing arm ignores poll ownership.**
   The arm queues an abandon for any entry at local ``FORCED`` past both
   graces, without consulting ``db_phase`` - i.e. without checking that
   THIS tick's own poll still returns the row.  Once the job has been
   reclaimed (``locked_by_worker`` no longer this worker) the poll stays
   silent, yet the abandon is re-issued every tick, unbounded.  Because
   ``mark_abandoned`` is deliberately worker-unfenced (guard: ``status =
   'running' AND cancel_phase = 2``), a stale entry can terminate a
   re-dispatched attempt now owned by ANOTHER worker - the PG-level proof
   is in ``tests/test_rt_cancelwatch_cross_worker_abandon.py``; this file
   pins the controller-level contract.
"""

import asyncio
import contextlib

import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend._sql import INSERT_EVENT_SQL
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend
from taskq.worker.cancel import CancelController, make_cancel_controller
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool

_FAKE_DSN = "postgresql://fake:fake@fake:5432/fake"


def _ws(**overrides: float | str) -> WorkerSettings:
    data: dict[str, str] = {
        "TASKQ_PG_DSN": _FAKE_DSN,
        "TASKQ_LOCK_LEASE": "360",
        "TASKQ_TERMINATION_GRACE_PERIOD": "360",
    }
    for k, v in overrides.items():
        data[f"TASKQ_{k}"] = str(v)
    return WorkerSettings.load_from_dict(data)


class _MockRow(dict[str, object]):
    """Dict subclass so row["id"] / row["cancel_phase"] work like asyncpg Records."""


class _Recorder:
    """Mock asyncpg.Connection: canned poll rows, ``UPDATE 1`` escalations."""

    def __init__(self, poll_rows: list[_MockRow]) -> None:
        self.poll_rows = poll_rows
        self.execute_calls: list[str] = []

    async def fetch(self, sql: str, *args: object) -> list[_MockRow]:
        return list(self.poll_rows)

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append(sql)
        return "UPDATE 1"


class _StubPayload(BaseModel):
    """Minimal payload for a cancel-path JobContext."""


def _make_ctx() -> JobContext[BaseModel]:
    from datetime import UTC, datetime

    from taskq.obs import bind_job_context
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    backend = InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))
    return JobContext(
        job_id=new_uuid(),
        actor="test",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=_StubPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=new_uuid(),
            actor="test",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


def _make_deps(ws: WorkerSettings) -> WorkerDeps:
    return WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


def _sleeper() -> asyncio.Task[object]:
    return asyncio.get_running_loop().create_task(asyncio.sleep(3600))


async def _reap_sleeper(task: asyncio.Task[object]) -> None:
    """Cancel and await the sleeper task the test registered as its job.

    The suite's loop-teardown doctrine (``_stop_loop`` in
    test_leader_sweeps_coverage.py, ``_stop_quietly`` in
    test_drain_liveness.py): a task minted on the module loop is the
    minting test's to retrieve. The registry's ``deregister`` only drops
    bookkeeping - it never touches the task - and the controller cancels
    the task only on the escalation path, which the poll-ownership
    contract below deliberately never reaches, so nothing but this
    reap ever stops the sleeper.
    """
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def _tick(controller: CancelController, conn: _Recorder) -> None:
    """One heartbeat-tick shape: in-tx hook, then the post-tx drain."""
    await controller.run_in_tx(conn)  # type: ignore[arg-type]
    await controller.run_post_tx()


async def test_abandon_write_failure_does_not_strand_job_at_abandon_pending() -> None:
    """A raised ``mark_abandoned`` (e.g. pool-acquire TimeoutError) must not
    permanently strand the job: a later tick must re-attempt the abandon.

    Current behavior violates the contract because the drain pops the entry
    and lets the exception escape while the registry entry keeps the
    in-process ``ABANDON_PENDING`` sentinel - a phase no arm in ``run_in_tx``
    ever matches again, so the abandon is never re-issued and the job is
    registered forever with no route to a terminal state.
    """
    job_id = new_job_id()
    worker_id = new_uuid()
    ws = _ws(CANCELLATION_GRACE_PERIOD="0.0", CLEANUP_GRACE_PERIOD="0.0")
    deps = _make_deps(ws)
    sleeper = _sleeper()
    await deps.active_jobs.register(job_id, sleeper, _make_ctx())
    entry = deps.active_jobs.get(job_id)
    assert entry is not None
    entry.cancel_phase = CancelPhase.COOPERATIVE
    entry.cancel_observed_at = asyncio.get_running_loop().time() - 1.0

    class _OnceFailingBackend(FakeBackend):
        """Raises once (transient infra shape), then applies."""

        def __init__(self) -> None:
            super().__init__()
            self.calls: list[object] = []

        async def mark_abandoned(self, job_id: object) -> bool:  # type: ignore[override]
            self.calls.append(job_id)
            if len(self.calls) == 1:
                raise TimeoutError("worker_pool acquire timed out")
            return True

    backend = _OnceFailingBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]

    try:
        # Tick 1: escalation applies, both deadlines already satisfied → the job
        # is queued with the ABANDON_PENDING sentinel, then the drain raises.
        recorder = _Recorder([_MockRow(id=job_id, cancel_phase=1)])
        with pytest.raises(TimeoutError, match="worker_pool acquire timed out"):
            await _tick(controller, recorder)
        assert backend.calls == [job_id], "fixture broken: first drain attempt must fire"
        assert INSERT_EVENT_SQL.format(schema=ws.schema_name) in recorder.execute_calls

        # Tick 2: the same state a production heartbeat reaches on its next
        # interval - PG healthy again, entry still registered.
        await _tick(controller, _Recorder([]))

        assert len(backend.calls) == 2, (
            "Contract: an abandon whose write raised (a transient pool-acquire "
            "TimeoutError) must be re-attempted on a later tick, exactly like the "
            "not-applied False path is (run_post_tx's documented fallback). Current "
            "behavior: the entry was left at the in-process ABANDON_PENDING sentinel, "
            "which no phase arm in run_in_tx matches, so mark_abandoned is never "
            "called again and the job can never reach a terminal state."
        )
        assert deps.active_jobs.get(job_id) is None, (
            "Contract: once the re-attempted abandon applies, the job must be "
            "deregistered. Current behavior: the entry is stranded registered at "
            "ABANDON_PENDING forever."
        )
    finally:
        await _reap_sleeper(sleeper)


async def test_phase3_abandon_not_issued_for_job_absent_from_own_poll() -> None:
    """A tick whose own poll no longer returns the job must not abandon it.

    The poll (``POLL_CANCEL_FLAGS_SQL``) is fenced on ``locked_by_worker =
    $1 AND cancel_requested_at IS NOT NULL AND status = 'running'`` - the
    exact set of rows an abandon from THIS worker is entitled to touch.
    Once a reclaim has moved the row to another holder, the poll goes
    silent, but the phase-3 arm queues on local state alone; because
    ``mark_abandoned`` is worker-unfenced, the issued abandon can terminate
    the new holder's attempt (see the PG proof in
    test_rt_cancelwatch_cross_worker_abandon.py).
    """
    job_id = new_job_id()
    worker_id = new_uuid()
    ws = _ws(CANCELLATION_GRACE_PERIOD="0.0", CLEANUP_GRACE_PERIOD="0.0")
    deps = _make_deps(ws)
    sleeper = _sleeper()
    await deps.active_jobs.register(job_id, sleeper, _make_ctx())
    entry = deps.active_jobs.get(job_id)
    assert entry is not None
    # Stale in-memory state from before the reclaim: already FORCED, both
    # deadlines long past.
    entry.cancel_phase = CancelPhase.FORCED
    entry.cancel_observed_at = asyncio.get_running_loop().time() - 100.0

    class _CountingBackend(FakeBackend):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[object] = []

        async def mark_abandoned(self, job_id: object) -> bool:  # type: ignore[override]
            self.calls.append(job_id)
            return True

    backend = _CountingBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]

    try:
        # The poll returns NOTHING for this worker: the job was reclaimed and is
        # no longer (running, locked-by-this-worker, cancel-flagged).
        await _tick(controller, _Recorder([]))

        assert backend.calls == [], (
            "Contract: a cancel-poll tick must not issue mark_abandoned for a job "
            "its own poll did not return - the poll's predicate (locked_by_worker, "
            "cancel_requested_at, status='running') is exactly the set of rows this "
            "worker's abandon may touch; mark_abandoned is worker-unfenced, so an "
            "abandon issued from stale local state can terminate another worker's "
            "re-dispatched attempt. Current behavior: the phase-3 arm queues on "
            "local phase alone and the abandon was issued."
        )
    finally:
        # The asserted contract leaves the job registered and its sleeper
        # running (no abandon, no escalation): the test stops the sleeper it
        # minted, or it stays pending on the module loop past teardown.
        await _reap_sleeper(sleeper)
