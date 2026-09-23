"""Red-team: the cancel ladder's ownership of a polled row with NO registry entry.

The attacking interleaving (surfaced by the system-e2e cancel-storm tier's
effects ledger - a body run with no claim row behind it):

1. A job is claimed (attempt 0->1, ``started_at`` stamped by the claim CTE)
   and its body runs; the body's own evidence row commits.
2. The body fails fast and the worker's outcome write (``mark_retry``) races
   an operator cancel: the row already carries ``cancel_phase = 1``, the
   cancel fence matches NO arm, the write no-ops, and the consumer's
   unconditional ``finally`` DEREGISTERS the row - which stays ``running``,
   locked by this worker, phase 1, holding nobody.
3. The ladder walked ``active_jobs.all()`` only, so the stranded row matched
   NO writer: the heartbeat kept renewing its lease, and once its
   claim-stamped ``started_at`` aged past the lock lease the claim-loss
   reconcile read it as "a claim that never reached an actor" and REFUNDED
   the attempt whose body had already run. The executed attempt lost its
   ledger row; the reclaim sweep's cancel branch then terminalised the row
   at the refunded attempt number with no attempt row anywhere.

The contract pinned here: the poll's own predicate (``locked_by_worker =
this worker AND cancel_requested_at IS NOT NULL AND status = 'running'``)
is exactly the set of rows this ladder owns - entry or not. An unheld row
must walk the SAME grace schedule from the controller's first sight: the
phase-2 escalation after the cancellation grace, the abandon after the
cleanup grace on top, never a stale-inherited elapsed.
"""

import asyncio
from typing import Any

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend._sql import CANCEL_ESCALATION_SQL, INSERT_EVENT_SQL
from taskq.testing.actor import FakeBackend
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from tests.test_rt_cancelwatch_abandon_drain import (
    _make_ctx,
    _make_deps,
    _MockRow,
    _reap_sleeper,
    _Recorder,
    _sleeper,
    _tick,
    _ws,
)


class _CountingBackend(FakeBackend):
    """Records ``mark_abandoned`` calls, always applies."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[object] = []

    async def mark_abandoned(self, job_id: object) -> bool:  # type: ignore[override]
        self.calls.append(job_id)
        return True


def _poll_row(job_id: object, phase: int) -> _MockRow:
    return _MockRow(id=job_id, cancel_phase=phase)


def _controller(deps: WorkerDeps, worker_id: Any) -> tuple[Any, _CountingBackend, dict[Any, float]]:
    """The controller under test plus its backend and its sighting map."""
    backend = _CountingBackend()
    controller = make_cancel_controller(deps, worker_id, backend)
    return controller, backend, controller._unheld_observed_at  # pyright: ignore[reportAttributeAccessIssue]  # Why: the sighting map is the private seam the graces-measure pin needs; the module's own code reaches ctx._abort_requested the same way.


async def test_unheld_phase1_row_walks_the_ladder_to_abandon() -> None:
    """A polled phase-1 row with no registry entry must reach mark_abandoned.

    Before the unheld walk existed, such a row matched no ladder arm: the
    walk only iterated registered entries, so the stranded row sat
    ``running`` forever (renewed lease, no writer) until the claim-loss
    reconcile refunded its executed attempt. The escalation (the
    ``cancel_phase = 1 -> 2`` UPDATE plus its state_change event) and the
    abandon must both fire from the poll's own witness.
    """
    job = new_job_id()
    worker_id = new_uuid()
    ws = _ws(CANCELLATION_GRACE_PERIOD="0.0", CLEANUP_GRACE_PERIOD="0.0")
    deps = _make_deps(ws)
    controller, backend, _sightings = _controller(deps, worker_id)

    recorder = _Recorder([_poll_row(job, 1)])
    await _tick(controller, recorder)  # type: ignore[arg-type]

    assert CANCEL_ESCALATION_SQL.format(schema=ws.schema_name) in recorder.execute_calls, (
        "Contract: an unheld polled row carrying cancel_phase=1 must be escalated "
        "by the ladder's own walk - the poll's predicate is the set of rows this "
        "ladder owns, entry or not. Current behavior: no arm matched the "
        "registry-less row, the escalation never fired, and the row was left "
        "running with its flag carried for no writer."
    )
    assert INSERT_EVENT_SQL.format(schema=ws.schema_name) in recorder.execute_calls, (
        "Contract: the escalation's state_change event must be written with the "
        "same shape the held walk's escalation writes."
    )
    assert backend.calls == [], (
        "fixture broken: the abandon waits for the escalated phase to be durable - "
        "this tick's poll still read phase 1"
    )

    # The escalated phase is durable: the next tick's poll reads phase 2
    # and the abandon fires.
    await _tick(controller, _Recorder([_poll_row(job, 2)]))  # type: ignore[arg-type]
    assert backend.calls == [job], (
        "Contract: an unheld polled row must reach mark_abandoned - mark_abandoned "
        "is worker-unfenced and guarded on cancel_phase=2, so the abandon it "
        "issues is the row's only route to a terminal state that still writes "
        "the executed attempt's ledger row. Current behavior: the abandon never "
        "fired, the row stranded running, and the claim-loss reconcile went on "
        "to refund an attempt whose body had run."
    )


async def test_unheld_row_graces_measure_first_observation() -> None:
    """The unheld walk's graces run from the controller's first sight.

    Same schedule as the held walk: observation, escalation no earlier than
    the cancellation grace, abandon no earlier than the cleanup grace on
    top - never immediately, never from a stale elapsed.
    """
    job = new_job_id()
    worker_id = new_uuid()
    ws = _ws(CANCELLATION_GRACE_PERIOD="1.0", CLEANUP_GRACE_PERIOD="1.0")
    deps = _make_deps(ws)
    controller, backend, sightings = _controller(deps, worker_id)
    escalation = CANCEL_ESCALATION_SQL.format(schema=ws.schema_name)

    # Tick 1: first sight, elapsed ~0 - observation only, nothing fires.
    await _tick(controller, _Recorder([_poll_row(job, 1)]))  # type: ignore[arg-type]
    assert backend.calls == [], (
        "Contract: the unheld walk must observe before it escalates - the graces "
        "give the in-flight handoff (claim committed, register not yet landed) "
        "the same room the held walk's cancel_observed_at gives it."
    )
    assert job in sightings, "fixture broken: the first tick must stamp the sighting"

    # Tick 2: past the cancellation grace, inside the cleanup grace -
    # the escalation fires, the abandon does not.
    sightings[job] -= 1.5
    recorder = _Recorder([_poll_row(job, 1)])
    await _tick(controller, recorder)  # type: ignore[arg-type]
    assert escalation in recorder.execute_calls
    assert backend.calls == [], (
        "Contract: the abandon waits for the escalation to be durable plus the "
        "cleanup grace - an abandon issued against cancel_phase=1 cannot apply "
        "(mark_abandoned's guard is cancel_phase=2) and would only burn a tick."
    )

    # Tick 3: the escalation is durable (the poll reads phase 2) and the
    # combined graces have passed - the abandon fires and the stamp drops.
    sightings[job] -= 1.0
    await _tick(controller, _Recorder([_poll_row(job, 2)]))  # type: ignore[arg-type]
    assert backend.calls == [job]
    assert job not in sightings, (
        "Contract: once the abandon is queued the observation stamp goes with it "
        "- the drain owns the row from there, and a still-polled row must "
        "re-observe with fresh graces rather than double-queue on stale elapsed."
    )


async def test_unheld_stamp_drops_when_the_poll_goes_silent() -> None:
    """A row the poll stops returning must lose its sighting.

    The graces measure THIS worker's observation of the flag; a row that
    left the poll (terminalised elsewhere, reclaimed) and later reappears
    must re-observe with fresh graces, never inherit stale elapsed and
    escalate on sight.
    """
    job = new_job_id()
    worker_id = new_uuid()
    ws = _ws(CANCELLATION_GRACE_PERIOD="1.0", CLEANUP_GRACE_PERIOD="1.0")
    deps = _make_deps(ws)
    controller, backend, sightings = _controller(deps, worker_id)

    await _tick(controller, _Recorder([_poll_row(job, 1)]))  # type: ignore[arg-type]
    assert job in sightings, "fixture broken: the first tick must stamp the sighting"

    # The row left this worker's poll: terminalised by another writer, or
    # reclaimed. The poll is silent; the stamp must drop.
    await _tick(controller, _Recorder([]))  # type: ignore[arg-type]
    assert job not in sightings, (
        "Contract: a row the poll stopped returning is no longer this ladder's "
        "to walk - keeping its sighting would let a LATER reappearance inherit "
        "stale elapsed and escalate a row (a re-claimed attempt's running row) "
        "this worker has only just met."
    )

    # The same id reappears (a re-dispatch's row): fresh sighting, no
    # escalation on sight.
    await _tick(controller, _Recorder([_poll_row(job, 1)]))  # type: ignore[arg-type]
    assert backend.calls == []


async def test_unheld_abandon_delivers_to_a_late_registered_entry() -> None:
    """The drain of an unheld abandon still cancels an entry that registered late.

    The claim->register handoff can complete between the queueing tick and
    the post-commit drain: the row that queued with NO entry now HAS one,
    and its body is running. The abandon is durable (mark_abandoned has
    applied), so the delivery must reach the live task and drop its
    registration - a skip would leave a body running against an
    already-terminal row with no route back to cancellation.
    """
    job = new_job_id()
    worker_id = new_uuid()
    ws = _ws(CANCELLATION_GRACE_PERIOD="0.0", CLEANUP_GRACE_PERIOD="0.0")
    deps = _make_deps(ws)
    controller, backend, _sightings = _controller(deps, worker_id)

    # The queueing tick: phase 2 already durable, both graces zero - the
    # walk queues the unheld abandon at first sight.
    await controller.run_in_tx(_Recorder([_poll_row(job, 2)]))  # type: ignore[arg-type]
    assert backend.calls == [], "fixture broken: the drain has not run yet"

    # The handoff completes: the dispatch's register lands before the drain.
    sleeper = _sleeper()
    try:
        await deps.active_jobs.register(job, sleeper, _make_ctx())
        await controller.run_post_tx()

        assert backend.calls == [job], "fixture broken: the drain must apply the queued abandon"
        assert deps.active_jobs.get(job) is None, (
            "Contract: an applied abandon's delivery must reach the entry the "
            "registry holds at drain time - the handoff completed between the "
            "queueing tick and the drain, and a body left running against a "
            "terminal row with no cancellation delivered is exactly the strand "
            "this drain exists to close."
        )
        assert sleeper.cancelling() > 0 or sleeper.cancelled()
    finally:
        await _reap_sleeper(sleeper)


@pytest.mark.parametrize("phase", [1, 2])
async def test_unheld_walk_never_touches_registered_entries(phase: int) -> None:
    """A polled row that HAS an entry stays the held walk's business.

    The unheld walk resolves ownership through the registry at walk time;
    a row whose entry exists must produce no escalation from the unheld
    side on top of the held walk's own arms.
    """
    job = new_job_id()
    worker_id = new_uuid()
    ws = _ws(CANCELLATION_GRACE_PERIOD="0.0", CLEANUP_GRACE_PERIOD="0.0")
    deps = _make_deps(ws)
    controller, _backend, _sightings = _controller(deps, worker_id)
    escalation = CANCEL_ESCALATION_SQL.format(schema=ws.schema_name)

    sleeper = _sleeper()
    try:
        await deps.active_jobs.register(job, sleeper, _make_ctx())
        entry = deps.active_jobs.get(job)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 100.0

        recorder = _Recorder([_poll_row(job, phase)])
        await _tick(controller, recorder)  # type: ignore[arg-type]

        escalations = recorder.execute_calls.count(escalation)
        assert escalations <= 1, (
            "Contract: the unheld walk must filter polled rows through the "
            "registry at walk time - a row whose entry exists is the held "
            "walk's, and a second escalation from the unheld side would double "
            "the phase-2 write and its event."
        )
    finally:
        await _reap_sleeper(sleeper)
