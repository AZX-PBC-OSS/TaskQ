"""The cancel controller takes an operator's row over from a shutdown stamp.

``orchestrate_shutdown``'s CANCELLING pass stamps every registered attempt
``COOPERATIVE`` + ``SHUTDOWN`` when the worker receives a SIGTERM. A cancel
request that lands on the row *after* that stamp is the operator's, and the
row is the arbiter: the controller's phase-1 arm has to restamp the entry
``OPERATOR`` so the consumer's ``CancelledError`` routing writes the
operator's own verdict instead of the deploy's ``mark_interrupted`` release.

The arm's predicate is therefore the origin, not the phase transition (a
row already ``COOPERATIVE`` still needs the takeover), and the takeover is
one event per request: it does not restart the shutdown's grace clock, and
it increments ``taskq.jobs.cancels_actored`` once however many heartbeat
ticks the request spans.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
import structlog
from pydantic import BaseModel

import taskq.obs._otel as otel_mod
from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import CancelOrigin, JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker.cancel import ActiveJobRegistry, make_cancel_controller
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool

_FAKE_DSN = "postgresql://fake:fake@fake:5432/fake"


class _Payload(BaseModel):
    sleep: float = 60.0


def _settings(*, cancel_grace: str = "30", cleanup_grace: str = "30") -> WorkerSettings:
    """Graces far past the test's own runtime unless a case wants an arm due."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": _FAKE_DSN,
            "TASKQ_CANCELLATION_GRACE_PERIOD": cancel_grace,
            "TASKQ_CLEANUP_GRACE_PERIOD": cleanup_grace,
            "TASKQ_LOCK_LEASE": "360",
            "TASKQ_TERMINATION_GRACE_PERIOD": "360",
        }
    )


def _make_ctx(job_id: object, worker_id: object) -> JobContext[BaseModel]:
    from taskq._ids import new_uuid as _new_uuid

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return JobContext(
        job_id=job_id,  # type: ignore[arg-type]  # Why: the context stores the id as given; the registry keys on the same value.
        actor="test_actor",
        queue="default",
        attempt=1,
        claim_epoch=1,
        worker_id=worker_id,  # type: ignore[arg-type]  # Why: as above.
        payload=_Payload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=InMemoryBackend(clock=clock),
        ),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=_new_uuid(),
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


class _OperatorCancelRowConn:
    """The heartbeat connection on the tick the operator's request is visible.

    ``cancel_phase = 1`` is the only phase an operator's request writes, and
    the controller poll returns exactly such a row. No escalation is due, so
    the connection's writes are never expected: a write here means the arm
    under test escalated a row it should have taken over first.
    """

    def __init__(self, job_id: object) -> None:
        self._job_id = job_id

    async def fetch(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
        return [{"id": self._job_id, "cancel_phase": 1}]

    async def execute(self, sql: str, *_args: object) -> str:
        raise AssertionError(f"unexpected write on the phase-1 takeover tick: {sql[:80]!r}")


class _RowAppearedAfterThePollConn:
    """The operator's request lands between the poll and the escalation write.

    ``fetch`` returns nothing (the row carried no cancel when the poll ran),
    and the escalation UPDATE's own ``cancel_phase = 1`` guard matches, which
    is the row-side proof that an operator's request arrived in the gap.
    """

    def __init__(self) -> None:
        self.writes: list[str] = []

    async def fetch(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
        return []

    async def execute(self, sql: str, *_args: object) -> str:
        self.writes.append(sql)
        return "UPDATE 1" if "UPDATE" in sql.upper() else "INSERT 0 1"


class _CountingCounter:
    """Stand-in for the ``taskq.jobs.cancels_actored`` instrument."""

    def __init__(self) -> None:
        self.points: list[int] = []

    def add(self, amount: int, attributes: dict[str, object] | None = None) -> None:
        self.points.append(amount)


class _ShutdownStamped:
    """A registry entry as the orchestrator's CANCELLING pass leaves it."""

    def __init__(self, *, observed_at: float) -> None:
        self.worker_id = new_uuid()
        self.registry = ActiveJobRegistry()
        self.job_id = new_job_id()
        self.ctx = _make_ctx(self.job_id, self.worker_id)
        self.observed_at = observed_at

    async def register(self) -> None:
        task: asyncio.Task[object] = asyncio.get_running_loop().create_task(asyncio.sleep(3600))
        self.task = task
        entry = await self.registry.register(self.job_id, task, self.ctx)
        # The CANCELLING pass's own stamps (orchestrate_shutdown): cooperative
        # phase, the shutdown grace clock, and the shutdown as the origin.
        entry.cancel_phase = CancelPhase.COOPERATIVE
        entry.cancel_observed_at = self.observed_at
        entry.cancel_origin = CancelOrigin.SHUTDOWN
        self.ctx._set_cancel_origin(CancelOrigin.SHUTDOWN)  # pyright: ignore[reportPrivateUsage]  # Why: the orchestrator's designated writer of the context stamp (set alongside cancel_event.set()).
        self.entry = entry

    def controller(self, settings: WorkerSettings, backend: object) -> object:
        deps = WorkerDeps(
            settings=settings,
            dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: asyncpg.Pool is a class, not an instance; the test bypasses real pool construction.
            heartbeat_pool=_FakePool(),  # type: ignore[arg-type]  # Why: as above.
            worker_pool=_FakePool(),  # type: ignore[arg-type]  # Why: as above.
            notify_conn=None,
            leader_conn=None,
            active_jobs=self.registry,
        )
        return make_cancel_controller(deps, self.worker_id, backend)  # type: ignore[arg-type]  # Why: the controller only calls mark_abandoned on the backend for the arms not exercised here.

    def reap(self) -> None:
        self.task.cancel()


async def test_a_controller_tick_takes_a_shutdown_stamped_entry_over_to_the_operator() -> None:
    """SIGTERM stamped the entry; the operator's later request owns the verdict."""
    from taskq.testing.actor import FakeBackend

    counter = _CountingCounter()
    original = otel_mod._cancels_actored  # pyright: ignore[reportPrivateUsage]  # Why: the counter is a module global the recorder reads at call time; swapping it is the established per-test isolation (tests/test_cancel_obs.py).
    otel_mod._cancels_actored = counter  # pyright: ignore[reportPrivateUsage]
    stamp = _ShutdownStamped(observed_at=asyncio.get_running_loop().time() - 1.0)
    await stamp.register()
    try:
        controller = stamp.controller(_settings(), FakeBackend())
        await controller.run_in_tx(_OperatorCancelRowConn(stamp.job_id))  # type: ignore[attr-defined]  # Why: the controller takes an asyncpg.Connection; the double implements the methods the arms call.

        assert stamp.entry.cancel_origin is CancelOrigin.OPERATOR, (
            "the entry still reads SHUTDOWN after the controller observed the "
            "operator's request on its row: the consumer's CancelledError "
            "routing will release the attempt back to the fleet "
            "(mark_interrupted) instead of writing the operator's verdict"
        )
        assert stamp.ctx.cancel_origin is CancelOrigin.OPERATOR, (
            "the context's origin stamp did not follow the takeover: the actor "
            "body reads it to decide whether to checkpoint or return"
        )
        assert stamp.entry.cancel_observed_at == stamp.observed_at, (
            "the takeover restarted the shutdown's grace clock: the CANCELLING "
            "grace would measure from the operator's request instead of from "
            "the signal, and a re-observed request would re-arm it every tick"
        )
        assert sum(counter.points) == 1, (
            "taskq.jobs.cancels_actored must count the one request the worker "
            f"began acting on; it recorded {counter.points!r}"
        )
    finally:
        otel_mod._cancels_actored = original  # pyright: ignore[reportPrivateUsage]
        stamp.reap()


async def test_a_second_controller_tick_does_not_restamp_or_recount() -> None:
    """The takeover is one event: later ticks walking the same row are silent."""
    from taskq.testing.actor import FakeBackend

    counter = _CountingCounter()
    original = otel_mod._cancels_actored  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    otel_mod._cancels_actored = counter  # pyright: ignore[reportPrivateUsage]
    stamp = _ShutdownStamped(observed_at=asyncio.get_running_loop().time() - 1.0)
    await stamp.register()
    try:
        controller = stamp.controller(_settings(), FakeBackend())
        conn = _OperatorCancelRowConn(stamp.job_id)
        await controller.run_in_tx(conn)  # type: ignore[attr-defined]
        await controller.run_in_tx(conn)  # type: ignore[attr-defined]

        assert stamp.entry.cancel_origin is CancelOrigin.OPERATOR
        assert stamp.entry.cancel_observed_at == stamp.observed_at
        assert sum(counter.points) == 1, (
            "the second tick counted the same operator request again: "
            f"taskq.jobs.cancels_actored moved {counter.points!r}, and its "
            "documented contract is one increment per request"
        )
    finally:
        otel_mod._cancels_actored = original  # pyright: ignore[reportPrivateUsage]
        stamp.reap()


async def test_the_escalation_arm_takes_over_a_row_that_appeared_after_the_poll() -> None:
    """The phase-2 arm is the takeover when the poll ran before the request landed."""
    from taskq.testing.actor import FakeBackend

    counter = _CountingCounter()
    original = otel_mod._cancels_actored  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    otel_mod._cancels_actored = counter  # pyright: ignore[reportPrivateUsage]
    # Grace 0 with the stamp in the past: the escalation arm is due this tick,
    # and the cleanup grace keeps it off the abandon queue.
    stamp = _ShutdownStamped(observed_at=asyncio.get_running_loop().time() - 5.0)
    await stamp.register()
    try:
        controller = stamp.controller(
            _settings(cancel_grace="0", cleanup_grace="30"), FakeBackend()
        )
        conn = _RowAppearedAfterThePollConn()
        await controller.run_in_tx(conn)  # type: ignore[attr-defined]

        assert stamp.entry.cancel_phase == CancelPhase.FORCED, (
            f"the escalation did not land, so this tick is not the gap race: {conn.writes!r}"
        )
        assert stamp.entry.cancel_origin is CancelOrigin.OPERATOR, (
            "the escalation's UPDATE matched a row carrying an operator's request "
            "and the entry was left stamped SHUTDOWN: the consumer will release "
            "the attempt back to the fleet instead of writing the operator's verdict"
        )
        assert stamp.ctx.cancel_origin is CancelOrigin.OPERATOR
        assert sum(counter.points) == 1, (
            "taskq.jobs.cancels_actored must count the one request this arm "
            f"observed first; it recorded {counter.points!r}"
        )
    finally:
        otel_mod._cancels_actored = original  # pyright: ignore[reportPrivateUsage]
        stamp.reap()


@pytest.mark.parametrize("ticks", [1, 2])
async def test_the_entry_stays_registered_across_the_takeover(ticks: int) -> None:
    """Nothing in the takeover deregisters the attempt it just took over."""
    from taskq.testing.actor import FakeBackend

    stamp = _ShutdownStamped(observed_at=asyncio.get_running_loop().time() - 1.0)
    await stamp.register()
    try:
        controller = stamp.controller(_settings(), FakeBackend())
        conn = _OperatorCancelRowConn(stamp.job_id)
        for _ in range(ticks):
            await controller.run_in_tx(conn)  # type: ignore[attr-defined]
        assert stamp.registry.get(stamp.job_id) is stamp.entry
        assert not stamp.task.done()
    finally:
        stamp.reap()
