"""A sibling crash must interrupt in-flight jobs, never phantom-cancel them.

The exec loop's ``CancelledError`` handler routes on the registry entry's
``cancel_origin``. A sibling crash in the worker TaskGroup (a leader sweep
hitting a dead PG, say) tears the consumers down by cancellation while no
orchestrator has stamped any entry: ``cancel_origin`` stays NONE and every
running job fell through to ``mark_cancelled``, a phantom operator cancel
(status 'cancelled', ``cancel_phase = 0``, ``cancel_requested_at = NULL``)
that spent an attempt, wrote a ``job_attempts`` row with
``outcome='cancelled'``, and fired ``on_cancel``, distinguishable in the
database from a real request by nothing at all.

The fix stamps SHUTDOWN origins at the crash (``_stamp_interrupt_origins``
in the sibling spawner, and on the bare ``_main`` cancel path), so the job
takes the interruption route the signal-driven shutdown already takes:
released to the fleet as pending, recoverable, no cancel write.
"""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from taskq._ids import new_uuid
from taskq.context import CancelOrigin
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._bootstrap import (  # pyright: ignore[reportPrivateUsage]  # Why: the crash site under test is the spawner's own guard; the test pins its stamping contract directly.
    _make_sibling_spawner,
)
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps
from tests.conftest import EmptyPayload, FakeBackend, as_backend, default_actor_config

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.4",
            "TASKQ_TERMINATION_GRACE_PERIOD": "20.0",
        }
    )


def _deps(settings: WorkerSettings, registry: ActiveJobRegistry) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.settings = settings
    deps.disowned_jobs = set()
    deps.progress_buffers = {}
    deps.worker_pool = None
    deps.redis_client = None
    deps.shutdown_started_at = None
    deps.active_jobs = registry
    return deps


def _entry(origin: CancelOrigin) -> SimpleNamespace:
    stamps: list[CancelOrigin] = []
    return SimpleNamespace(  # type: ignore[return-value]  # Why: the minimal attribute surface the stamping loop reads, mirroring tests/test_heartbeat_isolate.py's entry fakes.
        cancel_origin=origin,
        ctx=SimpleNamespace(_set_cancel_origin=stamps.append),
        stamps=stamps,
    )


async def _crash_a_sibling(deps: MagicMock, shutdown_event: asyncio.Event) -> None:
    """Run one crashing sibling through the real spawner inside a TaskGroup.

    The group teardown is suppressed: the ExceptionGroup is the crash
    propagating, the assertions below are about what the crash left on the
    registry entries before the group collected it.
    """
    with suppress(BaseExceptionGroup):

        async def crashing() -> None:
            raise RuntimeError("leader sweep hit a dead PG")

        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown_event, deps)
            spawn(crashing())


async def test_a_sibling_crash_stamps_origin_less_entries_shutdown() -> None:
    """A crashing sibling stamps every origin-less active entry SHUTDOWN.

    The stamp is what routes the consumers' teardown cancellations into
    the interrupt arm instead of the cancel ladder. It must be in place
    before the group starts cancelling siblings, which the spawner's crash
    arm guarantees by stamping synchronously before ``shutdown_event`` is
    set and the re-raise reaches the group.
    """
    registry = ActiveJobRegistry()
    entry = _entry(CancelOrigin.NONE)
    registry._by_id[new_uuid()] = entry  # type: ignore[index-assign]  # Why: unit test injects a minimal entry; the registry's real register() needs a full JobContext the fake replaces.
    shutdown_event = asyncio.Event()
    deps = _deps(_settings(), registry)

    await _crash_a_sibling(deps, shutdown_event)

    assert entry.cancel_origin is CancelOrigin.SHUTDOWN, (
        "a sibling crash must stamp origin-less entries SHUTDOWN so the "
        "consumers' teardown cancellations route to the interrupt arm, not "
        "to mark_cancelled"
    )
    assert entry.stamps == [CancelOrigin.SHUTDOWN]  # type: ignore[union-attr]  # Why: the SimpleNamespace entry carries the stamp recorder next to the origin.
    assert shutdown_event.is_set()


async def test_a_sibling_crash_never_clobbers_an_operator_stamp() -> None:
    """A real operator cancel survives the crash stamp untouched.

    The stamp exists to give origin-less cancels a route, never to
    reclassify an operator's request: the controller's OPERATOR stamp is
    the row's audit trail of who asked, and the crash arm must not
    overwrite it.
    """
    registry = ActiveJobRegistry()
    entry = _entry(CancelOrigin.OPERATOR)
    registry._by_id[new_uuid()] = entry  # type: ignore[index-assign]  # Why: unit test injects a minimal entry; the registry's real register() needs a full JobContext the fake replaces.
    shutdown_event = asyncio.Event()
    deps = _deps(_settings(), registry)

    await _crash_a_sibling(deps, shutdown_event)

    assert entry.cancel_origin is CancelOrigin.OPERATOR
    assert entry.stamps == []  # type: ignore[union-attr]  # Why: the SimpleNamespace entry carries the stamp recorder next to the origin.


async def test_a_sibling_crash_lands_running_jobs_interrupted_not_cancelled() -> None:
    """The behavior pin: a sibling crash releases the running job to the
    fleet via ``mark_interrupted`` and never writes a phantom cancel.

    The crash lands mid-job, exactly as a dead-PG leader sweep lands: the
    group tears the consumer down by cancellation, the consumer reads the
    SHUTDOWN stamp the crash arm left, and the attempt is released to the
    fleet (interrupted, recoverable) instead of terminalising 'cancelled'
    with no cancel request on the row.
    """
    settings = _settings()
    registry = ActiveJobRegistry()
    deps = _deps(settings, registry)
    backend = FakeBackend()
    job = make_job_row()
    crash_go = asyncio.Event()
    shutdown_event = asyncio.Event()

    async def body(running: object, ctx: object) -> object:
        del running, ctx
        await asyncio.sleep(3600)
        return {"unreachable": True}

    async def consumer() -> None:
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=body,  # type: ignore[arg-type]  # Why: the minimal run_actor surface the autonomous path awaits.
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            active_jobs=registry,
        )

    async def crashing() -> None:
        await crash_go.wait()
        raise RuntimeError("leader sweep hit a dead PG")

    try:
        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown_event, deps)
            spawn(consumer())
            spawn(crashing())
            # The crash fires only once the job is registered and running:
            # the exact mid-flight shape the bug terminalised.
            await wait_for_condition(
                lambda: registry.get(job.id) is not None,
                description="the consumer's registration of the job",
                timeout=5.0,
            )
            crash_go.set()
    except BaseExceptionGroup:
        pass

    assert len(backend.mark_interrupted_calls) == 1, (
        "a sibling crash must release the running job back to the fleet "
        "(the interrupted route), not terminalise it"
    )
    assert backend.mark_interrupted_calls[0]["hold"] == timedelta(0), (
        "an async actor has unwound by the time the handler runs: the "
        "immediate pending release is the earned shape"
    )
    assert backend.mark_cancelled_calls == [], (
        "a sibling crash must never write a phantom cancel: the row carries "
        "no cancel request, so outcome='cancelled' and the on_cancel hook "
        "would report an operator action that never happened"
    )
