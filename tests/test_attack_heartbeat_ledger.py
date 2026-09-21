"""ATTACK tests for fix/heartbeat-guard-phantom-cancel (PR 367).

Two surfaces, asserted at the behavior level only:

1. The heartbeat failure ledger: a failed tick of EITHER class
   (transient PG blip, unexpected error) must move the worker one tick
   closer to isolation, a fully successful tick must reset the run, and
   isolation must fire at exactly the documented tick - the (F+1)-th
   CONSECUTIVE failed tick, whatever the class mix. The observation
   point is the worker's own isolate action (the module seam the loop
   invokes: the worker handing its running jobs to isolation and
   returning), never a counter.

2. The sibling-crash stamp window: a crashing TaskGroup tears consumers
   down by cancellation; the crash stamps SHUTDOWN origins so the
   teardown routes to the interrupt arm (the job is released to the
   fleet), never to the cancel ladder (a phantom operator cancel). The
   branch documents a residual window for claims racing the stamp; the
   attacks below pin the bound's invariant (every claim that completed
   before the stamp is stamped) and probe the documented residual.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from taskq._ids import new_uuid
from taskq.context import CancelOrigin
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._bootstrap import (  # pyright: ignore[reportPrivateUsage]  # Why: the crash site under test is the spawner's own guard; the pin drives it through the real spawner.
    _make_sibling_spawner,
    _stamp_interrupt_origins,
)
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop
from tests.conftest import (  # type: ignore[attr-defined]  # Why: the consumer harness's fakes live in conftest.
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)

if TYPE_CHECKING:
    from taskq.testing.actor import FakeBackend as _FakeBackend

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()

# Tick classification for the scripted pool: OSError is the transient arm
# (connection loss), RuntimeError the unexpected arm.
TRANSIENT = "transient"
UNEXPECTED = "unexpected"
SUCCESS = "success"


class _FakeTickConn:
    """Minimal asyncpg.Connection stand-in for one clean heartbeat tick."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 1"

    async def fetch(self, _sql: str, *_args: object) -> list[dict[str, object]]:
        return []

    async def close(self) -> None:
        return None

    def terminate(self) -> None:
        return None

    def transaction(self) -> "_FakeTickTransaction":
        return _FakeTickTransaction()


class _FakeTickTransaction:
    def __init__(self) -> None:
        self.started = False
        self.committed = False
        self.rolled_back = False

    async def start(self) -> None:
        self.started = True

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class _ScriptedPool:
    """One scripted kind (or a clean tick) per pool acquire: one tick of
    the heartbeat loop per entry. After the script runs out, ticks succeed."""

    def __init__(self, script: list[str]) -> None:
        self._script = list(script)
        self.acquire_count = 0

    @contextlib.asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeTickConn, None]:
        del timeout
        self.acquire_count += 1
        kind = self._script.pop(0) if self._script else SUCCESS
        if kind == TRANSIENT:
            raise OSError("simulated connection loss")
        if kind == UNEXPECTED:
            raise RuntimeError("simulated driver contract violation")
        yield _FakeTickConn()

    @property
    def ticks_consumed(self) -> int:
        return self.acquire_count


def _settings(max_heartbeat_failures: int = 3) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            # Tier 1 must be able to fire before the terminal tier; both
            # knobs must sit inside the lock lease (settings validation).
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_LOCK_LEASE": "18.0",
            "TASKQ_MAX_HEARTBEAT_FAILURES": str(max_heartbeat_failures),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "2.0",
        }
    )


def _heartbeat_deps(pool: _ScriptedPool, max_heartbeat_failures: int) -> WorkerDeps:
    deps = MagicMock(spec=WorkerDeps)
    deps.settings = _settings(max_heartbeat_failures)
    deps.heartbeat_pool = pool  # type: ignore[assignment]
    deps.disowned_jobs = set()
    deps.heartbeat_failures = 0
    deps.is_leader = asyncio.Event()
    deps.liveness = MagicMock()
    deps.stall_tally = MagicMock()
    deps.stall_tally.metadata_value.return_value = {}
    deps.progress_buffers = {}
    deps.shutdown_started_at = None
    deps.producer_stop_event = asyncio.Event()
    deps.shutdown_phase = 0
    return deps


async def _drive_worker(
    deps: WorkerDeps,
    pool: _ScriptedPool,
    shutdown: asyncio.Event,
    *,
    script_length: int,
    isolate_sink: list[int],
) -> bool:
    """Run the heartbeat loop; return True iff the worker isolated. The
    isolate observation point is the worker's own isolate action."""
    import taskq.worker.heartbeat as hb_mod

    async def _recording_isolate(
        _deps: WorkerDeps, _worker_id: object, _shutdown: asyncio.Event
    ) -> None:
        isolate_sink.append(pool.ticks_consumed)
        _shutdown.set()

    saved = hb_mod.isolate_self
    hb_mod.isolate_self = _recording_isolate  # type: ignore[method-assign]
    task = asyncio.create_task(heartbeat_loop(deps, _WORKER_ID, shutdown))
    try:
        deadline = asyncio.get_running_loop().time() + 20.0
        while not isolate_sink and pool.ticks_consumed < script_length:
            if asyncio.get_running_loop().time() > deadline:
                break
            await asyncio.sleep(0.01)
        # Grace: the final scripted tick's ledger decision settles inside
        # the same tick; give the loop a beat to act on it.
        await asyncio.sleep(0.1)
        if isolate_sink:
            await asyncio.wait_for(task, timeout=5.0)
            return True
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return False
    finally:
        hb_mod.isolate_self = saved  # type: ignore[method-assign]


async def _run_script(
    script: list[str], *, max_heartbeat_failures: int = 3
) -> tuple[int, bool, int]:
    pool = _ScriptedPool(script)
    shutdown = asyncio.Event()
    isolate_sink: list[int] = []
    isolated = await _drive_worker(
        _heartbeat_deps(pool, max_heartbeat_failures),
        pool,
        shutdown,
        script_length=len(script),
        isolate_sink=isolate_sink,
    )
    return pool.ticks_consumed, isolated, isolate_sink[0] if isolate_sink else -1


async def test_transient_failures_alone_isolate_at_the_documented_tick() -> None:
    """F transient failures (connection loss) alone: the worker isolates on
    the (F+1)-th consecutive failed tick - the documented cadence."""
    ticks, isolated, at = await _run_script([TRANSIENT] * 4, max_heartbeat_failures=3)
    assert isolated, "RED: dead-PG connection loss never isolated the worker"
    assert at == 4, f"RED: isolation fired at tick {at}, the documented tick is 4"
    assert ticks == 4


async def test_unexpected_failures_alone_isolate_at_the_documented_tick() -> None:
    """F unexpected failures (a REVOKE'd UPDATE, a driver contract
    violation) alone: the same ledger, the same tick. The pre-fix
    asymmetric ledger looped here forever - the zombie this PR closes."""
    ticks, isolated, at = await _run_script([UNEXPECTED] * 4, max_heartbeat_failures=3)
    assert isolated, (
        "RED: a persistent non-transient fault failed every tick without "
        "isolating the worker; the lock expired under every running job"
    )
    assert at == 4, f"RED: isolation fired at tick {at}, the documented tick is 4"
    assert ticks == 4


@pytest.mark.parametrize(
    ("label", "script"),
    [
        ("unexpected-then-transient", [UNEXPECTED] * 3 + [TRANSIENT]),
        ("transient-then-unexpected", [TRANSIENT] * 3 + [UNEXPECTED]),
        ("interleaved", [UNEXPECTED, TRANSIENT, UNEXPECTED, TRANSIENT]),
    ],
)
async def test_mixed_failure_classes_isolate_on_the_consecutive_run(
    label: str, script: list[str]
) -> None:
    """The ledger is ONE threshold: (F) mixed failures isolate at tick F+1
    whatever the class mix and order."""
    _ticks, isolated, at = await _run_script(script, max_heartbeat_failures=3)
    assert isolated, f"RED ({label}): the mixed run never isolated"
    assert at == 4, (
        f"RED ({label}): isolation fired at tick {at}, the documented tick "
        "is 4 - the failure classes must share one consecutive run"
    )


async def test_a_successful_tick_resets_the_ledger() -> None:
    """A fully successful tick resets the consecutive run: two failures, a
    recovery, two more failures must NEVER isolate (the longest run is 2
    against F=3), and the worker must still be beating."""
    script = [UNEXPECTED, TRANSIENT, SUCCESS, UNEXPECTED, TRANSIENT]
    ticks, isolated, _at = await _run_script(script, max_heartbeat_failures=3)
    assert not isolated, (
        f"RED: isolation fired after {ticks} ticks with a recovery in the "
        "middle - failed ticks are outliving a recovery"
    )
    assert ticks >= 5, "attack broken: the script's ticks did not all run"


async def test_recovery_then_a_fresh_run_isolates_on_its_own_cadence() -> None:
    """After a reset the run starts over: F fresh failures still do not
    isolate (short by one), the (F+1)-th does."""
    script = [SUCCESS, UNEXPECTED, UNEXPECTED, UNEXPECTED, UNEXPECTED]
    _ticks, isolated, at = await _run_script(script, max_heartbeat_failures=3)
    assert isolated, "RED: a full consecutive run after recovery never isolated"
    assert at == 5, (
        f"RED: isolation fired at tick {at}; the run began after the "
        "recovery, the documented tick is 5"
    )


async def test_in_tx_hook_failures_count_once_per_tick() -> None:
    """A failing cancel hook is a failed tick, counted ONCE: F consecutive
    hook failures must isolate on the (F+1)-th, not a tick early (a double
    count) and never at all (the old carve-out)."""
    import taskq.worker.heartbeat as hb_mod

    pool = _ScriptedPool([])
    shutdown = asyncio.Event()
    isolate_sink: list[int] = []
    deps = _heartbeat_deps(pool, max_heartbeat_failures=3)

    class _HookFails:
        async def run_in_tx(self, _conn: object) -> None:
            raise OSError("cancel-poll statement lost its connection")

        async def run_post_tx(self) -> None:
            return None

    async def _recording_isolate(
        _deps: WorkerDeps, _worker_id: object, _shutdown: asyncio.Event
    ) -> None:
        isolate_sink.append(pool.ticks_consumed)
        _shutdown.set()

    saved = hb_mod.isolate_self
    hb_mod.isolate_self = _recording_isolate  # type: ignore[method-assign]
    try:
        task = asyncio.create_task(
            heartbeat_loop(deps, _WORKER_ID, shutdown, cancel_controller=_HookFails())  # type: ignore[arg-type]
        )
        deadline = asyncio.get_running_loop().time() + 20.0
        while not isolate_sink and pool.ticks_consumed < 10:
            if asyncio.get_running_loop().time() > deadline:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)
        if isolate_sink:
            await asyncio.wait_for(task, timeout=5.0)
        else:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        hb_mod.isolate_self = saved  # type: ignore[method-assign]

    assert isolate_sink, (
        "RED: the in-tx hook failure carve-out looped forever without "
        "isolating - every lease expired under a healthy-looking worker"
    )
    assert isolate_sink[0] == 4, (
        f"RED: hook failures isolated at tick {isolate_sink[0]}; each failed "
        "tick must count exactly once, the documented tick is 4"
    )


# ── The sibling-crash stamp window ──────────────────────────────────────


class _Entry:
    """The minimal registry-entry surface the crash stamp reads."""

    def __init__(self, origin: CancelOrigin) -> None:
        self.cancel_origin = origin
        stamps: list[CancelOrigin] = []
        self.ctx = SimpleNamespace(_set_cancel_origin=stamps.append)
        self.stamps = stamps


def _crash_deps(registry: ActiveJobRegistry) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.settings = _settings()
    deps.disowned_jobs = set()
    deps.progress_buffers = {}
    deps.worker_pool = None
    deps.redis_client = None
    deps.shutdown_started_at = None
    deps.active_jobs = registry
    return deps


async def _crash_a_sibling(deps: MagicMock, shutdown_event: asyncio.Event) -> None:
    with contextlib.suppress(BaseExceptionGroup):

        async def crashing() -> None:
            raise RuntimeError("leader sweep hit a dead PG")

        async with asyncio.TaskGroup() as tg:
            spawn = _make_sibling_spawner(tg, shutdown_event, deps)
            spawn(crashing())


async def test_every_pre_stamp_claim_is_stamped_the_bound_holds() -> None:
    """Looped: entries registered BEFORE the crash are always stamped
    SHUTDOWN. This is the bounded-window claim: the exposure is bounded to
    claims racing the stamp, so claims that already landed must never be
    left origin-less (their teardown cancellation would phantom-cancel)."""
    for i in range(25):
        registry = ActiveJobRegistry()
        entry = _Entry(CancelOrigin.NONE)
        registry._by_id[new_uuid()] = entry  # type: ignore[index-assign]  # Why: unit test injects a minimal entry; register() needs a full JobContext the fake replaces.
        shutdown_event = asyncio.Event()
        deps = _crash_deps(registry)
        await _crash_a_sibling(deps, shutdown_event)
        assert entry.cancel_origin is CancelOrigin.SHUTDOWN, (
            f"RED iter {i}: a claim that landed before the stamp was left "
            "origin-less - its teardown cancellation would phantom-cancel it"
        )
        assert shutdown_event.is_set()


async def test_pre_stamp_claims_interrupt_never_phantom_cancel() -> None:
    """The observable end-to-end pin, looped: a job claimed and running
    when the sibling crashes is released to the fleet (the interrupt
    route: the backend records the interrupted release) and the backend
    never records a cancel write for it."""
    for i in range(15):
        backend: _FakeBackend = FakeBackend()
        settings = _settings()
        registry = ActiveJobRegistry()
        deps = _crash_deps(registry)
        deps.settings = settings
        job = make_job_row()
        crash_go = asyncio.Event()
        shutdown_event = asyncio.Event()

        async def body(_running: object, _ctx: object) -> object:
            await asyncio.sleep(3600)
            return {"unreachable": True}

        async def consumer() -> None:
            await consume_one_job(
                as_backend(backend),
                job,
                _WORKER_ID,
                deps=deps,  # type: ignore[arg-type]
                run_actor=body,  # type: ignore[arg-type]
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
                await wait_for_condition(
                    lambda: registry.get(job.id) is not None,
                    description="the consumer's registration of the job",
                    timeout=5.0,
                )
                crash_go.set()
        except BaseExceptionGroup:
            pass

        interrupted = [
            call
            for call in backend.mark_interrupted_calls  # type: ignore[union-attr]
            if call["job_id"] == job.id
        ]
        cancelled = [
            call
            for call in backend.mark_cancelled_calls  # type: ignore[union-attr]
            if call["job_id"] == job.id
        ]
        assert interrupted, (
            f"RED iter {i}: a job claimed before the crash was not released "
            "to the fleet (the interrupt route never ran)"
        )
        assert not cancelled, (
            f"RED iter {i}: a job claimed before the crash terminalised as a "
            "cancel - a phantom operator cancel no operator issued"
        )


async def test_the_stamp_window_residual_admits_no_phantom_cancel() -> None:
    """The window probe, looped: a consumer parked across the crash's stamp
    (its registration completes, if at all, only after the stamp ran and
    the group's cancellation sweep began) must never produce a phantom
    cancel. The stamp runs synchronously ahead of the TaskGroup's
    cancellation sweep, so the stamp-to-delivery gap holds no scheduling
    point for a TaskGroup-sibling consumer: the observed fate of the
    racing claim is either the interrupt release or no write at all (the
    lease-expiry sweep's documented backstop), and the two terminal
    routes never double-fire on one dispatch."""
    for i in range(25):
        backend: _FakeBackend = FakeBackend()
        settings = _settings()
        registry = ActiveJobRegistry()
        deps = _crash_deps(registry)
        deps.settings = settings
        job = make_job_row()
        shutdown_event = asyncio.Event()
        stamped = asyncio.Event()
        real_register = registry.register

        async def gated_register(job_id: object) -> object:
            # A fresh claim completing its registration, if it completes at
            # all, exactly in the stamp-to-delivery gap: the crash arm's
            # stamp has run, the consumer's own cancellation has not been
            # observed yet.
            await stamped.wait()
            await asyncio.sleep(0)
            return await real_register(job_id)  # type: ignore[arg-type]

        registry.register = gated_register  # type: ignore[method-assign]

        async def body(_running: object, _ctx: object) -> object:
            await asyncio.sleep(3600)
            return {"unreachable": True}

        async def consumer() -> None:
            await consume_one_job(
                as_backend(backend),
                job,
                _WORKER_ID,
                deps=deps,  # type: ignore[arg-type]
                run_actor=body,  # type: ignore[arg-type]
                actor_config=default_actor_config(),
                payload_type=EmptyPayload,
                clock=FakeClock(_NOW),
                active_jobs=registry,
            )

        async def crashing() -> None:
            # The crash arm's stamp: the registry is empty at this instant,
            # so the fresh claim behind the gate is the one dispatch the
            # bound admits.
            _stamp_interrupt_origins(deps)
            stamped.set()
            shutdown_event.set()
            raise RuntimeError("leader sweep hit a dead PG")

        try:
            async with asyncio.TaskGroup() as tg:
                spawn = _make_sibling_spawner(tg, shutdown_event, deps)
                spawn(consumer())
                spawn(crashing())
        except BaseExceptionGroup:
            pass

        cancelled = [
            call
            for call in backend.mark_cancelled_calls  # type: ignore[union-attr]
            if call["job_id"] == job.id
        ]
        interrupted = [
            call
            for call in backend.mark_interrupted_calls  # type: ignore[union-attr]
            if call["job_id"] == job.id
        ]
        assert not cancelled, (
            f"RED iter {i}: a claim racing the stamp terminalised as a cancel "
            "- a phantom operator cancel no operator issued"
        )
        assert not (interrupted and cancelled), (
            f"RED iter {i}: two terminal writes for one dispatch"
        )
