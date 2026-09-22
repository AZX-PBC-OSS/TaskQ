"""The producer loop's unexpected-error budget.

The producer is a long-lived loop that awaits Postgres every round, and
every other such loop (the five leader maintenance loops, the sweep
loop) carries the ``UnexpectedLoopErrorGuard`` backstop: errors OUTSIDE
the transient set (a revoked grant, a code bug, a data error) are
tolerated and logged loudly for a few consecutive rounds, then
deliberately fatal, never an infinite silent retry. Before the fix the
producer's non-transient arm logged-and-continued forever: a worker
whose dispatching role had its UPDATE on jobs revoked kept ticking,
kept its liveness registration fresh, and kept ``/ready`` green while
claiming nothing, a functional zombie no detector could see (the loop's
first statement is the liveness tick). The pins below drive the REAL
loop over a raising backend, the way an operator observes it: the
injected permanent fault must raise the ORIGINAL error out of the loop
at ``DEFAULT_MAX_CONSECUTIVE_UNEXPECTED`` consecutive rounds, the
counter must fire per occurrence with the loop label, transient
failures must stay retry-next-tick forever (they never feed the
budget), and a successful round must reset the streak.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.testing.otel import counter_data_points
from taskq.worker import run as run_mod
from taskq.worker._transient import DEFAULT_MAX_CONSECUTIVE_UNEXPECTED
from taskq.worker.run import producer_loop
from tests._ns_patch import module_ns_proxy

_LOOP_LABEL = "worker.producer"
_UNEXPECTED_COUNTER = "taskq.worker.loop_unexpected_errors_total"


class _NoopPool:
    """asyncpg.Pool stand-in; the producer's exit hand-back is a no-op."""

    class _Conn:
        async def __aenter__(self) -> _NoopPool._Conn:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def execute(self, *_args: object) -> str:
            return "UPDATE 0"

    def acquire(self, *, timeout: float | None = None) -> _NoopPool._Conn:
        return self._Conn()


def _producer_deps(*, poll_interval: float = 0.05) -> SimpleNamespace:
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=poll_interval,
        notify_poll_interval=poll_interval,
        max_concurrency=1,
        schema_name="taskq",
        pg_is_pooled=False,
    )
    liveness = SimpleNamespace(tick=lambda *args, **kwargs: None, forget=lambda *a, **k: None)
    return SimpleNamespace(
        settings=settings,
        liveness=liveness,
        active_jobs=SimpleNamespace(all=list, count=lambda: 0),
        disowned_jobs=set(),
        dispatcher_pool=_NoopPool(),
    )


class _RaisingBackend:
    """A backend whose dispatch_batch raises a scriptable exception.

    ``failures`` counts failed rounds; ``script`` is either an exception
    instance raised every round or a per-round callable returning one
    (None means the round succeeds).
    """

    def __init__(self, script: Any) -> None:
        self.script = script
        self.failures = 0
        self.successes = 0

    async def dispatch_batch(self, **_kwargs: Any) -> list[Any]:
        script = cast("BaseException | Callable[[], BaseException | None]", self.script)
        exc: BaseException | None = script() if callable(script) else script
        if exc is None:
            self.successes += 1
            return []
        self.failures += 1
        raise exc


class _CleanExit:
    """Marker for a producer_loop that returned without raising."""


CleanExit = _CleanExit()


async def _drive_until(
    monkeypatch: pytest.MonkeyPatch,
    backend: _RaisingBackend,
    *,
    stop_after_rounds: int,
) -> list[BaseException]:
    """Run producer_loop to completion; return the escaped error, or the
    _CleanExit marker on a clean return. The patched sleep is the round counter's clock seam: it
    never actually waits, and sets the stop flag after the round cap so
    a loop that never raises still terminates deterministically."""
    outcomes: list[BaseException] = []
    real_sleep = asyncio.sleep
    stop_event = asyncio.Event()

    async def _round_counting_sleep(_delay: float, result: object = None) -> object:
        if backend.failures + backend.successes >= stop_after_rounds:
            stop_event.set()
        await real_sleep(0)
        return result

    # Patch where the name is LOOKED UP - run.py's own ``asyncio`` binding -
    # not through to the global asyncio module (tests/_ns_patch.py).
    monkeypatch.setattr(run_mod, "asyncio", module_ns_proxy(asyncio, sleep=_round_counting_sleep))

    async def _drive() -> None:
        try:
            await producer_loop(
                _producer_deps(),  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in for WorkerDeps, the established producer-loop unit pattern.
                asyncio.Queue(maxsize=1),
                asyncio.Event(),  # shutdown_event, stays clear
                stop_event,
                backend=cast(Backend, backend),
                worker_id=new_uuid(),
            )
            outcomes.append(CleanExit)
        except BaseException as exc:  # Why: the pin asserts on the escaped error itself.
            outcomes.append(exc)

    await _drive()
    return outcomes


async def test_permanent_dispatch_fault_raises_after_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-transient dispatch failure must not retry forever.

    A permanent fault (here ValueError, the shape a code bug or a data
    error takes; the repro tier drives the same shape with a real
    InsufficientPrivilegeError against PostgreSQL 18) is tolerated for
    ``DEFAULT_MAX_CONSECUTIVE_UNEXPECTED`` consecutive rounds, then the
    ORIGINAL error propagates out of the loop and tears the worker down
    deliberately, exactly like every leader maintenance loop.
    """
    backend = _RaisingBackend(ValueError("a permanent fault, not a PG moment"))

    outcomes = await _drive_until(monkeypatch, backend, stop_after_rounds=64)

    escaped = outcomes[0]
    assert not isinstance(escaped, _CleanExit), (
        f"producer_loop returned cleanly after {backend.failures} failed "
        "rounds: a permanent dispatch fault retried forever with the loop "
        "ticking and /ready green the whole time (a functional zombie no "
        "detector can see)"
    )
    assert type(escaped) is ValueError, f"the ORIGINAL error must escape, got {escaped!r}"
    assert backend.failures == DEFAULT_MAX_CONSECUTIVE_UNEXPECTED, (
        f"the raise must land at the budget, got {backend.failures} failed rounds"
    )


async def test_transient_dispatch_failures_never_feed_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TimeoutError is in TRANSIENT_PG_ERRORS: the round degrades to a
    warning and retries next tick, indefinitely, and must never consume
    the unexpected-error budget. A slow database is a retry-next-tick
    condition, not a crash-looping one."""
    backend = _RaisingBackend(TimeoutError("client command_timeout"))

    outcomes = await _drive_until(monkeypatch, backend, stop_after_rounds=8)

    escaped = outcomes[0]
    assert isinstance(escaped, _CleanExit), (
        f"transient failures must stay retry-next-tick, got {escaped!r}"
    )
    assert backend.failures >= 8


async def test_successful_round_resets_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a fully successful round resets the streak: four faults, one
    clean round, four more must stay inside a budget of five, the same
    reset semantics every leader loop pins."""
    state = {"rounds": 0}

    def _script() -> BaseException | None:
        state["rounds"] += 1
        if state["rounds"] % 5 == 0:
            return None  # every fifth round succeeds, resetting the streak
        return ValueError("an intermittent bug")

    backend = _RaisingBackend(_script)

    outcomes = await _drive_until(monkeypatch, backend, stop_after_rounds=20)

    assert isinstance(outcomes[0], _CleanExit), (
        f"a success between faults must reset the streak, got {outcomes[0]!r} "
        f"after {backend.failures} failures and {backend.successes} successes"
    )
    assert backend.successes >= 4


async def test_unexpected_budget_fires_the_loop_labelled_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every tolerated occurrence fires the unexpected-error counter with
    the loop label, the operator's alert surface: identical semantics to
    the leader loops' backstop, one metric family, labelled by loop."""
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.worker._transient as transient_mod

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("test")
    monkeypatch.setattr(
        transient_mod,
        "_unexpected_loop_errors",
        meter.create_counter(_UNEXPECTED_COUNTER, unit="1"),
    )

    backend = _RaisingBackend(ValueError("a permanent fault, not a PG moment"))
    outcomes = await _drive_until(monkeypatch, backend, stop_after_rounds=64)

    assert isinstance(outcomes[0], ValueError), f"expected the budget raise, got {outcomes[0]!r}"
    points = counter_data_points(reader, _UNEXPECTED_COUNTER)
    by_loop = {dict(p.attributes or {})["loop"]: p.value for p in points}
    assert by_loop.get(_LOOP_LABEL) == DEFAULT_MAX_CONSECUTIVE_UNEXPECTED, (
        f"the counter must fire once per tolerated occurrence under "
        f"loop={_LOOP_LABEL!r}, got {by_loop}"
    )
