"""Attack suite for PR 378 (hunt/loop-liveness).

Every test here is an executed attack against the PUSHED branch state,
asserting only what an operator or client observes:

1. Budget semantics - the worker loop's contract: after the documented
   budget of non-transient dispatch failures the loop RAISES the original
   error (tearing the worker down deliberately); transient failures never
   isolate the worker; the operator's alert surface
   (``taskq.worker.loop_unexpected_errors_total``) fires exactly once per
   tolerated occurrence, labelled by loop.
2. Metric truth - an operator watching the public metrics surface sees a
   round's pool wait in ``taskq.dispatch.pool_acquire_duration`` and NOT
   in ``taskq.dispatch.duration``; SQL latency only in the latter;
   ``taskq.dispatch.failures`` names the failing stage's error class.
3. Cancel/shutdown interleaving - a cancelled round is a shutdown, not a
   failure: the wait shows up as a wait, the failure counter stays
   silent, and the pool keeps serving (a leaked checkout would hang the
   follow-up rounds these tests run).
4. Pool differential - a dispatch round and a plain ``async with`` on the
   SAME pool must be indistinguishable to their caller: same exception
   (same object), same success, and the same pool health afterwards -
   proven against a REAL asyncpg.Pool (``max_size=1``, so a single leaked
   checkout hangs the follow-up round and the test times out).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch import _dispatch_batch
from taskq.backend._sql_templates import render
from taskq.testing.otel import (
    collect_metrics,
    counter_data_points,
    counter_value,
    histogram_points,
    setup_meter,
)
from taskq.worker import run as run_mod
from taskq.worker._transient import (
    DEFAULT_MAX_CONSECUTIVE_UNEXPECTED,
    PERMANENT_PG_REFUSALS,
    POOLED_TRANSIENT_PG_ERRORS,
    TRANSIENT_PG_ERRORS,
    UnexpectedLoopErrorGuard,
    is_transient_pg_error,
)
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.run import producer_loop

_LOOP_LABEL = "worker.producer"
_UNEXPECTED_COUNTER = "taskq.worker.loop_unexpected_errors_total"
_OLD_UNEXPECTED_NAME = "taskq.worker.leader_loop_unexpected_errors_total"

# The real-pool differential's Postgres; overridable, skipped when absent.
_PG_DSN = os.environ.get(
    "TASKQ_ATTACK_PG_DSN", "postgres://postgres:postgres@127.0.0.1:45432/postgres"
)
# Per-process unique: the real_pg_pool fixture is module-scoped and xdist
# may split this module's tests across workers, each instantiating the
# fixture; a shared hard-coded name makes one worker's DROP ... CASCADE
# teardown (and setup) remove the schema another worker's pool is using
# (InvalidSchemaNameError under -n, intermittently). Same convention as
# test_worker_bootstrap.py's twb_{new_base62()} label.
_PG_SCHEMA = f"liveness_{new_base62().lower()}"


# ─────────────────────────────────────────────────────────────────────────
# Shared drive harness: the REAL producer loop over a scripted backend.
# ─────────────────────────────────────────────────────────────────────────


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


class _ScriptedBackend:
    """dispatch_batch pops one script item per round.

    A script item is an exception instance (raised verbatim - the loop
    classifies it), ``None`` (a successful, empty round), or a list of
    job-like rows (a claimed round). When the script is exhausted,
    rounds succeed.
    """

    def __init__(self, script: list[Any]) -> None:
        self.script = list(script)
        self.rounds = 0
        self.failures = 0
        self.successes = 0

    async def dispatch_batch(self, **_kwargs: Any) -> list[Any]:
        self.rounds += 1
        item = self.script.pop(0) if self.script else None
        if item is None:
            self.successes += 1
            return []
        if isinstance(item, BaseException):
            self.failures += 1
            raise item
        return item  # a claimed round: a list of job-like rows


class _CleanExit:
    """Marker for a producer_loop that returned without raising."""


CleanExit = _CleanExit()


def _producer_deps(*, poll_interval: float = 0.005, pooled: bool = False) -> SimpleNamespace:
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=poll_interval,
        notify_poll_interval=poll_interval,
        max_concurrency=1,
        schema_name="taskq",
        pg_is_pooled=pooled,
    )
    liveness = SimpleNamespace(tick=lambda *a, **k: None, forget=lambda *a, **k: None)
    return SimpleNamespace(
        settings=settings,
        liveness=liveness,
        active_jobs=SimpleNamespace(
            all=list,
            count=lambda: 0,
            # The producer's enqueue path stamps the queued map on the
            # registry (run.py mark_enqueued); the stub tracks nothing,
            # but the call must land - a real registry's bound method,
            # the same shape test_producer_claim_cooldown.py uses.
            mark_enqueued=ActiveJobRegistry().mark_enqueued,
        ),
        disowned_jobs=set(),
        dispatcher_pool=_NoopPool(),
    )


def _patch_counter(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Swap the guard's counter for a test-metered one; return the reader."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("attack")
    import taskq.worker._transient as transient_mod

    monkeypatch.setattr(
        transient_mod,
        "_unexpected_loop_errors",
        meter.create_counter(_UNEXPECTED_COUNTER, unit="1"),
    )
    return reader


def _patch_budget(monkeypatch: pytest.MonkeyPatch, budget: int) -> None:
    import taskq.worker._transient as transient_mod

    monkeypatch.setattr(transient_mod, "DEFAULT_MAX_CONSECUTIVE_UNEXPECTED", budget)


async def _run_producer(
    monkeypatch: pytest.MonkeyPatch,
    backend: Any,
    *,
    stop_after_rounds: int,
    pooled: bool = False,
    hook: Any = None,
    stop_event: asyncio.Event | None = None,
    task_out: list[asyncio.Task[None]] | None = None,
) -> BaseException | _CleanExit:
    """Run producer_loop to completion in a background task.

    Returns what the loop's caller would observe: the escaping exception,
    or the clean-exit marker. ``hook``, given, replaces ``asyncio.sleep``
    (the round clock seam) inside the loop's task; ``task_out``, given,
    receives the loop's task so a test can cancel it mid-flight. The
    round-cap stop check ALWAYS runs (even under a hook) so a hook that
    swallows rounds cannot park the loop forever; pass ``stop_event`` to
    share it with the hook.
    """
    stop_event = stop_event if stop_event is not None else asyncio.Event()
    outcomes: list[BaseException | _CleanExit] = []
    real_sleep = asyncio.sleep
    # A small queue with headroom: a claimed round must not fill it, or
    # the loop's slot-refill branch (which never calls asyncio.sleep)
    # starves the round clock.
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=4)

    async def _round_clock(_delay: float, result: object = None) -> object:
        if backend.rounds >= stop_after_rounds:
            stop_event.set()
        # Stand in for the consumers: a claimed round's job leaves the
        # local queue, freeing the slot for the next claim round.
        with contextlib.suppress(asyncio.QueueEmpty):
            queue.get_nowait()
        if hook is not None:
            return await hook(_delay, result, real_sleep)
        await real_sleep(0)
        return result

    # Patch through monkeypatch (restored by the fixture even when the
    # test aborts) - a leaked module-global asyncio.sleep poisons every
    # later test in the process. run_mod.asyncio IS the asyncio module.
    monkeypatch.setattr(run_mod.asyncio, "sleep", _round_clock)  # pyright: ignore[reportPrivateImportUsage]  # Why: the shipped budget pins use the same seam (tests/test_producer_loop_unexpected_budget.py).

    async def _drive() -> None:
        try:
            await producer_loop(
                _producer_deps(pooled=pooled),
                queue,
                asyncio.Event(),
                stop_event,
                backend=backend,
                worker_id=new_uuid(),
            )
            outcomes.append(CleanExit)
        except BaseException as exc:
            outcomes.append(exc)

    task = asyncio.create_task(_drive())
    if task_out is not None:
        task_out.append(task)
    await task
    return outcomes[0]


def _counter_by_loop(reader: InMemoryMetricReader) -> dict[Any, float]:
    return {  # type: ignore[return-value]  # Why: the loop label is a str at every emit site.
        dict(p.attributes or {})["loop"]: p.value
        for p in counter_data_points(reader, _UNEXPECTED_COUNTER)
    }


# ─────────────────────────────────────────────────────────────────────────
# 1. Budget semantics - the worker raises after the documented budget;
#    transient failures never isolate the worker.
# ─────────────────────────────────────────────────────────────────────────


def _transient_instances() -> list[tuple[str, BaseException]]:
    return [(cls.__name__, cls("simulated transient")) for cls in TRANSIENT_PG_ERRORS]


async def test_exhaustive_transient_set_never_feeds_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every class in TRANSIENT_PG_ERRORS, driven through the REAL loop,
    must retry-next-tick forever: the worker survives, the alert counter
    stays at zero, and the classification predicate agrees with the
    behavior for every member (no tuple-vs-arm drift)."""
    _patch_budget(monkeypatch, 2)  # a misclassification fires at round 2
    reader = _patch_counter(monkeypatch)
    for name, exc in _transient_instances():
        backend = _ScriptedBackend([exc, exc, exc])
        outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=3)
        assert isinstance(outcome, _CleanExit), (
            f"{name} was classified non-transient by the producer loop "
            f"(worker crashed with {outcome!r}): a PG blip consumed the budget"
        )
        assert is_transient_pg_error(exc, pooled=False), (
            f"{name} behaves transient in the loop but the predicate "
            "disagrees: classification drift between the tuple and the arm"
        )
    assert counter_value(reader, _UNEXPECTED_COUNTER) == 0, (
        "some transient class incremented the unexpected-error counter"
    )


async def test_permanent_refusals_and_unpooled_statement_errors_feed_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The non-transient side of the classification, exhaustively:
    PERMANENT_REFUSALS, and the pooled-only statement errors on a DIRECT
    connection (pooled=False), must tear the worker down at the budget
    with the ORIGINAL error; pooled=True must flip the statement errors
    back to survivable."""
    reader = _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 2)
    permanent: list[tuple[str, BaseException]] = [
        (cls.__name__, cls("simulated refusal")) for cls in PERMANENT_PG_REFUSALS
    ]
    permanent += [
        (cls.__name__, cls("simulated pooler artifact")) for cls in POOLED_TRANSIENT_PG_ERRORS
    ]
    for name, exc in permanent:
        backend = _ScriptedBackend([exc, exc])  # budget 2: two faults needed
        outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=8)
        assert isinstance(outcome, BaseException) and not isinstance(outcome, _CleanExit), (
            f"{name} (pooled=False) never consumed the budget: misclassified "
            "as transient, a permanent fault retries forever"
        )
        assert outcome is exc, f"{name}: the ORIGINAL error object must escape, got {outcome!r}"
        assert backend.rounds == 2, (
            f"{name}: budget must fire at the cap (2), fired at round {backend.rounds}"
        )
    assert counter_value(reader, _UNEXPECTED_COUNTER) == 2 * len(permanent), (
        "the counter must fire exactly once per tolerated occurrence"
    )
    pooled_before = counter_value(reader, _UNEXPECTED_COUNTER)
    for cls in POOLED_TRANSIENT_PG_ERRORS:
        exc = cls("simulated pooler artifact")
        backend = _ScriptedBackend([exc, exc, exc])
        outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=3, pooled=True)
        assert isinstance(outcome, _CleanExit), (
            f"{cls.__name__} (pooled=True) consumed the budget: the operator's "
            "TASKQ_PG_IS_POOLED declaration must extend the transient set"
        )
    assert counter_value(reader, _UNEXPECTED_COUNTER) == pooled_before, (
        "the pooled=True rounds incremented the unexpected-error counter"
    )


async def test_interleaved_orders_raise_at_exactly_the_nth_non_transient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Interleavings of transient (T) and non-transient (N) failures in
    every order: the worker crashes on the round carrying the Nth
    non-transient failure - transients neither count nor reset - with the
    Nth error object as the crash and the alert counter exact."""
    reader = _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 3)
    cases: list[tuple[str, str, int, int]] = [
        # (label, script, expected raise round (1-based), expected counter)
        ("N,N,N", "NNN", 3, 3),
        ("T,N,T,N,N", "TNTNN", 5, 3),
        ("N,T,T,N,T,N", "NTTNTN", 6, 3),
        ("N,N,T,N,N", "NNTNN", 4, 3),  # a transient between faults must NOT reset
        ("T,T,N,N,N", "TTNNN", 5, 3),
        ("N,T,N,T,N", "NTNTN", 5, 3),
    ]
    for label, spec, expected_round, expected_count in cases:
        script: list[Any] = []
        n_instances: list[BaseException] = []
        for ch in spec:
            if ch == "T":
                script.append(TimeoutError("simulated blip"))
            else:
                exc = ValueError(f"permanent fault ({label})")
                n_instances.append(exc)
                script.append(exc)
        backend = _ScriptedBackend(script)
        before = counter_value(reader, _UNEXPECTED_COUNTER)
        outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=64)
        assert isinstance(outcome, ValueError), (
            f"{label}: expected the budget raise, got {outcome!r}"
        )
        assert outcome is n_instances[expected_count - 1], (
            f"{label}: the crashing error must be the Nth non-transient "
            f"OBJECT itself, got {outcome!r}"
        )
        assert backend.rounds == expected_round, (
            f"{label}: raise must land on round {expected_round}, landed on {backend.rounds}"
        )
        assert counter_value(reader, _UNEXPECTED_COUNTER) - before == expected_count, (
            f"{label}: counter must fire exactly once per tolerated "
            f"occurrence ({expected_count}), got "
            f"{counter_value(reader, _UNEXPECTED_COUNTER) - before}"
        )


async def test_budget_never_fires_on_all_transient_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rotation of EVERY transient class, round after round, must never
    fire: the worker survives them all, the counter silent."""
    _patch_budget(monkeypatch, 2)
    reader = _patch_counter(monkeypatch)
    instances = _transient_instances()
    script = [instances[i % len(instances)][1] for i in range(12)]
    backend = _ScriptedBackend(script)
    outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=12)
    assert isinstance(outcome, _CleanExit), f"all-transient stream raised {outcome!r}"
    assert counter_value(reader, _UNEXPECTED_COUNTER) == 0


async def test_success_round_resets_midstream_and_claimed_rounds_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mid-stream reset semantics: [N,N,OK,N,N,N] crashes on round 6 (the
    3rd N after the reset) - and the OK round is a CLAIMED round (jobs
    flow to the local queue), proving claimed rounds reset the streak
    exactly like empty ones."""
    reader = _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 3)
    ns = [ValueError(f"fault {i}") for i in range(5)]
    job = SimpleNamespace(id=new_uuid())
    backend = _ScriptedBackend([ns[0], ns[1], [job], ns[2], ns[3], ns[4]])
    outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=64)
    assert isinstance(outcome, ValueError) and not isinstance(outcome, _CleanExit), (
        f"expected the budget raise after the mid-stream reset, got {outcome!r}"
    )
    assert outcome is ns[4]
    assert backend.rounds == 6, (
        f"reset must land the raise on round 6, landed on {backend.rounds} "
        "(a broken reset fires early on round 5)"
    )
    assert counter_value(reader, _UNEXPECTED_COUNTER) == 5, (
        "the counter must fire once per tolerated occurrence across the "
        "whole run: two before the reset + three after"
    )


async def test_no_double_fire_at_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """The occurrence that crashes the worker counts exactly once:
    counter == budget, never budget + 1, never budget - 1."""
    reader = _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 3)
    backend = _ScriptedBackend([ValueError("fault") for _ in range(3)])
    outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=64)
    assert isinstance(outcome, ValueError)
    by_loop = _counter_by_loop(reader)
    assert by_loop.get(_LOOP_LABEL) == 3, f"double-fire or under-count: {by_loop}"


# ── stage-blindness: non-transient raised from any round stage ───────────


class _StageFaultConn:
    """Fake conn that raises a scripted non-transient error from a chosen
    stage of the real ``_dispatch_batch`` round: resolve (queue-mode
    resolution), claim (the dispatch CTE), or probe (the claimable-rows
    probe)."""

    def __init__(self, stage: str) -> None:
        self.stage = stage
        self.raised: list[BaseException] = []

    async def fetch(self, sql: str, *args: object) -> list[Any]:
        is_resolve = ".queues WHERE" in sql
        is_claim = len(args) == 5
        is_probe = len(args) == 1 and not is_resolve
        hit = (
            (self.stage == "resolve" and is_resolve)
            or (self.stage == "claim" and is_claim)
            or (self.stage == "probe" and is_probe)
        )
        if hit:
            exc = ValueError(f"permanent fault at the {self.stage} stage")
            self.raised.append(exc)
            raise exc
        return []

    async def execute(self, *_a: object) -> str:
        return "UPDATE 0"

    def terminate(self) -> None: ...


class _TrackingPool:
    """Pool stand-in yielding a fresh conn per acquire."""

    def __init__(self, conn_factory: Any, wait: float = 0.0) -> None:
        self._conn_factory = conn_factory
        self.wait = wait

    def acquire(self, *, timeout: float | None = None) -> Any:
        pool = self

        class _Ctx:
            async def __aenter__(self) -> Any:
                if pool.wait:
                    await asyncio.sleep(pool.wait)
                return pool._conn_factory()

            async def __aexit__(self, et: Any, ev: Any, tb: Any) -> None:
                return None

        return _Ctx()


class _RealDispatchBackend:
    """Backend that runs the REAL ``_dispatch_batch`` against a fake pool -
    the producer loop sees exactly the exceptions the real stage machinery
    raises."""

    def __init__(self, pool: _TrackingPool) -> None:
        self.pool = pool
        self.rounds = 0

    async def dispatch_batch(self, **kwargs: Any) -> list[Any]:
        self.rounds += 1
        return await _dispatch_batch(
            cast(Any, self.pool),
            render("taskq"),
            2,
            5.0,
            "taskq",
            cast(UUID, kwargs["worker_id"]),
            list(cast("list[str]", kwargs["queues"])),
            int(cast(int, kwargs["limit"])),
            timedelta(seconds=30),
            queue_mode_cache=None,
        )


async def test_budget_is_stage_blind_resolve_and_claim_and_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanent fault raised from ANY stage of the real dispatch round
    (resolve, claim, probe) must tear the worker down at the same budget
    with the ORIGINAL object; no stage may escape classification."""
    _patch_budget(monkeypatch, 2)
    reader = _patch_counter(monkeypatch)
    for stage in ("resolve", "claim", "probe"):
        pool = _TrackingPool(lambda stage=stage: _StageFaultConn(stage))
        backend = _RealDispatchBackend(pool)
        before = counter_value(reader, _UNEXPECTED_COUNTER)
        outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=16)
        assert isinstance(outcome, ValueError), (
            f"{stage}-stage permanent fault escaped the budget as {outcome!r}"
        )
        assert backend.rounds == 2, (
            f"{stage}-stage: budget must fire at 2, fired at {backend.rounds}"
        )
        assert counter_value(reader, _UNEXPECTED_COUNTER) - before == 2, (
            f"{stage}-stage: counter must fire once per tolerated occurrence, delta="
            f"{counter_value(reader, _UNEXPECTED_COUNTER) - before}"
        )


# ─────────────────────────────────────────────────────────────────────────
# 2. Metric truth - what an operator watching the metrics surface sees.
# ─────────────────────────────────────────────────────────────────────────


def _ok_conn() -> Any:
    class _OkConn:
        async def fetch(self, _sql: str, *args: object) -> list[Any]:
            return []

        async def execute(self, *_a: object) -> str:
            return "UPDATE 0"

        def terminate(self) -> None: ...

    return _OkConn()


async def test_successful_rounds_known_wait_lands_on_pool_histogram(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pushed doc contract (docs/guides/observability.md):
    ``taskq.dispatch.pool_acquire_duration`` is "Seconds a dispatch round
    spent waiting for a dispatcher connection (pool wait, no SQL)" - no
    failure qualification. A pool-exhausted pod's canonical symptom is a
    multi-second wait that EVENTUALLY SUCCEEDS; an operator must see that
    wait on the pool-acquire series, and ``taskq.dispatch.duration`` must
    carry only the round's SQL time."""
    reader = setup_meter(monkeypatch)
    pool = _TrackingPool(_ok_conn, wait=0.08)
    rows = await _dispatch_batch(
        cast(Any, pool),
        render("taskq"),
        2,
        5.0,
        "taskq",
        UUID(int=1),
        ["default"],
        10,
        timedelta(seconds=30),
        queue_mode_cache=None,
    )
    assert rows == []
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert len(wait_points) == 1, (
        "a successful round's pool-acquire wait recorded ZERO samples on "
        "taskq.dispatch.pool_acquire_duration: the histogram only ever sees "
        "failed waits, so the pool-exhausted pod whose waits eventually "
        "succeed (the motivating case in the instrument's own description) "
        "is invisible"
    )
    assert wait_points[0].count == 1
    assert wait_points[0].sum >= 0.06, (
        f"the recorded wait must capture the known ~80ms wait, got {wait_points[0].sum}"
    )
    sql_points = histogram_points(reader, "taskq.dispatch.duration")
    assert len(sql_points) == 1, "the successful round must record SQL duration exactly once"
    assert counter_value(reader, "taskq.dispatch.failures") == 0


async def test_failed_acquire_known_wait_full_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed acquire after a known-duration wait: the operator sees the
    WAIT on the pool-acquire series (not just a nonzero sample), NOTHING
    on the SQL-latency series, and one failures-counter hit naming the
    error class and queue."""
    reader = setup_meter(monkeypatch)

    def _timeout_ctx() -> Any:
        class _Ctx:
            async def __aenter__(self) -> object:
                await asyncio.sleep(0.08)
                raise TimeoutError("simulated dispatcher pool acquire timeout")

            async def __aexit__(self, *a: object) -> None:
                return None

        return _Ctx()

    pool = SimpleNamespace(acquire=lambda *, timeout=None: _timeout_ctx())
    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            cast(Any, pool),
            render("taskq"),
            2,
            5.0,
            "taskq",
            UUID(int=2),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert len(wait_points) == 1 and wait_points[0].count == 1
    assert wait_points[0].sum >= 0.06, (
        f"the failed wait must record the actual known duration, got {wait_points[0].sum}"
    )
    assert histogram_points(reader, "taskq.dispatch.duration") == [], (
        "a round that never ran SQL recorded on the SQL-latency histogram"
    )
    assert counter_value(reader, "taskq.dispatch.failures") == 1
    points = counter_data_points(reader, "taskq.dispatch.failures")
    attrs = dict(points[0].attributes or {})
    assert attrs.get("error_type") == "TimeoutError", (
        f"the acquire-stage failure must name its class, got {attrs!r}"
    )
    assert attrs.get("queue") == "default"


async def test_mixed_rounds_delta_accounting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two rounds - one failed acquire, one success - must leave exact
    DELTAS on the public series: failures +1, pool wait +2 (one failed,
    one successful), SQL duration +1 (success only)."""
    reader = setup_meter(monkeypatch)

    class _TimeoutCtx:
        async def __aenter__(self) -> object:
            raise TimeoutError("pool exhausted")

        async def __aexit__(self, *a: object) -> None:
            return None

    failing = SimpleNamespace(acquire=lambda *, timeout=None: _TimeoutCtx())
    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            cast(Any, failing),
            render("taskq"),
            2,
            5.0,
            "taskq",
            UUID(int=3),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )

    class _OkCtx:
        async def __aenter__(self) -> object:
            return _ok_conn()

        async def __aexit__(self, *a: object) -> None:
            return None

    ok = SimpleNamespace(acquire=lambda *, timeout=None: _OkCtx())
    await _dispatch_batch(
        cast(Any, ok),
        render("taskq"),
        2,
        5.0,
        "taskq",
        UUID(int=4),
        ["default"],
        10,
        timedelta(seconds=30),
        queue_mode_cache=None,
    )
    assert counter_value(reader, "taskq.dispatch.failures") == 1
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert sum(p.count for p in wait_points) == 2, (
        "exactly one wait sample per round is expected (failed + successful)"
    )
    sql_points = histogram_points(reader, "taskq.dispatch.duration")
    assert sum(p.count for p in sql_points) == 1, (
        "exactly one SQL duration sample: the successful round only"
    )


async def test_cancelled_acquires_no_double_record(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two cancelled-acquire rounds: one wait sample each on the
    pool-acquire series, the failure counter silent - never two samples
    for one round."""
    reader = setup_meter(monkeypatch)

    class _CancelledCtx:
        async def __aenter__(self) -> object:
            raise asyncio.CancelledError()

        async def __aexit__(self, *a: object) -> None:
            return None

    pool = SimpleNamespace(acquire=lambda *, timeout=None: _CancelledCtx())
    for _ in range(2):
        with pytest.raises(asyncio.CancelledError):
            await _dispatch_batch(
                cast(Any, pool),
                render("taskq"),
                2,
                5.0,
                "taskq",
                UUID(int=5),
                ["default"],
                10,
                timedelta(seconds=30),
                queue_mode_cache=None,
            )
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert sum(p.count for p in wait_points) == 2, "double-recorded the cancelled waits"
    assert counter_value(reader, "taskq.dispatch.failures") == 0


async def test_renamed_counter_one_series_no_cross_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The renamed backstop counter: unexpected errors forced from BOTH a
    leader maintenance loop's guard and the REAL producer loop must land
    on ONE series (the new name), correctly labelled per loop, and the
    old ``leader_loop_unexpected_errors_total`` name must not exist."""
    reader = _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 3)
    leader_guard = UnexpectedLoopErrorGuard("leader.cron")
    for _ in range(2):  # below the cap: tolerated, counted, no raise
        with contextlib.suppress(ValueError):
            leader_guard.unexpected(ValueError("leader-side surprise"))
    backend = _ScriptedBackend([ValueError("producer-side surprise") for _ in range(3)])
    outcome = await _run_producer(monkeypatch, backend, stop_after_rounds=16)
    assert isinstance(outcome, ValueError)
    names = {m.name for m in collect_metrics(reader)}
    assert _OLD_UNEXPECTED_NAME not in names, (
        "the old leader-only counter name is still being emitted somewhere"
    )
    assert names == {_UNEXPECTED_COUNTER}, f"unexpected series present: {names}"
    by_loop = _counter_by_loop(reader)
    assert by_loop == {"leader.cron": 2, _LOOP_LABEL: 3}, (
        f"cross-attribution or label drift: {by_loop}"
    )


# ─────────────────────────────────────────────────────────────────────────
# 3. Cancel/shutdown interleaving - a cancelled round is a shutdown, not
#    a failure; the pool keeps serving (a leaked checkout hangs the
#    follow-up round these tests run).
# ─────────────────────────────────────────────────────────────────────────


async def test_cancel_mid_acquire_records_wait_not_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelling a round parked inside the pool acquire: the operator
    sees one wait sample, no failure counter, no SQL-duration sample -
    and the pool still serves the next round."""
    reader = setup_meter(monkeypatch)
    parked = asyncio.Event()

    def _parking_ctx() -> Any:
        class _Ctx:
            async def __aenter__(self) -> object:
                parked.set()
                await asyncio.Event().wait()  # cancelled here
                raise AssertionError("unreachable: cancelled, never completed")

            async def __aexit__(self, *a: object) -> None:
                return None

        return _Ctx()

    pool = SimpleNamespace(acquire=lambda *, timeout=None: _parking_ctx())
    task = asyncio.create_task(
        _dispatch_batch(
            cast(Any, pool),
            render("taskq"),
            2,
            5.0,
            "taskq",
            UUID(int=6),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )
    )
    await parked.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert len(wait_points) == 1 and wait_points[0].count == 1, (
        "the interrupted wait must record exactly once"
    )
    assert counter_value(reader, "taskq.dispatch.failures") == 0, (
        "a cancelled acquire must not count as a dispatch failure"
    )
    assert histogram_points(reader, "taskq.dispatch.duration") == []


async def test_cancel_mid_dispatch_pool_keeps_serving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelling a round parked in the dispatch SQL: the caller sees the
    cancellation, no failure counter, no SQL-duration sample, no
    double-record on the pool series - and the pool still serves a
    follow-up round (a leaked checkout or wedged release would hang it)."""
    reader = setup_meter(monkeypatch)
    pool = _TrackingPool(_ok_conn)
    started = asyncio.Event()

    class _ParkingConn:
        async def fetch(self, _sql: str, *args: object) -> list[Any]:
            started.set()
            await asyncio.Event().wait()  # cancelled here
            return []

        async def execute(self, *_a: object) -> str:
            return "UPDATE 0"

        def terminate(self) -> None: ...

    pool._conn_factory = _ParkingConn
    task = asyncio.create_task(
        _dispatch_batch(
            cast(Any, pool),
            render("taskq"),
            2,
            5.0,
            "taskq",
            UUID(int=7),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert counter_value(reader, "taskq.dispatch.failures") == 0
    assert histogram_points(reader, "taskq.dispatch.duration") == []
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert sum(p.count for p in wait_points) == 1, (
        "the (near-zero) successful wait must record once, not twice"
    )
    # The pool keeps serving: a leaked checkout or wedged release hangs here.
    pool._conn_factory = _ok_conn
    rows = await _dispatch_batch(
        cast(Any, pool),
        render("taskq"),
        2,
        5.0,
        "taskq",
        UUID(int=7),
        ["default"],
        10,
        timedelta(seconds=30),
        queue_mode_cache=None,
    )
    assert rows == []


async def test_cancel_during_failure_sleep_absorbed_no_metric_lie(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation delivered while the loop sleeps off a non-transient
    failure is absorbed by the loop (its documented contract): the counted
    occurrence survives exactly once - no loss, no double-count - and the
    worker still exits cleanly on its stop event."""
    reader = _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 5)
    parked = asyncio.Event()
    stop_event = asyncio.Event()
    task_out: list[asyncio.Task[None]] = []
    backend = _ScriptedBackend([ValueError("fault 1"), None, None])

    async def _hook(_delay: float, _result: object, real_sleep: Any) -> object:
        # The failure arm's sleep: park so the test can cancel the task
        # exactly here (inside the suppress(CancelledError) window). The
        # round-cap stop check runs in _round_clock before this hook.
        if backend.failures >= 1 and not parked.is_set():
            parked.set()
            # The cancel must arrive while this sleep is parked: schedule
            # it from the loop, 50ms out, then park.
            asyncio.get_running_loop().call_later(0.05, lambda: task_out[0].cancel())
            # The loop's suppress catches this cancel and continues; the
            # stop event ends the loop on its own terms.
            with contextlib.suppress(asyncio.CancelledError):
                await real_sleep(3600)
            await real_sleep(0)
            return None
        await real_sleep(0)
        return None

    outcome = await _run_producer(
        monkeypatch,
        backend,
        stop_after_rounds=8,
        hook=_hook,
        stop_event=stop_event,
        task_out=task_out,
    )
    assert parked.is_set(), "the attack never landed (the failure-arm sleep never ran)"
    assert isinstance(outcome, _CleanExit), (
        f"an absorbed cancellation must not tear the loop down: {outcome!r}"
    )
    assert backend.failures == 1
    by_loop = _counter_by_loop(reader)
    assert by_loop.get(_LOOP_LABEL) == 1, (
        f"the absorbed cancel must leave the count exact (1), got {by_loop}"
    )


async def test_cancel_during_round_propagates_no_unexpected_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation landing while the round is parked inside the backend
    ends the worker task as CANCELLED (a shutdown, not a crash) and must
    NOT be counted as an unexpected error: the alert surface stays
    silent."""
    _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 5)
    parked = asyncio.Event()

    class _ParkingBackend:
        def __init__(self) -> None:
            self.rounds = 0

        async def dispatch_batch(self, **_kwargs: Any) -> list[Any]:
            self.rounds += 1
            parked.set()
            await asyncio.Event().wait()  # parked forever; cancelled here by the test
            raise AssertionError("unreachable")

    backend = _ParkingBackend()
    outcomes: list[BaseException | _CleanExit] = []

    async def _drive() -> None:
        try:
            await producer_loop(
                _producer_deps(),
                asyncio.Queue(maxsize=1),
                asyncio.Event(),
                asyncio.Event(),
                backend=cast(Any, backend),
                worker_id=new_uuid(),
            )
            outcomes.append(CleanExit)
        except BaseException as exc:
            outcomes.append(exc)

    task = asyncio.create_task(_drive())
    await parked.wait()
    task.cancel()
    await task
    assert len(outcomes) == 1 and isinstance(outcomes[0], asyncio.CancelledError), (
        f"a cancel landing mid-round must propagate, got {outcomes[0]!r}"
    )
    assert backend.rounds == 1


# ─────────────────────────────────────────────────────────────────────────
# 4. Caller-observable differential: a dispatch round and a plain
#    `async with` on the SAME pool must look identical to their caller.
# ─────────────────────────────────────────────────────────────────────────


def _failing_conn(error: BaseException | None) -> Any:
    class _Conn:
        async def fetch(self, _sql: str, *args: object) -> list[Any]:
            if error is not None:
                raise error
            return []

        async def execute(self, *_a: object) -> str:
            return "UPDATE 0"

        def terminate(self) -> None: ...

    return _Conn()


async def _reference_async_with(pool: _TrackingPool, body_error: BaseException | None) -> None:
    async with pool.acquire(timeout=5.0) as conn:
        if body_error is not None:
            await conn.fetch("SELECT 1")


async def _explicit_dispatch(pool: _TrackingPool) -> list[Any]:
    return await _dispatch_batch(
        cast(Any, pool),
        render("taskq"),
        2,
        5.0,
        "taskq",
        UUID(int=8),
        ["default"],
        10,
        timedelta(seconds=30),
        queue_mode_cache=None,
    )


async def test_round_failure_reaches_the_caller_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A body failure mid-round reaches the caller as the SAME exception
    OBJECT from both forms - no wrapping, no replacement, no swallowing."""
    setup_meter(monkeypatch)
    err = ValueError("the body's permanent fault")

    ref_pool = _TrackingPool(lambda: _failing_conn(err))
    with pytest.raises(ValueError) as ref_info:
        await _reference_async_with(ref_pool, err)
    atk_pool = _TrackingPool(lambda: _failing_conn(err))
    with pytest.raises(ValueError) as atk_info:
        await _explicit_dispatch(atk_pool)

    assert atk_info.value is err, (
        f"the caller saw {atk_info.value!r} instead of the in-flight error: "
        "the explicit acquire form mangled the exception"
    )
    assert ref_info.value is err
    assert type(atk_info.value) is type(ref_info.value)


async def test_round_success_reaches_the_caller(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful round returns normally from both forms."""
    setup_meter(monkeypatch)
    ref_pool = _TrackingPool(lambda: _failing_conn(None))
    await _reference_async_with(ref_pool, None)
    atk_pool = _TrackingPool(lambda: _failing_conn(None))
    rows = await _explicit_dispatch(atk_pool)
    assert rows == []


async def test_acquire_failure_reaches_the_caller_and_pool_serves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed acquire reaches the caller identically from both forms,
    and the pool still serves a follow-up round."""

    class _EnterFailsPool:
        def __init__(self) -> None:
            self.healthy = False

        def acquire(self, *, timeout: float | None = None) -> Any:
            pool = self

            class _Ctx:
                async def __aenter__(self) -> object:
                    if pool.healthy:
                        return _ok_conn()
                    raise TimeoutError("acquire timeout")

                async def __aexit__(self, *a: object) -> None:
                    return None

            return _Ctx()

    ref = _EnterFailsPool()
    with pytest.raises(TimeoutError):
        await _reference_async_with(cast(Any, ref), None)
    atk = _EnterFailsPool()
    with pytest.raises(TimeoutError):
        await _explicit_dispatch(cast(Any, atk))
    # The pool keeps serving after the failed acquire on both forms.
    ref.healthy = True
    async with ref.acquire(timeout=5.0) as c:
        assert c is not None
    atk.healthy = True
    rows = await _explicit_dispatch(cast(Any, atk))
    assert rows == []


async def test_terminate_arm_error_reaches_the_caller_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The terminate arm (InternalClientError): the caller sees the SAME
    error object the statement raised, from both forms."""
    setup_meter(monkeypatch)
    err = asyncpg.exceptions.InternalClientError("cannot switch to state 12")

    ref_pool = _TrackingPool(lambda: _failing_conn(err))
    with pytest.raises(asyncpg.exceptions.InternalClientError) as ref_info:
        await _reference_async_with(ref_pool, err)

    atk_pool = _TrackingPool(lambda: _failing_conn(err))
    with pytest.raises(asyncpg.exceptions.InternalClientError) as atk_info:
        await _explicit_dispatch(atk_pool)

    assert atk_info.value is err, (
        f"the terminate arm forwarded {atk_info.value!r} instead of the statement's error object"
    )
    assert ref_info.value is err


async def test_body_cancellation_reaches_the_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation in the body reaches the caller as CancelledError from
    both forms."""
    setup_meter(monkeypatch)

    class _CancellingConn:
        def __init__(self) -> None:
            self.mark = asyncio.Event()

        async def fetch(self, _sql: str, *args: object) -> list[Any]:
            self.mark.set()
            await asyncio.Event().wait()  # cancelled here
            return []

        async def execute(self, *_a: object) -> str:
            return "UPDATE 0"

        def terminate(self) -> None: ...

    async def _reference_cancel(pool: _TrackingPool, conn: _CancellingConn) -> None:
        async with pool.acquire(timeout=5.0) as c:
            await c.fetch("SELECT 1")

    ref_conn, atk_conn = _CancellingConn(), _CancellingConn()
    ref_pool = _TrackingPool(lambda: ref_conn)
    ref_task = asyncio.create_task(_reference_cancel(ref_pool, ref_conn))
    await ref_conn.mark.wait()
    ref_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await ref_task

    atk_pool = _TrackingPool(lambda: atk_conn)
    atk_task = asyncio.create_task(_explicit_dispatch(atk_pool))
    await atk_conn.mark.wait()
    atk_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await atk_task


# ─────────────────────────────────────────────────────────────────────────
# 5. The same differential against a REAL asyncpg.Pool: max_size=1 makes a
#    single leaked checkout observable as a hung follow-up round.
# ─────────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def real_pg_pool() -> Any:
    """A migrated TaskQ schema and a real one-connection asyncpg pool.

    max_size=1 is the sharpest leak detector available: any checkout the
    round fails to release hangs every follow-up round and the test
    times out. Skipped when no Postgres is reachable."""
    try:
        conn = await asyncpg.connect(_PG_DSN)
    except Exception:
        pytest.skip(f"no Postgres reachable at {_PG_DSN} for the real-pool differential")
        return  # unreachable
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_PG_SCHEMA}" CASCADE')
        from taskq.migrate import apply_pending
        from taskq.testing.pg import seed_actors

        await apply_pending(conn, schema=_PG_SCHEMA)
        await seed_actors(conn, _PG_SCHEMA)
    finally:
        await conn.close()
    pool = await asyncpg.create_pool(_PG_DSN, min_size=1, max_size=1)
    try:
        yield pool
    finally:
        try:
            await asyncio.wait_for(pool.close(), timeout=10)
        except TimeoutError:
            pool.terminate()  # test-infra teardown; leak observables live in the tests
    cleanup = await asyncpg.connect(_PG_DSN)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{_PG_SCHEMA}" CASCADE')
    finally:
        await cleanup.close()


async def _rename_jobs_async(from_name: str, to_name: str) -> None:
    conn = await asyncpg.connect(_PG_DSN)
    try:
        await conn.execute(f'ALTER TABLE "{_PG_SCHEMA}".{from_name} RENAME TO {to_name}')
    finally:
        await conn.close()


async def _run_round(pool: Any) -> list[Any]:
    return await _dispatch_batch(
        pool,
        render(_PG_SCHEMA),
        2,
        5.0,
        _PG_SCHEMA,
        UUID(int=9),
        ["default"],
        10,
        timedelta(seconds=30),
        queue_mode_cache=None,
    )


async def test_real_pool_failed_round_releases_and_next_rounds_serve(
    real_pg_pool: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A round that fails mid-claim (the jobs table vanishes) must (a)
    propagate the true server error to the caller, (b) release the pool's
    only connection - every follow-up round serves - and (c) leave the
    pool's size exactly as it was. The identical fault through a plain
    ``async with`` must be indistinguishable to its caller."""
    reader = setup_meter(monkeypatch)
    pool = real_pg_pool
    baseline = pool.get_size()

    # Fault injection at the operator level: the jobs table vanishes.
    await _rename_jobs_async("jobs", "jobs_shadow")
    with pytest.raises(asyncpg.UndefinedTableError):
        await _run_round(pool)
    # Differential: the identical fault through a plain `async with` -
    # the same error class reaches its caller too.
    with pytest.raises(asyncpg.UndefinedTableError):
        async with pool.acquire(timeout=5.0) as c:
            await c.fetch(
                render(_PG_SCHEMA).dispatch_strict_fifo,
                ["default"],
                10,
                UUID(int=9),
                timedelta(seconds=30),
                2,
            )
    # The fault is repaired: every follow-up round serves - with
    # max_size=1, a leaked checkout or wedged release hangs here.
    await _rename_jobs_async("jobs_shadow", "jobs")
    for _ in range(3):
        rows = await _run_round(pool)
        assert rows == []
    assert pool.get_size() == baseline, "the failed round grew the pool"
    # The round's failure was visible on the public failure counter,
    # naming the error class.
    assert counter_value(reader, "taskq.dispatch.failures") >= 1


async def test_real_pool_cancelled_round_parked_on_exhausted_pool(
    real_pg_pool: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A round parked waiting for the pool's only connection, then
    cancelled: the operator sees the wait on the pool-acquire series
    (with its real duration), no failure counter, no SQL-duration sample
    - and the pool still serves afterwards."""
    reader = setup_meter(monkeypatch)
    pool = real_pg_pool

    holder_ctx = pool.acquire(timeout=5.0)
    await holder_ctx.__aenter__()  # exhaust the pool's one connection
    try:
        task = asyncio.create_task(_run_round(pool))
        await asyncio.sleep(0.1)  # the round is now parked in the acquire wait
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
        assert len(wait_points) == 1 and wait_points[0].count == 1, (
            "the interrupted wait must record exactly once"
        )
        assert wait_points[0].sum >= 0.05, (
            f"the recorded wait must carry the real ~100ms wait, got {wait_points[0].sum}"
        )
        assert counter_value(reader, "taskq.dispatch.failures") == 0, (
            "a cancelled acquire must not count as a dispatch failure"
        )
        assert histogram_points(reader, "taskq.dispatch.duration") == []
    finally:
        await holder_ctx.__aexit__(None, None, None)
    rows = await _run_round(pool)
    assert rows == []


async def test_real_pool_successful_round_leaves_pool_serviceable(
    real_pg_pool: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful round (acquire, resolve, claim, probe, release on the
    real pool) leaves the pool exactly as it was and the failure counter
    silent; a plain `async with` round interleaved behaves identically."""
    reader = setup_meter(monkeypatch)
    pool = real_pg_pool
    baseline = pool.get_size()

    rows = await _run_round(pool)
    assert rows == []
    assert pool.get_size() == baseline

    async with pool.acquire(timeout=5.0) as c:
        assert await c.fetch("SELECT 1")

    rows = await _run_round(pool)
    assert rows == []
    assert pool.get_size() == baseline
    assert counter_value(reader, "taskq.dispatch.failures") == 0


# The default budget this suite's shrink-by-monkeypatch attacks relate to.
assert DEFAULT_MAX_CONSECUTIVE_UNEXPECTED == 5
