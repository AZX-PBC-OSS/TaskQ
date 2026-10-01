"""Red-team locks: unbounded terminal-write pool acquires starve the heartbeat loop.

``backend/_terminal.py``'s ``_mark_cancelled`` acquires from its pool with
NO timeout (``async with pool.acquire() as conn:``) and
``backend/postgres.py`` routes ``mark_cancelled`` to the HEARTBEAT pool,
while the heartbeat loop's own acquire is bounded
(``worker/heartbeat.py`` - ``acquire(timeout=interval)``) and its
``TimeoutError`` is classified transient (``worker/_transient.py``), so
each starved tick increments ``heartbeat_failures`` and crossing
``max_heartbeat_failures`` escalates to ``isolate_self`` - worker
self-shutdown.

A cancel storm (``heartbeat_pool_size`` - default 4, ``settings.py`` -
concurrent consumer ``mark_cancelled`` writes holding/gating the pool)
therefore starves the heartbeat loop into self-shutdown while the worker
was merely cancelling jobs. The RED contract: terminal-write pool
acquires must be bounded (or routed off the heartbeat pool).
"""

import asyncio
import contextlib
import math
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._sql_templates import render as render_sql
from taskq.backend._terminal import _mark_cancelled
from taskq.settings import WorkerSettings
from taskq.testing.asyncpg_chaos import (  # pyright: ignore[reportPrivateUsage]  # Why: the recording pool's ctx subclass must BE the pool's own checkout type for the override to typecheck; same package contract the double documents.
    ChaosConnection,
    ChaosPool,
    _ChaosAcquireCtx,
)
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop


class _NullConn:
    """Stand-in conn for a pool that never yields: none of these methods
    is reachable when every acquire waits forever."""

    async def execute(self, *args: object, **kwargs: object) -> str:
        return "OK"

    async def fetchrow(self, *args: object, **kwargs: object) -> object | None:
        return None

    async def fetch(self, *args: object, **kwargs: object) -> list[object]:
        return []

    async def fetchval(self, *args: object, **kwargs: object) -> object | None:
        return None

    def transaction(self, **kwargs: object) -> "_NullConn":
        return self

    async def __aenter__(self) -> "_NullConn":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


def _starved_pool() -> ChaosPool:
    """A pool with no connection to hand over - every acquire queues.

    ``acquire_delay=math.inf`` models a pool whose every connection
    (``heartbeat_pool_size``, default 4) is held by an in-flight gated
    write; asyncpg's acquire then waits indefinitely, exactly as this
    double does.
    """
    return ChaosPool(
        ChaosConnection(_NullConn(), fail_on_call=1),  # pyright: ignore[reportArgumentType]  # Why: conn double stands in for an asyncpg Connection, same as the pool stand-ins below.
        acquire_delay=math.inf,
    )


#: The hang guard (seconds) around the probe below, and the ceiling an
#: accepted acquire bound may carry. Arithmetic: 2 x the shipped default
#: terminal-write bound (:data:`~taskq.backend._terminal.DEFAULT_TERMINAL_POOL_ACQUIRE_TIMEOUT_S`
#: = 0.25) - the guard must outlive any bound the shipped code can pass
#: (so the bound's own TimeoutError is what the test observes, never the
#: guard), while any bound at or above the guard is too loose to protect
#: the heartbeat loop and fails the contract by STATE (the recorded
#: kwarg), not by wall clock.
_ACQUIRE_GUARD_S = 0.5


class _RecordingStarvedPool(ChaosPool):
    """The starved pool plus the record the probe's discrimination reads.

    The discrimination this test makes is STATE, not elapsed time: the
    contract ("terminal-write acquires must be bounded") is proven by
    (a) the ``timeout=`` kwarg the production code actually passed and
    (b) WHICH wait ended the probe - the bound's own ``TimeoutError``
    (the designed failure, raised from the pool's honored bound) or the
    test's guard (nothing internal ended it). The wall-clock margin
    dance this replaces (fail when elapsed >= 0.4) raced its own
    discrimination: any >=150ms event-loop stall between the probe's
    start and the caught TimeoutError - CI co-tenancy descheduling,
    coverage tracing - pushed the measured elapsed past the margin on a
    run whose production bound was honored, red on a healthy system
    (the recurring flake). Elapsed no longer participates: the pool
    records what it was handed and which wait fired, and those states
    decide.
    """

    def __init__(self, chaos_conn: ChaosConnection) -> None:
        super().__init__(chaos_conn, acquire_delay=math.inf)
        self.acquired_with: list[float | None] = []
        self.bounded_failure = False
        """True when the pool's OWN honored bound raised the TimeoutError -
        the designed bounded failure, never the test's guard."""

    def acquire(self, *, timeout: float | None = None) -> _ChaosAcquireCtx:
        self.acquired_with.append(timeout)
        pool = self

        class _BoundObservedCtx(_ChaosAcquireCtx):
            """The checkout, marking the pool when the handed bound's own
            timeout is what fired: asyncio re-raises the bound's
            TimeoutError from ``__aenter__`` (the guard, by contrast,
            CANCELS the probe - a CancelledError, not a TimeoutError - so
            only a genuinely honored bound can set the flag)."""

            async def __aenter__(self) -> ChaosConnection:
                try:
                    return await super().__aenter__()
                except TimeoutError:
                    pool.bounded_failure = True
                    raise

        return _BoundObservedCtx(self._conn, self._acquire_delay, timeout)


def _deps(pool: ChaosPool, *, max_heartbeat_failures: int) -> WorkerDeps:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            # 3.0 + the tiny command timeout satisfies the cascade
            # floor: 4 * (0.5 + 2 * 0.1) = 2.8 <= 3.0.
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_LOCK_LEASE": "3.0",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_MAX_HEARTBEAT_FAILURES": str(max_heartbeat_failures),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: pool double stands in for asyncpg.Pool; reportArgumentType is disabled for tests but keep the marker for local runs.
        heartbeat_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: same stand-in.
        worker_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: same stand-in.
        notify_conn=None,
        leader_conn=None,
    )


async def test_mark_cancelled_pool_acquire_must_be_bounded() -> None:
    """RED: mark_cancelled's pool acquire must not queue indefinitely on an
    exhausted pool.

    Contract: terminal-write pool acquires must be bounded (or routed off
    the heartbeat pool) - a cancel storm saturating the heartbeat pool must
    fail the individual write within a bound, not queue it forever. The
    starved pool below yields no connection at all, so the probe can only
    end by a timeout, and the discrimination is STATE (the ``timeout=``
    kwarg the pool recorded), never elapsed wall clock - the elapsed
    margin this replaces red on a healthy run whenever CI co-tenancy (or
    coverage tracing) descheduled the loop >=150ms inside the probe's
    window and pushed the measured elapsed past the margin while the
    production bound was honored.
    """
    pool = _RecordingStarvedPool(ChaosConnection(_NullConn(), fail_on_call=1))  # pyright: ignore[reportArgumentType]  # Why: conn double stands in for an asyncpg Connection, same as the pool stand-ins in _deps below.
    sql = render_sql("taskq")
    try:
        await asyncio.wait_for(
            _mark_cancelled(
                pool,  # pyright: ignore[reportArgumentType]  # Why: pool double stands in for asyncpg.Pool, same as _deps above.
                sql,
                new_job_id(),
                new_uuid(),
            ),
            timeout=_ACQUIRE_GUARD_S,
        )
    except TimeoutError:
        # Two waits can end this probe: the pool's OWN honored bound
        # (the DESIGNED terminal-write infra failure - the contract
        # holding) or the test's guard (nothing internal ended the wait
        # - the contract violated). bounded_failure says which fired.
        if not pool.bounded_failure:
            handed = pool.acquired_with[-1] if pool.acquired_with else None
            pytest.fail(
                "Contract: terminal-write pool acquires must be bounded (or routed off "
                "the heartbeat pool) - a starved pool must fail the write within a bound. "
                "Today backend/_terminal.py's _mark_cancelled handed its pool acquire "
                f"timeout={handed!r} (the guard is {_ACQUIRE_GUARD_S}s = 2x the shipped "
                "0.25s default), routed to the heartbeat pool by backend/postgres.py's "
                "mark_cancelled, so the cancel write was still queued at the test's own "
                f"{_ACQUIRE_GUARD_S}s guard and never resolves while the pool is exhausted."
            )
    handed = pool.acquired_with[-1] if pool.acquired_with else None
    assert handed is not None and handed <= _ACQUIRE_GUARD_S, (
        "Contract: terminal-write pool acquires must be bounded (or routed off "
        "the heartbeat pool) - the pool observed timeout="
        f"{handed!r} (guard {_ACQUIRE_GUARD_S}s = 2x the shipped 0.25s default); "
        "None means backend/_terminal.py's unbounded `pool.acquire()` regressed, "
        "a loose bound means the cancel write queues past the heartbeat loop's "
        "own bounded acquire and starves it into isolate_self"
    )


async def test_cancel_storm_starves_heartbeat_loop_into_isolate_self(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PIN of today's starvation spiral: heartbeat_pool_size gated cancel
    writes starve the heartbeat loop's bounded acquire past the failure
    threshold into isolate_self - worker self-shutdown.

    Contract this pin motivates (the RED twin above asserts it):
    terminal writes routed to the heartbeat pool with unbounded acquires
    (backend/_terminal.py - no timeout on pool.acquire) let a cancel storm
    of heartbeat_pool_size (default 4, settings.py) writes hold the whole
    pool; the heartbeat loop's own acquire is bounded
    (worker/heartbeat.py - acquire(timeout=interval)) and its TimeoutError
    is transient (worker/_transient.py), so failures escalate
    (worker/heartbeat.py) to isolate_self - self-shutdown while merely
    cancelling jobs. The storm's own writes never resolved either - both
    the writers and the liveness loop wedge on the same pool. A fix that
    bounds terminal acquires (or routes them off the heartbeat pool) must
    make this spiral unreachable.
    """
    pool = _starved_pool()
    deps = _deps(pool, max_heartbeat_failures=2)
    sql = render_sql("taskq")
    storm = [
        asyncio.create_task(
            _mark_cancelled(
                pool,  # pyright: ignore[reportArgumentType]  # Why: pool double stands in for asyncpg.Pool, same as _deps above.
                sql,
                new_job_id(),
                new_uuid(),
            ),
            name=f"cancel-storm-{i}",
        )
        for i in range(4)
    ]
    isolate_calls: list[tuple[UUID, int]] = []

    async def _record_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((worker_id, deps.heartbeat_failures))
        shutdown.set()

    monkeypatch.setattr("taskq.worker.heartbeat.isolate_self", _record_isolate)
    shutdown = asyncio.Event()
    loop_task = asyncio.create_task(
        heartbeat_loop(deps, new_uuid(), shutdown), name="heartbeat-starved"
    )
    try:
        await asyncio.wait_for(loop_task, timeout=10.0)
        assert all(task.done() for task in storm), (
            "Contract (the landed bounded terminal acquire): every storm "
            "write resolves within its acquire bound instead of queueing "
            "forever on the starved pool - a still-pending cancel write "
            "here means an unbounded terminal acquire regressed "
            "(backend/_terminal.py: pool.acquire(timeout=...))"
        )
        assert isolate_calls, (
            "Contract: the starved heartbeat loop must have escalated to isolate_self "
            f"(failures={deps.heartbeat_failures}, "
            f"max={deps.settings.max_heartbeat_failures}) - today's spiral: cancel "
            "storm holds the heartbeat pool, the loop's bounded acquire times out, "
            "TimeoutError counts transient (worker/_transient.py), failures cross "
            "max_heartbeat_failures, isolate_self shuts the worker down"
        )
        assert shutdown.is_set(), (
            "Contract: isolate_self must signal worker shutdown - the spiral's endpoint"
        )
        assert deps.heartbeat_failures > deps.settings.max_heartbeat_failures, (
            "Contract: the escalation must fire past the failure threshold "
            f"(failures={deps.heartbeat_failures})"
        )
    finally:
        shutdown.set()
        if not loop_task.done():
            loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await loop_task
        for task in storm:
            task.cancel()
        await asyncio.gather(*storm, return_exceptions=True)
