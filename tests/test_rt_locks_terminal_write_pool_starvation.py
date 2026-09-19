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
import time
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._sql_templates import render as render_sql
from taskq.backend._terminal import _mark_cancelled
from taskq.settings import WorkerSettings
from taskq.testing.asyncpg_chaos import ChaosConnection, ChaosPool
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


#: Elapsed margin (seconds) below which a TimeoutError counts as the
#: system's DESIGNED bounded failure rather than an unbounded queue -
#: the discrimination pattern of the sibling deliverable
#: ``tests/test_rt_locks_sweep_notify_pool_unbounded.py``: fail only when
#: the call took unboundedly long (nothing internal ended the wait), so
#: a bounded acquire firing inside the test's window passes. The twin's
#: own outer bound is 0.5 s, so the margin sits under it: a production
#: bound that fires before the margin satisfies the contract, and the
#: unbounded pre-fix acquire (ended only by the test's own 0.5 s
#: wait_for) fails.
_UNBOUNDED_MARGIN_S = 0.4


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
    fail the individual write within a bound, not queue it forever. Today
    the contract is violated: backend/_terminal.py's ``_mark_cancelled``
    acquires with ``async with pool.acquire() as conn:`` - NO timeout= -
    (and backend/postgres.py routes mark_cancelled onto the heartbeat
    pool), so the write below was still queued at the test's own 0.5 s
    bound and would never resolve while the pool stays exhausted.
    """
    pool = _starved_pool()
    sql = render_sql("taskq")
    started = time.monotonic()
    try:
        await asyncio.wait_for(
            _mark_cancelled(
                pool,  # pyright: ignore[reportArgumentType]  # Why: pool double stands in for asyncpg.Pool, same as _deps above.
                sql,
                new_job_id(),
                new_uuid(),
            ),
            timeout=0.5,
        )
    except TimeoutError as exc:
        # Sibling-pattern discrimination: a bounded acquire's fast
        # TimeoutError is the system's DESIGNED failure mode
        # (worker/_handlers.py's _TERMINAL_WRITE_INFRA_EXCEPTIONS
        # anticipates "timeout acquiring a pool connection") and passes;
        # only a wait that outlived the margin - nothing internal ended
        # it - violates the contract.
        elapsed = time.monotonic() - started
        if elapsed >= _UNBOUNDED_MARGIN_S:
            pytest.fail(
                "Contract: terminal-write pool acquires must be bounded (or routed off "
                "the heartbeat pool) - a starved pool must fail the write within a bound. "
                "Today backend/_terminal.py's _mark_cancelled acquires with "
                "`async with pool.acquire() as conn:` (no timeout=), routed to the "
                "heartbeat pool by backend/postgres.py's mark_cancelled, so the "
                "cancel write was still queued at the test's own 0.5 s bound "
                f"({exc!r}) and never resolves while the pool is exhausted."
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
