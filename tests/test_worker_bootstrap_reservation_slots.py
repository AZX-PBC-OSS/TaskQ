"""Unit pins for the bootstrap's ensure-slots loop (#293).

The bootstrap's per-reservation ensure pass must wait no longer than
``dispatcher_command_timeout`` — the budget every sibling bootstrap await
already carries — and a lapse must surface as the typed
``ensure_slots_timeout`` warning (distinct from the generic
``ensure_slots_failed`` so a starvation stall is diagnosable from startup
logs) without aborting the loop. Before #293 the pass called
``ensure_slots`` with no bound at all: under dispatcher-pool starvation the
bare ``pool.acquire()`` waited forever, which is the observed 300s CI hang.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import structlog

from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.worker._bootstrap import _ensure_own_reservation_slots
from taskq.worker.deps import WorkerDeps


class _StarvedThenHealthyPool:
    """asyncpg-shaped pool double: starves the first *starved_acquires*
    acquires (each raising TimeoutError once its budget lapses, the
    driver's own acquire contract), then hands out working connections."""

    def __init__(self, *, starved_acquires: int = 0, execute_raises: bool = False) -> None:
        self._pending_starvation = starved_acquires
        self._execute_raises = execute_raises
        self.acquire_timeouts: list[float | None] = []
        self.executes = 0

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[Any, None]:
        self.acquire_timeouts.append(timeout)
        if self._pending_starvation > 0:
            self._pending_starvation -= 1
            if timeout is None:
                await asyncio.Event().wait()  # never resolves: the #293 hang
            await asyncio.sleep(min(timeout, 5.0))
            raise TimeoutError("starved: could not acquire a pool connection")
        yield self

    async def execute(self, *_args: object) -> str:
        self.executes += 1
        if self._execute_raises:
            raise RuntimeError("ensure_slots boom")
        return "INSERT 0 0"


def _deps(pool: object) -> WorkerDeps:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            # The ge= floor: the smallest budget the validator allows, so
            # the starved-pool lapse costs one real second, not five.
            "TASKQ_DISPATCHER_COMMAND_TIMEOUT": "1.0",
        }
    )
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


@pytest.mark.asyncio
async def test_starved_ensure_slots_logs_typed_timeout_and_continues() -> None:
    """A pool-acquire lapse must log ``ensure_slots_timeout`` and move on.

    The bound is the whole point of #293: the loop must finish (here under
    a generous outer wait_for — on the pre-fix code the first acquire never
    resolves and the outer wait fires instead), the typed event must name
    the bucket and the budget, and the NEXT reservation's ensure must still
    run.
    """
    pool = _StarvedThenHealthyPool(starved_acquires=1)
    res_a = ConcurrencyReservation(name="res_a", slots=1, lease=30.0, schema="taskq")
    res_b = ConcurrencyReservation(name="res_b", slots=1, lease=30.0, schema="taskq")

    with structlog.testing.capture_logs() as captured:
        await asyncio.wait_for(
            _ensure_own_reservation_slots(_deps(pool), [res_a, res_b]),
            timeout=30.0,
        )

    timeouts = [e for e in captured if e.get("event") == "ensure_slots_timeout"]
    assert len(timeouts) == 1
    assert timeouts[0]["bucket_name"] == "res_a"
    assert timeouts[0]["timeout_seconds"] == 1.0
    # The typed event is DISTINCT from the generic failure: a starvation
    # lapse must not masquerade as a statement error in the logs.
    assert not any(e.get("event") == "ensure_slots_failed" for e in captured)
    # And the loop continued: res_b's materialisation ran.
    assert pool.executes >= 1


@pytest.mark.asyncio
async def test_ensure_slots_generic_failure_keeps_the_generic_event() -> None:
    """Non-timeout failures keep the existing ``ensure_slots_failed`` event
    (with ``bucket_name``) and still do not abort the loop."""
    pool = _StarvedThenHealthyPool(execute_raises=True)
    res_a = ConcurrencyReservation(name="res_a", slots=1, lease=30.0, schema="taskq")
    res_b = ConcurrencyReservation(name="res_b", slots=1, lease=30.0, schema="taskq")

    with structlog.testing.capture_logs() as captured:
        await asyncio.wait_for(
            _ensure_own_reservation_slots(_deps(pool), [res_a, res_b]),
            timeout=30.0,
        )

    failures = [e for e in captured if e.get("event") == "ensure_slots_failed"]
    assert len(failures) == 2
    assert {e["bucket_name"] for e in failures} == {"res_a", "res_b"}
    assert not any(e.get("event") == "ensure_slots_timeout" for e in captured)
