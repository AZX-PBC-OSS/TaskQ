"""The sweep loop's fleet-wide keyed-row reclaim wiring (#139 residual).

The reclaim seam itself (``sweep_idle_keyed_rows`` — one bounded,
committed batch per table per call, the keyed mark plus the
``last_used_at`` horizon deciding eligibility) is pinned against real
Postgres by ``tests/test_keyed_row_fleet_reclaim.py``. What that file
cannot see is the LEADER LOOP's driving of the seam: the block inside
``_sweep_loop`` must run once per tick with the operator's configured
horizon and batch size, and the settings-level disable sentinel
(``timedelta(0)``) must keep the tick from invoking the sweep at all —
the wiring contract, the same tier as
``tests/test_leader_sweep_event_retention_wiring.py`` pins for the
event-retention drain and
``tests/test_leader_sweep_reclaim_drain_wiring.py`` for the
pending-reclaim drain.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker._leader_shared import SweepContext
from taskq.worker.deps import WorkerDeps


class _FakeConn:
    async def execute(self, sql: str, *args: object) -> str:
        return "DELETE 0"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []

    async def fetchval(self, sql: str, *args: object) -> object:
        return None

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def close(self) -> None:
        pass

    def is_closed(self) -> bool:
        return False


class _FakePool:
    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
        yield _FakeConn()


class _KeyedReclaimBackend:
    """Backend double exposing the PG-only sweep surface the leader
    section gates on, recording every ``sweep_idle_keyed_rows`` call.

    ``reclaim_expired_locks`` / ``deadline_sweep`` return 0 so no drain
    runs; the PG-only sweeps (``sweep_leaked_reservation_slots`` /
    ``sweep_expired_results`` / ``sweep_expired_events``) exist so the
    ``hasattr`` gate around the keyed-reclaim block is entered, and
    return 0 for the same reason.
    """

    def __init__(self) -> None:
        self.keyed_reclaim_calls: list[dict[str, object]] = []
        self.retention_calls = 0
        self.leaked_slots_calls = 0

    async def reclaim_expired_locks(self, cancel_grace: timedelta, cleanup_grace: timedelta) -> int:
        return 0

    async def deadline_sweep(self) -> int:
        return 0

    async def sweep_leaked_reservation_slots(
        self, conn: object, *, schema: str, batch_size: int
    ) -> int:
        self.leaked_slots_calls += 1
        return 0

    async def sweep_expired_results(self, conn: object, *, schema: str, batch_size: int) -> int:
        return 0

    async def sweep_expired_events(
        self,
        conn: object,
        *,
        schema: str,
        retention: timedelta,
        batch_size: int,
    ) -> int:
        self.retention_calls += 1
        return 0

    async def sweep_idle_keyed_rows(
        self,
        conn: object,
        *,
        schema: str,
        horizon: timedelta,
        batch_size: int,
    ) -> int:
        self.keyed_reclaim_calls.append(
            {"schema": schema, "horizon": horizon, "batch_size": batch_size}
        )
        return 0


def _deps(keyed_row_reclaim_period: str) -> WorkerDeps:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SWEEP_INTERVAL": "0.05",
            "TASKQ_KEYED_ROW_RECLAIM_PERIOD": keyed_row_reclaim_period,
        },
        validate=False,
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    # The keyed-reclaim block lives in the leader-gated section of the tick.
    deps.is_leader.set()
    return deps


def _ctx(deps: WorkerDeps, backend: _KeyedReclaimBackend) -> SweepContext:
    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return SweepContext(
        deps=deps,
        backend=backend,  # type: ignore[arg-type]  # Why: test double for the Backend protocol
        clock=clock,
        worker_id=new_uuid(),
        rate_limit_registry=None,
    )


async def _run_loop_until(ctx: SweepContext, done: Callable[[], bool]) -> None:
    import taskq.worker._leader_sweeps as sweeps_mod

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the sweep loop directly, matching test_leader_sweep_reclaim_drain_wiring.py's pattern.
    try:
        for _ in range(200):
            if done():
                break
            await asyncio.sleep(0.01)
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_sweep_loop_drives_keyed_row_reclaim_with_configured_horizon() -> None:
    """One leader tick invokes the keyed-row reclaim sweep once, on the
    dispatcher pool, with the operator's configured horizon and batch
    size — the wiring that turns ``sweep_idle_keyed_rows`` into the
    #139 fleet reclaim (one bounded, committed batch per table per
    tick, deliberately NOT a ``_drain_bounded`` drain — the
    slow-and-constant discipline the event-retention block settled)."""
    deps = _deps(keyed_row_reclaim_period="2h")
    backend = _KeyedReclaimBackend()
    ctx = _ctx(deps, backend)

    await _run_loop_until(ctx, lambda: bool(backend.keyed_reclaim_calls))

    assert backend.keyed_reclaim_calls, (
        "the leader tick did not invoke sweep_idle_keyed_rows — the reclaim seam "
        "exists but nothing drives it, so keyed rows orphaned by a dead worker are "
        "never reclaimed by the leader"
    )
    call = backend.keyed_reclaim_calls[0]
    assert call["horizon"] == deps.settings.keyed_row_reclaim_period, (
        "the tick must drive the sweep with the configured keyed_row_reclaim_period, "
        "not a hardcoded horizon"
    )
    assert call["batch_size"] == deps.settings.keyed_row_reclaim_batch_size, (
        "the tick must drive the sweep with the configured keyed_row_reclaim_batch_size "
        "— the bound that keeps one tick's DELETE constant-size against any backlog"
    )
    assert call["schema"] == deps.settings.schema_name


async def test_disabled_keyed_row_reclaim_never_invokes_the_sweep() -> None:
    """``timedelta(0)`` is the settings-level disable sentinel: the tick
    must not invoke the keyed-row reclaim sweep at all (a disabled sweep
    acquires no connection and deletes nothing), while the rest of the
    leader section — proven by the sibling sweeps on the same gate —
    keeps running."""
    deps = _deps(keyed_row_reclaim_period="0")
    backend = _KeyedReclaimBackend()
    ctx = _ctx(deps, backend)

    # Two full leader-section passes (the leaked-slots sweep shares the
    # keyed-reclaim block's hasattr gate, and the retention sweep shares
    # its spot in the tick, so their calls prove the gate was entered
    # and the tick ran past the disabled block).
    await _run_loop_until(
        ctx, lambda: backend.leaked_slots_calls >= 2 and backend.retention_calls >= 2
    )

    assert backend.leaked_slots_calls >= 2, (
        "fixture broken: the loop did not run the leader section's gated block"
    )
    assert backend.retention_calls >= 2, (
        "fixture broken: the loop did not reach the retention block the keyed-reclaim "
        "wiring sits beside"
    )
    assert not backend.keyed_reclaim_calls, (
        "a disabled keyed-row reclaim (keyed_row_reclaim_period=timedelta(0)) must never "
        "invoke sweep_idle_keyed_rows — zero is the documented disable sentinel, and a "
        "sweep that ran anyway would delete every fleet-reclaimable keyed row older "
        "than 'now'"
    )
