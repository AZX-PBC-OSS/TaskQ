"""Keyed eviction is de-gated from leadership: every worker sweeps its OWN
registry (ctx.rate_limit_registry) each tick, leader or not; the module
singleton remains the fallback when no registry was supplied."""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic
from unittest.mock import MagicMock

import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
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


class _SimpleBackend:
    """Backend whose reclaim/deadline sweeps return 0 and lacks PG-only sweeps."""

    async def reclaim_expired_locks(self, now: datetime, cg: timedelta, ug: timedelta) -> int:
        return 0

    async def deadline_sweep(self, now: datetime) -> int:
        return 0


def _deps() -> WorkerDeps:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "2.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        },
        validate=False,
    )
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )


def _seed_idle_keyed_entry(reg: RateLimitRegistry) -> None:
    """Simulate a keyed-materialized reservation idle for 2 hours."""
    reg.register(ConcurrencyReservation(name="sess:k1", slots=1, lease=timedelta(minutes=5)))
    reg._keyed_reservation_last_used["sess:k1"] = monotonic() - 7200.0  # pyright: ignore[reportPrivateUsage]  # Why: seeding an idle keyed entry for eviction


async def _run_one_tick(ctx: SweepContext, done: Callable[[], bool]) -> None:
    import taskq.worker._leader_sweeps as sweeps_mod

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the sweep loop directly, matching precedent in test_keyed_reservation_hardening.py
    for _ in range(200):
        if done():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_non_leader_worker_sweeps_own_registry() -> None:
    """is_leader NOT set: the worker's OWN registry is still evicted (de-gated)."""
    own = RateLimitRegistry()
    _seed_idle_keyed_entry(own)
    deps = _deps()  # is_leader deliberately NOT set
    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    ctx = SweepContext(
        deps=deps,
        backend=_SimpleBackend(),  # type: ignore[arg-type]  # Why: test double for the Backend protocol
        clock=clock,
        worker_id=new_uuid(),
        rate_limit_registry=own,
    )

    await _run_one_tick(ctx, lambda: "sess:k1" not in own.reservations)

    assert own.reservations == {}
    assert own._keyed_reservation_last_used == {}  # pyright: ignore[reportPrivateUsage]


async def test_non_leader_falls_back_to_module_singleton(monkeypatch: pytest.MonkeyPatch) -> None:
    """ctx.rate_limit_registry=None → module-level singleton used, even when
    NOT leader (de-gating applies to the fallback path too)."""
    import taskq.worker._leader_sweeps as sweeps_mod

    mock_registry = MagicMock()
    mock_registry.has_keyed_reservations = True
    mock_registry.has_keyed_rate_limits = False
    mock_registry.evict_idle_keyed_reservations.return_value = 0
    monkeypatch.setattr(sweeps_mod, "rl_registry", mock_registry)

    deps = _deps()  # is_leader deliberately NOT set
    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    ctx = SweepContext(
        deps=deps,
        backend=_SimpleBackend(),  # type: ignore[arg-type]  # Why: test double for the Backend protocol
        clock=clock,
        worker_id=new_uuid(),
    )

    await _run_one_tick(ctx, lambda: mock_registry.evict_idle_keyed_reservations.called)

    mock_registry.evict_idle_keyed_reservations.assert_called_once()
    assert mock_registry.evict_idle_keyed_reservations.call_args.kwargs["idle_for"] == timedelta(
        hours=1
    )
