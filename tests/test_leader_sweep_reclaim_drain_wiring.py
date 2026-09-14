"""The sweep loop's pending-reclaim drain wiring.

The keyed-eviction block feeds the drain (evicted bucket names recorded
under the settings-derived pending cap), the drain runs on the same
non-leader-gated path with the dispatcher pool and its command timeout,
and a drain failure warns and leaves the loop alive to retry on the next
tick — the pending set survives the failure intact.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic

import asyncpg
import pytest
import structlog

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
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


def _deps(sweep_interval: float | None = None) -> WorkerDeps:
    data: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "2.0",
        "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
        "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    if sweep_interval is not None:
        data["TASKQ_SWEEP_INTERVAL"] = str(sweep_interval)
    settings = WorkerSettings.load_from_dict(data, validate=False)
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


def _seed_idle_keyed_rate_limit_entry(reg: RateLimitRegistry, schema: str) -> None:
    """Simulate a keyed-materialized rate limit idle for 2 hours whose
    publish landed (schema captured) — the shape whose eviction must
    record a pending reclaim under the settings-derived cap."""
    reg.register(TokenBucket(name="krl:k1", capacity=5, refill_per_second=0.5, backend="memory"))
    reg._keyed_rate_limit_last_used["krl:k1"] = monotonic() - 7200.0  # pyright: ignore[reportPrivateUsage]  # Why: seeding an idle keyed entry for eviction
    reg._keyed_rate_limit_row_schemas["krl:k1"] = schema  # pyright: ignore[reportPrivateUsage]  # Why: seeding the publish-schema capture the eviction records from


def _ctx(deps: WorkerDeps, reg: RateLimitRegistry) -> SweepContext:
    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return SweepContext(
        deps=deps,
        backend=_SimpleBackend(),  # type: ignore[arg-type]  # Why: test double for the Backend protocol
        clock=clock,
        worker_id=new_uuid(),
        rate_limit_registry=reg,
    )


async def _run_loop_until(ctx: SweepContext, done: Callable[[], bool]) -> None:
    import taskq.worker._leader_sweeps as sweeps_mod

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the sweep loop directly, matching test_leader_sweep_rl_registry.py's pattern.
    for _ in range(200):
        if done():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_sweep_loop_drains_pending_reclaims_via_dispatcher_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One tick: the idle keyed entry is evicted and recorded under the
    settings-derived pending cap, and the drain runs with the dispatcher
    pool and the dispatcher command timeout — the pool wait is bounded the
    same way as every other pool acquire in the loop."""
    own = RateLimitRegistry()
    _seed_idle_keyed_entry(own)
    deps = _deps()
    ctx = _ctx(deps, own)

    evict_calls: list[dict[str, object]] = []
    real_evict = own.evict_idle_keyed_reservations

    def _spy_evict(*, idle_for: timedelta, max_pending_reclaims: int | None = None) -> int:
        evict_calls.append({"idle_for": idle_for, "max_pending_reclaims": max_pending_reclaims})
        return real_evict(idle_for=idle_for, max_pending_reclaims=max_pending_reclaims)

    monkeypatch.setattr(own, "evict_idle_keyed_reservations", _spy_evict)

    drain_calls: list[tuple[object, float | None]] = []
    real_drain = own.drain_pending_reservation_reclaims

    async def _spy_drain(
        pool: object, *, batch_names: int = 256, acquire_timeout: float | None = None
    ) -> int:
        drain_calls.append((pool, acquire_timeout))
        return await real_drain(  # pyright: ignore[reportPrivateUsage]  # Why: delegating to the real drain; the pool is a test double the real path accepts.
            pool,  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool
            batch_names=batch_names,
            acquire_timeout=acquire_timeout,
        )

    monkeypatch.setattr(own, "drain_pending_reservation_reclaims", _spy_drain)

    await _run_loop_until(
        ctx, lambda: bool(drain_calls) and not own.has_pending_reservation_reclaims
    )

    assert evict_calls, (
        "the tick must evict the idle keyed entry — the eviction is the drain's feed"
    )
    assert evict_calls[0]["max_pending_reclaims"] == ctx.deps.settings.max_keyed_reservations, (
        "the sweep must record pending reclaims under the settings-derived cap "
        "(WorkerSettings.max_keyed_reservations), not the constant fallback — "
        "with a deliberately small setting the pending set would otherwise grow "
        "to the constant's 10 000 while the tracked entries are capped far lower"
    )
    assert drain_calls, "the tick must run the pending-reclaim drain after the evictions"
    assert drain_calls[0][0] is ctx.deps.dispatcher_pool, (
        "the drain must run on the dispatcher pool — the loop's convention for "
        "every pool acquire on this path"
    )
    assert drain_calls[0][1] == ctx.deps.settings.dispatcher_command_timeout, (
        "the drain's pool wait must be bounded by the dispatcher command timeout"
    )
    assert not own.has_pending_reservation_reclaims, "the tick's drain must empty the pending set"


async def test_sweep_loop_survives_drain_failure_and_retries_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing drain is warned and does not kill the sweep loop: the next
    tick retries with the pending set intact (a stranded-rows backlog is
    visible on the pending-depth gauge and the drain-failure counter, not
    as a dead sweeper)."""
    own = RateLimitRegistry()
    _seed_idle_keyed_entry(own)
    deps = _deps(sweep_interval=0.05)
    ctx = _ctx(deps, own)

    attempts = 0

    async def _failing_drain(
        pool: object, *, batch_names: int = 256, acquire_timeout: float | None = None
    ) -> int:
        nonlocal attempts
        attempts += 1
        raise asyncpg.PostgresConnectionError("drain connection lost")

    monkeypatch.setattr(own, "drain_pending_reservation_reclaims", _failing_drain)

    # A fresh, un-cached lazy proxy for the duration of the capture: an
    # earlier test in the same process can freeze the module-level proxy
    # (a monkeypatched-then-"restored" method pins the bound logger of the
    # moment against a stale structlog configuration), and capture_logs
    # cannot see through a frozen proxy. The loop's wiring is unchanged —
    # same logger kind, same module name, dynamically bound.
    import taskq.worker._leader_sweeps as sweeps_mod

    monkeypatch.setattr(sweeps_mod, "log", structlog.get_logger(sweeps_mod.__name__))
    with structlog.testing.capture_logs() as captured:
        await _run_loop_until(ctx, lambda: attempts >= 2)

    assert attempts >= 2, (
        "a drain failure must not kill the sweep loop — the next tick retries "
        "with the pending set intact"
    )
    assert any(
        e.get("event") == "sweep-drain-pending-reservation-reclaims-failed" for e in captured
    ), (
        "a failed drain must warn (the pending-depth gauge and drain-failure "
        "counter carry the steady signal; the log names the fault)"
    )
    assert own.has_pending_reservation_reclaims, (
        "a failed drain must keep its backlog for the retry"
    )


async def test_sweep_loop_records_rate_limit_reclaims_under_settings_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rate-limit eviction records its pending reclaims under the
    settings-derived cap — the same bound the reservation twin on the
    same tick applies, and the same bound the rate-limit opportunistic
    eviction already applies.

    A deliberately small ``max_keyed_rate_limits`` caps the tracked
    entries; a sweep-site eviction that passes no cap records pendings
    under the constant fallback (10 000), so under a persistently failing
    drain the pending set can grow two orders of magnitude past the
    operator's ceiling while the tracked entries it mirrors stay capped —
    the exact defect the reservation side's own comment describes."""
    own = RateLimitRegistry()
    deps = _deps()
    _seed_idle_keyed_rate_limit_entry(own, deps.settings.schema_name)
    ctx = _ctx(deps, own)

    evict_calls: list[dict[str, object]] = []
    real_evict = own.evict_idle_keyed_rate_limits

    def _spy_evict(*, idle_for: timedelta, max_pending_reclaims: int | None = None) -> int:
        evict_calls.append({"idle_for": idle_for, "max_pending_reclaims": max_pending_reclaims})
        return real_evict(idle_for=idle_for, max_pending_reclaims=max_pending_reclaims)

    monkeypatch.setattr(own, "evict_idle_keyed_rate_limits", _spy_evict)

    await _run_loop_until(
        ctx, lambda: bool(evict_calls) and not own.has_pending_reservation_reclaims
    )

    assert evict_calls, (
        "the tick must evict the idle keyed rate-limit entry — the eviction is the drain's feed"
    )
    assert evict_calls[0]["max_pending_reclaims"] == ctx.deps.settings.max_keyed_rate_limits, (
        "the sweep must record rate-limit pending reclaims under the "
        "settings-derived cap (WorkerSettings.max_keyed_rate_limits), not the "
        "constant fallback — the reservation twin on the same tick and the "
        "rate-limit opportunistic eviction both pass the settings-derived cap, "
        "and the same rationale binds here: with a deliberately small setting "
        "the pending set would otherwise grow to the constant's 10 000 while "
        "the tracked entries are capped far lower"
    )
    assert not own.has_pending_reservation_reclaims, "the tick's drain must empty the pending set"
