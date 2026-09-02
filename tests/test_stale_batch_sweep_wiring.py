"""Wiring of the ``complete_stale_batches`` sweep inside ``_sweep_loop``.

Regression (PR #62 merge f60db32): the stale-batch completion block ended
up nested inside ``if rl.has_keyed_rate_limits:`` — a PROCESS-LOCAL
registry condition — and lost its leader gating. Consequences:

- Deployments not using keyed rate-limit refs (the default) NEVER ran the
  sweep: batches whose completion hook was lost (consumer crash between
  the terminal write and ``complete_batch``/``abort_batch``) stayed
  ``active`` forever, ``wait_for_batch`` could snooze indefinitely, and
  ``prune_old_batches`` (which only deletes ``completed_at IS NOT NULL``
  rows) never caught up — unbounded ``batches`` growth.
- Non-leaders could run it while the actual leader might not.

The intended wiring (PR #62 branch head 4b1e34f, and
docs/architecture.md "``complete_stale_batches`` leader sweep" /
docs/guides/workers.md) is: leader-gated, keyed-registry-independent, and
gated on a PG-shaped backend (``hasattr`` on ``sweep_leaked_reservation_slots``
— ``complete_stale_batches`` needs the dispatcher pool, which the
in-memory backend does not provide).

These tests pin that wiring while preserving PR #42's de-gating of keyed
eviction: every worker sweeps its OWN registry each tick, leader or not.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker._leader_shared import SweepContext
from taskq.worker.deps import WorkerDeps

_KEYED_RL_NAME = "rl:sweep:k1"


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


class _PgShapedBackend:
    """Backend satisfying every ``_sweep_loop`` call, including the PG-only
    maintenance sweeps — the ``hasattr(ctx.backend, "sweep_leaked_reservation_slots")``
    gate the stale-batch sweep rides on."""

    async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
        return 0

    async def deadline_sweep(self) -> int:
        return 0

    async def sweep_leaked_reservation_slots(self, conn: object, *, schema: str) -> int:
        return 0

    async def sweep_expired_results(self, conn: object, *, schema: str) -> int:
        return 0


def _deps(*, is_leader: bool) -> WorkerDeps:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "2.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            # Not a watchdog test: disabling exempts the short-lease config
            # from the lag-budget < lock_lease invariant (which post_load
            # enforces even under validate=False, by design).
            "TASKQ_WATCHDOG_ENABLED": "false",
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
    # Many ticks inside a bounded drive window, so negative assertions
    # survive several iterations rather than one.
    deps.settings.sweep_interval = 0.01
    if is_leader:
        deps.is_leader.set()
    return deps


def _ctx(*, is_leader: bool, registry: RateLimitRegistry) -> SweepContext:
    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return SweepContext(
        deps=_deps(is_leader=is_leader),
        backend=_PgShapedBackend(),  # type: ignore[arg-type]  # Why: test double for the Backend protocol
        clock=clock,
        worker_id=new_uuid(),
        rate_limit_registry=registry,
    )


def _seed_keyed_rate_limit(reg: RateLimitRegistry, *, idle_secs: float) -> TokenBucket:
    """Register a keyed rate-limit primitive stamped ``idle_secs`` ago.

    A redis-backend bucket is never exempt from idle eviction (the
    exemption only covers memory fixed-quota buckets holding consumed
    quota), so eviction removes both the primitive and its tracking entry.
    """
    bucket = TokenBucket(_KEYED_RL_NAME, capacity=10.0, refill_per_second=1.0)
    reg.register(bucket)
    reg._keyed_rate_limit_last_used[_KEYED_RL_NAME] = monotonic() - idle_secs  # pyright: ignore[reportPrivateUsage]  # Why: seeding keyed tracking state, same pattern as tests/test_leader_sweep_rl_registry.py
    return bucket


async def _drive_sweep_loop(
    ctx: SweepContext,
    *,
    seconds: float,
    stop_on: Callable[[], bool] | None = None,
) -> None:
    """Run ``_sweep_loop`` for up to *seconds* (or until *stop_on*), then stop it."""
    import taskq.worker._leader_sweeps as sweeps_mod

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the loop directly, matching tests/test_leader_sweep_rl_registry.py
    try:
        deadline = monotonic() + seconds
        while monotonic() < deadline:
            if stop_on is not None and stop_on():
                break
            await asyncio.sleep(0.01)
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@contextlib.contextmanager
def _recording_stale_batches() -> Generator[list[dict[str, Any]], None, None]:
    """Swap ``complete_stale_batches`` in the sweeps module for a recorder."""
    import taskq.worker._leader_sweeps as sweeps_mod

    calls: list[dict[str, Any]] = []

    async def _fake(conn: object, *, schema: str) -> int:
        calls.append({"schema": schema})
        return 0

    original = sweeps_mod.complete_stale_batches
    sweeps_mod.complete_stale_batches = _fake  # type: ignore[assignment]  # Why: test-only instrumentation.
    try:
        yield calls
    finally:
        sweeps_mod.complete_stale_batches = original  # type: ignore[assignment]


async def test_stale_batch_sweep_runs_for_leader_without_keyed_rate_limits() -> None:
    """A leader with a registry holding NO keyed refs (the default deployment
    shape) still runs the stale-batch completion sweep.

    Pre-fix the sweep was nested inside ``if rl.has_keyed_rate_limits:`` and
    never fired for this deployment shape at all.
    """
    registry = RateLimitRegistry()  # no keyed refs, no keyed rate limits
    ctx = _ctx(is_leader=True, registry=registry)

    with _recording_stale_batches() as calls:
        await _drive_sweep_loop(ctx, seconds=5.0, stop_on=lambda: bool(calls))

    assert calls, "leader must run complete_stale_batches even with no keyed rate limits"
    assert calls[0]["schema"] == ctx.deps.settings.schema_name


async def test_stale_batch_sweep_skipped_when_not_leader() -> None:
    """A NON-leader must not run the stale-batch sweep, even when its registry
    holds keyed rate-limit refs (the configuration that made the pre-fix
    un-gated block fire on every worker).

    Pre-fix the block had no leader gating: any worker with keyed refs ran
    the leader's sweep.
    """
    registry = RateLimitRegistry()
    # Fresh stamp: the entry is NOT idle, so eviction keeps it and
    # ``has_keyed_rate_limits`` stays true for the whole drive window.
    _seed_keyed_rate_limit(registry, idle_secs=0.0)
    ctx = _ctx(is_leader=False, registry=registry)

    with _recording_stale_batches() as calls:
        await _drive_sweep_loop(ctx, seconds=0.3)

    assert not calls, "non-leader must not run the leader's complete_stale_batches sweep"


async def test_keyed_rate_limit_eviction_still_runs_when_not_leader() -> None:
    """PR #42 semantics pin: keyed eviction is de-gated from leadership — a
    non-leader worker still evicts idle keyed rate limits from its OWN
    registry each tick. The stale-batch fix must not regress this."""
    registry = RateLimitRegistry()
    bucket = _seed_keyed_rate_limit(registry, idle_secs=7200.0)  # idle 2 h > 1 h threshold
    ctx = _ctx(is_leader=False, registry=registry)

    await _drive_sweep_loop(
        ctx,
        seconds=5.0,
        stop_on=lambda: _KEYED_RL_NAME not in registry._keyed_rate_limit_last_used,  # pyright: ignore[reportPrivateUsage]  # Why: asserting on the seeded private tracking dict.
    )

    assert _KEYED_RL_NAME not in registry._keyed_rate_limit_last_used  # pyright: ignore[reportPrivateUsage]
    assert bucket.name not in registry._rate_limits  # pyright: ignore[reportPrivateUsage]
