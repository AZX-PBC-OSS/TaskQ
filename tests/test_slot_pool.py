"""The per-slot transaction pool: sizing, supply, and readiness probe.

Three behaviours this file pins, each at the level that can actually
see its failure mode:

* **Factory sizing** (unit, spied ``asyncpg.create_pool`` /
  ``make_pg_pool_factory`` — the ``tests/test_auth.py`` precedent): the
  pool the worker opens is fully warmed at ``max_concurrency + 1`` —
  one connection per consumer slot plus the readiness reserve — on the
  direct DSN with the dispatcher command timeout. Under-sizing is the
  defect class where a fully-utilised worker's readiness ping times
  out waiting on a slot-held connection and reports health as unready.
* **Supply rule** (integration, real PG): ``max_concurrency + 1``
  concurrent acquires all succeed — the pool genuinely holds a
  connection per slot plus the reserve, not just a configured number.
* **Single-flight readiness** (unit): concurrent probes share ONE
  in-flight slot-pool ping, which is what makes the one-connection
  reserve sufficient; and a failing shared probe fails every waiter
  that joined it.
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import _slot_pool_factory
from taskq.worker.health import _ping_slot_pool


def _make_settings(*, max_concurrency: int = 4, pg_dsn_direct: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn_direct,
            "TASKQ_PG_DSN_DIRECT": pg_dsn_direct,
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
        }
    )


# ── Factory sizing ───────────────────────────────────────────────────────


async def test_slot_pool_factory_sizes_warm_pool_on_direct_dsn() -> None:
    """min_size == max_size == max_concurrency + 1, direct DSN, command
    timeout — the fully-warmed shape whose absence puts connection
    establishment (and a credential fetch) inside the dispatch hot path."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")
    fake_pool = MagicMock()
    create_pool = AsyncMock(return_value=fake_pool)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.asyncpg.create_pool", create_pool)
        factory = _slot_pool_factory(settings, None)
        pool = await factory()

    assert pool is fake_pool
    call_kwargs = create_pool.call_args.kwargs
    assert call_kwargs["dsn"] == "postgresql://u:p@h:5432/db"
    assert call_kwargs["min_size"] == 5
    assert call_kwargs["max_size"] == 5
    assert call_kwargs["command_timeout"] == settings.dispatcher_command_timeout
    assert call_kwargs["max_inactive_connection_lifetime"] == settings.pool_max_inactive_lifetime


async def test_slot_pool_factory_is_provider_backed_when_given_provider() -> None:
    """With a credential provider the factory comes from
    make_pg_pool_factory — the documented managed-identity path — sized
    and timed out identically, so switching authentication never changes
    the pool's budget."""
    settings = _make_settings(max_concurrency=4, pg_dsn_direct="postgresql://u:p@h:5432/db")
    provider = MagicMock()
    fake_factory = MagicMock()
    make_pg_pool_factory = MagicMock(return_value=fake_factory)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("taskq.worker._bootstrap.make_pg_pool_factory", make_pg_pool_factory)
        factory = _slot_pool_factory(settings, provider)

    assert factory is fake_factory
    make_pg_pool_factory.assert_called_once_with(
        "postgresql://u:p@h:5432/db",
        provider,
        min_size=5,
        max_size=5,
        max_inactive_connection_lifetime=settings.pool_max_inactive_lifetime,
        command_timeout=settings.dispatcher_command_timeout,
    )


# ── Supply rule (real PG) ────────────────────────────────────────────────


@pytest.mark.integration
async def test_slot_pool_supplies_concurrency_plus_one_concurrent_acquires(
    module_pg_schema: Any,
) -> None:
    """max_concurrency + 1 connections are concurrently acquirable.

    The behavioral half of the sizing statement: every consumer slot
    can hold its transaction connection at once, with the one reserve
    still free for the readiness probe.
    """
    settings = _make_settings(max_concurrency=3, pg_dsn_direct=module_pg_schema.pg_dsn)
    factory = _slot_pool_factory(settings, None)
    pool = await factory()
    try:
        conns = await asyncio.wait_for(
            asyncio.gather(*(pool.acquire() for _ in range(4))),
            timeout=settings.dispatcher_command_timeout,
        )
        try:
            # All four holds are live at once — supply, not configuration.
            for conn in conns:
                await conn.execute("SELECT 1")
        finally:
            for conn in conns:
                await pool.release(conn)
    finally:
        await pool.close()


# ── Single-flight readiness probe ────────────────────────────────────────


class _SlowPool:
    """Slot-pool stand-in whose acquire is slow enough for callers to
    overlap, counting every acquire — the single-flight discriminator."""

    def __init__(self, *, delay: float = 0.05, error: Exception | None = None) -> None:
        self.delay = delay
        self.error = error
        self.acquire_calls = 0

    def acquire(self, *, timeout: float | None = None) -> _SlowAcquireCtx:
        self.acquire_calls += 1
        return _SlowAcquireCtx(self)


class _SlowAcquireCtx:
    def __init__(self, pool: _SlowPool) -> None:
        self._pool = pool

    async def __aenter__(self) -> Any:
        await asyncio.sleep(self._pool.delay)
        if self._pool.error is not None:
            raise self._pool.error
        return SimpleNamespace(execute=AsyncMock(return_value="OK"))

    async def __aexit__(self, *_exc: object) -> None:
        return None


def _deps_with_pool(pool: _SlowPool) -> SimpleNamespace:
    return SimpleNamespace(
        slot_pool=pool,
        # The single-flight probe state lives on deps (per-worker), so
        # each test's fresh deps starts with no in-flight probe.
        slot_pool_probe_task=None,
        settings=SimpleNamespace(health_pg_ping_timeout=1.0),
    )


async def test_concurrent_probes_share_one_in_flight_ping() -> None:
    pool = _SlowPool()
    deps = _deps_with_pool(pool)

    results = await asyncio.gather(*(_ping_slot_pool(deps) for _ in range(3)))

    assert pool.acquire_calls == 1, (
        "concurrent readiness requests must join one in-flight probe — "
        "parallel pings contend for the single readiness-reserve "
        "connection and report a healthy, busy worker as unready"
    )
    assert results == [(True, None), (True, None), (True, None)]


async def test_probe_state_is_per_worker_not_process_global() -> None:
    """Two workers can share a process; one worker's in-flight probe must
    never answer another worker's readiness request — worker B would be
    told about worker A's pool. A module-global probe (the shape this
    per-deps state replaced) coalesces B's ping onto A's probe and
    never touches B's pool at all."""
    pool_a = _SlowPool()
    pool_b = _SlowPool()
    deps_a = _deps_with_pool(pool_a)
    deps_b = _deps_with_pool(pool_b)

    results = await asyncio.gather(
        _ping_slot_pool(deps_a),
        _ping_slot_pool(deps_b),
    )

    assert pool_a.acquire_calls == 1, "worker A's probe must ping A's pool once"
    assert pool_b.acquire_calls == 1, (
        "worker B's readiness request was answered by worker A's probe — "
        "probe state has leaked across workers"
    )
    assert results == [(True, None), (True, None)]


async def test_wedged_probe_degrades_to_a_failed_ping_not_a_hang() -> None:
    """A probe task left over from a torn-down event loop can never
    complete; joining it must be bounded by one ping budget and report
    unready, never hang readiness (the constitution's every-wait-bounded
    rule). Simulated with a probe task that never completes."""
    pool = _SlowPool()
    deps = _deps_with_pool(pool)
    deps.settings = SimpleNamespace(health_pg_ping_timeout=0.05)

    never_completes = asyncio.ensure_future(asyncio.Event().wait())
    deps.slot_pool_probe_task = never_completes

    result = await _ping_slot_pool(deps)

    assert result == (False, "slot_pool_ping_timeout")
    assert pool.acquire_calls == 0, "the wedged probe must be joined, not replaced"
    never_completes.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await never_completes


async def test_shared_probe_failure_fails_every_waiter() -> None:
    pool = _SlowPool(error=TimeoutError())
    deps = _deps_with_pool(pool)

    results = await asyncio.gather(
        *(_ping_slot_pool(deps) for _ in range(3)), return_exceptions=False
    )

    assert pool.acquire_calls == 1
    ok = [r[0] for r in results]
    assert ok == [False, False, False]
    assert all(r[1] == "slot_pool_ping_timeout" for r in results)


async def test_cancelled_waiter_does_not_cancel_the_shared_probe() -> None:
    """A cancelled readiness request must not take the shared probe with
    it — the next waiter still joins the SAME in-flight ping, which is
    what keeps the one-connection reserve sufficient. A shield-less
    implementation fails here: the first cancellation kills the probe
    and the survivor pays a second acquire (and the reserve contention
    the single flight exists to prevent)."""
    pool = _SlowPool(delay=0.1)
    deps = _deps_with_pool(pool)

    waiter_to_cancel = asyncio.create_task(_ping_slot_pool(deps))
    await asyncio.sleep(0.01)  # let it start the probe
    survivor = asyncio.create_task(_ping_slot_pool(deps))
    await asyncio.sleep(0.01)  # let it join the in-flight probe
    waiter_to_cancel.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await waiter_to_cancel

    survivor_result = await survivor

    assert pool.acquire_calls == 1, (
        "the cancelled waiter took the shared probe with it — the "
        "survivor had to start a second acquire"
    )
    assert survivor_result == (True, None)


async def test_sequential_probes_each_ping_freshly() -> None:
    """A probe arriving after the previous one completed starts a new
    one — single-flight coalesces concurrent probes, never serves a
    stale result."""
    pool = _SlowPool(delay=0.0)
    deps = _deps_with_pool(pool)

    await _ping_slot_pool(deps)
    await _ping_slot_pool(deps)

    assert pool.acquire_calls == 2
