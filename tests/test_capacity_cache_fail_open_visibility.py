"""A backpressure gate that relaxes when the DB is sick must be alertable.

`ActorCapacityCache` fails OPEN on refresh failure: it keeps the last snapshot
(or, with none, falls back to the `@actor` literal) and stamps `_refreshed_at`
so the state persists a full TTL rather than re-querying a sick backend on every
enqueue. That design is deliberate and sound -- turning a degraded database into
a per-enqueue query storm would be worse.

What was missing is a signal. The only trace was a warning log, so an operator
whose tightened `max_pending` had silently stopped being enforced had nothing to
alert on. The worst case is the first-ever refresh failing at process start:
there is no stored data at all, so every enqueue enforces the code literal
instead of the operator's cap, indefinitely while the backend stays sick.

These tests do not change the fail-open behaviour -- they pin that it stays
fail-open AND is now observable, and that the two degradation modes are
distinguishable.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog.testing

from taskq.client._capacity import ActorCapacityCache


class _Backend:
    """Backend whose capacity read can be switched between good and broken."""

    def __init__(self, rows: dict[str, int] | None = None) -> None:
        self.rows = rows or {}
        self.fail = False
        self.calls = 0

    async def get_actor_max_pending(self) -> dict[str, int]:
        self.calls += 1
        if self.fail:
            raise ConnectionError("pg unreachable")
        return dict(self.rows)


def _cache(backend: Any, ttl: float = 5.0) -> ActorCapacityCache:
    return ActorCapacityCache(backend, ttl=ttl)


async def test_first_refresh_failure_falls_back_to_the_literal() -> None:
    """Behaviour is unchanged: fail-open, not fail-closed."""
    backend = _Backend()
    backend.fail = True
    cache = _cache(backend)

    assert await cache.effective_max_pending("a", literal=10) == 10


async def test_first_refresh_failure_is_flagged_as_no_snapshot() -> None:
    """The materially worse mode must be distinguishable from stale-serving."""
    backend = _Backend()
    backend.fail = True
    cache = _cache(backend)

    with structlog.testing.capture_logs() as logs:
        await cache.effective_max_pending("a", literal=10)

    entry = next(e for e in logs if e["event"] == "actor-capacity-cache-refresh-failed")
    assert entry["has_snapshot"] is False
    assert entry["degraded_to_literal"] is True
    assert entry["log_level"] == "warning"


async def test_later_failure_keeps_serving_the_stored_cap() -> None:
    """With a snapshot, the operator's cap is still enforced -- a different and
    much less dangerous degradation than falling back to the literal."""
    backend = _Backend({"a": 3})
    cache = _cache(backend, ttl=0.01)
    assert await cache.effective_max_pending("a", literal=999) == 3

    backend.fail = True
    await asyncio.sleep(0.02)  # let the TTL expire

    with structlog.testing.capture_logs() as logs:
        assert await cache.effective_max_pending("a", literal=999) == 3

    entry = next(e for e in logs if e["event"] == "actor-capacity-cache-refresh-failed")
    assert entry["has_snapshot"] is True
    assert entry["degraded_to_literal"] is False


async def test_failure_bumps_the_counter_with_the_degradation_mode() -> None:
    import taskq.client._capacity as cap_mod

    recorded: list[bool] = []
    original = cap_mod.record_capacity_refresh_failure

    def _spy(*, has_snapshot: bool) -> None:
        recorded.append(has_snapshot)

    cap_mod.record_capacity_refresh_failure = _spy  # type: ignore[assignment]  # Why: test-only instrumentation.
    try:
        backend = _Backend()
        backend.fail = True
        await _cache(backend).effective_max_pending("a", literal=10)
    finally:
        cap_mod.record_capacity_refresh_failure = original  # type: ignore[assignment]

    assert recorded == [False], "the no-snapshot mode must be reported"


async def test_retry_rate_is_still_bounded_by_the_ttl() -> None:
    """The stamp-on-failure behaviour is load-bearing: without it a sick
    backend gets queried on every enqueue."""
    backend = _Backend()
    backend.fail = True
    cache = _cache(backend, ttl=60.0)

    for _ in range(5):
        await cache.effective_max_pending("a", literal=10)

    assert backend.calls == 1, "a failed refresh must not be retried per enqueue"


async def test_recovery_restores_the_stored_cap() -> None:
    backend = _Backend({"a": 2})
    backend.fail = True
    cache = _cache(backend, ttl=0.01)
    assert await cache.effective_max_pending("a", literal=99) == 99

    backend.fail = False
    await asyncio.sleep(0.02)
    assert await cache.effective_max_pending("a", literal=99) == 2


def test_counter_is_not_gated_on_otel_enabled() -> None:
    """Same rule as record_backpressure_error: safety-critical signals are
    counted even with telemetry off."""
    import inspect

    from taskq.obs import _otel

    src = inspect.getsource(_otel.record_capacity_refresh_failure)
    # Strip the docstring: it mentions _otel_enabled by name to explain the
    # exemption, so a naive substring check would match its own rationale.
    body = src.split('"""')[-1]
    assert "_otel_enabled" not in body
