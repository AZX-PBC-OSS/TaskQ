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

import pytest
import structlog.testing
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.client._capacity import ActorCapacityCache
from taskq.testing.otel import counter_data_points

_FAILURE_COUNTER = "taskq.backpressure.capacity_refresh_failures"


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


def test_counter_still_records_with_telemetry_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same rule as record_backpressure_error: a safety-critical signal is
    counted even with telemetry off.

    The distinction is real and load-bearing — most TaskQ counters no-op on
    `_otel_enabled=False`. This one must not, because the condition it reports
    is a silently relaxed backpressure gate. Recorded through the real
    instrument rather than inferred from the function's source text, which
    could not tell an `_otel_enabled` check in the body from one the compiler
    never reaches.
    """
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME, otel_mod._version()
    )
    monkeypatch.setattr(
        otel_mod,
        "_capacity_refresh_failures",
        meter.create_counter(_FAILURE_COUNTER, unit="1"),
    )
    monkeypatch.setattr(otel_mod, "_otel_enabled", False)

    otel_mod.record_capacity_refresh_failure(has_snapshot=False)
    otel_mod.record_capacity_refresh_failure(has_snapshot=True)

    points = counter_data_points(reader, _FAILURE_COUNTER)
    assert {(p.attributes or {})["degraded"] for p in points} == {
        "no_snapshot",
        "stale_snapshot",
    }, f"both degradation modes must stay distinguishable with telemetry off, got {points}"
    assert sum(int(p.value) for p in points) == 2
