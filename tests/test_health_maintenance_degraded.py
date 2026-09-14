"""maintenance_health: the degraded-maintenance view in the readiness body.

Drives the helper directly against monkeypatched obs caches, then pins that
build_ready_body carries the ``maintenance`` key — including that a degraded
maintenance view never flips ``ready`` (degraded is not unready).
"""

import json
import time
from types import SimpleNamespace

import pytest

from taskq.obs import (
    _otel,  # pyright: ignore[reportPrivateUsage]  # Why: the caches under test are module-level singletons; monkeypatching them in place is the only seam the helper has.
)
from taskq.settings import WorkerSettings
from taskq.worker.health import (
    HealthReport,
    build_ready_body,
    maintenance_health,
)
from taskq.worker.shutdown import ShutdownPhase

# A non-default sweep interval (10 s) proves the helper reads the setting
# rather than a hardcoded cadence: the stall threshold becomes 30 s.
_PG_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"


def _settings() -> WorkerSettings:
    # Real settings, not a hand-listed SimpleNamespace: maintenance_health
    # reads tuning fields (sweep_interval, event_writer_batch_size) and a
    # stub that enumerates them turns every new read into an AttributeError
    # rather than a behaviour change — the same reasoning as the watchdog
    # health tests.
    return WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _PG_DSN, "TASKQ_SWEEP_INTERVAL": "10"})


def _patch_caches(
    monkeypatch: pytest.MonkeyPatch,
    success: dict[str, float],
    batch: dict[str, int],
) -> None:
    # Replace, never mutate: the real dicts are process-wide singletons and
    # in-place writes would leak into other tests.
    monkeypatch.setattr(_otel, "_sweep_success_cache", success)
    monkeypatch.setattr(_otel, "_sweep_batch_size_cache", batch)


def _report() -> HealthReport:
    return HealthReport(
        live=True,
        ready=True,
        reasons=[],
        shutdown_phase=ShutdownPhase.NONE,
        heartbeat_failures=0,
        max_heartbeat_failures=3,
        is_leader=True,
        redis_configured=False,
        pg_ping_ok=True,
        pg_ping_latency_ms=0.0,
        active_jobs=0,
        loop_tick_ages={},
        shutdown_elapsed_seconds=None,
    )


# ── (a) fresh process: empty success cache ─────────────────────────────


def test_fresh_cache_not_degraded_with_informational_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No sweep has completed yet → degraded False, gap named in reasons."""
    _patch_caches(monkeypatch, success={}, batch={})

    view = maintenance_health(_settings())

    assert view["degraded"] is False
    assert view["reasons"] == ["no sweep has completed yet"]


# ── (b) staleness: three whole intervals ───────────────────────────────


def test_stale_sweep_stamp_degrades(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sweep missing > 3 * sweep_interval is stalled → degraded True."""
    settings = _settings()
    # 4 intervals of age against a 3-interval threshold (sweep_interval=10).
    stale = time.time() - 4 * settings.sweep_interval
    _patch_caches(monkeypatch, success={"scheduled_to_pending": stale}, batch={})

    view = maintenance_health(settings)

    assert view["degraded"] is True
    assert any(r.startswith("sweep=scheduled_to_pending stalled ") for r in view["reasons"]), (
        f"expected a stalled reason, got {view['reasons']!r}"
    )


def test_slow_but_not_stalled_sweep_stays_healthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Staleness under 3 * sweep_interval is slow, not stalled → not degraded."""
    settings = _settings()
    # 2.5 intervals of age: jitter/slow, not three whole missed intervals.
    lagging = time.time() - 2.5 * settings.sweep_interval
    _patch_caches(monkeypatch, success={"scheduled_to_pending": lagging}, batch={})

    view = maintenance_health(settings)

    assert view["degraded"] is False
    assert view["reasons"] == []


# ── (c) reduced batch tier ─────────────────────────────────────────────


def test_reduced_batch_tier_degrades(monkeypatch: pytest.MonkeyPatch) -> None:
    """A batch size below event_writer_batch_size is the reduced tier → degraded."""
    settings = _settings()
    _patch_caches(
        monkeypatch,
        success={"scheduled_to_pending": time.time()},
        batch={"scheduled_to_pending": settings.event_writer_batch_size // 4},
    )

    view = maintenance_health(settings)

    assert view["degraded"] is True
    expected = (
        f"sweep=scheduled_to_pending batch size degraded to {settings.event_writer_batch_size // 4}"
    )
    assert expected in view["reasons"], f"expected {expected!r}, got {view['reasons']!r}"


# ── (d) healthy stamps ─────────────────────────────────────────────────


def test_healthy_sweeps_report_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fresh success stamp + full batch size → degraded False, no reasons."""
    settings = _settings()
    _patch_caches(
        monkeypatch,
        success={"scheduled_to_pending": time.time()},
        batch={"scheduled_to_pending": settings.event_writer_batch_size},
    )

    view = maintenance_health(settings)

    assert view["degraded"] is False
    assert view["reasons"] == []


# ── build_ready_body carries the maintenance view ──────────────────────


def test_ready_body_carries_maintenance_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_ready_body embeds maintenance_health's view verbatim."""
    _patch_caches(monkeypatch, success={}, batch={})
    settings = _settings()
    deps = SimpleNamespace(shutdown_phase=ShutdownPhase.NONE, settings=settings)

    body = json.loads(build_ready_body(_report(), deps))  # pyright: ignore[reportArgumentType]  # Why: test duck-type deps; same suppression pattern as the existing health body tests.

    assert body["maintenance"] == {
        "degraded": False,
        "reasons": ["no sweep has completed yet"],
    }


def test_degraded_maintenance_does_not_flip_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A degraded maintenance view stays a body signal — ready is untouched."""
    settings = _settings()
    stale = time.time() - 4 * settings.sweep_interval
    _patch_caches(monkeypatch, success={"scheduled_to_pending": stale}, batch={})
    deps = SimpleNamespace(shutdown_phase=ShutdownPhase.NONE, settings=settings)

    body = json.loads(build_ready_body(_report(), deps))  # pyright: ignore[reportArgumentType]  # Why: test duck-type deps; same suppression pattern as the existing health body tests.

    assert body["ready"] is True
    assert body["maintenance"]["degraded"] is True
