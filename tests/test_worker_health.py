"""Unit tests for worker health report redis_configured field."""

import time
from unittest.mock import AsyncMock, MagicMock

from taskq.settings import WorkerSettings
from taskq.worker._watchdog import LoopLiveness
from taskq.worker.deps import WorkerDeps
from taskq.worker.health import compute_health

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _make_settings(**overrides: str) -> WorkerSettings:
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


def _make_deps(settings: WorkerSettings) -> WorkerDeps:
    pool = AsyncMock()
    pool.acquire = MagicMock(return_value=pool._acquire_ctx)
    pool._acquire_ctx = AsyncMock()
    pool._acquire_ctx.__aenter__ = AsyncMock(return_value=AsyncMock())
    pool._acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )


# ── redis_configured reports truthfully ────────────────────────────


async def test_redis_configured_false_when_no_redis_url() -> None:
    """redis_configured is False when settings.redis_url is None."""
    settings = _make_settings()
    assert settings.redis_url is None
    deps = _make_deps(settings)
    report = await compute_health(deps)
    assert report.redis_configured is False


async def test_redis_configured_true_when_redis_url_set() -> None:
    """redis_configured is True when settings.redis_url is provided."""
    settings = _make_settings(TASKQ_REDIS_URL="redis://localhost:6379/0")
    deps = _make_deps(settings)
    report = await compute_health(deps)
    assert report.redis_configured is True


# ── Watchdog surfaces in compute_health ────────────────────────────────────


async def test_ready_includes_tick_ages_and_real_liveness() -> None:
    """The /ready body carries per-loop tick ages and a real live check."""
    settings = _make_settings()
    deps = _make_deps(settings)
    deps.liveness.tick("heartbeat", period=0.5)

    report = await compute_health(deps)

    assert report.live is True
    assert "heartbeat" in report.loop_tick_ages
    assert report.loop_tick_ages["heartbeat"] >= 0.0
    assert report.shutdown_elapsed_seconds is None
    assert report.ready is True


async def test_stale_loop_makes_worker_not_ready() -> None:
    """Zombie-ready must not pass: a loop silent far past its budget fails
    readiness even when PG pings fine."""
    settings = _make_settings()
    deps = _make_deps(settings)
    t = [time.monotonic()]
    deps.liveness = LoopLiveness(grace_factor=5.0, clock=lambda: t[0])
    deps.liveness.tick("heartbeat", period=0.5)
    t[0] += 120.0

    report = await compute_health(deps)

    assert report.ready is False
    assert any("stale_loops=heartbeat" in reason for reason in report.reasons)


async def test_shutdown_elapsed_reported_once_shutdown_starts() -> None:
    settings = _make_settings()
    deps = _make_deps(settings)
    deps.shutdown_started_at = time.monotonic() - 3.0

    report = await compute_health(deps)

    assert report.shutdown_elapsed_seconds is not None
    assert report.shutdown_elapsed_seconds >= 3.0
