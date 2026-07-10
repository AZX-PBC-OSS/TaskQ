"""Unit tests for worker health report redis_configured field."""

from unittest.mock import AsyncMock, MagicMock

from taskq.settings import WorkerSettings
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
