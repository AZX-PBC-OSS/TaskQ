"""Unit tests for drain monitor and until-idle mode."""

from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool


def test_worker_deps_drain_failures_default_zero() -> None:
    """WorkerDeps initializes drain_failures to 0."""
    settings = WorkerSettings.load_from_dict({
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEALTH_SOCKET_PATH": "/tmp/test_drain_deps.sock",  # noqa: S108
    })
    pool = _FakePool()
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    assert deps.drain_failures == 0
