"""Unit tests for open_worker_deps's fire-and-forget-publish shutdown drain.

``open_worker_deps`` (src/taskq/worker/deps.py) registers a teardown
callback (``_drain_pending_publishes``) via ``stack.push_async_callback``
*after* the Redis client's own ``aclose`` callback — because
``AsyncExitStack`` unwinds LIFO, the drain runs BEFORE the client closes,
giving in-flight ``WorkerDeps.pending_publish_tasks`` up to 2 seconds to
finish before the connection they depend on goes away.

This machinery is exercised directly here for isolation — the drain
logic is also covered end-to-end via ``JobContext.progress()`` in
``tests/test_context_progress_background.py``, which verifies that
tasks are tracked in ``pending_publish_tasks`` and self-remove on
completion.

Fully mocks ``asyncpg.create_pool`` and ``open_dedicated_conn`` so no real
Postgres is required — mirrors the fake-pool/fake-connection conventions
used throughout the test suite (see ``tests/conftest.py::_FakePool`` and
``tests/test_worker_deps.py`` for the equivalent real-PG integration
coverage of ``open_worker_deps`` lifecycle/teardown ordering).
"""

import asyncio
from typing import Self

import pytest

from taskq.testing.settings import make_integration_settings
from taskq.worker import deps as deps_mod
from taskq.worker.deps import open_worker_deps


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    async def execute(self, *args: object, **kwargs: object) -> str:
        return "LISTEN"

    async def close(self) -> None:
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed


class _FakeAcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePool:
    def __init__(self) -> None:
        self.closing = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.closing = True

    def is_closing(self) -> bool:
        return self.closing

    def acquire(self, timeout: float | None = None) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(_FakeConn())


class _FakeRedisClient:
    def __init__(self) -> None:
        self.closed = False
        self.closed_while_task_pending = False

    async def aclose(self) -> None:
        self.closed = True


def _patch_pg_and_dedicated_conns(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_create_pool(*args: object, **kwargs: object) -> _FakePool:
        return _FakePool()

    async def _fake_open_dedicated_conn(
        dsn: str, *, label: str, apply_keepalive: bool = True
    ) -> _FakeConn:
        return _FakeConn()

    monkeypatch.setattr("asyncpg.create_pool", _fake_create_pool)
    monkeypatch.setattr(deps_mod, "open_dedicated_conn", _fake_open_dedicated_conn)


def _settings_with_fake_redis(monkeypatch: pytest.MonkeyPatch) -> tuple[object, _FakeRedisClient]:
    fake_client = _FakeRedisClient()

    def _fake_from_url(*args: object, **kwargs: object) -> _FakeRedisClient:
        return fake_client

    monkeypatch.setattr("redis.asyncio.from_url", _fake_from_url)

    settings = make_integration_settings(
        "postgresql://taskq:taskq@127.0.0.1:1/taskq",
        REDIS_URL="redis://127.0.0.1:1/0",
    )
    return settings, fake_client


# ── Drain waits for pending tasks before redis_client.aclose() ──────────


async def test_drain_awaits_pending_publish_task_before_redis_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A task in deps.pending_publish_tasks is awaited by the drain callback
    before the Redis client's aclose() runs (LIFO teardown ordering)."""
    _patch_pg_and_dedicated_conns(monkeypatch)
    settings, fake_client = _settings_with_fake_redis(monkeypatch)

    task_completed_before_close = False

    async def _slow_publish() -> None:
        nonlocal task_completed_before_close
        await asyncio.sleep(0.05)
        task_completed_before_close = not fake_client.closed

    async with open_worker_deps(settings) as deps:  # type: ignore[arg-type]
        assert deps.redis_client is fake_client
        task = asyncio.create_task(_slow_publish())
        deps.pending_publish_tasks.add(task)

    assert task.done()
    assert task_completed_before_close is True
    assert fake_client.closed is True


async def test_drain_completes_fast_when_publish_resolves_quickly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The drain does not block the full 2s bound when the task finishes fast —
    it returns as soon as asyncio.wait's awaited task completes."""
    _patch_pg_and_dedicated_conns(monkeypatch)
    settings, fake_client = _settings_with_fake_redis(monkeypatch)

    async def _quick_publish() -> None:
        await asyncio.sleep(0.01)

    loop = asyncio.get_running_loop()
    start = loop.time()

    async with open_worker_deps(settings) as deps:  # type: ignore[arg-type]
        task = asyncio.create_task(_quick_publish())
        deps.pending_publish_tasks.add(task)

    elapsed = loop.time() - start

    assert elapsed < 1.0
    assert task.done()
    assert fake_client.closed is True


async def test_drain_is_noop_when_no_pending_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No pending_publish_tasks — teardown proceeds without calling asyncio.wait
    on an empty set (asyncio.wait([]) raises ValueError if ever called)."""
    _patch_pg_and_dedicated_conns(monkeypatch)
    settings, fake_client = _settings_with_fake_redis(monkeypatch)

    async with open_worker_deps(settings) as deps:  # type: ignore[arg-type]
        assert deps.pending_publish_tasks == set()

    assert fake_client.closed is True


async def test_drain_does_not_run_when_redis_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No redis_url configured — redis_client is None and no drain callback
    is registered at all (the whole block is guarded by `if redis_client is not None`)."""
    _patch_pg_and_dedicated_conns(monkeypatch)
    settings = make_integration_settings("postgresql://taskq:taskq@127.0.0.1:1/taskq")

    async with open_worker_deps(settings) as deps:  # type: ignore[arg-type]
        assert deps.redis_client is None
        assert deps.pending_publish_tasks == set()


async def test_multiple_pending_publish_tasks_all_awaited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple concurrently-pending tasks are all awaited by asyncio.wait
    before teardown proceeds past the drain callback."""
    _patch_pg_and_dedicated_conns(monkeypatch)
    settings, fake_client = _settings_with_fake_redis(monkeypatch)

    completed: list[int] = []

    async def _publish(i: int) -> None:
        await asyncio.sleep(0.01 * (i + 1))
        completed.append(i)

    async with open_worker_deps(settings) as deps:  # type: ignore[arg-type]
        tasks = [asyncio.create_task(_publish(i)) for i in range(5)]
        deps.pending_publish_tasks.update(tasks)

    assert sorted(completed) == [0, 1, 2, 3, 4]
    assert fake_client.closed is True
