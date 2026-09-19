"""Pytest fixtures for the taskq.web.progress test suite.

Pytest discovers conftest.py fixtures in the test file's directory and all
parent directories.  Test modules inside ``tests/web_progress/`` automatically
see every fixture defined here.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest

# ── Autouse fixtures ──────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Set TASKQ_ENVIRONMENT=dev for all web_progress tests so
    create_router's fail-closed auth check does not raise.

    These tests exercise SSE mechanics (subscribe-before-query, reconnect,
    keepalives, teardown, connection caps) against stub pools and pubsubs -
    none of them assert on authentication, and mounting the router is a
    prerequisite for all of them. The gate itself has its own test file
    (test_auth_gate.py), whose non-dev cases override this with their own
    monkeypatch.setenv, exactly as the web_admin suite's equivalent fixture
    allows.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")


@pytest.fixture(autouse=True)
async def _join_sse_starlette_shutdown_watcher() -> AsyncIterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Join the sse-starlette loop watcher a streaming test leaves behind.

    ``EventSourceResponse`` - the progress router's SSE response class -
    lazily starts ONE ``_shutdown_watcher`` task on the running loop the
    first time a response streams: a poller that parks until uvicorn sets
    ``AppStatus.should_exit``, a signal no in-process ``httpx.ASGITransport``
    test ever sends. The test cannot hold the task reference (sse-starlette
    creates it via ``loop.create_task`` and drops it), so the join targets
    the coroutine by name - the same identification the leaked-task guard's
    report uses. Cancel-and-await, never ``AppStatus.should_exit = True``:
    that class attribute is process-global, and flipping it here would make
    every LATER module's streams see a shutdown that never happened. The
    watcher's own ``finally`` resets its per-thread ``watcher_started``
    flag, so the next streaming test mints a fresh watcher this fixture
    joins in turn. Sync TestClient tests stream on their own portal
    thread's loop, never this one - the scan finds nothing and the fixture
    is a no-op for them.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop in this thread (a sync test, or a fixture
        # evaluated outside asyncio): nothing this test could have
        # streamed on the module loop is alive to join.
        yield
        return
    try:
        yield
    finally:
        watchers = [
            task
            for task in asyncio.all_tasks(loop)
            if not task.done() and getattr(task.get_coro(), "__name__", None) == "_shutdown_watcher"
        ]
        for task in watchers:
            task.cancel()
        for task in watchers:
            with contextlib.suppress(asyncio.CancelledError):
                await task
