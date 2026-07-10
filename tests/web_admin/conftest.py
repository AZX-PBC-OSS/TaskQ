"""Pytest fixtures for the taskq.web.admin test suite.

Pytest discovers conftest.py fixtures in the test file's directory and all
parent directories.  Test modules inside ``tests/web_admin/`` automatically
see every fixture defined here.

Shared stub classes live in the package ``__init__.py`` so they can be
imported explicitly where type annotations need them.
"""

from collections.abc import Callable, Generator
from typing import Any

import pytest
import structlog
import structlog.types

from . import StubBackend as _StubBackend
from . import StubPool as _StubPool

# ── Autouse fixtures ──────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Set TASKQ_ENVIRONMENT=dev and TASKQ_ADMIN_ACTIONS_ENABLED=true for all
    web_admin tests so create_router's fail-closed auth check does not raise
    and mutation endpoints (run-now, retry, cancel) are accessible.

    Tests that need non-dev or actions-disabled behavior override these with
    their own monkeypatch.setenv.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def structlog_capture() -> Generator[list[structlog.types.EventDict], None, None]:
    """Capture structlog events during a test and restore configuration after."""
    with structlog.testing.capture_logs() as logs:
        yield logs


@pytest.fixture()
def stub_pool() -> _StubPool:
    """Provide a fresh _StubPool instance for each test."""
    return _StubPool()


@pytest.fixture()
def make_app(stub_pool: _StubPool) -> Callable[..., Any]:
    """Factory fixture: returns a callable that creates a TestClient with a stub pool.

    Usage::

        def test_foo(make_app: Callable[..., TestClient]) -> None:
            client = make_app()                      # default
            client = make_app(auth_dependency=deny)   # with kwargs
    """
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import create_router, setup_admin_state

    def _factory(**router_kwargs: object) -> TestClient:
        bundle = create_router(stub_pool, **router_kwargs)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool; asyncpg.Pool is a protocol, _StubPool satisfies it at runtime.
        app = FastAPI()
        setup_admin_state(app, bundle)
        app.include_router(bundle.router)
        return TestClient(app)

    return _factory


@pytest.fixture()
def make_app_with_backend(stub_pool: _StubPool) -> Callable[..., Any]:
    """Factory fixture: returns a callable that creates a TestClient with a stub backend.

    Accepts a ``backend`` kwarg; defaults to a fresh :class:`StubBackend`.
    """
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import create_router, setup_admin_state

    def _factory(**router_kwargs: object) -> tuple[TestClient, _StubBackend]:
        backend = router_kwargs.pop("backend", _StubBackend())  # pyright: ignore[reportAssignmentType]
        bundle = create_router(stub_pool, backend=backend, **router_kwargs)  # pyright: ignore[reportArgumentType]
        app = FastAPI()
        setup_admin_state(app, bundle)
        app.include_router(bundle.router)
        return TestClient(app), backend  # pyright: ignore[reportReturnType]

    return _factory
