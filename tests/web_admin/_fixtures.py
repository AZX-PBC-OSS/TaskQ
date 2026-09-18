"""Fixtures for the taskq.web.admin test suite, registered from the root conftest.

These fixtures live here instead of ``tests/web_admin/conftest.py``.
Pytest 9.1.1 drops a nested conftest's fixtures for a file revisited
non-adjacently in the argument list: with
``pytest tests/web_admin/a.py tests/test_root.py tests/web_admin/b.py`` every
test in ``b.py`` errors with "fixture 'stub_pool' not found" while the same
files in adjacent order pass (pytest-dev/pytest#14971; the fix landed on
pytest main after 9.1.1 and is backported on the 9.1.x branch, but no release
carried it at the time of writing). Because ``tests/conftest.py`` is loaded
for every test regardless of argument order, registering the fixtures from
there removes the dependence on conftest adjacency.

The one autouse fixture here is path-gated: it mutates process env, so it
applies only to tests under ``tests/web_admin/``, not suite-wide.

Shared stub classes live in the package ``__init__.py`` so they can be
imported explicitly where type annotations need them.
"""

from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
import structlog
import structlog.types

from . import StubBackend as _StubBackend
from . import StubPool as _StubPool

_WEB_ADMIN_DIR = Path(__file__).resolve().parent

# ── Autouse fixtures ──────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _dev_env(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Set TASKQ_ENVIRONMENT=dev and TASKQ_ADMIN_ACTIONS_ENABLED=true for all
    web_admin tests so create_router's fail-closed auth check does not raise
    and mutation endpoints (run-now, retry, cancel) are accessible.

    TASKQ_ADMIN_UI_SECURE_COOKIES=false for the same reason: TestClient speaks
    http://testserver, and httpx (like a browser) refuses to store a Secure
    cookie received over plain http. With the production default the CSRF
    cookie would never reach the jar, so every GET-then-POST test would fail
    with a 403 that says nothing about the behaviour under test. Tests that
    assert on the Secure flag itself set the variable explicitly.

    Tests that need non-dev or actions-disabled behavior override these with
    their own monkeypatch.setenv.

    Registered from the root conftest (see the module docstring for why), so
    the env mutation is gated to ``tests/web_admin/`` paths: every other test
    in the suite runs with ambient env, exactly as before this fixture existed
    at that level.
    """
    path = getattr(request, "path", None)
    if path is None or not Path(path).is_relative_to(_WEB_ADMIN_DIR):
        return
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")


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
