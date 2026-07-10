"""Unit tests for taskq.web.admin: create_router factory and route structure."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state


class _FakeConn:
    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        # Return empty — web admin unit tests verify route registration, CSRF,
        # and template rendering structure, not data content. Shape-specific
        # data (queues, schedules, jobs) would pollute cross-template rendering
        # (e.g. queue-shaped dicts crash the schedules template which expects
        # schedule fields). Integration tests in test_web_admin_integration.py
        # cover data rendering against real PG.
        return []

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        return None

    async def execute(self, query: str, *args: object) -> str:
        return "SELECT 1"


class _AcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self) -> None:
        self._conn = _FakeConn()

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


def _mount_router(
    pool: _StubPool,
    *,
    redis_client: Any | None = None,
    auth_dependency: Any | None = None,
    base_path: str = "",
) -> tuple[FastAPI, TestClient]:
    bundle = create_router(
        pool,  # pyright: ignore[reportArgumentType] # Why: test duck-type; _StubPool satisfies asyncpg.Pool contract at runtime.
        redis_client=redis_client,
        auth_dependency=auth_dependency,
        base_path=base_path,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    client = TestClient(app)
    return app, client


def _raise_http_401() -> None:
    raise HTTPException(status_code=401, detail="Unauthorized")


# ── create_router returns APIRouter with all routes ────────────


def test_router_has_all_routes() -> None:
    """create_router(mock_pool) returns an AdminBundle; bundle.router route paths include
    queues, /jobs/{job_id}, /workers, /schedules, /rate-limits, /reservations,
    leader, /sse/{topic}, /static/{path}, / (redirect)."""
    pool = _StubPool()
    bundle = create_router(
        pool,  # pyright: ignore[reportArgumentType] # Why: test duck-type; _StubPool satisfies asyncpg.Pool contract at runtime.
    )

    route_paths: set[str] = set()
    for route in bundle.router.routes:
        if hasattr(route, "path"):
            route_paths.add(route.path)  # pyright: ignore[reportAttributeAccessIssue] # Why: route.path is str; pyright cannot narrow Starlette BaseRoute union after hasattr guard.
        elif hasattr(route, "routes"):
            for sub in route.routes:  # pyright: ignore[reportAttributeAccessIssue, reportUnknownVariableType] # Why: sub-route iteration; Starlette BaseRoute types are not fully exposed.
                if hasattr(sub, "path"):
                    route_paths.add(sub.path)  # pyright: ignore[reportAttributeAccessIssue] # Why: same as above.

    expected = {
        "/",
        "/queues",
        "/queues/{queue:path}",
        "/jobs/{job_id}",
        "/workers",
        "/schedules",
        "/rate-limits",
        "/reservations",
        "/leader",
        "/sse/{topic}",
        "/static/{path:path}",
    }
    assert expected.issubset(route_paths), f"missing routes: {expected - route_paths}"


# ── no auth — all routes accessible ────────────────────────────


def test_no_auth_all_routes_accessible() -> None:
    """create_router(mock_pool, auth_dependency=None); GET /queues returns
    HTTP 200 (all routes accessible without auth)."""
    pool = _StubPool()
    _app, client = _mount_router(pool, auth_dependency=None)

    response = client.get("/admin/queues")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType] # Why: response.status_code type is unknown due to upstream Any.


# ── auth dep raises 401 — routes return 401 ────────────────────


def test_auth_dep_raises_401() -> None:
    """Mount with auth_dependency that raises HTTPException(401).
    GET /queues returns HTTP 401."""
    pool = _StubPool()
    _app, client = _mount_router(pool, auth_dependency=_raise_http_401)

    response = client.get("/admin/queues")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 401  # pyright: ignore[reportUnknownVariableType] # Why: response.status_code type is unknown due to upstream Any.


# ── real-time badge with redis_client ──────────────────────────


def test_real_time_badge_with_redis() -> None:
    """create_router(mock_pool, redis_client=<non-None mock>); GET /queues
    response HTML contains 'real-time mode'."""
    pool = _StubPool()
    redis_mock = MagicMock()
    redis_mock.ping = AsyncMock(return_value=True)
    _app, client = _mount_router(pool, redis_client=redis_mock)

    response = client.get("/admin/queues")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType] # Why: response.status_code type is unknown due to upstream Any.
    assert "real-time mode" in response.text


# ── polling badge without redis_client ─────────────────────────


def test_polling_badge_without_redis() -> None:
    """create_router(mock_pool, redis_client=None); GET /queues response
    HTML contains 'polling mode'."""
    pool = _StubPool()
    _app, client = _mount_router(pool, redis_client=None)

    response = client.get("/admin/queues")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType] # Why: response.status_code type is unknown due to upstream Any.
    assert "polling mode" in response.text


# ── SSE endpoint returns sentinel ───────────────────────────────


def test_sse_returns_sentinel(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /sse/queues returns HTTP 200, Content-Type text/event-stream,
    body contains 'awaiting_progress_backend'."""
    from collections.abc import AsyncIterator

    from taskq.web.admin import sse as sse_mod

    async def _terminating_gen(
        semaphore: asyncio.Semaphore,
        pool: object,
        schema: object,
        topic: str,
    ) -> AsyncIterator[str]:
        try:
            from taskq import _json

            sentinel_data = _json.dumps_str({"status": "awaiting_progress_backend"})
            yield f"event: status\ndata: {sentinel_data}\n\n"
        finally:
            semaphore.release()

    monkeypatch.setattr(sse_mod, "_sse_generator", _terminating_gen)

    pool = _StubPool()
    _app, client = _mount_router(pool)

    response = client.get("/admin/sse/queues")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType] # Why: response.status_code type is unknown due to upstream Any.
    content_type = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType] # Why: headers.get return is Any; pyright cannot narrow.
    assert "text/event-stream" in content_type
    assert "awaiting_progress_backend" in response.text


# ── WARNING logged when no auth in non-dev environment ──────────


def test_warning_no_auth_non_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set TASKQ_ENVIRONMENT=production and TASKQ_ADMIN_UI_REQUIRE_AUTH=false;
    call create_router(mock_pool, auth_dependency=None); WARNING-level structlog
    event 'admin-ui-no-auth' emitted (opt-out path — fail-closed is tested in
    test_admin_security_fixes.py)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")
    monkeypatch.setenv("TASKQ_ADMIN_UI_REQUIRE_AUTH", "false")
    pool = _StubPool()

    import structlog

    with structlog.testing.capture_logs() as captured:
        create_router(
            pool,  # pyright: ignore[reportArgumentType] # Why: test duck-type.
            auth_dependency=None,
        )

    warnings = [e for e in captured if e.get("event") == "admin-ui-no-auth"]
    assert len(warnings) >= 1


# ── no WARNING when no auth in dev environment ─────────────────


def test_no_warning_in_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set TASKQ_ENVIRONMENT=dev; call create_router(mock_pool,
    auth_dependency=None); no 'admin-ui-no-auth' WARNING emitted."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    pool = _StubPool()

    import structlog

    with structlog.testing.capture_logs() as captured:
        create_router(
            pool,  # pyright: ignore[reportArgumentType] # Why: test duck-type.
            auth_dependency=None,
        )

    warnings = [e for e in captured if e.get("event") == "admin-ui-no-auth"]
    assert len(warnings) == 0


# ── SSE 429 on concurrency exhaustion ──────────────────────────


@pytest.mark.asyncio
async def test_sse_429_on_concurrency_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify SSE 429 on concurrency exhaustion. Set
    TASKQ_ADMIN_MAX_SSE_CONNECTIONS=1; make two concurrent requests to /sse/queues;
    one returns HTTP 200 and the other returns HTTP 429."""
    from collections.abc import AsyncIterator

    from taskq.web.admin import sse as sse_mod

    async def _terminating_gen(
        semaphore: asyncio.Semaphore,
        pool: object,
        schema: object,
        topic: str,
    ) -> AsyncIterator[str]:
        try:
            from taskq import _json

            sentinel_data = _json.dumps_str({"status": "awaiting_progress_backend"})
            yield f"event: status\ndata: {sentinel_data}\n\n"
            await asyncio.sleep(0.5)
        finally:
            semaphore.release()

    monkeypatch.setattr(sse_mod, "_sse_generator", _terminating_gen)

    monkeypatch.setenv("TASKQ_ADMIN_MAX_SSE_CONNECTIONS", "1")

    monkeypatch.setattr(sse_mod, "_TOPIC_SEMAPHORES", {})

    pool = _StubPool()
    bundle = create_router(
        pool,  # pyright: ignore[reportArgumentType] # Why: test duck-type.
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")

    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        results: list[int] = []

        async def _request() -> None:
            try:
                resp = await client.get("/admin/sse/queues", timeout=5.0)
                results.append(resp.status_code)
            except Exception:
                results.append(0)

        task1 = asyncio.create_task(_request())
        task2 = asyncio.create_task(_request())
        await asyncio.gather(task1, task2)

        assert 200 in results, f"expected at least one 200, got {results}"
        assert 429 in results, f"expected at least one 429, got {results}"


# ── CSRF token missing from POST returns 403 ────────────────────


def test_csrf_missing_returns_403() -> None:
    """POST /admin/jobs/{id}/cancel without CSRF token returns HTTP 403."""
    pool = _StubPool()
    _app, client = _mount_router(pool)

    response = client.post(f"/admin/jobs/{_FAKE_UUID}/cancel")

    assert response.status_code == 403


# ── CSRF token mismatch returns 403 ──────────────────────────────


def test_csrf_mismatch_returns_403() -> None:
    """POST /admin/jobs/{id}/cancel with wrong CSRF token returns HTTP 403."""
    pool = _StubPool()
    _app, client = _mount_router(pool)

    client.cookies.set("taskq_csrf_token", "cookie-token-value")
    response = client.post(
        f"/admin/jobs/{_FAKE_UUID}/cancel",
        data={"csrf_token": "different-form-value"},
    )

    assert response.status_code == 403


# ── CSRF token match allows POST through ─────────────────────────


def test_csrf_match_allows_post() -> None:
    """POST /admin/schedules/{id}/skip with matching CSRF token
    passes validation (returns 404 because schedule does not exist, not 403)."""
    pool = _StubPool()
    _app, client = _mount_router(pool)

    token = "matching-csrf-token-value"
    client.cookies.set("taskq_csrf_token", token)
    response = client.post(
        f"/admin/schedules/{_FAKE_UUID}/skip",
        data={"csrf_token": token},
    )

    assert response.status_code != 403


# ── GET schedules page sets CSRF cookie ──────────────────────────


def test_get_schedules_sets_csrf_cookie() -> None:
    """GET /admin/schedules response includes taskq_csrf_token cookie."""
    pool = _StubPool()
    _app, client = _mount_router(pool)

    response = client.get("/admin/schedules")

    assert response.status_code == 200
    cookie = response.cookies.get("taskq_csrf_token")
    assert cookie is not None
    assert len(cookie) == 64


_FAKE_UUID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture(autouse=True)
def _dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Set TASKQ_ENVIRONMENT=dev for all unit tests so create_router's
    fail-closed auth check does not raise. Tests that need a non-dev
    environment override this with their own monkeypatch.setenv."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
