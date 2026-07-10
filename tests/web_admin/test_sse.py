"""Tests for the SSE endpoint and template scaffold in taskq.web.admin.sse."""

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

import taskq.web.admin.sse as _sse_mod
from taskq.web.admin import create_router
from taskq.web.admin.sse import _TOPIC_SEMAPHORES, _sse_generator

from . import _StubPool


async def _finite_sse_generator(
    semaphore: asyncio.Semaphore,
    pool: object | None,
    schema: str | None,
    topic: str,
) -> AsyncIterator[str]:
    try:
        sentinel_data = '{"status": "awaiting_progress_backend"}'
        yield f"event: status\ndata: {sentinel_data}\n\n"
    finally:
        semaphore.release()


# ── SSE endpoint: route discovery ────────────────────────────────────────


def test_sse_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /sse/{topic} route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/sse/{topic}" in route_paths  # pyright: ignore[reportUnknownVariableType]


# ── SSE sentinel event ─────────────────────────────────────────────────


async def test_sse_generator_yields_sentinel() -> None:
    """The SSE async generator yields the sentinel event as its first output."""
    sem = asyncio.Semaphore(1)
    gen = _sse_generator(sem, None, None, "queues")
    first = await gen.__anext__()
    assert first == 'event: status\ndata: {"status":"awaiting_progress_backend"}\n\n'
    await gen.aclose()


async def test_sse_generator_releases_semaphore_on_close() -> None:
    """The generator releases the semaphore when closed (connection drop).

    The endpoint handler acquires the semaphore before creating the generator,
    and the generator releases it in its ``finally`` block.  The test must
    pre-acquire to simulate the real call path.
    """
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    assert sem._value == 0  # pyright: ignore[reportPrivateUsage]  # Why: no public API to read semaphore value; needed to verify permit state.
    gen = _sse_generator(sem, None, None, "queues")
    await gen.__anext__()
    await gen.aclose()
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: same as above.


# ── SSE response headers and content type ───────────────────────────────


def test_sse_endpoint_returns_event_stream_content_type(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /sse/{topic} returns text/event-stream content type.

    The infinite-hold generator is replaced with a finite one so TestClient
    can read the full response without hanging.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(_sse_mod, "_sse_generator", _finite_sse_generator)
    client = make_app()
    response = client.get("/sse/queues")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/event-stream" in ct


def test_sse_endpoint_sets_cache_control_no_cache(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """SSE response includes Cache-Control: no-cache header."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(_sse_mod, "_sse_generator", _finite_sse_generator)
    client = make_app()
    response = client.get("/sse/queues")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.headers.get("cache-control") == "no-cache"  # pyright: ignore[reportUnknownVariableType]


def test_sse_endpoint_sets_x_accel_buffering_no(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """SSE response includes X-Accel-Buffering: no header."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(_sse_mod, "_sse_generator", _finite_sse_generator)
    client = make_app()
    response = client.get("/sse/queues")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.headers.get("x-accel-buffering") == "no"  # pyright: ignore[reportUnknownVariableType]


def test_sse_endpoint_sentinel_in_response_body(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """SSE response body contains the sentinel event."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(_sse_mod, "_sse_generator", _finite_sse_generator)
    client = make_app()
    response = client.get("/sse/queues")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert "event: status" in response.text  # pyright: ignore[reportUnknownVariableType]
    assert "awaiting_progress_backend" in response.text  # pyright: ignore[reportUnknownVariableType]


# ── SSE 429 on semaphore exhaustion ────────────────────────────────────


async def test_sse_429_when_semaphore_exhausted(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /sse/{topic} returns HTTP 429 when the connection limit is reached.

    The semaphore is exhausted by directly holding a permit (simulating a
    held-open connection), then verifying the endpoint rejects the next
    request with 429.
    """
    _TOPIC_SEMAPHORES.clear()
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_MAX_SSE_CONNECTIONS", "1")
    client = make_app()
    semaphore = _sse_mod._get_semaphore("queues", 1)  # pyright: ignore[reportPrivateUsage]  # Why: need to access the module's semaphore to simulate a held connection; no public accessor exists.
    await semaphore.acquire()
    response = client.get("/sse/queues")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 429  # pyright: ignore[reportUnknownVariableType]
    semaphore.release()
    _TOPIC_SEMAPHORES.clear()


# ── SSE template scaffold ──────────────────────────────────────────────


def test_sse_console_template_has_htmx_sse_attributes(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """sse_console.html scaffold includes hx-ext and sse-connect attributes."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "_partials/sse_console.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'hx-ext="sse"' in source
    assert "sse-connect" in source


# ── SSE module: no worker imports, no future annotations ───────────────


def test_sse_no_worker_import() -> None:
    """Nsse.py does not import from taskq.worker.*."""
    source = inspect.getsource(_sse_mod)
    assert "taskq.worker" not in source


def test_sse_no_future_annotations() -> None:
    """sse.py has no from __future__ import annotations."""
    source = inspect.getsource(_sse_mod)
    assert "from __future__ import annotations" not in source


# ── SSE generator: keepalive-only path (no PG pool/schema) ──────────────


async def test_sse_generator_keepalive_when_no_pg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no pool/schema the generator emits keepalive comments after the sentinel."""
    monkeypatch.setattr(_sse_mod, "_KEEPALIVE_INTERVAL", 0.05)
    sem = asyncio.Semaphore(1)
    await sem.acquire()  # simulate the endpoint's pre-acquire
    gen = _sse_generator(sem, None, None, "queues")
    first = await gen.__anext__()
    assert "awaiting_progress_backend" in first
    second = await gen.__anext__()
    assert second == ": keepalive\n\n"
    await gen.aclose()
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: verify permit released on close.


# ── SSE generator: PG LISTEN path (state_change + keepalive) ────────────


async def test_sse_generator_pg_path_emits_state_change_and_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With pool+schema the generator forwards payloads as state_change events."""

    async def _fake_listen(pool: object, channel: str, **kw: object) -> AsyncIterator[str | None]:
        yield '{"job_id":"123","status":"running"}'
        yield None  # keepalive signal

    monkeypatch.setattr(_sse_mod, "listen_with_reconnect", _fake_listen)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    gen = _sse_generator(sem, object(), "taskq", "jobs")
    first = await gen.__anext__()
    assert "awaiting_progress_backend" in first
    second = await gen.__anext__()
    assert second.startswith("event: state_change")
    assert "running" in second
    assert "\n\n" in second
    third = await gen.__anext__()
    assert third == ": keepalive\n\n"
    await gen.aclose()
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: verify permit released on close.


# ── SSE endpoint: unknown topic → 400 ───────────────────────────────────


def test_sse_endpoint_unknown_topic_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /sse/{unknown_topic} returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/sse/bogus_topic")  # pyright: ignore[reportUnknownMemberType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownMemberType]
    assert "unknown SSE topic" in response.text  # pyright: ignore[reportUnknownMemberType]
