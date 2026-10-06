"""Smoke tests for the example app - and.

is a pure import test (no containers needed). uses
``TestClient`` with testcontainers PG + Redis to verify the trigger app
can enqueue a job and redirect to the admin sidecar.
"""

import asyncio
import contextlib
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import ModuleType
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_base62

pytestmark = pytest.mark.examples


# ── Actor import smoke ────────────────────────────────────────


def test_actor_imports() -> None:
    """Import all 9 actors from ``examples.actors``; each is an
    ``ActorRef`` with a ``payload_type`` that is a Pydantic ``BaseModel``
    subclass."""
    from examples.actors import (
        counter,
        deferred,
        flaky,
        inmemory_rate_limited,
        reserved,
        snoozer,
        ticker,
        token_rate_limited,
        window_rate_limited,
    )

    from taskq import ActorRef

    actors = [
        counter,
        flaky,
        snoozer,
        deferred,
        window_rate_limited,
        token_rate_limited,
        inmemory_rate_limited,
        reserved,
        ticker,
    ]
    for ref in actors:
        assert isinstance(ref, ActorRef), f"{ref!r} is not an ActorRef"
        assert issubclass(ref.payload_type, BaseModel), (
            f"{ref.name}.payload_type is not a BaseModel subclass"
        )


def test_ticker_cron_auto_registered() -> None:
    """The ``ticker`` actor's cron schedule is registered at import time
    via the ``cron()`` call in ``examples.actors.ticker``. The
    schedule appears in ``get_registered_crons()`` with the correct
    expression and actor name.

    Manual verification: after ``docker compose up``, the ticker
    schedule is visible at ``http://localhost:8001/admin/schedules``
    with ``enabled=true`` and a ``next_fire_at`` within 30 seconds
    of startup.
    """
    from examples.actors.ticker import ticker

    from taskq import ActorRef
    from taskq.scheduler import get_registered_crons

    assert isinstance(ticker, ActorRef)
    assert ticker.name == "ticker"

    specs = get_registered_crons()
    ticker_specs = [s for s in specs if s.actor == "ticker"]
    assert len(ticker_specs) >= 1, "ticker cron schedule not registered"
    spec = ticker_specs[-1]
    assert spec.cron_expr == "* * * * * */30"
    assert spec.name == "ticker"
    assert spec.enabled is True


# ── Trigger app enqueue smoke ──────────────────────────────────


def _fresh_trigger_app() -> ModuleType:
    """``examples.app`` imported FRESH, under the caller's settings env.

    Why this exists (the 2026-10-06 CI red wave, ~8 legs across 4 PRs +
    main itself): ``examples/app.py`` freezes ``settings =
    TaskQSettings.load()`` at IMPORT time - the operator-true pattern (a
    real deployment configures via the environment, once, before the app
    serves). The operator surface stays exactly as documented; the TEST
    adapts. The hazard is import ORDER inside one xdist worker process:
    any earlier test that imports ``examples.app`` without
    ``TASKQ_PG_DSN`` set (tests/test_api_fix_wave.py does - it stubs
    ``app.state.tq`` and never enters the lifespan, so the frozen DSN is
    invisible to it) leaves the module cached in ``sys.modules`` with
    settings pinned to the field DEFAULT - the one hardcoded-port surface
    left in the tree, ``postgresql://…@localhost:5432/taskq``
    (``TaskQSettings.pg_dsn``). This test's later ``from examples.app
    import app`` then hits the cache, and the lifespan dials the dead
    default ``127.0.0.1:5432`` while the shared container lives on its
    mapped port (``localhost:32768``) - the ``Connect call failed
    ('127.0.0.1', 5432)`` signature every red leg showed. Dropping the
    module and re-importing re-runs the import-time ``load()`` under THIS
    test's env, so the app's pool, migrations and redis client all
    resolve the mapped-port DSN the fixtures provisioned.

    The fastapi-app smoke below needs none of this:
    ``examples/fastapi_app/main.py`` loads its settings INSIDE the
    lifespan, at startup, from the live environment.
    """
    import sys

    sys.modules.pop("examples.app", None)
    import examples.app

    return examples.app


async def _migrate(dsn: str, schema: str) -> None:
    """Drop the test schema and apply all migrations."""
    from taskq.migrate import apply_pending

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


@asynccontextmanager
async def lifespan_handler(fastapi_app: object) -> AsyncGenerator[None]:
    """Manually run the FastAPI lifespan startup/shutdown for httpx testing."""
    from fastapi import FastAPI

    assert isinstance(fastapi_app, FastAPI)
    ctx = fastapi_app.router.lifespan_context
    async with ctx(fastapi_app):
        yield


@pytest.mark.integration
@pytest.mark.redis
def test_trigger_app_enqueue(
    pg_dsn: str,
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TestClient(app) from examples.app: GET / returns 200 with
    actor names; POST /enqueue/counter answers the ONE success envelope
    (201 {"job_id","url"}, F3) whose url keys at the admin job page."""
    monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
    monkeypatch.setenv("TASKQ_REDIS_URL", redis_url)
    schema = f"tes_trigger_{new_base62()}".lower()
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_URL", "http://localhost:9999")
    monkeypatch.setenv("TASKQ_QUEUES", "examples")

    asyncio.run(_migrate(pg_dsn, schema))

    trigger_app = _fresh_trigger_app()
    app = trigger_app.app

    # The drift pin: the app's import-time settings resolved THIS test's
    # DSN - the container's mapped port - never the hardcoded
    # ``localhost:5432`` field default (see ``_fresh_trigger_app`` for the
    # incident). Both sides derive from the same fixture value, so exact
    # equality is the invariant; a future regression that lets a stale
    # module (default DSN) reach the lifespan fails HERE, naming the
    # resolved DSN, instead of as a connection refused on 5432.
    assert str(trigger_app.settings.pg_dsn) == pg_dsn, (
        f"examples.app resolved settings for {trigger_app.settings.pg_dsn}, "
        f"not this test's DSN {pg_dsn} - the import-time settings freeze "
        "regressed (see _fresh_trigger_app)"
    )

    from fastapi.testclient import TestClient

    with TestClient(app, follow_redirects=False) as client:
        response = client.get("/")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.
        assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType] # Why: response.status_code type is unknown due to upstream Any.

        actor_names = [
            "counter",
            "flaky",
            "snoozer",
            "deferred",
            "window_rate_limited",
            "token_rate_limited",
            "inmemory_rate_limited",
            "reserved",
        ]
        for name in actor_names:
            assert name in response.text, f"actor {name!r} not in GET / response"

        response = client.post(  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.post return type is Any.
            "/enqueue/counter",
            data={"n": "1"},
        )
        # F3's one envelope: a plain enqueue answers 201 {"job_id","url"}.
        assert response.status_code == 201  # pyright: ignore[reportUnknownVariableType] # Why: response.status_code type is unknown due to upstream Any.
        body = response.json()  # pyright: ignore[reportUnknownMemberType] # Why: response.json() return is Any.
        url = body.get("url", "")
        assert re.match(
            r"(/taskq/jobs/[0-9a-f-]{36}|http://localhost:\d+/admin/jobs/[0-9a-f-]{36})",
            url,
        ), f"url {url!r} does not match expected pattern"


# ── FastAPI app smoke ──────────────────────────────────────────────────


async def _asgi_request(app: Any, method: str, path: str, body: bytes = b"") -> dict[str, Any]:
    """Send a complete ASGI request and collect the full response."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "server": ("test", 80),
    }
    out: dict[str, Any] = {"status": 0, "headers": [], "body": b""}
    receive_calls = 0

    async def receive() -> dict[str, Any]:
        nonlocal receive_calls
        if receive_calls == 0:
            receive_calls += 1
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            out["status"] = message["status"]
            out["headers"] = message.get("headers", [])
        elif message["type"] == "http.response.body":
            out["body"] += message.get("body", b"")

    await app(scope, receive, send)
    return out


async def _asgi_stream_first_data(
    app: Any, method: str, path: str, body: bytes = b""
) -> dict[str, Any]:
    """Send an ASGI streaming request, capture the first SSE data line, then disconnect."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "server": ("test", 80),
    }

    started = asyncio.Event()
    got_first_data = asyncio.Event()
    allow_disconnect = asyncio.Event()

    out: dict[str, Any] = {"status": 0, "headers": [], "body": b""}
    receive_calls = 0

    async def receive() -> dict[str, Any]:
        nonlocal receive_calls
        if receive_calls == 0:
            receive_calls += 1
            return {"type": "http.request", "body": body, "more_body": False}
        await allow_disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            out["status"] = message["status"]
            out["headers"] = message.get("headers", [])
            started.set()
        elif message["type"] == "http.response.body":
            out["body"] += message.get("body", b"")
            if b"data: " in out["body"] and not got_first_data.is_set():
                got_first_data.set()
                allow_disconnect.set()

    task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(started.wait(), timeout=5)
    await asyncio.wait_for(got_first_data.wait(), timeout=10)

    try:
        await asyncio.wait_for(task, timeout=5)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    return out


@pytest.mark.integration
@pytest.mark.redis
async def test_fastapi_app_enqueue_and_stream(
    pg_dsn: str,
    private_redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /jobs enqueues a job; GET /jobs/{job_id}/stream yields at least
    one ``data:`` line containing valid ``JobEvent`` JSON.

    PRIVATE broker (``private_redis_url``): the pin bounds the SSE stream's
    FIRST-DATA latency (``got_first_data.wait()`` window) - pub/sub fanout
    timing, the surface a shared-broker co-tenancy stall band reds without
    any code defect. See ``taskq.testing.fixtures.redis_url``'s tenancy
    contract. The sibling enqueue smoke (``test_trigger_app_enqueue``)
    stays on the shared pair: it only enqueues and reads a redirect, no
    fanout, no latency assertion.
    """
    monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
    monkeypatch.setenv("TASKQ_REDIS_URL", private_redis_url)
    schema = f"tes_fastapi_{new_base62()}".lower()
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    await _migrate(pg_dsn, schema)

    from examples.fastapi_app.main import app

    async with lifespan_handler(app):
        post_resp = await _asgi_request(app, "POST", "/jobs")
        assert post_resp["status"] == 200

        import orjson

        body = orjson.loads(post_resp["body"])
        assert "job_id" in body, f"response body missing job_id: {body!r}"
        job_id = body["job_id"]

        stream_resp = await _asgi_stream_first_data(app, "GET", f"/jobs/{job_id}/stream")
        assert stream_resp["status"] == 200

        text = stream_resp["body"].decode("utf-8", errors="replace")
        data_lines: list[str] = []
        for line in text.splitlines():
            if line.startswith("data: "):
                data_lines.append(line[len("data: ") :])

        assert len(data_lines) >= 1, "SSE stream had no data lines"

        from taskq import JobEvent

        first_event = JobEvent.model_validate_json(data_lines[0])
        assert str(first_event.job_id) == job_id
