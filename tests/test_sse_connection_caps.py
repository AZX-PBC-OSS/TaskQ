"""Every SSE endpoint must cap concurrent connections.

Each SSE connection pins server resources for as long as the client keeps it
open: an asyncio task, a Postgres LISTEN connection or a Redis pubsub
subscription, and a file descriptor. `web/admin/sse.py` capped its own
`/sse/{topic}` endpoint with a per-topic semaphore and returned 429 when
exhausted. Two other streams had no cap of any kind:

* `/jobs/api/job/{job_id}/progress/stream` -- the per-job progress bridge.
* `/jobs/sse/live` -- the admin live job feed, which lives in `admin/jobs.py`
  and never went through `admin/sse.py`, so the `admin_max_sse_connections`
  semaphore never applied to it. The original report missed this one.

Uncapped, any principal who can reach these routes opens streams until the
process runs out of Redis connections, event-loop tasks or descriptors -- on
the app hosting the ingestion pipeline.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from fastapi import HTTPException

from taskq.web import _sse_limit

_JOB_ID = UUID("00000000-0000-0000-0000-000000000001")

# A terminal PG row: the stream serves the snapshot + done and ends on its
# own, so TestClient can read the whole response without hanging.
_TERMINAL_ROW: dict[str, Any] = {
    "status": "succeeded",
    "progress_seq": 5,
    "progress_state": {"step": 1},
}


class _SwitchablePool:
    """asyncpg.Pool duck-type whose fetchrow result can be flipped at runtime.

    Lets one mounted router serve both the missing-job (404) requests and the
    following valid (200) request that proves the slot came back.
    """

    def __init__(self) -> None:
        self.row: dict[str, Any] | None = None

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
        return _AcquireCtx(self)


class _AcquireCtx:
    def __init__(self, pool: _SwitchablePool) -> None:
        self._pool = pool

    async def __aenter__(self) -> _PoolConn:
        return _PoolConn(self._pool)

    async def __aexit__(self, *args: object) -> None:
        pass


class _PoolConn:
    def __init__(self, pool: _SwitchablePool) -> None:
        self._pool = pool

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        return self._pool.row


class _StubPubSub:
    """redis pubsub duck-type; set ``subscribe_error`` to drive the 503 path."""

    def __init__(self) -> None:
        self.subscribe_error: Exception | None = None
        self.unsubscribed = False
        self.closed = False

    async def subscribe(self, channel: str | bytes) -> None:
        if self.subscribe_error is not None:
            raise self.subscribe_error

    async def unsubscribe(self, channel: str | bytes) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


class _StubRedis:
    def __init__(self, pubsub: _StubPubSub) -> None:
        self._pubsub = pubsub

    def pubsub(self) -> _StubPubSub:
        return self._pubsub


def _progress_client(pool: _SwitchablePool, redis: _StubRedis) -> Any:
    """Mount the real progress router with a one-connection cap and return a
    TestClient for it (same app shape as tests/web_progress/test_unit.py)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.progress import create_router

    router = create_router(
        pool,
        redis,
        schema="taskq",
        sse_heartbeat_interval=timedelta(milliseconds=50),
        max_sse_connections=1,
    )
    app = FastAPI()
    app.include_router(router, prefix="/jobs")
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _clear_semaphores() -> None:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture, invoked by pytest not by name.
    _sse_limit._SEMAPHORES.clear()  # Why: process-global registry; tests must not leak slots into each other.


async def test_slots_are_granted_up_to_the_limit() -> None:
    held = [await _sse_limit.acquire_sse_slot("t", 3) for _ in range(3)]
    assert len(held) == 3


async def test_exhausted_limit_returns_429_rather_than_queueing() -> None:
    """A queueing client would hold the request open waiting for a slot,
    which is the exhaustion being prevented."""
    for _ in range(2):
        await _sse_limit.acquire_sse_slot("t", 2)

    with pytest.raises(HTTPException) as excinfo:
        await _sse_limit.acquire_sse_slot("t", 2)
    assert excinfo.value.status_code == 429


async def test_release_frees_a_slot_for_the_next_client() -> None:
    first = await _sse_limit.acquire_sse_slot("t", 1)
    with pytest.raises(HTTPException):
        await _sse_limit.acquire_sse_slot("t", 1)
    first.release()
    # Must now succeed.
    await _sse_limit.acquire_sse_slot("t", 1)


async def test_keys_have_independent_budgets() -> None:
    await _sse_limit.acquire_sse_slot("a", 1)
    # A different endpoint must not be starved by another's usage.
    await _sse_limit.acquire_sse_slot("b", 1)
    with pytest.raises(HTTPException):
        await _sse_limit.acquire_sse_slot("a", 1)


async def test_release_after_frees_the_slot_when_the_stream_ends() -> None:
    semaphore = await _sse_limit.acquire_sse_slot("t", 1)

    async def _gen():  # type: ignore[no-untyped-def]  # Why: trivial test generator.
        yield "a"
        yield "b"

    chunks = [c async for c in _sse_limit.release_after(semaphore, _gen())]
    assert chunks == ["a", "b"]
    # Slot returned, so the next client is admitted.
    await _sse_limit.acquire_sse_slot("t", 1)


async def test_release_after_frees_the_slot_on_client_disconnect() -> None:
    """Disconnect arrives as CancelledError thrown into the generator, so the
    release must be in a finally around the iteration, not after it."""
    semaphore = await _sse_limit.acquire_sse_slot("t", 1)

    async def _gen():  # type: ignore[no-untyped-def]  # Why: trivial test generator.
        yield "a"
        await asyncio.sleep(3600)
        yield "never"

    wrapped = _sse_limit.release_after(semaphore, _gen())
    assert await wrapped.__anext__() == "a"
    await wrapped.aclose()

    await _sse_limit.acquire_sse_slot("t", 1)


def test_no_uncapped_sse_endpoint_remains() -> None:
    """Inventory guard over every streaming response in the web package."""
    from pathlib import Path

    web = Path(__file__).resolve().parent.parent / "src" / "taskq" / "web"
    offenders: list[str] = []
    for path in web.rglob("*.py"):
        text = path.read_text()
        streams = "EventSourceResponse(" in text or "StreamingResponse(" in text
        if not streams or path.name == "_sse_limit.py":
            continue
        capped = "acquire_sse_slot" in text or "_get_semaphore" in text
        if not capped:
            offenders.append(path.relative_to(web).as_posix())
    assert offenders == [], f"uncapped SSE endpoints: {offenders}"


# ── Route-level: slot release on every early exit, 429 at the cap ──────────
#
# PR #74's headline failure mode: "a run of requests for missing jobs would
# exhaust the cap without a single stream ever opening." The caps are taken
# in the route handler but live in the streaming generator, so every early
# exit between acquire and generator hand-off has to give the slot back.
# These tests drive the mounted routers through TestClient (stub PG pool and
# Redis pubsub duck-types, the same seams as tests/web_progress/test_unit.py)
# so the release is observed as admission behavior, not source text.


def test_missing_job_404s_do_not_exhaust_the_cap() -> None:
    """With max_sse_connections=1, two missing-job requests must each give
    the slot back (the second would be 429 if the first leaked it) and a
    following valid stream must still be admitted."""
    pool = _SwitchablePool()  # row is None → job not found
    client = _progress_client(pool, _StubRedis(_StubPubSub()))

    first = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert first.status_code == 404, first.text

    second = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert second.status_code == 404, (
        f"a 404 must release its slot: the second missing-job request got "
        f"{second.status_code} — the cap was exhausted without a single "
        f"stream ever opening"
    )

    pool.row = _TERMINAL_ROW
    admitted = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert admitted.status_code == 200, admitted.text
    assert "text/event-stream" in admitted.headers.get("content-type", "")


def test_subscribe_failure_503s_do_not_exhaust_the_cap() -> None:
    """The 503 subscribe-failure early exit runs after the slot is taken; a
    run of them must not exhaust the cap, and a healthy request afterwards
    must still be admitted."""
    pool = _SwitchablePool()
    pool.row = _TERMINAL_ROW  # PG is fine; the failure is at subscribe time
    pubsub = _StubPubSub()
    pubsub.subscribe_error = ConnectionError("broker down")
    client = _progress_client(pool, _StubRedis(pubsub))

    first = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert first.status_code == 503, first.text
    assert first.headers.get("retry-after") == "2"

    second = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert second.status_code == 503, (
        f"a 503 subscribe-failure must release its slot: the second request "
        f"got {second.status_code} — the cap was exhausted without a single "
        f"stream ever opening"
    )

    pubsub.subscribe_error = None
    admitted = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert admitted.status_code == 200, admitted.text
    assert "text/event-stream" in admitted.headers.get("content-type", "")


async def test_progress_stream_rejects_with_429_at_the_cap() -> None:
    """A request through the real route must get 429, not queue, when the
    progress-stream budget is exhausted (one permit held = one open stream)."""
    pool = _SwitchablePool()
    pool.row = _TERMINAL_ROW
    client = _progress_client(pool, _StubRedis(_StubPubSub()))

    held = await _sse_limit.acquire_sse_slot("progress-stream", 1)
    try:
        rejected = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
        assert rejected.status_code == 429, rejected.text
        assert "progress-stream" in rejected.text
    finally:
        held.release()


async def test_admin_live_sse_rejects_with_429_at_the_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """/jobs/sse/live never went through admin/sse.py, so its cap must be
    proven through its own route: with the admin-jobs-live budget exhausted,
    the request must be rejected with 429.

    Why the request runs on a bounded worker thread: a live-feed request
    that is (wrongly) admitted never completes against a stub pool — the
    LISTEN loop reconnects with backoff and streams keepalives forever. An
    uncapped endpoint, the exact regression this test guards, therefore
    manifests as "the request opened a stream instead of being rejected";
    the bound turns that into a fast, explicit failure instead of a hung
    test."""
    import threading

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import create_router, setup_admin_state

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_MAX_SSE_CONNECTIONS", "1")

    bundle = create_router(_SwitchablePool())  # pyright: ignore[reportArgumentType]  # Why: duck-typed pool; the 429 path rejects before any query runs.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    client = TestClient(app, raise_server_exceptions=False)

    held = await _sse_limit.acquire_sse_slot("admin-jobs-live", 1)
    outcome: dict[str, object] = {}

    def _request() -> None:
        response = client.get("/jobs/sse/live")
        outcome["status"] = response.status_code
        outcome["body"] = response.text

    requester = threading.Thread(target=_request, daemon=True)
    requester.start()
    try:
        requester.join(timeout=10.0)
        assert not requester.is_alive(), (
            "the /jobs/sse/live request never completed — it opened a "
            "stream instead of being rejected at the cap"
        )
        assert outcome.get("status") == 429, outcome
        assert "admin-jobs-live" in str(outcome.get("body")), outcome
    finally:
        held.release()
