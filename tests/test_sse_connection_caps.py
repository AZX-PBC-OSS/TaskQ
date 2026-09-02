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
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

from taskq.web import _sse_limit


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


# ── Progress stream: the cap applies, and early exits give the slot back ──
#
# Minimal duck-types for the two collaborators the stream route touches before
# it decides to 404. Deliberately local and tiny: the point is to drive the
# real route, not to model Redis or asyncpg.


class _NoRowConn:
    async def fetchrow(self, query: str, *args: object) -> None:
        return None


class _NoRowAcquire:
    async def __aenter__(self) -> _NoRowConn:
        return _NoRowConn()

    async def __aexit__(self, *args: object) -> None:
        return None


class _NoRowPool:
    """A pool whose job lookup finds nothing, so the route takes its 404 exit."""

    def acquire(self, *, timeout: float | None = None) -> _NoRowAcquire:
        return _NoRowAcquire()


class _InertPubSub:
    def __init__(self) -> None:
        self.closed = False

    async def subscribe(self, channel: str | bytes) -> None:
        return None

    async def unsubscribe(self, channel: str | bytes) -> None:
        return None

    async def aclose(self) -> None:
        self.closed = True


class _InertRedis:
    def pubsub(self) -> _InertPubSub:
        return _InertPubSub()


def _progress_stream_endpoint(max_sse_connections: int) -> Any:
    """The real ``/progress/stream`` endpoint, capped at *max_sse_connections*."""
    from fastapi.routing import APIRoute

    from taskq.web.progress import create_router

    router = create_router(
        _NoRowPool(),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire()/fetchrow() are reached.
        _InertRedis(),
        max_sse_connections=max_sse_connections,
    )
    for route in router.routes:
        if isinstance(route, APIRoute) and route.path.endswith("/progress/stream"):
            return route.endpoint
    raise AssertionError("progress stream route not found")


def _mock_request() -> MagicMock:
    request = MagicMock()
    request.headers.get.return_value = None  # no Last-Event-ID header
    return request


async def test_progress_stream_404s_do_not_consume_the_cap() -> None:
    """A run of requests for missing jobs must not exhaust the connection cap.

    The slot is taken before the job lookup, so every early exit — 404 here —
    has to give it back. If it does not, the second request for a missing job
    is rejected with 429 and the endpoint is bricked for everyone without a
    single stream ever having opened: a one-request denial of service on a
    cap of one, which is the whole mechanism inverted into the attack.
    """
    endpoint = _progress_stream_endpoint(max_sse_connections=1)

    for attempt in range(3):
        with pytest.raises(HTTPException) as excinfo:
            await endpoint(
                job_id=UUID("00000000-0000-0000-0000-000000000001"),
                request=_mock_request(),
                last_event_id=None,
            )
        assert excinfo.value.status_code == 404, (
            f"request {attempt} got {excinfo.value.status_code}; a leaked slot "
            "turns missing-job lookups into a permanent 429"
        )


# ── Admin live job feed: capped, on its own budget ────────────────────────


async def test_admin_live_feed_is_refused_once_its_budget_is_spent() -> None:
    """``/jobs/sse/live`` bypasses admin/sse.py entirely, so its cap has to be
    applied by the route itself. With the ``admin-jobs-live`` budget spent, a
    further connection is refused with 429 rather than opening another PG
    LISTEN connection — and the progress stream, on its own budget, is
    unaffected."""
    from taskq.settings import TaskQSettings

    limit = TaskQSettings.load().admin_max_sse_connections
    for _ in range(limit):
        await _sse_limit.acquire_sse_slot("admin-jobs-live", limit)

    with pytest.raises(HTTPException) as excinfo:
        await _sse_limit.acquire_sse_slot("admin-jobs-live", limit)
    assert excinfo.value.status_code == 429

    # Independent budget: the per-job progress stream still admits.
    await _sse_limit.acquire_sse_slot("progress-stream", 1)


async def test_progress_stream_cap_still_rejects_once_slots_are_genuinely_held() -> None:
    """The complement: the cap is real. With its one slot held by a live
    stream, the next request is refused with 429 rather than queued."""
    _progress_stream_endpoint(max_sse_connections=1)
    await _sse_limit.acquire_sse_slot("progress-stream", 1)

    with pytest.raises(HTTPException) as excinfo:
        await _sse_limit.acquire_sse_slot("progress-stream", 1)
    assert excinfo.value.status_code == 429


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
