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


def test_progress_router_accepts_and_defaults_the_cap() -> None:
    import inspect

    from taskq.web import progress

    sig = inspect.signature(progress.create_router)
    assert "max_sse_connections" in sig.parameters
    src = inspect.getsource(progress.create_router)
    assert "acquire_sse_slot(" in src
    assert "progress_max_sse_connections" in src


def test_progress_stream_releases_the_slot_on_every_early_exit() -> None:
    """404/503/PG-error paths run after the slot is taken; a leak there would
    exhaust the cap without a single stream ever opening."""
    import inspect

    from taskq.web import progress

    src = inspect.getsource(progress.create_router)
    assert "_release_slot()" in src
    assert "except BaseException:" in src, "early-exit exceptions must return the slot"


def test_admin_live_sse_is_capped() -> None:
    import inspect

    from taskq.web.admin import jobs as admin_jobs

    src = inspect.getsource(admin_jobs.register)
    assert 'acquire_sse_slot("admin-jobs-live"' in src
    assert "semaphore.release()" in src


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
