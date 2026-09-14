"""Red-team attacks on the SSE progress bridge's broker-facing waits.

Hunt scope: ``src/taskq/web/progress.py`` — the streaming loop's Redis
dependency, connection lifetime, and the SSE slot cap.

Papered defects and pins
------------------------
1. REAL DEFECT — the loop's only broker wait owns no deadline
   (progress.py:184-187)::

       raw_msg = await pubsub.get_message(
           ignore_subscribe_messages=True,
           timeout=heartbeat_secs,
       )

   The bound is entirely delegated to the ``pubsub`` object — typed ``Any``
   (an erasure boundary, not a guarantee). The project's own rule is that
   TaskQ-initiated waits on a possibly-dead broker are bounded *by this
   codebase*: ``_factory.get_realtime_mode`` wraps even a ``ping()`` in
   ``asyncio.wait_for(..., timeout=0.5)`` (admin/_factory.py:197), and every
   close goes through ``close_redis_bounded``. With redis-py 8.1.0 the
   delegated ``timeout=`` bounds only the *read*; ``PubSub.parse_response``
   re-enters via ``await conn.connect()`` when the connection dropped
   (redis/asyncio/client.py:1426) — a reconnect with no app-level bound
   (``socket_connect_timeout`` defaults to None) — so a wedged/tarpitted
   broker can stall the stream far past the heartbeat cadence while the
   Redis subscription, the asyncio task and the SSE slot stay pinned.
   ``test_wedged_broker_read_must_be_app_bounded`` pins the desired
   observable: the loop must own an app-level deadline around the broker
   wait. RED today.

2. GREEN PIN — broker death mid-stream is fail-visible and fully cleaned
   up: the generator terminates (exception propagates — no silent eternal
   keepalive loop), the pubsub is unsubscribed/closed, and the SSE slot is
   released (progress.py:263-278 finally).

3. GREEN PIN — the per-process SSE connection cap: exhausted cap answers
   HTTP 429 before allocating anything, and a disconnected stream's slot is
   returned to the budget (progress.py:417-442 + _sse_limit.py).

4. GREEN PIN (safe-unpinned) — a non-integer ``Last-Event-ID`` header is
   treated as an initial connection (progress.py:128-133), never a crash
   and never a silently-blackholed cursor.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")

from uuid import UUID

from fastapi import HTTPException
from fastapi.routing import APIRoute
from sse_starlette.event import ServerSentEvent

from taskq._ids import new_uuid
from taskq.constants import progress_channel
from taskq.web.progress import (
    _event_generator,  # pyright: ignore[reportPrivateUsage]  # Why: red-team tests exercise the production generator directly rather than reimplementing it.
    _resolve_last_event_id,  # pyright: ignore[reportPrivateUsage]  # Why: pins the malformed Last-Event-ID branch, which no existing test covers.
    create_router,
)


@pytest.fixture(autouse=True)
def _dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed implicitly by the runner; pyright does not track fixture usage.
    """Dev environment so create_router's fail-closed auth check does not
    raise (the gate itself is pinned by tests/web_progress/test_auth_gate.py)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")


# ── Stub doubles ──────────────────────────────────────────────────────────


class _SnapshotConn:
    """Fake asyncpg connection serving one progress-snapshot row."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        return self._row


class _AcquireCtx:
    def __init__(self, conn: _SnapshotConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _SnapshotConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._conn = _SnapshotConn(row)

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


def _pg_row(*, status: str = "running", seq: int = 0) -> dict[str, Any]:
    return {"status": status, "progress_seq": seq, "progress_state": {"step": 0}}


class _WedgedPubSub:
    """Pubsub double whose ``get_message`` never returns (wedged broker read).

    ``unsubscribe``/``aclose`` stay fast so the RED state isolates the
    unbounded *read*, not the (already bounded) teardown.
    """

    def __init__(self) -> None:
        self.unsubscribed = False
        self.closed = False
        self._gate: asyncio.Event = asyncio.Event()  # never set

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message's signature; the timeout is delegated, which is exactly what this attack proves insufficient.
    ) -> dict[str, Any] | None:
        await self._gate.wait()
        raise AssertionError("unreachable")  # pragma: no cover - the gate is never set

    async def unsubscribe(self, channel: str | bytes) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


class _DyingPubSub:
    """Pubsub double that yields one well-formed event, then raises
    ConnectionError — a broker that dies mid-stream after a good event."""

    def __init__(self, message: dict[str, Any]) -> None:
        self._message = message
        self._served = False
        self.unsubscribed = False
        self.closed = False

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message's signature.
    ) -> dict[str, Any] | None:
        if not self._served:
            self._served = True
            return self._message
        raise ConnectionError("broker connection reset mid-stream")

    async def unsubscribe(self, channel: str | bytes) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


class _PassivePubSub:
    """Pubsub double for slot-cap tests: generators are never started, so
    only subscribe/unsubscribe/aclose are exercised."""

    def __init__(self) -> None:
        self.unsubscribed = False
        self.closed = False

    async def subscribe(self, channel: str | bytes) -> None:
        pass

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message's signature.
    ) -> dict[str, Any] | None:
        return None

    async def unsubscribe(self, channel: str | bytes) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


class _StubRedis:
    def __init__(self, pubsub: object) -> None:
        self._pubsub = pubsub

    def pubsub(self) -> object:
        return self._pubsub


def _live_event(job_id: UUID, *, seq: int) -> dict[str, Any]:
    """A well-formed ProgressEvent-shaped Redis message for *job_id*."""
    envelope = {
        "v": 1,
        "kind": "progress",
        "job_id": str(job_id),
        "actor": "rt_web_actor",
        "ts": "2026-01-01T00:00:00Z",
        "seq": seq,
        "status": "running",
        "step": 1,
    }
    return {"type": "message", "data": json.dumps(envelope).encode()}


def _mock_request(header_value: str | None = None) -> MagicMock:
    request = MagicMock()
    request.headers.get.return_value = header_value
    return request


async def _drain(gen: AsyncGenerator[ServerSentEvent, None]) -> list[ServerSentEvent]:
    out: list[ServerSentEvent] = []
    async for event in gen:
        out.append(event)
        if len(out) >= 50:
            break
    return out


async def _first_event(gen: AsyncGenerator[ServerSentEvent, None]) -> ServerSentEvent:
    """Consume exactly one event, then close — never hangs on a live loop."""
    async with asyncio.timeout(2):
        event = await gen.__anext__()
    await gen.aclose()
    return event


# ── 1. Wedged broker read must be app-bounded ─────────────────────────────


async def test_wedged_broker_read_must_be_app_bounded() -> None:
    """A pubsub read that never returns must not pin the stream forever.

    CONTRACT: the SSE streaming loop must own an app-level deadline around
    its broker wait — every TaskQ-initiated wait on a possibly-dead broker
    is bounded by this codebase's own rule (compare admin/_factory.py:197
    wrapping even ``redis_client.ping()`` in ``asyncio.wait_for(..., 0.5)``,
    and every close via ``close_redis_bounded``). A read that wedges —
    dead/tarpitted broker, a reconnect inside redis-py's
    ``PubSub.parse_response`` with no ``socket_connect_timeout``, or any
    pubsub double that outlives the delegated ``timeout=`` parameter — must
    terminate the stream within a bounded window.

    CURRENT VIOLATION: progress.py:184-187 delegates the ONLY bound to
    ``pubsub.get_message(..., timeout=heartbeat_secs)`` — nothing in this
    module cancels a read that never returns — so 15 heartbeat intervals
    later the generator task is still pending, holding the Redis
    subscription, the asyncio task and the SSE slot.
    """
    pubsub = _WedgedPubSub()
    job_id = new_uuid()
    slot = asyncio.Semaphore(1)
    await slot.acquire()  # mirrors acquire_sse_slot before the generator owns it

    gen = _event_generator(
        pubsub=pubsub,
        channel=progress_channel("taskq", job_id),
        job_id=job_id,
        is_terminal=False,
        progress_seq=0,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=0.05,
        sse_slot_semaphore=slot,
    )
    task = asyncio.create_task(_drain(gen))
    try:
        # 0.75s = 15 heartbeats at 50ms — ample slack for any bounded loop.
        await asyncio.sleep(0.75)
        assert task.done() and not task.cancelled(), (
            "CONTRACT: the SSE streaming loop must own an app-level deadline around "
            "its broker wait (project rule: every TaskQ-initiated wait on a possibly-"
            "dead broker is bounded by this codebase — see admin/_factory.py:197 "
            "ping and close_redis_bounded). CURRENT VIOLATION: progress.py:184-187 "
            "delegates the only bound to pubsub.get_message(timeout=heartbeat_secs); "
            "a read that never returns is never cancelled, so the generator is still "
            "pending after 15 heartbeat intervals, pinning the Redis subscription, "
            "the asyncio task and the SSE slot."
        )
    finally:
        # Bounded teardown so the RED state can never hang the test run.
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
    # Cancellation must still run the generator's finally: slot released.
    assert slot.locked() is False, (
        "CONTRACT: once the stream ends for any reason (including cancellation from "
        "a bounded supervisor), the SSE slot must be released in the generator's "
        "finally (progress.py:263-269). CURRENT: released on cancel — pinning this "
        "so a fix for the wedged-read bound cannot regress the cleanup path."
    )


# ── 2. Broker death mid-stream: fail-visible + full cleanup ───────────────


async def test_midstream_broker_death_terminates_and_releases_everything() -> None:
    """A broker that dies mid-stream must terminate the stream visibly and
    clean up every resource — not hang, and not silently loop keepalives.

    CONTRACT: ConnectionError out of ``get_message`` propagates out of the
    generator (fail-visible — the browser EventSource reconnects on stream
    end), the pubsub is unsubscribed and closed, and the SSE slot is
    released (progress.py:263-278). This pins the current correct behavior
    so the wedged-read fix cannot trade it for a silent swallow.
    """
    job_id = new_uuid()
    pubsub = _DyingPubSub(_live_event(job_id, seq=3))
    slot = asyncio.Semaphore(1)
    await slot.acquire()

    gen = _event_generator(
        pubsub=pubsub,
        channel=progress_channel("taskq", job_id),
        job_id=job_id,
        is_terminal=False,
        progress_seq=0,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=0.05,
        sse_slot_semaphore=slot,
    )
    events: list[ServerSentEvent] = []
    with pytest.raises(ConnectionError, match="broker connection reset"):
        async with asyncio.timeout(5):
            async for event in gen:
                events.append(event)

    assert [e.event for e in events] == ["progress", "progress"], (
        "CONTRACT: the PG snapshot (seq 0) and the already-received live event "
        "(seq 3) must both be delivered before the broker error surfaces."
    )
    assert pubsub.unsubscribed is True, "pubsub.unsubscribe must run in the finally"
    assert pubsub.closed is True, "pubsub.aclose must run in the finally"
    assert slot.locked() is False, (
        "CONTRACT: the SSE slot must be released when the stream dies with the "
        "broker (progress.py:268-269) — otherwise every broker outage leaks one "
        "slot per open stream until the process-wide cap refuses all clients."
    )


# ── 3. SSE connection cap: 429 before allocation, slot reuse after close ──


async def test_progress_stream_cap_429_and_slot_reuse_after_disconnect() -> None:
    """The per-process SSE cap must answer 429 once exhausted, and a closed
    stream's slot must return to the budget.

    CONTRACT: with ``max_sse_connections=2``, the third concurrent stream
    is refused with HTTP 429 *before any pubsub subscription is created*
    (progress.py:417, _sse_limit.py:43-58), and closing two in-flight
    streams (client disconnect) frees both slots so a new stream connects.
    """
    pubsub = _PassivePubSub()
    router = create_router(
        _StubPool(_pg_row()),  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub pool satisfies the asyncpg.Pool surface the route reads.
        _StubRedis(pubsub),  # pyright: ignore[reportArgumentType]  # Why: duck-typed pubsub satisfies the erased Any redis boundary.
        schema="taskq",
        sse_heartbeat_interval=timedelta(seconds=15),
        max_sse_connections=2,
    )
    endpoint = next(
        route.endpoint
        for route in router.routes
        if isinstance(route, APIRoute) and route.path.endswith("/progress/stream")
    )
    request = _mock_request(header_value=None)
    job_id = new_uuid()

    async with asyncio.timeout(10):
        first = await endpoint(job_id=job_id, request=request, last_event_id=None)
        second = await endpoint(job_id=job_id, request=request, last_event_id=None)
        assert first.status_code == 200 and second.status_code == 200

        with pytest.raises(HTTPException) as exc_info:
            await endpoint(job_id=job_id, request=request, last_event_id=None)
        assert exc_info.value.status_code == 429, (
            "CONTRACT: the SSE cap must refuse the (cap+1)-th concurrent stream with "
            "429 before allocating a pubsub subscription or a task "
            "(progress.py:417 + _sse_limit.py) — an uncapped endpoint lets any "
            "principal who can reach the route exhaust Redis connections and file "
            "descriptors on the app hosting the pipeline."
        )

        # Client disconnect on both in-flight streams. Starting each generator
        # (one __anext__: the PG snapshot event) mirrors the real ASGI flow — a
        # disconnect always arrives mid-iteration, never before the first — and
        # makes aclose run the generator's finally, which releases each slot.
        for response in (first, second):
            started = await asyncio.wait_for(response.body_iterator.__anext__(), 5)  # pyright: ignore[reportUnknownMemberType, reportAny]  # Why: sse-starlette is untyped; body_iterator is the async generator passed in.
            assert started.event == "progress"
            close = getattr(response.body_iterator, "aclose", None)  # pyright: ignore[reportUnknownMemberType]  # Why: same as above.
            assert close is not None
            await close()  # pyright: ignore[reportUnknownArgumentType, reportAny]

        third = await endpoint(job_id=job_id, request=request, last_event_id=None)
        assert third.status_code == 200, (
            "CONTRACT: slots released by disconnected streams must return to the "
            "budget — a leaked slot turns the cap into a denial-of-service against "
            "the admin UI itself (first 2 disconnects permanently brick the "
            "endpoint)."
        )
        third_started = await asyncio.wait_for(third.body_iterator.__anext__(), 5)  # pyright: ignore[reportUnknownMemberType, reportAny]  # Why: same as above.
        assert third_started.event == "progress"
        third_close = getattr(third.body_iterator, "aclose", None)  # pyright: ignore[reportUnknownMemberType]  # Why: same as above.
        assert third_close is not None
        await third_close()  # pyright: ignore[reportUnknownArgumentType, reportAny]


# ── 4. Malformed Last-Event-ID header ─────────────────────────────────────


async def test_last_event_id_garbage_header_is_initial_connection() -> None:
    """A non-integer ``Last-Event-ID`` header must degrade to an initial
    connection (None), never crash and never blackhole the cursor.

    CONTRACT (safe-unpinned): progress.py:128-133 — an unparseable header
    value returns None so the stream serves the full PG snapshot; only a
    parseable integer acts as a resume cursor. No existing test covers the
    ValueError branch.
    """
    request = _mock_request(header_value="not-an-int")
    resolved = _resolve_last_event_id(request, 7)
    assert resolved is None, (
        "CONTRACT: a malformed Last-Event-ID header (progress.py:131-133) must "
        "return None — the reconnect cursor degrades to an initial connection "
        "with the full snapshot, rather than crashing the int() parse or being "
        "mistaken for a real cursor."
    )
    # And via the loop entry point: None cursor => snapshot event is emitted.
    job_id = new_uuid()
    pubsub = _PassivePubSub()
    gen = _event_generator(
        pubsub=pubsub,
        channel=progress_channel("taskq", job_id),
        job_id=job_id,
        is_terminal=False,
        progress_seq=4,
        progress_data='{"step": 4}',
        resolved_last_event_id=_resolve_last_event_id(_mock_request("garbage"), None),
        heartbeat_secs=0.01,
    )
    snapshot = await asyncio.wait_for(_first_event(gen), timeout=2)
    assert snapshot.event == "progress" and snapshot.id == "4", (
        "CONTRACT: with the malformed header resolved to None, the initial-"
        "connection path must emit the PG snapshot (progress.py:161-164)."
    )
    await gen.aclose()
    assert pubsub.unsubscribed is True and pubsub.closed is True
