"""Post-revocation auth re-check for the SSE progress stream (#316).

The stream authenticates once at request acceptance (router-level
``Depends(auth_dependency)``) and never again: a stream opened BEFORE a
session is invalidated kept delivering frames after revocation, while new
requests correctly got 401.  The contract after the fix: the stream
re-invokes the session verifier the auth dependency exposes (same
``session_verifier`` attribute ``create_auth_dependency`` attaches) once per
loop iteration -- before every yielded event and at every keepalive tick --
and ends the stream on failure, releasing the SSE slot and the Redis
subscription in the generator's ``finally``.

The revocation model here is a flag the dependency and its verifier both
read, which is exactly how a session invalidation reaches a stateless
signed-cookie session: secret rotation, expiry, or an allowlist change make
the same cookie stop verifying.  The verifier is a per-request closure the
dependency carries, mirroring the real wiring one-for-one.
"""

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")

import structlog.testing
from fastapi import FastAPI, HTTPException, Request

import taskq.web._sse_limit as sse_limit
from taskq._ids import new_uuid
from tests.web_progress.test_integration import (
    _reap_stream_teardown_tasks,  # pyright: ignore[reportPrivateUsage]  # Why: the wave-1 stream-teardown reap, shared across the SSE test modules the same way tests/test_saml_shared_replay_store.py imports the sso fixture helpers.
)
from taskq.web.admin.auth._session import IdentityClaims
from taskq.web.progress import (
    _event_generator,  # pyright: ignore[reportPrivateUsage]  # Why: unit tests exercise the production generator directly.
    create_router,
)

pytestmark = [pytest.mark.fastapi]

_HEARTBEAT = timedelta(milliseconds=20)
_MAX_SSE = 2


# ── doubles ───────────────────────────────────────────────────────────────


class _KeepalivePubSub:
    """Quiet-channel pubsub double: every read times out, so the stream
    emits only keepalives -- the exact shape of the triaged repro (a stream
    with nothing to deliver must still die when its session does)."""

    def __init__(self) -> None:
        self.unsubscribed = False
        self.closed = False

    async def subscribe(self, channel: str) -> None:
        pass

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message signature.
    ) -> dict[str, Any] | None:
        # Why the real sleep: redis-py's get_message(timeout=t) suspends the
        # caller for up to t; a double that returns instantly would make the
        # stream's wait_for/yield cycle complete without ever reaching a real
        # suspension point, spinning the event loop instead of streaming.
        await asyncio.sleep(timeout)
        return None

    async def unsubscribe(self, channel: str) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


class _StubRedis:
    def __init__(self, pubsub: _KeepalivePubSub) -> None:
        self._pubsub = pubsub

    def pubsub(self) -> _KeepalivePubSub:
        return self._pubsub


class _StubConn:
    async def fetchrow(self, query: str, *args: object) -> dict[str, Any]:
        return {"status": "running", "progress_seq": 5, "progress_state": {"step": 5}}


class _StubAcquire:
    async def __aenter__(self) -> _StubConn:
        return _StubConn()

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def acquire(self, *, timeout: float | None = None) -> _StubAcquire:
        return _StubAcquire()


# ── app factory: the SSO wiring in miniature ─────────────────────────────


def _make_app(pubsub: _KeepalivePubSub) -> tuple[FastAPI, dict[str, bool]]:
    """Build the progress router behind the real SSO wiring shape.

    ``_dependency`` is the per-request auth check; ``session_verifier`` is
    the re-check the same dependency exposes for long-lived streams (the
    attribute ``create_auth_dependency`` attaches).  Both read one shared
    revocation flag, the way both read the same session cookie/state.
    """
    auth_state = {"session_valid": True}
    claims = IdentityClaims(subject="ops", email=None, groups=frozenset(), raw={})

    async def _dependency(request: Request) -> IdentityClaims:  # pyright: ignore[reportUnusedFunction]  # Why: registered via Depends at router level.
        if not auth_state["session_valid"]:
            raise HTTPException(status_code=401, detail="session revoked")
        return claims

    async def _verifier(request: Request) -> bool:
        _ = request
        return auth_state["session_valid"]

    _dependency.session_verifier = _verifier  # pyright: ignore[reportFunctionMemberAccess]  # Why: the re-check the SSO dependency exposes; create_auth_dependency attaches the same attribute.

    router = create_router(
        _StubPool(),  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub pool satisfies the asyncpg surface the route reads.
        _StubRedis(pubsub),  # pyright: ignore[reportArgumentType]
        schema="taskq",
        auth_dependency=_dependency,
        sse_heartbeat_interval=_HEARTBEAT,
        max_sse_connections=_MAX_SSE,
    )
    app = FastAPI()
    app.include_router(router, prefix="/jobs")
    return app, auth_state


# ── in-process ASGI driver ────────────────────────────────────────────────


def _asgi_scope(job_id: UUID) -> dict[str, Any]:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": f"/jobs/api/job/{job_id}/progress/stream",
        "raw_path": f"/jobs/api/job/{job_id}/progress/stream".encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"host", b"test")],
        "client": ("testclient", 123),
        "server": ("testserver", 80),
    }


async def _wait_for_snapshot(chunks: list[bytes], *, timeout: float = 5.0) -> None:  # noqa: ASYNC109  # Why: mirrors the redis-py read the loop under test delegates to.
    """Poll the streamed chunks until the PG snapshot frame has arrived."""
    deadline = time.monotonic() + timeout
    # Why not an Event: the waiter can start after the frame already landed,
    # so it polls the buffer rather than racing a one-shot set.
    while time.monotonic() < deadline:
        if b"event: progress" in b"".join(chunks):
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"snapshot frame never arrived; got {chunks!r}")


# ── the triaged scenario, driven in-process over ASGI ─────────────────────


async def test_stream_opened_before_revocation_ends_after_recheck() -> None:
    """A stream opened BEFORE session invalidation must not outlive it.

    CONTRACT: the streaming loop re-checks the session before every yielded
    event and at every keepalive tick; a revoked session ends the stream at
    the next tick (bounded staleness: at most one keepalive interval), the
    generator's finally releases the SSE slot and unsubscribes/closes the
    Redis subscription, and a NEW request after revocation still gets 401.

    REPRO (#316, pre-fix): auth ran once at request acceptance, so the old
    stream kept emitting keepalives indefinitely -- three-plus keepalives in
    the triage -- while new requests were correctly refused.
    """
    pubsub = _KeepalivePubSub()
    app, auth_state = _make_app(pubsub)
    job_id = new_uuid()

    received: list[bytes] = []

    # The SSE interaction mints loop tasks the test cannot hold references
    # to (sse-starlette's _shutdown_watcher parks until a shutdown flag no
    # in-process server ever sets); the reap belongs in the call, before
    # the module-loop guard's snapshot.
    baseline: frozenset[asyncio.Task[object]] = frozenset(asyncio.all_tasks())

    # Why this blocks: a real ASGI server's receive() parks until the client
    # sends something or disconnects. A receive() that returns instantly makes
    # sse-starlette's _listen_for_disconnect loop spin the event loop without
    # ever reaching a suspension point. The disconnect surfaces as task
    # cancellation in the teardown, as it does under a real server.
    _disco = asyncio.Event()

    async def _receive() -> dict[str, Any]:
        await _disco.wait()
        return {"type": "http.disconnect", "body": b"", "more_body": False}

    async def _send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body":
            received.append(message.get("body", b""))

    task = asyncio.create_task(app(_asgi_scope(job_id), _receive, _send))
    try:
        await _wait_for_snapshot(received)

        # The session is invalidated while the stream is live.
        auth_state["session_valid"] = False

        # A NEW request after revocation is refused, as before the fix.
        state = await _get_state(app, job_id)
        assert state == 401

        # One grace window: the tick already in flight when the revocation
        # landed is allowed to complete (its check ran before the flip).
        await asyncio.sleep(_HEARTBEAT.total_seconds() * 3)

        tail_len = len(received)
        deadline = time.monotonic() + 2.0
        while not task.done() and time.monotonic() < deadline:  # noqa: ASYNC110  # Why: bounded task-done poll - completion is observable on the task, not a signal this task can await.
            await asyncio.sleep(0.005)

        assert task.done(), (
            "CONTRACT: a live SSE stream whose session is revoked must end at "
            "the next re-check tick (at most one keepalive interval later). "
            "PRE-FIX (#316): the stream authenticated once at subscribe and "
            f"kept delivering frames after revocation; tail after the grace "
            f"window: {b''.join(received[tail_len:])!r}"
        )

        body_after_flip = b"".join(received[tail_len:])
        # At most the single in-flight tick's keepalive may land after the
        # flip; the revoked stream never emits another frame.
        keepalives_after_flip = body_after_flip.count(b": keepalive")
        assert keepalives_after_flip <= 1, (
            f"CONTRACT: at most the in-flight tick may follow the revocation, "
            f"got {keepalives_after_flip} keepalive(s): {body_after_flip!r}"
        )

        # The finally must have run: slot released, subscription released.
        await asyncio.sleep(0.05)
        assert pubsub.unsubscribed, "the revoked stream must unsubscribe its Redis subscription"
        assert pubsub.closed, "the revoked stream must close its Redis subscription"
        slot = sse_limit._SEMAPHORES[("progress-stream", _MAX_SSE)]  # pyright: ignore[reportPrivateUsage]  # Why: no public accessor for the process-wide cap; the permit state is the assertion.
        assert slot.locked() is False, (
            "CONTRACT: the ended stream's SSE slot must be released, or every "
            "revocation leaks one slot from the connection cap"
        )
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
        await _reap_stream_teardown_tasks(baseline)


async def _get_state(app: FastAPI, job_id: UUID) -> int:
    status_holder: list[int] = []

    scope = dict(_asgi_scope(job_id))
    scope["path"] = f"/jobs/api/job/{job_id}/state"
    scope["raw_path"] = scope["path"].encode()
    scope["method"] = "GET"

    async def _receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            status_holder.append(message["status"])

    await app(scope, _receive, _send)
    return status_holder[0]


# ── generator-level pins ──────────────────────────────────────────────────


async def _drain_limited(gen: AsyncIterator[Any], *, limit: int) -> list[Any]:
    out: list[Any] = []
    async for event in gen:
        out.append(event)
        if len(out) >= limit:
            break
    return out


async def test_generator_recheck_false_at_open_yields_nothing() -> None:
    """A session already invalid when the generator starts yields no frame
    at all -- not even the PG snapshot."""
    pubsub = _KeepalivePubSub()
    slot = asyncio.Semaphore(1)
    await slot.acquire()

    async def _revoked() -> bool:
        return False

    with structlog.testing.capture_logs() as logs:
        gen = _event_generator(
            pubsub=pubsub,  # pyright: ignore[reportArgumentType]  # Why: duck-typed pubsub double.
            channel="chan",
            job_id=new_uuid(),
            is_terminal=False,
            progress_seq=5,
            progress_data="{}",
            resolved_last_event_id=None,
            heartbeat_secs=0.02,
            sse_slot_semaphore=slot,
            session_verifier=_revoked,
        )
        events = await _drain_limited(gen, limit=1)

    assert events == [], f"no frame may be emitted on a revoked session, got {events!r}"
    assert pubsub.unsubscribed and pubsub.closed
    assert slot.locked() is False, "the slot must be released when the generator exits"
    assert any(log["event"] == "sse-session-revoked" for log in logs)


async def test_generator_recheck_flip_ends_stream_at_next_tick() -> None:
    """The verifier passes for the snapshot and the first tick, then fails:
    the stream ends and never yields another frame."""
    pubsub = _KeepalivePubSub()
    slot = asyncio.Semaphore(1)
    await slot.acquire()

    calls = {"n": 0}

    async def _flip_after_two() -> bool:
        calls["n"] += 1
        return calls["n"] < 3

    gen = _event_generator(
        pubsub=pubsub,  # pyright: ignore[reportArgumentType]
        channel="chan",
        job_id=new_uuid(),
        is_terminal=False,
        progress_seq=5,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=0.02,
        sse_slot_semaphore=slot,
        session_verifier=_flip_after_two,
    )
    events: list[Any] = []
    async with asyncio.timeout(2.0):
        async for event in gen:
            events.append(event)

    assert events, "the frames checked before the flip must still be delivered"
    assert all(getattr(e, "comment", None) == "keepalive" for e in events[1:]) or len(events) <= 3
    assert pubsub.unsubscribed and pubsub.closed
    assert slot.locked() is False


async def test_generator_recheck_error_fails_closed() -> None:
    """A verifier that raises is treated as revoked: the stream ends rather
    than continuing on an unknown session state."""
    pubsub = _KeepalivePubSub()
    slot = asyncio.Semaphore(1)
    await slot.acquire()

    async def _broken() -> bool:
        raise RuntimeError("session store unavailable")

    gen = _event_generator(
        pubsub=pubsub,  # pyright: ignore[reportArgumentType]
        channel="chan",
        job_id=new_uuid(),
        is_terminal=False,
        progress_seq=5,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=0.02,
        sse_slot_semaphore=slot,
        session_verifier=_broken,
    )
    events = await _drain_limited(gen, limit=1)

    assert events == []
    assert pubsub.unsubscribed and pubsub.closed
    assert slot.locked() is False


async def test_generator_recheck_pass_delivers_events() -> None:
    """The re-check must not over-block: a session that stays valid keeps
    the stream fully working (snapshot + terminal + done)."""
    from datetime import UTC, datetime

    from taskq.progress._events import ProgressEvent

    job_id = new_uuid()
    terminal = ProgressEvent(
        v=1,
        kind="state_change",
        job_id=job_id,
        actor="a",
        ts=datetime.now(UTC),
        seq=6,
        status="succeeded",
        terminal=True,
    )

    class _EventPubSub(_KeepalivePubSub):
        async def get_message(
            self,
            *,
            ignore_subscribe_messages: bool = True,
            timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message signature.
        ) -> dict[str, Any] | None:
            if not self.closed:
                self.closed = True  # one live message, then quiet
                return {
                    "type": "message",
                    "data": terminal.model_dump_json(exclude_none=True).encode(),
                }
            await asyncio.sleep(timeout)
            return None

    pubsub2 = _EventPubSub()
    slot = asyncio.Semaphore(1)
    await slot.acquire()

    async def _valid() -> bool:
        return True

    gen = _event_generator(
        pubsub=pubsub2,  # pyright: ignore[reportArgumentType]
        channel="chan",
        job_id=job_id,
        is_terminal=False,
        progress_seq=5,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=0.02,
        sse_slot_semaphore=slot,
        session_verifier=_valid,
    )
    events = [e async for e in gen]

    kinds = [getattr(e, "event", None) for e in events]
    assert kinds == ["progress", "terminal", "done"], f"got {kinds!r}"
    assert slot.locked() is False


async def test_hung_verifier_is_bounded_and_fails_closed() -> None:
    """A verifier that hangs -- a wedged IdP introspection endpoint -- must
    not freeze the generator inside its own keepalive path: the check is
    bounded by SESSION_RECHECK_TIMEOUT_SECS, the timeout is fail-closed
    revocation, and the finally releases the slot and the subscription."""
    pubsub = _KeepalivePubSub()
    slot = asyncio.Semaphore(1)
    await slot.acquire()

    async def _hung() -> bool:
        await asyncio.sleep(3600)
        # Unreachable unless the re-check bound is gone: reaching this raise
        # means the verifier was awaited to completion, i.e. the stream froze.
        raise AssertionError("the hung verifier completed: the re-check is unbounded")

    gen = _event_generator(
        pubsub=pubsub,  # pyright: ignore[reportArgumentType]  # Why: duck-typed pubsub double.
        channel="chan",
        job_id=new_uuid(),
        is_terminal=False,
        progress_seq=5,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=0.02,
        sse_slot_semaphore=slot,
        session_verifier=_hung,
    )
    started = time.monotonic()
    with structlog.testing.capture_logs() as logs:
        events = await _drain_limited(gen, limit=1)
    elapsed = time.monotonic() - started

    assert events == [], "a hung verifier is revocation: no frame may be emitted"
    assert elapsed < 60, f"the hung verifier must be bounded, took {elapsed:.1f}s"
    assert pubsub.unsubscribed and pubsub.closed, (
        "the wedged stream must still release its Redis subscription"
    )
    assert slot.locked() is False, "the wedged stream must still release its SSE slot"
    timeouts = [e for e in logs if e.get("event") == "sse-session-recheck-timeout"]
    assert timeouts, (
        "the timeout must be visible as its own incident: logging it as a "
        "plain revocation sends an operator chasing a logout that never happened"
    )


async def test_heartbeat_interval_above_the_cap_is_clamped_and_warned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A host configuring an hour between keepalives gets the 60 s
    revocation bound anyway -- and the clamp itself must not be silent: the
    bound is a security property (#316), so a host that asked for hours has
    to be told the effective value differs."""
    import taskq.web.progress as progress_mod

    real_gen = progress_mod._event_generator
    captured: dict[str, Any] = {}

    async def _spy(
        pubsub: Any,
        channel: str,
        job_id: UUID,
        is_terminal: bool,
        progress_seq: int,
        progress_data: str,
        resolved_last_event_id: int | None,
        heartbeat_secs: float,
        sse_slot_semaphore: asyncio.Semaphore | None = None,
        session_verifier: Callable[[], Awaitable[bool]] | None = None,
    ) -> AsyncIterator[Any]:
        # Runs when the route builds the generator, before streaming starts.
        captured["heartbeat_secs"] = heartbeat_secs
        async for event in real_gen(
            pubsub,
            channel,
            job_id,
            is_terminal,
            progress_seq,
            progress_data,
            resolved_last_event_id,
            heartbeat_secs,
            sse_slot_semaphore,
            session_verifier,
        ):
            yield event

    monkeypatch.setattr(progress_mod, "_event_generator", _spy)

    claims = IdentityClaims(subject="ops", email=None, groups=frozenset(), raw={})

    async def _dependency(request: Request) -> IdentityClaims:  # pyright: ignore[reportUnusedFunction]  # Why: registered via Depends at router level.
        return claims

    _dependency.session_verifier = _dependency  # type: ignore[attr-defined]  # Why: never re-invoked; the request below disconnects before any tick.

    with structlog.testing.capture_logs() as logs:
        # The clamp warning fires inside create_router, so the capture must
        # wrap the construction.
        router = create_router(
            _StubPool(),  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub pool satisfies the asyncpg surface the route reads.
            _StubRedis(_KeepalivePubSub()),
            schema="taskq",
            auth_dependency=_dependency,
            sse_heartbeat_interval=timedelta(hours=1),
            max_sse_connections=_MAX_SSE,
        )
        app = FastAPI()
        app.include_router(router, prefix="/jobs")

        baseline: frozenset[asyncio.Task[object]] = frozenset(asyncio.all_tasks())
        try:
            # The generator object is constructed while the route builds the
            # response; an immediate disconnect tears the stream down after.
            await app(
                _asgi_scope(new_uuid()),
                _immediate_disconnect,
                _noop_send,
            )
        finally:
            await _reap_stream_teardown_tasks(baseline)

    assert captured.get("heartbeat_secs") == 60.0, (
        f"the configured 3600 s interval must be clamped to the 60 s cap, "
        f"got {captured.get('heartbeat_secs')!r}"
    )
    clamps = [e for e in logs if e.get("event") == "sse-heartbeat-interval-clamped"]
    assert len(clamps) == 1, f"the clamp must warn exactly once per router, got {clamps!r}"
    assert clamps[0].get("configured_seconds") == 3600.0
    assert clamps[0].get("effective_seconds") == 60.0


async def _immediate_disconnect() -> dict[str, Any]:
    return {"type": "http.disconnect", "body": b"", "more_body": False}


async def _noop_send(message: dict[str, Any]) -> None:
    pass
