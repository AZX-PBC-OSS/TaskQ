"""Post-revocation auth re-check for the admin /sse/{topic} endpoint (#316).

Same hole as the progress SSE bridge: the stream authenticated once at
request acceptance and never again, so an admin stream opened before the
session was invalidated kept receiving state_change frames after revocation.
The contract after the fix: ``_sse_generator`` re-invokes the session
verifier before the first frame and once per loop iteration -- before every
yielded event and at every keepalive tick -- and ends the stream on failure,
releasing the topic's semaphore slot in the finally.
"""

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

import structlog.testing
from fastapi import FastAPI, HTTPException, Request

import taskq.web.admin.sse as _sse_mod
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.auth._session import IdentityClaims
from taskq.web.admin.sse import _TOPIC_SEMAPHORES, _sse_generator

pytestmark = [pytest.mark.fastapi]

_TICK = 0.02


# ── generator-level pins (keepalive-only path) ────────────────────────────


async def test_sse_generator_revoked_before_open_yields_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A session already invalid yields no frame at all, not even the
    awaiting_progress_backend sentinel, and releases the slot."""
    monkeypatch.setattr(_sse_mod, "_KEEPALIVE_INTERVAL", _TICK)
    sem = asyncio.Semaphore(1)
    await sem.acquire()

    async def _revoked() -> bool:
        return False

    with structlog.testing.capture_logs() as logs:
        gen = _sse_generator(sem, lambda: None, None, _revoked)
        frames: list[str] = []
        async with asyncio.timeout(2.0):
            async for frame in gen:
                frames.append(frame)

    assert frames == [], f"no frame may be emitted on a revoked session, got {frames!r}"
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: no public API to read the permit state; the release is the assertion.
    assert any(log["event"] == "admin-sse-session-revoked" for log in logs)


async def test_sse_generator_flip_ends_stream_at_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The verifier passes for the sentinel and the first tick, then fails:
    the stream ends and never yields another frame."""
    monkeypatch.setattr(_sse_mod, "_KEEPALIVE_INTERVAL", _TICK)
    sem = asyncio.Semaphore(1)
    await sem.acquire()

    calls = {"n": 0}

    async def _flip_after_two() -> bool:
        calls["n"] += 1
        return calls["n"] < 3

    gen = _sse_generator(sem, lambda: None, None, _flip_after_two)
    frames: list[str] = []
    async with asyncio.timeout(2.0):
        async for frame in gen:
            frames.append(frame)

    assert frames[0].startswith("event: status"), "the checked pre-flip frame is still delivered"
    assert frames[-1] == ": keepalive\n\n"
    assert len(frames) <= 4, f"the revoked stream must stop at the next tick, got {frames!r}"
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: the finally must have released the slot.


async def test_sse_generator_verifier_error_fails_closed() -> None:
    """A verifier that raises is treated as revoked."""
    sem = asyncio.Semaphore(1)
    await sem.acquire()

    async def _broken() -> bool:
        raise RuntimeError("session store unavailable")

    gen = _sse_generator(sem, lambda: None, None, _broken)
    frames: list[str] = []
    async with asyncio.timeout(2.0):
        async for frame in gen:
            frames.append(frame)

    assert frames == []
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]


async def test_sse_generator_pg_path_gates_each_payload() -> None:
    """With the PG LISTEN backend, the re-check gates every payload and
    keepalive the same way."""

    async def _fake_listen(
        resolve_pool: Callable[[], object], channel: str, **kw: object
    ) -> AsyncIterator[str | None]:
        yield '{"job_id":"123","status":"running"}'
        yield None  # keepalive signal

    sem = asyncio.Semaphore(1)
    await sem.acquire()
    pool = object()

    calls = {"n": 0}

    async def _flip_after_three() -> bool:
        calls["n"] += 1
        return calls["n"] < 4

    import taskq.web.admin.sse as sse_mod

    original_listen = sse_mod.listen_with_reconnect
    sse_mod.listen_with_reconnect = _fake_listen  # type: ignore[assignment]  # Why: test-only seam; restored in finally.
    try:
        gen = _sse_generator(sem, lambda: pool, "taskq", _flip_after_three)
        frames: list[str] = []
        async with asyncio.timeout(2.0):
            async for frame in gen:
                frames.append(frame)
    finally:
        sse_mod.listen_with_reconnect = original_listen

    assert frames[0].startswith("event: status")
    assert any(f.startswith("event: state_change") for f in frames), (
        f"the payload checked pre-flip must be delivered, got {frames!r}"
    )
    assert frames[-1] == ": keepalive\n\n"
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]


# ── endpoint-level: the triaged scenario over raw ASGI ────────────────────


class _StubConn:
    async def fetchval(self, query: str, *args: object) -> object:
        # The clock-offset probe subtracts this from datetime.now(UTC): return
        # a datetime or the subtraction raises into a 500.
        return datetime.now(UTC)


class _StubAcquire:
    async def __aenter__(self) -> _StubConn:
        return _StubConn()

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def acquire(self, *, timeout: float | None = None) -> _StubAcquire:
        return _StubAcquire()


def _make_admin_app() -> tuple[FastAPI, dict[str, bool]]:
    """Build the admin router behind the real SSO wiring shape.

    Mirrors the progress-stream test one-for-one: the auth dependency and its
    exposed ``session_verifier`` attribute read one shared revocation flag.
    """
    auth_state = {"session_valid": True}
    claims = IdentityClaims(subject="ops", email=None, groups=frozenset(), raw={})

    async def _dependency(request: Request) -> IdentityClaims:
        _ = request
        if not auth_state["session_valid"]:
            raise HTTPException(status_code=401, detail="session revoked")
        return claims

    async def _verifier(request: Request) -> bool:
        _ = request
        return auth_state["session_valid"]

    _dependency.session_verifier = _verifier  # pyright: ignore[reportFunctionMemberAccess]  # Why: the re-check the SSO dependency exposes; create_auth_dependency attaches the same attribute.

    bundle = create_router(
        _StubPool(),  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub pool, as the rest of the web_admin suite.
        auth_dependency=_dependency,
        base_path="",
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app, auth_state


async def test_admin_sse_stream_opened_before_revocation_ends_after_recheck(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A /sse/{topic} stream opened BEFORE session invalidation must not
    outlive it: it ends at the next re-check tick and releases the slot."""
    monkeypatch.setattr(_sse_mod, "_KEEPALIVE_INTERVAL", _TICK)
    app, auth_state = _make_admin_app()

    received: list[bytes] = []
    _disco = asyncio.Event()

    async def _receive() -> dict[str, Any]:
        # Real ASGI receive parks until the client sends or disconnects; an
        # instant-return receive spins sse-starlette's/_sse_generator's loop.
        await _disco.wait()
        return {"type": "http.disconnect", "body": b"", "more_body": False}

    async def _send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body":
            received.append(message.get("body", b""))

    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/sse/jobs",
        "raw_path": b"/sse/jobs",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"host", b"test")],
        "client": ("testclient", 123),
        "server": ("testserver", 80),
    }
    task = asyncio.create_task(app(scope, _receive, _send))
    try:
        deadline = time.monotonic() + 5.0
        # Why not an Event: the waiter can start after the sentinel already
        # landed, so it polls the buffer rather than racing a one-shot set.
        while time.monotonic() < deadline:
            if b"awaiting_progress_backend" in b"".join(received):
                break
            await asyncio.sleep(0.005)
        assert b"awaiting_progress_backend" in b"".join(received), "sentinel never arrived"

        auth_state["session_valid"] = False
        await asyncio.sleep(_TICK * 3)

        tail_len = len(received)
        deadline = time.monotonic() + 2.0
        while not task.done() and time.monotonic() < deadline:  # noqa: ASYNC110  # Why: bounded task-done poll - completion is observable on the task, not a signal this task can await.
            await asyncio.sleep(0.005)

        assert task.done(), (
            "CONTRACT: an admin /sse stream whose session is revoked must end "
            "at the next re-check tick. PRE-FIX (#316): it kept delivering "
            f"frames after revocation; tail: {b''.join(received[tail_len:])!r}"
        )
        keepalives_after_flip = b"".join(received[tail_len:]).count(b": keepalive")
        assert keepalives_after_flip <= 1, (
            f"at most the in-flight tick may follow the revocation, got "
            f"{keepalives_after_flip}: {b''.join(received[tail_len:])!r}"
        )

        await asyncio.sleep(0.05)
        # The topic semaphore ("jobs") must have its permit back.
        semaphore = _TOPIC_SEMAPHORES["jobs"]
        assert semaphore.locked() is False, (
            "CONTRACT: the ended stream's semaphore slot must be released, or "
            "every revocation leaks one slot from the topic cap"
        )
    finally:
        _TOPIC_SEMAPHORES.clear()
        if not task.done():
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
