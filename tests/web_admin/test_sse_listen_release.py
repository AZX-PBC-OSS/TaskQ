"""The admin SSE consumer must return its LISTEN connection on every exit.

``_sse_generator`` hands the stream's Postgres LISTEN connection back through
the feed's own ``finally`` (remove_listener, UNLISTEN, ``pool.release``), and
that finally only runs when the feed generator itself is closed. The consumer
closes it from its own finally on every exit it observes: a client disconnect
arrives as GeneratorExit (the response iterator is closed out from under the
stream), a hard cancellation as CancelledError, a failed feed as the feed's
own exception. Each test here drives one exit through the REAL consumer, with
a stub feed that acquires from a counting pool and releases the way
``listen_with_reconnect`` does, and demands the pool's counts return to their
pre-stream state.

The fourth exit is the sharp one: a stream whose exit is ITSELF cancelled.
``wait_for`` cancels the task it is waiting on when the wait is cancelled, so
a second cancellation delivered while the close is in flight (an anyio cancel
scope re-delivers at every checkpoint, which is the shape every ASGI server
streams under) kills the close mid-flight and abandons the feed before its
release -- a connection stranded against the pool cap until restart. The
close therefore runs shielded: the release completes even when the exit is
cancelled, while the cancellation still propagates out of the stream.
"""

import asyncio
import gc
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
import structlog.testing

pytest.importorskip("fastapi")

import taskq.web.admin.sse as _sse_mod
from taskq.web.admin.sse import _sse_generator

pytestmark = [pytest.mark.fastapi]


class _Conn:
    """Stand-in for the pool's connection handle; identity is all that matters."""


class _CountingPool:
    """asyncpg.Pool duck-type recording every acquire/release, so a test can
    demand the counts return to their pre-stream state."""

    def __init__(self) -> None:
        self.acquired: list[_Conn] = []
        self.released: list[_Conn] = []

    @property
    def in_use(self) -> int:
        return len(self.acquired) - len(self.released)

    async def acquire(self, timeout: float | None = None) -> _Conn:
        conn = _Conn()
        self.acquired.append(conn)
        return conn

    async def release(self, conn: _Conn) -> None:
        self.released.append(conn)


class _StubFeed:
    """listen_with_reconnect double with the real contract's shape.

    Acquires on the first tick, yields its payloads, then parks (a live
    stream never exhausts on its own), and releases from its finally -- the
    release waits on ``release_gate`` so a test can hold the close mid-flight
    and re-cancel the stream while it runs.
    """

    def __init__(self, pool: _CountingPool, error: Exception | None = None) -> None:
        self._pool = pool
        self._error = error
        self.first_tick = asyncio.Event()
        self.aclose_started = asyncio.Event()
        self.released = asyncio.Event()
        self.release_gate = asyncio.Event()
        self.release_gate.set()

    def __call__(
        self, resolve_pool: Callable[[], Any], channel: str, **kwargs: object
    ) -> AsyncIterator[str | None]:
        return self._feed()

    async def _feed(self) -> AsyncIterator[str | None]:
        conn = await asyncio.wait_for(self._pool.acquire(), timeout=5.0)
        try:
            self.first_tick.set()
            yield '{"job_id":"1","status":"running"}'
            yield '{"job_id":"1","status":"running"}'
            if self._error is not None:
                raise self._error
            await asyncio.sleep(3600)
        finally:
            self.aclose_started.set()
            await self.release_gate.wait()
            await self._pool.release(conn)
            self.released.set()


class _StubConn:
    """Connection double for the REAL ``listen_with_reconnect``: every
    cleanup step is an immediate no-op except the release, which the pool
    below gates."""

    def is_closed(self) -> bool:
        return False

    async def execute(self, sql: str) -> None:
        return None

    async def add_listener(self, channel: str, cb: object) -> None:
        return None

    async def remove_listener(self, channel: str, cb: object) -> None:
        return None


class _GatedReleasePool:
    """Duck-typed pool driving the REAL ``listen_with_reconnect``: acquire
    hands out a ``_StubConn``, release parks on a gate so a test can cancel
    the close mid-cleanup (the shape the consumer's bounded close produces
    when its ``wait_for`` times out) and demand the release still happen."""

    def __init__(self) -> None:
        self.conn = _StubConn()
        self.acquired_n = 0
        self.released_n = 0
        self.cleanup_started = asyncio.Event()
        self.release_gate = asyncio.Event()
        self.released = asyncio.Event()

    async def acquire(self, timeout: float | None = None) -> _StubConn:
        self.acquired_n += 1
        return self.conn

    async def release(self, conn: _StubConn) -> None:
        self.cleanup_started.set()
        await self.release_gate.wait()
        self.released_n += 1
        self.released.set()

    @property
    def in_use(self) -> int:
        return self.acquired_n - self.released_n


class _ParkedVerifier:
    """Session verifier that passes twice (initial check + first event) and
    parks on the third, suspending the consumer at its OWN await while the
    feed stays suspended at a yield."""

    def __init__(self) -> None:
        self.calls = 0
        self.parked = asyncio.Event()

    async def __call__(self) -> bool:
        self.calls += 1
        if self.calls <= 2:
            return True
        self.parked.set()
        await asyncio.sleep(3600)
        return True


async def _await_released(feed: _StubFeed) -> None:
    await asyncio.wait_for(feed.released.wait(), timeout=5.0)


async def _detached_failure_logged(logs: list[dict[str, Any]]) -> None:
    """Bounded poll for the retrieval callback's log line. The callback is
    the only thing that retrieves the detached close's outcome, so the line
    is the proof of retrieval. How the running Python schedules done
    callbacks varies by version (3.14's C task internals reshuffle the
    timing), so the probe waits on the line itself instead of asserting at
    a fixed instant; wait_for bounds the wait."""
    while not any(log["event"] == "shield-detached-task-failed" for log in logs):  # noqa: ASYNC110  # Why: bounded poll for a log line capture_logs appends to; no Event exists to wait on.
        await asyncio.sleep(0)


async def _run_to_first_payload(
    gen: AsyncIterator[str],
) -> str:
    await gen.__anext__()  # sentinel
    payload = await gen.__anext__()  # first state_change event
    assert "state_change" in payload
    return payload


async def test_client_disconnect_returns_the_listen_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A client disconnect closes the stream: the feed must be closed with it
    and the pool's counts return to their pre-stream state."""
    pool = _CountingPool()
    feed = _StubFeed(pool)
    monkeypatch.setattr(_sse_mod, "listen_with_reconnect", feed)
    sem = asyncio.Semaphore(1)
    await sem.acquire()  # the route's pre-acquire
    gen = _sse_generator(sem, lambda: pool, "taskq")

    await _run_to_first_payload(gen)
    await gen.aclose()  # the disconnect shape: GeneratorExit at the yield

    await _await_released(feed)
    assert pool.in_use == 0
    assert len(pool.released) == 1
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: no public API reads a semaphore's permit count; the cap test needs it.


async def test_cancellation_mid_iteration_returns_the_listen_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation delivered into the stream's own await ends the stream;
    the feed must be closed and the connection returned."""
    pool = _CountingPool()
    feed = _StubFeed(pool)
    monkeypatch.setattr(_sse_mod, "listen_with_reconnect", feed)
    verifier = _ParkedVerifier()
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    gen = _sse_generator(sem, lambda: pool, "taskq", verifier)

    await _run_to_first_payload(gen)
    stream = asyncio.create_task(gen.__anext__())
    await verifier.parked.wait()
    stream.cancel()

    with pytest.raises(asyncio.CancelledError):
        await stream
    await _await_released(feed)
    assert pool.in_use == 0
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: as above.


async def test_a_cancelled_exit_cannot_defeat_the_listen_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sharp one: a SECOND cancellation delivered while the close is in
    flight must not kill the close. wait_for cancels the task it waits on
    when the wait itself is cancelled; unshielded, the feed's finally dies at
    its checkpoint and the LISTEN connection strands against the pool cap.
    The release must complete anyway, and the cancellation must still
    propagate."""
    pool = _CountingPool()
    feed = _StubFeed(pool)
    feed.release_gate.clear()  # hold the close mid-flight
    monkeypatch.setattr(_sse_mod, "listen_with_reconnect", feed)
    verifier = _ParkedVerifier()
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    gen = _sse_generator(sem, lambda: pool, "taskq", verifier)

    await _run_to_first_payload(gen)
    stream = asyncio.create_task(gen.__anext__())
    await verifier.parked.wait()
    stream.cancel()  # first cancel: the stream's exit begins
    await feed.aclose_started.wait()  # the close is now in flight
    stream.cancel()  # second cancel: re-delivery while the close runs
    feed.release_gate.set()

    with pytest.raises(asyncio.CancelledError):
        await stream
    await _await_released(feed)
    assert pool.in_use == 0
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: as above.


async def test_a_third_cancellation_during_the_shielded_close_still_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A THIRD cancellation - delivered after the shield has already detached
    the close - must not disturb the release either: each re-delivery
    re-raises at the shield (the stream still ends cancelled) while the
    detached close is a different task nothing can reach."""
    pool = _CountingPool()
    feed = _StubFeed(pool)
    feed.release_gate.clear()
    monkeypatch.setattr(_sse_mod, "listen_with_reconnect", feed)
    verifier = _ParkedVerifier()
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    gen = _sse_generator(sem, lambda: pool, "taskq", verifier)

    await _run_to_first_payload(gen)
    stream = asyncio.create_task(gen.__anext__())
    await verifier.parked.wait()
    stream.cancel()  # first cancel: the stream's exit begins
    await feed.aclose_started.wait()  # the close is now in flight
    stream.cancel()  # second cancel: re-delivery at the shield
    await asyncio.sleep(0)  # let the second delivery land at the shield
    stream.cancel()  # third cancel: while the detached close still runs
    feed.release_gate.set()

    with pytest.raises(asyncio.CancelledError):
        await stream
    await _await_released(feed)
    assert pool.in_use == 0
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: as above.


async def test_the_close_that_outlives_its_bound_still_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bound's own teeth: a close that outlives CLOSE_TIMEOUT_SECS is
    cancelled by its wait_for MID-CLEANUP - the one cancellation the
    consumer's shield cannot keep off the close. Unshielded at the feed,
    the cleanup dies before pool.release and the connection strands past
    any GC revival. The REAL feed's cleanup is cancellation-tolerant: the
    release must complete anyway (detached), and the bound must give up
    LOUDLY (a structured warning, not silence)."""
    monkeypatch.setattr(_sse_mod, "CLOSE_TIMEOUT_SECS", 0.1)
    monkeypatch.setattr(_sse_mod, "_KEEPALIVE_INTERVAL", 0.05)
    pool = _GatedReleasePool()
    pool.release_gate.clear()  # hold the cleanup mid-flight
    verifier = _ParkedVerifier()
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    # The REAL feed: no listen_with_reconnect monkeypatch here.
    gen = _sse_generator(sem, lambda: pool, "taskq", verifier)

    with structlog.testing.capture_logs() as logs:
        await gen.__anext__()  # sentinel
        keepalive = await gen.__anext__()  # the feed's first keepalive frame
        assert "keepalive" in keepalive
        stream = asyncio.create_task(gen.__anext__())
        await verifier.parked.wait()
        stream.cancel()  # the stream's exit begins
        await pool.cleanup_started.wait()  # the feed's cleanup is mid-flight
        await asyncio.sleep(0.25)  # past the 0.1s bound: it fires mid-cleanup
        with pytest.raises(asyncio.CancelledError):
            await stream
        pool.release_gate.set()
        await asyncio.wait_for(pool.released.wait(), timeout=3.0)

    assert pool.in_use == 0
    loud = [log for log in logs if log["event"] == "admin-sse-listen-close-failed"]
    assert loud, "a close that outlived its bound must give up loudly, not silently"


async def test_the_detached_close_does_not_lose_its_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The detached close must be RETRIEVED, not just launched: when the
    outer exit is cancelled, nobody awaits the shielded close, so a close
    that fails while detached (here: it outlives its bound and its
    wait_for raises TimeoutError) must be picked up by the retrieval
    callback and logged - not silently lost. The captured
    ``shield-detached-task-failed`` log record is the contract: the
    retrieval callback is the only code that emits it, so the line proves
    the failure was retrieved. (A loop exception-handler count is NOT the
    observation: 3.14's C task internals record the un-retrieved-exception
    bookkeeping on a different clock than the done-callback retrieval, so
    the handler sees the TimeoutError even when it was retrieved and
    logged - observed on 3.14.7 with the log line present. The log record
    is deterministic on every version; the handler bookkeeping is not.)"""
    monkeypatch.setattr(_sse_mod, "CLOSE_TIMEOUT_SECS", 0.1)
    pool = _CountingPool()
    feed = _StubFeed(pool)
    feed.release_gate.clear()  # the close wedges past its bound
    monkeypatch.setattr(_sse_mod, "listen_with_reconnect", feed)
    verifier = _ParkedVerifier()
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    gen = _sse_generator(sem, lambda: pool, "taskq", verifier)

    with structlog.testing.capture_logs() as logs:
        await _run_to_first_payload(gen)
        stream = asyncio.create_task(gen.__anext__())
        await verifier.parked.wait()
        stream.cancel()  # first cancel: the stream's exit begins
        await feed.aclose_started.wait()  # the close is in flight
        stream.cancel()  # second cancel: the close is now DETACHED
        with pytest.raises(asyncio.CancelledError):
            await stream
        await asyncio.sleep(0.3)  # the 0.1s bound fires in the detached close
        gc.collect()  # the retrieval callback's scheduling is GC-adjacent
        await asyncio.wait_for(_detached_failure_logged(logs), timeout=5.0)

    retrieved = [log for log in logs if log["event"] == "shield-detached-task-failed"]
    assert retrieved, "the detached close's failure must be retrieved and logged"


async def test_an_event_generator_exception_returns_the_listen_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A feed that dies mid-stream releases the connection as its exception
    propagates, and the consumer's close must not disturb that."""
    pool = _CountingPool()
    feed = _StubFeed(pool, error=RuntimeError("feed lost"))
    monkeypatch.setattr(_sse_mod, "listen_with_reconnect", feed)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    gen = _sse_generator(sem, lambda: pool, "taskq")

    await _run_to_first_payload(gen)
    await gen.__anext__()  # the second event; the feed parks past it
    with pytest.raises(RuntimeError, match="feed lost"):
        await gen.__anext__()

    await _await_released(feed)
    assert pool.in_use == 0
    assert sem._value == 1  # pyright: ignore[reportPrivateUsage]  # Why: as above.
