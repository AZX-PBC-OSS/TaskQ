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
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

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
