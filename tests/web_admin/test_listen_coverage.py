"""Coverage tests for taskq.web.admin._listen (LISTEN/NOTIFY with reconnect).

Unit tests use a mocked pool to drive the reconnection, backoff, and cleanup
branches deterministically. Integration tests exercise the happy path,
keepalive loop, and connection cleanup against a real Postgres container.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin._listen import (  # Why: importorskip guard must precede.
    _make_notify_callback,
    listen_with_reconnect,
)

# ── _make_notify_callback ───────────────────────────────────────────────


async def test_make_notify_callback_puts_nonempty_payload() -> None:
    """A non-empty NOTIFY payload is enqueued."""
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    cb = _make_notify_callback(queue)
    cb(None, 1, "chan", "hello")  # pyright: ignore[reportArgumentType]  # Why: connection arg unused by callback.
    assert queue.qsize() == 1
    assert queue.get_nowait() == "hello"


async def test_make_notify_callback_skips_empty_payload() -> None:
    """An empty payload string is not enqueued (falsy)."""
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    cb = _make_notify_callback(queue)
    cb(None, 1, "chan", "")  # pyright: ignore[reportArgumentType]  # Why: connection arg unused by callback.
    assert queue.empty()


async def test_make_notify_callback_skips_none_payload() -> None:
    """A None payload is not enqueued (falsy)."""
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    cb = _make_notify_callback(queue)
    cb(None, 1, "chan", None)  # pyright: ignore[reportArgumentType]  # Why: connection arg unused by callback.
    assert queue.empty()


# ── listen_with_reconnect: reconnection (mocked pool) ───────────────────


async def test_listen_reconnect_on_postgres_connection_error() -> None:
    """A PostgresConnectionError on acquire yields a keepalive, then reconnects."""
    pool = MagicMock()
    good_conn = AsyncMock()
    pool.acquire = AsyncMock(side_effect=[asyncpg.PostgresConnectionError("lost"), good_conn])
    pool.release = AsyncMock()

    gen = listen_with_reconnect(
        pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
        "chan",
        keepalive_interval=0.05,
        backoff_initial=0.01,
        backoff_max=0.05,
    )
    first = await asyncio.wait_for(gen.__anext__(), timeout=5.0)
    assert first is None  # reconnect signal from the connection-lost branch
    second = await asyncio.wait_for(gen.__anext__(), timeout=5.0)
    assert second is None  # keepalive from the reconnected good connection
    await gen.aclose()

    assert pool.acquire.call_count == 2
    pool.release.assert_awaited_with(good_conn)


async def test_listen_reconnect_on_generic_exception() -> None:
    """A generic Exception on acquire yields a keepalive, then reconnects."""
    pool = MagicMock()
    good_conn = AsyncMock()
    pool.acquire = AsyncMock(side_effect=[RuntimeError("boom"), good_conn])
    pool.release = AsyncMock()

    gen = listen_with_reconnect(
        pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
        "chan",
        keepalive_interval=0.05,
        backoff_initial=0.01,
        backoff_max=0.05,
    )
    first = await asyncio.wait_for(gen.__anext__(), timeout=5.0)
    assert first is None  # reconnect signal from the generic-exception branch
    second = await asyncio.wait_for(gen.__anext__(), timeout=5.0)
    assert second is None  # keepalive from the reconnected good connection
    await gen.aclose()

    assert pool.acquire.call_count == 2
    pool.release.assert_awaited_with(good_conn)


async def test_listen_backoff_doubles_up_to_max() -> None:
    """Backoff doubles on consecutive connection failures, capped at backoff_max."""
    pool = MagicMock()
    good_conn = AsyncMock()
    # Three failures then success: forces backoff to double twice.
    pool.acquire = AsyncMock(
        side_effect=[
            asyncpg.PostgresConnectionError("e1"),
            asyncpg.PostgresConnectionError("e2"),
            asyncpg.PostgresConnectionError("e3"),
            good_conn,
        ]
    )
    pool.release = AsyncMock()

    gen = listen_with_reconnect(
        pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
        "chan",
        keepalive_interval=0.05,
        backoff_initial=0.01,
        backoff_max=0.02,
    )
    # Consume three reconnect signals then one keepalive from the good connection.
    for _ in range(3):
        assert await asyncio.wait_for(gen.__anext__(), timeout=5.0) is None
    assert await asyncio.wait_for(gen.__anext__(), timeout=5.0) is None
    await gen.aclose()

    assert pool.acquire.call_count == 4
    pool.release.assert_awaited_with(good_conn)


async def test_listen_finally_releases_connection_on_close() -> None:
    """aclose() triggers the finally block which releases the connection."""
    pool = MagicMock()
    good_conn = AsyncMock()
    pool.acquire = AsyncMock(return_value=good_conn)
    pool.release = AsyncMock()

    gen = listen_with_reconnect(
        pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
        "chan",
        keepalive_interval=0.05,
        backoff_initial=0.01,
        backoff_max=0.05,
    )
    # Step once so a connection is acquired and the generator is mid-listen.
    payload = await asyncio.wait_for(gen.__anext__(), timeout=5.0)
    assert payload is None  # keepalive
    await gen.aclose()

    pool.release.assert_awaited_with(good_conn)
    good_conn.remove_listener.assert_awaited()
    good_conn.execute.assert_awaited()  # UNLISTEN


# ── listen_with_reconnect: real Postgres (integration) ──────────────────


@pytest.mark.integration
@pytest.mark.xdist_group(name="listen")
async def test_listen_delivers_payload(pg_dsn: str) -> None:
    """A NOTIFY payload from another connection is delivered to the listener."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=2, max_size=4)
    try:
        channel = "test_listen_payload"
        gen = listen_with_reconnect(pool, channel, keepalive_interval=5.0)

        async def notify() -> None:
            await asyncio.sleep(0.4)  # let LISTEN register
            async with pool.acquire() as c:
                await c.execute("SELECT pg_notify($1, $2)", channel, "hello-payload")

        async with asyncio.TaskGroup() as tg:
            tg.create_task(notify())
            payload = await asyncio.wait_for(gen.__anext__(), timeout=5.0)

        assert payload == "hello-payload"
        await gen.aclose()
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.xdist_group(name="listen")
async def test_listen_keepalive_yields_none(pg_dsn: str) -> None:
    """With no NOTIFY, the generator yields None after the keepalive interval."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        gen = listen_with_reconnect(pool, "test_listen_keepalive", keepalive_interval=0.2)
        payload = await asyncio.wait_for(gen.__anext__(), timeout=3.0)
        assert payload is None
        await gen.aclose()
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.xdist_group(name="listen")
async def test_listen_cleanup_releases_connection(pg_dsn: str) -> None:
    """Closing the generator releases its connection back to the pool.

    Uses ``max_size=1`` so a failure to release would block the subsequent
    ``pool.acquire()`` and trip the timeout.
    """
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        gen = listen_with_reconnect(pool, "test_listen_cleanup", keepalive_interval=0.1)
        payload = await asyncio.wait_for(gen.__anext__(), timeout=3.0)
        assert payload is None  # keepalive: the sole connection is held by the generator
        await gen.aclose()

        conn = await asyncio.wait_for(pool.acquire(), timeout=3.0)
        try:
            assert await conn.fetchval("SELECT 1") == 1
        finally:
            await pool.release(conn)
    finally:
        await pool.close()
