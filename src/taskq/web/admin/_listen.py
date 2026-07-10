"""Shared async generator for PG LISTEN/NOTIFY with automatic reconnection."""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from typing import TYPE_CHECKING

import asyncpg
import structlog

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

type _Conn = asyncpg.Connection | PoolConnectionProxy

_log = structlog.get_logger("taskq.web.admin.listen")

# Why: an unbounded queue lets a slow/disconnected SSE consumer accumulate
# NOTIFY payloads without limit, which is an unbounded-memory-growth risk
# under sustained load. Cap it and drop the oldest payload on overflow —
# admin-UI live updates are best-effort, so losing a stale event in favor
# of newer ones is the right tradeoff.
_QUEUE_MAXSIZE = 1000


def _make_notify_callback(
    q: asyncio.Queue[str | None],
    channel: str = "",
) -> Callable[[_Conn, int, str, str], None]:
    dropped_logged = False

    def _on_notify(c: _Conn, pid: int, ch: str, payload: str) -> None:
        nonlocal dropped_logged
        if not payload:
            return
        if q.full():
            with suppress(asyncio.QueueEmpty):
                q.get_nowait()
            if not dropped_logged:
                _log.warning(
                    "listen-queue-overflow-drop-oldest",
                    channel=channel,
                    maxsize=_QUEUE_MAXSIZE,
                )
                dropped_logged = True
        q.put_nowait(payload)

    return _on_notify


async def listen_with_reconnect(
    pool: asyncpg.Pool,
    channel: str,
    *,
    keepalive_interval: float = 30.0,
    backoff_initial: float = 1.0,
    backoff_max: float = 30.0,
    acquire_timeout: float = 5.0,
) -> AsyncGenerator[str | None, None]:
    """Yield NOTIFY payloads from *channel* with automatic reconnection.

    Yields each payload string as it arrives.  Yields ``None`` as a keepalive
    signal when no payload arrives within *keepalive_interval*.  The caller
    should break out of the loop (e.g. on client disconnect) to stop
    listening; the generator cleans up the connection in its ``finally``.
    """
    backoff = backoff_initial
    while True:
        queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        conn: _Conn | None = None
        cb = _make_notify_callback(queue, channel)
        try:
            conn = await asyncio.wait_for(pool.acquire(), timeout=acquire_timeout)
            await conn.execute(f'LISTEN "{channel}"')
            await conn.add_listener(channel, cb)  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow callback type; runtime asyncpg accepts sync callbacks
            backoff = backoff_initial
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=keepalive_interval)
                    if payload is None:
                        return
                    yield payload
                except TimeoutError:
                    yield None
        except (
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            asyncpg.AdminShutdownError,
            OSError,
        ):
            _log.debug("listen-connection-lost", channel=channel)
            yield None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max)
        except Exception:
            _log.debug("listen-reconnect", channel=channel, exc_info=True)
            yield None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max)
        finally:
            if conn is not None:
                with suppress(Exception):
                    await conn.remove_listener(channel, cb)  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow callback type; runtime accepts sync callbacks
                with suppress(Exception):
                    await conn.execute(f'UNLISTEN "{channel}"')
                with suppress(Exception):
                    await pool.release(conn)
