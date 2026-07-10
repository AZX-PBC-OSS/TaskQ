"""Shared transport helpers for client streaming (Redis pub/sub and PG polling)."""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import structlog

from taskq.backend._protocol import JobRow, JobStatus

logger = structlog.get_logger("taskq.client._transport")

_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
)


def _is_terminal(event: Any) -> bool:
    return bool(getattr(event, "terminal", None))


async def redis_event_stream[EventT](
    redis_client: Any,
    channel: str,
    poll_timeout: float,
    decode_message: Callable[[str], Awaitable[EventT | None]],
    on_timeout: Callable[[], Awaitable[EventT | None]] | None = None,
) -> AsyncIterator[EventT]:
    """Subscribe to a Redis channel and yield decoded events.

    Handles subscribe, the get_message loop, malformed message skipping,
    and cleanup (unsubscribe + close). *decode_message* is called for each
    valid message; *on_timeout* is called when get_message returns None
    (poll timeout). Both may return None to skip yielding.
    """
    pubsub = redis_client.pubsub()
    try:
        await pubsub.subscribe(channel)
        while True:
            raw_msg = await pubsub.get_message(  # type: ignore[reportUnknownVariableType]  # Why: redis-py async stubs type get_message return as partially unknown.
                ignore_subscribe_messages=True,
                timeout=poll_timeout,
            )
            if raw_msg is None:
                if on_timeout is not None:
                    event = await on_timeout()
                    if event is not None:
                        yield event
                        if _is_terminal(event):
                            return
                continue

            raw_data: Any = raw_msg.get("data")  # type: ignore[reportUnknownArgumentType,reportUnknownVariableType]  # Why: redis-py stubs model get_message return with Unknown.
            if raw_data is None:
                continue

            try:
                raw_str = (
                    raw_data.decode("utf-8")
                    if isinstance(raw_data, (bytes, bytearray))
                    else str(raw_data)  # type: ignore[reportUnknownArgumentType]  # Why: raw_data is Any from redis-py stubs.
                )
            except Exception as exc:
                logger.warning(
                    "stream-event-decode-error",
                    channel=channel,
                    error=repr(exc),
                )
                continue

            event = await decode_message(raw_str)
            if event is not None:
                yield event
                if _is_terminal(event):
                    return
    finally:
        with contextlib.suppress(Exception):
            await pubsub.unsubscribe(channel)
        with contextlib.suppress(Exception):
            await pubsub.aclose()


async def pg_poll_event_stream[EventT](
    fetch_row: Callable[[], Awaitable[JobRow | None]],
    row_to_event: Callable[[JobRow, bool], EventT],
    *,
    poll_interval: float = 0.5,
    last_seq: int = -1,
    last_status: JobStatus | None = None,
) -> AsyncIterator[EventT]:
    """Poll for row changes and yield events on seq/status change.

    *row_to_event* receives the row and a ``status_changed`` flag so the
    caller can distinguish state-change events from progress-only updates.
    Terminates when the row is not found or a terminal status is reached.
    """
    seq = last_seq
    status = last_status
    while True:
        await asyncio.sleep(poll_interval)
        row = await fetch_row()
        if row is None:
            return
        if row.progress_seq == seq and row.status == status:
            continue
        status_changed = row.status != status
        seq = row.progress_seq
        status = row.status
        yield row_to_event(row, status_changed)
        if row.status in _TERMINAL_STATUSES:
            return
