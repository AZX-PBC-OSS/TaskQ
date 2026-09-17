"""Shared transport helpers for client streaming (Redis pub/sub and PG polling)."""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import structlog

from taskq._close import CLOSE_TIMEOUT_SECS, close_redis_bounded
from taskq.backend._protocol import JobId, JobRow, JobStatus
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.progress._events import ProgressEvent

logger = structlog.get_logger("taskq.client._transport")


def _is_terminal(event: Any) -> bool:
    return bool(getattr(event, "terminal", None))


def parse_progress_event(raw_str: str, *, job_id: JobId) -> ProgressEvent | None:
    """Parse one published progress message, or ``None`` when it is not one.

    A message on a job's channel that does not deserialise is discarded so
    the stream carries on - the next publish or the safety-net re-fetch
    still delivers the state - but never silently: a publisher emitting
    garbage on the channel is a defect an operator has to be able to see,
    so every discard is logged with the job it belonged to. Both Redis
    streams (``TaskQ.stream`` and ``JobHandle.progress_stream``) go
    through here so neither can drop a message without a trace.
    """
    try:
        return ProgressEvent.model_validate_json(raw_str)
    except Exception as exc:
        logger.warning(
            "stream-event-deserialise-error",
            job_id=str(job_id),
            error=repr(exc),
        )
        return None


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
        # Why bounded: keeps "every TaskQ-initiated close is bounded" true —
        # the helper never raises, so the redundant suppress is dropped and a
        # hung broker cannot wedge the stream finalizer. Module-global read
        # at call time: tests monkeypatch CLOSE_TIMEOUT_SECS to shrink it.
        await close_redis_bounded(pubsub, "client-transport", CLOSE_TIMEOUT_SECS)


async def pg_poll_event_stream[EventT](
    fetch_row: Callable[[], Awaitable[JobRow | None]],
    row_to_event: Callable[[JobRow, bool], EventT],
    *,
    job_id: JobId,
    poll_interval: float = 0.5,
    last_seq: int = -1,
    last_status: JobStatus | None = None,
) -> AsyncIterator[EventT]:
    """Poll for row changes and yield events on seq/status change.

    *row_to_event* receives the row and a ``status_changed`` flag so the
    caller can distinguish state-change events from progress-only updates.
    Terminates when a terminal status is reached - or, without a terminal
    event, when the row is no longer found (pruned or deleted underneath
    the stream); that end is logged as ``progress-stream-job-missing`` so
    a stream that stopped short of terminal can be traced to its cause.

    A transient pool or connection error on a fetch does not end the
    stream: one warning (``stream-poll-error``, the job id and the
    exception type only - never the message, which can carry server-side
    text) is logged and the loop retries after the next poll interval, the
    same survive-and-retry the LISTEN transport gave connection blips. A
    vanished row is not an error and still ends the generator, so
    ``TaskQ.stream``'s KeyError contract is unchanged.
    """
    import asyncpg

    seq = last_seq
    status = last_status
    while True:
        await asyncio.sleep(poll_interval)
        try:
            row = await fetch_row()
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError) as exc:
            # One warning per failed fetch, named for the job and the
            # exception type: enough to trace an outage, without the
            # message text (server errors can quote data). The next poll
            # interval is the retry; a blip that clears is invisible
            # beyond this line, and one that does not keeps logging here
            # rather than killing the caller's async for.
            logger.warning(
                "stream-poll-error",
                job_id=str(job_id),
                error_type=type(exc).__name__,
            )
            continue
        if row is None:
            logger.warning("progress-stream-job-missing", job_id=str(job_id))
            return
        if row.progress_seq == seq and row.status == status:
            continue
        status_changed = row.status != status
        seq = row.progress_seq
        status = row.status
        yield row_to_event(row, status_changed)
        if row.status in TERMINAL_STATUSES:
            return
