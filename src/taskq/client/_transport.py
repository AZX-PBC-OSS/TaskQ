"""Shared transport helpers for client streaming (Redis pub/sub and PG polling)."""

import asyncio
import contextlib
import random
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any, Final

import structlog
from pydantic import ValidationError

from taskq._close import CLOSE_TIMEOUT_SECS, close_redis_bounded
from taskq.backend._protocol import JobId, JobRow, JobStatus
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.exceptions import StreamUnavailable
from taskq.progress._events import ProgressEvent

logger = structlog.get_logger("taskq.client._transport")

POLL_INTERVAL_FLOOR_SECS: Final[float] = 0.1
"""Shortest cadence the Postgres poll transport will re-read a job row at.

``TaskQ.stream`` lowers its cadence to ``poll_timeout`` when that is under
the default, so a caller asking for a 1 ms safety net on Redis would have
turned every Postgres-only stream into a thousand primary-key reads per
second. Ten reads per second per stream is the ceiling whatever the
caller asks for; the floor is the transport's own, not derived from
``poll_timeout``."""

POLL_JITTER_FRACTION: Final[float] = 0.2
"""Each wait between polls is spread uniformly over ±20% of the interval,
so N streams opened together do not re-read the database in lockstep for
their whole life."""

POLL_FAILURE_BUDGET_SECS: Final[float] = 30.0
"""How long consecutive poll failures are retried before the stream ends
with :class:`~taskq.exceptions.StreamUnavailable`.

A blip - a connection reset, a pool drained by a credential rotation -
clears in seconds and is retried silently beyond a warning per failed
poll. Thirty seconds without one successful read is the bound this tree
applies to every other wait on an outside system (the reload factory
timeout, the notify reconnect back-off ceiling): past it the database is
not coming back on its own, and a stream that keeps polling forever
leaves its caller's ``async for`` waiting on nothing with no error."""


def _is_infra_error(exc: BaseException) -> bool:
    import asyncpg

    return isinstance(exc, (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError))


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
    except ValidationError as exc:
        # The failing locations and their count, never the error's repr: a
        # pydantic ValidationError embeds input_value, which is the message
        # payload itself - a user's progress detail or data field - and
        # this warning is emitted for every consumer of the channel.
        logger.warning(
            "stream-event-deserialise-error",
            job_id=str(job_id),
            error_type=type(exc).__name__,
            error_count=exc.error_count(),
            locations=sorted({".".join(str(part) for part in err["loc"]) for err in exc.errors()}),
        )
        return None
    except Exception as exc:
        logger.warning(
            "stream-event-deserialise-error",
            job_id=str(job_id),
            error_type=type(exc).__name__,
        )
        return None


async def redis_event_stream[EventT](
    redis_client: Any,
    channel: str,
    poll_timeout: float,
    decode_message: Callable[[str], Awaitable[EventT | None]],
    on_timeout: Callable[[], Awaitable[EventT | None]] | None = None,
) -> AsyncGenerator[EventT, None]:
    """Subscribe to a Redis channel and yield decoded events.

    Handles subscribe, the get_message loop, malformed message skipping,
    and cleanup (unsubscribe + close). *decode_message* is called for each
    valid message; *on_timeout* is called when get_message returns None
    (poll timeout). Both may return None to skip yielding.

    The return type is the async-generator type, not the iterator protocol:
    consumers close this generator FROM UPSTREAM (contextlib.aclosing) on
    every exit that is not exhaustion, and aclosing needs the aclose
    surface.
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
        # Why bounded: keeps "every TaskQ-initiated close is bounded" true ,
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
    failure_budget: float = POLL_FAILURE_BUDGET_SECS,
    clock: Callable[[], float] = time.monotonic,
) -> AsyncGenerator[EventT, None]:
    """Poll for row changes and yield events on seq/status change.

    *row_to_event* receives the row and a ``status_changed`` flag so the
    caller can distinguish state-change events from progress-only updates.
    Terminates when a terminal status is reached - or, without a terminal
    event, when the row is no longer found (pruned or deleted underneath
    the stream); that end is logged as ``progress-stream-job-missing`` so
    a stream that stopped short of terminal can be traced to its cause.

    *poll_interval* is clamped to :data:`POLL_INTERVAL_FLOOR_SECS` and each
    wait is jittered by :data:`POLL_JITTER_FRACTION`, so a tiny interval
    cannot hammer the database and streams opened together drift apart.

    A transient pool or connection error on a fetch does not end the
    stream: one warning (``stream-poll-error``, the job id and the
    exception type only - never the message, which can carry server-side
    text) is logged and the loop retries after the next poll interval, the
    same survive-and-retry the LISTEN transport gave connection blips.
    Consecutive failures spanning more than *failure_budget* seconds
    (measured on *clock*) end the stream with
    :class:`~taskq.exceptions.StreamUnavailable`, logged as
    ``stream-poll-abandoned``: a database that has not answered in that
    long is not a blip, and the caller must not wait on it forever. A
    successful fetch resets the budget. A vanished row is not an error and
    still ends the generator, so ``TaskQ.stream``'s KeyError contract is
    unchanged.
    """
    interval = max(poll_interval, POLL_INTERVAL_FLOOR_SECS)
    seq = last_seq
    status = last_status
    failures_since = None  # clock reading of the first failure in the current run
    consecutive_failures = 0
    while True:
        await asyncio.sleep(_jittered(interval))
        try:
            row = await fetch_row()
        except Exception as exc:
            if not _is_infra_error(exc):
                raise
            now = clock()
            if failures_since is None:
                failures_since = now
            consecutive_failures += 1
            elapsed = now - failures_since
            if elapsed >= failure_budget:
                logger.error(
                    "stream-poll-abandoned",
                    job_id=str(job_id),
                    error_type=type(exc).__name__,
                    consecutive_failures=consecutive_failures,
                    elapsed_secs=round(elapsed, 3),
                    failure_budget_secs=failure_budget,
                )
                raise StreamUnavailable(
                    job_id,
                    consecutive_failures=consecutive_failures,
                    elapsed=elapsed,
                    last_error=exc,
                ) from exc
            # One warning per failed fetch, named for the job and the
            # exception type: enough to trace an outage, without the
            # message text (server errors can quote data). The next poll
            # interval is the retry; a blip that clears is invisible
            # beyond this line.
            logger.warning(
                "stream-poll-error",
                job_id=str(job_id),
                error_type=type(exc).__name__,
                consecutive_failures=consecutive_failures,
                elapsed_secs=round(elapsed, 3),
            )
            continue
        failures_since = None
        consecutive_failures = 0
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


def _jittered(interval: float) -> float:
    """*interval* spread uniformly over ±:data:`POLL_JITTER_FRACTION`."""
    return interval * random.uniform(1.0 - POLL_JITTER_FRACTION, 1.0 + POLL_JITTER_FRACTION)  # noqa: S311  # Why: uniform is for poll-timing jitter, not cryptography; same non-crypto use as the notify reconnect back-off.
