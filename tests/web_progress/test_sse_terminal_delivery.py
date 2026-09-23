"""Terminal-delivery pins for the progress SSE generator (the delivery-loss
class of #457: the durable write landed, the delivery is deferred to the
pub/sub stream, and a cut between the two strands the delivery).

The durable source of truth for a job's progress is the ``jobs`` row; the
SSE stream is the delivery. The stream's seq-discard cursor is IN-MEMORY
state derived from what this subscriber last saw on the wire, and the
durable row can legitimately sit BEHIND that cursor: a worker publishes
each progress event to Redis as it is produced, the coalesced flush lands
the seq on the row up to half a second later, and a crash in between
leaves the row (and every seq the redispatched attempt consumes) behind
what a live subscriber already saw. The redispatched attempt's terminal
write then carries a seq at or below that subscriber's cursor.

Pre-fix the generator discarded that terminal envelope as a duplicate: the
stream never emitted ``terminal`` + ``done``, and a browser sat on
keepalives forever for a job that is durably over - the mirror of #457's
lost cancellation, the delivery dropped after the durable write landed.
Both pins here fail under that discard (the generator never returns, the
``asyncio.timeout`` trips) and pass once the terminal delivery is
re-derived from the durable state: a terminal envelope is never filtered,
and a reconnect whose snapshot row is already terminal delivers the
terminal snapshot even when the cursor is at or ahead of its seq.

The pins are deliberately asymmetric: a stale PROGRESS event stays
discarded (``test_duplicate_filter`` in ``test_unit.py`` keeps pinning
that), only the terminal is exempt.
"""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sse_starlette.event import ServerSentEvent

from taskq.progress._events import ProgressEvent
from taskq.web.progress import (
    _event_generator,  # pyright: ignore[reportPrivateUsage]  # Why: unit tests import private symbols to exercise them directly.
)

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000045")


class _StubPubSub:
    """Minimal redis PubSub duck-type; the same shape
    ``tests/web_progress/test_unit.py`` drives the generator with."""

    def __init__(self, messages: list[dict[str, Any] | None]) -> None:
        self._messages = list(messages)
        self._pos = 0
        self.unsubscribed = False
        self.closed = False

    async def subscribe(self, channel: str | bytes) -> None:
        pass

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message signature.
    ) -> dict[str, Any] | None:
        # Every real broker read suspends the task at least once (network
        # I/O); a stub that returns without ever awaiting starves the event
        # loop, and a bounded consumer's asyncio.timeout can then never
        # fire. The scripted reads yield once; the reads past the script
        # model a real poll timeout and sleep out the broker cadence.
        if self._pos < len(self._messages):
            item = self._messages[self._pos]
            self._pos += 1
            await asyncio.sleep(0)
            return item
        await asyncio.sleep(timeout)
        return None  # every read after the script: keepalive path

    async def unsubscribe(self, channel: str | bytes) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


def _event(
    *,
    seq: int,
    terminal: bool = False,
) -> ProgressEvent:
    return ProgressEvent(
        v=1,
        kind="state_change" if terminal else "progress",
        job_id=_JOB_ID,
        actor="test_actor",
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        seq=seq,
        status="succeeded" if terminal else "running",
        step=1,
        percent=10.0,
        terminal=terminal,
    )


def _redis_msg(event: ProgressEvent) -> dict[str, Any]:
    return {
        "type": "message",
        "channel": b"taskq:taskq:progress:" + str(_JOB_ID).encode(),
        "data": event.model_dump_json(exclude_none=True).encode(),
    }


async def _collect(gen: AsyncGenerator[ServerSentEvent, None]) -> list[ServerSentEvent]:
    """Consume the generator to completion under a hard bound.

    The bound is the RED detector: pre-fix the generator never returns (it
    emits keepalives forever instead of the terminal delivery), and the
    timeout - not a hang - is the failure.
    """
    results: list[ServerSentEvent] = []
    async with asyncio.timeout(5):
        async for sse_event in gen:
            results.append(sse_event)
    return results


# ── Live loop: a terminal envelope behind the cursor still closes ───────


@pytest.mark.asyncio
async def test_terminal_envelope_behind_cursor_still_closes_stream() -> None:
    """The redispatch shape: the subscriber's cursor sits at 7 (the last
    publish that rode Redis out before the crash ate the flush), the
    durable row re-seeded behind it, and the new attempt's terminal write
    carries seq 5. The terminal is the stream's only close signal, so it is
    delivered from the durable state whatever the cursor says, and the
    stream ends. The stale PROGRESS envelope before it stays discarded."""
    pubsub = _StubPubSub(
        [
            _redis_msg(_event(seq=4)),  # stale progress: discarded
            _redis_msg(_event(seq=5, terminal=True)),  # the durable terminal
        ]
    )

    results = await _collect(
        _event_generator(
            pubsub=pubsub,
            channel="taskq:taskq:progress:" + str(_JOB_ID),
            job_id=_JOB_ID,
            is_terminal=False,
            progress_seq=3,
            progress_data='{"step": 1}',
            resolved_last_event_id=7,
            heartbeat_secs=timedelta(seconds=15).total_seconds(),
        )
    )

    events = [r.event for r in results]
    assert "terminal" in events, f"the durable terminal was not delivered: {events!r}"
    assert "done" in events, f"the stream never closed: {events!r}"
    assert events.index("terminal") < events.index("done")
    terminal_ev = results[events.index("terminal")]
    assert terminal_ev.id == "5"
    assert terminal_ev.data is not None and "succeeded" in terminal_ev.data
    # The stale PROGRESS envelope is still discarded: the exemption is the
    # terminal's alone.
    assert "id: 4" not in "".join(r.encode().decode("utf-8") for r in results)
    # The terminal is the last frame before done, nothing streams after.
    assert events[-1] == "done"


# ── Reconnect: a terminal snapshot row behind the cursor still closes ────


@pytest.mark.asyncio
async def test_reconnect_terminal_row_behind_cursor_closes_stream() -> None:
    """Reconnect with Last-Event-ID 7 against a durable row that is
    terminal at seq 5 (the same cut, resumed through the browser's
    reconnect). The catch-up comparison fires nothing, and the channel will
    never carry another event for this job, so the terminal snapshot is the
    delivery: emitted here, then done."""
    pubsub = _StubPubSub([])

    results = await _collect(
        _event_generator(
            pubsub=pubsub,
            channel="taskq:taskq:progress:" + str(_JOB_ID),
            job_id=_JOB_ID,
            is_terminal=True,
            progress_seq=5,
            progress_data='{"step": 9}',
            resolved_last_event_id=7,
            heartbeat_secs=timedelta(seconds=15).total_seconds(),
        )
    )

    events = [r.event for r in results]
    assert events == ["terminal", "done"], f"expected terminal+done, got {events!r}"
    assert results[0].id == "5"
    # The reconnect payload is the durable row's coalesced progress state
    # (the terminal status lives in the event name, the snapshot carries
    # progress_state), not the redispatch shape's wire envelope.
    assert results[0].data == '{"step": 9}'
