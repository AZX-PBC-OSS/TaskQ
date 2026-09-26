"""The SSE stream gauntlet: proxy lies and broker hangs on the progress
stream (``web/progress.py``'s ``_event_generator``).

Two defensive families previously had no unit execution:

1. The READ-TIMEOUT fail-visible arm (``web/progress.py``'s
   ``asyncio.wait_for`` around ``pubsub.get_message``). A broker whose
   read outlives ``heartbeat_secs + _BROKER_READ_GRACE_SECS`` must RAISE
   — the browser's EventSource reconnects — never silently retry into
   another unbounded wait; the ``sse-redis-read-timeout`` warning names
   the incident; and the ``finally`` releases BOTH held resources, the
   Redis subscription and the SSE-limit slot (a released-on-raise
   contract: the slot is held for the LIFE of the stream, so a raise
   that leaked it would permanently burn the process's connection cap).

2. The malformed-envelope validators — the crossed-wire/proxy-lying
   defenses. The per-job channel is not exclusively owned: a lying proxy
   can deliver another job's envelope on it, a publisher drift can stop
   emitting the ``ProgressEvent`` shape, and a seq that passes the type
   check beyond the wire ceiling (``_MAX_PROGRESS_SEQ`` = 2^31-1 — a
   wire-hygiene bound on the cursor domain a hostile envelope may claim,
   not the storage domain: the durable ``progress_seq`` column is a
   bigint) would advance ``last_emitted_seq`` past every future event
   and starve the stream into a blackhole. Every malformed message must
   be DISCARDED with its counter bump (``taskq
   .sse.malformed_messages``) and the stream must SURVIVE — the next
   valid envelope still forwards.

All unit-level, against stub pubsubs and the generator directly (the
tests/web_progress harness idiom); no broker, no PG, no containers.
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")

import structlog.testing
from sse_starlette.event import ServerSentEvent

import taskq.web.progress as progress_mod
from taskq.constants import progress_channel
from taskq.progress._events import ProgressEvent
from taskq.web._sse_limit import acquire_sse_slot
from taskq.web.progress import (
    _event_generator,  # pyright: ignore[reportPrivateUsage]  # Why: unit tests import private symbols to exercise them directly.
)

pytestmark = [pytest.mark.fastapi]

_SCHEMA_LABEL = "taskq"
_JOB_ID = UUID("00000000-0000-0000-0000-000000000001")
_FOREIGN_JOB_ID = UUID("00000000-0000-0000-0000-000000000002")
_HEARTBEAT_SECS = 0.02
#: Shrunk for the gauntlet: the production constant (0.5) would make the
#: read-timeout pin take 0.5s+ per case for no additional discrimination.
_GRACE_SECS = 0.05


def _pg_row(*, status: str = "running", progress_seq: int = 0) -> dict[str, Any]:
    return {
        "status": status,
        "progress_seq": progress_seq,
        "progress_state": {"step": 1},
    }


def _make_event(*, seq: int, terminal: bool = False) -> ProgressEvent:
    return ProgressEvent(
        v=1,
        kind="progress" if not terminal else "state_change",
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
        "channel": progress_channel(_SCHEMA_LABEL, _JOB_ID).encode(),
        "data": event.model_dump_json(exclude_none=True).encode(),
    }


class _StubPubSub:
    """Minimal redis PubSub duck-type fed a script of get_message answers."""

    def __init__(self, messages: list[dict[str, Any] | None]) -> None:
        self._messages = list(messages)
        self._pos = 0
        self.subscribed: list[str | bytes] = []
        self.unsubscribed = False
        self.closed = False

    async def subscribe(self, channel: str | bytes) -> None:
        self.subscribed.append(channel)

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message's kwarg shape.
    ) -> dict[str, Any] | None:
        if self._pos < len(self._messages):
            item = self._messages[self._pos]
            self._pos += 1
            return item
        return None

    async def unsubscribe(self, channel: str | bytes) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


class _HangingReadPubSub(_StubPubSub):
    """A pubsub whose reads NEVER return: the broker-lying/hang shape the
    read-timeout arm exists for (a wedged socket read, a broker that
    accepted the subscribe and stopped answering)."""

    def __init__(self) -> None:
        super().__init__([])
        self._hang = asyncio.Event()  # never set

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109
    ) -> dict[str, Any] | None:
        await self._hang.wait()
        return None  # pragma: no cover - the Event never sets


async def _open_generator(
    pubsub: Any,
    *,
    slot: asyncio.Semaphore | None = None,
    heartbeat_secs: float = _HEARTBEAT_SECS,
) -> AsyncIterator[ServerSentEvent]:
    """Build ``_event_generator`` the way the route handler does after the
    PG snapshot: subscribed, slot acquired, non-terminal running row."""
    channel = progress_channel(_SCHEMA_LABEL, _JOB_ID)
    await pubsub.subscribe(channel)
    return _event_generator(
        pubsub=pubsub,
        channel=channel,
        job_id=_JOB_ID,
        is_terminal=False,
        progress_seq=0,
        progress_data=json.dumps(_pg_row()),
        resolved_last_event_id=None,
        heartbeat_secs=heartbeat_secs,
        sse_slot_semaphore=slot,
    )


# ── Gauntlet 1: the broker hang (read-timeout fail-visible) ──────────────


@pytest.mark.asyncio
async def test_hung_broker_read_raises_fail_visible_and_releases_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broker whose read hangs past ``heartbeat_secs +
    _BROKER_READ_GRACE_SECS``: the stream RAISES (fail-visible, the
    browser reconnects), the ``sse-redis-read-timeout`` warning names the
    incident, and the subscription + SSE slot are released — the cap's
    counter is back to 0, a later client can still stream.

    Regression caught: (a) dropping the wait_for (a silent unbounded
    wait) wedges the generator on the hung read forever, holding the
    subscription AND the slot — after enough wedged streams the route
    429s every new client forever; (b) catching the TimeoutError and
    continuing the loop would pin the same resources behind a retry
    loop that never terminates against a dead broker.
    """
    monkeypatch.setattr(progress_mod, "_BROKER_READ_GRACE_SECS", _GRACE_SECS)
    slot = asyncio.Semaphore(1)
    await slot.acquire()  # the route's acquire_sse_slot already took the slot

    pubsub = _HangingReadPubSub()
    gen = await _open_generator(pubsub, slot=slot)

    with (
        structlog.testing.capture_logs() as logs,
        pytest.raises(TimeoutError),
    ):
        async with asyncio.timeout(5.0):
            async for _sse in gen:
                pass  # the snapshot may deliver; the hung read must end the stream

    warnings = [e for e in logs if e.get("event") == "sse-redis-read-timeout"]
    assert len(warnings) == 1, f"expected the fail-visible warning, got {logs!r}"
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["job_id"] == str(_JOB_ID)
    assert warnings[0]["channel"] == progress_channel(_SCHEMA_LABEL, _JOB_ID)
    assert warnings[0]["read_bound_secs"] == _HEARTBEAT_SECS + _GRACE_SECS

    assert pubsub.unsubscribed, "the Redis subscription must be released on the raise"
    assert pubsub.closed
    assert slot.locked() is False, "the SSE slot must be released on the raise"
    # The cap is a live budget again, not a slot burned for process life.
    await acquire_sse_slot("progress-stream", 1)


# ── Gauntlet 2: the proxy-lying envelopes (malformed, discard + survive) ─


def _base_envelope(**overrides: object) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "v": 1,
        "kind": "progress",
        "job_id": str(_JOB_ID),
        "actor": "test_actor",
        "ts": "2026-01-01T00:00:00Z",
        "seq": 3,
        "status": "running",
        "terminal": False,
    }
    envelope.update(overrides)
    for key in [k for k, v in envelope.items() if v is _MISSING]:
        del envelope[key]
    return envelope


_MISSING = object()

#: One case per validator arm — the crossed-wire/proxy-lying defenses:
#: a seq that is not an integer, a seq beyond the wire ceiling
#: (``_MAX_PROGRESS_SEQ``), a foreign job's envelope delivered on this
#: job's channel, a terminal flag that is not a boolean, and an envelope
#: missing a required field.
_LYING_ENVELOPES = [
    ("seq_not_an_int", _base_envelope(seq="3")),
    ("seq_beyond_wire_ceiling", _base_envelope(seq=2**31)),
    ("foreign_job_envelope", _base_envelope(job_id=str(_FOREIGN_JOB_ID))),
    ("terminal_not_a_bool", _base_envelope(terminal=1)),
    ("missing_required_status", _base_envelope(status=_MISSING)),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("case", "envelope"), _LYING_ENVELOPES)
async def test_lying_envelope_is_discarded_with_its_counter_and_the_stream_survives(
    case: str,
    envelope: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each lying envelope: exactly one malformed-counter bump, the debug
    drop (never a raise), and the NEXT VALID envelope still forwards.

    Regression caught: any validator that (a) let the lie through — the
    foreign envelope would be forwarded onto this job's stream and its
    seq could gate this job's future events; the beyond-the-ceiling seq
    would advance the cursor past every future event, blackholing the
    stream — or (b) propagated the ValueError — a shared-channel writer
    (or a lying proxy) gets a kill switch on every client's stream. The
    forward-after assertion is the survival half: the guard discards,
    the stream does not.
    """
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    from taskq.testing.otel import counter_value

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    counter = provider.get_meter("taskq").create_counter("taskq.sse.malformed_messages", unit="1")
    monkeypatch.setattr(progress_mod, "_sse_malformed_counter", counter)

    good = _make_event(seq=5)
    pubsub = _StubPubSub(
        [
            {"type": "message", "data": json.dumps(envelope).encode()},
            _redis_msg(good),
            None,  # keepalive tick, then the driver's max_events ends the stream
        ]
    )

    gen = await _open_generator(pubsub)
    results: list[ServerSentEvent] = []
    async with contextlib.aclosing(gen):
        async with asyncio.timeout(5.0):
            async for sse in gen:
                results.append(sse)
                if len(results) >= 3:  # snapshot + the good event, then stop
                    break

    data_events = [r for r in results if r.data is not None]
    assert len(data_events) == 2, f"[{case}] stream did not survive the lie: {results!r}"
    forwarded = json.loads(data_events[-1].data)
    assert forwarded["seq"] == 5, f"[{case}] the next valid envelope must still forward"
    assert data_events[-1].event == "progress"

    assert counter_value(reader, "taskq.sse.malformed_messages") == 1, (
        f"[{case}] the lying envelope must bump the malformed counter exactly once"
    )


@pytest.mark.asyncio
async def test_lying_envelopes_never_advance_the_cursor() -> None:
    """A discarded envelope must not move ``last_emitted_seq``: the valid
    event behind a lying HIGH-seq envelope still forwards.

    Regression caught: the blackhole shape — a beyond-the-ceiling seq (or
    a foreign envelope's seq) that passed validation would advance the
    cursor past every future event of this job; the stream would then
    silently drop the job's remaining progress while appearing healthy.
    """
    high_lie = _base_envelope(seq=2**31)  # would win every cursor comparison, if accepted
    low_good = _make_event(seq=2)
    pubsub = _StubPubSub(
        [
            {"type": "message", "data": json.dumps(high_lie).encode()},
            _redis_msg(low_good),
            None,
        ]
    )

    gen = await _open_generator(pubsub)
    results: list[ServerSentEvent] = []
    async with contextlib.aclosing(gen):
        async with asyncio.timeout(5.0):
            async for sse in gen:
                results.append(sse)
                if len(results) >= 3:
                    break

    data_events = [r for r in results if r.data is not None]
    assert len(data_events) == 2
    assert json.loads(data_events[-1].data)["seq"] == 2, (
        "the seq-2 event must forward after the discarded seq-2**31 lie"
    )
