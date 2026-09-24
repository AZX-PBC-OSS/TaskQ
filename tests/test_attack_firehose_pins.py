"""Attack-firehose pins: the event pipeline's bounds under a 10k/s flood.

The flood campaign measured five surfaces under a 10k events/second
retry-storm hypothesis (benchmarks/attack_firehose_*.py, numbers in the
branch's report). These pins hold the bounds the flood found holding, at
the flood's own magnitude, so a regression cannot re-open them quietly:

  1. the coalesced flush's statement bound: 10k dirty buffers coalesce
     into statements of at most ``_FLUSH_BATCH_ROWS`` rows and at most
     ``_FLUSH_MAX_BATCHES_PER_TICK`` statements per tick - never one
     monster unnest over the whole dirty set (the 452-class unbounded
     write);
  2. the SSE bridge's seq total order under a flood: every live event
     reaches the consumer exactly once, in order, nothing dropped, and
     the disconnect's teardown releases the subscription and closes the
     pubsub bounded, with a deep in-flight backlog;
  3. the notify wake callback's O(1) contract at flood magnitude: one
     callback per notify, every subscriber woken, queue-filtered
     subscribers stay asleep, and the callback retains nothing per
     notification (the listener's memory must not track the flood).
"""

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._events import ProgressEvent
from taskq.progress._flush import (
    _FLUSH_BATCH_ROWS,
    _FLUSH_MAX_BATCHES_PER_TICK,
    _flush_dirty_set,
)
from tests.test_progress_flush import _WORKER_ID

# Why: only the SSE pins need the fastapi/sse-starlette extra; the flush
# and notify-wake pins run on every extras leg. The web module imports
# fastapi at its own module level, so the import (and its guard) lives
# inside the SSE pins rather than at module scope - a module-level guard
# here would skip the flush pin on the legs that lack fastapi (aws,
# vault, ...), and a module-level import would fail collection on them.

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000f01")
_FLOOD_DIRTY = 10_000


# ── Pin 1: the coalesced flush's statement bound at flood magnitude ──


async def test_pin_flush_flood_10k_dirty_set_statement_bounds() -> None:
    """10k dirty buffers coalesce into a bounded tick, never a monster.

    The flood shape: a retry storm re-dirties every buffer between two
    ticks. Whatever the dirty set's size, each statement the tick issues
    carries at most ``_FLUSH_BATCH_ROWS`` rows and the tick issues at
    most ``_FLUSH_MAX_BATCHES_PER_TICK`` statements - the remainder
    stays dirty and drains on the next tick. An all-in-one unnest over
    10k rows would be the long-running-statement trap the doctrine
    forbids (it times out as a whole and stalls the loop).
    """
    statements: list[list[UUID]] = []

    class _Conn:
        async def fetch(self, *args: object) -> list[dict[str, object]]:
            job_ids = args[1] if len(args) > 1 else None
            assert isinstance(job_ids, list), "the flush must bind the unnest id array"
            statements.append(cast("list[UUID]", job_ids))
            return [{"id": jid, "progress_seq": 9} for jid in job_ids]

    conn = _Conn()

    class _Pool:
        @asynccontextmanager
        async def acquire(self) -> AsyncGenerator[_Conn, None]:
            yield conn

    buffers: dict[UUID, _ProgressBuffer] = {}
    for _ in range(_FLOOD_DIRTY):
        job_id = new_uuid()
        buf = _ProgressBuffer(job_id=job_id, base_seq=0, attempt=1)
        buf.pending_seq_delta = 1
        buf.pending_state["step"] = 1
        buf.dirty = True
        buffers[job_id] = buf

    # The 10k-dirty set coalesces into ONE tick's dirty snapshot.
    await _flush_dirty_set(
        _Pool(),
        "taskq_test",
        _WORKER_ID,
        buffers,
        [(jid, b) for jid, b in buffers.items() if b.dirty],
    )

    assert len(statements) == _FLUSH_MAX_BATCHES_PER_TICK, (
        f"the flood tick must issue exactly {_FLUSH_MAX_BATCHES_PER_TICK} bounded "
        f"statements; issued {len(statements)}"
    )
    for i, stmt_ids in enumerate(statements):
        assert len(stmt_ids) <= _FLUSH_BATCH_ROWS, (
            f"statement {i} carried {len(stmt_ids)} rows - above the "
            f"{_FLUSH_BATCH_ROWS}-row bound, the monster-statement shape the "
            "bounded-batch doctrine forbids"
        )
    flushed = sum(len(s) for s in statements)
    assert flushed == _FLUSH_MAX_BATCHES_PER_TICK * _FLUSH_BATCH_ROWS
    still_dirty = sum(1 for b in buffers.values() if b.dirty)
    assert still_dirty == _FLOOD_DIRTY - flushed, (
        f"the flood's remainder must stay dirty for the next tick; "
        f"{still_dirty} dirty vs {_FLOOD_DIRTY - flushed} expected"
    )
    # And the tick drains the remainder on the NEXT tick: run it again.
    statements.clear()
    await _flush_dirty_set(
        _Pool(),
        "taskq_test",
        _WORKER_ID,
        buffers,
        [(jid, b) for jid, b in buffers.items() if b.dirty],
    )
    assert sum(1 for b in buffers.values() if b.dirty) == _FLOOD_DIRTY - 2 * flushed


# ── Pin 2: the SSE bridge's seq total order and teardown under flood ──


class _FloodPubSub:
    """redis-py pubsub duck-type fed a pre-queued flood backlog."""

    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self._messages = list(messages)
        self._pos = 0
        self.unsubscribed = False
        self.closed = False
        self.get_calls = 0

    async def subscribe(self, channel: str) -> None:
        return

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109
    ) -> dict[str, Any] | None:
        self.get_calls += 1
        if self._pos < len(self._messages):
            msg = self._messages[self._pos]
            self._pos += 1
            return msg
        return None

    async def unsubscribe(self, channel: str) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


def _flood_event(seq: int, *, terminal: bool = False) -> dict[str, Any]:
    event = ProgressEvent(
        v=1,
        kind="progress" if not terminal else "state_change",
        job_id=_JOB_ID,
        actor="firehose",
        ts=datetime.now(UTC),
        seq=seq,
        status="running" if not terminal else "succeeded",
        step=seq % 100,
        percent=float(seq % 101),
        terminal=terminal,
    )
    return {
        "type": "message",
        "channel": b"ch",
        "data": event.model_dump_json(exclude_none=True).encode(),
    }


async def test_pin_sse_seq_total_order_under_10k_flood() -> None:
    """A 10k-event flood reaches the consumer exactly once, in order.

    The client's seq-cursor discipline discards anything at or below its
    cursor, so ONE out-of-order yield drops a LIVE delta (the client
    discards the re-sent higher seq later? no - it discards the STALE
    re-ordering) and one duplicate hides a fresh tick. The generator's
    single-connection read loop is the total order's guarantee: one
    message per iteration, strictly increasing yield order.
    """
    n = 10_000
    events = [_flood_event(seq) for seq in range(1, n + 1)]
    # Replay shapes the broker can produce under a storm (a pubsub
    # reconnect re-delivers, a dual-publish race double-fires): a stale
    # seq AFTER newer ones and an exact duplicate. The cursor gate must
    # drop both - one stale yield rendered is a client regression, one
    # duplicate hides nothing but breaks the exactly-once order.
    for replay_at, replay_seq in ((2_500, 2_000), (5_000, 2_000), (7_500, 7_499)):
        events.insert(replay_at, _flood_event(replay_seq))
    # The storm's tail: the terminal state-change consumes seq n+1 (one
    # past the head) and closes the stream.
    events.append(_flood_event(n + 1, terminal=True))
    pubsub = _FloodPubSub(events)
    seen_seqs: list[int] = []

    pytest.importorskip("fastapi")
    from taskq.web.progress import _event_generator

    gen = _event_generator(
        pubsub=cast("Any", pubsub),
        channel="ch",
        job_id=_JOB_ID,
        is_terminal=False,
        progress_seq=0,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=15.0,
    )
    async for sse in gen:
        seen_seqs.append(int(sse.id or -1) if sse.data else -1)

    # The initial snapshot (seq 0), then EVERY flood event 1..n in order
    # (every replay dropped by the cursor gate), then the terminal's
    # seq n+1; the trailing done event carries no id.
    assert seen_seqs == [*range(0, n + 2), -1], (
        "the flood must yield the snapshot then every event exactly once, "
        f"strictly increasing, replays dropped, then done; got "
        f"{len(seen_seqs)} yields, first 5 {seen_seqs[:5]}, last 5 "
        f"{seen_seqs[-5:]}"
    )


async def test_pin_sse_disconnect_reap_with_deep_inflight_backlog() -> None:
    """Disconnect mid-flood reaps the subscription bounded, at depth.

    The teardown runs against a pubsub still holding a 50k-event backlog:
    the finally unsubscribes and closes, and the reap cost does not scale
    with the backlog (a drain-on-close would be the unbounded teardown
    the close contract forbids).
    """
    pubsub = _FloodPubSub([_flood_event(seq) for seq in range(1, 50_001)])
    pytest.importorskip("fastapi")
    from taskq.web.progress import _event_generator

    gen = _event_generator(
        pubsub=cast("Any", pubsub),
        channel="ch",
        job_id=_JOB_ID,
        is_terminal=False,
        progress_seq=0,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=15.0,
    )
    # Enter the body: consume the snapshot and one live event so the
    # generator is parked inside the streaming loop, backlog queued.
    first = await gen.__anext__()
    assert int(first.id or -1) == 0
    second = await gen.__anext__()
    assert int(second.id or -1) == 1

    t0 = time.perf_counter()
    await gen.aclose()
    reap_ms = (time.perf_counter() - t0) * 1000

    assert pubsub.unsubscribed, "the disconnect must release the subscription"
    assert pubsub.closed, "the disconnect must close the pubsub"
    assert reap_ms < 100, (
        f"the teardown took {reap_ms:.1f}ms with a deep in-flight backlog - "
        "a reap that drains the backlog scales with the flood; the close "
        "must be bounded, not drained"
    )


# ── Pin 3: the notify wake callback at flood magnitude ───────────────


async def test_pin_notify_wake_callback_flood_o1_retention() -> None:
    """The wake callback wakes every subscriber and retains nothing.

    At one notify per re-pend the callback runs once per flood event.
    Two contracts at flood magnitude:

    - the EMPTY payload (the wake-everything shape the COPY fixup and
      rolling-deploy triggers send) wakes EVERY subscriber, the
      queue-filtered ones included - a dropped arm is the lost-wake
      dispatch stall;
    - a queue-filtered subscriber stays asleep on a foreign queue's
      payload - a 50-worker fleet must not answer every insert with a
      full claim round.

    And the callback retains nothing per notify: the listener's RSS
    tracking the flood would be the OOM class the campaign measured
    flat.
    """
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.notify import _make_callback

    backend = PostgresBackend.__new__(PostgresBackend)
    wake_all: list[asyncio.Event] = [asyncio.Event() for _ in range(32)]
    mine_only: list[asyncio.Event] = [asyncio.Event() for _ in range(32)]
    backend._wake_subscribers = {*wake_all, *mine_only}  # pyright: ignore[reportAttributeAccessIssue]
    backend._wake_queues = {  # pyright: ignore[reportAttributeAccessIssue]
        **dict.fromkeys(wake_all),
        **{event: {"mine"} for event in mine_only},
    }

    callback = _make_callback(backend)
    # The flood: one notify per re-pend, empty payload (the wake channel
    # carries '' by contract) alternating with a foreign queue's name.
    for _ in range(10_000):
        callback(None, 0, "wake", "")
        callback(None, 0, "wake", "foreign")

    assert all(event.is_set() for event in wake_all), (
        "the wake-everything subscribers must wake under the flood"
    )
    assert all(event.is_set() for event in mine_only), (
        "the empty payload must wake EVERY subscriber, the queue-filtered "
        "ones included - a dropped wake-all arm loses dispatches during a "
        "rolling deploy"
    )
    # Retention: the callback's own registry did not grow with the flood.
    assert len(backend._wake_subscribers) == 64  # pyright: ignore[reportAttributeAccessIssue]
    assert len(backend._wake_queues) == 64  # pyright: ignore[reportAttributeAccessIssue]


async def test_pin_notify_wake_callback_queue_filter_stays_asleep() -> None:
    """A queue-filtered subscriber must NOT wake on a foreign payload.

    The flood's other half: the filter exists so a 50-worker fleet stops
    answering every insert with a full claim round. A foreign queue's
    notify leaves the subscriber's event unset - and the subscriber must
    still wake the moment its OWN queue's payload lands.
    """
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.notify import _make_callback

    backend = PostgresBackend.__new__(PostgresBackend)
    mine_only: list[asyncio.Event] = [asyncio.Event() for _ in range(32)]
    backend._wake_subscribers = set(mine_only)  # pyright: ignore[reportAttributeAccessIssue]
    backend._wake_queues = {  # pyright: ignore[reportAttributeAccessIssue]
        event: {"mine"} for event in mine_only
    }

    callback = _make_callback(backend)
    for _ in range(10_000):
        callback(None, 0, "wake", "foreign")

    assert not any(event.is_set() for event in mine_only), (
        "a foreign queue's flood must not wake this worker's subscribers - "
        "the filter is the fleet's wake amplification bound"
    )

    for event in mine_only:
        event.clear()
    callback(None, 0, "wake", "mine")
    assert all(event.is_set() for event in mine_only), (
        "the subscriber's OWN queue's payload must wake it"
    )


async def test_pin_notify_wake_callback_never_raises_on_junk_payload() -> None:
    """A flood of junk payloads cannot kill the listener loop.

    asyncpg's listener callback runs inline in the protocol reader: an
    exception out of the callback poisons the connection. The wake
    callback is exercised with every payload shape the flood can carry.
    """
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.notify import _make_callback

    backend = PostgresBackend.__new__(PostgresBackend)
    woken = asyncio.Event()
    backend._wake_subscribers = {woken}  # pyright: ignore[reportAttributeAccessIssue]

    callback = _make_callback(backend)
    for payload in ("", "foreign-queue", "{}", "\x00binary-ish", "x" * 8000):
        callback(None, 0, "wake", payload)  # must not raise

    assert woken.is_set()
