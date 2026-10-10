"""THE PROGRESS PUSH PINS (the consumer-face lane's CURE 3 — the
deploy-matrix's PARTIAL): the SSE progress stream was 1s POLLING — every
frame the poll's own round trip late. The cure rides the SAME
LISTEN/NOTIFY transport the HITL broadcast landed (the pattern proven:
the in-tx NOTIFY, the listener's backfill+tail, the zero-window, the
dedup):

* THE WRITE'S NOTIFY: the progress flush carries ONE ``pg_notify`` on
  the ``taskq_wf_progress`` channel — the POINTER only (schema, flow,
  node, seq — never the payload's content; the row is the truth); the
  auto projections (node start/terminal) knock the same channel;
* THE LISTENER (:class:`taskq.workflows.api._progress_listen.
  ProgressListener` — the HitlListener's sibling): ONE dedicated
  connection, LISTEN → BACKFILL → TAIL (the zero-window construction —
  an emission visible to the snapshot is announced by it, a missed
  event unrepresentable by construction); the per-connection dedup (a
  knock whose seq the snapshot already covered yields nothing); the
  FAN-OUT, never a partition (per-subscriber queues); the coalesce
  order (the overflow drops the OLDEST — observability degrades FIRST,
  the writer never blocks, correctness never);
* THE SSE FACE UPGRADED: the generator's ``listener=`` — the knock
  wakes the replay immediately (the push PRIMARY, sub-poll latency);
  the poll tick is the fallback BELT (a missed knock costs one poll
  interval, never correctness — the seq-cursor read stays the source of
  truth).

Red-first: the pins ran RED at the pre-cure head (no notify, no
listener, no ``listener=`` param); the reds are captured in
``.measurements/cons-cure3-reds.json``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.backend._protocol import JobId
from taskq.workflows._progress import ProgressEmitter, project_auto_event
from taskq.workflows._progress_read import progress_stream_generator
from tests._wf_fixtures import RedLog

pytestmark = pytest.mark.integration


@pytest.fixture
def cons3_redlog() -> RedLog:
    """The red sink for the consumer-face lane's CURE 3 pins."""
    log = RedLog("cons-cure3-reds.json")
    return log


class Ingest(BaseModel):
    doc_id: str


def _loads(raw: Any) -> Any:
    return json.loads(raw) if isinstance(raw, str) else raw


def _stale_knock_sql(schema: str) -> str:
    """The drill's raw knock statement (the channel is a module
    constant; only $1 binds)."""
    return "SELECT pg_notify('taskq_wf_progress', $1)"


# ── THE WRITE'S NOTIFY ───────────────────────────────────────────────────


async def test_the_flush_knocks_the_progress_channel(
    cons3_redlog: RedLog,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: Any,
    module_pg_schema: Any,
) -> None:
    """THE FLUSH'S NOTIFY: one emission → one cadence flush → ONE
    ``pg_notify`` on ``taskq_wf_progress`` — the payload the POINTER
    only (schema/flow/node/seq), never the pct's sole copy."""
    from taskq.workflows._progress import PROGRESS_NOTIFY_CHANNEL

    flow_id, node_id = new_job_id(), new_job_id()
    knocks: list[tuple[str, Any]] = []
    got = asyncio.Event()

    listener_conn = await asyncpg.connect(module_pg_schema.pg_dsn)

    def _on_notify(_c: object, _pid: int, channel: str, payload: str) -> None:
        knocks.append((channel, _loads(payload)))
        got.set()

    await listener_conn.add_listener(PROGRESS_NOTIFY_CHANNEL, _on_notify)

    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0.05)
    await emitter.emit(45, "half", {"step": 2})
    await emitter.aclose()

    try:
        await asyncio.wait_for(got.wait(), timeout=5.0)
    except TimeoutError:
        cons3_redlog.red(
            "cons3-flush-notify",
            "the progress flush carries no pg_notify — the SSE face's only "
            "freshness is the 1s poll (the deploy matrix's PARTIAL)",
            {"knocks": knocks},
        )
        pytest.fail("no knock within 5s — the push transport is absent (the red)")
    channel, payload = knocks[0]
    assert channel == PROGRESS_NOTIFY_CHANNEL
    assert payload.get("schema") == wf_schema, (
        "the schema rides the payload (the channels are global — P8)"
    )
    assert payload.get("flow_id") and payload.get("node_id") and "seq" in payload
    assert "pct" not in payload, (
        "the POINTER only — the row is the truth, never the payload's content"
    )
    await listener_conn.close()


async def test_the_auto_projection_knocks_the_same_channel(
    wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: Any, module_pg_schema: Any
) -> None:
    """THE AUTO PROJECTION'S NOTIFY: the node start/terminal projections
    knock the SAME channel (the ring's every event class rides one
    transport)."""
    from taskq.workflows._progress import PROGRESS_NOTIFY_CHANNEL

    flow_id, node_id = new_job_id(), new_job_id()
    knocks: list[tuple[str, Any]] = []
    got = asyncio.Event()

    listener_conn = await asyncpg.connect(module_pg_schema.pg_dsn)

    def _on_notify(_c: object, _pid: int, channel: str, payload: str) -> None:
        knocks.append((channel, _loads(payload)))
        got.set()

    await listener_conn.add_listener(PROGRESS_NOTIFY_CHANNEL, _on_notify)

    seq = await project_auto_event(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        node_id=node_id,
        kind="wf.node.terminal",
        payload={"outcome": "succeeded", "step_key": "ocr"},
    )
    assert seq >= 1
    try:
        await asyncio.wait_for(got.wait(), timeout=5.0)
    except TimeoutError:
        pytest.fail("the auto projection knocked nothing — the push transport is absent (the red)")
    assert knocks[0][1].get("seq") == seq
    await listener_conn.close()


# ── THE LISTENER ─────────────────────────────────────────────────────────


async def test_the_listener_backfills_the_zero_window_and_dedups(
    cons3_redlog: RedLog,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_conn: Any,
    wf_sql: Any,
    module_pg_schema: Any,
) -> None:
    """THE ZERO-WINDOW CONSTRUCTION (the #316 pattern, generalized): the
    emissions written BEFORE the listener starts are announced by the
    BACKFILL (source=backfill) — LISTEN first, THEN the snapshot; the
    backfill ends with ONE reconcile snapshot (the last_seqs the dedup
    reads). AND THE DEDUP: a knock whose seq the snapshot already
    covered yields NOTHING (one emission, ONE event)."""
    from taskq.workflows.api._progress_listen import (
        ProgressBackfilled,
        ProgressListener,
        ProgressUpdated,
    )
    from tests._wf_fixtures import seed_flow, seed_running_node

    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0.05)
    await emitter.emit(10, "early", None)
    await emitter.aclose()

    listener = ProgressListener(wf_pool, wf_schema, flow_id=flow_id)
    async with listener:
        events: list[Any] = []

        async def _drain_until_snapshot() -> None:
            async for event in listener.updates():
                events.append(event)
                if isinstance(event, ProgressBackfilled):
                    return

        await asyncio.wait_for(_drain_until_snapshot(), timeout=5.0)

        updates = [e for e in events if isinstance(e, ProgressUpdated)]
        if not updates or not any(e.source == "backfill" for e in updates):
            cons3_redlog.red(
                "cons3-zero-window-backfill",
                "the listener's backfill announced nothing for the pre-listen "
                "emissions — the zero-window construction is absent",
                {"kinds": [type(e).__name__ for e in events]},
            )
            pytest.fail("no backfill update — the zero-window construction is absent (the red)")
        early = updates[0]
        assert early.pct == 10 and early.source == "backfill"
        assert early.flow_id == str(flow_id) and early.node_id == str(node_id)
        snapshot = next(e for e in events if isinstance(e, ProgressBackfilled))
        last_seq = snapshot.last_seqs.get(str(node_id))
        assert last_seq is not None and last_seq >= 1

        # A LATE SUBSCRIBER replays the history first (the
        # buffer-then-consume behavior), then goes live — the drill's
        # collector runs until cancelled, so the dedup window and the
        # fresh knock both land in ONE stream.
        seen: list[Any] = []

        async def _collect_until_cancel() -> None:
            async for event in listener.updates():
                seen.append(event)

        collector = asyncio.create_task(_collect_until_cancel())
        await asyncio.sleep(0.3)  # the history replay lands
        # THE DEDUP: the covered seq's own knock (the LISTEN→snapshot
        # race's witness) yields NOTHING.
        await _raw_knock(
            module_pg_schema.pg_dsn,
            _stale_knock_payload(wf_schema, flow_id, node_id, int(last_seq)),
        )
        await asyncio.sleep(0.6)
        assert not any(isinstance(e, ProgressUpdated) and e.source == "notify" for e in seen), (
            "the backfill-covered knock re-announced — the dedup is broken"
        )

        # A FRESH knock (a seq past the snapshot) announces.
        await _raw_knock(
            module_pg_schema.pg_dsn,
            _stale_knock_payload(wf_schema, flow_id, node_id, int(last_seq) + 5000),
        )

        async def _wait_fresh() -> None:
            # THE DRILL'S OWN POLL CADENCE (bounded ticks — the wake
            # condition is the collector's list, not an event this test
            # owns).
            for _tick in range(100):
                if any(isinstance(e, ProgressUpdated) and e.source == "notify" for e in seen):
                    return
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_wait_fresh(), timeout=5.0)
        fresh_event = next(
            e for e in seen if isinstance(e, ProgressUpdated) and e.source == "notify"
        )
        assert fresh_event.last_seq == int(last_seq) + 5000
        collector.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await collector


def _stale_knock_payload(schema: str, flow_id: JobId, node_id: JobId, seq: int) -> str:
    """The drill's raw knock payload (the exact wire shape the write legs
    send — the pointer only)."""
    return json.dumps(
        {"schema": schema, "flow_id": str(flow_id), "node_id": str(node_id), "seq": seq}
    )


async def _raw_knock(dsn: str, payload: str) -> None:
    """A RAW pg_notify on a throwaway connection (the drill's knock —
    not the production write legs)."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(_stale_knock_sql("unused"), payload)
    finally:
        await conn.close()


async def test_the_listener_fans_out_and_degrades_first(
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_conn: Any,
    wf_sql: Any,
    module_pg_schema: Any,
) -> None:
    """THE FAN-OUT, NOT A PARTITION (P7's law on the push face): two
    subscribers each see EVERY event. THE COALESCE ORDER (the
    backpressure's law): the bounded queues drop the OLDEST — the writer
    never blocks, the newest survives, correctness never."""
    from taskq.workflows.api import _progress_listen
    from taskq.workflows.api._progress_listen import ProgressListener, ProgressUpdated
    from tests._wf_fixtures import seed_flow, seed_running_node

    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0.05)
    await emitter.emit(30, "fan", None)
    await emitter.aclose()

    listener = ProgressListener(wf_pool, wf_schema, flow_id=flow_id)
    async with listener:
        a: list[Any] = []
        b: list[Any] = []

        async def _sub(sink_list: list[Any]) -> None:
            async for event in listener.updates():
                sink_list.append(event)
                return

        await asyncio.gather(_sub(a), _sub(b))
    assert any(isinstance(e, ProgressUpdated) for e in a) and any(
        isinstance(e, ProgressUpdated) for e in b
    ), "a shared queue PARTITIONS the stream — each subscriber must see the event"

    # THE COALESCE ORDER: with the queues at the BOUND, the knock
    # producer NEVER BLOCKS and the OLDEST events are the casualties
    # (the newest survive — the row is the truth, the poll converges).
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(_progress_listen, "_QUEUE_MAXSIZE", 2)
        listener2 = ProgressListener(wf_pool, wf_schema, flow_id=flow_id)
        async with listener2:
            # TEN knocks against queues bound at 2, with the consumer
            # NOT yet draining — the producer must never await it.
            conn = await asyncpg.connect(module_pg_schema.pg_dsn)
            t0 = time.monotonic()
            for seq in range(10):
                await conn.execute(
                    _stale_knock_sql(wf_schema),
                    _stale_knock_payload(wf_schema, flow_id, node_id, 90000 + seq),
                )
            produced = time.monotonic() - t0
            await conn.close()
            assert produced < 5.0, "the producer blocked on the consumer — the law is broken"
            # THE SETTLE: the pump decodes the raw queue into the bounded
            # subscriber queues — the drop-oldest runs while nobody
            # drains; the HEAD of the knock sequence is the casualty.
            await asyncio.sleep(0.5)
            seen: list[Any] = []

            async def _collect_two() -> None:
                async for event in listener2.updates():
                    seen.append(event)
                    if len(seen) >= 2:
                        return

            await asyncio.wait_for(_collect_two(), timeout=5.0)
            # THE ORDER: the delivered events are the knock sequence's
            # TAIL (the head was the drop-oldest's casualty) — the
            # newest survive.
            delivered = [e.last_seq for e in seen if isinstance(e, ProgressUpdated)]
            assert delivered and delivered[-1] >= 90005, (
                f"the delivered seqs {delivered} are not the knock tail — "
                "the drop-oldest order is broken"
            )
            assert delivered == sorted(delivered), "the seq order is the coalesce's own law"


async def test_the_listener_isolates_another_runs_events(
    wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: Any, module_pg_schema: Any
) -> None:
    """ONE RUN'S LISTENER: a listener scoped to flow A drops flow B's
    knocks (the run-scoped backfill is bounded; the filter is the
    listener's own)."""
    from taskq.workflows.api._progress_listen import ProgressListener, ProgressUpdated

    flow_a, flow_b, node_b = new_job_id(), new_job_id(), new_job_id()
    emitter_b = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_b, node_id=node_b, cadence_s=0.05)
    await emitter_b.emit(50, "other run", None)
    await emitter_b.aclose()

    listener = ProgressListener(wf_pool, wf_schema, flow_id=flow_a)
    async with listener:
        seen: list[Any] = []

        async def _collect() -> None:
            async for event in listener.updates():
                seen.append(event)

        task = asyncio.create_task(_collect())
        # Flow B's knock rides the global channel while A listens.
        await _raw_knock(
            module_pg_schema.pg_dsn, _stale_knock_payload(wf_schema, flow_b, node_b, 700)
        )
        await asyncio.sleep(0.5)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert not any(isinstance(e, ProgressUpdated) and e.flow_id == str(flow_b) for e in seen), (
        "another run's knock reached this run's listener"
    )


# ── THE SSE FACE UPGRADED ────────────────────────────────────────────────


async def test_the_sse_push_delivers_sub_poll_latency(
    cons3_redlog: RedLog, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: Any
) -> None:
    """THE PUSH PRIMARY: with a listener wired, an emission committed
    AFTER the stream started delivers its ``progress`` frame well under
    the poll interval (the poll's round trip is gone). The cursor
    advances; the frames stay the seq-cursor read's own (the read is the
    truth, the push buys latency)."""
    from taskq.workflows.api._progress_listen import ProgressListener

    flow_id, node_id = new_job_id(), new_job_id()
    listener = ProgressListener(wf_pool, wf_schema, flow_id=flow_id)
    await listener.start()
    stop = asyncio.Event()
    try:

        async def _stream() -> list[dict[str, str]]:
            frames: list[dict[str, str]] = []
            gen = progress_stream_generator(
                wf_pool,
                wf_sql,
                flow_id=flow_id,
                last_event_id=0,
                poll_s=30.0,
                stop=stop,
                listener=listener,
            )
            async for frame in gen:
                frames.append(frame)
                if frame["event"] == "progress":
                    break
            return frames

        task = asyncio.create_task(_stream())
        await asyncio.sleep(0.3)  # the display frame's connect shape lands
        t0 = time.monotonic()
        emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0.05)
        await emitter.emit(77, "pushed", None)
        await emitter.aclose()
        try:
            frames = await asyncio.wait_for(task, timeout=10.0)
        except TimeoutError:
            cons3_redlog.red(
                "cons3-sse-push",
                "the SSE generator ignores the listener — the poll_s=30 stream "
                "never delivered the pushed emission (the push transport's "
                "absence)",
                {},
            )
            pytest.fail("the push never delivered — the generator's poll is the only leg (the red)")
        delivered = time.monotonic() - t0
        progress_frames = [f for f in frames if f["event"] == "progress"]
        assert progress_frames, "the emission's frame never rode the stream"
        assert delivered < 25.0, f"delivered in {delivered:.2f}s — the poll interval, not the push"
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await listener.stop()


async def test_the_poll_belt_still_carries_the_frames(
    wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: Any
) -> None:
    """THE FALLBACK BELT: a SILENT listener (the knock never arrives —
    the PgBouncer landmine's shape) degrades to the poll cadence, never
    to silence: the emission's frame still arrives within one poll
    interval. The push is the primary; the belt is the guarantee."""
    from unittest.mock import patch

    from taskq.workflows.api._progress_listen import ProgressListener

    flow_id, node_id = new_job_id(), new_job_id()
    listener = ProgressListener(wf_pool, wf_schema, flow_id=flow_id)
    await listener.start()
    stop = asyncio.Event()
    try:

        async def _stream() -> list[dict[str, str]]:
            frames: list[dict[str, str]] = []
            gen = progress_stream_generator(
                wf_pool,
                wf_sql,
                flow_id=flow_id,
                last_event_id=0,
                poll_s=0.5,
                stop=stop,
                listener=listener,
            )
            async for frame in gen:
                frames.append(frame)
                if frame["event"] == "progress":
                    break
            return frames

        task = asyncio.create_task(_stream())
        await asyncio.sleep(0.2)
        emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0.05)
        await emitter.emit(88, "belts", None)
        await emitter.aclose()

        async def _silent_wait(_timeout: float) -> bool:
            return False  # the knock NEVER arrives — the PgBouncer landmine

        with patch.object(ProgressListener, "wait", _silent_wait):
            frames = await asyncio.wait_for(task, timeout=10.0)
        assert any(f["event"] == "progress" for f in frames), "the belt must carry the frames"
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await listener.stop()
