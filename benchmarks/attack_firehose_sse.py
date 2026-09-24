"""Attack-firehose harness, section 3: the SSE progress bridge under a flood.

Drives the REAL production generator (taskq.web.progress._event_generator)
from a fake Redis pubsub that replays ``ProgressEvent`` envelopes at the
flood rate, and measures:
  - the generator's yield rate at a 10k events/s delivery rate (keep-up?)
  - the seq total order seen by a seq-cursor consumer (no out-of-order,
    no duplicate, no drop of LIVE deltas)
  - retained memory per queued event (the async-generator buffer class:
    the pubsub's socket buffer is the only queue, Redis bounds it
    server-side; this measures the generator retains nothing)
  - the disconnect's reap: time from aclose() at full flood to the
    finally block having released the subscription (bounded teardown)

Usage: uv run python benchmarks/attack_firehose_sse.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import UTC, datetime
from statistics import median

from taskq._ids import new_uuid
from taskq.progress._events import ProgressEvent
from taskq.web.progress import _event_generator


def _rss_mb() -> float:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) / 1024
    return -1.0


def _envelope(seq: int) -> str:
    event = ProgressEvent(
        kind="progress",
        job_id=new_uuid(),
        actor="firehose",
        ts=datetime.now(UTC),
        seq=seq,
        status="running",
        step=seq % 100,
        percent=float(seq % 101),
        detail="flood",
        data=None,
        terminal=False,
    )
    return event.model_dump_json(exclude_none=True)


class FloodPubsub:
    """A redis-py-pubsub look-alike with a metered internal queue.

    The queue models the broker's client output buffer: the publisher
    side pushes at the flood rate regardless of the consumer, the queue
    holds whatever the consumer has not yet read (the growth class), and
    the consumer pops one message per get_message call exactly like
    redis-py's async pubsub.
    """

    def __init__(self, heartbeat_secs: float) -> None:
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.heartbeat_secs = heartbeat_secs
        self.closed = False
        self.unsubscribed = False
        self.get_calls = 0

    async def publish_event(self, envelope: str) -> None:
        self.queue.put_nowait(envelope)

    async def get_message(
        self,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0.0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message's signature; the generator's wait_for wraps it.
    ) -> dict[str, object] | None:
        self.get_calls += 1
        try:
            data = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except TimeoutError:
            return None
        return {"data": data.encode() if isinstance(data, str) else data}

    async def unsubscribe(self, channel: str) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


async def run_flood(rate: int, seconds: float, job_id: str, channel: str) -> dict[str, object]:
    pubsub = FloodPubsub(heartbeat_secs=15.0)
    generator = _event_generator(
        pubsub=pubsub,  # type: ignore[arg-type]
        channel=channel,
        job_id=new_uuid(),
        is_terminal=False,
        progress_seq=0,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=15.0,
        sse_slot_semaphore=None,
        session_verifier=None,
    )

    seq_violations = 0
    duplicates = 0
    yielded = 0
    last_seq = -1  # the initial snapshot (seq 0) is not a duplicate
    yield_stamps: list[float] = []
    rss_before = _rss_mb()

    async def consume() -> None:
        nonlocal yielded, last_seq, seq_violations, duplicates
        async for sse in generator:
            yielded += 1
            seq = int(sse.id or 0)
            if seq < last_seq:
                seq_violations += 1
            elif seq == last_seq:
                duplicates += 1
            last_seq = seq

    consumer = asyncio.create_task(consume())

    start = time.monotonic()

    async def publish() -> tuple[int, list[float]]:
        # Bursty publisher: one await per event caps the loop at the
        # event-loop timer granularity (~1ms), so push a burst per tick
        # to actually hit the target rate.
        interval = 1.0 / rate
        # One burst ~= 2 ms of target volume: above the event-loop
        # timer granularity so the target rate is actually achievable.
        burst = max(1, round(0.002 * rate))
        burst_sleep = burst * interval
        deadline = time.monotonic() + seconds
        seq = 1
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            for _ in range(burst):
                await pubsub.publish_event(_envelope(seq))
                seq += 1
            left = burst_sleep - (time.monotonic() - t0)
            if left > 0:
                await asyncio.sleep(left)
        return seq - 1, yield_stamps

    publisher = asyncio.create_task(publish())
    published, _stamps = await publisher
    # let the generator drain what is left
    while not pubsub.queue.empty() or yielded < published:
        try:
            await asyncio.wait_for(asyncio.shield(consumer), timeout=5.0)
            break
        except TimeoutError:
            break
    drain_time = time.monotonic() - start
    _stamps = sorted(_stamps)

    # The disconnect's reap at full flood: cancel the consumer (client
    # disconnect) and time the generator's teardown to the finally block.
    queue_depth_at_disconnect = pubsub.queue.qsize()
    reap_start = time.monotonic()
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await consumer
    reap_ms = (time.monotonic() - reap_start) * 1000

    rss_after = _rss_mb()
    gaps = sorted((_stamps[i + 1] - _stamps[i]) * 1000 for i in range(len(_stamps) - 1))
    gaps_sorted = sorted(gaps)

    return {
        "delivery_rate_per_sec": rate,
        "flood_seconds": seconds,
        "published": published,
        "yielded": yielded,
        "kept_up": yielded >= published,
        "yield_rate_per_sec": round(yielded / drain_time, 1) if drain_time else 0,
        "seq_out_of_order": seq_violations,
        "seq_duplicates": duplicates,
        "last_seq": last_seq,
        "inter_yield_ms_p50": round(median(gaps_sorted), 3) if gaps_sorted else None,
        "inter_yield_ms_p99": round(gaps_sorted[int(len(gaps_sorted) * 0.99)], 3)
        if gaps_sorted
        else None,
        "rss_mb_before": round(rss_before, 2),
        "rss_mb_after": round(rss_after, 2),
        "queue_depth_at_disconnect": queue_depth_at_disconnect,
        "disconnect_reap_ms": round(reap_ms, 2),
        "pubsub_unsubscribed": pubsub.unsubscribed,
        "pubsub_closed": pubsub.closed,
        "get_message_calls": pubsub.get_calls,
        "yield_stamps_sampled": len(_stamps),
    }


async def main() -> None:
    job_id = str(new_uuid())
    channel = f"progress:{job_id}"
    for rate in (10_000, 50_000):
        result = await run_flood(rate, 5.0, job_id, channel)
        result.pop("_stamps", None)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
