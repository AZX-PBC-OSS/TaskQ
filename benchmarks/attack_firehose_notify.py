"""Attack-firehose harness, section 1: the LISTEN/NOTIFY path under a flood.

Drives the worker's real listener shape (worker/notify.py `_make_callback`
slimmed to its per-notify work: counter + subscriber wake) against a real
Postgres on a dedicated connection, while N sender connections fire
``pg_notify`` at a combined target rate for a fixed window.

Senders batch (``SELECT pg_notify($1, v) FROM unnest($2::text[])``) so the
combined send rate is not bounded by one round trip per notify - the
production shape is one notify folded into each enqueue/re-pend write.

Measured per run:
  - delivered notify rate (callbacks/sec) vs the send rate (keep-up?)
  - send-to-callback latency p50/p99 (the wake's freshness)
  - sender-side send failures (pg_notify queue pressure)
  - listener-process RSS over the window (the OOM class)
  - pg_notification_queue_usage() sampled through the window

Usage: FIREHOSE_DSN=... uv run python benchmarks/attack_firehose_notify.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections import Counter

import asyncpg

from taskq._ids import new_uuid

DSN = os.environ.get("FIREHOSE_DSN", "postgresql://taskq:taskq@127.0.0.1:55433/taskq")

CHANNEL = f"firehose_wake_{new_uuid().hex[:8]}"


def _rss_mb() -> float:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) / 1024
    return -1.0


class _LatencyAgg:
    """Bounded reservoir so the harness never retains one object per notify."""

    __slots__ = ("max", "n", "p50", "p99")

    def __init__(self) -> None:
        self.n = 0
        self.p50 = 0.0
        self.p99 = 0.0
        self.max = 0.0

    def add(self, lat: float) -> None:
        # Reservoir of 1: an online quantile estimate, no per-notify retention.
        self.n += 1
        if lat > self.max:
            self.max = lat
        if self.n == 1 or (self.n % 97 == 0):
            self.p50 = lat
            self.p99 = lat
        elif lat > self.p99:
            self.p99 = lat
        elif lat > self.p50 and self.n % 2 == 0:
            self.p50 = lat


async def sender(
    dsn: str,
    rate: float,
    seconds: float,
    batch: int,
    sent: list[int],
    failures: list[str],
) -> None:
    conn = await asyncpg.connect(dsn)
    interval = batch / rate  # seconds per batch
    deadline = time.monotonic() + seconds
    seq = 0
    stamps = [""] * batch
    while time.monotonic() < deadline:
        t0 = time.monotonic()
        now = time.monotonic()
        # Every notify needs a UNIQUE payload: PG collapses identical
        # (channel, payload) pairs within one transaction down to one
        # delivery, so a batch of identical stamps would deliver once.
        for i in range(batch):
            seq += 1
            stamps[i] = f"{now:.6f}:{seq}"
        try:
            await conn.execute(
                "SELECT pg_notify($1, v) FROM unnest($2::text[]) AS v",
                CHANNEL,
                stamps,
            )
        except Exception as exc:
            failures.append(repr(exc))
            await asyncio.sleep(0.01)
        sent[0] += batch
        left = interval - (time.monotonic() - t0)
        if left > 0:
            await asyncio.sleep(left)
    await conn.close()


async def queue_usage_sampler(dsn: str, samples: list[float], stop: asyncio.Event) -> None:
    conn = await asyncpg.connect(dsn)
    while not stop.is_set():
        usage = await conn.fetchval("SELECT pg_notification_queue_usage()")
        samples.append(float(usage))
        await asyncio.sleep(0.5)
    await conn.close()


async def _rss_sampler(samples: list[float], stop: asyncio.Event) -> None:
    while not stop.is_set():
        samples.append(_rss_mb())
        await asyncio.sleep(1.0)


def _buckets(delivered_counts: Counter[int], start: float, elapsed: float) -> list[int]:
    """Callbacks per one-second bucket over the run (keep-up profile)."""
    n = int(elapsed) + 1
    out = [0] * n
    for sec, count in delivered_counts.items():
        out[min(int(sec - start), n - 1)] += count
    return out


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=10_000, help="combined notify/sec")
    parser.add_argument("--senders", type=int, default=8)
    parser.add_argument("--batch", type=int, default=64, help="notifies per sender round trip")
    parser.add_argument("--seconds", type=float, default=20.0)
    args = parser.parse_args()

    delivered_counts: Counter[int] = Counter()
    lat = _LatencyAgg()
    rss_samples: list[float] = []

    def on_notify(conn: object, pid: int, channel: str, payload: str) -> None:
        # Retains NOTHING per notify: one Counter increment plus the
        # bounded reservoir. The listener's own memory under flood is the
        # thing under measurement; harness retention would pollute RSS.
        lat.add(time.monotonic() - float(payload.split(":")[0]))
        delivered_counts[int(time.monotonic())] += 1

    listener = await asyncpg.connect(DSN)
    await listener.add_listener(CHANNEL, on_notify)

    stop = asyncio.Event()
    usage: list[float] = []
    sampler = asyncio.create_task(queue_usage_sampler(DSN, usage, stop))
    rss_task = asyncio.create_task(_rss_sampler(rss_samples, stop))

    start = time.monotonic()
    rss_start = _rss_mb()
    await asyncio.sleep(2.0)
    rss_warm = _rss_mb()
    delivered_counts.clear()

    per_sender = args.rate / args.senders
    sent_count = [0]
    failures: list[str] = []
    tasks = [
        asyncio.create_task(sender(DSN, per_sender, args.seconds, args.batch, sent_count, failures))
        for _ in range(args.senders)
    ]
    await asyncio.gather(*tasks)
    send_window = args.seconds
    await asyncio.sleep(2.0)
    elapsed = time.monotonic() - start
    stop.set()
    rss_end = _rss_mb()

    n = sum(delivered_counts.values())
    delivered_rate = n / elapsed
    steady_secs = [s for s in delivered_counts if start + 1.0 < s < start + elapsed - 1.0]
    steady_rate = (
        sum(delivered_counts[s] for s in steady_secs) / max(len(steady_secs), 1)
        if steady_secs
        else 0.0
    )

    result = {
        "target_rate_per_sec": args.rate,
        "senders": args.senders,
        "batch_per_round_trip": args.batch,
        "send_window_s": send_window,
        "sent": sent_count[0],
        "delivered_callbacks": n,
        "delivered_rate_per_sec": round(delivered_rate, 1),
        "steady_delivered_rate_per_sec": round(max(steady_rate, 0), 1),
        "latency_ms_p50_est": round(lat.p50 * 1000, 3),
        "latency_ms_p99_est": round(lat.p99 * 1000, 3),
        "latency_max_ms": round(lat.max * 1000, 3),
        "send_failures": len(failures),
        "first_send_failure": failures[0] if failures else None,
        "rss_mb_start": round(rss_start, 2),
        "rss_mb_warm": round(rss_warm, 2),
        "rss_mb_end": round(rss_end, 2),
        "rss_peak_mb": round(max(rss_samples), 2) if rss_samples else None,
        "rss_growth_mb": round(rss_end - rss_warm, 2),
        "notify_queue_usage_max": round(max(usage), 5) if usage else None,
        "listener_time_in_loop_s": round(elapsed, 2),
        "delivered_per_second_buckets": _buckets(delivered_counts, start, elapsed),
    }
    print(json.dumps(result, indent=2))

    sampler.cancel()
    rss_task.cancel()
    await asyncio.gather(sampler, rss_task, return_exceptions=True)
    await listener.close()


if __name__ == "__main__":
    asyncio.run(main())
