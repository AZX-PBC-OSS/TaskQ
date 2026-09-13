"""Reconnect-storm simulation (Hunt 5) — worker/notify.py:357-392 backoff shape.

Models the notify health-check reconnect loop verbatim:
  - detection on the per-worker health-check tick (notify_health_check_interval)
  - first reconnect attempt IMMEDIATELY after detection (no delay)
  - on failure: sleep(delay); delay = min(delay * 2, 30.0)   <-- NO jitter

Three scenarios over a 100-worker fleet, PG down 47s then up:
  a) aligned phases  (fleet deployed together — the common k8s case)
  b) random phases   (workers started at random offsets within the interval)
  c) random phases + +-25% multiplicative jitter (the candidate fix)

Metric: connection attempts per 100ms bucket (burst size) and the max
simultaneous in-flight connects (each attempt = 1 TCP+auth + 3 LISTEN
round-trips, modeled as 150ms of server work).
"""

from __future__ import annotations

import asyncio
import random
from collections import Counter

W = 100
INTERVAL = 5.0
INITIAL = 1.0
CAP = 30.0
PG_DOWN_FOR = 47.0
ATTEMPT_COST = 0.15  # seconds of PG server work per connect attempt
SIM_HORIZON = 60.0


async def worker(
    rng: random.Random,
    phase: float,
    jitter: bool,
    bursts: Counter[float],
    inflight_hist: Counter[int],
    success_at: float,
) -> None:
    clock = 0.0
    # health check loop (first tick offset by the worker's phase)
    while clock < SIM_HORIZON:
        await asyncio.sleep(INTERVAL - phase)
        clock += INTERVAL - phase
        if clock >= success_at:
            return
        # connection error detected -> reconnect retry loop (notify.py:357-392)
        delay = INITIAL
        attempt = 0
        while True:
            # immediate first attempt
            start = clock
            bursts[round(start, 1)] += 1
            # model server work: attempts fail while PG is down, succeed after
            await asyncio.sleep(ATTEMPT_COST)
            clock += ATTEMPT_COST
            if clock >= success_at:
                inflight_hist[1] += 1
                return
            inflight_hist[1] += 1  # each attempt lands on the (down) server
            await asyncio.sleep(delay)
            clock += delay
            delay = min(delay * 2, 30.0)
            if jitter:
                delay *= 0.75 + rng.random() * 0.5
            attempt += 1


async def fleet(rng: random.Random, aligned: bool, jitter: bool, label: str) -> str:
    bursts: Counter[float] = Counter()
    inflight_hist: Counter[int] = Counter()
    success_at = PG_DOWN_FOR
    tasks = []
    for _ in range(W):
        phase = 0.0 if aligned else rng.uniform(0, INTERVAL)
        tasks.append(
            asyncio.create_task(worker(rng, phase, jitter, bursts, inflight_hist, success_at))
        )
    await asyncio.gather(*tasks)

    per_sec = Counter()
    for ts, n in bursts.items():
        per_sec[int(ts)] += n
    hist = " ".join(f"{s}:{per_sec[s]}" for s in sorted(per_sec))
    top = bursts.most_common(1)[0]
    return (
        f"{label}\n"
        f"    total attempts: {sum(bursts.values())} over the {PG_DOWN_FOR:.0f}s down-window\n"
        f"    attempts per second: {hist}\n"
        f"    biggest 100ms bucket: {top[1]} simultaneous connect attempts "
        f"(@t={top[0]}s) -> {top[1] * ATTEMPT_COST:.2f}s of serialized server work "
        f"if the server handles them one-at-a-time"
    )


def say(line: str = "") -> None:
    print(line)


async def main() -> None:
    say("=" * 78)
    say("RECONNECT STORM — notify.py:357-392 backoff (initial=1.0s, x2, cap 30s, NO jitter)")
    say(f"fleet={W} workers, health_check_interval={INTERVAL}s, PG down {PG_DOWN_FOR}s,")
    say(f"each attempt = 1 connect + 3 LISTEN round-trips ~{ATTEMPT_COST}s of server work")
    say("=" * 78)
    rng_a = random.Random(1)  # noqa: S311  # Why: seeded deterministic simulation, not cryptographic
    rng_b = random.Random(2)  # noqa: S311  # Why: seeded deterministic simulation, not cryptographic
    rng_c = random.Random(3)  # noqa: S311  # Why: seeded deterministic simulation, not cryptographic
    a, b, c = await asyncio.gather(
        fleet(rng_a, aligned=True, jitter=False, label="a) ALIGNED phases (deploy cohort)"),
        fleet(rng_b, aligned=False, jitter=False, label="b) RANDOM phases (no jitter)"),
        fleet(
            rng_c,
            aligned=False,
            jitter=True,
            label="c) RANDOM phases + +-25% jitter (candidate fix)",
        ),
    )
    for r in (a, b, c):
        say(r)
        say()


if __name__ == "__main__":
    asyncio.run(main())
