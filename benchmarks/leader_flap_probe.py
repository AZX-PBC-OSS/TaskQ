"""Leader-election probe: advisory-lock behaviour under leader flap.

Quantifies the election mechanics worker/leader.py relies on:

  1. Lock level: pg_try_advisory_lock is SESSION-scoped — verified by
     holding the lock across a committed transaction and confirming a
     second connection still cannot acquire it.
  2. Clean flap: leader connection closes (SIGTERM, socket FIN) → how
     fast can a follower acquire the lock (re-election latency floor).
  3. Thundering re-election: N candidates poll pg_try_advisory_lock in
     the same tick when the lock frees — how many rounds/attempts until
     exactly one wins, and what the losers pay.

The half-open-socket case (cable pull; server keeps the session, and the
lock, alive until TCP keepalive fires) is bounded by TaskQ's keepalive
policy (worker/deps.py: _TCP_KEEPIDLE=30 + _TCP_KEEPINTVL=5 x
_KEEPCNT=3 ≈ 45 s to detect) + one election retry (heartbeat_interval,
default 10 s) — reported arithmetically, since a real cable-pull needs
network fault injection.

Read-only with respect to src/; writes only its own artifacts under
results/.

Usage:
    python benchmarks/leader_flap_probe.py
    python benchmarks/leader_flap_probe.py --candidates 32
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path

import asyncpg

from taskq.constants import schema_lock_name

DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
SCHEMA = "tq_churn_probe_election"
RESULTS_DIR = Path(__file__).parent / "results"

LOCK_NAME = schema_lock_name("maintenance_leader", SCHEMA)
TRY_LOCK = "SELECT pg_try_advisory_lock(hashtextextended($1, 0))"


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=DSN)
    ap.add_argument("--candidates", type=int, default=10)
    args = ap.parse_args()

    out: dict = {"lock_name": LOCK_NAME}

    leader = await asyncpg.connect(args.dsn)

    # ── 1. Session-level check: lock survives a committed transaction ──
    got = await leader.fetchval(TRY_LOCK, LOCK_NAME)
    assert got is True, "failed to acquire the probe lock on an idle database"
    async with leader.transaction():
        pass  # transaction boundary on the holder
    follower = await asyncpg.connect(args.dsn)
    t0 = time.perf_counter()
    follower_got = await follower.fetchval(TRY_LOCK, LOCK_NAME)
    probe_ms = (time.perf_counter() - t0) * 1000
    out["session_level_lock"] = {
        "holder_kept_lock_after_commit": follower_got is False,
        "follower_try_lock_ms": round(probe_ms, 3),
    }
    print(f"1) session-level: holder kept lock across tx commit: {follower_got is False}")

    # ── 2. Clean flap: holder conn closes → follower acquires ──
    flap_samples = []
    for _ in range(5):
        await leader.close()
        leader = await asyncpg.connect(args.dsn)
        assert await leader.fetchval(TRY_LOCK, LOCK_NAME) is True
        t_close = time.perf_counter()
        await leader.close()
        got = None
        while got is not True:
            t0 = time.perf_counter()
            got = await follower.fetchval(TRY_LOCK, LOCK_NAME)
            if got is not True:
                await asyncio.sleep(0.005)
        flap_samples.append((time.perf_counter() - t_close) * 1000)
        # Release the follower's hold so the next iteration's holder can
        # acquire (try-lock is re-entrant on the holding session itself).
        assert (
            await follower.fetchval("SELECT pg_advisory_unlock(hashtextextended($1, 0))", LOCK_NAME)
            is True
        )
    out["clean_flap_reacquire_ms"] = {
        "p50": round(statistics.median(flap_samples), 3),
        "max": round(max(flap_samples), 3),
        "samples": [round(s, 3) for s in flap_samples],
    }
    print(
        f"2) clean flap: follower acquires in p50={out['clean_flap_reacquire_ms']['p50']}ms "
        f"max={out['clean_flap_reacquire_ms']['max']}ms after holder close"
    )

    # ── 3. Thundering re-election: N candidates poll the same tick ──
    out["thundering"] = []
    attempts_total = 0

    async def run_candidate(c: asyncpg.Connection, t0: float, counter: list[int]) -> float:
        while True:
            counter[0] += 1
            if await c.fetchval(TRY_LOCK, LOCK_NAME) is True:
                return (time.perf_counter() - t0) * 1000
            await asyncio.sleep(0.010)  # compressed heartbeat_interval

    for rep in range(5):
        holder = await asyncpg.connect(args.dsn)
        assert await holder.fetchval(TRY_LOCK, LOCK_NAME) is True
        cands = [await asyncpg.connect(args.dsn) for _ in range(args.candidates)]
        # Everyone gets one "free" failed probe before the flap, mimicking
        # the fleet already parked on the retry cadence.
        for c in cands:
            assert await c.fetchval(TRY_LOCK, LOCK_NAME) is False
        counter = [0]
        for c in cands:
            assert await c.fetchval(TRY_LOCK, LOCK_NAME) is False
        pre_flap_attempts = counter[0]
        t0 = time.perf_counter()
        await holder.close()  # the flap

        tasks = [asyncio.create_task(run_candidate(c, t0, counter)) for c in cands]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        win_ms = min(t.result() for t in done)
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        failed = counter[0] - pre_flap_attempts - 1  # -1: the winner's success
        for c in cands:
            await c.close()
        print(
            f"3) flap #{rep + 1}: {args.candidates} candidates, winner in "
            f"{win_ms:.1f}ms, {failed} failed try-locks across the fleet"
        )
        out["thundering"].append({"failed_try_locks": failed, "winner_ms": round(win_ms, 2)})
        attempts_total += failed

    out["thundering_summary"] = {
        "candidates": args.candidates,
        "median_failed_try_locks": statistics.median(
            r["failed_try_locks"] for r in out["thundering"]
        ),
    }

    RESULTS_DIR.mkdir(exist_ok=True)
    p = RESULTS_DIR / "leader_flap_probe.json"
    p.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nwrote {p}")


if __name__ == "__main__":
    asyncio.run(main())
