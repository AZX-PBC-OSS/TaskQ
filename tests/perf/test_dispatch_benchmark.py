"""dispatch CTE p99 latency benchmark.

One-off manual measurement gate — not a CI gate.
Runs on demand: ``uv run pytest tests/perf -m "slow and integration" -v --capture=no``
"""

import datetime as dt
import time
from collections.abc import Sequence
from datetime import timedelta
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.worker.deps import WorkerDeps

ITERS = 200
WARMUP = 20
MEASURED = ITERS - WARMUP
LIMIT_N = 50
NUM_ACTORS = 10
JOBS_PER_ACTOR = 1000
TOTAL_JOBS = NUM_ACTORS * JOBS_PER_ACTOR
LOCK_LEASE_S = 90
MS_THRESHOLD = 50  # gate: p99 ≤ 50ms


def _percentile(data: Sequence[int], pct: float) -> int:
    """Nearest-rank percentile (pct ∈ [0, 100])."""
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * pct / 100.0)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


@pytest.mark.slow
@pytest.mark.integration
async def test_dispatch_cte_p99_at_10k_pending(
    jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Measure dispatch CTE p99 latency at 10k pending jobs across 10 actors.

    Seeds 10,000 pending jobs (1,000 per actor) with varied priority,
    pre-populates ``actor_config`` with ``max_concurrent=10``, then runs
    the strict-FIFO dispatch CTE 200 times (20 warm-up, 180 measured) and
    asserts p99 ≤ 50ms.
    """
    deps, backend = jobs_app

    # ── Access internal attributes for direct CTE measurement ──
    # Private attrs accessed for benchmark-only measurement of the raw
    # CTE (no OTel-span/JobRow-decoding overhead).
    pool = backend._dispatcher_pool  # benchmark-only: direct raw-CTE measurement, no OTel span
    assert pool is not None, "dispatcher_pool required for benchmark"
    sql = (
        backend._sql.dispatch_strict_fifo  # benchmark-only: direct raw-CTE measurement, no OTel span
    )
    schema: str = deps.settings.schema_name

    worker_id: UUID = new_uuid()
    lock_lease = timedelta(seconds=LOCK_LEASE_S)
    queues = ["default"]
    now_utc = dt.datetime.now(tz=dt.UTC)

    # ── Seed actor_config (10 actors, max_concurrent=10) ──
    actor_names = [f"actor_{i}" for i in range(NUM_ACTORS)]
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '  # noqa: S608 # Why: schema validated by TaskQSettings regex; asyncpg has no identifier binding
            f"SELECT * FROM unnest($1::text[], $2::int[], $3::text[], $4::jsonb[])",
            actor_names,
            [10] * NUM_ACTORS,
            ["default"] * NUM_ACTORS,
            ["{}"] * NUM_ACTORS,
        )

    # ── Seed 10k pending jobs via bulk unnest ──
    ids = [new_uuid() for _ in range(TOTAL_JOBS)]
    actors: list[str] = []
    priorities: list[int] = []
    for actor_i in range(NUM_ACTORS):
        for _ in range(JOBS_PER_ACTOR):
            actors.append(f"actor_{actor_i}")
            priorities.append(actor_i)

    payload_json = '{"x": 1}'

    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, status, priority, scheduled_at) '  # noqa: S608 # Why: schema validated by TaskQSettings regex; asyncpg has no identifier binding
            f'SELECT u.id, u.actor, u.queue, u.payload::jsonb, u.max_attempts, u.retry_kind, u.status::"{schema}".job_status, u.priority, u.scheduled_at '
            f"FROM unnest("
            f"  $1::uuid[], $2::text[], $3::text[], $4::text[], $5::smallint[], $6::text[], $7::text[], $8::smallint[], $9::timestamptz[]"
            f") AS u(id, actor, queue, payload, max_attempts, retry_kind, status, priority, scheduled_at)",
            ids,
            actors,
            ["default"] * TOTAL_JOBS,
            [payload_json] * TOTAL_JOBS,
            [3] * TOTAL_JOBS,
            ["transient"] * TOTAL_JOBS,
            ["pending"] * TOTAL_JOBS,
            priorities,
            [now_utc] * TOTAL_JOBS,
        )

    # ── Benchmark: 200 iterations (20 warm-up, 180 measured) ──
    measurements_ns: list[int] = []
    async with pool.acquire() as conn:
        for _ in range(ITERS):
            t0 = time.perf_counter_ns()
            await conn.fetch(sql, queues, LIMIT_N, worker_id, lock_lease, 2)  # oversample=2
            t1 = time.perf_counter_ns()
            measurements_ns.append(t1 - t0)

    measured = measurements_ns[WARMUP:]

    p50_ns = _percentile(measured, 50)
    p95_ns = _percentile(measured, 95)
    p99_ns = _percentile(measured, 99)
    max_ns = max(measured)

    p50_ms = p50_ns / 1e6
    p95_ms = p95_ns / 1e6
    p99_ms = p99_ns / 1e6
    max_ms = max_ns / 1e6

    # ── Output for reviewer (requires --capture=no) ──
    print(f"\n── Dispatch CTE Benchmark @ {TOTAL_JOBS} pending jobs ──")
    print(f"  Warm-up iterations: {WARMUP}")
    print(f"  Measured iterations: {len(measured)}")
    print(f"  p50: {p50_ms:.2f} ms")
    print(f"  p95: {p95_ms:.2f} ms")
    print(f"  p99: {p99_ms:.2f} ms")
    print(f"  max: {max_ms:.2f} ms")
    print(f"  Gate: p99 ≤ {MS_THRESHOLD} ms → {'PASS' if p99_ms <= MS_THRESHOLD else 'FAIL'}")
    print("──")

    assert p99_ms <= MS_THRESHOLD, (
        f"latency gate violated: p99={p99_ms:.2f}ms > {MS_THRESHOLD}ms "
        f"(p50={p50_ms:.2f}, p95={p95_ms:.2f}, max={max_ms:.2f})"
    )
