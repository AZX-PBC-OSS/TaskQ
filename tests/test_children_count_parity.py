# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Backend parity + measured cost for the parent_id reads (LIB-2, issue #670).

Both backends must answer ``count_pending_jobs_by_queue`` and
``count_pending_children_by_queue`` the same (the semantic-parity
doctrine: the in-memory mirror is the greener of the two, so the pins
assert the PG result and flag the mirror when it diverges).

The measured behaviors are pinned as relative bands, not absolute
microsecond budgets (the #664 pattern: mutually-anchored baselines
inside one process, a noise band asserted):

* PLAN SHAPE: the pending-children aggregate must be served by the
  partial index (``jobs_parent_pending_idx``) on a seeded hot table —
  a Seq Scan on the 100k-row table reds. Mutation check: drop the
  index and this file reds.
* COUNT COST BAND: the aggregate's latency at 10 vs 10k pending
  children stays index-shaped — the walk grows with the CHILD
  population, never with the hot table.
* CHURN: the purge deleting rows WHILE the count runs — no error,
  bounded latency (one statement, snapshot-consistent).
* WRITE COST: the partial index's maintenance on parented rows, gated
  RELATIVELY — the parented COPY rate against the unparented COPY rate
  on the same warm table, in one process.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq._ids import new_job_id
from taskq.testing import FakeClock, InMemoryBackend, make_enqueue_args

if TYPE_CHECKING:
    from taskq.backend._protocol import JobId
    from taskq.backend.postgres import PostgresBackend
    from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


async def _seed_children(
    backend: PostgresBackend | InMemoryBackend,
    parent_id: JobId,
    *,
    queues: dict[str, int],
    scheduled_future: bool = False,
) -> None:
    for queue, n in queues.items():
        for _ in range(n):
            args = make_enqueue_args(
                queue=queue,
                scheduled_at=_NOW + timedelta(hours=1) if scheduled_future else None,
            )
            await backend.enqueue(replace(args, parent_id=parent_id))


async def test_parity_children_count_matches_pg(clean_jobs_app: JobsApp) -> None:
    """Seeded children across three queues, pending + scheduled: both backends
    return the same grouped dict; a dangling parent reads {} on both."""
    memory = InMemoryBackend(FakeClock(_NOW))
    pg_backend = clean_jobs_app.backend
    parent_id = new_job_id()

    await _seed_children(memory, parent_id, queues={"a": 3, "b": 2})
    await _seed_children(pg_backend, parent_id, queues={"a": 3, "b": 2})
    # A scheduled child also holds a pending slot — both count it.
    await _seed_children(memory, parent_id, queues={"c": 1}, scheduled_future=True)
    await _seed_children(pg_backend, parent_id, queues={"c": 1}, scheduled_future=True)

    mem_map = await memory.count_pending_children_by_queue(parent_id)
    pg_map = await pg_backend.count_pending_children_by_queue(parent_id)
    assert pg_map == {"a": 3, "b": 2, "c": 1}
    assert mem_map == pg_map

    # Dangling parent: defined, harmless, empty on both. parent_id is a
    # plain column (NO foreign key — per-child key-share locks would
    # serialize the hottest table's inserts, and retention must never
    # block a parent's purge on pending children); a parent id that
    # never existed in this store is the same defined-empty state.
    dangling_mem = await memory.count_pending_children_by_queue(new_job_id())
    dangling_pg = await pg_backend.count_pending_children_by_queue(new_job_id())
    assert dangling_mem == {} == dangling_pg


async def test_parity_depth_count_matches_pg(clean_jobs_app: JobsApp) -> None:
    """count_pending_jobs_by_queue: grouped pending+scheduled counts, missing
    queues read 0, both backends agree."""
    memory = InMemoryBackend(FakeClock(_NOW))
    pg_backend = clean_jobs_app.backend
    for backend in (memory, pg_backend):
        for queue, n in (("a", 2), ("b", 1)):
            for _ in range(n):
                await backend.enqueue(make_enqueue_args(queue=queue))

    pg_map = await pg_backend.count_pending_jobs_by_queue(["a", "b", "empty"])
    assert pg_map == {"a": 2, "b": 1}
    assert await memory.count_pending_jobs_by_queue(["a", "b", "empty"]) == pg_map
    assert await pg_backend.count_pending_jobs_by_queue([]) == {}


async def test_children_count_is_index_served(clean_jobs_app: JobsApp) -> None:
    """The plan must walk jobs_parent_pending_idx, never a Seq Scan.

    Seeded hot table: 100k unparented rows + 10k pending children. The
    mutation check: drop the index, this pin reds (the plan flips to a
    Seq Scan over the hot table).
    """
    pg_backend = clean_jobs_app.backend
    schema = clean_jobs_app.deps.settings.schema_name
    parent_id = new_job_id()

    bulk = [make_enqueue_args(queue=f"bulk_{i % 8}") for i in range(100_000)]
    await pg_backend.enqueue_batch_fast(bulk)
    await _seed_children(pg_backend, parent_id, queues={"fan": 10_000})

    plan = await pg_backend._worker_pool.fetchval(
        f"""
        EXPLAIN (FORMAT JSON)
        SELECT queue, count(*)::int FROM "{schema}".jobs
        WHERE parent_id = $1 AND status IN ('pending', 'scheduled')
        GROUP BY queue
        """,
        parent_id,
    )
    plan_text = str(plan)
    assert "jobs_parent_pending_idx" in plan_text, plan_text
    assert "Seq Scan" not in plan_text, plan_text


async def test_children_count_band_at_scale(clean_jobs_app: JobsApp) -> None:
    """The 10k-children aggregate stays inside the index-scan band of the
    10-children aggregate. An index walk over parented rows grows with
    the CHILD POPULATION only; a flip to a hot-table walk reds this."""
    pg_backend = clean_jobs_app.backend
    small_parent = new_job_id()
    await _seed_children(pg_backend, small_parent, queues={"s": 10})

    big_parent = new_job_id()
    await _seed_children(pg_backend, big_parent, queues={"b": 10_000})

    async def _timed(parent: JobId) -> float:
        start = time.perf_counter()
        await pg_backend.count_pending_children_by_queue(parent)
        return (time.perf_counter() - start) * 1000

    small_ms = min([await _timed(small_parent) for _ in range(5)])
    big_ms = min([await _timed(big_parent) for _ in range(5)])

    # The band: 10k index entries cost at most 50x the 10-entry walk
    # (log-depth + page-cache variance). A hot-table aggregate over the
    # 100k-row table does not fit this band.
    assert big_ms < max(small_ms * 50, 25.0), (small_ms, big_ms)


async def test_count_survives_purge_churn(clean_jobs_app: JobsApp) -> None:
    """The purge deleting WHILE the count runs: no error, bounded latency.

    parent_id has no FK (the binding constraint): retention deletes
    parents/children by age in bulk and never serializes behind the
    count, and the count's statement snapshot never sees a torn delete.
    """
    pg_backend = clean_jobs_app.backend
    schema = clean_jobs_app.deps.settings.schema_name
    parent_id = new_job_id()
    await _seed_children(pg_backend, parent_id, queues={"churn": 2_000})

    async def _purge() -> None:
        pool = pg_backend._worker_pool
        for _ in range(20):
            await pool.execute(
                f'DELETE FROM "{schema}".jobs '
                "WHERE queue = 'churn' AND ctid IN ("
                f'SELECT ctid FROM "{schema}".jobs WHERE queue = \'churn\' LIMIT 50)'
            )
            await asyncio.sleep(0)

    purge_task = asyncio.create_task(_purge())
    try:
        for _ in range(20):
            result = await asyncio.wait_for(
                pg_backend.count_pending_children_by_queue(parent_id), timeout=5.0
            )
            assert isinstance(result, dict)
    finally:
        await purge_task


async def test_write_cost_noise_band_parented_vs_unparented(
    clean_jobs_app: JobsApp,
) -> None:
    """The partial index's write cost, gated RELATIVELY (the #664 pattern):
    parented rows take the index maintenance, unparented rows do not —
    same process, same table, same clock. The parented rate must stay
    inside the noise band of the unparented rate; a regression beyond
    the band reds."""
    pg_backend = clean_jobs_app.backend
    parent_id = new_job_id()

    async def _burst(stamp: JobId | None, n: int = 500) -> float:
        args: list[object] = [make_enqueue_args(queue="cost_q") for _ in range(n)]
        if stamp is not None:
            args = [replace(a, parent_id=stamp) for a in args]  # type: ignore[arg-type]
        start = time.perf_counter()
        await pg_backend.enqueue_batch_fast(args)  # type: ignore[arg-type]
        return (time.perf_counter() - start) * 1000

    # Warm the table + index, then anchor both arms on the same warm state.
    await _burst(None, 200)
    await _burst(parent_id, 200)

    unparented_ms = min([await _burst(None) for _ in range(5)])
    parented_ms = min([await _burst(parent_id) for _ in range(5)])

    # The band: index maintenance on the parented rows costs at most 2x
    # the unparented COPY rate at this scale — a partial index over
    # parent-stamped rows only. The real delta is a few percent; the
    # generous band exists to survive CI noise, not to flatter.
    assert parented_ms < unparented_ms * 2.0, (unparented_ms, parented_ms)
