"""Regression test for round-robin fairness-key starvation (Finding 5).

``DISPATCH_ROUND_ROBIN_SQL`` samples up to ``residual * oversample``
candidate rows per actor+queue *before* computing ``fairness_rank``. When
that sampling LIMIT is applied globally (ordered by priority/scheduled_at
only, ignoring fairness_key), a deep cohort enqueued first can occupy the
entire oversampled candidate window, so a shallow cohort's jobs never even
reach the fairness_rank computation — starving it indefinitely regardless
of how many dispatch rounds run.

The fix partitions the oversample LIMIT by ``fairness_key`` (via an inner
``ROW_NUMBER() OVER (PARTITION BY fairness_key ...)`` filter) so every
fairness_key contributes candidates up to the oversample bound.
"""

# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; values are $-bound.

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.fixtures import JobsApp, ModulePgSchema

pytestmark = pytest.mark.integration

_LEASE_SECONDS = 30


def _args(actor: str, queue: str, fairness_key: str) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        fairness_key=fairness_key,
    )


async def _setup_round_robin_queue(conn: object, schema: str, queue: str) -> None:
    await conn.execute(  # type: ignore[attr-defined]
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING',
        queue,
        "round_robin",
    )


@pytest.mark.asyncio
async def test_deep_cohort_does_not_starve_shallow_fairness_key(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """500 pending jobs under fairness_key A, 1 job under fairness_key B.

    Both enqueued to the same actor/queue, A enqueued first (so A sorts
    ahead of B on priority/scheduled_at/id — the ordering the old global
    LIMIT used). Dispatching with limit=4 must surface B's job within a
    couple of rounds — not be starved indefinitely by A's depth.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    actor = "fairness-starvation"
    queue = "fq"

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        await _setup_round_robin_queue(conn, schema, queue)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, $2, $3, $4::jsonb) "
            "ON CONFLICT (actor) DO UPDATE SET max_concurrent = $2, queue = $3, metadata = $4::jsonb",
            actor,
            None,
            "default",
            "{}",
        )

    # Deep cohort A enqueued first — 500 pending jobs.
    for _ in range(500):
        await backend.enqueue(_args(actor, queue, "A"))

    # Shallow cohort B — a single job, enqueued after A.
    await backend.enqueue(_args(actor, queue, "B"))

    found_b = False
    for _round in range(2):
        dispatched = await backend.dispatch_batch(
            worker_id=worker_id,
            queues=[queue],
            limit=4,
            lock_lease=timedelta(seconds=_LEASE_SECONDS),
        )
        if any(row.fairness_key == "B" for row in dispatched):  # type: ignore[comparison-overlap]
            found_b = True
            break

    assert found_b, (
        "fairness_key 'B' (1 pending job) was starved by fairness_key 'A' "
        "(500 pending jobs) across 2 dispatch rounds of limit=4 — the "
        "oversample LIMIT is truncating the candidate list before "
        "fairness_rank partitioning"
    )


@pytest.mark.asyncio
async def test_concurrent_dispatch_identity_key_bounded_overcount(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Identity-key exclusivity is best-effort under concurrent dispatchers.

    ``running_identities`` is an unlocked SELECT evaluated once per dispatch
    statement, so two concurrent dispatchers can each admit one job for the
    same identity_key (TOCTOU). Within a single statement the identity_dedup
    rank admits at most one, so the documented bound is one job per
    concurrent dispatcher. This test codifies that bound: with two
    barrier-synced dispatchers, at most 2 jobs sharing identity 'K' may run.
    """
    import asyncio

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    actor = "identity-toctou"
    queue = "iq"

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        await _setup_round_robin_queue(conn, schema, queue)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, $2, $3, $4::jsonb) "
            "ON CONFLICT (actor) DO UPDATE SET max_concurrent = $2, queue = $3, metadata = $4::jsonb",
            actor,
            10,
            "default",
            "{}",
        )

    for _ in range(10):
        args = _args(actor, queue, "F")
        await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor=args.actor,
                queue=args.queue,
                payload=args.payload,
                max_attempts=args.max_attempts,
                retry_kind=args.retry_kind,
                scheduled_at=args.scheduled_at,
                identity_key="K",
            )
        )

    barrier = asyncio.Barrier(2)

    async def _dispatch() -> int:
        await barrier.wait()
        rows = await backend.dispatch_batch(
            worker_id=new_uuid(),
            queues=[queue],
            limit=5,
            lock_lease=timedelta(seconds=_LEASE_SECONDS),
        )
        return len(rows)

    await asyncio.gather(_dispatch(), _dispatch())

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        running_k = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE status = 'running' AND actor = $1 AND identity_key = $2",
            actor,
            "K",
        )

    assert 1 <= running_k <= 2, (
        f"expected 1..2 running jobs for identity 'K' under 2 concurrent "
        f"dispatchers (best-effort bound = one per dispatcher), got {running_k}"
    )
