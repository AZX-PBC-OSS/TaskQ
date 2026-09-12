"""Red-team attacks on dispatch fairness: batch seriality vs cross-actor starvation (real PG).

HYPOTHESIS (unverified suspicion): the batch-claim path silently upgrades serial
queues to concurrent (graphile-worker #621 precedent — one batch fetch locked
several jobs from one named queue; fixed by enforcing at-most-one-per-queue
IN SQL via DISTINCT ON) or lets a flooded actor starve a lone actor's job
beyond any bounded number of batches (global FIFO-by-id defeating fairness).

What was read before choosing the angles
(``src/taskq/backend/_dispatch_sql.py`` fully, plus the dispatch indexes in
``src/taskq/migrations/01.00.00_01_pre_initial.sql:124-131``):

* The per-actor/per-queue seriality semantics TaskQ actually offers are
  (a) ``identity_key`` — one running job per ``(actor, identity_key)``,
  enforced IN SQL by ``DISTINCT ON (actor, identity_key)`` in the
  ``identity_dedup`` CTE plus the ``running_identities`` exclusion — and
  (b) ``actor_config.max_concurrent`` — a per-round admission damper enforced
  IN SQL by the ``per_actor_capacity`` residual and the ``eligible``
  ``actor_rank <= max_concurrent - in_flight`` gate. Both are the same
  in-SQL enforcement shape as the graphile fix, so angle 1 attacks them
  directly: a single dispatch batch must never hand out two jobs that must
  serialize.
* ``singleton`` (``jobs_singleton_uniq``) is enqueue-time mutual exclusion,
  not a dispatch-time seriality semantic, so it is out of scope here.
* Cross-actor fairness in ``strict_fifo`` mode comes from ``pending_rank``
  (``ROW_NUMBER() PARTITION BY actor``): every actor's rank-1 job sorts ahead
  of any actor's rank-2 job in the ``locked`` CTE, which is what must defeat
  naive global FIFO-by-id. Angle 2 floods actor A (500 pending, enqueued
  first, strictly ahead of B on ``(priority, scheduled_at)``) and asserts B's
  lone job still dispatches within a small bounded number of batches.

Both angles are deterministic by construction: single statement batches run
sequentially on one connection (no concurrent dispatchers, so the documented
best-effort TOCTOU windows in ``running_per_actor``/``running_identities``
cannot fire), seeded rows are always due (``scheduled_at`` in the past), and
no assertion depends on wall-clock timing or uuid ordering.
"""

# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; values are $-bound.

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs, IdentityKey, JobRow
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

from .test_rt_cron_harness import cron_settings, make_backend, seed_actor_config

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "rt_dispatch_q"

_SERIAL_ACTOR = "rt_dispatch_serial"
_CAPPED_ACTOR = "rt_dispatch_capped"
_FLOOD_ACTOR = "rt_dispatch_flood"
_LONE_ACTOR = "rt_dispatch_lone"

_FLOOD_COUNT = 500
_BATCH_LIMIT = 10
_STARVATION_BOUND_BATCHES = 2


async def _dispatch(
    conn: asyncpg.Connection,
    schema: str,
    queues: list[str],
    limit_n: int,
) -> list[asyncpg.Record]:
    """One dispatch batch via the narrowest entry point: the SQL helper directly.

    Each call mints a fresh worker id (a separate claimant), but calls run
    sequentially on one connection, so there is no inter-dispatcher race —
    whatever comes back is what a single statement admitted.
    """
    return await dispatch_batch_sql(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=queues,
        limit_n=limit_n,
        worker_id=new_uuid(),
        lock_lease=_LEASE,
    )


async def _enqueue_jobs(
    backend: PostgresBackend,
    conn: asyncpg.Connection,
    *,
    actor: str,
    count: int,
    scheduled_at: datetime,
    identity_key: IdentityKey | None = None,
) -> list[JobRow]:
    """Enqueue *count* due jobs through the production enqueue path, pool-free."""
    args_list = [
        EnqueueArgs(
            id=new_job_id(),
            actor=actor,
            queue=_QUEUE,
            payload={"probe": actor},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=scheduled_at,
            identity_key=identity_key,
        )
        for _ in range(count)
    ]
    return await backend.enqueue_batch(args_list, connection=conn)


async def _count_by_status(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    status: str,
) -> int:
    """How many jobs *actor* holds in *status* (``status`` compared as text)."""
    total: object = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND status::text = $2',
        actor,
        status,
    )
    assert isinstance(total, int)
    return total


class TestSerialQueueUpgrade:
    """Angle 1: a single dispatch batch must never hand out two jobs that must
    serialize — the graphile-worker #621 shape (batch fetch upgrading a serial
    queue to concurrent). Both per-actor seriality semantics TaskQ offers are
    attacked: ``identity_key`` serialization and ``max_concurrent = 1``."""

    async def test_single_batch_admits_at_most_one_job_per_identity(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Five pending jobs sharing one ``(actor, identity_key)``: one batch
        with ``limit=10`` must admit exactly one (the ``identity_dedup``
        ``DISTINCT ON`` in-SQL enforcement), leaving four pending — a batch
        claim that handed out two would run the "serial" identity
        concurrently."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        await seed_actor_config(clean_pg_conn, schema, _SERIAL_ACTOR, queue=_QUEUE)
        due = datetime.now(UTC) - timedelta(seconds=60)
        await _enqueue_jobs(
            backend,
            clean_pg_conn,
            actor=_SERIAL_ACTOR,
            count=5,
            scheduled_at=due,
            identity_key=IdentityKey("serial-acct-1"),
        )

        rows = await _dispatch(clean_pg_conn, schema, [_QUEUE], _BATCH_LIMIT)

        pairs = [(row["actor"], row["identity_key"]) for row in rows]
        assert len(pairs) == len(set(pairs)), (
            f"single batch handed out two jobs for one identity: {pairs} — "
            "the serial-queue upgrade (graphile-worker #621 shape)"
        )
        assert len(rows) == 1, (
            f"expected exactly 1 of 5 same-identity jobs admitted, got {len(rows)}"
        )
        assert await _count_by_status(clean_pg_conn, schema, _SERIAL_ACTOR, "pending") == 4
        assert await _count_by_status(clean_pg_conn, schema, _SERIAL_ACTOR, "running") == 1

    async def test_single_batch_respects_max_concurrent_one(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """An actor with ``max_concurrent = 1`` is a serial queue. Five pending
        jobs, nothing running, one batch with ``limit=10`` must admit exactly
        one (the ``eligible`` ``actor_rank <= max_concurrent - in_flight``
        gate) — admitting two would run a serial actor concurrently."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        await seed_actor_config(clean_pg_conn, schema, _CAPPED_ACTOR, queue=_QUEUE)
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".actor_config SET max_concurrent = 1 WHERE actor = $1',
            _CAPPED_ACTOR,
        )
        due = datetime.now(UTC) - timedelta(seconds=60)
        await _enqueue_jobs(backend, clean_pg_conn, actor=_CAPPED_ACTOR, count=5, scheduled_at=due)

        rows = await _dispatch(clean_pg_conn, schema, [_QUEUE], _BATCH_LIMIT)

        assert len(rows) == 1, (
            f"max_concurrent=1 actor admitted {len(rows)} jobs in one batch — "
            "the serial-queue upgrade (graphile-worker #621 shape)"
        )
        assert await _count_by_status(clean_pg_conn, schema, _CAPPED_ACTOR, "pending") == 4
        assert await _count_by_status(clean_pg_conn, schema, _CAPPED_ACTOR, "running") == 1


class TestCrossActorStarvation:
    """Angle 2: a flooded actor must not starve a lone actor's job — the
    ``pending_rank`` per-actor fairness must defeat naive global FIFO-by-id."""

    async def test_flooded_actor_does_not_starve_lone_job(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Actor A floods the shared queue with 500 pending jobs enqueued
        strictly before B's single job (A sorts ahead on ``(priority,
        scheduled_at)`` — the order a global FIFO-by-id batch claim would
        serve). Repeated ``limit=10`` dispatch batches, run sequentially via
        the dispatch function directly (deterministic by seed, no timing),
        must surface B's job within 2 batches: B holds ``pending_rank = 1``
        while A's rank-2+ jobs sort behind every rank-1 job."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        await seed_actor_config(clean_pg_conn, schema, _FLOOD_ACTOR, queue=_QUEUE)
        await seed_actor_config(clean_pg_conn, schema, _LONE_ACTOR, queue=_QUEUE)
        now = datetime.now(UTC)
        await _enqueue_jobs(
            backend,
            clean_pg_conn,
            actor=_FLOOD_ACTOR,
            count=_FLOOD_COUNT,
            scheduled_at=now - timedelta(seconds=60),
        )
        await _enqueue_jobs(
            backend,
            clean_pg_conn,
            actor=_LONE_ACTOR,
            count=1,
            scheduled_at=now - timedelta(seconds=1),
        )

        found_at: int | None = None
        first_batch_size: int | None = None
        for batch_no in range(5):
            rows = await _dispatch(clean_pg_conn, schema, [_QUEUE], _BATCH_LIMIT)
            assert rows, f"batch {batch_no} dispatched nothing with jobs still pending"
            if first_batch_size is None:
                first_batch_size = len(rows)
            if any(row["actor"] == _LONE_ACTOR for row in rows):
                found_at = batch_no
                break

        assert first_batch_size == _BATCH_LIMIT, (
            f"first batch dispatched {first_batch_size}, not a full {_BATCH_LIMIT} — "
            "the test only means something when the batch is full yet still carries B"
        )
        assert found_at is not None, (
            f"lone job starved across 5 batches of {_BATCH_LIMIT} behind "
            f"{_FLOOD_COUNT} flooded jobs — fairness sampling lost to FIFO-by-id"
        )
        assert found_at < _STARVATION_BOUND_BATCHES, (
            f"lone job surfaced only in batch {found_at} — beyond the "
            f"{_STARVATION_BOUND_BATCHES}-batch fairness bound"
        )
        assert await _count_by_status(clean_pg_conn, schema, _LONE_ACTOR, "running") == 1
