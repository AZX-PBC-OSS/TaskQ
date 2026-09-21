"""Real-PG attacks on the one-step actor queue move (``move_actor_queue``).

Contract under attack, in the feature's own words:

* the ``actor_config_ops`` module docstring: the backlog rewrite plus the
  flip exist "so old-queue strays drain through the target's consumers";
* ``ActorQueueMoveResult``: the running rows left behind are "deliberately
  untouched - their queue field is inert once claimed and they finish on
  the worker that claimed them";
* the ``move_actor_queue`` docstring: a crash mid-drain "leaves the batches
  already committed as partial progress; a re-run continues where it
  stopped", and a move that loses the concurrent-assignment race raises
  ``ValueError``;
* the stale-producer residual is the one carve-out: jobs enqueued to the
  source queue after the flip "are served by source-queue consumers".

The author's own pins cover the happy-path move, the both-sides boot
window, the refusal errors, and the configured-target carry.  This file
attacks what those pins do not: the running-job tail (every re-pend path -
failure retry, lease-expiry reclaim, operator retry - keeps the row's OLD
queue label, and dispatch candidates match ``jobs.queue`` against the
consumer's subscription, not the stored assignment), the mid-drain abort's
report, two concurrent moves of one actor, and the
unconfigured-to-unconfigured queue-row carry.  Integration tier only:
every attack runs against a real migrated schema (``module_pg_schema``)
through the production enqueue, dispatch, terminal-write, and sweep paths.
"""

# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; values are $-bound.

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog
from asyncpg.exceptions import QueryCanceledError

from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import ActorQueueMoveResult, move_actor_queue
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId
from taskq.backend._sweeps import sweep_expired_locks, sweep_scheduled_to_pending
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.startup import sync_actor_config

from .test_rt_cron_harness import cron_settings, make_backend, pool_backend

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_GRACE = timedelta(seconds=1)
_OLD_QUEUE = "tqm_old"
_NEW_QUEUE = "tqm_new"
_ALT_QUEUE = "tqm_alt"
_ACTOR = "tqm_actor"
_DUE = datetime(2020, 1, 1, tzinfo=UTC)


async def _enqueue(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    count: int = 1,
    max_attempts: int = 3,
) -> None:
    settings = cron_settings(schema)
    backend = make_backend(settings)
    args = [
        EnqueueArgs(
            id=new_job_id(),
            actor=actor,
            queue=queue,
            payload={"probe": actor},
            max_attempts=max_attempts,
            retry_kind="transient",
            scheduled_at=_DUE,
        )
        for _ in range(count)
    ]
    await backend.enqueue_batch(args, connection=conn)


async def _dispatch(
    conn: asyncpg.Connection, schema: str, queues: list[str], limit_n: int
) -> list[asyncpg.Record]:
    return await dispatch_batch_sql(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=queues,
        limit_n=limit_n,
        worker_id=new_uuid(),
        lock_lease=_LEASE,
    )


async def _count_jobs(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    status: str,
) -> int:
    total: object = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND queue = $2 AND status::text = $3',
        actor,
        queue,
        status,
    )
    return int(total or 0)


async def _stored_queue(conn: asyncpg.Connection, schema: str, actor: str) -> str:
    stored: object = await conn.fetchval(
        f'SELECT queue FROM "{schema}".actor_config WHERE actor = $1', actor
    )
    return str(stored)


async def _row_state(conn: asyncpg.Connection, schema: str, job_id: UUID) -> tuple[str, str]:
    row = await conn.fetchrow(
        f'SELECT status::text AS status, queue FROM "{schema}".jobs WHERE id = $1', job_id
    )
    assert row is not None
    return str(row["status"]), str(row["queue"])


async def _move_actor_with_one_running_job(
    conn: asyncpg.Connection, schema: str, *, max_attempts: int
) -> asyncpg.Record:
    """Steady state every tail attack starts from: the actor lives on the old
    queue with exactly one claimed (running) row there, then the move flips
    the assignment and deliberately leaves that row behind."""
    await sync_actor_config(
        conn,
        [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
        schema=schema,
    )
    await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=1, max_attempts=max_attempts)
    claimed = await _dispatch(conn, schema, [_OLD_QUEUE], 1)
    assert len(claimed) == 1 and claimed[0]["actor"] == _ACTOR
    result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)
    assert result.running_jobs_left == 1
    return claimed[0]


async def _make_tail_due(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """Normalize a re-pend's backoff/due stamp to a past instant: the attacks
    assert WHERE the tail re-landed, not when it next comes due."""
    await conn.execute(f'UPDATE "{schema}".jobs SET scheduled_at = $2 WHERE id = $1', job_id, _DUE)


# ═══════════════════════════════════════════════════════════════════════════════
# The running-job tail: the move's left-behind rows re-enter the dispatchable
# pool through the retry/reclaim paths - on the old queue.
# ═══════════════════════════════════════════════════════════════════════════════


class TestRunningJobTail:
    """``ActorQueueMoveResult`` claims the left-behind running rows' "queue
    field is inert once claimed and they finish on the worker that claimed
    them".  Every re-pend path sends the row back into the pending pool
    still carrying the OLD queue label, and dispatch matches ``jobs.queue``
    against the consumer's subscription - so the tail is claimable only by
    consumers of the queue the operator is told to retire once the producers
    have moved."""

    async def test_failure_retry_tail_is_served_only_by_the_retired_source_queue(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """The worker's own failure-retry (``mark_failed_or_retry`` with a
        backoff) is the most common tail: a transient error on the claimed
        row after the flip."""
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        claimed = await _move_actor_with_one_running_job(conn, schema, max_attempts=3)
        job_id = UUID(str(claimed["id"]))

        backend = pool_backend(cron_settings(schema), module_pg_pool)
        await backend.mark_failed_or_retry(
            JobId(job_id),
            UUID(str(claimed["locked_by_worker"])),
            ErrorInfo(
                error_class="TransientError", error_message="attack probe", error_traceback=None
            ),
            timedelta(seconds=5),
            attempt=int(claimed["attempt"]),
            claim_epoch=int(claimed["claim_epoch"]),
        )
        status, queue = await _row_state(conn, schema, job_id)
        assert status == "scheduled"
        assert queue == _OLD_QUEUE
        await _make_tail_due(conn, schema, job_id)
        promoted = await sweep_scheduled_to_pending(conn, schema=schema)
        assert promoted == 1

        claimed_new = await _dispatch(conn, schema, [_NEW_QUEUE], 5)
        claimed_old = await _dispatch(conn, schema, [_OLD_QUEUE], 5)
        assert len(claimed_new) == 1, (
            "the target's consumers must serve the moved actor's failure-retry "
            f"tail (its queue field is claimed to be inert once claimed); the "
            f"row re-landed status={status} on queue={queue}, the target's "
            f"consumers claimed {len(claimed_new)} of it and only the retired "
            f"source queue's consumers claimed {len(claimed_old)}"
        )

    async def test_lease_expiry_reclaim_tail_is_served_only_by_the_retired_source_queue(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """The crash tail: the claiming worker dies, the leader's reclaim
        sweep re-pends the row with its retry budget - onto the old queue."""
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        claimed = await _move_actor_with_one_running_job(conn, schema, max_attempts=3)
        job_id = UUID(str(claimed["id"]))

        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = $2 WHERE id = $1', job_id, _DUE
        )
        reclaimed = await sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)
        assert reclaimed == 1
        status, queue = await _row_state(conn, schema, job_id)
        assert status == "pending"
        assert queue == _OLD_QUEUE
        await _make_tail_due(conn, schema, job_id)

        claimed_new = await _dispatch(conn, schema, [_NEW_QUEUE], 5)
        claimed_old = await _dispatch(conn, schema, [_OLD_QUEUE], 5)
        assert len(claimed_new) == 1, (
            "the target's consumers must serve the moved actor's crash-reclaim "
            f"tail; the sweep re-pended the row status={status} on queue={queue}, "
            f"the target's consumers claimed {len(claimed_new)} of it and only "
            f"the retired source queue's consumers claimed {len(claimed_old)}"
        )

    async def test_operator_retry_tail_is_served_only_by_the_retired_source_queue(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """The operator tail: the left-behind row fails terminally, an
        operator re-runs it via the admin retry - back onto the old queue."""
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        claimed = await _move_actor_with_one_running_job(conn, schema, max_attempts=1)
        job_id = UUID(str(claimed["id"]))

        backend = pool_backend(cron_settings(schema), module_pg_pool)
        await backend.mark_failed_or_retry(
            JobId(job_id),
            UUID(str(claimed["locked_by_worker"])),
            ErrorInfo(
                error_class="PermanentError", error_message="attack probe", error_traceback=None
            ),
            None,
            attempt=int(claimed["attempt"]),
            claim_epoch=int(claimed["claim_epoch"]),
        )
        status, queue = await _row_state(conn, schema, job_id)
        assert status == "failed"
        assert queue == _OLD_QUEUE
        retried = await backend.retry_job(JobId(job_id))
        assert retried is True
        status, queue = await _row_state(conn, schema, job_id)
        assert status == "pending"
        assert queue == _OLD_QUEUE
        await _make_tail_due(conn, schema, job_id)

        claimed_new = await _dispatch(conn, schema, [_NEW_QUEUE], 5)
        claimed_old = await _dispatch(conn, schema, [_OLD_QUEUE], 5)
        assert len(claimed_new) == 1, (
            "the target's consumers must serve the moved actor's operator-retry "
            f"tail; the retry re-pended the row status={status} on queue={queue}, "
            f"the target's consumers claimed {len(claimed_new)} of it and only "
            f"the retired source queue's consumers claimed {len(claimed_old)}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# The mid-drain abort: committed partial progress that nothing reports.
# ═══════════════════════════════════════════════════════════════════════════════


class TestMidDrainAbort:
    """A batch that cannot finish (here: a row lock held by another
    connection until the batch's own ``statement_timeout`` cancels it) is
    the documented crash-mid-drain state: earlier batches stay committed,
    the assignment still names the source queue, and a re-run continues
    where the aborted drain stopped.  The abort also destroys durable
    state (rows already rewritten onto the target), so the failure itself
    must surface what it began - the count already moved - to whoever
    operates the queue."""

    async def test_mid_drain_abort_is_re_runnable_but_leaves_partial_progress_unreported(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=12)
        id_rows = await conn.fetch(
            f'SELECT id FROM "{schema}".jobs WHERE actor = $1 ORDER BY id', _ACTOR
        )
        assert len(id_rows) == 12
        blocked_id: Any = id_rows[6]["id"]

        blocker = await asyncpg.connect(module_pg_schema.pg_dsn)
        surfaced = False
        try:
            await blocker.execute("BEGIN")
            await blocker.execute(
                f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE', blocked_id
            )
            with (
                structlog.testing.capture_logs() as logs,
                pytest.raises(QueryCanceledError) as exc_info,
            ):
                await move_actor_queue(
                    conn,
                    _ACTOR,
                    _NEW_QUEUE,
                    schema=schema,
                    batch_size=5,
                    statement_timeout_ms=1000,
                )
            assert (
                await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="pending")
                == 5
            )
            assert (
                await _count_jobs(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, status="pending")
                == 7
            )
            assert await _stored_queue(conn, schema, _ACTOR) == _OLD_QUEUE
            surfaced = "moved" in str(exc_info.value).lower() or any(
                "move" in str(event.get("event", "")).lower() for event in logs
            )
        finally:
            await blocker.execute("ROLLBACK")
            await blocker.close()

        result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema, batch_size=5)
        assert result.jobs_moved == 7
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="pending") == 12
        )
        assert await _stored_queue(conn, schema, _ACTOR) == _NEW_QUEUE
        assert surfaced, (
            "the aborted move rewrote 5 of 12 rows onto the target before "
            "failing, and neither the raised error nor any log event during "
            "the call names that partial progress - an operator sees a "
            "cancelled statement and no evidence of the durable writes "
            "already committed"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Two concurrent moves of one actor.
# ═══════════════════════════════════════════════════════════════════════════════


class TestConcurrentMoves:
    """Both moves preflight the same source assignment, partition the backlog
    between their drains, and then serialize on the flip's ``FOR UPDATE`` of
    the assignment row: exactly one flip may land, the loser must refuse
    with the assignment-changed ``ValueError``, and no row may be lost,
    duplicated, or left behind on the source queue."""

    async def test_two_concurrent_moves_flip_exactly_once_and_lose_no_rows(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=30)

        conn_b = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            outcomes = await asyncio.gather(
                move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema, batch_size=3),
                move_actor_queue(conn_b, _ACTOR, _ALT_QUEUE, schema=schema, batch_size=3),
                return_exceptions=True,
            )
        finally:
            await conn_b.close()

        winners = [o for o in outcomes if isinstance(o, ActorQueueMoveResult)]
        losers = [o for o in outcomes if isinstance(o, BaseException)]
        assert len(winners) == 1
        assert len(losers) == 1
        assert isinstance(losers[0], ValueError)
        assert "changed" in str(losers[0])
        winner = winners[0]
        assert winner.to_queue in (_NEW_QUEUE, _ALT_QUEUE)
        assert await _stored_queue(conn, schema, _ACTOR) == winner.to_queue

        total = 0
        for queue in (_OLD_QUEUE, _NEW_QUEUE, _ALT_QUEUE):
            total += await _count_jobs(conn, schema, actor=_ACTOR, queue=queue, status="pending")
        assert total == 30
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, status="pending") == 0
        )


# ═══════════════════════════════════════════════════════════════════════════════
# The queue-row carry's unconfigured-to-unconfigured variant.
# ═══════════════════════════════════════════════════════════════════════════════


class TestQueueRowCarry:
    """The carry is fill-in: a configured target stands, an unconfigured
    source contributes nothing - so an unconfigured-to-unconfigured move must
    leave the ``queues`` table exactly as it found it (no row created for
    the target) while the moved backlog stays dispatchable from the target
    on the queue-table defaults."""

    async def test_unconfigured_to_unconfigured_carry_creates_no_queue_row(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=2)
        rows_before: object = await conn.fetchval(f'SELECT count(*) FROM "{schema}".queues')
        assert int(rows_before or 0) == 0

        result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        assert result.queues_row_carried is False
        rows_after: object = await conn.fetchval(f'SELECT count(*) FROM "{schema}".queues')
        assert int(rows_after or 0) == 0
        assert await _stored_queue(conn, schema, _ACTOR) == _NEW_QUEUE
        claimed = await _dispatch(conn, schema, [_NEW_QUEUE], 5)
        assert len(claimed) == 2
        assert all(r["queue"] == _NEW_QUEUE for r in claimed)


# ═══════════════════════════════════════════════════════════════════════════════
# The documented stale-producer residual, pinned so the running-job tail's
# undocumented status stays distinguishable from it.
# ═══════════════════════════════════════════════════════════════════════════════


class TestSourceQueueStray:
    """The one residual the feature documents: a stale producer enqueuing to
    the source queue after the flip is served by source-queue consumers -
    never by the target's.  This pins that the documented remedy actually
    holds, and that the stray is invisible to the target's consumers - the
    same invisibility the running-job tail suffers without any documented
    remedy or consumer guidance."""

    async def test_post_move_producer_stray_is_served_by_a_source_queue_consumer(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=1)
        await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=1)
        stray_rows = await conn.fetch(
            f'SELECT id FROM "{schema}".jobs '
            f"WHERE actor = $1 AND queue = $2 AND status = 'pending'",
            _ACTOR,
            _OLD_QUEUE,
        )
        assert len(stray_rows) == 1
        stray_id = UUID(str(stray_rows[0]["id"]))

        claimed_new = await _dispatch(conn, schema, [_NEW_QUEUE], 5)
        assert stray_id not in {UUID(str(r["id"])) for r in claimed_new}
        claimed_old = await _dispatch(conn, schema, [_OLD_QUEUE], 5)
        assert [UUID(str(r["id"])) for r in claimed_old] == [stray_id]
