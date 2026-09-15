"""Issue #104 — moving an actor between queues is ONE atomic operator action,
and worker boot stays consistent at every intermediate state of the rolling
deploy that ships the matching ``@actor(queue=...)`` literal.

Pre-fix failure mode these pins replace: a move took four coordinated writes —
the ``@actor`` literal, the stored ``actor_config.queue`` row, the worker's
consumed-queue set, and the ``queues`` row for the target — and the first pair
was fail-closed: whichever order the operator picked, one side of a rolling
deploy could not boot (``ActorConfigDriftList`` from ``sync_actor_config``)
until the other write landed, while the stranding half (a queue nobody
consumes, a missing ``queues`` row degrading round_robin to strict_fifo) was
silent. The move therefore is:

* one operator action in two phases — the actor's pending/scheduled backlog
  is rewritten onto the target queue as bounded committed batches (the
  deregister force-drain doctrine: windowed CTE, per-batch
  statement_timeout, termination on the window count), then ONE final
  transaction locks the assignment row, carries the source queue's
  ``queues`` row (mode + max_concurrent) to the target when the target has
  none, and flips the stored assignment. Running jobs are untouched: their
  queue is inert once claimed. The flip lands last so a crash mid-drain
  re-runs cleanly (the drain's queue predicate skips rows earlier batches
  moved);
* boot semantics that hold across the window — the stored queue is the
  operator-owned assignment once a row exists (a differing literal logs
  ``actor-config-queue-override`` and boots; the startup UPSERT never
  rewrites the stored queue), so old-literal and new-literal workers both boot
  at every intermediate state. Metadata stays fail-closed: no operator surface
  moves it, so a mismatch there is always a bug.

Unit tier uses a fake connection; integration tier runs against a real
migrated schema (``module_pg_schema``) through the production enqueue and
dispatch paths.
"""

# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; values are $-bound.

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
import structlog
from typer.testing import CliRunner

from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import (
    _MOVE_BACKLOG_BATCH_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the drain's own statement is the oracle for per-batch cost; a copy here would drift from the shipped one.
    ActorQueueMoveResult,
    move_actor_queue,
)
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs
from taskq.backend._sql_templates import render
from taskq.cli import app
from taskq.exceptions import ActorConfigDriftList, ActorNotFoundError
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.queue_ops import set_queue_max_concurrent, set_queue_mode
from taskq.worker.startup import sync_actor_config

from .test_rt_cron_harness import cron_settings, make_backend

runner = CliRunner()

_LEASE = timedelta(seconds=30)
_OLD_QUEUE = "tqm_old"
_NEW_QUEUE = "tqm_new"
_ACTOR = "tqm_actor"
_OTHER_ACTOR = "tqm_other"
# A fixed past instant for due rows, and a genuinely future one for the
# scheduled row — enqueue stamps 'scheduled' only when the delay from the
# server clock is positive, so a "future" date in the past would silently
# produce another pending row.
_DUE = datetime(2020, 1, 1, tzinfo=UTC)
_FUTURE = datetime.now(UTC) + timedelta(hours=1)


# ═══════════════════════════════════════════════════════════════════════════════
# Unit tier (fake connection): boot consistency across the move window
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class _FakeRecord:
    """Record-like double supporting dict-style access, as ``ConnLike`` reads it."""

    _fields: dict[str, object] = field(default_factory=dict[str, object])

    def __getitem__(self, key: str) -> object:
        return self._fields[key]


class _FakeConn:
    """Test double for ``asyncpg.Connection`` recording SELECT/UPSERT calls."""

    def __init__(self) -> None:
        self._select_rows: list[_FakeRecord] = []
        self.execute_calls: list[tuple[str, list[Any]]] = []

    def set_select_rows(self, rows: list[_FakeRecord]) -> None:
        self._select_rows = list(rows)

    async def fetch(self, query: str, *params: Any) -> list[_FakeRecord]:
        return list(self._select_rows)

    async def execute(self, query: str, *params: Any) -> str:
        self.execute_calls.append((query, list(params)))
        return "OK"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


class _FakeTransaction:
    def __init__(self) -> None:
        self._entered = False

    async def __aenter__(self) -> _FakeTransaction:
        self._entered = True
        return self

    async def __aexit__(self, *args: object) -> None:
        if not self._entered:
            raise RuntimeError("transaction exited without entering")


def _stored_row(
    actor: str, *, queue: str, metadata: dict[str, object] | None = None
) -> _FakeRecord:
    return _FakeRecord(
        {
            "actor": actor,
            "max_concurrent": None,
            "max_pending": None,
            "queue": queue,
            "result_ttl": None,
            "metadata": json.dumps(metadata if metadata is not None else {}),
        }
    )


def _config(actor: str, *, queue: str, metadata: dict[str, object] | None = None) -> ActorConfig:
    return ActorConfig(
        actor=actor,
        max_concurrent=None,
        max_pending=None,
        queue=queue,
        result_ttl=None,
        metadata=metadata if metadata is not None else {},
    )


async def test_mid_transition_boot_succeeds_and_preserves_assignment() -> None:
    """The mid-transition state — stored row already moved to the new queue,
    worker still carrying the OLD code literal — must boot, and its startup
    UPSERT must not flip the stored assignment back. Pre-fix this exact state
    raised ``ActorConfigDriftList`` and refused boot."""
    fake_conn = _FakeConn()
    fake_conn.set_select_rows([_stored_row(_ACTOR, queue=_NEW_QUEUE)])

    with structlog.testing.capture_logs() as logs:
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double; real asyncpg.Connection subtyping would need protocol-level mocking
            [_config(_ACTOR, queue=_OLD_QUEUE)],
            force=False,
        )

    # Booted: the UPSERT ran (no refusal, no force flag needed).
    assert len(fake_conn.execute_calls) == 1
    # The assignment is preserved: the conflict clause never rewrites the
    # stored queue, so a stale-literal worker cannot undo the move.
    sql, _params = fake_conn.execute_calls[0]
    on_conflict = sql.split("DO UPDATE SET", 1)[1]
    assert "queue" not in on_conflict
    # Relocated detection: the mismatch is a named, warn-level event.
    override = [e for e in logs if e["event"] == "actor-config-queue-override"]
    assert override, "queue mismatch must be surfaced as actor-config-queue-override"
    assert override[0]["registered"] == _OLD_QUEUE
    assert override[0]["stored"] == _NEW_QUEUE


async def test_queue_only_mismatch_never_refuses_but_metadata_drift_still_does() -> None:
    """A queue mismatch alone never blocks boot (it is the move window, or a
    literal that has not followed the assignment yet). Metadata drift stays
    fail-closed — no operator surface moves metadata, so a mismatch there is
    always a bug and raises exactly as before."""
    fake_conn = _FakeConn()
    fake_conn.set_select_rows([_stored_row(_ACTOR, queue=_NEW_QUEUE)])
    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double; see above
        [_config(_ACTOR, queue=_OLD_QUEUE)],
        force=False,
    )
    assert len(fake_conn.execute_calls) == 1

    fake_conn = _FakeConn()
    fake_conn.set_select_rows([_stored_row(_ACTOR, queue=_NEW_QUEUE, metadata={"team": "ops"})])
    with pytest.raises(ActorConfigDriftList) as exc_info:
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double; see above
            [_config(_ACTOR, queue=_OLD_QUEUE, metadata={"team": "platform"})],
            force=False,
        )
    assert {d.field for d in exc_info.value.drifts} == {"metadata"}


# ═══════════════════════════════════════════════════════════════════════════════
# Integration tier (real migrated schema): the one-step move
# ═══════════════════════════════════════════════════════════════════════════════


async def _enqueue(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    count: int = 1,
    scheduled_at: datetime = _DUE,
) -> None:
    """Enqueue due (or scheduled) jobs through the production path, pool-free."""
    settings = cron_settings(schema)
    backend = make_backend(settings)
    args = [
        EnqueueArgs(
            id=new_job_id(),
            actor=actor,
            queue=queue,
            payload={"probe": actor},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=scheduled_at,
        )
        for _ in range(count)
    ]
    await backend.enqueue_batch(args, connection=conn)


async def _enqueue_one(conn: asyncpg.Connection, schema: str, *, actor: str, queue: str) -> object:
    """Enqueue a single due job through the production path, returning its id."""
    settings = cron_settings(schema)
    backend = make_backend(settings)
    job_id = new_job_id()
    await backend.enqueue_batch(
        [
            EnqueueArgs(
                id=job_id,
                actor=actor,
                queue=queue,
                payload={"probe": actor},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_DUE,
            )
        ],
        connection=conn,
    )
    return job_id


async def _dispatch(
    conn: asyncpg.Connection, schema: str, queues: list[str], limit_n: int
) -> list[asyncpg.Record]:
    """One dispatch batch via the narrowest entry point: the SQL helper."""
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


def _plan_buffers(node: dict[str, Any]) -> int:
    """Total shared buffers hit+read across a JSON EXPLAIN plan tree.

    Buffers track the rows a statement actually visited, so they expose a
    batch re-walking already-moved rows without depending on machine speed.
    """
    total = int(node.get("Shared Hit Blocks", 0)) + int(node.get("Shared Read Blocks", 0))
    for child in node.get("Plans", []):
        total += _plan_buffers(child)
    return total


class TestMoveActorQueue:
    """The one-transaction move and the boot window it must survive."""

    pytestmark = pytest.mark.integration

    async def test_rewrites_assignment_backlog_and_queue_row_in_one_step(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        # Steady state: the actor (and a neighbor) live on the old queue, which
        # is operator-configured round_robin with a cap — the config a naive
        # move silently loses.
        await sync_actor_config(
            conn,
            [
                ActorConfig(actor=_ACTOR, max_concurrent=3, queue=_OLD_QUEUE),
                ActorConfig(actor=_OTHER_ACTOR, max_concurrent=None, queue=_OLD_QUEUE),
            ],
            schema=schema,
        )
        await set_queue_mode(conn, _OLD_QUEUE, "round_robin", schema=schema)
        await set_queue_max_concurrent(conn, _OLD_QUEUE, 4, schema=schema)

        # 3 due + 1 scheduled for the actor; one due job is claimed (running)
        # BEFORE the neighbor's job exists, so the claim is deterministic.
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=3)
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=1, scheduled_at=_FUTURE)
        claimed = await _dispatch(conn, schema, [_OLD_QUEUE], 1)
        assert len(claimed) == 1 and claimed[0]["actor"] == _ACTOR
        await _enqueue(conn, schema, actor=_OTHER_ACTOR, queue=_OLD_QUEUE, count=1)

        result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        assert result.actor == _ACTOR
        assert result.from_queue == _OLD_QUEUE
        assert result.to_queue == _NEW_QUEUE
        assert result.jobs_moved == 3  # 2 pending + 1 scheduled
        assert result.running_jobs_left == 1
        assert result.queues_row_carried is True

        # The assignment moved; capacity survives untouched.
        row = await conn.fetchrow(
            f'SELECT queue, max_concurrent FROM "{schema}".actor_config WHERE actor = $1',
            _ACTOR,
        )
        assert row is not None
        assert row["queue"] == _NEW_QUEUE
        assert row["max_concurrent"] == 3

        # The backlog followed: pending AND scheduled rows now sit on the new
        # queue; the claimed (running) job did not; the neighbor's job did not.
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="pending") == 2
        )
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="scheduled") == 1
        )
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, status="running") == 1
        )
        assert (
            await _count_jobs(conn, schema, actor=_OTHER_ACTOR, queue=_OLD_QUEUE, status="pending")
            == 1
        )
        assert (
            await _count_jobs(conn, schema, actor=_OTHER_ACTOR, queue=_NEW_QUEUE, status="pending")
            == 0
        )

        # The target queue inherited the source queue's row; the source row
        # itself is untouched (the neighbor still lives there).
        new_q = await conn.fetchrow(
            f'SELECT mode, max_concurrent FROM "{schema}".queues WHERE name = $1',
            _NEW_QUEUE,
        )
        assert new_q is not None
        assert new_q["mode"] == "round_robin"
        assert new_q["max_concurrent"] == 4
        old_q = await conn.fetchrow(
            f'SELECT mode, max_concurrent FROM "{schema}".queues WHERE name = $1',
            _OLD_QUEUE,
        )
        assert old_q is not None
        assert old_q["mode"] == "round_robin"
        assert old_q["max_concurrent"] == 4

    async def test_mid_transition_boots_both_sides_and_strays_drain(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """The rolling-deploy window end to end: after the move, a worker with
        the OLD literal boots without refusing (and without undoing the
        assignment), a worker with the NEW literal boots, and the moved
        backlog drains through the NEW queue's consumers."""
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=3)

        await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        # Old-literal worker boots mid-window — pre-fix: ActorConfigDriftList.
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        # New-literal worker boots.
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_NEW_QUEUE)],
            schema=schema,
        )
        # Neither boot undid the assignment.
        stored: object = await conn.fetchval(
            f'SELECT queue FROM "{schema}".actor_config WHERE actor = $1', _ACTOR
        )
        assert stored == _NEW_QUEUE

        # Old-queue strays drain: a consumer of ONLY the new queue claims the
        # whole moved backlog.
        claimed = await _dispatch(conn, schema, [_NEW_QUEUE], 10)
        assert len(claimed) == 3
        assert all(r["actor"] == _ACTOR for r in claimed)
        assert all(r["queue"] == _NEW_QUEUE for r in claimed)

    async def test_refusals(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        with pytest.raises(ActorNotFoundError):
            await move_actor_queue(conn, "tqm_ghost", _NEW_QUEUE, schema=schema)

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        # A move onto the queue the actor already occupies is a no-op the
        # operator must be told about, not silently executed.
        with pytest.raises(ValueError, match="already"):
            await move_actor_queue(conn, _ACTOR, _OLD_QUEUE, schema=schema)
        # Queue names follow the same rule every producer follows.
        with pytest.raises(ValueError):
            await move_actor_queue(conn, _ACTOR, "bad:name", schema=schema)

    async def test_never_clobbers_an_existing_target_queue_row(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """A configured target queue stands: the carry is fill-in for an
        unconfigured target, never an overwrite of the operator's own setup."""
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await set_queue_mode(conn, _OLD_QUEUE, "round_robin", schema=schema)
        await set_queue_max_concurrent(conn, _OLD_QUEUE, 4, schema=schema)
        # The target is deliberately configured differently.
        await set_queue_mode(conn, _NEW_QUEUE, "strict_fifo", schema=schema)
        await set_queue_max_concurrent(conn, _NEW_QUEUE, 9, schema=schema)

        result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        assert result.queues_row_carried is False
        new_q = await conn.fetchrow(
            f'SELECT mode, max_concurrent FROM "{schema}".queues WHERE name = $1',
            _NEW_QUEUE,
        )
        assert new_q is not None
        assert new_q["mode"] == "strict_fifo"
        assert new_q["max_concurrent"] == 9

    async def test_retry_of_never_claimed_job_is_claimable_on_the_actors_current_queue(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """A job terminalized before it was ever claimed, then retried by an
        operator after the actor moved, must still reach a consumer of the
        actor's CURRENT queue.

        An operator retry is a re-pended tail, not a producer placement: the
        row keeps its original queue label as an audit trail, but dispatch
        routes it by the actor's stored assignment. Operators are told to stop
        consuming the source queue once every producer ships the new literal,
        so a tail that routed by its stale label instead would be stranded
        permanently — pending, due, and invisible to every running consumer.
        """
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        sql = render(schema)

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )

        # A job enqueued (and never claimed) before the move, then cancelled
        # while still pending, so started_at stays NULL.
        job_id = await _enqueue_one(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE)
        cancel_rec = await conn.fetchrow(sql.cancel_pending_scheduled, job_id)
        assert cancel_rec is not None, "setup: job must cancel while still pending"

        started_at = await conn.fetchval(
            f'SELECT started_at FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert started_at is None, "setup: job must never have been claimed"

        # The actor moves. There is no backlog to drain (the job is terminal),
        # so this only flips the assignment.
        await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        # The operator now runs a consumer of the new queue only, and an admin
        # retries the pre-move cancellation.
        retry_rec = await conn.fetchrow(sql.retry_job, job_id)
        assert retry_rec is not None, "retry_job must accept the terminal row"

        row = await conn.fetchrow(
            f'SELECT status, queue, started_at FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None
        assert row["status"] == "pending"

        claimed = await _dispatch(conn, schema, [_NEW_QUEUE], 10)
        claimed_ids = {r["id"] for r in claimed}
        assert job_id in claimed_ids, (
            "admin-retried never-claimed job is stranded on the retired "
            f"source queue {row['queue']!r} instead of being routed to the "
            f"actor's current assignment {_NEW_QUEUE!r}; a consumer of only "
            "the target queue never sees it"
        )

    async def test_move_reports_pending_jobs_still_carrying_the_old_queue(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """The move must report how many pending jobs still carry the source
        queue label, so the operator knows what residual the retired queue's
        consumers still have to serve.

        The move deliberately does not chase every row: a stale producer keeps
        enqueueing to the source queue, and those strays stay served by the
        source queue's consumers. That trade-off is only safe if it is
        *visible* — the operator's decision of when to stop consuming the
        source queue depends on a count, not a guess. The move onto the queue
        the actor already occupies stays a refusal: it is a no-op the operator
        must be told about, and the drain is not the recovery surface for
        producer-placed strays.
        """
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=2)

        result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)
        assert result.jobs_moved == 2

        # A stale producer, still running the old literal, places one more job
        # on the retired source queue after the flip.
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=1)

        rerun_refused = False
        try:
            await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)
        except ValueError:
            rerun_refused = True
        assert rerun_refused, (
            "a move onto the queue the actor already occupies must stay a "
            "refusal; the drain is not the recovery path for producer-placed "
            "strays"
        )

        # The residual the operator must plan for is reported, not inferred.
        residual = getattr(result, "pending_jobs_on_old_queue", None)
        assert residual is not None, (
            "ActorQueueMoveResult must report how many pending jobs still "
            "carry the source queue label; without it the operator has no "
            "supported way to know when the retired queue can stop being "
            "consumed"
        )

    async def test_drain_batch_cost_does_not_grow_as_the_backlog_moves(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """Each drain batch costs the same whether it is the first batch of the
        backlog or the last, so total drain time is linear in the backlog and
        not quadratic.

        The drain re-selects "the next ``batch_size`` of this actor's rows
        still carrying the source queue" on every pass. If the plan fixes only
        one of ``actor``/``queue`` in an index condition and leaves the other
        as a post-scan filter, every later batch re-walks the population that
        earlier batches already rewrote onto the target: batch N pays for the
        (N-1) * batch_size rows already moved. Operationally that is the
        difference between a routine queue move and a drain that keeps blowing
        its own per-batch statement timeout the deeper the backlog gets — and
        it only shows up on the backlogs large enough that an operator most
        needs the command to work.

        Measured as buffers touched, which tracks rows actually visited rather
        than wall clock, so the pin does not turn into a timing flake.
        """
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        batch_size = 20
        total = batch_size * 10

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=total)
        await conn.execute(f'ANALYZE "{schema}".jobs')

        drain_sql = _MOVE_BACKLOG_BATCH_SQL.format(schema=schema)
        per_batch_buffers: list[int] = []
        while True:
            plan_rows = await conn.fetch(
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {drain_sql}",
                _ACTOR,
                _OLD_QUEUE,
                _NEW_QUEUE,
                batch_size,
            )
            plan = json.loads(plan_rows[0][0])[0]["Plan"]
            per_batch_buffers.append(_plan_buffers(plan))
            remaining = await _count_jobs(
                conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, status="pending"
            )
            if remaining == 0:
                break

        assert len(per_batch_buffers) >= 5, (
            f"expected the backlog to drain over several batches; got "
            f"{len(per_batch_buffers)} batches for {total} rows at "
            f"batch_size={batch_size}"
        )
        first, last = per_batch_buffers[0], per_batch_buffers[-1]
        assert last <= first * 3, (
            "the last drain batch touched far more buffers than the first, so "
            "each batch is re-walking the part of the backlog earlier batches "
            "already moved onto the target queue — the quadratic-drain "
            f"mechanism. Buffers per batch: {per_batch_buffers!r}"
        )

    async def test_large_backlog_drains_completely_under_the_per_batch_deadline(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """A backlog far larger than one batch drains to completion without any
        batch hitting its own statement timeout, and every pending and
        scheduled row ends up on the target queue.

        The per-batch deadline is a safety net against a single pathological
        statement, not a ceiling on how much backlog the command can handle:
        the bound belongs to the batch, and the loop keeps going. An operator
        moving a deep queue must get one completed move, not a cancelled
        statement that strands half the backlog on a queue they were told to
        stop consuming.
        """
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn
        total = 400
        scheduled = 25

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=total)
        await _enqueue(
            conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=scheduled, scheduled_at=_FUTURE
        )
        # A neighbour on the same source queue: the drain predicate is scoped
        # to the moving actor, so these must be left exactly where they are.
        await _enqueue(conn, schema, actor=_OTHER_ACTOR, queue=_OLD_QUEUE, count=10)

        result = await move_actor_queue(
            conn, _ACTOR, _NEW_QUEUE, schema=schema, batch_size=25, statement_timeout_ms=5_000
        )

        assert result.jobs_moved == total + scheduled
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, status="pending") == 0
        )
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, status="scheduled") == 0
        )
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="pending")
            == total
        )
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="scheduled")
            == scheduled
        )
        assert (
            await _count_jobs(conn, schema, actor=_OTHER_ACTOR, queue=_OLD_QUEUE, status="pending")
            == 10
        )
        assert (
            await conn.fetchval(
                f'SELECT queue FROM "{schema}".actor_config WHERE actor = $1', _ACTOR
            )
            == _NEW_QUEUE
        )


# ═══════════════════════════════════════════════════════════════════════════════
# CLI tier: `taskq actor-config move-queue ACTOR NEW_QUEUE`
# ═══════════════════════════════════════════════════════════════════════════════


def _patch_move(
    monkeypatch: pytest.MonkeyPatch, result: Any = None, exc: BaseException | None = None
) -> None:
    """Fake the move at the taskq.cli boundary, the diff tests' pattern."""

    async def fake_connect(dsn: str) -> Any:
        class _FakeConn:
            async def close(self) -> None: ...

        return _FakeConn()

    async def fake_move(conn: Any, actor: str, new_queue: str, **kwargs: Any) -> Any:
        if exc is not None:
            raise exc
        return result

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.move_actor_queue", fake_move, raising=False)


def test_cli_move_queue_reports_result(monkeypatch: pytest.MonkeyPatch) -> None:
    moved = ActorQueueMoveResult(
        actor=_ACTOR,
        from_queue=_OLD_QUEUE,
        to_queue=_NEW_QUEUE,
        jobs_moved=3,
        running_jobs_left=1,
        queues_row_carried=True,
    )
    _patch_move(monkeypatch, result=moved)

    result = runner.invoke(app, ["actor-config", "move-queue", _ACTOR, _NEW_QUEUE])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "tqm_old" in result.output
    assert "tqm_new" in result.output
    assert "jobs_moved=3" in result.output
    # The irreducible producer-side residual the operator must plan for.
    assert "tqm_old" in result.stderr


def test_cli_move_queue_unknown_actor_exit_3(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_move(monkeypatch, exc=ActorNotFoundError("tqm_ghost"))

    result = runner.invoke(app, ["actor-config", "move-queue", "tqm_ghost", _NEW_QUEUE])

    assert result.exit_code == 3


def test_cli_move_queue_refusal_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_move(monkeypatch, exc=ValueError("already assigned"))

    result = runner.invoke(app, ["actor-config", "move-queue", _ACTOR, _OLD_QUEUE])

    assert result.exit_code == 2


def test_cli_move_queue_statement_timeout_uses_documented_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A drain-batch statement timeout must surface as one of the command's
    own documented exit codes, never as an unhandled traceback.

    The move's backlog drain runs as bounded batches under a per-batch
    server-side ``statement_timeout``, so a large backlog can abort a batch
    with a Postgres query cancellation. The command documents exactly three
    exit codes — 0 moved, 2 refusal, 3 no stored row — and a raw driver
    error escaping past them turns a bounded, re-runnable drain into an
    unreadable failure for the operator scripting the move.
    """
    _patch_move(
        monkeypatch,
        exc=asyncpg.exceptions.QueryCanceledError("canceling statement due to statement timeout"),
    )

    result = runner.invoke(app, ["actor-config", "move-queue", _ACTOR, _NEW_QUEUE])

    assert result.exit_code in (0, 2, 3), (
        f"move-queue must exit with one of its documented codes (0, 2, 3) on a "
        f"drain-batch statement timeout, not escape uncaught; got exit_code="
        f"{result.exit_code!r} exception={result.exception!r}"
    )
    assert not isinstance(result.exception, asyncpg.exceptions.QueryCanceledError), (
        "QueryCanceledError from a drain-batch statement timeout escaped the CLI "
        "uncaught instead of being translated to a documented exit code"
    )
    # The drain commits per batch, so an aborted run leaves real partial
    # progress: the operator must be told the move is incomplete and
    # re-runnable, not left to infer it from an exit code alone.
    assert "move-queue" in result.stderr or "re-run" in result.stderr, (
        "an aborted drain must tell the operator the move is incomplete and "
        f"safe to re-run; stderr={result.stderr!r}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CLI tier: `taskq queue migrate ACTOR --to QUEUE`
# ═══════════════════════════════════════════════════════════════════════════════
#
# The queue move is a queue-lifecycle operation, and an operator reaching for
# it is thinking about queues, not about the actor_config table it happens to
# be stored in. It is reachable under the `queue` noun with the target named
# by an explicit `--to` option rather than positionally: the two arguments of
# a move are an actor and a queue, and two bare positionals are exactly the
# shape an operator gets backwards under pressure — with the consequence that
# the backlog drains onto the wrong queue.


def test_queue_migrate_moves_the_actor_and_reports_the_move(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The command exists under the queue noun, takes the target as ``--to``,
    and reports the move it performed."""
    moved = ActorQueueMoveResult(
        actor=_ACTOR,
        from_queue=_OLD_QUEUE,
        to_queue=_NEW_QUEUE,
        jobs_moved=3,
        running_jobs_left=1,
        queues_row_carried=True,
    )
    _patch_move(monkeypatch, result=moved)

    result = runner.invoke(app, ["queue", "migrate", _ACTOR, "--to", _NEW_QUEUE])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert _OLD_QUEUE in result.output
    assert _NEW_QUEUE in result.output


def test_queue_migrate_reports_pending_jobs_still_on_the_old_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator's next decision after a move is when to stop consuming the
    retired queue, and the only safe answer is a count.

    The move deliberately leaves producer-placed strays on the source queue —
    a stale producer keeps enqueueing there until its deploy lands — so the
    source queue's consumers must stay up for an interval the command is the
    only thing that can measure. Printing the move without the residual makes
    the retirement a guess, and guessing wrong strands work on a queue nobody
    consumes any more.
    """
    moved = ActorQueueMoveResult(
        actor=_ACTOR,
        from_queue=_OLD_QUEUE,
        to_queue=_NEW_QUEUE,
        jobs_moved=3,
        running_jobs_left=1,
        queues_row_carried=True,
    )
    # The residual field is the move result's own reporting surface; the CLI
    # must surface it rather than leave it to the caller to query by hand.
    object.__setattr__(moved, "pending_jobs_on_old_queue", 2)
    _patch_move(monkeypatch, result=moved)

    result = runner.invoke(app, ["queue", "migrate", _ACTOR, "--to", _NEW_QUEUE])

    combined = result.output + result.stderr
    assert "2" in combined and _OLD_QUEUE in combined, (
        "the command must report how many pending jobs still carry the old "
        f"queue; output={combined!r}"
    )
    assert "reaches zero" in combined, (
        "with a residual outstanding, the command must tell the operator to keep "
        f"the retired queue's consumers running until it drains; output={combined!r}"
    )


def test_queue_migrate_with_zero_residual_omits_the_keep_consuming_advice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A move whose residual is already zero must not advise keeping the
    retired queue's consumers running until the count reaches zero.

    The advice is the operator's next action; emitting it when the count is
    already 0 tells the operator to wait on a condition that already holds —
    and an operator who follows it keeps a retired queue's consumers running
    forever, which is the cost the move exists to retire. The residual count
    itself is still reported: the count is the contract, the advice is
    conditional on there being one.
    """
    moved = ActorQueueMoveResult(
        actor=_ACTOR,
        from_queue=_OLD_QUEUE,
        to_queue=_NEW_QUEUE,
        jobs_moved=3,
        running_jobs_left=0,
        queues_row_carried=True,
    )
    assert moved.pending_jobs_on_old_queue == 0
    _patch_move(monkeypatch, result=moved)

    result = runner.invoke(app, ["queue", "migrate", _ACTOR, "--to", _NEW_QUEUE])

    combined = result.output + result.stderr
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "0" in combined and _OLD_QUEUE in combined, (
        f"the zero residual must still be reported; output={combined!r}"
    )
    assert "reaches zero" not in combined, (
        "advising the operator to keep consumers up until a count that is "
        f"already zero reaches zero is noise that reads as an outstanding "
        f"action; output={combined!r}"
    )


def test_queue_migrate_leaves_no_partial_move_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed migrate must not leave the deployment half-moved.

    The coordinated writes of a move — the stored assignment, the target
    ``queues`` row carrying the source's mode and cap — are what make the
    target queue able to serve the actor at all. Landing the assignment
    without the queues row silently degrades a round_robin queue to
    strict_fifo and drops its cap; landing the queues row without the
    assignment configures a queue nothing routes to. Applied in one
    transaction, a failure leaves the deployment exactly where it started
    and the operator with one action to take: run it again.
    """
    _patch_move(monkeypatch, exc=ValueError("assignment changed concurrently"))

    result = runner.invoke(app, ["queue", "migrate", _ACTOR, "--to", _NEW_QUEUE])

    assert not isinstance(result.exception, ValueError), (
        "a refusal must surface as an exit code, not an escaped traceback"
    )
    # Exit 2 is the move surface's documented refusal code. Pinning the exact
    # code (rather than "non-zero") keeps this from passing vacuously on the
    # exit 2 typer returns for a command it does not recognise.
    assert result.exit_code == 2, (
        f"a concurrent-assignment refusal must exit 2; got {result.exit_code}: {result.output!r}"
    )
    assert "assignment changed concurrently" in (result.output + result.stderr), (
        "the refusal's reason must reach the operator, not just its exit code"
    )


def test_queue_migrate_unknown_actor_uses_the_documented_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An actor with no stored row has nothing to move, and the operator
    scripting a migration needs to tell that apart from a refusal — the
    exit codes are shared with the move surface they wrap."""
    _patch_move(monkeypatch, exc=ActorNotFoundError("tqm_ghost"))

    result = runner.invoke(app, ["queue", "migrate", "tqm_ghost", "--to", _NEW_QUEUE])

    assert result.exit_code == 3


def test_queue_migrate_requires_the_target_queue_to_be_named_explicitly() -> None:
    """Without ``--to``, the command must refuse rather than guess.

    Two bare positionals — an actor and a queue, both plain strings — are
    the shape an operator inverts under pressure, and an inverted move
    drains the backlog onto a queue that was never the target. The explicit
    option is what makes the argument order unmistakable.
    """
    missing_target = runner.invoke(app, ["queue", "migrate", _ACTOR])
    assert missing_target.exit_code != 0, "a migrate with no target queue must not be accepted"

    # The command itself must exist, or the refusal above is just typer
    # rejecting an unknown subcommand and this pin means nothing.
    help_result = runner.invoke(app, ["queue", "migrate", "--help"])
    assert help_result.exit_code == 0, (
        f"`taskq queue migrate` must exist; got {help_result.exit_code}: {help_result.output!r}"
    )
    assert "--to" in help_result.output, (
        "the target queue must be named by an explicit --to option, so the "
        "actor and the queue can never be transposed"
    )
