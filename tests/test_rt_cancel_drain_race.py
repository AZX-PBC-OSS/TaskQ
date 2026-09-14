"""Red-team attacks on the bounded bulk cancel's drain-time races (real PG).

Three properties of ``_cancel_where``'s bounded drain that the two-statement
design promises but no pinned test exercises at DRAIN scale (≥2 batches):

* **EPQ under the drain shape** — a job claimed (``pending→running``, the
  dispatch claim UPDATE shape) by a real second connection *mid-drain*
  must land in cooperative cancel (``cancel_phase=1``, one
  ``cancel_request`` event, never terminal-cancelled), and a job that
  *finishes* (``running→succeeded``) mid-drain must be untouched by the
  running batch.  The single-job EPQ pin exists
  (``test_cancel_where_pg.py::test_pg_cancel_where_does_not_clobber_concurrent_claim``);
  this is the same attack with the claim arriving between two committed
  ps-batches, where a cursor-based or single-snapshot rewrite would
  either miss it or double-cancel it.
* **No-match drains are statement-minimal** — a filter matching nothing
  writes nothing, notifies nothing, and issues exactly the two driving
  statements (one per phase), never an event INSERT.
* **NOTIFY target accumulation across batches** — a running backlog
  larger than one batch produces each running job's notify target
  exactly once, and NULL ``locked_by_worker`` rows are excluded.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    status: str,
    tags: Sequence[str],
) -> None:
    """Seed *job_ids* in one INSERT ... SELECT FROM unnest -- never row by row."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, 'rt_actor', 'default', '{{}}'::jsonb, $2::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', $3::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        status,
        list(tags),
    )


async def _ids_in_drain_order(
    conn: asyncpg.Connection,
    schema: str,
    tag: str,
) -> list[UUID]:
    """The ids in the order the drain's ``ORDER BY id`` CTE admits them."""
    rows = await conn.fetch(
        f'SELECT id FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] ORDER BY id',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above; the tag is $-bound.
        tag,
    )
    return [r["id"] for r in rows]


class _GatedEventConn:
    """Delegates to a real connection, pausing the drain on the Nth event INSERT.

    The pause is the deterministic stand-in for "a dispatch happened
    mid-drain": the gated statement is one of a batch's two event INSERTs,
    so batch k-1 is committed, batch k's driving UPDATE is in flight
    (its row locks held), and every later batch is still unselected — the
    exact window in which a dispatcher claims a job out of a later batch.
    """

    def __init__(self, conn: Any, state: _DrainGateState) -> None:
        self._conn = conn
        self._state = state

    def transaction(self, **kwargs: object) -> Any:
        outer = self

        @asynccontextmanager
        async def _tx() -> AsyncGenerator[None]:
            async with outer._conn.transaction(**kwargs):
                yield

        return _tx()

    async def fetchrow(self, sql: str, *args: object) -> Any:
        return await self._conn.fetchrow(sql, *args)

    async def execute(self, sql: str, *args: object) -> Any:
        if self._state.is_event_insert(sql):
            self._state.event_inserts += 1
            if self._state.event_inserts == self._state.pause_at:
                self._state.entered.set()
                await self._state.release.wait()
        return await self._conn.execute(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _DrainGateState:
    """Pool-level gate bookkeeping — the drain takes a fresh connection per batch."""

    def __init__(self, pause_at: int) -> None:
        self.event_inserts = 0
        self.pause_at = pause_at
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    @staticmethod
    def is_event_insert(sql: str) -> bool:
        upper = sql.lstrip().upper()
        return upper.startswith("INSERT INTO") and ".JOB_EVENTS" in upper


class _GatedPool:
    """Pool stand-in yielding ``_GatedEventConn`` over real pooled connections."""

    def __init__(self, pool: Any, state: _DrainGateState) -> None:
        self._pool = pool
        self.state = state

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[_GatedEventConn]:
        async with self._pool.acquire(**kwargs) as conn:
            yield _GatedEventConn(conn, self.state)


class _CountingConn:
    """Delegates to a real connection, recording every awaited statement."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.statements: list[str] = []
        self.event_inserts = 0

    def _record(self, sql: str) -> None:
        # Full squeezed statement, not a prefix: the driving CTEs open with
        # multi-line SQL comments, so a truncated record would hide every
        # needle the assertions below search for.
        self.statements.append(" ".join(sql.split()))

    async def fetchrow(self, sql: str, *args: object) -> Any:
        self._record(sql)
        return await self._conn.fetchrow(sql, *args)

    async def execute(self, sql: str, *args: object) -> Any:
        self._record(sql)
        if _DrainGateState.is_event_insert(sql):
            self.event_inserts += 1
        return await self._conn.execute(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _CountingPool:
    """Pool stand-in aggregating statement counts across per-batch connections."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self.conns: list[_CountingConn] = []

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[_CountingConn]:
        async with self._pool.acquire(**kwargs) as conn:
            counting = _CountingConn(conn)
            self.conns.append(counting)
            yield counting

    @property
    def statements(self) -> list[str]:
        return [s for c in self.conns for s in c.statements]

    @property
    def event_inserts(self) -> int:
        return sum(c.event_inserts for c in self.conns)


# ── P1-3: EPQ race under the DRAIN shape ─────────────────────────────────


async def test_mid_drain_claim_lands_in_cooperative_cancel_and_finisher_is_untouched(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A claim arriving between committed ps-batches: the claimed job is
    skipped by the ps batch that would have terminalised it (EPQ status
    predicate) and caught by the running drain (fresh snapshot), ending
    ``running`` with ``cancel_phase=1`` and exactly one ``cancel_request``
    event — never terminal-cancelled.  A job that finishes mid-drain
    (``running→succeeded`` on a second connection) is invisible to the
    running batch: no phase, no stamp, no event."""
    schema = module_pg_schema.schema_name
    render(schema)
    # 250 pending jobs → ps batches of 100/100/50; the claim targets a
    # batch-3 job so the claim provably lands between batch 2's commit
    # and batch 3's selection.
    pending_ids = [new_uuid() for _ in range(250)]
    finisher_id = new_uuid()
    await _seed_jobs(clean_pg_conn, schema, pending_ids, status="pending", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, [finisher_id], status="running", tags=["bulk"])
    drain_order = await _ids_in_drain_order(clean_pg_conn, schema, "bulk")
    claim_target = drain_order[230]
    worker_id = new_uuid()

    # Pause inside batch 2's state_change INSERT: batch 1 is committed,
    # batch 2's driving rows are locked in flight, batch 3 unselected.
    state = _DrainGateState(pause_at=3)
    pool = _GatedPool(module_pg_pool, state)
    cancel_task = asyncio.create_task(
        _cancel_where(
            pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
            schema,
            render(schema),
            JobFilter(tags=("bulk",)),
            "offboard",
            batch_size=100,
        )
    )
    await state.entered.wait()

    claim_conn = await module_pg_pool.acquire()
    try:
        # The dispatch claim UPDATE shape: guarded on the pre-claim status,
        # so it proves the row was still pending when the dispatcher took it.
        claim_tag = await claim_conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
            "SET status = 'running', locked_by_worker = $1, started_at = clock_timestamp() "
            "WHERE id = $2 AND status = 'pending'",
            worker_id,
            claim_target,
        )
        assert claim_tag == "UPDATE 1", "the claim must take a still-pending row"
        finish_tag = await claim_conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
            "SET status = 'succeeded', finished_at = clock_timestamp() "
            "WHERE id = $1 AND status = 'running'",
            finisher_id,
        )
        assert finish_tag == "UPDATE 1"
    finally:
        await module_pg_pool.release(claim_conn)
    state.release.set()
    result, notify_targets = await cancel_task

    # The claimed job: cooperative cancel, never terminal.
    claimed = await clean_pg_conn.fetchrow(
        f"SELECT status::text AS status, cancel_phase, cancel_requested_at, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = $1',
        claim_target,
    )
    assert claimed is not None
    assert claimed["status"] == "running", (
        "a job claimed mid-drain must never be terminalised by the ps batch"
    )
    assert claimed["cancel_phase"] == 1, (
        "the running drain's fresh snapshot must catch the mid-drain claim"
    )
    assert claimed["cancel_requested_at"] is not None
    assert claimed["finished_at"] is None

    # The finisher: untouched by the running batch.
    finisher = await clean_pg_conn.fetchrow(
        f"SELECT status::text AS status, cancel_phase, cancel_requested_at, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = $1',
        finisher_id,
    )
    assert finisher is not None
    assert finisher["status"] == "succeeded"
    assert finisher["cancel_phase"] == 0, "a finished job is not a cancel target"
    assert finisher["cancel_requested_at"] is None

    # Totals: 249 terminal + 1 cooperative, exactly once each.
    assert result.cancelled_directly == 249
    assert result.cancel_requested == 1
    assert set(result.cancelled_ids) == set(pending_ids) - {claim_target}
    assert result.cancel_requested_ids == (claim_target,)

    # Events: the claimed job gets one cancel_request and no state_change;
    # the finisher gets nothing; every directly-cancelled job gets one of
    # each kind.
    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, detail FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE job_id = ANY($1::uuid[])",
        [claim_target, finisher_id],
    )
    assert [dict(e)["kind"] for e in events] == ["cancel_request"]
    assert parse_detail(events[0]["detail"]) == {"reason": "offboard"}

    per_job = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "GROUP BY job_id, kind"
    )
    assert {r["kind"] for r in per_job} == {"state_change", "cancel_request"}
    assert all(r["n"] == 1 for r in per_job), "exactly one event of each kind per job"
    assert len(per_job) == 2 * 249 + 1

    # NOTIFY: exactly the claimed job, on its claiming worker.
    assert [(t.job_id, t.worker_id) for t in notify_targets] == [(claim_target, worker_id)]


# ── P1-8: empty / no-match filters ───────────────────────────────────────


async def test_no_match_filter_writes_nothing_and_issues_only_the_two_driving_statements(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A filter matching nothing: all-zero result, zero events, zero notify
    targets, and exactly the two driving statements (one per phase) — the
    short-batch termination must not re-loop an empty match set."""
    schema = module_pg_schema.schema_name
    render(schema)
    keep_ids = [new_uuid() for _ in range(10)]
    await _seed_jobs(clean_pg_conn, schema, keep_ids, status="pending", tags=["keep"])

    pool = _CountingPool(module_pg_pool)
    result, notify_targets = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("matches-nothing",)),
        "offboard",
        batch_size=100,
    )

    assert result.cancelled_directly == 0
    assert result.cancel_requested == 0
    assert result.cancelled_ids == ()
    assert result.cancel_requested_ids == ()
    assert result.total_affected == 0
    assert notify_targets == []

    events: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert events == 0
    untouched = await clean_pg_conn.fetch(
        f"SELECT status::text AS status, finished_at, cancel_requested_at, cancel_phase "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
        keep_ids,
    )
    assert len(untouched) == len(keep_ids)
    assert all(r["status"] == "pending" for r in untouched)
    assert all(r["finished_at"] is None for r in untouched)
    assert all(r["cancel_requested_at"] is None for r in untouched)
    assert all(r["cancel_phase"] == 0 for r in untouched)

    driving = [s for s in pool.statements if "WITH matching AS MATERIALIZED" in s]
    assert len(driving) == 2, (
        f"a no-match drain must issue exactly the two driving statements; saw {pool.statements}"
    )
    assert pool.event_inserts == 0, "no matched rows means no event INSERT at all"


async def test_no_match_actor_filter_is_equally_minimal(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Same property through a different filter shape: an actor with no jobs."""
    schema = module_pg_schema.schema_name
    render(schema)
    keep_ids = [new_uuid() for _ in range(5)]
    await _seed_jobs(clean_pg_conn, schema, keep_ids, status="scheduled", tags=["keep"])

    pool = _CountingPool(module_pg_pool)
    result, notify_targets = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(actor="ghost_actor_never_registered"),
        None,
        batch_size=100,
    )

    assert result.total_affected == 0
    assert notify_targets == []
    assert len([s for s in pool.statements if "WITH matching AS MATERIALIZED" in s]) == 2
    assert pool.event_inserts == 0


# ── P2-10: NOTIFY target accumulation across batches ─────────────────────


async def test_notify_targets_accumulate_exactly_once_across_running_batches(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A running backlog spanning 3 running batches: every running job with
    a worker appears exactly once in the notify targets (no per-batch
    duplication, no cross-batch loss), and the NULL-worker rows are
    requested but excluded from the fan-out."""
    schema = module_pg_schema.schema_name
    render(schema)
    worker_id = new_uuid()
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "VALUES ($1, 'rt-host', 1, ARRAY['default'])",
        worker_id,
    )
    owned_ids = [new_uuid() for _ in range(250)]
    null_worker_ids = [new_uuid() for _ in range(10)]
    await _seed_jobs(clean_pg_conn, schema, owned_ids, status="running", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, null_worker_ids, status="running", tags=["bulk"])
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET locked_by_worker = $1 WHERE id = ANY($2::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        worker_id,
        owned_ids,
    )

    result, notify_targets = await _cancel_where(
        module_pg_pool,
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
        batch_size=100,
    )

    assert result.cancel_requested == 260, "every running job is requested, worker or not"
    assert result.cancelled_directly == 0
    target_jobs = [t.job_id for t in notify_targets]
    assert len(target_jobs) == len(set(target_jobs)) == 250, (
        "260 running jobs across 3 batches must yield exactly 250 targets — one per "
        "owned job, no duplicates, none lost at a batch boundary"
    )
    assert set(target_jobs) == set(owned_ids)
    assert {t.worker_id for t in notify_targets} == {worker_id}
    assert set(result.cancel_requested_ids) == set(owned_ids) | set(null_worker_ids)
