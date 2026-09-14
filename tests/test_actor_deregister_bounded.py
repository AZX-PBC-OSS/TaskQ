"""force=True actor deregistration must not do unbounded work in one transaction.

``deregister_actor``'s force branch (``src/taskq/actor_config_ops.py``) runs
one unbounded cancel UPDATE over every pending/scheduled job of the actor
plus one ``executemany`` event INSERT per cancelled job, all inside the
single ``conn.transaction()`` that also performs the schedule disable, the
``actor_config`` delete, and the optional purge.  For an actor with a large
backlog that transaction holds row locks on the whole match set for its
whole duration and inserts one ``job_events`` row per job — precisely the
"abnormally large batch inserted in one transaction" that
``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``'s docstring names as a
way to silently miss a reclaim event.

The contract these tests encode: a force deregistration still cancels
EVERY pending/scheduled job for the actor and still reports
``jobs_cancelled`` for all of them — but the cancel drains as bounded
committed batches (``batch_size`` driving rows per transaction), each
batch writing its ``state_change`` events as ONE batched ``unnest`` INSERT
inside the same transaction as its driving UPDATE.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import deregister_actor
from taskq.backend._sql_templates import render
from taskq.exceptions import ActorNotFoundError
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.startup import sync_actor_config

pytestmark = pytest.mark.integration

# Large enough that an unbounded implementation is unambiguously unbounded,
# small enough to seed and deregister quickly.  Mixed pending/scheduled so
# the per-row from_state assertion proves the batched event INSERT carries
# each row's REAL prior status, not one shared detail.
_PENDING = 150
_SCHEDULED = 100
_BACKLOG = _PENDING + _SCHEDULED

# The bound under test: no committed transaction may mutate more than this
# many driving rows, and no single event INSERT may carry more than this
# many ids.
_BATCH = 100


# ── Seeding ──────────────────────────────────────────────────────────────


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    status: str,
    actor: str,
) -> None:
    """Seed *job_ids* in one INSERT ... SELECT FROM unnest -- never row by row."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, $2, 'default', '{{}}'::jsonb, $3::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', ARRAY[]::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        actor,
        status,
    )


# ── Counting instrumentation ─────────────────────────────────────────────


class _CountingConn:
    """Delegates to a real connection, recording per-transaction write volume.

    Same measurement philosophy as ``tests/test_cancel_where_bounded.py``:
    RTT-independent counting, never a clock.  Two things are recorded:

    * ``tx_rows_updated`` -- rows mutated by each driving cancel statement,
      taken from that statement's own RETURNING aggregate
      (``cancelled_directly``) rather than by parsing SQL, so a rewritten
      CTE is measured the same way.
    * ``event_batches`` / ``event_insert_statements`` -- the size and count
      of every write into ``job_events``, so a bounded implementation is
      visible as bounded batched INSERTs, and a per-row ``executemany``
      rewrite is visible as ``executemany_calls``.

    Transaction boundaries are recorded by wrapping ``transaction()``: what
    the defect is about is work *per transaction*, not work in total.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        # One entry per transaction opened on this connection.
        self.tx_rows_updated: list[int] = []
        self.tx_event_rows: list[int] = []
        self.event_batches: list[int] = []
        self.event_insert_statements = 0
        self.executemany_calls = 0
        self._depth = 0

    # -- transaction boundary ------------------------------------------

    def transaction(self, **kwargs: object) -> Any:
        outer = self

        @asynccontextmanager
        async def _tx() -> AsyncGenerator[None]:
            if outer._depth == 0:
                outer.tx_rows_updated.append(0)
                outer.tx_event_rows.append(0)
            outer._depth += 1
            try:
                async with outer._conn.transaction(**kwargs):
                    yield
            finally:
                outer._depth -= 1

        return _tx()

    def _note_rows(self, n: int) -> None:
        if self.tx_rows_updated:
            self.tx_rows_updated[-1] += n
        else:  # pragma: no cover - only if a write escapes a transaction
            self.tx_rows_updated.append(n)

    def _note_events(self, n: int) -> None:
        self.event_batches.append(n)
        if self.tx_event_rows:
            self.tx_event_rows[-1] += n
        else:  # pragma: no cover - only if a write escapes a transaction
            self.tx_event_rows.append(n)

    # -- instrumented statement surface --------------------------------

    async def fetchrow(self, sql: str, *args: object) -> Any:
        row = await self._conn.fetchrow(sql, *args)
        if row is not None:
            # Count rows the driving cancel statement actually mutated,
            # from its own RETURNING aggregate -- independent of how the
            # CTE is spelled.  ``Record.get`` (not ``[...]``) so a
            # statement that returns no such column -- e.g. the batch's
            # statement_timeout capture -- is simply skipped instead of
            # raising.
            value = row.get("cancelled_directly") if hasattr(row, "get") else None
            if isinstance(value, int):
                self._note_rows(value)
        return row

    async def execute(self, sql: str, *args: object, **kwargs: object) -> Any:
        if _is_event_insert(sql):
            # A batched write spells the INSERT as one execute() carrying a
            # uuid[]; the batch size is the length of that array.
            self.event_insert_statements += 1
            self._note_events(_uuid_array_len(args))
        return await self._conn.execute(sql, *args, **kwargs)

    async def executemany(self, sql: str, args: Sequence[Any], **kwargs: object) -> Any:
        self.executemany_calls += 1
        return await self._conn.executemany(sql, list(args), **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _is_event_insert(sql: str) -> bool:
    upper = sql.lstrip().upper()
    return upper.startswith("INSERT INTO") and ".JOB_EVENTS" in upper


def _uuid_array_len(args: Sequence[object]) -> int:
    for arg in args:
        if isinstance(arg, list | tuple) and all(isinstance(v, UUID) for v in arg) and arg:
            return len(arg)
    return 1


# ══════════════════════════════════════════════════════════════════════════
# The bounded contract: every matching job cancelled, in bounded committed
# batches, with batched event INSERTs and an intact audit trail.
# ══════════════════════════════════════════════════════════════════════════


async def test_force_deregister_drains_backlog_in_bounded_batches(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """force=True cancels the whole backlog, but never in one unbounded
    transaction, and its events are batched INSERTs with real per-row
    ``from_state``."""
    schema = module_pg_schema.schema_name
    render(schema)
    pending_ids = [new_uuid() for _ in range(_PENDING)]
    scheduled_ids = [new_uuid() for _ in range(_SCHEDULED)]
    await _seed_jobs(clean_pg_conn, schema, pending_ids, status="pending", actor="bulk_actor")
    await _seed_jobs(clean_pg_conn, schema, scheduled_ids, status="scheduled", actor="bulk_actor")
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="bulk_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )

    counting = _CountingConn(clean_pg_conn)
    result = await deregister_actor(
        counting, "bulk_actor", force=True, schema=schema, batch_size=_BATCH
    )

    # Completeness: every pending/scheduled job is cancelled and reported.
    assert result.jobs_cancelled == _BACKLOG, "every matching job must still be cancelled"
    assert result.actor_config_deleted is True
    statuses = await clean_pg_conn.fetch(
        f'SELECT status::text AS status FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert len(statuses) == _BACKLOG
    assert {row["status"] for row in statuses} == {"cancelled"}, (
        "the bounded drain must be complete, not best-effort"
    )

    # Bounded: no single transaction mutated more than batch_size driving
    # rows, and covering the backlog took more than one batch.
    assert max(counting.tx_rows_updated, default=0) <= _BATCH, (
        f"one transaction mutated {max(counting.tx_rows_updated, default=0)} driving rows — "
        f"the force-path cancel still has no LIMIT, so row locks cover the entire "
        f"backlog for the whole transaction"
    )
    driving_txs = [n for n in counting.tx_rows_updated if n > 0]
    assert len(driving_txs) >= 3, (
        f"cancelling {_BACKLOG} rows at batch_size={_BATCH} needs at least 3 committed "
        f"batches; saw {len(driving_txs)} — nothing is being drained"
    )

    # Batched event writes: no per-row executemany; the events land as
    # bounded batched INSERTs.
    assert counting.executemany_calls == 0, (
        "the state_change events are still written one executemany row at a time"
    )
    assert counting.event_insert_statements >= 3, (
        f"a {len(driving_txs)}-batch drain must write at least one batched event "
        f"INSERT per batch; saw {counting.event_insert_statements}"
    )
    assert max(counting.event_batches, default=0) <= _BATCH, (
        f"one event INSERT covered {max(counting.event_batches, default=0)} rows — "
        f"the batch cap is not applied to the event write"
    )

    # Audit trail: exactly one state_change per job, each carrying its row's
    # REAL prior status and the deregistration reason.
    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, detail FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    state_changes = [e for e in events if e["kind"] == "state_change"]
    assert len(state_changes) == _BACKLOG, "exactly one state_change per cancelled job"
    expected_from: dict[UUID, str] = dict.fromkeys(pending_ids, "pending")
    expected_from.update(dict.fromkeys(scheduled_ids, "scheduled"))
    for row in state_changes:
        jid = row["job_id"]
        assert parse_detail(row["detail"]) == {
            "from_state": expected_from[jid],
            "to_state": "cancelled",
            "reason": "actor_deregistered",
        }, (
            f"state_change detail for {jid} must carry that job's ACTUAL previous status "
            f"({expected_from[jid]!r}); a batched rewrite that shares one detail across "
            f"the whole unnest would hardcode a single from_state here"
        )

    # Both previous statuses must actually be represented — otherwise the
    # mixed-seed premise of this test has silently evaporated.
    observed = {parse_detail(r["detail"])["from_state"] for r in state_changes}
    assert observed == {"pending", "scheduled"}, (
        "the seed must produce both from_state values, or this test proves nothing"
    )


async def test_rerun_force_deregister_after_drain_raises_not_found(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A second force deregister after a completed drain raises ActorNotFoundError.

    This is the primary consumer pattern (cleanup loops using
    try/except ActorNotFoundError) and the re-run safety property of the
    bounded drain: the per-batch re-selection is EPQ-safe, so nothing is
    double-cancelled or double-logged even if the row still existed.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(5)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", actor="again_actor")
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="again_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )

    first = await deregister_actor(
        clean_pg_conn, "again_actor", force=True, schema=schema, batch_size=2
    )
    assert first.jobs_cancelled == 5
    events_after_first = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert events_after_first == 5

    with pytest.raises(ActorNotFoundError, match="no stored actor_config row"):
        await deregister_actor(clean_pg_conn, "again_actor", force=True, schema=schema)

    events_after_second = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert events_after_second == events_after_first, "no duplicate events on the re-run"
    statuses = await clean_pg_conn.fetch(
        f'SELECT status::text AS status FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert {row["status"] for row in statuses} == {"cancelled"}
