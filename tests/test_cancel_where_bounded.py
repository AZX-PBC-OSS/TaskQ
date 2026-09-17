"""Bulk cancel must not do unbounded work inside a single transaction.

``_cancel_where`` (``src/taskq/backend/_cancel_bulk.py``) cancels every
job matching a filter as a sequence of bounded committed batches: each
batch's driving CTE windows at most ``batch_size`` rows (MATERIALIZED
``matching`` + ``LIMIT``), the batch's ``job_events`` writes land as one
batched ``unnest`` INSERT per event kind inside the same transaction as
the driving UPDATE, and the drain terminates on the window count the
statement itself returns.  The two layers below pin that contract:

* **Layer 1 — the boundedness contract.**  No single transaction may
  mutate more than a bound well below the backlog, no transaction may
  insert an unbounded number of ``job_events`` rows, and the event
  writes must not take one round trip per row.  This is the property
  ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY`` depends on: its
  trailing-watermark guarantee for ``poll_reclaim_events`` holds only
  while no ``job_events`` writer takes longer than the margin between
  its INSERT and its COMMIT, and it names "an abnormally large batch
  inserted in one transaction" as a known way to violate it — the
  consequence being a **silently missed event** (a lower-``id`` row
  committing after the consumer's cursor has already advanced past its
  position, with no error raised anywhere).  A bulk cancel is squarely
  inside that class of writers, so its per-transaction volume is bounded
  and each batch carries a server-side ``statement_timeout`` as
  enforcement.
* **Layer 2 — the behaviour pins.**  Completeness (every matching job
  reaches terminal ``cancelled``), containment (non-matching jobs are
  bit-for-bit untouched), per-row ``from_state`` in the
  ``state_change`` detail, per-row ``occurred_at`` co-monotonic with
  ``job_events.id``, running jobs get cooperative cancel (never
  terminal), re-runs are idempotent, and the returned counts equal the
  rows actually cancelled.  Completeness includes the mid-drain re-pend
  dimension (#237): a matching running row moved back to pending /
  scheduled BEHIND the pending arm's keyset cursor — a crash reclaim, a
  denial snooze, a shutdown interrupt, a consumer retry — must still be
  cancelled by the same call, and the two-arm drain must terminate as a
  bounded fixpoint (a first empty round stops it; a hard round cap
  bounds it under sustained churn), never an unbounded loop.

Counting mutated rows and event rows per transaction — from the driving
statement's own RETURNING aggregates — rather than wall-clock seconds
keeps every assertion RTT-independent and deterministic in any
environment, the same discipline ``test_dispatch_event_batching`` and
``test_sweep_scheduled_to_pending_batching`` use for round trips.

Bounding this path was an API-contract decision, not a mechanical
refactor: ``cancel_where`` still cancels everything matching (the
``limit``/``cursor``/``order_by`` filter fields remain ignored for bulk
writes), but across several committed transactions — the operation is
deliberately non-atomic, and a concurrent enqueue can slip a new
matching row in between batches.  The EPQ-safe predicates and the
window-count termination keep the drain complete and exactly-once
regardless; a mid-operation failure leaves the committed batches as
partial progress and a re-run resumes where it stopped.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import (
    _MAX_CANCEL_DRAIN_ROUNDS,
    _UUID_MIN,
    _cancel_where,
)
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.backend.postgres import PostgresBackend
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

# Large enough that an unbounded implementation is unambiguously unbounded and
# small enough to seed and cancel quickly.  Any bounded implementation must
# split this across more than one transaction under a sane cap.
_BACKLOG = 400

# The cap a bounded implementation must respect.  Deliberately generous: the
# assertion is "some bound well below the backlog is honoured", not "exactly
# this number".  A fix that chunks at 100, 250 or 1000 all pass at _BACKLOG=400
# only if the chunk is smaller than the backlog -- which is the property under
# test.  See the layer-1 docstrings for why the number itself is an API
# decision, not something this test gets to dictate.
_MAX_ROWS_PER_TX = _BACKLOG // 2


# ── Seeding ──────────────────────────────────────────────────────────────


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    status: str,
    tags: Sequence[str],
    actor: str = "test_actor",
) -> None:
    """Seed *job_ids* in one INSERT ... SELECT FROM unnest -- never row by row."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, $2, 'default', '{{}}'::jsonb, $3::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', $4::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        actor,
        status,
        list(tags),
    )


# ── Counting instrumentation ─────────────────────────────────────────────


class _CountingConn:
    """Delegates to a real connection, recording per-transaction write volume.

    Two things are recorded, and both are RTT-independent -- the assertions
    below never look at a clock, so they are deterministic in any environment:

    * ``tx_rows_updated`` -- rows mutated by each ``fetchrow`` of a driving
      cancel CTE, taken from that statement's own RETURNING counts rather than
      by parsing SQL, so a rewritten CTE is measured the same way.
    * ``event_batches`` -- the size of every ``executemany``/``execute`` write
      into ``job_events``, so a bounded implementation is visible as bounded
      batches whichever spelling it uses (``executemany`` over a list, or a
      single ``unnest`` statement carrying a ``uuid[]``).

    Transaction boundaries are recorded by wrapping ``transaction()``: what the
    defect is about is work *per transaction*, not work in total.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        # One entry per transaction opened on this connection.
        self.tx_rows_updated: list[int] = []
        self.tx_event_rows: list[int] = []
        self.event_batches: list[int] = []
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
            # Count rows the driving CTE actually mutated, from its own
            # RETURNING aggregates -- independent of how the CTE is spelled.
            # ``Record.get`` (not ``[...]``) so a statement that returns
            # neither column -- e.g. a chunked rewrite's own bookkeeping
            # query -- is simply skipped instead of raising.
            for key in ("cancelled_directly", "cancel_requested"):
                value = row.get(key) if hasattr(row, "get") else None
                if isinstance(value, int):
                    self._note_rows(value)
        return row

    async def executemany(self, sql: str, args: Sequence[Any], **kwargs: object) -> Any:
        rows = list(args)
        if _is_event_insert(sql):
            self.executemany_calls += 1
            self._note_events(len(rows))
        return await self._conn.executemany(sql, rows, **kwargs)

    async def execute(self, sql: str, *args: object, **kwargs: object) -> Any:
        if _is_event_insert(sql):
            # A batched fix spells the write as one execute() carrying a
            # uuid[]; the batch size is the length of that array.
            self._note_events(_uuid_array_len(args))
        return await self._conn.execute(sql, *args, **kwargs)

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


class _CountingPool:
    """Pool stand-in yielding ``_CountingConn`` over real pooled connections.

    ``_cancel_where`` takes a pool, not a connection, and acquires inside its
    retry loop -- so a chunked implementation is free to take a fresh
    connection per chunk.  Every acquired connection is retained so the
    assertions can aggregate across all of them.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self.conns: list[_CountingConn] = []

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[_CountingConn]:
        async with self._pool.acquire(**kwargs) as conn:
            counting = _CountingConn(conn)
            self.conns.append(counting)
            yield counting

    # -- aggregate views used by the assertions -------------------------

    @property
    def transactions(self) -> int:
        return sum(len(c.tx_rows_updated) for c in self.conns)

    @property
    def max_rows_in_one_tx(self) -> int:
        return max((max(c.tx_rows_updated, default=0) for c in self.conns), default=0)

    @property
    def max_event_rows_in_one_tx(self) -> int:
        return max((max(c.tx_event_rows, default=0) for c in self.conns), default=0)

    @property
    def max_event_batch(self) -> int:
        return max((max(c.event_batches, default=0) for c in self.conns), default=0)

    @property
    def executemany_calls(self) -> int:
        return sum(c.executemany_calls for c in self.conns)


# ══════════════════════════════════════════════════════════════════════════
# LAYER 1 — the boundedness contract.  These assert the properties the
# bounded implementation enforces; a regression to an unbounded single
# transaction (no LIMIT on the matching CTE, per-row event writes) fails
# here.
#
# Contract context: making ``cancel_where`` bounded was an API-CONTRACT
# DECISION, not a mechanical refactor — the documented contract
# (``src/taskq/backend/_protocol.py`` on ``JobFilter``: "for
# ``cancel_where``, the ``limit``, ``cursor`` and ``order_by`` fields are
# ignored — a bulk cancel is not paginated") now means "cancel everything
# matching, but in bounded committed chunks", not "in one transaction".
# The operation is deliberately non-atomic: a concurrent enqueue can slip
# a new matching row in between batches, and a mid-operation failure
# leaves partial progress a re-run continues from.  These tests take that
# position because it preserves the existing return contract
# (``cancelled_directly`` still counts every job cancelled) while keeping
# every ``job_events`` writer inside the
# ``RECLAIM_EVENT_VISIBILITY_DELAY`` margin.  If the project decides
# otherwise, change these tests deliberately and say so in the commit; do
# not silently relax them.
# ══════════════════════════════════════════════════════════════════════════


async def test_bulk_cancel_does_not_update_the_whole_backlog_in_one_transaction(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: one transaction must not touch the whole match set.

    The bounded drain commits the match set in batches of at most the
    batch size: no single transaction mutates more than
    ``_MAX_ROWS_PER_TX`` job rows, and covering a backlog larger than the
    cap therefore takes more than one committed transaction — so row
    locks are never held on the entire match set for one transaction's
    duration.

    Counting mutated rows (from the driving statement's own RETURNING
    aggregates) rather than wall-clock seconds keeps the assertion
    RTT-independent and deterministic -- the same reason
    ``test_dispatch_event_batching`` and
    ``test_sweep_scheduled_to_pending_batching`` count round trips.
    """
    schema = module_pg_schema.schema_name
    render(schema)  # validates the schema identifier interpolated below
    job_ids = [new_uuid() for _ in range(_BACKLOG)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", tags=["bulk"])

    pool = _CountingPool(module_pg_pool)
    result, _notify = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert result.cancelled_directly == _BACKLOG, "every matching job must still be cancelled"
    assert pool.max_rows_in_one_tx <= _MAX_ROWS_PER_TX, (
        f"one transaction mutated {pool.max_rows_in_one_tx} job rows out of a "
        f"{_BACKLOG}-row backlog — the driving CTE still has no LIMIT, so row locks "
        f"are held on the entire match set for the whole transaction, and at production "
        f"backlog sizes that transaction is the 'abnormally large batch inserted in one "
        f"transaction' that RECLAIM_EVENT_VISIBILITY_DELAY's docstring names as a cause "
        f"of a silently missed reclaim event"
    )
    assert pool.transactions > 1, (
        f"cancelling {_BACKLOG} rows under a {_MAX_ROWS_PER_TX}-row cap must commit in "
        f"more than one transaction; saw {pool.transactions} — nothing is being chunked"
    )


async def test_bulk_cancel_event_writes_are_bounded_per_transaction(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: no transaction may insert an unbounded number of
    ``job_events`` rows.

    This is the assertion that ties the site to the critical invariant.
    ``RECLAIM_EVENT_VISIBILITY_DELAY`` (2 seconds,
    ``src/taskq/constants.py``) guarantees ``poll_reclaim_events`` never skips
    a lower-``id`` event ONLY while every ``job_events`` writer commits within
    that margin; its docstring names "an abnormally large batch inserted in one
    transaction" as a known violation, and the consequence as a **silently
    missed event** -- no error raised anywhere.

    A bulk cancel writes TWO event rows per directly-cancelled job (a
    ``state_change`` and a ``cancel_request``), so bounding the rows per
    transaction bounds the commit latency, which is what the invariant
    actually needs.  The assertion counts rows, not seconds, so it does
    not depend on how fast the test machine's Postgres is.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(_BACKLOG)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", tags=["bulk"])

    pool = _CountingPool(module_pg_pool)
    result, _notify = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert result.cancelled_directly == _BACKLOG
    written = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert written == 2 * _BACKLOG, "two events per cancelled job must still be written in total"

    # 2 events per job, so the per-transaction event budget scales with the
    # per-transaction row budget.
    assert pool.max_event_rows_in_one_tx <= 2 * _MAX_ROWS_PER_TX, (
        f"one transaction inserted {pool.max_event_rows_in_one_tx} job_events rows for a "
        f"{_BACKLOG}-row backlog — RECLAIM_EVENT_VISIBILITY_DELAY's 2s margin assumes no "
        f"job_events writer holds a transaction open longer than that between INSERT and "
        f"COMMIT, and an unbounded bulk cancel is exactly the 'abnormally large batch "
        f"inserted in one transaction' its docstring names; the consequence is a silently "
        f"missed reclaim event"
    )


async def test_bulk_cancel_does_not_issue_one_round_trip_per_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: the event writes must not take one round trip per row.

    ``executemany`` is not one round trip: asyncpg pipelines the bind/execute
    pairs but still sends one Bind+Execute message per argument tuple and waits
    for the whole sequence, so the work inside the transaction scales linearly
    with the match count either way.

    Every ``cancel_request`` row shares one ``cr_detail`` entirely (one
    ``reason`` computed once per call), and the ``state_change`` detail
    differs per row (``prev_statuses[jid]`` is ``'pending'`` for some
    rows and ``'scheduled'`` for others, carried as a second ``unnest``
    column by the two-column batch template).  Both land as one batched
    ``unnest`` INSERT per event kind per batch — the same
    ``INSERT_EVENTS_DETAIL_BATCH_SQL`` shape the dispatch path uses.

    That is why the assertion is on the batch size, not on statement
    identity: it is satisfied by any shape that stops sending one
    parameter tuple per row.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(_BACKLOG)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", tags=["bulk"])

    pool = _CountingPool(module_pg_pool)
    result, _notify = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert result.cancelled_directly == _BACKLOG
    assert pool.max_event_batch <= _MAX_ROWS_PER_TX, (
        f"a single event write covered {pool.max_event_batch} rows (executemany calls: "
        f"{pool.executemany_calls}) — the per-row parameter tuples are still being sent "
        f"for the entire unbounded match set inside one transaction"
    )


async def test_bulk_cancel_of_running_jobs_is_also_bounded(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 1: statement 2 (running → cooperative cancel) is bounded too.

    The running arm has the same shape as the pending/scheduled arm: a
    windowed driving CTE plus one batched ``cancel_request`` event INSERT
    per batch.  A regression that only bounds statement 1 leaves half
    the lock hold in place, so this pins the running arm separately.

    ``locked_by_worker`` is left NULL here on purpose: that suppresses the
    post-commit NOTIFY fan-out (``notify_targets`` filters ``wid is not
    None``) so the test measures the transaction, not the notification
    path.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(_BACKLOG)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="running", tags=["bulk"])

    pool = _CountingPool(module_pg_pool)
    result, notify_targets = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert result.cancel_requested == _BACKLOG, "every running job must still be cancel-requested"
    assert result.cancelled_directly == 0
    assert notify_targets == [], "no locked_by_worker means no NOTIFY targets"
    assert pool.max_rows_in_one_tx <= _MAX_ROWS_PER_TX, (
        f"one transaction set cancel_phase=1 on {pool.max_rows_in_one_tx} running rows out "
        f"of {_BACKLOG} — statement 2's matching CTE has no LIMIT either, so chunking only "
        f"statement 1 would leave this half of the lock hold untouched"
    )
    assert pool.max_event_rows_in_one_tx <= _MAX_ROWS_PER_TX, (
        f"one transaction inserted {pool.max_event_rows_in_one_tx} cancel_request events "
        f"for {_BACKLOG} running jobs"
    )


# ══════════════════════════════════════════════════════════════════════════
# LAYER 2 — CORRECTNESS PINNING.  Every test below PASSES NOW and MUST STILL
# PASS AFTER the fix.  The fix rewrites SQL that mutates job state and writes
# the audit log, so these pin the observable behaviour that must not change.
# ══════════════════════════════════════════════════════════════════════════


async def test_pins_every_matching_job_reaches_cancelled_and_others_are_untouched(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded drain must preserve.

    Completeness and containment: every job matching the predicate ends in
    terminal ``cancelled`` with ``finished_at`` stamped, and every job that does
    NOT match is bit-for-bit untouched -- same status, no ``finished_at``, no
    ``cancel_requested_at``, ``cancel_phase`` still 0, and not one ``job_events``
    row written against it.

    A chunked rewrite is exactly where completeness is at risk (a cursor that
    drops rows at a chunk boundary, or an ``ORDER BY`` that no longer covers
    the match set), and a rewritten predicate is exactly where containment is
    at risk -- so both are pinned here rather than assumed.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    matching = [new_uuid() for _ in range(60)]
    other_tag = [new_uuid() for _ in range(10)]
    other_status = [new_uuid() for _ in range(10)]
    await _seed_jobs(clean_pg_conn, schema, matching, status="pending", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, other_tag, status="pending", tags=["keep"])
    await _seed_jobs(clean_pg_conn, schema, other_status, status="succeeded", tags=["bulk"])

    result, _notify = await _cancel_where(
        _CountingPool(module_pg_pool),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert result.cancelled_directly == len(matching)
    assert set(result.cancelled_ids) == set(matching)
    assert result.cancel_requested == 0
    assert result.cancel_requested_ids == ()

    cancelled = await clean_pg_conn.fetch(
        f'SELECT id, status, finished_at FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        matching,
    )
    assert len(cancelled) == len(matching)
    assert {r["status"] for r in cancelled} == {"cancelled"}, "every matching job must be cancelled"
    assert all(r["finished_at"] is not None for r in cancelled), "finished_at must be stamped"

    untouched = await clean_pg_conn.fetch(
        "SELECT id, status, finished_at, cancel_requested_at, cancel_phase "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[]) ORDER BY id',
        other_tag + other_status,
    )
    assert len(untouched) == len(other_tag) + len(other_status)
    by_id = {r["id"]: r for r in untouched}
    for jid in other_tag:
        assert by_id[jid]["status"] == "pending", "a non-matching tag must not be cancelled"
    for jid in other_status:
        assert by_id[jid]["status"] == "succeeded", "a terminal job must not be re-cancelled"
    for row in untouched:
        assert row["finished_at"] is None or row["status"] == "succeeded"
        assert row["cancel_requested_at"] is None, "non-matching jobs keep cancel_requested_at NULL"
        assert row["cancel_phase"] == 0, "non-matching jobs keep cancel_phase 0"

    stray = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        other_tag + other_status,
    )
    assert stray == 0, "no events may be written against jobs the filter did not match"


async def test_pins_state_change_from_state_is_each_jobs_actual_previous_status(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded drain must preserve.

    The ``state_change`` batch carries a PER-ROW detail: ``from_state`` comes
    from ``prev_statuses[jid]``, which is that job's own status as read
    by the ``matching`` CTE -- ``'pending'`` for some rows, ``'scheduled'`` for
    others, in the same call.

    That is the exact thing a careless batching rewrite breaks: collapsing this
    into a shared-detail batch (one ``$3::jsonb`` detail for the whole
    ``unnest``) would silently stamp ONE ``from_state`` on every event and
    corrupt the audit log for half the rows, with every count assertion still
    passing.  The mix seeded here (pending AND scheduled, cancelled in one call)
    is what catches it.

    Also pinned: exactly one ``state_change`` and exactly one ``cancel_request``
    per cancelled job, ``to_state='cancelled'`` on every state change, and the
    ``reason`` carried verbatim on every cancel request.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    pending_ids = [new_uuid() for _ in range(25)]
    scheduled_ids = [new_uuid() for _ in range(25)]
    await _seed_jobs(clean_pg_conn, schema, pending_ids, status="pending", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, scheduled_ids, status="scheduled", tags=["bulk"])
    expected_from: dict[UUID, str] = dict.fromkeys(pending_ids, "pending")
    expected_from.update(dict.fromkeys(scheduled_ids, "scheduled"))

    result, _notify = await _cancel_where(
        _CountingPool(module_pg_pool),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert result.cancelled_directly == len(expected_from)

    rows = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, detail FROM "{schema}".job_events ORDER BY id',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    state_changes = [r for r in rows if r["kind"] == "state_change"]
    cancel_requests = [r for r in rows if r["kind"] == "cancel_request"]
    assert {r["kind"] for r in rows} == {"state_change", "cancel_request"}, (
        "a bulk cancel writes exactly these two event kinds"
    )
    assert len(state_changes) == len(expected_from), "exactly one state_change per cancelled job"
    assert len(cancel_requests) == len(expected_from), (
        "exactly one cancel_request per cancelled job"
    )
    assert {r["job_id"] for r in state_changes} == set(expected_from)
    assert {r["job_id"] for r in cancel_requests} == set(expected_from)

    for row in state_changes:
        detail = parse_detail(row["detail"])
        jid = row["job_id"]
        assert detail == {"from_state": expected_from[jid], "to_state": "cancelled"}, (
            f"state_change detail for {jid} must carry that job's ACTUAL previous status "
            f"({expected_from[jid]!r}); a batched rewrite that shares one detail across "
            f"the whole unnest would hardcode a single from_state here"
        )

    for row in cancel_requests:
        assert parse_detail(row["detail"]) == {"reason": "offboard"}

    # Both previous statuses must actually be represented -- otherwise the
    # mixed-seed premise of this test has silently evaporated.
    observed = {parse_detail(r["detail"])["from_state"] for r in state_changes}
    assert observed == {"pending", "scheduled"}, (
        "the seed must produce both from_state values, or this test proves nothing"
    )


async def test_pins_event_occurred_at_is_per_row_and_co_monotonic_with_id(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded drain must preserve.

    ``INSERT_EVENT_SQL`` stamps ``occurred_at`` with ``clock_timestamp()``, not
    ``now()``: ``now()`` is frozen at transaction start, so a rewrite that
    switched to it (or that computed one timestamp in Python and bound it for
    every row) would give every event in a bulk cancel an IDENTICAL
    ``occurred_at``.

    Two separate properties are pinned:

    * **Co-monotonicity** of ``occurred_at`` with ``id`` -- the assumption
      ``RECLAIM_EVENT_VISIBILITY_DELAY``'s whole trailing-watermark argument
      rests on ("whichever row was inserted first has both the lower ``id``
      and the earlier ``occurred_at``").  Assert non-decreasing, not strictly
      increasing: two rows in one batch can legitimately land on the same
      microsecond.
    * **Distinctness across statements** -- events written by the
      ``state_change`` pass and events written by the later ``cancel_request``
      pass must not share a timestamp, which they would under a frozen
      ``now()``.  This is the assertion that actually proves per-row
      ``clock_timestamp()`` survived; a same-microsecond collision inside a
      single batch cannot make it fail, because the two passes are separated
      by a full round trip.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(50)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", tags=["bulk"])

    result, _notify = await _cancel_where(
        _CountingPool(module_pg_pool),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )
    assert result.cancelled_directly == len(job_ids)

    rows = await clean_pg_conn.fetch(
        f'SELECT id, kind, occurred_at FROM "{schema}".job_events ORDER BY id',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert len(rows) == 2 * len(job_ids)

    stamps = [r["occurred_at"] for r in rows]
    inversions = [
        (rows[i]["id"], rows[i + 1]["id"])
        for i in range(len(stamps) - 1)
        if stamps[i + 1] < stamps[i]
    ]
    assert inversions == [], (
        f"occurred_at must be non-decreasing in id order — {len(inversions)} inversion(s) "
        f"found, e.g. {inversions[:3]}; poll_reclaim_events' trailing watermark assumes "
        f"job_events.id and occurred_at are co-monotonic (see "
        f"taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY)"
    )

    sc_stamps = {r["occurred_at"] for r in rows if r["kind"] == "state_change"}
    cr_stamps = {r["occurred_at"] for r in rows if r["kind"] == "cancel_request"}
    assert sc_stamps and cr_stamps
    assert sc_stamps.isdisjoint(cr_stamps), (
        "state_change and cancel_request events written by separate statements in the "
        "same transaction must not share an occurred_at — a shared value means the "
        "timestamp is frozen at transaction start (now()) or bound once from Python, "
        "not the per-row clock_timestamp() INSERT_EVENT_SQL specifies"
    )
    assert len(sc_stamps) > 1, (
        "per-row clock_timestamp() must produce more than one distinct occurred_at across "
        f"{len(job_ids)} state_change events"
    )


async def test_pins_running_jobs_get_cooperative_cancel_not_terminal_status(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded drain must preserve.

    The two arms are not interchangeable and a rewrite must not merge them: a
    ``running`` job is NEVER moved to terminal ``cancelled`` by a bulk cancel.
    It gets ``cancel_phase=1`` and ``cancel_requested_at`` stamped, stays
    ``running``, and receives a ``cancel_request`` event but NO ``state_change``
    event -- the worker owns the terminal write.

    Also pinned: ``locked_by_worker`` is carried out as a NOTIFY target for
    running jobs that have one, and a running job already at ``cancel_phase=1``
    is not re-requested (the driving CTE's ``cancel_phase = 0`` guard).
    """
    schema = module_pg_schema.schema_name
    render(schema)
    worker_id = new_uuid()
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "VALUES ($1, 'test-host', 1, ARRAY['default'])",
        worker_id,
    )
    running_ids = [new_uuid() for _ in range(20)]
    already_requested = [new_uuid() for _ in range(5)]
    pending_ids = [new_uuid() for _ in range(5)]
    await _seed_jobs(clean_pg_conn, schema, running_ids, status="running", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, already_requested, status="running", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, pending_ids, status="pending", tags=["bulk"])
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET locked_by_worker = $2 WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        running_ids + already_requested,
        worker_id,
    )
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET cancel_phase = 1, '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "cancel_requested_at = clock_timestamp() WHERE id = ANY($1::uuid[])",
        already_requested,
    )

    result, notify_targets = await _cancel_where(
        _CountingPool(module_pg_pool),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert result.cancelled_directly == len(pending_ids), "only pending/scheduled go terminal"
    assert set(result.cancelled_ids) == set(pending_ids)
    assert result.cancel_requested == len(running_ids), (
        "jobs already at cancel_phase=1 must not be re-requested"
    )
    assert set(result.cancel_requested_ids) == set(running_ids)
    assert {t.job_id for t in notify_targets} == set(running_ids)
    assert {t.worker_id for t in notify_targets} == {worker_id}

    rows = await clean_pg_conn.fetch(
        "SELECT id, status, cancel_phase, cancel_requested_at, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
        running_ids,
    )
    assert len(rows) == len(running_ids)
    for row in rows:
        assert row["status"] == "running", "a bulk cancel never terminalises a running job"
        assert row["cancel_phase"] == 1
        assert row["cancel_requested_at"] is not None
        assert row["finished_at"] is None, "finished_at belongs to the worker's terminal write"

    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, detail FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        running_ids,
    )
    assert len(events) == len(running_ids), "exactly one event per cancel-requested running job"
    assert {e["kind"] for e in events} == {"cancel_request"}, (
        "a running job gets no state_change from a bulk cancel"
    )
    for event in events:
        assert parse_detail(event["detail"]) == {"reason": "offboard"}

    skipped = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        already_requested,
    )
    assert skipped == 0, "an already-cancel-requested job gets no second event"


async def test_pins_rerunning_the_bulk_cancel_is_idempotent(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded drain must preserve.

    A second identical bulk cancel cancels nothing extra and writes no extra
    events: the ``status IN ('pending','scheduled')`` guard in the driving CTE
    already excludes the now-terminal rows, so the second call returns zero and
    the ``job_events`` count is unchanged.

    This matters most for a CHUNKED rewrite, which is likely to be driven by a
    drain loop: a loop whose termination condition is wrong (or whose cursor
    resets) would re-cancel and re-log.  Pinning the event count after the
    second call catches a duplicated audit trail that a status-only assertion
    would miss entirely.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(40)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", tags=["bulk"])

    first, _n1 = await _cancel_where(
        _CountingPool(module_pg_pool),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )
    assert first.cancelled_directly == len(job_ids)
    events_after_first = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert events_after_first == 2 * len(job_ids)
    finished_at_first = await clean_pg_conn.fetch(
        f'SELECT id, finished_at FROM "{schema}".jobs ORDER BY id'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )

    second, _n2 = await _cancel_where(
        _CountingPool(module_pg_pool),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )

    assert second.cancelled_directly == 0, "a second bulk cancel must cancel nothing extra"
    assert second.cancel_requested == 0
    assert second.cancelled_ids == ()
    assert second.cancel_requested_ids == ()

    events_after_second = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert events_after_second == events_after_first, "no duplicate events on the second pass"

    finished_at_second = await clean_pg_conn.fetch(
        f'SELECT id, finished_at FROM "{schema}".jobs ORDER BY id'  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert [(r["id"], r["finished_at"]) for r in finished_at_second] == [
        (r["id"], r["finished_at"]) for r in finished_at_first
    ], "finished_at must not be re-stamped by a second bulk cancel"


async def test_pins_returned_count_equals_the_number_actually_cancelled(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """LAYER 2 — pins the behaviour the bounded drain must preserve.

    ``BulkCancelResult.cancelled_directly`` is the count of rows that actually
    moved to ``cancelled`` by THIS call -- not the size of the match set, and
    not the size of the predicate's superset.  It is checked against three
    independent sources: the database's own ``cancelled`` row count, the length
    of ``cancelled_ids``, and the number of ``state_change`` events written.

    A chunked rewrite accumulating counts across chunks is precisely where this
    can drift (returning the last chunk's count, double-counting a retried
    chunk, or counting matched-but-not-updated rows), and every one of those
    bugs is invisible to a test that only checks final statuses.  The mixed
    seed -- matching-and-cancellable, matching-but-terminal, non-matching --
    makes "count of rows updated" differ from every nearby number.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    cancellable = [new_uuid() for _ in range(30)]
    matching_terminal = [new_uuid() for _ in range(7)]
    non_matching = [new_uuid() for _ in range(11)]
    await _seed_jobs(clean_pg_conn, schema, cancellable, status="scheduled", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, matching_terminal, status="failed", tags=["bulk"])
    await _seed_jobs(clean_pg_conn, schema, non_matching, status="pending", tags=["keep"])

    result, _notify = await _cancel_where(
        _CountingPool(module_pg_pool),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        None,
    )

    db_cancelled = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'cancelled'"  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    state_changes = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".job_events WHERE kind = 'state_change'"  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )

    assert result.cancelled_directly == len(cancellable)
    assert result.cancelled_directly == db_cancelled, (
        "the returned count must equal the rows this call actually moved to 'cancelled'"
    )
    assert result.cancelled_directly == len(result.cancelled_ids)
    assert result.cancelled_directly == state_changes
    assert set(result.cancelled_ids) == set(cancellable)
    assert result.total_affected == len(cancellable)

    # reason=None writes an empty detail, not a NULL and not {"reason": null}.
    cancel_requests = await clean_pg_conn.fetch(
        f"SELECT detail FROM \"{schema}\".job_events WHERE kind = 'cancel_request'"  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert len(cancel_requests) == len(cancellable)
    assert all(parse_detail(r["detail"]) == {} for r in cancel_requests), (
        "reason=None must serialise to an empty detail object"
    )


# ══════════════════════════════════════════════════════════════════════════
# LAYER 3 — the scaling contract. Layer 1 bounds what ONE transaction does;
# these bound how the drain behaves ACROSS transactions as the backlog
# grows. A bulk cancel that is bounded per batch can still be unusable at
# scale in two ways an operator feels and no layer-1 assertion can see:
# each batch costing more than the last (quadratic total drain), and a
# batch's cost growing with jobs the filter never matches (coupling one
# tenant's offboard to every other tenant's backlog).
# ══════════════════════════════════════════════════════════════════════════


def _plan_rows_discarded(node: dict[str, Any]) -> int:
    """Total rows the plan visited and then threw away, across the tree.

    ``Rows Removed by Filter`` times ``Actual Loops`` is the exact count of
    rows a node touched that contributed nothing to the result -- the
    measure of wasted work, deterministic for a fixed seed and independent
    of machine speed.

    Buffers are deliberately NOT the oracle here. A drain that re-walks
    every already-cancelled row still shows nearly flat buffer counts
    while the whole table fits in shared cache, so a buffer-based
    assertion passes at test scale and says nothing about the backlog
    depth where the drain actually fails. Discarded rows expose the
    re-walk at any size.
    """
    total = int(node.get("Rows Removed by Filter", 0) or 0) * int(node.get("Actual Loops", 1) or 1)
    for child in node.get("Plans", []):
        total += _plan_rows_discarded(child)
    return total


async def _advance_drive_cursor(
    conn: asyncpg.Connection,
    schema: str,
    drive_args: tuple[object, ...],
    tag: str,
    *,
    handled: str,
) -> tuple[object, ...]:
    """Advance a keyset cursor in *drive_args* past the rows already handled.

    The drive statement's arguments are the filter parameters followed by
    whatever per-batch state it takes.  If one of those is a ``UUID`` it is
    the drain's keyset cursor, and a measurement loop driving the statement
    by hand must move it between batches exactly as the production drain
    does -- otherwise every pass re-runs the first batch and the loop never
    terminates.

    *handled* is the SQL predicate identifying rows this arm has already
    dealt with: the terminal arm moves them to ``cancelled``, while the
    cooperative arm leaves them ``running`` and only sets
    ``cancel_phase``, so each arm recognises its own progress differently.

    Finding the cursor by type rather than by position keeps this agnostic
    to the statement's parameter layout, and a statement that takes no
    cursor is returned unchanged -- so the same loop drives both the
    bounded and the unbounded shape.
    """
    last_handled: UUID | None = await conn.fetchval(
        f'SELECT max(id::text)::uuid FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; `handled` is a test-local literal, never user input.
        f"WHERE {handled} AND tags @> ARRAY[$1]::text[]",
        tag,
    )
    if last_handled is None:
        return drive_args
    return tuple(last_handled if isinstance(arg, UUID) else arg for arg in drive_args)


class _StatementRecordingPool:
    """Pool stand-in that records the first drive statement and its params,
    then lets the real drain proceed untouched."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[Any]:
        async with self._pool.acquire(**kwargs) as conn:
            outer = self

            class _Recorder:
                def __init__(self, inner: Any) -> None:
                    self._inner = inner

                async def fetchrow(self, sql: str, *args: object) -> Any:
                    outer.statements.append((sql, args))
                    return await self._inner.fetchrow(sql, *args)

                def __getattr__(self, name: str) -> Any:
                    return getattr(self._inner, name)

            yield _Recorder(conn)


async def test_drain_batch_cost_does_not_grow_as_the_backlog_is_cancelled(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Every batch of a bulk cancel costs the same whether it is the first
    batch of the backlog or the last, so a deep offboard finishes in time
    linear in the backlog rather than quadratic in it.

    The drive statement re-selects "the next ``batch_size`` matching
    pending/scheduled rows" on every pass. If the plan cannot skip the rows
    earlier batches already moved to ``cancelled`` -- because the filter's
    predicate is a post-scan filter rather than an index condition, or the
    ``ORDER BY id`` forces a fresh sort of the whole match population each
    time -- then batch N pays for the (N-1) * batch_size rows already
    cancelled. Operationally that is the difference between an offboard an
    operator can run on a real tenant and one that keeps tripping its own
    per-batch statement timeout the deeper it gets, stranding the tail on
    exactly the backlogs where the command matters most. Nothing errors;
    the drain just stops finishing.

    Measured as rows the plan visited and discarded per batch, which is the
    wasted work itself rather than a proxy for it, so the pin holds at any
    table size and does not turn into a timing flake.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    batch_size = 20
    total = batch_size * 10

    job_ids = [new_uuid() for _ in range(total)]
    await _seed_jobs(conn, schema, job_ids, status="pending", tags=["tenant-acme"])
    await conn.execute(f'ANALYZE "{schema}".jobs')

    # Capture the exact production drive statement rather than re-spelling
    # the CTE here: a rewritten implementation must stay measured.
    recording = _StatementRecordingPool(module_pg_pool)
    probe_id = [new_uuid()]
    await _seed_jobs(conn, schema, probe_id, status="pending", tags=["probe-only"])
    await _cancel_where(
        recording,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("probe-only",)),
        None,
        batch_size=batch_size,
    )
    drive_sql, probe_args = recording.statements[0]
    # Re-bind the statement with the arguments the drain itself passed,
    # substituting this test's tag for the probe's. Hand-spelling the
    # argument list here instead would pin the statement's parameter
    # COUNT -- an implementation detail -- so a drive statement that
    # gained a parameter (a keyset cursor, say) would fail these on
    # arity rather than on the cost they exist to measure.
    drive_args = (["tenant-acme"], *probe_args[1:])

    per_batch_discarded: list[int] = []
    while True:
        plan_rows = await conn.fetch(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {drive_sql}",  # Why: drive_sql is the production constant captured above; its only interpolation is the fixture schema identifier.
            *drive_args,
        )
        plan = json.loads(plan_rows[0][0])[0]["Plan"]
        per_batch_discarded.append(_plan_rows_discarded(plan))
        # EXPLAIN ANALYZE executes the statement, so this batch has
        # landed. If the drive statement takes a keyset cursor, advance it
        # past the rows just cancelled so the next pass resumes where this
        # one stopped -- exactly what the production drain does between
        # batches. Reading the cursor back from the table keeps this
        # independent of how the statement returns it.
        drive_args = await _advance_drive_cursor(
            conn, schema, drive_args, "tenant-acme", handled="status = 'cancelled'"
        )
        remaining = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
            "WHERE status IN ('pending', 'scheduled') AND tags @> ARRAY['tenant-acme']::text[]"
        )
        if remaining == 0:
            break

    assert len(per_batch_discarded) >= 5, (
        f"expected the backlog to drain over several batches; got "
        f"{len(per_batch_discarded)} batches for {total} rows at batch_size={batch_size}"
    )
    # The bound is the batch, not the backlog: a batch that skips the rows
    # earlier batches cancelled discards at most a batch-sized handful
    # whatever pass it is on. A batch that re-walks them discards
    # (N-1) * batch_size, which crosses this bound almost immediately.
    worst = max(per_batch_discarded)
    assert worst <= batch_size * 2, (
        "a cancel batch visited and discarded far more rows than one batch's "
        "worth, so each pass is re-walking the part of the match set earlier "
        "batches already cancelled: total drain work is quadratic in backlog "
        "depth, and on a real backlog the later batches blow their own "
        "statement timeout and strand the tail. Rows discarded per batch: "
        f"{per_batch_discarded!r}"
    )


async def test_drain_batch_cost_is_independent_of_non_matching_backlog(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """One tenant's bulk cancel costs the same whether the table holds only
    that tenant's jobs or a large backlog belonging to everyone else.

    An operator offboarding one tenant is issuing a filtered write; the
    filter is the whole point. If the drive statement's cost tracks the
    table's total pending population rather than the match set, then every
    tenant's offboard slows down as the fleet grows, and the only symptom
    is a command that used to finish and now times out -- with nothing
    about that tenant's own backlog having changed, so nothing in their
    metrics explains it.

    The match set is held fixed at one small cohort while the unrelated
    backlog grows by two orders of magnitude, and the assertion is on rows
    the production drive statement visited and discarded, so it measures
    the wasted work itself at any table size rather than a cache-sensitive
    proxy for it.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    batch_size = 20
    matching = 20

    async def _drive_discarded() -> int:
        recording = _StatementRecordingPool(module_pg_pool)
        probe = [new_uuid()]
        await _seed_jobs(conn, schema, probe, status="pending", tags=["probe-only"])
        await _cancel_where(
            recording,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
            schema,
            render(schema),
            JobFilter(tags=("probe-only",)),
            None,
            batch_size=batch_size,
        )
        drive_sql, probe_args = recording.statements[0]
        # Re-bind with the arguments the drain itself passed, swapping in
        # this test's tag. Only one batch is driven here, so no cursor
        # needs advancing -- but binding positionally off the captured
        # arguments keeps this from pinning the statement's parameter
        # count, which is an implementation detail.
        plan_rows = await conn.fetch(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {drive_sql}",  # Why: drive_sql is the production constant captured above; its only interpolation is the fixture schema identifier.
            ["tenant-acme"],
            *probe_args[1:],
        )
        return _plan_rows_discarded(json.loads(plan_rows[0][0])[0]["Plan"])

    # Baseline: only the match set exists.
    await _seed_jobs(
        conn, schema, [new_uuid() for _ in range(matching)], status="pending", tags=["tenant-acme"]
    )
    await conn.execute(f'ANALYZE "{schema}".jobs')
    small_backlog_discarded = await _drive_discarded()

    # The match set is unchanged; every other tenant's backlog grows.
    for tenant in range(20):
        await _seed_jobs(
            conn,
            schema,
            [new_uuid() for _ in range(100)],
            status="pending",
            tags=[f"tenant-other-{tenant}"],
        )
    await conn.execute(f'ANALYZE "{schema}".jobs')
    large_backlog_discarded = await _drive_discarded()

    # The bound is absolute, not a ratio: a filtered write whose cost is
    # scoped to its match set discards at most a batch's worth of rows
    # however deep the rest of the table gets. A ratio would let the
    # baseline's own waste license proportional growth.
    assert large_backlog_discarded <= batch_size * 2, (
        "a filtered bulk cancel visited and discarded far more rows once "
        "other tenants' jobs were present, so its cost tracks the table's "
        "whole pending population rather than the match set: every tenant's "
        "offboard slows as the fleet grows, with nothing in that tenant's own "
        f"metrics to explain it. Rows discarded: small={small_backlog_discarded} "
        f"large={large_backlog_discarded}"
    )


async def test_running_arm_drain_batch_cost_does_not_grow_as_requests_accumulate(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The cooperative-cancel arm's batches must cost the same whether they
    run at the start of the backlog or at its end.

    The running arm re-selects "the next ``batch_size`` matching rows with
    ``status='running' AND cancel_phase=0``" on every pass, and a row it
    already requested STAYS ``running`` (the worker owns the terminal
    write) — only ``cancel_phase`` moves. If the plan cannot skip the
    already-requested rows, batch N pays for the (N-1) * batch_size rows
    its predecessors moved, exactly as the terminal arm does, and an
    offboard landing on a fleet with a deep running backlog strands its
    tail against the per-batch statement timeout with nothing raised.
    Fixing the terminal arm alone leaves this half of the defect in place,
    so the same visit-cost pin is applied here, measured the same way:
    rows the production drive statement visited and discarded per batch.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    batch_size = 20
    total = batch_size * 10

    job_ids = [new_uuid() for _ in range(total)]
    await _seed_jobs(conn, schema, job_ids, status="running", tags=["tenant-acme"])
    await conn.execute(f'ANALYZE "{schema}".jobs')

    # Capture the exact production drive statement rather than re-spelling
    # the CTE here. The pending/scheduled arm always drains first and runs
    # exactly once against this probe (its window holds the single pending
    # probe row), so the second recorded statement is the running arm's.
    recording = _StatementRecordingPool(module_pg_pool)
    probe_id = [new_uuid()]
    await _seed_jobs(conn, schema, probe_id, status="pending", tags=["probe-only"])
    await _cancel_where(
        recording,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("probe-only",)),
        None,
        batch_size=batch_size,
    )
    drive_sql, probe_args = recording.statements[1]
    drive_args = (["tenant-acme"], *probe_args[1:])

    per_batch_discarded: list[int] = []
    while True:
        plan_rows = await conn.fetch(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {drive_sql}",  # Why: drive_sql is the production constant captured above; its only interpolation is the fixture schema identifier.
            *drive_args,
        )
        plan = json.loads(plan_rows[0][0])[0]["Plan"]
        per_batch_discarded.append(_plan_rows_discarded(plan))
        # This arm leaves a handled row `running` and only moves
        # cancel_phase, so its progress is recognised by cancel_phase = 1
        # rather than by a terminal status.
        drive_args = await _advance_drive_cursor(
            conn, schema, drive_args, "tenant-acme", handled="cancel_phase = 1"
        )
        remaining = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
            "WHERE status = 'running' AND cancel_phase = 0 "
            "AND tags @> ARRAY['tenant-acme']::text[]"
        )
        if remaining == 0:
            break

    assert len(per_batch_discarded) >= 5, (
        f"expected the backlog to drain over several batches; got "
        f"{len(per_batch_discarded)} batches for {total} rows at batch_size={batch_size}"
    )
    # The bound is the batch, not the backlog: a batch that skips the rows
    # earlier batches already cancel-requested discards at most a
    # batch-sized handful whatever pass it is on. A batch that re-walks
    # them discards (N-1) * batch_size, which crosses this bound almost
    # immediately.
    worst = max(per_batch_discarded)
    assert worst <= batch_size * 2, (
        "a running-arm cancel batch visited and discarded far more rows than "
        "one batch's worth, so each pass is re-walking the rows earlier "
        "batches already moved to cancel_phase=1: total drain work is "
        "quadratic in the running backlog, and on a real backlog the later "
        "batches blow their own statement timeout and strand the tail. Rows "
        f"discarded per batch: {per_batch_discarded!r}"
    )


async def test_running_arm_drain_batch_cost_is_independent_of_non_matching_backlog(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The cooperative-cancel arm's batch cost must not track a running
    backlog the filter never matches.

    A running job stays ``running`` after the arm requests its cancel, so
    the arm re-selects from a population that only shrinks by its own
    batch size -- and if the drive statement answers the tag filter by
    walking the table, every batch pays for every other tenant's running
    jobs. An offboard that lands while the fleet is busy then slows for
    reasons visible in no metric of the tenant being offboarded. The
    pending/scheduled arm has the same pin above; the running arm's
    population is shaped differently (rows never leave ``running`` on
    this path), so it is pinned separately rather than assumed.

    Measured as rows the production drive statement visited and
    discarded, the wasted work itself, so the pin holds at any table
    size.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    batch_size = 20
    matching = 20

    async def _drive_discarded() -> int:
        recording = _StatementRecordingPool(module_pg_pool)
        probe = [new_uuid()]
        await _seed_jobs(conn, schema, probe, status="pending", tags=["probe-only"])
        await _cancel_where(
            recording,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
            schema,
            render(schema),
            JobFilter(tags=("probe-only",)),
            None,
            batch_size=batch_size,
        )
        # The pending/scheduled arm drains the probe in one batch and the
        # running arm runs second, so the second recorded statement is the
        # running arm's.
        drive_sql, probe_args = recording.statements[1]
        # Re-bind with this test's tag substituted for the probe's; only
        # one batch is driven here, so the captured per-batch arguments
        # (a keyset cursor at its initial position) are correct as-is.
        drive_args = (["tenant-acme"], *probe_args[1:])
        plan_rows = await conn.fetch(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {drive_sql}",  # Why: drive_sql is the production constant captured above; its only interpolation is the fixture schema identifier.
            *drive_args,
        )
        return _plan_rows_discarded(json.loads(plan_rows[0][0])[0]["Plan"])

    # Baseline: only the match set exists.
    await _seed_jobs(
        conn, schema, [new_uuid() for _ in range(matching)], status="running", tags=["tenant-acme"]
    )
    await conn.execute(f'ANALYZE "{schema}".jobs')
    small_backlog_discarded = await _drive_discarded()

    # The match set is unchanged; every other tenant's running backlog grows.
    for tenant in range(20):
        await _seed_jobs(
            conn,
            schema,
            [new_uuid() for _ in range(100)],
            status="running",
            tags=[f"tenant-other-{tenant}"],
        )
    await conn.execute(f'ANALYZE "{schema}".jobs')
    large_backlog_discarded = await _drive_discarded()

    # The bound is absolute, not a ratio, for the same reason the
    # pending/scheduled arm's pin states: a filtered write scoped to its
    # match set discards at most a batch's worth of rows however deep the
    # rest of the table's running population gets.
    assert large_backlog_discarded <= batch_size * 2, (
        "a filtered bulk cancel's running arm visited and discarded far more "
        "rows once other tenants' running jobs were present, so its cost "
        "tracks the table's whole running population rather than the match "
        "set: every offboard that lands mid-incident slows as the fleet's "
        f"running backlog grows. Rows discarded: small={small_backlog_discarded} "
        f"large={large_backlog_discarded}"
    )


async def test_partial_drain_progress_is_durable_and_a_rerun_resumes(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A bulk cancel interrupted mid-drain leaves the batches it already
    committed cancelled on disk, and a re-run finishes the remainder
    without re-cancelling or double-counting what already landed.

    This is what makes a large backlog completable at all: the operator
    whose offboard died against a deep queue re-runs the same command and
    it converges, rather than restarting from zero every time and never
    finishing. Durability of partial progress is only observable from
    outside the failed call -- the raised exception carries no count -- so
    it has to be read off the rows and off the second call's result.

    The interruption is injected at a true boundary (the pool hands out a
    connection that refuses to drive a further batch), not by patching the
    drain's internals, so the code path that commits each batch is the
    production one.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    batch_size = 25
    total = 200

    job_ids = [new_uuid() for _ in range(total)]
    await _seed_jobs(conn, schema, job_ids, status="pending", tags=["tenant-acme"])
    untouched = [new_uuid() for _ in range(13)]
    await _seed_jobs(conn, schema, untouched, status="pending", tags=["tenant-keep"])

    class _FailAfterNBatches:
        """Pool stand-in whose connections stop driving after *n* batches."""

        def __init__(self, pool: Any, n: int) -> None:
            self._pool = pool
            self._n = n
            self.batches = 0

        @asynccontextmanager
        async def acquire(self, **kwargs: object) -> AsyncGenerator[Any]:
            async with self._pool.acquire(**kwargs) as inner:
                outer = self

                class _Conn:
                    def __init__(self, c: Any) -> None:
                        self._c = c

                    async def fetchrow(self, sql: str, *args: object) -> Any:
                        if outer.batches >= outer._n:
                            raise ConnectionResetError("drain interrupted")
                        outer.batches += 1
                        return await self._c.fetchrow(sql, *args)

                    def __getattr__(self, name: str) -> Any:
                        return getattr(self._c, name)

                yield _Conn(inner)

    interrupted = _FailAfterNBatches(module_pg_pool, 3)
    with pytest.raises(ConnectionResetError):
        await _cancel_where(
            interrupted,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
            schema,
            render(schema),
            JobFilter(tags=("tenant-acme",)),
            None,
            batch_size=batch_size,
        )

    committed = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE status = 'cancelled' AND tags @> ARRAY['tenant-acme']::text[]"
    )
    assert committed > 0, (
        "the batches that committed before the interruption must be durable; a "
        "drain that rolls everything back can never complete a backlog larger "
        "than one batch under any real failure rate"
    )
    assert committed < total, (
        "the interruption must have landed mid-drain for this pin to mean "
        f"anything; {committed} of {total} were already cancelled"
    )

    # The re-run: same command, same filter, no operator bookkeeping.
    result, _notify = await _cancel_where(
        module_pg_pool,
        schema,
        render(schema),
        JobFilter(tags=("tenant-acme",)),
        None,
        batch_size=batch_size,
    )

    assert result.cancelled_directly == total - committed, (
        "the re-run must report only the rows IT cancelled -- rows an earlier "
        "run already committed are skipped by the status predicate, not "
        "re-cancelled and not re-counted"
    )
    final_cancelled = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE status = 'cancelled' AND tags @> ARRAY['tenant-acme']::text[]"
    )
    assert final_cancelled == total, "the re-run must converge the whole match set"

    # Exactly one state_change per job across BOTH runs: a resumed drain
    # that re-walked committed rows would double-write their events.
    event_rows = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events e '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'JOIN "{schema}".jobs j ON j.id = e.job_id '
        "WHERE e.kind = 'state_change' AND j.tags @> ARRAY['tenant-acme']::text[]"
    )
    assert event_rows == total, (
        f"expected exactly one state_change per cancelled job across both runs; "
        f"got {event_rows} for {total} jobs"
    )

    still_pending = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE status = 'pending' AND tags @> ARRAY['tenant-keep']::text[]"
    )
    assert still_pending == len(untouched), (
        "the interrupted drain and its re-run must both stay inside the filter"
    )


# ══════════════════════════════════════════════════════════════════════════
# LAYER 2b — the fixpoint rounds (#237): mid-drain re-pends behind the
# keyset cursor, and their termination/cost bound.
#
# The keyset cursor that keeps each batch cheap (perf-evidence-bulk-cancel.md)
# has a blind spot no single pair of passes can close: a matching RUNNING row
# rescheduled mid-drain (crash reclaim → pending, denial snooze → scheduled,
# shutdown interrupt → pending, consumer retry → scheduled/pending) lands at
# an id the pending arm's cursor has ALREADY passed, and the running arm —
# strictly after it, matching only `running AND cancel_phase = 0` — cannot
# see the re-pended row either.  The pre-rounds drain returned normally with
# such a row uncancelled, contradicting the "cancels EVERY matching job"
# contract.  These tests interleave real concurrent writers (the production
# reclaim sweep, a churn producer) between the drain's batches through a
# pool stand-in, then pin that the next round's fresh-cursor pass catches
# the straggler, that a quiescent tail terminates at the first empty round,
# and that sustained churn stops at the hard round cap instead of looping.
# ══════════════════════════════════════════════════════════════════════════


class _DrainSpyPool:
    """Pool stand-in that interleaves a hook between the drain's batches
    and records every driving fetchrow's arm and keyset cursor.

    The hook fires BEFORE each acquire's transaction opens — the one
    moment a concurrent writer can act on rows the drain has windowed
    past without contending a held row lock — and runs on the RAW pool,
    exactly where a real reclaim sweep or peer producer would run.

    The driving-statement observation sniffs the two arms' distinctive
    status predicates (`status IN ('pending', 'scheduled')` /
    `status = 'running'`) and reads the cursor from the drain's own
    bind order (`*filter_params, batch_cursor, batch_size`), so the
    round arithmetic below measures the shipped statements rather than
    a reimplementation.
    """

    def __init__(
        self,
        pool: Any,
        before_acquire: Callable[[int], Awaitable[None]] | None = None,
    ) -> None:
        self._pool = pool
        self._before_acquire = before_acquire
        self.acquire_count = 0
        #: (arm, keyset cursor) per driving fetchrow, in call order.
        self.driving: list[tuple[str, UUID]] = []

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[Any]:
        self.acquire_count += 1
        if self._before_acquire is not None:
            await self._before_acquire(self.acquire_count)
        async with self._pool.acquire(**kwargs) as inner:
            outer = self

            class _Conn:
                def __init__(self, c: Any) -> None:
                    self._c = c

                async def fetchrow(self, sql: str, *args: object) -> Any:
                    if "status IN ('pending', 'scheduled')" in sql:
                        outer.driving.append(("ps", args[-2]))
                    elif "status = 'running'" in sql:
                        outer.driving.append(("run", args[-2]))
                    return await self._c.fetchrow(sql, *args)

                def __getattr__(self, name: str) -> Any:
                    return getattr(self._c, name)

            yield _Conn(inner)

    # -- round arithmetic over the recorded driving statements -------

    @property
    def ps_drains_started(self) -> int:
        """How many pending/scheduled arm drains began — one per fixpoint
        round that actually ran, identified by the fresh-cursor pass
        (the first batch of every arm binds ``_UUID_MIN``)."""
        return sum(1 for arm, cursor in self.driving if arm == "ps" and cursor == _UUID_MIN)

    @property
    def ps_batches(self) -> int:
        return sum(1 for arm, _cursor in self.driving if arm == "ps")

    @property
    def run_batches(self) -> int:
        return sum(1 for arm, _cursor in self.driving if arm == "run")


# The grace pair the sweep tests use: the cancel carve-out's deep
# threshold is then 30 + 30 + 60 = 120s, so a lock 180s past is deeply
# expired (admits a cancel_phase != 0 row) and a lock 10s past is only
# baseline-expired (admits a phase-0 row, keeps a phase-1 row running).
_SWEEP_CANCEL_GRACE = timedelta(seconds=30)
_SWEEP_CLEANUP_GRACE = timedelta(seconds=30)


async def _seed_running_matching_job(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    *,
    tags: Sequence[str],
    cancel_phase: int = 0,
    lock_expires_at: datetime,
) -> UUID:
    """One running job carrying *tags*, via the suite's shared seeder
    (the running-row shape the drain's running arm matches), with the
    tag stamped by a follow-up UPDATE — ``create_running_job`` predates
    tag-aware seeding and tests patch columns this way."""
    job_id = await create_running_job(
        conn,
        schema,
        worker_id,
        cancel_phase=cancel_phase,
        cancel_requested_at=datetime.now(UTC) if cancel_phase else None,
        lock_expires_at=lock_expires_at,
        with_events=False,
    )
    await conn.execute(
        f'UPDATE "{schema}".jobs SET tags = $2::text[] WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; the tag array is $N-bound.
        job_id,
        list(tags),
    )
    return job_id


async def test_repend_behind_the_cursor_is_caught_by_the_next_round(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A matching running row the crash-reclaim sweep re-pends BEHIND the
    pending arm's keyset cursor mid-drain must still be cancelled by the
    same call (#237).

    Reproduction of the defect on the pre-rounds drain: the row was
    'running' when the pending arm windowed past its id (no window ever
    held it), the sweep hands it back 'pending' between the two arms,
    and the running arm matches only ``running AND cancel_phase = 0`` —
    so the call returned normally with the row uncancelled.  The fix's
    next round restarts the pending arm's cursor at the bottom of the
    key space and cancels the straggler there.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    worker_id = new_uuid()
    await create_worker(conn, schema, worker_id)

    # Seeded FIRST and 20ms before the pending backlog: job ids are
    # UUIDv7 (millisecond precision + random tail), so the sleep puts
    # the running row's id strictly below every backlog id — the
    # pending arm's first batch advances the cursor past it.
    moved_job_id = await _seed_running_matching_job(
        conn,
        schema,
        worker_id,
        tags=["midflight"],
        lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
    )
    await asyncio.sleep(0.02)
    backlog_ids = [new_uuid() for _ in range(4)]
    await _seed_jobs(conn, schema, backlog_ids, status="pending", tags=["midflight"])

    batch_size = 2
    swept: list[bool] = []

    async def _sweep_the_running_row_on_arm2(n: int) -> None:
        # Acquire ordinal 4 = the running arm's first batch: the pending
        # arm drained 3 batches (2, 2, 0 windows) and its cursor is above
        # the running row's id; the running arm's transaction has NOT
        # opened yet, so the production sweep acts on the unlocked row
        # exactly where a real leader's tick would.
        if n != 4 or swept:
            return
        swept.append(True)
        async with module_pg_pool.acquire() as sweep_conn:
            count = await PostgresBackend.sweep_expired_locks(
                sweep_conn,
                _SWEEP_CANCEL_GRACE,
                _SWEEP_CLEANUP_GRACE,
                schema=schema,
            )
        assert count == 1, "the hook's sweep must reclaim the running row"

    pool = _DrainSpyPool(module_pg_pool, _sweep_the_running_row_on_arm2)
    result, _notify = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("midflight",)),
        "offboard",
        batch_size=batch_size,
    )

    assert swept, "the interleaving hook never fired — the reproduction did not run"
    assert result.cancelled_directly == 5, (
        f"every matching job must be cancelled by the one call: the 4 seeded "
        f"pending rows plus the row the sweep re-pended behind the cursor "
        f"(got {result.cancelled_directly}; the pre-rounds drain returned 4 "
        f"and left the straggler pending with its cancel silently lost)"
    )
    row = await conn.fetchrow(
        f'SELECT status, cancel_phase FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above; the job id is $N-bound.
        moved_job_id,
    )
    assert row is not None
    assert row["status"] == "cancelled"
    assert row["cancel_phase"] == 0

    # The round arithmetic: round 1 drained the backlog, round 2 caught
    # the straggler, round 3 confirmed zero and stopped the fixpoint.
    assert pool.ps_drains_started == 3, (
        f"expected the fixpoint to run rounds 1 (backlog), 2 (straggler) and "
        f"3 (empty confirmation); saw {pool.ps_drains_started} pending-arm "
        f"drains"
    )


async def test_cancel_in_flight_reclaimed_mid_drain_is_terminal_cancelled(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The #237 + #238 composition, end to end: a running job whose cancel
    is ALREADY in flight (phase 1, requested_at stamped — a prior
    single-job cancel), reclaimed by the production sweep between the
    drain's two arms, behind the pending arm's cursor.

    Doubly lost on the pre-fix code: the budget-first sweep re-pended the
    row 'pending' with the operator's cancel columns WIPED (#238), and
    the re-pended row sat at an id the pending arm had already passed so
    no later arm of the call could see it (#237) — the call returned
    normally reporting nothing about a job whose cancel it had been asked
    to complete.  Either fix alone saves this timing; the composition is
    red only when both are broken, which is the point of walking it end
    to end.  With both fixes the sweep itself terminalises the row
    'cancelled' — the honest resolution for a request whose only
    cooperative writer is provably gone — with the audit columns
    preserved.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    worker_id = new_uuid()
    await create_worker(conn, schema, worker_id)

    # Phase 1 from the start: an operator already asked for this cancel
    # before the bulk call.  Lock 180s past — deeply expired, so the
    # sweep's cancel carve-out admits the phase-1 row between the arms.
    cancelled_job_id = await _seed_running_matching_job(
        conn,
        schema,
        worker_id,
        tags=["midflight"],
        cancel_phase=1,
        lock_expires_at=datetime.now(UTC) - timedelta(seconds=180),
    )
    await asyncio.sleep(0.02)
    backlog_ids = [new_uuid() for _ in range(4)]
    await _seed_jobs(conn, schema, backlog_ids, status="pending", tags=["midflight"])

    batch_size = 2
    swept: list[bool] = []

    async def _sweep_the_cancel_in_flight_row_on_arm2(n: int) -> None:
        if n != 4 or swept:
            return
        swept.append(True)
        async with module_pg_pool.acquire() as sweep_conn:
            count = await PostgresBackend.sweep_expired_locks(
                sweep_conn,
                _SWEEP_CANCEL_GRACE,
                _SWEEP_CLEANUP_GRACE,
                schema=schema,
            )
        assert count == 1, "the hook's sweep must reclaim the cancel-in-flight row"

    pool = _DrainSpyPool(module_pg_pool, _sweep_the_cancel_in_flight_row_on_arm2)
    result, _notify = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("midflight",)),
        "offboard",
        batch_size=batch_size,
    )

    assert swept, "the interleaving hook never fired — the reproduction did not run"
    # The drain itself never matched the row (running+phase-1 for arm 2's
    # predicate, terminal by the time any fresh cursor ran): its cancel
    # was honored by the sweep, and the result must not claim otherwise.
    assert result.cancelled_directly == 4
    assert result.cancel_requested == 0

    row = await conn.fetchrow(
        f"SELECT status, cancel_phase, cancel_requested_at, finished_at, error_class "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above; the job id is $N-bound.
        f'FROM "{schema}".jobs WHERE id = $1',
        cancelled_job_id,
    )
    assert row is not None
    assert row["status"] == "cancelled", (
        "the composition's doubly-lost cancel must land terminal 'cancelled' — "
        "on the pre-fix code this row sat 'pending' with cancel_phase=0 and a "
        "NULL cancel_requested_at, the operator's request erased"
    )
    assert row["cancel_phase"] == 1, "the honored request's audit trail survives"
    assert row["cancel_requested_at"] is not None
    assert row["finished_at"] is not None

    attempt_row = await conn.fetchrow(
        f'SELECT outcome, error_class FROM "{schema}".job_attempts '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above; the job id is $N-bound.
        f"WHERE job_id = $1 ORDER BY started_at DESC LIMIT 1",
        cancelled_job_id,
    )
    assert attempt_row is not None
    assert attempt_row["outcome"] == "crashed"
    assert attempt_row["error_class"] == "WorkerCrashed"

    # The fixpoint still converged: round 2 confirmed nothing left.
    assert pool.ps_drains_started == 2


async def test_drain_rounds_terminate_at_the_first_empty_round(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The fixpoint's termination and cost bound on a quiescent match set:
    exactly two rounds run — round 1 drains, round 2 re-walks from the
    bottom of the key space, matches nothing, and stops the loop.

    This is the bound proof for the no-churn case: the second pass is the
    price of the #237 fix (one extra empty two-arm pass), and it is paid
    ONCE, not per batch — a regression to an unbounded loop shows up as
    more pending-arm drains, and a regression to the single-pass drain as
    exactly one.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    job_ids = [new_uuid() for _ in range(3)]
    await _seed_jobs(conn, schema, job_ids, status="pending", tags=["quiescent"])

    pool = _DrainSpyPool(module_pg_pool)
    result, _notify = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("quiescent",)),
        None,
        batch_size=2,
    )

    assert result.cancelled_directly == 3
    assert pool.ps_drains_started == 2, (
        f"a quiescent drain must run exactly 2 rounds (drain + one empty "
        f"confirmation); saw {pool.ps_drains_started} — the fixpoint either "
        f"never confirmed (1) or did not terminate (> 2)"
    )
    # The per-batch cost bound is unchanged by the rounds: every batch is
    # still one keyset window. 3 rows / batch 2 → round 1 windows 2,1;
    # round 2 windows 0; the running arm windows 0 once per round.
    assert pool.ps_batches == 3
    assert pool.run_batches == 2


async def test_drain_rounds_are_hard_capped_under_sustained_repend_churn(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The adversarial termination bound: a producer that re-feeds the
    match set on EVERY batch of the drain is a steady input, and an
    uncapped while-progress fixpoint would chase it forever.

    The hard cap (``_MAX_CANCEL_DRAIN_ROUNDS``) is what keeps one call's
    work a constant multiple of one drain: the call must return at the
    cap with the churn it caught cancelled and the tail left for a
    re-run — the same non-atomic contract the drain already documents
    for concurrent enqueues, bounded instead of unbounded.
    """
    schema = module_pg_schema.schema_name
    conn = clean_pg_conn
    render(schema)
    job_ids = [new_uuid() for _ in range(2)]
    await _seed_jobs(conn, schema, job_ids, status="pending", tags=["churny"])

    async def _feed_one_matching_row_on_every_acquire(_n: int) -> None:
        async with module_pg_pool.acquire() as churn_conn:
            await _seed_jobs(churn_conn, schema, [new_uuid()], status="pending", tags=["churny"])

    pool = _DrainSpyPool(module_pg_pool, _feed_one_matching_row_on_every_acquire)
    result, _notify = await _cancel_where(
        pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=("churny",)),
        None,
        batch_size=2,
    )

    assert pool.ps_drains_started == _MAX_CANCEL_DRAIN_ROUNDS, (
        f"sustained churn must exhaust the hard round cap (exactly "
        f"{_MAX_CANCEL_DRAIN_ROUNDS} pending-arm drains), not exceed it and "
        f"not stop early while progress was still being made; saw "
        f"{pool.ps_drains_started}"
    )
    # Everything seeded before the final round's running arm drained was
    # cancelled; the row fed during that last arm's acquire is the
    # documented residual, left for a re-run.
    assert result.cancelled_directly >= 2
    leftover = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE status = 'pending' AND tags @> ARRAY['churny']::text[]"
    )
    assert leftover >= 1, (
        "the churn fed after the last pending-arm drain must be left for a "
        "re-run — the cap's documented residual, not a silent loss: a re-run "
        "resumes and converges"
    )
