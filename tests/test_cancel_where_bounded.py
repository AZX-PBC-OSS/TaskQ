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
  rows actually cancelled.

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
    into ``INSERT_EVENTS_BATCH_SQL`` (one shared ``$3::jsonb`` detail for the
    whole ``unnest``) would silently stamp ONE ``from_state`` on every event and
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
