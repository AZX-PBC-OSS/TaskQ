# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The §6 drain-ceiling controls (RULING_718_DIFF Ruling 2 + correction
layer 1's C2) and the census's per-transaction D accounting (Ruling 1 /
A10), driven against real PostgreSQL on the real
``wake_emit_backpressure`` primitive.

The controls carry their own independent instruments — a connection-proxy
JOURNAL that counts actual transaction entries, admission/root/intent lock
statement executions, per-transaction notifications, and read-point pairs,
plus a real LISTEN connection that counts delivered queue wakes. Neither
instrument reads :data:`taskq.workflows._emit.COUNTERS`; the census's own
numbers are asserted against them, so a census bump in the wrong place
(the invocation-level D the correction layer forbids) or a ceiling shape
that hides its work cannot pass.

Red carriers (each preserved as a named control):
* exactly 200 fitting waiters, no 201st: the fixed shape completes in ONE
  transaction (D=1, 402 = 2*(200+1) read executions, one admission/root
  /intent lock set, the handled mark committed in that same transaction);
  the pre-ceiling-fix shape (and the no-final-observation mutant) opens a
  second transaction — journalled, and, once Ruling 1's per-transaction
  bump has landed, visible in ``flow_drains`` 2 -> 1.
* 201 fitting waiters: the ceiling transaction commits exactly 200 wins
  (per-transaction notifications [200, 1]) and leaves the intent pending;
  the second transaction grants exactly one and completes from its fresh
  no-fit read; 406 = 2*(201+2) read executions (the missing final pair is
  the pre-fix red, never ``candidate_reads``-only: 402-vs-402 is an
  expected-green identity for the 200 shape and not a discriminator).
* a deterministic zero-CAS miss: one transaction, 2 = 2*(0+1) read
  executions, no handled mark, no immediate retry.
"""

from __future__ import annotations

import asyncio
import json as _json
from dataclasses import dataclass, field
from typing import Any, cast

import asyncpg
import pytest
import pytest_asyncio

from taskq.backend._protocol import JobId
from taskq.constants import wake_channel
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_DEFERRED_AT = "2026-11-18T09:30:00.123456+00:00"


def _park_meta(flow_id: str, *, width: int, bound: int) -> dict[str, Any]:
    """The exact-park marker metadata (the §2.2 representation's own
    keys — the same shape the product's park write puts on a row)."""
    return {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
        "emit_cursor": {"page": 0},
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": width,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }


async def _seed_drain_estate(
    conn: asyncpg.Connection,
    schema: str,
    *,
    waiters: int,
    width: int,
    bound: int,
    wake_generation: int = 5,
    handled_generation: int = 2,
) -> str:
    """Seed one bounded run's drain estate: a live ``__flow__`` root, the
    intent row with a PENDING generation (wake > handled, so the handled
    mark is a real CAS landing, never a no-op over an already-equal
    pair), and *waiters* exact parked source rows of *width* each."""
    flow_id = str(_new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'r17', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
        flow_id,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"flow:{flow_id}",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity (flow_id, bound, wake_generation, '
        "handled_generation) VALUES ($1::uuid, $2, $3, $4)",
        flow_id,
        bound,
        wake_generation,
        handled_generation,
    )
    for i in range(waiters):
        source_id = str(_new_uuid())
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
            "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
            "fairness_key, assignment_routed, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'r17', 'default', '{}', 'scheduled', 0, 2, 'transient', "
            "'source', $2::jsonb, clock_timestamp(), clock_timestamp(), $3, true, "
            "'workflow-run', $4)",
            source_id,
            _json.dumps(_park_meta(flow_id, width=width, bound=bound)),
            flow_id,
            f"wf:{flow_id}:source:{i}",
        )
    return flow_id


def _new_uuid() -> Any:
    from taskq._ids import new_uuid

    return new_uuid()


@dataclass
class _Journal:
    """The independent instrument: actual transaction entries, lock
    statement executions, per-transaction notifications, and read-point
    pairs, counted at the connection boundary — never a census read."""

    transactions: int = 0
    admission_locks: int = 0
    root_locks: int = 0
    intent_locks: int = 0
    notifications: int = 0
    observation_reads: int = 0
    candidate_reads: int = 0
    handled_writes: int = 0
    notifications_per_tx: list[int] = field(default_factory=list)


class _JournalConn:
    """The connection proxy: the full asyncpg surface the drain touches,
    delegating everything except the journalled statement identities."""

    def __init__(
        self, real: asyncpg.Connection, journal: _Journal, sql_names: dict[str, str]
    ) -> None:
        self._real = real
        self._j = journal
        self._names = sql_names

    def transaction(self) -> Any:
        self._j.transactions += 1
        self._j.notifications_per_tx.append(0)
        return self._real.transaction()

    async def execute(self, sql: str, *args: object) -> Any:
        if sql == self._names["admission"]:
            self._j.admission_locks += 1
        if "pg_notify" in sql:
            self._j.notifications += 1
            if self._j.notifications_per_tx:
                self._j.notifications_per_tx[-1] += 1
        return await self._real.execute(sql, *args)

    async def fetchval(self, sql: str, *args: object) -> Any:
        if sql == self._names["root"]:
            self._j.root_locks += 1
        if sql == self._names["handled"]:
            self._j.handled_writes += 1
        return await self._real.fetchval(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> Any:
        if sql == self._names["intent"]:
            self._j.intent_locks += 1
        if sql == self._names["observation"]:
            self._j.observation_reads += 1
        if sql == self._names["candidate"]:
            self._j.candidate_reads += 1
        return await self._real.fetchrow(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


class _JournalAcquire:
    def __init__(self, pool: asyncpg.Pool, journal: _Journal, sql_names: dict[str, str]) -> None:
        self._pool = pool
        self._j = journal
        self._names = sql_names
        self._held: list[asyncpg.Connection] = []

    async def __aenter__(self) -> _JournalConn:
        conn = await self._pool.acquire()
        self._held.append(conn)
        return _JournalConn(conn, self._j, self._names)

    async def __aexit__(self, *exc: object) -> None:
        assert self._held, "the journal acquire exited without its connection"
        await self._pool.release(self._held.pop())


class _JournalPool:
    """The pool proxy the drain's own ``pool.acquire()`` enters."""

    def __init__(self, real: asyncpg.Pool, journal: _Journal, sql_names: dict[str, str]) -> None:
        self._real = real
        self._j = journal
        self._names = sql_names

    def acquire(self) -> _JournalAcquire:
        return _JournalAcquire(self._real, self._j, self._names)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


@pytest_asyncio.fixture
async def drain_sql(module_pg_schema: ModulePgSchema) -> Any:
    from taskq.workflows.engine import render_workflow_sql

    return render_workflow_sql(module_pg_schema.schema_name)


def _sql_names(wsql: Any) -> dict[str, str]:
    """The drain's statement identities (the rendered strings the drain
    passes to its connection, journalled by equality)."""
    return {
        "admission": wsql.emit_admission_lock,
        "root": wsql.emit_root_lock,
        "intent": wsql.emit_intent_lock,
        "observation": wsql.emit_capacity_observation,
        "candidate": wsql.emit_candidate,
        "handled": wsql.emit_intent_handled,
    }


async def _intent_row(pool: asyncpg.Pool, schema: str, flow_id: str) -> dict[str, int]:
    row = await pool.fetchrow(
        f"""SELECT wake_generation::bigint AS wake, handled_generation::bigint AS handled
            FROM "{schema}".wf_emit_capacity WHERE flow_id = $1::uuid""",
        flow_id,
    )
    assert row is not None
    return {"wake": int(row["wake"]), "handled": int(row["handled"])}


async def _waiter_facts(pool: asyncpg.Pool, schema: str, flow_id: str) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        f"""SELECT status::text AS status,
                   metadata->'wf_emit_backpressure'->>'wakes' AS wakes,
                   metadata ? 'wf_emit_reservation' AS reserved,
                   metadata->'wf_emit_reservation'->>'width' AS width
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND step_key = 'source' ORDER BY id""",
        flow_id,
    )
    return [dict(r) for r in rows]


class TestDrainCeiling:
    async def test_200_fitting_waiters_complete_in_the_ceiling_transaction(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
        drain_sql: Any,
    ) -> None:
        """Exactly 200 fitting waiters, no 201st: ONE transaction, one of
        each lock, 200 wins and notifications in it, the handled mark
        committed in that same transaction, 402 = 2*(200+1) read
        executions, D=1, and every waiter promoted exactly once."""
        from taskq.workflows._emit import COUNTERS, wake_emit_backpressure
        from taskq.workflows._emit import reset_counters as _rc

        schema = module_pg_schema.schema_name
        async with module_pg_pool.acquire() as conn:
            flow_id = await _seed_drain_estate(conn, schema, waiters=200, width=1, bound=400)
        _rc()
        journal = _Journal()
        jpool = cast("asyncpg.Pool", _JournalPool(module_pg_pool, journal, _sql_names(drain_sql)))
        delivered: list[object] = []
        listener = await asyncpg.connect(module_pg_schema.pg_dsn)
        await listener.add_listener(wake_channel(schema), lambda *a: delivered.append(a))
        try:
            wake = await wake_emit_backpressure(jpool, drain_sql, flow_id=JobId(flow_id))
        finally:
            await listener.close()
        snap = COUNTERS.snapshot()
        print("CEILING-200 CENSUS:", _json.dumps(snap, sort_keys=True))
        print("CEILING-200 JOURNAL:", _json.dumps(vars(journal), sort_keys=True))
        # THE FIXED SHAPE: one transaction, completed in it.
        assert wake.promoted == 200, wake
        assert wake.intent_completed is True, wake
        assert wake.missed is False, wake
        assert journal.transactions == 1, journal
        assert journal.admission_locks == 1, journal
        assert journal.root_locks == 1, journal
        assert journal.intent_locks == 1, journal
        assert journal.notifications == 200, journal
        assert journal.notifications_per_tx == [200], journal
        # D IS PER ENTERED TRANSACTION (A10): the census agrees with the
        # independent journal.
        assert snap["flow_drains"] == journal.transactions == 1, (snap, journal)
        assert snap["fitted_grants"] == 200, snap
        assert snap["candidate_reads"] == 402, snap
        assert snap["candidate_reads"] == 2 * (snap["fitted_grants"] + snap["flow_drains"]), snap
        # The handled mark landed in the SAME transaction (the pending
        # generation was real: wake=5, handled=2 seeded).
        intent = await _intent_row(module_pg_pool, schema, flow_id)
        assert intent == {"wake": 5, "handled": 5}, intent
        # Every waiter promoted exactly once, each with its own single
        # reservation and single queue wake.
        facts = await _waiter_facts(module_pg_pool, schema, flow_id)
        assert len(facts) == 200
        assert all(f["status"] == "pending" for f in facts), facts[:3]
        assert all(f["wakes"] == "1" for f in facts), facts[:3]
        assert all(f["reserved"] is True and f["width"] == "1" for f in facts), facts[:3]
        # The real LISTEN connection received the wakes. PostgreSQL
        # collapses same-transaction same-(channel, payload) NOTIFYs, and
        # every waiter's effective queue here is the same 'default' — so
        # the delivered face reads one per committing chunk; the EXACT
        # pg_notify execution counts are the journal's (200, per-tx
        # [200]), never this deduped delivery count.
        assert 1 <= len(delivered) <= 2, len(delivered)

    async def test_201_fitting_waiters_commit_200_then_grant_one_from_a_fresh_read(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
        drain_sql: Any,
    ) -> None:
        """201 fitting waiters: the ceiling transaction commits exactly 200
        wins and leaves the intent pending (no 201st CAS, no handled mark);
        the second transaction grants exactly one and completes from its
        fresh no-fit read; 406 = 2*(201+2); D=2; all 201 rows promoted
        once, no second reservation or queue wake."""
        from taskq.workflows._emit import COUNTERS, wake_emit_backpressure
        from taskq.workflows._emit import reset_counters as _rc

        schema = module_pg_schema.schema_name
        async with module_pg_pool.acquire() as conn:
            flow_id = await _seed_drain_estate(conn, schema, waiters=201, width=1, bound=600)
        _rc()
        journal = _Journal()
        jpool = cast("asyncpg.Pool", _JournalPool(module_pg_pool, journal, _sql_names(drain_sql)))
        delivered: list[object] = []
        listener = await asyncpg.connect(module_pg_schema.pg_dsn)
        await listener.add_listener(wake_channel(schema), lambda *a: delivered.append(a))
        try:
            wake = await wake_emit_backpressure(jpool, drain_sql, flow_id=JobId(flow_id))
        finally:
            await listener.close()
        snap = COUNTERS.snapshot()
        print("CEILING-201 CENSUS:", _json.dumps(snap, sort_keys=True))
        print("CEILING-201 JOURNAL:", _json.dumps(vars(journal), sort_keys=True))
        assert wake.promoted == 201, wake
        assert wake.intent_completed is True, wake
        assert wake.missed is False, wake
        assert journal.transactions == 2, journal
        assert journal.admission_locks == 2, journal
        assert journal.root_locks == 2, journal
        assert journal.intent_locks == 2, journal
        # THE CEILING TRANSACTION COMMITS EXACTLY 200 WINS: the
        # per-transaction notification journal reads [200, 1] — a 201st
        # CAS in the ceiling transaction reads [201, 0] and reds here.
        assert journal.notifications_per_tx == [200, 1], journal
        assert journal.notifications == 201, journal
        assert snap["flow_drains"] == journal.transactions == 2, (snap, journal)
        assert snap["fitted_grants"] == 201, snap
        # THE MISSING 201ST READ PAIR is this shape's pre-fix red: the
        # fixed shape's final ceiling pair makes 406 = 2*(201+2); the
        # pre-ceiling-fix shape read 404.
        assert snap["candidate_reads"] == 406, snap
        assert snap["candidate_reads"] == 2 * (snap["fitted_grants"] + snap["flow_drains"]), snap
        intent = await _intent_row(module_pg_pool, schema, flow_id)
        assert intent == {"wake": 5, "handled": 5}, intent
        facts = await _waiter_facts(module_pg_pool, schema, flow_id)
        assert len(facts) == 201
        assert all(f["status"] == "pending" for f in facts), facts[:3]
        assert all(f["wakes"] == "1" for f in facts), facts[:3]
        # Same-transaction same-(channel, payload) collapse: the delivered
        # face reads at most one per committing chunk (the exact 201
        # pg_notify executions are the journal's, per-tx [200, 1]).
        assert 1 <= len(delivered) <= 2, len(delivered)

    async def test_ceiling_leaves_the_intent_pending_at_the_chunk_boundary(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
        drain_sql: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The 201-waiter shape, observed AT the chunk boundary: after the
        ceiling transaction commits and while the next chunk's transaction
        is paused inside the drain (the test-only pause hook), the intent
        row is STILL PENDING — the ceiling transaction did not mark
        handled beside a fitting candidate. A mutant that marks handled at
        the ceiling (before its no-fit read owns that decision) reds
        here."""
        from taskq.workflows._emit import wake_emit_backpressure

        schema = module_pg_schema.schema_name
        async with module_pg_pool.acquire() as conn:
            flow_id = await _seed_drain_estate(conn, schema, waiters=201, width=1, bound=600)
        monkeypatch.setenv("TASKQ_TEST_DRAIN_CHUNK_PAUSE_S", "3")
        journal = _Journal()
        jpool = cast("asyncpg.Pool", _JournalPool(module_pg_pool, journal, _sql_names(drain_sql)))
        drain_task = asyncio.create_task(
            wake_emit_backpressure(jpool, drain_sql, flow_id=JobId(flow_id))
        )
        # Wait for the chunk boundary: chunk 1 committed (total > 0) and
        # chunk 2's transaction is paused. Poll the row count of promoted
        # rows to know chunk 1 landed, then read the intent row.
        boundary = False
        for _ in range(200):
            await asyncio.sleep(0.05)
            facts = await _waiter_facts(module_pg_pool, schema, flow_id)
            if sum(1 for f in facts if f["status"] == "pending") == 200:
                boundary = True
                break
        assert boundary, "the ceiling transaction never committed its 200 wins"
        intent = await _intent_row(module_pg_pool, schema, flow_id)
        # THE PENDING BOUNDARY: the ceiling transaction left the intent
        # pending (a mutant marking handled before the no-fit read reds).
        assert intent["handled"] < intent["wake"], intent
        wake = await drain_task
        assert wake.promoted == 201, wake
        assert wake.intent_completed is True, wake
