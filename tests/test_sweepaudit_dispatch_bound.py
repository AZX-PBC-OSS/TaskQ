"""Dispatch candidates-lateral bound pins: STABLE bound, index-served.

The sweep/cron index audit (tests/test_index_audit.py, the
statement_timestamp() rewrite in taskq.backend._sweeps) established the
two-clock doctrine: row-selection RANGE predicates must use a STABLE
``now`` so the planner can serve them as btree Index Conds — a VOLATILE
``clock_timestamp()`` bound degrades to a post-scan Filter that walks
the pending backlog per dispatch round. The dispatch candidates laterals
(dispatch's hottest path) were left on the volatile bound.

Measured on a 20k-row not-yet-due pending backlog (PostgreSQL 18,
EXPLAIN ANALYZE BUFFERS, this file's seed shape):

* volatile bound: Index Scan with ``Filter: (scheduled_at <=
  clock_timestamp())``, ``Rows Removed by Filter: 20000``, 20,172
  buffers, ~5.1 ms — the whole backlog walked to admit 10 due rows;
* stable bound: ``Index Cond: (... AND scheduled_at <=
  statement_timestamp())`` on jobs_actor_dispatch_idx, 10 buffers,
  ~0.04 ms — the scan terminates at the range boundary.

The written values (started_at / last_heartbeat_at / lock_expires_at)
stay ``clock_timestamp()`` per the same doctrine; only the row-selection
bounds move.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import (
    _ROUND_ROBIN_CANDIDATES_LATERAL,  # pyright: ignore[reportPrivateUsage]  # Why: pinning the production lateral, not a copy; a copy could drift from the SQL that actually runs.
    _STRICT_FIFO_CANDIDATES_LATERAL,  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)

pytestmark = pytest.mark.integration

_BOTH_LATERALS = (
    ("strict_fifo", _STRICT_FIFO_CANDIDATES_LATERAL),
    ("round_robin", _ROUND_ROBIN_CANDIDATES_LATERAL),
)


# ── 1. SQL shape: STABLE row-selection bounds ─────────────────────────


@pytest.mark.parametrize(("variant", "lateral"), _BOTH_LATERALS, ids=[v for v, _ in _BOTH_LATERALS])
def test_candidates_lateral_bounds_are_stable(variant: str, lateral: str) -> None:
    """The scheduled_at / schedule_to_close selection bounds must be
    ``statement_timestamp()`` (STABLE) so they can serve as Index Conds."""
    assert "j2.scheduled_at <= statement_timestamp()" in lateral, (
        f"{variant}: scheduled_at bound must be STABLE (statement_timestamp)"
    )
    assert "j2.schedule_to_close > statement_timestamp()" in lateral, (
        f"{variant}: schedule_to_close bound must be STABLE (statement_timestamp)"
    )
    assert "clock_timestamp()" not in lateral, (
        f"{variant}: a VOLATILE clock_timestamp() bound in the candidates "
        "lateral degrades to a post-scan Filter over the pending backlog"
    )


def test_written_values_stay_clock_timestamp() -> None:
    """Only the selection bounds move; written timestamps stay volatile
    wall-clock per the two-clock doctrine (co-monotonic with the rows the
    statement writes)."""
    for sql in (DISPATCH_STRICT_FIFO_SQL, DISPATCH_ROUND_ROBIN_SQL):
        assert "lock_expires_at = clock_timestamp()" in sql
        assert "started_at = clock_timestamp()" in sql


# ── 2. Plan pin: the bound is index-served on the dispatch index ──────


@pytest.fixture(scope="module")
async def dispatch_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, bulk-seeded into the shape
    that exposes the walk: a 20k-row PENDING backlog that is not yet due
    (high priority, scheduled_at in the future — the direct-INSERT /
    restored-dump shape; TaskQ's own writers classify future-scheduled
    rows as 'scheduled', so this pin also keeps the defensive bound
    index-servable for any writer that does not) plus a handful of due
    rows at lower priority.
    """
    schema = f"dispatch_audit_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)

        now = datetime.now(UTC)
        actor, queue = "dispatch_probe", "default"

        # 20k not-yet-due pending rows at the head of the index order.
        future_rows = [
            (
                new_uuid(),
                actor,
                queue,
                '{"v": 1}',
                "pending",
                100,
                now + timedelta(hours=1),
                3,
                "transient",
            )
            for _ in range(20_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "priority",
                "scheduled_at",
                "max_attempts",
                "retry_kind",
            ],
            records=future_rows,
        )
        # 20 due rows behind them in index order.
        due_rows = [
            (
                new_uuid(),
                actor,
                queue,
                '{"v": 1}',
                "pending",
                1,
                now - timedelta(minutes=1),
                3,
                "transient",
            )
            for _ in range(20)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "priority",
                "scheduled_at",
                "max_attempts",
                "retry_kind",
            ],
            records=due_rows,
        )
        await conn.execute(f'ANALYZE "{schema}".jobs')
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _explain(conn: asyncpg.Connection, sql: str, *params: object) -> str:
    rows = await conn.fetch(f"EXPLAIN (BUFFERS) {sql}", *params)
    return "\n".join(r["QUERY PLAN"] for r in rows)


async def test_dispatch_lateral_scheduled_at_bound_is_index_served(
    pg_dsn: str, dispatch_schema: str
) -> None:
    """EXPLAIN the production strict-FIFO lateral (verbatim constant,
    wrapped only with the outer CTE names it references): the
    scheduled_at bound must appear in an Index Cond on
    jobs_actor_dispatch_idx — the migration's promised "bounded LATERAL
    range scan decoupled from backlog depth". A bound that only appears
    as a Filter walks the whole not-yet-due backlog per dispatch round.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        lateral = _STRICT_FIFO_CANDIDATES_LATERAL.format(schema=dispatch_schema)
        # Mirrors the production candidates CTE's FROM shape
        # (pac CROSS JOIN LATERAL sq CROSS JOIN LATERAL (<lateral>) j) so
        # the lateral's outer references resolve exactly as at dispatch
        # time; only the outer producers are literalized.
        wrapped = (
            "WITH params AS (SELECT 2::int AS oversample) "
            "SELECT * FROM (SELECT 'dispatch_probe'::text AS actor, 10::int AS residual) pac "
            "CROSS JOIN LATERAL (VALUES ('default'::text)) AS sq(queue_name) "
            f"CROSS JOIN LATERAL ({lateral}) j"
        )
        plan = await _explain(conn, wrapped)

        assert "jobs_actor_dispatch_idx" in plan, (
            f"expected the per-(actor, queue) dispatch index in the plan:\n{plan}"
        )
        cond_lines = [line for line in plan.splitlines() if "Index Cond:" in line]
        assert cond_lines and any("scheduled_at <=" in line for line in cond_lines), (
            "expected an Index Cond containing 'scheduled_at <= ...' on "
            "jobs_actor_dispatch_idx; a bound that only appears as a Filter "
            f"is not index-served:\n{plan}"
        )
    finally:
        await conn.close()
