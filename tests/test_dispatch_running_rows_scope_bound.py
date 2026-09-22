"""A dispatch round's running-row work is cap-gated, never fleet-wide.

The axis this module isolates: RUNNING rows. Pending backlog is held
fixed (the round's own small due backlog only), and only the fleet's
running population grows - unrelated actors' rows this round never
admits, plus (in the capped shape) the polled actor's own running rows.
The pre-fix statement paid this axis on every round through the
``running_per_actor`` CTE: referenced three times, so materialized once
per round, scanning and aggregating EVERY running row in the fleet
(``SELECT actor, count(*) FROM jobs WHERE status='running' GROUP BY
actor``) whether or not any actor declared ``max_concurrent`` - the
cost rode every claim round at O(fleet running rows).

The shipped statement precomputes the running count per capped actor
ONCE per round in the ``capped_running`` CTE (the #283 fix): its driver
is the round's own live-actor population, its count subplan is gated on
``ac.max_concurrent IS NOT NULL`` inside a CASE, and all three former
count sites (both capacity CTEs' residuals and
``eligible_candidates``' post-lock re-check) read the precomputed value,
so the count subplan is evaluated only for actors that declared a cap,
reading only that actor's own ``jobs_actor_running_idx`` entries. An
uncapped fleet - the default - materializes an empty CTE and does zero
running-row work per round, and a capped fleet pays its own capped
actors' running rows once, never the fleet's and never per claimed row.

Oracle: EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) of the production
constants, executed once per seeded running-rows size; a node's
``Actual Rows`` x ``Actual Loops`` is its exact total row work (the
doctrine shared with tests/test_dispatch_backlog_depth_bound.py and
tests/test_dispatch_actor_registry_scope_bound.py). A cap-gated round's
widest node carries the round's own candidate/locked set (the fixed
pending backlog) at every running-rows size; only running-row
proportional work (the materialized fleet-wide count) grows with the
axis.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import (
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining.
)

pytestmark = pytest.mark.integration

_LIMIT_N = 50
_OVERSAMPLE = 2
_LOCK_LEASE = timedelta(seconds=30)

_VARIANTS = (
    ("round_robin", DISPATCH_ROUND_ROBIN_SQL),
    ("strict_fifo", DISPATCH_STRICT_FIFO_SQL),
)

# The round's own slice: one polled actor, one polled queue, a small
# fixed due backlog - identical at every running-rows size below.
_POLLED_QUEUE = "running_scope_polled_q"
_POLLED_ACTOR = "running_scope_polled_actor"
_OWN_PENDING = 60

# The running-row axis: rows of actors this round never polls (and
# never could admit - their actor_config rows name other queues). Pure
# running-population bloat, the exact population the fleet-wide
# running_per_actor CTE paid for on every round.
_UNRELATED_RUNNING_ACTORS = 50
_RUNNING_SIZES = (0, 1_000)

# A correctly gated round's work is bounded by its own pending
# candidate/locked set (~limit_n * oversample plus the bounded lock and
# eligibility stages), never by the running population: generous
# headroom over the round's own ~120-row work while still catching a
# scan of a thousand running rows.
_NODE_ROW_BOUND = 600
_RUNNING_RATIO_BOUND = 3

# The cap axis for the #283 pin: the per-round running-count work must
# be flat (linear) in the actor's declared cap, never the
# O(oversample x cap^2) the correlated per-claimed-row recount paid
# (measured 28/648/2550/10100 running-index rows at these caps on the
# pre-CTE shape, work/cap^2 ~1.0). The precomputed capped_running CTE
# evaluates the count ONCE per capped live actor per round: the actor's
# own running-row scan, ~cap/2 rows at the sweep's own_running=cap // 2
# seeding - so a LINEAR bound (work <= cap) is the pin, and the
# cap=100/cap=5 ratio stays under the linear 25x with headroom while
# the quadratic shape's 360x fails it.
_CAPS = (5, 25, 50, 100)
_CAP_LINEAR_RATIO_BOUND = 30


@pytest.fixture(scope="module")
async def running_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, one registered actor."""
    schema = f"dispatch_runscope_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            _POLLED_ACTOR,
            _POLLED_QUEUE,
        )
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _seed(
    conn: asyncpg.Connection,
    schema: str,
    running_rows: int,
    *,
    polled_actor_cap: int | None,
    own_running: int = 0,
) -> UUID:
    """Re-seed the round's fixed due backlog and the running population.

    ``running_rows`` rows are spread over
    ``_UNRELATED_RUNNING_ACTORS`` never-polled actors (running, lease
    live, no identity_key) - the fleet's running load. The polled
    actor's own cap is applied to its actor_config row so the caller can
    exercise the gated count's taken branch (a capped actor with its own
    running rows) beside the untaken one. ``own_running`` seeds that
    many of the polled actor's OWN running rows (0 by default; the
    capped-count test inserts its own two rows so it can hold them under
    the worker id it asserts against). Returns the worker id the
    running rows are locked to.
    """
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(f'TRUNCATE TABLE "{schema}".actor_config CASCADE')
    await conn.execute(f'TRUNCATE TABLE "{schema}".workers CASCADE')
    if polled_actor_cap is None:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            _POLLED_ACTOR,
            _POLLED_QUEUE,
        )
    else:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
            "VALUES ($1, $2, $3)",
            _POLLED_ACTOR,
            _POLLED_QUEUE,
            polled_actor_cap,
        )
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
        "VALUES ($1, 'runscope-host', 1, ARRAY[$2])",
        worker_id,
        _POLLED_QUEUE,
    )
    # The round's own due backlog: identical at every running-rows size.
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind) "
        "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
        "(g % 3)::smallint, clock_timestamp() - interval '1 minute', 3, "
        "'transient' FROM generate_series(1, $3::int) AS g",
        _POLLED_ACTOR,
        _POLLED_QUEUE,
        _OWN_PENDING,
    )
    if own_running:
        # The polled actor's own running rows, live leases: the
        # population the capped count subplan reads (the gate's taken
        # branch).
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, attempt, locked_by_worker, "
            "lock_expires_at, started_at, last_heartbeat_at) "
            "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, "
            "'running', 0, clock_timestamp() - interval '1 minute', 3, "
            "'transient', 1, $3, "
            "clock_timestamp() + interval '5 minutes', "
            "clock_timestamp(), clock_timestamp() "
            "FROM generate_series(1, $4::int)",
            _POLLED_ACTOR,
            _POLLED_QUEUE,
            worker_id,
            own_running,
        )
    if running_rows:
        # The running population: never-polled actors, live leases. On
        # the pre-fix statement this is the population the
        # running_per_actor CTE scanned and aggregated per round.
        per_actor = running_rows // _UNRELATED_RUNNING_ACTORS
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, attempt, locked_by_worker, "
            "lock_expires_at, started_at, last_heartbeat_at) "
            "SELECT gen_random_uuid(), "
            "'running_scope_other_actor_' || (g % $1)::text, "
            "'running_scope_other_queue_' || (g % $1)::text, "
            "'{\"v\": 1}'::jsonb, 'running', 0, "
            "clock_timestamp() - interval '1 minute', 3, 'transient', 1, "
            "$2, clock_timestamp() + interval '5 minutes', "
            "clock_timestamp(), clock_timestamp() "
            "FROM generate_series(1, $3::int) AS g",
            _UNRELATED_RUNNING_ACTORS,
            worker_id,
            per_actor * _UNRELATED_RUNNING_ACTORS,
        )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs, "{schema}".actor_config')
    return worker_id


def _plan_node_row_counts(plan: dict[str, Any]) -> list[tuple[float, str]]:
    """(rows * loops, node label) for every plan node, widest first."""
    counted: list[tuple[float, str]] = []
    stack: list[dict[str, Any]] = [plan]
    while stack:
        node = stack.pop()
        rows = float(node.get("Actual Rows", 0) or 0)
        loops = int(node.get("Actual Loops", 1) or 1)
        node_type = str(node.get("Node Type", "unknown node"))
        relation = node.get("Relation Name")
        label = node_type if relation is None else f"{node_type} on {relation}"
        counted.append((rows * loops, label))
        stack.extend(node.get("Plans") or [])
    counted.sort(key=lambda entry: entry[0], reverse=True)
    return counted


def _count_subplan_row_work(plan: dict[str, Any]) -> float:
    """Total row work of the running-count subplans: every scan of jobs
    under the statement's ``rj`` count alias, whether the planner served
    it from a running partial index or a seq scan (at small fleet sizes
    it picks either)."""
    total = 0.0
    stack: list[dict[str, Any]] = [plan]
    while stack:
        node = stack.pop()
        alias = node.get("Alias")
        if isinstance(alias, str) and alias.startswith("rj"):
            rows = float(node.get("Actual Rows", 0) or 0)
            loops = int(node.get("Actual Loops", 1) or 1)
            total += rows * loops
        stack.extend(node.get("Plans") or [])
    return total


async def _explain_row_work(
    conn: asyncpg.Connection, sql: str, worker_id: UUID
) -> tuple[float, list[tuple[float, str]]]:
    rows = await conn.fetch(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}",
        [_POLLED_QUEUE],
        _LIMIT_N,
        worker_id,
        _LOCK_LEASE,
        _OVERSAMPLE,
    )
    raw = rows[0]["QUERY PLAN"]
    document: Any = json.loads(raw) if isinstance(raw, str) else raw
    top: dict[str, Any] = document[0]
    return float(top["Execution Time"]), _plan_node_row_counts(top["Plan"])


@pytest.mark.parametrize(("variant", "sql"), _VARIANTS, ids=[v for v, _ in _VARIANTS])
async def test_uncapped_round_ignores_the_fleet_running_population(
    pg_dsn: str, running_schema: str, variant: str, sql: str
) -> None:
    """An uncapped actor's dispatch round does zero running-row work,
    whether the fleet holds zero running rows or a thousand.

    The pre-fix statement materialized the fleet-wide running count on
    every round regardless of caps - this is the axis that made it a
    per-round tax proportional to fleet concurrency. The gated count's
    branch is never taken for an uncapped actor, so no plan node may
    carry the running population's rows.
    """
    rendered = sql.format(schema=running_schema)
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        widest_by_running: dict[int, float] = {}
        for running_rows in _RUNNING_SIZES:
            await _seed(conn, running_schema, running_rows, polled_actor_cap=None)
            execution_ms, nodes = await _explain_row_work(conn, rendered, worker_id)
            widest, label = nodes[0]
            widest_by_running[running_rows] = widest
            assert widest <= _NODE_ROW_BOUND, (
                f"{variant}: with {running_rows} unrelated running rows and "
                f"no actor declaring a cap, the dispatch plan's widest node "
                f"({label}) did {widest:.0f} rows of work (bound "
                f"{_NODE_ROW_BOUND}) - the round is counting or scanning the "
                "fleet's running population it must not touch (the "
                "running_per_actor CTE regression). Widest nodes: "
                f"{nodes[:5]}; execution {execution_ms:.2f} ms."
            )
        empty_widest = widest_by_running[_RUNNING_SIZES[0]]
        full_widest = widest_by_running[_RUNNING_SIZES[-1]]
        assert full_widest <= _RUNNING_RATIO_BOUND * max(empty_widest, 1.0), (
            f"{variant}: the dispatch plan's widest node grows with the "
            f"fleet's running population - {empty_widest:.0f} rows of work "
            f"with {_RUNNING_SIZES[0]} running rows vs {full_widest:.0f} with "
            f"{_RUNNING_SIZES[-1]}, while the round's own pending backlog "
            f"never changed (ratio bound {_RUNNING_RATIO_BOUND}x). An "
            "uncapped round must not pay for running rows it cannot admit."
        )
    finally:
        await conn.close()


@pytest.mark.parametrize(("variant", "sql"), _VARIANTS, ids=[v for v, _ in _VARIANTS])
async def test_capped_round_pays_only_its_own_running_rows(
    pg_dsn: str, running_schema: str, variant: str, sql: str
) -> None:
    """A capped actor's round counts its OWN running rows (the gate's
    taken branch) and still never the fleet's.

    The polled actor declares ``max_concurrent`` and holds two of its
    own running rows, so the gated count executes - a scan of that
    actor's running-row partial-index entries - while the fleet's
    unrelated running population must not move the widest node: the
    count is correlated per actor, not a fleet-wide aggregate.
    """
    rendered = sql.format(schema=running_schema)
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        widest_by_running: dict[int, float] = {}
        for running_rows in _RUNNING_SIZES:
            holder = await _seed(
                conn,
                running_schema,
                running_rows,
                polled_actor_cap=5,
            )
            # The polled actor's own running rows: held under a live
            # lease, so the residual arithmetic must count them (the
            # gate's taken branch) - capped at 5 with 2 running, the
            # round admits at most 3 more.
            await conn.execute(
                f'INSERT INTO "{running_schema}".jobs '
                "(id, actor, queue, payload, status, priority, scheduled_at, "
                "max_attempts, retry_kind, attempt, locked_by_worker, "
                "lock_expires_at, started_at, last_heartbeat_at) "
                "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, "
                "'running', 0, clock_timestamp() - interval '1 minute', 3, "
                "'transient', 1, $3, "
                "clock_timestamp() + interval '5 minutes', "
                "clock_timestamp(), clock_timestamp() "
                "FROM generate_series(1, 2)",
                _POLLED_ACTOR,
                _POLLED_QUEUE,
                holder,
            )
            await conn.execute(f'VACUUM (ANALYZE) "{running_schema}".jobs')
            execution_ms, nodes = await _explain_row_work(conn, rendered, worker_id)
            widest, label = nodes[0]
            widest_by_running[running_rows] = widest
            assert widest <= _NODE_ROW_BOUND, (
                f"{variant}: with {running_rows} unrelated running rows, a "
                f"capped polled actor, and 2 of its own running rows, the "
                f"dispatch plan's widest node ({label}) did {widest:.0f} rows "
                f"of work (bound {_NODE_ROW_BOUND}) - the capped count must "
                "read only its own actor's running rows, never the fleet's "
                "(the running_per_actor CTE regression). Widest nodes: "
                f"{nodes[:5]}; execution {execution_ms:.2f} ms."
            )
        empty_widest = widest_by_running[_RUNNING_SIZES[0]]
        full_widest = widest_by_running[_RUNNING_SIZES[-1]]
        assert full_widest <= _RUNNING_RATIO_BOUND * max(empty_widest, 1.0), (
            f"{variant}: the capped round's widest node grows with the "
            f"fleet's running population - {empty_widest:.0f} rows of work "
            f"with {_RUNNING_SIZES[0]} running rows vs {full_widest:.0f} with "
            f"{_RUNNING_SIZES[-1]} - the count is per capped actor and must "
            f"not move with the fleet's running rows (ratio bound "
            f"{_RUNNING_RATIO_BOUND}x)."
        )
    finally:
        await conn.close()


@pytest.mark.parametrize(("variant", "sql"), _VARIANTS, ids=[v for v, _ in _VARIANTS])
async def test_capped_count_work_is_flat_in_cap(
    pg_dsn: str, running_schema: str, variant: str, sql: str
) -> None:
    """A capped round's running-count work is LINEAR in the cap (the
    #283 pin).

    The correlated-count form this pin replaces re-evaluated the count
    per CLAIMED row in eligible_candidates: a capped fleet paid
    O(oversample x cap^2) running-index rows per round - measured
    28/648/2550/10100 rows at caps 5/25/50/100 (work/cap^2 ~1.0,
    crossover against the fleet-wide CTE at cap~30, the issue's red).
    The precomputed capped_running CTE evaluates the count ONCE per
    capped live actor: the sweep seeds the polled actor with half its
    cap as own running rows, so the per-round count work is ~cap/2 -
    every cap's work must stay under a LINEAR bound, and the cap=100 to
    cap=5 ratio under the linear ratio with headroom (the quadratic
    shape's ~360x fails both). The fleet axis rides at a thousand
    unrelated running rows, the sweep must not move with it.
    """
    rendered = sql.format(schema=running_schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        work_by_cap: dict[int, float] = {}
        for cap in _CAPS:
            # Re-seed per cap: EXPLAIN ANALYZE executes the claim for
            # real (its writes are the statement's own side effects,
            # only its output is discarded), so every sweep point needs
            # an identical fresh fleet.
            await _seed(
                conn,
                running_schema,
                _RUNNING_SIZES[-1],
                polled_actor_cap=cap,
                own_running=cap // 2,
            )
            rows = await conn.fetch(
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {rendered}",
                [_POLLED_QUEUE],
                _LIMIT_N,
                new_uuid(),
                _LOCK_LEASE,
                _OVERSAMPLE,
            )
            raw = rows[0]["QUERY PLAN"]
            document: Any = json.loads(raw) if isinstance(raw, str) else raw
            work = _count_subplan_row_work(document[0]["Plan"])
            work_by_cap[cap] = work
            assert work <= cap, (
                f"{variant}: at cap {cap} the round's running-count work "
                f"was {work:.0f} rows (linear bound {cap}) - the count is "
                "no longer evaluated once per capped actor (a "
                "per-claimed-row recount, the #283 regression, scales it "
                "by the claim batch)."
            )
        ratio = work_by_cap[_CAPS[-1]] / max(work_by_cap[_CAPS[0]], 1.0)
        assert ratio <= _CAP_LINEAR_RATIO_BOUND, (
            f"{variant}: the running-count work scales quadratically in "
            f"the cap - {work_by_cap[_CAPS[0]]:.0f} rows at cap "
            f"{_CAPS[0]} vs {work_by_cap[_CAPS[-1]]:.0f} at cap "
            f"{_CAPS[-1]} (ratio {ratio:.0f}x, linear bound "
            f"{_CAP_LINEAR_RATIO_BOUND}x) - the per-claimed-row recount "
            "is back (the #283 regression)."
        )
    finally:
        await conn.close()
