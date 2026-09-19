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

The shipped statement counts running rows per capped actor instead: a
correlated count gated on ``ac.max_concurrent IS NOT NULL`` inside the
CASE that computes the residual (and the same gate in
``eligible_candidates``' post-lock re-check), so the count subplan is
evaluated only for actors that declared a cap, reading only that
actor's own ``jobs_actor_running_idx`` entries. An uncapped fleet - the
default - does zero running-row work per round, and a capped fleet pays
its own capped actors' running rows, never the fleet's.

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
) -> UUID:
    """Re-seed the round's fixed due backlog and the running population.

    ``running_rows`` rows are spread over
    ``_UNRELATED_RUNNING_ACTORS`` never-polled actors (running, lease
    live, no identity_key) - the fleet's running load. The polled
    actor's own cap is applied to its actor_config row so the caller can
    exercise the gated count's taken branch (a capped actor with its own
    running rows) beside the untaken one. Returns the worker id the
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
