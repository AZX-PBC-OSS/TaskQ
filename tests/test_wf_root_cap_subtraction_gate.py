"""The flow-root subtraction's execution gate and row set (#747).

``capped_running`` computes an actor's in-flight count as its running rows,
minus — for ``wf`` only — its live flow roots (``step_key = '__flow__'``),
because a root is a cache the dispatch fence never claims. Two properties of
that expression decide whether it is safe, and neither is observable from the
round's outcome alone:

* **the gate.** The subtraction sits inside ``CASE WHEN live.max_concurrent IS
  NULL THEN 0 ELSE ... END``. The module's contract is that an uncapped fleet
  materializes an empty CTE and does ZERO running-row work per round (see the
  ``capped_running`` header in ``backend/_dispatch_sql.py``). The whole
  statement's cost estimate is identical either way, so only the executed plan
  can show that the extra count never runs when no cap is declared. Nothing
  else observes this: every row-work oracle in the suite keys on the count's
  ``rj`` alias, and the subtraction's alias is ``rf``.

* **the row set.** When the branch IS taken, the count must read exactly the
  running roots — one heap row per running ``wf`` row (``jobs_actor_running_idx``
  carries no ``step_key``) — so the subtracted population is the roots and
  nothing else.

Both are measured on the executed plan of the fleet's own claim statement,
``EXPLAIN (ANALYZE)`` over the rendered ``DISPATCH_STRICT_FIFO_SQL``.
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier, not user input; the catalog-derived double-quoted {col_list} column identifiers come from this schema's own information_schema.columns; every value is $-bound.
import json
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "default"
_FLOW = "wf_root_cap_gate_flow"
# Enough roots that a rooted plan's scan is unmistakable, few enough that the
# end-of-test status reconstruction walks a short list.
_ROOTS = 500


async def _only(ctx: StepContext) -> int:
    return 1  # pragma: no cover - the fixture claims, never executes


def _app() -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow(_FLOW)
    def one_step() -> Promise[object]:  # pyright: ignore[reportUnusedFunction]  # Why: registered by the decorator, read back through app.get.
        return build(step(_only, key="only"))

    return app


async def _seed(
    conn: asyncpg.Connection, schema: str, cap: int | None, neighbour_cap: int | None = None
) -> UUID:
    """The queue row, ``wf``'s stored cap (plus an optional capped neighbour),
    and one ``workflow_execution``-capable worker (the dispatch fence's
    execution leg admits ``wf`` rows only for such a worker)."""
    await conn.execute(
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET mode = $2",
        _QUEUE,
        "strict_fifo",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
        "VALUES ('wf', $1, $2) ON CONFLICT (actor) DO UPDATE SET queue = $1, max_concurrent = $2",
        _QUEUE,
        cap,
    )
    if neighbour_cap is not None:
        # A capped actor beside wf, so the capped_running CTE is actually
        # materialized on the round: without one, no reference site takes its
        # capped branch and the WHOLE CTE stays unexecuted, which would hide
        # whether wf's own uncapped row reaches the subtraction.
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
            "VALUES ('wf_root_cap_neighbour', $1, $2) "
            "ON CONFLICT (actor) DO UPDATE SET queue = $1, max_concurrent = $2",
            _QUEUE,
            neighbour_cap,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, attempt, step_key) "
            "VALUES (gen_random_uuid(), 'wf_root_cap_neighbour', $1, '{}'::jsonb, "
            "'pending', 0, clock_timestamp() - interval '1 minute', 3, "
            "'transient', 0, 'n')",
            _QUEUE,
        )
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'wf-root-cap-gate', 1, $2, $3::jsonb)",
        worker_id,
        [_QUEUE],
        json.dumps({"workflow_execution": True}),
    )
    return worker_id


def _rf_nodes(plan: dict[str, Any]) -> list[tuple[float, float, str]]:
    """(rows, loops, node type) for every scan under the root-count alias.

    ``Actual Loops`` is 0 for a plan node the executor never ran; the node is
    still printed, which is exactly what distinguishes "the branch was never
    taken" from "the branch was taken and matched nothing".
    """
    found: list[tuple[float, float, str]] = []
    stack: list[dict[str, Any]] = [plan]
    while stack:
        node = stack.pop()
        alias = node.get("Alias")
        if isinstance(alias, str) and alias.startswith("rf"):
            found.append(
                (
                    float(node.get("Actual Rows", 0) or 0),
                    float(node.get("Actual Loops", 0) or 0),
                    str(node.get("Node Type")),
                )
            )
        stack.extend(node.get("Plans") or [])
    return found


async def _plan_roots_round(
    conn: asyncpg.Connection, schema: str, worker_id: UUID
) -> dict[str, Any]:
    """The rendered claim statement's executed plan, rolled back after."""
    sql = DISPATCH_STRICT_FIFO_SQL.format(schema=schema)
    tr = conn.transaction()
    await tr.start()
    try:
        raw = await conn.fetchval(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql,
            [_QUEUE],
            10,
            worker_id,
            _LEASE,
            2,
        )
    finally:
        await tr.rollback()
    document: Any = json.loads(raw) if isinstance(raw, str) else raw
    top: dict[str, Any] = document[0]
    return top


async def _live_roots(conn: asyncpg.Connection, schema: str, flow_id: UUID, clones: int) -> None:
    """*clones* extra live roots beside the real run's own one (the create's
    root plus a due step), so ``wf`` holds a root per row with no work at
    all behind them."""
    cols = [
        str(r["column_name"])
        for r in await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = 'jobs' AND is_generated = 'NEVER' "
            "AND column_name NOT IN ('id', 'idempotency_key') ORDER BY ordinal_position",
            schema,
        )
    ]
    col_list = ", ".join(f'"{c}"' for c in cols)
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, {col_list}) '
        f'SELECT gen_random_uuid(), {col_list} FROM "{schema}".jobs, generate_series(1, $2) '
        "WHERE id = $1",
        flow_id,
        clones,
    )
    roots = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE actor = 'wf' AND status = 'running' AND step_key = '__flow__'"
    )
    assert roots == clones + 1, roots


async def test_the_root_count_never_runs_for_an_uncapped_actor(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """``wf`` uncapped with live roots, beside a capped actor that forces the
    ``capped_running`` CTE to materialize: the executor never calls the root
    count.

    The plan shape is the same whether or not a cap is declared (the CASE arm
    is template text), so a round that runs the root count for an uncapped
    actor pays a jobs read proportional to that actor's running rows on every
    claim round — the per-round, fleet-proportional work the ``capped_running``
    gate exists to avoid. The capped neighbour is what makes that reachable at
    all: with an all-uncapped fleet no reference site takes the CTE's capped
    branch, the CTE is never materialized, and the pin would pass without
    measuring anything.
    """
    schema = wf_schema
    worker_id = await _seed(wf_conn, schema, cap=None, neighbour_cap=1)
    flow_id = (await FlowRunner(_app().get(_FLOW), wf_pool, schema).create_flow()).flow_id
    try:
        await _live_roots(wf_conn, schema, flow_id, _ROOTS - 1)
        await wf_conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs, "{schema}".actor_config')

        top = await _plan_roots_round(wf_conn, schema, worker_id)
    finally:
        # Clone roots have no node rows; drop them before the end-of-test
        # status reconstruction walks the schema's roots.
        await wf_conn.execute(
            f"DELETE FROM \"{schema}\".jobs WHERE step_key = '__flow__' AND id <> $1",
            flow_id,
        )

    nodes = _rf_nodes(top["Plan"])
    executed = [node for node in nodes if node[1] > 0]
    assert nodes, (
        "the capped neighbour's round carries no root-count subplan in its "
        "plan — the subtraction is missing from the rendered statement"
    )
    assert not executed, (
        "an uncapped round executed the flow-root count subplan "
        f"({executed}, of plan nodes {nodes}) — with {_ROOTS} live roots on an "
        "actor that declares no cap, every claim round reads a jobs scan it "
        "cannot act on"
    )


async def test_the_root_count_reads_exactly_the_running_roots(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """``wf`` capped with live roots: the subtraction's subplan runs once and
    yields exactly the running roots — not the run's pending step, not a
    neighbouring actor's rows.

    This is the keyed-to-the-literal-``'wf'`` read that the cap arithmetic
    subtracts, measured where it is read rather than inferred from admission.
    """
    schema = wf_schema
    worker_id = await _seed(wf_conn, schema, cap=5)
    flow_id = (await FlowRunner(_app().get(_FLOW), wf_pool, schema).create_flow()).flow_id
    try:
        await _live_roots(wf_conn, schema, flow_id, _ROOTS - 1)
        await wf_conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs, "{schema}".actor_config')

        top = await _plan_roots_round(wf_conn, schema, worker_id)
    finally:
        await wf_conn.execute(
            f"DELETE FROM \"{schema}\".jobs WHERE step_key = '__flow__' AND id <> $1",
            flow_id,
        )

    nodes = _rf_nodes(top["Plan"])
    assert nodes, (
        "the capped round's plan carries no root-count subplan: the subtraction "
        "is missing from the rendered statement"
    )
    read = sum(rows * max(loops, 1.0) for rows, loops, _ in nodes)
    assert read == _ROOTS, (
        f"the root-count subplan read {read} rows for {_ROOTS} running roots "
        f"(plan nodes {nodes}) — the subtracted population is not exactly the "
        "actor's live roots"
    )
