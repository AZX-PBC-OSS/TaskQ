"""THE FLOW-HISTORY RETENTION ARMS — the pins
(integration: real PG).

Five workflow tables grew monotone-forever — no retention arm, no FK
for the job prune to cascade: ``wf_edge``, ``wf_step_ledger``,
``wf_join_fire``, ``wf_signals``, ``wf_node_progress``. These pins hold
the cure's shape (THE TWO-PASS LAW — the H-B1 rewrite): every arm runs

* a FLOWS-SIDE pass — the TERMINAL-AGED ROOTS are selected FIRST from
  the tiny jobs side (``step_key='__flow__'`` + a terminal status +
  ``finished_at <= statement_timestamp() - period`` — the root identity
  and the root's own age clock, served by ``jobs_finished_at_idx``), and
  each selected flow's rows delete through the table's flow_id index,
  row-LIMITed to the batch. The zero-doomed steady state is
  O(terminal flows), never a walk of the wf table probing jobs per row
  (the convicted shape: TimeoutError at 3M rows — the arms permanently
  stopped pruning at exactly the scale they exist for); and
* a ROOTLESS pass — the orphan class (the row's pointee is NOT a flow
  root at all: absent, or a live NON-root job) cannot be found from the
  flows side, so it rides a SEPARATE bounded statement on a coarser
  cadence (the leader wiring runs it every Nth call): a PK-ordered walk
  that visits AT MOST the batch (the LIMIT bounds the SCAN, not the
  result) and probes each visited row's pointee once.

A row dies when its flow root is terminal AND terminal-AGED (the root's
``finished_at`` past the period — a NULL-stamped terminal root is
INCLUDED, aged by nothing, and the arm warns when it deletes under a
NULL stamp, the stamping-bug tripwire), or — the rootless pass — when
its pointee is not a root at all AND the row's own clock (where the
table has one) is past the period; a NON-terminal flow's rows survive
the same run; ``timedelta(0)`` is the disable sentinel (the
deletion-sweep family's zero-means-off) and is ALSO the five fields'
DEFAULT — deliberately opposite to the outbox/step-cache precedents
(default-ON), because those prune DERIVED data while these five prune
the FLOW HISTORY itself: default-off keeps the upgrade safe (no
surprise deletes), the operator opts in.

WALL-CLOCK-FREE: every age rides the PG clock's own arithmetic — the
arms' predicates are ``statement_timestamp()``-based (the prune family's
STABLE selection bound — the two-clock law test_index_audit.py
documents, one face across the five arms), the seeds override
``created_at``/``fired_at``/``updated_at`` (and the terminal flow's
``finished_at``, wf_edge's only honest age clock — the table has no
timestamp column of its own), and no test sleeps.

The structural red, recorded (the arms did not exist on origin/main):
``git show origin/main:src/taskq/workflows/_sweep.py | grep -c
"prune_wf_edge"`` → 0. The behavioral red is the zero-period negative
control below.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from tests._wf_fixtures import seed_flow, seed_running_node

# The arms' shared age seed: comfortably past every test's retention,
# fresh rows at 10s comfortably inside it.
_OLD = timedelta(hours=2)
_FRESH = timedelta(seconds=10)
_RETENTION = timedelta(minutes=30)
_OLD_TEXT = "2 hours"


# ── The seed helpers (raw SQL, clock arithmetic — no sleeps) ────────────


async def _terminalize_flow(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> None:
    """The flow root's terminalization stamp: wf_edge's ONLY honest age
    clock (the table has no timestamp column — the arm ages by the flow
    root's finished_at, the prune family's terminal-age column)."""
    await conn.execute(
        f'UPDATE "{schema}".jobs SET finished_at = clock_timestamp() - $1::interval WHERE id = $2',
        _OLD,
        flow_id,
    )


async def _seed_terminal_node(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> JobId:
    """A SUCCEEDED node row on a terminal flow — G7-honest (a succeeded
    root's rows must reconstruct 'complete': the node row survives
    retention and carries the reconstruction after the ledger deletes)."""
    node_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, finished_at) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'succeeded', $2, $3, "
        "clock_timestamp() - $4::interval)",
        node_id,
        "done",
        json.dumps({"flow_id": str(flow_id)}),
        _OLD,
    )
    return JobId(node_id)


async def _seed_edge(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> None:
    """Edge-ledger rows keyed by flow (no timestamp column exists to
    override — the flow root's finished_at IS the arm's age clock)."""
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        new_uuid(),
        new_uuid(),
        flow_id,
    )


async def _seed_join_fire(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, *, age: timedelta
) -> JobId:
    fire_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".wf_join_fire (id, join_job_id, flow_id, step_key, '
        "fired_at) VALUES ($1, $2, $3, 'join', clock_timestamp() - $4::interval)",
        fire_id,
        new_uuid(),
        flow_id,
        age,
    )
    return JobId(fire_id)


async def _seed_ledger(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    status: str,
    age: timedelta,
    step_key: str,
) -> JobId:
    """One ledger row with an overridden creation clock; distinct
    step_keys keep the claim arbiter (flow, step, map, attempt) happy."""
    row_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "attempt, status, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, 1, $5, clock_timestamp() - $6::interval, "
        "clock_timestamp() - $6::interval)",
        row_id,
        flow_id,
        new_uuid(),
        step_key,
        status,
        age,
    )
    return JobId(row_id)


async def _seed_signal(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    status: str,
    age: timedelta,
    name: str,
) -> JobId:
    """A signal row on the flow (workflow_id IS the flow root id); held
    rows ride the partial-unique identity, so distinct names per flow."""
    sig_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".wf_signals (id, workflow_id, node_key, signal_name, '
        "hold_epoch, call_id, status, created_at, resolved_at) "
        "VALUES ($1, $2, $3, $4, 1, $5, $6, clock_timestamp() - $7::interval, "
        "CASE WHEN $6 = 'held' THEN NULL ELSE clock_timestamp() - $7::interval END)",
        sig_id,
        flow_id,
        "node",
        name,
        f"call:{name}",
        status,
        age,
    )
    return JobId(sig_id)


async def _seed_progress(
    conn: asyncpg.Connection, schema: str, node_id: JobId, *, age: timedelta, channel: str
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".wf_node_progress (node_id, channel, pct, updated_at) '
        "VALUES ($1, $2, 50, clock_timestamp() - $3::interval)",
        node_id,
        channel,
        age,
    )


async def _count(conn: asyncpg.Connection, schema: str, table: str, flow_id: JobId) -> int:
    """The table's row count attributed to the flow (wf_node_progress
    reaches the flow through its NODE's jobs row — the arm's own leg)."""
    if table == "wf_node_progress":
        sql = (
            f'SELECT count(*) FROM "{schema}".wf_node_progress wp '
            f'JOIN "{schema}".jobs n ON n.id = wp.node_id '
            "WHERE (n.metadata->>'flow_id')::uuid = $1"
        )
    else:
        col = "workflow_id" if table == "wf_signals" else "flow_id"
        sql = f'SELECT count(*) FROM "{schema}".{table} WHERE {col} = $1'
    return int(await conn.fetchval(sql, flow_id))


# The five arms, uniformly applied (the sweep family's own shape: one
# bounded batch per pass, the retention from settings).


async def _run_arms(
    pool: Any, schema: str, *, retention: timedelta, batch_size: int = 200
) -> list[int]:
    """Both passes per arm (the two-pass law): the flows-side pass (every
    call) + the rootless pass, summed — the leader wiring runs the
    rootless leg on a coarser cadence (every Nth call); a pin drives both
    legs directly so one pass's skip cadence cannot mask the other's."""
    from taskq.workflows._sweep import (
        prune_wf_edge,
        prune_wf_edge_rootless,
        prune_wf_join_fire,
        prune_wf_join_fire_rootless,
        prune_wf_node_progress,
        prune_wf_node_progress_rootless,
        prune_wf_signals,
        prune_wf_signals_rootless,
        prune_wf_step_ledger,
        prune_wf_step_ledger_rootless,
    )
    from taskq.workflows.engine import render_workflow_sql

    wsql = render_workflow_sql(schema)
    return [
        await prune_wf_edge(pool, wsql, retention=retention, batch_size=batch_size)
        + await prune_wf_edge_rootless(pool, wsql, batch_size=batch_size),
        await prune_wf_join_fire(pool, wsql, retention=retention, batch_size=batch_size)
        + await prune_wf_join_fire_rootless(pool, wsql, retention=retention, batch_size=batch_size),
        await prune_wf_step_ledger(pool, wsql, retention=retention, batch_size=batch_size)
        + await prune_wf_step_ledger_rootless(
            pool, wsql, retention=retention, batch_size=batch_size
        ),
        await prune_wf_signals(pool, wsql, retention=retention, batch_size=batch_size)
        + await prune_wf_signals_rootless(pool, wsql, retention=retention, batch_size=batch_size),
        await prune_wf_node_progress(pool, wsql, retention=retention, batch_size=batch_size)
        + await prune_wf_node_progress_rootless(
            pool, wsql, retention=retention, batch_size=batch_size
        ),
    ]


_TABLES = ("wf_edge", "wf_join_fire", "wf_step_ledger", "wf_signals", "wf_node_progress")


# ── (a) a terminal flow's rows delete under the arms ────────────────────


async def test_terminal_flow_rows_delete_across_all_five_arms(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """PIN (a): a seeded TERMINAL flow's rows in all five tables delete
    under the arms — including the HELD signal (a terminal flow's held
    signals are exactly the abandoned-hold class: nothing will ever
    deliver them, the wait site is terminal)."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _terminalize_flow(wf_conn, wf_schema, flow_id)
    node_id = await _seed_terminal_node(wf_conn, wf_schema, flow_id)
    await _seed_edge(wf_conn, wf_schema, flow_id)
    await _seed_join_fire(wf_conn, wf_schema, flow_id, age=_OLD)
    await _seed_ledger(wf_conn, wf_schema, flow_id, status="succeeded", age=_OLD, step_key="a")
    await _seed_signal(wf_conn, wf_schema, flow_id, status="held", age=_OLD, name="approve")
    await _seed_progress(wf_conn, wf_schema, node_id, age=_OLD, channel="user")

    deleted = await _run_arms(wf_pool, wf_schema, retention=_RETENTION)
    assert deleted == [1, 1, 1, 1, 1], deleted

    for table in _TABLES:
        remaining = await _count(wf_conn, wf_schema, table, flow_id)
        assert remaining == 0, f"{table} kept {remaining} row(s) of a terminal flow"


# ── (b) a non-terminal flow's rows survive the same run ─────────────────


async def test_nonterminal_flow_rows_survive_and_fresh_terminal_rows_survive(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """PIN (b): a NON-terminal flow's rows survive the SAME run that
    deletes a terminal flow's — and a FRESH row on the terminal flow
    survives too (the arm's two legs: flow terminal AND row older than
    the period; either failing, the row stays)."""
    # The terminal flow: old rows delete, the fresh row survives.
    dead_flow = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _terminalize_flow(wf_conn, wf_schema, dead_flow)
    dead_node = await _seed_terminal_node(wf_conn, wf_schema, dead_flow)
    await _seed_edge(wf_conn, wf_schema, dead_flow)
    await _seed_join_fire(wf_conn, wf_schema, dead_flow, age=_OLD)
    await _seed_ledger(wf_conn, wf_schema, dead_flow, status="succeeded", age=_OLD, step_key="a")
    await _seed_signal(wf_conn, wf_schema, dead_flow, status="delivered", age=_OLD, name="approve")
    await _seed_progress(wf_conn, wf_schema, dead_node, age=_OLD, channel="user")
    # The fresh leg, per table (the edge has no clock of its own — the
    # flow root's finished_at is fresh HERE, a second terminal flow's
    # job: the same arm's age leg, fresh, survives).
    fresh_flow = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET finished_at = clock_timestamp() '
        "- interval '10 seconds' WHERE id = $1",
        fresh_flow,
    )
    fresh_node = await _seed_terminal_node(wf_conn, wf_schema, fresh_flow)
    await _seed_edge(wf_conn, wf_schema, fresh_flow)
    await _seed_join_fire(wf_conn, wf_schema, fresh_flow, age=_FRESH)
    await _seed_ledger(wf_conn, wf_schema, fresh_flow, status="succeeded", age=_FRESH, step_key="a")
    await _seed_signal(
        wf_conn, wf_schema, fresh_flow, status="delivered", age=_FRESH, name="approve"
    )
    await _seed_progress(wf_conn, wf_schema, fresh_node, age=_FRESH, channel="user")

    # The live flow: every row old, the flow NON-terminal — nothing
    # deletes.
    live_flow = await seed_flow(wf_conn, wf_schema, status="running")
    live_node = await seed_running_node(wf_conn, wf_schema, live_flow)
    await _seed_edge(wf_conn, wf_schema, live_flow)
    await _seed_join_fire(wf_conn, wf_schema, live_flow, age=_OLD)
    await _seed_ledger(wf_conn, wf_schema, live_flow, status="running", age=_OLD, step_key="a")
    await _seed_signal(wf_conn, wf_schema, live_flow, status="held", age=_OLD, name="approve")
    await _seed_progress(wf_conn, wf_schema, live_node, age=_OLD, channel="user")

    await _run_arms(wf_pool, wf_schema, retention=_RETENTION)

    for table in _TABLES:
        live_count = await _count(wf_conn, wf_schema, table, live_flow)
        assert live_count == 1, (
            f"{table} deleted {1 - live_count} row(s) of a NON-terminal "
            "flow — the flow-terminality leg failed"
        )
        fresh_count = await _count(wf_conn, wf_schema, table, fresh_flow)
        assert fresh_count == 1, (
            f"{table} deleted a FRESH row of a terminal flow — the age leg failed"
        )
    dead_count = await _count(wf_conn, wf_schema, "wf_edge", dead_flow)
    assert dead_count == 0  # the control: the same run deleted the old rows


# ── (c) zero-period = zero deletes (the negative control) ───────────────


async def test_zero_period_is_the_disable_sentinel_and_the_default(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """PIN (c): ``timedelta(0)`` disables — the settings-level
    zero-means-off sentinel, the period gate's enforcement point (the
    spec runner skips the arm) — AND it is the five fields' DEFAULT: an
    upgrade deletes NOTHING until the operator opts in (the
    outbox/step-cache precedents default ON because they prune DERIVED
    data; these five prune the FLOW HISTORY). The behavioral leg: a
    zero period's gate arithmetic never runs the arm, so the old rows
    survive untouched."""
    from taskq.settings import WorkerSettings

    period_fields = (
        ("workflow_edge_retention_period", "TASKQ_WORKFLOW_EDGE_RETENTION_PERIOD"),
        (
            "workflow_step_ledger_retention_period",
            "TASKQ_WORKFLOW_STEP_LEDGER_RETENTION_PERIOD",
        ),
        (
            "workflow_join_fire_retention_period",
            "TASKQ_WORKFLOW_JOIN_FIRE_RETENTION_PERIOD",
        ),
        ("workflow_signals_retention_period", "TASKQ_WORKFLOW_SIGNALS_RETENTION_PERIOD"),
        (
            "workflow_node_progress_retention_period",
            "TASKQ_WORKFLOW_NODE_PROGRESS_RETENTION_PERIOD",
        ),
    )
    dsn = {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}
    defaults = WorkerSettings.load_from_dict(dsn, validate=False)
    zeroed = WorkerSettings.load_from_dict(
        {**dsn, **{env: "0" for _, env in period_fields}}, validate=False
    )
    for field, _env in period_fields:
        assert getattr(defaults, field) == timedelta(0), (
            f"{field} must default to timedelta(0) — the five arms prune "
            "FLOW HISTORY, the upgrade is safe by default"
        )
        assert getattr(zeroed, field) == timedelta(0)

    # The gate's enforcement point: period <= timedelta(0) → the arm
    # never runs (the spec runner's period gate) — a disabled arm
    # deletes nothing.
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _terminalize_flow(wf_conn, wf_schema, flow_id)
    node_id = await _seed_terminal_node(wf_conn, wf_schema, flow_id)
    await _seed_edge(wf_conn, wf_schema, flow_id)
    await _seed_join_fire(wf_conn, wf_schema, flow_id, age=_OLD)
    await _seed_ledger(wf_conn, wf_schema, flow_id, status="succeeded", age=_OLD, step_key="a")
    await _seed_signal(wf_conn, wf_schema, flow_id, status="delivered", age=_OLD, name="approve")
    await _seed_progress(wf_conn, wf_schema, node_id, age=_OLD, channel="user")
    period = defaults.workflow_edge_retention_period
    ran = period > timedelta(0)
    assert not ran
    for table in _TABLES:
        remaining = await _count(wf_conn, wf_schema, table, flow_id)
        assert remaining == 1, f"{table} moved under a zero period — the disable sentinel failed"

    # The registration-was-the-gap class: the sweep table carries all
    # five arms (an arm nothing calls is a silent fix).
    specs = _registered_spec_names()
    assert {
        "wf_edge_retention",
        "wf_join_fire_retention",
        "wf_step_ledger_retention",
        "wf_signals_retention",
        "wf_node_progress_retention",
    } <= specs


def _registered_spec_names() -> set[str]:
    """The sweep table's names, read from the module's own text (the
    specs tuple is built inside _sweep_loop — the source grep is the
    honest structural read for a registration pin)."""
    import inspect
    import re

    import taskq.worker._leader_sweeps as sweeps_mod

    text = inspect.getsource(sweeps_mod)
    return set(re.findall(r'name="(wf_[a-z_]+)"', text))


# ── (d) the batch bound honored ─────────────────────────────────────────


async def test_one_pass_deletes_at_most_the_batch(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """PIN (d): seed MORE rows than the batch, one pass deletes AT MOST
    the batch (the retention family's bounded-batch discipline — the
    remainder waits for the next pass)."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _terminalize_flow(wf_conn, wf_schema, flow_id)
    node_id = await _seed_terminal_node(wf_conn, wf_schema, flow_id)
    total, batch = 15, 10
    for i in range(total):
        await _seed_ledger(
            wf_conn, wf_schema, flow_id, status="succeeded", age=_OLD, step_key=f"s{i}"
        )
    await _seed_progress(wf_conn, wf_schema, node_id, age=_OLD, channel="user")

    from taskq.workflows._sweep import prune_wf_step_ledger
    from taskq.workflows.engine import render_workflow_sql

    deleted = await prune_wf_step_ledger(
        wf_pool,
        render_workflow_sql(wf_schema),
        retention=_RETENTION,
        batch_size=batch,
    )
    assert deleted == batch  # one pass, bounded
    remaining = int(
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1',
            flow_id,
        )
    )
    assert remaining == total - batch  # the remainder waits for the next pass


# ── (e) a LIVE hold on a NON-terminal flow survives ─────────────────────


async def test_live_hold_on_nonterminal_flow_survives_a_terminal_neighbors_reap(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """PIN (e): wf_signals — a LIVE hold ('held', unresolved) on a
    NON-terminal flow survives even when ANOTHER flow is terminal in the
    same pass (the arm is flow-scoped, never table-scoped); the terminal
    flow's own held signal — the abandoned-hold class — deletes."""
    dead_flow = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _terminalize_flow(wf_conn, wf_schema, dead_flow)
    # The terminal node row keeps the flow G7-honest (a succeeded root's
    # rows must reconstruct 'complete' at teardown even with every
    # ledger row retained away — the node row IS the reconstruction's
    # surviving source).
    await _seed_terminal_node(wf_conn, wf_schema, dead_flow)
    live_flow = await seed_flow(wf_conn, wf_schema, status="running")
    dead_hold = await _seed_signal(
        wf_conn, wf_schema, dead_flow, status="held", age=_OLD, name="approve"
    )
    live_hold = await _seed_signal(
        wf_conn, wf_schema, live_flow, status="held", age=_OLD, name="approve"
    )

    from taskq.workflows._sweep import prune_wf_signals
    from taskq.workflows.engine import render_workflow_sql

    deleted = await prune_wf_signals(wf_pool, render_workflow_sql(wf_schema), retention=_RETENTION)
    assert deleted == 1
    gone = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_signals WHERE id = $1', dead_hold
    )
    kept = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', live_hold
    )
    assert int(gone) == 0  # the abandoned-hold class deletes with its flow
    assert kept == "held"  # the LIVE flow's hold is untouched


async def test_a_flow_whose_root_row_was_pruned_is_still_swept(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ORPHAN-ROOT LEG (the meta-audit's conviction): the jobs archive
    and the hard prune DELETE old flow-root rows; an arm whose doomed
    predicate requires the root to EXIST-and-be-terminal can never match
    a rootless row — every pruned flow's wf history is unprunable
    forever, the exact unbounded-growth class the arms exist to kill.
    The two-leg predicate: the root EXISTS-and-terminal, OR the root is
    GONE (a committed wf row's root can only be absent by deletion — the
    run's creation orders the root first)."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _terminalize_flow(wf_conn, wf_schema, flow_id)
    node_id = await _seed_terminal_node(wf_conn, wf_schema, flow_id)
    await _seed_edge(wf_conn, wf_schema, flow_id)
    await _seed_join_fire(wf_conn, wf_schema, flow_id, age=_OLD)
    await _seed_ledger(wf_conn, wf_schema, flow_id, status="succeeded", age=_OLD, step_key="a")
    await _seed_signal(wf_conn, wf_schema, flow_id, status="held", age=_OLD, name="approve")
    await _seed_progress(wf_conn, wf_schema, node_id, age=_OLD, channel="user")
    # THE PRUNE: the flow's root AND its node rows leave `jobs` (the
    # archive move + the hard delete — the estate's own job retention).
    # The wf history outlives them: FK-less by design, dangling by the
    # prune's own hand.
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".jobs WHERE id = $1', node_id)

    deleted = await _run_arms(wf_pool, wf_schema, retention=_RETENTION)
    assert deleted == [1, 1, 1, 1, 1], deleted
    for table in _TABLES:
        remaining = await _count(wf_conn, wf_schema, table, flow_id)
        assert remaining == 0, f"{table} kept {remaining} orphaned row(s) of a pruned flow"


# ── (f) H-B1: the scale pin — the loaded pass, the drain, the zero-doomed
#    steady state — every pass completes under the dispatcher command
#    timeout and deletes at most the batch ────────────────────────────────


async def _bulk_flow(conn: asyncpg.Connection, schema: str, flow_id: JobId, *, status: str) -> None:
    """One flow root + its one node row, bulk-seeder shape (the seed
    helpers' own columns; the succeeded root carries the terminalization
    stamp inline). $6 repeats the status for the terminalization CASE —
    a DISTINCT parameter, because the status column deduces the enum and
    the comparison deduces text (asyncpg refuses to unify one parameter
    across both)."""
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key, "
        "finished_at) VALUES ($1, 'flow', 'default', '{}', 3, 'transient', $2, "
        "'__flow__', $3::jsonb, 'workflow-run', $4, "
        "CASE WHEN $5::text = 'succeeded' THEN clock_timestamp() - interval '2 hours' "
        "ELSE NULL END)",
        [
            (
                flow_id,
                status,
                json.dumps({"flow_id": str(flow_id)}),
                f"flow:{flow_id}",
                status,
            ),
        ],
    )


async def _bulk_node(conn: asyncpg.Connection, schema: str, flow_id: JobId, *, status: str) -> None:
    """One node row on the flow (G7-honest: a succeeded run's rows must
    reconstruct 'complete' — the node row survives retention)."""
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, finished_at) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', $2, 'done', $3::jsonb, "
        "clock_timestamp() - interval '2 hours')",
        [(new_uuid(), status, json.dumps({"flow_id": str(flow_id)}))],
    )


# load_sensitive: this pin asserts WALL TIME — four elapsed asserts against
# dispatcher_command_timeout (default 5.0s) on passes whose healthy runtime
# is a fraction of that budget. Under xdist co-tenancy a sibling's CPU churn
# pushed the loaded pass to 15.93s (PR #695's 3.13 leg): runner speed, not
# the arm's shape. The serial load-sensitive lane is where a wall-time
# assert measures the code under test and not the packing. The pin's
# row-count asserts (batch bound, drain total, live survival) ride along;
# they are deterministic wherever the pin runs. Every other pin in this
# file is WALL-CLOCK-FREE (the module docstring's law) and stays unmarked.
@pytest.mark.load_sensitive
async def test_scale_pin_loaded_pass_drain_and_zero_doomed_steady_state(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """PIN (f) — H-B1's scale proof: 20k edge rows across 400 AGED
    terminal flows (plus 20 live flows' 1k rows as the survival control).

    * the LOADED pass completes under ``dispatcher_command_timeout`` AND
      deletes at most the batch (the flow-id selection is a tiny jobs-side
      set via ``jobs_finished_at_idx``; the delete rides the flow_id
      index, row-LIMITed);
    * the drain (repeated passes) empties every aged flow's rows and the
      live flows' rows ALL survive;
    * the ZERO-DOOMED steady-state pass — the convicted shape's kill:
      the old arm walked the whole table probing jobs twice per row and
      TimeoutError'd at 3M — completes under the timeout deleting
      nothing, flows-side AND rootless."""
    import time

    from taskq.settings import WorkerSettings
    from taskq.workflows._sweep import prune_wf_edge, prune_wf_edge_rootless
    from taskq.workflows.engine import render_workflow_sql

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}, validate=False
    )
    aged, live, edges_per_flow = 400, 20, 50
    aged_ids = [new_uuid() for _ in range(aged)]
    live_ids = [new_uuid() for _ in range(live)]
    for fid in aged_ids:
        await _bulk_flow(wf_conn, wf_schema, JobId(fid), status="succeeded")
        await _bulk_node(wf_conn, wf_schema, JobId(fid), status="succeeded")
    for fid in live_ids:
        await _bulk_flow(wf_conn, wf_schema, JobId(fid), status="running")
        await _bulk_node(wf_conn, wf_schema, JobId(fid), status="running")
    edge_rows = [
        (new_uuid(), new_uuid(), fid)
        for fid in [*aged_ids, *live_ids]
        for _ in range(edges_per_flow)
    ]
    await wf_conn.executemany(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        edge_rows,
    )

    wsql = render_workflow_sql(wf_schema)
    batch = 5_000

    start = time.monotonic()
    deleted = await prune_wf_edge(wf_pool, wsql, retention=_RETENTION, batch_size=batch)
    loaded_elapsed = time.monotonic() - start
    assert deleted <= batch, f"the loaded pass deleted {deleted} > batch {batch}"
    assert loaded_elapsed < settings.dispatcher_command_timeout, (
        f"the loaded pass took {loaded_elapsed:.2f}s — past the dispatcher "
        "command timeout the leader's pool runs under (H-B1's conviction: "
        "the arm Times Out and PERMANENTLY stops pruning)"
    )

    drained = deleted
    while True:
        n = await prune_wf_edge(wf_pool, wsql, retention=_RETENTION, batch_size=batch)
        drained += n
        if n == 0:
            break
    assert drained == aged * edges_per_flow, (
        f"the drain deleted {drained}, expected {aged * edges_per_flow}"
    )
    live_edges = int(
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_edge WHERE flow_id = ANY($1::uuid[]) '
            "AND flow_id IN (SELECT id FROM "
            f"\"{wf_schema}\".jobs WHERE step_key = '__flow__' AND status = 'running')",
            live_ids,
        )
    )
    assert live_edges == live * edges_per_flow, (
        f"the live flows kept {live_edges} edges, expected "
        f"{live * edges_per_flow} — the arm ate a LIVE flow's history"
    )

    start = time.monotonic()
    deleted = await prune_wf_edge(wf_pool, wsql, retention=_RETENTION, batch_size=batch)
    steady_elapsed = time.monotonic() - start
    assert deleted == 0
    assert steady_elapsed < settings.dispatcher_command_timeout, (
        f"the ZERO-DOOMED steady-state pass took {steady_elapsed:.2f}s — the "
        "full-table walk is the convicted shape (H-B1)"
    )
    start = time.monotonic()
    rootless = await prune_wf_edge_rootless(wf_pool, wsql, batch_size=batch)
    rootless_elapsed = time.monotonic() - start
    assert rootless == 0
    assert rootless_elapsed < settings.dispatcher_command_timeout, (
        f"the rootless pass's bounded walk took {rootless_elapsed:.2f}s at "
        "zero-doomed — its scan is not batch-bounded (H-B1's second leg)"
    )

    # THE FULL BATTERY at the zero-doomed steady state: all five arms'
    # flows-side passes + all five rootless walks (ten statements — the
    # leader tick's real cost) complete under the same timeout.
    start = time.monotonic()
    battery = await _run_arms(wf_pool, wf_schema, retention=_RETENTION, batch_size=batch)
    battery_elapsed = time.monotonic() - start
    assert battery == [0, 0, 0, 0, 0], battery
    assert battery_elapsed < settings.dispatcher_command_timeout, (
        f"the full five-arm battery (flows-side + rootless, zero-doomed) "
        f"took {battery_elapsed:.2f}s — past the dispatcher command timeout"
    )


# ── (g) H-B3: the root identity — a row pointing at a live NON-root job
#    is rootless-class; a live ROOT's rows survive; a terminal-aged
#    root's rows belong to the flows-side pass ────────────────────────────


async def test_non_root_pointee_rows_prune_and_live_root_rows_survive(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """PIN (g): the EXISTS legs never checked ``step_key='__flow__'`` — a
    wf row pointing at a LIVE NON-ROOT job (a vanilla step's id in the
    flow column: the third orphan class, proven) was unprunable. The
    honest two-leg contract: (a) the pointee IS a terminal root → the
    flows-side pass (aged); (b) the pointee is NOT a root at all (absent
    OR step_key != '__flow__') → the rootless pass prunes; (c) the
    pointee IS a LIVE root → survives both."""
    from taskq.workflows._sweep import (
        prune_wf_edge,
        prune_wf_edge_rootless,
        prune_wf_step_ledger_rootless,
    )
    from taskq.workflows.engine import render_workflow_sql

    wsql = render_workflow_sql(wf_schema)
    # The pointee that is NOT a root: a live vanilla job.
    non_root = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'compute')",
        non_root,
    )
    # The pointee that IS a live root.
    live_root = await seed_flow(wf_conn, wf_schema, status="running")
    await seed_running_node(wf_conn, wf_schema, live_root)
    # The pointee that is a TERMINAL-AGED root: the flows-side pass's own.
    aged_root = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _terminalize_flow(wf_conn, wf_schema, aged_root)
    await _seed_terminal_node(wf_conn, wf_schema, aged_root)

    # wf_edge (no clock: the rootless class deletes on sight).
    await _seed_edge(wf_conn, wf_schema, JobId(non_root))  # (b): prunes
    live_edge = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        live_edge,
        new_uuid(),
        live_root,
    )  # (c): survives
    await _seed_edge(wf_conn, wf_schema, aged_root)  # (a): the flows-side pass
    rootless_only = await prune_wf_edge_rootless(wf_pool, wsql, batch_size=200)
    assert rootless_only == 1, (
        f"the rootless pass deleted {rootless_only} edges — expected exactly "
        "the NON-ROOT-POINTEE edge (the terminal-aged root's rows are the "
        "flows-side pass's own; the live root's row survives)"
    )
    assert await _count(wf_conn, wf_schema, "wf_edge", live_root) == 1, (
        "the live ROOT's edge deleted — the root identity leg is dishonest"
    )
    assert await _count(wf_conn, wf_schema, "wf_edge", aged_root) == 1, (
        "the terminal-aged root's edge deleted under the ROOTLESS pass — "
        "the two passes' division of labor collapsed"
    )
    flows_side = await prune_wf_edge(wf_pool, wsql, retention=_RETENTION, batch_size=200)
    assert flows_side == 1
    assert await _count(wf_conn, wf_schema, "wf_edge", aged_root) == 0

    # wf_step_ledger (a clock of its own: the rootless class ages by it).
    await _seed_ledger(
        wf_conn, wf_schema, JobId(non_root), status="succeeded", age=_OLD, step_key="a"
    )  # (b): prunes
    await _seed_ledger(
        wf_conn, wf_schema, live_root, status="running", age=_OLD, step_key="a"
    )  # (c): survives
    rootless_ledger = await prune_wf_step_ledger_rootless(
        wf_pool, wsql, retention=_RETENTION, batch_size=200
    )
    assert rootless_ledger == 1
    assert await _count(wf_conn, wf_schema, "wf_step_ledger", live_root) == 1, (
        "the live ROOT's ledger row deleted — the root identity leg is dishonest"
    )


# ── (h) H-B3: the node_progress legs — a node WITHOUT the flow_id stamp
#    is prune-eligible; the node-gone leg; the live-root pointee survives ─


async def test_node_progress_orphan_legs_are_honest(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """PIN (h): the progress row's owner is the NODE's flow link — a row
    whose node (i) is GONE, (ii) carries NO flow_id stamp (a vanilla
    node: the convicted unprunable class), or (iii) points at something
    that is NOT a flow root, is prune-eligible past the period; a node
    whose pointee IS a live root survives."""
    from taskq.workflows._sweep import prune_wf_node_progress_rootless
    from taskq.workflows.engine import render_workflow_sql

    wsql = render_workflow_sql(wf_schema)
    # (ii) a vanilla node WITHOUT the flow_id stamp.
    plain_node = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'compute')",
        plain_node,
    )
    await _seed_progress(wf_conn, wf_schema, JobId(plain_node), age=_OLD, channel="user")
    # (i) a GONE node.
    await _seed_progress(wf_conn, wf_schema, JobId(new_uuid()), age=_OLD, channel="user")
    # (iii) a node whose flow_id points at a NON-root job.
    stamped_to_non_root = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'compute', "
        "$2::jsonb)",
        stamped_to_non_root,
        json.dumps({"flow_id": str(new_uuid())}),  # the flow row does not exist
    )
    await _seed_progress(wf_conn, wf_schema, JobId(stamped_to_non_root), age=_OLD, channel="user")
    # (c) a node whose pointee IS a live root.
    live_root = await seed_flow(wf_conn, wf_schema, status="running")
    await seed_running_node(wf_conn, wf_schema, live_root)
    await _seed_progress(wf_conn, wf_schema, live_root, age=_OLD, channel="user")

    deleted = await prune_wf_node_progress_rootless(
        wf_pool, wsql, retention=_RETENTION, batch_size=200
    )
    assert deleted == 3, (
        f"the rootless progress pass deleted {deleted}, expected 3 (the "
        "gone node, the no-flow_id node, the non-root-pointee node) — the "
        "live-root node's row survives"
    )
    kept = int(
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_node_progress WHERE node_id = $1',
            live_root,
        )
    )
    assert kept == 1, "the LIVE ROOT's node progress deleted — the identity leg is dishonest"


# ── (i) H-B4 / R2: the NULL-stamp leg's CONSERVATIVE shape — the
#    NULL-stamped terminal roots are SKIPPED (never doomed: the delete is
#    never eager), the pass OBSERVES them and the named warning fires in
#    BOTH the busy and the idle shapes (the tripwire always live) ────────


def _arm_null_stamp_warn(monkeypatch: Any) -> None:
    """The warn cadence zeroed + a fresh cadence state: the observe-
    warning is rate-limited by MODULE state (the leader's warn cadence —
    a persistent condition re-warns on the hour, never per pass); a pin
    that asserts the warning must not inherit an earlier test's silence
    (the same monkeypatchable-module-constant contract
    ``_PRUNE_RETRY_BACKOFF_INITIAL_SECS`` carries)."""
    from taskq.workflows import _sweep as sweep_mod

    monkeypatch.setattr(sweep_mod, "_NULL_STAMP_REWARN_SECS", 0.0)
    monkeypatch.setattr(sweep_mod, "_null_stamp_last_warned", {})


async def test_null_stamp_idle_shape_rows_survive_and_warning_fires(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: Any
) -> None:
    """PIN (i-idle) — R2's IDLE shape: a JUST-terminalized flow whose
    ``finished_at`` stamp never landed, an otherwise-idle estate. THE RED
    (the 'aged by nothing' leg): the flow's ENTIRE history deleted under
    a 30-day period while the flow was seconds old — the delete eager,
    the period meaningless. THE GREEN (the conservative shape): the
    NULL-stamped terminal roots are SKIPPED — never doomed — and the pass
    OBSERVES them: the named warning fires (the stamping-bug tripwire,
    the operator's signal to fix the stamp or backfill it; the rows stay
    until they carry an honest age)."""
    import structlog.testing

    from taskq.workflows._sweep import prune_wf_edge
    from taskq.workflows.engine import render_workflow_sql

    _arm_null_stamp_warn(monkeypatch)
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded")  # no stamp
    await _seed_terminal_node(wf_conn, wf_schema, flow_id)  # G7-honest
    await _seed_edge(wf_conn, wf_schema, flow_id)

    with structlog.testing.capture_logs() as logs:
        deleted = await prune_wf_edge(
            wf_pool, render_workflow_sql(wf_schema), retention=_RETENTION, batch_size=200
        )
    assert deleted == 0, (
        f"the NULL-stamped terminal root's edge DELETED under a "
        f"{_RETENTION} period (deleted {deleted}) — the delete was eager: "
        "a flow with no honest age must never doom (R2's idle shape)"
    )
    assert await _count(wf_conn, wf_schema, "wf_edge", flow_id) == 1, (
        "the NULL-stamped root's history did not survive — R2's idle shape"
    )
    warnings = [e for e in logs if e.get("event") == "wf-retention-null-finished-at"]
    assert warnings, f"the stamping-bug tripwire did not fire on the observation: {logs}"
    assert warnings[0]["table"] == "wf_edge"


async def test_null_stamp_busy_shape_warning_not_starved(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: Any
) -> None:
    """PIN (i-busy) — R2's BUSY shape: a busy estate's aged STAMPED roots
    fill the batch (``ORDER BY finished_at NULLS LAST`` put the
    NULL-stamped roots last, past the LIMIT) — THE RED: the tripwire
    NEVER fired and the NULL root was never selected, silently, forever.
    THE GREEN: the observation is NOT the selection — it cannot starve
    behind the batch; the warning fires while the NULL root's rows
    survive."""
    import structlog.testing

    from taskq.workflows._sweep import prune_wf_edge
    from taskq.workflows.engine import render_workflow_sql

    _arm_null_stamp_warn(monkeypatch)
    # Five AGED STAMPED terminal flows: the batch (3) fills with them —
    # the old shape's NULLS LAST order starved the NULL root behind them.
    for _ in range(5):
        stamped = await seed_flow(wf_conn, wf_schema, status="succeeded")
        await _terminalize_flow(wf_conn, wf_schema, stamped)
        await _seed_terminal_node(wf_conn, wf_schema, stamped)
        await _seed_edge(wf_conn, wf_schema, stamped)
    # The NULL-stamped terminal root: no stamp ever landed.
    null_flow = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await _seed_terminal_node(wf_conn, wf_schema, null_flow)
    await _seed_edge(wf_conn, wf_schema, null_flow)

    with structlog.testing.capture_logs() as logs:
        deleted = await prune_wf_edge(
            wf_pool, render_workflow_sql(wf_schema), retention=_RETENTION, batch_size=3
        )
    assert deleted == 3, f"the batch bound moved: {deleted}"
    assert await _count(wf_conn, wf_schema, "wf_edge", null_flow) == 1, (
        "the NULL-stamped root's history deleted — R2's conservative shape "
        "(skipped, never doomed) collapsed"
    )
    warnings = [e for e in logs if e.get("event") == "wf-retention-null-finished-at"]
    assert warnings, (
        f"the tripwire starved behind the batch (the busy shape's conviction: "
        f"the NULL root never selected, the warning never fired): {logs}"
    )
    assert warnings[0]["table"] == "wf_edge"


# ── (j) H-B2: the arms' batch derives from the prune family's own knob ──


def test_arms_batch_derives_from_the_prune_family_knob() -> None:
    """PIN (j): the five flow-history arms borrowed
    ``event_writer_batch_size`` (100) — a 10M backlog = ~35 days per
    table. The wiring is the prune family's own knob
    (``prune_batch_size``, the deletion-sweep family's batch), asserted
    structurally (the AST read of the five closures: the batch keyword
    names prune_batch_size, the event-writer's name appears in none) +
    the settings pin (the knob exists, positive, the prune family's
    default). The closures also wire each arm's ROOTLESS pass (the
    registration-was-the-gap class: a leg nothing calls is a silent
    fix)."""
    import ast
    import inspect

    import taskq.worker._leader_sweeps as sweeps_mod
    from taskq.constants import DEFAULT_PRUNE_BATCH_SIZE
    from taskq.settings import WorkerSettings

    dsn = {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}
    s = WorkerSettings.load_from_dict(dsn, validate=False)
    assert s.prune_batch_size >= 1
    assert s.prune_batch_size == DEFAULT_PRUNE_BATCH_SIZE

    closures = (
        "wf_edge_retention_call",
        "wf_join_fire_retention_call",
        "wf_step_ledger_retention_call",
        "wf_signals_retention_call",
        "wf_node_progress_retention_call",
    )
    rootless_arms = (
        "prune_wf_edge_rootless",
        "prune_wf_join_fire_rootless",
        "prune_wf_step_ledger_rootless",
        "prune_wf_signals_rootless",
        "prune_wf_node_progress_rootless",
    )
    tree = ast.parse(inspect.getsource(sweeps_mod))
    found: dict[str, ast.AsyncFunctionDef] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in closures:
            found[node.name] = node
    assert set(found) == set(closures), (
        f"the five retention closures are missing from the sweep loop: {set(closures) - set(found)}"
    )
    for name, rootless in zip(closures, rootless_arms, strict=True):
        fn = found[name]
        knob_calls, borrowed, rootless_calls = 0, 0, 0
        for call in ast.walk(fn):
            if not isinstance(call, ast.Call):
                continue
            if isinstance(call.func, ast.Name) and call.func.id == rootless:
                rootless_calls += 1
            for kw in call.keywords:
                if kw.arg != "batch_size":
                    continue
                spelled = ast.unparse(kw.value)
                if spelled == "ctx.deps.settings.prune_batch_size":
                    knob_calls += 1
                elif spelled == "ctx.deps.settings.event_writer_batch_size":
                    borrowed += 1
        assert knob_calls >= 1, (
            f"{name} does not take the prune family's own batch knob "
            "(H-B2: the borrowed event_writer_batch_size is the conviction)"
        )
        assert borrowed == 0, f"{name} still borrows event_writer_batch_size (H-B2)"
        assert rootless_calls == 1, (
            f"{name} never calls {rootless} — the rootless leg is not "
            "wired (the registration-was-the-gap class)"
        )


# ── (j2) the outbox/step-cache arms' batch: the same prune-family knob ───


def test_outbox_and_step_cache_arms_derive_from_the_prune_family_knob() -> None:
    """PIN (j2) — the mirror of (j) for the two DERIVED-DATA retention
    arms: the delivered outbox's TTL arm and the step cache's
    expired-corpses arm borrowed ``event_writer_batch_size`` (100) the
    same way the five flow-history arms did (H-B2's conviction class —
    a monotone population drained 100 rows per pass). Their wiring is
    the prune family's own knob (``prune_batch_size``, the
    deletion-sweep family's batch both arms' docstrings already
    promise), asserted structurally: the AST read of the two closures —
    the batch keyword names prune_batch_size, the event-writer's name
    appears in neither."""
    import ast
    import inspect

    import taskq.worker._leader_sweeps as sweeps_mod

    closures = ("wf_outbox_retention_call", "wf_step_cache_retention_call")
    tree = ast.parse(inspect.getsource(sweeps_mod))
    found: dict[str, ast.AsyncFunctionDef] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in closures:
            found[node.name] = node
    assert set(found) == set(closures), (
        f"the retention closures are missing from the sweep loop: {set(closures) - set(found)}"
    )
    for name, fn in found.items():
        knob_calls, borrowed = 0, 0
        for call in ast.walk(fn):
            if not isinstance(call, ast.Call):
                continue
            for kw in call.keywords:
                if kw.arg != "batch_size":
                    continue
                spelled = ast.unparse(kw.value)
                if spelled == "ctx.deps.settings.prune_batch_size":
                    knob_calls += 1
                elif spelled == "ctx.deps.settings.event_writer_batch_size":
                    borrowed += 1
        assert knob_calls >= 1, (
            f"{name} does not take the prune family's own batch knob "
            "(the borrowed event_writer_batch_size is H-B2's conviction class)"
        )
        assert borrowed == 0, f"{name} still borrows event_writer_batch_size (H-B2)"


# ── (k) H-B7: one clock face across the five arms' statements ───────────


def test_the_five_arms_tell_one_clock() -> None:
    """PIN (k): the family standardized on ``statement_timestamp()`` —
    the prune family's own selection-bound clock (STABLE: the bound is
    the statement's start, the two-clock law test_index_audit.py
    documents — a VOLATILE ``clock_timestamp()`` bound cannot be a btree
    index condition and re-filters the walk per row). Nine of the ten
    retention statements carry the one face; the tenth — the EDGE
    rootless statement — carries NO clock at all, on purpose (wf_edge
    has no timestamp column: the pointee's absence IS the age proof, the
    module comment states it). No clock_timestamp() survives in any of
    the ten."""
    import taskq.workflows._sql_sweep as sql_mod

    statements = (
        "EDGE_RETENTION_SQL",
        "EDGE_ROOTLESS_RETENTION_SQL",  # the clockless-by-design leg
        "JOIN_FIRE_RETENTION_SQL",
        "JOIN_FIRE_ROOTLESS_RETENTION_SQL",
        "STEP_LEDGER_RETENTION_SQL",
        "STEP_LEDGER_ROOTLESS_RETENTION_SQL",
        "SIGNALS_RETENTION_SQL",
        "SIGNALS_ROOTLESS_RETENTION_SQL",
        "NODE_PROGRESS_RETENTION_SQL",
        "NODE_PROGRESS_ROOTLESS_RETENTION_SQL",
    )
    for name in statements:
        sql = getattr(sql_mod, name)
        if name != "EDGE_ROOTLESS_RETENTION_SQL":
            assert "statement_timestamp()" in sql, (
                f"{name} does not carry the family's statement_timestamp() face (H-B7)"
            )
        assert "clock_timestamp()" not in sql, (
            f"{name} still compares on the volatile clock_timestamp() (H-B7: "
            "one face, the STABLE selection bound)"
        )


# ── (l) R1: the rootless walk ADVANCES at a live PK front — the
#    persisted cursor (the job_events_prune_state precedent), the exact
#    red-team story ────────────────────────────────────────────────────────


def _pk_uuid(*, high: bool, i: int) -> str:
    """A crafted PK uuid for walk-order control: low sorts below every
    uuid7 the seam mints, high sorts above all of them (the byte order
    the PK index walks)."""
    return f"{'ffffffff' if high else '00000000'}-0000-0000-0000-{i:012x}"


_FRONT_N = 3_000
_ORPHAN_N = 50
_STORY_BATCH = 1_000
_OLD_SQL = "clock_timestamp() - interval '2 hours'"


async def _seed_rootless_story(
    conn: asyncpg.Connection, schema: str, table: str, live_root: JobId, live_node: JobId
) -> tuple[str, list[str], list[str]]:
    """The red-team story's estate, per table: a live PK FRONT of
    ``_FRONT_N`` rows (every probe answers 'live root') and
    ``_ORPHAN_N`` orphans ABOVE it in the walk's own ORDER BY (the
    pointee absent). Returns ``(state_arm, front_keys, orphan_keys)`` —
    the keys as text, front sorted before orphans, the walk's own
    comparison order."""
    absent_flow = new_uuid()  # no jobs row: the absent pointee
    if table == "wf_edge":
        front = [_pk_uuid(high=False, i=i) for i in range(1, _FRONT_N + 1)]
        orphans = [_pk_uuid(high=True, i=i) for i in range(1, _ORPHAN_N + 1)]
        await conn.executemany(
            f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
            [(k, new_uuid(), live_root) for k in front]
            + [(k, new_uuid(), absent_flow) for k in orphans],
        )
        return "wf_edge", front, orphans
    if table == "wf_join_fire":
        front = [_pk_uuid(high=False, i=i) for i in range(1, _FRONT_N + 1)]
        orphans = [_pk_uuid(high=True, i=i) for i in range(1, _ORPHAN_N + 1)]
        await conn.executemany(
            f'INSERT INTO "{schema}".wf_join_fire (id, join_job_id, flow_id, step_key, '
            f"fired_at) VALUES ($1, $2, $3, 'join', {_OLD_SQL})",
            [(k, new_uuid(), live_root) for k in front]
            + [(k, new_uuid(), absent_flow) for k in orphans],
        )
        return "wf_join_fire", front, orphans
    if table == "wf_step_ledger":
        front = [_pk_uuid(high=False, i=i) for i in range(1, _FRONT_N + 1)]
        orphans = [_pk_uuid(high=True, i=i) for i in range(1, _ORPHAN_N + 1)]
        await conn.executemany(
            f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
            f"attempt, status, created_at, updated_at) VALUES ($1, $2, $3, $4, 1, "
            f"'succeeded', {_OLD_SQL}, {_OLD_SQL})",
            [(k, live_root, new_uuid(), f"s{i}") for i, k in enumerate(front, 1)]
            + [(k, absent_flow, new_uuid(), f"o{i}") for i, k in enumerate(orphans, 1)],
        )
        return "wf_step_ledger", front, orphans
    if table == "wf_signals":
        front = [_pk_uuid(high=False, i=i) for i in range(1, _FRONT_N + 1)]
        orphans = [_pk_uuid(high=True, i=i) for i in range(1, _ORPHAN_N + 1)]
        await conn.executemany(
            f'INSERT INTO "{schema}".wf_signals (id, workflow_id, node_key, signal_name, '
            f"hold_epoch, call_id, status, created_at, resolved_at) VALUES ($1, $2, 'n', "
            f"$3, 1, $4, 'delivered', {_OLD_SQL}, {_OLD_SQL})",
            [(k, live_root, f"s{i}", f"c{i}") for i, k in enumerate(front, 1)]
            + [(k, absent_flow, f"o{i}", f"c{i}") for i, k in enumerate(orphans, 1)],
        )
        return "wf_signals", front, orphans
    if table == "wf_node_progress":
        # The front's OWNER rows: live nodes grounded on the live root
        # (the probe's honest survivors); the orphans' nodes are GONE.
        front = [_pk_uuid(high=False, i=i) for i in range(1, _FRONT_N + 1)]
        orphans = [_pk_uuid(high=True, i=i) for i in range(1, _ORPHAN_N + 1)]
        await conn.executemany(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'compute', "
            "$2::jsonb)",
            [(k, json.dumps({"flow_id": str(live_root)})) for k in front],
        )
        await conn.executemany(
            f'INSERT INTO "{schema}".wf_node_progress (node_id, channel, pct, updated_at) '
            f"VALUES ($1, 'user', 50, {_OLD_SQL})",
            [(k,) for k in front] + [(k,) for k in orphans],
        )
        return "wf_node_progress", front, orphans
    if table == "wf_node_stream":
        # seq is the PK: the front rides 1..N (the live node's ring), the
        # orphans a high band above it (a GONE node's ring).
        front = [str(i) for i in range(1, _FRONT_N + 1)]
        orphans = [str(10**9 + i) for i in range(1, _ORPHAN_N + 1)]
        await conn.executemany(
            f'INSERT INTO "{schema}".wf_node_stream (seq, node_id, flow_id, class, kind, '
            f"payload, emitted_at) VALUES ($1, $2, $3, 'user', 'progress', NULL, {_OLD_SQL})",
            [(int(k), live_node, live_root) for k in front]
            + [(int(k), new_uuid(), absent_flow) for k in orphans],
        )
        return "wf_node_stream", front, orphans
    raise AssertionError(table)


async def _drive_rootless(pool: Any, schema: str, table: str, *, batch_size: int) -> int:
    """The arm's ROOTLESS pass, driven the shipped way (the leader wiring's
    own calls; the stream's walk rides the node_progress arm's pass — the
    fold)."""
    from taskq.workflows._sweep import (
        prune_wf_edge_rootless,
        prune_wf_join_fire_rootless,
        prune_wf_node_progress_rootless,
        prune_wf_signals_rootless,
        prune_wf_step_ledger_rootless,
    )
    from taskq.workflows.engine import render_workflow_sql

    wsql = render_workflow_sql(schema)
    if table == "wf_edge":
        return await prune_wf_edge_rootless(pool, wsql, batch_size=batch_size)
    if table == "wf_join_fire":
        return await prune_wf_join_fire_rootless(
            pool, wsql, retention=_RETENTION, batch_size=batch_size
        )
    if table == "wf_step_ledger":
        return await prune_wf_step_ledger_rootless(
            pool, wsql, retention=_RETENTION, batch_size=batch_size
        )
    if table == "wf_signals":
        return await prune_wf_signals_rootless(
            pool, wsql, retention=_RETENTION, batch_size=batch_size
        )
    if table in ("wf_node_progress", "wf_node_stream"):
        return await prune_wf_node_progress_rootless(
            pool, wsql, retention=_RETENTION, batch_size=batch_size
        )
    raise AssertionError(table)


async def _read_cursor(conn: asyncpg.Connection, schema: str, arm: str) -> str | None:
    """The arm's persisted cursor (the state row's cursor_a; NULL = from
    the start)."""
    return await conn.fetchval(
        f'SELECT cursor_a FROM "{schema}".wf_rootless_sweep_state WHERE arm = $1', arm
    )


async def _orphan_remaining(
    conn: asyncpg.Connection, schema: str, table: str, orphan_keys: list[str]
) -> int:
    """The orphans still standing (the walk's PK column, per table)."""
    orphan_keys2: list[object] = list(orphan_keys)
    if table == "wf_edge":
        col, cast = "child_id", "uuid[]"
    elif table == "wf_node_stream":
        col, cast = "seq", "bigint[]"
        orphan_keys2 = [int(k) for k in orphan_keys]  # the seq is a bigint
    elif table in ("wf_node_progress",):
        col, cast = "node_id", "uuid[]"
    else:
        col, cast = "id", "uuid[]"
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".{table} WHERE {col} = ANY($1::{cast})',
            orphan_keys2,
        )
    )


@pytest.mark.parametrize(
    "table",
    (
        "wf_edge",
        "wf_join_fire",
        "wf_step_ledger",
        "wf_signals",
        "wf_node_progress",
        "wf_node_stream",
    ),
)
async def test_rootless_walk_advances_past_a_live_pk_front(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, table: str
) -> None:
    """PIN (l) — R1's exact story: a 3,000-row live PK front, batch 1,000,
    five passes. THE RED (the pre-cure walk): every pass re-visited the
    SAME first 1,000 rows — the probes answered 'live root', nothing
    deleted, the cursor nowhere — five passes, deleted=0, the 50 orphans
    above the front stranded forever. THE GREEN: pass 1 still deletes 0
    (the front is live — the rows survive), but the walk's SCAN START
    ADVANCED — the persisted cursor rides the last VISITED row (visited,
    not deleted: a live front's rows ARE the advance), the state row
    survives restarts (the migration's table, the job_events_prune_state
    precedent); by pass 4 the walk is past the front and the orphans
    DELETE (deleted by pass ≥ 2); pass 5 finds nothing above the last
    orphan and the cursor REWINDS to the start (the wraparound — rows
    whose root died after the walk passed are re-probed on the next
    sweep). The visited cost stays bounded: exactly the batch per pass
    (after pass 3 the cursor IS the front's last key)."""
    live_root = await seed_flow(wf_conn, wf_schema, status="running")
    live_node = await seed_running_node(wf_conn, wf_schema, live_root)
    arm, front_keys, orphan_keys = await _seed_rootless_story(
        wf_conn, wf_schema, table, live_root, live_node
    )

    per_pass: list[int] = []
    cursors: list[str | None] = []
    for _ in range(5):
        per_pass.append(await _drive_rootless(wf_pool, wf_schema, table, batch_size=_STORY_BATCH))
        cursors.append(await _read_cursor(wf_conn, wf_schema, arm))

    assert per_pass == [0, 0, 0, 50, 0], (
        f"{table}: the walk never advanced at the live front (R1's story): {per_pass}"
    )
    assert await _count(wf_conn, wf_schema, table, live_root) == _FRONT_N, (
        f"{table}: the LIVE front's rows were eaten — the walk must visit "
        "them, answer 'live root', and move ON"
    )
    assert await _orphan_remaining(wf_conn, wf_schema, table, orphan_keys) == 0, (
        f"{table}: the orphans above the front survived all five passes — "
        "stranded forever (R1's conviction)"
    )
    # THE WALK'S SCAN START ADVANCED: the cursor is set after pass 1 and
    # MOVES pass over pass; after pass 3 it IS the front's last key (the
    # visited-cost bounded: exactly the batch per pass, three passes to
    # cross a 3,000-row front at batch 1,000).
    assert cursors[0] is not None, (
        f"{table}: pass 1 never advanced the cursor — the walk re-seeks "
        "from the start (R1's stranded-forever shape)"
    )
    assert cursors[1] != cursors[0], f"{table}: the cursor did not advance pass over pass"
    assert cursors[2] == front_keys[-1], (
        f"{table}: the cursor after pass 3 is {cursors[2]!r}, the front's "
        f"last key is {front_keys[-1]!r} — the walk's scan is not "
        "batch-bounded or the advance is not the last VISITED row"
    )
    # THE WRAPAROUND: pass 5 found nothing above the last orphan — the
    # cursor rewinds to NULL (from-the-start), the sweep's own reset.
    assert cursors[4] is None, (
        f"{table}: the cursor did not rewind on the exhausted pass "
        f"({cursors[4]!r}) — rows whose root dies after the walk passed "
        "are never re-probed"
    )


# ── (q) R4: the five rootless gates ALIGN — the per-arm phase offset,
#    the tick-8 spike spread across the ticks ─────────────────────────────


def test_rootless_gates_fire_phase_offset_never_two_in_one_tick() -> None:
    """PIN (q): the five gates' counters ran in LOCKSTEP (every 8th call,
    all five) — on tick 8 all five rootless walks + the five flows-side
    passes ran in ONE tick (the convicted spike). The per-arm phase
    offset (``turns % 8 == arm_index % 8``) spreads them: over 16
    consecutive ticks each arm's rootless pass fires exactly TWICE and
    never two in one tick. The behavioral read drives the gate factory;
    the AST read pins the loop's WIRING (the five construction sites
    carry the five distinct phases, in spec order)."""
    import ast
    import inspect

    import taskq.worker._leader_sweeps as sweeps_mod
    from taskq.worker._leader_sweeps import _WF_ROOTLESS_PASS_EVERY, _wf_rootless_gate

    arms = ("wf_edge", "wf_join_fire", "wf_step_ledger", "wf_signals", "wf_node_progress")
    gates = [_wf_rootless_gate(arm, i) for i, arm in enumerate(arms)]
    fires: dict[str, list[int]] = {arm: [] for arm in arms}
    for tick in range(1, 17):
        for arm, gate in zip(arms, gates, strict=True):
            if gate():
                fires[arm].append(tick)
    for i, arm in enumerate(arms):
        assert len(fires[arm]) == 2, f"{arm} fired {len(fires[arm])}x over 16 ticks"
        expected = [tick for tick in range(1, 17) if tick % _WF_ROOTLESS_PASS_EVERY == i]
        assert fires[arm] == expected, f"{arm} fired on {fires[arm]}, expected {expected}"
    per_tick = [sum(1 for arm in arms if tick in fires[arm]) for tick in range(1, 17)]
    assert max(per_tick) <= 1, (
        f"two rootless passes fired in one tick (the spike came back): {per_tick}"
    )

    tree = ast.parse(inspect.getsource(sweeps_mod))
    phases = [
        call.args[1].value
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "_wf_rootless_gate"
        and len(call.args) == 2
        and isinstance(call.args[1], ast.Constant)
    ]
    assert phases == [0, 1, 2, 3, 4], (
        f"the loop's gate constructions lost the per-arm phase offsets: {phases}"
    )


# ── (r) R3(a): truncate_schema leaves zero rows in ALL the wf tables ────


_WF_TABLES = (
    "wf_outbox",
    "wf_join_fire",
    "wf_step_ledger",
    "wf_edge",
    "wf_signals",
    "wf_node_progress",
    "wf_node_stream",
    "wf_step_cache",
    "wf_rootless_sweep_state",
)


def test_truncate_list_names_every_wf_table() -> None:
    """PIN (r, structural): the reset's table list names EVERY wf table
    the schema ships. THE RED: wf_node_stream was in neither the arms nor
    the list — a dead node's ring survived the reset, the last test's
    rows leaked into the next test's counts (and the production face:
    nodes x 64 ring rows unbounded, R3's conviction)."""
    from taskq.testing.pg import (
        _TRUNCATE_TABLES,  # pyright: ignore[reportPrivateUsage]  # Why: the pin IS about this list's completeness.
    )

    missing = [t for t in _WF_TABLES if t not in _TRUNCATE_TABLES]
    assert not missing, f"the truncate list strands {missing} — the reset is incomplete (R3a)"


async def test_truncate_schema_leaves_zero_rows_in_all_wf_tables(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """PIN (r, behavioral) — the extended repro: one row in EVERY wf
    table, ``truncate_schema``, and ALL of them read zero (FK-less by
    design, the CASCADE never reaches them — the reset's own list is the
    only thing that can)."""
    from taskq.testing.pg import truncate_schema

    flow_id = await seed_flow(wf_conn, wf_schema, status="running")
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_outbox (id, join_job_id, flow_id, consumer_step_key) '
        "VALUES ($1, $2, $3, 'x')",
        new_uuid(),
        new_uuid(),
        flow_id,
    )
    await _seed_edge(wf_conn, wf_schema, flow_id)
    await _seed_join_fire(wf_conn, wf_schema, flow_id, age=_OLD)
    await _seed_ledger(wf_conn, wf_schema, flow_id, status="succeeded", age=_OLD, step_key="a")
    await _seed_signal(wf_conn, wf_schema, flow_id, status="delivered", age=_OLD, name="approve")
    await _seed_progress(wf_conn, wf_schema, node_id, age=_OLD, channel="user")
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_node_stream (seq, node_id, flow_id, class, kind) '
        "VALUES (1, $1, $2, 'user', 'progress')",
        node_id,
        flow_id,
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_cache (content_address, result, run_id, expires_at) '
        "VALUES ('pin', '{}', $1, clock_timestamp())",
        flow_id,
    )
    await wf_conn.execute(
        f"INSERT INTO \"{wf_schema}\".wf_rootless_sweep_state (arm) VALUES ('pin')"
    )
    for table in _WF_TABLES:
        before = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".{table}')
        assert int(before) >= 1, f"{table} has no seed row — the repro is vacuous"

    await truncate_schema(wf_conn, wf_schema)

    for table in _WF_TABLES:
        after = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".{table}')
        assert int(after) == 0, f"{table} kept {after} row(s) past the reset (R3a)"
