"""The T18 workflow-aware-pruner pins — the retention/result-expiry
coupling (B4's owner), red-first per the doctrine.

THE COUPLING: result-expiry must not eat a map's children before the join
fires (EXPIRY-EATS-CHILDREN); retention must not prune a live run's
parent rows (PRUNED-PARENT — the sweep's ledger recount reads parent rows
as truth); the guards are WHERE-class conditions on the EXISTING arms
(no new sweep process, §22.6 exclusivity preserved); the guards must NOT
over-hold (a terminal run prunes on the normal schedule); and the
collect's FailureInfo JSONB is BOUNDED (UNBOUNDED-JSONB: the cap + the
compaction — the full detail stays on the ledger).

THE CLOCK MECHANISM, NAMED (GAPS-ESTATE F13): the fixture SHORTENS
``result_expires_at`` on the rows (an UPDATE) — a FakeClock advance moves
nothing the expiry sweep reads (the sweep's clock is the DATABASE's own
``statement_timestamp()``; the two-clock doctrine); advancing the DB
clock is the alternative and is not needed here: the fixture writes the
rows' expiry into the past directly.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.backend._sweeps import sweep_expired_results
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows._types import ChildSpec, ConsumerBinding, ForkSpec, JoinSpec
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import (
    RedLog,
    claim_view,
    fire_count,
    node_state,
    seed_flow,
    seed_join,
    seed_running_node,
)

# ── Pin 1: EXPIRY-EATS-CHILDREN ─────────────────────────────────────────


@pytest.mark.integration
async def test_t18_pin1_expiry_eats_children(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """A 1000-child map forced so its children's ``result_expires_at``
    elapses BEFORE the join fires (the fixture SHORTENS the rows' expiry —
    the named clock mechanism): the reduce's batch read must see ALL 1000
    results. THE UNGUARDED VARIANT (the expiry applies mid-run) reds with
    the reduce reading NULLs/holes — the record showing a silent
    partial."""

    n = 1000
    flow_id = await seed_flow(wf_conn, wf_schema)
    fork_parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=fork_parent,
        step_key="map",
        worker_id=(await _claim(wf_conn, wf_schema, fork_parent)),
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        fork=ForkSpec(
            children=tuple(
                ChildSpec(step_key="child", actor="wf", queue="default", map_index=m)
                for m in range(n)
            ),
            join=JoinSpec(
                step_key="reduce",
                actor="wf",
                queue="default",
                consumers=(ConsumerBinding(step_key="post", actor="wf", queue="default"),),
            ),
        ),
    )
    assert result.applied
    join_row = await wf_conn.fetchrow(
        f'SELECT id FROM "{wf_schema}".jobs WHERE parent_id = $1 '
        'AND metadata @> \'{"blocking_reason": "join"}\'::jsonb',
        fork_parent,
    )
    assert join_row is not None
    join_job_id = JobId(join_row["id"])

    # The children "complete": terminal succeeded WITH results stored, the
    # completion's own expiry stamp — then THE FIXTURE SHORTENS the
    # children's result_expires_at INTO THE PAST (the named clock
    # mechanism; a FakeClock advance moves nothing the sweep reads).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now(), "
        "result = to_jsonb(jsonb_build_object('i', map_index::int)), "
        "result_expires_at = now() - interval '1 second' "
        "WHERE parent_id = $1 AND step_key = 'child'",
        fork_parent,
    )

    # THE SHIPPED EXPIRY SWEEP: the guard passes the children BY (their
    # joins are un-fired) — the results survive.
    expired = await sweep_expired_results(module_pg_pool, schema=wf_schema)
    survivors = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1 AND result IS NOT NULL',
        fork_parent,
    )
    propagation_redlog.red(
        "t18-pin1-expiry-eats-children",
        "the guard stripped from the expiry arm — the children's results "
        "expire BEFORE the join fires, the reduce reads holes (the silent "
        "partial)",
        {"expired": expired, "survivors": survivors},
    )
    assert survivors == n, (
        f"EXPIRY-EATS-CHILDREN: {n - survivors} results expired while "
        "their join is un-fired — the reduce reads holes"
    )

    # The children terminalize THROUGH THE ENGINE (the decrements) — the
    # join fires; the reduce's batch read sees ALL 1000 results.
    # The children were flipped terminal ABOVE (the fixture's own
    # update); their decrements never ran — the SWEEP's rederive is the
    # shipped healer for exactly that window (the crash-window heal).
    await sweep_join_rederive(module_pg_pool, wf_sql)
    assert await fire_count(wf_conn, wf_schema, join_job_id) == 1, (
        "the join fires once every child is terminal"
    )
    # THE REDUCE'S BATCH READ: all 1000 results, never a NULL/hole.
    results = await wf_conn.fetch(
        f'SELECT result FROM "{wf_schema}".jobs '
        "WHERE parent_id = $1 AND map_index IS NOT NULL ORDER BY map_index",
        fork_parent,
    )
    assert len(results) == n
    assert all(r["result"] is not None for r in results), (
        "the reduce must never read a hole: the expiry held until the join fired"
    )

    # THE UNGUARDED VARIANT (the revert drill, live): the expiry WITHOUT
    # the guard eats the children — the survivors assert reds.
    unguarded = wf_sql_unguarded_expiry(wf_sql)
    flow2 = await seed_flow(wf_conn, wf_schema)
    join2 = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata) "
        f"VALUES ($1::uuid, 'wf', 'default', '{{}}'::jsonb, 3, 'transient', "
        f"'pending'::\"{wf_schema}\".job_status, 'reduce2', 1, "
        "to_jsonb(jsonb_build_object('flow_id', $2::text, 'blocking_reason', 'join')))",
        join2,
        str(flow2),
    )
    child2 = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, step_key, result, result_expires_at, metadata, "
        "idempotency_scope, idempotency_key) "
        f"VALUES ($1::uuid, 'wf', 'default', '{{}}'::jsonb, 3, 'transient', "
        f"'succeeded'::\"{wf_schema}\".job_status, 1, 'child2', "
        "'{\"i\": 1}'::jsonb, now() - interval '1 second', "
        "to_jsonb(jsonb_build_object('flow_id', $2::text)), "
        "'workflow-scope', 'key2')",
        child2,
        str(flow2),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        join2,
        child2,
        flow2,
    )
    # Drain to empty: the unguarded sweep eats the guard-held rows too —
    # the child2 row must fall (the conviction: the expiry applies mid-run).
    for _ in range(10):
        tag = await module_pg_pool.execute(unguarded, 500)
        if tag == "UPDATE 0":
            break
    holes = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1 AND result IS NULL',
        child2,
    )
    assert holes == 1, (
        "the unguarded variant did not eat the child's result — the red "
        "comparator is broken (the guard must be load-bearing)"
    )


def wf_sql_unguarded_expiry(wf_sql: WorkflowSql) -> str:
    """THE CONVICTED SHAPE: the expiry arm WITHOUT the workflow-liveness
    guard — the pre-T18 shipped statement (the revert drill's target)."""
    from taskq.backend import _sweeps

    mutated = _sweeps._SWEEP_RESULT_TTL_SQL.replace(  # pyright: ignore[reportPrivateUsage]  # Why: the convicted variant is the SHIPPED statement minus its guard — the mutation is the drill.
        """      AND NOT EXISTS (
          SELECT 1
          FROM "{schema}".wf_edge e
          JOIN "{schema}".jobs c ON c.id = e.child_id
          WHERE e.parent_id = jobs.id
            AND c.status = 'pending'
            AND c.deps_pending > 0
            AND c.metadata @> '{{"blocking_reason": "join"}}'::jsonb
      )
""",
        "",
    )
    assert mutated != _sweeps._SWEEP_RESULT_TTL_SQL, (  # pyright: ignore[reportPrivateUsage]
        "the mutation drill did not arm"
    )
    return mutated.format(schema=wf_sql.schema)


async def _claim(wf_conn: asyncpg.Connection, wf_schema: str, node_id: JobId) -> JobId:
    worker, _attempt, _epoch = await claim_view(wf_conn, wf_schema, node_id)
    return worker


# ── Pin 2 + 3: PRUNED-PARENT + the over-hold audit ──────────────────────


@pytest.mark.integration
async def test_t18_pin2_pruned_parent_of_a_live_run(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """Retention pruning a parent row of a LIVE run mid-march: the guard
    PREVENTS the prune (the sweep's recount reads parent rows as truth; a
    pruned parent mid-run corrupts the count). THE OVER-HOLD AUDIT (pin
    3): a TERMINAL run's rows prune on the NORMAL schedule — a guard that
    holds every workflow row forever reds this pin."""
    from datetime import timedelta

    from taskq.constants import DEFAULT_PRUNE_RETENTION
    from taskq.worker._leader_shared import prune_terminal_jobs

    # ── the live run: a TERMINAL parent of a join-wait child.
    flow_id = await seed_flow(wf_conn, wf_schema)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        join_id,
        parent,
        flow_id,
    )
    # The parent is terminal-past-retention (the prune's own eligibility).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = "
        "now() - interval '40 days' WHERE id = $1",
        parent,
    )

    async with module_pg_pool.acquire() as prune_conn:
        await prune_terminal_jobs(
            prune_conn,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=wf_schema,
        )
    still_there = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', parent
    )
    propagation_redlog.red(
        "t18-pin2-pruned-parent",
        "the guard stripped from the candidate predicate — the live run's "
        "parent prunes mid-march, the recount's count corrupted by the "
        "missing row",
        {"still_there": still_there},
    )
    assert still_there == 1, (
        "PRUNED-PARENT: retention pruned a parent row of a live run — the "
        "recount reads parent rows as truth"
    )

    # ── THE OVER-HOLD AUDIT (pin 3): the SAME parent, terminal RUN: the
    # flow root terminal → the row prunes on the NORMAL schedule.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'cancelled' WHERE id = $1", flow_id
    )
    async with module_pg_pool.acquire() as prune_conn2:
        await prune_terminal_jobs(
            prune_conn2,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=wf_schema,
        )
    gone = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', parent)
    assert gone == 0, (
        "the guard OVER-HOLDS: a terminal run's rows must prune on the "
        "normal schedule (the guard is liveness-scoped, not a retention "
        "exemption)"
    )
    _ = DEFAULT_PRUNE_RETENTION


# ── Pin 4: UNBOUNDED-JSONB (the cap + the compaction) ───────────────────


@pytest.mark.integration
async def test_t18_pin4_unbounded_jsonb_bounded_collect_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """A collect whose FailureInfos total > the cap must land a BOUNDED
    row (the compaction: the summary + the ledger pointers — the full
    detail still on the ledger/attempts; the record never loses it); the
    unbounded variant reds the row-size assert."""
    from taskq.workflows._sql_finalize import FANIN_FAILURES_BYTE_CAP

    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    child = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge_collect(wf_conn, wf_schema, join_id, child, flow_id)

    # The child's ledger rows: the attempt history (the detail's home).
    for attempt in (1, 2):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_step_ledger '
            "(id, flow_id, job_id, step_key, map_index, attempt, status, error_class, error_message) "
            "VALUES ($1, $2, $3, 'child', NULL, $4, 'failed', 'ValueError', $5)",
            new_uuid(),
            flow_id,
            child,
            attempt,
            f"boom-{attempt}-" + "x" * 2000,
        )

    async with module_pg_pool.acquire() as conn:
        # MANY fan-ins of a FAT item: the array crosses the cap — the row
        # must land BOUNDED (the compaction), never unbounded.
        for _ in range(60):
            fanin_rows = await conn.fetch(
                wf_sql.collect_fan_in,
                child,
                flow_id,
                "child",
                None,
            )
            for row in fanin_rows:
                await conn.execute(
                    wf_sql.collect_fan_in_append,
                    row["join_job_id"],
                    json.dumps(
                        {
                            "node_key": "child",
                            "map_index": None,
                            "policy": "collect",
                            "error": {
                                "error_class": "ValueError",
                                "error_message": "boom-" + "y" * 2000,
                                "error_traceback": None,
                            },
                            "attempts": [
                                {"attempt": 1, "error_class": "ValueError", "error_message": "b"},
                                {"attempt": 2, "error_class": "ValueError", "error_message": "b"},
                            ],
                        }
                    ),
                    FANIN_FAILURES_BYTE_CAP,
                )

    state = await node_state(wf_conn, wf_schema, join_id)
    failures: Any = state["metadata"].get("failures")
    size = len(json.dumps(state["metadata"]["failures"]).encode())
    propagation_redlog.red(
        "t18-pin4-unbounded-jsonb",
        "the fan-in append unbounded (the plain concat) — the collect row "
        "grows with the failure count (the P3 residual risk, measured "
        "real)",
        {"failures_size_bytes": size},
    )
    assert size <= FANIN_FAILURES_BYTE_CAP, (
        f"UNBOUNDED-JSONB: the collect row's failures array is {size} "
        f"bytes (cap {FANIN_FAILURES_BYTE_CAP}) — the row must land bounded"
    )
    # The compaction's record: the marker + the ledger pointer, the
    # detail's home named.
    assert isinstance(failures, dict), (
        f"the over-cap row must be the COMPACTED summary, got {type(failures)}"
    )
    assert failures.get("__truncated__", 0) >= 1
    assert "wf_step_ledger" in str(failures.get("detail_home", ""))
    # THE RECORD NEVER LOSES IT: the full detail is still on the ledger.
    ledger_rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE job_id = $1',
        child,
    )
    assert ledger_rows == 2, "the full attempt history stays on the ledger"


async def seed_edge_collect(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    child_id: JobId,
    parent_id: JobId,
    flow_id: JobId,
) -> None:
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'collect')",
        child_id,
        parent_id,
        flow_id,
    )
