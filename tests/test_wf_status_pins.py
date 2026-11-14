"""The state-totality status + progress pins: the derivation table's rows (each
mutation-checked — flipping any two rows' precedence must red), the
rows-only reconstruction (the kill-everything shape), the gauge's
cardinality law (never per-node labels; the `_other_` collapse), the
query-count pin (the admin's status panel and the gauge share one read),
the G7 always-on assertion's teeth, the (state, event) totality table,
and one-run-one-trace under concurrency.

Driven against a live Postgres through the REAL engine; the derivation's
pure function lives in ``taskq.workflows._status`` (the table stated
before the failed row — the absorbed-failure clause, B2).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import (
    NodeView,
    WorkflowStatus,
    derive_workflow_status,
    reconstruct_workflow_status,
)
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import (
    RedLog,
    claim_view,
    g7_check,
    node_state,
    seed_edge,
    seed_flow,
    seed_join,
    seed_running_node,
    write_band_artifact,
)


def _nodes(*statuses: str, **kwargs: Any) -> tuple[NodeView, ...]:
    """A node multiset from bare status names (the property's helper —
    kwargs: deps_pending, blocking_reason, held, absorbed,
    cancel_in_flight applied to every node)."""
    return tuple(NodeView(status=s, deps_pending=kwargs.get("deps_pending", 0)) for s in statuses)


# ── The derivation table's rows + the precedence matrix ────────────────


def test_derivation_rows_precedence_matrix() -> None:
    """Every rule row exercised, IN ORDER — and each FLIP caught: swapping
    any two rows' precedence reds (the derivation is order-sensitive by
    the table's own contract). The absorbed-failure clause is stated
    first (B2): the partial-failure collect derives through its parent,
    never failed."""
    # Row 1: any running → running (even with a failed node present —
    # the flip: failed-before-running would red this row).
    assert derive_workflow_status(_nodes("running", "failed")) == "running"
    assert derive_workflow_status(_nodes("running", "succeeded")) == "running"
    assert (
        derive_workflow_status((NodeView(status="pending", cancel_in_flight=True),)) == "running"
    ), "the cancel-in-flight leg"
    # crashed/abandoned are terminals (the state machine gives them zero
    # outbound transitions; no sweep arm ever reclaims them) — they fold into
    # the failed-class terminal, never liveness. The old claim ("a crashed node
    # is the reclaim's input — the run is live") was the wedge: the {succeeded,
    # crashed} corpse derived 'running' forever.
    assert derive_workflow_status((NodeView(status="crashed"),)) == "failed", (
        "the terminal-crash fold: a crashed row is the failed-class "
        "terminal — the reclaim's input is 'running' only"
    )

    # Row 2 (B2): the absorbed failure derives through its parent — a
    # collect's 3 Failed among 997 Ok derive complete, never failed; the
    # maybe-absorption the same.
    collect_partial = tuple(
        [NodeView(status="succeeded")] * 997
        + [
            NodeView(status="failed", absorbed=True),
            NodeView(status="failed", absorbed=True),
            NodeView(status="failed", absorbed=True),
        ]
    )
    assert derive_workflow_status(collect_partial) == "complete", (
        "the absorbed-failure clause: the partial-failure collect derives "
        "through the parent's outcome, never failed"
    )
    # The flip: un-absorb one of them → the failed row fires.
    un_absorbed = (*collect_partial[:-1], NodeView(status="failed"))
    assert derive_workflow_status(un_absorbed) == "failed", (
        "the flip is caught: one non-absorbed failure → failed"
    )

    # Row 3: any non-absorbed failed (none running) → failed.
    assert derive_workflow_status(_nodes("failed", "succeeded")) == "failed"

    # Row 4: held / join-wait / blocked → blocked (the pending-row
    # representations — no hold/pending_join status exists to group).
    assert derive_workflow_status((NodeView(status="pending", deps_pending=2),)) == "blocked", (
        "join-wait derives blocked"
    )
    assert (
        derive_workflow_status((NodeView(status="pending", blocking_reason="orphan_parent"),))
        == "blocked"
    )
    assert derive_workflow_status((NodeView(status="pending", held=True),)) == "blocked", (
        "the held shape derives blocked"
    )

    # Row 5: all terminal-succeeded/skipped → complete.
    assert derive_workflow_status(_nodes("succeeded", "succeeded")) == "complete"
    assert derive_workflow_status(_nodes("succeeded", "skipped")) == "complete"

    # Row 5.5 (the terminal-crash fold): a crashed/abandoned node IS the
    # failed-class terminal — the deterministic death (the reclaim's
    # crashed branch wrote it: the budget exhausted, nothing will ever
    # revive it; the state machine gives it zero outbound transitions).
    # the old row-1 read crashed as liveness and the {succeeded, crashed}
    # corpse derived 'running' forever.
    assert derive_workflow_status(_nodes("succeeded", "crashed")) == "failed", (
        "the terminal-crash fold: a budget-exhausted crash terminal is the "
        "failed-class terminal — never liveness (the crashed-terminal wedge)"
    )
    assert (
        derive_workflow_status(
            _nodes(
                "crashed",
            )
        )
        == "failed"
    )
    assert derive_workflow_status(_nodes("abandoned", "succeeded")) == "failed"
    # The flip: the fold's absorption consistency — an absorbed crash
    # derives through its parent's outcome (the collect's partial-failure
    # clause reads the crash the way it reads the failure).
    assert (
        derive_workflow_status(
            (NodeView(status="succeeded"), NodeView(status="crashed", absorbed=True))
        )
        == "complete"
    )
    # And the fold loses only to row 1's genuine liveness (a running row
    # — the reclaim's real input — or a cancel in flight), never to the
    # crash itself.
    assert derive_workflow_status(_nodes("running", "crashed")) == "running"

    # Row 6: any cancelled → cancelled (a cancel beats complete — the
    # flip: complete-before-cancelled would red).
    assert derive_workflow_status(_nodes("succeeded", "cancelled")) == "cancelled"

    # Row 7: plain queued nodes → pending.
    assert derive_workflow_status(_nodes("pending", "pending")) == "pending"


# ── The reconstruction: rows-only, the two-source rule (D4) ─────────────


@pytest.mark.integration
async def test_reconstruction_two_sources_rows_only(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """Kill everything mid-run (the worker and the admin), re-derive, and
    the status matches the pre-kill semantic state — reconstructed from
    ROWS ALONE: the ledger (the attempted terminals) + the node row's
    error jsonb (the never-granted terminals — a cancelled pending node,
    a skip: no ledger rows, the terminal CLASS rides the error row).
    Pinned so no status cache is ever introduced: the reconstruction
    consults no cached surface."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=2)
    child_a = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="a")
    child_b = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join_id, child_a, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, child_b, flow_id)

    # a terminalizes (an attempted terminal — the ledger carries it).
    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=child_a,
        step_key="a",
        worker_id=(await claim_view(wf_conn, wf_schema, child_a))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        result={"ok": "a"},
    )
    # b is cancelled while pending-never-granted (the cancel's own write —
    # no ledger row exists for it: the two-source rule's second source).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'cancelled', "
        "finished_at = now(), error_class = 'CancelledBeforeStart' WHERE id = $1",
        child_b,
    )

    # no cache anywhere — the reconstruction is a pure read. The sweep's pass
    # runs first (the rederive resolves the join: both parents terminal — the
    # fired join is claimable, never blocked).
    await sweep_join_rederive(module_pg_pool, wf_sql)
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    propagation_redlog.red(
        "t08-reconstruction-rows-only",
        "a status cache (the reconstruction consulting anything but rows — "
        "the pinned never-again variant)",
        {"reconstructed": reconstructed},
    )
    # a succeeded (the ledger's truth), b cancelled → the derivation's
    # cancelled row: the run's semantic state is cancelled — the same
    # verdict a root flip would report.
    assert reconstructed == "cancelled"

    # a root lied to ('running') must disagree with the rows — the check's
    # sensitivity demonstrated on this very state (the G7 teeth-scenario runs
    # the full lie through the fixture).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running' WHERE id = $1", flow_id
    )
    lied_root = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert lied_root == "running"
    assert reconstructed == "cancelled", "the rows reconstruct the truth the lie cannot touch"
    _ = join_id


@pytest.mark.integration
async def test_g7_teeth_the_lying_fixture_reds(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """The G7 always-on assertion's teeth, proven: a deliberately-lying
    fixture status (a root hand-written 'succeeded' while the rows say a
    node is still pending) REDS the check — the always-on assertion can
    fail, it is not a decoration."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    await seed_running_node(wf_conn, wf_schema, flow_id)
    # the root reports succeeded while a node is still pending — the exact
    # shape a status cache would produce.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded' WHERE id = $1",
        flow_id,
    )
    with pytest.raises(AssertionError, match="drifted from the rows"):
        await g7_check(wf_conn, wf_schema, wf_sql)
        # the scenario's lie is in-body only — the always-on teardown runs the same
    # check the scenario just rejected (the round made the teardown real), so
    # the record must end lawful: the rows (a pending node) are the truth, the
    # root is the cache.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running' WHERE id = $1",
        flow_id,
    )


# ── The gauge's cardinality law ─────────────────────────────────────────


def test_cardinality_never_per_node_labels() -> None:
    """The metric registration must REJECT a per-node label series — a
    per-node series cannot be created: the cache is (workflow, state)-keyed
    (a node-dimensioned key cannot enter — ATTEMPTED here, the rejection
    observed: the callback's strict pair-unpack is the enforcement, so a
    lenient `key[0], key[1]` rewrite REDS this pin), and the observation
    callback's label set is exactly {workflow, state}."""
    from taskq.obs._otel import (  # pyright: ignore[reportPrivateUsage]  # Why: the pin watches the registration's own surface.
        _observe_wf_progress,
        update_wf_progress_cache,
    )

    update_wf_progress_cache(
        {
            ("registered-wf", "running"): 7,
            ("registered-wf", "blocked"): 2,
            ("_other_", "pending"): 11,
        }
    )
    observations = list(_observe_wf_progress(None))  # pyright: ignore[reportArgumentType]  # Why: the callback ignores the options argument (the OTel contract).
    assert observations, "the callback must emit the sampled series"
    for obs in observations:
        labels = obs.attributes or {}
        assert set(labels) == {"workflow", "state"}, (
            f"the label set is exactly the declared-workflow dimension + "
            f"state, never a per-node label: {labels}"
        )
    # The _other_ collapse: the unregistered runs' summed counts ride one
    # bucket (the named-bucket precedent).
    others = [o for o in observations if (o.attributes or {}).get("workflow") == "_other_"]
    assert len(others) == 1 and others[0].value == 11

    # a node-dimensioned key (the rejected per-node series' shape) cannot enter
    # the cache — the callback's strict pair-unpack raises, the series is never
    # emitted.
    update_wf_progress_cache(
        {
            ("registered-wf", "running", "c7"): 1,  # type: ignore[dict-item]  # Why: the attempt — the rejected per-node key shape, rejected at the observation.
        }
    )
    with pytest.raises(ValueError, match="too many values to unpack"):
        list(_observe_wf_progress(None))  # pyright: ignore[reportArgumentType]  # Why: as above — the options argument is ignored.
        # the rejected key must not sit in the process-global cache for the next
    # observer (the scenario's own hygiene — the lawful shape goes back).
    update_wf_progress_cache(
        {
            ("registered-wf", "running"): 7,
            ("_other_", "pending"): 11,
        }
    )


@pytest.mark.integration
async def test_query_count_one_read_per_status_surface(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """one query per status read, INDEPENDENT OF the POPULATION: the
    status surfaces (the per-run reconstruction — the admin's debug view —
    and the wf-progress gauge's fleet sampler) are single-statement reads
    per class, never a per-row loop. the REAL-MUTATION SHAPE (the audit's
    rewrite — the pre-rewrite pin counted ITS own three calls through a
    passthrough, no surface driven, nothing could fail): the pin drives
    the REAL surfaces through a counting connection at TWO node counts —
    the statement count must not move with the population, so an N+1
    implementation (a per-node read loop — the monster class) REDS it,
    and a surface that grew a second instrument (a second query per
    status read) reds it too."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    for i in range(3):
        child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"a{i}")
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
            "WHERE id = $1",
            child,
        )
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="j", deps=2)

    queries: list[str] = []

    class CountingConn:
        """A passthrough that records the statement count."""

        def __init__(self, inner: asyncpg.Connection) -> None:
            self._inner = inner

        async def fetch(self, sql: str, *args: object) -> list[Any]:
            queries.append(sql)
            return await self._inner.fetch(sql, *args)

        async def fetchval(self, sql: str, *args: object) -> Any:
            queries.append(sql)
            return await self._inner.fetchval(sql, *args)

    counting = CountingConn(wf_conn)
    await reconstruct_workflow_status(counting, wf_sql, flow_id)
    small_run_statements = len(queries)
    # TWO statements: the node read + the never-granted read. The step
    # ledger is claim/attempt history and replay data — it is not a
    # current-status source, so the reconstruction reads NO ledger table
    # at all (a reintroduced ledger overlay — ordered or unordered —
    # fails this inventory AND the live-node behavior controls).
    assert small_run_statements == 2, queries
    for sql in queries:
        assert "wf_step_ledger" not in sql, (
            f"the reconstruction read the ledger — the ledger is "
            f"claim/attempt history, never a current-status source: {sql!r}"
        )

    # more terminal nodes + the join's edges. the statement count must not.
    for i in range(30):
        child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"b{i}")
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
            "WHERE id = $1",
            child,
        )
    for _ in range(2):
        extra = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="e")
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
            join_id,
            extra,
            flow_id,
        )
    queries.clear()
    await reconstruct_workflow_status(counting, wf_sql, flow_id)
    assert len(queries) == small_run_statements, (
        f"the reconstruction's statement count MOVED with the population "
        f"({small_run_statements} -> {len(queries)}) — a per-node read "
        "loop (the N+1 monster class) arrived"
    )

    # one grouped statement — the same read class the panel uses — never a
    # per-workflow query (the query-count pin's original claim, driven at the
    # real surface).
    from taskq.worker._leader_shared import (
        _QUERY_WF_PROGRESS_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]
    )

    queries.clear()
    await counting.fetch(_QUERY_WF_PROGRESS_SQL_TEMPLATE.format(schema=wf_schema))
    assert len(queries) == 1, (
        f"the gauge's fleet sample issued {len(queries)} statements — the "
        "gauge and the panel share ONE grouped read"
    )
    assert "GROUP BY" in queries[0]
    # The rollup (the panel's grouped read) — one statement, GROUP BY
    # carried.
    queries.clear()
    await counting.fetch(wf_sql.workflow_rollup, flow_id)
    assert len(queries) == 1 and "GROUP BY" in queries[0], queries


# ── The maintenance leg: the rows are truth, the root row is a cache ────


@pytest.mark.integration
async def test_maintenance_leg_finalizes_the_wedged_root(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """The crash-window root wedge (the phase-2 attack's wedge): tx1 commits
    the child's terminal FAILURE, tx2 (the cascade) never runs — the
    sweep's blocked_required heal stamps the join, but the maintenance
    leg's ``has_live`` counted the stamped join row LIVE forever (nothing
    dispatches it, nothing fires it, nothing will ever terminalize it), so
    the root wedged 'running': unbounded retention (the pruner's liveness
    guard holds every row) + the dead run re-scanned every sweep pass.
    The maintenance leg derives the root's terminal state
    FROM the ROWS — the same reconstruction the debug view uses — and
    FINALIZES the root in the same tx when the rows are terminal and the
    root row isn't (the named terminal state + the reason). One sweep pass
    releases the retention.
    The has_live predicate counting the
    resolved-blocked rows live — the root wedges 'running'."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="fc_join", deps=2)
    failed_child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c0")
    peer = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="c1")
    await seed_edge(wf_conn, wf_schema, join_id, failed_child, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, peer, flow_id)

    # tx1's fenced terminal failure lands (the direct write — the exact tx1
    # shape), tx2 (the cascade) never runs.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now(), "
        "error_class = 'ValueError' WHERE id = $1",
        failed_child,
    )
    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert summary.blocked_required >= 1, "the heal must stamp the join first"

    # The peer's own work completes (its worker finalizes) — now every
    # node row is terminal-or-resolved-blocked. One more sweep pass: the
    # maintenance leg's window to finalize the root.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
        peer,
    )
    await sweep_join_rederive(module_pg_pool, wf_sql)

    root = await wf_conn.fetchrow(
        f'SELECT status, finished_at, error_class FROM "{wf_schema}".jobs WHERE id = $1',
        flow_id,
    )
    assert root is not None
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    propagation_redlog.red(
        "t08-maintenance-root-wedge",
        "the maintenance leg without the failed-root arm (the old gate: the "
        "finalize required EVERY row terminal, and the stamped join row is "
        "terminalizable by nothing) — the root wedges 'running' forever "
        "(unbounded retention, the dead-run rescan every pass)",
        {
            "root_status": root["status"],
            "reconstructed": reconstructed,
            "finished_at": str(root["finished_at"]),
            "error_class": root["error_class"],
        },
    )
    assert reconstructed == "failed", "the rows reconstruct the un-absorbed failure"
    assert root["status"] == "failed", (
        f"THE ROOT WEDGE: the rows reconstruct {reconstructed!r} but the "
        f"root row reports {root['status']!r} — the maintenance leg must "
        "finalize the root from the rows in the same pass"
    )
    assert root["finished_at"] is not None, "the finalize stamps the completion"
    # the sweep's finalize names its mechanism — the root the cascade never
    # flipped carries the maintenance stamp (the cascade's own flip stamps the
    # peer-cancel origin instead).
    assert root["error_class"] is not None, "the finalize names the reason"
    # retention released: the root is terminal, so the pruner's liveness
    # guard (the retention coupling  no longer holds the run's rows — the terminal state is
    # the release. (The root's own rows prune on the normal schedule.)

    # live, on a fresh wedged flow. the maintenance leg without the failed-root
    # arm (the old gate's shape — the finalize requires every row terminal, and
    # the stamped join row is terminalizable by nothing) — the wedge reproduces
    # and the root never finalizes.
    mutated = wf_sql.workflow_root_maintain.replace(
        "(pf.has_failed AND NOT COALESCE(pf.has_unresolved, false))",
        "false",
    )
    assert mutated != wf_sql.workflow_root_maintain, "the mutation test did not arm"
    flow2 = await seed_flow(wf_conn, wf_schema)
    join2 = await seed_join(wf_conn, wf_schema, flow2, step_key="fc2_join", deps=1)
    child2 = await seed_running_node(wf_conn, wf_schema, flow2, step_key="d0")
    await seed_edge(wf_conn, wf_schema, join2, child2, flow2)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now() WHERE id = $1",
        child2,
    )
    async with module_pg_pool.acquire() as conn:
        await conn.execute(mutated, 200)
    wedged = await node_state(wf_conn, wf_schema, flow2)
    assert wedged["status"] == "running", (
        f"the mutated predicate did NOT wedge the root ({wedged['status']!r}) — "
        "the scenario's contract is broken: the mutant must reproduce the wedge"
    )
    # The shipped statement finalizes the same shape (the control arm —
    # the scenario's comparator is honest both ways).
    await sweep_join_rederive(module_pg_pool, wf_sql)
    healed = await node_state(wf_conn, wf_schema, flow2)
    assert healed["status"] == "failed", f"the shipped statement must finalize: {healed}"
    _ = join_id


# ── One run = one trace, under concurrency ──────────────────────────────


@pytest.mark.integration
async def test_one_run_one_trace_concurrent(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """10 concurrent runs — every surface flow-scoped, trace ids unique
    per run, event seqs never shared, ZERO cross-run bleed."""
    runs = []
    for r in range(10):
        flow_id = await seed_flow(wf_conn, wf_schema, workflow=f"traced-wf-{r}")
        runs.append(flow_id)

    async def one_run(flow_id: JobId, index: int) -> None:
        # short holds only: the leg's own reads/writes take a connection
        # and release it before the finalize (which manages its own
        # acquisitions) — a held connection across the finalize deadlocks
        # the pool (10 legs x 2 connections each).
        async with module_pg_pool.acquire() as conn:
            child = await seed_running_node(conn, wf_schema, flow_id, step_key=f"s{index}")
            trace_id = f"trace-{index}"
            await conn.execute(
                f'UPDATE "{wf_schema}".jobs SET trace_id = $2 WHERE id = $1',
                child,
                trace_id,
            )
            worker, attempt, epoch = await claim_view(conn, wf_schema, child)
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=child,
            step_key=f"s{index}",
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            outcome="succeeded",
            result={"run": index},
        )

    await asyncio.gather(*(one_run(rid, i) for i, rid in enumerate(runs)))

    # Every run's surfaces are flow-scoped: each run's ledger rows carry
    # only its own trace, trace ids unique per run, no shared seqs.
    traces: dict[JobId, str | None] = {}
    for flow_id in runs:
        row = await wf_conn.fetchrow(
            f'SELECT trace_id, count(*) AS n FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND trace_id IS NOT NULL "
            "GROUP BY trace_id",
            flow_id,
        )
        assert row is not None and row["n"] == 1
        traces[flow_id] = row["trace_id"]
    assert len(set(traces.values())) == len(runs), "trace ids unique per run — zero cross-run bleed"


# ── The rollup cost gate (measured, with a plan assert) ─────────────────


@pytest.mark.load_sensitive
@pytest.mark.integration
async def test_rollup_cost_gate_index_driven(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """At the FLEET shape the flow-scoped reads are INDEX-DRIVEN (the
    EXPLAIN assert: no seq scan of the fleet table) and inside the recorded
    band. the SHAPE IS the LOAD-BEARING PART (the phase-2 attack's H3b):
    the pre-rewrite pin measured a table that contained only the measured
    flow's own rows — where the measured flow IS the whole table, a seq
    scan and an index scan cost the same and the plan assert cannot fail.
    Here the measured run is one AMONG MANY on a fleet table (40 runs +
    a 60k-row vanilla population), the production shape — the seq-scan
    shape (the raw-TEXT index: the key the reads' uuid cast
    broke, measured p50 21.2 ms for the rollup and 20.7 ms for the
    per-node read at this very shape —
    .measurements/fix2/index-before-text-index.txt) REDS the plan assert.
    The ride: jobs_wf_flow_nodes_idx (01.00.26_02 — the index expression
    carries the same uuid cast the reads carry; the partial covers the
    workflow rows only)."""
    import time

    n_vanilla = 60_000
    n_flows = 40
    n_nodes = 500

    # production's fleet table is mostly not workflow rows — this is what makes
    # the plan assert honest (a seq scan must traverse this to find the
    # measured run).
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, scheduled_at, idempotency_scope, idempotency_key) "
        "SELECT gen_random_uuid(), 'van', 'default', '{}', 3, 'transient', "
        f"(ARRAY['succeeded','failed']::text[])[1 + (g % 2)]::{wf_schema}.job_status, "
        "'v', now() - interval '1 hour', 'scope', 'van-' || g "
        "FROM generate_series(1, $1) g",
        n_vanilla,
    )

    flow_ids: list[JobId] = []
    for _ in range(n_flows):
        flow_id = await seed_flow(wf_conn, wf_schema)
        flow_ids.append(flow_id)
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, idempotency_key) "
            "SELECT gen_random_uuid(), 'wf', 'default', '{}', 3, 'transient', "
            f"(ARRAY['succeeded','running','pending','failed']::text[])[1 + (g % 4)]::{wf_schema}.job_status, "
            "'c', jsonb_build_object('flow_id', $1::text, 'blocking_reason', 'join'), "
            "$1::text, "
            "'workflow:' || $1::text, 'wf:' || $1::text || ':' || g "
            "FROM generate_series(1, $2) g",
            str(flow_id),
            n_nodes,
        )

        # the vacuumed table (index scans need the visibility map; a freshly-seeded
    # table heap-visits every row — the two measurements both recorded, the
    # argument in the artifact).
    await wf_conn.execute("VACUUM (ANALYZE)")

    # no seq scan of jobs/wf_edge in any flow-scoped read (the rollup, the
    # per-node read, the maintenance leg — the fleet gauge's sampler is
    # deliberately fleet-wide: it groups the whole fleet, there is no run id to
    # key it; its recorded band is its own).
    async def assert_index_driven(sql: str, *args: object) -> None:
        plan_rows = await wf_conn.fetch(f"EXPLAIN (FORMAT JSON) {sql}", *args)
        plan = plan_rows[0][0]
        if isinstance(plan, str):
            plan = json.loads(plan)

        def walk(node: dict[str, Any]) -> None:
            node_type = node.get("Node Type")
            rel = node.get("Relation Name")
            # The fleet TABLE is the gate's subject: a seq scan of jobs at
            # the fleet shape is the monster class. (wf_edge is empty in
            # this shape — a seq scan of an empty relation is the
            # planner's correct, zero-cost pick, not a fleet walk; the
            # per-node probes ride wf_edge_parent_idx when rows exist.)
            assert not (node_type == "Seq Scan" and rel == "jobs"), (
                f"the read seq-scans {rel} at the fleet shape — the "
                "index-driven gate reds (the fix: 01.00.26_02's uuid-cast "
                "expression + the workflow-rows-only partial)"
            )
            for child in node.get("Plans", []) or []:
                walk(child)  # pyright: ignore[reportArgumentType]

        walk(plan[0]["Plan"])  # pyright: ignore[reportIndexType]

    measured_flow = flow_ids[-1]
    await assert_index_driven(wf_sql.workflow_rollup, measured_flow)
    await assert_index_driven(wf_sql.workflow_nodes, measured_flow)
    await assert_index_driven(wf_sql.workflow_root_maintain, 200)

    # the fleet-shape read is O(the run's own node count) — the measured p50
    # sits in the sub-millisecond class at the 500-node run shape (measured
    # 0.12-0.24 ms; the band is set from the measurement with CI-noise margin —
    # bands are set from measurements, then pinned — the band moves only by
    # argument in review).
    samples: list[float] = []
    for _ in range(5):
        start_ns = time.perf_counter_ns()
        rows = await module_pg_pool.fetch(wf_sql.workflow_rollup, measured_flow)
        samples.append((time.perf_counter_ns() - start_ns) / 1e6)
        assert rows, "the rollup answered"
    p50 = sorted(samples)[len(samples) // 2]
    band_budget_ms = 10.0
    assert p50 <= band_budget_ms, (
        f"the grouped rollup p50 {p50:.2f} ms exceeds the recorded band "
        f"({band_budget_ms} ms) at the fleet shape — re-argue the band in "
        "review, never edit it silently"
    )

    # the band is a run-scoped, head-stamped artifact — the in-place write_text
    # was the torn-write race + the unstamped rumor.
    write_band_artifact(
        "wf-rollup-band.json",
        {
            "shape": {
                "fleet_rows": n_vanilla + n_flows * (n_nodes + 1),
                "runs": n_flows,
                "nodes_per_run": n_nodes,
                "measured_flow": str(measured_flow),
            },
            "p50_ms": round(p50, 3),
            "samples_ms": [round(s, 3) for s in samples],
            "recorded_band_ms": band_budget_ms,
            "argument": (
                "the pin REWRITTEN for the fleet shape: the flow-solo shape measured a table "
                "containing ONLY the measured flow's rows — the "
                "measured flow WAS the whole table, so the plan assert "
                "could not fail on a seq scan. At the fleet shape the "
                "raw-TEXT index (01.00.26_01's key) served NO "
                "flow-scoped read: the rollup read p50 21.2 ms and the "
                "per-node read 20.7 ms, BOTH Seq Scans, linear in the "
                "fleet table (.measurements/fix2/index-before-text-"
                "index.txt). The fix (01.00.26_02: the uuid-cast "
                "expression + the metadata?'flow_id' partial, the "
                "reads carrying the proving clauses) puts every "
                "flow-scoped read on the index: p50 ~0.1-0.3 ms — "
                "O(the run's own node count). The plan gate is the "
                "load-bearing assert; the band is set from THIS "
                "measurement. The fleet-wide gauge sampler ("
                "_QUERY_WF_PROGRESS_SQL_TEMPLATE) is fleet-wide BY "
                "DESIGN (no run id to key); its band is recorded in "
                "index-fleet-bands.json (~23 ms at the 220k-row fleet)."
            ),
            "plan": "index-driven (jobs_wf_flow_nodes_idx, 01.00.26_02)",
        },
    )


# ── The reconstruction's ledger terminal identity ───────────────────────


async def _seed_wf_row(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: uuid.UUID,
    *,
    job_id: uuid.UUID | None = None,
    status: str,
    step_key: str,
    map_index: int | None = None,
    attempt: int = 0,
    deps_pending: int = 0,
    metadata: dict[str, object] | None = None,
    fairness_key: str | None = None,
    actor: str = "wf",
) -> None:
    """One jobs row in the run's raw shape (the reconstruction's input)."""
    row_id = job_id or new_uuid()
    await conn.execute(
        f'''INSERT INTO "{schema}".jobs
            (id, actor, queue, payload, attempt, max_attempts, retry_kind,
             status, step_key, map_index, deps_pending, metadata,
             idempotency_scope, idempotency_key, fairness_key)
            VALUES ($1, $2, 'default', '{{}}'::jsonb, $3, 3, 'transient',
                    $4, $5, $6, $7, $8::jsonb, 'probe', $9, $10)''',
        row_id,
        actor,
        attempt,
        status,
        step_key,
        map_index,
        deps_pending,
        json.dumps({"flow_id": str(flow_id), **(metadata or {})}),
        f"probe:{row_id}",
        fairness_key,
    )


async def _seed_ledger_terminal(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: uuid.UUID,
    *,
    job_id: uuid.UUID,
    step_key: str,
    map_index: int | None,
    attempt: int,
    status: str,
) -> None:
    """One ledger row (the claim's own attempt record)."""
    await conn.execute(
        f'''INSERT INTO "{schema}".wf_step_ledger
            (id, flow_id, job_id, step_key, map_index, attempt, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7)''',
        new_uuid(),
        flow_id,
        job_id,
        step_key,
        map_index,
        attempt,
        status,
    )


# ── The reconstruction's live-node law (the ledger is history, not state) ──
#
# One control per demonstrated wrong direction. Discipline, every control:
# (1) assert the EXACT seeded jobs + ledger tuples (a fixture accident can
# never silently turn the test into another aggregate), (2) reconstruct and
# assert the EXACT verdict, (3) run the REAL root-maintenance statement and
# assert the root. `assert verdict in ("pending", "blocked")` is never
# used: it would let a false-complete or false-failed bug hide.


async def _assert_jobs(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: uuid.UUID,
    expected: set[tuple[str, int | None, str, int]],
) -> None:
    """The exact seeded node tuples: (step_key, map_index, status, attempt)."""
    rows = await conn.fetch(
        f'''SELECT step_key, map_index, status::text AS status, attempt
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key <> '__flow__'
            ORDER BY step_key, COALESCE(map_index, -1)''',
        flow_id,
    )
    got = {(r["step_key"], r["map_index"], r["status"], r["attempt"]) for r in rows}
    assert got == expected, f"the seeded jobs tuples drifted: {sorted(got)!r}"


async def _assert_ledger(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: uuid.UUID,
    expected: set[tuple[str, int | None, int, str]],
) -> None:
    """The exact seeded ledger tuples: (step_key, map_index, attempt, status)."""
    rows = await conn.fetch(
        f'''SELECT step_key, map_index, attempt, status::text AS status
            FROM "{schema}".wf_step_ledger WHERE flow_id = $1::uuid''',
        flow_id,
    )
    got = {(r["step_key"], r["map_index"], r["attempt"], r["status"]) for r in rows}
    assert got == expected, f"the seeded ledger tuples drifted: {sorted(got)!r}"


async def _assert_root_running(
    conn: asyncpg.Connection, schema: str, wf_sql: WorkflowSql, flow_id: uuid.UUID
) -> None:
    """The REAL root-maintenance statement runs, and the live root stands."""
    await conn.execute(wf_sql.workflow_root_maintain, 100)
    root = await conn.fetchval(f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', flow_id)
    assert root == "running", (
        f"the root writer finalized a LIVE run ({root!r}) — reconstruction "
        "and maintenance disagree on the same committed rows"
    )


async def _seed_held_step(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: uuid.UUID,
    *,
    step_key: str = "publish",
    substep_key: str = "before_publish",
    substep_status: str = "succeeded",
) -> uuid.UUID:
    """The REAL hold shape: the outer row pending under the hold metadata
    (`_HOLD_HOLD_NODE_SQL`'s write), its own claim ledger row 'awaited'
    (`mark_awaited` — resume-not-retry), and an inner ``ctx.substep``
    claim's terminal at the SAME attempt (substeps share the outer job id
    and attempt; they are their own step keys)."""
    outer = new_uuid()
    await _seed_wf_row(
        conn,
        schema,
        flow_id,
        job_id=outer,
        status="pending",
        step_key=step_key,
        attempt=1,
        metadata={"hold": str(new_uuid()), "held_signal": "gate"},
        fairness_key=str(flow_id),
    )
    await _seed_ledger_terminal(
        conn,
        schema,
        flow_id,
        job_id=outer,
        step_key=step_key,
        map_index=None,
        attempt=1,
        status="awaited",
    )
    await _seed_ledger_terminal(
        conn,
        schema,
        flow_id,
        job_id=outer,
        step_key=substep_key,
        map_index=None,
        attempt=1,
        status=substep_status,
    )
    return outer


@pytest.mark.integration
async def test_reconstruction_failed_map_sibling_leaves_the_live_node_pending(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """FAILED map-sibling direction. Child A is pending at its own first
    attempt; sibling B (same step key, DIFFERENT map index and job) is the
    ladder's re-pend donor: scheduled at attempt 2 behind its own failed
    attempt-2 ledger terminal — the one named live donor the ruling
    admits. With no other node in the run the verdict is EXACTLY
    ``pending``: a sibling's terminal is that sibling's claim's history
    and can never answer for child A.
    RED on the base (the step-key collapse keyed A to B's failed terminal
    and derived ``failed`` — a live run reported dead by its own status
    read). A second phase adds the fan-in join and asserts the aggregate
    ``blocked`` — the join must never be needed to see the node truth."""
    flow = new_uuid()
    child_a = new_uuid()
    donor = new_uuid()
    meta: dict[str, object] = {"workflow": "probe_march"}
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=flow,
        actor="flow",
        status="running",
        step_key="__flow__",
        metadata=meta,
    )
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=child_a,
        status="pending",
        step_key="sources.item",
        map_index=0,
        attempt=1,
        metadata=meta,
        fairness_key=str(flow),
    )
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=donor,
        status="scheduled",
        step_key="sources.item",
        map_index=1,
        attempt=2,
        metadata=meta,
        fairness_key=str(flow),
    )
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow,
        job_id=donor,
        step_key="sources.item",
        map_index=1,
        attempt=2,
        status="failed",
    )
    # (1) the exact seeded tuples.
    await _assert_jobs(
        wf_conn,
        wf_schema,
        flow,
        {("sources.item", 0, "pending", 1), ("sources.item", 1, "scheduled", 2)},
    )
    await _assert_ledger(
        wf_conn,
        wf_schema,
        flow,
        {("sources.item", 1, 2, "failed")},
    )
    # (2) the reconstruction: EXACTLY pending.
    verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
    assert verdict == "pending", (
        f"a sibling's failed retry terminal reconstructed the live run "
        f"{verdict!r} — the terminal is the donor claim's history; child A "
        "is pending and the run is live"
    )
    # (3) the real root maintenance agrees: the live root stands.
    await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)

    # Second phase — the join aggregate, seeded ONLY now: the map family's
    # fan-in reads blocked while child A stays pending. The join may never
    # be required to see A's own truth.
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        status="pending",
        step_key="sources.join",
        deps_pending=4,
        metadata={**meta, "blocking_reason": "join"},
        fairness_key=str(flow),
    )
    joined = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
    assert joined == "blocked", (
        f"the fan-in phase derived {joined!r} — the join-wait row's own "
        "blocked shape is the aggregate"
    )
    await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)


@pytest.mark.integration
async def test_reconstruction_succeeded_map_sibling_leaves_the_live_node_pending(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """SUCCEEDED map-sibling direction (the false-complete killer the old
    aggregate control missed): child A pending at its own first attempt;
    sibling B genuinely terminal — its job row succeeded, its own ledger
    terminal succeeded at B's attempt — and NO other node in the run. The
    verdict is EXACTLY ``pending``, never ``complete``, and the root
    stays running.
    RED on the base (the step-key collapse keyed A to B's succeeded
    terminal; the all-succeeded rows derived ``complete`` — a live run
    reported finished). Also pins the NULL-map-index / map-index-0
    distinction in the arbiter tuple: the COALESCE key can never
    conflate the two."""
    flow = new_uuid()
    child_a = new_uuid()
    sibling = new_uuid()
    meta: dict[str, object] = {"workflow": "probe_march"}
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=flow,
        actor="flow",
        status="running",
        step_key="__flow__",
        metadata=meta,
    )
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=child_a,
        status="pending",
        step_key="sources.item",
        map_index=0,
        attempt=1,
        metadata=meta,
        fairness_key=str(flow),
    )
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=sibling,
        status="succeeded",
        step_key="sources.item",
        map_index=1,
        attempt=1,
        metadata=meta,
        fairness_key=str(flow),
    )
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET finished_at = now() WHERE id = $1',
        sibling,
    )
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow,
        job_id=sibling,
        step_key="sources.item",
        map_index=1,
        attempt=1,
        status="succeeded",
    )
    await _assert_jobs(
        wf_conn,
        wf_schema,
        flow,
        {("sources.item", 0, "pending", 1), ("sources.item", 1, "succeeded", 1)},
    )
    await _assert_ledger(
        wf_conn,
        wf_schema,
        flow,
        {("sources.item", 1, 1, "succeeded")},
    )
    verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
    assert verdict == "pending", (
        f"the succeeded sibling reconstructed the live run {verdict!r} — "
        "B's terminal is B's own claim's history; child A is pending and "
        "the run is NOT complete"
    )
    await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)

    # The NULL-vs-0 poison: two rows sharing one step key whose map
    # indices are NULL and 0 — the arbiter's COALESCE(map_index, -1) key
    # must return exactly each claim's own row for its own tuple (the
    # direct keyed check: no helper may conflate NULL with 0).
    null_child, zero_child = new_uuid(), new_uuid()
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=null_child,
        status="pending",
        step_key="mi.poison",
        map_index=None,
        attempt=1,
        metadata=meta,
        fairness_key=str(flow),
    )
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=zero_child,
        status="pending",
        step_key="mi.poison",
        map_index=0,
        attempt=1,
        metadata=meta,
        fairness_key=str(flow),
    )
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow,
        job_id=null_child,
        step_key="mi.poison",
        map_index=None,
        attempt=1,
        status="failed",
    )
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow,
        job_id=zero_child,
        step_key="mi.poison",
        map_index=0,
        attempt=1,
        status="succeeded",
    )
    null_rows = await wf_conn.fetch(
        f'''SELECT job_id::text FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'mi.poison'
              AND COALESCE(map_index, -1) = COALESCE($2::smallint, -1)
              AND attempt = 1''',
        flow,
        None,
    )
    zero_rows = await wf_conn.fetch(
        f'''SELECT job_id::text FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'mi.poison'
              AND COALESCE(map_index, -1) = COALESCE($2::smallint, -1)
              AND attempt = 1''',
        flow,
        0,
    )
    assert [r["job_id"] for r in null_rows] == [str(null_child)], (
        "the NULL map index's keyed lookup must return exactly its own row"
    )
    assert [r["job_id"] for r in zero_rows] == [str(zero_child)], (
        "the 0 map index's keyed lookup must return exactly its own row — "
        "NULL and 0 are distinct arbiter tuples, never conflated"
    )


@pytest.mark.integration
async def test_reconstruction_held_step_with_inner_substep_stays_pending(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """HELD STEP + an inner ``ctx.substep`` terminal at the same attempt.
    The outer row is pending under the REAL hold metadata, its own claim's
    ledger row reads ``awaited`` (resume-not-retry), and the substep — its
    OWN step key sharing the outer job id and attempt — is terminal. With
    the outer node the run's ONLY non-root node row, the verdict is
    EXACTLY ``pending`` (a job-wide terminal map derives ``complete``
    here: the substep's terminal would answer for the held claim), and
    the root stays running. The failed-substep companion must likewise
    stay ``pending``, never ``failed``.
    RED on the job-keyed fix (both directions); the base's step-key
    keying never matched the substep's own key, so this pins the
    regression the job-key fix introduced."""
    for substep_status, expect_note in (
        ("succeeded", "the succeeded substep is the held claim's inner history"),
        ("failed", "the failed substep is that claim's inner history too"),
    ):
        flow = new_uuid()
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            job_id=flow,
            actor="flow",
            status="running",
            step_key="__flow__",
            metadata={"workflow": "probe_march"},
        )
        await _seed_held_step(
            wf_conn,
            wf_schema,
            flow,
            substep_status=substep_status,
        )
        await _assert_jobs(
            wf_conn,
            wf_schema,
            flow,
            {("publish", None, "pending", 1)},
        )
        await _assert_ledger(
            wf_conn,
            wf_schema,
            flow,
            {("publish", None, 1, "awaited"), ("before_publish", None, 1, substep_status)},
        )
        verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
        assert verdict == "pending", (
            f"the held step reconstructed {verdict!r} — {expect_note}; the "
            "held claim is live (its own ledger row is 'awaited', excluded "
            "from terminals) and the run is not terminal"
        )
        await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)


@pytest.mark.integration
async def test_reconstruction_held_loop_with_inner_iteration_stays_pending(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """HELD LOOP + an earlier iteration terminal at the same attempt. The
    loop row is pending under the loop hold's own shape (``kind: loop`` +
    the hold metadata), its own claim reads ``awaited``, and
    ``review.iter0`` — the iteration's OWN step key on the loop row's job
    id — is terminal. Iteration claims are not the loop node's claim:
    with the loop row the run's only non-root node, the verdict is
    EXACTLY ``pending``, never ``complete``, and the root stays running.
    The failed-iteration opposite direction must likewise stay ``pending``,
    never ``failed``. RED on the job-keyed fix (both directions)."""
    for iter_status in ("succeeded", "failed"):
        flow = new_uuid()
        loop_row = new_uuid()
        meta: dict[str, object] = {"workflow": "probe_march"}
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            job_id=flow,
            actor="flow",
            status="running",
            step_key="__flow__",
            metadata=meta,
        )
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            job_id=loop_row,
            status="pending",
            step_key="review",
            attempt=1,
            metadata={"kind": "loop", "hold": str(new_uuid()), "held_signal": "next"},
            fairness_key=str(flow),
        )
        await _seed_ledger_terminal(
            wf_conn,
            wf_schema,
            flow,
            job_id=loop_row,
            step_key="review",
            map_index=None,
            attempt=1,
            status="awaited",
        )
        await _seed_ledger_terminal(
            wf_conn,
            wf_schema,
            flow,
            job_id=loop_row,
            step_key="review.iter0",
            map_index=None,
            attempt=1,
            status=iter_status,
        )
        await _assert_jobs(
            wf_conn,
            wf_schema,
            flow,
            {("review", None, "pending", 1)},
        )
        await _assert_ledger(
            wf_conn,
            wf_schema,
            flow,
            {("review", None, 1, "awaited"), ("review.iter0", None, 1, iter_status)},
        )
        verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
        assert verdict == "pending", (
            f"the held loop reconstructed {verdict!r} — the iteration's "
            "terminal belongs to the iteration claim (its own step key), "
            "never to the loop node's own claim; the loop is held live and "
            "the run is not terminal"
        )
        await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)


@pytest.mark.integration
async def test_reconstruction_killed_child_with_substep_pending_then_blocked_with_join(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """KILLED/RECLAIMED child + an inner ``ctx.substep`` terminal. The
    child is pending at attempt 1, its OWN claim's ledger row still reads
    ``running`` (the holder died; the reclaim has not processed it), and
    the substep — its own step key on the child's job id — is terminal.
    ISOLATED (phase 1, the child the run's only non-root node): EXACTLY
    ``pending`` — a job-wide terminal map derives ``complete``/
    ``failed`` here. Then the genuine pending join-wait row is seeded
    (phase 2) and the aggregate reads ``blocked`` — never ``complete``/
    ``failed`` — with the root running throughout. RED on the job-keyed
    fix, isolated phase first, in both terminal directions."""
    for substep_status in ("succeeded", "failed"):
        flow = new_uuid()
        child = new_uuid()
        meta: dict[str, object] = {"workflow": "probe_march"}
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            job_id=flow,
            actor="flow",
            status="running",
            step_key="__flow__",
            metadata=meta,
        )
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            job_id=child,
            status="pending",
            step_key="fan.item",
            map_index=0,
            attempt=1,
            metadata=meta,
            fairness_key=str(flow),
        )
        await _seed_ledger_terminal(
            wf_conn,
            wf_schema,
            flow,
            job_id=child,
            step_key="fan.item",
            map_index=0,
            attempt=1,
            status="running",
        )
        await _seed_ledger_terminal(
            wf_conn,
            wf_schema,
            flow,
            job_id=child,
            step_key="fan.item.sub",
            map_index=0,
            attempt=1,
            status=substep_status,
        )
        await _assert_jobs(
            wf_conn,
            wf_schema,
            flow,
            {("fan.item", 0, "pending", 1)},
        )
        await _assert_ledger(
            wf_conn,
            wf_schema,
            flow,
            {("fan.item", 0, 1, "running"), ("fan.item.sub", 0, 1, substep_status)},
        )
        # Phase 1 — isolated: the killed child stays pending.
        isolated = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
        assert isolated == "pending", (
            f"the killed child reconstructed {isolated!r} — its claim's "
            "ledger row is 'running' (the reclaim's pending input) and the "
            "substep's terminal belongs to the SUBSTEP claim; the child is "
            "live and the run is not terminal"
        )
        await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)
        # Phase 2 — the genuine fan-in join, seeded only now.
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            status="pending",
            step_key="fan.join",
            deps_pending=1,
            metadata={**meta, "blocking_reason": "join"},
            fairness_key=str(flow),
        )
        joined = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
        assert joined == "blocked", (
            f"the fan-in phase derived {joined!r} — the join-wait row's own "
            f"blocked shape with the substep {substep_status!r} inner history"
        )
        await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)


@pytest.mark.integration
async def test_reconstruction_real_ladder_repend_promotion_stays_pending(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """REAL ladder re-pend promotion: the run seeds the node's claim (the
    real ``LEDGER_CLAIM_SQL``), invokes the real ``repend_attempt`` (the
    row scheduled at the UNCHANGED attempt + that claim's ledger terminal
    ``failed``), runs the real scheduled→pending sweep, and the promoted
    row reconstructs EXACTLY ``pending`` with the root running after the
    real maintenance. An older opposite-status terminal (the same job's
    substep claim, succeeded) is seeded so no implementation can pass by
    selecting a convenient historical row.
    RED on the base (the step-key collapse derived ``failed``), on the
    ``(job_id, attempt)`` fix, and on exact same-attempt tuple
    projection: the pending row at attempt N beside its own failed
    attempt-N terminal is a LIVE retry, not a terminal."""
    from taskq.backend._sweeps import sweep_scheduled_to_pending
    from taskq.workflows.api._attempt import AttemptClaim, repend_attempt

    flow = new_uuid()
    meta: dict[str, object] = {"workflow": "probe_march"}
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=flow,
        actor="flow",
        status="running",
        step_key="__flow__",
        metadata=meta,
    )
    node = await seed_running_node(wf_conn, wf_schema, flow, step_key="item")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, node)
    claimed = await wf_conn.fetchrow(
        wf_sql.ledger_claim, new_uuid(), flow, node, "item", None, attempt
    )
    assert claimed is not None
    claim = AttemptClaim(
        job_id=node,
        flow_id=flow,
        step_key="item",
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        ledger_id=claimed["id"],
        ledger_claimed_at=claimed["updated_at"],
        map_index=None,
    )
    disposition = await repend_attempt(
        wf_conn,
        wf_sql,
        claim=claim,
        error_class="ValueError",
        error_message="the ladder's transient failure",
        failures_before=0,
        max_attempts=3,
        retry_kind="transient",
        backoff_s=0.05,
    )
    assert disposition == "repended", f"the repend must apply: {disposition}"
    # The older opposite-status terminal: the same job's substep claim,
    # succeeded — a 'succeeded-first' selector must not pass either.
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow,
        job_id=node,
        step_key="item.sub",
        map_index=None,
        attempt=1,
        status="succeeded",
    )
    await _assert_jobs(
        wf_conn,
        wf_schema,
        flow,
        {("item", None, "scheduled", 1)},
    )
    await _assert_ledger(
        wf_conn,
        wf_schema,
        flow,
        {("item", None, 1, "failed"), ("item.sub", None, 1, "succeeded")},
    )
    # The real sweep at its real cadence: the row becomes due when its
    # (real) backoff elapses, so the sweep is re-run — the production
    # sweeper's own shape — until the promotion lands (bounded).
    promoted = 0
    async with asyncio.timeout(15):
        while promoted == 0:
            promoted = await sweep_scheduled_to_pending(wf_conn, schema=wf_schema)
            if promoted == 0:
                await asyncio.sleep(0.05)
    assert promoted == 1, f"the real sweep must promote the scheduled row: {promoted}"
    await _assert_jobs(
        wf_conn,
        wf_schema,
        flow,
        {("item", None, "pending", 1)},
    )
    verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
    assert verdict == "pending", (
        f"the promoted retry reconstructed {verdict!r} — the ladder's "
        "re-pend keeps the attempt ordinal, so the pending row at attempt "
        "1 beside its own failed attempt-1 terminal is a LIVE retry (the "
        "terminal and the re-pend commit atomically), never a terminal "
        "verdict"
    )
    await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)


@pytest.mark.integration
async def test_reconstruction_real_manual_retry_reopen_stays_pending(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """REAL manual retry/reopen: the run finalizes the node failed (the
    real ``finalize_node``), converges the root with the real maintenance
    (the run IS terminal-failed at that instant), then invokes the
    shipped ``retry_workflow_node`` (the node CAS + the root reopen —
    ``failed → pending`` at the UNCHANGED attempt, the root's
    ``run_epoch`` advanced). The re-armed row reconstructs EXACTLY
    ``pending`` with the root running; the pre-retry failed ledger
    terminal is attempt history that must never win. An older
    opposite-status terminal (the same job's substep claim, succeeded) is
    seeded so no convenient-historical-row selector can pass.
    RED on the base, the ``(job_id, attempt)`` fix, and exact same-attempt
    tuple projection."""
    from taskq.workflows.api._runner_exit import retry_workflow_node

    flow = new_uuid()
    meta: dict[str, object] = {"workflow": "probe_march"}
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow,
        job_id=flow,
        actor="flow",
        status="running",
        step_key="__flow__",
        metadata=meta,
    )
    node = await seed_running_node(wf_conn, wf_schema, flow, step_key="triage")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, node)
    # The claim's own ledger row (the real claim write — the terminal's
    # by-attempt arbiter row must exist for the finalize to record into).
    claimed = await wf_conn.fetchrow(
        wf_sql.ledger_claim, new_uuid(), flow, node, "triage", None, attempt
    )
    assert claimed is not None
    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow,
        job_id=node,
        step_key="triage",
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="failed",
        error_class="ValueError",
        error_message="the first attempt failed",
    )
    # The older opposite-status terminal: the same job's substep claim,
    # succeeded — a 'succeeded-first' selector must not pass either.
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow,
        job_id=node,
        step_key="triage.sub",
        map_index=None,
        attempt=1,
        status="succeeded",
    )
    await _assert_jobs(wf_conn, wf_schema, flow, {("triage", None, "failed", 1)})
    await _assert_ledger(
        wf_conn,
        wf_schema,
        flow,
        {("triage", None, 1, "failed"), ("triage.sub", None, 1, "succeeded")},
    )
    await wf_conn.execute(wf_sql.workflow_root_maintain, 100)
    pre_root = await wf_conn.fetchrow(
        f'''SELECT status::text AS status, run_epoch FROM "{wf_schema}".jobs
            WHERE id = $1''',
        flow,
    )
    assert pre_root is not None and pre_root["status"] == "failed", (
        f"the genuinely terminal run must converge first: {dict(pre_root) if pre_root else None}"
    )
    pre_verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
    assert pre_verdict == "failed", (
        f"the pre-retry rows reconstructed {pre_verdict!r} — the failed "
        "node row IS the terminal fact at this instant"
    )
    reopened = await retry_workflow_node(
        module_pg_pool,
        schema=wf_schema,
        flow_id=flow,
        node_key="triage",
        reason="the operator's manual resume",
    )
    assert reopened == 1, f"the node CAS must re-arm exactly the one row: {reopened}"
    node_post = await wf_conn.fetchrow(
        f'''SELECT status::text AS status, attempt FROM "{wf_schema}".jobs
            WHERE id = $1''',
        node,
    )
    assert node_post is not None and node_post["status"] == "pending" and (
        node_post["attempt"] == attempt
    ), (
        "the re-armed row must be pending at the UNCHANGED attempt: "
        f"{dict(node_post) if node_post else None}"
    )
    root_post = await wf_conn.fetchrow(
        f'''SELECT status::text AS status, run_epoch FROM "{wf_schema}".jobs
            WHERE id = $1''',
        flow,
    )
    assert root_post is not None and root_post["status"] == "running", (
        f"the root reopen must re-open the run: {dict(root_post) if root_post else None}"
    )
    assert root_post is not None and (root_post["run_epoch"] or 0) == 1, (
        f"the root reopen must advance run_epoch: {dict(root_post) if root_post else None}"
    )
    await _assert_ledger(
        wf_conn,
        wf_schema,
        flow,
        {("triage", None, 1, "failed"), ("triage.sub", None, 1, "succeeded")},
    )
    verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
    assert verdict == "pending", (
        f"the manually re-armed run reconstructed {verdict!r} — the "
        "pre-retry failed terminal is attempt HISTORY (the retry keeps the "
        "attempt ordinal; there is no ledger field distinguishing this "
        "live shape from a fictitious split commit), never a terminal "
        "verdict"
    )
    await _assert_root_running(wf_conn, wf_schema, wf_sql, flow)


@pytest.mark.integration
async def test_reconstruction_terminal_node_rows_win_over_ledger_history(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """ALREADY-TERMINAL direct facts: every terminal node status
    (failed, succeeded, cancelled, crashed, abandoned) reconstructs from
    the NODE ROW — with the ledger MISSING, STALE (an older attempt's
    opposite-status terminal), or CONTRADICTORY (a same-attempt terminal
    of the opposite class) — and the real maintenance converges the root
    the same way. Deleting the overlay must not discard the two-source
    tests' intended terminal coverage: the current node row is the
    record."""
    shapes: dict[str, tuple[str, set[tuple[str, int | None, int, str]]]] = {
        # status -> (the folded verdict, the ledger tuples to seed)
        "failed": (
            "failed",
            {("t", None, 1, "succeeded")},  # contradictory same-attempt
        ),
        "succeeded": (
            "complete",
            {("t", None, 1, "failed")},  # contradictory same-attempt
        ),
        "cancelled": (
            "cancelled",
            {("t", None, 1, "succeeded")},  # contradictory
        ),
        "crashed": (
            "failed",
            {("t", None, 1, "succeeded")},  # contradictory (the fold)
        ),
        "abandoned": (
            "failed",
            {("t", None, 1, "failed")},  # contradictory (the fold agrees)
        ),
    }
    for status, (expected, ledger) in shapes.items():
        # MISSING ledger.
        flow = new_uuid()
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            job_id=flow,
            actor="flow",
            status="running",
            step_key="__flow__",
            metadata={"workflow": "probe_march"},
        )
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow,
            status=status,
            step_key="t",
            attempt=1,
            metadata={"workflow": "probe_march"},
            fairness_key=str(flow),
        )
        await _assert_jobs(wf_conn, wf_schema, flow, {("t", None, status, 1)})
        await _assert_ledger(wf_conn, wf_schema, flow, set())
        verdict = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))
        assert verdict == expected, (
            f"the terminal node row ({status}) with NO ledger must "
            f"reconstruct {expected!r}, got {verdict!r}"
        )
        # CONTRADICTORY ledger (a same-attempt terminal of the opposite
        # class — the corrupt shape the node row must survive).
        flow2 = new_uuid()
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow2,
            job_id=flow2,
            actor="flow",
            status="running",
            step_key="__flow__",
            metadata={"workflow": "probe_march"},
        )
        await _seed_wf_row(
            wf_conn,
            wf_schema,
            flow2,
            status=status,
            step_key="t",
            attempt=1,
            metadata={"workflow": "probe_march"},
            fairness_key=str(flow2),
        )
        for step_key, map_index, attempt_n, ledger_status in ledger:
            await _seed_ledger_terminal(
                wf_conn,
                wf_schema,
                flow2,
                job_id=new_uuid(),
                step_key=step_key,
                map_index=map_index,
                attempt=attempt_n,
                status=ledger_status,
            )
        await _assert_jobs(wf_conn, wf_schema, flow2, {("t", None, status, 1)})
        await _assert_ledger(wf_conn, wf_schema, flow2, ledger)
        verdict2 = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow2)))
        assert verdict2 == expected, (
            f"the terminal node row ({status}) must WIN over the "
            f"contradictory ledger history: expected {expected!r}, got {verdict2!r}"
        )

    # STALE ledger: a failed node at attempt 2 with an OLDER succeeded
    # attempt-1 terminal (history) — the current row wins, maintenance
    # converges.
    flow3 = new_uuid()
    stale_node = new_uuid()
    meta: dict[str, object] = {"workflow": "probe_march"}
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow3,
        job_id=flow3,
        actor="flow",
        status="running",
        step_key="__flow__",
        metadata=meta,
    )
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        flow3,
        job_id=stale_node,
        status="failed",
        step_key="t",
        attempt=2,
        metadata=meta,
        fairness_key=str(flow3),
    )
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow3,
        job_id=stale_node,
        step_key="t",
        map_index=None,
        attempt=1,
        status="succeeded",
    )
    await _seed_ledger_terminal(
        wf_conn,
        wf_schema,
        flow3,
        job_id=stale_node,
        step_key="t",
        map_index=None,
        attempt=2,
        status="failed",
    )
    await _assert_jobs(wf_conn, wf_schema, flow3, {("t", None, "failed", 2)})
    await _assert_ledger(
        wf_conn,
        wf_schema,
        flow3,
        {("t", None, 1, "succeeded"), ("t", None, 2, "failed")},
    )
    verdict3 = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow3)))
    assert verdict3 == "failed", (
        f"the failed node row at attempt 2 reconstructed {verdict3!r} — "
        "the older succeeded terminal is history"
    )
    await wf_conn.execute(wf_sql.workflow_root_maintain, 100)
    root3 = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow3
    )
    assert root3 == "failed", f"the terminal run must converge: {root3!r}"


@pytest.mark.integration
async def test_reconstruction_is_byte_identical_under_ledger_heap_motion_and_analyze(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """The ordering concept is GONE and harmless: reconstruction consults
    no ledger row, so moving the ledger's heap tuples and re-analyzing
    cannot move the verdict. Representative LIVE states (the held step
    with an inner substep terminal; the real ladder's promoted retry) are
    sampled before, after a physical tuple move, and after ``ANALYZE`` —
    the three samples must be BYTE-IDENTICAL and exactly the pinned live
    verdicts. (There is no terminal-fetch query and no ORDER BY left to
    remove; a reintroduced overlay fails the query-count pin's inventory
    and the live-node controls.)"""
    from taskq.backend._sweeps import sweep_scheduled_to_pending
    from taskq.workflows.api._attempt import AttemptClaim, repend_attempt

    # Live shape 1: the held step with a succeeded inner substep.
    held_flow = new_uuid()
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        held_flow,
        job_id=held_flow,
        actor="flow",
        status="running",
        step_key="__flow__",
        metadata={"workflow": "probe_march"},
    )
    await _seed_held_step(wf_conn, wf_schema, held_flow, substep_status="succeeded")

    # Live shape 2: the real ladder's promoted retry (pending at the same
    # attempt as its own failed terminal).
    retry_flow = new_uuid()
    await _seed_wf_row(
        wf_conn,
        wf_schema,
        retry_flow,
        job_id=retry_flow,
        actor="flow",
        status="running",
        step_key="__flow__",
        metadata={"workflow": "probe_march"},
    )
    node = await seed_running_node(wf_conn, wf_schema, retry_flow, step_key="item")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, node)
    claimed = await wf_conn.fetchrow(
        wf_sql.ledger_claim, new_uuid(), retry_flow, node, "item", None, attempt
    )
    assert claimed is not None
    await repend_attempt(
        wf_conn,
        wf_sql,
        claim=AttemptClaim(
            job_id=node,
            flow_id=retry_flow,
            step_key="item",
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            ledger_id=claimed["id"],
            ledger_claimed_at=claimed["updated_at"],
            map_index=None,
        ),
        error_class="ValueError",
        error_message="the ladder's transient failure",
        failures_before=0,
        max_attempts=3,
        retry_kind="transient",
        backoff_s=0.05,
    )
    promoted = 0
    async with asyncio.timeout(15):
        while promoted == 0:
            promoted = await sweep_scheduled_to_pending(wf_conn, schema=wf_schema)
            if promoted == 0:
                await asyncio.sleep(0.05)
    assert promoted == 1, promoted

    async def sample(flow: uuid.UUID) -> WorkflowStatus:
        return await reconstruct_workflow_status(wf_conn, wf_sql, JobId(str(flow)))

    for flow, expected in ((held_flow, "pending"), (retry_flow, "pending")):
        first = await sample(flow)
        # The ledger's tuples move to the heap's end (the physical
        # arrival order reverses).
        await wf_conn.execute(
            f'''UPDATE "{wf_schema}".wf_step_ledger SET updated_at = clock_timestamp()
                WHERE flow_id = $1::uuid''',
            flow,
        )
        second = await sample(flow)
        await wf_conn.execute(f'ANALYZE "{wf_schema}".wf_step_ledger')
        third = await sample(flow)
        assert first == second == third == expected, (
            f"the verdict MOVED under ledger heap motion + ANALYZE "
            f"({first!r} → {second!r} → {third!r}) — reconstruction must "
            "not consult ledger history at all"
        )
