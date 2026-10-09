"""THE DEPLOY MATRIX (T15 — §22's cells as system-tier scenarios).

The workflow variant of the rolling-fleet harness's contract (see
``test_rolling_release_fleet.py``'s header): real worker subprocesses on
one real Postgres; every timing assertion derived from the scenario's
own knobs (the lock lease, the sweep interval, the poll floor) with the
MEASURED value printed next to the bound; the shared invariants close
every scenario (``_invariants.assert_balanced``) plus the workflow
invariants (the join fires exactly once, the ledger reconciles).

The cells (the operator table's rows, docs/guides/deployment.md):

1. WORKER DEPLOY — a node mid-flight on a SIGKILLed worker: the
   lease/reclaim machinery re-pends it (``lock_expired``); the join
   counter never moved; a surviving pod finishes the work; the per-
   attempt ``code_version`` record rides the claim (§22.1's mixed-version
   record).
2. SCHEMA MIGRATION MID-FLIGHT — a held migration lock during a
   finalize: the transient classification ladders, the node succeeds,
   no state corruption (the sweep repairs the post-commit-pre-decrement
   window) — the injection rides the REAL held-lock drill, never a
   bespoke fault hack.
3. ROLLBACK — a worker that cannot execute workflow bodies (the vanilla
   entry: the v1 deployment) meets a v2 fleet: the dispatch fence never
   hands it a workflow row (no crash loop, no claim, the pod serves
   vanilla work); the additive-only ledger makes the vanilla rollback
   always safe — the row shapes the old code meets are its OWN.
4. OUTAGE — the broker dies mid-map: the run stalls, recovers, the join
   fires exactly once (the finalize transactions are PG-local).
5. CONFIG DRIFT — a downstream node re-routed to a queue nothing
   serves: the run stays LIVE (blocked is a live state), the row stays
   claimable, no crash, no silent orphan; the alert the fleet ships
   (``TaskQQueueUnserved``) is the operator's next step, named.
6. REDISPATCH OWNERSHIP — the four rows (crashed worker → the reclaim;
   failed node → the ladder; held node → the signal/timeout) each fire
   for its failure and none of the others double-fires.
7. CRON x WORKFLOW — the cron slot firing twice (clock edge, operator
   re-trigger) yields ONE run (the run-key arbiter's composition, G3).
8. SIGTERM CLEAN-DRAIN — a worker receiving SIGTERM mid-map-child /
   mid-loop-iteration finishes-or-requeues per the termination grace;
   the loop's carry advances exactly once across the requeue (F7).
9. RETENTION MID-FLIGHT — a retention pass running mid-march does not
   prune a live run's parents or expire a joined map's children's
   results before the join fires (ticket 18's liveness guard at fleet
   scale).

The RED-FIRST defect drills (the ticket's seven) live in
``test_wf_matrix_red_drills.py`` — each mutated variant reds the
corresponding pin; these scenarios are their GREEN faces.
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from the settings boundary the fixtures validated; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from taskq.workflows import FlowRunner
from taskq.workflows.ledger import run_idempotency_scope
from tests.system_e2e._harness import (
    LOCK_LEASE_S,
    SWEEP_INTERVAL_S,
    TERMINATION_GRACE_S,
    TIER_LOAD_STRETCH,
    WorkerProc,
    reap,
    spawn_worker,
    wait_worker_ready,
)
from tests.system_e2e._invariants import assert_balanced
from tests.system_e2e._wf_app import (
    MARCH_FLOWS,
    WF_QUEUE,
)
from tests.system_e2e._wf_harness import (
    MARCH_LEADER_LEASE_S,
    MARCH_SETTLE_BOUND_S,
    RECLAIM_BOUND_S,
    join_fires,
    spawn_wf_fleet,
    tag_run_rows,
    wait_flow_terminal,
)

pytestmark = [pytest.mark.system, pytest.mark.integration]

#: The test-side tag: every row the scenarios create carries it, so the
#: invariants' population is this module's own (the tier's convention).
_TAG = "wf-matrix"

#: The drift cell's hang guard: the unserved node NEVER resolves, so the
#: cell's observation is a bounded NON-wait (the row's pending shape is
#: read once the fleet has had one full dispatch round to serve it) —
#: the bound prices that round, stretched.
_DRIFT_OBSERVE_BOUND_S = (LOCK_LEASE_S + SWEEP_INTERVAL_S + 5.0) * TIER_LOAD_STRETCH

#: The drain cell's requeue bound: the grace (the drain's own budget) +
#: one poll + the reclaim sweep, stretched — a requeue slower than this
#: is a lost-iteration defect, not runner weather.
_DRAIN_REQUEUE_BOUND_S = (TERMINATION_GRACE_S + LOCK_LEASE_S + SWEEP_INTERVAL_S + 2.0) * (
    TIER_LOAD_STRETCH
)


@pytest_asyncio.fixture(scope="module")
async def matrix_pool(pg_dsn: str, module_pg_schema: Any) -> AsyncGenerator[asyncpg.Pool, None]:
    """The module's test-process pool (the march's own client)."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    yield pool
    await pool.close()


async def _create_run(
    pool: asyncpg.Pool, schema: str, flow_name: str, *, run_key: str | None = None
) -> str:
    runner = FlowRunner(MARCH_FLOWS[flow_name], pool, schema)  # pyright: ignore[reportArgumentType]  # Why: the compiled graph's module identity is the march app's; the runner accepts the CompiledWorkflow face.
    flow_id = await runner.create_flow(run_key=run_key)
    return str(flow_id)


async def _tagged_run(
    pool: asyncpg.Pool, schema: str, flow_name: str, *, run_key: str | None = None
) -> str:
    """Create + tag in one step (every scenario's entry)."""
    flow_id = await _create_run(pool, schema, flow_name, run_key=run_key)
    async with pool.acquire() as conn:
        await conn.execute(
            f"""UPDATE "{schema}".jobs
                SET tags = tags || ARRAY[$2::text]
                WHERE (metadata->>'flow_id')::uuid = $1::uuid""",
            flow_id,
            _TAG,
        )
    return flow_id


# ── Cell 1: WORKER DEPLOY — the SIGKILLed node, the reclaim, the join ────


@pytest.mark.timeout(600)
async def test_worker_deploy_cell_reclaims_a_killed_node_and_the_join_never_moves(
    matrix_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """§22.1: a node job mid-flight on a killed worker is re-pended by
    the lease/reclaim machinery (``lock_expired``); the join counter
    never moved while the row was owned by the corpse; a surviving pod
    finishes the work; the per-attempt ``code_version`` record rides
    BOTH claims (§22.1's per-attempt record)."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["w1", "w2"])
        flow_id = await _tagged_run(matrix_pool, schema, "reclaim_target")

        # THE PREMISE: the long node is CLAIMED by some pod (the fence
        # hands it to a capable worker). The claim is near-instant on a
        # healthy fleet; the bound prices one poll round, stretched.
        claim_bound = (LOCK_LEASE_S + SWEEP_INTERVAL_S + 5.0) * TIER_LOAD_STRETCH
        start = time.monotonic()
        row: dict[str, Any] | None = None
        while time.monotonic() - start < claim_bound:
            rows = await conn.fetch(
                f"""SELECT id::text, status::text AS status, attempt, locked_by_worker::text AS holder
                    FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'long'""",
                flow_id,
            )
            if rows and rows[0]["status"] == "running":
                row = dict(rows[0])
                break
            await asyncio.sleep(0.2)
        assert row is not None, (
            f"the long node was never claimed within {claim_bound}s — the dispatch fence "
            "never handed the flow row to the capable fleet"
        )
        print(f"[cell-1] claim: measured={time.monotonic() - start:.2f}s bound={claim_bound}s")

        # THE CHAOS: SIGKILL the pod that holds the row (the crash-injection
        # fixture's shape — the corpse owns a running row).
        victim = None
        holder_pid = await conn.fetchval(
            f'SELECT pid FROM "{schema}".workers WHERE id = $1::uuid', row["holder"]
        )
        for name, pod in fleet.items():
            if pod.proc.pid == holder_pid:
                victim = name
                break
        assert victim is not None, (
            f"no fleet pod holds the row (holder={row['holder']}, pids="
            f"{[(n, p.proc.pid) for n, p in fleet.items()]})"
        )
        fleet[victim].proc.send_signal(signal.SIGKILL)
        reap(fleet[victim])
        del fleet[victim]
        print(f"[cell-1] SIGKILL -> {victim}")

        # THE PIN: the join counter NEVER moved while the row was a
        # corpse's — `after` (the downstream join-wait row) keeps
        # deps_pending = 1 (the counter is decremented by a FINALIZE, and
        # the corpse's finalize was fenced out).
        after = await conn.fetchrow(
            f"""SELECT deps_pending, status::text AS status, metadata->>'blocking_reason' AS blocking
                FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'after'""",
            flow_id,
        )
        assert after is not None and after["deps_pending"] == 1, (
            f"the join counter moved while its parent was a corpse's running row: {after}"
        )

        # THE RECLAIM: the lapsed lease re-pends the row (lock_expired);
        # a surviving pod re-claims; the run terminalizes. The bound: the
        # lease + the sweep + one poll, stretched.
        status, measured = await wait_flow_terminal(conn, schema, flow_id, bound_s=RECLAIM_BOUND_S)
        print(f"[cell-1] terminal: measured={measured:.2f}s bound={RECLAIM_BOUND_S}s")
        assert status == "complete", f"the reclaimed run derived {status!r}, not complete"

        # THE PER-ATTEMPT RECORD: the §22.1 record lives on the ROW
        # (T03's code_version, written at claim — each attempt's claim
        # rewrites it; the attempt LEDGER is the history). The run
        # terminalized, so the FINAL attempt's record is stamped; the
        # ledger shows the crashed attempt AND the retry.
        ledger = await conn.fetch(
            f"""SELECT a.attempt, a.error_class FROM "{schema}".job_attempts a
                JOIN "{schema}".jobs j ON j.id = a.job_id
                WHERE j.metadata->>'flow_id' = $1
                ORDER BY a.attempt""",
            flow_id,
        )
        assert len(ledger) >= 2, (
            f"the crashed node's ledger shows {len(ledger)} attempts — the reclaim never re-ran it"
        )
        stamped = await conn.fetchval(
            f"""SELECT code_version FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'long'""",
            flow_id,
        )
        assert stamped, "the claimed node carries no code_version record (§22.1's record)"

        # THE INVARIANTS close the cell.
        await tag_run_rows(matrix_pool, schema, flow_id, _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
        fires = await join_fires(conn, schema, flow_id)
        assert all(f["fires"] == 1 for f in fires), f"the join fired more than once: {fires}"
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()


# ── Cell 2: SCHEMA MIGRATION MID-FLIGHT — the held lock, the ladder ─────


@pytest.mark.timeout(600)
async def test_schema_migration_midflight_cell_the_ladder_heals_the_held_lock(
    matrix_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """§22.2: a held migration lock (a REAL session holding ACCESS
    EXCLUSIVE on jobs) during a finalize → the transient classification
    (``LockNotAvailable``) → the ladder retries → succeeds; no state
    corruption, no orphaned counter decrement. The injection rides the
    real held-migration-lock drill (a second session's ``LOCK TABLE``),
    never a bespoke fault hack."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    lock_session: asyncpg.Connection | None = None
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["m1", "m2"])
        flow_id = await _tagged_run(matrix_pool, schema, "doc_ingest_march")

        # Wait until the map's children are live (the fleet is executing),
        # then hold the migration lock (the DEPLOY window: the migration's
        # ACCESS EXCLUSIVE on jobs) for a bounded stretch — shorter than
        # the row's ladder, so the finalize's transient survives it.
        settle = await wait_for_map_children(conn, schema, flow_id)
        print(f"[cell-2] map children live: measured={settle:.2f}s")

        lock_session = await asyncpg.connect(module_pg_schema.pg_dsn)
        await lock_session.execute(
            f'SET lock_timeout = 0; BEGIN; LOCK TABLE "{schema}".jobs IN ACCESS EXCLUSIVE MODE'
        )
        hold_s = 3.0
        await asyncio.sleep(hold_s)
        await lock_session.execute("COMMIT")
        print(f"[cell-2] migration lock held {hold_s}s (the deploy window)")

        status, measured = await wait_flow_terminal(conn, schema, flow_id)
        print(f"[cell-2] terminal: measured={measured:.2f}s bound={MARCH_SETTLE_BOUND_S}s")
        assert status == "complete", f"the run derived {status!r} under the deploy window"

        # NO STATE CORRUPTION: every join fired exactly once, the ledger
        # reconciles, the invariants balance.
        fires = await join_fires(conn, schema, flow_id)
        assert all(f["fires"] == 1 for f in fires), f"double-fired join under the window: {fires}"
        await tag_run_rows(matrix_pool, schema, flow_id, _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
    finally:
        if lock_session is not None:
            with contextlib.suppress(Exception):
                await lock_session.execute("ROLLBACK")
                await lock_session.close()
        for pod in fleet.values():
            reap(pod)
        await conn.close()


async def wait_for_map_children(conn: asyncpg.Connection, schema: str, flow_id: str) -> float:
    """Wait until the run's map children exist (the fork fired) — the
    migration window must land on LIVE work."""
    start = time.monotonic()
    while time.monotonic() - start < MARCH_SETTLE_BOUND_S:
        n = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND map_index IS NOT NULL""",
            flow_id,
        )
        if n:
            return time.monotonic() - start
        await asyncio.sleep(0.2)
    raise AssertionError("the map's fork never fired")


# ── Cell 3: ROLLBACK — the v1 pod meets the v2 fleet ─────────────────────


@pytest.mark.timeout(600)
async def test_rollback_cell_the_v1_pod_never_claims_a_flow_row(
    matrix_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """§22.3: a v1 worker (the VANILLA entry — no workflow capability)
    meets a v2 fleet mid-run: the dispatch fence NEVER hands it a
    workflow row (no crash loop — the pod claims nothing it cannot
    execute); the capable fleet finishes the run; the additive-only
    ledger means the vanilla rollback always safe — the v1 pod's own
    writes are row shapes it knows (tags, statuses), never the wf
    columns."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    v1: WorkerProc | None = None
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["v2a"])
        # THE ROLLBACK'S POD: the vanilla entry — the workers row carries
        # NO workflow_execution stamp (the metadata key is stamped false,
        # the fence's data leg).
        v1 = spawn_worker(
            module_pg_schema.pg_dsn,
            schema,
            tag="wf-v1pod",
            extra_env={
                "TASKQ_LEADER_LEASE": str(MARCH_LEADER_LEASE_S),
                "TASKQ_QUEUES": "system_e2e,default",
            },
        )
        wait_worker_ready(v1)

        flow_id = await _tagged_run(matrix_pool, schema, "reclaim_target")
        start = time.monotonic()
        holder: str | None = None
        while time.monotonic() - start < _DRIFT_OBSERVE_BOUND_S:
            holder = await conn.fetchval(
                f"""SELECT locked_by_worker::text FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid
                      AND step_key = 'long' AND status = 'running'""",
                flow_id,
            )
            if holder:
                break
            await asyncio.sleep(0.2)
        assert holder is not None, "the run never got claimed by the capable fleet"

        # THE FENCE'S PIN: the v1 pod NEVER claims a workflow row —
        # its worker row's metadata says workflow_execution=false, and
        # every row the fence hands the fleet lands on a capable worker.
        v1_row = await conn.fetchrow(
            f"""SELECT metadata->>'workflow_execution' AS capable, id::text
                FROM "{schema}".workers WHERE pid = $1""",
            v1.proc.pid,
        )
        assert v1_row is not None, "the v1 pod never registered"
        assert v1_row["capable"] == "false", (
            f"the vanilla pod stamped {v1_row['capable']!r} — the fence's data leg is broken"
        )
        holder_capable = await conn.fetchval(
            f"SELECT metadata->>'workflow_execution' FROM \"{schema}\".workers WHERE id = $1::uuid",
            holder,
        )
        assert holder_capable == "true", (
            f"a workflow row was handed to a worker stamped {holder_capable!r} — "
            "the dispatch fence failed closed"
        )

        status, measured = await wait_flow_terminal(conn, schema, flow_id)
        print(f"[cell-3] terminal: measured={measured:.2f}s bound={MARCH_SETTLE_BOUND_S}s")
        assert status == "complete", f"the mixed-version run derived {status!r}"

        # THE MIXED-VERSION LEDGER: both pods' attempts in the ledger —
        # the v2 records carry code_version (the §22.1 pins).
        await tag_run_rows(matrix_pool, schema, flow_id, _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
        # The v1 pod is ALIVE and served vanilla work: no crash loop.
        assert v1.proc.poll() is None, "the v1 pod crashed under the mixed fleet"
    finally:
        if v1 is not None:
            reap(v1)
        for pod in fleet.values():
            reap(pod)
        await conn.close()


# ── Cell 5: CONFIG DRIFT — the unserved queue is named, not orphaned ────


@pytest.mark.timeout(600)
async def test_config_drift_cell_the_unserved_queue_is_named_not_orphaned(
    matrix_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """§22.5: an in-flight run whose downstream node is re-routed to a
    REMOVED queue: the node enters the blocked-with-reason state the
    derivation renders, the row stays claimable (never a crash, never a
    silent orphan), and the shipped alert's confirming read
    (``TaskQQueueUnserved``: depth > 0 with zero live workers) names the
    queue — the operator's next step is NAMED."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["d1"])
        flow_id = await _tagged_run(matrix_pool, schema, "drift_target")

        # The drift: the run goes live, then the operator re-routes the
        # downstream node's queue (the UPDATE is the config change the
        # deploy drilled) — no worker serves ``nowhere_queue``.
        start = time.monotonic()
        while time.monotonic() - start < _DRIFT_OBSERVE_BOUND_S:
            pending = await conn.fetchval(
                f"""SELECT count(*) FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid
                      AND queue = 'nowhere_queue' AND status = 'pending'""",
                flow_id,
            )
            if pending:
                break
            await asyncio.sleep(0.2)

        # THE PIN: the node is pending ON the unserved queue (claimable,
        # zero live workers on its queue) — the alert's exact join.
        drift = await conn.fetchrow(
            f"""SELECT status::text AS status, scheduled_at, queue
                FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'gone'""",
            flow_id,
        )
        assert drift is not None, "the drift node never existed"
        assert drift["status"] in ("pending", "scheduled"), (
            f"the drifted node reached {drift['status']!r} with no worker serving it — "
            "something claims work no pod can run"
        )
        live_workers = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".workers
                WHERE $1::text = ANY(
                    SELECT jsonb_array_elements_text(metadata->'queues'))""",
            WF_QUEUE,  # the fleet's queues — never nowhere_queue
        )
        depth = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".jobs
                WHERE queue = 'nowhere_queue' AND status IN ('pending', 'scheduled')""",
        )
        assert depth > 0 and live_workers == 0, (
            f"the TaskQQueueUnserved join must hold: depth={depth}, live_workers={live_workers}"
        )

        # THE CONSERVATION: no crash, no orphan — the population is whole
        # (the run stays live; blocked/pending is a live state). The
        # settle is OFF: this cell's run NEVER terminalizes (the unserved
        # node is the design), the LIVE-run variant closes conservation
        # + the audit trail only.
        await tag_run_rows(matrix_pool, schema, flow_id, _TAG)
        await assert_balanced(sys_ledger, schema, _TAG, settle=False)
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()


# ── Cell 7: CRON x WORKFLOW — the slot twice, ONE run (G3) ───────────────


@pytest.mark.timeout(600)
async def test_cron_slot_fires_twice_one_run(
    matrix_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """G3's system-tier pin: the cron entry's slot fires twice (a clock
    edge, an operator re-trigger) → ONE run — the run-level arbiter
    composes with the cron-slot key. The variant regimes RACE (the two
    creates run concurrently); one row is born."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["c1"])
        slot = f"deep-research:{datetime.now(UTC).strftime('%Y%m%dT%H%M')}"
        scope = run_idempotency_scope("deep_research")
        print(f"[cell-7] slot={slot} scope={scope}")

        # THE DOUBLE FIRE: two concurrent creations, same slot key.
        first, second = await asyncio.gather(
            _tagged_run(matrix_pool, schema, "deep_research", run_key=slot),
            _tagged_run(matrix_pool, schema, "deep_research", run_key=slot),
        )
        assert first == second, (
            f"the double-fired slot yielded TWO runs ({first}, {second}) — "
            "the arbiter lost the race"
        )

        # The second creation inserted NO second root (one row is born).
        roots = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = '__flow__'""",
            first,
        )
        assert roots == 1, f"{roots} roots for one slot — the arbiter duplicated the run"

        # And the run LIVES: the fleet executes it to terminal (the
        # holds need decisions — resolve them all as they appear).
        resolver = asyncio.create_task(_resolve_holds(matrix_pool, schema, first))
        status, measured = await wait_flow_terminal(conn, schema, first)
        resolver.cancel()
        print(f"[cell-7] terminal: measured={measured:.2f}s bound={MARCH_SETTLE_BOUND_S * 2}s")
        assert status == "complete", f"the run derived {status!r}"
        await tag_run_rows(matrix_pool, schema, first, _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()


async def _resolve_holds(pool: asyncpg.Pool, schema: str, flow_id: str) -> None:
    """Resolve every hold as it appears (approve) — the marches' human."""
    from taskq.workflows.api._hitl import HitlClient

    client = HitlClient(pool, schema=schema)
    seen: set[str] = set()
    while True:
        holds = await client.list(flow_id)
        for hold in holds:
            if hold.hold_id in seen or hold.status != "held":
                continue
            seen.add(hold.hold_id)
            await client.resolve(
                hold.hold_id,
                {"verdict": "approve", "note": "the march's operator"},
                principal="matrix-test",
            )
        await asyncio.sleep(0.3)


# ── Cell 8: SIGTERM CLEAN-DRAIN — the carry advances exactly once (F7) ──


@pytest.mark.timeout(600)
async def test_sigterm_drain_cell_mid_loop_the_carry_advances_exactly_once(
    matrix_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """F7: a worker receiving SIGTERM MID-LOOP-ITERATION must
    finish-or-requeue per the termination grace; the loop's carry
    advances EXACTLY ONCE on the requeue (no double-apply, no lost
    iteration) — P3 rule 3's shape under drain."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["dr1", "dr2"])
        flow_id = await _tagged_run(matrix_pool, schema, "drain_loop")

        # THE DRAIN: once the loop node is claimed, SIGTERM the holder —
        # the graceful shutdown requeues the mid-flight iteration (the
        # interruption is not an execution outcome), and the surviving
        # pod picks it up.
        start = time.monotonic()
        holder_pid: int | None = None
        while time.monotonic() - start < _DRAIN_REQUEUE_BOUND_S:
            holder_pid = await conn.fetchval(
                f"""SELECT w.pid
                    FROM "{schema}".jobs j
                    JOIN "{schema}".workers w ON w.id = j.locked_by_worker
                    WHERE (j.metadata->>'flow_id')::uuid = $1::uuid
                      AND j.step_key = 'carry_loop' AND j.status = 'running'""",
                flow_id,
            )
            if holder_pid:
                break
            await asyncio.sleep(0.1)
        assert holder_pid is not None, "the loop node never claimed"
        for name, pod in fleet.items():
            if pod.proc.pid == holder_pid:
                from tests.system_e2e._harness import graceful_stop

                rc = graceful_stop(pod, timeout=TERMINATION_GRACE_S + 10)
                print(f"[cell-8] drained {name} rc={rc}")
                del fleet[name]
                break

        status, measured = await wait_flow_terminal(
            conn, schema, flow_id, bound_s=MARCH_SETTLE_BOUND_S * 2
        )
        print(f"[cell-8] terminal: measured={measured:.2f}s")
        assert status == "complete", f"the drained run derived {status!r}"

        # THE CARRY ADVANCED EXACTLY ONCE PER ITERATION: the loop's
        # ITERATION rows are the timeline (the loop node's own row is the
        # NODE, its `.iterN` rows are the iterations — the node row is
        # not an iteration). The APPLIED record is the terminal one: a
        # fenced row is the corpse's claim record (the drain's own
        # interruption is not an execution outcome — the F7 doctrine),
        # so the pin counts the SUCCEEDED records: 3 iterations
        # (carry 0,1,2 → Done at 2), each applied EXACTLY ONCE (no
        # double-applied carry, no lost one — the resume's memo replay
        # never re-ran an applied body).
        iterations = await conn.fetch(
            f"""SELECT step_key, count(*)::int AS rows
                FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1::uuid AND step_key LIKE 'carry_loop.iter%'
                  AND status = 'succeeded'
                GROUP BY step_key""",
            flow_id,
        )
        iters = {r["step_key"]: r["rows"] for r in iterations}
        for key, count in iters.items():
            assert count == 1, (
                f"iteration {key} has {count} ledger rows — the carry double-applied "
                "(or the drain lost and re-ran it)"
            )
        assert len(iters) == 3, f"the loop ran {len(iters)} iterations, expected 3: {iters}"
        await tag_run_rows(matrix_pool, schema, flow_id, _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
