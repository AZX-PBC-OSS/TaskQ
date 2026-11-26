"""The hold-stamp wedge + the outbox TTL — the D2 soak's P1/P3 fixes'
pins (integration: real PG, the real engine surfaces).

The soak's evidence (REPORT-D2-soak): a SIGKILL during a HELD loop leaves
a node row pending + ``metadata.hold`` stamped whose hold row is no longer
``'held'`` — the claimable fence ``and not metadata ? 'hold'`` then
excludes it FOREVER; TWENTY runs sat wedged at close, unclaimable,
undetected, unnamed. And the delivered ``wf_outbox`` population grew
monotonically forever — no retention law touched it.

These pins hold the fixes: the hold's state DECIDES (still 'held' → the
held representation stands — the resume re-dispatches; delivered /
abandoned / absent → the stamp cleared + the row re-claims, the body's
re-execution landing the next NAMED state), and the delivered outbox has
a TTL (undelivered rows never touched; ``timedelta(0)`` the disable
sentinel).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, cast
from unittest.mock import Mock

import asyncpg
import pytest
import structlog.testing
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobId
from taskq.backend.clock import SystemClock
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.pg import RowVisitCounter, install_row_visit_counter
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _stranded_jobs_loop
from taskq.worker.deps import WorkerDeps
from taskq.workflows import (
    FlowRunner,
    GateDecl,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows._sql_sweep import HOLD_STAMP_RECONCILE_SQL
from taskq.workflows._sweep import sweep_hold_stamps
from taskq.workflows.api._hitl import HitlClient
from taskq.workflows.engine import render_workflow_sql


class Approval(BaseModel):
    verdict: str


class Ingest(BaseModel):
    doc_id: str


def _wedge_app(name: str) -> WorkflowApp:
    """A one-node workflow whose node HOLDS on an Approval — the demo's
    review shape (the registry is exact: a unique name per app). the expiry-as-value'S
    AMENDMENT: the wait's expiry face is the ``Expired`` MEMBER — the
    body converts it into the RAISED failure (the ladder's own use)."""
    from taskq.exceptions import SignalTimeoutError
    from taskq.workflows import Expired

    app = WorkflowApp()

    @app.workflow(name)
    def wedge_flow() -> Promise[object]:
        async def review(ctx: StepContext, params: Ingest) -> object:
            outcome = await ctx.wait_signal(Approval, timeout_s=120.0)
            match outcome:
                case Approval() as approval:
                    return approval
                case Expired():
                    raise SignalTimeoutError("the hold expired — the wedge pin's conversion")

        return build(
            step(
                review,
                Ingest(doc_id="d1"),
                key="review",
                gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),),
            )
        )

    return app


async def _held_flow(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    *,
    name: str,
) -> tuple[JobId, JobId, JobId, FlowRunner]:
    """A flow driven to its hold: (flow_id, node_id, hold_id, runner)."""
    app = _wedge_app(name)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    node_id = await wf_pool.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'review' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    hold_id = await wf_pool.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert node_id and hold_id
    return JobId(flow_id), JobId(node_id), JobId(hold_id), runner


async def _plant_wedge(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    node_id: JobId,
    hold_id: JobId,
    *,
    signal_status: str | None = None,
    delete_signal: bool = False,
) -> None:
    """The SIGKILL wedge's verified END STATE: the signal resolved past
    'held' (the deliver CAS / the expiry arm won) while the node's stamp
    SURVIVED — the exact rows the soak measured 20 of. ``delete_signal``
    plants the ABSENT-hold variant (the pointer outlived its row)."""
    if delete_signal:
        await wf_pool.execute(f'DELETE FROM "{wf_schema}".wf_signals WHERE id = $1', hold_id)
    else:
        assert signal_status is not None
        # A delivered hold carries the CAS's payload — the answer the
        # body's replay consumes (the plant must be the deliver tx's
        # committed shape, payload included).
        await wf_pool.execute(
            f'UPDATE "{wf_schema}".wf_signals SET status = $2, '
            "payload = CASE WHEN $2 = 'delivered' THEN '{\"verdict\": \"approve\"}'::jsonb "
            "ELSE payload END, "
            "resolved_at = clock_timestamp() WHERE id = $1",
            hold_id,
            signal_status,
        )
    # The node: still pending, still stamped (the wedge's shape).
    stamped = await wf_pool.fetchval(
        f"SELECT status = 'pending' AND metadata ? 'hold' FROM \"{wf_schema}\".jobs WHERE id = $1",
        node_id,
    )
    assert stamped, "fixture broken: the wedge's node row is not pending+stamped"


# ── the wedge's paths: every path lands in a named state ────────────────


async def test_delivered_hold_stamp_is_cleared_and_the_run_completes(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The delivered path: the wedge (the signal delivered, the stamp
    standing) is reclaimed by the arm — the stamp cleared, the row
    re-claimed, the body's re-execution REPLAYS the delivered answer from
    the queue (the resume contract) — the run COMPLETES. The 20-wedge
    class's delivered half, dead."""

    flow_id, node_id, hold_id, runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_delivered_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, signal_status="delivered")

    # red shape on the ungated row: the claimable fence excludes it.
    claimable = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND status IN ('pending','scheduled') "
        "AND NOT metadata ? 'hold'",
        flow_id,
    )
    assert claimable == 0, "fixture broken: the wedged row is claimable"

    with structlog.testing.capture_logs() as captured:
        cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 1
    # the heal is a named event (nothing detects or names them — was the soak's
    # half-complaint).
    assert any(e["event"] == "wf-hold-stamp-reclaimed" for e in captured)

    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "succeeded", status


async def test_expired_hold_stamp_is_cleared_and_the_run_lands_the_named_timeout(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The expired path: the hold the expiry arm ABANDONED (the stamp
    survived — the expiry sweep's two-statement shape is one wedge door)
    is reclaimed: the stamp cleared, the body re-executes, the wait site
    raises the typed timeout face, and the ladder terminal-fails the node
    — the NAMED state, never a silent wedge."""

    flow_id, node_id, hold_id, runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_expired_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, signal_status="abandoned")

    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 1

    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "failed", status
    error_class = await wf_pool.fetchval(
        f'SELECT error_class FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert error_class == "SignalTimeoutError", error_class


async def test_absent_hold_stamp_is_cleared_and_a_fresh_hold_resumes(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The absent-hold path: the stamp pointing at a hold row that no
    longer exists (a purge, a corruption) is a DEAD POINTER — cleared, the
    body re-executes, mints a FRESH hold (a NEW epoch — the multi-hold
    law), and a resolve on it completes the run."""

    flow_id, node_id, hold_id, runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_absent_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, delete_signal=True)

    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 1

    # The re-claim re-mints: drive back to the hold.
    verdict = await runner.drive(flow_id, until="held", max_ticks=400, tick=0.01)
    assert verdict == "held"
    new_hold_id = await wf_pool.fetchval(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
        "ORDER BY hold_epoch DESC LIMIT 1",
        flow_id,
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    result = await client.resolve(new_hold_id, {"verdict": "approve"}, principal="pin")
    assert result.status == "delivered"
    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "succeeded", status


async def test_live_hold_is_left_standing(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The held path (the hold is the truth): a stamp whose hold row is
    still 'held' is the LEGITIMATE held representation — the arm leaves
    it untouched, and the resume re-dispatches the run to completion."""

    flow_id, node_id, hold_id, runner = await _held_flow(wf_pool, wf_schema, name="wedge_live_flow")
    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 0
    still = await wf_pool.fetchval(
        f"SELECT metadata ? 'hold' FROM \"{wf_schema}\".jobs WHERE id = $1", node_id
    )
    assert still, "the arm cleared a LIVE hold's stamp — the hold is the truth"

    client = HitlClient(wf_pool, schema=wf_schema)
    result = await client.resolve(hold_id, {"verdict": "approve"}, principal="pin")
    assert result.status == "delivered"
    verdict = await runner.drive(flow_id, until="terminal", max_ticks=400, tick=0.01)
    assert verdict == "terminal"
    status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert status == "succeeded", status


async def test_twenty_wedged_runs_all_land_named_terminal_or_resumed(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The 20-wedge repro (the soak's own receipt): twenty runs planted in
    the exact wedged end state — mixed delivered (10) / abandoned (5) /
    absent hold (5) — and one reconcile pass leaves ZERO of them in the
    wedge; every run then reaches its named state (resumed-and-completed,
    resumed-and-completed, terminal-failed) and nothing is left pending +
    stamped with a dead hold."""
    # Stage every flow to its hold FIRST, then plant the wedge on all of
    # them: the dev drive's own tick runs the reconcile (the #748 dev-face
    # parity), so planting between drives would let a later drive heal an
    # earlier plant and the one-pass claim below would count a residue.
    staged: list[tuple[JobId, JobId, JobId, FlowRunner, str]] = []
    for i in range(10):
        fid, nid, hid, runner = await _held_flow(wf_pool, wf_schema, name=f"wedge20_d{i}_flow")
        staged.append((fid, nid, hid, runner, "delivered"))
    for i in range(5):
        fid, nid, hid, runner = await _held_flow(wf_pool, wf_schema, name=f"wedge20_a{i}_flow")
        staged.append((fid, nid, hid, runner, "abandoned"))
    for i in range(5):
        fid, nid, hid, runner = await _held_flow(wf_pool, wf_schema, name=f"wedge20_x{i}_flow")
        staged.append((fid, nid, hid, runner, "absent"))

    runs: list[tuple[JobId, JobId, JobId, FlowRunner, str]] = []
    for fid, nid, hid, runner, kind in staged:
        if kind == "absent":
            await _plant_wedge(wf_pool, wf_schema, nid, hid, delete_signal=True)
        else:
            await _plant_wedge(wf_pool, wf_schema, nid, hid, signal_status=kind)
        runs.append((fid, nid, hid, runner, kind))

    cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 20, cleared

    # no row sits pending/scheduled + stamped with a dead hold.
    wedged = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs j '
        "WHERE j.metadata ? 'hold' AND j.status IN ('pending','scheduled') "
        f'AND NOT EXISTS (SELECT 1 FROM "{wf_schema}".wf_signals s '
        "WHERE s.id::text = j.metadata->>'hold' AND s.status = 'held')"
    )
    assert wedged == 0, f"{wedged} run(s) remain wedged after the reconcile"

    # every run reaches its named state.
    for fid, _nid, _hid, runner, kind in runs:
        if kind == "absent":
            # The absent-hold path re-mints: the body's re-execution
            # registers a fresh hold (a new epoch) — a resolve on it
            # resumes the run.
            verdict = await runner.drive(fid, until="held", max_ticks=400, tick=0.01)
            assert verdict == "held", (kind, fid)
            new_hold_id = await wf_pool.fetchval(
                f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
                "ORDER BY hold_epoch DESC LIMIT 1",
                fid,
            )
            client = HitlClient(wf_pool, schema=wf_schema)
            result = await client.resolve(new_hold_id, {"verdict": "approve"}, principal="pin")
            assert result.status == "delivered", (kind, result)
        verdict = await runner.drive(fid, until="terminal", max_ticks=400, tick=0.01)
        assert verdict == "terminal", (kind, fid)
        status = await wf_pool.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', fid)
        if kind == "abandoned":
            assert status == "failed", (kind, status)  # the named timeout face
        else:
            assert status == "succeeded", (kind, status)  # resumed + completed


async def test_hold_stamp_reconcile_statement_is_the_batch_bounded_self_consuming_shape(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The arm's own statement shape: LIMIT-bounded (a pass never
    unbounds) and self-consuming (a cleared row loses the stamp — the
    second pass returns zero), and it never touches a row whose hold is
    still 'held' (the held-row exclusivity, P3 rule 1's family)."""
    app = _wedge_app("wedge_batch_flow")
    runner = FlowRunner(app.get("wedge_batch_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    node_id, hold_id = await _node_and_hold(wf_conn, wf_schema, flow_id)
    await _plant_wedge(wf_conn, wf_schema, node_id, hold_id, signal_status="delivered")

    # The batch bound: LIMIT 1 clears exactly one row per pass.
    rows = await wf_conn.fetch(HOLD_STAMP_RECONCILE_SQL.replace("{schema}", wf_schema), 1)
    assert len(rows) == 1
    again = await wf_conn.fetch(HOLD_STAMP_RECONCILE_SQL.replace("{schema}", wf_schema), 1)
    assert again == [], "the arm's predicate must self-consume (a cleared row loses the stamp)"


async def test_hold_stamp_reconcile_window_never_spends_a_slot_on_a_live_hold(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Issue #748: the batch window is taken over dead stamps only. An older
    row whose hold is still 'held' sorts first by id; if it took the LIMIT's
    slot, every newer dead stamp behind a batch of standing holds would never
    be examined (the fleet's fence now excludes stamped rows, so that row
    would wait forever)."""
    _, node_old, _, _ = await _held_flow(wf_pool, wf_schema, name="wedge_window_old_flow")
    _, node_new, hold_new, _ = await _held_flow(wf_pool, wf_schema, name="wedge_window_new_flow")
    await _plant_wedge(wf_pool, wf_schema, node_new, hold_new, signal_status="delivered")
    assert str(node_old) < str(node_new)  # the live hold sorts first

    rows = await wf_conn.fetch(HOLD_STAMP_RECONCILE_SQL.replace("{schema}", wf_schema), 1)
    assert [r["id"] for r in rows] == [node_new], (
        f"LIMIT 1 cleared {[str(r['id']) for r in rows]}, not the dead stamp {node_new}: "
        f"the live hold on {node_old} took the batch's only slot"
    )
    still_stamped = await wf_conn.fetchval(
        f"SELECT metadata ? 'hold' FROM \"{wf_schema}\".jobs WHERE id = $1", node_old
    )
    assert still_stamped is True, "a live hold's stamp is the held representation"


async def _node_and_hold(
    conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> tuple[JobId, JobId]:
    node_id = await conn.fetchval(
        f"SELECT id FROM \"{schema}\".jobs WHERE step_key = 'review' "
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    hold_id = await conn.fetchval(
        f'SELECT id FROM "{schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert node_id and hold_id
    return JobId(node_id), JobId(hold_id)


# ── the arm's operability surface (issue #748's review) ─────────────────


async def test_the_reclaimed_event_names_the_row_it_healed(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The heal's own comment claims the operator "can correlate it with the
    process death that stamped it" — a count cannot. The event carries the
    cleared batch's node ids and its flow ids, so a heal is attributable."""
    flow_id, node_id, hold_id, _runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_named_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, signal_status="delivered")

    with structlog.testing.capture_logs() as captured:
        cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
    assert cleared == 1
    records = [e for e in captured if e["event"] == "wf-hold-stamp-reclaimed"]
    assert records, [e["event"] for e in captured]
    record = records[0]
    assert record["job_ids"] == [str(node_id)], record
    assert record["flow_ids"] == [str(flow_id)], record


class _DetectorDeps:
    """Duck-typed WorkerDeps for ``_stranded_jobs_loop``: the fields its body
    reads."""

    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool
        self.is_leader = asyncio.Event()
        self.is_leader.set()
        self.liveness = Mock()
        self.leader_term = None

    def leading(self) -> bool:
        return bool(WorkerDeps.leading(cast(WorkerDeps, self)))


async def test_a_dead_stamped_row_is_reported_as_stamp_unresolved(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shape issue #748 creates: the row is ``pending``, due, and no
    dispatch round will ever claim it (the fence's ``AND NOT metadata ?
    'hold'``). The detector's charter is "pending/scheduled jobs that cannot
    make progress, with the reason each carries" — so the dead stamp is its
    own reason, ``stamp_unresolved``, and it is visible on the gauge."""
    flow_id, node_id, hold_id, _runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_detect_flow"
    )
    await _plant_wedge(wf_pool, wf_schema, node_id, hold_id, signal_status="delivered")

    # The routing legs must be satisfied, or the row would classify as one of
    # them instead: an actor_config row exists and a live worker serves the
    # row's queue.
    row = await wf_conn.fetchrow(
        f'SELECT actor, queue FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert row is not None
    actor, queue = str(row["actor"]), str(row["queue"])
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '
        "ON CONFLICT (actor) DO NOTHING",
        actor,
        queue,
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, last_seen_at) '
        "VALUES (gen_random_uuid(), 'probe', 1, ARRAY[$1]::text[], clock_timestamp())",
        queue,
    )
    try:
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": wf_schema,
                "TASKQ_STRANDED_JOBS_INTERVAL": "0.05",
            },
            validate=False,
        )
        ctx = SweepContext(
            deps=_DetectorDeps(settings, wf_pool),  # type: ignore[arg-type]  # Why: duck-typed deps carrying exactly the fields the loop reads.
            backend=None,  # type: ignore[arg-type]  # Why: the stranded loop never touches ctx.backend.
            clock=SystemClock(),
            worker_id=JobId(new_uuid()),
        )
        published: list[dict[tuple[str, str], int]] = []

        def _capture(data: Mapping[tuple[str, str], int]) -> None:
            published.append(dict(data))

        monkeypatch.setattr("taskq.worker._leader_sweeps.update_stranded_jobs_cache", _capture)
        shutdown = asyncio.Event()
        task = asyncio.create_task(_stranded_jobs_loop(ctx, shutdown))
        try:
            for _ in range(200):
                if published:
                    break
                await asyncio.sleep(0.02)
            assert published, "the detector loop never published a tick"
        finally:
            shutdown.set()
            await asyncio.wait_for(task, timeout=5.0)

        stranded = {reason: count for (_actor, reason), count in published[-1].items() if count}
        assert stranded.get("stamp_unresolved", 0) >= 1, (
            "the stranded-jobs detector published no stamp_unresolved reason for "
            f"the dead-stamped run {flow_id} (node {node_id}): its actor has an "
            f"actor_config row, queue {queue!r} is served by a live worker, and "
            "its run root exists — so all the old legs pass it, while the "
            f"fleet's dispatch fence refuses it. Published: {stranded}"
        )
    finally:
        await wf_conn.execute(f'DELETE FROM "{wf_schema}".workers WHERE hostname = $1', "probe")


class _SinglePool:
    """One asyncpg connection wearing the ``pool.acquire()`` shape."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self, **_kwargs: object) -> Any:
        conn = self._conn

        class _Ctx:
            async def __aenter__(self) -> asyncpg.Connection:
                return conn

            async def __aexit__(self, *exc: object) -> Any:
                return None

        return _Ctx()


async def test_reconcile_read_cost_stays_a_single_pass_over_the_two_tables(pg_dsn: str) -> None:
    """The arm runs on every leader tick (``sweep_interval``, default 30 s)
    and, with ``drain=True``, up to ``sweep_drain_batches`` times per tick.

    The statement plans as a Hash Anti Join, so its read is one pass over the
    stamped ``jobs`` rows plus one pass over ``wf_signals`` (a seq scan with
    ``status = 'held'`` — the cast ``live.id::text`` cannot be an index
    condition). What this pins is the PASS COUNT: a rewrite that turns the
    ``NOT EXISTS`` into a per-candidate probe pays
    ``candidates x |wf_signals|`` on an arm that now runs unattended on the
    leader."""
    holds, history = 1_000, 20_000
    schema = f"hsprobe_cost_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await install_row_visit_counter(conn, schema, table="jobs")
        await install_row_visit_counter(conn, schema, table="wf_signals")
        await conn.execute(
            f'INSERT INTO "{schema}".wf_signals '  # Why: generated identifier; values $-bound.
            "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status) "
            "SELECT gen_random_uuid(), gen_random_uuid(), 'review', 'Approval', 1, "
            "'call', 'held' FROM generate_series(1, $1::int)",
            holds,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".wf_signals '  # Why: generated identifier; values $-bound.
            "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status) "
            "SELECT gen_random_uuid(), gen_random_uuid(), 'review', 'Approval', 1, "
            "'call', 'delivered' FROM generate_series(1, $1::int)",
            history,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # Why: generated identifier; values $-bound.
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind, metadata, step_key, fairness_key) "
            "SELECT gen_random_uuid(), 'wf', 'default', '{\"v\": 1}'::jsonb, 'pending', 0, "
            "clock_timestamp() - interval '5 minutes', 3, 'transient', "
            "jsonb_build_object('hold', live.id::text, 'flow_id', live.id::text), "
            f"'review', live.id::text FROM \"{schema}\".wf_signals live "
            "WHERE live.status = 'held'",
        )
        await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')
        await conn.execute(f'VACUUM (ANALYZE) "{schema}".wf_signals')

        counter = RowVisitCounter(_SinglePool(conn), schema, method="fetch")
        sql = HOLD_STAMP_RECONCILE_SQL.replace("{schema}", schema)
        async with counter.acquire() as counted:
            rows = await counted.fetch(sql, 100)
        visits = counter.per_statement[-1]
        assert rows == [], "the steady state has no dead stamp to clear"
        budget = 4 * (holds + history)
        assert visits <= budget, (
            f"the reconcile read {visits} rows for {holds} stamped rows and "
            f"{history + holds} signal rows (budget {budget}): a per-candidate "
            "probe instead of one pass over wf_signals, on a leader-tick arm "
            "that runs up to sweep_drain_batches times every sweep_interval"
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_a_mint_whose_park_matches_nothing_is_named(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The mint's park is a conditional write (``AND status = 'running'``).
    A body whose lease lapsed, or whose node a concurrent resume/cancel moved,
    mints a live ``held`` signal while the node update matches nothing — the
    approval queue then shows a hold for a node that is not waiting. The miss
    is a named event (the result of the write is read, not discarded)."""
    from taskq.workflows.api._hitl import register_hold

    _flow_id, node_id, _hold_id, _runner = await _held_flow(
        wf_pool, wf_schema, name="wedge_mint_miss_flow"
    )
    # The node rests pending (the standing hold's held representation): a
    # second mint's park cannot match.
    with structlog.testing.capture_logs() as captured:
        minted = await register_hold(
            wf_conn,
            schema=wf_schema,
            workflow_id=_flow_id,
            node_id=node_id,
            node_key="review",
            signal_name="Approval",
            hold_epoch=99,
            call_id="call:1:1",
            payload_schema={"Approval": {"type": "object"}},
            timeout_s=None,
        )
    assert minted is not None
    misses = [e for e in captured if e["event"] == "wf-hold-mint-park-missed"]
    assert misses, [e["event"] for e in captured]
    assert misses[0]["node_id"] == str(node_id), misses[0]
    assert misses[0]["hold_id"] == str(minted), misses[0]
