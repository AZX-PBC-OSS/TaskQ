"""T21 — THE FACES + THE FENCE PINS: the SSE replay, the aggregation, the
progress-lie fence, the gauge's cardinality.

The provenance: the PoC's PROOF 1/2/5/6 — the chain, the fan-out
aggregation, the lie/crash dragons, the bounds — ported onto the BUILT
surfaces. Each pin's CONVICTED VARIANT is named and — where observable —
drilled with the red captured to ``.measurements/t21-pin-reds.json``.

THE FENCE (DH4/DH7, the law of the display): the node's STATUS derives
from the LEDGER; the progress renders INSIDE the terminal state, never
over it — a body that reports pct=99 and FAILS shows FAILED with pct=99
inside, never 99%-running. The progress is ADVISORY.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    ProgressEmitter,
    WorkflowApp,
    build,
    map_progress_line,
    map_source,
    progress_sse_face,
    read_map_aggregate,
    rebuild_display,
    run_display,
    sink,
    step,
)
from taskq.workflows._sql import WorkflowSql
from tests._wf_fixtures import RedLog, claim_view, seed_flow, seed_running_node

pytestmark = pytest.mark.integration


class Ingest(BaseModel):
    doc_id: str


def _loads(raw: Any) -> Any:
    return json.loads(raw) if isinstance(raw, str) else raw


async def _map_flow(
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    children: int,
    aggregate: Any | None = None,
    name: str,
) -> tuple[JobId, FlowRunner, JobId]:
    """A MAP flow's SKELETON (source wired, not yet run): the source's
    body returns N item indices; the item body emits honest progress and
    returns {"risk": i}. Returns (flow_id, runner, source_id) — the
    caller drives."""
    app = WorkflowApp()

    async def fetch(ctx: Any, params: Ingest) -> list[int]:
        return list(range(children))

    async def item(ctx: Any, value: int) -> dict[str, object]:
        pct = ((value + 1) * 100) // children
        await ctx.progress(pct, f"child {value}", {"child": value})
        return {"risk": value}

    @app.workflow(name)
    def map_flow() -> object:
        source = step(fetch, Ingest(doc_id="d1"), key="fetch")
        items = map_source(source, item, key="proc", aggregate=aggregate)
        return build(items)

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # ONE tick: the source runs + forks the children; the children stay
    # pending (the caller drives them in its own rhythm).
    await runner.tick(flow_id)
    source = await wf_pool.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs WHERE step_key = 'fetch'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    return flow_id, runner, JobId(source)


# ── THE CHAIN'S RECONNECT (PROOF 1 + P6b) ────────────────────────────────


async def test_reconnect_reconstructs_the_display_full_and_partial(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """A consumer disconnecting mid-map (the cursor at the median seq) and
    reconnecting reconstructs the display for every node: the connect
    shape is the STATE channel + the LEDGER first, then the replayed tail
    (latest-wins needs no history). And the PRUNED window (DH6) names
    itself: mode='partial' with the state re-sync — the display converges
    from the re-sync."""
    n = 12
    flow_id, runner, source = await _map_flow(
        wf_schema, wf_pool, children=n, name="t21_reconnect_flow"
    )
    _ = source
    await runner.drive(flow_id)

    async def node_of(map_index: int) -> JobId:
        nid = await wf_conn.fetchval(
            f"""SELECT id FROM "{wf_schema}".jobs WHERE step_key = 'fetch.item'
                AND map_index = $1 AND (metadata->>'flow_id')::uuid = $2::uuid""",
            map_index,
            flow_id,
        )
        assert nid is not None
        return JobId(nid)

    ledger_rows = [
        dict(r)
        for r in await wf_conn.fetch(
            f"""SELECT id, step_key, status::text AS status, error_class
                FROM "{wf_schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                AND metadata ? 'flow_id' AND step_key <> '__flow__'""",
            flow_id,
        )
    ]
    state_rows = [dict(r) for r in await wf_conn.fetch(wf_sql.progress_state_read_run, flow_id)]
    state_by_node = {str(s["node_id"]): s for s in state_rows if s["channel"] == "progress"}

    # FULL: the replay from the beginning alone reconstructs.
    full = await progress_sse_face(wf_pool, wf_sql, flow_id=flow_id, last_event_id=0)
    assert full.mode == "full"
    display = rebuild_display(ledger_rows, [], full.events)
    for i in range(n):
        nid = str(await node_of(i))
        d = display[nid]
        s = state_by_node[nid]
        assert d["status"] == "succeeded"
        assert d["pct"] == s["pct"], f"child {i}: the fold lost its progress"
        assert d["message"] == s["message"]

    # THE MID-MAP DISCONNECT: the cursor at the median seq — the
    # reconnect (the state backfill + the tail) converges to the SAME
    # display.
    cursor = int(full.events[len(full.events) // 2]["seq"])
    tail = await progress_sse_face(wf_pool, wf_sql, flow_id=flow_id, last_event_id=cursor)
    display2 = rebuild_display(ledger_rows, state_rows, tail.events)
    for i in range(n):
        nid = str(await node_of(i))
        d = display2[nid]
        assert d["status"] == "succeeded"
        assert d["pct"] == state_by_node[nid]["pct"]

    # THE PRUNE (DH6): the ring prunes past the cursor — the face MUST
    # name the partial mode + serve the state re-sync (never a silent
    # empty-success), and the display converges from the re-sync alone.
    await wf_conn.execute(
        f"""DELETE FROM "{wf_schema}".wf_node_stream
            WHERE flow_id = $1::uuid AND seq <= $2::bigint + 5""",
        flow_id,
        cursor,
    )
    pruned = await progress_sse_face(wf_pool, wf_sql, flow_id=flow_id, last_event_id=cursor)
    assert pruned.mode == "partial", "a pruned-past cursor must NAME its mode"
    assert pruned.state_sync, "the partial mode must carry the state re-sync"
    display3 = rebuild_display(ledger_rows, pruned.state_sync, pruned.events)
    for i in range(n):
        nid = str(await node_of(i))
        d = display3[nid]
        assert d["status"] == "succeeded"
        assert d["pct"] == state_by_node[nid]["pct"], (
            "the display did not converge from the state re-sync"
        )


async def test_silent_gap_variant_is_structurally_impossible(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    progress_redlog: RedLog,
) -> None:
    """DH6's RED, kept forever: the silent-gap face — a replay that
    returns nothing over a pruned window while claiming success — is the
    convicted variant. The BUILT face's mode logic refuses the shape:
    whenever the cursor predates the retained window the mode is
    'partial' (named), so 'full' + zero-events-over-a-pruned-window
    cannot be constructed."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    from tests.test_wf_progress_persistence import _append

    seqs = [await _append(wf_conn, wf_sql, node, flow, payload={"pct": i}) for i in range(5)]
    # the consumer's cursor INSIDE the window (the second event's seq) → full
    face = await progress_sse_face(wf_pool, wf_sql, node_id=node, last_event_id=seqs[1][0])
    assert face.mode == "full"
    # the pruned-past cursor → partial, NAMED (the silent gap cannot exist)
    await wf_conn.execute(
        f'DELETE FROM "{wf_schema}".wf_node_stream WHERE node_id = $1 AND seq <= $2',
        node,
        seqs[3][0],
    )
    face = await progress_sse_face(wf_pool, wf_sql, node_id=node, last_event_id=seqs[1][0])
    assert face.mode == "partial"
    progress_redlog.red(
        "silent_gap_variant_structurally_impossible",
        "the convicted variant: a face that replays nothing over a pruned "
        "window while claiming mode=full — the stale display forever",
        {"observed": "mode='partial' + the state re-sync; the variant cannot be built"},
    )


# ── THE PROGRESS-LIE FENCE (DH4) + THE CRASH WINDOW (DH7) ────────────────


async def test_display_shows_failure_with_pct_inside_the_liar_reds(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    progress_redlog: RedLog,
) -> None:
    """DH4 on the BUILT display: a body reports pct=99 'almost done' then
    FAILS → the explorer shows FAILED with pct=99 rendered INSIDE it —
    never 99%-running. THE LIAR RED (the counterfactual display deriving
    the state from the progress events) shows running/99 on the dead node
    — observed, kept red forever."""
    app = WorkflowApp()

    async def liar(ctx: Any, params: Ingest) -> dict[str, object]:
        await ctx.progress(99, "almost done", None)
        raise ValueError("the body failed at 99%")

    @app.workflow("t21_liar_display")
    def t21_liar_display() -> object:
        return build(step(liar, Ingest(doc_id="d1"), key="liar", max_attempts=1))

    runner = FlowRunner(app.get("t21_liar_display"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    await runner.drive(flow_id)

    node = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs WHERE step_key = 'liar'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    ledger_rows = [
        dict(r)
        for r in await wf_conn.fetch(
            f"""SELECT id, step_key, status::text AS status, error_class
                FROM "{wf_schema}".jobs WHERE id = $1""",
            node,
        )
    ]
    state_rows = [dict(r) for r in await wf_conn.fetch(wf_sql.progress_state_read_node, node)]
    events = [dict(r) for r in await wf_conn.fetch(wf_sql.progress_replay_node, 0, node, 1000)]
    display = rebuild_display(ledger_rows, state_rows, events)
    d = display[str(node)]
    assert d["status"] == "failed", (
        f"the display shows {d['status']!r} at pct {d['pct']} — the LIE (the "
        "progress overrode the ledger)"
    )
    assert d["pct"] == 99 and d["message"] == "almost done", (
        "the last progress must render INSIDE the terminal state (advisory)"
    )
    # THE LIAR RED: the counterfactual — the state derived from the
    # emissions naively (an emission means alive; the terminal only read
    # when no progress exists) — shows the lie.
    liar_display: dict[str, Any] = {"status": None, "pct": None}
    for e in sorted(events, key=lambda e: int(e["seq"])):
        p = _loads(e["payload"]) or {}
        if e["kind"] == "progress":
            liar_display["pct"] = p.get("pct")
            liar_display["status"] = "running"  # the liar's law: emission = alive
        elif liar_display["status"] is None:
            liar_display["status"] = p.get("outcome")
    progress_redlog.red(
        "display_shows_failure_with_pct_inside",
        "the liar counterfactual: the display deriving the node's state "
        "from the progress emissions (the terminal read only when no "
        "progress exists)",
        {"liar_shows": liar_display, "truth": d},
    )
    assert liar_display["status"] == "running" and liar_display["pct"] == 99, (
        "the convicted variant did not convict"
    )


async def test_crash_window_freshness_only_loss_then_the_terminal_heals(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """DH7 on the BUILT surfaces: a worker dies mid-emission (the buffer's
    unflushed emissions die with it — emulated by a cancelled flush task;
    the PoC's real SIGKILL'd worker proved the same shape). The display
    goes STALE — pct frozen at 99 while the LEDGER still says running —
    then the terminal heals it. THE INVARIANT: the display's STATE is
    ledger-derived at every sample; freshness is the only thing the crash
    can lose."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)

    # the live attempt flushed to 99...
    live = ProgressEmitter(wf_pool, wf_sql, flow_id=flow, node_id=node)
    await live.emit(99, "almost done", None)
    await live.aclose()

    async def sample_display() -> dict[str, Any]:
        d = await run_display(wf_pool, wf_sql, flow)
        return d[str(node)]

    stale = await sample_display()
    assert stale["status"] == "running", "the LEDGER owns the state"
    assert stale["pct"] == 99

    # ...the next attempt emits 100 and DIES before the flush (the kill
    # window — the buffer's emissions unflushed, the task cancelled as the
    # SIGKILL cancels the process).
    dying = ProgressEmitter(wf_pool, wf_sql, flow_id=flow, node_id=node)
    await dying.emit(100, "done", None)
    assert dying._flush_task is not None  # pyright: ignore[reportPrivateUsage]
    dying._flush_task.cancel()  # pyright: ignore[reportPrivateUsage]  # the kill

    window = await sample_display()
    # THE WINDOW: the display is STALE — pct frozen at 99 — and the STATE
    # is still the LEDGER's (running), never anything the dead emitter
    # claimed.
    assert window["pct"] == 99, "the unflushed emission leaked past the kill"
    assert window["status"] == "running", "the state stopped being ledger-derived"

    # THE HEAL: the terminal lands (the reclaim re-ran the body; the
    # finalize owns the terminal) — the display heals from the LEDGER.
    from taskq.workflows.engine import finalize_node

    worker_id, attempt, epoch = await claim_view(wf_conn, wf_schema, node)
    await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow,
        job_id=node,
        step_key="a",
        worker_id=worker_id,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"value": 1},
    )
    healed = await sample_display()
    assert healed["status"] == "succeeded", "the terminal did not heal the display"
    assert healed["pct"] == 99, "the stale pct renders INSIDE the healed state (advisory)"


# ── THE AGGREGATION (DH8) + THE GAUGE (DH5) ──────────────────────────────


async def test_midflight_read_side_aggregate_writes_nothing(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """DH8 on the BUILT surface: a map MID-FLIGHT (half the children done)
    — the DECLARED ``aggregate=`` fn runs over the DONE children's RESULT
    rows AT READ TIME: mid-flight, unblocked, writing NOTHING. No join
    node exists for it (``wf_join_fire`` count 0 for the whole run). The
    map's progress line rides the grouped read."""

    def risk_mean(rows: list[dict[str, object]]) -> float:
        return sum(int(r["risk"]) for r in rows) / len(rows)

    n = 12
    flow_id, runner, source = await _map_flow(
        wf_schema, wf_pool, children=n, aggregate=risk_mean, name="t21_agg_flow"
    )
    # run HALF the children (the mid-flight world: the PoC's 125/200)
    rows = await runner._claimable(flow_id)  # pyright: ignore[reportPrivateUsage]
    assert rows, "the fork's children are claimable"
    for row in rows[: len(rows) // 2]:
        await runner._run_node(flow_id, row)  # pyright: ignore[reportPrivateUsage]

    done = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE parent_id = $1::uuid AND status = 'succeeded'""",
        source,
    )
    assert 0 < done < n, "the world is not mid-flight"

    line = await map_progress_line(wf_pool, wf_sql, source)
    r = next(r for r in line if r["step_key"] == "fetch.item")
    assert r["total"] == n and r["done"] == done and r["running"] == 0, (
        "the grouped read's counts: done children counted, the rest pending"
    )

    before = {
        t: await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".{t}')
        for t in ("jobs", "wf_node_progress", "wf_node_stream", "wf_join_fire")
    }
    read = await read_map_aggregate(wf_pool, wf_sql, flow_id, source)
    after = {
        t: await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".{t}')
        for t in ("jobs", "wf_node_progress", "wf_node_stream", "wf_join_fire")
    }
    assert read.children == done, "the fn ran over the DONE children's results"
    assert read.value == sum(range(done)) / done, (
        "the declared fn ran over the children's results (mid-flight)"
    )
    assert before == after, "the read-side aggregate WROTE something (DH8's fence broken)"
    assert before["wf_join_fire"] == 0, "a join node exists for progress — the blocking mistake"
    assert read.fn_ms < 50, f"the fn's read-time cost exploded: {read.fn_ms}ms"

    # the EXPLICIT fn's door (the read-side caller's own): it overrides
    # the declared fn — the same rows, the caller's question
    raw = await read_map_aggregate(wf_pool, wf_sql, flow_id, source, fn=lambda rows: len(rows))
    assert raw.value == done


async def test_gauge_cardinality_stays_workflow_dimensioned(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, progress_redlog: RedLog
) -> None:
    """DH5 on the BUILT surface: a per-child-labeled progress series is
    the convicted variant (a 200-child map would make 200 series — the
    metrics doctrine violated). The BUILT gauge's input groups by
    (workflow, state) — WORKFLOW-dimensioned; per-child progress lives in
    the ROWS (the map progress line / the display), read on demand."""
    from taskq.worker._leader_shared import (
        _QUERY_WF_PROGRESS_SQL_TEMPLATE,  # pyright: ignore[reportPrivateImportUsage]
    )

    n = 12
    flow_id, runner, _source = await _map_flow(
        wf_schema, wf_pool, children=n, name="t21_gauge_flow"
    )
    _ = flow_id
    await runner.drive(flow_id)
    rows = await wf_conn.fetch(_QUERY_WF_PROGRESS_SQL_TEMPLATE.format(schema=wf_schema))
    this_flow = {(r["workflow"], r["state"]) for r in rows if r["workflow"] == "t21_gauge_flow"}
    counterfactual = n  # the per-child-label variant's series count
    progress_redlog.red(
        "gauge_cardinality_stays_workflow_dimensioned",
        "the per-child-label counterfactual: one series per child "
        f"({counterfactual} series for this map)",
        {"built_series": len(this_flow), "counterfactual_series": counterfactual},
    )
    assert len(this_flow) < counterfactual, (
        "the gauge's series count grows with children — DH5's explosion"
    )
    # every series is (workflow, state) — never a node identity
    for _workflow, state in this_flow:
        assert state in (
            "pending",
            "scheduled",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "crashed",
            "abandoned",
        )


async def test_sunk_join_for_progress_warns_at_validate(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """W2 (DH8's build-phase obligation): a join whose only reader is the
    explorer's display — the SUNK join — warns at validate; the aggregate
    door is named in the warning."""
    from taskq.workflows.api._validate import validate_compiled

    app = WorkflowApp()

    async def fetch(ctx: Any, params: Ingest) -> list[int]:
        return [1, 2]

    async def item(ctx: Any, value: int) -> int:
        return value

    @app.workflow("t21_w2_flow")
    def w2_flow() -> object:
        source = step(fetch, Ingest(doc_id="d1"), key="fetch")
        items = map_source(source, item, key="proc")
        sink(items)  # the display-only reader, declared
        return build(source)

    compiled = app.get("t21_w2_flow")
    diagnostics = validate_compiled(compiled)
    w2 = [d for d in diagnostics if d.rule == "W2-join-for-progress"]
    assert w2, "the sunk join did not warn"
    assert "aggregate=" in w2[0].message
    _ = wf_pool, wf_schema
