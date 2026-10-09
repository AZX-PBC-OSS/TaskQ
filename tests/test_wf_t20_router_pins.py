"""T20's ROUTER pins — the typed-outcome Chain/Step/Route surface
(PROOF.md §3 + §4).

TOTALITY IS THE FENCE, at TWO doors (the asymmetry doctrine — the spike's
§3): the DECLARATION-time refusal (a non-total route is a coding error,
convicted before any row exists) and the RUNTIME loud refusal
(``RouterNotTotal`` — the step terminal-FAILS with
``error_class='RouterNotTotal'``, the record names the defect; the
record's chain visibly dies, never silently drops).

The routing RUNS through the certified fork-at-finalize machinery: a
chain step's finalize forks AT MOST ONE child (no fan-in, no join), the
record's ``map_index`` + trace riding forward — the per-record lineage is
one query (the drill-down by trace).

Red-first: the declaration-refusal and the runtime-refusal pins ran RED
against the tree before the chain surface existed; the greens below are
the built code's evidence. Captured:
``.measurements/t20-router-*.txt``.
"""

from __future__ import annotations

import enum
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import StepContext
from taskq.workflows.chain import (
    DONE,
    Chain,
    Route,
    RouterNotTotal,
    Step,
    chain_start,
)
from tests._wf_fixtures import RedLog, seed_flow
from tests.test_wf_t20_emit_pins import claim_source, make_source


class ScreenOutcome(enum.Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


class EnrichOutcome(enum.Enum):
    OK = "ok"
    SPARSE = "sparse"


class ReviewOutcome(enum.Enum):
    APPROVE = "approve"
    REJECT = "reject"


async def screen_app(ctx: object, item: dict[str, object]) -> ScreenOutcome:
    risk = float(item["risk"])  # pyright: ignore[reportArgumentType]  # Why: the pin's own item shape (the emit wrote it).
    return ScreenOutcome.FLAGGED if risk > 0.8 else ScreenOutcome.CLEAN


async def enrich_app(ctx: object, item: dict[str, object]) -> EnrichOutcome:
    return EnrichOutcome.OK


async def manual_review(ctx: object, item: dict[str, object]) -> ReviewOutcome:
    return ReviewOutcome.APPROVE


def the_chain() -> Chain:
    """The spike's §4 chain: screen routes flagged → manual_review,
    clean → enrich; enrich OK → score... reduced to the two-hop shape the
    pins drive (the route DICT is the subject, not the chain's length)."""
    return Chain(
        name="application-enrichment",
        start="screen",
        steps={
            "screen": Step(
                body=screen_app,
                outcomes=ScreenOutcome,
                route=Route(
                    {
                        ScreenOutcome.CLEAN: "enrich",
                        ScreenOutcome.FLAGGED: "manual_review",
                    }
                ),
            ),
            "enrich": Step(
                body=enrich_app,
                outcomes=EnrichOutcome,
                route=Route({EnrichOutcome.OK: DONE, EnrichOutcome.SPARSE: DONE}),
            ),
            "manual_review": Step(
                body=manual_review,
                outcomes=ReviewOutcome,
                route=Route({ReviewOutcome.APPROVE: DONE, ReviewOutcome.REJECT: DONE}),
            ),
        },
    )


# ── DOOR 1: the declaration-time refusal ────────────────────────────────


def test_t20_route_not_total_is_refused_at_declaration() -> None:
    """A route missing an outcome is REFUSED when the chain is declared —
    the outcome it drops would silently strand a record's chain. The
    refusal NAMES the missing member (the spike's message shape)."""
    t20_redlog_entry: dict[str, object] = {}
    try:
        Chain(
            name="broken",
            start="screen",
            steps={
                "screen": Step(
                    body=screen_app,
                    outcomes=ScreenOutcome,
                    # the FLAGGED outcome is MISSING:
                    route=Route({ScreenOutcome.CLEAN: "enrich"}),
                ),
            },
        )
    except ValueError as exc:
        t20_redlog_entry = {"refused": str(exc)}
        assert "missing" in str(exc) and "'flagged'" in str(exc), (
            f"the refusal must NAME the dropped outcome, got: {exc}"
        )
    else:
        raise AssertionError("NOT REFUSED — the totality fence is broken")

    # the flip: an UNKNOWN key is the same refusal's other face.
    with pytest.raises(ValueError, match="unknown"):
        Chain(
            name="broken-2",
            start="screen",
            steps={
                "screen": Step(
                    body=screen_app,
                    outcomes=ScreenOutcome,
                    route=Route(
                        {
                            ScreenOutcome.CLEAN: "enrich",
                            ScreenOutcome.FLAGGED: "enrich",
                            "ghost": "enrich",  # type: ignore[dict-item]  # Why: the drill — the unknown key is the refusal's subject.
                        }
                    ),
                ),
            },
        )

    # the route to a NON-STEP key is refused too (the chain's shape).
    with pytest.raises(ValueError, match="not a step"):
        Chain(
            name="broken-3",
            start="screen",
            steps={
                "screen": Step(
                    body=screen_app,
                    outcomes=ScreenOutcome,
                    route=Route({ScreenOutcome.CLEAN: "enrich", ScreenOutcome.FLAGGED: "ghost"}),
                ),
                "enrich": Step(
                    body=enrich_app,
                    outcomes=EnrichOutcome,
                    route=Route({EnrichOutcome.OK: DONE, EnrichOutcome.SPARSE: DONE}),
                ),
            },
        )

    # the start must be a step.
    with pytest.raises(ValueError, match="not a step"):
        Chain(name="broken-4", start="ghost", steps={})

    # the healthy declaration (the ergonomics bar): every route total.
    chain = the_chain()
    assert chain.start == "screen"
    assert set(chain.steps) == {"screen", "enrich", "manual_review"}
    del t20_redlog_entry


# ── DOOR 2: the runtime loud refusal ────────────────────────────────────


def test_t20_router_not_total_loud_refusal() -> None:
    """An outcome with NO arm raises :class:`RouterNotTotal` — the runner
    converts it to a terminal-FAILED finalize carrying
    ``error_class='RouterNotTotal'`` (the integration below pins the ROW).
    The silent-drop variant (a bare KeyError swallowing, a ``None``
    returned) is the convicted shape — never shipped."""
    route = Route({ScreenOutcome.CLEAN: "enrich", ScreenOutcome.FLAGGED: "manual_review"})
    with pytest.raises(RouterNotTotal, match="'corrupted'"):
        route.next_step("corrupted")  # a foreign outcome at runtime
    # DONE routes end the chain (None — no fork).
    total = Route({EnrichOutcome.OK: DONE, EnrichOutcome.SPARSE: None})
    assert total.next_step("ok") is DONE
    assert total.next_step("sparse") is None


# ── THE ROUTE'S DECISION RIDES THE CERTIFIED FORK ───────────────────────


@pytest.mark.integration
async def test_t20_chain_routes_through_the_certified_fork(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
    t20_redlog: RedLog,
) -> None:
    """The full conditional-edge walk on the REAL engine: the source emits
    two records' chain starts (clean + flagged); the chain steps' bodies
    return their typed outcomes; each finalize forks AT MOST ONE child
    (the route's arm — no join anywhere); the record's map_index + trace
    ride EVERY row of its chain. The two records end on DIFFERENT routes
    (the conditional edge, observed on the rows)."""
    chain = the_chain()
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    from taskq.workflows._emit import emit_batch
    from taskq.workflows.engine import finalize_node
    from taskq.workflows.ledger import claim_step_ledger

    # THE EMIT: two records' chain starts — clean(1) + flagged(2).
    starts = [
        chain_start(chain, {"app_id": 1, "risk": 0.1}, map_index=1, trace_id="trace-1"),
        chain_start(chain, {"app_id": 2, "risk": 0.95}, map_index=2, trace_id="trace-2"),
    ]
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=starts,
        cursor={"page": 0},
    )

    async def run_chain_row(row_id: JobId, step_key: str, body: Any) -> object:
        """One chain row's worker pass: the claim + the ledger claim +
        the body + the ROUTED finalize (the runner's chain path's
        shape)."""
        from taskq.workflows.chain import chain_fork as _fork

        w = JobId(new_uuid())
        async with module_pg_pool.acquire() as conn:
            rec = await conn.fetchrow(
                f"UPDATE \"{wf_schema}\".jobs SET status = 'running', started_at = now(), "
                "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
                "locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds', "
                "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
                "AND deps_pending = 0 RETURNING attempt, claim_epoch, map_index, payload",
                row_id,
                w,
            )
        assert rec is not None
        attempt_n, epoch_n = int(rec["attempt"]), int(rec["claim_epoch"])
        async with module_pg_pool.acquire() as conn:
            await claim_step_ledger(
                conn,
                wf_sql,
                flow_id=flow_id,
                job_id=row_id,
                step_key=step_key,
                map_index=int(rec["map_index"]),
                attempt=attempt_n,
            )
        item_raw = rec["payload"]
        import json

        item = json.loads(item_raw) if isinstance(item_raw, str) else item_raw
        outcome = await body(None, item["wf_item"])
        child = chain.next_child(
            step_key,
            outcome,
            payload=item,
            map_index=int(rec["map_index"]),
            trace_id=f"trace-{rec['map_index']}",
        )
        fork = _fork(child, trace_id=f"trace-{rec['map_index']}")
        res = await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=row_id,
            step_key=step_key,
            worker_id=w,
            attempt=attempt_n,
            claim_epoch=epoch_n,
            outcome="succeeded",
            result={"value": outcome.value},
            fork=fork,
            map_index=int(rec["map_index"]),
        )
        assert res.applied
        return outcome

    # The two STARTS route DIFFERENTLY (the conditional edge).
    async with module_pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT id, map_index FROM "{wf_schema}".jobs '
            "WHERE parent_id = $1 AND step_key = 'screen' ORDER BY map_index",
            source_id,
        )
    assert [r["map_index"] for r in rows] == [1, 2]
    outcomes = [await run_chain_row(JobId(r["id"]), "screen", screen_app) for r in rows]
    assert outcomes == [ScreenOutcome.CLEAN, ScreenOutcome.FLAGGED]

    # The CHILDREN exist per the ROUTE (one fork each — clean → enrich,
    # flagged → manual_review), the record's identity riding forward.
    children = await wf_conn.fetch(
        f'SELECT id, step_key, map_index, trace_id, idempotency_key FROM "{wf_schema}".jobs '
        "WHERE parent_id = ANY($1) ORDER BY map_index, step_key",
        [r["id"] for r in rows],
    )
    t20_redlog.red(
        "t20-router-fork",
        "the route NOT riding the certified fork (a hand-wired child row "
        "outside the parent's terminal-mark tx — the fork-debt dragon) or "
        "the record's map_index/trace NOT riding the child (the siblings "
        "collide onto one row)",
        {
            "children": [
                {"step": r["step_key"], "map": r["map_index"], "trace": r["trace_id"]}
                for r in children
            ]
        },
    )
    assert {(r["step_key"], r["map_index"]) for r in children} == {
        ("enrich", 1),
        ("manual_review", 2),
    }, "the conditional edge routed each record to its own arm"
    assert all(r["trace_id"] == f"trace-{r['map_index']}" for r in children), (
        "the record's trace rides the fork"
    )

    # The chain ENDS at DONE (the terminal step finalizes with NO fork —
    # zero children of its own).
    enrich_row = next(r for r in children if r["step_key"] == "enrich")
    await run_chain_row(JobId(enrich_row["id"]), "enrich", enrich_app)
    grandchildren = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1',
        enrich_row["id"],
    )
    assert int(grandchildren or 0) == 0, "DONE ends the chain — no fork"


@pytest.mark.integration
async def test_t20_router_not_total_fails_the_row_loudly(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
    t20_redlog: RedLog,
) -> None:
    """THE RUNTIME REFUSAL'S ROW: a chain step whose body returns a
    FOREIGN outcome (not the declared enum) terminal-FAILS with
    ``error_class='RouterNotTotal'`` — the record NAMES the defect; the
    chain visibly dies. The silent-drop variant (the record vanishing,
    the run 'succeeding' minus one) is the convicted shape."""
    from taskq.backend._protocol import JobId as JId
    from taskq.workflows._emit import emit_batch
    from taskq.workflows.engine import finalize_node

    chain = the_chain()
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=[chain_start(chain, {"app_id": 9}, map_index=9, trace_id="trace-9")],
        cursor={"page": 0},
    )
    async with module_pg_pool.acquire() as conn:
        row_id = JId(
            await conn.fetchval(
                f"SELECT id FROM \"{wf_schema}\".jobs WHERE parent_id = $1 AND step_key = 'screen'",
                source_id,
            )
        )
    # THE BODY THAT LIED: 'corrupted' is not a ScreenOutcome.
    w = JobId(new_uuid())
    async with module_pg_pool.acquire() as conn:
        rec = await conn.fetchrow(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'running', started_at = now(), "
            "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
            "locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds' "
            "WHERE id = $1 AND status = 'pending' AND deps_pending = 0 "
            "RETURNING attempt, claim_epoch, map_index",
            row_id,
            w,
        )
    assert rec is not None
    from taskq.workflows.ledger import claim_step_ledger

    async with module_pg_pool.acquire() as conn:
        await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow_id,
            job_id=row_id,
            step_key="screen",
            map_index=int(rec["map_index"]),
            attempt=int(rec["attempt"]),
        )

    async def _run() -> None:
        try:
            chain.next_child("screen", "corrupted", payload={}, map_index=9, trace_id="trace-9")
        except RouterNotTotal as exc:
            # THE RUNNER'S LOUD REFUSAL (the runner's chain path): the
            # step terminal-FAILS with the defect's name — never a
            # silent drop.
            await finalize_node(
                module_pg_pool,
                wf_sql,
                flow_id=flow_id,
                job_id=row_id,
                step_key="screen",
                worker_id=w,
                attempt=int(rec["attempt"]),
                claim_epoch=int(rec["claim_epoch"]),
                outcome="failed",
                error_class="RouterNotTotal",
                error_message=str(exc)[:500],
                map_index=int(rec["map_index"]),
            )
            return
        raise AssertionError("NOT REFUSED — the runtime door is broken")

    await _run()

    row = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs WHERE id = $1',
        row_id,
    )
    assert row is not None
    t20_redlog.red(
        "t20-router-not-total-row",
        "the foreign outcome SILENTLY DROPPED (no arm matched, nothing "
        "refused) — the run 'succeeds' minus a record; the row's "
        "error_class must NAME the defect",
        {"status": row["status"], "error_class": row["error_class"]},
    )
    assert row["status"] == "failed", "the chain dies VISIBLY (a failed row)"
    assert row["error_class"] == "RouterNotTotal", (
        f"the record must name the defect, got {row['error_class']!r}"
    )
    assert "corrupted" in (row["error_message"] or ""), "the refusal names the outcome"


# ── THE AUTHOR SURFACE, END TO END (PROOF §4's shape, on the runner) ────


class T20ScreenOutcome(enum.Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


class T20ReviewOutcome(enum.Enum):
    APPROVE = "approve"
    REJECT = "reject"


async def t20_screen(ctx: object, item: dict[str, object]) -> T20ScreenOutcome:
    risk = float(item["risk"])  # pyright: ignore[reportArgumentType]  # Why: the pin's own item shape (the emit wrote it).
    return T20ScreenOutcome.FLAGGED if risk > 0.8 else T20ScreenOutcome.CLEAN


async def t20_review(ctx: object, item: dict[str, object]) -> T20ReviewOutcome:
    return T20ReviewOutcome.APPROVE


T20_CHAIN = Chain(
    name="t20-application-enrichment",
    start="screen",
    steps={
        "screen": Step(
            body=t20_screen,
            outcomes=T20ScreenOutcome,
            route=Route({T20ScreenOutcome.CLEAN: DONE, T20ScreenOutcome.FLAGGED: "manual_review"}),
        ),
        "manual_review": Step(
            body=t20_review,
            outcomes=T20ReviewOutcome,
            route=Route({T20ReviewOutcome.APPROVE: DONE, T20ReviewOutcome.REJECT: DONE}),
        ),
    },
)

_T20_PAGE = [
    {"app_id": 1, "risk": 0.1},
    {"app_id": 2, "risk": 0.95},
]


async def t20_source(ctx: StepContext) -> None:
    """The paged source: ONE page, emitted while the source is running
    (each yield = ONE emit tx — the children + edges + the cursor)."""
    await ctx.emit_batch(
        [
            chain_start(T20_CHAIN, item, map_index=item["app_id"], trace_id=f"app-{item['app_id']}")
            for item in _T20_PAGE
        ],
        cursor={"page": 0},
    )


@pytest.mark.integration
async def test_t20_chain_end_to_end_through_the_runner(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
    t20_redlog: RedLog,
) -> None:
    """THE WHOLE AUTHOR SURFACE (PROOF §4's ergonomics bar), lived: a
    ``chain_source`` node whose paged body emits the chain starts; the
    chain's steps route on their TYPED OUTCOMES; the run drains and the
    root derives from the rows (no join anywhere). One query per record's
    lineage: every row of a record's chain shares its trace."""
    from taskq.workflows import FlowRunner, WorkflowApp, build, chain_source

    app = WorkflowApp()

    @app.workflow("t20_chain_e2e")
    def t20_chain_e2e() -> object:
        src = chain_source(T20_CHAIN, t20_source, key="t20_source")
        return build(src)

    runner = FlowRunner(app.get("t20_chain_e2e"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await wf_conn.fetch(
        f'SELECT step_key, map_index, trace_id, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__' "
        "ORDER BY map_index, id",
        flow_id,
    )
    by_trace: dict[str, list[tuple[str, str]]] = {}
    for r in rows:
        by_trace.setdefault(str(r["trace_id"]), []).append((str(r["step_key"]), str(r["status"])))
    t20_redlog.red(
        "t20-chain-e2e",
        "the chain's rows NOT sharing one trace per record (the lineage "
        "crosses two systems) or a record's chain stranded non-terminal",
        dict(sorted(by_trace.items())),
    )
    # THE RECORD LINEAGES (the source row itself carries no record trace):
    record_traces = {k: v for k, v in by_trace.items() if k != "None"}
    assert len(record_traces) == 2, "two records, two traces — the one-query lineage"
    # app-1 (clean): screen only — DONE ended the chain.
    # app-2 (flagged): screen → manual_review — the conditional edge.
    assert sorted(k for k, _ in record_traces["app-1"]) == ["screen"]
    assert sorted(k for k, _ in record_traces["app-2"]) == ["manual_review", "screen"]
    assert all(status == "succeeded" for steps in record_traces.values() for _, status in steps), (
        "every chain row terminal-succeeded"
    )
    root = await wf_conn.fetchrow(
        f'SELECT status, finished_at FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root is not None and root["status"] == "succeeded" and root["finished_at"] is not None


async def t20_lying_screen(ctx: object, item: dict[str, object]) -> T20ScreenOutcome:
    """THE BODY THAT LIED: returns a raw string — not the declared enum."""
    return "corrupted"  # type: ignore[return-value]  # Why: the drill — the foreign outcome IS the runtime door's subject.


T20_LYING_CHAIN = Chain(
    name="t20-lying-chain",
    start="screen",
    steps={
        "screen": Step(
            body=t20_lying_screen,
            outcomes=T20ScreenOutcome,
            route=Route({T20ScreenOutcome.CLEAN: DONE, T20ScreenOutcome.FLAGGED: DONE}),
        ),
    },
)


async def t20_lying_source(ctx: StepContext) -> None:
    await ctx.emit_batch(
        [chain_start(T20_LYING_CHAIN, {"app_id": 1}, map_index=1, trace_id="app-1")],
        cursor={"page": 0},
    )


@pytest.mark.integration
async def test_t20_router_not_total_through_the_runner(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
    t20_redlog: RedLog,
) -> None:
    """THE RUNTIME LOUD REFUSAL, END TO END: a chain step whose body
    returns a foreign outcome fails the row LOUDLY —
    ``error_class='RouterNotTotal'`` on the record — and the RUN dies
    'failed' (the envelope cannot lie about a chain that died mid-route).
    The silent-drop variant is the convicted shape."""
    from taskq.workflows import FlowRunner, WorkflowApp, build, chain_source

    app = WorkflowApp()

    @app.workflow("t20_router_not_total_e2e")
    def t20_router_not_total_e2e() -> object:
        src = chain_source(T20_LYING_CHAIN, t20_lying_source, key="t20_lying_source")
        return build(src)

    runner = FlowRunner(app.get("t20_router_not_total_e2e"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)

    row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'screen'",
        flow_id,
    )
    assert row is not None
    t20_redlog.red(
        "t20-router-not-total-runner",
        "the foreign outcome silently dropped by the runner's chain path "
        "— the run 'succeeds' minus a record; the row must terminal-FAIL "
        "with error_class='RouterNotTotal'",
        {"status": row["status"], "error_class": row["error_class"]},
    )
    assert row["status"] == "failed"
    assert row["error_class"] == "RouterNotTotal"
    root = await wf_conn.fetchrow(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root is not None and root["status"] == "failed", (
        "the run's derivation must not claim success over a chain that died mid-route"
    )
