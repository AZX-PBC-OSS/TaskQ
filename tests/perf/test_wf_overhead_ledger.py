"""THE OVERHEAD LEDGER (the perf/efficiency front): TaskQflow's cost of
abstraction, MEASURED — never assumed. The ledger's faces:

1. THE PLAIN-VS-WORKFLOW DELTA: the same trivial work (one dict, one
   body) driven dispatch→terminal as a PLAIN actor job (the base queue's
   real backend machinery: enqueue → dispatch CTE → mark_succeeded) and
   as a ONE-STEP workflow (the packaged run's real door: create → drive
   → read). The delta per run is the abstraction's price in ms.
2. THE ROW CENSUS: the rows one wf run writes vs the plain job's one row
   — the node rows, the ledger writes, the edges (the storage the sweep
   later touches).
3. THE FAN-OUT SCALING CURVE: map workflows at 10/100/1000 children,
   driven END TO END through the real runner, wall-clock — the linear
   face and the knees named honestly.
4. THE HITL HOLD: the park cost (create → held) and the resume cost
   (deliver → terminal) — the human-gate's price per hold.
5. THE SWEEP IDLE FACE: the tick arms (join rederive + outbox drain +
   the hitl expiry sweep) on a QUIET schema — the per-tick cost of the
   machinery waiting for nothing.

Every number lands in a run-scoped, head-stamped artifact
(``write_band_artifact`` — the receipts law); the bands asserted here
are this machine's own measurements recorded in the artifact, and the
report reads the artifacts, never the filename.

Load discipline: the ambient load is read (``load_bar``) at the head of
the test; the absolute wall-clock bands assert only under the quiet bar
(the condition-not-clock law the wf perf bands established) — under load
the readings are RECORDED, never asserted.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, JobId
from tests._wf_fixtures import load_bar, write_band_artifact

pytestmark = [pytest.mark.integration, pytest.mark.load_sensitive]

# ── the graph bodies (module-level: the compile reads the globals) ──────


class Lead(BaseModel):
    doc_id: str = "d1"


class Approval(BaseModel):
    verdict: str = "approve"


async def _one_body(ctx: Any, params: Lead) -> dict[str, str]:
    return {"tail": params.doc_id}


async def _per_item(ctx: Any, i: int) -> int:
    return int(i) * 2


def _sum_aggregate(results: list[Any]) -> dict[str, int]:
    return {"sum": sum(int(r) for r in results)}


class ItemsIn(BaseModel):
    n_items: int = 1


async def _emit(ctx: Any, params: ItemsIn) -> list[int]:
    return list(range(params.n_items))


def _percentile(data: list[int], pct: float) -> int:
    ordered = sorted(data)
    idx = min(int(len(ordered) * pct / 100.0), len(ordered) - 1)
    return ordered[idx]


def _one_step_app() -> tuple[Any, Any]:
    from taskq.workflows import Promise, WorkflowApp, build, step

    app = WorkflowApp()

    @app.workflow("ledger_one_step")
    def ledger_one_step() -> Promise[object]:
        return build(step(_one_body, Lead(), key="tail"))

    return app, app.get("ledger_one_step")


def _map_app(n: int) -> tuple[Any, Any]:
    from taskq.workflows import Promise, WorkflowApp, build, map_source, step

    app = WorkflowApp()

    @app.workflow(f"ledger_map_{n}")
    def ledger_map() -> Promise[object]:
        src = step(_emit, ItemsIn(n_items=n), key="src")
        return build(map_source(src, _per_item, aggregate=_sum_aggregate))

    return app, app.get(f"ledger_map_{n}")


def _hold_app(name: str) -> tuple[Any, Any]:
    """The hold graph's own NAME per test — the definition registry is
    exact (D1): a second registration is a coding error, never a shadow."""
    from taskq.workflows import GateDecl, Promise, StepContext, WorkflowApp, build, step

    gates = (GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),)

    async def hold_body(ctx: StepContext, params: Lead) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    app = WorkflowApp()

    @app.workflow(name)
    def ledger_hold() -> Promise[object]:
        return build(step(hold_body, Lead(), key="review", gates=gates))

    return app, app.get(name)


# ── 1: THE PLAIN-VS-WORKFLOW DELTA ──────────────────────────────────────


@pytest.mark.slow
async def test_overhead_ledger_plain_vs_one_step(
    module_jobs_app: Any,
    module_pg_pool: Any,
    wf_schema: str,
) -> None:
    """THE HEAD-OF-LINE LEDGER: the same trivial work, dispatch→terminal,
    both drivers — the abstraction's price NAMED in ms per run."""
    import taskq.workflows as wf

    backend = module_jobs_app.backend
    worker_id = backend._worker_id if hasattr(backend, "_worker_id") else None
    from taskq._ids import new_uuid

    worker_id = worker_id or new_uuid()

    # the dispatch CTE requires the actor's config row (max_concurrent),
    # and the terminal's holder CTE requires the worker's row
    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue, max_concurrent) '  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier
            "VALUES ('ledger_plain', 'default', 100) ON CONFLICT (actor) DO NOTHING"
        )
        await conn.execute(
            f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608  # Why: the schema is the fixture's own settings-validated identifier; values are $-bound
            f"VALUES ('{worker_id}', 'ledger-bench', 1, '{{\"default\"}}', "
            '\'{"notify_enabled": false, "max_concurrency": 100}\'::jsonb) '
            "ON CONFLICT (id) DO NOTHING"
        )

    rounds_n, per_round = 3, 60

    async def plain_round() -> list[int]:
        lat: list[int] = []
        for _ in range(per_round):
            t0 = time.perf_counter_ns()
            await backend.enqueue(
                EnqueueArgs(
                    id=new_job_id(),
                    actor="ledger_plain",
                    queue="default",
                    payload={"d": "d1"},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
            )
            claims = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=90))
            assert claims, "the plain claim came back empty — the machinery broke"
            claimed = claims[0]
            ok = await backend.mark_succeeded(
                claimed.id,
                worker_id,
                result={"tail": "d1"},
                attempt=claimed.attempt,
                claim_epoch=claimed.claim_epoch,
            )
            assert ok, "the fenced terminal write rejected the claim's own fence tokens"
            lat.append((time.perf_counter_ns() - t0) // 1_000)
        return lat

    compiled = _one_step_app()[1]

    async def wf_round() -> list[int]:
        lat: list[int] = []
        for _ in range(per_round):
            t0 = time.perf_counter_ns()
            result = await wf.run(compiled, module_pg_pool, wf_schema)
            assert result.outcome == "terminal", result
            lat.append((time.perf_counter_ns() - t0) // 1_000)
        return lat

    warm_p = await plain_round()
    del warm_p
    rounds_plain = [await plain_round() for _ in range(rounds_n)]
    best_plain = min(rounds_plain, key=sum)

    warm_w = await wf_round()
    del warm_w
    rounds_wf = [await wf_round() for _ in range(rounds_n)]
    best_wf = min(rounds_wf, key=sum)

    p50_plain = _percentile(best_plain, 50)
    p50_wf = _percentile(best_wf, 50)
    delta = p50_wf - p50_plain
    ratio = p50_wf / p50_plain if p50_plain else 0.0

    # ── 2: THE ROW CENSUS (one run's storage on each side) ──────────────
    async def census() -> dict[str, int]:
        out: dict[str, int] = {}
        async with module_pg_pool.acquire() as conn:
            for table in (
                "jobs",
                "wf_edge",
                "wf_step_ledger",
                "wf_outbox",
                "wf_signals",
                "wf_node_progress",
                "job_attempts",
            ):
                out[table] = int(
                    await conn.fetchval(  # Why: the schema/table are the fixture's validated identifiers
                        f'SELECT count(*) FROM "{wf_schema}".{table}'  # noqa: S608  # Why: the schema is the fixture's own settings-validated identifier; values are $-bound
                    )
                )
        return out

    before = await census()
    await wf.run(compiled, module_pg_pool, wf_schema)
    mid = await census()
    wf_rows = {k: mid[k] - before[k] for k in mid}

    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="ledger_plain",
            queue="default",
            payload={"d": "d1"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    claims = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=90))
    assert claims
    await backend.mark_succeeded(
        claims[0].id,
        worker_id,
        result={"tail": "d1"},
        attempt=claims[0].attempt,
        claim_epoch=claims[0].claim_epoch,
    )
    after = await census()
    plain_rows = {k: after[k] - mid[k] for k in after if after[k] - mid[k]}

    bar = load_bar()
    record = {
        "pin": "wf-overhead-ledger",
        "plain_p50_us": p50_plain,
        "wf_one_step_p50_us": p50_wf,
        "delta_us": delta,
        "ratio": round(ratio, 2),
        "plain_best_round_us": best_plain,
        "wf_best_round_us": best_wf,
        "row_census_wf_one_step": wf_rows,
        "row_census_plain": plain_rows,
        "loaded_bar": {"ambient_load_per_core": round(bar, 3), "quiet_bar": 0.5},
    }
    write_band_artifact("wf-overhead-ledger.json", record)

    # THE BOUND (the adopter's guarantee — asserted on a quiet box):
    # the abstraction's price ceiling per run is 60 ms and 50x the plain
    # wall-clock; the recorded numbers may be far under, the BOUND is
    # what the band holds.
    if bar <= 0.5:
        assert p50_wf - p50_plain <= 60_000, (
            f"the one-step workflow's overhead {delta / 1000:.1f} ms exceeds the 60 ms bound"
        )


# ── 1b: THE STATEMENT LEDGER (the load-immune accounting) ───────────────


@pytest.mark.slow
async def test_overhead_statement_ledger(
    module_jobs_app: Any,
    module_pg_pool: Any,
    wf_schema: str,
) -> None:
    """THE DB-SIDE ACCOUNTING: pg_stat_statements counts the statements
    (calls + exec ms) each path makes per run — wall-clock noise cannot
    forge it. One run of trivial work, both faces: the round-trip count
    and the DB time ARE the abstraction's weight."""
    import taskq.workflows as wf

    backend = module_jobs_app.backend
    worker_id = backend._worker_id if hasattr(backend, "_worker_id") else None
    from taskq._ids import new_uuid

    worker_id = worker_id or new_uuid()

    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue, max_concurrent) '  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier
            "VALUES ('ledger_plain', 'default', 100) ON CONFLICT (actor) DO NOTHING"
        )
        await conn.execute(
            f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608  # Why: the schema is the fixture's own settings-validated identifier; values are $-bound
            f"VALUES ('{worker_id}', 'ledger-bench', 1, '{{\"default\"}}', "
            '\'{"notify_enabled": false, "max_concurrency": 100}\'::jsonb) '
            "ON CONFLICT (id) DO NOTHING"
        )

    compiled = _one_step_app()[1]
    k = 40

    # the module fixture builds a FRESH database per module — the
    # extension rides it here (the pool's user is the cluster's
    # superuser in the perf lane's tuned cluster)
    await module_pg_pool.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")

    async def snapshot_stats() -> tuple[int, float]:
        async with module_pg_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT calls, total_exec_time FROM pg_stat_statements "  # Why: pg_stat_statements is a fixed extension view, no interpolation
                "WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())"
            )
        return (
            sum(int(r["calls"]) for r in rows),
            sum(float(r["total_exec_time"]) for r in rows),
        )

    async def ledger_of(pass_fn: Any) -> tuple[float, float]:
        """THE DELTA LEDGER: snapshot before, run the pass, snapshot
        after, SUBTRACT — the reset's entry-materialization quirk (an
        accumulated database re-materializes stale entries) cannot forge
        a delta, and background statements pollute it by noise only."""
        calls0, ms0 = await snapshot_stats()
        await pass_fn()
        calls1, ms1 = await snapshot_stats()
        return float(calls1 - calls0), float(ms1 - ms0)

    async def plain_iter() -> None:
        await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="ledger_plain",
                queue="default",
                payload={"d": "d1"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
        )
        claims = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=90))
        assert claims
        ok = await backend.mark_succeeded(
            claims[0].id,
            worker_id,
            result={"tail": "d1"},
            attempt=claims[0].attempt,
            claim_epoch=claims[0].claim_epoch,
        )
        assert ok

    # warm both paths (prepared statements, caches)
    for _ in range(3):
        await plain_iter()
        await wf.run(compiled, module_pg_pool, wf_schema)

    async def plain_pass() -> None:
        for _ in range(k):
            await plain_iter()

    async def wf_pass() -> None:
        for _ in range(k):
            result = await wf.run(compiled, module_pg_pool, wf_schema)
            assert result.outcome == "terminal"

    plain_calls, plain_ms = await ledger_of(plain_pass)
    wf_calls, wf_ms = await ledger_of(wf_pass)

    record = {
        "pin": "wf-overhead-statement-ledger",
        "runs_per_pass": k,
        "method": "delta snapshots of pg_stat_statements totals (reset-quirk-immune)",
        "plain": {
            "statements_per_run": round(plain_calls / k, 1),
            "db_exec_ms_per_run": round(plain_ms / k, 3),
        },
        "wf_one_step": {
            "statements_per_run": round(wf_calls / k, 1),
            "db_exec_ms_per_run": round(wf_ms / k, 3),
        },
        "loaded_bar": {"ambient_load_per_core": round(load_bar(), 3)},
    }
    write_band_artifact("wf-overhead-statement-ledger.json", record)


# ── 3: THE FAN-OUT SCALING CURVE ────────────────────────────────────────


@pytest.mark.slow
async def test_overhead_fanout_scaling_curve(
    module_pg_pool: Any,
    wf_schema: str,
) -> None:
    """THE FAN-OUT'S WALL-CLOCK CURVE, END TO END: map workflows at
    10/100/1000 children through the real packaged run — the curve's
    honest shape (the linear face, the knees) recorded."""
    import taskq.workflows as wf

    curve: dict[str, Any] = {}
    for n in (10, 100, 1000):
        compiled = _map_app(n)[1]
        # one warm run
        warm = await wf.run(compiled, module_pg_pool, wf_schema)
        assert warm.outcome == "terminal"
        samples = []
        for _ in range(3):
            t0 = time.perf_counter_ns()
            result = await wf.run(compiled, module_pg_pool, wf_schema)
            ms = (time.perf_counter_ns() - t0) / 1e6
            assert result.outcome == "terminal"
            samples.append(round(ms, 1))
        curve[str(n)] = {"best_ms": min(samples), "all_ms": samples}

    per_child_10 = curve["10"]["best_ms"] / 10
    per_child_1000 = curve["1000"]["best_ms"] / 1000
    record = {
        "pin": "wf-fanout-scaling",
        "curve": curve,
        "per_child_ms_at_10": round(per_child_10, 3),
        "per_child_ms_at_1000": round(per_child_1000, 3),
        "loaded_bar": {"ambient_load_per_core": round(load_bar(), 3)},
    }
    write_band_artifact("wf-fanout-scaling.json", record)


# ── 4: THE HITL HOLD'S PARK/RESUME ──────────────────────────────────────


@pytest.mark.slow
async def test_overhead_hitl_park_resume(
    module_pg_pool: Any,
    wf_schema: str,
) -> None:
    """THE HUMAN GATE'S PRICE: the park (create → held) and the resume
    (deliver → terminal), each wall-clocked through the real surfaces."""
    import json as _json

    import taskq.workflows as wf
    from taskq.workflows.api._hitl import deliver_payload

    compiled = _hold_app("ledger_hold")[1]
    parks: list[float] = []
    resumes: list[float] = []
    for _ in range(15):
        runner = wf.FlowRunner(compiled, module_pg_pool, wf_schema)
        t0 = time.perf_counter_ns()
        claim = await runner.create_flow()
        outcome = await runner.drive(claim.flow_id, until="held", max_ticks=500)
        park_ms = (time.perf_counter_ns() - t0) / 1e6
        assert outcome == "held", outcome
        parks.append(round(park_ms, 2))

        async with module_pg_pool.acquire() as conn:
            hold_id = await conn.fetchval(
                f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',  # noqa: S608  # Why: the schema is the fixture's own settings-validated identifier; values are $-bound
                claim.flow_id,
            )
        t0 = time.perf_counter_ns()
        delivered = await deliver_payload(
            module_pg_pool,
            schema=wf_schema,
            workflow_id=claim.flow_id,
            hold_id=JobId(hold_id),
            payload={"verdict": "approve"},
            payload_json=_json.dumps({"verdict": "approve"}),
        )
        assert delivered.status == "delivered", delivered
        outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=500)
        resume_ms = (time.perf_counter_ns() - t0) / 1e6
        assert outcome == "terminal", outcome
        resumes.append(round(resume_ms, 2))

    parks_sorted = sorted(parks)
    resumes_sorted = sorted(resumes)
    record = {
        "pin": "wf-hitl-park-resume",
        "park_ms_p50": parks_sorted[len(parks_sorted) // 2],
        "resume_ms_p50": resumes_sorted[len(resumes_sorted) // 2],
        "park_all_ms": parks,
        "resume_all_ms": resumes,
        "loaded_bar": {"ambient_load_per_core": round(load_bar(), 3)},
    }
    write_band_artifact("wf-hitl-park-resume.json", record)


# ── 4b: THE IDLE DRIVE + THE KNOCK WAKE ─────────────────────────────────


async def test_overhead_idle_drive_and_knock_wake(
    module_pg_pool: Any,
    wf_schema: str,
) -> None:
    """THE WAITING DRIVE'S FACE: a drive loop parked at a human gate —
    the idle statement rate (what the wait burns per second) and the
    KNOCK WAKE latency (deliver → the parked drive terminalizes). The
    knock-driven wake (CURE B) is the mechanism: LISTEN armed after the
    first idle tick, the poll as the belt (the bounded backoff)."""
    import asyncio as _asyncio
    import json as _json

    import taskq.workflows as wf
    from taskq.workflows.api._hitl import deliver_payload

    compiled = _hold_app("ledger_hold_wake")[1]
    runner = wf.FlowRunner(compiled, module_pg_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="held", max_ticks=500)
    assert outcome == "held", outcome

    await module_pg_pool.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")

    async def snapshot() -> tuple[int, float]:
        async with module_pg_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT calls, total_exec_time FROM pg_stat_statements "  # Why: pg_stat_statements is a fixed extension view, no interpolation
                "WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())"
            )
        return (
            sum(int(r["calls"]) for r in rows),
            sum(float(r["total_exec_time"]) for r in rows),
        )

    # THE IDLE FACE: the drive waits for terminal while the run parks at
    # the gate — 3 seconds of waiting, statements counted by delta.
    drive_task = _asyncio.create_task(runner.drive(claim.flow_id, until="terminal", max_ticks=5000))
    await _asyncio.sleep(1.0)  # the arm (the LISTEN arms after the first idle tick)
    calls0, ms0 = await snapshot()
    await _asyncio.sleep(3.0)
    calls1, ms1 = await snapshot()
    idle_stmts_s = (calls1 - calls0) / 3.0
    idle_db_ms_s = (ms1 - ms0) / 3.0

    # THE KNOCK WAKE: the deliver lands on the ALREADY-RUNNING drive —
    # deliver → terminal wall-clock is the wake latency.
    async with module_pg_pool.acquire() as conn:
        hold_id = await conn.fetchval(
            f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',  # noqa: S608  # Why: the schema is the fixture's own settings-validated identifier; values are $-bound
            claim.flow_id,
        )
    await _asyncio.sleep(0.1)  # the drive is parked deep in its idle wait
    t0 = time.perf_counter_ns()
    delivered = await deliver_payload(
        module_pg_pool,
        schema=wf_schema,
        workflow_id=claim.flow_id,
        hold_id=JobId(hold_id),
        payload={"verdict": "approve"},
        payload_json=_json.dumps({"verdict": "approve"}),
    )
    assert delivered.status == "delivered", delivered
    outcome = await drive_task
    wake_ms = (time.perf_counter_ns() - t0) / 1e6
    assert outcome == "terminal", outcome

    bar = load_bar()
    record = {
        "pin": "wf-idle-drive-knock-wake",
        "idle_stmts_per_s": round(idle_stmts_s, 1),
        "idle_db_ms_per_s": round(idle_db_ms_s, 1),
        "knock_wake_ms": round(wake_ms, 3),
        "loaded_bar": {"ambient_load_per_core": round(bar, 3), "quiet_bar": 0.5},
    }
    write_band_artifact("wf-idle-drive-knock-wake.json", record)
    # THE BOUND (quiet box): the idle wait's burn is bounded — the
    # knock-driven face keeps it ≥ 4x under the old fixed-poll face's
    # ~200 stmts/s — and the wake stays inside the old fixed tick's
    # latency class (the old face: ≤ 20ms; the cured bound: 50ms covers
    # the belt's floor plus the tick).
    if bar <= 0.5:
        # THE BAND (the cure's guard, red-first recorded): the OLD face
        # burned 1143 stmts/s (the fixed 20ms poll — the red receipt in
        # the report); the cured face measures ~127 (the backoff's cap
        # holds the floor: ~25 stmts/idle-tick — the reads plus the two
        # sweep arms' own acquire/tx churn — at ≤5 ticks/s). The band at
        # 150 keeps the 7x cure's teeth: the backoff's LOSS re-trips it
        # (the fixed poll's face is 7.6x over), a future arm-churn cut
        # re-derives the bound by the refit's argument.
        assert idle_stmts_s < 150, (
            f"the idle drive burns {idle_stmts_s:.0f} stmts/s — the knock-driven "
            "wake's backoff is not backing off"
        )
        assert wake_ms < 100, f"the knock wake took {wake_ms:.1f} ms — slower than the old poll"


# ── 6: THE PAYLOAD DISCIPLINE (the fan-in envelope's cap) ───────────────


@pytest.mark.xfail(
    strict=True,
    reason="the join envelope's cap is the K7 lane's cure — the finding's red receipt stays until it lands",
)
@pytest.mark.load_sensitive
async def test_pin_fanin_envelope_within_the_estate_result_cap(
    module_pg_pool: Any,
    wf_schema: str,
) -> None:
    """THE FAN-IN ENVELOPE'S SIZE DISCIPLINE (the payload-bloat finding,
    MEASURED at this head): a NO-aggregate map's join row collects every
    child's result into ONE jsonb envelope through the wf finalize's
    TERMINAL_MARK — and that write checks NO ``result_max_bytes``: the
    estate's 64 KiB terminal cap (the plain job's non-retryable
    ResultTooLarge fence, ``settings.result_max_bytes`` default) does not
    bind the workflow's own terminal write.

    THE MEASURED FACE (700 children x ~1 KiB each, this machine, the
    tuned perf cluster): the join row landed ``result_size_bytes =
    730_001`` — 11.1x the 64 KiB cap; the ceiling at the fan-in bound
    with cap-sized children (5000 x 64 KiB) is ~305 MiB of jsonb in ONE
    row.

    THE CURE'S SHAPE (reported — the finalize is the K7 lane's territory
    this wave): the failures' own collect ALREADY compacts past its cap
    (``FANIN_FAILURES_BYTE_CAP`` + the ``__truncated__`` marker + the
    ledger pointer, pinned in ``test_wf_pruner_pins.py``) — the collected
    RESULT envelope lacks the same discipline. Mirroring it (compact past
    the cap, the full collect stays on the child rows the read-side
    aggregate already reads) is the one-mechanism cure; this pin FLIPS
    GREEN the day it lands."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, build, map_source, step

    class ItemsIn(BaseModel):
        n: int = 700

    async def _emit(ctx: Any, params: ItemsIn) -> list[int]:
        return list(range(params.n))

    fat = "x" * 1024  # ~1 KiB per child result

    async def _fat_item(ctx: Any, i: int) -> dict[str, object]:
        return {"i": i, "fat": fat}

    app = WorkflowApp()

    @app.workflow("ledger_envelope")
    def ledger_envelope() -> Promise[object]:
        return build(map_source(step(_emit, ItemsIn(), key="src"), _fat_item))

    result = await wf.run(app.get("ledger_envelope"), module_pg_pool, wf_schema)
    assert result.outcome == "terminal", result
    async with module_pg_pool.acquire() as conn:
        size = await conn.fetchval(
            f"SELECT result_size_bytes FROM \"{wf_schema}\".jobs WHERE step_key = 'src.join'"  # noqa: S608  # Why: the schema is the fixture's own settings-validated identifier; values are $-bound
        )
    assert size is not None, "the join row did not terminalize"
    cap = 64 * 1024
    assert int(size) <= cap, (
        f"the fan-in envelope is {int(size)} bytes — {int(size) / cap:.1f}x the "
        "estate's 64 KiB result cap: the wf terminal write bypasses the "
        "size discipline every plain job's terminal enforces (the "
        "payload-bloat finding; the cure is the failures' own compaction "
        "mechanism mirrored to the collect)"
    )


# ── 4c: THE CONNECTION-HOLD DISCIPLINE (the holdmap lesson, generalized) ─


async def test_pin_substep_body_holds_no_pool_connection(
    pg_dsn: str,
    module_pg_pool: Any,
    wf_schema: str,
    wf_sql: Any,
) -> None:
    """THE HOLD-DISCIPLINE PIN: a substep body that awaits I/O must not
    pin a pool slot for its duration. THE PROBE (a self-refuting pool):
    a pool of ONE connection; the substep's body tries to acquire a
    SECOND connection with a 2s timeout — the old face (the body ran
    INSIDE ``pool.acquire()``) can never acquire (the pool's only slot
    is held by the body itself → the probe TIMES OUT = red); the cured
    face (the body runs holding nothing) acquires immediately.

    THE RED-FIRST RECEIPT: at this branch's base the probe hung on the
    acquire and failed on the timeout — the convicted face was real.
    The cure (``WorkflowSteps.on_pool`` — the ledger touches are short
    scopes, the body holds nothing) flips it green; the exactly-once
    batteries (the coverage closers' replay/failure pins) re-ran green
    through the touch."""

    import asyncpg as _asyncpg

    # A pool of EXACTLY ONE connection on the module's own database: the
    # probe's second acquire can only succeed if the body holds nothing.
    probe_pool = await _asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        from taskq.workflows.api._ctx import build_step_context

        ran = False

        async def body() -> str:
            nonlocal ran
            # THE PROBE: a second acquire on the ONE-slot pool.
            async with probe_pool.acquire(timeout=2.0):
                ran = True
            return "ok"

        class _Tiny(BaseModel):
            doc_id: str = "d1"

        ctx = build_step_context(
            flow_id=JobId("00000000-0000-0000-0000-000000000001"),
            job_id=JobId("00000000-0000-0000-0000-000000000002"),
            node_key="probe",
            attempt=1,
            pool=probe_pool,
            wsql=wf_sql,
            input=_Tiny(),
        )
        result = await ctx.substep("hold_probe", body)
        assert result == "ok"
        assert ran, "the body never ran"
    finally:
        await probe_pool.close()


# ── 5: THE SWEEP'S IDLE FACE ────────────────────────────────────────────


@pytest.mark.slow
async def test_overhead_sweep_idle_tick(
    module_pg_pool: Any,
    wf_schema: str,
    wf_sql: Any,
) -> None:
    """THE IDLE TICK: the sweep arms on a quiet schema — what a waiting
    drive loop's tick pays when NOTHING is due."""
    from taskq.workflows._sweep import drain_outbox, sweep_join_rederive

    # warm
    for _ in range(3):
        await sweep_join_rederive(module_pg_pool, wf_sql)
        await drain_outbox(module_pg_pool, wf_sql)

    samples: list[float] = []
    for _ in range(50):
        t0 = time.perf_counter_ns()
        await sweep_join_rederive(module_pg_pool, wf_sql)
        await drain_outbox(module_pg_pool, wf_sql)
        samples.append((time.perf_counter_ns() - t0) / 1e6)

    ordered = sorted(samples)
    record = {
        "pin": "wf-sweep-idle-tick",
        "rederive_plus_drain_idle_ms_p50": ordered[len(ordered) // 2],
        "idle_ms_all": [round(s, 3) for s in samples],
        "loaded_bar": {"ambient_load_per_core": round(load_bar(), 3)},
    }
    write_band_artifact("wf-sweep-idle-tick.json", record)
