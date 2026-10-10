"""THE CROSS-RUN STEP CACHE's pins (T25) — integration: real PG.

THE LAW THE PINS HOLD (the two-claims law — the design's spine):

* the ARBITER dedups CONCURRENT (the run-key claim, T05's ledger);
* the CACHE dedups TEMPORAL (a LATER run over the same body + the same
  input reads the EARLIER run's result — the body never runs).

The cache is OPT-IN per step (``step(..., cache=True)``), stores on
TERMINAL-SUCCEEDED ONLY (a failed run's key never squats the address —
the failed-run-squats-the-key bug closed by construction), delivers a
HIT through the ordinary fenced finalize (the consumer's downstream
decodes the SAME typed shape — R3's face), carries THE RECEIPT on the
node row's metadata (the address + the producing run's id), and expires
in-DB (the TTL's freshness leg; the sweep's retention arm prunes the
expired rows). The address is the Nix-style recursive hash: the body's
§22.1 code-version canon + the decoded input's canonical jsonb — a
body's code change IS a new address (the stale-code face is dead by
construction). The cache dedups TEMPORAL, never CONCURRENT: two
concurrent misses both run their bodies and the CAS (ON CONFLICT — a
fresh winner never loses its row; an expired corpse is re-filled) yields
ONE winner — the loser's re-read picks the winner's payload.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    step,
)


def _meta(row: asyncpg.Record) -> dict[str, Any]:
    """The row's metadata dict (asyncpg returns jsonb as str on an
    uncoded connection — the estate's parse seam)."""
    raw = row["metadata"]
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


# ── the pinned graph's bodies (module-level: the annotations must
#    resolve — the compile's hint resolution reads the defining globals).
#    The CALLS ledger is the pin's instrument: it counts BODY EXECUTIONS,
#    the cache's entire point. ──

CALLS: dict[str, int] = {}


class Doc(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


async def _ocr(ctx: StepContext, d: Doc) -> Report:
    CALLS["ocr"] = CALLS.get("ocr", 0) + 1
    return Report(ref=d.doc_id)


async def _ocr_v2(ctx: StepContext, d: Doc) -> Report:
    # The CODE CHANGE face: same signature, same behavior, DIFFERENT
    # source — the address must move (the stale-code face is dead).
    CALLS["ocr_v2"] = CALLS.get("ocr_v2", 0) + 1
    ref = d.doc_id
    return Report(ref=ref)


async def _consume(ctx: StepContext, r: Report) -> str:
    # THE TYPED CONTRACT (R3's face): the body's declared param IS the
    # decode's target — a cache hit's jsonb must arrive the model
    # INSTANCE, never a raw dict. The AssertionError fails the run —
    # the typed-decode pins read the run's own status.
    assert isinstance(r, Report), (
        f"the consumer received {type(r).__name__!r}, not the declared Report"
    )
    CALLS["consume"] = CALLS.get("consume", 0) + 1
    return r.ref


async def _consume_list(ctx: StepContext, a: Report, b: Report) -> str:
    return a.ref + b.ref


async def _always_fails(ctx: StepContext, d: Doc) -> Report:
    CALLS["always_fails"] = CALLS.get("always_fails", 0) + 1
    raise RuntimeError("the body never succeeds — the failure path's own body")


_FLOW = "step_cache_flow"
_V2_FLOW = "step_cache_v2_flow"
_FAIL_FLOW = "step_cache_fail_flow"


def _cache_app() -> tuple[WorkflowApp, Any]:
    app = WorkflowApp()

    @app.workflow(_FLOW)
    def cache_flow() -> Promise[object]:
        o = step(_ocr, Doc(doc_id="d1"), key="ocr", cache=True)
        return build(step(_consume, o, key="consume"))

    @app.workflow(_V2_FLOW)
    def v2_flow() -> Promise[object]:
        o = step(_ocr_v2, Doc(doc_id="d1"), key="ocr", cache=True)
        return build(step(_consume, o, key="consume"))

    @app.workflow(_FAIL_FLOW)
    def fail_flow() -> Promise[object]:
        o = step(_always_fails, Doc(doc_id="d1"), key="ocr", cache=True, max_attempts=1)
        return build(step(_consume, o, key="consume"))

    return app, app.get(_FLOW)


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    CALLS.clear()


async def _run_flow(
    compiled: Any,
    pool: asyncpg.Pool,
    schema: str,
    *,
    run_key: str,
    expect: str = "succeeded",
) -> JobId:
    runner = FlowRunner(compiled, pool, schema)
    claim = await runner.create_flow(input=None, run_key=run_key)
    assert claim.created
    outcome = await runner.drive(claim.flow_id)
    assert outcome == "terminal", outcome
    status = await runner._flow_status(claim.flow_id)
    assert status == expect, f"run {run_key!r} ended {status!r}, expected {expect!r}"
    return claim.flow_id


async def _node_row(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, step_key: str
) -> asyncpg.Record:
    row = await conn.fetchrow(
        f"SELECT id, status::text AS status, result, metadata "
        f'FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        step_key,
    )
    assert row is not None, f"no {step_key!r} node row for run {flow_id}"
    return row


async def _cache_rows(conn: asyncpg.Connection, schema: str) -> list[asyncpg.Record]:
    return await conn.fetch(f'SELECT * FROM "{schema}".wf_step_cache')


# ── THE HIT PIN ─────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_hit_the_body_runs_once_across_runs(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE HIT PATH (the cache's entire point): the SAME input, the
    second run — the body runs ONCE across BOTH runs. The second run's
    step row carries THE RECEIPT (the address + the producing run's
    id), and its consumer sees the SAME typed result — the body NEVER
    runs, the cached payload delivers through the ordinary finalize."""
    _app, compiled = _cache_app()
    run_a = await _run_flow(compiled, wf_pool, wf_schema, run_key="hit:a")
    assert CALLS["ocr"] == 1
    row_a = await _node_row(wf_conn, wf_schema, run_a, "ocr")
    assert _meta(row_a).get("wf_cache_hit") is None, (
        "the FIRST run must be a MISS (its body paid for the result)"
    )

    run_b = await _run_flow(compiled, wf_pool, wf_schema, run_key="hit:b")
    assert CALLS["ocr"] == 1, (
        f"the SECOND run re-executed the body (calls={CALLS['ocr']}) — the "
        "cache hit never happened: the cross-run dedup is dead"
    )
    row_b = await _node_row(wf_conn, wf_schema, run_b, "ocr")
    receipt = _meta(row_b).get("wf_cache_hit")
    assert isinstance(receipt, dict), (
        f"the hit run's node row carries NO receipt (metadata={row_b['metadata']!r})"
    )
    assert receipt.get("run_id") == str(run_a), (
        f"the receipt's run_id {receipt.get('run_id')!r} is not the "
        f"producing run's id ({run_a}) — the receipt does not name who paid"
    )
    assert isinstance(receipt.get("address"), str) and receipt["address"], (
        "the receipt's address is missing — the cache hit is unauditable"
    )
    # The SAME result envelope: the consumer sees the same thing.
    assert row_b["result"] == row_a["result"]

    # The consumer ran in BOTH runs (it is uncached) and decoded the hit.
    assert CALLS["consume"] == 2


# ── THE SUCCESS-ONLY PIN ────────────────────────────────────────────────


@pytest.mark.integration
async def test_success_only_a_failed_body_never_squats_the_key(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE SUCCESS-ONLY LAW: a FAILING body's key never squats the
    address — the failure path writes NOTHING. The second run
    re-EXECUTES the body (never a cached failure): the address is free,
    the body's own attempt is the only truth."""
    _app, _compiled = _cache_app()
    fail_compiled = _app.get(_FAIL_FLOW)

    run_a = await _run_flow(fail_compiled, wf_pool, wf_schema, run_key="fail:a", expect="failed")
    assert CALLS["always_fails"] == 1
    squatters = await _cache_rows(wf_conn, wf_schema)
    assert squatters == [], (
        f"the FAILED run wrote {len(squatters)} cache row(s) — a cached "
        "failure is a lie the next run would read as truth (the "
        "failed-run-squats-the-key bug)"
    )

    # The second run: the body RE-EXECUTES (calls == 2) — the failure
    # never cached, and nothing else fills the address either.
    await _run_flow(fail_compiled, wf_pool, wf_schema, run_key="fail:b", expect="failed")
    assert CALLS["always_fails"] == 2, (
        f"the second run did not re-execute the body (calls={CALLS['always_fails']}) — "
        "a cached failure would be the only way to skip it"
    )
    assert await _cache_rows(wf_conn, wf_schema) == []
    assert run_a is not None  # the first run's record stands (the rows own it)


# ── THE TTL PIN ─────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_expired_ttl_is_a_miss(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE TTL'S FRESHNESS LEG: an expired row IS a miss — the second
    run re-executes the body and RE-FILLS the cache (a fresh
    expires_at). The expiry comparison is the DB clock's; the re-fill
    beats the corpse (the CAS's DO UPDATE arm is gated on the EXPIRED
    row — a fresh winner never loses its row)."""
    _app, compiled = _cache_app()
    await _run_flow(compiled, wf_pool, wf_schema, run_key="ttl:a")
    assert CALLS["ocr"] == 1

    # THE SCALED CLOCK: backdate the row's expiry (the DB clock's own
    # comparison — no sleeping).
    backdated = await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_step_cache '
        "SET expires_at = clock_timestamp() - interval '1 second'"
    )
    assert backdated == "UPDATE 1", "the first run never filled the cache"

    await _run_flow(compiled, wf_pool, wf_schema, run_key="ttl:b")
    assert CALLS["ocr"] == 2, (
        f"the expired row was served as a HIT (calls={CALLS['ocr']}) — the "
        "TTL's freshness leg is dead: stale truth served as fresh"
    )
    # The re-run RE-FILLS: exactly one row, freshly expiring.
    rows = await _cache_rows(wf_conn, wf_schema)
    assert len(rows) == 1, f"the re-fill left {len(rows)} rows"
    fresh = await wf_conn.fetchval(
        f'SELECT expires_at > clock_timestamp() FROM "{wf_schema}".wf_step_cache'
    )
    assert fresh is True, "the re-fill did not write a FRESH expiry"


# ── THE CAS PIN (the concurrent miss) ───────────────────────────────────


@pytest.mark.integration
async def test_concurrent_miss_one_winner(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE CAS: two concurrent misses — the two-claims law's honest
    face, the cache dedups TEMPORAL, never CONCURRENT (both bodies run;
    the ARBITER owns concurrent dedup) — yield ONE winning cache row,
    and the loser's re-read (the NEXT lookup) picks the winner's
    payload + the winner's receipt."""
    _app, compiled = _cache_app()
    runner_a = FlowRunner(compiled, wf_pool, wf_schema)
    runner_b = FlowRunner(compiled, wf_pool, wf_schema)
    claim_a = await runner_a.create_flow(input=None, run_key="cas:a")
    claim_b = await runner_b.create_flow(input=None, run_key="cas:b")

    await asyncio.gather(
        runner_a.drive(claim_a.flow_id),
        runner_b.drive(claim_b.flow_id),
    )

    rows = await _cache_rows(wf_conn, wf_schema)
    assert len(rows) == 1, (
        f"the concurrent misses left {len(rows)} rows — the CAS lost: "
        "two winners is the double-write the ON CONFLICT exists to refuse"
    )
    winner_run = rows[0]["run_id"]
    assert str(winner_run) in (str(claim_a.flow_id), str(claim_b.flow_id)), (
        f"the winning row's run_id {winner_run!r} is neither racing run — the receipt lies"
    )

    # The loser's re-read: the NEXT run's lookup picks the WINNER's
    # payload (the winner's receipt rides it).
    run_c = await _run_flow(compiled, wf_pool, wf_schema, run_key="cas:c")
    assert CALLS["ocr"] == 2, (
        f"the third run re-executed the body (calls={CALLS['ocr']}) — the "
        "loser's re-read never picked up the winner's payload"
    )
    row_c = await _node_row(wf_conn, wf_schema, run_c, "ocr")
    receipt = _meta(row_c).get("wf_cache_hit")
    assert isinstance(receipt, dict) and receipt.get("run_id") == str(winner_run), (
        f"the third run's receipt {receipt!r} does not name the CAS winner ({winner_run})"
    )


# ── THE TYPED-HIT PIN (R3's face) ───────────────────────────────────────


@pytest.mark.integration
async def test_hit_delivers_the_declared_type(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE TYPED DECODE AT THE HIT (R3's face): the cached jsonb decodes
    to the consumer's DECLARED param type — the consumer's body asserts
    the model instance (the module-level ``_consume``), and the hit run
    SUCCEEDS only if the decode held."""
    _app, compiled = _cache_app()
    await _run_flow(compiled, wf_pool, wf_schema, run_key="type:a")
    await _run_flow(compiled, wf_pool, wf_schema, run_key="type:b")
    assert CALLS["ocr"] == 1  # the hit
    assert CALLS["consume"] == 2  # the consumer decoded the hit BOTH times


# ── THE ADDRESS PIN (the code change) ───────────────────────────────────


@pytest.mark.integration
async def test_code_change_is_a_new_address(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ADDRESS'S SENSITIVITY: the body's code change = a NEW address
    (§22.1's stamper's canon in the hash) — a different body over the
    same input NEVER hits the first body's entry: the stale-code face is
    dead by construction."""
    from taskq.workflows._cache import cache_address
    from taskq.workflows._version import compute_code_version
    from taskq.workflows.api._hints import inner_fn, own_source

    src_1 = own_source(inner_fn(_ocr))
    src_2 = own_source(inner_fn(_ocr_v2))
    assert src_1 is not None and src_2 is not None
    v1 = compute_code_version(_ocr.__module__ or "", "step_cache_pins._ocr", src_1)
    v2 = compute_code_version(_ocr_v2.__module__ or "", "step_cache_pins._ocr_v2", src_2)
    addr_1 = cache_address(v1, [{"doc_id": "d1"}])
    addr_2 = cache_address(v2, [{"doc_id": "d1"}])
    assert addr_1 is not None and addr_2 is not None
    assert addr_1 != addr_2, (
        "two DIFFERENT bodies over the same input hashed to ONE address — "
        "the stale-code face is alive (a body's code change must be a new address)"
    )

    _app, compiled = _cache_app()
    await _run_flow(compiled, wf_pool, wf_schema, run_key="code:a")
    # The V2 body (different source, same input) is a MISS: it executes.
    await _run_flow(_app.get(_V2_FLOW), wf_pool, wf_schema, run_key="code:b")
    assert CALLS["ocr_v2"] == 1, (
        "the changed body was never executed — its address collided with "
        "the original's (the stale-code face: the OLD body's cached truth "
        "served for the NEW code)"
    )
    assert CALLS["ocr"] == 1  # the original body ran exactly once, for its own run
    rows = await _cache_rows(wf_conn, wf_schema)
    assert len(rows) == 2, (
        f"two distinct bodies over one input must hold TWO addresses (got {len(rows)})"
    )


# ── THE WIRING FACE (the opt-in's refusals) ─────────────────────────────


def test_ttl_without_cache_refuses_at_the_wiring() -> None:
    """cache_ttl without cache=True is REFUSED (a silent no-op parameter
    is the surprise family — the cache is a decision, never a
    surprise)."""
    app = WorkflowApp()

    @app.workflow("ttl_no_cache")
    def ttl_no_cache() -> Promise[object]:
        return build(step(_ocr, Doc(doc_id="d1"), key="n", cache_ttl=600))

    with pytest.raises(WorkflowBuildError, match="cache_ttl"):
        app.get("ttl_no_cache")


def test_non_positive_ttl_refuses() -> None:
    """A non-positive TTL is refused (an always-expired cache is a
    lie)."""
    app = WorkflowApp()

    @app.workflow("bad_ttl")
    def bad_ttl() -> Promise[object]:
        return build(step(_ocr, Doc(doc_id="d1"), key="n", cache=True, cache_ttl=0))

    with pytest.raises(WorkflowBuildError, match="cache_ttl"):
        app.get("bad_ttl")


def test_cache_on_a_gather_refuses() -> None:
    """THE V1 SURFACE IS THE PLAIN STEP: cache=True on the multi-parent
    fan-in (the gather) is refused at the wiring site."""
    app = WorkflowApp()

    @app.workflow("gather_cache")
    def gather_cache() -> Promise[object]:
        a = step(_ocr, Doc(doc_id="d1"), key="a")
        b = step(_ocr, Doc(doc_id="d2"), key="b")
        return build(step(_consume_list, a, b, key="g", cache=True))

    with pytest.raises(WorkflowBuildError, match="gather"):
        app.get("gather_cache")


# ── THE RETENTION PIN (the sweep's arm) ─────────────────────────────────


@pytest.mark.integration
async def test_the_sweep_prunes_expired_rows_only(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE SWEEP'S RETENTION ARM: expired rows delete (bounded batch),
    fresh rows survive — the dead weight never grows monotone-forever
    (the delivered-outbox arm's own shape)."""
    from taskq.workflows._sweep import prune_expired_step_cache
    from taskq.workflows.engine import render_workflow_sql

    wsql = render_workflow_sql(wf_schema)
    fresh_addr = "addr:fresh"
    old_addr = "addr:old"
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_cache '
        "(content_address, result, run_id, expires_at) "
        "VALUES ($1, '{\"value\": 1}'::jsonb, $2, clock_timestamp() + interval '1 hour')",
        fresh_addr,
        new_uuid(),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_cache '
        "(content_address, result, run_id, expires_at) "
        "VALUES ($1, '{\"value\": 2}'::jsonb, $2, clock_timestamp() - interval '1 hour')",
        old_addr,
        new_uuid(),
    )

    deleted = await prune_expired_step_cache(wf_pool, wsql)
    assert deleted == 1, f"the arm deleted {deleted} rows — the fresh/expired split is broken"
    surviving = {r["content_address"] for r in await _cache_rows(wf_conn, wf_schema)}
    assert surviving == {fresh_addr}, f"the sweep kept {surviving!r} — the expired row survived"


async def test_the_retention_arm_is_registered_with_the_disable_sentinel(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The retention arm's WIRING: the leader's sweep table carries it
    (an arm nothing calls is a silent fix), and ``timedelta(0)`` is the
    disable sentinel (the deletion-sweep family's zero-means-off); the
    DEFAULT keeps the arm alive."""
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_WORKFLOW_STEP_CACHE_SWEEP_PERIOD": "0",
        },
        validate=False,
    )
    assert settings.workflow_step_cache_sweep_period == timedelta(0)

    defaults = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}, validate=False
    )
    assert defaults.workflow_step_cache_sweep_period == timedelta(hours=1)

    specs = _registered_spec_names()
    assert "wf_step_cache_retention" in specs, (
        f"the sweep table registers {sorted(specs)} — the step-cache "
        "retention arm exists but nothing calls it (the silent-fix class)"
    )


def _registered_spec_names() -> set[str]:
    """The sweep table's names, read from the module's own text (the
    specs tuple is built inside _sweep_loop — the source grep is the
    honest structural read for a registration pin)."""
    import inspect
    import re

    import taskq.worker._leader_sweeps as sweeps_mod

    src = inspect.getsource(sweeps_mod)
    return set(re.findall(r'name="([a-z0-9_]+)"', src))
