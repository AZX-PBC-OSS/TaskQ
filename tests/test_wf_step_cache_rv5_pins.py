# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""THE CROSS-RUN STEP CACHE's rv5 HOSTILE-REVIEW pins (feat/taskqflow-cross-run-cache
@ 0839019c1) — the convicted defects of the T25 lane, pinned asserting the
SAFE behavior.

Provenance: the rv5 internals review's live convictions, each reproduced
against this head BEFORE pinning (the pre-pin repro shapes + outputs are in
the pin pack's RECEIPTS.md; the scripts in the pack's ``repro/``). Per the
doctrine, a LIVE finding is pinned under ``pytest.mark.xfail(strict=True,
reason="LIVE FINDING rv5-k<id>: …")`` asserting the SAFE behavior — the cure
flips the pin XPASS-strict (a red that says: remove the marker WITH the
cure). The one GREEN guard (rv5-k2) pins the behavior that is already
correct (the address IS input-sensitive) and must survive the cures — it is
the pack's own hole closed (the mutation that drops the input leg reds it;
the drill's capture is in RECEIPTS.md).

The findings (each convicted live; the receipts carry the verbatim reds):

* F-K1 (MERGE-BLOCKER) — THE ROUTE-SOURCE WEDGE: ``cache=True`` on a
  typed-route SOURCE is accepted at wiring; run 1 stores; run 2 HITS and
  the hit path never rebuilds the route fork (api/_runner.py:666-681 —
  the hit's ``_finalize_success`` carries no ``prebuilt_route_fork``,
  vs :750-770's miss path) → the route children never spawn → the run
  wedges FOREVER with the source row succeeded + the receipt on it
  (observed: ``drive()`` → ``max_ticks``, ``docs`` succeeded with
  ``wf_cache_hit`` naming run 1, zero ``docs.item:*`` rows, ``sum``
  pending, the root ``running``). THE PIN (either cure satisfies — the
  wedge must be impossible): the combination is a BUILD REFUSAL naming
  the why, OR the hit re-forks (the children spawn, the join fires, the
  run completes).
* F-K2 — THE INPUT-SENSITIVITY PIN GAP (the pins' own hole): the address
  must cover the WIRED INPUT — two DIFFERENT inputs to one cached body
  must MISS each other's entries (the mutation that drops the input leg
  survives all 11 shipped pins). THE GREEN GUARD: body B cached with
  input X then called with input Y → MISS + re-execute + separate
  addresses. Green at the head already; the mutation drill (drop the
  ``"input"`` leg from ``cache_address``'s doc) reds it — the capture
  rides RECEIPTS.md.
* F-K3 — THE CTX.INPUT EXCLUSION: a cached body reading
  ``ctx.input["tenant"]`` serves the FIRST run's truth to a second run
  with a different ``ctx.input`` (observed: run 2 with
  ``tenant=globex`` received ``'tenant:acme'`` — the cross-tenant leak
  as terminal success). The address covers the body's code + the WIRED
  args only; the run-scoped input is invisible to it. THE PIN (the
  W-rule direction per the house doctrine — the deterministic-looking
  body reading run-scoped state under the cache opt-in): a build-time
  diagnostic (warning-or-refusal) naming the hazard. The docs-only
  alternative is flagged in PINMAP.md.
* F-K4 — THE CLOSURE HOLE's LAW: the address covers the body's OWN
  source only; a changed HELPER keeps the address. THE PIN (the honest
  minimum, the campaign's docs-numbers pattern): the guide's cache
  section states the law — the address covers the body's own source and
  a changed helper does NOT move the address. (The stronger optional
  leg — a validator note when a cached body calls module-level
  callables — is NOT pinned; see PINMAP.md. If the lanes cure at root
  with transitive hashing, the pin flips.)
* F-K5 — THE EXIT DEAD-CACHE: ``cache=True`` on an ``Exit``-returning
  body silently never stores (api/_runner.py:727-733's
  ``_finalize_exit`` returns before :822-824's store) — the opt-in
  no-ops SILENTLY (observed: two runs, zero cache rows, the body ran
  both times). THE PIN: refused at the wiring, the combination named
  (the Exit-annotation face is compile-visible). The store-instead
  alternative is flagged in PINMAP.md.
* F-K6 — THE EVICTION DOOR: no CLI verb / admin endpoint evicts a cache
  entry (raw SQL only — ``taskq cache purge`` exits 2). THE PIN:
  ``taskq cache purge <address>`` exists and works (a stored row is
  gone after the purge; the next run misses and re-executes).
* F-K7 — THE CLASSIFIER ASYMMETRY: a ``ConnectionError`` on a HIT's
  finalize is laddered as a BODY failure (the hit's finalize sits
  INSIDE the ladder's try — with ``max_attempts=1`` the node
  terminal-fails ``error_class='ConnectionError'`` with the body NEVER
  run); the SAME fault on the MISS path's finalize escapes as infra
  (api/_runner.py:805's call is OUTSIDE the try — the reclaim class;
  observed: the miss's drive ESCAPES with the node left
  reclaim-eligible, the hit's drive returns 'terminal' over a
  terminal-FAILED node). THE PIN: one fault, one semantics — the
  hit-finalize's infra-class fault is the reclaim class (never burns
  the ladder): the node never terminal-fails, no ledger 'failed' row
  lands, and the cleared-fault re-drive completes via the hit.
"""

from __future__ import annotations

import re
from asyncio import to_thread
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.workflows import (
    Exit,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    route,
    step,
)
from taskq.workflows.api._validate import WorkflowValidationError, validate_compiled

# ── the pinned graph's bodies (module-level: the annotations must resolve —
#    the compile's hint resolution reads the defining globals; the §22.1
#    stamper + the cache's address read the body's own SOURCE, which a
#    function-scope body does not reproducibly carry). CALLS counts BODY
#    EXECUTIONS — the cache's entire point. ──

CALLS: dict[str, int] = {}


def _meta(row: asyncpg.Record) -> dict[str, Any]:
    """The row's metadata dict (asyncpg returns jsonb as str on an
    uncoded connection — the estate's parse seam)."""
    raw = row["metadata"]
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


class KDoc(BaseModel):
    doc_id: str


class KText(BaseModel):
    text: str = ""


class KAudio(BaseModel):
    url: str = ""


class KReport(BaseModel):
    ref: str


async def _k_route_source(ctx: StepContext, d: KDoc) -> list[KText | KAudio]:
    CALLS["k_source"] = CALLS.get("k_source", 0) + 1
    return [KText(text="t-" + d.doc_id), KAudio(url="a-" + d.doc_id)]


async def _k_arm_text(ctx: StepContext, item: KText) -> str:
    CALLS["k_arm_text"] = CALLS.get("k_arm_text", 0) + 1
    return "text:" + item.text


async def _k_arm_audio(ctx: StepContext, item: KAudio) -> str:
    CALLS["k_arm_audio"] = CALLS.get("k_arm_audio", 0) + 1
    return "audio:" + item.url


async def _k_sum(ctx: StepContext, items: list[str]) -> str:
    CALLS["k_sum"] = CALLS.get("k_sum", 0) + 1
    return "+".join(sorted(items))


async def _k2_work(ctx: StepContext, d: KDoc) -> KReport:
    CALLS["k2_work"] = CALLS.get("k2_work", 0) + 1
    return KReport(ref=d.doc_id)


async def _k3_tenant_reader(ctx: StepContext) -> str:
    # THE RUN-SCOPED READ UNDER THE CACHE OPT-IN: the address never sees
    # ctx.input — the first run's tenant answer serves every later tenant.
    CALLS["k3_tenant"] = CALLS.get("k3_tenant", 0) + 1
    tenant = "?"
    if isinstance(ctx.input, dict):
        tenant = str(ctx.input.get("tenant", "?"))
    return "tenant:" + tenant


async def _k5_exit_body(ctx: StepContext, d: KDoc) -> Exit[KReport]:
    CALLS["k5_exit"] = CALLS.get("k5_exit", 0) + 1
    return Exit(KReport(ref="exit:" + d.doc_id))


async def _k5_tail(ctx: StepContext, r: KReport) -> str:
    CALLS["k5_tail"] = CALLS.get("k5_tail", 0) + 1
    return r.ref


async def _k7_ocr(ctx: StepContext, d: KDoc) -> KReport:
    CALLS["k7_ocr"] = CALLS.get("k7_ocr", 0) + 1
    return KReport(ref=d.doc_id)


async def _k7_consume(ctx: StepContext, r: KReport) -> str:
    CALLS["k7_consume"] = CALLS.get("k7_consume", 0) + 1
    return r.ref


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
    status = await runner._flow_status(claim.flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the shipped cache pins' own read (the run's status IS the root row's — the derivation's linearization point).
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


# ── F-K1: THE ROUTE-SOURCE WEDGE (either cure satisfies) ────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-k1: cache=True on a typed-route source is accepted at "
        "wiring and the hit path never rebuilds the route fork (api/_runner.py's "
        "hit _finalize_success carries no prebuilt_route_fork) — observed: run 2 "
        "wedged (drive=max_ticks), the source row succeeded WITH the wf_cache_hit "
        "receipt, zero route children, the consumer pending, the root running "
        "forever"
    ),
)
async def test_k1_a_cached_route_source_is_refused_or_the_hit_reforks(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE WEDGE MUST BE IMPOSSIBLE — either cure satisfies:

    * THE BUILD REFUSAL: ``cache=True`` on a typed-route source refuses
      at the wiring/validate door NAMING the why (the route's fork is
      the engine's machinery; a cached envelope cannot carry it), OR
    * THE RE-FORK: the hit rebuilds the route fork from the cached
      envelope — the children spawn, the arms run, the join fires, the
      consumer consumes, the run terminalizes SUCCEEDED.
    """
    app = WorkflowApp()

    @app.workflow("rv5k1_route_cache")
    def k1_flow() -> Promise[object]:
        src = step(_k_route_source, KDoc(doc_id="d1"), key="docs", cache=True)
        routed = route(src, {KText: _k_arm_text, KAudio: _k_arm_audio})
        return build(step(_k_sum, routed, key="sum"))

    try:
        compiled = app.get("rv5k1_route_cache")
    except (WorkflowBuildError, WorkflowValidationError) as exc:
        # THE REFUSAL CURE: the why named (the combination + the cache).
        message = str(exc)
        assert "cache" in message and ("route" in message or "fork" in message), (
            f"the refusal does not name the route+cache combination: {message!r}"
        )
        return

    run_a = await _run_flow(compiled, wf_pool, wf_schema, run_key="rv5k1:a")
    assert CALLS["k_source"] == 1
    assert CALLS["k_arm_text"] == 1 and CALLS["k_arm_audio"] == 1 and CALLS["k_sum"] == 1
    assert len(await _cache_rows(wf_conn, wf_schema)) == 1, "run 1 must store the source's result"

    # THE HIT RUN: the source's body NEVER runs (the receipt proves the
    # hit); the route fork MUST rebuild from the envelope — the children
    # spawn, the join fires, the consumer answers, the run completes.
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    claim_b = await runner.create_flow(input=None, run_key="rv5k1:b")
    assert claim_b.created
    outcome = await runner.drive(claim_b.flow_id, tick=0.02, max_ticks=120)
    assert outcome == "terminal", (
        f"F-K1: the cached route source's HIT wedged the run (drive returned "
        f"{outcome!r}) — the hit's finalize never rebuilt the route fork: the "
        "children never spawn, the consumer's dep never releases, the run is a "
        "'running' corpse over a succeeded source"
    )
    status = await runner._flow_status(claim_b.flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the run's status read, the shipped pins' own.
    assert status == "succeeded", f"the hit run ended {status!r}"
    src_row = await _node_row(wf_conn, wf_schema, claim_b.flow_id, "docs")
    assert _meta(src_row).get("wf_cache_hit") is not None, (
        "the second run's source row carries no receipt — the hit never happened "
        "(this pin's drill REQUIRES the hit path)"
    )
    assert CALLS["k_source"] == 1, "the hit re-executed the source body"
    assert CALLS["k_arm_text"] == 2 and CALLS["k_arm_audio"] == 2, (
        f"the hit run never re-forked the route (arm calls: "
        f"text={CALLS.get('k_arm_text')}, audio={CALLS.get('k_arm_audio')}) — "
        "the cached envelope was delivered but the fork was never rebuilt"
    )
    assert CALLS["k_sum"] == 2, "the consumer never ran on the hit run"
    join_row = await wf_conn.fetchrow(
        f'SELECT status::text AS status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'docs.join'",
        claim_b.flow_id,
    )
    assert join_row is not None and join_row["status"] == "succeeded", (
        "the route's join row never spawned/fired on the hit run"
    )
    assert run_a is not None  # the producing run's record stands


# ── F-K2: THE INPUT-SENSITIVITY GREEN GUARD (the pack's own hole) ────────


@pytest.mark.integration
async def test_k2_two_inputs_to_one_cached_body_miss_each_other(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE GREEN GUARD (already correct at the head — pinned so the
    mutation that drops the address's input leg can never ship silently):
    ONE cached body under TWO different wired inputs → the second call
    MISSES the first's entry (the body re-executes) and the two calls
    hold TWO distinct addresses. The mutation drill (drop the ``input``
    leg from ``cache_address``'s doc) reds this pin — the capture rides
    the pack's RECEIPTS.md."""
    from taskq.workflows._cache import cache_address

    app = WorkflowApp()

    @app.workflow("rv5k2_x")
    def k2_x() -> Promise[object]:
        return build(step(_k2_work, KDoc(doc_id="X"), key="work", cache=True))

    @app.workflow("rv5k2_y")
    def k2_y() -> Promise[object]:
        return build(step(_k2_work, KDoc(doc_id="Y"), key="work", cache=True))

    compiled_x = app.get("rv5k2_x")
    # The address's halves, computed through the RUNNER's OWN seam (the
    # versioning pins' read — the row's record answers the runner's own
    # hash, never the pin's parallel construction).
    runner_x = FlowRunner(compiled_x, wf_pool, wf_schema)
    node = compiled_x.nodes["work"]
    version = runner_x._body_code_version("work", node, _k2_work)  # pyright: ignore[reportPrivateUsage]  # Why: the address's code-version half must be the runner's OWN computation (the versioning pins' idiom) — a parallel hash proves nothing about the stored rows.
    assert version is not None, "the module-level body's hash must be verifiable"
    addr_x = cache_address(version, [{"doc_id": "X"}])
    addr_y = cache_address(version, [{"doc_id": "Y"}])
    assert addr_x != addr_y, (
        "F-K2: one body over two inputs hashed to ONE address — the input leg "
        "is absent from the address (any run's input reads every run's entry)"
    )

    await _run_flow(compiled_x, wf_pool, wf_schema, run_key="rv5k2:x")
    assert CALLS["k2_work"] == 1
    # THE SECOND INPUT: a MISS — the body RE-EXECUTES (never the X entry
    # served for the Y call).
    await _run_flow(app.get("rv5k2_y"), wf_pool, wf_schema, run_key="rv5k2:y")
    assert CALLS["k2_work"] == 2, (
        "F-K2: the second input HIT the first's entry — the address is blind "
        "to the wired input (the cross-input leak)"
    )
    rows = await _cache_rows(wf_conn, wf_schema)
    addresses = {r["content_address"] for r in rows}
    assert addresses == {addr_x, addr_y}, (
        f"the two inputs must hold TWO addresses (got {sorted(addresses)}, "
        f"expected the runner-seamed {sorted([addr_x, addr_y])})"
    )


# ── F-K3: THE CTX.INPUT EXCLUSION (the build-time diagnostic) ────────────


@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-k3: a cached body reading ctx.input serves the FIRST "
        "run's truth to any later run with a different input (observed: run 2 "
        "with tenant=globex received 'tenant:acme', the body never ran) — no "
        "build surface names the hazard (validate_compiled returned zero "
        "diagnostics for the shape)"
    ),
)
def test_k3_a_cached_body_reading_ctx_input_is_a_build_diagnostic() -> None:
    """THE RUN-SCOPED STATE UNDER THE CACHE OPT-IN (the house's W-rule
    doctrine — the deterministic-looking body reading run-scoped state):
    ``step(..., cache=True)`` whose body reads ``ctx.input`` is a
    build-time diagnostic — a WARNING-or-refusal naming the hazard (the
    address covers the body's code + the wired args; the run's
    ``ctx.input`` is invisible to it). The docs-only alternative is
    flagged in PINMAP.md (the guide would carry the law verbatim)."""
    app = WorkflowApp()

    @app.workflow("rv5k3_ctx_input_cache")
    def k3_flow() -> Promise[object]:
        return build(step(_k3_tenant_reader, key="read", cache=True))

    try:
        compiled = app.get("rv5k3_ctx_input_cache")
    except WorkflowValidationError as exc:
        # THE REFUSAL FACE — accepted, provided the hazard is named.
        message = str(exc)
        assert "cache" in message and "input" in message, (
            f"the refusal does not name the cache+ctx.input hazard: {message!r}"
        )
        return
    diagnostics = validate_compiled(compiled)
    assert any("cache" in d.message and "input" in d.message for d in diagnostics), (
        f"F-K3: no build-time diagnostic named the cache+ctx.input hazard — "
        f"the cross-tenant leak ships silently. validate() said: "
        f"{[f'{d.rule}: {d.message}' for d in diagnostics]}"
    )


# ── F-K4: THE CLOSURE HOLE's LAW (the docs pin) ─────────────────────────

_DOCS = Path(__file__).resolve().parent.parent / "docs"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-k4: the address covers the body's OWN source only — a "
        "changed HELPER keeps the address (the closure hole) — and NO doc states "
        "it: the guide's cache section and the api-reference carry 'module, "
        "qualname, the body's own source' but never the operational consequence "
        "(a changed helper does NOT move the address)"
    ),
)
def test_k4_the_docs_carry_the_closure_holes_law() -> None:
    """THE HONEST MINIMUM (the campaign's docs-numbers pattern): the
    guide's cache section states the closure law — the address covers
    the body's OWN source; a changed HELPER (a module-level callable the
    body calls) does NOT move the address. An operator who fixes a
    helper and watches the stale entry keep serving learns the law here
    or nowhere. If the lanes cure at root (transitive hashing), this pin
    flips to the new law's text."""
    guide = (_DOCS / "guides" / "workflows.md").read_text()
    section = guide[guide.index("## The cross-run step cache") :]
    # The section ends at the next level-2 heading (or the file's end).
    tail = section.split("\n## ", 1)
    cache_section = tail[0]
    lowered = cache_section.lower()
    assert "own source" in cache_section, (
        "the guide's cache section dropped the own-source claim — the address "
        "law's first half must survive any rewrite"
    )
    assert "helper" in lowered, (
        "F-K4: the guide's cache section never names the closure hole — the "
        "law 'the address covers the body's own source; a changed HELPER does "
        "NOT move the address' is nowhere written"
    )
    assert re.search(
        r"helper[^.\n]*(not|never)[^.\n]*address|address[^.\n]*(not|never)[^.\n]*helper", lowered
    ), (
        "F-K4: the guide names 'helper' but never states the law's operational "
        "half — that a changed helper does NOT move the address"
    )


# ── F-K5: THE EXIT DEAD-CACHE (refused at the wiring) ────────────────────


@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-k5: cache=True on an Exit-returning body builds clean "
        "and silently never stores (api/_runner.py's _finalize_exit returns "
        "before the cache store) — observed: two runs, zero cache rows, the "
        "body executed both times, no diagnostic anywhere"
    ),
)
def test_k5_cache_on_an_exit_returning_body_refuses() -> None:
    """THE SILENT NO-OP DIES AT THE DOOR: a body whose declared return is
    ``Exit[...]`` under ``cache=True`` is a wiring/validate REFUSAL
    naming the combination (the Exit finalize is a DIFFERENT terminal —
    the cached envelope can never replay the skipped-downstream record;
    the opt-in is a lie the wiring must refuse). The store-instead
    alternative is flagged in PINMAP.md."""
    app = WorkflowApp()

    @app.workflow("rv5k5_exit_cache")
    def k5_flow() -> Promise[object]:
        e = step(_k5_exit_body, KDoc(doc_id="d1"), key="exit", cache=True)
        return build(step(_k5_tail, e, key="tail"))

    with pytest.raises((WorkflowBuildError, WorkflowValidationError)) as exc_info:
        app.get("rv5k5_exit_cache")
    message = str(exc_info.value)
    assert "cache" in message and ("Exit" in message or "exit" in message), (
        f"F-K5: the refusal does not name the Exit+cache combination: {message!r}"
    )


# ── F-K6: THE EVICTION DOOR (the purge verb) ─────────────────────────────


@pytest.fixture
def cli_settings(module_pg_schema: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """The CLI's settings read THIS module's migrated schema (the tests'
    own env — the cli pins' idiom): the env is the settings model's own
    front door."""
    monkeypatch.setenv("TASKQ_PG_DSN", module_pg_schema.pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)
    return module_pg_schema


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-k6: no eviction door exists — `taskq cache purge "
        "<address>` exits 2 (no such command) and the admin surface carries no "
        "equivalent; the operator's only eviction tool is raw SQL against "
        "wf_step_cache"
    ),
)
async def test_k6_the_cache_purge_verb_evicts(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    cli_settings: Any,
) -> None:
    """THE OPERATOR'S DOOR: ``taskq cache purge <address>`` evicts the
    named entry — after the purge the row is GONE and the next run
    MISSES (the body re-executes). The natural face is the CLI verb (the
    cure names it); an admin endpoint with the same semantics satisfies
    the law and flips this pin's invocation, flagged in PINMAP.md."""
    from typer.testing import CliRunner

    from taskq.cli import app as cli_app

    app = WorkflowApp()

    @app.workflow("rv5k6_purge_flow")
    def k6_flow() -> Promise[object]:
        return build(step(_k2_work, KDoc(doc_id="purge-me"), key="work", cache=True))

    compiled = app.get("rv5k6_purge_flow")
    await _run_flow(compiled, wf_pool, wf_schema, run_key="rv5k6:a")
    assert CALLS["k2_work"] == 1
    rows = await _cache_rows(wf_conn, wf_schema)
    assert len(rows) == 1, "the first run must store exactly one entry"
    address = rows[0]["content_address"]

    result = await to_thread(CliRunner().invoke, cli_app, ["cache", "purge", str(address)])
    assert result.exit_code == 0, (
        f"F-K6: `taskq cache purge <address>` failed (exit={result.exit_code}) — "
        f"the eviction door does not exist. output: {result.output[:300]!r}"
    )
    assert await _cache_rows(wf_conn, wf_schema) == [], (
        "the purge returned success but the row survives — a lying door"
    )
    # The next run MISSES (the body re-executes — the eviction is real).
    await _run_flow(compiled, wf_pool, wf_schema, run_key="rv5k6:b")
    assert CALLS["k2_work"] == 2, "the purged address still served a hit"


# ── F-K7: THE CLASSIFIER ASYMMETRY (one fault, one semantics) ────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-k7: a ConnectionError on a HIT's finalize is laddered "
        "as a BODY failure (the hit's _finalize_success sits INSIDE the ladder's "
        "try — observed: max_attempts=1 → the node terminal-failed "
        "error_class='ConnectionError', the run failed, the body NEVER ran); the "
        "SAME fault on the MISS path's finalize escapes as infra (the reclaim "
        "class — observed: drive() raised, the node left running/reclaim-eligible, "
        "the ladder unburned)"
    ),
)
async def test_k7_the_hit_finalize_infra_fault_is_the_reclaim_class(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ONE FAULT, ONE SEMANTICS: the infra-class fault (the storm's
    ``ConnectionError``) at the HIT's finalize is the RECLAIM class —
    the ladder NEVER burns on it (the miss path's own semantics, the
    machinery boundary's classifier's law applied symmetrically):

    * the node NEVER terminal-fails on the fault;
    * NO ledger 'failed' row lands for it;
    * the run is reclaimable — the cleared-fault re-drive completes it
      via the HIT (the body STILL never runs).
    """
    app = WorkflowApp()

    @app.workflow("rv5k7_classifier_flow")
    def k7_flow() -> Promise[object]:
        o = step(_k7_ocr, KDoc(doc_id="d1"), key="ocr", cache=True, max_attempts=1)
        return build(step(_k7_consume, o, key="consume"))

    compiled = app.get("rv5k7_classifier_flow")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    # The seed run (the clean miss): the body pays for the entry.
    claim_a = await runner.create_flow(input=None, run_key="rv5k7:a")
    assert (await runner.drive(claim_a.flow_id)) == "terminal"
    assert CALLS["k7_ocr"] == 1

    async def _faulty_finalize(self: Any, *a: Any, **kw: Any) -> Any:
        raise ConnectionError("the storm's class (the pin's injected finalize fault)")

    monkeypatch.setattr(FlowRunner, "_finalize_success", _faulty_finalize)
    claim_b = await runner.create_flow(input=None, run_key="rv5k7:b")
    assert claim_b.created
    from contextlib import suppress

    with suppress(ConnectionError):
        # the reclaim-class escape — the machinery boundary's own shape
        await runner.drive(claim_b.flow_id, tick=0.02, max_ticks=60)

    node = await _node_row(wf_conn, wf_schema, claim_b.flow_id, "ocr")
    assert node["status"] != "failed", (
        f"F-K7: the hit-finalize's infra fault terminal-FAILED the node "
        f"(status={node['status']!r}) — the SAME fault on the miss path escapes "
        "as the reclaim class; here it was laddered as a BODY failure with the "
        "body never run (the classifier asymmetry: one fault, two semantics)"
    )
    failed_ledgers = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'ocr' AND status = 'failed'",
        claim_b.flow_id,
    )
    assert failed_ledgers == 0, (
        f"F-K7: the infra fault burned the ladder ({failed_ledgers} 'failed' "
        "ledger row(s)) — the reclaim class writes NO failure record"
    )

    # THE RECLAIM'S PROOF: the fault clears, the re-drive completes the
    # run — via the HIT (the body STILL never ran in run B).
    monkeypatch.undo()
    outcome = await runner.drive(claim_b.flow_id)
    assert outcome == "terminal", outcome
    status = await runner._flow_status(claim_b.flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the run's status read, the shipped pins' own.
    assert status == "succeeded", (
        f"the reclaimed run ended {status!r} — the infra fault's reclaim never healed"
    )
    assert CALLS["k7_ocr"] == 1, (
        "run B executed the body — the whole point of the drill is the HIT path's "
        "finalize (the body must NEVER run in the hit run)"
    )
    receipt = _meta(await _node_row(wf_conn, wf_schema, claim_b.flow_id, "ocr")).get("wf_cache_hit")
    assert isinstance(receipt, dict), "the reclaimed run's row carries no hit receipt"
