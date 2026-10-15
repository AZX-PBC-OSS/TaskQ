"""THE CACHE CURES' PINS — the cache-attack front's convictions, cured.

The lane's red receipts (all captured live BEFORE the cures, on the base
head ``0e2f5cc09`` against :5810 — the files live in
``.measurements/cache-cures/``):

* CURE 1 (P8f, the ROUTE-SOURCE WEDGE): ``route_wedge_repro.py`` — run 1
  stores, run 2 HITS and the hit path never rebuilds the route fork →
  the run wedged FOREVER (``running``), the source row reading
  ``succeeded`` wearing the ``wf_cache_hit`` receipt, ZERO route
  children, ZERO join rows.
* CURE 3 (P2d, the ctx.input CROSS-TENANT LEAK):
  ``ctx_input_leak_repro.py`` — tenant B's run terminal-succeeded with
  tenant A's answer (the body never ran), audits clean.
* AMENDMENT A (the import-probe's #4, the SOURCE-NODE INPUT BLINDNESS):
  ``source_feed_stale_hit_repro.py`` — the 640-row feed stored; the
  60-row feed was served 640 rows (the content-blind stale hit).
* AMENDMENT B (the import-probe's #3, the UNVERIFIABLE-SOURCE SILENCE):
  ``unverifiable_source_silent_repro.py`` — ``cache=True`` accepted at
  wiring, the body ran EVERY run, zero cache rows, the only trace a log
  line.
* CURE 6a (the HIT-FINALIZE FAULT'S TWO SEATS):
  ``hit_finalize_fault_repro.py`` — an InterfaceError inside the hit's
  finalize was ABSORBED by the body ladder: the node terminal-FAILED on
  a ``max_attempts=1`` step whose body NEVER ran, the run killed
  ``UnabsorbedNodeFailure``.

The cure choices are the READS' choices, and the pins hold them:

* the route wedge is REFUSED, not rebuilt — the arm dispatch reads the
  element's RUNTIME type and the cached envelope carries only encoded
  dicts: the fork CANNOT be rebuilt from the envelope by construction
  (the map face rebuilds — ``_map_fork`` reads the encoded ``value`` —
  the route face mechanically cannot). The pin survives the same law:
  at the first hit the fork spawns identically — because the hit can
  never happen (the wiring refuses).
* the ctx.input leak is REFUSED with the static source-scan — crude,
  declared, one walk shared by the wiring verb and E18; the wired-param
  version (the teaching example) caches per value and is pinned green.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

from asyncio import to_thread
from typing import TYPE_CHECKING, Any, cast

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.cli import app as cli_app
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    route,
    step,
)
from taskq.workflows.api._graph import Exit
from taskq.workflows.api._validate import WorkflowValidationError

if TYPE_CHECKING:
    import asyncpg


# ── the pinned bodies (module-level: the sources must be readable —
#    E20's own subject) ────────────────────────────────────────────────


class Doc(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class ImageItem(BaseModel):
    doc_id: str


class AudioItem(BaseModel):
    doc_id: str


class ImageResult(BaseModel):
    doc_id: str


class AudioResult(BaseModel):
    doc_id: str


CALLS: dict[str, int] = {}


async def media_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    CALLS["media_source"] = CALLS.get("media_source", 0) + 1
    return [ImageItem(doc_id="d1"), AudioItem(doc_id="d2")]


async def process_image(ctx: StepContext, item: ImageItem) -> ImageResult:
    CALLS["image"] = CALLS.get("image", 0) + 1
    return ImageResult(doc_id=item.doc_id)


async def process_audio(ctx: StepContext, item: AudioItem) -> AudioResult:
    CALLS["audio"] = CALLS.get("audio", 0) + 1
    return AudioResult(doc_id=item.doc_id)


async def tenant_read(ctx: StepContext) -> str:
    """THE UNCACHED LIFT (the teaching example's upstream): the flow
    input's value enters the DATAFLOW here — never cached."""
    CALLS["tenant_read"] = CALLS.get("tenant_read", 0) + 1
    flow_input = cast(
        "dict[str, object]", ctx.input
    )  # the Any-contract walk (the seed wrote the shape)
    tenant = flow_input["tenant_id"]
    return str(tenant)


async def tenant_body(ctx: StepContext, tenant: str) -> Report:
    """THE CACHED BODY, CURED: the tenant is a PARAM — it enters the
    address (the wired-param version caches per value)."""
    CALLS["tenant_body"] = CALLS.get("tenant_body", 0) + 1
    return Report(ref=f"{tenant}:{CALLS['tenant_body']}")


async def leaking_tenant_body(ctx: StepContext) -> Report:
    """THE LEAKY BODY (the E18 subject): the tenant rides ``ctx.input`` —
    which the address NEVER covers."""
    CALLS["leaking"] = CALLS.get("leaking", 0) + 1
    flow_input = cast(
        "dict[str, object]", ctx.input
    )  # the Any-contract walk (the seed wrote the shape)
    tenant = flow_input["tenant_id"]
    return Report(ref=f"{tenant}:leak")


async def plain_body(ctx: StepContext, d: Doc) -> Report:
    CALLS["plain"] = CALLS.get("plain", 0) + 1
    return Report(ref=d.doc_id)


async def exit_body(ctx: StepContext, d: Doc) -> Exit[Report]:
    CALLS["exit_body"] = CALLS.get("exit_body", 0) + 1
    return Exit(Report(ref=d.doc_id))


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    CALLS.clear()


# ── CURE 1: THE ROUTE-SOURCE WEDGE'S REFUSAL (E17) ──────────────────────


def test_route_source_cache_refuses_at_the_route_attachment() -> None:
    """THE WEDGE'S DOOR (P8f): ``cache=True`` on a typed-route source is
    refused at the wiring — the typed refusal names WHY (the hit path
    cannot rebuild the fork: the arm dispatch reads the element's runtime
    type) and the ALTERNATIVE (cache the arms' work, not the
    fork-spawning source). The pre-cure shape wired clean and wedged
    forever at the first hit — the red receipt is the lane's live repro."""
    app = WorkflowApp()

    @app.workflow("wedge_flow")
    def flow() -> Promise[object]:
        src = step(media_source, key="source", cache=True)
        routed = route(src, {ImageItem: process_image, AudioItem: process_audio})
        return build(routed)

    with pytest.raises(WorkflowBuildError, match="route fork NEVER SPAWNS") as exc_info:
        app.get("wedge_flow")
    message = str(exc_info.value)
    assert "the element's runtime type" in message, (
        "the refusal must name WHY the fork cannot be rebuilt"
    )
    assert "Cache the route's ARMS' work" in message, "the refusal must name the alternative"


def test_route_source_cache_refuses_at_the_map_dict_form_too() -> None:
    """The dict form lowers through the SAME attachment — the same
    refusal (the two spellings are one machinery, one fence)."""
    from taskq.workflows import map_source

    app2 = WorkflowApp()

    @app2.workflow("wedge_map_flow2")
    def flow2() -> Promise[object]:
        src = step(media_source, key="source", cache=True)
        return build(map_source(src, {ImageItem: process_image, AudioItem: process_audio}))

    with pytest.raises(WorkflowBuildError, match="route fork NEVER SPAWNS"):
        app2.get("wedge_map_flow2")


def test_route_source_cache_refused_by_validate_e17() -> None:
    """E17's checker-independent re-proof: the compiled graph is public,
    mutable data (E3's precedent) — the wiring verb's refusal re-proved
    from the compiled nodes themselves."""
    from taskq.workflows.api._validate import validate_compiled

    app = WorkflowApp()

    @app.workflow("e17_flow")
    def flow() -> Promise[object]:
        src = step(media_source, key="source")  # cache=False: the wiring door passes
        routed = route(src, {ImageItem: process_image, AudioItem: process_audio})
        return build(routed)

    compiled = app.get("e17_flow")
    compiled.nodes["source"].cache = True  # the mutated shape E17 convicts
    with pytest.raises(WorkflowValidationError, match="E17-cache-route-source"):
        validate_compiled(compiled)


# ── CURE 3: THE ctx.input CROSS-TENANT LEAK'S REFUSAL (E18) ────────────


def test_ctx_input_cache_refuses_at_the_wiring() -> None:
    """THE LEAK'S DOOR (P2d): a body reading ``ctx.input`` with
    ``cache=True`` is refused at the wiring — the flow input never
    enters the address, so the cached answer under one input would serve
    a LATER run whose input differs (the cross-tenant leak: the red
    receipt served A's answer to B, terminal-success). The refusal names
    the fix: wire the value as a param so it enters the address."""
    app = WorkflowApp()

    @app.workflow("leak_wiring_flow")
    def flow() -> Promise[object]:
        return build(step(leaking_tenant_body, key="tstep", cache=True))

    with pytest.raises(WorkflowBuildError, match="cross-tenant leak") as exc_info:
        app.get("leak_wiring_flow")
    assert "wire its result as this body's PARAM" in str(exc_info.value), (
        "the refusal must name the fix"
    )


def test_ctx_input_cache_refused_by_validate_e18() -> None:
    """E18's checker-independent re-proof from the compiled graph (the
    same static source-scan the wiring verb reads — one walk, two
    faces)."""
    from taskq.workflows.api._validate import validate_compiled

    app = WorkflowApp()

    @app.workflow("e18_flow")
    def flow() -> Promise[object]:
        return build(step(leaking_tenant_body, key="tstep"))  # cache=False passes

    compiled = app.get("e18_flow")
    compiled.nodes["tstep"].cache = True  # the mutated shape E18 convicts
    with pytest.raises(WorkflowValidationError, match="E18-cache-ctx-input"):
        validate_compiled(compiled)


@pytest.mark.integration
async def test_the_wired_param_version_caches_per_value(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE TEACHING EXAMPLE, GREEN (CURE 3's positive face): the tenant
    lifted into the dataflow by an UNcached upstream step and wired as
    the cached body's PARAM — the param enters the address, and two
    DIFFERENT flow inputs are two DIFFERENT addresses: both bodies run,
    each run serves its own tenant's value, no cross-serve."""
    app = WorkflowApp()

    @app.workflow("tenant_a_flow")
    def flow_a() -> Promise[object]:
        t = step(tenant_read, key="tenant")
        return build(step(tenant_body, t, key="tstep", cache=True))

    @app.workflow("tenant_b_flow")
    def flow_b() -> Promise[object]:
        t = step(tenant_read, key="tenant")
        return build(step(tenant_body, t, key="tstep", cache=True))

    async def _drive(compiled: Any, run_key: str, tenant: str) -> str:
        runner = FlowRunner(compiled, wf_pool, wf_schema)
        claim = await runner.create_flow(input={"tenant_id": tenant}, run_key=run_key)
        await runner.drive(claim.flow_id)
        ref = await wf_conn.fetchval(
            f"SELECT result->'value'->>'ref' FROM \"{wf_schema}\".jobs "
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'tstep'",
            claim.flow_id,
        )
        return str(ref)

    ref_a1 = await _drive(app.get("tenant_a_flow"), "tenant:a1", "tenant-A")
    ref_b1 = await _drive(app.get("tenant_b_flow"), "tenant:b1", "tenant-B")
    assert ref_a1.startswith("tenant-A:") and ref_b1.startswith("tenant-B:"), (
        "each run's FIRST answer must be its own tenant's (the input sensitivity)"
    )
    ref_a2 = await _drive(app.get("tenant_a_flow"), "tenant:a2", "tenant-A")
    ref_b2 = await _drive(app.get("tenant_b_flow"), "tenant:b2", "tenant-B")
    # The SECOND run per tenant HITS (the body ran once per tenant) and
    # serves THAT tenant's value — the cross-serve is dead.
    assert CALLS["tenant_body"] == 2, (
        f"the cached bodies ran {CALLS['tenant_body']}x — one per tenant, never one per run"
    )
    assert ref_a2 == ref_a1 and ref_b2 == ref_b1, "each tenant's hit serves THAT tenant's own value"


# ── AMENDMENT A: THE SOURCE-NODE INPUT BLINDNESS (the refusal's face) ──


def test_source_node_run_input_refused_the_import_probe_shape() -> None:
    """THE IMPORT PROBE'S #4 (AMENDMENT A): a SOURCE node whose body
    reads the RUN INPUT — the 640→60 stale-hit shape — is refused at the
    wiring. The address covers the wired args only; a source node whose
    only run-varying input is the flow input has NO sound address. The
    refusal teaches the wire-it-in fix (their worked example worked only
    because they had learned to wire the spec in — now the API teaches
    it)."""
    app = WorkflowApp()

    @app.workflow("feed_wedge_flow")
    def flow() -> Promise[object]:
        return build(step(leaking_tenant_body, key="feed", cache=True))

    with pytest.raises(WorkflowBuildError, match="cross-tenant leak"):
        app.get("feed_wedge_flow")


# ── AMENDMENT B: THE UNVERIFIABLE-SOURCE PRECONDITION (E20) ────────────


def test_unverifiable_source_cache_refuses_at_the_wiring() -> None:
    """THE SILENCE'S DOOR (AMENDMENT B — the import-probe's #3): an
    unverifiable body source made the step SILENTLY uncached — the
    wiring accepted cache=True, every run re-executed the body, the only
    trace a log line (the red receipt). The operator must never learn
    the face from a MISSING HIT: the precondition refuses here, by
    name."""
    ns: dict[str, object] = {"Report": Report, "StepContext": StepContext}
    exec(  # noqa: S102  # Why: the exec'd body IS the pin's subject — the unverifiable source the refusal convicts.
        """
async def ghost_body(ctx: StepContext, d: Report) -> Report:
    return Report(ref=d.ref)
""",
        ns,
    )
    ghost_body = ns["ghost_body"]

    app = WorkflowApp()

    @app.workflow("ghost_flow")
    def flow() -> Promise[object]:
        return build(step(ghost_body, Report(ref="r"), key="ghost", cache=True))  # type: ignore[arg-type]

    with pytest.raises(WorkflowBuildError, match="source is not verifiable") as exc_info:
        app.get("ghost_flow")
    assert "module-level def" in str(exc_info.value), "the refusal must name the fix"


def test_unverifiable_source_refused_by_validate_e20() -> None:
    """E20's checker-independent re-proof from the compiled graph (the
    same own_source read the claim path's stamper shares — the refusal
    carries no false-positive risk: a source the wiring cannot read is a
    source the hash will never reproduce)."""
    from taskq.workflows.api._validate import validate_compiled

    ns: dict[str, object] = {"Report": Report, "StepContext": StepContext}
    exec(  # noqa: S102  # Why: the exec'd body IS the pin's subject — the unverifiable source the refusal convicts.
        """
async def ghost_body(ctx: StepContext, d: Report) -> Report:
    return Report(ref=d.ref)
""",
        ns,
    )
    ghost_body = ns["ghost_body"]

    app = WorkflowApp()

    @app.workflow("e20_flow")
    def flow() -> Promise[object]:
        return build(step(ghost_body, Report(ref="r"), key="ghost"))  # type: ignore[arg-type]

    compiled = app.get("e20_flow")
    compiled.nodes["ghost"].cache = True  # the mutated shape E20 convicts
    with pytest.raises(WorkflowValidationError, match="E20-cache-unverifiable-source"):
        validate_compiled(compiled)


# ── CURE 6b: THE Exit NO-OP'S REFUSAL (E19) ────────────────────────────


def test_exit_body_cache_refuses_at_the_wiring() -> None:
    """THE NO-OP'S DOOR (CURE 6b): cache=True on an Exit-returning body
    is the silent no-op — the sentinel terminalizes through the exit
    finalize, which the success-only store never sees. Refused (the
    picked door), bare AND inside a union (the union face is the next
    pin)."""
    app = WorkflowApp()

    @app.workflow("exit_cache_flow")
    def flow() -> Promise[object]:
        return build(step(exit_body, Doc(doc_id="d1"), key="x", cache=True))

    with pytest.raises(WorkflowBuildError, match="silently no-op") as exc_info:
        app.get("exit_cache_flow")
    assert "the success-only store never sees" in str(exc_info.value)


async def union_exit_body(ctx: StepContext, d: Doc) -> Report | Exit[Report]:
    CALLS["union_exit"] = CALLS.get("union_exit", 0) + 1
    return Exit(Report(ref=d.doc_id))


def test_exit_union_body_cache_refuses_too() -> None:
    """A body whose return is a UNION containing the sentinel is as
    exit-terminal as a bare Exit — the walk goes through unions and
    list elements."""
    app = WorkflowApp()

    @app.workflow("exit_union_flow")
    def flow() -> Promise[object]:
        return build(step(union_exit_body, Doc(doc_id="d1"), key="x", cache=True))

    with pytest.raises(WorkflowBuildError, match="silently no-op"):
        app.get("exit_union_flow")


def test_exit_body_cache_refused_by_validate_e19() -> None:
    """E19's checker-independent re-proof from the compiled graph."""
    from taskq.workflows.api._validate import validate_compiled

    app = WorkflowApp()

    @app.workflow("e19_flow")
    def flow() -> Promise[object]:
        return build(step(exit_body, Doc(doc_id="d1"), key="x"))  # cache=False passes

    compiled = app.get("e19_flow")
    compiled.nodes["x"].cache = True  # the mutated shape E19 convicts
    with pytest.raises(WorkflowValidationError, match="E19-cache-exit-body"):
        validate_compiled(compiled)


# ── CURE 6a: THE HIT-FINALIZE FAULT'S ONE SEAT ──────────────────────────


@pytest.mark.integration
async def test_hit_finalize_fault_is_infra_never_a_body_failure(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """ONE FAULT, ONE SEAT (CURE 6a): a transient blip escaping the HIT
    path's finalize is the MACHINERY's seat — the same seat the MISS
    path's finalize faults own (the escape-point contract: the
    classifier reads WHERE the error escaped). The pre-cure seat routed
    the blip through the body ladder: the node terminal-FAILED on a
    max_attempts=1 step whose body NEVER ran (the red receipt — the
    live repro's exact shape). The cure: the blip ESCAPES the claim tail
    (the reclaim-eligible transport fault — the ledger says crashed, the
    ladder unburned, no error record, the node reclaimable)."""
    app = WorkflowApp()

    @app.workflow("hit_fault_flow")
    def flow() -> Promise[object]:
        return build(step(plain_body, Doc(doc_id="d1"), key="step", cache=True, max_attempts=1))

    compiled = app.get("hit_fault_flow")

    runner1 = FlowRunner(compiled, wf_pool, wf_schema)
    claim1 = await runner1.create_flow(input=None, run_key="fault:a")
    await runner1.drive(claim1.flow_id)
    assert CALLS["plain"] == 1, "run 1 is the miss — the body paid"

    import asyncpg as _asyncpg

    runner2 = FlowRunner(compiled, wf_pool, wf_schema)
    claim2 = await runner2.create_flow(input=None, run_key="fault:b")

    async def blipping_finalize(*args: Any, **kwargs: Any) -> object:
        raise _asyncpg.exceptions.InterfaceError("the transient blip")

    runner2._finalize_success = blipping_finalize  # type: ignore[method-assign,assignment]

    # THE ESCAPE: the fault propagates out of the drive (the machinery
    # boundary's own seat) — never absorbed as a body failure.
    with pytest.raises(_asyncpg.exceptions.InterfaceError):
        await runner2.drive(claim2.flow_id)

    status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'step'",
        claim2.flow_id,
    )
    assert status != "failed", (
        f"the hit's finalize blip terminal-FAILED the node ({status!r}) — a "
        "transient blip can never terminal-fail a max_attempts=1 step whose "
        "body never ran (the two-seats bug, red)"
    )
    errors = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND error_class IS NOT NULL",
        claim2.flow_id,
    )
    assert int(errors) == 0, (
        "the blip must never write an error record — the ladder was never "
        "the hit's finalize fault's seat"
    )


# ── CURE 4a: THE INPUT-SENSITIVITY PIN ──────────────────────────────────


@pytest.mark.integration
async def test_two_different_inputs_two_addresses_no_cross_serve(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE INPUT-SENSITIVITY PIN (CURE 4a — the strengthened pack): the
    pack feeds TWO DIFFERENT inputs to one body — different addresses,
    different results, NO cross-serve. The pre-cure pack had no pin
    feeding distinct inputs through one cached body: the mutation (an
    address that ignores the input) SURVIVED it — the amendment's
    640→60 red is that mutation's live face. Here the pin dies only if
    the address truly covers the input."""
    app = WorkflowApp()

    @app.workflow("sens_a_flow")
    def flow_a() -> Promise[object]:
        return build(step(plain_body, Doc(doc_id="d1"), key="ocr", cache=True))

    @app.workflow("sens_b_flow")
    def flow_b() -> Promise[object]:
        return build(step(plain_body, Doc(doc_id="DIFFERENT"), key="ocr", cache=True))

    async def _drive(compiled: Any, run_key: str) -> str:
        runner = FlowRunner(compiled, wf_pool, wf_schema)
        claim = await runner.create_flow(input=None, run_key=run_key)
        await runner.drive(claim.flow_id)
        return str(
            await wf_conn.fetchval(
                f"SELECT result->'value'->>'ref' FROM \"{wf_schema}\".jobs "
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'ocr'",
                claim.flow_id,
            )
        )

    ref_a1 = await _drive(app.get("sens_a_flow"), "sens:a1")
    ref_b1 = await _drive(app.get("sens_b_flow"), "sens:b1")
    assert CALLS["plain"] == 2, "two different inputs: BOTH bodies pay (no cross-serve)"
    ref_a2 = await _drive(app.get("sens_a_flow"), "sens:a2")
    ref_b2 = await _drive(app.get("sens_b_flow"), "sens:b2")
    assert CALLS["plain"] == 2, (
        f"the second run per input re-executed the body (calls={CALLS['plain']}) — "
        "the per-input hit face is dead"
    )
    assert ref_a2 == ref_a1 and ref_b2 == ref_b1, "each input's hit serves its OWN result"
    assert ref_a1 != ref_b1, "the two inputs' results differ"


# ── CURE 4b: THE CAS-FRESHNESS PIN ──────────────────────────────────────


@pytest.mark.integration
async def test_cas_freshness_first_winner_keeps_expired_corpse_refills(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: Any
) -> None:
    """THE CAS-FRESHNESS PIN (CURE 4b — the strengthened pack): the TWO
    SEMANTICS distinguished, both arms provable.

    * FIRST-WINNER-KEEPS: a store against a FRESH row LOSES (the
      last-writer-wins shape is dead — a fresh winner never loses its
      row, so a racing loser's late fill can never overwrite the winner
      the lookups already read);
    * LAST-WRITER-WINS ON THE CORPSE ONLY: a store against an EXPIRED
      row WINS (the re-fill — the freshness leg's own arm);
    * THE RACE: two concurrent stores against one backdated corpse —
      exactly ONE winner (the loser's DO UPDATE re-checks the freshness
      leg against the row the winner just made fresh and yields).
    """
    from taskq.workflows._cache import store_step_cache

    wsql = wf_sql

    async def _store(address: str, run_id: str) -> bool:
        return await store_step_cache(
            wf_pool,
            wsql,
            address=address,
            envelope={"value": run_id},
            run_id=JobId(new_uuid()),
            ttl_s=3600.0,
        )

    # ── arm 1: FIRST-WINNER-KEEPS (the fresh row) ──
    assert await _store("addr:fresh", "winner-1") is True, (
        "the first store on an empty address WINS"
    )
    fresh_row = await wf_conn.fetchval(
        f"SELECT result->>'value' FROM \"{wf_schema}\".wf_step_cache "
        "WHERE content_address = 'addr:fresh'"
    )
    assert fresh_row == "winner-1", "the fresh row carries the winner's payload"
    assert await _store("addr:fresh", "loser-2") is False, (
        "a store against a FRESH row LOSES — first-winner-keeps: the last-writer-wins shape is dead"
    )
    still = await wf_conn.fetchval(
        f"SELECT result->>'value' FROM \"{wf_schema}\".wf_step_cache "
        "WHERE content_address = 'addr:fresh'"
    )
    assert still == "winner-1", "the loser's write NEVER landed — the winner's payload stands"

    # ── arm 2: the EXPIRED CORPSE is RE-FILLED (last-writer-wins, corpse only) ──
    assert await _store("addr:corpse", "first") is True
    backdated = await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_step_cache '
        "SET expires_at = clock_timestamp() - interval '1 second' "
        "WHERE content_address = 'addr:corpse'"
    )
    assert backdated == "UPDATE 1", "the corpse was backdated for the re-fill arm"
    assert await _store("addr:corpse", "refiller") is True, (
        "a store against an EXPIRED corpse WINS — the re-fill arm is the freshness leg's own face"
    )
    refilled = await wf_conn.fetchval(
        f"SELECT result->>'value' FROM \"{wf_schema}\".wf_step_cache "
        "WHERE content_address = 'addr:corpse'"
    )
    assert refilled == "refiller", "the corpse carries the REFILLER's payload"

    # ── arm 3: THE RACE — two concurrent stores on one backdated corpse ──
    assert await _store("addr:race", "corpse") is True
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_step_cache '
        "SET expires_at = clock_timestamp() - interval '1 second' "
        "WHERE content_address = 'addr:race'"
    )
    import asyncio as _asyncio

    winners = await _asyncio.gather(
        _store("addr:race", "racer-a"),
        _store("addr:race", "racer-b"),
    )
    assert sorted(winners) == [False, True], (
        f"the concurrent stores yielded {winners!r} — exactly ONE winner: "
        "the loser's DO UPDATE re-checks the freshness leg against the row "
        "the winner just made fresh and yields (the fresh-winner-never-"
        "loses leg, provable under contention)"
    )
    final_row = await wf_conn.fetchval(
        f"SELECT result->>'value' FROM \"{wf_schema}\".wf_step_cache "
        "WHERE content_address = 'addr:race'"
    )
    assert final_row in ("racer-a", "racer-b"), "the race's row carries ONE racer's payload"


# ── CURE 5: THE EVICTION FACE'S PINS (the CLI) ──────────────────────────


@pytest.fixture
def cache_cli_env(module_pg_schema: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """The CLI's settings read THIS module's migrated schema (the
    test_wf_cli_pins convention — the env is the front door)."""
    monkeypatch.setenv("TASKQ_PG_DSN", module_pg_schema.pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)
    return module_pg_schema


async def test_cache_evict_bare_refuses(cache_cli_env: Any) -> None:
    """A bare ``evict`` that deletes EVERYTHING is a footgun — refused
    by name (exit 1, never a traceback)."""
    result = await to_thread(CliRunner().invoke, cli_app, ["wf", "cache", "evict"])
    assert result.exit_code == 1, "the bare evict refuses"
    assert "no selector" in result.output


async def test_cache_evict_dry_run_default_then_yes(
    cache_cli_env: Any, wf_pool: asyncpg.Pool, wf_schema: str, wf_sql: Any
) -> None:
    """THE DRY-RUN DEFAULT (CURE 5's spine): ``evict`` prints what WOULD
    go and touches NOTHING; ``--yes`` deletes. The row the eviction took
    out is a MISS for the next run — the lie's service ends when the
    operator says so, never silently."""
    from taskq.workflows._cache import store_step_cache

    address = "deadbeef" + "0" * 56
    wsql = wf_sql
    landed = await store_step_cache(
        wf_pool,
        wsql,
        address=address,
        envelope={"value": "the-poison-pill"},
        run_id=JobId(new_uuid()),
        ttl_s=3600.0,
    )
    assert landed is True, "the poison pill's row is in place"

    dry = await to_thread(
        CliRunner().invoke, cli_app, ["wf", "cache", "evict", "--address-prefix", "deadbeef"]
    )
    assert dry.exit_code == 0
    assert "DRY RUN" in dry.output and "WOULD be evicted" in dry.output

    remaining = await wf_pool.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_step_cache')
    assert int(remaining) == 1, "the DRY RUN touched NOTHING"

    fired = await to_thread(
        CliRunner().invoke,
        cli_app,
        ["wf", "cache", "evict", "--address-prefix", "deadbeef", "--yes"],
    )
    assert fired.exit_code == 0
    assert "evicted" in fired.output
    after = await wf_pool.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_step_cache')
    assert int(after) == 0, "the --yes eviction TOOK THE ROW OUT"


async def test_cache_list_shows_rows_and_receipts(cache_cli_env: Any) -> None:
    """THE HIT OBSERVABILITY (CURE 5's other half): ``wf cache list``
    shows the cache rows AND the hits' wf_cache_hit receipts — the
    receipt already existed on the node rows; the surface folds it into
    the operator's one command."""
    result = await to_thread(CliRunner().invoke, cli_app, ["wf", "cache", "list"])
    assert result.exit_code == 0
    assert "cache rows" in result.output and "hit receipts" in result.output
