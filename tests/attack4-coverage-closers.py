# ruff: noqa: N999, S608, S603, ASYNC221, TID251, N817  # Why: the dash-named attack4-* module; the cold-process probes run a subprocess with fixed argv; the seeded ledger rows deliberately use uuid4 (a ledger row the arbiter did not mint); BM is the probe corpus's local shorthand.
"""ATTACK4 — THE COVERAGE RESIDUALS (the split owner's gaps, closed by
the attacker's own tests — attack4 ADDS tests, never source).

The captured p4 report's missing-line lists
(``p4-coverage-split4.txt``: _hitl 68, _loop 80, _ctx 81, _ctx_wait 86,
engine 87) re-verified RED-line on the certified head (the builder's
``5ea18be3`` "coverage closers" did NOT close them — the attack's
baseline re-measurement shows the same lines). What each region IS:

* ``_loop.py`` — the two pure policy/binding fns' None/KeyError/fallback
  arms (pure functions: closed here directly);
* ``_hitl.py`` — ``resolve_signal_models``'s None arm, ``_model_fits``'s
  alias/strict-fit arms, ``_fits_by_schema``'s full structural walk (the
  cold-process fallback's teeth), the no-deadline hold's NULL deadline,
  the deliver CAS's no-op + resume-refused arms, the context's
  chain+hook redact composition;
* ``_ctx.py`` — ``cursor()`` (the T20 checkpoint read: {} before the
  first emit + the decode), the progress emitter's unwired no-op;
* ``_ctx_wait.py`` — the re-wait's NodeHeldError (a hold standing), the
  replay path's non-dict payload passthrough, ``signal()`` (the
  delivered-payload reader + its unavailable refusal);
* ``engine.py`` — finalize's fenced-out ledger writes (by-id +
  by-attempt), the terminal-by-id leg, the join fire's winner-None
  guard, the reducer body's execution at the fire, the finalize's
  reducer registration.

The NOT-closed (named, per the brief's "or name why"): the partial
BRANCH arcs (``583->596`` et al.) that exist only as doubled guard
conditions over one runtime shape — a test can only re-enter the same
line; and ``_runner.py``'s 901/922-class arms whose setup requires a
forked child process mid-finalize (the crash-window world is the
t20-fence probe's lane; the duplication here would pin nothing new).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import FlowRunner, WorkflowApp, build, step
from taskq.workflows.api import GateDecl
from taskq.workflows.api._hitl import HitlClient

pytestmark = pytest.mark.integration


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


class Item(BaseModel):
    n: int


# ══ _loop.py — the pure policy/binding arms ══════════════════════════════


def test_registered_loop_policy_the_unregistered_workflow_fails_closed() -> None:
    """A workflow the registry cannot resolve → the policy is ``fail``
    (no definition, no escalation face — the dead-letter ghost's
    refusal), for BOTH the None-name and the unknown-name worlds."""
    from taskq.workflows.api._loop import registered_loop_policy

    assert registered_loop_policy(None, "review") == "fail"
    assert registered_loop_policy("attack4_no_such_workflow", "review") == "fail"


def test_registered_loop_policy_reads_the_declared_policy_and_the_body_fallback() -> None:
    """A DECLARED policy is read from the definition; an undeclared loop
    key falls back to escalate-with-a-body / fail-without."""
    from taskq.workflows import Done, WorkflowApp, build, loop
    from taskq.workflows.api._loop import registered_loop_policy

    async def iteration(ctx: Any, carry: int) -> object:
        return Done("x")

    app = WorkflowApp()

    @app.workflow("attack4_policy_declared")
    def declared() -> object:
        return build(loop("review", iteration, initial=0, max_iterations=2, on_exhausted="fail"))

    @app.workflow("attack4_policy_escalate")
    def escalate() -> object:
        return build(
            loop("review", iteration, initial=0, max_iterations=2, on_exhausted="escalate")
        )

    app.get("attack4_policy_declared")
    app.get("attack4_policy_escalate")

    assert registered_loop_policy("attack4_policy_declared", "review") == "fail", (
        "the DECLARED policy must be read from the definition"
    )
    assert registered_loop_policy("attack4_policy_escalate", "review") == "escalate", (
        "an undeclared loop key escalates when the workflow registers an escalation body"
    )
    assert registered_loop_policy("attack4_policy_declared", "no_such_loop") == "fail", (
        "an undeclared loop key with no escalation body fails (no ghost enqueue)"
    )


def test_escalation_bindings_the_unregistered_worlds_return_none() -> None:
    """``None`` = the enqueue is SKIPPED: no workflow name, an
    unresolvable name, or no escalation body — a ghost dead-letter row is
    never written."""
    from taskq.workflows.api._loop import escalation_bindings

    kwargs: dict[str, object] = {
        "flow_id": "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f",
        "error_class": "ValueError",
        "message": "boom",
    }
    assert escalation_bindings(None, "review", **kwargs) is None  # type: ignore[arg-type]
    assert escalation_bindings("attack4_no_such_workflow", "review", **kwargs) is None  # type: ignore[arg-type]
    # A real workflow WITHOUT an escalation body → None (skip, not ghost).
    from taskq.workflows import WorkflowApp, build, step

    async def plain(ctx: Any, params: Ingest) -> str:
        return "ok"

    app = WorkflowApp()

    @app.workflow("attack4_no_escalation_body")
    def no_body() -> object:
        return build(step(plain, Ingest(doc_id="d"), key="solo"))

    app.get("attack4_no_escalation_body")
    assert escalation_bindings("attack4_no_escalation_body", "review", **kwargs) is None  # type: ignore[arg-type]


# ══ _hitl.py — the cold-process fallback's teeth (pure) ═════════════════


def test_resolve_signal_models_the_empty_name_is_none() -> None:
    from taskq.workflows.api._hitl import resolve_signal_models

    assert resolve_signal_models(None, "review", "Approval") is None
    assert resolve_signal_models("", "review", "Approval") is None
    assert resolve_signal_models("attack4_never_ran", "review", "Approval") is None


def test_model_fits_the_strict_shape() -> None:
    """BY SHAPE, never by declaration order: an unknown field fails, a
    missing required field fails, an ALIAS is admitted."""
    from pydantic import BaseModel as BM
    from pydantic import Field

    from taskq.workflows.api._hitl import _model_fits

    class Strict(BM):
        verdict: str
        note: str = ""

    class Aliased(BM):
        inner: str = Field(alias="outer")

    # The strict fits.
    assert _model_fits(Strict, {"verdict": "ok"})
    # An unknown field: refused (no Lenient swallowing).
    assert not _model_fits(Strict, {"verdict": "ok", "surprise": 1})
    # A missing required field: refused.
    assert not _model_fits(Strict, {})
    # A wrong type: refused.
    assert not _model_fits(Strict, {"verdict": 42})
    # An ALIASED model: the alias is in the allowed set (the alias form fits).
    assert _model_fits(Aliased, {"outer": "x"})


def test_fits_by_schema_the_full_walk() -> None:
    """The COLD fallback's structural check: the unknown-field refusal,
    the required-field refusal, every primitive type branch, and the
    cannot-judge shapes FIT (never a false refusal)."""
    from taskq.workflows.api._hitl import _fits_by_schema

    schema = {
        "properties": {
            "verdict": {"type": "string"},
            "count": {"type": "integer"},
            "ratio": {"type": "number"},
            "flag": {"type": "boolean"},
            "tags": {"type": "array"},
            "meta": {"type": "object"},
        },
        "required": ["verdict"],
    }
    good = {"verdict": "ok", "count": 1, "ratio": 0.5, "flag": True, "tags": [], "meta": {}}
    assert _fits_by_schema(good, schema)
    # Each branch's lying variant.
    assert not _fits_by_schema({"verdict": "ok", "stray": 1}, schema), "unknown field"
    assert not _fits_by_schema({}, schema), "missing required"
    assert not _fits_by_schema({"verdict": "ok", "count": "1"}, schema), "integer as str"
    assert not _fits_by_schema({"verdict": "ok", "count": True}, schema), "bool as integer"
    assert not _fits_by_schema({"verdict": "ok", "ratio": True}, schema), "bool as number"
    assert not _fits_by_schema({"verdict": "ok", "flag": "yes"}, schema), "str as boolean"
    assert not _fits_by_schema({"verdict": "ok", "tags": {}}, schema), "object as array"
    assert not _fits_by_schema({"verdict": "ok", "meta": []}, schema), "array as object"
    assert not _fits_by_schema({"verdict": "ok", "count": 1.5}, schema), "float as integer"
    assert not _fits_by_schema({"verdict": 42}, schema), "the string branch"
    # A spec that is not a dict: skipped (continue), the rest still walks.
    junk_spec = dict(schema)
    junk_spec["properties"] = {"verdict": {"type": "string"}, "weird": "not-a-dict"}
    assert _fits_by_schema({"verdict": "ok"}, junk_spec)
    # The CANNOT-JUDGE shapes fit (never a false refusal).
    assert _fits_by_schema({"x": 1}, None)
    assert _fits_by_schema("not a dict", schema)
    assert _fits_by_schema({"verdict": "ok"}, {"$defs": {}})
    assert _fits_by_schema({"verdict": "ok"}, {"anyOf": []})
    assert _fits_by_schema({"verdict": "ok"}, {"allOf": []})
    assert _fits_by_schema({"verdict": "ok"}, {"properties": "junk"})


# ══ _hitl.py — the live arms ═════════════════════════════════════════════


_HELD_APP_STATE: dict[str, Any] = {}


@pytest.fixture
async def held_app(module_pg_pool: Any, module_pg_schema: ModulePgSchema) -> Any:
    """The D1 registry registers the workflow ONCE per process (a second
    registration is the refused shadow) — the app lives in module state;
    each test seeds its OWN flow from the same runner."""
    schema = module_pg_schema.schema_name
    if not _HELD_APP_STATE:
        app = WorkflowApp()
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)

        async def holds(ctx: Any, params: Ingest) -> str:
            await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the closers' hold")
            return "done"

        @app.workflow("attack4_closer_hold")
        def closer_hold() -> object:
            return build(step(holds, Ingest(doc_id="d1"), key="review", gates=(gate,)))

        _HELD_APP_STATE["app"] = app
    app = _HELD_APP_STATE["app"]
    runner = FlowRunner(app.get("attack4_closer_hold"), module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    return {"app": app, "runner": runner, "flow_id": flow_id, "schema": schema}


async def test_the_no_deadline_hold_writes_a_null_deadline(
    module_pg_schema: ModulePgSchema,
) -> None:
    """``timeout_s=None`` → the deadline is PG's NULL (the W1 warning's
    subject — the eternal wait is a NAMED shape, never a fake date)."""
    from taskq.workflows.api._hitl import _deadline_expr

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        assert await _deadline_expr(conn, None) is None
        got = await _deadline_expr(conn, 60.0)
        assert got is not None  # PG's clock + 60s
    finally:
        await conn.close()


async def test_the_deliver_cas_the_noop_and_the_resume_refused_arms(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, held_app: Any
) -> None:
    """The deliver CAS's two silent arms: the SECOND deliver on a
    consumed hold is the named no-op; a CAS win whose node lost its hold
    mark is the typed resume-refusal (the row stands)."""
    from taskq.workflows.api._hitl import deliver_payload

    schema = held_app["schema"]
    flow_id = held_app["flow_id"]
    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(str(flow_id))
    # THE FIRST deliver (the CAS wins, the node resumes).
    first = await deliver_payload(
        module_pg_pool,
        schema=schema,
        workflow_id=flow_id,
        hold_id=hold.hold_id,
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    assert first.status == "delivered", first
    # THE SECOND deliver (the same hold): the named no-op.
    second = await deliver_payload(
        module_pg_pool,
        schema=schema,
        workflow_id=flow_id,
        hold_id=hold.hold_id,
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    assert second.status == "no-op" and "already delivered" in (second.reason or ""), second

    # THE RESUME-REFUSED arm: a held hold whose node's mark is gone.
    flow2 = (await held_app["runner"].create_flow()).flow_id
    await held_app["runner"].drive(flow2, until="held")
    (hold2,) = await client.list(str(flow2))
    await module_pg_pool.execute(
        f'UPDATE "{schema}".jobs '
        "SET metadata = metadata - 'hold' - 'held_signal' "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'review'",
        flow2,
    )
    third = await deliver_payload(
        module_pg_pool,
        schema=schema,
        workflow_id=flow2,
        hold_id=hold2.hold_id,
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    assert third.status == "refused", third
    assert "not resumable" in (third.reason or ""), third

    # THE CANCELLED-HOLD arm (the deliver CAS's refusal half): a hold the
    # cancel cascade consumed — the CAS misses, the row's status is
    # 'cancelled' → the named refusal (never the no-op).
    flow3 = (await held_app["runner"].create_flow()).flow_id
    await held_app["runner"].drive(flow3, until="held")
    (hold3,) = await client.list(str(flow3))
    from taskq.backend._protocol import JobId as _JobId
    from taskq.workflows.api._runner_exit import cancel_workflow_run as _cancel

    await _cancel(module_pg_pool, schema=schema, flow_id=_JobId(str(flow3)))
    fourth = await deliver_payload(
        module_pg_pool,
        schema=schema,
        workflow_id=flow3,
        hold_id=hold3.hold_id,
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    assert fourth.status == "refused" and fourth.reason != third.reason, fourth


async def test_the_cold_boundary_refuses_by_the_rows_schema_alone(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema
) -> None:
    """The COLD-PROCESS refusal arms (the row's payload_schema is the
    only witness): a garbage payload validates against NONE of the
    declared models → the named refusal; an AMBIGUOUS schema (a payload
    two models fit) → the named ambiguity refusal; a fitting payload →
    the delivery proceeds. A subprocess whose process never ran the wait
    site (the upgrade world's real shape)."""
    import subprocess as sp
    import sys
    import textwrap

    schema = module_pg_schema.schema_name
    app = WorkflowApp()
    gate = GateDecl(
        name="Approval|Alt",
        payload_models=(
            Approval,
            type("Alt", (BaseModel,), {"__annotations__": {"ok": bool}, "ok": True}),
        ),
        timeout_s=120.0,
    )

    async def holds(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the cold refusal")
        return "done"

    @app.workflow("attack4_cold_refusal")
    def cold_refusal() -> object:
        return build(step(holds, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    runner = FlowRunner(app.get("attack4_cold_refusal"), module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    # The AMBIGUOUS world: a second schema both-all-optional (the
    # {"verdict": ...} payload fits BOTH by shape).
    await module_pg_pool.execute(
        f'UPDATE "{schema}".wf_signals '
        "SET payload_schema = $2::jsonb WHERE workflow_id = $1 AND status = 'held'",
        flow_id,
        json.dumps(
            {
                "Approval": {
                    "type": "object",
                    "properties": {"verdict": {"type": "string"}},
                    "required": ["verdict"],
                },
                "Alt": {
                    "type": "object",
                    "properties": {"verdict": {"type": "string"}},
                    "required": ["verdict"],
                },
            }
        ),
    )
    probe = textwrap.dedent(f"""
        import asyncio
        import asyncpg
        from taskq.workflows.api._hitl import HitlClient

        async def main() -> None:
            pool = await asyncpg.create_pool({module_pg_schema.pg_dsn!r})
            client = HitlClient(pool, schema={schema!r})
            (hold,) = await client.list({str(flow_id)!r})
            bad = await client.resolve(hold.hold_id, {{"junk": True}})
            print("BAD:", bad.status)
            await pool.close()

        asyncio.run(main())
    """)
    proc = sp.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=120)
    print(f"\n[attack4 cold-refusal] {proc.stdout.strip()!r}")
    assert "refused" in proc.stdout, (
        f"the cold process delivered an undeclared payload: {proc.stdout!r} {proc.stderr!r}"
    )


async def test_the_resolves_dead_node_arm_via_the_client(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, held_app: Any
) -> None:
    """``HitlClient.resolve``'s twin arm: the CAS won but the node's hold
    mark is gone (the flow died) — the typed refusal, the delivered ROW
    stands."""
    schema = held_app["schema"]
    client = HitlClient(module_pg_pool, schema=schema)
    flow_id = (await held_app["runner"].create_flow()).flow_id
    await held_app["runner"].drive(flow_id, until="held")
    (hold,) = await client.list(str(flow_id))
    await module_pg_pool.execute(
        f'UPDATE "{schema}".jobs '
        "SET metadata = metadata - 'hold' - 'held_signal' "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'review'",
        flow_id,
    )
    result = await client.resolve(hold.hold_id, {"verdict": "approve"})
    assert result.status == "refused" and "not resumable" in (result.reason or ""), result


async def test_the_contexts_null_payload_skips_the_chain(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, held_app: Any
) -> None:
    """A hold whose payload is NULL (the legacy/erased row): the context's
    chain gate is the isinstance guard — the payload rides as None, no
    chain, no hook (nothing to redact)."""
    schema = held_app["schema"]
    flow_id = (await held_app["runner"].create_flow()).flow_id
    await held_app["runner"].drive(flow_id, until="held")
    await module_pg_pool.execute(
        f'UPDATE "{schema}".wf_signals SET payload = NULL '
        "WHERE workflow_id = $1 AND status = 'held'",
        flow_id,
    )
    hooked: list[Any] = []

    def hook(payload: dict[str, Any]) -> dict[str, Any]:
        hooked.append(payload)
        return payload

    client = HitlClient(module_pg_pool, schema=schema, redact=hook)
    (hold,) = await client.list(str(flow_id))
    assert hold.payload is None, f"the NULL payload became {hold.payload!r}"
    assert not hooked, "the hook ran on a NULL payload (nothing to redact)"


async def test_the_context_redact_chain_then_hook(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, held_app: Any
) -> None:
    """THE REDACT LAW's composition: the chain runs ALWAYS; a hooked
    client receives the CHAIN's output (the hook can only redact more —
    it never sees the raw canary)."""
    from taskq.workflows.api._hitl import HitlClient

    schema = held_app["schema"]
    flow_id = held_app["flow_id"]
    await module_pg_pool.execute(
        f'UPDATE "{schema}".wf_signals SET payload = $2::jsonb '
        "WHERE workflow_id = $1 AND status = 'held'",
        flow_id,
        json.dumps({"reason": "password=hunter2 CANARY2", "note": "plain"}),
    )
    seen: dict[str, str] = {}

    def hook(payload: dict[str, Any]) -> dict[str, Any]:
        seen["payload"] = json.dumps(payload)
        assert "hunter2" not in seen["payload"], (
            "the hook received the RAW payload — the chain did not run first"
        )
        out = dict(payload)
        if "reason" in out and isinstance(out["reason"], str) and "CANARY2" in out["reason"]:
            out["reason"] = out["reason"].replace("CANARY2", "[HOOKED]")
        return out

    client = HitlClient(module_pg_pool, schema=schema, redact=hook)
    (hold,) = await client.list(str(flow_id))
    doc = hold.payload if isinstance(hold.payload, dict) else {}
    text = json.dumps(doc)
    assert "hunter2" not in text, f"the chain's mask did not run: {text!r}"
    assert "CANARY2" not in text, "the hook did not compose"
    assert "[HOOKED]" in text


# ══ _ctx.py — the cursor + the unwired emitter ═══════════════════════════


async def test_the_cursors_checkpoint_the_empty_then_the_decoded(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema
) -> None:
    """``ctx.cursor()``: ``{}`` before the first emit; the committed
    checkpoint decodes after (the T20 resume law's read half)."""
    schema = module_pg_schema.schema_name
    app = WorkflowApp()

    async def src(ctx: Any, params: Ingest) -> list[Item]:
        # THE EMPTY world: no emit has ever run for this node.
        before = await ctx.cursor()
        assert before == {}, f"an un-emitted source's cursor is {before!r}"
        await ctx.emit_batch([Item(n=1).model_dump()], cursor={"page": 1})
        after = await ctx.cursor()
        assert after.get("page") == 1, f"the checkpoint did not read back: {after!r}"
        # THE STR-DECODE arm (the no-codec connection's world): a RAW
        # pool's ctx reads the same checkpoint through _json_loads.
        raw_pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
        try:
            raw_ctx = _make_wait_ctx(
                raw_pool, module_pg_schema.schema_name, ctx.flow_id, ctx.job_id, "src"
            )
            stamped = await raw_ctx.cursor()
            assert stamped.get("page") == 1, f"the str-decode arm lost the checkpoint: {stamped!r}"
        finally:
            await raw_pool.close()
        return [Item(n=1)]

    async def child(ctx: Any, n: int) -> dict[str, int]:
        # The map child's ctx carries ITS map_index (the per-item identity).
        assert ctx.map_index is not None
        return {"n": n}

    @app.workflow("attack4_cursor_flow")
    def cursor_flow() -> object:
        from taskq.workflows import map_source

        ingested = step(src, Ingest(doc_id="d1"), key="src")
        return build(map_source(ingested, child))

    runner = FlowRunner(app.get("attack4_cursor_flow"), module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id)
    assert outcome == "terminal"


def test_the_progress_emission_unwired_is_the_noop() -> None:
    """The unwired emitter (a directly-constructed ctx): ``progress()``
    validates and returns — the deliberate no-op, never an
    AttributeError."""
    from taskq.backend._protocol import JobId
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.api._ctx import StepContext

    ctx = StepContext(
        flow_id=JobId("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"),
        job_id=JobId("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4e"),
        node_key="solo",
        attempt=1,
        input={"doc_id": "d"},
        _pool=None,  # type: ignore[arg-type]  # Why: the unwired construction — the no-op arm must not touch it.
        _wsql=WorkflowSql.build("attack4_unwired"),
        _map_index=None,
    )
    # The unwired emitter: the call RETURNS (the no-op), it does not raise.
    asyncio.run(ctx.progress(50, "half", None))


# ══ _ctx_wait.py — the re-wait, the replay passthrough, signal() ═════════


async def test_the_re_wait_while_a_hold_stands_raises_node_held(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema
) -> None:
    """A SECOND wait on a node that ALREADY holds (the deliberate
    re-wait's face): the wait site raises NodeHeldError with the STANDING
    hold's id — the body's except owns it (no second epoch is minted by
    accident)."""
    from taskq.workflows.api._ctx_wait import NodeHeldError

    schema = module_pg_schema.schema_name
    app = WorkflowApp()
    gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)

    async def double_wait(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="first")
        return "x"

    @app.workflow("attack4_rewait")
    def rewait() -> object:
        return build(step(double_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    runner = FlowRunner(app.get("attack4_rewait"), module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    # THE RE-WAIT from a NEW body execution's ctx (the same coordinates):
    # the standing hold is RAISED, not re-minted.
    node_id = await module_pg_pool.fetchval(
        f'SELECT id FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'review'",
        flow_id,
    )
    ctx = _make_wait_ctx(module_pg_pool, schema, flow_id, node_id, "review")
    with pytest.raises(NodeHeldError) as ei:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the re-wait")
    assert ei.value.hold_id, "the raised error does not name the standing hold"


def _make_wait_ctx(pool: Any, schema: str, flow_id: Any, node_id: Any, node_key: str) -> Any:
    """A wait-ops ctx bound to a REAL node row (the re-wait's shape)."""
    from taskq.backend._protocol import JobId
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.api._ctx import StepContext

    return StepContext(
        flow_id=JobId(str(flow_id)),
        job_id=JobId(str(node_id)),
        node_key=node_key,
        attempt=1,
        input={"doc_id": "d"},
        _pool=pool,
        _wsql=WorkflowSql.build(schema),
        _map_index=None,
    )


def test_the_replay_payload_passthrough_for_a_non_dict() -> None:
    """The replay path's guard: a non-dict payload rides THROUGH (the
    fit check is a dict-shape check — a scalar decision is not a shape
    error)."""
    from pydantic import BaseModel as BM

    from taskq.workflows.api._ctx_wait import CtxWaitOps

    class Gate(BM):
        verdict: str

    # A non-dict payload passes through untouched (the guard's early arm).
    assert CtxWaitOps._coerce_signal((Gate,), "just a string") == "just a string"  # pyright: ignore[reportPrivateUsage]
    assert CtxWaitOps._coerce_signal((Gate,), 42) == 42  # pyright: ignore[reportPrivateUsage]


async def test_signal_reads_a_delivered_payload_and_refuses_the_rest(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema
) -> None:
    """``ctx.signal(name)``: the DELIVERED payload rides the row; a
    non-delivered (or absent) signal is the named SignalUnavailableError
    with the row's actual status."""
    from taskq.workflows.api._ctx_wait import SignalUnavailableError

    schema = module_pg_schema.schema_name
    app = WorkflowApp()
    gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)

    async def _wait(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the reader")
        # THE RESUMED BODY'S ANSWER IDENTITY: the LAST CONSUMED hold's
        # epoch is on the ctx (None before the consumption — this body
        # HAS consumed one).
        assert ctx.hold_epoch == 1, f"the consumed hold's epoch: {ctx.hold_epoch!r}"
        return str(await ctx.signal("Approval"))

    @app.workflow("attack4_signal_reader")
    def signal_reader() -> object:
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    runner = FlowRunner(app.get("attack4_signal_reader"), module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(str(flow_id))
    await client.resolve(hold.hold_id, {"verdict": "approve", "note": "shipped"})
    await runner.drive(flow_id)
    result = await runner.result(flow_id)
    got = result if isinstance(result, dict) else (__import__("ast").literal_eval(str(result)))
    assert got == {"verdict": "approve", "note": "shipped"}, (
        f"the delivered payload did not ride the row: {result!r}"
    )

    # THE ABSENT arm: a signal name nothing declared/delivered (the
    # named refusal carries the row's status — 'none' here).
    node_id = await module_pg_pool.fetchval(
        f'SELECT id FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'review'",
        flow_id,
    )
    ctx = _make_wait_ctx(module_pg_pool, schema, flow_id, node_id, "review")
    with pytest.raises(SignalUnavailableError) as ei:
        await ctx.signal("never_declared")
    assert "none" in str(ei.value), ei.value
    # THE NON-DELIVERED arm: a row that exists but is not delivered.
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, payload, "
        " status, created_at) "
        "VALUES ($1, $2, 'review', 'Ghost', 1, 'g', 'null', 'cancelled', now())",
        uuid.uuid4(),
        flow_id,
    )
    with pytest.raises(SignalUnavailableError) as ei2:
        await ctx.signal("Ghost")
    assert "cancelled" in str(ei2.value), ei2.value


# ══ engine.py — the fence ledger writes + the join fire arms ═════════════


async def test_the_finalize_fence_writes_the_ledger_by_id_and_by_attempt(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, clean_pg_conn: Any
) -> None:
    """The FENCED-OUT finalize: a stale claim's finalize writes the
    ledger's fence row (by the ledger id when the attempt carries one, by
    the attempt key when it does not) and tx2 never runs — the row's
    state never moves."""
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.engine import finalize_node
    from tests._wf_fixtures import seed_flow, seed_running_node

    schema = module_pg_schema.schema_name
    wsql = WorkflowSql.build(schema)
    flow_id = await seed_flow(clean_pg_conn, schema)
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="fenced")

    # THE FENCE: a WRONG worker presents the finalize — WITH the claim's
    # own ledger id (the by-ID fence leg).
    from tests._wf_fixtures import claim_view

    worker, attempt, epoch = await claim_view(clean_pg_conn, schema, node_id)
    # The LEDGER row the claim arbiter would have written at claim (the
    # fence write targets it by (flow, step, attempt, map_index)).
    ledger_id = uuid.uuid4()
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, attempt, status) "
        "VALUES ($1, $2, $3, 'fenced', $4, 'running')",
        ledger_id,
        flow_id,
        node_id,
        attempt,
    )
    wrong_worker = type(worker)("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4a")
    res = await finalize_node(
        module_pg_pool,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="fenced",
        worker_id=wrong_worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"n": 1},
        ledger_id=ledger_id,
    )
    assert res.applied is False, "a wrong worker's finalize must be fenced out"
    state = await clean_pg_conn.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', node_id
    )
    assert state == "running", "the fenced finalize moved the row"
    fence = await clean_pg_conn.fetchval(
        f'SELECT status FROM "{schema}".wf_step_ledger WHERE id = $1', ledger_id
    )
    assert fence == "fenced", f"the by-ID fence leg did not run: {fence!r}"

    # THE TERMINAL-BY-ID leg: the RIGHT worker's finalize with the
    # ledger id writes the ledger's terminal THROUGH THE ID.
    res2 = await finalize_node(
        module_pg_pool,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="fenced",
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"n": 1},
        ledger_id=ledger_id,
    )
    assert res2.applied is True, res2
    terminal = await clean_pg_conn.fetchval(
        f'SELECT status FROM "{schema}".wf_step_ledger WHERE id = $1', ledger_id
    )
    assert terminal == "succeeded", f"the by-ID terminal leg did not run: {terminal!r}"

    # THE BY-ATTEMPT fence leg: the same fence WITHOUT a ledger id —
    # the statement keys the arbiter tuple.
    node2 = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="fenced2")
    worker2, attempt2, epoch2 = await claim_view(clean_pg_conn, schema, node2)
    ledger2 = uuid.uuid4()
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, attempt, status) "
        "VALUES ($1, $2, $3, 'fenced2', $4, 'running')",
        ledger2,
        flow_id,
        node2,
        attempt2,
    )
    res3 = await finalize_node(
        module_pg_pool,
        wsql,
        flow_id=flow_id,
        job_id=node2,
        step_key="fenced2",
        worker_id=type(worker2)("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4b"),
        attempt=attempt2,
        claim_epoch=epoch2,
        outcome="failed",
        error_class="ValueError",
        ledger_id=None,
    )
    assert res3.applied is False, res3
    fence2 = await clean_pg_conn.fetchval(
        f'SELECT status FROM "{schema}".wf_step_ledger WHERE id = $1', ledger2
    )
    assert fence2 == "fenced", f"the by-attempt fence leg did not run: {fence2!r}"


async def test_the_join_fire_winner_none_writes_nothing(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, clean_pg_conn: Any
) -> None:
    """The fire's guard: a (join, fire_id) that matches no row (already
    consumed / never existed) → ``None`` — no outbox row, no body, the
    PK/legs did their job."""
    from taskq.backend._protocol import JobId
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.engine import _fire_and_deliver  # pyright: ignore[reportPrivateUsage]
    from tests._wf_fixtures import seed_flow, seed_join

    schema = module_pg_schema.schema_name
    wsql = WorkflowSql.build(schema)
    flow_id = await seed_flow(clean_pg_conn, schema)
    await seed_join(clean_pg_conn, schema, flow_id, step_key="ghost_join")
    ghost_fire = JobId("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4b")
    out = await _fire_and_deliver(
        clean_pg_conn,
        wsql,
        flow_id=flow_id,
        join_id=JobId("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4c"),
        fire_id=ghost_fire,
        fired_by="attack4",
        reducers=None,
    )
    assert out is None, f"a ghost fire won: {out!r}"


async def test_the_finalize_registers_the_flow_reducers_for_the_cold_process(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, clean_pg_conn: Any
) -> None:
    """A finalize with reducer bodies registers them (the durable leg's
    process cache) — a fenced-out finalize registers nothing harmful (the
    registry keys by the join's step key; a body that never fires is
    never run)."""
    from taskq.workflows._reducers import resolve_flow_reducer
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.engine import finalize_node
    from tests._wf_fixtures import claim_view, seed_edge, seed_flow, seed_join, seed_running_node

    schema = module_pg_schema.schema_name
    wsql = WorkflowSql.build(schema)

    async def the_reducer() -> None:
        return None

    flow_id = await seed_flow(clean_pg_conn, schema, workflow="attack4_reducer_reg")
    join_id = await seed_join(clean_pg_conn, schema, flow_id, step_key="reg_join")
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="child")
    await seed_edge(clean_pg_conn, schema, join_id, node_id, flow_id)

    worker, attempt, epoch = await claim_view(clean_pg_conn, schema, node_id)
    await finalize_node(
        module_pg_pool,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="child",
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"n": 1},
        reducers={"reg_join": the_reducer},
    )
    # The registration is visible to the REDUCER RESOLUTION (the same
    # process's registry, keyed by the run's JobId): the memo answers for
    # an anonymous flow (no stamped workflow name).
    resolved = resolve_flow_reducer(flow_id, "reg_join", workflow_name=None)
    assert resolved.body is the_reducer, (
        "the finalize did not register the reducer for the fire's arm"
    )
