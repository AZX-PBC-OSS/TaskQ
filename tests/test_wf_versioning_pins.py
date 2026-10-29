"""T29 — THE IN-FLIGHT VERSIONING CONTRACT's pins: the redeploy face.

THE DECISION (the registry-is-truth contract — the guide's versioning
section carries the full text): **a redeployed definition = the
in-flight runs adopt the NEW code at their NEXT node claim** — the
compile's STRUCTURE is the create-time snapshot (the nodes, the edges,
the wiring), but every BODY resolves from the CURRENT registry at the
claim seam (D1's resolution is per-claim, never per-run), and the
§22.1 stamp (``jobs.code_version`` — the per-node canonical content
hash written at claim) records WHICH code ran each row.

Red-first evidence: the pins ran against the PRE-IMPLEMENTATION tree —
the doc's TODAY was the silent behavior change (no contract text, no
stamp read taught); the captures ride
``.measurements/t29-versioning-pin-reds-pre.txt``. These pins are the
contract's OWN proof (the adoption + the stamp are the shipped
semantics, now named and taught), not mutation drills.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.api._hitl import sweep_due_timers
from taskq.workflows.definitions import WorkflowDef, get_registry


class Doc(BaseModel):
    doc_id: str


async def _first_v1(ctx: StepContext, params: Doc) -> str:
    return "first:v1"


async def _first_v2(ctx: StepContext, params: Doc) -> str:
    return "first:v2"


async def _second_v1(ctx: StepContext, params: str) -> str:
    return f"second:v1:{params}"


async def _second_v2(ctx: StepContext, params: str) -> str:
    return f"second:v2:{params}"


def _install_registry(name: str, bodies: dict[str, Any]) -> None:
    """THE REDEPLOY SEAM (the pin's honest simulation): in production a
    redeploy is a NEW PROCESS whose registry carries the new code — the
    module-level map simply holds the new definition. In-test the same
    map is replaced under the SAME workflow name (the registry IS the
    module-level map; the replacement is the deploy, and nothing else
    in the engine is touched)."""
    definition = WorkflowDef(name=name, bodies=bodies)
    registry = get_registry()
    registry._workflows[name] = definition  # pyright: ignore[reportPrivateUsage]  # Why: the redeploy simulation IS the registry map's replacement — the new process's registry holds only the new code; the test reproduces exactly that.


async def test_redeploy_adopts_the_new_body_at_the_next_node_claim(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, versioning_redlog: Any
) -> None:
    """THE REDEPLOY FACE (the contract's pin, both halves):

    * THE ADOPT — the flow's FIRST node runs the OLD code (its claim
      predates the deploy); the registry mutates mid-flow (the
      redeploy); the flow's SECOND node runs the NEW body — adopted at
      its NEXT node claim, the run never restarted, never wedged.
    * THE STAMP — ``jobs.code_version`` records WHICH code ran each
      row: the first row's stamp is the v1 body's canonical hash, the
      second row's is the v2 body's — the §22.1 deploy-drift read is
      the rows telling the truth about the drift.

    THE CONTRACT's edge cases the guide names (each re-enters the NEW
    body through the same claim-time resolution): a run mid-LOOP (the
    iteration's body re-resolution), a run mid-WAIT (the wake's resume
    re-enters the new body — the timer interplay pin below), a cached
    step (the cache's address INCLUDES the code_version — T25's own
    law: a redeploy is a new address, the new code's cache line)."""
    app = WorkflowApp()
    name = "redeploy_face_v1"

    @app.workflow(name)
    def redeploy_flow() -> Promise[object]:
        p = step(_first_v1, Doc(doc_id="d1"), key="first")
        return build(step(_second_v1, p, key="second"))

    compiled = app.get(name)  # the create-process's compile: the STRUCTURE snapshot
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # THE OLD CODE'S CLAIM: exactly one node runs — 'first' claims +
    # runs v1; 'second' is edge-blocked (deps_pending=1) and cannot
    # claim before the deploy.
    rows = await runner._claimable(flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the pin stops the driver mid-flow — the claim loop's seam is the behavior under test.
    assert rows, "no claimable node (the create did not land the roots?)"
    assert [r["step_key"] for r in rows] == ["first"], rows
    await runner._run_node(flow_id, rows[0])  # pyright: ignore[reportPrivateUsage]

    # THE REDEPLOY: the registry now carries the NEW code (the new
    # process's map). The SECOND node's body is v2 from here on.
    _install_registry(name, {"first": _first_v2, "second": _second_v2})

    await runner.drive(flow_id)
    verdict = await runner.result(flow_id)
    assert verdict == "second:v2:first:v1", (
        f"the in-flight run did NOT adopt the new code at the next claim: {verdict}"
    )

    # THE STAMPS: WHICH code ran each row — the per-node canonical hash.
    stamp_rows = await wf_conn.fetch(
        f'SELECT step_key, code_version FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__' ORDER BY id",
        flow_id,
    )
    stamps = {r["step_key"]: r["code_version"] for r in stamp_rows}
    assert stamps["first"] is not None and stamps["second"] is not None, stamps
    assert stamps["first"] != stamps["second"], (
        "the stamps record NO drift — the deploy is invisible"
    )
    node_first = compiled.nodes["first"]
    node_second = compiled.nodes["second"]
    expected_first = runner._body_code_version("first", node_first, _first_v1)  # pyright: ignore[reportPrivateUsage]  # Why: the stamp's canonical computation — the row's record is compared to the hash the runner itself writes for the body that ran.
    expected_second = runner._body_code_version("second", node_second, _second_v2)  # pyright: ignore[reportPrivateUsage]
    assert stamps["first"] == expected_first, stamps
    assert stamps["second"] == expected_second, stamps
    versioning_redlog.red(
        "pin-redeploy-silent-change",
        "the pre-contract tree (the doc's TODAY): a mid-flow deploy changed the "
        "behavior SILENTLY — no contract text, no taught stamp read",
        {
            "conviction": "the stamps landed with T03/§22.1; THIS pin pins the adopt + the stamp together"
        },
    )


async def test_redeploy_mid_wait_the_wake_reenters_the_new_body(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE CONTRACT's mid-WAIT edge case (the timer interplay — T28
    x T29): a run holding on a DURABLE TIMER when the registry mutates —
    the wake's resume re-enters the NEW body (the claim-time resolution
    is the only body source; a wake is a resume like any other). The
    stamp proves WHICH code ran the post-wake side, and the answer
    queue's replay law holds across the deploy: the new body's wait
    site consumes the ALREADY-FIRED wake (the operator's re-run never
    re-sleeps)."""

    async def wait_then_v1(ctx: StepContext, params: Doc) -> str:
        await ctx.wait_until(0.05, key="redeploy-wait")
        return "wake:v1"

    async def wait_then_v2(ctx: StepContext, params: Doc) -> str:
        await ctx.wait_until(120.0, key="redeploy-wait")
        return "wake:v2"

    app = WorkflowApp()
    name = "redeploy_mid_wait_v1"

    @app.workflow(name)
    def redeploy_wait_flow() -> Promise[object]:
        return build(step(wait_then_v1, Doc(doc_id="d1"), key="solo"))

    compiled = app.get(name)  # the create-process's compile: the STRUCTURE snapshot
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    # THE REDEPLOY, mid-wait: the registry carries the NEW body (whose
    # own wait site declares a far timer — different code, same node).
    _install_registry(name, {"solo": wait_then_v2})

    await asyncio.sleep(0.12)  # the OLD hold's deadline passes
    fired = await sweep_due_timers(wf_pool, schema=wf_schema)
    assert fired == 1, fired
    await runner.drive(flow_id)
    verdict = await runner.result(flow_id)
    assert verdict == "wake:v2", f"the wake did NOT re-enter the new body: {verdict}"
    stamp = await wf_conn.fetchval(
        f'SELECT code_version FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'solo'",
        flow_id,
    )
    assert stamp is not None
    node = compiled.nodes["solo"]
    expected = runner._body_code_version("solo", node, wait_then_v2)  # pyright: ignore[reportPrivateUsage]  # Why: the stamp's canonical computation — the row's record names the body that RAN.
    assert stamp == expected, "the stamp does not record the NEW code on the wake's row"
