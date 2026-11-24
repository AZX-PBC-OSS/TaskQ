# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""Three contract legs of the ``ctx.substep`` scope change.

Each pin below passes on the shipped code and dies when the leg it names is
mutated. The mutations reversed the memo's ``ORDER BY``, logged the legacy
warning unconditionally and respelled the loop key; the whole substep-touching
suite (``test_wf_substep_scope_pins.py`` + the five updated selectors' files,
110 tests) stayed green under them before these pins existed.

1. ``LEDGER_SUBSTEP_MEMOIZED_SQL``'s ``ORDER BY attempt DESC, claim_epoch DESC,
   created_at DESC, id DESC``. The statement's own comment calls the ordering
   "the claim history's own"; it decides WHICH terminal row answers when one
   scoped key has more than one. Reversed to ``ASC``, every committed pin stays
   green and a hold resume replays an EARLIER attempt's ``failed`` row, so the
   substep re-runs its side effect.
2. ``WorkflowSteps.step``'s ``if memo.step_key != name`` — the
   ``workflow-substep-legacy-replay`` warning. The upgrading guide makes that
   warning a detector: "Only pre-upgrade code writes bare rows ... One for a run
   started after the upgrade means an old executor ran part of that run." The
   committed pin asserts the warning FIRES for a bare row and never that it
   stays quiet for a scoped one, so logging unconditionally stays green.
3. The loop body's scoped key text, ``<job id>.iter<n>/<name>``, which
   ``docs/guides/upgrading.md`` publishes as the operator's replacement SQL.
   Every committed loop pin asserts per-iteration BEHAVIOUR, which any
   distinct-per-iteration spelling satisfies; changing the scope's ``.iter``
   segment leaves them green and the published SQL silently empty.
"""

from __future__ import annotations

import asyncpg
import structlog
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    Done,
    FlowRunner,
    GateDecl,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    step,
)
from taskq.workflows.api._hitl import HitlClient

_LEGACY_EVENT = "workflow-substep-legacy-replay"


class Approval(BaseModel):
    verdict: str


_GATES = (GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),)


async def _resolve_all_holds(client: HitlClient, flow_id: JobId) -> None:
    for hold in await client.list(run=flow_id):
        await client.resolve(hold.hold_id, {"verdict": "ok"})


async def test_a_substep_resume_replays_the_latest_attempts_record_not_an_earlier_failure(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Attempt 1's substep records ``failed``; attempt 2's re-run records
    ``succeeded`` and then holds. Attempt 3's resume must replay attempt 2's
    success — not attempt 1's older failure, which re-runs the side effect."""
    oks: list[str] = []

    async def boom() -> str:
        raise ValueError("the substep's own failure")

    async def ret(v: str) -> str:
        oks.append(v)
        return v

    async def body(ctx: StepContext) -> str:
        if ctx.attempt == 1:
            await ctx.substep("x", boom)
        got = await ctx.substep("x", ret, "v")
        await ctx.wait_signal(Approval, timeout_s=120.0)
        return got

    app = WorkflowApp()

    @app.workflow("substep_memo_latest_attempt")
    def wf() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=5, gates=_GATES))

    runner = FlowRunner(app.get("substep_memo_latest_attempt"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held", max_ticks=500) == "held"
    await _resolve_all_holds(HitlClient(wf_pool, schema=wf_schema), flow_id)
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert oks == ["v"], f"the resume re-ran the substep (a stale row answered): {oks}"
    assert await runner.result(flow_id) == "v"


async def test_a_scoped_substep_replay_does_not_log_the_legacy_replay_warning(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The upgrade's detector must stay SILENT for a scoped replay: its only
    meaning is "an old executor wrote this run's record". A node records a
    scoped substep, holds, resumes; the resume replays the scoped row and
    logs nothing."""
    calls: list[str] = []

    async def ret(v: str) -> str:
        calls.append(v)
        return v

    async def body(ctx: StepContext) -> str:
        got = await ctx.substep("x", ret, "v")
        await ctx.wait_signal(Approval, timeout_s=120.0)
        return got

    app = WorkflowApp()

    @app.workflow("substep_scoped_replay_is_silent")
    def wf() -> Promise[object]:
        return build(step(body, key="held", gates=_GATES))

    runner = FlowRunner(app.get("substep_scoped_replay_is_silent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held", max_ticks=500) == "held"
    await _resolve_all_holds(HitlClient(wf_pool, schema=wf_schema), flow_id)
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert calls == ["v"], f"the resume re-ran the substep: {calls}"
    legacy = [e for e in logs if e["event"] == _LEGACY_EVENT]
    assert legacy == [], f"a scoped replay logged the pre-upgrade detector: {legacy}"


async def test_a_loop_bodys_substep_rows_carry_the_published_iteration_key(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``docs/guides/upgrading.md`` publishes the loop substep key as
    ``job_id::text || '.iter<n>/<name>'`` — the shape an operator's
    replacement SQL selects on. Two iterations recording ``ctx.substep("x")``
    leave exactly those two keys beside the loop's own iteration rows."""
    calls: list[int] = []

    async def ret(i: int) -> int:
        calls.append(i)
        return i

    async def body(ctx: StepContext, carry: object) -> object:
        i = 0 if carry is None else int(str(carry))
        got = await ctx.substep("x", ret, i)
        if i >= 1:
            return Done({"last": got})
        return Refine(got + 1)

    app = WorkflowApp()

    @app.workflow("substep_loop_key_shape")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=5))

    runner = FlowRunner(app.get("substep_loop_key_shape"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert calls == [0, 1], calls
    job_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'ticker'",
        flow_id,
    )
    assert job_id is not None
    keys = await wf_conn.fetch(
        f'SELECT step_key FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1',
        flow_id,
    )
    substep_keys = sorted(r["step_key"] for r in keys if r["step_key"].startswith(str(job_id)))
    assert substep_keys == [f"{job_id}.iter0/x", f"{job_id}.iter1/x"], substep_keys
