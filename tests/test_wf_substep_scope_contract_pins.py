# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""Four contract legs of the ``ctx.substep`` scope change.

Each pin below passes on the shipped code and dies when the leg it names is
mutated. The mutations reversed the memo's ``ORDER BY``, logged the legacy
warning unconditionally and respelled the loop key; the whole substep-touching
suite (``test_wf_substep_scope_pins.py`` + the five updated selectors' files,
110 tests) stayed green under them before these pins existed.

1. ``LEDGER_SUBSTEP_MEMOIZED_SQL``'s ``ORDER BY claim_epoch DESC, attempt DESC,
   created_at DESC, id DESC``: the latest claim answers (the emit-park leg is
   ``test_wf_substep_chain_source_pins.py``); it decides WHICH terminal row answers when one
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
4. The phantom reap's fence on a ``ctx.substep`` row is provisional. The
   reap's live-claim exclusion reads the OWNING job's lease, which the
   duplicate refusal's orphan outlives (the node is already failed while the
   first call is still inside ``fn``), so the reap fences that row mid-call.
   The reap keeps a substep row's claim instant, and the claimant's own
   terminal write (row id + instant) still records the completed effect.
   A fence that re-stamps the instant (a claim fence, a newer claim) stays
   final, and ``WorkflowSteps._record_terminal`` logs
   ``workflow-substep-terminal-refused`` with the flow, the job, the row and
   the outcome it could not record.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
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
from taskq.workflows.engine import render_workflow_sql

_LEGACY_EVENT = "workflow-substep-legacy-replay"
_REFUSED_EVENT = "workflow-substep-terminal-refused"


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


async def _drive_to_a_live_orphan(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    name: str,
    in_loop: bool,
    release: asyncio.Event,
    tasks: list[asyncio.Task[Any]],
) -> tuple[JobId, JobId, str, asyncpg.Record]:
    """Drive a same-name refusal to a terminal flow whose first ``x`` call is
    still inside ``fn`` (parked on *release*): the node (or the loop) is
    failed, the flow terminal, the substep row ``running``. Returns the flow,
    the job, the substep key and the row (its ``updated_at`` is the claim
    instant). The caller owns *release* and *tasks*, so a failed assertion
    here never leaves the orphan parked."""
    entered = asyncio.Event()
    started: list[str] = []

    async def slow() -> str:
        started.append("slow")
        entered.set()
        await release.wait()
        return "slow"

    async def fast() -> str:
        return "fast"

    async def both(ctx: StepContext) -> list[str]:
        first = asyncio.create_task(ctx.substep("x", slow))

        async def second() -> Any:
            # `first` must have committed its 'running' ledger row and entered
            # `slow()` before the duplicate is raised, or the refusal can race
            # the claim and this test pins the wrong thing.
            await asyncio.wait_for(entered.wait(), timeout=10)
            return await ctx.substep("x", fast)

        other = asyncio.create_task(second())
        tasks.extend((first, other))
        return [str(v) for v in await asyncio.gather(first, other)]

    async def node_body(ctx: StepContext) -> list[str]:
        return await both(ctx)

    async def loop_body(ctx: StepContext, carry: object) -> object:
        return Done(await both(ctx))

    app = WorkflowApp()

    @app.workflow(name)
    def wf() -> Promise[object]:
        if in_loop:
            return build(loop("dup", loop_body, max_iterations=3))
        return build(step(node_body, key="dup", max_attempts=3))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=500) == "terminal"
    assert started == ["slow"], f"the refusal's shape changed: {started}"

    job_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'dup'",
        flow_id,
    )
    assert job_id is not None
    substep_key = f"{job_id}.iter0/x" if in_loop else f"{job_id}/x"
    row = await wf_conn.fetchrow(
        f"SELECT id, status, attempt, claim_epoch, updated_at "
        f'FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 AND step_key = $2',
        flow_id,
        substep_key,
    )
    assert row is not None and row["status"] == "running", row
    return flow_id, JobId(job_id), substep_key, row


@pytest.mark.parametrize("in_loop", [False, True], ids=["node", "loop-body"])
async def test_a_phantom_reaped_live_substep_still_records_its_completed_effect(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, in_loop: bool
) -> None:
    """The phantom reap fences a substep row whose ``fn`` is still running:
    the node already terminalized, the flow terminal, this worker alive
    inside the call. That fence is provisional. When ``fn`` returns, the
    claimant's own terminal write (its row id and claim instant) still
    records the completed effect, so the row ends ``succeeded`` with the
    result a later retry replays, and no refusal is logged."""
    release = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = []
    try:
        flow_id, _, _, row = await _drive_to_a_live_orphan(
            wf_conn,
            wf_schema,
            wf_pool,
            name=f"substep_reaped_orphan_{'loop' if in_loop else 'node'}",
            in_loop=in_loop,
            release=release,
            tasks=tasks,
        )
        reaped = await wf_conn.fetch(render_workflow_sql(wf_schema).phantom_reap)
        reaped_ids = {str(r["id"]) for r in reaped if str(r["flow_id"]) == str(flow_id)}
        # The node's flow has one live row, the orphan's; a loop flow also
        # leaves its iteration claim row for the reap.
        assert str(row["id"]) in reaped_ids, reaped
        assert in_loop or reaped_ids == {str(row["id"])}, reaped
        mid = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE id = $1', row["id"]
        )
        assert mid == "fenced", f"the reap no longer fences the live row: {mid}"

        with structlog.testing.capture_logs() as logs:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)

        after = await wf_conn.fetchrow(
            f'SELECT status, result FROM "{wf_schema}".wf_step_ledger WHERE id = $1', row["id"]
        )
        assert after is not None
        assert (after["status"], after["result"]) == ("succeeded", '"slow"'), dict(after)
        assert [e for e in logs if e["event"] == _REFUSED_EVENT] == [], logs
    finally:
        # Never leave `slow` parked on `release.wait()` behind a failed
        # assertion: the detached call is the thing under test, not a leak.
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("fence", ["claim_fence", "re_mint_then_reap"])
async def test_a_finally_fenced_substep_terminal_write_logs_the_flow_job_and_outcome(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, fence: str
) -> None:
    """A fence that re-stamps the claim instant is final: a claim fence
    (``LEDGER_FENCE_BY_ID_SQL``), or a newer claim of the same arbiter row
    that the phantom reap then fences at the NEW claimant's instant. The
    orphan's terminal write (``status IN (...)`` and the instant guard)
    matches no row, and the refusal must be traceable: one
    ``workflow-substep-terminal-refused`` warning naming the flow, the job,
    the substep key, the ledger row and the outcome it could not record
    (``succeeded``)."""
    release = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = []
    try:
        flow_id, job_id, substep_key, row = await _drive_to_a_live_orphan(
            wf_conn,
            wf_schema,
            wf_pool,
            name=f"substep_fenced_orphan_{fence}",
            in_loop=False,
            release=release,
            tasks=tasks,
        )
        wsql = render_workflow_sql(wf_schema)
        if fence == "claim_fence":
            fenced = await wf_conn.fetchval(
                wsql.ledger_fence_by_id, row["id"], "SyntheticClaimFence", row["updated_at"]
            )
            assert fenced == row["id"], fenced
        else:
            again = await wf_conn.fetchrow(
                wsql.ledger_claim,
                new_uuid(),
                flow_id,
                job_id,
                substep_key,
                None,
                row["attempt"],
                row["claim_epoch"],
            )
            assert again is not None and again["id"] == row["id"], again
            assert again["updated_at"] != row["updated_at"], again
            await wf_conn.fetch(wsql.phantom_reap)

        with structlog.testing.capture_logs() as logs:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)

        status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE id = $1', row["id"]
        )
        assert status == "fenced", status
        refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
        assert len(refused) == 1, logs
        entry = refused[0]
        assert entry["flow_id"] == str(flow_id), entry
        assert entry["job_id"] == str(job_id), entry
        assert entry["step_key"] == substep_key, entry
        assert entry["ledger_id"] == str(row["id"]), entry
        assert entry["status"] == "succeeded", entry
        assert entry["error_class"] is None, entry
        assert entry["reason"] == "fenced", entry
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
