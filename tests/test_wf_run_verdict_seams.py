"""THE RUN-VERDICT SEAMS: what the packaged run door and the runner's read
face report when a run ends on an edge the adoption cures just made loud.

* a FRESH run's failure carries NO claim verdict — ``claim_verdict`` is the
  replay face's marker (``"existing-terminal"``), never a label every raise
  wears;
* a cancel landing between the read's status poll and its terminal-row
  read still raises the cancelled verdict — the race must not turn a
  cancelled run back into the empty read the cure exists to remove;
* a route arm declaring ``reservations=None`` (the accepted "no
  reservations" spelling on every face) forks its children like an arm
  that declares nothing at all.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.workflows import (
    Expired,
    FlowRunner,
    GateDecl,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    cancel_workflow_run,
    route,
    sink,
    step,
)

pytestmark = pytest.mark.integration

# ruff: noqa: S608  # Why: every schema interpolation below is a fixture-derived test identifier, never user input; every value is $-bound.


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Item(BaseModel):
    v: int


class Approval(BaseModel):
    verdict: str


async def test_a_fresh_failure_raises_without_a_claim_verdict(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """``WorkflowRunError.claim_verdict`` is the REPLAY face's marker:
    the class contract says ``None`` on every raise that is not the replay
    face, and the guide says a non-None value tells the handler "this
    raise is the replay refusal, not a fresh failure". A first run's own
    failure must therefore wear no verdict — a handler branching on the
    attribute would otherwise be told to pick a new run key for a run it
    created moments earlier."""
    from taskq.workflows import WorkflowRunError, run

    app = WorkflowApp()

    async def doomed(ctx: StepContext, params: Ingest) -> Report:
        raise RuntimeError("this run's own first failure")

    @app.workflow("fresh_failure_no_replay_verdict")
    def wf() -> Promise[object]:
        return build(step(doomed, Ingest(doc_id="d1"), key="only", max_attempts=1))

    with pytest.raises(WorkflowRunError) as exc_info:
        await run(app.get("fresh_failure_no_replay_verdict"), wf_pool, wf_schema)

    assert exc_info.value.claim_verdict is None, (
        "a FIRST run's failure wore the replay marker: "
        f"claim_verdict={exc_info.value.claim_verdict!r}. The attribute is "
        "the replay face's signal, so this raise now claims its run key was "
        "already terminal when the run was created moments earlier"
    )


async def test_a_cancel_landing_after_the_status_read_still_raises_cancelled(
    wf_pool: asyncpg.Pool, wf_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE READ'S CANCEL RACE: ``result()`` polls the run's status, then
    reads the terminal node's row. An operator's cancel is a single flip
    of the root row; when it lands in the window between those two reads
    the poll saw ``running`` and the terminal row's result is NULL. The
    cure's stated property is that a cancelled run's read is LOUD — a
    poller must never be handed the empty read (which this same cure
    removed because it made cancelled and not-yet-terminal
    indistinguishable) for a run that is now terminal-cancelled.
    """
    from taskq.workflows import WorkflowRunCancelledError

    app = WorkflowApp()

    @app.actor(queue="default")
    async def waiter(ctx: StepContext, params: Ingest) -> Report:
        outcome = await ctx.wait_signal((Approval,), reason="cancel-race", timeout_s=None)
        match outcome:
            case Approval():
                return Report(ref=params.doc_id)
            case Expired():
                return Report(ref="expired")
        raise AssertionError("unreachable: the wait's union is closed")  # pragma: no cover

    gate = GateDecl(name="Approval", payload_models=(Approval,))

    @app.workflow("cancelled_read_race")
    def wf() -> Promise[object]:
        return build(step(waiter, Ingest(doc_id="d1"), key="hold", gates=(gate,), max_attempts=1))

    runner = FlowRunner(app.get("cancelled_read_race"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held", max_ticks=10) == "held"

    real_status = runner._flow_status  # pyright: ignore[reportPrivateUsage]  # Why: the scheduling seam — the test interleaves the cancel exactly inside result()'s own two reads.
    cancelled = False

    async def status_then_cancel(fid: Any) -> str:
        nonlocal cancelled
        status = await real_status(fid)
        if not cancelled:
            cancelled = True
            await cancel_workflow_run(
                wf_pool,
                flow_id=fid,
                schema=wf_schema,
                reason="the operator cancelled during the read",
                principal=None,
            )
        return status

    monkeypatch.setattr(runner, "_flow_status", status_then_cancel)

    with pytest.raises(WorkflowRunCancelledError):
        await runner.result(flow_id)

    # The interleaving really happened (the hook ran once and the run is
    # terminal-cancelled on its root row); a green here means the read saw
    # the cancel, never that the hook was skipped.
    assert cancelled
    root_status = await wf_pool.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root_status == "cancelled"


async def test_a_route_arm_declaring_no_reservations_forks_its_children(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """``reservations=None`` is the accepted "nothing reserved" spelling
    on every face, the route arm included. An arm that declares it is
    the same arm as one that declares nothing: the source finalizes, the
    fork stamps its children, the join fires, and the run terminalizes
    with the arms' results — never a fork-time crash for the accepted
    no-op spelling.
    """
    app = WorkflowApp()

    async def source(ctx: StepContext) -> list[Item]:
        return [Item(v=1), Item(v=2)]

    async def arm(ctx: object, item: Item) -> Report:
        return Report(ref=str(item.v))

    @app.workflow("route_arm_none_reservations")
    def wf() -> Promise[object]:
        src = step(source, key="source")
        routed = route(src, {Item: RouteArm(body=arm, reservations=None)})
        return build(routed)

    runner = FlowRunner(app.get("route_arm_none_reservations"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    result = await runner.result(flow_id)
    assert isinstance(result, list) and len(result) == 2, (
        f"the route with a reservations=None arm did not produce its two "
        f"children's results: {result!r}"
    )


async def test_a_succeeded_terminal_beside_a_failed_sink_branch_still_raises_the_failure(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """THE ENDED-RUN VERDICT IS THE RUN'S, NOT THE TERMINAL NODE'S: a
    flow may finish its terminal node while a fire-and-forget branch
    (`sink(...)`) is still live; the branch's failure terminalises the
    RUN as failed (an unabsorbed failure). The read must raise that
    failure, not decode the succeeded terminal row and present the run as
    a success — a failed run whose read returns a result is the
    failure-looks-like-success shape the read face exists to prevent.
    """
    from taskq.workflows import WorkflowRunError

    app = WorkflowApp()

    async def main(ctx: StepContext) -> Report:
        return Report(ref="the terminal's own result")

    async def side(ctx: StepContext) -> Report:
        raise RuntimeError("the fire-and-forget branch failed")

    @app.workflow("terminal_ok_side_branch_failed")
    def wf() -> Promise[object]:
        finished = step(main, key="finished")
        sink(step(side, key="side", max_attempts=1))
        return build(finished)

    runner = FlowRunner(app.get("terminal_ok_side_branch_failed"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    # The precondition: the run itself derived FAILED (the side branch is
    # unabsorbed) even though the terminal node row succeeded.
    run_status = await runner._flow_status(flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the run's derived status is the precondition this pin reads; the read face's own verdict is the assertion.
    assert run_status == "failed", (
        f"the side branch's failure did not terminalise the run: {run_status!r}"
    )

    with pytest.raises(WorkflowRunError):
        await runner.result(flow_id)


async def test_a_cancelled_run_whose_terminal_succeeded_still_raises_cancelled(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """THE TERMINAL NODE IS NOT THE RUN: a flow's terminal step can
    succeed while a sibling branch is still held; the operator then
    cancels the run. The read must raise the cancelled verdict — the
    cure's own property — not return the succeeded terminal's result and
    present a cancelled run as a completed one.
    """
    from taskq.workflows import WorkflowRunCancelledError

    app = WorkflowApp()

    async def main(ctx: StepContext) -> Report:
        return Report(ref="the terminal finished first")

    @app.actor(queue="default")
    async def waiter(ctx: StepContext, params: Ingest) -> Report:
        outcome = await ctx.wait_signal((Approval,), reason="sibling-hold", timeout_s=None)
        match outcome:
            case Approval():
                return Report(ref=params.doc_id)
            case Expired():
                return Report(ref="expired")
        raise AssertionError("unreachable: the wait's union is closed")  # pragma: no cover

    gate = GateDecl(name="Approval", payload_models=(Approval,))

    @app.workflow("terminal_ok_run_cancelled")
    def wf() -> Promise[object]:
        finished = step(main, key="finished")
        sink(step(waiter, Ingest(doc_id="d1"), key="side", gates=(gate,), max_attempts=1))
        return build(finished)

    runner = FlowRunner(app.get("terminal_ok_run_cancelled"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held", max_ticks=10) == "held"
    await cancel_workflow_run(
        wf_pool, flow_id=flow_id, schema=wf_schema, reason="the operator cancelled", principal=None
    )

    with pytest.raises(WorkflowRunCancelledError):
        await runner.result(flow_id)


async def test_an_adopted_live_run_s_failure_wears_no_replay_verdict(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """THE OTHER NON-REPLAY CLAIM VERDICT: a re-drive of a still-live run
    claims it ``existing-running`` and then executes it. When that drive's
    step fails, the raise is a FRESH failure of this drive — not the
    terminal replay — so it carries no ``claim_verdict`` either. Only
    ``existing-terminal`` is the replay face.
    """
    from taskq.workflows import WorkflowRunError, run

    app = WorkflowApp()

    async def doomed(ctx: StepContext, params: Ingest) -> Report:
        raise RuntimeError("the adopted run's step failed")

    @app.workflow("adopted_live_run_failure")
    def wf() -> Promise[object]:
        return build(step(doomed, Ingest(doc_id="d1"), key="only", max_attempts=1))

    compiled = app.get("adopted_live_run_failure")
    key = f"adopted-live-{new_uuid()}"
    first = FlowRunner(compiled, wf_pool, wf_schema)
    await first.create_flow(run_key=key)

    with pytest.raises(WorkflowRunError) as exc_info:
        await run(compiled, wf_pool, wf_schema, key=key)

    assert exc_info.value.claim_verdict is None, (
        "an adopted LIVE run's own failure wore the replay marker: "
        f"claim_verdict={exc_info.value.claim_verdict!r}"
    )


async def test_a_replayed_cancelled_run_carries_the_replay_verdict(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """THE REPLAY FACE, CANCELLED FAMILY: replaying a terminal CANCELLED
    run's key re-raises the cancelled verdict with the claim verdict
    attached — the same replay contract the failed-run replay carries,
    across the new error family.
    """
    from taskq.workflows import WorkflowRunCancelledError, run

    app = WorkflowApp()

    async def main(ctx: StepContext) -> Report:
        return Report(ref="never reached")

    @app.workflow("replayed_cancelled_run")
    def wf() -> Promise[object]:
        return build(step(main, key="only", max_attempts=1))

    compiled = app.get("replayed_cancelled_run")
    key = f"replayed-cancelled-{new_uuid()}"
    first = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await first.create_flow(run_key=key)).flow_id
    await cancel_workflow_run(
        wf_pool,
        flow_id=flow_id,
        schema=wf_schema,
        reason="cancelled before the replay",
        principal=None,
    )

    with pytest.raises(WorkflowRunCancelledError) as exc_info:
        await run(compiled, wf_pool, wf_schema, key=key)
    assert exc_info.value.claim_verdict == "existing-terminal"
