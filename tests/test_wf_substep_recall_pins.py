# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""Same-attempt ``ctx.substep`` re-call after a caught failure.

When a workflow body catches an exception from ``ctx.substep(name, fn)`` and
re-calls ``ctx.substep(name, fn)`` with the same name in the same attempt, the
re-call must record its own terminal outcome on the substep's ledger row
without misreporting a fence warning, so that subsequent calls in the same
attempt and later retries/resumes replay the recorded ``succeeded`` result
instead of re-executing ``fn``.

The file has two halves. The first drives the core re-call contract end to
end through a real ``FlowRunner`` (plain step retry, same-attempt third call,
map child, loop hold resume, and latest-error replacement). The second half
pins the substep claim statement itself — its guard legs, settled-row
preservation of every outcome column, and refused terminal writes with their
classified causes — together with the edge arms and interleavings the
``failed -> running`` re-open makes reachable (JSON-``null`` memo and
claim-leg replay, the retired-failure log and cross-generation silence, the
provisional phantom fence, the abandoned re-open closed by the phantom reap,
and a duplicate delivery inside the window).
"""

from __future__ import annotations

import asyncio
import contextlib
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
    StepContext,
    WorkflowApp,
    build,
    loop,
    map_source,
    step,
)
from taskq.workflows._sweep import reap_phantom_ledger
from taskq.workflows.api._hitl import HitlClient
from taskq.workflows.context import WorkflowSteps
from taskq.workflows.engine import render_workflow_sql
from taskq.workflows.ledger import (
    LedgerClaim,
    claim_substep_ledger,
    memoized_substep_result,
)
from tests._wf_fixtures import seed_flow, seed_running_node

_REFUSED_EVENT = "workflow-substep-terminal-refused"
_RETIRED_EVENT = "workflow-substep-failure-retired"


class _Item(BaseModel):
    n: int


class _Approval(BaseModel):
    verdict: str


_GATES = (GateDecl(name="_Approval", payload_models=(_Approval,), timeout_s=120.0),)


async def _resolve_all_holds(client: HitlClient, flow_id: JobId) -> None:
    for hold in await client.list(run=flow_id):
        await client.resolve(hold.hold_id, {"verdict": "ok"})


async def test_same_attempt_substep_recall_after_failure_records_success_and_replays_on_retry(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Plain step: attempt 1 catches a failed substep, re-calls the same
    substep name and succeeds, then fails later in the body. Attempt 2 must
    replay the recorded success without re-running ``fn``, and no false fence
    warning is logged."""
    calls: list[str] = []

    async def flaky() -> str:
        if not calls:
            calls.append("fail")
            raise ValueError("transient substep failure")
        calls.append("ok")
        return "recovered"

    async def body(ctx: StepContext) -> str:
        if ctx.attempt == 1:
            with contextlib.suppress(ValueError):
                await ctx.substep("x", flaky)
        got = await ctx.substep("x", flaky)
        if ctx.attempt == 1:
            raise RuntimeError("downstream failure after inline recovery")
        return got

    app = WorkflowApp()

    @app.workflow("substep_recall_plain_retry")
    def wf() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=3))

    runner = FlowRunner(app.get("substep_recall_plain_retry"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    assert await runner.result(flow_id) == "recovered"
    assert calls == ["fail", "ok"], f"next attempt re-ran recovered substep: {calls}"
    refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
    assert refused == [], f"re-call after caught failure logged a false fence refusal: {refused}"

    job_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'solo'",
        flow_id,
    )
    rows = await wf_conn.fetch(
        f'SELECT attempt, status, result, error_class FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = $2 ORDER BY attempt, claim_epoch",
        flow_id,
        f"{job_id}/x",
    )
    assert [(r["attempt"], r["status"], r["result"], r["error_class"]) for r in rows] == [
        (1, "succeeded", '"recovered"', None)
    ], [dict(r) for r in rows]


async def test_same_attempt_substep_recall_replays_within_same_attempt(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Within a single attempt: call 1 fails (caught), call 2 succeeds, and
    call 3 with the same name replays call 2's recorded value without running
    ``fn`` a third time."""
    calls: list[str] = []

    async def flaky() -> str:
        if not calls:
            calls.append("fail")
            raise ValueError("transient substep failure")
        calls.append("ok")
        return "recovered"

    async def body(ctx: StepContext) -> list[str]:
        with contextlib.suppress(ValueError):
            await ctx.substep("x", flaky)
        second = await ctx.substep("x", flaky)
        third = await ctx.substep("x", flaky)
        return [second, third]

    app = WorkflowApp()

    @app.workflow("substep_recall_same_attempt_replay")
    def wf() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=2))

    runner = FlowRunner(app.get("substep_recall_same_attempt_replay"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    assert await runner.result(flow_id) == ["recovered", "recovered"]
    assert calls == ["fail", "ok"], f"third call in same attempt re-ran substep: {calls}"
    refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
    assert refused == [], f"re-call after caught failure logged a false fence refusal: {refused}"


async def test_same_attempt_substep_recall_in_map_child_replays_on_child_retry(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Map child: attempt 1 catches a failed substep, re-calls it and succeeds,
    then fails downstream. Attempt 2 of each map child must replay its own
    recorded success without re-running ``fn``."""
    calls: dict[int, list[str]] = {0: [], 1: []}

    async def flaky(n: int) -> str:
        if not calls[n]:
            calls[n].append("fail")
            raise ValueError(f"transient item {n}")
        calls[n].append("ok")
        return f"val-{n}"

    async def source(ctx: StepContext) -> list[_Item]:
        return [_Item(n=0), _Item(n=1)]

    async def child(ctx: StepContext, item: _Item) -> str:
        if ctx.attempt == 1:
            with contextlib.suppress(ValueError):
                await ctx.substep("fetch", flaky, item.n)
        got = await ctx.substep("fetch", flaky, item.n)
        if ctx.attempt == 1:
            raise RuntimeError(f"child {item.n} fails after inline recovery")
        return got

    app = WorkflowApp()

    @app.workflow("substep_recall_map_child_retry")
    def wf() -> Promise[object]:
        return build(map_source(step(source, key="src"), child, max_attempts=3))

    runner = FlowRunner(app.get("substep_recall_map_child_retry"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    assert await runner.result(flow_id) == ["val-0", "val-1"]
    assert calls == {0: ["fail", "ok"], 1: ["fail", "ok"]}, (
        f"map child retry re-ran recovered substep: {calls}"
    )
    refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
    assert refused == [], f"map child re-call logged a false fence refusal: {refused}"


async def test_same_attempt_substep_recall_in_loop_iteration_replays_on_hold_resume(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Loop iteration: iteration 0 catches a failed substep, re-calls it and
    succeeds, then holds on a signal. Resuming iteration 0 under a new claim
    epoch must replay the recorded success without re-running ``fn``."""
    calls: list[str] = []
    first_pass = True

    async def flaky() -> str:
        if not calls:
            calls.append("fail")
            raise ValueError("transient loop substep failure")
        calls.append("ok")
        return "iter0-ok"

    async def body(ctx: StepContext, carry: object) -> object:
        nonlocal first_pass
        if first_pass:
            first_pass = False
            with contextlib.suppress(ValueError):
                await ctx.substep("work", flaky)
        got = await ctx.substep("work", flaky)
        await ctx.wait_signal(_Approval, timeout_s=120.0)
        return Done({"got": got})

    app = WorkflowApp()

    @app.workflow("substep_recall_loop_hold_resume")
    def wf() -> Promise[object]:
        return build(loop("ticker", body, max_iterations=3, gates=_GATES))

    runner = FlowRunner(app.get("substep_recall_loop_hold_resume"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id, until="held", max_ticks=500) == "held"
        await _resolve_all_holds(HitlClient(wf_pool, schema=wf_schema), flow_id)
        assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    assert await runner.result(flow_id) == {"got": "iter0-ok"}
    assert calls == ["fail", "ok"], f"loop hold resume re-ran recovered substep: {calls}"
    refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
    assert refused == [], f"loop iteration re-call logged a false fence refusal: {refused}"


async def test_same_attempt_substep_recall_failing_again_records_latest_error(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """When a same-attempt re-call fails again with a new exception, the
    substep ledger row records the latest error instead of refusing the write
    and keeping the first call's stale error."""

    async def fail_first() -> str:
        raise ValueError("first-err")

    async def fail_second() -> str:
        raise KeyError("second-err")

    async def body(ctx: StepContext) -> str:
        with contextlib.suppress(ValueError):
            await ctx.substep("x", fail_first)
        with contextlib.suppress(KeyError):
            await ctx.substep("x", fail_second)
        return "done"

    app = WorkflowApp()

    @app.workflow("substep_recall_second_failure")
    def wf() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=1))

    runner = FlowRunner(app.get("substep_recall_second_failure"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    assert await runner.result(flow_id) == "done"

    job_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'solo'",
        flow_id,
    )
    row = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = $2",
        flow_id,
        f"{job_id}/x",
    )
    assert row is not None
    assert row["status"] == "failed", dict(row)
    assert row["error_class"] == "KeyError", dict(row)
    assert "second-err" in str(row["error_message"]), dict(row)
    refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
    assert refused == [], f"second failure re-call logged a false fence refusal: {refused}"


# ── the substep claim statement: its guard legs, edge arms and interleavings ──
# The pins below hold the properties the substep claim statement trades on —
# only 'failed' re-opens (so every settled column survives), the claim
# re-stamps a live row and the stale terminal is refused — and the arms that
# surround them: the exact failed row a re-open retired, the refusal's own
# cause, the abandoned re-open the phantom reap must close, and a duplicate
# delivery inside the window. A future edit to the conflict arm reds here
# instead of silently re-breaking the replay.


async def _seed_ledger_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    *,
    flow_id: JobId,
    job_id: JobId,
    status: str,
    map_index: int | None = None,
    result: str | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
    capture: str | None = None,
) -> asyncpg.Record:
    """One ``wf_step_ledger`` row at the substep keys this file uses
    (``<job id>/x``, attempt 1, claim epoch 0). No flow or job row is
    needed: ``wf_step_ledger`` carries no foreign key."""
    row_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, "
        " status, result, error_class, error_message, capture) "
        "VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7::jsonb, $8, $9, $10::jsonb)",
        row_id,
        flow_id,
        job_id,
        f"{job_id}/x",
        map_index,
        status,
        result,
        error_class,
        error_message,
        capture,
    )
    row = await wf_conn.fetchrow(
        f"SELECT id, status, result, error_class, error_message, capture, updated_at "
        f'FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        row_id,
    )
    assert row is not None
    return row


async def _ledger_row(
    wf_conn: asyncpg.Connection, wf_schema: str, ledger_id: JobId | None
) -> dict[str, object]:
    """One ledger row's settled state, by its own id."""
    assert ledger_id is not None
    rec = await wf_conn.fetchrow(
        f"SELECT status, result, error_class, error_message, updated_at "
        f'FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        ledger_id,
    )
    assert rec is not None
    return dict(rec)


async def test_substep_claim_reopens_the_callers_failed_row(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """Guard (a): colliding with the caller's own ``failed`` row re-opens it to
    ``running`` and clears that run's outcome, so the row is truthful while
    ``fn`` runs — which is what ``PHANTOM_REAP_SQL`` and
    ``wf_step_ledger_running_idx`` both read — and the re-call's terminal
    write has a row to land on."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    seeded = await _seed_ledger_row(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        job_id=job_id,
        status="failed",
        result='"first"',
        error_class="ValueError",
        error_message="boom",
        capture='{"k":1}',
    )

    claim = await claim_substep_ledger(
        wf_conn,
        wsql,
        flow_id=flow_id,
        job_id=job_id,
        step_key=f"{job_id}/x",
        map_index=None,
        attempt=1,
        claim_epoch=0,
    )
    assert claim.ledger_id == JobId(seeded["id"]), claim
    assert claim.status == "running", claim
    assert claim.result is None, claim
    assert claim.error_class is None and claim.error_message is None, claim
    # A conflicting claim never inherits the instant the earlier call wrote
    # at: the terminal write keys that instant, so keeping it would let a
    # stale claimant's write land.
    assert claim.claimed_at != seeded["updated_at"], claim

    row = await wf_conn.fetchrow(
        f"SELECT status, result, error_class, error_message, capture "
        f'FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        seeded["id"],
    )
    assert row is not None
    assert dict(row) == {
        "status": "running",
        "result": None,
        "error_class": None,
        "error_message": None,
        "capture": None,
    }, dict(row)


@pytest.mark.parametrize("status", ["succeeded", "fenced"])
async def test_substep_claim_preserves_a_settled_row(
    wf_conn: asyncpg.Connection, wf_schema: str, status: str
) -> None:
    """Guard (b): only ``failed`` re-opens. A ``succeeded`` row keeps its
    result (the colliding claim replays it rather than re-opening work) and a
    ``fenced`` row stays fenced with its error — the claim re-stamps the
    instant either way, which is what makes the earlier claimant's terminal
    write refusable.

    Every outcome column is seeded with a value on BOTH settled statuses, and
    all five survive verbatim (nothing cleared, nothing invented). A real run
    fills a subset per status, but every ``CASE`` arm in the conflict clause is
    part of the preservation contract: narrowing one to "the columns
    production fills today" must red here, not pass because the other status
    happened to seed that column NULL."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    seeded = await _seed_ledger_row(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        job_id=job_id,
        status=status,
        result='"kept"',
        error_class="KeptClass",
        error_message="kept-message",
        capture='{"io": "kept"}',
    )

    claim = await claim_substep_ledger(
        wf_conn,
        wsql,
        flow_id=flow_id,
        job_id=job_id,
        step_key=f"{job_id}/x",
        map_index=None,
        attempt=1,
        claim_epoch=0,
    )
    assert claim.ledger_id == JobId(seeded["id"]), claim
    assert claim.status == status, claim
    assert claim.result == "kept", claim

    row = await wf_conn.fetchrow(
        f"SELECT status, result, error_class, error_message, capture, updated_at "
        f'FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        seeded["id"],
    )
    assert row is not None
    assert (
        row["status"],
        row["result"],
        row["error_class"],
        row["error_message"],
        row["capture"],
    ) == (
        status,
        '"kept"',
        "KeptClass",
        "kept-message",
        '{"io": "kept"}',
    ), dict(row)
    assert row["updated_at"] != seeded["updated_at"], dict(row)


async def test_substep_claim_restamps_a_live_row_and_refuses_the_stale_terminal(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Guard (c): a newer substep claim on a live ``running`` row re-stamps
    ``updated_at``, so the earlier claimant's terminal write matches no row
    and logs exactly one ``workflow-substep-terminal-refused`` — while the
    newer claimant's own write lands. The refusal names its cause: a newer
    claim is ``superseded``, and no fence was involved. This is the re-mint
    guard ``LEDGER_FENCE_BY_ID_SQL`` documents, pinned against this
    statement."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    keys: dict[str, Any] = {
        "flow_id": flow_id,
        "job_id": job_id,
        "step_key": f"{job_id}/x",
        "map_index": None,
        "attempt": 1,
        "claim_epoch": 0,
    }
    stale = await claim_substep_ledger(wf_conn, wsql, **keys)
    newer = await claim_substep_ledger(wf_conn, wsql, **keys)
    assert newer.ledger_id == stale.ledger_id, (stale, newer)
    assert newer.status == "running", newer
    assert newer.claimed_at != stale.claimed_at, (stale, newer)

    steps = WorkflowSteps(
        wf_pool,
        wsql,
        flow_id=flow_id,
        job_id=job_id,
        attempt=1,
        claim_epoch=0,
    )
    with structlog.testing.capture_logs() as logs:
        await steps._record_terminal("x", stale, "succeeded", "stale", None, None)
    refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
    assert len(refused) == 1, logs
    assert refused[0]["reason"] == "superseded", refused[0]
    assert refused[0]["ledger_id"] == str(stale.ledger_id), refused[0]

    await steps._record_terminal("x", newer, "succeeded", "fresh", None, None)
    row = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        stale.ledger_id,
    )
    assert row is not None
    assert (row["status"], row["result"]) == ("succeeded", '"fresh"'), dict(row)


async def test_a_superseded_and_a_retention_deleted_refusal_name_their_cause(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A refused substep terminal write says WHICH writer took the row.

    Both legs are production-reachable and neither involves a fence: a newer
    claim of the same arbitration row (a duplicate execution) and retention
    having deleted the aged row. The record must separate them —
    ``reason="superseded"`` versus ``reason="missing"`` — so a responder can
    tell "another execution owns this row" from "the row is gone" without a
    manual lookup. A genuine by-id fence stays ``reason="fenced"``, pinned by
    ``test_wf_substep_scope_contract_pins.py::
    test_a_finally_fenced_substep_terminal_write_logs_the_flow_job_and_outcome``.
    """
    wsql = render_workflow_sql(wf_schema)
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    steps = WorkflowSteps(
        wf_pool,
        wsql,
        flow_id=flow_id,
        job_id=job_id,
        attempt=1,
        claim_epoch=0,
    )

    # leg A — a newer claim re-stamped the instant (a duplicate execution).
    keys: dict[str, Any] = {
        "flow_id": flow_id,
        "job_id": job_id,
        "step_key": f"{job_id}/a",
        "map_index": None,
        "attempt": 1,
        "claim_epoch": 0,
    }
    stale = await claim_substep_ledger(wf_conn, wsql, **keys)
    newer = await claim_substep_ledger(wf_conn, wsql, **keys)
    assert newer.claimed_at != stale.claimed_at, (stale, newer)
    with structlog.testing.capture_logs() as superseded_logs:
        # the scoped key is what production passes: ctx.substep hands
        # WorkflowSteps.step the ``<job id>/<name>`` key, not the bare name.
        await steps._record_terminal(f"{job_id}/a", stale, "succeeded", "stale", None, None)

    # leg B — the row is gone (retention physically deletes aged ledger rows).
    pruned = await claim_substep_ledger(wf_conn, wsql, **{**keys, "step_key": f"{job_id}/b"})
    await wf_conn.execute(
        f'DELETE FROM "{wf_schema}".wf_step_ledger WHERE id = $1', pruned.ledger_id
    )
    with structlog.testing.capture_logs() as deleted_logs:
        await steps._record_terminal(f"{job_id}/b", pruned, "succeeded", "pruned", None, None)

    superseded = [e for e in superseded_logs if e["event"] == _REFUSED_EVENT]
    deleted = [e for e in deleted_logs if e["event"] == _REFUSED_EVENT]
    assert len(superseded) == 1 and len(deleted) == 1, (superseded_logs, deleted_logs)
    assert superseded[0]["reason"] == "superseded", superseded[0]
    assert deleted[0]["reason"] == "missing", deleted[0]
    # Both refusals keep the level and the outcome they could not record.
    assert superseded[0]["log_level"] == deleted[0]["log_level"] == "warning"
    assert superseded[0]["status"] == deleted[0]["status"] == "succeeded"


async def test_same_attempt_substep_recall_records_and_replays_json_null(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A recovered call whose result is JSON null still terminalizes once and
    replays by status, both later in its own attempt and on a later attempt;
    ``None`` is the recorded value, not evidence that no record exists."""
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    key = f"{job_id}/x"
    seeded = await _seed_ledger_row(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        job_id=job_id,
        status="failed",
        error_class="ValueError",
        error_message="first-err",
    )
    calls = 0

    async def recover() -> None:
        nonlocal calls
        calls += 1

    steps = WorkflowSteps(
        wf_pool, wsql=render_workflow_sql(wf_schema), flow_id=flow_id, job_id=job_id
    )
    assert await steps.step(key, recover) is None
    assert await steps.step(key, recover) is None
    retry = WorkflowSteps(
        wf_pool,
        wsql=render_workflow_sql(wf_schema),
        flow_id=flow_id,
        job_id=job_id,
        attempt=2,
        claim_epoch=1,
    )
    assert await retry.step(key, recover) is None

    assert calls == 1, "a recorded JSON-null success re-executed instead of replaying"
    row = await wf_conn.fetchrow(
        f'SELECT status, result, error_class FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        seeded["id"],
    )
    assert row is not None
    assert (row["status"], row["result"], row["error_class"]) == (
        "succeeded",
        "null",
        None,
    ), dict(row)


async def test_a_stale_executor_replays_its_recorded_null_success_via_the_claim_leg(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The claim leg replays a recorded JSON-``null`` success exactly like the
    memo leg.

    A stale execution (attempt 1, epoch 0) records a ``None`` effect. A newer
    generation (attempt 2, epoch 2) then records a ``failed`` row for the same
    key, which outranks the stale row in the memo's order — so the zombie's
    next call reads the newer failure as its memo and the CLAIM is the leg
    that answers, returning the zombie's own ``succeeded`` row. The recorded
    ``None`` is the value, never evidence that nothing was recorded: ``fn``
    must not run again. And because the claim re-opened no failed row (the
    memo's row is another generation's), it must not announce a retirement.
    """
    wsql = render_workflow_sql(wf_schema)
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    key = f"{job_id}/x"
    calls: list[str] = []

    async def null_effect() -> None:
        calls.append("ran")

    zombie = WorkflowSteps(wf_pool, wsql, flow_id=flow_id, job_id=job_id, attempt=1, claim_epoch=0)
    assert await zombie.step(key, null_effect) is None
    assert calls == ["ran"], calls

    # The newer generation's failure. ``_record_terminal`` seeds the exact row
    # production's terminal write records; driving a real second generation
    # would add its own job row/fence and obscure the guard under test.
    newer_claim = await claim_substep_ledger(
        wf_conn,
        wsql,
        flow_id=flow_id,
        job_id=job_id,
        step_key=key,
        map_index=None,
        attempt=2,
        claim_epoch=2,
    )
    newer = WorkflowSteps(wf_pool, wsql, flow_id=flow_id, job_id=job_id, attempt=2, claim_epoch=2)
    await newer._record_terminal(key, newer_claim, "failed", None, "ValueError", "newer-boom")

    # The newer generation now outranks the zombie's row in the memo's order.
    memo = await memoized_substep_result(
        wf_conn, wsql, flow_id=flow_id, job_id=job_id, step_keys=[key], map_index=None
    )
    assert memo is not None and memo.status == "failed", memo

    with structlog.testing.capture_logs() as logs:
        assert await zombie.step(key, null_effect) is None
    assert calls == ["ran"], f"the claim leg re-executed a recorded null success: {calls}"
    assert [e for e in logs if e["event"] == _RETIRED_EVENT] == [], logs


async def test_a_retired_substep_failure_is_logged_with_the_error_it_cleared(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The re-open erases the failed call's outcome in place, so the recovery
    path logs what the ledger is about to forget: one structured
    ``workflow-substep-failure-retired`` warning naming the flow, the job, the
    scoped key, the attempt/epoch, and the class + message of the failure the
    re-call retired. Without it, an ordinary caught-and-retried failure is
    indistinguishable from a first-try success."""
    calls: list[str] = []

    async def flaky() -> str:
        if not calls:
            calls.append("fail")
            raise ValueError("first-boom-probe")
        calls.append("ok")
        return "recovered"

    async def body(ctx: StepContext) -> str:
        with contextlib.suppress(ValueError):
            await ctx.substep("x", flaky)
        return await ctx.substep("x", flaky)

    app = WorkflowApp()

    @app.workflow("substep_failure_retired_trace")
    def wf() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=3))

    runner = FlowRunner(app.get("substep_failure_retired_trace"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with structlog.testing.capture_logs() as logs:
        assert await runner.drive(flow_id, max_ticks=500) == "terminal"

    assert await runner.result(flow_id) == "recovered"
    assert calls == ["fail", "ok"], calls

    job_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'solo'",
        flow_id,
    )
    assert job_id is not None

    # The row itself carries the LATEST outcome: the failure is not retained
    # as a second row, which is the trade-off the log compensates for.
    row = await wf_conn.fetchrow(
        f"SELECT attempt, claim_epoch, status, result, error_class, error_message "
        f'FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 AND step_key = $2',
        flow_id,
        f"{job_id}/x",
    )
    assert row is not None
    assert (
        row["status"],
        row["result"],
        row["error_class"],
        row["error_message"],
    ) == ("succeeded", '"recovered"', None, None), dict(row)

    retired = [e for e in logs if e["event"] == _RETIRED_EVENT]
    assert len(retired) == 1, logs
    entry = retired[0]
    assert entry["flow_id"] == str(flow_id), entry
    assert entry["job_id"] == str(job_id), entry
    assert entry["step_key"] == f"{job_id}/x", entry
    assert (entry["attempt"], entry["claim_epoch"]) == (
        row["attempt"],
        row["claim_epoch"],
    ), entry
    assert entry["error_class"] == "ValueError", entry
    assert entry["error_message"] == "first-boom-probe", entry


async def test_a_newer_generations_failure_is_never_announced_as_retired_by_this_claim(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The memo answers the HIGHEST-RANKED terminal row, which need not be the
    row the claim re-opens.

    A stale caller's own ``failed`` row is outranked by a newer generation's
    ``failed`` row for the same key, so the memo carries the NEWER error while
    the claim re-opens this caller's own row. The retirement record must stay
    silent — announcing the newer generation's error as retired would name a
    failure this claim never cleared — and the stale caller's re-call still
    records its own new outcome on its own row.
    """
    wsql = render_workflow_sql(wf_schema)
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    key = f"{job_id}/x"

    stale = WorkflowSteps(wf_pool, wsql, flow_id=flow_id, job_id=job_id, attempt=1, claim_epoch=0)
    stale_claim = await claim_substep_ledger(
        wf_conn,
        wsql,
        flow_id=flow_id,
        job_id=job_id,
        step_key=key,
        map_index=None,
        attempt=1,
        claim_epoch=0,
    )
    await stale._record_terminal(key, stale_claim, "failed", None, "ValueError", "stale-boom")

    newer_claim = await claim_substep_ledger(
        wf_conn,
        wsql,
        flow_id=flow_id,
        job_id=job_id,
        step_key=key,
        map_index=None,
        attempt=2,
        claim_epoch=2,
    )
    newer = WorkflowSteps(wf_pool, wsql, flow_id=flow_id, job_id=job_id, attempt=2, claim_epoch=2)
    await newer._record_terminal(key, newer_claim, "failed", None, "KeyError", "newer-boom")

    memo = await memoized_substep_result(
        wf_conn, wsql, flow_id=flow_id, job_id=job_id, step_keys=[key], map_index=None
    )
    assert memo is not None and memo.error_message == "newer-boom", memo
    assert memo.ledger_id != stale_claim.ledger_id, (memo, stale_claim)

    async def recovered() -> str:
        return "ok"

    with structlog.testing.capture_logs() as logs:
        assert await stale.step(key, recovered) == "ok"
    assert [e for e in logs if e["event"] == _RETIRED_EVENT] == [], logs

    stale_row = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        stale_claim.ledger_id,
    )
    assert stale_row is not None
    assert (stale_row["status"], stale_row["result"]) == ("succeeded", '"ok"'), dict(stale_row)
    newer_row = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".wf_step_ledger '
        "WHERE id = $1",
        newer_claim.ledger_id,
    )
    assert newer_row is not None
    assert (
        newer_row["status"],
        newer_row["error_class"],
        newer_row["error_message"],
    ) == ("failed", "KeyError", "newer-boom"), dict(newer_row)


async def test_substep_claim_reopens_a_failed_row_beyond_the_smallint_map_boundary(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The substep-specific claim keeps the ledger's bigint map identity:
    a same-attempt recall at map index 32768 conflicts with and re-opens that
    child's own failed row rather than failing at the old smallint boundary."""
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    seeded = await _seed_ledger_row(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        job_id=job_id,
        status="failed",
        map_index=32768,
        error_class="ValueError",
    )

    claim = await claim_substep_ledger(
        wf_conn,
        render_workflow_sql(wf_schema),
        flow_id=flow_id,
        job_id=job_id,
        step_key=f"{job_id}/x",
        map_index=32768,
        attempt=1,
        claim_epoch=0,
    )

    assert claim.ledger_id == JobId(seeded["id"]), claim
    assert (claim.status, claim.map_index, claim.error_class) == (
        "running",
        32768,
        None,
    ), claim


async def test_reopened_substep_survives_a_provisional_phantom_fence(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A downstream failure can terminalize the owning node while its
    caught-failure re-call is still inside ``fn``. The reopened row is visible
    to the phantom reaper, whose provisional substep fence preserves the new
    claim instant; the completed re-call must still record success."""
    calls: list[str] = []
    recall_entered = asyncio.Event()
    release_recall = asyncio.Event()
    orphans: list[asyncio.Task[str]] = []

    async def flaky() -> str:
        if not calls:
            calls.append("fail")
            raise ValueError("first-err")
        calls.append("ok")
        recall_entered.set()
        await asyncio.wait_for(release_recall.wait(), timeout=10)
        return "recovered"

    async def body(ctx: StepContext) -> str:
        with contextlib.suppress(ValueError):
            await ctx.substep("x", flaky)
        recall = asyncio.create_task(ctx.substep("x", flaky))
        orphans.append(recall)
        await asyncio.wait_for(recall_entered.wait(), timeout=10)
        raise RuntimeError("downstream failure strands the in-flight re-call")

    app = WorkflowApp()

    @app.workflow("substep_recall_phantom_fence")
    def wf() -> Promise[object]:
        return build(step(body, key="solo", max_attempts=1))

    runner = FlowRunner(app.get("substep_recall_phantom_fence"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with structlog.testing.capture_logs() as logs:
        try:
            assert await runner.drive(flow_id, max_ticks=500) == "terminal"
            job_id = await wf_conn.fetchval(
                f'SELECT id FROM "{wf_schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'solo'",
                flow_id,
            )
            assert job_id is not None
            running = await wf_conn.fetchrow(
                f'SELECT id, status, error_class, updated_at FROM "{wf_schema}".wf_step_ledger '
                "WHERE flow_id = $1 AND step_key = $2",
                flow_id,
                f"{job_id}/x",
            )
            assert running is not None
            assert (running["status"], running["error_class"]) == ("running", None), dict(running)

            reaped = await wf_conn.fetch(runner.wsql.phantom_reap)
            assert running["id"] in {r["id"] for r in reaped}, reaped
            fenced = await wf_conn.fetchrow(
                f'SELECT status, updated_at FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
                running["id"],
            )
            assert fenced is not None
            assert fenced["status"] == "fenced", dict(fenced)
            assert fenced["updated_at"] == running["updated_at"], dict(fenced)

            release_recall.set()
            assert await asyncio.wait_for(orphans[0], timeout=10) == "recovered"
        finally:
            release_recall.set()
            await asyncio.gather(*orphans, return_exceptions=True)

    row = await wf_conn.fetchrow(
        f'SELECT status, result, error_class FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        running["id"],
    )
    assert row is not None
    assert (row["status"], row["result"], row["error_class"]) == (
        "succeeded",
        '"recovered"',
        None,
    ), dict(row)
    assert calls == ["fail", "ok"], calls
    assert [e for e in logs if e["event"] == _REFUSED_EVENT] == [], logs


async def test_an_abandoned_reopen_is_fenced_only_once_the_owning_node_dies(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The re-open's crash window, end to end.

    A re-call that re-opens its caller's ``failed`` row and then never writes a
    terminal leaves the row ``running`` with the earlier error erased — the
    state only the reap can close. The reap's population is keyed on the
    OWNING NODE's lease, so while that node is still a live claimed row the
    re-opened substep row is left alone (it may be genuinely in flight); once
    the node is terminal the row is fenced provisionally, keeping the re-open's
    claim instant so a still-live re-call could still write, and the memo never
    replays the abandoned re-call as an outcome.

    The fence invents no error for the row: the earlier call's diagnostic is
    genuinely gone, which is why the retirement log is the crash window's only
    record of the failure the re-open cleared.
    """
    wsql = render_workflow_sql(wf_schema)
    # A terminal flow root (G7 asserts nothing about a cancelled root) whose
    # node is STILL live — the orphaned-re-call shape.
    flow_id = await seed_flow(wf_conn, wf_schema, status="cancelled")
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="solo")
    steps = WorkflowSteps(wf_pool, wsql, flow_id=flow_id, job_id=node_id, attempt=1, claim_epoch=0)
    step_key = f"{node_id}/x"

    async def claim() -> LedgerClaim:
        return await claim_substep_ledger(
            wf_conn,
            wsql,
            flow_id=flow_id,
            job_id=node_id,
            step_key=step_key,
            map_index=None,
            attempt=1,
            claim_epoch=0,
        )

    async def memo() -> LedgerClaim | None:
        return await memoized_substep_result(
            wf_conn,
            wsql,
            flow_id=flow_id,
            job_id=node_id,
            step_keys=[step_key],
            map_index=None,
        )

    first = await claim()
    await steps._record_terminal("x", first, "failed", None, "ValueError", "first-err")
    failed = await _ledger_row(wf_conn, wf_schema, first.ledger_id)
    assert (failed["status"], failed["error_class"]) == ("failed", "ValueError"), failed

    # the re-call re-opens the row and then dies / is cancelled inside fn
    abandoned = await claim()
    assert abandoned.status == "running", abandoned
    assert abandoned.ledger_id == first.ledger_id, (first, abandoned)
    reopened = await _ledger_row(wf_conn, wf_schema, abandoned.ledger_id)
    assert (
        reopened["status"],
        reopened["result"],
        reopened["error_class"],
        reopened["error_message"],
    ) == ("running", None, None, None), reopened

    # the row left the memo's terminal population: the next invocation re-runs fn
    assert await memo() is None

    # the owning node's lease still holds: a live attempt is never fenced
    assert await reap_phantom_ledger(wf_pool, wsql) == 0
    still_running = await _ledger_row(wf_conn, wf_schema, abandoned.ledger_id)
    assert still_running["status"] == "running", still_running

    # the node dies: now the abandoned re-open is reapable, provisionally
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET status = $2, finished_at = clock_timestamp() WHERE id = $1',
        node_id,
        "succeeded",
    )
    assert await reap_phantom_ledger(wf_pool, wsql) == 1
    fenced = await _ledger_row(wf_conn, wf_schema, abandoned.ledger_id)
    assert fenced["status"] == "fenced", fenced
    assert fenced["updated_at"] == reopened["updated_at"], (fenced, reopened)
    # the fence closed the row WITHOUT inventing an error for it: the earlier
    # call's diagnostic is genuinely gone (the trade-off the re-open makes)
    assert fenced["error_class"] is None, fenced

    # 'fenced' is outside the memo's ('succeeded', 'failed') population, so the
    # fenced row is never replayed as an outcome: the next invocation runs fn
    # again rather than inheriting the abandoned re-call's silence.
    assert await memo() is None


async def test_a_duplicate_delivery_in_the_window_reads_no_memo_and_refuses_the_window_writer(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Two executors on one substep row inside one ``(attempt, claim_epoch)``.

    The re-opened row is invisible to the memo for as long as the re-call is
    inside ``fn``, so a duplicate delivery claims the SAME arbiter row (re-stamp
    only) and re-runs ``fn`` — the pre-existing multi-executor duplicate
    semantics. What the re-open changes is which write survives: the re-stamp is
    unconditional, so the earlier claimant's terminal is refused (as
    ``superseded``, not as a fence) and the newest claimant's lands, instead of
    neither landing.
    """
    wsql = render_workflow_sql(wf_schema)
    flow_id, job_id = JobId(new_uuid()), JobId(new_uuid())
    steps = WorkflowSteps(wf_pool, wsql, flow_id=flow_id, job_id=job_id, attempt=1, claim_epoch=0)
    step_key = f"{job_id}/x"

    async def claim() -> LedgerClaim:
        return await claim_substep_ledger(
            wf_conn,
            wsql,
            flow_id=flow_id,
            job_id=job_id,
            step_key=step_key,
            map_index=None,
            attempt=1,
            claim_epoch=0,
        )

    async def memo() -> LedgerClaim | None:
        return await memoized_substep_result(
            wf_conn, wsql, flow_id=flow_id, job_id=job_id, step_keys=[step_key], map_index=None
        )

    first = await claim()
    await steps._record_terminal("x", first, "failed", None, "ValueError", "first-err")

    # the re-call: memo says 'failed' (not succeeded), so it claims and fn runs
    read = await memo()
    assert read is not None and read.status == "failed", read
    recall = await claim()
    assert recall.status == "running", recall

    # inside the window a duplicate delivery (another executor) reads NO memo
    assert await memo() is None
    duplicate = await claim()
    assert duplicate.ledger_id == recall.ledger_id, (recall, duplicate)
    assert duplicate.status == "running", duplicate
    assert duplicate.claimed_at != recall.claimed_at, (recall, duplicate)

    with structlog.testing.capture_logs() as logs:
        await steps._record_terminal("x", recall, "succeeded", "recall", None, None)
    refused = [e for e in logs if e["event"] == _REFUSED_EVENT]
    assert len(refused) == 1, logs
    assert refused[0]["reason"] == "superseded", refused[0]

    await steps._record_terminal("x", duplicate, "succeeded", "duplicate", None, None)
    row = await _ledger_row(wf_conn, wf_schema, recall.ledger_id)
    assert (row["status"], row["result"], row["error_class"]) == (
        "succeeded",
        '"duplicate"',
        None,
    ), row
