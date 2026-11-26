# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""Docs-contract pins for the #743 executor cutover in ``docs/guides/upgrading.md``.

The cutover paragraph is the only fence between old and new workflow
executors, so it must say two things a reader would otherwise have to ask
about: why the capability gate (``required_worker_schema`` / ``WF001``) is
not used for this change (any stamp makes the in-process ``FlowRunner``
refuse the run; ``tests/test_wf_capability_guard.py`` pins that), and that a
new executor can re-run a side effect in a mixed window too, when a later
attempt's scoped ``failed`` row hides an earlier attempt's bare ``succeeded``
row. The last pin anchors that second sentence to the memo read: if the read
ever ranks a bare success over a later scoped failure, the sentence is wrong
and must go.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from taskq._ids import new_uuid
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows.ledger import memoized_substep_result

_GUIDE = Path(__file__).resolve().parent.parent / "docs" / "guides" / "upgrading.md"


def _cutover_paragraph() -> str:
    text = " ".join(_GUIDE.read_text(encoding="utf-8").split())
    start = text.index("**The executor cutover (required).**")
    return text[start : text.index("**Rollback is not safe", start)]


def test_the_cutover_says_why_the_capability_gate_does_not_fence_it() -> None:
    para = _cutover_paragraph()
    assert "`required_worker_schema`" in para and "`WF001`" in para, (
        "the #743 cutover must name the capability gate it does not use, so the "
        "next reader does not have to ask why a mixed fleet is not refused"
    )
    assert "in-process `FlowRunner`" in para and "not declared per workflow" in para, (
        "the cutover must give the reason: any stamp refuses in-process execution, "
        "and ctx.substep use is not declared, so every run would need the stamp"
    )
    assert "The drain is the only fence." in para


def test_the_mixed_window_names_the_new_executors_rerun() -> None:
    para = _cutover_paragraph()
    assert "A new executor can re-run a side effect" in para, (
        "the mixed-window paragraph lists only the old executor's failures"
    )
    assert "scoped `failed` row" in para and "bare `succeeded` row" in para, (
        "the cutover must say which row hides which: a later attempt's scoped "
        "failure hides an earlier attempt's bare success"
    )


async def test_a_later_attempts_scoped_failure_hides_an_earlier_bare_success(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    async def body(ctx: StepContext) -> str:
        return "unused"

    app = WorkflowApp()

    @app.workflow("cutover_docs_shadow")
    def wf() -> Promise[object]:
        return build(step(body, key="solo"))

    runner = FlowRunner(app.get("cutover_docs_shadow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    job_id: Any = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'solo'",
        flow_id,
    )
    insert = (
        f'INSERT INTO "{wf_schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status, result) "
        "VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8::jsonb)"
    )
    # the old executor's call: attempt 1, bare name, landed succeeded
    await wf_conn.execute(insert, new_uuid(), flow_id, job_id, "x", 1, 1, "succeeded", '"done"')
    # the new executor's later claim of the same substep, scoped, failed
    await wf_conn.execute(insert, new_uuid(), flow_id, job_id, f"{job_id}/x", 2, 2, "failed", None)

    memo = await memoized_substep_result(
        wf_conn,
        runner.wsql,
        flow_id=flow_id,
        job_id=job_id,
        step_keys=[f"{job_id}/x", "x"],
        map_index=None,
    )

    assert memo is not None
    assert (memo.step_key, memo.status) == (f"{job_id}/x", "failed"), (
        "upgrading.md's cutover says a later scoped failure hides an earlier bare "
        f"success; the read answered {(memo.step_key, memo.status)!r}"
    )
