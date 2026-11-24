"""A workflow step's two retry budgets (#746): crashes and body failures
are counted by different arbiters, so they stack.

* A CRASH (the holder's process dies mid-body; nothing is written) is
  handed back by the leader's reclaim sweep only while the row's claim
  counter has room: ``_RECLAIM_HAS_BUDGET_SQL`` reads
  ``jobs.attempt < jobs.max_attempts``, and every claim advances
  ``jobs.attempt`` (crashed claims and failed claims alike).
* A BODY FAILURE is laddered by the workflow runner, which counts the
  step's ``failed`` ledger rows (``_FAILED_COUNT_SQL``) and retries while
  ``failed + 1 < max_attempts``. A crashed attempt's ledger row is never
  ``failed`` (the RESUME-NOT-RETRY rule's "the ladder burns for BODY
  failures only"), so crashes do not spend the ladder.

Counting crashes and body failures alone, the worst case is therefore
``max_attempts - 1`` crashes (each at a claim ordinal below the ceiling)
followed by ``max_attempts`` body failures: ``2 * max_attempts - 1``
executions, where the retries guide's plain-actor arithmetic ("two crashes
leave one attempt") predicts ``max_attempts``. The order matters: a crash
at or past the ceiling is terminal even when the ladder still has room. A
machinery fault after the body returned is outside both budgets (the last
pin below).

The crash is the in-process attack suite's established "worker died"
shape (``test_wf_attack_loop``: the body raises ``CancelledError`` and the
drive dies with it, the claim left unwritten), then the dead lease lapses
and the REAL reclaim sweep (``sweep_expired_locks``) runs, and a fresh
runner (the restarted process) drives on. These pins hold the shipped
behaviour; the guide's statement of it is pinned in
``tests/test_retries_crash_accounting_docs_contract.py``.
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: schema is the controlled fixture identifier; all values are parameter-bound.
import asyncio
import contextlib
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.backend._sweeps import sweep_expired_locks
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step

pytestmark = pytest.mark.integration

MAX_ATTEMPTS = 3


class Ingest(BaseModel):
    doc_id: str


@pytest.mark.parametrize(
    ("script", "terminal"),
    [
        # crashes first: max_attempts - 1 crashes, then the full ladder
        ("CCFFF", "failed"),
        # crashes only: the third crash's claim is at the ceiling
        ("CCC", "crashed"),
        # failures first: the crash at the ceiling is terminal even though
        # the ladder has one failure left
        ("FFC", "crashed"),
    ],
)
async def test_crashes_and_body_failures_are_separate_budgets(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    script: str,
    terminal: str,
) -> None:
    """Each character is one execution of the step body: ``C`` = the worker
    dies mid-body, ``F`` = the body raises. The step runs exactly
    ``len(script)`` times and lands ``terminal``."""
    calls: list[int] = []

    async def flaky(ctx: StepContext, params: Ingest) -> str:
        calls.append(ctx.attempt)
        n = len(calls)
        if n <= len(script) and script[n - 1] == "C":
            raise asyncio.CancelledError("the worker process died mid-body")
        raise RuntimeError(f"body failure on execution {n}")

    app = WorkflowApp()
    name = f"crash_budget_{script.lower()}"

    @app.workflow(name)
    def _wf() -> Promise[object]:
        return build(step(flaky, Ingest(doc_id="d1"), key="s", max_attempts=MAX_ATTEMPTS))

    compiled = app.get(name)
    flow_id = (await FlowRunner(compiled, wf_pool, wf_schema).create_flow()).flow_id
    node_sql = (
        f'SELECT id, status::text AS status, attempt, max_attempts FROM "{wf_schema}".jobs '
        "WHERE step_key = 's' AND (metadata->>'flow_id')::uuid = $1"
    )

    lives = 0
    while lives <= len(script):
        lives += 1
        runner = FlowRunner(compiled, wf_pool, wf_schema)  # a fresh process per life
        drive = asyncio.create_task(runner.drive(flow_id, max_ticks=1500))
        died = False
        try:
            await drive
        except asyncio.CancelledError:
            died = True
        if not died:
            break
        node = await wf_conn.fetchrow(node_sql, flow_id)
        assert node is not None and node["status"] == "running", (
            f"the simulated crash must leave the claim unwritten: {dict(node or {})}"
        )
        # the dead holder's lease lapses, then the leader's real reclaim
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET lock_expires_at = now() - interval '1 second' "
            "WHERE id = $1",
            node["id"],
        )
        async with wf_pool.acquire() as conn:
            reclaimed = await sweep_expired_locks(
                conn,
                timedelta(0),
                timedelta(0),
                schema=wf_schema,
                max_retry_backoff=timedelta(milliseconds=10),
            )
        assert reclaimed == 1
        node = await wf_conn.fetchrow(node_sql, flow_id)
        assert node is not None
        if node["status"] == "crashed":
            break
        assert node["status"] == "pending", dict(node)

    # a crash-terminal node must never execute again: one more life, bounded
    if terminal == "crashed":
        with contextlib.suppress(asyncio.CancelledError):
            await FlowRunner(compiled, wf_pool, wf_schema).drive(flow_id, max_ticks=100)

    node = await wf_conn.fetchrow(node_sql, flow_id)
    assert node is not None
    attempts = await wf_conn.fetch(
        f'SELECT attempt, outcome::text AS outcome FROM "{wf_schema}".job_attempts '
        "WHERE job_id = $1 ORDER BY attempt",
        node["id"],
    )
    ledger = await wf_conn.fetch(
        f'SELECT attempt, status::text AS status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 's' ORDER BY attempt, claim_epoch",
        flow_id,
    )
    evidence: dict[str, Any] = {
        "node": dict(node),
        "executions": calls,
        "job_attempts": [tuple(r) for r in attempts],
        "ledger": [tuple(r) for r in ledger],
    }
    crashed_at = [i + 1 for i, c in enumerate(script) if c == "C"]
    failed_at = [i + 1 for i, c in enumerate(script) if c == "F"]

    assert len(calls) == len(script), (
        f"the body ran {len(calls)} times, not {len(script)}: {evidence}"
    )
    assert calls == list(range(1, len(script) + 1)), evidence
    assert (node["status"], node["attempt"], node["max_attempts"]) == (
        terminal,
        len(script),
        MAX_ATTEMPTS,
    ), evidence
    # the reclaim gate: every crashed claim is a 'crashed' job_attempts row
    assert [r["attempt"] for r in attempts if r["outcome"] == "crashed"] == crashed_at, evidence
    # the ladder's count: exactly the body failures are 'failed' ledger rows;
    # no crashed attempt's ledger row is one
    assert [r["attempt"] for r in ledger if r["status"] == "failed"] == failed_at, evidence


async def test_post_body_infra_fault_is_outside_both_budgets(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bound above counts crashes and body failures alone. A transport
    fault in the runner's own bookkeeping AFTER the body returned (here the
    success finalize) is reclaimed with ``repend_attempt(machinery_crashed=True)``:
    the ledger row says ``crashed`` (not ``failed``, so the ladder is not
    spent) and the re-pend has no ``attempt < max_attempts`` predicate, so an
    uncached body re-runs past the claim ceiling. With ``max_attempts=2``
    four such faults give five executions, where ``2 * max_attempts - 1`` is
    three; the guide names this path as outside the count."""
    faults = 4
    real_finalize = FlowRunner._finalize_success

    async def faulty_finalize(self: FlowRunner, *args: Any, **kwargs: Any) -> Any:
        nonlocal faults
        if faults > 0:
            faults -= 1
            raise ConnectionError("transport lost after the body returned")
        return await real_finalize(self, *args, **kwargs)

    monkeypatch.setattr(FlowRunner, "_finalize_success", faulty_finalize)
    calls: list[int] = []

    async def ok(ctx: StepContext, params: Ingest) -> str:
        calls.append(ctx.attempt)
        return "done"

    app = WorkflowApp()

    @app.workflow("post_body_infra_fault")
    def _wf() -> Promise[object]:
        return build(step(ok, Ingest(doc_id="d1"), key="s", max_attempts=2))

    compiled = app.get("post_body_infra_fault")
    flow_id = (await FlowRunner(compiled, wf_pool, wf_schema).create_flow()).flow_id
    node_sql = (
        f'SELECT status::text AS status, attempt, max_attempts FROM "{wf_schema}".jobs '
        "WHERE step_key = 's' AND (metadata->>'flow_id')::uuid = $1"
    )
    driver_errors: list[str] = []
    for _life in range(10):  # a fresh runner per re-drive, bounded
        try:
            await FlowRunner(compiled, wf_pool, wf_schema).drive(flow_id, max_ticks=300)
        except ConnectionError as exc:  # the reclaimed fault still escapes to the driver
            driver_errors.append(type(exc).__name__)
        node = await wf_conn.fetchrow(node_sql, flow_id)
        assert node is not None
        if node["status"] in ("succeeded", "failed", "crashed"):
            break
        await asyncio.sleep(0.1)

    node = await wf_conn.fetchrow(node_sql, flow_id)
    assert node is not None
    ledger = await wf_conn.fetch(
        f'SELECT attempt, status::text AS status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 's' ORDER BY attempt, claim_epoch",
        flow_id,
    )
    evidence: dict[str, Any] = {
        "node": dict(node),
        "executions": calls,
        "driver_errors": driver_errors,
        "ledger": [tuple(r) for r in ledger],
    }
    assert calls == [1, 2, 3, 4, 5], f"the body ran {len(calls)} times, not 5: {evidence}"
    assert faults == 0, evidence
    assert (node["status"], node["attempt"], node["max_attempts"]) == ("succeeded", 5, 2), evidence
    assert [tuple(r) for r in ledger] == [
        (1, "crashed"),
        (2, "crashed"),
        (3, "crashed"),
        (4, "crashed"),
        (5, "succeeded"),
    ], evidence
