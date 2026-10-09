"""ATTACK PINS — THE RUN-KEY SEAM (the run-creation front, run-key half).

Provenance: the hostile review of the consolidated head (af1b8779) —
the migrations front's F-CREATE-2 / F-RUNKEY-4 / F-RUNKEY-5. The pins
assert the SAFE behavior; each carries its unmarked red run in the
pack's RECEIPTS.md.

F-CREATE-2 (LANDED): the retry interaction. ``create_flow(run_key=K)``
after an interrupted create hits the arbiter's conflict path
(``ledger.insert_flow_run`` → ``RunClaim(created=False)``) and
``FlowRunner.create_flow`` EARLY-RETURNS the existing run's id — the
static nodes are NEVER inserted. The key is squatted forever by a shell
that can never run: zero node rows, no error, the caller told "your
run". The SAFE law: a run-key conflict whose existing run has an
INCOMPLETE node census must COMPLETE the census (the replay heals the
interrupted create) or REFUSE with a named error — never return 'your
run' to a shell.

F-RUNKEY-4 (LANDED): a FAILED run squats its key. ``create_flow``
returns the bare ``JobId`` — ``RunClaim.status`` (which
``insert_flow_run`` read off the existing row) is DISCARDED at
``_runner.py``'s ``return claim.flow_id`` — nothing re-fires, and the
caller cannot distinguish 'already running' from 'already failed'
without a hand-written SQL query. The ledger layer honors the guide's
own contract ("a conflict returns the EXISTING run's id + status" —
docs/guides/workflows.md); the runner's public API drops it. The SAFE
law: the conflict's STATUS is learnable from the ``create_flow`` call
itself (a returned claim object, or a typed refusal carrying the
status).

F-RUNKEY-5 (doc lie): docs/guides/workflows.md instructs
``workflows.run(flow, input, key=…)`` — an API that does not exist
(grep-verified: ``taskq.workflows.__all__`` carries no ``run``;
``hasattr(taskq.workflows, 'run')`` is False). The pin is the
doc↔package consistency smoke: either the API exists or the guide does
not name it.

GUARD DISPOSITION (the doctrine's tail): the run-key replay pin
(``tests/test_workflows_ledger_pins.py::test_pin_4_run_key_replay_one_run``)
already holds one-run-row-forever GREEN at the ledger layer. The
terminal-failed extension does NOT ship as a green guard: the current
runner semantics is 'return the failed run's bare id' and the guide
promises 'id + status' — the doc does not bless the shipped API shape,
so the case folds into F-RUNKEY-4's xfail (above), per the doctrine's
"otherwise".
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, StepContext, WorkflowApp, build, step
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


class _RunkeyIn(BaseModel):
    doc_id: str


async def _runkey_body(ctx: StepContext, params: _RunkeyIn) -> Any:
    return {"doc_id": params.doc_id}


def _one_step_runner(wf_pool: asyncpg.Pool, wf_schema: str, name: str) -> FlowRunner:
    """A compiled one-step flow's runner — the census law's subject: a
    complete create inserts exactly ONE static node row."""
    app = WorkflowApp()

    @app.workflow(name)
    def _wf() -> object:
        return build(step(_runkey_body, _RunkeyIn(doc_id="d1"), key="only"))

    return FlowRunner(app.get(name), wf_pool, wf_schema)


async def _node_census(wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId) -> int:
    """The run's node census (every non-root row — the work the run can
    ever do)."""
    return int(
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs WHERE step_key <> '
            "'__flow__' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
    )


# ── F-CREATE-2: THE RETRY COMPLETES THE CENSUS (OR REFUSES NAMED) ──────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-CREATE-2 @ af1b8779: create_flow(run_key=K) "
    "after an interrupted create early-returns the existing run id "
    "(RunClaim.created=False) and NEVER inserts the static nodes — the "
    "key is squatted by a zero-node shell that can never run. The cure "
    "(the replay completes the census, or refuses with a named error) "
    "flips this to XPASS-strict.",
)
async def test_runkey_retry_after_interrupted_create_completes_the_census(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F-CREATE-2: kill the first create after the root insert
    (``_insert_static_nodes`` raises once), then RETRY with the same run
    key — the caller's recovery move. The SAFE law: the retry leaves a
    run whose node census is COMPLETE (one static node for the one-step
    flow), or the retry refuses with a named error. Today the retry
    returns the squatted id and the census stays 0 (the red in
    RECEIPTS.md)."""
    runner = _one_step_runner(wf_pool, wf_schema, "create2_retry_flow")
    key = "create2:retry"

    original = runner._insert_static_nodes

    async def _killed_once(*args: Any, **kwargs: Any) -> None:
        monkeypatch.setattr(runner, "_insert_static_nodes", original)
        raise RuntimeError("killed mid-create — the process died after the root insert")

    monkeypatch.setattr(runner, "_insert_static_nodes", _killed_once)
    with pytest.raises(RuntimeError, match="killed mid-create"):
        await runner.create_flow(run_key=key)

    # THE RETRY (the caller's recovery): the arbiter's conflict path —
    # created=False — must not strand the shell.
    conflict_error: Exception | None = None
    replay: Any = None
    try:
        replay = await runner.create_flow(run_key=key)
    except Exception as exc:  # the named-refusal cure shape
        conflict_error = exc

    flow_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs WHERE step_key = '
        "'__flow__' AND idempotency_key = $1",
        key,
    )
    assert flow_id is not None  # the interrupted create's root row exists
    if conflict_error is None:
        # The replay named a run: it must be the SAME row (the replay law,
        # pin 4's green at the ledger layer) with a COMPLETE census.
        assert str(getattr(replay, "flow_id", replay)) == str(flow_id)
        census = await _node_census(wf_conn, wf_schema, JobId(flow_id))
        assert census == 1, (
            f"the retry returned the squatted run with a {census}-node census "
            "(the one-step flow's complete census is 1) — 'your run' to a "
            "shell that can never run"
        )
    else:
        # The named-refusal cure: a WORKFLOW error naming the squatted,
        # census-incomplete run — never a bare id.
        from taskq.workflows import WorkflowRunError

        assert isinstance(conflict_error, WorkflowRunError), (
            f"the retry's refusal must be the named WorkflowRunError, got "
            f"{type(conflict_error).__name__}: {conflict_error}"
        )


# ── F-RUNKEY-4: THE CONFLICT'S STATUS SURFACES ON THE CALL ─────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RUNKEY-4 @ af1b8779: a FAILED run squats its "
    "key — create_flow returns the bare JobId, discarding RunClaim.status, "
    "so the caller cannot learn the conflict is 'already failed' (vs "
    "'already running') without a hand-written SQL query. The guide "
    "promises 'the EXISTING run's id + status'. The cure (the call "
    "surfaces the status — a returned claim object or a typed refusal "
    "carrying it) flips this to XPASS-strict.",
)
async def test_runkey_conflict_with_a_failed_run_surfaces_its_status(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """F-RUNKEY-4: create a run, terminal-fail it (the cascade's shape),
    replay the key. The CALLER must learn the squatting run's status from
    the call itself — the replay law (the same run id, one row forever)
    is pin 4's green and unchanged; this pin owns what the replay
    WITHHOLDS today: the status."""
    runner = _one_step_runner(wf_pool, wf_schema, "runkey4_failed_flow")
    key = "runkey4:failed"
    flow_id = await runner.create_flow(run_key=key)
    # The run terminal-fails (the cascade/maintenance leg's own stamp).
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET status = \'failed\', '
        "finished_at = clock_timestamp(), error_class = 'UnabsorbedNodeFailure' "
        "WHERE id = $1",
        flow_id,
    )

    conflict_status: Any = None
    try:
        replay = await runner.create_flow(run_key=key)
    except Exception as exc:  # the typed-refusal cure shape
        conflict_status = getattr(exc, "status", None) or getattr(exc, "run_status", None)
    else:
        conflict_status = getattr(replay, "status", None)

    assert conflict_status == "failed", (
        f"create_flow hid the conflict's status (surfaced "
        f"{conflict_status!r}): the squatting run is FAILED and the caller "
        "learns nothing from the call — the guide's 'id + status' contract "
        "stops at the ledger layer; the runner returns the bare JobId"
    )


# ── F-RUNKEY-5: THE GUIDE'S run() ENTRYPOINT RESOLVES (doc-lie smoke) ──


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RUNKEY-5 @ af1b8779: docs/guides/workflows.md "
    "instructs `workflows.run(flow, input, key=…)` — no such API exists "
    "(taskq.workflows.__all__ carries no 'run'; hasattr is False). The "
    "cure — ship the API or fix the guide — flips this to XPASS-strict.",
)
def test_the_guide_s_documented_run_entrypoint_resolves() -> None:
    """F-RUNKEY-5, the doc-reference smoke: the guide's named run-level
    idempotency surface resolves against the package, or the guide no
    longer names it. Sync by design — the derivation of the lie is
    import-and-read, no PG."""
    import taskq.workflows

    guide = Path(__file__).resolve().parents[1] / "docs" / "guides" / "workflows.md"
    names_the_api = "workflows.run(" in guide.read_text()
    assert hasattr(taskq.workflows, "run") or not names_the_api, (
        "the guide instructs `workflows.run(flow, input, key=…)` and the "
        "package has no `run` — the documented run-key door does not exist"
    )
