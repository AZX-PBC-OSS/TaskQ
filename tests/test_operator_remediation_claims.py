"""The operator's remedy for a fenced join: the runbook, the rules and the
CLI must agree.

``docs/guides/runbooks.md`` (``## TaskQWorkflowBlockedStuck``) states that an
``'orphan_parent'`` join, a ``'flow_dead'`` join and an abandoned
``'failed_parent'`` join *"have no shipped CLI verb that clears them"*, and
that ``taskq flows cancel`` on an already-terminal root is *"an idempotent
no-op"* that *"does NOT clear a retained pending fenced join"*.

The same runbook section's **stuck-run walk-through** sends the operator to
``taskq flows status <run_id>`` — *"every stuck node names its waiting-on
state + its remedy"* — which is the CLI rendering in
``taskq.workflows._cli.stuck_lines``. So the two surfaces answer one
question and have to give one answer.

These pins hold the surfaces together:

* **the rendered remedy**
  (``test_the_flow_dead_remedy_never_prescribes_a_documented_no_op``): the
  CLI's ``'flow_dead'`` remedy never prescribes a command the runbook
  documents as inert.
* **the behavioural claim**
  (``test_cancel_on_a_terminal_root_does_not_clear_a_fenced_join``): a
  cancel addressed at an already-terminal root neither terminalises the
  retained ``'flow_dead'`` join nor moves the ``state="blocked"`` series the
  alert reads. This is what the runbook says, established against real
  PostgreSQL rows.
* **the fenced classes**
  (``test_a_failed_parent_join_and_a_flow_dead_join_are_both_blocked`` and
  ``test_retrying_the_failed_parent_leaves_the_join_still_blocked``): both
  terminal stamps derive ``state="blocked"``, and a retry's re-stamp back to
  ``'join'`` stays blocked until the join actually fires.
* **the hold qualifier**
  (``test_the_blocked_stuck_prose_carries_the_hold_qualifier``): both rule
  files and the runbook say a fired ``'join'`` row is blocked only while it
  also carries the hold stamp.
* **the held fired join** (``test_a_held_fired_join_still_derives_blocked``):
  the gauge counts that row ``state="blocked"``.
"""

# ruff: noqa: S608  # Why: fixture-derived schema identifier; every value is $-bound.

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest
import yaml

from taskq.backend._protocol import JobId
from taskq.workflows import cancel_workflow_run, retry_workflow_node
from taskq.workflows._cli import FlowNodeRow, stuck_lines
from taskq.workflows._sql_status import (
    WORKFLOW_PROGRESS_SQL,  # pyright: ignore[reportPrivateImportUsage]  # Why: the test reads the statement the leader's gauge sampler renders.
)
from tests._wf_fixtures import seed_flow, seed_join

_REPO = Path(__file__).parent.parent

_RUN_ID = "3f7c1a2e-5b64-4c8d-9e01-2a3b4c5d6e7f"


def test_the_flow_dead_remedy_never_prescribes_a_documented_no_op() -> None:
    """``stuck_lines`` on a ``'flow_dead'`` node must not hand the operator a
    command the runbook proves cannot clear the row.

    The stamp's own construction decides both facts: ``flow_dead`` is written
    by the fence's ``fenced_waiters`` arm
    (``WORKFLOW_ROOT_MAINTAIN_SQL``, ``'flow_dead'`` when ``fp.parent_id IS
    NULL``) over the roots that pass terminalises — so the root is terminal,
    and ``cancel_workflow_run`` returns ``0`` on a terminal root without
    touching a node row (``_cancel_one_run``: ``if flipped is None: return
    0``). And the stamp is chosen only when NO parent of the join is
    ``failed`` — a failed parent is stamped ``'failed_parent'`` instead — so
    "retry the failed upstream node" names a row that does not exist.

    Both halves of the rendered remedy are therefore inert, and the operator
    who follows them gets ``no-op: run … is already terminal — nothing
    cancelled`` while the ``state="blocked"`` series keeps the alert firing.
    """
    node = FlowNodeRow(
        step_key="doc_ingest.join",
        status="pending",
        deps_pending=1,
        blocking_reason="flow_dead",
    )
    rendered = "\n".join(stuck_lines(node, _RUN_ID))
    assert rendered, "a retained flow_dead join is an operator-visible fault: it must render"

    violations: list[str] = []
    if "flows cancel" in rendered:
        violations.append(
            "prescribes `taskq flows cancel`, which the same runbook documents as an "
            "idempotent no-op on the already-terminal root a 'flow_dead' stamp implies "
            "(runbooks.md#taskqworkflowblockedstuck, 'How to remediate' step 4)"
        )
    if "retry the failed upstream node" in rendered:
        violations.append(
            "sends the operator after 'the failed upstream node', which a 'flow_dead' "
            "stamp says does not exist: the fence stamps 'failed_parent' when a parent "
            "is failed and 'flow_dead' only when none is"
        )
    assert not violations, (
        "THE FLOW_DEAD REMEDY LIES: the CLI's rendered remedy for a retained "
        "'flow_dead' join prescribes an action that cannot clear it —\n  "
        + "\n  ".join(violations)
        + f"\n\nThe rendered lines were:\n{rendered}\n\n"
        "A remedy line that names a verb which cannot do the job is worse than no "
        "remedy line: it ends the investigation, and the alert has no other remedy."
    )


async def _blocked_counts(conn: asyncpg.Connection, schema: str) -> dict[tuple[str, str], int]:
    rows = await conn.fetch(WORKFLOW_PROGRESS_SQL.format(schema=schema))
    return {(r["workflow"], r["state"]): r["count"] for r in rows}


@pytest.mark.integration
async def test_cancel_on_a_terminal_root_does_not_clear_a_fenced_join(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The runbook's claim, executed: cancel is an idempotent no-op on the
    terminal root a fenced join hangs beneath, and the blocked series the
    alert reads is unchanged by it."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded", workflow="debris_flow")
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="doc_ingest.join", deps=1)
    # the fence's stamp, verbatim: metadata || jsonb_build_object('blocking_reason', $2)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs '
        "SET metadata = metadata || jsonb_build_object('blocking_reason', $2::text) "
        "WHERE id = $1",
        join_id,
        "flow_dead",
    )

    before = await _blocked_counts(wf_conn, wf_schema)
    assert before.get(("debris_flow", "blocked")) == 1, (
        f"the seeded fenced join must render state=blocked before the cancel: {before}"
    )

    stopped = await cancel_workflow_run(
        wf_pool, schema=wf_schema, flow_id=flow_id, reason="operator follows the remedy"
    )
    assert stopped == 0, (
        f"cancel on an already-terminal root reported {stopped} cleared rows/held signals — "
        "the runbook's 'idempotent no-op' claim (How to remediate, step 4) says 0"
    )

    row = await wf_conn.fetchrow(
        f"SELECT status::text AS status, metadata->>'blocking_reason' AS reason "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        join_id,
    )
    assert row is not None
    assert row["status"] == "pending" and row["reason"] == "flow_dead", (
        "the cancel terminalised (or re-stamped) a retained fenced join — if that is the "
        f"shipped behaviour the runbook's no-op claim is wrong: {dict(row)!r}"
    )

    after = await _blocked_counts(wf_conn, wf_schema)
    assert after.get(("debris_flow", "blocked")) == 1, (
        "the blocked series moved across a cancel the runbook calls a no-op — "
        f"before={before} after={after}; the alert would read this as cleared"
    )


@pytest.mark.integration
async def test_a_failed_parent_join_and_a_flow_dead_join_are_both_blocked(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """Both fenced stamps derive ``state="blocked"`` on a retained ``pending``
    row — the runbook's "same alert-visible consequence" claim for
    ``'flow_dead'`` — so neither can be read as a resolved row by the gauge."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="failed", workflow="fenced_flow")
    failed_parent = await seed_join(wf_conn, wf_schema, flow_id, step_key="a.join", deps=0)
    flow_dead = await seed_join(wf_conn, wf_schema, flow_id, step_key="b.join", deps=0)
    for node_id, reason in ((failed_parent, "failed_parent"), (flow_dead, "flow_dead")):
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs '
            f"SET metadata = metadata || jsonb_build_object('blocking_reason', $2::text) "
            "WHERE id = $1",
            JobId(node_id),
            reason,
        )
    counts = await _blocked_counts(wf_conn, wf_schema)
    assert counts.get(("fenced_flow", "blocked")) == 2, (
        f"both fenced stamps must read state=blocked: {counts}"
    )


@pytest.mark.integration
async def test_retrying_the_failed_parent_leaves_the_join_still_blocked(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The retry's success signal is the join firing, not the retry returning.

    ``runbooks.md`` "How to remediate" step 3 says the retry re-stamps the
    ``'failed_parent'`` closure back to ``'join'`` (the run resumes) and that
    the blocked series clears **when the join fires**, not when the retry
    returns.

    Both mechanical halves hold (the root re-opens, the closure is
    re-stamped), and the re-stamped row is a genuine JOIN-WAIT row again —
    ``pending`` with its ``deps_pending`` counter still reserved — which the
    gauge's own ``blocked`` branch counts (``n.deps_pending > 0``). The
    series therefore stays at 1 across the retry; an operator who reads the
    alert as still firing after a successful retry is seeing the documented
    shape, not a failed remedy.
    """
    flow_id = await seed_flow(wf_conn, wf_schema, status="failed", workflow="retry_flow")
    parent_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="a", deps=0)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="a.join", deps=1)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "metadata = metadata - 'blocking_reason' WHERE id = $1",
        parent_id,
    )
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs '
        "SET metadata = metadata || jsonb_build_object('blocking_reason', $2::text) "
        "WHERE id = $1",
        join_id,
        "failed_parent",
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        join_id,
        parent_id,
        flow_id,
    )

    before = await _blocked_counts(wf_conn, wf_schema)
    assert before.get(("retry_flow", "blocked")) == 1, before

    reopened = await retry_workflow_node(
        wf_pool,
        schema=wf_schema,
        flow_id=flow_id,
        node_key="a",
        reason="operator follows the remedy",
    )
    assert reopened == 1, "the failed parent's CAS must re-arm the row"

    root_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root_status == "running", (
        "the retry must re-open the failed root (the runbook's own claim): " + repr(root_status)
    )
    restamped = await wf_conn.fetchval(
        f"SELECT metadata->>'blocking_reason' FROM \"{wf_schema}\".jobs WHERE id = $1", join_id
    )
    assert restamped == "join", "the retry must re-stamp the failed_parent closure back to 'join'"

    after = await _blocked_counts(wf_conn, wf_schema)
    assert after.get(("retry_flow", "blocked"), 0) == 1, (
        "THE RETRY'S HONEST SIGNAL: the re-stamped row is a join-wait row "
        "(pending + deps_pending > 0), which the gauge's own blocked branch "
        "counts, so the series must STILL read 1 after the retry "
        f"(before={before}, after={after}). The runbook must say the series "
        "clears when the join fires — not when the retry returns."
    )


def _rule_description(path: Path) -> str:
    doc = yaml.safe_load(path.read_text())
    groups = doc.get("spec", doc).get("groups", [])
    for group in groups:
        for rule in group.get("rules", []):
            if rule.get("alert") == "TaskQWorkflowBlockedStuck":
                return " ".join(str(rule["annotations"]["description"]).split())
    raise AssertionError(f"TaskQWorkflowBlockedStuck not found in {path}")


def _runbook_blocked_stuck_section() -> str:
    text = (_REPO / "docs" / "guides" / "runbooks.md").read_text()
    start = text.index("## TaskQWorkflowBlockedStuck")
    end = text.index("\n## ", start + 1)
    return text[start:end]


def test_the_blocked_stuck_prose_carries_the_hold_qualifier() -> None:
    """Both rule files and the runbook qualify the fired-join statement with
    the hold: a fired ``'join'`` (or ``'body_unavailable'``) row derives
    ``state="pending"`` only while the row is not ALSO held.

    The ``'join'`` marker is never stripped, so a joined step that later gates
    on a human carries ``blocking_reason='join'``, ``deps_pending = 0`` and
    ``metadata ? 'hold'`` in one row — which the gauge counts
    ``state="blocked"``. Without the qualifier the prose tells an operator
    auditing that count that the row cannot exist. One pin over all three
    surfaces keeps the wording honest wherever the operator reads it.
    """
    surfaces = {
        "contrib/prometheus/rules.yaml": _rule_description(
            _REPO / "src" / "taskq" / "contrib" / "prometheus" / "rules.yaml"
        ),
        "contrib/kubernetes/prometheus_rule.yaml": _rule_description(
            _REPO / "src" / "taskq" / "contrib" / "kubernetes" / "prometheus_rule.yaml"
        ),
        "docs/guides/runbooks.md": _runbook_blocked_stuck_section(),
    }
    missing = [
        surface
        for surface, text in surfaces.items()
        if "unless the same row is also held" not in " ".join(text.split())
        or "metadata ? 'hold'" not in " ".join(text.split())
    ]
    assert not missing, (
        "THE BLOCKED-STUCK HOLD QUALIFIER: these surfaces state the fired-join "
        f"rule without the hold qualifier: {missing}. A fired 'join' row with "
        'deps_pending = 0 is state="pending" UNLESS the same row is also held '
        "(metadata ? 'hold'), which the gauge counts state=\"blocked\"; without "
        "the qualifier an operator auditing that count is told it cannot exist."
    )


@pytest.mark.integration
async def test_a_held_fired_join_still_derives_blocked(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """A fired join that then holds is ``state="blocked"`` — the hold
    qualifier both rule files and the runbook state.

    The ``'join'`` marker is never stripped, so a joined step that then gates
    on a human carries ``blocking_reason='join'``, ``deps_pending = 0``
    **and** ``metadata ? 'hold'`` in the same row, and the gauge's ``blocked``
    branch reads the hold leg. The row is unclaimable, so ``blocked`` is the
    truthful class; the prose qualifies its fired-join statement with
    "unless the same row is also held".
    """
    flow_id = await seed_flow(wf_conn, wf_schema, status="running", workflow="gated_flow")
    # a FIRED join (deps 0, the retained 'join' marker) whose step then holds
    step_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="approve", deps=0)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs '
        "SET metadata = metadata || jsonb_build_object('hold', $2::text, "
        "'held_signal', 'approval') WHERE id = $1",
        step_id,
        "9d1f3a4b-6c25-4e7f-8a90-1b2c3d4e5f60",
    )
    reason = await wf_conn.fetchval(
        f"SELECT metadata->>'blocking_reason' FROM \"{wf_schema}\".jobs WHERE id = $1", step_id
    )
    deps = await wf_conn.fetchval(
        f'SELECT deps_pending FROM "{wf_schema}".jobs WHERE id = $1', step_id
    )
    assert (reason, deps) == ("join", 0), (
        f"the seeded row must be the fired-join shape the text describes: {(reason, deps)!r}"
    )

    counts = await _blocked_counts(wf_conn, wf_schema)
    assert counts.get(("gated_flow", "blocked")) == 1, (
        "a fired 'join' row that also carries the hold stamp must derive "
        f'state="blocked": {counts}. The prose says a fired join derives '
        '"pending" only while it is not also held; without that qualifier an '
        "operator auditing this count is told the row cannot exist."
    )
