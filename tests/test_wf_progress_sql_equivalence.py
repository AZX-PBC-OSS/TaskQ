"""The fleet wf-progress sampler's ``state`` classification, checked as an
EQUIVALENCE: the SQL against the reference derivation, over the full
cross-product of the row-shape inputs.

Why this file exists beside ``test_wf_progress_sampler_emits_derived_node_states``
(which hand-seeds ~18 representative shapes and asserts exact counts): the
sampler's ``CASE`` re-expresses ``taskq.workflows._status``'s derivation in SQL,
and a divergence between the two does not raise, does not fail a gate and does
not look wrong in a diff — it produces a silent wrong count on the series
``TaskQWorkflowBlockedStuck`` selects. That pin proves each branch is
*somewhere* reached; this one proves the classification is a faithful function
of its inputs, including combinations no branch-specific seed produces (a
``scheduled`` row carrying a blocked stamp, a join-wait row that is also held,
the terminal-crash class under an absorbing edge, ...).

The oracle is the derivation itself — ``derive_workflow_status`` applied to a
one-node tuple, which is its per-row projection — so the two implementations
are compared without a third hand-transcribed table.

The second test ties the documented six-class vocabulary
(``docs/guides/observability.md``) to the ``job_status`` enum: every member must
derive one of the six, which is what makes the ``ELSE n.status::text`` fallback
a deliberate loud signal rather than a silent seventh series.

The third ties the classifier's EXEMPT ``blocking_reason`` vocabulary to the
constants the writers stamp (``taskq.workflows._sql``): a rename there is the one
classifier drift that can page an operator for a healthy fired join, and until
now the two sites shared no compile-time or test-time coupling.
"""

from __future__ import annotations

import json
import uuid
from collections import Counter
from typing import get_args

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows._sql import (
    BLOCKING_REASON_BODY_UNAVAILABLE,
    BLOCKING_REASON_FAILED_PARENT,
    BLOCKING_REASON_FLOW_DEAD,
    BLOCKING_REASON_JOIN,
    BLOCKING_REASON_ORPHAN_PARENT,
)
from taskq.workflows._sql_status import (
    WORKFLOW_PROGRESS_SQL,  # pyright: ignore[reportPrivateImportUsage]  # Why: the test reads the statement the leader's loop renders.
)
from taskq.workflows._status import NodeView, WorkflowStatus, derive_workflow_status

#: The documented six derived node-state classes, read off the derivation's own
#: return type so this file cannot carry a third copy of the vocabulary. The
#: literal below pins that type to what ``docs/guides/observability.md``'s
#: ``taskq.wf_progress_nodes_total`` row and ``WORKFLOW_PROGRESS_SQL``'s comment
#: block both name — widening one without the others is a documentation defect.
_DERIVED_STATES = frozenset(get_args(WorkflowStatus))
_DOCUMENTED_STATES = frozenset({"running", "failed", "blocked", "complete", "cancelled", "pending"})

#: The two ``blocking_reason`` values a pending row does NOT read as blocked
#: (``NodeView.is_blocked_row`` and the sampler's ``NOT IN`` literal list agree on
#: exactly these).
_EXEMPT_REASONS = frozenset({"join", "body_unavailable"})

#: The statuses whose failure an edge ledger can absorb (the derivation folds the
#: terminal-crash class into the failure class). ``reconstruct_workflow_status``
#: admits absorption for these only, so it is never set on any other row.
_FAILURE_CLASS = frozenset({"failed", "crashed", "abandoned"})

_STATUSES = (
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
)
_REASONS: tuple[str | None, ...] = (
    None,
    "join",
    "body_unavailable",
    "orphan_parent",
    "failed_parent",
    "flow_dead",
)

_INSERT_JOB = """\
INSERT INTO "{schema}".jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     status, step_key, deps_pending, metadata,
     idempotency_scope, idempotency_key, fairness_key)
VALUES ($1, 'wf', 'default', '{{}}'::jsonb, 0, 3, 'transient',
        $2, $3, $4, $5::jsonb, 'wf_equiv', $6, $7)
"""

#: The absorption variants each shape is exercised under. ``None`` means no
#: edge leaves the node; a reason seeds a ``pending`` child fenced with that
#: terminal stamp (the fan-in can never be delivered); ``absorbing`` seeds a
#: real succeeded child. Every fenced reason the writers stamp is covered.
_ABSORPTION_VARIANTS: tuple[tuple[str, str | None], ...] = (
    ("none", None),
    ("absorbing", None),
    ("fenced_failed_parent", "failed_parent"),
    ("fenced_orphan_parent", "orphan_parent"),
    ("fenced_flow_dead", "flow_dead"),
)


@pytest.mark.integration
async def test_sampler_state_classification_matches_the_derivation(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    flow_id = new_uuid()
    workflow = f"equiv_{flow_id.hex[:8]}"

    jobs: list[tuple[uuid.UUID, str, str, int, str, str, str]] = []
    edges: list[tuple[uuid.UUID, uuid.UUID, uuid.UUID]] = []
    # (status, deps_pending, blocking_reason, held, cancel_phase, absorbed)
    shapes: list[tuple[str, int, str | None, bool, int, bool]] = []
    # Rows seeded as absorption scaffolding are counted by the sampler too (they
    # ride the same flow), so they enter the expectation.
    helper_states: list[str] = []

    def _add(node_id: uuid.UUID, status: str, step_key: str, meta: dict[str, object]) -> None:
        jobs.append(
            (
                node_id,
                status,
                step_key,
                int(meta.pop("_deps", 0)),
                json.dumps({"flow_id": str(flow_id), **meta}),
                f"wf_equiv:{node_id}",
                str(flow_id),
            )
        )

    # The run root: excluded from the count (step_key '__flow__'), and the
    # workflow dimension's source.
    _add(flow_id, "running", "__flow__", {"workflow": workflow})

    ordinal = 0
    cancel_phase_rows: list[uuid.UUID] = []
    for status in _STATUSES:
        for deps in (0, 1):
            for reason in _REASONS:
                for held in (False, True):
                    for cancel_phase in (0, 1):
                        for absorption, fence_reason in _ABSORPTION_VARIANTS:
                            ordinal += 1
                            node_id = new_uuid()
                            step = f"s{ordinal:04d}"
                            meta: dict[str, object] = {"_deps": deps}
                            if reason is not None:
                                meta["blocking_reason"] = reason
                            if held:
                                meta["hold"] = str(new_uuid())
                                meta["held_signal"] = "gate"
                            _add(node_id, status, step, dict(meta))
                            if cancel_phase:
                                cancel_phase_rows.append(node_id)
                            if absorption != "none":
                                # ``absorbed_exists`` reads edges FROM this node
                                # to a collect/maybe child. The child is a real
                                # row on the same flow, so the sampler counts it.
                                child_id = new_uuid()
                                if fence_reason is None:
                                    _add(child_id, "succeeded", f"{step}_child", {})
                                    helper_states.append("complete")
                                else:
                                    # The fence's record: a child waiting on a
                                    # terminal reason never delivers the fan-in.
                                    _add(
                                        child_id,
                                        "pending",
                                        f"{step}_child",
                                        {"blocking_reason": fence_reason},
                                    )
                                    helper_states.append("blocked")
                                edges.append((child_id, node_id, flow_id))
                            shapes.append(
                                (
                                    status,
                                    deps,
                                    reason,
                                    held,
                                    cancel_phase,
                                    absorption == "absorbing",
                                )
                            )

    await wf_conn.executemany(_INSERT_JOB.format(schema=wf_schema), jobs)
    await wf_conn.executemany(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '  # noqa: S608  # Why: the schema is the module fixture's validated identifier; every value is a bind parameter.
        "VALUES ($1, $2, $3, 'collect')",
        edges,
    )
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET cancel_phase = 1 WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: the schema is the module fixture's validated identifier; the id list is a bind parameter.
        cancel_phase_rows,
    )

    # ── the oracle: the reference derivation's own per-row projection ──
    expected: Counter[str] = Counter()
    for status, deps, reason, held, cancel_phase, absorbed in shapes:
        derived = derive_workflow_status(
            (
                NodeView(
                    status=status,
                    deps_pending=deps,
                    blocking_reason=reason,
                    held=held,
                    absorbed=absorbed and status in _FAILURE_CLASS,
                ),
            )
        )
        if cancel_phase and status in ("pending", "scheduled"):
            # The gauge is per-ROW: it projects the derivation's run-level
            # ``cancel_in_flight`` row (which outranks "blocked") onto the row's
            # own ``cancel_phase``, so a cancelling pending row reads ``running``
            # where the run-level table reads ``pending``/``blocked``. Asserted
            # here as the documented mapping, not folded into the oracle.
            assert derived in ("blocked", "pending"), derived
            derived = "running"
        expected[derived] += 1
    for helper_state in helper_states:
        expected[helper_state] += 1

    rows = await wf_conn.fetch(WORKFLOW_PROGRESS_SQL.format(schema=wf_schema))
    got: Counter[str] = Counter()
    for row in rows:
        if str(row["workflow"]) == workflow:
            got[str(row["state"])] += int(row["count"])

    assert set(got) <= _DERIVED_STATES, (
        f"the sampler emitted non-derived state labels: {sorted(set(got) - _DERIVED_STATES)}"
    )
    assert set(expected) <= _DERIVED_STATES, (
        f"the oracle produced a class outside the documented six: {sorted(set(expected))}"
    )
    diff = {
        state: {"sampler": got.get(state, 0), "derivation": expected.get(state, 0)}
        for state in sorted(set(got) | set(expected))
        if got.get(state, 0) != expected.get(state, 0)
    }
    assert not diff, (
        "the sampler's classification diverges from the reference derivation over "
        f"the {len(shapes)}-shape cross-product: {diff}"
    )
    assert sum(got.values()) == len(shapes) + len(helper_states), (
        "a seeded node row went uncounted — the sampler's WHERE dropped a shape "
        "the classification is supposed to cover"
    )


@pytest.mark.integration
async def test_every_job_status_member_derives_a_documented_class(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The derivation table is TOTAL over the ``job_status`` enum: every member
    derives one of the six documented classes.

    This is the gate behind ``WORKFLOW_PROGRESS_SQL``'s ``ELSE n.status::text``
    branch — the deliberate "a future enum member surfaces loudly" choice. The
    fallback is only loud while every CURRENT member is classified explicitly, so
    an additive migration that adds a member without a branch has to fail here
    (a new raw label would otherwise appear as a seventh ``state`` series on the
    gauge, outside the vocabulary the docs, the dashboards and the cardinality
    pin all name).
    """
    flow_id = new_uuid()
    workflow = f"enum_{flow_id.hex[:8]}"
    members = [
        str(r["status"])
        for r in await wf_conn.fetch(
            f'SELECT unnest(enum_range(NULL::"{wf_schema}".job_status))::text AS status'
        )
    ]
    assert members, "the job_status enum read returned nothing"
    assert _DERIVED_STATES == _DOCUMENTED_STATES, (
        "the derivation's return type has drifted from the six classes the gauge's "
        f"documentation names: {sorted(_DERIVED_STATES)} — update docs/guides/"
        "observability.md, WORKFLOW_PROGRESS_SQL's branch list and the alert text "
        "together, never one of the three"
    )

    rows: list[tuple[uuid.UUID, str, str, int, str, str, str]] = [
        (
            flow_id,
            "running",
            "__flow__",
            0,
            json.dumps({"flow_id": str(flow_id), "workflow": workflow}),
            f"wf_equiv:{flow_id}",
            str(flow_id),
        )
    ]
    for member in members:
        node_id = new_uuid()
        rows.append(
            (
                node_id,
                member,
                f"only_{member}",
                0,
                json.dumps({"flow_id": str(flow_id)}),
                f"wf_equiv:{node_id}",
                str(flow_id),
            )
        )
    await wf_conn.executemany(_INSERT_JOB.format(schema=wf_schema), rows)

    emitted = {
        str(r["state"]): int(r["count"])
        for r in await wf_conn.fetch(WORKFLOW_PROGRESS_SQL.format(schema=wf_schema))
        if str(r["workflow"]) == workflow
    }
    unclassified = sorted(set(emitted) - _DERIVED_STATES)
    assert not unclassified, (
        f"job_status members {sorted(members)} include one the sampler does not "
        f"classify — it falls through the CASE's fallback and emits the raw enum "
        f"value as a state label: {unclassified} (all emitted: {sorted(emitted)}). "
        "Add an explicit branch to WORKFLOW_PROGRESS_SQL (and the class to "
        "docs/guides/observability.md's six) rather than letting the gauge grow a "
        "series nobody queries"
    )
    assert sum(emitted.values()) == len(members), (
        f"every enum member must produce exactly one counted row: {emitted}"
    )


@pytest.mark.integration
async def test_pending_blocking_reason_vocabulary_agrees_with_the_classifier(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """Every ``metadata.blocking_reason`` value the writers stamp classifies the
    way ``NodeView.is_blocked_row`` says it should — driven from the CONSTANTS, not
    from literal copies.

    The coupling this closes: the classifier's exemption list is the string pair
    ``('join', 'body_unavailable')``, written twice (the sampler's SQL and
    ``is_blocked_row``'s tuple), while the values themselves live in
    ``taskq.workflows._sql``'s named constants and are stamped by the sweep, the
    runner and the engine. Nothing tied the two together, so renaming
    ``BLOCKING_REASON_BODY_UNAVAILABLE``'s value would leave both classifiers
    exempting the dead spelling and reading every freshly-stamped fired join as
    ``blocked`` — a false page, on a healthy run, with no gate in the way. Seeding
    from the constants makes exactly that rename fail here.
    """
    flow_id = new_uuid()
    workflow = f"reason_{flow_id.hex[:8]}"
    stampers: tuple[tuple[str, str], ...] = (
        (BLOCKING_REASON_JOIN, "pending"),
        (BLOCKING_REASON_BODY_UNAVAILABLE, "pending"),
        (BLOCKING_REASON_ORPHAN_PARENT, "blocked"),
        (BLOCKING_REASON_FAILED_PARENT, "blocked"),
        (BLOCKING_REASON_FLOW_DEAD, "blocked"),
    )
    assert {reason for reason, _ in stampers if reason in _EXEMPT_REASONS} == (_EXEMPT_REASONS), (
        "the two reasons the classifier exempts are no longer both stamped by a "
        f"named constant: {sorted(_EXEMPT_REASONS)}"
    )

    rows: list[tuple[uuid.UUID, str, str, int, str, str, str]] = [
        (
            flow_id,
            "running",
            "__flow__",
            0,
            json.dumps({"flow_id": str(flow_id), "workflow": workflow}),
            f"wf_equiv:{flow_id}",
            str(flow_id),
        )
    ]
    for reason, _ in stampers:
        node_id = new_uuid()
        rows.append(
            (
                node_id,
                "pending",
                f"stamped_{reason}",
                0,
                json.dumps({"flow_id": str(flow_id), "blocking_reason": reason}),
                f"wf_equiv:{node_id}",
                str(flow_id),
            )
        )
    await wf_conn.executemany(_INSERT_JOB.format(schema=wf_schema), rows)

    emitted = {
        str(r["state"]): int(r["count"])
        for r in await wf_conn.fetch(WORKFLOW_PROGRESS_SQL.format(schema=wf_schema))
        if str(r["workflow"]) == workflow
    }
    expected_blocked = sum(1 for _, want in stampers if want == "blocked")
    expected_pending = sum(1 for _, want in stampers if want == "pending")
    assert emitted == {"blocked": expected_blocked, "pending": expected_pending}, (
        "a stamped ``blocking_reason`` derived the wrong class — the classifier's "
        "exemption list and the writers' constants have drifted: "
        f"emitted {emitted}, expected {{'blocked': {expected_blocked}, "
        f"'pending': {expected_pending}}}. Exempt: {sorted(_EXEMPT_REASONS)}"
    )
    for reason, wanted in stampers:
        view = NodeView(status="pending", blocking_reason=reason)
        assert view.is_blocked_row is (wanted == "blocked"), (
            f"the reference derivation disagrees about {reason!r}: "
            f"is_blocked_row={view.is_blocked_row}, the gauge says {wanted!r}"
        )
