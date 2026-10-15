"""ATTACK2's negative type probes for the PHASE-2 public surfaces (T06/
T07/T08/T18). Every ``MUST_ERROR(rules...)`` marker line must produce at
least one ERROR diagnostic from EACH checker (pyright 1.1.414 + ty
0.0.85) whose rule-id is in the marker's DECLARED set — the gate
(``_gate.py``, this corpus wired in) fails on a marker that reds under
the WRONG rule or on any error outside the markers. Runs under
tests/typeprobe's own pyrightconfig.json (reportArgumentType ON, never
the root's false).

The surfaces the probes guard:
  * FailureInfo (T06's fan-in item — the envelope door);
  * fan_in_skip (T06's public skip fan-in);
  * WorkflowStatus / derive_workflow_status / reconstruct_workflow_status
    (T08's derivation door);
  * NodeView (T08's canonical node vocabulary);
  * MAX_FAN_IN_PER_JOIN (T07's bound — an int Final, the probe defends
    its misuse as a string join target).

THE EVIDENCE-INTEGRITY ROUND'S REPAIR (wire-or-delete, no zombie
corpus): this file had ROTTED off the gate — the signatures moved
(``ErrorInfo`` grew the required ``error_traceback``, ``WorkflowSql``
became a fully-rendered bundle constructed by
:func:`taskq.workflows.engine.render_workflow_sql`, ``fan_in_skip``'s
remaining parameters went keyword-only-required, ``JobId`` is
``NewType`` over ``UUID``) and the markers carried the WRONG rule-ids
(pyright's vocabulary is ``reportAttributeAccessIssue``, not
``reportAttributeAccess``; ty's is ``invalid-argument-type``). Every
marker here is re-derived against the CURRENT signatures; the rule-id
declarations name BOTH checkers' vocabularies.

Run:  pyright --project tests/typeprobe/pyrightconfig.json tests/typeprobe/attack2_wf_phase2_types.py
      ty check tests/typeprobe/attack2_wf_phase2_types.py
"""

from __future__ import annotations

import uuid
from typing import cast

from taskq.backend._protocol import ConnLike, ErrorInfo, JobId
from taskq.workflows._status import (
    NodeView,
    WorkflowStatus,
    derive_workflow_status,
    reconstruct_workflow_status,
)
from taskq.workflows._types import FailureInfo
from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN
from taskq.workflows.engine import fan_in_skip, render_workflow_sql


def probe_failure_info_envelope() -> None:
    ok = FailureInfo(
        node_key="c",
        map_index=None,
        error=ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None),
        attempts=((1, "ValueError", "boom"),),
        policy="maybe",
    )
    _ = ok
    lie = FailureInfo(
        node_key="c",
        map_index=None,
        error=ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None),
        attempts=(),
        policy="fail_closed",  # MUST_ERROR(reportArgumentType, invalid-argument-type): the policy marker is the TYPED absorbing vocabulary (collect|maybe); a fail_closed edge absorbs nothing, the envelope cannot claim it
    )
    _ = lie


def probe_failure_info_error_type() -> None:
    bad = FailureInfo(
        node_key="c",
        map_index=None,
        error="boom",  # MUST_ERROR(reportArgumentType, invalid-argument-type): the envelope is ErrorInfo, never a bare str
    )
    _ = bad


def probe_node_view_shapes() -> None:
    good = NodeView(status="pending", deps_pending=2, blocking_reason="join")
    _ = good
    bad_status = NodeView(
        status=42
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the jobs vocabulary is str, never int
    _ = bad_status
    bad_deps = NodeView(
        status="pending", deps_pending="2"
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the counter is int
    _ = bad_deps


def probe_workflow_status_vocabulary() -> None:
    verdict: WorkflowStatus = derive_workflow_status((NodeView(status="succeeded"),))
    _ = verdict
    next_state: WorkflowStatus = derive_workflow_status((NodeView(status="failed"),))
    if next_state == "complete":
        pass
    number = len(
        next_state
    )  # NOT a type error — a WorkflowStatus IS a str union; recorded as a (mild) type-law note
    _ = number


def probe_derive_rejects_a_raw_dict() -> None:
    nodes = [{"status": "succeeded"}]
    verdict = derive_workflow_status(
        nodes
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the derivation takes tuple[NodeView, ...], never raw dicts — a status-cache shape
    _ = verdict


async def probe_fan_in_skip_signature() -> None:
    conn: ConnLike = cast("ConnLike", None)  # the probe holds no real conn
    wsql = render_workflow_sql("probe_schema")
    fanned = await fan_in_skip(
        conn,
        wsql,
        flow_id=JobId(uuid.UUID(int=0)),
        parent_id=JobId(uuid.UUID(int=0)),
        step_key="c",
        map_index=None,
    )
    _ = fanned
    fan_in_skip(
        conn, wsql, flow_id="not-a-jobid"
    )  # MUST_ERROR(reportArgumentType, reportCallIssue, invalid-argument-type, missing-argument): flow_id is JobId (a str literal is not a UUID) and parent_id/step_key/map_index are required keyword-only — the call is missing 3. A bare expression statement: the marked call's own diagnostics are the assertion (no assignment residue for the unknown-typed result to leak into).


def probe_bound_is_an_int() -> None:
    bound: int = MAX_FAN_IN_PER_JOIN
    _ = bound
    usage = "x" * MAX_FAN_IN_PER_JOIN  # type-correct (an int IS a repeat count)
    _ = usage
    MAX_FAN_IN_PER_JOIN.join(
        ""
    )  # MUST_ERROR(reportAttributeAccessIssue, unresolved-attribute): the bound is int, not str — a misuse the checker must name. A bare expression statement: the marked call's own diagnostics are the assertion.


async def probe_reconstruct_conn_door() -> None:
    wsql = render_workflow_sql("probe_schema")
    verdict = await reconstruct_workflow_status(
        "not-a-conn",  # MUST_ERROR(reportArgumentType, invalid-argument-type): the read takes ConnLike, never a bare str
        wsql,
        JobId(uuid.UUID(int=0)),
    )
    _ = verdict
