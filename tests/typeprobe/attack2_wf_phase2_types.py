"""ATTACK2's negative type probes for the PHASE-2 public surfaces (T06/
T07/T08/T18). Every MUST_ERROR marker line must produce at least one
ERROR diagnostic from EACH checker (pyright 1.1.414 + ty 0.0.85) — the
same convention the T01 corpus (attack_wf_negative_types.py) pins; run
under tests/typeprobe's own pyrightconfig.json (reportArgumentType ON,
never the root's false).

The surfaces the probes guard:
  * FailureInfo (T06's fan-in item — the envelope door);
  * fan_in_skip (T06's public skip fan-in);
  * WorkflowStatus / derive_workflow_status / reconstruct_workflow_status
    (T08's derivation door);
  * NodeView (T08's canonical node vocabulary);
  * MAX_FAN_IN_PER_JOIN (T07's bound — an int Final, the probe defends
    its misuse as a string join target).

Run:  pyright --project tests/typeprobe/pyrightconfig.json tests/typeprobe/attack2_wf_phase2_types.py
      ty check tests/typeprobe/attack2_wf_phase2_types.py
"""

from __future__ import annotations

import asyncio

from taskq.backend._protocol import ConnLike, ErrorInfo, JobId
from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN
from taskq.workflows.engine import fan_in_skip
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import (
    NodeView,
    WorkflowStatus,
    derive_workflow_status,
    reconstruct_workflow_status,
)
from taskq.workflows._types import FailureInfo


def probe_failure_info_envelope() -> None:
    ok = FailureInfo(
        node_key="c",
        map_index=None,
        error=ErrorInfo(error_class="ValueError", error_message="boom"),
        attempts=((1, "ValueError", "boom"),),
        policy="maybe",
    )
    _ = ok
    lie = FailureInfo(
        node_key="c",
        map_index=None,
        error=ErrorInfo(error_class="ValueError", error_message="boom"),
        attempts=(),
        policy="fail_closed",  # MUST_ERROR (reportArgumentType: the L2 cure — the policy marker is the TYPED absorbing vocabulary (collect|maybe); a fail_closed edge absorbs nothing, the envelope cannot claim it)
    )
    _ = lie


def probe_failure_info_error_type() -> None:
    bad = FailureInfo(
        node_key="c",
        map_index=None,
        error="boom",  # MUST_ERROR (reportArgumentType: the envelope is ErrorInfo, never a bare str)
    )
    _ = bad


def probe_node_view_shapes() -> None:
    good = NodeView(status="pending", deps_pending=2, blocking_reason="join")
    _ = good
    bad_status = NodeView(
        status=42
    )  # MUST_ERROR (reportArgumentType: the jobs vocabulary is str, never int)
    _ = bad_status
    bad_deps = NodeView(
        status="pending", deps_pending="2"
    )  # MUST_ERROR (reportArgumentType: the counter is int)
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
    )  # MUST_ERROR (reportArgumentType: the derivation takes tuple[NodeView, ...], never raw dicts — a status-cache shape)
    _ = verdict


async def probe_fan_in_skip_signature() -> None:
    conn: ConnLike = None  # type: ignore[assignment] — the probe holds no real conn
    wsql = WorkflowSql("probe_schema")
    fanned = await fan_in_skip(
        conn,
        wsql,
        flow_id=JobId("00000000-0000-0000-0000-000000000000"),
        parent_id=JobId("00000000-0000-0000-0000-000000000000"),
        step_key="c",
        map_index=None,
    )
    _ = fanned
    wrong = fan_in_skip(
        conn, wsql, flow_id="not-a-jobid"
    )  # MUST_ERROR (reportArgumentType: flow_id is JobId + parent_id/step_key/map_index are required keyword-only — the call is missing 3)
    _ = wrong


def probe_bound_is_an_int() -> None:
    bound: int = MAX_FAN_IN_PER_JOIN
    _ = bound
    usage = "x" * MAX_FAN_IN_PER_JOIN  # type-correct (an int IS a repeat count)
    _ = usage
    orphan = MAX_FAN_IN_PER_JOIN.join(
        ""
    )  # MUST_ERROR (reportAttributeAccess: the bound is int, not str — a misuse the checker must name)
    _ = orphan


async def probe_reconstruct_conn_door() -> None:
    wsql = WorkflowSql("probe_schema")
    verdict = await reconstruct_workflow_status(
        "not-a-conn",  # MUST_ERROR (reportArgumentType: the read takes ConnLike, never a bare str)
        wsql,
        JobId("00000000-0000-0000-0000-000000000000"),
    )
    _ = verdict


def probe_asyncio_is_unused() -> None:  # pyright: ignore[reportUnusedFunction] — the corpus's shape helper
    _ = asyncio
