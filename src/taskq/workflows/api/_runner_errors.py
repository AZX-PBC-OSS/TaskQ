"""The flow RUNNER's error types (the split of ``api/_runner``): the loud
refusals a caller can catch — the graph is not registered, or the flow
row is missing, or the machinery's own bound was exhausted, or a node's
key vanished from the registry (the structural redeploy). Caller bugs,
refused loudly; never a silent wedge."""

from __future__ import annotations

__all__ = ["VanishedNodeError", "WorkflowRunCancelledError", "WorkflowRunError"]


class WorkflowRunError(RuntimeError):
    """The run cannot proceed — the graph is not registered, or the flow
    row is missing (a caller bug, refused loudly)."""


class WorkflowRunCancelledError(WorkflowRunError):
    """A CANCELLED run's read raises this — the same family as the
    failed run's read (existing ``except WorkflowRunError`` clauses
    catch it). Carries the terminal ``status`` ("cancelled") and the
    cancel ``reason`` the cancel verb recorded on the root row — the
    cancelled run's first question (was it cancelled, and why) answered
    AT the read face, never the empty read that made cancelled and
    not-yet-terminal indistinguishable."""

    def __init__(self, flow_id: object, reason: str | None) -> None:
        self.status = "cancelled"
        self.reason = reason
        super().__init__(f"run {flow_id} was cancelled" + (f": {reason}" if reason else ""))


class VanishedNodeError(WorkflowRunError):
    """THE VANISHED-NODE REDEPLOY WEDGE (the structural redeploy):
    a deploy renamed or removed a node while
    a run was in flight, and the run's pending row's step key now
    resolves NOWHERE in the new process — not in the compiled graph, not
    in the registered definition.

    THE REGISTRY-IS-TRUTH LAW (D1): the registered definition is the
    ONLY body source. A key the registry does not carry has no body —
    no re-run, no retry, no later tick can ever resolve it: the failure
    is DETERMINISTIC BY CONSTRUCTION (the ladder's deterministic route
    — the retry budget is never spent on it).

    THE REMEDY IS CANCEL-THEN-RESTART: cancel this run
    (``taskq flows cancel <run_id>``) and restart the work on the new
    deploy under a fresh run key. The run terminal-FAILS LOUDLY with
    this class on the record — the run's own rows tell the operator the
    truth — never an eternal zombie (a ``running`` root over a
    forever-pending node, parked budget-free, surfaced only in a
    detector's log)."""
