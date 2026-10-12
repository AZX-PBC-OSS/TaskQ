"""The flow RUNNER's error types (the split of ``api/_runner``): the loud
refusals a caller can catch — the graph is not registered, or the flow
row is missing, or the machinery's own bound was exhausted, or a node's
key vanished from the registry (the structural redeploy). Caller bugs,
refused loudly; never a silent wedge."""

from __future__ import annotations

__all__ = ["VanishedNodeError", "WorkflowRunError"]


class WorkflowRunError(RuntimeError):
    """The run cannot proceed — the graph is not registered, or the flow
    row is missing (a caller bug, refused loudly)."""


class VanishedNodeError(WorkflowRunError):
    """THE VANISHED-NODE REDEPLOY WEDGE (the structural redeploy —
    rv5-t6's convicted class): a deploy renamed or removed a node while
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
