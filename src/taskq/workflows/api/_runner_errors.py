"""The flow RUNNER's error types (the split of ``api/_runner``): the loud
refusals a caller can catch — the graph is not registered, or the flow
row is missing, or the machinery's own bound was exhausted, or a node's
key vanished from the registry (the structural redeploy). Caller bugs,
refused loudly; never a silent wedge."""

from __future__ import annotations

__all__ = [
    "DuplicateSubstepError",
    "VanishedNodeError",
    "WorkflowRootMissingError",
    "WorkflowRunCancelledError",
    "WorkflowRunError",
]


class WorkflowRunError(RuntimeError):
    """The run cannot proceed — the graph is not registered, or the flow
    row is missing (a caller bug, refused loudly).

    ``claim_verdict`` rides the packaged ``run()``'s replay face: when a
    terminal run's key is replayed, the prior run's re-raised error
    carries the claim's verdict (``"existing-terminal"``) — the handler
    reads the refusal as the replay, not a fresh failure. ``None`` (the
    default) on every raise that is not the replay face."""

    claim_verdict: str | None = None


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
    """
    a deploy renamed or removed a node while
    a run was in flight, and the run's pending row's step key now
    resolves NOWHERE in the new process — not in the compiled graph, not
    in the registered definition.

    The registered definition is the
    only body source. A key the registry does not carry has no body —
    no re-run, no retry, no later tick can ever resolve it: the failure
    is DETERMINISTIC BY CONSTRUCTION (the ladder's deterministic route
    — the retry budget is never spent on it).

    Cancel this run
    (``taskq flows cancel <run_id>``) and restart the work on the new
    deploy under a fresh run key. The run terminal-FAILS LOUDLY with
    this class on the record — the run's own rows tell the operator the
    truth — never an eternal zombie (a ``running`` root over a
    forever-pending node, parked budget-free, surfaced only in a
    detector's log)."""


class DuplicateSubstepError(WorkflowRunError):
    """One node execution issued ``ctx.substep(name, ...)`` while a call
    with the same name was still running in it (#743). Both calls would
    claim one ledger row, and the second claim's re-stamp drops the first
    call's record. Every retry re-runs the same body, so the refusal is
    DETERMINISTIC: uncaught, the node fails on its first attempt with this
    class on the record (a loop body exhausts the loop as a body failure,
    the class named in its message). Concurrent substeps need distinct
    names."""


class WorkflowRootMissingError(WorkflowRunError):
    """Durable run-root corruption: the flow's own ``__flow__`` root row
    is gone while one of its nodes needs it. Raised at the root-ordered
    seats' ENTRY — the failed-parent propagation's seat (before any
    cascade, fan-in, decrement, fire, or outbox write) and the capacity
    park's seat (before any intent adoption or park write) — so the
    transaction rolls back whole and the already-committed state stands
    untouched.

    This is control-plane data corruption, never an author/body-resolution
    error and never a capacity fault: it is not converted into a
    body-unresolvable snooze, it is not fed to the infra-fault classifier,
    and the direct lane propagates the exact class to the caller. The
    fleet-claimed executor passes it through unwrapped (its own bounded
    release arm owns it there). The remedy is the runbook's
    restore-or-quarantine recovery, not a worker knob."""

    def __init__(self, flow_id: object, parent_id: object, seat: str = "failed-parent") -> None:
        self.flow_id = flow_id
        self.parent_id = parent_id
        self.seat = seat
        super().__init__(
            f"run {flow_id}'s __flow__ root row is missing while its node "
            f"{parent_id} reached the {seat} seat (durable run-root corruption; "
            "the transaction rolled back without any write)"
        )
