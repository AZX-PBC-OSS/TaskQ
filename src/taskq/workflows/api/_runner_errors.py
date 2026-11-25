"""The flow RUNNER's error types (the split of ``api/_runner``): the loud
refusals a caller can catch — the graph is not registered, or the flow
row is missing, or the machinery's own bound was exhausted, or a node's
key vanished from the registry (the structural redeploy). Caller bugs,
refused loudly; never a silent wedge."""

from __future__ import annotations

__all__ = [
    "VanishedNodeError",
    "WorkflowResultDetailUnavailableError",
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


class WorkflowResultDetailUnavailableError(WorkflowRunError):
    """A row's value cannot be REBUILT from the durable detail rows the
    bounded collect's ``detail_home`` contract names: the join row is a
    bounded summary, and its parent rows (the only home of the full
    collect) are missing, purged, or short of the recorded count.

    THE FAIL-CLOSED FACE OF THE DETAIL RE-READ. Every reader that
    decodes a workflow row either rebuilds the whole declared value or
    raises THIS class — never a truncated prefix and never ``[]``. A
    silent prefix is the defect family this class closes: the caller's
    declared ``list[...]`` decoded fine over 64 KiB of a 106 KiB collect,
    and nobody could tell.

    DETERMINISTIC BY CONSTRUCTION (registered in
    :func:`_deterministic_classes`): the detail rows are the record, and
    no re-run, retry, or later tick can restore rows a purge removed —
    the ladder's retry budget is never spent on it.

    ``expected``/``rebuilt`` are the item counts when the failure is a
    count mismatch (``None`` when the row itself is gone); ``reason``
    names the concrete missing face. ``node_id``/``step_key`` locate the
    row the operator must inspect."""

    def __init__(
        self,
        *,
        node_id: object,
        step_key: str,
        reason: str,
        expected: int | None = None,
        rebuilt: int | None = None,
    ) -> None:
        self.node_id = node_id
        self.step_key = step_key
        self.reason = reason
        self.expected = expected
        self.rebuilt = rebuilt
        counts = (
            ""
            if expected is None
            else f" (expected {expected} items, rebuilt {0 if rebuilt is None else rebuilt})"
        )
        super().__init__(
            f"the value of node {step_key!r} (row {node_id}) cannot be read{counts}: "
            f"{reason}. A bounded collect's row carries only the summary — the full "
            "value lives on the parent rows — and the read fails closed rather than "
            "handing back a truncated prefix"
        )


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
