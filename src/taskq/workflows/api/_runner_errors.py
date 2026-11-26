"""The flow RUNNER's error types (the split of ``api/_runner``): the loud
refusals a caller can catch — the graph is not registered, or the flow
row is missing, or the machinery's own bound was exhausted, or a node's
key vanished from the registry (the structural redeploy). Caller bugs,
refused loudly; never a silent wedge."""

from __future__ import annotations

from typing import Final, Literal, get_args

__all__ = [
    "ClaimEpochMissingError",
    "DuplicateSubstepError",
    "RootMissingSeat",
    "VanishedNodeError",
    "WorkflowResultDetailUnavailableError",
    "WorkflowRootMissingError",
    "WorkflowRunCancelledError",
    "WorkflowRunError",
]

#: Every seat that raises :class:`WorkflowRootMissingError`. The set is
#: closed: a caller cannot invent a fourth disposition at runtime, because
#: the class validates the value against this literal and refuses anything
#: else rather than silently asserting a transactional rollback it cannot
#: know happened.
RootMissingSeat = Literal["failed-parent", "park", "execution-door"]

_ROOT_MISSING_SEATS: Final[frozenset[str]] = frozenset(get_args(RootMissingSeat))

#: The per-seat disposition sentence. ``failed-parent`` and ``park`` raise at
#: their transaction's entry and roll it back whole; ``execution-door`` runs
#: after dispatch already claimed the row (``attempt``/``claim_epoch`` spent),
#: so it must describe the worker's bounded release rather than a rollback.
_SEAT_DISPOSITION: Final[dict[str, str]] = {
    "failed-parent": "the transaction rolled back without any write",
    "park": "the transaction rolled back without any write",
    "execution-door": (
        "the row's claim was already spent and no body ran, so the worker's bounded release owns it"
    ),
}


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


class ClaimEpochMissingError(WorkflowRunError):
    """A wait site was handed ``claim_epoch`` 0 — the executing claim's
    identity was never supplied. Both claim doors grant at least 1, so 0
    names a wiring defect, not a transient condition: a re-run cannot
    supply the epoch the door never granted, and every stored marker would
    compare unequal to it, so the re-executed site would treat its
    already-delivered answer as spent.

    DETERMINISTIC BY CONSTRUCTION (the ladder's deterministic route — the
    retry budget is never spent on it)."""


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
    names. Fix the collision rather than retrying the node: the running
    call is not cancelled, so a retry can run its effect again while the
    first call is still inside ``fn``."""


class WorkflowResultDetailUnavailableError(WorkflowRunError):
    """A row's value cannot be READ from the durable state the bounded
    collect's ``detail_home`` contract names: the row is a bounded summary
    whose parent rows (the only home of the full collect) are missing,
    purged, or do not reconcile with the recorded count; or the row's own
    ``result`` is NULL because the result-TTL sweep claimed it or an
    operator retry re-pended it.

    THE FAIL-CLOSED FACE OF THE DETAIL RE-READ. The seats that enforce it
    are the four workflow row readers: :meth:`FlowRunner.result`,
    :meth:`FlowRunner._flow_state` (and ``_parent_results``, the body-arg
    read they share) and :meth:`ChildHandle._ok_result`. Each either
    rebuilds the whole declared value or raises THIS class — never a
    truncated prefix, never ``[]``, never a bare ``None`` where a stored
    value stood. A silent prefix is the defect family this class closes:
    the caller's declared ``list[...]`` decoded fine over 64 KiB of a
    106 KiB collect, and nobody could tell. A swept or re-pended row has
    the same shape — its result is SQL NULL, which is never a collected
    ``None`` (a body returning ``None`` is stored as the empty document).

    DETERMINISTIC BY CONSTRUCTION (registered in
    :func:`_deterministic_classes`): the detail rows are the record, and
    no re-run, retry, or later tick can restore rows a purge removed or a
    TTL claimed — the ladder's retry budget is never spent on it.

    ``expected``/``rebuilt`` are the item counts when the failure is a
    count mismatch (``None`` when the row itself is gone); ``reason``
    names the concrete missing face. ``node_id``/``step_key`` locate the
    row the operator must inspect. The reason and the counts lead the
    message and the row id trails it: the CLI's failed-node line clamps
    to ~100 characters, and the operator needs the diagnosis there more
    than the uuid."""

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
        counts = ""
        if expected is not None:
            counts = f" (expected {expected} items"
            if rebuilt is not None:
                counts += f", rebuilt {rebuilt}"
            counts += ")"
        super().__init__(
            f"node {step_key!r} cannot be read{counts}: {reason}. (row {node_id}) "
            "A bounded collect's row carries only the summary — the full "
            "value lives on the parent rows — and the read fails closed rather than "
            "handing back a truncated prefix"
        )


class _RouteForkMissingError(RuntimeError):  # pyright: ignore[reportUnusedClass]  # Why: consumed by the runner's finalize guard and the ladder's deterministic tuple in sibling modules; the underscore keeps it out of the caller-catchable surface.
    """The fail-closed success-finalize guard (engine-private).

    A typed-route source reached the success finalize without its
    prebuilt fork. The wiring refuses the two authoring shapes that could
    produce it (a route source declaring ``cache=True`` or ``skip=``), so
    this is unreachable on valid paths — it exists so that if one of those
    fences is ever wrong, the refusal lands on the deterministic terminal
    (the named class on the row, the retry ladder unburned) instead of
    escaping the machinery boundary as an unnamed ``running`` row."""


class WorkflowRootMissingError(WorkflowRunError):
    """Durable run-root corruption: the flow's own ``__flow__`` root row
    is gone while one of its nodes needs it. Raised at the root-ordered
    seats' ENTRY — the failed-parent propagation's seat (before any
    cascade, fan-in, decrement, fire, or outbox write), the capacity
    park's seat (before any intent adoption or park write), so the
    transaction rolls back whole and the already-committed state stands
    untouched — and at the worker EXECUTION DOOR's seat
    (``seat='execution-door'``), where the claimed row's root-ordered read
    finds no ``__flow__`` row at all. The execution-door seat is
    pre-execution rather than transactional: no workflow transaction has
    run, and the row was already claimed by dispatch (its
    ``attempt``/``claim_epoch`` are spent), so the message says what the
    worker's bounded release does.

    This is control-plane data corruption, never an author/body-resolution
    error and never a capacity fault: it is not converted into a
    body-unresolvable snooze, it is not fed to the infra-fault classifier,
    and the direct lane propagates the exact class to the caller. The
    fleet-claimed executor passes it through unwrapped (its own bounded
    release arm owns it there). The remedy is the runbook's
    restore-or-quarantine recovery, not a worker knob."""

    def __init__(
        self,
        flow_id: object,
        parent_id: object,
        seat: RootMissingSeat = "failed-parent",
    ) -> None:
        if seat not in _ROOT_MISSING_SEATS:
            raise ValueError(
                f"unknown WorkflowRootMissingError seat: {seat!r}; "
                f"expected one of {sorted(_ROOT_MISSING_SEATS)!r}"
            )
        self.flow_id = flow_id
        self.parent_id = parent_id
        self.seat = seat
        super().__init__(
            f"run {flow_id}'s __flow__ root row is missing while its node "
            f"{parent_id} reached the {seat} seat (durable run-root corruption; "
            f"{_SEAT_DISPOSITION[seat]})"
        )
