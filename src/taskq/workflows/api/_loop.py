"""The loop machinery (T19): ``wf.loop`` — PROMOTED TO V1 by the
maintainer's ruling (*"you do not cut must haves"*).

THE SHAPE: ``wf.loop(name, body, carry, until, max_iterations, budget,
on_exhausted)``. Each iteration = FRESH jobs — the iteration-scoped step
keys ``(workflow, loop_key, iteration, step)`` keep the idempotency
ledger per-iteration (T05's contract unchanged; the keys are TEXT
business keys). The control union ``Done[DoneT] | Refine[FooT]`` is the
body's return: ``Done`` stops the loop (its payload is the loop's
result); ``Refine`` threads the carry.

THE CARRY IS FROZEN AT SPAWN and advanced EXACTLY ONCE per iteration —
in the ADVANCE STATEMENT, ONE atomic write shared with the CAP GUARD
(``iteration < max_iterations`` is the same statement's WHERE leg): a
carry advanced at hold/retry time is the optimistic-apply dragon (the
double-apply/lost-apply variants) — the advance runs in the owning
iteration's terminal tx, never earlier.

THE TWO WALLS ARE DIFFERENT (the spike's cut 4): the ITERATION CAP
bounds total spawns regardless of time; the BUDGET (``budget_s``) is
the TIME wall — and it is BLIND while the loop holds on a human
(``budget_paused`` — the budget sweep's arm carries ``AND NOT
budget_paused``; the CONSUME-BUDGET dragon — the consume variant that
killed a held loop and refused the operator's later approval — is the
convicted alternative, kept RED forever by the mutation drill).

EXHAUSTION IS NAMED, NEVER SILENT: the cap or the budget wall terminates
the loop into the ``iteration_cap_exhausted`` state (the metadata's
``iteration_state`` + the node's typed failure) — and the FLOW
TERMINALIZES in the SAME transaction (STRANDED-FLOW: a wedged
``running`` flow that ticks forever is the convicted variant).

THE SEMANTICS DECISION, STATED ONCE: **infra fault ≠ body failure.**
Connection-loss/reclaim-eligible faults route to RECLAIM (the ledger
records ``crashed``; the ladder does NOT burn — the vanilla lease
machinery re-claims from the ledger); the retry ladder burns for BODY
failures only (the ledger's ``failed`` rows are the ladder's count).

NAIVE-MEMO IS AUTHOR GUIDANCE, PINNED: the durable memo (one model
invocation per iteration, never one per resume) is the ONE-TX shape —
read + write inside one transaction (the estate's claim statements ARE
that shape; the loop's ledger claim is its loop face). The engine gives
the carry + the TX boundary; the memo shape is author work (the don't-
pay law — the guide carries the pattern + the negative example).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final, Literal

from taskq.workflows.api._graph import BodyFn, Promise, WorkflowBuildError, active_graph

__all__ = [
    "Done",
    "Refine",
    "UntilPredicate",
    "default_escalation_body",
    "escalation_bindings",
    "loop",
    "registered_loop_policy",
]

#: The on-exhausted vocabulary (the named states, never a silent stop).
ExhaustionPolicy = Literal["escalate", "fail"]

#: The ESCALATION STEP KEY (attack-3 H1's cure): the workflow's registered
#: escalation step — the outbox's ``consumer_step_key`` for EVERY
#: ``on_exhausted="escalate"`` loop. The step's BODY resolves from the
#: REGISTERED DEFINITION (D1 — ``_register_bodies`` puts it there at
#: compile; the runner's ``_resolve_body`` reads it back at claim), so the
#: drained consumer job is never the dead-letter ghost the hardcoded
#: ``loop_escalation`` actor was.
ESCALATION_STEP_KEY: Final[str] = "loop.escalation"


class Done[T]:
    """The typed EXIT: the body's ``Done(payload)`` stops the loop; the
    payload is the loop node's result."""

    __slots__ = ("payload",)

    def __init__(self, payload: T) -> None:
        self.payload = payload


class Refine[T]:
    """The typed CONTINUE: the body's ``Refine(feedback)`` threads the
    carry into the NEXT iteration (advanced once, in the terminal tx)."""

    __slots__ = ("feedback",)

    def __init__(self, feedback: T) -> None:
        self.feedback = feedback


#: The ``until=`` predicate's TYPE (the fanout proof's cut #6, hit live):
#: ``Callable[[], Awaitable[bool]]`` — AWAITED by the driver. A bare sync
#: closure returning a coroutine object is TRUTHY (the spike's cancel
#: test cancelled an IDLE flow — proving nothing); the predicate is
#: awaited, never trusted as a value.
UntilPredicate = Callable[[], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class LoopSpec:
    """The loop node's declaration (the compile-visible face — the
    validate warnings and the runner's driver read it)."""

    loop_key: str
    max_iterations: int | None
    budget_s: float | None
    on_exhausted: ExhaustionPolicy
    carry_type: object  # the declared carry type (the CARRIER-TYPE check's subject)
    #: The REGISTERED ESCALATION STEP's body (attack-3 H1's cure): the
    #: author's ``escalates_to=`` when declared; the framework default
    #: otherwise. Registered under :data:`ESCALATION_STEP_KEY` at compile
    #: (D1 — the registry is the only body source), so the outbox's
    #: consumer job ALWAYS resolves a body — the escalation is never a
    #: dead letter.
    escalation_body: BodyFn | None = None


async def default_escalation_body(ctx: Any, escalation: dict[str, object]) -> dict[str, object]:
    """The DEFAULT escalation step's body (the operator-facing arm of
    ``on_exhausted="escalate"``): the loop's exhaustion context lands as
    the consumer job's OWN result — the job terminal-succeeds with the
    escalation record on it (the observable consequence: the outbox row
    drains, the consumer job runs, the record is readable from rows
    alone — no dead letter), and the structured WARNING names it for the
    live log. An author replaces it with ``loop(..., escalates_to=fn)``
    (page a human, open a ticket); the default makes the POLICY real
    without one."""
    from taskq.obs import get_logger

    get_logger(__name__).warning(
        "loop.escalation",
        loop=escalation.get("loop"),
        flow_id=escalation.get("flow_id"),
        error_class=escalation.get("error_class"),
        message=escalation.get("message"),
    )
    return escalation


def registered_loop_policy(workflow_name: str | None, loop_key: str) -> str:
    """The REGISTERED DEFINITION's ``on_exhausted`` policy for one loop
    (D1 — the sweep's arm reads the policy from HERE, never from the
    node row's metadata: a hand-crafted row has no policy face). A loop
    key the definition cannot resolve defaults to ``escalate`` only when
    the workflow registers an escalation body — the refusal of the
    dead-letter ghost is structural: no registered body, no enqueue."""
    if not workflow_name:
        return "fail"
    from taskq.workflows.definitions import get_registry

    try:
        definition = get_registry().get(workflow_name)
    except KeyError:
        return "fail"
    declared = definition.loop_policies.get(loop_key)
    if declared is not None:
        return declared
    return "escalate" if ESCALATION_STEP_KEY in definition.bodies else "fail"


def escalation_bindings(
    workflow_name: str | None,
    loop_key: str,
    *,
    flow_id: object,
    error_class: str | None,
    message: str | None,
) -> dict[str, object] | None:
    """The ESCALATION OUTBOX ROW's bindings (the driver's and the sweep's
    one enqueue shape): the consumer step key is
    :data:`ESCALATION_STEP_KEY`, the actor/queue resolve from the
    REGISTERED DEFINITION (D1 — the definition is the placement's
    source), and the payload carries the exhaustion context as the
    consumer body's data args. ``None`` = the workflow registers no
    escalation body — the enqueue is SKIPPED (the named state is still
    the record; a ghost row whose body never resolves is the convicted
    dead letter, never written)."""
    if not workflow_name:
        return None
    from taskq.workflows.definitions import get_registry

    try:
        definition = get_registry().get(workflow_name)
    except KeyError:
        return None
    if ESCALATION_STEP_KEY not in definition.bodies:
        return None
    info: dict[str, object] = {
        "loop": loop_key,
        "flow_id": str(flow_id),
        "error_class": error_class,
        "message": message,
    }
    return {
        "actor": definition.actor,
        "queue": definition.queue,
        "payload": {"wf_args": [info]},
    }


def loop(
    name: str,
    body: BodyFn,
    *,
    carry: object | None = None,
    until: UntilPredicate | None = None,
    max_iterations: int | None = None,
    budget_s: float | None = None,
    on_exhausted: ExhaustionPolicy = "escalate",
    escalates_to: BodyFn | None = None,
) -> Promise[Any]:
    """Wire a LOOP node: fresh jobs per iteration, the carry advanced
    exactly once per iteration (in the atomic advance+cap statement),
    the control union consumed with the residual machinery.

    *body* receives ``(ctx, carry)`` and returns ``Done(payload)`` or
    ``Refine(feedback)`` — a body whose union has an unconsumed member is
    the Never-residual red (the API's E-rules); a body returning anything
    else is the shape error at run time.

    ``until=`` is AWAITED per iteration (``Callable[[], Awaitable[bool]]``
    — a sync closure returning a coroutine object is the convicted
    dragon). ``max_iterations`` and ``budget_s`` are TWO DIFFERENT walls;
    unset ``until`` with both unset is the "waits forever" class — the
    validate warning.

    ``on_exhausted="escalate"`` (the default) ENQUEUES the escalation
    through the SAME outbox the fired joins use, addressed to THIS
    WORKFLOW'S REGISTERED ESCALATION STEP (attack-3 H1's cure): the
    step's body is *escalates_to* when declared, the framework's
    :func:`default_escalation_body` otherwise — registered under
    ``loop.escalation`` at compile (D1), so the drained consumer job
    always resolves a body (never the ``loop_escalation``-actor ghost).
    ``on_exhausted="fail"`` terminal-fails the flow and enqueues
    NOTHING — the policy is READ by the driver AND the sweep, both arms
    pinned end-to-end."""
    from taskq.workflows.api._graph import NodeDecl

    # THE NAMING RULE (cut #6's API-compile face — the same refusal
    # step() runs): the loop's derived keys (``<key>.iter<i>``) own the
    # dot.
    if "." in name:
        raise WorkflowBuildError(
            f"loop key {name!r} carries a dot — the dot is the engine's "
            "derived namespace (the loop's <key>.iter<i>); a wiring key "
            "may not collide with it"
        )
    graph = active_graph()
    spec = LoopSpec(
        loop_key=name,
        max_iterations=max_iterations,
        budget_s=budget_s,
        on_exhausted=on_exhausted,
        carry_type=carry,
        escalation_body=escalates_to,
    )
    node = NodeDecl(
        key=name,
        actor="wf",
        queue="default",
        body=None,  # the runner's loop driver is the engine-side body
        kind="loop",
        loop_spec=spec,
        loop_body=body,
        loop_until=until,
    )
    graph.add(node)
    return Promise(name, object, graph)
