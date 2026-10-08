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
from typing import Any, Literal

from taskq.workflows.api._graph import BodyFn, Promise, active_graph

__all__ = [
    "Done",
    "Refine",
    "UntilPredicate",
    "loop",
]

#: The on-exhausted vocabulary (the named states, never a silent stop).
ExhaustionPolicy = Literal["escalate", "fail"]


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


def loop(
    name: str,
    body: BodyFn,
    *,
    carry: object | None = None,
    until: UntilPredicate | None = None,
    max_iterations: int | None = None,
    budget_s: float | None = None,
    on_exhausted: ExhaustionPolicy = "escalate",
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
    validate warning."""
    from taskq.workflows.api._graph import NodeDecl

    graph = active_graph()
    spec = LoopSpec(
        loop_key=name,
        max_iterations=max_iterations,
        budget_s=budget_s,
        on_exhausted=on_exhausted,
        carry_type=carry,
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
