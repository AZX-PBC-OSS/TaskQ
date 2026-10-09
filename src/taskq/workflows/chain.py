"""THE TYPED-OUTCOME CHAIN SURFACE (T20) — the router: conditional edges
over a step's OWN typed outcomes, total or refused.

THE SPIKE'S PICKED SHAPE (PROOF.md §4 — the ergonomics bar):

    CHAIN = Chain(
        name="application-enrichment",
        start="screen",
        steps={
            "screen": Step(body=screen_app, outcomes=ScreenOutcome,
                           route=Route({ScreenOutcome.CLEAN: "enrich",
                                        ScreenOutcome.FLAGGED: "manual_review"})),
            "enrich": Step(body=enrich_app, outcomes=EnrichOutcome,
                           route=Route({EnrichOutcome.OK: "score",
                                        EnrichOutcome.SPARSE: DONE})),
        },
    )

A chain step's body is an ordinary typed-outcome coroutine (its return IS
the route's decision); the conditional edge is a dict keyed by the body's
OWN outcome enum; the terminal is ``DONE``; the totality fence is
invisible until violated.

TOTALITY IS THE FENCE, at TWO doors (the asymmetry doctrine — refuse
loudly, never silently drop):

1. DECLARATION TIME: :class:`Chain` refuses a route that is not total
   over its step's outcome enum — the outcome a route drops would
   silently strand a record's chain (a coding error, convicted before
   any row exists; the same shape as ``validate_fork``'s refusals).
2. RUN TIME: :class:`RouterNotTotal` — an outcome with no arm (a body
   that returned a foreign value) fails the step LOUDLY with
   ``error_class='RouterNotTotal'``: the record names the defect; the
   record's chain visibly dies, never vanishes.

THE ROUTE RIDES THE CERTIFIED FORK: a chain step's finalize forks AT MOST
ONE child (the route's next step — no fan-in, no join); the record's
identity (``map_index``) and trace ride forward on the child, and the
chain instantiates per record through the certified fork-at-finalize
machinery. The run completes from the rows (the shipped root-finalize
derivation) — never a join.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any

from taskq.workflows._types import ChildSpec, EmitChild, ForkSpec

__all__ = ["DONE", "Chain", "Route", "RouterNotTotal", "Step", "chain_fork", "chain_start"]

#: The chain's terminal route: a body outcome that ends the record's
#: chain (the step's finalize carries NO fork — the chain ends here).
DONE = "__done__"


class RouterNotTotal(Exception):
    """An outcome arrived at a route with no arm for it. THE LOUD REFUSAL:
    the runner converts this to a terminal-FAILED finalize carrying
    ``error_class='RouterNotTotal'`` — the record names the defect; the
    record's chain visibly dies instead of silently dropping. (With a
    total route declared over the body's outcome enum this is
    unreachable unless the body lied about its type — exactly the defect
    the door exists to name.)"""


class Route:
    """The conditional edge map for ONE chain step: outcome → next step
    (a step key of the chain, or :data:`DONE`).

    The keys are the members of the step body's outcome enum (declared
    total — validated by :class:`Chain` at declaration); the runtime door
    (:meth:`next_step`) raises the loud refusal for an outcome with no
    arm, never a silent ``None``."""

    __slots__ = ("_routes",)

    def __init__(self, routes: dict[enum.Enum, str | None]) -> None:
        # The keys normalize through their enum value (a NON-enum key —
        # a raw string — is the declaration-time refusal's other face:
        # bind() names it 'unknown' below).
        normalized: dict[str, str | None] = {
            str(getattr(k, "value", k)): v for k, v in routes.items()
        }
        self._routes = normalized

    def bind(self, outcome_enum: type[enum.Enum], step_key: str) -> None:
        """The declaration-time totality check (the Chain's door): the
        route's keys must be EXACTLY the enum's members — a route that
        drops an outcome would silently strand the record's chain."""
        members = {m.value for m in outcome_enum}  # type: ignore[attr-defined]
        declared = set(self._routes)
        if declared != members:
            raise ValueError(
                f"chain step {step_key!r}: route is not total over "
                f"{outcome_enum.__name__} — missing "
                f"{sorted(members - declared)}, unknown {sorted(declared - members)}. "
                "A non-total route is refused at declaration: the outcome it "
                "drops would silently strand a record's chain."
            )

    def targets(self) -> set[str | None]:
        """The route's arms' VALUES (the Chain's declaration check reads
        them — never the private map)."""
        return set(self._routes.values())

    def next_step(self, outcome: str) -> str | None:
        """The runtime door — the route's arm for *outcome*, or ``None``
        for DONE. Raises the LOUD refusal (never a silent drop) when the
        outcome has no arm."""
        try:
            return self._routes[outcome]
        except KeyError:
            raise RouterNotTotal(
                f"outcome {outcome!r} has no route — the chain's route is "
                "total over the step's outcome enum; this is a defect, not a "
                "dead end"
            ) from None


@dataclass(frozen=True, slots=True)
class Step:
    """One chain step: the body, the body's typed outcome vocabulary, the
    route over it. The body signature: ``body(ctx, item) -> Outcome`` —
    the item is the record riding the row (the chain's steps share the
    record's identity through it); the route is ``None`` for a step the
    author declared terminal."""

    body: Any
    outcomes: type[enum.Enum]
    route: Route | None = None  # None = the chain's terminal step


@dataclass(frozen=True, slots=True)
class Chain:
    """The chain declared ONCE, instantiated per record (each emit inserts
    the start step's row; each step's finalize forks the route's next
    step). ``start`` names the entry step key."""

    name: str
    start: str
    steps: dict[str, Step]
    #: The chain steps' placement (every row of the chain — the fork's
    #: children and the emit's starts alike — lands here).
    actor: str = "wf"
    queue: str = "default"

    def __post_init__(self) -> None:
        if self.start not in self.steps:
            raise ValueError(f"chain {self.name!r}: start {self.start!r} is not a step")
        for key, step in self.steps.items():
            if step.route is not None:
                step.route.bind(step.outcomes, key)
                for nxt in step.route.targets():
                    if nxt is not None and nxt != DONE and nxt not in self.steps:
                        raise ValueError(
                            f"chain {self.name!r}: step {key!r} routes to "
                            f"{nxt!r}, which is not a step"
                        )

    def next_child(
        self,
        step_key: str,
        outcome: object,
        *,
        payload: dict[str, object] | None,
        map_index: int | None,
    ) -> ChildSpec | None:
        """The router's decision for ONE step's outcome — the fork's child
        spec (or ``None`` = the chain ends here). Raises
        :class:`RouterNotTotal` loudly when the outcome has no arm.

        THE PER-RECORD IDENTITY RIDES map_index (the refuted-claim
        discipline): the certified fork's idempotency key and the
        step-ledger's arbiter both discriminate siblings by it — the
        record's index rides EVERY row of its chain, and the child
        carries it forward. Without it, two records' children of one step
        collide onto one row (the spike's 198 UniqueViolations).

        THE TRACE IS THE FORK'S OWN DOOR (the accept-and-ignore hunt's
        cure): ``next_child`` once accepted ``trace_id=`` and consumed
        nothing — the trace actually propagates through
        :func:`chain_fork`'s ``trace_id=`` (the caller passes both legs
        the same row value). The decorative kwarg is GONE; the fork's
        door is the trace's only home."""
        step = self.steps[step_key]
        if step.route is None:
            return None
        outcome_str = outcome.value if isinstance(outcome, enum.Enum) else outcome
        if not isinstance(outcome_str, str):
            raise RouterNotTotal(
                f"step {step_key!r}'s body returned {type(outcome).__name__}"
                f"{outcome!r} — not the step's declared outcome enum; the "
                "router refuses loudly (the record's chain dies visibly)"
            )
        nxt = step.route.next_step(outcome_str)
        if nxt is DONE or nxt is None:
            return None
        return ChildSpec(
            step_key=nxt,
            actor=self.actor,
            queue=self.queue,
            payload=payload,
            map_index=map_index,
        )


def chain_start(
    chain: Chain,
    item: object,
    *,
    map_index: int,
    trace_id: str,
) -> EmitChild:
    """One record's chain START (the emit's child spec): the chain's entry
    step, the record riding the row's payload under the item key (every
    chain step's body receives it — the fork carries the payload
    verbatim), the record's identity stamped at emit (the refuted-claim
    discipline — map_index + trace_id are REQUIRED here)."""
    return EmitChild(
        step_key=chain.start,
        actor=chain.actor,
        queue=chain.queue,
        payload={"wf_item": item},
        trace_id=trace_id,
        map_index=map_index,
    )


def chain_fork(
    child: ChildSpec | None,
    *,
    trace_id: str | None,
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> ForkSpec | None:
    """The fork a chain step's finalize carries: AT MOST ONE child (no
    fan-in, no join — the route rides the certified fork-at-finalize
    machinery), the record's trace riding to it. ``None`` = the chain
    ends here (the finalize carries no fork)."""
    if child is None:
        return None
    return ForkSpec(
        children=(child,),
        join=None,
        trace_id=trace_id,
        max_attempts=max_attempts,
        retry_kind=retry_kind,
    )
