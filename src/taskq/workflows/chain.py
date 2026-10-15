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
import types
from dataclasses import dataclass
from typing import Any, get_args

from pydantic import BaseModel

from taskq.workflows._types import ChildSpec, EmitChild, ForkSpec

__all__ = [
    "DONE",
    "Chain",
    "Route",
    "RouterNotTotal",
    "Step",
    "chain_fork",
    "chain_start",
    "type_tag",
]

#: The chain's terminal route: a body outcome that ends the record's
#: chain (the step's finalize carries NO fork — the chain ends here).
DONE = "__done__"


def type_tag(cls: type) -> str:
    """The TYPE-TAGGED route's dispatch key for one payload class: the
    canonical ``module.qualname`` — the tag the router matches a body's
    returned ELEMENT against (``type(element)``), and the tag the
    declaration's totality check compares the route's keys to. PUBLIC
    since T27: the graph-level typed route (``api/_graph.route``) shares
    the tag — the dispatch key is ONE vocabulary at both levels (the
    chain's steps and the graph's arms route the same union the same
    way)."""
    return f"{cls.__module__}.{cls.__qualname__}"


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

    TWO dispatch vocabularies (the type-tagged route — the routing
    proof's cure — beside the original enum face):

    * THE ENUM FACE: the keys are the members of the step body's outcome
      enum — the LITERAL TAG (the member's ``.value`` string) is the
      dispatch key; the record rides the row's payload VERBATIM.
    * THE TYPE-TAGGED FACE: the keys are the member TYPES of the step
      body's payload UNION (``Step.outcomes=Summary | Unreadable``) —
      the body returns the union ELEMENT itself and the router
      dispatches on the element's runtime TYPE; on a type-tagged arm the
      ELEMENT IS THE RECORD (the arm's body declares its arm's type —
      the narrowed arms in the editor — and the typed boundary
      re-validates the round-trip, so a mis-routed element dies LOUDLY
      in the coercion).

    Both faces are declared total — validated by :class:`Chain` at
    declaration (over the enum's members, or the union's member types);
    the runtime door (:meth:`next_step`) raises the loud refusal for an
    outcome with no arm, never a silent ``None``."""

    __slots__ = ("_routes", "_type_tags")

    def __init__(self, routes: dict[enum.Enum | type, str | None]) -> None:
        # The keys normalize: an enum member through its enum value (the
        # literal tag); a payload CLASS through its type-tag. A NON-enum,
        # non-type key — a raw string — is the declaration-time refusal's
        # other face: bind() names it 'unknown' below.
        normalized: dict[str, str | None] = {}
        type_tags: set[str] = set()
        for k, v in routes.items():
            if isinstance(k, type):
                tag = type_tag(k)
                type_tags.add(tag)
            else:
                tag = str(getattr(k, "value", k))
            normalized[tag] = v
        self._routes = normalized
        self._type_tags = type_tags

    def bind(
        self, outcomes: type[enum.Enum] | types.UnionType | type[object], step_key: str
    ) -> None:
        """The declaration-time totality check (the Chain's door): the
        route's keys must be EXACTLY the outcome vocabulary's members —
        the enum's members on the enum face, the union's member TYPES on
        the type-tagged face. A route that drops an outcome would
        silently strand the record's chain."""
        if isinstance(outcomes, types.UnionType):
            arms = get_args(outcomes)
            members = {type_tag(m) for m in arms}
            vocab = " | ".join(m.__name__ for m in arms)
        elif issubclass(outcomes, enum.Enum):  # pyright: ignore[reportArgumentType]  # Why: the union arm above narrowed the UnionType away; what remains is a class object, enum or not.
            members = {str(m.value) for m in outcomes}
            vocab = outcomes.__name__
        else:
            members = {type_tag(outcomes)}
            vocab = outcomes.__name__
        declared = set(self._routes)
        if declared != members:
            raise ValueError(
                f"chain step {step_key!r}: route is not total over "
                f"{vocab} — missing "
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
                "total over the step's outcome vocabulary (the enum's members, "
                "or the payload union's member types); this is a defect, not a "
                "dead end"
            ) from None


@dataclass(frozen=True, slots=True)
class Step:
    """One chain step: the body, the body's typed outcome vocabulary, the
    route over it. The body signature: ``body(ctx, item) -> Outcome`` —
    the item is the record riding the row (the chain's steps share the
    record's identity through it); the route is ``None`` for a step the
    author declared terminal.

    The outcome vocabulary (``outcomes``) is EITHER the body's outcome
    ENUM (the literal-tag face: the body returns the enum member, the
    record rides verbatim) OR the body's payload UNION — the member
    TYPES key the type-tagged route (the body returns the union ELEMENT
    itself; on a type-tagged arm the element IS the record)."""

    body: Any
    outcomes: type[enum.Enum] | types.UnionType | type[object]
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

        THE TYPE-TAGGED FACE: a non-enum, non-str outcome is a union
        ELEMENT — its runtime type is the dispatch key, and on its arm
        the ELEMENT IS THE RECORD (the child's ``wf_item`` is the
        element, jsonb-encoded; the arm's body declares its arm's type
        and the typed boundary re-validates).

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
        if isinstance(outcome, enum.Enum):
            outcome_str = str(outcome.value)
        elif isinstance(outcome, str):
            outcome_str = outcome
        else:
            # THE TYPE-TAGGED FACE: the body returned the union ELEMENT
            # itself — the element's runtime TYPE is the dispatch key.
            outcome_str = type_tag(type(outcome))
        nxt = step.route.next_step(outcome_str)
        if nxt is DONE or nxt is None:
            return None
        if not isinstance(outcome, (enum.Enum, str)):
            # THE ELEMENT IS THE RECORD (the type-tagged arm's law): the
            # body's returned union element rides the fork as the
            # child's wf_item — the arm's body declares its arm's type
            # and the typed boundary re-validates the round-trip (a
            # mis-routed element dies LOUDLY in the coercion). The
            # enum face's verbatim-payload law is untouched.
            element: object = (
                outcome.model_dump(mode="json") if isinstance(outcome, BaseModel) else outcome
            )
            payload = {"wf_item": element}
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
