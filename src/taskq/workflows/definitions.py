"""The registered-definition registry (T04, D1 — dispatch resolves bodies
from the REGISTERED definition).

DISPATCH RESOLVES BODIES FROM THE REGISTERED DEFINITION: per-call body maps
must not exist in the public API — two overlapping dispatches with
different body maps double-task a pending node and the claim-CAS loser
runs the WRONG body (the observed hazard; pin 16). The registry is the
ONLY body source: a body resolved here is the definition's body, whatever
the caller passed.

The registry is a module-level map keyed by the workflow's registered
name — the same shape the actor registry uses (one registry, the estate's
no-second-registry rule, F3).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Final

from taskq.backend._protocol import JobId
from taskq.workflows._types import ForkSpec

__all__ = [
    "FAILURE_POLICIES",
    "MAX_FAN_IN_PER_JOIN",
    "AggregateFn",
    "DuplicateStepBodyError",
    "DuplicateWorkflowError",
    "FanInBoundExceededError",
    "StepBody",
    "WorkflowDef",
    "WorkflowRegistry",
    "get_registry",
    "resolve_step_body",
    "validate_fork",
    "validate_join_spec",
]

#: A step body: the coroutine the dispatch runs for one step.
StepBody = Callable[[Any], Awaitable[Any]]

#: The map's DECLARED read-side aggregate fn (T21 decision c): a PURE fn
#: over the children's decoded result rows, evaluated AT READ TIME by the
#: aggregation surfaces — never a blocking fan-in (DH8's fence: the join
#: is for DATAFLOW, progress aggregation is OBSERVABILITY).
AggregateFn = Callable[[list[Any]], object]


class DuplicateWorkflowError(TypeError):
    """Two registrations of one workflow name — a coding error, refused."""


class DuplicateStepBodyError(TypeError):
    """Two registrations of one step key within one workflow — refused."""


@dataclass(frozen=True, slots=True)
class WorkflowDef:
    """The registered workflow definition (T09's API formalizes the authoring
    surface; the engine needs the body map + the placement fields)."""

    name: str
    bodies: dict[str, StepBody]
    actor: str = "workflow"
    queue: str = "default"
    max_attempts: int = 3
    retry_kind: str = "transient"
    capture_policy: str = "errors-only"
    redact: Callable[[str], str] | None = None
    #: The loop nodes' declared ``on_exhausted`` policies, keyed by loop
    #: key (attack-3 H1's cure): the SWEEP's arm reads the policy from
    #: the REGISTERED DEFINITION (D1) — a hand-crafted loop row carries
    #: no policy face, and the node row's metadata is the counter's
    #: cache, never the declaration's source.
    loop_policies: dict[str, str] = field(default_factory=dict[str, str])
    #: The maps' DECLARED read-side aggregates (T21 decision c), keyed by
    #: the SOURCE step key (the children's parent — the read surfaces
    #: resolve by the parent row's own step key), resolved DURABLY at
    #: read time (the flow root's stamped workflow name → THIS
    #: registry), the same doctrine the fired join's body uses.
    aggregates: dict[str, AggregateFn] = field(default_factory=lambda: dict[str, AggregateFn]())


class WorkflowRegistry:
    """The one registry: workflow name → definition. Dispatch resolves step
    bodies from HERE only."""

    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowDef] = {}

    def register(self, definition: WorkflowDef) -> WorkflowDef:
        if definition.name in self._workflows:
            existing = self._workflows[definition.name]
            # IDEMPOTENT RE-REGISTRATION (the compile's contract: same
            # module → same graph → the same body map): recompiling the
            # SAME definition is not a second registration — the registry
            # is keyed by name and the compile is deterministic. Only a
            # DIFFERING re-definition (a shadow) is the coding error.
            if (
                existing.bodies == definition.bodies
                and existing.aggregates == definition.aggregates
            ):
                return existing
            raise DuplicateWorkflowError(
                f"workflow {definition.name!r} is already registered; a second "
                "registration is a coding error, never a shadow"
            )
        seen: set[str] = set()
        for key in definition.bodies:
            if key in seen:
                raise DuplicateStepBodyError(
                    f"workflow {definition.name!r} registers step {key!r} twice"
                )
            seen.add(key)
        self._workflows[definition.name] = definition
        return definition

    def get(self, name: str) -> WorkflowDef:
        try:
            return self._workflows[name]
        except KeyError:
            raise KeyError(
                f"workflow {name!r} is not registered — dispatch resolves bodies "
                "from the registered definition only (D1)"
            ) from None

    def body(self, name: str, step_key: str) -> StepBody:
        """Resolve ONE step body from the registered definition (D1)."""
        definition = self.get(name)
        try:
            return definition.bodies[step_key]
        except KeyError:
            raise KeyError(
                f"workflow {name!r} has no body for step {step_key!r} — the "
                "definition is the only body source"
            ) from None

    def __contains__(self, name: str) -> bool:
        return name in self._workflows


_default_registry = WorkflowRegistry()


def get_registry() -> WorkflowRegistry:
    """The module-level default registry."""
    return _default_registry


def resolve_step_body(name: str, step_key: str) -> StepBody:
    """Resolve one step body from the DEFAULT registry (the dispatch path's
    entry — per-call body maps must not exist in the public API, D1)."""
    return _default_registry.body(name, step_key)


# ── The build-time graph validators (the declarative API's refusals) ────
# A malformed GRAPH is a coding error, refused at build time — never a
# runtime dragon the sweep has to diagnose after the fact (the sweep's
# orphan_parent stamp remains the runtime diagnosis for rows that reach
# the DB anyway; the validators are the door the API layer composes).


#: The declared failure policies (T06/T07) — the edge ledger's
#: ``failure_policy`` column's vocabulary; a fork/join declaring anything
#: else is refused at build time (the runtime never sees an unknown
#: policy — the propagation rule's split would silently take the default).
#: ``maybe`` is T07's optional edge: a maybe child's TERMINAL failure is
#: ABSORBED like collect's (fan-in + the join fires) but is SURFACED —
#: the fan-in item names the policy that absorbed it, so the result
#: envelope cannot lie about which policy ran.
FAILURE_POLICIES: Final[tuple[str, ...]] = ("fail_closed", "collect", "maybe")


class FanInBoundExceededError(ValueError):
    """The fork's fan-in past :data:`MAX_FAN_IN_PER_JOIN` (the rv4 cure —
    F8's typed bound): the exceeded bound is the NAMED refusal, the
    message carrying the ``child_driven`` escape so the remedy is
    reachable from the refusal's own text.

    A DETERMINISTIC authoring failure (:func:`taskq.workflows.api.
    _runner_ladder.is_deterministic_authoring_failure`'s fourth class):
    re-running cannot shrink the corpus — the retry ladder never burns;
    the node terminal-fails on the first attempt with this class on the
    record. (The pre-cure raise was the raw ``ValueError`` OUTSIDE the
    ladder's try — the reclaim re-crashed it forever, the remedy
    unreachable.)"""


def validate_fork(fork: ForkSpec) -> None:
    """Refuse a malformed fork at build time — the 'record healthy, work
    wrong' class, convicted before any row is written.

    THE EMPTY-JOIN PRECEDENT (the rv4 cure — F1): a fork over a
    legitimately-empty corpus STILL declares its join — the join is born
    ``deps_pending=0``, fires IMMEDIATELY, and packs the EMPTY list (the
    typed sum's honest value: ``result() == []``, the run terminal,
    SUCCESS truthful — a night with no documents chunked zero documents,
    and that is correct). The old blanket refusal ("a join over zero
    children waits on edges that never exist") convicted the LEGITIMATE
    shape — the empty join does not wait, it fires; the wedge it left
    behind was the route/map source over an empty corpus either sticking
    ``running`` forever with zero error rows or terminalizing with the
    lying ``result() is None``."""
    if not fork.children and fork.join is None:
        raise ValueError(
            "an empty fork (zero children) with no join is refused at "
            "build time: it carries no work and no collect — the "
            "stranded invisible join's shape"
        )
    # THE EMPTY-JOIN PRECEDENT (fall-through): a zero-children fork WITH
    # a join passes the refusal above — the join fires with the empty
    # list; the join's own declared policy and bound still validate
    # below.
    if fork.join is not None and fork.join.failure_policy not in FAILURE_POLICIES:
        raise ValueError(
            f"join {fork.join.step_key!r} declares unknown failure_policy "
            f"{fork.join.failure_policy!r} — one of {FAILURE_POLICIES}"
        )
    # T07'S FAN-IN BOUND (the fork's door): the fork's join over more
    # children than the bound is refused unless it declares the
    # child-driven shape explicitly (the choice is recorded on the joined
    # node's metadata — the docs state when child-driven engages).
    if (
        fork.join is not None
        and len(fork.children) > MAX_FAN_IN_PER_JOIN
        and not fork.join.child_driven
    ):
        raise FanInBoundExceededError(
            f"join {fork.join.step_key!r} fans in {len(fork.children)} "
            f"children — above the declared maximum fan-in per join "
            f"({MAX_FAN_IN_PER_JOIN}). Use JoinSpec(child_driven=True) "
            "(the child-driven shape: the fire counts terminal children "
            "from the edge ledger, never a per-joined-row edge list) or "
            "partition the map."
        )


#: The DECLARED MAXIMUM FAN-IN PER JOIN (T07's operational bound): the
#: join's re-derive cost scales with the edge-ledger row count per join,
#: and the measured curve's honest reading (P1's three points — 200 →
#: 14.9 ms, 1000 → 23.8 ms, 5000 → 39.3 ms — are NOT one line: the 1000
#: point is the outlier; the endpoint fit ~5.1 µs/edge + ~14 ms base) is
#: that the BASE term dominates — no fan-in meets a 5 ms-class budget at
#: the ~14-16 ms base, and the marginal edge cost is ~5 µs. The bound's
#: candidate therefore stands on the BASE-COST argument (the base is paid
#: once per sweep pass regardless of fan-in; the bound bounds the
#: PER-JOIN marginal work inside one pass), not on the false 5 ms-class
#: comparison. Past the bound the child-driven shape is the documented
#: escape (an explicit opt-in, recorded on the joined node).
MAX_FAN_IN_PER_JOIN: Final[int] = 1000


def validate_join_spec(step_key: str, parents: tuple[JobId, ...], deps_pending: int) -> None:
    """Refuse a JOIN NODE with zero incoming edges at build time (the
    declarative API's door — ``wf.validate()``'s shape): a join that
    declares deps_pending > 0 with no parents is a stranded invisible join
    — it can never fire, and (before the sweep's LEFT-JOIN hardening) it
    was not even diagnosable."""
    if deps_pending > 0 and not parents:
        raise ValueError(
            f"join node {step_key!r} declares deps_pending={deps_pending} with "
            "zero incoming edges: a join without parents can never fire — "
            "declare the parents (the edge writer rides the same call)"
        )
    if deps_pending > 0 and len(parents) != deps_pending:
        raise ValueError(
            f"join node {step_key!r} declares deps_pending={deps_pending} but "
            f"{len(parents)} parents — the counter's cache must equal the "
            "declared edge count"
        )
    # T07'S FAN-IN BOUND: past the operational bound the declared-edge
    # shape is refused — the error NAMES the bound and the child-driven
    # alternative (the documented escape for the genuinely-needed case:
    # the child-driven join counts its terminal children from the edge
    # ledger at fire time instead of trusting the per-joined-row cache).
    if len(parents) > MAX_FAN_IN_PER_JOIN:
        raise FanInBoundExceededError(
            f"join node {step_key!r} declares {len(parents)} parents — above "
            f"the declared maximum fan-in per join ({MAX_FAN_IN_PER_JOIN}). "
            "The declared-edge re-derive cost scales with the edge count per "
            "join; use the child-driven shape (JoinSpec(child_driven=True) — "
            "each child's finalize names its join target and the fire counts "
            "terminal children from the edge ledger), or partition the map."
        )
