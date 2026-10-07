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
from dataclasses import dataclass
from typing import Any

from taskq.backend._protocol import JobId
from taskq.workflows._types import ForkSpec

__all__ = [
    "DuplicateStepBodyError",
    "DuplicateWorkflowError",
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


class WorkflowRegistry:
    """The one registry: workflow name → definition. Dispatch resolves step
    bodies from HERE only."""

    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowDef] = {}

    def register(self, definition: WorkflowDef) -> WorkflowDef:
        if definition.name in self._workflows:
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


def validate_fork(fork: ForkSpec) -> None:
    """Refuse a malformed fork at build time: an EMPTY fork (zero children)
    owes a join that can never fire, and a join declared over them counts
    edges that will never exist — both the 'record healthy, work wrong'
    class, convicted before any row is written."""
    if not fork.children:
        raise ValueError(
            "an empty fork (zero children) is refused at build time: a join "
            "declared over zero children waits on edges that never exist — "
            "the stranded invisible join"
        )
    if fork.join is not None and not fork.children:
        raise ValueError(  # pragma: no cover - unreachable above, stated for the reader
            "a join over zero children is refused at build time"
        )


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
