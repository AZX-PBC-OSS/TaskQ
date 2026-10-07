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

__all__ = [
    "DuplicateStepBodyError",
    "DuplicateWorkflowError",
    "StepBody",
    "WorkflowDef",
    "WorkflowRegistry",
    "get_registry",
    "resolve_step_body",
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
