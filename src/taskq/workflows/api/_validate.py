"""``wf.validate()`` — the checker-independent validator (T09).

Checker-INDEPENDENT: pyright/ty are not present at runtime — the validator
re-proves the wiring's totality from the compiled graph itself, in pytest,
CI, and at worker boot. ~6 rules, each classified ERROR (the graph cannot
be right if this fires) or WARNING (probably wrong, never a refusal — the
zero-false-positive doctrine, §15.5: over-refusing valid graphs is the
compile's version of over-rejection).

THE TOTALITY REFUSALS (the dispatcher's list, each a named rule):

* E1 acyclicity — OWNED UNCONDITIONALLY here (the §5.5 checker claim is
  dead errata): a self-referential wiring reds HERE; the probe documenting
  pyright's silence stays in T01's corpus.
* E2 produced-never-consumed — a promise nobody consumes, nobody sunk, and
  nobody named terminal is the unconsumed residual (the Never-residual
  red): work that will never be acknowledged.
* E3 edge-less join / empty fork — a gather over zero upstreams (refused
  at the verb) or a join node whose parents never materialize.
* E4 unannotated step — ``@wf.actor`` REQUIRES a return annotation: it IS
  the wiring (the fan-in's decoded args and the promise's data type come
  from it).
* E5 incompatible consumer — the producer's declared data type and the
  consumer's declared param type are both pydantic models and unrelated:
  the wiring promises data the consumer cannot accept.
* E6 fan-in bound — a join over more parents than
  ``MAX_FAN_IN_PER_JOIN`` is refused (the T07 bound; the error names the
  child-driven escape).
* W1 the-eternal-wait — a node whose gate declares no timeout: a workflow
  that waits forever on a human is a support ticket (the explicitness
  warning, T10's ``timeout=None`` rule).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from taskq.workflows.api._hints import body_hints
from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN

if TYPE_CHECKING:
    from taskq.workflows.api._app import CompiledWorkflow

__all__ = ["WorkflowValidationError", "validate_compiled"]


class WorkflowValidationError(Exception):
    """The validation refusal (the tsc-style one-pass report's item — the
    report carries every rule's verdict, not the first failure alone).
    The diagnostics tuple is the report; raising THIS is the ERROR class's
    enforcement."""

    def __init__(self, rule: str, severity: str, message: str) -> None:
        super().__init__(f"[{severity}] {rule}: {message}")
        self.rule = rule
        self.severity = severity
        self.message = message


def validate_compiled(compiled: CompiledWorkflow) -> tuple[WorkflowValidationError, ...]:
    """Run EVERY rule, return the diagnostics (the one-pass report). The
    ERRORs also raise — a graph with an error must not run; the tuple is
    what the API's report format spells (rule → severity → message)."""
    diagnostics = _run_rules(compiled)
    errors = [d for d in diagnostics if d.severity == "error"]
    if errors:
        report = "; ".join(f"{d.rule}: {d.message}" for d in errors)
        raise WorkflowValidationError("validate", "error", f"{len(errors)} error(s) — {report}")
    return tuple(diagnostics)


def _run_rules(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    diagnostics: list[WorkflowValidationError] = []
    diagnostics += _rule_acyclicity(compiled)
    diagnostics += _rule_residuals(compiled)
    diagnostics += _rule_edgeless_join(compiled)
    diagnostics += _rule_annotations(compiled)
    diagnostics += _rule_consumer_compat(compiled)
    diagnostics += _rule_fan_in_bound(compiled)
    diagnostics += _rule_eternal_wait(compiled)
    return diagnostics


def _rule_acyclicity(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E1: a cycle in the wiring (the unconditional owner — a cycle makes
    every join wait forever; no sweep can heal a topological lie)."""
    white, gray, black = 0, 1, 2
    color = dict.fromkeys(compiled.nodes, white)

    def visit(key: str, path: tuple[str, ...]) -> WorkflowValidationError | None:
        color[key] = gray
        for parent in compiled.nodes[key].parents:
            if color.get(parent, white) == gray:
                cycle = (*path, key, parent)
                return WorkflowValidationError(
                    "E1-acyclicity",
                    "error",
                    "the wiring is cyclic: "
                    + " → ".join(cycle)
                    + " — a cycle makes every join in it wait forever",
                )
            if color.get(parent, white) == white and parent in compiled.nodes:
                found = visit(parent, (*path, key))
                if found is not None:
                    return found
        color[key] = black
        return None

    for key in compiled.nodes:
        if color[key] == white:
            found = visit(key, ())
            if found is not None:
                return [found]
    return []


def _rule_residuals(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E2: every promise is consumed by a downstream edge, sunk
    explicitly, or named the terminal — anything else is the unconsumed
    residual (work that will never be acknowledged)."""
    consumed = {parent for node in compiled.nodes.values() for parent in node.parents}
    diagnostics: list[WorkflowValidationError] = []
    for key in compiled.nodes:
        if key in consumed or key in compiled.sunk or key == compiled.terminal:
            continue
        # A map's JOIN node is consumed THROUGH its source's promise (the
        # flat shape — the map's promise IS the join).
        if key.endswith(".join"):
            continue
        diagnostics.append(
            WorkflowValidationError(
                "E2-produced-never-consumed",
                "error",
                f"node {key!r} produces a result nobody consumes — sink it "
                "explicitly (wf.sink(p)), consume it downstream, or name it "
                "the terminal (wf.build(p)); an unconsumed promise is work "
                "that will never be acknowledged",
            )
        )
    return diagnostics


def _rule_edgeless_join(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E3: a join-shaped node (the fan-in) whose parents never exist —
    the stranded invisible join, convicted before any row is written."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.parents or not (node.kind == "gather" or node.key.endswith(".join")):
            continue
            diagnostics.append(
                WorkflowValidationError(
                    "E3-edgeless-join",
                    "error",
                    f"node {node.key!r} is a join with zero incoming edges — "
                    "it can never fire (the stranded invisible join)",
                )
            )
    return diagnostics


def _rule_annotations(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E4: a step body without a return annotation — the annotation IS
    the wiring (the promise's data type and the fan-in's decoded args
    come from it)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.body is None:
            continue
        if "return" not in body_hints(node.body):
            diagnostics.append(
                WorkflowValidationError(
                    "E4-unannotated-step",
                    "error",
                    f"node {node.key!r}'s body ({getattr(node.body, '__name__', '<anon>')!r}) "
                    "has no return annotation — @wf.actor requires one: it IS "
                    "the wiring (the promise's data type comes from it)",
                )
            )
    return diagnostics


def _rule_consumer_compat(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E5: a producer's declared data type and a consumer's param type —
    both pydantic models, unrelated — refuse at compile (the checker-
    independent half of the wiring's typing story; the checker half is
    the typeprobe corpus)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.body is None:
            continue
        hints = body_hints(node.body)
        params = [v for k, v in hints.items() if k not in ("return", "ctx")]
        for parent_key in node.parents:
            parent = compiled.nodes.get(parent_key)
            if parent is None or parent.body is None:
                continue
            produced = body_hints(parent.body).get("return")
            if not (isinstance(produced, type) and issubclass(produced, BaseModel)):
                continue
            for param in params:
                if (
                    isinstance(param, type)
                    and issubclass(param, BaseModel)
                    and (produced is not param and not issubclass(produced, param))
                ):
                    diagnostics.append(
                        WorkflowValidationError(
                            "E5-incompatible-consumer",
                            "error",
                            f"{node.key!r} consumes {parent_key!r}'s "
                            f"{produced.__name__} as {param.__name__} — "
                            "unrelated payload models: the wiring promises "
                            "data the consumer cannot accept",
                        )
                    )
    return diagnostics


def _rule_fan_in_bound(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E6: the T07 fan-in bound (the error names the child-driven escape
    — the same vocabulary the engine's validators use)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if len(node.parents) > MAX_FAN_IN_PER_JOIN:
            diagnostics.append(
                WorkflowValidationError(
                    "E6-fan-in-bound",
                    "error",
                    f"join {node.key!r} fans in {len(node.parents)} parents — "
                    f"above the declared maximum fan-in per join "
                    f"({MAX_FAN_IN_PER_JOIN}); partition the map",
                )
            )
    return diagnostics


def _rule_eternal_wait(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """W1: a gate with no declared timeout — probably wrong, never a
    refusal (the warning class; the timer-policy matrix is T10's)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        for gate in node.gates:
            if gate.timeout_s is None:
                diagnostics.append(
                    WorkflowValidationError(
                        "W1-eternal-wait",
                        "warning",
                        f"node {node.key!r} holds on gate {gate.name!r} with no "
                        "timeout — a workflow that waits forever on a human is "
                        "a support ticket; declare the deadline explicitly",
                    )
                )
    return diagnostics
