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
  the wiring promises data the consumer cannot accept. An UNANNOTATED or
  duck-typed consumer param (``Any`` / a plain dict) consumes a model
  producer unseen — the totality claim's duck-shaped hole (attack-3 M4),
  convicted by the same rule.
* E6 fan-in bound — a join over more parents than
  ``MAX_FAN_IN_PER_JOIN`` is refused (the T07 bound; the error names the
  child-driven escape).
* E7 cross-graph promise — a promise wired from ANOTHER app's recorder
  (attack-3 M3's smuggle): under a colliding key it builds a silently
  WRONG edge to this app's own same-named node; the verbs record the
  smuggle, this rule convicts it.
* E8 carrier-type — the loop's declared ``carry=`` model vs the body's
  ``Refine[...]`` feedback model (T19's pin 5, enforced): unrelated
  carriers refuse at compile; undeclarable shapes are never convicted
  on a guess.
* W1 the-eternal-wait — a node whose gate declares no timeout: a workflow
  that waits forever on a human is a support ticket (the explicitness
  warning, T10's ``timeout=None`` rule).
* W2 unknown-queue — a node projected onto a queue this app cannot see
  (no workflow actor declares it and ``TASKQ_QUEUES`` does not name it):
  probably a typo — the job dispatches onto a queue no worker listens on
  (attack-3 M5's build-side face; the worker-boot fail-fast remains the
  runtime door). The warning class: the queue may be declared on ANOTHER
  app the same fleet serves (probably wrong, never a refusal).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union, cast

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
    diagnostics += _rule_cross_graph(compiled)
    diagnostics += _rule_unknown_queue(compiled)
    diagnostics += _rule_carrier_type(compiled)
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
    the stranded invisible join, convicted before any row is written.
    (Attack-3 M2's cure: the diagnostic sat AFTER the loop's ``continue``
    — dead code whose only pin tested the gather([]) verb, not the rule;
    the compiled graph is public, mutable data and the rule owns the
    shape injected into it.)"""
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
    come from it).

    THE PRESENCE CHECK IS RAW (the pattern miner's false-positive cure):
    the rule reads ``body.__annotations__`` — does a return annotation
    EXIST — never the RESOLVED hints. Under ``from __future__ import
    annotations`` an annotation is a STRING; a return type that is not
    module-level (a function-scope model the body's code never names)
    resolves to NOTHING (annotation strings create no closure cells) —
    body_hints returns {} and the RESOLVED view cannot distinguish
    "unannotated" from "unresolvable". A false conviction is worse than
    a missed lint (the zero-false-positive doctrine), so the type-
    dependent rules (E5/E8, the runner's codec) keep the resolved view —
    an unresolvable type SKIPS there — while E4, whose question is
    presence and not type, reads the raw signature."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.body is None:
            continue
        if "return" not in getattr(node.body, "__annotations__", {}):
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
                if not (isinstance(param, type) and issubclass(param, BaseModel)):
                    # THE DUCK-SHAPED HOLE (attack-3 M4's cure): the
                    # producer declares a MODEL; the consumer's param
                    # carries NO model annotation (``Any``, a plain
                    # dict, a duck) — the payload crosses UNVALIDATED
                    # and UNCHECKED (the runner's codec hook skips
                    # it too). The wiring's totality claim covers the
                    # consumer's declared params only: an undeclared
                    # one is the same promise-break, convicted here.
                    diagnostics.append(
                        WorkflowValidationError(
                            "E5-incompatible-consumer",
                            "error",
                            f"{node.key!r} consumes {parent_key!r}'s "
                            f"{produced.__name__} through a param the "
                            "compile cannot see a model on — annotate "
                            "the param with the payload's model (the "
                            "annotation IS the wiring; a duck-typed "
                            "param consumes any producer unseen)",
                        )
                    )
                    continue
                if produced is not param and not issubclass(produced, param):
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


def _rule_cross_graph(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E7: a promise wired from ANOTHER app's recorder (attack-3 M3's
    smuggle — recorded by the verbs at wiring time, convicted here before
    any row is written)."""
    return [
        WorkflowValidationError(
            "E7-cross-graph-promise",
            "error",
            f"node {consumer_key!r} consumes promise {parent_key!r} from a "
            "DIFFERENT workflow's recorder — a cross-graph smuggle: under a "
            "colliding key it builds a silently WRONG edge to this app's own "
            "same-named node; wire the promise from the workflow that owns it",
        )
        for consumer_key, parent_key in compiled.smuggled
    ]


def _rule_unknown_queue(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """W2: a node projected onto a queue this app cannot see (attack-3
    M5's build-side face — the worker-boot fail-fast stays the runtime
    door). The warning class: the queue may be served by another app the
    same fleet hosts (probably wrong, never a refusal)."""
    if compiled.known_queues is None:
        return []  # no declared queue universe — nothing to convict
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.queue in compiled.known_queues:
            continue
        diagnostics.append(
            WorkflowValidationError(
                "W2-unknown-queue",
                "warning",
                f"node {node.key!r} projects onto queue {node.queue!r} — no "
                "workflow actor on this app declares it and TASKQ_QUEUES does "
                "not name it: the job dispatches onto a queue no worker may "
                "listen on (declare the queue on an @app.actor, or add it to "
                "TASKQ_QUEUES); the worker-boot fail-fast remains the runtime "
                "door",
            )
        )
    return diagnostics


def _rule_carrier_type(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E8: the loop's CARRIER-TYPE declaration, ENFORCED (T19's pin 5 —
    the attack-audit's "recorded, never enforced" finding): the declared
    ``carry=`` value's type and the body's ``Refine[...]`` feedback type
    are both pydantic models and unrelated — the loop threads a carry
    the body cannot receive. Unenforceable when undeclarable (no
    ``carry=`` value, or an unresolvable body hint): the zero-false-
    positive doctrine — a guess is never convicted."""
    from types import UnionType
    from typing import get_args, get_origin

    from taskq.workflows.api._loop import Refine

    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        spec = node.loop_spec
        if spec is None or node.loop_body is None:
            continue
        carry = cast("object", spec.carry_type)  # pyright: ignore[reportUnknownVariableType, reportAttributeAccessIssue]  # Why: the LoopSpec's declared carry rides the object-typed loop_spec attachment on NodeDecl — the loop module's own declaration is the type's source.
        # The carry is DECLARED AS A VALUE (the initial carry — an
        # instance or a JSON-scalar default); the model it names is the
        # instance's type when the value is a model.
        if isinstance(carry, BaseModel):
            carry_model = type(carry)
        elif isinstance(carry, type) and issubclass(carry, BaseModel):
            carry_model = carry
        else:
            continue  # no declared model — nothing to enforce against
        hints = body_hints(node.loop_body)
        returned = hints.get("return")
        if returned is None:
            continue
        origin = get_origin(returned)
        members = list(get_args(returned)) if origin is Union or origin is UnionType else [returned]
        for member in members:
            if get_origin(member) is not Refine:
                continue
            (feedback,) = get_args(member)
            if (
                isinstance(feedback, type)
                and issubclass(feedback, BaseModel)
                and feedback is not carry_model
                and not issubclass(feedback, carry_model)
            ):
                diagnostics.append(
                    WorkflowValidationError(
                        "E8-carrier-type",
                        "error",
                        f"loop {node.key!r} declares carry "
                        f"{carry_model.__name__} but its body refines with "
                        f"{feedback.__name__} — unrelated carrier models: "
                        "the thread promises data the next iteration "
                        "cannot receive",
                    )
                )
    return diagnostics
