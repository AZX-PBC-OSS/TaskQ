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
* E9 ctx-annotation (F3-1) — the body's ``ctx`` annotation must BE
  StepContext (or a subclass), or the declared-unchecked Any/object: the
  fabricated stand-in is the build refusal (the annotation is
  verification, not documentation).
* E10 arity (F3-2) — the body's params (beyond ctx) must match the
  wired sources' count: the mismatch is a build refusal, never a
  mid-flow ladder discovery. ONE param beyond the wiring is the DEPS
  shape — E12's contract, not an arity mismatch.
* E12 deps-contract — the DI capability's rule: a body declaring the
  deps shape (one param beyond ctx + the wired sources) receives the
  app's bound deps instance; the app binding NONE (or an instance that
  does not SATISFY the body's declared deps type) is the build refusal,
  the message naming the fix.
* E13 gate-door — a node's ``gates=`` must carry ``GateDecl``
  declarations: the channel.gate(...) bound-door object wired into the
  declaration seat is the build refusal (it used to crash the
  validator's own gate walk — a raw AttributeError — the rv2 round's
  conviction).
* E14 gate-wiring (THE TEARDOWN ROUND'S E2-ANALOG) — the gate seat and
  the bodies' waits are walked against each other, BOTH provable
  directions: a DECLARED gate whose body carries NO ``wait_signal``
  reference at all (the hold seat with no waiter), and a body's
  ``wait_signal`` with NO declared gate (the hold the compile's
  surfaces cannot see). The conditional-interior wait is NOT statically
  provable — that face is the documented C9/W-rule (the loop-kind
  mis-index, W5).
* E15 route-totality (T27 — THE TOTALITY FENCE AT THE GRAPH LEVEL) — a
  typed route (``route(promise, arms)`` / ``map_source``'s dict form)
  is walked against its source's declared union, BOTH provable
  directions: the arms' keys must cover the union's members EXACTLY
  (the missing member would route NOTHING — the silent drop the route
  exists to refuse; the unknown member would never fire), and every
  arm body satisfies the typed-param contract (a duck-typed arm —
  ``dict``/unannotated/``Any`` — consumes the element UNVALIDATED; an
  unrelated model is the wiring promising data the arm cannot accept).
  The wiring verbs refuse the same breaches at the wiring site; this
  rule re-proves the fence from the compiled graph itself (public,
  mutable data — E3's precedent). The runtime door for the body that
  LIED about its union is :class:`taskq.workflows.RouterNotTotal`.
* E8 carrier-type — the loop's declared ``carry_type=`` model vs the body's
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
* W4 gate-timeout-split — the double-sourced timeout cross-checked
  where it is cross-checkable: a ``GateDecl.timeout_s`` and the body's
  statically-readable literal wait ``timeout_s=`` that DISAGREE are the
  drift, named (both values in the message; the precedence — the wait
  site arms the runtime, the declaration feeds the compile surfaces —
  is the declaration's own docstring).
* W5 loop-wait-shape — THE LOOP'S SHAPE LAW, NAMED (never prose): a
  LOOP body whose ``wait_signal`` sits in a CONDITIONAL interior
  mis-indexes the answer cursor (the iteration counter IS the cursor —
  an iteration that does not wait drifts the sequence; the T26 review's
  C9 is the same question). The unconditional wait is the shape kept.
"""

from __future__ import annotations

import ast
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Union, cast, get_args, get_origin

from pydantic import BaseModel

from taskq.workflows.api._graph import GateDecl, route_child_key
from taskq.workflows.api._hints import body_hints, inner_fn, own_source
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
    diagnostics += _rule_ctx_annotation(compiled)
    diagnostics += _rule_arity(compiled)
    diagnostics += _rule_deps_contract(compiled)
    diagnostics += _rule_route_totality(compiled)
    diagnostics += _rule_gate_door(compiled)
    diagnostics += _rule_gate_wiring(compiled)
    diagnostics += _rule_fan_in_bound(compiled)
    diagnostics += _rule_eternal_wait(compiled)
    diagnostics += _rule_gate_timeout_split(compiled)
    diagnostics += _rule_loop_wait_shape(compiled)
    diagnostics += _rule_cross_graph(compiled)
    diagnostics += _rule_unknown_queue(compiled)
    diagnostics += _rule_carrier_type(compiled)
    diagnostics += _rule_loop_promise_carry(compiled)
    diagnostics += _rule_eternal_loop(compiled)
    diagnostics += _rule_join_for_progress(compiled)
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


def _join_produced_shape(compiled: CompiledWorkflow, join_node: object) -> type[BaseModel] | None:
    """The BODYLESS join's produced ELEMENT shape (the gather's
    ``Promise[list[R]]``): the model EVERY one of its sources declares
    as its return — ``None`` when the sources disagree, produce
    non-models, or the join stands alone (no shape to enforce; the skip
    is honest). The union-of-models case returns ``None`` too: the
    element contract is ONE model or nothing (the runtime codec owns
    unions)."""
    shapes: set[type[BaseModel]] = set()
    parents: list[str] = getattr(join_node, "parents", []) or []
    for source_key in parents:
        source = compiled.nodes.get(source_key)
        if source is None:
            continue
        produced = body_hints(source.body).get("return") if source.body is not None else None
        if isinstance(produced, type) and issubclass(produced, BaseModel):
            shapes.add(produced)
    if len(shapes) == 1:
        return shapes.pop()
    return None


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
        # THE SIGNATURE-ORDERED WALK (F3-4's cure — the zero-false-
        # positive doctrine): the body's params map to the wired SOURCES
        # IN ORDER (the wiring's own rule — node.args records the sources
        # in signature order). The CROSS-PRODUCT convicted the legitimate
        # mixed signature — step(body, p, 3) for body(ctx, item: Report,
        # page: int) — the page param compared against p's Report and
        # refused, told to annotate the param that IS annotated. The real
        # mismatches still refuse; the DATA sources (the literals) need
        # no compatibility check (the runtime codec coerces them).
        wired = [a for a in node.args if a[0] == "p"]
        # strict=True: a wiring with MORE promise sources than the
        # body's params is E10's own refusal (the rule below); the zip
        # never silently truncates.
        for (_kind, parent_key), param in zip(wired, params, strict=False):
            parent = compiled.nodes.get(str(parent_key))
            if parent is None:
                continue
            # THE BODYLESS JOIN'S EDGE (the ergonomic tail's cure): the
            # join/collect node has NO body — its produced shape is the
            # ELEMENT model its own sources declare (the gather's
            # ``Promise[list[R]]`` shape). E5 skipping the bodyless
            # parent both directions was the probed hole: a consumer's
            # ``list[Other]`` param under a join of ``Report`` sources
            # crossed unvalidated. The shape is DERIVED here; no sources
            # → no shape → the skip is honest.
            if parent.body is None:
                produced = _join_produced_shape(compiled, parent)
            else:
                produced = body_hints(parent.body).get("return")
            if not (isinstance(produced, type) and issubclass(produced, BaseModel)):
                continue
            # THE LIST-PARAM ARM (the bodyless-join edge's other half): a
            # ``list[Model]`` param under a bodyless join is the GATHER's
            # own shape — the ELEMENT model is the comparison. Under a
            # MODEL-PRODUCING body parent it is a real mismatch (one
            # model cannot feed a list param — the runtime codec's
            # TypeAdapter reds it there; the compile names it here).
            if get_origin(param) is list:
                (elem,) = get_args(param)
                if not (isinstance(elem, type) and issubclass(elem, BaseModel)):
                    continue  # list[non-model] — the codec's TypeAdapter owns it
                if parent.body is not None:
                    diagnostics.append(
                        WorkflowValidationError(
                            "E5-incompatible-consumer",
                            "error",
                            f"{node.key!r} consumes {parent_key!r}'s "
                            f"{produced.__name__} as {param!r} — a "
                            "single-model producer cannot feed a list "
                            "param (the gather's shape is the join's: "
                            "wire the consumer under the bodyless "
                            "collect, or take the model bare)",
                        )
                    )
                    continue
                if produced is not elem and not issubclass(produced, elem):
                    diagnostics.append(
                        WorkflowValidationError(
                            "E5-incompatible-consumer",
                            "error",
                            f"{node.key!r} consumes {parent_key!r}'s "
                            f"list[{produced.__name__}] as {param!r} — "
                            "unrelated payload models: the join's "
                            "element and the consumer's element are "
                            "different models",
                        )
                    )
                continue
            if not (isinstance(param, type) and issubclass(param, BaseModel)):
                # THE DUCK-SHAPED HOLE (attack-3 M4's cure): the PAIRED
                # param carries NO model annotation (``Any``, a plain
                # dict, a duck) — the payload crosses UNVALIDATED and
                # UNCHECKED. The signature-ordered map's own caveat: a
                # mixed signature's non-model params (the ``page: int``
                # arm) sit at the DATA sources' positions — never paired
                # here.
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


def _rule_ctx_annotation(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E9 (F3-1's cure — THE PICKED DOOR): the body's ``ctx`` annotation
    must BE :class:`StepContext` (or a subclass), or the
    declared-unchecked Any/object. A FABRICATED ctx annotation (a
    stand-in that CLAIMS the type it does not have) is the wiring-site
    erasure: the checker verifies the body's ``ctx.*`` reads against the
    LIE, every face reports clean. The conformance is verified at BUILD
    — the annotation is verification, not documentation."""
    from taskq.workflows.api._ctx import StepContext

    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.body is None:
            continue
        hints = body_hints(node.body)
        ctx_ann = hints.get("ctx")
        if ctx_ann is None:
            continue  # the annotation's ABSENCE is honest (E4 owns the return's law)
        if ctx_ann is Any or ctx_ann is object:
            continue  # THE DECLARED-UNCHECKED: Any/object HONESTLY declare
            # the absence of verification — never a lie.
        if ctx_ann is StepContext or (
            isinstance(ctx_ann, type) and issubclass(ctx_ann, StepContext)
        ):
            continue
        diagnostics.append(
            WorkflowValidationError(
                "E9-ctx-annotation",
                "error",
                f"{node.key!r}'s body declares its context as {ctx_ann!r} — "
                "the ctx annotation must BE StepContext (or a subclass), "
                "or the declared-unchecked Any/object: the checker "
                "verifies the body's ctx.* reads against the REAL "
                "surface; a fabricated stand-in verifies a lie",
            )
        )
    return diagnostics


def _rule_arity(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E10 (F3-2's cure — THE ARITY GAP): the body's params (beyond ctx)
    must match the wired sources' count. A body taking MORE params than
    wired compiled + validated clean and the mismatch rode the RETRY
    LADDER MID-FLOW (the TypeError at the body's invocation — the
    run-time discovery of a wiring-time lie). The mismatch is a BUILD
    refusal; the ladder never sees it.

    THE MESSAGE'S NUMBERS ARE THE SIGNATURE'S (finding 13's cure): the
    declared count reads the body's ACTUAL signature
    (``inspect.signature``), never the resolved-hints dict — hints list
    only the ANNOTATED params, so a partially-annotated body's message
    under-counted (``def body(ctx, params: Ingest, page)`` reported
    ``takes 1 param(s)`` while the signature declares 2) and, worse, an
    UNANNOTATED extra param slipped the rule entirely (hints omit it)
    and rode the ladder as the very TypeError this rule exists to refuse.
    Arity is STRUCTURAL — the mismatch needs names, not annotations; the
    resolvable-hints guard stays only as the zero-false-positive skip
    (a function-scope model the compile cannot resolve is never
    convicted on a guess)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.body is None:
            continue
        hints = body_hints(node.body)
        if not hints:
            # THE UNRESOLVABLE ANNOTATIONS (the zero-false-positive
            # doctrine's own pin: a function-scope model the body's code
            # never names — the resolved hints are {}): the arity is a
            # GUESS on an unresolvable signature — skip (a guess is
            # never convicted; E4's own pin spells the doctrine).
            continue
        sig = inspect.signature(node.body)
        params = [
            name
            for name, p in sig.parameters.items()
            if name not in ("ctx", "return")
            and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]
        if len(params) == len(node.args) + 1:
            # THE DEPS SHAPE: one param beyond the wiring is the deps
            # contract's opt-in — E12 owns the extra parameter's contract
            # (the bound instance, the declared type); E10's count is
            # satisfied by the shape itself.
            continue
        if len(params) != len(node.args):
            diagnostics.append(
                WorkflowValidationError(
                    "E10-arity",
                    "error",
                    f"{node.key!r}'s body takes {len(params)} param(s) "
                    f"({', '.join(params)}) but the wiring wired "
                    f"{len(node.args)} argument(s) — the arity is the "
                    "wiring's own promise: a body param with no wired "
                    "source is a TypeError mid-flow (the ladder's "
                    "discovery of a wiring-time lie)",
                )
            )
    return diagnostics


def _rule_deps_contract(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E12 (THE DEPS SEAM'S CONTRACT — the DI capability's build-time
    conviction): a body that declares ONE parameter beyond ``ctx`` + its
    wired sources is the DEPS opt-in — the extra parameter receives the
    app's bound deps instance at every invocation. The contract, checked
    at build against the binding the compile carries:

    * a body declaring the deps shape where the app binds NO deps
      (``compiled.deps is None``) is refused — the message names the fix
      (bind ONE instance at the door, or drop the parameter);
    * a body whose deps annotation the bound instance does not SATISFY
      is refused — both type names in the message.

    The walk covers EVERY body invocation the runner can inject into:
    the step bodies, the map's item children, and the loop driver's
    bodies (the escalation body rides the same shape — wired 1). An
    unresolvable-annotations body (``body_hints`` → ``{}``) is SKIPPED —
    the zero-false-positive doctrine (a guess is never convicted); the
    runner's positional read is the shape's runtime face, and E10's own
    arithmetic owns the true mismatches."""
    from taskq.workflows.api._deps import deps_satisfied

    diagnostics: list[WorkflowValidationError] = []
    deps = compiled.deps
    checks: list[tuple[str, Callable[..., object], int]] = []
    for node in compiled.nodes.values():
        if node.body is not None:
            checks.append((node.key, node.body, len(node.args)))
        if node.map_item is not None:
            checks.append((f"{node.key}.item", node.map_item, 1))
        if node.loop_body is not None:
            checks.append((node.key, node.loop_body, 1))
        if node.loop_spec is not None and node.loop_spec.escalation_body is not None:
            escalation: Callable[..., object] = cast(
                "Callable[..., object]", node.loop_spec.escalation_body
            )  # Why: the escalation body's declared shape is the StepBody contract (the same cast the registration walk keeps).
            checks.append((f"{node.key}.escalation", escalation, 1))
    for owner, body, wired in checks:
        hints = body_hints(body)
        if not hints:
            continue  # THE UNRESOLVABLE ANNOTATIONS: a guess is never convicted
        params = [k for k in hints if k not in ("return", "ctx")]
        if len(params) != wired + 1:
            continue  # not the deps shape — E10 owns the arity
        deps_param = params[-1]
        declared = hints[deps_param]
        if deps is None:
            diagnostics.append(
                WorkflowValidationError(
                    "E12-deps-contract",
                    "error",
                    f"{owner!r}'s body declares a deps parameter "
                    f"({deps_param!r}) but this workflow's app binds no "
                    "deps — bind ONE instance at the door "
                    "(WorkflowApp(deps=…), or FlowRunner(…, deps=…) / "
                    "run(…, deps=…) for the direct doors), or drop the "
                    "parameter: one param beyond ctx + the wired sources "
                    "IS the deps contract, never a fourth data source",
                )
            )
            continue
        verdict = deps_satisfied(deps, declared)
        if verdict is False:
            diagnostics.append(
                WorkflowValidationError(
                    "E12-deps-contract",
                    "error",
                    f"{owner!r}'s body declares its deps as "
                    f"{getattr(declared, '__name__', declared)!r} but the "
                    "app's bound instance is "
                    f"{type(deps).__name__!r} — the bound instance must "
                    "SATISFY the body's declared deps type: bind the "
                    "declared type (or a subtype) on the app's deps=, or "
                    "fix the body's annotation",
                )
            )
    return diagnostics


def _rule_route_totality(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E15 (T27 — THE TOTALITY FENCE AT THE GRAPH LEVEL): the typed
    route's arms walked against the source's declared union — the
    checker-independent re-proof of the fence the wiring verbs raise
    (the compiled graph is public, mutable data; the rule owns the shape
    injected into it — E3's precedent). BOTH provable directions:

    * the arms' keys must be EXACTLY the union's member types — a
      MISSING member would route NOTHING (the silent drop the route
      exists to refuse: the reviewer's live conviction — a "video" tag
      that skipped both arms and terminalized succeeded-having-routed-
      nothing), an UNKNOWN member would never fire;
    * every arm body declares its arm's MODEL on the item param — the
      decode's target (R3): a DUCK-typed arm (``dict``/unannotated/
      ``Any``) consumes the element UNVALIDATED (E5's own conviction
      shape at the consumer face), an UNRELATED model is the wiring
      promising data the arm cannot accept.

    An unresolvable source or arm annotation SKIPS (the zero-false-
    positive doctrine — a guess is never convicted). The runtime door
    for the body that LIED about its union is ``RouterNotTotal``."""
    from taskq.workflows.chain import type_tag

    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.map_arms is None:
            continue
        # THE SOURCE'S DECLARED UNION (the resolved-hints seam — the
        # actor handle unwrapped first, the same resolution E5 reads).
        source_hints = body_hints(inner_fn(node.body)) if node.body is not None else {}
        returned = source_hints.get("return")
        member_types: dict[str, type[BaseModel]] = {}  # the type-tag → the member type
        if get_origin(returned) is list:
            (element_type,) = get_args(cast("type[object]", returned))
            union_members = (
                get_args(element_type) if get_origin(element_type) is not None else (element_type,)
            )
            for m in union_members:
                if isinstance(m, type) and issubclass(m, BaseModel):
                    member_types[type_tag(m)] = m
        if not member_types:
            continue  # the unresolvable/non-model return — the zero-false-positive skip
        declared = set(node.map_arms)
        if declared != set(member_types):
            missing = sorted(member_types[t].__name__ for t in set(member_types) - declared)
            diagnostics.append(
                WorkflowValidationError(
                    "E15-route-totality",
                    "error",
                    f"the route over {node.key!r} is not total — missing "
                    f"{missing}, declared {sorted(declared)}. A non-total "
                    "route is refused: the element it drops would route "
                    "NOTHING (the silent drop the route exists to refuse — "
                    "the skipped-both-arms run that terminalized SUCCEEDED). "
                    "Declare one arm per union member.",
                )
            )
            continue
        # THE ARMS' TYPED-PARAM CONTRACT (R3's compile face).
        for tag, arm in node.map_arms.items():
            hints = body_hints(inner_fn(arm.body))
            if not hints:
                continue  # the unresolvable arm — a guess is never convicted
            params = [k for k in hints if k not in ("return", "ctx")]
            if not params:
                continue  # E10/E12's faces own the arity; the item contract needs a param to read
            item_param = hints[params[0]]
            member_type = member_types[tag]
            member_name = member_type.__name__
            if item_param is Any or item_param is object:
                duck, related = True, False
            elif isinstance(item_param, type) and issubclass(item_param, BaseModel):
                duck = False
                related = (
                    member_type is item_param
                    or issubclass(member_type, item_param)
                    or issubclass(item_param, member_type)
                )
            else:
                duck, related = True, False
            if duck or not related:
                shape = "duck-typed (dict/unannotated/Any)" if duck else "an unrelated model"
                diagnostics.append(
                    WorkflowValidationError(
                        "E15-route-totality",
                        "error",
                        f"the route arm {route_child_key(node.key, tag)!r} "
                        f"({getattr(arm.body, '__name__', '<anon>')!r}) "
                        f"declares its item as {item_param!r} — {shape}: "
                        "the arm's param IS the decode's target (the typed "
                        f"boundary) and this shape consumes the {member_name} "
                        "element UNVALIDATED. Annotate the arm's param as "
                        f"{member_name} (or a related model).",
                    )
                )
    return diagnostics


def _rule_gate_door(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E13 (THE GATE DOOR — the rv2 finding's cure): a node's ``gates=``
    must carry DECLARATIONS (``GateDecl`` — the compile-visible shape the
    Mermaid render, the W1 timeout warning and T10's machinery read), not
    the channel's BOUND-DOOR object. ``app.channel().gate(Model)`` returns
    a ``TypedGate`` — the runtime delivery handle — and wiring it into
    ``step(gates=(…,))`` used to crash the validator's own gate walk (a
    raw ``AttributeError`` out of W1's ``gate.timeout_s`` read): the
    mistake now refuses by NAME, the message naming both doors and the
    fix."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        for gate in node.gates:
            if isinstance(gate, GateDecl):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the declaration's type is a LIE this rule convicts (a TypedGate rides the tuple at runtime) — the isinstance IS the check; statically the tuple claims GateDecl.
                continue
            diagnostics.append(
                WorkflowValidationError(
                    "E13-gate-door",
                    "error",
                    f"node {node.key!r} holds a {type(gate).__name__} in its "
                    "gates= — that is the channel.gate(...) BOUND-DOOR object "
                    "(the runtime delivery handle), not a gate declaration. "
                    "Declare the hold with GateDecl(name=…, payload_models=(…), "
                    "timeout_s=…) — the declaration is what the compile's hold "
                    "nodes, the timeout warning and the delivery runtime read.",
                )
            )
    return diagnostics


#: The AST containers whose interior makes a wait CONDITIONAL — the
#: static walk cannot prove a call inside one never fires (the
#: zero-false-positive bound E14's rule text spells).
_CONDITIONAL_CONTAINERS = (
    ast.If,
    ast.Try,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.Match,
    ast.ExceptHandler,
)


class _WaitSite:
    """ONE statically-readable ``ctx.wait_signal`` call (the E14/W4/W5
    walk's facts): the payload models' SOURCE names (``None`` = the
    argument is not a statically-resolvable tuple/name — the dynamic
    dispatch the compile never guesses at), the literal ``timeout_s=``
    kwarg when the call declares one, and whether the call sits in a
    conditional interior."""

    __slots__ = ("conditional", "names", "timeout_s")

    def __init__(
        self, names: tuple[str, ...] | None, timeout_s: float | None, conditional: bool
    ) -> None:
        self.names = names
        self.timeout_s = timeout_s
        self.conditional = conditional


def _wait_signal_sites(body: object) -> list[_WaitSite] | None:
    """The body's wait sites, STATICALLY read (the E4/E9 signature-read
    machinery's source-level extension): an AST walk over the body's own
    source, collecting every ``.wait_signal`` attribute call.

    Returns ``None`` when the body's source is NOT readable (a builtin,
    a partial, a syntax the walk cannot parse) — the caller SKIPS (the
    zero-false-positive doctrine: a guess is never convicted). The
    wrapper forms are unwrapped first (:func:`inner_fn` — the
    ``@app.actor`` handle's source is the INNER function's)."""
    # THE SOURCE READS' ONE HOME (own_source — the unwrap + the
    # slice-identity guard + the parse-family skip): the fence's
    # exec'd bodies, the builtins, the partials, the wrapper — every
    # unreadable or unprovable source SKIPS (a guess is never
    # convicted).
    source = own_source(body)
    if source is None:
        return None
    tree = ast.parse(source)

    sites: list[_WaitSite] = []

    def walk(node: ast.AST, conditional: bool) -> None:
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "wait_signal":
                names: tuple[str, ...] | None
                first = node.args[0] if node.args else None
                if isinstance(first, ast.Tuple):
                    names = tuple(elt.id for elt in first.elts if isinstance(elt, ast.Name))
                    if len(names) != len(first.elts):
                        names = None  # a non-name element — the dispatch is not statically readable
                elif isinstance(first, ast.Name):
                    names = (first.id,)
                else:
                    names = None
                timeout: float | None = None
                for kw in node.keywords:
                    if (
                        kw.arg == "timeout_s"
                        and isinstance(kw.value, ast.Constant)
                        and isinstance(kw.value.value, (int, float))
                    ):
                        timeout = float(kw.value.value)
                sites.append(_WaitSite(names, timeout, conditional))
        for child in ast.iter_child_nodes(node):
            walk(child, conditional or isinstance(node, _CONDITIONAL_CONTAINERS))

    walk(tree, False)
    return sites


def _node_bodies(node: object) -> list[Callable[..., object]]:
    """The node's OWN bodies (the walks' candidate set): the step body,
    the loop driver's body, the map's item child — whichever exist."""
    bodies: list[Callable[..., object]] = []
    for candidate in (
        getattr(node, "body", None),
        getattr(node, "loop_body", None),
        getattr(node, "map_item", None),
    ):
        if candidate is not None:
            bodies.append(candidate)
    return bodies


def _rule_gate_wiring(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E14 (THE GATE-WIRING WALK — the teardown round's E2-analog cure):
    the gate seat and the bodies' waits, walked against each other in
    BOTH provable directions. E2 walks the promise wiring (every
    produced promise is consumed); the gate wiring had no walk — a
    DECLARED gate the body never waits on, and a body's
    ``ctx.wait_signal`` with NO declared gate, both validated CLEAN.

    THE TWO PROVABLE FACES (the reviewer's own rule):

    * declared-never-waited — a node's ``gates=`` carries a ``GateDecl``
      and NO body of the node references ``wait_signal`` AT ALL: the
      declared hold seat has no waiter. Provable, because the conviction
      needs the ABSENCE of any reference (not the absence of a firing).
    * waited-never-declared — a body calls ``ctx.wait_signal`` and the
      node declares NO gate: the hold is invisible to every compile
      surface (the Mermaid hold nodes, W1's timeout read, the admin's
      resolve/deliver doors — the loop declaration's own docstring names
      the invisibility).

    THE CONDITIONAL-INTERIOR BOUND: a wait inside a branch cannot be
    proven never-to-fire — that face is the documented C9 question, the
    loop-kind mis-index W5 names (a conditional wait in a LOOP body
    mis-indexes the answer cursor). The static walk never guesses: an
    unreadable body (a builtin, a partial, an unparseable source) SKIPS
    the node — a guess is never convicted."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        bodies = _node_bodies(node)
        if not bodies:
            continue
        any_wait = False
        any_unreadable = False
        for body in bodies:
            sites = _wait_signal_sites(body)
            if sites is None:
                any_unreadable = True
                continue
            if sites:
                any_wait = True
        if node.gates:
            if any_wait or any_unreadable:
                continue  # a reference exists (or the walk cannot prove the absence) — not the provable case
            names = ", ".join(repr(gate.name) for gate in node.gates)
            diagnostics.append(
                WorkflowValidationError(
                    "E14-gate-wiring",
                    "error",
                    f"node {node.key!r} declares gate(s) {names} but its "
                    "body NEVER waits — no wait_signal reference anywhere "
                    "in the body: the declared hold seat has no waiter "
                    "(work that waits for nobody). Wire the body's "
                    "ctx.wait_signal to the declared payload models, or "
                    "drop the gate declaration",
                )
            )
        elif any_wait:
            # THE T26 RULING (the hostile-review merge's design call): the
            # DANGEROUS face is declared-never-waited (the error above —
            # "we have an approval step" shipping with no approval in it).
            # A wait with NO declared gate is NOT a lie: the wait site is
            # SELF-DESCRIBING (the payload models + the timeout ride the
            # call; the hold row is real; the listener, the resolve door
            # and the broadcast all work). What is lost is the COMPILE
            # visibility — the Mermaid hold node, W1's timeout read — so
            # this face is the WARNING, never the refusal.
            diagnostics.append(
                WorkflowValidationError(
                    "E14-gate-wiring",
                    "warning",
                    f"node {node.key!r}'s body calls ctx.wait_signal but "
                    "declares NO gate — the hold is ROW-real (the wait site "
                    "carries the payload models and the timeout) but "
                    "COMPILE-invisible: no Mermaid hold node, no W1 timeout "
                    "read. Declare it for the compile surfaces: step(body, …, "
                    "gates=(GateDecl(name=…, payload_models=(…), "
                    "timeout_s=…),))",
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
    refusal (the warning class; the timer-policy matrix is T10's). The
    walk reads only ``GateDecl`` declarations: a foreign object in
    ``gates=`` (the door-confusion E13 convicts) is SKIPPED here — the
    rule walk never crashes on a declaration lie (the raw
    ``AttributeError`` out of this walk was the finding's own
    conviction)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        for gate in node.gates:
            if not isinstance(gate, GateDecl):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the declaration's type is a lie E13 convicts (see _rule_gate_door) — the walk's defense is the runtime check.
                continue
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


def _rule_gate_timeout_split(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """W4 (THE GATE-TIMEOUT SPLIT, cross-checked — the teardown round's
    cure): the timeout is declared in TWO places — the ``GateDecl`` (the
    compile surfaces' face) and the wait site (the runtime's arm). Where
    BOTH are statically readable and they disagree, the drift is NAMED —
    the two sources must agree or the operator reads which side is
    which. The precedence lives on the declaration's own docstring (the
    union face): the wait site's value arms the runtime; the
    declaration feeds the compile surfaces and never arms the runtime.
    A non-literal wait timeout (a variable the compile cannot read) is
    SKIPPED — a guess is never convicted (the zero-false-positive
    doctrine)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if not node.gates:
            continue
        timeout_by_name: dict[str, float] = {}
        for gate in node.gates:
            if not isinstance(gate, GateDecl) or gate.timeout_s is None:  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the declaration's type is a LIE a runtime gate can break (E13's subject — a TypedGate rides the tuple); the isinstance IS the check, statically it reads unnecessary.
                continue
            # THE MATCH KEY IS THE PAYLOAD MODEL'S NAME (the runtime's
            # own identity — the wait site's tuple names the MODELS;
            # the Decl's name= is the free-form label the Mermaid face
            # reads).
            for model in gate.payload_models:
                timeout_by_name[getattr(model, "__name__", "")] = gate.timeout_s
        if not timeout_by_name:
            continue
        for body in _node_bodies(node):
            sites = _wait_signal_sites(body)
            if sites is None:
                continue
            for site in sites:
                if site.names is None or site.timeout_s is None:
                    continue  # the dispatch or the bound is not statically readable — skip
                for name in site.names:
                    declared = timeout_by_name.get(name)
                    if declared is not None and declared != site.timeout_s:
                        diagnostics.append(
                            WorkflowValidationError(
                                "W4-gate-timeout-split",
                                "warning",
                                f"node {node.key!r}'s gate for {name!r} declares "
                                f"timeout_s={declared:g} but its body waits with "
                                f"timeout_s={site.timeout_s:g} — the timeout is "
                                "DOUBLE-SOURCED and the sources disagree: the "
                                "WAIT SITE's value arms the runtime (the hold's "
                                "expiry), the DECLARATION feeds the compile "
                                "surfaces (the Mermaid face, W1) — align them "
                                "(the operator reads both; a silent drift is a "
                                "deadline lie on one of the two faces)",
                            )
                        )
    return diagnostics


def _rule_loop_wait_shape(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """W5 (THE LOOP'S SHAPE LAW — the teardown round's cure, the T26
    review's C9): the loop's answer cursor IS the row's iteration
    counter — iteration k's wait consumes the k-th answer. A LOOP body
    whose ``wait_signal`` sits in a CONDITIONAL interior (an ``if``, a
    ``try``, a nested loop) can SKIP a wait on some iterations: the
    counter and the answer sequence drift apart — the mis-index (the
    operator's later decision lands on the wrong iteration's consume).
    The shape law was DOCSTRING-ENFORCED ("a loop body declares ONE wait
    per iteration"); the conditional-wait shape is now the NAMED
    warning. The unconditional wait is the shape kept — the
    zero-false-positive bound (the plain STEP's conditional wait is
    fine: the per-attempt cursor replays it honestly)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.loop_spec is None:
            continue  # the shape law is the LOOP's — the per-attempt cursor owns the plain steps
        for body in _node_bodies(node):
            sites = _wait_signal_sites(body)
            if sites is None:
                continue
            for site in sites:
                if not site.conditional:
                    continue
                diagnostics.append(
                    WorkflowValidationError(
                        "W5-loop-wait-shape",
                        "warning",
                        f"loop {node.key!r}'s body waits CONDITIONALLY (the "
                        "wait_signal sits inside a branch) — the loop's "
                        "shape law is ONE wait per iteration, unconditional: "
                        "the row's iteration counter IS the answer cursor, "
                        "so an iteration that does not wait drifts the "
                        "sequence (the operator's answer consumed by the "
                        "wrong iteration — the T26 review's C9 mis-index). "
                        "Make the wait unconditional (a no-op refinement "
                        "arm for the skip case), or move the gate to a "
                        "plain step the loop calls",
                    )
                )
    return diagnostics


def _rule_eternal_loop(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """W3: the loop's "waits forever" class — the docstring's own
    promise (F-LOOP-6's finding, kept honest): a loop with NO ``until=``
    and NEITHER wall set can never stop on its own; the refiner runs
    until the fleet dies under it. Probably wrong, never a refusal (the
    warning class — an unbounded refiner may be exactly what a
    daemon-loop author means; the explicitness is the point)."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        spec = node.loop_spec
        if spec is None:
            continue
        if node.loop_until is None and spec.max_iterations is None and spec.budget_s is None:
            diagnostics.append(
                WorkflowValidationError(
                    "W3-eternal-loop",
                    "warning",
                    f"loop {node.key!r} has no until= predicate and neither "
                    "wall set (no max_iterations, no budget_s) — a loop that "
                    "waits forever: it can never stop on its own; declare "
                    "until=, max_iterations=, or budget_s= explicitly",
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
    ``carry_type=`` (or the model instance passed as ``initial=``, whose
    own type IS the declaration) vs the body's ``Refine[...]`` feedback
    type — both pydantic models and unrelated means the loop threads a
    carry the body cannot receive. Unenforceable when undeclarable (no
    model declared — a scalar/dict initial, or an unresolvable body
    hint): the zero-false-positive doctrine — a guess is never
    convicted."""
    from types import UnionType
    from typing import get_args, get_origin

    from taskq.workflows.api._loop import Refine

    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        spec = node.loop_spec
        if spec is None or node.loop_body is None:
            continue
        # THE CARRY'S TYPED SPLIT (the ergonomic tail's cure): the
        # CARRIER-TYPE is its own declared field — the model the body's
        # Refine[Feedback] must match. It is DERIVED from the initial
        # carry when the author passed a model instance (the instance
        # carries its own type); a dict/list/scalar carry declares NO
        # model — the check is skipped, never guessed (the old silent
        # no-check was the conflation's gap).
        carry_model = spec.carry_type
        if carry_model is None:
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


def _rule_loop_promise_carry(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """E11: the loop's initial carry is a VALUE, never a promise handle.

    THE LOOP-PARENTS GAP (probe-convicted at this head): ``loop()``'s
    signature takes no ``*args`` — a parent's result cannot be wired as
    an edge — so an author who passes a ``Promise`` as ``initial=`` (the
    natural reading of "the loop starts from the parent's result")
    threaded the HANDLE itself into the carry: ``jsonable(Promise)``
    raises ``UnencodableValue`` at the FIRST CLAIM — mid-flow, untyped
    by any compile rule, after the rows exist. The refusal belongs at
    the construction door: the promise handle is refused here, with the
    named fix (await-shape: wire the parent's result into a first STEP
    whose body returns the initial carry, and start the loop from THAT
    value — or read the parent's result in the loop body via
    ``ctx.substep``). The rule reads the handle's own identity (a
    ``Promise`` instance), never its static type — the smuggle-check's
    shape."""
    from taskq.workflows.api._graph import Promise

    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        spec = node.loop_spec
        if spec is None:
            continue
        if isinstance(spec.initial_carry, Promise):
            diagnostics.append(
                WorkflowValidationError(
                    "E11-loop-promise-carry",
                    "error",
                    f"loop {node.key!r}'s initial= is a promise handle "
                    f"(wired from {spec.initial_carry.key!r}) — the loop's "
                    "initial carry is a VALUE, never a handle: the handle "
                    "cannot ride the row (the first claim died "
                    "UnencodableValue mid-flow), and the loop takes no "
                    "promise args. Wire the parent's result through a "
                    "first step whose body returns the initial carry and "
                    "start the loop from that step's promise, or read the "
                    "parent's result inside the body (ctx.substep).",
                )
            )
            continue
        # THE STRUCTURE WALK (finding 12's cure — the claim→crash→reclaim
        # loop's second face): a promise handle NESTED inside the carry —
        # a dict's value, a list element, a model's field — dies the SAME
        # death the bare handle does (the rehydration's own walk —
        # ``encode_data_arg`` — walks dicts, lists, and models, and the
        # nested handle passes through it unchanged into the jsonb write,
        # where the FIRST CLAIM of the resume loop dies untyped). The
        # validator walks the SAME structure the rehydration walks, so
        # the construction door refuses every shape the runtime cannot
        # encode.
        nested = _nested_promise(spec.initial_carry)
        if nested is not None:
            diagnostics.append(
                WorkflowValidationError(
                    "E11-loop-promise-carry",
                    "error",
                    f"loop {node.key!r}'s initial= carries a promise "
                    f"handle NESTED in its structure (wired from "
                    f"{nested!r}) — the loop's initial carry is a "
                    "VALUE, never a handle, at ANY depth: the nested "
                    "handle cannot ride the row (the first claim after "
                    "the crash died UnencodableValue mid-reclaim), and "
                    "the loop takes no promise args. Wire the parent's "
                    "result through a first step whose body returns the "
                    "carry VALUE and start the loop from that step's "
                    "promise, or read the parent's result inside the "
                    "body (ctx.substep).",
                )
            )
    return diagnostics


_NESTED_WALK_DEPTH = 8


def _nested_promise(value: object, depth: int = 0) -> object | None:
    """The carry's STRUCTURE walk: the promise handle at any depth the
    rehydration's codec can reach — dict values (and keys' faces aside,
    only the values ride the walk the codec does), list/tuple elements,
    and pydantic model FIELDS (``model_dump`` walks them the same way).
    Depth-bounded (the carry is a value, not a graph); a cycle is the
    author's own recursion and the bound ends the walk honestly. Returns
    the FOUND HANDLE's KEY (the message names it) or None."""
    from taskq.workflows.api._graph import Promise

    if depth > _NESTED_WALK_DEPTH:
        return None
    if isinstance(value, Promise):
        return str(value.key)
    if isinstance(value, dict):
        # The narrowing lands `dict[Unknown, Unknown]` (the carry is a
        # value of declared `object`); the cast pins the walk's element
        # type — the walk reads VALUES only.
        mapping = cast("dict[object, object]", value)
        for v in mapping.values():
            found = _nested_promise(v, depth + 1)
            if found is not None:
                return found
        return None
    if isinstance(value, (list, tuple, set, frozenset)):
        members = cast(
            "tuple[object, ...]", value if isinstance(value, tuple) else tuple(cast("Any", value))
        )
        for v in members:
            found = _nested_promise(v, depth + 1)
            if found is not None:
                return found
        return None
    if isinstance(value, BaseModel):
        fields: dict[str, object] = dict(value.__dict__)
        for v in fields.values():
            found = _nested_promise(v, depth + 1)
            if found is not None:
                return found
    return None


def _rule_join_for_progress(compiled: CompiledWorkflow) -> list[WorkflowValidationError]:
    """W2 (T21/DH8): a join whose only reader is the explorer's display —
    the JOIN-FOR-PROGRESS anti-pattern — warns at validate. The
    detectable shape: a join node (a gather, or the map's auto-join)
    whose promise the wiring SUNK (``sink(...)`` — the declared
    no-dataflow-consumer marker). The blocking join's rows-only semantics
    (T08's) make the DAG wait on a display question; the read-side
    ``aggregate=`` fn (the map declaration) answers the same question
    mid-flight, unblocked. Advisory — the aggregate may be dataflow in
    spirit and sunk for wiring economy; the warning names the door."""
    diagnostics: list[WorkflowValidationError] = []
    for node in compiled.nodes.values():
        if node.kind not in ("gather", "map_join"):
            continue
        if node.key in compiled.sunk:
            diagnostics.append(
                WorkflowValidationError(
                    "W2-join-for-progress",
                    "warning",
                    f"join {node.key!r} is sunk — its only reader is the "
                    "explorer's display, and the DAG still blocks on it (a "
                    "join is for DATAFLOW): declare the map's "
                    "``aggregate=`` fn (the read-side aggregate — "
                    "mid-flight, unblocked) instead",
                )
            )
    return diagnostics
