"""The flow API's app surface (T09): ``WorkflowApp`` — the authoring
entry — ``@app.actor`` / ``@app.workflow``, the compiled workflow, and the
signal channel's TYPED-GATE door.

``@app.actor`` is a DIFFERENT decorator from ``taskq.actor`` but composes
with the SAME machinery: the decorated function is registered into the
workflow definition registry (D1 — the only body source), and
:meth:`WorkflowActor.actor_config` projects it into the estate's
``ActorConfig`` carrier so the config-sync, the drift guards, the
deregistration guards, the admin's actors page and ``TASKQ_QUEUES_STRICT``'s
fail-fast all SEE workflow actors — no second, invisible actor population
(GAPS-ESTATE F3). The vanilla path is untouched: this decorator adds
registrations, it never edits an existing one (the byte-identity pin
holds).

The channel is a PER-WORKFLOW object created by :meth:`WorkflowApp.channel`
— its registry is the declared gates (keyed by gate name); its lifetime is
the workflow definition's (module-level through the app, never a global
singleton). ``channel.gate(Model) → TypedGate[Model]`` binds the payload
type ONCE at the gate; the wrong model is simply not assignable at the
deliver call site (the classic ``(type[P], P)`` signature is the documented
dead door — both solvers unify ``P = A | B``).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, cast, overload

from pydantic import BaseModel

from taskq.workflows.api._graph import BuildGraph, NodeDecl, Promise, record_under
from taskq.workflows.definitions import DuplicateWorkflowError

__all__ = [
    "CompiledWorkflow",
    "SignalChannel",
    "TypedGate",
    "WorkflowActor",
    "WorkflowApp",
]


class WorkflowActor:
    """The workflow actor handle: preserves ``__name__``/``__doc__``, is
    DIRECTLY callable for unit tests (``await fetch(ctx, params)``), and
    carries the placement + retry + wiring metadata the compile and the
    runner consume."""

    def __init__(
        self,
        fn: Callable[..., Awaitable[object]],
        *,
        name: str,
        queue: str,
        max_attempts: int,
        retry_kind: str,
        capture_policy: str,
        redact: Callable[[dict[str, object]], dict[str, object]] | None,
    ) -> None:
        self._fn = fn
        self.name = name
        self.queue = queue
        self.max_attempts = max_attempts
        self.retry_kind = retry_kind
        self.capture_policy = capture_policy
        self.redact = redact

    @property
    def fn(self) -> Callable[..., Awaitable[object]]:
        return self._fn

    # Callable-preserving: the DIRECT (unit-test) call has the body's own
    # signature — (ctx, *data). The WIRING never calls this; it goes
    # through the recorder's verbs (wf.step(...)), which resolve bodies
    # from the definition registry (D1).
    async def __call__(self, ctx: object, /, *args: object) -> object:
        return await self._fn(ctx, *args)

    def __getattr__(self, attr: str) -> object:
        return getattr(self._fn, attr)

    def actor_config(self, workflow_name: str) -> dict[str, object]:
        """The F3 projection: the ``ActorConfig``-compatible carrier the
        estate's config-sync and guards consume (the same shape
        ``ActorRef`` produces at worker startup). A workflow actor is
        VISIBLE to every existing guard by construction — the drift
        machinery, the deregistration guards, the admin page,
        ``TASKQ_QUEUES_STRICT`` (pin 9)."""
        return {
            "actor": f"{workflow_name}.{self.name}",
            "queue": self.queue,
            "max_attempts": self.max_attempts,
            "retry_kind": self.retry_kind,
        }


class TypedGate[G: BaseModel]:
    """The BOUND-DOOR form (Package B, finding 1): the payload type is
    bound ONCE at the gate; the wrong model is not assignable at the
    deliver call site. One door, two addresses: by ``(run, node, gate)``
    for the wire form, by hold id for the reply form (T10's client)."""

    def __init__(self, channel: SignalChannel, *payload_models: type[BaseModel]) -> None:
        self._channel = channel
        self.payload_models = payload_models
        self.name = payload_models[0].__name__


class SignalChannel:
    """The per-workflow signal channel: the declared gates' registry. Its
    runtime delivery is wired by T10's HITL module (the row is the truth);
    the DECLARATION is the compile's — mermaid's hold nodes and the
    validate warnings read it from here."""

    def __init__(self, workflow_name: str) -> None:
        self.workflow_name = workflow_name
        self._gates: dict[str, TypedGate[BaseModel]] = {}

    def gate[G: BaseModel](self, *payload_models: type[G]) -> TypedGate[G]:
        """Bind the payload type(s) ONCE at the gate. The delivery
        runtime (the CAS'd resume, the refusal shape) is T10's HITL
        module — the DECLARATION is the compile's: this registry is what
        the mermaid render's hold nodes, the validate warnings and the
        typed delivery all read."""
        if not payload_models:
            raise TypeError("a gate binds at least one payload model")
        declared = TypedGate[G](self, *payload_models)
        self._gates[declared.name] = declared  # pyright: ignore[reportArgumentType]  # Why: the registry is keyed by the bound gate's own name; the variance is the door's deliberate shape (one channel, many gates).
        return declared


@dataclass(frozen=True, slots=True)
class CompiledWorkflow:
    """The compile's product: the spelled graph, lowered-ready — the SAME
    module compiles to the SAME graph, byte-stable (the Mermaid golden's
    law). Validation and Mermaid are PURE functions of this object."""

    name: str
    nodes: dict[str, NodeDecl]
    sunk: tuple[str, ...]
    terminal: str | None
    channel: SignalChannel
    input_type: object = None
    #: The workflow's declared chains (T20) — the chain SOURCE node owns
    #: its Chain; the runner resolves chain-step routes from here.
    chains: tuple[object, ...] = ()
    #: The cross-graph smuggles the wiring verbs recorded (attack-3 M3's
    #: cure): ``(consumer_key, parent_key)`` pairs — validate's E7 reads
    #: it.
    smuggled: tuple[tuple[str, str], ...] = ()
    #: The app's declared queue universe (``"default"`` + every workflow
    #: actor's queue + TASKQ_QUEUES' names — attack-3 M5's cure):
    #: validate's W2 reads it; ``None`` = no universe declared, nothing
    #: to convict.
    known_queues: frozenset[str] | None = None

    def node_keys(self) -> list[str]:
        """The compiled node keys (the wiring's census)."""
        return list(self.nodes)

    def parents_of(self, key: str) -> list[str]:
        """One node's incoming edges (the promise wiring's parent side)."""
        return list(self.nodes[key].parents)

    def skip_predicate(self, key: str) -> object | None:
        """The node's dispatch-time skip predicate (cut #4), if declared."""
        return self.nodes[key].skip

    def validate(self) -> None:
        """The checker-independent validator (the totality refusals) —
        :mod:`taskq.workflows.api._validate`."""
        from taskq.workflows.api._validate import validate_compiled

        validate_compiled(self)

    def mermaid(self) -> str:
        """The compile-time Mermaid emission (byte-stable) —
        :mod:`taskq.workflows.api._mermaid`."""
        from taskq.workflows.api._mermaid import render_mermaid

        return render_mermaid(self)


class WorkflowApp:
    """The authoring surface: one app, many workflows; the definitions'
    registry is shared with the engine (D1 — no second registry)."""

    def __init__(self, *, actor: str = "wf") -> None:
        self._actor = actor
        self._workflows: dict[str, Callable[..., object]] = {}
        #: The app's declared queue universe (the workflow actors'
        #: queues — validate's W2 rule reads the compiled projection;
        #: attack-3 M5's cure).
        self._actor_queues: set[str] = set()

    @overload
    def actor(
        self,
        fn: Callable[..., Awaitable[object]],
        /,
        *,
        name: str | None = None,
        queue: str = "default",
        max_attempts: int = 3,
        retry_kind: str = "transient",
        capture_policy: Literal["none", "errors-only", "all"] = "errors-only",
        redact: Callable[[dict[str, object]], dict[str, object]] | None = None,
    ) -> WorkflowActor: ...

    @overload
    def actor(
        self,
        fn: None = None,
        /,
        *,
        name: str | None = None,
        queue: str = "default",
        max_attempts: int = 3,
        retry_kind: str = "transient",
        capture_policy: Literal["none", "errors-only", "all"] = "errors-only",
        redact: Callable[[dict[str, object]], dict[str, object]] | None = None,
    ) -> Callable[[Callable[..., Awaitable[object]]], WorkflowActor]: ...

    def actor(
        self,
        fn: Callable[..., Awaitable[object]] | None = None,
        /,
        *,
        name: str | None = None,
        queue: str = "default",
        max_attempts: int = 3,
        retry_kind: str = "transient",
        capture_policy: Literal["none", "errors-only", "all"] = "errors-only",
        redact: Callable[[dict[str, object]], dict[str, object]] | None = None,
    ) -> WorkflowActor | Callable[[Callable[..., Awaitable[object]]], WorkflowActor]:
        """``@app.actor`` — the workflow step decorator: registers the
        body into the workflow definition registry (D1) and projects the
        F3 carrier. The redact hook POST-COMPOSES on the capture chain
        (chain → hook, never a replacement — pin 11's composed-anyway)."""

        def decorate(body: Callable[..., Awaitable[object]]) -> WorkflowActor:
            actor_name = name or body.__name__
            handle = WorkflowActor(
                body,
                name=actor_name,
                queue=queue,
                max_attempts=max_attempts,
                retry_kind=retry_kind,
                capture_policy=capture_policy,
                redact=redact,
            )
            self._actor_queues.add(queue)
            return handle

        if fn is not None:
            return decorate(fn)
        return decorate

    def workflow(
        self,
        name: str,
        *,
        capture: Literal["none", "errors-only", "all"] = "errors-only",
        redact: Callable[[dict[str, object]], dict[str, object]] | None = None,
        channel: SignalChannel | None = None,
    ) -> Callable[[Callable[..., Awaitable[object]]], Callable[..., Awaitable[object]]]:
        """``@app.workflow(name, capture=…, redact=…)`` — the per-workflow
        declaration (§10.3's policies). The declaration is what T04's
        capture writer and every export surface consume; ``redact=fn``
        POST-COMPOSES on the default chain's output (it can only redact
        more, never less — TORS-REV-0.16 §G1)."""
        if name in self._workflows:
            raise DuplicateWorkflowError(f"workflow {name!r} is already declared on this app")

        def decorate(
            build_fn: Callable[..., Awaitable[object]],
        ) -> Callable[..., Awaitable[object]]:
            self._workflows[name] = build_fn
            build_fn.__wf_name__ = name  # type: ignore[attr-defined]  # Why: the declaration rides the function; the app is the registry.
            build_fn.__wf_capture__ = capture  # type: ignore[attr-defined]
            build_fn.__wf_redact__ = redact  # type: ignore[attr-defined]
            return build_fn

        return decorate

    def channel(self) -> SignalChannel:
        """The per-workflow signal channel (the declared gates' registry)."""
        return SignalChannel(self._actor)

    def has(self, name: str) -> bool:
        return name in self._workflows

    def get(self, name: str) -> CompiledWorkflow:
        """Compile the NAMED workflow: run its build function under a
        fresh recorder. Same module → same graph, every time.

        The build function is SYNC and PURE — the wiring is compile-time
        dataflow spelling (the recorder's verbs); nothing async happens
        at compile. Its RETURN is the terminal promise (or an explicit
        ``build(...)`` result)."""
        build_fn = self._workflows.get(name)
        if build_fn is None:
            raise KeyError(f"workflow {name!r} is not declared on this app")
        graph = BuildGraph()
        returned = record_under(graph, build_fn)
        if graph.terminal is None:
            if isinstance(returned, Promise):
                graph.terminal = returned.key
            elif returned is not None:
                raise TypeError(
                    f"workflow {name!r}'s build function returned "
                    f"{type(returned).__name__!r} — the return IS the "
                    "terminal promise (wire one, or return build(p))"
                )
        channel = SignalChannel(name)
        compiled = CompiledWorkflow(
            name=name,
            nodes=graph.nodes,
            sunk=graph.sunk,
            terminal=graph.terminal,
            channel=channel,
            smuggled=graph.smuggles,
            known_queues=self._known_queues(),
            chains=tuple(graph.chains),
        )
        _register_bodies(compiled, redact=getattr(build_fn, "__wf_redact__", None))
        return compiled

    def _known_queues(self) -> frozenset[str]:
        """The app's declared queue universe: ``default`` + the workflow
        actors' queues + TASKQ_QUEUES' names (the strict boot's own list
        — the same universe the worker's fail-fast reads; attack-3 M5's
        build-side face)."""
        import os

        queues = {"default", *self._actor_queues}
        declared = os.environ.get("TASKQ_QUEUES", "")
        queues.update(q.strip() for q in declared.split(",") if q.strip())
        return frozenset(queues)


def _register_bodies(
    compiled: CompiledWorkflow,
    *,
    redact: object | None = None,
) -> None:
    """D1: the compiled nodes' bodies go into the DEFINITION REGISTRY —
    the dispatch resolves them from there, never from a per-call map.
    The map's CHILDREN resolve under their OWN step keys
    (``<map>.item`` — the fork's per-item identity): the alias is the
    same body, registered under the key the child rows carry.

    THE ESCALATION STEP (attack-3 H1's cure): a loop declaring
    ``on_exhausted="escalate"`` registers ITS escalation body under the
    ``loop.escalation`` step key — the author's ``escalates_to=`` body,
    or the framework default. The outbox's consumer job resolves its
    body from HERE (D1) at claim: the escalation is never the
    ``loop_escalation``-actor ghost (a hardcoded binding whose body
    never resolves). ONE escalation step per workflow: two loops
    declaring DIFFERENT escalation bodies is the refused shadow."""
    from taskq.workflows.api._loop import (
        ESCALATION_STEP_KEY,
        LoopSpec,
        default_escalation_body,
    )
    from taskq.workflows.definitions import (
        DuplicateStepBodyError,
        StepBody,
        WorkflowDef,
        get_registry,
    )

    bodies: dict[str, StepBody] = {}
    loop_policies: dict[str, str] = {}
    escalation_body: StepBody | None = None
    aggregates: dict[str, object] = {}
    for node in compiled.nodes.values():
        if node.body is not None:
            bodies[node.key] = node.body
        # The map's CHILDREN resolve under their OWN step keys
        # (``<source>.item`` — the fork's per-item identity): the alias
        # is the item body, registered under the key the child rows
        # carry.
        if node.map_item is not None:
            bodies[f"{node.key}.item"] = node.map_item
        if node.loop_spec is not None:
            # The loop attachment's declared type (the compile-visible
            # LoopSpec — the NodeDecl field is the object-typed carrier).
            spec = cast("LoopSpec", node.loop_spec)  # pyright: ignore[reportUnknownVariableType]  # Why: the NodeDecl's loop attachment is the object-typed carrier; the driver's own declaration is the LoopSpec.
            loop_policies[node.key] = spec.on_exhausted
            if spec.on_exhausted == "escalate":
                candidate: StepBody = cast(
                    "StepBody", spec.escalation_body or default_escalation_body
                )  # Why: the escalation body's declared shape is the StepBody contract.
                if escalation_body is not None and escalation_body is not candidate:
                    raise DuplicateStepBodyError(
                        f"workflow {compiled.name!r} declares TWO loop "
                        f"escalation bodies ({node.key!r}'s differs from the "
                        f"first) — one escalation step per workflow "
                        f"({ESCALATION_STEP_KEY!r}); declare the same body or "
                        "fold them"
                    )
                escalation_body = candidate
        # THE MAP'S DECLARED AGGREGATE (T21): keyed by the SOURCE node's
        # step key — the children's PARENT (the read surfaces resolve by
        # the parent row's own step key; the durable leg is the flow
        # root's stamped workflow name).
        if node.map_aggregate is not None:
            aggregates[node.key] = node.map_aggregate
    if escalation_body is not None:
        bodies[ESCALATION_STEP_KEY] = escalation_body
    # THE CHAIN STEP BODIES (T20): registered under their own step keys
    # (D1 — the emitted + fork-spawned chain rows resolve their bodies
    # from the registry; the chain's steps have no compiled NodeDecl).
    from taskq.workflows.chain import Chain

    for chain_decl in compiled.chains:
        chain = cast("Chain", chain_decl)
        for step_key, chain_step in chain.steps.items():
            if chain_step.body is not None:
                bodies.setdefault(step_key, chain_step.body)
    if bodies or aggregates or loop_policies:
        get_registry().register(
            WorkflowDef(
                name=compiled.name,
                bodies=bodies,
                capture_policy="errors-only",
                redact=redact,  # type: ignore[arg-type]  # Why: the app's redact declaration rides the registered definition — the hold-context chain's hook (the same callable the capture path composes).
                loop_policies=loop_policies,
                aggregates=aggregates,  # pyright: ignore[reportArgumentType]
            )
        )
