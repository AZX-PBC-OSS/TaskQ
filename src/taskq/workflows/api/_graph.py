"""The flow API's wiring graph — :class:`Promise`, the fan-in/fan-out
combinators, and the compile-time recorder (T09).

THE TYPE CONTRACT (§5.4-4): plain types for data, ``Promise[T]`` for
wiring. A wiring function decorated with ``@app.workflow`` runs once under
the :class:`_Graph` recorder; its calls to :func:`step` / :func:`map` /
:func:`gather` / :func:`sink` / :func:`build` spell the DAG as DATAFLOW —
a promise consumed downstream is an edge; a call with several promise
arguments is the fan-in join. Nothing is stored about the tree: the graph
is SPELLED by the wiring, compiled fresh from the same module every time
(same module → same compile, byte-stable — the Mermaid golden's law).

THE FLAT SHAPE: ``map`` returns a plain ``Promise[list[R]]`` over the
map's JOIN — never ``Promise[list[Promise[R]]]`` (the checker rejected the
nested shape at T01's v2:124).

The recorder is a ``ContextVar``, never a global: two apps compile
concurrently without seeing each other's wiring.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel

from taskq.workflows._types import EdgeFailurePolicy
from taskq.workflows.chain import Chain

if TYPE_CHECKING:
    from typing import Never

__all__ = [
    "Exit",
    "GateDecl",
    "NodeDecl",
    "Promise",
    "WorkflowBuildError",
    "active_graph",
    "build",
    "gather",
    "map_source",
    "sink",
    "step",
]

#: A step body: the coroutine the runner executes for one node —
#: ``await body(ctx, *decoded_parent_results)`` (the fan-in's decoded
#: args — cut #14's decode-once; the bodies never see raw rows).
#:
#: THE MECHANISM (the type-mechanism probes, pyright 1.1.414 + ty 0.0.85):
#: the WIRING verbs are generic over the body's RESULT —
#: :func:`step` is ``step[R](body: Callable[..., Awaitable[R]]) -> Promise[R]``
#: (R is INFERRED from the body's declared return — the promise carries the
#: body's type); :func:`gather` is
#: ``gather[R](promises: list[Promise[R]]) -> Promise[list[R]]`` (the join
#: PRESERVES the element type); :func:`build` is
#: ``build[R](result: Promise[R], *residuals: Promise[Never]) -> Promise[R]``
#: (a residual must be a produces-nothing handle — a body annotated
#: ``-> NoReturn``; any REAL promise in the residual slot is the checker's
#: error, because :class:`Promise` is COVARIANT and ``Never`` is the bottom
#: type). The verbs' bodies still take the DECODED payload shapes — the
#: wiring-site compat between a producer's model and a consumer's param is
#: the VALIDATOR's face (:func:`taskq.workflows.api._validate`'s
#: ``E5-incompatible-consumer``); the checker's face is the HANDLE flow.
BodyFn = Callable[..., Awaitable[object]]

#: A skip guard: ``bool`` or ``Callable[[state], bool]`` — evaluated AT
#: DISPATCH against the flow's state (A-CRITICAL-4, cut #4), never at
#: create time.
SkipPredicate = Callable[[dict[str, object]], bool]


class WorkflowBuildError(TypeError):
    """The wiring itself is malformed — refused at compile, before any
    row exists."""


class Exit[T]:
    """THE TYPED EARLY-EXIT SENTINEL (T09's §17.1 — the None-end's
    sanctioned escape): a body returns ``Exit(payload)`` to
    TERMINAL-SUCCEED the node with a typed result NOW — the workflow's
    answer is decided, the remaining downstream graph does not run.

    * THE RUNNER UNWRAPS IT: the exited node terminal-succeeds with the
      Exit's payload as its typed result (a plain ``T`` return is the
      DATA result — sentinels appear only where the body's control flow
      says so), and every non-terminal DOWNSTREAM node is marked
      SKIPPED-WITH-THE-RECORD (succeeds with ``{"skipped": true,
      "exit_from": <node>}`` — the record never lies about the nodes
      that didn't get to run; absorbing joins fan in the typed skip
      item, a skip is not an attempt, zero ledger rows). All rows
      terminal → the derivation reports the workflow COMPLETE.
    * THE LEDGER RECORDS IT: the exited node's own ledger terminal
      carries the exit marker on the recorded result.
    * THE TYPE SURFACE: a body whose return annotation promises
      ``Exit[DoneT]`` that returns a bare value (or a bare ``return``)
      is the checker's MUST_ERROR — the sentinel is the annotation's
      only valid return; a bare ``T`` return is the data result under a
      ``T`` annotation.

    An ``Exit`` falling off the graph (returned but never reaching a
    downstream consumer) is the residual red — E2's rule owns the
    unconsumed promise; the runner's unwrap owns the reached one."""

    __slots__ = ("payload",)

    def __init__(self, payload: T) -> None:
        self.payload = payload


@dataclass(frozen=True, slots=True)
class GateDecl:
    """A node's declared HOLD gate (T09 compiles it; T10's machinery runs
    it): the signal type(s) the body waits on, the deadline policy. The
    declaration is what the Mermaid render's ``[(hold)]`` nodes and the
    validate warning ("a workflow that waits forever on a human") read —
    the gate is COMPILE-VISIBLE, not discovered at runtime.

    THE TIMER-POLICY DISPOSITION (recorded — the alignment audit's
    finding, the don't-pay law): ``on_timeout="fail"`` is the v1 arm and
    its face is the RUNTIME raise — the wait site raises
    :class:`taskq.exceptions.SignalTimeoutError` on an abandoned hold
    and the body's ladder/except owns it. The other arms
    (``resume_with_default`` / the timer's own ``escalate``) are recorded
    LATER (the docs' timer section names the deferral + the sanctioned
    body-level composition: catch the raise, return the default or
    enqueue the escalation yourself) — this field records the AUTHOR'S
    DECLARATION for the compile-time surfaces (the Mermaid face, the
    docs), it does not introduce a second runtime vocabulary."""

    name: str
    payload_models: tuple[type[BaseModel], ...]
    timeout_s: float | None = None
    on_timeout: str = "fail"


@dataclass(slots=True)
class NodeDecl:
    """One node of the spelled graph (the compile-time shape; the runner
    lowers it into the engine's ``NodeSpec`` / ``ForkSpec`` rows)."""

    key: str
    actor: str
    queue: str
    body: BodyFn | None
    parents: tuple[str, ...] = ()
    #: The wiring's argument sources, IN SIGNATURE ORDER — ``("p", key)``
    #: for a promise (resolved to that parent's decoded result at run
    #: time), ``("d", value)`` for plain data (rides the row's payload).
    #: This is the v1 wiring rule: ARGUMENT ORDER IS SIGNATURE ORDER.
    args: tuple[tuple[str, object], ...] = ()
    #: The declared failure policy on THIS node's incoming edges (the
    #: JOIN-FAILURE-POLICY duality — T06/T07): required (fail_closed)
    #: default; the timeout arm resolves per the same policy.
    on_failure: str = "fail_closed"
    max_attempts: int = 3
    retry_kind: str | None = None
    skip: SkipPredicate | None = None
    gates: tuple[GateDecl, ...] = ()
    kind: str = "step"  # "step" | "map_source" | "gather" | "map_join"
    # THE PROGRESS SCHEMA DECLARATION (T21 decision a — the TypedGate-door
    # pattern): a pydantic model the node's ``ctx.progress`` data
    # emissions must satisfy. On a MAP SOURCE the declaration reaches the
    # children (the ``<src>.item`` keys resolve it through their source).
    progress_schema: type[BaseModel] | None = None
    # THE MAP ATTACHMENT (the source node owns its fork): the per-item
    # body + the children's placement/policy. ``map_item is not None`` IS
    # the map-source marker (the runner's fork decision reads it).
    map_item: BodyFn | None = None
    map_queue: str = "default"
    map_max_attempts: int = 3
    map_on_failure: str = "fail_closed"
    # THE MAP'S READ-SIDE AGGREGATE (T21 decision c): the declared pure
    # fn over the children's result rows — evaluated AT READ TIME, never
    # a blocking fan-in (DH8's fence).
    map_aggregate: Callable[[list[Any]], object] | None = None
    # THE LOOP ATTACHMENT (T19): the loop node's spec + the iteration
    # body + the awaited until-predicate. ``loop_spec is not None`` IS
    # the loop-node marker.
    loop_spec: object | None = None
    loop_body: BodyFn | None = None
    loop_until: Callable[[], Awaitable[bool]] | None = None
    # THE CHAIN ATTACHMENT (T20): a chain SOURCE node owns its declared
    # Chain — the chain's step rows exist only from the emit onward
    # (no NodeDecl); the runner resolves their routes from the compiled
    # workflow's chains. ``chain is not None`` IS the source marker.
    chain: object | None = None


class Promise[T_co]:
    """The wiring handle to one node's future result.

    A promise is created ONLY by the recorder (``step`` / ``map`` /
    ``gather``) — never constructed directly; the data type travels ON the
    promise (the compile's compatibility rule reads it), never through a
    runtime cast.

    THE COVARIANCE (the type-mechanism probes' load-bearing half — proven
    on pyright 1.1.414 + ty 0.0.85): the type parameter appears in no
    method signature — a promise is only ever READ through (its result
    flows downstream), never written — so the checkers infer COVARIANCE:
    ``Promise[Report]`` is a ``Promise[object]``, and a consumer that
    declares ``Promise[Config]`` REJECTS ``Promise[Report]`` (the
    contravariant-consumer rejection — the handle-flow compat face). The
    same covariance is what makes :func:`build`'s ``Promise[Never]``
    residual slot refuse every real promise.
    """

    __slots__ = ("_data_type", "_graph", "_key")

    def __init__(self, key: str, data_type: object, graph: BuildGraph) -> None:
        self._key = key
        self._data_type = data_type
        self._graph = graph

    @property
    def key(self) -> str:
        """The producing node's key (the edge's parent side)."""
        return self._key

    @property
    def graph(self) -> BuildGraph:
        """The recorder this promise belongs to (the verbs' backlink —
        public read; the gather resolves its home graph through it)."""
        return self._graph

    @property
    def data_type(self) -> object:
        """The declared data type (the wiring's typing story)."""
        return self._data_type

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<Promise {self._key}: {self._data_type!r}>"


class BuildGraph:
    """The recorder: nodes added by the wiring verbs while a build
    function runs; compiled into a :class:`CompiledWorkflow` when it
    returns."""

    def __init__(self) -> None:
        self.nodes: dict[str, NodeDecl] = {}
        self.sunk: tuple[str, ...] = ()
        self.terminal: str | None = None
        self.chains: list[object] = []
        self._auto_counter: dict[str, int] = {}
        #: The CROSS-GRAPH SMUGGLES recorded by the wiring verbs
        #: (attack-3 M3's cure): ``(consumer_key, parent_key)`` pairs
        #: wired from a promise whose home graph is NOT this recorder.
        #: The verbs RECORD (never raise mid-build — the report is
        #: one-pass); validate CONVICTS (``E7-cross-graph-promise``,
        #: error — the graph cannot run). The convicted shape: a foreign
        #: promise wired under a colliding key builds a silently WRONG
        #: edge to this app's own same-named node.
        self.smuggles: tuple[tuple[str, str], ...] = ()

    def record_smuggle(self, consumer_key: str, parent_key: str) -> None:
        """One foreign-promise wiring recorded (the validate report's
        subject; the build completes — the run is what refuses)."""
        self.smuggles = (*self.smuggles, (consumer_key, parent_key))

    def auto_key(self, base: str) -> str:
        """A stable unique key for anonymous nodes (``gather[0]``...)."""
        n = self._auto_counter.get(base, 0)
        self._auto_counter[base] = n + 1
        return base if n == 0 else f"{base}:{n}"

    def add(self, node: NodeDecl) -> None:
        if node.key in self.nodes:
            raise WorkflowBuildError(
                f"node {node.key!r} is declared twice in one workflow — a "
                "step key is the ledger's identity, never a shadow"
            )
        self.nodes[node.key] = node


_active: ContextVar[BuildGraph | None] = ContextVar("wf_build_graph", default=None)


def active_graph() -> BuildGraph:
    """The recorder of the RUNNING build function; error outside one."""
    graph = _active.get()
    if graph is None:
        raise WorkflowBuildError(
            "the wiring verbs (step/map/gather/sink/build) run only inside "
            "an @app.workflow build function — there is no active graph"
        )
    return graph


def record_under[R](graph: BuildGraph, build_fn: Callable[[], R]) -> R:
    """Run *build_fn* with *graph* as the active recorder (the compile's
    entry — the verbs resolve the recorder from this ContextVar, so two
    apps' compiles never see each other's wiring). Returns the build
    function's result (the terminal promise, when it returns one)."""
    token = _active.set(graph)
    try:
        return build_fn()
    finally:
        _active.reset(token)


def _promise_args(args: tuple[object, ...]) -> tuple[list[str], list[object]]:
    """Split wiring args into (promise keys → parent edges, data args)."""
    keys: list[str] = []
    data: list[object] = []
    for arg in args:
        if isinstance(arg, Promise):
            keys.append(arg.key)
        else:
            data.append(arg)
    return keys, data


def step[R](
    body: Callable[..., Awaitable[R]],
    *args: object,
    key: str | None = None,
    actor: str = "wf",
    queue: str = "default",
    on_failure: EdgeFailurePolicy = "fail_closed",
    max_attempts: int = 3,
    retry_kind: str | None = None,
    skip: SkipPredicate | None = None,
    gates: tuple[GateDecl, ...] = (),
    progress_schema: type[BaseModel] | None = None,
) -> Promise[R]:
    """Wire ONE node: ``p = step(fetch_body, params)`` — the promise is
    ``Promise[R]`` where ``R`` is the BODY's declared return (inferred —
    the mechanism, not a cast: a body annotated ``-> Report`` wires a
    ``Promise[Report]``).

    Promise arguments become the node's incoming edges (the fan-in when
    there are several — the join's user body IS this node's body, run
    inside the join's finalize tx with the DECODED parent results); plain
    arguments ride the node payload. ``skip=`` is the dispatch-time
    predicate (cut #4). ``progress_schema=`` is the node's DECLARED
    progress payload schema (T21 — the TypedGate-door pattern): the
    body's ``ctx.progress`` data emissions are validated against it, a
    wrong shape refused (the declaration is what makes a separate UI
    render the emission — the context-contract law).

    THE TWO FACES (be honest about the boundary): the checker reads the
    HANDLE flow (R's inference, the promise threading, build's
    ``Promise[Never]`` residuals); the DECODED-payload compat between a
    producer's model and this body's param models is the validator's face
    (``validate()``'s ``E5-incompatible-consumer``) — the bodies take
    decoded data, not handles, so the wiring call itself is erased to
    ``object`` and the checker cannot see the edge's payload."""
    graph = active_graph()
    keys, _data = _promise_args(args)
    node_key = key or graph.auto_key(getattr(body, "__name__", "step"))
    # THE NAMING RULE (cut #6's API-compile face): a DOTTED wiring key is
    # refused — the fork's derived namespace (``<key>.item``,
    # ``<key>.join``, ``<key>.iter<i>``) owns the dot; a wiring key
    # carrying one collides with the engine's own derived keys (the
    # parent-by-string-split dragon's remaining seam).
    if "." in node_key:
        raise WorkflowBuildError(
            f"node key {node_key!r} carries a dot — the dot is the engine's "
            "derived namespace (the map's <key>.item / <key>.join, the "
            "loop's <key>.iter<i>); a wiring key may not collide with it"
        )
    # THE SMUGGLE CHECK (attack-3 M3's cure): a promise arg whose home
    # recorder is NOT this graph is RECORDED — validate convicts it
    # (E7-cross-graph-promise, error) before any row is written. The
    # convicted shape: a foreign promise wired under a colliding key
    # resolves its edge against THIS app's own same-named node — a
    # silently WRONG edge, clean at build, invisible to every rule that
    # reads only keys.
    for arg in args:
        if isinstance(arg, Promise) and arg.graph is not graph:
            graph.record_smuggle(node_key, arg.key)
    # The argument sources IN ORDER (the wiring rule: argument order is
    # signature order — a promise position resolves to the parent's
    # decoded result, a data position to the value itself).
    sources: list[tuple[str, object]] = []
    for arg in args:
        if isinstance(arg, Promise):
            sources.append(("p", arg.key))
        else:
            sources.append(("d", arg))
    graph.add(
        NodeDecl(
            key=node_key,
            actor=actor,
            queue=queue,
            body=body,
            parents=tuple(keys),
            args=tuple(sources),
            on_failure=on_failure,
            max_attempts=max_attempts,
            retry_kind=retry_kind,
            skip=skip,
            gates=gates,
            progress_schema=progress_schema,
            kind="gather" if len(keys) > 1 else "step",
        )
    )
    return cast(
        "Promise[R]",
        Promise(node_key, getattr(body, "__annotations__", {}).get("return", object), graph),
    )  # Why: the promise's STATIC type is the generic R (the body's declared return — the checker's face); the runtime data_type stays the annotation's DECLARATION (string under future-annotations), read back by validate()'s compat rule. The constructor does not bind R — the cast is the seam.


def map_source[S, R](
    source: Promise[S],
    body: Callable[..., Awaitable[R]],
    *,
    key: str | None = None,
    queue: str = "default",
    on_failure: EdgeFailurePolicy = "fail_closed",
    max_attempts: int = 3,
    aggregate: Callable[[list[Any]], object] | None = None,
) -> Promise[list[R]]:
    """Wire a MAP over *source*'s items: the source's body returns the
    list; each item runs *body* as a FRESH job (per-item ledger
    identity); the map's join collects — the flat ``Promise[list[R]]``
    shape (R INFERRED from the per-item body's declared return — the
    generic threads through every child body, the mechanism's map face).
    The map attaches to the SOURCE node (its finalize forks the
    children — the engine's FORK ATOMICITY); a second map on the same
    source is refused (a node finalizes ONCE — one fork).

    ``aggregate=`` is the map's DECLARED READ-SIDE aggregate (T21
    decision c): a PURE fn over the children's decoded result rows,
    evaluated AT READ TIME by the aggregation surfaces
    (:func:`taskq.workflows._progress_read.read_map_aggregate`) — NEVER a
    blocking fan-in (DH8's fence: the join node is for DATAFLOW; a
    progress question is answered at read, unblocked, mid-flight)."""
    graph = source.graph
    source_node = graph.nodes.get(source.key)
    if source_node is None:
        raise WorkflowBuildError(
            f"map_source's promise {source.key!r} is not a node of this "
            "graph — maps attach to a wired source"
        )
    if source_node.map_item is not None:
        raise WorkflowBuildError(
            f"node {source.key!r} already carries a map — a node finalizes "
            "once (one fork); wire the second map from a distinct source"
        )
    if key is not None:
        # THE ACCEPT-AND-IGNORE CLASS'S CURE (the key param consumed or
        # refused, never ignored): the map's join key is DERIVED — the
        # engine's fork + the runner address the map join by the SOURCE's
        # own key + '.join' (the addressing is load-bearing across the
        # fork's atomic write set and the consumption door), so a custom
        # key cannot be honored without a second addressing scheme. The
        # param's presence names the author's intent to control the key —
        # a silent ignore would let them believe they had.
        raise WorkflowBuildError(
            f"map_source(key={key!r}) — the map's join key is DERIVED from "
            f"the source ({source.key!r}.join): the engine's fork addresses "
            "the map join by the source's own key; a custom join key is "
            "refused, never silently ignored"
        )
    source_node.map_item = body
    source_node.map_queue = queue
    source_node.map_max_attempts = max_attempts
    source_node.map_on_failure = on_failure
    source_node.map_aggregate = aggregate
    join_key = f"{source.key}.join"
    graph.add(
        NodeDecl(
            key=join_key,
            actor=source_node.actor,
            queue=queue,
            body=None,  # the default identity packer (the join's result IS the items' list)
            parents=(source.key,),
            on_failure=on_failure,
            kind="map_join",
        )
    )
    item_type = getattr(body, "__annotations__", {}).get("return", object)
    return cast(
        "Promise[list[R]]",
        Promise(join_key, list[item_type] if isinstance(item_type, type) else object, graph),
    )  # Why: the join promise's STATIC type is the flat Promise[list[R]] (R from the per-item body); the runtime data_type stays the wiring's DECLARATION read back by validate(). The constructor does not bind R — the cast is the seam.


def gather[R](
    promises: list[Promise[R]], *, on_failure: EdgeFailurePolicy = "fail_closed"
) -> Promise[list[R]]:
    """The ALL-upstream join: ``gather([pa, pb]) → Promise[list[R]]`` —
    the flat shape, the ELEMENT TYPE PRESERVED (a homogeneous gather over
    ``Promise[Report]``s is a ``Promise[list[Report]]`` — the generic's
    join face; a heterogeneous list upcasts to ``Promise[object]``
    elements and the join degrades to ``Promise[list[object]]``
    honestly). The join's default body packs the decoded parents; a
    consumer of the gather's promise is the join's downstream (a NORMAL
    step — the cascade, cut #1's cure)."""
    if not promises:
        raise WorkflowBuildError(
            "gather([]) is the stranded invisible join — a join over zero "
            "upstreams can never fire (declare the parents, or drop the join)"
        )
    graph = promises[0].graph
    node_key = graph.auto_key("gather")
    # THE SMUGGLE CHECK (the same conviction step() runs — attack-3 M3):
    # every joined promise must belong to the ACTIVE recorder, not merely
    # to the first promise's graph (a build running under one graph while
    # joining promises from two others is exactly the smuggle).
    from taskq.workflows.api._graph import active_graph as _active_graph

    recorder = _active_graph()
    for p in promises:
        if p.graph is not recorder:
            graph.record_smuggle(node_key, p.key)
    graph.add(
        NodeDecl(
            key=node_key,
            actor="wf",
            queue="default",
            body=None,
            parents=tuple(p.key for p in promises),
            on_failure=on_failure,
            kind="gather",
        )
    )
    return cast(
        "Promise[list[R]]", Promise(node_key, list[object], graph)
    )  # Why: the join promise's STATIC type is Promise[list[R]] (the element type preserved); the runtime data_type stays the packing declaration (list[object]) read back by validate(). The cast is the seam.


def chain_source(
    chain: Chain,
    source_body: BodyFn,
    *args: object,
    key: str | None = None,
    actor: str = "wf",
    queue: str = "default",
    max_attempts: int = 3,
) -> Promise[Any]:
    """Wire a CHAIN SOURCE node (T20): *source_body* is the paged
    generator (each ``ctx.emit_batch`` yield = ONE page's emit tx — the
    children + edges + the cursor checkpoint while the source stays
    running); *chain* is the declared :class:`taskq.workflows.chain.Chain`
    the source instantiates per record; *max_attempts* is the source's
    own ladder budget (a backpressured source's stalls ride the ladder —
    budget it for the pages a resume must re-drive). The chain's step
    rows exist only
    from the emit onward — they are NOT graph nodes; the runner resolves
    their bodies (registry, D1) and routes (the compiled chain). A chain
    step key that collides with a wired node or another chain's step is
    refused at declaration (a coding error)."""
    graph = active_graph()
    step_keys = set(chain.steps)
    clashes = (step_keys & set(graph.nodes)) | (
        step_keys & {k for c in graph.chains for k in c.steps}  # type: ignore[attr-defined]
    )
    if clashes:
        raise WorkflowBuildError(
            f"chain {chain.name!r}'s step keys {sorted(clashes)} collide "
            "with wired nodes or another chain's steps — a step key is the "
            "ledger's identity, never a shadow"
        )
    graph.chains.append(chain)
    node_key = key or chain.name
    sources: list[tuple[str, object]] = []
    for arg in args:
        sources.append(("p", arg.key) if isinstance(arg, Promise) else ("d", arg))
    graph.add(
        NodeDecl(
            key=node_key,
            actor=actor,
            queue=queue,
            body=source_body,
            parents=tuple(k for k, _ in sources if k == "p"),
            args=tuple(sources),
            max_attempts=max_attempts,
            kind="chain_source",
            chain=chain,
        )
    )
    return Promise(node_key, object, graph)


def sink(*dropped: Promise[object]) -> None:
    """Explicit fire-and-forget: the dropped promises are RECORDED in the
    compiled metadata — never silent (produced-never-consumed convicts the
    ones nobody declared)."""
    graph = active_graph()
    graph.sunk = (*graph.sunk, *(p.key for p in dropped))


def build[R](result: Promise[R], *residuals: Promise[Never]) -> Promise[R]:
    """The terminal completeness point: names the workflow's RESULT and
    accounts for every residual promise. THE STATIC LAW (the
    type-mechanism probes' terminal half): the residual slot is
    ``Promise[Never]`` — a handle whose body produces NOTHING (annotated
    ``-> NoReturn``). :class:`Promise` is covariant, so every REAL
    promise (``Promise[Report]``, ... ) is REJECTED in the slot — an
    unconsumed data handle passed to ``build`` is the CHECKER's error,
    the same shape validate's ``E2-produced-never-consumed`` convicts at
    build (the two faces, one law). At runtime the residuals are
    recorded so the produced-never-consumed rule never flags them."""
    graph = active_graph()
    graph.sunk = (*graph.sunk, *(p.key for p in residuals))
    graph.terminal = result.key
    # THE SMUGGLE CHECK (the terminal + the residuals — attack-3 M3's
    # same conviction): a foreign promise named terminal or residual is
    # recorded for validate's E7 report.
    for p in (result, *residuals):
        if p.graph is not graph:
            graph.record_smuggle(graph.terminal or "<terminal>", p.key)
    return result
