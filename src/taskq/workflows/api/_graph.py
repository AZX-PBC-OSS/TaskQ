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

from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, cast, get_args, get_origin, overload

from pydantic import BaseModel

from taskq.actor_config import validate_capacity_owner
from taskq.constants import check_priority_domain
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.workflows._types import EdgeFailurePolicy
from taskq.workflows.api._hints import body_hints, ctx_input_reads, inner_fn, own_source
from taskq.workflows.chain import Chain

if TYPE_CHECKING:
    from typing import Never

    # THE CYCLE-BREAK'S STATIC FACE: the handle's type for the
    # capacity-wiring helpers' annotations (the runtime import stays
    # function-local — the app module imports THIS module).
    from taskq.workflows.api._app import WorkflowActor

    # THE LOOP ATTACHMENT'S TYPE (the validator's door — no object-typed
    # poke crosses modules): _loop imports _graph's wiring verbs at
    # runtime, so the import rides TYPE_CHECKING here; the annotation is
    # lazy (``from __future__ import annotations``) and pyright binds it.
    from taskq.workflows.api._loop import LoopSpec

__all__ = [
    "Exit",
    "GateDecl",
    "NodeDecl",
    "Promise",
    "RateLimitDecl",
    "RouteArm",
    "WorkflowBuildError",
    "active_graph",
    "build",
    "declared_exit_return",
    "gather",
    "map_source",
    "rate_limit_names",
    "reservation_names",
    "route",
    "route_child_key",
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
#:
#: DEPRECATED FOR DISPATCH (T27 — the conviction, named honestly): this
#: face is STRINGLY — the state it reads is decoded JSON dicts, and a
#: tag-keyed predicate ("skip unless mime == video") can drop an element
#: from EVERY downstream arm and still let the run terminalize SUCCEEDED
#: having routed NOTHING (the routing-proof round's live conviction —
#: the exact silent drop the totality fence exists to refuse). For
#: dispatch-by-type the taught face is the TYPED ROUTE
#: (:func:`route` — the union's members key the arms, a non-total route
#: is a build refusal (E15), and the no-match element dies LOUDLY in
#: ``RouterNotTotal``). The predicate stays for the sibling-conditional
#: shapes it is honest for (an "only if X succeeded" dispatch) — never
#: for routing an element by what it is.
SkipPredicate = Callable[[dict[str, object]], bool]

#: THE AUTHORING UNION for the per-step admission terms (CURE 2 — the
#: vanilla ``@actor``'s own ``rate_limits=`` shapes): a primitive
#: instance, a plain name (resolved against the worker's registry — W2's
#: warning register when nothing registers it), or a keyed ref. The KEYED
#: REF is refused on the WORKFLOW surface (the wiring verbs' loud door):
#: its concrete bucket materializes per payload, and the fork stamps
#: NAMES onto the child rows — a declaration that cannot name what it
#: stamps is the admitted-never-limited lie wearing the signature.
RateLimitDecl = str | KeyedRateLimitRef | TokenBucket | SlidingWindow


def rate_limit_names(
    decls: tuple[RateLimitDecl, ...], *, what: str = "rate_limits"
) -> tuple[str, ...]:
    """The DECLARATION's concrete bucket NAMES — the fork's stamp source:
    an instance contributes its ``.name`` (the registry's own
    normalization, ``acquire_for_actor``'s), a plain name passes through.
    A keyed ref is the REFUSED shape (the union's own docstring) — the
    wiring verbs call this helper, so the refusal lands at the wiring
    site, before any row exists."""
    names: list[str] = []
    for entry in decls:
        if isinstance(entry, KeyedRateLimitRef):
            raise WorkflowBuildError(
                f"{what}: a KeyedRateLimitRef names no concrete bucket — "
                "its bucket materializes PER PAYLOAD, and a workflow row's "
                "admission terms are stamped as NAMES at the fork. Declare "
                "the static buckets (rate_limits=[TokenBucket(...)]) or "
                "route the keyed shape through a vanilla @actor."
            )
        names.append(entry.name if isinstance(entry, TokenBucket | SlidingWindow) else entry)
    return tuple(names)


def reservation_names(decls: tuple[object, ...], *, what: str = "reservations") -> tuple[str, ...]:
    """The CAPACITY DECLARATION's concrete reservation NAMES (H-A10's
    loud door — ``rate_limit_names``' own pattern): a
    :class:`ConcurrencyReservation` instance contributes its ``.name``, a
    plain name (``str`` — the vanilla face's registry-name shape) passes
    through, and ANYTHING ELSE — a :class:`KeyedReservationRef` above
    all — is the REFUSED shape: its concrete bucket materializes per
    payload, and a workflow row's strict cap is stamped as NAMES at the
    fork — a declaration that cannot name what it stamps would be the
    silently-dropped cap (the admitted-never-limited lie). The wiring
    verbs call this helper, so the refusal lands at the wiring site,
    before any row exists."""
    names: list[str] = []
    for entry in decls:
        if isinstance(entry, ConcurrencyReservation):
            names.append(entry.name)
        elif isinstance(entry, str):
            names.append(entry)
        else:
            raise WorkflowBuildError(
                f"{what}: a {type(entry).__name__} names no concrete "
                "reservation — its bucket materializes PER PAYLOAD, and a "
                "workflow row's strict cap is stamped as NAMES at the fork; "
                "the keyed shape would be silently dropped here (the "
                "admitted-never-limited lie). Declare the static "
                "reservations (ConcurrencyReservation instances or their "
                "registered names) or route the keyed shape through a "
                "vanilla @actor."
            )
    return tuple(names)


#: THE HANDLE'S TERM FIELDS (the capacity surface the ``@app.actor``
#: handle carries — H-A6's removal left three; C15's owner opt-in is
#: the fourth).
_HANDLE_TERM_FIELDS: Final = ("max_concurrent", "reservations", "result_ttl", "capacity_owner")


def _capped_handle_of(body: object) -> WorkflowActor | None:
    """The wired body's ``WorkflowActor`` handle WHEN it carries capacity
    terms (H-A4's subject — "a capped body"); ``None`` when the body is
    bare or the handle declares no terms.

    THE CODE-ONLY HANDLE (C15's review cure): an owner declaration IS a
    capacity declaration — a handle carrying ``capacity_owner="code"``
    with no caps is RETURNED (wired owner-only at the honoring sites,
    refused NAMED at the dropping sites), never the zero-signal no-op
    the pre-cure predicate was."""
    from taskq.workflows.api._app import WorkflowActor  # the cycle-break (inner_fn's own pattern)

    if not isinstance(body, WorkflowActor):
        return None
    if (
        body.max_concurrent is not None
        or body.reservations
        or body.result_ttl is not None
        or body.capacity_owner != "operator"
    ):
        return body
    return None


def _same_reservation_names(a: tuple[object, ...], b: tuple[object, ...]) -> bool:
    """Whether two reservation DECLARATIONS name the same buckets (the
    collect's name-keyed identity — never instance identity, H-A3's own
    law: equal configs on different instances are ONE bucket)."""
    names_a = reservation_names(tuple(a), what="reservations")
    names_b = reservation_names(tuple(b), what="reservations")
    return names_a == names_b


def _wire_handle_terms_into_node(node: NodeDecl, handle: WorkflowActor, *, what: str) -> None:
    """THE HANDLE'S TERMS FLOW at the map/route attachment (the same law
    ``step()`` keeps): a capped handle wired where the placement KEEPS
    the node's actor fills the node's UNSET term slots — the children
    share the node's cohort and ``actor_config`` is keyed by actor (one
    capacity per actor, the estate's own law). A term the node ALREADY
    declares at a DIFFERENT value is the capacity conflict, named at the
    wiring — never a silent drop, never a silent overwrite.

    THE OWNER AXIS (C15's review cure): the handle's ``capacity_owner``
    is a carried VALUE (every handle has one — the default "operator" is
    the value, not an absence), so it flows UNCONDITIONALLY: the node's
    unset face ("operator") adopts the handle's; a node already carrying
    a DIFFERENT owner (a code-owned step handle met by a default-owner
    map/route body on one cohort) is the named conflict — one cohort,
    one owner. An OVERRIDING route arm never reaches here (its
    placement drops the terms — ``_uncovered_handle_terms`` demands the
    arm's own re-declaration, the per-arm thread)."""
    conflicts: list[str] = []
    if node.capacity_owner != handle.capacity_owner:
        # The adopt: the unset node ("operator") takes the handle's face.
        # The conflict: the node already carries a DIFFERENT owner.
        if node.capacity_owner == "operator":
            node.capacity_owner = handle.capacity_owner
        else:
            conflicts.append(
                f"capacity_owner: {node.capacity_owner!r} vs the handle's {handle.capacity_owner!r}"
            )
    if handle.max_concurrent is not None:
        if node.max_concurrent is None:
            node.max_concurrent = handle.max_concurrent
        elif node.max_concurrent != handle.max_concurrent:
            conflicts.append(
                f"max_concurrent: {node.max_concurrent!r} vs the handle's {handle.max_concurrent!r}"
            )
    if handle.reservations:
        if not node.reservations:
            node.reservations = tuple(handle.reservations)
        elif not _same_reservation_names(node.reservations, tuple(handle.reservations)):
            # THE MESSAGE SPEAKS THE NAME LAW (the dump finding): the
            # reservation instances have no __repr__ — rendering them
            # raw gives the operator two pointers and no bucket. The
            # conflict's terms are the NAMES (the same identity the
            # comparison itself keys on).
            conflicts.append(
                f"reservations: {reservation_names(node.reservations, what='reservations')!r} "
                "vs the handle's "
                f"{reservation_names(tuple(handle.reservations), what='reservations')!r}"
            )
    if handle.result_ttl is not None:
        if node.result_ttl is None:
            node.result_ttl = handle.result_ttl
        elif node.result_ttl != handle.result_ttl:
            conflicts.append(
                f"result_ttl: {node.result_ttl!r} vs the handle's {handle.result_ttl!r}"
            )
    if conflicts:
        raise WorkflowBuildError(
            f"{what}: the wired handle's capacity terms conflict with the "
            f"node's own declaration ({'; '.join(conflicts)}) — actor_config "
            "is keyed by actor (one capacity per actor, the estate's own "
            "law): give the differing placement its own actor name."
        )


def _uncovered_handle_terms(arm: RouteArm[object], handle: WorkflowActor) -> list[str]:
    """The capped handle's terms the arm's OWN declaration does not
    re-declare (H-A4's refusal list — the terms the placement override
    would drop)."""
    uncovered: list[str] = []
    if handle.max_concurrent is not None and arm.max_concurrent is None:
        uncovered.append(f"max_concurrent={handle.max_concurrent!r}")
    if handle.reservations and not arm.reservations:
        uncovered.append("reservations")
    if handle.result_ttl is not None and arm.result_ttl is None:
        uncovered.append(f"result_ttl={handle.result_ttl!r}")
    # THE OWNER AXIS (C15's review cure): the override drops the handle's
    # owner exactly like the caps — the arm's own declaration carries it
    # (the per-arm thread; the projection reads the arm's field), and a
    # missing/differing one is the named refusal, never the silent drop.
    if handle.capacity_owner != "operator" and arm.capacity_owner != handle.capacity_owner:
        uncovered.append(f"capacity_owner={handle.capacity_owner!r}")
    return uncovered


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
class RouteArm[R]:
    """ONE typed route arm (T27): the arm's body + the PER-ARM placement
    override (R4 — ``processA`` on the gpu queue, ``processB`` on the io
    queue) + the PER-ARM admission terms (the consumer-face lane's CURE
    2 — ``rate_limits=[TokenBucket(...)]``; "5 concurrent" is not
    "50/min" under varying latency). The generic ``R`` is the arm body's
    declared return — the route's join promise solves its
    ``Promise[list[R]]`` to the UNION of the arms' returns (the typed
    sum). A bare ``Callable`` arm (no placement) is accepted at the verb
    and normalized to ``RouteArm(body=fn)`` — the placement defaults to
    the route's own (``queue=``) and the source's actor."""

    body: Callable[..., Awaitable[R]]
    #: ``None`` = the source node's actor (the route carries no actor of
    #: its own — the arms override DOWN from the source, never up).
    actor: str | None = None
    #: ``None`` = the route's declared queue (the verb's ``queue=``).
    queue: str | None = None
    #: The arm's children's ADMISSION TERMS (CURE 2): the fork stamps
    #: the buckets' NAMES onto the child rows' metadata; the claim path
    #: acquires them through the SAME registry + the SAME denial path
    #: the queue-concurrency fence rides (one mechanism, the authoring
    #: face added). ``()`` = the arm declares no bucket.
    rate_limits: tuple[RateLimitDecl, ...] = ()
    #: THE ARM'S OWN CAPACITY DECLARATION (H-A4's passthrough — the
    #: never-a-silent-fail-open law): a capped handle wired under an
    #: actor override must have its terms re-declared HERE — the arm's
    #: cohort projects the arm's own terms, never the handle's. The
    #: fields are the node surface's own (``max_concurrent`` /
    #: ``reservations`` / ``result_ttl``; ``max_pending`` is off the wf
    #: surface, H-A6).
    max_concurrent: int | None = None
    reservations: tuple[object, ...] = ()
    result_ttl: float | None = None
    #: THE ARM'S PRIORITY OVERRIDE (the wf-priority cure — the enqueue
    #: face the vanilla rows always had): the arm's children's rows land
    #: at this value in the ``jobs.priority`` column — the claim SQL's
    #: ``priority DESC`` then serves them ahead of the default-0 backlog.
    #: ``None`` = the arm does not override: the SOURCE node's resolution
    #: applies (the arm overrides DOWN from the source, never up — the
    #: placement law's own arithmetic). NOT an actor_config axis (the
    #: column is the row's own, never the cohort's), so the arm's
    #: priority needs no one-cap-per-actor conflict law.
    priority: int | None = None
    #: THE ARM'S OWN OWNER DECLARATION (C15's review cure — the priority
    #: surface's per-arm shape): an overriding arm's body handle carrying
    #: ``capacity_owner="code"`` must be re-declared HERE (the handle's
    #: owner would be dropped by the placement override — the refusal
    #: names it); the projection reads THIS field onto the arm's
    #: effective cohort. PER-ARM, never folded into the source node: the
    #: other arms and the join must not inherit it (the join stays at
    #: the source's terms). Declare it only on an OVERRIDING arm — a
    #: no-override arm's cohort inherits the handle's flow, and a
    #: redundant declaration here enters the projection's one-cap
    #: comparison.
    capacity_owner: str = "operator"


@dataclass(frozen=True, slots=True)
class GateDecl:
    """A node's declared HOLD gate (T09 compiles it; T10's machinery runs
    it): the signal type(s) the body waits on, the deadline policy. The
    declaration is what the Mermaid render's ``[(hold)]`` nodes and the
    validate warning ("a workflow that waits forever on a human") read —
    the gate is COMPILE-VISIBLE, not discovered at runtime.

    THE TIMER-POLICY DISPOSITION (recorded — the alignment audit's
    finding, the don't-pay law): ``on_timeout="fail"`` is the v1 arm
    and its face is the CLOSED UNION's ``Expired`` MEMBER (T26's
    amendment — the expiry is a VALUE, not an exception): the wait site
    RETURNS ``Expired`` and the body MATCHES the fail-close arm (the
    checker forces it); a body that WANTS the failure raises
    :class:`taskq.exceptions.SignalTimeoutError` ITSELF off the member.
    The other arms (``resume_with_default`` / the timer's own
    ``escalate``) are recorded LATER (the docs' timer section names the
    deferral + the sanctioned body-level composition: match the member,
    return the default or enqueue the escalation yourself) — this field
    records the AUTHOR'S DECLARATION for the compile-time surfaces (the
    Mermaid face, the docs), it does not introduce a second runtime
    vocabulary.

    THE DOUBLE-TIMEOUT PRECEDENCE (C8's law, the timeout is DECLARED in
    TWO places, each with its own face — the split the teardown round
    documented and W4 enforces where statically readable):

    * ``GateDecl.timeout_s`` feeds the COMPILE SURFACES ONLY: the
      Mermaid render's hold-node deadline, W1's eternal-wait warning.
      It NEVER arms the runtime.
    * the WAIT SITE's ``ctx.wait_signal(..., timeout_s=…)`` arms the
      RUNTIME expiry (the hold row's ``expires_at``). When the two
      disagree, THE WAIT'S VALUE WINS on the rows (pinned); keep them
      equal on purpose."""

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
    #: THE NODE'S OWN ADMISSION TERMS (CURE 2): the claim path acquires
    #: these buckets before the body runs — the vanilla ``@actor``'s
    #: ``rate_limits=`` face on the workflow surface, the SAME registry,
    #: the SAME denial-snooze path. The fork's children carry the MAP's
    #: (``map_rate_limits``) / the ARM's (``RouteArm.rate_limits``).
    rate_limits: tuple[RateLimitDecl, ...] = ()
    #: THE NODE'S CAPACITY TERMS (the vanilla ``@actor``'s own fields —
    #: ``max_concurrent`` is the per-round admission damper, NOT a hard
    #: fleet-wide cap; ``reservations`` are the STRICT slots
    #: (ConcurrencyReservation) — the dispatch-admission gate the vanilla
    #: path enforces via reservation_slots; ``result_ttl`` seeds the
    #: cohort's result expiry). The projection carries them into the
    #: cohort's ``ActorConfig`` (seed-only, operator-owned thereafter);
    #: the fork's children inherit the source's terms unless the arm
    #: overrides. NB: ``max_pending`` is deliberately NOT on this
    #: surface (H-A6 — the engine's inserts bypass the enqueue admission
    #: and the claim SQL cannot bound a backlog without wedging it, so
    #: the knob was dead; removal is the honest scope).
    max_concurrent: int | None = None
    reservations: tuple[object, ...] = ()
    result_ttl: float | None = None
    #: THE NODE'S DECLARED PRIORITY (the wf-priority cure — the enqueue
    #: face the vanilla rows always had): the node's rows land at this
    #: value in the ``jobs.priority`` column; the dispatch claim's
    #: ``priority DESC`` (the same ORDER BY the vanilla rows obey) then
    #: serves them ahead of the default-0 backlog. The RESOLUTION LAW per
    #: row, first non-None wins: the explicit per-call
    #: (``create_flow``/``run`` ``priority=``) > this declaration > the
    #: actor handle's (folded in at the wiring when this is None) > the
    #: column default. The map's children + the route's arms resolve
    #: through the SAME law (the arms override DOWN from the source).
    #: ``None`` = nothing declared — the column default applies, the
    #: pre-cure byte-identical shape. NOT an actor_config axis (the
    #: column is the row's own, never the cohort's — the vanilla
    #: ``ActorRef.priority`` rides the ref and the enqueue, never the
    #: config row either), so two steps of one actor may declare
    #: different priorities with no conflict law.
    priority: int | None = None
    #: THE CAPACITY OWNER (C15's opt-in — the handle's declaration, read
    #: at wiring): "operator" (the default) keeps the estate's seed-only
    #: law; "code" makes the config sync rewrite the cohort's
    #: max_concurrent/result_ttl from the projected literal on every
    #: boot. The projection carries it into the cohort's ``ActorConfig``.
    capacity_owner: str = "operator"
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
    #: THE FAN-IN BOUND'S ESCAPE, EXPOSED AT THE WIRING (the docshelf
    #: adoption team's F6): the map/route join's ``child_driven`` shape —
    #: past :data:`MAX_FAN_IN_PER_JOIN` the fork's join counts its
    #: terminal children FROM THE EDGE LEDGER at fire time (the
    #: counter-as-cache never trusted; ``metadata.join_shape`` records
    #: the choice). ``False`` (the default) keeps the bounded declared-
    #: edge shape the bound exists to protect.
    map_child_driven: bool = False
    #: The map's CHILDREN's admission terms (CURE 2 — the plain map's
    #: face; the typed route's arms carry their own on the RouteArm).
    map_rate_limits: tuple[RateLimitDecl, ...] = ()
    # THE TYPED ROUTE ATTACHMENT (T27 — the type-tagged Route's machinery
    # extended to the graph level): the arms dict keyed by the source's
    # union members' TYPE TAGS (``module.qualname`` — the chain's
    # ``type_tag``), each arm the body + its per-arm placement override.
    # ``map_arms is not None`` IS the route-source marker (the runner's
    # fork decision reads it; it and ``map_item`` are mutually exclusive —
    # a node finalizes once, one fork). The route IS a map attachment: the
    # same join/carrier fields below carry the fork spec. The arm's R is
    # ERASED to ``object`` at the attachment (the arms' returns are the
    # join's union — the SUM rides the promise's static type, never the
    # decl's field).
    map_arms: dict[str, RouteArm[object]] | None = None
    # THE MAP'S READ-SIDE AGGREGATE (T21 decision c): the declared pure
    # fn over the children's result rows — evaluated AT READ TIME, never
    # a blocking fan-in (DH8's fence).
    map_aggregate: Callable[[list[Any]], object] | None = None
    # THE LOOP ATTACHMENT (T19): the loop node's spec + the iteration
    # body + the awaited until-predicate. ``loop_spec is not None`` IS
    # the loop-node marker. TYPED (the lazy-Any correction): the spec is
    # the loop module's own declared dataclass — the validator and the
    # runner consume the FIELD's type, never an object-shaped cast.
    loop_spec: LoopSpec | None = None
    loop_body: BodyFn | None = None
    loop_until: Callable[[], Awaitable[bool]] | None = None
    # THE CHAIN ATTACHMENT (T20): a chain SOURCE node owns its declared
    # Chain — the chain's step rows exist only from the emit onward
    # (no NodeDecl); the runner resolves their routes from the compiled
    # workflow's chains. ``chain is not None`` IS the source marker.
    chain: object | None = None
    # THE CROSS-RUN CACHE OPT-IN (T25): the node's result may serve a
    # LATER run over the same body + the same input (the TEMPORAL dedup
    # — the arbiter owns the CONCURRENT one). Default OFF: the cache is
    # a decision, never a surprise. ``cache_ttl`` is the in-DB expiry's
    # seconds (``None`` = the cache module's 24h default); the wiring
    # face refuses a TTL without the opt-in and a non-positive TTL.
    cache: bool = False
    cache_ttl: float | None = None


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
        #: THE WORKFLOWDEF'S OWN PLACEMENT (C6): the ``@app.workflow``
        #: decorator's declared (actor, queue) — set at compile BEFORE
        #: the build fn runs, so the LOOP's fallback placement (the
        #: wiring verbs read it mid-build) resolves the same cohort the
        #: definition's own rows (the escalation consumer rows) stamp.
        #: ``None`` = no placement declared — every fallback keeps its
        #: current default.
        self.declared_actor: str | None = None
        self.declared_queue: str | None = None

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


def declared_exit_return(body: object) -> bool:
    """Whether the body's DECLARED return is (or contains) the
    :class:`Exit` sentinel — the static read the cache-opt-in's refusal
    (below) shares with the validator's E19 re-proof. The resolved
    hints are walked THROUGH unions and list elements (a
    ``Report | Exit[Report]`` body is as exit-terminal as a bare
    ``Exit[Report]``); an UNRESOLVABLE annotation resolves to no hint —
    the skip (the zero-false-positive doctrine: a guess is never
    convicted)."""
    from types import UnionType
    from typing import Union

    returned = body_hints(cast("Callable[..., object]", body)).get("return")
    stack: list[object] = [returned]
    while stack:
        ann = stack.pop()
        if ann is None:
            continue
        if ann is Exit or get_origin(ann) is Exit:
            return True
        origin = get_origin(ann)
        if origin is Union or origin is UnionType or origin is list:
            stack.extend(get_args(ann))
    return False


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
    rate_limits: tuple[RateLimitDecl, ...] | list[RateLimitDecl] = (),
    max_concurrent: int | None = None,
    reservations: tuple[object, ...] | list[object] = (),
    result_ttl: float | None = None,
    priority: int | None = None,
    cache: bool = False,
    cache_ttl: float | None = None,
) -> Promise[R]:
    """Wire ONE node: ``p = step(fetch_body, params)`` — the promise is
    ``Promise[R]`` where ``R`` is the BODY's declared return (inferred —
    the mechanism, not a cast: a PLAIN-FUNCTION body annotated
    ``-> Report`` wires a ``Promise[Report]``). THE BOUNDARY (the honest
    edge of the promise): an ``@app.actor``-wrapped body does NOT carry
    the inference — the actor handle's ``__call__`` returns ``object``
    (the checker cannot see through the wrapper), so wiring one degrades
    the promise to ``Promise[object]`` and the downstream decoding rides
    the runtime validator; ``cast`` the promise at the wiring site when
    the checker must carry the declared return.

    Promise arguments become the node's incoming edges (the fan-in when
    there are several — the join's user body IS this node's body, run
    inside the join's finalize tx with the DECODED parent results); plain
    arguments ride the node payload. ``skip=`` is the dispatch-time
    predicate (cut #4). ``progress_schema=`` is the node's DECLARED
    progress payload schema (T21 — the TypedGate-door pattern): the
    body's ``ctx.progress`` data emissions are validated against it, a
    wrong shape refused (the declaration is what makes a separate UI
    render the emission — the context-contract law).

    ``rate_limits=`` is the node's ADMISSION TERMS (the consumer-face
    lane's CURE 2 — the vanilla ``@actor``'s own face): the claim path
    acquires the buckets through the SAME registry the vanilla pre-flight
    reads (a denial snoozes the row budget-free — the queue-concurrency
    fence's own path); the fork stamps the names onto the child rows so
    the row carries its own terms.

    THE CROSS-RUN CACHE OPT-IN (T25): ``cache=True`` lets this step's
    result serve a LATER run over the same body + the same input — the
    TEMPORAL dedup (the run-key arbiter owns the CONCURRENT one; two
    concurrent misses both run their bodies). The cache stores on
    TERMINAL-SUCCEEDED only (a failure never squats the address),
    delivers a hit through the ordinary finalize (the downstream
    decodes the same typed shape), and stamps the node row's metadata
    with the receipt (the address + the producing run's id).
    ``cache_ttl=`` is the in-DB expiry's seconds (default 24h).
    THE DETERMINISM HAZARD, stated plainly: the cache assumes the body
    is DETERMINISTIC for the same input — a non-deterministic body with
    ``cache=True`` is the stale truth BY CHOICE (the address promises
    "same body, same input"; it cannot promise the world stood still).
    THE ADDRESS'S SCOPE (the which-input-counts law): the address
    covers the body's OWN source + the WIRED args — a changed HELPER is
    invisible to the address (bump the body's own source when the
    closure's truth changes — the documented contract), and the FLOW
    input (``ctx.input``) NEVER enters it. That scope is why the
    refusals below exist: a body reading ``ctx.input`` is refused (the
    flow input differs per run — the cross-tenant leak; a SOURCE node's
    run input is its only input, so wire the spec IN as data or lift it
    through an uncached upstream step's wired result), an
    Exit-returning body is refused (the sentinel never reaches the
    store), a typed-route source is refused at the route attachment
    (the hit path cannot rebuild the fork), and an unverifiable body
    source is refused (the cache cannot hash what it cannot read —
    without the refusal the step is silently uncached).

    THE TWO FACES (be honest about the boundary): the checker reads the
    HANDLE flow (R's inference, the promise threading, build's
    ``Promise[Never]`` residuals — and the ELEMENT law on the handle:
    ``Promise[list[Word]]`` is not ``Promise[list[str]]``, the covariant
    promise + the invariant list). The DECODED-payload compat between a
    producer's declared data type and this body's param types is the
    validator's face (``validate()``'s ``E5-incompatible-consumer`` —
    since the engine-faces cure, at the ELEMENT level too: a
    ``list[Word]`` producer wired into a ``list[str]`` consumer refuses
    at build) — the bodies take decoded data, not handles, so the wiring
    call itself is erased to ``object`` and the checker cannot see the
    edge's payload. A fallback overload cannot rescue a seam it erases:
    the wire-type walk was probed (the corpus's wiring-site probe) and
    the permissive ``*args: object`` overload always matched last — the
    validate-time probe is the mechanism the reads picked."""
    graph = active_graph()
    keys, _data = _promise_args(args)
    node_key = key or graph.auto_key(getattr(body, "__name__", "step"))
    # THE HANDLE'S TERMS FLOW (the capacity cure's declaration half): an
    # ``@app.actor(max_concurrent=…, reservations=…)``-wrapped body wired
    # here carries ITS terms as the node's ADMISSION defaults — the
    # explicit step params override, never the reverse. Placement
    # (``actor=``/``queue=``) stays the step's own decision (the handle's
    # name is NOT the node's actor: the wiring's placement law is
    # unchanged).
    from taskq.workflows.api._app import WorkflowActor  # the cycle-break (inner_fn's own pattern)

    # THE CAPACITY OWNER (C15): "operator" unless the wired body is a
    # code-owned handle (below) — the per-actor declaration, defaulted.
    capacity_owner = "operator"
    handle = body if isinstance(body, WorkflowActor) else None
    if handle is not None:
        if max_concurrent is None:
            max_concurrent = getattr(handle, "max_concurrent", None)
        if not reservations:
            handle_res = getattr(handle, "reservations", ())
            reservations = tuple(handle_res) if handle_res else ()
        if result_ttl is None:
            result_ttl = getattr(handle, "result_ttl", None)
        if priority is None:
            priority = getattr(handle, "priority", None)
        # THE CAPACITY OWNER (C15): the handle's declaration is the
        # node's — the projection carries it into the cohort's
        # ActorConfig, where the config sync's code-owned upsert reads
        # it. The wiring surface has no override kwarg of its own: the
        # declaration is PER-ACTOR (the handle's), never per-node.
        capacity_owner = handle.capacity_owner
    # THE PRIORITY DOMAIN (the vanilla law at every layer that accepts a
    # priority — constants.py's own statement of it): the RESOLVED
    # declaration (the step's own or the handle's folded default) is
    # checked ONCE here — a value the smallint column cannot store is
    # refused at the wiring, before any row exists. ``None`` rides (it
    # means "nothing declared — the column default").
    if priority is not None:
        check_priority_domain(priority, what=f"step {node_key!r}'s priority")
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
    # THE KEY CHARSET LAW (H-C8, the dot precedent's own shape): the key
    # is a URL PATH SEGMENT (the admin's affordances address nodes by
    # key — /nodes/<key>) and a Mermaid label; these characters 404 the
    # affordances or kill the graph render, so they never compile:
    # /, ?, #, % (the URL's own syntax), both quotes (the Mermaid label's
    # delimiter), and control characters (incl. newlines — the label's
    # injected-structure door). Edge whitespace refuses too (a key the
    # operator cannot address exactly).
    _forbidden = next(
        (ch for ch in node_key if ch in "/?#%\"'" or ord(ch) < 0x20 or ch == "\x7f"),
        None,
    )
    if _forbidden is not None:
        raise WorkflowBuildError(
            f"node key {node_key!r} carries {_forbidden!r} "
            f"(U+{ord(_forbidden):04X}) — the key is a URL path segment (the "
            "admin affordances address nodes by key) and a Mermaid label: "
            "/, ?, #, %, quotes and control characters 404 the affordances "
            "or break the graph render; rename the node (letters, digits, "
            "'-', '_', and — within the derived namespace's law — no dot)"
        )
    if node_key != node_key.strip():
        raise WorkflowBuildError(
            f"node key {node_key!r} carries leading/trailing whitespace — the "
            "key is a URL path segment (the admin affordances address nodes "
            "by key): an unaddressable key 404s its own panel; rename the "
            "node without the edge whitespace"
        )
    # THE CACHE OPT-IN'S OWN REFUSALS (T25): the mechanically-impossible
    # at the wiring site (the dotted-key precedent). A TTL on a
    # NON-cache is a silent no-op parameter (the surprise family); a
    # non-positive TTL is an always-expired cache (the lie); a cache on
    # the MULTI-PARENT fan-in is outside the v1 surface (the plain step
    # is the cached shape — the gather's join face is the engine's own
    # machinery).
    if cache_ttl is not None and not cache:
        raise WorkflowBuildError(
            f"node {node_key!r} declares cache_ttl without cache=True — "
            "a TTL on a cache that does not exist is a silent no-op "
            "(the cache is a decision, never a surprise): pass cache=True"
        )
    if cache and cache_ttl is not None and cache_ttl <= 0:
        raise WorkflowBuildError(
            f"node {node_key!r} declares cache_ttl={cache_ttl} — a "
            "non-positive TTL is an always-expired cache (the lie): a "
            "cached result must outlive at least one freshness window"
        )
    if cache and len(keys) > 1:
        raise WorkflowBuildError(
            f"node {node_key!r} declares cache=True on a gather (the "
            "multi-parent fan-in) — the v1 cache surface is the plain "
            "step: the gather's join face is the engine's own machinery, "
            "not a cacheable body's"
        )
    # THE ADDRESS-SCOPE REFUSALS (the cache-cures round): two more
    # mechanically-impossible shapes, both LIES the address cannot see —
    #
    # * the body reads ``ctx.input`` (CURE 3 — THE CROSS-TENANT LEAK):
    #   the address covers the WIRED args only; the FLOW input never
    #   enters it. A cached answer produced under one flow input serves
    #   a LATER run whose flow input differs — tenant A's answer
    #   delivered to tenant B, terminal-success, audits clean (the live
    #   repro: .measurements/cache-cures/ctx_input_leak_repro.py). The
    #   typed refusal names the fix: lift the flow input into the
    #   dataflow with an UNcached upstream step and wire its result as a
    #   PARAM — the param enters the address, the value differs per
    #   input, the cache caches per value.
    # * the body returns the Exit sentinel (CURE 6b): an Exit
    #   terminalizes through the exit finalize, which the success-only
    #   store never sees — cache=True would silently no-op (every run
    #   re-executes the body while the operator believes it caches).
    if cache and bool(ctx_input_reads(body)):
        raise WorkflowBuildError(
            f"node {node_key!r} declares cache=True but its body reads "
            "ctx.input — the flow input never enters the cache address "
            "(the wired args do): the cached answer produced under one "
            "flow input would serve a LATER run whose input differs (the "
            "cross-tenant leak — A's answer delivered to B, "
            "terminal-success, audits clean). Lift the flow input into "
            "the dataflow with an UNcached upstream step and wire its "
            "result as this body's PARAM (the param enters the address), "
            "or drop cache=True"
        )
    if cache and declared_exit_return(body):
        raise WorkflowBuildError(
            f"node {node_key!r} declares cache=True but its body "
            "declares the Exit sentinel as (or inside) its return — an "
            "Exit terminalizes through the exit finalize, which the "
            "success-only store never sees: the cache would silently "
            "no-op (every run re-executes the body while the operator "
            "believes it caches). Return the DATA result (the declared "
            "R) and let the cache hold it, or drop cache=True"
        )
    # THE UNVERIFIABLE-SOURCE PRECONDITION (the cache-cures round's
    # AMENDMENT B — the import-probe's #3): the address requires the
    # §22.1 code-version canon (module + qualname + the body's OWN
    # source, through the same own_source read the stamper and the
    # validator share) — an UNVERIFIABLE source (a builtin, a partial,
    # an exec'd fragment whose slice cannot be proven its own) made the
    # step SILENTLY uncached: the wiring accepted cache=True, every run
    # re-executed the body, and the ONLY trace was the
    # ``step-cache-unaddressable`` log line (the live repro:
    # .measurements/cache-cures/unverifiable_source_silent_repro.py).
    # The operator must never learn the face from a missing hit — the
    # precondition refuses here, by name. The read is DETERMINISTIC (the
    # runner's own hash reads the same source through the same seam), so
    # the refusal carries no false-positive risk: a source the wiring
    # cannot read is a source the claim path will never hash.
    if cache and own_source(body) is None:
        raise WorkflowBuildError(
            f"node {node_key!r} declares cache=True but its body's "
            "source is not verifiable (a builtin, a partial, or an "
            "exec'd fragment — the cache cannot hash what it cannot "
            "read): the address requires the §22.1 code-version canon "
            "(module + qualname + the body's own source), and an "
            "unverifiable source made this step SILENTLY uncached — "
            "every run re-executes the body while the cache machinery "
            "looks armed. Define the body as a module-level def its "
            "source can be read from, or drop cache=True"
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
    # THE LOUD DOOR at the wiring site (the keyed-ref refusal — before
    # any row exists); the DECLS ride the node (the boot's collect pass
    # registers the instances, the fork stamps the names).
    rate_limit_names(tuple(rate_limits), what=f"step {node_key!r}'s rate_limits")
    # THE CAPACITY DOOR (H-A10 — the same keyed-ref refusal on the
    # strict-cap face): a KeyedReservationRef in reservations= is the
    # refused shape — never a silently-dropped cap.
    reservation_names(tuple(reservations), what=f"step {node_key!r}'s reservations")
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
            rate_limits=tuple(rate_limits),
            max_concurrent=max_concurrent,
            reservations=tuple(reservations),
            result_ttl=result_ttl,
            priority=priority,
            capacity_owner=capacity_owner,
            cache=cache,
            cache_ttl=cache_ttl,
            kind="gather" if len(keys) > 1 else "step",
        )
    )
    return cast(
        "Promise[R]",
        Promise(node_key, getattr(body, "__annotations__", {}).get("return", object), graph),
    )  # Why: the promise's STATIC type is the generic R (the body's declared return — the checker's face); the runtime data_type stays the annotation's DECLARATION (string under future-annotations), read back by validate()'s compat rule. The constructor does not bind R — the cast is the seam.


@overload
def map_source[S, R](
    source: Promise[S],
    body: Callable[..., Awaitable[R]],
    *,
    queue: str = "default",
    on_failure: EdgeFailurePolicy = "fail_closed",
    max_attempts: int = 3,
    aggregate: Callable[[list[Any]], object] | None = None,
    rate_limits: tuple[RateLimitDecl, ...] | list[RateLimitDecl] = (),
    child_driven: bool = False,
) -> Promise[list[R]]: ...


@overload
def map_source[S, R](
    source: Promise[S],
    body: Mapping[type, RouteArm[R] | Callable[..., Awaitable[R]]],
    *,
    queue: str = "default",
    on_failure: EdgeFailurePolicy = "fail_closed",
    max_attempts: int = 3,
    aggregate: Callable[[list[Any]], object] | None = None,
    rate_limits: tuple[RateLimitDecl, ...] | list[RateLimitDecl] = (),
    child_driven: bool = False,
) -> Promise[list[R]]: ...


def map_source[S, R](
    source: Promise[S],
    body: Callable[..., Awaitable[R]] | Mapping[type, RouteArm[R] | Callable[..., Awaitable[R]]],
    *,
    queue: str = "default",
    on_failure: EdgeFailurePolicy = "fail_closed",
    max_attempts: int = 3,
    aggregate: Callable[[list[Any]], object] | None = None,
    rate_limits: tuple[RateLimitDecl, ...] | list[RateLimitDecl] = (),
    child_driven: bool = False,
) -> Promise[list[R]]:
    """Wire a MAP over *source*'s items: the source's body returns the
    list; each item runs *body* as a FRESH job (per-item ledger
    identity); the map's join collects — the flat ``Promise[list[R]]``
    shape (R INFERRED from the per-item body's declared return — the
    generic threads through every child body, the mechanism's map face).
    The map attaches to the SOURCE node (its finalize forks the
    children — the engine's FORK ATOMICITY); a second map on the same
    source is refused (a node finalizes ONCE — one fork).

    THE DICT FORM IS THE TYPED ROUTE (T27 — the reviewer's (b), the
    per-element case): ``map_source(src, {A: fn_a, B: fn_b})`` keys the
    arms by the source's union members' TYPES — each element runs ITS
    type's arm as a fresh job (the fork stamps the child's placement
    from the arm's ``RouteArm``), the child rows feed the same derived
    join (the join-back BY CONSTRUCTION), and the join packs the arms'
    returns — the typed sum. This lowers through the SAME attachment the
    :func:`route` verb spells; the two spellings are one machinery.
    Totality is the fence at BOTH compile doors (the verb's refusal and
    the validator's ``E15-route-totality``) plus the runtime loud door
    (``RouterNotTotal``): the route's keys must cover the union's
    members EXACTLY.

    THE JOIN KEY IS DERIVED (the map has NO ``key=`` param — the
    teardown round's removal, documented in the changelog): the engine's
    fork + the runner address the map join by the SOURCE's own key +
    ``'.join'`` (the addressing is load-bearing across the fork's
    atomic write set and the consumption door) — a custom join key
    cannot be honored without a second addressing scheme. An earlier
    revision shipped the ``key=`` param as the accept-and-REFUSE seat
    (every value raised the build error — a param whose only possible
    outcome is the author's own refusal is a trap, not API); the param
    is GONE. To name the node, name the SOURCE (``step(source_body,
    key=…)``) — the join's key follows.

    ``aggregate=`` is the map's DECLARED READ-SIDE aggregate (T21
    decision c): a PURE fn over the children's decoded result rows,
    evaluated AT READ TIME by the aggregation surfaces
    (:func:`taskq.workflows._progress_read.read_map_aggregate`) — NEVER a
    blocking fan-in (DH8's fence: the join node is for DATAFLOW; a
    progress question is answered at read, unblocked, mid-flight).

    ``rate_limits=`` is the CHILDREN's admission terms (CURE 2 — the
    plain map's face): the fork stamps the names onto every child row;
    the typed route's dict form carries the per-arm buckets on the
    ``RouteArm``s instead (a route-level ``rate_limits=`` would lie about
    which arm pays which bucket)."""
    if isinstance(body, Mapping):
        return cast(
            "Promise[list[R]]",
            _attach_route(
                source,
                cast("Mapping[type, RouteArm[object] | Callable[..., Awaitable[object]]]", body),
                queue=queue,
                on_failure=on_failure,
                max_attempts=max_attempts,
                aggregate=aggregate,
                child_driven=child_driven,
            ),
        )  # Why: the promise's STATIC type is the overload's Promise[list[R]] (R solved at the call site); the attachment erases to list[object] — the cast is the seam.
    # THE LOUD DOOR at the wiring site (the keyed-ref refusal — the same
    # door step() keeps).
    rate_limit_names(tuple(rate_limits), what=f"map_source {source.key!r}'s rate_limits")
    graph = source.graph
    source_node = graph.nodes.get(source.key)
    if source_node is None:
        raise WorkflowBuildError(
            f"map_source's promise {source.key!r} is not a node of this "
            "graph — maps attach to a wired source"
        )
    if source_node.map_item is not None or source_node.map_arms is not None:
        # THE DOUBLE-ATTACH REFUSAL, BOTH SPELLINGS (the rv4 cure — the
        # message-level lie fixed): the pre-cure check read map_item
        # only — a map over a ROUTED source attached ON TOP silently
        # (the map_item clobbered the route's placement fields, the
        # route's arms dead, the runner's map branch winning the elif),
        # and the error's own text ("already carries a map") lied about
        # the attachment it refused. Both attachments refused, the text
        # names WHICH.
        carried = "a map" if source_node.map_item is not None else "a typed route"
        raise WorkflowBuildError(
            f"node {source.key!r} already carries {carried} — a node "
            "finalizes once (one fork); wire the second map or route "
            "from a distinct source"
        )
    source_node.map_item = body
    source_node.map_queue = queue
    source_node.map_max_attempts = max_attempts
    source_node.map_on_failure = on_failure
    source_node.map_aggregate = aggregate
    source_node.map_child_driven = child_driven
    source_node.map_rate_limits = tuple(rate_limits)
    # THE CAPPED HANDLE'S SAME-ACTOR LAW (H-A4 — the map arm): the map's
    # body carries capacity terms and the placement keeps the node's
    # actor (the map has no actor override — ``map_queue`` only): the
    # terms FLOW into the node's unset slots (the children share the
    # node's cohort), never silently dropped. THE GATE IS IS-HANDLE, not
    # is-capped: the OWNER is a carried value on every handle (the
    # default "operator" is a value, not an absence), so a default
    # handle meeting a node that already carries a DIFFERENT owner is
    # the named conflict (one cohort, one owner) — while a default
    # handle alone writes nothing.
    from taskq.workflows.api._app import WorkflowActor  # the cycle-break (inner_fn's own pattern)

    if isinstance(body, WorkflowActor):
        _wire_handle_terms_into_node(source_node, body, what=f"map {source.key!r}")
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


def route_child_key(source_key: str, type_tag: str) -> str:
    """The route child's DERIVED step key (T27 — one home; the register,
    the runner and the validator derive the same address): the map's own
    ``<source>.item`` namespace extended by the arm's TYPE TAG —
    ``<source>.item:<module.qualname>``. The row's step_key NAMES its arm
    (the ledger receipt is direct), the body resolves from the registry
    under it (D1), and the idempotency key + the claim arbiter
    discriminate normally (the element's ``map_index`` rides every
    child)."""
    return f"{source_key}.item:{type_tag}"


def _attach_route[S](
    source: Promise[S],
    arms: Mapping[type, RouteArm[object] | Callable[..., Awaitable[object]]],
    *,
    queue: str,
    on_failure: EdgeFailurePolicy,
    max_attempts: int,
    aggregate: Callable[[list[Any]], object] | None,
    child_driven: bool = False,
) -> Promise[list[object]]:
    """THE TYPED ROUTE'S LOWERING (T27 — the type-tagged Route's machinery
    extended to the graph level): the arms are keyed by the source's
    union MEMBERS' types; the source node carries the attachment
    (``map_arms``, keyed by the members' type tags) and the derived
    ``<source>.join`` node is created exactly as the map's — the join-back
    is BY CONSTRUCTION (every routed child is a fork child with an edge
    to the join).

    THE VERB'S DOOR (the map_source precedent — the mechanically-
    impossible refuses at the wiring site): an EMPTY arms dict, a
    non-TYPE key, a source that already carries an attachment (a node
    finalizes once — one fork), a source whose declared return does not
    resolve to a list of pydantic models, and the TOTALITY breach —
    the keys must be EXACTLY the union's members (a missing member would
    route NOTHING — the silent drop the route exists to refuse; an
    unknown member would never fire). The validator's
    ``E15-route-totality`` re-proves the fence from the compiled graph
    (checker-independently); the runtime ``RouterNotTotal`` names the
    body that lied about its type."""
    from taskq.workflows.chain import type_tag

    if not arms:
        raise WorkflowBuildError(
            "route() over an EMPTY arms dict — a route with no arms can "
            "route nothing (the stranded invisible dispatch); declare at "
            "least one arm per union member"
        )
    normalized: dict[str, RouteArm[object]] = {}
    tag_owners: dict[str, type] = {}  # the tag injectivity's ledger (the twin refusal reads it)
    for key, value in arms.items():
        if not isinstance(key, type):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the keys are statically `type` — but the STRING KEY is the lie this door convicts (the corpus's probe rides it; E13's own pattern: the runtime check IS the subject).
            raise WorkflowBuildError(
                f"route() arm key {key!r} is not a TYPE — the route's keys "
                "are the source's union MEMBERS (the type IS the tag; a "
                "string key is the enum face's habit, refused here)"
            )
        # THE TAG-INJECTIVITY REFUSAL (the rv4 cure — the same-qualname
        # twin): the tag (module.qualname) is the arms' IDENTITY — two
        # distinct classes sharing one tag (the same-qualname twin:
        # same module, same qualname — a class defined twice, a helper
        # factory's products) collapsed SILENTLY, the second arm
        # overwriting the first in the normalized map (the route routed
        # the twin's elements to the WRONG body, clean at build). The
        # refusal names both owners and the fix.
        tag = type_tag(key)
        owner = tag_owners.get(tag)
        if owner is not None and owner is not key:
            raise WorkflowBuildError(
                f"route() arms {owner!r} and {key!r} collapse to the same "
                f"type tag {tag!r} — the tags are the arms' identity and "
                "they are not injective here (the same-qualname twin: "
                "the second arm silently overwrote the first). Rename one "
                "class, or move it to its own module, so each arm owns "
                "its tag"
            )
        tag_owners[tag] = key
        # The R-erase at the attachment (the SUM rides the promise's
        # static type, never the decl's field — the cast is the seam).
        arm = cast(
            "RouteArm[object]",
            value if isinstance(value, RouteArm) else RouteArm(body=value),
        )
        # THE PRIORITY DOMAIN (the step()'s own law — the wiring site
        # refuses before any row exists): the arm's override checked at
        # the same door the arms normalize through (both map_source's
        # dict form and route() lower HERE).
        if arm.priority is not None:
            check_priority_domain(arm.priority, what=f"route arm {tag!r}'s priority")
        # THE ARM'S OWN OWNER SHAPE DOOR (C15's review cure — the
        # priority surface's door shape): the arm's declared owner is
        # exactly one of the two names, validated at the wiring both
        # this lower and the map_source dict form share.
        validate_capacity_owner(arm.capacity_owner, what=f"route arm {tag!r}'s")
        normalized[tag] = arm
    graph = source.graph
    source_node = graph.nodes.get(source.key)
    if source_node is None:
        raise WorkflowBuildError(
            f"route's promise {source.key!r} is not a node of this "
            "graph — routes attach to a wired source"
        )
    if source_node.map_item is not None or source_node.map_arms is not None:
        raise WorkflowBuildError(
            f"node {source.key!r} already carries a map or a route — a node "
            "finalizes once (one fork); wire the second from a distinct source"
        )
    if source_node.cache:
        # THE ROUTE-SOURCE WEDGE'S REFUSAL (the cache-cures round's
        # CURE 1 — MERGE-BLOCKER P8f): cache=True on a typed-route
        # source wedges the run FOREVER at the first cache hit — the
        # hit path never rebuilds the route fork (the arm dispatch
        # reads the element's RUNTIME type; the cached envelope carries
        # only encoded dicts — the fork CANNOT be rebuilt from it), the
        # route children never spawn, the join never fires, and the run
        # stands wedged with the source row reading `succeeded` wearing
        # a wf_cache_hit receipt (the live repro:
        # .measurements/cache-cures/route_wedge_repro.py). The typed
        # refusal names the alternative: the fork is the engine's own
        # machinery — cache the deterministic ARM BODIES' work (each
        # arm's expensive step its own cache=True), never the
        # fork-spawning source.
        raise WorkflowBuildError(
            f"node {source.key!r} declares cache=True and now carries a "
            "typed route — the combination is refused: at the first "
            "cache hit the source terminalizes from the cached envelope "
            "and the route fork NEVER SPAWNS (the arm dispatch reads the "
            "element's runtime type, which the cached envelope cannot "
            "carry) — the route's children and join never exist, the "
            "downstream waits forever, and the run wedges with the "
            "source reading succeeded. Cache the route's ARMS' work, not "
            "the fork: leave the source uncached and let each arm's own "
            "downstream cacheable step (its deterministic, "
            "addressable subwork) carry the cache=True"
        )
    # THE UNION MEMBERS, RESOLVED (the same resolution E5 and the codec
    # read — the resolved-hints seam; the actor handle unwrapped first).
    source_body = source_node.body
    if source_body is not None:
        inner = inner_fn(source_body)
        hints = body_hints(inner)
        returned = hints.get("return")
        members: set[type[BaseModel]] = set()
        if returned is None:
            # THE MESSAGE'S TRUTH (the rv4 cure — the "does not declare"
            # lie fixed): an UNRESOLVABLE return annotation (a
            # function-scope model, a string the compile cannot chase —
            # body_hints resolved {}) is not a body that "does not
            # declare" — it declared and the compile CANNOT RESOLVE it.
            # The refusal names the resolution failure (the fix: name
            # the model at module scope so the annotation resolves).
            #
            # THE TWO FACES, DISTINGUISHED (the de-slop round's finding —
            # the pre-fix cure collapsed them: a body with NO return
            # annotation also resolves to hints {} and got the
            # CANNOT-RESOLVE refusal — a lie in the OTHER direction). The
            # discriminator is the RAW annotation dict's own "return" key
            # (presence survives every resolution failure; the strings'
            # VALUES never resolve here):
            declared_raw = getattr(inner_fn(source_body), "__annotations__", {})
            if "return" in declared_raw:
                raise WorkflowBuildError(
                    f"route's source {source.key!r} has a return annotation "
                    "the compile CANNOT RESOLVE — the route keys its arms by "
                    "the union MEMBERS (the resolved hints are the only "
                    "resolution): move the element models to module scope so "
                    "the list[...] return resolves"
                )
            raise WorkflowBuildError(
                f"route's source {source.key!r} does not declare a "
                "list[...] return — the route is a MAP face: the source "
                "produces the elements' list, the arms key its members"
            )
        if get_origin(returned) is list:
            (element_type,) = get_args(cast("type[object]", returned))
            union_members = (
                get_args(element_type) if get_origin(element_type) is not None else (element_type,)
            )
            for member in union_members:
                if not (isinstance(member, type) and issubclass(member, BaseModel)):
                    raise WorkflowBuildError(
                        f"route's source {source.key!r} declares a return "
                        f"element {member!r} that is not a pydantic model — "
                        "the route dispatches by the element's TYPE TAG and "
                        "decodes into the arm's declared model: a non-model "
                        "member has no declared shape to decode"
                    )
                members.add(member)
            declared = set(normalized)
            wanted = {type_tag(m) for m in members}
            if declared != wanted:
                missing = sorted(wanted - declared)
                unknown = sorted(declared - wanted)
                raise WorkflowBuildError(
                    f"route over {source.key!r} is not total — missing "
                    f"{missing}, unknown {unknown}. A non-total route is "
                    "refused at the wiring: the element it drops would route "
                    "NOTHING (the silent drop the route exists to refuse; "
                    "the validator's E15-route-totality re-proves this fence "
                    "from the compiled graph, and the runtime's "
                    "RouterNotTotal names the body that lied)."
                )
        else:
            raise WorkflowBuildError(
                f"route's source {source.key!r} does not declare a "
                "list[...] return — the route is a MAP face: the source "
                "produces the elements' list, the arms key its members"
            )
    else:
        raise WorkflowBuildError(
            f"route's source {source.key!r} declares no body — a route "
            "attaches to a wired source node"
        )
    source_node.map_arms = normalized
    source_node.map_queue = queue
    source_node.map_max_attempts = max_attempts
    source_node.map_on_failure = on_failure
    source_node.map_aggregate = aggregate
    source_node.map_child_driven = child_driven
    # THE CAPPED HANDLE'S PLACEMENT LAW (H-A4 — never a silent fail-open):
    # an arm body carrying capacity terms is either carried (the arm's
    # own declaration — the passthrough above) or the wiring refuses.
    for arm_tag, arm in normalized.items():
        handle = _capped_handle_of(arm.body)
        if handle is None:
            # THE OWNER'S IS-HANDLE FLOW (the map arm's own law): a
            # DEFAULT-owner arm body handle is not "capped" (nothing to
            # refuse), but its owner face still flows when the placement
            # KEEPS the actor — the same-cohort conflict law reads it.
            if arm.actor is None or arm.actor == source_node.actor:
                from taskq.workflows.api._app import (
                    WorkflowActor,  # the cycle-break (inner_fn's own pattern)
                )

                if isinstance(arm.body, WorkflowActor):
                    _wire_handle_terms_into_node(
                        source_node, arm.body, what=f"route arm {arm_tag!r}"
                    )
            continue
        overrides = arm.actor is not None and arm.actor != source_node.actor
        if overrides:
            uncovered = _uncovered_handle_terms(arm, handle)
            if uncovered:
                raise WorkflowBuildError(
                    f"route arm {arm_tag!r}'s body carries capacity terms "
                    f"({', '.join(uncovered)}) — the handle's capacity "
                    "terms would be dropped by the placement override "
                    f"(actor={arm.actor!r}): the arm's cohort projects no "
                    "capacity and the guard fails open by rename. Declare "
                    "the terms on the override (RouteArm(max_concurrent=…,"
                    "reservations=…, result_ttl=…)) or keep the handle's "
                    "actor."
                )
        else:
            _wire_handle_terms_into_node(source_node, handle, what=f"route arm {arm_tag!r}")
    join_key = f"{source.key}.join"
    graph.add(
        NodeDecl(
            key=join_key,
            actor=source_node.actor,
            queue=queue,
            body=None,  # the default identity packer (the join's result IS the arms' returns — the typed sum)
            parents=(source.key,),
            on_failure=on_failure,
            kind="map_join",
        )
    )
    return cast(
        "Promise[list[object]]", Promise(join_key, list[object], graph)
    )  # Why: the join promise's RUNTIME data_type stays the packing declaration (list[object]) read back by validate(); the STATIC face is the verb's overload (Promise[list[R]], R solved at the call site). The cast is the seam.


def route[S, R](
    source: Promise[S],
    arms: Mapping[type, RouteArm[R] | Callable[..., Awaitable[R]]],
    *,
    queue: str = "default",
    on_failure: EdgeFailurePolicy = "fail_closed",
    max_attempts: int = 3,
    aggregate: Callable[[list[Any]], object] | None = None,
    child_driven: bool = False,
) -> Promise[list[R]]:
    """Wire a TYPED ROUTE over *source*'s items (T27 — the graph-DSL's
    face of the type-tagged Route): the source's body returns the
    elements' list (``list[ImageItem | AudioItem]``), and *arms* keys the
    union MEMBERS' types to the arm bodies — each element runs ITS type's
    arm as a FRESH job (per-item ledger identity, the fork's child row
    NAMING its arm: ``<source>.item:<module.qualname>``), the children
    feed the derived ``<source>.join`` (the join-back BY CONSTRUCTION —
    R2's), and the join packs the arms' returns VERBATIM — the TYPED SUM
    (``Promise[list[R]]``, R solved to the union of the arms' returns).
    VERBATIM means ONE join element per child, the arm's return as-is:
    an arm returning ``Report`` packs ``list[Report]``; an arm returning
    ``list[RowOutcome]`` packs that LIST as one element (the shape
    NESTS — the join is ``list[list[RowOutcome]]`` there, and the
    consumer's annotation must declare the nesting or the decode dies
    loudly). Flatten in the arm itself (return the items' unpacked sum)
    when a flat list is wanted.

    THE PER-ARM PLACEMENT (R4): an arm spelled
    ``RouteArm(body=process_image, queue="gpu")`` stamps ITS children's
    actor/queue — ``processA`` on the gpu queue, ``processB`` on the io
    queue, the ROWS the receipt. A bare ``Callable`` arm defaults to the
    route's own ``queue=`` and the source's actor. The PER-ARM ADMISSION
    TERMS (CURE 2) ride the same shape: ``RouteArm(rate_limits=[...])``
    stamps the arm's own token buckets onto its children's rows.

    THE DECODED TYPED MODELS (R3): the arm body's declared param type IS
    the decode's target — the jsonb element re-validates into the arm's
    model (the typed boundary), a mis-declared arm dies LOUDLY in the
    coercion, and a duck-typed arm (``dict``/unannotated/``Any``) is
    refused at build (``E15-route-totality`` — the duck-shaped hole is
    the exact face the route closes).

    TOTALITY IS THE FENCE (R1), at THREE doors: the wiring verb refuses a
    non-total route (missing/unknown members, NAMED) before any row
    exists; the validator's ``E15-route-totality`` re-proves the fence
    from the compiled graph (checker-independently — the compiled graph
    is public, mutable data); and the RUNTIME door — an element whose
    type has NO arm (the body that lied about its union) raises
    :class:`taskq.workflows.RouterNotTotal`: the source terminal-FAILS
    with ``error_class='RouterNotTotal'``, nothing routes, the run
    FAILS. The skip-silent shape is DEAD: the stringly ``skip=``
    predicate face (a decoded-dict predicate that can drop an element
    from every arm and terminalize succeeded-having-routed-nothing) is
    deprecated for dispatch — teach the typed route.

    ``map_source(src, {A: fn_a, B: fn_b})`` is this verb's dict form —
    the SAME lowering (the per-element case at the map face).

    THE FAN-IN BOUND'S ESCAPE (``child_driven=True`` — the docshelf
    adoption team's F6): a route/map over a corpus past
    :data:`MAX_FAN_IN_PER_JOIN` is REFUSED
    (:class:`taskq.workflows.FanInBoundExceededError`) — the escape is
    the CHILD-DRIVEN join shape, declared HERE at the wiring (the
    escape was reachable only through the raw ``JoinSpec`` before; the
    verb is the authoring face): the fire counts its terminal children
    FROM THE EDGE LEDGER, never the per-joined-row counter cache, and
    the choice is recorded on the joined node's
    ``metadata.join_shape='child_driven'``. Prefer PARTITIONING the map
    over the escape (the guide's fan-in section prices both)."""
    return cast(
        "Promise[list[R]]",
        _attach_route(
            source,
            cast("Mapping[type, RouteArm[object] | Callable[..., Awaitable[object]]]", arms),
            queue=queue,
            on_failure=on_failure,
            max_attempts=max_attempts,
            aggregate=aggregate,
            child_driven=child_driven,
        ),
    )  # Why: the promise's STATIC type is the overload's Promise[list[R]] (R solved from the arms' bodies' returns — the union of the arms); the attachment erases to list[object] — the cast is the seam.


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
    # THE CAPPED HANDLE'S PLACEMENT LAW (H-A4 — the chain arm): the
    # chain's rows ride the CHAIN's own actor (its own placement
    # override) and the chain surface has no capacity passthrough — a
    # capped handle wired as a chain step body would have its terms
    # dropped silently. The wiring refuses.
    for step_key, chain_step in chain.steps.items():
        chain_handle = _capped_handle_of(chain_step.body)
        if chain_handle is not None:
            raise WorkflowBuildError(
                f"chain {chain.name!r}'s step {step_key!r} wires a capped "
                "actor handle — the chain's rows ride the CHAIN's own "
                f"actor (actor={chain.actor!r}) and the chain surface has "
                "no capacity passthrough: the handle's capacity terms "
                "would be dropped by the placement override (the guard "
                "failing open by rename). Wire the capped body as a graph "
                "step (its own cohort), or strip its terms."
            )
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
