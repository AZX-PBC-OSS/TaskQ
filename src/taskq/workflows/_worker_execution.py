"""THE WORKER-HOSTED EXECUTION SEAM (the execution verdict's gap-1 and
gap-2 cures): the machinery that makes a claimed workflow row EXECUTE on
the worker that claimed it — the queue IS the runtime, in production.

Three faces, one seam:

* **THE APP REGISTRY** — every constructed :class:`~taskq.workflows.api.
  _app.WorkflowApp` registers here (weakly; the authored apps are
  module-level singletons, the registry never keeps an app alive).
  ``workflow_execution_capable`` reads it: a worker whose process
  imported at least one app CAN execute workflow bodies (the
  definitions are importable and the projection can compile them); a
  worker that never imported any cannot — and the dispatch fence
  (``_dispatch_sql``'s execution leg) makes sure it is never HANDED a
  workflow row to fail on.

* **THE BOOT PROJECTION (the F3 law's call site)** —
  :func:`project_workflow_actor_configs` compiles every imported app's
  workflows (which is also what populates the D1 definition registry in
  THIS process — the intercept's ``resolve_step_body`` answers from
  here) and projects the (actor, queue) cohorts the compiled graphs can
  stamp into ``ActorConfig`` rows, the same surface the vanilla
  ``@actor`` refs ride at boot (``sync_actor_config``). One actor name,
  one queue — the schema's own law. A flow whose declaration gives ONE
  actor name TWO queues is the refused defect: the split placement is
  expressed with DISTINCT actor names per queue (the chain's gpu step =
  a gpu-named actor) — the cohort conflict error names that cure. The
  override-warning path (one queue silently winning) is dead by
  construction: the conflict refuses, nothing silently drops.

* **THE EXECUTION DOOR** — :func:`execute_flow_job`: a CLAIMED row
  (``dispatch_batch`` claimed it — running, attempt incremented, the
  epoch bumped, the lock held by this worker) resolves its body from
  the registered definition (D1 — the flow root's stamped
  ``metadata.workflow`` name, the same durable leg the leader's fire
  arm uses) and executes it through a worker-hosted :class:`FlowRunner`
  — the runner's OWN ledger-claim + finalize machinery, the same tx1/
  tx2 and the same fences, NOT a second execution semantics. The
  in-process driver and this door differ only in WHO CLAIMED: the
  runner's ``run_fleet_claimed_step`` is the door's one leg.

THE IMPORT LAW (§16.1): the worker's modules import THIS module lazily,
inside the hook seams that need it — ``import taskq`` never imports the
workflows package, and a worker that never opens this seam pays nothing.
"""

from __future__ import annotations

import weakref
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Final, NamedTuple, cast
from uuid import UUID

from taskq.actor_config import ActorConfig
from taskq.backend._protocol import JobId, JobRow
from taskq.constants import resolve_priority_stamp
from taskq.obs import get_logger
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.workflows.api._app import CompiledWorkflow
from taskq.workflows.api._runner_errors import WorkflowRunError
from taskq.workflows.api._sql_runner import render_sql

if TYPE_CHECKING:
    import asyncpg

    class WorkflowAppFace:
        """The projection's contract with a registered app (the two
        attribute reads — the same face ``WorkflowApp`` exposes; the
        runtime registry holds the real objects)."""

        def workflow_names(self) -> list[str]: ...

        def get(self, name: str) -> CompiledWorkflow: ...


__all__ = [
    "FlowExecution",
    "WorkflowActorCapacityConflictError",
    "WorkflowActorQueueConflictError",
    "WorkflowBodyUnresolvableError",
    "WorkflowQueueUnconsumedError",
    "any_app_declares_strict_queues",
    "collect_workflow_rate_limits",
    "collect_workflow_reservations",
    "execute_flow_job",
    "get_compiled_workflow",
    "iter_imported_apps",
    "project_workflow_actor_configs",
    "record_compiled",
    "register_app",
    "reset_app_registry_for_tests",
    "workflow_execution_capable",
]

logger = get_logger(__name__)

#: The node-row read the execution door makes: the claimed row's step
#: identity (the dispatch decode drops ``step_key``/``map_index`` — the
#: vanilla JobRow has no fields for them) + the flow root's stamped
#: workflow name + the run's stamped priority override (the wf-priority
#: cure — ``create_flow(priority=…)`` wrote it; the door's fork builds
#: resolve it as the precedence law's first level), ONE bounded
#: primary-key round trip per claimed flow row (the same cost class the
#: leader's fire arm pays for the same stamped name).
_CLAIMED_NODE_READ_SQL_TEMPLATE = """
SELECT j.step_key, j.map_index,
       root.metadata->>'workflow' AS workflow,
       root.metadata->>'wf_priority' AS wf_priority
FROM {schema}.jobs j
LEFT JOIN {schema}.jobs root ON root.id = (j.metadata->>'flow_id')::uuid
WHERE j.id = $1
"""

#: The projection's metadata stamp on the projected ``actor_config``
#: rows — the estate's surfaces (the drift guards, the admin actors
#: page, ``TASKQ_QUEUES_STRICT``) see the workflow cohorts as the
#: projected population they are (F3: one registry, no invisible
#: cohort).
_PROJECTION_METADATA: dict[str, object] = {"workflow_actor": True}


class WorkflowActorQueueConflictError(TypeError):
    """One workflow actor name declared over TWO queues — the estate's
    one-queue-per-actor law (``actor_config`` is keyed by actor), refused
    wherever the projection computes the cohorts. THE CURE the error
    names: the split placement (a source on ``default``, a chain on
    ``gpu``) is expressed with DISTINCT actor names per queue — the
    chain's gpu step = a gpu-named actor."""


class WorkflowActorCapacityConflictError(TypeError):
    """One workflow actor name declared with TWO different capacity
    terms (``max_concurrent``/``max_pending``/``reservations``/
    ``result_ttl``) — the same one-row-per-actor law, on the capacity
    axes. The projection refuses wherever it computes the terms. THE
    CURE the error names: a node carrying different terms is a
    DIFFERENT actor — give it its own name (the capacity is the
    actor's, not the node's)."""


class _CapacityTerms(NamedTuple):
    """One workflow actor's declared capacity terms (the node's
    admission declaration, gathered at the projection).

    THE LAW'S FIELD SET (H-A3): the ROW-CARRIED fields ONLY — the
    ``ActorConfig`` columns the projection seeds. The RESERVATIONS are
    deliberately NOT here: they have no ``__eq__`` (identity-comparing
    ``__slots__`` classes), so folding them into this tuple compared
    INSTANCE IDENTITY — equal configs on different instances were false
    conflicts (skip-loudly within a workflow, boot-refusals across
    apps). A reservation's identity is the collect's own name-keyed law
    (:func:`collect_workflow_reservations` — differing configs for one
    name are ITS ValueError). ``max_pending`` is deliberately not a
    field (H-A6 — the knob is off the wf surface).

    ``capacity_owner`` is C15's opt-in (the handle's declaration, read
    at the wiring): "operator" (the default) seeds the row and leaves
    the stored capacity operator-owned; "code" projects into the
    ``ActorConfig`` the config sync consumes, whose upsert rewrites the
    cohort's max_concurrent/result_ttl from the literal on every boot.
    The route arms declare their OWN owner (``RouteArm.capacity_owner``,
    default "operator"): an overriding arm's declaration threads
    per-arm onto the arm's effective cohort, and its owner participates
    in the one-cap law's comparison below."""

    max_concurrent: int | None
    result_ttl: float | None
    capacity_owner: str = "operator"


#: The projection's default for an actor whose nodes declare no capacity
#: terms — every field the seed-only ``None`` (the vanilla rows' own
#: undeclared shape). A 3-tuple is always truthy, so the ``.get() is
#: None`` test — never an ``or`` — picks it.
_UNSET_CAPACITY_TERMS = _CapacityTerms(None, None)


def _capacity_terms_of(node: object) -> _CapacityTerms:
    """One node's declared terms, read through the decl surface (the
    ``getattr`` walk is the projection's own contract with the decl)."""
    return _CapacityTerms(
        getattr(node, "max_concurrent", None),
        getattr(node, "result_ttl", None),
        getattr(node, "capacity_owner", "operator"),
    )


def _capacity_conflict_fields(existing: _CapacityTerms, declared: _CapacityTerms) -> list[str]:
    """The FIELD NAMES the two terms differ on (H-A3 — the conflict's
    error names the differing fields, never a values-only dump)."""
    field_names = _CapacityTerms._fields
    differing: list[str] = []
    for name, a, b in zip(field_names, existing, declared, strict=True):
        if a != b:
            differing.append(f"{name}: {a!r} vs {b!r}")
    return differing


class WorkflowQueueUnconsumedError(RuntimeError):
    """A projected workflow cohort's queue is not in the booting worker's
    consumed set (``TASKQ_QUEUES``/``--queues``) — the queue-trap door:
    steps default to ``queue="default"``, and a worker whose consumed set
    excludes a cohort's queue would strand every row that workflow stamps
    there — enqueued fine, pending forever, no error, no log, anywhere
    (the dispatch claim matches only the worker's own queues). THE BOOT
    REFUSES: :func:`taskq.worker._bootstrap.
    _fail_fast_on_unconsumed_workflow_queues` raises this naming the
    unconsumed queues, the consumed set, and the remedy — the same loud
    boot the vanilla queues get from the strict gate."""


class WorkflowBodyUnresolvableError(WorkflowRunError):
    """A claimed flow row whose body cannot resolve in this process: the
    stamped workflow name is not in the definition registry (D1), or the
    step key is not in the registered definition. The fence makes this
    unconstructible for a capable worker's own projection (it compiled
    every imported app); it names a deployment defect — the definitions
    module not imported anywhere — or a hand-crafted row. The defined
    behavior is the actor-not-found semantics: the row parks at the
    snooze cadence, budget-free, the stranded-jobs detector surfaces it
    — LOUD, never a silent wedge."""


class FlowExecution(NamedTuple):
    """The door's execution record: the tail's outcome label + the REAL
    identities the door resolved on the way (the row's step key, the
    stamped workflow name, the flow id). The caller's log line reads
    THESE — never the dispatch decode's metadata (the vanilla JobRow
    decode drops the workflow's step identity; the door's bounded read
    is where the truth is)."""

    outcome: str
    step_key: str
    workflow_name: str
    flow_id: str


# ── the app registry + the compiled cache ───────────────────────────────

_apps: weakref.WeakSet[object] = weakref.WeakSet()
_compiled: dict[str, object] = {}
"""The compiled workflows, by their registered name — the SAME global
namespace the D1 definition registry keys (one name, one graph; the
compile is deterministic, a re-record of the same name is idempotent by
the registry's own law)."""


def register_app(app: object) -> None:
    """The app's boot registration (WorkflowApp.__init__ calls this).
    Weak: the registry observes the imported apps, never pins them."""
    _apps.add(app)


def reset_app_registry_for_tests() -> None:
    """THE TEST-ISOLATION SEAM for the app registry (the conftest's
    ``_isolate_workflow_app_registry`` autouse fixture drives it around
    every test).

    ``_apps`` is the boot projection's ONLY input (``iter_imported_apps``),
    and the projection runs at EVERY in-process worker boot — so a
    WorkflowApp any test module constructed (a module-scoped fixture's
    exec'd doc fence, an imported examples app) keeps projecting its
    actor cohorts into every LATER test's boot on the same xdist worker:
    the boot adds the strangers' ``actor_config`` rows to the test's own
    (the bootstrap pin's ``9 == 2``), and a stranger whose default-named
    actor (``step``'s ``actor="wf"`` default) lands on two queues trips
    the one-queue law at the boot's compile step
    (``WorkflowActorQueueConflictError`` in the health-lifecycle pins).
    Which stranger poisons which victim rotates with pytest-randomly's
    seed — the class is order-dependent by construction, every victim
    green solo.

    Clearing at BOTH ends makes the law explicit: a test's projection
    sees exactly the apps ITS OWN setup registered (a within-test
    ``WorkflowApp()`` registration happens after the setup clear and
    survives until the teardown clear) — never another module's import
    residue. The WeakSet clear does not destroy any app object: an owner
    that still holds its app can re-derive everything from the object
    itself (``app.get`` recompiles idempotently; the D1 registry's
    absorption guard owns the name collision).
    """
    _apps.clear()


def iter_imported_apps() -> list[object]:
    """Snapshot of the constructed (still-alive) WorkflowApps."""
    return list(_apps)


def record_compiled(name: str, compiled: object) -> None:
    """Record a compiled workflow under its registered name (the app's
    ``get`` calls this — the door's compiled lookup answers from here).
    Idempotent by the compile's own law: same module → same graph, and a
    differing body map is refused by the D1 registry's register()."""
    _compiled[name] = compiled


def get_compiled_workflow(name: str) -> object:
    """The compiled workflow registered under *name* (the door's D1
    face). ``KeyError`` = not compiled in this process."""
    return _compiled[name]


def workflow_execution_capable() -> bool:
    """Whether THIS process can execute workflow bodies: at least one
    WorkflowApp was imported (the projection can compile it, which
    populates the D1 registry the body resolution answers). The same
    predicate the boot stamps into the workers row's metadata — the
    dispatch fence's capability is THIS capability, as data."""
    return len(_apps) > 0


def any_app_declares_strict_queues() -> bool:
    """THE APP-DECLARED STRICT FACE, read off the registry the projection
    itself walks: any still-alive imported app declaring
    ``queues_strict=True`` makes THIS process's queue-trap gate strict —
    the app's own definition carries the contract in code (the fleet
    that needs this face cannot coordinate a fleet-wide env var; the
    declaration rides the app every worker child already imports).
    An undeclared app (the default) changes nothing:
    ``getattr``'s False is today's behavior byte-identically. One strict
    app tightens the gate for the WHOLE projected cohort set — the same
    aggregated shape the warning arm reports in (the cohorts are ONE
    projection; the gate is its one door). The faces' OR-law (strict if
    EITHER this face or the env is set) is the bootstrap gate's own
    docstring (``_fail_fast_on_unconsumed_workflow_queues``)."""
    return any(getattr(app, "queues_strict", False) for app in _apps)


# ── the boot projection (the F3 law's call site) ─────────────────────────


def _workflow_cohorts(
    compiled: Any,
) -> tuple[set[tuple[str, str]], dict[str, _CapacityTerms]]:
    """The (actor, queue) cohorts *compiled*'s rows can stamp: the wired
    nodes' decls, the map attachments' child placement (``map_queue`` —
    the fork's children ride it), the chains' step rows (every row of a
    chain — the emitted starts and the forked children alike — lands on
    the chain's own pair), and THE ROUTE ARMS' FORKED COHORTS (the
    docshelf adoption team's F2): the arms' children fork at RUNTIME —
    the source's finalize stamps each arm's effective placement
    (``actor=arm.actor or node.actor, queue=arm.queue or node.map_queue``,
    the runner's ``_route_fork``) — so the projection walks the SAME
    effective pairs here. The pre-cure walk read the decls only: an
    arm's rows stamped a cohort the boot never projected — the rows
    STRANDED SILENTLY (no actor_config → no dispatch capacity, the
    strict-queue worker never listened) until the app provisioned the
    cohorts itself. A boot that strands silently is the defect; the
    projection extends to every cohort the fork can stamp."""
    cohorts: set[tuple[str, str]] = set()
    # THE CAPACITY TERMS per actor (the node's declared admission
    # terms — the handle's or the step's own): the one-cap-per-actor
    # law's raw material. The arms' overridden cohorts (a DIFFERENT
    # actor name) carry NO terms here — an arm's capacity declaration
    # is the RouteArm's own field, not the source node's (the arm's
    # actor is a different cohort; inheriting the source's cap onto a
    # different actor name would be a silent lie). An arm DECLARING its
    # own terms (the RouteArm passthrough) carries THOSE onto the arm's
    # effective cohort.
    caps: dict[str, _CapacityTerms] = {}
    for node in compiled.nodes.values():
        cohorts.add((node.actor, node.queue))
        declared = _capacity_terms_of(node)
        if declared != _UNSET_CAPACITY_TERMS:  # the all-default terms never enter the law
            existing = caps.get(node.actor)
            if existing is not None and existing != declared:
                differing = _capacity_conflict_fields(existing, declared)
                raise WorkflowActorCapacityConflictError(
                    f"workflow actor {node.actor!r} is declared with two "
                    f"different capacity terms ({'; '.join(differing)}) — "
                    "actor_config is keyed by actor "
                    "(one capacity per actor, the estate's own law); give "
                    "the differing node its own actor name."
                )
            caps.setdefault(node.actor, declared)
        if node.map_item is not None or node.map_arms is not None:
            cohorts.add((node.actor, node.map_queue))
        if node.map_arms is not None:
            for arm in node.map_arms.values():
                # THE EFFECTIVE PLACEMENT (the runner's _route_fork's own
                # arithmetic — one vocabulary): the arm overrides DOWN
                # from the source, never up.
                arm_actor = arm.actor or node.actor
                cohorts.add((arm_actor, arm.queue or node.map_queue))
                # THE ARM'S OWN DECLARATION (H-A4's passthrough): the
                # arm's terms project onto the arm's effective cohort —
                # the same one-cap law, the same refusal shape.
                arm_terms = _CapacityTerms(
                    getattr(arm, "max_concurrent", None),
                    getattr(arm, "result_ttl", None),
                    getattr(arm, "capacity_owner", "operator"),
                )
                if arm_terms != _UNSET_CAPACITY_TERMS:  # the all-default terms never enter the law
                    existing = caps.get(arm_actor)
                    if existing is not None and existing != arm_terms:
                        differing = _capacity_conflict_fields(existing, arm_terms)
                        raise WorkflowActorCapacityConflictError(
                            f"workflow actor {arm_actor!r} is declared with two "
                            f"different capacity terms ({'; '.join(differing)}) — "
                            "actor_config is keyed by actor "
                            "(one capacity per actor, the estate's own law); give "
                            "the differing node its own actor name."
                        )
                    caps.setdefault(arm_actor, arm_terms)
    for chain in getattr(compiled, "chains", ()):
        for _step_key in chain.steps:
            cohorts.add((chain.actor, chain.queue))
    # THE LOOP ESCALATION'S COHORT (H-A11): an ``on_exhausted="escalate"``
    # loop ENQUEUES its escalation through the outbox, addressed to the
    # REGISTERED DEFINITION's own (actor, queue) — the WorkflowDef's
    # placement fields ("workflow"/"default"). The projection never
    # walked that cohort: the escalation rows parked forever under the
    # DEFAULT on_exhausted policy (no actor_config row → no dispatch
    # capacity, the strict-queue worker never listening). The escalation
    # body's presence is the registered definition's own law (the
    # enqueue is SKIPPED without one — no body, no rows, no cohort).
    from taskq.workflows.api._loop import ESCALATION_STEP_KEY
    from taskq.workflows.definitions import get_registry

    try:
        definition = get_registry().get(compiled.name)
    except KeyError:
        definition = None
    if (
        definition is not None
        and ESCALATION_STEP_KEY in definition.bodies
        and any(
            node.loop_spec is not None and node.loop_spec.on_exhausted == "escalate"
            for node in compiled.nodes.values()
        )
    ):
        cohorts.add((definition.actor, definition.queue))
    return cohorts, caps


def project_workflow_actor_configs() -> list[ActorConfig]:
    """THE BOOT PROJECTION: every imported app's workflows compile HERE
    (populating the D1 definition registry in this process — the
    intercept's body resolution answers from these very compiles), and
    the compiled graphs' (actor, queue) cohorts project into
    ``ActorConfig`` rows — the same carrier, the same ``sync_actor_config``
    surface, the same drift guards the vanilla ``@actor`` refs ride at
    boot. THE F3 LAW'S CALL SITE: the workflow actors are VISIBLE to the
    estate — the dispatch capacity LATERAL sees their cohorts, the drift
    machinery, the admin page, ``TASKQ_QUEUES_STRICT`` — no second,
    invisible actor population.

    THE ONE-QUEUE LAW: an actor name carrying TWO queues is refused
    (``WorkflowActorQueueConflictError``). TWO granularities, two arms:
    a workflow whose OWN cohorts conflict — or whose build function
    raises, or whose graph fails validation — skips THAT WORKFLOW
    LOUDLY (``workflow-projection-skipped``: the app, the workflow, the
    defect named) and the healthy workflows still project — ONE broken
    build fn must not refuse the ENTIRE worker's boot (F2-3: the
    healthy apps boot). The conflict across workflows/apps — two HEALTHY
    declarations projecting the same actor name onto different queues —
    is the remaining refusal: that is not a broken app to skip, it is
    the drift the estate's guards exist to refuse (skipping one side
    silently would make its rows unclaimable — the invisible cohort by
    another door). The split placement (a source on ``default``, a chain
    on ``gpu``) is expressed with DISTINCT actor names per queue; the
    override-warning path (one queue silently winning) is dead.

    THE ONE-CAPACITY LAW (the same shape, on the capacity axes): an
    actor name carrying TWO different capacity terms is refused
    (``WorkflowActorCapacityConflictError``). Two granularities, the
    same two arms: within ONE workflow the declaration is broken — that
    workflow skips LOUDLY (the isolation arm above, its cohorts
    unprojected); ACROSS workflows/apps two HEALTHY declarations
    fighting over one actor's terms REFUSES the boot — skipping one
    side would project the OTHER's terms, and the actor would claim
    under a capacity nobody declared (the segfault guard failing open
    by another door). A node carrying different terms is a DIFFERENT
    actor — give it its own name.

    DETERMINISTIC ORDER (sorted): the sync's drift comparison and the
    event stream read the projection — a stable order is the readable
    one.
    """
    totals: dict[str, tuple[str, set[str]]] = {}
    capacity_totals: dict[str, _CapacityTerms] = {}
    for app in iter_imported_apps():
        workflow_app = cast("WorkflowAppFace", app)
        app_face = f"{type(app).__module__}.{type(app).__name__}"
        for name in sorted(workflow_app.workflow_names()):
            workflow_cohorts: dict[str, set[str]] = {}
            _cohorts: set[tuple[str, str]] = set()
            _caps: dict[str, _CapacityTerms] = {}
            try:
                compiled = workflow_app.get(name)
                record_compiled(name, compiled)
                _cohorts, _caps = _workflow_cohorts(compiled)
                for actor, queue in sorted(_cohorts):
                    seen = workflow_cohorts.setdefault(actor, set())
                    seen.add(queue)
                    if len(seen) > 1:
                        raise WorkflowActorQueueConflictError(
                            f"workflow actor {actor!r} is declared over queues "
                            f"{sorted(seen)} — actor_config is keyed by actor "
                            "(one queue per actor, the estate's own law). The "
                            "split placement is expressed with DISTINCT actor "
                            "names per queue: the chain's gpu step is a "
                            "gpu-named actor (e.g. actor='wf-gpu', queue='gpu')."
                        )
            except WorkflowActorCapacityConflictError:
                # H-A7: the CAPACITY conflict is NEVER skippable — the
                # isolation arm's skip stranded the conflicted workflow's
                # rows forever (no cohort projected, nothing claims them,
                # the queue-trap door never sees the cohort) and the
                # refusal is the across-apps arm's own semantics. The
                # OTHER failure classes (a raising build fn, a validation
                # refusal, the queue conflict) keep the skip-loudly arm.
                raise
            except Exception as exc:  # Why: the isolation IS the cure (F2-3) — one broken build fn (a raising builder, a validation refusal, the workflow's own queue conflict) must not refuse the worker's whole boot; the failure is the same skip-loudly arm for every shape.
                logger.error(
                    "workflow-projection-skipped",
                    app=app_face,
                    workflow=name,
                    error_class=type(exc).__name__,
                    error=str(exc)[:300],
                    remedy=(
                        "this workflow's cohorts are NOT projected and its "
                        "rows will not claim until the declaration is fixed; "
                        "the healthy workflows' cohorts project unchanged"
                    ),
                )
                continue
            # THE ACROSS-WORKFLOW CAPACITY MERGE — deliberately OUTSIDE the
            # isolation above, the across-workflow QUEUE merge's own shape:
            # two HEALTHY declarations declaring one actor name with
            # DIFFERENT capacity terms is the drift the guards refuse —
            # skipping one side silently would project the OTHER side's
            # terms, and the actor would claim under a capacity nobody
            # declared (the segfault guard failing OPEN by another door).
            # Raised, never skipped.
            for actor, terms in _caps.items():
                existing_terms = capacity_totals.get(actor)
                if existing_terms is not None and existing_terms != terms:
                    differing = _capacity_conflict_fields(existing_terms, terms)
                    raise WorkflowActorCapacityConflictError(
                        f"workflow actor {actor!r} is declared with two "
                        f"different capacity terms across workflows/apps "
                        f"({'; '.join(differing)}) — "
                        "actor_config is keyed by actor (one capacity "
                        "per actor, the estate's own law)."
                    )
                capacity_totals.setdefault(actor, terms)
            for actor, seen in workflow_cohorts.items():
                queues = totals.get(actor)
                if queues is None:
                    totals[actor] = (next(iter(seen)), seen)
                    continue
                merged = queues[1] | seen
                if len(merged) > 1:
                    raise WorkflowActorQueueConflictError(
                        f"workflow actor {actor!r} is projected over queues "
                        f"{sorted(merged)} across workflows/apps — "
                        "actor_config is keyed by actor (one queue per "
                        "actor, the estate's own law); two HEALTHY "
                        "declarations fighting over one cohort name is the "
                        "drift the guards refuse, never a silent skip"
                    )
                totals[actor] = (next(iter(merged)), merged)
    configured: list[ActorConfig] = []
    for actor, (queue, _seen) in sorted(totals.items()):
        # THE CAPACITY TERMS (or their unset defaults): an actor whose
        # nodes declare none projects the all-None row — the same
        # seed-only shape the vanilla rows carry. RESERVATIONS DO NOT RIDE
        # THE ROW (ActorConfig has no field for them): their slot rows
        # materialize through the bootstrap's ensure-slots pass, fed by
        # :func:`collect_workflow_reservations`.
        terms = capacity_totals.get(actor)
        if terms is None:
            terms = _UNSET_CAPACITY_TERMS
        configured.append(
            ActorConfig(
                actor=actor,
                max_concurrent=terms.max_concurrent,
                # max_pending is NOT on the wf surface (H-A6 — the engine's
                # inserts bypass the enqueue admission; the knob was dead):
                # the row seeds the estate's field UNSET.
                max_pending=None,
                result_ttl=terms.result_ttl,
                queue=queue,
                metadata=dict(_PROJECTION_METADATA),
                # THE CAPACITY OWNER (C15): the handle's declaration,
                # carried through the decl's terms — "operator" (the
                # default) is the estate's seed-only law, unchanged;
                # "code" is the config sync's deploy-by-code opt-in.
                capacity_owner=terms.capacity_owner,
            )
        )
    return configured


# ── the boot's rate-limit collection (CURE 2) ────────────────────────────


#: The rate-limit buckets' CONFIG FIELDS the duplicate-name law compares
#: (the ``_same_config`` surface's union — ``TokenBucket``:
#: capacity/refill_per_second/backend/ttl; ``SlidingWindow``:
#: limit/window/backend/style/ttl; ``name`` is the registry's key, equal
#: by construction). The collect's conflict refusal names THESE — never
#: a raw ``__slots__`` repr (the reservation face's own fix, mirrored:
#: the default repr is an address, not a diagnosis).
_RATE_LIMIT_CONFIG_FIELDS: Final[frozenset[str]] = frozenset(
    {"capacity", "refill_per_second", "limit", "window", "backend", "style", "ttl"}
)


def _rate_limit_config_diff(existing: object, declared: object) -> str:
    """The differing config fields' render (the reservation face's
    ``_reservation_config_diff`` shape, mirrored for the rate-limit
    buckets — the field names WITH their values, never an ``object at
    0x…`` dump). A kind mismatch (a TokenBucket against a SlidingWindow
    of one name) is itself the first differing field."""
    if type(existing) is not type(declared):
        return f"kind: {type(existing).__name__} vs {type(declared).__name__}"
    differing = [
        f"{field}: {getattr(existing, field, None)!r} vs {getattr(declared, field, None)!r}"
        for field in sorted(_RATE_LIMIT_CONFIG_FIELDS)
        if getattr(existing, field, None) != getattr(declared, field, None)
    ]
    return "; ".join(differing)


def _iter_live_recorded_workflows() -> Iterator[tuple[str, str, CompiledWorkflow | None]]:
    """THE SIBLING COLLECTS' ONE BASE (the round-3 cure — the ghost's
    grave): the LIVE apps' workflow names (:func:`iter_imported_apps`'s
    own walk — the reservations collect's base all along), each name
    resolved through the RECORDED compile cache (H-D1's law: the build
    fn is never re-invoked here), yielded as ``(app_face, name,
    compiled)`` — ``compiled`` is ``None`` when the projection's
    isolation arm skipped the workflow (the cache holds no recording).

    Never the process-global ``_compiled`` dict alone: that cache has no
    eviction, so a workflow deleted from its app in-process (the
    reload/redeploy story) leaves its graph strongly referenced FOREVER,
    and a collect walking the dict kept registering an ORPHAN's buckets
    beside the live estate's — a surviving workflow re-declaring the
    same bucket name with a different config then raised the registry's
    conflict refusal at the collect's register — THE GHOST REFUSED THE
    BOOT. The live-apps walk is the one base both siblings share: the
    orphaned graphs' terms stop registering (the ghosts die), and the
    two collects cannot drift apart again."""
    for app in iter_imported_apps():
        workflow_app = cast("WorkflowAppFace", app)
        app_face = f"{type(app).__module__}.{type(app).__name__}"
        for name in sorted(workflow_app.workflow_names()):
            try:
                compiled = get_compiled_workflow(name)
            except KeyError:
                compiled = None
            yield app_face, name, compiled if isinstance(compiled, CompiledWorkflow) else None


def collect_workflow_rate_limits(registry: RateLimitRegistry) -> tuple[list[str], list[str]]:
    """THE BOOT'S COLLECT (the vanilla actors' collection pass's sibling
    — the consumer-face lane's CURE 2): every imported app's compiled
    graphs walk once (the LIVE apps' names — :func:
    `_iter_live_recorded_workflows`, the reservations sibling's ONE
    base; the orphan-inclusive ``_compiled`` walk was the round-3
    conviction — the ghost's buckets refused the boot); each
    workflow-declared rate-limit INSTANCE (``TokenBucket``/
    ``SlidingWindow`` — on a ``step``, a map_source, or a ``RouteArm``)
    registers into the worker's resolved ``RateLimitRegistry``
    (``register()``'s own idempotency + the ``_same_config`` conflict
    refusal — the bootstrap's law); every plain-``str`` name that NO
    registry entry backs is REPORTED (the caller's WARNING — W2's own
    register: probably a typo, may be declared on another app the fleet
    serves, never a refusal). The keyed-ref shape cannot reach here
    (the wiring verbs refuse it).

    THE CONFLICT'S RENDER (the reservation face's own fix, mirrored):
    two LIVE workflows declaring one bucket name with different configs
    is the refused defect — the registry's own duplicate-name law — but
    the refusal names the RESOLVED BUCKET and the differing config
    fields, never the raw ``__slots__`` reprs the registry's message
    carries (an address is not a diagnosis).

    Returns ``(registered_names, unknown_names)`` — the registration
    log's and the warning's inputs. Call AFTER
    :func:`project_workflow_actor_configs` (the compiles are recorded).
    A workflow the projection skipped (a raising build fn, a validation
    refusal, the workflow's own queue conflict) skips LOUDLY here too
    (``workflow-rate-limit-collect-skipped`` — the reservations sibling's
    own isolation arm; the build fn is never re-invoked to resurrect
    it)."""
    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.ratelimit.token_bucket import TokenBucket

    registered: list[str] = []
    unknown: list[str] = []
    for app_face, name, compiled in _iter_live_recorded_workflows():
        if compiled is None:
            logger.error(
                "workflow-rate-limit-collect-skipped",
                app=app_face,
                workflow=name,
                error_class="KeyError",
                error=f"no recorded compile for {name!r}",
                remedy=(
                    "this workflow's rate limits are NOT collected and "
                    "its declared buckets will not register at boot; "
                    "the healthy workflows' buckets register unchanged"
                ),
            )
            continue
        decls: list[object] = []
        for node in compiled.nodes.values():
            decls.extend(node.rate_limits)
            decls.extend(node.map_rate_limits)
            if node.map_arms is not None:
                for arm in node.map_arms.values():
                    decls.extend(arm.rate_limits)
        for entry in decls:
            if isinstance(entry, TokenBucket | SlidingWindow):
                try:
                    registry.register(entry)
                except ValueError as exc:
                    # THE RESOLVED NAMES' REFUSAL (the round-3 mirror):
                    # re-render the registry's raw-repr message with the
                    # bucket's own name + the config diff, the original
                    # chained (``from exc`` — the cause is kept).
                    try:
                        existing = registry.get_rate_limit(entry.name)
                    except KeyError:
                        existing = None
                    where = (
                        _rate_limit_config_diff(existing, entry)
                        if existing is not None
                        else f"the registry refused it ({exc})"
                    )
                    raise ValueError(
                        f"workflow rate-limit bucket {entry.name!r} is declared "
                        f"with a config that conflicts with the bucket already "
                        f"registered under the same name ({where}) — one bucket "
                        "per name (the registry's own duplicate-name law); give "
                        "the differing node its own bucket."
                    ) from exc
                registered.append(entry.name)
            elif isinstance(entry, str) and not registry.has_rate_limit(entry):
                unknown.append(entry)
    return registered, sorted(set(unknown))


def collect_workflow_reservations() -> tuple[list[ConcurrencyReservation], list[str]]:
    """THE WORKFLOW-DECLARED RESERVATIONS' COLLECT (the vanilla actors'
    collection pass's sibling — ``_bootstrap``'s ``actor_ref.reservations``
    loop, over the projected graphs): every imported app's RECORDED
    compiled graphs walk once — the collect reads the projection's own
    ``record_compiled`` cache (the sibling
    :func:`collect_workflow_rate_limits`' shape; H-D1: the collect never
    re-invokes the build fn, the compile+validate doubling is dead);
    each node decl's ``ConcurrencyReservation`` INSTANCE is gathered —
    the ``@app.actor(reservations=[...])`` handle's terms flowing through
    the wiring into the decls, the step's own ``reservations=``
    beside it, the route arms' own declarations too — and every plain
    ``str`` name is GATHERED BESIDE THEM (C4 — the rate-limits collect's
    own second slot): the bootstrap warns when no registry entry backs
    the name (probably a typo, may be registered by a vanilla actor —
    never a refusal; the row still stamps the name, the claim-time
    fail-closed arm remains the enforcer). Call AFTER
    :func:`project_workflow_actor_configs` (the compiles are recorded).

    Returns ``(reservations, unknown_names)`` — the registration pass's
    and the warning's inputs.

    DEDUP BY NAME, DETERMINISTIC ORDER: the same instance (or an equal
    config) declared on several nodes is returned ONCE; the return is
    name-sorted, so the collection is byte-stable regardless of the app
    registry's iteration order. A name declared with TWO DIFFERENT
    configs is the refused defect (the registry's own
    duplicate-name-different-config law, pre-empted at the collect):
    ``ValueError``, both configs named — one reservation per name.
    THE DEDUP'S IDENTITY (H-A9): ``(name, slots, lease, keyed,
    lock_lease)`` — the identity that SURVIVES the rebind. The schema is
    EXCLUDED: the declaration's schema is the import-time default (the
    settings are unknowable there) and the boot's rebind owns the live
    binding — two same-name same-config differing-schema declarations
    are ONE reservation, not a conflict.

    The caller (the bootstrap) registers the returned instances into the
    resolved rate-limit registry (ONE registry, ONE acquire path) and
    feeds them to the SAME ensure-slots pass the vanilla reservations
    ride — their slot rows materialize at boot exactly like the vanilla
    ones'. Per-workflow build failures skip LOUDLY (the projection's own
    isolation arm — one broken builder must not refuse the boot; the
    projection has already named it). THE WALK'S BASE is the sibling
    rate-limit collect's own (:func:`_iter_live_recorded_workflows` —
    the LIVE apps' names over the recorded cache, ONE base the two
    collects share: the orphaned graphs' terms stop registering, the
    ghosts die)."""
    by_name: dict[str, ConcurrencyReservation] = {}
    unknown: list[str] = []

    def _take(entry: object) -> None:
        """THE DEDUP'S OWN TAKE (one home — the node decls AND the route
        arms' own declarations (H-A4's passthrough) feed the same
        name-keyed law; a plain ``str`` name is GATHERED (C4), never
        silently dropped)."""
        if isinstance(entry, ConcurrencyReservation):
            existing = by_name.get(entry.name)
            if existing is None:
                by_name[entry.name] = entry
            # THE DEDUP'S IDENTITY (H-A9): the fields that survive the
            # rebind — the schema is the rebind's own decision, never a
            # discriminator. A differing keyed/lock_lease is a materially
            # different bucket (the fleet-reclaim mark, the heartbeat's
            # extension), refused like any other config drift.
            elif (existing.slots, existing.lease, existing.keyed, existing.lock_lease) != (
                entry.slots,
                entry.lease,
                entry.keyed,
                entry.lock_lease,
            ):
                raise ValueError(
                    f"reservation name {entry.name!r} is declared with "
                    f"two different configs across workflow nodes "
                    f"(slots={existing.slots}, lease={existing.lease}, "
                    f"keyed={existing.keyed}, lock_lease="
                    f"{existing.lock_lease} vs slots={entry.slots}, "
                    f"lease={entry.lease}, keyed={entry.keyed}, "
                    f"lock_lease={entry.lock_lease}) — one "
                    "reservation per name (the registry's own "
                    "duplicate-name law); give the differing node its "
                    "own reservation."
                )
        elif isinstance(entry, str):
            unknown.append(entry)

    for app_face, name, compiled in _iter_live_recorded_workflows():
        if compiled is None:
            # NOT RECORDED = the projection skipped this workflow (a
            # raising build fn, a validation refusal, the workflow's
            # own queue conflict — the projection's isolation arm
            # named the defect THERE): the collect's isolation
            # matches (skip loudly, H-D1's cache-read shape — the
            # build fn is never re-invoked here to resurrect it),
            # the healthy workflows' reservations still collect.
            logger.error(
                "workflow-reservation-collect-skipped",
                app=app_face,
                workflow=name,
                error_class="KeyError",
                error=f"no recorded compile for {name!r}",
                remedy=(
                    "this workflow's reservations are NOT collected and "
                    "its declared slots will not materialize at boot; "
                    "the healthy workflows' reservations collect "
                    "unchanged"
                ),
            )
            continue
        for node in compiled.nodes.values():
            for entry in node.reservations:
                _take(entry)
            if node.map_arms is not None:
                for arm in node.map_arms.values():
                    # THE ARM'S OWN DECLARATION (H-A4): the arm's
                    # reservations collect beside the nodes'.
                    for entry in getattr(arm, "reservations", ()) or ():
                        _take(entry)
    return [by_name[res_name] for res_name in sorted(by_name)], sorted(set(unknown))


# ── the execution door ───────────────────────────────────────────────────

_runners: dict[tuple[str, str, str], tuple[Any, Any]] = {}
"""The worker-hosted runners, keyed (schema, workflow name, worker id) —
the worker's pool + identity are process-stable, so the cache holds ONE
runner per workflow per process (the construction validates the graph
once). The pool rides the value so a swapped pool rebuilds the runner."""


def _runner_for(name: str, pool: asyncpg.Pool, schema: str, worker_id: JobId) -> Any:
    from taskq.workflows.api._runner import FlowRunner

    key = (schema, name, str(worker_id))
    cached = _runners.get(key)
    if cached is not None and cached[0] is pool:
        return cached[1]
    compiled = get_compiled_workflow(name)
    # THE DEPS SEAM'S DOOR (the DI capability): the compiled carries the
    # app's bound instance (bound once at WorkflowApp(deps=…)) — the
    # worker-hosted runner hands the SAME instance to every body
    # invocation the vanilla door does (the E12 contract checked the
    # bodies' declarations against THIS binding at the projection's
    # compile).
    runner = FlowRunner(
        compiled,
        pool,
        schema,
        worker_id=worker_id,
        deps=compiled.deps if isinstance(compiled, CompiledWorkflow) else None,
    )
    _runners[key] = (pool, runner)
    return runner


async def execute_flow_job(
    *,
    pool: asyncpg.Pool,
    schema: str,
    worker_id: JobId,
    job: JobRow,
) -> FlowExecution:
    """Execute ONE claimed workflow row on THIS worker.

    *job* is the row the fleet's dispatch claimed (running, the attempt
    incremented, the epoch bumped, the lock held by *worker_id*). The
    door resolves the body from the REGISTERED definition — the flow
    root's stamped ``metadata.workflow`` name (D1, the same durable leg
    the leader's fire arm uses), one bounded primary-key read for the
    step identity + the stamp — and runs it through the runner's OWN
    machinery (:meth:`FlowRunner.run_fleet_claimed_step`): the ledger
    claim, the emitter, the router, the ladder, the two-transaction
    finalize, the same fences. NOT a second execution semantics.

    Returns the execution record (:class:`FlowExecution`) — the outcome
    label (``succeeded`` / ``laddered`` / ``held`` / ``deferred`` — the
    tail's outcome faces, the deferral the body's own control-flow
    signals) beside the REAL identities the door resolved on the way.

    Raises :class:`WorkflowBodyUnresolvableError` when the stamped name
    or the step key resolves to nothing registered in this process —
    the caller's defined behavior is the actor-not-found parking (the
    loud, budget-free snooze), never a silent wedge.
    """
    flow_raw = job.metadata.get("flow_id")
    if flow_raw is None:
        raise WorkflowBodyUnresolvableError(
            f"job {job.id} carries no flow_id in its metadata — the "
            "execution door is for workflow rows only"
        )
    async with pool.acquire() as conn:
        read = await conn.fetchrow(render_sql(_CLAIMED_NODE_READ_SQL_TEMPLATE, schema), job.id)
    if read is None or read["step_key"] is None:
        raise WorkflowBodyUnresolvableError(
            f"the claimed row {job.id} is gone or carries no step_key — "
            "the execution door resolves workflow rows only"
        )
    workflow_name = read["workflow"]
    if not workflow_name:
        raise WorkflowBodyUnresolvableError(
            f"the flow root of row {job.id} stamps no workflow name — "
            "the body resolves from the registered definition (D1), and "
            "the stamp is the resolution's durable leg"
        )
    try:
        get_compiled_workflow(workflow_name)
    except KeyError:
        raise WorkflowBodyUnresolvableError(
            f"workflow {workflow_name!r} is not compiled in this process "
            "— the definitions are imported by the projection's compile "
            "pass (D1); a row stamped with an unimported workflow names "
            "a deployment defect"
        ) from None

    runner = _runner_for(workflow_name, pool, schema, worker_id)
    # THE RUN-PRIORITY STAMP'S RESOLUTION: the root's stamped override
    # through the shared parse → domain-clamp → degrade cascade
    # (``resolve_priority_stamp`` — a stamp a hand-crafted row rendered
    # unparseable or out-of-domain degrades to NO override, never a door
    # death).
    raw_priority: object | None = read["wf_priority"]
    run_priority = resolve_priority_stamp(raw_priority)
    row = {
        "id": job.id,
        "step_key": read["step_key"],
        "map_index": read["map_index"],
        "payload": job.payload,
        "trace_id": job.trace_id,
    }
    from taskq.workflows.api._runner_errors import WorkflowRunError

    try:
        outcome = await runner.run_fleet_claimed_step(
            JobId(cast(UUID, flow_raw)),
            row,
            attempt=job.attempt,
            claim_epoch=job.claim_epoch,
            run_priority=run_priority,
        )
    except WorkflowRunError as exc:
        # THE UNRESOLVABLE STEP's parking shape (the deploy matrix's
        # fleet-crash cure): a step key this process's graph does not
        # carry (the root marker row, a version-skewed step) raised the
        # runner's WorkflowRunError — which ESCAPED the door and killed
        # the worker (the whole fleet died with it, every live run of
        # the estate stalled behind the corpse). The runner's OWN ladder
        # owns execution failures; only the RESOLUTION failure escapes,
        # and that one is the parking's shape — translate it, the
        # caller's defined snooze/release handles it (the loud, budget-
        # free parking, never a crash).
        raise WorkflowBodyUnresolvableError(
            f"row {job.id}'s step {read['step_key']!r} is unresolvable in this process: {exc}"
        ) from exc
    return FlowExecution(
        outcome=outcome,
        step_key=str(read["step_key"]),
        workflow_name=workflow_name,
        flow_id=str(flow_raw),
    )
