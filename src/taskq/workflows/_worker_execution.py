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
from typing import TYPE_CHECKING, Any, NamedTuple, cast
from uuid import UUID

from taskq.actor_config import ActorConfig
from taskq.backend._protocol import JobId, JobRow
from taskq.obs import get_logger
from taskq.ratelimit.registry import RateLimitRegistry
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
    "WorkflowActorQueueConflictError",
    "WorkflowBodyUnresolvableError",
    "collect_workflow_rate_limits",
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
#: workflow name, ONE bounded primary-key round trip per claimed flow
#: row (the same cost class the leader's fire arm pays for the same
#: stamped name).
_CLAIMED_NODE_READ_SQL_TEMPLATE = """
SELECT j.step_key, j.map_index,
       root.metadata->>'workflow' AS workflow
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


# ── the boot projection (the F3 law's call site) ─────────────────────────


def _workflow_cohorts(compiled: Any) -> set[tuple[str, str]]:
    """The (actor, queue) cohorts *compiled*'s rows can stamp: the wired
    nodes' decls, the map attachments' child placement (``map_queue`` —
    the fork's children ride it), the chains' step rows (every row of a
    chain — the emitted starts and the forked children alike — lands on
    the chain's own pair)."""
    cohorts: set[tuple[str, str]] = set()
    for node in compiled.nodes.values():
        cohorts.add((node.actor, node.queue))
        if node.map_item is not None:
            cohorts.add((node.actor, node.map_queue))
    for chain in getattr(compiled, "chains", ()):
        for _step_key in chain.steps:
            cohorts.add((chain.actor, chain.queue))
    return cohorts


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

    DETERMINISTIC ORDER (sorted): the sync's drift comparison and the
    event stream read the projection — a stable order is the readable
    one.
    """
    totals: dict[str, tuple[str, set[str]]] = {}
    for app in iter_imported_apps():
        workflow_app = cast("WorkflowAppFace", app)
        app_face = f"{type(app).__module__}.{type(app).__name__}"
        for name in sorted(workflow_app.workflow_names()):
            try:
                compiled = workflow_app.get(name)
                record_compiled(name, compiled)
                workflow_cohorts: dict[str, set[str]] = {}
                for actor, queue in sorted(_workflow_cohorts(compiled)):
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
            except Exception as exc:  # Why: the isolation IS the cure (F2-3) — one broken build fn (a raising builder, a validation refusal, the workflow's own cohort conflict) must not refuse the worker's whole boot; the failure is the same skip-loudly arm for every shape.
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
    return [
        ActorConfig(
            actor=actor,
            max_concurrent=None,
            queue=queue,
            metadata=dict(_PROJECTION_METADATA),
        )
        for actor, (queue, _seen) in sorted(totals.items())
    ]


# ── the boot's rate-limit collection (CURE 2) ────────────────────────────


def collect_workflow_rate_limits(registry: RateLimitRegistry) -> tuple[list[str], list[str]]:
    """THE BOOT'S COLLECT (the vanilla actors' collection pass's sibling
    — the consumer-face lane's CURE 2): every imported app's compiled
    graphs walk once; each workflow-declared rate-limit INSTANCE
    (``TokenBucket``/``SlidingWindow`` — on a ``step``, a map_source, or
    a ``RouteArm``) registers into the worker's resolved
    ``RateLimitRegistry`` (``register()``'s own idempotency + the
    ``_same_config`` conflict refusal — the bootstrap's law); every
    plain-``str`` name that NO registry entry backs is REPORTED (the
    caller's WARNING — W2's own register: probably a typo, may be
    declared on another app the fleet serves, never a refusal). The
    keyed-ref shape cannot reach here (the wiring verbs refuse it).

    Returns ``(registered_names, unknown_names)`` — the registration
    log's and the warning's inputs. Call AFTER
    :func:`project_workflow_actor_configs` (the compiles are recorded)."""
    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.ratelimit.token_bucket import TokenBucket

    registered: list[str] = []
    unknown: list[str] = []
    for name in sorted(_compiled):
        compiled = _compiled[name]
        if not isinstance(compiled, CompiledWorkflow):
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
                registry.register(entry)
                registered.append(entry.name)
            elif isinstance(entry, str) and not registry.has_rate_limit(entry):
                unknown.append(entry)
    return registered, sorted(set(unknown))


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
    label (``succeeded`` / ``laddered`` / ``held`` — the tail's terminal
    faces) beside the REAL identities the door resolved on the way.

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
