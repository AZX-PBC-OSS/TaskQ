"""THE KEYED ADMISSION TERMS ON THE WORKFLOW SURFACE — the declaration
accepts a keyed ref, the fork/static-insert resolves each child's
CONCRETE name (``f"{base_name}:{key}"``) from the child's own declared
payload, and the claim path materializes the named bucket against the
boot's collected decls.

The row truth the pins hold:

* two runs' map forks under one keyed rate limit stamp DISTINCT
  concrete bucket names on the child rows (per-tenant buckets are per
  child, never per declaration);
* the claim path denies PER KEY: one tenant's saturated bucket snoozes
  that tenant's children budget-free while the other tenant's children
  claim (the dispatch damper's own trade — keyed buckets never gate the
  actor's dispatch arithmetic);
* registry growth stays bounded by the same idle-eviction sweep the
  vanilla keyed acquires ride;
* a child whose key cannot resolve fails the fork LOUDLY (naming the
  child) — the row never lands half-stamped;
* the RESERVATION face is cohort-scoped: a fan-out source's children
  RIDE the source row's already-resolved concrete names (one bucket per
  run), and a keyed reservation that cannot bind the source's declared
  payload face is refused at the wiring;
* one base_name is one ref: a base_name that is a ``:``-prefix of
  another ref's is the refused registration (the longest-match
  re-binding would silently over-admit);
* the materialization's entry cap is the budget-free park at the gate,
  never a crash out of the intercept;
* the run's own INPUT participates in a parentless step's key
  resolution (one bucket per run — two runs, two inputs, two names);
  and the boot's own keyed registration is load-bearing (a forged row's
  concrete name resolves through the boot-registered registry).
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier, not user input; every value is $-bound.
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId, JobRow
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _flow_rate_limit_gate
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)
from tests.conftest import unique_health_sock_path

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_CAPABLE = UUID(int=7)
_JOB_LOG = structlog.get_logger("wf_keyed_admission_pins")


class Ingest(BaseModel):
    doc_id: str


class Item(BaseModel):
    tenant_id: str


def _loads(raw: object) -> object:
    return json.loads(raw) if isinstance(raw, str) else raw  # type: ignore[arg-type]


def _keyed_ref(
    base_name: str = "tenant-api", capacity: float = 1, refill: float = 0.0
) -> KeyedRateLimitRef:
    return KeyedRateLimitRef.typed(
        Item,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=capacity,
        refill_per_second=refill,
        backend="memory",
    )


def _keyed_job(n: int, tenant: str) -> JobRow:
    return JobRow(
        id=JobId(UUID(int=n)),
        actor="wf",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=_START,
        scheduled_at=_START,
        metadata={"flow_id": str(JobId(UUID(int=1))), "rate_limits": [f"tenant-api:{tenant}"]},
    )


# ── THE FORK RESOLVES PER CHILD ──────────────────────────────────────────


async def test_two_tenants_forks_stamp_distinct_concrete_buckets(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Two runs' map forks under ONE keyed rate limit: each child row's
    metadata carries ITS OWN tenant's concrete bucket name — the
    per-tenant buckets are per child, never per declaration."""
    app = WorkflowApp()
    keyed = _keyed_ref()

    async def src(ctx: StepContext, params: Ingest) -> list[Item]:
        return [Item(tenant_id="acme"), Item(tenant_id="globex")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id

    @app.workflow("wf_keyed_fork_stamp")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src")
        return build(map_source(source, item, rate_limits=[keyed]))

    runner = FlowRunner(app.get("wf_keyed_fork_stamp"), wf_pool, wf_schema)
    flow_one = (await runner.create_flow()).flow_id
    await runner.tick(flow_one)
    flow_two = (await runner.create_flow()).flow_id
    await runner.tick(flow_two)

    rows = await wf_pool.fetch(
        f"""SELECT map_index, metadata FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = ANY($1::uuid[])
              AND metadata ? 'flow_id' AND step_key = 'src.item'
            ORDER BY map_index""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        [str(flow_one), str(flow_two)],
    )
    assert len(rows) == 4, "both runs' forks created their children"
    stamped: dict[str, list[str]] = {}
    for r in rows:
        meta = _loads(r["metadata"])
        assert isinstance(meta, dict)
        names = meta.get("rate_limits")
        assert isinstance(names, list)
        stamped.setdefault(meta.get("flow_id", ""), []).append(str(names[0]))
    # BOTH runs, BOTH tenants: the concrete names are DISTINCT per child.
    assert sorted(stamped[str(flow_one)]) == ["tenant-api:acme", "tenant-api:globex"]
    assert sorted(stamped[str(flow_two)]) == ["tenant-api:acme", "tenant-api:globex"]


async def test_a_child_whose_key_cannot_resolve_fails_the_fork_loudly(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A keyed ref whose payload_type cannot validate a child's payload
    kills the fork LOUDLY (the error names the child) — the rows never
    land half-stamped."""
    app = WorkflowApp()

    class Other(BaseModel):
        v: int

    ref = KeyedRateLimitRef.typed(
        Other,
        base_name="mismatch-api",
        key_fn=lambda p: str(p.v),
        capacity=1,
        refill_per_second=0.0,
    )

    async def src(ctx: StepContext, params: Ingest) -> list[Item]:
        return [Item(tenant_id="acme")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id  # pragma: no cover - the fork never completes

    @app.workflow("wf_keyed_unresolvable")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src")
        return build(map_source(source, item, rate_limits=[ref]))

    runner = FlowRunner(app.get("wf_keyed_unresolvable"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # THE STEP IS NAMED: the death carries the map source's own key (the
    # `what`), never a diagnosis that cannot point at the child.
    with pytest.raises(Exception, match=r"'src'"):
        await runner.tick(flow_id)
    children = await wf_pool.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key = 'src.item'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    assert int(children or 0) == 0, "the failed fork wrote no half-stamped rows"


async def test_a_keyed_term_on_a_non_entry_step_is_refused_at_the_wiring() -> None:
    """A keyed ref on a node whose payload resolves from PARENT results
    has no payload at create — the wiring refuses it (the keyed shapes
    declare on the entry step or a fan-out, where a child payload
    exists)."""
    app = WorkflowApp()
    keyed = _keyed_ref()

    async def first(ctx: StepContext, params: Ingest) -> Item:
        return Item(tenant_id=params.doc_id)

    async def second(ctx: StepContext, value: Item) -> str:
        return value.tenant_id  # pragma: no cover - the wiring refuses first

    @app.workflow("wf_keyed_non_entry")
    def w() -> Promise[object]:
        one = step(first, Ingest(doc_id="d1"), key="one")
        return build(step(second, one, key="two", rate_limits=[keyed]))

    from taskq.workflows.api._graph import WorkflowBuildError

    with pytest.raises(WorkflowBuildError, match="keyed"):
        app.get("wf_keyed_non_entry")


# ── THE CLAIM PATH DENIES PER KEY ────────────────────────────────────────


def _settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("wf_keyed_admission_pins"),
        }
    )


def _deps(pool: asyncpg.Pool, schema: str) -> WorkerDeps:
    return WorkerDeps(
        settings=_settings(schema),
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )


async def test_one_tenant_s_saturated_bucket_snoozes_only_that_tenant(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The claim path's per-key denial: tenant acme's saturated bucket
    denies acme's child (the budget-free snooze — the gate's held-claim
    release), tenant globex's child claims through the SAME registry,
    the SAME gate. The dispatch damper's own trade, verified against wf
    rows: one saturated tenant must not gate the actor."""
    clock = FakeClock(_START)
    registry = RateLimitRegistry()
    registry.register_keyed_rate_limit_ref(_keyed_ref())
    deps = _deps(wf_pool, wf_schema)
    job_log = _JOB_LOG

    acme = _keyed_job(101, "acme")
    globex = _keyed_job(202, "globex")

    # acme's first child claims (capacity 1 — the bucket drains its
    # token), globex's child claims beside it (its own bucket).
    first = await _flow_rate_limit_gate(registry, clock, acme, _CAPABLE, deps, job_log=job_log)
    assert first.dependency_failure is None and first.denied is None, (
        f"acme's first child must claim: {first.dependency_failure} {first.denied}"
    )
    other = await _flow_rate_limit_gate(registry, clock, globex, _CAPABLE, deps, job_log=job_log)
    assert other.dependency_failure is None and other.denied is None, (
        f"globex's child must claim: {other.dependency_failure} {other.denied}"
    )
    # acme's SECOND child hits ITS OWN tenant's saturated bucket — the
    # denial names the CONCRETE bucket and the snooze is budget-free
    # (the recorded denials counter, the held claim's release).
    denials: list[str] = []
    with patch("taskq.worker.run.record_ratelimit_denial", side_effect=denials.append):
        second = await _flow_rate_limit_gate(registry, clock, acme, _CAPABLE, deps, job_log=job_log)
    assert second.denied is not None, "acme's saturated bucket must deny acme's second child"
    assert "tenant-api:acme" in str(second.denied)
    assert denials, "the deny is observable (the denials counter bumped)"
    # And a THIRD tenant's child still claims after acme's denial (the
    # denial is per key, never per actor — the saturated bucket gates
    # only its own key's children).
    newcomer = await _flow_rate_limit_gate(
        registry, clock, _keyed_job(203, "newcomer"), _CAPABLE, deps, job_log=job_log
    )
    assert newcomer.dependency_failure is None and newcomer.denied is None, (
        "a fresh tenant's child claims through the same gate after the denial"
    )


async def test_registry_growth_is_bounded_by_the_idle_eviction(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The wf claims' keyed buckets ride the SAME growth bound as the
    vanilla keyed acquires: materialized concrete entries evict when
    idle, and re-materialize on the next claim. (A REFILLING bucket is
    the evictable shape: a memory fixed-quota bucket that consumed
    quota is exempt — eviction would hand back a budget the tenant
    already spent.)"""
    clock = FakeClock(_START)
    registry = RateLimitRegistry()
    registry.register_keyed_rate_limit_ref(_keyed_ref(capacity=2, refill=0.1))
    deps = _deps(wf_pool, wf_schema)
    job_log = _JOB_LOG

    for i in range(5):
        gate = await _flow_rate_limit_gate(
            registry, clock, _keyed_job(300 + i, f"t{i}"), _CAPABLE, deps, job_log=job_log
        )
        assert gate.dependency_failure is None and gate.denied is None
    assert registry.has_rate_limit("tenant-api:t0"), "the concrete bucket materialized"

    evicted = registry.evict_idle_keyed_rate_limits(timedelta(0))
    assert evicted >= 5, "the idle keyed entries evict (the growth bound)"
    assert not registry.has_rate_limit("tenant-api:t0")

    # The next claim re-materializes (the bound is a recycle, never a
    # lockout).
    gate = await _flow_rate_limit_gate(
        registry, clock, _keyed_job(400, "t0"), _CAPABLE, deps, job_log=job_log
    )
    assert gate.dependency_failure is None and gate.denied is None, (
        "the evicted key re-materializes on the next claim"
    )
    assert registry.has_rate_limit("tenant-api:t0")


# ── THE RESERVATION FACE IS COHORT-SCOPED ────────────────────────────────


async def test_a_fan_out_source_s_keyed_reservation_rides_the_run_s_cohort(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The fan-out's RESERVATION face: the source's keyed reservation
    resolves ONCE (from the run's input and the source step's declared
    args) onto the source row, and the children RIDE the source row's
    already-resolved names — ONE concrete bucket for the run, never a
    per-item re-derivation."""
    from taskq.ratelimit.refs import KeyedReservationRef

    keyed = KeyedReservationRef.typed(
        Ingest,
        base_name="run-guard",
        key_fn=lambda p: p.doc_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[Item]:
        return [Item(tenant_id="acme"), Item(tenant_id="globex")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id

    @app.workflow("wf_keyed_res_rides")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="run-42"), key="src", reservations=[keyed])
        return build(map_source(source, item))

    runner = FlowRunner(app.get("wf_keyed_res_rides"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)

    rows = await wf_pool.fetch(
        f"""SELECT step_key, map_index, metadata FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key IN ('src', 'src.item')
            ORDER BY step_key, map_index NULLS FIRST""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    assert len(rows) == 3, "the source row and both children exist"
    for r in rows:
        meta = _loads(r["metadata"])
        assert isinstance(meta, dict)
        assert meta.get("reservations") == ["run-guard:run-42"], (
            f"step {r['step_key']}[{r['map_index']}] must ride the run's own "
            f"reservation cohort; got {meta.get('reservations')}"
        )

    # THE FLEET DOOR'S OWN SHAPE: the second run's source claims through
    # the production dispatch and executes through execute_flow_job —
    # the fork's row read there carries the metadata DICT (the dispatch
    # decode's), the same ride.
    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.workflows._worker_execution import execute_flow_job
    from tests.conftest import unique_health_sock_path as _sock

    fleet_settings = WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": wf_schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "TASKQ_HEALTH_SOCKET_PATH": _sock("wf_keyed_admission_pins"),
        }
    )
    fleet_deps = WorkerDeps(
        settings=fleet_settings,
        dispatcher_pool=wf_pool,
        heartbeat_pool=wf_pool,
        worker_pool=wf_pool,
        notify_conn=None,
        leader_conn=None,
    )
    backend = PostgresBackend(
        fleet_deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )
    fleet_worker = new_uuid()
    await wf_pool.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    await wf_pool.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'wf-keyed-pin', 1, '{default}', $2::jsonb)",
        fleet_worker,
        json.dumps({"workflow_execution": True}),
    )
    flow_two = (await runner.create_flow()).flow_id
    claimed = await backend.dispatch_batch(
        fleet_worker, ["default"], limit=5, lock_lease=timedelta(seconds=30)
    )
    source_claims = [j for j in claimed if str((j.metadata or {}).get("flow_id")) == str(flow_two)]
    assert source_claims, "the fleet round claimed the second run's source"
    for job in source_claims:
        execution = await execute_flow_job(
            pool=wf_pool, schema=wf_schema, worker_id=JobId(fleet_worker), job=job
        )
        assert execution.outcome == "succeeded", execution
    fleet_rows = await wf_pool.fetch(
        f"""SELECT step_key, metadata FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key IN ('src', 'src.item')""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_two,
    )
    assert len(fleet_rows) == 3, "the fleet door's fork created the children"
    for r in fleet_rows:
        meta = _loads(r["metadata"])
        assert isinstance(meta, dict)
        assert meta.get("reservations") == ["run-guard:run-42"], (
            f"the fleet door's fork must ride the source row's cohort; got "
            f"{meta.get('reservations')}"
        )


def test_a_keyed_reservation_on_a_fan_out_source_without_the_run_s_face_is_refused() -> None:
    """The wiring door: a keyed reservation on a node that is also a
    map/route source, whose payload_type binds NONE of the source
    step's declared payload face, is the refused declaration — the
    error states the rule (the children ride the source row's
    resolved names; the per-item shape it implies is not honored)."""
    from taskq.ratelimit.refs import KeyedReservationRef
    from taskq.workflows.api._graph import WorkflowBuildError

    keyed = KeyedReservationRef.typed(
        Item,
        base_name="item-guard",
        key_fn=lambda p: p.tenant_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[Item]:
        return [Item(tenant_id="acme")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id  # pragma: no cover - the wiring refuses first

    @app.workflow("wf_keyed_res_refused")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src", reservations=[keyed])
        return build(map_source(source, item))

    with pytest.raises(WorkflowBuildError, match=r"ride|per-item|run-scoped|run's"):
        app.get("wf_keyed_res_refused")


def test_a_keyed_reservation_on_a_route_arm_is_refused() -> None:
    """The route arm's face: an arm declares no payload of its own — a
    keyed reservation in an arm's ``reservations=`` is the refused
    declaration (the rule stated: the source step's own face resolves
    the run's cohort; the arms ride it)."""
    from taskq.ratelimit.refs import KeyedReservationRef
    from taskq.workflows import RouteArm
    from taskq.workflows.api._graph import WorkflowBuildError

    keyed = KeyedReservationRef.typed(
        Item,
        base_name="arm-guard",
        key_fn=lambda p: p.tenant_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[Item]:
        return [Item(tenant_id="acme")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id  # pragma: no cover - the wiring refuses first

    @app.workflow("wf_keyed_res_arm_refused")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src")
        return build(map_source(source, {Item: RouteArm(body=item, reservations=[keyed])}))

    with pytest.raises(WorkflowBuildError, match=r"arm|ride|per-item|run-scoped|run's"):
        app.get("wf_keyed_res_arm_refused")


# ── ONE BASE_NAME, ONE REF (the prefix-collision refusal) ────────────────


def test_a_base_name_that_is_a_prefix_of_another_ref_s_is_refused() -> None:
    """The registry's keyed-name space is unambiguous: a base_name that
    is a ``:``-prefix of another registered ref's base_name would let
    the longest-match re-bind a fork-stamped child to the WRONG ref's
    config (the silent over-admission) — the SECOND registration
    refuses, naming both refs."""
    registry = RateLimitRegistry()
    registry.register_keyed_rate_limit_ref(_keyed_ref("tenant"))
    with pytest.raises(ValueError, match=r"tenant"):
        registry.register_keyed_rate_limit_ref(_keyed_ref("tenant:api"))


def test_the_prefix_refusal_holds_in_the_reverse_registration_order() -> None:
    """The same refusal when the LONG base_name registers first and the
    SHORT one arrives second (the check is symmetric)."""
    registry = RateLimitRegistry()
    registry.register_keyed_rate_limit_ref(_keyed_ref("tenant:api"))
    with pytest.raises(ValueError, match=r"tenant"):
        registry.register_keyed_rate_limit_ref(_keyed_ref("tenant"))


def test_the_prefix_refusal_on_the_reservation_face() -> None:
    """The reservation refs' own name space keeps the same law."""
    from taskq.ratelimit.refs import KeyedReservationRef

    registry = RateLimitRegistry()

    def res(base_name: str) -> KeyedReservationRef:
        return KeyedReservationRef.typed(
            Ingest,
            base_name=base_name,
            key_fn=lambda p: p.doc_id,
            slots=1,
            lease=timedelta(seconds=30),
        )

    registry.register_keyed_reservation_ref(res("sess"))
    with pytest.raises(ValueError, match=r"sess"):
        registry.register_keyed_reservation_ref(res("sess:api"))


def test_distinct_base_names_that_only_share_a_text_prefix_register_clean() -> None:
    """The ':' delimiter is the law's own boundary: ``tenant`` and
    ``tenantx`` share a TEXT prefix but no concrete-name collision
    (``tenant:<key>`` can never equal ``tenantx:<key>``) — both
    register."""
    registry = RateLimitRegistry()
    registry.register_keyed_rate_limit_ref(_keyed_ref("tenant"))
    registry.register_keyed_rate_limit_ref(_keyed_ref("tenantx"))


# ── THE ENTRY CAP PARKS, NEVER CRASHES ───────────────────────────────────


async def test_the_materialization_s_entry_cap_is_the_budget_free_park(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The keyed materialization's entry cap (the settings ceiling)
    denies at the GATE — the budget-free park the denial verdict
    carries — never an exception out of the intercept (a raise would
    crash the worker's loop and strand the claimed row)."""
    registry = RateLimitRegistry()
    registry.register_keyed_rate_limit_ref(_keyed_ref(capacity=2, refill=0.1))
    settings = WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": wf_schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("wf_keyed_admission_pins"),
            "max_keyed_rate_limits": 1,
        }
    )
    capped = WorkerDeps(
        settings=settings,
        dispatcher_pool=wf_pool,
        heartbeat_pool=wf_pool,
        worker_pool=wf_pool,
        notify_conn=None,
        leader_conn=None,
    )
    first = await _flow_rate_limit_gate(
        registry, FakeClock(_START), _keyed_job(501, "a"), _CAPABLE, capped, job_log=_JOB_LOG
    )
    assert first.dependency_failure is None and first.denied is None, "the first key materializes"
    denials: list[str] = []
    with patch("taskq.worker.run.record_ratelimit_denial", side_effect=denials.append):
        second = await _flow_rate_limit_gate(
            registry, FakeClock(_START), _keyed_job(502, "b"), _CAPABLE, capped, job_log=_JOB_LOG
        )
    assert second.denied is not None, (
        "the cap hit is the denial verdict at the gate (the budget-free "
        "park) — never a raise out of the gate"
    )
    assert denials, "the deny is observable"
    assert second.dependency_failure is None, "the cap is not a dependency failure"


# ── THE RUN'S INPUT PARTICIPATES IN A PARENTLESS STEP'S KEY ──────────────


async def test_two_runs_with_different_inputs_get_distinct_concrete_buckets(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A parentless step's keyed resolution reads the RUN'S OWN INPUT
    first (the run's declared face): two runs of one workflow, two
    inputs, two concrete bucket names on the entry rows — one bucket
    per run."""
    from taskq.ratelimit.refs import KeyedRateLimitRef

    class RunInput(BaseModel):
        tenant_id: str

    run_keyed = KeyedRateLimitRef.typed(
        RunInput,
        base_name="tenant-api",
        key_fn=lambda p: p.tenant_id,
        capacity=1,
        refill_per_second=0.0,
        backend="memory",
    )
    app = WorkflowApp()

    async def entry(ctx: StepContext) -> str:
        return str(ctx.input)  # pragma: no cover - the runner drives real bodies

    @app.workflow("wf_keyed_run_input")
    def w() -> Promise[object]:
        return build(step(entry, key="entry", rate_limits=[run_keyed]))

    runner = FlowRunner(app.get("wf_keyed_run_input"), wf_pool, wf_schema)
    flow_one = (await runner.create_flow(input=RunInput(tenant_id="acme"))).flow_id
    flow_two = (await runner.create_flow(input=RunInput(tenant_id="globex"))).flow_id

    stamps: dict[str, str] = {}
    for tenant, fid in (("acme", str(flow_one)), ("globex", str(flow_two))):
        raw = await wf_pool.fetchval(
            f"""SELECT metadata FROM "{wf_schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND metadata ? 'flow_id' AND step_key = 'entry'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
            fid,
        )
        assert raw is not None, f"the {tenant} run's entry row exists"
        meta = _loads(raw)
        assert isinstance(meta, dict)
        names = meta.get("rate_limits")
        assert isinstance(names, list) and names, f"the {tenant} run's entry row carries its bucket"
        stamps[tenant] = str(names[0])
    assert stamps["acme"] == "tenant-api:acme", stamps
    assert stamps["globex"] == "tenant-api:globex", stamps


# ── THE BOOT'S KEYED REGISTRATION IS LOAD-BEARING ────────────────────────


async def test_a_forged_row_s_concrete_name_resolves_through_the_boot_s_registry(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The boot's keyed registration (the collect pass) is the claim
    path's resolution source: an app declaring a keyed ref, the collect
    run, then a FORGED row carrying a concrete ``base:key`` name — the
    gate resolves it through the boot-registered registry (the bucket
    materializes) instead of the fail-closed park."""
    from taskq.workflows._worker_execution import (
        collect_workflow_rate_limits,
        project_workflow_actor_configs,
        register_app,
    )

    app = WorkflowApp()

    async def src(ctx: StepContext) -> list[Item]:
        return [Item(tenant_id="acme")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id  # pragma: no cover - the gate is the subject

    @app.workflow("wf_keyed_boot_collect")
    def w() -> Promise[object]:
        source = step(src, key="src")
        return build(map_source(source, item, rate_limits=[_keyed_ref()]))

    # THE BOOT'S OWN ORDER (H-D1): register, project (the compile records),
    # then the collects read the recorded cache.
    register_app(app)
    project_workflow_actor_configs()
    registry = RateLimitRegistry()
    collect_workflow_rate_limits(registry)

    gate = await _flow_rate_limit_gate(
        registry,
        FakeClock(_START),
        _keyed_job(601, "acme"),
        _CAPABLE,
        _deps(wf_pool, wf_schema),
        job_log=_JOB_LOG,
    )
    assert gate.dependency_failure is None and gate.denied is None, (
        "the forged row's concrete name resolves through the boot's keyed "
        f"registration; got {gate.dependency_failure} {gate.denied}"
    )


# ── THE FAN-OUT COHORT HOLDS AT DEPTH-N ──────────────────────────────────


async def test_a_nested_map_s_depth_2_children_ride_the_run_s_cohort(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A map over a map's JOIN: the join row itself carries the source
    row's resolved reservation names (the join rides the run's cohort),
    so the depth-2 fork reads the stamp and the nested children carry
    the run's own names — the cohort guarantee is depth-N, never
    depth-1."""
    from taskq.ratelimit.refs import KeyedReservationRef

    class Nested(BaseModel):
        tenant_id: str
        seq: int = 0

    keyed = KeyedReservationRef.typed(
        Ingest,
        base_name="run-guard",
        key_fn=lambda p: p.doc_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[Nested]:
        return [Nested(tenant_id="acme"), Nested(tenant_id="globex")]

    async def item(ctx: StepContext, value: Nested) -> list[Nested]:
        return [Nested(tenant_id=value.tenant_id, seq=value.seq + 100)]

    async def item2(ctx: StepContext, value: Nested) -> str:
        return value.tenant_id

    @app.workflow("wf_keyed_nested_map")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="deep-9"), key="src", reservations=[keyed])
        level1 = map_source(source, item)
        return build(map_source(level1, item2))

    runner = FlowRunner(app.get("wf_keyed_nested_map"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)

    # The depth-1 join row rides the run's cohort (the fork stamped it).
    join_stamp = await wf_pool.fetchval(
        f"""SELECT metadata->'reservations' FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key = 'src.join'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    join_names = _loads(join_stamp) if join_stamp is not None else None
    assert join_names == ["run-guard:deep-9"], (
        f"the join row rides the run's cohort; got {join_names}"
    )

    # Drive the depth-1 children to their finalizes: the join fires, the
    # join's own finalize forks the depth-2 children.
    level2 = 0
    for _ in range(6):
        await runner.tick(flow_id)
        level2 = int(
            await wf_pool.fetchval(
                f"""SELECT count(*) FROM "{wf_schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid
                      AND metadata ? 'flow_id' AND step_key = 'src.join.item'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
                flow_id,
            )
            or 0
        )
        if level2 >= 2:
            break
    assert level2 >= 2, "the nested fork created the depth-2 children"

    stamps = await wf_pool.fetch(
        f"""SELECT map_index, metadata->'reservations' AS reservations
            FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key = 'src.join.item'
            ORDER BY map_index""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    assert len(stamps) >= 2, "the depth-2 children exist"
    for r in stamps:
        names = _loads(r["reservations"]) if r["reservations"] is not None else None
        assert names == ["run-guard:deep-9"], (
            f"the depth-2 child {r['map_index']} must ride the run's own "
            f"reservation cohort; got {names}"
        )


def test_an_actor_overriding_arm_refuses_the_source_s_keyed_reservation() -> None:
    """The route arm's placement override (a different actor) drops the
    source's stamped reservation names — the arm's children would run
    OUTSIDE the run's cap. The same H-A4 law the capped handle keeps
    (an override may not drop a cap silently): the wiring refuses, the
    rule stated."""
    from taskq.ratelimit.refs import KeyedReservationRef
    from taskq.workflows import RouteArm
    from taskq.workflows.api._graph import WorkflowBuildError

    keyed = KeyedReservationRef.typed(
        Ingest,
        base_name="override-guard",
        key_fn=lambda p: p.doc_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[Item]:
        return [Item(tenant_id="acme")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id  # pragma: no cover - the wiring refuses first

    @app.workflow("wf_keyed_arm_override_refused")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src", reservations=[keyed])
        return build(map_source(source, {Item: RouteArm(body=item, actor="other-actor")}))

    with pytest.raises(WorkflowBuildError, match=r"override|drop|cap|cohort"):
        app.get("wf_keyed_arm_override_refused")


# ── THE ADOPTED JOIN RIDES THE COHORT (the depth-N induction's other face) ──


async def test_an_adopted_join_carries_the_stamp_and_feeds_depth_3(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The ADOPTED join (a pre-created row the fork's probe finds — the
    drain-precreated shape: the flow link + the stamp, no join-wait
    marker) must leave the adoption UPDATE shaped EXACTLY like the
    non-adopted join: the join-wait marker ON, the run's cohort ON. A
    THIRD map over that join then reads the stamp and its children ride
    the run's cap — the depth-3 face."""
    from taskq.ratelimit.refs import KeyedReservationRef

    class Nested(BaseModel):
        tenant_id: str
        seq: int = 0

    keyed = KeyedReservationRef.typed(
        Ingest,
        base_name="run-guard",
        key_fn=lambda p: p.doc_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[Nested]:
        return [Nested(tenant_id="acme"), Nested(tenant_id="globex")]

    async def item(ctx: StepContext, value: Nested) -> Nested:
        return Nested(tenant_id=value.tenant_id, seq=value.seq + 100)

    async def item2(ctx: StepContext, value: Nested) -> Nested:
        return Nested(tenant_id=value.tenant_id, seq=value.seq + 200)

    async def item3(ctx: StepContext, value: Nested) -> str:
        return value.tenant_id

    @app.workflow("wf_keyed_adopted_join")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="deep-9"), key="src", reservations=[keyed])
        level1 = map_source(source, item)
        level2 = map_source(level1, item2)
        return build(map_source(level2, item3))

    runner = FlowRunner(app.get("wf_keyed_adopted_join"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # THE ADOPTION IS NATURAL HERE: the nested map's join node
    # ('src.join.join', kind map_join) is SKIPPED by the static insert,
    # so the fired depth-1 join's outbox drain pre-creates it (the
    # consumer row: the flow link + the cohort stamp, NO join-wait
    # marker, the static-shape idempotency key). The depth-2 fork's
    # adoption probe finds THAT row and adopts it.
    level3 = 0
    for _ in range(12):
        await runner.tick(flow_id)
        level3 = int(
            await wf_pool.fetchval(
                f"""SELECT count(*) FROM "{wf_schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid
                      AND metadata ? 'flow_id' AND step_key = 'src.join.join.item'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
                flow_id,
            )
            or 0
        )
        if level3 >= 2:
            break
    assert level3 >= 2, "the depth-3 fork created its children"

    # THE ADOPTED JOIN'S SHAPE: exactly the non-adopted join's — the
    # join-wait marker AND the run's cohort, ON THE ADOPTED ROW.
    adopted = await wf_pool.fetchrow(
        f"""SELECT metadata->'reservations' AS res, metadata->>'blocking_reason' AS reason
            FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key = 'src.join.join'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    assert adopted is not None
    names = _loads(adopted["res"]) if adopted["res"] is not None else None
    assert names == ["run-guard:deep-9"], (
        f"the ADOPTED join row carries the run's cohort; got {names}"
    )
    assert adopted["reason"] == "join", (
        f"the adopted join row carries the join-wait marker (the non-adopted "
        f"join's shape); got {adopted['reason']!r}"
    )

    # THE DEPTH-3 CHILDREN: the adopted join's own stamp feeds their ride.
    depth3 = await wf_pool.fetch(
        f"""SELECT map_index, metadata->'reservations' AS reservations
            FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key = 'src.join.join.item'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    for r in depth3:
        names = _loads(r["reservations"]) if r["reservations"] is not None else None
        assert names == ["run-guard:deep-9"], (
            f"the depth-3 child {r['map_index']} must ride the run's own "
            f"cohort through the ADOPTED join's stamp; got {names}"
        )


# ── THE OVERRIDE'S CAP DOOR, THE ARM'S OWN NAMES FACE ────────────────────


def test_an_arm_with_its_own_reservation_names_refuses_over_a_keyed_source() -> None:
    """A same-actor arm declaring its OWN reservation names drops the
    source's keyed run cap (the arm's rows stamp the arm's static names,
    never the run's keyed bucket) — the same override law's face: the
    run's cap holds or the wiring refuses. The error states the two
    options (keep the source's, or move the arm to its own step)."""
    from taskq.ratelimit.refs import KeyedReservationRef
    from taskq.ratelimit.reservation import ConcurrencyReservation
    from taskq.workflows import RouteArm
    from taskq.workflows.api._graph import WorkflowBuildError

    keyed = KeyedReservationRef.typed(
        Ingest,
        base_name="override-guard",
        key_fn=lambda p: p.doc_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    own = ConcurrencyReservation("own-guard", slots=2, lease=timedelta(seconds=30))
    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[Item]:
        return [Item(tenant_id="acme")]

    async def item(ctx: StepContext, value: Item) -> str:
        return value.tenant_id  # pragma: no cover - the wiring refuses first

    @app.workflow("wf_keyed_arm_own_res_refused")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src", reservations=[keyed])
        return build(map_source(source, {Item: RouteArm(body=item, reservations=[own])}))

    with pytest.raises(WorkflowBuildError, match=r"cap|cohort|own step"):
        app.get("wf_keyed_arm_own_res_refused")
