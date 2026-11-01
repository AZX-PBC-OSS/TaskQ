"""THE CAPACITY ENFORCEMENT PINS (§H-A's claim-path + finalize faces):

* H-A1 — THE WF RESERVATIONS ARE ACQUIRED: the flow rows carry the
  step's declared reservation NAMES on their metadata (the static node
  inserts + the fork's children, the ``rate_limits`` stamp's own
  five-point ride: stamp → resolve → acquire → release → the fail-closed
  arms), and the flow gate RESOLVES those names through the resolved
  registry (the instances live there — registered at boot from
  ``collect_workflow_reservations`` with the rebind) and acquires them
  with the queue-cap's own denial discipline. A slots=1 reservation
  shared by two claimed flow rows runs exactly ONE body at a time: the
  second gate DENIES (the row snoozed budget-free, its release_reason
  naming the reservation), and the release re-opens the gate. The
  overlap-guard tripwire (the flows demo's pattern) never fires.
* H-A5 — result_ttl IS A LIVE KNOB: the wf finalize's terminal mark
  writes ``result_expires_at`` on the SUCCEEDED terminal rows from the
  cohort's stored ``actor_config.result_ttl`` (the vanilla
  ``mark_succeeded``'s lateral equivalent); a cohort with no TTL writes
  NULL, and a FAILED terminal never stamps an expiry (the vanilla
  ``mark_failed``'s parity).

The reservations are PG-BACKED (the production shape — the boot's
``sync_slots`` materializes the slot rows, the acquire's lease/reclaim
read them), on the module's OWN schema.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.backend._protocol import JobId, JobRow
from taskq.backend.clock import Clock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation, sync_slots
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _dispatch_flow_job, _flow_rate_limit_gate
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    sink,
    step,
)

_START = datetime(2025, 1, 1, tzinfo=UTC)

CAPABLE_WORKER = UUID(int=7)

JOB_LOG = structlog.get_logger("wf_capacity_enforcement_pins")

#: The pins' shared reservation name (the row-stamped bucket).
GUARD_NAME = "pin_wf_capacity_guard"


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


def _settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
        }
    )


def _deps(real_pool: asyncpg.Pool, schema: str) -> WorkerDeps:
    return WorkerDeps(
        settings=_settings(schema),
        dispatcher_pool=real_pool,
        heartbeat_pool=real_pool,
        worker_pool=real_pool,
        notify_conn=None,
        leader_conn=None,
    )


def _guard(schema: str, clock: Clock, *, slots: int = 1) -> ConcurrencyReservation:
    return ConcurrencyReservation(
        GUARD_NAME,
        slots=slots,
        lease=timedelta(seconds=30),
        clock=clock,
        schema=schema,
    )


async def _registered_guard(
    schema: str, clock: Clock, pool: asyncpg.Pool, *, slots: int = 1
) -> tuple[RateLimitRegistry, ConcurrencyReservation]:
    """The resolved registry the boot would have assembled: the wf
    reservation registered (the rebind's own shape) + the boot's own
    slot sync (the acquire has no ensure-slots retry)."""
    registry = RateLimitRegistry()
    reservation = _guard(schema, clock, slots=slots)
    registry.register(reservation.rebind_schema(schema))
    await sync_slots([reservation], pool, schema=schema)
    return registry, reservation


def _flow_job(
    flow_id: JobId, node_id: JobId, payload: object, *, extra_meta: dict[str, object] | None = None
) -> JobRow:
    metadata: dict[str, object] = {"flow_id": str(flow_id)}
    metadata.update(extra_meta or {})
    return JobRow(
        id=node_id,
        actor="wf-pin",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload=payload,  # type: ignore[arg-type]  # Why: the door's payload is the engine's envelope dict — the test's literal is that shape.
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
        metadata=metadata,
        trace_id=None,
    )


def _door_seam() -> ModuleType:
    from taskq.workflows import _worker_execution

    return _worker_execution


class _NoEnqueuer:
    """The door's enqueuer knob: the pin's flow writes no sub-jobs."""


class OverlapGuard:
    """The flows demo's tripwire pattern: a second entry while one is in
    flight raises — the not-thread-safe resource's scream."""

    def __init__(self) -> None:
        self.in_flight = 0
        self.tripped: list[str] = []
        self.entries = 0

    async def enter(self, stage: str) -> None:
        self.entries += 1
        self.in_flight += 1
        try:
            if self.in_flight > 1:
                self.tripped.append(stage)
                raise RuntimeError(f"the not-thread-safe resource was entered twice ({stage})")
            await asyncio.sleep(0.15)
        finally:
            self.in_flight -= 1


# ── H-A1: the stamp ──────────────────────────────────────────────────────


@pytest.mark.integration
async def test_the_reservation_stamp_rides_the_static_and_fork_rows(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE STAMP: the step's declared reservation NAMES ride the flow
    rows' metadata beside ``rate_limits`` — the static node inserts AND
    the fork's children (a map's items inherit the source node's terms:
    the children share the source's cohort). The claim path's read."""
    app = WorkflowApp()

    async def capped_source(ctx: StepContext, params: Ingest) -> list[str]:
        return ["a", "b"]  # pragma: no cover - the runner drives real bodies

    async def capped_item(ctx: StepContext, item: str) -> str:
        return item  # pragma: no cover - the runner drives real bodies

    @app.workflow("reservation-stamp-flow")
    def reservation_stamp_flow() -> Promise[object]:
        source = step(
            capped_source,
            Ingest(doc_id="d"),
            key="src",
            actor="wf-stamp",
            reservations=[_guard(wf_schema, FakeClock(_START))],
        )
        mapped = map_source(source, capped_item)
        return build(mapped)

    runner = FlowRunner(app.get("reservation-stamp-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    state = await runner.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = await wf_pool.fetch(
        f'SELECT step_key, map_index, metadata FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        flow_id,
    )
    stamped = {row["step_key"]: _meta(row["metadata"]) for row in rows}
    assert stamped["src"].get("reservations") == [GUARD_NAME], stamped["src"]
    for item_key in [k for k in stamped if k.startswith("src.item")]:
        assert stamped[item_key].get("reservations") == [GUARD_NAME], (
            f"the fork's child row {item_key} carries the source's reservation stamp"
        )
    join_meta = stamped["src.join"]
    # THE JOIN RIDES THE RUN'S COHORT (the depth-N law): the join row's
    # metadata carries the source row's already-resolved names, so a
    # NESTED fan-out over this join reads the stamp — the depth-2
    # children inherit the run's cap.
    assert join_meta.get("reservations") == [GUARD_NAME], (
        f"the join row rides the run's own reservation cohort; got {join_meta}"
    )


def _meta(raw: Any) -> dict[str, Any]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict), decoded
    return decoded


# ── H-A1: the acquire — exactly ONE body at a time ──────────────────────


@pytest.mark.integration
async def test_two_flow_rows_sharing_a_reservation_run_one_body_at_a_time(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ACQUIRE (H-A1's conviction): two flow rows sharing a slots=1
    reservation, both CLAIMED concurrently — exactly ONE body runs at a
    time. The second gate DENIES (the row snoozed budget-free, its
    release_reason naming the reservation), and the release RE-OPENS the
    gate: the re-claimed row runs after the first. The overlap guard's
    tripwire never fires."""
    clock = FakeClock(_START)
    registry, _reservation = await _registered_guard(wf_schema, clock, wf_pool)
    guard = OverlapGuard()
    ran: list[str] = []

    app = WorkflowApp()

    async def guarded_one(ctx: StepContext, params: Ingest) -> Report:
        await guard.enter("one")
        ran.append("one")
        return Report(ref=params.doc_id)

    async def guarded_two(ctx: StepContext, params: Ingest) -> Report:
        await guard.enter("two")
        ran.append("two")
        return Report(ref=params.doc_id)

    @app.workflow("reservation-serial-flow")
    def reservation_serial_flow() -> Promise[object]:
        one = step(
            guarded_one,
            Ingest(doc_id="d"),
            key="one",
            actor="wf-serial",
            reservations=[_guard(wf_schema, FakeClock(_START))],
        )
        two = step(guarded_two, Ingest(doc_id="d"), key="two", actor="wf-serial")
        sink(two)
        return build(one)

    from taskq.worker.run import _flow_denial_snooze_metadata

    runner = FlowRunner(app.get("reservation-serial-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    rows = await wf_pool.fetch(
        f'SELECT id, payload FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__' "
        "ORDER BY step_key",
        flow_id,
    )
    assert len(rows) == 2, [r["step_key"] for r in rows]
    claimed: list[JobRow] = []
    for row in rows:
        await wf_pool.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
            "claim_epoch = 1, locked_by_worker = $2, "
            "lock_expires_at = now() + interval '60 seconds' WHERE id = $1",
            row["id"],
            CAPABLE_WORKER,
        )
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        claimed.append(
            _flow_job(
                JobId(flow_id),
                JobId(row["id"]),
                payload,
                extra_meta={"reservations": [GUARD_NAME]},
            )
        )

    async def _claim_then_run(reg: RateLimitRegistry, job: JobRow) -> str:
        """The consumer loop's own shape: gate → (deny: the budget-free
        snooze, its stamp naming the reservation) → door (the release
        rides the door's own finally)."""
        gate = await _flow_rate_limit_gate(
            reg, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
        )
        if gate.denied is not None:
            snooze_stamp = _flow_denial_snooze_metadata(gate.denied)
            assert snooze_stamp["released_bucket"] == GUARD_NAME
            return "snoozed"
        assert gate.acquired and gate.registry is not None
        return await _dispatch_flow_job(
            deps=_deps(wf_pool, wf_schema),
            job=job,
            worker_id=CAPABLE_WORKER,
            enqueuer=_NoEnqueuer(),
            flow_seam=_door_seam(),
            flow_slot=(gate.acquired, gate.registry),
        )

    # ── phase 1: THE DENIED CLAIM — the slot held, the gate denies (the
    # budget-capped local retry proves the DENIAL propagated, never a
    # pass-through), and the row's snooze stamp NAMES the reservation.
    holder = await registry.acquire_for_actor(
        rate_limits=(),
        reservations=[GUARD_NAME],
        job_id=new_job_id(),
        worker_id=CAPABLE_WORKER,
        pg_pool=wf_pool,
    )
    assert holder
    starved_settings = WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": wf_schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "MAX_CONCURRENCY": 1,  # the local retry budget: 2 — the denial propagates fast
        }
    )
    starved_deps = WorkerDeps(
        settings=starved_settings,
        dispatcher_pool=wf_pool,
        heartbeat_pool=wf_pool,
        worker_pool=wf_pool,
        notify_conn=None,
        leader_conn=None,
    )
    denied_gate = await _flow_rate_limit_gate(
        registry, clock, claimed[0], CAPABLE_WORKER, starved_deps, job_log=JOB_LOG
    )
    assert denied_gate.denied is not None, "the held reservation DENIES the flow row's claim"
    assert denied_gate.denied.bucket_name == GUARD_NAME
    assert denied_gate.acquired == []
    stamp = _flow_denial_snooze_metadata(denied_gate.denied)
    assert stamp["released_reason"] == "flow-rate-limit-denied"
    assert stamp["released_bucket"] == GUARD_NAME, "the release_reason NAMES the reservation"

    # ── phase 2: THE RELEASE RE-OPENS the gate — the re-claimed row runs.
    await registry.release_for_actor(holder)
    retry_gate = await _flow_rate_limit_gate(
        registry, clock, claimed[0], CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert retry_gate.denied is None and retry_gate.acquired, "the release re-opens the gate"
    assert retry_gate.registry is not None
    first_outcome = await _dispatch_flow_job(
        deps=_deps(wf_pool, wf_schema),
        job=claimed[0],
        worker_id=CAPABLE_WORKER,
        enqueuer=_NoEnqueuer(),
        flow_seam=_door_seam(),
        flow_slot=(retry_gate.acquired, retry_gate.registry),
    )
    assert first_outcome == "succeeded", first_outcome
    assert ran == ["one"] and guard.tripped == [], (ran, guard.tripped)

    # ── phase 3: TWO CONCURRENT claims — exactly one body at a time.
    # The denial-retry's local re-probe may win the freed slot for the
    # loser (a fast body's completion wakes it); it may never OVERLAP
    # the winner — the tripwire screams if it did.
    outcomes = await asyncio.gather(
        _claim_then_run(registry, claimed[0]), _claim_then_run(registry, claimed[1])
    )
    assert all(o in ("succeeded", "snoozed") for o in outcomes), outcomes
    assert "two" in ran, ran
    assert guard.tripped == [], "THE TRIPWIRE: two bodies overlapped — the reservation did not hold"


# ── H-A1: the fail-closed arms ──────────────────────────────────────────


@pytest.mark.integration
async def test_an_unknown_reservation_name_parks_fail_closed(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE FAIL-CLOSED ARM (the rate_limits' own): a flow row whose
    metadata names a reservation this worker's registry does not carry
    parks budget-free, LOUD — never admitted unprotected, never a
    crash."""
    clock = FakeClock(_START)
    registry = RateLimitRegistry()  # the guard is NOT registered here
    job = _flow_job(
        JobId(new_job_id()),
        JobId(new_job_id()),
        {},
        extra_meta={"reservations": ["no-such-guard"]},
    )
    gate = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert gate.denied is None and gate.acquired == []
    assert gate.dependency_failure is not None, "the row parks (the fail-closed arm)"
    assert "no-such-guard" in str(gate.dependency_failure)


@pytest.mark.integration
async def test_malformed_reservation_metadata_parks_fail_closed(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE HAND-CRAFTED ROW'S SHAPE (the jsonb boundary): a metadata
    ``reservations`` value that is not a list of str is the SAME
    fail-closed arm — the row parks, the WARNING names it."""
    clock = FakeClock(_START)
    registry, _reservation = await _registered_guard(wf_schema, clock, wf_pool)
    job = _flow_job(
        JobId(new_job_id()),
        JobId(new_job_id()),
        {},
        extra_meta={"reservations": "not-a-list"},
    )
    gate = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert gate.denied is None and gate.acquired == []
    assert gate.dependency_failure is not None, "the metadata lies — the row parks"


# ── H-A5: result_ttl is a live knob ─────────────────────────────────────


@pytest.mark.integration
async def test_terminal_node_writes_result_expires_at_from_the_cohort_ttl(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """H-A5 (the (a) cure): the wf finalize's terminal mark writes
    ``result_expires_at`` on the SUCCEEDED terminal rows from the
    cohort's stored ``actor_config.result_ttl`` — the vanilla
    ``mark_succeeded``'s lateral equivalent. A cohort with no TTL writes
    NULL; a FAILED terminal never stamps an expiry (the vanilla
    ``mark_failed``'s parity)."""
    from taskq.worker.startup import sync_actor_config
    from taskq.workflows._worker_execution import project_workflow_actor_configs

    app = WorkflowApp()

    async def ttl_body(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)

    async def plain_body(ctx: StepContext, data: Report) -> Report:
        return data

    async def doomed_body(ctx: StepContext, data: Report) -> Report:
        raise RuntimeError("the doomed step's terminal-failure leg")  # pragma: no cover

    @app.workflow("ttl-finalize-flow")
    def ttl_finalize_flow() -> Promise[object]:
        one = step(ttl_body, Ingest(doc_id="d"), key="one", actor="wf-ttl", result_ttl=3600.0)
        two = step(plain_body, one, key="two", actor="wf-plain")
        return build(two)

    @app.workflow("ttl-doomed-flow")
    def ttl_doomed_flow() -> Promise[object]:
        one = step(ttl_body, Ingest(doc_id="d"), key="one", actor="wf-doom", result_ttl=3600.0)
        two = step(doomed_body, one, key="two", actor="wf-doom", max_attempts=1)
        return build(two)

    # THE COHORT ROWS (the projection's seed — the finalize reads the
    # STORED row, the operator-owned face).
    register_rows = project_workflow_actor_configs()
    async with wf_pool.acquire() as conn:
        await sync_actor_config(conn, register_rows, schema=wf_schema)

    runner = FlowRunner(app.get("ttl-finalize-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    state = await runner.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    row = await wf_pool.fetchrow(
        f'SELECT result_expires_at, finished_at FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'one'",
        flow_id,
    )
    assert row is not None
    assert row["result_expires_at"] is not None, "result_ttl is a LIVE knob — the expiry is stamped"
    delta = (row["result_expires_at"] - row["finished_at"]).total_seconds()
    assert 3599.0 <= delta <= 3601.0, f"the TTL prices the expiry from the completion: {delta}s"

    plain = await wf_pool.fetchrow(
        f'SELECT result_expires_at FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'two'",
        flow_id,
    )
    assert plain is not None
    assert plain["result_expires_at"] is None, "a cohort with no TTL writes no expiry"

    # THE FAILED TERMINAL'S PARITY: the vanilla mark_failed stamps no expiry.
    doomed_runner = FlowRunner(app.get("ttl-doomed-flow"), wf_pool, wf_schema)
    doomed_id = (await doomed_runner.create_flow()).flow_id
    doomed_state = await doomed_runner.drive(doomed_id, until="terminal", max_ticks=500)
    assert doomed_state == "terminal", doomed_state
    failed_row = await wf_pool.fetchrow(
        f'SELECT result_expires_at, status FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'two'",
        doomed_id,
    )
    assert failed_row is not None
    assert failed_row["status"] == "failed", failed_row
    assert failed_row["result_expires_at"] is None, "a FAILED terminal stamps no expiry"


# ══ ROUND-2 RED-TEAM CONVICTIONS (§H-A's round-2 subsection) ═════════════


# ── C1: the fail-closed park snoozes (never disowns) ─────────────────────


class _RecordingParkBackend:
    """THE PARK'S WITNESS: ``mark_snoozed`` records (the stamp's own
    read); the fail-closed park touches nothing else on the backend."""

    def __init__(self) -> None:
        self.snoozes: list[dict[str, object]] = []

    async def mark_snoozed(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict[str, object] | None = None,
        **_kwargs: object,
    ) -> str:
        self.snoozes.append({"job_id": job_id, "delay": delay, "metadata_update": metadata_update})
        return "ok"


async def _run_one_job_through_the_intercept(
    deps: WorkerDeps, registry: RateLimitRegistry, job: JobRow
) -> tuple[_RecordingParkBackend, WorkerDeps]:
    """THE INTERCEPT'S OWN DRIVE: one claimed flow row through
    ``di_consumer_loop``'s workflow intercept — the loop processes the
    ONE queued job, then the shutdown event ends it. The fail-closed
    park's witness (the stub backend + the deps' disowned set) is
    returned for the pin's assertions."""
    from taskq._di import ProviderRegistry
    from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
    from taskq.worker.run import di_consumer_loop

    process_scope = ProcessScope(resolver=lambda *_a, **_k: None, factory_timeout=1.0)
    process_scope._cache[Clock] = FakeClock(_START)  # the loop's own Clock read
    thread_scope = ThreadScope(resolver=lambda *_a, **_k: None, factory_timeout=1.0)
    loop_scope = LoopScope(resolver=lambda *_a, **_k: None, factory_timeout=1.0)
    loop_scope._cache[RateLimitRegistry] = registry  # the intercept's loop-scope read

    backend = _RecordingParkBackend()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    queue.put_nowait(job)
    shutdown = asyncio.Event()
    loop_task = asyncio.create_task(
        di_consumer_loop(
            deps,
            queue,
            shutdown,
            backend=backend,  # type: ignore[arg-type]  # Why: the park arm's duck-typed witness — the intercept touches mark_snoozed only.
            worker_id=CAPABLE_WORKER,
            registry=ProviderRegistry(),
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_registry={},
            enqueuer=_NoEnqueuer(),
            slot_freed_event=asyncio.Event(),
        )
    )
    # THE PROCESSED SIGNAL: either arm settles the job (the fixed shape
    # snoozes it; the convicted shape disowns it) — wait for either, then
    # stop the loop.
    for _ in range(1000):
        if backend.snoozes or deps.disowned_jobs:
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    await asyncio.wait_for(loop_task, timeout=10)
    return backend, deps


@pytest.mark.integration
async def test_a_renamed_reservation_park_snoozes_and_stamps_never_disowns(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """C1 (THE CHURN CONVICTION): a flow row stamped with a reservation
    NAME the registry lacks hits the FAIL-CLOSED park arm — the park is
    a SNOOZE (``mark_snoozed`` called, the stamp landed: the
    ``released_reason`` naming the park, the arm's own kind beside it),
    NEVER a disown. The pre-fix shape asserted ``denied is not None``
    inside the caller's broad except: the AssertionError disowned the
    row, the heartbeat stopped renewing it, the reclaim handed it back,
    the re-claim re-denied — the infinite churn at lease cadence, the
    denial stamps never landing."""
    deps = _deps(wf_pool, wf_schema)
    registry = RateLimitRegistry()  # the guard is NOT registered — the rename
    job = _flow_job(
        JobId(new_job_id()),
        JobId(new_job_id()),
        {},
        extra_meta={"reservations": ["renamed-guard"]},
    )
    backend, deps_after = await _run_one_job_through_the_intercept(deps, registry, job)
    assert not deps_after.disowned_jobs, (
        f"THE CHURN: the row was disowned (the assert's shape) — {deps_after.disowned_jobs}"
    )
    assert len(backend.snoozes) == 1, backend.snoozes
    stamp = backend.snoozes[0]["metadata_update"]
    assert stamp is not None
    stamp_d = cast("dict[str, object]", stamp)
    assert stamp_d["released_reason"] == "flow-rate-limit-parked", stamp_d
    assert stamp_d["parked"] == "flow-reservation-unknown-bucket", stamp_d


@pytest.mark.integration
async def test_a_malformed_metadata_park_snoozes_and_stamps_never_disowns(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """C1, the malformed-metadata story: a flow row whose
    ``reservations`` metadata lies (not a list of str — the jsonb
    boundary's hand-crafted row) parks by the SAME snooze-and-stamp —
    the arm's own kind on the record, the claim kept (the disown's
    churn never starts)."""
    deps = _deps(wf_pool, wf_schema)
    clock = FakeClock(_START)
    registry, _reservation = await _registered_guard(wf_schema, clock, wf_pool)
    job = _flow_job(
        JobId(new_job_id()),
        JobId(new_job_id()),
        {},
        extra_meta={"reservations": "not-a-list"},
    )
    backend, deps_after = await _run_one_job_through_the_intercept(deps, registry, job)
    assert not deps_after.disowned_jobs, deps_after.disowned_jobs
    assert len(backend.snoozes) == 1, backend.snoozes
    stamp = backend.snoozes[0]["metadata_update"]
    assert stamp is not None
    stamp_d = cast("dict[str, object]", stamp)
    assert stamp_d["released_reason"] == "flow-rate-limit-parked", stamp_d
    assert stamp_d["parked"] == "flow-reservation-metadata-malformed", stamp_d


# ── C5: the route arm's stamp symmetry ───────────────────────────────────


class ImageItem(BaseModel):
    kind_id: str


class AudioItem(BaseModel):
    kind_id: str


@pytest.mark.integration
async def test_the_route_arm_stamp_symmetry_inherits_the_source_terms(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """C5: THE ROUTE ARM'S INHERITANCE IS SYMMETRIC — a BARE arm (no own
    rate_limits, no actor override) inherits the SOURCE node's
    ``rate_limits`` ALONGSIDE its ``reservations`` (the CURE-2 face's own
    terms ride the cohort); an arm declaring its own terms keeps its OWN
    (no silent merge). The pre-fix asymmetry: the reservations flowed
    from the source, the rate_limits didn't — the bare arm's rows lost
    the source's rate caps at the stamp."""
    from taskq.ratelimit.token_bucket import TokenBucket
    from taskq.workflows import RouteArm, route

    app = WorkflowApp()
    arm_bucket = TokenBucket("route-arm-bucket", 3, 1.0)
    arm_guard = ConcurrencyReservation("route-arm-own-guard", slots=2, lease=30.0)

    async def route_union_source(ctx: StepContext, params: Ingest) -> list[ImageItem | AudioItem]:
        return [ImageItem(kind_id="i"), AudioItem(kind_id="a")]  # pragma: no cover

    async def image_arm(ctx: StepContext, item: ImageItem) -> Report:
        return Report(ref=item.kind_id)  # pragma: no cover - the runner drives real bodies

    async def audio_arm(ctx: StepContext, item: AudioItem) -> Report:
        return Report(ref=item.kind_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("route-stamp-symmetry-flow")
    def route_stamp_symmetry_flow() -> Promise[object]:
        source = step(
            route_union_source,
            Ingest(doc_id="d"),
            key="src",
            actor="wf-route",
            rate_limits=[TokenBucket("route-src-bucket", 5, 1.0)],
            reservations=[_guard(wf_schema, FakeClock(_START))],
        )
        return build(
            route(
                source,
                {
                    # THE DECLARING ARM: its own terms ONLY (no silent merge).
                    ImageItem: RouteArm(
                        image_arm, rate_limits=[arm_bucket], reservations=[arm_guard]
                    ),
                    # THE BARE ARM: the source's terms ride BOTH stamps.
                    AudioItem: audio_arm,
                },
            )
        )

    runner = FlowRunner(app.get("route-stamp-symmetry-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    state = await runner.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = await wf_pool.fetch(
        f'SELECT step_key, metadata FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'src.item:%'",
        flow_id,
    )
    stamped = {row["step_key"]: _meta(row["metadata"]) for row in rows}
    assert len(stamped) == 2, sorted(stamped)
    image_child = next(m for k, m in stamped.items() if "ImageItem" in k)
    audio_child = next(m for k, m in stamped.items() if "AudioItem" in k)
    # THE DECLARING ARM: its own terms only.
    assert image_child.get("rate_limits") == ["route-arm-bucket"], image_child
    assert image_child.get("reservations") == ["route-arm-own-guard"], image_child
    # THE BARE ARM: the SOURCE's terms ride BOTH stamps (the symmetry).
    assert audio_child.get("rate_limits") == ["route-src-bucket"], audio_child
    assert audio_child.get("reservations") == [GUARD_NAME], audio_child
