"""THE QUEUE'S RATE LIMIT ON THE WORKFLOW PATH (the DI lane's blocker
#2 cure): a workflow row's claim honors the SAME queue rate limits the
vanilla actors honor — ONE mechanism, not a second one.

The vanilla path's queue rate limit: a queue whose ``max_concurrent``
column is set rides a fleet-wide queue-cap reservation
(``taskq:global:queue:<name>``), prepended per job by
``_effective_reservations`` and acquired post-claim by the consumer's
pre-flight (``acquire_for_actor`` — the admission authority); a denial
snoozes the row (budget-free) and the deny is observable (the
``taskq.ratelimit.denials`` counter). The WORKFLOW path had NO such
acquire: every claimed flow row executed unconditionally — a
rate-limited queue's flow BURST past the cap its vanilla siblings
honored.

The cure pins (the intercept's gate — ``taskq.worker.run.
_flow_rate_limit_gate`` — and the door's slot release):

* THE DENY — the cap's slots held, the gate denies, the counter bumps
  (``postgres`` backend label): the deny is observable;
* THE CADENCE — one slot, sequential acquire-release-acquire: the
  second acquire succeeds ONLY after the first release (never a burst);
* THE NO-CAPS FAST PATH — no registry, no cap: the gate passes
  everything through (the vanilla shape, zero per-job cost);
* THE FAIL-CLOSED DEPENDENCY ARM — a store outage is the limiter's own
  denial shape (the snooze, budget-free), never a job outcome;
* THE DOOR'S SLOT LAW — a HELD outcome releases too (the wait is not
  occupancy): the flow step that holds on a human gate gives the
  queue's capacity back while it waits, and the wake's re-execution
  re-acquires cleanly.

The reservations are PG-BACKED (the production shape — the boot's
``sync_slots`` materializes the slot rows, the acquire's lease/reclaim
read them), on the module's OWN schema; each pin's OWN queue (the
bucket namespace is the PG table — a pin's held slot must never leak
into the next pin's acquire).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import ModuleType
from unittest.mock import patch
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.backend._protocol import JobId, JobRow
from taskq.backend.clock import Clock
from taskq.ratelimit.registry import (
    RateLimitRegistry,
    queue_concurrency_reservation_name,
)
from taskq.ratelimit.reservation import ConcurrencyReservation, sync_slots
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _dispatch_flow_job, _flow_rate_limit_gate
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows.api._hitl import HitlClient
from tests.conftest import unique_health_sock_path

_START = datetime(2025, 1, 1, tzinfo=UTC)

CAPABLE_WORKER = UUID(int=7)

JOB_LOG = structlog.get_logger("wf_rate_limit_pins")

RAN: list[str] = []


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Approval(BaseModel):
    verdict: str


def _queue(pin: str) -> str:
    """Each pin's own queue (its own cap bucket, its own slot rows)."""
    return f"pin-wf-{pin}"


def _settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("wf_rate_limit_pins"),
        }
    )


def _deps(real_pool: asyncpg.Pool, schema: str) -> WorkerDeps:
    """The door's deps: every pool the REAL pg fixture, the schema the
    module's own (the queue-cap reservation's slot rows are PG-backed —
    the production shape, the fleet's one mechanism)."""
    return WorkerDeps(
        settings=_settings(schema),
        dispatcher_pool=real_pool,
        heartbeat_pool=real_pool,
        worker_pool=real_pool,
        notify_conn=None,
        leader_conn=None,
    )


async def _capped_registry(
    pin: str, slots: int, clock: Clock, schema: str, pool: asyncpg.Pool
) -> RateLimitRegistry:
    registry = RateLimitRegistry()
    reservation = ConcurrencyReservation(
        name=queue_concurrency_reservation_name(_queue(pin)),
        slots=slots,
        lease=timedelta(seconds=30),
        clock=clock,
        schema=schema,
    )
    registry.register_queue_cap_reservation(reservation)
    # THE BOOT'S OWN SYNC (the fleet's shape): the slot rows materialize
    # at worker startup — the acquire has no ensure-slots retry, so the
    # pin performs the boot's sync before the gate runs.
    await sync_slots([reservation], pool, schema=schema)
    return registry


def _flow_job(
    pin: str, flow_id: JobId, node_id: JobId, payload: object, attempt: int = 1
) -> JobRow:
    return JobRow(
        id=node_id,
        actor="wf-pin",
        queue=_queue(pin),
        identity_key=None,
        fairness_key=None,
        payload=payload,  # type: ignore[arg-type]  # Why: the door's payload is the engine's envelope dict — the test's literal is that shape.
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=attempt,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=_START,
        scheduled_at=_START,
        metadata={"flow_id": str(flow_id)},
        trace_id=None,
    )


def _door_seam() -> ModuleType:
    """The lazily-imported execution seam module (the intercept's own
    resolution — imported here for the door call)."""
    from taskq.workflows import _worker_execution

    return _worker_execution


class _NoEnqueuer:
    """The door's enqueuer knob: the pin's flow writes no sub-jobs."""


# ── the gate's unit pins ─────────────────────────────────────────────────


async def test_the_deny_is_observable_and_names_the_bucket(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE DENY: the cap's ONE slot held, the gate denies with the
    queue-cap bucket's name, and the denial counter bumps (the
    observable). No claim, no attempt, no budget: the row parks."""
    pin_queue = _queue("deny")
    clock = FakeClock(_START)
    registry = await _capped_registry("deny", 1, clock, wf_schema, wf_pool)
    cap = queue_concurrency_reservation_name(pin_queue)
    holder = await registry.acquire_for_actor(
        rate_limits=(),
        reservations=[cap],
        job_id=new_job_id(),
        worker_id=CAPABLE_WORKER,
        pg_pool=wf_pool,  # the SAME table the gate's acquire reads (PG-backed, the production shape)
    )
    assert holder  # the fixture held the slot

    denials: list[str] = []
    with patch("taskq.worker.run.record_ratelimit_denial", side_effect=denials.append):
        gate = await _flow_rate_limit_gate(
            registry,
            clock,
            _flow_job("deny", JobId(new_job_id()), JobId(new_job_id()), {}),
            CAPABLE_WORKER,
            _deps(wf_pool, wf_schema),
            job_log=JOB_LOG,
        )
    assert gate.denied is not None
    assert gate.denied.bucket_name == cap
    assert gate.acquired == []
    assert denials == ["postgres"]


async def test_the_cadence_one_slot_never_bursts(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """THE CADENCE: one slot; acquire → (deny) → release → acquire. The
    release RE-OPENS the gate — a flow on a rate-limited queue fires at
    the limit's cadence, never a burst."""
    clock = FakeClock(_START)
    registry = await _capped_registry("cadence", 1, clock, wf_schema, wf_pool)
    job = _flow_job("cadence", JobId(new_job_id()), JobId(new_job_id()), {})

    first = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert first.denied is None and first.acquired, "the first acquire passes"

    second = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert second.denied is not None, "the held slot DENIES the second (never a burst)"
    assert second.acquired == []

    assert first.registry is not None
    await first.registry.release_for_actor(first.acquired)
    third = await _flow_rate_limit_gate(
        registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert third.denied is None and third.acquired, "the RELEASE re-opens the gate"


async def test_the_no_caps_fast_path_passes_through(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """No registry (the scope never resolved one) and no cap on the
    queue: the gate passes everything through — the vanilla shape, zero
    per-job cost, nothing denied."""
    clock = FakeClock(_START)
    job = _flow_job("nocaps", JobId(new_job_id()), JobId(new_job_id()), {})
    none_gate = await _flow_rate_limit_gate(
        None, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert none_gate.acquired == [] and none_gate.denied is None
    bare = await _flow_rate_limit_gate(
        RateLimitRegistry(), clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert bare.acquired == [] and bare.denied is None


async def test_the_dependency_failure_is_fail_closed(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """A store outage is the limiter's own denial shape (the gate's
    fail-closed arm), never a job outcome — the row parks budget-free."""
    clock = FakeClock(_START)
    registry = await _capped_registry("outage", 1, clock, wf_schema, wf_pool)
    job = _flow_job("outage", JobId(new_job_id()), JobId(new_job_id()), {})

    class _StoreDownError(ConnectionError):
        pass

    async def _down(*args: object, **kwargs: object) -> object:
        raise _StoreDownError("the limiter's store is unreachable")

    with patch.object(RateLimitRegistry, "acquire_for_actor", _down):
        gate = await _flow_rate_limit_gate(
            registry, clock, job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
        )
    assert gate.dependency_failure is not None
    assert isinstance(gate.dependency_failure, _StoreDownError)
    assert gate.denied is None and gate.acquired == []


# ── the door's slot law: a HELD outcome releases ─────────────────────────


@pytest.mark.integration
async def test_the_door_releases_the_slot_while_the_flow_holds(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE SLOT LAW (the holds'): the flow step that holds on a human
    gate gives the queue's capacity back — the slot is re-acquirable
    while the run waits — and the wake's re-execution acquires again
    cleanly."""
    clock = FakeClock(_START)
    registry = await _capped_registry("hold", 1, clock, wf_schema, wf_pool)
    RAN.clear()

    app = WorkflowApp()

    @app.workflow("rate_limit_hold_flow")
    def rate_limit_hold_flow() -> Promise[object]:
        async def review(ctx: StepContext, params: Ingest) -> object:
            if ctx.attempt < 2:
                return await ctx.wait_signal(Approval, timeout_s=120.0)
            RAN.append("woke")
            return Report(ref=params.doc_id)

        return build(step(review, Ingest(doc_id="d1"), key="review", queue=_queue("hold")))

    runner = FlowRunner(app.get("rate_limit_hold_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    node = await wf_pool.fetchrow(
        f"SELECT id, payload FROM \"{wf_schema}\".jobs WHERE step_key = 'review' "  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node is not None
    await wf_pool.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "claim_epoch = 1, locked_by_worker = $2, "
        "lock_expires_at = now() + interval '60 seconds' WHERE id = $1",
        node["id"],
        CAPABLE_WORKER,
    )
    payload = json.loads(node["payload"]) if isinstance(node["payload"], str) else node["payload"]

    gate = await _flow_rate_limit_gate(
        registry,
        clock,
        _flow_job("hold", JobId(flow_id), JobId(node["id"]), payload),
        CAPABLE_WORKER,
        _deps(wf_pool, wf_schema),
        job_log=JOB_LOG,
    )
    assert gate.acquired, "the capped queue admits the first flow attempt"
    assert gate.registry is not None

    execution = await _dispatch_flow_job(
        deps=_deps(wf_pool, wf_schema),
        job=_flow_job("hold", JobId(flow_id), JobId(node["id"]), payload),
        worker_id=CAPABLE_WORKER,
        enqueuer=_NoEnqueuer(),
        flow_seam=_door_seam(),
        flow_slot=(gate.acquired, gate.registry),
    )
    assert execution == "held"
    # THE SLOT IS FREE WHILE THE FLOW WAITS: the wait is not occupancy.
    next_job = _flow_job("hold", JobId(flow_id), JobId(node["id"]), payload)
    next_gate = await _flow_rate_limit_gate(
        registry, clock, next_job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert next_gate.acquired, "the hold RELEASED the queue's slot"
    assert next_gate.registry is not None
    await next_gate.registry.release_for_actor(next_gate.acquired)

    # THE WAKE: deliver through the HITL door, re-execute — the
    # re-execution acquires cleanly and terminalizes.
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(JobId(flow_id))
    assert holds
    resolved = await client.resolve(holds[0].hold_id, {"verdict": "approve"})
    assert resolved.status == "delivered"

    wake_gate = await _flow_rate_limit_gate(
        registry, clock, next_job, CAPABLE_WORKER, _deps(wf_pool, wf_schema), job_log=JOB_LOG
    )
    assert wake_gate.acquired and wake_gate.registry is not None
    execution = await _dispatch_flow_job(
        deps=_deps(wf_pool, wf_schema),
        job=_flow_job(
            "hold", JobId(flow_id), JobId(node["id"]), payload, attempt=2
        ),  # the wake re-claims: the attempt incremented (the resume's own grant)
        worker_id=CAPABLE_WORKER,
        enqueuer=_NoEnqueuer(),
        flow_seam=_door_seam(),
        flow_slot=(wake_gate.acquired, wake_gate.registry),
    )
    assert execution == "succeeded"
    assert RAN == ["woke"]
