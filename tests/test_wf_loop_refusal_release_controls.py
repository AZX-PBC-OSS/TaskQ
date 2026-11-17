# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""N-4(b) — the ruling's three loop-refusal release controls (CL-4's
landing preconditions): the loop machinery's DETERMINISTIC refusals —
the driver's iteration-bound exhaustion, an iteration terminal with no
ledger claim identity, and the carry contract's `LoopCarryContractError`
— reach the bounded body-unresolvable release, never the generic
`dispatch-flow-failed` owner.

The defect these controls guard against (the ruling's own narrowing
example): `_worker_execution`'s broad `except WorkflowRunError` is
LOAD-BEARING — the three refusals ride it into the
`WorkflowBodyUnresolvableError` translation and the worker's bounded
budget-free release (`released_reason='workflow-body-unresolvable'`, the
documented 10-second delay, `dispatch-workflow-body-unresolvable`). A
catch narrowed to `VanishedNodeError` would instead drop them into the
generic arm, whose recovery is the non-consuming refund plus the
reclaim re-claim — an unbounded deterministic claim→fail→refund→reclaim
cycle for a refusal that is deterministic by construction.

Every control drives the REAL fleet-claimed door end-to-end: a real
compiled loop workflow over real PostgreSQL, the row genuinely claimed
(`running` under the control's own fleet claim), the REAL
`di_consumer_loop` with the REAL `_workflow_execution_seam` — nothing on
the workflow execution path is patched — so the refusal, the seam's
translation, and the worker's release arm are the shipped code. CL-4's
landing preconditions are asserted before the release call (the row
running under this exact claim with `cancel_phase=0`, and where
`schedule_to_close` is set, `clock_timestamp() + 10s <= schedule_to_close`),
and the landing itself is asserted as the ACTUAL `scheduled` outcome —
not the tri-state union.
"""

from __future__ import annotations

import asyncio
import contextlib
import json as _json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
import structlog.testing
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend._protocol import JobId, JobRow
from taskq.context import JobContext
from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    Refine,
    WorkflowApp,
    build,
    loop,
)

pytestmark = pytest.mark.integration

#: The driver's own hard iteration bound (`_runner_loop.py`'s
#: ``max_wall``) — the count control 1 must actually exhaust.
MAX_WALL = 10_000


class _Carry(BaseModel):
    """The declared carry type control 3 poisons."""

    acc: int = 0


class _WfPayload(BaseModel):
    pass


@actor(name="wf")
async def _wf_placeholder(payload: _WfPayload, ctx: JobContext[_WfPayload]) -> None:
    del payload, ctx


def _make_loop_app(
    body: Callable[..., Awaitable[object]],
    *,
    name: str,
    **loop_kwargs: Any,
) -> WorkflowApp:
    """The loop workflow's app: one loop node, compiled + recorded (the
    D1 registry — the fleet door's own resolution source)."""

    app = WorkflowApp()

    @app.workflow(name)
    def the_flow() -> Promise[object]:
        return build(loop("counter", body, **loop_kwargs))

    app.get(name)  # compile + record — the door's get_compiled_workflow answers from here
    return app


def _new_uuid_str() -> str:
    return str(new_uuid())


def _fleet_worker_id() -> JobId:
    # a REAL UUID (NewType never converts): the row's locked_by_worker
    # column and the loop's worker_id must be the same UUID, not a str
    # that only looks like one.
    return JobId(new_uuid())


async def _loop_node_row(
    pool: Any, schema: str, flow_id: str, node_key: str = "counter"
) -> dict[str, Any]:
    raw = await pool.fetchrow(
        f"""SELECT id, actor, queue, payload, trace_id, attempt, max_attempts,
                   retry_kind, metadata, schedule_to_close
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = $2""",
        flow_id,
        node_key,
    )
    assert raw is not None, f"the loop node row {node_key!r} never materialized"
    return dict(raw)


def _decode_jsonb(value: object) -> Any:
    if isinstance(value, str):
        return _json.loads(value)
    return value


async def _fleet_claim_loop_node(
    pool: Any,
    schema: str,
    flow_id: str,
    fleet_worker: JobId,
    *,
    node_key: str = "counter",
) -> JobRow:
    """The fleet claim shape (`dispatch_batch`'s own effect: `running`,
    the attempt incremented, the epoch bumped, the lock held by this
    worker) plus the row's own one-hour deadline — then the claimed
    row's decode (the JobRow the dispatch would hand the loop)."""
    node = await _loop_node_row(pool, schema, flow_id, node_key)
    pre_claim_attempt = int(node["attempt"])
    await pool.execute(
        f"""UPDATE "{schema}".jobs SET status = 'running',
            attempt = $2, claim_epoch = 1, locked_by_worker = $3,
            lock_expires_at = now() + interval '90 seconds',
            last_heartbeat_at = now(),
            schedule_to_close = clock_timestamp() + interval '1 hour'
            WHERE id = $1""",
        node["id"],
        pre_claim_attempt + 1,
        fleet_worker,
    )
    claimed = await _loop_node_row(pool, schema, flow_id, node_key)
    metadata = _decode_jsonb(claimed["metadata"])
    assert isinstance(metadata, dict) and metadata.get("flow_id") is not None, (
        "the claimed row lost the flow lineage — the dispatch intercept never runs"
    )
    payload = _decode_jsonb(claimed["payload"])
    assert isinstance(payload, dict)
    return JobRow(
        id=JobId(str(claimed["id"])),
        actor=str(claimed["actor"]),
        queue=str(claimed["queue"]),
        identity_key=None,
        fairness_key=None,
        payload=payload,
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=int(claimed["attempt"]),
        max_attempts=int(claimed["max_attempts"]),
        retry_kind=str(claimed["retry_kind"]),  # type: ignore[arg-type]  # Why: the column's domain is RetryKind's; the claimed row's own value rides.
        claim_epoch=1,
        schedule_to_close=claimed["schedule_to_close"],
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime.now(UTC),
        scheduled_at=datetime.now(UTC),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=fleet_worker,
        lock_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        cancel_requested_at=None,
        cancel_phase=0,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="workflow-run",
        trace_id=None,
        span_id=None,
        metadata=metadata,
        tags=(),
    )


async def _assert_landed_release_preconditions(
    pool: Any, schema: str, job_id: str, fleet_worker: JobId, claimed_attempt: int
) -> None:
    """CL-4: before the release call, the row is RUNNING under this
    exact claim with `cancel_phase=0`; where `schedule_to_close` is set,
    the documented 10-second snooze FITS inside it. The preconditions
    are what make the landing assertion honest — a fixture that claimed
    nothing (or left no deadline room) could otherwise legitimately
    produce `noop`/`failed` and a `scheduled` assertion would conceal
    the fault."""
    row = await pool.fetchrow(
        f"""SELECT status::text AS status, attempt, claim_epoch, locked_by_worker,
                   cancel_phase, cancel_requested_at, schedule_to_close,
                   (schedule_to_close IS NULL
                    OR clock_timestamp() + interval '10 seconds' <= schedule_to_close)
                       AS deadline_fits
            FROM "{schema}".jobs WHERE id = $1""",
        job_id,
    )
    assert row is not None, "the claimed row vanished before the release call"
    assert row["status"] == "running", row
    assert row["attempt"] == claimed_attempt, row
    assert row["claim_epoch"] == 1, row
    assert row["locked_by_worker"] == fleet_worker, row
    assert row["cancel_phase"] == 0, row
    assert row["cancel_requested_at"] is None, row
    assert row["schedule_to_close"] is not None, (
        "the control sets the row's deadline — the precondition's deadline "
        "leg must be exercised, never vacuous"
    )
    assert row["deadline_fits"], row


async def _assert_bounded_landing(
    pool: Any, schema: str, job_id: str, *, claimed_attempt: int
) -> None:
    """The release LANDED: the row is `scheduled` (the live claim's
    deferral — not the tri-state union), at the documented 10-second
    delay, with the refund's attempt accounting, the lease cleared, and
    the released reason stamped."""
    row = await pool.fetchrow(
        f"""SELECT status::text AS status, attempt, snooze_count,
                   metadata->>'released_reason' AS reason,
                   locked_by_worker, lock_expires_at, last_heartbeat_at,
                   cancel_phase,
                   (scheduled_at > clock_timestamp()) AS deferred_forward,
                   extract(epoch FROM (scheduled_at - clock_timestamp())) AS remaining_s
            FROM "{schema}".jobs WHERE id = $1""",
        job_id,
    )
    assert row is not None
    assert row["status"] == "scheduled", (
        f"the release never landed: status={row['status']!r} — the generic "
        "arm's recovery (or a fenced noop) answered instead"
    )
    # THE ATTEMPT ACCOUNTING: the release returns the claim's increment
    # (ATTEMPT_REFUND_SQL = GREATEST(attempt - 1, 0)) — the deterministic
    # refusal never burns retry budget.
    assert row["attempt"] == max(claimed_attempt - 1, 0), row
    assert row["snooze_count"] == 1, row
    assert row["reason"] == "workflow-body-unresolvable", row
    # THE LEASE TRIO cleared together (LEASE_CLEAR_SQL) — the claim died
    # with the transition.
    assert row["locked_by_worker"] is None, row
    assert row["lock_expires_at"] is None, row
    assert row["last_heartbeat_at"] is None, row
    assert row["cancel_phase"] == 0, row
    # THE DOCUMENTED 10-SECOND DELAY: deferred into the future, within a
    # small bracket of exactly 10 s (the read's own latency eats into
    # the remaining time; the upper bound guards against a padded one).
    assert row["deferred_forward"], row
    assert 8.0 <= float(row["remaining_s"]) <= 11.0, row


async def _drive_through_consumer_loop(
    pool: Any,
    dsn: str,
    schema: str,
    job: JobRow,
    fleet_worker: JobId,
    *,
    hang_guard_s: float,
) -> tuple[list[dict[str, object]], SimpleNamespace]:
    """Run the REAL `di_consumer_loop` — the REAL workflow execution
    seam, nothing on the workflow path patched — over the claimed row,
    then a witness job whose dispatch stops the loop. Returns the
    captured log events and the loop's deps (the generic arm's own
    counters). The witness dispatched is the loop-survival evidence.
    *fleet_worker* is the row's own lock holder — the release arm's
    fence matches the loop's worker_id against it, so the loop must run
    AS the claim's worker for the release to land."""
    from taskq._di.registry import ProviderRegistry
    from taskq._di.scope import Scope
    from taskq.backend.clock import Clock
    from taskq.backend.postgres import PostgresBackend
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.worker.cancel import ActiveJobRegistry
    from taskq.worker.run import di_consumer_loop
    from taskq.worker.shutdown import ShutdownPhase
    from tests._di_scopes import bootstrap_scopes, make_scopes
    from tests.conftest import unique_health_sock_path

    real_backend = PostgresBackend(
        _BackendDeps(schema, pool),  # pyright: ignore[reportArgumentType]  # Why: the protocol double declares every field the backend's paths read (the test_admin_audit_trail mirror).
        clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("loop_refusal_n4"),
        },
        validate=False,
    )
    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))
    registry.validate()
    process_scope, thread_scope, loop_scope = make_scopes(registry)
    await bootstrap_scopes(registry, process_scope, thread_scope, loop_scope, settings)

    deps = SimpleNamespace(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        active_jobs=ActiveJobRegistry(),
        producer_stop_event=asyncio.Event(),
        shutdown_phase=ShutdownPhase.NONE,
        disowned_jobs=set(),
        drain_failures=0,
    )

    witness = JobRow(
        id=JobId(_new_uuid_str()),
        actor="wf",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=1,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime.now(UTC),
        scheduled_at=datetime.now(UTC),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=JobId(_new_uuid_str()),
        lock_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        cancel_requested_at=None,
        cancel_phase=0,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    await local_queue.put(job)
    await local_queue.put(witness)
    stop_event = asyncio.Event()
    dispatched: list[Any] = []

    async def _witness_dispatch(**kwargs: object) -> str:
        dispatched.append(kwargs.get("job"))
        stop_event.set()
        return "succeeded"

    loop_task: asyncio.Task[Any] | None = None
    try:
        with (
            structlog.testing.capture_logs() as captured,
            patch("taskq.worker.run.dispatch_one_job", side_effect=_witness_dispatch),
        ):
            loop_task = asyncio.create_task(
                di_consumer_loop(
                    deps,  # type: ignore[arg-type]  # Why: the duck-typed deps carry exactly the fields the loop reads (the existing loop controls' shape).
                    local_queue,
                    stop_event,
                    backend=real_backend,
                    worker_id=fleet_worker,
                    registry=registry,
                    process_scope=process_scope,
                    thread_scope=thread_scope,
                    loop_scope=loop_scope,
                    actor_registry={"wf": _wf_placeholder},
                    enqueuer=SubJobEnqueuer(
                        loop_scope_resolved=None, worker_pool=None, backend=real_backend
                    ),
                )
            )
            try:
                await asyncio.wait_for(loop_task, timeout=hang_guard_s)
            except TimeoutError:
                loop_task.cancel()
                pytest.fail(
                    "the consumer loop never exited — the witness's dispatch never "
                    "ran (the release arm wedged the loop)"
                )
    finally:
        if loop_task is not None and not loop_task.done():
            loop_task.cancel()
            with contextlib.suppress(BaseException):
                await loop_task
        await loop_scope.shutdown()
        await thread_scope.shutdown()
        await process_scope.shutdown()

    assert len(dispatched) == 1, dispatched
    return [dict(e) for e in captured if "event" in e], deps


class _BackendSettings:
    """The declared ``BackendSettings`` protocol's double (the
    test_admin_audit_trail mirror)."""

    schema_name: str
    dispatch_oversample: int = 2
    dispatcher_command_timeout: float = 5.0
    result_max_bytes: int = 1_000_000
    event_writer_batch_size: int = 500
    event_writer_statement_timeout_ms: float = 5_000.0
    event_writer_reduced_batch_divisor: int = 4
    sweep_breaker_failure_threshold: int = 3
    sweep_breaker_window_secs: float = 600.0
    max_pending_lock_timeout_ms: float = 5000.0
    unique_for_lock_timeout_ms: float = 5000.0

    def __init__(self, schema: str) -> None:
        self.schema_name = schema


class _BackendDeps:
    """The declared ``BackendDeps`` protocol's double (the same mirror)."""

    settings: _BackendSettings
    worker_pool: Any
    heartbeat_pool: Any
    dispatcher_pool: Any = None

    def __init__(self, schema: str, pool: Any) -> None:
        self.settings = _BackendSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


def _assert_bounded_vocabulary(captured: list[dict[str, object]], deps: SimpleNamespace) -> None:
    """The vocabulary: the body-unresolvable arm owned the refusal — the
    distinct log present, the generic owner's log/counter absent, no
    disown (the release succeeded; nothing was disowned)."""
    events = [e["event"] for e in captured]
    assert "dispatch-workflow-body-unresolvable" in events, events
    assert "dispatch-flow-failed" not in events, events
    assert "dispatch-workflow-root-missing" not in events, events
    assert deps.drain_failures == 0, deps.drain_failures
    assert not deps.disowned_jobs, deps.disowned_jobs


async def test_iteration_bound_exhaustion_reaches_the_bounded_release(
    wf_schema: str, module_pg_pool: Any, module_pg_schema: Any
) -> None:
    """The driver's iteration bound (`max_wall`) exhausted by an
    always-Refine body with NO declared wall — the loud
    `WorkflowRunError` refusal routes through the fleet seam's broad
    catch into the bounded body-unresolvable release: the row LANDS
    `scheduled` (CL-4's preconditions asserted first), the attempt
    refunded, the documented 10-second delay, never
    `dispatch-flow-failed`, the worker loop alive."""
    bodies = [0]

    async def refine_forever(ctx: Any, carry: object) -> object:
        del carry
        bodies[0] += 1
        return Refine({"acc": bodies[0]})

    name = f"loop_refusal_bound_{_new_uuid_str()}"
    app = _make_loop_app(refine_forever, name=name)
    fleet_worker = _fleet_worker_id()
    runner = FlowRunner(app.get(name), module_pg_pool, wf_schema, worker_id=fleet_worker)
    flow_id = (await runner.create_flow()).flow_id
    real_flow_id = str(flow_id)

    job = await _fleet_claim_loop_node(module_pg_pool, wf_schema, real_flow_id, fleet_worker)
    await _assert_landed_release_preconditions(
        module_pg_pool, wf_schema, str(job.id), fleet_worker, job.attempt
    )

    captured, deps = await _drive_through_consumer_loop(
        module_pg_pool,
        module_pg_schema.pg_dsn,
        wf_schema,
        job,
        fleet_worker,
        # the drive's own measured cost is the 10,000 real iterations
        # (~9 s solo); the guard is a hang detector with an order of
        # magnitude of headroom, never a threshold the outcome depends
        # on.
        hang_guard_s=120,
    )

    await _assert_bounded_landing(
        module_pg_pool, wf_schema, str(job.id), claimed_attempt=job.attempt
    )
    _assert_bounded_vocabulary(captured, deps)
    # THE DRIVE'S OWN EVIDENCE: the bound was exhausted by real
    # iterations, not short-circuited — exactly `max_wall` iteration
    # ledger rows (iter0 .. iter{max_wall-1}), exactly `max_wall` body
    # invocations, and the advance counter at `max_wall`.
    iter_rows = await module_pg_pool.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key LIKE 'counter.iter%'""",
        real_flow_id,
    )
    assert int(iter_rows) == MAX_WALL, int(iter_rows)
    assert bodies[0] == MAX_WALL, bodies[0]
    advanced = await module_pg_pool.fetchval(
        f"""SELECT (metadata->>'iteration')::bigint FROM "{wf_schema}".jobs
            WHERE id = $1""",
        job.id,
    )
    assert int(advanced) == MAX_WALL, int(advanced)


async def test_missing_iteration_ledger_identity_reaches_the_bounded_release(
    wf_schema: str, module_pg_pool: Any, module_pg_schema: Any
) -> None:
    """An iteration terminal with NO ledger claim identity — the
    replay/legacy face the guard names (a healthy claim always carries
    its row's id, so the control injects the ledger-less claim at the
    driver's own claim dependency; the raise, the seam's translation,
    and the release arm are the shipped code). The bounded release
    lands, never the generic owner."""
    from taskq.workflows import ledger as ledger_module

    async def done_at_zero(ctx: Any, carry: object) -> object:
        del ctx, carry
        return Done({"acc": 0})

    real_claim = ledger_module.claim_step_ledger

    async def ledgerless_iteration_claim(conn: Any, wsql: Any, **kw: Any) -> Any:
        claim = await real_claim(conn, wsql, **kw)
        if ".iter" in str(kw.get("step_key", "")):
            # no identity to write and none may be manufactured — the
            # guard's own replay/legacy face
            return replace(claim, ledger_id=None)
        return claim

    name = f"loop_refusal_noidentity_{_new_uuid_str()}"
    app = _make_loop_app(done_at_zero, name=name)
    fleet_worker = _fleet_worker_id()
    runner = FlowRunner(app.get(name), module_pg_pool, wf_schema, worker_id=fleet_worker)
    flow_id = (await runner.create_flow()).flow_id
    real_flow_id = str(flow_id)

    job = await _fleet_claim_loop_node(module_pg_pool, wf_schema, real_flow_id, fleet_worker)
    await _assert_landed_release_preconditions(
        module_pg_pool, wf_schema, str(job.id), fleet_worker, job.attempt
    )

    with patch.object(ledger_module, "claim_step_ledger", ledgerless_iteration_claim):
        captured, deps = await _drive_through_consumer_loop(
            module_pg_pool,
            module_pg_schema.pg_dsn,
            wf_schema,
            job,
            fleet_worker,
            hang_guard_s=60,
        )

    await _assert_bounded_landing(
        module_pg_pool, wf_schema, str(job.id), claimed_attempt=job.attempt
    )
    _assert_bounded_vocabulary(captured, deps)
    # THE REFUSAL'S OWN EVIDENCE: the iteration claim was minted (a real
    # ledger row) and its terminal was NEVER written — the identity-less
    # terminal is the refusal, not a silently succeeded iteration.
    iter0 = await module_pg_pool.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'counter.iter0'""",
        real_flow_id,
    )
    assert iter0 == "running", iter0


async def test_carry_contract_refusal_reaches_the_bounded_release(
    wf_schema: str, module_pg_pool: Any, module_pg_schema: Any
) -> None:
    """`LoopCarryContractError` — a body whose Refine feedback the
    declared carry type cannot re-hydrate at the next iteration's
    boundary. The contract's loud refusal routes through the fleet
    seam's broad catch into the bounded release, never the generic
    owner whose refund/reclaim would loop a deterministic refusal."""

    async def poison_the_carry(ctx: Any, carry: object) -> object:
        del ctx, carry
        # the jsonb boundary cannot honor the declared model from this
        # scalar — the next iteration's rehydrate is the contract's
        # refusal
        return Refine("a scalar the declared carry type cannot honor")

    name = f"loop_refusal_carry_{_new_uuid_str()}"
    app = _make_loop_app(poison_the_carry, name=name, carry_type=_Carry, initial=_Carry(acc=0))
    fleet_worker = _fleet_worker_id()
    runner = FlowRunner(app.get(name), module_pg_pool, wf_schema, worker_id=fleet_worker)
    flow_id = (await runner.create_flow()).flow_id
    real_flow_id = str(flow_id)

    job = await _fleet_claim_loop_node(module_pg_pool, wf_schema, real_flow_id, fleet_worker)
    await _assert_landed_release_preconditions(
        module_pg_pool, wf_schema, str(job.id), fleet_worker, job.attempt
    )

    captured, deps = await _drive_through_consumer_loop(
        module_pg_pool,
        module_pg_schema.pg_dsn,
        wf_schema,
        job,
        fleet_worker,
        hang_guard_s=60,
    )

    await _assert_bounded_landing(
        module_pg_pool, wf_schema, str(job.id), claimed_attempt=job.attempt
    )
    _assert_bounded_vocabulary(captured, deps)
    # THE REFUSAL'S OWN EVIDENCE: the poisoning iteration recorded its
    # truth (a succeeded terminal carrying the scalar feedback), the
    # advance wrote the poisoned carry onto the row, and the NEXT
    # iteration's claim was minted but never terminalized — the
    # rehydrate refused before the body could run again.
    iter0 = await module_pg_pool.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'counter.iter0'""",
        real_flow_id,
    )
    assert iter0 == "succeeded", iter0
    poisoned = await module_pg_pool.fetchval(
        f"""SELECT metadata->>'carry' FROM "{wf_schema}".jobs WHERE id = $1""",
        job.id,
    )
    assert poisoned == "a scalar the declared carry type cannot honor", poisoned
    iter1 = await module_pg_pool.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'counter.iter1'""",
        real_flow_id,
    )
    assert iter1 == "running", iter1
