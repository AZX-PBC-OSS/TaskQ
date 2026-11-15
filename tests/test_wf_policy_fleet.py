"""Real-Postgres/two-claimant execution-policy controls.

These are real dispatch claims plus real workflow bodies and reservation SQL.
They are deliberately focused controls, not a claim of a two-process soak.
"""

from __future__ import annotations

import asyncio
import enum
import json
from datetime import datetime, timedelta
from typing import Any, Protocol, cast

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation, sync_slots
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import (
    _flow_denial_snooze_metadata,
    _flow_rate_limit_gate,
    register_worker,
)
from taskq.worker.startup import sync_actor_config
from taskq.workflows import (
    Chain,
    ExecutionPolicy,
    FiniteReservationCohort,
    FlowRunner,
    Promise,
    Route,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
)
from taskq.workflows._worker_execution import (
    collect_workflow_reservations,
    project_workflow_actor_configs,
    record_compiled,
)

_LOG = structlog.get_logger("wf-policy-fleet-test")


class _NamedCompiled(Protocol):
    name: str


class _Outcome(enum.Enum):
    NEXT = "next"
    DONE = "done"


class _CohortPayload(BaseModel):
    cohort: str
    record: int


def _metadata(raw: object) -> dict[str, object]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return cast("dict[str, object]", decoded)


def _job_row(row: dict[str, Any]) -> JobRow:
    """Decode the real dispatch row into the existing worker read model."""
    scheduled = row["scheduled_at"] or row["started_at"]
    assert isinstance(scheduled, datetime)
    assert isinstance(row["created_at"], datetime)
    return JobRow(
        id=JobId(row["id"]),
        actor=row["actor"],
        queue=row["queue"],
        payload=_metadata(row["payload"]),
        payload_schema_ver=int(row["payload_schema_ver"]),
        status="running",
        priority=int(row["priority"]),
        attempt=int(row["attempt"]),
        max_attempts=int(row["max_attempts"]),
        retry_kind=row["retry_kind"],
        created_at=row["created_at"],
        scheduled_at=scheduled,
        start_to_close=row["start_to_close"],
        lock_expires_at=row["lock_expires_at"],
        retry_base=row["retry_base"],
        retry_cap=row["retry_cap"],
        retry_backoff=row["retry_backoff"],
        retry_jitter=float(row["retry_jitter"]),
        metadata=_metadata(row["metadata"]),
        trace_id=row["trace_id"],
        claim_epoch=int(row["claim_epoch"]),
    )


def _settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": schema,
            "MAX_CONCURRENCY": 1,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
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


async def _register_workers(pool: asyncpg.Pool, schema: str, pg_dsn: str) -> list[JobId]:
    workers: list[JobId] = []
    for _ in range(2):
        worker = await register_worker(
            pool,
            WorkerSettings.load_from_dict({"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema}),
            workflow_execution=True,
        )
        workers.append(JobId(worker))
    assert len(set(workers)) == 2
    return workers


async def _register_projection(
    compiled: _NamedCompiled,
    actors: tuple[tuple[str, str], ...],
    pool: asyncpg.Pool,
    schema: str,
) -> RateLimitRegistry:
    """Project cohorts and materialize every declared reservation through boot collectors."""
    # The test app was already registered by app.get. Explicit record_compiled is
    # idempotent and makes the collector input visible at this focused door.
    record_compiled(compiled.name, compiled)
    projected = {config.actor: config for config in project_workflow_actor_configs()}
    for actor, queue in actors:
        assert actor in projected, f"the resolver did not project {actor!r}"
        assert projected[actor].queue == queue
    async with pool.acquire() as conn:
        await sync_actor_config(
            conn,
            [projected[actor] for actor, _queue in actors],
            schema=schema,
        )
    registry = RateLimitRegistry()
    declarations, unknown = collect_workflow_reservations(registry)
    assert unknown == []
    rebound: list[ConcurrencyReservation] = []
    for declaration in declarations:
        reservation = declaration.rebind_schema(schema)
        registry.register(reservation)
        rebound.append(reservation)
    await sync_slots(rebound, pool, schema=schema)
    return registry


async def _tick_source(
    *,
    flow_id: JobId,
    step_key: str,
    worker_index: int,
    workers: list[JobId],
    runners: list[FlowRunner],
    conn: asyncpg.Connection,
    schema: str,
) -> None:
    rows = await dispatch_batch(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=["default"],
        limit_n=1,
        worker_id=workers[worker_index],
        lock_lease=timedelta(seconds=30),
    )
    assert len(rows) == 1 and rows[0]["step_key"] == step_key, (
        f"expected the next claim {step_key!r}; got "
        f"{[(row['step_key'], row['map_index'], row['priority']) for row in rows]!r}"
    )
    outcome = await runners[worker_index].run_fleet_claimed_step(
        flow_id,
        dict(rows[0]),
        attempt=int(rows[0]["attempt"]),
        claim_epoch=int(rows[0]["claim_epoch"]),
    )
    assert outcome == "succeeded", outcome


@pytest.mark.integration
async def test_two_real_claimants_enforce_reserved_target_and_release_all_slots(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """A routed target (not its source/parent) owns the strict reservation."""
    suffix = new_uuid().hex
    guard_name = f"policy-leaf-{suffix}:finite-a"
    guard = FiniteReservationCohort(
        ref=KeyedReservationRef.typed(
            _CohortPayload,
            base_name=f"policy-leaf-{suffix}",
            key_fn=lambda payload: payload.cohort,
            slots=1,
            lease=timedelta(seconds=2),
        ),
        keys=("finite-a",),
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    in_bodies = 0
    maximum_observed = 0
    active_rows: list[JobId] = []

    async def first(ctx: StepContext, item: object) -> _Outcome:
        return _Outcome.NEXT

    async def reserved(ctx: StepContext, item: object) -> _Outcome:
        nonlocal in_bodies, maximum_observed
        active_rows.append(ctx.job_id)
        in_bodies += 1
        maximum_observed = max(maximum_observed, in_bodies)
        entered.set()
        try:
            await release.wait()
        finally:
            in_bodies -= 1
        return _Outcome.DONE

    chain = Chain(
        name=f"fleet-policy-chain-{suffix}",
        start="first",
        steps={
            "first": Step(
                body=first,
                outcomes=_Outcome,
                route=Route({_Outcome.NEXT: "reserved", _Outcome.DONE: None}),
                policy=ExecutionPolicy(actor=f"first-{suffix}", priority=4),
            ),
            "reserved": Step(
                body=reserved,
                outcomes=_Outcome,
                policy=ExecutionPolicy(
                    actor=f"reserved-{suffix}", reservations=(guard,), priority=3
                ),
            ),
        },
    )
    app = WorkflowApp()
    name = f"fleet_policy_{suffix}"

    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [
                chain_start(
                    chain,
                    {"record": index, "cohort": "finite-a"},
                    map_index=index,
                    trace_id=f"r{index}",
                )
                for index in range(2)
            ],
            cursor={"page": 0},
        )

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(
            chain_source(
                chain,
                source,
                key="source",
                policy=ExecutionPolicy(actor=f"source-{suffix}"),
            )
        )

    compiled = app.get(name)
    creator = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await creator.create_flow()).flow_id
    registry = await _register_projection(
        compiled,
        (
            (f"source-{suffix}", "default"),
            (f"first-{suffix}", "default"),
            (f"reserved-{suffix}", "default"),
        ),
        wf_pool,
        wf_schema,
    )
    workers = await _register_workers(wf_pool, wf_schema, pg_dsn)
    runners = [FlowRunner(compiled, wf_pool, wf_schema, worker_id=worker) for worker in workers]
    await _tick_source(
        flow_id=flow_id,
        step_key="source",
        worker_index=0,
        workers=workers,
        runners=runners,
        conn=wf_conn,
        schema=wf_schema,
    )
    for index, _worker in enumerate(workers):
        await _tick_source(
            flow_id=flow_id,
            step_key="first",
            worker_index=index,
            workers=workers,
            runners=runners,
            conn=wf_conn,
            schema=wf_schema,
        )
    rows = await wf_conn.fetch(
        f'SELECT id, map_index, retry_kind, metadata FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'reserved'",
        flow_id,
    )
    assert len(rows) == 2
    assert all(_metadata(row["metadata"])["reservations"] == [guard_name] for row in rows)

    backend = PostgresBackend(
        _deps(wf_pool, wf_schema),
        SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )

    async def consume(worker_index: int) -> str:
        async with wf_pool.acquire() as claim_conn:
            claimed_rows = await dispatch_batch(
                claim_conn,
                sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
                queues=["default"],
                limit_n=1,
                worker_id=workers[worker_index],
                lock_lease=timedelta(seconds=30),
            )
        if not claimed_rows or claimed_rows[0]["step_key"] != "reserved":
            return "not-claimable"
        claimed_dict = dict(claimed_rows[0])
        # Restore explicitly routed dispatch fields (the raw table names use seconds;
        # the JobRow decoder is the actual conversion used by this focused fixture).
        claimed_dict["retry_base"] = timedelta(seconds=float(claimed_dict["retry_base_seconds"]))
        claimed_dict["retry_cap"] = timedelta(seconds=float(claimed_dict["retry_cap_seconds"]))
        job = _job_row(claimed_dict)
        gate = await _flow_rate_limit_gate(
            registry,
            SystemClock(),
            job,
            workers[worker_index],
            _deps(wf_pool, wf_schema),
            job_log=_LOG,
        )
        assert gate.dependency_failure is None, gate.dependency_failure
        if gate.denied is not None:
            verdict = await backend.mark_snoozed(
                job.id,
                workers[worker_index],
                gate.denied.retry_after,
                metadata_update=_flow_denial_snooze_metadata(gate.denied),
                attempt=job.attempt,
                claim_epoch=job.claim_epoch,
            )
            assert verdict != "noop"
            return "denied"
        assert gate.registry is not None and gate.acquired
        try:
            # The public ctx deliberately does not expose worker identity. The
            # body witness below reads the runner through the instance-local map.
            outcome = await runners[worker_index].run_fleet_claimed_step(
                flow_id,
                claimed_dict,
                attempt=job.attempt,
                claim_epoch=job.claim_epoch,
            )
            assert outcome == "succeeded"
            return outcome
        finally:
            await gate.registry.release_for_actor(gate.acquired)

    first_task = asyncio.create_task(consume(0))
    second_task = asyncio.create_task(consume(1))
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        # Reservation headroom may stop the second dispatch before its gate.
        # Wait for that actual non-claim result while the winner remains inside
        # the body; then release and complete both attempts.
        completed, _pending_tasks = await asyncio.wait(
            (first_task, second_task),
            timeout=10,
            return_when=asyncio.FIRST_COMPLETED,
        )
        non_running = [task.result() for task in completed]
        assert non_running == ["not-claimable"], non_running
        release.set()
        outcomes = await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=20)
    finally:
        release.set()
        for task in (first_task, second_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(first_task, second_task, return_exceptions=True)
    assert sorted(outcomes) == ["not-claimable", "succeeded"], outcomes
    assert maximum_observed == 1, maximum_observed
    assert len(active_rows) == 1, active_rows
    held = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".reservation_slots '  # noqa: S608
        "WHERE bucket_name = $1 AND job_id IS NOT NULL",
        guard_name,
    )
    assert held == 0, "the completed/denied claims leaked a reservation lease"

    # The real denial was requeued by mark_snoozed; after the floor, another
    # worker claims the same row and succeeds, using a fresh acquisition.
    async def completion() -> str:
        deadline = asyncio.get_running_loop().time() + 10
        while asyncio.get_running_loop().time() < deadline:
            claimed = await dispatch_batch(
                wf_conn,
                sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
                queues=["default"],
                limit_n=1,
                worker_id=workers[1],
                lock_lease=timedelta(seconds=30),
            )
            if claimed and claimed[0]["step_key"] == "reserved":
                claimed_dict = dict(claimed[0])
                outcome = await runners[1].run_fleet_claimed_step(
                    flow_id,
                    claimed_dict,
                    attempt=int(claimed[0]["attempt"]),
                    claim_epoch=int(claimed[0]["claim_epoch"]),
                )
                assert outcome == "succeeded"
                return outcome
            await asyncio.sleep(0.01)
        final = await wf_conn.fetch(
            f'SELECT status::text AS status, scheduled_at FROM "{wf_schema}".jobs '  # noqa: S608
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'reserved'",
            flow_id,
        )
        raise TimeoutError(
            f"denied reserved row did not become claimable: {[dict(r) for r in final]}"
        )

    await completion()
    statuses = await wf_conn.fetch(
        f'SELECT status::text AS status FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'reserved'",
        flow_id,
    )
    assert [row["status"] for row in statuses] == ["succeeded", "succeeded"]
    released = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".reservation_slots '  # noqa: S608
        "WHERE bucket_name = $1 AND (job_id IS NOT NULL OR held_by_worker_id IS NOT NULL)",
        guard_name,
    )
    assert released == 0


@pytest.mark.integration
async def test_late_policy_priority_beats_older_backlog_under_two_workers(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    suffix = new_uuid().hex
    first_done = asyncio.Event()
    source_entered = asyncio.Event()
    observed_claims: list[tuple[int, str | None, int]] = []

    async def work(ctx: StepContext, item: object) -> _Outcome:
        return _Outcome.DONE

    chain = Chain(
        name=f"priority-policy-{suffix}",
        start="work",
        steps={
            "work": Step(
                body=work,
                outcomes=_Outcome,
                policy=ExecutionPolicy(actor=f"priority-leaf-{suffix}", priority=9),
            )
        },
    )
    app = WorkflowApp()
    name = f"policy_priority_{suffix}"

    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(chain, {"record": 0}, map_index=0, trace_id="r0")],
            cursor={"page": 0},
        )
        source_entered.set()
        await first_done.wait()
        await ctx.emit_batch(
            [chain_start(chain, {"record": 1}, map_index=1, trace_id="r1")],
            cursor={"page": 1},
        )

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(
            chain_source(
                chain,
                source,
                key="source",
                policy=ExecutionPolicy(actor=f"priority-source-{suffix}"),
            )
        )

    compiled = app.get(name)
    creator = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await creator.create_flow()).flow_id
    await _register_projection(
        compiled,
        ((f"priority-source-{suffix}", "default"), (f"priority-leaf-{suffix}", "default")),
        wf_pool,
        wf_schema,
    )
    workers = await _register_workers(wf_pool, wf_schema, pg_dsn)
    runners = [FlowRunner(compiled, wf_pool, wf_schema, worker_id=worker) for worker in workers]
    backlog_actor = f"priority-backlog-{suffix}"
    await wf_pool.execute(
        f"INSERT INTO \"{wf_schema}\".actor_config (actor, queue) VALUES ($1, 'default')",  # noqa: S608
        backlog_actor,
    )
    await wf_conn.executemany(
        f'INSERT INTO "{wf_schema}".jobs '  # noqa: S608
        "(id, actor, queue, status, payload, max_attempts, retry_kind, scheduled_at, "
        "idempotency_scope, idempotency_key, priority) "
        "VALUES ($1, $2, 'default', 'pending', '{}', 3, 'transient', clock_timestamp(), $3, $4, 0)",
        [
            (
                new_uuid(),
                backlog_actor,
                f"priority-backlog:{suffix}",
                f"backlog-{index}",
            )
            for index in range(6)
        ],
    )

    async def claim(worker: int) -> dict[str, Any] | None:
        rows = await dispatch_batch(
            wf_conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
            queues=["default"],
            limit_n=1,
            worker_id=workers[worker],
            lock_lease=timedelta(seconds=30),
        )
        if not rows:
            return None
        row = dict(rows[0])
        observed_claims.append((worker, row["step_key"], int(row["map_index"] or -1)))
        return row

    source_row = await claim(0)
    assert source_row is not None and source_row["step_key"] == "source"
    source_task = asyncio.create_task(
        runners[0].run_fleet_claimed_step(
            flow_id,
            source_row,
            attempt=int(source_row["attempt"]),
            claim_epoch=int(source_row["claim_epoch"]),
        )
    )
    try:
        await asyncio.wait_for(source_entered.wait(), timeout=10)
        first = await claim(1)
        assert first is not None
        assert (first["step_key"], first["map_index"], first["priority"]) == ("work", 0, 9), (
            f"first record did not beat the older priority-0 backlog: {observed_claims!r}"
        )
        assert (
            await runners[1].run_fleet_claimed_step(
                flow_id,
                first,
                attempt=int(first["attempt"]),
                claim_epoch=int(first["claim_epoch"]),
            )
            == "succeeded"
        )
        first_done.set()
        await asyncio.wait_for(source_task, timeout=10)

        # This row is materialized only after create_flow and after the first
        # child's terminal. It too beats the still-older/deeper backlog.
        second = await claim(0)
        assert second is not None
        assert (second["step_key"], second["map_index"], second["priority"]) == ("work", 1, 9), (
            f"late record did not beat the backlog: {observed_claims!r}"
        )
        assert (
            await runners[0].run_fleet_claimed_step(
                flow_id,
                second,
                attempt=int(second["attempt"]),
                claim_epoch=int(second["claim_epoch"]),
            )
            == "succeeded"
        )
    finally:
        first_done.set()
        if not source_task.done():
            source_task.cancel()
        elif not source_task.cancelled():
            source_task.exception()

    assert {worker for worker, _key, _index in observed_claims} == {0, 1}
    backlog_claims = [entry for entry in observed_claims if entry[1] is None]
    assert backlog_claims == []
    pending_backlog = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE actor = $1 AND status = 'pending'",  # noqa: S608
        backlog_actor,
    )
    assert pending_backlog == 6
