"""P5 cancellation carrier and durable terminal-effect behavior on real PG.

Fleet rows are acquired by the production dispatch SQL and executed by distinct
registered worker identities; no in-memory backend or engine-state test double
stands in for those claims.  The two-process crash matrix remains a separate
system lane.
"""

# ruff: noqa: S608  # fixture-generated schema identifiers; all values are bound

from __future__ import annotations

import asyncio
import enum
import json
from datetime import timedelta
from typing import Any, cast

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import register_worker
from taskq.workflows import (
    Chain,
    ExecutionPolicy,
    ExhaustionEvent,
    FlowRunner,
    Promise,
    RetrySpec,
    Step,
    StepContext,
    TerminalEffect,
    WorkflowApp,
    WorkflowPolicyError,
    build,
    chain_source,
    chain_start,
)
from taskq.workflows._sweep import drain_outbox
from taskq.workflows.api._ctx import build_step_context


class _Outcome(enum.Enum):
    DONE = "done"


class _Payload(BaseModel):
    value: int = 1


def _document(raw: object) -> dict[str, object]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return cast("dict[str, object]", decoded)


def _settings(schema: str, dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema, "HEARTBEAT_INTERVAL": 0.05}
    )


def _deps(pool: asyncpg.Pool, schema: str, dsn: str) -> WorkerDeps:
    return WorkerDeps(
        settings=_settings(schema, dsn),
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )


async def _workers(pool: asyncpg.Pool, schema: str, dsn: str, *, count: int = 2) -> list[JobId]:
    workers = [
        JobId(await register_worker(pool, _settings(schema, dsn), workflow_execution=True))
        for _ in range(count)
    ]
    assert len(set(workers)) == count
    return workers


async def _claim(
    pool: asyncpg.Pool,
    schema: str,
    worker: JobId,
    *,
    timeout_s: float = 10,
) -> dict[str, Any]:
    """One actual pending workflow claim, retrying only a not-yet-due row."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    from taskq.backend._sweeps import sweep_scheduled_to_pending

    while loop.time() < deadline:
        async with pool.acquire() as sweep_conn:
            await sweep_scheduled_to_pending(sweep_conn, schema=schema)
        rows = await dispatch_batch(
            pool,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
            queues=["default"],
            limit_n=1,
            worker_id=worker,
            lock_lease=timedelta(seconds=30),
        )
        if rows:
            return dict(rows[0])
        await asyncio.sleep(0.005)
    final = await pool.fetch(
        f"SELECT id,step_key,status,actor,queue,scheduled_at,attempt,deps_pending,"
        f"cancel_phase,required_worker_schema,assignment_routed,"
        f'locked_by_worker,lock_expires_at,actor,retry_kind FROM "{schema}".jobs '
        "ORDER BY created_at"
    )
    raise TimeoutError(
        f"worker {worker} found no due workflow row within {timeout_s}s; "
        f"rows={[dict(row) for row in final]!r}"
    )


async def _execute(
    pool: asyncpg.Pool,
    schema: str,
    workers: list[JobId],
    runners: list[FlowRunner],
    expected_step: str,
) -> tuple[dict[str, Any], str]:
    worker_index = len(workers) - 1 if expected_step.endswith(".__on_exhaustion") else 0
    claimed = await _claim(pool, schema, workers[worker_index])
    assert claimed["step_key"] == expected_step, (
        f"expected {expected_step!r}, claimed {claimed['step_key']!r} "
        f"attempt={claimed['attempt']} epoch={claimed['claim_epoch']}"
    )
    flow_value = _document(claimed["metadata"]).get("flow_id")
    assert isinstance(flow_value, str)
    outcome = await runners[worker_index].run_fleet_claimed_step(
        JobId(flow_value),
        claimed,
        attempt=int(claimed["attempt"]),
        claim_epoch=int(claimed["claim_epoch"]),
    )
    return claimed, outcome


@pytest.mark.integration
async def test_step_context_unwired_events_are_independent_and_worker_face_is_shared() -> None:
    """Direct contexts report NONE/False and each owns a real unset Event."""
    schema = "unused"
    pool = cast("asyncpg.Pool", object())
    from taskq.workflows._sql import WorkflowSql

    wsql = WorkflowSql.build("public")
    first = build_step_context(
        flow_id=JobId(new_uuid()),
        job_id=JobId(new_uuid()),
        node_key="one",
        attempt=1,
        pool=pool,
        wsql=wsql,
        input=None,
    )
    second = build_step_context(
        flow_id=first.flow_id,
        job_id=JobId(new_uuid()),
        node_key="two",
        attempt=1,
        pool=pool,
        wsql=wsql,
        input=None,
    )
    from taskq.context import CancelOrigin

    assert schema == "unused"
    assert first.cancel_event is not second.cancel_event
    assert not first.cancel_event.is_set() and not second.cancel_event.is_set()
    assert (first.cancellation_requested, first.cancel_origin) == (False, CancelOrigin.NONE)
    assert (first.cancel_reason, first.cancel_principal) == (None, None)

    class _Face:
        def __init__(self) -> None:
            self.cancel_event = asyncio.Event()
            self._origin = CancelOrigin.NONE

        @property
        def cancel_origin(self) -> CancelOrigin:
            return self._origin

        def _set_cancel_origin(self, origin: CancelOrigin) -> None:
            self._origin = origin

    from taskq.workflows._cancellation import WorkflowCancelDetail

    face = _Face()
    detail = WorkflowCancelDetail()
    wired = build_step_context(
        flow_id=first.flow_id,
        job_id=first.job_id,
        node_key="one",
        attempt=1,
        pool=pool,
        wsql=wsql,
        input=None,
        cancel_face=face,
        cancel_detail=detail,
    )
    detail.reason = "bounded operator reason"
    detail.principal = "normalized-subject"
    face._set_cancel_origin(CancelOrigin.OPERATOR)
    face.cancel_event.set()
    assert wired.cancel_event is face.cancel_event
    assert wired.cancellation_requested
    assert wired.cancel_origin is CancelOrigin.OPERATOR
    assert wired.cancel_reason == "bounded operator reason"
    assert wired.cancel_principal == "normalized-subject"


@pytest.mark.integration
async def test_durable_operator_observer_delivers_exact_late_registration(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    """An unregistered first sight is not delivery; registration later gets proof."""
    worker = (await _workers(wf_pool, wf_schema, pg_dsn, count=1))[0]
    deps = _deps(wf_pool, wf_schema, pg_dsn)
    registry = deps.active_jobs
    app = WorkflowApp()
    name = f"late_cancel_{new_uuid().hex}"

    async def body(ctx: StepContext) -> None:
        return None

    @app.workflow(name)
    def flow() -> Promise[object]:
        from taskq.workflows import step

        return build(step(body, key="work"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema, worker_id=worker)
    flow_id = (await runner.create_flow()).flow_id
    await wf_pool.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor,queue) VALUES ($1,$2) '
        "ON CONFLICT (actor) DO NOTHING",
        "wf",
        "default",
    )
    claimed = await _claim(wf_pool, wf_schema, worker)
    job_id = JobId(claimed["id"])
    await runner.cancel_workflow(flow_id, reason="stop this run", principal="operator-a")

    # No active entry is present. The first read preserves proof for the actual
    # future entry and does not mark the vanilla cancel phase/force a task.
    controller = make_cancel_controller(deps, worker, cast("Any", object()))
    async with wf_pool.acquire() as conn, conn.transaction():
        await controller.run_in_tx(conn)  # type: ignore[arg-type]
    assert registry.get(job_id) is None

    task = asyncio.create_task(asyncio.sleep(30))
    ctx = JobContextForTest(job_id=job_id, attempt=1, claim_epoch=1)
    from taskq.workflows._cancellation import WorkflowJobRegistration

    registration = WorkflowJobRegistration(flow_id=flow_id)
    entry = await registry.register(job_id, task, ctx.context, workflow=registration)
    try:
        assert entry.cancel_origin.value == 1
        assert ctx.context.cancel_event.is_set()
        assert registration.detail.reason == "stop this run"
        assert registration.detail.principal == "operator-a"
        assert entry.cancel_phase.value == 0
        root = await wf_conn.fetchrow(
            f'SELECT status::text AS status, error_message FROM "{wf_schema}".jobs WHERE id=$1',
            flow_id,
        )
        assert root is not None and root["status"] == "cancelled"
        assert root["status"] == "cancelled"  # notify can never replace this proof

        # A foreign marker is not authority. A different live run on the same
        # worker neither signals nor inherits the cancelled run's details.
        other_flow = (await runner.create_flow(run_key=f"unrelated:{new_uuid()}")).flow_id
        other_claim = await _claim(wf_pool, wf_schema, worker)
        other_id = JobId(other_claim["id"])
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs '
            'SET metadata=metadata||\'{"cancel_phase":"foreign"}\'::jsonb WHERE id=$1',
            other_id,
        )
        other_task = asyncio.create_task(asyncio.sleep(30))
        other_ctx = JobContextForTest(
            job_id=other_id,
            attempt=other_claim["attempt"],
            claim_epoch=other_claim["claim_epoch"],
        )
        other_registration = WorkflowJobRegistration(flow_id=other_flow)
        other_entry = await registry.register(
            other_id,
            other_task,
            other_ctx.context,
            workflow=other_registration,
        )
        try:
            async with wf_pool.acquire() as conn, conn.transaction():
                await controller.run_in_tx(conn)  # type: ignore[arg-type]
            assert not other_ctx.context.cancel_event.is_set()
            assert other_entry.cancel_origin.value == 0
            assert other_registration.detail.reason is None
            assert other_registration.detail.principal is None
        finally:
            other_task.cancel()
            await asyncio.gather(other_task, return_exceptions=True)
            await registry.deregister(other_id, other_entry)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await registry.deregister(job_id, entry)


class JobContextForTest:
    """Construct the actual vanilla carrier without changing its state model."""

    def __init__(self, *, job_id: JobId, attempt: int, claim_epoch: int) -> None:
        from taskq.context import JobContext
        from taskq.obs import get_logger

        self.context = JobContext[_Payload](
            job_id=job_id,
            actor="wf",
            queue="default",
            attempt=attempt,
            claim_epoch=claim_epoch,
            worker_id=JobId(new_uuid()),
            payload=_Payload(),
            jobs=cast("Any", None),
            log=get_logger("p5-test"),
        )


async def _effect_fixture(
    pool: asyncpg.Pool,
    schema: str,
    dsn: str,
    *,
    effect_body: Any,
    work_body: Any | None = None,
    ordinary_sibling: bool = False,
) -> tuple[FlowRunner, list[FlowRunner], list[JobId], JobId, str]:
    suffix = new_uuid().hex
    calls: list[int] = []

    async def work(ctx: StepContext, item: object) -> _Outcome:
        calls.append(1)
        raise RuntimeError("synthetic permanent record failure")

    effect = TerminalEffect(
        body=effect_body,
        policy=ExecutionPolicy(
            actor=f"effect-{suffix}",
            queue="default",
            priority=7,
            start_to_close=timedelta(seconds=5),
            retry=RetrySpec(
                policy=RetryPolicy(
                    max_attempts=2,
                    base=timedelta(seconds=0.001),
                    cap=timedelta(seconds=0.001),
                    jitter=0,
                )
            ),
        ),
    )
    chain = Chain(
        name=f"effect-chain-{suffix}",
        start="work",
        steps={
            "work": Step(
                body=work if work_body is None else work_body,
                outcomes=_Outcome,
                policy=ExecutionPolicy(
                    actor="wf",
                    retry=RetrySpec(
                        policy=RetryPolicy(max_attempts=1, jitter=0),
                        on_exhaustion=effect,
                    ),
                ),
            )
        },
    )
    app = WorkflowApp()
    name = f"effect_flow_{suffix}"

    async def source(ctx: StepContext) -> None:
        records = [17, 18] if ordinary_sibling else [17]
        await ctx.emit_batch(
            [
                chain_start(chain, {"record": record}, map_index=record, trace_id=f"r{record}")
                for record in records
            ],
            cursor={"page": 0},
        )

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source, key="source"))

    compiled = app.get(name)
    creator = FlowRunner(compiled, pool, schema)
    flow_id = (await creator.create_flow()).flow_id
    for actor in ("wf", f"effect-{suffix}"):
        await pool.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1,$2) '
            "ON CONFLICT (actor) DO NOTHING",
            actor,
            "default",
        )
    workers = await _workers(pool, schema, dsn)
    runners = [FlowRunner(compiled, pool, schema, worker_id=worker) for worker in workers]
    return runners[0], runners, workers, flow_id, suffix


@pytest.mark.integration
async def test_terminal_effect_intent_job_and_bounded_replay_are_idempotent(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    """One intent/job; body replay is visible while same-DB closure stays once."""
    domain_table = f"p5_effect_domain_{new_uuid().hex[:8]}"
    await wf_conn.execute(
        f'CREATE TABLE "{wf_schema}"."{domain_table}" '
        "(effect_key text PRIMARY KEY, flow_id uuid NOT NULL, "
        "record_id int NOT NULL, error_class text NOT NULL, closes int NOT NULL)"
    )
    invocations: list[ExhaustionEvent] = []

    async def close_domain(ctx: StepContext, event: ExhaustionEvent) -> None:
        assert ctx.queue == "default"
        invocations.append(event)
        # The application marker and its domain effect are one real PG tx. The
        # engine does not pretend the callback itself is externally exactly-once.
        effect_key = f"{event.flow_id}:{event.run_epoch}:{event.node_id}:on_exhaustion"
        async with wf_pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f'INSERT INTO "{wf_schema}"."{domain_table}" '
                "(effect_key,flow_id,record_id,error_class,closes) "
                "VALUES ($1,$2,$3,$4,1) ON CONFLICT (effect_key) DO NOTHING",
                effect_key,
                event.flow_id,
                17,
                event.error.error_class,
            )
        if len(invocations) == 1:
            raise ConnectionError("synthetic replay after application commit")

    creator, runners, workers, flow_id, _suffix = await _effect_fixture(
        wf_pool, wf_schema, pg_dsn, effect_body=close_domain
    )
    assert (
        await wf_conn.fetchval(
            f'SELECT required_worker_schema FROM "{wf_schema}".jobs WHERE id=$1', flow_id
        )
        == 2
    )
    assert (
        await wf_conn.fetchval(f'SELECT run_epoch FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == 1
    )

    _, source_outcome = await _execute(wf_pool, wf_schema, workers, runners, "source")
    assert source_outcome == "succeeded"
    failed_claim, failed_outcome = await _execute(wf_pool, wf_schema, workers, runners, "work")
    assert failed_outcome == "laddered"
    failed_id = JobId(failed_claim["id"])
    intent = await wf_conn.fetchrow(
        f'SELECT id, join_job_id, delivered, bindings FROM "{wf_schema}".wf_outbox '
        "WHERE flow_id=$1",
        flow_id,
    )
    assert intent is not None and intent["join_job_id"] == failed_id
    assert intent["delivered"] is False
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id=$1 AND job_id=$2 AND status='failed'",
            flow_id,
            failed_id,
        )
        == 1
    )
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_attempts '
            "WHERE job_id=$1 AND outcome='failed'",
            failed_id,
        )
        == 1
    )
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_events '
            "WHERE job_id=$1 AND detail->>'to_state'='failed'",
            failed_id,
        )
        == 1
    )
    assert invocations == []
    from taskq.workflows._effects import has_unfinished_terminal_effects

    assert await has_unfinished_terminal_effects(wf_conn, creator.wsql, flow_id)

    assert await drain_outbox(wf_pool, creator.wsql) == 1
    # Reconciliation/re-drain cannot materialize a sibling job.
    assert await drain_outbox(wf_pool, creator.wsql) == 0
    effect_rows = await wf_conn.fetch(
        f"SELECT id, status::text AS status, attempt, max_attempts, priority, "
        f'required_worker_schema, metadata FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
        flow_id,
    )
    assert len(effect_rows) == 1
    effect_row = effect_rows[0]
    assert effect_row["required_worker_schema"] == 2
    assert effect_row["priority"] == 7 and effect_row["max_attempts"] == 2
    metadata = _document(effect_row["metadata"])
    assert metadata["wf_after_terminal_effect"] is True

    while effect_row["status"] != "succeeded":
        claimed, outcome = await _execute(
            wf_pool, wf_schema, workers, runners, "work.__on_exhaustion"
        )
        assert claimed["id"] == effect_row["id"]
        assert outcome == "laddered" or outcome == "succeeded"
        effect_row = await wf_conn.fetchrow(
            f'SELECT id, status::text AS status FROM "{wf_schema}".jobs WHERE id=$1',
            effect_row["id"],
        )
        assert effect_row is not None
        if effect_row["status"] == "failed":
            terminal_failure = await wf_conn.fetchrow(
                f"SELECT status,error_class,error_message,attempt,finished_at "
                f'FROM "{wf_schema}".jobs WHERE id=$1',
                effect_row["id"],
            )
            assert terminal_failure is not None
            raise AssertionError(
                "effect's second bounded attempt failed instead of replaying closure: "
                f"{dict(terminal_failure)!r}; invocations={invocations!r}"
            )
    assert len(invocations) == 2
    assert invocations[0].reason == "retry_exhausted"
    assert invocations[0].record == {"record": 17}
    assert invocations[0].attempt == 1
    assert invocations[0].run_epoch == 1
    assert invocations[0].error.error_class == "RuntimeError"
    assert not await has_unfinished_terminal_effects(wf_conn, creator.wsql, flow_id)
    assert (
        await wf_conn.fetchval(
            f'SELECT COALESCE(sum(closes),0) FROM "{wf_schema}"."{domain_table}"'
        )
        == 1
    )
    from taskq.workflows._sweep import sweep_join_rederive

    # The actual fleet's maintenance arm, not a fixture's status rewrite.
    await sweep_join_rederive(wf_pool, creator.wsql)
    assert (
        await wf_conn.fetchval(f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == "failed"
    )


@pytest.mark.integration
async def test_cancel_before_terminal_cas_creates_no_effect_intent(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    async def close_domain(ctx: StepContext, event: ExhaustionEvent) -> None:
        raise AssertionError("cancel before the claim's disposition cannot invoke an effect")

    entered = asyncio.Event()
    release = asyncio.Event()

    async def fail_after_cancel(ctx: StepContext, item: object) -> _Outcome:
        entered.set()
        await release.wait()
        raise RuntimeError("ordinary body failure after operator cancel")

    creator, runners, workers, flow_id, _suffix = await _effect_fixture(
        wf_pool,
        wf_schema,
        pg_dsn,
        effect_body=close_domain,
        work_body=fail_after_cancel,
    )
    await _execute(wf_pool, wf_schema, workers, runners, "source")
    work_drive = asyncio.create_task(_execute(wf_pool, wf_schema, workers, runners, "work"))
    await asyncio.wait_for(entered.wait(), timeout=10)
    await creator.cancel_workflow(flow_id, reason="before exhaustion", principal="operator-b")
    release.set()
    claimed, outcome = await asyncio.wait_for(work_drive, timeout=10)
    assert outcome == "cancelled"
    assert claimed["status"] == "running"  # dispatch's pre-disposition row shape
    assert (
        await wf_pool.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', claimed["id"]
        )
        == "cancelled"
    )
    assert (
        await wf_pool.fetchval(
            f'SELECT error_class FROM "{wf_schema}".jobs WHERE id=$1', claimed["id"]
        )
        == "WorkflowCancelled"
    )
    assert (
        await wf_pool.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_outbox WHERE flow_id=$1', flow_id
        )
        == 0
    )


@pytest.mark.integration
async def test_committed_intent_survives_cancel_and_runs_once_on_cancelled_root(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.workflows import engine

    observed: list[ExhaustionEvent] = []

    async def effect(ctx: StepContext, event: ExhaustionEvent) -> None:
        observed.append(event)

    creator, runners, workers, flow_id, _suffix = await _effect_fixture(
        wf_pool, wf_schema, pg_dsn, effect_body=effect
    )
    await _execute(wf_pool, wf_schema, workers, runners, "source")
    tx2_entered = asyncio.Event()
    release_tx2 = asyncio.Event()
    original = engine._run_tx2

    async def paused_tx2(conn: Any, wsql: Any, **kwargs: Any) -> Any:
        tx2_entered.set()
        await release_tx2.wait()
        return await original(conn, wsql, **kwargs)

    monkeypatch.setattr(engine, "_run_tx2", paused_tx2)
    work_drive = asyncio.create_task(_execute(wf_pool, wf_schema, workers, runners, "work"))
    await asyncio.wait_for(tx2_entered.wait(), timeout=10)
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_outbox WHERE flow_id=$1', flow_id
        )
        == 1
    )
    await creator.cancel_workflow(flow_id, reason="after intent", principal="operator-c")
    release_tx2.set()
    _claim, outcome = await asyncio.wait_for(work_drive, timeout=10)
    assert outcome == "laddered"  # failure committed before the later cancel
    assert (
        await wf_conn.fetchval(f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == "cancelled"
    )
    assert await drain_outbox(wf_pool, creator.wsql) == 1
    _effect_claim, effect_outcome = await _execute(
        wf_pool, wf_schema, workers, runners, "work.__on_exhaustion"
    )
    assert effect_outcome == "succeeded"
    assert len(observed) == 1
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
            flow_id,
        )
        == 1
    )


@pytest.mark.integration
async def test_materialized_pending_effect_survives_cancel_while_sibling_cancels(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    observed: list[ExhaustionEvent] = []

    async def effect(ctx: StepContext, event: ExhaustionEvent) -> None:
        observed.append(event)

    creator, runners, workers, flow_id, _suffix = await _effect_fixture(
        wf_pool, wf_schema, pg_dsn, effect_body=effect, ordinary_sibling=True
    )
    await _execute(wf_pool, wf_schema, workers, runners, "source")
    failed_claim, outcome = await _execute(wf_pool, wf_schema, workers, runners, "work")
    assert outcome == "laddered"
    assert await drain_outbox(wf_pool, creator.wsql) == 1
    effect_row = await wf_conn.fetchrow(
        f"SELECT id,idempotency_key,status::text AS status,attempt,cancel_phase,metadata "
        f'FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
        flow_id,
    )
    assert effect_row is not None
    assert effect_row["status"] == "pending"
    effect_id = JobId(effect_row["id"])
    original_key = effect_row["idempotency_key"]
    assert original_key is not None
    effect_metadata = _document(effect_row["metadata"])
    sibling = await wf_conn.fetchrow(
        f'SELECT id,status::text AS status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work' "
        "AND id<>$2 AND status='pending'",
        flow_id,
        JobId(failed_claim["id"]),
    )
    assert sibling is not None

    # Root plus the genuine pending map sibling flip; the materialized effect
    # is neither terminalized nor counted as operator cancellation.
    assert (
        await creator.cancel_workflow(
            flow_id, reason="after pending effect", principal="operator-effect-pending"
        )
        == 2
    )
    after = await wf_conn.fetchrow(
        f"SELECT status::text AS status,attempt,cancel_phase,metadata,idempotency_key "
        f'FROM "{wf_schema}".jobs WHERE id=$1',
        effect_id,
    )
    assert after is not None
    assert after["status"] == "pending"
    assert after["attempt"] == 0 and after["cancel_phase"] == 0
    assert after["idempotency_key"] == original_key
    assert "cancel_phase" not in _document(after["metadata"])
    assert (
        await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', sibling["id"]
        )
        == "cancelled"
    )
    claim, effect_outcome = await _execute(
        wf_pool, wf_schema, workers, runners, "work.__on_exhaustion"
    )
    assert claim["id"] == effect_id and claim["attempt"] == 1
    assert claim["idempotency_key"] == original_key
    assert effect_outcome == "succeeded"
    assert len(observed) == 1
    assert observed[0].node_id == JobId(failed_claim["id"])
    assert observed[0].run_epoch == 1
    assert f"{flow_id}:1:{failed_claim['id']}:on_exhaustion" == effect_metadata["wf_effect_key"]
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_outbox WHERE flow_id=$1', flow_id
        )
        == 1
    )
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
            flow_id,
        )
        == 1
    )


@pytest.mark.integration
async def test_running_effect_keeps_independent_retry_across_parent_cancel(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    observations: list[int] = []

    async def effect(ctx: StepContext, event: ExhaustionEvent) -> None:
        observations.append(ctx.attempt)
        if ctx.attempt == 1:
            entered.set()
            await release.wait()
            raise RuntimeError("effect attempt one failed independently")

    creator, runners, workers, flow_id, _suffix = await _effect_fixture(
        wf_pool, wf_schema, pg_dsn, effect_body=effect
    )
    await _execute(wf_pool, wf_schema, workers, runners, "source")
    await _execute(wf_pool, wf_schema, workers, runners, "work")
    assert await drain_outbox(wf_pool, creator.wsql) == 1
    effect_id = JobId(
        await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
            flow_id,
        )
    )
    first_drive = asyncio.create_task(
        _execute(wf_pool, wf_schema, workers, runners, "work.__on_exhaustion")
    )
    await asyncio.wait_for(entered.wait(), timeout=10)
    assert (
        await creator.cancel_workflow(
            flow_id, reason="during running effect", principal="operator-effect-running"
        )
        == 1
    )
    release.set()
    first_claim, first_outcome = await asyncio.wait_for(first_drive, timeout=10)
    assert first_claim["id"] == effect_id and first_claim["attempt"] == 1
    assert first_outcome == "laddered"
    during_retry = await wf_conn.fetchrow(
        f"SELECT status::text AS status,attempt,cancel_phase,metadata "
        f'FROM "{wf_schema}".jobs WHERE id=$1',
        effect_id,
    )
    assert during_retry is not None
    assert during_retry["status"] == "scheduled" and during_retry["attempt"] == 1
    assert during_retry["cancel_phase"] == 0
    assert "cancel_phase" not in _document(during_retry["metadata"])
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_events '
            "WHERE job_id=$1 AND detail->>'to_state'='cancelled'",
            effect_id,
        )
        == 0
    )
    second_claim, second_outcome = await _execute(
        wf_pool, wf_schema, workers, runners, "work.__on_exhaustion"
    )
    assert second_claim["id"] == effect_id and second_claim["attempt"] == 2
    assert second_outcome == "succeeded"
    assert observations == [1, 2]
    assert (
        await wf_conn.fetchval(f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == "cancelled"
    )


@pytest.mark.integration
async def test_effect_exhaustion_after_parent_cancel_preserves_real_failure(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from taskq.workflows import engine

    observations: list[int] = []

    async def effect(ctx: StepContext, event: ExhaustionEvent) -> None:
        observations.append(ctx.attempt)
        raise RuntimeError("the independent effect failed")

    creator, runners, workers, flow_id, _suffix = await _effect_fixture(
        wf_pool, wf_schema, pg_dsn, effect_body=effect
    )
    await _execute(wf_pool, wf_schema, workers, runners, "source")
    tx2_entered = asyncio.Event()
    release_tx2 = asyncio.Event()
    original = engine._run_tx2

    async def paused_tx2(conn: Any, wsql: Any, **kwargs: Any) -> Any:
        tx2_entered.set()
        await release_tx2.wait()
        return await original(conn, wsql, **kwargs)

    monkeypatch.setattr(engine, "_run_tx2", paused_tx2)
    work_drive = asyncio.create_task(_execute(wf_pool, wf_schema, workers, runners, "work"))
    await asyncio.wait_for(tx2_entered.wait(), timeout=10)
    await creator.cancel_workflow(
        flow_id, reason="after committed intent", principal="operator-effect-failure"
    )
    release_tx2.set()
    await asyncio.wait_for(work_drive, timeout=10)
    assert await drain_outbox(wf_pool, creator.wsql) == 1
    effect_id = JobId(
        await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
            flow_id,
        )
    )
    for expected_attempt in (1, 2):
        claim, result = await _execute(wf_pool, wf_schema, workers, runners, "work.__on_exhaustion")
        assert claim["id"] == effect_id and claim["attempt"] == expected_attempt
        assert result == "laddered"
    await asyncio.sleep(0.01)  # any invalid third scheduled row would become due

    row = await wf_conn.fetchrow(
        f"SELECT status::text AS status,attempt,error_class,error_message "
        f'FROM "{wf_schema}".jobs WHERE id=$1',
        effect_id,
    )
    assert row is not None
    assert row["status"] == "failed" and row["attempt"] == 2
    assert row["error_class"] == "RuntimeError"
    assert row["error_message"] == "the independent effect failed"
    attempts = await wf_conn.fetch(
        f"SELECT attempt,outcome::text AS outcome,error_class,error_message "
        f'FROM "{wf_schema}".job_attempts WHERE job_id=$1 ORDER BY attempt',
        effect_id,
    )
    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt["attempt"] == 2
    assert attempt["outcome"] == "failed" and attempt["error_class"] == "RuntimeError"
    assert attempt["error_message"] == "the independent effect failed"
    ledgers = await wf_conn.fetch(
        f"SELECT attempt,status::text AS status,error_class,error_message,capture "
        f'FROM "{wf_schema}".wf_step_ledger WHERE job_id=$1 ORDER BY attempt',
        effect_id,
    )
    assert len(ledgers) == 2
    for ledger in ledgers:
        assert ledger["status"] == "failed" and ledger["error_class"] == "RuntimeError"
        assert ledger["error_message"] == "the independent effect failed"
    assert [ledger["attempt"] for ledger in ledgers] == [1, 2]
    final_capture = _document(ledgers[-1]["capture"])
    assert "the independent effect failed" in str(final_capture)
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_events '
            "WHERE job_id=$1 AND detail->>'to_state'='failed' "
            "AND detail->>'error_class'='RuntimeError'",
            effect_id,
        )
        == 1
    )
    assert observations == [1, 2]
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
            flow_id,
        )
        == 1
    )
    intents = await wf_conn.fetch(
        f'SELECT delivered,bindings FROM "{wf_schema}".wf_outbox WHERE flow_id=$1', flow_id
    )
    assert len(intents) == 1 and intents[0]["delivered"] is True
    assert (
        await wf_conn.fetchval(f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == "cancelled"
    )


@pytest.mark.integration
async def test_lease_recovery_of_operator_marked_workflow_has_cancel_audit_totality(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    from taskq.backend._sweeps import sweep_expired_locks

    entered = asyncio.Event()
    release = asyncio.Event()
    worker = (await _workers(wf_pool, wf_schema, pg_dsn, count=1))[0]
    app = WorkflowApp()
    name = f"cancel_lease_audit_{new_uuid().hex}"

    async def body(ctx: StepContext) -> None:
        entered.set()
        await release.wait()

    @app.workflow(name)
    def flow() -> Promise[object]:
        from taskq.workflows import step

        return build(step(body, key="work", max_attempts=1))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema, worker_id=worker)
    flow_id = (await runner.create_flow()).flow_id
    drive = asyncio.create_task(runner.tick(flow_id))
    await asyncio.wait_for(entered.wait(), timeout=10)
    await runner.cancel_workflow(flow_id, reason="lease recovery", principal="operator-d")
    node_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work'",
        flow_id,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET lock_expires_at=clock_timestamp()-interval '1 second' "
        "WHERE id=$1",
        node_id,
    )
    assert (
        await sweep_expired_locks(
            wf_conn,
            timedelta(seconds=30),
            timedelta(seconds=30),
            schema=wf_schema,
        )
        == 1
    )
    release.set()
    await asyncio.wait_for(drive, timeout=10)
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status,attempt,error_class FROM "{wf_schema}".jobs WHERE id=$1',
        node_id,
    )
    assert row is not None
    assert dict(row) == {
        "status": "cancelled",
        "attempt": 1,
        "error_class": "WorkflowCancelled",
    }
    attempt = await wf_conn.fetchrow(
        f'SELECT outcome,error_class FROM "{wf_schema}".job_attempts WHERE job_id=$1', node_id
    )
    ledger = await wf_conn.fetchrow(
        f'SELECT status,error_class FROM "{wf_schema}".wf_step_ledger WHERE job_id=$1', node_id
    )
    events = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".job_events '
        "WHERE job_id=$1 AND detail->>'to_state'='cancelled'",
        node_id,
    )
    assert attempt is not None and dict(attempt) == {
        "outcome": "cancelled",
        "error_class": "WorkflowCancelled",
    }
    assert ledger is not None and dict(ledger) == {
        "status": "cancelled",
        "error_class": "WorkflowCancelled",
    }
    assert events == 1


@pytest.mark.integration
async def test_explicit_reopen_separates_effect_episode_without_revoking_old_intent(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    from taskq.workflows._sweep import sweep_join_rederive
    from taskq.workflows.api._runner_exit import retry_workflow_node

    async def effect(ctx: StepContext, event: ExhaustionEvent) -> None:
        return None

    creator, runners, workers, flow_id, _suffix = await _effect_fixture(
        wf_pool, wf_schema, pg_dsn, effect_body=effect
    )
    await _execute(wf_pool, wf_schema, workers, runners, "source")
    await _execute(wf_pool, wf_schema, workers, runners, "work")
    await sweep_join_rederive(wf_pool, creator.wsql)
    assert (
        await wf_conn.fetchval(f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == "failed"
    )
    assert (
        await wf_conn.fetchval(f'SELECT run_epoch FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == 1
    )
    assert (
        await retry_workflow_node(
            wf_pool,
            schema=wf_schema,
            flow_id=flow_id,
            node_key="work",
            reason="deliberate explicit reopen",
            principal="operator-reopen",
        )
        == 1
    )
    assert (
        await wf_conn.fetchval(f'SELECT run_epoch FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == 2
    )
    assert (
        await wf_conn.fetchval(f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', flow_id)
        == "running"
    )
    await _execute(wf_pool, wf_schema, workers, runners, "work")
    intents = await wf_conn.fetch(
        f'SELECT id,join_job_id,bindings FROM "{wf_schema}".wf_outbox WHERE flow_id=$1 ORDER BY id',
        flow_id,
    )
    epochs: list[int] = []
    for row in intents:
        effect_key = _document(row["bindings"])["wf_effect_key"]
        assert isinstance(effect_key, str)
        epochs.append(int(effect_key.split(":")[1]))
    epochs.sort()
    assert len(intents) == 2 and epochs == [1, 2]
    assert len({row["join_job_id"] for row in intents}) == 1
    assert await drain_outbox(wf_pool, creator.wsql) == 2
    effect_jobs = await wf_conn.fetch(
        f'SELECT id,idempotency_key,status::text AS status FROM "{wf_schema}".jobs '
        "WHERE metadata->>'flow_id'=$1 AND step_key='work.__on_exhaustion' ORDER BY id",
        str(flow_id),
    )
    assert len(effect_jobs) == 2
    assert len({row["idempotency_key"] for row in effect_jobs}) == 2


@pytest.mark.integration
async def test_direct_per_row_cancel_refuses_and_preserves_node_state(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    pg_dsn: str,
) -> None:
    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.client._jobs import JobsClient
    from taskq.exceptions import WorkflowRunCancellationRequiredError

    worker = (await _workers(wf_pool, wf_schema, pg_dsn, count=1))[0]
    app = WorkflowApp()
    name = f"direct_cancel_refusal_{new_uuid().hex}"

    async def body(ctx: StepContext) -> None:
        return None

    @app.workflow(name)
    def flow() -> Promise[object]:
        from taskq.workflows import step

        return build(step(body, key="work"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema, worker_id=worker)
    flow_id = (await runner.create_flow()).flow_id
    node_id = await wf_pool.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work'",
        flow_id,
    )
    backend = PostgresBackend(
        _deps(wf_pool, wf_schema, pg_dsn),
        SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )
    client = JobsClient(backend)
    with pytest.raises(WorkflowRunCancellationRequiredError, match="cancel_workflow_run"):
        await client.cancel(JobId(node_id), reason="wrong scope")
    row = await wf_pool.fetchrow(
        f"SELECT status::text AS status,cancel_phase,cancel_requested_at "
        f'FROM "{wf_schema}".jobs WHERE id=$1',
        node_id,
    )
    assert row is not None
    assert dict(row) == {"status": "pending", "cancel_phase": 0, "cancel_requested_at": None}


def test_terminal_effect_validation_walk_rejects_wrong_arity_and_deps() -> None:
    from taskq.workflows.api._validate import WorkflowValidationError

    app = WorkflowApp()
    name = f"bad_effect_{new_uuid().hex}"

    async def bad_effect(ctx: StepContext, event: ExhaustionEvent, deps: object) -> None:
        return None

    async def failing(ctx: StepContext, item: object) -> _Outcome:
        return _Outcome.DONE

    chain = Chain(
        name=f"bad-effect-chain-{new_uuid().hex}",
        start="work",
        steps={
            "work": Step(
                body=failing,
                outcomes=_Outcome,
                policy=ExecutionPolicy(
                    retry=RetrySpec(
                        on_exhaustion=TerminalEffect(
                            body=bad_effect,
                            policy=ExecutionPolicy(actor="effect", queue="default"),
                        )
                    )
                ),
            )
        },
    )

    async def source(ctx: StepContext) -> None:
        return None

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source, key="source"))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        app.get(name)


def test_terminal_effect_rejects_missing_bounding_or_recursive_callback() -> None:
    async def effect(ctx: StepContext, event: ExhaustionEvent) -> None:
        return None

    with pytest.raises(WorkflowPolicyError):
        ExecutionPolicy(retry=RetrySpec(on_exhaustion=cast("Any", object())))
    with pytest.raises(WorkflowPolicyError):
        TerminalEffect(body=effect, policy=ExecutionPolicy())
    inner = TerminalEffect(
        body=effect,
        policy=ExecutionPolicy(actor="inner-effect", queue="default"),
    )
    with pytest.raises(WorkflowPolicyError):
        TerminalEffect(
            body=effect,
            policy=ExecutionPolicy(
                actor="effect",
                queue="default",
                retry=RetrySpec(on_exhaustion=inner),
            ),
        )
