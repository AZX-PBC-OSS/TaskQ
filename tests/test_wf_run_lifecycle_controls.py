"""Behavior controls for the run lifecycle (#722 exclusion, #723 admission/events).

These are red-first controls, not an implementation claim.  The first group
uses only behavior the current engine already exposes, so it is a genuine
control against a vacuous "the base already passed" reading:

* ordinary per-node declarations do not serialize two runs' graph stages;
* a cancelled root can be terminal while its Python body is still active.

The remaining controls state the accepted lifecycle contract at its public
surface: run-scoped reservations, producer admission/window identity, the
durable terminal episode, and the declaration-born body-active marker.  A
future package must make those controls pass without changing their
observable assertions.  Direct/managed primitives are used only to drive the
real PG rows/body/finalization behavior; the genuine multi-worker/process
matrix is a separate implementation obligation (see the report).
"""

# ruff: noqa: S608  # Why: the schema identifier is the fixture-derived test identifier; values are $-bound.

from __future__ import annotations

# pyright: reportUntypedFunctionDecorator=false
import asyncio
import enum
import importlib
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, cast

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import JobId, JobRow
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.exceptions import MaxPendingExceededError
from taskq.ratelimit import ConcurrencyReservation
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import (
    Chain,
    ChildOk,
    ExecutionPolicy,
    ExhaustionEvent,
    FlowRunner,
    Promise,
    RetrySpec,
    RunClaim,
    Step,
    StepContext,
    TerminalEffect,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
    map_source,
    retry_workflow_node,
    step,
)
from taskq.workflows._capability import (
    WorkflowWorkerCapabilityError,
    create_guarded_flow,
)

if TYPE_CHECKING:
    from taskq.workflows import (
        WorkflowTerminalEvent,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the future public event type is the controls' subject.
    )

_TERMINAL = frozenset(TERMINAL_STATUSES)
_RUN_SCOPE = "workflow-run:"


class _ChildOut(BaseModel):
    ok: bool = True


class _EffectOutcome(enum.Enum):
    DONE = "done"


async def _row(
    conn: asyncpg.Connection, schema: str, sql: str, *args: object
) -> asyncpg.Record | None:
    return await conn.fetchrow(sql.format(schema=schema), *args)


def _meta(raw: object) -> dict[str, object]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return cast("dict[str, object]", decoded)


async def _root(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> asyncpg.Record:
    row = await _row(
        conn,
        schema,
        "SELECT id,status::text AS status,run_epoch,idempotency_key,metadata,payload,created_at "
        'FROM "{schema}".jobs WHERE id=$1',
        flow_id,
    )
    assert row is not None, f"run {flow_id} has no root row"
    return row


async def _root_status(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> str:
    return str((await _root(conn, schema, flow_id))["status"])


async def _flow_nodes(
    conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT id,step_key,status::text AS status,attempt,metadata FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key <> '__flow__' ORDER BY step_key,id",
        flow_id,
    )


async def _node(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, step_key: str
) -> asyncpg.Record:
    rows = [row for row in await _flow_nodes(conn, schema, flow_id) if row["step_key"] == step_key]
    assert len(rows) == 1, f"run {flow_id} must have exactly one {step_key!r} node; got {rows!r}"
    return rows[0]


async def _wait_until(
    description: str,
    check: Callable[[], Awaitable[bool]],
    *,
    deadline_s: float = 15.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_s
    while loop.time() < deadline:
        if await check():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {description}")


class _GatedTask[T]:
    """Own one gated execution task and always release/reap it on exit.

    The primary assertion/exception wins: cleanup never converts a red body
    failure into a green test, and a cleanup failure is suppressed only while
    preserving an already-active primary failure.
    """

    def __init__(
        self,
        task: asyncio.Task[T],
        *releases: asyncio.Event,
        deadline_s: float = 15.0,
    ) -> None:
        self._task = task
        self._releases = releases
        self._deadline_s = deadline_s

    async def __aenter__(self) -> asyncio.Task[T]:
        return self._task

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        for release in self._releases:
            release.set()
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout=self._deadline_s)
        except TimeoutError:
            if exc_type is None:
                raise
        except BaseException:
            if exc_type is None:
                raise
        finally:
            if not self._task.done():
                self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        return False


def _terminal_event_type() -> type[BaseModel]:
    """The exported typed terminal event; absence is the retained API doorway."""
    module = importlib.import_module("taskq.workflows")
    event_type = getattr(module, "WorkflowTerminalEvent", None)
    assert event_type is not None, (
        "the workflow public surface does not export WorkflowTerminalEvent; #723's "
        "terminal-event declaration has no typed delivery contract"
    )
    return cast("type[BaseModel]", event_type)


def _settlement_actor(  # pyright: ignore[reportUnknownParameterType]  # Why: the future event type is unknown until its export lands.
    name: str,
    queue: str,
    body: Callable[[WorkflowTerminalEvent], Awaitable[None]],  # pyright: ignore[reportUnknownParameterType]  # Why: future event type unknown until export.
) -> ActorRef[WorkflowTerminalEvent, None]:
    """Build the ordinary typed settlement actor the workflow declares."""

    _terminal_event_type()  # the clean public-export doorway before the typed import
    from tests._wf_run_lifecycle_terminal_actor import make_terminal_actor

    return make_terminal_actor(name, queue, body)


async def _event_jobs(
    conn: asyncpg.Connection, schema: str, actor_name: str, workflow: str
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT id,status::text AS status,attempt,metadata,payload FROM "{schema}".jobs '
        "WHERE actor=$1 AND payload->>'workflow'=$2 ORDER BY created_at,id",
        actor_name,
        workflow,
    )


async def _terminal_materialization_seat(runner: FlowRunner, flow_id: JobId) -> None:
    """One explicit production maintenance/drain seat, never an opaque poll.

    P8 must make the terminal-event drain ride the existing ``tick``
    maintenance path (the accepted architecture's existing outbox sweep); this
    control calls that seat exactly once at each readiness observation.
    """
    await runner.tick(flow_id, execute=False)


async def _event_job_for_flow(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    actor_name: str,
    workflow: str,
) -> asyncpg.Record | None:
    jobs = await _event_jobs(conn, schema, actor_name, workflow)
    mine = [job for job in jobs if _meta(job["payload"]).get("flow_id") == str(flow_id)]
    assert len(mine) <= 1, f"expected at most one terminal event job for {flow_id}; got {mine!r}"
    return mine[0] if mine else None


async def _require_event_job(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    actor_name: str,
    workflow: str,
) -> asyncpg.Record:
    job = await _event_job_for_flow(conn, schema, flow_id, actor_name, workflow)
    assert job is not None, f"no terminal event job materialized for {flow_id}"
    assert "flow_id" not in _meta(job["metadata"]), (
        "the event job must not be intercepted as a workflow node by metadata.flow_id"
    )
    return job


def _event(job: asyncpg.Record) -> WorkflowTerminalEvent:  # pyright: ignore[reportUnknownParameterType]  # Why: same future export.
    return cast(
        "WorkflowTerminalEvent", _terminal_event_type().model_validate(_meta(job["payload"]))
    )


async def _worker(pool: asyncpg.Pool, schema: str, dsn: str) -> JobId:
    from taskq.worker.run import register_worker

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema, "HEARTBEAT_INTERVAL": 0.05}
    )
    return JobId(await register_worker(pool, settings, workflow_execution=True))


async def _configure_actor(
    conn: asyncpg.Connection, schema: str, actor_name: str, queue: str
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor,queue) VALUES ($1,$2) '
        "ON CONFLICT (actor) DO NOTHING",
        actor_name,
        queue,
    )


async def _try_claim_one(
    pool: asyncpg.Pool,
    schema: str,
    worker: JobId,
    *,
    queues: Sequence[str],
) -> list[JobRow]:
    """One bounded, non-raising production claim attempt.

    Used for the "competitor cannot enter" observation: an empty result is the
    exclusion fact, never a timeout or a guessed body-side effect.
    """
    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
    from taskq.backend._records import _job_row_from_record

    rows = await dispatch_batch(
        pool,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=list(queues),
        limit_n=1,
        worker_id=worker,
        lock_lease=timedelta(seconds=30),
    )
    return [_job_row_from_record(row) for row in rows]


async def _claim_due(
    pool: asyncpg.Pool,
    schema: str,
    worker: JobId,
    *,
    queues: Sequence[str] = ("default",),
) -> JobRow:
    """Claim one actual due workflow row through the production dispatch SQL."""
    from taskq.backend._sweeps import sweep_scheduled_to_pending

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 15
    while loop.time() < deadline:
        async with pool.acquire() as conn:
            await sweep_scheduled_to_pending(conn, schema=schema)
        rows = await _try_claim_one(pool, schema, worker, queues=queues)
        if rows:
            return rows[0]
        await asyncio.sleep(0.01)
    raise AssertionError(f"worker {worker} found no due workflow row on {list(queues)!r}")


async def _execute_due(
    pool: asyncpg.Pool,
    schema: str,
    worker: JobId,
    *,
    queues: Sequence[str] = ("default",),
) -> tuple[JobRow, str]:
    """Execute one claimed row through the real worker execution door."""
    from taskq.workflows._worker_execution import execute_flow_job

    job = await _claim_due(pool, schema, worker, queues=queues)
    execution = await execute_flow_job(
        pool=pool,
        schema=schema,
        worker_id=worker,
        job=job,
    )
    return job, execution.outcome


async def _maintain_root(runner: FlowRunner, flow_id: JobId) -> None:
    """Run the production root-maintenance arm without executing a body."""
    from taskq.workflows._sweep import sweep_join_rederive

    await sweep_join_rederive(runner.pool, runner.wsql)
    _ = flow_id


async def _wait_terminal_root(
    runner: FlowRunner, conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> str:
    async def terminal() -> bool:
        await _maintain_root(runner, flow_id)
        return await _root_status(conn, schema, flow_id) in _TERMINAL

    await _wait_until(f"terminal root {flow_id}", terminal)
    return await _root_status(conn, schema, flow_id)


async def _job_step(conn: asyncpg.Connection, schema: str, job_id: JobId) -> str:
    step = await conn.fetchval(
        f'SELECT step_key FROM "{schema}".jobs WHERE id=$1',
        job_id,
    )
    assert step is not None
    return str(step)


# ── current-behavior controls (genuine no-exclusion / no-quiescence) ─────


@pytest.mark.integration
async def test_control_per_node_declarations_do_not_serialize_two_runs_stages(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The base defect: run A's second stage can execute after run B's stage.

    No run-scoped declaration exists, so ticking A's first node to success and
    then letting B run shows the issue's A.first -> B.first -> A.second shape.
    This control stays green after #722 adds run reservations; it is the
    non-vacuous counterexample the new contract is measured against.
    """
    name = f"control_interleave_{new_uuid().hex[:8]}"
    guard = ConcurrencyReservation(f"control-guard-{name}", slots=1, lease=timedelta(seconds=30))
    events: list[tuple[str, str]] = []

    async def first(ctx: StepContext) -> dict[str, int]:
        events.append((str(ctx.flow_id), "first"))
        return {"n": 1}

    async def second(ctx: StepContext, previous: dict[str, int]) -> dict[str, int]:
        events.append((str(ctx.flow_id), "second"))
        return {"n": previous["n"] + 1}

    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(
            step(
                second,
                step(first, key="first", reservations=[guard]),
                key="second",
                reservations=[guard],
            )
        )

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    claim_a = await runner.create_flow(run_key="a")
    claim_b = await runner.create_flow(run_key="b")

    assert await runner.tick(claim_a.flow_id) is True
    assert events == [(str(claim_a.flow_id), "first")]

    assert await runner.drive(claim_b.flow_id) == "terminal"
    assert await runner.drive(claim_a.flow_id) == "terminal"

    labels = [(flow_id, stage) for flow_id, stage in events]
    assert (str(claim_a.flow_id), "first") in labels
    assert (str(claim_b.flow_id), "first") in labels
    assert labels.index((str(claim_b.flow_id), "first")) < labels.index(
        (str(claim_a.flow_id), "second")
    ), f"the control must demonstrate cross-run stage interleaving, got {labels!r}"


@pytest.mark.integration
async def test_control_terminal_root_is_not_body_quiescence(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The base defect: cancelling flips the root terminal while the body lives.

    The node stays running and its body is still gated, so root status alone
    cannot be the release/readiness fact.  The accepted lifecycle keeps this
    root/body distinction and releases only after the body-owning tail ends.
    """
    name = f"control_terminal_body_{new_uuid().hex[:8]}"
    entered = asyncio.Event()
    release = asyncio.Event()

    async def gated(ctx: StepContext) -> None:
        entered.set()
        await release.wait()

    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(step(gated, key="work"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    async with _GatedTask(asyncio.create_task(runner.drive(flow_id)), release) as drive:
        await asyncio.wait_for(entered.wait(), timeout=10)

        await runner.cancel_workflow(flow_id, reason="control: body still active")
        assert await _root_status(wf_conn, wf_schema, flow_id) == "cancelled"
        assert (await _node(wf_conn, wf_schema, flow_id, "work"))["status"] == "running"
        assert not drive.done(), "the body gate is still held; the driving task cannot be done"

        release.set()
        assert await asyncio.wait_for(drive, timeout=10) == "terminal"
        # A cooperative running body may retain its own succeeded result while the
        # root is cancelled (the #720 carrier's accepted shape); what matters here
        # is that the body frame is over, not which terminal status it wrote.
        assert (await _node(wf_conn, wf_schema, flow_id, "work"))["status"] in _TERMINAL


# ── #722: one run incarnation owns exclusion across its whole graph ──────


@pytest.mark.integration
async def test_direct_guarded_refusal_precedes_body_and_unguarded_run_works(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The landed direct-door law: guarded refusal before any body, ordinary still runs.

    This is the maintained control for the explicit direct/fleet split.  A
    guarded create uses the real P0 capability column; the direct driver must
    refuse with the typed actionable error before claim/body.  The same app's
    ordinary unguarded run proves the negative control is a working run, not a
    second refusal.
    """
    suffix = new_uuid().hex[:8]
    name = f"direct_refusal_{suffix}"
    calls: list[int] = []

    async def body(ctx: StepContext) -> None:
        calls.append(1)

    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(step(body, key="only"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    guarded = await create_guarded_flow(
        runner, required_worker_schema=1, run_key=f"guarded:{suffix}"
    )
    assert guarded.required_worker_schema == 1
    with pytest.raises(WorkflowWorkerCapabilityError) as caught:
        await runner.drive(guarded.flow_id)
    assert caught.value.required_worker_schema == 1
    assert caught.value.flow_id == guarded.flow_id
    assert "fleet" in str(caught.value)
    assert "No workflow body was started" in str(caught.value)
    assert calls == []
    rows = await wf_conn.fetch(
        f"SELECT step_key,status::text AS status,attempt,locked_by_worker,required_worker_schema "
        f"FROM \"{wf_schema}\".jobs WHERE id=$1 OR (metadata->>'flow_id')::uuid=$1 ORDER BY step_key",
        guarded.flow_id,
    )
    assert [(row["step_key"], row["status"], row["attempt"]) for row in rows] == [
        ("__flow__", "running", 0),
        ("only", "pending", 0),
    ]
    assert [row["required_worker_schema"] for row in rows] == [1, 1]
    assert all(row["locked_by_worker"] is None for row in rows)
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1',
            guarded.flow_id,
        )
        == 0
    )

    ordinary = await runner.create_flow(run_key=f"ordinary:{suffix}")
    assert ordinary.required_worker_schema is None
    assert await runner.drive(ordinary.flow_id) == "terminal"
    assert calls == [1]
    assert await _root_status(wf_conn, wf_schema, ordinary.flow_id) == "succeeded"


@pytest.mark.integration
async def test_run_reservations_serialize_across_nodes_and_worker_replacement(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A run holds its declared bucket from first body through terminal.

    Both runs are guarded declarations and execute only through the real
    registered-worker door.  A's first node completes; the short declaration
    lease ages while A remains live, and a second worker cannot claim B.  A
    replacement worker attaches A's same incarnation/epoch for its second node.
    Each node has one physical attempt.  Only after A's root is maintained
    terminal (and exclusion released) may B enter.
    """
    suffix = new_uuid().hex[:8]
    name_a = f"run_exclusion_a_{suffix}"
    name_b = f"run_exclusion_b_{suffix}"
    bucket = f"run-window-{suffix}"
    queue_a = f"run-a-{suffix}"
    queue_b = f"run-b-{suffix}"
    actor_a = f"actor-a-{suffix}"
    actor_b = f"actor-b-{suffix}"
    events: list[tuple[str, str]] = []

    async def first(ctx: StepContext) -> dict[str, int]:
        events.append((str(ctx.flow_id), "first"))
        return {"n": 1}

    async def second(ctx: StepContext, previous: dict[str, int]) -> dict[str, int]:
        events.append((str(ctx.flow_id), "second"))
        return {"n": previous["n"] + 1}

    app = WorkflowApp()

    @app.workflow(name_a)
    def flow_a() -> Promise[object]:
        return build(
            step(
                second,
                step(first, key="first", actor=actor_a, queue=queue_a),
                key="second",
                actor=actor_a,
                queue=queue_a,
            )
        )

    @app.workflow(name_b)
    def flow_b() -> Promise[object]:
        return build(
            step(
                second,
                step(first, key="first", actor=actor_b, queue=queue_b),
                key="second",
                actor=actor_b,
                queue=queue_b,
            )
        )

    compiled_a = app.get(name_a)
    compiled_b = app.get(name_b)
    runner_a = FlowRunner(compiled_a, wf_pool, wf_schema)
    runner_b = FlowRunner(compiled_b, wf_pool, wf_schema)
    reservation = [ConcurrencyReservation(bucket, slots=1, lease=timedelta(milliseconds=100))]
    a = (
        await runner_a.create_flow(
            run_key="a",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]  # Why: the accepted run-scoped declaration.
        )
    ).flow_id
    b = (
        await runner_b.create_flow(
            run_key="b",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    root_before = await _root(wf_conn, wf_schema, a)
    assert root_before["status"] == "running"
    assert root_before["run_epoch"] == 1

    # The declaration is guarded: the direct driver must refuse before A's body.
    with pytest.raises(WorkflowWorkerCapabilityError):
        await runner_a.tick(a)
    assert events == []
    first_node = await _node(wf_conn, wf_schema, a, "first")
    assert (first_node["status"], int(first_node["attempt"])) == ("pending", 0)

    await _configure_actor(wf_conn, wf_schema, actor_a, queue_a)
    await _configure_actor(wf_conn, wf_schema, actor_b, queue_b)
    worker_one = await _worker(wf_pool, wf_schema, module_pg_schema.pg_dsn)
    worker_two = await _worker(wf_pool, wf_schema, module_pg_schema.pg_dsn)
    first_job, first_outcome = await _execute_due(wf_pool, wf_schema, worker_one, queues=(queue_a,))
    assert first_job.locked_by_worker == worker_one
    assert first_job.attempt == 1
    assert first_outcome == "succeeded"
    assert [stage for flow_id, stage in events if flow_id == str(a)] == ["first"]

    # The hint lease is deliberately past its duration.  A live guarded run
    # must not become acquirable because a clock moved (expiry is not ownership).
    await asyncio.sleep(0.15)
    assert await _try_claim_one(wf_pool, wf_schema, worker_two, queues=(queue_b,)) == []
    assert not [stage for flow_id, stage in events if flow_id == str(b)]

    second_job, second_outcome = await _execute_due(
        wf_pool, wf_schema, worker_two, queues=(queue_a,)
    )
    assert second_job.locked_by_worker == worker_two, (
        "the replacement worker must attach the same run incarnation"
    )
    assert second_job.attempt == 1
    assert second_outcome == "succeeded"
    root_after_second = await _root(wf_conn, wf_schema, a)
    assert root_after_second["run_epoch"] == 1, (
        "the replacement worker must attach the same epoch, not reopen it"
    )
    await _wait_terminal_root(runner_a, wf_conn, wf_schema, a)
    root_after_a = await _root(wf_conn, wf_schema, a)
    assert root_after_a["status"] == "succeeded"
    assert root_after_a["run_epoch"] == 1
    assert not [stage for flow_id, stage in events if flow_id == str(b)]

    b_first_job, b_first_outcome = await _execute_due(
        wf_pool, wf_schema, worker_one, queues=(queue_b,)
    )
    assert b_first_job.locked_by_worker == worker_one
    assert b_first_job.attempt == 1
    assert b_first_outcome == "succeeded"
    b_second_job, b_second_outcome = await _execute_due(
        wf_pool, wf_schema, worker_two, queues=(queue_b,)
    )
    assert b_second_job.locked_by_worker == worker_two
    assert b_second_job.attempt == 1
    assert b_second_outcome == "succeeded"
    await _wait_terminal_root(runner_b, wf_conn, wf_schema, b)

    labels = [(flow_id, stage) for flow_id, stage in events]
    assert labels == [
        (str(a), "first"),
        (str(a), "second"),
        (str(b), "first"),
        (str(b), "second"),
    ], f"the successor overlapped the first run's window: {labels!r}"
    assert await _root_status(wf_conn, wf_schema, b) == "succeeded"
    node_states = []
    for flow_id in (a, b):
        for step_key in ("first", "second"):
            node = await _node(wf_conn, wf_schema, flow_id, step_key)
            node_states.append((node["status"], int(node["attempt"])))
    assert node_states == [("succeeded", 1)] * 4, (
        f"each run must have exactly one physical attempt per node: {node_states!r}"
    )
    running = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = ANY($1::uuid[]) "
        "AND status='running'",
        [a, b],
    )
    assert int(running) == 0


@pytest.mark.integration
async def test_cancelled_root_retains_exclusion_and_event_readiness_until_body_unwinds(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A terminal root is not a release point when its real body is active.

    A is guarded and executed by a registered worker.  It is cancelled while
    its body is gated.  A single explicit maintenance/drain seat observes no
    terminal-event job, and one bounded claim attempt for B observes no claimed
    row and no body entry.  After the body-owning tail returns, one seat
    materializes the one cancelled event and B enters.
    """
    suffix = new_uuid().hex[:8]
    name = f"cancel_quiescence_{suffix}"
    name_b = f"{name}_b"
    bucket = f"cancel-window-{suffix}"
    queue_a = f"cancel-a-{suffix}"
    queue_b = f"cancel-b-{suffix}"
    actor_a = f"cancel-actor-a-{suffix}"
    actor_b = f"cancel-actor-b-{suffix}"
    actor_name = f"settle-cancel-{suffix}"
    entered = asyncio.Event()
    release = asyncio.Event()
    b_entered = asyncio.Event()

    async def gated(ctx: StepContext) -> None:
        entered.set()
        await release.wait()

    async def b_body(ctx: StepContext) -> None:
        b_entered.set()

    app = WorkflowApp()
    settlement = _settlement_actor(actor_name, "settle", _noop_settlement)

    @app.workflow(name, on_terminal=settlement)  # pyright: ignore[reportCallIssue]  # Why: the accepted terminal-event declaration.
    def flow() -> Promise[object]:
        return build(step(gated, key="work", actor=actor_a, queue=queue_a))

    @app.workflow(name_b)
    def flow_b() -> Promise[object]:
        return build(step(b_body, key="work", actor=actor_b, queue=queue_b))

    compiled = app.get(name)
    compiled_b = app.get(name_b)
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    runner_b = FlowRunner(compiled_b, wf_pool, wf_schema)
    reservation = [ConcurrencyReservation(bucket, slots=1, lease=timedelta(seconds=30))]
    a = (
        await runner.create_flow(
            run_key="a",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    b = (
        await runner_b.create_flow(
            run_key="b",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    await _configure_actor(wf_conn, wf_schema, actor_a, queue_a)
    await _configure_actor(wf_conn, wf_schema, actor_b, queue_b)
    worker = await _worker(wf_pool, wf_schema, module_pg_schema.pg_dsn)

    async with _GatedTask(
        asyncio.create_task(_execute_due(wf_pool, wf_schema, worker, queues=(queue_a,))),
        release,
    ) as drive:
        await asyncio.wait_for(entered.wait(), timeout=15)
        await runner.cancel_workflow(a, reason="cancel while body active")
        assert await _root_status(wf_conn, wf_schema, a) == "cancelled"

        await _terminal_materialization_seat(runner, a)
        assert await _event_job_for_flow(wf_conn, wf_schema, a, actor_name, name) is None, (
            "the cancelled event materialized while the body-owning tail was still active"
        )
        assert await _try_claim_one(wf_pool, wf_schema, worker, queues=(queue_b,)) == []
        assert not b_entered.is_set()

        release.set()
        _body_job, body_outcome = await asyncio.wait_for(drive, timeout=15)
        assert body_outcome in {"succeeded", "cancelled"}
    await _terminal_materialization_seat(runner, a)
    event_job = await _require_event_job(wf_conn, wf_schema, a, actor_name, name)
    event = _event(event_job)
    assert str(event.status) == "cancelled"
    assert int(event.run_epoch) == 1

    b_job, b_outcome = await _execute_due(wf_pool, wf_schema, worker, queues=(queue_b,))
    assert b_job.locked_by_worker == worker
    assert b_outcome == "succeeded"
    assert b_entered.is_set()
    await _wait_terminal_root(runner_b, wf_conn, wf_schema, b)
    assert await _root_status(wf_conn, wf_schema, b) == "succeeded"


@pytest.mark.integration
async def test_final_graph_settlement_is_in_exclusion_event_delivery_is_outside(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The #721/#722/#723 boundary in one worker-driven timeline.

    A's final in-graph settlement is gated and executes through a registered
    worker: B cannot claim/enter while it is active.  After the final node and
    the maintained root are quiescent, one explicit materialization seat yields
    one event job.  The post-run settlement handler is then gated: B must run
    while the handler is still active (delivery is outside run exclusion).
    """
    suffix = new_uuid().hex[:8]
    name = f"boundary_{suffix}"
    name_b = f"{name}_b"
    bucket = f"boundary-window-{suffix}"
    queue_a = f"boundary-a-{suffix}"
    queue_b = f"boundary-b-{suffix}"
    actor_a = f"boundary-actor-a-{suffix}"
    actor_b = f"boundary-actor-b-{suffix}"
    actor_name = f"settle-boundary-{suffix}"
    graph_entered = asyncio.Event()
    graph_release = asyncio.Event()
    settlement_entered = asyncio.Event()
    settlement_release = asyncio.Event()
    b_entered = asyncio.Event()

    async def final_settlement(ctx: StepContext) -> None:
        graph_entered.set()
        await graph_release.wait()

    async def b_body(ctx: StepContext) -> None:
        b_entered.set()

    async def settle(event: WorkflowTerminalEvent) -> None:  # pyright: ignore[reportUnknownParameterType]  # Why: same future export.
        settlement_entered.set()
        await settlement_release.wait()

    app = WorkflowApp()
    settlement = _settlement_actor(actor_name, "settle", settle)

    @app.workflow(name, on_terminal=settlement)  # pyright: ignore[reportCallIssue]
    def flow() -> Promise[object]:
        return build(step(final_settlement, key="final", actor=actor_a, queue=queue_a))

    @app.workflow(name_b)
    def flow_b() -> Promise[object]:
        return build(step(b_body, key="work", actor=actor_b, queue=queue_b))

    compiled = app.get(name)
    compiled_b = app.get(name_b)
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    runner_b = FlowRunner(compiled_b, wf_pool, wf_schema)
    reservation = [ConcurrencyReservation(bucket, slots=1, lease=timedelta(seconds=30))]
    a = (
        await runner.create_flow(
            run_key="a",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    b = (
        await runner_b.create_flow(
            run_key="b",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    await _configure_actor(wf_conn, wf_schema, actor_a, queue_a)
    await _configure_actor(wf_conn, wf_schema, actor_b, queue_b)
    worker = await _worker(wf_pool, wf_schema, module_pg_schema.pg_dsn)

    async with _GatedTask(
        asyncio.create_task(_execute_due(wf_pool, wf_schema, worker, queues=(queue_a,))),
        graph_release,
    ) as drive:
        await asyncio.wait_for(graph_entered.wait(), timeout=15)
        assert await _try_claim_one(wf_pool, wf_schema, worker, queues=(queue_b,)) == []
        assert not b_entered.is_set()
        await _terminal_materialization_seat(runner, a)
        assert await _event_job_for_flow(wf_conn, wf_schema, a, actor_name, name) is None

        graph_release.set()
        _final_job, final_outcome = await asyncio.wait_for(drive, timeout=15)
        assert final_outcome == "succeeded"
    await _wait_terminal_root(runner, wf_conn, wf_schema, a)
    await _terminal_materialization_seat(runner, a)
    event_job = await _require_event_job(wf_conn, wf_schema, a, actor_name, name)
    event = _event(event_job)
    assert event.flow_id == a
    assert str(event.status) == "succeeded"

    async with _GatedTask(asyncio.create_task(settlement(event)), settlement_release) as delivery:
        await asyncio.wait_for(settlement_entered.wait(), timeout=15)
        b_job, b_outcome = await _execute_due(wf_pool, wf_schema, worker, queues=(queue_b,))
        assert b_job.locked_by_worker == worker
        assert b_outcome == "succeeded"
        assert b_entered.is_set(), "post-run event delivery must not hold the run window"
        await _wait_terminal_root(runner_b, wf_conn, wf_schema, b)
        assert await _root_status(wf_conn, wf_schema, b) == "succeeded"
        settlement_release.set()
        await asyncio.wait_for(delivery, timeout=15)


async def _noop_settlement(event: WorkflowTerminalEvent) -> None:  # pyright: ignore[reportUnknownParameterType]  # Why: same future export.
    return None


# ── #723: producer admission, window identity, event episodes ────────────


@pytest.mark.integration
async def test_max_pending_is_exact_and_atomic_under_concurrent_creates(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The cap is a create-side live-root cap, exact under real contention.

    Thirty-two distinct run keys race a cap of four: exactly four roots and
    four work nodes exist, every refusal is typed, and no refused key leaves a
    partial root/node.  One terminal run releases a slot for the next key.
    """
    suffix = new_uuid().hex[:8]
    name = f"cap_{suffix}"
    cap = 4

    async def work(ctx: StepContext) -> None:
        return None

    app = WorkflowApp()

    @app.workflow(name, max_pending=cap)  # pyright: ignore[reportCallIssue]
    def flow() -> Promise[object]:
        return build(step(work, key="work"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    results = await asyncio.gather(
        *(runner.create_flow(run_key=f"cap:{index}") for index in range(32)),
        return_exceptions=True,
    )
    claims = [result for result in results if isinstance(result, RunClaim)]
    refusals = [result for result in results if isinstance(result, MaxPendingExceededError)]
    other = [
        result for result in results if not isinstance(result, (RunClaim, MaxPendingExceededError))
    ]
    assert not other, f"unexpected create outcomes: {other!r}"
    assert len(claims) == cap, f"the cap admitted {len(claims)} roots, not {cap}: {claims!r}"
    assert len(refusals) == 32 - cap
    assert all(claim.created for claim in claims)
    assert {refusal.max_pending for refusal in refusals} == {cap}
    assert {refusal.current_count for refusal in refusals} == {cap}

    roots = await wf_conn.fetch(
        f'SELECT id,idempotency_key FROM "{wf_schema}".jobs '
        "WHERE step_key='__flow__' AND idempotency_scope=$1",
        f"{_RUN_SCOPE}{name}",
    )
    assert len(roots) == cap, f"refused creates leaked roots: {roots!r}"
    admitted_keys = {str(root["idempotency_key"]) for root in roots}
    refused_keys = {f"cap:{index}" for index in range(32)} - admitted_keys
    assert len(refused_keys) == 32 - cap
    assert not (refused_keys & admitted_keys)

    nodes = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = ANY($1::uuid[]) "
        "AND step_key='work'",
        [root["id"] for root in roots],
    )
    assert int(nodes) == cap, f"refused creates leaked work rows: {nodes}"
    for key in refused_keys:
        leaked = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs WHERE idempotency_key=$1',
            key,
        )
        assert int(leaked) == 0, f"refused create {key!r} left a row"

    first = JobId(roots[0]["id"])
    assert await runner.drive(first) == "terminal"
    assert await _root_status(wf_conn, wf_schema, first) == "succeeded"
    released = await runner.create_flow(run_key="cap:released")
    assert released.created
    final_roots = await wf_conn.fetch(
        f'SELECT status::text AS status FROM "{wf_schema}".jobs '
        "WHERE step_key='__flow__' AND idempotency_scope=$1",
        f"{_RUN_SCOPE}{name}",
    )
    assert len(final_roots) == cap + 1
    assert sum(1 for row in final_roots if row["status"] in _TERMINAL) == 1


@pytest.mark.integration
async def test_window_dedupe_is_exact_precedes_cap_and_expires(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """One logical identity commits once; the window is distinct from the key.

    Concurrent distinct run keys sharing an identity yield one root and typed
    ``dedupe='window'`` claims.  A window hit under a full cap must return the
    dedupe verdict, not a cap refusal (window lock before cap lock).  Backdating
    the window's ``created_at`` admits the next distinct run once capacity is
    released.
    """
    suffix = new_uuid().hex[:8]
    name = f"window_{suffix}"
    identity = f"source:{suffix}"

    async def work(ctx: StepContext) -> None:
        return None

    app = WorkflowApp()

    @app.workflow(  # pyright: ignore[reportCallIssue]  # Why: accepted producer/window declaration.
        name,
        max_pending=3,  # pyright: ignore[reportCallIssue]
        unique_for=timedelta(minutes=10),  # pyright: ignore[reportCallIssue]
    )
    def flow() -> Promise[object]:
        return build(step(work, key="work"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    claims = await asyncio.gather(
        *(
            runner.create_flow(
                run_key=f"cron:{suffix}:{index}",
                identity_key=identity,  # pyright: ignore[reportCallIssue]
            )
            for index in range(16)
        )
    )
    assert len(claims) == 16, f"all same-identity racers must return RunClaim: {claims!r}"
    created_claims = [claim for claim in claims if claim.created]
    assert len(created_claims) == 1, f"one identity minted {len(created_claims)} roots: {claims!r}"
    winner_claim = created_claims[0]
    assert winner_claim.dedupe == "created"  # pyright: ignore[reportAttributeAccessIssue]
    winner = winner_claim.flow_id
    window_claims = [claim for claim in claims if not claim.created]
    assert len(window_claims) == 15, f"expected fifteen window hits: {window_claims!r}"
    for claim in window_claims:
        assert claim.dedupe == "window"  # pyright: ignore[reportAttributeAccessIssue]
        assert claim.flow_id == winner
    other = await runner.create_flow(
        run_key=f"manual:{suffix}",
        identity_key=f"source:other-{suffix}",  # pyright: ignore[reportCallIssue]
    )
    assert other.created
    third = await runner.create_flow(
        run_key=f"manual:{suffix}:3",
        identity_key=f"source:third-{suffix}",  # pyright: ignore[reportCallIssue]
    )
    assert third.created

    duplicate = await runner.create_flow(
        run_key=f"manual:{suffix}:duplicate",
        identity_key=identity,  # pyright: ignore[reportCallIssue]
    )
    assert not duplicate.created
    assert duplicate.dedupe == "window"  # pyright: ignore[reportAttributeAccessIssue]

    with pytest.raises(MaxPendingExceededError):
        await runner.create_flow(
            run_key=f"manual:{suffix}:4",
            identity_key=f"source:fourth-{suffix}",  # pyright: ignore[reportCallIssue]
        )

    roots = await wf_conn.fetch(
        f'SELECT id,idempotency_key,identity_key FROM "{wf_schema}".jobs '
        "WHERE step_key='__flow__' AND idempotency_scope=$1",
        f"{_RUN_SCOPE}{name}",
    )
    assert len(roots) == 3
    assert len([root for root in roots if root["identity_key"] == identity]) == 1

    assert await runner.drive(winner) == "terminal"
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET created_at=created_at - interval '11 minutes' "
        "WHERE id=$1 AND step_key='__flow__'",
        winner,
    )
    reopened_window = await runner.create_flow(
        run_key=f"cron:{suffix}:after-expiry",
        identity_key=identity,  # pyright: ignore[reportCallIssue]
    )
    assert reopened_window.created, (
        "the window must measure created_at age; an expired prior run cannot suppress "
        f"the next distinct run: {reopened_window!r}"
    )


@pytest.mark.integration
async def test_node_materialization_is_not_producer_admission(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """One admitted run may materialize more node rows than the root cap.

    A map source creating four children under ``max_pending=1`` proves the
    producer count is roots, not rows.  A row-counting cap would refuse the
    create or stall the fork.
    """
    suffix = new_uuid().hex[:8]
    name = f"materialization_{suffix}"

    async def source(ctx: StepContext) -> list[dict[str, int]]:
        return [{"n": index} for index in range(4)]

    async def per_item(ctx: StepContext, item: dict[str, int]) -> dict[str, int]:
        return {"n": item["n"] * 2}

    app = WorkflowApp()

    @app.workflow(name, max_pending=1)  # pyright: ignore[reportCallIssue]
    def flow() -> Promise[object]:
        return build(map_source(step(source, key="source"), per_item))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow(run_key="materialized")).flow_id
    assert await runner.drive(flow_id) == "terminal"
    roots = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key='__flow__' AND idempotency_scope=$1",
        f"{_RUN_SCOPE}{name}",
    )
    assert int(roots) == 1, f"one admitted producer run became {roots} roots"
    children = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid=$1 "
        "AND step_key='source.item'",
        flow_id,
    )
    assert int(children) == 4, f"the map materialization did not create four children: {children}"


@pytest.mark.integration
async def test_private_child_spawn_is_counted_but_not_refused(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A child is a real counted root, but the private spawn seam is not gated.

    The child declares ``max_pending=0``: a public child create refuses in the
    child's own workflow scope, while the admitted parent's private spawn must
    succeed.  The parent's public create refuses in the parent's separate
    workflow scope.  Both real root rows remain and both eventually terminalize.
    """
    suffix = new_uuid().hex[:8]
    parent_name = f"child_parent_{suffix}"
    child_name = f"child_{suffix}"
    entered = asyncio.Event()
    release = asyncio.Event()

    async def child_body(ctx: StepContext) -> _ChildOut:
        entered.set()
        await release.wait()
        return _ChildOut()

    async def parent_body(ctx: StepContext) -> _ChildOut:
        handle = await ctx.spawn_flow(child_name, result=_ChildOut, key="child")
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=result):
                return result
            case _:
                return _ChildOut(ok=False)

    app = WorkflowApp()

    @app.workflow(child_name, max_pending=0)  # pyright: ignore[reportCallIssue]
    def child() -> Promise[object]:
        return build(step(child_body, key="work"))

    @app.workflow(parent_name, max_pending=1)  # pyright: ignore[reportCallIssue]
    def parent() -> Promise[object]:
        return build(step(parent_body, key="spawn"))

    compiled_parent = app.get(parent_name)
    compiled_child = app.get(child_name)  # registered for ctx.spawn_flow's definition lookup
    runner = FlowRunner(compiled_parent, wf_pool, wf_schema)
    child_runner = FlowRunner(compiled_child, wf_pool, wf_schema)
    parent_id = (await runner.create_flow(run_key="parent")).flow_id
    async with _GatedTask(asyncio.create_task(runner.drive(parent_id)), release) as drive:
        await asyncio.wait_for(entered.wait(), timeout=15)

        child_roots = await wf_conn.fetch(
            f'SELECT id,status::text AS status FROM "{wf_schema}".jobs '
            "WHERE step_key='__flow__' AND idempotency_scope=$1",
            f"{_RUN_SCOPE}{child_name}",
        )
        assert len(child_roots) == 1, (
            f"the private spawn did not create one child root: {child_roots!r}"
        )
        child_id = JobId(child_roots[0]["id"])
        assert await _root_status(wf_conn, wf_schema, child_id) not in _TERMINAL

        with pytest.raises(MaxPendingExceededError) as child_refusal:
            await child_runner.create_flow(run_key="child-public")
        assert child_refusal.value.current_count == 1
        assert child_refusal.value.max_pending == 0
        with pytest.raises(MaxPendingExceededError) as caught:
            await runner.create_flow(run_key="parent-public")
        assert caught.value.current_count == 1, (
            "the parent workflow scope has one live parent root; the child belongs "
            f"to its own workflow scope: {caught.value.current_count!r}"
        )
        assert caught.value.max_pending == 1

        release.set()
        assert await asyncio.wait_for(drive, timeout=30) == "terminal"
        assert await _root_status(wf_conn, wf_schema, parent_id) == "succeeded"
        assert await _root_status(wf_conn, wf_schema, child_id) == "succeeded"


@pytest.mark.integration
async def test_run_epoch_is_durable_and_reopen_is_a_new_episode(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The root's durable run incarnation starts at one and reopens at two.

    No Python counter supplies the episode identity: the row carries it across
    the terminal/reopen transition.  The terminal-effect/event controls below
    use this value as their episode fence.
    """
    name = f"epoch_{new_uuid().hex[:8]}"

    async def fail(ctx: StepContext) -> None:
        raise RuntimeError("synthetic terminal failure")

    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(step(fail, key="work", max_attempts=1))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow(run_key="epoch")).flow_id
    root = await _root(wf_conn, wf_schema, flow_id)
    assert root["run_epoch"] == 1
    assert root["status"] == "running"

    assert await runner.drive(flow_id) == "terminal"
    failed = await _root(wf_conn, wf_schema, flow_id)
    assert failed["status"] == "failed"
    assert failed["run_epoch"] == 1

    reopened = await retry_workflow_node(
        wf_pool, schema=wf_schema, flow_id=flow_id, node_key="work", reason="control reopen"
    )
    assert reopened >= 1
    root_after = await _root(wf_conn, wf_schema, flow_id)
    assert root_after["status"] == "running"
    assert root_after["run_epoch"] == 2


@pytest.mark.integration
async def test_late_episode_event_after_reopen_is_episode_scoped(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A late episode-one delivery cannot overwrite the reopened episode.

    Episode one fails through the registered-worker door and its event job is
    deliberately held undelivered while the failed root reopens.  Episode two
    then fails too.  Episode one's job primary key, idempotency key, and public
    dedupe key survive rematerialization; episode two has distinct job and
    episode identity.  Invoking the late first event after episode two exists
    writes only the epoch-one settlement row.  Duplicate invocation is
    at-least-once execution with one same-DB row.
    """
    suffix = new_uuid().hex[:8]
    name = f"late_event_{suffix}"
    actor_name = f"settle-late-{suffix}"
    table = f"late_settlement_{suffix}"
    queue = f"late-{suffix}"
    late_actor = f"late-actor-{suffix}"
    invocations: list[tuple[str, int]] = []
    await wf_conn.execute(
        f'CREATE TABLE "{wf_schema}"."{table}" '
        "(flow_id uuid NOT NULL, run_epoch bigint NOT NULL, closes int NOT NULL, "
        "PRIMARY KEY(flow_id,run_epoch))"
    )

    async def settle(event: WorkflowTerminalEvent) -> None:  # pyright: ignore[reportUnknownParameterType]  # Why: same future export.
        flow_value = str(event.flow_id)
        epoch = int(event.run_epoch)
        invocations.append((flow_value, epoch))
        await wf_pool.execute(
            f'INSERT INTO "{wf_schema}"."{table}" (flow_id,run_epoch,closes) '
            "VALUES ($1,$2,1) ON CONFLICT (flow_id,run_epoch) DO NOTHING",
            flow_value,
            epoch,
        )

    async def fail(ctx: StepContext) -> None:
        raise RuntimeError("synthetic terminal failure")

    app = WorkflowApp()
    settlement = _settlement_actor(actor_name, "settle", settle)

    @app.workflow(name, on_terminal=settlement)  # pyright: ignore[reportCallIssue]
    def flow() -> Promise[object]:
        return build(step(fail, key="work", max_attempts=1, actor=late_actor, queue=queue))

    compiled = app.get(name)
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    await _configure_actor(wf_conn, wf_schema, late_actor, queue)
    worker = await _worker(wf_pool, wf_schema, module_pg_schema.pg_dsn)
    flow_id = (await runner.create_flow(run_key="late")).flow_id

    _job, first_outcome = await _execute_due(wf_pool, wf_schema, worker, queues=(queue,))
    assert first_outcome in {"laddered", "failed"}
    await _wait_terminal_root(runner, wf_conn, wf_schema, flow_id)
    await _terminal_materialization_seat(runner, flow_id)
    first_job = await _require_event_job(wf_conn, wf_schema, flow_id, actor_name, name)
    first_event = _event(first_job)
    assert first_event.flow_id == flow_id
    assert int(first_event.run_epoch) == 1
    assert str(first_event.status) == "failed"
    first_job_id = str(first_job["id"])
    first_idempotency_key = str(first_job["idempotency_key"])
    first_dedupe_key = str(first_event.dedupe_key)
    assert first_idempotency_key
    assert first_dedupe_key

    assert (
        await retry_workflow_node(
            wf_pool, schema=wf_schema, flow_id=flow_id, node_key="work", reason="late episode"
        )
        >= 1
    )
    assert len(await _event_jobs(wf_conn, wf_schema, actor_name, name)) == 1

    _job2, second_outcome = await _execute_due(wf_pool, wf_schema, worker, queues=(queue,))
    assert second_outcome in {"laddered", "failed"}
    await _wait_terminal_root(runner, wf_conn, wf_schema, flow_id)
    await _terminal_materialization_seat(runner, flow_id)
    jobs = await _event_jobs(wf_conn, wf_schema, actor_name, name)
    events = sorted((_event(job) for job in jobs), key=lambda event: int(event.run_epoch))
    assert [int(event.run_epoch) for event in events] == [1, 2]
    assert len({str(event.flow_id) for event in events}) == 1
    assert [str(event.status) for event in events] == ["failed", "failed"]
    by_epoch = {}
    for job in jobs:
        event = _event(job)
        by_epoch[int(event.run_epoch)] = (job, event)
    first_job_again, first_event_again = by_epoch[1]
    second_job, second_event = by_epoch[2]
    assert str(first_job_again["id"]) == first_job_id
    assert str(first_job_again["idempotency_key"]) == first_idempotency_key
    assert str(first_event_again.dedupe_key) == first_dedupe_key
    assert str(second_job["id"]) != first_job_id
    second_idempotency_key = str(second_job["idempotency_key"])
    second_dedupe_key = str(second_event.dedupe_key)
    assert second_idempotency_key != first_idempotency_key
    assert second_dedupe_key != first_dedupe_key

    # A repeated maintenance/drain seat must not replace either episode's job.
    await _terminal_materialization_seat(runner, flow_id)
    jobs_again = await _event_jobs(wf_conn, wf_schema, actor_name, name)
    again_by_epoch = {}
    for job in jobs_again:
        event_again = _event(job)
        again_by_epoch[int(event_again.run_epoch)] = (job, event_again)
    assert set(again_by_epoch) == {1, 2}
    assert str(again_by_epoch[1][0]["id"]) == first_job_id
    assert str(again_by_epoch[1][0]["idempotency_key"]) == first_idempotency_key
    assert str(again_by_epoch[1][1].dedupe_key) == first_dedupe_key
    assert str(again_by_epoch[2][0]["id"]) == str(second_job["id"])
    assert str(again_by_epoch[2][0]["idempotency_key"]) == second_idempotency_key
    assert str(again_by_epoch[2][1].dedupe_key) == second_dedupe_key

    # Episode one delivers late, after episode two has already terminalized.
    await settlement(events[0])
    await settlement(events[1])
    await settlement(events[0])  # duplicate delivery: at-least-once execution
    assert invocations == [
        (str(flow_id), 1),
        (str(flow_id), 2),
        (str(flow_id), 1),
    ]
    rows = await wf_conn.fetch(
        f'SELECT flow_id,run_epoch,closes FROM "{wf_schema}"."{table}" ORDER BY run_epoch'
    )
    assert [(str(row["flow_id"]), int(row["run_epoch"]), int(row["closes"])) for row in rows] == [
        (str(flow_id), 1, 1),
        (str(flow_id), 2, 1),
    ]


@pytest.mark.integration
async def test_terminal_event_projection_uses_the_five_status_vocabulary(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The root-status trigger projects every engine terminal status once.

    This is a trigger-level projection control: it drives the real root update
    and one explicit maintenance/drain seat but not each engine terminal writer
    (the writer inventory is a separate executable implementation obligation).
    A nodeless terminal root has no unresolved graph row, so readiness is true.
    """
    suffix = new_uuid().hex[:8]
    for status in sorted(_TERMINAL):
        name = f"projection_{suffix}_{status}"
        actor_name = f"settle-{status}-{suffix}"
        app = WorkflowApp()
        settlement = _settlement_actor(actor_name, "settle", _noop_settlement)

        @app.workflow(name, on_terminal=settlement)  # pyright: ignore[reportCallIssue]
        def flow() -> Promise[object]:
            return build(step(_noop_body, key="work"))

        runner = FlowRunner(app.get(name), wf_pool, wf_schema)
        flow_id = (await runner.create_flow(run_key=f"status:{status}")).flow_id
        await wf_conn.execute(
            f"DELETE FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid=$1 "
            "AND step_key <> '__flow__'",
            flow_id,
        )
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET status=$2, finished_at=clock_timestamp() '
            "WHERE id=$1 AND step_key='__flow__'",
            flow_id,
            status,
        )
        await _terminal_materialization_seat(runner, flow_id)
        job = await _require_event_job(wf_conn, wf_schema, flow_id, actor_name, name)
        event = _event(job)
        assert str(event.status) == status
        assert int(event.run_epoch) == 1
        other = await _event_jobs(wf_conn, wf_schema, actor_name, name)
        assert len(other) == 1
        await _terminal_materialization_seat(runner, flow_id)
        assert len(await _event_jobs(wf_conn, wf_schema, actor_name, name)) == 1


async def _noop_body(ctx: StepContext) -> None:
    return None


@pytest.mark.integration
async def test_active_body_marker_is_declaration_gated_and_cleared(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Only declared runs carry the body-active marker; it clears at the tail.

    The declared guarded run executes its body through the registered-worker
    door: its claimed node exposes ``wf_body_active`` while the body is gated,
    and the marker is gone after the body-owning execution tail returns.  The
    ordinary unguarded run stays on the direct door and never inherits the
    marker, so it pays no new heartbeat state.  Missing/stale marker recovery
    remains an implementation proof obligation.
    """
    suffix = new_uuid().hex[:8]
    actor_name = f"settle-marker-{suffix}"
    queue = f"marker-{suffix}"
    declared_actor = f"marker-actor-{suffix}"
    entered = asyncio.Event()
    release = asyncio.Event()

    async def gated(ctx: StepContext) -> None:
        entered.set()
        await release.wait()

    app = WorkflowApp()
    settlement = _settlement_actor(actor_name, "settle", _noop_settlement)
    reservation = [
        ConcurrencyReservation(f"marker-window-{suffix}", slots=1, lease=timedelta(seconds=30))
    ]

    @app.workflow(f"declared_{suffix}", on_terminal=settlement)  # pyright: ignore[reportCallIssue]
    def declared() -> Promise[object]:
        return build(step(gated, key="work", actor=declared_actor, queue=queue))

    @app.workflow(f"ordinary_{suffix}")
    def ordinary() -> Promise[object]:
        return build(step(gated, key="work"))

    declared_runner = FlowRunner(app.get(f"declared_{suffix}"), wf_pool, wf_schema)
    declared_id = (
        await declared_runner.create_flow(
            run_key="declared",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    await _configure_actor(wf_conn, wf_schema, declared_actor, queue)
    worker = await _worker(wf_pool, wf_schema, module_pg_schema.pg_dsn)
    async with _GatedTask(
        asyncio.create_task(_execute_due(wf_pool, wf_schema, worker, queues=(queue,))),
        release,
    ) as declared_drive:
        await asyncio.wait_for(entered.wait(), timeout=15)
        declared_node = await _node(wf_conn, wf_schema, declared_id, "work")
        assert _meta(declared_node["metadata"]).get("wf_body_active"), (
            "a declaration-born run must mark its claimed body active while it executes"
        )
        release.set()
        _declared_job, declared_outcome = await asyncio.wait_for(declared_drive, timeout=15)
        assert declared_outcome == "succeeded"
        cleared = await _node(wf_conn, wf_schema, declared_id, "work")
        assert not _meta(cleared["metadata"]).get("wf_body_active")
        await _wait_terminal_root(declared_runner, wf_conn, wf_schema, declared_id)

    entered.clear()
    release.clear()
    ordinary_runner = FlowRunner(app.get(f"ordinary_{suffix}"), wf_pool, wf_schema)
    ordinary_id = (await ordinary_runner.create_flow(run_key="ordinary")).flow_id
    async with _GatedTask(
        asyncio.create_task(ordinary_runner.drive(ordinary_id)), release
    ) as ordinary_drive:
        await asyncio.wait_for(entered.wait(), timeout=15)
        ordinary_node = await _node(wf_conn, wf_schema, ordinary_id, "work")
        assert not _meta(ordinary_node["metadata"]).get("wf_body_active"), (
            "an undeclared workflow must not inherit the active-body heartbeat state"
        )
        release.set()
        assert await asyncio.wait_for(ordinary_drive, timeout=15) == "terminal"


@pytest.mark.integration
async def test_terminal_effect_remains_inside_run_exclusion_until_it_finishes(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The ordinary terminal effect is run work, not a post-run delivery.

    A and B have dedicated source queues; the effect has an independent queue.
    While A's effect body is gated, exactly one bounded non-raising claim
    attempt on B's source queue returns zero and B's source body has not run.
    After A's effect completes and releases exclusion, B's source is claimable
    and its body event appears.  No elapsed-time denial is used.
    """
    suffix = new_uuid().hex[:8]
    name_a = f"effect_exclusion_a_{suffix}"
    name_b = f"effect_exclusion_b_{suffix}"
    bucket = f"effect-window-{suffix}"
    queue_a = f"effect-source-a-{suffix}"
    queue_b = f"effect-source-b-{suffix}"
    work_queue = f"effect-work-{suffix}"
    effect_queue = f"effect-run-{suffix}"
    actor_a = f"effect-actor-a-{suffix}"
    actor_b = f"effect-actor-b-{suffix}"
    work_actor = f"effect-work-actor-{suffix}"
    effect_actor = f"effect-{suffix}"
    effect_entered = asyncio.Event()
    effect_release = asyncio.Event()
    source_events: list[str] = []

    async def source_body(ctx: StepContext) -> None:
        source_events.append(str(ctx.flow_id))
        await ctx.emit_batch(
            [chain_start(chain, {"record": 7}, map_index=7, trace_id="record-7")],
            cursor={"page": 0},
        )

    async def work_body(ctx: StepContext, item: object) -> _EffectOutcome:
        raise RuntimeError("synthetic effect-source failure")

    async def effect_body(ctx: StepContext, event: ExhaustionEvent) -> None:
        effect_entered.set()
        await effect_release.wait()

    chain = Chain(
        name=f"effect-chain-{suffix}",
        start="work",
        steps={
            "work": Step(
                body=work_body,
                outcomes=_EffectOutcome,
                policy=ExecutionPolicy(
                    actor=work_actor,
                    queue=work_queue,
                    retry=RetrySpec(
                        policy=RetryPolicy(max_attempts=1, jitter=0),
                        on_exhaustion=TerminalEffect(
                            body=effect_body,
                            policy=ExecutionPolicy(actor=effect_actor, queue=effect_queue),
                        ),
                    ),
                ),
            )
        },
    )

    app = WorkflowApp()

    @app.workflow(name_a)
    def flow_a() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", actor=actor_a, queue=queue_a))

    @app.workflow(name_b)
    def flow_b() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", actor=actor_b, queue=queue_b))

    compiled_a = app.get(name_a)
    compiled_b = app.get(name_b)
    runner_a = FlowRunner(compiled_a, wf_pool, wf_schema)
    runner_b = FlowRunner(compiled_b, wf_pool, wf_schema)
    reservation = [ConcurrencyReservation(bucket, slots=1, lease=timedelta(seconds=30))]
    a = (
        await runner_a.create_flow(
            run_key="a",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    b = (
        await runner_b.create_flow(
            run_key="b",
            run_reservations=reservation,  # pyright: ignore[reportCallIssue]
        )
    ).flow_id
    await _configure_actor(wf_conn, wf_schema, actor_a, queue_a)
    await _configure_actor(wf_conn, wf_schema, actor_b, queue_b)
    await _configure_actor(wf_conn, wf_schema, work_actor, work_queue)
    await _configure_actor(wf_conn, wf_schema, effect_actor, effect_queue)
    worker = await _worker(wf_pool, wf_schema, module_pg_schema.pg_dsn)

    source_job, source_outcome = await _execute_due(wf_pool, wf_schema, worker, queues=(queue_a,))
    assert await _job_step(wf_conn, wf_schema, source_job.id) == "source"
    assert source_outcome == "succeeded"
    work_job, work_outcome = await _execute_due(wf_pool, wf_schema, worker, queues=(work_queue,))
    assert await _job_step(wf_conn, wf_schema, work_job.id) == "work"
    assert work_outcome in {"laddered", "failed"}
    await _wait_terminal_root(runner_a, wf_conn, wf_schema, a)
    assert await _root_status(wf_conn, wf_schema, a) == "failed"

    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows._sweep import drain_outbox

    assert await drain_outbox(wf_pool, WorkflowSql.build(wf_schema)) == 1
    effect_jobs = await wf_conn.fetch(
        'SELECT id,step_key,status::text AS status FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='work.__on_exhaustion'",
        a,
    )
    assert len(effect_jobs) == 1, f"expected one terminal effect job: {effect_jobs!r}"

    async with _GatedTask(
        asyncio.create_task(_execute_due(wf_pool, wf_schema, worker, queues=(effect_queue,))),
        effect_release,
    ) as effect_task:
        await asyncio.wait_for(effect_entered.wait(), timeout=15)
        assert await _root_status(wf_conn, wf_schema, a) == "failed"

        assert await _try_claim_one(wf_pool, wf_schema, worker, queues=(queue_b,)) == []
        b_source = await _node(wf_conn, wf_schema, b, "source")
        assert (b_source["status"], int(b_source["attempt"])) == ("pending", 0)
        assert str(b) not in source_events, (
            "the successor entered while the failed run's terminal effect was still active"
        )

        effect_release.set()
        _effect_job, effect_outcome = await asyncio.wait_for(effect_task, timeout=15)
        assert effect_outcome == "succeeded"
        assert (
            await wf_conn.fetchval(
                f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE id=$1 AND status='succeeded'",
                effect_jobs[0]["id"],
            )
            == 1
        )

    b_job, b_outcome = await _execute_due(wf_pool, wf_schema, worker, queues=(queue_b,))
    assert await _job_step(wf_conn, wf_schema, b_job.id) == "source"
    assert b_outcome == "succeeded"
    assert str(b) in source_events, (
        "the successor did not enter after the terminal effect completed and released exclusion"
    )
    await runner_b.cancel_workflow(b, reason="control cleanup")
