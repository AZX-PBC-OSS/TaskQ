"""The assembled fleet-level proof of worker-scoped workflow deps.

Every capability of the deps seam has a slice pin where it lives. This file
assembles them into the shapes an operator actually runs, against a real
Postgres schema:

* a real worker composition (``_bootstrap._main``) executing two workflow
  families concurrently under ``max_concurrency > 1`` — one lazy instance,
  bound to that worker's loop, released exactly once before the role pools
  close;
* the same worker identity entered from a second event loop with its own pool —
  two instances, never shared;
* two workgroup children spawned by the supervisor's own ``_spawn_child`` — each
  resolves a real ``asyncpg`` pool on its own loop, the later child never sees
  the earlier child's instance, and each releases its own on shutdown;
* a SIGHUP crossing the supervisor boundary into a child's reload coordinator —
  the next body receives the new generation's instance while the superseded one
  is released exactly once.

Observations come only from the real surfaces: the bodies' own loop identity, the
factories' invocation ledger, the bounded-close tap on the worker's role pools,
and the event ledger each workgroup child writes for itself. No SQL is faked and
no stdlib module is monkeypatched.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import importlib
import json
import os
import signal
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest
import structlog.testing
from pydantic import BaseModel

import taskq.worker._bootstrap as bootstrap_mod
import taskq.worker.deps as worker_deps_mod
from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId, JobRow
from taskq.backend._sweeps import sweep_scheduled_to_pending
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.health import unique_health_sock_path
from taskq.worker.run import register_worker
from taskq.worker.workgroup import WorkerSpec, _ChildState, _spawn_child
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
    build,
    chain_source,
    chain_start,
    step,
)
from taskq.workflows import _worker_deps as scope
from taskq.workflows import _worker_execution as seam
from taskq.workflows._sweep import drain_outbox
from taskq.workflows.api._validate import WorkflowValidationError

pytestmark = pytest.mark.integration

# ── the in-process probe (the worker composition's own ledger) ────────────

_PRODUCED: list[int] = [0]
"""Factory invocation count — the one-instance witness."""

_INSTANCES: list[object] = []
"""The deps instance each body observed, in execution order."""

_EXECUTED: list[str] = []
"""One entry per body that ran: that body's doc id and the flow it ran for."""

_ORDER: list[str] = []
"""Lifecycle markers in the order they happened: body boundaries, the deps
release, each role pool's bounded close."""

_BARRIER: list[asyncio.Event] = []
"""The concurrency gate: the first body parks until the second arrives, so a
serial dispatch fails the test instead of passing it silently."""

_EFFECT_EVENTS: list[ExhaustionEvent] = []
"""The ExhaustionEvent each terminal-effect handler ran with."""


def _reset() -> None:
    _PRODUCED[0] = 0
    _INSTANCES.clear()
    _EXECUTED.clear()
    _ORDER.clear()
    _BARRIER.clear()
    _EFFECT_EVENTS.clear()


@pytest.fixture(autouse=True)
def _reset_probe_state() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner.
    _reset()
    yield


class _Probe:
    """A loop-bound deps resource released by ``aclose()``."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    async def aclose(self) -> None:
        _ORDER.append("deps-teardown")


async def _make_probe() -> _Probe:
    _PRODUCED[0] += 1
    # Widen the first-claim window: a concurrent second claim really enters the
    # resolution while this factory is still running.
    for _ in range(5):
        await asyncio.sleep(0)
    return _Probe(asyncio.get_running_loop())


class _Doc(BaseModel):
    doc_id: str


async def _deps_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> None:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EXECUTED.append(f"{doc.doc_id}:{ctx.flow_id}")
    _INSTANCES.append(deps)
    _ORDER.append(f"body:{doc.doc_id}")
    if _BARRIER and len(_EXECUTED) < 2:
        await asyncio.wait_for(_BARRIER[0].wait(), timeout=20.0)
    elif _BARRIER:
        _BARRIER[0].set()


async def _effect_body(ctx: StepContext, event: ExhaustionEvent, deps: _Probe) -> None:
    assert deps.loop is asyncio.get_running_loop(), (
        "the terminal-effect handler's deps left the claim's loop"
    )
    _EFFECT_EVENTS.append(event)
    _INSTANCES.append(deps)


_FAMILIES = ("wf-fleet-alpha", "wf-fleet-beta")


def _fleet_app(names: Sequence[str]) -> WorkflowApp:
    app = WorkflowApp(deps_factory=_make_probe)
    for name in names:

        @app.workflow(name)
        def _build(name: str = name) -> Promise[object]:
            return build(step(_deps_body, _Doc(doc_id=name), key="solo", queue="default"))

    return app


def _claimed_job(job_id: JobId, *, metadata: dict[str, object], payload: object = None) -> JobRow:
    return JobRow(
        id=job_id,
        actor="wf-fleet",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload=payload if payload is not None else {},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        metadata=metadata,
        claim_epoch=1,
    )


async def _create_run(pool: asyncpg.Pool, schema: str, compiled: Any, doc_id: str) -> JobId:
    """The run's rows through the runner's own ``create_flow`` — the production
    producer the fleet's dispatch claims from."""
    async with FlowRunner(compiled, pool, schema) as runner:
        return (await runner.create_flow(input={"doc_id": doc_id})).flow_id


async def _claim_and_execute(
    pool: asyncpg.Pool, schema: str, flow_id: JobId, worker_id: UUID
) -> seam.FlowExecution:
    """One node row through the real execution door, running under
    *worker_id* (the dispatch claim's own shape)."""
    row = await pool.fetchrow(
        f'SELECT id, payload FROM "{schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own; asyncpg cannot bind identifiers.
        "WHERE step_key = 'solo' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert row is not None, f"no node row for flow {flow_id}"
    await pool.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'running', attempt = 1, claim_epoch = 1, "  # noqa: S608  # Why: same fixture-owned schema identifier.
        "locked_by_worker = $2, lock_expires_at = now() + interval '60 seconds' WHERE id = $1",
        row["id"],
        worker_id,
    )
    payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    return await seam.execute_flow_job(
        pool=pool,
        schema=schema,
        worker_id=JobId(worker_id),
        job=_claimed_job(JobId(row["id"]), metadata={"flow_id": str(flow_id)}, payload=payload),
    )


def _worker_settings(
    pg_dsn: str, schema: str, *, tag: str, max_concurrency: int = 1
) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
            "TASKQ_HEALTH_ENABLED": "false",
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path(f"fleet_{tag}"),
            "TASKQ_LOOP_LAG_WARN_BUDGET": "9999",
            "TASKQ_LOOP_LAG_BUDGET": "9999",
        }
    )


def _tap_pool_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record each role pool's bounded close beside the deps release markers —
    the before-pool-close proof."""
    real_close = worker_deps_mod.close_pool_bounded  # pyright: ignore[reportPrivateImportUsage]  # Why: the call site reads this module global at call time; capturing it is how the tap keeps calling the real bounded close.

    async def _tapped_close(
        pool: asyncpg.Pool, label: str, close_timeout: float, *, family: Any = "teardown"
    ) -> None:
        _ORDER.append(f"pool:{label}")
        await real_close(pool, label, close_timeout, family=family)

    monkeypatch.setattr(worker_deps_mod, "close_pool_bounded", _tapped_close)  # pyright: ignore[reportPrivateImportUsage]  # Why: the call site reads this module global at close time, so replacing it here is the only way to observe the pool-close order.
    monkeypatch.setattr(bootstrap_mod, "close_pool_bounded", _tapped_close)


def _assert_released_before_the_pools() -> None:
    assert _ORDER.count("deps-teardown") == 1, f"exactly one release; got {_ORDER}"
    release_at = _ORDER.index("deps-teardown")
    for label in ("dispatcher", "heartbeat", "worker"):
        marker = f"pool:{label}"
        assert marker in _ORDER, f"the {label} pool never closed; order={_ORDER}"
        assert release_at < _ORDER.index(marker), (
            f"the deps release must complete before the {label} pool closes; order={_ORDER}"
        )


def _assert_no_loop_or_pool_errors(logs: list[dict[str, Any]]) -> None:
    forbidden = ("Event loop is closed", "attached to a different loop", "pool is closed")
    offending = [entry for entry in logs if any(text in repr(entry) for text in forbidden)]
    assert offending == [], offending


# ── a real worker composition under concurrent body families ──────────────


async def test_a_real_worker_mints_one_instance_for_concurrent_body_families(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``max_concurrency > 1`` across two workflow families of one app, two
    runs each: the first bodies of both families are in flight at the same
    instant, the factory runs exactly once, every body of every run receives
    that instance on the worker's loop, and the worker's shutdown releases it
    exactly once before any role pool closes."""
    schema = module_pg_schema.schema_name
    app = _fleet_app(_FAMILIES)
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    _BARRIER.append(asyncio.Event())
    worker: asyncio.Task[int] | None = None
    try:
        flows = {
            name: [
                await _create_run(pool, schema, app.get(name), f"{name}-{run}") for run in (1, 2)
            ]
            for name in _FAMILIES
        }
        _tap_pool_closes(monkeypatch)
        settings = _worker_settings(
            module_pg_schema.pg_dsn, schema, tag="concurrency", max_concurrency=2
        )
        with structlog.testing.capture_logs() as logs:
            worker = asyncio.create_task(
                bootstrap_mod._main(  # pyright: ignore[reportPrivateUsage]  # Why: the real worker entry point is the composition under test.
                    settings,
                    actor_registry={},
                    until_idle=True,
                    idle_settle_window=0.2,
                    idle_poll_interval=0.1,
                    idle_max_runtime=120.0,
                )
            )
            await wait_for_condition(
                lambda: len(_EXECUTED) == 4,
                description="every run of both workflow families executed (the bodies' barrier proves concurrency)",
                timeout=60.0,
            )
            # Stamp each run terminal before draining: the run's own root is
            # finalized by the leader's outbox sweep on its own cadence, and
            # making the drained worker's shutdown wait out that cadence would
            # add a sweep-interval sleep to a test about the deps lifecycle.
            # The node bodies already succeeded for real; this only marks the
            # run they completed, exactly as the sweep would.
            for flow_ids in flows.values():
                for flow_id in flow_ids:
                    await pool.execute(
                        f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "  # noqa: S608  # Why: the schema identifier is the fixture's own.
                        "finished_at = clock_timestamp() WHERE id = $1 AND step_key = '__flow__'",
                        flow_id,
                    )
            exit_code = await asyncio.wait_for(worker, timeout=120.0)

        assert exit_code == 0, exit_code
        assert _PRODUCED[0] == 1, f"the factory ran {_PRODUCED[0]}x under concurrency"
        assert set(_EXECUTED) == {
            f"{name}:{flow_id}" for name, flow_ids in flows.items() for flow_id in flow_ids
        }, f"every run of both families executed exactly one body; got {_EXECUTED}"
        assert len(_INSTANCES) == 4
        assert all(instance is _INSTANCES[0] for instance in _INSTANCES), (
            f"every body of every run shares the worker's one instance; got {_INSTANCES}"
        )
        _assert_released_before_the_pools()
        _assert_no_loop_or_pool_errors(logs)

        resolved = [e for e in logs if e["event"] == "workflow-deps-resolved"]
        assert len(resolved) == 1, resolved
        assert resolved[0]["workflow"] in _FAMILIES
        assert resolved[0]["generation"] == scope.INITIAL_DEPS_GENERATION
        assert len([e for e in logs if e["event"] == "workflow-deps-torn-down"]) == 1
    finally:
        _BARRIER[0].set()
        if worker is not None:
            if not worker.done():
                worker.cancel()
            # Retrieve a failure as well as cancelling a live worker: this
            # module's tests share one loop, so neither may leak into the next.
            await asyncio.gather(worker, return_exceptions=True)
        await pool.close()


def test_a_second_event_loop_never_shares_the_workers_instance(
    module_pg_schema: ModulePgSchema,
) -> None:
    """The same worker identity and binding entered from two event loops (each
    with its own pool) resolves one instance per loop: a loop-bound resource is
    never handed across loops, and each loop's shutdown releases its own."""
    app = _fleet_app(["wf-fleet-loops"])
    compiled = app.get("wf-fleet-loops")
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()

    def _one_loop(doc_id: str) -> object:
        async def _scenario() -> object:
            pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
            try:
                flow_id = await _create_run(pool, schema, compiled, doc_id)
                execution = await _claim_and_execute(pool, schema, flow_id, worker_id)
                assert execution.outcome == "succeeded", execution
                await seam.teardown_worker_workflow_state(worker_id)
                return _INSTANCES[-1]
            finally:
                await pool.close()

        return asyncio.run(_scenario())

    first = _one_loop("first")
    second = _one_loop("second")

    assert _PRODUCED[0] == 2, f"one instance per event loop; got {_PRODUCED[0]}"
    assert first is not second, "a second event loop reused the first loop's instance"
    assert _ORDER.count("deps-teardown") == 2, f"each loop released its own; got {_ORDER}"


async def test_the_direct_door_binds_the_factory_to_the_running_loop(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The direct-door arm of the same criterion: a long-lived
    ``FlowRunner`` over real PG resolves the factory on the caller's running
    loop, hands that instance to the body, and releases it once on exit."""
    app = _fleet_app(["wf-fleet-direct"])
    runner = FlowRunner(app.get("wf-fleet-direct"), wf_pool, wf_schema)
    async with runner:
        flow_id = (await runner.create_flow(input={"doc_id": "direct"})).flow_id
        assert await runner.drive(flow_id) == "terminal"

    assert _PRODUCED[0] == 1, f"the factory ran {_PRODUCED[0]}x"
    assert len(_INSTANCES) == 1
    assert cast("_Probe", _INSTANCES[0]).loop is asyncio.get_running_loop()
    assert _ORDER.count("deps-teardown") == 1, f"one release on exit; got {_ORDER}"


# ── the terminal-effect row: the ExhaustionEvent arm's deps tail ──────


async def _dispatch_claim(
    pool: asyncpg.Pool, schema: str, worker_id: JobId, *, timeout_s: float = 10.0
) -> dict[str, Any]:
    """One workflow row through the production dispatch SQL — the claim the
    fleet's queue-routed dispatch performs, not a hand-flipped row."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        async with pool.acquire() as conn:
            await sweep_scheduled_to_pending(conn, schema=schema)
        rows = await dispatch_batch(
            pool,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
            queues=["default"],
            limit_n=1,
            worker_id=JobId(worker_id),
            lock_lease=timedelta(seconds=30),
        )
        if rows:
            return dict(rows[0])
        await asyncio.sleep(0.005)
    raise TimeoutError(f"worker {worker_id} found no due workflow row within {timeout_s}s")


async def test_a_terminal_effect_handler_receives_the_worker_resolved_deps(
    wf_schema: str, wf_pool: asyncpg.Pool, pg_dsn: str
) -> None:
    """The effect row reaches its body through ``run_fleet_claimed_step`` and
    the ``ExhaustionEvent`` arm of ``_resolve_args`` — exactly one wired
    source the deps tail must ride behind. A step that exhausts under a
    ``deps_factory`` app enqueues its terminal effect; the handler the fleet
    claim runs declares ``(ctx, ExhaustionEvent, deps)`` and receives the
    worker-resolved instance on the claim's loop, then the runner's exit
    releases it exactly once."""
    suffix = new_uuid().hex

    class _Outcome(enum.Enum):
        DONE = "done"

    async def _work(ctx: StepContext, item: object) -> _Outcome:
        raise RuntimeError("synthetic permanent record failure")

    effect = TerminalEffect(
        body=_effect_body,
        policy=ExecutionPolicy(
            actor=f"effect-{suffix}",
            queue="default",
            start_to_close=timedelta(seconds=5),
            retry=RetrySpec(policy=RetryPolicy(max_attempts=1, jitter=0)),
        ),
    )
    chain = Chain(
        name=f"effect-deps-chain-{suffix}",
        start="work",
        steps={
            "work": Step(
                body=_work,
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
    app = WorkflowApp(deps_factory=_make_probe)
    name = f"fleet_effect_deps_{suffix}"

    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(chain, {"record": 17}, map_index=17, trace_id="r17")],
            cursor={"page": 0},
        )

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source, key="source"))

    compiled = app.get(name)
    worker_id = JobId(
        await register_worker(
            wf_pool, _worker_settings(pg_dsn, wf_schema, tag="effect"), workflow_execution=True
        )
    )
    runner = FlowRunner(compiled, wf_pool, wf_schema, worker_id=worker_id)
    try:
        async with FlowRunner(compiled, wf_pool, wf_schema) as creator:
            flow_id = (await creator.create_flow()).flow_id
            wsql = creator.wsql
        for actor_name in ("wf", f"effect-{suffix}"):
            await wf_pool.execute(
                f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1,$2) '  # noqa: S608  # Why: the schema identifier is the fixture's own; asyncpg cannot bind identifiers.
                "ON CONFLICT (actor) DO NOTHING",
                actor_name,
                "default",
            )

        async def _execute(expected_step: str) -> str:
            claimed = await _dispatch_claim(wf_pool, wf_schema, worker_id)
            assert claimed["step_key"] == expected_step, (
                f"expected {expected_step!r}, claimed {claimed['step_key']!r}"
            )
            return await runner.run_fleet_claimed_step(
                flow_id,
                claimed,
                attempt=int(claimed["attempt"]),
                claim_epoch=int(claimed["claim_epoch"]),
            )

        assert await _execute("source") == "succeeded"
        assert await _execute("work") == "laddered"
        assert await drain_outbox(wf_pool, wsql) == 1
        assert await _execute("work.__on_exhaustion") == "succeeded"
    finally:
        await runner.aclose()

    assert _PRODUCED[0] == 1, f"the factory resolved once for the handler; got {_PRODUCED[0]}"
    assert len(_INSTANCES) == 1, f"the handler got exactly one instance; got {_INSTANCES}"
    assert _ORDER.count("deps-teardown") == 1, f"one release on the runner's exit; got {_ORDER}"
    assert len(_EFFECT_EVENTS) == 1, _EFFECT_EVENTS
    event = _EFFECT_EVENTS[0]
    assert event.reason == "retry_exhausted"
    assert event.error.error_class == "RuntimeError"


# ── the workgroup children (real processes) ───────────────────────────────

_PROBE_MODULE = '''"""The fleet probe app: a real loop-bound dependency per worker process."""

from __future__ import annotations

import asyncio
import itertools
import json
import os
from pathlib import Path

import asyncpg
from pydantic import BaseModel

from taskq.actor import actor
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step


_SERIAL = itertools.count(1)


def _note(event: str, **fields: object) -> None:
    record = {"event": event, "pid": os.getpid(), **fields}
    with Path(os.environ["TASKQ_FLEET_EVENTS"]).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\\n")


def _generation() -> int:
    version = int(Path(os.environ["TASKQ_FLEET_GENERATION"]).read_text(encoding="utf-8").strip())
    _note("generation-read", generation=version)
    return version


class _Probe:
    """The factory-created dependency: a real asyncpg pool bound to the running
    loop, stamped with the generation that minted it."""

    def __init__(self, pool: asyncpg.Pool, generation: int, serial: int) -> None:
        self.pool = pool
        self.loop = asyncio.get_running_loop()
        self.generation = generation
        # A serial remains unique after the superseded object is freed; id()
        # does not, because a replacement may reuse its address.
        self.instance = f"probe-{serial}"
        self.pool_id = f"pool-{serial}"

    async def aclose(self) -> None:
        loop_open = not self.loop.is_closed()
        await self.pool.close()
        _note(
            "released",
            instance=self.instance,
            generation=self.generation,
            loop_open=loop_open,
            pool_id=self.pool_id,
        )


async def _make_probe() -> _Probe:
    generation = _generation()
    pool = await asyncpg.create_pool(os.environ["TASKQ_PG_DSN"], min_size=1)
    assert pool is not None
    probe = _Probe(pool, generation, next(_SERIAL))
    _note(
        "resolved",
        instance=probe.instance,
        generation=probe.generation,
        loop_id=id(probe.loop),
        pool_id=probe.pool_id,
    )
    return probe


app = WorkflowApp(deps_factory=_make_probe, deps_generation=_generation)


class _Doc(BaseModel):
    doc_id: str


async def _body(ctx: StepContext, doc: _Doc, deps: _Probe) -> None:
    del ctx
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    # A real round trip through the factory-created pool: a pool attached to a
    # different loop would raise here, not silently serve.
    async with deps.pool.acquire() as conn:
        assert await conn.fetchval("SELECT 1") == 1
    _note(
        "body",
        instance=deps.instance,
        generation=deps.generation,
        loop_id=id(deps.loop),
        pool_id=deps.pool_id,
        doc_id=doc.doc_id,
    )


def _declare(name: str, queue: str) -> None:
    @app.workflow(name)
    def _build(name: str = name, queue: str = queue) -> Promise[object]:
        return build(
            step(_body, _Doc(doc_id=name), key="solo", actor="probe_" + name, queue=queue)
        )


for _name, _queue in __WORKFLOWS__:
    _declare(_name, _queue)


class _NoopPayload(BaseModel):
    value: int = 0


@actor(name="fleet_probe_noop", queue=__QUEUE__)
async def _noop(payload: _NoopPayload) -> None:
    del payload


ACTORS = {_noop.name: _noop}
'''


def _write_probe_module(
    tmp_path: Path, *, tag: str, workflows: Sequence[tuple[str, str]]
) -> tuple[ModuleType, str]:
    """Write the probe app for one scenario and import it into this process
    (the flow rows' producer). Returns the module and its ``module:attr``
    reference for the child's ``--actors``."""
    module_name = f"fleet_probe_{tag}"
    source = _PROBE_MODULE.replace("__WORKFLOWS__", repr(list(workflows))).replace(
        "__QUEUE__", repr(workflows[0][1])
    )
    (tmp_path / f"{module_name}.py").write_text(source, encoding="utf-8")
    sys.path.insert(0, os.fspath(tmp_path))
    try:
        module = importlib.import_module(module_name)
    finally:
        sys.path.remove(os.fspath(tmp_path))
    return module, f"{module_name}:ACTORS"


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        cast("dict[str, Any]", json.loads(line))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _events_of(
    events: list[dict[str, Any]], event: str, *, pid: int | None = None
) -> list[dict[str, Any]]:
    return [
        entry
        for entry in events
        if entry["event"] == event and (pid is None or entry["pid"] == pid)
    ]


def _probe_environment(
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    """The child's inherited environment plus the two ledgers it writes: the
    event file and the generation source a reload rotates."""
    events = tmp_path / "events.jsonl"
    generation = tmp_path / "generation.txt"
    generation.write_text("0", encoding="utf-8")
    monkeypatch.setenv("TASKQ_PG_DSN", module_pg_schema.pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)
    monkeypatch.setenv("TASKQ_FLEET_EVENTS", os.fspath(events))
    monkeypatch.setenv("TASKQ_FLEET_GENERATION", os.fspath(generation))
    monkeypatch.setenv("PYTHONPATH", os.fspath(tmp_path))
    monkeypatch.delenv("TASKQ_REDIS_URL", raising=False)
    monkeypatch.delenv("TASKQ_PG_CREDENTIAL_PROVIDER", raising=False)
    return events, generation


async def _spawn_probe_child(tag: str, queues: Sequence[str], actors: str) -> _ChildState:
    child = _ChildState(
        spec=WorkerSpec(name=tag, queues=list(queues), poll_interval=0.05, max_concurrency=1)
    )
    await _spawn_child(child, actors, new_uuid())
    assert child.process is not None, "the supervisor produced no process"
    return child


async def _stop_child(child: _ChildState) -> int:
    """The supervisor's own shutdown shape: SIGTERM, bounded wait, then a hard
    kill as the last resort; the stream pumps are reaped, never orphaned.
    Returns the child's exit code so a crash is never mistaken for a clean
    stop."""
    proc = child.process
    assert proc is not None
    if proc.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=30.0)
        except TimeoutError:  # a hung graceful shutdown is a real failure, not a skip
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
    stream_tasks = [
        task
        for task in (child.stdout_task, child.stderr_task)
        if task is not None and not task.done()
    ]
    if stream_tasks:
        try:
            # Process exit closes both pipes. Let their pumps drain to EOF so
            # the assertions below see the child's complete diagnostic output.
            await asyncio.wait_for(asyncio.gather(*stream_tasks), timeout=5.0)
        except TimeoutError:
            for task in stream_tasks:
                task.cancel()
            await asyncio.gather(*stream_tasks, return_exceptions=True)
    returncode = proc.returncode
    assert returncode is not None, "the child was reaped without an exit code"
    return returncode


def _child_output(logs: list[Mapping[str, Any]], worker: str) -> str:
    return "\n".join(
        str(entry["line"])
        for entry in logs
        if entry["event"] == "workgroup.child_output" and entry["worker"] == worker
    )


def _assert_child_stopped(returncode: int, logs: list[Mapping[str, Any]], worker: str) -> None:
    assert returncode == 0, (
        f"the child {worker} exited rc={returncode} instead of a clean shutdown:\n"
        f"{_child_output(logs, worker)}"
    )


async def _wait_for_events(
    path: Path,
    predicate: Callable[[list[dict[str, Any]]], bool],
    *,
    description: str,
    child: _ChildState | None = None,
    logs: list[Mapping[str, Any]] | None = None,
    bound_seconds: float = 90.0,
) -> list[dict[str, Any]]:
    """Wait for the child's own event ledger to satisfy *predicate*, failing at
    once if the child dies first — a dead child never gets to look like a slow
    one, and its output rides the failure."""

    def _ready() -> bool:
        if child is not None:
            proc = child.process
            if proc is not None and proc.returncode is not None:
                output = (
                    _child_output(logs, child.spec.name) if logs is not None else "<not captured>"
                )
                raise AssertionError(
                    f"the child {child.spec.name} exited rc={proc.returncode} "
                    f"before {description}:\n{output}"
                )
        return predicate(_read_events(path))

    await wait_for_condition(
        _ready,
        description=description,
        timeout=bound_seconds,
        poll_interval=0.05,
    )
    return _read_events(path)


async def test_two_workgroup_children_resolve_their_own_loop_bound_instance(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Two real workgroup children, each consuming its own workflow's queue.

    The first child runs two runs of its workflow: the factory runs once, both
    bodies query through that one pool, and one release lands on shutdown. The
    second child starts AFTER the first has resolved its instance and mints its
    own pool on its own loop — it never inherits the earlier child's instance.
    """
    schema = module_pg_schema.schema_name
    workflows = [("wf-fleet-child-alpha", "fleet-alpha"), ("wf-fleet-child-beta", "fleet-beta")]
    module, actors = _write_probe_module(tmp_path, tag="children", workflows=workflows)
    app = cast("WorkflowApp", module.app)
    events, _ = _probe_environment(module_pg_schema, tmp_path, monkeypatch)

    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    first_child: _ChildState | None = None
    second_child: _ChildState | None = None
    try:
        await _create_run(pool, schema, app.get(workflows[0][0]), "alpha-1")
        await _create_run(pool, schema, app.get(workflows[0][0]), "alpha-2")
        with structlog.testing.capture_logs() as logs:
            first_child = await _spawn_probe_child("fleet-first", ["fleet-alpha"], actors)
            first_pid = first_child.process.pid  # pyright: ignore[reportOptionalMemberAccess]  # Why: _spawn_child asserts the process before returning.
            assert first_pid is not None
            after_first = await _wait_for_events(
                events,
                lambda entries: len(_events_of(entries, "body", pid=first_pid)) == 2,
                description="the first child executed both of its workflow's runs",
                child=first_child,
                logs=logs,
            )
            first_resolved = _events_of(after_first, "resolved", pid=first_pid)
            assert len(first_resolved) == 1, first_resolved

            # The later child starts only now, with the earlier child's
            # instance already live in its own process: a second resolution
            # under the second pid is the isolation the criterion names.
            await _create_run(pool, schema, app.get(workflows[1][0]), "beta-1")
            second_child = await _spawn_probe_child("fleet-second", ["fleet-beta"], actors)
            second_pid = second_child.process.pid  # pyright: ignore[reportOptionalMemberAccess]  # Why: _spawn_child asserts the process before returning.
            assert second_pid is not None
            after_second = await _wait_for_events(
                events,
                lambda entries: len(_events_of(entries, "body", pid=second_pid)) == 1,
                description="the second child executed its workflow's run",
                child=second_child,
                logs=logs,
            )

            first_returncode = await _stop_child(first_child)
            second_returncode = await _stop_child(second_child)
            shutdown_events = _read_events(events)

        _assert_child_stopped(first_returncode, logs, "fleet-first")
        _assert_child_stopped(second_returncode, logs, "fleet-second")
        assert first_pid != second_pid, "the two children must be distinct processes"

        # Loop addresses and probe serials are process-local: they are
        # meaningful only within one child's ledger, never across two address
        # spaces. Every cross-process claim here is carried by the pid, and the
        # identity claims are checked inside each child's own events.
        second_resolved = _events_of(after_second, "resolved", pid=second_pid)
        assert len(second_resolved) == 1, second_resolved
        assert second_resolved[0]["generation"] == 0
        resolves = _events_of(shutdown_events, "resolved")
        assert [entry["pid"] for entry in resolves] == [first_pid, second_pid], (
            f"the later child resolved its own instance after the earlier one: {resolves}"
        )

        for pid, expected_bodies in ((first_pid, 2), (second_pid, 1)):
            resolved = _events_of(shutdown_events, "resolved", pid=pid)
            released = _events_of(shutdown_events, "released", pid=pid)
            bodies = _events_of(shutdown_events, "body", pid=pid)
            assert len(resolved) == 1, resolved
            assert len(bodies) == expected_bodies, bodies
            assert [entry["instance"] for entry in released] == [
                entry["instance"] for entry in resolved
            ], f"one release per resolved instance for pid {pid}: {shutdown_events}"
            assert {entry["instance"] for entry in bodies} == {resolved[0]["instance"]}, (
                f"every body of pid {pid} used that child's one instance: {bodies}"
            )
            assert {entry["loop_id"] for entry in bodies} == {resolved[0]["loop_id"]}, (
                f"the bodies of pid {pid} ran on the loop the factory resolved on"
            )
            assert {entry["pool_id"] for entry in bodies} == {resolved[0]["pool_id"]}, (
                f"the bodies of pid {pid} queried through the factory's one pool"
            )
            assert released[0]["loop_open"] is True, (
                "the resource must close while its loop is alive"
            )

        for worker in ("fleet-first", "fleet-second"):
            output = _child_output(logs, worker)
            for forbidden in (
                "Event loop is closed",
                "attached to a different loop",
                "pool is closed",
            ):
                assert forbidden not in output, f"{worker}: {forbidden}\n{output}"
    finally:
        if first_child is not None:
            await _stop_child(first_child)
        if second_child is not None:
            await _stop_child(second_child)
        await pool.close()


async def test_a_sighup_reload_rotates_a_workgroup_childs_instance(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The rolling-reload path across the supervisor boundary: SIGHUP reaches
    the child's reload coordinator, the next body receives the new generation's
    pool, and the superseded instance is released exactly once — all before the
    child's own shutdown releases the live one."""
    schema = module_pg_schema.schema_name
    workflows = [("wf-fleet-reload", "fleet-reload")]
    module, actors = _write_probe_module(tmp_path, tag="reload", workflows=workflows)
    app = cast("WorkflowApp", module.app)
    events, generation_file = _probe_environment(module_pg_schema, tmp_path, monkeypatch)

    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    child: _ChildState | None = None
    try:
        await _create_run(pool, schema, app.get(workflows[0][0]), "gen-0")
        with structlog.testing.capture_logs() as logs:
            child = await _spawn_probe_child("fleet-reload", ["fleet-reload"], actors)
            pid = child.process.pid  # pyright: ignore[reportOptionalMemberAccess]  # Why: _spawn_child asserts the process before returning.
            assert pid is not None
            after_first = await _wait_for_events(
                events,
                lambda entries: len(_events_of(entries, "body", pid=pid)) == 1,
                description="the child executed its first body on generation 0",
                child=child,
                logs=logs,
            )
            assert _events_of(after_first, "body", pid=pid)[0]["generation"] == 0

            generation_file.write_text("1", encoding="utf-8")
            child.process.send_signal(signal.SIGHUP)  # pyright: ignore[reportOptionalMemberAccess]  # Why: the child process is asserted above.
            await _wait_for_events(
                events,
                lambda entries: any(
                    entry["generation"] == 1
                    for entry in _events_of(entries, "generation-read", pid=pid)
                ),
                description="the SIGHUP reached the child's deps rotation",
                child=child,
                logs=logs,
            )
            rotated = await _wait_for_events(
                events,
                lambda entries: len(_events_of(entries, "released", pid=pid)) == 1,
                description="the superseded generation was released once",
                child=child,
                logs=logs,
            )

            await _create_run(pool, schema, app.get(workflows[0][0]), "gen-1")
            after_reload = await _wait_for_events(
                events,
                lambda entries: len(_events_of(entries, "body", pid=pid)) == 2,
                description="the post-reload body received the new generation",
                child=child,
                logs=logs,
            )

            returncode = await _stop_child(child)
            shutdown_events = _read_events(events)

        _assert_child_stopped(returncode, logs, "fleet-reload")
        superseded = _events_of(rotated, "released", pid=pid)
        assert [entry["generation"] for entry in superseded] == [0], superseded
        bodies = _events_of(after_reload, "body", pid=pid)
        assert [entry["generation"] for entry in bodies] == [0, 1], bodies
        assert bodies[0]["instance"] != bodies[1]["instance"], "the reload minted a new instance"
        assert bodies[1]["pool_id"] != bodies[0]["pool_id"], "the reload minted a new pool"

        released = _events_of(shutdown_events, "released", pid=pid)
        assert [entry["generation"] for entry in released] == [0, 1], (
            f"one release per generation, exactly once; got {released}"
        )
        assert released[0]["loop_open"] is True and released[1]["loop_open"] is True

        output = _child_output(logs, "fleet-reload")
        assert "workflow-deps-rotated" in output, output
        for forbidden in ("Event loop is closed", "attached to a different loop", "pool is closed"):
            assert forbidden not in output, f"{forbidden}\n{output}"
    finally:
        if child is not None:
            await _stop_child(child)
        await pool.close()


# ── the declaration contract the assembling system inherits ───────────────


def test_the_fleet_declarations_keep_the_build_time_contract() -> None:
    """The assembled app's own declaration face: ``deps=`` and
    ``deps_factory=`` are mutually exclusive, a mismatched factory product is
    refused at build naming both types, and a true arity mismatch stays E10's."""
    with pytest.raises(ValueError, match="deps_factory="):
        WorkflowApp(deps=object(), deps_factory=_make_probe)

    class _Wrong:
        pass

    async def _wrong_product() -> _Wrong:
        return _Wrong()

    app = WorkflowApp(deps_factory=_wrong_product)

    @app.workflow("wf-fleet-mismatch")
    def _build() -> Promise[object]:
        return build(step(_deps_body, _Doc(doc_id="mismatch"), key="solo"))

    with pytest.raises(WorkflowValidationError) as mismatch:
        app.get("wf-fleet-mismatch")
    assert "_Probe" in str(mismatch.value) and "_Wrong" in str(mismatch.value)

    verdict_app = WorkflowApp(deps_factory=_make_probe)

    async def _two_extra(ctx: StepContext, doc: _Doc, deps: _Probe, other: object) -> None:
        del ctx, doc, deps, other

    @verdict_app.workflow("wf-fleet-arity")
    def _build_arity() -> Promise[object]:
        return build(step(_two_extra, _Doc(doc_id="arity"), key="solo"))

    with pytest.raises(WorkflowValidationError) as arity:
        verdict_app.get("wf-fleet-arity")
    assert "E10" in str(arity.value)
