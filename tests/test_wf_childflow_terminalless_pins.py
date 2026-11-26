"""Terminal-less child workflow pins for ``ctx.spawn_flow``.

``ctx.spawn_flow(name, ..., result=G)`` promises a :class:`ChildHandle` whose
``wait()`` decodes the child workflow's terminal node into ``G``. A compiled
child workflow with ``terminal is None`` (for example, one whose steps are
explicitly sunk via ``sink(...)`` and returns ``None``) can never produce a
terminal result, so ``spawn_flow`` refuses it before any child row or spawn
cursor is written.

Because no retry can give a terminal-less workflow a terminal node, this
refusal is a deterministic authoring failure: the parent step fails on its
first ladder attempt with exactly one ``failed`` ledger row, zero child flows
minted, and no retry backoff burned. ``jobs.attempt`` reads the claim ordinal,
so a node that held before it refused records 2 — the ladder itself never
burned.

The same invariant has four more faces pinned here:

* the discriminator split — the terminal-less subclass routes
  deterministically, the unregistered-name base it subclasses keeps its
  rolling-deploy retry tolerance;
* the packaged run's door — ``taskq.workflows.run`` on a terminal-less
  workflow reads ``("terminal", None)`` instead of raising;
* the compile registry's one-name-one-graph law — a second app cannot flip a
  recorded name's terminal wiring;
* the persisted message window — both target refusals keep their diagnosis
  and remedy inside ``MESSAGE_TERMINAL_MAX`` for any registry key, including
  names whose ``repr`` expands escaped code points;
* a declared ``on_exhaustion`` effect is suppressed for the deterministic
  refusal.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

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
from taskq.worker.run import register_worker
from taskq.workflows import (
    Chain,
    ChildWorkflowNoTerminalError,
    ChildWorkflowUnknownError,
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
    run,
    sink,
    step,
)
from taskq.workflows._childflow import _SPAWN_NAME_DISPLAY_MAX, CtxSpawnOps
from taskq.workflows._progress import MESSAGE_TERMINAL_MAX
from taskq.workflows._worker_execution import get_compiled_workflow
from taskq.workflows.api._runner_ladder import is_deterministic_authoring_failure
from tests._wf_fixtures import StubSpawnHost, expire_holds


class _LeafOut(BaseModel):
    n: int


class _ParentOut(BaseModel):
    ok: bool


class _WorkOutcome(enum.Enum):
    ok = "ok"


def _register_terminalless_child(app: WorkflowApp, name: str) -> None:
    @app.actor(queue="cpu", max_attempts=1)
    async def sunk_leaf(ctx: StepContext) -> _LeafOut:
        return _LeafOut(n=1)

    workflow_factory = app.workflow(name)

    @workflow_factory
    def child() -> None:
        sink(step(sunk_leaf, key="leaf"))

    compiled = app.get(name)
    assert compiled.terminal is None, compiled.terminal


def _spawn_refusal(host: Any, name: str) -> ChildWorkflowUnknownError:
    """Invoke the spawn on ``host`` and return the refusal it raised."""
    with pytest.raises(ChildWorkflowUnknownError) as exc_info:
        asyncio.run(CtxSpawnOps.spawn_flow(host, name, result=_LeafOut))
    return exc_info.value


def _spawn_outcome(name: str) -> ChildWorkflowUnknownError | None:
    """The target refusal ``spawn_flow`` raises, or ``None`` when the target
    check passes (the inert host's own trip-wire proves it was reached)."""
    try:
        asyncio.run(CtxSpawnOps.spawn_flow(cast("Any", StubSpawnHost()), name, result=_LeafOut))
    except ChildWorkflowUnknownError as exc:
        return exc
    except AssertionError:
        return None
    raise AssertionError("spawn_flow neither refused nor reached the host")


def _register(app: WorkflowApp, name: str) -> Exception | None:
    """``app.get`` through the registration door; returns its refusal, if any."""
    try:
        app.get(name)
    except Exception as exc:  # Why: the pin reports whatever the door did.
        return exc
    return None


async def _shared_leaf_body(ctx: StepContext) -> _LeafOut:
    """One shared body object: both apps wire the SAME function, so their body
    maps are equal and the D1 registry's differing-body guard cannot fire."""
    return _LeafOut(n=1)


def _terminal_less_app(name: str) -> WorkflowApp:
    app = WorkflowApp()
    workflow_factory = app.workflow(name)

    @workflow_factory
    def child() -> None:
        sink(step(_shared_leaf_body, key="leaf"))

    return app


def _terminal_bearing_app(name: str) -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow(name)
    def child() -> Promise[object]:
        return build(step(_shared_leaf_body, key="leaf"))

    return app


def _worker_settings(schema: str, dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema, "HEARTBEAT_INTERVAL": 0.05}
    )


async def _claim_next(
    pool: asyncpg.Pool, schema: str, worker: JobId, *, timeout_s: float = 10
) -> dict[str, Any]:
    """One actual pending workflow claim, retrying only a not-yet-due row."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    from taskq.backend._sweeps import sweep_scheduled_to_pending

    while loop.time() < deadline:
        async with pool.acquire() as conn:
            await sweep_scheduled_to_pending(conn, schema=schema)
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
    raise TimeoutError(f"worker {worker} found no due workflow row within {timeout_s}s")


async def _run_claimed_step(
    pool: asyncpg.Pool,
    schema: str,
    worker: JobId,
    runner: FlowRunner,
    expected_step: str,
) -> tuple[dict[str, Any], str]:
    claimed = await _claim_next(pool, schema, worker)
    assert claimed["step_key"] == expected_step, (
        f"expected {expected_step!r}, claimed {claimed['step_key']!r} "
        f"attempt={claimed['attempt']} epoch={claimed['claim_epoch']}"
    )
    raw_meta = claimed["metadata"]
    meta = cast("dict[str, Any]", json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta)
    flow_value = meta.get("flow_id")
    assert isinstance(flow_value, str)
    outcome = await runner.run_fleet_claimed_step(
        JobId(flow_value),
        claimed,
        attempt=int(claimed["attempt"]),
        claim_epoch=int(claimed["claim_epoch"]),
    )
    return claimed, outcome


# ── the deterministic classification ────────────────────────────────────


def test_terminalless_child_refusal_is_classified_as_deterministic() -> None:
    """Refusing a terminal-less child workflow must route deterministically
    (both directly and when embedded in an ``ExceptionGroup``) while keeping
    ``ChildWorkflowUnknownError`` catch compatibility."""
    child_name = f"terminalless_unit_child_{new_uuid().hex[:12]}"
    child_app = WorkflowApp()
    _register_terminalless_child(child_app, child_name)

    exc = _spawn_refusal(cast("Any", StubSpawnHost()), child_name)

    assert isinstance(exc, ChildWorkflowNoTerminalError), (
        f"a registered-but-terminal-less child is reported as {type(exc).__name__} — "
        "the record must name the terminal-less refusal, not an unknown workflow"
    )
    assert "terminal" in str(exc).lower(), str(exc)
    assert is_deterministic_authoring_failure(exc), (
        f"terminal-less child refusal ({type(exc).__name__}) is not classified as a "
        "deterministic authoring failure — the retry ladder will burn all attempts"
    )
    grouped = ExceptionGroup("spawn fanout", [exc])
    assert is_deterministic_authoring_failure(grouped), (
        f"grouped terminal-less child refusal ({type(exc).__name__}) is not classified "
        "as a deterministic authoring failure"
    )


def test_the_unregistered_name_refusal_keeps_its_rolling_deploy_retry_tolerance() -> None:
    """The base refusal must stay OUT of the deterministic tuple (directly and
    inside the ``ExceptionGroup`` spelling a fan-out produces), while the
    subclass it carries stays IN: the class name is the only discriminator
    between "wait for the deploy" and "no retry can fix this".

    The subclass relation makes the tempting simplification ("list the base
    instead of the subclass") invisible to every other gate: the terminal-less
    refusal keeps routing deterministically, so the whole child-flow estate
    stays green while the rolling-deploy tolerance is gone."""
    refusal = ChildWorkflowUnknownError("workflow 'x' is not registered")

    assert not is_deterministic_authoring_failure(refusal), (
        "the unregistered-name refusal is classified deterministic — a definition "
        "the rolling deploy has not imported on this worker loses its retry "
        "tolerance and terminal-fails on attempt 1"
    )
    assert not is_deterministic_authoring_failure(ExceptionGroup("fanout", [refusal])), (
        "a grouped unregistered-name refusal is classified deterministic — the "
        "tolerance is lost for the ``asyncio.TaskGroup`` fan-out spelling too"
    )
    assert issubclass(ChildWorkflowNoTerminalError, ChildWorkflowUnknownError)
    assert is_deterministic_authoring_failure(ChildWorkflowNoTerminalError("terminal-less"))
    assert not is_deterministic_authoring_failure(ChildWorkflowUnknownError("terminal-less"))


# ── the persisted message window ───────────────────────────────────────


def test_terminalless_refusal_keeps_its_remedy_when_the_name_escapes_in_repr() -> None:
    """A name AT the display budget whose code points ``repr`` escapes.

    ``U+E0020`` is a non-printable astral code point: ``repr`` renders it as
    ``\\U000e0020`` (10 characters), so 40 of them — exactly
    ``_SPAWN_NAME_DISPLAY_MAX``, i.e. a name the raw-length budget would accept
    unchanged — become a 402-character repr. The bound is measured on the
    rendered ``repr``, so the remedy (the tail the budget exists to protect)
    survives the persisted ``MESSAGE_TERMINAL_MAX`` slice."""
    name = "\U000e0020" * _SPAWN_NAME_DISPLAY_MAX
    assert len(name) == _SPAWN_NAME_DISPLAY_MAX
    assert len(repr(name)) == 402, len(repr(name))

    child_app = WorkflowApp()
    _register_terminalless_child(child_app, name)

    exc = _spawn_refusal(cast("Any", StubSpawnHost()), name)
    message = str(exc)
    persisted = message[:MESSAGE_TERMINAL_MAX]

    assert len(message) <= MESSAGE_TERMINAL_MAX, (
        f"the message is {len(message)} characters — past the "
        f"{MESSAGE_TERMINAL_MAX}-character window the name budget is chosen for"
    )
    assert "declares no terminal node" in persisted, persisted
    assert "instead of sink-ing it" in persisted, (
        "the remedy is truncated out of the persisted row while the diagnosis "
        f"survives: {persisted[-120:]!r}"
    )


def test_terminalless_child_refusal_keeps_its_diagnosis_for_any_name_length() -> None:
    """The refusal's persisted form is truncated to ``MESSAGE_TERMINAL_MAX``,
    so the terminal diagnosis must survive that slice for a registry key of any
    length (nothing else bounds a workflow name). The name here deliberately
    does NOT contain the word "terminal": an unbounded ``{name!r}`` would push
    the diagnosis past the window and this pin would fail."""
    child_name = "longname_" + "x" * MESSAGE_TERMINAL_MAX
    child_app = WorkflowApp()
    _register_terminalless_child(child_app, child_name)
    assert len(child_name) > MESSAGE_TERMINAL_MAX

    exc = _spawn_refusal(cast("Any", StubSpawnHost()), child_name)

    persisted = str(exc)[:MESSAGE_TERMINAL_MAX].lower()
    assert "terminal" in persisted, (
        f"the terminal diagnosis is truncated away for a {len(child_name)}-character "
        f"workflow name (the name must be bounded in the message): {persisted!r}"
    )


def test_unregistered_name_refusal_keeps_its_diagnosis_for_a_long_name() -> None:
    """The sibling refusal is bounded too: a long unregistered dispatch target
    must not push ``is not registered`` out of the persisted window."""
    name = f"unregistered_target_{new_uuid().hex[:12]}_" + "x" * 500
    assert len(name) > MESSAGE_TERMINAL_MAX

    exc = _spawn_refusal(cast("Any", StubSpawnHost()), name)

    persisted = str(exc)[:MESSAGE_TERMINAL_MAX]
    assert "is not registered" in persisted, (
        f"the diagnosis is truncated away for a {len(name)}-character target: {persisted[-120:]!r}"
    )


# ── the compile registry's one-name-one-graph law ──────────────────────


def test_a_second_app_cannot_flip_a_registered_name_to_terminal_bearing() -> None:
    """A name recorded terminal-less must not silently become terminal-bearing
    because another app in this process declares the same name over the same
    bodies — the deterministic spawn verdict reads this entry."""
    name = f"flip_probe_{new_uuid().hex[:12]}"

    assert _register(_terminal_less_app(name), name) is None
    assert get_compiled_workflow(name).terminal is None  # pyright: ignore[reportAttributeAccessIssue]  # Why: the cache is typed ``object``; the pin reads the one attribute at issue.

    from taskq.workflows.definitions import DuplicateWorkflowError

    refused = _register(_terminal_bearing_app(name), name)

    recorded = get_compiled_workflow(name)
    terminal = getattr(recorded, "terminal", None)
    assert isinstance(refused, DuplicateWorkflowError) and terminal is None, (
        "a second WorkflowApp declaring the same workflow name over an equal body "
        f"map must fail closed with DuplicateWorkflowError (got {refused!r}, "
        f"terminal={terminal!r})"
    )


def test_a_second_app_cannot_take_a_working_spawn_target_away() -> None:
    """The dangerous direction: a name whose spawn is satisfied must not start
    refusing (non-retryably, on attempt 1) because another app re-recorded it."""
    from taskq.workflows.definitions import DuplicateWorkflowError

    name = f"flip_spawn_probe_{new_uuid().hex[:12]}"

    assert _register(_terminal_bearing_app(name), name) is None
    assert get_compiled_workflow(name).terminal == "leaf"  # pyright: ignore[reportAttributeAccessIssue]  # Why: the cache is typed ``object``; the pin reads the one attribute at issue.
    assert _spawn_outcome(name) is None, "the terminal-bearing graph was refused"

    refused = _register(_terminal_less_app(name), name)
    assert isinstance(refused, DuplicateWorkflowError), (
        f"expected DuplicateWorkflowError on equal-body terminal flip, got {refused!r}"
    )

    refusal = _spawn_outcome(name)
    assert not isinstance(refusal, ChildWorkflowNoTerminalError), (
        "a second app's equal-body, terminal-less declaration turned a satisfied "
        "spawn target into a non-retryable per-node refusal — one name compiles both "
        "ways in one process and the verdict follows whichever recorded last"
    )


# ── the persisted record: the deterministic refusal ────────────────────


@pytest.mark.integration
class TestSpawnTerminalLessChild:
    async def test_spawn_flow_on_terminalless_child_fails_deterministically_on_attempt_one(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """A parent step with ``max_attempts=3`` spawning a registered
        terminal-less child must fail on attempt 1 with exactly 1 failed
        ledger row, 1 body invocation, 0 child flows created, and no spawn
        cursor or receipt written."""
        suffix = new_uuid().hex[:12]
        child_name = f"terminalless_child_{suffix}"
        parent_name = f"terminalless_parent_{suffix}"

        child_app = WorkflowApp()
        _register_terminalless_child(child_app, child_name)

        calls = 0
        parent_app = WorkflowApp()

        @parent_app.actor(queue="cpu", max_attempts=3)
        async def spawner(ctx: StepContext) -> _ParentOut:
            nonlocal calls
            calls += 1
            handle = await ctx.spawn_flow(child_name, result=_LeafOut)
            await handle.wait()
            return _ParentOut(ok=True)

        @parent_app.workflow(parent_name)
        def parent() -> Promise[object]:
            return build(step(spawner, key="wrap"))

        runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        children = await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        assert children == 0, f"spawn_flow minted {children} child flow(s) for terminal-less child"

        node = await wf_conn.fetchrow(
            f'SELECT status, attempt, error_class, error_message, metadata FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["status"] == "failed", node["status"]
        assert node["error_class"] == "ChildWorkflowNoTerminalError", (
            f"the parent row records {node['error_class']!r} for a registered child — "
            "an operator would read that as an unregistered workflow name"
        )
        assert "terminal" in str(node["error_message"]).lower(), str(node["error_message"])

        raw_meta = node["metadata"]
        meta = cast(
            "dict[str, Any]", json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
        )
        touched = sorted(
            k for k in meta if k.startswith(("spawn_slot_", "spawn_cursor_")) or k == "child_flows"
        )
        assert touched == [], f"spawn_flow wrote {touched} before refusing terminal-less child"

        failed = await wf_conn.fetch(
            f'SELECT error_class, error_message FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )
        assert len(failed) == 1, (
            f"terminal-less child refusal burned {len(failed)} attempts "
            f"({[r['error_class'] for r in failed]}) — expected deterministic failure on attempt 1"
        )
        assert failed[0]["error_class"] == "ChildWorkflowNoTerminalError", failed[0]["error_class"]
        assert node["attempt"] == 1, f"expected attempt=1, got {node['attempt']}"
        assert calls == 1, f"expected parent body to run once, ran {calls} times"

    async def test_a_declared_on_exhaustion_effect_does_not_fire_for_the_refusal(
        self,
        wf_pool: asyncpg.Pool,
        wf_schema: str,
        wf_conn: asyncpg.Connection,
        pg_dsn: str,
    ) -> None:
        """The guide promises the deterministic refusal suppresses a declared
        ``on_exhaustion`` effect. A chain step declaring one whose body hits the
        terminal-less refusal must record exactly one failed ledger row and
        materialise no ``.__on_exhaustion`` job — a regression here dispatches
        the author's compensation for an exhaustion the ladder never reached."""
        suffix = new_uuid().hex[:12]
        child_name = f"terminalless_effect_child_{suffix}"
        parent_name = f"terminalless_effect_parent_{suffix}"

        child_app = WorkflowApp()
        _register_terminalless_child(child_app, child_name)

        effect_calls = 0

        async def effect_body(ctx: StepContext, event: ExhaustionEvent) -> None:
            nonlocal effect_calls
            effect_calls += 1

        effect = TerminalEffect(
            body=effect_body,
            policy=ExecutionPolicy(actor=f"effect-{suffix}", queue="default"),
        )

        calls = 0

        async def spawner(ctx: StepContext, item: object) -> _WorkOutcome:
            nonlocal calls
            calls += 1
            await ctx.spawn_flow(child_name, result=_LeafOut)
            return _WorkOutcome.ok

        chain = Chain(
            name=f"terminalless_effect_chain_{suffix}",
            start="work",
            steps={
                "work": Step(
                    body=spawner,
                    outcomes=_WorkOutcome,
                    policy=ExecutionPolicy(
                        actor="wf",
                        retry=RetrySpec(
                            policy=RetryPolicy(max_attempts=3, jitter=0),
                            on_exhaustion=effect,
                        ),
                    ),
                )
            },
        )

        async def source(ctx: StepContext) -> None:
            await ctx.emit_batch(
                [chain_start(chain, {"n": 1}, map_index=0, trace_id="t0")],
                cursor={"page": 0},
            )

        parent_app = WorkflowApp()

        @parent_app.workflow(parent_name)
        def flow() -> Promise[object]:
            return build(chain_source(chain, source, key="source"))

        runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        worker = JobId(
            await register_worker(
                wf_pool, _worker_settings(wf_schema, pg_dsn), workflow_execution=True
            )
        )
        for actor in ("wf", f"effect-{suffix}"):
            await wf_pool.execute(
                f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1,$2) '
                "ON CONFLICT (actor) DO NOTHING",
                actor,
                "default",
            )
        fleet_runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema, worker_id=worker)

        _source_claim, source_outcome = await _run_claimed_step(
            wf_pool, wf_schema, worker, fleet_runner, "source"
        )
        assert source_outcome == "succeeded", source_outcome
        _work_claim, work_outcome = await _run_claimed_step(
            wf_pool, wf_schema, worker, fleet_runner, "work"
        )
        assert work_outcome != "succeeded", work_outcome

        effects = await wf_conn.fetch(
            f'SELECT id FROM "{wf_schema}".jobs WHERE step_key = $1 '
            "AND (metadata->>'flow_id')::uuid = $2",
            "work.__on_exhaustion",
            flow_id,
        )
        assert effects == [], (
            f"the deterministic refusal materialised {len(effects)} on_exhaustion job(s) — "
            "a declared effect must not fire for a failure the ladder never exhausted"
        )
        assert effect_calls == 0, f"the on_exhaustion body ran {effect_calls} times"

        failed = await wf_conn.fetch(
            f'SELECT error_class FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'work' AND status = 'failed'",
            flow_id,
        )
        assert [r["error_class"] for r in failed] == ["ChildWorkflowNoTerminalError"], [
            r["error_class"] for r in failed
        ]
        assert calls == 1, f"the chain step body ran {calls} times, not the declared 1"

        node = await wf_conn.fetchrow(
            f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '
            "WHERE step_key = 'work' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["status"] == "failed", (node["status"], node["error_class"])
        assert node["error_class"] == "ChildWorkflowNoTerminalError", node["error_class"]
        assert node["attempt"] == 1, f"the refusal spent the ladder: attempt={node['attempt']}"


@pytest.mark.integration
async def test_a_held_step_records_the_claim_ordinal_not_ladder_attempt_one(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """A parent step with ``max_attempts=3`` that holds first and reaches the
    terminal-less spawn only on its resumed execution: the node fails with the
    deterministic class and exactly one ``failed`` ledger row (the ladder is
    unburned), while ``jobs.attempt`` reads 2 — the claim ordinal, the resumed
    claim's own increment, not the one attempt the refusal actually spent."""
    suffix = new_uuid().hex[:12]
    child_name = f"probe_terminalless_child_{suffix}"
    parent_name = f"probe_terminalless_parent_{suffix}"
    child_app = WorkflowApp()
    _register_terminalless_child(child_app, child_name)

    calls = 0
    parent_app = WorkflowApp()

    @parent_app.actor(queue="cpu", max_attempts=3)
    async def spawner(ctx: StepContext) -> _ParentOut:
        nonlocal calls
        calls += 1
        await ctx.wait_until(120.0, key="before-spawn")
        handle = await ctx.spawn_flow(child_name, result=_LeafOut)
        await handle.wait()
        return _ParentOut(ok=True)

    @parent_app.workflow(parent_name)
    def parent() -> Promise[object]:
        return build(step(spawner, key="wrap"))

    runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held", max_ticks=200) == "held"
    await expire_holds(wf_conn, wf_schema, flow_id)
    outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
    assert outcome == "terminal", outcome

    node = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node is not None
    failed = await wf_conn.fetch(
        f'SELECT error_class FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
        flow_id,
    )

    assert node["status"] == "failed", node["status"]
    assert node["error_class"] == "ChildWorkflowNoTerminalError", node["error_class"]
    assert "terminal" in str(node["error_message"]).lower(), node["error_message"]
    assert len(failed) == 1, (
        f"the terminal-less refusal burned {len(failed)} ledger attempts "
        f"({[r['error_class'] for r in failed]}) after a hold — the deterministic "
        "route must not spend the ladder whatever the node's hold history"
    )
    assert failed[0]["error_class"] == "ChildWorkflowNoTerminalError", failed[0]["error_class"]
    assert calls == 2, f"the body ran {calls} times (once to hold, once on resume)"
    assert node["attempt"] == 2, (
        f"jobs.attempt reads {node['attempt']}: it is the CLAIM ordinal (the resumed "
        "claim incremented it), not the ladder attempt the refusal spent — the guide's "
        "'terminal-fails on attempt 1' is the ladder's count, and an operator query "
        "keyed on attempt = 1 under-collects this shape"
    )


@pytest.mark.integration
class TestUnregisteredNameKeepsItsLadder:
    async def test_an_unregistered_name_refusal_burns_the_full_retry_ladder(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The behavioural half of the split, end to end: an unregistered name
        spends its declared budget (the definitions may be imported on the
        workers a rolling deploy has already replaced), records the BASE class,
        and re-pends between attempts. Listing the base in the deterministic
        tuple would collapse this to attempt 1 with every other gate green."""
        suffix = new_uuid().hex[:12]
        parent_name = f"unregistered_parent_{suffix}"
        missing_name = f"never_registered_child_{suffix}"

        calls = 0
        parent_app = WorkflowApp()

        @parent_app.actor(queue="cpu", max_attempts=3)
        async def spawner(ctx: StepContext) -> _ParentOut:
            nonlocal calls
            calls += 1
            await ctx.spawn_flow(missing_name, result=_LeafOut)
            return _ParentOut(ok=True)

        @parent_app.workflow(parent_name)
        def parent() -> Promise[object]:
            return build(step(spawner, key="wrap"))

        runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        node = await wf_conn.fetchrow(
            f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["status"] == "failed", (node["status"], node["error_class"])
        assert node["error_class"] == "ChildWorkflowUnknownError", (
            f"the unregistered-name refusal records {node['error_class']!r}; the base "
            "class is the vocabulary the rolling-deploy tolerance names"
        )
        assert node["attempt"] == 3, (
            f"an unregistered name terminal-failed on attempt {node['attempt']} — the "
            "definition may be imported on the workers this deploy has not replaced yet"
        )
        failed = await wf_conn.fetch(
            f'SELECT error_class FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'wrap' AND status = 'failed'",
            flow_id,
        )
        assert [r["error_class"] for r in failed] == ["ChildWorkflowUnknownError"] * 3, (
            f"expected 3 laddered attempts recorded, got {[r['error_class'] for r in failed]}"
        )
        assert calls == 3, f"the parent body ran {calls} times, not the declared 3"


@pytest.mark.integration
class TestTerminalLessThroughTheRunDoor:
    async def test_running_a_terminal_less_workflow_in_a_body_reads_no_result_and_burns_no_ladder(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """``run()`` on a terminal-less workflow returns ``("terminal",
        None)`` instead of raising the non-deterministic ``WorkflowRunError``
        at the result read. A body doing so therefore keeps attempt 1 — the
        guard is what stands between that door and the retry ladder."""
        suffix = new_uuid().hex[:12]
        child_name = f"terminalless_rundoor_child_{suffix}"
        parent_name = f"terminalless_rundoor_parent_{suffix}"

        child_app = WorkflowApp()
        _register_terminalless_child(child_app, child_name)
        child_compiled = child_app.get(child_name)
        assert child_compiled.terminal is None

        calls = 0
        seen: list[tuple[str, object]] = []
        parent_app = WorkflowApp()

        @parent_app.actor(queue="cpu", max_attempts=3)
        async def rundoor(ctx: StepContext) -> _ParentOut:
            nonlocal calls
            calls += 1
            nested = await run(child_compiled, wf_pool, wf_schema, max_ticks=50)
            seen.append((nested.outcome, nested.result))
            return _ParentOut(ok=True)

        @parent_app.workflow(parent_name)
        def parent() -> Promise[object]:
            return build(step(rundoor, key="wrap"))

        runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", outcome

        node = await wf_conn.fetchrow(
            f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '
            "WHERE step_key = 'wrap' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node is not None
        assert node["status"] == "succeeded", (node["status"], node["attempt"], node["error_class"])
        assert node["attempt"] == 1, f"the run door burned the ladder: attempt={node['attempt']}"
        assert calls == 1, f"the parent body ran {calls} times"
        assert seen == [("terminal", None)], (
            "the packaged run's terminal-less read returned "
            f"{seen!r} — a WorkflowRunError here rides the retry ladder and "
            "records an unfixable defect as a retryable one"
        )
