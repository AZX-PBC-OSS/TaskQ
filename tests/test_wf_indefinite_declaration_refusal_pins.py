"""Declaration-time refusal of ``retry_kind="indefinite"`` on new workflows.

Workflow rows never set ``schedule_to_close``, so a newly declared workflow
row persisted with ``retry_kind="indefinite"`` has no crash ceiling under
``_RECLAIM_HAS_BUDGET_SQL`` and contradicts ``RetrySpec``'s finite-budget
contract (diagnostic ``E21-execution-policy``). Every new workflow
declaration boundary must reject ``"indefinite"`` with
:class:`~taskq.workflows.WorkflowPolicyError`, while
:func:`~taskq.workflows._policy.canonical_retry_kind` and the shared reclaim
SQL retain legacy ``"indefinite"`` support for previously persisted rows and
vanilla actors.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import enum
from typing import Any, cast

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.backend._sweeps import _RECLAIM_HAS_BUDGET_SQL
from taskq.retry import RetryOverride, RetryPolicy
from taskq.workflows import (
    DONE,
    Chain,
    Promise,
    RetrySpec,
    Route,
    RouteArm,
    Step,
    StepContext,
    WorkflowApp,
    WorkflowPolicyError,
    WorkflowRetryOverride,
    build,
    chain_fork,
    chain_source,
    loop,
    map_source,
    route,
    step,
)
from taskq.workflows._emit import _emit_tx, emit_batch
from taskq.workflows._policy import canonical_retry_kind
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import _drain_outbox_once, drain_outbox
from taskq.workflows._types import ChildSpec, EmitChild, FiniteRetryKind, ForkSpec, NodeSpec
from taskq.workflows.api._graph import NodeDecl
from taskq.workflows.api._runner import _node_retry_spec
from taskq.workflows.api._runner_codec import FlowEntryShim
from taskq.workflows.chain import type_tag
from taskq.workflows.definitions import WorkflowDef
from taskq.workflows.engine import render_workflow_sql
from taskq.workflows.ledger import insert_flow_run, run_idempotency_scope

# Typed as Any so static narrowing of declaration Literals to finite kinds
# continues to type-check while runtime callers still probe the refused word.
_INDEFINITE: Any = "indefinite"
_FLOW_ID = JobId("00000000-0000-0000-0000-000000000001")
_JOB_ID = JobId("00000000-0000-0000-0000-000000000002")
_WORKER_ID = JobId("00000000-0000-0000-0000-000000000003")


class _Item(BaseModel):
    value: int


class _Outcome(enum.Enum):
    OK = "ok"


async def _plain_body(ctx: StepContext) -> str:
    return "ok"


async def _list_body(ctx: StepContext) -> list[_Item]:
    return [_Item(value=1)]


async def _item_body(ctx: StepContext, item: _Item) -> str:
    return str(item.value)


async def _consumer_body(ctx: StepContext, items: list[str]) -> str:
    return ",".join(items)


def _assert_e21_indefinite(
    exc_info: pytest.ExceptionInfo[WorkflowPolicyError], *, what: str | None = None
) -> None:
    assert exc_info.value.code == "E21-execution-policy"
    assert "E21-execution-policy" in str(exc_info.value)
    assert "indefinite" in str(exc_info.value)
    if what is not None:
        # The ``what`` prefix is the only field that distinguishes one door's
        # refusal from another's, so a pin that names its door proves the
        # refusal came from that door rather than a validator behind it.
        assert exc_info.value.message.startswith(f"{what} must be a finite retry kind"), (
            f"expected the refusal to name {what!r}, got {exc_info.value.message!r}"
        )


def test_step_direct_refuses_indefinite_retry_kind() -> None:
    app = WorkflowApp()

    @app.workflow("wf_step_direct_indefinite")
    def _wf() -> Promise[object]:
        return build(step(_plain_body, key="s", retry_kind="indefinite"))

    with pytest.raises(WorkflowPolicyError) as exc_info:
        app.get("wf_step_direct_indefinite")
    _assert_e21_indefinite(exc_info, what=f"step {'s'!r}'s retry_kind")


@pytest.mark.parametrize("fanout", ["map_source", "route"])
def test_step_downstream_of_fanout_refuses_indefinite_at_declaration(fanout: str) -> None:
    app = WorkflowApp()
    wf_name = f"wf_downstream_{fanout}_indefinite"

    @app.workflow(wf_name)
    def _wf() -> Promise[object]:
        src = step(_list_body, key="src")
        joined = (
            map_source(src, _item_body)
            if fanout == "map_source"
            else route(src, {_Item: _item_body})
        )
        return build(step(_consumer_body, joined, key="after", retry_kind="indefinite"))

    with pytest.raises(WorkflowPolicyError) as exc_info:
        app.get(wf_name)
    _assert_e21_indefinite(exc_info, what=f"step {'after'!r}'s retry_kind")


def test_workflow_actor_declaration_refuses_indefinite_retry_kind() -> None:
    app = WorkflowApp()

    with pytest.raises(WorkflowPolicyError) as exc_info:

        @app.actor(retry_kind="indefinite")
        async def _indefinite_actor(ctx: StepContext) -> str:
            return "ok"

    _assert_e21_indefinite(exc_info, what="actor retry_kind")


def test_step_wired_with_indefinite_actor_handle_refuses_indefinite() -> None:
    app = WorkflowApp()

    @app.actor(retry_kind="transient")
    async def _handle(ctx: StepContext) -> str:
        return "ok"

    # Exercise step()'s own handle-copy validation path even when @app.actor
    # rejects "indefinite" earlier.
    cast("Any", _handle).retry_kind = _INDEFINITE

    @app.workflow("wf_step_wired_actor_indefinite")
    def _wf() -> Promise[object]:
        return build(step(_handle, key="s"))

    with pytest.raises(WorkflowPolicyError) as exc_info:
        app.get("wf_step_wired_actor_indefinite")
    _assert_e21_indefinite(exc_info, what=f"step {'s'!r}'s retry_kind")


@pytest.mark.parametrize("entrypoint", ["step", "map_source", "route", "loop", "chain"])
def test_actor_handle_wiring_entrypoints_refuse_indefinite_actor(entrypoint: str) -> None:
    app = WorkflowApp()

    @app.actor(retry_kind="transient")
    async def _actor_body(ctx: StepContext, item: _Item) -> str:
        return str(item.value)

    # Bypass the decorator's own door so each wiring entrypoint is actually
    # exercised; raising during @app.actor would leave all five paths untested.
    cast("Any", _actor_body).retry_kind = _INDEFINITE

    door = "actor retry_kind"
    with pytest.raises(WorkflowPolicyError) as exc_info:
        if entrypoint == "step":
            door = f"step {'s'!r}'s retry_kind"

            @app.workflow("wf_actor_step")
            def _wf_step() -> Promise[object]:
                return build(step(_actor_body, _Item(value=1), key="s"))

            app.get("wf_actor_step")
        elif entrypoint == "map_source":
            door = f"map {'src'!r}'s actor retry_kind"

            @app.workflow("wf_actor_map")
            def _wf_map() -> Promise[object]:
                src = step(_list_body, key="src")
                return build(map_source(src, _actor_body))

            app.get("wf_actor_map")
        elif entrypoint == "route":
            door = f"route arm {type_tag(_Item)!r}'s actor retry_kind"

            @app.workflow("wf_actor_route")
            def _wf_route() -> Promise[object]:
                src = step(_list_body, key="src")
                return build(route(src, {_Item: RouteArm(body=_actor_body)}))

            app.get("wf_actor_route")
        elif entrypoint == "loop":
            door = "loop body actor retry_kind"

            @app.workflow("wf_actor_loop")
            def _wf_loop() -> Promise[object]:
                return build(loop("lp", _actor_body, max_iterations=2))

            app.get("wf_actor_loop")
        else:
            # Construction is the chain's door: __post_init__ validates each
            # step's handle before any record is started.
            door = "actor retry_kind"
            Chain(
                name="wf_actor_chain",
                start="s",
                steps={
                    "s": Step(
                        body=_actor_body,
                        outcomes=_Outcome,
                        route=Route({_Outcome.OK: DONE}),
                    )
                },
            )

    _assert_e21_indefinite(exc_info, what=door)


def test_chain_declaration_refuses_indefinite_actor_handle() -> None:
    app = WorkflowApp()

    @app.actor(retry_kind="transient")
    async def _actor_body(ctx: StepContext, item: _Item) -> str:
        return str(item.value)

    cast("Any", _actor_body).retry_kind = _INDEFINITE

    with pytest.raises(WorkflowPolicyError) as exc_info:
        Chain(
            name="wf_actor_chain_declaration",
            start="s",
            steps={
                "s": Step(
                    body=_actor_body,
                    outcomes=_Outcome,
                    route=Route({_Outcome.OK: DONE}),
                )
            },
        )
    _assert_e21_indefinite(exc_info, what="actor retry_kind")


def test_chain_source_refuses_handle_mutated_after_chain_construction() -> None:
    app = WorkflowApp()

    @app.actor(retry_kind="transient")
    async def _actor_body(ctx: StepContext, item: _Item) -> str:
        return str(item.value)

    chain = Chain(
        name="wf_actor_chain_source",
        start="s",
        steps={
            "s": Step(
                body=_actor_body,
                outcomes=_Outcome,
                route=Route({_Outcome.OK: DONE}),
            )
        },
    )
    cast("Any", _actor_body).retry_kind = _INDEFINITE

    async def _source(ctx: StepContext) -> None:
        return None

    @app.workflow("wf_chain_source_mutated_actor")
    def _wf() -> Promise[object]:
        return build(chain_source(chain, _source, key="source"))

    with pytest.raises(WorkflowPolicyError) as exc_info:
        app.get("wf_chain_source_mutated_actor")
    _assert_e21_indefinite(exc_info, what="actor retry_kind")


@pytest.mark.parametrize("with_child", [True, False])
def test_chain_fork_refuses_indefinite_retry_kind(with_child: bool) -> None:
    child = (
        ChildSpec(step_key="next", actor="wf", queue="default", payload={}, map_index=0)
        if with_child
        else None
    )

    with pytest.raises(WorkflowPolicyError) as exc_info:
        chain_fork(child, trace_id="trace-1", retry_kind=_INDEFINITE)
    _assert_e21_indefinite(exc_info, what="chain_fork retry_kind")


def test_fork_spec_refuses_indefinite_retry_kind() -> None:
    child = ChildSpec(step_key="next", actor="wf", queue="default", payload={}, map_index=0)

    with pytest.raises(WorkflowPolicyError) as exc_info:
        ForkSpec(children=(child,), retry_kind=_INDEFINITE)
    _assert_e21_indefinite(exc_info, what="ForkSpec.retry_kind")


def test_node_spec_refuses_indefinite_retry_kind() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        NodeSpec(
            flow_id=_FLOW_ID,
            step_key="s",
            actor="wf",
            queue="default",
            retry_kind=_INDEFINITE,
        )
    _assert_e21_indefinite(exc_info, what="NodeSpec.retry_kind")


def test_node_spec_with_resolved_retry_refuses_indefinite_retry_kind() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        NodeSpec(
            flow_id=_FLOW_ID,
            step_key="s",
            actor="wf",
            queue="default",
            retry=RetrySpec(),
            retry_kind=_INDEFINITE,
        )
    _assert_e21_indefinite(exc_info, what="NodeSpec.retry_kind")


def test_compiled_node_lowering_refuses_indefinite_retry_kind() -> None:
    node = NodeDecl(
        key="after",
        actor="wf",
        queue="default",
        body=None,
        retry_kind=_INDEFINITE,
    )

    with pytest.raises(WorkflowPolicyError) as exc_info:
        _node_retry_spec(node)
    _assert_e21_indefinite(exc_info, what="retry_kind")


@pytest.mark.parametrize(
    "mapped_value",
    [
        _INDEFINITE,
        RetryOverride(kind="indefinite"),
        WorkflowRetryOverride(kind="indefinite"),
    ],
    ids=["string", "retry-override", "workflow-retry-override"],
)
def test_retry_spec_exceptions_map_refuses_indefinite(mapped_value: Any) -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        RetrySpec(exceptions={ValueError: mapped_value})
    _assert_e21_indefinite(exc_info, what="exception retry kind")


def test_dynamic_classifier_cannot_reintroduce_indefinite() -> None:
    spec = RetrySpec(
        classifier=lambda _exc, _attempt: RetryOverride(kind="indefinite"),
        non_consuming_limit=1,
    )

    with structlog.testing.capture_logs() as logs:
        assert spec.classify(ValueError("boom"), 1) is None

    assert [entry["event"] for entry in logs] == ["workflow-retry-classifier-indefinite-refused"]
    assert "flow_id" not in logs[0] and "step_key" not in logs[0]

    # A row-context call attributes the refusal to the run and step whose
    # declaration carried the hook — without it the event names nothing.
    with structlog.testing.capture_logs() as scoped_logs:
        assert spec.classify(ValueError("boom"), 1, flow_id=str(_FLOW_ID), step_key="s") is None

    assert [entry["event"] for entry in scoped_logs] == [
        "workflow-retry-classifier-indefinite-refused"
    ]
    assert scoped_logs[0]["flow_id"] == str(_FLOW_ID)
    assert scoped_logs[0]["step_key"] == "s"


class _UnreachablePool:
    """Guard proving door validation rejects before any pool acquisition."""

    def acquire(self) -> Any:
        raise AssertionError("reached pool.acquire() without refusing retry_kind='indefinite'")


class _UnreachableConn:
    """Guard proving a refused declaration issues no statement at all.

    The flow-root statements are recognised by identity against the rendered
    ``wsql`` (never by SQL text), so a whitespace-only reflow of one keeps
    this double honest."""

    def __init__(self, wsql: WorkflowSql) -> None:
        self.wsql = wsql
        self.insert_attempted = False

    def fetchval(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(
            "reached the emit cursor write without refusing retry_kind='indefinite'"
        )

    async def fetchrow(self, sql: str, *_args: Any, **_kwargs: Any) -> Any:
        if sql == self.wsql.flow_run_read:
            raise AssertionError("read the flow root without refusing retry_kind='indefinite'")
        if sql == self.wsql.flow_run_insert:
            self.insert_attempted = True
            raise AssertionError(
                "reached the flow root insert without refusing retry_kind='indefinite'"
            )
        raise AssertionError(f"issued an unexpected statement without refusing: {sql!r}")


async def test_emit_batch_refuses_indefinite_retry_kind_at_door() -> None:
    child = EmitChild(
        step_key="start",
        actor="wf",
        queue="default",
        payload={},
        trace_id="trace-1",
        map_index=0,
    )

    with pytest.raises(WorkflowPolicyError) as exc_info:
        await emit_batch(
            cast("Any", _UnreachablePool()),
            render_workflow_sql("taskq"),
            flow_id=_FLOW_ID,
            source_id=_JOB_ID,
            worker_id=_WORKER_ID,
            attempt=1,
            claim_epoch=1,
            children=(child,),
            cursor={},
            retry_kind="indefinite",
        )
    _assert_e21_indefinite(exc_info, what="emit retry_kind")


async def test_emit_tx_refuses_indefinite_retry_kind_at_its_own_door() -> None:
    wsql = render_workflow_sql("taskq")

    with pytest.raises(WorkflowPolicyError) as exc_info:
        await _emit_tx(
            cast("Any", _UnreachableConn(wsql)),
            wsql,
            flow_id=_FLOW_ID,
            source_id=_JOB_ID,
            worker_id=_WORKER_ID,
            attempt=1,
            claim_epoch=1,
            children=(),
            cursor={},
            child_ids=[],
            max_attempts=3,
            retry_kind="indefinite",
            _window_hook=None,
        )
    _assert_e21_indefinite(exc_info, what="emit retry_kind")


async def test_drain_outbox_refuses_indefinite_default_retry_kind_at_door() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        await drain_outbox(
            cast("Any", _UnreachablePool()),
            render_workflow_sql("taskq"),
            default_retry_kind="indefinite",
        )
    _assert_e21_indefinite(exc_info, what="outbox default_retry_kind")


async def test_drain_outbox_once_refuses_indefinite_default_retry_kind_at_its_own_door() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        await _drain_outbox_once(
            cast("Any", _UnreachablePool()),
            render_workflow_sql("taskq"),
            default_retry_kind="indefinite",
        )
    _assert_e21_indefinite(exc_info, what="outbox default_retry_kind")


def test_workflow_def_refuses_indefinite_retry_kind() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        WorkflowDef(name="wf_def_indefinite", bodies={}, retry_kind="indefinite")
    _assert_e21_indefinite(exc_info, what="WorkflowDef.retry_kind")


async def test_insert_flow_run_refuses_indefinite_retry_kind_at_its_own_door() -> None:
    wsql = render_workflow_sql("taskq")
    conn = _UnreachableConn(wsql)
    entry = FlowEntryShim("wf_root_indefinite", {}, retry_kind="indefinite")

    with pytest.raises(WorkflowPolicyError) as exc_info:
        await insert_flow_run(
            cast("Any", conn),
            wsql,
            entry=entry,
            run_key="run-1",
        )
    _assert_e21_indefinite(exc_info, what="flow retry_kind")
    assert conn.insert_attempted is False


@pytest.mark.integration
async def test_legacy_indefinite_root_is_adopted_through_the_production_entry_shape(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """A legacy root (its COLUMN says ``indefinite``) replays onto the same run
    through the shape ``_insert_root`` builds: created=False, one row, and the
    stored kind survives the replay untouched.

    The adoption rides the run-key arbiter's conflict path, which never reads
    the declaration's kind, so the production entry shape (a ``FlowEntryShim``
    built with the dataclass default ``"transient"``) is all the replay needs."""
    entry = FlowEntryShim("wf_legacy_root", {"taskq:wf_input": None})
    assert entry.retry_kind == "transient"
    key = "legacy:2026-01-01T00:00:00Z"

    first = await insert_flow_run(wf_conn, wf_sql, entry=entry, run_key=key)
    assert first.created is True

    # the legacy row: persisted before the kind became undeclarable.
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET retry_kind = $1 WHERE id = $2',
        "indefinite",
        first.flow_id,
    )
    stamped = await wf_conn.fetchval(
        f'SELECT retry_kind FROM "{wf_schema}".jobs WHERE id = $1', first.flow_id
    )
    assert stamped == "indefinite"

    replay = await insert_flow_run(wf_conn, wf_sql, entry=entry, run_key=key)

    assert replay.created is False
    assert replay.kind == "existing-running"
    assert replay.flow_id == first.flow_id
    rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE idempotency_scope = $1 AND idempotency_key = $2",
        run_idempotency_scope(entry.name),
        key,
    )
    assert int(rows) == 1, "the replay never inserts a second root"
    kept = await wf_conn.fetchval(
        f'SELECT retry_kind FROM "{wf_schema}".jobs WHERE id = $1', first.flow_id
    )
    assert kept == "indefinite", "the replay leaves the legacy row's kind as it found it"


async def test_declared_indefinite_is_refused_whether_or_not_the_run_key_is_taken() -> None:
    """The refusal belongs to the DECLARATION: a fresh run key raises before
    any statement, and a taken run key raises just as firmly rather than
    silently adopting the existing run."""

    class _KeyedConn:
        """A run-key connection: a row may sit at the key, but a refused
        declaration must never read it or attempt the insert."""

        def __init__(self, wsql: WorkflowSql, row: dict[str, object] | None) -> None:
            self.wsql = wsql
            self.row = row
            self.read_attempted = False
            self.insert_attempted = False

        async def fetchrow(self, sql: str, *_args: Any, **_kwargs: Any) -> Any:
            if sql == self.wsql.flow_run_read:
                self.read_attempted = True
                return self.row
            if sql == self.wsql.flow_run_insert:
                self.insert_attempted = True
                raise AssertionError("a refused declaration reached the flow-root insert")
            raise AssertionError(f"unexpected statement on a refused declaration: {sql!r}")

    wsql = render_workflow_sql("taskq")
    entry = FlowEntryShim("wf_refused_root", {}, retry_kind="indefinite")

    for label, row in (
        ("fresh-key", None),
        (
            "taken-key",
            {"id": _FLOW_ID, "status": "running", "required_worker_schema": None},
        ),
    ):
        conn = _KeyedConn(wsql, row)
        with pytest.raises(WorkflowPolicyError) as exc_info:
            await insert_flow_run(
                cast("Any", conn),
                wsql,
                entry=entry,
                run_key=label,
            )
        _assert_e21_indefinite(exc_info, what="flow retry_kind")
        assert conn.read_attempted is False, "the refusal precedes any occupancy read"
        assert conn.insert_attempted is False


def test_canonical_retry_kind_preserves_legacy_indefinite_and_alias() -> None:
    assert canonical_retry_kind("indefinite") == "indefinite"
    assert canonical_retry_kind("permanent") == "non_retryable"
    assert canonical_retry_kind("transient") == "transient"
    assert canonical_retry_kind("non_retryable") == "non_retryable"

    with pytest.raises(WorkflowPolicyError) as exc_info:
        canonical_retry_kind("bogus")
    assert exc_info.value.code == "E21-execution-policy"


@pytest.mark.parametrize(
    ("declared", "expected"),
    [
        ("transient", "transient"),
        ("non_retryable", "non_retryable"),
        ("permanent", "non_retryable"),
    ],
)
def test_valid_finite_retry_kinds_remain_accepted(
    declared: FiniteRetryKind, expected: FiniteRetryKind
) -> None:
    app = WorkflowApp()

    @app.actor(retry_kind=declared)
    async def _actor(ctx: StepContext) -> str:
        return "ok"

    wf_name = f"wf_valid_{declared}"

    @app.workflow(wf_name)
    def _wf() -> Promise[object]:
        return build(step(_actor, key="s"))

    compiled = app.get(wf_name)
    expected_node_kind = None if declared == "transient" else expected
    assert compiled.nodes["s"].retry_kind == expected_node_kind

    child = ChildSpec(step_key="next", actor="wf", queue="default", payload={}, map_index=0)
    fork = chain_fork(child, trace_id="trace-1", retry_kind=declared)
    assert fork is not None
    assert fork.retry_kind == expected

    node = NodeSpec(
        flow_id=_FLOW_ID,
        step_key="s",
        actor="wf",
        queue="default",
        retry_kind=declared,
    )
    assert node.retry_kind == expected

    spec = RetrySpec(
        policy=RetryPolicy(kind=cast("Any", expected)),
        exceptions={ValueError: declared},
    )
    classified = spec.classify(ValueError("boom"), 1)
    assert classified is not None and classified.kind == expected

    wf_def = WorkflowDef(name=f"def_{declared}", bodies={}, retry_kind=declared)
    assert wf_def.retry_kind == expected


def test_reclaim_has_budget_sql_preserves_indefinite_arm() -> None:
    assert "j.retry_kind = 'indefinite'" in _RECLAIM_HAS_BUDGET_SQL
