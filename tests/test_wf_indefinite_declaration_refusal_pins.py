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

from __future__ import annotations

import enum
from typing import Any, cast

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
    chain_start,
    loop,
    map_source,
    route,
    step,
)
from taskq.workflows._emit import _emit_tx, emit_batch
from taskq.workflows._policy import canonical_retry_kind
from taskq.workflows._sweep import _drain_outbox_once, drain_outbox
from taskq.workflows._types import ChildSpec, EmitChild, ForkSpec, NodeSpec
from taskq.workflows.api._graph import NodeDecl
from taskq.workflows.api._runner import _node_retry_spec
from taskq.workflows.api._runner_codec import FlowEntryShim
from taskq.workflows.definitions import WorkflowDef
from taskq.workflows.engine import render_workflow_sql
from taskq.workflows.ledger import insert_flow_run

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


def _assert_e21_indefinite(exc_info: pytest.ExceptionInfo[WorkflowPolicyError]) -> None:
    assert exc_info.value.code == "E21-execution-policy"
    assert "E21-execution-policy" in str(exc_info.value)
    assert "indefinite" in str(exc_info.value)


def test_step_direct_refuses_indefinite_retry_kind() -> None:
    app = WorkflowApp()

    @app.workflow("wf_step_direct_indefinite")
    def _wf() -> Promise[object]:
        return build(step(_plain_body, key="s", retry_kind="indefinite"))

    with pytest.raises(WorkflowPolicyError) as exc_info:
        app.get("wf_step_direct_indefinite")
    _assert_e21_indefinite(exc_info)


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
    _assert_e21_indefinite(exc_info)


def test_workflow_actor_declaration_refuses_indefinite_retry_kind() -> None:
    app = WorkflowApp()

    with pytest.raises(WorkflowPolicyError) as exc_info:

        @app.actor(retry_kind="indefinite")
        async def _indefinite_actor(ctx: StepContext) -> str:
            return "ok"

    _assert_e21_indefinite(exc_info)


def test_step_wired_with_indefinite_actor_handle_refuses_indefinite() -> None:
    app = WorkflowApp()

    @app.actor(retry_kind="transient")
    async def _handle(ctx: StepContext) -> str:
        return "ok"

    # Exercise step()'s own handle-copy validation path even when @app.actor
    # rejects "indefinite" earlier.
    _handle.retry_kind = "indefinite"

    @app.workflow("wf_step_wired_actor_indefinite")
    def _wf() -> Promise[object]:
        return build(step(_handle, key="s"))

    with pytest.raises(WorkflowPolicyError) as exc_info:
        app.get("wf_step_wired_actor_indefinite")
    _assert_e21_indefinite(exc_info)


@pytest.mark.parametrize("entrypoint", ["step", "map_source", "route", "loop", "chain"])
def test_actor_handle_wiring_entrypoints_refuse_indefinite_actor(entrypoint: str) -> None:
    app = WorkflowApp()

    @app.actor(retry_kind="transient")
    async def _actor_body(ctx: StepContext, item: _Item) -> str:
        return str(item.value)

    # Bypass the decorator's own door so each wiring entrypoint is actually
    # exercised; raising during @app.actor would leave all five paths untested.
    _actor_body.retry_kind = "indefinite"

    with pytest.raises(WorkflowPolicyError) as exc_info:
        if entrypoint == "step":

            @app.workflow("wf_actor_step")
            def _wf_step() -> Promise[object]:
                return build(step(_actor_body, _Item(value=1), key="s"))

            app.get("wf_actor_step")
        elif entrypoint == "map_source":

            @app.workflow("wf_actor_map")
            def _wf_map() -> Promise[object]:
                src = step(_list_body, key="src")
                return build(map_source(src, _actor_body))

            app.get("wf_actor_map")
        elif entrypoint == "route":

            @app.workflow("wf_actor_route")
            def _wf_route() -> Promise[object]:
                src = step(_list_body, key="src")
                return build(route(src, {_Item: RouteArm(body=_actor_body)}))

            app.get("wf_actor_route")
        elif entrypoint == "loop":

            @app.workflow("wf_actor_loop")
            def _wf_loop() -> Promise[object]:
                return build(loop("lp", _actor_body, max_iterations=2))

            app.get("wf_actor_loop")
        else:
            chain = Chain(
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
            chain_start(chain, _Item(value=1), map_index=0, trace_id="trace-1")

    _assert_e21_indefinite(exc_info)


def test_chain_declaration_refuses_indefinite_actor_handle() -> None:
    app = WorkflowApp()

    @app.actor(retry_kind="transient")
    async def _actor_body(ctx: StepContext, item: _Item) -> str:
        return str(item.value)

    _actor_body.retry_kind = "indefinite"

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
    _assert_e21_indefinite(exc_info)


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
    _actor_body.retry_kind = "indefinite"

    async def _source(ctx: StepContext) -> None:
        return None

    @app.workflow("wf_chain_source_mutated_actor")
    def _wf() -> Promise[object]:
        return build(chain_source(chain, _source, key="source"))

    with pytest.raises(WorkflowPolicyError) as exc_info:
        app.get("wf_chain_source_mutated_actor")
    _assert_e21_indefinite(exc_info)


@pytest.mark.parametrize("with_child", [True, False])
def test_chain_fork_refuses_indefinite_retry_kind(with_child: bool) -> None:
    child = (
        ChildSpec(step_key="next", actor="wf", queue="default", payload={}, map_index=0)
        if with_child
        else None
    )

    with pytest.raises(WorkflowPolicyError) as exc_info:
        chain_fork(child, trace_id="trace-1", retry_kind=_INDEFINITE)
    _assert_e21_indefinite(exc_info)


def test_fork_spec_refuses_indefinite_retry_kind() -> None:
    child = ChildSpec(step_key="next", actor="wf", queue="default", payload={}, map_index=0)

    with pytest.raises(WorkflowPolicyError) as exc_info:
        ForkSpec(children=(child,), retry_kind=_INDEFINITE)
    _assert_e21_indefinite(exc_info)


def test_node_spec_refuses_indefinite_retry_kind() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        NodeSpec(
            flow_id=_FLOW_ID,
            step_key="s",
            actor="wf",
            queue="default",
            retry_kind=_INDEFINITE,
        )
    _assert_e21_indefinite(exc_info)


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
    _assert_e21_indefinite(exc_info)


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
    _assert_e21_indefinite(exc_info)


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
    _assert_e21_indefinite(exc_info)


def test_dynamic_classifier_cannot_reintroduce_indefinite() -> None:
    spec = RetrySpec(
        classifier=lambda _exc, _attempt: RetryOverride(kind="indefinite"),
        non_consuming_limit=1,
    )

    with structlog.testing.capture_logs() as logs:
        assert spec.classify(ValueError("boom"), 1) is None

    assert [entry["event"] for entry in logs] == ["workflow-retry-classifier-indefinite-refused"]


class _UnreachablePool:
    """Guard proving door validation rejects before any pool acquisition."""

    def acquire(self) -> Any:
        raise AssertionError("reached pool.acquire() without refusing retry_kind='indefinite'")


class _UnreachableConn:
    def fetchval(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(
            "reached the emit cursor write without refusing retry_kind='indefinite'"
        )

    async def fetchrow(self, sql: str, *_args: Any, **_kwargs: Any) -> Any:
        if sql.strip().startswith("SELECT id, status::text"):
            return None
        raise AssertionError(
            "reached the flow root insert without refusing retry_kind='indefinite'"
        )


class _LegacyFlowConn:
    def __init__(self, wsql: Any) -> None:
        self.wsql = wsql
        self.insert_attempted = False

    async def fetchrow(self, sql: str, *_args: Any, **_kwargs: Any) -> Any:
        if sql == self.wsql.flow_run_read:
            return {
                "id": _FLOW_ID,
                "status": "running",
                "required_worker_schema": None,
            }
        self.insert_attempted = True
        raise AssertionError("a legacy run-key replay attempted a new root insert")


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
    _assert_e21_indefinite(exc_info)


async def test_emit_tx_refuses_indefinite_retry_kind_at_its_own_door() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        await _emit_tx(
            cast("Any", _UnreachableConn()),
            render_workflow_sql("taskq"),
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
    _assert_e21_indefinite(exc_info)


async def test_drain_outbox_refuses_indefinite_default_retry_kind_at_door() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        await drain_outbox(
            cast("Any", _UnreachablePool()),
            render_workflow_sql("taskq"),
            default_retry_kind="indefinite",
        )
    _assert_e21_indefinite(exc_info)


async def test_drain_outbox_once_refuses_indefinite_default_retry_kind_at_its_own_door() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        await _drain_outbox_once(
            cast("Any", _UnreachablePool()),
            render_workflow_sql("taskq"),
            default_retry_kind="indefinite",
        )
    _assert_e21_indefinite(exc_info)


def test_workflow_def_refuses_indefinite_retry_kind() -> None:
    with pytest.raises(WorkflowPolicyError) as exc_info:
        WorkflowDef(name="wf_def_indefinite", bodies={}, retry_kind="indefinite")
    _assert_e21_indefinite(exc_info)


async def test_insert_flow_run_refuses_indefinite_retry_kind_at_its_own_door() -> None:
    entry = FlowEntryShim("wf_root_indefinite", {}, retry_kind="indefinite")

    with pytest.raises(WorkflowPolicyError) as exc_info:
        await insert_flow_run(
            cast("Any", _UnreachableConn()),
            render_workflow_sql("taskq"),
            entry=entry,
            run_key="run-1",
        )
    _assert_e21_indefinite(exc_info)


async def test_insert_flow_run_adopts_a_legacy_indefinite_run_without_inserting() -> None:
    wsql = render_workflow_sql("taskq")
    conn = _LegacyFlowConn(wsql)
    entry = FlowEntryShim("wf_root_indefinite", {}, retry_kind="indefinite")

    claim = await insert_flow_run(
        cast("Any", conn),
        wsql,
        entry=entry,
        run_key="run-1",
    )

    assert claim.created is False
    assert claim.kind == "existing-running"
    assert claim.flow_id == _FLOW_ID
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
def test_valid_finite_retry_kinds_remain_accepted(declared: str, expected: str) -> None:
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
    fork = chain_fork(child, trace_id="trace-1", retry_kind=cast("Any", declared))
    assert fork is not None
    assert fork.retry_kind == expected

    node = NodeSpec(
        flow_id=_FLOW_ID,
        step_key="s",
        actor="wf",
        queue="default",
        retry_kind=cast("Any", declared),
    )
    assert node.retry_kind == expected

    spec = RetrySpec(
        policy=RetryPolicy(kind=cast("Any", expected)),
        exceptions={ValueError: cast("Any", declared)},
    )
    classified = spec.classify(ValueError("boom"), 1)
    assert classified is not None and classified.kind == expected

    wf_def = WorkflowDef(name=f"def_{declared}", bodies={}, retry_kind=declared)
    assert wf_def.retry_kind == expected


def test_reclaim_has_budget_sql_preserves_indefinite_arm() -> None:
    assert "j.retry_kind = 'indefinite'" in _RECLAIM_HAS_BUDGET_SQL
