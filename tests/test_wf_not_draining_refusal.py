"""The §5.4 non-drainage refusal control: the refusal fires ONLY on the
committed durable proof — a genuine child failure exhausted its declared
retry policy AND the root terminal ``failed`` committed, while an exact
parked source still holds an un-emitted committed cursor. Healthy
saturation, elapsed time, park counts, and nonterminal retries never
trigger it."""

from __future__ import annotations

import asyncio
import enum
import json as _json
from typing import Any

import pytest

from taskq.backend._protocol import JobId
from taskq.constants import WORKFLOW_NOT_DRAINING_CLASS
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


class Ok(enum.Enum):
    OK = "ok"


def _chain(name: str, body: Any, *, max_attempts: int = 3) -> Any:
    from datetime import timedelta

    from taskq.retry import RetryPolicy
    from taskq.workflows import DONE, Chain, Route, Step
    from taskq.workflows._policy import ExecutionPolicy, RetrySpec

    return Chain(
        name=name,
        start="screen",
        steps={
            "screen": Step(
                body=body,
                outcomes=Ok,
                route=Route({Ok.OK: DONE}),
                policy=ExecutionPolicy(
                    retry=RetrySpec(
                        policy=RetryPolicy(
                            max_attempts=max_attempts,
                            base=timedelta(seconds=0.01),
                            cap=timedelta(seconds=0.02),
                            jitter=0,
                        )
                    )
                ),
            )
        },
    )


async def test_exhausted_child_failure_with_parked_cursor_fences_the_source(
    wf_conn: Any,
    wf_schema: str,
    module_pg_pool: Any,
    wf_sql: WorkflowSql,
) -> None:
    """The positive proof: the child's genuine failure exhausts its declared
    retry policy, the root terminal ``failed`` commits, and the SAME
    transaction fences the parked source with the named durable class — no
    logical-attempt spend on the source, no ladder retry, the child's own
    error untouched."""
    from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, chain_source

    child_calls: list[int | None] = []

    async def screen(ctx: StepContext, item: object) -> Ok:
        child_calls.append(ctx.map_index)
        raise RuntimeError("the genuine child failure")

    chain = _chain(name=f"not-draining-{new_uuid_hex()}", body=screen, max_attempts=1)
    app = WorkflowApp()

    @app.workflow("not_draining_flow", max_in_flight=2)
    def flow() -> Promise[object]:
        async def source(ctx: StepContext) -> None:
            from taskq.workflows.chain import chain_start

            # THE CURSOR-DRIVEN PAGES (the honest author shape): the
            # resume continues from the last COMMITTED page. Page 0: two
            # children (they fit the bound exactly); page 1: two more —
            # 2 live + 2 > 2 refuses, and the source parks on the exact
            # marker (page_width=2). THE PARK STAYS UNFITTABLE while
            # either child lives (1 + 2 > 2), so no drain can resolve it
            # mid-estate; when the LAST child's genuine failure exhausts
            # its single declared attempt, the fail_closed join shape
            # downstream (the gather's own durable shape, seeded — the
            # chain+emit composition cannot express a cross-item fan-in)
            # makes the exhaustion's OWN cascade transaction the §5.4
            # seat: the gated root failure (no unresolved live work rests
            # — the parked source is resolved-blocked) commits the root
            # terminal ``failed`` and the SAME transaction fences the
            # still-parked source, before any post-commit drain could
            # look (its own intent_created bit is False: the park never
            # fit while work lived).
            cursor = await ctx.cursor()
            start_page = int(cursor.get("page", -1)) + 1
            for page in range(start_page, 2):
                await ctx.emit_batch(
                    [
                        chain_start(
                            chain,
                            {"app_id": page * 2 + offset},
                            map_index=page * 2 + offset,
                            trace_id=f"app-{page}-{offset}",
                        )
                        for offset in (0, 1)
                    ],
                    cursor={"page": page},
                )

        return build(chain_source(chain, source, key="source", max_attempts=2))

    compiled = app.get("not_draining_flow")
    runner = FlowRunner(compiled, module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    # Tick 1 (under the drive's own adoption): the source claims, emits
    # page 0, parks on page 1.
    await asyncio.wait_for(runner.drive(flow_id, max_ticks=1), timeout=60)
    parked = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
        WHERE (metadata->>'flow_id')::uuid = $1::uuid
          AND metadata @> '{{"blocking_reason": "backpressure"}}'""",
        flow_id,
    )
    assert int(parked) == 1, parked
    # THE FAIL_CLOSED JOIN SHAPE (the gather's own durable shape, seeded):
    # each page-0 child fans into its own pending join row — deps_pending=1
    # (the already-terminal sibling parent resolved, this child unresolved),
    # every edge fail_closed — so each child's exhaustion cascade finds
    # blocked >= 1 and reaches the gated root failure.
    sibling = new_uuid_hex()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, finished_at, metadata, fairness_key, "
        "idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'succeeded', 1, 1, 'transient', 'sibling', "
        "clock_timestamp(), $2::jsonb, $3, 'workflow-run', $4)",
        sibling,
        _json.dumps({"flow_id": str(flow_id)}),
        str(flow_id),
        f"wf:{flow_id}:sibling",
    )
    children = await wf_conn.fetch(
        f"""SELECT id FROM "{wf_schema}".jobs
        WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'screen'
        ORDER BY id""",
        flow_id,
    )
    assert len(children) == 2, [dict(r) for r in children]
    for row in children:
        join_id = new_uuid_hex()
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
            "max_attempts, retry_kind, step_key, deps_pending, metadata, fairness_key, "
            "idempotency_scope, idempotency_key) "
            "VALUES ($1, 'wf', 'default', '{}', 'pending', 0, 1, 'transient', "
            "'gather_join', 1, $2::jsonb, $3, 'workflow-run', $4)",
            join_id,
            _json.dumps({"flow_id": str(flow_id), "blocking_reason": "join"}),
            str(flow_id),
            f"wf:{flow_id}:gather:{join_id}",
        )
        for parent in (row["id"], sibling):
            await wf_conn.execute(
                f'INSERT INTO "{wf_schema}".wf_edge '
                "(child_id, parent_id, flow_id, failure_policy) "
                "VALUES ($1, $2, $3, 'fail_closed')",
                join_id,
                parent,
                flow_id,
            )
    await asyncio.wait_for(runner.drive(flow_id), timeout=60)

    source = await wf_conn.fetchrow(
        f"SELECT status::text AS status, attempt::int AS attempt, error_class, metadata "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        source_id,
    )
    assert source is not None
    assert source["status"] == "failed", source
    assert source["error_class"] == WORKFLOW_NOT_DRAINING_CLASS, source
    metadata = _meta(source["metadata"])
    refusal = metadata.get("wf_not_draining")
    assert isinstance(refusal, dict) and refusal.get("reason") == (
        "terminal_root_after_child_failure_exhaustion"
    ), metadata
    assert refusal.get("root_id") == str(flow_id), metadata
    assert refusal.get("child_id"), metadata
    # The refusal spent no source logical attempt (the parked ordinal stands).
    assert int(source["attempt"]) == 0, source
    # The child's genuine error is untouched.
    child_ledger = await wf_conn.fetchval(
        f'SELECT error_class FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'screen' AND status = 'failed' "
        "ORDER BY updated_at DESC LIMIT 1",
        flow_id,
    )
    assert child_ledger == "RuntimeError", child_ledger
    # The refusal is durable on the event stream.
    event = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".job_events WHERE job_id = $1 '
        "AND detail->>'error_class' = $2",
        source_id,
        WORKFLOW_NOT_DRAINING_CLASS,
    )
    assert int(event) >= 1, "the refusal's state_change event is on the record"
    # No further claim: the fenced row is terminal.
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root == "failed", root


async def test_healthy_saturation_and_nonterminal_retries_never_refuse(
    wf_conn: Any,
    wf_schema: str,
    module_pg_pool: Any,
    wf_sql: WorkflowSql,
) -> None:
    """The opposite directions: a healthy parked source under a LIVE root
    (however long the wait, however many parks), and a child's NONTERMINAL
    retry, never produce the refusal — the proof condition is the committed
    terminal root behind an EXHAUSTED child failure, never a wait count or
    elapsed time."""
    from taskq._ids import new_uuid

    flow = JobId(new_uuid())
    source = JobId(new_uuid())
    child = JobId(new_uuid())
    # The live root.
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key) "
        "VALUES ($1, 'wf-node', 'default', '{}', 'running', 3, 'transient', '__flow__', "
        "$2::jsonb, $3)",
        flow,
        _json.dumps({"flow_id": str(flow), "workflow": "healthy_flow"}),
        str(flow),
    )
    # The parked source: the exact marker + the park instant + the
    # un-emitted committed cursor, ledger `awaited` — the shape a healthy
    # saturation parks into.
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key, attempt, scheduled_at) "
        "VALUES ($1, 'wf-node', 'default', '{}', 'scheduled', 2, 'transient', 'source', "
        "$2::jsonb, $3, 0, clock_timestamp())",
        source,
        _json.dumps(
            {
                "flow_id": str(flow),
                "blocking_reason": "backpressure",
                "emit_cursor": {"page": 0},
                "wf_deferral": {
                    "signal": "EmitBackpressure",
                    "consume_budget": False,
                    "attempt": 0,
                },
                "wf_emit_backpressure": {
                    "version": 1,
                    "page_width": 2,
                    "bound": 2,
                    "outstanding": 2,
                    "deferrals": 3,
                    "wakes": 0,
                },
                "wf_source_logical_attempt": 1,
            }
        ),
        str(flow),
    )
    # A NONTERMINAL child retry: a failed attempt ledger row at an
    # unchanged-attempt re-pend — genuine ladder history, never exhaustion.
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key, attempt) "
        "VALUES ($1, 'wf-node', 'default', '{}', 'pending', 3, 'transient', 'screen', "
        "$2::jsonb, $3, 1)",
        child,
        _json.dumps({"flow_id": str(flow)}),
        str(flow),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status, "
        "error_class, error_message) "
        "VALUES ($1, $2, $3, 'screen', NULL, 1, 1, 'failed', 'RuntimeError', "
        "'the transient failure the ladder re-pends')",
        new_uuid(),
        flow,
        child,
    )
    # Wait well past any plausible count/elapsed trigger; the maintain leg's
    # failure arm runs (it would fence the parked source if the root were
    # terminal behind an exhausted failure — it is not).
    for _ in range(6):
        await module_pg_pool.execute("SELECT 1")
        await asyncio.sleep(0.5)
        from taskq.workflows._sweep import sweep_join_rederive

        await sweep_join_rederive(module_pg_pool, wf_sql, batch_size=10)
    state = await wf_conn.fetchrow(
        f'SELECT status::text AS status, error_class, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        source,
    )
    assert state is not None
    assert state["status"] == "scheduled", state
    assert state["error_class"] is None, state
    assert "wf_not_draining" not in _meta(state["metadata"]), state
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow
    )
    assert root == "running", "a healthy parked run never derives not-draining"


def _meta(raw: object) -> dict[str, Any]:
    decoded = _json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


def new_uuid_hex() -> str:
    from taskq._ids import new_uuid

    return new_uuid().hex


async def _source_id(conn: Any, schema: str, flow_id: JobId, key: str = "source") -> JobId:
    value = await conn.fetchval(
        f'SELECT id FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2 "
        "AND step_key <> '__flow__' LIMIT 1",
        flow_id,
        key,
    )
    assert value is not None
    return JobId(value)
