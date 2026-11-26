# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""Real-Postgres pins for the missing-root workflow dispatch fence,
retention prune root-hold guard, and execution-door root-missing
classification.

Covers:
1. Fleet dispatch fences (``_WF_DISPATCH_FENCE_TEMPLATE`` and
   ``_WF_PROBE_FENCE_TEMPLATE`` across strict-FIFO, round-robin, plain
   probe, and cursor probe): a workflow row whose ``__flow__`` root is
   absent from ``jobs`` must be refused, while vanilla jobs, live
   workflow rows, and terminal-run escalation / after-terminal effect
   rows remain claimable.
2. In-process claim fences (``CLAIMABLE_NODES_SQL_TEMPLATE`` and
   ``NODE_CLAIM_SQL_TEMPLATE`` via ``FlowRunner.tick``): a workflow row
   whose ``__flow__`` root is absent must neither enumerate as claimable
   nor execute.
3. Retention prune guard (``_WORKFLOW_LIVENESS_GUARD_SQL`` on both the
   fleet-wide and per-actor candidate windows): a finished ``__flow__``
   root past retention must not be archived while any non-terminal child
   row of that ``flow_id`` or undelivered outbox intent still exists; once
   all children are terminal and intents drained, the root prunes normally.
4. Worker execution door (``execute_flow_job``): a claimed workflow row
   whose ``__flow__`` root does not exist raises
   ``WorkflowRootMissingError`` rather than misclassifying the missing
   root as ``WorkflowBodyUnresolvableError`` ("stamps no workflow name").
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq.backend._dispatch_sql import (
    DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL,
    DISPATCH_CLAIMABLE_PROBE_SQL,
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_CURSOR_SQL,
    DISPATCH_STRICT_FIFO_SQL,
    dispatch_batch,
)
from taskq.backend._protocol import JobId, JobRow
from taskq.backend._sql_templates import render
from taskq.constants import WF_LOOP_ESCALATION_STEP_KEY
from taskq.worker._leader_shared import (
    _ARCHIVE_CANDIDATE_SQL,
    _ARCHIVE_CTE_SQL,
    prune_terminal_jobs,
)
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import drain_outbox
from taskq.workflows._worker_execution import (
    WorkflowBodyUnresolvableError,
    execute_flow_job,
)
from taskq.workflows.api._runner_errors import WorkflowRootMissingError
from taskq.workflows.api._sql_runner import (
    CLAIMABLE_NODES_SQL_TEMPLATE,
    NODE_CLAIM_SQL_TEMPLATE,
    render_sql,
)
from tests._wf_fixtures import seed_flow

pytestmark = pytest.mark.integration

_IN_PROCESS_SENTINEL: list[str] = []


async def _orphan_step_body(ctx: StepContext) -> str:
    del ctx
    _IN_PROCESS_SENTINEL.append("ran")
    return "ran"


async def _register_capable_worker(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queues: list[str] | None = None,
) -> uuid.UUID:
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'wf-host', 1, $2::text[], $3::jsonb)",
        worker_id,
        queues or ["default", "orphan_q"],
        json.dumps({"workflow_execution": True}),
    )
    return worker_id


async def _probe_counts(conn: asyncpg.Connection, schema: str, queue: str) -> tuple[int, int]:
    """(plain probe rows, cursor probe rows) for one isolated queue — every
    case below owns its own queue so no co-resident row can mask a count."""
    plain = await conn.fetch(DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=schema), [queue])
    cursor = await conn.fetch(
        DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL.format(schema=schema),
        [queue],
        uuid.UUID(int=0),
    )
    return len(plain), len(cursor)


async def _register_actor(conn: asyncpg.Connection, schema: str, *, actor: str, queue: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        actor,
        queue,
    )


async def _seed_wf_child(
    conn: asyncpg.Connection,
    schema: str,
    *,
    flow_id: uuid.UUID,
    step_key: str,
    actor: str = "wf_actor",
    queue: str = "default",
    status: str = "pending",
    extra_meta: dict[str, object] | None = None,
) -> uuid.UUID:
    job_id = new_uuid()
    meta: dict[str, object] = {"flow_id": str(flow_id)}
    if extra_meta:
        meta.update(extra_meta)
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata, fairness_key, scheduled_at) "
        "VALUES ($1, $2, $3, '{}', 3, 'transient', $4, $5, 0, $6::jsonb, $7, "
        "now() - interval '1 hour')",
        job_id,
        actor,
        queue,
        status,
        step_key,
        json.dumps(meta),
        str(flow_id),
    )
    return job_id


def _claimed_job_row(job_id: JobId, *, flow_id: uuid.UUID) -> JobRow:
    return JobRow(
        id=job_id,
        actor="wf_actor",
        queue="default",
        identity_key=None,
        fairness_key=str(flow_id),
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        metadata={"flow_id": str(flow_id)},
        claim_epoch=1,
    )


async def test_fleet_dispatch_and_probe_fence_refuse_workflow_row_with_missing_root(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Fleet dispatch (strict FIFO + round robin) and both claimable probes
    (plain + cursor) must refuse a pending workflow row whose ``__flow__``
    root is missing from ``jobs``, while still admitting vanilla jobs,
    live workflow rows, and terminal-run escalation / after-terminal
    effect rows."""
    worker_id = await _register_capable_worker(wf_conn, wf_schema)
    await _register_actor(wf_conn, wf_schema, actor="orphan_actor", queue="orphan_q")
    await _register_actor(wf_conn, wf_schema, actor="vanilla_actor", queue="default")
    await _register_actor(wf_conn, wf_schema, actor="wf_actor", queue="default")

    # 1. Orphaned pending workflow row on isolated queue `orphan_q`: its
    #    finished root has been archived/deleted from `jobs`.
    missing_flow_id = new_uuid()
    orphan_id = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=missing_flow_id,
        step_key="leftover_join",
        actor="orphan_actor",
        queue="orphan_q",
    )

    plain_probe = await wf_conn.fetch(
        DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=wf_schema),
        ["orphan_q"],
    )
    cursor_probe = await wf_conn.fetch(
        DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL.format(schema=wf_schema),
        ["orphan_q"],
        uuid.UUID(int=0),
    )
    assert len(plain_probe) == 0, (
        "plain claimable probe admitted a workflow row whose __flow__ root is missing"
    )
    assert len(cursor_probe) == 0, (
        "cursor claimable probe admitted a workflow row whose __flow__ root is missing"
    )

    fifo_claimed = await dispatch_batch(
        wf_pool,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["orphan_q"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert [r["id"] for r in fifo_claimed] == [], (
        f"strict-FIFO dispatch claimed rootless workflow row {orphan_id}"
    )

    rr_claimed = await dispatch_batch(
        wf_pool,
        sql=DISPATCH_ROUND_ROBIN_SQL.format(schema=wf_schema),
        queues=["orphan_q"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert [r["id"] for r in rr_claimed] == [], (
        f"round-robin dispatch claimed rootless workflow row {orphan_id}"
    )

    # 2. Positive admissions and terminal-run fencing on `default`:
    #    (a) vanilla job (step_key IS NULL)
    vanilla_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, metadata, scheduled_at) "
        "VALUES ($1, 'vanilla_actor', 'default', '{}', 3, 'transient', 'pending', "
        "'{}'::jsonb, now() - interval '1 hour')",
        vanilla_id,
    )
    #    (b) live workflow child
    live_flow = await seed_flow(wf_conn, wf_schema, status="running")
    live_child_id = await _seed_wf_child(
        wf_conn, wf_schema, flow_id=live_flow, step_key="live_step"
    )
    #    (c) escalation step on a terminal (failed) flow
    failed_flow = await seed_flow(wf_conn, wf_schema, status="failed")
    escalation_id = await _seed_wf_child(
        wf_conn, wf_schema, flow_id=failed_flow, step_key=WF_LOOP_ESCALATION_STEP_KEY
    )
    #    (d) after-terminal effect on a terminal (cancelled) flow
    cancelled_flow = await seed_flow(wf_conn, wf_schema, status="cancelled")
    effect_id = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=cancelled_flow,
        step_key="work.__on_exhaustion",
        extra_meta={"wf_after_terminal_effect": True},
    )
    #    (e) normal workflow child on a terminal (failed) flow — must stay
    #        fenced. A `failed` root can legitimately carry live rows (the
    #        failure's precedence derives over a fired-but-unclaimed join);
    #        a `succeeded` root cannot (the suite's always-on G7 status-truth
    #        fixture rejects a complete root over live rows), so this case
    #        seeds the terminal status the state machine actually produces.
    failed_flow = await seed_flow(wf_conn, wf_schema, status="failed")
    terminal_child_id = await _seed_wf_child(
        wf_conn, wf_schema, flow_id=failed_flow, step_key="stale_step"
    )

    default_probe = await wf_conn.fetch(
        DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=wf_schema),
        ["default"],
    )
    assert len(default_probe) == 1

    admitted = await dispatch_batch(
        wf_pool,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=10,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    admitted_ids = {r["id"] for r in admitted}
    assert admitted_ids == {vanilla_id, live_child_id, escalation_id, effect_id}, (
        f"unexpected admitted set: got {admitted_ids}, terminal_child_id={terminal_child_id}"
    )


async def test_claimable_probes_admit_exempt_kinds_only_with_their_root(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Per-queue, isolated probe parity (no co-resident vanilla row can mask
    the count): each probe admits the escalation consumer and the
    after-terminal effect row on a TERMINAL root that is present, refuses an
    ordinary step on a terminal root, and refuses both exempt kinds once
    their root is gone. Every case owns a distinct queue, and each dispatch
    variant claims from its own queue so neither sees the other's claim."""
    # (label, root status (None = no root row at all), step_key, extra metadata, admitted)
    cases = (
        ("escalation-terminal", "failed", WF_LOOP_ESCALATION_STEP_KEY, None, 1),
        (
            "effect-terminal",
            "cancelled",
            "work.__on_exhaustion",
            {"wf_after_terminal_effect": True},
            1,
        ),
        ("ordinary-terminal", "failed", "stale_step", None, 0),
        ("escalation-rootless", None, WF_LOOP_ESCALATION_STEP_KEY, None, 0),
        (
            "effect-rootless",
            None,
            "work.__on_exhaustion",
            {"wf_after_terminal_effect": True},
            0,
        ),
    )
    variants = ("fifo", "rr")
    worker_id = await _register_capable_worker(
        wf_conn,
        wf_schema,
        queues=["default"] + [f"{label}-{variant}" for label, *_ in cases for variant in variants],
    )
    seeded: list[tuple[str, object, int]] = []
    for label, root_status, step_key, extra_meta, admitted in cases:
        for variant in variants:
            queue = f"{label}-{variant}"
            actor = f"{queue}-actor"
            await _register_actor(wf_conn, wf_schema, actor=actor, queue=queue)
            flow_id = (
                await seed_flow(wf_conn, wf_schema, status=root_status)
                if root_status is not None
                else new_uuid()
            )
            node_id = await _seed_wf_child(
                wf_conn,
                wf_schema,
                flow_id=flow_id,
                step_key=step_key,
                actor=actor,
                queue=queue,
                extra_meta=extra_meta,
            )
            seeded.append((queue, node_id, admitted))

    for queue, node_id, admitted in seeded:
        expected = (admitted, admitted)
        assert await _probe_counts(wf_conn, wf_schema, queue) == expected, (
            f"{queue}: both probes must answer {admitted} for node {node_id}"
        )

    for queue, node_id, admitted in seeded:
        sql = DISPATCH_ROUND_ROBIN_SQL if queue.endswith("-rr") else DISPATCH_STRICT_FIFO_SQL
        claimed = await dispatch_batch(
            wf_pool,
            sql=sql.format(schema=wf_schema),
            queues=[queue],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )
        assert [r["id"] for r in claimed] == ([node_id] if admitted else []), (
            f"{queue}: dispatch disagreed with its probe about node {node_id}"
        )


async def test_repended_rootless_rows_stay_fenced_on_every_dispatch_render(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The snooze/release half of the original loop: a released workflow row
    comes back ``assignment_routed`` and must still be refused by the
    assignment-routed arm, including the strict-FIFO cursor render. A live
    re-pended workflow row on the same queue is the positive control."""
    variants = (
        ("repended-fifo", DISPATCH_STRICT_FIFO_SQL),
        ("repended-cursor", DISPATCH_STRICT_FIFO_CURSOR_SQL),
        ("repended-rr", DISPATCH_ROUND_ROBIN_SQL),
    )
    worker_id = await _register_capable_worker(
        wf_conn, wf_schema, queues=[queue for queue, _sql in variants]
    )

    for queue, statement in variants:
        actor = f"{queue}-actor"
        await _register_actor(wf_conn, wf_schema, actor=actor, queue=queue)
        rootless_id = await _seed_wf_child(
            wf_conn,
            wf_schema,
            flow_id=new_uuid(),
            step_key="released_orphan",
            actor=actor,
            queue=queue,
        )
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET assignment_routed = true WHERE id = $1',
            rootless_id,
        )

        assert await _probe_counts(wf_conn, wf_schema, queue) == (0, 0)
        claimed = await dispatch_batch(
            wf_pool,
            sql=statement.format(schema=wf_schema),
            queues=[queue],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
            claim_cursor=uuid.UUID(int=0) if queue == "repended-cursor" else None,
        )
        assert claimed == [], f"{queue} re-claimed rootless re-pended row {rootless_id}"

        live_flow = await seed_flow(wf_conn, wf_schema, status="running")
        live_id = await _seed_wf_child(
            wf_conn,
            wf_schema,
            flow_id=live_flow,
            step_key="live_repend",
            actor=actor,
            queue=queue,
        )
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET assignment_routed = true WHERE id = $1',
            live_id,
        )
        assert await _probe_counts(wf_conn, wf_schema, queue) == (1, 1)
        claimed = await dispatch_batch(
            wf_pool,
            sql=statement.format(schema=wf_schema),
            queues=[queue],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
            claim_cursor=uuid.UUID(int=0) if queue == "repended-cursor" else None,
        )
        assert [row["id"] for row in claimed] == [live_id]
        assert (
            await wf_conn.fetchval(
                f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', rootless_id
            )
            == "pending"
        )


async def test_in_process_claim_fence_refuses_workflow_row_with_missing_root(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The in-process claim door (``CLAIMABLE_NODES_SQL_TEMPLATE`` and
    ``NODE_CLAIM_SQL_TEMPLATE``) must agree with fleet dispatch: a pending
    workflow row whose ``__flow__`` root is missing must neither appear
    claimable nor be claimed/executed by ``FlowRunner.tick``."""
    _IN_PROCESS_SENTINEL.clear()
    app = WorkflowApp()

    @app.workflow("missing_root_inproc_flow")
    def _flow() -> Promise[object]:
        return build(step(_orphan_step_body, key="only"))

    runner = FlowRunner(app.get("missing_root_inproc_flow"), wf_pool, wf_schema)
    missing_flow_id = new_uuid()
    node_id = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=missing_flow_id,
        step_key="only",
        actor="wf",
    )

    claimable = await wf_conn.fetch(
        render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema),
        missing_flow_id,
    )
    assert claimable == [], (
        f"CLAIMABLE_NODES_SQL_TEMPLATE enumerated rootless workflow row {node_id}"
    )

    direct_claim = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema),
        node_id,
        new_uuid(),
    )
    assert direct_claim is None, f"NODE_CLAIM_SQL_TEMPLATE claimed rootless workflow row {node_id}"

    await runner.tick(JobId(missing_flow_id))
    assert _IN_PROCESS_SENTINEL == [], "FlowRunner.tick executed a rootless workflow row"


async def test_in_process_claim_admits_exempt_kinds_only_with_their_root(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The in-process door's own exemption parity: both of its statements
    admit the escalation consumer and the after-terminal effect row on a
    present terminal root, and refuse both once that root is gone."""
    failed_flow = await seed_flow(wf_conn, wf_schema, status="failed")
    escalation_id = await _seed_wf_child(
        wf_conn, wf_schema, flow_id=failed_flow, step_key=WF_LOOP_ESCALATION_STEP_KEY
    )
    cancelled_flow = await seed_flow(wf_conn, wf_schema, status="cancelled")
    effect_id = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=cancelled_flow,
        step_key="work.__on_exhaustion",
        extra_meta={"wf_after_terminal_effect": True},
    )

    rootless_flow = new_uuid()
    rootless_escalation_id = await _seed_wf_child(
        wf_conn, wf_schema, flow_id=rootless_flow, step_key=WF_LOOP_ESCALATION_STEP_KEY
    )
    rootless_effect_id = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=rootless_flow,
        step_key="work.__on_exhaustion2",
        extra_meta={"wf_after_terminal_effect": True},
    )

    claimable_escalation = await wf_conn.fetch(
        render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema), failed_flow
    )
    assert [row["id"] for row in claimable_escalation] == [escalation_id], (
        "the claimable door must enumerate the escalation consumer on a present terminal root"
    )
    claimable_effect = await wf_conn.fetch(
        render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema), cancelled_flow
    )
    assert [row["id"] for row in claimable_effect] == [effect_id], (
        "the claimable door must enumerate the after-terminal effect on a present terminal root"
    )
    assert (
        await wf_conn.fetch(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema), rootless_flow)
        == []
    ), "the claimable door enumerated rootless exempt rows"

    for job_id in (escalation_id, effect_id):
        assert (
            await wf_conn.fetchrow(
                render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), job_id, new_uuid()
            )
            is not None
        ), f"the claim CAS must claim exempt node {job_id} on a present terminal root"
    for job_id in (rootless_escalation_id, rootless_effect_id):
        assert (
            await wf_conn.fetchrow(
                render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), job_id, new_uuid()
            )
            is None
        ), f"the claim CAS claimed rootless exempt node {job_id}"


async def test_retention_prune_holds_finished_root_with_non_terminal_child(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Retention pruning must hold back a finished ``__flow__`` root past
    retention while any non-terminal child row of that ``flow_id`` remains
    in ``jobs`` (both fleet-wide and per-actor override windows), and must
    prune the root normally once no non-terminal children remain."""
    # 1. Finished root past retention WITH a stray pending child (fleet-wide prune).
    held_flow = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs '
        "SET finished_at = now() - interval '40 days', actor = 'flow_root' "
        "WHERE id = $1",
        held_flow,
    )
    stray_child = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=held_flow,
        step_key="stray_join",
        status="pending",
    )

    # 2. Finished root past per-actor override retention WITH a scheduled child.
    override_flow = await seed_flow(wf_conn, wf_schema, status="failed")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs '
        "SET finished_at = now() - interval '5 days', actor = 'override_root' "
        "WHERE id = $1",
        override_flow,
    )
    await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=override_flow,
        step_key="delayed_effect",
        status="scheduled",
    )

    # 3. Cleanly finished root past retention whose child is ALSO terminal — must prune.
    clean_flow = await seed_flow(wf_conn, wf_schema, status="succeeded")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs '
        "SET finished_at = now() - interval '40 days', actor = 'flow_root' "
        "WHERE id = $1",
        clean_flow,
    )
    clean_child = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=clean_flow,
        step_key="done_step",
        status="succeeded",
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET finished_at = now() - interval '40 days' WHERE id = $1",
        clean_child,
    )

    async with wf_pool.acquire() as prune_conn:
        await prune_terminal_jobs(
            prune_conn,
            retention_per_status={
                "succeeded": timedelta(days=30),
                "failed": timedelta(days=30),
            },
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=wf_schema,
            actor_overrides={"override_root": timedelta(days=1)},
        )

    assert (
        await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', held_flow)
        == 1
    ), "fleet-wide prune archived a finished root while a pending child still existed in jobs"
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', override_flow
        )
        == 1
    ), "per-actor prune archived a finished root while a scheduled child still existed in jobs"
    assert (
        await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', clean_flow)
        == 0
    ), "cleanly finished root with only terminal children must prune on schedule"

    # Once the stray child becomes terminal, the previously held root prunes cleanly.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'cancelled', finished_at = now() WHERE id = $1",
        stray_child,
    )
    async with wf_pool.acquire() as prune_conn2:
        await prune_terminal_jobs(
            prune_conn2,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=wf_schema,
        )
    assert (
        await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', held_flow)
        == 0
    ), "finished root must prune once its last non-terminal child has terminalized"


@pytest.mark.parametrize(
    ("consumer_step_key", "terminal_effect"),
    [("work.__on_exhaustion", True), (WF_LOOP_ESCALATION_STEP_KEY, False)],
    ids=["terminal-effect", "loop-escalation"],
)
async def test_retention_preserves_an_undelivered_post_terminal_intent(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    consumer_step_key: str,
    terminal_effect: bool,
) -> None:
    """A post-terminal obligation is durable before its consumer job exists.
    Retention must hold the root while that intent is undelivered and across
    the drain's materialization handoff; otherwise the drain's root join
    inserts nothing but its delivered flip silently consumes the intent."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="failed", workflow="effect_flow")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs '
        "SET finished_at = now() - interval '40 days', actor = 'flow_root' WHERE id = $1",
        flow_id,
    )
    failed_node = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        step_key="work",
        status="failed",
    )
    binding: dict[str, object] = {
        "step_key": consumer_step_key,
        "actor": "wf",
        "queue": "default",
        "payload": {},
        "map_index": None,
    }
    if terminal_effect:
        binding["wf_effect_key"] = f"{flow_id}:1:{failed_node}:on_exhaustion"
        binding["after_terminal"] = True
    await wf_conn.execute(
        wf_sql.outbox_insert,
        new_uuid(),
        failed_node,
        flow_id,
        consumer_step_key,
        None,
        dumps_jsonb_str(binding),
    )

    async with wf_pool.acquire() as prune_conn:
        await prune_terminal_jobs(
            prune_conn,
            retention_per_status={"failed": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=wf_schema,
        )

    assert (
        await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
        == 1
    ), "retention archived the root beneath an undelivered post-terminal effect intent"
    assert await drain_outbox(wf_pool, wf_sql) == 1
    effect = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        consumer_step_key,
    )
    assert effect is not None, "the production drain dropped the post-terminal intent with its root"
    assert effect["status"] == "pending"

    # The delivered intent's active child now owns the hold: a second prune
    # must not archive the root during the materialization handoff.
    async with wf_pool.acquire() as prune_conn_after_drain:
        await prune_terminal_jobs(
            prune_conn_after_drain,
            retention_per_status={"failed": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=wf_schema,
        )
    assert (
        await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
        == 1
    )


async def test_execution_door_distinguishes_missing_root_from_missing_workflow_name(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """When a claimed workflow row's ``__flow__`` root row does not exist
    in ``jobs``, ``execute_flow_job`` must raise ``WorkflowRootMissingError``
    instead of misdiagnosing the NULL ``LEFT JOIN`` workflow name as
    ``WorkflowBodyUnresolvableError``; when the root row DOES exist without
    a ``workflow`` stamp, ``WorkflowBodyUnresolvableError`` still fires."""
    worker_id = JobId(new_uuid())

    # Case A: root row does NOT exist in `jobs` -> WorkflowRootMissingError.
    missing_flow_id = new_uuid()
    orphan_node_id = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=missing_flow_id,
        step_key="orphan_step",
        status="running",
    )
    with pytest.raises(WorkflowRootMissingError) as missing_exc:
        await execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=worker_id,
            job=_claimed_job_row(JobId(orphan_node_id), flow_id=missing_flow_id),
        )
    assert not isinstance(missing_exc.value, WorkflowBodyUnresolvableError)
    assert missing_exc.value.seat == "execution-door"
    assert str(missing_flow_id) in str(missing_exc.value)
    assert str(orphan_node_id) in str(missing_exc.value)
    assert "claim was already spent and no body ran" in str(missing_exc.value)
    assert "transaction rolled back" not in str(missing_exc.value)

    # Case B: root row EXISTS in `jobs` with no `workflow` key -> WorkflowBodyUnresolvableError.
    unstamped_flow_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', '__flow__', "
        "$2::jsonb, 'workflow-run', $3)",
        unstamped_flow_id,
        json.dumps({"flow_id": str(unstamped_flow_id)}),
        f"flow:{unstamped_flow_id}",
    )
    unstamped_node_id = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=unstamped_flow_id,
        step_key="solo",
        status="running",
    )
    with pytest.raises(WorkflowBodyUnresolvableError, match="stamps no workflow name"):
        await execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=worker_id,
            job=_claimed_job_row(JobId(unstamped_node_id), flow_id=unstamped_flow_id),
        )


async def test_outbox_drain_does_not_acknowledge_a_rootless_intent(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An undelivered intent whose run root is gone must stay undelivered.

    The drain's consumer insert inner-joins the root, so a rootless intent can
    materialize nothing. It must be reported (``sweep-wf-outbox-root-missing``)
    and left undelivered — never flipped as if the delivery happened — and it
    must not occupy the fetch's batch head ahead of a healthy intent.
    """
    import structlog.testing

    from taskq.workflows import _sweep as sweep_mod

    # The warn cadence is process-global; reset it so this test's observation
    # is not silenced by an earlier pass.
    monkeypatch.setattr(sweep_mod, "_outbox_root_missing_last_warned", None)

    missing_flow = new_uuid()
    rootless_intent = new_uuid()
    for _ in range(5):  # rootless intents sort ahead of the healthy one
        await wf_conn.execute(
            wf_sql.outbox_insert,
            new_uuid(),
            new_uuid(),
            missing_flow,
            WF_LOOP_ESCALATION_STEP_KEY,
            None,
            dumps_jsonb_str(
                {
                    "actor": "wf",
                    "queue": "default",
                    "payload": {"wf_args": [{"loop": "poll"}]},
                }
            ),
        )
    await wf_conn.execute(
        wf_sql.outbox_insert,
        rootless_intent,
        new_uuid(),
        missing_flow,
        WF_LOOP_ESCALATION_STEP_KEY,
        None,
        dumps_jsonb_str(
            {"actor": "wf", "queue": "default", "payload": {"wf_args": [{"loop": "poll"}]}}
        ),
    )

    healthy_flow = await seed_flow(wf_conn, wf_schema, status="failed")
    healthy_node = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=healthy_flow,
        step_key="work",
        status="failed",
    )
    await wf_conn.execute(
        wf_sql.outbox_insert,
        new_uuid(),
        healthy_node,
        healthy_flow,
        "work.__on_exhaustion",
        None,
        dumps_jsonb_str(
            {
                "step_key": "work.__on_exhaustion",
                "actor": "wf",
                "queue": "default",
                "payload": {},
                "map_index": None,
                "wf_effect_key": f"{healthy_flow}:1:{healthy_node}:on_exhaustion",
                "after_terminal": True,
            }
        ),
    )

    with structlog.testing.capture_logs() as captured:
        drained = await drain_outbox(wf_pool, wf_sql, batch_size=2)

    delivered = await wf_conn.fetchval(
        f'SELECT delivered FROM "{wf_schema}".wf_outbox WHERE id = $1', rootless_intent
    )
    consumers = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1",
        missing_flow,
    )
    healthy_consumer = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'work.__on_exhaustion'",
        healthy_flow,
    )

    assert delivered is False, "the drain acknowledged a delivery it did not make"
    assert consumers == 0, "a consumer materialized without a run root"
    assert drained == 1, "the healthy intent behind the rootless batch did not drain"
    assert healthy_consumer == 1, "the healthy intent's consumer never materialized"
    warnings = [event for event in captured if event.get("event") == "sweep-wf-outbox-root-missing"]
    assert warnings, (
        "the rootless intent was left undelivered without the "
        f"sweep-wf-outbox-root-missing warning: {[e.get('event') for e in captured]}"
    )
    assert any(
        any(intent.get("flow_id") == str(missing_flow) for intent in event.get("intents", []))
        for event in warnings
    ), warnings


async def test_archive_write_rechecks_the_root_hold_at_lock_time(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """A child re-pended between the candidate fetch and the archive write
    must keep its run's root.

    The candidate window evaluates the root hold once; the write statement
    locks the batch and re-checks it. A concurrent ``retry_job`` that turns a
    terminal child back into a pending one inside that window must drop the
    root out of the batch instead of re-creating the rootless state the fence
    set exists to refuse.
    """
    flow_id = await seed_flow(wf_conn, wf_schema, status="failed")
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET finished_at = now() - interval '40 days', "
        "actor = 'flow' WHERE id = $1",
        flow_id,
    )
    child = await _seed_wf_child(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        step_key="work",
        status="succeeded",
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET finished_at = now() - interval '40 days' WHERE id = $1",
        child,
    )

    candidate_sql = _ARCHIVE_CANDIDATE_SQL.format(schema=wf_schema)
    write_sql = _ARCHIVE_CTE_SQL.format(schema=wf_schema)
    retry_sql = render(wf_schema).retry_job
    retention = timedelta(days=30)

    async with wf_pool.acquire() as prune_conn, prune_conn.transaction():
        candidates = await prune_conn.fetch(candidate_sql, "failed", retention, 100)
        ids = [row["id"] for row in candidates]
        assert flow_id in ids, "fixture: the eligible root must be a candidate"

        # The concurrent writer commits INSIDE the window: a real re-pend of a
        # terminal node of the same run.
        async with wf_pool.acquire() as writer_conn:
            repended = await writer_conn.fetchrow(retry_sql, child)
        assert repended is not None, "fixture: retry_job must re-pend the terminal child"

        await prune_conn.fetch(write_sql, "failed", timedelta(days=365), ids, retention)

    root_left = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    child_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', child
    )
    assert root_left == 1, (
        "the archive write archived a finished run's root although a child of "
        f"it was non-terminal at lock time (child status now {child_status!r}): "
        "the root hold is not re-evaluated under the archive lock"
    )
