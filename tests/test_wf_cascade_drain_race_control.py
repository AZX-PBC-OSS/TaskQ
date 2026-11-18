# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The N1 cascade-vs-drain control: the failure-cascade seat — the SECOND
root-failure seat — cannot strand a promoted reserved source.

The defect this control pins: the failure cascade's root finalize was
UNCONDITIONAL (its `flow_failed` consulted only its own blocked-join
fact), and its §5.4 fence ran as a separate statement with a fresh
snapshot that selects only exact parks — so when the drain's promotion
committed between the cascade's decision and its fence, the root
terminalized `failed` over a live pending reserved source the fence
never saw: unclaimable on a terminal flow, charging its reservation
against the run forever (`flow_failed=1, fenced=0` — the D1 strand
through the other root-failure seat).

THE FIX under the state machine's own precedence (the maintenance seat's
own law, now shared by both root-failure seats): the cascade's
`flow_failed` carries the UNRESOLVED-LIVE gate (a promoted pending
source counts as unresolved live work — precedence row 1 outranks the
failure; the maintenance's later pass owns the finalize once the live
work rests), and the engine's cascade seat fires the §5.4 fence ONLY
when its own root finalize actually committed.

THE CONTROL builds the exact executed counterexample for real: the
estate (a running root, an exhausted failed child, its fail_closed join,
the parked source with its cursor, a pending intent); the control holds
the park row's own lock, starts the real `wake_emit_backpressure`
(blocked at its CAS while holding the root lock), starts the real
`FAIL_CLOSED_CASCADE_SQL` (blocked at `flow_failed` on the drain-held
root), releases the holder, and awaits both. The assertions:

* the DRAIN's own outcome: the promotion committed (the source is
  `pending` with its reservation);
* the CASCADE's own outcome: `flow_failed = 0` — the unresolved-live
  gate withheld the root's failure (the strand's precondition is gone);
* the root is still `running`; the fence fired ZERO rows;
* the strand (root `failed` + pending reserved source + fenced 0) never
  forms — in EITHER serial order (the cascade-wins order fences the
  still-parked source, the same face the maintenance control proves).
"""

from __future__ import annotations

import asyncio
import contextlib
import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows._emit import wake_emit_backpressure
from taskq.workflows.engine import render_workflow_sql

pytestmark = pytest.mark.integration

_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


def _park_meta(flow_id: str, *, width: int, bound: int) -> dict[str, Any]:
    return {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
        "emit_cursor": {"page": 0},
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": width,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }


async def _seed_cascade_estate(conn: Any, schema: str) -> tuple[str, str]:
    """A running root, an exhausted failed child, the child's fail_closed
    join row, the parked source (with its cursor), a pending intent: the
    cascade's own estate."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', "
        "'__flow__', $2::jsonb, 'workflow-run', $3)",
        flow_id,
        _json.dumps({"flow_id": flow_id}),
        f"flow:{flow_id}",
    )
    failed_child = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, error_class, error_message, "
        "finished_at, metadata, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'failed', 1, 1, 'transient', "
        "'screen', 'ValueError', 'exhausted', clock_timestamp(), $2::jsonb, "
        "$3, 'workflow-run', $4)",
        failed_child,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:screen",
    )
    # THE FAIL_CLOSED JOIN: the failed child's own downstream join row
    # (pending, deps_pending=1, the join marker) + the fail_closed edge.
    join_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, deps_pending, metadata, scheduled_at, "
        "created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'pending', 0, 2, 'transient', 'join_step', "
        "1, $2::jsonb, clock_timestamp() + interval '1 hour', clock_timestamp(), "
        "$3, 'workflow-run', $4)",
        join_id,
        _json.dumps({"flow_id": flow_id, "blocking_reason": "join"}),
        flow_id,
        f"wf:{flow_id}:join_step",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'fail_closed')",
        join_id,
        failed_child,
        flow_id,
    )
    source_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, assignment_routed, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 0, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, true, 'workflow-run', $4)",
        source_id,
        _json.dumps(_park_meta(flow_id, width=2, bound=10)),
        flow_id,
        f"wf:{flow_id}:source",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity '
        "(flow_id, bound, wake_generation, handled_generation) VALUES ($1::uuid, 10, 1, 0)",
        flow_id,
    )
    return flow_id, source_id


async def _run_real_cascade_seat(conn: Any, wsql: Any, flow_id: str) -> None:
    """The REAL failure-cascade seat (`engine._resolve_failed_parent` —
    the cascade statement + the gated §5.4 fence, one transaction), driven
    the way the runner's own exhaustion arm drives it."""
    from taskq.workflows.engine import _resolve_failed_parent

    failed_child = JobId(
        await conn.fetchval(
            f"""SELECT id FROM "{wsql.schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND step_key <> 'source' AND status = 'failed' LIMIT 1""",
            flow_id,
        )
    )
    tx = conn.transaction()
    await tx.start()
    try:
        await _resolve_failed_parent(
            conn,
            wsql,
            flow_id=JobId(flow_id),
            parent_id=failed_child,
            step_key="screen",
            map_index=None,
            error_class="ValueError",
            error_message="exhausted",
            error_traceback=None,
        )
        await tx.commit()
    except BaseException:
        await tx.rollback()
        raise


async def test_cascade_never_strands_a_promoted_reserved_source(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """THE DRAIN-WINS ORDER (forced, and the only order this topology's
    lock queue can produce with the park row held first): the real drain
    and the real failure cascade race over the real estate; the
    promotion commits, the unresolved-live gate withholds the root's
    failure, and the strand never forms. The cascade-wins order is the
    separately forced test below."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, source_id = await _seed_cascade_estate(wf_conn, wf_schema)

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    poller = await asyncpg.connect(module_pg_schema.pg_dsn)
    drain_task: asyncio.Task[Any] | None = None
    cascade_task: asyncio.Task[Any] | None = None
    try:
        blocker_tx = holder.transaction()
        await blocker_tx.start()
        held = await holder.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1::uuid FOR UPDATE', source_id
        )
        assert held is not None

        # THE DRAIN FIRST: its chunk transaction takes the live root's lock
        # and blocks at its candidate CAS on the held park row — THE
        # CASCADE (started after) blocks at its ENTRY root lock on the
        # drain-held root (the R2/A9.2 canonical order). This is the
        # drain-wins forced order (the promotion must be able to commit
        # before the cascade's decision).
        from taskq.workflows._emit import COUNTERS

        pre_census = COUNTERS.snapshot()
        drain_task = asyncio.create_task(
            wake_emit_backpressure(wf_pool, wsql, flow_id=JobId(flow_id))
        )
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            census_now = COUNTERS.snapshot()
            if (
                census_now["flow_drains"] > pre_census["flow_drains"]
                and census_now["candidate_reads"] > pre_census["candidate_reads"]
            ):
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("the drain never reached its blocked CAS")

        cascade_task = asyncio.create_task(_run_real_cascade_seat(wf_conn, wsql, flow_id))
        # One yield so the cascade task's own first statement (its ENTRY
        # root lock) queues on the drain-held root — the queue's formation
        # is the census + the release's own resolution, never a sleep.
        await asyncio.sleep(0)
        await blocker_tx.commit()

        drain = await asyncio.wait_for(drain_task, 30)
        await asyncio.wait_for(cascade_task, 30)

        # THE DRAIN-WINS ASSERTIONS (the exact order this test forces):
        # the promotion committed (the source is `pending` with its
        # reservation) and the unresolved-live gate withheld the root's
        # failure — the run continues.
        root_status = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
        )
        fenced = await wf_conn.fetchval(
            f"""SELECT count(*) FROM "{wf_schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND error_class = 'WorkflowNotDrainingError'""",
            flow_id,
        )
        src = await wf_conn.fetchrow(
            f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1::uuid',
            source_id,
        )
        assert src is not None
        src_status = src["status"]
        src_meta = (
            _json.loads(src["metadata"])
            if isinstance(src["metadata"], str)
            else dict(src["metadata"])
        )
        assert root_status is not None

        # THE DRAIN-WINS ORDER'S OWN LAW (no unreachable conditional).
        assert drain.promoted == 1, drain
        assert root_status == "running", root_status
        assert src_status == "pending", src_status
        assert src_meta.get("wf_emit_reservation") == {"version": 1, "width": 2}, src_meta
        assert int(fenced) == 0, fenced

        # THE STRAND: never.
        assert not (
            root_status == "failed"
            and src_status == "pending"
            and "wf_emit_reservation" in src_meta
            and int(fenced) == 0
        ), {
            "root": root_status,
            "source": src_status,
            "reservation": src_meta.get("wf_emit_reservation"),
            "fenced": fenced,
        }
    finally:
        # DETERMINISTIC RESOURCE OWNERSHIP (F8): every task reaped, the
        # blocker's own transaction rolled back, every owned connection
        # closed — on success and failure alike (a database drop at
        # module teardown is not connection ownership).
        for task in (drain_task, cascade_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
        with contextlib.suppress(Exception):
            await holder.execute("ROLLBACK")
        await holder.close()
        await poller.close()


async def test_cascade_wins_fences_the_still_parked_source(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """THE CASCADE-WINS ORDER (separately forced): the cascade takes the
    ENTRY root lock first; its §5.4 fence queues on the held park row
    (keyed to the blocker's own xid); the drain queues behind the
    cascade-held root. On release the fence wins: the still-parked source
    receives its lawful terminal disposition/event/ledger fence, and the
    later drain promotes ZERO (the intent completes with no fitting
    candidate)."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, source_id = await _seed_cascade_estate(wf_conn, wf_schema)

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    poller = await asyncpg.connect(module_pg_schema.pg_dsn)
    drain_task: asyncio.Task[Any] | None = None
    cascade_task: asyncio.Task[Any] | None = None
    try:
        blocker_tx = holder.transaction()
        await blocker_tx.start()
        held = await holder.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1::uuid FOR UPDATE', source_id
        )
        assert held is not None
        holder_xid = str(await holder.fetchval("SELECT pg_current_xact_id()::xid::text"))

        from taskq.workflows._emit import COUNTERS

        pre_census = COUNTERS.snapshot()

        # THE CASCADE FIRST: its ENTRY root lock is free — it grants, the
        # cascade statement stamps the blocked join, the gated root
        # failure commits, and the §5.4 fence QUEUES on the held park row
        # (keyed to the blocker's own xid).
        cascade_task = asyncio.create_task(_run_real_cascade_seat(wf_conn, wsql, flow_id))
        keyed_sql = """
        SELECT DISTINCT l.pid
        FROM pg_locks l
        WHERE l.locktype = 'transactionid'
          AND NOT l.granted
          AND l.transactionid::text = $1
          AND l.pid <> pg_backend_pid()
        """
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            rows = await poller.fetch(keyed_sql, holder_xid)
            if rows:
                break
            await asyncio.sleep(0.02)
        else:
            pytest.fail("the cascade's §5.4 fence never queued on the held park row")

        # THE DRAIN SECOND: it queues behind the cascade-held root (its
        # own flow_drains bump is its first statement's fact).
        drain_task = asyncio.create_task(
            wake_emit_backpressure(wf_pool, wsql, flow_id=JobId(flow_id))
        )
        while asyncio.get_running_loop().time() < deadline:
            census_now = COUNTERS.snapshot()
            if census_now["flow_drains"] > pre_census["flow_drains"]:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("the drain never started its chunk")

        await blocker_tx.commit()
        drain = await asyncio.wait_for(drain_task, 30)
        await asyncio.wait_for(cascade_task, 30)

        # THE CASCADE-WINS ORDER'S OWN LAW: the still-parked source took
        # the §5.4 disposition (the fence won the park row), the root
        # failed, and the later drain promoted ZERO.
        root_status = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
        )
        assert root_status == "failed", root_status
        src = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, error_class, metadata
                FROM "{wf_schema}".jobs WHERE id = $1::uuid""",
            source_id,
        )
        assert src is not None
        assert src["status"] == "failed", dict(src)
        assert src["error_class"] == "WorkflowNotDrainingError", dict(src)
        src_meta = (
            _json.loads(src["metadata"])
            if isinstance(src["metadata"], str)
            else dict(src["metadata"])
        )
        assert "wf_not_draining" in src_meta, src_meta
        assert "wf_emit_reservation" not in src_meta, src_meta
        assert drain.promoted == 0, drain
        # The intent handled (the drain's fresh observation returned no
        # fitting candidate).
        intent_row = await wf_conn.fetchrow(
            f'SELECT wake_generation, handled_generation FROM "{wf_schema}".wf_emit_capacity '
            "WHERE flow_id = $1::uuid",
            flow_id,
        )
        assert intent_row is not None
        assert int(intent_row["handled_generation"]) == int(intent_row["wake_generation"]), dict(
            intent_row
        )
    finally:
        for task in (drain_task, cascade_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
        with contextlib.suppress(Exception):
            await holder.execute("ROLLBACK")
        await holder.close()
        await poller.close()
