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
import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
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
    wf_conn: Any, wf_schema: str, wf_pool: Any, module_pg_schema: Any
) -> None:
    """The real drain and the real failure cascade race over the real
    estate; the strand may never form through the second root-failure
    seat."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, source_id = await _seed_cascade_estate(wf_conn, wf_schema)

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    blocker_tx = holder.transaction()
    await blocker_tx.start()
    held = await holder.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1::uuid FOR UPDATE', source_id
    )
    assert held is not None

    # THE DRAIN FIRST: its chunk transaction takes the live root's lock
    # and blocks at its candidate CAS on the held park row — THE
    # CASCADE (started after) blocks at `flow_failed` on the drain-held
    # root. This is the executed counterexample's own order (the
    # promotion must be able to commit before the cascade's decision).
    from taskq.workflows._emit import COUNTERS

    pre_census = COUNTERS.snapshot()
    drain_task = asyncio.create_task(wake_emit_backpressure(wf_pool, wsql, flow_id=JobId(flow_id)))
    poller = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
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
    finally:
        await poller.close()

    cascade_task = asyncio.create_task(_run_real_cascade_seat(wf_conn, wsql, flow_id))
    await asyncio.sleep(0.5)  # the cascade is queued at the drain-held root
    await blocker_tx.commit()

    drain = await drain_task
    cascade = await cascade_task

    # THE REAL SEAT'S OWN RECEIPT: the cascade's own counters (the
    # blocked joins, the peer cancels, the root finalize's own
    # gated outcome) — read back from the cascade's own function's
    # recorded facts.
    cascade = await wf_conn.fetchrow(
        f"""SELECT
              (SELECT count(*)::int FROM "{wf_schema}".jobs
               WHERE (metadata->>'flow_id')::uuid = $1::uuid
                 AND metadata ? 'failed_parent') AS blocked,
              (SELECT count(*)::int FROM "{wf_schema}".jobs
               WHERE id = $1::uuid AND status = 'failed') AS flow_failed""",
        flow_id,
    )
    assert cascade is not None

    # THE TWO SERIAL ORDERS (both are the no-strand law):
    # * the CASCADE wins the root: its own root finalize committed AND its
    #   same-tx fence fenced the still-parked source (the source is
    #   `failed` with the fence's own record, the key removed);
    # * the DRAIN wins the root: the promotion committed (the source is
    #   `pending` with its reservation) and the unresolved-live gate
    #   withheld the cascade's root failure (`flow_failed = 0`) — the run
    #   continues.
    # THE STRAND — root `failed` beside a `pending` source still holding
    # its reservation with `fenced = 0` — is the red in both directions.
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
        _json.loads(src["metadata"]) if isinstance(src["metadata"], str) else dict(src["metadata"])
    )
    assert root_status is not None

    if root_status == "failed":
        # THE CASCADE-WINS ORDER: the fence must have done its own job.
        assert int(cascade["flow_failed"]) == 1, dict(cascade)
        assert src_status == "failed", (src_status, dict(cascade))
        assert int(fenced) == 1, fenced
        assert "wf_emit_reservation" not in src_meta, src_meta
        assert "wf_not_draining" in src_meta, src_meta
    else:
        # THE DRAIN-WINS ORDER: the promotion committed; the
        # unresolved-live gate withheld the root's failure.
        assert drain.promoted == 1, drain
        assert int(cascade["flow_failed"]) == 0, dict(cascade)
        assert int(cascade["blocked"]) == 1, dict(cascade)
        assert root_status == "running", root_status
        assert int(fenced) == 0, fenced
        assert src_status == "pending", src_status
        assert src_meta.get("wf_emit_reservation") == {"version": 1, "width": 2}, src_meta

    # THE STRAND: never, in either order.
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
        "cascade": dict(cascade) if cascade is not None else None,
    }
