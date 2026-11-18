# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The D1 root-race control: the flow drain serializes with the
root-failure and maintenance seats on the LIVE root row's own lock.

The defect this control pins: the drain's promotion CAS and the §5.4
non-drainage fence (which the root-maintenance failure arm and the
failure-cascade seat both run) raced each other — the maintenance's
root finalize computed its aggregates while the source was still an
exact park (a resolved-blocked row, excluded from ``has_unresolved``),
then the fence's park UPDATE re-evaluated the row AFTER the drain's CAS
had converted it to a pending reserved source, skipped it, and
committed a ``failed`` root over a stranded pending reserved source
(unclaimable on a terminal flow, ``not_draining_fenced = 0``).

The fix under contract §3.1 ("A flow drain locks the live root, then
the intent row"): every drain chunk's transaction takes the live root
row ``FOR UPDATE`` before the intent row and any candidate CAS. The two
legal serial orders are then:

* the maintenance wins the root: it finalizes ``failed`` and its own
  same-transaction fence fences the still-parked source (source
  ``failed`` + ``WorkflowNotDrainingError`` + the key removed) — the
  review's control arm, ``not_draining_fenced: 1``;
* the drain wins the root: the maintenance's ``FOR UPDATE SKIP LOCKED``
  roots scan skips the flow (no finalize), and after the drain commits
  the promoted source is unresolved live work, so the root legitimately
  does not finalize — the run continues.

The STRAND (root ``failed`` beside a pending source holding its
reservation with ``not_draining_fenced = 0``) is the red in both
directions. The control builds the race for real: it holds the park
row's lock, starts the real :func:`wake_emit_backpressure` (blocked at
the CAS while holding the root lock), starts the real
``WORKFLOW_ROOT_MAINTAIN_SQL`` maintenance statement (the fence's own
seat), releases the holder, and awaits both. Under the fix the
maintenance's roots scan skips the drain-held root (``roots_updated =
0``), so the maintenance-first fence order cannot interleave with the
promotion; the mutant proof (draining without the root lock) reproduces
the strand and reds this control.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows._emit import wake_emit_backpressure
from taskq.workflows.engine import render_workflow_sql

pytestmark = pytest.mark.integration

_WAIT_S = 10.0

_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


def _park_meta(flow_id: str, *, width: int, bound: int) -> dict[str, Any]:
    """The complete §2.2 park row's metadata as exact literal values —
    every key and value, NO other metadata key (plus the real park's
    durable un-moved cursor, which the §5.4 fence's proof requires)."""
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


async def _seed_race(conn: asyncpg.Connection, schema: str) -> tuple[str, str]:
    """One running root, one failed non-absorbed child, one exact park
    (with the cursor), one pending intent: the D1 race's estate."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', "
        "'__flow__', $2::jsonb, 'workflow-run', $3)",
        flow_id,
        json.dumps({"flow_id": flow_id}),
        f"flow:{flow_id}",
    )
    child_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, step_key, error_class, error_message, "
        "finished_at, metadata, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 1, 'transient', 'failed', 1, "
        "'screen', 'ValueError', 'exhausted', clock_timestamp(), $2::jsonb, "
        "$3, 'workflow-run', $4)",
        child_id,
        json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:screen",
    )
    park_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
        "attempt, max_attempts, retry_kind, step_key, metadata, "
        "scheduled_at, created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 0, 2, 'transient', "
        "'source', $2::jsonb, clock_timestamp(), clock_timestamp(), $3, "
        "'workflow-run', $4)",
        park_id,
        json.dumps(_park_meta(flow_id, width=2, bound=10)),
        flow_id,
        f"wf:{flow_id}:source",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity '
        "(flow_id, bound, wake_generation, handled_generation) "
        "VALUES ($1::uuid, 10, 1, 0)",
        flow_id,
    )
    return flow_id, park_id


async def _row_state(conn: asyncpg.Connection, schema: str, flow_id: str) -> dict[str, Any]:
    root_status = await conn.fetchval(
        f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid', flow_id
    )
    src = await conn.fetchrow(
        f"""SELECT status::text AS status, metadata
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
        flow_id,
    )
    assert src is not None
    meta = src["metadata"]
    doc: dict[str, Any] = json.loads(meta) if isinstance(meta, str) else dict(meta)
    fenced = await conn.fetchval(
        f"""SELECT count(*) FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND error_class = 'WorkflowNotDrainingError'""",
        flow_id,
    )
    intent = await conn.fetchrow(
        f"""SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity
            WHERE flow_id = $1::uuid""",
        flow_id,
    )
    return {
        "root": root_status,
        "source_status": src["status"],
        "source_metadata": doc,
        "fenced": int(fenced or 0),
        "intent": dict(intent) if intent is not None else None,
    }


def _is_strand(state: dict[str, Any]) -> bool:
    """THE STRAND: a terminal ``failed`` root beside a pending source
    that still holds its reservation, with no fence having fired."""
    return (
        state["root"] == "failed"
        and state["source_status"] == "pending"
        and "wf_emit_reservation" in state["source_metadata"]
        and state["fenced"] == 0
    )


async def _drain_pid_holding_advisory(pool: asyncpg.Pool, flow_id: str) -> int | None:
    """The pid of the backend that holds this flow's admission advisory
    lock and is blocked on a lock — the drain's own chunk transaction,
    committed to the attempt and parked at its CAS."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _WAIT_S
    while loop.time() < deadline:
        rows = await pool.fetch(
            """SELECT a.pid, a.wait_event_type
               FROM pg_locks l
               JOIN pg_stat_activity a ON a.pid = l.pid
               WHERE l.locktype = 'advisory' AND l.objid = hashtext($1::text)
                 AND l.granted AND a.datname = current_database()""",
            flow_id,
        )
        for row in rows:
            if row["wait_event_type"] == "Lock":
                return int(row["pid"])
        await asyncio.sleep(0.02)
    return None


async def test_drain_serializes_with_root_maintenance_no_stranded_reserved_source(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The real drain and the real maintenance race over the real estate;
    the strand may never form."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, park_id = await _seed_race(wf_conn, wf_schema)

    # THE HOLDER: the park row's own lock — both the drain's CAS and the
    # maintenance's fence UPDATE queue on it.
    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    blocker_tx = holder.transaction()
    await blocker_tx.start()
    held = await holder.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1::uuid FOR UPDATE', park_id
    )
    assert held is not None

    drain_task = asyncio.create_task(wake_emit_backpressure(wf_pool, wsql, flow_id=JobId(flow_id)))
    try:
        # The drain is committed to the attempt: it holds the flow's
        # admission advisory lock (and, under the fix, the live root's
        # row lock) and is blocked on the park row (its CAS).
        drain_pid = await _drain_pid_holding_advisory(wf_pool, flow_id)
        assert drain_pid is not None, "the drain never reached its locked chunk"

        # THE MAINTENANCE (the fence's own seat) against the drain-held
        # root. Under the fix it completes immediately — the roots scan
        # SKIP LOCKED skips the drain-held root, so no finalize and no
        # fence can interleave between the drain's observations and its
        # CAS. Under the defect (no root lock in the drain) it proceeds
        # to finalize the root and blocks at the fence's park UPDATE.
        maintain_task: asyncio.Task[Any] = asyncio.create_task(
            wf_conn.fetchrow(wsql.workflow_root_maintain, 10)
        )
        early: dict[str, Any] | None = None
        done, _pending = await asyncio.wait([maintain_task], timeout=2.0)
        if done:
            early = dict(done.pop().result())

        # Release the holder: the drain's (older) CAS request wins the
        # park row, promotes the source, and completes the intent.
        await blocker_tx.commit()
        result = await drain_task
        assert result.promoted == 1, result
        assert result.intent_completed is True, result

        maintain = early
        if maintain is None:
            row = await maintain_task
            maintain = dict(row)
        # THE NO-RACE FENCE: the maintenance either skipped the
        # drain-held root (the fix) or fenced the still-parked source
        # before the drain ever ran — the finalize can never interleave
        # between the drain's observations and its CAS.
        assert int(maintain["roots_updated"]) == 0, maintain
        assert int(maintain["not_draining_fenced"]) == 0, maintain

        # The strand never formed — and the promoted source is
        # unresolved live work, so the maintenance still does not
        # finalize the root behind it.
        state = await _row_state(wf_conn, wf_schema, flow_id)
        assert not _is_strand(state), state
        assert state["root"] == "running", state
        assert state["fenced"] == 0, state
        maintain_again = await wf_conn.fetchrow(wsql.workflow_root_maintain, 10)
        assert maintain_again is not None
        assert int(maintain_again["roots_updated"]) == 0, dict(maintain_again)
        state = await _row_state(wf_conn, wf_schema, flow_id)
        assert not _is_strand(state), state
        assert state["root"] == "running", state
    finally:
        if not drain_task.done():
            drain_task.cancel()
        with contextlib.suppress(Exception):
            await blocker_tx.rollback()
        await holder.close()
