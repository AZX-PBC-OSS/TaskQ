"""The §2.4 control: the page-width RESERVATION key's fate at every exit.

Real PostgreSQL. Every terminal family that can own a promoted source's row
removes the key in its own winning statement (a CAS on the key's presence
computed over the winning rows, never a Python pre-read), and the next
derived observation necessarily reflects the row and the removed key. The
opposite direction: a non-winning writer changes neither the charge nor the
key."""

from __future__ import annotations

import json as _json
from typing import Any

import asyncpg
import pytest

from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration

_RESERVATION = _json.dumps({"version": 1, "width": 3})
_PARK_MARKER = _json.dumps(
    {
        "flow_id": "FLOW",
        "blocking_reason": "backpressure",
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {"version": 1, "page_width": 3, "bound": 3},
    }
)


def _meta(raw: object) -> dict[str, Any]:
    import json

    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


async def _seed_reserved_running(
    conn: asyncpg.Connection, schema: str, flow: JobId, step_key: str = "source"
) -> JobId:
    """A promoted source's claimed row carrying its page-width reservation."""
    from taskq._ids import new_uuid

    job = JobId(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key, attempt, claim_epoch, locked_by_worker, "
        "lock_expires_at) "
        "VALUES ($1, 'wf-node', 'default', '{}', 'running', 3, 'transient', $2, "
        "$3::jsonb, $4, 1, 1, $5, now() + interval '90 seconds')",
        job,
        step_key,
        _json.dumps({"flow_id": str(flow), "wf_emit_reservation": _json.loads(_RESERVATION)}),
        str(flow),
        job,
    )
    return job


async def _observation(conn: asyncpg.Connection, wsql: WorkflowSql, flow: JobId) -> tuple[int, int]:
    row = await conn.fetchrow(wsql.emit_capacity_observation, flow, None)
    assert row is not None  # the aggregate always returns exactly one row
    return int(row["live_rows"]), int(row["reservations"])


async def test_reserved_row_terminalizes_through_each_family_exactly_once(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """Every terminal family removes the key in its own winning statement and
    the derived charge moves exactly once; a non-winning writer changes
    neither."""
    from taskq._ids import new_uuid

    # ── the wf terminal mark (the engine's own family) ──
    flow = JobId(new_uuid())
    job = await _seed_reserved_running(wf_conn, wf_schema, flow)
    live, reservations = await _observation(wf_conn, wf_sql, flow)
    assert (live, reservations) == (1, 3), (live, reservations)
    await wf_conn.execute(
        wf_sql.terminal_mark,
        job,
        "succeeded",
        "{}",
        2,
        None,
        None,
        None,
        job,
        1,
        1,
    )
    live, reservations = await _observation(wf_conn, wf_sql, flow)
    assert (live, reservations) == (0, 0), "the terminal moved charge exactly once"
    row = await wf_conn.fetchrow(f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', job)
    assert row is not None
    assert "wf_emit_reservation" not in _meta(row["metadata"])

    # ── the cooperative cancel mark RETAINS the key (the opposite direction) ──
    flow2 = JobId(new_uuid())
    await _seed_reserved_running(wf_conn, wf_schema, flow2)
    from taskq.workflows.api._sql_runner import CANCEL_NODES_SQL_TEMPLATE, render_sql

    # The statement's CTE does the flip; the outer SELECT's tag is not a
    # rowcount — the row state is the assertion.
    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow2)
    live, reservations = await _observation(wf_conn, wf_sql, flow2)
    assert (live, reservations) == (1, 3), (
        "the cooperative phase-1 mark is not a terminal: the key is retained"
    )
    # The TERMINAL flip: an unclaimed reserved row flips cancelled and
    # removes the key exactly once (the running row's cooperative mark
    # above is the phase-1 arm that retains it — same family, both faces).
    flow2b = JobId(new_uuid())
    job2b = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key) "
        "VALUES ($1, 'wf-node', 'default', '{}', 'pending', 3, 'transient', 'source', "
        "$2::jsonb, $3)",
        job2b,
        _json.dumps({"flow_id": str(flow2b), "wf_emit_reservation": _json.loads(_RESERVATION)}),
        str(flow2b),
    )
    probe = await wf_conn.fetchrow(
        f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1', job2b
    )
    assert probe is not None and probe["status"] == "pending", probe
    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), flow2b)
    live, reservations = await _observation(wf_conn, wf_sql, flow2b)
    assert (live, reservations) == (0, 0), "the cancel's winning flip moved charge exactly once"
    row = await wf_conn.fetchrow(f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', job2b)
    assert row is not None
    assert "wf_emit_reservation" not in _meta(row["metadata"])

    # ── the loop exhaust's batched family ──
    from taskq.workflows.api._sql_loop import LOOP_EXHAUST_SQL, render_loop_sql

    flow3 = JobId(new_uuid())
    job3 = await _seed_reserved_running(wf_conn, wf_schema, flow3, step_key="counter")
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',
        job3,
        _json.dumps({"iteration": 3, "max_iterations": 3, "kind": "loop"}),
    )
    from taskq.workflows.ledger import claim_step_ledger

    async with wf_pool.acquire() as conn:
        ledger = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow3,
            job_id=job3,
            step_key="counter",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
    assert ledger.ledger_id is not None
    verdict = await wf_conn.fetchrow(
        render_loop_sql(LOOP_EXHAUST_SQL, wf_schema),
        job3,
        "IterationLimitExhausted",
        '{"iteration_state": "iteration_cap_exhausted", "kind": "loop"}',
        "the loop's wall fired",
        job3,
        1,
        1,
        "counter",
        None,
    )
    assert verdict is not None and int(verdict["loop_exhausted"]) == 1, verdict
    live, reservations = await _observation(wf_conn, wf_sql, flow3)
    assert (live, reservations) == (0, 0), "the exhaust's winning CAS moved charge exactly once"
    row = await wf_conn.fetchrow(f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', job3)
    assert row is not None
    assert "wf_emit_reservation" not in _meta(row["metadata"])

    # ── a NON-winning writer changes neither the charge nor the key ──
    flow4 = JobId(new_uuid())
    job4 = await _seed_reserved_running(wf_conn, wf_schema, flow4)
    live_before, reservations_before = await _observation(wf_conn, wf_sql, flow4)
    stale = await wf_conn.execute(
        wf_sql.terminal_mark,
        job4,
        "failed",
        "{}",
        2,
        "Zombie",
        None,
        None,
        job4,
        9,  # a stale attempt — the fence refuses
        1,
    )
    assert stale in ("UPDATE 0", "SELECT 0"), stale
    live_after, reservations_after = await _observation(wf_conn, wf_sql, flow4)
    assert (live_after, reservations_after) == (live_before, reservations_before)
    row = await wf_conn.fetchrow(f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', job4)
    assert row is not None
    assert _meta(row["metadata"])["wf_emit_reservation"] == _json.loads(_RESERVATION), (
        "a fenced-out writer never touches the key"
    )
