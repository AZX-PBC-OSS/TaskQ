# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The D3 terminal-family controls: every named terminal family removes
the page-width reservation key in its own winning statement, and the
manual re-arm restores an ordinary claimable live row.

The defect this control pins: six terminal families (the deadline
sweep, the early-Exit skip, the fail-closed cascade's peer cancel, the
bulk cancel, the batch abort, the force actor deregistration)
terminalized WITHOUT removing ``wf_emit_reservation`` — a terminal row
kept charging its width against the run's derived capacity forever —
and the manual re-arm (``retry_job``) re-pended a terminal row WITH its
key (the derived observation then reported the stale width as live
reservation charge), while a cancelled exact park re-armed to a pending
row still carrying the full park marker was unclaimable by the real
node claim and uncountable by the real claimable-nodes read — the
operator's Retry reporting success and leaving a permanently stuck row.

Contract §2.4: every terminal family removes the key in the winning
terminal UPDATE; a manual re-arm strips the key defensively and
restores only an ordinary live row. Each control below drives the REAL
statement and asserts the row's own metadata and the derived charge.
"""

from __future__ import annotations

import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


def _meta(raw: object) -> dict[str, Any]:
    import json

    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


async def _seed_reserved(
    conn: Any, schema: str, flow: JobId, step_key: str, status: str, *, width: int
) -> JobId:
    """A reserved row in the given status (the families' shared input)."""
    job = JobId(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key, attempt, claim_epoch, locked_by_worker, "
        "lock_expires_at) "
        f"VALUES ($1, 'wf-node', 'default', '{{}}', $2, 3, 'transient', $3, "
        "$4::jsonb, $5, 1, 1, $1, now() + interval '90 seconds')",
        job,
        status,
        step_key,
        _json.dumps({"flow_id": str(flow), "wf_emit_reservation": {"version": 1, "width": width}}),
        str(flow),
    )
    return job


def _wsql(schema: str) -> WorkflowSql:
    return WorkflowSql.build(schema)


async def _observation(conn: Any, wsql: WorkflowSql, flow: JobId) -> tuple[int, int]:
    """The derived charge after a winning statement (the §2.3 read)."""
    row = await conn.fetchrow(wsql.emit_capacity_observation, flow, None)
    return int(row["live_rows"]), int(row["reservations"])


async def test_deadline_sweep_terminal_removes_the_reservation_key(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """The deadline family's terminal removes the key in its own winning
    statement — no key survives for re-arm (the review's proof: the key
    survived with width 5)."""
    from taskq.backend._sweeps import sweep_deadline_exceeded

    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "source", "pending", width=5)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET schedule_to_close = now() - interval '1 hour' "
        "WHERE id = $1",
        job,
    )
    swept = await sweep_deadline_exceeded(wf_conn, schema=wf_schema, batch_size=10)
    assert swept >= 1, swept
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "failed", row
    assert "wf_emit_reservation" not in _meta(row["metadata"]), row
    obs = await wf_conn.fetchrow(wf_sql.emit_capacity_observation, flow, None)
    assert obs is not None and (int(obs["live_rows"]), int(obs["reservations"])) == (0, 0), (
        "the deadline terminal moved charge exactly once"
    )


async def test_early_exit_skip_terminal_removes_the_reservation_key(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The early-Exit family's skip-terminal removes the key in its own
    winning statement (the review's proof: the key survived with width 3)."""
    from taskq.workflows.api._sql_runner import EXIT_SKIP_SQL_TEMPLATE, render_sql

    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "doomed_branch", "pending", width=3)
    await wf_conn.execute(
        render_sql(EXIT_SKIP_SQL_TEMPLATE, wf_schema), flow, "{}", ["doomed_branch"]
    )
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "succeeded", row
    assert "wf_emit_reservation" not in _meta(row["metadata"]), row


async def test_peer_cancel_terminal_removes_the_reservation_key(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The fail-closed cascade's peer-cancel family removes the key in its
    own winning statement; the cancel-origin marker still rides the same
    write. The peers are the fan-in's OTHER parents sharing the failed
    child (two fail_closed parents joined to one child)."""
    wsql = _wsql(wf_schema)
    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "sibling_parent", "pending", width=3)
    failed_parent = JobId(new_uuid())
    child = JobId(new_uuid())
    for parent_id in (failed_parent, job):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
            "VALUES ($1, $2, $3, 'fail_closed')",
            child,
            parent_id,
            flow,
        )
    await wf_conn.execute(
        wsql.fail_closed_cascade,
        failed_parent,
        _json.dumps({"blocking_reason": "failed_parent"}),
        "TestParentFailure",
        _json.dumps({"peer_cancel": {"by": "peer_failure"}}),
    )
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "cancelled", row
    meta = _meta(row["metadata"])
    assert "wf_emit_reservation" not in meta, meta
    assert "peer_cancel" in meta, meta


async def test_manual_rearm_strips_key_and_park_marker_and_restores_claimability(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The manual re-arm (retry_job) strips the reservation key AND the
    park marker family, restoring an ORDINARY claimable live row — the
    cancelled exact park is claimable by the real node claim and counted
    by the real claimable-nodes read (the pre-fix behavior: claim None,
    claimable 0, forever)."""
    from taskq.backend._sql_templates import render as render_backend_sql
    from taskq.workflows.api._sql_runner import (
        CLAIMABLE_NODES_SQL_TEMPLATE,
        NODE_CLAIM_SQL_TEMPLATE,
        render_sql,
    )

    wsql = _wsql(wf_schema)
    backend_sql = render_backend_sql(wf_schema)

    # ── arm 1: a FAILED reserved row re-armed → ordinary pending, no key ──
    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "source", "pending", width=7)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "finished_at = clock_timestamp() WHERE id = $1",
        job,
    )
    retried = await wf_conn.fetchrow(backend_sql.retry_job, job)
    assert retried is not None, "the re-arm re-pends the terminal row"
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "pending", row
    assert "wf_emit_reservation" not in _meta(row["metadata"]), row
    obs = await wf_conn.fetchrow(wsql.emit_capacity_observation, flow, None)
    assert obs is not None and (int(obs["live_rows"]), int(obs["reservations"])) == (1, 0), (
        "the re-armed row is an ordinary live row: charge 1, no reservation"
    )

    # ── arm 2: a CANCELLED EXACT PARK re-armed → claimable pending ──
    flow2 = JobId(new_uuid())
    park = JobId(new_uuid())
    park_meta = {
        "flow_id": str(flow2),
        "blocking_reason": "backpressure",
        "emit_cursor": {"page": 0},
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": 2,
            "bound": 4,
            "outstanding": 4,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": "2026-11-15T09:30:00.123456Z",
        },
        "wf_source_logical_attempt": 1,
        "wf_emit_reservation": {"version": 1, "width": 2},
    }
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
        "step_key, metadata, fairness_key, assignment_routed) "
        "VALUES ($1, 'wf', 'default', '{}', 'cancelled', 0, 2, 'transient', 'source', "
        "$2::jsonb, $3, true)",
        park,
        _json.dumps(park_meta),
        str(flow2),
    )
    retried2 = await wf_conn.fetchrow(backend_sql.retry_job, park)
    assert retried2 is not None
    row2 = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', park
    )
    assert row2 is not None and row2["status"] == "pending", row2
    meta2 = _meta(row2["metadata"])
    for banned in (
        "wf_emit_reservation",
        "blocking_reason",
        "wf_deferral",
        "wf_emit_backpressure",
        "wf_not_draining",
    ):
        assert banned not in meta2, (banned, meta2)

    # THE CLAIMABILITY: the real node claim claims the re-armed row (the
    # claim's own RETURNING pair proves the claim happened); the real
    # claimable-nodes read counts it.
    # THE CLAIMABILITY: the real claimable-nodes read counts the re-armed
    # row, and the real node claim claims it (the claim's own RETURNING
    # pair proves the claim happened).
    countable = await wf_conn.fetchval(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema), flow2)
    assert countable is not None and int(countable) >= 1, countable
    worker = JobId(new_uuid())
    claimed = await wf_conn.fetchrow(render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), park, worker)
    assert claimed is not None, "the re-armed cancelled park is claimable by the real node claim"
    assert int(claimed["claim_epoch"]) >= 1, claimed


async def test_bulk_cancel_terminal_removes_the_reservation_key(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The bulk-cancel family's terminal removes the key in its own
    winning statement."""
    from taskq.backend._cancel_bulk import _cancel_where
    from taskq.backend._protocol import JobFilter
    from taskq.backend._sql_templates import render as render_backend_sql

    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "source", "pending", width=4)
    result, _targets = await _cancel_where(
        wf_pool,
        wf_schema,
        render_backend_sql(wf_schema),
        JobFilter(actor="wf-node"),
        "the control's cancel",
    )
    assert len(result.cancelled_ids) >= 1, result
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "cancelled", row
    assert "wf_emit_reservation" not in _meta(row["metadata"]), row


async def test_batch_abort_terminal_removes_the_reservation_key(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The batch-abort family's terminal removes the key in its own
    winning statement."""
    from taskq.backend._batch_sql import abort_batch, render_batch_sql

    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "source", "pending", width=4)
    batch_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".batches (id, queue, status, expected_size) '
        "VALUES ($1, 'default', 'active', 1)",
        batch_id,
    )
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',
        job,
        _json.dumps({"batch_id": str(batch_id)}),
    )
    cancelled = await abort_batch(wf_conn, render_batch_sql(wf_schema), batch_id)
    assert cancelled >= 1, cancelled
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "cancelled", row
    assert "wf_emit_reservation" not in _meta(row["metadata"]), row


async def test_actor_deregister_force_terminal_removes_the_reservation_key(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The force-deregistration family's terminal removes the key in its
    own winning statement."""
    from taskq.actor_config_ops import deregister_actor

    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "source", "pending", width=4)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2)',
        "wf-node",
        "wf-node-q",
    )
    result = await deregister_actor(wf_conn, "wf-node", force=True, schema=wf_schema)
    assert result is not None
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "cancelled", row
    assert "wf_emit_reservation" not in _meta(row["metadata"]), row


async def test_node_retry_rearm_strips_key_and_restores_claimability(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """N4: the per-node/manual re-arm (RETRY_NODE_CAS — the admin run
    page's node retry, the CLI, `retry_workflow_node`) strips the
    reservation key AND the park-marker family; the re-armed row is an
    ordinary claimable live row (the pre-fix shape: `reservations=7`
    charged by a stale key, and the row's own honest next page would die
    through the deterministic grant-violation route)."""
    from taskq.workflows.api._sql_runner import (
        NODE_CLAIM_SQL_TEMPLATE,
        RETRY_NODE_CAS_SQL_TEMPLATE,
        render_sql,
    )

    wsql = _wsql(wf_schema)
    flow = JobId(new_uuid())
    job = await _seed_reserved(wf_conn, wf_schema, flow, "source", "pending", width=7)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "finished_at = clock_timestamp() WHERE id = $1",
        job,
    )
    # THE INPUT: the terminal row carrying a STALE key (the mixed-version /
    # foreign-writer shape the defensive strip is written for).
    stale = await wf_conn.fetchrow(f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', job)
    assert stale is not None and "wf_emit_reservation" in _meta(stale["metadata"])

    matched = await wf_conn.fetchval(
        render_sql(RETRY_NODE_CAS_SQL_TEMPLATE, wf_schema), flow, "source"
    )
    assert int(matched or 0) >= 1, matched
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1', job
    )
    assert row is not None and row["status"] == "pending", row
    meta = _meta(row["metadata"])
    for banned in (
        "wf_emit_reservation",
        "blocking_reason",
        "wf_deferral",
        "wf_emit_backpressure",
        "wf_not_draining",
    ):
        assert banned not in meta, (banned, meta)
    # THE DERIVED CHARGE: no stale reservation.
    obs = await wf_conn.fetchrow(wsql.emit_capacity_observation, flow, None)
    assert obs is not None and (int(obs["live_rows"]), int(obs["reservations"])) == (1, 0), obs
    # THE CLAIMABILITY: the real node claim claims the re-armed row.
    worker = JobId(new_uuid())
    claimed = await wf_conn.fetchrow(render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), job, worker)
    assert claimed is not None, "the node-retry re-armed row is claimable"


async def test_terminal_mixed_key_present_and_key_absent_winners(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """§2.4's mixed-winner arm: one terminal statement over BOTH a
    key-present and a key-absent winner removes the key from exactly the
    winners that had one, charges exactly the captured widths, and never
    double-accounts. THE CAPTURE'S OWN PRODUCTION CONSUMER (F10): the
    captured old width rides the terminal's OWN durable `job_events`
    record as the optional bounded `released_emit_reservation_width`
    detail (jsonb_strip_nulls omits it for the key-absent winner while a
    valid ZERO width stays visible) — the Python-facing terminal SELECT
    no longer carries the capture at all."""
    flow = JobId(new_uuid())
    with_key = await _seed_reserved(wf_conn, wf_schema, flow, "source", "running", width=7)
    without_key = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key, attempt, claim_epoch, locked_by_worker, "
        "lock_expires_at) "
        f"VALUES ($1, 'wf-node', 'default', '{{}}', 'running', 3, 'transient', 'source', "
        "$2::jsonb, $3, 1, 1, $1, now() + interval '90 seconds')",
        without_key,
        _json.dumps({"flow_id": str(flow)}),
        str(flow),
    )
    live, reservations = await _observation(wf_conn, wf_sql, flow)
    assert (live, reservations) == (2, 7), (live, reservations)

    # THE ENGINE TERMINAL over both rows: the key-present winner loses its
    # key; the key-absent winner is unchanged; the charge moves exactly
    # once. THE DURABLE EVENT DETAIL is the capture's own consumer.
    for job, had_key in ((with_key, True), (without_key, False)):
        captured = await wf_conn.fetchrow(
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
        assert captured is not None, "the terminal mark's own fence matched"
        # THE PYTHON SURFACE: the capture no longer rides the terminal
        # SELECT at all (the columns are gone).
        assert "had_reservation" not in captured, dict(captured)
        assert "old_reservation" not in captured, dict(captured)
        detail = await wf_conn.fetchrow(
            f"""SELECT detail FROM "{wf_schema}".job_events
                WHERE job_id = $1 AND kind = 'state_change'""",
            job,
        )
        assert detail is not None
        raw = detail["detail"]
        doc = _json.loads(raw) if isinstance(raw, str) else dict(raw)
        if had_key:
            assert int(doc["released_emit_reservation_width"]) == 7, doc
        else:
            assert "released_emit_reservation_width" not in doc, doc
    live, reservations = await _observation(wf_conn, wf_sql, flow)
    assert (live, reservations) == (0, 0), "the charge moved exactly once, no double accounting"
    for job in (with_key, without_key):
        row = await wf_conn.fetchrow(
            f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1',
            job,
        )
        assert row is not None and row["status"] == "succeeded", row
        meta = _meta(row["metadata"])
        assert "wf_emit_reservation" not in meta, meta
