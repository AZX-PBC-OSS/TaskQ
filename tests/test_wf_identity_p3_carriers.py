"""#717 P3 real high-identity paths for archive, outbox and signal siblings."""

# ruff: noqa: S608  # Why: only this fixture's generated schema identifier and the shipped frozen column-name tuple are interpolated; every value is $-bound.

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import JobId
from taskq.backend._sql_templates import COPY_FROM_COLUMNS
from taskq.workflows._emit import EMIT_MAX_IN_FLIGHT_DEFAULT, emit_batch
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import drain_outbox
from taskq.workflows._types import EmitChild
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

pytestmark = pytest.mark.integration


async def test_high_identity_archives_and_outbox_drains_once(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)
    ids = await emit_batch(
        wf_pool,
        wf_sql,
        flow_id=flow,
        source_id=source,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=(
            EmitChild(
                step_key="record",
                actor="wf",
                queue="default",
                payload={"record": 32768},
                trace_id="record-32768",
                map_index=32768,
            ),
        ),
        cursor={"page": 0},
        max_in_flight=EMIT_MAX_IN_FLIGHT_DEFAULT,
    )
    child = ids[0]
    child_worker = JobId(new_uuid())
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "
        "locked_by_worker = $2, lock_expires_at = now() + interval '1 minute', "
        "claim_epoch = 0 WHERE id = $1",
        child,
        child_worker,
    )
    claim = await wf_conn.fetchrow(
        wf_sql.ledger_claim, new_uuid(), flow, child, "record", 32768, 1, 0
    )
    assert claim is not None and claim["job_id"] == child
    applied = await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow,
        job_id=child,
        step_key="record",
        worker_id=child_worker,
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        result={"record": 32768},
        map_index=32768,
        ledger_id=JobId(claim["id"]),
        ledger_claimed_at=claim["updated_at"],
    )
    assert applied.applied

    columns = ", ".join(COPY_FROM_COLUMNS)
    archived = await wf_conn.fetchval(
        f'INSERT INTO "{wf_schema}".jobs_archive ({columns}, expire_at) '
        f"SELECT {columns}, now() + interval '1 day' FROM \"{wf_schema}\".jobs "
        "WHERE id = $1 RETURNING map_index",
        child,
    )
    assert archived == 32768

    outbox_id = new_uuid()
    await wf_conn.execute(
        wf_sql.outbox_insert,
        outbox_id,
        source,
        flow,
        "consumer-high",
        32768,
        dumps_jsonb_str(
            {
                "actor": "wf",
                "queue": "default",
                "payload": {"record": 32768},
                "trace_id": "record-32768",
            }
        ),
    )
    assert await drain_outbox(wf_pool, wf_sql) == 1
    consumer = await wf_conn.fetchrow(
        f'SELECT map_index, trace_id, idempotency_key, parent_id FROM "{wf_schema}".jobs '
        "WHERE step_key = 'consumer-high' AND (metadata->>'flow_id')::uuid = $1",
        flow,
    )
    assert consumer is not None
    assert consumer["map_index"] == 32768
    assert consumer["trace_id"] == "record-32768"
    assert consumer["parent_id"] == source
    assert consumer["idempotency_key"] == "wf:consumer-high:32768"
    delivered = await wf_conn.fetchval(
        f'SELECT delivered FROM "{wf_schema}".wf_outbox WHERE id = $1', outbox_id
    )
    assert delivered is True
    assert await drain_outbox(wf_pool, wf_sql) == 0
    assert (
        int(
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".jobs '
                "WHERE step_key = 'consumer-high' AND (metadata->>'flow_id')::uuid = $1",
                flow,
            )
            or 0
        )
        == 1
    )


async def test_32767_and_32768_signal_holds_are_independent(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
) -> None:
    flow = await seed_flow(wf_conn, wf_schema)
    ids: dict[int, JobId] = {}
    for index in (32767, 32768):
        hold = JobId(new_uuid())
        ids[index] = hold
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_signals '
            "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status, map_index) "
            "VALUES ($1, $2, 'wait', 'approve', $3, $4, 'held', $5)",
            hold,
            flow,
            index + 1,
            f"call-{index}",
            index,
        )
    held = await wf_conn.fetch(
        f'SELECT id, map_index, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY map_index",
        flow,
    )
    assert [row["map_index"] for row in held] == [32767, 32768]
    resolved = await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".wf_signals SET status = 'delivered', "
        "resolved_at = clock_timestamp() WHERE id = $1 AND status = 'held'",
        ids[32768],
    )
    assert resolved == "UPDATE 1"
    after = await wf_conn.fetch(
        f'SELECT map_index, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 ORDER BY map_index",
        flow,
    )
    assert [(row["map_index"], row["status"]) for row in after] == [
        (32767, "held"),
        (32768, "delivered"),
    ]

    # The new bigint expression still physically arbitrates an open sibling:
    # same map/epoch conflicts, a different map is independently addressable.
    with pytest.raises(asyncpg.UniqueViolationError):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_signals '
            "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status, map_index) "
            "VALUES ($1, $2, 'wait', 'approve', 32768, 'duplicate', 'held', 32767)",
            new_uuid(),
            flow,
        )
