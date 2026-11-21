"""#717 diagnosis pins: a streaming record's identity is capped at 32767.

The record identity in a streaming chain is ``map_index``. The smallest
identity past the 16-bit domain (``SMALLINT_MAX + 1``) must be emittable and
the five columns that carry the identity — ``jobs``, ``jobs_archive``,
``wf_step_ledger``, ``wf_outbox``, ``wf_signals`` — must agree on a domain
wide enough to hold it. Before the repair the emit door refused that record
(``MapIndexExhaustedError``: ``MAP_INDEX_CEILING``) and every carrier was
``smallint``, so a long-lived source died terminally at record 32768. These
pins are the RCA's red→green gate and assert only that the widened contract
holds.
"""

# ruff: noqa: S608  # Why: the f-strings interpolate only the module fixture's throwaway schema identifier; every value is $-bound.

from __future__ import annotations

import asyncpg
import pytest

from taskq.workflows._emit import emit_batch
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import EmitChild
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

pytestmark = pytest.mark.integration

#: The smallest record identity the current 16-bit domain cannot hold.
FIRST_IDENTITY_PAST_SMALLINT = 32768

#: Every column that carries the record identity must support the same domain.
IDENTITY_CARRIERS = (
    "jobs",
    "jobs_archive",
    "wf_step_ledger",
    "wf_outbox",
    "wf_signals",
)


async def test_record_identity_32768_is_emitted_and_stored(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """A source emitting its 32,768th record must commit that record.

    The identity is the record's discriminator (the emit idempotency key and
    the ledger arbiter ride it), so the emit door cannot refuse it without
    killing the source's whole stream. This currently raises
    ``MapIndexExhaustedError`` before any row is written.
    """
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source_id)

    child_ids = await emit_batch(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=[
            EmitChild(
                step_key="record",
                actor="wf",
                queue="default",
                payload={"record": FIRST_IDENTITY_PAST_SMALLINT},
                trace_id=f"record-{FIRST_IDENTITY_PAST_SMALLINT}",
                map_index=FIRST_IDENTITY_PAST_SMALLINT,
            )
        ],
        cursor={"page": 1},
    )
    assert len(child_ids) == 1

    stored = await wf_conn.fetchval(
        f'SELECT map_index FROM "{wf_schema}".jobs WHERE id = $1',
        child_ids[0],
    )
    assert stored == FIRST_IDENTITY_PAST_SMALLINT, (
        f"the record's identity did not round-trip through jobs.map_index: {stored!r}"
    )


async def test_all_five_identity_carriers_hold_identity_past_smallint(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
) -> None:
    """A widened ``jobs`` alone is not permission to write a high identity.

    The archive, the claim ledger, the outbox and the signal holds all key on
    the same identity, so every carrier must share a domain that holds it.
    """
    rows = await wf_conn.fetch(
        """
        SELECT c.relname AS table_name,
               format_type(a.atttypid, a.atttypmod) AS type_name
        FROM pg_attribute AS a
        JOIN pg_class AS c ON c.oid = a.attrelid
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND a.attname = 'map_index'
          AND c.relname = ANY($2::text[])
        """,
        wf_schema,
        list(IDENTITY_CARRIERS),
    )
    carriers = {row["table_name"]: row["type_name"] for row in rows}
    assert set(carriers) == set(IDENTITY_CARRIERS), (
        f"identity carriers missing map_index: {sorted(set(IDENTITY_CARRIERS) - set(carriers))}"
    )
    narrowed = {name: domain for name, domain in carriers.items() if domain != "bigint"}
    assert not narrowed, (
        "record identity carriers still cannot hold an identity past 32767 "
        f"(expected bigint): {narrowed}"
    )
