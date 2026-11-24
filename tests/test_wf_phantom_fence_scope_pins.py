"""The phantom reaper fences ledger rows — not every running record claim.

The crash lane (``tests/system_e2e/test_wf_identity_boundary_kill.py``) used
to equate the fenced ``wf_step_ledger`` ids with *every* ``running`` record
row the killed worker held. That is not the reaper's contract: the worker's
producer claims rows ``running`` (``taskq.worker.run``'s ``dispatch_batch``
round) before the consumer writes the workflow ledger claim
(``taskq.workflows.api._runner``'s ``claim_step_ledger``), so a corpse can
hold a claimed-but-unledgered record. ``PHANTOM_REAP_SQL`` updates
``wf_step_ledger`` only — such a row has no phantom to fence.

This pin holds the product side of that asymmetry: with one ledger-claimed
and one claimed-but-unledgered record row on a terminal flow, the reaper
fences exactly the ledger-claimed row. The lane's sound invariant is therefore
``fenced_ids == the corpse's ledger-claimed subset``, not ``== corpse_records``.
"""

# ruff: noqa: S608  # Why: every query's schema identifier is the fixture-validated module schema and every value is $-bound; only the schema interpolation is flagged.

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows._sweep import reap_phantom_ledger
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import seed_flow, seed_running_node

pytestmark = pytest.mark.integration


async def test_reaper_fences_only_the_ledger_claimed_corpse_row(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    conn = await asyncpg.connect(pg_dsn)
    try:
        wsql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema, status="cancelled")
        # The dispatch claim -> consumer ledger-claim window: running, no
        # wf_step_ledger row yet.
        unledgered = await seed_running_node(conn, schema, flow, step_key="record")
        # The body has started: running and ledger-claimed.
        ledgered = await seed_running_node(conn, schema, flow, step_key="record")
        # Expire both leases so the reaper's live-claim exclusion hides
        # neither row (the crash lane's corpse is past its lease by the time
        # the terminal sweep runs).
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET lock_expires_at = now() - interval '1 minute' "
            "WHERE id = ANY($1::uuid[])",
            [unledgered, ledgered],
        )
        await conn.execute(
            f'INSERT INTO "{schema}".wf_step_ledger '
            "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status) "
            "VALUES ($1, $2, $3, 'record', 1, 1, 0, 'running')",
            new_uuid(),
            flow,
            ledgered,
        )

        corpse_records = {
            str(row["id"])
            for row in await conn.fetch(
                f"SELECT id FROM \"{schema}\".jobs WHERE step_key = $1 AND status = 'running'",
                "record",
            )
        }
        assert corpse_records == {str(unledgered), str(ledgered)}, corpse_records

        reaped = await reap_phantom_ledger(pool, wsql)
        fenced = {
            str(row["job_id"])
            for row in await conn.fetch(
                f"""SELECT job_id FROM "{schema}".wf_step_ledger
                    WHERE flow_id = $1 AND step_key = 'record' AND status = 'fenced'""",
                flow,
            )
        }

        assert reaped == 1, reaped
        assert fenced == {str(ledgered)}, (
            "the reaper did not fence exactly the ledger-claimed corpse row: "
            f"fenced={sorted(fenced)} ledgered={ledgered}"
        )
        assert str(unledgered) not in fenced, (
            "the reaper fenced a claimed-but-unledgered row — the ledger "
            "claim window no longer protects the producer's claim"
        )
    finally:
        await conn.close()
        await pool.close()
