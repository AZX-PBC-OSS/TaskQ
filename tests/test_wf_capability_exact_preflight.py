# Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The D4 capability-preflight negative controls: the catalog preflight
compares EXACT definitions (``pg_get_indexdef``, normalized, full
equality) — never substrings.

The defect this control pins: the preflight's arbiter check was a
five-NAME substring scan and the park-index check a three-token
substring scan, so a WRONG same-name index — a five-expression arbiter
whose claim then fails 42P10 at the ON CONFLICT insert, or a
wrong-column park index whose probe then plans a Seq Scan on jobs —
was accepted by the preflight and failed only later, at feature use,
in the claim's or the probe's own path. A same-name wrong object is
exactly the shape ``CREATE INDEX IF NOT EXISTS`` cannot detect; the
preflight's exact-definition equality is the enforcement point.

Each control: apply the real folded migration to a fresh schema,
replace one index with a plausible wrong definition, reset the
capability cache (the per-schema verdict is a process-lifetime fact
about the catalog's SHAPE — a replaced object must re-verify), and
assert the REAL ``verify_emit_capacity_objects`` refuses with the
typed capability error. The positive arm re-asserts the migrated
schema's own definitions pass, so the constants cannot drift away from
PostgreSQL's real ``pg_get_indexdef`` output format without a red.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq.migrate import apply_pending
from taskq.workflows._capability import (
    WorkflowWorkerCapabilityError,
    reset_emit_capability_cache,
    verify_emit_capacity_objects,
)
from taskq.workflows.engine import render_workflow_sql

pytestmark = pytest.mark.integration


async def _fresh_migrated(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await apply_pending(conn, schema=schema)


async def test_preflight_accepts_the_real_migrated_definitions_exactly(
    pg_dsn: str,
) -> None:
    """The positive arm: the real migration's own object set satisfies the
    exact-definition preflight — the constants track PostgreSQL's real
    ``pg_get_indexdef`` output format (a format drift reds here)."""
    schema = "cap4_positive_probe"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        reset_emit_capability_cache(schema)
        wsql = render_workflow_sql(schema)
        await verify_emit_capacity_objects(conn, schema)
        # The cache took the verdict (a second check is the cached path).
        await verify_emit_capacity_objects(conn, schema)
        assert wsql is not None
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_wrong_same_name_arbiter(pg_dsn: str) -> None:
    """A five-expression arbiter that carries every arbiter NAME but
    drops ``claim_epoch``'s unique enforcement (a COALESCE-wrapped
    ``claim_epoch + 0`` — the review's own wrong shape) must refuse
    BEFORE any feature use: the real claim's ON CONFLICT would fail
    42P10 against it."""
    schema = "cap4_wrong_arbiter"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(f'DROP INDEX "{schema}".wf_step_ledger_claim_uniq')
        await conn.execute(
            f'CREATE UNIQUE INDEX wf_step_ledger_claim_uniq ON "{schema}".wf_step_ledger '
            "USING btree (flow_id, step_key, COALESCE((map_index)::integer, '-1'::integer), "
            "attempt, (claim_epoch + 0))"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_step_ledger_claim_uniq"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_wrong_same_name_park_index(pg_dsn: str) -> None:
    """A same-name park index over the wrong key columns (every
    substring token present: the review's own wrong shape) must refuse —
    its probe planned a Seq Scan on jobs and its candidate fell back to
    the flow-nodes index."""
    schema = "cap4_wrong_park_idx"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(f'DROP INDEX "{schema}".jobs_wf_emit_park_idx')
        await conn.execute(
            f'CREATE INDEX jobs_wf_emit_park_idx ON "{schema}".jobs '
            "USING btree (((metadata->>'flow_id')::uuid), created_at, id, step_key) "
            f"WHERE status = 'scheduled'::\"{schema}\".job_status "
            'AND (metadata @> \'{"blocking_reason": "backpressure"}\'::jsonb) '
            'AND (metadata @> \'{"wf_deferral": {"signal": "EmitBackpressure", '
            '"attempt": 0, "consume_budget": false}}\'::jsonb) '
            'AND (metadata @> \'{"wf_emit_backpressure": {"version": 1}}\'::jsonb) '
            "AND (step_key IS NOT NULL) AND (step_key <> '__flow__'::text) "
            "AND (metadata ? 'flow_id'::text)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="jobs_wf_emit_park_idx"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_wrong_same_name_pending_index(pg_dsn: str) -> None:
    """A same-name pending index over the wrong predicate must refuse."""
    schema = "cap4_wrong_pending_idx"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(f'DROP INDEX "{schema}".wf_emit_capacity_pending_idx')
        await conn.execute(
            f'CREATE INDEX wf_emit_capacity_pending_idx ON "{schema}".wf_emit_capacity '
            "USING btree (flow_id) WHERE (wake_generation >= 0)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_emit_capacity_pending_idx"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
