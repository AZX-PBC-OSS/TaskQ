"""Real-Postgres pins for chain-source logical attempts and bounded repend diagnostics."""

from __future__ import annotations

# ruff: noqa: S608  # Why: schema is the controlled fixture identifier; all values are parameter-bound.
import json
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    step,
)
from taskq.workflows._types import NodeSpec
from taskq.workflows.api._attempt import AttemptClaim, repend_attempt
from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE, render_sql
from taskq.workflows.engine import insert_node
from tests._wf_fixtures import seed_flow

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql


async def _unused_chain_step(ctx: StepContext, item: object) -> object:
    return item


def _empty_chain(name: str) -> Chain:
    return Chain(
        name=name,
        start="unused",
        steps={"unused": Step(body=_unused_chain_step, outcomes=object)},
    )


async def _claimed(
    conn: asyncpg.Connection,
    schema: str,
    wsql: WorkflowSql,
    *,
    metadata: dict[str, object] | None = None,
    claim_statement: str | None = None,
) -> tuple[JobId, AttemptClaim]:
    """Seed a flow, claim its source node and ledger row.

    ``claim_statement`` is the era's own rendered ledger-claim statement for
    the mixed-version controls: a pre-epoch schema cannot take the current
    seven-bind claim, so those controls pass the era's six-bind form and the
    epoch bind is dropped (the JOB row's granted epoch — the fence's truth —
    still comes from the claim template's own RETURNING)."""
    flow_id = await seed_flow(conn, schema)
    node_id = await insert_node(
        conn,
        wsql,
        NodeSpec(
            step_key="source",
            actor="wf-node",
            queue="default",
            payload={},
            flow_id=flow_id,
        ),
    )
    if metadata is not None:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',
            node_id,
            json.dumps(metadata),
        )
    worker = JobId(new_uuid())
    claimed = await conn.fetchrow(render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), node_id, worker)
    assert claimed is not None
    claimed_attempt = int(claimed["attempt"])
    # The claim template returns the claim's own granted epoch (it
    # increments per claim): the fence epoch is the JOB ROW's truth on
    # every schema that carries the column (01.00.18+).
    granted_epoch = int(claimed["claim_epoch"])
    binds: list[object] = [
        new_uuid(),
        flow_id,
        node_id,
        "source",
        None,
        claimed_attempt,
    ]
    ledger = await conn.fetchrow(
        claim_statement if claim_statement is not None else wsql.ledger_claim,
        *binds,
    )
    assert ledger is not None and ledger["updated_at"] is not None
    return flow_id, AttemptClaim(
        job_id=node_id,
        flow_id=flow_id,
        step_key="source",
        worker_id=worker,
        attempt=claimed_attempt,
        claim_epoch=granted_epoch,
        ledger_id=JobId(ledger["id"]),
        ledger_claimed_at=ledger["updated_at"],
        map_index=None,
    )


async def _next_claim(
    conn: asyncpg.Connection,
    schema: str,
    wsql: WorkflowSql,
    claim: AttemptClaim,
) -> AttemptClaim:
    """Make the real scheduled row due, then take its real next physical claim."""
    changed = await conn.execute(
        f"UPDATE \"{schema}\".jobs SET scheduled_at = now() WHERE id = $1 AND status = 'scheduled'",
        claim.job_id,
    )
    assert changed == "UPDATE 1"
    claimed = await conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), claim.job_id, claim.worker_id
    )
    assert claimed is not None
    claimed_attempt = int(claimed["attempt"])
    claimed_epoch = int(claimed["claim_epoch"])
    ledger = await conn.fetchrow(
        wsql.ledger_claim,
        new_uuid(),
        claim.flow_id,
        claim.job_id,
        claim.step_key,
        claim.map_index,
        claimed_attempt,
    )
    assert ledger is not None and ledger["updated_at"] is not None
    return AttemptClaim(
        job_id=claim.job_id,
        flow_id=claim.flow_id,
        step_key=claim.step_key,
        worker_id=claim.worker_id,
        attempt=claimed_attempt,
        claim_epoch=claimed_epoch,
        ledger_id=JobId(ledger["id"]),
        ledger_claimed_at=ledger["updated_at"],
        map_index=claim.map_index,
    )


def _metadata_value(raw: object) -> object:
    return json.loads(raw) if isinstance(raw, str) else raw


@pytest.mark.integration
async def test_source_body_retry_and_exhaustion_advance_durable_attempt_once(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    suffix = new_uuid().hex
    observed_context_attempts: list[int] = []

    async def failing_source(ctx: StepContext) -> None:
        observed_context_attempts.append(ctx.attempt)
        raise RuntimeError("synthetic source body failure")

    app = WorkflowApp()
    chain = _empty_chain(f"source-attempt-chain-{suffix}")

    @app.workflow(f"source_attempt_{suffix}")
    def workflow() -> Promise[object]:
        return build(chain_source(chain, failing_source, key="source", max_attempts=2))

    runner = FlowRunner(app.get(f"source_attempt_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)
    first_row = await wf_conn.fetchrow(
        f'SELECT status, attempt, metadata FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
        flow_id,
    )
    assert first_row is not None
    first_metadata = _metadata_value(first_row["metadata"])
    assert isinstance(first_metadata, dict)
    assert (first_row["status"], first_row["attempt"]) == ("scheduled", 1)
    assert first_metadata["wf_source_logical_attempt"] == 2
    assert observed_context_attempts == [1]

    assert await runner.drive(flow_id) == "terminal"
    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, metadata FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
        flow_id,
    )
    assert row is not None and row["status"] == "failed"
    metadata = _metadata_value(row["metadata"])
    assert isinstance(metadata, dict)
    assert metadata.get("wf_source_logical_attempt") == 3, (
        "two genuine source body failures must materialize attempt 1 and advance it "
        f"exactly once each, including exhaustion; observed metadata={metadata!r}"
    )
    assert row["attempt"] == 2, "the existing physical attempt surface is unchanged"
    assert observed_context_attempts == [1, 2], (
        "without physical backpressure refund, public and physical attempts agree"
    )
    failed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'source' AND status = 'failed'",
        flow_id,
    )
    assert failed == 2


@pytest.mark.integration
@pytest.mark.parametrize("bad", [None, True, 1.0, "1", 0, -1, [], {}])
async def test_malformed_source_attempt_fails_the_active_write_closed(
    bad: object,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    suffix = new_uuid().hex

    async def failing_source(ctx: StepContext) -> None:
        raise RuntimeError("must not silently repair malformed attempt metadata")

    app = WorkflowApp()
    chain = _empty_chain(f"bad-source-attempt-chain-{suffix}")

    @app.workflow(f"bad_source_attempt_{suffix}")
    def workflow() -> Promise[object]:
        return build(chain_source(chain, failing_source, key="source"))

    runner = FlowRunner(app.get(f"bad_source_attempt_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
        flow_id,
    )
    assert source_id is not None
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',
        source_id,
        json.dumps({"wf_source_logical_attempt": bad}),
    )
    with pytest.raises(ValueError, match="wf_source_logical_attempt"):
        await runner.tick(flow_id)

    row = await wf_conn.fetchrow(
        f"SELECT status, attempt, metadata, "
        f"(metadata->'wf_source_logical_attempt' = $2::jsonb) AS unchanged "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        source_id,
        json.dumps(bad),
    )
    assert row is not None
    assert (row["status"], row["attempt"], row["unchanged"]) == ("running", 1, True)
    metadata = _metadata_value(row["metadata"])
    assert isinstance(metadata, dict) and "wf_repend" not in metadata
    ledger = await wf_conn.fetchrow(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE job_id = $1',
        source_id,
    )
    assert ledger is not None and ledger["status"] == "running"


@pytest.mark.integration
async def test_static_retry_never_owns_or_validates_the_source_carrier(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    suffix = new_uuid().hex
    observed_context_attempts: list[int] = []

    async def failing_static(ctx: StepContext) -> None:
        observed_context_attempts.append(ctx.attempt)
        raise RuntimeError("ordinary static body failure")

    app = WorkflowApp()

    @app.workflow(f"static_attempt_control_{suffix}")
    def workflow() -> Promise[object]:
        return build(step(failing_static, key="work", max_attempts=2))

    runner = FlowRunner(app.get(f"static_attempt_control_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    node_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'work'",
        flow_id,
    )
    assert node_id is not None
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',
        node_id,
        json.dumps({"wf_source_logical_attempt": True}),
    )
    assert await runner.drive(flow_id) == "terminal"
    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, metadata FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert row is not None
    assert (row["status"], row["attempt"]) == ("failed", 2)
    assert observed_context_attempts == [1, 2]
    metadata = _metadata_value(row["metadata"])
    assert isinstance(metadata, dict)
    assert metadata["wf_source_logical_attempt"] is True


@pytest.mark.integration
async def test_repend_diagnostics_cap_real_disposals_without_losing_ledger_records(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    cap = 8
    disposals = cap + 3
    _flow_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    for ordinal in range(disposals):
        async with wf_conn.transaction():
            verdict = await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="SyntheticInfrastructureLoss",
                error_message=f"machinery disposal {ordinal + 1}",
                failures_before=0,
                max_attempts=1,
                retry_kind="non_retryable",
                backoff_s=0.0,
                machinery_crashed=True,
            )
        assert verdict == "repended"
        if ordinal + 1 < disposals:
            claim = await _next_claim(wf_conn, wf_schema, wf_sql, claim)

    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        claim.job_id,
    )
    assert row is not None
    assert (row["status"], row["attempt"]) == ("scheduled", disposals)
    metadata = _metadata_value(row["metadata"])
    assert isinstance(metadata, dict)
    history = metadata.get("wf_repend")
    assert isinstance(history, dict)
    assert len(history) == cap, (
        f"{disposals} real machinery disposals must leave only {cap} recent metadata "
        f"diagnostics, not {len(history)}"
    )
    assert set(history) == {str(value) for value in range(disposals - cap + 1, disposals + 1)}
    assert history[str(disposals)]["backoff"] == 0.0
    assert metadata.get("wf_repend_folded") == disposals - cap
    crashed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE job_id = $1 AND status = 'crashed'",
        claim.job_id,
    )
    assert crashed == disposals, "the complete ledger history remains authoritative"
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_attempts WHERE job_id = $1', claim.job_id
        )
        == 0
    )
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_events WHERE job_id = $1', claim.job_id
        )
        == 0
    )


@pytest.mark.integration
async def test_source_infra_reclaim_preserves_attempt_and_bounded_history(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    cap = 8
    disposals = cap + 4
    _flow_id, claim = await _claimed(
        wf_conn,
        wf_schema,
        wf_sql,
        metadata={"wf_source_logical_attempt": 1},
    )
    for ordinal in range(disposals):
        async with wf_conn.transaction():
            verdict = await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="SyntheticSourceInfrastructureLoss",
                error_message=f"source machinery disposal {ordinal + 1}",
                failures_before=0,
                max_attempts=1,
                retry_kind="non_retryable",
                backoff_s=0.0,
                machinery_crashed=True,
                chain_source=True,
            )
        assert verdict == "repended"
        carrier = await wf_conn.fetchval(
            f"SELECT metadata->'wf_source_logical_attempt' FROM \"{wf_schema}\".jobs WHERE id = $1",
            claim.job_id,
        )
        assert _metadata_value(carrier) == 1
        if ordinal + 1 < disposals:
            claim = await _next_claim(wf_conn, wf_schema, wf_sql, claim)

    metadata_raw = await wf_conn.fetchval(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', claim.job_id
    )
    metadata = _metadata_value(metadata_raw)
    assert isinstance(metadata, dict)
    history = metadata.get("wf_repend")
    assert isinstance(history, dict)
    assert len(history) == cap
    assert set(history) == {str(value) for value in range(disposals - cap + 1, disposals + 1)}
    assert metadata["wf_repend_folded"] == disposals - cap
    assert metadata["wf_source_logical_attempt"] == 1
    crashed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE job_id = $1 AND status = 'crashed'",
        claim.job_id,
    )
    assert crashed == disposals


@pytest.mark.integration
async def test_legacy_source_materializes_physical_fallback_then_uses_only_carrier(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    # Simulate a source claimed under the old binary: its second physical
    # claim exists without the new carrier. This is the sole missing-key path.
    async with wf_conn.transaction():
        assert (
            await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="LegacyInfrastructureLoss",
                error_message="legacy physical recovery",
                failures_before=0,
                max_attempts=1,
                retry_kind="transient",
                backoff_s=0.0,
                machinery_crashed=True,
            )
            == "repended"
        )
    claim = await _next_claim(wf_conn, wf_schema, wf_sql, claim)
    assert claim.attempt == 2
    async with wf_conn.transaction():
        assert (
            await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="FirstNewBinaryReclaim",
                error_message="materialize legacy current ordinal",
                failures_before=0,
                max_attempts=1,
                retry_kind="transient",
                backoff_s=0.0,
                machinery_crashed=True,
                chain_source=True,
            )
            == "repended"
        )
    materialized = await wf_conn.fetchval(
        f"SELECT metadata->'wf_source_logical_attempt' FROM \"{wf_schema}\".jobs WHERE id = $1",
        claim.job_id,
    )
    assert _metadata_value(materialized) == 2

    claim = await _next_claim(wf_conn, wf_schema, wf_sql, claim)
    assert claim.attempt == 3
    async with wf_conn.transaction():
        assert (
            await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="GenuineSourceFailure",
                error_message="advance the durable carrier once",
                failures_before=99,
                max_attempts=101,
                retry_kind="transient",
                backoff_s=0.0,
                chain_source=True,
            )
            == "repended"
        )
    advanced = await wf_conn.fetchrow(
        f"SELECT metadata->'wf_source_logical_attempt' AS logical_attempt, attempt "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        claim.job_id,
    )
    assert advanced is not None
    assert (_metadata_value(advanced["logical_attempt"]), advanced["attempt"]) == (3, 3)
