"""Real-Postgres proof of the shared claim-owned attempt transition.

This is the primitive's contract, not a fleet/adoption certification: a
retried row and its ledger outcome commit together; cancellation and a stale
writer never re-pend; exhaustion waits for the engine terminal; machinery
crash remains separate from a body failure.
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: schema is the controlled fixture identifier; all values are parameter-bound.
import asyncio
import json
from typing import TYPE_CHECKING, Any, cast

import asyncpg
import pytest
import structlog

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows._types import NodeSpec
from taskq.workflows.api._attempt import AttemptClaim, repend_attempt
from taskq.workflows.api._runner_defer import WORKFLOW_CANCELLED_CLASS
from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE, render_sql
from taskq.workflows.engine import finalize_node, insert_node
from tests._wf_fixtures import seed_flow

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql


async def _claimed(
    conn: asyncpg.Connection, schema: str, wsql: WorkflowSql
) -> tuple[JobId, JobId, AttemptClaim]:
    flow_id = await seed_flow(conn, schema)
    node_id = await insert_node(
        conn,
        wsql,
        NodeSpec(
            step_key="work",
            actor="wf-node",
            queue="default",
            payload={},
            flow_id=flow_id,
        ),
    )
    worker = JobId(new_uuid())
    claimed = await conn.fetchrow(render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), node_id, worker)
    assert claimed is not None
    claimed_attempt = int(claimed["attempt"])
    claimed_epoch = int(claimed["claim_epoch"])
    ledger = await conn.fetchrow(
        wsql.ledger_claim,
        new_uuid(),
        flow_id,
        node_id,
        "work",
        None,
        claimed_attempt,
    )
    assert ledger is not None and ledger["updated_at"] is not None
    claim = AttemptClaim(
        job_id=node_id,
        flow_id=flow_id,
        step_key="work",
        worker_id=worker,
        attempt=claimed_attempt,
        claim_epoch=claimed_epoch,
        ledger_id=JobId(ledger["id"]),
        ledger_claimed_at=ledger["updated_at"],
        map_index=None,
    )
    return flow_id, node_id, claim


async def _row_and_ledger(
    conn: asyncpg.Connection, schema: str, claim: AttemptClaim
) -> tuple[dict[str, Any], dict[str, Any]]:
    row = await conn.fetchrow(
        f"SELECT status::text AS status, locked_by_worker, lock_expires_at, "
        f'attempt, claim_epoch, metadata, error_class FROM "{schema}".jobs WHERE id = $1',
        claim.job_id,
    )
    ledger = await conn.fetchrow(
        f"SELECT status::text AS status, result, error_class, error_message, "
        f'updated_at FROM "{schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    assert row is not None and ledger is not None
    return dict(row), dict(ledger)


def _metadata(row: dict[str, Any]) -> dict[str, object]:
    raw = row["metadata"]
    if isinstance(raw, str):
        raw = json.loads(raw)
    assert isinstance(raw, dict)
    return raw


class _DispositionInterposedConn:
    """Delegate to a real connection with one explicit race/fault hook."""

    def __init__(
        self,
        inner: asyncpg.Connection,
        *,
        remint: tuple[WorkflowSql, AttemptClaim] | None = None,
        refuse_fence_sql: str | None = None,
    ) -> None:
        self._inner = inner
        self._remint = remint
        self._refuse_fence_sql = refuse_fence_sql
        self.reminted_at: object = None
        self.fence_refused = False

    async def fetchrow(self, query: str, *args: object) -> asyncpg.Record | None:
        record = await self._inner.fetchrow(query, *args)
        if record is not None and "verdict" in record and record["verdict"] == "repended":
            if self._remint is None:
                raise AssertionError("unexpected repended verdict in this interposition")
            from taskq.workflows.ledger import claim_step_ledger

            wsql, claim = self._remint
            latest = await claim_step_ledger(
                self._inner,
                wsql,
                flow_id=claim.flow_id,
                job_id=claim.job_id,
                step_key=claim.step_key,
                map_index=claim.map_index,
                attempt=claim.attempt,
            )
            self.reminted_at = latest.claimed_at
        return record

    async def fetchval(self, query: str, *args: object) -> object:
        if query == self._refuse_fence_sql:
            self.fence_refused = True
            return None
        return await self._inner.fetchval(query, *args)


async def _assert_cancel_terminal_records(
    conn: asyncpg.Connection, schema: str, claim: AttemptClaim
) -> None:
    attempt = await conn.fetchrow(
        f'SELECT outcome, error_class FROM "{schema}".job_attempts '
        "WHERE job_id = $1 AND attempt = $2",
        claim.job_id,
        claim.attempt,
    )
    assert attempt is not None
    assert (attempt["outcome"], attempt["error_class"]) == (
        "cancelled",
        WORKFLOW_CANCELLED_CLASS,
    )
    event = await conn.fetchrow(
        f"SELECT detail FROM \"{schema}\".job_events WHERE job_id = $1 AND kind = 'state_change'",
        claim.job_id,
    )
    assert event is not None
    detail = event["detail"]
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert isinstance(detail, dict)
    assert detail["to_state"] == "cancelled"


async def _claim_second_attempt(
    conn: asyncpg.Connection, schema: str, claim: AttemptClaim
) -> tuple[int, int]:
    """Wait for the database due instant; never weaken the claim fence.

    The retry row is explicitly scheduled 50 ms ahead. Immediate re-claim
    is the wrong fixture assumption; report the persisted row if that
    deliberate delay cannot be observed within a bounded allowance.
    """
    deadline = asyncio.get_running_loop().time() + 2.0
    while True:
        result = await conn.fetchrow(
            render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), claim.job_id, claim.worker_id
        )
        if result is not None:
            # The claim's own granted pair — the retry's real epoch, never a
            # hardcoded zero.
            assert type(result["attempt"]) is int
            assert type(result["claim_epoch"]) is int
            return (int(result["attempt"]), int(result["claim_epoch"]))
        if asyncio.get_running_loop().time() >= deadline:
            state = await conn.fetchrow(
                f"SELECT status::text AS status, scheduled_at, locked_by_worker, "
                f'attempt, claim_epoch FROM "{schema}".jobs WHERE id = $1',
                claim.job_id,
            )
            pytest.fail(f"the scheduled retry never became claimable: {state!r}")
        await asyncio.sleep(0.01)


@pytest.mark.integration
async def test_body_retry_atomically_updates_row_and_current_ledger(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    async with wf_conn.transaction():
        outcome = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="SimulatedBodyError",
            error_message="first body failure",
            failures_before=0,
            max_attempts=3,
            retry_kind="transient",
            backoff_s=0.05,
        )
    assert outcome == "repended"
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert (row["status"], ledger["status"]) == ("scheduled", "failed")
    assert row["locked_by_worker"] is None and row["lock_expires_at"] is None
    assert ledger["error_class"] == "SimulatedBodyError"
    proof = _metadata(row)["wf_repend"]
    assert isinstance(proof, dict)
    first = proof.get("1")
    assert isinstance(first, dict) and first["backoff"] == 0.05

    # An actual second claim re-mints this worker's attempt. A different
    # logical attempt cannot accidentally overwrite the first ledger row.
    next_attempt, _next_epoch = await _claim_second_attempt(wf_conn, wf_schema, claim)
    assert next_attempt == 2
    new_ledger = await wf_conn.fetchrow(
        wf_sql.ledger_claim,
        new_uuid(),
        flow_id,
        claim.job_id,
        "work",
        None,
        next_attempt,
    )
    assert new_ledger is not None and new_ledger["id"] != claim.ledger_id
    assert new_ledger["status"] == "running"
    ledger_count = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1', flow_id
    )
    assert ledger_count == 2


@pytest.mark.integration
async def test_awaited_same_attempt_keeps_its_claim_for_retry_and_terminal(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """A lawful hold stamp is not a ledger re-mint or a fresh attempt."""
    from taskq.workflows.api._hitl import mark_awaited

    for exhausted in (False, True):
        flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
        assert claim.ledger_id is not None and claim.ledger_claimed_at is not None
        await mark_awaited(wf_conn, wf_schema, claim.ledger_id)
        _row, awaited = await _row_and_ledger(wf_conn, wf_schema, claim)
        assert awaited["status"] == "awaited"
        ledger_count = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1',
            flow_id,
        )
        assert ledger_count == 1

        async with wf_conn.transaction():
            outcome = await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="BodyFailedAfterAwait",
                error_message="the current awaited body retains its claim",
                failures_before=0,
                max_attempts=1 if exhausted else 3,
                retry_kind="transient",
                backoff_s=0.05,
            )

        if not exhausted:
            assert outcome == "repended"
            row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
            assert (row["status"], ledger["status"]) == ("scheduled", "failed")
        else:
            assert outcome == "exhausted"
            row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
            assert (row["status"], ledger["status"]) == ("running", "awaited")
            final = await finalize_node(
                wf_pool,
                wf_sql,
                flow_id=flow_id,
                job_id=claim.job_id,
                step_key=claim.step_key,
                worker_id=claim.worker_id,
                attempt=claim.attempt,
                claim_epoch=claim.claim_epoch,
                outcome="failed",
                error_class="BodyFailedAfterAwait",
                error_message="the current awaited body retains its claim",
                capture_policy="none",
                ledger_id=claim.ledger_id,
                ledger_claimed_at=claim.ledger_claimed_at,
            )
            assert final.applied
            row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
            assert (row["status"], ledger["status"]) == ("failed", "failed")
            attempt = await wf_conn.fetchrow(
                f'SELECT outcome, error_class FROM "{wf_schema}".job_attempts '
                "WHERE job_id = $1 AND attempt = $2",
                claim.job_id,
                claim.attempt,
            )
            assert attempt is not None
            assert (attempt["outcome"], attempt["error_class"]) == (
                "failed",
                "BodyFailedAfterAwait",
            )
            event = await wf_conn.fetchrow(
                f"SELECT detail->>'to_state' AS to_state FROM \"{wf_schema}\".job_events "
                "WHERE job_id = $1 AND kind = 'state_change'",
                claim.job_id,
            )
            assert event is not None and event["to_state"] == "failed"

        assert awaited["updated_at"] == claim.ledger_claimed_at
        ledger_count = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1',
            flow_id,
        )
        assert ledger_count == 1


@pytest.mark.integration
async def test_operator_cancel_outranks_an_ordinary_body_failure(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET status = $2 WHERE id = $1',
        flow_id,
        "cancelled",
    )
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',
        claim.job_id,
        json.dumps({"cancel_phase": "cooperative"}),
    )
    async with wf_conn.transaction():
        outcome = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="BodyRaisedAfterCancel",
            error_message="must be cancelled, not a body failure ladder",
            failures_before=2,
            max_attempts=2,
            retry_kind="permanent",
            backoff_s=0.05,
        )
    assert outcome == "cancel_fenced"
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert (row["status"], ledger["status"]) == ("cancelled", "cancelled")
    assert row["attempt"] == 1, "operator cancel does not create a fresh claim attempt"
    assert row["error_class"] == WORKFLOW_CANCELLED_CLASS
    assert ledger["error_class"] == WORKFLOW_CANCELLED_CLASS
    assert "cancel_phase" in _metadata(row)
    await _assert_cancel_terminal_records(wf_conn, wf_schema, claim)


@pytest.mark.integration
async def test_vanilla_cancel_column_outranks_the_retry_ladder(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    changed = await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET cancel_phase = 1 WHERE id = $1',
        claim.job_id,
    )
    assert changed == "UPDATE 1"
    async with wf_conn.transaction():
        outcome = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="BodyFailedAfterVanillaCancel",
            error_message="the column carrier must win the failure budget",
            failures_before=2,
            max_attempts=2,
            retry_kind="permanent",
            backoff_s=0.05,
        )
    assert outcome == "cancel_fenced"
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert (row["status"], ledger["status"]) == ("cancelled", "cancelled")
    assert row["attempt"] == claim.attempt
    assert row["error_class"] == WORKFLOW_CANCELLED_CLASS
    await _assert_cancel_terminal_records(wf_conn, wf_schema, claim)


@pytest.mark.integration
async def test_stale_claim_fences_its_ledger_without_touching_the_winner(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET claim_epoch = claim_epoch + 1 WHERE id = $1',
        claim.job_id,
    )
    async with wf_conn.transaction():
        outcome = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="StaleWriter",
            error_message="old owner must not overwrite the newer epoch",
            failures_before=0,
            max_attempts=3,
            retry_kind="transient",
            backoff_s=0.05,
        )
    assert outcome == "stale"
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert row["status"] == "running" and row["claim_epoch"] == claim.claim_epoch + 1
    assert row["locked_by_worker"] == claim.worker_id
    assert ledger["status"] == "fenced"
    assert ledger["error_class"] == "StaleWorkflowClaim"
    assert "wf_repend" not in _metadata(row)


@pytest.mark.integration
async def test_a_stale_claim_after_ledger_remint_is_a_safe_noop(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    from taskq.workflows.ledger import claim_step_ledger

    flow_id, node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    latest = await claim_step_ledger(
        wf_conn,
        wf_sql,
        flow_id=flow_id,
        job_id=node_id,
        step_key=claim.step_key,
        map_index=claim.map_index,
        attempt=claim.attempt,
    )
    assert latest.ledger_id == claim.ledger_id
    assert latest.claimed_at is not None
    assert latest.claimed_at != claim.ledger_claimed_at, (
        "a re-mint must have a new claim instant for this stale-writer proof"
    )
    async with wf_conn.transaction():
        outcome = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="OldAttempt",
            error_message="the old claim cannot re-pend a re-minted ledger",
            failures_before=0,
            max_attempts=3,
            retry_kind="transient",
            backoff_s=0.05,
        )
    assert outcome == "stale"
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert row["status"] == "running"
    assert ledger["status"] == "running"
    assert ledger["updated_at"] == latest.claimed_at
    assert "wf_repend" not in _metadata(row)


@pytest.mark.integration
async def test_repended_ledger_remint_raises_and_rolls_back_everything(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    proxied = _DispositionInterposedConn(wf_conn, remint=(wf_sql, claim))
    with pytest.raises(RuntimeError, match="lost its own ledger claim"):
        async with wf_conn.transaction():
            await repend_attempt(
                cast("ConnLike", proxied),
                wf_sql,
                claim=claim,
                error_class="RaceBetweenVerdictAndLedger",
                error_message="the protected write must fail and roll back",
                failures_before=0,
                max_attempts=3,
                retry_kind="transient",
                backoff_s=0.05,
            )
    assert proxied.reminted_at is not None
    assert proxied.reminted_at != claim.ledger_claimed_at
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert row["status"] == "running"
    assert "wf_repend" not in _metadata(row)
    # PostgreSQL rolls back both the attempted repend and the interposed
    # same-transaction re-mint; the original claim instant is restored.
    assert ledger["status"] == "running"
    assert ledger["updated_at"] == claim.ledger_claimed_at


@pytest.mark.integration
async def test_live_current_claim_fence_refusal_stays_loud(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET claim_epoch = claim_epoch + 1 WHERE id = $1',
        claim.job_id,
    )
    proxied = _DispositionInterposedConn(
        wf_conn,
        refuse_fence_sql=wf_sql.ledger_fence_by_id,
    )
    with pytest.raises(RuntimeError, match="live current ledger claim"):
        async with wf_conn.transaction():
            await repend_attempt(
                cast("ConnLike", proxied),
                wf_sql,
                claim=claim,
                error_class="CurrentClaimFenceRefused",
                error_message="an impossible same-instant refusal stays loud",
                failures_before=0,
                max_attempts=3,
                retry_kind="transient",
                backoff_s=0.05,
            )
    assert proxied.fence_refused
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert row["status"] == "running" and row["claim_epoch"] == claim.claim_epoch + 1
    assert ledger["status"] == "running"
    assert ledger["updated_at"] == claim.ledger_claimed_at
    assert "wf_repend" not in _metadata(row)


@pytest.mark.integration
async def test_retention_deleted_ledger_is_an_observable_stale_noop(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    deleted = await wf_conn.execute(
        f'DELETE FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    assert deleted == "DELETE 1"
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET claim_epoch = claim_epoch + 1 WHERE id = $1',
        claim.job_id,
    )
    with structlog.testing.capture_logs() as logs:
        async with wf_conn.transaction():
            outcome = await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="RetainedClaimDisappeared",
                error_message="retention removes only an already-stale record",
                failures_before=0,
                max_attempts=3,
                retry_kind="transient",
                backoff_s=0.05,
            )
    assert outcome == "stale"
    assert any(log["event"] == "workflow-attempt-stale-missing-ledger" for log in logs)
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, claim_epoch, metadata FROM "{wf_schema}".jobs '
        "WHERE id = $1",
        claim.job_id,
    )
    assert row is not None
    assert row["status"] == "running" and row["claim_epoch"] == claim.claim_epoch + 1
    assert "wf_repend" not in _metadata(dict(row))
    remaining = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    assert remaining == 0


@pytest.mark.integration
async def test_permanent_and_exhausted_wait_for_the_engine_terminal(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    for retry_kind in ("permanent", "transient"):
        _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
        async with wf_conn.transaction():
            outcome = await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="RejectedInput",
                error_message="terminal policy failure",
                failures_before=0,
                max_attempts=1,
                retry_kind=retry_kind,
                backoff_s=0.05,
            )
        assert outcome == "exhausted"
        row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
        assert row["status"] == "running"
        assert ledger["status"] == "running", "no ledger terminal before terminal tx1"

        final = await finalize_node(
            wf_pool,
            wf_sql,
            flow_id=claim.flow_id,
            job_id=claim.job_id,
            step_key=claim.step_key,
            worker_id=claim.worker_id,
            attempt=claim.attempt,
            claim_epoch=claim.claim_epoch,
            outcome="failed",
            error_class="RejectedInput",
            error_message="terminal policy failure",
            capture_policy="none",
            ledger_id=claim.ledger_id,
            ledger_claimed_at=claim.ledger_claimed_at,
        )
        assert final.applied
        row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
        assert (row["status"], ledger["status"]) == ("failed", "failed")


@pytest.mark.integration
async def test_machinery_crash_never_spends_the_body_failure_ladder(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    async with wf_conn.transaction():
        outcome = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="SimulatedTransportLoss",
            error_message="the runner machinery lost its connection",
            failures_before=99,
            max_attempts=1,
            retry_kind="non_retryable",
            backoff_s=0.05,
            machinery_crashed=True,
        )
    assert outcome == "repended"
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert (row["status"], ledger["status"]) == ("scheduled", "crashed")
    failed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'failed'",
        claim.flow_id,
    )
    assert failed == 0


@pytest.mark.integration
async def test_disposition_requires_a_real_claim_identity(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    _flow_id, _node_id, claim = await _claimed(wf_conn, wf_schema, wf_sql)
    missing = AttemptClaim(
        job_id=claim.job_id,
        flow_id=claim.flow_id,
        step_key=claim.step_key,
        worker_id=claim.worker_id,
        attempt=claim.attempt,
        claim_epoch=claim.claim_epoch,
        ledger_id=None,
        ledger_claimed_at=None,
        map_index=None,
    )
    with pytest.raises(RuntimeError, match="claim-owned ledger id"):
        await repend_attempt(
            wf_conn,
            wf_sql,
            claim=missing,
            error_class="NeverWrite",
            error_message="missing ownership must fail before any write",
            failures_before=0,
            max_attempts=1,
            retry_kind="transient",
            backoff_s=0.05,
        )
    row, ledger = await _row_and_ledger(wf_conn, wf_schema, claim)
    assert (row["status"], ledger["status"]) == ("running", "running")
