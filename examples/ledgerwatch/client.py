"""LedgerWatch's client face: trigger runs, await terminals.

The trigger is IDEMPOTENT by the run-key arbiter: the run key is the
batch id — an at-least-once feed's re-delivery gets the FIRST run's
typed claim (``existing-running`` / ``existing-terminal``), never a
second silent run.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import asyncpg

from examples.ledgerwatch.app import app
from examples.ledgerwatch.models import Batch, ReconReport
from examples.ledgerwatch.pooling import LazyPool
from examples.ledgerwatch.store import bootstrap_conn
from taskq.workflows import FlowRunner, HitlClient, RunClaim

_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})


async def submit_batch(
    pool: asyncpg.Pool, schema: str, batch: Batch, *, run_key: str | None = None
) -> RunClaim:
    """Trigger one batch's run. The run key IS the batch id (unless the
    caller overrides it): re-delivery is a no-op by construction."""
    runner = FlowRunner(app.get("ledger_watch"), pool, schema)
    return await runner.create_flow(
        input=batch, run_key=run_key or f"ledger_watch:{batch.batch_id}"
    )


async def submit_reconciliation(
    pool: asyncpg.Pool, schema: str, *, run_key: str | None = None
) -> RunClaim:
    """Trigger one scheduled reconciliation pass (the timer's arm)."""
    runner = FlowRunner(app.get("reconciliation"), pool, schema)
    return await runner.create_flow(run_key=run_key or "ledger_watch:reconciliation")


async def await_terminal(
    pool: asyncpg.Pool,
    schema: str,
    flow_id: str,
    *,
    workflow: str = "ledger_watch",
    timeout_s: float = 300.0,
) -> Any:
    """Wait (from OUTSIDE the fleet) for a run to terminalize, then read
    its decoded result.

    This is the CLIENT's wait: the fleet's workers execute the rows; we
    poll the root row's status and read through ``FlowRunner.result``
    (the typed failure face — a failed run raises the
    ``WorkflowRunError`` carrying the failing node's class + message).
    """
    runner = FlowRunner(app.get(workflow), pool, schema)
    async with asyncio.timeout(timeout_s):
        while True:
            status = await pool.fetchval(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema identifier only; every caller value is bound.
                flow_id,
            )
            if status in _TERMINAL:
                break
            await asyncio.sleep(0.25)
    return await runner.result(flow_id)


async def await_terminal_report(
    pool: asyncpg.Pool, schema: str, flow_id: str, *, timeout_s: float = 300.0
) -> Any:
    """``await_terminal`` for a ledger_watch run (the typed receipt)."""
    return await await_terminal(pool, schema, flow_id, timeout_s=timeout_s)


async def await_recon_report(
    pool: asyncpg.Pool, schema: str, flow_id: str, *, timeout_s: float = 300.0
) -> ReconReport:
    raw = await await_terminal(
        pool, schema, flow_id, workflow="reconciliation", timeout_s=timeout_s
    )
    return ReconReport.model_validate(raw)


async def resolve_one_hold(
    pool: asyncpg.Pool, schema: str, flow_id: str, decision: dict[str, object]
) -> str:
    """The operator's reply: resolve the run's ONE pending hold (the
    typed door — a wrong shape is the typed refusal and the hold
    survives)."""
    client = HitlClient(pool, schema=schema)
    holds = await client.list(flow_id)
    if not holds:
        raise RuntimeError(f"run {flow_id} holds nothing")
    result = await client.resolve(holds[0].hold_id, decision)
    return result.status


async def bootstrap_schema(dsn: str, schema: str) -> None:
    """Migrations + the app's tables (the trigger's first-run duty)."""
    import taskq.migrate

    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    conn = await asyncpg.connect(dsn)
    try:
        await bootstrap_conn(conn, schema)
    finally:
        await conn.close()


@contextlib.asynccontextmanager
async def client_pool(dsn: str, schema: str):
    """Open the pool + the schema (the client script's one-liner boot)."""
    await bootstrap_schema(dsn, schema)
    pool_ref = LazyPool(dsn)
    try:
        yield await pool_ref.get()
    finally:
        await pool_ref.close()
