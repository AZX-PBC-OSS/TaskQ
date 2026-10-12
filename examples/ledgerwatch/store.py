"""LedgerWatch's own durable tables (the app's side of the estate).

The framework's rows are the truth about RUNS; these tables are the
app's own record of the world outside the framework: the enrichment
journal (the mock API's access log), the compliance queue (the
escalation's durable inbox), the completion reports (the typed terminal
receipts), and the reconciliation passes.

Everything is idempotent by construction: the feed re-delivers, workers
crash mid-body and re-execute — so every write here is an upsert or an
ON CONFLICT no-op. ZERO double-effects is a property of the writes, not
a hope.
"""

from __future__ import annotations

from datetime import datetime

import asyncpg

from examples.ledgerwatch.config import LedgerWatchSettings
from examples.ledgerwatch.models import Batch, CompletionReport, ReconReport
from examples.ledgerwatch.pooling import LazyPool

_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.lw_batches (
    batch_id    text PRIMARY KEY,
    txn_count   int  NOT NULL,
    delivered_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {schema}.lw_enrich_calls (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    txn_id      text NOT NULL,
    call_index  int  NOT NULL,
    outcome     text NOT NULL,  -- inflight | ok | flake | hang | timeout
    latency_s   double precision,
    detail      text NOT NULL DEFAULT '',
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);
CREATE INDEX IF NOT EXISTS lw_enrich_calls_txn ON {schema}.lw_enrich_calls (txn_id);

CREATE TABLE IF NOT EXISTS {schema}.lw_compliance_queue (
    txn_id      text PRIMARY KEY,
    batch_id    text NOT NULL,
    flow_id     text NOT NULL,
    escalated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {schema}.lw_reports (
    batch_id    text PRIMARY KEY,
    report      jsonb NOT NULL,
    written_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {schema}.lw_recon_passes (
    pass_id     text PRIMARY KEY,
    report      jsonb NOT NULL,
    fired_at    timestamptz NOT NULL,
    written_at  timestamptz NOT NULL DEFAULT now()
);
"""


async def bootstrap(pool: asyncpg.Pool, schema: str) -> None:
    """Create the app's tables (idempotent — run at boot and in tests)."""
    async with pool.acquire() as conn:
        await bootstrap_conn(conn, schema)


async def bootstrap_conn(conn: asyncpg.Connection, schema: str) -> None:
    """The connection-shaped variant (the worker's boot connection)."""
    await conn.execute(_DDL.format(schema=f'"{schema}"'))


class LedgerWatchStore:
    """The app's writes (all idempotent — see the module docstring)."""

    def __init__(self, pool_ref: LazyPool, schema: str, settings: LedgerWatchSettings) -> None:
        self._pool_ref = pool_ref
        self._schema = schema
        self.settings = settings

    async def _pool(self) -> asyncpg.Pool:
        return await self._pool_ref.get()

    async def record_batch(self, batch: Batch) -> bool:
        """The batch's delivery record. Returns True for the FIRST
        delivery (the at-least-once feed's dedup face at the app's own
        table — the framework's run-key arbiter is the stronger door;
        this row is the operator-facing delivery ledger)."""
        pool = await self._pool()
        row = await pool.fetchval(
            f'INSERT INTO "{self._schema}".lw_batches (batch_id, txn_count)'  # noqa: S608  # Why: the schema identifier only; every caller value is bound.
            " VALUES ($1, $2) ON CONFLICT (batch_id) DO NOTHING RETURNING batch_id",
            batch.batch_id,
            len(batch.transactions),
        )
        return row is not None

    async def escalate_to_compliance(self, txn_id: str, batch_id: str, flow_id: str) -> bool:
        """The Expired arm's escalation: ONE durable row per txn, ever."""
        pool = await self._pool()
        row = await pool.fetchval(
            f'INSERT INTO "{self._schema}".lw_compliance_queue (txn_id, batch_id, flow_id)'  # noqa: S608  # Why: as above.
            " VALUES ($1, $2, $3) ON CONFLICT (txn_id) DO NOTHING RETURNING txn_id",
            txn_id,
            batch_id,
            flow_id,
        )
        return row is not None

    async def record_report(self, report: CompletionReport) -> None:
        """The typed terminal receipt, durable. Latest-wins per batch
        (the report is a deterministic function of the run's collect)."""
        pool = await self._pool()
        await pool.execute(
            f'INSERT INTO "{self._schema}".lw_reports (batch_id, report)'  # noqa: S608  # Why: as above.
            " VALUES ($1, $2) ON CONFLICT (batch_id)"
            " DO UPDATE SET report = EXCLUDED.report, written_at = now()",
            report.batch_id,
            report.model_dump_json(),
        )

    async def get_report(self, batch_id: str) -> CompletionReport | None:
        pool = await self._pool()
        raw = await pool.fetchval(
            f'SELECT report FROM "{self._schema}".lw_reports WHERE batch_id = $1',  # noqa: S608
            batch_id,
        )
        return CompletionReport.model_validate_json(raw) if raw else None

    async def record_recon_pass(self, report: ReconReport, fired_at: object) -> None:
        pool = await self._pool()
        await pool.execute(
            f'INSERT INTO "{self._schema}".lw_recon_passes (pass_id, report, fired_at)'  # noqa: S608  # Why: as above.
            " VALUES ($1, $2, $3) ON CONFLICT (pass_id) DO NOTHING",
            report.pass_id,
            report.model_dump_json(),
            fired_at,
        )

    async def db_now(self) -> datetime:
        """The DB clock (the timers' truth — never the process clock)."""
        pool = await self._pool()
        return await pool.fetchval("SELECT now()")

    async def enrich_call_stats(self) -> dict[str, int]:
        """The journal's outcome histogram (the drills' retry receipt)."""
        pool = await self._pool()
        rows = await pool.fetch(
            f'SELECT outcome, count(*) AS n FROM "{self._schema}".lw_enrich_calls'  # noqa: S608  # Why: as above.
            " GROUP BY outcome"
        )
        return {r["outcome"]: r["n"] for r in rows}
