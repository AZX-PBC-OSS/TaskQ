"""The ledger store: idempotent upserts, quarantine writes, evidence reads.

The tables live in a dedicated ``ledger`` schema next to (never inside)
TaskQ's own tables. Three properties carry the drills:

- **Idempotent upsert.** ``ON CONFLICT … DO UPDATE … WHERE`` change
  detection: a row whose content hash is unchanged is not updated (the
  statement returns nothing for it), so a re-import of the same feed
  reports zero changed rows and the table's fingerprint is stable.
- **Quarantine with reasons.** Invalid rows never touch the ledger
  table; they land in ``ledger.quarantine`` with machine-readable
  reasons, upserted so a re-import refreshes rather than duplicates.
- **Body-run evidence.** Every body execution records one row; the
  resume drill reads this table to prove a resumed run does not
  re-execute completed work.

The DSN is process configuration, set once by the entry point before
the run starts (workflow bodies execute in the driver's process under
the ``execute=True`` dev-loop door).
"""

import asyncio
import hashlib
import json
import os
from datetime import datetime
from typing import Any

import asyncpg

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS ledger;
CREATE TABLE IF NOT EXISTS ledger.entries (
    entry_id     text PRIMARY KEY,
    account      text NOT NULL,
    amount       numeric NOT NULL,
    currency     text NOT NULL,
    memo         text NOT NULL,
    posted_at    date NOT NULL,
    content_hash text NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ledger.quarantine (
    entry_id   text PRIMARY KEY,
    reasons    jsonb NOT NULL,
    raw        jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ledger.body_runs (
    body       text NOT NULL,
    node_key   text NOT NULL,
    map_index  integer,
    run_key    text,
    ran_at     timestamptz NOT NULL DEFAULT now()
);
"""

_dsn: str | None = None
_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


def configure(dsn: str | None = None) -> str:
    """Pin the store's DSN (env fallback: ``TASKQ_PG_DSN``); returns it."""
    global _dsn
    _dsn = dsn or os.environ["TASKQ_PG_DSN"]
    return _dsn


async def pool() -> asyncpg.Pool:
    """The shared pool, created on first use."""
    global _pool
    async with _pool_lock:
        if _pool is None:
            assert _dsn is not None, "store.configure(dsn) must run before the pool is used"
            _pool = await asyncpg.create_pool(_dsn)
    assert _pool is not None
    return _pool


async def apply_ddl() -> None:
    p = await pool()
    await p.execute(SCHEMA_DDL)


def content_hash(record: dict[str, Any]) -> str:
    """The row's canonical content hash (the change-detection key)."""
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def record_body_run(
    body: str, node_key: str, map_index: int | None, run_key: str | None
) -> None:
    """One row per body execution — the resume drill's evidence table."""
    p = await pool()
    await p.execute(
        "INSERT INTO ledger.body_runs (body, node_key, map_index, run_key) VALUES ($1, $2, $3, $4)",
        body,
        node_key,
        map_index,
        run_key,
    )


async def upsert_valid_batch(
    batch: list[tuple[str, dict[str, Any], str]],
) -> dict[str, str]:
    """Upsert a batch of valid rows; return per-entry "imported"/"updated"/"unchanged".

    The WHERE on the conflict branch is the change detector: a row whose
    stored hash equals the incoming hash updates NOTHING — the re-import
    proof's mechanism. The pre-SELECT (locked) classifies each row, so
    the receipt's per-kind counts are exact.
    """
    p = await pool()
    async with p.acquire() as conn, conn.transaction():
        entry_ids = [entry_id for entry_id, _, _ in batch]
        prior = {
            r["entry_id"]: r["content_hash"]
            for r in await conn.fetch(
                "SELECT entry_id, content_hash FROM ledger.entries "
                "WHERE entry_id = ANY($1) FOR UPDATE",
                entry_ids,
            )
        }
        rows_by_id = {entry_id: (row, chash) for entry_id, row, chash in batch}
        ordered_rows = [rows_by_id[entry_id][0] for entry_id in entry_ids]
        ordered_hashes = [rows_by_id[entry_id][1] for entry_id in entry_ids]
        await conn.execute(
            """
                INSERT INTO ledger.entries
                    (entry_id, account, amount, currency, memo, posted_at, content_hash, updated_at)
                SELECT e, a, m, c, memo, p, h, now() FROM unnest(
                    $1::text[], $2::text[], $3::float8[], $4::text[],
                    $5::text[], $6::date[], $7::text[]
                ) AS t(e, a, m, c, memo, p, h)
                ON CONFLICT (entry_id) DO UPDATE SET
                    account      = EXCLUDED.account,
                    amount       = EXCLUDED.amount,
                    currency     = EXCLUDED.currency,
                    memo         = EXCLUDED.memo,
                    posted_at    = EXCLUDED.posted_at,
                    content_hash = EXCLUDED.content_hash,
                    updated_at   = now()
                WHERE ledger.entries.content_hash IS DISTINCT FROM EXCLUDED.content_hash
                """,
            entry_ids,
            [row["account"] for row in ordered_rows],
            [row["amount"] for row in ordered_rows],
            [row["currency"] for row in ordered_rows],
            [row["memo"] for row in ordered_rows],
            [row["posted_at"] for row in ordered_rows],
            ordered_hashes,
        )
        kinds: dict[str, str] = {}
        for entry_id, _, chash in batch:
            was = prior.get(entry_id)
            kinds[entry_id] = (
                "unchanged" if was == chash else ("imported" if was is None else "updated")
            )
        return kinds


async def quarantine_batch(rows: list[tuple[str | None, dict[str, Any], list[str]]]) -> None:
    """Upsert a batch of invalid rows into quarantine with their reasons."""
    keyed = [(entry_id, raw, reasons) for entry_id, raw, reasons in rows if entry_id is not None]
    if not keyed:
        return  # rows too malformed to key: their reasons still ride the receipt
    p = await pool()
    await p.execute(
        """
        INSERT INTO ledger.quarantine (entry_id, reasons, raw, updated_at)
        SELECT e, r, w, now() FROM unnest($1::text[], $2::jsonb[], $3::jsonb[]) AS t(e, r, w)
        ON CONFLICT (entry_id) DO UPDATE SET
            reasons = EXCLUDED.reasons, raw = EXCLUDED.raw, updated_at = now()
        """,
        [entry_id for entry_id, _, _ in keyed],
        [json.dumps(reasons) for _, _, reasons in keyed],
        [json.dumps(raw) for _, raw, _ in keyed],
    )


async def table_fingerprint() -> tuple[int, str]:
    """(row count, content hash) of the ledger table — the re-import proof's face."""
    p = await pool()
    row = await p.fetchrow(
        """
        SELECT count(*) AS n, md5(string_agg(h, '' ORDER BY entry_id)) AS fp
        FROM (SELECT entry_id, entry_id || content_hash AS h FROM ledger.entries) t
        """
    )
    assert row is not None
    return int(row["n"]), str(row["fp"] or "")


async def quarantine_count() -> int:
    p = await pool()
    return int(await p.fetchval("SELECT count(*) FROM ledger.quarantine"))


async def quarantine_reason_counts() -> dict[str, int]:
    """The reasons taxonomy, tallied across quarantined rows."""
    p = await pool()
    rows = await p.fetch(
        """
        SELECT reason, count(*) AS n
        FROM ledger.quarantine, jsonb_array_elements_text(reasons) AS reason
        GROUP BY reason ORDER BY reason
        """
    )
    return {str(r["reason"]): int(r["n"] or 0) for r in rows}


async def body_run_count(body: str) -> int:
    p = await pool()
    return int(await p.fetchval("SELECT count(*) FROM ledger.body_runs WHERE body = $1", body))


async def body_runs_since(since_iso: str, body: str | None = None) -> list[dict[str, Any]]:
    """Body runs recorded after a timestamp — the resume drill's re-execution check."""
    since = datetime.fromisoformat(since_iso)
    p = await pool()
    if body is None:
        rows = await p.fetch(
            "SELECT body, node_key, map_index, run_key, ran_at FROM ledger.body_runs WHERE ran_at > $1 ORDER BY ran_at",
            since,
        )
    else:
        rows = await p.fetch(
            "SELECT body, node_key, map_index, run_key, ran_at FROM ledger.body_runs WHERE ran_at > $1 AND body = $2 ORDER BY ran_at",
            since,
            body,
        )
    return [dict(r) for r in rows]


async def reset_tables() -> None:
    """Truncate the demo's tables (a drill suite runs from a clean slate)."""
    p = await pool()
    await p.execute("TRUNCATE ledger.entries, ledger.quarantine, ledger.body_runs")
