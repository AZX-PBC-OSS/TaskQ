"""Durable state for the gauge-fleet example.

The example's own tables (plain Postgres, outside TaskQ's schema):

- ``gauge_readings``      the source: raw gauge readings with an ``updated_at``
- ``gauge_cursor``        THE durable cursor — one row, the last refreshed instant
- ``gauge_digest``        the materialized refresh target (one row per gauge)
- ``gauge_cycle_log``     one row per tick attempt: the receipt (cursor before/after,
                          processed count, outcome) — the operator-facing run history
- ``gauge_cycle_guard``   the overlap guard: one row, ``running``/``idle`` with a
                          claim timestamp (stale claims are reclaimable — a dead
                          worker's leftover claim must not wedge the chain)
- ``gauge_breaches``      threshold breaches: the approval window and who approved
- ``gauge_escalations``   the escalation record written on the expired arm

Every helper takes an explicit ``asyncpg.pool.Pool`` (or connection); nothing here
holds global state, so the driver and the worker can use the same module.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import asyncpg

DSN = "postgresql://postgres:taskq@localhost:5800/taskq"

#: Demo knobs (deliberately arbitrary — the shapes are what matter).
TICK_INTERVAL_S = 4.0
FAN_OUT_THRESHOLD = 12
BREACH_THRESHOLD = 900.0
APPROVAL_WINDOW_S = 8.0
GUARD_STALE_S = 6.0

_DDL = """
CREATE TABLE IF NOT EXISTS gauge_readings (
    gauge_id   text             NOT NULL,
    reading    double precision NOT NULL,
    updated_at timestamptz      NOT NULL,
    PRIMARY KEY (gauge_id, updated_at)
);

CREATE TABLE IF NOT EXISTS gauge_cursor (
    id          boolean     PRIMARY KEY DEFAULT true CHECK (id),
    cursor_ts   timestamptz NOT NULL DEFAULT '-infinity'::timestamptz,
    advanced_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO gauge_cursor (id) VALUES (true) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS gauge_digest (
    gauge_id          text             PRIMARY KEY,
    reading           double precision NOT NULL,
    source_updated_at timestamptz      NOT NULL,
    digested_by_tick  integer          NOT NULL,
    digested_at       timestamptz      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gauge_cycle_log (
    tick_index    integer     PRIMARY KEY,
    job_id        uuid        NOT NULL,
    attempt       integer     NOT NULL,
    outcome       text        NOT NULL,
    cursor_before timestamptz NOT NULL,
    cursor_after  timestamptz,
    processed     integer     NOT NULL DEFAULT 0,
    fan_out       integer     NOT NULL DEFAULT 0,
    gauge_ids     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz
);

CREATE TABLE IF NOT EXISTS gauge_cycle_guard (
    id          boolean     PRIMARY KEY DEFAULT true CHECK (id),
    status      text        NOT NULL DEFAULT 'idle',
    run_token   text,
    claimed_at  timestamptz,
    released_at timestamptz
);
INSERT INTO gauge_cycle_guard (id) VALUES (true) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS gauge_breaches (
    breach_id        text             PRIMARY KEY,
    gauge_id         text             NOT NULL,
    reading          double precision NOT NULL,
    detected_at      timestamptz      NOT NULL DEFAULT now(),
    window_closes_at timestamptz      NOT NULL,
    approved_at      timestamptz
);

CREATE TABLE IF NOT EXISTS gauge_escalations (
    breach_id    text        PRIMARY KEY,
    escalated_at timestamptz NOT NULL DEFAULT now(),
    detail       text        NOT NULL
);
"""

_DROP = """
DROP TABLE IF EXISTS gauge_escalations, gauge_breaches, gauge_cycle_guard,
                     gauge_cycle_log, gauge_digest, gauge_cursor, gauge_readings;
"""


async def reset_schema(pool: asyncpg.Pool) -> None:
    """Drop and recreate the example's tables (a fresh demo run)."""
    async with pool.acquire() as conn:
        await conn.execute(_DROP)
        await conn.execute(_DDL)


async def inject_readings(
    pool: asyncpg.Pool,
    readings: list[tuple[str, float]],
) -> int:
    """Insert source rows stamped with the DATABASE clock.

    The cursor is advanced to ``max(updated_at)`` of the processed increment, and
    the increment is ``updated_at > cursor`` — all DB-side, so no app/DB clock
    domain ever crosses the comparison.
    """
    async with pool.acquire() as conn, conn.transaction():
        await conn.executemany(
            "INSERT INTO gauge_readings (gauge_id, reading, updated_at) "
            "VALUES ($1, $2, clock_timestamp())",
            readings,
        )
    return len(readings)


async def read_cursor(pool: asyncpg.Pool) -> Any:
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT cursor_ts FROM gauge_cursor WHERE id")


async def claim_cycle(pool: asyncpg.Pool, run_token: str) -> bool:
    """Atomically claim the guard row, or report the tick must park.

    The claim condition covers both cases the docs name: the guard is idle, OR
    its claim is stale (a worker died holding it — the crash-sweep shape). The
    single UPDATE is the serialization point: two concurrent ticks cannot both
    see ``status='idle'`` and both win.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE gauge_cycle_guard "
            "SET status = 'running', run_token = $1, claimed_at = now(), released_at = NULL "
            "WHERE id AND (status = 'idle' OR claimed_at < now() - make_interval(secs => $2)) "
            "RETURNING run_token",
            run_token,
            GUARD_STALE_S,
        )
    return row is not None


async def release_cycle(pool: asyncpg.Pool, run_token: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE gauge_cycle_guard SET status = 'idle', released_at = now() "
            "WHERE id AND run_token = $1",
            run_token,
        )


async def guard_row(pool: asyncpg.Pool) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM gauge_cycle_guard WHERE id")
    return dict(row) if row else {}


async def log_cycle(pool: asyncpg.Pool, **cols: Any) -> None:
    """Upsert this attempt's receipt row (a retried tick overwrites its own row)."""
    await pool.execute(
        "INSERT INTO gauge_cycle_log "
        "(tick_index, job_id, attempt, outcome, cursor_before, cursor_after, "
        " processed, fan_out, gauge_ids, finished_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now()) "
        "ON CONFLICT (tick_index) DO UPDATE SET "
        "  job_id = EXCLUDED.job_id, attempt = EXCLUDED.attempt, outcome = EXCLUDED.outcome, "
        "  cursor_before = EXCLUDED.cursor_before, cursor_after = EXCLUDED.cursor_after, "
        "  processed = EXCLUDED.processed, fan_out = EXCLUDED.fan_out, "
        "  gauge_ids = EXCLUDED.gauge_ids, finished_at = EXCLUDED.finished_at",
        cols["tick_index"],
        cols["job_id"],
        cols["attempt"],
        cols["outcome"],
        cols["cursor_before"],
        cols.get("cursor_after"),
        cols.get("processed", 0),
        cols.get("fan_out", 0),
        json.dumps(cols.get("gauge_ids", [])),
    )


async def fetch_increment(pool: asyncpg.Pool, cursor: Any) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT gauge_id, reading, updated_at FROM gauge_readings "
            "WHERE updated_at > $1 ORDER BY updated_at",
            cursor,
        )


async def apply_increment(
    pool: asyncpg.Pool,
    tick_index: int,
    increment: list[asyncpg.Record],
    job_id: str,
    attempt: int,
    outcome: str,
    processed: int | None = None,
    fan_out: int = 0,
) -> Any:
    """The all-or-nothing commit: digest upserts + cursor advance + receipt row
    land in ONE transaction (the doc'd per-cycle commit boundary).

    ``processed`` is the receipt's count: the number of gauges THIS tick
    digested inline (the fan-out arm passes 0 — its children do the digesting).

    Returns the new cursor (``max(updated_at)`` of the increment), or ``None``
    for an empty increment / parked tick (the cursor must not move).
    """
    new_cursor = max((r["updated_at"] for r in increment), default=None)
    async with pool.acquire() as conn, conn.transaction():
        if increment:
            await conn.executemany(
                "INSERT INTO gauge_digest "
                "(gauge_id, reading, source_updated_at, digested_by_tick) "
                "VALUES ($1, $2, $3, $4) "
                "ON CONFLICT (gauge_id) DO UPDATE SET "
                "  reading = EXCLUDED.reading, "
                "  source_updated_at = EXCLUDED.source_updated_at, "
                "  digested_by_tick = EXCLUDED.digested_by_tick, "
                "  digested_at = now()",
                [(r["gauge_id"], r["reading"], r["updated_at"], tick_index) for r in increment],
            )
        if new_cursor is not None:
            await conn.execute(
                "UPDATE gauge_cursor SET cursor_ts = $1, advanced_at = now() WHERE id",
                new_cursor,
            )
        await conn.execute(
            "INSERT INTO gauge_cycle_log "
            "(tick_index, job_id, attempt, outcome, cursor_before, cursor_after, "
            " processed, fan_out, gauge_ids, finished_at) "
            "VALUES ($1, $2, $3, $4, "
            "        (SELECT cursor_ts FROM gauge_cursor WHERE id), $5, $6, $7, $8, now()) "
            "ON CONFLICT (tick_index) DO UPDATE SET "
            "  job_id = EXCLUDED.job_id, attempt = EXCLUDED.attempt, "
            "  outcome = EXCLUDED.outcome, cursor_after = EXCLUDED.cursor_after, "
            "  processed = EXCLUDED.processed, fan_out = EXCLUDED.fan_out, "
            "  gauge_ids = EXCLUDED.gauge_ids, finished_at = EXCLUDED.finished_at",
            tick_index,
            uuid.UUID(job_id),
            attempt,
            outcome,
            new_cursor,
            len(increment) if processed is None else processed,
            fan_out,
            json.dumps([r["gauge_id"] for r in increment]),
        )
    return new_cursor


async def record_breach(pool: asyncpg.Pool, breach_id: str, gauge_id: str, reading: float) -> None:
    """Insert a breach row for a threshold-violating reading."""
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO gauge_breaches (breach_id, gauge_id, reading, window_closes_at) "
            "VALUES ($1, $2, $3, now() + make_interval(secs => $4)) "
            "ON CONFLICT (breach_id) DO UPDATE SET "
            "  reading = EXCLUDED.reading, window_closes_at = EXCLUDED.window_closes_at, "
            "  approved_at = NULL, detected_at = now()",
            breach_id,
            gauge_id,
            reading,
            APPROVAL_WINDOW_S,
        )


async def breach_row(pool: asyncpg.Pool, breach_id: str) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM gauge_breaches WHERE breach_id = $1", breach_id)
    return dict(row) if row else {}


async def approve(pool: asyncpg.Pool, breach_id: str) -> None:
    """The human half of the HITL face: an operator (or route) grants approval."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE gauge_breaches SET approved_at = now() WHERE breach_id = $1", breach_id
        )


async def record_escalation(pool: asyncpg.Pool, breach_id: str, detail: str) -> None:
    """The expired arm's durable escalation record — written BEFORE the typed
    error is raised, so the operator's audit trail survives the fail-close."""
    await pool.execute(
        "INSERT INTO gauge_escalations (breach_id, detail) VALUES ($1, $2) "
        "ON CONFLICT (breach_id) DO UPDATE SET detail = EXCLUDED.detail, escalated_at = now()",
        breach_id,
        detail,
    )


async def cycle_rows(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM gauge_cycle_log ORDER BY tick_index, attempt")
    return [dict(r) for r in rows]


async def digest_rows(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM gauge_digest ORDER BY gauge_id")
    return [dict(r) for r in rows]
