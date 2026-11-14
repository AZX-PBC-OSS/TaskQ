# ruff: noqa: S608  # Why: the schema is the settings-validated identifier and every data value is $-bound.
"""Private two-process worker entry for the P2 real-saturation proof.

This module also OWNS the small synthetic source/leaf/probe declarations and
their minimal durable gates: a keyset-paged record stream whose leaves wait on
a real PostgreSQL release row.  Every table, queue, actor, and record below is
synthetic and hermetic to this suite; no external provider, network, or private
data participates.

The production bootstrap and native asyncpg connections remain in use.  A
narrow method observer records each actual ``EMIT_IN_FLIGHT_SQL`` read in its
own committed witness row; it does not replace a pool, result, claim, or
transaction.  The independent witness insert survives a subsequent emit
rollback, so refused locked admissions cannot disappear from the evidence.
"""

from __future__ import annotations

import asyncio
import contextlib
import faulthandler
import json
import os
import signal
import sys
from collections.abc import Awaitable, Callable
from typing import Final

import asyncpg
from pydantic import BaseModel

from taskq import JobContext, actor
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from taskq.workflows import (
    Chain,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
)

FLOW_NAME: Final = "p2_saturation_stream"
SOURCE_QUEUE: Final = "p2sat_source"
LEAF_QUEUE: Final = "p2sat_record"
SIBLING_QUEUE: Final = "p2sat_sibling"
PAGE_WIDTH: Final = 2
RECORD_COUNT: Final = 4

_SOURCE_ACTOR: Final = "p2sat_source_worker"
_LEAF_ACTOR: Final = "p2sat_record_worker"


def _scenario_name() -> str:
    scenario = os.environ.get("P2_SATURATION_SCENARIO")
    if not scenario:
        raise RuntimeError("P2_SATURATION_SCENARIO must name this local scenario")
    return scenario


def gate_channel(schema: str) -> str:
    """Real cross-process cooperative release channel for the local scenario."""
    return f"p2sat_gate_{schema}"


async def write_record(ctx: StepContext, item: dict[str, object]) -> str:
    """Wait on a real PostgreSQL gate, then durably apply exactly one record."""
    record_id = item["record_id"]
    if not isinstance(record_id, str):
        raise TypeError("record_id must be text")
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    scenario = _scenario_name()
    # One native session per leaf body: the leaf-run witness, the gate's
    # LISTEN, and the applied record all ride it; it closes in the finally.
    conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    released = asyncio.Event()

    def notice(*_args: object) -> None:
        released.set()

    try:
        await conn.execute(
            f'INSERT INTO "{schema}".p2sat_leaf_runs '
            "(scenario, flow_id, job_id, map_index, record_id, pid, attempt) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            scenario,
            ctx.flow_id,
            ctx.job_id,
            ctx.map_index,
            record_id,
            os.getpid(),
            ctx.attempt,
        )
        await ctx.progress(10, "record waiting", {"record_id": record_id})

        # NOTIFY is only an accelerator; each wakeup also reads the durable
        # release row (the durable row is the gate's authority).
        await conn.add_listener(gate_channel(schema), notice)
        while True:
            released_at = await conn.fetchval(
                f'SELECT release_at FROM "{schema}".p2sat_control WHERE scenario = $1',
                scenario,
            )
            if released_at is not None:
                break
            released.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(released.wait(), timeout=1.0)
        await conn.execute(
            f'INSERT INTO "{schema}".p2sat_records_applied '
            "(scenario, flow_id, job_id, record_id, content, applied_by_pid) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            scenario,
            ctx.flow_id,
            ctx.job_id,
            record_id,
            item["content"],
            os.getpid(),
        )
        await conn.execute(
            f'UPDATE "{schema}".p2sat_leaf_runs '
            "SET released_at = clock_timestamp(), body_returned_at = clock_timestamp() "
            "WHERE scenario = $1 AND job_id = $2",
            scenario,
            ctx.job_id,
        )
    finally:
        await conn.close()
    await ctx.progress(100, "record applied", {"record_id": record_id})
    return record_id


RECORD_CHAIN: Final = Chain(
    name="p2sat-record-work",
    start="write_record",
    steps={"write_record": Step(body=write_record, outcomes=str)},
    actor=_LEAF_ACTOR,
    queue=LEAF_QUEUE,
)


async def _fetch_page(
    conn: asyncpg.Connection, schema: str, scenario: str, cursor: str | None
) -> tuple[list[dict[str, object]], str | None]:
    """The synthetic keyset page read plus its scenario counters.

    ``next_cursor`` is ``None`` exactly when no records remain after this
    page, so a fully-consumed set terminates without an empty fetch."""
    after = int(cursor) if cursor is not None else -1
    rows = await conn.fetch(
        f'SELECT ordinal, record_id, content FROM "{schema}".p2sat_records '
        "WHERE scenario_id = $1 AND ordinal > $2 ORDER BY ordinal LIMIT $3",
        scenario,
        after,
        PAGE_WIDTH,
    )
    page = [dict(row) for row in rows]
    last = int(page[-1]["ordinal"]) if page else after
    remaining = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".p2sat_records '
        "WHERE scenario_id = $1 AND ordinal > $2",
        scenario,
        last,
    )
    next_cursor = str(last + 1) if page and int(remaining or 0) > 0 else None
    await conn.execute(
        f'UPDATE "{schema}".p2sat_provider_scenario '
        "SET fetch_calls = fetch_calls + 1, pages_returned = pages_returned + 1, "
        "records_returned = records_returned + $2 "
        "WHERE scenario_id = $1",
        scenario,
        len(page),
    )
    return page, next_cursor


async def stream_pages(ctx: StepContext) -> None:
    """Re-drive only the uncommitted page after slot-free parking."""
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    scenario = _scenario_name()
    conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    try:
        entry_id = await conn.fetchval(
            f'INSERT INTO "{schema}".p2sat_source_entries '
            "(scenario, flow_id, job_id, pid, logical_attempt, claim_epoch) "
            "VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
            scenario,
            ctx.flow_id,
            ctx.job_id,
            os.getpid(),
            ctx.attempt,
            ctx.claim_epoch,
        )
        checkpoint = await ctx.cursor()
        raw_cursor = checkpoint.get("next_cursor")
        cursor = raw_cursor if isinstance(raw_cursor, str) else None
        while True:
            page, next_cursor = await _fetch_page(conn, schema, scenario, cursor)
            await conn.execute(
                f'INSERT INTO "{schema}".p2sat_fetches '
                "(scenario, source_entry_id, cursor_in, cursor_out, record_ids) "
                "VALUES ($1, $2, $3, $4, $5::jsonb)",
                scenario,
                entry_id,
                cursor,
                next_cursor,
                json.dumps([str(row["record_id"]) for row in page]),
            )
            if not page:
                break
            children = [
                chain_start(
                    RECORD_CHAIN,
                    {"record_id": row["record_id"], "content": row["content"]},
                    map_index=int(row["ordinal"]),
                    trace_id=f"record:{row['record_id']}",
                )
                for row in page
            ]
            await ctx.emit_batch(
                children,
                cursor={"next_cursor": next_cursor},
                backpressure_timeout_s=30.0,
            )
            await ctx.progress(
                50,
                "page emitted",
                {"records": len(page), "logical_attempt": ctx.attempt},
            )
            if next_cursor is None:
                break
            cursor = next_cursor
        await conn.execute(
            f'UPDATE "{schema}".p2sat_source_entries '
            "SET body_returned_at = clock_timestamp() WHERE id = $1",
            entry_id,
        )
    finally:
        await conn.close()


p2sat_app: Final = WorkflowApp()


@p2sat_app.workflow(FLOW_NAME, queue=SOURCE_QUEUE, max_in_flight=PAGE_WIDTH)
def p2_saturation_stream() -> Promise[object]:
    return build(
        chain_source(
            RECORD_CHAIN,
            stream_pages,
            key="source",
            actor=_SOURCE_ACTOR,
            queue=SOURCE_QUEUE,
            max_attempts=2,
        )
    )


class ProbePayload(BaseModel):
    scenario: str
    ordinal: int


async def _probe(payload: ProbePayload, ctx: JobContext[ProbePayload]) -> dict[str, int]:
    """Ordinary same-queue work, independent of leaf caps and source state."""
    conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    try:
        schema = os.environ["TASKQ_SCHEMA_NAME"]
        await conn.execute(
            f'INSERT INTO "{schema}".p2sat_probe_runs '
            "(scenario, ordinal, job_id, actor, queue, pid, attempt) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            payload.scenario,
            payload.ordinal,
            ctx.job_id,
            ctx.actor,
            ctx.queue,
            os.getpid(),
            ctx.attempt,
        )
    finally:
        await conn.close()
    return {"ordinal": payload.ordinal}


source_probe = actor(name="p2sat_source_probe", queue=SOURCE_QUEUE)(_probe)
leaf_probe = actor(name="p2sat_record_probe", queue=LEAF_QUEUE)(_probe)
sibling_probe = actor(name="p2sat_sibling_probe", queue=SIBLING_QUEUE)(_probe)


async def initialize_sample(
    pool: asyncpg.Pool,
    *,
    schema: str,
    scenario: str,
) -> None:
    """Initialize observer/control tables and the synthetic record scenario."""
    await pool.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_control (
            scenario text PRIMARY KEY,
            release_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_provider_scenario (
            scenario_id text PRIMARY KEY,
            record_count integer NOT NULL,
            fetch_calls integer NOT NULL DEFAULT 0,
            pages_returned integer NOT NULL DEFAULT 0,
            records_returned integer NOT NULL DEFAULT 0,
            fault_gates_tripped integer NOT NULL DEFAULT 0,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_records (
            scenario_id text NOT NULL,
            ordinal integer NOT NULL,
            record_id text NOT NULL,
            content text NOT NULL,
            PRIMARY KEY (scenario_id, ordinal)
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_source_entries (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            flow_id uuid NOT NULL,
            job_id uuid NOT NULL,
            pid integer NOT NULL,
            logical_attempt integer NOT NULL,
            claim_epoch bigint NOT NULL,
            entered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            body_returned_at timestamptz
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_fetches (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            source_entry_id bigint NOT NULL,
            cursor_in text,
            cursor_out text,
            record_ids jsonb NOT NULL,
            fetched_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_leaf_runs (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            flow_id uuid NOT NULL,
            job_id uuid NOT NULL,
            map_index bigint NOT NULL,
            record_id text NOT NULL,
            pid integer NOT NULL,
            attempt integer NOT NULL,
            started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            released_at timestamptz,
            body_returned_at timestamptz,
            UNIQUE (scenario, job_id, attempt)
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_records_applied (
            scenario text NOT NULL,
            flow_id uuid NOT NULL,
            job_id uuid NOT NULL,
            record_id text NOT NULL,
            content text NOT NULL,
            applied_by_pid integer NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (scenario, flow_id, record_id)
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_probe_runs (
            scenario text NOT NULL,
            ordinal integer NOT NULL,
            job_id uuid NOT NULL,
            actor text NOT NULL,
            queue text NOT NULL,
            pid integer NOT NULL,
            attempt integer NOT NULL,
            occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (scenario, actor, ordinal)
        );
        CREATE TABLE IF NOT EXISTS "{schema}".p2sat_admission_reads (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            pid integer NOT NULL,
            flow_id uuid NOT NULL,
            source_id uuid NOT NULL,
            outstanding integer NOT NULL,
            source_status text NOT NULL,
            in_transaction boolean NOT NULL,
            occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )
        """,
    )
    await pool.execute(
        f'INSERT INTO "{schema}".p2sat_control (scenario) VALUES ($1) '
        "ON CONFLICT (scenario) DO NOTHING",
        scenario,
    )
    await pool.execute(
        f'INSERT INTO "{schema}".p2sat_provider_scenario '
        "(scenario_id, record_count) VALUES ($1, $2) "
        "ON CONFLICT (scenario_id) DO UPDATE SET updated_at = clock_timestamp()",
        scenario,
        RECORD_COUNT,
    )
    await pool.execute(
        f'DELETE FROM "{schema}".p2sat_records WHERE scenario_id = $1',
        scenario,
    )
    await pool.execute(
        f'INSERT INTO "{schema}".p2sat_records (scenario_id, ordinal, record_id, content) '
        "SELECT $1, ordinal, 'record-' || ordinal, 'value-' || ordinal "
        "FROM generate_series(0, $2 - 1) AS ordinal",
        scenario,
        RECORD_COUNT,
    )


# ── The two-process worker entry ──────────────────────────────────────

_ORIGINAL_FETCHVAL: Callable[..., Awaitable[object]] = asyncpg.Connection.fetchval


# Why: this transparent observer mirrors the native asyncpg timeout parameter.
async def _observed_fetchval(
    conn: asyncpg.Connection,
    query: str,
    *args: object,
    column: int = 0,
    timeout: float | None = None,
) -> object:
    result = await _ORIGINAL_FETCHVAL(conn, query, *args, column=column, timeout=timeout)
    if (
        "Only the exact committed park representation is not executable work." in query
        and "AND id <> $2::uuid" in query
        and len(args) >= 2
    ):
        # The schema is validated by WorkerSettings before this process serves
        # work.  A separate native-bound read makes the admission's phase visible
        # even when the engine's enclosing emit transaction later rolls back.
        schema = os.environ["TASKQ_SCHEMA_NAME"]
        source_status = await _ORIGINAL_FETCHVAL(
            conn,
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',  # settings-validated identifier only
            args[1],
            column=0,
        )
        witness = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
        try:
            await witness.execute(
                f'INSERT INTO "{schema}".p2sat_admission_reads '  # settings-validated identifier only
                "(scenario, pid, flow_id, source_id, outstanding, source_status, in_transaction) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                os.environ["P2_SATURATION_SCENARIO"],
                os.getpid(),
                args[0],
                args[1],
                int(result or 0),
                source_status,
                conn.is_in_transaction(),
            )
        finally:
            await witness.close()
    return result


async def _run() -> int:
    # Instrument only in these owned proof subprocesses.  Production and every
    # other system worker retain the untouched method.
    asyncpg.Connection.fetchval = _observed_fetchval  # type: ignore[method-assign]  # Why: process-local transparent observation only.
    settings = WorkerSettings.load()
    return await _main(
        settings,
        actor_registry={
            "p2sat_source_probe": source_probe,
            "p2sat_record_probe": leaf_probe,
            "p2sat_sibling_probe": sibling_probe,
        },
    )


if __name__ == "__main__":
    faulthandler.register(signal.SIGUSR2, all_threads=True)
    try:
        with asyncio.Runner() as runner:
            code = runner.run(_run())
        with contextlib.suppress(ValueError):  # Why: retain the normal post-drain verdict.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        sys.exit(code)
    except KeyboardInterrupt:
        sys.exit(0)
