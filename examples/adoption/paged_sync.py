# ruff: noqa: S608  # Why: schema identifiers are validated at the settings/application boundary; every data value is $N-bound.
"""Runnable generic paged-provider synchronization workflow.

The application uses only synthetic records and a local PostgreSQL provider.  A
``PgPagedProvider`` is constructed by ``deps_factory`` independently in every
real worker; its snapshot, cursors, counters, and records are durable and shared
by scenario rather than kept in process memory.  The provider is borrowed from
the worker's pool: neither it nor :class:`SyncDeps` closes that pool.

``python -m examples.adoption.paged_sync`` initializes a scenario and creates a
run for an already-running fleet.  The system-e2e proof supplies that fleet and
its cooperative database release gate.  No network provider, private tenant,
or external side effect is involved.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
from dataclasses import dataclass
from typing import Final

import asyncpg
from pydantic import BaseModel

from examples.adoption.pg_provider import PgPagedProvider
from examples.adoption.provider import ProviderCursor, ProviderRecord
from taskq import JobContext, actor
from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    WorkflowDepsContext,
    build,
    chain_source,
    chain_start,
)

FLOW_NAME: Final = "generic_paged_sync"
SOURCE_QUEUE: Final = "adoption_source"
LEAF_QUEUE: Final = "adoption_record"
SIBLING_QUEUE: Final = "adoption_sibling"
PAGE_WIDTH: Final = 2
RECORD_COUNT: Final = 4


def _schema_name(value: str) -> str:
    if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value) is None:
        raise ValueError(f"invalid application schema: {value!r}")
    return value


def gate_channel(schema: str) -> str:
    """Real cross-process cooperative release channel for the local sample."""
    return f"adoption_sync_gate_{_schema_name(schema)}"


def instrumentation_channel(schema: str) -> str:
    return f"adoption_sync_instrument_{_schema_name(schema)}"


@dataclass(slots=True)
class SyncDeps:
    """One P6-owned worker product; the asyncpg pool remains worker-owned."""

    pool: asyncpg.Pool
    schema: str
    scenario: str
    provider: PgPagedProvider
    worker_id: str
    pid: int
    closes: int = 0

    async def aclose(self) -> None:
        if self.closes:
            raise RuntimeError("sync dependency product closed more than once")
        self.closes += 1
        # P6 closes the product before the borrowed dispatcher pool.  This row
        # makes both that ordering and exact-once close observable outside the
        # process; it deliberately does not close the provider's borrowed pool.
        await self.pool.execute(
            f'INSERT INTO "{self.schema}".adoption_sync_factory_events '
            "(scenario, pid, worker_id, event, record_count) "
            "SELECT $1, $2, $3::uuid, 'close', record_count "
            f'FROM "{self.schema}".adoption_provider_scenario WHERE scenario_id = $1',
            self.scenario,
            self.pid,
            self.worker_id,
        )


async def make_sync_deps(context: WorkflowDepsContext) -> SyncDeps:
    """Construct/adopt one durable scenario independently on each worker loop."""
    scenario = os.environ.get("ADOPTION_SYNC_SCENARIO")
    if not scenario:
        raise RuntimeError("ADOPTION_SYNC_SCENARIO must name this local scenario")
    provider = PgPagedProvider(
        context.pool,
        schema=context.schema,
        scenario=scenario,
        max_records=32,
        max_page_size=PAGE_WIDTH,
        max_snapshots=8,
        max_cursors=64,
    )
    # Accepted provider initialization takes its schema/scenario locks and is
    # safe when two independent OS processes enter here concurrently.
    await provider.initialize()
    state = await provider.read_scenario_state()
    product = SyncDeps(
        pool=context.pool,
        schema=context.schema,
        scenario=scenario,
        provider=provider,
        worker_id=str(context.worker_id),
        pid=os.getpid(),
    )
    await context.pool.execute(
        f'INSERT INTO "{context.schema}".adoption_sync_factory_events '
        "(scenario, pid, worker_id, event, record_count) VALUES ($1, $2, $3::uuid, 'open', $4)",
        scenario,
        product.pid,
        product.worker_id,
        state.record_count,
    )
    return product


async def write_record(ctx: StepContext, item: dict[str, object], deps: SyncDeps) -> str:
    """Wait on a real PostgreSQL gate, then durably apply exactly one record."""
    record_id = item["record_id"]
    if not isinstance(record_id, str):
        raise TypeError("record_id must be text")
    schema = deps.schema
    await deps.pool.execute(
        f'INSERT INTO "{schema}".adoption_sync_leaf_runs '
        "(scenario, flow_id, job_id, map_index, record_id, pid, worker_id, attempt) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7::uuid, $8)",
        deps.scenario,
        ctx.flow_id,
        ctx.job_id,
        ctx.map_index,
        record_id,
        os.getpid(),
        deps.worker_id,  # per-process factory witness (public WorkflowDepsContext.worker_id)
        ctx.attempt,
    )
    await ctx.progress(10, "record waiting", {"record_id": record_id})

    # The listener is local to this child, not a parent-process event.  NOTIFY
    # is only an accelerator; each wakeup also reads the durable release row.
    listener = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    released = asyncio.Event()

    def notice(*_args: object) -> None:
        released.set()

    try:
        await listener.add_listener(gate_channel(schema), notice)
        while True:
            released_at = await listener.fetchval(
                f'SELECT release_at FROM "{schema}".adoption_sync_control WHERE scenario = $1',
                deps.scenario,
            )
            if released_at is not None:
                break
            released.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(released.wait(), timeout=1.0)
        async with deps.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f'INSERT INTO "{schema}".adoption_sync_records '
                "(scenario, flow_id, job_id, record_id, content, applied_by_pid) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                deps.scenario,
                ctx.flow_id,
                ctx.job_id,
                record_id,
                item["content"],
                os.getpid(),
            )
            await conn.execute(
                f'UPDATE "{schema}".adoption_sync_leaf_runs '
                "SET released_at = clock_timestamp(), body_returned_at = clock_timestamp() "
                "WHERE scenario = $1 AND job_id = $2",
                deps.scenario,
                ctx.job_id,
            )
    finally:
        await listener.close()
    await ctx.progress(100, "record applied", {"record_id": record_id})
    return record_id


RECORD_CHAIN: Final = Chain(
    name="generic-record-work",
    start="write_record",
    steps={"write_record": Step(body=write_record, outcomes=str)},
    actor="adoption_record_worker",
    queue=LEAF_QUEUE,
)


async def synchronize_pages(ctx: StepContext, deps: SyncDeps) -> None:
    """Re-drive only the uncommitted provider page after slot-free parking."""
    schema = deps.schema
    entry_id = await deps.pool.fetchval(
        f'INSERT INTO "{schema}".adoption_sync_source_entries '
        "(scenario, flow_id, job_id, pid, worker_id, logical_attempt, claim_epoch) "
        "VALUES ($1, $2, $3, $4, $5::uuid, $6, $7) RETURNING id",
        deps.scenario,
        ctx.flow_id,
        ctx.job_id,
        os.getpid(),
        deps.worker_id,  # per-process factory witness (public WorkflowDepsContext.worker_id)
        ctx.attempt,
        ctx.claim_epoch,
    )
    checkpoint = await ctx.cursor()
    raw_cursor = checkpoint.get("next_cursor")
    cursor = ProviderCursor.model_validate(raw_cursor) if raw_cursor is not None else None
    while True:
        page = await deps.provider.fetch(cursor, PAGE_WIDTH)
        await deps.pool.execute(
            f'INSERT INTO "{schema}".adoption_sync_fetches '
            "(scenario, source_entry_id, cursor_in, cursor_out, record_ids) "
            "VALUES ($1, $2, $3, $4, $5::jsonb)",
            deps.scenario,
            entry_id,
            None if cursor is None else cursor.token,
            None if page.next_cursor is None else page.next_cursor.token,
            json.dumps([record.record_id for record in page.records]),
        )
        children = await _page_children(deps, page.records, cursor, page.next_cursor)
        if not page.records:
            break
        await ctx.emit_batch(
            children,
            cursor={"next_cursor": None if page.next_cursor is None else page.next_cursor.token},
            backpressure_timeout_s=30.0,
        )
        await ctx.progress(
            50,
            "page emitted",
            {"records": len(page.records), "logical_attempt": ctx.attempt},
        )
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    await deps.pool.execute(
        f'UPDATE "{schema}".adoption_sync_source_entries '
        "SET body_returned_at = clock_timestamp() WHERE id = $1",
        entry_id,
    )


async def _page_children(
    deps: SyncDeps,
    records: tuple[ProviderRecord, ...],
    cursor: ProviderCursor | None,
    next_cursor: ProviderCursor | None,
):
    """Resolve exact snapshot ordinals into stable per-record identities.

    A just-fetched nonterminal page's *input* cursor addresses the immutable
    snapshot; for the first page, the output cursor does.  The final short page
    has no output cursor, so its input cursor is used.  An empty final page is
    never passed here by the source.

    The element type is the engine's emit-child spec, inferred from the public
    ``chain_start`` factory (which keeps the required ``map_index`` ordinal and
    per-record ``trace_id`` stamps).  It is deliberately not imported from the
    private ``taskq.workflows._types`` module; re-exporting that class from
    ``taskq.workflows`` is a future P9 public-surface decision, not made here.
    """
    snapshot_cursor = cursor or next_cursor
    rows = (
        await deps.provider.snapshot_record_identities(snapshot_cursor)
        if snapshot_cursor is not None
        else await deps.provider.record_identities()
    )
    identities = {row.record_id: row.ordinal for row in rows}
    return [
        chain_start(
            RECORD_CHAIN,
            {
                "record_id": record.record_id,
                "revision": record.revision,
                "content": record.content,
            },
            map_index=identities[record.record_id] - 1,
            trace_id=f"record:{record.record_id}",
        )
        for record in records
    ]


paged_sync_app: Final = WorkflowApp(deps_factory=make_sync_deps)


@paged_sync_app.workflow(FLOW_NAME, queue=SOURCE_QUEUE, max_in_flight=PAGE_WIDTH)
def generic_paged_sync() -> Promise[object]:
    return build(
        chain_source(
            RECORD_CHAIN,
            synchronize_pages,
            key="source",
            actor="adoption_source_worker",
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
        schema = _schema_name(os.environ["TASKQ_SCHEMA_NAME"])
        await conn.execute(
            f'INSERT INTO "{schema}".adoption_sync_probe_runs '
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


source_probe = actor(name="adoption_source_probe", queue=SOURCE_QUEUE)(_probe)
leaf_probe = actor(name="adoption_record_probe", queue=LEAF_QUEUE)(_probe)
sibling_probe = actor(name="adoption_sibling_probe", queue=SIBLING_QUEUE)(_probe)


async def initialize_sample(
    pool: asyncpg.Pool,
    *,
    schema: str,
    scenario: str,
    records: tuple[ProviderRecord, ...] | None = None,
) -> None:
    """Initialize observer/control tables and the shared provider scenario."""
    schema = _schema_name(schema)
    await pool.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_control (
            scenario text PRIMARY KEY,
            release_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_factory_events (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            pid integer NOT NULL,
            worker_id uuid NOT NULL,
            event text NOT NULL,
            record_count integer NOT NULL,
            occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_source_entries (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            flow_id uuid NOT NULL,
            job_id uuid NOT NULL,
            pid integer NOT NULL,
            worker_id uuid NOT NULL,
            logical_attempt integer NOT NULL,
            claim_epoch bigint NOT NULL,
            entered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            body_returned_at timestamptz
        );
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_fetches (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            source_entry_id bigint NOT NULL,
            cursor_in text,
            cursor_out text,
            record_ids jsonb NOT NULL,
            fetched_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_leaf_runs (
            id bigserial PRIMARY KEY,
            scenario text NOT NULL,
            flow_id uuid NOT NULL,
            job_id uuid NOT NULL,
            map_index bigint NOT NULL,
            record_id text NOT NULL,
            pid integer NOT NULL,
            worker_id uuid NOT NULL,
            attempt integer NOT NULL,
            started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            released_at timestamptz,
            body_returned_at timestamptz,
            UNIQUE (scenario, job_id, attempt)
        );
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_records (
            scenario text NOT NULL,
            flow_id uuid NOT NULL,
            job_id uuid NOT NULL,
            record_id text NOT NULL,
            content text NOT NULL,
            applied_by_pid integer NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (scenario, flow_id, record_id)
        );
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_probe_runs (
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
        CREATE TABLE IF NOT EXISTS "{schema}".adoption_sync_admission_reads (
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
        f'INSERT INTO "{schema}".adoption_sync_control (scenario) VALUES ($1) '
        "ON CONFLICT (scenario) DO NOTHING",
        scenario,
    )
    provider = PgPagedProvider(
        pool,
        schema=schema,
        scenario=scenario,
        max_records=32,
        max_page_size=PAGE_WIDTH,
        max_snapshots=8,
        max_cursors=64,
    )
    await provider.initialize()
    await provider.replace_records(
        records
        or tuple(
            ProviderRecord(record_id=f"record-{index}", revision=0, content=f"value-{index}")
            for index in range(RECORD_COUNT)
        )
    )


async def main() -> None:
    """Initialize and create a fleet-executed run from environment settings."""
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = _schema_name(os.environ["TASKQ_SCHEMA_NAME"])
    scenario = os.environ.get("ADOPTION_SYNC_SCENARIO", "local")
    pool = await asyncpg.create_pool(dsn)
    try:
        await initialize_sample(pool, schema=schema, scenario=scenario)
        runner = FlowRunner(paged_sync_app.get(FLOW_NAME), pool, schema)
        try:
            claim = await runner.create_flow(run_key=f"generic-sync:{scenario}")
            print(json.dumps({"scenario": scenario, "flow_id": str(claim.flow_id)}))
        finally:
            await runner.aclose()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
