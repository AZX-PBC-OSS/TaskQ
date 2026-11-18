# ruff: noqa: S608,ASYNC109  # Why: the schema is the settings-validated identifier and every data value is $-bound; the transparent observer mirrors asyncpg's native fetchval(timeout=...) surface.
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
import enum
import faulthandler
import json
import os
import signal
import sys
from collections.abc import Awaitable, Callable
from typing import Any, Final

import asyncpg
from pydantic import BaseModel

from taskq import JobContext, actor
from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from taskq.workflows import (
    DONE,
    Chain,
    EmitBackpressure,
    Promise,
    Route,
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

    The cursor token is the NEXT ordinal to fetch (inclusive): the first
    page's token names the first unseen ordinal, so a resumed page
    re-reads exactly the parked page's records. ``next_cursor`` is
    ``None`` exactly when no records remain after this page, so a
    fully-consumed set terminates without an empty fetch."""
    after = int(cursor) if cursor is not None else 0
    rows = await conn.fetch(
        f'SELECT ordinal, record_id, content FROM "{schema}".p2sat_records '
        "WHERE scenario_id = $1 AND ordinal >= $2 ORDER BY ordinal LIMIT $3",
        scenario,
        after,
        PAGE_WIDTH,
    )
    page = [dict(row) for row in rows]
    last = int(page[-1]["ordinal"]) if page else after - 1
    remaining = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".p2sat_records WHERE scenario_id = $1 AND ordinal > $2',
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


# ── the §9 control 4's own declarations (the pods resolve the flow's rows
# through THIS entry's registry; a test-local WorkflowApp is invisible to
# them) ────────────────────────────────────────────────────────────────────
class C4Ok(enum.Enum):
    OK = "ok"


_C4_GATE_S: Final = 3.0


async def _c4_screen(ctx: StepContext, item: object) -> C4Ok:
    del item
    # The page-0 children hold their slots for the gate's own span (the
    # committed-intent window's precondition); the resumed page-1 child
    # runs free.
    if ctx.map_index is not None and ctx.map_index < 2:
        await asyncio.sleep(_C4_GATE_S)
    return C4Ok.OK


async def _c4_source_body(ctx: Any) -> None:
    cursor = await ctx.cursor()
    start_page = int(cursor.get("page", -1)) + 1
    pages = ([0, 1], [2])
    for page in range(start_page, len(pages)):
        await ctx.emit_batch(
            [
                chain_start(_C4_CHAIN, {"app_id": m}, map_index=m, trace_id=f"app-{m}")
                for m in pages[page]
            ],
            cursor={"page": page},
        )


_C4_CHAIN: Final = Chain(
    name=f"c4-{new_uuid()}",
    start="screen",
    queue=SOURCE_QUEUE,
    steps={"screen": Step(body=_c4_screen, outcomes=C4Ok, route=Route({C4Ok.OK: DONE}))},
)


@p2sat_app.workflow("c4_kill_recovery_flow", queue=SOURCE_QUEUE, max_in_flight=2)
def c4_kill_recovery_flow() -> Promise[object]:
    return build(
        chain_source(
            _C4_CHAIN,
            _c4_source_body,
            key="source",
            actor=_SOURCE_ACTOR,
            max_attempts=2,
            queue=SOURCE_QUEUE,
        )
    )


async def _c4b_screen(ctx: StepContext, item: object) -> C4Ok:
    del item
    if ctx.map_index == 0:
        await asyncio.sleep(_C4_GATE_S)
    return C4Ok.OK


async def _c4b_source_body(ctx: Any) -> None:
    cursor = await ctx.cursor()
    start_page = int(cursor.get("page", -1)) + 1
    pages = ([0], [1])
    for page in range(start_page, len(pages)):
        await ctx.emit_batch(
            [
                chain_start(_C4B_CHAIN, {"app_id": m}, map_index=m, trace_id=f"app-{m}")
                for m in pages[page]
            ],
            cursor={"page": page},
        )


_C4B_CHAIN: Final = Chain(
    name=f"c4b-{new_uuid()}",
    start="screen",
    queue=SOURCE_QUEUE,
    steps={"screen": Step(body=_c4b_screen, outcomes=C4Ok, route=Route({C4Ok.OK: DONE}))},
)


@p2sat_app.workflow("c4_park_kill_flow", queue=SOURCE_QUEUE, max_in_flight=1)
def c4_park_kill_flow() -> Promise[object]:
    return build(
        chain_source(
            _C4B_CHAIN,
            _c4b_source_body,
            key="source",
            actor=_SOURCE_ACTOR,
            max_attempts=2,
            queue=SOURCE_QUEUE,
        )
    )


# ── the §9 control 4's MID-CHUNK estate (the many-waiter flow): the
# source emits CHUNK_ESTATE A-children at the flow's full bound; each A's
# own body raises the product's own EmitBackpressure on its FIRST run
# (the real body-seam park path — the runner's step-body catch), so every
# A parks deterministically; the post-promotion re-run sees the A's own
# gate row (the effect table) and completes. The gate rows are committed
# autonomously by the body before the raise, so the park's rollback never
# erases the discriminator. ────────────────────────────────────────────
CHUNK_ESTATE: Final = 400


async def _c4c_spawn(ctx: StepContext, item: object) -> C4Ok:
    del item
    # The FIRST run holds its claim briefly (the control's admission-lock
    # seam lands in this window, before any park exists), then stamps the
    # gate row (committed autonomously — the park's rollback never erases
    # the discriminator) and parks through the product's own park path.
    conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    try:
        inserted = await conn.fetchval(
            'INSERT INTO "'
            + os.environ["TASKQ_SCHEMA_NAME"]
            + '".p2sat_c4_gate (flow_id, map_index) VALUES ($1::uuid, $2) '
            "ON CONFLICT DO NOTHING RETURNING 1",
            str(ctx.flow_id),
            int(ctx.map_index if ctx.map_index is not None else 0),
        )
    finally:
        await conn.close()
    if inserted is None:
        # The re-run after the promotion: this A's gate row exists, so
        # the estate's park window is over and this A completes.
        return C4Ok.OK
    await asyncio.sleep(2.0)
    # The FIRST run: park this A through the product's own park path —
    # the outstanding is this body's own honest read of the live estate,
    # and the bound is THIS flow's own declared max_in_flight read off
    # the wired ctx (the shared chain serves estates declaring different
    # bounds — c4_chunk_flow 400, c4_seed_flow 800 — so a hard-coded
    # constant drifts the estate's declaration and the body's raise apart;
    # the ctx wiring is the declaration's own runtime face).
    declared = ctx._max_in_flight
    if declared is None:
        # The explicit unbounded declaration has no park bound to raise
        # with; fabricating one would be exactly the drift this body
        # refuses. Fail closed.
        raise RuntimeError(
            "the c4 spawn estate's flow declared no max_in_flight; "
            "the park raise has no declared bound to present"
        )
    conn2 = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    try:
        outstanding = int(
            await conn2.fetchval(
                'SELECT count(*) FROM "'
                + os.environ["TASKQ_SCHEMA_NAME"]
                + "\".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid "
                "AND status IN ('pending', 'running')",
                str(ctx.flow_id),
            )
        )
    finally:
        await conn2.close()
    raise EmitBackpressure(
        page_width=1,
        bound=declared,  # this flow's own declared max_in_flight
        outstanding=outstanding,
    )


_C4C_CHAIN: Final = Chain(
    name=f"c4c-{new_uuid()}",
    start="spawn",
    queue=SOURCE_QUEUE,
    steps={"spawn": Step(body=_c4c_spawn, outcomes=C4Ok, route=Route({C4Ok.OK: DONE}))},
)


async def _c4c_source_body(ctx: Any) -> None:
    await ctx.emit_batch(
        [
            chain_start(_C4C_CHAIN, {"app_id": i}, map_index=i, trace_id=f"c4c-{i}")
            for i in range(CHUNK_ESTATE)
        ],
        cursor={"page": 0},
    )


@p2sat_app.workflow("c4_chunk_flow", queue=SOURCE_QUEUE, max_in_flight=CHUNK_ESTATE)
def c4_chunk_flow() -> Promise[object]:
    return build(
        chain_source(
            _C4C_CHAIN,
            _c4c_source_body,
            key="source",
            actor=_SOURCE_ACTOR,
            max_attempts=2,
            queue=SOURCE_QUEUE,
        )
    )


async def _c4seed_source_body(ctx: Any) -> None:
    """The seeded estate's own source: the estate's park rows are SEEDED
    (the §9.2 "SEEDED, never engine-born" pattern), so the source's only
    work is to terminalize."""
    del ctx


FLOW600_ESTATE: Final = 600  # the §9 control 8's flow_600 waiter estate


# ── the §9 control 7's occupancy estate: the consuming child's own actor
# (its own queue + the projected cohort) — the actor's own capacity gate
# (actor_config.max_concurrent = 1, seeded by the control) serialises the
# children; the source's park + the wake must never need a slot. ────────
C7_CHILD_QUEUE: Final = "p2sat_c7_child"


async def _c7_child_body(ctx: StepContext, item: object) -> C4Ok:
    del item
    # The occupancy's own hold: the child holds its actor's ONE slot for
    # the gate's span (the control passes the span through the gate
    # table).
    import asyncio as _aio

    conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
    try:
        hold = await conn.fetchval(
            'SELECT hold_s FROM "'
            + os.environ["TASKQ_SCHEMA_NAME"]
            + '".p2sat_c7_gate WHERE id = 1'
        )
    finally:
        await conn.close()
    if hold is not None:
        await _aio.sleep(float(hold))
    return C4Ok.OK


_C7_CHAIN: Final = Chain(
    name=f"c7-{new_uuid()!s}",
    start="child",
    actor="p2sat_c7_child",
    queue=C7_CHILD_QUEUE,
    steps={"child": Step(body=_c7_child_body, outcomes=C4Ok, route=Route({C4Ok.OK: DONE}))},
)


def _aio_monotonic() -> float:
    import time as _t

    return _t.monotonic()


async def _c7_source_body(ctx: Any) -> None:
    cursor = await ctx.cursor()
    start_page = int(cursor.get("page", -1)) + 1
    for page in range(start_page, 2):
        await ctx.emit_batch(
            [
                chain_start(
                    _C7_CHAIN,
                    {"app_id": page},
                    map_index=page,
                    trace_id=f"c7-app-{page}",
                )
            ],
            cursor={"page": page},
        )
        if page == 0:
            # The occupancy's own shape: the FIRST child must be LIVE
            # (claimed, holding its actor's one slot) when the second
            # page's admission runs — the source OBSERVES the fleet's own
            # dispatch (the child's row flips running) instead of guessing
            # a duration; the page-1 emit then sees charge 1 and parks
            # the source deterministically.
            conn = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
            try:
                schema_name = os.environ["TASKQ_SCHEMA_NAME"]
                deadline = _aio_monotonic() + 60.0
                while _aio_monotonic() < deadline:
                    state = await conn.fetchval(
                        'SELECT status FROM "'
                        + schema_name
                        + "\".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid "
                        "AND step_key = 'child' AND map_index = 0",
                        ctx.flow_id,
                    )
                    if state == "running":
                        break
                    await asyncio.sleep(0.05)
            finally:
                await conn.close()


@p2sat_app.workflow("c7_occupancy_flow", queue=SOURCE_QUEUE, max_in_flight=1)
def c7_occupancy_flow() -> Promise[object]:
    return build(
        chain_source(
            _C7_CHAIN,
            _c7_source_body,
            key="source",
            actor=_SOURCE_ACTOR,
            max_attempts=2,
            queue=SOURCE_QUEUE,
        )
    )


async def _c4f600_source_body(ctx: Any) -> None:
    """The flow_600 estate's own source: the 600 park rows are SEEDED (the
    §9.2 pattern); the source only terminalizes."""
    del ctx


@p2sat_app.workflow("c4_flow600", queue=SOURCE_QUEUE, max_in_flight=FLOW600_ESTATE * 2)
def c4_flow600() -> Promise[object]:
    return build(
        chain_source(
            _C4C_CHAIN,
            _c4f600_source_body,
            key="source",
            actor=_SOURCE_ACTOR,
            max_attempts=2,
            queue=SOURCE_QUEUE,
        )
    )


@p2sat_app.workflow("c4_seed_flow", queue=SOURCE_QUEUE, max_in_flight=CHUNK_ESTATE * 2)
def c4_seed_flow() -> Promise[object]:
    return build(
        chain_source(
            _C4C_CHAIN,
            _c4seed_source_body,
            key="source",
            actor=_SOURCE_ACTOR,
            max_attempts=2,
            queue=SOURCE_QUEUE,
        )
    )


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
    # THE §4.2 CENSUS, logged PERIODICALLY by this proof pod: the counts
    # are cumulative, so the LAST census line in the pod's log is its
    # end-of-run census (within the cadence's tail). The lane's
    # pre-registered V reads the two pods' last snapshots from their worker
    # logs. Test-owned proof subprocesses only; production workers never
    # log the census.
    import structlog

    from taskq.workflows._emit import COUNTERS

    async def _census_logger() -> None:
        while True:
            await asyncio.sleep(10.0)
            structlog.get_logger("p2sat.census").warning(
                "p2sat-census",
                **{key: int(value) for key, value in COUNTERS.snapshot().items()},
            )

    census_task = asyncio.create_task(_census_logger())
    try:
        code = await _main(
            settings,
            actor_registry={
                "p2sat_source_probe": source_probe,
                "p2sat_record_probe": leaf_probe,
                "p2sat_sibling_probe": sibling_probe,
            },
        )
    finally:
        census_task.cancel()
        # The FINAL snapshot at return (the loop's own 10s cadence may
        # miss the tail's arms): the `final=True` field is the reader's
        # own discriminator — the control asserts this line EXISTS for
        # every pod and reads IT (never a periodic line) as the pod's
        # census.
        structlog.get_logger("p2sat.census").warning(
            "p2sat-census",
            final=True,
            **{key: int(value) for key, value in COUNTERS.snapshot().items()},
        )
    return code


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
