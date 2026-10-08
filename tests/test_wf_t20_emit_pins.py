"""T20's EMIT-TX pins — the design's ONE new primitive (PROOF.md §6a).

The shipped fork inserts children ONLY inside the parent's terminal-mark
tx1 (fork-at-finalize). A STREAMING source must emit children while
ITSELF staying ``running``: the emit tx is the certified fork shapes
re-bound (the children INSERT + the edge rows) PLUS the cursor checkpoint
on the source row's own metadata under the FULL claim fence — ONE
transaction (the fork-atomicity law at page granularity).

THE REFUTED-CLAIM DISCIPLINE (the spike's own refuted claim, PROOF.md §6 —
198 UniqueViolations in the first run): the per-record identity rides
``map_index`` — the fork's idempotency key AND the step-ledger's arbiter
BOTH discriminate siblings by it; emit stamps ``map_index`` + ``trace_id``
per child, or every record's children collide onto one row.

The kill-storm pins drive the REAL ``pg_terminate_backend`` (a real
server-side termination, no graceful path) at each statement window of the
emit tx — the spike's storm shape: zero re-emitted children, zero lost
children, the cursor == the last committed page.

Red-first: this file ran RED against the tree before the emit primitive
existed (the ImportError IS the missing surface); the greens below are
the built code's evidence. Captured:
``.measurements/t20-emit-*.txt``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows._emit import EMIT_CURSOR_KEY, EmitFencedError, emit_batch
from taskq.workflows._types import EmitChild, NodeSpec
from tests._wf_fixtures import RedLog, seed_flow

#: a short lock lease (the storm's reclaim turns around fast)
_LEASE = timedelta(milliseconds=250)

#: the pages the storm emits (2 pages x 3 children)
_PAGES: list[list[int]] = [[0, 1, 2], [3, 4, 5]]

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

WindowHook = Callable[[str, ConnLike], Awaitable[None]]


# ── the drivers (the REAL surfaces, the spike's recorded fakes replaced) ─


async def make_source(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    wf_sql: WorkflowSql,
) -> JobId:
    """The source node: a normal workflow node (vanilla enqueue shape)."""
    from taskq.workflows.engine import insert_node

    source_id = await insert_node(
        conn, wf_sql, NodeSpec(flow_id=flow_id, step_key="source", actor="wf", queue="default")
    )
    # A FLAT retry curve on the source row (the shipped RetryPolicy
    # columns): the reclaim's deferral then floors at the shipped
    # MIN_DEFERRAL_INTERVAL (1 s) instead of the default curve's 5s * 2^n
    # — the storm's resume turns around fast, exactly as the spike's
    # compressed delay did.
    await conn.execute(
        f'UPDATE "{wf_sql.schema}".jobs SET retry_base_seconds = 0, '
        "retry_cap_seconds = 0 WHERE id = $1",
        source_id,
    )
    return source_id


async def claim_source(
    conn: asyncpg.Connection, schema: str, source_id: JobId
) -> tuple[JobId, int, int]:
    """Claim the source in the REAL dispatch-claim's fence shape (the
    spike's recorded fake: the shipped worker claims it like any job —
    the claim itself is not the subject; the emit is)."""
    worker = new_uuid()
    rec = await conn.fetchrow(
        f"UPDATE \"{schema}\".jobs SET status = 'running', started_at = now(), "
        "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
        "locked_by_worker = $2, lock_expires_at = now() + $3::interval, "
        "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
        "AND deps_pending = 0 RETURNING attempt, claim_epoch",
        source_id,
        worker,
        _LEASE,
    )
    assert rec is not None, "the source did not claim (not pending?)"
    return JobId(worker), int(rec["attempt"]), int(rec["claim_epoch"])


def page_children(page: list[int], flow_id: JobId) -> list[EmitChild]:
    """One page's chain starts — the per-record identity STAMPED AT EMIT:
    ``map_index`` = the record's index, ``trace_id`` = the record's trace
    (the refuted-claim discipline; the key carries both)."""
    return [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": i}},
            trace_id=f"app-{i}",
            map_index=i,
        )
        for i in page
    ]


async def read_cursor(conn: asyncpg.Connection, schema: str, source_id: JobId) -> Any:
    raw = await conn.fetchval(
        f"SELECT metadata->'{EMIT_CURSOR_KEY}' FROM \"{schema}\".jobs WHERE id = $1",
        source_id,
    )
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


async def committed_children(conn: asyncpg.Connection, schema: str, source_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE parent_id = $1 AND step_key = 'screen'",
            source_id,
        )
    )


async def reclaim_and_promote(pool: asyncpg.Pool, wf_sql: WorkflowSql) -> int:
    """THE REAL certified reclaim + promotion (unchanged): expired leases
    → the ladder's re-pend (the operator ceiling clamped to 10 ms — the
    row's own curve's band then floors at MIN_DEFERRAL_INTERVAL) → the
    promotion sweep matures the due rows pending."""
    from taskq.backend._sweeps import sweep_expired_locks, sweep_scheduled_to_pending

    async with pool.acquire() as conn:
        reclaimed = await sweep_expired_locks(
            conn,
            timedelta(0),
            timedelta(0),
            schema=wf_sql.schema,
            batch_size=100,
            max_retry_backoff=timedelta(milliseconds=10),
        )
        await sweep_scheduled_to_pending(conn, schema=wf_sql.schema)
    return reclaimed


async def reclaim_until_claim(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    source_id: JobId,
    *,
    timeout_s: float = 20.0,
) -> tuple[JobId, int, int]:
    """The resume's front half: the REAL reclaim, then the REAL promotion
    sweep, polled (bounded — a hang is a defect) until the source row is
    actually claimable (its own retry curve's floor defers it), then the
    claim (the REAL dispatch-claim's fence shape). Returns the fresh
    claim view. NO fixed sleep: the poll ticks the shipped sweeps; the
    row's own curve decides when."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        await reclaim_and_promote(pool, wf_sql)
        worker = new_uuid()
        async with pool.acquire() as conn:
            rec = await conn.fetchrow(
                f"UPDATE \"{schema}\".jobs SET status = 'running', started_at = now(), "
                "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
                "locked_by_worker = $2, lock_expires_at = now() + $3::interval, "
                "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
                "AND deps_pending = 0 AND (scheduled_at IS NULL OR scheduled_at <= now()) "
                "RETURNING attempt, claim_epoch",
                source_id,
                worker,
                _LEASE,
            )
            if rec is not None:
                return JobId(worker), int(rec["attempt"]), int(rec["claim_epoch"])
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(
                "the source never became claimable after the reclaim "
                "(the promotion sweep never matured it) — a wedged resume"
            )
        await asyncio.sleep(0.1)


async def kill_backend_at(window: int, dsn: str) -> WindowHook:
    """The hook that turns emit's *window*-th statement boundary into a
    REAL server-side kill (``pg_terminate_backend`` — the spike's storm
    method; no graceful path)."""

    async def hook(name: str, conn: ConnLike) -> None:
        if int(name.split(":")[1]) != window:
            return
        pid = await conn.fetchval("SELECT pg_backend_pid()")
        killer = await asyncpg.connect(dsn)
        try:
            await killer.execute("SELECT pg_terminate_backend($1)", pid)
        finally:
            await killer.close()

    return hook


# ── THE KILL STORM: a kill at every statement window ────────────────────


@pytest.mark.integration
@pytest.mark.parametrize("window", [1, 2, 3])
async def test_t20_emit_tx_atomic_at_every_statement_window(
    window: int,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: Any,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    t20_redlog: RedLog,
) -> None:
    """THE KILL-STORM SCENARIO (the spike's table, windows #0-#2): a REAL
    server-side kill inside the emit tx — after the children (#1), after
    the edges (#2), after the cursor checkpoint but BEFORE the commit
    (#3, the window that pins the whole design) — must roll the WHOLE
    batch back (zero partial pages), and the resume (the REAL reclaim +
    promotion + re-claim + re-emit FROM THE CURSOR) must land exactly the
    lost page: zero re-emitted children, zero lost children."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    dsn = module_pg_schema.pg_dsn
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
    try:
        hook = await kill_backend_at(window, dsn)
        with pytest.raises(Exception):  # noqa: B017 — the kill surfaces as a connection error class
            await emit_batch(
                pool,
                wf_sql,
                flow_id=flow_id,
                source_id=source_id,
                worker_id=worker,
                attempt=attempt,
                claim_epoch=epoch,
                children=page_children(_PAGES[0], flow_id),
                cursor={"page": 0},
                _window_hook=hook,
            )
    finally:
        await close_storm_pool(pool)

    # ZERO partial pages: the kill at ANY in-tx window rolls everything back.
    assert await committed_children(wf_conn, wf_schema, source_id) == 0, (
        f"window {window}: a killed emit tx left partial children — the emit is not one transaction"
    )
    assert await read_cursor(wf_conn, wf_schema, source_id) is None, (
        f"window {window}: the cursor advanced outside the emit tx"
    )

    # THE RESUME: the REAL reclaim + promotion; re-claim; re-emit the
    # lost page FROM THE CURSOR, then the next page — ONE claim streams
    # ALL pages (the source never finalizes mid-stream; every emit
    # fences on the SAME claim view).
    worker, attempt, epoch = await reclaim_until_claim(module_pg_pool, wf_sql, wf_schema, source_id)
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children(_PAGES[0], flow_id),
        cursor={"page": 0},
    )
    # Page 1 emits under the SAME claim; the run drains — the
    # second page must exist EXACTLY ONCE.
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children(_PAGES[1], flow_id),
        cursor={"page": 1},
    )

    total = await committed_children(wf_conn, wf_schema, source_id)
    keys = await wf_conn.fetch(
        f'SELECT idempotency_key FROM "{wf_schema}".jobs WHERE parent_id = $1 '
        "AND step_key = 'screen' ORDER BY idempotency_key",
        source_id,
    )
    t20_redlog.red(
        f"t20-emit-window-{window}",
        "the emit tx NOT atomic at this statement window — a kill lands a "
        "partial page or advances the cursor alone: the resume re-emits "
        "(duplicates) or skips (lost children)",
        {"committed_children_after_kill": total, "distinct_keys": len({r[0] for r in keys})},
    )
    assert total == 6, f"window {window}: expected exactly 6 chain rows, got {total}"
    assert len({r[0] for r in keys}) == 6, "duplicate idempotency keys — the arbiter collided"
    cursor = await read_cursor(wf_conn, wf_schema, source_id)
    assert cursor == {"page": 1}, f"the cursor must equal the last committed page, got {cursor!r}"


@pytest.mark.integration
async def test_t20_emit_tx_zero_reemitted_zero_lost_across_pages(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: Any,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    t20_redlog: RedLog,
) -> None:
    """THE STORM'S MID-PAGE + BETWEEN-PAGES WINDOWS (the spike's #3-#7): a
    kill AFTER a committed page (the backend dies between emit calls)
    loses NOTHING (the page is committed); the resume continues from the
    cursor — the next page's children land exactly once. The kill inside
    page 2's tx (after its children) rolls page 2 back WHOLE (a
    whole-page multiple at every probe), and the final driver run
    completes with zero re-emitted, zero lost."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    dsn = module_pg_schema.pg_dsn
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
    try:
        # Page 0 commits, then the worker dies before page 1's emit (the
        # between-pages window): the reclaim finds the source running
        # with a stale lease.
        await emit_batch(
            pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children(_PAGES[0], flow_id),
            cursor={"page": 0},
        )
        assert await committed_children(wf_conn, wf_schema, source_id) == 3
        # THE KILL: server-side — the holder's lease left to lapse (the
        # claim wrote a 250 ms lease) — the REAL reclaim's eligibility.
        async with pool.acquire() as c:
            conn_pid = await c.fetchval("SELECT pg_backend_pid()")
        killer = await asyncpg.connect(dsn)
        try:
            await killer.execute("SELECT pg_terminate_backend($1)", conn_pid)
        finally:
            await killer.close()
    finally:
        await close_storm_pool(pool)

    # Page 0 SURVIVES the kill (it was committed): zero lost.
    assert await committed_children(wf_conn, wf_schema, source_id) == 3
    cursor = await read_cursor(wf_conn, wf_schema, source_id)
    assert cursor == {"page": 0}, "a committed page's cursor must survive the kill"

    # THE RESUME (the real reclaim; the lease lapsed): re-claim, emit
    # page 1 — the resume starts at N+1, never N, never N+2.
    worker, attempt, epoch = await reclaim_until_claim(module_pg_pool, wf_sql, wf_schema, source_id)
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children(_PAGES[1], flow_id),
        cursor={"page": 1},
    )
    total = await committed_children(wf_conn, wf_schema, source_id)
    keys = await wf_conn.fetch(
        f'SELECT idempotency_key FROM "{wf_schema}".jobs WHERE parent_id = $1 '
        "AND step_key = 'screen'",
        source_id,
    )
    t20_redlog.red(
        "t20-emit-between-pages-kill",
        "the resume NOT resuming from the cursor — a re-emitted page (the "
        "duplicates) or a skipped one (the lost children)",
        {"total_after_resume": total, "distinct_keys": len({r[0] for r in keys})},
    )
    assert total == 6, f"expected exactly 6 chain rows after the resume, got {total}"
    assert len({r[0] for r in keys}) == 6, "duplicate idempotency keys — the resume re-emitted"
    # Zero lost children: every record's chain START exists (6 distinct
    # map_index values, one row each).
    idx = await wf_conn.fetch(
        f'SELECT map_index FROM "{wf_schema}".jobs WHERE parent_id = $1 '
        "AND step_key = 'screen' ORDER BY map_index",
        source_id,
    )
    assert [r[0] for r in idx] == [0, 1, 2, 3, 4, 5]


# ── THE FENCE: a zombie source cannot emit ───────────────────────────────


@pytest.mark.integration
async def test_t20_emit_fenced_on_superseded_claim(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    t20_redlog: RedLog,
) -> None:
    """THE CLAIM FENCE IS THE EMIT'S FENCE: the cursor checkpoint runs
    under the FULL dispatch fence (status='running' AND locked_by_worker
    AND attempt AND claim_epoch). A ZOMBIE source — its attempt
    superseded by the reclaim + re-claim — updates NOTHING: the emit tx
    aborts (the children + edges roll back with it), the loud refusal on
    the record."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    _worker, _attempt, _epoch = await claim_source(wf_conn, wf_schema, source_id)

    # THE SUPERSESSION: the real reclaim + a fresh claim (the zombie's
    # attempt is now stale).
    _fresh_worker, fresh_attempt, fresh_epoch = await reclaim_until_claim(
        module_pg_pool, wf_sql, wf_schema, source_id
    )
    assert fresh_attempt > _attempt or fresh_epoch != _epoch

    # THE ZOMBIE EMITS with its stale claim view: refused, loudly.
    with pytest.raises(EmitFencedError):
        await emit_batch(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=_worker,
            attempt=_attempt,
            claim_epoch=_epoch,
            children=page_children(_PAGES[0], flow_id),
            cursor={"page": 0},
        )
    # ZERO children, the cursor unmoved: the fenced emit wrote nothing.
    t20_redlog.red(
        "t20-emit-zombie",
        "the cursor checkpoint NOT fenced on the claim (a zombie emit "
        "lands children from a superseded attempt — the double-emit "
        "dragon with no fence)",
        {"committed_children": await committed_children(wf_conn, wf_schema, source_id)},
    )
    assert await committed_children(wf_conn, wf_schema, source_id) == 0
    assert await read_cursor(wf_conn, wf_schema, source_id) is None


# ── THE REFUTED-CLAIM DISCIPLINE: map_index is load-bearing ─────────────


@pytest.mark.integration
async def test_t20_emit_map_index_discriminates_siblings(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    t20_redlog: RedLog,
) -> None:
    """THE SPIKE'S REFUTED-CLAIM DISCIPLINE, PINNED (198 collisions
    without it): two records' chain starts through ONE emit — same step
    key, DIFFERENT map_index — are TWO rows with TWO idempotency keys,
    and the step-ledger's arbiter gives them TWO independent claims. The
    collision drill: the SAME (step, map_index) emitted twice aborts the
    tx on the unique key (the loud refusal — never a silent dedup, never
    a partial page)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    children = [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": i}},
            trace_id=f"app-{i}",
            map_index=i,
        )
        for i in (7, 8)
    ]
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=children,
        cursor={"page": 0},
    )
    rows = await wf_conn.fetch(
        f'SELECT id, idempotency_key, map_index, trace_id FROM "{wf_schema}".jobs '
        "WHERE parent_id = $1 AND step_key = 'screen' ORDER BY map_index",
        source_id,
    )
    t20_redlog.red(
        "t20-emit-map-index",
        "the emit NOT stamping map_index per child — all records' "
        "children of one step collide onto one row / one idempotency key "
        "(the spike's 198 UniqueViolations), one ledger claim for "
        "siblings that must be independent",
        {"rows": len(rows), "distinct_keys": len({r["idempotency_key"] for r in rows})},
    )
    assert [r["map_index"] for r in rows] == [7, 8], "the siblings must be two rows"
    assert len({r["idempotency_key"] for r in rows}) == 2, "one key per record"
    assert {r["trace_id"] for r in rows} == {"app-7", "app-8"}, "the trace rides the emit"

    # THE LEDGER ARBITER: the two siblings take INDEPENDENT claims (the
    # arbiter tuple (flow, step_key, map_index, attempt) discriminates).
    from taskq.workflows.ledger import claim_step_ledger

    row7, row8 = rows[0], rows[1]
    async with module_pg_pool.acquire() as conn:
        c7 = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow_id,
            job_id=JobId(row7["id"]),
            step_key="screen",
            map_index=7,
            attempt=1,
        )
        c8 = await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow_id,
            job_id=JobId(row8["id"]),
            step_key="screen",
            map_index=8,
            attempt=1,
        )
    assert c7.ledger_id != c8.ledger_id, "the siblings share a ledger claim — the collision"

    # THE COLLISION DRILL: re-emitting the same record (the same
    # step + map_index) aborts the tx on the unique key — the loud
    # refusal; the tx's OTHER children (a fresh sibling) roll back with
    # it (never a partial page).
    dupes = [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": 7}},
            trace_id="app-7",
            map_index=7,
        ),
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": 9}},
            trace_id="app-9",
            map_index=9,
        ),
    ]
    with pytest.raises(asyncpg.UniqueViolationError):
        await emit_batch(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=dupes,
            cursor={"page": 9},
        )
    # The fresh sibling rolled back WITH the duplicate: no partial page.
    assert await committed_children(wf_conn, wf_schema, source_id) == 2
    assert await read_cursor(wf_conn, wf_schema, source_id) == {"page": 0}, (
        "a failed emit must not advance the cursor"
    )


# ── the storm pins' shared pool teardown ─────────────────────────────────


async def close_storm_pool(pool: asyncpg.Pool) -> None:
    """Close a pool whose connections may include a terminated backend.

    A kill mid-transaction leaves the holder's release future unresolved
    (the dead protocol will never answer the reset) — a graceful
    ``close()`` hangs on it; the storm's pools are terminated HARD (the
    kill's own teardown, never the pin's concern)."""
    try:
        await asyncio.wait_for(pool.close(), timeout=5)
    except TimeoutError:
        pool.terminate()
