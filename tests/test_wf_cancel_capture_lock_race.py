# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""B-1/C-1/N5-1/N6-3: the cancel target capture's `FOR UPDATE` is
load-bearing for the TRUTHFUL old-state record — and the race control's
evidence is a keyed `pg_locks` handshake, never a sleep.

The race: the cancel's target capture reads a row's status while a
concurrent due-state writer's `scheduled -> pending` transition is
uncommitted. WITHOUT the capture lock the reader takes the stale
pre-image (`scheduled`) and the resulting event falsely records
`scheduled -> cancelled` although the transition's preceding locked
state was `pending`. WITH it, whichever writer locks first establishes
the transition and the other captures the committed winner state.

The handshake's own evidence rule (N5-1, refined by layer 6): an
ordinary row lock's ownership lives in the tuple header — PostgreSQL
supplies NO `pg_locks` row for a granted row lock. The keyed marker is
the waiter's UNGRANTED `transactionid` entry whose `transactionid::text`
equals the HOLDER's own `pg_current_xact_id()::xid::text` (the `::xid`
conversion before `::text` is mandatory — the raw xid8 text diverges
after wrap) and whose pid is the distinct cancel backend.
`pg_stat_activity.wait_event_type='Lock'` is corroboration only. A
`locktype='tuple'` entry may appear where the interleaving produces one
and is acceptable; it is never required and never the gate."""

from __future__ import annotations

import asyncio
import contextlib
import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId

pytestmark = pytest.mark.integration


def new_uuid_str() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


async def _seed_scheduled_root(conn: Any, schema: str) -> str:
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, scheduled_at, fairness_key, "
        "idempotency_scope, idempotency_key) VALUES ($1, 'b1', 'default', '{}', 3, "
        "'transient', 'scheduled', '__flow__', $2::jsonb, clock_timestamp(), $3, "
        "'workflow-run', $4)",
        flow_id,
        _json.dumps({"flow_id": flow_id, "workflow": "b1_flow"}),
        flow_id,
        f"flow:{flow_id}",
    )
    return flow_id


async def _xid_text(conn: Any) -> str:
    """The connection's own current transaction id in the PLAIN xid domain
    (N6-3's mandatory spelling)."""
    return str(await conn.fetchval("SELECT pg_current_xact_id()::xid::text"))


async def test_xid_comparison_key_matches_pg_locks_domain(
    module_pg_schema: Any, wf_schema: str
) -> None:
    """The real-PG self-check (N6-3): the control's comparison key
    (`pg_current_xact_id()::xid::text`) is the SAME textual domain
    `pg_locks.transactionid::text` carries — the connection's own granted
    self-lock row is found by that key."""
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        tx = conn.transaction()
        await tx.start()
        xid = await _xid_text(conn)
        row = await conn.fetchrow(
            """SELECT l.transactionid::text AS tid, l.granted
               FROM pg_locks l
               WHERE l.locktype = 'transactionid' AND l.pid = pg_backend_pid()
                 AND l.transactionid::text = $1""",
            xid,
        )
        assert row is not None, "the connection's own xid must key its own lock row"
        assert row["granted"] is True
        await tx.commit()
    finally:
        await conn.close()
    _ = wf_schema


async def test_cancel_capture_lock_wins_the_old_state_race(
    wf_conn: Any, wf_schema: str, wf_pool: Any, module_pg_schema: Any
) -> None:
    """The fixed capture: the real cancel verb, racing the real uncommitted
    `scheduled -> pending` writer, captures the COMMITTED winner state
    (`pending`) — the keyed handshake proves the cancel was blocked on the
    writer's own xid before the commit, and the event records the TRUE
    transition."""
    flow_id = await _seed_scheduled_root(wf_conn, wf_schema)

    writer = await asyncpg.connect(module_pg_schema.pg_dsn)
    poller = await asyncpg.connect(module_pg_schema.pg_dsn)
    cancel_task: asyncio.Task[Any] | None = None
    try:
        writer_tx = writer.transaction()
        await writer_tx.start()
        # THE UNCOMMITTED DUE-STATE WRITER: the real scheduled -> pending
        # transition, held open.
        await writer.execute(
            f"""UPDATE "{wf_schema}".jobs SET status = 'pending'
                WHERE id = $1::uuid""",
            flow_id,
        )
        writer_xid = await _xid_text(writer)

        # THE REAL CANCEL VERB (the runner-exit's own seat).
        from taskq.workflows.api._runner_exit import _cancel_one_run

        cancel_task = asyncio.create_task(
            _cancel_one_run(
                wf_pool,
                schema=wf_schema,
                flow_id=JobId(flow_id),
                reason="the B-1 race control's cancel",
                principal=None,
            )
        )

        # THE KEYED HANDSHAKE (bounded; never a sleep-as-evidence): the
        # cancel backend's ungranted transactionid entry keyed to the
        # WRITER's xid.
        sql = """
        SELECT DISTINCT l.pid, a.wait_event_type
        FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.locktype = 'transactionid'
          AND NOT l.granted
          AND l.transactionid::text = $1
          AND l.pid <> pg_backend_pid()
          AND a.datname = current_database()
        """
        loop = asyncio.get_running_loop()
        end = loop.time() + 20.0
        waiter_pids: list[int] = []
        while loop.time() < end:
            rows = await poller.fetch(sql, writer_xid)
            waiter_pids = [int(r["pid"]) for r in rows]
            if waiter_pids:
                break
            await asyncio.sleep(0.02)
        assert waiter_pids, (
            "the cancel backend never queued on the writer's own xid — the "
            "target capture's lock is the serialization point and the "
            "handshake is its evidence"
        )
        # Corroboration only: the waiter's wait_event_type is 'Lock' —
        # observed within the same bound, never demanded in the same
        # snapshot as the gate (PostgreSQL queues the ungranted pg_locks
        # row before the wait-event latch is set; the two views are
        # sampled at their own instants — the flake the round-5
        # correction caught).
        deadline2 = asyncio.get_running_loop().time() + 20.0
        corroborated = False
        while asyncio.get_running_loop().time() < deadline2:
            wet_rows = await poller.fetch(
                """SELECT pid, wait_event_type FROM pg_stat_activity
                   WHERE pid = ANY($1::int[])
                     AND datname = current_database()""",
                waiter_pids,
            )
            if wet_rows and all(r["wait_event_type"] == "Lock" for r in wet_rows):
                corroborated = True
                break
            await asyncio.sleep(0.02)
        assert corroborated, "the queued cancel backend's wait_event_type corroborates"

        # THE COMMIT — only after the keyed blocked state was observed.
        await writer_tx.commit()
        held = await asyncio.wait_for(cancel_task, 20)
        assert held is not None

        # THE TRUE TRANSITION: the capture read the committed winner state.
        event = await wf_conn.fetchrow(
            f"""SELECT detail FROM "{wf_schema}".job_events
                WHERE job_id = $1::uuid AND kind = 'state_change'""",
            flow_id,
        )
        assert event is not None, "the cancel's own state_change event"
        detail = event["detail"]
        detail = _json.loads(detail) if isinstance(detail, str) else dict(detail)
        assert detail["from_state"] == "pending", (
            f"the capture must record the COMMITTED winner state: {detail}"
        )
        assert detail["to_state"] == "cancelled", detail
        root_status = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
        )
        assert root_status == "cancelled", root_status
    finally:
        if cancel_task is not None and not cancel_task.done():
            cancel_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await cancel_task
        await writer.close()
        await poller.close()
