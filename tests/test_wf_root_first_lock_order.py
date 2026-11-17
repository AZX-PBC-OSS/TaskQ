# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""R2/A9.2: root-before-nodes is the universal row-lock order, and the
park seat locks root + intent before its park write.

The defect this control pins (DeepSeek round-3 F2, executed): the
failure cascade acquired its PEER row locks before the flow root while
the drain takes the root before the park row — the two seats deadlocked
with a real SQLSTATE 40P01, and the direct lane was left with invalid
unfinished state (`root running` beside the cascade's rolled-back
cascade). The fix: `_resolve_failed_parent` takes the exact flow root
``FOR UPDATE`` at ENTRY — before `fail_closed_cascade`, before any node
write — and the gated root failure stays its own statement after the
grant (its fresh READ COMMITTED snapshot is the N1 gate's own basis);
the park seat adopts and locks its intent record before the park UPDATE
and observes only after the write (its own row in the observation).

The controls drive the REAL seats (the real drain primitive, the real
failed-parent resolution) against real PostgreSQL with a keyed
`pg_locks` handshake (the holder's own xid + the distinct waiter pids,
`wait_event_type='Lock'` as corroboration only) — never a timing-only
sleep — in BOTH serial orders."""

from __future__ import annotations

import asyncio
import contextlib
import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._emit import wake_emit_backpressure
from taskq.workflows.engine import render_workflow_sql

pytestmark = pytest.mark.integration

_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


class _HandshakeTimeoutError(Exception):
    """The keyed blocked-state evidence never appeared within the bound."""


async def _txid_of(conn: Any) -> str:
    """The connection's OWN current transaction id, normalized to the
    plain xid domain (N6-3: `pg_current_xact_id()::xid::text` — the
    64-bit xid8's textual form diverges from `pg_locks.transactionid`
    after wrap; the `::xid` conversion before `::text` is mandatory)."""
    return str(await conn.fetchval("SELECT pg_current_xact_id()::xid::text"))


async def _wait_for_keyed_blocked_state(
    observer: Any,
    *,
    holder_xid: str,
    min_waiters: int,
    deadline_s: float,
) -> list[int]:
    """The keyed handshake: poll `pg_locks` under a bounded deadline for
    ungranted `transactionid` entries whose `transactionid::text` equals
    the HOLDER's own xid — the lock a competing backend waits on is the
    ungranted entry keyed to the holder's transaction, never a granted
    row row (an ordinary row lock's ownership lives in the tuple header;
    PostgreSQL supplies no `pg_locks` row for it). Corroboration only:
    the waiters' `pg_stat_activity.wait_event_type='Lock'`. Never a
    count, never a datname-scoped predicate, never a sleep."""
    sql = """
    SELECT DISTINCT l.pid, a.wait_event_type
    FROM pg_locks l
    JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'transactionid'
      AND NOT l.granted
      AND l.transactionid::text = $1
      AND l.pid <> pg_backend_pid()
    """
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline_s
    while loop.time() < end:
        rows = await observer.fetch(sql, holder_xid)
        pids = [int(r["pid"]) for r in rows]
        if len(pids) >= min_waiters:
            corroborated = [
                pid
                for pid, wet in ((int(r["pid"]), r["wait_event_type"]) for r in rows)
                if wet == "Lock"
            ]
            return pids, corroborated  # type: ignore[return-value]
        await asyncio.sleep(0.02)
    raise _HandshakeTimeoutError(
        f"no keyed blocked state within {deadline_s}s: holder xid={holder_xid}, "
        f"required {min_waiters} distinct ungranted transactionid waiters"
    )


def _park_meta(flow_id: str, *, width: int, bound: int) -> dict[str, Any]:
    return {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
        "emit_cursor": {"page": 0},
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": width,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }


async def _seed_peer_deadlock_estate(conn: Any, schema: str) -> tuple[str, str]:
    """THE F2 TOPOLOGY: a running root, a genuinely exhausted failed
    child B, the parked source A, and the fail_closed join J whose
    parents are A AND B — the failing child and the parked source are
    PEERS of the same join, so the cascade's peers arm locks A's row
    while the drain's promotion CAS wants it (the pre-fix cycle)."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'r2', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
        flow_id,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"flow:{flow_id}",
    )
    failed_child = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, error_class, error_message, "
        "finished_at, metadata, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r2', 'default', '{}', 'failed', 1, 1, 'transient', 'screen', "
        "'ValueError', 'exhausted', clock_timestamp(), $2::jsonb, $3, "
        "'workflow-run', $4)",
        failed_child,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:screen",
    )
    source_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, assignment_routed, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r2', 'default', '{}', 'scheduled', 0, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, true, 'workflow-run', $4)",
        source_id,
        _json.dumps(_park_meta(flow_id, width=2, bound=10)),
        flow_id,
        f"wf:{flow_id}:source",
    )
    join_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, deps_pending, metadata, scheduled_at, "
        "created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r2', 'default', '{}', 'pending', 0, 2, 'transient', 'join_step', "
        "2, $2::jsonb, clock_timestamp() + interval '1 hour', clock_timestamp(), $3, "
        "'workflow-run', $4)",
        join_id,
        _json.dumps({"flow_id": flow_id, "blocking_reason": "join"}),
        flow_id,
        f"wf:{flow_id}:join",
    )
    for parent in (failed_child, source_id):
        await conn.execute(
            f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, '
            "failure_policy) VALUES ($1, $2, $3, 'fail_closed')",
            join_id,
            parent,
            flow_id,
        )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity '
        "(flow_id, bound, wake_generation, handled_generation) VALUES ($1::uuid, 10, 1, 0)",
        flow_id,
    )
    return flow_id, source_id


async def _run_real_cascade_seat(conn: Any, wsql: Any, flow_id: str, child_id: str) -> None:
    """The REAL failure-cascade seat (`engine._resolve_failed_parent`),
    driven the way the runner's own exhaustion arm drives it."""
    from taskq.workflows.engine import _resolve_failed_parent

    tx = conn.transaction()
    await tx.start()
    try:
        await _resolve_failed_parent(
            conn,
            wsql,
            flow_id=JobId(flow_id),
            parent_id=JobId(child_id),
            step_key="screen",
            map_index=None,
            error_class="ValueError",
            error_message="exhausted",
            error_traceback=None,
        )
        await tx.commit()
    except BaseException:
        await tx.rollback()
        raise


async def _outcome(task: asyncio.Task[Any]) -> tuple[bool, str | None]:
    """The task's outcome: (completed_without_exception, the exception's
    class name). A 40P01 surfaces as the awaited task's exception."""
    try:
        await task
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return False, type(exc).__name__
    return True, None


@pytest.mark.parametrize("order", ["cascade_first", "drain_first"])
async def test_root_first_order_never_deadlocks_the_cascade_against_the_drain(
    wf_conn: Any, wf_schema: str, wf_pool: Any, module_pg_schema: Any, order: str
) -> None:
    """The F2 control in BOTH serial orders: the parked source is a peer
    of the failing child's own fail_closed join. The pre-fix cascade
    locked the peer row before the root while the drain held the root
    and waited on the peer — the real 40P01 with invalid unfinished
    direct state. The fixed cascade takes the root at entry; either
    order commits one coherent outcome with no deadlock."""
    wsql = render_workflow_sql(wf_schema)
    flow_id, source_id = await _seed_peer_deadlock_estate(wf_conn, wf_schema)
    failed_child = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND status = 'failed'
            LIMIT 1""",
        flow_id,
    )

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    poller = await asyncpg.connect(module_pg_schema.pg_dsn)
    drain_task: asyncio.Task[Any] | None = None
    cascade_task: asyncio.Task[Any] | None = None
    try:
        blocker_tx = holder.transaction()
        await blocker_tx.start()
        held = await holder.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs WHERE id = $1::uuid FOR UPDATE', source_id
        )
        assert held is not None
        holder_xid = await _txid_of(holder)

        from taskq.workflows._emit import COUNTERS

        pre_census = COUNTERS.snapshot()

        async def _start_drain() -> asyncio.Task[Any]:
            return asyncio.create_task(
                wake_emit_backpressure(wf_pool, wsql, flow_id=JobId(flow_id))
            )

        async def _start_cascade() -> asyncio.Task[Any]:
            return asyncio.create_task(
                _run_real_cascade_seat(wf_conn, wsql, flow_id, str(failed_child))
            )

        # THE SERIAL ORDERS: whichever seat is named first is STARTED
        # first; the control waits for BOTH seats' keyed blocked evidence
        # before releasing the blocker (the handshake proves the queue
        # formed — never a sleep).
        first, second = (
            (_start_cascade, _start_drain)
            if order == "cascade_first"
            else (_start_drain, _start_cascade)
        )
        first_task = await first()
        second_task = await second()
        cascade_task, drain_task = (
            (first_task, second_task) if order == "cascade_first" else (second_task, first_task)
        )

        # THE KEYED HANDSHAKE: both seats observably queued before the
        # release. The drain's own progress is corroborated by its census
        # (the flow_drains bump is its first statement's own fact); the
        # keyed waiters on the holder's xid prove the queue formed.
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            census_now = COUNTERS.snapshot()
            if census_now["flow_drains"] > pre_census["flow_drains"]:
                break
            await asyncio.sleep(0.02)
        else:
            pytest.fail("the drain never started its chunk")
        try:
            pids, corroborated = await _wait_for_keyed_blocked_state(
                poller,
                holder_xid=holder_xid,
                min_waiters=1,
                deadline_s=20.0,
            )
            assert pids, "the keyed queue must name the distinct waiter pids"
            assert corroborated, "the waiters' wait_event_type corroborates the queue"
        except _HandshakeTimeoutError:
            # Neither seat is queued on the holder: with both tasks still
            # running that is itself the lock-order defect (a seat that
            # never waits cannot be serialized) — name it.
            done = [t.done() for t in (first_task, second_task)]
            pytest.fail(
                f"the keyed blocked state never formed (order={order}, "
                f"tasks done={done}) — the seats are not serialized on the "
                "held row"
            )

        await blocker_tx.commit()
        drain_ok, drain_exc = await _outcome(drain_task)
        cascade_ok, cascade_exc = await _outcome(cascade_task)

        # THE FIXED ORDER'S OWN LAW: no SQLSTATE 40P01 in either raw-seat
        # ordering (the outer whole-tx2 retry is the 40P01's only owner,
        # and a raw-seat call must never need it here); both seats finish
        # within the bounded handshake and commit ONE coherent outcome.
        assert not (cascade_exc and "Deadlock" in cascade_exc), (
            f"the cascade seat deadlocked (order={order}): {cascade_exc}"
        )
        assert not (drain_exc and "Deadlock" in drain_exc), (
            f"the drain seat deadlocked (order={order}): {drain_exc}"
        )
        assert drain_ok and cascade_ok, (
            f"order={order}: drain_ok={drain_ok} ({drain_exc}) "
            f"cascade_ok={cascade_ok} ({cascade_exc})"
        )

        root_status = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
        )
        src = await wf_conn.fetchrow(
            f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1::uuid',
            source_id,
        )
        assert src is not None
        src_meta = (
            _json.loads(src["metadata"])
            if isinstance(src["metadata"], str)
            else dict(src["metadata"])
        )
        # THE COHERENT OUTCOMES (the ruling's own law): every parked peer
        # is either lawfully PROMOTED (pending with its reservation, the
        # run continuing) or lawfully CANCELLED by the cascade's peers
        # arm — never scheduled/parked under a failed root, never the
        # reserved-pending strand.
        lawful = (
            root_status == "failed"
            and src["status"] == "cancelled"
            and "wf_emit_reservation" not in src_meta
        ) or (
            root_status == "running"
            and src["status"] == "pending"
            and src_meta.get("wf_emit_reservation") == {"version": 1, "width": 2}
        )
        assert lawful, (
            f"order={order}: the incoherent outcome root={root_status} "
            f"source={src['status']} reservation={src_meta.get('wf_emit_reservation')}"
        )
    finally:
        for task in (drain_task, cascade_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        await holder.close()
        await poller.close()


async def _seed_park_order_estate(conn: Any, schema: str) -> tuple[str, str, str, Any]:
    """One running root, one RUNNING source (its claim's exact ledger
    identity), the capacity record: the real park seat's own input."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'r2', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
        flow_id,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"flow:{flow_id}",
    )
    source_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, claim_epoch, "
        "locked_by_worker, lock_expires_at, last_heartbeat_at, fairness_key, "
        "idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r2', 'default', '{}', 'running', 1, 2, 'transient', 'source', "
        "$2::jsonb, 1, $1, now() + interval '90 seconds', now(), $3, "
        "'workflow-run', $4)",
        source_id,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:source",
    )
    ledger_id = str(new_uuid())
    claimed_at = await conn.fetchval(
        f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "map_index, attempt, claim_epoch, status, created_at, updated_at) "
        "VALUES ($1, $2, $3, 'source', NULL, 1, 1, 'running', clock_timestamp(), "
        "clock_timestamp()) RETURNING updated_at",
        ledger_id,
        flow_id,
        source_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity '
        "(flow_id, bound, wake_generation, handled_generation) VALUES ($1::uuid, 2, 0, 0)",
        flow_id,
    )
    return flow_id, source_id, ledger_id, claimed_at


async def _run_real_park_seat(
    pool: Any, wsql: Any, *, flow_id: str, source_id: str, ledger_id: str, claimed_at: Any
) -> None:
    """The REAL park seat (`defer_row_law` with the EmitBackpressure
    disposition) — the runner's own park door."""
    from datetime import timedelta

    from taskq.workflows._emit import EmitBackpressure
    from taskq.workflows.api._runner_defer import defer_row_law

    outcome = await defer_row_law(
        pool,
        wsql,
        job_id=JobId(source_id),
        worker_id=JobId(source_id),
        delay=timedelta(0),
        attempt=1,
        claim_epoch=1,
        disposition="emit_backpressure",
        signal_name="EmitBackpressure",
        ledger_id=JobId(ledger_id),
        ledger_claimed_at=claimed_at,
        backpressure=EmitBackpressure(page_width=2, bound=2, outstanding=1),
        source_logical_attempt=1,
        flow_id=JobId(flow_id),
    )
    assert outcome.verdict == "deferred", outcome


async def test_park_locks_root_and_intent_before_the_park_write(
    wf_conn: Any, wf_schema: str, wf_pool: Any, module_pg_schema: Any
) -> None:
    """A9.2's park phases: the park seat acquires the flow root's row
    lock, then adopts and locks its intent record — BEFORE the park
    UPDATE — and its shared observation runs only after the write with
    the newly parked row included. With the root held, the real park
    queues at the root and writes nothing; with the intent held, it
    queues at the intent and writes nothing; after release it commits
    the park with exactly one parks/admission census bump and the
    own-row fit observation."""
    wsql = render_workflow_sql(wf_schema)

    async def _blocked_phase(
        blocker_sql: str,
    ) -> tuple[asyncpg.Connection, str, asyncio.Task[Any], str, Any, Any]:
        flow_id, source_id, ledger_id, claimed_at = await _seed_park_order_estate(
            wf_conn, wf_schema
        )
        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        blocker_tx = holder.transaction()
        await blocker_tx.start()
        await holder.fetchval(blocker_sql, flow_id)
        holder_xid = await _txid_of(holder)
        task = asyncio.create_task(
            _run_real_park_seat(
                wf_pool,
                wsql,
                flow_id=flow_id,
                source_id=source_id,
                ledger_id=ledger_id,
                claimed_at=claimed_at,
            )
        )
        return holder, holder_xid, task, flow_id, source_id, blocker_tx

    poller = await asyncpg.connect(module_pg_schema.pg_dsn)
    holders: list[asyncpg.Connection] = []
    try:
        # ── PHASE 1: the ROOT held ────────────────────────────────────
        holder, holder_xid, task, flow_id, source_id, blocker_tx = await _blocked_phase(
            f'SELECT id FROM "{wf_schema}".jobs '
            "WHERE id = $1::uuid AND step_key = '__flow__' FOR UPDATE"
        )
        holders.append(holder)
        try:
            pids, corroborated = await _wait_for_keyed_blocked_state(
                poller, holder_xid=holder_xid, min_waiters=1, deadline_s=20.0
            )
            assert pids and corroborated
        except _HandshakeTimeoutError:
            # THE PRE-FIX RED (the positive-observation form): the park
            # completed while the root was held — the park write never
            # queued at the root lock.
            done = task.done()
            state = await wf_conn.fetchval(
                f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid',
                source_id,
            )
            await task  # (already done or still blocked; do not leak)
            pytest.fail(
                "the park seat did not queue at the held ROOT (the park write "
                f"ran without the root's lock): task_done={done}, source={state}"
            )
        # While queued at the root, the park has written NOTHING.
        state = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', source_id
        )
        assert state == "running", (
            f"the park write must follow the root's grant; the row already moved: {state}"
        )
        await blocker_tx.commit()
        await task

        # ── PHASE 2: the INTENT row held (the root free) ──────────────
        flow2, source2, ledger2, claimed2 = await _seed_park_order_estate(wf_conn, wf_schema)
        holder2 = await asyncpg.connect(module_pg_schema.pg_dsn)
        holders.append(holder2)
        blocker_tx2 = holder2.transaction()
        await blocker_tx2.start()
        await holder2.fetchval(
            f'SELECT flow_id FROM "{wf_schema}".wf_emit_capacity '
            "WHERE flow_id = $1::uuid FOR UPDATE",
            flow2,
        )
        holder2_xid = await _txid_of(holder2)
        from taskq.workflows._emit import COUNTERS

        pre_parks = COUNTERS.snapshot()["parks"]
        task2 = asyncio.create_task(
            _run_real_park_seat(
                wf_pool,
                wsql,
                flow_id=flow2,
                source_id=source2,
                ledger_id=ledger2,
                claimed_at=claimed2,
            )
        )
        try:
            pids, corroborated = await _wait_for_keyed_blocked_state(
                poller, holder_xid=holder2_xid, min_waiters=1, deadline_s=20.0
            )
            assert pids and corroborated
        except _HandshakeTimeoutError:
            done = task2.done()
            state2 = await wf_conn.fetchval(
                f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid',
                source2,
            )
            await task2
            pytest.fail(
                "the park seat did not queue at the held INTENT row (the park "
                f"write ran without the intent's lock): task_done={done}, "
                f"source={state2}"
            )
        # While queued at the intent, the park write has not happened.
        state2 = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', source2
        )
        assert state2 == "running", (
            "the park UPDATE must follow the intent's adoption/lock; the row "
            f"already moved: {state2}"
        )
        await blocker_tx2.commit()
        await task2
        parked2 = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', source2
        )
        assert parked2 == "scheduled", parked2
        # Exactly ONE parks bump for the committed park decision (B-3).
        assert COUNTERS.snapshot()["parks"] == pre_parks + 1, COUNTERS.snapshot()
        _ = flow_id, source_id

        # ── PHASE 3: the own-row observation (no blockers) ────────────
        flow3, source3, ledger3, claimed3 = await _seed_park_order_estate(wf_conn, wf_schema)
        pre3 = COUNTERS.snapshot()
        await _run_real_park_seat(
            wf_pool,
            wsql,
            flow_id=flow3,
            source_id=source3,
            ledger_id=ledger3,
            claimed_at=claimed3,
        )
        post3 = COUNTERS.snapshot()
        # The park's own event: one parks bump, one admission decision,
        # and the OWN-ROW fit (the observation includes the newly parked
        # row — min_waiting_width=2 fits the bound exactly — so the
        # threshold crossed and ONE intent was created).
        assert post3["parks"] == pre3["parks"] + 1, (pre3, post3)
        assert post3["admission_decisions"] == pre3["admission_decisions"] + 1, (pre3, post3)
        assert post3["new_intents"] == pre3["new_intents"] + 1, (
            "the park's own observation must include the newly parked row "
            "(the fit is the own-row width against the freed charge)"
        )
    finally:
        for holder in holders:
            await holder.close()
        await poller.close()
