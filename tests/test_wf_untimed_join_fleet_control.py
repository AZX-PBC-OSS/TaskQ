# ruff: noqa: S608  # Why: every schema interpolation is the module fixture's validated test identifier; every value is $-bound.
"""Issue #748: an untimed ``handle.wait()`` on a spawned child, on the fleet path.

The held representation is ``pending`` + the ``metadata.hold`` stamp; an
untimed hold has no deadline, so ``scheduled_at`` stays due. The marker is the
discriminator: the in-process claim CAS and claimable SELECT both carry
``AND NOT metadata ? 'hold'``, and the fleet dispatch fence must carry the same
leg, or a worker re-claims the held row at once. The re-entered body then finds
its own standing hold, and the row must come back to the held representation,
never stay ``running`` with no execution behind it (that zombie is what the
lost-claim refund and the lease reclaim later re-run under the same attempt
number, where the per-attempt spawn cursor has moved and a second child is
minted).

The third control pins the slot law on its own: a non-consuming deferral
(``Snooze``) refunds the attempt by design, so the body's next execution runs
under the SAME attempt number; the default spawn slot must still re-derive
from zero per execution and adopt, never mint slot 1.

The re-entry pins then hold the re-park on every wait kind (the join, the
signal, the timer, a loop body's wait), its claim fence (a stale body whose
lease lapsed never parks the row a newer claim runs), and the in-process
driver's door.

The controls drive the REAL fleet claim (``dispatch_batch`` over the
production strict-FIFO render, a ``workflow_execution`` worker row) and the
worker door's runner (``run_fleet_claimed_step``), the same harness
``test_wf_emit_wake_recovery`` uses.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.backend._sql_templates import render
from taskq.backend._sweeps import sweep_expired_locks, sweep_scheduled_to_pending
from taskq.exceptions import RetryAfter, Snooze
from taskq.settings import WorkerSettings
from taskq.worker.run import register_worker
from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    step,
)
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import sweep_hold_stamps, sweep_join_rederive
from taskq.workflows.api._ctx import build_step_context
from taskq.workflows.api._hitl import sweep_child_joins
from taskq.workflows.engine import render_workflow_sql


class _Leaf(BaseModel):
    n: int


class _Approval(BaseModel):
    verdict: str


def _compile(suffix: str, wrap: Any) -> tuple[Any, Any]:
    """The issue's shape: a one-step child, and a parent whose one step
    spawns it and joins it (the D1 registry is global: names are unique)."""
    child_app = WorkflowApp()

    @child_app.workflow(f"i748_child_{suffix}")
    def child() -> Promise[object]:
        async def leaf(ctx: StepContext) -> _Leaf:
            return _Leaf(n=1)

        return build(step(leaf, key="leaf"))

    parent_app = WorkflowApp()

    @parent_app.workflow(f"i748_parent_{suffix}")
    def parent() -> Promise[object]:
        return build(step(wrap, key="wrap"))

    return child_app.get(f"i748_child_{suffix}"), parent_app.get(f"i748_parent_{suffix}")


async def _fleet(conn: asyncpg.Connection, pool: asyncpg.Pool, schema: str, pg_dsn: str) -> JobId:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    worker_id = await register_worker(
        pool,
        WorkerSettings.load_from_dict({"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema}),
        workflow_execution=True,
    )
    return JobId(worker_id)


async def _claim(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: JobId,
    *,
    limit: int,
    lock_lease: timedelta = timedelta(seconds=30),
) -> list[dict[str, Any]]:
    rows = await dispatch_batch(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=["default"],
        limit_n=limit,
        worker_id=worker_id,
        lock_lease=lock_lease,
    )
    return [dict(row) for row in rows]


async def _run(runners: dict[str, FlowRunner], row: dict[str, Any]) -> str:
    """The worker door's dispatch: the claimed row's own flow picks the runner."""
    meta = row["metadata"]
    meta = json.loads(meta) if isinstance(meta, str) else meta
    flow_id = JobId(meta["flow_id"])
    name = "parent" if row["step_key"] == "wrap" else "child"
    return await runners[name].run_fleet_claimed_step(
        flow_id,
        row,
        attempt=int(row["attempt"]),
        claim_epoch=int(row["claim_epoch"]),
    )


async def _wrap(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> asyncpg.Record:
    row = await conn.fetchrow(
        f'SELECT id, status, attempt, locked_by_worker, metadata FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'wrap'",
        flow_id,
    )
    assert row is not None
    return row


def _meta(raw: object) -> dict[str, Any]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded  # pyright: ignore[reportUnknownVariableType]


async def _children(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> int:
    """The child flow roots the run minted (each spawn slot is one root)."""
    return int(
        await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE step_key = '__flow__' "
            "AND metadata->>'parent_flow_id' = $1::text",
            str(flow_id),
        )
    )


async def _child_keys(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> list[str]:
    """The child roots' run keys (the spawn's identity the arbiter remembers)."""
    rows = await conn.fetch(
        f"SELECT idempotency_key FROM \"{schema}\".jobs WHERE step_key = '__flow__' "
        "AND metadata->>'parent_flow_id' = $1::text ORDER BY idempotency_key",
        str(flow_id),
    )
    return [str(r["idempotency_key"]) for r in rows]


async def _held_signals(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> list[str]:
    rows = await conn.fetch(
        f'SELECT id FROM "{schema}".wf_signals WHERE workflow_id = $1 '
        "AND node_key = 'wrap' AND status = 'held' ORDER BY hold_epoch",
        flow_id,
    )
    return [str(r["id"]) for r in rows]


@pytest.mark.integration
async def test_an_untimed_join_s_held_row_is_never_fleet_claimed(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    pg_dsn: str,
) -> None:
    """The issue's own path: the held wrap row (pending, stamped, due) must
    not come back from the fleet's claim; the child's leaf is the only work.
    The run then joins its ONE child: the wake resumes the wrap row, the
    resumed attempt adopts slot 0, and no second child is ever minted."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"

    async def wrap(ctx: StepContext) -> int:
        handle = await ctx.spawn_flow(child_name, result=_Leaf)
        await handle.wait()  # untimed: no deadline, scheduled_at stays due
        return 0

    child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }

    first = await _claim(wf_conn, wf_schema, worker_id, limit=1)
    assert [r["step_key"] for r in first] == ["wrap"] and first[0]["attempt"] == 1
    assert await _run(runners, first[0]) == "held"
    held = await _wrap(wf_conn, wf_schema, flow_id)
    assert held["status"] == "pending" and held["locked_by_worker"] is None
    stamp = _meta(held["metadata"]).get("hold")
    assert stamp is not None and [stamp] == await _held_signals(wf_conn, wf_schema, flow_id)

    second = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    claimed = {str(r["id"]): (r["step_key"], r["attempt"]) for r in second}
    assert str(held["id"]) not in claimed, (
        "the fleet dispatch claimed the HELD wrap row (pending + metadata.hold "
        f"{stamp}, an untimed hold so scheduled_at is due): claimed={claimed}. "
        "The in-process claim refuses a stamped row (AND NOT metadata ? 'hold'); "
        "the fleet fence has no such leg, so the held join re-enters at once"
    )

    # The child's leaf is the round's real work; the wake arm delivers the
    # join and resumes the wrap row, whose next claim adopts the same child.
    assert [r["step_key"] for r in second] == ["leaf"]
    assert await _run(runners, second[0]) == "succeeded"
    await sweep_join_rederive(wf_pool, wf_sql)  # the root maintenance: the child root terminal
    assert await sweep_child_joins(wf_pool, wf_sql) == 1
    resumed = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    assert [(r["step_key"], r["attempt"]) for r in resumed] == [("wrap", 2)]
    assert await _run(runners, resumed[0]) == "succeeded"
    done = await _wrap(wf_conn, wf_schema, flow_id)
    assert done["status"] == "succeeded"
    assert await _children(wf_conn, wf_schema, flow_id) == 1


@pytest.mark.integration
async def test_a_reentered_join_on_its_own_standing_hold_parks_the_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    pg_dsn: str,
) -> None:
    """The re-entry path that survives a fence fix: a body's lease lapses
    between its spawn and its wait (a stalled worker), Sweep 1 re-pends the
    row, and the stalled body's wait then mints its hold: the signal row is
    'held' but the node update matched nothing (the row was no longer
    running), so the row carries no stamp and the next claim legitimately
    runs the body again. That re-entered wait finds its own standing hold.
    It must leave the row in the held representation (pending, unlocked,
    stamped with that hold), never 'running' with nothing executing it."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"
    spawned = asyncio.Event()
    release = asyncio.Event()

    async def wrap(ctx: StepContext) -> int:
        handle = await ctx.spawn_flow(child_name, result=_Leaf)
        if ctx.attempt == 1:
            spawned.set()
            await release.wait()  # the stalled worker: the lease lapses here
        await handle.wait()
        return 0

    child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }

    first = await _claim(wf_conn, wf_schema, worker_id, limit=1, lock_lease=timedelta(seconds=1))
    assert [r["step_key"] for r in first] == ["wrap"]
    stalled = asyncio.create_task(_run(runners, first[0]))
    await asyncio.wait_for(spawned.wait(), timeout=10.0)
    await asyncio.sleep(1.2)
    assert await sweep_expired_locks(wf_conn, timedelta(0), timedelta(0), schema=wf_schema) == 1
    release.set()
    assert await asyncio.wait_for(stalled, timeout=10.0) == "held"
    standing = await _held_signals(wf_conn, wf_schema, flow_id)
    repended = await _wrap(wf_conn, wf_schema, flow_id)
    assert len(standing) == 1 and repended["status"] == "pending"
    assert "hold" not in _meta(repended["metadata"])
    # skip the reclaim backoff (the time travel only; every write below is production's)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET scheduled_at = now() WHERE id = $1', repended["id"]
    )

    again = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    wraps = [r for r in again if r["step_key"] == "wrap"]
    assert len(wraps) == 1 and wraps[0]["attempt"] == 2
    assert await _run(runners, wraps[0]) == "held"
    after = await _wrap(wf_conn, wf_schema, flow_id)
    assert (after["status"], after["locked_by_worker"], _meta(after["metadata"]).get("hold")) == (
        "pending",
        None,
        standing[0],
    ), (
        "the re-entered wait found its own standing hold and raised NodeHeldError "
        "without re-applying the held representation: the row is "
        f"{after['status']!r} locked by {after['locked_by_worker']} with stamp "
        f"{_meta(after['metadata']).get('hold')!r} (standing hold {standing[0]}). "
        "Nothing executes it; the lost-claim refund + the lease reclaim re-run it "
        "under the same attempt number, where the spawn cursor has moved"
    )
    # Resume-not-retry's ledger face on the re-entry claim itself (the
    # stalled attempt-1 mint already marked ITS row; this claim's row is new).
    reentry_ledger = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'wrap' AND claim_epoch = $2",
        flow_id,
        int(wraps[0]["claim_epoch"]),
    )
    assert reentry_ledger == "awaited", (
        f"the re-entered join left its claim's ledger row {reentry_ledger!r}: a "
        "held body's claim records 'awaited' (resume-not-retry), never 'running' "
        "until the flow terminates"
    )

    # The join then completes on the ONE child.
    for row in again:
        if row["step_key"] == "leaf":
            assert await _run(runners, row) == "succeeded"
    await sweep_join_rederive(wf_pool, wf_sql)  # the root maintenance: the child root terminal
    assert await sweep_child_joins(wf_pool, wf_sql) == 1
    resumed = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    assert [r["step_key"] for r in resumed] == ["wrap"]
    assert await _run(runners, resumed[0]) == "succeeded"
    assert await _children(wf_conn, wf_schema, flow_id) == 1


@pytest.mark.integration
@pytest.mark.parametrize("deferral", ["snooze", "retry_after"])
async def test_a_deferred_spawning_body_re_runs_onto_its_own_child(
    deferral: str,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """``Snooze`` (or ``RetryAfter(consume_budget=False)``) after an unkeyed
    spawn: the deferral refunds the claim's attempt (a deferral did not
    execute), so the re-run carries the same ``attempt`` as the first
    execution. The spawn's default slot is the
    per-execution cursor's: the re-run adopts the child it already minted."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"
    entries: list[tuple[int, str]] = []

    async def wrap(ctx: StepContext) -> int:
        handle = await ctx.spawn_flow(child_name, result=_Leaf)
        entries.append((ctx.attempt, str(handle.flow_id)))
        if len(entries) == 1:  # the entry count, never ctx.attempt: the refund repeats it
            if deferral == "snooze":
                raise Snooze(timedelta(0))
            raise RetryAfter(timedelta(0), consume_budget=False)
        return 0

    child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }

    first = await _claim(wf_conn, wf_schema, worker_id, limit=1)
    assert [(r["step_key"], r["attempt"]) for r in first] == [("wrap", 1)]
    assert await _run(runners, first[0]) == "deferred"
    snoozed = await _wrap(wf_conn, wf_schema, flow_id)
    assert (snoozed["status"], snoozed["attempt"]) == ("scheduled", 0)  # the refund
    # skip the snooze's delay (the time travel only), then the real Sweep 3 promotion
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET scheduled_at = now() WHERE id = $1', snoozed["id"]
    )
    assert await sweep_scheduled_to_pending(wf_conn, schema=wf_schema) == 1

    again = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    wraps = [r for r in again if r["step_key"] == "wrap"]
    assert [r["attempt"] for r in wraps] == [1]  # the same attempt number, a new execution
    assert await _run(runners, wraps[0]) == "succeeded"
    children = await _children(wf_conn, wf_schema, flow_id)
    assert entries[0][1] == entries[1][1] and children == 1, (
        f"the re-run under the refunded attempt minted a NEW child: entries={entries}, "
        f"child roots={children}. The default slot must restart at 0 for every claim: "
        "a deferral's refund hands the next execution the same attempt number, so a "
        "counter keyed by the attempt alone has already moved"
    )
    # a step outside a loop keeps its pre-#748 run key byte for byte
    assert await _child_keys(wf_conn, wf_schema, flow_id) == [f"childflow:{flow_id}:wrap:0"]
    # The slot cursor's shape: the attempt's key carries the claim that last
    # spawned under it, so a claim_epoch dropped on the way to the context
    # (the 0 default) is visible here, never a silent per-attempt fallback.
    final = await wf_conn.fetchrow(
        f'SELECT claim_epoch, metadata FROM "{wf_schema}".jobs WHERE id = $1', snoozed["id"]
    )
    assert final is not None
    assert _meta(final["metadata"]).get("spawn_slot_1") == {
        "epoch": int(final["claim_epoch"]),
        "n": 0,
    }


@pytest.mark.integration
async def test_an_untimed_signal_hold_is_neither_claimed_nor_probed_routable(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """The same held representation from the HITL door (``ctx.wait_signal``
    with no ``timeout_s``): the only row left is the held one. The claim must
    not take it, and the claimable probe (the window-expansion arbiter) must
    not count it as routable work, or every empty round on the queue pays the
    expansion budget for as long as the hold stands."""
    suffix = new_uuid().hex[:12]

    async def wrap(ctx: StepContext) -> int:
        await ctx.wait_signal(_Approval, reason="the untimed approval")
        return 0

    child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }

    first = await _claim(wf_conn, wf_schema, worker_id, limit=1)
    assert [(r["step_key"], r["attempt"]) for r in first] == [("wrap", 1)]
    assert await _run(runners, first[0]) == "held"
    held = await _wrap(wf_conn, wf_schema, flow_id)
    assert held["status"] == "pending" and "hold" in _meta(held["metadata"])

    probed = len(await wf_conn.fetch(render(wf_schema).dispatch_claimable_probe, ["default"]))
    claimed = [
        (r["step_key"], r["attempt"]) for r in await _claim(wf_conn, wf_schema, worker_id, limit=10)
    ]
    assert (probed, claimed) == (0, []), (
        f"the held row is fleet work: the claimable probe counted {probed} routable "
        f"row(s) and the claim took {claimed}; a pending row stamped metadata.hold is "
        "the held representation, never claimable"
    )


# ── the re-entry seam on every wait kind, and its claim fence ───────────

#: A timed wait's deadline: far enough that no sweep resolves it mid-test,
#: and the re-parked row's scheduled_at must land exactly on it.
_TIMED_S = 600.0


class _Reject(BaseModel):
    reason: str


def _compile_loop(suffix: str, body: Any) -> Any:
    app = WorkflowApp()

    @app.workflow(f"i748_loop_{suffix}")
    def flow() -> Promise[object]:
        return build(loop("wrap", body, max_iterations=2, budget_s=600.0))

    return app.get(f"i748_loop_{suffix}")


async def _lapse_into_unstamped_hold(
    conn: asyncpg.Connection,
    schema: str,
    runners: dict[str, FlowRunner],
    worker_id: JobId,
    flow_id: JobId,
    stalled: asyncio.Event,
    release: asyncio.Event,
) -> asyncpg.Record:
    """Test 2's recipe: the attempt-1 body stalls before its wait, its lease
    lapses, Sweep 1 re-pends the row, and the released body's wait mints a
    'held' signal row whose node update matched nothing. Returns the standing
    hold (id, signal_name, expires_at); the row is pending, unstamped, due."""
    first = await _claim(conn, schema, worker_id, limit=1, lock_lease=timedelta(seconds=1))
    assert [r["step_key"] for r in first] == ["wrap"]
    task = asyncio.create_task(_run(runners, first[0]))
    await asyncio.wait_for(stalled.wait(), timeout=10.0)
    await asyncio.sleep(1.2)
    assert await sweep_expired_locks(conn, timedelta(0), timedelta(0), schema=schema) == 1
    release.set()
    assert await asyncio.wait_for(task, timeout=10.0) in ("held", "loop")
    standing = await conn.fetch(
        f'SELECT id, signal_name, expires_at FROM "{schema}".wf_signals '
        "WHERE workflow_id = $1 AND node_key = 'wrap' AND status = 'held'",
        flow_id,
    )
    repended = await _wrap(conn, schema, flow_id)
    assert len(standing) == 1 and repended["status"] == "pending"
    assert "hold" not in _meta(repended["metadata"])
    # skip the reclaim backoff (the time travel only)
    await conn.execute(
        f'UPDATE "{schema}".jobs SET scheduled_at = now() WHERE id = $1', repended["id"]
    )
    return standing[0]


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["join", "signal", "timer", "loop"])
async def test_a_reentered_wait_of_every_kind_re_parks_onto_its_standing_hold(
    kind: str,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """Every wait site that finds its own standing hold parks the row onto it
    exactly as the mint would have: pending, unlocked, stamped with THAT
    hold's id and signal name, scheduled at the hold's own deadline, and a
    loop's budget paused. Nothing is then claimable or probed routable."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"
    stalled = asyncio.Event()
    release = asyncio.Event()

    async def stall(ctx: StepContext) -> None:
        if ctx.attempt == 1:
            stalled.set()
            await release.wait()  # the stalled worker: the lease lapses here

    async def wrap(ctx: StepContext) -> int:
        if kind == "join":
            handle = await ctx.spawn_flow(child_name, result=_Leaf)
            await stall(ctx)
            await handle.wait(timeout_s=_TIMED_S)
        elif kind == "signal":
            await stall(ctx)
            # A redeploy widened the wait's models: the site still matches the
            # standing single-name row, so the stamp must name THAT row, never
            # this execution's joined name.
            models: tuple[type[BaseModel], ...] = (
                (_Approval,) if ctx.attempt == 1 else (_Approval, _Reject)
            )
            await ctx.wait_signal(models, timeout_s=_TIMED_S)
        else:
            await stall(ctx)
            await ctx.wait_until(_TIMED_S, key="remind")
        return 0

    async def loop_body(ctx: StepContext, carry: object) -> object:
        await stall(ctx)
        await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        return Done({"ok": True})

    child_c, parent_c = _compile(suffix, wrap)
    if kind == "loop":
        parent_c = _compile_loop(suffix, loop_body)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }
    standing = await _lapse_into_unstamped_hold(
        wf_conn, wf_schema, runners, worker_id, flow_id, stalled, release
    )

    again = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    wraps = [r for r in again if r["step_key"] == "wrap"]
    assert len(wraps) == 1 and wraps[0]["attempt"] == 2
    assert await _run(runners, wraps[0]) in ("held", "loop")
    after = await wf_conn.fetchrow(
        f"SELECT status, locked_by_worker, scheduled_at, budget_paused, metadata "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        wraps[0]["id"],
    )
    assert after is not None
    meta = _meta(after["metadata"])
    assert (
        after["status"],
        after["locked_by_worker"],
        meta.get("hold"),
        meta.get("held_signal"),
        after["scheduled_at"],
    ) == (
        "pending",
        None,
        str(standing["id"]),
        standing["signal_name"],
        standing["expires_at"],
    ), (
        f"the re-entered {kind} wait did not park the row onto its standing hold "
        f"{dict(standing)}: row status={after['status']!r} locked_by="
        f"{after['locked_by_worker']} hold={meta.get('hold')!r} "
        f"held_signal={meta.get('held_signal')!r} scheduled_at={after['scheduled_at']}"
    )
    if kind == "loop":
        assert after["budget_paused"] is True, "a held loop's budget must pause"
    # The join kind's leaf was claimed by the same round and sits unrun.
    probed = len(await wf_conn.fetch(render(wf_schema).dispatch_claimable_probe, ["default"]))
    claimed = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    assert (probed, claimed) == (0, [])


@pytest.mark.integration
async def test_a_stale_body_s_reentry_never_parks_a_newer_claim_s_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """The fencing-token rule on the re-park: X (attempt 1) stalls and mints a
    stamp-less standing hold; Y (attempt 2) stalls before its wait and loses
    its lease; Z (attempt 3) claims the row. Y's re-entered wait finds the
    standing hold but must not park Z's running row; Z's own re-entry does."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"
    gates = {a: asyncio.Event() for a in (1, 2, 3)}
    at_gate = {a: asyncio.Event() for a in (1, 2, 3)}

    async def wrap(ctx: StepContext) -> int:
        handle = await ctx.spawn_flow(child_name, result=_Leaf)
        at_gate[ctx.attempt].set()
        await gates[ctx.attempt].wait()
        await handle.wait()
        return 0

    child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }

    async def lapse() -> None:
        await asyncio.sleep(1.2)
        # the wrap row, and the child's leaf when a 1 s round claimed it too
        assert await sweep_expired_locks(wf_conn, timedelta(0), timedelta(0), schema=wf_schema) >= 1
        row = await _wrap(wf_conn, wf_schema, flow_id)
        assert row["status"] == "pending"
        # skip the reclaim backoff (the time travel only)
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET scheduled_at = now() WHERE id = $1', row["id"]
        )

    async def claim_wrap() -> dict[str, Any]:
        got = await _claim(wf_conn, wf_schema, worker_id, limit=10, lock_lease=timedelta(seconds=1))
        wraps = [r for r in got if r["step_key"] == "wrap"]
        assert len(wraps) == 1, got
        return wraps[0]

    tasks: list[asyncio.Task[str]] = []

    def spawn(coro: Any) -> asyncio.Task[str]:
        tasks.append(asyncio.create_task(coro))
        return tasks[-1]

    try:
        x = spawn(_run(runners, await claim_wrap()))
        await asyncio.wait_for(at_gate[1].wait(), timeout=10.0)
        await lapse()
        gates[1].set()
        assert await asyncio.wait_for(x, timeout=10.0) == "held"
        standing = await _held_signals(wf_conn, wf_schema, flow_id)
        assert len(standing) == 1

        y_row = await claim_wrap()
        y = spawn(_run(runners, y_row))
        await asyncio.wait_for(at_gate[2].wait(), timeout=10.0)
        await lapse()
        z_row = await claim_wrap()
        assert int(z_row["claim_epoch"]) > int(y_row["claim_epoch"])
        z = spawn(_run(runners, z_row))
        await asyncio.wait_for(at_gate[3].wait(), timeout=10.0)

        gates[2].set()  # release the STALE body
        assert await asyncio.wait_for(y, timeout=10.0) == "held"
        mid = await _wrap(wf_conn, wf_schema, flow_id)
        assert (mid["status"], mid["locked_by_worker"], _meta(mid["metadata"]).get("hold")) == (
            "running",
            worker_id,
            None,
        ), (
            f"the stale body (claim_epoch {y_row['claim_epoch']}) parked the row that "
            f"claim {z_row['claim_epoch']} is running: status={mid['status']!r} "
            f"stamp={_meta(mid['metadata']).get('hold')!r}"
        )

        gates[3].set()  # the live claim's own re-entry parks it
        assert await asyncio.wait_for(z, timeout=10.0) == "held"
        end = await _wrap(wf_conn, wf_schema, flow_id)
        assert (end["status"], end["locked_by_worker"], _meta(end["metadata"]).get("hold")) == (
            "pending",
            None,
            standing[0],
        )
    finally:  # an early assertion must not leave a stalled body running
        for gate in gates.values():
            gate.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.integration
async def test_an_in_process_reentry_re_parks_onto_its_standing_hold(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """The in-process driver's door (``FlowRunner.drive``, the node claim
    CAS) threads its own claim epoch to the wait site: its re-entry onto a
    standing hold parks the row exactly as the fleet door's does."""
    suffix = new_uuid().hex[:12]
    stalled = asyncio.Event()
    release = asyncio.Event()

    async def wrap(ctx: StepContext) -> int:
        if ctx.attempt == 1:
            stalled.set()
            await release.wait()
        await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        return 0

    child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }
    standing = await _lapse_into_unstamped_hold(
        wf_conn, wf_schema, runners, worker_id, flow_id, stalled, release
    )

    driver = FlowRunner(parent_c, wf_pool, wf_schema)
    assert await driver.drive(flow_id, until="held", max_ticks=50, tick=0.0) == "held"
    after = await _wrap(wf_conn, wf_schema, flow_id)
    assert after["attempt"] == 2
    assert (after["status"], after["locked_by_worker"], _meta(after["metadata"]).get("hold")) == (
        "pending",
        None,
        str(standing["id"]),
    ), (
        "the in-process re-entry left the row "
        f"{after['status']!r} locked by {after['locked_by_worker']} with stamp "
        f"{_meta(after['metadata']).get('hold')!r} (standing hold {standing['id']})"
    )


@pytest.mark.integration
async def test_a_stale_writer_sharing_the_attempt_never_collapses_the_live_claim_s_slots(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Two executions can share one attempt number while both run: a chain
    source's ``ctx.attempt`` is its logical attempt, which a lease reclaim
    never advances. The live claim's first unkeyed spawn adopts slot 0 (the
    re-run law); a stale body's later spawn must not reset the counter under
    it, or the live claim's next spawn re-derives slot 0 and adopts its own
    first child (the silent fabricated result). The epoch only moves forward:
    the stale writer takes the next slot instead (base's duplicate)."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"

    async def wrap(ctx: StepContext) -> int:
        return 0

    _child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    wrap_id = JobId((await _wrap(wf_conn, wf_schema, flow_id))["id"])
    wsql = render_workflow_sql(wf_schema)

    def ctx_at(epoch: int) -> StepContext:
        return build_step_context(
            flow_id=flow_id,
            job_id=wrap_id,
            node_key="wrap",
            attempt=1,
            pool=wf_pool,
            wsql=wsql,
            input=None,
            claim_epoch=epoch,
        )

    stale, live = ctx_at(1), ctx_at(2)
    s1 = await stale.spawn_flow(child_name, result=_Leaf)  # before its lease lapsed
    l1 = await live.spawn_flow(child_name, result=_Leaf)  # the re-run adopts slot 0
    s2 = await stale.spawn_flow(child_name, result=_Leaf)
    l2 = await live.spawn_flow(child_name, result=_Leaf)
    meta = _meta((await _wrap(wf_conn, wf_schema, flow_id))["metadata"])
    assert l1.flow_id == s1.flow_id  # the adoption law, unchanged
    assert len({l1.flow_id, s2.flow_id, l2.flow_id}) == 3, (
        "an older claim_epoch reset the attempt's slot counter under the live "
        f"claim: live=({l1.flow_id}, {l2.flow_id}) stale=({s1.flow_id}, {s2.flow_id}) "
        f"spawn_slot_1={meta.get('spawn_slot_1')}"
    )
    assert meta.get("spawn_slot_1") == {"epoch": 2, "n": 2}


@pytest.mark.integration
async def test_a_delivery_inside_the_reentry_window_is_repaired_by_the_reconcile(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The accepted race: a delivery commits between the re-entered wait's
    standing-hold read and its re-park. The deliver finds no stamped node to
    resume, the re-park then stamps a hold that is no longer 'held', and the
    fleet refuses the stamped row. The leader's hold-stamp reconcile is the
    repair: one pass clears the dead stamp and the next claim completes the
    body on the delivered answer."""
    import taskq.workflows.api._hitl as hitl

    suffix = new_uuid().hex[:12]
    stalled = asyncio.Event()
    release = asyncio.Event()

    async def wrap(ctx: StepContext) -> str:
        if ctx.attempt == 1:
            stalled.set()
            await release.wait()
        answer = await ctx.wait_signal(_Approval)
        assert isinstance(answer, _Approval)
        return answer.verdict

    child_c, parent_c = _compile(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }
    standing = await _lapse_into_unstamped_hold(
        wf_conn, wf_schema, runners, worker_id, flow_id, stalled, release
    )

    real_reassert = hitl.reassert_hold
    raced: list[str] = []

    async def deliver_first(conn: Any, **kw: Any) -> None:
        result = await hitl.HitlClient(wf_pool, schema=wf_schema).resolve(
            kw["hold_id"], {"verdict": "approve"}, principal="pin"
        )
        raced.append(result.status)
        await real_reassert(conn, **kw)

    monkeypatch.setattr(hitl, "reassert_hold", deliver_first)
    again = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    wraps = [r for r in again if r["step_key"] == "wrap"]
    assert len(wraps) == 1 and wraps[0]["attempt"] == 2
    assert await _run(runners, wraps[0]) == "held"
    monkeypatch.setattr(hitl, "reassert_hold", real_reassert)
    assert raced == ["refused"]  # the CAS committed; no stamped node to resume

    parked = await _wrap(wf_conn, wf_schema, flow_id)
    signal = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', standing["id"]
    )
    assert (parked["status"], _meta(parked["metadata"]).get("hold"), signal) == (
        "pending",
        str(standing["id"]),
        "delivered",
    )
    probed = len(await wf_conn.fetch(render(wf_schema).dispatch_claimable_probe, ["default"]))
    assert (probed, await _claim(wf_conn, wf_schema, worker_id, limit=10)) == (0, [])

    assert await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema)) == 1
    resumed = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    assert [(r["step_key"], r["attempt"]) for r in resumed] == [("wrap", 3)]
    assert await _run(runners, resumed[0]) == "succeeded"


@pytest.mark.integration
@pytest.mark.parametrize("keyed", [False, True], ids=["unkeyed", "keyed"])
async def test_a_deferred_loop_iteration_re_runs_onto_its_own_iteration_s_child(
    keyed: bool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """A loop's iterations run on the loop node's one row, under one attempt
    and claim. A ``Snooze`` in iteration 1 refunds the attempt; the re-run
    resumes at iteration 1 from the row's counter (iteration 0 never runs
    again), and iteration 1's two spawns must adopt iteration 1's own two
    children in order, never iteration 0's (the unkeyed spawn scopes its slot
    cursor and its run key to the iteration, and the cursor still counts up
    within it) and never a fresh duplicate. An explicit ``key=`` keeps the
    author's run key as is."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"
    spawns: list[tuple[int, str]] = []

    async def unused(ctx: StepContext) -> int:
        return 0

    child_c, _ = _compile(suffix, unused)

    async def body(ctx: StepContext, carry: object) -> object:
        iteration = 0 if carry is None else int(str(carry))
        key = f"it{iteration}" if keyed else None
        handle = await ctx.spawn_flow(child_name, result=_Leaf, key=key)
        spawns.append((iteration, str(handle.flow_id)))
        if iteration == 0:
            return Refine(1)
        # a second spawn in the same iteration takes the iteration's next slot
        second = await ctx.spawn_flow(child_name, result=_Leaf, key=f"{key}b" if keyed else None)
        spawns.append((iteration, str(second.flow_id)))
        if len(spawns) == 3:  # iteration 1's first execution defers
            raise Snooze(timedelta(0))
        return Done({"ok": True})

    parent_c = _compile_loop(suffix, body)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }

    first = [
        r for r in await _claim(wf_conn, wf_schema, worker_id, limit=10) if r["step_key"] == "wrap"
    ]
    assert [r["attempt"] for r in first] == [1]
    await _run(runners, first[0])
    deferred = await _wrap(wf_conn, wf_schema, flow_id)
    assert (deferred["status"], deferred["attempt"]) == ("scheduled", 0)  # the refund
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET scheduled_at = now() WHERE id = $1', first[0]["id"]
    )
    assert await sweep_scheduled_to_pending(wf_conn, schema=wf_schema) == 1
    again = [
        r for r in await _claim(wf_conn, wf_schema, worker_id, limit=10) if r["step_key"] == "wrap"
    ]
    assert [r["attempt"] for r in again] == [1]  # the refunded attempt number
    await _run(runners, again[0])
    # the re-run resumes at iteration 1 (the row's counter): iteration 0 never re-runs
    assert [i for i, _ in spawns] == [0, 1, 1, 1, 1]
    flows = [f for _, f in spawns]
    assert len(set(flows[:3])) == 3 and flows[3:] == flows[1:3], (
        "iteration 1's re-run after its Snooze did not adopt iteration 1's own two "
        f"children in order (0 = iteration 0's, 1-2 = iteration 1's first run, "
        f"3-4 = the re-run): {spawns}"
    )
    assert await _children(wf_conn, wf_schema, flow_id) == 3
    scopes = (
        ["wrap:it0", "wrap:it1", "wrap:it1b"]
        if keyed
        else ["wrap.iter0:0", "wrap.iter1:0", "wrap.iter1:1"]
    )
    assert await _child_keys(wf_conn, wf_schema, flow_id) == [
        f"childflow:{flow_id}:{scope}" for scope in scopes
    ]


@pytest.mark.integration
async def test_a_join_resumed_loop_iteration_adopts_its_own_iteration_s_child(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    pg_dsn: str,
) -> None:
    """The resume face of the same law: iteration 1 spawns unkeyed and holds
    on its child's join; the join's wake resumes the loop row under a new
    claim (attempt 2, a fresh cursor key), iteration 1 runs again, and its
    spawn must return iteration 1's child, never iteration 0's, never a
    third child."""
    suffix = new_uuid().hex[:12]
    child_name = f"i748_child_{suffix}"
    spawns: list[tuple[int, str]] = []

    async def unused(ctx: StepContext) -> int:
        return 0

    child_c, _ = _compile(suffix, unused)

    async def body(ctx: StepContext, carry: object) -> object:
        iteration = 0 if carry is None else int(str(carry))
        handle = await ctx.spawn_flow(child_name, result=_Leaf)
        spawns.append((iteration, str(handle.flow_id)))
        if iteration == 0:
            return Refine(1)
        await handle.wait()  # iteration 1 holds on its own child's join
        return Done({"ok": True})

    parent_c = _compile_loop(suffix, body)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _fleet(wf_conn, wf_pool, wf_schema, pg_dsn)
    runners = {
        "parent": FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id),
        "child": FlowRunner(child_c, wf_pool, wf_schema, worker_id=worker_id),
    }
    first = [
        r for r in await _claim(wf_conn, wf_schema, worker_id, limit=10) if r["step_key"] == "wrap"
    ]
    assert [r["attempt"] for r in first] == [1]
    assert await _run(runners, first[0]) in ("held", "loop")
    leaves = await _claim(wf_conn, wf_schema, worker_id, limit=10)
    assert sorted(r["step_key"] for r in leaves) == ["leaf", "leaf"], leaves
    for leaf in leaves:
        assert await _run(runners, leaf) == "succeeded"
    await sweep_join_rederive(wf_pool, wf_sql)
    assert await sweep_child_joins(wf_pool, wf_sql) == 1
    resumed = [
        r for r in await _claim(wf_conn, wf_schema, worker_id, limit=10) if r["step_key"] == "wrap"
    ]
    assert [r["attempt"] for r in resumed] == [2]
    await _run(runners, resumed[0])
    assert [i for i, _ in spawns] == [0, 1, 1], spawns
    assert spawns[2][1] == spawns[1][1] != spawns[0][1], (
        f"the join-resumed iteration 1 did not adopt iteration 1's own child: {spawns}"
    )
    assert await _children(wf_conn, wf_schema, flow_id) == 2
    assert await _child_keys(wf_conn, wf_schema, flow_id) == [
        f"childflow:{flow_id}:wrap.iter0:0",
        f"childflow:{flow_id}:wrap.iter1:0",
    ]
    assert (await _wrap(wf_conn, wf_schema, flow_id))["status"] == "succeeded"
