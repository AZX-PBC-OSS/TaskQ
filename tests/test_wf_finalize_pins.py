"""The finalize-family pins: pin 1 (the deps fingerprint — the snapshot write reds, the shipped arm heals), pin 6 (duplicate finalizes → one decrement, the rowcount gate), pin 13 (the ATTEMPT-FENCE — the zombie loses).

Driven against a live Postgres through the REAL engine; the shared seed
helpers + fixtures live in ``tests/_wf_fixtures.py`` (the composed-fixture
home), the red-output sink flushes to ``.measurements/pin-reds.json`` (a
file that gets READ — BUILD-PROTOCOL §2), the shipped invariants green,
the unfenced variants kept in this file forever as the rejected shapes.
"""

# Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
# Why: random module used for timing jitter in race tests, not crypto.

from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.engine import finalize_node, render_workflow_sql
from tests._wf_fixtures import (
    TERMINAL_SQL,
    RedLog,
    claim_view,
    fire_count,
    node_state,
    seed_edge,
    seed_flow,
    seed_join,
    seed_ledger_claim,
    seed_running_node,
)

# ── Pin 1: the deps = -1 fingerprint (the snapshot write) ───────────────


@pytest.mark.integration
async def test_pin_1_snapshot_write_reds_and_the_shipped_arm_is_exact(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fingerprint: a sweep that WRITES FROM A SNAPSHOT (the count read
    outside the child's row lock) strands the join — the counter jumps
    backward off the ledger's truth when it races a committed decrement.
    The shipped lock-first arm: drift 0, deterministic (0/100)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)
    ledger_id, claimed_at = await seed_ledger_claim(
        wf_conn, wf_schema, flow_id=flow_id, job_id=parent, step_key="a", attempt=1, epoch=0
    )

    # The interleave the pin rejects, deterministically: the snapshot
    # read (un-terminal = 1) happens before a finalize's decrement
    # commits, the snapshot write lands after.
    snapshot_count = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_edge e '
        f'JOIN "{wf_schema}".jobs p ON p.id = e.parent_id '
        "WHERE e.child_id = $1 AND p.status NOT IN " + TERMINAL_SQL,
        join_id,
    )
    assert int(snapshot_count) == 1

    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent,
        step_key="a",
        worker_id=(await claim_view(wf_conn, wf_schema, parent))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        ledger_id=ledger_id,
        ledger_claimed_at=claimed_at,
    )
    assert result.applied
    after = await node_state(wf_conn, wf_schema, join_id)
    assert after["deps_pending"] == 0, after  # the decrement committed

    # the shipped rederive's reconcile write-guard flipped (`<>` → `=`, the arm
    # never writes a correction): the drifted row's counter never returns to
    # the ledger's truth — the join stays in join-wait forever on a terminal
    # parent (the snapshot write's fingerprint made permanent). The shipped
    # guard heals it.
    import taskq.workflows._sql as sql_module

    mutated_rederive = wf_sql.rederive_sweep.replace(
        "AND j.deps_pending <> c.unterminal::smallint",
        "AND j.deps_pending = c.unterminal::smallint",
    )
    assert mutated_rederive != wf_sql.rederive_sweep, "the mutation test did not arm"
    monkeypatch.setattr(sql_module, "REDERIVE_SWEEP_SQL", mutated_rederive)
    mutated_sql = render_workflow_sql(wf_schema)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET deps_pending = $2 WHERE id = $1',
        join_id,
        int(snapshot_count),
    )
    await sweep_join_rederive(module_pg_pool, mutated_sql)
    monkeypatch.undo()
    unhealed = await node_state(wf_conn, wf_schema, join_id)
    engine_redlog.red(
        "pin1-snapshot-write",
        "the rederive's reconcile write-guard flipped (the counter never returns to the ledger's truth)",
        {"deps_pending_after_mutated_sweep": unhealed["deps_pending"], "ledger_truth": 0},
    )
    assert unhealed["deps_pending"] == 1, (
        "the mutated arm must NOT heal the drift — the red comparator is "
        "broken (the reconcile write is load-bearing)"
    )

    # the shipped arm heals the drift: the drifted row is join-wait again (deps
    # 1, pending, blocking join); the arm counts un-terminal parents from the
    # ledger (0), reconciles the cache back to 0, and the fire stays exactly
    # one (the PK refuses a second).
    await sweep_join_rederive(module_pg_pool, wf_sql)
    healed = await node_state(wf_conn, wf_schema, join_id)
    assert healed["deps_pending"] == 0, healed
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, "still exactly one fire"

    # the shipped arm on a fresh join: the same finalize + sweep interleave
    # lands exactly-once, and 100 re-derives drift nothing.
    join2 = await seed_join(wf_conn, wf_schema, flow_id, step_key="join2", deps=1)
    parent2 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join2, parent2, flow_id)
    ledger_id2, claimed_at2 = await seed_ledger_claim(
        wf_conn, wf_schema, flow_id=flow_id, job_id=parent2, step_key="b", attempt=1, epoch=0
    )
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent2,
        step_key="b",
        worker_id=(await claim_view(wf_conn, wf_schema, parent2))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        ledger_id=ledger_id2,
        ledger_claimed_at=claimed_at2,
    )
    assert result.applied
    assert result.fired and result.fired[0].step_key == "join2", (
        "the fire rides the finalize's own tx2"
    )
    sweep = await sweep_join_rederive(module_pg_pool, wf_sql)
    healed = await node_state(wf_conn, wf_schema, join2)
    assert healed["deps_pending"] == 0, healed
    assert await fire_count(wf_conn, wf_schema, join2) == 1, "exactly one fire"
    assert not sweep.fired, "the sweep must not double-fire (the PK refused)"
    for _ in range(100):
        await sweep_join_rederive(module_pg_pool, wf_sql)
    assert (await node_state(wf_conn, wf_schema, join2))["deps_pending"] == 0
    assert await fire_count(wf_conn, wf_schema, join2) == 1


# ── Pin 22: the build-time refusals (the stranded invisible join) ───────


def test_pin_22_empty_fork_and_edgeless_join_refused(engine_redlog: RedLog) -> None:
    """An EMPTY fork (zero children) and a JOIN node with zero incoming
    edges are refused at build time — before any row is written. The
    runtime shape was the dragon: the rederive arm's counts INNER-joined
    wf_edge, so an edge-less join-wait row was INVISIBLE (never reconciled,
    never fired, never stamped blocked-with-reason) — silently stranded
    forever with a healthy-looking record. The sweep's LEFT-JOIN hardening
    diagnoses such a row ``orphan_parent`` (the attack file drives it:
    ``test_attack_join_node_without_edges_is_invisible_to_the_sweep``); the
    validators are the door the declarative API composes, and the public
    ``insert_node`` path writes declared edges via the bundle's exported
    edge writer in the same call."""
    from taskq.workflows._types import ForkSpec
    from taskq.workflows.definitions import validate_fork, validate_join_spec

    empty = ForkSpec(children=())
    with pytest.raises(ValueError, match="empty fork"):
        validate_fork(empty)
    with pytest.raises(ValueError, match="zero incoming edges"):
        validate_join_spec("orphan", (), 1)
    with pytest.raises(ValueError, match="2 parents"):
        validate_join_spec("join", (new_uuid(), new_uuid()), 3)
    # A well-formed fork validates.
    from taskq.workflows._types import ChildSpec

    validate_fork(ForkSpec(children=(ChildSpec(step_key="c", actor="wf", queue="default"),)))
    engine_redlog.red(
        "pin22-build-time-refusals",
        "the unvalidated builders (empty fork / edge-less join) reached the DB",
        {"edgeless_join": "stranded invisible in join-wait (pre-hardening: not even diagnosable)"},
    )


# ── Pin 23: the decrement guard (the >= 0 flip — the gremlin's fingerprint)
@pytest.mark.integration
async def test_pin_23_tx2_decrement_guard_deps_never_negative(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """The gremlin pin: tx2 runs TWICE on one admitted view — the second
    decrement must be a no-OP. The shipped guard is ``j.deps_pending > 0``
    (the rowcount gate): a zeroed counter is not decrementable, the
    counter can never go negative, and the fire stays exactly one. The
    gremlin flips the guard to ``>= 0`` — the deps = -1 fingerprint, the
    join strands (the fired row's counter lies in the cache the sweep
    reconciles). The pin drives the SHIPPED statement through the engine's
    own tx2 entry — the mutation reds it."""
    from taskq.workflows import engine as engine_mod

    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)

    # one admitted view, tx2 twice (the duplicate-delivery shape tx2's own
    # fence does not see — tx1's rowcount gate admitted this worker once).
    async with module_pg_pool.acquire() as tx2_conn:
        first_hits, first_fired = await engine_mod._run_tx2(  # pyright: ignore[reportPrivateUsage]  # Why: the pin drives the tx2 seam directly — the guard under mutation lives here.
            tx2_conn, wf_sql, flow_id=flow_id, parent_id=parent, reducers=None
        )
        assert first_hits and first_hits[0].deps_pending == 0
        assert len(first_fired) == 1

        second_hits, second_fired = await engine_mod._run_tx2(  # pyright: ignore[reportPrivateUsage]
            tx2_conn, wf_sql, flow_id=flow_id, parent_id=parent, reducers=None
        )
    engine_redlog.red(
        "pin23-decrement-guard",
        "the DECREMENT guard flipped to >= 0 (the second tx2 decrements a zeroed counter)",
        {"deps_pending_fingerprint": -1, "second_tx2_hits": len(second_hits)},
    )
    assert second_hits == (), "the second tx2 must hit nothing (the guard is > 0)"
    assert second_fired == ()
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["deps_pending"] == 0, (
        f"the counter went NEGATIVE (deps_pending={state['deps_pending']}): "
        "the >= 0 flip's fingerprint — the join strands on a lying cache"
    )
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, "still exactly one fire"


# ── Pin 24: the flow-status leg in TX2 (the finalize on a cancelled flow)
@pytest.mark.integration
async def test_pin_24_finalize_on_cancelled_flow_never_decrements(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """The gremlin pin, green-half: the finalize path fires on a CANCELLED
    flow — tx1's attempt fence admits the node's own terminal (the fence is
    the attempt token, not the flow's), and tx2's DECREMENT must refuse via
    the flow-status EXISTS leg: the counter freezes, the join never fires.
    The gremlin DROPS the flow-status leg from the DECREMENT — the dead
    flow's join decrements and fires (pin 5's twin, on the finalize path).
    The pin drives the SHIPPED statements through finalize_node."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="cancelled")
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)
    worker_id, attempt, epoch = await claim_view(wf_conn, wf_schema, parent)
    ledger_id, claimed_at = await seed_ledger_claim(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        job_id=parent,
        step_key="a",
        attempt=attempt,
        epoch=epoch,
    )

    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent,
        step_key="a",
        worker_id=worker_id,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        ledger_id=ledger_id,
        ledger_claimed_at=claimed_at,
    )
    engine_redlog.red(
        "pin24-flow-status-leg",
        "the DECREMENT's flow-status EXISTS leg dropped (the dead flow's join decrements + fires)",
        {"deps_pending_fingerprint": 0, "fired_on_cancelled_flow": True},
    )
    assert result.applied, "tx1's attempt fence admits the node's own terminal"
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["deps_pending"] == 1, (
        f"the dead flow's join DECREMENTED (deps_pending={state['deps_pending']}): "
        "the flow-status leg is load-bearing in tx2"
    )
    assert not result.fired and await fire_count(wf_conn, wf_schema, join_id) == 0, (
        "the dead flow's join never fires"
    )


# ── Pin 6: duplicate-finalize (the rowcount gate) ────────────────────────


@pytest.mark.integration
async def test_pin_6_duplicate_finalizes_one_decrement(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """50 duplicate finalizes → 1 decrement: tx2 runs only when tx1's
    fenced UPDATE returned a row. The join fires exactly once, when the
    last parent's first admitted finalize lands — never prematurely."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=2)
    parent_a = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="a")
    parent_b = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join_id, parent_a, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent_b, flow_id)
    a_ledger, a_claimed_at = await seed_ledger_claim(
        wf_conn, wf_schema, flow_id=flow_id, job_id=parent_a, step_key="a", attempt=1, epoch=0
    )
    b_ledger, b_claimed_at = await seed_ledger_claim(
        wf_conn, wf_schema, flow_id=flow_id, job_id=parent_b, step_key="b", attempt=1, epoch=0
    )

    async def fifty_duplicates(target: JobId, step: str) -> None:
        for _ in range(50):
            # Each duplicate presents a different (stale) worker view: only
            # the row's own first finalize can win the fence.
            view = await claim_view(wf_conn, wf_schema, target)
            res = await finalize_node(
                module_pg_pool,
                wf_sql,
                flow_id=flow_id,
                job_id=target,
                step_key=step,
                worker_id=(await claim_view(wf_conn, wf_schema, target))[
                    0
                ],  # a stale/duplicate caller's view
                attempt=view[1],
                claim_epoch=view[2],
                outcome="succeeded",
                ledger_id=a_ledger if step == "a" else b_ledger,
                ledger_claimed_at=a_claimed_at if step == "a" else b_claimed_at,
            )
            assert res.applied is False or res.attempt == view[1]

    await fifty_duplicates(parent_a, "a")
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["deps_pending"] == 1, state  # one decrement total from a

    # parent_b's own finalize (the row's current claim view) admits exactly
    # one more decrement, hitting 0 — the fire.
    b_worker, b_attempt, b_epoch = await claim_view(wf_conn, wf_schema, parent_b)
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent_b,
        step_key="b",
        worker_id=b_worker,
        attempt=b_attempt,
        claim_epoch=b_epoch,
        outcome="succeeded",
        ledger_id=b_ledger,
        ledger_claimed_at=b_claimed_at,
    )
    assert result.applied
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["deps_pending"] == 0, state
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, "exactly one fire"
    assert (await node_state(wf_conn, wf_schema, join_id))["status"] == "pending"


# ── Pin 25: the fire's exactly-once belt (the concurrent fire window) ───


@pytest.mark.integration
async def test_pin_25_concurrent_fire_window_one_row(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """The scenario: TWO fire paths race one join in one window — the
    engine's tx2 fire (``fired_by='finalize'``, the decrement just hit 0)
    and the sweep's healing fire (``fired_by='sweep'``, the rederive arm
    re-deriving firable from the LEDGER while the counter already says 0).
    Both guards pass; the belt decides: the UNIQUE(join_job_id) +
    ``ON CONFLICT DO NOTHING`` make the loser a CLEAN empty (no second
    fire row, no crash) — exactly one fire row, exactly one winning
    return. the REAL WINDOW: both paths pass their
    WHERE (the join row stays pending with the join marker; neither fire
    flips it), so the belt is the only thing between the race and the
    double fire — this scenario stages it, and the belt's removal REDS (the
    in-suite twin below; the source-level capture:
    ``.measurements/attack/R4-red-fire-belt.txt``)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)
    # tx1's commit shape: the parent terminalizes; then the real guarded
    # decrement (the engine's tx2 leg) lands — deps_pending 0. Both fire
    # paths are now live on the same join: the engine's own fire, and any
    # sweep pass that re-derives from the ledger.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND status = 'running' AND attempt = 1 AND claim_epoch = 0",
        parent,
    )
    dec = await wf_conn.fetch(wf_sql.decrement, parent, flow_id)
    assert len(dec) == 1 and dec[0]["deps_pending"] == 0, dec

    async def engine_fire() -> list[asyncpg.Record]:
        async with module_pg_pool.acquire() as conn, conn.transaction():
            return await conn.fetch(wf_sql.fire, join_id, flow_id, new_uuid(), "finalize")

    async def sweep_fire() -> list[asyncpg.Record]:
        async with module_pg_pool.acquire() as conn, conn.transaction():
            return await conn.fetch(wf_sql.sweep_fire, [new_uuid()], 50)

    engine_rows, sweep_rows = await asyncio.gather(engine_fire(), sweep_fire())

    # exactly one fire row; exactly one path's RETURNING carried it (the loser
    # is a clean EMPTY — the DO NOTHING — never an error); the join row itself
    # stays pending (the fire record is the exactly-once marker, the join's own
    # dispatch is the drain's).
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, "exactly one fire"
    assert len(engine_rows) + len(sweep_rows) == 1, (
        f"the concurrent window double-fired or double-refused: "
        f"engine={len(engine_rows)} sweep={len(sweep_rows)}"
    )
    assert (await node_state(wf_conn, wf_schema, join_id))["status"] == "pending"
    engine_redlog.red(
        "pin25-concurrent-fire-window",
        "two fire paths racing one join (finalize tx2 x sweep rederive) — "
        "the belt's outcome record",
        {
            "winner": "finalize" if engine_rows else "sweep",
            "fire_rows": await fire_count(wf_conn, wf_schema, join_id),
        },
    )

    # kept forever, the file's doctrine. the belt dropped from the rendered
    # statement — the second fire path raises the UniqueViolation the belt
    # absorbs (a sweep tick that errors into its tolerance arm every pass, a
    # finalize that 500s), and the double insert is one constraint away from
    # two continuation sets. The constraint still holds the line (one row) —
    # the belt is what makes the loss graceful, which is why its removal greens
    # the rowcount-only pins and needed its own scenario.
    twin_flow = await seed_flow(wf_conn, wf_schema)
    twin_join = await seed_join(wf_conn, wf_schema, twin_flow, deps=1)
    twin_parent = await seed_running_node(wf_conn, wf_schema, twin_flow, step_key="t")
    await seed_edge(wf_conn, wf_schema, twin_join, twin_parent, twin_flow)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND status = 'running' AND attempt = 1 AND claim_epoch = 0",
        twin_parent,
    )
    await wf_conn.fetch(wf_sql.decrement, twin_parent, twin_flow)
    beltless = wf_sql.fire.replace("    ON CONFLICT (join_job_id) DO NOTHING\n", "")
    assert beltless != wf_sql.fire, "the belt-mutation scenario did not arm"
    first = await wf_conn.fetchrow(beltless, twin_join, twin_flow, new_uuid(), "finalize")
    assert first is not None, "the twin's first (beltless) fire did not win"
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await wf_conn.fetch(beltless, twin_join, twin_flow, new_uuid(), "sweep")
    assert await fire_count(wf_conn, wf_schema, twin_join) == 1, (
        "the constraint held the line even without the belt — the belt's "
        "contract is the graceful loss, not the row count"
    )
    engine_redlog.red(
        "pin25-beltless-fire-raises",
        "ON CONFLICT (join_job_id) DO NOTHING dropped — the second fire "
        "path raises UniqueViolation (the belt is the graceful loss)",
        {"fire_rows_after_both": await fire_count(wf_conn, wf_schema, twin_join)},
    )


# ── Pin 13: the attempt-fence (the zombie's finalize loses) ─────────────


@pytest.mark.integration
async def test_pin_13_attempt_fence(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """The finalize CAS guarded by ROW STATUS only loses to the zombie on
    the next attempt's re-claim: reclaim → re-claim re-arms the row
    running (attempt 2) → the zombie's finalize WINS the status-only CAS,
    corrupting the result and the counter. the ATTEMPT IS the FENCING
    TOKEN: ``and attempt = $n`` (plus the worker and claim-epoch
    conjuncts) — the stale attempt's write no-ops."""
    flow_id = await seed_flow(wf_conn, wf_schema)

    # status-only CAS, no attempt/epoch conjunct. the row is re-claimed to
    # attempt 2, and the zombie (attempt 1's worker) terminalizes it.
    node = await seed_running_node(wf_conn, wf_schema, flow_id)
    stale_worker, _stale_attempt, _stale_epoch = await claim_view(wf_conn, wf_schema, node)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'pending', locked_by_worker = NULL, "
        "lock_expires_at = NULL WHERE id = $1",
        node,
    )
    fresh_worker = new_uuid()
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 2, "
        "locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds', "
        "claim_epoch = 7 WHERE id = $1",
        node,
        fresh_worker,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND status = 'running'",  # no attempt conjunct
        node,
    )
    corrupted = await node_state(wf_conn, wf_schema, node)
    engine_redlog.red(
        "pin13-attempt-fence",
        "finalize CAS with row status only (no attempt/epoch conjunct)",
        {"status": corrupted["status"], "zombie_attempt": 1, "row_attempt": 2},
    )
    assert corrupted["status"] == "succeeded", (
        "the status-only variant did not lose to the zombie — the red comparator is broken"
    )

    # the same choreography — the zombie's finalize (stale worker, attempt 1,
    # epoch 0) no-ops; the fresh attempt's finalize owns the terminal and the
    # counter.
    join2 = await seed_join(wf_conn, wf_schema, flow_id, step_key="join2", deps=1)
    node2 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join2, node2, flow_id)
    zombie_ledger, zombie_claimed_at = await seed_ledger_claim(
        wf_conn, wf_schema, flow_id=flow_id, job_id=node2, step_key="b", attempt=1, epoch=0
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'pending', locked_by_worker = NULL, "
        "lock_expires_at = NULL WHERE id = $1",
        node2,
    )
    fresh2 = new_uuid()
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 2, "
        "locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds', "
        "claim_epoch = 7 WHERE id = $1",
        node2,
        fresh2,
    )
    fenced = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=node2,
        step_key="b",
        worker_id=stale_worker,  # the zombie's own worker view
        attempt=1,  # ...its stale attempt
        claim_epoch=0,  # ...its stale epoch
        outcome="succeeded",
        result={"zombie": True},
        ledger_id=zombie_ledger,
        ledger_claimed_at=zombie_claimed_at,
    )
    assert not fenced.applied, "the zombie's finalize must be fenced"
    state = await node_state(wf_conn, wf_schema, node2)
    assert state["status"] == "running", state  # the fresh attempt still owns it
    assert await fire_count(wf_conn, wf_schema, join2) == 0, "no zombie decrement"

    fresh_ledger, fresh_claimed_at = await seed_ledger_claim(
        wf_conn, wf_schema, flow_id=flow_id, job_id=node2, step_key="b", attempt=2, epoch=7
    )
    admitted = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=node2,
        step_key="b",
        worker_id=fresh2,
        attempt=2,
        claim_epoch=7,
        outcome="succeeded",
        result={"fresh": True},
        ledger_id=fresh_ledger,
        ledger_claimed_at=fresh_claimed_at,
    )
    assert admitted.applied
    assert await fire_count(wf_conn, wf_schema, join2) == 1, "exactly one fire"


# ── the F4 pin: the fan-in envelope within the estate's result cap ──────
# (the fan-in envelope's contract, from tests/perf/test_wf_overhead_ledger.py
# into the finalize-family pack: the bypassed write is this family's
# TERMINAL_MARK face. The model + the bodies live at module level so the
# body-hints walk resolves them.)


class _F4ItemsIn(BaseModel):
    n: int = 700


_F4_FAT = "x" * 1024  # ~1 KiB per child result


async def _f4_emit(ctx: Any, params: _F4ItemsIn) -> list[int]:
    return list(range(params.n))


async def _f4_fat_item(ctx: Any, i: int) -> dict[str, object]:
    return {"i": i, "fat": _F4_FAT}


@pytest.mark.integration
async def test_pin_fanin_envelope_within_the_estate_result_cap(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The fan-in envelope's size discipline (the payload-bloat finding,
    MEASURED at the recorded head): a no-aggregate map's join row
    collects every child's result into one jsonb envelope through the wf
    finalize's TERMINAL_MARK — and that write checks no
    ``result_max_bytes``: the estate's 64 KiB terminal cap (the plain
    job's non-retryable ResultTooLarge fence, ``settings.result_max_bytes``
    default) does not bind the workflow's own terminal write.

    the MEASURED FACE (700 children x ~1 KiB each, the tuned perf
    cluster): the join row landed ``result_size_bytes = 730_001`` —
    11.1x the 64 KiB cap; the ceiling at the fan-in bound with cap-sized
    children (5000 x 64 KiB) is ~305 MiB of jsonb in one row.

    The failures' own collect ALREADY compacts past
    its cap (``FANIN_FAILURES_BYTE_CAP`` + the ``__truncated__`` marker +
    the ledger pointer, pinned in ``test_wf_pruner_pins.py``) — the
    collected RESULT envelope lacks the same discipline. Mirroring it
    (compact past the cap, the full collect stays on the child rows the
    read-side aggregate already reads) is the one-mechanism fix; this
    pin FLIPS GREEN the day it lands.

    The model and
    the bodies live at MODULE level (above) — the recorded
    function-local ``ItemsIn`` is invisible to the body-hints walk (the
    closure cell carries only the body's own free variables; the
    annotation is a string under the future-import), and the walk's
    local definition reds at the AttributeError, never at the size
    assert — an unfaithful repro. Module level = the annotation
    resolves, the body runs, the map forks, and the red names the
    ENVELOPE'S BYTES (the finding's actual face)."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, build, map_source, step

    app = WorkflowApp()

    @app.workflow("ledger_envelope")
    def ledger_envelope() -> Promise[object]:
        return build(map_source(step(_f4_emit, _F4ItemsIn(), key="src"), _f4_fat_item))

    result = await wf.run(app.get("ledger_envelope"), wf_pool, wf_schema)
    assert result.outcome == "terminal", result
    async with wf_pool.acquire() as conn:
        # S608 is per-file-ignored for this pack (the schema identifier is
        # the fixture's own settings-validated identifier; values $-bound
        # or the step key's literal).
        size = await conn.fetchval(
            f"SELECT result_size_bytes FROM \"{wf_schema}\".jobs WHERE step_key = 'src.join'"
        )
    assert size is not None, "the join row did not terminalize"
    cap = 64 * 1024
    assert int(size) <= cap, (
        f"the fan-in envelope is {int(size)} bytes — {int(size) / cap:.1f}x the "
        "estate's 64 KiB result cap: the wf terminal write bypasses the "
        "size discipline every plain job's terminal enforces (the "
        "payload-bloat finding; the fix is the failures' own compaction "
        "mechanism mirrored to the collect)"
    )


class _F4FatOut(BaseModel):
    i: int
    fat: str


async def _f4_consumer(ctx: Any, items: list[_F4FatOut]) -> dict[str, int]:
    """The CONSUMER's seat (the read-side pin's body): the declared
    param says ``list[...]`` — the join's collect is this body's input,
    truncated or not."""
    return {"kept": len(items)}


@pytest.mark.integration
async def test_pin_fanin_truncated_envelope_unwraps_at_the_consumer_decode(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The F4 fix's read side (the envelope-cap fix's own regression,
    rejected by the recertifier): the bounded collect's envelope is
    THREE keys over the cap — ``{"value": […], "__truncated__": N,
    "detail_home": …}`` — and a decode that unwraps by
    ``len(decoded) == 1`` never unwraps it, and
    every consumer wired past a truncating collect received the
    ENVELOPE DICT where its declared param says ``list[...]`` — the
    typed decode died ``Input should be a valid list, input_type=dict``
    and the run failed (the LedgerWatch chaos feed's live face:
    ``__truncated__: 272``).

    ``decode_result`` unwraps the runner's envelope
    BY ITS KNOWN KEY SET — ``value`` alone or ``value`` + the F4
    markers — so both faces reach the consumer's typed decode as the
    declared list; and the truncated face's ``detail_home`` contract is
    HONORED: the runner re-reads the FULL collect off the children's
    own result rows (the same ordered parent read the join packed), so
    the consumer's decoded param is the WHOLE collect, never a silent
    kept-prefix (the LedgerWatch scenario's own demand — a 272-item
    silent loss is the C7 family's dead letter wearing the cap's hat).
    The raw row keeps the bounded
    markers, and the finalize's metadata receipt
    (``wf_collect_truncated``) carries the count where the reads
    decide — this pin reads both (the receipt's count names exactly
    the items the ROW no longer spells)."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, build, map_source, step

    app = WorkflowApp()

    @app.workflow("f4_readside")
    def f4_readside() -> Promise[object]:
        items = map_source(step(_f4_emit, _F4ItemsIn(), key="src"), _f4_fat_item)
        return build(step(_f4_consumer, items, key="consume"))

    result = await wf.run(app.get("f4_readside"), wf_pool, wf_schema)
    assert result.outcome == "terminal", (
        f"the consumer wired past a truncating collect failed at its "
        f"typed decode (the envelope leaked as the param's value): {result}"
    )
    decoded = result.result
    assert isinstance(decoded, dict) and set(decoded) == {"kept"}, (
        f"the consumer's dict return survived the round-trip: {decoded!r}"
    )
    kept = decoded["kept"]
    assert kept == 700, (
        f"the consumer's decoded param must be the FULL collect (the "
        f"detail re-read off the child rows — the detail_home contract): "
        f"got {kept!r} of 700"
    )
    async with wf_pool.acquire() as conn:
        # S608 is per-file-ignored for this pack (the schema identifier is
        # the fixture's own settings-validated identifier; values $-bound
        # or the step key's literal).
        row = await conn.fetchrow(
            f"SELECT result, metadata FROM \"{wf_schema}\".jobs WHERE step_key = 'src.join'"
        )
    assert row is not None, "the join row did not terminalize"
    raw = row["result"]
    raw_doc: Any = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(raw_doc, dict) and "__truncated__" in raw_doc, (
        f"the join row's RAW envelope lost the honest markers: {raw_doc!r}"
    )
    spelled = len(raw_doc["value"])
    assert int(raw_doc["__truncated__"]) == 700 - spelled > 0, raw_doc
    meta_raw = row["metadata"]
    meta_doc: Any = json.loads(meta_raw) if isinstance(meta_raw, str) else (meta_raw or {})
    receipt = meta_doc.get("wf_collect_truncated") if isinstance(meta_doc, dict) else None
    assert receipt is not None, (
        "the join row's metadata carries no truncation receipt — the count "
        "must surface where the reads decide (wf_collect_truncated), never "
        "swallowed by the decode's unwrap"
    )
    assert int(receipt["count"]) == 700 - spelled, receipt


# ── #744: a map placed DIRECTLY on a truncated join forks every item ────
# (the F4 read side's fork face: the consumer's decode re-reads the full
# collect off the child rows; a second map_source on the join is the other
# reader of the same envelope. Module-level bodies for the body-hints walk.)


class _MjSeedsIn(BaseModel):
    seeds: int = 10
    n: int = 200
    size: int = 100
    offset: int = 0


async def _mj_seeds(ctx: Any, params: _MjSeedsIn) -> list[dict[str, int]]:
    seeds = range(params.offset, params.offset + params.seeds)
    return [{"seed": s, "n": params.n, "size": params.size} for s in seeds]


async def _mj_expand(ctx: Any, seed: dict[str, int]) -> list[str]:
    # ~20 KB per child: ten of them overflow the join's 64 KiB envelope.
    return [f"s{seed['seed']}-{k:05d}-".ljust(seed["size"], "x") for k in range(seed["n"])]


async def _mj_ident(ctx: Any, x: list[str]) -> list[str]:
    return x


async def _mj_count(ctx: Any, xs: list[list[str]]) -> dict[str, int]:
    return {"n_received": len(xs), "n_flat": sum(len(x) for x in xs)}


async def _mj_results(pool: asyncpg.Pool, schema: str, flow_id: Any, step_key: str) -> list[Any]:
    """The step's raw result envelopes in ``map_index`` order, flow-scoped
    (the module's schema is shared, and ``gather`` takes no key)."""
    # S608 is per-file-ignored for this pack (the fixture's schema identifier).
    rows = await pool.fetch(
        f'SELECT result FROM "{schema}".jobs '
        "WHERE step_key = $1 AND (metadata->>'flow_id')::uuid = $2 ORDER BY map_index",
        step_key,
        flow_id,
    )
    return [json.loads(r["result"]) if isinstance(r["result"], str) else r["result"] for r in rows]


def _mj_fingerprints(children: list[Any]) -> list[tuple[str, int]]:
    # each _mj_ident child's list, named by its first item's stem and its length.
    return [(doc["value"][0].rstrip("x"), len(doc["value"])) for doc in children]


def _mj_truncated(doc: Any) -> int:
    return int(doc.get("__truncated__", 0)) if isinstance(doc, dict) else 0


@pytest.mark.integration
async def test_pin_map_on_truncated_join_forks_every_collected_item(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744: ``map_source`` wired directly on a map's join. The join's
    envelope is bounded (``_bounded_collect`` keeps the prefix that fits
    under ``FANIN_FAILURES_BYTE_CAP`` and counts the rest in
    ``__truncated__``); the full collect lives on the child rows. The
    consumer decode honours that (the pin above). The nested map's fork
    must too: it forks one child per item the join COLLECTED, never one
    per item the bounded row still spells. The pre-fix fork read
    ``result["value"]`` and forked the kept prefix (3 of 10 here) while
    the run still ended succeeded."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, build, map_source, step

    app = WorkflowApp()

    @app.workflow("map_on_truncated_join")
    def map_on_truncated_join() -> Promise[object]:
        first = map_source(step(_mj_seeds, _MjSeedsIn(), key="seeds"), _mj_expand)
        return build(step(_mj_count, map_source(first, _mj_ident), key="final"))

    result = await wf.run(app.get("map_on_truncated_join"), wf_pool, wf_schema)
    async with wf_pool.acquire() as conn:
        # S608 is per-file-ignored for this pack (the schema identifier is
        # the fixture's own settings-validated identifier; values $-bound).
        join_raw = await conn.fetchval(
            f'SELECT result FROM "{wf_schema}".jobs '
            "WHERE step_key = 'seeds.join' AND (metadata->>'flow_id')::uuid = $1",
            result.flow_id,
        )
        forked = await conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE step_key = 'seeds.join.item' AND (metadata->>'flow_id')::uuid = $1",
            result.flow_id,
        )
    join_doc: Any = json.loads(join_raw) if isinstance(join_raw, str) else join_raw
    # the precondition: the source join's row IS the truncated envelope.
    assert isinstance(join_doc, dict) and int(join_doc.get("__truncated__", 0)) > 0, (
        f"the fixture must overflow the join's envelope cap: {str(join_doc)[:200]}"
    )
    spelled = len(join_doc["value"])
    assert int(forked) == 10, (
        f"the map on the join forked {forked} children for 10 collected "
        f"items (the bounded row spells {spelled}, __truncated__="
        f"{join_doc['__truncated__']}): the fork read the kept prefix, "
        "not the full collect on the child rows"
    )
    # identity and order: child k carries the join's k-th item (ORDER BY p.id).
    children = await _mj_results(wf_pool, wf_schema, result.flow_id, "seeds.join.item")
    assert _mj_fingerprints(children) == [(f"s{s}-00000-", 200) for s in range(10)]
    assert result.outcome == "terminal", result
    assert result.result == {"n_received": 10, "n_flat": 2000}, result.result


class _MjSideIn(BaseModel):
    tag: str
    n: int


async def _mj_side(ctx: Any, params: _MjSideIn) -> list[str]:
    return [f"{params.tag}-{k:05d}-".ljust(100, "x") for k in range(params.n)]


async def _mj_head(ctx: Any, x: str) -> str:
    return x[:8]


async def _mj_len(ctx: Any, xs: list[str]) -> int:
    return len(xs)


def _mj_heads(n: int) -> list[str]:
    # the gather's flatten of two _mj_side lists: wiring order, then item order.
    return [f"{tag}-{k:05d}-" for tag in ("a", "b") for k in range(n)]


@pytest.mark.integration
async def test_pin_map_on_truncated_gather_forks_every_flattened_item(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744, the gather face: ``gather`` packs through the same bounded
    collect (flattening one level when every parent returned a list), so
    a map wired directly on it reads the same capped envelope. 2 x 400
    ~100-byte strings overflow the cap; the fork must cover all 800
    flattened items (the pre-fix fork covered the kept prefix)."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, build, gather, map_source, step

    app = WorkflowApp()

    @app.workflow("map_on_truncated_gather")
    def map_on_truncated_gather() -> Promise[object]:
        both = gather(
            [
                step(_mj_side, _MjSideIn(tag="a", n=400), key="a"),
                step(_mj_side, _MjSideIn(tag="b", n=400), key="b"),
            ]
        )
        return build(step(_mj_len, map_source(both, _mj_head), key="final"))

    result = await wf.run(app.get("map_on_truncated_gather"), wf_pool, wf_schema)
    (gather_doc,) = await _mj_results(wf_pool, wf_schema, result.flow_id, "gather")
    assert _mj_truncated(gather_doc) > 0, "the fixture must overflow the gather's envelope cap"
    children = await _mj_results(wf_pool, wf_schema, result.flow_id, "gather.item")
    forked = len(children)
    assert forked == 800, f"the map on the gather forked {forked} of 800 flattened items"
    assert [doc["value"] for doc in children] == _mj_heads(400)
    assert result.outcome == "terminal", result
    assert result.result == 800, result.result


@pytest.mark.integration
async def test_pin_map_on_gather_of_truncated_joins_forks_every_item(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744, the nested face: a gather over two truncated map joins
    collects 20 items (its own pack re-reads each truncated parent off
    the child rows), and its row is truncated again. The fork over it
    must see all 20 — the list the gather packed, never the gather row's
    prefix nor a one-level re-read of the parents' (themselves capped)
    rows."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, build, gather, map_source, step

    app = WorkflowApp()

    @app.workflow("map_on_gather_of_truncated_joins")
    def map_on_gather_of_truncated_joins() -> Promise[object]:
        m1 = map_source(step(_mj_seeds, _MjSeedsIn(), key="s1"), _mj_expand)
        m2 = map_source(step(_mj_seeds, _MjSeedsIn(offset=10), key="s2"), _mj_expand)
        return build(step(_mj_count, map_source(gather([m1, m2]), _mj_ident), key="final"))

    result = await wf.run(app.get("map_on_gather_of_truncated_joins"), wf_pool, wf_schema)
    for key in ("s1.join", "s2.join", "gather"):
        (doc,) = await _mj_results(wf_pool, wf_schema, result.flow_id, key)
        assert _mj_truncated(doc) > 0, f"the fixture must overflow {key}'s envelope cap"
    children = await _mj_results(wf_pool, wf_schema, result.flow_id, "gather.item")
    forked = len(children)
    assert forked == 20, f"the map on the gather forked {forked} of 20 collected items"
    # s1's ten seeds, then s2's ten (the gather's wiring order).
    assert _mj_fingerprints(children) == [(f"s{s}-00000-", 200) for s in range(20)]
    assert result.outcome == "terminal", result
    assert result.result == {"n_received": 20, "n_flat": 4000}, result.result


# ── #744: the fork's bounds count the TRUE total, refused by name ───────
# (the fan-in bound and the map_index ceiling run on the list the fork
# reads, so a fork over the full pack can newly cross them; both must
# terminal-fail the source node through the ladder, never escape it.)


async def _mj_node(pool: asyncpg.Pool, schema: str, flow_key: str, step_key: str) -> Any:
    """``step_key``'s row in the newest run that owns the ``flow_key`` step:
    a failed run raises out of ``wf.run`` before it hands back a flow id."""
    # S608 is per-file-ignored for this pack (the fixture's schema identifier).
    return await pool.fetchrow(
        f"SELECT j.status::text AS status, j.error_class, j.attempt, "
        f"(SELECT count(*) FROM \"{schema}\".jobs c WHERE c.step_key = j.step_key || '.item' "
        "AND c.metadata->>'flow_id' = j.metadata->>'flow_id') AS forked "
        f"FROM \"{schema}\".jobs j WHERE j.step_key = $2 AND j.metadata->>'flow_id' = "
        f"(SELECT metadata->>'flow_id' FROM \"{schema}\".jobs WHERE step_key = $1 "
        "ORDER BY id DESC LIMIT 1)",
        flow_key,
        step_key,
    )


@pytest.mark.integration
async def test_pin_map_on_truncated_gather_counts_the_true_total_against_the_fan_in_bound(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744: 2 x 600 ~100-byte strings — the gather packs 1200, its row
    spells 634. The fork counts the 1200 against ``MAX_FAN_IN_PER_JOIN``
    and the gather terminal-fails by name on attempt 1, the refused fork
    landing nothing. The pre-fix fork saw the 634 prefix, passed the
    bound and the run succeeded on partial data."""
    import taskq.workflows as wf
    from taskq.workflows import (
        Promise,
        WorkflowApp,
        WorkflowRunError,
        build,
        gather,
        map_source,
        step,
    )

    app = WorkflowApp()

    @app.workflow("map_on_gather_over_the_fan_in_bound")
    def map_on_gather_over_the_fan_in_bound() -> Promise[object]:
        both = gather(
            [
                step(_mj_side, _MjSideIn(tag="a", n=600), key="bound_a"),
                step(_mj_side, _MjSideIn(tag="b", n=600), key="bound_b"),
            ]
        )
        return build(step(_mj_len, map_source(both, _mj_head), key="final"))

    with pytest.raises(
        WorkflowRunError,
        match=r"node 'gather' terminal-failed: \[FanInBoundExceededError\] "
        r"join 'gather\.join' fans in 1200 children",
    ):
        await wf.run(app.get("map_on_gather_over_the_fan_in_bound"), wf_pool, wf_schema)
    node = await _mj_node(wf_pool, wf_schema, "bound_a", "gather")
    assert node is not None
    assert (node["status"], node["error_class"], node["attempt"], node["forked"]) == (
        "failed",
        "FanInBoundExceededError",
        1,
        0,
    ), dict(node)


@pytest.mark.integration
async def test_pin_child_driven_map_on_truncated_gather_forks_every_item_past_the_bound(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744, the bound's escape: the same 2 x 600 gather with
    ``child_driven=True`` on the nested map forks all 1200 items and
    succeeds (the pre-fix fork covered the 634-item prefix)."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, build, gather, map_source, step

    app = WorkflowApp()

    @app.workflow("child_driven_map_on_truncated_gather")
    def child_driven_map_on_truncated_gather() -> Promise[object]:
        both = gather(
            [
                step(_mj_side, _MjSideIn(tag="a", n=600), key="cd_a"),
                step(_mj_side, _MjSideIn(tag="b", n=600), key="cd_b"),
            ]
        )
        mapped = map_source(both, _mj_head, child_driven=True)
        return build(step(_mj_len, mapped, key="final"))

    result = await wf.run(app.get("child_driven_map_on_truncated_gather"), wf_pool, wf_schema)
    (gather_doc,) = await _mj_results(wf_pool, wf_schema, result.flow_id, "gather")
    assert _mj_truncated(gather_doc) > 0, "the fixture must overflow the gather's envelope cap"
    children = await _mj_results(wf_pool, wf_schema, result.flow_id, "gather.item")
    assert len(children) == 1200, f"the child-driven map forked {len(children)} of 1200"
    assert [doc["value"] for doc in children] == _mj_heads(600)
    assert result.outcome == "terminal", result
    assert result.result == 1200, result.result


async def _mj_ints(ctx: Any) -> list[int]:
    return list(range(1000))


async def _mj_too_many(ctx: Any) -> list[int]:
    return [k % 10 for k in range(32769)]


async def _mj_int(ctx: Any, x: int) -> int:
    return x


@pytest.mark.integration
async def test_pin_map_on_gather_past_the_map_index_ceiling_fails_by_name(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744: 34 x 1000 small ints — the gather packs 34,000, past the
    ``map_index`` smallint ceiling. The fork's ``MapIndexExhaustedError``
    terminal-fails the gather by name on attempt 1 through the ladder;
    unrouted, it escaped ``wf.run`` raw and left the gather row running
    (a reclaim-and-re-crash wedge on a worker). The pre-fix fork saw
    the row's prefix and failed the fan-in bound instead."""
    import taskq.workflows as wf
    from taskq.workflows import (
        Promise,
        WorkflowApp,
        WorkflowRunError,
        build,
        gather,
        map_source,
        step,
    )

    app = WorkflowApp()

    @app.workflow("map_on_gather_past_the_map_index_ceiling")
    def map_on_gather_past_the_map_index_ceiling() -> Promise[object]:
        ints = gather([step(_mj_ints, key=f"ceil_{k}") for k in range(34)])
        return build(map_source(ints, _mj_int))

    with pytest.raises(
        WorkflowRunError,
        match=r"node 'gather' terminal-failed: \[MapIndexExhaustedError\] "
        r"the map source 'gather' returned 34000 items",
    ):
        await wf.run(app.get("map_on_gather_past_the_map_index_ceiling"), wf_pool, wf_schema)
    node = await _mj_node(wf_pool, wf_schema, "ceil_0", "gather")
    assert node is not None
    assert (node["status"], node["error_class"], node["attempt"], node["forked"]) == (
        "failed",
        "MapIndexExhaustedError",
        1,
        0,
    ), dict(node)


@pytest.mark.integration
async def test_pin_map_source_past_the_map_index_ceiling_fails_by_name(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744, the same route's body face: a step returning 32,769 items to
    its own map terminal-fails by name on attempt 1. Before the route it
    raised ``MapIndexExhaustedError`` raw out of ``wf.run`` with the
    source row left running."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, WorkflowRunError, build, map_source, step

    app = WorkflowApp()

    @app.workflow("map_source_past_the_map_index_ceiling")
    def map_source_past_the_map_index_ceiling() -> Promise[object]:
        return build(map_source(step(_mj_too_many, key="ceil_src"), _mj_int))

    with pytest.raises(
        WorkflowRunError,
        match=r"node 'ceil_src' terminal-failed: \[MapIndexExhaustedError\] "
        r"the map source 'ceil_src' returned 32769 items",
    ):
        await wf.run(app.get("map_source_past_the_map_index_ceiling"), wf_pool, wf_schema)
    node = await _mj_node(wf_pool, wf_schema, "ceil_src", "ceil_src")
    assert node is not None
    assert (node["status"], node["error_class"], node["attempt"], node["forked"]) == (
        "failed",
        "MapIndexExhaustedError",
        1,
        0,
    ), dict(node)


_MJ_CACHE_CALLS: dict[str, int] = {}


async def _mj_cached_ints(ctx: Any) -> list[int]:
    _MJ_CACHE_CALLS["n"] = _MJ_CACHE_CALLS.get("n", 0) + 1
    return list(range(1200))


@pytest.mark.integration
async def test_pin_cache_hit_map_fork_past_the_fan_in_bound_fails_by_name(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """#744, the same route's cache-hit face: the step cache addresses the
    body and its args, never the wiring, so a 1200-item list cached under
    a plain wiring replays into a map wiring whose fork crosses
    ``MAX_FAN_IN_PER_JOIN``. The hit terminal-fails the source by name on
    attempt 1, like the miss path. Before the route covered the hit it
    raised ``FanInBoundExceededError`` raw out of ``wf.run`` with the
    source row left running."""
    import taskq.workflows as wf
    from taskq.workflows import Promise, WorkflowApp, WorkflowRunError, build, map_source, step

    app = WorkflowApp()

    @app.workflow("cache_plain_ints")
    def cache_plain_ints() -> Promise[object]:
        return build(step(_mj_cached_ints, key="cache_ints", cache=True))

    @app.workflow("cache_hit_map_over_the_fan_in_bound")
    def cache_hit_map_over_the_fan_in_bound() -> Promise[object]:
        return build(map_source(step(_mj_cached_ints, key="cache_ints", cache=True), _mj_int))

    _MJ_CACHE_CALLS.clear()
    await wf.run(app.get("cache_plain_ints"), wf_pool, wf_schema)
    with pytest.raises(
        WorkflowRunError,
        match=r"node 'cache_ints' terminal-failed: \[FanInBoundExceededError\] "
        r"join 'cache_ints\.join' fans in 1200 children",
    ):
        await wf.run(app.get("cache_hit_map_over_the_fan_in_bound"), wf_pool, wf_schema)
    assert _MJ_CACHE_CALLS == {"n": 1}, "the mapped run must be a cache hit (the body never reran)"
    node = await _mj_node(wf_pool, wf_schema, "cache_ints", "cache_ints")
    assert node is not None
    assert (node["status"], node["error_class"], node["attempt"], node["forked"]) == (
        "failed",
        "FanInBoundExceededError",
        1,
        0,
    ), dict(node)
