"""The hold/map machinery's pins — the hold identity, the item ladder,
the substep's pool boundary, the watchdog budgets.

Each pin's REJECTED VARIANT (the wedge/theft repro, the lying ladder,
the pool-boundary collapse, the warning storm's config) is recorded to
the failure log sink; the pins assert the shipped behavior.

* the hold identity carries the map index — concurrent HITL
  holds from map siblings are DISTINCT rows, addressed and resolved
  INDIVIDUALLY. The rejected variant: one hold row for two siblings
  (the held-representation contamination), the cross-sibling answer
  theft, the wedged ``running`` corpse.
* the map item's ladder HONORS the stamped
  ``max_attempts`` — the always-fail probe dies AT 7. The rejected
  variant: the row stamped 7, the death at 3 (the lying middle).
* the substep claim does not hold a pool connection
  across the body — concurrent map children claim CONCURRENTLY. The
  rejected variant: the execution door rides the 4-conn dispatcher
  pool and the substep holds its connection across ``fn`` — the fan-out
  collapses to 4 in-flight (the A/B: 11.1 s -> 42.2 s at 300 x 0.5 s).
* the boot-time typed refusal of ``warn_budget <=
  check_interval`` — the warning storm's config cannot boot. The
  rejected variant: warn 0.4 / check 0.5 validates, then 295 warn+dump
  pairs starve a heartbeat.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest
from pydantic import BaseModel

from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, map_source, step
from taskq.workflows.api._ctx_wait import Expired
from taskq.workflows.api._hitl import HitlClient


class Ingest(BaseModel):
    batch_id: str


class ItemApproval(BaseModel):
    note: str


class Verdict(BaseModel):
    index: int | None = None
    note: str = ""


async def test_map_sibling_holds_are_addressable_and_resolvable_individually(
    wf_pool: Any, wf_schema: str, wf_conn: Any, hitl_redlog: Any
) -> None:
    """FIX 1 (LedgerWatch F2): two map siblings hold CONCURRENTLY on the
    same signal name; ``HitlClient.list`` shows both distinctly; each
    resolve wakes exactly ITS own sibling; each sibling consumes ITS own
    answer; the join fires; the run completes.

    The
    sibling's wait site found the OTHER sibling's held row (the
    held-representation read filtered by node_key only) — one hold row
    for two siblings, the second sibling wedged ``running`` forever (in
    process) or the hold INSERT UniqueViolationed into the ladder
    (worker mode); a resolve woke one arbitrary sibling and the OTHER
    consumed the first's delivered answer (the answer theft).
    """
    app = WorkflowApp()

    async def src_body(ctx: Any, params: Ingest) -> list[int]:
        return [0, 1]

    async def item_body(ctx: Any, txn_id: int) -> Verdict:
        outcome = await ctx.wait_signal((ItemApproval,), timeout_s=120.0)
        if isinstance(outcome, Expired):
            raise AssertionError("the hold expired inside the pin's window")
        return Verdict(index=ctx.map_index, note=outcome.note)

    async def join_body(ctx: Any, items: list[Verdict]) -> list[Verdict]:
        return items

    @app.workflow("holdmap_sibling_holds")
    def flow() -> Promise[object]:
        src = step(src_body, Ingest(batch_id="b1"), key="src")
        items = map_source(src, item_body, max_attempts=7)
        return build(step(join_body, items, key="collect"))

    runner = FlowRunner(app.get("holdmap_sibling_holds"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    client = HitlClient(wf_pool, wf_schema)

    # Drive until both siblings hold (the second sibling's claim needs a
    # tick past the first hold). Bounded: the rejected variant never
    # produces a second hold — the loop exits on the tick budget.
    holds: list[Any] = []
    for _ in range(20):
        holds = await client.list(flow_id)
        if len(holds) == 2:
            break
        await runner.drive(flow_id, until="held")
    # a run with multiple concurrent holds lists all of them distinctly.
    assert len(holds) == 2, (
        f"the map siblings' concurrent holds are NOT distinct rows: "
        f"{len(holds)} hold(s) — the rejected variant (the held-"
        "representation contamination: the sibling's wait site found the "
        "OTHER sibling's held row and never registered its own; the "
        f"sibling wedged). Observed: "
        f"{[(h.node_key, h.signal_name, h.hold_epoch) for h in holds]}"
    )
    # the siblings are DISTINCT rows on the same (node_key, signal_name,
    # epoch) coordinates — only the map index tells them apart.
    keys = {(h.node_key, h.signal_name, h.hold_epoch) for h in holds}
    assert len(keys) == 1, keys
    indexes = sorted(h.map_index for h in holds)
    assert indexes == [0, 1], indexes
    hitl_redlog.red(
        "holdmap-c1-map-sibling-holds",
        "the variant (one hold row for two siblings; the second "
        "sibling wedged 'running' forever; a resolve woke one arbitrary "
        "sibling and the other stole the first's answer)",
        {
            "holds_listed": len(holds),
            "map_indexes": indexes,
            "recorded finding": "the shipped identity lists BOTH siblings distinctly",
        },
    )

    # address each hold by ID; each answer must land on its own sibling (the
    # cross-sibling theft is the dragon).
    holds.sort(key=lambda h: h.map_index or 0)
    first, second = holds
    r1 = await client.resolve(first.hold_id, {"note": "answer-zero"})
    assert r1.status == "delivered", r1
    await runner.drive(flow_id, until="held")
    r2 = await client.resolve(second.hold_id, {"note": "answer-one"})
    assert r2.status == "delivered", r2

    # the run completes — the join fired, the root terminal truthful.
    status = None
    for _ in range(40):
        await runner.drive(flow_id)
        status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        if status in ("succeeded", "failed", "cancelled"):
            break
        await asyncio.sleep(0.05)
    assert status == "succeeded", status

    # each sibling delivered its own note — the cross-sibling theft (sibling 1
    # succeeding on sibling 0's answer) is the rejected dragon.
    collect = await wf_conn.fetchval(
        f"SELECT result->>'value' FROM \"{wf_schema}\".jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'collect'",
        flow_id,
    )
    assert collect is not None
    items = json.loads(collect)
    by_index = {v["index"]: v["note"] for v in items}
    assert by_index == {0: "answer-zero", 1: "answer-one"}, by_index


async def test_map_item_ladder_honors_the_stamped_max_attempts(
    wf_pool: Any, wf_schema: str, wf_conn: Any, hitl_redlog: Any
) -> None:
    """FIX 2 (LedgerWatch F3): ``map_source(..., max_attempts=7)`` is
    stamped on the child rows — the item ladder HONORS it: the
    always-fail probe dies AT 7 (seven failed ledger rows, the terminal
    at attempt 7).

    A plain step
    wired 7 laddered 7 times; a map item wired 7 died at 3 — the row
    LIED, every adoption re-implemented the ladder client-side.
    """
    app = WorkflowApp()

    async def src_body(ctx: Any, params: Ingest) -> list[int]:
        return [5]

    async def fail_body(ctx: Any, txn_id: int) -> Verdict:
        raise RuntimeError(f"always-fail item, attempt {ctx.attempt}")

    async def join_body(ctx: Any, items: list[Verdict]) -> int:
        return len(items)

    @app.workflow("holdmap_item_ladder_seven")
    def flow() -> Promise[object]:
        src = step(src_body, Ingest(batch_id="b1"), key="src")
        items = map_source(src, fail_body, max_attempts=7)
        return build(step(join_body, items, key="collect"))

    runner = FlowRunner(app.get("holdmap_item_ladder_seven"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    for _ in range(400):
        await runner.drive(flow_id)
        root = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        if root in ("failed", "succeeded", "cancelled"):
            break
        await asyncio.sleep(0.02)

    # the item's ledger rows — one failed row per ladder attempt, the terminal
    # at the stamped ceiling.
    rows = await wf_conn.fetch(
        f'SELECT attempt, status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'src.item' ORDER BY attempt",
        flow_id,
    )
    failed = [(r["attempt"], r["status"]) for r in rows if r["status"] == "failed"]
    assert len(failed) == 7, (
        f"the map item's ladder ignored the stamped max_attempts=7 — the "
        f"item died after {len(failed)} failed attempt(s) {failed}: the "
        "rejected variant (the row stamped 7, the death at 3 — the "
        "lying middle every adoption re-implements client-side)"
    )
    assert [a for a, _ in failed] == [1, 2, 3, 4, 5, 6, 7], failed
    # the job row terminalized (the propagation took over at the ceiling).
    item = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'src.item'",
        flow_id,
    )
    assert item is not None and item["status"] == "failed", item
    hitl_redlog.red(
        "holdmap-c2-item-ladder-ceiling",
        "the variant (the item's ladder capped at 3 regardless "
        "of the stamped max_attempts=7 — the row lied)",
        {
            "stamped": 7,
            "observed_attempts": len(failed),
            "recorded finding": "the shipped ladder dies AT the stamped ceiling",
        },
    )


# Why load_sensitive: the pin's ceiling (six 0.3 s bodies inside 0.6 s)
# is one round of claim round-trips of margin — co-tenant scheduling on
# a parallel lane can spend that on its own. The overlap law the pin
# exists for is a serial-lane measurement.
@pytest.mark.load_sensitive
async def test_the_substep_does_not_hold_the_pool_connection_across_the_body(
    module_pg_schema: Any, wf_schema: str, hitl_redlog: Any
) -> None:
    """FIX 3 (LedgerWatch F4): the substep's claim path releases the
    pool connection ACROSS the body — concurrent map children claim
    CONCURRENTLY, the exactly-once law untouched.

    The scenario: SIX concurrent substeps whose bodies sleep 0.3 s each, on
    a TWO-connection pool (the dispatcher pool's shipped default is 4 —
    the A/B's collapsed ceiling). With the connection held across
    ``fn`` the substeps serialize onto the pool: ~3 rounds x 0.3 s. With
    the release: one round (~0.3 s) — the bodies overlap.

    the REJECTED VARIANT (the adoption team's A/B, re-measured on this
    tree): 300-child fan-out, 0.5 s per call — bare 11.1 s at 32
    in-flight; substep-wrapped 42.2 s at 22 (the instrumented arm: the
    pool at 4 conns, the acquire wait ~600 ms per substep). The ledger
    claim itself never serialized (300 concurrent claims: 12 ms).
    """
    import asyncpg

    from taskq._ids import new_uuid
    from taskq.backend._protocol import JobId
    from taskq.workflows.api._ctx import build_step_context
    from taskq.workflows.engine import render_workflow_sql

    # The two-connection pool: the rejected ceiling, made exact.
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    try:
        wsql = render_workflow_sql(wf_schema)
        fid, jid = JobId(new_uuid()), JobId(new_uuid())
        t0 = time.perf_counter()

        async def one(i: int) -> None:
            ctx = build_step_context(
                flow_id=fid,
                job_id=jid,
                node_key=f"item:{i}",
                attempt=1,
                input=None,
                pool=pool,
                wsql=wsql,
                map_index=i,
            )
            got = await ctx.substep(f"enrich:{i}", _sleep_body, 0.3)
            assert got == "slept", got

        await asyncio.gather(*[one(i) for i in range(6)])
        wall = time.perf_counter() - t0

        # six 0.3 s bodies on a 2-conn pool with the connection released across
        # the bodies complete in ~one round (the claims are ~1 ms statements);
        # the held-connection variant serializes to ~3 rounds. The margin is
        # generous (the CI noise floor) but the serialized shape cannot fit
        # under it.
        assert wall < 0.6, (
            f"six 0.3 s concurrent substeps took {wall:.2f}s on a "
            "2-connection pool — the substep HELD its connection across "
            "the body (the rejected variant: the fan-out's in-flight "
            "collapsed to the pool's conn count; the A/B's 3.8x tax)"
        )

        # six terminal rows, the replay returns the recorded result (never a
        # re-execution).
        rows = await pool.fetch(
            f'SELECT step_key, status FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key LIKE 'enrich:%'",
            fid,
        )
        assert len(rows) == 6 and all(r["status"] == "succeeded" for r in rows), rows
        replay_ctx = build_step_context(
            flow_id=fid,
            job_id=jid,
            node_key="item:0",
            attempt=1,
            input=None,
            pool=pool,
            wsql=wsql,
            map_index=0,
        )
        ran_again = {"called": False}

        async def spy_body(_s: float) -> str:
            ran_again["called"] = True
            return "re-ran"

        got = await replay_ctx.substep("enrich:0", spy_body, 0.3)
        assert got == "slept", got  # the recorded result...
        assert ran_again["called"] is False  # ...never a re-execution
        hitl_redlog.red(
            "holdmap-c3-substep-conn-release",
            "the variant (the substep holds its pool connection "
            "across the body: the dispatcher pool's 4 conns throttled "
            "the fan-out — the A/B's 11.1 s → 42.2 s collapse)",
            {
                "pool_conns": 2,
                "bodies": 6,
                "body_s": 0.3,
                "observed_wall_s": round(wall, 2),
                "recorded finding": "the shipped substep releases across the body",
            },
        )
    finally:
        await pool.close()


async def _sleep_body(seconds: float) -> str:
    await asyncio.sleep(seconds)
    return "slept"


def test_the_watchdog_refuses_a_warn_budget_below_the_check_interval(
    hitl_redlog: Any,
) -> None:
    """FIX 4 (LedgerWatch F6): the warning storm's config cannot BOOT.

    warn_budget <= check_interval validates today (the lattice catches
    budget <= check and warn >= budget, never warn vs check) and then
    warning-STORMS: the warn budget below the check cadence warns on
    every healthy beat and EACH warn dumps every live task — the dump
    blocks the loop, a self-sustaining storm (the team's 295 warn+dump
    pairs starved a heartbeat). The refusal teaches the fix.
    """
    from taskq.settings import WorkerSettings

    def _load(**overrides: str) -> WorkerSettings:
        base: dict[str, str] = {"TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost/taskq"}
        base.update(overrides)
        return WorkerSettings.load_from_dict(base)

    # passes the the storm lattice (warn 0.4 < budget 1.0; budget 1.0 > check
    # 0.5), then storms.
    with pytest.raises(Exception) as exc_info:
        _load(
            TASKQ_WATCHDOG_CHECK_INTERVAL="0.5",
            TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET="0.4",
            TASKQ_WATCHDOG_LOOP_LAG_BUDGET="1.0",
            TASKQ_LOCK_LEASE="60.0",
        )
    msg = str(exc_info.value)
    assert "warn_budget" in msg or "warn budget" in msg, msg
    assert "check_interval" in msg, msg
    # the remedy is taught (the lattice's own standard).
    assert "Raise" in msg or "raise" in msg, msg
    hitl_redlog.red(
        "holdmap-c4-warn-below-check-refusal",
        "the variant (warn 0.4 < check 0.5 validated, then 295 "
        "warn+dump pairs starved the heartbeat)",
        {
            "check": 0.5,
            "warn": 0.4,
            "budget": 1.0,
            "recorded finding": "the shipped lattice refuses the config at boot",
        },
    )

    # the team's valid tuning boots (the fleet's own fix, unchanged).
    s = _load(
        TASKQ_WATCHDOG_CHECK_INTERVAL="1.0",
        TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET="1.5",
        TASKQ_WATCHDOG_LOOP_LAG_BUDGET="2.0",
        TASKQ_LOCK_LEASE="60.0",
    )
    assert s.watchdog_loop_lag_warn_budget == 1.5
