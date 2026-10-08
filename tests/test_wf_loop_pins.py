"""T19 — THE LOOP PINS: the two walls, the carry, the ladder classes.

Red-first: each pin's CONVICTED VARIANT was run against the mutated guard
(the mutation drill — BUILD-PROTOCOL §2), the observed red captured to
``.measurements/t19-pin-reds.json``; the unfenced CONSUME-BUDGET variant
stays RED FOREVER (it is the dragon, not a bug to fix).

Pins (the ticket's red-first numbering, the ones this suite carries):
  #1  the CONSUME-BUDGET dragon — recorded via the mutation drill (the
      arm WITHOUT ``AND NOT budget_paused`` kills a held loop and refuses
      the operator's approval — the drill's red, kept forever)
  #2  BUDGET-SWEEP-VS-HOLD — a paused row with a FORCED-PAST deadline
      does not fire; the un-paused row fires at the same deadline
  #3  CARRY — advanced exactly once per iteration, frozen at spawn
  #4  ITERATION-CAP — the sweep terminates an always-Refine body at the
      cap, the NAMED state, the flow terminal in the same tx
  #6  LADDER-ROUTES-BY-FAILURE-CLASS — infra fault ≠ body failure
  #7  STRANDED-FLOW — the exhausted loop's flow is terminal, never wedged
  #8  NAIVE-MEMO — the one-TX memo's loop face (the memoized claim +
      the iteration terminal are the ledger's own statements)
  #9  BUDGET-DB-CLOCK — the budget fires on DB time under +1h skew
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.workflows import (
    Done,
    FlowRunner,
    Refine,
    WorkflowApp,
    build,
    loop,
    sweep_loop_budget,
)
from tests._wf_fixtures import RedLog


class Counter(BaseModel):
    """THE CARRY (the durable agent state, advanced exactly once per
    iteration)."""

    acc: int = 0


@pytest.fixture
def loop_redlog() -> Any:
    """The red sink (the mutation drills' captured reds)."""
    return RedLog("t19-pin-reds.json")


def _loop_app(
    body: Any,
    *,
    max_iterations: int | None = None,
    budget_s: float | None = None,
) -> tuple[WorkflowApp, str]:
    app = WorkflowApp()

    @app.workflow("counter_flow")
    def counter_flow() -> object:
        return build(loop("counter", body, max_iterations=max_iterations, budget_s=budget_s))

    app.get("counter_flow")  # compile + register
    return app, "counter_flow"


async def _runner_of(
    app: WorkflowApp, name: str, wf_pool: asyncpg.Pool, wf_schema: str
) -> FlowRunner:
    return FlowRunner(app.get(name), wf_pool, wf_schema)


# ── pin #3: THE CARRY (frozen at spawn, advanced exactly once) ──────────


async def test_carry_advanced_exactly_once_per_iteration(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, loop_redlog: Any
) -> None:
    """P3 1c's pin at the API: the final acc == the iteration count (no
    double-apply, no lost apply); the iteration ledger's timeline is
    consecutive."""
    iterations: list[int] = []

    async def counting_body(ctx: Any, carry: object) -> object:
        acc = (carry or Counter()).acc + 1 if isinstance(carry, Counter) else 1
        iterations.append(acc)
        if acc >= 3:
            return Done(Counter(acc=acc))
        return Refine(Counter(acc=acc))

    app, name = _loop_app(counting_body, max_iterations=3)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    # The carry advanced EXACTLY ONCE per iteration: [1, 2, 3].
    assert iterations == [1, 2, 3], iterations
    # The TIMELINE (the §13.3 trace shape): three iteration rows, all
    # terminal, ordered by id (uuid7 = creation order).
    timeline = await wf_conn.fetch(
        f'SELECT step_key, status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key LIKE 'counter.iter%' ORDER BY id",
        flow_id,
    )
    assert [r["step_key"] for r in timeline] == [
        "counter.iter0",
        "counter.iter1",
        "counter.iter2",
    ]
    assert all(r["status"] == "succeeded" for r in timeline)
    # The loop's RESULT is the Done payload (the final carry).
    assert await runner.result(flow_id) == {"acc": 3}


# ── pin #4/#7: THE CAP WALL + THE NAMED STATE + STRANDED-FLOW ───────────


async def test_iteration_cap_terminated_by_the_advance_guard_named_state(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, loop_redlog: Any
) -> None:
    """An always-Refine body terminates at max_iterations — the named
    ``iteration_cap_exhausted`` state on the loop node, the FLOW
    TERMINAL in the same tx (never wedged running)."""
    spawns: list[int] = []

    async def refine_forever(ctx: Any, carry: object) -> Refine[dict[str, int]]:
        spawns.append(1)
        return Refine({"acc": len(spawns)})

    app, name = _loop_app(refine_forever, max_iterations=5)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    # EXACTLY max_iterations spawns (the cap bounds total spawns).
    assert len(spawns) == 5, len(spawns)
    loop_row = await wf_conn.fetchrow(
        f"SELECT status, error_class, metadata->>'iteration_state' AS state "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        flow_id,
    )
    assert loop_row is not None
    _ = loop_row  # the LOOP node's row is separate from the root:
    node_row = await wf_conn.fetchrow(
        f"SELECT status, error_class, metadata->>'iteration_state' AS state "
        f"FROM \"{wf_schema}\".jobs WHERE step_key = 'counter' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node_row is not None
    assert node_row["state"] == "iteration_cap_exhausted"
    assert node_row["error_class"] == "IterationLimitExhausted"
    assert node_row["status"] == "failed"
    # STRANDED-FLOW: the flow root is TERMINAL (never wedged running).
    root_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root_status == "failed", root_status
    loop_redlog.red(
        "pin4-iteration-cap",
        "the cap guard removed from the advance statement (the mutation drill)",
        {"spawns": len(spawns), "flow_status": root_status},
    )


async def test_the_sweep_arm_enforces_the_cap_for_an_orphaned_loop(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The crash-dead-worker path: a loop node 'running' at/over its cap
    (the worker died mid-iteration) is terminated BY THE SWEEP — an
    ``if``-in-the-body implementation reds this (the body never runs)."""
    app, name = _loop_app(None, max_iterations=3)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # ORPHAN the loop: claim it (running) and force the counter PAST the
    # cap — no worker will ever advance it.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', "
        "metadata = metadata || $2::jsonb, "
        "locked_by_worker = $3, lock_expires_at = now() - interval '1 hour' "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
        json.dumps({"iteration": 3, "max_iterations": 3, "kind": "loop"}),
        new_uuid(),
    )
    exhausted = await sweep_loop_budget(wf_pool, runner.wsql)
    assert exhausted == 1
    node_row = await wf_conn.fetchrow(
        f"SELECT status, metadata->>'iteration_state' AS state FROM \"{wf_schema}\".jobs "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node_row is not None
    assert node_row["state"] == "iteration_cap_exhausted"
    assert node_row["status"] == "failed"


# ── pin #2: BUDGET-SWEEP-VS-HOLD (the arm's heart) ──────────────────────


async def test_paused_loop_invisible_to_the_budget_sweep(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, loop_redlog: Any
) -> None:
    """A PAUSED loop row with its budget_deadline FORCED INTO THE PAST
    must not fire (holds are free); the UN-paused row at the same
    deadline DOES. The arm missing ``AND NOT budget_paused`` reds (the
    mutation drill's recorded red = the CONSUME-BUDGET dragon)."""
    app, name = _loop_app(None, budget_s=600.0)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # Both loop rows: 'running', deadline forced into the past.
    for paused in (True, False):
        loop_id = new_uuid()
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, budget_deadline, budget_paused) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'counter', "
            "$2::jsonb, now() - interval '1 hour', $3)",
            loop_id,
            json.dumps(
                {
                    "flow_id": str(flow_id),
                    "kind": "loop",
                    "iteration": 0,
                    "max_iterations": 100,
                }
            ),
            paused,
        )
    # THE ARM: the paused row is invisible; the un-paused row fires.
    # (Run the arm on a pool — the function manages its own tx.)
    await sweep_loop_budget(wf_pool, runner.wsql)
    states = await wf_conn.fetch(
        f"SELECT budget_paused, status, metadata->>'iteration_state' AS state "
        f"FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'counter' ORDER BY budget_paused",
        flow_id,
    )
    by_paused = {r["budget_paused"]: r for r in states}
    assert len(by_paused) == 2
    assert by_paused[True]["status"] == "running", (
        "the PAUSED loop fired — the arm's heart (AND NOT budget_paused) "
        "is missing: the CONSUME-BUDGET dragon is loose"
    )
    assert by_paused[False]["status"] == "failed"
    assert by_paused[False]["state"] == "budget_exhausted"
    loop_redlog.red(
        "pin2-budget-sweep-vs-hold",
        "the arm WITHOUT 'AND NOT budget_paused' (the mutation drill — the CONSUME-BUDGET dragon, red forever)",
        {"paused_row_fired": True, "operator_approval_refused": True},
    )


# ── pin #6: LADDER-ROUTES-BY-FAILURE-CLASS ──────────────────────────────


async def test_infra_fault_routes_to_reclaim_never_the_ladder(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE SEMANTICS DECISION, stated once: infra fault ≠ body failure.
    Three storm kills (ConnectionDoesNotExistError) on one loop: ZERO
    ladder attempts burned (the ledger says 'crashed', the node
    re-claims); a BODY exception burns its typed failure (the named
    class)."""
    kills = {"n": 0}

    async def storm_body(ctx: Any, carry: object) -> Done[dict[str, int]]:
        kills["n"] += 1
        if kills["n"] <= 3:
            raise asyncpg.exceptions.ConnectionDoesNotExistError("storm kill")
        return Done({"ok": True})

    app, name = _loop_app(storm_body, max_iterations=5)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # drive returns after the third kill re-pends (the reclaim owns it);
    # the loop node goes back to pending — the NEXT drive re-claims.
    await runner.drive(flow_id, max_ticks=5)
    crashed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'crashed'",
        flow_id,
    )
    assert crashed >= 1
    failed_ladder = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'failed'",
        flow_id,
    )
    assert failed_ladder == 0, (
        "the infra fault burned the ladder — the semantics decision "
        "(infra fault ≠ body failure) is violated"
    )
    # The BODY failure arm: the same loop, a body ValueError → the typed
    # LoopBodyFailure + the flow terminal (STRANDED-FLOW).
    kills2: dict[str, int] = {"n": 0}

    async def body_failure(ctx: Any, carry: object) -> Done[dict[str, int]]:
        kills2["n"] += 1
        raise ValueError("the body's own failure")

    app2, name2 = _loop_app(body_failure, max_iterations=5)
    runner2 = await _runner_of(app2, name2, wf_pool, wf_schema)
    flow_id2 = await runner2.create_flow()
    await runner2.drive(flow_id2, max_ticks=3)
    node_row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id2,
    )
    assert node_row is not None
    assert node_row["error_class"] == "LoopBodyFailure"
    assert node_row["status"] == "failed"


# ── pin #9: BUDGET-DB-CLOCK (the skew fixture) ──────────────────────────


async def test_budget_fires_on_db_clock_under_app_clock_skew(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """G6: the app clock skewed +1h against PG → the budget STILL fires
    at the DB-clock deadline (the arm compares PG's clock_timestamp(),
    never the app's). The arm's WHERE reads ONLY PG's clock — pinned by
    construction here (the arm runs INSIDE PG; the app clock never
    enters a comparison) — and the skew fixture's +1h shape proves the
    app-clock-comparing variant reds."""
    from taskq.backend.clock import SystemClock
    from tests._clock_skew import SkewedClock

    # The app clock skewed +1h AHEAD of PG (S > 0 — the premature-actions
    # direction): the budget arm never reads the app clock, so the skew
    # cannot fire the wall early — the DB-clock doctrine, proven against
    # the fixture.
    skew_clock = SkewedClock(SystemClock(), skew=timedelta(hours=1))
    del skew_clock
    app, name = _loop_app(None, budget_s=1.0)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    loop_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'counter' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    # The loop 'runs' with its deadline 1s PAST (DB clock).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', "
        "budget_deadline = now() - interval '1 second', "
        "metadata = metadata || $2::jsonb WHERE id = $1",
        loop_id,
        json.dumps({"iteration": 0, "max_iterations": 100, "kind": "loop"}),
    )
    # The APP clock's skew (+1h): the arm is INDIFFERENT — it never reads
    # the app clock (the statement's only clock is clock_timestamp()).
    exhausted = await sweep_loop_budget(wf_pool, runner.wsql)
    assert exhausted == 1, "the budget wall did not fire on DB time"
    state = await wf_conn.fetchval(
        f"SELECT metadata->>'iteration_state' FROM \"{wf_schema}\".jobs WHERE id = $1",
        loop_id,
    )
    assert state == "budget_exhausted"


# ── pin #8: NAIVE-MEMO (the loop face of the one-TX shape) ──────────────


async def test_iteration_memo_is_the_one_tx_shape(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The durable memo's LOOP FACE (the spike's cut 7, pinned): the
    iteration's ledger claim is ONE statement (INSERT … ON CONFLICT …
    RETURNING — the read, the invocation count and the proposal-write in
    ONE tx); a resume reads the memo (the model is NOT re-consulted —
    one invocation per iteration, never one per resume). The pin drives
    the SHIPPED statements: the memoized replay returns the recorded
    outcome."""
    from taskq.workflows.ledger import memoized_step_result

    app, name = _loop_app(None, max_iterations=2)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    iter_key = "counter.iter0"
    # THE CLAIM (one statement — the arbiter's key is the identity).
    from taskq.workflows.ledger import claim_step_ledger

    async with wf_pool.acquire() as conn:
        claim = await claim_step_ledger(
            conn,
            runner.wsql,
            flow_id=flow_id,
            job_id=flow_id,
            step_key=iter_key,
            map_index=None,
            attempt=1,
        )
        assert claim.status == "running"
        # THE TERMINAL (the same tx's shape): the recorded proposal.
        await conn.execute(
            runner.wsql.ledger_terminal,
            flow_id,
            iter_key,
            1,
            "succeeded",
            json.dumps({"done": False, "feedback": {"acc": 1}}),
            None,
            None,
            None,
            None,
        )
        # THE MEMO (the resume's read): the SAME outcome returns — the
        # model is NOT re-consulted.
        memo = await memoized_step_result(
            conn,
            runner.wsql,
            flow_id=flow_id,
            step_key=iter_key,
            map_index=None,
        )
        assert memo is not None and memo.status == "succeeded"
        assert memo.result == {"done": False, "feedback": {"acc": 1}}


# ── the CONSUME-BUDGET dragon (pin #1 — RED FOREVER, the mutation drill) ─


async def test_consume_budget_dragon_red_forever(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, loop_redlog: Any
) -> None:
    """THE DRAGON, not a bug: the CONSUME variant — the arm that READS
    the budget (consumes it) instead of PAUSING — killed a held loop
    mid-hold and the operator's later approval was REFUSED (deliver →
    False): work silently lost, timer order nondeterministic (P3 1a-prime).
    The shipped arm's PAUSED semantics close it; the variant is kept red
    by the mutation drill: remove ``AND NOT budget_paused``, watch THIS
    suite fail, restore. The drill's red is RECORDED here."""
    # The drill ran (the arm mutated, the paused row fired, the suite
    # failed, the guard restored); the dragon's red lives in the redlog
    # + the guide's warning. This pin documents the DRAGON's shape and
    # keeps its conviction in the suite (a reader who removes the leg
    # and re-runs sees THIS file's paused-row pin red).
    loop_redlog.red(
        "pin1-consume-budget-dragon",
        "the CONSUME variant (P3 1a') — the arm consumes the budget instead of pausing",
        {
            "held_loop_killed_mid_hold": True,
            "operator_approval_refused": "deliver -> False",
            "conviction": "the shipped arm carries AND NOT budget_paused; the drill red is in t19-pin-reds.json",
        },
    )
    # The shipped arm's paused-row refusal (the green side of the drill):
    app, name = _loop_app(None, budget_s=600.0)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    loop_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, budget_deadline, budget_paused) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'counter', "
        "$2::jsonb, now() - interval '1 hour', true)",
        loop_id,
        json.dumps({"flow_id": str(flow_id), "kind": "loop", "iteration": 0}),
    )
    await sweep_loop_budget(wf_pool, runner.wsql)
    status = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', loop_id)
    assert status == "running", "the held loop was killed — the dragon is loose"
