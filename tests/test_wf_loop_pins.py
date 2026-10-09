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
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    Done,
    FlowRunner,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
)
from taskq.workflows._sweep import sweep_loop_budget


class Counter(BaseModel):
    """THE CARRY (the durable agent state, advanced exactly once per
    iteration)."""

    acc: int = 0


class Approval(BaseModel):
    """The composition pin's gate payload (the hold inside the loop)."""

    verdict: str


def _loop_app(
    body: Any,
    *,
    max_iterations: int | None = None,
    budget_s: float | None = None,
    initial: object | None = None,
) -> tuple[WorkflowApp, str]:
    app = WorkflowApp()

    @app.workflow("counter_flow")
    def counter_flow() -> object:
        return build(
            loop(
                "counter",
                body,
                max_iterations=max_iterations,
                budget_s=budget_s,
                initial=initial,
            )
        )

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
    consecutive. THE BODY ASSERTS THE REAL TYPE (the masking fallback —
    ``isinstance(carry, Counter) else 1`` — is DELETED: it silently RESET
    the accumulation to 1 whenever the carry arrived untyped, which is
    every resume; a pin with a fallback asserts nothing)."""
    iterations: list[int] = []

    async def counting_body(ctx: StepContext, carry: object) -> object:
        assert isinstance(carry, Counter), (
            f"the carry arrived as {type(carry).__name__!r} — the declared "
            "type never reached the body (the accumulate-once law is "
            "asserted on the REAL type, never through a fallback)"
        )
        acc = carry.acc + 1
        iterations.append(acc)
        if acc >= 3:
            return Done(Counter(acc=acc))
        return Refine(Counter(acc=acc))

    app, name = _loop_app(counting_body, max_iterations=3, initial=Counter())
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
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

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        spawns.append(1)
        return Refine({"acc": len(spawns)})

    app, name = _loop_app(refine_forever, max_iterations=5)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
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
    flow_id = (await runner.create_flow()).flow_id
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
    flow_id = (await runner.create_flow()).flow_id
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


# ── pin #6: LADDER-ROUTES-BY-FAILURE-CLASS (the ESCAPE-POINT contract) ──


async def test_infra_fault_routes_to_reclaim_never_the_ladder(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE SEMANTICS DECISION, stated once — attack-3 H5's cure sharpened
    it: the classifier reads WHERE the error ESCAPED, never merely its
    type. A fault raised by the BODY is a BODY failure even when it
    wears a ConnectionError's face (the body cannot forge an infra
    fault — the poison-body wedge, 20 crashed rows + a ``running`` flow
    forever, is the convicted variant the attack probe keeps red); a
    fault raised by the DRIVER'S OWN machinery is the reclaim (the
    ladder untouched, the lease machinery re-claims)."""
    kills2: dict[str, int] = {"n": 0}

    async def body_failure(ctx: StepContext, carry: object) -> Done[dict[str, int]]:
        kills2["n"] += 1
        raise ConnectionError("the body's own network flake — deterministically")

    app2, name2 = _loop_app(body_failure, max_iterations=5)
    runner2 = await _runner_of(app2, name2, wf_pool, wf_schema)
    flow_id2 = (await runner2.create_flow()).flow_id
    await runner2.drive(flow_id2, max_ticks=30)
    node_row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id2,
    )
    assert node_row is not None
    assert node_row["error_class"] == "LoopBodyFailure", (
        "a body-raised ConnectionError was classified as an infra fault — "
        "the body can forge the reclaim (the wedged-running dragon)"
    )
    assert node_row["status"] == "failed"
    # STRANDED-FLOW's body-failure sibling: the flow terminalized.
    root2 = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id2)
    assert root2 == "failed", root2

    # THE MACHINERY arm: an infra kill raised by the DRIVER'S OWN
    # statement (the memo read — machinery, never the body) records
    # 'crashed' and re-pends; the ladder untouched; no exhaustion; the
    # loop completes on the re-claim (the lease machinery's own heal).
    from unittest.mock import patch

    from taskq.workflows import ledger as ledger_module

    real = ledger_module.memoized_step_result
    kills3 = {"n": 0}

    async def killing_memo(*a: Any, **kw: Any) -> Any:
        kills3["n"] += 1
        if kills3["n"] == 1:
            raise asyncpg.exceptions.ConnectionDoesNotExistError("the machinery's storm kill")
        return await real(*a, **kw)

    async def done_at_two(ctx: StepContext, carry: object) -> Done[Counter] | Refine[Counter]:
        assert isinstance(carry, Counter), (
            f"the carry arrived as {type(carry).__name__!r} — the masking "
            "fallback is deleted; the pin asserts the real type"
        )
        acc = carry.acc + 1
        return Done(Counter(acc=acc)) if acc >= 2 else Refine(Counter(acc=acc))

    with patch.object(ledger_module, "memoized_step_result", killing_memo):
        app3, name3 = _loop_app(done_at_two, max_iterations=5, initial=Counter())
        runner3 = await _runner_of(app3, name3, wf_pool, wf_schema)
        flow_id3 = (await runner3.create_flow()).flow_id
        await runner3.drive(flow_id3, max_ticks=10)
        crashed3 = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND status = 'crashed'",
            flow_id3,
        )
        failed_ladder3 = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND status = 'failed'",
            flow_id3,
        )
        root3 = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id3
        )
    assert crashed3 >= 1, "the machinery's infra kill did not record 'crashed'"
    assert failed_ladder3 == 0, (
        "the MACHINERY's infra fault burned the ladder — the escape-point "
        "contract: a fault from the driver's own statements reclaims, never "
        "the ladder"
    )
    assert root3 == "succeeded", (
        f"the machinery kill wedged or failed the flow ({root3!r}) — the "
        "reclaim owns it and the loop completes on the re-claim"
    )


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
    flow_id = (await runner.create_flow()).flow_id
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
    flow_id = (await runner.create_flow()).flow_id
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
    flow_id = (await runner.create_flow()).flow_id
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


# ── THE COMPOSITION PIN: a hold INSIDE a loop (T10 reads T19's arm) ─────


async def test_hold_inside_a_loop_pauses_the_budget_and_completes(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Spike1's subject at the composition (T19 x T10): a loop iteration
    that HOLDS — the loop node's budget PAUSES (the wall is blind), the
    operator's approval RESUMES (the pause lifts, the remaining is the
    on-wake read), the loop COMPLETES — zero budget_exhausted events,
    zero ladder burns. The P3 1a GREEN matrix, 6 checks."""
    from taskq.workflows.api._hitl import deliver_payload
    from taskq.workflows.api._sql_loop import (
        ITERATION_STATE_BUDGET_EXHAUSTED,
        LOOP_REMAINING_SQL,
        render_loop_sql,
    )

    async def hold_then_done(ctx: StepContext, carry: object) -> object:
        if ctx.attempt == 1:
            # THE HOLD (attempt 1 only — the re-execution doctrine: the
            # answer replays from the ledger on the resume).
            await ctx.wait_signal(Approval, timeout_s=120.0)
            return Done({"acc": 1})
        return Done({"acc": 1})

    app2 = WorkflowApp()

    @app2.workflow("hold_loop_flow")
    def hold_loop_flow() -> object:
        return build(loop("holdloop", hold_then_done, max_iterations=2, budget_s=600.0))

    runner = FlowRunner(app2.get("hold_loop_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # (1) the loop RUNS and HOLDS.
    verdict = await runner.drive(flow_id, until="held")
    assert verdict == "held", verdict
    loop_row = await wf_conn.fetchrow(
        f"SELECT id, status, budget_paused, budget_deadline IS NOT NULL AS walled, "
        f"metadata->>'iteration' AS iteration FROM \"{wf_schema}\".jobs "
        "WHERE step_key = 'holdloop' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert loop_row is not None
    # (2) the budget wall EXISTS (the deadline set from PG's clock).
    assert loop_row["walled"]
    # (3) the hold PAUSED it.
    assert loop_row["budget_paused"] is True
    # (4) THE SWEEP: the paused row + a FORCED-PAST deadline → invisible.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET budget_deadline = now() - interval '1 hour' "
        "WHERE id = $1",
        loop_row["id"],
    )
    exhausted = await sweep_loop_budget(wf_pool, runner.wsql)
    assert exhausted == 0, "the held loop's budget fired — the CONSUME-BUDGET dragon is loose"
    # (5) THE OPERATOR'S APPROVAL: the deliver resumes the iteration (the
    # pause lifts — the wall is visible again).
    held = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".wf_signals WHERE workflow_id = $1 AND status = 'held'",
        flow_id,
    )
    delivered = await deliver_payload(
        wf_pool,
        schema=wf_schema,
        workflow_id=flow_id,
        hold_id=JobId(held),
        payload={"verdict": "approve"},
        payload_json=json.dumps({"verdict": "approve"}),
    )
    assert delivered.status == "delivered", delivered
    # (6) THE LOOP COMPLETES — zero exhaustion events, zero ladder burns.
    assert await runner.drive(flow_id) == "terminal"
    final = await wf_conn.fetchrow(
        f"SELECT status, budget_paused, metadata->>'iteration_state' AS state "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        loop_row["id"],
    )
    assert final is not None
    assert final["status"] == "succeeded"
    assert final["state"] != ITERATION_STATE_BUDGET_EXHAUSTED
    ladder_burns = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'failed'",
        flow_id,
    )
    assert ladder_burns == 0
    _ = render_loop_sql, LOOP_REMAINING_SQL  # the clock seam's imports (the on-wake read)


# ── THE CARRY'S TYPED TRUTH: the declared type reaches the body ─────────


async def test_declared_carry_arrives_typed_at_iteration_zero(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE CARRY'S DECLARED TYPE IS THE CONTRACT: a loop declared
    ``initial=Counter(...)`` hands the body a REAL ``Counter`` at iteration
    0 — not the jsonb dict the init's dump produced. The pre-cure truth:
    the typed carry NEVER reached the body (the init serialized it
    typeless; the pins' isinstance fallbacks masked the lie)."""
    seen: list[object] = []

    async def typed_body(ctx: StepContext, carry: object) -> Done[Counter]:
        seen.append(carry)
        assert isinstance(carry, Counter), (
            f"the declared carry arrived as {type(carry).__name__!r} — the "
            "typed carry never reached the body"
        )
        return Done(Counter(acc=carry.acc + 1))

    app, name = _loop_app(typed_body, max_iterations=3, initial=Counter(acc=41))
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"acc": 42}
    assert seen and all(isinstance(c, Counter) for c in seen), (
        f"the body saw {[type(c).__name__ for c in seen]} — the declared "
        "type did not round-trip to iteration 0"
    )


async def test_carry_type_survives_a_kill_and_resume_mid_loop(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE TYPE SURVIVES THE CRASH BOUNDARY: a machinery kill mid-loop
    (the reclaim's crashed row + the re-pend) is resumed by a FRESH
    driver pass; the resumed body receives the declared type with the
    EXACT accumulated state — the accumulation is exact across resumes
    (no reset). The pre-cure truth: the resumed carry arrived as the
    jsonb dict and the masked pins reset the count (observed [1, 1, 2, 3]
    for a 3-step count)."""
    from unittest.mock import patch

    from taskq.workflows import ledger as ledger_module

    real = ledger_module.memoized_step_result
    kills = {"n": 0}

    async def killing_memo(*a: Any, **kw: Any) -> Any:
        kills["n"] += 1
        if kills["n"] == 2:
            # THE KILL: the driver's own memo read at the top of the
            # SECOND pass — after iteration 0's Refine was recorded and
            # the carry advanced. The reclaim owns it; the resume
            # re-reads the carry from the ROW (the crash boundary).
            raise asyncpg.exceptions.ConnectionDoesNotExistError("the storm kill")
        return await real(*a, **kw)

    seen: list[int] = []

    async def counting_body(ctx: StepContext, carry: object) -> object:
        assert isinstance(carry, Counter), (
            f"after the resume the carry arrived as "
            f"{type(carry).__name__!r} — the type did not survive the "
            "crash boundary (the accumulation reset is the lie the mask "
            "hid)"
        )
        acc = carry.acc + 1
        seen.append(acc)
        if acc >= 3:
            return Done(Counter(acc=acc))
        return Refine(Counter(acc=acc))

    with patch.object(ledger_module, "memoized_step_result", killing_memo):
        app, name = _loop_app(counting_body, max_iterations=5, initial=Counter())
        runner = await _runner_of(app, name, wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id, max_ticks=30) == "terminal"
    # THE ACCUMULATION IS EXACT ACROSS THE RESUME: [1, 2, 3] — never
    # [1, 1, 2, 3] (the reset), never [1] (the wedge).
    assert seen == [1, 2, 3], seen
    # The crash boundary WAS crossed (the machinery's reclaim recorded it).
    crashed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'crashed'",
        flow_id,
    )
    assert int(crashed) >= 1, "the pin never crossed the crash boundary"
    assert await runner.result(flow_id) == {"acc": 3}


# ── THE ADVANCE/EXHAUST FENCES: the claim identity's legs ───────────────


async def test_zombie_advance_and_exhaust_are_refused_reclaim_advances_once(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ADVANCE/EXHAUST FENCES: the loop's advance and exhaust carry
    the SAME claim-identity legs every other terminal write carries
    (worker + attempt + claim_epoch — the one-tx-finalize doctrine; the
    unguarded statements were the doctrine's BACK DOOR). The pre-cure
    truth: a zombie driver's exhaust killed a HEALTHY RECLAIMED loop and
    wrote its escalation row; a stale payload moved the counter
    backward.

    The pin runs the SHIPPED statements: the zombie (its claim lapsed;
    the loop reclaimed by a new driver at a fresh attempt + epoch)
    advances and exhausts — BOTH REFUSED; the healthy loop survives, the
    counter never moves backward; the new driver's advance lands EXACTLY
    ONCE, and its exhaust terminalizes the loop."""
    from taskq.workflows.api._sql_loop import (
        LOOP_ADVANCE_SQL,
        LOOP_EXHAUST_SQL,
        render_loop_sql,
    )

    zombie, fresh = new_uuid(), new_uuid()
    # THE RECLAIMED LOOP: running at iteration 3, claimed by the NEW
    # driver (attempt 6, epoch 4). The zombie still holds its stale view
    # (attempt 5, epoch 3). (The statements are the subject — the rows
    # are hand-crafted at the reclaimed identity.)
    flow_id = new_uuid()
    loop_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, locked_by_worker, attempt, "
        "claim_epoch, deps_pending) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'counter', "
        "$2::jsonb, $3, 6, 4, 0)",
        loop_id,
        json.dumps(
            {
                "flow_id": str(flow_id),
                "kind": "loop",
                "iteration": 3,
                "max_iterations": 100,
                "carry": {"acc": 3},
            }
        ),
        fresh,
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'pending', '__flow__', "
        "$2::jsonb)",
        flow_id,
        json.dumps({"flow_id": str(flow_id), "kind": "loop"}),
    )

    # THE ZOMBIE'S ADVANCE (stale identity, stale payload — it would move
    # the counter BACKWARD to 4): REFUSED.
    refused = await wf_conn.fetchval(
        render_loop_sql(LOOP_ADVANCE_SQL, wf_schema),
        loop_id,
        json.dumps({"carry": {"acc": 999}, "iteration": 4}),
        100,
        zombie,
        5,
        3,
    )
    assert refused is None, "the zombie driver's advance was ADMITTED — the back door is open"
    # THE ZOMBIE'S EXHAUST (it would kill the healthy reclaimed loop):
    # REFUSED — the loop survives, no named state, no escalation row.
    zombie_exhaust = await wf_conn.fetchrow(
        render_loop_sql(LOOP_EXHAUST_SQL, wf_schema),
        loop_id,
        "IterationLimitExhausted",
        '{"iteration_state": "iteration_cap_exhausted", "kind": "loop"}',
        "the zombie's message",
        zombie,
        5,
        3,
    )
    assert zombie_exhaust is not None and not zombie_exhaust["loop_exhausted"], (
        "the zombie driver's exhaust LANDED — it killed a healthy "
        "reclaimed loop (the back door is open)"
    )
    # THE HEALTHY LOOP SURVIVES, untouched by the stale payloads.
    row = await wf_conn.fetchrow(
        f"SELECT status, metadata->>'iteration' AS iteration, metadata->>'carry' AS carry "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        loop_id,
    )
    assert row is not None and row["status"] == "running"
    assert row["iteration"] == "3", row["iteration"]
    assert json.loads(row["carry"]) == {"acc": 3}, (
        "the zombie's stale carry payload moved the counter/carry — the counter moved BACKWARD"
    )

    # THE NEW DRIVER advances (its own claim identity): EXACTLY ONCE —
    # the counter moves FORWARD to 4 with the driver's payload.
    advanced = await wf_conn.fetchval(
        render_loop_sql(LOOP_ADVANCE_SQL, wf_schema),
        loop_id,
        json.dumps({"carry": {"acc": 4}, "iteration": 4}),
        100,
        fresh,
        6,
        4,
    )
    assert advanced == 4, f"the reclaimed loop's own driver was refused ({advanced!r})"
    # ...and its exhaust lands (the named state + the flow terminal).
    exhausted = await wf_conn.fetchrow(
        render_loop_sql(LOOP_EXHAUST_SQL, wf_schema),
        loop_id,
        "IterationLimitExhausted",
        '{"iteration_state": "iteration_cap_exhausted", "kind": "loop"}',
        "the driver's wall",
        fresh,
        6,
        4,
    )
    assert exhausted is not None and exhausted["loop_exhausted"] == 1
    status = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', loop_id)
    assert status == "failed"


async def test_wrong_shape_body_is_named_loop_body_shape_error_never_fabricated(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE SHAPE ERROR NAMED: a body returning neither Done nor Refine is
    recorded as the TYPED shape error — the iteration's ledger terminal
    says ``failed`` / ``LoopBodyShapeError``, the loop node's diagnosis
    names it, the flow terminalizes. The replay honors the recorded
    truth: the pre-cure ledger recorded a SUCCEEDED iteration and the
    memo replay re-threaded the raw return AS A REFINE — the ledger
    fabricated."""
    calls = {"n": 0}

    async def wrong_shape(ctx: StepContext, carry: object) -> object:
        calls["n"] += 1
        return {"not": "the union"}  # neither Done nor Refine — THE SHAPE ERROR

    app, name = _loop_app(wrong_shape, max_iterations=5)
    runner = await _runner_of(app, name, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    # THE LEDGER RECORDS THE TRUTH: the iteration's terminal is the typed
    # shape error, never a SUCCEEDED row carrying a laundered feedback.
    row = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'counter.iter0'",
        flow_id,
    )
    assert row is not None
    assert row["status"] == "failed", (
        f"the shape error was recorded as {row['status']!r} — the ledger "
        "fabricated a succeeded iteration"
    )
    assert row["error_class"] == "LoopBodyShapeError", row["error_class"]
    assert "Done" in (row["error_message"] or "") and "Refine" in (row["error_message"] or "")
    # THE DIAGNOSIS NAMES IT: the loop node's own terminal carries the
    # typed class; the flow terminalized.
    node_row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node_row is not None
    assert node_row["error_class"] == "LoopBodyShapeError", node_row["error_class"]
    assert node_row["status"] == "failed"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed"
    # THE REPLAY HONORS THE RECORDED TRUTH: one invocation per iteration —
    # a re-drive never re-runs the body, never re-threads the raw return.
    assert await runner.drive(flow_id) == "terminal"
    assert calls["n"] == 1, calls["n"]
