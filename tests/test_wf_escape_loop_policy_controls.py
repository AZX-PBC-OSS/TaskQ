# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The fail-policy loop exhaust's post-COMMIT drain law (the diff review's
N-1 stop-ship + its N-2 settlement).

THE LAW (CONTRACT_718 §5.3): a terminal route "uses the one-bit
``intent_created`` result to call the same drain once **after commit**" —
and RULING_718_ROUND3:237 pins the same shape ("with the terminal commit
acknowledged **before** drain"). The loop's exhaust seat is one of those
terminal routes, so its fail-policy arm must drain the intent it created
only after the exhaust transaction's acknowledged COMMIT.

THE DEFECT (Qwen's executed reproduction at head ``b58f88e2d``): the
fail-policy arm called ``_drain_created_intent`` INSIDE its own open
``async with self.pool.acquire() as conn, conn.transaction():`` block.
``emit_capacity_event`` had already taken the intent row's ``FOR UPDATE``
lock there, and ``LOOP_EXHAUST_SQL``'s ``flow_terminal`` leg had taken the
root row's — while ``_drain_created_intent`` acquires a SECOND pool
connection whose canonical order queues on exactly those two row locks.
The holder is idle-in-transaction (a client wait, not a lock wait), so
PostgreSQL's deadlock detector sees no cycle, no ``statement_timeout`` or
``lock_timeout`` applies, and the seat hangs unbounded. Qwen's shipped
repro: the tick still hung at 40 s, one pid ``idle in transaction`` with
``xact_age_s=40.07`` and another pid waiting (``granted=false``) on
``transactionid`` for ``EMIT_ROOT_LOCK_SQL`` — while nothing had committed.
The same estate with the drain post-COMMIT: the seat returns and commits
its terminals in well under a second.

This control pins the fixed law on real PostgreSQL end to end, fails
closed on a hang (a bounded wait that REDS — it never hangs the lane), and
asserts the exhaust's post-commit outcome is exactly the documented §5.4
shape: the terminal root owns the parked row, so the promotion CAS refuses
and the same primitive, called post-COMMIT, returns
``EmitWakeResult(promoted=0, intent_completed=False, missed=True)``.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    loop,
)
from taskq.workflows._emit import EmitWakeResult, wake_emit_backpressure
from tests.test_wf_emit_parking_controls import (
    _chain,
    _Outcome,
    _park_is_live,
    _source_body,
    _source_state,
)

pytestmark = pytest.mark.integration

#: The estate-formation bound (the source must park; the loop body's own
#: poll rides the same deadline). Generous, bounded, never the test's red.
_PARK_DEADLINE_S = 30.0

#: THE EXHAUST BOUND — the red that never hangs the lane. The fixed seat
#: commits its terminals in well under a second once the park is live
#: (Qwen's fixed-shape run: the whole estate finished in 6.5 s); the
#: in-transaction self-wait cannot complete at any bound — Qwen's shipped
#: reference hung past 40 s — so a TimeoutError here IS the defect's red.
_EXHAUST_BOUND_S = 20.0

#: The post-commit primitive's own bound (the same fail-closed rule).
_DRAIN_BOUND_S = 20.0


async def _rows(pool: asyncpg.Pool, schema: str, flow_id: JobId) -> dict[str, dict[str, Any]]:
    """The run's rows keyed by step key (the terminal/state facts)."""
    rows = await pool.fetch(
        f"""SELECT step_key, status::text AS status, attempt,
                   metadata->>'iteration_state' AS iteration_state,
                   metadata->>'blocking_reason' AS blocking_reason
            FROM "{schema}".jobs
            WHERE id = $1::uuid OR (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    return {str(r["step_key"]): dict(r) for r in rows}


async def _intent_facts(pool: asyncpg.Pool, schema: str, flow_id: JobId) -> dict[str, int]:
    """The run's capacity/intent generations (empty when never stamped)."""
    row = await pool.fetchrow(
        f"SELECT bound, wake_generation::bigint AS wake, "
        f"handled_generation::bigint AS handled "
        f'FROM "{schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow_id,
    )
    if row is None:
        return {}
    return {"bound": int(row["bound"]), "wake": int(row["wake"]), "handled": int(row["handled"])}


async def _schema_activity(pool: asyncpg.Pool, schema: str) -> dict[str, list[dict[str, Any]]]:
    """This schema's own lock/transaction account from the real catalogs.

    Scoped to THIS schema so a parallel xdist worker's unrelated
    transactions cannot flake the assertion: an idle-in-transaction pid
    counts only when it holds a lock on one of this schema's relations,
    and a waiting lock counts only when the waiting query names this
    schema (the transactionid wait the defect produces has no relation of
    its own — the waiting query text is its carrier)."""
    idle = await pool.fetch(
        """SELECT a.pid, a.state, a.wait_event_type,
                  EXTRACT(EPOCH FROM (now() - a.xact_start))::numeric(10,2)
                    AS xact_age_s
           FROM pg_stat_activity AS a
           WHERE a.state = 'idle in transaction'
             AND a.pid <> pg_backend_pid()
             AND EXISTS (
                 SELECT 1 FROM pg_locks AS l
                 WHERE l.pid = a.pid
                   AND l.relation::regclass::text LIKE $1
             )""",
        f"{schema}.%",
    )
    waiting = await pool.fetch(
        """SELECT a.pid, a.wait_event_type, a.wait_event,
                  left(a.query, 300) AS query, l.locktype, l.mode, l.granted
           FROM pg_locks AS l
           JOIN pg_stat_activity AS a ON a.pid = l.pid
           WHERE NOT l.granted
             AND a.pid <> pg_backend_pid()
             AND a.query LIKE $1""",
        f"{schema}.%",
    )
    return {
        "idle_in_transaction": [dict(r) for r in idle],
        "waiting_locks": [dict(r) for r in waiting],
    }


async def _park_live(pool: asyncpg.Pool, schema: str, flow_id: JobId, source_id: JobId) -> bool:
    """The product's own park representation, read on this helper's own conn."""
    async with pool.acquire() as conn:
        state = await _source_state(conn, schema, flow_id, source_id, "source")
    return _park_is_live(state)


async def test_fail_policy_exhaust_drains_after_commit_with_a_live_parked_waiter(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bounded flow's ``loop(..., on_exhausted="fail")`` exhaust seat, with
    a live parked waiter at the exhaust instant, must complete promptly with
    its failed terminals COMMITTED and drain the intent it created only
    after that COMMIT — never inside its own open transaction."""
    suffix = new_uuid().hex[:12]
    bound = 2
    # late-bound once create_flow has produced the identities the loop body
    # polls for (the estate construction order, not a product fact).
    target: list[tuple[JobId, JobId]] = []
    child_gate = asyncio.Event()

    async def screen(ctx: StepContext, item: object) -> _Outcome:
        # The emitted child stays non-terminal through the exhaust instant:
        # a completed child's own terminal event would create and drain the
        # intent BEFORE the loop's exhaust, and the exhaust's crossing (the
        # hang's precondition) would never fire.
        await child_gate.wait()
        return _Outcome.OK

    async def refine_once(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        # The loop's one iteration refines only after the source's REAL park
        # marker is live (the product's own representation, read on this
        # body's own pool acquisitions) — so the exhaust's terminal event
        # observes a live parked waiter and crosses the threshold.
        loop_ = asyncio.get_running_loop()
        deadline = loop_.time() + _PARK_DEADLINE_S
        seen = False
        while loop_.time() < deadline:
            if target and await _park_live(wf_pool, wf_schema, target[0][0], target[0][1]):
                seen = True
                break
            await asyncio.sleep(0.05)
        assert seen, (
            "ESTATE PRECONDITION FAILED — the loop body never saw the "
            "source's real park marker; the exhaust estate did not form"
        )
        return Refine({"n": 1})

    chain = _chain(f"exhaust-drain-chain-{suffix}", screen)
    attempts: list[tuple[int, int]] = []
    source_body = _source_body(chain, ((0,), (1,)), attempts)

    app = WorkflowApp()

    @app.workflow(f"exhaust_drain_{suffix}", max_in_flight=bound)
    def flow() -> Promise[object]:
        return build(
            chain_source(chain, source_body, key="source", max_attempts=4),
            loop("counter", refine_once, max_iterations=1, on_exhausted="fail"),
        )

    compiled = app.get(f"exhaust_drain_{suffix}")
    assert compiled.max_in_flight == bound, compiled.max_in_flight
    runner = FlowRunner(compiled, wf_pool, wf_schema)

    # The observation seam: every drain call is recorded AND delegated to
    # the real method, so the product path runs unmodified while the
    # control can assert the exhaust's own post-COMMIT call happened. The
    # in-transaction defect hangs inside the delegation — the bounded wait
    # below still reds at the bound.
    real_drain = FlowRunner._drain_created_intent
    drain_calls: list[tuple[JobId, bool]] = []

    async def _observing_drain(self: FlowRunner, flow_id: JobId, intent_created: bool) -> None:
        drain_calls.append((flow_id, intent_created))
        await real_drain(self, flow_id, intent_created)

    monkeypatch.setattr(FlowRunner, "_drain_created_intent", _observing_drain)

    flow_id = (await runner.create_flow()).flow_id
    async with wf_pool.acquire() as conn:
        source_row = await conn.fetchrow(
            f'SELECT id FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
            flow_id,
        )
    assert source_row is not None, "the created flow must carry its source node"
    source_id = JobId(source_row["id"])
    target.append((flow_id, source_id))

    # Two concurrent ticks: one seat takes the loop (its body waits for the
    # park), the other takes the source (which parks under the declared
    # bound). Exactly the estate the defect's reproduction used.
    tick_a: asyncio.Task[bool] = asyncio.create_task(runner.tick(flow_id))
    await asyncio.sleep(0.5)
    tick_b: asyncio.Task[bool] = asyncio.create_task(runner.tick(flow_id))
    tasks = [tick_a, tick_b]
    seat_elapsed_s: float | None = None
    try:
        parked = False
        park_deadline = asyncio.get_running_loop().time() + _PARK_DEADLINE_S
        while asyncio.get_running_loop().time() < park_deadline:
            parked = await _park_live(wf_pool, wf_schema, flow_id, source_id)
            if parked or all(t.done() for t in tasks):
                break
            await asyncio.sleep(0.05)
        assert parked, (
            "ESTATE PRECONDITION FAILED — the source never parked under the "
            f"declared bound: {await _rows(wf_pool, wf_schema, flow_id)}"
        )

        # ── the exhaust: the loop's cap fires the fail-policy seat ──
        exhaust_started = asyncio.get_running_loop().time()
        try:
            gathered = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=_EXHAUST_BOUND_S
            )
        except TimeoutError:
            raise AssertionError(
                "THE FAIL-POLICY EXHAUST DID NOT COMPLETE within "
                f"{_EXHAUST_BOUND_S}s — the drain is waiting on the row locks "
                "its own open exhaust transaction holds (the in-transaction "
                f"self-wait). rows={await _rows(wf_pool, wf_schema, flow_id)} "
                f"activity={await _schema_activity(wf_pool, wf_schema)}"
            ) from None
        seat_elapsed_s = asyncio.get_running_loop().time() - exhaust_started

        # No hung task and no tick exception afterwards.
        assert all(not isinstance(r, BaseException) for r in gathered), gathered
        assert all(t.done() and not t.cancelled() for t in tasks)

        # The loop row and the flow root COMMITTED their failed terminals.
        rows = await _rows(wf_pool, wf_schema, flow_id)
        counter = rows["counter"]
        assert counter["status"] == "failed", rows
        assert counter["iteration_state"] == "iteration_cap_exhausted", rows
        assert rows["__flow__"]["status"] == "failed", rows

        # THE §5.3 LAW: the exhaust's own post-COMMIT drain call happened,
        # with the crossing's one bit. The park route's earlier call carried
        # the False bit (its observation predated the fit); exactly ONE True
        # call exists — the exhaust seat's own.
        assert sum(1 for f, bit in drain_calls if f == flow_id and bit) == 1, drain_calls

        # THE INTENT'S POST-COMMIT OUTCOME IS THE DOCUMENTED ONE (§5.4): the
        # exhaust crossed (wake=1), the terminal root owns the parked row, so
        # the promotion CAS refuses — the intent rests pending and the source
        # stays parked with zero wakes.
        intent = await _intent_facts(wf_pool, wf_schema, flow_id)
        assert intent == {"bound": bound, "wake": 1, "handled": 0}, intent
        async with wf_pool.acquire() as conn:
            state = await _source_state(conn, wf_schema, flow_id, source_id, "source")
        assert _park_is_live(state), state
        park_record = state["metadata"]["wf_emit_backpressure"]
        assert park_record["wakes"] == 0, state

        # The SAME primitive, called post-COMMIT (the fixed arm's own shape),
        # RETURNS — bounded — with exactly the documented fence result. The
        # in-transaction call never returns at all.
        result = await asyncio.wait_for(
            wake_emit_backpressure(wf_pool, runner.wsql, flow_id=flow_id),
            timeout=_DRAIN_BOUND_S,
        )
        assert isinstance(result, EmitWakeResult)
        assert (result.promoted, result.intent_completed, result.missed) == (0, False, True), result

        # Nothing is left idle in transaction and no lock wait survives —
        # the defect's catalog fingerprint (an idle-in-transaction holder of
        # this schema's rows plus a transactionid waiter) is absent.
        activity = await _schema_activity(wf_pool, wf_schema)
        assert not activity["idle_in_transaction"], activity
        assert not activity["waiting_locks"], activity
    finally:
        child_gate.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # Evidence of the exercised shape (never an acceptance claim): the
        # recorded drain calls and the seat's elapsed time.
        print(
            "EXHAUST_DRAIN_CONTROL",
            json.dumps(
                {
                    "drain_calls": [[str(f), bit] for f, bit in drain_calls],
                    "seat_elapsed_s": None if seat_elapsed_s is None else round(seat_elapsed_s, 3),
                }
            ),
        )
