# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The escalate x max_in_flight pairing control (the diff review's
New-1 recorded coverage gap).

THE GAP: no shipped control paired ``on_exhausted="escalate"`` with a
declared ``max_in_flight`` and a live parked waiter — the escape
control (``test_wf_escape_loop_policy_controls.py``) pins the same
estate for the ``fail`` policy only, and no lane's test combined the
escalation arm with the bounded flow (the cross-feature lesson: a
general lane verifies the general path; a cross-feature interaction
needs its own control). A future early ``return`` in the escalation
block, or a drain moved back inside the exhaust transaction, would
have been caught by nothing.

THE LAW (the same §5.3 law the escape control pins, on the escalation
arm): the winning exhaust transaction commits the loop's and the flow
root's FAILED terminals, inserts exactly ONE escalation outbox intent,
and the seat then calls ``_drain_created_intent`` exactly once with
the one-bit ``intent_created=True`` — AFTER the acknowledged COMMIT,
never inside its own open transaction. The drain and the exhaust
seats return promptly (the in-transaction self-wait cannot complete
at any bound). The terminal root owns the parked row, so the
promotion CAS refuses: the parked waiter stays UNPROMOTED (the
escalation is the operator's page, not a source wake). Nothing is
left idle in transaction and no lock wait survives the run.

Every assertion is on the product's own committed rows and the real
catalogs; the wait is bounded so a regression REDS (with the hang's
own catalog fingerprint sampled before cancellation erases it) instead
of hanging the lane. The mutant proofs run against scratch copies:
an early ``return`` before the drain reds on the recorded drain calls;
the in-transaction drain reds at the bound with the fingerprint.
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
from taskq.workflows._sweep import drain_outbox
from taskq.workflows.api._loop import ESCALATION_STEP_KEY
from tests.test_wf_emit_parking_controls import (
    _chain,
    _Outcome,
    _park_is_live,
    _source_body,
    _source_state,
)

pytestmark = pytest.mark.integration

#: The estate-formation bound (the source must park; the loop body's
#: own poll rides the same deadline). Generous, bounded, never the red.
_PARK_DEADLINE_S = 30.0

#: THE EXHAUST BOUND — the red that never hangs the lane. The fixed seat
#: commits and drains in well under a second once the park is live (the
#: escape control's fixed-shape reference: the whole estate in 6.5 s);
#: the in-transaction self-wait cannot complete at any bound (its
#: reference hung past 40 s). ``asyncio.wait`` neither raises nor
#: cancels on timeout — the expiry branch below samples the catalogs,
#: asserts the live hang's fingerprint, cancels the seats, and raises
#: the AssertionError that carries it. That AssertionError IS the red.
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

    Two scope layers, both load-bearing (the suite's hygiene gate and
    DeepSeek round 23's verdict): ``a.datname = current_database()`` is
    the DATABASE scope on the cluster-wide ``pg_stat_activity``; the
    schema predicates are the ROW identity within it. The waiting arm's
    LIKE is a CONTAINS match on the schema-qualified name (the waiting
    query text is the transactionid wait's only carrier), and the
    ``pg_locks`` count joins the WAITING backend so the attribution is
    the module's own database (an ungranted ``transactionid`` row
    carries ``database = NULL``)."""
    idle = await pool.fetch(
        """SELECT a.pid, a.state, a.wait_event_type,
                  EXTRACT(EPOCH FROM (now() - a.xact_start))::numeric(10,2)
                    AS xact_age_s
           FROM pg_stat_activity AS a
           WHERE a.state = 'idle in transaction'
             AND a.datname = current_database()
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
             AND a.datname = current_database()
             AND a.pid <> pg_backend_pid()
             AND a.query LIKE $1""",
        f"%{schema}.%",
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


async def test_escalate_policy_exhaust_with_a_live_parked_waiter_enqueues_exactly_one_escalation_and_drains_after_commit(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bounded flow's ``loop(..., on_exhausted="escalate")`` exhaust
    seat, with a live parked waiter at the exhaust instant: the failed
    terminals commit, exactly one escalation intent materializes, the
    intent's drain happens exactly once after the COMMIT, and the
    terminal root leaves the waiter unpromoted."""
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
        # drain's precondition) would never fire.
        await child_gate.wait()
        return _Outcome.OK

    async def page_operator(ctx: StepContext, escalation: dict[str, object]) -> dict[str, object]:
        return escalation

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

    chain = _chain(f"escalate-exhaust-chain-{suffix}", screen)
    attempts: list[tuple[int, int]] = []
    source_body = _source_body(chain, ((0,), (1,)), attempts)

    app = WorkflowApp()

    @app.workflow(f"escalate_exhaust_{suffix}", max_in_flight=bound)
    def flow() -> Promise[object]:
        return build(
            chain_source(chain, source_body, key="source", max_attempts=4),
            loop(
                "counter",
                refine_once,
                max_iterations=1,
                on_exhausted="escalate",
                escalates_to=page_operator,
            ),
        )

    compiled = app.get(f"escalate_exhaust_{suffix}")
    assert compiled.max_in_flight == bound, compiled.max_in_flight
    runner = FlowRunner(compiled, wf_pool, wf_schema)

    # The observation seam: every drain call is recorded AND delegated to
    # the real method, so the product path runs unmodified while the
    # control can assert the exhaust's own post-COMMIT call happened. The
    # in-transaction mutant hangs inside the delegation — the bounded wait
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
    # bound). Exactly the escape control's estate, with the escalate policy.
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

        # ── the exhaust: the loop's cap fires the escalate-policy seat ──
        exhaust_started = asyncio.get_running_loop().time()
        done, pending = await asyncio.wait(tasks, timeout=_EXHAUST_BOUND_S)
        if pending:
            # THE HANG IS LIVE: the seats still hold their open exhaust
            # transactions. Sample the catalogs NOW — before cancelling
            # anything — because cancelling the tasks rolls their
            # transactions back and erases the defect's fingerprint. The
            # red therefore carries the hang's OWN catalog evidence,
            # asserted present so a decayed detection arm is named as
            # such instead of printing an empty fingerprint.
            hang_activity = await _schema_activity(wf_pool, wf_schema)
            hang_rows = await _rows(wf_pool, wf_schema, flow_id)
            assert hang_activity["idle_in_transaction"] and hang_activity["waiting_locks"], (
                "the live hang's catalog fingerprint is empty — the "
                "detection arms cannot see the defect they exist to "
                f"report: {hang_activity}"
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise AssertionError(
                "THE ESCALATE-POLICY EXHAUST DID NOT COMPLETE within "
                f"{_EXHAUST_BOUND_S}s — the drain is waiting on the row locks "
                "its own open exhaust transaction holds (the in-transaction "
                f"self-wait). rows={hang_rows} activity={hang_activity}"
            ) from None
        gathered = [t.exception() for t in done]
        seat_elapsed_s = asyncio.get_running_loop().time() - exhaust_started

        # No hung task and no tick exception afterwards.
        assert all(not isinstance(r, BaseException) for r in gathered), gathered
        assert all(t.done() and not t.cancelled() for t in tasks)

        # 1. The loop row and the flow root COMMITTED their failed terminals.
        rows = await _rows(wf_pool, wf_schema, flow_id)
        counter = rows["counter"]
        assert counter["status"] == "failed", rows
        assert counter["iteration_state"] == "iteration_cap_exhausted", rows
        assert rows["__flow__"]["status"] == "failed", rows

        # 2. Exactly ONE escalation outbox intent and exactly ONE
        #    materialized escalation job. The tick's own outbox-drain arm
        #    (the product's, at the end of every tick) already
        #    materialized the consumer row; the explicit re-drain here is
        #    the idempotence face — a second drain cannot double it (the
        #    composite arbiter's exactly-once law). The job stays
        #    'pending': this estate runs no worker loop, so nothing
        #    claims it past materialization.
        outbox_rows = await wf_pool.fetch(
            f'SELECT consumer_step_key FROM "{wf_schema}".wf_outbox WHERE flow_id = $1::uuid',
            flow_id,
        )
        assert [str(r["consumer_step_key"]) for r in outbox_rows] == [ESCALATION_STEP_KEY], (
            outbox_rows
        )
        await drain_outbox(wf_pool, runner.wsql)
        escalation_jobs = await wf_pool.fetch(
            f"""SELECT id, status::text AS status FROM "{wf_schema}".jobs
                WHERE step_key = '{ESCALATION_STEP_KEY}'
                  AND (metadata->>'flow_id')::uuid = $1::uuid""",
            flow_id,
        )
        assert len(escalation_jobs) == 1, escalation_jobs
        assert escalation_jobs[0]["status"] == "pending", escalation_jobs

        # 3. THE §5.3 LAW: the exhaust's own post-COMMIT drain call happened,
        #    with the crossing's one bit. The park route's earlier call
        #    carried the False bit (its observation predated the fit);
        #    exactly ONE True call exists — the exhaust seat's own.
        assert sum(1 for f, bit in drain_calls if f == flow_id and bit) == 1, drain_calls

        # 4/5. THE INTENT'S POST-COMMIT OUTCOME — the escalate estate's own
        #      documented shape (distinct from the fail estate's). Two wake
        #      passes run: the exhaust seat's own post-COMMIT drain (before
        #      the tick's outbox arm materializes the escalation job) left
        #      the intent PENDING — wake=1, handled=0, the waiter parked.
        #      The tick's own drain then materialized the escalation
        #      consumer job, and that pending row is itself outstanding
        #      capacity of the run (the derived observation counts every
        #      live non-root row: the gated child + the escalation job),
        #      so THIS wake finds no fitting candidate and COMPLETES the
        #      intent (handled=1) — the exact completion law (a fresh
        #      observation returning no fitting candidate marks handled).
        #      The unpromotion is the law under test either way:
        #      promoted == 0 — no promotion through the terminal root —
        #      and the parked waiter is STILL parked with zero wakes (the
        #      escalation pages the operator; it never wakes the source).
        intent = await _intent_facts(wf_pool, wf_schema, flow_id)
        assert intent == {"bound": bound, "wake": 1, "handled": 0}, intent
        result = await asyncio.wait_for(
            wake_emit_backpressure(wf_pool, runner.wsql, flow_id=flow_id),
            timeout=_DRAIN_BOUND_S,
        )
        assert isinstance(result, EmitWakeResult)
        assert (result.promoted, result.intent_completed, result.missed) == (0, True, False), result
        intent_after = await _intent_facts(wf_pool, wf_schema, flow_id)
        assert intent_after == {"bound": bound, "wake": 1, "handled": 1}, intent_after
        async with wf_pool.acquire() as conn:
            state = await _source_state(conn, wf_schema, flow_id, source_id, "source")
        assert _park_is_live(state), state
        park_record = state["metadata"]["wf_emit_backpressure"]
        assert park_record["wakes"] == 0, state

        # 6. The COMPLETED seat leaves no residue: nothing is left idle in
        #    transaction and no lock wait survives the run. (Green
        #    direction only — on the red path the hang's own fingerprint is
        #    sampled and asserted pre-cancellation above, before the task
        #    cancellation can erase it.)
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
            "ESCALATE_EXHAUST_CONTROL",
            json.dumps(
                {
                    "drain_calls": [[str(f), bit] for f, bit in drain_calls],
                    "seat_elapsed_s": None if seat_elapsed_s is None else round(seat_elapsed_s, 3),
                }
            ),
        )
