"""T26 — THE HITL BROADCAST'S PINS: the transactional NOTIFY legs, the
zero-window backfill, the resolve wake, the expiry fail-close, the
escalation interplay — plus the hostile-review round's P6-P10: the
reconnect reconcile, the concurrent-consumer fan-out, the schema
isolation, the payload-size bound, the resolve-rollback silence.

The red-first evidence: P1's convicted variant (the knock sent OUTSIDE
the transaction — a rolled-back hold-create that still broadcasts) is
captured as the mutation drill through ``hitl_redlog``; the shipped
legs are transactional by construction (``pg_notify`` inside the
caller's tx — PG delivers on commit only). P6/P7/P9 were RED at the
pre-cure head (the reconnect hole, the partitioned stream, the unnamed
cap — the reds captured in ``.measurements/t26-cure-pins-red.txt``);
P8/P10 convict their variant as a PERMANENT drill (the unfiltered
channel ear; the lying resolve knock — reproducible forever, recorded
through ``hitl_redlog``).

The channels are GLOBAL; the schema rides the payload (the
schema-per-module estate shares one database across many schemas —
``pg_notify`` is per-database; the listener filters by payload). The
legacy ``taskq_wf_holds`` pointer knob keeps its pinned shape (T10's
pin 12); the broadcast legs ride their own channels.
"""

from __future__ import annotations

import asyncio
import json
import uuid as uuid_module
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.exceptions import SignalTimeoutError
from taskq.testing.assertions import wait_for_condition
from taskq.workflows import (
    Done,
    Expired,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    step,
)
from taskq.workflows.api import GateDecl
from taskq.workflows.api._hitl import (
    HOLD_CREATED_CHANNEL,
    HOLD_RESOLVED_CHANNEL,
    HitlClient,
    register_hold,
    sweep_expired_signals,
)
from taskq.workflows.api._hitl_listen import (
    Backfilled,
    HitlListener,
    HoldCreated,
    HoldExpired,
    HoldResolved,
)
from tests._wf_fixtures import RedLog


class Approval(BaseModel):
    verdict: str
    note: str = ""


class ContinueApproval(BaseModel):
    approved: bool
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


async def _held_flow(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    wait_body: Any,
    name: str,
) -> tuple[JobId, FlowRunner, JobId]:
    """A flow whose single node HOLDS: created, driven to the hold
    (T10's helper's shape; ``name`` is UNIQUE PER TEST — D1)."""
    app = WorkflowApp()

    @app.workflow(name)
    def hold_flow() -> Promise[object]:
        return build(step(wait_body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    node_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'review' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    return flow_id, runner, JobId(node_id)


class _RawKnocks:
    """A raw pg_notify consumer on the MODULE's database (pin 12's
    vacuous-audit cure): collects (channel, payload) pairs."""

    def __init__(self, dsn: str, *channels: str) -> None:
        self._dsn = dsn
        self._channels = channels
        self.knocks: list[tuple[str, str]] = []
        self._conn: asyncpg.Connection | None = None

    async def __aenter__(self) -> _RawKnocks:
        self._conn = await asyncpg.connect(self._dsn)
        for channel in self._channels:
            await self._conn.add_listener(channel, self._on_notify)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._conn is not None:
            await self._conn.close()

    def _on_notify(self, _c: Any, _pid: int, channel: str, payload: str) -> None:
        self.knocks.append((channel, payload))

    def of(self, channel: str) -> list[str]:
        return [payload for ch, payload in self.knocks if ch == channel]


# ── T26-P1: the create leg is TRANSACTIONAL (a rollback is silent) ──────


async def test_hold_create_broadcast_is_transactional_and_shaped(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
    hitl_redlog: RedLog,
) -> None:
    """Pin (a): the hold-create broadcast rides the INSERT's OWN tx —
    the payload is the POINTER set (schema, flow_id, run_id, hold_id,
    signal, node_key, created_at), never the payload content; and a
    rolled-back hold-create emits NO notification.

    THE RED (the convicted variant, captured): the knock sent OUTSIDE
    the tx (autocommit) outlives its row — the drill rolls the INSERT
    back under a knock that already fired, and the broadcast LIES (a
    hold the rows disown). The shipped leg cannot lie: the NOTIFY
    commits or nothing does."""
    # ── THE RED DRILL (the pre-cure shape): the knock on a separate
    # autocommit connection, the insert rolled back under it. ──
    drill_hold = str(new_uuid())
    async with _RawKnocks(module_pg_schema.pg_dsn, HOLD_CREATED_CHANNEL) as knocks:
        drill = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            await drill.execute(
                "SELECT pg_notify($1, $2)",
                HOLD_CREATED_CHANNEL,
                json.dumps({"schema": wf_schema, "hold_id": drill_hold}),
            )
            async with wf_conn.transaction():
                await wf_conn.execute(
                    f'INSERT INTO "{wf_schema}".wf_signals '
                    "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status) "
                    "VALUES ($1::uuid, $2::uuid, 'drill', 'Approval', 1, 'call:drill', 'held')",
                    drill_hold,
                    str(new_uuid()),
                )
                raise RollbackDrillError
        except RollbackDrillError:
            pass
        finally:
            await drill.close()
        await asyncio.sleep(0.2)
        drill_knocks = [k for k in knocks.of(HOLD_CREATED_CHANNEL) if drill_hold in k]
        row = await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".wf_signals WHERE id = $1', uuid_module.UUID(drill_hold)
        )
        assert row is None, "the drill's insert did not roll back — the drill proves nothing"
        assert drill_knocks, (
            "the pre-cure mutation did not reproduce the dragon — the drill is vacuous"
        )
        hitl_redlog.red(
            "T26-P1-create-knock-outside-the-tx",
            "the broadcast sent OUTSIDE the hold's transaction (autocommit): the knock "
            "outlives its rolled-back row — the broadcast LIES about a hold the rows disown",
            {"knocks_without_row": len(drill_knocks), "row_committed": False},
        )

    # ── THE GREEN: the shipped leg — create through the real wait site,
    # the knock shaped, the pointer only. ──
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="T26_create_broadcast_flow"
    )
    hold_row = await wf_conn.fetchrow(
        f'SELECT id, created_at FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert hold_row is not None
    async with _RawKnocks(module_pg_schema.pg_dsn, HOLD_CREATED_CHANNEL) as knocks:
        # A SECOND hold (new epoch) through the REAL leg — its knock is
        # the shipped one's witness.
        hold_id = await register_hold(
            wf_conn,
            schema=wf_schema,
            workflow_id=flow_id,
            node_id=JobId(new_uuid()),
            node_key="review",
            signal_name="Approval",
            hold_epoch=2,
            call_id="call:T26-shape",
            payload_schema={"Approval": {"type": "object", "properties": {}}},
            timeout_s=120.0,
        )
        await asyncio.sleep(0.2)
    shaped = [json.loads(k) for k in knocks.of(HOLD_CREATED_CHANNEL) if str(hold_id) in k]
    assert shaped, "the shipped create leg emitted NO knock — the broadcast is dead"
    doc = shaped[0]
    assert set(doc) == {
        "schema",
        "flow_id",
        "run_id",
        "hold_id",
        "signal",
        "node_key",
        "created_at",
    }, doc
    assert doc["schema"] == wf_schema
    assert doc["hold_id"] == str(hold_id)
    assert doc["flow_id"] == str(flow_id) and doc["run_id"] == str(flow_id)
    assert doc["signal"] == "Approval" and doc["node_key"] == "review"
    # THE POINTER-ONLY LAW: the knock names the hold, never carries it.
    assert "doc_id" not in json.dumps(doc)


class RollbackDrillError(Exception):
    """The drill's own unwinding (named — never a bare raise)."""


# ── T26-P2: the zero-window backfill + the dedup ────────────────────────


async def test_listener_backfills_open_holds_and_dedups(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Pin (b): a hold created BEFORE the listener's start is STILL
    delivered — the first event is the backfilled ``HoldCreated``; and
    a hold the snapshot already announced dedups its own late NOTIFY
    (one id, ONE created event — the LISTEN→snapshot race's duplicate
    is unrepresentable in the stream)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="T26_backfill_flow"
    )
    hold_row = await wf_conn.fetchrow(
        f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
    )
    assert hold_row is not None
    hold_id = str(hold_row["id"])

    listener = HitlListener(wf_pool, wf_schema)
    async with listener:
        # THE ZERO WINDOW: the hold existed BEFORE the listener — the
        # FIRST event on THIS RUN'S filtered stream is its backfilled
        # HoldCreated (the backfill announces every open hold in the
        # schema — earlier tests' leftovers included; holds(run=…)
        # scopes the stream to this run's).
        first = await asyncio.wait_for(_first_of(listener.holds(run=flow_id)), timeout=5.0)
        assert first is not None
        assert isinstance(first, HoldCreated), first
        assert first.event == "hold_created"
        assert first.hold_id == hold_id
        assert first.source == "backfill", first
        # THE DEDUP LEG (unit, at the decoder — the snapshot's id vs its
        # own late notify): the backfilled id's create-notify yields
        # NOTHING; a fresh id yields the typed event.
        snapshot_doc = json.dumps(
            {
                "schema": wf_schema,
                "flow_id": str(flow_id),
                "run_id": str(flow_id),
                "hold_id": hold_id,
                "signal": "Approval",
                "node_key": "review",
                "created_at": "2026-01-01T00:00:00+00:00",
            }
        )
        fresh_id = str(new_uuid())
        fresh_doc = json.dumps({**json.loads(snapshot_doc), "hold_id": fresh_id})
        assert listener._decode(HOLD_CREATED_CHANNEL, snapshot_doc) is None, (
            "the backfilled hold's own notify RE-DELIVERED — the dedup died "
            "(the LISTEN→snapshot race doubles the created event)"
        )
        decoded = listener._decode(HOLD_CREATED_CHANNEL, fresh_doc)
        assert decoded is not None and isinstance(decoded, HoldCreated)
        assert decoded.hold_id == fresh_id
        assert decoded.source == "notify"


# ── T26-P3: the resolve wake reaches the listener ───────────────────────


async def test_resolve_wake_reaches_the_listener(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin (c): ``HitlClient.resolve`` → the listener's ``HoldResolved``
    within a bound (condition-not-clock: the wait_for is the SAFETY
    net — the event fires in ms); ``verdict_kind`` is the payload model
    the typed door validated against (the verdict's DECLARED kind,
    never the verdict's content)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="T26_resolve_wake_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    listener = HitlListener(wf_pool, schema=wf_schema)
    async with listener:
        result = await client.resolve(holds[0].hold_id, {"verdict": "approve"})
        assert result.status == "delivered"
        event = await asyncio.wait_for(_next_of(listener, "hold_resolved"), timeout=5.0)
        assert event is not None and event.hold_id == holds[0].hold_id
        assert event.run_id == str(flow_id)
        assert event.verdict_kind == "Approval", (
            f"the verdict's declared kind did not ride the broadcast: {event}"
        )
        assert "approve" not in json.dumps(event.model_dump()), (
            "THE RESOLVED BROADCAST CARRIED THE VERDICT'S CONTENT — the pointer-only law"
        )


async def _first_of(events: Any) -> Any:
    """The filtered stream's first event (the holds(run=…) read)."""
    async for event in events:
        return event
    return None


async def _next_of(listener: HitlListener, kind: str) -> Any:
    """The listener's next event of ONE kind (the dedup-ordered stream
    read — skips the other kinds' events)."""
    async for event in listener.events():
        if event.event == kind:
            return event
    return None


# ── T26-P4: the expiry fail-close (the worked example) ──────────────────


async def test_deep_research_approve_path(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE WORKED EXAMPLE'S APPROVE PATH end-to-end: the loop → the
    hold → the broadcast → the resolve → the resume → the FULL REPORT.
    The REAL 120 s default stands (the approval lands first —
    condition-not-clock); no constant is scaled on this path. THE
    LISTENER'S AMENDED FACE is the surface under test: the DIRECT
    async-for over ``holds(run=...)`` — the backend author's
    zero-lookup flow (the ticket's DX section, verbatim)."""
    import examples.deep_research as dr

    listener = HitlListener(wf_pool, wf_schema)
    async with listener:
        runner = FlowRunner(dr.dr_app.get("deep_research"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        await runner.drive(flow_id, until="held")
        # THE BROADCAST, the ceremony-free filtered stream: the closed
        # union matched exhaustively — the backend author's tour.
        created: HoldCreated | None = None
        wake: HoldResolved | None = None
        async for event in listener.holds(run=flow_id):
            match event:
                case HoldCreated():
                    created = event
                    break
                case HoldResolved() | HoldExpired() | Backfilled():
                    continue
        assert created is not None, "the gate's hold never reached the holds(run=...) stream"
        # THE HUMAN'S YES through the unchanged typed door.
        client = HitlClient(wf_pool, schema=wf_schema)
        resolved = await dr.approve_continue(client, created.hold_id)
        assert resolved.status == "delivered", resolved
        async for event in listener.holds(run=flow_id):
            match event:
                case HoldResolved() as r:
                    wake = r
                    break
                case HoldCreated() | HoldExpired() | Backfilled():
                    continue
        assert wake is not None and wake.verdict_kind == "ContinueApproval", (
            "the resolve wake never carried the verdict's declared kind"
        )
    # THE RESUME: the loop continues to the full report.
    await runner.drive(flow_id, max_ticks=40)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "succeeded", f"the approved loop never finished: {root!r}"
    result = await runner.result(flow_id)
    assert isinstance(result, dict)
    assert result["status"] == "report_complete", result
    assert len(result["notes"]) == 5, result  # the three free passes + the approved extensions


async def _created_for(listener: HitlListener, run_id: str) -> Any:
    """The listener's next HoldCreated for ONE run."""
    async for event in listener.events():
        if event.event == "hold_created" and event.run_id == run_id:
            return event
    return None


async def test_deep_research_expiry_fail_close(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin (d): the expiry fail-close — the approval window expires →
    the body's TYPED expiry is the fail-close arm → the run SUCCEEDS
    carrying the finish-with-what-you-have RESULT; the broadcast
    delivered the HoldExpired.

    THE SLOT LAW (asserted DURING the wait): the held node is
    ``pending`` with the hold marker and NO worker — the worker
    RELEASED the slot; a human thinking must never hold a slot
    hostage."""
    import examples.deep_research as dr

    monkeypatch.setattr(
        dr, "APPROVAL_TIMEOUT_S", 1.0
    )  # the TESTS scale it — the example keeps the 120 s

    listener = HitlListener(wf_pool, schema=wf_schema)
    async with listener:
        runner = FlowRunner(dr.dr_app.get("deep_research"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        await runner.drive(flow_id, until="held")
        created = await asyncio.wait_for(_created_for(listener, str(flow_id)), timeout=5.0)
        assert created is not None
        node_id = await wf_conn.fetchval(
            f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = 'research' AND "
            "(metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        # THE SLOT LAW: no slot held while the loop waits on a human.
        node_row = await wf_conn.fetchrow(
            f'SELECT status, locked_by_worker, metadata FROM "{wf_schema}".jobs WHERE id = $1',
            node_id,
        )
        assert node_row is not None
        assert node_row["status"] == "pending", node_row
        assert node_row["locked_by_worker"] is None, "THE WAIT HELD THE SLOT HOSTAGE"
        node_meta = node_row["metadata"]
        meta_doc = json.loads(node_meta) if isinstance(node_meta, str) else (node_meta or {})
        assert "hold" in meta_doc, f"the hold marker is gone from the node: {meta_doc}"
        # THE EXPIRY (the sweep is the only live timer; the deadline is
        # PG's clock): the deadline passes, the sweep abandons.
        await asyncio.sleep(1.1)
        abandoned = await sweep_expired_signals(wf_pool, schema=wf_schema)
        assert abandoned >= 1
        expired = await asyncio.wait_for(_expired_for(listener, str(flow_id)), timeout=5.0)
        assert expired is not None and expired.hold_id == created.hold_id, (
            "the expiry leg never delivered for the LOOP-kind hold — the broadcast is "
            "blind to the budget-paused rows"
        )
    # THE FAIL-CLOSE: the body caught the typed expiry — the run
    # SUCCEEDS with the named result.
    await runner.drive(flow_id, max_ticks=40)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "succeeded", (
        f"the fail-close is not the success it claims: the flow is {root!r} — the user "
        "not watching must be a RESULT, never a failure"
    )
    result = await runner.result(flow_id)
    assert isinstance(result, dict)
    assert result["status"] == "finished_with_what_you_have", result
    assert len(result["notes"]) == 3, result  # the free passes stand; nothing invented


async def _expired_for(listener: HitlListener, run_id: str) -> Any:
    """The listener's next HoldExpired for ONE run."""
    async for event in listener.events():
        if event.event == "hold_expired" and event.run_id == run_id:
            return event
    return None


# ── T26-P5: the expiry inside a LOOP's hold — the escalation leg ────────


async def test_expiry_in_loop_escalation_leg_still_delivers(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin (e): a LOOP whose hold expires with ``on_exhausted="escalate"``:
    the typed expiry crosses the body boundary (the body does NOT
    fail-close — the escalation lane's scenario) → the loop exhausts
    with the body failure → the escalation row + the flow's terminal
    commit TOGETHER → and the ESCALATION-KIND exemption's leg STILL
    DELIVERS: the ``loop.escalation`` consumer dispatches through the
    REAL fleet claim despite the terminal flow (a flow's death must not
    orphan its pages-a-human duty)."""
    from datetime import timedelta

    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch

    app = WorkflowApp()

    async def wait_and_die(ctx: StepContext, carry: int) -> Done[int] | Refine[int]:
        # NO fail-close here (the escalation lane's body — the opposite
        # disposition from the example's): THE ESCALATION LADDER'S OWN
        # USE (the amendment) — the body converts the expiry MEMBER into
        # the RAISED failure itself (the machinery never raises it).
        outcome = await ctx.wait_signal(Approval, timeout_s=1.0)
        match outcome:
            case Approval():
                return Done(carry)
            case Expired():
                raise SignalTimeoutError(
                    "the escalation lane's body does not fail-close — "
                    "the expiry is the failure it raises itself"
                )

    escalation_gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=1.0)

    @app.workflow("T26_escalation_expiry_flow")
    def escalation_flow() -> Promise[object]:
        return build(
            loop(
                "review",
                wait_and_die,
                initial=0,
                max_iterations=6,
                on_exhausted="escalate",
                gates=(escalation_gate,),
            )
        )

    listener = HitlListener(wf_pool, schema=wf_schema)
    async with listener:
        runner = FlowRunner(app.get("T26_escalation_expiry_flow"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        await runner.drive(flow_id, until="held")
        # THE DEADLINE + THE SWEEP: the loop-kind hold (budget-paused)
        # expires through the SAME arm — and the leg delivers.
        await asyncio.sleep(1.1)
        abandoned = await sweep_expired_signals(wf_pool, schema=wf_schema)
        assert abandoned >= 1
        expired = await asyncio.wait_for(_next_of(listener, "hold_expired"), timeout=5.0)
        assert expired is not None, (
            "the expiry broadcast never reached the listener for the LOOP's hold — "
            "the budget-paused row went blind"
        )
    # THE RESUME: the typed expiry crosses the body boundary → the loop
    # exhausts WITH the escalation row; the flow terminalizes.
    await runner.drive(flow_id, max_ticks=40)
    flow_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert flow_status == "failed", flow_status
    outbox = await wf_conn.fetch(
        f'SELECT consumer_step_key FROM "{wf_schema}".wf_outbox WHERE flow_id = $1', flow_id
    )
    assert any(r["consumer_step_key"] == "loop.escalation" for r in outbox), (
        "the expiry-induced exhaust wrote NO escalation row — the interplay died before the fence"
    )
    await runner.tick(flow_id, execute=False)
    consumer_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE step_key = 'loop.escalation' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert consumer_id is not None, "the escalation outbox row never drained into a consumer job"
    consumer_row = await wf_conn.fetchrow(
        f'SELECT status, actor, queue, step_key FROM "{wf_schema}".jobs WHERE id = $1',
        consumer_id,
    )
    assert consumer_row is not None and consumer_row["status"] == "pending"
    # THE REAL FLEET CLAIM — the dispatch fence's terminal-flow leg with
    # the ESCALATION-KIND exemption is the ONLY door this row has.
    worker_id = JobId(new_uuid())
    runner = FlowRunner(
        app.get("T26_escalation_expiry_flow"), wf_pool, wf_schema, worker_id=worker_id
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        consumer_row["actor"],
        consumer_row["queue"],
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 't26-escalation-pin', 1, $2::text[], $3::jsonb)",
        worker_id,
        [consumer_row["queue"]],
        json.dumps({"workflow_execution": True}),
    )
    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=[consumer_row["queue"]],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    claimed = {str(r["id"]) for r in dispatched}
    assert str(consumer_id) in claimed, (
        f"the escalation consumer {consumer_id} was NOT claimable on the terminal flow — "
        "the ESCALATION-KIND exemption's leg stopped delivering (the page-a-human duty died)"
    )
    job = next(r for r in dispatched if str(r["id"]) == str(consumer_id))
    outcome = await runner.run_fleet_claimed_step(
        flow_id,
        {
            "id": job["id"],
            "step_key": "loop.escalation",
            "map_index": None,
            "payload": job["payload"],
            "trace_id": job["trace_id"],
        },
        attempt=int(job["attempt"]),
        claim_epoch=int(job["claim_epoch"]),
    )
    assert outcome == "succeeded", outcome


# ── T26-P6: the reconnect reconcile (the fourth union member) ───────────


class _Cards:
    """THE TAUGHT CONSUMER'S CARD SET (the watching user's state, driven
    ONLY by the union's events): create opens a card, resolve/expired
    close it, and the ``Backfilled`` snapshot RECONCILES — anything open
    on the consumer's side that the snapshot's ids disown is DROPPED.
    This is C1's consumer half: a hold resolved or expired during a
    listener outage never announces itself (its notify went to no one —
    the connection was down), so the SNAPSHOT is the only witness."""

    def __init__(self) -> None:
        self.open: dict[str, str] = {}
        self.snapshots: list[frozenset[str]] = []

    def apply(self, event: Any) -> None:
        kind = event.event
        if kind == "hold_created":
            self.open[event.hold_id] = event.signal
        elif kind in ("hold_resolved", "hold_expired"):
            self.open.pop(event.hold_id, None)
        elif kind == "backfilled":
            snapshot = frozenset(event.open_hold_ids)
            self.snapshots.append(snapshot)
            self.open = {hid: sig for hid, sig in self.open.items() if hid in snapshot}


async def test_reconnect_reconcile_clears_holds_resolved_during_the_outage(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    hitl_redlog: RedLog,
) -> None:
    """Pin P6 (the reconnect-reconcile hole — the hostile review's C1):
    ``_backfill()`` selects only ``status='held'``, so a hold RESOLVED
    during a listener outage never clears the consumer's card — the
    create event announced it, nothing ever announced its death.

    THE CURE: the FOURTH union member — ``Backfilled(open_hold_ids=…)``
    — emitted after EVERY (re)backfill; the consumer drops anything not
    in the snapshot. THE RED (captured at the pre-cure head): after the
    reconnect the stream carried NOTHING that closes the card — the
    consume-until-reconcile timed out; the stale history even REPLAYED
    the dead hold's create (the ghost) into the new subscriber.

    THE DRILL (permanent, recorded): at the outage window the card set
    and the ROWS diverge — the consumer still holds the card the rows
    disown. THE C7 VERIFICATION rides the same walk: the stale
    ``_backfilled``/``_raw`` notifies announcing dead holds die the
    SAME death — the new subscriber replays the ghost ``HoldCreated``
    from the pre-outage history, and the reconcile snapshot drops its
    card (the ghost cannot survive the snapshot)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="T26_reconnect_reconcile_flow"
    )
    hold_id = str(
        await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
        )
    )

    listener = HitlListener(wf_pool, wf_schema)
    cards = _Cards()
    await listener.start()
    try:
        # THE OPEN: the backfilled create announces the hold; the taught
        # consumer opens its card.
        async with asyncio.timeout(5.0):
            async for event in listener.holds(run=flow_id):
                cards.apply(event)
                break
        assert cards.open == {hold_id: "Approval"}, cards.open
        # THE OUTAGE: the listener disconnects (the pump stopped, the
        # dedicated LISTEN connection released) — and the rows move on
        # while nobody watches.
        await listener.stop()
        client = HitlClient(wf_pool, schema=wf_schema)
        resolved = await client.resolve(hold_id, {"verdict": "approve"})
        assert resolved.status == "delivered"
        # THE DRILL (the dragon, observed at the outage window): the
        # consumer's card set still holds the hold the ROWS disown.
        row_status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1',
            uuid_module.UUID(hold_id),
        )
        assert row_status == "delivered" and cards.open == {hold_id: "Approval"}, (
            "the drill's divergence is gone — the rows and the cards must "
            "disagree HERE, or the pin proves nothing"
        )
        hitl_redlog.red(
            "T26-P6-card-vs-rows-divergence",
            "the hold resolved during the listener outage: the consumer's card set "
            "still holds it open (no event ever announces a death the disconnected "
            "listener cannot hear) — the snapshot reconcile is the only cure",
            {"cards_open": sorted(cards.open), "row_status": row_status},
        )
        # THE RECONNECT: the same listener object (its HISTORY still
        # carries the dead hold's create — the ghost), a fresh
        # connection, a fresh backfill.
        await listener.start()
        reconcile: Any = None
        async with asyncio.timeout(5.0):
            async for event in listener.holds(run=flow_id):
                cards.apply(event)
                if event.event == "backfilled":
                    reconcile = event
                    break
        assert reconcile is not None, (
            "the reconnect carried NO Backfilled snapshot — the reconnect-reconcile "
            "hole is open: a hold resolved during the outage never clears the "
            "consumer's card"
        )
        assert hold_id not in reconcile.open_hold_ids, reconcile
        # THE C7 DEATH: the ghost (the stale create in the replayed
        # history) may cross the stream — the card does not survive the
        # snapshot.
        assert cards.open == {}, (
            f"the resolved hold's card survived the reconnect reconcile: {cards.open}"
        )
    finally:
        await listener.stop()


# ── T26-P7: the concurrent-consumer fan-out (no partitioned stream) ─────


async def test_two_consumers_fan_out_and_each_gets_its_own_sentinel(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    hitl_redlog: RedLog,
) -> None:
    """Pin P7 (the silently single-consumer listener — the hostile
    review's C2): the pre-cure listener pulled ONE shared queue — two
    concurrent consumers PARTITIONED the stream (each stole the other's
    events) and ``stop()``'s single ``None`` sentinel stranded all but
    one waiter. THE PRODUCT SHAPE is one backend listener fanning out to
    N watching users (the progress stream's own pub/sub discipline —
    Redis pub/sub broadcasts, it never partitions).

    THE DRILL (permanent): the convicted shape, hand-built — ONE shared
    queue, ONE sentinel, two waiters — recorded: one consumer hauled the
    events, the other starved against a sentinel that was never its own.

    THE GREEN: per-subscriber queues — ``holds()`` and each consumer get
    their OWN queue + their OWN sentinel on stop; every consumer sees
    EVERY event; both consumers END on stop."""

    # ── THE DRILL (the convicted variant, reproducible forever) ──
    shared: asyncio.Queue[int | None] = asyncio.Queue()
    for i in range(3):
        shared.put_nowait(i)
    shared.put_nowait(None)  # THE SINGLE SENTINEL — the pre-cure stop()'s shape

    async def _shared_consumer(haul: list[int]) -> None:
        while True:
            item = await shared.get()
            if item is None:
                return
            haul.append(item)

    haul_a: list[int] = []
    haul_b: list[int] = []
    # The single sentinel ends ONE waiter; the other is stranded — the
    # bound proves the strand (wait_for is the pin's net, not the clock).
    await asyncio.wait_for(_shared_consumer(haul_a), timeout=1.0)
    try:
        await asyncio.wait_for(_shared_consumer(haul_b), timeout=0.5)
        stranded = False
    except TimeoutError:
        stranded = True
    assert stranded and haul_a == [0, 1, 2] and haul_b == [], (
        f"the convicted partition did not reproduce: a={haul_a} b={haul_b}"
    )
    hitl_redlog.red(
        "T26-P7-shared-queue-partition",
        "ONE shared queue + ONE stop sentinel: two concurrent consumers partition "
        "the stream — consumer A steals every event, consumer B starves against a "
        "sentinel that is never its own",
        {"consumer_a_haul": haul_a, "consumer_b_haul": haul_b, "b_stranded": stranded},
    )

    # ── THE GREEN: the shipped fan-out ──
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="T26_fanout_flow"
    )
    listener = HitlListener(wf_pool, wf_schema)
    await listener.start()
    try:
        events_a: list[Any] = []
        events_b: list[Any] = []

        async def consume(events: list[Any]) -> None:
            async for event in listener.events():
                events.append(event)

        task_a = asyncio.create_task(consume(events_a))
        task_b = asyncio.create_task(consume(events_b))
        await asyncio.sleep(0.1)  # both subscribers attached
        hold_id = await register_hold(
            wf_conn,
            schema=wf_schema,
            workflow_id=flow_id,
            node_id=JobId(new_uuid()),
            node_key="review",
            signal_name="Approval",
            hold_epoch=2,
            call_id="call:T26-fanout",
            payload_schema={"Approval": {"type": "object", "properties": {}}},
            timeout_s=120.0,
        )
        async with asyncio.timeout(5.0):
            await wait_for_condition(
                lambda: any(getattr(e, "hold_id", None) == str(hold_id) for e in events_a),
                description="consumer A never received the create",
            )
            await wait_for_condition(
                lambda: any(getattr(e, "hold_id", None) == str(hold_id) for e in events_b),
                description="consumer B never received the create",
            )
        # THE FAN-OUT LAW: EVERY consumer saw EVERY event — never a partition.
        assert any(getattr(e, "hold_id", None) == str(hold_id) for e in events_a), events_a
        assert any(getattr(e, "hold_id", None) == str(hold_id) for e in events_b), events_b
        # THE SENTINEL LAW: stop() ends BOTH waiters (each its own None).
        await listener.stop()
        await asyncio.wait_for(task_a, timeout=5.0)
        await asyncio.wait_for(task_b, timeout=5.0)
    finally:
        await listener.stop()


# ── T26-P8: the schema isolation (the payload filter IS the isolation) ──


async def test_two_schemas_one_database_the_payload_filter_is_the_isolation(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
    hitl_redlog: RedLog,
) -> None:
    """Pin P8: TWO schemas on ONE database — the global channels carry
    BOTH schemas' knocks, and the PAYLOAD FILTER is the isolation: the
    listener on schema A delivers A's holds and is SILENT on B's.

    THE DRILL (permanent): a consumer that trusts the CHANNEL without
    the payload filter hears the OTHER schema's holds — the cross-schema
    leak the convicted channel-arithmetic routing buys. THE GREEN: the
    shipped listener filters every decoded event by the payload's
    ``schema`` field (and the ``Backfilled`` snapshot only ever lists
    ITS schema's open holds)."""

    from taskq.migrate import apply_pending

    schema_b = module_pg_schema.schema_name[:-2] + "_b"
    setup = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await setup.execute(f'DROP SCHEMA IF EXISTS "{schema_b}" CASCADE')
        await apply_pending(setup, schema=schema_b)
    finally:
        await setup.close()

    # ── THE DRILL: the unfiltered channel ear hears the other schema ──
    foreign_id = str(new_uuid())
    async with _RawKnocks(module_pg_schema.pg_dsn, HOLD_CREATED_CHANNEL) as knocks:
        leaked = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            await leaked.execute(
                "SELECT pg_notify($1, $2)",
                HOLD_CREATED_CHANNEL,
                json.dumps(
                    {
                        "schema": schema_b,
                        "hold_id": foreign_id,
                        "flow_id": str(new_uuid()),
                        "run_id": str(new_uuid()),
                        "signal": "Approval",
                        "node_key": "review",
                        "created_at": None,
                    }
                ),
            )
        finally:
            await leaked.close()
        await asyncio.sleep(0.2)
        assert any(foreign_id in k for k in knocks.of(HOLD_CREATED_CHANNEL)), (
            "the drill's knock never crossed — the raw ear proves nothing"
        )
        hitl_redlog.red(
            "T26-P8-unfiltered-channel-ear",
            "a consumer that trusts the CHANNEL (pg_notify is per-DATABASE, the "
            "channels are global) hears EVERY schema's holds — the payload filter "
            "is the isolation, and without it the estate's schemas leak",
            {"foreign_schema": schema_b, "foreign_knocks": len(knocks.of(HOLD_CREATED_CHANNEL))},
        )

    # ── THE GREEN: the shipped listener filters by the payload ──
    foreign_flow = str(new_uuid())
    await register_hold(
        wf_conn,  # any connection on the SAME DATABASE — the schema selects the tables
        schema=schema_b,
        workflow_id=JobId(uuid_module.UUID(foreign_flow)),
        node_id=JobId(new_uuid()),
        node_key="review",
        signal_name="Approval",
        hold_epoch=1,
        call_id="call:T26-isolation",
        payload_schema={"Approval": {"type": "object", "properties": {}}},
        timeout_s=120.0,
    )

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="T26_isolation_flow"
    )
    listener = HitlListener(wf_pool, wf_schema)
    async with listener:
        seen: list[Any] = []
        async with asyncio.timeout(5.0):
            async for event in listener.holds(run=flow_id):
                seen.append(event)
                if event.event == "backfilled":
                    break
        # THE ISOLATION: the foreign schema's hold NEVER crossed (its
        # notify rode the same database's same channels); the snapshot
        # lists only THIS schema's open holds.
        assert all(getattr(e, "hold_id", None) != foreign_id for e in seen), seen
        reconcile = [e for e in seen if getattr(e, "event", "") == "backfilled"]
        assert reconcile, "the backfilled snapshot never arrived"
        assert all(foreign_id not in e.open_hold_ids for e in reconcile), reconcile
        own_hold = str(
            await wf_conn.fetchval(
                f'SELECT id FROM "{wf_schema}".wf_signals WHERE workflow_id = $1', flow_id
            )
        )
        assert all(own_hold in e.open_hold_ids for e in reconcile), reconcile

    teardown = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await teardown.execute(f'DROP SCHEMA IF EXISTS "{schema_b}" CASCADE')
    finally:
        await teardown.close()


# ── T26-P9: the payload-size bound (the pointer-law creep protector) ────


async def test_notify_payloads_stay_under_the_named_cap(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
    hitl_redlog: RedLog,
) -> None:
    """Pin P9 (the pointer-law creep protector): PG's ``pg_notify``
    refuses a payload at 8000 bytes — and a knock refused MID-TX rolls
    the hold's OWN transaction back with it (the create leg dies inside
    register_hold's tx). The pointer payloads are SMALL by law; the cap
    must be NAMED and GUARDED so the pointer law's creep (a payload
    field grown to carry content) dies at the code seam, loudly, before
    PG's opaque error.

    THE DRILL (permanent): an over-cap knock handed to pg_notify raises
    — recorded. THE GREEN: the named cap constant + the guard in
    ``_broadcast`` (the typed refusal BEFORE the wire) + every shipped
    leg measured under the cap (a pathologically long — but valid —
    signal name included)."""

    from taskq.workflows.api._hitl import NOTIFY_PAYLOAD_MAX_BYTES, _broadcast

    # ── THE DRILL: PG's own refusal (what an unguarded over-cap knock costs) ──
    drill = await asyncpg.connect(module_pg_schema.pg_dsn)
    refused = "unreached"
    try:
        async with drill.transaction():
            try:
                await drill.execute("SELECT pg_notify($1, $2)", HOLD_CREATED_CHANNEL, "x" * 9000)
                refused = "NOT-refused"
            except Exception as exc:
                refused = f"{type(exc).__name__}: {str(exc)[:120]}"
    finally:
        await drill.close()
    assert refused != "NOT-refused", "PG accepted a 9000-byte payload — the cap moved"
    hitl_redlog.red(
        "T26-P9-over-cap-pg-notify",
        "the over-cap knock reaches pg_notify UNGUARDED: PG refuses it mid-tx and "
        "the caller's whole transaction dies with the opaque server error — the "
        "pointer-law creep must die at the code seam instead, loudly, named",
        {"observed": refused},
    )

    # ── THE GUARD: the named cap, refused before the wire ──
    assert NOTIFY_PAYLOAD_MAX_BYTES == 8000, NOTIFY_PAYLOAD_MAX_BYTES
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        with pytest.raises(ValueError, match="8000"):
            await _broadcast(conn, HOLD_CREATED_CHANNEL, {"blob": "y" * 9000})
    finally:
        await conn.close()

    # ── THE SHIPPED LEGS MEASURED: even a pathological (but valid) name ──
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    long_key = "review" * 60  # 360 chars of node key — legal, ugly
    app = WorkflowApp()

    @app.workflow("T26_cap_flow")
    def cap_flow() -> Promise[object]:
        return build(step(hold_body, Ingest(doc_id="d1"), key=long_key))

    runner = FlowRunner(app.get("T26_cap_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    async with _RawKnocks(module_pg_schema.pg_dsn, HOLD_CREATED_CHANNEL) as knocks:
        hold_id = await register_hold(
            wf_conn,
            schema=wf_schema,
            workflow_id=flow_id,
            node_id=JobId(new_uuid()),
            node_key=long_key,
            signal_name="Approval",
            hold_epoch=2,
            call_id="call:T26-cap",
            payload_schema={"Approval": {"type": "object", "properties": {}}},
            timeout_s=120.0,
        )
        await asyncio.sleep(0.2)
    shaped = [k for k in knocks.of(HOLD_CREATED_CHANNEL) if str(hold_id) in k]
    assert shaped, "the create leg never knocked — the cap pin's leg is dead"
    assert max(len(k.encode()) for k in shaped) < NOTIFY_PAYLOAD_MAX_BYTES


# ── T26-P10: the resolve-rollback silence (symmetric to P1) ─────────────


class AuditBoomError(Exception):
    """The drill's injected audit failure (named — never a bare raise)."""


async def test_rolled_back_resolve_emits_no_notification(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: Any,
    hitl_redlog: RedLog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin P10 (symmetric to P1): a ROLLED-BACK resolve emits NO
    notification — the CAS + the audit + the broadcast ride ONE tx, so
    a resolve whose tx dies is SILENT (nothing announces a decision the
    rows disown).

    THE DRILL (permanent, the convicted variant like P1's): the resolve
    knock sent OUTSIDE the CAS's tx outlives the rollback — the
    broadcast LIES about a decision that never landed.

    THE GREEN: the shipped resolve path, its tx FORCED to fail (the
    audit write injected to raise): the exception propagates, the tx
    rolls back, the hold SURVIVES as ``held``, and NO
    ``taskq_wf_hold_resolved`` knock ever fires."""

    # ── THE DRILL: the lying knock (the knock outside the tx) ──
    drill_hold = str(new_uuid())
    async with _RawKnocks(module_pg_schema.pg_dsn, HOLD_RESOLVED_CHANNEL) as knocks:
        liar = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            await liar.execute(
                "SELECT pg_notify($1, $2)",
                HOLD_RESOLVED_CHANNEL,
                json.dumps({"schema": wf_schema, "hold_id": drill_hold}),
            )
            # The "resolve" the knock announced — rolled back under it.
            async with wf_conn.transaction():
                await wf_conn.execute(
                    f"UPDATE \"{wf_schema}\".wf_signals SET status = 'delivered' "
                    "WHERE id = $1 AND status = 'held'",
                    uuid_module.UUID(drill_hold),
                )
                raise RollbackDrillError
        except RollbackDrillError:
            pass
        finally:
            await liar.close()
        await asyncio.sleep(0.2)
        assert any(drill_hold in k for k in knocks.of(HOLD_RESOLVED_CHANNEL)), (
            "the drill's knock never crossed — the drill proves nothing"
        )
        hitl_redlog.red(
            "T26-P10-resolve-knock-outside-the-tx",
            "the resolve knock sent OUTSIDE the CAS tx (autocommit): the knock "
            "outlives the rolled-back resolve — the broadcast LIES about a "
            "decision the rows disown (P1's dragon, on the resolve leg)",
            {"lying_knocks": len(knocks.of(HOLD_RESOLVED_CHANNEL))},
        )

    # ── THE GREEN: the shipped path's tx forced to fail — SILENT ──
    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    flow_id, _runner, _node = await _held_flow(
        wf_conn, wf_schema, wf_pool, wait_body=hold_body, name="T26_resolve_rollback_flow"
    )
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    hold_id = holds[0].hold_id
    async with _RawKnocks(module_pg_schema.pg_dsn, HOLD_RESOLVED_CHANNEL) as knocks:
        import taskq.audit as audit_module

        real_record = audit_module.record_admin_action

        async def _boom(*a: Any, **k: Any) -> None:
            raise AuditBoomError("the injected audit failure (P10's rollback)")

        monkeypatch.setattr(audit_module, "record_admin_action", _boom)
        try:
            with pytest.raises(AuditBoomError):
                await client.resolve(hold_id, {"verdict": "approve"})
        finally:
            monkeypatch.setattr(audit_module, "record_admin_action", real_record)
        await asyncio.sleep(0.2)
    status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', uuid_module.UUID(hold_id)
    )
    assert status == "held", f"the rolled-back resolve consumed the hold: {status!r}"
    assert not any(hold_id in k for k in knocks.of(HOLD_RESOLVED_CHANNEL)), (
        "THE ROLLED-BACK RESOLVE KNOCKED — the broadcast announced a decision "
        "the rows disown (P10's dragon, on the shipped leg)"
    )


# ── C8: the double-timeout precedence (GateDecl's vs the wait's) ────────


async def test_wait_signals_timeout_arms_the_sweep_not_the_gate_declaration(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin (C8): BOTH faces declare a timeout — ``GateDecl(timeout_s=…)``
    AND ``ctx.wait_signal(timeout_s=…)`` — and the precedence is: THE
    WAIT'S VALUE ARMS THE SWEEP (``register_hold`` writes ``expires_at``
    from the wait site's ``timeout_s``); the gate's ``timeout_s`` is the
    COMPILE-VISIBLE declaration (the Mermaid face, the W1 warning) —
    never a second runtime clock. The pin holds the precedence on the
    rows: a gate declaring 5 s under a wait declaring 120 s leaves a
    ~120 s deadline on the row (the DB clock's)."""

    async def hold_body(ctx: StepContext, params: Ingest) -> Any:
        return await ctx.wait_signal(Approval, timeout_s=120.0)

    app = WorkflowApp()
    dissenting_gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=5.0)

    @app.workflow("T26_timeout_precedence_flow")
    def precedence_flow() -> Promise[object]:
        return build(step(hold_body, Ingest(doc_id="d1"), key="review", gates=(dissenting_gate,)))

    runner = FlowRunner(app.get("T26_timeout_precedence_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    row = await wf_conn.fetchrow(
        f'SELECT created_at, expires_at FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',
        flow_id,
    )
    assert row is not None
    from datetime import datetime

    created = row["created_at"]
    expires = row["expires_at"]
    assert isinstance(created, datetime) and isinstance(expires, datetime)
    delta = (expires - created).total_seconds()
    assert 119.0 <= delta <= 121.0, (
        f"the row's deadline is {delta}s — the WAIT's timeout_s (120) must arm the "
        f"expiry sweep; the gate's declaration ({dissenting_gate.timeout_s}s) is the "
        "compile-visible face, never a second runtime clock"
    )


# ── C9/E13: the loop's uniform-wait law (the conditional-wait shape) ────


async def test_loop_conditional_wait_is_the_named_shape_error(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Pin (C9/E13): the loop's answer-queue cursor IS the iteration
    counter — a body that CONDITIONALLY waits (some iterations, not
    others) mis-indexes the queue SILENTLY pre-cure: the iteration
    counter advances past answers that were never consumed, the stranded
    answer can never be read again, and a RETRY of a waited iteration
    replays the WRONG slot (the retry-replay law, broken silently).

    THE CURE: the E13 RULE — the loop-kind wait site REFUSES the
    mis-aligned state (the iteration cursor PAST the answer queue) with
    the named typed error: the shape law ("ONE wait per iteration")
    gets teeth. Red-first: pre-cure the pin's loop silently minted a
    second hold and the error never raised."""

    app = WorkflowApp()

    async def conditional_wait(ctx: StepContext, carry: int) -> Done[int] | Refine[int]:
        # THE CONDITIONAL SHAPE: iteration 1 skips the wait (carry==1);
        # iteration 2 waits again — the cursor/queue mis-index.
        if carry == 1:
            return Refine(carry + 1)
        outcome = await ctx.wait_signal(Approval, timeout_s=120.0)
        match outcome:
            case Approval():
                return Refine(carry + 1)
            case Expired():
                return Done(carry)

    gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)

    @app.workflow("T26_conditional_wait_flow")
    def conditional_flow() -> Promise[object]:
        return build(
            loop(
                "review",
                conditional_wait,
                initial=0,
                max_iterations=6,
                on_exhausted="fail",
                gates=(gate,),
            )
        )

    runner = FlowRunner(app.get("T26_conditional_wait_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(run=flow_id)
    assert len(holds) == 1
    resolved = await client.resolve(holds[0].hold_id, {"verdict": "approve"})
    assert resolved.status == "delivered"
    # The loop continues: iteration 1 skips the wait, iteration 2 waits
    # — cursor 2 past a 1-answer queue: the E13 refusal, named.
    await runner.drive(flow_id, max_ticks=40)
    node_row = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE step_key = 'review' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node_row is not None
    assert node_row["status"] == "failed", (
        f"the conditional-wait loop was never refused: {dict(node_row)} — "
        "E13's teeth are gone (the mis-index is silent again)"
    )
    # The loop's failure-class rules route the body's exceptions through
    # the LOOP_BODY_FAILURE class — the E13 name rides the MESSAGE.
    assert node_row["error_class"] == "LoopBodyFailure", dict(node_row)
    assert (
        node_row["error_message"] is not None
        and "E13-uniform-loop-wait" in (node_row["error_message"])
    )
    # And the stranded answer is STILL on the rows (the row is the
    # truth — the refusal named the shape, it destroyed nothing).
    answer = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
        "AND status = 'delivered'",
        flow_id,
    )
    assert answer == 1


async def test_loop_wait_shape_error_is_the_typed_seam() -> None:
    """E13's unit face: the rule fires on the cursor-past-queue state
    ONLY for loop-kind nodes — a plain step's per-attempt cursor is
    consumption-ordered (conditional waits are legal there) and never
    sees this rule. The typed seam named: the body failure the loop's
    failure-class rules route (the same face SignalTimeoutError rides
    off the Expired member)."""
    import inspect

    from taskq.workflows.api._ctx_wait import LoopWaitShapeError

    assert issubclass(LoopWaitShapeError, RuntimeError)
    doc = inspect.getdoc(LoopWaitShapeError) or ""
    assert "E13" in doc, doc
