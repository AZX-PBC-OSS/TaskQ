"""T26 — THE HITL BROADCAST'S PINS: the transactional NOTIFY legs, the
zero-window backfill, the resolve wake, the expiry fail-close, the
escalation interplay.

The red-first evidence: P1's convicted variant (the knock sent OUTSIDE
the transaction — a rolled-back hold-create that still broadcasts) is
captured as the mutation drill through ``hitl_redlog``; the shipped
legs are transactional by construction (``pg_notify`` inside the
caller's tx — PG delivers on commit only).

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
    HitlClient,
    register_hold,
    sweep_expired_signals,
)
from taskq.workflows.api._hitl_listen import (
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

    listener = HitlListener(wf_pool, schema=wf_schema)
    async with listener:
        # THE ZERO WINDOW: the hold existed BEFORE the listener — the
        # FIRST event is its backfilled HoldCreated.
        first = await asyncio.wait_for(listener.events().__anext__(), timeout=5.0)
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
                case HoldResolved() | HoldExpired():
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
                case HoldCreated() | HoldExpired():
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
