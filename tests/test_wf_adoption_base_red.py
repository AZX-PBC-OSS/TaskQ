"""Red-first adoption behavior pins on the unmodified base.

This file is the base-control artifact for the adoption work, not an
implementation claim. Each pin asserts the *application-visible* outcome the
adoption contract requires; on the unmodified base it fails because the
engine currently produces the documented wrong outcome (or, where a new
declaration door is the subject, because the declaration is not accepted yet).

Pins:

* #720 — a chain source whose run is cancelled while its body is running
  must not materialise a page. Today the emit fence is claim-only, so the
  cancelled source still commits children and an ``emit_cursor``.
* #721 — application writes and the emit checkpoint must be able to share a
  caller-owned asyncpg transaction. Today the emit acquires its own pooled
  connection and commits there, so a caller rollback removes the application
  row but leaves the engine page behind.
* #723 — five distinct run keys against a workflow declaring ``max_pending=4``
  must yield four live roots and one typed refusal. The first pin records the
  base (unbounded) behavior as the control; the second is the real cap pin.
* #716 — a chain step's retry policy must come from its target declaration.
  The legacy no-policy fallback (three transient attempts, 50 ms curve) is the
  control; a body handle declaring ``max_attempts=1`` must be honored instead
  of silently falling back to three.

The tests use only generic synthetic records. They fail closed on unexpected
engine state and never refer to a consumer application.
"""

# ruff: noqa: S608  # Why: the schema identifier is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import asyncio
import enum
import inspect
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.exceptions import MaxPendingExceededError
from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
    step,
)
from taskq.workflows._types import EmitChild, NodeSpec
from taskq.workflows.api._ctx import build_step_context
from taskq.workflows.api._runner_defer import WORKFLOW_CANCELLED_CLASS
from taskq.workflows.engine import insert_node
from tests._wf_fixtures import seed_flow

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

_LEASE_SECONDS = 30
_TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


class _Outcome(enum.Enum):
    OK = "ok"


async def _ok_screen(ctx: StepContext, item: object) -> _Outcome:
    return _Outcome.OK


async def _noop_step(ctx: StepContext) -> None:
    return None


def _single_page_source(chain: Chain, page: list[int]) -> Any:
    """A paged source body emitting one synthetic page through ``ctx``."""

    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [
                chain_start(
                    chain,
                    {"record_id": record_id},
                    map_index=record_id,
                    trace_id=f"record-{record_id}",
                )
                for record_id in page
            ],
            cursor={"page": 0},
        )

    return source


def _failing_body(invocations: list[int]) -> Any:
    async def body(ctx: StepContext, item: object) -> _Outcome:
        invocations.append(len(invocations) + 1)
        raise RuntimeError("synthetic chain body failure")

    return body


async def _row(
    conn: asyncpg.Connection, schema: str, flow_id: JobId | None, step_key: str
) -> asyncpg.Record | None:
    if flow_id is None:
        return None
    return await conn.fetchrow(
        f"SELECT * FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = $2",
        flow_id,
        step_key,
    )


async def _node_id(conn: asyncpg.Connection, schema: str, flow_id: JobId, step_key: str) -> JobId:
    row = await _row(conn, schema, flow_id, step_key)
    assert row is not None, f"the {step_key!r} row must exist after create"
    return JobId(row["id"])


async def _insert_source(
    conn: asyncpg.Connection, schema: str, wf_sql: WorkflowSql, flow_id: JobId
) -> JobId:
    return await insert_node(
        conn,
        wf_sql,
        NodeSpec(flow_id=flow_id, step_key="source", actor="wf", queue="default"),
    )


async def _claim_source(
    conn: asyncpg.Connection, schema: str, source_id: JobId
) -> tuple[JobId, int, int]:
    worker = JobId(new_uuid())
    rec = await conn.fetchrow(
        f"UPDATE \"{schema}\".jobs SET status = 'running', started_at = now(), "
        "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
        "locked_by_worker = $2, lock_expires_at = now() + $3::interval, "
        "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
        "AND deps_pending = 0 RETURNING attempt, claim_epoch",
        source_id,
        worker,
        timedelta(seconds=_LEASE_SECONDS),
    )
    assert rec is not None, "the source did not claim (not pending?)"
    return worker, int(rec["attempt"]), int(rec["claim_epoch"])


async def _children(
    conn: asyncpg.Connection, schema: str, source_id: JobId
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT id, step_key, status FROM "{schema}".jobs WHERE parent_id = $1',
        source_id,
    )


async def _cursor(conn: asyncpg.Connection, schema: str, source_id: JobId) -> object:
    raw = await conn.fetchval(
        f"SELECT metadata->'emit_cursor' FROM \"{schema}\".jobs WHERE id = $1",
        source_id,
    )
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


async def _repend_backoffs(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, step_key: str
) -> list[float]:
    row = await _row(conn, schema, flow_id, step_key)
    assert row is not None, f"the {step_key!r} row must exist"
    raw = row["metadata"]
    metadata: dict[str, Any] = json.loads(raw) if isinstance(raw, str) else raw
    repends: dict[str, Any] = metadata.get("wf_repend", {})
    ordered = sorted(repends.items(), key=lambda item: int(item[0]))
    return [float(value["backoff"]) for _, value in ordered]


# ── #720: a cancelled running source cannot emit a page ─────────────────


@pytest.mark.integration
async def test_720_cancelled_running_chain_source_cannot_emit(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A run cancelled while its chain-source body is running must leave no
    child rows and no cursor movement from the page the body tries to emit
    after the cancellation linearization point.

    The base defect is concrete: ``CANCEL_NODES`` keeps a running source row
    ``running`` and marks its metadata, while ``EMIT_CURSOR_SQL`` fences only
    the claim identity. The post-cancel page therefore commits children,
    edges, and the checkpoint on a run whose root already reads ``cancelled``.
    The source's own disposition is pinned too: after the fenced drive it must
    settle terminal ``cancelled`` with the workflow cancellation error class
    under the still-``cancelled`` root — never re-pended by the ladder.
    """
    suffix = new_uuid().hex[:8]
    chain = Chain(
        name=f"red-cancel-chain-{suffix}",
        start="screen",
        steps={"screen": Step(body=_ok_screen, outcomes=_Outcome, route=None)},
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    emitted: list[str] = []

    async def gated_source(ctx: StepContext) -> None:
        entered.set()
        await release.wait()
        await ctx.emit_batch(
            [chain_start(chain, {"record_id": 1}, map_index=1, trace_id="record-1")],
            cursor={"page": 0},
        )
        emitted.append("page-0")

    app = WorkflowApp()

    @app.workflow(f"red_cancel_source_{suffix}")
    def flow() -> Promise[object]:
        return build(chain_source(chain, gated_source, key="source"))

    compiled = app.get(f"red_cancel_source_{suffix}")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    claim = await runner.create_flow()
    source_id = await _node_id(wf_conn, wf_schema, claim.flow_id, "source")

    drive = asyncio.create_task(runner.tick(claim.flow_id))
    try:
        await asyncio.wait_for(entered.wait(), timeout=15)
        running = await wf_conn.fetchrow(
            f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1',
            source_id,
        )
        assert running is not None and running["status"] == "running", (
            "the source body must be running before the cancel: "
            f"{dict(running) if running is not None else None}"
        )

        await runner.cancel_workflow(claim.flow_id, reason="red-first", principal="red-first")

        root_status = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', claim.flow_id
        )
        marked = await wf_conn.fetchrow(
            f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1',
            source_id,
        )
        assert root_status == "cancelled", f"the cancel did not flip the root: {root_status!r}"
        assert marked is not None and marked["status"] == "running", (
            "the cancel is required to leave the running source under its own terminal "
            f"write; observed {dict(marked) if marked is not None else None}"
        )
        metadata = marked["metadata"]
        metadata = json.loads(metadata) if isinstance(metadata, str) else metadata
        assert metadata.get("cancel_phase") == "cooperative", (
            f"the running source was not marked with the cancel carrier: {metadata!r}"
        )

        release.set()
        await asyncio.wait_for(drive, timeout=30)
    finally:
        release.set()
        if not drive.done():
            drive.cancel()
            with pytest.raises(asyncio.CancelledError):
                await drive

    children = await _children(wf_conn, wf_schema, source_id)
    cursor = await _cursor(wf_conn, wf_schema, source_id)
    assert (emitted, children, cursor) == ([], [], None), (
        "a cancelled running chain source emitted after the cancel linearization "
        f"point: emit calls={emitted!r}, committed children="
        f"{[(str(c['id']), c['step_key']) for c in children]!r}, "
        f"cursor={cursor!r} — the run is cancelled but its domain page exists"
    )

    # The fence is not emit-only: an emit-only fix that leaves the transient
    # ladder free to re-pend the fenced body's failure would leave a live
    # ``scheduled`` row under the cancelled root. The disposition is the
    # cancelled terminal with the workflow cancellation error class, and the
    # root stays ``cancelled``.
    source_after = await _row(wf_conn, wf_schema, claim.flow_id, "source")
    assert source_after is not None, "the source row must exist after the drive"
    assert source_after["status"] == "cancelled", (
        "the cancelled running source must settle 'cancelled' (never "
        f"failed/scheduled/running/pending), got {source_after['status']!r}"
    )
    assert source_after["error_class"] == WORKFLOW_CANCELLED_CLASS, (
        "the cancelled source must carry the workflow cancellation error class "
        f"{WORKFLOW_CANCELLED_CLASS!r}, got {source_after['error_class']!r}"
    )
    root_after = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', claim.flow_id
    )
    assert root_after == "cancelled", (
        f"the root must still read cancelled after the fenced drive, got {root_after!r}"
    )


# ── #721: caller transaction and emit checkpoint are one unit ───────────


@pytest.mark.integration
async def test_721_joined_emit_rolls_back_with_caller_transaction(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """The application write and the emit checkpoint must share one
    caller-owned asyncpg transaction.

    On the base, ``emit_batch`` always acquires its own pooled connection and
    commits there. The observed corruption is therefore exactly the issue's:
    the caller rolls its application row back, while the engine's children and
    ``emit_cursor`` remain committed on another connection.

    The pre-adoption base has no joined door. When the keyword is absent, the
    fallback below deliberately exercises today's pool-owned emit so the red
    artifact shows the split-domain outcome rather than a bare ``TypeError``.
    The door is gated on genuine ``connection=`` support (signature
    inspection, not the error text): when it is really present, the pin first
    proves the joined emit is not a silent no-op — it returns one child id and
    the caller's own connection sees that one child and the ``{'page': 0}``
    checkpoint uncommitted before the rollback removes them with the
    application row. Once the joined door exists, the fallback branch is never
    reached and the pin is a real same-transaction rollback assertion.
    """
    await wf_conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{wf_schema}".red_first_domain '
        "(id integer PRIMARY KEY, note text NOT NULL)"
    )
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".red_first_domain')

    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await _insert_source(wf_conn, wf_schema, wf_sql, flow_id)
    worker, attempt, epoch = await _claim_source(wf_conn, wf_schema, source_id)
    ctx = build_step_context(
        flow_id=flow_id,
        job_id=source_id,
        node_key="source",
        attempt=attempt,
        pool=wf_pool,
        wsql=wf_sql,
        input={},
        worker_id=worker,
        claim_epoch=epoch,
    )
    children = [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"record_id": 1}},
            trace_id="record-1",
            map_index=1,
        )
    ]

    caller = await wf_pool.acquire()
    tx = caller.transaction()
    await tx.start()
    try:
        await caller.execute(
            f'INSERT INTO "{wf_schema}".red_first_domain (id, note) VALUES (1, $1)',
            "caller-owned application write",
        )
        # Genuine capability gate: the joined door is detected from the
        # signature, not from the error text.
        supports_joined = "connection" in inspect.signature(ctx.emit_batch).parameters
        joined_call: dict[str, Any] = {"connection": caller}
        joined_children: tuple[JobId, ...] | None = None
        try:
            joined_children = await ctx.emit_batch(children, cursor={"page": 0}, **joined_call)
        except TypeError as exc:
            message = str(exc)
            absence_shape = "unexpected keyword argument" in message and "'connection'" in message
            if supports_joined or not absence_shape:
                # Fail closed: a TypeError that is not exactly the documented
                # API-absence shape (or any refusal under an advertised joined
                # door) is a different defect and must not fall back silently.
                raise
            # Base doorway: the signature inspection and the call rejection
            # agree the joined door does not exist yet. Exercise the shipped
            # pool-owned emit to expose the actual split commit.
            await ctx.emit_batch(children, cursor={"page": 0})
        else:
            # The joined door is real: the emit must have written inside the
            # caller's transaction — the returned child id and the checkpoint
            # visible uncommitted on the caller's own connection before the
            # rollback. A silently no-write joined door fails here instead of
            # passing vacuously.
            assert joined_children is not None and len(joined_children) == 1, (
                f"the joined emit must return the one child id it wrote, got {joined_children!r}"
            )
            uncommitted = await _children(caller, wf_schema, source_id)
            assert len(uncommitted) == 1, (
                "the caller's same-connection read must see the one uncommitted "
                "child before the rollback, got "
                f"{[(str(c['id']), c['step_key'], c['status']) for c in uncommitted]!r}"
            )
            assert JobId(uncommitted[0]["id"]) == joined_children[0], (
                "the uncommitted child must be the returned child id: returned "
                f"{joined_children[0]!r}, read {str(uncommitted[0]['id'])!r}"
            )
            uncommitted_cursor = await _cursor(caller, wf_schema, source_id)
            assert uncommitted_cursor == {"page": 0}, (
                "the caller's same-connection read must see the uncommitted "
                f"checkpoint {{'page': 0}} before the rollback, got "
                f"{uncommitted_cursor!r}"
            )
    finally:
        await tx.rollback()
        await wf_pool.release(caller)

    domain_rows = int(
        await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".red_first_domain')
    )
    committed = await _children(wf_conn, wf_schema, source_id)
    cursor = await _cursor(wf_conn, wf_schema, source_id)
    assert (domain_rows, committed, cursor) == (0, [], None), (
        "the caller transaction rolled its application row back, but the engine "
        "page survived on a different connection: "
        f"domain_rows={domain_rows}, committed_children="
        f"{[(str(c['id']), c['step_key']) for c in committed]!r}, cursor={cursor!r}"
    )


# ── #723: producer admission is a real cap, not a declaration echo ──────


@pytest.mark.integration
async def test_723_unbounded_control_five_distinct_run_keys_create_five_roots(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Control: with no admission declaration, five distinct run keys create
    five live roots. This proves the cap pin below is not passing because the
    create path is inert on this shape.
    """
    suffix = new_uuid().hex[:8]
    name = f"red_unbounded_{suffix}"
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(step(_noop_step, key="work"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    claims = [await runner.create_flow(run_key=f"baseline:{index}") for index in range(5)]
    assert all(claim.created for claim in claims), (
        f"the unbounded control did not create every run: {[c.created for c in claims]!r}"
    )
    assert len({claim.flow_id for claim in claims}) == 5, "the five run keys must be five runs"

    roots = await wf_conn.fetch(
        f'SELECT id, status::text AS status FROM "{wf_schema}".jobs '
        "WHERE step_key = '__flow__' AND idempotency_scope = $1",
        f"workflow-run:{name}",
    )
    assert len(roots) == 5, f"the unbounded control must create five roots, got {len(roots)}"
    assert all(root["status"] not in _TERMINAL for root in roots), (
        f"the control's roots must be live: {[root['status'] for root in roots]!r}"
    )


@pytest.mark.integration
async def test_723_max_pending_four_refuses_the_fifth_distinct_run_key(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A workflow declaring ``max_pending=4`` admits four distinct run keys
    and refuses the fifth with the typed producer backpressure error before
    any fifth root or node row is written. The cap counts live roots, not a
    historical monotonic row counter: once one admitted run is driven to
    success, a further distinct run key is admitted again.
    """
    suffix = new_uuid().hex[:8]
    name = f"red_capped_{suffix}"
    app = WorkflowApp()
    declaration: dict[str, Any] = {"max_pending": 4}

    @app.workflow(name, **declaration)
    def flow() -> Promise[object]:
        return build(step(_noop_step, key="work"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    for index in range(4):
        claim = await runner.create_flow(run_key=f"admitted:{index}")
        assert claim.created, f"run key admitted:{index} was not created: {claim!r}"

    with pytest.raises(MaxPendingExceededError) as caught:
        await runner.create_flow(run_key="admitted:4")

    assert caught.value.max_pending == 4, (
        f"the refusal must name the declared cap 4, got {caught.value.max_pending!r}"
    )
    assert caught.value.current_count == 4, (
        f"the refusal must report four live roots, got {caught.value.current_count!r}"
    )
    assert caught.value.actor == f"workflow:{name}", (
        f"the refusal must name the workflow admission cohort, got {caught.value.actor!r}"
    )

    roots = await wf_conn.fetch(
        f'SELECT id, idempotency_key, status::text AS status FROM "{wf_schema}".jobs '
        "WHERE step_key = '__flow__' AND idempotency_scope = $1 ORDER BY idempotency_key",
        f"workflow-run:{name}",
    )
    assert len(roots) == 4, f"the cap admitted {len(roots)} roots instead of four"
    keys = [root["idempotency_key"] for root in roots]
    assert keys == [f"admitted:{index}" for index in range(4)], (
        f"the admitted roots are not exactly the first four keys: {keys!r}"
    )
    assert "admitted:4" not in keys, "the refused fifth key wrote a root"
    assert all(root["status"] not in _TERMINAL for root in roots), (
        f"all four admitted roots must remain live: {[root['status'] for root in roots]!r}"
    )

    nodes = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = ANY($1::uuid[]) AND step_key = 'work'",
        [root["id"] for root in roots],
    )
    assert int(nodes) == 4, f"the refused fifth create leaked node rows: work rows={nodes}"

    # The cap is a live-root cap, never a historical monotonic row counter:
    # release one cap slot by driving an admitted run to success, then a
    # further distinct run key must be admitted.
    driven_root = JobId(roots[0]["id"])
    assert await runner.drive(driven_root) == "terminal", (
        "the admitted no-op run must reach a terminal verdict to release its cap slot"
    )
    driven_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', driven_root
    )
    assert driven_status == "succeeded", (
        f"the driven admitted root must read succeeded, got {driven_status!r}"
    )

    fifth = await runner.create_flow(run_key="admitted:5")
    assert fifth.created, (
        "the fifth distinct run key must be admitted once one live root went "
        "terminal — a live-root cap admits it, a historical monotonic row "
        f"counter would refuse it: {fifth!r}"
    )

    final_roots = await wf_conn.fetch(
        f'SELECT id, idempotency_key, status::text AS status FROM "{wf_schema}".jobs '
        "WHERE step_key = '__flow__' AND idempotency_scope = $1 ORDER BY idempotency_key",
        f"workflow-run:{name}",
    )
    assert [root["idempotency_key"] for root in final_roots] == [
        "admitted:0",
        "admitted:1",
        "admitted:2",
        "admitted:3",
        "admitted:5",
    ], (
        "the released cap must admit exactly the new admitted:5 root: "
        f"{[root['idempotency_key'] for root in final_roots]!r}"
    )
    statuses = [root["status"] for root in final_roots]
    assert len(statuses) == 5, f"expected five total roots, got {len(statuses)}"
    assert statuses.count("succeeded") == 1, (
        f"exactly the driven root must be terminal-succeeded, got {statuses!r}"
    )
    assert len([status for status in statuses if status not in _TERMINAL]) == 4, (
        f"the cap must hold exactly four live roots after the release-and-admit, got {statuses!r}"
    )

    final_nodes = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = ANY($1::uuid[]) AND step_key = 'work'",
        [root["id"] for root in final_roots],
    )
    assert int(final_nodes) == 5, (
        f"the admitted fifth run must carry its own work node: work rows={final_nodes}"
    )


# ── #716: chain policy comes from the target declaration ────────────────


@pytest.mark.integration
async def test_716_authored_chain_retry_policy_replaces_the_fixed_fallback(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Legacy absent-policy chain steps keep the documented three-attempt,
    50 ms exponential fallback. A chain step whose body handle declares
    ``max_attempts=1`` must use that authored policy instead: one failed
    attempt, one failed ledger row, and a row stamped ``max_attempts=1``.
    """
    suffix = new_uuid().hex[:8]
    legacy_invocations: list[int] = []
    authored_invocations: list[int] = []
    app = WorkflowApp()

    legacy_chain = Chain(
        name=f"red-legacy-chain-{suffix}",
        start="legacy_screen",
        steps={
            "legacy_screen": Step(
                body=_failing_body(legacy_invocations), outcomes=_Outcome, route=None
            )
        },
    )
    authored_handle = app.actor(name=f"red-authored-actor-{suffix}", max_attempts=1)(
        _failing_body(authored_invocations)
    )
    authored_chain = Chain(
        name=f"red-authored-chain-{suffix}",
        start="authored_screen",
        steps={"authored_screen": Step(body=authored_handle, outcomes=_Outcome, route=None)},
    )

    @app.workflow(f"red_chain_legacy_{suffix}")
    def legacy_flow() -> Promise[object]:
        return build(
            chain_source(legacy_chain, _single_page_source(legacy_chain, [1]), key="source")
        )

    @app.workflow(f"red_chain_authored_{suffix}")
    def authored_flow() -> Promise[object]:
        return build(
            chain_source(authored_chain, _single_page_source(authored_chain, [1]), key="source")
        )

    legacy_runner = FlowRunner(app.get(f"red_chain_legacy_{suffix}"), wf_pool, wf_schema)
    legacy_claim = await legacy_runner.create_flow()
    assert await legacy_runner.drive(legacy_claim.flow_id) == "terminal", (
        "the legacy failing chain must reach a terminal verdict"
    )

    legacy_row = await _row(wf_conn, wf_schema, legacy_claim.flow_id, "legacy_screen")
    assert legacy_row is not None, "the legacy chain row must exist"
    assert legacy_row["status"] == "failed", (
        f"the legacy chain row must fail loudly, got {legacy_row['status']!r}"
    )
    assert legacy_row["attempt"] == 3, (
        "the legacy absent-policy fallback must remain exactly three attempts, "
        f"got {legacy_row['attempt']!r}"
    )
    assert legacy_row["max_attempts"] == 3, (
        f"the legacy row must keep the fallback row stamp 3, got {legacy_row['max_attempts']!r}"
    )
    assert legacy_row["retry_kind"] == "transient", (
        f"the legacy fallback kind must stay transient, got {legacy_row['retry_kind']!r}"
    )
    assert len(legacy_invocations) == 3, (
        f"the legacy body must run exactly three attempts, ran {len(legacy_invocations)}"
    )
    legacy_failed_ledger = int(
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'legacy_screen' AND status = 'failed'",
            legacy_claim.flow_id,
        )
    )
    assert legacy_failed_ledger == 3, (
        f"the legacy fallback must record three failed ledger terminals, got {legacy_failed_ledger}"
    )
    assert await _repend_backoffs(wf_conn, wf_schema, legacy_claim.flow_id, "legacy_screen") == [
        pytest.approx(0.05),
        pytest.approx(0.1),
    ], "the legacy absent-policy curve must remain the fixed 50 ms exponential sequence"

    authored_runner = FlowRunner(app.get(f"red_chain_authored_{suffix}"), wf_pool, wf_schema)
    authored_claim = await authored_runner.create_flow()
    assert await authored_runner.drive(authored_claim.flow_id) == "terminal", (
        "the authored failing chain must reach a terminal verdict"
    )

    authored_row = await _row(wf_conn, wf_schema, authored_claim.flow_id, "authored_screen")
    assert authored_row is not None, "the authored chain row must exist"
    assert authored_row["status"] == "failed", (
        f"the authored chain row must fail loudly, got {authored_row['status']!r}"
    )
    authored_failed_ledger = int(
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'authored_screen' AND status = 'failed'",
            authored_claim.flow_id,
        )
    )
    authored_observed = (
        int(authored_row["max_attempts"]),
        int(authored_row["attempt"]),
        len(authored_invocations),
        authored_failed_ledger,
    )
    assert authored_observed == (1, 1, 1, 1), (
        "the authored body handle's max_attempts=1 did not replace the fixed "
        "fallback (row max_attempts, row attempt, body invocations, failed "
        f"ledger rows) — expected (1, 1, 1, 1), got {authored_observed!r}"
    )
