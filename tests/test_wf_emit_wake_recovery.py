# ruff: noqa: S608, ASYNC109  # Why: fixture schemas are validated identifiers; the transparent asyncpg proxy mirrors timeout-taking calls.
"""Real-PostgreSQL wake, cancellation, identity, and multi-source #718 controls."""

from __future__ import annotations

import asyncio
import enum
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any, Never, cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
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
)
from taskq.workflows._emit import EmitBackpressure, wake_emit_backpressure
from taskq.workflows._sweep import reap_phantom_ledger
from taskq.workflows.api._attempt import AttemptClaim, repend_attempt
from taskq.workflows.api._runner_defer import defer_row_law
from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE, render_sql
from taskq.workflows.chain import DONE, Route
from tests.test_wf_emit_parking_controls import (
    _await_source_status,
    _chain,
    _cursor,
    _park_is_live,
    _source_body,
    _source_state,
)
from tests.test_wf_source_attempts import _claimed, _next_claim

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy
    from asyncpg.transaction import Transaction

    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.api._app import CompiledWorkflow


_STATE_POLL_S = 0.02
_WAIT_S = 10.0


def _plan_document(raw: object) -> list[dict[str, object]]:
    decoded: object = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, list) and len(decoded) == 1
    root = decoded[0]
    assert isinstance(root, dict) and isinstance(root.get("Plan"), dict)
    return [cast("dict[str, object]", root)]


def _plan_nodes(document: list[dict[str, object]]) -> list[dict[str, object]]:
    """Flatten the real PostgreSQL plan; no pruning/censoring of its content."""
    result: list[dict[str, object]] = []

    def visit(node: object) -> None:
        if not isinstance(node, dict):
            return
        typed = cast("dict[str, object]", node)
        result.append(typed)
        children = typed.get("Plans")
        if isinstance(children, list):
            for child in children:
                visit(cast("object", child))

    visit(document[0]["Plan"])
    return result


def _nonnegative_metric(record: dict[str, object], key: str) -> float:
    value = record.get(key)
    assert isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0, {
        key: value
    }
    return float(value)


def _actual_root_rows(document: list[dict[str, object]]) -> int:
    root = document[0]["Plan"]
    assert isinstance(root, dict)
    value = cast("dict[str, object]", root).get("Actual Rows")
    assert (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and value >= 0
        and float(value).is_integer()
    ), value
    return int(value)


async def _source_id(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, key: str = "source"
) -> JobId:
    value = await conn.fetchval(
        f'SELECT id FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        key,
    )
    assert isinstance(value, UUID)
    return JobId(value)


def _metadata(raw: object) -> dict[str, object]:
    decoded: object = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return cast("dict[str, object]", decoded)


async def _park_primitive_claim(
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    claim: AttemptClaim,
    *,
    logical_attempt: int,
    page_width: int = 2,
    bound: int = 2,
    outstanding: int | None = None,
    flow_id: JobId | None = None,
):
    verdict = await defer_row_law(
        wf_pool,
        wf_sql,
        job_id=claim.job_id,
        worker_id=claim.worker_id,
        delay=timedelta(0),
        attempt=claim.attempt,
        claim_epoch=claim.claim_epoch,
        disposition="emit_backpressure",
        signal_name="EmitBackpressure",
        ledger_id=claim.ledger_id,
        ledger_claimed_at=claim.ledger_claimed_at,
        backpressure=EmitBackpressure(
            page_width=page_width,
            bound=bound,
            outstanding=bound if outstanding is None else outstanding,
        ),
        source_logical_attempt=logical_attempt,
        flow_id=flow_id,
    )
    assert verdict.verdict == "deferred"
    return verdict.intent_created


@pytest.mark.integration
async def test_cancel_while_parked_is_terminal_fenced_and_never_reenters(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    suffix = new_uuid().hex
    gate = asyncio.Event()
    page_zero = asyncio.Event()
    child_zero = asyncio.Event()
    child_one = asyncio.Event()
    source_attempts: list[tuple[int, int]] = []
    child_calls: list[int | None] = []

    class Outcome(enum.Enum):
        OK = "ok"

    async def screen(ctx: StepContext, item: object) -> Outcome:
        child_calls.append(ctx.map_index)
        (child_zero if ctx.map_index == 0 else child_one).set()
        await gate.wait()
        return Outcome.OK

    chain = _chain(f"parking-cancel-chain-{suffix}", screen)
    app = WorkflowApp()

    @app.workflow(f"parking_cancel_{suffix}", max_in_flight=2)
    def flow() -> Promise[object]:
        return build(
            chain_source(
                chain,
                _source_body(chain, ((0, 1), (2, 3)), source_attempts, page_zero_emitted=page_zero),
                key="source",
                max_attempts=2,
            )
        )

    runner = FlowRunner(app.get(f"parking_cancel_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    source_task = asyncio.create_task(runner.tick(flow_id))
    child_tasks: list[asyncio.Task[bool]] = []
    try:
        await asyncio.wait_for(page_zero.wait(), _WAIT_S)
        child_tasks.append(asyncio.create_task(runner.tick(flow_id)))
        await asyncio.wait_for(child_zero.wait(), _WAIT_S)
        child_tasks.append(asyncio.create_task(runner.tick(flow_id)))
        await asyncio.wait_for(child_one.wait(), _WAIT_S)
        parked = await _await_source_status(
            wf_conn, wf_schema, flow_id, source_id, "source", _park_is_live, deadline_s=_WAIT_S
        )
        assert _park_is_live(parked), parked
        await asyncio.wait_for(source_task, _WAIT_S)
        at_cancel_attempts = list(source_attempts)
        await runner.cancel_workflow(flow_id, reason="test parked source cancel")

        cancelled = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
        assert (cancelled["root_status"], cancelled["status"]) == ("cancelled", "cancelled")
        assert cancelled["attempt"] == 0
        assert cancelled["locked_by_worker"] is None
        assert cancelled["ledger_status"] == "fenced"
        assert cancelled["ledger_attempt"] == 1
        assert isinstance(cancelled["ledger_id"], UUID)
        assert cancelled["ledger_job_id"] == source_id
        assert cancelled["metadata"]["wf_source_logical_attempt"] == 1
        cooperative_children = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger AS l '
            f'JOIN "{wf_schema}".jobs AS j ON j.id = l.job_id '
            "WHERE j.parent_id = $1 AND l.status = 'running'",
            source_id,
        )
        assert cooperative_children == 2
        # THE REAPER'S LIVE-CLAIM EXCLUSION: the cooperative children are
        # LIVE claimed attempts (their bodies hold the gate; their leases
        # hold) — their own cooperative-cancel dispositions write their own
        # truthful ledger terminals; the reaper must not fence them (a
        # fenced record over a heartbeating attempt is the ledger lie, and
        # it would refuse the attempt's own truthful write).
        assert await reap_phantom_ledger(wf_pool, wf_sql) == 0
        event_count = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_events '
            "WHERE job_id = $1 AND kind = 'state_change' "
            "AND detail->>'to_state' = 'cancelled'",
            source_id,
        )
        assert event_count == 1
        # A refunded parked source has no terminal body-attempt row; its state
        # transition and fenced current ledger are the truthful audit record.
        assert (
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".job_attempts WHERE job_id = $1', source_id
            )
            == 0
        )
        for _ in range(3):
            await runner.tick(flow_id)
            await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
        final = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
        assert source_attempts == at_cancel_attempts == [(0, 1), (1, 1)]
        assert final["status"] == "cancelled"
        assert final["metadata"]["wf_emit_backpressure"]["wakes"] == 0  # type: ignore[index]
        assert await _cursor(wf_conn, wf_schema, source_id) == {"page": 0}
        assert child_calls == [0, 1]
    finally:
        gate.set()
        tasks = [source_task, *child_tasks]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class _DelayedTransaction:
    def __init__(self, owner: _DelayedCommitConnection, transaction: Transaction) -> None:
        self.owner = owner
        self.transaction = transaction

    async def __aenter__(self) -> _DelayedTransaction:
        await self.transaction.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.owner.delay_commit and exc_type is None:
            self.owner.pool.commits_delayed += 1
            self.owner.pool.terminal_ready.set()
            await self.owner.pool.allow_commit.wait()
        await self.transaction.__aexit__(exc_type, exc, traceback)


class _DelayedCommitConnection:
    def __init__(
        self,
        pool: _DelayedCommitPool,
        conn: asyncpg.Connection | PoolConnectionProxy[asyncpg.Record],
    ) -> None:
        self.pool = pool
        self.conn = conn
        self.delay_commit = False

    async def fetchrow(self, query: str, *args: object, timeout: float | None = None) -> Any:
        if query == self.pool.wf_sql.terminal_mark and args[0] == self.pool.blocked_child_id:
            self.delay_commit = True
        return await self.conn.fetchrow(query, *args, timeout=timeout)

    def transaction(self, **kwargs: object) -> _DelayedTransaction:
        transaction = self.conn.transaction(**kwargs)  # type: ignore[arg-type]
        return _DelayedTransaction(self, transaction)

    def __getattr__(self, name: str) -> Any:
        # Explicit real-connection delegation; no fabricated values/results.
        return getattr(self.conn, name)


class _DelayedCommitAcquire:
    def __init__(self, pool: _DelayedCommitPool) -> None:
        self.pool = pool
        self.conn: PoolConnectionProxy[asyncpg.Record] | None = None

    async def __aenter__(self) -> _DelayedCommitConnection:
        self.conn = await self.pool.pool.acquire()
        return _DelayedCommitConnection(self.pool, self.conn)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.conn is not None:
            await self.pool.pool.release(self.conn)
            self.conn = None


class _DelayedCommitPool:
    def __init__(self, pool: asyncpg.Pool, wf_sql: WorkflowSql, blocked_child_id: JobId) -> None:
        self.pool = pool
        self.wf_sql = wf_sql
        self.blocked_child_id = blocked_child_id
        self.terminal_ready = asyncio.Event()
        self.allow_commit = asyncio.Event()
        self.commits_delayed = 0

    def acquire(self, *, timeout: float | None = None) -> _DelayedCommitAcquire:
        return _DelayedCommitAcquire(self)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.pool, name)


@pytest.mark.integration
async def test_uncommitted_terminal_cannot_hide_from_both_commit_acknowledged_reads(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The last child terminal is uncommitted while the source parks/rechecks.

    Releasing the already-written COMMIT afterwards must wake the durable park;
    neither side can form the old pre-commit-trigger lost-wake interleaving.
    """
    suffix = new_uuid().hex
    allow_page_one = asyncio.Event()
    child_gate = asyncio.Event()
    child_one_started = asyncio.Event()
    source_attempts: list[tuple[int, int]] = []

    async def screen(ctx: StepContext, item: object) -> enum.Enum:
        if ctx.map_index == 1:
            child_one_started.set()
            await child_gate.wait()
        return _Outcome.OK

    class _Outcome(enum.Enum):
        OK = "ok"

    chain = _chain(f"commit-race-chain-{suffix}", screen)
    app = WorkflowApp()

    @app.workflow(f"commit_race_{suffix}", max_in_flight=2)
    def flow() -> Promise[object]:
        return build(
            chain_source(
                chain,
                _source_body(
                    chain,
                    ((0, 1), (2, 3)),
                    source_attempts,
                    wait_before_page_one=allow_page_one,
                ),
                key="source",
                max_attempts=2,
            )
        )

    compiled = app.get(f"commit_race_{suffix}")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    child_rows = await wf_conn.fetch(
        f'SELECT id FROM "{wf_schema}".jobs WHERE parent_id = $1 ORDER BY map_index',
        source_id,
    )
    # Page rows are committed before either child claims.
    source_task = asyncio.create_task(runner.tick(flow_id))
    child_tasks: list[asyncio.Task[bool]] = []
    delayed_pool: _DelayedCommitPool | None = None
    try:
        # The source emitted page zero and then deliberately waits before its
        # page-one admission; this lets us hold the last child uncommitted first.
        while not child_rows:
            child_rows = await wf_conn.fetch(
                f'SELECT id FROM "{wf_schema}".jobs WHERE parent_id = $1 ORDER BY map_index',
                source_id,
            )
            await asyncio.sleep(_STATE_POLL_S)
        # Both real direct claims race independently: the first terminal can
        # commit while the second occupies the last unit and pauses between its
        # terminal statement and COMMIT.
        delayed_pool = _DelayedCommitPool(wf_pool, wf_sql, JobId(child_rows[1]["id"]))
        delayed_runner = FlowRunner(compiled, cast("asyncpg.Pool", delayed_pool), wf_schema)
        child_tasks.append(asyncio.create_task(runner.tick(flow_id)))
        child_tasks.append(asyncio.create_task(delayed_runner.tick(flow_id)))
        await asyncio.wait_for(child_one_started.wait(), _WAIT_S)
        child_gate.set()
        await asyncio.wait_for(delayed_pool.terminal_ready.wait(), _WAIT_S)
        allow_page_one.set()
        # THE SERIALIZED PARK (the canonical root-first order, R2/A9.2):
        # the page-one admission refused while the second child's terminal
        # was uncommitted, but the PARK TRANSACTION now queues at the flow
        # root's own row lock — held by the delayed child's own tx1 — so
        # the park CANNOT interleave with the uncommitted terminal: the
        # old pre-commit lost-wake interleaving is impossible by
        # construction, and the park's own observation never sees an
        # uncommitted sibling.
        keyed_sql = """
        SELECT DISTINCT l.pid, l.transactionid::text AS holder_xid,
               a.wait_event_type
        FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.locktype = 'transactionid'
          AND NOT l.granted
          AND l.pid <> pg_backend_pid()
          AND a.datname = current_database()
        """
        loop_deadline = asyncio.get_running_loop().time() + _WAIT_S
        holder_xid = ""
        while asyncio.get_running_loop().time() < loop_deadline:
            waiters = await wf_conn.fetch(keyed_sql)
            if waiters:
                holder_xid = str(waiters[0]["holder_xid"])
                break
            await asyncio.sleep(_STATE_POLL_S)
        else:
            pytest.fail("the park never queued at the root the uncommitted terminal holds")
        # THE HOLDER IS THE DELAYED CHILD'S OWN BACKEND: the keyed
        # transactionid's GRANTED row names it, and its pid is distinct
        # from this control connection's.
        holder_rows = await wf_conn.fetch(
            """SELECT pid FROM pg_locks
               WHERE locktype = 'transactionid' AND granted
                 AND transactionid::text = $1""",
            holder_xid,
        )
        assert holder_rows, "the held root's own transaction must be the keyed holder"
        # While queued, the park has written NOTHING: the source is still
        # 'running' with no marker (the durable record cannot interleave).
        parked_early = await wf_conn.fetchrow(
            f'SELECT status::text AS status, metadata FROM "{wf_schema}".jobs WHERE id = $1',
            source_id,
        )
        assert parked_early is not None
        assert parked_early["status"] == "running", dict(parked_early)
        assert "blocking_reason" not in _metadata(parked_early["metadata"]), dict(parked_early)
        # THE RELEASE: the child's terminal commits; the park lands AFTER
        # it and its own observation sees the COMMITTED terminal (charge
        # zero) — the waiter fits, the intent is created, and the runner's
        # own post-commit drain promotes the source.
        delayed_pool.allow_commit.set()
        await asyncio.wait_for(source_task, _WAIT_S)
        final_child_status = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', child_rows[1]["id"]
        )
        assert final_child_status == "succeeded"
        await asyncio.gather(*child_tasks)
        # The parked-then-promoted source: the durable park committed (its
        # own record) and the promotion woke it — the commit-acknowledged
        # reads never lost the wake.
        woken = await _await_source_status(
            wf_conn,
            wf_schema,
            flow_id,
            source_id,
            "source",
            lambda state: state["status"] == "pending",
            deadline_s=_WAIT_S,
        )
        assert woken["blocking_reason"] is None, woken
        assert woken["metadata"]["wf_emit_backpressure"]["wakes"] == 1  # type: ignore[index]
        assert await runner.drive(flow_id) == "terminal"
        assert delayed_pool.commits_delayed == 1
        assert source_attempts == [(0, 1), (1, 1), (1, 1)]
        children = await wf_conn.fetch(
            f'SELECT map_index, status::text AS status FROM "{wf_schema}".jobs '
            "WHERE parent_id = $1 ORDER BY map_index",
            source_id,
        )
        assert [row["map_index"] for row in children] == [0, 1, 2, 3]
        assert {row["status"] for row in children} == {"succeeded"}
    finally:
        allow_page_one.set()
        child_gate.set()
        if delayed_pool is not None:
            delayed_pool.allow_commit.set()
        tasks = [source_task, *child_tasks]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.integration
async def test_divergent_physical_composition_park_and_diagnostic_fold_stability(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """Carrier + divergent physical + genuine failure => 2, never physical+1."""
    flow_id, claim = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )
    for _ in range(2):
        async with wf_conn.transaction():
            verdict = await repend_attempt(
                wf_conn,
                wf_sql,
                claim=claim,
                error_class="SyntheticInfra",
                error_message="synthetic machinery recovery",
                failures_before=0,
                max_attempts=20,
                retry_kind="transient",
                backoff_s=0,
                machinery_crashed=True,
                chain_source=True,
            )
        assert verdict == "repended"
        claim = await _next_claim(wf_conn, wf_schema, wf_sql, claim)
    assert claim.attempt == 3
    async with wf_conn.transaction():
        verdict = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="SyntheticSourceFailure",
            error_message="one real source failure after divergent machinery claims",
            failures_before=0,
            max_attempts=20,
            retry_kind="transient",
            backoff_s=0,
            machinery_crashed=False,
            chain_source=True,
        )
    assert verdict == "repended"
    row = await wf_conn.fetchrow(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', claim.job_id
    )
    assert row is not None and _metadata(row["metadata"])["wf_source_logical_attempt"] == 2
    claim = await _next_claim(wf_conn, wf_schema, wf_sql, claim)
    assert claim.attempt == 4

    control = EmitBackpressure(page_width=2, bound=2, outstanding=2)
    _park_outcome = await defer_row_law(
        wf_pool,
        wf_sql,
        job_id=claim.job_id,
        worker_id=claim.worker_id,
        delay=timedelta(0),
        attempt=claim.attempt,
        claim_epoch=claim.claim_epoch,
        disposition="emit_backpressure",
        signal_name="EmitBackpressure",
        ledger_id=claim.ledger_id,
        ledger_claimed_at=claim.ledger_claimed_at,
        backpressure=control,
        source_logical_attempt=2,
        flow_id=flow_id,
    )
    assert _park_outcome.verdict == "deferred"
    parked = await wf_conn.fetchrow(
        f'SELECT status, scheduled_at, attempt, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        claim.job_id,
    )
    assert parked is not None
    assert parked["status"] == "scheduled" and parked["scheduled_at"] is not None
    assert parked["attempt"] == 0
    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
    assert wake.promoted == 1
    physical_claim = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), claim.job_id, claim.worker_id
    )
    assert physical_claim is not None
    physical = int(physical_claim["attempt"])
    physical_epoch = int(physical_claim["claim_epoch"])
    assert physical == 1
    ledger = await wf_conn.fetchrow(
        wf_sql.ledger_claim,
        new_uuid(),
        claim.flow_id,
        claim.job_id,
        claim.step_key,
        claim.map_index,
        physical,
        physical_epoch,
    )
    assert ledger is not None
    persisted_ledger = await wf_conn.fetchrow(
        f'SELECT id, status, updated_at FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        ledger["id"],
    )
    assert persisted_ledger is not None
    assert persisted_ledger["updated_at"] == ledger["updated_at"], {
        "returned": dict(ledger),
        "persisted": dict(persisted_ledger),
    }
    claim = replace(
        claim,
        attempt=physical,
        claim_epoch=physical_epoch,
        ledger_id=JobId(ledger["id"]),
        ledger_claimed_at=persisted_ledger["updated_at"],
    )
    before = await wf_conn.fetchrow(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', claim.job_id
    )
    assert before is not None
    before_metadata = _metadata(before["metadata"])
    folded_before = before_metadata.get("wf_repend_folded", 0)
    stale_job = await wf_conn.fetchrow(
        f"SELECT status, attempt, claim_epoch, locked_by_worker, cancel_phase, metadata "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        claim.job_id,
    )
    stale_ledger = await wf_conn.fetchrow(
        f'SELECT id, status, updated_at FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    async with wf_conn.transaction():
        reused_verdict = await repend_attempt(
            wf_conn,
            wf_sql,
            claim=claim,
            error_class="SyntheticSourceFailure",
            error_message="genuine failure after physical key reuse",
            failures_before=1,
            max_attempts=20,
            retry_kind="transient",
            backoff_s=0,
            chain_source=True,
        )
    if reused_verdict != "repended":
        raise AssertionError(
            json.dumps(
                {
                    "claim": str(claim),
                    "job": None if stale_job is None else dict(stale_job),
                    "ledger": None if stale_ledger is None else dict(stale_ledger),
                    "verdict": reused_verdict,
                },
                default=str,
                indent=2,
            )
        )
    after = await wf_conn.fetchrow(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', claim.job_id
    )
    assert after is not None
    after_metadata = _metadata(after["metadata"])
    assert after_metadata["wf_source_logical_attempt"] == 3
    assert after_metadata.get("wf_repend_folded", 0) == folded_before


@pytest.mark.integration
async def test_two_sources_park_without_cross_cursor_or_sibling_deadlock(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    suffix = new_uuid().hex
    gate_a = asyncio.Event()
    gate_b = asyncio.Event()
    observed_a: list[tuple[int, int]] = []
    observed_b: list[tuple[int, int]] = []

    async def body_a(ctx: StepContext, item: object) -> enum.Enum:
        await gate_a.wait()
        return _MultiOutcome.OK

    async def body_b(ctx: StepContext, item: object) -> enum.Enum:
        await gate_b.wait()
        return _MultiOutcome.OK

    class _MultiOutcome(enum.Enum):
        OK = "ok"

    chain_a = Chain(
        name=f"multi-a-{suffix}",
        start="a_screen",
        steps={
            "a_screen": Step(
                body=body_a, outcomes=_MultiOutcome, route=Route({_MultiOutcome.OK: DONE})
            )
        },
    )
    chain_b = Chain(
        name=f"multi-b-{suffix}",
        start="b_screen",
        steps={
            "b_screen": Step(
                body=body_b, outcomes=_MultiOutcome, route=Route({_MultiOutcome.OK: DONE})
            )
        },
    )
    source_a = _source_body(chain_a, ((0, 1), (2, 3)), observed_a)
    source_b = _source_body(chain_b, ((10, 11), (12, 13)), observed_b)
    app = WorkflowApp()

    @app.workflow(f"multi_source_park_{suffix}", max_in_flight=2)
    def flow() -> Promise[object]:
        pa = chain_source(chain_a, source_a, key="source_a", max_attempts=2)
        pb = chain_source(chain_b, source_b, key="source_b", max_attempts=2)
        return build(pa, cast("Promise[Never]", pb))

    runner = FlowRunner(app.get(f"multi_source_park_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_a_id = await _source_id(wf_conn, wf_schema, flow_id, "source_a")
    source_b_id = await _source_id(wf_conn, wf_schema, flow_id, "source_b")
    try:
        # One initial dispatch pass parks both multi-page full-width sources.
        # No second pass is allowed to enter their still-gated children.
        await asyncio.wait_for(runner.tick(flow_id), timeout=_WAIT_S)
        parked_a = await _source_state(wf_conn, wf_schema, flow_id, source_a_id, "source_a")
        parked_b = await _source_state(wf_conn, wf_schema, flow_id, source_b_id, "source_b")
        assert _park_is_live(parked_a) and _park_is_live(parked_b)
        assert parked_a["metadata"]["wf_source_logical_attempt"] == 1
        assert parked_b["metadata"]["wf_source_logical_attempt"] == 1
        gate_a.set()
        gate_b.set()
        drive_result = await runner.drive(flow_id, max_ticks=250)
        assert drive_result == "terminal", {
            "drive": drive_result,
            "source_a": await _source_state(wf_conn, wf_schema, flow_id, source_a_id, "source_a"),
            "source_b": await _source_state(wf_conn, wf_schema, flow_id, source_b_id, "source_b"),
            "observed_a": observed_a,
            "observed_b": observed_b,
        }
        all_children = await wf_conn.fetch(
            f"SELECT step_key, map_index, idempotency_key, status::text AS status FROM "
            f'"{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND map_index IS NOT NULL "
            "ORDER BY step_key, map_index",
            flow_id,
        )
        assert [(row["step_key"], row["map_index"]) for row in all_children] == [
            ("a_screen", 0),
            ("a_screen", 1),
            ("a_screen", 2),
            ("a_screen", 3),
            ("b_screen", 10),
            ("b_screen", 11),
            ("b_screen", 12),
            ("b_screen", 13),
        ]
        assert len({row["idempotency_key"] for row in all_children}) == 8
        assert {row["status"] for row in all_children} == {"succeeded"}
        assert {attempt for _page, attempt in observed_a + observed_b} == {1}
        assert await _cursor(wf_conn, wf_schema, source_a_id) == {"page": 1}
        assert await _cursor(wf_conn, wf_schema, source_b_id) == {"page": 1}
    finally:
        gate_a.set()
        gate_b.set()


@pytest.mark.integration
async def test_phantom_reap_covers_awaited_and_crashed_but_retains_live_root(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    entries: list[tuple[JobId, AttemptClaim]] = []
    for _ in range(3):
        entries.append(
            await _claimed(wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1})
        )
    for entry_flow, claim in entries:
        await _park_primitive_claim(wf_pool, wf_sql, claim, logical_attempt=1, flow_id=entry_flow)
    awaited_flow, awaited_claim = entries[0]
    crashed_flow, crashed_claim = entries[1]
    _live_flow, live_claim = entries[2]
    for flow_id in (awaited_flow, crashed_flow):
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now() WHERE id = $1",
            flow_id,
        )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".wf_step_ledger SET status = 'crashed' WHERE id = $1",
        crashed_claim.ledger_id,
    )
    assert await reap_phantom_ledger(wf_pool, wf_sql) == 2
    statuses = await wf_conn.fetch(
        f'SELECT id, status::text AS status FROM "{wf_schema}".wf_step_ledger '
        "WHERE id = ANY($1::uuid[]) ORDER BY id",
        [claim.ledger_id for _fid, claim in entries],
    )
    by_id = {row["id"]: row["status"] for row in statuses}
    assert by_id[awaited_claim.ledger_id] == "fenced"
    assert by_id[crashed_claim.ledger_id] == "fenced"
    assert by_id[live_claim.ledger_id] == "awaited"
    assert (
        await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', live_claim.job_id
        )
        == "scheduled"
    )


@pytest.mark.integration
async def test_two_registered_fleet_claimants_release_source_and_wake(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    pg_dsn: str,
) -> None:
    """Real dispatch claimed rows, not an in-memory model or a fleet soak claim."""
    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
    from taskq.settings import WorkerSettings
    from taskq.worker.run import register_worker

    suffix = new_uuid().hex
    source_attempts: list[tuple[int, int]] = []

    async def screen(ctx: StepContext, item: object) -> enum.Enum:
        return _FleetOutcome.OK

    class _FleetOutcome(enum.Enum):
        OK = "ok"

    chain = _chain(f"fleet-park-chain-{suffix}", screen)
    app = WorkflowApp()

    @app.workflow(f"fleet_park_{suffix}", max_in_flight=2)
    def flow() -> Promise[object]:
        source = _source_body(chain, ((0, 1), (2, 3)), source_attempts)
        return build(chain_source(chain, source, key="source", max_attempts=2))

    compiled = app.get(f"fleet_park_{suffix}")
    creator = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await creator.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    worker_ids: list[JobId] = []
    for _ in range(2):
        worker_id = await register_worker(
            wf_pool,
            WorkerSettings.load_from_dict({"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": wf_schema}),
            workflow_execution=True,
        )
        worker_ids.append(JobId(worker_id))
    runners = [
        FlowRunner(compiled, wf_pool, wf_schema, worker_id=worker_id) for worker_id in worker_ids
    ]

    async def claim(worker_index: int, *, limit: int) -> list[dict[str, Any]]:
        rows = await dispatch_batch(
            wf_conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
            queues=["default"],
            limit_n=limit,
            worker_id=worker_ids[worker_index],
            lock_lease=timedelta(seconds=30),
        )
        return [dict(row) for row in rows]

    first = await claim(0, limit=1)
    assert len(first) == 1 and first[0]["id"] == source_id
    first_epoch = int(first[0]["claim_epoch"])
    assert first[0]["attempt"] == 1
    assert (
        await runners[0].run_fleet_claimed_step(
            flow_id,
            first[0],
            attempt=int(first[0]["attempt"]),
            claim_epoch=first_epoch,
        )
        == "deferred"
    )
    parked = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
    assert _park_is_live(parked), parked
    assert parked["attempt"] == 0
    assert parked["locked_by_worker"] is None
    assert parked["lock_expires_at"] is None
    assert parked["last_heartbeat_at"] is None
    assert parked["ledger_status"] == "awaited"
    assert parked["failed_ledger"] == 0

    # The parked source retains no physical claim; worker 1 can claim and
    # complete both child rows immediately on the same ordinary queue.
    children = await claim(1, limit=2)
    assert len(children) == 2
    assert {row["parent_id"] for row in children} == {source_id}
    for child in children:
        assert (
            await runners[1].run_fleet_claimed_step(
                flow_id,
                child,
                attempt=int(child["attempt"]),
                claim_epoch=int(child["claim_epoch"]),
            )
            == "succeeded"
        )
    wake_state = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
    assert wake_state["status"] == "pending"
    assert wake_state["blocking_reason"] is None
    assert wake_state["metadata"]["wf_emit_backpressure"]["wakes"] == 1  # type: ignore[index]

    resumed = await claim(1, limit=1)
    assert len(resumed) == 1 and resumed[0]["id"] == source_id
    assert resumed[0]["attempt"] == 1
    assert int(resumed[0]["claim_epoch"]) == first_epoch + 1
    assert (
        await runners[1].run_fleet_claimed_step(
            flow_id,
            resumed[0],
            attempt=1,
            claim_epoch=int(resumed[0]["claim_epoch"]),
        )
        == "succeeded"
    )
    # No predecessor remains between page one and its children; worker 0 makes
    # independent progress while the resumed source is terminal.
    last_children = await claim(0, limit=2)
    assert len(last_children) == 2
    for child in last_children:
        assert (
            await runners[0].run_fleet_claimed_step(
                flow_id,
                child,
                attempt=int(child["attempt"]),
                claim_epoch=int(child["claim_epoch"]),
            )
            == "succeeded"
        )
    await creator.tick(flow_id)
    final = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
    assert final["root_status"] == "succeeded"
    assert final["status"] == "succeeded"
    assert final["metadata"]["wf_source_logical_attempt"] == 1
    assert final["failed_ledger"] == 0
    assert source_attempts == [(0, 1), (1, 1), (1, 1)]
    assert await _cursor(wf_conn, wf_schema, source_id) == {"page": 1}


def test_waiter_migration_checksum_registration_and_shape() -> None:
    """The folded 42_01 is the WHOLE object set's sole DDL owner: exactly one
    file, its frozen bytes registered, no 41_03/41_04 identity anywhere, and
    every folded object present in its split statements (the park index's
    predicate carries the marker, never a NULL timestamp leg)."""
    import hashlib
    from pathlib import Path

    from taskq.migrate import discover, split_statements

    keys = [item.key for item in discover()]
    assert "01.00.41_03:pre" not in keys and "01.00.41_04:pre" not in keys
    matches = [item for item in discover() if item.key == "01.00.42_01:pre"]
    assert len(matches) == 1
    migration = matches[0]
    assert migration.filename == "01.00.42_01_pre_wf_step_ledger_claim_epoch.sql"
    assert migration.use_transaction
    manifest_entry = (
        f"{hashlib.sha256(migration.sql_template.encode()).hexdigest()}  {migration.filename}"
    )
    assert manifest_entry in Path("tests/data/released_migrations.sha256").read_text().splitlines()
    statements = split_statements(migration.sql_template)
    joined = "\n".join(statements)
    # the folded object set, one DDL owner
    for object_sql in (
        "ADD COLUMN IF NOT EXISTS claim_epoch",
        "wf_step_ledger_claim_uniq",
        "CREATE TABLE IF NOT EXISTS",
        "wf_emit_capacity",
        "wf_emit_intent_rotation",
        "wf_emit_capacity_pending_idx",
        "wake_generation > handled_generation",
        "wf_emit_park_guard",
        "CREATE TRIGGER",
        "jobs_wf_emit_park_idx",
    ):
        assert object_sql in joined, object_sql
    park_index = next(s for s in statements if "jobs_wf_emit_park_idx" in s and "CREATE INDEX" in s)
    assert "blocking_reason" in park_index
    assert "consume_budget" in park_index
    # REPRESENTATION INVERSION: no NULL/due timestamp leg exists in the
    # park identity; scheduled_at is never touched by the park index.
    assert "NULL" not in park_index.replace("IS NOT NULL", "")
    assert "CONCURRENTLY" not in joined


@pytest.mark.integration
async def test_waiter_probe_tiny_plans_and_wake_semantics(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """Tiny-table plans are factual micro controls, not a scale/index mandate."""
    query = wf_sql.emit_park_exists
    flow_id, claim = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )

    async def plan() -> list[dict[str, object]]:
        raw = await wf_conn.fetchval(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}", flow_id)
        return _plan_document(raw)

    empty_plan = await plan()
    assert _actual_root_rows(empty_plan) == 0
    await _park_primitive_claim(wf_pool, wf_sql, claim, logical_attempt=1, flow_id=flow_id)
    parked_plan = await plan()
    assert _actual_root_rows(parked_plan) == 1
    print(f"EXPLAIN ANALYZE tiny empty: {json.dumps(empty_plan, default=str)}")
    print(f"EXPLAIN ANALYZE tiny live: {json.dumps(parked_plan, default=str)}")
    cap_flow, cap_claim = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )
    await _park_primitive_claim(
        wf_pool,
        wf_sql,
        cap_claim,
        logical_attempt=1,
        page_width=1000,
        bound=1000,
        outstanding=1000,
        flow_id=cap_flow,
    )
    cap_query = wf_sql.emit_candidate.replace("$2::bigint", "0::bigint").replace(
        "$3::bigint", "1000::bigint"
    )
    cap_plan = _plan_document(
        await wf_conn.fetchval(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {cap_query}", cap_flow)
    )
    assert _actual_root_rows(cap_plan) == 1
    print(f"EXPLAIN ANALYZE tiny shipped-cap: {json.dumps(cap_plan, default=str)}")
    # The uncensored tiny documents may legitimately select Seq Scan and are the
    # explanatory record of that cheap choice. Populated index proof is separate.
    assert (await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)).promoted == 1


@pytest.mark.integration
@pytest.mark.load_sensitive
async def test_waiter_probe_populated_cap_uses_partial_index(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """1000-active + real history/coexisting rows prove the shared index walk."""
    from tests._wf_fixtures import seed_flow

    cap_flow, cap_claim = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )
    other_roots = [await seed_flow(wf_conn, wf_schema) for _ in range(8)]
    active = 1000
    history = 2000
    coexisting = 10_000
    active_statuses = ("pending", "scheduled")
    terminal_statuses = ("succeeded", "failed", "cancelled", "crashed", "abandoned")
    rows: list[tuple[object, ...]] = []
    for index in range(active + history):
        status = (
            active_statuses[index % len(active_statuses)]
            if index < active
            else terminal_statuses[index % len(terminal_statuses)]
        )
        rows.append(
            (
                new_uuid(),
                status,
                f"ordinary-{index}",
                json.dumps({"flow_id": str(cap_flow)}),
                f"wf-plan:{wf_schema}",
                f"target:{index}",
                # Active filler carries a real due time: a scheduled row
                # with no timer is the capacity park's exclusive storage
                # domain (01.00.41_04), never synthetic background noise.
                datetime.now(UTC),
                str(cap_flow),
            )
        )
    for index in range(coexisting):
        other_flow = other_roots[index % len(other_roots)]
        other_status = (
            terminal_statuses[index % len(terminal_statuses)]
            if index % 3 == 0
            else active_statuses[index % len(active_statuses)]
        )
        rows.append(
            (
                new_uuid(),
                other_status,
                f"ordinary-other-{index}",
                json.dumps({"flow_id": str(other_flow)}),
                f"wf-plan:{wf_schema}",
                f"other:{index}",
                # Active filler carries a real due time: a scheduled row
                # with no timer is the capacity park's exclusive storage
                # domain (01.00.41_04), never synthetic background noise.
                datetime.now(UTC),
                str(other_flow),
            )
        )
    # Bounded app-side identity minting; no process-modulo IDs, DB UUID minting,
    # fake SQL outcomes, or fabricated result rows enter this control.
    await wf_conn.executemany(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, idempotency_scope, idempotency_key, scheduled_at, fairness_key) "
        "VALUES ($1, 'wf-node', 'default', '{}', $2, 3, 'transient', $3, $4::jsonb, $5, $6, $7, $8)",
        rows,
    )
    await wf_conn.execute(f'ANALYZE "{wf_schema}".jobs')
    await _park_primitive_claim(
        wf_pool,
        wf_sql,
        cap_claim,
        logical_attempt=1,
        page_width=1000,
        bound=1000,
        outstanding=1000,
        flow_id=cap_flow,
    )
    population = await wf_conn.fetch(
        f'SELECT status::text AS status, count(*)::int AS count FROM "{wf_schema}".jobs '
        "GROUP BY status ORDER BY status"
    )
    counts = {str(row["status"]): int(row["count"]) for row in population}
    target_states = await wf_conn.fetch(
        f'SELECT status::text AS status, count(*)::int AS count FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND id <> $2 AND step_key <> '__flow__' "
        "GROUP BY status ORDER BY status",
        cap_flow,
        cap_claim.job_id,
    )
    target_counts = {str(row["status"]): int(row["count"]) for row in target_states}
    active_target = sum(target_counts.get(status, 0) for status in active_statuses)
    terminal_target = sum(target_counts.get(status, 0) for status in terminal_statuses)
    assert (active_target, terminal_target) == (active, history), target_counts
    total_jobs = sum(counts.values())
    assert total_jobs >= active + history + coexisting, counts

    # A legal parked population across the coexisting flows (the exact park
    # representation, storage-legal per 01.00.41_04): the generic
    # scheduled-NULL partial index's matching set IS the parked population,
    # so at this shape the dedicated waiter index — whose leading key is the
    # flow id — is strictly the best access path for the flow-target probe,
    # and the planner's choice is forced by cost, never by a hint.
    parked_population = 200
    parked_rows: list[tuple[object, ...]] = []
    for index in range(parked_population):
        other_flow = other_roots[index % len(other_roots)]
        parked_rows.append(
            (
                new_uuid(),
                json.dumps(
                    {
                        "flow_id": str(other_flow),
                        "blocking_reason": "backpressure",
                        "wf_deferral": {
                            "signal": "EmitBackpressure",
                            "consume_budget": False,
                            "attempt": 0,
                        },
                        "wf_emit_backpressure": {
                            "version": 1,
                            "page_width": 2,
                            "bound": 2,
                            "outstanding": 2,
                            "deferrals": 1,
                            "wakes": 0,
                        },
                    }
                ),
                str(other_flow),
            )
        )
    await wf_conn.executemany(
        f'INSERT INTO "{wf_schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, scheduled_at, fairness_key, attempt) "
        "VALUES ($1, 'wf-node', 'default', '{}', 'scheduled', 3, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), $3, 0)",
        parked_rows,
    )
    await wf_conn.execute(f'ANALYZE "{wf_schema}".jobs')

    query = wf_sql.emit_park_exists.replace("SELECT 1", "SELECT j.id").replace("LIMIT 1", "")
    candidate_rows = await wf_conn.fetch(query, cap_flow)
    assert [row["id"] for row in candidate_rows] == [cap_claim.job_id]
    raw_plan = await wf_conn.fetchval(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}", cap_flow)
    document = _plan_document(raw_plan)
    nodes = _plan_nodes(document)
    # THE POPULATED LAW: the estate probe runs on the DEDICATED waiter
    # partial index — its leading key is the flow id, so the flow-target
    # probe reads ONLY its own flow's waiters out of a populated parked
    # estate (200 parked rows across the coexisting flows + the target's
    # one). A plan that falls back to the generic scheduled-NULL index
    # scans the whole parked population per probe — the degradation this
    # pin exists to red.
    waiter_nodes = [node for node in nodes if node.get("Index Name") == "jobs_wf_emit_park_idx"]
    print(
        "EXPLAIN ANALYZE populated waiter before index assertion: "
        + json.dumps(document, default=str, sort_keys=True)
    )
    assert len(waiter_nodes) == 1, document
    waiter = waiter_nodes[0]
    assert waiter.get("Relation Name") == "jobs" and waiter.get("Alias") == "j"
    assert waiter.get("Node Type") in {"Index Scan", "Index Only Scan"}
    assert str(waiter["Scan Direction"]) == "Forward"
    assert waiter.get("Actual Rows") == 1
    condition = waiter.get("Index Cond")
    assert isinstance(condition, str), waiter
    normalized_condition = "".join(condition.split())
    assert "metadata->>'flow_id'::text" in normalized_condition, condition
    assert f"'{cap_flow}'::uuid" in normalized_condition, condition
    seq_nodes = [node for node in nodes if node.get("Node Type") == "Seq Scan"]
    assert seq_nodes == [], document
    print(
        "EXPLAIN ANALYZE populated waiter before index assertion: "
        + json.dumps(document, default=str, sort_keys=True)
    )
    # THE ASSERTION IS LOAD-BEARING (the red-when-absent direction): with the
    # dedicated waiter index dropped, the same probe's plan no longer shows
    # it — this exact pin would red.
    await wf_conn.execute(f'DROP INDEX "{wf_schema}".jobs_wf_emit_park_idx')
    degraded = _plan_document(
        await wf_conn.fetchval(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}", cap_flow)
    )
    degraded_nodes = _plan_nodes(degraded)
    assert [
        node for node in degraded_nodes if node.get("Index Name") == "jobs_wf_emit_park_idx"
    ] == [], degraded_nodes
    print(
        "EXPLAIN ANALYZE populated waiter WITHOUT the index (the red direction): "
        + json.dumps(degraded, default=str, sort_keys=True)
    )
    execution_ms = _nonnegative_metric(document[0], "Execution Time")
    planning_ms = _nonnegative_metric(document[0], "Planning Time")
    root_plan = document[0]["Plan"]
    assert isinstance(root_plan, dict)
    typed_root = cast("dict[str, object]", root_plan)
    buffers = {
        key: _nonnegative_metric(typed_root, key)
        for key in (
            "Shared Hit Blocks",
            "Shared Read Blocks",
            "Shared Dirtied Blocks",
            "Shared Written Blocks",
            "Temp Read Blocks",
            "Temp Written Blocks",
        )
    }
    print("EXPLAIN ANALYZE populated waiter: " + json.dumps(document, default=str, sort_keys=True))
    print(
        "POPULATED WAITER FACTS: "
        + json.dumps(
            {
                "all_jobs": counts,
                "target_jobs": target_counts,
                "execution_ms": execution_ms,
                "planning_ms": planning_ms,
                "buffers": buffers,
                "index": waiter["Index Name"],
                "index_condition": condition,
            },
            sort_keys=True,
        )
    )
    # 1000 actual live work + a 1000-wide page cannot wake under cap 1000.
    # Ordinary coexisting rows stay unchanged and do not enter the parked result.
    before_statuses = await wf_conn.fetch(
        f'SELECT id, status::text AS status FROM "{wf_schema}".jobs ORDER BY id',
    )
    wake = await wake_emit_backpressure(wf_pool, wf_sql, flow_id=cap_flow)
    assert wake.promoted == 0
    after = await wf_conn.fetchrow(
        f'SELECT status, scheduled_at, attempt, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        cap_claim.job_id,
    )
    assert after is not None
    assert after["status"] == "scheduled" and after["scheduled_at"] is not None
    assert after["attempt"] == 0
    assert _metadata(after["metadata"])["blocking_reason"] == "backpressure"
    after_statuses = await wf_conn.fetch(
        f'SELECT id, status::text AS status FROM "{wf_schema}".jobs ORDER BY id',
    )
    assert [dict(row) for row in after_statuses] == [dict(row) for row in before_statuses]


@pytest.mark.integration
async def test_real_failure_after_park_uses_logical_then_physical_attempts(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    suffix = new_uuid().hex
    gate = asyncio.Event()
    page_zero = asyncio.Event()
    started = [asyncio.Event(), asyncio.Event()]
    entries: list[tuple[str, int]] = []
    page_one_entries = 0

    async def screen(ctx: StepContext, item: object) -> enum.Enum:
        assert ctx.map_index in (0, 1, 2, 3)
        if ctx.map_index in (0, 1):
            started[ctx.map_index].set()
        await gate.wait()
        return _Outcome.OK

    class _Outcome(enum.Enum):
        OK = "ok"

    chain = _chain(f"post-park-failure-{suffix}", screen)
    app = WorkflowApp()

    @app.workflow(f"post_park_failure_{suffix}", max_in_flight=2)
    def flow() -> Promise[object]:
        async def source(ctx: StepContext) -> None:
            nonlocal page_one_entries
            cursor = await ctx.cursor()
            for page in range(int(cast("int", cursor.get("page", -1))) + 1, 3):
                if page == 2:
                    # Physical page list is zero-based for these fixtures.
                    break
                entries.append((f"page-{page}", ctx.attempt))
                if page == 1:
                    page_one_entries += 1
                    if page_one_entries == 2:
                        raise RuntimeError("one real source failure after wake")
                await ctx.emit_batch(
                    [
                        chain_start(
                            chain, {"record_id": record}, map_index=record, trace_id=f"r-{record}"
                        )
                        for record in ((0, 1) if page == 0 else (2, 3))
                    ],
                    cursor={"page": page},
                )
                if page == 0:
                    page_zero.set()

        return build(chain_source(chain, source, key="source", max_attempts=2))

    runner = FlowRunner(app.get(f"post_park_failure_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = await _source_id(wf_conn, wf_schema, flow_id)
    source_task = asyncio.create_task(runner.tick(flow_id))
    children: list[asyncio.Task[bool]] = []
    try:
        try:
            await asyncio.wait_for(page_zero.wait(), _WAIT_S)
        except TimeoutError as exc:
            task_error = source_task.exception() if source_task.done() else None
            state = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
            raise AssertionError(
                json.dumps(
                    {"entries": entries, "source": state, "task_error": str(task_error)},
                    default=str,
                    indent=2,
                )
            ) from exc
        for index in range(2):
            children.append(asyncio.create_task(runner.tick(flow_id)))
            await asyncio.wait_for(started[index].wait(), _WAIT_S)
        parked = await _await_source_status(
            wf_conn, wf_schema, flow_id, source_id, "source", _park_is_live, deadline_s=_WAIT_S
        )
        assert _park_is_live(parked), parked
        await asyncio.wait_for(source_task, _WAIT_S)
        gate.set()
        await asyncio.gather(*children)
        assert await runner.drive(flow_id, max_ticks=250) == "terminal"
    finally:
        gate.set()
        tasks = [source_task, *children]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert entries == [("page-0", 1), ("page-1", 1), ("page-1", 1), ("page-1", 2)]
    final = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
    all_rows = await wf_conn.fetch(
        f"SELECT step_key, map_index, status::text AS status, attempt, error_class, error_message "
        f"FROM \"{wf_schema}\".jobs WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1 "
        "ORDER BY step_key, map_index",
        flow_id,
    )
    diagnostic = {
        "entries": entries,
        "source": final,
        "rows": [dict(row) for row in all_rows],
    }
    rendered = json.dumps(diagnostic, default=str, indent=2)
    assert final["status"] == "succeeded", rendered
    assert final["root_status"] == "succeeded", rendered
    assert final["attempt"] == 2
    assert final["ledger_attempt"] == 2
    assert final["ledger_job_id"] == source_id
    assert final["metadata"]["wf_source_logical_attempt"] == 2
    assert final["failed_ledger"] == 1
    assert final["metadata"]["wf_emit_backpressure"]["deferrals"] == 1  # type: ignore[index]
    assert final["metadata"]["wf_emit_backpressure"]["wakes"] == 1  # type: ignore[index]


@pytest.mark.integration
async def test_cancel_fenced_park_uses_atomic_engine_terminal_records(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    flow_id, claim = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET cancel_phase = 1, cancel_requested_at = now() '
        "WHERE id = $1 AND status = 'running'",
        claim.job_id,
    )
    raw = await wf_conn.fetchrow(f'SELECT * FROM "{wf_schema}".jobs WHERE id = $1', claim.job_id)
    assert raw is not None
    runner = FlowRunner(_cancel_compiled(), wf_pool, wf_schema, worker_id=claim.worker_id)
    verdict = await runner._defer_step(  # pyright: ignore[reportPrivateUsage]  # Why: this is the control/cancel door under test.
        flow_id,
        dict(raw),
        claim.attempt,
        claim.ledger_id,
        EmitBackpressure(page_width=1, bound=1, outstanding=1),
        claim_epoch=claim.claim_epoch,
        ledger_claimed_at=claim.ledger_claimed_at,
    )
    assert verdict == "cancelled"
    row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, attempt, error_class, metadata FROM "{wf_schema}".jobs '
        "WHERE id = $1",
        claim.job_id,
    )
    assert row is not None
    assert (row["status"], row["attempt"], row["error_class"]) == (
        "cancelled",
        1,
        "WorkflowCancelled",
    )
    metadata = _metadata(row["metadata"])
    assert metadata["wf_source_logical_attempt"] == 1
    assert "blocking_reason" not in metadata and "wf_emit_backpressure" not in metadata
    ledger_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".wf_step_ledger WHERE id = $1', claim.ledger_id
    )
    assert ledger_status == "cancelled"
    attempt_record = await wf_conn.fetchrow(
        f'SELECT outcome::text AS outcome, attempt FROM "{wf_schema}".job_attempts WHERE job_id = $1',
        claim.job_id,
    )
    assert attempt_record is not None
    assert (attempt_record["outcome"], attempt_record["attempt"]) == ("cancelled", 1)
    event = await wf_conn.fetchrow(
        f'SELECT detail FROM "{wf_schema}".job_events WHERE job_id = $1', claim.job_id
    )
    assert event is not None
    detail = _metadata(event["detail"])
    assert (detail["from_state"], detail["to_state"]) == ("running", "cancelled")


def _cancel_compiled() -> CompiledWorkflow:
    app = WorkflowApp()

    async def never_body(ctx: StepContext, item: object) -> None:
        raise AssertionError("the control/cancel primitive must never execute a body")

    chain = Chain(
        name=f"cancel-control-{new_uuid().hex}",
        start="unused",
        steps={"unused": Step(body=never_body, outcomes=object)},
    )

    workflow_name = f"cancel_control_{new_uuid().hex}"

    @app.workflow(workflow_name)
    def flow() -> Promise[object]:
        async def source(ctx: StepContext) -> None:
            raise AssertionError("unused declaration body")

        return build(chain_source(chain, source, key="source"))

    return app.get(workflow_name)


@pytest.mark.integration
async def test_pending_same_tx_fork_counts_before_parent_wake(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    from taskq.workflows._types import ChildSpec, ForkSpec, NodeSpec
    from taskq.workflows.engine import finalize_node, insert_node

    flow_id, source = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )
    worker = JobId(new_uuid())
    peer_id = await insert_node(
        wf_conn,
        wf_sql,
        NodeSpec(
            flow_id=flow_id,
            step_key="peer",
            actor="wf-node",
            queue="default",
            payload={},
        ),
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', locked_by_worker = $2, "
        "lock_expires_at = now() + interval '90 seconds' WHERE id = $1",
        peer_id,
        worker,
    )
    await _park_primitive_claim(
        wf_pool,
        wf_sql,
        source,
        logical_attempt=1,
        page_width=1,
        bound=1,
        flow_id=flow_id,
        outstanding=1,
    )
    parent_id = await insert_node(
        wf_conn,
        wf_sql,
        NodeSpec(
            flow_id=flow_id,
            step_key="parent",
            actor="wf-node",
            queue="default",
            payload={},
        ),
    )
    parent_claim_row = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), parent_id, worker
    )
    assert parent_claim_row is not None
    parent_attempt = int(parent_claim_row["attempt"])
    parent_epoch = int(parent_claim_row["claim_epoch"])
    assert parent_attempt == 1
    parent_ledger = await wf_conn.fetchrow(
        wf_sql.ledger_claim,
        new_uuid(),
        flow_id,
        parent_id,
        "parent",
        None,
        parent_attempt,
        parent_epoch,
    )
    assert parent_ledger is not None
    finalized = await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent_id,
        step_key="parent",
        worker_id=worker,
        attempt=parent_attempt,
        claim_epoch=parent_epoch,
        outcome="succeeded",
        fork=ForkSpec(
            children=(ChildSpec(step_key="forked", actor="wf-node", queue="default", payload={}),)
        ),
        ledger_id=JobId(parent_ledger["id"]),
        ledger_claimed_at=parent_ledger["updated_at"],
        emit_capacity=True,
    )
    assert finalized.applied
    assert not finalized.intent_created, (
        "the fork parent's own terminal counts its same-tx fork rows in the "
        "derived charge only after they commit; the parent's event observes "
        "the real rows as they are"
    )
    source_after_fork = await wf_conn.fetchrow(
        f'SELECT status, metadata FROM "{wf_schema}".jobs WHERE id = $1', source.job_id
    )
    assert source_after_fork is not None and source_after_fork["status"] == "scheduled"
    assert _metadata(source_after_fork["metadata"])["blocking_reason"] == "backpressure"
    # The fixture's peer (the park-shape's live capacity consumer) leaves:
    # the child terminal below is then the seat that can cross.
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".jobs WHERE id = $1', peer_id)
    child_id = await wf_conn.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE parent_id = $1 AND step_key = 'forked'",
        parent_id,
    )
    assert child_id is not None
    child_claim_row = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema), child_id, worker
    )
    assert child_claim_row is not None
    child_attempt = int(child_claim_row["attempt"])
    child_epoch = int(child_claim_row["claim_epoch"])
    assert child_attempt == 1
    child_ledger = await wf_conn.fetchrow(
        wf_sql.ledger_claim,
        new_uuid(),
        flow_id,
        child_id,
        "forked",
        None,
        child_attempt,
        child_epoch,
    )
    assert child_ledger is not None
    child_final = await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=JobId(child_id),
        step_key="forked",
        worker_id=worker,
        attempt=child_attempt,
        claim_epoch=child_epoch,
        outcome="succeeded",
        ledger_id=JobId(child_ledger["id"]),
        ledger_claimed_at=child_ledger["updated_at"],
        emit_capacity=True,
    )
    assert child_final.applied
    if child_final.intent_created:
        from taskq.workflows._emit import wake_emit_backpressure

        await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
    source_after_child = await wf_conn.fetchrow(
        f'SELECT status, metadata FROM "{wf_schema}".jobs WHERE id = $1', source.job_id
    )
    assert source_after_child is not None and source_after_child["status"] == "pending"
    assert "blocking_reason" not in _metadata(source_after_child["metadata"])


@pytest.mark.integration
async def test_pre_waiter_schema_park_rolls_back_without_stranding_marker(pg_dsn: str) -> None:
    from taskq.migrate import apply_pending
    from taskq.workflows.engine import render_workflow_sql

    schema = f"bp_old_{new_uuid().hex}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        applied = await apply_pending(conn, schema=schema, target="01.00.38_01")
        assert applied[-1].key == "01.00.38_01:pre"
        old_sql = render_workflow_sql(schema)
        # The era's own six-bind ledger claim: this schema predates the
        # ledger's claim-epoch column, so the current binary's claim cannot
        # run here and the era's statement seeds the row the park attacks.
        era_claim = (
            old_sql.ledger_claim.replace(", claim_epoch, status)", ", status)")
            .replace("$6, $7, 'running'", "$6, 'running'")
            .replace(", attempt, claim_epoch) DO UPDATE", ", attempt) DO UPDATE")
        )
        _flow_id, claim = await _claimed(
            conn,
            schema,
            old_sql,
            metadata={"wf_source_logical_attempt": 1},
            claim_statement=era_claim,
        )
        pool = await asyncpg.create_pool(pg_dsn)
        # The pre-fold schema lacks the 42_01 object set: the §2.1 whole
        # -object-set preflight refuses before ANY body, child, cursor, or
        # park write (the old-era park cannot even run — and the folded
        # schema's scheduled_at stays NOT NULL, so the deleted NULL
        # representation is unreachable in every era).
        from taskq.workflows._capability import verify_emit_capacity_objects

        with pytest.raises(Exception, match=r"01\.00\.42_01"):
            await verify_emit_capacity_objects(pool, schema)
        with pytest.raises(Exception, match=r"01\.00\.42_01"):
            await _park_primitive_claim(pool, old_sql, claim, logical_attempt=1, flow_id=_flow_id)
        row = await conn.fetchrow(
            f"SELECT status, scheduled_at, attempt, locked_by_worker, metadata "
            f'FROM "{schema}".jobs WHERE id = $1',
            claim.job_id,
        )
        assert row is not None
        assert (row["status"], row["attempt"], row["locked_by_worker"]) == (
            "running",
            1,
            claim.worker_id,
        )
        metadata = _metadata(row["metadata"])
        assert "blocking_reason" not in metadata and "wf_emit_backpressure" not in metadata
        assert metadata["wf_source_logical_attempt"] == 1
        ledger_status = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".wf_step_ledger WHERE id = $1', claim.ledger_id
        )
        assert ledger_status == "running"
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_claimable_listing_agrees_with_the_claim_cas(pg_dsn: str) -> None:
    """The direct driver's listing and its claim CAS must agree on every row
    shape. REPRESENTATION INVERSION: the scheduled-NULL storage state is
    UNWRITABLE on the folded schema (scheduled_at is NOT NULL — the deleted
    representation), so the agreement control asserts the un-writability
    itself plus the exact park's unclaimability through both the listing
    and the CAS."""
    import json as _json

    from taskq.migrate import apply_pending
    from taskq.workflows.api._sql_runner import (
        CLAIMABLE_NODES_SQL_TEMPLATE,
        NODE_CLAIM_SQL_TEMPLATE,
    )
    from taskq.workflows.engine import render_workflow_sql

    schema = f"bp_agree_{new_uuid().hex}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await apply_pending(conn, schema=schema)
        wsql = render_workflow_sql(schema)
        flow_id, claim = await _claimed(
            conn, schema, wsql, metadata={"wf_source_logical_attempt": 1}
        )
        pool = await asyncpg.create_pool(pg_dsn)
        await _park_primitive_claim(pool, wsql, claim, logical_attempt=1, flow_id=flow_id)

        # The park instant is stored; the marker — never the timestamp —
        # discriminates. The parked row is offered by no probe and taken by
        # no claim CAS.
        listing = await pool.fetch(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, schema), flow_id)
        assert [row["id"] for row in listing] == [], (
            f"the claimable listing must not offer the parked row, "
            f"got {[str(r['id']) for r in listing]}"
        )
        claimed = await pool.fetchrow(
            render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), claim.job_id, JobId(new_uuid())
        )
        assert claimed is None, "the parked row must be unclaimable (the CAS's own marker leg)"

        # The deleted representation's storage state is unwritable: a
        # scheduled row with a NULL due time violates the column's NOT NULL.
        ambiguous = new_uuid()
        with pytest.raises(asyncpg.NotNullViolationError):
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
                "metadata, scheduled_at, fairness_key) "
                "VALUES ($1, 'wf-node', 'default', '{}', 'scheduled', 3, 'transient', 'work', "
                "$2::jsonb, NULL, $3)",
                ambiguous,
                _json.dumps({"flow_id": str(flow_id)}),
                str(flow_id),
            )

        # An ordinary due scheduled row (a real timer) is listed AND claimed
        # by the same fences — the agreement's positive direction.
        due = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
            "metadata, scheduled_at, fairness_key) "
            "VALUES ($1, 'wf-node', 'default', '{}', 'scheduled', 3, 'transient', 'work', "
            "$2::jsonb, clock_timestamp() - interval '1 second', $3)",
            due,
            _json.dumps({"flow_id": str(flow_id)}),
            str(flow_id),
        )
        listing = await pool.fetch(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, schema), flow_id)
        assert [row["id"] for row in listing] == [due], (
            f"the due ordinary row must be listed (and only it), got {[str(r['id']) for r in listing]}"
        )
        worker = JobId(new_uuid())
        claimed = await pool.fetchrow(render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), due, worker)
        assert claimed is not None, "the due ordinary row must be claimable (the listing agrees)"
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_malformed_waiter_marker_fails_closed_not_skip(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """REPRESENTATION INVERSION: the estate pass's per-row skip is deleted —
    a malformed parked marker is genuine corruption, and the drain fails
    closed (roll back, bounded diagnostic, no promotion, the generation
    stays pending) instead of laundering the bad row past a wake."""
    import json as _json

    flow_v, claim_v = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )
    # The malformed live marker: version 1 present, page_width absent (the
    # width validator's fail-closed arm).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', "
        "metadata = metadata || $2::jsonb WHERE id = $1",
        claim_v.job_id,
        _json.dumps(
            {
                "blocking_reason": "backpressure",
                "wf_deferral": {
                    "signal": "EmitBackpressure",
                    "consume_budget": False,
                    "attempt": 0,
                },
                "wf_emit_backpressure": {"version": 1, "bound": 2, "deferrals": 1},
            }
        ),
    )
    from taskq.workflows._emit import ensure_emit_capacity

    async with wf_pool.acquire() as cap_conn:
        await ensure_emit_capacity(cap_conn, wf_sql, flow_id=flow_v, bound=2)
    with pytest.raises(Exception, match="integer"):
        await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_v)
    bad_state = await _source_state(wf_conn, wf_schema, flow_v, claim_v.job_id, "source")
    assert _park_is_live(bad_state), (
        f"the malformed marker stays parked, visible and bounded: {bad_state}"
    )
    assert bad_state["status"] == "scheduled", bad_state


async def test_pre_epoch_schema_refuses_the_normal_ledger_claim_fail_closed(
    pg_dsn: str,
) -> None:
    """The PRE gate's envelope at the NORMAL claim (not just the park): the
    new binary's epoch-scoped ledger claim against a pre-`01.00.42_01`
    schema fails CLOSED with the typed driver error (42703 — the column
    does not exist), AFTER the job row is already 'running' — the old body
    never runs, the transient running row is the documented recovery
    envelope, and applying the PRE file before deploying the binary is the
    only protection."""
    from taskq.migrate import apply_pending
    from taskq.workflows.engine import render_workflow_sql
    from taskq.workflows.ledger import claim_step_ledger

    schema = f"bp_pre42_{new_uuid().hex}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        applied = await apply_pending(conn, schema=schema, target="01.00.41_02")
        assert applied[-1].key == "01.00.41_02:pre"
        wsql = render_workflow_sql(schema)
        flow_id, claim = await _claimed(
            conn,
            schema,
            wsql,
            metadata={"wf_source_logical_attempt": 1},
            claim_statement=(
                wsql.ledger_claim.replace(", claim_epoch, status)", ", status)")
                .replace("$6, $7, 'running'", "$6, 'running'")
                .replace(", attempt, claim_epoch) DO UPDATE", ", attempt) DO UPDATE")
            ),
        )
        row = await conn.fetchrow(
            f"SELECT status::text AS status, attempt::int AS attempt "
            f'FROM "{schema}".jobs WHERE id = $1',
            claim.job_id,
        )
        assert row is not None and row["status"] == "running", str(row)
        pool = await asyncpg.create_pool(pg_dsn)
        # The new binary's NORMAL ledger claim (the every-claim path, not a
        # park-specific arm) against the pre-epoch schema.
        with pytest.raises(asyncpg.UndefinedColumnError):
            async with pool.acquire() as claim_conn:
                await claim_step_ledger(
                    claim_conn,
                    wsql,
                    flow_id=flow_id,
                    job_id=claim.job_id,
                    step_key=claim.step_key,
                    map_index=claim.map_index,
                    attempt=claim.attempt,
                    claim_epoch=claim.claim_epoch,
                )
        row_after = await conn.fetchrow(
            f"SELECT status::text AS status, attempt::int AS attempt "
            f'FROM "{schema}".jobs WHERE id = $1',
            claim.job_id,
        )
        assert row_after is not None and row_after["status"] == "running", (
            "the claim refused after the job row was already running — the "
            "documented transient-running envelope"
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_the_reaper_never_fences_a_live_claimed_attempt(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """THE REAPER'S LIVE-CLAIM EXCLUSION (the deadline race's own fence): a
    terminal flow's LIVE claimed running job (its lease still holds) is an
    attempt executing RIGHT NOW — its ledger row must stay live so the
    attempt's own disposition writes its truthful terminal. The rejected
    shape: a reaper pass inside the claim→disposition window fenced the
    running row, the disposition's own truthful write then REFUSED (the
    fenced status is outside every terminal write's guard), and the record
    read 'fenced' forever — a heartbeating attempt whose ledger said it
    was dead. When the lease lapses, the row IS the reaper's subject (the
    opposite direction: the crash window's own fence)."""
    flow_id, claim = await _claimed(
        wf_conn, wf_schema, wf_sql, metadata={"wf_source_logical_attempt": 1}
    )
    # The flow terminalizes while the job's attempt is live (a real
    # claimed row: running, a future lease).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now() WHERE id = $1",
        flow_id,
    )
    live_row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, lock_expires_at FROM "{wf_schema}".jobs WHERE id = $1',
        claim.job_id,
    )
    assert live_row is not None
    assert live_row["status"] == "running" and live_row["lock_expires_at"] is not None

    assert await reap_phantom_ledger(wf_pool, wf_sql) == 0, (
        "the reaper fenced a LIVE claimed attempt's ledger row — the "
        "attempt is heartbeating and will write its own terminal"
    )
    live_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    assert live_status == "running", live_status

    # The lease lapses (the worker died): the row is now the reaper's subject.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET lock_expires_at = now() - interval '1 second' "
        "WHERE id = $1",
        claim.job_id,
    )
    assert await reap_phantom_ledger(wf_pool, wf_sql) == 1
    fenced_status = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".wf_step_ledger WHERE id = $1',
        claim.ledger_id,
    )
    assert fenced_status == "fenced", fenced_status
