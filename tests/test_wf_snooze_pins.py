# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""THE WF SNOOZE/RETRY-AFTER PINS — the body's control-flow signals, red-first.

THE CONVICTION (the OCR-429 exhaustion): a workflow body raising the
vanilla surface's typed control-flow signals —
:class:`~taskq.exceptions.Snooze` (reschedule, does NOT consume retry
budget) and :class:`~taskq.exceptions.RetryAfter(delay, consume_budget=)`
(reschedule, consumes by default) — burned the step's attempt budget: the
signals fell into the runner's ladder's body-failure catch, the attempt
laddered, and a 429-with-`Retry-After`-header poller exhausted
``max_attempts`` parked at the ladder's backoff — the exact defect the
vanilla consumer's catch (``src/taskq/worker/_consumer.py``'s
``_preserved_exc`` machinery, :2069) exists to prevent for plain jobs.
The consumer translates the signals into the row dispositions
``mark_snoozed`` / ``mark_retry_after_consume_{true,false}``
(``backend/_sql_templates.py``); the workflow runner had no translation.

THE DISPOSITION LAW (the vanilla semantics, mirrored onto the wf finalize):

* the node row re-schedules: ``scheduled_at = clock_timestamp() + delay``,
  status ``scheduled`` (``pending`` for a zero effective delay) — the SAME
  live-row shape the ladder's own ``NODE_REPEND_SQL_TEMPLATE`` writes, so
  both doors re-claim it (the dev tick's claimable scan directly; the
  fleet via the leader's scheduled→pending promotion sweep);
* the attempt NEVER walks under a non-consuming deferral — the claim's
  increment is REFUNDED (the vanilla ``ATTEMPT_REFUND_SQL`` law,
  ``backend/_sql_fragments.py``:72), so ``max_attempts`` is never
  exhausted by snoozes alone and the ledger re-claims the SAME attempt
  arbiter row (the record never grows per snooze);
* a consuming ``RetryAfter`` KEEPS the claim's increment (the burn) and
  is bounded by the budget — the vanilla ``mark_retry_after_consume_true``'s
  ``max_attempts`` fence: past it the step terminal-fails
  ``MaxAttemptsExceeded`` (the vanilla's own class + message);
* the non-consuming delay carries the vanilla's deferral floor
  (``MIN_DEFERRAL_INTERVAL``) — a zero/now snooze must not park the node
  at the head of the dispatch order (the monopolisation guard);
* the join's accounting is UNCHANGED: the deferral runs no decrement, no
  fire — the snoozed step's downstream still counts it (the row stays
  pending, just later), and the run stays live (the maintenance leg's
  ``has_unresolved`` reads ``scheduled`` rows as live work);
* the step's result capture stays EMPTY (nothing succeeded), the claim's
  ledger row rests ``awaited`` (the hold's own resume-not-retry status —
  never a ``failed`` row the ladder would count), and the node's metadata
  carries the deferral stamp (the run page's visibility).

THE EXTENSIBILITY SEAM (the named-disposition law): the signals resolve
to NAMED dispositions (:data:`~taskq.workflows.api._runner_defer.
DeferralDisposition`), one statement arm per disposition, one catch arm
in the runner — the next deferral verdict adds a literal member, a CASE
arm, and a resolver entry, never a third special case.

The vanilla path is untouched: its catch, its row dispositions, and its
pins are byte-identical (this file pins the WF face only).
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.exceptions import RetryAfter, Snooze
from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, step

pytestmark = pytest.mark.integration

MODULE_NAME = "wf_snooze_demo_flows"

#: The deferral floor's seconds (``constants.MIN_DEFERRAL_INTERVAL``) —
#: every non-consuming deferral lands at least this far out; the pins'
#: waits read the constant, never a re-spelled literal.
FLOOR_S = 1.0


class Ingest(BaseModel):
    doc_id: str


def _snooze_once_then_ok(snoozes: list[int], delay: timedelta) -> Any:
    """The one-snooze body factory: a closure counter (the refund re-claims
    the SAME attempt ordinal, so ``ctx.attempt`` cannot count — the pins'
    counters are the calls') records each snooze."""

    async def _body(ctx: Any, params: Ingest) -> str:
        if not snoozes:
            snoozes.append(1)
            raise Snooze(delay)
        return "done"

    return _body


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The registered definitions (the compile's own door — the bodies
    resolve from the D1 registry, never a per-call map)."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    once: list[int] = []

    @app_obj.workflow("snooze_once_flow")
    def snooze_once() -> Promise[object]:
        return build(
            step(
                _snooze_once_then_ok(once, timedelta(seconds=FLOOR_S)),
                Ingest(doc_id="d1"),
                key="solo",
            )
        )

    @app_obj.workflow("snooze_five_flow")
    def snooze_five() -> Promise[object]:
        burns: list[int] = []

        async def _body(ctx: Any, params: Ingest) -> str:
            if len(burns) < 5:
                burns.append(1)
                raise Snooze(timedelta(seconds=FLOOR_S))
            return "done"

        return build(step(_body, Ingest(doc_id="d1"), key="solo", max_attempts=2))

    @app_obj.workflow("retry_consuming_flow")
    def retry_consuming() -> Promise[object]:
        burns: list[int] = []

        async def _body(ctx: Any, params: Ingest) -> str:
            if not burns:
                burns.append(1)
                raise RetryAfter(timedelta(seconds=0), consume_budget=True)
            return "done"

        return build(step(_body, Ingest(doc_id="d1"), key="solo", max_attempts=2))

    @app_obj.workflow("retry_consuming_exhausted_flow")
    def retry_consuming_exhausted() -> Promise[object]:
        async def _body(ctx: Any, params: Ingest) -> str:
            raise RetryAfter(timedelta(seconds=0), consume_budget=True)

        return build(step(_body, Ingest(doc_id="d1"), key="solo", max_attempts=1))

    @app_obj.workflow("retry_free_flow")
    def retry_free() -> Promise[object]:
        burns: list[int] = []

        async def _body(ctx: Any, params: Ingest) -> str:
            if not burns:
                burns.append(1)
                raise RetryAfter(timedelta(seconds=FLOOR_S), consume_budget=False)
            return "done"

        return build(step(_body, Ingest(doc_id="d1"), key="solo", max_attempts=1))

    @app_obj.workflow("ladder_regression_flow")
    def ladder_regression() -> Promise[object]:
        burns: list[int] = []

        async def _body(ctx: Any, params: Ingest) -> str:
            if not burns:
                burns.append(1)
                raise RuntimeError("the armed transient failure")
            return "done"

        return build(step(_body, Ingest(doc_id="d1"), key="solo", max_attempts=2))

    @app_obj.workflow("snooze_child_join_flow")
    def snooze_child_join() -> Promise[object]:
        child_burns: list[int] = []

        async def _child(ctx: Any, parent: str) -> str:
            if not child_burns:
                child_burns.append(1)
                raise Snooze(timedelta(seconds=FLOOR_S))
            return f"child:{parent}"

        async def _src(ctx: Any, params: Ingest) -> str:
            return "src"

        async def _join(ctx: Any, a: str, b: str) -> str:
            return f"{a}|{b}"

        src = step(_src, Ingest(doc_id="d1"), key="src")
        child = step(_child, src, key="child")
        return build(step(_join, child, src, key="join"))

    # THE STATEMENT-PINS' FLOWS (the cancel/zombie fences drive the
    # runner's own seam against SEEDED rows — the bodies never run; the
    # names are unique per test: the D1 registry is the process's own,
    # and a second registration of a name is refused by its law).
    @app_obj.workflow("snooze_cancel_fence_flow")
    def snooze_cancel_fence() -> Promise[object]:
        async def _body(ctx: Any, params: Ingest) -> str:
            return "done"

        return build(step(_body, Ingest(doc_id="d1"), key="solo"))

    @app_obj.workflow("snooze_zombie_fence_flow")
    def snooze_zombie_fence() -> Promise[object]:
        async def _body(ctx: Any, params: Ingest) -> str:
            return "done"

        return build(step(_body, Ingest(doc_id="d1"), key="solo"))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


async def _node_row(pool: asyncpg.Pool, schema: str, flow_id: str, step_key: str) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"SELECT id, status, attempt, snooze_count, scheduled_at, result, "
        f'deps_pending, metadata FROM "{schema}".jobs '
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2 "
        "AND step_key <> '__flow__'",
        uuid.UUID(flow_id),
        step_key,
    )
    assert row is not None, f"the node row {step_key!r} must exist"
    meta = row["metadata"]
    return {
        **dict(row),
        "metadata": json.loads(meta) if isinstance(meta, str) else meta,
    }


async def _ledger_rows(
    pool: asyncpg.Pool, schema: str, flow_id: str, step_key: str
) -> list[asyncpg.Record]:
    return await pool.fetch(
        f'SELECT attempt, status, result, error_class FROM "{schema}".wf_step_ledger '
        f"WHERE flow_id = $1 AND step_key = $2 ORDER BY attempt",
        uuid.UUID(flow_id),
        step_key,
    )


async def _root_status(pool: asyncpg.Pool, schema: str, flow_id: str) -> str:
    return await pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(flow_id)
    )


# ── (1) the headline pin: the snooze defers, the budget survives ─────────


async def test_snooze_defers_the_row_and_never_burns_the_budget(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE HEADLINE PIN (the OCR-429 conviction's cure): a body raising
    ``Snooze`` re-schedules the node at the delay — the attempt UNCHANGED
    (the claim's increment refunded), the run alive, the ledger row
    rested ``awaited`` (never a failed row), the result capture empty —
    and the flow PROCEEDS TO TERMINAL after the delay."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("snooze_once_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    # THE FIRST TICK: the claim + the body's Snooze → the deferral lands.
    assert await runner.tick(flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "scheduled", (
        f"the deferral must land the row 'scheduled' (the ladder's own "
        f"live-row shape), got {row['status']!r}"
    )
    assert row["scheduled_at"] is not None
    wait_s = (
        row["scheduled_at"].replace(tzinfo=UTC)
        if row["scheduled_at"].tzinfo is None
        else row["scheduled_at"]
    ) - datetime.now(UTC)
    assert 0 < wait_s.total_seconds() <= FLOOR_S + 2.0, (
        f"the deferral must land at now + delay (the floor's own bound), got {wait_s}"
    )
    # THE ATTEMPT LAW: the claim's increment is refunded — the row's
    # attempt reads 0 (the next claim re-mints the SAME attempt ordinal).
    assert row["attempt"] == 0, (
        f"the snooze must refund the claim's increment, got {row['attempt']}"
    )
    assert row["result"] is None, "nothing succeeded — the result capture stays empty"
    # THE METADATA RECORD (the run page's visibility): the deferral stamp
    # — the signal, the budget face, and the disposition's OWN
    # post-attempt ordinal (the label's sitting-state discriminator).
    stamp = row["metadata"].get("wf_deferral")
    assert stamp == {"signal": "Snooze", "consume_budget": False, "attempt": 0}, stamp
    assert row["snooze_count"] == 1, "the row's deferral counter incremented"
    # THE LEDGER: the claim's row rested 'awaited' — never 'failed' (the
    # ladder's budget read counts failures only).
    (ledger,) = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert ledger["status"] == "awaited", ledger["status"]
    assert ledger["result"] is None
    # THE RUN STAYS LIVE.
    assert await _root_status(module_pg_pool, schema, flow_id) == "running"

    # THE FLOW PROCEEDS: past the delay, the tick re-claims the SAME
    # attempt ordinal and the body succeeds.
    await asyncio.sleep(FLOOR_S + 0.5)
    deadline_ticks = 10
    for _ in range(deadline_ticks):
        if not await runner.tick(flow_id):
            await asyncio.sleep(0.2)
            continue
        break
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "succeeded", row["status"]
    assert row["attempt"] == 1, (
        f"the re-claim re-minted the SAME attempt ordinal (the refund's "
        f"own law — the attempt never walked), got {row['attempt']}"
    )
    ledger_rows = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert len(ledger_rows) == 1, (
        f"the snooze must never grow the ledger (the same arbiter row "
        f"re-claimed), got {len(ledger_rows)} rows"
    )
    assert ledger_rows[0]["status"] == "succeeded"
    assert not [r for r in ledger_rows if r["status"] == "failed"], (
        "a snooze must never land a 'failed' ledger row (the budget law)"
    )


# ── (2) the consuming RetryAfter burns the attempt ───────────────────────


async def test_retry_after_consuming_keeps_the_attempt_increment(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE BURN PIN: ``RetryAfter(consume_budget=True)`` re-schedules at
    the delay KEEPING the claim's increment — the next claim mints the
    NEXT attempt ordinal (a new ledger arbiter row), the counter does not
    read as a snooze."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("retry_consuming_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)  # the deferral (attempt 1 kept)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    # THE ZERO-DELAY CONSUMING FACE (the vanilla's own CASE law): an
    # immediate consuming retry is a real execution — the row lands
    # 'pending' (immediately claimable), never parked at the floor.
    assert row["status"] == "pending", (
        f"a zero-delay consuming retry lands 'pending' (the immediate retry), got {row['status']!r}"
    )
    assert row["attempt"] == 1, (
        f"the consuming retry KEEPS the claim's increment (the burn), got {row['attempt']}"
    )
    assert row["snooze_count"] == 0, (
        "a consuming retry is not a snooze — the deferral counter must not move"
    )
    stamp = row["metadata"].get("wf_deferral")
    assert stamp == {"signal": "RetryAfter", "consume_budget": True, "attempt": 1}, stamp

    deadline_ticks = 10
    for _ in range(deadline_ticks):
        if not await runner.tick(flow_id):
            await asyncio.sleep(0.2)
            continue
        break
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "succeeded", row["status"]
    assert row["attempt"] == 2, "the re-claim minted the NEXT attempt (the burn's record)"
    ledger_rows = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert [r["attempt"] for r in ledger_rows] == [1, 2], (
        "the consuming deferral's re-claim is a NEW attempt arbiter row"
    )
    assert not [r for r in ledger_rows if r["status"] == "failed"]


async def test_retry_after_consuming_past_max_attempts_terminals_max_attempts(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE BUDGET'S BOUND (the vanilla ``mark_retry_after_consume_true``'s
    own ``max_attempts`` fence): a consuming retry past the budget is the
    terminal ``MaxAttemptsExceeded`` — the vanilla's own class + message —
    through the wf terminal door (the ledger's failed row, the node's
    record, the run fails)."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("retry_consuming_exhausted_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "failed", (
        f"the consuming retry past max_attempts terminal-fails the step, got {row['status']!r}"
    )
    assert row["attempt"] == 1  # the claim's increment (the burn) stands
    ledger_rows = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert ledger_rows[0]["status"] == "failed"
    assert ledger_rows[0]["error_class"] == "MaxAttemptsExceeded"
    assert await _root_status(module_pg_pool, schema, flow_id) == "failed"


# ── (3) the non-consuming RetryAfter is snooze semantics ─────────────────


async def test_retry_after_free_never_burns_the_budget(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """``RetryAfter(consume_budget=False)`` is the vanilla's own snooze
    semantics (docs/guides/retries.md: 'consume_budget=False: snooze
    semantics'): the increment refunds, ``max_attempts`` never moves — a
    max_attempts=1 step DEFERS and still succeeds."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("retry_free_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "scheduled"
    assert row["attempt"] == 0, "the non-consuming retry refunds the claim's increment"
    assert row["snooze_count"] == 1
    stamp = row["metadata"].get("wf_deferral")
    assert stamp == {"signal": "RetryAfter", "consume_budget": False, "attempt": 0}, stamp

    await asyncio.sleep(FLOOR_S + 0.5)
    deadline_ticks = 10
    for _ in range(deadline_ticks):
        if not await runner.tick(flow_id):
            await asyncio.sleep(0.2)
            continue
        break
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "succeeded", (
        f"a non-consuming retry must never exhaust max_attempts=1, got {row['status']!r}"
    )
    ledger_rows = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert not [r for r in ledger_rows if r["status"] == "failed"]


# ── (4) the budget law: snoozes alone never exhaust ──────────────────────


async def test_snoozing_past_max_attempts_never_exhausts(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE BUDGET LAW (the interaction's headline): a body snoozing FIVE
    times against ``max_attempts=2`` NEVER exhausts — a snooze never
    increments the attempt — and the run completes."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("snooze_five_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    # Five deferral rounds + the success round, each due after the floor.
    for _ in range(60):
        ran = await runner.tick(flow_id)
        row = await _node_row(module_pg_pool, schema, flow_id, "solo")
        if row["status"] == "succeeded":
            break
        await asyncio.sleep(0.2 if not ran else 0.0)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "succeeded", (
        f"five snoozes past max_attempts=2 must NEVER exhaust (the budget "
        f"law), got {row['status']!r}"
    )
    assert row["snooze_count"] == 5, row["snooze_count"]
    assert row["attempt"] == 1, "the attempt never walked past the first claim"
    ledger_rows = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert len(ledger_rows) == 1, "the refunds re-claim the SAME arbiter row"
    assert not [r for r in ledger_rows if r["status"] == "failed"]
    assert await _root_status(module_pg_pool, schema, flow_id) == "succeeded"


# ── (5) the regression fence: a non-signal exception still ladders ────────


async def test_non_signal_exception_still_burns_the_ladder(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE REGRESSION FENCE: a non-signal exception's ladder semantics are
    byte-unchanged — the failed ledger row lands (the burn), the attempt
    walks, the ladder's backoff re-pends. The deferral disposition must
    never swallow the ladder's own face."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("ladder_regression_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)  # the failure — the ladder re-pends
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "scheduled", "the ladder's own repend shape (unchanged)"
    assert row["attempt"] == 1, "the ladder does NOT refund — the attempt walked"
    assert row["snooze_count"] == 0, "a ladder retry is not a snooze"
    assert "wf_deferral" not in row["metadata"], "a ladder retry carries no deferral stamp"
    ledger_rows = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert ledger_rows[0]["status"] == "failed", "the burn is on the record"

    deadline_ticks = 10
    for _ in range(deadline_ticks):
        if not await runner.tick(flow_id):
            await asyncio.sleep(0.2)
            continue
        break
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "succeeded"
    assert row["attempt"] == 2


# ── (6) the join's accounting across a snoozed child ─────────────────────


async def test_snoozed_child_keeps_the_join_accounting(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE JOIN'S PENDING COUNTER: a snoozed step is still pending — the
    deferral writes NO decrement, NO fire; the join's ``deps_pending``
    accounting is UNCHANGED until the child's real terminal, then the join
    fires and the run completes."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("snooze_child_join_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)  # src succeeds; the child claims + snoozes
    assert await runner.tick(flow_id) is not None
    child = await _node_row(module_pg_pool, schema, flow_id, "child")
    assert child["status"] == "scheduled", child["status"]
    join = await _node_row(module_pg_pool, schema, flow_id, "join")
    assert join["deps_pending"] == 1, (
        f"the snoozed child's join counter is UNCHANGED (no decrement, no "
        f"fire), got {join['deps_pending']}"
    )
    assert await _root_status(module_pg_pool, schema, flow_id) == "running"

    await asyncio.sleep(FLOOR_S + 0.5)
    for _ in range(20):
        status = await _root_status(module_pg_pool, schema, flow_id)
        if status in ("succeeded", "failed", "cancelled"):
            break
        await runner.tick(flow_id)
        await asyncio.sleep(0.2)
    child = await _node_row(module_pg_pool, schema, flow_id, "child")
    assert child["status"] == "succeeded"
    join = await _node_row(module_pg_pool, schema, flow_id, "join")
    assert join["deps_pending"] == 0
    assert await _root_status(module_pg_pool, schema, flow_id) == "succeeded"


# ── (7) the zero-delay floor (the monopolisation guard) ──────────────────


async def test_zero_delay_snooze_carries_the_deferral_floor(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE FLOOR (the vanilla ``mark_snoozed``'s own guard, mirrored): a
    zero-delay snooze must not park the node at the head of the dispatch
    order — the non-consuming deferral lands at least
    ``MIN_DEFERRAL_INTERVAL`` out, status ``scheduled``."""
    schema = module_pg_schema.schema_name
    burns: list[int] = []

    async def _body(ctx: Any, params: Ingest) -> str:
        if not burns:
            burns.append(1)
            raise Snooze(timedelta(0))
        return "done"

    app_obj = WorkflowApp()

    @app_obj.workflow("snooze_zero_flow")
    def snooze_zero() -> Promise[object]:
        return build(step(_body, Ingest(doc_id="d1"), key="solo"))

    runner = FlowRunner(app_obj.get("snooze_zero_flow"), module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "scheduled", (
        f"the floored deferral lands 'scheduled' (never a pending-at-now "
        f"head-of-order park), got {row['status']!r}"
    )
    assert row["scheduled_at"] is not None
    wait_s = (
        row["scheduled_at"].replace(tzinfo=UTC)
        if row["scheduled_at"].tzinfo is None
        else row["scheduled_at"]
    ) - datetime.now(UTC)
    assert wait_s.total_seconds() >= FLOOR_S * 0.5, (
        f"the floored deferral must land at least MIN_DEFERRAL_INTERVAL out, got {wait_s}"
    )


# ── (8) the statement's fences (the cancel mark + the zombie) ─────────────


async def test_defer_fences_a_cancel_marked_row(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE CANCEL FENCE (the vanilla deferral's own law — 'an operator
    cancel in flight WINS over the deferral'): a node row carrying the wf
    cancel's own mark (``metadata.cancel_phase`` — the cancel cascade's
    cooperative stamp) never matches the deferral arm; the statement
    answers ``cancel_fenced`` and writes nothing — and the runner's route
    terminalises the node under the CANCEL's own vocabulary (the vanilla
    cancel ladder's face: the mark's row never reads as a body failure,
    and a body that ended on a refused deferral is never left wedged)."""
    from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

    schema = module_pg_schema.schema_name
    flow_id = await seed_flow(clean_pg_conn, schema, status="running")
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="solo")
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET metadata = metadata || '
        '\'{"cancel_phase": "cooperative"}\'::jsonb WHERE id = $1',
        node_id,
    )
    worker_id, attempt, epoch = await claim_view(clean_pg_conn, schema, node_id)
    # The claim's own ledger row (the record the terminal route keys —
    # the real claim's H6 law: the claim and the running ledger row are
    # one statement; the seed's shape supplies it here).
    from taskq.workflows.api._runner_defer import defer_node_row
    from taskq.workflows.engine import render_workflow_sql
    from taskq.workflows.ledger import claim_step_ledger

    wsql = render_workflow_sql(schema)
    claim = await claim_step_ledger(
        clean_pg_conn,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="solo",
        map_index=None,
        attempt=int(attempt),
    )
    compiled = sys.modules[MODULE_NAME].app.get("snooze_cancel_fence_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(
        compiled, module_pg_pool, schema, worker_id=worker_id
    )  # the fence binds the runner's own worker id — the seeded claim's.

    # THE STATEMENT'S VERDICT: the cancel wins, nothing lands.
    verdict = await defer_node_row(
        module_pg_pool,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        worker_id=worker_id,
        attempt=int(attempt),
        claim_epoch=int(epoch),
        disposition="snooze",
        delay=timedelta(seconds=FLOOR_S),
        signal_name="Snooze",
    )
    assert verdict == "cancel_fenced", verdict
    row = await _node_row(module_pg_pool, schema, str(flow_id), "solo")
    assert row["status"] == "running", "the fenced row stays running carrying its phase"
    assert row["metadata"].get("cancel_phase") == "cooperative", (
        "the cancel mark survives — the deferral must never launder it"
    )
    assert row["snooze_count"] == 0

    # THE RUNNER'S ROUTE: the cancel-fenced node terminalises 'cancelled'
    # through the wf terminal door — never wedged, never a body failure.
    label = await runner._defer_step(  # pyright: ignore[reportPrivateUsage]  # Why: the pins drive the runner's own seam directly — the mixin method is the machinery under test.
        flow_id,
        {"id": node_id, "step_key": "solo", "map_index": None},
        int(attempt),
        claim.ledger_id,
        Snooze(timedelta(seconds=FLOOR_S)),
        claim_epoch=int(epoch),
    )
    assert label == "cancelled"
    row = await _node_row(module_pg_pool, schema, str(flow_id), "solo")
    assert row["status"] == "cancelled", row["status"]
    ledger_rows = await _ledger_rows(module_pg_pool, schema, str(flow_id), "solo")
    assert ledger_rows[0]["status"] == "cancelled"
    assert ledger_rows[0]["error_class"] == "WorkflowCancelled"


async def test_defer_fences_a_zombie_attempt(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE CLAIM FENCE (the finalize's own CAS vocabulary): a deferral
    presenting a stale attempt/epoch — a zombie's — matches NOTHING (the
    statement answers no verdict, the row untouched), and the runner's
    route records the attempt 'fenced' on the ledger (H1 — never a
    running row left forever on the record)."""
    from taskq.workflows.api._runner_defer import defer_node_row
    from taskq.workflows.engine import render_workflow_sql
    from taskq.workflows.ledger import claim_step_ledger
    from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

    schema = module_pg_schema.schema_name
    flow_id = await seed_flow(clean_pg_conn, schema, status="running")
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="solo")
    worker_id, attempt, epoch = await claim_view(clean_pg_conn, schema, node_id)
    wsql = render_workflow_sql(schema)
    # The claim's own ledger row (the record the zombie's fence stamps).
    claim = await claim_step_ledger(
        clean_pg_conn,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="solo",
        map_index=None,
        attempt=int(attempt),
    )

    compiled = sys.modules[MODULE_NAME].app.get("snooze_zombie_fence_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(
        compiled, module_pg_pool, schema, worker_id=worker_id
    )  # the fence binds the runner's own worker id — the seeded claim's.

    # THE STATEMENT'S VERDICT: the zombie matches nothing.
    verdict = await defer_node_row(
        module_pg_pool,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        worker_id=worker_id,
        attempt=int(attempt) + 5,  # the zombie's stale attempt
        claim_epoch=int(epoch),
        disposition="snooze",
        delay=timedelta(seconds=FLOOR_S),
        signal_name="Snooze",
    )
    assert verdict is None, "the zombie's deferral must match no verdict row"
    row = await _node_row(module_pg_pool, schema, str(flow_id), "solo")
    assert row["status"] == "running", "the zombie's deferral moved nothing"
    assert row["snooze_count"] == 0

    # THE RUNNER'S ROUTE: the attempt records 'fenced' (H1's law — never
    # a running ledger row left forever).
    label = await runner._defer_step(  # pyright: ignore[reportPrivateUsage]  # Why: the pins drive the runner's own seam directly — the mixin method is the machinery under test.
        flow_id,
        {"id": node_id, "step_key": "solo", "map_index": None},
        int(attempt) + 5,
        claim.ledger_id,
        Snooze(timedelta(seconds=FLOOR_S)),
        claim_epoch=int(epoch),
    )
    assert label == "fenced"
    ledger_rows = await _ledger_rows(module_pg_pool, schema, str(flow_id), "solo")
    assert ledger_rows[0]["status"] == "fenced", ledger_rows[0]["status"]
    row = await _node_row(module_pg_pool, schema, str(flow_id), "solo")
    assert row["status"] == "running", "the winner's row is untouched"


# ── (9) the disposition resolver (the extension seam's own pin) ───────────


def test_the_signal_resolver_names_the_dispositions() -> None:
    """THE NAMED DISPOSITIONS (the extensibility law): the resolver maps
    the two signals onto the named dispositions — a Snooze and a
    non-consuming RetryAfter are the snooze semantics; a consuming
    RetryAfter is the budget-bounded retry. The NEXT deferral verdict adds
    a member, a statement arm, and a resolver entry — never a third
    special case."""
    from taskq.workflows.api._runner_defer import deferral_disposition

    assert deferral_disposition(Snooze(timedelta(seconds=1))) == "snooze"
    assert deferral_disposition(RetryAfter(timedelta(seconds=1), consume_budget=False)) == "snooze"
    assert (
        deferral_disposition(RetryAfter(timedelta(seconds=1), consume_budget=True))
        == "retry_after_consuming"
    )
