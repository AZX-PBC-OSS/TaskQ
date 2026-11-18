# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The emit diagnostics' shared-contract controls (QWEN718_DIFF_REVIEW's
M-3, M-4, M-5 — all binding).

* M-3: ONE `taskq.workflows.wf_not_draining` counter declaration
  (exported from `workflows/_emit.py`, imported by the maintenance seat —
  never a second creation), with ONE attribute contract: both seats emit
  exactly ``error_class`` (the `WorkflowNotDrainingError` constant), so
  an error_class-filtered alert sees every refusal from either seat.
* M-4: the global drain's per-flow failure handler logs the siblings'
  diagnostic fields (`error_class` + a bounded `error`), so a typed
  `EmitCapacityCorruptionError` from the drain's own doors is NAMED, not
  anonymised into a re-offering warning.
* M-5: the park verdict's lost-exact-current-ledger-claim corruption
  raises the TYPED `EmitCapacityCorruptionError` — a
  `_deterministic_classes()` member — so the corruption terminalises on
  the deterministic route instead of riding the transient ladder.

Mutation notes: recreating the counter (or dropping either seat's
`error_class` attribute) reds the static one-declaration scan and the
spy's attribute assertions; removing the handler's diagnostic fields reds
the structlog capture; restoring the bare `RuntimeError` reds the typed
raise control.
"""

from __future__ import annotations

import json as _json
from typing import Any

import asyncpg
import pytest

from taskq.backend._protocol import JobId
from taskq.constants import WORKFLOW_NOT_DRAINING_CLASS
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


class _CounterSpy:
    """The ONE instrument's stand-in: records every seat's (amount,
    attributes) add through the module's own global."""

    def __init__(self) -> None:
        self.adds: list[tuple[int, dict[str, str]]] = []

    def add(self, amount: int, attributes: dict[str, str] | None = None) -> None:
        self.adds.append((amount, dict(attributes or {})))


class _CaptureLogger:
    """A structlog stand-in recording every call's event + fields (the
    handler's own log face)."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        def _log(event: str, **kw: Any) -> None:
            self.records.append({"event_": event, **kw})

        return _log


def _park_meta(flow_id: str, *, width: int, bound: int) -> dict[str, Any]:
    return {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
        "emit_cursor": {"page": 0},
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": width,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": "2026-11-18T09:30:00.123456+00:00",
        },
        "wf_source_logical_attempt": 1,
    }


async def _seed_not_draining_estate(conn: asyncpg.Connection, schema: str) -> tuple[str, str]:
    """The §5.4 proof's own estate: a running root, a genuinely exhausted
    failed child, and the exact parked source still holding un-emitted
    work with its current awaited claim (the causal child id is the failed
    child)."""
    from taskq._ids import new_uuid

    flow = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'r17', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
        flow,
        _json.dumps({"flow_id": flow}),
        flow,
        f"flow:{flow}",
    )
    child = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, error_class, error_message, "
        "finished_at, metadata, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r17', 'default', '{}', 'failed', 1, 1, 'transient', 'screen', "
        "'RuntimeError', 'the genuine exhausted failure', clock_timestamp(), "
        "$2::jsonb, $3, 'workflow-run', $4)",
        child,
        _json.dumps({"flow_id": flow}),
        flow,
        f"wf:{flow}:screen:{child}",
    )
    source = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, assignment_routed, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'r17', 'default', '{}', 'scheduled', 0, 2, 'transient', "
        "'source', $2::jsonb, clock_timestamp(), clock_timestamp(), $3, true, "
        "'workflow-run', $4)",
        source,
        _json.dumps(_park_meta(flow, width=2, bound=10)),
        flow,
        f"wf:{flow}:source:{source}",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "map_index, attempt, claim_epoch, status, created_at, updated_at) "
        "VALUES ($1, $2, $3, 'source', NULL, 0, 1, 'awaited', clock_timestamp(), "
        "clock_timestamp())",
        str(new_uuid()),
        flow,
        source,
    )
    return flow, child


class TestOneNotDrainingCounter:
    """M-3: one declaration, one attribute contract, both seats."""

    def test_one_declaration_in_the_product(self) -> None:
        """Exactly one `create_counter` names the counter in the product
        source, exported from `_emit.py`; the maintenance seat has no
        creation of its own."""
        from pathlib import Path

        from taskq.workflows import _emit

        creations: list[str] = []
        for path in Path("src/taskq").rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if 'name="taskq.workflows.wf_not_draining"' in text:
                creations.append(str(path))
        assert creations == ["src/taskq/workflows/_emit.py"], creations
        sweep = Path("src/taskq/workflows/_sweep.py").read_text(encoding="utf-8")
        assert "create_counter" not in sweep, (
            "the maintenance seat declares its own instrument — the M-3 divergence"
        )
        assert "from taskq.workflows._emit import NOT_DRAINING_COUNTER" in sweep, sweep
        assert "NOT_DRAINING_COUNTER" in _emit.__all__

    async def test_both_seats_emit_the_same_attribute_through_one_instrument(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Driving BOTH seats with a spy on the one global: the cascade
        seat and the maintenance seat each add through the SAME
        instrument with the SAME attribute set (``error_class`` always
        present and always the constant)."""
        from taskq.workflows import _emit
        from taskq.workflows._emit import fence_not_draining_parked_sources
        from taskq.workflows._sweep import sweep_join_rederive
        from taskq.workflows.engine import render_workflow_sql

        schema = module_pg_schema.schema_name
        wsql = render_workflow_sql(schema)
        spy = _CounterSpy()
        monkeypatch.setattr(_emit, "NOT_DRAINING_COUNTER", spy)

        # THE CASCADE SEAT (the failure cascade's own transaction's fence
        # call — the caller owns the exhausted-child proof).
        async with module_pg_pool.acquire() as conn:
            flow_a, child_a = await _seed_not_draining_estate(conn, schema)
            fenced = await fence_not_draining_parked_sources(
                conn, wsql, flow_id=JobId(flow_a), child_id=JobId(child_a)
            )
        assert fenced == 1, fenced

        # THE MAINTENANCE SEAT: the derivation's own failure arm — the
        # root's maintain leg terminal-fails behind the exhausted child
        # and the SAME transaction fences the parked source through the
        # migration's own fence function.
        async with module_pg_pool.acquire() as conn:
            flow_b, _child_b = await _seed_not_draining_estate(conn, schema)
        await sweep_join_rederive(module_pg_pool, wsql, batch_size=10)
        maintained = await module_pg_pool.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid', flow_b
        )
        assert maintained == "failed", maintained

        assert len(spy.adds) == 2, spy.adds
        for amount, attributes in spy.adds:
            assert amount == 1, spy.adds
            assert attributes == {"error_class": WORKFLOW_NOT_DRAINING_CLASS}, spy.adds


class TestDrainFlowFailureDiagnostics:
    """M-4: the per-flow drain handler names the failure like its
    siblings."""

    async def test_per_flow_drain_failure_logs_error_class_and_error(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A flow whose drain fails (its root row is absent — the typed
        corruption door) is logged with `error_class` and a bounded
        `error`, the fields both sibling seats carry; the rotation still
        offers the flow (the advancement law is untouched)."""
        from taskq.workflows import _emit
        from taskq.workflows._emit import drain_pending_emit_intents
        from taskq.workflows.engine import render_workflow_sql

        schema = module_pg_schema.schema_name
        wsql = render_workflow_sql(schema)
        capture = _CaptureLogger()
        monkeypatch.setattr(_emit, "logger", capture)
        # The pending intent's flow has NO root row (the drain's typed
        # corruption door refuses before any read pair).
        missing_flow_id = await module_pg_pool.fetchval(
            f'INSERT INTO "{schema}".wf_emit_capacity (flow_id, bound, '
            "wake_generation, handled_generation) "
            "VALUES (gen_random_uuid(), 2, 3, 2) RETURNING flow_id::text"
        )
        offered = await drain_pending_emit_intents(module_pg_pool, wsql, trigger="notification")
        assert offered >= 1
        failures = [r for r in capture.records if r["event_"] == "wf.emit_intent_drain_flow_failed"]
        assert failures, capture.records
        record = failures[0]
        assert record["flow_id"] == str(missing_flow_id), record
        assert record["offered_anyway"] is True, record
        # THE SIBLINGS' DIAGNOSTIC CONTRACT: the failure is NAMED.
        assert record["error_class"] == "EmitCapacityCorruptionError", record
        assert "no live root row" in record["error"], record
        assert len(record["error"]) <= 300, record


class TestParkLedgerCorruptionIsTyped:
    """M-5: the park verdict's lost exact-current-ledger-claim corruption
    raises the typed `EmitCapacityCorruptionError` (a deterministic-class
    member), never a bare `RuntimeError`."""

    async def test_lost_ledger_claim_raises_the_typed_corruption_class(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
        wf_sql: WorkflowSql,
    ) -> None:
        """The park's exact-claim ledger move matches ZERO rows (the
        current ledger claim's own instant was disturbed after the
        claim): `_defer_step` raises the TYPED corruption class — the
        deterministic route's own vocabulary — and the estate shows no
        partial park."""
        from taskq.workflows import EmitBackpressure
        from taskq.workflows._emit import EmitCapacityCorruptionError
        from taskq.workflows.api._runner import FlowRunner
        from taskq.workflows.engine import render_workflow_sql
        from tests.test_wf_source_attempts import _claimed

        schema = module_pg_schema.schema_name
        wsql = render_workflow_sql(schema)
        async with module_pg_pool.acquire() as conn:
            flow_id, claim = await _claimed(conn, schema, wf_sql)
            raw = await conn.fetchrow(
                f'SELECT * FROM "{schema}".jobs WHERE id = $1::uuid', claim.job_id
            )
        assert raw is not None
        # THE CORRUPTION: the current ledger claim's own instant moves
        # after the claim — the park's exact-claim fence (the ledger id
        # AND its claimed instant) can no longer take the single awaited
        # transition.
        await module_pg_pool.execute(
            f"""UPDATE "{schema}".wf_step_ledger
                SET updated_at = clock_timestamp() + interval '1 microsecond'
                WHERE id = $1::uuid""",
            claim.ledger_id,
        )
        runner = FlowRunner.__new__(FlowRunner)
        runner.pool = module_pg_pool  # type: ignore[attr-defined]  # Why: the park door's own dependencies; the control's subject is the verdict raise, not the host graph.
        runner.wsql = wsql  # type: ignore[attr-defined]  # Why: ditto — the statement bundle.
        runner._worker_id = claim.worker_id  # type: ignore[attr-defined]  # Why: the park's fence binds the claim's worker id.
        with pytest.raises(EmitCapacityCorruptionError) as exc_info:
            await runner._defer_step(  # type: ignore[attr-defined]  # Why: the control/cancel door under test — the same private-drive shape the shipped cancel-fenced park control uses.
                flow_id,
                dict(raw),
                claim.attempt,
                claim.ledger_id,
                EmitBackpressure(page_width=1, bound=10, outstanding=1),
                claim_epoch=claim.claim_epoch,
                ledger_claimed_at=claim.ledger_claimed_at,
            )
        assert type(exc_info.value) is EmitCapacityCorruptionError, type(exc_info.value)
        # No partial park: the row is untouched (the park's transaction
        # rolled back whole).
        row = await module_pg_pool.fetchrow(
            f'SELECT status::text AS status, metadata FROM "{schema}".jobs WHERE id = $1::uuid',
            claim.job_id,
        )
        assert row is not None and row["status"] == "running", row
        meta = row["metadata"]
        if isinstance(meta, str):
            meta = _json.loads(meta)
        assert "blocking_reason" not in meta, meta
