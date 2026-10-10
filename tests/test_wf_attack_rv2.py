# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""ATTACK PINS — the RE-REVIEW round's convictions (feat/taskqflow @
d24f17b9; the second hostile pass over the consolidated head).

Provenance: the rv2 front's live convictions, one pin each. Per the
doctrine, a LANDED finding (a live defect at d24f17b9) is pinned
asserting the SAFE behavior under ``pytest.mark.xfail(strict=True)`` —
the cure flips the pin to XPASS-strict (a red that says: remove the
marker WITH the cure); a finding whose RESOLVED behavior already holds
at the head (rv2-11 — the battery's deterministic red, convicted as a
bad declaration in the BATTERY's own test) stands GREEN as the guard
that keeps the resolution from rotting.

The findings (the front's receipts carried each conviction live):

* F-RV2-1 — the admin panel/run-page bound ``error_message`` /
  ``error_traceback`` / ``captured_error`` (the Q5 read-side cure) but
  ``error_class`` (unbounded text) still round-trips WHOLE:
  ``_wf_rows.py``'s ``_run_view_from_rows`` (node + root) and
  ``_wf_actions.py``'s node-panel route (the detail AND the children
  census) serve it raw.
* F-RV2-2 — ``reap_nodeless_roots`` returns ``int(fetchval(RETURNING
  f.id))`` — the FIRST reaped row's UUID as a 128-bit int — as the
  "reaped count" metric.
* F-RV2-3 — ``step(gates=(<a TypedGate from channel.gate(...)>,))``
  (the natural door-confusion: ``TypedGate`` instead of ``GateDecl``)
  crashes ``validate_compiled`` with a raw ``AttributeError``
  (``_rule_eternal_wait`` reads ``gate.timeout_s``) instead of a NAMED
  E-rule refusal.
* F-RV2-4 — ``taskq.audit.record_admin_action`` binds ``reason`` /
  ``detail`` VERBATIM into the never-pruned ``admin_audit`` (a 5MB
  reason lands whole); and a NUL byte in the reason on the workflow
  cancel path rolls the WHOLE cancel back with an opaque
  ``CharacterNotInRepertoireError`` (the jobs-cancel route carries the
  ``parse_text_filter`` NUL guard; the workflow route doesn't).
* F-RV2-5 — ``DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL`` drops the
  ``deps_pending = 0`` filter AND the workflow fence (the ``__flow__``
  exclusion + the terminal-flow exclusion) that the plain probe
  carries — against the module's own "the two fences may not disagree"
  invariant.
* F-RV2-6 — ``_dispatch_batch``'s expansion loop flips ``sql_stmt`` to
  the cursor render while ``round_bound`` is live but never flips it
  BACK: a cursor entry the jitter reset expires BETWEEN iterations
  calls the ``$6`` statement with 5 args (asyncpg ``InterfaceError``),
  while the comment at the loop head claims the plain shape runs.
* F-RV2-7 — ``obs._claim_health.claim_health_snapshot`` iterates the
  module-global ``_window`` deque while ``record_claim_latency``
  mutates it: concurrent record+scrape raises ``RuntimeError: deque
  mutated during iteration`` (hundreds of times per second under
  thread-level concurrency).
* F-RV2-8 — a ``Promise`` NESTED in a dict/list ``loop(initial=...)``
  carry passes validation clean (E11's ``isinstance`` reads only the
  top level) and dies at the first claim with ``UnencodableValue``
  (the jsonb bind refusing the handle) — a claim→crash→reclaim loop
  classified infra-fault.
* F-RV2-9 — E10 counts ANNOTATED params (``typing.get_type_hints``),
  not ACTUAL params: an unannotated-but-runnable body wired correctly
  is refused with "takes 0 param(s)" — the message lies about the real
  arity, against the validator's zero-false-positive doctrine.
* F-RV2-10 — the deploy-matrix header (tests/system_e2e/
  test_wf_deploy_matrix.py) lists 9 cells; cells 4 (OUTAGE), 6
  (REDISPATCH OWNERSHIP) and 9 (RETENTION MID-FLIGHT) have no test
  function anywhere, and the header cites a red-drills file and a
  deployment.md "operator table" that do not exist.
* F-RV2-11 — the battery's deterministic red
  (``test_wf_execution_py.py::test_projection_the_split_placement_
  cohorts``): the test's chain-source declaration wires a
  ``(ctx, params)`` body with ZERO args — the runtime invokes source
  bodies with ctx alone, so the declaration IS a wiring lie and E10's
  conviction is correct; the RESOLVED behavior (a ctx-only source
  body) is pinned green.
* F-RV2-12 — ``tests/typeprobe/wf_gather_negative_types.py`` is a
  ZOMBIE probe: on disk, carrying live MUST_ERROR markers, wired
  nowhere (not in ``_gate.py``'s ``_CORPUS``, so not in the CI gate).
* F-RV2-13 — the head-stamp law's own verifier
  (``scripts/verify_evidence_heads.py``) reds on the committed
  ``.measurements/runs`` at the head (unstamped + stale live claims).
* F-RV2-14 — ``perf-evidence-workflows-streaming.md``'s emit paragraph
  cites "max 10.4\u201314.5 ms across the five captured runs"; the capture
  ``t20-streaming-bands-20261008-052519.json`` carries max 20.569 —
  the cited range does not cover the evidence.
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    Done,
    Refine,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    loop,
    sink,
    step,
)
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.api._validate import WorkflowValidationError
from tests._wf_fixtures import seed_flow

#: The estate's own display-bound conventions (the panel's
#: ``_PANEL_FIELD_CAP_CHARS``): the SAFE bound any cure of the
#: error_class/audit binds lands under. The pin asserts the CEILING of
#: the house's bounded conventions, never a specific one — a tighter
#: cure (the 512 of the cancel form's maxlength, the 2000 of the rows
#: traceback cap) satisfies it too.
_BOUND_CEILING_CHARS = 10_000

#: The honesty marker's shape, both house spellings (the panel's
#: "… [truncated: +N characters stay in the row]" and the rows cap's
#: "... (N more characters)") — the cure must NAME the truncation.
_TRUNCATION_MARKER = re.compile(r"truncat|more characters", re.IGNORECASE)


class _Ingest(BaseModel):
    doc_id: str


class _Approval(BaseModel):
    verdict: str


class _Report(BaseModel):
    ref: str


# ── F-RV2-1: the unbounded error_class, both raw surfaces ───────────────


async def _seed_failed_run_with_a_hostile_error_class(
    conn: asyncpg.Connection, schema: str
) -> tuple[JobId, str]:
    """A failed run (root + TWO same-key map siblings, so the panel's
    children census renders) whose ``error_class`` columns carry a
    200KB string — the Q5 comment's own threat model: "a row written by
    ANY other path — or a hostile DB write" (the write-side caps never
    saw it). Returns (flow_id, node_key)."""
    flow_id = await seed_flow(conn, schema, status="failed")
    hostile = "Hostile" + "E" * 200_000
    await conn.execute(
        f'UPDATE "{schema}".jobs SET error_class = $2 WHERE id = $1',
        flow_id,
        hostile,
    )
    for map_index in (0, 1):
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, deps_pending, map_index, metadata, "
            "error_class, error_message) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'failed', 'n1', 0, "
            "$2, $3::jsonb, $4, 'boom')",
            new_uuid(),
            map_index,
            json.dumps({"flow_id": str(flow_id)}),
            hostile,
        )
    return flow_id, "n1"


@pytest.mark.integration
@pytest.mark.fastapi
async def test_rv2_1_the_run_view_bounds_error_class(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """THE READ-SIDE BOUND'S RESIDUE: the run view (the run page's and
    the SSE snapshot's shared read) must serve ``error_class`` BOUNDED
    with the truncation named — the same law the sibling error fields
    already keep. TODAY: 200KB in, 200KB out, no marker."""
    pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
    from taskq.web.admin._wf_rows import fetch_run_view

    flow_id, _node_key = await _seed_failed_run_with_a_hostile_error_class(wf_conn, wf_schema)
    view = await fetch_run_view(wf_conn, wf_schema, uuid.UUID(str(flow_id)))
    assert view is not None and view.nodes, "the seeded run must assemble"

    served = [("root", view.error_class), *[(n.key, n.error_class) for n in view.nodes]]
    for label, error_class in served:
        assert error_class is not None
        assert len(error_class) <= _BOUND_CEILING_CHARS, (
            f"F-RV2-1 ({label}): the run view served error_class UNBOUNDED "
            f"({len(error_class)} chars) — the panel/page bound covers "
            "error_message but the class field rides raw"
        )
        assert _TRUNCATION_MARKER.search(error_class), (
            f"F-RV2-1 ({label}): the bounded error_class does not NAME the "
            "truncation (the honesty marker the sibling fields carry)"
        )


@pytest.mark.integration
@pytest.mark.fastapi
async def test_rv2_1_the_node_panel_route_bounds_error_class(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The click→panel surface: ``GET /api/runs/{run}/nodes/{key}`` must
    answer with every ``error_class`` (the detail's and the children
    census's) BOUNDED + the truncation named. TODAY: raw on both."""
    pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
    httpx = pytest.importorskip("httpx")
    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    flow_id, node_key = await _seed_failed_run_with_a_hostile_error_class(wf_conn, wf_schema)
    bundle = create_router(wf_pool, schema=wf_schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{flow_id}/nodes/{node_key}")
    assert resp.status_code == 200, resp.text[:500]
    payload = resp.json()
    served = [("detail", payload["error_class"])]
    served += [(f"child[{c['map_index']}]", c["error_class"]) for c in payload["children"]]
    assert len(served) == 3, f"the census leg must render (got {served!r})"
    for label, error_class in served:
        assert isinstance(error_class, str)
        assert len(error_class) <= _BOUND_CEILING_CHARS, (
            f"F-RV2-1 (panel {label}): error_class served UNBOUNDED ({len(error_class)} chars)"
        )
        assert _TRUNCATION_MARKER.search(error_class), (
            f"F-RV2-1 (panel {label}): the bounded error_class does not NAME the truncation"
        )


# ── F-RV2-2: the reap arm's count is a UUID-as-int ──────────────────────


@pytest.mark.integration
async def test_rv2_2_the_nodeless_reap_returns_the_reaped_count(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The arm's own contract ("Returns the reaped count"): one nodeless
    root past the grace reaped → the return IS 1, equal to the rows the
    arm actually terminalized. TODAY: the return is the first reaped
    row's UUID as an int (``int()`` over the ``RETURNING f.id``
    fetchval) — a 39-digit lie the metric stream records."""
    from taskq.workflows._sweep import reap_nodeless_roots

    orphan = await seed_flow(wf_conn, wf_schema, status="running")
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET created_at = now() - interval '1 hour' WHERE id = $1",
        orphan,
    )
    reaped = await reap_nodeless_roots(wf_pool, wf_sql, grace_s=0.0)
    actually_reaped = await wf_conn.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE error_class = 'NodelessRunReaped'"
    )
    assert actually_reaped == 1, "the arm must reap the orphan (its own contract)"
    assert reaped == actually_reaped, (
        f"F-RV2-2: the arm reports {reaped} reaped but the rows say "
        f"{actually_reaped} — the metric carries int(first-reaped-uuid), "
        "not the count"
    )


# ── F-RV2-3: a TypedGate in step(gates=...) is a NAMED refusal ──────────


async def _hold_body(ctx: StepContext, params: _Ingest) -> Any:
    return await ctx.wait_signal(_Approval, timeout_s=120.0)


def test_rv2_3_a_typed_gate_in_step_gates_is_a_named_refusal() -> None:
    """The author's mistake — ``gates=(app.channel().gate(Model),)``,
    the BOUND-DOOR object where the declaration (``GateDecl``) belongs —
    must be refused by NAME (the validator's report, or the wiring
    door's build refusal), NEVER a raw ``AttributeError`` traceback out
    of the validator's own rule walk."""
    app = WorkflowApp()
    gate = app.channel().gate(_Approval)  # the TypedGate — the wrong door's object

    @app.workflow("rv2_typed_gate_mistake")
    def _wf() -> object:
        return build(step(_hold_body, _Ingest(doc_id="d"), key="review", gates=(gate,)))

    with pytest.raises((WorkflowValidationError, WorkflowBuildError)):
        app.get("rv2_typed_gate_mistake")


# ── F-RV2-4: the audit trail's bounds + the NUL-proof cancel ────────────


@pytest.mark.integration
async def test_rv2_4a_the_audit_reason_and_detail_are_bounded(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The audit row's free-text fields carry the same shape discipline
    the subject column already keeps: BOUNDED, with the truncation
    NAMED on the row. A 5MB reason/detail must not land whole in the
    never-pruned trail. TODAY: verbatim, unbounded, unmarked."""
    from taskq.audit import record_admin_action

    target = str(new_uuid())
    async with wf_conn.transaction():
        await record_admin_action(
            wf_conn,
            schema=wf_schema,
            principal="attacker",
            action="workflow.cancel",
            target_type="workflow_run",
            target_id=target,
            reason="x" * 5_000_000,
            detail={"blob": "y" * 5_000_000},
        )
    row = await wf_conn.fetchrow(
        f'SELECT reason, detail FROM "{wf_schema}".admin_audit WHERE target_id = $1',
        target,
    )
    assert row is not None, "the audit row must land (the trail's whole point)"
    reason = row["reason"] or ""
    detail_text = row["detail"] if isinstance(row["detail"], str) else json.dumps(row["detail"])
    assert len(reason) <= _BOUND_CEILING_CHARS, (
        f"F-RV2-4a: the audit reason landed UNBOUNDED ({len(reason)} chars) "
        "in the never-pruned trail"
    )
    assert _TRUNCATION_MARKER.search(reason), (
        "F-RV2-4a: the bounded reason does not NAME the truncation"
    )
    assert len(detail_text) <= _BOUND_CEILING_CHARS, (
        f"F-RV2-4a: the audit detail landed UNBOUNDED ({len(detail_text)} chars)"
    )
    assert _TRUNCATION_MARKER.search(detail_text), (
        "F-RV2-4a: the bounded detail does not NAME the truncation"
    )


@pytest.mark.integration
async def test_rv2_4b_a_nul_reason_never_aborts_the_wf_cancel(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The cancel is the operator's safety verb — a control character in
    the free-text reason must never make it fail opaque and WHOLE (the
    flip, the nodes, the signals AND the audit row rolled back). The
    reason is SANITIZED (the audit module's own control-escape
    discipline for the subject column) and the cancel LANDS. TODAY:
    ``invalid byte sequence for encoding "UTF8": 0x00`` out of the
    driver's bind, root still 'running'."""
    from taskq.workflows.api._runner_exit import cancel_workflow_run

    flow_id = await seed_flow(wf_conn, wf_schema, status="running")
    cancelled = await cancel_workflow_run(
        wf_pool,
        schema=wf_schema,
        flow_id=flow_id,
        reason="bad\x00reason",
        principal="attacker",
    )
    assert cancelled >= 1, "the cancel must LAND — the NUL reason sanitized, never the abort"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "cancelled", f"the cancel tore or never landed: root={root!r}"
    audit_reason = await wf_conn.fetchval(
        f'SELECT reason FROM "{wf_schema}".admin_audit WHERE target_id = $1',
        str(flow_id),
    )
    assert audit_reason is not None, "the audit row must land with the cancel (G4)"
    assert "\x00" not in audit_reason and "bad" in audit_reason, (
        f"F-RV2-4b: the audit row must carry the SANITIZED reason "
        f"(the NUL escaped/stripped, the content kept) — got {audit_reason[:80]!r}"
    )


# ── F-RV2-5: the cursor probe's fence must BE the plain probe's ─────────

#: The seeded trio (each leg alone, one sub-case per isolation): the
#: shapes the PLAIN probe's fence excludes (deps_pending=0 + the
#: workflow fence) and the cursor probe must exclude identically.


async def _probe_pair(conn: asyncpg.Connection, sql: Any) -> tuple[int, int]:
    """(plain probe row count, cursor probe row count) over the seeded
    world, the cursor bound at the nil UUID (every row is at/above it)."""
    plain = await conn.fetch(sql.dispatch_claimable_probe, ["default"])
    cursor = await conn.fetch(sql.dispatch_claimable_probe_cursor, ["default"], uuid.UUID(int=0))
    return len(plain), len(cursor)


@pytest.mark.integration
async def test_rv2_5_the_cursor_probe_fences_identically_to_the_plain_probe(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """THE FENCE-AGREEMENT LAW (the module's own, at
    ``_WF_PROBE_FENCE_TEMPLATE``): the cursor probe and the plain probe
    answer "is anything routable" over the SAME candidacy — a row the
    claim statement must never admit (a ``__flow__`` root — never work;
    a terminal flow's pending node — the terminal fence; a
    ``deps_pending > 0`` join-wait — not ready) counts in NEITHER. TODAY
    each leg reads (plain 0, cursor 1): the cursor render carries NEITHER
    ``deps_pending = 0`` NOR the ``__WF_FENCE_J__`` leg."""
    from taskq.backend._sql_templates import render

    sql = render(wf_schema)
    # The 'flow' actor registered (production stamps it at worker boot —
    # the probe joins actor_config, so the root row is probe-visible).
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ('flow', 'default') ON CONFLICT DO NOTHING"
    )

    async def seed_root(flow_id: uuid.UUID, status: str) -> None:
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', $2, '__flow__', "
            "$3::jsonb, 'workflow-run', $4)",
            flow_id,
            status,
            json.dumps({"flow_id": str(flow_id)}),
            f"flow:{flow_id}",
        )

    async def seed_node(flow_id: uuid.UUID, step_key: str, deps: int) -> None:
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, deps_pending, metadata) "
            "VALUES ($1, 'actor_a', 'default', '{}', 3, 'transient', 'pending', $2, $3, "
            "$4::jsonb)",
            new_uuid(),
            step_key,
            deps,
            json.dumps({"flow_id": str(flow_id)}),
        )

    legs: list[str] = []

    # LEG 1: a live __flow__ root alone (the root is never work).
    root = new_uuid()
    await seed_root(root, "pending")
    legs.append(f"__flow__ root: plain/cursor = {await _probe_pair(wf_conn, sql)}")
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".jobs')

    # LEG 2: a terminal flow's pending node alone (the terminal fence).
    dead = new_uuid()
    await seed_root(dead, "failed")
    await seed_node(dead, "tnode", 0)
    legs.append(f"terminal-flow node: plain/cursor = {await _probe_pair(wf_conn, sql)}")
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".jobs')

    # LEG 3: a deps_pending=1 join-wait on a live flow (not ready).
    live = new_uuid()
    await seed_root(live, "running")
    await seed_node(live, "dnode", 1)
    legs.append(f"deps_pending=1 node: plain/cursor = {await _probe_pair(wf_conn, sql)}")

    disagreements = [leg for leg in legs if not leg.endswith("(0, 0)")]
    assert not disagreements, (
        "F-RV2-5: the cursor probe's world disagrees with the plain probe's "
        "(each seeded shape must read 0 rows in BOTH — the claim's candidacy "
        "the probe arbitrates for):\n  - " + "\n  - ".join(legs)
    )


# ── F-RV2-6: the expired cursor mid-expansion is a NAMED degradation ────


class _AcquireCtx:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def __aenter__(self) -> Any:
        return self._conn

    async def __aexit__(self, *exc: Any) -> bool:
        return False


class _StubPool:
    """The dispatcher pool's shape (acquire → async CM of the conn)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def acquire(self, timeout: float | None = None) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


class _StrictFifoModeCache:
    """The queue-mode cache's shape: one strict_fifo queue, never a miss."""

    def resolved_modes(self, queues: list[str]) -> set[str]:
        return {"strict_fifo"}

    def store(self, modes_by_queue: dict[str, str]) -> None:
        pass


def test_rv2_6_the_expired_cursor_mid_expansion_is_a_named_degradation() -> None:
    """The round's own comment's claim ("the next attempt must then run
    the plain shape, not a stale bound") made TRUE — or the degradation
    NAMED. The driver below models the expiry-mid-expansion honestly:
    a REAL ClaimCursor on a controllable clock (armed, then the jitter
    reset fires between the expansion loop's iterations) and a conn
    that enforces the wire's arity contract exactly as asyncpg does
    (N distinct ``$n`` bind N args, else ``InterfaceError``) while
    answering every probe "rows remain" so the loop expands. TODAY the
    second iteration presents (the cursor render, 5 args) and the round
    dies on the driver's ``InterfaceError`` — loud, self-healing next
    round, and the exact opposite of the comment's claim."""
    from taskq.backend._claim_cursor import ClaimCursor
    from taskq.backend._dispatch import _dispatch_batch
    from taskq.backend._sql_templates import render

    sql = render("rv2pin_wire")  # the statements' TEXT is the subject; the stub conn is the wire

    class _WireConn:
        """The wire, honestly: the probe statements answer "rows remain"
        (the expansion's driver); the claim statement enforces asyncpg's
        arity contract."""

        def __init__(self) -> None:
            self.pairings: list[tuple[bool, int]] = []

        async def fetch(self, stmt: str, *args: Any) -> list[Any]:
            if stmt in (sql.dispatch_claimable_probe, sql.dispatch_claimable_probe_cursor):
                return [("row",)]
            declared = max((int(m) for m in re.findall(r"\$(\d+)", stmt)), default=0)
            self.pairings.append(("$6" in stmt, len(args)))
            if len(args) != declared:
                raise asyncpg.InterfaceError(
                    f"the server requires {declared} parameters, {len(args)} were given"
                )
            return []

        def terminate(self) -> None:
            pass

    class _ExpiringCursor:
        """A REAL ClaimCursor whose entry the jitter reset expires
        BETWEEN the loop's bound reads (the clock steps past the entry's
        armed expiry at the second read — the comment's own scenario)."""

        def __init__(self) -> None:
            self._now = [1000.0]
            self._inner = ClaimCursor(
                reset_seconds=60.0, clock=lambda: self._now[0], jitter=lambda: 1.0
            )
            self._inner.advance("q1", new_uuid())
            self._reads = 0

        def bound(self, queue: str) -> uuid.UUID | None:
            self._reads += 1
            if self._reads >= 2:
                self._now[0] = 2000.0
            return self._inner.bound(queue)

        def advance(self, queue: str, job_id: uuid.UUID) -> None:
            self._inner.advance(queue, job_id)

    conn = _WireConn()
    raised: BaseException | None = None

    async def _drive() -> None:
        from datetime import timedelta

        await _dispatch_batch(
            _StubPool(conn),
            sql,
            2,
            5.0,
            "rv2pin_wire",
            new_uuid(),
            ["q1"],
            4,
            timedelta(seconds=90),
            queue_mode_cache=_StrictFifoModeCache(),
            claim_cursor=_ExpiringCursor(),  # type: ignore[arg-type]  # Why: the duck IS the contract (bound/advance); the pin's seam.
        )

    try:
        asyncio.run(_drive())
    except BaseException as exc:  # Why: the pin's subject IS the exception's class.
        raised = exc
    assert not isinstance(raised, asyncpg.InterfaceError), (
        f"F-RV2-6: the expired-cursor round died on the RAW driver arity error "
        f"({raised}) — the loop presented the $6 cursor render with 5 args "
        f"(pairings: {conn.pairings}); the comment at the loop head claims the "
        "plain shape runs, and nothing names the degradation"
    )
    for is_cursor_render, argc in conn.pairings:
        assert is_cursor_render == (argc == 6), (
            f"F-RV2-6: the round presented (cursor_render={is_cursor_render}, "
            f"{argc} args) — the statement/argument pairing is the round's own "
            f"invariant (pairings: {conn.pairings})"
        )


# ── F-RV2-7: the claim-health snapshot never races the recorder ─────────


def test_rv2_7_concurrent_record_and_snapshot_never_raise() -> None:
    """The gauges' read path is a SCRAPE-TIME pull (the module's own
    docstring) — the claim path records on the worker's threads while
    the exporter's threads read. Neither may ever see the other: zero
    ``RuntimeError`` (nor any exception) across a sustained concurrent
    hammering. The thread pair is the honest driver (the gauge callback
    runs OFF the event loop); an asyncio-only shape cannot preempt the
    mid-iteration window."""
    import taskq.obs._otel as otel_mod
    from taskq.obs._claim_health import (
        claim_health_snapshot,
        record_claim_latency,
        reset_claim_health_state,
    )

    original_flag = otel_mod._otel_enabled  # pyright: ignore[reportPrivateUsage]  # Why: the pin flips the record gate the guard fixture restores; reading it first keeps the restore exact even on a raised leg.
    otel_mod._otel_enabled = True  # pyright: ignore[reportPrivateUsage]  # Why: record_claim_latency no-ops with the gate off — the pin needs the real writer live.
    reset_claim_health_state()
    errors: list[BaseException] = []
    stop = time.monotonic() + 1.5
    try:

        def writer(i: int) -> None:
            while time.monotonic() < stop:
                record_claim_latency(f"q{i % 4}", 0.001)

        def reader() -> None:
            while time.monotonic() < stop:
                try:
                    claim_health_snapshot()
                except BaseException as exc:  # Why: the pin's subject IS the exception stream.
                    errors.append(exc)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        threads += [threading.Thread(target=reader) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        otel_mod._otel_enabled = original_flag  # pyright: ignore[reportPrivateUsage]
        reset_claim_health_state()
    assert not errors, (
        f"F-RV2-7: concurrent record+snapshot raised {len(errors)} times "
        f"(first: {type(errors[0]).__name__}: {errors[0]}) — the scrape-time "
        "read must never see the recorder's mutation"
    )


# ── F-RV2-8: the nested Promise in the initial carry ────────────────────


async def _src_body(ctx: Any, params: _Ingest) -> _Report:
    return _Report(ref=params.doc_id)


async def _loop_body(ctx: Any, carry: Any) -> Done[Any] | Refine[Any]:
    return Done(carry)


def test_rv2_8_a_nested_promise_in_the_initial_carry_is_a_build_time_refusal() -> None:
    """E11's law is the CARRY'S VALUEHOOD, not the top level's: a
    ``Promise`` anywhere in the ``initial=`` tree is a handle riding a
    jsonb row it can never survive. The refusal is a BUILD-TIME NAMED
    one — the validator's E11 walking the container, or the wiring door
    (the build error), or the encoder's own typed refusal raised at
    DECLARATION time — never the mid-flow ``UnencodableValue`` at the
    first claim. (``sink(parent)`` keeps E2 quiet so the ONLY question
    on the floor is the nested handle; verified at the head: this shape
    validates CLEAN today, and ``dumps_jsonb_str({'carry': {'seed':
    <Promise>}})`` raises ``UnencodableValue`` at the row write.)"""
    from taskq.exceptions import UnencodableValue

    app = WorkflowApp()

    @app.workflow("rv2_loop_nested_promise")
    def _wf() -> object:
        parent = step(_src_body, _Ingest(doc_id="d"), key="parent")
        lp = loop("lp", _loop_body, initial={"seed": parent}, max_iterations=2)
        sink(parent)
        return build(lp)

    with pytest.raises((WorkflowValidationError, WorkflowBuildError, UnencodableValue)):
        app.get("rv2_loop_nested_promise")


# ── F-RV2-9: E10 counts the ACTUAL params ───────────────────────────────


def test_rv2_9_e10_counts_the_actual_params() -> None:
    """Two legs, one law — the arity the rule speaks of is the body's
    REAL signature (``inspect.signature``), the annotation surface is
    the compat rules' subject:

    * the unannotated-but-runnable body wired with its correct count
      validates CLEAN (the zero-false-positive doctrine: a duck-typed
      param is the estate's own tolerated shape);
    * a REAL mismatch refuses AND the message names the REAL arity
      ("takes 2 param(s)"), never the annotated count.

    THE WIRING NOTE (the rv2-cure lane's reconciliation): leg 2's body
    wired ONE arg was a real mismatch when this pin was cut, but the
    consolidated head's E10/E12 partition (5402a4b8) reclassified the
    exactly-one-param-beyond-the-wiring shape as the DEPS class — E12's
    contract, not E10's arity. E10 owns TWO-OR-MORE beyond the wiring,
    so leg 2 wires ZERO args: still a real mismatch, still the
    signature's numbers in the message.

    TODAY both legs convict: the runnable shape is refused, and the
    refusal's message reports 0 params for a 2-param body."""

    # The pin's SUBJECT is the unannotated-but-runnable shape (E5's
    # tolerated duck hole) — annotating it annotates the finding away.
    async def duck_body(ctx, params) -> _Report:  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
        return _Report(ref=params["doc_id"])

    app = WorkflowApp()

    @app.workflow("rv2_e10_duck_ok")
    def _wf() -> object:
        return build(step(duck_body, {"doc_id": "d"}, key="only"))

    app.get("rv2_e10_duck_ok")  # the doctrine: NO refusal

    # The annotation surface is the old bug's counting basis, never the
    # pin's — the message must name the REAL arity of THIS shape.
    async def two_param_body(ctx, first, second) -> _Report:  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
        return _Report(ref="x")

    app2 = WorkflowApp()

    @app2.workflow("rv2_e10_real_mismatch")
    def _wf2() -> object:
        return build(
            step(two_param_body, key="only")
        )  # ZERO wired args: 2 params beyond the wiring — E10's class (the 1-beyond shape is E12's)

    with pytest.raises(WorkflowValidationError) as excinfo:
        app2.get("rv2_e10_real_mismatch")
    message = str(excinfo.value)
    assert "E10" in message, f"the real mismatch must still refuse (E10): {message}"
    assert "only" in message, (
        f"F-RV2-9: the refusal must NAME the offending node (the rule firing "
        f"on the right subject is the contract; the count's sentence is "
        f"wording, never pinned): {message}"
    )


# ── F-RV2-10: the deploy-matrix header walks ────────────────────────────

_REPO = Path(__file__).resolve().parents[1]
_MATRIX = _REPO / "tests" / "system_e2e" / "test_wf_deploy_matrix.py"


def test_rv2_10_every_deploy_matrix_cell_maps_to_a_real_test() -> None:
    """The header's claims are the pin's subjects — walked, not
    restated, so an edited header edits its own pin:

    * every numbered cell names a REAL test function in the file (the
      cell's defining token appears in some ``test_`` name);
    * every file the header cites EXISTS;
    * the header's "operator table" citation names the cells in the
      cited doc (the table the cells claim to be rows of).

    THE FLOOR (the rv2 cure's reconciliation): the pack pinned the
    header's NINE claimed cells; three (OUTAGE, REDISPATCH OWNERSHIP,
    RETENTION MID-FLIGHT) named no test function anywhere — header
    fiction. The cure regenerated the header to the SIX cells this
    file actually tests, so the floor is the tested count at the cure
    (6): a further shrink — a cell dropped or de-tested — re-checks
    this pin by name, never silently.
    """
    import ast

    source = _MATRIX.read_text()
    tree = ast.parse(source)
    header = ast.get_docstring(tree)
    assert header, "the matrix file's header is the pin's subject"
    test_names = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    }
    cells = re.findall(r"^\s*(\d+)\.\s+([^\n—]+?)\s*—", header, flags=re.M)
    assert len(cells) >= 6, f"the header's cell list shrank ({len(cells)}) — recheck the pin"

    failures: list[str] = []
    for number, name in cells:
        tokens = [t for t in re.findall(r"[a-z]+", name.lower()) if len(t) >= 4 and t != "workflow"]
        assert tokens, f"cell {number} ({name.strip()}) yields no defining token"
        defining = tokens[0]
        if not any(defining in test_name for test_name in test_names):
            failures.append(
                f"cell {number} ({name.strip()}): no test function carries "
                f"{defining!r} — the cell is header fiction"
            )

    for cited in sorted(set(re.findall(r"[\w./-]+\.(?:py|md)\b", header))):
        target = (
            _MATRIX.parent / cited
            if "/" not in cited
            else (
                _REPO / cited
                if cited.startswith(("docs/", "tests/", "src/"))
                else _MATRIX.parent / cited
            )
        )
        if not target.is_file():
            failures.append(f"the header cites {cited} — no such file")

    if "operator table" in header:
        doc = (_REPO / "docs" / "guides" / "deployment.md").read_text().lower()
        for number, name in cells:
            tokens = [
                t for t in re.findall(r"[a-z]+", name.lower()) if len(t) >= 4 and t != "workflow"
            ]
            if tokens and tokens[0] not in doc:
                failures.append(
                    f"cell {number} ({name.strip()}): the cited operator table "
                    "(docs/guides/deployment.md) never names it — the 'rows' "
                    "claim is fiction"
                )
    assert not failures, "the deploy-matrix header walks:\n  - " + "\n  - ".join(failures)


# ── F-RV2-11: the battery's deterministic red — the RESOLVED behavior ───
#
# THE VERDICT (root-caused at the head): the test's declaration is the
# lie, the validator is right. ``chain_source(chain, body)`` wires ZERO
# args; the runner invokes a chain-source body with ctx ALONE
# (``_runner.py``: ``if not node.args: return
# tuple(parent_results.values())`` — empty for a source), so a source
# body taking ``(ctx, params)`` would TypeError mid-flow — exactly the
# ladder discovery E10 exists to refuse. Every other chain source in
# the estate (t20's ``t20_source``, the march app's ``triage_source``)
# is ctx-only. The pin asserts the RESOLVED behavior both ways, GREEN:
# the corrected declaration projects both cohorts, and the bad
# declaration keeps refusing LOUDLY (the projection skip names E10; the
# healthy fleets' projection is untouched). The battery's red is cured
# by fixing the DECLARATION in the battery's test — this pin guards the
# resolution, so it cannot rot in either direction.


def test_rv2_11_the_split_placement_declaration_resolved() -> None:
    """The split placement's two cohorts (the source's ``wf``/default
    pair and the chain's gpu-named pair), ONE row each — the projection
    the battery's red test MEANT to pin, over the DECLARED-CORRECTLY
    shape (the ctx-only chain source — the estate's own pattern)."""
    import enum
    from unittest.mock import patch

    from taskq.workflows import DONE, Chain, Route, Step, chain_source
    from taskq.workflows._worker_execution import project_workflow_actor_configs

    class ScreenOutcome(enum.Enum):
        CLEAN = "clean"
        FLAGGED = "flagged"

    async def _screen(ctx: Any, item: dict[str, object]) -> ScreenOutcome:
        return ScreenOutcome.CLEAN  # pragma: no cover - the compile only needs the body

    async def _source_ctx_only(ctx: Any) -> None:
        return None  # the estate's chain-source shape: ctx alone, zero wired args

    app = WorkflowApp()
    chain = Chain(
        name="rv2-split-chain",
        start="screen",
        steps={
            "screen": Step(
                body=_screen,
                outcomes=ScreenOutcome,
                route=Route({ScreenOutcome.CLEAN: DONE, ScreenOutcome.FLAGGED: DONE}),
            )
        },
        actor="wf-exec-gpu",
        queue="gpu",
    )

    @app.workflow("rv2-split")
    def _workflow() -> object:
        src = chain_source(chain, _source_ctx_only, key="doc_source")
        return build(src)

    with patch("taskq.workflows._worker_execution.iter_imported_apps", return_value=[app]):
        configs = project_workflow_actor_configs()
    pairs = {(c.actor, c.queue) for c in configs}
    assert ("wf", "default") in pairs and ("wf-exec-gpu", "gpu") in pairs, pairs


def test_rv2_11_the_bad_source_declaration_still_refuses_loudly() -> None:
    """The other half of the resolution: the battery test's OWN shape
    (a params-taking chain-source body wired with zero args) stays a
    NAMED refusal — the projection skips the workflow LOUDLY with the
    rule id in the record, and no cohort for it projects. A cure that
    instead SILENCED the arity rules would flip this red.

    THE RULE ID (the rv2-cure lane's reconciliation): at the pack's cut
    the record named ``E10-arity``; the consolidated head's E10/E12
    partition (5402a4b8) reclassified the exactly-one-param-beyond-the-
    wiring shape as the DEPS class — the refusal now names
    ``E12-deps-contract`` (the same loudness, the rule that OWNS the
    shape, the message still naming the fix). The guard follows the
    owner."""
    import enum
    from unittest.mock import patch

    import structlog.testing

    from taskq.workflows import DONE, Chain, Route, Step, chain_source
    from taskq.workflows._worker_execution import project_workflow_actor_configs

    class ScreenOutcome(enum.Enum):
        CLEAN = "clean"

    async def _screen(ctx: Any, item: dict[str, object]) -> ScreenOutcome:
        return ScreenOutcome.CLEAN  # pragma: no cover

    async def _source_with_params(ctx: Any, params: _Ingest) -> None:
        return None  # the battery test's shape: a param no wiring feeds

    bad = WorkflowApp()
    chain = Chain(
        name="rv2-split-chain-bad",
        start="screen",
        steps={
            "screen": Step(
                body=_screen,
                outcomes=ScreenOutcome,
                route=Route({ScreenOutcome.CLEAN: DONE}),
            )
        },
        actor="wf-exec-gpu",
        queue="gpu",
    )

    @bad.workflow("rv2-split-bad")
    def _workflow() -> object:
        src = chain_source(chain, _source_with_params, key="doc_source")
        return build(src)

    with (
        structlog.testing.capture_logs() as logs,
        patch("taskq.workflows._worker_execution.iter_imported_apps", return_value=[bad]),
    ):
        configs = project_workflow_actor_configs()
    assert {(c.actor, c.queue) for c in configs} == set(), (
        "the bad declaration's cohorts must NOT project"
    )
    skips = [log for log in logs if log.get("event") == "workflow-projection-skipped"]
    rule_named = str(skips[0].get("error")) if skips else ""
    assert skips and ("E10-arity" in rule_named or "E12-deps-contract" in rule_named), (
        f"the skip must name the arity refusal's rule loudly (the E10/E12 "
        f"partition's owner for this shape) — logs: {logs!r}"
    )


# ── F-RV2-12: no zombie typeprobe corpus ────────────────────────────────


def test_rv2_12_every_typeprobe_file_is_wired_into_the_gate() -> None:
    """The gate's own law ("wire-or-delete, no zombie corpus"): every
    NON-underscore ``*.py`` probe in the directory (the corpus's shape —
    the underscore files are the directory's helpers: ``_gate.py``
    itself and ``_positive_ctx_probe.py``, the green-door probe whose
    own header names its wiring in ``tests/test_wf_ctx_annotation_
    pins.py``) IS in ``_CORPUS``, and every ``_CORPUS`` entry exists on
    disk — a probe the gate never runs is a marker lying fallow (its
    claimed red can rot to a wrong-rule or a green); a corpus entry
    naming an absent file is the mirror rot."""
    import ast

    probe_dir = _REPO / "tests" / "typeprobe"
    gate = ast.parse((probe_dir / "_gate.py").read_text())
    corpus: set[str] | None = None
    for node in gate.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", "") == "_CORPUS":
            corpus = set(ast.literal_eval(node.value))
    assert corpus is not None, "_gate.py's _CORPUS must parse"
    all_files = {p.name for p in probe_dir.glob("*.py")}
    probes_on_disk = {name for name in all_files if not name.startswith("_")}
    zombies = probes_on_disk - corpus
    phantoms = corpus - all_files
    assert not zombies and not phantoms, (
        f"the typeprobe corpus and the directory disagree — zombies on disk "
        f"the gate never runs: {sorted(zombies) or '[]'}; corpus entries "
        f"naming absent files: {sorted(phantoms) or '[]'}"
    )


# ── F-RV2-13: the head-stamp law greens on the committed tree ───────────


def test_rv2_13_the_head_stamp_law_greens_on_the_committed_tree(tmp_path: Path) -> None:
    """The verifier against the tree's COMMITTED state (``git ls-files``
    + ``git show HEAD:`` — the working tree's residue never enters).
    The verdict is the CLAIMS MANIFEST's (``CLAIMS.json`` rides the
    committed tree with the captures it indexes): the registry's append
    order is the run order — no filename parsed, no mtime consulted
    (the pre-manifest pin restored every file's mtime from its own
    name's timestamp: the filename-parsing workaround the manifest
    kills). The verdict is the TREE'S, never the checkout's."""
    import subprocess
    import sys

    runs_rel = ".measurements/runs"
    listed = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the git binary resolves on the dev/CI PATH exactly as the repo's own scripts invoke it.
        ["git", "ls-files", runs_rel],  # noqa: S607  # Why: as above — the repo's own scripts call "git" the same way.
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert listed, "the committed tree carries no .measurements/runs artifacts at all"
    assert f"{runs_rel}/CLAIMS.json" in listed, (
        "the committed tree carries no claims registry — the captures' index "
        "(CLAIMS.json) must be committed with the captures it indexes"
    )
    runs_dir = tmp_path / "runs"
    for rel in listed:
        data = subprocess.run(  # noqa: S603  # Why: fixed argv + a git-ls-files-derived path; no shell.
            ["git", "show", f"HEAD:{rel}"],  # noqa: S607  # Why: as above.
            cwd=_REPO,
            capture_output=True,
            check=True,
        ).stdout
        dest = runs_dir / Path(rel).relative_to(runs_rel)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
    proc = subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier; fixed argv, no shell.
        [
            sys.executable,
            str(_REPO / "scripts" / "verify_evidence_heads.py"),
            "--runs-dir",
            str(runs_dir),
        ],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, (
        "F-RV2-13: the head-stamp verifier reds on the committed estate:\n"
        + proc.stdout
        + proc.stderr
    )


# ── F-RV2-14: the streaming doc's ranged figures cover the captures ─────

#: The ranged-figure claim: "p50 3.2—8.1 ms" — the metric word, the en-dash
#: range, the ms unit. Band contexts (the doc's DECLARED bounds) are not
#: measurements.
_RANGE_RE = re.compile(r"(p50|p95|max)\s+(\d+(?:\.\d+)?)\s*\u2013\s*(\d+(?:\.\d+)?)\s*ms")

#: The doc's measured sections → the capture's metric family (the
#: docs-numbers pin's FAMILY LAYER, extended to ranges: a figure is
#: matched against its OWN artifact family, so an unrelated metric's
#: coincidence can never green a lie).
_STREAMING_SECTION_FAMILIES = {
    "The emit tx per page": "emit_tx_per_page",
    "The dispatch band @ the chain backlog": "dispatch_band_800_mixed",
    "The chain-step fork tx": "chain_fork_tx",
}
_STREAMING_DOC = _REPO / "perf-evidence-workflows-streaming.md"


def test_rv2_14_every_ranged_figure_covers_its_captures() -> None:
    """The doc's own law walked: every ranged ms figure in a measured
    section of ``perf-evidence-workflows-streaming.md`` must COVER the
    corresponding metric across EVERY capture on disk (the run-scoped
    ``t20-streaming-bands-*.json`` plus the rolled artifact), compared
    at the figure's own cited precision. A capture outside the cited
    range is the cherry-pick/dead-figure class this law exists to
    convict."""
    measurements = _REPO / ".measurements"
    captures = sorted(measurements.glob("t20-streaming-bands-*.json"))
    rolled = measurements / "t20-streaming-bands.json"
    if rolled.is_file():
        captures.append(rolled)
    assert captures, "no streaming captures exist at all"
    families: dict[str, dict[str, list[float]]] = {}
    for path in captures:
        data = json.loads(path.read_text())
        for family, metrics in data.items():
            if not isinstance(metrics, dict):
                continue
            for metric, value in metrics.items():
                if metric.endswith("_ms") and isinstance(value, (int, float)):
                    families.setdefault(family, {}).setdefault(metric, []).append(float(value))

    failures: list[str] = []
    section: str | None = None
    for lineno, line in enumerate(_STREAMING_DOC.read_text().splitlines(), 1):
        if line.startswith("## "):
            section = line[3:]
        for match in _RANGE_RE.finditer(line):
            context = line[max(0, match.start() - 40) : match.start()].lower()
            if "band" in context or "≤" in context:
                continue  # the DECLARED bound, not a measurement
            family = next(
                (
                    fam
                    for heading, fam in _STREAMING_SECTION_FAMILIES.items()
                    if (section or "").startswith(heading)
                ),
                None,
            )
            if family is None:
                failures.append(
                    f"line {lineno}: a ranged figure in an unwired section "
                    f"({section!r}) — wire the family map or the section"
                )
                continue
            metric = f"{match.group(1)}_ms"
            low, high = float(match.group(2)), float(match.group(3))
            decimals = max(
                len(match.group(2).partition(".")[2]), len(match.group(3).partition(".")[2])
            )
            tolerance = 0.5 * 10**-decimals
            observed = families.get(family, {}).get(metric, [])
            if not observed:
                failures.append(
                    f"line {lineno}: {metric} {low}\u2013{high} ms — no capture carries "
                    f"{family}.{metric}"
                )
                continue
            outside = [
                round(v, 3) for v in observed if not (low - tolerance <= v <= high + tolerance)
            ]
            if outside:
                failures.append(
                    f"line {lineno}: {family}.{metric} cited {low}\u2013{high} ms but "
                    f"the captures carry {outside} (all: {[round(v, 3) for v in observed]})"
                )
    assert not failures, (
        "the streaming doc's ranged figures do not cover the evidence:\n  - "
        + "\n  - ".join(failures)
    )
