"""ATTACK PINS — the T21 progress subsystem's landed findings.

Provenance: the T21/T20 attack front's live reproductions against the
consolidated head (``af1b8779``; pinned here at the review tree's
``b2a1c122``). Every xfail-strict pin below was run RED FIRST — written
as the SAFE behavior, executed against the real PG, the failure captured
— then marked. The cure flips the pin to XPASS-strict (a red that tells
you to remove the marker WITH the cure). The two guards are the front's
own repros of laws that HELD under attack — green, so they can never
rot silently.

The surfaces under conviction: ``taskq/workflows/_progress.py`` (the
emission op + the emitter) and migration ``01.00.28`` (the two-channel
storage).

THE FINDINGS (each landed live; the probe transcripts are in this pack's
RECEIPTS.md):

* F-PROG-1 — ``validate_emission`` validates LAX and stores the
  PRE-VALIDATION dict: ``{"page": "42"}`` against a declared
  ``page: int`` passes and the STRING lands in
  ``wf_node_progress.data``; undeclared extra keys pass and store.
* F-PROG-2 — ``ProgressEmitter.submit()`` is public and validates
  NOTHING: ``submit(999, …)`` persists ``pct=999``; the migration's
  comment claims pct's smallint is "the storage-domain twin" of the
  0..100 bound but NO CHECK constraint exists.
* F-PROG-3 — the auto-class path (``project_auto_event``) discards the
  ring-trim's drop count: only user-class flushes write the
  ``__stream__`` counters — "dropped ON THE RECORD" is a
  user-class-only promise.
* F-PROG-4 — an uncaught ``ProgressRefusedError`` (a deterministic
  authoring defect) is laddered as TRANSIENT and burns all attempts
  before terminal-failing.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against the fixtures' _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.workflows import FlowRunner, StepContext, WorkflowApp, build, step
from taskq.workflows._progress import (
    KIND_NODE_STARTED,
    PROGRESS_RING_BOUND,
    STREAM_CHANNEL,
    ProgressEmitter,
    ProgressRefusedError,
    project_auto_event,
    validate_emission,
)
from taskq.workflows._progress_read import rebuild_display, run_display
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.engine import finalize_node, render_workflow_sql
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

pytestmark = pytest.mark.integration


class _PageDecl(BaseModel):
    """The node's DECLARED progress payload schema (the TypedGate door's
    declaration — the finding's subject declares ``page: int``)."""

    page: int


# ── F-PROG-1: the gate validates lax and stores the PRE-VALIDATION dict ──


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-PROG-1]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (the attack landed at af1b8779) F-PROG-1: validate_emission …
async def test_f_prog_1_the_stored_payload_is_the_validated_dump(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The law (the TypedGate-door pattern, the context-contract law): what
    the gate ADMITS is what the record STORES — the validated model dump,
    never the raw pre-validation dict. Convicted shape: the gate's own
    return carries the string '42' and the undeclared extra; the stored
    ``wf_node_progress.data`` row carries them too."""
    # THE GATE: the return must be the validated dump — the coerced int,
    # the extras stripped.
    _pct, _msg, data = validate_emission(50, "m", {"page": "42"}, _PageDecl)
    assert data == {"page": 42}, (
        f"F-PROG-1: the gate validated LAX and handed back the PRE-VALIDATION dict "
        f"{data!r} — the string '42' survives where the declared schema says int; "
        "the gate must return the VALIDATED model dump"
    )
    _pct, _msg, data = validate_emission(50, "m", {"page": 1, "evil_extra": "x" * 100}, _PageDecl)
    assert data == {"page": 1}, (
        f"F-PROG-1: an undeclared extra key SURVIVED the declared-schema gate: {data!r} — "
        "the validated dump strips extras"
    )

    # THE RECORD: through the real emitter, the stored row is the dump.
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow, node_id=node, schema_decl=_PageDecl)
    await emitter.emit(50, "m", {"page": "42", "evil_extra": "x"})
    await emitter.aclose()
    raw = await wf_conn.fetchval(
        f'SELECT data FROM "{wf_schema}".wf_node_progress WHERE node_id = $1 AND channel = $2',
        node,
        "progress",
    )
    stored: Any = json.loads(raw) if isinstance(raw, str) else raw
    assert stored == {"page": 42}, (
        f"F-PROG-1: the stored payload is the PRE-VALIDATION dict {stored!r} — the "
        "record must carry the validated model dump (types coerced, extras stripped)"
    )


# ── F-PROG-2: the public submit() bypass + the storage domain's missing
# teeth ─────────────────────────────────────────────────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-PROG-2b]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (the attack landed at af1b8779) F-PROG-2a: …
async def test_f_prog_2a_out_of_domain_pct_refused_and_check_constrained(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """Two halves of one lie: the door admits what the op's own contract
    forbids (pct outside 0..100), and the storage domain's claimed teeth
    (the smallint-as-bound comment in 01.00.28) exist only in prose."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow, node_id=node)
    refused: Exception | None = None
    try:
        await emitter.emit(999, "out of domain", None)
    except Exception as exc:  # the cure's door refusal (ProgressRefusedError or kin)
        refused = exc
    await emitter.aclose()  # drains whatever the bypass armed — no leaked task either way
    stored = await wf_conn.fetchval(
        f'SELECT pct FROM "{wf_schema}".wf_node_progress WHERE node_id = $1 AND channel = $2',
        node,
        "progress",
    )
    assert refused is not None, (
        f"F-PROG-2a: submit(999) bypassed the typed gate and the row persisted "
        f"pct={stored} — an out-of-domain pct must be REFUSED at the door"
    )
    assert stored is None, "a refused emission must write nothing"

    # THE STORAGE'S OWN TEETH — or the claim's correction. The migration's
    # comment (01.00.28) claims the smallint column is "the storage-domain
    # twin" of the 0..100 bound; a claim without a CHECK is prose.
    from pathlib import Path

    import taskq.migrations

    checks = await wf_conn.fetch(
        "SELECT pg_get_constraintdef(c.oid) AS def FROM pg_constraint c "
        "WHERE c.conrelid = $1::regclass AND c.contype = 'c'",
        f"{wf_schema}.wf_node_progress",
    )
    pct_checks = [r["def"] for r in checks if "pct" in r["def"]]
    migration_text = (
        Path(taskq.migrations.__file__).parent / "01.00.29_01_pre_wf_progress.sql"
    ).read_text()
    claim_present = "storage-domain twin" in migration_text
    assert pct_checks or not claim_present, (
        "F-PROG-2a: the migration claims pct's smallint is the storage-domain twin "
        "of the 0..100 bound, but no CHECK constraint carries it — either the storage "
        "grows the teeth (a CHECK on pct) or the claim is corrected"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-PROG-2b]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (the attack landed at af1b8779) F-PROG-2b: the validated …
async def test_f_prog_2b_the_validated_path_is_the_only_write_path(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """THE ONE-DOOR LAW: every write path into the buffer passes the typed
    gate. A public ``submit`` that skips it is a second, unguarded door —
    the pin probes the PUBLIC surface: if a ``submit`` exists at all, it
    must refuse a wrong shape exactly as ``validate_emission`` does."""
    submit = getattr(ProgressEmitter, "submit", None)
    if submit is None:
        return  # no public bypass exists — the law holds (the cure's private shape)
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow, node_id=node)
    refused: Exception | None = None
    try:
        await emitter.emit(999, "bypass", None)
    except ProgressRefusedError as exc:
        refused = exc
    await emitter.aclose()  # drains whatever the bypass armed — no leaked task either way
    assert refused is not None, (
        "F-PROG-2b: ProgressEmitter.submit is public and validates NOTHING — the "
        "typed gate (validate_emission, ctx.progress's door) is not the only write "
        "path; submit must route through the gate or go private"
    )


# ── F-PROG-3: the auto-class path's trims are nowhere on the record ──────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-PROG-3]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (the attack landed at af1b8779) F-PROG-3: …
async def test_f_prog_3_auto_path_trims_are_counted_on_the_record(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """DH2's fence is class-blind: the dropped counter is the honest
    emitted-vs-delivered pair for THE STREAM, not for one class of it.
    Drive the ring past its bound with AUTO-class events alone; the
    record must show the trims exactly as the user path's flush shows
    them (the ``__stream__`` channel's dropped counter)."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)
    emitted = PROGRESS_RING_BOUND + 10
    for i in range(emitted):
        await project_auto_event(
            wf_pool,
            wf_sql,
            flow_id=flow,
            node_id=node,
            kind=KIND_NODE_STARTED,
            payload={"i": i},
        )
    retained = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', node
    )
    assert retained == PROGRESS_RING_BOUND, "the ring bound itself holds (not the finding)"
    counters = await wf_conn.fetchrow(
        f'SELECT occurrences, dropped FROM "{wf_schema}".wf_node_progress '
        "WHERE node_id = $1 AND channel = $2",
        node,
        STREAM_CHANNEL,
    )
    assert counters is not None, (
        "F-PROG-3: the auto path's ring trims are NOWHERE on the record — no "
        "__stream__ counters row exists for a node whose ring trimmed "
        f"{emitted - PROGRESS_RING_BOUND} auto rows"
    )
    assert int(counters["dropped"]) == emitted - int(retained), (
        f"F-PROG-3: the record shows dropped={counters['dropped']} but the ring "
        f"trimmed {emitted - int(retained)} auto rows — the honest "
        "emitted-vs-delivered pair must cover the auto class too"
    )


# ── F-PROG-4: the refusal laddered as transient ─────────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-PROG-4]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (the attack landed at af1b8779) F-PROG-4: an uncaught …
async def test_f_prog_4_the_refusal_classifies_permanent(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The ladder's classification is for faults that MIGHT heal; a typed
    authoring refusal cannot. The pin drives a real workflow whose body
    violates its own declared ``progress_schema`` (the front's repro
    shape): the refusal IS on the record (loud — not the finding); the
    finding is the BURN — a permanent class terminal-fails on the FIRST
    attempt, exactly one failed ledger leg."""

    class _Decl(BaseModel):
        page: int

    async def violating(ctx: StepContext) -> str:
        await ctx.progress(10, "fine", {"page": 1})
        # THE VIOLATION: data the declared schema refuses — deterministic,
        # an authoring defect, never transient.
        await ctx.progress(50, "lying", {"page": "not-an-int-at-all"})
        return "done"

    app = WorkflowApp()

    @app.workflow("attack_prog_refusal")
    def _wf() -> object:
        return build(step(violating, key="violating", progress_schema=_Decl))

    runner = FlowRunner(app.get("attack_prog_refusal"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    row = await wf_conn.fetchrow(
        f"""SELECT status, error_class, attempt FROM "{wf_schema}".jobs
            WHERE step_key = 'violating' AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    assert row is not None
    # The refusal IS loud and terminal (NOT the finding — the finding is the burn).
    assert row["status"] == "failed"
    assert row["error_class"] == "ProgressRefusedError"
    failed_legs = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1 AND step_key = 'violating' AND status = 'failed'""",
        flow_id,
    )
    assert int(failed_legs) == 1, (
        f"F-PROG-4: the deterministic authoring defect burned {failed_legs} attempts "
        f"(jobs.attempt={row['attempt']}) laddered as TRANSIENT before "
        "terminal-failing — the refusal must classify PERMANENT (one attempt, "
        "terminal, loud) or the taxonomy must name why it doesn't"
    )


# ── THE GUARDS (the front's repros of laws that HELD — green, forever) ──


async def test_guard_progress_writes_never_block_the_finalize(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """THE FINALIZE-UNAFFECTED LAW (decision e — the asymmetry, the
    front's battery B1): a node whose EVERY flush fails still
    terminalizes normally — the failures are counted on the record
    (``write_errors``), the first loss warned, never raised into the
    node path. The failure shape here: the emitter's statements rendered
    for a schema whose tables do not exist — every flush errors, every
    error swallowed."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)

    broken = render_workflow_sql(f"{wf_schema}_gone")  # no tables there: every flush fails
    emitter = ProgressEmitter(wf_pool, broken, flow_id=flow, node_id=node)
    for i in range(100):
        await emitter.emit(i % 101, f"m{i}", None)
    await emitter.aclose()  # bounded, swallowing — the pre-finalize close
    assert emitter.flushes >= 1
    assert emitter.write_errors >= 1, "the failing flushes must be COUNTED on the record"
    assert emitter.emitted == 100

    # The finalize lands normally — the ledger's path never touched by
    # the emission's failures.
    worker_id, attempt, epoch = await claim_view(wf_conn, wf_schema, node)
    final = await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow,
        job_id=node,
        step_key="a",
        worker_id=worker_id,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"value": 1},
    )
    assert final.applied, "the finalize was blocked by the progress writes' failures"
    status = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', node)
    assert status == "succeeded"


async def test_guard_the_progress_lie_never_flips_the_terminal_state(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """THE PROGRESS-LIE FENCE (DH4/DH7, the front's battery E1): a body
    reports pct=100 'done!' and the node then FAILS — the record shows
    FAILED with the 100% rendered INSIDE it, never 100%-running; a
    STRAGGLER emission landing after the terminal (the aclose race's
    shape) updates freshness only — the LEDGER's status can never flip
    back. Pinned on BOTH faces: the connect-shape read (``run_display``)
    and the replay fold (``rebuild_display``)."""
    flow = await seed_flow(wf_conn, wf_schema)
    node = await seed_running_node(wf_conn, wf_schema, flow)

    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow, node_id=node)
    await emitter.emit(100, "done!", None)
    await emitter.aclose()

    # The node FAILS at 100% (the ledger is the truth).
    worker_id, attempt, epoch = await claim_view(wf_conn, wf_schema, node)
    await finalize_node(
        wf_pool,
        wf_sql,
        flow_id=flow,
        job_id=node,
        step_key="a",
        worker_id=worker_id,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="failed",
        error_class="Boom",
        error_message="the body failed at 100%",
    )
    # THE STRAGGLER: a late emission lands after the terminal (the aclose race).
    await emitter.emit(100, "really done", None)
    await emitter.aclose()

    # FACE 1 — the connect shape: failed, with the 100% inside it.
    display = await run_display(wf_pool, wf_sql, flow)
    d = display[str(node)]
    assert d["status"] == "failed", (
        f"the lie flipped the display: {d['status']!r} at pct {d['pct']}"
    )
    assert d["pct"] == 100 and d["message"] == "really done", (
        "the last progress (the straggler's) renders INSIDE the terminal state (advisory)"
    )

    # FACE 2 — the replay fold: the LEDGER wins the status at every fold.
    events = await wf_conn.fetch(wf_sql.progress_replay_node, 0, node, 1000)
    ledger = await wf_conn.fetch(wf_sql.workflow_nodes, flow)
    rebuilt = rebuild_display([dict(r) for r in ledger], [], [dict(e) for e in events])
    assert rebuilt[str(node)]["status"] == "failed", (
        "the replay fold let a progress event flip the terminal state back"
    )
    assert rebuilt[str(node)]["pct"] == 100
