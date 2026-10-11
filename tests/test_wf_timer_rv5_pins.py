# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""THE DURABLE TIMER's rv5 HOSTILE-REVIEW pins (feat/taskqflow-durable-timers
@ 08dccb92d) — the convicted defects of the T28 lane (+ the T29 redeploy
face's operator surfaces), pinned asserting the SAFE behavior.

Provenance: the rv5 internals review's live convictions, each reproduced
against this head BEFORE pinning (the pre-pin repro shapes + outputs are in
the pin pack's RECEIPTS.md; the scripts in the pack's ``repro/``). Per the
doctrine, a LIVE finding is pinned under ``pytest.mark.xfail(strict=True,
reason="LIVE FINDING rv5-t<id>: …")`` asserting the SAFE behavior — the cure
flips the pin XPASS-strict (a red that says: remove the marker WITH the
cure).

The findings (each convicted live; the receipts carry the verbatim reds):

* F-T1 (SEVERE) — THE TROJAN PREFIX: a payload model named
  ``__timer__Approval`` mints a "human hold" that is INVISIBLE to
  ``HitlClient.list()`` (the _LIST_SQL's prefix exclusion), UNANSWERABLE
  (the boundary refuses the hand-delivered answer as a forgery of the
  clock), NEVER fail-closes (the expiry arm excludes the prefix), and
  SELF-FIRES at the deadline — the timer arm resolves it with the
  ``TimerWake`` receipt, the wait site's revalidation against the
  trojan model fails, and the run dies ``error_class='SignalPayloadError'``
  (every leg observed live). THE PIN: ``wait_signal`` refuses a model
  whose name carries the reserved prefix — the mint-time refusal NAMING
  the rule (build door or wait site), never the self-fire death.
* F-T2 — THE BROADCAST LEAK: durable timers surface as pending HUMAN
  holds on the T26 broadcast + backfill: the hold-create notify is
  unconditional (``register_hold``'s create leg), and the listener's
  backfill (``_hitl_listen.py`` — ``WHERE status = 'held'``) carries no
  prefix filter (observed: ``HoldCreated(signal='__timer__leak')`` on
  BOTH the notify and the backfill legs; ``Backfilled.open_hold_ids``
  carries the timer id). THE PIN: the broadcast + the backfill never
  surface a timer hold as a HUMAN hold (the filter cure; a typed
  TimerScheduled event distinct from HoldCreated also satisfies —
  flagged in PINMAP.md).
* F-T3 — THE 10K-TIMER TAIL: ``sweep_due_timers`` is one bounded batch
  (``batch_size=100``, no drain) at the 30s default cadence → 10k due
  timers drain in ~50 minutes (observed: 1500 seeded due, ONE pass fired
  exactly 100, 1400 left held; no ``drain=`` knob exists); and NO index
  leads the due-scan (the wf_signals indexes are ``(id, status)`` and
  ``(workflow_id, status, id)`` — the ``status='held' AND
  signal_name LIKE '__timer__%' AND expires_at <= now ORDER BY
  expires_at`` scan is a seq scan + sort per tick). THE PIN: (a) the
  arm DRAINS (``drain=True`` semantics — one call cascade loops until
  the due-set empties): 1500 due timers all resolve within ONE cascade
  in a handful of seconds; (b) a static pin: a migration carries an
  index covering the due-scan (``expires_at`` on ``wf_signals``, the
  held-partial or equivalent).
* F-T4 — THE EXPLORER'S FALSE AFFORDANCE: the admin run page renders the
  human RESOLVE form on a TIMER hold row (``workflow_detail.html``'s
  ``{% if h.status == 'held' and actions_enabled %}`` — any held row;
  observed at render level: a ``__timer__`` hold row renders
  ``data-wf-form="resolve"`` exactly like a human hold's). THE PIN:
  timer rows never render the human-resolve form (the template branch;
  the render-level pin keeps the HUMAN affordance intact).
* F-T5 — THE DRIFT-INVISIBLE OPERATOR: zero ``code_version`` references
  in the CLI/admin surfaces — a run redeployed mid-flight (two distinct
  §22.1 hashes on one run's rows — the stamps EXIST,
  ``jobs.code_version``) is flagged nowhere (observed: the mixed-version
  run's ``taskq flows status <run>`` output carries no version line at
  all). THE PIN: ``flows status <run>`` surfaces the version drift when
  a run's node rows carry >1 distinct ``code_version``.
* F-T6 — THE STRUCTURAL-REDEPLOY WEDGE: rename a node in a redeploy →
  the in-flight run's pending row at that key is unresolvable in the new
  process (observed: the new-process drive RAISES
  ``WorkflowRunError: step 'second' is not in workflow ... compiled
  graph nor in its registered definition`` raw out of ``drive()``; the
  row stays ``pending``, the root stays ``running`` — NEVER terminal;
  the fleet's face parks it budget-free at the snooze cadence with only
  the stranded-detector's log — nothing surfaces ON THE RUN). THE PIN
  (the house's loud law — direction flagged in PINMAP.md): the run whose
  pending node's key vanished from the registry TERMINAL-FAILS LOUDLY
  with a NAMED error class, never an eternal zombie.
"""

from __future__ import annotations

import asyncio
import re
from asyncio import to_thread
from contextlib import suppress
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel, create_model

from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    step,
)
from taskq.workflows.api._hitl import sweep_due_timers
from taskq.workflows.api._hitl_listen import HitlListener
from taskq.workflows.api._validate import WorkflowValidationError


class Nudge(BaseModel):
    note: str = "x"


class TDoc(BaseModel):
    doc_id: str


# ── the pinned graph's bodies (module-level: the annotations must resolve —
#    the compile's hint resolution + the §22.1 stamper read the body's own
#    SOURCE, which a function-scope body does not reproducibly carry). ──


async def _t1_trojan_body(ctx: StepContext, params: Nudge) -> str:
    trojan = create_model("__timer__Approval", verdict=(str, ...))
    outcome = await ctx.wait_signal(trojan, timeout_s=0.1)
    return f"got:{type(outcome).__name__}"


async def _t2_timer_body(ctx: StepContext, params: Nudge) -> str:
    await ctx.wait_until(120.0, key="leak")
    return "woken"


async def _t5_first_v1(ctx: StepContext, params: TDoc) -> str:
    return "first:v1"


async def _t5_second_v1(ctx: StepContext, params: str) -> str:
    return f"second:v1:{params}"


async def _t5_second_v2(ctx: StepContext, params: str) -> str:
    # THE REDEPLOY's new body (different source → a different §22.1 stamp).
    return f"second:v2:{params}!"


async def _t6_first(ctx: StepContext, params: TDoc) -> str:
    return "first-done"


async def _t6_second(ctx: StepContext, params: str) -> str:
    return f"second:{params}"


async def _t6_renamed(ctx: StepContext, params: str) -> str:
    return f"renamed:{params}"


def _install_registry(name: str, bodies: dict[str, Any]) -> None:
    """THE REDEPLOY SEAM (the versioning pins' honest simulation): in
    production a redeploy is a NEW PROCESS whose registry carries the new
    code — the module-level map simply holds the new definition. In-test
    the same map is replaced under the SAME workflow name (the registry IS
    the module-level map; the replacement is the deploy, and nothing else
    in the engine is touched)."""
    from taskq.workflows.definitions import WorkflowDef, get_registry

    definition = WorkflowDef(name=name, bodies=bodies)
    registry = get_registry()
    registry._workflows[name] = definition  # pyright: ignore[reportPrivateUsage]  # Why: the redeploy simulation IS the registry map's replacement — the versioning pins' own seam.


@pytest.fixture
def cli_settings(module_pg_schema: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """The CLI's settings read THIS module's migrated schema (the tests'
    own env — the cli pins' idiom): the env is the settings model's own
    front door."""
    monkeypatch.setenv("TASKQ_PG_DSN", module_pg_schema.pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)
    return module_pg_schema


async def _collect(listener: HitlListener, seconds: float) -> list[Any]:
    """The bounded event read (condition-not-clock: the window is the
    SAFETY NET — the notifies land in ms; the cured tree simply has
    nothing to deliver)."""
    events: list[Any] = []

    async def go() -> None:
        async for event in listener.events():
            events.append(event)

    with suppress(TimeoutError):
        await asyncio.wait_for(go(), timeout=seconds)
    return events


# ── F-T1: THE TROJAN PREFIX (the mint-time refusal) ─────────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-t1: wait_signal mints a hold under the RESERVED "
        "__timer__ prefix from a trojan model name (observed: the "
        "__timer__Approval hold invisible to HitlClient.list(), the hand answer "
        "refused as a forgery, the expiry arm's fail-close never fires on it, "
        "and the TIMER arm self-fires it at the deadline into "
        "error_class='SignalPayloadError' killing the run) — no mint-time "
        "refusal exists"
    ),
)
async def test_t1_wait_signal_refuses_a_timer_prefixed_model(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE RESERVED PREFIX IS THE MINT'S DOOR: a ``wait_signal`` payload
    model whose name carries ``__timer__`` is refused NAMING the rule —
    at the build/validate door, or at the wait site as a LOUD named
    terminal (never the trojan lifecycle: invisible, unanswerable,
    fail-close-proof, self-fired into the ``SignalPayloadError`` death
    of an innocent-looking run)."""
    app = WorkflowApp()

    @app.workflow("rv5t1_trojan")
    def t1_flow() -> Promise[object]:
        return build(step(_t1_trojan_body, Nudge(), key="gate", max_attempts=1))

    try:
        compiled = app.get("rv5t1_trojan")
    except (WorkflowBuildError, WorkflowValidationError) as exc:
        # THE BUILD DOOR's refusal — the rule must be named.
        message = str(exc)
        assert "__timer__" in message and "reserved" in message.lower(), (
            f"the build refusal does not name the reserved-prefix rule: {message!r}"
        )
        return

    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id, tick=0.02, max_ticks=120)
    assert outcome == "terminal", outcome
    # THE WAIT-SITE REFUSAL FACE: the node terminal-fails LOUDLY with the
    # reserved-prefix rule NAMED — and NEVER with the self-fire's
    # SignalPayloadError (the convicted shape: the trojan hold minted,
    # invisible + unanswerable, then fired by the clock into the payload
    # mismatch).
    node = await wf_conn.fetchrow(
        f'SELECT status::text AS status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gate'",
        flow_id,
    )
    assert node is not None
    assert node["status"] == "failed", (
        f"the trojan mint must die LOUDLY at the wait site — the node ended {node['status']!r}"
    )
    assert node["error_class"] != "SignalPayloadError", (
        "F-T1: the run died the TROJAN death — the minted '__timer__Approval' hold "
        "was invisible to the client and unanswerable by hand, then the TIMER ARM "
        "fired it at the deadline and the wait site's revalidation killed the run "
        "(SignalPayloadError). The mint itself must be refused"
    )
    message = node["error_message"] or ""
    assert "__timer__" in message and "reserved" in message.lower(), (
        f"F-T1: the wait-site death does not name the reserved-prefix rule: "
        f"{node['error_class']}: {message!r}"
    )


# ── F-T2: THE BROADCAST LEAK (the listener never sees a timer as human) ──


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-t2: durable timers surface as pending HUMAN holds on "
        "the T26 faces — the hold-create notify is unconditional "
        "(register_hold's create leg) and the listener's backfill reads WHERE "
        "status='held' with no prefix filter (_hitl_listen.py) — observed: "
        "HoldCreated(signal='__timer__leak') on the notify leg AND the backfill "
        "leg, and Backfilled.open_hold_ids carrying the timer id"
    ),
)
async def test_t2_the_broadcast_and_backfill_never_surface_timer_holds(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A timer hold is the CLOCK's, never a human's pending card: across
    the timer's whole lifecycle the listener emits NO ``HoldCreated``
    for a ``__timer__`` signal (neither the live notify NOR the
    backfill), and no ``Backfilled`` reconcile carries the timer's id in
    ``open_hold_ids``. (A TYPED ``TimerScheduled``-style event distinct
    from ``HoldCreated`` satisfies the law — the pin convicts only the
    human-hold shape; PINMAP.md flags the alternative.)"""
    app = WorkflowApp()

    @app.workflow("rv5t2_leak")
    def t2_flow() -> Promise[object]:
        return build(step(_t2_timer_body, Nudge(), key="timer_node"))

    runner = FlowRunner(app.get("rv5t2_leak"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # THE NOTIFY LEG: the listener is LIVE when the timer mints.
    try:
        listener = HitlListener(wf_pool, wf_schema)
        async with listener:
            outcome = await runner.drive(flow_id, until="held")
            assert outcome == "held"
            events = await _collect(listener, 1.5)
        timer_creations = [
            e
            for e in events
            if getattr(e, "event", "") == "hold_created"
            and str(getattr(e, "signal", "")).startswith("__timer__")
        ]
        assert timer_creations == [], (
            f"F-T2 (notify leg): the listener announced a TIMER hold as a HUMAN "
            f"hold — HoldCreated events for the clock's own rows: "
            f"{[(getattr(e, 'signal', None), getattr(e, 'source', None)) for e in timer_creations]}"
        )

        # THE BACKFILL LEG: a listener started AFTER the mint snapshots the
        # open rows — the timer id must not arrive as a pending human hold.
        hold_id = await wf_conn.fetchval(
            f'SELECT id::text FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',
            flow_id,
        )
        assert hold_id is not None
        listener2 = HitlListener(wf_pool, wf_schema)
        async with listener2:
            events2 = await _collect(listener2, 1.5)
        timer_creations2 = [
            e
            for e in events2
            if getattr(e, "event", "") == "hold_created"
            and str(getattr(e, "signal", "")).startswith("__timer__")
        ]
        assert timer_creations2 == [], (
            f"F-T2 (backfill leg): the snapshot announced a TIMER hold as a HUMAN "
            f"hold: {[(getattr(e, 'signal', None), getattr(e, 'source', None)) for e in timer_creations2]}"
        )
        backfilled = [e for e in events2 if getattr(e, "event", "") == "backfilled"]
        assert backfilled, (
            "the listener's reconcile event never arrived (the harness's own witness)"
        )
        assert all(hold_id not in e.open_hold_ids for e in backfilled), (
            "F-T2 (the reconcile): Backfilled.open_hold_ids carries the TIMER id — "
            "the consumer's pending-human cards include the clock's hold"
        )
    finally:
        # the pin's own hygiene: the held timer dies with the flow even on
        # the red path (the module's schema is shared across the file).
        await runner.cancel_workflow(flow_id, reason="the pin's cleanup")


# ── F-T3: THE 10K-TIMER TAIL (the drain + the index) ────────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-t3a: sweep_due_timers fires ONE bounded batch "
        "(batch_size=100, no drain) per pass — observed: 1500 seeded due timers, "
        "one pass fired exactly 100 and left 1400 held; at the 30s default "
        "cadence a 10k backlog drains in ~50 minutes; no drain= knob exists "
        "(TypeError)"
    ),
)
async def test_t3a_the_timer_arm_drains_the_due_set_in_one_cascade(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE DRAIN LAW: one sweep call cascade loops (bounded per batch)
    until the due-set EMPTIES — 1500 due timers all resolve 'delivered'
    within the cascade, in a handful of seconds (never
    backlog/batch-per-30s-tick arithmetic). The pin calls the natural
    face (``drain=True``); a head without the knob falls back to the
    plain call and reds on the remaining-held assertion (the knob's
    absence IS the finding)."""
    import time

    held0 = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_signals '
        "WHERE status = 'held' AND starts_with(signal_name, '__timer__')"
    )
    delivered0 = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_signals '
        "WHERE status = 'delivered' AND starts_with(signal_name, '__timer__')"
    )
    seeded = 1500
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, payload, "
        " payload_schema, status, created_at, expires_at) "
        "SELECT gen_random_uuid(), gen_random_uuid(), 'n' || g, '__timer__k' || g, 1, "
        "       'c', '{}'::jsonb, '{}'::jsonb, 'held', clock_timestamp(), "
        "       clock_timestamp() - interval '1 second' "
        "FROM generate_series(1, $1) g",
        seeded,
    )
    start = time.monotonic()
    try:
        await sweep_due_timers(wf_pool, schema=wf_schema, drain=True)  # type: ignore[call-arg]  # Why: the pin is written against the cure's natural face; the head's refusal (TypeError) IS the red.
    except TypeError:
        await sweep_due_timers(wf_pool, schema=wf_schema)
    elapsed = time.monotonic() - start
    remaining = (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_signals '
            "WHERE status = 'held' AND starts_with(signal_name, '__timer__')"
        )
        - held0
    )
    assert remaining == 0, (
        f"F-T3a: one sweep cascade left {remaining} of {seeded} due timers HELD "
        f"(the arm fires one bounded batch per pass — at the 30s default cadence "
        "a 10k backlog takes ~50 minutes to drain)"
    )
    delivered = (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_signals '
            "WHERE status = 'delivered' AND starts_with(signal_name, '__timer__')"
        )
        - delivered0
    )
    assert delivered == seeded, f"only {delivered} of {seeded} due timers resolved"
    assert elapsed < 15.0, (
        f"the drain took {elapsed:.1f}s — the cascade must be a handful of seconds, "
        "never the per-tick march"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-t3b: no index leads the timer due-scan — the "
        "wf_signals indexes are (id, status) and (workflow_id, status, id) plus "
        "the hold-identity unique partial; the sweep's WHERE status='held' AND "
        "starts_with(signal_name,'__timer__') AND expires_at <= now ORDER BY "
        "expires_at is a seq scan + sort per tick"
    ),
)
def test_t3b_a_due_scan_index_exists_in_the_migrations() -> None:
    """THE DUE-SCAN'S INDEX (the static leg): the migrations carry an
    index covering the timer due-scan — ``expires_at`` on ``wf_signals``
    (the ``(status, expires_at)`` partial, the plain ``expires_at``, or
    the equivalent whose column list LEADS the scan's filter+sort)."""
    migrations = Path(__file__).resolve().parent.parent / "src" / "taskq" / "migrations"
    statements: list[str] = []
    for path in sorted(migrations.glob("*.sql")):
        statements += re.findall(
            r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+\w+\s+ON\s+[^;]+", path.read_text(), re.IGNORECASE
        )
    signals_indexes = [s for s in statements if "wf_signals" in s]
    assert signals_indexes, "no wf_signals index exists at all (the harness's own witness)"
    due_scan = [s for s in signals_indexes if "expires_at" in s.lower()]
    assert due_scan, (
        f"F-T3b: no wf_signals index covers expires_at — the timer due-scan "
        f"(status='held' + the __timer__ prefix + expires_at <= now ORDER BY "
        f"expires_at) seq-scans + sorts per tick. The shipped indexes: "
        f"{[s.splitlines()[0] for s in signals_indexes]}"
    )


# ── F-T4: THE EXPLORER'S FALSE AFFORDANCE (the render-level pin) ─────────


@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-t4: the admin run page renders the human RESOLVE form "
        "on a TIMER hold row — workflow_detail.html's guard is `h.status == "
        "'held' and actions_enabled` (any held row); observed at render level: "
        'the __timer__ hold row renders data-wf-form="resolve" exactly like '
        "the human hold's"
    ),
)
def test_t4_timer_rows_never_render_the_human_resolve_form() -> None:
    """THE FALSE AFFORDANCE DIES AT THE TEMPLATE: render the run page
    with ONE timer hold + ONE human hold (both 'held', actions on) — the
    HUMAN row keeps its resolve form (the affordance's guard), the TIMER
    row renders NONE (the clock is the only resolver; a form that can
    only be refused-as-forgery is a lie on the page). The view-level
    alternative (timer rows never reach the holds projection) is flagged
    in PINMAP.md."""
    import jinja2

    templates = Path(__file__).resolve().parent.parent / "src" / "taskq" / "web" / "templates"
    env = jinja2.Environment(  # the admin factory's own construction (autoescape on)
        autoescape=True,
        loader=jinja2.FileSystemLoader(str(templates)),
    )
    from taskq.web.admin._constants import (
        BADGE_CHIP_BASE,
        STATUS_CHIP_CLASSES,
        STATUS_TEXT_CLASSES,
    )

    env.globals["status_chip_classes"] = STATUS_CHIP_CLASSES
    env.globals["status_text_classes"] = STATUS_TEXT_CLASSES
    env.globals["badge_chip_base"] = BADGE_CHIP_BASE
    env.globals["poll_interval_ms"] = 5000
    env.globals["base_path"] = ""
    env.globals["sso_logout_token"] = lambda: ""
    env.globals["admin_actions_banner"] = ""
    from types import SimpleNamespace

    run = SimpleNamespace(
        run_id="12345678-abcd",
        workflow="rv5t4",
        root_status="running",
        root_map_children=0,
        cancel_requested_at=None,
    )

    def hold(hold_id: str, signal: str) -> dict[str, Any]:
        return {
            "hold_id": hold_id,
            "signal": signal,
            "node": "n",
            "epoch": 1,
            "status": "held",
            "payload": {},
            "payload_schema": {},
            "reason": None,
            "tool": None,
            "args": None,
            "created_at": "",
            "expires_at": "",
        }

    html = env.get_template("workflow_detail.html").render(
        run=run,
        installed=True,
        derived="running",
        nodes=[],
        holds=[hold("rv5t4timer", "__timer__leak"), hold("rv5t4human", "Approval")],
        events=[],
        boot=None,
        csrf_token="t",
        actions_enabled=True,
        realtime_mode="off",
        mode_label="off",
        refusal_banner=None,
    )
    forms = re.findall(r'data-wf-form="resolve" data-hold-id="([^"]+)"', html)
    assert "rv5t4human" in forms, (
        "the HUMAN hold lost its resolve form — the cure must not amputate the real affordance"
    )
    assert "rv5t4timer" not in forms, (
        "F-T4: the TIMER hold row renders the human resolve form — a control "
        "whose every submission is refused-as-forgery is a false affordance on "
        "the operator's page"
    )


# ── F-T5: THE DRIFT-INVISIBLE OPERATOR (the version-drift surface) ───────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-t5: zero code_version references in the CLI/admin — "
        "the stamps exist (jobs.code_version per claim) but no surface reads "
        "them: the mixed-version run (observed: 2 distinct stamps on one run's "
        "rows) gets NO drift line in `taskq flows status`"
    ),
)
async def test_t5_flows_status_surfaces_the_version_drift(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    cli_settings: Any,
) -> None:
    """THE STAMPS MUST BE READ: a run whose node rows carry >1 distinct
    ``code_version`` (the redeployed-mid-run shape — the T29 contract's
    own adoption) is FLAGGED on the operator's run view: ``taskq flows
    status <run>`` carries a drift line naming the versions' plurality.
    The staging is the versioning pins' own redeploy seam: 'first' runs
    v1, the registry swaps, 'second' runs v2 — two stamps on one run."""
    from typer.testing import CliRunner

    from taskq.cli import app as cli_app

    app = WorkflowApp()
    name = "rv5t5_drift_flow"

    @app.workflow(name)
    def t5_flow() -> Promise[object]:
        p = step(_t5_first_v1, TDoc(doc_id="d1"), key="first")
        return build(step(_t5_second_v1, p, key="second"))

    compiled = app.get(name)
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    rows = await runner._claimable(flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the versioning pins' mid-flow stop seam — the claim loop's seam is the behavior under test.
    assert [r["step_key"] for r in rows] == ["first"], rows
    await runner._run_node(flow_id, rows[0])  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    # THE REDEPLOY: 'second' resolves to v2 from here on.
    _install_registry(name, {"first": _t5_first_v1, "second": _t5_second_v2})
    outcome = await runner.drive(flow_id)
    assert outcome == "terminal"
    assert await runner.result(flow_id) == "second:v2:first:v1!"

    # THE STAGING's OWN PROOF: the run's rows carry TWO distinct stamps.
    stamps = await wf_conn.fetch(
        f'SELECT step_key, code_version FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        flow_id,
    )
    distinct = {r["code_version"] for r in stamps if r["code_version"] is not None}
    assert len(distinct) == 2, f"the drift staging failed (stamps: {distinct})"

    result = await to_thread(CliRunner().invoke, cli_app, ["flows", "status", str(flow_id)])
    assert result.exit_code == 0, f"flows status failed: {result.output[:200]!r}"
    lines = result.output.splitlines()
    drift_lines = [
        ln
        for ln in lines
        if "version" in ln.lower()
        and ("drift" in ln.lower() or "differ" in ln.lower() or "mixed" in ln.lower())
    ]
    assert drift_lines, (
        f"F-T5: `taskq flows status` never mentions the version drift — the run "
        f"carries 2 distinct code_versions and the operator sees nothing. "
        f"The output: {lines!r}"
    )


# ── F-T6: THE STRUCTURAL-REDEPLOY WEDGE (the loud terminal) ──────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-t6: rename a node in a redeploy and the in-flight "
        "run's pending row zombies FOREVER — observed: the new-process drive "
        "RAISES WorkflowRunError raw ('step \\'second\\' is not in workflow ... "
        "compiled graph nor in its registered definition'), the node stays "
        "'pending', the root stays 'running', NEVER terminal (the fleet's face "
        "parks it budget-free at the snooze cadence; nothing surfaces ON THE "
        "RUN)"
    ),
)
async def test_t6_a_vanished_node_key_terminal_fails_loudly(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE LOUD LAW (the house's failure doctrine): a run whose PENDING
    node's key vanished from the registry (the structural redeploy — the
    rename) terminal-FAILS with a NAMED error class — the run's own rows
    tell the operator the truth — NEVER an eternal zombie (a 'running'
    root over a forever-pending node, surfaced only in a detector's
    log). The cancel-then-restart remediation alternative is flagged in
    PINMAP.md."""
    app = WorkflowApp()
    name = "rv5t6_wedge_flow"

    @app.workflow(name)
    def t6_flow() -> Promise[object]:
        p = step(_t6_first, TDoc(doc_id="d1"), key="first")
        return build(step(_t6_second, p, key="second", max_attempts=1))

    compiled = app.get(name)
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    rows = await runner._claimable(flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the versioning pins' mid-flow stop seam.
    assert [r["step_key"] for r in rows] == ["first"], rows
    await runner._run_node(flow_id, rows[0])  # pyright: ignore[reportPrivateUsage]  # Why: as above — 'first' terminal-succeeds, 'second' pends.

    # THE STRUCTURAL REDEPLOY (the new process's face): the registry AND
    # the compiled graph carry 'renamed', never 'second'.
    _install_registry(name, {"first": _t6_first, "renamed": _t6_renamed})
    app2 = WorkflowApp()

    @app2.workflow(name)
    def t6_flow_v2() -> Promise[object]:
        p = step(_t6_first, TDoc(doc_id="d1"), key="first")
        return build(step(_t6_renamed, p, key="renamed", max_attempts=1))

    runner2 = FlowRunner(app2.get(name), wf_pool, wf_schema)
    with suppress(Exception):
        # the raw escape is the CONVICTED shape — the pin reads the ROWS.
        await runner2.drive(flow_id, tick=0.02, max_ticks=60)

    node = await wf_conn.fetchrow(
        f'SELECT status::text AS status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'second'",
        flow_id,
    )
    assert node is not None
    assert node["status"] == "failed", (
        f"F-T6: the vanished node's run NEVER terminalizes — 'second' is "
        f"{node['status']!r} and the root follows it (the eternal zombie: "
        "budget-free parked, detector-logged at best, surfaced nowhere ON THE "
        "RUN). The loud law: terminal-fail with a NAMED error class"
    )
    assert node["error_class"], "the loud terminal must NAME the error class"
    message = (node["error_message"] or "").lower()
    assert "second" in message or "registry" in message or "redeploy" in message, (
        f"the terminal failure does not name the vanished key / the redeploy "
        f"cause: {node['error_class']}: {node['error_message']!r}"
    )
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root == "failed", f"the root row is {root!r} — the run must FAIL LOUDLY"
