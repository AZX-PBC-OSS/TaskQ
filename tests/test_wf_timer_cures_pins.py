"""The timer machinery's pins — the reserved ``__timer__`` prefix, the
approval-broadcast split, the explorer's timer face, the expiry drain,
the redeploy wedge's status surface, and the naive-datetime doc.

What each pin holds shut:

* the ``__timer__`` TROJAN PREFIX: a HITL payload model named
  ``__timer__Approval`` must never mint a hold readers mistake for the
  CLOCK's. The refusal is at the MINT —
  ``wait_signal`` (and ``wait_until``'s key) raise the typed
  E17-reserved-timer-prefix error naming the rule, no row is written.
* the TIMER LEAK into the APPROVAL BROADCAST: the expiry-as-value
  create-broadcast skips a timer hold (the leg is the APPROVAL
  broadcast; the wake's RESOLVED leg already announces the fire), and
  the backfill's snapshot excludes the prefix (mirroring
  ``HitlClient.list``'s exclusion). No second marker (a kind flag) is
  introduced: the prefix IS the row's own kind-truth, and two markers
  would be two mechanisms.
* the EXPLORER'S TIMER TEMPLATE: the run explorer renders a timer AS
  a timer (the key, the scheduled instant, the fired_by) with no resolve
  affordance; the CLI's why-stuck line gets the same branch (the same
  one mechanism, the same prefix).
* the 10K-TIMER TAIL: the sweep DRAINS due timers until empty per tick
  (batch-bounded per iteration, cap-bounded per drain) + the PARTIAL
  index the reads chose: ``(expires_at, id) WHERE status = 'held' and
  starts_with(signal_name, '__timer__')`` — the EXPLAIN receipt is an
  Index Only Scan (no sort, the prefix consumed by the index predicate;
  the full ``(status, expires_at)`` index measured strictly worse: a
  Bitmap Heap Scan + Sort + residual filter).
* the VERSIONING WEDGE'S SURFACE: rename/remove a node in a
  redeploy and in-flight runs at that node zombie — the registry-is-truth
  contract entails the mixing, so the surface carries it: the docs' §6
  carries the wedge paragraph, and ``flows status --app`` compares the
  run's node inventory against the CURRENT registry and NAMES the
  stranded nodes.
* the NAIVE-DATETIME DOC: the silent UTC coercion is documented
  AT the API FACE (the ``wait_until`` docstring + the guide's timer
  section) — one paragraph, exact.

The mutation reds for these pins ride
``.measurements/timer-fixes-pin-reds-pre.txt`` and the scenario's
``.measurements/timer-fixes-mutation pin-pre.txt``.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sys
import time
import types
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.backend._protocol import JobId
from taskq.exceptions import ReservedTimerNamespaceError
from taskq.workflows import (
    Expired,
    FlowRunner,
    GateDecl,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)
from taskq.workflows.api._ctx_timer import CtxTimerOps
from taskq.workflows.api._ctx_wait import CtxWaitOps
from taskq.workflows.api._hitl import (
    TIMER_SIGNAL_PREFIX,
    HitlClient,
    sweep_due_timers,
)
from taskq.workflows.api._hitl_listen import (
    Backfilled,
    HitlListener,
    HoldCreated,
)
from taskq.workflows.api._sql_runner import render_sql
from taskq.workflows.definitions import WorkflowDef, get_registry
from taskq.workflows.engine import render_workflow_sql

cli_runner = CliRunner()

MODULE_NAME = "timer_cures_demo_flows"


# ── the demo bodies ──────────────────────────────────────────────────────


class Nudge(BaseModel):
    note: str = "the clock's knock"


class Approval(BaseModel):
    verdict: str
    note: str = ""


class TrojannParams(BaseModel):
    pass


async def _c5_first(ctx: StepContext, params: Nudge) -> str:
    return "first:v1"


async def _c5_renamed(ctx: StepContext, params: str) -> str:
    return "renamed:v2"


# the defect is the name itself — a hand-facing mint whose payload model
# collides with the reserved namespace. Module level: no name mangling.
class __timer__Approval(BaseModel):  # noqa: N801  # Why: the name is the defect — a payload model colliding with the reserved __timer__ namespace is the mint the refusal owns.
    verdict: str


async def timer_body(ctx: StepContext, params: Nudge) -> str:
    wake = await ctx.wait_until(30.0, key="fixes-timer")
    return f"woken by {wake.fired_by}"


async def human_body(ctx: StepContext, params: Nudge) -> str:
    await ctx.wait_signal((Approval,), timeout_s=30.0, reason="the human gate")
    return "answered"


async def trojan_body(ctx: StepContext, params: TrojannParams) -> str:
    await ctx.wait_signal((__timer__Approval,), timeout_s=0.2, reason="the trojan's mint")
    return "answered"


# ── the mint probe (fix 1's direct face — the mixin's typed host) ──────


class MintProbe(CtxWaitOps, CtxTimerOps):
    """A minimal WaitHost: enough context for the mint paths — the E17
    refusal fires before any pool use, so the probe proves the refusal's
    ORDER (at the mint, pre-pool)."""

    def __init__(self, pool: asyncpg.Pool, schema: str) -> None:
        self._pool = pool
        self._wsql = render_workflow_sql(schema)
        self.flow_id = JobId("00000000-0000-0000-0000-000000000001")
        self.job_id = JobId("00000000-0000-0000-0000-000000000002")
        self.node_key = "probe_node"
        self.attempt = 1
        self._is_loop = False
        self._ledger_id = None
        # a plain step's probe carries map_index None (the identity the refusal
        # answers).
        self._map_index = None
        self._workflow_name = "timer_cures_probe_flow"
        self._redact = None
        self._runtime = {}
        # the wait machinery's per-attempt call counter (the WaitHost
        # protocol's field — the probe never reaches the mint paths, but
        # it implements the host's shape).
        self._wait_call_seq = 0


@pytest.fixture(autouse=True)
def _cures_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dev posture (the admin render's resolve affordance must be
    ENABLED for the red to be the form itself)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The demo workflows (the timer, the human gate, the trojan) under
    one module — ``--app``'s target for the CLI pin."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("cures_timer_flow")
    def cures_timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    @app_obj.workflow("cures_human_flow")
    def cures_human_flow() -> Promise[object]:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=30.0)
        return build(step(human_body, Nudge(), key="human_node", gates=(gate,)))

    # fix 5's --app face. the redeployed code — 'second' renamed away, the node
    # the in-flight run sits at.
    @app_obj.workflow("c5_zombie_flow")
    def c5_zombie_flow() -> Promise[object]:
        p = step(_c5_first, Nudge(), key="first")
        return build(step(_c5_renamed, p, key="renamed"))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


def _make_admin_app(pool: asyncpg.Pool, schema: str) -> Any:
    """The admin app (the [fastapi] extra's surface): the imports live
    here — the module must import clean on the non-fastapi legs (the
    vault/aad extras suites run this file too; only the explorer test
    consumes fastapi, and it importorskips)."""
    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    bundle = create_router(pool, schema=schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


# ── the trojan prefix is refused at the mint ──────────────────────────


async def test_e17_the_trojan_prefix_is_refused_at_the_wait_signal_mint(
    wf_pool: asyncpg.Pool, wf_schema: str, timer_cures_redlog: Any
) -> None:
    """The mint's direct face: a payload model whose NAME collides
    with the reserved ``__timer__`` namespace is the typed E17 refusal at
    the mint — LOUDLY, with the fix named, and no hold row written (the
    invisible/unanswerable/unfail-closing/deadly hold is
    unrepresentable).

    The rejected shape: the hold minted, ``HitlClient.list()`` empty, the human's answer
    refused as a forgery, the run dead at the deadline)."""
    probe = MintProbe(wf_pool, wf_schema)
    with pytest.raises(ReservedTimerNamespaceError) as exc_info:
        await probe.wait_signal((__timer__Approval,), timeout_s=1.0)
    message = str(exc_info.value)
    assert "E17-reserved-timer-prefix" in message, message
    assert "__timer__Approval" in message, "the refusal must name the offending model"
    assert "rename" in message.lower(), "the refusal must name the fix"
    # no trojan hold exists to mis-read.
    rows = await wf_pool.fetch(
        render_sql("SELECT id FROM {schema}.wf_signals WHERE workflow_id = $1", wf_schema),
        probe.flow_id,
    )
    assert not rows, f"the refusal is at the mint — no hold row may be written: {rows}"
    timer_cures_redlog.red(
        "e17-mint-refusal",
        "the mint without the reserved-prefix refusal: a payload model named "
        "__timer__Approval minted a hold invisible to HitlClient.list(), "
        "unanswerable, never fail-closing, fired at the deadline into a "
        "SignalPayloadError death",
        {
            "recorded finding": "the shipped mint refuses the reserved prefix loudly; "
            "the scenario capture records the refusal",
        },
    )


async def test_e17_the_run_dies_named_and_no_trojan_hold_is_minted(
    wf_pool: asyncpg.Pool, wf_schema: str, timer_cures_redlog: Any
) -> None:
    """The runner's face: the trojan
    flow's node terminal-fails with the E17 class on the record — the
    run DIES NAMED (never silent, never hanging, never firing a trojan
    wake), and no trojan hold is ever minted."""
    app = WorkflowApp()

    @app.workflow("cures_trojan_flow")
    def cures_trojan_flow() -> Promise[object]:
        return build(step(trojan_body, TrojannParams(), key="trojan_node"))

    runner = FlowRunner(app.get("cures_trojan_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)  # one tick: the claim + the E17 death
    node = await wf_pool.fetchrow(
        render_sql(
            "SELECT status, error_class, error_message FROM {schema}.jobs "
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'trojan_node'",
            wf_schema,
        ),
        flow_id,
    )
    assert node is not None, "the trojan node never claimed"
    assert node["error_class"] == "ReservedTimerNamespaceError", (
        f"the run must die NAMED (the deterministic authoring route), got "
        f"{node['error_class']}: {node['error_message']}"
    )
    assert "E17-reserved-timer-prefix" in str(node["error_message"])
    holds = await wf_pool.fetchval(
        render_sql(
            "SELECT count(*) FROM {schema}.wf_signals WHERE workflow_id = $1",
            wf_schema,
        ),
        flow_id,
    )
    assert holds == 0, f"no trojan hold may ever be minted: {holds}"
    timer_cures_redlog.red(
        "e17-run-dies-named",
        "the mint without the refusal: the trojan mint succeeds, the wake fires at the "
        "deadline and the run died SignalPayloadError (UnabsorbedNodeFailure on "
        "the root) — the scenario's capture",
        {
            "recorded finding": "the shipped node dies on the FIRST attempt with the "
            "E17 class on the record, zero holds minted",
        },
    )


async def test_e17_wait_until_refuses_a_key_in_the_reserved_namespace(
    wf_pool: asyncpg.Pool, wf_schema: str, timer_cures_redlog: Any
) -> None:
    """The caller's key: ``wait_until(…, key='__timer__…')``
    collides with the reserved namespace the same way (the mint would
    double-prefix the row into a mis-named wake) — the same typed E17
    refusal, the fix named."""
    probe = MintProbe(wf_pool, wf_schema)
    with pytest.raises(ReservedTimerNamespaceError) as exc_info:
        await probe.wait_until(1.0, key=f"{TIMER_SIGNAL_PREFIX}evil")
    message = str(exc_info.value)
    assert "E17-reserved-timer-prefix" in message, message
    timer_cures_redlog.red(
        "e17-key-refusal",
        "the key face without the refusal: wait_until accepts a key inside the reserved "
        "namespace (a double-prefixed row the wake's key-strip mis-names)",
        {"recorded finding": "the shipped mint refuses the caller's key too"},
    )


# ── the timer never rides the approval broadcast ──────────────────────


async def test_the_backfill_never_surfaces_a_timer_hold(
    wf_pool: asyncpg.Pool, wf_schema: str, timer_cures_redlog: Any
) -> None:
    """The backfill's face: the listener's snapshot is the
    pending-APPROVALS read — a standing timer hold is announced as
    NEITHER a ``HoldCreated`` NOR an open id in the ``Backfilled``
    reconcile; the HUMAN hold beside it is (the control: the filter
    never ate the leg). The rejected shape: ``HoldCreated(signal='__timer__leak')``)."""
    compiled = WorkflowApp()
    # (bodies are module-level; the same names as the demo module)

    @compiled.workflow("c2_timer_flow")
    def c2_timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    @compiled.workflow("c2_human_flow")
    def c2_human_flow() -> Promise[object]:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=30.0)
        return build(step(human_body, Nudge(), key="human_node", gates=(gate,)))

    timer_runner = FlowRunner(compiled.get("c2_timer_flow"), wf_pool, wf_schema)
    timer_flow = (await timer_runner.create_flow()).flow_id
    await timer_runner.drive(timer_flow, until="held")
    human_runner = FlowRunner(compiled.get("c2_human_flow"), wf_pool, wf_schema)
    human_flow = (await human_runner.create_flow()).flow_id
    await human_runner.drive(human_flow, until="held")

    # both holds stand (the timer's and the human's).
    timer_row = await wf_pool.fetchrow(
        render_sql(
            "SELECT id, signal_name FROM {schema}.wf_signals "
            "WHERE workflow_id = $1 AND status = 'held'",
            wf_schema,
        ),
        timer_flow,
    )
    human_row = await wf_pool.fetchrow(
        render_sql(
            "SELECT id, signal_name FROM {schema}.wf_signals "
            "WHERE workflow_id = $1 AND status = 'held'",
            wf_schema,
        ),
        human_flow,
    )
    assert timer_row is not None and human_row is not None, "the drives never held"
    assert timer_row["signal_name"].startswith(TIMER_SIGNAL_PREFIX)

    listener = HitlListener(wf_pool, wf_schema)
    await listener.start()
    try:
        events: list[Any] = []

        async def collect() -> None:
            async for event in listener.events():
                events.append(event)
                if isinstance(event, Backfilled):
                    return

        await asyncio.wait_for(collect(), timeout=10)
    finally:
        await listener.stop()

    created = [e for e in events if isinstance(e, HoldCreated)]
    timer_created = [e for e in created if e.signal.startswith(TIMER_SIGNAL_PREFIX)]
    assert not timer_created, (
        f"the backfill surfaced the TIMER as a pending approval: {timer_created}"
    )
    human_created = [e for e in created if e.hold_id == str(human_row["id"])]
    assert human_created, (
        f"the control failed — the backfill stopped announcing HUMAN holds too: {created}"
    )
    backfilled = next(e for e in events if isinstance(e, Backfilled))
    assert str(timer_row["id"]) not in backfilled.open_hold_ids, (
        "the reconcile snapshot owns the timer as an OPEN APPROVAL — the timer "
        "is not a human's decision"
    )
    assert str(human_row["id"]) in backfilled.open_hold_ids
    # And the programmatic face agrees: HitlClient.list() stays timer-blind.
    client_timer = await HitlClient(wf_pool, wf_schema).list(timer_flow)
    assert not client_timer
    timer_cures_redlog.red(
        "c2-backfill-filter",
        "the backfill without the prefix filter: the snapshot "
        "surfaced the timer hold as HoldCreated(signal='__timer__leak') — every "
        "SSE/cowork-UI consumer saw the timer as a pending approval",
        {
            "recorded finding": "the shipped snapshot excludes the reserved prefix; the "
            "human hold beside it is announced (the control leg)"
        },
    )


async def test_the_create_broadcast_never_announces_a_timer_hold(
    wf_pool: asyncpg.Pool, wf_schema: str, timer_cures_redlog: Any
) -> None:
    """The create leg's face: the create-broadcast is the
    APPROVAL broadcast — a timer hold minted under a live listener
    announces NOTHING on it (the human control minted beside it does).
    The rejected shape:"""
    listener = HitlListener(wf_pool, wf_schema)
    await listener.start()
    try:
        # Drain the start-up backfill (an empty schema — nothing open).
        async def drain_snapshot() -> None:
            async for event in listener.events():
                if isinstance(event, Backfilled):
                    return

        await asyncio.wait_for(drain_snapshot(), timeout=10)

        compiled = WorkflowApp()

        @compiled.workflow("c2_live_timer_flow")
        def c2_live_timer_flow() -> Promise[object]:
            return build(step(timer_body, Nudge(), key="timer_node"))

        @compiled.workflow("c2_live_human_flow")
        def c2_live_human_flow() -> Promise[object]:
            gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=30.0)
            return build(step(human_body, Nudge(), key="human_node", gates=(gate,)))

        live: list[Any] = []

        async def collect_live() -> None:
            async for event in listener.events():
                if isinstance(event, Backfilled):
                    continue
                live.append(event)
                if len(live) >= 1:
                    return

        collector = asyncio.create_task(collect_live())
        timer_runner = FlowRunner(compiled.get("c2_live_timer_flow"), wf_pool, wf_schema)
        timer_flow = (await timer_runner.create_flow()).flow_id
        await timer_runner.drive(timer_flow, until="held")
        human_runner = FlowRunner(compiled.get("c2_live_human_flow"), wf_pool, wf_schema)
        human_flow = (await human_runner.create_flow()).flow_id
        await human_runner.drive(human_flow, until="held")
        # the timer's silence is the assertion (the human control below
        # names the leg's aliveness) — a TimeoutError here is the pass
        # face (the collector never got its one event: no HoldCreated
        # announced anything... the control's assert below speaks).
        with suppress(TimeoutError):
            await asyncio.wait_for(collector, timeout=10)
        timer_created = [
            e
            for e in live
            if isinstance(e, HoldCreated) and e.signal.startswith(TIMER_SIGNAL_PREFIX)
        ]
        assert not timer_created, (
            f"the create broadcast announced the TIMER as a pending approval: {timer_created}"
        )
        human_created = [
            e
            for e in live
            if isinstance(e, HoldCreated) and not e.signal.startswith(TIMER_SIGNAL_PREFIX)
        ]
        assert human_created, (
            f"the control failed — the create leg stopped announcing HUMAN holds: {live}"
        )
    finally:
        await listener.stop()
    timer_cures_redlog.red(
        "c2-create-broadcast-skip",
        "the create leg without the refusal: register_hold "
        "broadcast the timer's mint on the approval channel unconditionally",
        {
            "recorded finding": "the shipped leg skips the reserved prefix; the human "
            "mint beside it still announces (the control leg)"
        },
    )


# ── the explorer renders a timer as a timer ───────────────────────────


@pytest.mark.fastapi  # the admin surface is the [fastapi] extra's — the extras legs select by this
async def test_the_explorer_renders_a_timer_as_a_timer(
    wf_pool: asyncpg.Pool, wf_schema: str, timer_cures_redlog: Any
) -> None:
    """The run explorer's hold card branches — a timer hold
    renders AS a timer (the key, the scheduled instant, the clock as the
    resolver) with no Resolve affordance (the boundary is refuse-only:
    a Resolve form on a timer is a lie the operator answers into). The
    rejected shape: the page renders the Resolve form on the timer."""
    pytest.importorskip("fastapi")
    app = WorkflowApp()

    @app.workflow("c3_timer_flow")
    def c3_timer_flow() -> Promise[object]:
        return build(step(timer_body, Nudge(), key="timer_node"))

    runner = FlowRunner(app.get("c3_timer_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    admin = _make_admin_app(wf_pool, wf_schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=admin), base_url="http://test"
    ) as http:
        page = await http.get(f"/workflows/{flow_id}")
    assert page.status_code == 200, f"the run page never rendered: {page.status_code}"
    text = page.text
    assert "durable timer" in text.lower(), (
        "the timer renders as an unnamed hold card — the template has no timer "
        "branch (the key, the scheduled instant, the clock's resolver mark are "
        "the card's own truth)"
    )
    assert "timer key: fixes-timer" in text or "fixes-timer" in text, (
        "the timer card must name the timer's key"
    )
    # the row's own wake time renders as the card's `scheduled:` line — never
    # the placeholder dash (the dead face the page projection's double-_iso
    # carried).
    row_expires_at = await wf_pool.fetchval(
        render_sql(
            "SELECT expires_at FROM {schema}.wf_signals "
            "WHERE workflow_id = $1 AND starts_with(signal_name, '__timer__')",
            wf_schema,
        ),
        flow_id,
    )
    assert row_expires_at is not None, "the timer hold must carry its scheduled instant"
    assert f"scheduled: {row_expires_at.isoformat()}" in text, (
        "the timer card never rendered the scheduled instant — the page shows "
        "the placeholder dash on a hold whose wake time is in the row"
    )
    assert 'data-wf-form="resolve"' not in text, (
        "the explorer rendered a RESOLVE FORM on a timer hold — the boundary "
        "refuses every hand delivery to the clock's hold (refuse-only); the "
        "form is the lie the operator answers into"
    )
    timer_cures_redlog.red(
        "c3-explorer-timer-branch",
        "the template without the timer branch: workflow_detail.html "
        "rendered the Resolve form on a __timer__ hold",
        {
            "recorded finding": "the shipped template branches — the timer renders AS a "
            "timer, no resolve affordance"
        },
    )


# ── the drain + the partial index ─────────────────────────────────────


async def test_the_drain_empties_the_due_set_in_one_tick(
    wf_pool: asyncpg.Pool, wf_schema: str, timer_cures_redlog: Any
) -> None:
    """FIX 4: the leader's signal-sweep arm DRAINS — 10k due timers
    empty in one call (batch-bounded per iteration, cap-bounded per
    drain). A one-batch-per-pass face measures
    100-fires-per-pass ⇒ 100 passes x the 30 s cadence ≈ 50 minutes
    worst-case late (the measured tail). The lateness is MEASURED for
    the report (the wall time printed, never a band-flake assert); the
    PINNED semantics are the drain-until-empty + the index's plan.

    The rejected shape:"""
    n = 10000
    await wf_pool.execute(
        render_sql(
            """INSERT INTO {schema}.wf_signals
            (id, workflow_id, node_key, signal_name, hold_epoch, call_id,
             payload, payload_schema, status, expires_at)
        SELECT gen_random_uuid(), gen_random_uuid(), 'n', $2, 1,
               'timer:x', '{}'::jsonb,
               '{"TimerWake": {"type": "object"}}'::jsonb, 'held',
               now() - interval '1 second'
        FROM generate_series(1, $1) g""",
            wf_schema,
        ),
        n,
        f"{TIMER_SIGNAL_PREFIX}bulk",
    )
    due_before = await wf_pool.fetchval(
        render_sql(
            "SELECT count(*) FROM {schema}.wf_signals WHERE status = 'held' AND signal_name = $1",
            wf_schema,
        ),
        f"{TIMER_SIGNAL_PREFIX}bulk",
    )
    assert due_before == n, f"the seed failed: {due_before}"

    # the due-scan is index-led — the partial index consumes the prefix + the
    # status, orders by (expires_at, id) with no sort and no seq scan.
    plan_rows = await wf_pool.fetch(
        render_sql(
            "EXPLAIN (COSTS OFF) SELECT s.id FROM {schema}.wf_signals s "
            "WHERE s.status = 'held' "
            "AND starts_with(s.signal_name, '__timer__') "
            "AND s.expires_at IS NOT NULL AND s.expires_at <= clock_timestamp() "
            "ORDER BY s.expires_at, s.id LIMIT 100",
            wf_schema,
        )
    )
    plan = "\n".join(r[0] for r in plan_rows)
    assert "Seq Scan" not in plan, f"the due-scan is a SEQ SCAN (the 10k-tail's shape):\n{plan}"
    assert "wf_signals_timer_due_idx" in plan, f"the due-scan is not index-led:\n{plan}"
    assert "Sort" not in plan, f"the due-scan still sorts (the index must own the order):\n{plan}"

    # one call empties the due set.
    t0 = time.perf_counter()
    fired = await sweep_due_timers(wf_pool, schema=wf_schema, drain=True)
    elapsed = time.perf_counter() - t0
    fired_total = await wf_pool.fetchval(
        render_sql(
            "SELECT count(*) FROM {schema}.wf_signals "
            "WHERE status = 'delivered' AND signal_name = $1",
            wf_schema,
        ),
        f"{TIMER_SIGNAL_PREFIX}bulk",
    )
    assert fired == n, f"the drain did not empty the due set in one call: {fired}"
    assert fired_total == n, f"the rows disagree with the drain's return: {fired_total}"
    # A second pass: the empty read (the drain's own idempotence).
    assert await sweep_due_timers(wf_pool, schema=wf_schema, drain=True) == 0
    print(
        f"[timer-fixes] the drain: {n} due timers emptied in {elapsed:.2f}s — "
        f"the worst-case tail is now ONE sweep pass (~30s cadence) + "
        f"{elapsed:.2f}s of drain, not {n // 100} passes x 30s = "
        f"{(n // 100) * 30 / 60:.0f} minutes"
    )
    timer_cures_redlog.red(
        "c4-drain-until-empty",
        "the sweep without the drain: one pass fires 100 of "
        "10000 due timers — 100 passes x the 30 s cadence = ~50 minutes worst-case "
        "tail; the due-scan EXPLAIN was a Seq Scan + Sort",
        {
            "recorded finding": "the shipped arm drains until empty (batch-bounded per "
            "iteration) and the partial index leads the scan (the EXPLAIN receipt "
            "is in this pin's assertion)"
        },
    )


# ── fix 5: the versioning wedge's surface ──────────────────────────────


async def test_the_zombie_run_s_status_names_the_stranded_nodes(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    module_pg_schema: Any,
    monkeypatch: pytest.MonkeyPatch,
    demo_app_module: Any,
    timer_cures_redlog: Any,
) -> None:
    """FIX 5: the wedge's surface. A run mid-flight at a node the
    CURRENT registry no longer declares (a rename/remove redeploy) is a
    ZOMBIE: never terminal, nothing fails it, and the plain ``flows
    status`` shows NOTHING — the surface makes the drift VISIBLE: the
    status names the stranded nodes, the zombie law, and the remedy.

    The rejected shape:"""
    from taskq.cli import app as cli_app

    monkeypatch.setenv("TASKQ_PG_DSN", module_pg_schema.pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)

    # first → second; the first node runs v1, the second never claims (its
    # parent's success re-pends it mid-run). The old compile (the create-time
    # snapshot) declares 'second'.
    old_app = WorkflowApp()
    name = "c5_zombie_flow"

    @old_app.workflow(name)
    def zombie_flow_old() -> Promise[object]:
        p = step(_c5_first, Nudge(), key="first")
        return build(step(_c5_renamed, p, key="second"))

    compiled = old_app.get(name)
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    rows = await runner._claimable(flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the pin stops the driver mid-flow — the claim loop's seam is the behavior under test.
    assert [r["step_key"] for r in rows] == ["first"], rows
    await runner._run_node(flow_id, rows[0])  # pyright: ignore[reportPrivateUsage]

    # the registry carries the new code ('second' renamed away) — the
    # versioning pins' own swap seam (a new process's registry holds only the
    # new definition).
    definition = WorkflowDef(name=name, bodies={"first": _c5_first, "renamed": _c5_renamed})
    get_registry()._workflows[name] = definition  # pyright: ignore[reportPrivateUsage]  # Why: the redeploy simulation is the registry map's replacement.

    # the CLI's status, with the current registry (--app): the run's node
    # 'second' is stranded — the current graph declares {'first', 'renamed'}.
    # The drift is visible.
    result = await asyncio.to_thread(
        cli_runner.invoke,
        cli_app,
        [
            "flows",
            "status",
            str(flow_id),
            "--app",
            f"{MODULE_NAME}:app",
        ],
    )
    assert result.exit_code == 0, result.output
    out = result.output
    assert "versioning drift" in out, (
        f"the zombie run's status shows NOTHING — the stranded node is "
        f"invisible to the operator the docs teach:\n{out}"
    )
    assert "second" in out, f"the drift line must NAME the stranded node:\n{out}"
    assert "ZOMBIE" in out.upper(), f"the drift line must name the zombie law:\n{out}"
    assert "flows cancel" in out, f"the drift line must name the remedy:\n{out}"
    # The un-drifted control: a node the current registry still declares
    # ('first') is never named stranded.
    drift_line = next(line for line in out.splitlines() if "versioning drift" in line)
    assert "first" not in drift_line.split(":", 2)[-1] or "'first'" not in drift_line, (
        f"the drift line names a node the current registry DECLARES:\n{drift_line}"
    )
    timer_cures_redlog.red(
        "c5-zombie-status-drift",
        "the status without the wedge surface: the zombie run's status "
        "showed NOTHING — no --app door, no stranded-node line, the drift "
        "invisible to the operator the docs teach",
        {"recorded finding": "the shipped status names the stranded nodes + the remedy"},
    )


# ── the naive-datetime law at the API face ────────────────────────────


def test_the_naive_datetime_law_is_documented_at_the_api_face(
    timer_cures_redlog: Any,
) -> None:
    """FIX 6: the silent UTC coercion is documented AT the API FACE —
    the ``wait_until`` docstring carries the exact law (a naive datetime
    is interpreted as UTC; the DB never re-bases it) and the guide's
    timer section carries the same paragraph. The rejected shape: at the base
    tree the docstring carried a clause, not the law (the expanded
    paragraph is the fix)."""
    doc = " ".join((inspect.getdoc(CtxTimerOps.wait_until) or "").split())
    assert "interpreted as UTC" in doc, (
        "the wait_until docstring does not carry the naive-datetime law "
        "(a naive datetime is interpreted as UTC — the coercion is silent)"
    )
    assert "never re-bases" in doc, "the docstring must name the DB's never-re-bases law"
    guide = (Path(__file__).parent.parent / "docs" / "guides" / "workflows.md").read_text()
    section = " ".join(guide.split("## §4b", 1)[1].split("## §5", 1)[0].split())
    assert "interpreted as UTC" in section, (
        "the guide's timer section does not carry the naive-datetime law"
    )
    assert "never re-bases" in section
    timer_cures_redlog.red(
        "c6-naive-utc-doc",
        "the doc without the coercion paragraph: the coercion is a clause, not "
        "the law — the docstring and the guide carried no exact paragraph",
        {"recorded finding": "the shipped faces carry the exact one-paragraph law"},
    )


# ── fix 7: the timer hold gains the map INDEX (the ruling's execution) ──


class TimerItem(BaseModel):
    pass


class TimerNote(BaseModel):
    index: int


# Why load_sensitive: the pin's window ("the timer expired inside the
# pin's window") holds when the wait-site's resume lands inside it —
# under -n 8 the resume's scheduling delay crossed the window once;
# solo and serial it holds. The identity law the pin exists for (the
# map sibling's own hold identity) is a serial-lane fact.
@pytest.mark.load_sensitive
async def test_a_timer_hold_in_a_map_sibling_context_gets_its_own_identity(
    wf_pool: Any, wf_schema: str, wf_conn: Any, timer_cures_redlog: Any
) -> None:
    """The merge-order ruling's timer x holdmap execution: the holdmap
    lane's map-index identity (``wf_signals_hold_identity_uniq`` gains
    ``COALESCE(map_index, -1)``) must reach the TIMER hold too —
    ``ctx.wait_until`` inside a map item mints EACH sibling its own
    timer row (the clock never collides across siblings), and each
    sibling wakes on its own fire.

    REJECTED VARIANT (probed live): the timer
    mint threaded no map index — every sibling's timer hold landed on
    the ``-1`` sentinel, and the second sibling's ``wait_until`` raised
    ``UniqueViolationError`` (``wf_signals_hold_identity_uniq``) into
    the ladder (worker mode) or wedged the run (the wake read one
    arbitrary sibling's row).
    """
    app = WorkflowApp()

    async def src_body(ctx: Any, params: TimerItem) -> list[int]:
        return [0, 1]

    async def item_body(ctx: Any, item: int) -> TimerNote:
        wake = await ctx.wait_until(0.05, key="item-tick")
        if isinstance(wake, Expired):
            raise AssertionError("the timer expired inside the pin's window")
        return TimerNote(index=ctx.map_index)

    async def join_body(ctx: Any, notes: list[TimerNote]) -> list[TimerNote]:
        return notes

    @app.workflow("timer_map_sibling_identity")
    def flow() -> Promise[object]:
        src = step(src_body, TimerItem(), key="src")
        items = map_source(src, item_body)
        return build(step(join_body, items, key="collect"))

    runner = FlowRunner(app.get("timer_map_sibling_identity"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    # both siblings' timer holds exist as DISTINCT rows (the second
    # sibling's mint must never collide on the -1 sentinel).
    rows: list[Any] = []
    for _ in range(20):
        rows = await wf_conn.fetch(
            render_sql(
                "SELECT node_key, signal_name, hold_epoch, "
                "map_index FROM {schema}.wf_signals "
                "WHERE workflow_id = $1 AND status = 'held'",
                wf_schema,
            ),
            flow_id,
        )
        if len(rows) == 2:
            break
        await runner.drive(flow_id, until="held")
    assert len(rows) == 2, (
        "the map siblings' TIMER holds are NOT distinct rows — the second "
        f"sibling's wait_until collided on the identity arbiter. Observed: "
        f"{[dict(r) for r in rows]}"
    )
    indexes = sorted(r["map_index"] for r in rows)
    assert indexes == [0, 1], indexes

    # drive past the deadline; the sweep fires both timers; the run completes
    # with both siblings' notes (a sibling that never woke is the wedged
    # variant's face).
    await asyncio.sleep(0.2)
    from taskq.workflows.api._hitl import sweep_due_timers

    await sweep_due_timers(wf_pool, schema=wf_schema)
    status = None
    for _ in range(40):
        await runner.drive(flow_id)
        status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        if status in ("succeeded", "failed", "cancelled"):
            break
        await asyncio.sleep(0.05)
    assert status == "succeeded", status
    collect = await wf_conn.fetchval(
        f"SELECT result->>'value' FROM \"{wf_schema}\".jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'collect'",
        flow_id,
    )
    assert collect is not None
    notes = [n["index"] for n in json.loads(collect)]
    assert sorted(notes) == [0, 1], notes
    timer_cures_redlog.red(
        "c7-timer-hold-map-index",
        "the variant: the timer mint threads no map "
        "index — the second map sibling's wait_until raised "
        "wf_signals_hold_identity_uniq INTO the ladder (the rejected "
        "collision); the fix threads the item's own index through "
        "register_timer_hold → register_hold",
        {"timer_holds": len(rows), "map_indexes": indexes},
    )
