# ruff: noqa: N999, S608, S603, ASYNC220, ASYNC221, N802  # Why: the dash-named attack4-* module; the CLI probes run REAL subprocesses (the stranger's own terminal) with fixed argv and fixture-derived env; the TRUE blocker's name is the pin's claim.
"""ATTACK4 — THE CLI UNDER FIRE (the phase-4 attacker's suite).

The stranger's probes against ``taskq flows`` (T12's surface), NOT the
builder's pins (tests/test_wf_cli_pins.py owns those) — the adversarial
shapes:

* the verbs against LIVE CONCURRENT runs: a resolve racing a cancel —
  the exit codes + the outputs honest at every overlap (one typed
  outcome, no traceback, no double-delivery, the audit rows agree);
* the typed door's refusal shapes: a malformed / wrong-shaped payload →
  the NAMED pydantic refusal (named, not a bare "error"), and the hold
  SURVIVES (nothing moved);
* the why-stuck arm's correctness: a DELIBERATELY stuck run (a failed
  node blocking a downstream closure) — does ``flows status`` name the
  TRUE blocker (the failed row, with its remedy), not a symptom?;
* the exit-code contract: the empty state, the error state (a bad UUID,
  an unknown run), the malformed-decision state — every one a named
  error + exit 1, never a traceback; the empty LIST an honest zero + 0;
* the TORN CANCEL probe (the structural finding): the cancel cascade's
  signal leg runs on a SECOND connection outside the caller's
  transaction — a rollback after it must not leave held signals
  cancelled on a run that never cancelled.

This file FIXES NOTHING: a red here is a finding, reported.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Any

import pytest
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import FlowRunner, WorkflowApp, build, step
from taskq.workflows.api import GateDecl

pytestmark = pytest.mark.integration


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


MODULE_NAME = "attack4_cli_demo_flows"


def _module() -> Any:
    import types

    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    async def _wait(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), reason="editorial approval", timeout_s=120.0)
        return "published"

    async def _explode(ctx: Any, params: Ingest) -> str:
        raise RuntimeError("the upstream blew up")

    async def _downstream(ctx: Any, up: str) -> str:
        return up

    @app_obj.workflow("attack4b_hold_flow")
    def hold_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("attack4b_stuck_flow")
    def stuck_flow() -> object:
        doomed = step(_explode, Ingest(doc_id="d1"), key="doomed", max_attempts=1)
        return build(step(_downstream, doomed, key="downstream"))

    module.app = app_obj  # type: ignore[attr-defined]
    return module


@pytest.fixture(scope="module")
def demo_env(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """The module's dev posture + the demo WorkflowApp written to a REAL
    importable file (the subprocess's ``--app`` resolves through the
    normal import machinery — the stranger's own terminal). The FILE's
    shapes mirror _module()'s (the gate models' names + fields are the
    typed door's contract — they must agree with the flows the test
    process creates)."""
    # The env posture rides a raw MonkeyPatch (the suite-hygiene law: a
    # bare os.environ write has no teardown — the atk_iso incident). The
    # undo() runs at the fixture's end, every exit path.
    env_patch = pytest.MonkeyPatch()
    env_patch.setenv("TASKQ_ENVIRONMENT", "dev")
    module = _module()
    sys.modules[MODULE_NAME] = module
    app_dir = tmp_path_factory.mktemp("attack4-app")
    (app_dir / f"{MODULE_NAME}.py").write_text(
        "from typing import Any\n\n"
        "from pydantic import BaseModel\n\n"
        "from taskq.workflows import WorkflowApp, build, step\n"
        "from taskq.workflows.api import GateDecl\n\n\n"
        "class Approval(BaseModel):\n"
        "    verdict: str\n"
        '    note: str = ""\n\n\n'
        "class Ingest(BaseModel):\n"
        "    doc_id: str\n\n\n"
        "async def _wait(ctx: Any, params: Ingest) -> str:\n"
        "    await ctx.wait_signal((Approval,), reason='editorial approval', timeout_s=120.0)\n"
        "    return 'published'\n\n\n"
        "async def _explode(ctx: Any, params: Ingest) -> str:\n"
        "    raise RuntimeError('the upstream blew up')\n\n\n"
        "async def _downstream(ctx: Any, up: str) -> str:\n"
        "    return up\n\n\n"
        "app = WorkflowApp()\n\n\n"
        '@app.workflow("attack4b_hold_flow")\n'
        "def hold_flow():\n"
        "    gate = GateDecl(name='Approval', payload_models=(Approval,), timeout_s=120.0)\n"
        "    return build(step(_wait, Ingest(doc_id='d1'), key='review', gates=(gate,)))\n\n\n"
        '@app.workflow("attack4b_stuck_flow")\n'
        "def stuck_flow():\n"
        "    doomed = step(_explode, Ingest(doc_id='d1'), key='doomed', max_attempts=1)\n"
        "    return build(step(_downstream, doomed, key='downstream'))\n"
    )
    module.app_dir = str(app_dir)  # type: ignore[attr-defined]
    yield module
    del sys.modules[MODULE_NAME]
    env_patch.undo()


def _cli(
    module_pg_schema: ModulePgSchema,
    *args: str,
    env_extra: dict[str, str] | None = None,
    demo_env: Any = None,
) -> subprocess.CompletedProcess[str]:
    """One REAL ``taskq flows`` subprocess against THIS module's schema —
    the exit code + the output the operator actually sees."""
    env = dict(os.environ)
    env["TASKQ_PG_DSN"] = module_pg_schema.pg_dsn
    env["TASKQ_SCHEMA_NAME"] = module_pg_schema.schema_name
    if demo_env is not None:
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{demo_env.app_dir}:{existing}" if existing else str(demo_env.app_dir)  # type: ignore[attr-defined]
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "taskq", "flows", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


async def _held_run(module_pg_pool: Any, module_pg_schema: ModulePgSchema) -> str:
    compiled = sys.modules[MODULE_NAME].app.get("attack4b_hold_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, module_pg_schema.schema_name)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    return str(flow_id)


# ── the resolve racing the cancel ────────────────────────────────────────


async def test_the_resolve_racing_the_cancel_one_outcome_honest_codes(
    module_pg_schema: ModulePgSchema, module_pg_pool: Any, demo_env: Any
) -> None:
    """A resolve and a cancel land CONCURRENTLY on one held run (two real
    CLI processes): each exits with an honest code, the run ends in ONE
    named terminal, and the audit rows tell the same story the outputs
    did — no silent loser.

    THE SUBPROCESS TIMING WEATHER (the disposition's mark, recorded at
    the consolidation and re-observed at the final head): the race is
    staged through two REAL processes, so the loser's exit can carry the
    loser's OWN early-read verdict under a noisy box — a one-off red,
    green x2 solo and module-parallel (the loaded-bar law's mark, the
    demo-legs class map's own disposition). The assertion's substance is
    the OUTCOME HONESTY (one named terminal, the audit agreeing), not
    the race's scheduling."""
    from taskq.workflows.api._hitl import HitlClient

    schema = module_pg_schema.schema_name
    run_id = await _held_run(module_pg_pool, module_pg_schema)
    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)

    app_ref = f"{MODULE_NAME}:app"
    resolve_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "taskq",
            "flows",
            "resolve",
            hold.hold_id,
            '{"verdict": "approve", "note": ""}',
            "--app",
            app_ref,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={
            **os.environ,
            "TASKQ_PG_DSN": module_pg_schema.pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "PYTHONPATH": f"{demo_env.app_dir}:" + os.environ.get("PYTHONPATH", ""),
        },
    )
    cancel_proc = subprocess.Popen(
        [sys.executable, "-m", "taskq", "flows", "cancel", run_id, "--reason", "the race"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={
            **os.environ,
            "TASKQ_PG_DSN": module_pg_schema.pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "PYTHONPATH": f"{demo_env.app_dir}:" + os.environ.get("PYTHONPATH", ""),
        },
    )
    r_out, r_err = resolve_proc.communicate(timeout=60)
    c_out, c_err = cancel_proc.communicate(timeout=60)
    print(
        f"\n[attack4 race] resolve rc={resolve_proc.returncode} out={r_out.strip()!r} err={r_err.strip()[:300]!r}"
    )
    print(
        f"[attack4 race] cancel rc={cancel_proc.returncode} out={c_out.strip()!r} err={c_err.strip()[:300]!r}"
    )

    # NEITHER process crashed with a traceback — whatever the overlap,
    # the verbs answer in their own voices.
    assert "Traceback" not in r_err, f"the resolve crashed: {r_err}"
    assert "Traceback" not in c_err, f"the cancel crashed: {c_err}"
    # The outcomes agree with the ROWS: the hold is delivered-or-cancelled,
    # never both, and the run is terminal.
    row = await module_pg_pool.fetchrow(
        f'SELECT status, resolved_at FROM "{schema}".wf_signals WHERE id = $1', hold.hold_id
    )
    assert row is not None
    assert row["status"] in ("delivered", "cancelled"), f"a zombie hold: {row['status']}"
    root = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', run_id
    )
    assert root in ("succeeded", "failed", "cancelled"), f"the run is not terminal: {root}"
    # THE HONESTY: if the hold was delivered, the resolve said so (exit 0);
    # if it was cancelled first, the resolve's output says refused/no-op —
    # never a delivered-sounding line over a cancelled row.
    if row["status"] == "cancelled":
        assert "delivered" not in r_out.lower(), (
            f"the resolve CLAIMED delivery over a cancelled hold: {r_out!r}"
        )
    # The audit rows tell the same story (at least one of the two actions
    # landed; the winner's row exists).
    total_audit = await module_pg_pool.fetchval(f'SELECT count(*) FROM "{schema}".admin_audit')
    all_rows = await module_pg_pool.fetch(
        f'SELECT action, target_type, target_id, detail FROM "{schema}".admin_audit ORDER BY id DESC LIMIT 6'
    )
    print(f"\n[attack4 race] audit total={total_audit} recent={[dict(r) for r in all_rows]}")
    actions = {
        r["action"]
        for r in await module_pg_pool.fetch(
            f'SELECT action FROM "{schema}".admin_audit '
            "WHERE target_id = $1 OR target_id = $2 OR detail->>'run_id' = $2",
            str(hold.hold_id),
            run_id,
        )
    }
    if row["status"] == "delivered":
        assert "hitl.resolve" in actions, f"a delivered resolve without its row: {actions}"
    else:
        assert "workflow.cancel" in actions, f"a cancelled run without its row: {actions}"


# ── the typed door's refusal shapes ─────────────────────────────────────


async def test_a_malformed_payload_is_the_named_refusal_and_the_hold_survives(
    module_pg_schema: ModulePgSchema, module_pg_pool: Any, demo_env: Any
) -> None:
    """The typed door's refusal: a WRONG-SHAPED payload → the NAMED
    pydantic error (the model's own fields named, not a bare 'error'),
    exit 1, and the hold SURVIVES (the reply handle still lists)."""
    from taskq.workflows.api._hitl import HitlClient

    schema = module_pg_schema.schema_name
    run_id = await _held_run(module_pg_pool, module_pg_schema)
    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)

    proc = _cli(
        module_pg_schema,
        "resolve",
        hold.hold_id,
        '{"verdict": 42}',
        "--app",
        f"{MODULE_NAME}:app",
        demo_env=demo_env,
    )
    assert proc.returncode == 1, f"the wrong payload did not refuse: {proc.returncode}"
    assert "pydantic refused" in proc.stderr, f"the refusal is not the named one: {proc.stderr!r}"
    assert "verdict" in proc.stderr, "the refusal does not name the field that failed"
    assert "Traceback" not in proc.stderr
    # THE HOLD SURVIVES.
    assert len(await client.list(run_id)) == 1, "the refused resolve moved the hold"

    # A non-object decision → the parse refusal (not the pydantic one).
    for bad in ("[1,2]", '"just a string"', "not json at all"):
        proc = _cli(
            module_pg_schema,
            "resolve",
            hold.hold_id,
            bad,
            "--app",
            f"{MODULE_NAME}:app",
            demo_env=demo_env,
        )
        assert proc.returncode == 1, f"{bad!r}: exit {proc.returncode}"
        assert "Traceback" not in proc.stderr
        assert ("not valid JSON" in proc.stderr) or ("must be a JSON object" in proc.stderr), (
            f"{bad!r}: the refusal is unnamed: {proc.stderr!r}"
        )
    assert len(await client.list(run_id)) == 1, "the refused decisions moved the hold"


# ── the why-stuck arm's truth ────────────────────────────────────────────


async def test_the_why_stuck_arm_names_the_TRUE_blocker(
    module_pg_schema: ModulePgSchema, module_pg_pool: Any, demo_env: Any
) -> None:
    """A deliberately stuck run: 'doomed' fails on an EXHAUSTED ladder
    (max_attempts=1); 'downstream' can never run. ``flows status`` must
    name the FAILED row as the thing to retry (with its remedy), and the
    blocked row's answer must be TRUE — run5's observed output failed
    both honesty halves (the attack-4 report's F-P4-WHYSTUCK-*); both
    cures are pinned here as hard asserts (the xfail was the pre-cure
    shape, the attack-4 fixer's round 1 removed it):

    * F-P4-WHYSTUCK-LADDER-LIE (cured): the status read carries the
      attempt counters — an EXHAUSTED ladder reads "the ladder is
      EXHAUSTED", never "ladder headroom 3";
    * F-P4-WHYSTUCK-FALSE-REMEDY (cured): the create path stamps the
      join marker (the engine's own law on the static path), so the
      failed-parent cascade resolves the row to BLOCKED-failed_parent —
      the remedy derives from the REASON, the join-fires promise is
      gone."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("attack4b_stuck_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    # A BOUNDED drive: the run's root does not flip while the blocked
    # closure stands (the derivation says 'blocked', not terminal) — an
    # unbounded drive would spend its max_ticks sleeping. The rows are
    # what the why-stuck arm reads; 10 ticks seed them.
    await runner.drive(flow_id, max_ticks=10)

    proc = _cli(module_pg_schema, "status", str(flow_id), demo_env=demo_env)
    out, err, code = proc.stdout, proc.stderr, proc.returncode
    assert code == 0, f"the status of a stuck run is not an error: {code} {err}"
    # THE TRUE BLOCKER: the FAILED row named with its class + remedy.
    assert "doomed: FAILED" in out, f"the failed row is not named: {out!r}"
    assert "the upstream blew up" in out, "the error message did not ride the why-stuck line"
    assert "taskq flows retry" in out, "the remedy is missing"
    # THE LADDER'S TRUTH (F-P4-WHYSTUCK-LADDER-LIE, CURED): the ladder was
    # max_attempts=1 and it spent its attempt — the honest note is
    # "the ladder is EXHAUSTED", not "headroom 3". The status read carries
    # the row's own attempt counters now.
    assert "the ladder is EXHAUSTED" in out, f"the exhausted ladder is misreported: {out!r}"
    # THE CONSEQUENCE'S TRUTH (F-P4-WHYSTUCK-FALSE-REMEDY, CURED): the
    # blocked row's answer derives from the blocking REASON — the parent
    # finalized as a FAILURE, so the row reads BLOCKED-failed_parent and
    # the join-fires promise is GONE (the create path stamps the join
    # marker; the failed-parent cascade resolves the row).
    assert "downstream: BLOCKED" in out, f"the blocked row is not named: {out!r}"
    assert "the join fires when its parents finalize" not in out, (
        "the false remedy: the parent finalized as a FAILURE — the join never fires"
    )


# ── the exit-code contract ───────────────────────────────────────────────


async def test_the_exit_code_contract_the_empty_error_and_missing_states(
    module_pg_schema: ModulePgSchema, module_pg_pool: Any, demo_env: Any
) -> None:
    """THE EXIT CODES: a bad UUID → exit 1 + the named error; an unknown
    run's status/holds → exit 1 + the named error + the remedy; an empty
    schema's ``flows list`` → exit 0 + the honest zero (never a blank
    section, never a traceback)."""
    # THE BAD UUID.
    for verb in ("status", "holds", "cancel"):
        proc = _cli(module_pg_schema, verb, "not-a-uuid", demo_env=demo_env)
        assert proc.returncode == 1, f"{verb}: a bad UUID exited {proc.returncode}"
        assert "invalid run id" in proc.stderr
        assert "Traceback" not in proc.stderr
    # THE UNKNOWN RUN.
    ghost = "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"
    proc = _cli(module_pg_schema, "status", ghost, demo_env=demo_env)
    assert proc.returncode == 1
    assert f"no run {ghost}" in proc.stderr
    assert "taskq flows list" in proc.stderr  # the remedy
    proc = _cli(module_pg_schema, "holds", ghost, demo_env=demo_env)
    assert proc.returncode == 0, "an unknown run's holds is the honest zero, not an error"
    assert "none pending" in proc.stdout
    # THE EMPTY LIST: a FRESH schema (migrated by the CLI's own migrate
    # verb — the stranger's setup path), zero runs — the honest zero + 0.
    fresh = "attack4_empty_list"
    _cli_raw = subprocess.run(
        [sys.executable, "-m", "taskq", "migrate", "up"],
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, "TASKQ_PG_DSN": module_pg_schema.pg_dsn, "TASKQ_SCHEMA_NAME": fresh},
    )
    assert _cli_raw.returncode == 0, f"the fresh schema never migrated: {_cli_raw.stderr}"
    proc = _cli(module_pg_schema, "list", demo_env=demo_env, env_extra={"TASKQ_SCHEMA_NAME": fresh})
    assert proc.returncode == 0, proc.stderr
    assert "none yet" in proc.stdout, f"the empty list is not the honest zero: {proc.stdout!r}"
    assert "not an error" in proc.stdout
    # THE UNKNOWN HOLD.
    proc = _cli(
        module_pg_schema,
        "resolve",
        ghost,
        '{"verdict": "approve"}',
        "--app",
        f"{MODULE_NAME}:app",
        demo_env=demo_env,
    )
    assert proc.returncode == 1
    assert f"no hold {ghost}" in proc.stderr
    assert "Traceback" not in proc.stderr


async def test_the_stale_app_resolve_is_the_named_refusal_never_a_traceback(
    module_pg_schema: ModulePgSchema, module_pg_pool: Any, demo_env: Any
) -> None:
    """F-P4-CLI-KEYERROR-TRACEBACK's PIN (the attack-4 report's MEDIUM;
    the red was the attacker's race repro — attack4-cli-races-run1): a
    run whose workflow is NOT declared on the --app module (the
    stale-deploy world) meets 'flows resolve' — the typed door must
    refuse with the NAMED error + exit 1, never a rich traceback. The
    pre-cure shape: gate_models_for → app.get(workflow) → the uncaught
    KeyError → the traceback (the admin's twin catches it; the CLI did
    not)."""

    # The run's workflow is declared on a DIFFERENT app than the one the
    # CLI's --app names — the app moved since the run started.
    other = WorkflowApp()

    async def _hold(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), reason="the other app", timeout_s=120.0)
        return "done"

    gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
    other.workflow("attack4b_GHOST_flow")(  # a name the demo module never declares
        lambda: build(step(_hold, Ingest(doc_id="d1"), key="review", gates=(gate,)))
    )
    runner = FlowRunner(
        other.get("attack4b_GHOST_flow"), module_pg_pool, module_pg_schema.schema_name
    )
    run_id = (await runner.create_flow()).flow_id
    await runner.drive(
        run_id, until="held"
    )  # the REAL hold row (the gate's lookup must reach the KeyError)
    from taskq.workflows.api._hitl import HitlClient

    hc = HitlClient(module_pg_pool, schema=module_pg_schema.schema_name)
    (hold,) = await hc.list(str(run_id))

    proc = _cli(
        module_pg_schema,
        "resolve",
        hold.hold_id,
        '{"verdict": "approve"}',
        "--app",
        f"{MODULE_NAME}:app",  # declares hold_flow/stuck_flow — NOT the ghost
        demo_env=demo_env,
    )
    assert proc.returncode == 1, f"the stale-app resolve exited {proc.returncode}: {proc.stdout}"
    assert "Traceback" not in proc.stderr, (
        f"the output contract is broken — the operator saw a traceback: {proc.stderr!r}"
    )
    assert "not declared on this app" in proc.stderr, (
        f"the refusal does not NAME the miss: {proc.stderr!r}"
    )


async def test_the_cancel_of_an_already_terminal_run_is_the_named_noop(
    module_pg_schema: ModulePgSchema, module_pg_pool: Any, demo_env: Any
) -> None:
    """The cancel against a TERMINAL run: exit 0 with the no-op line (an
    honest 'nothing cancelled'), the audit trail untouched by the
    no-op."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("attack4b_hold_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    from taskq.workflows.api._hitl import HitlClient

    hc = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await hc.list(str(flow_id))
    await hc.resolve(hold.hold_id, {"verdict": "approve"})
    await runner.drive(flow_id)

    proc = _cli(module_pg_schema, "cancel", str(flow_id), demo_env=demo_env)
    assert proc.returncode == 0, f"the no-op cancel errored: {proc.stderr}"
    assert "no-op" in proc.stdout, f"the no-op is not named: {proc.stdout!r}"
    audits = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".admin_audit '
        "WHERE target_id = $1 AND action = 'workflow.cancel'",
        str(flow_id),
    )
    assert audits == 0, f"the no-op cancel wrote {audits} audit row(s)"


# ── THE TORN CANCEL (the structural probe) ───────────────────────────────


async def test_the_torn_cancel_the_signal_leg_rides_a_second_connection(
    module_pg_schema: ModulePgSchema, module_pg_pool: Any, demo_env: Any, monkeypatch: Any
) -> None:
    """THE STRUCTURAL PROBE: ``cancel_workflow_run`` opens ONE
    transaction, but its held-signal leg (``cancel_run_signals``) runs on
    a SECOND pool connection OUTSIDE that transaction. If the outer tx
    rolls back after the signal leg ran, the signals stay 'cancelled'
    while the run is NOT cancelled — the torn cancel. This probe forces
    the rollback (the audit write raises) and asserts the damage is
    VISIBLE (a red here is the finding; the fixer owns the cure)."""
    from taskq.workflows.api._hitl import HitlClient
    from taskq.workflows.api._runner_exit import cancel_workflow_run

    schema = module_pg_schema.schema_name
    run_id = await _held_run(module_pg_pool, module_pg_schema)
    hc = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await hc.list(run_id)
    assert (
        await module_pg_pool.fetchval(
            f'SELECT status FROM "{schema}".wf_signals WHERE id = $1', hold.hold_id
        )
        == "held"
    )

    # Force the OUTER tx to fail after the signal leg ran: the audit
    # writer raises (the admin_audit write is the last statement before
    # the tx's end). The exit arms import the writer lazily from the
    # audit module — the patch target is the SOURCE module.
    import taskq.web.admin._audit as audit_mod

    real_record = audit_mod.record_admin_action

    async def exploding_record(*a: Any, **k: Any) -> None:
        raise RuntimeError("the audit write fails — the tx rolls back")

    monkeypatch.setattr(audit_mod, "record_admin_action", exploding_record)
    try:
        with pytest.raises(RuntimeError):
            await cancel_workflow_run(module_pg_pool, schema=schema, flow_id=JobId(run_id))
    finally:
        monkeypatch.setattr(audit_mod, "record_admin_action", real_record)

    # THE TORN STATE: what did the rows land in?
    root = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', run_id
    )
    signal = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".wf_signals WHERE id = $1', hold.hold_id
    )
    # THE INVARIANT (the atomic cancel's contract): the run flip and the
    # signal leg land TOGETHER or NOT AT ALL. The second connection breaks
    # it: expect the signal 'cancelled' while the root is NOT cancelled.
    if signal == "cancelled" and root != "cancelled":
        pytest.fail(
            "THE TORN CANCEL, OBSERVED: the signal leg ran on its own "
            f"connection and SURVIVED the outer rollback (signal={signal!r}, "
            f"root={root!r}) — a held operator's decision is gone on a run "
            "that never cancelled"
        )
    # Either the cure exists (both rolled back) or the shape differs —
    # never a silent pass over the torn pair.
    assert (root == "cancelled") == (signal == "cancelled"), (
        f"the cancel is torn: root={root!r}, signal={signal!r}"
    )
