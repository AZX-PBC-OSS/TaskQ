# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""THE WF START_TO_CLOSE PINS — the step deadline's declaration + enforcement, red-first.

THE CONVICTION: the vanilla machinery EXISTS end to end — the ``jobs``
table's ``start_to_close interval`` column (``migrations/
01.00.00_01_pre_initial.sql``:85, nullable, no default), the enforcement
(``worker/_consumer.py``'s ``_enforce_start_to_close`` — the #791
TimeoutError-conflation cure, the tx-unwind budget, the hostile-finally
drills), and the resolution ``job.start_to_close if not None else
settings.default_start_to_close`` (_consumer.py:1180-1183). The WORKFLOW
runner's body execution (``FlowRunner.run_fleet_claimed_step`` →
``_execute_claimed`` → the body invocation) never called it: a claimed wf
step that hangs lives forever.

THE DESIGN (plumbing, not new machinery — #698's priority threading
pattern): ``start_to_close=`` (a timedelta) declares on ``@app.actor``,
``step()``, and ``RouteArm`` — the value STAMPS the row's EXISTING
column at the node INSERTs (the static rows, the fork's children + join,
and the drain's outbox-materialized consumer rows), no new column, no
migration; the runner's body invocation wraps in the SAME
``_enforce_start_to_close``; the expiry's disposition is the wf ladder's
failure arm (a START_TO_CLOSE TimeoutError is a REAL attempt burn —
never a Snooze), with the vanilla race semantics mirrored
verbatim:

* a body raising ``Snooze`` WITHIN the window: the deferral wins (the
  deadline never fires — the enforcement's own race, verified on the
  vanilla path);
* a body catching the deadline's cancellation and raising ``Snooze``:
  the DEADLINE wins (the body task is detached, its outcome discarded —
  a deferral can never win a race the clock already ended);
* a cancel mark on EITHER carrier (the wf cancel's ``metadata.
  cancel_phase`` or the vanilla cancel's ``cancel_phase`` COLUMN):
  the cancel wins — the deadline's re-pend arm is fenced in-statement
  (the vanilla ``mark_retry``'s own law: a phase-carrying row matches
  no arm), and the node terminalises 'cancelled' through the wf
  terminal door (the C4 deferral's cancel_fenced shape).

THE RESOLUTION LAW per row, first non-``None`` wins:
the step's/arm's own declaration > the actor's declaration (folded at
the wiring when the step declares none) > the row's NULL > the worker's
``default_start_to_close`` (the enforcement-time fallback — the
``_effective_start_to_close`` chain's own tail).

The vanilla surface is untouched: worker/ and backend/ carry no
behavioral diff (the enforcement's annotations generalize to the shared
ctx face; its body is byte-identical), and the no-declaration rows are
byte-identical (the body invoked INLINE — no task, no machinery).
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.exceptions import Snooze
from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)

pytestmark = pytest.mark.integration

MODULE_NAME = "wf_stc_demo_flows"

#: The pins' deadline bound: short enough that a hung body's tick drives
#: the expiry inside the pin's own patience, long enough that the claim
#: machinery's own scheduling noise never fires it early.
BOUND_S = 0.25

#: The hung body's sleep: far past the bound (the deadline must win) but
#: bounded — a detached unwind is tracked, never leaked.
HANG_S = 30.0

#: The runner default's bound (level 4 of the resolution law).
DEFAULT_BOUND_S = 0.3


class Ingest(BaseModel):
    doc_id: str


class Image(BaseModel):
    uri: str


class Audio(BaseModel):
    uri: str


async def _hang_body(ctx: StepContext, params: Ingest) -> str:
    await asyncio.sleep(HANG_S)
    return "never"


async def _ok_body(ctx: StepContext, params: Ingest) -> str:
    await asyncio.sleep(0.02)
    return "done"


async def _list_source(ctx: StepContext, params: Ingest) -> list[str]:
    return ["a", "b"]


async def _union_source(ctx: StepContext, params: Ingest) -> list[Image | Audio]:
    return [Image(uri="img://1"), Audio(uri="aud://1")]


async def _item_body(ctx: StepContext, item: str) -> str:
    await asyncio.sleep(0.02)
    return item


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The registered definitions (the compile's own door — the bodies
    resolve from the D1 registry, never a per-call map)."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("hang_flow_declared")
    def hang_flow_declared() -> Promise[object]:
        return build(
            step(
                _hang_body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=2,
                start_to_close=timedelta(seconds=BOUND_S),
            )
        )

    @app_obj.workflow("ok_flow")
    def ok_flow() -> Promise[object]:
        return build(
            step(
                _ok_body,
                Ingest(doc_id="d1"),
                key="solo",
                start_to_close=timedelta(seconds=30),
            )
        )

    # THE NAME IS THE FILE'S OWN (the D1 registry is process-global — a
    # name another pin file registers is the DuplicateWorkflowError
    # order-dependent flake; this file's map flow carries the stc prefix).
    @app_obj.workflow("stc_map_children_flow")
    def stc_map_children_flow() -> Promise[object]:
        source = step(
            _list_source,
            Ingest(doc_id="d1"),
            key="src",
            start_to_close=timedelta(seconds=17),
        )
        mapped = map_source(source, _item_body)
        return build(mapped)

    @app_obj.workflow("route_arms_flow")
    def route_arms_flow() -> Promise[object]:
        async def _image_arm(ctx: StepContext, item: Image) -> str:
            return f"img:{item.uri}"

        async def _audio_arm(ctx: StepContext, item: Audio) -> str:
            return f"aud:{item.uri}"

        source = step(
            _union_source,
            Ingest(doc_id="d1"),
            key="src",
            start_to_close=timedelta(seconds=11),
        )
        routed = map_source(
            source,
            {Image: RouteArm(_image_arm, start_to_close=timedelta(seconds=23)), Audio: _audio_arm},
        )
        return build(routed)

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


async def _node_row(pool: asyncpg.Pool, schema: str, flow_id: str, step_key: str) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"SELECT id, status, attempt, error_class, error_message, "
        f"start_to_close, scheduled_at, result "
        f'FROM "{schema}".jobs '
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2 "
        "AND step_key <> '__flow__'",
        uuid.UUID(flow_id),
        step_key,
    )
    assert row is not None, f"the node row {step_key!r} must exist"
    return dict(row)


async def _ledger_rows(
    pool: asyncpg.Pool, schema: str, flow_id: str, step_key: str
) -> list[asyncpg.Record]:
    return await pool.fetch(
        f'SELECT attempt, status, error_class, error_message FROM "{schema}".wf_step_ledger '
        f"WHERE flow_id = $1 AND step_key = $2 ORDER BY attempt",
        uuid.UUID(flow_id),
        step_key,
    )


async def _root_status(pool: asyncpg.Pool, schema: str, flow_id: str) -> str:
    return await pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(flow_id)
    )


async def _drive_until(
    pool: asyncpg.Pool, schema: str, runner: Any, flow_id: str, want: str
) -> None:
    """Tick until the flow's status reads *want* (bounded — a hang is a
    defect with a name)."""
    for _ in range(200):
        status = await _root_status(pool, schema, flow_id)
        if status == want:
            return
        await runner.tick(flow_id)
        await asyncio.sleep(0.05)
    status = await _root_status(pool, schema, flow_id)
    pytest.fail(f"the flow never reached {want!r} (status {status!r})")


# ── (1) the headline: the hung step's deadline fires, the attempt burns,
#        the retry per the contract, the run alive ──────────────────────────


async def test_hung_step_past_its_bound_burns_the_attempt_and_retries(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE HEADLINE PIN: a claimed wf step hung past its declared bound —
    the deadline fires, the attempt BURNS (the ledger's failed row, the
    claim's increment kept), the ladder re-pends per the contract, and
    the RUN STAYS ALIVE (the root 'running' — never a zombie, never a
    dead run)."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("hang_flow_declared")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)  # the claim + the hung body + the expiry
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    # THE DISPOSITION: the attempt burned — the row re-pended for the
    # ladder's next attempt (the vanilla timeout arm's own shape: the
    # decision's retry, the row 'scheduled' at the backoff).
    assert row["status"] == "scheduled", (
        f"the deadline's re-pend lands the row 'scheduled', got {row['status']!r}"
    )
    assert row["attempt"] == 1, (
        f"the deadline is a REAL attempt burn — the claim's increment stands, got {row['attempt']}"
    )
    # THE RECORD: the row carries NO terminal stamp (the ladder's own
    # re-pend face — the attempt is not terminal); the attempt's failed
    # LEDGER row is the record, with the truthful class (the vanilla
    # rows' own class for the deadline — the private marker's name never
    # reaches a record) and the handler's own fallback message.
    assert row["error_class"] is None, row["error_class"]
    ledger = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert [r["status"] for r in ledger] == ["failed"], ledger
    assert ledger[0]["error_class"] == "TimeoutError", ledger[0]
    assert ledger[0]["error_message"] == "start_to_close", ledger[0]
    # THE RUN IS ALIVE.
    assert await _root_status(module_pg_pool, schema, flow_id) == "running"

    # THE RETRY PER THE CONTRACT: the ladder re-claims (the body hangs
    # again) and the second burn exhausts max_attempts=2 — the terminal
    # 'failed' (the deadline's stamp on the row's record), the root
    # failed with it.
    await _drive_until(module_pg_pool, schema, runner, flow_id, "failed")
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "failed", row["status"]
    assert row["attempt"] == 2, row["attempt"]
    assert row["error_class"] == "TimeoutError", row["error_class"]
    assert row["error_message"] == "start_to_close", row["error_message"]
    ledger = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert [r["status"] for r in ledger] == ["failed", "failed"], ledger


async def test_step_within_its_window_is_untouched(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """A step that finishes WITHIN its declared window: the deadline is
    cancelled, the attempt succeeds, nothing ladders."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("ok_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    await _drive_until(module_pg_pool, schema, runner, flow_id, "succeeded")
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "succeeded", row["status"]
    assert row["result"] is not None
    ledger = await _ledger_rows(module_pg_pool, schema, flow_id, "solo")
    assert [r["status"] for r in ledger] == ["succeeded"], ledger


async def test_no_declaration_runs_the_body_inline_byte_identical(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE CONTROL (the byte-identical law): a step with NO declaration
    anywhere — the row's column NULL, the body invoked INLINE (no
    enforcement task, no machinery, no deadline handle): the ctx's
    body-task handle stays unset, the pre-cure execution shape
    exactly."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()
    seen: dict[str, Any] = {}

    @app_obj.workflow("stc_control_flow")
    def stc_control() -> Promise[object]:
        async def _inline_probe(ctx: StepContext, params: Ingest) -> str:
            seen["ran"] = True
            seen["body_task"] = ctx._actor_body_task
            return "done"

        return build(step(_inline_probe, Ingest(doc_id="d1"), key="solo"))

    runner = FlowRunner(app_obj.get("stc_control_flow"), module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    # THE ROW: the unset declaration stamps NULL — the column default's
    # own value (the pre-cure row).
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["start_to_close"] is None, row["start_to_close"]

    await _drive_until(module_pg_pool, schema, runner, flow_id, "succeeded")
    assert seen["ran"], "the probe body never ran"
    assert seen["body_task"] is None, (
        f"the no-declaration body must run INLINE (no enforcement task "
        f"on the ctx), got {seen['body_task']!r}"
    )


# ── (1b) the fleet door: the same deadline through the REAL dispatch ─────


async def test_the_fleet_door_enforces_the_row_deadline(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """THE FLEET FACE (production's own path — the priority pins' own
    rig): the step's declared bound stamps the row at create; the REAL
    dispatch claims it; the worker-hosted door
    (``execute_flow_job``) runs the body through the SAME enforcement —
    the hung step's deadline fires on the fleet worker too (the row's
    own stamp outranking the threaded worker default, and the worker's
    default arming an UNDECLARED row)."""
    from taskq._ids import new_uuid
    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
    from taskq.backend._records import _job_row_from_record
    from taskq.workflows._worker_execution import (
        execute_flow_job,
        project_workflow_actor_configs,
    )

    app_obj = WorkflowApp()

    @app_obj.workflow("stc_fleet_flow")
    def stc_fleet() -> Promise[object]:
        return build(
            step(
                _hang_body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=2,
                start_to_close=timedelta(seconds=BOUND_S),
            )
        )

    for config in project_workflow_actor_configs():
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )
    runner = FlowRunner(app_obj.get("stc_fleet_flow"), module_pg_pool, wf_schema)
    flow_id = JobId((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'stc-fleet-pin', 1, '{default}', $2::jsonb)",
        worker_id,
        json.dumps({"workflow_execution": True}),
    )
    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert len(dispatched) == 1, dispatched

    # THE FLEET EXECUTION: the hung body, the deadline fires — the
    # outcome label the ladder's own ("laddered"), the row re-pended,
    # the ledger's failed row the deadline's record.
    execution = await execute_flow_job(
        pool=module_pg_pool,
        schema=wf_schema,
        worker_id=JobId(worker_id),
        job=_job_row_from_record(dispatched[0]),
        default_start_to_close=timedelta(seconds=300),
    )
    assert execution.outcome == "laddered", execution.outcome
    row = await _node_row(module_pg_pool, wf_schema, str(flow_id), "solo")
    assert row["status"] == "scheduled", row["status"]
    assert row["attempt"] == 1, row["attempt"]
    ledger = await _ledger_rows(module_pg_pool, wf_schema, str(flow_id), "solo")
    assert ledger and ledger[0]["error_class"] == "TimeoutError", ledger


# ── (2) the resolution law's four levels ─────────────────────────────────


async def test_the_resolution_law_the_row_levels(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """THE RESOLUTION LAW per row, first non-``None`` wins:
    the step's declaration > the actor's declaration (folded at the
    wiring) > the row's NULL — the stamps asserted on the rows."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()

    @app_obj.actor(start_to_close=timedelta(seconds=60))
    async def _actor_declared(ctx: StepContext, params: Ingest) -> str:
        return "done"

    async def _plain(ctx: StepContext, params: Ingest) -> str:
        return "done"

    @app_obj.workflow("stc_resolution_flow")
    def stc_resolution() -> Promise[object]:
        a = step(_actor_declared, Ingest(doc_id="d1"), key="actor_only")
        b = step(
            _actor_declared,
            Ingest(doc_id="d1"),
            key="step_wins",
            start_to_close=timedelta(seconds=90),
        )
        c = step(_plain, Ingest(doc_id="d1"), key="undeclared")
        return build(a, b, c)

    runner = FlowRunner(app_obj.get("stc_resolution_flow"), module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    # LEVEL 2 (the actor's declaration, folded at the wiring): the
    # handle's 60s is the row's stamp when the step declares none.
    row = await _node_row(module_pg_pool, schema, flow_id, "actor_only")
    assert row["start_to_close"] == timedelta(seconds=60), row["start_to_close"]
    # LEVEL 1 (the step's own): 90s overrides the handle's 60s.
    row = await _node_row(module_pg_pool, schema, flow_id, "step_wins")
    assert row["start_to_close"] == timedelta(seconds=90), row["start_to_close"]
    # LEVEL 3: nothing declared — the row's column NULL (the worker's
    # default applies at the ENFORCEMENT, never on the row).
    row = await _node_row(module_pg_pool, schema, flow_id, "undeclared")
    assert row["start_to_close"] is None, row["start_to_close"]


async def test_the_row_null_falls_to_the_worker_default_at_the_enforcement(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """LEVEL 4 (the worker's default): an undeclared step under a runner
    carrying the worker's settings value — a hung body dies at the
    DEFAULT's bound, never unbounded. The default never stamps the row
    (the row's NULL is the declaration's own truth)."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_default_face_flow")
    def stc_default_face() -> Promise[object]:
        return build(
            step(
                _hang_body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=1,
            )
        )

    runner = FlowRunner(
        app_obj.get("stc_default_face_flow"),
        module_pg_pool,
        schema,
        default_start_to_close=timedelta(seconds=DEFAULT_BOUND_S),
    )
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["start_to_close"] is None, "the default never stamps the row"
    assert await runner.tick(flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "failed", (
        f"the runner's default_start_to_close arms the enforcement (the "
        f"worker's own fallback face), got {row['status']!r}"
    )
    assert row["error_class"] == "TimeoutError", row["error_class"]
    assert row["error_message"] == "start_to_close", row["error_message"]


async def test_the_row_stamp_beats_the_runner_default(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """THE LAW'S ORDER AT THE ENFORCEMENT: the row's own stamp outranks
    the runner's default (the vanilla ``_effective_start_to_close``
    chain: ``job.start_to_close if not None else default``) — a step
    declaring 0.25s under a runner defaulted to 300s dies at 0.25s."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_row_beats_default_flow")
    def stc_row_beats_default() -> Promise[object]:
        return build(
            step(
                _hang_body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=1,
                start_to_close=timedelta(seconds=BOUND_S),
            )
        )

    runner = FlowRunner(
        app_obj.get("stc_row_beats_default_flow"),
        module_pg_pool,
        schema,
        default_start_to_close=timedelta(seconds=300),
    )
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)
    started = asyncio.get_running_loop().time()
    assert await runner.tick(flow_id)
    elapsed = asyncio.get_running_loop().time() - started
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "failed", row["status"]
    assert elapsed < 10.0, (
        f"the ROW's stamp governed the enforcement (the deadline at "
        f"{BOUND_S}s), not the runner's 300s default — took {elapsed:.2f}s"
    )


# ── (3) the fork's rows: the map children + the route arms + the join ────


async def test_map_children_carry_the_source_step_declaration(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """The fork's children (FORK_CHILDREN_SQL) stamp the SOURCE step's
    resolution — the children share the source's cohort (the priority
    cure's own arithmetic)."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("stc_map_children_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = JobId((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    await runner.tick(flow_id)  # the source runs + finalizes with the fork
    rows = await module_pg_pool.fetch(
        f'SELECT step_key, start_to_close FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        flow_id,
    )
    by_key = {row["step_key"]: row["start_to_close"] for row in rows}
    assert by_key["src"] == timedelta(seconds=17), by_key
    item_rows = {k: v for k, v in by_key.items() if k.startswith("src.item")}
    assert item_rows, f"no forked children found: {sorted(by_key)}"
    assert all(v == timedelta(seconds=17) for v in item_rows.values()), by_key


async def test_route_arm_declaration_overrides_down_and_the_join_stays_the_sources(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE ARM'S DOWN-OVERRIDE (the priority cure's own shape): the arm's
    children land at the ARM's declaration; an arm declaring none keeps
    the source's; the JOIN (the source's own cohort) stays the source's
    resolution."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("route_arms_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = JobId((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    await runner.tick(flow_id)
    rows = await module_pg_pool.fetch(
        f'SELECT step_key, start_to_close FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        flow_id,
    )
    by_key = {row["step_key"]: row["start_to_close"] for row in rows}
    assert by_key["src"] == timedelta(seconds=11), by_key
    assert by_key["src.join"] == timedelta(seconds=11), (
        f"the JOIN stays the source's resolution, got {by_key}"
    )
    arm_a = {k: v for k, v in by_key.items() if k.startswith("src.item:")}
    assert arm_a, f"no forked arm children found: {sorted(by_key)}"
    # THE ARM'S OWN DECLARATION (the Image arm's 23s) vs the bare arm's
    # inheritance (the Audio arm: the source's 11s). The child keys name
    # their arm's type tag.
    declared = [v for v in arm_a.values() if v == timedelta(seconds=23)]
    inherited = [v for v in arm_a.values() if v == timedelta(seconds=11)]
    assert len(declared) == 1, f"the Image arm's child carries the arm's 23s, got {arm_a}"
    assert len(inherited) == 1, f"the bare Audio arm inherits the source's 11s, got {arm_a}"


# ── (4) the drain's consumer rows inherit the declared bound ─────────────


async def test_the_drain_materializes_consumers_at_the_declared_bound(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
) -> None:
    """THE DRAIN'S LEG: an outbox-materialized consumer row inherits the
    declared bound the way priority's consumer rows inherit the run's
    stamp — the fire's bindings carry it, the drain's consumer INSERT
    stamps the ``jobs.start_to_close`` column. The unset case binds
    NULL (the pre-cure row)."""
    from tests._wf_fixtures import seed_flow

    flow_id = await seed_flow(wf_conn, wf_schema)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_outbox '
        "(id, join_job_id, flow_id, consumer_step_key, bindings) "
        "VALUES ($1, $2, $3, 'stc_drained_consumer', $4::jsonb)",
        new_uuid(),
        new_uuid(),
        flow_id,
        json.dumps({"actor": "wf", "queue": "default", "payload": {}, "start_to_close_s": 42.0}),
    )
    from taskq.workflows._sweep import drain_outbox

    delivered = await drain_outbox(module_pg_pool, wf_sql)
    assert delivered == 1, delivered
    consumer = await wf_conn.fetchrow(
        f'SELECT start_to_close FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'stc_drained_consumer'",
        flow_id,
    )
    assert consumer is not None, "the drain inserted no consumer row"
    assert consumer["start_to_close"] == timedelta(seconds=42), consumer["start_to_close"]

    # THE UNSET CASE: no binding value — the consumer row's column NULL.
    flow2 = await seed_flow(wf_conn, wf_schema)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_outbox '
        "(id, join_job_id, flow_id, consumer_step_key, bindings) "
        "VALUES ($1, $2, $3, 'stc_drained_consumer_unset', $4::jsonb)",
        new_uuid(),
        new_uuid(),
        flow2,
        json.dumps({"actor": "wf", "queue": "default", "payload": {}}),
    )
    await drain_outbox(module_pg_pool, wf_sql)
    consumer2 = await wf_conn.fetchrow(
        f'SELECT start_to_close FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'stc_drained_consumer_unset'",
        flow2,
    )
    assert consumer2 is not None
    assert consumer2["start_to_close"] is None, consumer2["start_to_close"]


async def test_the_fork_bindings_carry_the_downstream_resolution(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    demo_app_module: Any,
) -> None:
    """THE BINDING'S CARRIER: the fork's join-row metadata ``consumers``
    array carries the downstream node's own resolution (serialized as
    the binding's seconds) — the fire copies it into the outbox
    bindings, the drain stamps the fresh-materialized consumer row. The
    undeclared downstream keeps the pre-cure shape (no key)."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_binding_flow")
    def stc_binding() -> Promise[object]:
        async def _downstream(ctx: StepContext, items: list[str]) -> str:
            return ",".join(items)

        source = step(_list_source, Ingest(doc_id="d1"), key="src")
        mapped = map_source(source, _item_body)
        return build(step(_downstream, mapped, key="down", start_to_close=timedelta(seconds=33)))

    compiled = app_obj.get("stc_binding_flow")
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = JobId((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)
    await runner.tick(flow_id)  # the source's fork stamps the join's bindings

    join_meta = await module_pg_pool.fetchval(
        f"SELECT metadata FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'src.join'",
        flow_id,
    )
    assert join_meta is not None, "the map fork's join row must exist"
    decoded = json.loads(join_meta) if isinstance(join_meta, str) else join_meta
    consumers = decoded.get("consumers") or []
    assert consumers, f"the join carries no consumer bindings: {decoded}"
    (binding,) = consumers
    assert binding["step_key"] == "down", binding
    assert binding.get("start_to_close_s") == 33.0, (
        f"the binding carries the downstream node's own resolution, got {binding}"
    )


# ── (5) the deadline-cancel → body-Snooze interop (the vanilla race,
#        mirrored) ─────────────────────────────────────────────────────────


async def test_snooze_within_the_window_defers(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """A body raising ``Snooze`` WITHIN the window: the DEFERRAL wins —
    the vanilla enforcement's own race (verified on the vanilla path:
    the body task completes first, its Snooze propagates, the deadline
    handle is cancelled). The node re-schedules, the budget unburned."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_snooze_within_flow")
    def stc_snooze_within() -> Promise[object]:
        async def _body(ctx: StepContext, params: Ingest) -> str:
            raise Snooze(timedelta(seconds=0.5))

        return build(
            step(
                _body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=1,
                start_to_close=timedelta(seconds=30),
            )
        )

    runner = FlowRunner(app_obj.get("stc_snooze_within_flow"), module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "scheduled", (
        f"the Snooze's deferral must win a race inside the window, got {row['status']!r}"
    )
    assert row["attempt"] == 0, (
        f"the deferral refunds the claim's increment (the C4 law), got {row['attempt']}"
    )
    assert row["error_class"] is None, row["error_class"]


async def test_body_converting_the_deadline_cancel_to_snooze_loses_the_race(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """A body that catches the deadline's cancellation and raises
    ``Snooze``: the DEADLINE wins — the vanilla enforcement's own race
    (verified: once the clock fired, the body task is detached and its
    outcome discarded; a deferral can never win a race the clock already
    ended). The attempt BURNS, never a deferral."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_snooze_race_flow")
    def stc_snooze_race() -> Promise[object]:
        async def _body(ctx: StepContext, params: Ingest) -> str:
            try:
                await asyncio.sleep(HANG_S)
            except asyncio.CancelledError:
                raise Snooze(timedelta(seconds=5)) from None
            return "never"

        return build(
            step(
                _body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=1,
                start_to_close=timedelta(seconds=BOUND_S),
            )
        )

    runner = FlowRunner(app_obj.get("stc_snooze_race_flow"), module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    assert await runner.tick(flow_id)
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "failed", (
        f"the deadline's win survives the body's Snooze conversion (the "
        f"vanilla race, mirrored), got {row['status']!r}"
    )
    assert row["error_class"] == "TimeoutError", row["error_class"]
    assert row["attempt"] == 1, "the burn stands — a Snooze never refunded it"


# ── (6) the cancel interop: the deadline + a cancel mark → the cancel wins


async def test_the_deadline_fences_a_cancel_mark_and_the_cancel_wins(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """THE CANCEL WINS (the vanilla ``mark_retry``'s own law — a
    phase-carrying row matches no arm; the C4 deferral's cancel_fenced
    shape): a claimed step hung past its bound, the operator's cancel
    mark (the wf cancel's metadata carrier) landing mid-flight — the
    deadline's disposition FENCES the mark in-statement and the node
    terminalises 'cancelled' through the wf terminal door (the cancel's
    vocabulary, never the deadline's), the ledger row the cancel's
    record."""
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_cancel_fence_flow")
    def stc_cancel_fence() -> Promise[object]:
        return build(
            step(
                _hang_body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=3,
                start_to_close=timedelta(seconds=BOUND_S),
            )
        )

    compiled = app_obj.get("stc_cancel_fence_flow")
    runner = FlowRunner(compiled, module_pg_pool, wf_schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)
    node_id = await module_pg_pool.fetchval(
        f"SELECT id FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'solo'",
        uuid.UUID(flow_id),
    )
    assert node_id is not None

    # THE CLAIM, then the cancel mark lands mid-flight (the wf cancel's
    # cooperative phase-1 mark on the running row).
    task = asyncio.create_task(runner.tick(flow_id))
    await asyncio.sleep(0.05)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || \'{{"cancel_phase": "cooperative"}}\'::jsonb '
        "WHERE id = $1",
        node_id,
    )
    await task
    row = await _node_row(module_pg_pool, wf_schema, flow_id, "solo")
    assert row["status"] == "cancelled", (
        f"the cancel outranks the deadline (the cancel_fenced law), got {row['status']!r}"
    )
    assert row["error_class"] == "WorkflowCancelled", row["error_class"]
    ledger = await _ledger_rows(module_pg_pool, wf_schema, flow_id, "solo")
    assert ledger and ledger[-1]["status"] == "cancelled", ledger


# ── (7) the hostile-finally face (the vanilla drill's shape, wf face) ────


async def test_hostile_finally_cannot_defer_the_wf_deadline(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
) -> None:
    """THE HOSTILE UNWIND (the vanilla drill's shape 3 — the deferrable
    deadline): a body whose ``finally`` sleeps past the deadline cannot
    hold the attempt hostage — the attempt ends AT the deadline, the
    disposition lands (the re-pend), and the body unwinds DETACHED
    (tracked — never leaked, never blocking the ladder)."""
    schema = module_pg_schema.schema_name
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_hostile_finally_flow")
    def stc_hostile_finally() -> Promise[object]:
        async def _body(ctx: StepContext, params: Ingest) -> str:
            try:
                await asyncio.sleep(HANG_S)
            finally:
                await asyncio.sleep(2.0)  # the hostile unwind
            return "never"

        return build(
            step(
                _body,
                Ingest(doc_id="d1"),
                key="solo",
                max_attempts=1,
                start_to_close=timedelta(seconds=BOUND_S),
            )
        )

    runner = FlowRunner(app_obj.get("stc_hostile_finally_flow"), module_pg_pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)

    started = asyncio.get_running_loop().time()
    assert await runner.tick(flow_id)
    elapsed = asyncio.get_running_loop().time() - started
    row = await _node_row(module_pg_pool, schema, flow_id, "solo")
    assert row["status"] == "failed", (
        f"the deadline's win survives the hostile finally, got {row['status']!r}"
    )
    assert row["error_class"] == "TimeoutError", row["error_class"]
    assert elapsed < 2.0, (
        f"the attempt must end AT the deadline (the hostile finally "
        f"unwinds detached), took {elapsed:.2f}s"
    )
    assert await _root_status(module_pg_pool, schema, flow_id) == "failed"
    # THE UNWINDING ZOMBIE IS ACCOUNTED FOR (the vanilla drill's own
    # assertion — the shutdown watchdog can see it): tracked, not leaked.
    from taskq.worker._watchdog import live_tracked_actor_handles

    zombies = [z for z in live_tracked_actor_handles() if not z.done()]
    assert zombies, "a hostile unwind must stay tracked"
    # Teardown: reap the detached unwinds so the test loop closes clean.
    for z in zombies:
        z.cancel()
    await asyncio.gather(*zombies, return_exceptions=True)


# ── (8) the declaration-site refusals (the vanilla law's own shape) ──────


def test_the_declaration_refuses_a_nonpositive_bound() -> None:
    """THE VANILLA LAW (``start_to_close must be > 0`` — the @actor
    decorator's own refusal, at the resolution chain's every declaration
    site): zero and negative bounds are refused at the declaration,
    before anything registers. The step's and the arm's sites refuse at
    the compile (the wiring verbs run inside the build fn — the
    priority pins' own shape)."""
    app_obj = WorkflowApp()

    async def _body(ctx: StepContext, params: Ingest) -> str:
        return "done"

    with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
        app_obj.actor(start_to_close=timedelta(seconds=-1))(_body)
    with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
        app_obj.actor(start_to_close=timedelta(seconds=0))(_body)

    @app_obj.workflow("stc-refused-step-flow")
    def stc_refused_step() -> Promise[object]:
        return build(step(_body, Ingest(doc_id="d"), key="x", start_to_close=timedelta(seconds=-5)))

    with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
        app_obj.get("stc-refused-step-flow")

    @app_obj.workflow("stc-refused-arm-flow")
    def stc_refused_arm() -> Promise[object]:
        src = step(_list_source, Ingest(doc_id="d"), key="src")
        routed = map_source(
            src,
            {Image: RouteArm(body=_item_body, start_to_close=timedelta(seconds=0))},
        )
        return build(routed)

    with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
        app_obj.get("stc-refused-arm-flow")


def test_the_declaration_refuses_a_non_timedelta() -> None:
    """THE DECLARED TYPE (a timedelta — the vanilla ``@actor``'s own
    face): a bare number is the refused declaration, never a
    seconds-silent-coercion."""
    app_obj = WorkflowApp()

    async def _body(ctx: StepContext, params: Ingest) -> str:
        return "done"

    with pytest.raises(ValueError, match=r"start_to_close must be a timedelta"):
        app_obj.actor(start_to_close=30)(_body)  # type: ignore[arg-type]

    @app_obj.workflow("stc-refused-step-type-flow")
    def stc_refused_step_type() -> Promise[object]:
        return build(step(_body, Ingest(doc_id="d"), key="x", start_to_close=30))  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=r"start_to_close must be a timedelta"):
        app_obj.get("stc-refused-step-type-flow")

    @app_obj.workflow("stc-refused-arm-type-flow")
    def stc_refused_arm_type() -> Promise[object]:
        src = step(_list_source, Ingest(doc_id="d"), key="src")
        routed = map_source(src, {Image: RouteArm(body=_item_body, start_to_close=30)})  # type: ignore[arg-type]
        return build(routed)

    with pytest.raises(ValueError, match=r"start_to_close must be a timedelta"):
        app_obj.get("stc-refused-arm-type-flow")


# ── (9) THE RE-REVIEW'S HIGH CONVICTION: the deadline's claim-identity
#        fence (the zombie W1's disposition must never flip a re-minted
#        LIVE W2 row) ─────────────────────────────────────────────────────


async def test_the_zombie_deadline_cannot_flip_a_live_reclaimed_row(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE ATTACKER'S DRILL A (the reviewer's live repro): W1 claims the
    node (the ledger row live at its own instant); the 90s wf lease
    LAPSES; the reclaim re-pends the row; W2 re-claims (the attempt
    walks, W2's ledger claim live); W1's timer FIRES. The deadline's
    disposition must carry THE CLAIM IDENTITY'S FENCE (the
    NODE_DEFER_SQL's own legs — status + worker + attempt + epoch): W1's
    zombie disposition matches NOTHING on W2's live row — the row stays
    RUNNING (never flipped 'scheduled', never a phantom-runs ledger,
    W2's finalize never refuses) — and W1's own attempt takes the ZOMBIE
    face (the ledger's 'fenced' record, keyed on the claim's own
    instant — the C5 shape). The pre-cure shape: the unfenced WHERE (id +
    status) flipped W2's row out from under it."""
    from tests._wf_fixtures import seed_flow, seed_running_node

    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("hang_flow_declared")  # type: ignore[attr-defined]
    flow_id = await seed_flow(clean_pg_conn, schema, status="running")
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="solo")
    # W1 = the seeded claim's own identity (the runner binds it).
    seeded = await clean_pg_conn.fetchrow(
        f'SELECT locked_by_worker, attempt, claim_epoch FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert seeded is not None and seeded["locked_by_worker"] is not None
    w1 = JobId(seeded["locked_by_worker"])
    runner = FlowRunner(compiled, module_pg_pool, schema, worker_id=w1)
    from taskq.workflows.api._sql_runner import NODE_CLAIM_SQL_TEMPLATE, render_sql
    from taskq.workflows.ledger import claim_step_ledger

    # W1's ledger claim (the row's own stamp: the claim's RETURNING
    # instant — the C5 discriminator).
    w1_claim = await claim_step_ledger(
        clean_pg_conn,
        runner.wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="solo",
        map_index=None,
        attempt=int(seeded["attempt"]),
    )
    assert w1_claim.claimed_at is not None
    # THE LAPSE + THE RECLAIM: the row re-pends (the reclaim's shape).
    await clean_pg_conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'pending', locked_by_worker = NULL, "
        "lock_expires_at = NULL WHERE id = $1",
        node_id,
    )
    # THE RE-CLAIM AS W2 (the claim CAS's own grant of work — the attempt
    # walks to 2): W2 is LIVE on the row.
    w2 = JobId(new_uuid())
    w2_attempt = await clean_pg_conn.fetchval(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, schema), node_id, w2
    )
    assert w2_attempt is not None, "W2's re-claim must grant the work"
    # W2's ledger claim (the LIVE claimant's own row).
    await claim_step_ledger(
        clean_pg_conn,
        runner.wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="solo",
        map_index=None,
        attempt=int(w2_attempt),
    )

    # W1'S TIMER FIRES (the zombie's deadline disposition, its own binds).
    node = compiled.nodes.get("solo")
    label = await runner._deadline_or_fail(  # pyright: ignore[reportPrivateUsage]  # Why: the pins drive the ladder's own disposition seam.
        flow_id,
        {"id": node_id, "step_key": "solo", "map_index": None},
        int(seeded["attempt"]),
        node,
        claim_epoch=int(seeded["claim_epoch"]),
        ledger_id=w1_claim.ledger_id,
        ledger_claimed_at=w1_claim.claimed_at,
    )
    # THE ZOMBIE FACE: no arm matched — the disposition says so.
    assert label == "fenced", (
        f"the zombie's deadline must take the zombie face (the claim "
        f"identity's fence refuses the live row), got {label!r}"
    )
    # W2'S ROW STAYS LIVE (never flipped 'scheduled' — the phantom-runs
    # shape is dead).
    row = await _node_row(module_pg_pool, schema, str(flow_id), "solo")
    assert row["status"] == "running", (
        f"THE CLAIM IDENTITY'S FENCE: the zombie W1's deadline disposition "
        f"must never flip the re-claimed LIVE W2 row, got {row['status']!r}"
    )
    assert row["attempt"] == int(w2_attempt), (
        f"the live row's attempt is W2's own (never walked by the zombie), got {row['attempt']}"
    )
    # THE RECORDS: W1's own attempt records 'fenced' (H1, keyed on the
    # claim's own instant); W2's live ledger row untouched ('running').
    ledger = await _ledger_rows(module_pg_pool, schema, str(flow_id), "solo")
    by_attempt = {r["attempt"]: r for r in ledger}
    assert by_attempt[int(seeded["attempt"])]["status"] == "fenced", dict(
        by_attempt[int(seeded["attempt"])]
    )
    assert by_attempt[int(w2_attempt)]["status"] == "running", (
        "W2's live ledger claim must be untouched by the zombie's disposition"
    )


async def test_the_deadlines_burn_cannot_stamp_a_reminted_live_row(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE BURN'S GUARD (the re-mint rig): the refund cycle re-mints the
    ordinal — the arbiter re-use hands a LIVE winner the SAME ledger row
    a zombie once claimed (the row's instant moves to the winner). The
    zombie's deadline disposition — the fence legs refuse at the node
    row, the zombie face's fence-by-id keys the STALE instant — can
    never mark the winner's row: no 'failed' stamp, no 'fenced' stamp;
    the winner's ledger row survives live (the burn's own
    LEDGER_TERMINAL_BY_ID re-mint guard: the failed stamp cannot land on
    a re-minted live winner's ledger row)."""
    from tests._wf_fixtures import seed_flow, seed_running_node

    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("hang_flow_declared")  # type: ignore[attr-defined]
    flow_id = await seed_flow(clean_pg_conn, schema, status="running")
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="solo")
    seeded = await clean_pg_conn.fetchrow(
        f'SELECT locked_by_worker, attempt, claim_epoch FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert seeded is not None and seeded["locked_by_worker"] is not None
    w1 = JobId(seeded["locked_by_worker"])
    runner = FlowRunner(compiled, module_pg_pool, schema, worker_id=w1)
    from taskq.workflows.ledger import claim_step_ledger

    # W1's ledger claim — then THE RE-MINT (the refund cycle's own face:
    # a later claimant re-claims the SAME ordinal, the arbiter re-use
    # hands the SAME row to the LIVE winner W2, the instant re-stamps).
    w1_claim = await claim_step_ledger(
        clean_pg_conn,
        runner.wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="solo",
        map_index=None,
        attempt=int(seeded["attempt"]),
    )
    assert w1_claim.claimed_at is not None
    winner_claim = await claim_step_ledger(
        clean_pg_conn,
        runner.wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="solo",
        map_index=None,
        attempt=int(seeded["attempt"]),
    )
    assert winner_claim.ledger_id == w1_claim.ledger_id, (
        "the re-mint re-uses the SAME arbiter row — the winner is LIVE on the zombie's row"
    )
    assert winner_claim.claimed_at is not None
    assert w1_claim.claimed_at is not None
    assert winner_claim.claimed_at > w1_claim.claimed_at, (
        "the re-mint re-stamped the claim instant (the discriminator moved to the winner)"
    )
    # THE WINNER W2 IS LIVE on the row at ITS OWN identity (the re-mint's
    # own shape: the node row carries W2's claim).
    w2 = JobId(new_uuid())
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET locked_by_worker = $2, claim_epoch = 7, '
        "lock_expires_at = now() + interval '90 seconds' WHERE id = $1",
        node_id,
        w2,
    )

    # W1'S TIMER FIRES (the zombie's deadline disposition).
    node = compiled.nodes.get("solo")
    label = await runner._deadline_or_fail(  # pyright: ignore[reportPrivateUsage]
        flow_id,
        {"id": node_id, "step_key": "solo", "map_index": None},
        int(seeded["attempt"]),
        node,
        claim_epoch=int(seeded["claim_epoch"]),
        ledger_id=w1_claim.ledger_id,
        ledger_claimed_at=w1_claim.claimed_at,
    )
    assert label == "fenced"
    # THE WINNER'S ROW STAYS LIVE at BOTH faces: the node row never
    # flipped, the re-minted ledger row never stamped (no 'failed' burn,
    # no 'fenced' record — the stale instant refuses both).
    row = await _node_row(module_pg_pool, schema, str(flow_id), "solo")
    assert row["status"] == "running", (
        f"the zombie's deadline must never flip the winner's live row, got {row['status']!r}"
    )
    ledger = await _ledger_rows(module_pg_pool, schema, str(flow_id), "solo")
    assert len(ledger) == 1, "the re-mint never grew the arbiter"
    assert ledger[0]["status"] == "running", (
        f"THE BURN'S GUARD: a re-minted live winner's ledger row must "
        f"survive the zombie's disposition (no failed stamp, no fenced "
        f"record), got {ledger[0]['status']!r}"
    )
