"""THE WF-PRIORITY PINS — the workflow surface's priority law.

THE CONVICTION (the deepest downstream consumer — a 36+-actor deployment,
23 ``priority=`` enqueue-time call sites, a BACKLOG/interactive split):
the vanilla surface carries priority end to end — the ``jobs.priority``
smallint column (``migrations/01.00.00_01_pre_initial.sql``, DEFAULT 0),
the enqueue's ``priority=`` kwarg (the per-call override over the actor's
declared default), and the dispatch claim's ``priority DESC`` ORDER BY.
The workflow surface carried NONE of it: every wf INSERT omitted the
column, so a flow step landed at the column default — an interactive step
could not jump a 100k-deep sweep backlog. A regression on a shipped
surface, cured additively (the vanilla face byte-identical).

THE PRECEDENCE LAW (per row, first non-None wins):

    explicit per-call (``run``/``create_flow`` ``priority=``)
  > the step's declaration (``step(priority=)`` / ``RouteArm(priority=)``)
  > the actor's declaration (``@app.actor(priority=)``)
  > the column default (0).

The pins mirror the law: the declaration-site refusals (the smallint
domain — the vanilla ``check_priority_domain``'s law at every layer that
accepts a priority), the projected row carrying the value (the column
asserted directly), all four precedence levels, the claim order (a
handful of rows — the shared claim SQL's ``priority DESC`` honoring wf
rows; no 100k simulation), and the vanilla surface's untouched face.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.client._args import build_enqueue_args
from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    run,
    sink,
    step,
)
from taskq.workflows._worker_execution import (
    execute_flow_job,
    project_workflow_actor_configs,
)

# ── the shared fixtures' vocabulary (the capacity pins' own shapes) ──────


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Image(BaseModel):
    uri: str


class Audio(BaseModel):
    uri: str


async def _source_body(ctx: StepContext, params: Ingest) -> list[Image | Audio]:
    """The route/map source: two elements, one per arm."""
    return [Image(uri="img://1"), Audio(uri="aud://1")]


async def _list_source(ctx: StepContext, params: Ingest) -> list[str]:
    """The plain map's source."""
    return ["a", "b"]


async def _item_body(ctx: StepContext, item: str) -> str:
    return item


async def _plain_body(ctx: StepContext, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def _image_arm(ctx: StepContext, item: Image) -> Report:
    return Report(ref=item.uri)


async def _audio_arm(ctx: StepContext, item: Audio) -> Report:
    return Report(ref=item.uri)


def _rows_priority(
    rows: list[Any],
) -> dict[str, int]:
    """The step rows' priority column, keyed by step key (the root's
    ``__flow__`` row excluded — the law's subjects are the step rows)."""
    return {row["step_key"]: int(row["priority"]) for row in rows}


async def _flow_rows(pool: asyncpg.Pool, schema: str, flow_id: JobId) -> list[asyncpg.Record]:
    return await pool.fetch(
        f'SELECT step_key, map_index, priority, metadata FROM "{schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (module_pg_schema); asyncpg cannot bind identifiers as parameters.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        flow_id,
    )


def _meta(raw: Any) -> dict[str, Any]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict), decoded
    return decoded


# ── (a) the declaration-site refusals (the smallint domain law) ──────────


def test_actor_priority_out_of_domain_refused() -> None:
    """A priority past the smallint ceiling is the vanilla ``@actor``'s
    own refusal (``check_priority_domain`` — the same refusal the client
    override and the actor declaration raise), at the wf DECLARATION
    site: the column cannot store the value, so nothing may declare it."""
    app = WorkflowApp()
    with pytest.raises(ValueError, match="priority"):

        @app.actor(priority=32768)
        async def bad(ctx: StepContext, params: Ingest) -> Report:
            return Report(ref=params.doc_id)  # pragma: no cover - the refusal is the pin


def test_actor_priority_floor_refused() -> None:
    """The smallint FLOOR refuses too (the domain is two-sided)."""
    app = WorkflowApp()
    with pytest.raises(ValueError, match="priority"):

        @app.actor(priority=-32769)
        async def bad(ctx: StepContext, params: Ingest) -> Report:
            return Report(ref=params.doc_id)  # pragma: no cover - the refusal is the pin


def test_actor_priority_bool_refused() -> None:
    """``True``/``False`` are ints in Python — a bool passed as a
    priority is a bug wearing an int's clothes (the vanilla
    ``check_priority_domain``'s own bool refusal)."""
    app = WorkflowApp()
    with pytest.raises(ValueError, match="priority"):

        @app.actor(priority=True)
        async def bad(ctx: StepContext, params: Ingest) -> Report:
            return Report(ref=params.doc_id)  # pragma: no cover - the refusal is the pin


def test_step_priority_out_of_domain_refused() -> None:
    """The step's own declaration refuses the same domain (the wiring
    site — before anything registers)."""
    app = WorkflowApp()

    @app.workflow("step-domain-flow")
    def step_domain_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo", priority=32768))

    with pytest.raises(ValueError, match="priority"):
        app.get("step-domain-flow")


def test_route_arm_priority_out_of_domain_refused() -> None:
    """The RouteArm's override refuses at the route's wiring door (the
    arms' normalization loop — the same door that normalizes bare
    callables)."""
    app = WorkflowApp()

    @app.workflow("arm-domain-flow")
    def arm_domain_flow() -> Promise[object]:
        src = step(_source_body, Ingest(doc_id="d"), key="src")
        routed = map_source(
            src,
            {Image: RouteArm(body=_image_arm, priority=32768), Audio: _audio_arm},
        )
        return build(routed)

    with pytest.raises(ValueError, match="priority"):
        app.get("arm-domain-flow")


async def test_run_priority_out_of_domain_refused() -> None:
    """The per-call face refuses at the run door — the same domain law at
    every layer that accepts a priority (the constants module's own
    docstring's law). The check precedes any pool use (the refusal is
    the door's first act)."""
    app = WorkflowApp()

    @app.workflow("run-domain-flow")
    def run_domain_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    compiled = app.get("run-domain-flow")
    with pytest.raises(ValueError, match="priority"):
        await run(compiled, Any, "s", priority=32768)  # type: ignore[arg-type]  # Why: the domain check precedes any pool use — the refusal is the pin.


# ── (b) the declaration → row leg (the column asserted directly) ─────────


@pytest.mark.integration
async def test_actor_declared_priority_lands_on_the_rows(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ACTOR DECLARATION'S RIDE: ``@app.actor(priority=5)`` wired via
    ``step()`` without its own priority — the node's rows land with the
    actor's declared priority in the ``jobs.priority`` COLUMN (asserted
    directly). The convicted variant: every wf INSERT omitted the column
    and the row landed at the column default — the actor's declaration
    silently dropped."""
    app = WorkflowApp()

    @app.actor(priority=5)
    async def prio_body(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("actor-prio-flow")
    def actor_prio_flow() -> Promise[object]:
        return build(step(prio_body, Ingest(doc_id="d"), key="solo"))

    runner = FlowRunner(app.get("actor-prio-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    rows = await _flow_rows(wf_pool, wf_schema, flow_id)
    assert _rows_priority(rows) == {"solo": 5}, _rows_priority(rows)


@pytest.mark.integration
async def test_undeclared_priority_keeps_the_column_default_and_the_row_shape(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE BYTE-IDENTITY LAW (the cure's own non-negotiable): no
    priority declared anywhere — the row's priority is the COLUMN DEFAULT
    (0) and the row's metadata keeps the pre-cure shape exactly (the
    flow_id stamp, no new keys). The no-kwarg surface is today's surface,
    byte for byte."""
    app = WorkflowApp()

    @app.workflow("default-prio-flow")
    def default_prio_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    runner = FlowRunner(app.get("default-prio-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    rows = await _flow_rows(wf_pool, wf_schema, flow_id)
    assert _rows_priority(rows) == {"solo": 0}, _rows_priority(rows)
    assert _meta(rows[0]["metadata"]) == {"flow_id": str(flow_id)}, _meta(rows[0]["metadata"])


# ── (c) the precedence law — all four levels ─────────────────────────────


@pytest.mark.integration
async def test_the_precedence_law_all_four_levels(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """THE FOUR LEVELS, PINNED IN TWO RUNS of one workflow:

    * run A (no per-call): the actor-only step reads the ACTOR's 5; the
      step-declared step reads its own 7 (the step overrides the actor,
      never the reverse); the undeclared step reads the COLUMN DEFAULT 0.
    * run B (per-call 8): EVERY step reads 8 — the explicit per-call
      overrides all declarations (the load-bearing face: the
      enqueue-time override wins).

    The resolution is per RUN and per ROW: the run-level override never
    edits the declaration — run A re-runs the same workflow at its
    declared priorities (the create_flow call's own kwarg, not app
    state)."""
    app = WorkflowApp()

    @app.actor(priority=5)
    async def prio_body(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("precedence-flow")
    def precedence_flow() -> Promise[object]:
        actor_only = step(prio_body, Ingest(doc_id="d"), key="actor_only")
        step_declared = step(prio_body, Ingest(doc_id="d"), key="step_declared", priority=7)
        undeclared = step(_plain_body, Ingest(doc_id="d"), key="undeclared")
        sink(actor_only, step_declared, undeclared)
        return build(step_declared)

    compiled = app.get("precedence-flow")

    # Run A — the declarations speak.
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_a = (await runner.create_flow()).flow_id
    rows_a = _rows_priority(await _flow_rows(wf_pool, wf_schema, flow_a))
    assert rows_a == {"actor_only": 5, "step_declared": 7, "undeclared": 0}, rows_a

    # Run B — the per-call override wins at every level.
    runner_b = FlowRunner(compiled, wf_pool, wf_schema)
    flow_b = (await runner_b.create_flow(priority=8)).flow_id
    rows_b = _rows_priority(await _flow_rows(wf_pool, wf_schema, flow_b))
    assert rows_b == {"actor_only": 8, "step_declared": 8, "undeclared": 8}, rows_b


@pytest.mark.integration
async def test_the_packaged_run_carries_the_per_call_override(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE PACKAGED DOOR (the ergonomic face): ``run(...,
    priority=6)`` — the run's rows land at 6, the same law the
    create_flow face carries (the packaged run is create + drive, not a
    second enqueue semantics)."""
    app = WorkflowApp()

    @app.actor(priority=5)
    async def prio_body(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("packaged-prio-flow")
    def packaged_prio_flow() -> Promise[object]:
        return build(step(prio_body, Ingest(doc_id="d"), key="solo"))

    compiled = app.get("packaged-prio-flow")
    result = await run(compiled, wf_pool, wf_schema, input={"doc_id": "d"}, priority=6)
    assert result.outcome == "terminal", result.outcome
    rows = _rows_priority(await _flow_rows(wf_pool, wf_schema, result.flow_id))
    assert rows == {"solo": 6}, rows


# ── (d) the fork's children — the map and the route arms ─────────────────


@pytest.mark.integration
async def test_map_children_carry_the_source_step_declaration(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE MAP'S RIDE: the map's children share the source node's cohort
    (the capacity law's own arithmetic) — the source step's declared
    priority rides the children's rows AND the join's (the fork's rows
    land at the fork's declared priority; the join is the source's own
    cohort, so the source's resolution)."""
    app = WorkflowApp()

    @app.workflow("map-prio-flow")
    def map_prio_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src", priority=4)
        mapped = map_source(source, _item_body)
        return build(mapped)

    runner = FlowRunner(app.get("map-prio-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    state = await runner.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = _rows_priority(await _flow_rows(wf_pool, wf_schema, flow_id))
    item_rows = {k: v for k, v in rows.items() if k.startswith("src.item")}
    assert item_rows and all(v == 4 for v in item_rows.values()), rows
    assert rows["src.join"] == 4, rows


@pytest.mark.integration
async def test_route_arm_priority_overrides_down_and_the_join_stays_the_sources(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE ARM'S DOWN-OVERRIDE (the RouteArm placement law's own
    arithmetic — an arm overrides DOWN from the source, never up): the
    image arm's ``priority=9`` lands on the image rows ONLY; the bare
    audio arm keeps the source step's declared 2; the join (the SOURCE's
    cohort — the arm override does not retouch it) reads the source's 2.
    The convicted variant: the arm's override either dropped silently or
    leaked onto the join."""
    app = WorkflowApp()

    @app.workflow("arm-prio-flow")
    def arm_prio_flow() -> Promise[object]:
        src = step(_source_body, Ingest(doc_id="d"), key="src", priority=2)
        routed = map_source(
            src,
            {Image: RouteArm(body=_image_arm, priority=9), Audio: RouteArm(body=_audio_arm)},
        )
        return build(routed)

    runner = FlowRunner(app.get("arm-prio-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    state = await runner.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = await _flow_rows(wf_pool, wf_schema, flow_id)
    by_key = {row["step_key"]: int(row["priority"]) for row in rows}
    image_rows = {k: v for k, v in by_key.items() if k.startswith("src.item:") and "Image" in k}
    audio_rows = {k: v for k, v in by_key.items() if k.startswith("src.item:") and "Audio" in k}
    assert image_rows and all(v == 9 for v in image_rows.values()), by_key
    assert audio_rows and all(v == 2 for v in audio_rows.values()), by_key
    assert by_key["src.join"] == 2, by_key


# ── (e) the fleet door — the run override reaches the forked rows ────────


@pytest.mark.integration
async def test_the_run_override_reaches_the_fleet_forks(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """THE FLEET DOOR'S THREAD (production's own path): the run-level
    override stamps the ROOT row's metadata (``wf_priority``) at create;
    the worker-hosted door's bounded read (the SAME round trip the
    workflow-name stamp rides) resolves it, and the fork spawned at the
    claimed source's finalize lands its children at the OVERRIDE — the
    per-call face governs rows materialized AFTER create too (the map
    children a fleet worker forks mid-run)."""
    app = WorkflowApp()

    @app.workflow("fleet-prio-flow")
    def fleet_prio_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src", priority=2)
        mapped = map_source(source, _item_body)
        return build(mapped)

    runner = FlowRunner(app.get("fleet-prio-flow"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow(priority=6)).flow_id

    # THE ROOT STAMP: the override rides the root's metadata — the door's
    # read leg.
    root = await wf_conn.fetchrow(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
        flow_id,
    )
    assert root is not None
    assert _meta(root["metadata"]).get("wf_priority") == 6, _meta(root["metadata"])

    # The fleet path: project the cohorts, stamp a capable worker, claim
    # the source through the REAL dispatch, execute it through the door.
    for config in project_workflow_actor_configs():
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
        "VALUES ($1, 'prio-pin', 1, '{default}', $2::jsonb)",
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
    claimed = dispatched[0]
    assert claimed["step_key"] == "src", claimed

    from taskq.backend._records import _job_row_from_record

    await execute_flow_job(
        pool=module_pg_pool,
        schema=wf_schema,
        worker_id=JobId(worker_id),
        # The dispatch's own record decoded through the backend's read
        # model (the fleet's dispatch loop's own decode).
        job=_job_row_from_record(claimed),
    )
    # The fork's children: the run override (6) beats the source's own
    # declared 2 — the per-call face is the load-bearing one.
    rows = await _flow_rows(module_pg_pool, wf_schema, flow_id)
    by_key = {row["step_key"]: int(row["priority"]) for row in rows}
    item_rows = {k: v for k, v in by_key.items() if k.startswith("src.item")}
    assert item_rows and all(v == 6 for v in item_rows.values()), by_key
    assert by_key["src.join"] == 6, by_key


# ── (f) the claim order — the shared SQL honors wf rows ──────────────────


async def _seed_vanilla_pending(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    priority: int,
    scheduled_at: datetime,
) -> UUID:
    """One vanilla pending row, seeded directly (the claim's raw
    material) — the explicit priority is the point."""
    job_id = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, scheduled_at, schedule_to_close
            ) VALUES (
                $1, $2, 'default', '{{}}'::jsonb, 3, 'transient',
                'pending', $3, $4, $5
            )""",  # noqa: S608
        job_id,
        actor,
        priority,
        scheduled_at,
        datetime.now(UTC) + timedelta(seconds=60),
    )
    return job_id


async def _stamp_capable_worker(conn: asyncpg.Connection, schema: str) -> UUID:
    """The dispatch fence's capability leg + the cohort rows both actors
    need (the claim's per-actor arithmetic is inner-safe on actor_config)."""
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
        "VALUES ($1, 'prio-order-pin', 1, '{default}', $2::jsonb)",
        worker_id,
        json.dumps({"workflow_execution": True}),
    )
    return worker_id


@pytest.mark.integration
async def test_the_claim_orders_the_high_priority_wf_row_first(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """THE CONVICTION'S OWN SCENARIO, IN MINIATURE: a small pending
    backlog (three vanilla rows, priority 0, older scheduled_at — they
    win every tie today) and ONE wf step carrying priority 5 → the
    dispatch's next round claims the wf row FIRST. The wf rows ride the
    SAME claim statement's ``priority DESC`` the vanilla rows always
    obeyed — no wf-specific ordering exists; the column is the law. No
    100k simulation: four rows prove the ordering."""
    app = WorkflowApp()

    @app.workflow("claim-order-flow")
    def claim_order_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo", priority=5))

    # The cohorts' actor_config rows (the claim's inner-safe join) — the
    # wf cohort AND the vanilla actor's.
    for config in project_workflow_actor_configs():
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '  # noqa: S608
        "VALUES ('test_actor', 'default') ON CONFLICT (actor) DO NOTHING",
    )
    worker_id = await _stamp_capable_worker(wf_conn, wf_schema)

    # The backlog: three vanilla rows, priority 0, one second OLDER —
    # scheduled_at would serve them first if priority did not dominate.
    past = datetime.now(UTC) - timedelta(seconds=1)
    for _ in range(3):
        await _seed_vanilla_pending(
            wf_conn, wf_schema, actor="test_actor", priority=0, scheduled_at=past
        )

    # The wf row: priority 5 (the step's declaration).
    runner = FlowRunner(app.get("claim-order-flow"), wf_pool, wf_schema)
    await runner.create_flow()

    claimed = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=1,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert len(claimed) == 1, claimed
    assert claimed[0]["step_key"] == "solo", (
        f"the high-priority wf row was not claimed first: {claimed}"
    )


@pytest.mark.integration
async def test_the_claim_orders_a_high_priority_vanilla_row_over_a_default_wf_row(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """THE NO-BIAS ARM: the wf surface adds no wf-favoring ordering — a
    vanilla row at priority 5 beats a wf row at the column default (0)
    even though the wf row is NEWER. The column decides; the surface
    that declares it is irrelevant (the shared claim SQL's own law)."""
    app = WorkflowApp()

    @app.workflow("claim-order-default-flow")
    def claim_order_default_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    for config in project_workflow_actor_configs():
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '  # noqa: S608
        "VALUES ('test_actor', 'default') ON CONFLICT (actor) DO NOTHING",
    )
    worker_id = await _stamp_capable_worker(wf_conn, wf_schema)

    # The vanilla row: priority 5, one second OLDER.
    past = datetime.now(UTC) - timedelta(seconds=1)
    await _seed_vanilla_pending(
        wf_conn, wf_schema, actor="test_actor", priority=5, scheduled_at=past
    )
    # The wf row: the column default, NEWER.
    runner = FlowRunner(app.get("claim-order-default-flow"), wf_pool, wf_schema)
    await runner.create_flow()

    claimed = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=1,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert len(claimed) == 1, claimed
    assert claimed[0]["actor"] == "test_actor", (
        f"the vanilla high-priority row lost to a default wf row: {claimed}"
    )


# ── (g) the vanilla surface is untouched ─────────────────────────────────


def test_the_vanilla_per_call_priority_law_is_untouched() -> None:
    """The vanilla resolution law — per-call > the actor's declared
    default (``build_enqueue_args``) — reads exactly as before the wf
    cure: the wf surface's changes added no parameter, no default, no
    rewrite to the vanilla path. The fence: the same two assertions the
    vanilla pins have always made, made here BESIDE the wf pins."""
    from taskq.actor import actor

    @actor(queue="default", priority=2)
    async def vanilla_body(params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the resolution is the pin

    declared = build_enqueue_args(vanilla_body, Ingest(doc_id="d"))
    assert declared.priority == 2, declared.priority
    overridden = build_enqueue_args(vanilla_body, Ingest(doc_id="d"), priority=9)
    assert overridden.priority == 9, overridden.priority


# ── (h) the review's cures — the drain, the arm-body handles, the ────────
#        adoption door, the post-parse domain clamp


@pytest.mark.integration
async def test_the_loop_escalation_materializes_at_the_run_priority(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """CURE 1 (review 3a-ii — the reviewer's preferred remedy): the
    root's ``wf_priority`` stamp threads through the OUTBOX DRAIN —
    ``run(priority=10)`` whose loop exhausts enqueues the escalation
    through the outbox, and the drain's consumer INSERT lands the
    escalation row's ``jobs.priority`` at 10 (the run's interactive
    follow-up jumps the backlog with the run). The convicted variant:
    the drain's insert omitted the priority column — the escalation, an
    interactive run's most latency-sensitive follow-up, materialized at
    the column default while the pin's own invariant ("the per-call face
    governs rows materialized AFTER create too") held everywhere else."""
    from taskq.workflows import Refine, loop
    from taskq.workflows.api._loop import ESCALATION_STEP_KEY
    from taskq.workflows.api._runner_errors import WorkflowRunError

    async def never_done(ctx: StepContext, carry: object) -> object:
        return Refine(1)

    app = WorkflowApp()

    @app.workflow("escalation-prio-flow")
    def escalation_prio_flow() -> Promise[object]:
        return build(loop("wall", never_done, max_iterations=2, initial=0))

    # THE RUN FACE: the exhausted loop terminal-FAILS the flow, so the
    # run's result face raises the failure's own error — AFTER the drive
    # (the exhaust tx committed the escalation's outbox row, the tick's
    # drain materialized the consumer). The per-call stamp rode the
    # create; the escalation row is the subject.
    with pytest.raises(WorkflowRunError, match="IterationLimitExhausted"):
        await run(app.get("escalation-prio-flow"), wf_pool, wf_schema, priority=10)
    escalation_row = await wf_conn.fetchrow(
        f'SELECT priority FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE step_key = $1 AND metadata ? 'flow_id' AND "
        "(metadata->>'flow_id')::uuid IN ("
        f"   SELECT (metadata->>'flow_id')::uuid FROM \"{wf_schema}\".jobs "
        "   WHERE step_key = 'wall')",
        ESCALATION_STEP_KEY,
    )
    assert escalation_row is not None, (
        "the exhaust enqueued no escalation consumer row — the pin never "
        "manifested the drain's subject"
    )
    assert int(escalation_row["priority"]) == 10, escalation_row["priority"]


@pytest.mark.integration
async def test_the_drain_materializes_consumers_at_the_stamped_priority(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
) -> None:
    """CURE 1's direct drain face: an outbox row whose flow root stamps
    ``wf_priority`` 7 — the drain's consumer INSERT lands the consumer's
    ``jobs.priority`` at 7 (the stamp's own arithmetic, mirrored in the
    drain's SQL: one join to the root's metadata + COALESCE to the column
    default)."""
    from tests._wf_fixtures import seed_flow

    flow_id = await seed_flow(wf_conn, wf_schema)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',  # noqa: S608
        flow_id,
        json.dumps({"wf_priority": 7}),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_outbox '  # noqa: S608
        "(id, join_job_id, flow_id, consumer_step_key, bindings) "
        "VALUES ($1, $2, $3, 'drained_consumer', $4::jsonb)",
        new_uuid(),
        new_uuid(),
        flow_id,
        json.dumps({"actor": "wf", "queue": "default", "payload": {}}),
    )
    from taskq.workflows._sweep import drain_outbox

    delivered = await drain_outbox(module_pg_pool, wf_sql)
    assert delivered == 1, delivered
    consumer = await wf_conn.fetchrow(
        f'SELECT priority FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'drained_consumer'",
        flow_id,
    )
    assert consumer is not None, "the drain inserted no consumer row"
    assert int(consumer["priority"]) == 7, consumer["priority"]


@pytest.mark.integration
async def test_the_drain_degrades_a_malformed_stamp_to_the_default(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
) -> None:
    """CURE 1's degrade arm (the fleet door's own semantics, mirrored):
    a stamp a hand-crafted row rendered unparseable ("abc") or
    out-of-domain-but-parseable ("99999" — the smallint wedge) degrades
    to the COLUMN DEFAULT (0) — the drain delivers, never wedges. A
    priority is an ordering hint, never a correctness input worth
    crashing the sweep on."""
    from taskq.workflows._sweep import drain_outbox
    from tests._wf_fixtures import seed_flow

    for stamp in ("abc", "99999"):
        flow_id = await seed_flow(wf_conn, wf_schema)
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',  # noqa: S608
            flow_id,
            json.dumps({"wf_priority": stamp}),
        )
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_outbox '  # noqa: S608
            "(id, join_job_id, flow_id, consumer_step_key, bindings) "
            "VALUES ($1, $2, $3, 'degraded_consumer', $4::jsonb)",
            new_uuid(),
            new_uuid(),
            flow_id,
            json.dumps({"actor": "wf", "queue": "default", "payload": {}}),
        )
        delivered = await drain_outbox(module_pg_pool, wf_sql)
        assert delivered == 1, (stamp, delivered)
        consumer = await wf_conn.fetchrow(
            f'SELECT priority FROM "{wf_schema}".jobs '  # noqa: S608
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'degraded_consumer'",
            flow_id,
        )
        assert consumer is not None, (stamp, "the drain inserted no consumer row")
        assert int(consumer["priority"]) == 0, (stamp, consumer["priority"])


@pytest.mark.integration
async def test_map_body_handle_priority_reaches_the_children(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """CURE 2 (review F3 — the map's face): a map body that is an
    ``@app.actor(priority=5)`` HANDLE — the arm children resolve the
    handle's declared 5 (the actor level of the law). The convicted
    variant: the runner's only priority readers were the four
    ``_resolve_priority`` sites — a handle body's declaration dropped
    SILENTLY (the identical shape fails CLOSED for capacity terms —
    H-A4's never-a-silent-drop law). The handle's value NEVER folds into
    the source node: the source's own row and the JOIN stay at the
    source's resolution."""
    app = WorkflowApp()

    @app.actor(priority=5)
    async def prio_item_body(ctx: StepContext, item: str) -> str:
        return item  # pragma: no cover - the runner drives real bodies

    @app.workflow("map-handle-prio-flow")
    def map_handle_prio_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src")
        mapped = map_source(source, prio_item_body)
        return build(mapped)

    runner = FlowRunner(app.get("map-handle-prio-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    state = await runner.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = _rows_priority(await _flow_rows(wf_pool, wf_schema, flow_id))
    item_rows = {k: v for k, v in rows.items() if k.startswith("src.item")}
    assert item_rows and all(v == 5 for v in item_rows.values()), rows
    assert rows["src.join"] == 0, rows
    assert rows["src"] == 0, rows


@pytest.mark.integration
async def test_route_arm_body_handle_priority_reaches_that_arms_children(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """CURE 2 (review F3 — the route's face): ``RouteArm(body=handle)``
    where the handle declares 5 — THAT arm's children resolve 5 (the
    actor level), the OTHER arm's children stay at the source's declared
    2, and the JOIN stays the source's 2 (the per-arm level is per-ARM —
    never folded into the source node, which would cross-contaminate the
    other arms and the join)."""
    app = WorkflowApp()

    @app.actor(priority=5)
    async def prio_image_arm(ctx: StepContext, item: Image) -> Report:
        return Report(ref=item.uri)  # pragma: no cover - the runner drives real bodies

    @app.workflow("arm-handle-prio-flow")
    def arm_handle_prio_flow() -> Promise[object]:
        src = step(_source_body, Ingest(doc_id="d"), key="src", priority=2)
        routed = map_source(
            src,
            {Image: RouteArm(body=prio_image_arm), Audio: RouteArm(body=_audio_arm)},
        )
        return build(routed)

    runner = FlowRunner(app.get("arm-handle-prio-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    state = await runner.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = await _flow_rows(wf_pool, wf_schema, flow_id)
    by_key = {row["step_key"]: int(row["priority"]) for row in rows}
    image_rows = {k: v for k, v in by_key.items() if k.startswith("src.item:") and "Image" in k}
    audio_rows = {k: v for k, v in by_key.items() if k.startswith("src.item:") and "Audio" in k}
    assert image_rows and all(v == 5 for v in image_rows.values()), by_key
    assert audio_rows and all(v == 2 for v in audio_rows.values()), by_key
    assert by_key["src.join"] == 2, by_key


@pytest.mark.integration
async def test_the_adoption_drive_resolves_the_run_priority_from_the_row(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """CURE 3 (review F2 — the dev door's arbiter-adoption path): the
    first create crashes (the echo died with the process); the re-drive's
    FRESH runner adopts the existing run — the per-call level resolves
    from the ROOT'S STAMP (one bounded PK read — the fleet door's own
    SELECT shape), so the fork's children land at 6. The re-drive passes
    NO kwarg: the row is the law, never the echo. The convicted variant:
    the echo was set only inside ``if complete:`` — the adoption drove
    with ``_run_priority = None`` and forked the children at the
    declarations while the root's stamp said 6 (one run, split cohort)."""
    app = WorkflowApp()

    @app.workflow("adopt-prio-flow")
    def adopt_prio_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src")
        mapped = map_source(source, _item_body)
        return build(mapped)

    compiled = app.get("adopt-prio-flow")
    first = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (
        await first.create_flow(input=Ingest(doc_id="d"), run_key="adopt:prio", priority=6)
    ).flow_id

    # THE MID-RUN ADOPTION: a fresh runner (the crashed process's echo is
    # gone) adopts the existing run and drives it.
    second = FlowRunner(compiled, wf_pool, wf_schema)
    claim = await second.create_flow(input=Ingest(doc_id="d"), run_key="adopt:prio")
    assert not claim.created, claim
    assert claim.flow_id == flow_id, claim
    state = await second.drive(flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = _rows_priority(await _flow_rows(wf_pool, wf_schema, flow_id))
    item_rows = {k: v for k, v in rows.items() if k.startswith("src.item")}
    assert item_rows and all(v == 6 for v in item_rows.values()), rows
    assert rows["src.join"] == 6, rows


@pytest.mark.integration
async def test_the_adoption_drive_degrades_an_out_of_domain_stamp(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """THE ADOPTION DOOR'S POST-PARSE DOMAIN CLAMP (the re-review's
    one-line residual — cure 4's own law at the adopt door): a
    hand-crafted root stamp ``wf_priority="99999"`` — PARSEABLE, past the
    smallint domain — degrades to NO override at the ADOPT door too, and
    the adopted drive PROCEEDS (the children + the join at the source's
    DECLARED 2, never parked). The convicted variant: the adopt door
    parsed with ``int()`` alone — the 99999 rode to the fork's smallint
    bind and WEDGED the re-drive (the same DataError mid-finalize cure 4
    closed on the fleet door; the two doors share one arithmetic)."""
    app = WorkflowApp()

    @app.workflow("adopt-clamp-prio-flow")
    def adopt_clamp_prio_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src", priority=2)
        mapped = map_source(source, _item_body)
        return build(mapped)

    compiled = app.get("adopt-clamp-prio-flow")
    first = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await first.create_flow(input=Ingest(doc_id="d"), run_key="adopt:clamp")).flow_id
    # THE HAND-CRAFTED STAMP: parseable, out of the smallint domain (the
    # create's own door refuses the kwarg — the corruption-only shape).
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',  # noqa: S608
        flow_id,
        json.dumps({"wf_priority": "99999"}),
    )

    # THE ADOPTED DRIVE: a fresh runner adopts the existing run and
    # drives it — the stamp's resolution degrades, the declarations speak.
    second = FlowRunner(compiled, wf_pool, wf_schema)
    claim = await second.create_flow(input=Ingest(doc_id="d"), run_key="adopt:clamp")
    assert not claim.created, claim
    state = await second.drive(claim.flow_id, until="terminal", max_ticks=500)
    assert state == "terminal", state

    rows = _rows_priority(await _flow_rows(wf_pool, wf_schema, claim.flow_id))
    item_rows = {k: v for k, v in rows.items() if k.startswith("src.item")}
    assert item_rows and all(v == 2 for v in item_rows.values()), rows
    assert rows["src.join"] == 2, rows


@pytest.mark.integration
async def test_the_fleet_door_degrades_an_out_of_domain_stamp(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """CURE 4 (the review's recommended — the post-parse domain clamp):
    a hand-crafted root stamp ``wf_priority="99999"`` — PARSEABLE, past
    the smallint domain — degraded to NO override (the declarations — or
    the column default — speak) and the fork PROCEEDED. The convicted
    variant: the parse accepted 99999 and the fork's smallint bind
    WEDGED the door (the DataError mid-finalize — the fork never landed,
    the run parked)."""
    app = WorkflowApp()

    @app.workflow("clamp-prio-flow")
    def clamp_prio_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src", priority=2)
        mapped = map_source(source, _item_body)
        return build(mapped)

    runner = FlowRunner(app.get("clamp-prio-flow"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',  # noqa: S608
        flow_id,
        json.dumps({"wf_priority": "99999"}),
    )

    for config in project_workflow_actor_configs():
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
        "VALUES ($1, 'prio-clamp-pin', 1, '{default}', $2::jsonb)",
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

    from taskq.backend._records import _job_row_from_record

    # THE DOOR PROCEEDS: no raise, the fork lands at the DECLARED 2 (the
    # stamp degraded to no-override — the declaration speaks).
    await execute_flow_job(
        pool=module_pg_pool,
        schema=wf_schema,
        worker_id=JobId(worker_id),
        job=_job_row_from_record(dispatched[0]),
    )
    rows = await _flow_rows(module_pg_pool, wf_schema, flow_id)
    by_key = {row["step_key"]: int(row["priority"]) for row in rows}
    item_rows = {k: v for k, v in by_key.items() if k.startswith("src.item")}
    assert item_rows and all(v == 2 for v in item_rows.values()), by_key
    assert by_key["src.join"] == 2, by_key
