"""THE WORKFLOW FAIRNESS STAMP — every claimable wf row is born with its
run's ``flow_id`` as the ``jobs.fairness_key`` cohort.

The dispatch's round-robin variant partitions candidates by
``COALESCE(fairness_key, '__null__')``; every wf row was born with the
column NULL, so ALL workflow rows on a queue collapsed into ONE FIFO
cohort and a deep run starved a shallow one exactly the way the vanilla
starvation finding described. The stamp rides the five claimable-row
INSERT sites (the static node insert, the fork children, the emit
children, the fork's join node, the outbox drain's consumer rows); the
flow ROOT (``step_key = '__flow__'``) stays unstamped — it is the
dispatch fence's root-marker exclusion, never work.

On a ``strict_fifo`` queue the stamp is stored and ignored: the
strict-FIFO candidates lateral orders by ``priority DESC, scheduled_at,
id`` alone (its own candidates render no fairness rank), so a later-born
run waits behind an earlier-born run exactly as before the stamp
existed.
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier, not user input; every value is $-bound.
import json
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import (
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
    dispatch_batch,
)
from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    map_source,
    step,
)
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import drain_outbox
from tests.conftest import unique_health_sock_path

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)


class Ingest(BaseModel):
    doc_id: str


async def _seed_queue(
    conn: asyncpg.Connection,
    schema: str,
    queue: str,
    mode: str,
    actors: tuple[str, ...] = ("wf", "workflow"),
) -> None:
    """The queue's mode row + the seeded actors' config rows (the
    capacity question is off the table — the fence leg is the only
    refuser the pins isolate)."""
    await conn.execute(
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET mode = $2",
        queue,
        mode,
    )
    for actor in actors:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            actor,
            queue,
        )


async def _capable_worker_row(conn: asyncpg.Connection, schema: str, queue: str) -> UUID:
    """A workflow_execution-capable worker row (the boot projection's
    stamp): the dispatch fence's execution leg admits wf rows only for
    this worker."""
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'wf-fairness-pin', 1, $2, $3::jsonb)",
        worker_id,
        [queue],
        json.dumps({"workflow_execution": True}),
    )
    return worker_id


async def _dispatch(
    conn: asyncpg.Connection,
    sql: str,
    schema: str,
    worker_id: UUID,
    queue: str,
    limit: int,
) -> list[asyncpg.Record]:
    return await dispatch_batch(
        conn,
        sql=sql.format(schema=schema),
        queues=[queue],
        limit_n=limit,
        worker_id=worker_id,
        lock_lease=_LEASE,
    )


def _flow_ids(rows: list[asyncpg.Record]) -> set[str]:
    """The dispatched rows' flow ids (the metadata's flow_id link)."""
    out: set[str] = set()
    for r in rows:
        raw = r["metadata"]
        meta = json.loads(raw) if isinstance(raw, str) else raw
        if meta and meta.get("flow_id"):
            out.add(meta["flow_id"])
    return out


# ── THE INTERLEAVE PIN ───────────────────────────────────────────────────


async def test_a_deep_run_does_not_starve_a_shallow_run_on_round_robin(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """Run A's map forks ~500 pending children; run B holds ONE pending
    node; both runs' rows sit on one round_robin queue and A's rows were
    born first (they sort ahead on priority/scheduled_at/id). Dispatching
    with limit=4 must surface B's row within two rounds — the rows'
    per-run cohorts partition the admission, a deep cohort cannot occupy
    the whole round."""
    queue = "wf-fair-q"
    await _seed_queue(wf_conn, wf_schema, queue, "round_robin")
    worker_id = await _capable_worker_row(wf_conn, wf_schema, queue)

    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[int]:
        return list(range(500))

    async def item(ctx: StepContext, value: int) -> int:
        return value  # pragma: no cover - the pins claim, never execute

    @app.workflow("wf_fair_deep_map")
    def deep() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src", queue=queue)
        return build(map_source(source, item, queue=queue))

    @app.workflow("wf_fair_shallow")
    def shallow() -> Promise[object]:
        return build(step(src, Ingest(doc_id="d2"), key="only", queue=queue))

    deep_runner = FlowRunner(app.get("wf_fair_deep_map"), wf_pool, wf_schema)
    deep_flow = (await deep_runner.create_flow()).flow_id
    await deep_runner.tick(deep_flow)  # the source's finalize forks the 500 children

    shallow_runner = FlowRunner(app.get("wf_fair_shallow"), wf_pool, wf_schema)
    shallow_flow = (await shallow_runner.create_flow()).flow_id

    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'src.item'",
        deep_flow,
    )
    assert int(children or 0) == 500, "the deep run's map fork created its children"

    claimed_flow_ids: set[str] = set()
    for _round in range(2):
        got = await _dispatch(
            wf_conn, DISPATCH_ROUND_ROBIN_SQL, wf_schema, worker_id, queue, limit=4
        )
        claimed_flow_ids |= _flow_ids(list(got))
        if str(shallow_flow) in claimed_flow_ids:
            break

    assert str(shallow_flow) in claimed_flow_ids, (
        "the shallow run's single pending row was starved by the deep run's "
        "500 children across 2 dispatch rounds of limit=4 — the wf rows share "
        "one fairness cohort, so the round-robin partition admits them as one "
        "FIFO stream"
    )


# ── THE COHORT PIN ───────────────────────────────────────────────────────


async def test_wf_rows_carry_their_run_s_cohort_at_birth(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """Every wf row of a run carries THAT run's flow_id as its
    fairness_key — the static node insert, the fork's children, the
    fork's join node, and the outbox drain's consumer rows alike; the
    flow ROOT alone is unstamped (it is never work). DISTINCT over the
    run's claimable rows shows exactly one value, never NULL."""
    queue = "wf-cohort-q"
    await _seed_queue(wf_conn, wf_schema, queue, "round_robin")

    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[int]:
        return [0, 1]

    async def item(ctx: StepContext, value: int) -> int:
        return value  # pragma: no cover - the runner drives real bodies

    async def after(ctx: StepContext, value: list[int]) -> int:
        return sum(value)  # pragma: no cover - the runner drives real bodies

    @app.workflow("wf_cohort_stamp")
    def w() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src", queue=queue)
        mapped = map_source(source, item, queue=queue)
        return build(step(after, mapped, key="after", queue=queue))

    runner = FlowRunner(app.get("wf_cohort_stamp"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)  # the fork: children + join

    stamped = await wf_conn.fetch(
        f"""SELECT step_key, fairness_key FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id'
              AND step_key <> '__flow__'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    assert stamped, "the run's rows exist"
    cohorts = {str(r["fairness_key"]) for r in stamped}
    assert cohorts == {str(flow_id)}, (
        f"the run's claimable rows carry exactly one cohort (the run's own "
        f"flow_id); got {sorted(cohorts)}"
    )
    nulls = [dict(r) for r in stamped if r["fairness_key"] is None]
    assert not nulls, f"NULL-cohort wf rows: {nulls}"

    root = await wf_conn.fetchval(
        f'SELECT fairness_key FROM "{wf_schema}".jobs WHERE id = $1::uuid',
        flow_id,
    )
    assert root is None, "the flow root stays unstamped (it is never work)"


async def test_the_outbox_drain_s_consumer_row_carries_the_cohort(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """The drain's consumer INSERT is the fifth stamp site: a loop's
    exhaustion enqueues its escalation consumer through the outbox — the
    consumer row is born with the run's flow_id as its cohort."""
    queue = "wf-drain-q"
    await _seed_queue(wf_conn, wf_schema, queue, "round_robin")

    paged: list[dict[str, object]] = []

    async def page_operator(ctx: StepContext, escalation: dict[str, object]) -> dict[str, object]:
        paged.append(escalation)  # pragma: no cover - the driver runs real bodies
        return escalation  # pragma: no cover

    async def refine_forever(ctx: StepContext, carry: object) -> Refine[dict[str, int]]:
        return Refine({"n": 1})

    app = WorkflowApp()

    @app.workflow("wf_cohort_drain")
    def w() -> Promise[object]:
        return build(
            loop(
                "counter",
                refine_forever,
                max_iterations=1,
                on_exhausted="escalate",
                escalates_to=page_operator,
            )
        )

    runner = FlowRunner(app.get("wf_cohort_drain"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    await drain_outbox(wf_pool, runner.wsql)

    esc = await wf_conn.fetchval(
        f"""SELECT fairness_key FROM "{wf_schema}".jobs
            WHERE step_key = 'loop.escalation'
              AND (metadata->>'flow_id')::uuid = $1::uuid""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    assert esc is not None, "the escalation consumer row exists (the drain delivered it)"
    assert str(esc) == str(flow_id), "the drain's consumer row carries the run's cohort"


async def test_the_emit_s_children_carry_the_cohort(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The emit tx's chain starts (the streaming source's page children)
    are claimable wf rows — they are born with the run's flow_id as
    their cohort (the emit children INSERT's own stamp)."""
    from taskq.workflows._emit import emit_batch
    from taskq.workflows._types import EmitChild, NodeSpec
    from taskq.workflows.engine import insert_node
    from tests._wf_fixtures import seed_flow

    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await insert_node(
        wf_conn, wf_sql, NodeSpec(flow_id=flow_id, step_key="source", actor="wf", queue="default")
    )
    # The source's claim view (the emit's cursor checkpoint fences on it).
    worker = new_uuid()
    claim = await wf_conn.fetchrow(
        f'UPDATE "{wf_schema}".jobs SET status = $2, attempt = 1, claim_epoch = 1, '
        "locked_by_worker = $3, lock_expires_at = now() + interval '30 seconds', "
        "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
        "RETURNING attempt, claim_epoch",
        source_id,
        "running",
        worker,
    )
    assert claim is not None, "the source row claimed"
    children = [
        EmitChild(
            step_key="screen",
            actor="wf",
            queue="default",
            payload={"application": {"app_id": i}},
            trace_id=f"app-{i}",
            map_index=i,
        )
        for i in range(3)
    ]
    await emit_batch(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=JobId(worker),
        attempt=1,
        claim_epoch=1,
        children=children,
        cursor={"page": 0},
    )
    stamped = await wf_conn.fetch(
        f"""SELECT fairness_key FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id'
              AND step_key = 'screen'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_id,
    )
    assert len(stamped) == 3, "the emit's three chain starts exist"
    assert all(str(r["fairness_key"]) == str(flow_id) for r in stamped), (
        "the emit children are born with the run's cohort"
    )
    # THE SOURCE ROW ITSELF (engine.insert_node's own insert): the row
    # that emitted the page carries the run's cohort too — the stamp's
    # write-once law covers every claimable wf row the engine inserts.
    source_stamp = await wf_conn.fetchval(
        f'SELECT fairness_key FROM "{wf_schema}".jobs WHERE id = $1',  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        source_id,
    )
    assert str(source_stamp) == str(flow_id), (
        "the inserted source row carries the run's cohort (engine.insert_node's stamp)"
    )


# ── THE STRICT_FIFO INERTNESS PIN ────────────────────────────────────────


async def test_the_stamp_is_stored_and_ignored_on_strict_fifo(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """On a strict_fifo queue the stamp rides the row and the claim
    ignores it: the round admits in priority/scheduled_at/id order — a
    run born later waits behind a deeper run born earlier, the exact
    order the round_robin partition breaks. The stamp itself rides the
    claimed rows (stored)."""
    queue = "wf-fifo-q"
    await _seed_queue(wf_conn, wf_schema, queue, "strict_fifo")
    worker_id = await _capable_worker_row(wf_conn, wf_schema, queue)

    app = WorkflowApp()

    async def body(ctx: StepContext, params: Ingest) -> str:
        return params.doc_id  # pragma: no cover - the pins claim, never execute

    @app.workflow("wf_fifo_stamp")
    def w() -> Promise[object]:
        return build(step(body, Ingest(doc_id="d1"), key="first", queue=queue))

    runner = FlowRunner(app.get("wf_fifo_stamp"), wf_pool, wf_schema)
    flow_a = (await runner.create_flow()).flow_id
    flow_b = (await runner.create_flow()).flow_id
    rows_a = await wf_conn.fetch(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key <> '__flow__'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_a,
    )

    got = await _dispatch(wf_conn, DISPATCH_STRICT_FIFO_SQL, wf_schema, worker_id, queue, limit=1)
    rows = list(got)
    assert rows, "the strict-FIFO round claimed the stamped rows"
    # STORED: the stamps ride the dispatched rows.
    for r in rows:
        fid = r["fairness_key"]
        assert fid is not None, "the wf row's stamp is stored"
        assert str(fid) in {str(flow_a), str(flow_b)}
    # IGNORED: the round admits strictly in birth order — run A's row
    # (born first) takes the single claim and run B's row waits, although
    # it is its own cohort (the round_robin partition surfaces it in
    # round 1).
    claimed_ids = {str(r["id"]) for r in rows}
    assert claimed_ids == {str(r["id"]) for r in rows_a}, (
        "the strict-FIFO round claimed run A's row in birth order — the "
        "stamp did not reorder admission"
    )
    pending_b = await wf_conn.fetchval(
        f"""SELECT count(*) FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata ? 'flow_id' AND step_key <> '__flow__'
              AND status = 'pending'""",  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        flow_b,
    )
    assert int(pending_b or 0) == 1, "run B's row waits behind run A's (the FIFO order)"


# ── THE MIXED-FLEET PIN ──────────────────────────────────────────────────


async def test_wf_cohorts_interleave_beside_vanilla_keyed_cohorts(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """A round_robin queue serving VANILLA keyed rows AND wf rows: the
    wf runs' per-run cohorts and the vanilla jobs' declared keys are ONE
    partition vocabulary — each cohort surfaces within the first
    rounds."""
    queue = "wf-mixed-q"
    await _seed_queue(
        wf_conn, wf_schema, queue, "round_robin", actors=("wf", "workflow", "vanilla-actor")
    )
    worker_id = await _capable_worker_row(wf_conn, wf_schema, queue)

    # The vanilla keyed rows: two jobs under their own cohort key, born
    # FIRST (they sort ahead on the FIFO ordering). step_key stays NULL —
    # the vanilla row's own shape (the fence short-circuits on it).
    for _ in range(2):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind, status, "
            "fairness_key, scheduled_at) "
            "VALUES ($1, 'vanilla-actor', $2, '{}', 3, 'transient', 'pending', "
            "$3, now() - interval '1 hour')",
            new_uuid(),
            queue,
            "vanilla-team",
        )

    app = WorkflowApp()

    async def src(ctx: StepContext, params: Ingest) -> list[int]:
        return list(range(40))

    async def item(ctx: StepContext, value: int) -> int:
        return value  # pragma: no cover - the pins claim, never execute

    @app.workflow("wf_mixed_deep")
    def deep() -> Promise[object]:
        source = step(src, Ingest(doc_id="d1"), key="src", queue=queue)
        return build(map_source(source, item, queue=queue))

    @app.workflow("wf_mixed_shallow")
    def shallow() -> Promise[object]:
        return build(step(src, Ingest(doc_id="d2"), key="only", queue=queue))

    deep_runner = FlowRunner(app.get("wf_mixed_deep"), wf_pool, wf_schema)
    deep_flow = (await deep_runner.create_flow()).flow_id
    await deep_runner.tick(deep_flow)
    shallow_runner = FlowRunner(app.get("wf_mixed_shallow"), wf_pool, wf_schema)
    shallow_flow = (await shallow_runner.create_flow()).flow_id

    claimed: list[asyncpg.Record] = []
    for _round in range(3):
        got = await _dispatch(
            wf_conn, DISPATCH_ROUND_ROBIN_SQL, wf_schema, worker_id, queue, limit=4
        )
        claimed.extend(got)
        flows = _flow_ids(claimed)
        if str(shallow_flow) in flows:
            break

    flows = _flow_ids(claimed)
    assert str(shallow_flow) in flows, "the shallow run's row surfaced"
    assert str(deep_flow) in flows, "the deep run's cohort surfaced"
    vanilla = [r for r in claimed if r["fairness_key"] == "vanilla-team"]
    assert vanilla, "the vanilla keyed cohort surfaced beside the wf cohorts"


# ── the re-pend write-once leg ───────────────────────────────────────────


async def test_a_re_pended_row_keeps_its_stamp(
    wf_schema: str, wf_conn: asyncpg.Connection, wf_pool: asyncpg.Pool
) -> None:
    """The re-pend paths never touch the column: a claimed wf row that
    comes back through the REAL deferral write (the backend's
    ``mark_snoozed`` — the row goes ``scheduled`` with
    ``assignment_routed`` set) re-enters the round through the
    rr_tail_keys REPENDED arm the routing contract names, and it keeps
    the stamp it was born with."""
    from taskq.backend._sweeps import sweep_scheduled_to_pending
    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

    queue = "wf-repend-q"
    await _seed_queue(wf_conn, wf_schema, queue, "round_robin")
    worker_id = await _capable_worker_row(wf_conn, wf_schema, queue)

    app = WorkflowApp()

    async def body(ctx: StepContext, params: Ingest) -> str:
        return params.doc_id  # pragma: no cover - the pins claim, never execute

    @app.workflow("wf_repend_stamp")
    def w() -> Promise[object]:
        return build(step(body, Ingest(doc_id="d1"), key="first", queue=queue))

    runner = FlowRunner(app.get("wf_repend_stamp"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    got = await _dispatch(wf_conn, DISPATCH_ROUND_ROBIN_SQL, wf_schema, worker_id, queue, limit=4)
    rows = list(got)
    assert rows, "the round claimed the row"
    row = rows[0]
    assert str(row["fairness_key"]) == str(flow_id), "the row was born stamped"

    # THE REAL DEFERRAL WRITE: the backend's mark_snoozed (the claimed
    # row's own attempt/epoch fences it) — the row goes scheduled with
    # assignment_routed set, the re-pend shape the repended arm serves.
    settings = WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "schema_name": wf_schema,
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("wf_fairness_pins"),
        }
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=wf_pool,
        heartbeat_pool=wf_pool,
        worker_pool=wf_pool,
        notify_conn=None,
        leader_conn=None,
    )
    backend = PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )
    outcome = await backend.mark_snoozed(
        JobId(row["id"]),
        worker_id,
        timedelta(seconds=1),
        attempt=int(row["attempt"]),
        claim_epoch=int(row["claim_epoch"]),
    )
    assert outcome == "scheduled", f"the deferral write scheduled the row; got {outcome!r}"
    routed = await wf_conn.fetchval(
        f'SELECT assignment_routed FROM "{wf_schema}".jobs WHERE id = $1',  # Why: the schema is the module fixture's validated identifier; every value is $-bound.
        row["id"],
    )
    assert routed, "the deferral set the repended arm's discriminator"

    # THE PROMOTION (the leader's own arm): after the deferral elapses,
    # the due scheduled row comes back pending; the round re-admits it
    # through the REPENDED arm.
    import asyncio

    await asyncio.sleep(1.1)
    async with wf_pool.acquire() as conn:
        promoted = await sweep_scheduled_to_pending(conn, schema=wf_schema)
    assert promoted >= 1, "the due scheduled row promoted"
    again = await _dispatch(wf_conn, DISPATCH_ROUND_ROBIN_SQL, wf_schema, worker_id, queue, limit=4)
    reclaims = [r for r in again if str(r["id"]) == str(row["id"])]
    assert reclaims, "the re-pended row re-entered the round (the repended arm)"
    assert bool(reclaims[0]["assignment_routed"]), "the re-claim rode the repended arm"
    assert str(reclaims[0]["fairness_key"]) == str(flow_id), (
        "the re-pended row kept the stamp it was born with"
    )
