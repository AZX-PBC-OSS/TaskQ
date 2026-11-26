"""The flow root is never in flight against its actor's cap (#747).

Every run's root row (``step_key = '__flow__'``) lives on the default
node actor ``wf`` and sits in ``running`` for the life of the run
(ROOT_START flips it in the create's own transaction). The root is a
cache, never work: the dispatch fence's ROOT-MARKER leg refuses to
claim it. The per-actor cap's running count (``capped_running`` in
``backend/_dispatch_sql.py``) must make the same exception: a live
run's root is not a unit of ``wf``'s ``max_concurrent``. Counting it
lets N live runs fill a cap of N with roots, and no step of any of them
is ever claimed; the runs stay pending with no error and no log line.

The pins drive the fleet's own claim statement (``dispatch_batch`` over
the rendered ``DISPATCH_*_SQL``, the statement every worker's producer
runs), because the in-process runner's claimable read carries no cap
leg and cannot see the defect.

* the red pin: N live runs under a ``wf`` cap of N claim their N steps;
* the harness control: the same fixture uncapped claims the same rows
  (the cap is the only refuser the red pin isolates);
* the preservation pin: the cap still counts running STEPS (a fix that
  stops counting workflow rows altogether over-admits);
* the NULL-safety pin: a vanilla actor's running rows (``step_key`` is
  NULL) still count (a bare ``step_key <> '__flow__'`` evaluates NULL on
  them and uncaps every vanilla actor);
* the re-pended pin: a ``wf`` step re-pended by a deferral, an emit park
  or wake, a reclaim or a drain is admitted through ``repend_capacity``,
  the second reader the roots filled;
* the post-lock clamp pin: the ``eligible`` re-limit still cuts the
  locked rows to ``max_concurrent - in_flight`` with steps running;
* the scope pin: the root subtraction applies to ``wf`` only, never to a
  capped vanilla actor beside live roots;
* the terminal-root pin: a FINISHED run's root is not running, so it is
  never subtracted (dropping ``rf.status = 'running'`` over-admits by the
  number of terminal roots in the retention window);
* the placement pin: a workflow's declared ``actor=`` moves its loop rows,
  but its root still lives on ``wf``;
* the live-root JIT oracle: the root subtraction is priced once per
  statement, so 10 000 live roots on all-visible pages keep the claim's
  estimate under ``jit_above_cost`` (a count correlated to the driver's
  actor that reads ``step_key`` is priced once per driver row and
  crosses it).

Every pin runs over the three rendered statements: strict FIFO, strict
FIFO with a claim-cursor bound, and round robin.
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: the only interpolations are {schema} (a fixture-derived identifier, not user input), the catalog-derived double-quoted {col_list}/{filler_list} column identifiers from this schema's own information_schema.columns, and the filler dict's five constant SQL literals ('wf_root_cap_filler', 'wf_root_cap_filler_q', 'pending', NULL, '{}'::jsonb); every caller value is $-bound.
import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import (
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_CURSOR_SQL,
    DISPATCH_STRICT_FIFO_SQL,
    dispatch_batch,
)
from taskq.backend._protocol import JobId
from taskq.testing.pg import create_pending_job, create_running_job
from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    step,
)

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "default"
_FLOW = "wf_root_cap_flow"
_TWO_STEP_FLOW = "wf_root_cap_two_step_flow"
_PLACED_FLOW = "wf_root_cap_placed_flow"

_VARIANTS = [
    pytest.param("strict_fifo", DISPATCH_STRICT_FIFO_SQL, id="strict_fifo"),
    pytest.param("strict_fifo", DISPATCH_STRICT_FIFO_CURSOR_SQL, id="strict_fifo_cursor"),
    pytest.param("round_robin", DISPATCH_ROUND_ROBIN_SQL, id="round_robin"),
]
# The cursor render binds $6; the zero UUID is the lowest bound, so it
# admits every row.
_NO_CURSOR_BOUND = UUID(int=0)


# ── the pinned graph's bodies (module-level: the compile resolves the
#    annotations from the defining globals). The pins claim, never execute.


async def _only(ctx: StepContext) -> int:
    return 1  # pragma: no cover - the pins claim, never execute


async def _left(ctx: StepContext) -> int:
    return 1  # pragma: no cover - the pins claim, never execute


async def _right(ctx: StepContext) -> int:
    return 2  # pragma: no cover - the pins claim, never execute


async def _join(ctx: StepContext, left: int, right: int) -> int:
    return left + right  # pragma: no cover - the pins claim, never execute


async def _placed_loop_body(ctx: StepContext, carry: object) -> Done[int] | Refine[int]:
    return Done(1)  # pragma: no cover - the pins claim, never execute


def _app() -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow(_FLOW)
    def one_step() -> Promise[object]:  # pyright: ignore[reportUnusedFunction]  # Why: registered by the decorator, read back through app.get.
        return build(step(_only, key="only"))

    @app.workflow(_TWO_STEP_FLOW)
    def two_steps() -> Promise[object]:  # pyright: ignore[reportUnusedFunction]  # Why: registered by the decorator, read back through app.get.
        # left and right are both born due (deps 0); the join waits on both.
        return build(step(_join, step(_left, key="left"), step(_right, key="right"), key="join"))

    @app.workflow(_PLACED_FLOW, actor="wf_root_cap_placed")
    def placed() -> Promise[object]:  # pyright: ignore[reportUnusedFunction]  # Why: registered by the decorator, read back through app.get.
        # A loop node carries the definition's declared placement
        # (``graph.declared_actor``), so this workflow has a row that
        # actually lands on ``wf_root_cap_placed`` — the treatment the
        # placement pin asserts beside the root's ``wf``.
        return build(loop("placed_loop", _placed_loop_body, max_iterations=2))

    return app


async def _seed(conn: asyncpg.Connection, schema: str, mode: str, cap: int | None) -> UUID:
    """The queue's mode row, ``wf``'s stored cap (UPSERT so ``_seed`` is
    idempotent across repeat calls and the cap stays authoritative
    regardless of what seeded the row), and one workflow_execution-capable
    worker on the queue (the dispatch fence's execution leg admits wf rows
    only for it)."""
    await conn.execute(
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET mode = $2",
        _QUEUE,
        mode,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
        "VALUES ('wf', $1, $2) "
        "ON CONFLICT (actor) DO UPDATE SET queue = $1, max_concurrent = $2",
        _QUEUE,
        cap,
    )
    stored = await conn.fetchval(
        f"SELECT max_concurrent FROM \"{schema}\".actor_config WHERE actor = 'wf'"
    )
    assert stored == cap, f"wf's stored cap reads back {stored!r}, seeded {cap!r}"
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'wf-root-cap-pin', 1, $2, $3::jsonb)",
        worker_id,
        [_QUEUE],
        json.dumps({"workflow_execution": True}),
    )
    return worker_id


async def _create_runs(pool: asyncpg.Pool, schema: str, flow: str, n: int) -> list[JobId]:
    """N live runs through the shipped create (root + nodes + edges +
    ROOT_START in one transaction), never a hand-built root."""
    runner = FlowRunner(_app().get(flow), pool, schema)
    return [(await runner.create_flow()).flow_id for _ in range(n)]


async def _wf_running(conn: asyncpg.Connection, schema: str) -> list[str]:
    rows = await conn.fetch(
        f"SELECT step_key FROM \"{schema}\".jobs WHERE actor = 'wf' AND status = 'running' "
        "ORDER BY step_key"
    )
    return [str(r["step_key"]) for r in rows]


async def _dispatch(
    conn: asyncpg.Connection, sql: str, schema: str, worker_id: UUID, limit: int = 10
) -> list[asyncpg.Record]:
    return await dispatch_batch(
        conn,
        sql=sql.format(schema=schema),
        queues=[_QUEUE],
        limit_n=limit,
        worker_id=worker_id,
        lock_lease=_LEASE,
        claim_cursor=_NO_CURSOR_BOUND if "$6" in sql else None,
    )


async def _assert_roots_live(conn: asyncpg.Connection, schema: str, flows: list[JobId]) -> None:
    """The precondition the defect needs: every run's root is ``running``
    on ``wf``, and nothing else of ``wf`` is."""
    roots = await conn.fetch(
        f'SELECT actor, status::text AS status FROM "{schema}".jobs '
        "WHERE id = ANY($1::uuid[]) AND step_key = '__flow__'",
        flows,
    )
    assert len(roots) == len(flows), roots
    assert all(r["actor"] == "wf" and r["status"] == "running" for r in roots), roots
    assert await _wf_running(conn, schema) == ["__flow__"] * len(flows)


# ── the red pin ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("runs", [1, 3])
@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_live_roots_do_not_fill_the_wf_cap(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
    runs: int,
) -> None:
    """N live runs under ``wf`` ``max_concurrent = N``: the N roots are
    running, each run's one step is pending and due. One dispatch round
    must claim the N steps. Counting the roots leaves a residual of 0 and
    the round claims nothing, every round, forever."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=runs)
    flows = await _create_runs(wf_pool, wf_schema, _FLOW, runs)
    await _assert_roots_live(wf_conn, wf_schema, flows)

    claimed = await _dispatch(wf_conn, sql, wf_schema, worker_id)

    assert sorted(str(r["step_key"]) for r in claimed) == ["only"] * runs, (
        f"{mode}: {runs} live run(s) under wf max_concurrent={runs} claimed "
        f"{[r['step_key'] for r in claimed]} — the running roots are counted "
        "as wf's in-flight work, so the cap is full before any step is claimed"
    )


# ── the harness control (green on HEAD) ──────────────────────────────────


@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_control_the_same_runs_uncapped_claim_their_steps(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
) -> None:
    """The red pin's fixture with ``wf`` uncapped claims the same rows:
    the capability row, the queue label and the fence admit them, so the
    red pin's empty round is the cap's refusal and nothing else."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=None)
    flows = await _create_runs(wf_pool, wf_schema, _FLOW, 3)
    await _assert_roots_live(wf_conn, wf_schema, flows)

    claimed = await _dispatch(wf_conn, sql, wf_schema, worker_id)

    assert sorted(str(r["step_key"]) for r in claimed) == ["only"] * 3, (
        f"{mode}: the uncapped control claimed {[r['step_key'] for r in claimed]}"
    )


# ── the preservation pin: running steps still fill the cap ───────────────


@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_running_steps_still_fill_the_wf_cap(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
) -> None:
    """One run, two independent pending steps, ``wf`` cap 1. The first
    round claims exactly one step (the root takes no unit); the second
    round claims nothing, because the claimed step is running and holds
    the cap's only unit. A count that drops every workflow row (or the
    count altogether) over-admits here."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=1)
    flows = await _create_runs(wf_pool, wf_schema, _TWO_STEP_FLOW, 1)
    await _assert_roots_live(wf_conn, wf_schema, flows)

    first = await _dispatch(wf_conn, sql, wf_schema, worker_id)
    assert len(first) == 1 and first[0]["step_key"] in ("left", "right"), (
        f"{mode}: round 1 under wf cap 1 claimed {[r['step_key'] for r in first]} "
        "— expected exactly one step (the live root is not in flight)"
    )

    second = await _dispatch(wf_conn, sql, wf_schema, worker_id)
    assert second == [], (
        f"{mode}: round 2 claimed {[r['step_key'] for r in second]} while "
        f"{first[0]['step_key']!r} is running under wf cap 1 — the cap stopped "
        "counting running workflow steps"
    )
    running = await _wf_running(wf_conn, wf_schema)
    assert sorted(running) == sorted(["__flow__", str(first[0]["step_key"])]), running


# ── the NULL-safety pin: vanilla running rows still count ─────────────────


@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_vanilla_running_rows_still_fill_their_actor_cap(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    mode: str,
    sql: str,
) -> None:
    """A vanilla actor (``step_key`` NULL on every row) at its cap: one
    running row under ``max_concurrent = 1`` and one due pending row.
    The round claims nothing. A root exclusion spelled
    ``step_key <> '__flow__'`` is NULL on these rows, drops them from the
    count, and admits the pending row."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=None)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".actor_config SET max_concurrent = 1 WHERE actor = 'test_actor'"
    )
    await create_running_job(wf_conn, wf_schema, worker_id, with_events=False)
    pending = await create_pending_job(
        wf_conn, wf_schema, scheduled_at=datetime.now(UTC) - timedelta(minutes=5)
    )
    step_keys = await wf_conn.fetch(
        f"SELECT step_key FROM \"{wf_schema}\".jobs WHERE actor = 'test_actor'"
    )
    assert [r["step_key"] for r in step_keys] == [None, None], step_keys

    claimed = await _dispatch(wf_conn, sql, wf_schema, worker_id)
    assert claimed == [], (
        f"{mode}: the vanilla actor at max_concurrent=1 with one running row "
        f"claimed {[str(r['id']) for r in claimed]} (pending {pending}) — its "
        "running rows dropped out of the cap's count"
    )

    # the control half: lift the cap, the same row is claimable.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".actor_config SET max_concurrent = 2 WHERE actor = 'test_actor'"
    )
    lifted = await _dispatch(wf_conn, sql, wf_schema, worker_id)
    assert [r["id"] for r in lifted] == [pending], lifted


# ── the re-pended pin: repend_capacity reads the same count ──────────────


@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_a_repended_step_is_not_starved_by_live_roots(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
) -> None:
    """One live run under ``wf`` cap 1, its step re-pended
    (``assignment_routed``, the state a deferral, an emit park or wake, a
    lease reclaim or a drain hand-back leaves). The re-pended arm admits
    through ``repend_capacity``, whose residual the root filled; one
    round must claim the step."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=1)
    flows = await _create_runs(wf_pool, wf_schema, _FLOW, 1)
    await _assert_roots_live(wf_conn, wf_schema, flows)
    repended = await wf_conn.fetchval(
        f'WITH u AS (UPDATE "{wf_schema}".jobs SET assignment_routed = true '
        "WHERE actor = 'wf' AND step_key = 'only' AND status = 'pending' RETURNING 1) "
        "SELECT count(*) FROM u"
    )
    assert repended == 1, repended

    claimed = await _dispatch(wf_conn, sql, wf_schema, worker_id)

    assert [str(r["step_key"]) for r in claimed] == ["only"], (
        f"{mode}: the re-pended step under wf cap 1 claimed "
        f"{[r['step_key'] for r in claimed]} — the live root fills repend_capacity's residual"
    )


# ── the post-lock clamp pin: the re-limit reads in_flight ────────────────


@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_running_steps_bound_the_post_lock_relimit(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
) -> None:
    """Two runs of the two-step flow: 2 roots, 4 due steps. Round 1 at cap
    1 claims one step. Raised to cap 2, round 2 has one step in flight
    and a residual of 1, so its window (``residual * oversample = 2``)
    locks two rows and the ``eligible`` re-limit must cut them to one.
    This is the only shape with ``0 < in_flight < cap`` and more locked
    rows than residual."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=1)
    flows = await _create_runs(wf_pool, wf_schema, _TWO_STEP_FLOW, 2)
    await _assert_roots_live(wf_conn, wf_schema, flows)

    first = await _dispatch(wf_conn, sql, wf_schema, worker_id)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".actor_config SET max_concurrent = 2 WHERE actor = 'wf'"
    )
    second = await _dispatch(wf_conn, sql, wf_schema, worker_id)

    assert (len(first), len(second)) == (1, 1), (
        f"{mode}: rounds claimed {[r['step_key'] for r in first]} then "
        f"{[r['step_key'] for r in second]} — expected one step each: round 2 "
        "has one step in flight under cap 2"
    )


# ── the scope pin: only wf's roots are subtracted, only from wf ──────────


@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_live_roots_are_not_subtracted_from_other_actors(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
) -> None:
    """``wf`` uncapped; the vanilla ``test_actor`` at cap 1 with one
    running row and one due pending row; three live runs. The round
    claims the three steps and never the vanilla row: the root
    subtraction belongs to ``wf``'s count alone, and subtracting the
    three roots from ``test_actor``'s count would admit its pending row."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=None)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".actor_config SET max_concurrent = 1 WHERE actor = 'test_actor'"
    )
    await create_running_job(wf_conn, wf_schema, worker_id, with_events=False)
    pending = await create_pending_job(
        wf_conn, wf_schema, scheduled_at=datetime.now(UTC) - timedelta(minutes=5)
    )
    flows = await _create_runs(wf_pool, wf_schema, _FLOW, 3)
    await _assert_roots_live(wf_conn, wf_schema, flows)

    claimed = await _dispatch(wf_conn, sql, wf_schema, worker_id)

    assert pending not in [r["id"] for r in claimed], (
        f"{mode}: test_actor at max_concurrent=1 with one running row admitted "
        "its pending row — wf's live roots were subtracted from its count"
    )
    assert sorted(str(r["step_key"]) for r in claimed) == ["only"] * 3, [
        r["step_key"] for r in claimed
    ]


# ── the terminal-root pin: only LIVE roots are subtracted ────────────────


async def _finish_run(conn: asyncpg.Connection, schema: str, flow_id: object) -> None:
    """Put a run's own rows into the terminal state every finished run
    holds: the root and its node ``succeeded`` (a node row links to its
    root through ``metadata->>'flow_id'``). Asserted, so a future schema
    change cannot make the pin pass by updating nothing."""
    finished = await conn.fetchval(
        f"WITH u AS (UPDATE \"{schema}\".jobs SET status = 'succeeded' "
        "WHERE id = $1::uuid OR (metadata->>'flow_id')::uuid = $1::uuid RETURNING 1) "
        "SELECT count(*) FROM u",
        flow_id,
    )
    assert finished == 2, (
        f"finished {finished!r} rows for run {flow_id} — expected the root and its node"
    )


@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_a_finished_runs_root_does_not_offset_the_wf_cap(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
) -> None:
    """One earlier run has finished (its root and node are terminal and
    still in the table); one two-step run is live under ``wf``
    ``max_concurrent = 1``. The live root takes no unit of the cap and the
    finished root is not running, so the residual is the full 1 and the
    round claims exactly one step. Subtracting every ``__flow__`` row
    regardless of status reads ``in_flight = -1`` and admits both."""
    worker_id = await _seed(wf_conn, wf_schema, mode, cap=1)
    finished = await _create_runs(wf_pool, wf_schema, _FLOW, 1)
    await _finish_run(wf_conn, wf_schema, finished[0])

    flows = await _create_runs(wf_pool, wf_schema, _TWO_STEP_FLOW, 1)
    await _assert_roots_live(wf_conn, wf_schema, flows)

    claimed = await _dispatch(wf_conn, sql, wf_schema, worker_id)

    assert len(claimed) == 1 and claimed[0]["step_key"] in ("left", "right"), (
        f"{mode}: one finished run beside one live run under wf max_concurrent=1 "
        f"claimed {[r['step_key'] for r in claimed]} — a finished run's root was "
        "subtracted from wf's running count, widening the cap by the number of "
        "terminal __flow__ rows in the table"
    )


# ── the root's actor ─────────────────────────────────────────────────────


async def test_a_placed_workflows_root_still_lives_on_wf(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """``capped_running`` subtracts roots on the literal ``'wf'`` only, which
    holds because every root the engine writes carries ``FlowEntryShim``'s
    actor. The placement face (``@app.workflow(actor=...)``) moves a
    definition's loop and escalation rows; this flow's loop row must land on
    ``wf_root_cap_placed`` while its root stays on ``wf``. Both halves are
    asserted, so deleting the placement declaration reds the treatment."""
    flows = await _create_runs(wf_pool, wf_schema, _PLACED_FLOW, 1)
    await _assert_roots_live(wf_conn, wf_schema, flows)

    rows = await wf_conn.fetch(
        f'SELECT step_key, actor FROM "{wf_schema}".jobs '
        "WHERE id = $1::uuid OR (metadata->>'flow_id')::uuid = $1::uuid",
        flows[0],
    )
    actors = {str(r["step_key"]): str(r["actor"]) for r in rows}
    assert actors == {"__flow__": "wf", "placed_loop": "wf_root_cap_placed"}, actors


# ── the live-root JIT oracle ─────────────────────────────────────────────

_ORACLE_ROOTS = 10_000
# Without fillers the table is small enough that a Seq Scan prices every
# count shape cheaply, and the oracle cannot tell them apart.
_ORACLE_FILLERS = 100_000


async def _vacuum_jobs(conn: asyncpg.Connection, schema: str) -> tuple[int, int]:
    """VACUUM the jobs table; return its (relpages, relallvisible)."""
    await conn.execute(f'VACUUM (INDEX_CLEANUP ON) "{schema}".jobs')
    row = await conn.fetchrow(
        "SELECT c.relpages, c.relallvisible FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND c.relname = 'jobs'",
        schema,
    )
    assert row is not None
    return int(row["relpages"]), int(row["relallvisible"])


@pytest.mark.parametrize("cap", [None, 5], ids=["uncapped", "capped"])
@pytest.mark.parametrize(("mode", "sql"), _VARIANTS)
async def test_live_roots_keep_the_claim_under_jit_above_cost(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    mode: str,
    sql: str,
    cap: int | None,
) -> None:
    """10 000 live roots on all-visible pages, beside 100 000 pending rows
    no worker polls. The claim statement's planner estimate stays under
    ``jit_above_cost``, capped or not.

    ``capped_running``'s count subplan is priced once per estimated driver
    row (about 202) even when its branch never runs. On all-visible pages
    the root-blind running count is an index-only scan; a count
    correlated to the driver's actor that also reads ``step_key`` is a
    heap scan priced 202 times, which crosses ``jit_above_cost`` from
    about 3 000 roots and JIT-compiles every round. The root subtraction
    is uncorrelated, an InitPlan priced once."""
    schema = wf_schema
    worker_id = await _seed(wf_conn, schema, mode, cap=cap)
    flows = await _create_runs(wf_pool, schema, _FLOW, 1)
    await _assert_roots_live(wf_conn, schema, flows)
    cols = [
        str(r["column_name"])
        for r in await wf_conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = 'jobs' AND is_generated = 'NEVER' "
            "AND column_name NOT IN ('id', 'idempotency_key') ORDER BY ordinal_position",
            schema,
        )
    ]
    col_list = ", ".join(f'"{c}"' for c in cols)
    filler = {
        "actor": "'wf_root_cap_filler'",
        "queue": "'wf_root_cap_filler_q'",
        "status": "'pending'",
        "step_key": "NULL",
        "metadata": "'{}'::jsonb",
    }
    filler_list = ", ".join(filler.get(c, f'"{c}"') for c in cols)
    try:
        await wf_conn.execute(
            f'INSERT INTO "{schema}".jobs (id, {col_list}) '
            f'SELECT gen_random_uuid(), {col_list} FROM "{schema}".jobs, generate_series(1, $2) '
            "WHERE id = $1",
            flows[0],
            _ORACLE_ROOTS - 1,
        )
        await wf_conn.execute(
            f'INSERT INTO "{schema}".jobs (id, {col_list}) '
            f'SELECT gen_random_uuid(), {filler_list} FROM "{schema}".jobs, '
            "generate_series(1, $2) WHERE id = $1",
            flows[0],
            _ORACLE_FILLERS,
        )
        roots = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE actor = 'wf' AND status = 'running' AND step_key = '__flow__'"
        )
        assert roots == _ORACLE_ROOTS, roots
        await wf_conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')
        # The estimate gap exists only on all-visible pages (where the
        # root-blind count is index-only). A plain VACUUM can leave LP_DEAD
        # items behind through the index-vacuum bypass.
        relpages, relallvisible = await _vacuum_jobs(wf_conn, schema)
        for _ in range(7):
            if relallvisible >= relpages:
                break
            await asyncio.sleep(0.5)
            relpages, relallvisible = await _vacuum_jobs(wf_conn, schema)
        assert relallvisible >= relpages - 1, (
            f"jobs is not all-visible ({relallvisible}/{relpages} pages) — the oracle "
            "measures nothing on pages the index-only count cannot use"
        )

        args: list[object] = [[_QUEUE], 10, worker_id, _LEASE, 2]
        if "$6" in sql:
            args.append(_NO_CURSOR_BOUND)
        # EXPLAIN ANALYZE executes the claim; the rollback undoes it.
        tr = wf_conn.transaction()
        await tr.start()
        try:
            await wf_conn.execute("SET LOCAL jit = on")
            # The default threshold, the one this oracle's arithmetic assumes.
            await wf_conn.execute("SET LOCAL jit_above_cost = 100000")
            above = float(await wf_conn.fetchval("SELECT current_setting('jit_above_cost')"))
            raw = await wf_conn.fetchval(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql.format(schema=schema), *args
            )
        finally:
            await tr.rollback()
    finally:
        # The G7 teardown reconstructs every root; a clone has no node rows.
        await wf_conn.execute(
            f"DELETE FROM \"{schema}\".jobs WHERE step_key = '__flow__' AND id <> $1",
            flows[0],
        )
    document: Any = json.loads(raw) if isinstance(raw, str) else raw
    top: dict[str, Any] = document[0]
    estimate = float(top["Plan"]["Total Cost"])
    assert estimate < above, (
        f"{mode}/cap={cap}: at {_ORACLE_ROOTS} live roots the claim's estimate "
        f"{estimate:.0f} is not under jit_above_cost {above:.0f} — every round "
        "JIT-compiles; a per-actor count that reads step_key is priced once per driver row"
    )
    assert top.get("JIT") is None, f"{mode}/cap={cap}: JIT fired: {top.get('JIT')}"
