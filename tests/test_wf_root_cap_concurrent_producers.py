"""Composed concurrency pin: two producers against a root-saturated ``wf``.

The #747 pins drive one producer's round at a time. This one drives two,
because removing the live roots from ``wf``'s in-flight count makes a
fleet-level path reachable that the pre-fix arithmetic kept shut: with
N live runs under a cap of N the residual was 0 for every producer, so
``wf`` never entered the capped candidate/lock path at all. With the
roots subtracted, every producer's round now enters it, and two of them
can overlap -- producer A holding its claimed rows' locks uncommitted
while producer B reads its own snapshot and takes its own locks.

What the overlap is pinned to:

* the round admits exactly the cap (the roots hold no unit of it, and
  the running STEPS still do);
* neither producer claims a flow root, and the two rounds never claim
  the same step (``FOR UPDATE SKIP LOCKED`` on the capped path's
  pre-lock window: the window is locked for the round's whole
  statement, so a peer's round is empty rather than a second
  over-admitting round -- the realized ceiling is therefore below the
  documented ``(num_producers - 1) * max_concurrent`` worst case);
* the steps simultaneously ``running`` after both rounds is within the
  documented worst case of ``producers * max_concurrent``;
* the round after the overlap admits nothing, i.e. the cap -- not the
  accidentally-counted roots -- is what damps a fleet-wide burst of
  claimable steps (the crash-recovery shape).
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier, not user input; every value is $-bound.
import json
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "default"
_FLOW = "wf_root_cap_concurrent_flow"
_CAP = 3
_RUNS = 6
_ROUND_LIMIT = 10


async def _only(ctx: StepContext) -> int:
    return 1  # pragma: no cover - the pin claims, never executes


def _app() -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow(_FLOW)
    def one_step() -> Promise[object]:  # pyright: ignore[reportUnusedFunction]  # Why: registered by the decorator, read back through app.get.
        return build(step(_only, key="only"))

    return app


async def _seed(conn: asyncpg.Connection, schema: str) -> list[UUID]:
    """The queue row, ``wf``'s stored cap, and one workflow_execution-
    capable worker per producer (the dispatch fence's EXECUTION leg
    admits ``wf`` rows only for a worker that registered itself capable)."""
    await conn.execute(
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET mode = $2",
        _QUEUE,
        "strict_fifo",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
        "VALUES ('wf', $1, $2) ON CONFLICT (actor) DO UPDATE SET queue = $1, max_concurrent = $2",
        _QUEUE,
        _CAP,
    )
    ids: list[UUID] = []
    for i in range(2):
        worker_id = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
            "VALUES ($1, $2, 1, $3, $4::jsonb)",
            worker_id,
            f"wf-root-cap-concurrent-{i}",
            [_QUEUE],
            json.dumps({"workflow_execution": True}),
        )
        ids.append(worker_id)
    return ids


async def _create_runs(pool: asyncpg.Pool, schema: str, n: int) -> list[JobId]:
    """N live runs through the shipped create (root + nodes + edges +
    ROOT_START in one transaction), never a hand-built root."""
    runner = FlowRunner(_app().get(_FLOW), pool, schema)
    return [(await runner.create_flow()).flow_id for _ in range(n)]


async def _dispatch(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> list[asyncpg.Record]:
    return await dispatch_batch(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=[_QUEUE],
        limit_n=_ROUND_LIMIT,
        worker_id=worker_id,
        lock_lease=_LEASE,
    )


async def _wf_running(conn: asyncpg.Connection, schema: str) -> int:
    value = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE actor = 'wf' AND status = 'running' AND step_key <> '__flow__'"
    )
    return int(value)


async def test_two_overlapping_producers_against_a_root_saturated_wf(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    workers = await _seed(wf_conn, wf_schema)
    flows = await _create_runs(wf_pool, wf_schema, _RUNS)

    roots = await wf_conn.fetch(
        f'SELECT actor, status::text AS status FROM "{wf_schema}".jobs '
        "WHERE id = ANY($1::uuid[]) AND step_key = '__flow__'",
        flows,
    )
    assert len(roots) == _RUNS, roots
    assert all(r["actor"] == "wf" and r["status"] == "running" for r in roots), (
        f"the defect's precondition is missing: the runs' roots are {roots!r}"
    )
    assert await _wf_running(wf_conn, wf_schema) == 0, (
        "no step of any run is running yet, so the cap is filled by roots alone"
    )

    # Two producers, two connections, overlapping transactions: B's round
    # runs while A still holds the rows it claimed uncommitted, which is
    # the interleaving the read-once-before-lock doctrine is stated for.
    async with (
        wf_pool.acquire() as conn_a,
        wf_pool.acquire() as conn_b,
        conn_a.transaction(),
    ):
        claims_a = await _dispatch(conn_a, wf_schema, workers[0])
        async with conn_b.transaction():
            claims_b = await _dispatch(conn_b, wf_schema, workers[1])

    assert len(claims_a) == _CAP, (
        f"a round claimed {len(claims_a)} steps against wf's cap of {_CAP} while "
        f"{_RUNS} live roots were running: the roots hold no unit of the cap, and "
        "the running steps still hold all of it"
    )
    claimed = [*(r["step_key"] for r in claims_a), *(r["step_key"] for r in claims_b)]
    assert all(key != "__flow__" for key in claimed), f"a round claimed a flow ROOT: {claimed!r}"

    ids_a = {r["id"] for r in claims_a}
    ids_b = {r["id"] for r in claims_b}
    assert not (ids_a & ids_b), (
        f"both producers claimed {sorted(map(str, ids_a & ids_b))}: one pending step "
        "was dispatched twice in one round"
    )
    assert len(ids_a | ids_b) <= 2 * _CAP, (
        f"the fleet admitted {len(ids_a | ids_b)} steps against wf's cap of {_CAP} "
        "in one round, past the documented (producers * max_concurrent) bound"
    )

    running_steps = await _wf_running(wf_conn, wf_schema)
    assert running_steps == len(ids_a | ids_b), running_steps
    assert running_steps <= 2 * _CAP, running_steps

    # The damping question a crash-recovery burst raises: with producers *
    # cap steps running beside the live roots, the next round must admit
    # nothing -- the cap corrects itself; the root count was never the
    # damper the fleet depended on.
    async with wf_pool.acquire() as conn_c:
        claims_c = await _dispatch(conn_c, wf_schema, workers[0])
    assert claims_c == [], (
        f"the round after the overlap claimed {[r['step_key'] for r in claims_c]} "
        f"with {running_steps} steps already running against wf's cap of {_CAP}: "
        "the cap does not damp a fleet-wide burst of claimable steps"
    )
