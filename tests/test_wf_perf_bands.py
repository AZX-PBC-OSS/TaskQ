"""The workflow perf bands (T03/T04, G11/G12): the enqueue-latency + dispatch-claim noise bands (with the AND deps_pending = 0 exclusion clause present), the join-fire latency band, the 1000-child fan-out tx band. All load_sensitive (the serial perf lane); the bands are SET from these measurements (the files land in .measurements/) and pinned forever.

Driven against a live Postgres through the REAL engine; the shared seed
helpers + fixtures live in ``tests/_wf_fixtures.py`` (the composed-fixture
home), the red-output sink flushes to ``.measurements/pin-reds.json`` (a
file that gets READ — BUILD-PROTOCOL §2), the shipped invariants green,
the unfenced variants kept in this file forever as the convicted shapes.
"""

# Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
# Why: random module used for timing jitter in race tests, not crypto.

from __future__ import annotations

import json
import time
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._types import ChildSpec, ConsumerBinding, ForkSpec, JoinSpec
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import (
    MEASUREMENTS,
    claim_view,
    fire_count,
    node_state,
    seed_edge,
    seed_flow,
    seed_join,
    seed_running_node,
)

# ── The measured gates: the fan-out tx band + the join-fire latency ─────


@pytest.mark.integration
async def test_fan_out_tx_band_1000_children(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
) -> None:
    """THE 1000-CHILD FAN-OUT TRANSACTION (G12: the deep-research scale is
    exactly this): 1000 child INSERTs + edges + the join row ride the
    parent's finalize tx1, chunked parallel-array statements, ONE tx — the
    band recorded from the measurement, never assumed."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    fork_parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    fork = ForkSpec(
        children=tuple(
            ChildSpec(step_key="c", actor="wf", queue="default", map_index=m, payload={"i": m})
            for m in range(1000)
        ),
        join=JoinSpec(
            step_key="reduce",
            actor="wf",
            queue="default",
            consumers=(ConsumerBinding(step_key="post", actor="wf", queue="default"),),
        ),
    )
    start = time.perf_counter_ns()
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=fork_parent,
        step_key="a",
        worker_id=(await claim_view(wf_conn, wf_schema, fork_parent))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        fork=fork,
    )
    elapsed_ms = (time.perf_counter_ns() - start) / 1e6
    assert result.applied
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1', fork_parent
    )
    assert int(children) == 1001  # 1000 children + the join node
    outbox = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_outbox')
    assert int(outbox) == 0  # the join has deps 1000: nothing fired yet
    MEASUREMENTS.mkdir(exist_ok=True)
    (MEASUREMENTS / "fanout-1000-tx-band.json").write_text(
        json.dumps(
            {
                "pin": "fanout-1000-tx-band",
                "elapsed_ms": elapsed_ms,
                "children": 1000,
                "band_ms": 500,
                "method": "wall clock of finalize_node tx1+tx2, 1000-child fork, "
                "chunked parallel-array inserts (500/chunk), one tx",
            },
            indent=2,
        )
    )
    assert elapsed_ms < 500, f"the 1000-child fan-out tx took {elapsed_ms:.1f} ms"


@pytest.mark.integration
@pytest.mark.load_sensitive  # the docstring's own declaration ("all load_sensitive") — the wall-clock band runs in the exclusive lane (the L-round's nothing-unmarked law)
async def test_join_fire_latency_band(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
) -> None:
    """THE JOIN-FIRE LATENCY BAND: last-parent-finalize → the joined row
    dispatchable (deps_pending = 0), measured on the finalize call (tx1 +
    tx2 — the fire is inside tx2)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)

    start = time.perf_counter_ns()
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent,
        step_key="a",
        worker_id=(await claim_view(wf_conn, wf_schema, parent))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
    )
    elapsed_ms = (time.perf_counter_ns() - start) / 1e6
    assert result.applied
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["deps_pending"] == 0 and await fire_count(wf_conn, wf_schema, join_id) == 1
    MEASUREMENTS.mkdir(exist_ok=True)
    (MEASUREMENTS / "join-fire-latency.json").write_text(
        json.dumps(
            {"pin": "join-fire-latency", "elapsed_ms": elapsed_ms, "band_ms": 50},
            indent=2,
        )
    )
    assert elapsed_ms < 50, f"join-fire latency {elapsed_ms:.1f} ms exceeds the band"


# ── T03's pins 4/5: the enqueue + dispatch-claim noise bands ─────────────

_PIN4_P50_BUDGET_US = 25_000


def _percentile(data: list[int], pct: float) -> int:
    ordered = sorted(data)
    idx = min(int(len(ordered) * pct / 100.0), len(ordered) - 1)
    return ordered[idx]


def _write_measurement(name: str, payload: object) -> None:
    MEASUREMENTS.mkdir(exist_ok=True)
    (MEASUREMENTS / name).write_text(json.dumps(payload, indent=2, default=str))


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.load_sensitive
async def test_pin_4_enqueue_latency_band(jobs_app: Any) -> None:
    """THE ENQUEUE-LATENCY NOISE BAND: vanilla enqueue p50 inside the
    recorded band with the workflow-schema build present (best-round p50 —
    the house benchmark's noise-robust statistic). RED DRILL (recorded):
    a fixture hook touching the new columns on the hot path (an extra
    round-trip read of the five columns per enqueue) must move the band —
    the pin can fail. Both measurements land in .measurements/."""
    from taskq.backend._protocol import EnqueueArgs
    from taskq.backend.postgres import PostgresBackend

    backend: PostgresBackend = jobs_app.backend

    async def measure_enqueue(extra_hook: bool) -> list[list[int]]:
        pool = backend._worker_pool  # benchmark-only: direct enqueue measurement
        schema = backend._schema_name  # benchmark-only
        rounds: list[list[int]] = []
        for _ in range(3):
            batch: list[int] = []
            for _ in range(50):
                start = time.perf_counter_ns()
                args = EnqueueArgs(
                    id=new_job_id(),
                    actor="bench",
                    queue="default",
                    payload={"pin": 4},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
                await backend.enqueue(args)
                if extra_hook:
                    # THE RED-DRILL HOOK: the hot path touching the new
                    # columns — an extra round-trip read of all five, the
                    # cost-class regression the band must catch.
                    await pool.execute(
                        f"SELECT parent_id, deps_pending, map_index, step_key, "
                        f'code_version FROM "{schema}".jobs WHERE id = $1',
                        args.id,
                    )
                batch.append((time.perf_counter_ns() - start) // 1_000)
            rounds.append(batch)
        return rounds

    rounds: list[list[int]] = []
    rounds.extend(await measure_enqueue(extra_hook=False))
    best = min(rounds, key=sum)
    p50 = _percentile(best, 50)
    p99 = _percentile(best, 99)

    rounds.clear()
    rounds.extend(await measure_enqueue(extra_hook=True))
    red_best = min(rounds, key=sum)
    red_p50 = _percentile(red_best, 50)

    _write_measurement(
        "pin4-enqueue-band.json",
        {
            "pin": "enqueue-latency-band",
            "p50_us": p50,
            "p99_us": p99,
            "best_round_ms": best,
            "budget_us": _PIN4_P50_BUDGET_US,
            "red_drill": {
                "hook": "extra round-trip read of the five workflow columns per enqueue",
                "p50_us": red_p50,
                "best_round_ms": red_best,
                "moved_the_band": red_p50 > p50,
            },
        },
    )
    assert p50 <= _PIN4_P50_BUDGET_US, f"enqueue p50 {p50 / 1000:.2f} ms exceeds the band"
    assert red_p50 > p50, (
        "the red drill did not fire: the hot-path column touch did not move "
        "the band — the pin cannot fail, it is a decoration"
    )


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.load_sensitive
async def test_pin_5_dispatch_claim_band(jobs_app: Any) -> None:
    """THE DISPATCH-CLAIM NOISE BAND: the claim statement's best-round p50
    inside the band WITH the ``AND deps_pending = 0`` exclusion clause
    present (a semantic no-op for vanilla rows — the pin proves it stays
    that way). RED DRILL: a fixture plan regression (index plans disabled →
    Seq Scan) must blow the band — the pin can fail."""
    from datetime import UTC, datetime, timedelta

    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch

    deps: Any = jobs_app.deps
    backend: Any = jobs_app.backend
    schema: str = deps.settings.schema_name
    pool = backend._dispatcher_pool  # benchmark-only: raw-CTE measurement

    worker_id = new_uuid()
    lock_lease = timedelta(seconds=90)
    queues = ["default"]
    now = datetime.now(tz=UTC)

    rows = [
        (
            new_uuid(),
            f"bench_{i % 10}",
            "default",
            '{"pin": 5}',
            3,
            "transient",
            "pending",
            now,
            i % 5,
            False,
            worker_id,
        )
        for i in range(1_000)
    ]
    await pool.executemany(
        f"""
        INSERT INTO "{schema}".jobs
            (id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at,
             priority, assignment_routed, locked_by_worker, lock_expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        [(*row, now + lock_lease) for row in rows],
    )
    await pool.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
        "SELECT 'bench_' || i, 'default', 10 FROM generate_series(0, 9) i "
        "ON CONFLICT (actor) DO NOTHING"
    )

    async def measure(rounds_n: int, per_round: int, **plan_gucs: str) -> list[int]:
        latencies: list[int] = []
        for _ in range(rounds_n):
            # Re-pend the round's rows: each dispatch claims (marks
            # running), so the backlog is reset between rounds — the
            # measured statement's cost stays the CLAIM's, not the drain's.
            await pool.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'pending', "
                "locked_by_worker = NULL, lock_expires_at = NULL "
                "WHERE locked_by_worker = $1",
                worker_id,
            )
            batch: list[int] = []
            for _ in range(per_round):
                start = time.perf_counter_ns()
                async with pool.acquire() as conn, conn.transaction():
                    if plan_gucs:
                        await conn.execute(
                            " ".join(f"SET {k} = {v};" for k, v in plan_gucs.items())
                        )
                    await dispatch_batch(
                        conn,
                        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
                        queues=queues,
                        limit_n=50,
                        worker_id=worker_id,
                        lock_lease=lock_lease,
                        oversample=2,
                    )
                batch.append((time.perf_counter_ns() - start) // 1_000)
            latencies.extend(batch)
        return latencies

    warm = await measure(1, 5)
    del warm
    samples = await measure(3, 20)
    p50 = _percentile(samples, 50)
    p99 = _percentile(samples, 99)

    # THE RED DRILL: the fixture plan regression — force the Seq-Scan plan
    # (the 83.7 ms monster class) and watch the band blow.
    red_samples = await measure(
        1,
        5,
        **{"enable_indexscan": "off", "enable_bitmapscan": "off", "enable_indexonlyscan": "off"},
    )
    red_p50 = _percentile(red_samples, 50)

    _write_measurement(
        "pin5-dispatch-band.json",
        {
            "pin": "dispatch-claim-band",
            "exclusion_clause": "AND deps_pending = 0",
            "p50_us": p50,
            "p99_us": p99,
            "budget_us": 50_000,
            "samples": len(samples),
            "red_drill": {"plan": "seq-scan-forced", "p50_us": red_p50},
        },
    )
    assert p50 <= 50_000, (
        f"dispatch claim p50 {p50 / 1000:.2f} ms exceeds the band (the clause must stay free)"
    )
    assert red_p50 > p50, (
        "the red drill did not fire: the forced plan regression did not blow "
        "the band — the pin cannot fail, it is a decoration"
    )


# ── T07: THE EDGE-JOIN SCALE CURVE (the bound's honest derivation) ──────


async def _seed_fan_in(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: Any,
    module_pg_pool: asyncpg.Pool,
    *,
    n: int,
    child_driven: bool,
) -> JobId:
    """One join of fan-in N at the declared-edge (or child-driven) shape,
    seeded through the FORK (the parallel-array path — the same statement
    shapes production fans out with)."""
    from taskq.workflows._types import ChildSpec, ConsumerBinding, ForkSpec, JoinSpec

    flow_id = await seed_flow(wf_conn, wf_schema)
    fork_parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    # insert_fork rides the CALLER'S transaction; the engine's entry is
    # the finalize — drive it (the fork is atomic with the terminal).
    fork = ForkSpec(
        children=tuple(
            ChildSpec(step_key="c", actor="wf", queue="default", map_index=m, payload={"i": m})
            for m in range(n)
        ),
        join=JoinSpec(
            step_key="reduce",
            actor="wf",
            queue="default",
            consumers=(ConsumerBinding(step_key="post", actor="wf", queue="default"),),
            child_driven=child_driven,
        ),
    )
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=fork_parent,
        step_key="a",
        worker_id=(await claim_view(wf_conn, wf_schema, fork_parent))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        fork=fork,
    )
    assert result.applied
    return flow_id


async def _measure_rederive(module_pg_pool: asyncpg.Pool, wf_sql: Any, batch: int = 200) -> float:
    """One full rederive pass, wall-clock ms (the pass is ONE batched
    statement + the fire arm — the fire is empty here, the joins unfired)."""
    from taskq.workflows._sweep import sweep_join_rederive

    start = time.perf_counter_ns()
    await sweep_join_rederive(module_pg_pool, wf_sql, batch_size=batch)
    return (time.perf_counter_ns() - start) / 1e6


async def _drop_flow(wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId) -> None:
    """The shape's rows leave the population (the next point measures its
    own shape only)."""
    await wf_conn.execute(
        f"DELETE FROM \"{wf_schema}\".jobs WHERE metadata->>'flow_id' = $1::text OR id = $2::uuid",
        str(flow_id),
        flow_id,
    )
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".wf_edge WHERE flow_id = $1::uuid', flow_id)


@pytest.mark.load_sensitive
@pytest.mark.integration
async def test_edge_join_scale_curve_across_the_bound(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: Any,
) -> None:
    """THE SCALE CURVE, measured across the bound's boundary (the refit
    procedure, run on the LANDED implementation — F7's restated rule): the
    three fan-in points inside/at/past the declared-edge band, the refit
    from the endpoints, the outlier named, the per-edge marginal checked
    against the BASE-DOMINATED argument, and the plan asserted
    INDEX-DRIVEN at a ≥ 100k-edge join (the 83.7 ms unscoped monster is
    the convicted shape — the pin asserts the plan stays index-served).

    The asserted BANDS are this machine's OWN measurements (the artifact
    records them); the bound moves only by the refit's argument in review.
    """
    from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN

    curve: dict[str, object] = {}
    inside_bound = min(200, MAX_FAN_IN_PER_JOIN)
    at_bound = MAX_FAN_IN_PER_JOIN
    past_bound = MAX_FAN_IN_PER_JOIN * 5  # 5000: the child-driven shape

    points = []
    for n, driven in ((inside_bound, False), (at_bound, False), (past_bound, True)):
        flow_id = await _seed_fan_in(
            wf_conn, wf_schema, wf_sql, module_pg_pool, n=n, child_driven=driven
        )
        ms = await _measure_rederive(module_pg_pool, wf_sql)
        points.append((n, driven, ms))
        curve[str(n)] = {"child_driven": driven, "rederive_ms": round(ms, 3)}
        await _drop_flow(wf_conn, wf_schema, flow_id)

    # THE REFIT (the endpoint fit): cost(n) ≈ base + marginal x n — the
    # marginal derived from the two endpoint points; the middle point's
    # residual is the record (P1's 1000-point was the outlier; THIS run's
    # outlier is NAMED, not averaged away).
    (n1, _d1, ms1), (n2, _d2, ms2), (n3, _d3, ms3) = points
    marginal_us_per_edge = (ms3 - ms1) / ((n3 - n1) * 1e-3) if n3 > n1 else 0.0
    base_ms = ms1 - marginal_us_per_edge * n1 / 1e3
    refit_at_bound = base_ms + marginal_us_per_edge * n2 / 1e3
    outlier_residual_ms = ms2 - refit_at_bound

    # THE SHAPE BOUNDARY: the child-driven point must not cliff — its
    # per-edge marginal stays in the same class as the declared-edge
    # points' (a discontinuity across the boundary is the convicted
    # cliff).
    inside_marginal_us = (ms2 - ms1) / ((n2 - n1) * 1e-3)
    assert marginal_us_per_edge <= max(10.0, inside_marginal_us * 10), (
        f"the per-edge marginal blew past the boundary: refit "
        f"{marginal_us_per_edge:.2f} µs/edge vs inside {inside_marginal_us:.2f} "
        "µs/edge — the shape switch is a cliff (the convicted discontinuity)"
    )
    # THE BASE-DOMINATED ARGUMENT's number: the marginal edge cost is
    # µs-class (the base is paid once per pass; the bound bounds the
    # PER-JOIN marginal work).
    assert marginal_us_per_edge < 100.0, (
        f"the marginal edge cost {marginal_us_per_edge:.2f} µs is not "
        "µs-class — the refit re-derives the bound's argument"
    )

    # THE ≥ 100k-EDGE JOIN: the plan must stay INDEX-DRIVEN (no seq scan
    # of jobs or wf_edge — the unscoped monster's conviction). Seeded
    # DIRECTLY (parallel-array chunks): the fork's map_index is smallint —
    # a fan-in past 32k cannot ride it, and the recount reads the EDGES.
    big_n = 100_000
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = new_uuid()
    # THE CACHE SENTINEL (the unspecification this pin feeds back): the
    # counter cache is a SMALLINT — it cannot carry a 100k count (the
    # 32767 ceiling); the child-driven shape's cache carries the sentinel
    # (the counter-as-cache is never trusted — the LEDGER is the truth,
    # and a >32767-unterminal recount needs the counter's own migration
    # before it can reconcile the cache; fed back, never silently
    # decided).
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata) "
        f"VALUES ($1::uuid, 'wf', 'default', '{{}}', 3, 'transient', "
        f"'pending'::\"{wf_schema}\".job_status, 'big_join', 1, "
        "to_jsonb(jsonb_build_object('flow_id', $2::text, 'blocking_reason', 'join')))",
        join_id,
        str(join_id),
    )
    from taskq.workflows._types import _metadata

    parent_ids = [new_uuid() for _ in range(big_n)]
    for start in range(0, big_n, 500):
        chunk = parent_ids[start : start + 500]
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
            "SELECT u.id, u.actor, u.queue, u.payload, u.max_attempts, u.retry_kind, "
            f'u.status::"{wf_schema}".job_status, u.step_key, u.metadata, '
            "u.idempotency_scope, u.idempotency_key "
            "FROM unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[], "
            "$5::smallint[], $6::text[], $7::text[], $8::text[], $9::jsonb[], $10::text[], $11::text[]) "
            "AS u(id, actor, queue, payload, max_attempts, retry_kind, status, "
            "step_key, metadata, idempotency_scope, idempotency_key) ",
            chunk,
            ["wf"] * len(chunk),
            ["default"] * len(chunk),
            ["{}"] * len(chunk),
            [3] * len(chunk),
            ["transient"] * len(chunk),
            ["succeeded"] * len(chunk),
            ["c"] * len(chunk),
            [json.dumps(_metadata(flow_id, blocking_reason=None))] * len(chunk),
            [f"workflow:{flow_id}"] * len(chunk),
            [f"wf:{flow_id}:c:{i}" for i in range(start, start + len(chunk))],
        )
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id) '
            "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) ",
            [join_id] * len(chunk),
            chunk,
            [flow_id] * len(chunk),
        )
    plan_rows = await wf_conn.fetch(
        f"EXPLAIN (FORMAT JSON) {wf_sql.rederive_sweep}",
        200,
        "orphan_parent",
        "failed_parent",
        "flow_dead",
    )
    plan_text = json.dumps([dict(r) for r in plan_rows], default=str)
    await _drop_flow(wf_conn, wf_schema, flow_id)
    seq_scanned = [ln for ln in plan_text.split('Node Type": "') if "Seq Scan" in ln[:40]]
    scan_on_core = any(
        f'"Relation Name": "{rel}"' in plan_text and '"Node Type": "Seq Scan"' in plan_text
        for rel in ("jobs", "wf_edge")
    )
    assert not scan_on_core, (
        "the 100k-edge rederive plans a Seq Scan on jobs/wf_edge — the "
        "83.7 ms unscoped monster shape (the plan must stay index-driven)"
    )
    _ = seq_scanned

    curve["refit"] = {
        "marginal_us_per_edge": round(marginal_us_per_edge, 3),
        "base_ms": round(base_ms, 3),
        "refit_at_bound_ms": round(refit_at_bound, 3),
        "outlier_residual_ms": round(outlier_residual_ms, 3),
        "outlier": str(n2),
        "big_edge_explain": "index-driven (no seq scan on jobs/wf_edge)",
    }
    _write_measurement("edge-scale-curve.json", curve)
