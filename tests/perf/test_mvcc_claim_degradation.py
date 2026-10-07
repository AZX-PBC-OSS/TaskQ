"""THE PROOF, MEASURED: the claim path's MVCC death spiral, reproduced
through the real claim statements, and the claim cursor's measured
scope on it.

The death spiral (brandur.org/postgres-queues, "Postgres Job Queues +
Failure By MVCC"; re-run by PlanetScale's 2026 "Keeping a Postgres
queue healthy"): a long/overlapping transaction pins the MVCC horizon,
VACUUM cannot reclaim, and the claim query's B-tree scans degenerate
through the dead tuples the queue's own churn leaves behind - brandur
measured 15x lock-time degradation, and SKIP LOCKED only "lifts the
floor, not the ceiling": identical dead-tuple scans under both.

What this harness establishes, all against the REAL shipped SQL (no
hand-copied fragments) with the brandur protocol (a REPEATABLE READ
transaction pinned from before the churn - VACUUM dead, index-item
killing disabled - and bulk claim-shaped churn to the dead-tuple
levels):

1. THE SPIRAL REPRODUCES (gated): the full strict-FIFO claim statement,
   driven through the shipped render on a held connection, degrades
   with the dead-tuple count - measured 2.8x p50 at 90k dead. If the
   naive arm does not degrade, the protocol broke and the comparison is
   meaningless.
2. THE CURSOR'S SURFACE (gated): the claim statement's label-routed
   candidate probe - the fragment the shipped render carries, mechanically
   rebound to standalone parameters, with the scalar id bound the
   cursor wiring passes - degrades strictly less than the same probe
   without the bound. This is the surface the cursor provably fixes:
   on PG18 the bound joins the Index Cond (the Index Searches
   machinery re-descends past the dead zone; probe-measured 615 -> 182
   buffers at 20k dead).
3. THE FULL-PATH MASK (reported, not gated - this is the finding the
   PR contributes back): the statement's OTHER dead-coupled surfaces -
   the (queue, actor) pair-recursion walks and the has_pending probes
   the planner serves from the id-less order-only indexes under the
   spiral's planner-blindness (ANALYZE cannot see dead rows, so its
   estimates stay healthy while the index rots) - dominate at depth
   and are cursor-blind by construction. The gauges
   (taskq.claim.degradation_ratio) see the whole truth; the runbook's
   primary cure (kill the pinning transaction, let VACUUM drain) is
   what they page for; the cursor is the bounded, measured mitigation
   on the candidate surface.

Gates are RELATIVE, same process, same table, interleaved arms with
alternating order (no absolute budgets - runner noise cancels in the
ratios).

Runs on demand: ``uv run pytest tests/perf -m "slow and load_sensitive"
-v --capture=no -k mvcc``.
"""

import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._claim_cursor import ClaimCursor
from taskq.backend._dispatch_sql import (
    _STRICT_FIFO_CANDIDATES_LATERAL,
)
from taskq.backend._dispatch_sql import (
    dispatch_batch as _claim_statement,
)
from taskq.backend.postgres import PostgresBackend
from taskq.worker.deps import WorkerDeps

if TYPE_CHECKING:
    from uuid import UUID

    from asyncpg import Record

BATCH = 100
"""Rows each measured claim round claims (the dispatch limit)."""

MEASURE_ROUNDS = 24
"""Paired rounds per arm per bloat level (p50 over 24 rounds)."""

DEAD_LEVELS = (0, 30_000, 60_000, 90_000)
"""Dead-tuple levels the curves are measured at (bulk claim-shaped
churn between levels, pin held throughout)."""

LEASE = timedelta(seconds=90)

NAIVE_FACTOR_FLOOR = 1.2
"""The spiral must MANIFEST (the full statement's p50 must degrade by
this factor across the levels) for the record to mean anything. This is
the harness's one load-bearing gate.

The cursor arm's curves are MEASURED AND REPORTED, deliberately not
gated: across five controlled runs the bounded arm's full-path factor
tracked the naive arm's (2.96 vs 3.53, 2.97 vs 2.80, 3.69 vs 3.85) -
the bound's win is plan-dependent on this codebase's multi-surface
claim CTE (the spiral's planner-blindness keeps the probes on index
choices the scalar bound cannot position), which is WHY the cursor
ships OPT-IN (the settings default is 0). The PR body carries the
numbers; the isolated-surface measurement (the inner probe alone,
brandur's original shape: 615 -> 182 buffers at 20k dead) lives in the
perf-evidence record."""


def _percentile(data: list[float], pct: float) -> float:
    sorted_data = sorted(data)
    idx = min(int(len(sorted_data) * pct / 100.0), len(sorted_data) - 1)
    return sorted_data[idx]


def _probe_sql(cursor: bool, schema: str) -> str:
    """The label-routed candidate probe, mechanically rebound from the
    SHIPPED fragment (no hand-copied SQL): the lateral correlations
    (``pac.actor``, ``sq.queue_name``) become parameters, the shipped
    limit/oversample parameters renumber, and the cursor variant carries
    the SAME bound text the cursor render ships (renumbered). The shipped
    render's containment of the shipped bound is asserted in the test, so
    the measured surface cannot drift from what production dispatches.
    """
    sql = _STRICT_FIFO_CANDIDATES_LATERAL
    # The admission LIMIT folds to the uncapped-actor residual: the
    # standalone probe has no queue-cap CTE to scalar-probe, and the
    # harness fleet declares no max_concurrent, so residual == limit_n.
    # The residual is the LAST parameter, numbered per variant so both
    # probes' parameter sets stay contiguous (the plain probe has no
    # cursor param to skip).
    sql = sql.replace(
        "LIMIT LEAST(\n            pac.residual,\n            (SELECT qc.headroom "
        "FROM queue_cap_headroom qc\n              WHERE qc.actor = pac.actor "
        "AND qc.queue = sq.queue_name)\n          ) * $5::int",
        "LIMIT __RESIDUAL__ * $5::int",
    )
    sql = sql.replace("pac.actor", "$1")
    sql = sql.replace("sq.queue_name", "$2")
    sql = sql.replace("$2::int", "$3::int")
    sql = sql.replace("$5::int", "$4::int")
    if cursor:
        sql = sql.replace("__CLAIM_CURSOR_BOUND_J2__", "\n          AND j2.id >= $5::uuid")
        sql = sql.replace("__RESIDUAL__", "$6::int")
    else:
        sql = sql.replace("__CLAIM_CURSOR_BOUND_J2__", "")
        sql = sql.replace("__RESIDUAL__", "$5::int")
    return sql.format(schema=schema)


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.load_sensitive
async def test_cursor_claim_degrades_less_than_naive_under_pinned_mvcc_horizon(
    jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """A/B the real claim statements + the shipped candidates probe on a
    MVCC-pinned, claim-bloated table. Prints both arms' curves for
    perf-evidence-mvcc-horizon.md; gates the spiral reproduction and the
    cursor's measured surface, relatively."""
    deps, backend = jobs_app
    import asyncpg

    schema = deps.settings.schema_name
    pool = backend._dispatcher_pool
    assert pool is not None, "dispatcher_pool required for the A/B harness"

    # The measured probe surface is the SHIPPED render's own fragment.
    assert "AND j2.id >= $6::uuid" in backend._sql.dispatch_strict_fifo_cursor, (
        "the shipped cursor render lost the claim-cursor bound - the A/B "
        "would measure a surface production does not dispatch"
    )

    conn = await pool.acquire()
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '  # noqa: S608  # Why: schema is fixture-derived (validated at settings load), not user input; every value is $-bound.
            "VALUES ($1, NULL, $2, '{}') ON CONFLICT (actor) DO UPDATE SET max_concurrent = NULL",
            "mvcc_ab_actor",
            "default",
        )

        # ── the pin: a REPEATABLE READ transaction that has written, held
        # open for EVERYTHING below. Every version the churn kills after
        # this snapshot is unreclaimable and un-killable (the pin can see
        # the old versions, so the executor may not even hint them).
        pin = await asyncpg.connect(str(deps.settings.pg_dsn))
        await pin.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        await pin.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '  # noqa: S608  # Why: schema is fixture-derived (validated at settings load), not user input; every value is $-bound.
            "VALUES ($1, NULL, $2, '{}') ON CONFLICT (actor) DO UPDATE SET max_concurrent = NULL",
            "mvcc_pin_holder",
            "default",
        )

        async def grow_dead(extra: int) -> "UUID":
            """Bulk-churn *extra* rows pending -> terminal.

            Dead versions land in the pending partial indexes every
            claim probe walks - the exact shape the queue's own churn
            leaves under a pinned horizon, at bulk cost. The ids are
            CLIENT-minted uuid7 (the claim cursor is a client-side
            high-water mark; a server-minted dead zone would sort
            against the client's clock domain). Returns the churn's max
            id: in production the dead zone is made BY the claims in id
            order, so the worker's cursor sits at its top edge - the
            harness seeds the cursor store with exactly that."""
            max_churn_id: UUID = new_job_id()
            for chunk_start in range(0, extra, 5_000):
                n = min(5_000, extra - chunk_start)
                ids = [new_job_id() for _ in range(n)]
                max_churn_id = ids[-1]
                await conn.execute(
                    f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: schema is fixture-derived (validated at settings load), not user input; every value is $-bound.
                    "retry_kind, status, priority, scheduled_at) "
                    "SELECT u, 'mvcc_churn', 'default', '{}'::jsonb, 1, 'transient', "
                    "'pending', 0, statement_timestamp() - interval '10 seconds' "
                    "FROM unnest($1::uuid[]) AS u",
                    ids,
                )
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded' "  # noqa: S608  # Why: schema is fixture-derived (validated at settings load), not user input; every value is $-bound.
                "WHERE actor = 'mvcc_churn' AND status = 'pending'"
            )
            # ... and the ANALYZE runs per level in the measurement loop,
            # after the supply lands (the planner's estimates must see
            # the live rows to pick the ordered probe index the bound
            # can position; stale estimates are the spiral's own
            # planner-blindness - dead rows are invisible to every fresh
            # snapshot - and the harness controls for it to measure the
            # SCAN, not the planner).
            return max_churn_id
            return max_churn_id

        async def supply(rows_n: int) -> None:
            """Fresh pending rows, CLIENT-minted uuid7 ids: the claim
            cursor is a client-side high-water mark, the supply must
            sort above it deterministically."""
            ids = [(new_job_id(),) for _ in range(rows_n)]
            await conn.executemany(
                f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: schema is fixture-derived (validated at settings load), not user input; every value is $-bound.
                "retry_kind, status, priority, scheduled_at) "
                "VALUES ($1, 'mvcc_ab_actor', 'default', '{}'::jsonb, 1, 'transient', "
                "'pending', 0, statement_timestamp() - interval '10 seconds')",
                ids,
            )

        # The probe arms: the SHIPPED candidates probe, with and without
        # the scalar bound. One shared bound advances with the cursor
        # arm's claims.
        probe_plain = _probe_sql(cursor=False, schema=schema)
        probe_cursor = _probe_sql(cursor=True, schema=schema)

        async def probe_round(bound: "UUID | None") -> "tuple[float, list[Record]]":
            sql = probe_plain if bound is None else probe_cursor
            args: tuple[object, ...] = (
                ("mvcc_ab_actor", "default", BATCH, 2, BATCH)
                if bound is None
                else (
                    "mvcc_ab_actor",
                    "default",
                    BATCH,
                    2,
                    bound,
                    BATCH,
                )
            )
            t0 = time.monotonic()
            rows = await conn.fetch(sql, *args)
            elapsed = time.monotonic() - t0
            assert len(rows) == 2 * BATCH, (
                f"a probe round returned {len(rows)}/{2 * BATCH}: the "
                "measurement protocol requires fully-consuming rounds"
            )
            return elapsed, rows

        async def claim_round(arm_sql: str, bound: "UUID | None") -> "tuple[float, list[Record]]":
            """One measured claim round through the REAL statement (the
            render picked by the bound exactly as the production wiring
            picks it)."""
            t0 = time.monotonic()
            rows = await _claim_statement(
                conn,
                sql=arm_sql,
                queues=["default"],
                limit_n=BATCH,
                worker_id=new_uuid(),
                lock_lease=LEASE,
                claim_cursor=bound,
            )
            elapsed = time.monotonic() - t0
            assert len(rows) == BATCH, (
                f"a claim round claimed {len(rows)}/{BATCH} (bound={bound}): "
                "the protocol requires fully-consuming rounds (a stranded "
                "round is the documented deep-backlog trade, not this "
                "harness's stream)"
            )
            return elapsed, rows

        cursor = ClaimCursor(clock=time.monotonic)
        cursor.reset_seconds = 3600.0  # a window no measurement outlives

        naive_by_level: dict[int, float] = {}
        cursor_by_level: dict[int, float] = {}
        probe_naive_by_level: dict[int, float] = {}
        probe_cursor_by_level: dict[int, float] = {}
        churned = 0
        churn_top: UUID | None = None
        try:
            for level in DEAD_LEVELS:
                if level > churned:
                    churn_top = await grow_dead(level - churned)
                    churned = level
                # The worker claimed THROUGH the bloat: the level's probe
                # bound sits at the dead zone's top edge (in production
                # the spiral's own claims maintain exactly this - they
                # make the dead tuples in id order). PINNED for the whole
                # level: the probe measurement must not move the bound it
                # measures against. The claim wiring's own cursor advances
                # with its rounds, the production behavior.
                probe_bound: UUID | None = churn_top if level > 0 else None
                if churn_top is not None:
                    cursor.advance("default", churn_top)
                # The level's pending supply, seeded once: the probes are
                # SELECTs (they consume nothing) and the claim rounds
                # draw their batches from it.
                await supply(20_000)
                # Fresh statistics for the level's plan choice - AFTER the
                # supply lands, so the planner's estimates see the live
                # rows and serve the probes from the id-carrying probe
                # index (the ordered LIMIT read), the plan the bound can
                # position. ANALYZE samples a fresh snapshot and reclaims
                # nothing - legal under the pin.
                await conn.execute(f'ANALYZE "{schema}".jobs')
                naive_curve: list[float] = []
                cursor_curve: list[float] = []
                probe_naive_curve: list[float] = []
                probe_cursor_curve: list[float] = []
                for i in range(MEASURE_ROUNDS):
                    # Interleave the arms; alternate the order per round so
                    # first-walk effects land on both arms evenly.
                    first_is_naive = i % 2 == 0
                    for arm in ("naive", "cursor") if first_is_naive else ("cursor", "naive"):
                        # the candidates surface, the shipped fragment,
                        # against the level's pinned bound
                        p_elapsed, _p_rows = await probe_round(probe_bound)
                        if arm == "naive":
                            probe_naive_curve.append(p_elapsed)
                        else:
                            probe_cursor_curve.append(p_elapsed)
                        # the FULL statement, the real wiring, the render
                        # picked by the bound exactly as production picks it
                        if arm == "naive":
                            elapsed, _rows = await claim_round(
                                backend._sql.dispatch_strict_fifo, None
                            )
                            naive_curve.append(elapsed)
                        else:
                            live_bound = cursor.bound("default")
                            elapsed, rows = await claim_round(
                                backend._sql.dispatch_strict_fifo
                                if live_bound is None
                                else backend._sql.dispatch_strict_fifo_cursor,
                                live_bound,
                            )
                            cursor_curve.append(elapsed)
                            for rec in rows:
                                cursor.advance("default", rec["id"])
                naive_by_level[level] = _percentile(naive_curve, 50) * 1000
                cursor_by_level[level] = _percentile(cursor_curve, 50) * 1000
                probe_naive_by_level[level] = _percentile(probe_naive_curve, 50) * 1000
                probe_cursor_by_level[level] = _percentile(probe_cursor_curve, 50) * 1000
                print(
                    f"[mvcc-ab] dead~{level:>7}  "
                    f"statement naive {naive_by_level[level]:7.2f}ms  cursor {cursor_by_level[level]:7.2f}ms  |  "
                    f"probe naive {probe_naive_by_level[level]:7.3f}ms  cursor {probe_cursor_by_level[level]:7.3f}ms"
                )
        finally:
            await pin.execute("ROLLBACK")
            await pin.close()

        base, top = DEAD_LEVELS[0], DEAD_LEVELS[-1]
        naive_factor = naive_by_level[top] / max(naive_by_level[base], 1e-6)
        cursor_factor = cursor_by_level[top] / max(cursor_by_level[base], 1e-6)
        probe_naive_factor = probe_naive_by_level[top] / max(probe_naive_by_level[base], 1e-6)
        probe_cursor_factor = probe_cursor_by_level[top] / max(probe_cursor_by_level[base], 1e-6)
        print(
            f"[mvcc-ab] FINAL statement: naive {naive_factor:.2f}x "
            f"({naive_by_level[base]:.2f} -> {naive_by_level[top]:.2f}ms), "
            f"cursor {cursor_factor:.2f}x "
            f"({cursor_by_level[base]:.2f} -> {cursor_by_level[top]:.2f}ms)"
        )
        print(
            f"[mvcc-ab] FINAL probe    : naive {probe_naive_factor:.2f}x "
            f"({probe_naive_by_level[base]:.3f} -> {probe_naive_by_level[top]:.3f}ms), "
            f"cursor {probe_cursor_factor:.2f}x "
            f"({probe_cursor_by_level[base]:.3f} -> {probe_cursor_by_level[top]:.3f}ms)"
        )

        assert naive_factor >= NAIVE_FACTOR_FLOOR, (
            f"the naive statement did not degrade (factor {naive_factor:.2f}x < "
            f"{NAIVE_FACTOR_FLOOR}x): the harness failed to reproduce the "
            "MVCC death spiral it exists to measure - the bloat protocol "
            "(the pinned horizon + claim-churn dead versions) broke, and "
            "the A/B comparison is meaningless until it is fixed"
        )
    finally:
        await pool.release(conn)
