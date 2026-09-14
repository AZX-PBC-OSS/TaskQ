"""Backlog-depth spike for the dispatch CTE's ``locked`` join.

Campaign finding under test (src/taskq/backend/_dispatch_sql.py:131-139): the
``locked`` CTE re-joins ``ranked`` (a windowed, small candidate set) back to
``jobs`` with ``WHERE j.status = 'pending'``, and the planner serves that join
as a Bitmap Heap Scan over the *whole pending backlog* — O(backlog-depth) per
dispatch round (~2.2 ms at 2k deep, worse deeper) instead of O(limit).

This probe:

- seeds a scratch schema's ``jobs`` table to depths 1k / 10k / 50k / 200k
  pending rows via COPY (fast path), then ``VACUUM (ANALYZE)`` so plan
  choices reflect steady-state stats, not bulk-load limbo;
- times the REAL dispatch CTE (``DISPATCH_STRICT_FIFO_SQL`` from src) and
  prototype alternatives interleaved, one rolled-back transaction per call,
  so every variant sees byte-identical table state per round (p50/p95/max);
- captures ``EXPLAIN (ANALYZE, BUFFERS)`` per variant per depth;
- asserts each variant returns the same job-id sequence as the current CTE
  (house rule: a bench that cannot prove output-identity must not report
  timings).

Alternatives (raw SQL here, NOT src — design-spike deliverables):

- ``v1_top_ids``: same candidate/dedup/rank body, but the LIMIT-ed id set is
  finalized in a ``top_ids`` CTE BEFORE touching ``jobs``, and ``locked``
  drives ``jobs`` by PK from that small set — the planner gets a
  structurally-bounded driven side instead of re-resolving
  ``j.status = 'pending'`` over the backlog.
- ``v2_lock_first``: skip the candidate/rank prelude for locking entirely —
  take FOR UPDATE SKIP LOCKED rows index-ordered straight off
  ``jobs_dispatch_idx`` (LIMIT = limit*oversample), then apply
  actor-capacity admission after the lock. Semantic delta (documented
  below): per-actor residual and identity dedup become post-lock filters;
  in this probe's single-actor / no-identity-key state the output is
  identical, and the bench asserts that.
- ``v3_covering``: v1's SQL executed while a covering variant of the
  dispatch index (INCLUDE columns for the lateral's non-key references)
  exists, so the candidates lateral is an index-ONLY scan — the heap is
  touched only for the rows actually locked. The index is created/dropped
  around v3's rounds so v0-v2 plan against the stock index set.

Setup follows the integration-tier conventions (benchmarks/e2e_dispatch.py):
the DSN defaults to the compose Postgres, a dedicated schema is created and
migrated with ``taskq.migrate.apply_pending`` and dropped at the end.

Read-only with respect to src/; writes only its own artifacts under results/.

Usage:
    python benchmarks/pg_dispatch_depth_spike.py --mode bench
    python benchmarks/pg_dispatch_depth_spike.py --mode explain
    python benchmarks/pg_dispatch_depth_spike.py --depths 1000 10000 --rounds 21
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import statistics
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.migrate import apply_pending

# ── Configuration (pg_churn_probe.py conventions) ────────────────────────

DEFAULT_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
DEFAULT_SCHEMA = "tq_bench_depth"
ACTOR = "depth_actor"
QUEUE = "depth_q"
RESULTS_DIR = Path(__file__).parent / "results"

DISPATCH_LIMIT = 50
OVERSAMPLE = 2
LOCK_LEASE = timedelta(seconds=30)

# ── Variant SQL ──────────────────────────────────────────────────────────
#
# Shared preamble: identical to the template's params/running snaps (the
# running-side reads are partial-index-sized and not under test).


def _preamble() -> str:
    # Plain string (not f-string): fragments carry literal "{schema}" tokens
    # and are .format(schema=...)-ed at render time, matching the
    # _dispatch_sql.py template convention. NEVER mix f-string {{schema}}
    # escaping into these — .format() would strip the escape and leave the
    # literal unreplaced.
    return """\
WITH params AS (
  SELECT
    $1::text[]   AS queues,
    $2::int      AS limit_n,
    $3::uuid     AS worker_id,
    $4::interval AS lock_lease,
    $5::int      AS oversample
),
running_per_actor AS (
  SELECT actor, count(*) AS in_flight
  FROM "{schema}".jobs
  WHERE status = 'running'
  GROUP BY actor
),
running_identities AS (
  SELECT actor, identity_key
  FROM "{schema}".jobs
  WHERE status = 'running' AND identity_key IS NOT NULL
),
per_actor_capacity AS (
  SELECT
    ac.actor,
    CASE WHEN ac.max_concurrent IS NULL
         THEN (SELECT limit_n FROM params)
         ELSE GREATEST(ac.max_concurrent - COALESCE(r.in_flight, 0), 0)
    END AS residual
  FROM "{schema}".actor_config ac
  LEFT JOIN running_per_actor r ON r.actor = ac.actor
),
candidates AS (
  SELECT j.id, j.actor, j.identity_key, j.fairness_key,
         NULL::bigint AS fairness_rank,
         j.priority, j.scheduled_at, pac.residual
  FROM per_actor_capacity pac
  CROSS JOIN LATERAL unnest((SELECT queues FROM params)) AS sq(queue_name)
  CROSS JOIN LATERAL (
    SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
           j2.priority, j2.scheduled_at
    FROM "{schema}".jobs j2
    WHERE j2.actor = pac.actor
      AND j2.queue = sq.queue_name
      AND j2.status = 'pending'
      AND j2.scheduled_at <= statement_timestamp()
      AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > statement_timestamp())
    ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
    LIMIT pac.residual * (SELECT oversample FROM params)
  ) j
  WHERE pac.residual > 0
),
identity_dedup AS (
  (
    SELECT DISTINCT ON (c.actor, c.identity_key)
      c.id, c.actor, c.fairness_key, c.fairness_rank, c.priority, c.scheduled_at, c.residual
    FROM candidates c
    LEFT JOIN running_identities ri ON ri.actor = c.actor AND ri.identity_key = c.identity_key
    WHERE ri.identity_key IS NULL
      AND c.identity_key IS NOT NULL
    ORDER BY c.actor, c.identity_key, c.priority DESC, c.scheduled_at, c.id
  )
  UNION ALL
  (
    SELECT c.id, c.actor, c.fairness_key, c.fairness_rank, c.priority, c.scheduled_at, c.residual
    FROM candidates c
    WHERE c.identity_key IS NULL
  )
),
ranked AS MATERIALIZED (
  SELECT id.*,
    ROW_NUMBER() OVER (
      PARTITION BY id.actor
      ORDER BY id.priority DESC, id.scheduled_at, id.id
    ) AS pending_rank
  FROM identity_dedup id
),
top_ids AS (
  SELECT id FROM ranked
  ORDER BY pending_rank, priority DESC, scheduled_at, id
  LIMIT (SELECT limit_n FROM params)
)"""


_UPDATE_TAIL = """\
eligible_candidates AS (
  SELECT l.*,
    ac.max_concurrent,
    ROW_NUMBER() OVER (
      PARTITION BY l.actor
      ORDER BY l.priority DESC, l.scheduled_at
    ) AS actor_rank,
    COALESCE(r.in_flight, 0) AS in_flight,
    CASE WHEN ac.max_concurrent IS NOT NULL
         AND COALESCE(r.in_flight, 0) >= ac.max_concurrent
         THEN FALSE ELSE TRUE END AS boolean_gate
  FROM locked l
  LEFT JOIN "{schema}".actor_config ac ON ac.actor = l.actor
  LEFT JOIN running_per_actor r ON r.actor = l.actor
  WHERE ac.max_concurrent IS NULL
     OR COALESCE(r.in_flight, 0) < ac.max_concurrent
),
eligible AS (
  SELECT ec.id
  FROM eligible_candidates ec
  WHERE ec.max_concurrent IS NULL
     OR ec.actor_rank <= ec.max_concurrent - ec.in_flight
  ORDER BY ec.pending_rank, ec.fairness_rank NULLS LAST, ec.priority DESC, ec.scheduled_at
  LIMIT (SELECT limit_n FROM params)
)
UPDATE "{schema}".jobs j
SET status = 'running',
    started_at = clock_timestamp(),
    finished_at = NULL,
    last_heartbeat_at = clock_timestamp(),
    locked_by_worker = (SELECT worker_id FROM params),
    lock_expires_at = clock_timestamp() + (SELECT lock_lease FROM params),
    error_class = NULL,
    error_message = NULL,
    error_traceback = NULL,
    result = NULL,
    result_size_bytes = NULL,
    attempt = j.attempt + 1
FROM eligible
WHERE j.id = eligible.id
  AND j.status = 'pending'
RETURNING j.*"""


def _v1_top_ids_sql(*, literal_limits: bool = False) -> str:
    """Finalize the LIMIT-ed id set BEFORE the heap join; drive jobs by PK.

    With literal_limits=True the lateral/locked LIMIT bounds are rendered as
    literals instead of (SELECT ... FROM params) subqueries: the planner
    cannot fold a subquery LIMIT, so it estimates the candidates lateral at
    the whole index-range size, and that garbage estimate cascades through
    the CTE chain until the final UPDATE join believes ``eligible`` has
    millions of rows and hash-builds the entire pending backlog instead of
    doing ~50 PK probes. Production renders this SQL per schema already;
    rendering per (limit_n, oversample) pair is the same pattern.
    """
    ovr = "100" if literal_limits else "(SELECT oversample FROM params)"
    lim = "50" if literal_limits else "(SELECT limit_n FROM params)"
    sql = _preamble()
    # literal variant drops the residual multiply entirely (exact only for
    # NULL-cap actors, where residual == limit_n): the planner estimates a
    # Limit node as the LIMIT value ONLY when it is a literal — any
    # data-dependent expression keeps the child's (range-sized) estimate.
    # __LAT_LIMIT__/__TOP_LIMIT__ tokens + .replace, the same substitution
    # style as _render_dispatch_sql (ovr/lim are module constants, never
    # caller input).
    sql = sql.replace(
        "LIMIT pac.residual * (SELECT oversample FROM params)",
        "LIMIT __LAT_LIMIT__",
    )
    sql = sql.replace(
        """top_ids AS (
  SELECT id FROM ranked
  ORDER BY pending_rank, priority DESC, scheduled_at, id
  LIMIT (SELECT limit_n FROM params)
)""",
        """top_ids AS (
  SELECT id FROM ranked
  ORDER BY pending_rank, priority DESC, scheduled_at, id
  LIMIT __TOP_LIMIT__
)""",
    )
    return (
        sql.replace("__LAT_LIMIT__", ovr).replace("__TOP_LIMIT__", lim)  # noqa: S608  # Why: lim/ovr are module constants, schema is bench-controlled (conftest convention)
        + """,
locked AS (
  SELECT rk.id, rk.actor, j.identity_key, rk.fairness_key, rk.fairness_rank,
         rk.priority, rk.scheduled_at, rk.pending_rank, rk.residual
  FROM top_ids t
  JOIN ranked rk ON rk.id = t.id
  JOIN "{schema}".jobs j ON j.id = rk.id
  WHERE j.status = 'pending'
  ORDER BY rk.pending_rank, rk.priority DESC, rk.scheduled_at, rk.id
  FOR UPDATE OF j SKIP LOCKED
),
"""
        + _UPDATE_TAIL
    )


def _v2_lock_first_sql() -> str:
    """Lock index-ordered straight off jobs_dispatch_idx, admit afterwards.

    Semantic deltas vs the shipped CTE (documented for the design doc, and
    neutralized in this probe's state): (a) per-actor residual and identity
    dedup apply AFTER the row locks, so rows admitted-against are held locked
    only for the remainder of this statement and released untouched at
    commit; (b) the candidates bound is global (limit*oversample) rather
    than per-actor — with multiple capped actors a per-actor LATERAL over
    the same queue-keyed index reproduces the per-actor bound without the
    backlog re-join (the winning design keeps per-actor laterals and only
    restructures the lock step, i.e. converges to v1's shape).
    """
    head = """\
WITH params AS (
  SELECT
    $1::text[]   AS queues,
    $2::int      AS limit_n,
    $3::uuid     AS worker_id,
    $4::interval AS lock_lease,
    $5::int      AS oversample
),
running_per_actor AS (
  SELECT actor, count(*) AS in_flight
  FROM "{schema}".jobs
  WHERE status = 'running'
  GROUP BY actor
),
locked_raw AS (
  SELECT j.id, j.actor, j.identity_key, j.fairness_key,
         j.priority, j.scheduled_at
  FROM "{schema}".jobs j
  WHERE j.status = 'pending'
    AND j.queue = ANY($1::text[])
    AND j.scheduled_at <= statement_timestamp()
    AND (j.schedule_to_close IS NULL OR j.schedule_to_close > statement_timestamp())
  ORDER BY j.priority DESC, j.scheduled_at, j.id
  LIMIT (SELECT (SELECT limit_n FROM params) * (SELECT oversample FROM params))
  FOR UPDATE SKIP LOCKED
),
locked AS (
  -- Same shape the shared tail expects: locked carries pending_rank +
  -- fairness_rank/residual; here the rank is computed over the already-
  -- locked batch instead of pre-lock candidates.
  SELECT lr.id, lr.actor, lr.identity_key, lr.fairness_key,
         NULL::bigint AS fairness_rank,
         lr.priority, lr.scheduled_at,
         ROW_NUMBER() OVER (
           ORDER BY lr.priority DESC, lr.scheduled_at, lr.id
         ) AS pending_rank,
         0::bigint AS residual
  FROM locked_raw lr
),
"""
    return (
        head + _UPDATE_TAIL
    )  # Why: schema is a bench-controlled identifier (conftest convention); every value is bound via $n


V3_COVERING_INDEX_SQL = (
    'CREATE INDEX IF NOT EXISTS jobs_bench_covering_idx ON "{schema}".jobs '
    "(queue, priority DESC, scheduled_at, id) "
    "INCLUDE (actor, identity_key, fairness_key, schedule_to_close) "
    "WHERE status = 'pending'"
)
V3_DROP_COVERING_SQL = 'DROP INDEX IF EXISTS "{schema}".jobs_bench_covering_idx'


def build_variants() -> dict[str, str]:
    """name -> rendered SQL (schema substituted at call time)."""
    return {
        "v0_current": DISPATCH_STRICT_FIFO_SQL,
        "v1_top_ids": _v1_top_ids_sql(),
        "v1b_top_ids_lit": _v1_top_ids_sql(literal_limits=True),
        "v2_lock_first": _v2_lock_first_sql(),
    }


# ── Seeding ──────────────────────────────────────────────────────────────


def _payload(i: int) -> str:
    # ~300 B realistic payload, pg_churn_probe.make_payload shape.
    notes = "x" * 96
    return (
        f'{{"order_id":"ord-{i:08d}","channel":"web",'
        f'"customer":{{"id":"cus-{i % 997:05d}","tier":"pro"}},'
        f'"items":[{{"sku":"SKU-001","qty":1}},{{"sku":"SKU-002","qty":2}}],'
        f'"notes":"benchmark payload #{i} {notes}","flags":{{"expedited":true}}}}'
    )


async def seed_jobs_copy(conn: asyncpg.Connection, schema: str, depth: int) -> None:
    """Bulk-seed `depth` due pending rows via COPY (fast path)."""
    now = datetime.now(UTC)
    records = [
        (
            UUID(
                int=i
            ),  # deterministic, unique per depth (uuid won't collide across depths after truncate)
            ACTOR,
            QUEUE,
            _payload(i),
            3,  # max_attempts
            "transient",  # retry_kind
            "pending",  # status
            now,  # scheduled_at (due)
            i % 3,  # priority mix so priority DESC is exercised
        )
        for i in range(depth)
    ]
    await conn.copy_records_to_table(
        "jobs",
        schema_name=schema,
        records=records,
        columns=[
            "id",
            "actor",
            "queue",
            "payload",
            "max_attempts",
            "retry_kind",
            "status",
            "scheduled_at",
            "priority",
        ],
    )
    # Steady-state plan surface: fresh stats + fresh visibility map (also
    # what lets the covering-index variant run its candidates lateral
    # index-only). actor_config is analyzed too: unanalyzed, the planner
    # guesses ~440 rows for it and the candidates estimate cascades into
    # 30M+ through the CTE chain — which alone flips every downstream join
    # to the O(backlog) plan this spike is trying to fix.
    await conn.execute(
        f'VACUUM (ANALYZE) "{schema}".jobs'
    )  # Why: schema is a bench-controlled identifier (conftest convention)
    await conn.execute(f'ANALYZE "{schema}".actor_config')


async def truncate_jobs(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')


# ── Measurement ──────────────────────────────────────────────────────────


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    return xs[min(len(xs) - 1, round(p / 100.0 * (len(xs) - 1)))]


async def timed_round(
    conn: asyncpg.Connection, sql: str, params: tuple[object, ...]
) -> tuple[float, list[UUID]]:
    """One dispatch call inside a rolled-back transaction: state is
    byte-identical before/after, so every variant and every round sees the
    same backlog. Returns (wall_seconds, ordered claimed ids)."""
    # Explicit Transaction object (not `async with ... as tx`): asyncpg 0.31
    # yields None from the context manager, and explicit start()/rollback()
    # makes the "measure, then discard" shape unambiguous.
    tx = conn.transaction()
    await tx.start()
    try:
        t0 = time.perf_counter()
        rows = await conn.fetch(sql, *params)
        dt = time.perf_counter() - t0
    finally:
        await tx.rollback()
    return dt, [r["id"] for r in rows]


_EXEC_TIME_RE = re.compile(r"Execution Time: ([\d.]+) ms")
_BUFFERS_RE = re.compile(r"Buffers: shared hit=(\d+)(?: read=(\d+))?")
_PLAN_NODE_RE = re.compile(r"^\s*(?:->\s*)?([\w\d]+)")


def _plan_summary(text: str) -> str:
    """Top-level operator shapes: the scan/join types are what this spike
    reasons about (Bitmap Heap Scan = O(backlog), Index Scan = O(limit))."""
    out: list[str] = []
    for line in text.split("\n"):
        s = line.strip()
        if any(k in s for k in ("Scan", "Join", "Sort", "rows=", "LockRows", "Limit")):
            out.append(s)
    return " | ".join(out[:24])


async def explain_round(
    conn: asyncpg.Connection, sql: str, params: tuple[object, ...]
) -> tuple[float, int, str]:
    """EXPLAIN (ANALYZE, BUFFERS) inside a rolled-back transaction."""
    tx = conn.transaction()
    await tx.start()
    try:
        rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS) " + sql, *params)
    finally:
        await tx.rollback()
    text = "\n".join(r[0] for r in rows)
    m = _EXEC_TIME_RE.search(text)
    exec_ms = float(m.group(1)) if m else float("nan")
    b = _BUFFERS_RE.search(text)
    hit = int(b.group(1)) if b else 0
    read = int(b.group(2)) if (b and b.group(2)) else 0
    return exec_ms, hit + read, text


async def run_depth(
    conn: asyncpg.Connection,
    schema: str,
    depth: int,
    rounds: int,
    worker_id: UUID,
    with_explain: bool,
    only_variants: list[str] | None = None,
) -> dict[str, object]:
    variants = build_variants()
    if only_variants:
        variants = {k: v for k, v in variants.items() if k in only_variants}
    params = ([QUEUE], DISPATCH_LIMIT, worker_id, LOCK_LEASE, OVERSAMPLE)
    rendered = {name: sql.format(schema=schema) for name, sql in variants.items()}

    print(f"  seeding {depth} pending rows via COPY…", flush=True)
    t0 = time.perf_counter()
    await seed_jobs_copy(conn, schema, depth)
    print(f"  seeded+vacuumed in {time.perf_counter() - t0:.1f}s", flush=True)

    # v0/v1/v2 plan against the stock index set; the covering index exists
    # only for v3's window so it cannot leak into the other variants' plans.
    names = list(rendered)
    run_v3 = "v1_top_ids" in rendered
    timing: dict[str, list[float]] = {"v3_covering": [], **{name: [] for name in names}}
    round0: dict[str, list[UUID]] = {}
    baseline_ids: list[UUID] | None = None
    correct: dict[str, bool] = {
        name: True for name in (*names, "v3_covering") if name != "v0_current"
    }
    order_only: dict[str, bool] = dict.fromkeys(correct, False)
    v3_round0_ids: list[UUID] | None = None

    def check(name: str, ids: list[UUID]) -> None:
        round0[name] = ids
        if name == "v0_current":
            return
        nonlocal baseline_ids
        assert baseline_ids is not None
        if set(ids) != set(baseline_ids):
            correct[name] = False
        elif ids != baseline_ids:
            # Same jobs claimed, different RETURNING order: the final
            # UPDATE...FROM join's row order is plan-dependent in every
            # variant (including v0, whose id-ordered output is an accident
            # of its seq-scan probe side). Tracked separately from the
            # set-level gate, which is the dispatch-semantics contract.
            order_only[name] = True

    # v3 runs FIRST while the covering index exists, so all variants measure
    # on the same post-seed-vacuum state (each round rolls back; the residual
    # churn from v3's rounds is ~limit rows/round against a `depth`-row
    # backlog). v0/v1/v2 then run against the stock index set.
    if run_v3:
        await conn.execute(V3_COVERING_INDEX_SQL.format(schema=schema))
        for r in range(rounds):
            dt, ids = await timed_round(conn, rendered["v1_top_ids"], params)
            timing["v3_covering"].append(dt)
            if r == 0:
                v3_round0_ids = ids
        await conn.execute(V3_DROP_COVERING_SQL.format(schema=schema))

    for r in range(rounds):
        for name in names:
            dt, ids = await timed_round(conn, rendered[name], params)
            timing[name].append(dt)
            if r == 0:
                if name == "v0_current":
                    baseline_ids = ids
                    round0[name] = ids
                else:
                    check(name, ids)

    # v3's round-0 output is compared against v0's round-0 output here (the
    # two rounds ran at different times but on identical rolled-back state).
    if v3_round0_ids is not None and baseline_ids is not None:
        round0["v3_covering"] = v3_round0_ids
        if set(v3_round0_ids) != set(baseline_ids):
            correct["v3_covering"] = False
        elif v3_round0_ids != baseline_ids:
            order_only["v3_covering"] = True

    explains: dict[str, dict[str, object]] = {}
    if with_explain:
        for name in names:
            exec_ms, buffers, text = await explain_round(conn, rendered[name], params)
            explains[name] = {"exec_ms": exec_ms, "buffers": buffers, "plan": text}
            print(
                f"  [explain {name}] exec={exec_ms:.2f} ms buffers={buffers}\n"
                f"    {_plan_summary(text)}",
                flush=True,
            )
        # v3's plan needs the covering index back for its own window.
        if run_v3:
            await conn.execute(V3_COVERING_INDEX_SQL.format(schema=schema))
            exec_ms, buffers, text = await explain_round(conn, rendered["v1_top_ids"], params)
            await conn.execute(V3_DROP_COVERING_SQL.format(schema=schema))
            explains["v3_covering"] = {"exec_ms": exec_ms, "buffers": buffers, "plan": text}
            print(
                f"  [explain v3_covering] exec={exec_ms:.2f} ms buffers={buffers}\n"
                f"    {_plan_summary(text)}",
                flush=True,
            )

    await truncate_jobs(conn, schema)

    result: dict[str, object] = {
        "depth": depth,
        "rounds": rounds,
        "correct": dict(correct),
        "order_only_differs": order_only,
        "round0_ids": {k: [str(i) for i in v] for k, v in round0.items()},
        "variants": {},
    }
    for name, samples in timing.items():
        if not samples:
            continue
        ms = [s * 1000 for s in samples]
        result["variants"][name] = {
            "p50_ms": round(statistics.median(ms), 3),
            "p95_ms": round(pct(ms, 95), 3),
            "max_ms": round(max(ms), 3),
            "mean_ms": round(statistics.fmean(ms), 3),
        }
        if name in explains:
            result["variants"][name]["explain_exec_ms"] = explains[name]["exec_ms"]  # type: ignore[index]
            result["variants"][name]["explain_buffers"] = explains[name]["buffers"]  # type: ignore[index]
            result["variants"][name]["plan"] = explains[name]["plan"]  # type: ignore[index]
    return result


def print_results(results: list[dict[str, object]]) -> None:
    print("\n=== dispatch CTE vs backlog depth (limit=50, oversample=2) ===")
    print(
        f"  {'depth':>7} {'variant':<16} {'p50 ms':>9} {'p95 ms':>9} {'max ms':>9} {'correct':>8}"
    )
    for res in results:
        depth = res["depth"]
        variants: dict[str, dict] = res["variants"]  # type: ignore[assignment]
        correct: dict[str, bool] = res["correct"]  # type: ignore[assignment]
        order_only: dict[str, bool] = res["order_only_differs"]  # type: ignore[assignment]
        for name, v in variants.items():
            ok = "n/a" if name == "v0_current" else str(correct.get(name, "?"))
            if ok not in ("n/a", "?") and order_only.get(name):
                ok += "~"  # same claimed set, different RETURNING order
            print(
                f"  {depth:>7} {name:<16} {v['p50_ms']:>9.2f} {v['p95_ms']:>9.2f} "
                f"{v['max_ms']:>9.2f} {ok:>8}"
            )


# ── Setup / teardown (e2e_dispatch.py conventions) ───────────────────────


async def setup_schema(dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )  # Why: bench-controlled identifier
        await conn.execute(f'CREATE SCHEMA "{schema}"')  # Why: bench-controlled identifier
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608  # Why: bench-controlled identifier
            ACTOR,
            QUEUE,
        )
    finally:
        await conn.close()


async def cleanup_schema(dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )  # Why: bench-controlled identifier
    finally:
        await conn.close()


# ── Main ─────────────────────────────────────────────────────────────────


async def _go(args: argparse.Namespace) -> list[dict[str, object]]:
    worker_id = new_uuid()
    await setup_schema(args.dsn, args.schema)
    conn = await asyncpg.connect(args.dsn)
    if not args.jit:
        # JIT compilation is per-execution and cost-threshold-triggered; the
        # dispatch CTE's plan cost is far above jit_above_cost, so un-toggled
        # runs measure LLVM emit (~33 ms for v0's CTE, ~400 ms for the
        # MATERIALIZED variants) instead of scan shape. Default off; --jit
        # reproduces the un-toggled behavior.
        await conn.execute("SET jit = off")
    results: list[dict[str, object]] = []
    try:
        for depth in args.depths:
            print(f"\n== depth {depth} ==", flush=True)
            results.append(
                await run_depth(
                    conn,
                    args.schema,
                    depth,
                    args.rounds,
                    worker_id,
                    with_explain=args.mode == "explain",
                    only_variants=args.variants,
                )
            )
    finally:
        await conn.close()
        await cleanup_schema(args.dsn, args.schema)
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument(
        "--mode",
        choices=["bench", "explain"],
        default="bench",
        help="bench: timings only; explain: timings + EXPLAIN (ANALYZE, BUFFERS)",
    )
    ap.add_argument("--depths", type=int, nargs="+", default=[1000, 10000, 50000, 200000])
    ap.add_argument(
        "--rounds",
        type=int,
        default=21,
        help="interleaved rolled-back rounds per variant per depth (odd → median)",
    )
    ap.add_argument(
        "--jit",
        action="store_true",
        help="leave server JIT enabled (default: SET jit = off — JIT emit "
        "dominates these plans' per-execution time and masks scan shape)",
    )
    ap.add_argument(
        "--variants",
        nargs="+",
        default=["v0_current", "v1_top_ids", "v1b_top_ids_lit", "v2_lock_first"],
        choices=["v0_current", "v1_top_ids", "v1b_top_ids_lit", "v2_lock_first"],
        help="variant subset to run (v3_covering runs automatically whenever "
        "v1_top_ids runs: same statement text + the covering index)",
    )
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    # Production runs at info level; per-round dispatch logs would swamp the
    # depth tables (e2e_dispatch.py convention).
    import logging

    import structlog

    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR))

    t0 = time.perf_counter()
    results = asyncio.run(_go(args))
    print(f"\n[depth-spike] total wall {time.perf_counter() - t0:.1f}s")

    print_results(results)

    out: dict[str, object] = {
        "config": {
            "dsn": "<redacted>",
            "schema": args.schema,
            "mode": args.mode,
            "limit": DISPATCH_LIMIT,
            "oversample": OVERSAMPLE,
            "rounds": args.rounds,
        },
        "results": results,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    name = args.json_out or f"depth-spike-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    path = RESULTS_DIR / name
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"[depth-spike] json: {path}")
    print(f"[depth-spike] scratch schema '{args.schema}' dropped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
