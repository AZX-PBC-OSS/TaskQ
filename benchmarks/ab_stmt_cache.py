"""Statement-cache thrash probe against a real Postgres.

Hypothesis: asyncpg caches prepared statements keyed by SQL *text*
(per-connection LRU, ``statement_cache_size=100`` default,
``max_cached_statement_lifetime=300``). TaskQ renders ``list_jobs`` /
``cancel_where`` WHERE clauses dynamically (``_filter_sql.py`` +
``_cursor.py``), so if filter combinations vary across calls, each new
text is a Parse/Describe round trip — and when distinct texts exceed the
cache cap, LRU eviction turns every call into a miss ("thrash").

This probe:

- enumerates the SQL texts ``_list_jobs`` actually emits, using the real
  rendering helpers (``build_filter_conditions`` + ``ordering_for`` +
  ``JobOrdering.sql_after``), so variant cardinality is measured, not guessed;
- times ``backend.list_jobs`` through the real ``PostgresBackend`` under
  (a) a fixed filter and (b) rotating filter sets of 10/100/1000/all
  distinct combos, reporting ms/call plus real prepare (miss) counts;
- isolates the Parse/Describe cost with raw ``conn.fetch`` timings
  (fixed vs rotating vs ``statement_cache_size=0``);
- (``--mode e2e``) runs dispatch loops with a concurrent ``list_jobs``
  hammer + sweeps on a shared pool, timing calls and sampling
  ``pg_stat_activity`` for contention signals.

Setup follows the integration-tier conventions (``benchmarks/e2e_dispatch.py``):
DSN defaults to the compose Postgres, a dedicated schema is created and
migrated with ``taskq.migrate.apply_pending`` and truncated at the end.

Read-only with respect to src/; writes only its own artifacts under
results/.

Usage:
    python benchmarks/ab_stmt_cache.py                  # fixed vs rotating
    python benchmarks/ab_stmt_cache.py --mode raw       # per-miss overhead
    python benchmarks/ab_stmt_cache.py --mode e2e       # pool contention
    python benchmarks/ab_stmt_cache.py --calls 2000
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import statistics
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import asyncpg

from taskq._ids import new_job_id, new_uuid
from taskq.backend._cursor import ordering_for
from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._protocol import EnqueueArgs, JobFilter, JobSortField
from taskq.backend.postgres import PostgresBackend

# ── Configuration (e2e_dispatch.py conventions) ─────────────────────────

DEFAULT_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
DEFAULT_SCHEMA = "tq_bench_stmt_cache"
ACTOR = "bench_actor"
QUEUE = "default"

RESULTS_DIR = Path(__file__).parent / "results"

# ── Variant enumeration ──────────────────────────────────────────────────
#
# Reproduce _list_jobs' SQL assembly (src/taskq/backend/_reads.py:56-104)
# with the same helpers, purely in-process, so we can count distinct texts
# without a database.  A JobFilter present/absent per predicate field
# x order_by x cursor-seam shape is the entire cardinality of the text:
# values never enter the SQL (all bound via $n).

_SCHEMA = DEFAULT_SCHEMA  # rendered into the reproduced texts only


def build_query(filter: JobFilter) -> tuple[str, list[Any]]:
    """Exact mirror of ``_list_jobs``' assembly (text AND params).

    Used both to *count* distinct texts and to drive raw ``conn.fetch``
    with consistent (text, params) pairs.
    """
    filter_sql = build_filter_conditions(filter)
    conditions: list[str] = list(filter_sql.conditions)
    params: list[Any] = list(filter_sql.params)
    n = len(params)
    ordering = ordering_for(filter.order_by)
    if filter.cursor is not None:
        cursor_sql, cursor_params = ordering.sql_after(ordering.decode(filter.cursor), n + 1)
        conditions.append(cursor_sql)
        params.extend(cursor_params)
        n += len(cursor_params)
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    n += 1
    params.append(filter.limit)
    text = (  # Why: schema is a bench-controlled identifier; the WHERE/ORDER BY fragments are rendered from typed JobFilter fields and every value is bound via $n (mirrors _reads._list_jobs).
        f'SELECT * FROM "{_SCHEMA}".jobs {where_clause} '  # noqa: S608
        f"{f'ORDER BY {ordering.order_by_sql()} '}LIMIT ${n}"
    )
    return text, params


def enumerate_filter_specs() -> list[tuple[str, JobFilter]]:
    """Every legitimate ``JobFilter`` presence pattern x ordering x seam.

    Returns ``(label, filter)`` pairs deduped by the SQL text they emit —
    that deduped set is the variant cardinality asyncpg's per-connection
    statement cache sees for a fixed schema.
    """
    orderings: list[JobSortField | None] = [
        None,
        JobSortField.SCHEDULED_AT_ASC,
        JobSortField.CREATED_AT_DESC,
        JobSortField.FINISHED_AT_DESC,
    ]
    statuses: list[tuple[str, Any]] = [
        ("", None),
        ("st", "pending"),
        ("stany", ("pending", "running")),
        ("act", True),
        ("actF", False),
    ]
    specs: list[tuple[str, JobFilter]] = []
    for has_queue, (
        st_label,
        st,
    ), has_actor, has_ikey, has_batch, has_tags, ob in itertools.product(
        [False, True],
        statuses,
        [False, True],
        [False, True],
        [False, True],
        [False, True],
        orderings,
    ):
        kw: dict[str, Any] = {"limit": 50}
        label_parts = []
        if has_queue:
            kw["queue"] = QUEUE
            label_parts.append("q")
        if st_label == "st" or st_label == "stany":
            kw["status"] = st
            label_parts.append(st_label)
        elif st_label:
            kw["active"] = st
            label_parts.append(st_label)
        if has_actor:
            kw["actor"] = ACTOR
            label_parts.append("a")
        if has_ikey:
            kw["identity_key"] = "idem-key"
            label_parts.append("ik")
        if has_batch:
            kw["batch_id"] = new_uuid()
            label_parts.append("b")
        if has_tags:
            kw["tags"] = ("alpha", "beta")
            label_parts.append("t")
        if ob is not None:
            kw["order_by"] = ob
            label_parts.append(ob.name.removesuffix("_ASC").removesuffix("_DESC").lower())
        base_label = "+".join(label_parts) or "none"
        specs.append((base_label, JobFilter(**kw)))
        # A NULL seam (empty cursor field on the nullable ordering) renders
        # "col IS NULL" terms — a distinct text.  Only FINISHED_AT_DESC has a
        # nullable lead column.
        if ordering_for(ob).columns[0].nullable:
            specs.append(
                (base_label + "/nullseam", JobFilter(**{**kw, "cursor": f"|{new_uuid()}"}))
            )
    seen: dict[str, tuple[str, JobFilter]] = {}
    for label, f in specs:
        seen.setdefault(build_query(f)[0], (label, f))
    return list(seen.values())


# ── Miss counter (private-API poke, bench-only) ─────────────────────────


class MissCounter:
    """Counts real prepared-statement cache *puts* (i.e. misses) process-wide."""

    def __init__(self) -> None:
        self.prepares = 0
        self._orig_put: Any = None

    def install(self) -> None:
        from asyncpg.connection import _StatementCache

        counter = self
        orig_put = _StatementCache.put

        def put(self_cache: Any, key: Any, statement: Any) -> None:
            counter.prepares += 1
            orig_put(self_cache, key, statement)

        _StatementCache.put = put  # type: ignore[method-assign]
        self._orig_put = orig_put

    def uninstall(self) -> None:
        if self._orig_put is not None:
            from asyncpg.connection import _StatementCache

            _StatementCache.put = self._orig_put  # type: ignore[method-assign]


# ── Timing helpers ───────────────────────────────────────────────────────


def stats_ms(durs: list[float]) -> dict[str, float]:
    ms = [d * 1000 for d in durs]
    return {
        "mean_ms": statistics.fmean(ms),
        "p50_ms": statistics.median(ms),
        "p95_ms": ms[int(0.95 * (len(ms) - 1))],
        "max_ms": max(ms),
    }


# ── Seeding ──────────────────────────────────────────────────────────────


async def seed_jobs(backend: PostgresBackend, n: int = 200) -> None:
    args = [
        EnqueueArgs(
            id=new_job_id(),
            actor=ACTOR,
            queue=QUEUE,
            payload={"i": i},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
            priority=i % 3,
        )
        for i in range(n)
    ]
    await backend.enqueue_batch(args)


# ── Mode: backend regimes ────────────────────────────────────────────────


async def run_backend_regimes(
    backend: PostgresBackend, specs: list[tuple[str, JobFilter]], calls: int
) -> dict[str, Any]:
    """list_jobs through the real backend: fixed vs rotating k∈{10,100,1000,all}."""
    results: dict[str, Any] = {}
    counter = MissCounter()
    counter.install()
    try:
        await backend.list_jobs(specs[0][1])  # warm type introspection etc.

        async def regime(name: str, rot: list[tuple[str, JobFilter]]) -> None:
            durs = []
            counter.prepares = 0
            for i in range(calls):
                _, f = rot[i % len(rot)]
                t0 = time.perf_counter()
                await backend.list_jobs(f)
                durs.append(time.perf_counter() - t0)
            s = stats_ms(durs)
            s["prepares"] = counter.prepares
            s["miss_rate"] = counter.prepares / calls
            results[name] = s

        label0 = specs[0][0]
        await regime(f"fixed[{label0}]", specs[:1])
        for k in (10, 100, 1000):
            if k > len(specs):
                continue
            await regime(f"rotating[k={k}]", specs[:k])
        await regime(f"rotating[k={len(specs)}]=every-request", specs)
    finally:
        counter.uninstall()
    return results


# ── Mode: raw conn.fetch (Parse/Describe isolation) ─────────────────────


async def run_raw_regimes(pool: asyncpg.Pool, calls: int, dsn: str) -> dict[str, Any]:
    """Raw ``conn.fetch`` with generated texts — isolates Parse/Describe cost."""
    specs = enumerate_filter_specs()
    pair_specs = [build_query(f) for _, f in specs]
    results: dict[str, Any] = {}
    counter = MissCounter()
    counter.install()

    async def drive(
        p: asyncpg.Pool, labels: list[str], rotating: bool, k: int | None, passes: int = 1
    ) -> None:
        """Run `passes` passes over the same sequence; one stats block per pass.

        Pass 1 over a rotating sequence is the cold/thrash regime; pass 2 is
        the warm regime over an identical filter distribution — the row
        counts (and thus decode cost) are identical, isolating the cache.
        """
        seq = pair_specs if rotating else pair_specs[:1]
        if k:
            seq = seq[:k]
        for n_pass in range(passes):
            durs = []
            counter.prepares = 0
            async with p.acquire() as conn:
                warm_text, warm_params = seq[0]
                await conn.fetch(warm_text, *warm_params)  # warm codecs
                for i in range(calls):
                    text, params = seq[i % len(seq)]
                    t0 = time.perf_counter()
                    await conn.fetch(text, *params)
                    durs.append(time.perf_counter() - t0)
            label = labels[min(n_pass, len(labels) - 1)]
            s = stats_ms(durs)
            s["prepares"] = counter.prepares
            s["miss_rate"] = counter.prepares / calls
            results[label] = s

    # Single-connection pool: the per-connection LRU is the unit under test,
    # and serial calls must not spread across connections.
    pin_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
    assert pin_pool is not None
    try:
        await drive(pin_pool, ["raw fixed"], rotating=False, k=None)
        await drive(
            pin_pool,
            ["raw rotating[cold pass]", "raw rotating[warm pass]", "raw rotating[warm 2]"],
            rotating=True,
            k=None,
            passes=3,
        )
    finally:
        await pin_pool.close()
        counter.uninstall()

    # Cache-disabled floor: statement_cache_size=0.
    floor_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1, statement_cache_size=0)
    assert floor_pool is not None
    try:
        await drive(floor_pool, ["raw nocache fixed"], rotating=False, k=None)
        await drive(floor_pool, ["raw nocache rotating[all]"], rotating=True, k=None)
    finally:
        await floor_pool.close()
    return results


# ── Mode: e2e contention ─────────────────────────────────────────────────


async def run_e2e_contention(
    backend: PostgresBackend, pool: asyncpg.Pool, seconds: float
) -> dict[str, Any]:
    """Dispatch loops + concurrent list_jobs hammer + sweeps, one shared pool."""
    stop_at = time.perf_counter() + seconds
    list_ms: list[float] = []
    counters = {"dispatched": 0, "listed": 0, "enqueued": 0}

    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{backend._schema_name}".actor_config (actor, queue) '  # noqa: S608  # Why: schema is a bench-controlled identifier (e2e_dispatch convention)
            "VALUES ($1, $2) ON CONFLICT DO NOTHING",
            ACTOR,
            QUEUE,
        )

    async def dispatcher() -> None:
        worker_id = new_uuid()
        lock_lease = timedelta(seconds=30)
        while time.perf_counter() < stop_at:
            batch_args = [
                EnqueueArgs(
                    id=new_job_id(),
                    actor=ACTOR,
                    queue=QUEUE,
                    payload={"i": i},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
                for i in range(100)
            ]
            await backend.enqueue_batch(batch_args)
            counters["enqueued"] += len(batch_args)
            while time.perf_counter() < stop_at:
                got = await backend.dispatch_batch(worker_id, [QUEUE], 50, lock_lease)
                counters["dispatched"] += len(got)
                if not got:
                    break
                for j in got:
                    await backend.mark_succeeded(j.id, worker_id, result={"ok": True})

    async def lister() -> None:
        # Admin-UI worst case: filters vary every request.  Uses the full
        # distinct-text set, which exceeds asyncpg's 100-entry cache cap.
        specs = enumerate_filter_specs()
        while time.perf_counter() < stop_at:
            for _, f in specs:
                t0 = time.perf_counter()
                await backend.list_jobs(f)
                list_ms.append((time.perf_counter() - t0) * 1000)
                counters["listed"] += 1
            await asyncio.sleep(0)

    async def sweeper() -> None:
        while time.perf_counter() < stop_at:
            await backend.scheduled_to_pending()
            await backend.deadline_sweep()
            await backend.reclaim_expired_locks(timedelta(seconds=30), timedelta(seconds=30))
            async with pool.acquire() as conn:
                await backend.sweep_leaked_reservation_slots(conn, schema=backend._schema_name)
                await backend.sweep_expired_results(conn, schema=backend._schema_name)
            await asyncio.sleep(0.05)

    async def pg_sampler() -> dict[str, int]:
        states: dict[str, int] = {}
        while time.perf_counter() < stop_at + 0.2:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT state, wait_event_type, count(*) AS n "
                    "FROM pg_stat_activity WHERE datname = current_database() "
                    "GROUP BY state, wait_event_type"
                )
            for r in rows:
                key = f"{r['state']}/{r['wait_event_type']}"
                states[key] = states.get(key, 0) + r["n"]
            await asyncio.sleep(0.1)
        return states

    t0 = time.perf_counter()
    _, _, _, _, states = await asyncio.gather(
        dispatcher(), dispatcher(), lister(), sweeper(), pg_sampler()
    )
    wall = time.perf_counter() - t0

    return {
        "wall_s": wall,
        **counters,
        "list_ms_mean": statistics.fmean(list_ms) if list_ms else 0.0,
        "list_ms_p95": sorted(list_ms)[int(0.95 * (len(list_ms) - 1))] if list_ms else 0.0,
        "pg_activity_state_samples": states,
        "pool_max_size": pool.get_max_size(),
        "pool_idle_at_end": pool.get_idle_size(),
    }


# ── Reporting ────────────────────────────────────────────────────────────


def print_table(results: dict[str, Any], calls: int) -> None:
    print(f"\n{'regime':<42} {'mean':>9} {'p50':>9} {'p95':>9} {'max':>9} {'prepares':>10}")
    print("-" * 96)
    for name, s in results.items():
        print(
            f"{name:<42} {s['mean_ms']:>7.2f}ms {s['p50_ms']:>7.2f}ms "
            f"{s['p95_ms']:>7.2f}ms {s['max_ms']:>7.2f}ms {s.get('prepares', ''):>10}"
        )
    for name, s in results.items():
        if "miss_rate" in s:
            print(
                f"{name}: {s['prepares']} prepares / {calls} calls = {s['miss_rate']:.1%} miss rate"
            )


# ── main ─────────────────────────────────────────────────────────────────


async def _setup_and_go(args: argparse.Namespace) -> dict[str, Any]:
    from e2e_dispatch import build_backend, cleanup_schema, setup_schema

    await setup_schema(args.dsn, args.schema)
    backend, _deps, pool = await build_backend(args.dsn, args.schema)
    out: dict[str, Any] = {}
    try:
        await seed_jobs(backend, n=200)
        if args.mode == "backend":
            specs = enumerate_filter_specs()
            out["distinct_sql_texts"] = len(specs)
            out["regimes"] = await run_backend_regimes(backend, specs, args.calls)
        elif args.mode == "raw":
            out["raw"] = await run_raw_regimes(pool, args.calls, args.dsn)
        elif args.mode == "e2e":
            out["e2e"] = await run_e2e_contention(backend, pool, seconds=args.seconds)
    finally:
        await cleanup_schema(args.dsn, args.schema)
        await pool.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--mode", choices=["backend", "raw", "e2e"], default="backend")
    ap.add_argument("--calls", type=int, default=1000, help="calls per regime")
    ap.add_argument("--seconds", type=float, default=10.0, help="e2e mode duration")
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    result = asyncio.run(_setup_and_go(args))

    if args.mode == "backend":
        print(f"distinct list_jobs SQL texts enumerated: {result['distinct_sql_texts']}")
        print_table(result["regimes"], args.calls)
    elif args.mode == "raw":
        print_table(result["raw"], args.calls)
    else:
        print(json.dumps(result["e2e"], indent=2, default=str))

    if args.json_out:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        path = RESULTS_DIR / args.json_out
        path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        print(f"[ab_stmt_cache] json: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
