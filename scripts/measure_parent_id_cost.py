# ruff: noqa: S608, T201  # Why: a measurement script, not a test; prints its own table.

"""LIB-2 (issue #670) cost measurements: the parent_id column + partial
index + the pending-children aggregate, as NUMBERS for the PR body.

Run against one PG server, once per tree, on separate databases/schemas:

    # BASELINE (origin/main checkout — the column does not exist there):
    python scripts/measure_parent_id_cost.py \
        --src /tmp/opencode/baseline670/src --dsn $DSN --schema bench_base \
        --seed 100000 --stages write,migration

    # FEATURE (this tree — column + index + the staged reads):
    python scripts/measure_parent_id_cost.py \
        --src /tmp/opencode/feat670/src --dsn $DSN --schema bench_feat \
        --seed 100000 --stages write,count

The ``migration`` stage (baseline tree only, by construction) times the
column + index on an ALREADY-POPULATED table: it applies the baseline
migrations, seeds, then replays the feature migration's three statements
verbatim — the operationally relevant case, an upgrade of an existing
deployment whose jobs table already holds rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

_MIGRATION_SQL = (
    'ALTER TABLE "{s}".jobs ADD COLUMN parent_id uuid',
    'ALTER TABLE "{s}".jobs_archive ADD COLUMN parent_id uuid',
    'CREATE INDEX IF NOT EXISTS jobs_parent_pending_idx '
    'ON "{s}".jobs (parent_id) '
    "WHERE status IN ('pending', 'scheduled') AND parent_id IS NOT NULL",
)


def _load_tree(src: str) -> None:
    sys.path.insert(0, src)
    for mod in list(sys.modules):
        if mod == "taskq" or mod.startswith("taskq."):
            del sys.modules[mod]


async def _open_backend(dsn: str, schema: str) -> tuple[object, object]:
    """Apply THIS tree's migrations on a fresh schema, open its backend."""
    import asyncpg

    from taskq.migrate import apply_pending
    from taskq.testing.fixtures import _open_pg_backend_on_schema

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    stack, _deps, backend = await _open_pg_backend_on_schema(dsn, schema)
    return stack, backend


def _mk_args(n: int, queue: str) -> list[object]:
    from taskq._ids import new_job_id
    from taskq.backend._protocol import EnqueueArgs

    return [
        EnqueueArgs(
            id=new_job_id(),
            actor="bench_actor",
            queue=queue,
            payload={"value": i},
            max_attempts=3,
            retry_kind="transient",  # type: ignore[arg-type]
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        for i in range(n)
    ]


async def _seed_hot(backend: object, seed: int) -> float:
    """Seed the hot table through the real COPY arm; returns elapsed ms."""
    t0 = time.perf_counter()
    chunk = 5_000
    for i in range(0, seed, chunk):
        written = await backend.enqueue_batch_fast(_mk_args(min(chunk, seed - i), "hot_q"))  # type: ignore[attr-defined]
        assert written > 0  # noqa: S101 — the COPY arm must write what it is given
    return (time.perf_counter() - t0) * 1000


async def _seed_children(backend: object, parent: UUID, n: int, queue: str) -> None:
    children = [replace(a, parent_id=parent) for a in _mk_args(n, queue)]  # type: ignore[arg-type, union-attr]
    chunk = 5_000
    for i in range(0, n, chunk):
        await backend.enqueue_batch_fast(children[i : i + chunk])  # type: ignore[attr-defined]


async def stage_write(backend: object, parent: UUID) -> dict[str, float]:
    """COPY bursts, min of 5, unparented vs parented, one warm table."""

    async def burst(stamped: bool, n: int = 2_000) -> float:
        args = _mk_args(n, "cost_q")
        if stamped:
            args = [replace(a, parent_id=parent) for a in args]  # type: ignore[arg-type, union-attr]
        t0 = time.perf_counter()
        await backend.enqueue_batch_fast(args)  # type: ignore[attr-defined]
        return (time.perf_counter() - t0) * 1000

    await burst(False, 500)
    unparented = min([await burst(False) for _ in range(5)])
    out: dict[str, float] = {
        "copy_unparented_ms_per_2000": unparented,
    }
    from taskq.backend._protocol import EnqueueArgs  # noqa: PLC0415

    if "parent_id" in {f.name for f in EnqueueArgs.__dataclass_fields__.values()}:
        await burst(True, 500)
        out["copy_parented_ms_per_2000"] = min([await burst(True) for _ in range(5)])
        out["parented_over_unparented"] = out["copy_parented_ms_per_2000"] / unparented
    else:
        # The baseline tree: the column does not exist, the parented arm
        # is the FEATURE run's comparison.
        out["copy_parented_ms_per_2000"] = -1.0
        out["parented_over_unparented"] = -1.0
    return out


async def stage_count(backend: object, parent: UUID) -> dict[str, float]:
    """count_pending_children_by_queue latency at 10/100/1k/10k children."""
    out: dict[str, float] = {}
    for n in (10, 100, 1_000, 10_000):
        await _seed_children(backend, parent, n, "fan_q")
        times: list[float] = []
        for _ in range(5):
            t0 = time.perf_counter()
            await backend.count_pending_children_by_queue(parent)  # type: ignore[attr-defined]
            times.append((time.perf_counter() - t0) * 1000)
        out[f"count_children_{n}_ms"] = sorted(times)[0]
    return out


async def stage_plan(backend: object, parent: UUID, schema: str) -> dict[str, str]:
    """The EXPLAIN plan shape on the populated hot table (the pin's evidence)."""
    pool = backend._worker_pool  # noqa: SLF001
    plan_raw = await pool.fetchval(  # type: ignore[union-attr]
        f"""
        EXPLAIN (FORMAT JSON)
        SELECT queue, count(*)::int FROM "{schema}".jobs
        WHERE parent_id = $1 AND status IN ('pending', 'scheduled')
        GROUP BY queue
        """,
        parent,
    )
    plan = json.loads(plan_raw) if isinstance(plan_raw, str) else plan_raw
    # Walk the plan tree: the top node is the Aggregate, the index use
    # (or the scan) lives in its children.
    index_names: list[str] = []
    node_types: list[str] = []

    def _walk(node: dict[str, object]) -> None:
        node_types.append(str(node.get("Node Type")))
        if node.get("Index Name") is not None:
            index_names.append(str(node["Index Name"]))
        for child in node.get("Plans", []) or []:
            _walk(child)  # type: ignore[arg-type]

    _walk(plan[0]["Plan"])  # type: ignore[index]
    return {"plan_nodes": " -> ".join(node_types), "plan_indexes": ", ".join(index_names) or "NONE"}


async def stage_migration(dsn: str, schema: str, seed: int) -> dict[str, float]:
    """Baseline tree only: seed on the UNMIGRATED-for-parent_id schema,
    then replay the feature migration's statements with timings."""
    import asyncpg

    from taskq.migrate import apply_pending
    from taskq.testing.fixtures import _open_pg_backend_on_schema

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    stack, backend = await _open_backend(dsn, schema)
    try:
        elapsed = await _seed_hot(backend, seed)
        print(f"  (seed {seed} rows: {elapsed:.0f}ms)")
    finally:
        await stack.aclose()  # type: ignore[union-attr]

    out: dict[str, float] = {}
    conn = await asyncpg.connect(dsn)
    try:
        for i, statement in enumerate(_MIGRATION_SQL):
            stmt = statement.replace("{s}", schema)
            t0 = time.perf_counter()
            await conn.execute(stmt)
            out[f"migration_stmt_{i}_ms"] = (time.perf_counter() - t0) * 1000
    finally:
        await conn.close()
    return out


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True, help="the tree's src/ to measure")
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--seed", type=int, default=100_000)
    parser.add_argument("--stages", default="write")
    ns = parser.parse_args()

    _load_tree(ns.src)
    stages = set(ns.stages.split(","))

    stack = backend = None
    results: dict[str, object] = {}
    if stages & {"write", "count", "plan"}:
        stack, backend = await _open_backend(ns.dsn, ns.schema)
        # A synthetic parent id: the count never joins to the parent row
        # (plain column, no FK), a parent that exists only as the ledger
        # stamp on its children is the exact shape the aggregate reads.
        parent = UUID(int=1)

        print(f"== seeding {ns.seed} rows via the COPY arm ==")
        t0 = time.perf_counter()
        await _seed_hot(backend, ns.seed)
        print(f"  seed: {(time.perf_counter() - t0) / 1000:.1f}s")

        if "write" in stages:
            results["write"] = await stage_write(backend, parent)
        if "count" in stages:
            results["count"] = await stage_count(backend, parent)
        if "plan" in stages:
            results["plan"] = await stage_plan(backend, parent, ns.schema)
        await stack.aclose()  # type: ignore[union-attr]

    if "migration" in stages:
        results["migration_on_populated"] = await stage_migration(ns.dsn, ns.schema, ns.seed)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
