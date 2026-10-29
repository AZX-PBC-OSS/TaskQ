"""T20's STREAMING BANDS — the perf evidence, re-measured ON THE BUILT
CODE (the spike's §7 protocol, the shipped surfaces):

1. THE EMIT TX per page (40 chain starts + edges + the cursor
   checkpoint, ONE tx — ``taskq.workflows._emit.emit_batch``).
2. THE REAL certified dispatch band (the backend's own ``_dispatch_batch``
   — the shipped claim SQL, unchanged) at a 200-CHAIN backlog.
3. The plain-jobs baseline @ 200 (the same queue mechanics, no workflow
   shape) + the ratio.
4. The 800-row mixed backlog (the depth-invariance probe).
5. THE CHAIN-STEP FORK tx (the routed finalize: the fenced terminal mark
   + ONE child + its edge — ``finalize_node`` with the chain's ForkSpec).

Raw output: ``.measurements/t20-streaming-bands.json`` (the capture law:
every run lands a timestamped file beside it). The script is self-contained:
apply-migrate a scratch schema, measure, drop.

    uv run python scripts/measure_t20_streaming_bands.py [--dsn DSN] [--schema S]
"""

# ruff: noqa: S608  # Why: one-off measurement script; the schema name is the operator's own --schema argument (a scratch schema the script drops at exit), never untrusted input.

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._dispatch import _dispatch_batch
from taskq.backend._protocol import JobId
from taskq.backend._sql_templates import render as render_backend_sql
from taskq.workflows._emit import emit_batch
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import EmitChild, ForkSpec
from taskq.workflows.engine import finalize_node, render_workflow_sql

MIGRATIONS = Path(__file__).parent.parent / "src" / "taskq" / "migrations"
MEASUREMENTS = Path(__file__).parent.parent / ".measurements"

BASELINE_QUEUE = "t20-perf-base"
CHAIN_QUEUE = "t20-perf-chain"


def band(latencies: list[float]) -> dict[str, float]:
    latencies = sorted(latencies)
    return {
        "p50_ms": round(latencies[len(latencies) // 2], 3),
        "p95_ms": round(latencies[int(len(latencies) * 0.95)], 3),
        "max_ms": round(latencies[-1], 3),
        "rounds": len(latencies),
    }


async def apply_migrations(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    files = sorted(f for f in MIGRATIONS.glob("*.sql") if "_variants" not in f.parts)
    for f in files:
        sql = f.read_text().replace("{schema}", schema).replace("{{", "{").replace("}}", "}")
        async with conn.transaction():
            await conn.execute(sql)


def page_children(page: list[int], flow_id: object) -> list[EmitChild]:
    return [
        EmitChild(
            step_key="screen",
            actor="t20-chain",
            queue=CHAIN_QUEUE,
            payload={"application": {"app_id": i}},
            trace_id=f"app-{i}",
            map_index=i,
        )
        for i in page
    ]


async def dispatch_band(
    pool: asyncpg.Pool, schema: str, queue: str, rounds: int = 30
) -> dict[str, float]:
    """The REAL certified dispatch claim (the shipped `_dispatch_batch`),
    the backlog held constant by re-pending each claimed row."""
    lats: list[float] = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        rows = await _dispatch_batch(
            pool,
            render_backend_sql(schema),
            2,
            5.0,
            schema,
            new_uuid(),
            [queue],
            25,
            timedelta(seconds=3),
        )
        lats.append((time.perf_counter() - t0) * 1000)
        if rows:
            await pool.execute(
                f"UPDATE \"{schema}\".jobs SET status='pending', locked_by_worker=NULL, "
                "lock_expires_at=NULL, attempt=GREATEST(attempt-1,0), "
                "claim_epoch=claim_epoch-1 WHERE id = ANY($1)",
                [r.id for r in rows],
            )
        await asyncio.sleep(0.002)
    return band(lats)


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default="postgresql://taskq:taskq@localhost:5704/taskq")
    parser.add_argument("--schema", default="t20_perf")
    parser.add_argument("--page-size", type=int, default=40)
    parser.add_argument("--pages", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=30)
    args = parser.parse_args()

    schema = args.schema
    conn = await asyncpg.connect(args.dsn)
    await apply_migrations(conn, schema)
    await conn.close()

    pool = await asyncpg.create_pool(args.dsn, min_size=2, max_size=10)
    wsql: WorkflowSql = render_workflow_sql(schema)
    results: dict[str, object] = {
        "measured_at": datetime.now(UTC).isoformat(),
        "dsn_schema": schema,
        "page_size": args.page_size,
        "jit": "off (the container's tuning; fsync/synchronous_commit off)",
    }

    # ── the flow + source scaffolding (the emit tx's subjects) ────────
    flow_id = new_uuid()
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'wf', 'default', '{}', 10, 'transient', 'running', '__flow__', "
            "$2::jsonb, 'workflow-run', $3)",
            flow_id,
            json.dumps({"flow_id": str(flow_id)}),
            f"flow:{flow_id}",
        )
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, max_pending, queue) '
            "VALUES ('t20-chain', NULL, NULL, $1) ON CONFLICT DO NOTHING",
            CHAIN_QUEUE,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name) VALUES ($1), ($2) ON CONFLICT DO NOTHING',
            CHAIN_QUEUE,
            BASELINE_QUEUE,
        )
    source_id = new_uuid()
    # THE CLAIM VIEW (the emit's cursor checkpoint fences on it): ONE
    # worker id, minted once, written by the seed AND passed to the emit.
    worker = new_uuid()
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, attempt, locked_by_worker, lock_expires_at, "
            "claim_epoch, step_key, metadata) "
            "VALUES ($1, 'wf', 'default', '{}', 10, 'transient', 'running', 1, $2, "
            "now() + interval '90 seconds', 0, 'source', $3::jsonb)",
            source_id,
            worker,
            json.dumps({"flow_id": str(flow_id)}),
        )

    # ── 1. THE EMIT TX per page ──────────────────────────────────────
    emit_latencies: list[float] = []
    for page in range(args.pages):
        children = page_children(
            list(range(page * args.page_size, (page + 1) * args.page_size)), flow_id
        )
        # the source row's claim view: attempt 1, epoch 0, worker set above
        t0 = time.perf_counter()
        await emit_batch(
            pool,
            wsql,
            flow_id=JobId(flow_id),
            source_id=JobId(source_id),
            worker_id=JobId(worker),
            attempt=1,
            claim_epoch=0,
            children=children,
            cursor={"page": page},
        )
        emit_latencies.append((time.perf_counter() - t0) * 1000)
    results["emit_tx_per_page"] = band(emit_latencies)

    # ── 2. the dispatch band @ the chain backlog ─────────────────────
    results["dispatch_band_200_chain"] = await dispatch_band(pool, schema, CHAIN_QUEUE, args.rounds)

    # ── 3. the plain baseline @ 200 ──────────────────────────────────
    rows = [
        (str(new_uuid()), "t20-vanilla", BASELINE_QUEUE, json.dumps({"v": 1}), 3, "transient")
        for _ in range(200)
    ]
    async with pool.acquire() as conn:
        await conn.executemany(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind) '
            "VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
            rows,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, max_pending, queue) '
            "VALUES ('t20-vanilla', NULL, NULL, $1) ON CONFLICT DO NOTHING",
            BASELINE_QUEUE,
        )
    results["dispatch_band_200_plain"] = await dispatch_band(
        pool, schema, BASELINE_QUEUE, args.rounds
    )

    # ── 4. the 800-row mixed backlog ─────────────────────────────────
    extra = [
        (str(new_uuid()), "t20-chain", CHAIN_QUEUE, json.dumps({"v": 1}), 3, "transient")
        for _ in range(600)
    ]
    async with pool.acquire() as conn:
        await conn.executemany(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind) '
            "VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
            extra,
        )
    results["dispatch_band_800_mixed"] = await dispatch_band(pool, schema, CHAIN_QUEUE, args.rounds)

    # ── 5. the chain-step fork tx (the routed finalize) ──────────────
    # One chain row claimed + finalized THROUGH the route's one-child
    # fork: the fenced terminal mark + the child + the edge (tx1), then
    # the guarded decrement (tx2 — no join; the chain has none).
    from taskq.workflows._types import ChildSpec

    fork_latencies: list[float] = []
    async with pool.acquire() as conn:
        pending = await conn.fetch(
            f"SELECT id FROM \"{schema}\".jobs WHERE queue = $1 AND status = 'pending' "
            "AND step_key = 'screen' LIMIT 50",
            CHAIN_QUEUE,
        )
    for r in pending:
        chain_worker = new_uuid()
        async with pool.acquire() as conn:
            rec = await conn.fetchrow(
                f"UPDATE \"{schema}\".jobs SET status='running', started_at=now(), "
                "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
                "locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds', "
                "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
                "RETURNING attempt, claim_epoch",
                r["id"],
                chain_worker,
            )
        if rec is None:
            continue
        map_index = int(
            await pool.fetchval(
                f'SELECT COALESCE(map_index, 0) FROM "{schema}".jobs WHERE id = $1', r["id"]
            )
        )
        t0 = time.perf_counter()
        result = await finalize_node(
            pool,
            wsql,
            flow_id=JobId(flow_id),
            job_id=JobId(r["id"]),
            step_key="screen",
            worker_id=JobId(chain_worker),
            attempt=int(rec["attempt"]),
            claim_epoch=int(rec["claim_epoch"]),
            outcome="succeeded",
            result={"value": "clean"},
            fork=ForkSpec(
                children=(
                    ChildSpec(
                        step_key="enrich",
                        actor="t20-chain",
                        queue=CHAIN_QUEUE,
                        payload={"application": {"app_id": map_index}},
                        map_index=map_index,
                    ),
                ),
                join=None,
                trace_id=f"app-{map_index}",
                max_attempts=3,
                retry_kind="transient",
            ),
            map_index=map_index,
        )
        assert result.applied
        fork_latencies.append((time.perf_counter() - t0) * 1000)
    results["chain_fork_tx"] = band(fork_latencies) if fork_latencies else "not measured"

    out = MEASUREMENTS / "t20-streaming-bands.json"
    MEASUREMENTS.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2))
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    (MEASUREMENTS / f"t20-streaming-bands-{stamp}.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))

    await pool.close()
    conn2 = await asyncpg.connect(args.dsn)
    await conn2.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn2.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
