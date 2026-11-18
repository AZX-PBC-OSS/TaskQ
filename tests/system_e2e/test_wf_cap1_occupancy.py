# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The §9 control 7: the occupancy at the cap — `max_in_flight=1`, the
consuming child's own `max_concurrent=1` (the actor's own capacity gate,
seeded on the child cohort's `actor_config` row), all slots occupied by
same-run children plus one parked source. Terminalizing one child makes
progress WITHOUT a free slot for the wake (the promotion is the drain's
own row flip under the run's locks — a wake is never a worker task), the
next single-record page emits and the run completes, and UNRELATED work
on the same capped actor progresses through the same gate.

RATIONALE (the contract's own naming): this is the occupancy failure
Kestra/Argo-class engines exhibit — the wake of a blocked run is
delivered BY a worker task that consumes the same capacity the run's own
children hold; with every slot occupied by same-run children the wake's
own task queues behind the children, and the run can never make
progress. TaskQ's park/wake pair is slot-free by construction: the park
releases the source's claim entirely (no owner, no lease, no slot), and
the promotion is a row flip the drain performs — the control proves the
law at the cap.

THE DURABLE PROOFS (no live-window race):
* **the cap's own serial law**: every child of the capped actor ran
  inside a pairwise-DISJOINT ``[started_at, finished_at]`` interval —
  read from the rows after the fact; a second concurrent child under
  ``max_concurrent=1`` is the cap's own violation, and any occupancy
  deadlock (the wake needing a slot) shows as a run that never
  completes;
* **the park/wake record**: the source's own ``wakes`` counter (a wake
  follows only a park) with the logical attempt unchanged — the run's
  own park → wake → next-page cycle happened under the cap;
* **the unrelated work's own progress**: the second run's child ran and
  completed THROUGH the same capped actor.

Both one-worker and two-worker variants run the same shape: the pod
count changes who executes the bodies, never the wake's slot-free law.
"""

from __future__ import annotations

import asyncio
import json as _json
import time
from typing import Any

import asyncpg
import pytest

from taskq.testing.fixtures import ModulePgSchema
from tests.system_e2e import _p2_saturation_worker as entry

pytestmark = [pytest.mark.system, pytest.mark.integration]

_QUEUES = f"{entry.SOURCE_QUEUE},{entry.C7_CHILD_QUEUE}"


async def _seed_gate(pool: Any, schema: str, hold_s: float) -> None:
    await pool.execute(
        f'CREATE TABLE IF NOT EXISTS "{schema}".p2sat_c7_gate (id int PRIMARY KEY, hold_s float8)'
    )
    await pool.execute(
        f'INSERT INTO "{schema}".p2sat_c7_gate (id, hold_s) VALUES (1, $1) '
        "ON CONFLICT (id) DO UPDATE SET hold_s = $1",
        hold_s,
    )


async def _run_variant(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any, workers: int
) -> None:
    from taskq.workflows import FlowRunner
    from tests.system_e2e._harness import spawn_joined_worker

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    conn = await asyncpg.connect(pg_dsn)
    pods: list[Any] = []
    try:
        await _seed_gate(pool, schema, 6.0)
        # THE CONSUMING CHILD'S OWN CAPACITY GATE: the child cohort's own
        # actor_config row carries max_concurrent = 1 — seeded by the
        # control (the shipped operator-side pattern; the projection's
        # default capacity_owner='operator' never overwrites an
        # operator's own capacity).
        runner = FlowRunner(entry.p2sat_app.get("c7_occupancy_flow"), pool, schema)
        flow_id = str((await runner.create_flow()).flow_id)
        await pool.execute(
            f"""INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent, metadata)
                VALUES ($1, $2, 1, $3::jsonb)
                ON CONFLICT (actor) DO UPDATE SET max_concurrent = 1""",
            "p2sat_c7_child",
            entry.C7_CHILD_QUEUE,
            _json.dumps({"workflow_actor": True}),
        )
        row = await pool.fetchrow(
            f'SELECT max_concurrent FROM "{schema}".actor_config WHERE actor = $1',
            "p2sat_c7_child",
        )
        assert row is not None and int(row["max_concurrent"]) == 1, row
        # THE UNRELATED WORK: a second run on the SAME capped actor — its
        # own child queues behind run A's (the cap's serial law) and runs
        # through the same gate (the unrelated work's own progress).
        other_flow = str((await runner.create_flow()).flow_id)

        for index in range(workers):
            pods.append(
                await spawn_joined_worker(
                    conn,
                    pg_dsn,
                    schema,
                    tag=f"c7-occ-{workers}w-{index}",
                    entry="tests.system_e2e._p2_saturation_worker",
                    log_sink=str(log_dir / f"p2-system-c7-occ-{workers}w-{index}.worker.log"),
                    extra_env={
                        "TASKQ_QUEUES": _QUEUES,
                        "TASKQ_MAX_CONCURRENCY": "4",
                    },
                )
            )

        # THE COMPLETION: the capped actor serialises every child; the
        # park/wake cycle runs under it (slot-free); both runs complete.
        async def _all_terminal() -> bool:
            roots = await pool.fetch(
                f"""SELECT status::text AS status FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = ANY($1::uuid[])
                      AND step_key = '__flow__'""",
                [flow_id, other_flow],
            )
            return len(roots) == 2 and all(r["status"] == "succeeded" for r in roots)

        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            if await _all_terminal():
                break
            await asyncio.sleep(0.5)
        else:
            states = await pool.fetch(
                f"""SELECT step_key, map_index::int AS m, status::text AS status
                    FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = ANY($1::uuid[])""",
                [flow_id, other_flow],
            )
            pytest.fail(
                "the runs never completed under the capped actor: "
                + str([(r["step_key"], r["m"], r["status"]) for r in states])
            )

        # THE CAP'S OWN SERIAL LAW, DURABLY: the capped actor's children's
        # running intervals are pairwise DISJOINT — a second concurrent
        # child under max_concurrent=1 is the cap's own violation.
        kids = await pool.fetch(
            f"""SELECT (metadata->>'flow_id')::uuid AS f, map_index::int AS m,
                       started_at, finished_at
                FROM "{schema}".jobs
                WHERE actor = 'p2sat_c7_child' AND status = 'succeeded'
                ORDER BY started_at"""
        )
        assert len(kids) >= 4, len(kids)
        for i in range(len(kids) - 1):
            for j in range(i + 1, len(kids)):
                a, b = kids[i], kids[j]
                if a["f"] == b["f"] and a["m"] == b["m"]:
                    continue
                overlap = a["started_at"] < b["finished_at"] and b["started_at"] < a["finished_at"]
                assert not overlap, (
                    f"the capped actor ran two children concurrently "
                    f"(max_concurrent=1 violated): {[dict(r) for r in (a, b)]}"
                )

        # THE PARK/WAKE RECORD: the source's own wakes counter (a wake
        # follows only a park), the logical attempt unchanged, every child
        # ran EXACTLY once.
        for run_flow in (flow_id, other_flow):
            src_meta = await pool.fetchval(
                f"""SELECT metadata FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
                run_flow,
            )
            doc = _json.loads(src_meta) if isinstance(src_meta, str) else dict(src_meta)
            pressure = doc.get("wf_emit_backpressure") or {}
            assert int(pressure.get("wakes", 0)) >= 1, (run_flow, doc)
            assert doc.get("wf_source_logical_attempt") == 1, doc
            run_kids = [r for r in kids if str(r["f"]) == run_flow]
            assert len(run_kids) == 2, run_kids
            assert all(r["started_at"] is not None for r in run_kids)

        print(
            f"C7-OCCUPANCY-COMPLETE workers={workers} children={len(kids)} "
            "cap_serial_law=pairwise_disjoint park_wake_cycle=completed",
            flush=True,
        )
    finally:
        for pod in pods:
            if pod.proc.poll() is None:
                pod.proc.kill()
                await asyncio.to_thread(pod.proc.wait)
        await conn.close()
        await pool.close()


async def test_cap_one_child_cap_one_never_occupancy_deadlocks_one_worker(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """The one-worker variant."""
    await _run_variant(pg_dsn, module_pg_schema, log_dir, workers=1)


async def test_cap_one_child_cap_one_never_occupancy_deadlocks_two_workers(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """The two-worker variant."""
    await _run_variant(pg_dsn, module_pg_schema, log_dir, workers=2)
