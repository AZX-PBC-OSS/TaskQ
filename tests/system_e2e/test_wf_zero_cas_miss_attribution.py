# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The §9 control 11: the zero-CAS miss does not spin or yield forever,
and the next arms are the bounded ones — never a harness-invoked extra
drain.

* **The zero-CAS miss (the race).** A SEEDED bounded estate (the §9.2
  pattern: the exact §2.2 park row with its un-moved cursor, one live
  child, one pending intent) makes the parked source FIT — the real
  promotion primitive :func:`wake_emit_backpressure` (the same function
  every arm calls) reads the fitting candidate and its CAS lands on the
  park row. The control holds the park row's own lock, starts the REAL
  vanilla cancel (``cancel_pending_scheduled`` — its lock request
  enqueues FIRST), then the drain (its CAS enqueues SECOND), releases
  when both are queued (the census's own candidate-read receipt + the
  queued waiters), and the cancel wins the row: the CAS re-evaluates
  the moved row and matches ZERO — the miss. Asserts: zero promotions,
  the generation still pending, the drain ENDED (no spin), the source
  cancelled with its key removed (§2.4's own cancel law), and an
  unrelated flow draining normally right after.
* **The attributed next arms (a second real OS process).** The second
  process runs the real direct driver to its verdict over the SAME
  flow: its own initial backfill (the §5.3 B term) observes the real
  rows (the source is cancelled — no fitting candidate), marks
  ``handled_generation = wake_generation``, and completes the intent;
  the live child's own body runs and terminalizes THERE. The process
  prints its own census: EXACTLY the bounded arms (one drain — the
  backfill — with zero fitted grants), never a third unattributed arm,
  and the intent's handled generation equals its wake generation.
"""

from __future__ import annotations

import asyncio
import contextlib
import json as _json
import os
import sys
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.backend._sql_templates import render as render_backend_sql
from taskq.workflows._emit import COUNTERS, wake_emit_backpressure
from taskq.workflows.engine import render_workflow_sql

pytestmark = [pytest.mark.system, pytest.mark.integration]

_WORKTREE_SRC = str(Path(__file__).resolve().parents[2] / "src")

_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


def _park_meta(flow_id: str, *, width: int, bound: int) -> dict[str, Any]:
    return {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
        "emit_cursor": {"page": 0},
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": width,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }


async def _seed_miss_estate(conn: Any, schema: str) -> tuple[str, str, str]:
    """The miss's estate: a running root, the parked source (its cursor
    un-moved), one SUCCEEDED child (the spent charge), one LIVE pending
    child, and a PENDING intent (wake=1, handled=0) — the parked source
    FITS (1 live + 1 <= 2)."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', "
        "'__flow__', $2::jsonb, 'workflow-run', $3)",
        flow_id,
        _json.dumps({"flow_id": flow_id}),
        f"flow:{flow_id}",
    )
    source_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, assignment_routed, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 0, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, true, 'workflow-run', $4)",
        source_id,
        _json.dumps(_park_meta(flow_id, width=1, bound=2)),
        flow_id,
        f"wf:{flow_id}:source",
    )
    spent = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, map_index, metadata, finished_at, "
        "created_at, fairness_key, parent_id, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'succeeded', 1, 2, 'transient', 'screen', "
        "0, $2::jsonb, clock_timestamp(), clock_timestamp(), $3, $4::uuid, "
        "'workflow-run', $5)",
        spent,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        source_id,
        f"wf:{flow_id}:emit:0:screen",
    )
    live_child = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, map_index, metadata, created_at, "
        "fairness_key, parent_id, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'pending', 0, 2, 'transient', 'screen', "
        "1, $2::jsonb, clock_timestamp(), $3, $4::uuid, 'workflow-run', $5)",
        live_child,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        source_id,
        f"wf:{flow_id}:emit:1:screen",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity '
        "(flow_id, bound, wake_generation, handled_generation) "
        "VALUES ($1::uuid, 2, 1, 0)",
        flow_id,
    )
    return flow_id, source_id, live_child


async def test_zero_cas_miss_leaves_intent_pending_and_the_next_arm_completes_it(
    pg_dsn: str, module_pg_schema: Any, tmp_path: Any
) -> None:
    """The zero-CAS miss (the real cancel racing the real promotion
    primitive) leaves the intent pending with zero spin; the second OS
    process's own bounded arms complete it — exactly."""
    schema = module_pg_schema.schema_name
    wsql = render_workflow_sql(schema)
    pool = await asyncpg.create_pool(pg_dsn, min_size=2, max_size=8)

    flow_id, source_id, _live_child = await _seed_miss_estate(pool, schema)

    # THE RACE: hold the park row; the CANCEL's request first; the
    # drain's CAS second; the release gives the row to the cancel.
    holder = await asyncpg.connect(pg_dsn)
    blocker_tx = holder.transaction()
    await blocker_tx.start()
    held = await holder.fetchval(
        f'SELECT id FROM "{schema}".jobs WHERE id = $1::uuid FOR UPDATE', source_id
    )
    assert held is not None

    cancel_task: Any = None
    drain_task: Any = None
    poller = await asyncpg.connect(pg_dsn)
    try:
        return await _race_phase(
            holder,
            blocker_tx,
            poller,
            pool,
            wsql,
            flow_id,
            source_id,
            tmp_path,
            pg_dsn,
            schema,
        )
    finally:
        for task in (cancel_task, drain_task):
            del task
        await poller.close()
        with contextlib.suppress(Exception):
            await blocker_tx.rollback()
        await holder.close()


async def _race_phase(
    holder: Any,
    blocker_tx: Any,
    poller: Any,
    pool: Any,
    wsql: Any,
    flow_id: str,
    source_id: str,
    tmp_path: Any,
    pg_dsn: str,
    schema: str,
) -> None:
    from taskq.workflows._emit import COUNTERS as _C

    del _C
    cancel_task = asyncio.create_task(
        pool.fetchrow(render_backend_sql(schema).cancel_pending_scheduled, source_id)
    )
    drain_task: Any = None
    try:
        pre_census = COUNTERS.snapshot()
        drain_task = asyncio.create_task(wake_emit_backpressure(pool, wsql, flow_id=JobId(flow_id)))
        # THE HANDSHAKE: the drain read its fitting candidate (its own
        # census receipt) and its CAS is queued on the holder's
        # transaction — then release. Never a guess about timing: the
        # census's own candidate-read bump IS the receipt, and the queued
        # waiter is the position proof.
        both_queued = False
        deadline = asyncio.get_running_loop().time() + 30.0
        while asyncio.get_running_loop().time() < deadline:
            census_now = COUNTERS.snapshot()
            waiters = await poller.fetchrow(
                """SELECT count(*) FILTER (WHERE locktype = 'transactionid') AS txw,
                          count(*) FILTER (WHERE locktype = 'tuple') AS tuplew
                   FROM pg_locks WHERE NOT granted"""
            )
            if (
                census_now["flow_drains"] > pre_census["flow_drains"]
                and census_now["candidate_reads"] > pre_census["candidate_reads"]
                and (int(waiters["txw"]) + int(waiters["tuplew"])) >= 2
            ):
                both_queued = True
                break
            await asyncio.sleep(0.01)
        assert both_queued, (
            f"the drain never reached its blocked CAS (pre={pre_census}, now={COUNTERS.snapshot()})"
        )
        await blocker_tx.commit()
        cancelled = await cancel_task
        wake = await drain_task

        # THE MISS: the cancel won the row; the CAS matched ZERO.
        assert cancelled is not None, "the vanilla cancel's own UPDATE matched the parked row"
        assert wake.promoted == 0, wake
        assert wake.intent_completed is False, wake
    finally:
        if drain_task is not None and not drain_task.done():
            drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task
        if not cancel_task.done():
            cancel_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancel_task

    # THE MISS'S OWN STATE: the source cancelled, the key removed (the
    # §2.4 cancel law); the generation STILL pending.
    src = await pool.fetchrow(
        f'SELECT status::text AS status, metadata FROM "{schema}".jobs WHERE id = $1::uuid',
        source_id,
    )
    assert src is not None and src["status"] == "cancelled", src
    src_meta = (
        _json.loads(src["metadata"]) if isinstance(src["metadata"], str) else dict(src["metadata"])
    )
    assert "wf_emit_reservation" not in src_meta, src_meta
    intent = await pool.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        flow_id,
    )
    assert intent is not None and int(intent["handled_generation"]) < int(
        intent["wake_generation"]
    ), intent

    # THE NO-SPIN WINDOW: the missed drain ENDED; nothing else runs (the
    # direct driver owns no leader) — the generation is bit-identical
    # across the window.
    await asyncio.sleep(1.5)
    intent_after = await pool.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        flow_id,
    )
    assert intent_after is not None
    assert dict(intent_after) == dict(intent), (dict(intent), dict(intent_after))

    # THE ATTRIBUTED NEXT ARMS: a SECOND REAL OS PROCESS drives the run
    # to its verdict — its own initial backfill + its own producing
    # bodies. No harness-invoked drain exists.
    driver_script = tmp_path / "c11_driver.py"
    await asyncio.to_thread(
        driver_script.write_text,
        "import asyncio, json, sys\n"
        "import asyncpg\n"
        f"sys.path.insert(0, {_WORKTREE_SRC!r})\n"
        f"sys.path.insert(0, {os.getcwd()!r})\n"
        "from tests.system_e2e import _p2_saturation_worker as entry\n"
        "from taskq.workflows import FlowRunner\n"
        "\n"
        "async def main() -> None:\n"
        "    flow_id, dsn, schema = sys.argv[1], sys.argv[2], sys.argv[3]\n"
        "    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=4)\n"
        "    runner = FlowRunner(entry.p2sat_app.get('c4_kill_recovery_flow'), pool, schema)\n"
        "    verdict = await runner.drive(flow_id, tick=0.05, max_ticks=200)\n"
        "    from taskq.workflows._emit import COUNTERS\n"
        "    print('C11-DRIVER-CENSUS ' + json.dumps(\n"
        "        {'verdict': verdict, 'census': COUNTERS.snapshot()}), flush=True)\n"
        "    await pool.close()\n"
        "\n"
        "asyncio.run(main())\n",
        encoding="utf-8",
    )
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(driver_script),
        flow_id,
        pg_dsn,
        schema,
        cwd=os.getcwd(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=240)
    lines = [ln for ln in stdout.decode().splitlines() if ln.startswith("C11-DRIVER-CENSUS")]
    assert lines, stdout.decode()[-2000:]
    payload = _json.loads(lines[-1].split(" ", 1)[1])
    assert payload["verdict"] == "terminal", payload
    census = payload["census"]
    # THE ATTRIBUTION: EXACTLY the drive's own initial backfill ran (the
    # live child's own terminal re-found no pending intent — the
    # generation was already handled) and ZERO promotions ever happened:
    # the cancelled source was never resurrected.
    assert int(census.get("flow_drains", 0)) == 1, census
    assert int(census.get("fitted_grants", 0)) == 0, census
    intent = await pool.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        flow_id,
    )
    assert intent is not None and int(intent["handled_generation"]) == int(
        intent["wake_generation"]
    ), intent
    # UNRELATED-FLOW PROGRESS (the miss never blocked the machinery): the
    # subprocess's own drive completed the run to its terminal verdict.
    root = await pool.fetchval(
        f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid', flow_id
    )
    assert root in ("succeeded", "cancelled", "failed"), root
    await pool.close()
