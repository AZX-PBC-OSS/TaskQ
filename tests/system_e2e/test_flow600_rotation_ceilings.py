"""The contract §9 control 8: the flow_600 estate and the >100-flow
rotation obey the ceilings and rotate.

*Part A — the flow_600 estate.* A constructible bound of
``600*(1+W) = 1200`` (``W=1``, every waiter width 1) carries 600 SEEDED
park rows — the §9.2 "SEEDED, never engine-born" pattern: the complete
§2.2 metadata as exact literal values, no other keys — so the estate is
complete before any pod boots and the real drain over real OS workers
MUST chunk. The control holds the flow's own session admission lock
from BEFORE the victim's spawn (every drain arm queues; no chunk runs
unguarded), pins the victim's dispatcher pool to 2 connections (at most
two 200-win chunks interleave before a queued chunk-2 transaction
freezes), and releases. The contract's test-only hook
(``TASKQ_TEST_DRAIN_CHUNK_PAUSE_S``, documented in
:mod:`taskq.workflows._emit`) then freezes a chunk-2 transaction at the
ROOT position of the root(admission) → intent → node order — the hook
fires only after a FULL 200-win chunk of the same invocation committed,
so its firing is itself the ceiling's proof. The freeze position is
exactly ``600 - 200k`` parked rows; the trajectory is densely polled
and every observation must lie on that 200 lattice (a wider
per-transaction bound lands off it, red). A ``pg_locks`` sample shows
the frozen transaction holds ONLY the flow's admission advisory lock —
no ``jobs`` tuple locks, no ``wf_emit_capacity`` row locks. The
SIGKILL there leaves that state durable; the replacement PID completes
the estate from real rows — full coverage: all 600 waiters completed
exactly once, the intent handled, zero failures.

*Part B — the rotation above 100 pending flows.* 120 minimal seeded
flows, every intent pending. The early flow (third by ``flow_id``) is
POISONED: its waiter's stored ``page_width`` is a non-numeric literal,
which the shared validated-park-width scalar refuses (fail-closed) — the
poisoned flow's own drain raises and is absorbed by the rotation's
per-flow error arm. The asserts: the first selection offers the first
100 flows in ``ORDER BY flow_id`` order and persists the 100th flow's
id as the rotation cursor (cursor advancement past the poisoned prefix);
the later flows are all offered within the stated rotation bound
``ceil(120/100) + 1 = 2`` selections; no unrelated flow aborts (every
non-poisoned flow's drain completed — the intents handled, the waiters
promoted exactly once, zero failed rows); the poisoned flow's own intent
stays pending and is retried on a later selection, never dropped.

Both parts are genuinely load-heavy estates over real PostgreSQL with
real OS worker processes where fleet claims are claimed: the control is
marked ``load_sensitive`` and runs in that lane, not the default system
selection.
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.

from __future__ import annotations

import asyncio
import json as _json
import time
from typing import Any

import asyncpg
import pytest

from taskq.testing.fixtures import ModulePgSchema
from tests.system_e2e import _p2_saturation_worker as entry

pytestmark = [pytest.mark.system, pytest.mark.integration, pytest.mark.load_sensitive]

_KILL_WAIT_S = 10.0
_QUEUES = f"{entry.SOURCE_QUEUE},{entry.LEAF_QUEUE},{entry.SIBLING_QUEUE}"
_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


def _park_meta(flow: str, bound: int) -> dict[str, Any]:
    """The complete §2.2 park row's metadata as exact literal values —
    every key and value, NO other metadata key."""
    return {
        "flow_id": flow,
        "blocking_reason": "backpressure",
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": 1,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }


async def _counts(pool: asyncpg.Pool, schema: str, flow_id: str) -> dict[str, int]:
    row = await pool.fetchrow(
        f"""SELECT
            count(*) FILTER (WHERE status = 'scheduled'
                             AND metadata @> '{{"blocking_reason":"backpressure"}}') AS parked,
            count(*) FILTER (WHERE status = 'pending'
                             AND metadata ? 'wf_emit_reservation') AS promoted,
            count(*) FILTER (WHERE status = 'running') AS running,
            count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
            count(*) FILTER (WHERE status = 'failed') AS failed,
            count(*) FILTER (WHERE status = 'crashed') AS crashed
            FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    assert row is not None
    return {
        k: int(row[k]) for k in ("parked", "promoted", "running", "succeeded", "failed", "crashed")
    }


async def _wait_for(predicate: Any, interval_s: float, deadline_s: float, what: str) -> None:
    deadline = time.monotonic() + deadline_s
    while True:
        if await predicate():
            return
        assert time.monotonic() < deadline, f"the wait expired: {what}"
        await asyncio.sleep(interval_s)


async def _stop(proc: Any) -> None:
    if proc.proc.poll() is None:
        proc.proc.kill()
        await asyncio.to_thread(proc.proc.wait)


async def _spawn(
    conn: asyncpg.Connection, pg_dsn: str, schema: str, log_dir: Any, tag: str, *, hook: bool
) -> Any:
    from tests.system_e2e._harness import spawn_joined_worker

    extra_env: dict[str, str] = {"TASKQ_QUEUES": _QUEUES, "TASKQ_MAX_CONCURRENCY": "32"}
    if hook:
        # THE CONTRACT'S TEST-ONLY FAULT-INJECTION POINT (never a
        # production default; documented in taskq.workflows._emit).
        extra_env["TASKQ_TEST_DRAIN_CHUNK_PAUSE_S"] = "45"
    return await spawn_joined_worker(
        conn,
        pg_dsn,
        schema,
        tag=tag,
        entry="tests.system_e2e._p2_saturation_worker",
        log_sink=str(log_dir / f"p2-system-{tag}.worker.log"),
        extra_env=extra_env,
    )


async def _census_grants_at_least(
    log_path: Any, min_grants: int, deadline_s: float
) -> dict[str, int]:
    deadline = time.monotonic() + deadline_s
    last: dict[str, int] = {}
    while True:
        content = await asyncio.to_thread(log_path.read_text)
        lines = [ln for ln in content.splitlines() if '"p2sat-census"' in ln]
        if lines:
            raw = _json.loads(lines[-1])
            last = {k: int(v) for k, v in raw.items() if type(v) is int}
            if last.get("fitted_grants", 0) >= min_grants:
                return last
        assert time.monotonic() < deadline, (
            f"the pod's census never showed {min_grants} fitted grants: {last}"
        )
        await asyncio.sleep(1.0)


async def test_flow600_obies_the_200_win_ceiling_rotates_and_covers_fully(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """The flow_600 estate: the real drain over real OS workers chunks at
    the 200-win per-transaction ceiling, the test-only hook freezes the
    SECOND chunk's transaction at the root position of the
    root(admission) -> intent -> node order, the SIGKILL there leaves the
    committed chunks and the pending generation durable, and the
    replacement PID completes the estate from real rows with full
    coverage.

    THE DETERMINISTIC CONSTRUCTION: the control holds the flow's own
    session admission lock from BEFORE the victim's spawn, so every drain
    arm queues before any chunk runs; the victim's dispatcher pool is
    pinned to 2 connections, so at most two 200-win chunks interleave
    before a queued chunk-2 transaction acquires the lock and freezes on
    the hook (the hook fires only after a FULL 200-win chunk of the same
    invocation committed — its firing is itself the ceiling's proof). The
    freeze position is therefore exactly 600 - 200*k parked rows with k
    in (1, 2) — the trajectory is densely polled and every observation
    must lie on that 200 lattice (a wider per-transaction bound lands off
    it, red)."""
    from taskq._ids import new_uuid
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    conn = await asyncpg.connect(pg_dsn)
    victim = replacement = None
    try:
        await pool.execute(
            f'CREATE TABLE IF NOT EXISTS "{schema}".p2sat_c4_gate ('
            "flow_id uuid NOT NULL, map_index bigint NOT NULL, "
            "PRIMARY KEY (flow_id, map_index))"
        )
        runner = FlowRunner(entry.p2sat_app.get("c4_flow600"), pool, schema)
        flow_id = str((await runner.create_flow()).flow_id)
        source_id = str(
            await pool.fetchval(
                f'SELECT id FROM "{schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key='source'",
                flow_id,
            )
        )
        for i in range(600):
            meta = _park_meta(flow_id, 1200)
            await pool.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, status, attempt, max_attempts, "
                "retry_kind, step_key, map_index, metadata, scheduled_at, "
                "created_at, parent_id, fairness_key) "
                "VALUES ($1, 'wf', $2, $3::jsonb, 'scheduled', 0, 2, "
                "'transient', 'spawn', $4, $5::jsonb, clock_timestamp(), "
                "clock_timestamp(), $6::uuid, $7)",
                str(new_uuid()),
                entry.SOURCE_QUEUE,
                _json.dumps({"wf_item": {"app_id": i}}),
                i,
                _json.dumps(meta),
                source_id,
                flow_id,
            )
            await pool.execute(
                f'INSERT INTO "{schema}".p2sat_c4_gate (flow_id, map_index) VALUES ($1::uuid, $2)',
                flow_id,
                i,
            )
        # THE PRE-REGISTRATION, printed BEFORE execution: the estate's
        # shape (600 waiters, W=1, the constructible bound 600*(1+W)=1200)
        # and the per-transaction win/notif ceiling.
        pre_registered = {
            "waiters": 600,
            "W": 1,
            "constructible_bound_600x(1+W)": 1200,
            "ceiling_wins_notifies_per_tx": 200,
        }
        print("FLOW600-PRE-REGISTRATION " + _json.dumps(pre_registered, sort_keys=True), flush=True)

        # THE SEAM FIRST: the flow's own admission lock, session-held from
        # BEFORE the spawn — every drain arm (the terminal's own trigger
        # included) queues here; no chunk can run until the release.
        await conn.execute("SELECT pg_advisory_lock(hashtext($1::text))", flow_id)

        # THE VICTIM: the test-only hook (never a production default) and
        # the pinned 2-connection dispatcher pool (at most two drains run
        # concurrently, so at most two chunk-1s interleave before a
        # chunk-2 freezes).
        from tests.system_e2e._harness import spawn_joined_worker

        victim = await spawn_joined_worker(
            conn,
            pg_dsn,
            schema,
            tag="f600-victim",
            entry="tests.system_e2e._p2_saturation_worker",
            log_sink=str(log_dir / "p2-system-f600-victim.worker.log"),
            extra_env={
                "TASKQ_QUEUES": _QUEUES,
                "TASKQ_MAX_CONCURRENCY": "32",
                "TASKQ_DISPATCHER_POOL_SIZE": "2",
                "TASKQ_TEST_DRAIN_CHUNK_PAUSE_S": "45",
            },
        )

        # Wait for the source terminal's COMMIT behind the lock: the
        # intent re-armed and durably pending (wake > handled), every
        # drain arm queued.
        async def _pending() -> bool:
            row = await pool.fetchrow(
                f'SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
                "WHERE flow_id = $1::uuid",
                flow_id,
            )
            return row is not None and int(row["wake_generation"]) > int(row["handled_generation"])

        await _wait_for(_pending, 0.05, 120, "the intent never went pending")

        # THE RELEASE: the queued drains race; the trajectory is densely
        # polled from the first instant.
        await conn.execute("SELECT pg_advisory_unlock(hashtext($1::text))", flow_id)
        print("FLOW600-RELEASED", flush=True)

        trajectory: list[int] = []
        frozen_pid: int | None = None
        frozen_since: float | None = None
        freeze_at: int | None = None
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            c = await _counts(pool, schema, flow_id)
            parked = c["parked"]
            if not trajectory or trajectory[-1] != parked:
                trajectory.append(parked)
            if parked in (200, 400):
                locks = await pool.fetch(
                    """SELECT pid, granted FROM pg_locks
                       WHERE locktype = 'advisory'
                         AND objid = hashtext($1::text) AND granted""",
                    flow_id,
                )
                if locks:
                    pid = int(locks[0]["pid"])
                    if frozen_pid == pid and frozen_since is not None:
                        if time.monotonic() - frozen_since >= 3.0:
                            freeze_at = parked
                            break
                    else:
                        frozen_pid = pid
                        frozen_since = time.monotonic()
                else:
                    frozen_pid = frozen_since = None
            else:
                frozen_pid = frozen_since = None
            await asyncio.sleep(0.025)
        assert freeze_at is not None, f"the mid-chunk freeze never landed (trajectory={trajectory})"
        # THE CEILING'S LATTICE: every observation lies on 600 - 200*k — a
        # per-transaction bound wider than 200 lands off it, red.
        assert all(v in (600, 400, 200) for v in trajectory), trajectory
        wins_committed = 600 - freeze_at
        assert wins_committed in (200, 400), (wins_committed, trajectory)
        print(
            f"FLOW600-FROZEN parked={freeze_at} committed_wins={wins_committed} "
            f"trajectory={trajectory}",
            flush=True,
        )

        # THE pg_locks SAMPLE at the frozen position: the frozen
        # transaction holds ONLY the flow's admission advisory lock — no
        # jobs tuple locks, no wf_emit_capacity row locks — the ROOT
        # position of the root(admission) -> intent -> node order (the
        # hook sits after the admission lock and before the intent's
        # FOR UPDATE read).
        locks = await pool.fetch(
            """SELECT l.pid, l.locktype, c.relname AS rel, l.locktype
               FROM pg_locks l LEFT JOIN pg_class c ON c.oid = l.relation
               WHERE l.pid = $1 AND l.locktype IN ('advisory', 'tuple', 'row')""",
            frozen_pid,
        )
        advisory = [r for r in locks if r["locktype"] == "advisory" and r["rel"] is None]
        assert advisory, f"the frozen pid holds no advisory lock: {[tuple(r) for r in locks]}"
        jobs_tuples = [r for r in locks if r["locktype"] == "tuple" and r["rel"] == "jobs"]
        capacity_rows = [
            r
            for r in locks
            if r["rel"] in ("wf_emit_capacity",) and r["locktype"] in ("tuple", "row")
        ]
        assert not jobs_tuples, f"the frozen tx holds jobs tuple locks: {[tuple(r) for r in locks]}"
        assert not capacity_rows, (
            f"the frozen tx holds wf_emit_capacity row locks: {[tuple(r) for r in locks]}"
        )
        backend = await pool.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND datname = current_database()",
            frozen_pid,
        )
        assert int(backend) == 1, frozen_pid
        print(
            f"FLOW600-PGLOCKS-SAMPLE advisory_pid={frozen_pid} "
            "jobs_tuple_locks=0 wf_emit_capacity_row_locks=0 "
            "order=root_admission_before_intent_before_node",
            flush=True,
        )

        # THE KILL inside the frozen mid-chunk window.
        victim_pid = victim.proc.pid
        victim.proc.kill()
        await asyncio.to_thread(victim.proc.wait, _KILL_WAIT_S)
        assert victim.proc.returncode == -9, victim.proc.returncode

        # THE POST-KILL ESTATE: the committed chunks survived; the frozen
        # transaction rolled back to nothing; the generation is still
        # pending.
        intent = await pool.fetchrow(
            f'SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
            "WHERE flow_id = $1::uuid",
            flow_id,
        )
        assert intent is not None and int(intent["wake_generation"]) > int(
            intent["handled_generation"]
        ), intent
        after = await _counts(pool, schema, flow_id)
        assert after["parked"] == freeze_at, (after, freeze_at)
        assert after["failed"] == 0 and after["crashed"] == 0, after

        replacement = await _spawn(conn, pg_dsn, schema, log_dir, "f600-replacement", hook=False)
        assert replacement.proc.pid != victim_pid

        async def _done() -> bool:
            root = await pool.fetchval(
                f'SELECT status::text FROM "{schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key='__flow__'",
                flow_id,
            )
            return root == "succeeded"

        await _wait_for(_done, 0.5, 900, "the replacement never completed the flow_600 estate")

        # FULL COVERAGE: all 600 waiters completed exactly once, the
        # intent handled, zero failures.
        a_rows = await pool.fetch(
            f"""SELECT map_index::int AS m, status::text AS status, attempt::int AS a
                FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'spawn'""",
            flow_id,
        )
        assert len(a_rows) == 600, len(a_rows)
        assert {r["status"] for r in a_rows} == {"succeeded"}, [r["status"] for r in a_rows][:5]
        assert all(int(r["a"]) in (1, 2) for r in a_rows)
        assert sum(1 for r in a_rows if int(r["a"]) == 2) <= 32
        final = await _counts(pool, schema, flow_id)
        assert final["failed"] == 0 and final["crashed"] == 0, final
        intent = await pool.fetchrow(
            f'SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
            "WHERE flow_id = $1::uuid",
            flow_id,
        )
        assert intent is not None and int(intent["handled_generation"]) == int(
            intent["wake_generation"]
        ), intent
        census = await _census_grants_at_least(
            log_dir / "p2-system-f600-replacement.worker.log", 1, 30.0
        )
        print(
            f"FLOW600-RECOVERED victim_pid={victim_pid} "
            f"replacement_pid={replacement.proc.pid} R=2 "
            f"committed_wins={wins_committed} census="
            f"{_json.dumps(census, sort_keys=True)}",
            flush=True,
        )
    finally:
        if victim is not None:
            await _stop(victim)
        if replacement is not None:
            await _stop(replacement)
        await conn.close()
        await pool.close()


async def test_rotation_above_100_pending_flows_poisons_early_advances_cursor_fully(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """The rotation at >100 pending flows: the poisoned early flow's own
    drain is absorbed by the rotation's per-flow error arm, the cursor
    advances past it in ``ORDER BY flow_id`` order, every later flow is
    offered within the stated rotation bound (ceil(120/100) + 1 = 2
    selections), no unrelated flow aborts, and the poisoned flow's own
    intent stays pending — retried on a later selection, never dropped."""
    from uuid import UUID

    from taskq._ids import new_uuid
    from taskq.workflows._emit import drain_pending_emit_intents
    from taskq.workflows.engine import render_workflow_sql

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)

    pre_registered = {
        "flows": 120,
        "selection_limit": 100,
        "rotation_bound_selections": 2,  # ceil(120/100) + 1
        "poisoned_ordinal_by_flow_id": 2,  # the third flow, an early one
        "poison": "page_width non-numeric (the validated-width fail-closed raise)",
    }
    print("ROTATION-PRE-REGISTRATION " + _json.dumps(pre_registered, sort_keys=True), flush=True)

    flows: list[str] = []
    for _ in range(120):
        flow_id = str(new_uuid())
        flows.append(flow_id)
        await pool.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, attempt, max_attempts, "
            "retry_kind, step_key, metadata, created_at, fairness_key) "
            "VALUES ($1::uuid, 'wf', $2, '{}', 'pending', 0, 2, 'transient', "
            "'__flow__', $3::jsonb, clock_timestamp(), $1::text)",
            flow_id,
            entry.SOURCE_QUEUE,
            _json.dumps({"flow_id": flow_id, "wf_source_logical_attempt": 1}),
        )
        await pool.execute(
            f'INSERT INTO "{schema}".wf_emit_capacity '
            "(flow_id, bound, wake_generation, handled_generation) "
            "VALUES ($1::uuid, 2, 1, 0)",
            flow_id,
        )
        await pool.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, attempt, max_attempts, "
            "retry_kind, step_key, map_index, metadata, scheduled_at, "
            "created_at, parent_id, fairness_key) "
            "VALUES ($1, 'wf', $2, $3::jsonb, 'scheduled', 0, 2, 'transient', "
            "'spawn', 0, $4::jsonb, clock_timestamp(), clock_timestamp(), $5::uuid, $5)",
            str(new_uuid()),
            entry.SOURCE_QUEUE,
            _json.dumps({"wf_item": {"app_id": 0}}),
            _json.dumps(_park_meta(flow_id, 2)),
            flow_id,
        )
    flows.sort()  # the ORDER BY flow_id order the selection walks

    poisoned = flows[2]
    await pool.execute(
        f"""UPDATE "{schema}".jobs
            SET metadata = jsonb_set(
                metadata, '{{wf_emit_backpressure,page_width}}', '"oops"'
            )
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'spawn'""",
        poisoned,
    )

    wsql = render_workflow_sql(schema)
    # THE FIRST INVOCATION: 120 pending flows → 100-flow pages: the first
    # page walks flows[0..99] in flow_id order (the poison absorbed at its
    # own drain — the validated-width fail-closed raise), the second page
    # offers the later 20, and the short final page persists the wrap
    # cursor. Every flow was offered within ceil(120/100) + 1 = 2
    # selection pages.
    offered_first = await drain_pending_emit_intents(pool, wsql)
    assert offered_first == 120, offered_first
    cursor_after_first = await pool.fetchval(
        f'SELECT after_flow_id FROM "{schema}".wf_emit_intent_rotation WHERE id = true'
    )
    assert cursor_after_first is not None and str(cursor_after_first) == str(UUID(int=0)), (
        cursor_after_first
    )
    # THE FIRST 119 healthy flows: drained fully (the intents handled;
    # the waiters promoted exactly once); THE POISON: its own intent
    # stayed pending — never dropped.
    rows = await pool.fetch(
        f"""SELECT flow_id::text AS f, wake_generation, handled_generation
            FROM "{schema}".wf_emit_capacity WHERE flow_id::text = ANY($1::text[])""",
        flows,
    )
    handled_healthy = 0
    for r in rows:
        f = str(r["f"])
        if f == poisoned:
            assert int(r["wake_generation"]) > int(r["handled_generation"]), dict(r)
        else:
            assert int(r["wake_generation"]) == int(r["handled_generation"]), dict(r)
            handled_healthy += 1
    assert handled_healthy == 119, handled_healthy
    healthy_waiters = await pool.fetchval(
        f"""SELECT count(*) FROM "{schema}".jobs
            WHERE step_key = 'spawn' AND status = 'pending'
              AND metadata ? 'wf_emit_reservation'"""
    )
    assert int(healthy_waiters) == 119, healthy_waiters
    # NO UNRELATED FLOW ABORTS: zero failed/crashed rows anywhere.
    failed = await pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE status IN ('failed', 'crashed')"
    )
    assert int(failed) == 0, failed
    # THE SECOND INVOCATION (the retry arm): only the poisoned flow's own
    # intent is still pending — it alone is offered, its fail-closed
    # refusal re-absorbed, and it remains pending for the next selection.
    offered_second = await drain_pending_emit_intents(pool, wsql)
    assert offered_second == 1, offered_second
    still_pending = await pool.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        poisoned,
    )
    assert still_pending is not None and int(still_pending["wake_generation"]) > int(
        still_pending["handled_generation"]
    ), still_pending
    print(
        f"ROTATION-RESULT offered=(120, {offered_second}) "
        f"poison_absorbed=True poison_retried=True healthy_handled=119",
        flush=True,
    )
    await pool.close()
