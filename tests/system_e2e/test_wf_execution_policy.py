"""Two-process execution-policy controls on the real workflow dispatch fleet.

This is a short focused system scenario, not the separate ten-minute saturation
or 200k-record acceptance artifacts.
"""

# ruff: noqa: S608 — all schema identifiers are validated fixture values; data is bound.

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import FlowRunner
from tests.system_e2e._harness import WorkerProc, reap
from tests.system_e2e._wf_app import (
    MARCH_FLOWS,
    clear_e2e_release_gate,
    e2e_release_gate_ddl,
    open_e2e_release_gate,
    seed_e2e_release_gate,
)
from tests.system_e2e._wf_harness import WF_DIAG_ENTRY, spawn_joined_worker, spawn_wf_fleet

pytestmark = [pytest.mark.system, pytest.mark.integration]


async def _run(pool: asyncpg.Pool, schema: str, name: str) -> str:
    runner = FlowRunner(MARCH_FLOWS[name], pool, schema)
    return str((await runner.create_flow()).flow_id)


async def _wait_policy_root(
    conn: asyncpg.Connection, schema: str, flow_id: str, *, bound_s: float
) -> tuple[str, float]:
    """Read the maintained root, not the shim's key-collapsed chain projection."""
    async with asyncio.timeout(bound_s):
        start = asyncio.get_running_loop().time()
        while True:
            status = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid', flow_id
            )
            if status in ("succeeded", "failed", "cancelled", "crashed", "abandoned"):
                return str(status), asyncio.get_running_loop().time() - start
            await asyncio.sleep(0.05)
        # asyncio.timeout exposes the deadline only for cancellation handling;
        # the timeout's failure text below is provided by the caller.
    raise AssertionError("unreachable root wait")


def _max_overlap(rows: list[dict[str, Any]]) -> int:
    transitions: list[tuple[datetime, int]] = []
    for row in rows:
        assert isinstance(row["started_at"], datetime)
        assert isinstance(row["finished_at"], datetime)
        transitions.append((row["started_at"], 1))
        # At an equal timestamp, finishing releases before the next entry begins.
        transitions.append((row["finished_at"], -1))
    current = 0
    maximum = 0
    for _at, delta in sorted(transitions, key=lambda item: (item[0], item[1])):
        current += delta
        maximum = max(maximum, current)
    return maximum


async def _attempt_rows(
    conn: asyncpg.Connection, schema: str, flow_id: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f"""SELECT a.job_id, a.attempt, a.started_at, a.finished_at, a.outcome,
                   a.worker_id::text AS worker_id, j.map_index, j.created_at
            FROM "{schema}".job_attempts AS a
            JOIN "{schema}".jobs AS j ON j.id = a.job_id
            WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'
            ORDER BY a.started_at, j.map_index""",
        flow_id,
    )
    return [dict(row) for row in rows]


@pytest.mark.timeout(300)
async def test_two_process_chain_policy_capacity_and_late_priority(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    schema = module_pg_schema.schema_name
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    assert pool is not None
    conn = await asyncpg.connect(pg_dsn)
    try:
        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["policy-a", "policy-b"])

        # Eight real chain records share one strict slot across two processes.
        reserved_flow = await _run(pool, schema, "policy_reserved_fleet")
        verdict, measured = await _wait_policy_root(conn, schema, reserved_flow, bound_s=90)
        print(f"[policy-reserved] terminal measured={measured:.3f}s bound=90s")
        assert verdict == "succeeded"
        attempts = await _attempt_rows(conn, schema, reserved_flow)
        if len(attempts) != 8:
            every_row = await conn.fetch(
                f"""SELECT id, parent_id, step_key, map_index, attempt, status::text AS status,
                           actor, locked_by_worker, error_class, error_message, metadata
                    FROM "{schema}".jobs
                    WHERE id = $1::uuid OR (metadata->>'flow_id')::uuid = $1::uuid
                    ORDER BY id""",
                reserved_flow,
            )
            raise AssertionError(
                f"reserved run did not have all eight attempts: attempts={attempts!r}; "
                f"all rows={[dict(row) for row in every_row]!r}"
            )
        assert all(row["outcome"] == "succeeded" for row in attempts)
        windows = await conn.fetch(
            f"""SELECT created_at AS started_at, updated_at AS finished_at
                FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1::uuid AND step_key = 'policy-reserved-window'
                ORDER BY created_at""",
            reserved_flow,
        )
        typed_windows = [dict(row) for row in windows]
        assert len(typed_windows) == 8
        observed_overlap = _max_overlap(typed_windows)
        if observed_overlap != 1:
            diagnostic = await conn.fetch(
                f"""SELECT j.id, j.map_index, j.actor, j.metadata, j.locked_by_worker,
                           j.status::text AS status, a.started_at, a.finished_at,
                           a.worker_id::text AS worker_id
                    FROM "{schema}".job_attempts AS a
                    JOIN "{schema}".jobs AS j ON j.id = a.job_id
                    WHERE j.metadata->>'flow_id' = $1 AND j.step_key = 'work'
                    ORDER BY a.started_at""",
                reserved_flow,
            )
            slots = await conn.fetch(
                f"""SELECT * FROM "{schema}".reservation_slots
                    WHERE bucket_name = 'wf-policy-reservation' ORDER BY slot_index"""
            )
            raise AssertionError(
                f"reserved overlap={observed_overlap}; attempts={[dict(r) for r in attempts]!r}; "
                f"rows={[dict(r) for r in diagnostic]!r}; slots={[dict(r) for r in slots]!r}"
            )
        workers = {row["worker_id"] for row in attempts}
        assert len(workers) == 2, workers
        held = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".reservation_slots
                WHERE bucket_name = 'wf-policy-reservation'
                  AND (job_id IS NOT NULL OR held_by_worker_id IS NOT NULL)"""
        )
        assert held == 0
        policy = await conn.fetchrow(
            f"""SELECT max_concurrent, result_ttl FROM "{schema}".actor_config
                WHERE actor = 'wf-policy-reserved'"""
        )
        assert policy is not None  # the chain cohort was projected at real boot
        assert policy["max_concurrent"] == 1  # the capped handle survived resolution/boot
        node = await conn.fetchrow(
            f"""SELECT start_to_close, priority, max_attempts FROM "{schema}".jobs
                WHERE metadata->>'flow_id' = $1 AND step_key = 'work' LIMIT 1""",
            reserved_flow,
        )
        assert node is not None
        assert node["start_to_close"] == timedelta(seconds=5)
        assert node["priority"] == 4 and node["max_attempts"] == 1

        # ── the late-priority phase, deterministic by construction ──────────
        # Active workers must stop before the priority fixture is born, and
        # the priority phase is PHASED: the chain source runs on a dedicated
        # control queue with no default-queue dispatch in flight, the high
        # row's commit is observed on this connection, and only then does the
        # first default-queue worker start — so its first eligible claim is
        # decided among fully-visible candidates.
        for pod in fleet.values():
            reap(pod)
        fleet = {}

        # The release gate for the priority body's hold: the TEST opens it
        # (after worker B has genuinely joined) — the first worker's
        # occupancy, not a body duration, brackets the handshake.
        await conn.execute(e2e_release_gate_ddl(schema))
        await seed_e2e_release_gate(conn, schema, "priority-release")

        priority_flow = await _run(pool, schema, "policy_priority_fleet")
        # The older priority-0 backlog is committed BEFORE the high row exists.
        # The exact inserted ids are captured so every count/attempt assertion
        # below is scoped to THIS test's own rows — never a stale row from the
        # module schema's other tests.
        backlog_ids = [
            row["id"]
            for row in (
                await conn.fetch(
                    f"""INSERT INTO "{schema}".jobs
                        (id, actor, queue, status, payload, max_attempts, retry_kind,
                         scheduled_at, idempotency_scope, idempotency_key, priority)
                        SELECT j.id, 'wf_policy_backlog', 'default', 'pending', '{{}}'::jsonb,
                               3, 'transient', clock_timestamp(), j.scope, j.key, 0
                        FROM unnest($1::uuid[], $2::text[], $3::text[]) AS j(id, scope, key)
                        RETURNING id""",
                    [new_uuid() for _ in range(12)],
                    [f"policy-priority-backlog:{schema}"] * 12,
                    [f"backlog-{index}" for index in range(12)],
                )
            )
        ]
        assert len(backlog_ids) == 12
        # The source's own worker, alone on the control queue: the real source
        # emits the priority-9 chain row while no default-queue worker exists.
        fleet = await spawn_wf_fleet(
            conn, pg_dsn, schema, ["policy-src"], extra_env={"TASKQ_QUEUES": "policy-control"}
        )
        # Commit visibility, observed on this independent connection: the
        # high row is readable as `pending` (never inferred from a clock).
        # Its exact id scopes the first-work/done assertions to THIS run.
        high_row_id: str | None = None
        async with asyncio.timeout(45):
            while True:
                high_row = await conn.fetchrow(
                    f"""SELECT id, status::text AS status FROM "{schema}".jobs
                        WHERE metadata->>'flow_id' = $1 AND step_key = 'work'""",
                    priority_flow,
                )
                if high_row is not None and high_row["status"] == "pending":
                    high_row_id = str(high_row["id"])
                    break
                await asyncio.sleep(0.02)
        assert high_row_id is not None
        for pod in fleet.values():
            reap(pod)
        fleet = {}

        # The FIRST default-queue worker, claiming one job per round: its
        # claims are strictly sequential (the next claim's statement runs
        # after the previous body's completion), so the worker's own earliest
        # attempt IS its first eligible claim — a durable record, never a
        # cross-worker clock inference.
        fleet = await spawn_wf_fleet(
            conn,
            pg_dsn,
            schema,
            ["policy-pa"],
            extra_env={"TASKQ_QUEUES": "default", "TASKQ_MAX_CONCURRENCY": "1"},
        )
        # The FIRST-WORK HANDSHAKE, observed on this independent connection
        # before worker B exists: the exact high row is claimed BY worker A —
        # its `locked_by_worker` is A's own registered row and the row is
        # still running inside A's gate-held workload (the attempt stays
        # genuinely occupied until this test opens the release gate, so this
        # observation has no duration to race). Worker B starts only after
        # this observation, so no legal schedule can put B's dispatch ahead
        # of A's first claim.
        pa_worker_id = await conn.fetchval(
            f'SELECT id FROM "{schema}".workers WHERE pid = $1',
            fleet["policy-pa"].proc.pid,
        )
        assert pa_worker_id is not None
        async with asyncio.timeout(60):
            while True:
                claimed = await conn.fetchval(
                    f"""SELECT count(*) FROM "{schema}".jobs
                        WHERE id = $1::uuid AND status = 'running'
                          AND locked_by_worker = $2::uuid""",
                    high_row_id,
                    pa_worker_id,
                )
                if int(claimed or 0) == 1:
                    break
                await asyncio.sleep(0.02)
        # The SECOND default worker, spawned after the first-work observation;
        # the backlog drains across the two real processes.
        fleet.update(
            await spawn_wf_fleet(
                conn,
                pg_dsn,
                schema,
                ["policy-pb"],
                extra_env={"TASKQ_QUEUES": "default", "TASKQ_MAX_CONCURRENCY": "1"},
            )
        )
        # Worker B has genuinely joined (the spawn's own join verification).
        # THE RELEASE — the test's decision, not a timer's: worker A's held
        # attempt ends here, and the backlog drains across both processes.
        await open_e2e_release_gate(conn, schema, "priority-release")
        async with asyncio.timeout(120):
            while True:
                done = await conn.fetchval(
                    f"""SELECT count(*) FROM "{schema}".jobs
                        WHERE id = ANY($1::uuid[]) AND status = 'succeeded'""",
                    backlog_ids,
                )
                high_done = await conn.fetchval(
                    f"""SELECT count(*) FROM "{schema}".jobs
                        WHERE id = $1::uuid AND status = 'succeeded'""",
                    high_row_id,
                )
                if int(done or 0) == 12 and int(high_done or 0) == 1:
                    break
                await asyncio.sleep(0.05)

        # The direct first-work assertion: the high row was policy-pa's OWN
        # first claim, decided among fully-visible candidates (the priority-0
        # backlog committed first, the priority-9 row observed pending before
        # either default worker existed). A broken ordering claims the
        # backlog first.
        first_work = await conn.fetchrow(
            f"""SELECT a.job_id, j.actor
                FROM "{schema}".job_attempts AS a
                JOIN "{schema}".jobs AS j ON j.id = a.job_id
                WHERE a.worker_id = (SELECT id FROM "{schema}".workers WHERE pid = $1)
                ORDER BY a.started_at
                LIMIT 1""",
            fleet["policy-pa"].proc.pid,
        )
        assert first_work is not None, "the first default worker recorded no attempt"
        assert str(first_work["job_id"]) == high_row_id, (
            "the first eligible default-queue claim must be THIS run's "
            f"priority-9 row; the worker's first work was {dict(first_work)!r} "
            f"(the run's high row is {high_row_id})"
        )
        backlog_workers = await conn.fetch(
            f"""SELECT DISTINCT a.worker_id::text AS worker_id
                FROM "{schema}".job_attempts AS a
                WHERE a.job_id = ANY($1::uuid[])""",
            backlog_ids,
        )
        assert len(backlog_workers) == 2, (
            f"the backlog must be processed across both real worker processes: {backlog_workers!r}"
        )

        for pod in fleet.values():
            reap(pod)
        fleet = {}
        # The release observation is read after both healthy processes stop;
        # enforcement itself was proved above, before cleanup.
        released = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".reservation_slots
                WHERE bucket_name = 'wf-policy-reservation'
                  AND (job_id IS NOT NULL OR held_by_worker_id IS NOT NULL)"""
        )
        assert released == 0
    finally:
        for pod in fleet.values():
            reap(pod)
        # Best-effort gate release + cleanup: a failed assertion must not
        # leave a worker inside the gate-held body through reap's grace,
        # and the gate row must not outlive this test in the shared module
        # schema. Never masks the test's own verdict.
        try:
            await open_e2e_release_gate(conn, schema, "priority-release")
            await clear_e2e_release_gate(conn, schema, "priority-release")
        except Exception as exc:
            print(f"[priority] gate cleanup warning: {exc!r}")
        await conn.close()
        await pool.close()


def _repend_delays(metadata: str | dict[str, Any]) -> dict[str, float]:
    """The row's applied re-pend curve, keyed by attempt ordinal."""
    import json

    if isinstance(metadata, str):
        decoded = json.loads(metadata)
        assert isinstance(decoded, dict)
        metadata = decoded
    repend = metadata.get("wf_repend")
    assert isinstance(repend, dict)
    applied: dict[str, float] = {}
    for key, value in repend.items():
        assert isinstance(value, dict)
        applied[str(key)] = float(value["backoff"])  # type: ignore[union-attr]
    return applied


def _row_metadata_of(row: dict[str, Any]) -> dict[str, Any]:
    import json

    raw = row["metadata"]
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded


async def _settled_work_row(
    conn: asyncpg.Connection, schema: str, flow_id: str, step_key: str, *, bound_s: float
) -> dict[str, Any]:
    """The named chain step's row once EVERY run row is terminal."""
    async with asyncio.timeout(bound_s):
        while True:
            rows = await conn.fetch(
                f"""SELECT id, step_key, map_index, status::text AS status, attempt,
                           max_attempts, snooze_count, priority, start_to_close,
                           error_class, error_message, metadata
                    FROM "{schema}".jobs
                    WHERE metadata->>'flow_id' = $1 AND step_key <> '__flow__'""",
                flow_id,
            )
            if rows and all(row["status"] in ("succeeded", "failed") for row in rows):
                return dict(next(row for row in rows if row["step_key"] == step_key))
            await asyncio.sleep(0.02)
    raise AssertionError("unreachable settled-row wait")


def _log_has_event(log_path: str | None, event: str, actor: str) -> bool:
    assert log_path is not None, "the capacity-owner diagnostic requires the worker's log sink"
    with open(log_path, "rb") as handle:
        log = handle.read().decode("utf-8", errors="replace")
    return event in log and actor in log


@pytest.mark.timeout(300)
async def test_two_process_chain_deadline_retry_and_capacity_owner_roll(
    pg_dsn: str, module_pg_schema: Any, tmp_path: Any
) -> None:
    """The #716 fleet surface on real worker processes and real PostgreSQL:
    the declared exponential/linear/fixed curves land their exact applied rungs;
    an advertised non-consuming delay never spends the attempt budget; the
    engine's own start_to_close (not a body raise) owns the deadline terminal;
    and the chain cohorts' capacity owners roll by the declared law across a
    real worker replacement (`roll-a` is reaped, then `roll-b` boots — the
    simultaneous two-worker case is
    ``test_two_process_chain_policy_capacity_and_late_priority``)."""
    schema = module_pg_schema.schema_name
    log_sink = str(tmp_path / "policy-roll-worker.log")
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    conn = await asyncpg.connect(pg_dsn)
    try:
        # The operator pre-seeds both cohort rows (as `taskq actor-config set`
        # would); ownership is the boot-time DECLARATION's property, never a
        # stored column: the code-owned cohort's literal deploys by code on
        # every boot, the operator-owned cohort's stored row is never
        # capacity-touched by the boot.
        for actor in ("wf-policy-code-cap", "wf-policy-operator-cap"):
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config '
                "(actor, queue, max_concurrent, metadata) "
                "VALUES ($1, 'default', 2, '{\"workflow_actor\": true}'::jsonb) "
                "ON CONFLICT (actor) DO UPDATE SET max_concurrent = 2",
                actor,
            )

        fleet = await spawn_wf_fleet(
            conn,
            pg_dsn,
            schema,
            ["roll-a"],
            log_sink=log_sink,
            extra_env={"TASKQ_LOG_LEVEL": "INFO"},
        )

        # The code-owned cohort's literal deployed over the operator's stored 2;
        # the operator-owned cohort's stored 2 was never overwritten by the
        # code's 1.
        code_row = await conn.fetchrow(
            f'SELECT max_concurrent FROM "{schema}".actor_config '
            "WHERE actor = 'wf-policy-code-cap'"
        )
        operator_row = await conn.fetchrow(
            f'SELECT max_concurrent FROM "{schema}".actor_config '
            "WHERE actor = 'wf-policy-operator-cap'"
        )
        assert code_row is not None
        code_config = dict(code_row)
        assert code_config["max_concurrent"] == 1, code_config
        assert operator_row is not None
        operator_config = dict(operator_row)
        assert operator_config["max_concurrent"] == 2, operator_config

        # The rolling-curve flow: three records, one per declared curve; each
        # fails attempts 1-3 and succeeds on attempt 4, so each row's applied
        # wf_repend carries the curve's DISTINCT third rung
        # (exponential base*4 = 0.2, linear base*3 = 0.15, fixed base = 0.05).
        flow_by_curve = {
            "exponential": ("policy_exp_fleet", "exp", 0.2),
            "linear": ("policy_lin_fleet", "lin", 0.15),
            "fixed": ("policy_fix_fleet", "fix", 0.05),
        }
        for curve, (flow_name, step_key, expected_third_rung) in flow_by_curve.items():
            flow_id = await _run(pool, schema, flow_name)
            row = await _settled_work_row(conn, schema, flow_id, step_key, bound_s=90)
            assert row["status"] == "succeeded", (curve, row)
            assert row["step_key"] == step_key and row["attempt"] == 4, (curve, row)
            assert row["max_attempts"] == 4 and row["snooze_count"] == 0, (curve, row)
            delays = _repend_delays(row["metadata"])
            assert set(delays) == {"1", "2", "3"}, (curve, delays)
            # The declared curve's own rungs land as the applied values.
            # jitter=0: the applied value IS the curve's rung.
            assert delays["3"] == expected_third_rung, (curve, delays)
            rungs = [delays[key] for key in sorted(delays, key=int)]
            assert rungs == sorted(rungs), (curve, rungs)
            failed_ledger = await conn.fetchval(
                f"""SELECT count(*) FROM "{schema}".wf_step_ledger
                    WHERE flow_id = $1::uuid AND status = 'failed'""",
                flow_id,
            )
            assert failed_ledger == 3, (curve, failed_ledger)
            print(f"[policy-roll] curve={curve} rungs={delays!r}")

        # The non-consuming advertised delay: two waits, no attempt spend, no
        # snooze_count move, then the bounded terminal.
        nc_flow = await _run(pool, schema, "policy_non_consuming_fleet")
        nc_row = await _settled_work_row(conn, schema, nc_flow, "work", bound_s=90)
        assert nc_row["status"] == "failed"
        assert nc_row["error_class"] == "RetryDeferralBudgetExceeded", nc_row
        assert nc_row["attempt"] == 1, ("the advertised waits never consumed the budget", nc_row)
        assert nc_row["max_attempts"] == 1 and nc_row["snooze_count"] == 0, nc_row
        metadata = _row_metadata_of(nc_row)
        retry_meta = metadata.get("wf_retry")
        assert isinstance(retry_meta, dict)
        assert retry_meta["non_consuming_count"] == 2, retry_meta
        assert retry_meta["non_consuming_limit"] == 2, retry_meta
        # The deferral stamp carries the ADVERTISED (non-consuming) signal;
        # no wf_repend ladder history and no snooze_count move exist — the
        # advertised wait is not a consumed retry.
        deferral = metadata.get("wf_deferral")
        assert isinstance(deferral, dict)
        assert deferral["consume_budget"] is False, deferral
        assert deferral["signal"] == "PolicyRollTransientError", deferral
        assert "wf_repend" not in metadata, metadata.get("wf_repend")
        nc_failed = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1::uuid AND status = 'failed'""",
            nc_flow,
        )
        assert nc_failed == 1, nc_failed
        print(f"[policy-roll] non-consuming count={retry_meta} deferral={deferral!r}")

        # The engine's own deadline: the body never raises; the row's terminal
        # is the enforcement's TimeoutError ('start_to_close').
        deadline_flow = await _run(pool, schema, "policy_deadline_fleet")
        deadline_row = await _settled_work_row(conn, schema, deadline_flow, "work", bound_s=90)
        assert deadline_row["status"] == "failed"
        assert deadline_row["error_class"] == "TimeoutError", deadline_row
        assert deadline_row["error_message"] == "start_to_close", deadline_row
        assert deadline_row["start_to_close"] == timedelta(seconds=0.4)
        assert deadline_row["attempt"] == 1, deadline_row
        print("[policy-roll] deadline row:", deadline_row["error_class"], "engine-enforced")

        # The code-owned cap re-converges across a real worker replacement:
        # the operator drifts the stored row, the replacement worker's boot
        # re-deploys the code's literal and logs the change diagnostic.
        await conn.execute(
            f'UPDATE "{schema}".actor_config SET max_concurrent = 5 '
            "WHERE actor = 'wf-policy-code-cap'"
        )
        for pod in fleet.values():
            reap(pod)
        fleet = {}
        fleet = await spawn_wf_fleet(
            conn,
            pg_dsn,
            schema,
            ["roll-b"],
            log_sink=log_sink,
            extra_env={"TASKQ_LOG_LEVEL": "INFO"},
        )
        code_row = await conn.fetchrow(
            f'SELECT max_concurrent FROM "{schema}".actor_config '
            "WHERE actor = 'wf-policy-code-cap'"
        )
        assert code_row is not None
        code_config = dict(code_row)
        assert code_config["max_concurrent"] == 1, (
            "the code-owned cap must re-converge at the replacement worker's boot",
            code_config,
        )
        assert _log_has_event(
            log_sink + "-roll-b", "actor-config-capacity-code-change", "wf-policy-code-cap"
        ), "the code-owned cap's change diagnostic must name the actor in the worker's log"
        # The operator-owned cohort's override event is INFO-level
        # (startup's sync); the worker entry's default root logging level
        # filters it from the log file, so its non-overwrite law is asserted
        # on the stored row above, and the diagnostic pair itself is pinned
        # in-process by test_actor_config_capacity_owner_sync.py.
        assert not _log_has_event(
            log_sink + "-roll-b", "actor-config-capacity-code-change", "wf-policy-operator-cap"
        ), "an operator-owned cohort must never log the code-change diagnostic"
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()


@pytest.mark.timeout(300)
async def test_priority_first_claim_tracks_the_law_inverted_backlog_outranks(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    """The first-claim assertion's opposite direction: with the older backlog
    seeded at a HIGHER priority than the chain row, the same phased shape's
    first eligible default-queue claim is a backlog row and the chain row
    stays unclaimed — the assertion tracks the priority law, never the
    fixture's row identity."""
    schema = module_pg_schema.schema_name
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    conn = await asyncpg.connect(pg_dsn)
    priority_flow: str | None = None
    backlog_ids: list[str] = []
    try:
        # The same release gate the primary test seeds: this control's shape
        # leaves the chain row unclaimed (the backlog outranks it), but a
        # slow run can still see the chain row claimed after the backlog
        # drains — the gate holds that body too, and the cleanup below
        # releases it so reap stays graceful.
        await conn.execute(e2e_release_gate_ddl(schema))
        await seed_e2e_release_gate(conn, schema, "priority-release")
        priority_flow = await _run(pool, schema, "policy_priority_fleet")
        # This control's own rows, captured by id: its counts and its cleanup
        # are scoped to them, so the shared module schema's other tests (and
        # this test's own leftovers) never enter its assertions.
        backlog_ids = [
            row["id"]
            for row in (
                await conn.fetch(
                    f"""INSERT INTO "{schema}".jobs
                        (id, actor, queue, status, payload, max_attempts, retry_kind,
                         scheduled_at, idempotency_scope, idempotency_key, priority)
                        SELECT j.id, 'wf_policy_backlog', 'default', 'pending', '{{}}'::jsonb,
                               3, 'transient', clock_timestamp(), j.scope, j.key, 10
                        FROM unnest($1::uuid[], $2::text[], $3::text[]) AS j(id, scope, key)
                        RETURNING id""",
                    [new_uuid() for _ in range(12)],
                    [f"policy-priority-backlog:{schema}"] * 12,
                    [f"inverted-backlog-{index}" for index in range(12)],
                )
            )
        ]
        assert len(backlog_ids) == 12
        fleet = await spawn_wf_fleet(
            conn, pg_dsn, schema, ["inv-src"], extra_env={"TASKQ_QUEUES": "policy-control"}
        )
        high_row_id: str | None = None
        async with asyncio.timeout(45):
            while True:
                high_row = await conn.fetchrow(
                    f"""SELECT id, status::text AS status FROM "{schema}".jobs
                        WHERE metadata->>'flow_id' = $1 AND step_key = 'work'""",
                    priority_flow,
                )
                if high_row is not None and high_row["status"] == "pending":
                    high_row_id = str(high_row["id"])
                    break
                await asyncio.sleep(0.02)
        assert high_row_id is not None
        for pod in fleet.values():
            reap(pod)
        fleet = {}
        fleet = await spawn_wf_fleet(
            conn,
            pg_dsn,
            schema,
            ["inv-pa"],
            extra_env={"TASKQ_QUEUES": "default", "TASKQ_MAX_CONCURRENCY": "1"},
        )
        # The first eligible claim is the priority-10 backlog row; the
        # priority-9 chain row stays unclaimed while it runs.
        async with asyncio.timeout(60):
            while True:
                backlog_claimed = await conn.fetchval(
                    f"""SELECT count(*) FROM "{schema}".jobs
                        WHERE id = ANY($1::uuid[]) AND started_at IS NOT NULL""",
                    backlog_ids,
                )
                if int(backlog_claimed or 0) > 0:
                    break
                await asyncio.sleep(0.02)
        chain_claimed = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".job_attempts AS a
                WHERE a.job_id = $1::uuid AND a.started_at IS NOT NULL""",
            high_row_id,
        )
        if int(chain_claimed or 0) != 0:
            priorities = await conn.fetch(
                f"""SELECT actor, priority, status::text AS status
                    FROM "{schema}".jobs
                    WHERE id = ANY($1::uuid[]) OR id = $2::uuid""",
                backlog_ids,
                high_row_id,
            )
            raise AssertionError(
                "with a higher-priority backlog the chain row must NOT be the "
                "first eligible claim — the first-claim assertion tracks the "
                f"priority law, not the fixture: {[dict(r) for r in priorities]!r}"
            )
        print("[priority-inverted] the backlog row was claimed first; the chain row is unclaimed")
    finally:
        for pod in fleet.values():
            reap(pod)
        # The control's rows must not outlive it: a priority-10 backlog row or
        # a stale priority-9 chain row in the shared module schema would
        # otherwise sit ahead of the primary test's own population. A cleanup
        # failure is printed loudly (the rows may remain) and never masks the
        # test's own verdict.
        try:
            # The gate first: release any gate-held body (reap stays
            # graceful) and remove the row (the shared module schema's
            # other tests seed their own).
            await open_e2e_release_gate(conn, schema, "priority-release")
            await clear_e2e_release_gate(conn, schema, "priority-release")
            flow_row_ids: list[str] = []
            if priority_flow is not None:
                flow_row_ids = [
                    row["id"]
                    for row in await conn.fetch(
                        f"""SELECT id FROM "{schema}".jobs
                            WHERE metadata->>'flow_id' = $1 OR id = $1::uuid""",
                        priority_flow,
                    )
                ]
            owned_ids = [*backlog_ids, *flow_row_ids]
            await conn.execute(
                f'DELETE FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])', owned_ids
            )
            await conn.execute(
                f'DELETE FROM "{schema}".job_attempts WHERE job_id = ANY($1::uuid[])', owned_ids
            )
            if priority_flow is not None:
                await conn.execute(
                    f'DELETE FROM "{schema}".wf_step_ledger WHERE flow_id = $1::uuid',
                    priority_flow,
                )
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = ANY($1::uuid[])', owned_ids)
        except Exception as exc:  # Why: the cleanup's own failure is a hygiene warning, never a replacement for the test's verdict.
            print(f"[priority-inverted] cleanup warning (rows may remain): {exc!r}")
        await conn.close()
        await pool.close()


@pytest.mark.timeout(300)
async def test_two_process_distinct_cohorts_enforce_independently_and_the_operator_mismatch_is_observed(
    pg_dsn: str, module_pg_schema: Any, tmp_path: Any
) -> None:
    """The issue's distinct-cohorts acceptance on two real worker processes:
    the chain's two cohorts project two actor_config rows with different
    capacity owners; cohort A's strict slot saturates (its rows serialize
    through one execution while the others are refused or denied) and that
    saturation does not throttle cohort B, whose rows claim and complete
    inside A's execution span; and the operator-owned mismatch's diagnostic
    actually fires on every real worker's boot while the matching code-owned
    cohort stays quiet."""
    schema = module_pg_schema.schema_name
    log_sink = str(tmp_path / "cohort-worker.log")
    fleet: dict[str, WorkerProc] = {}
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    conn = await asyncpg.connect(pg_dsn)
    try:
        # The operator pre-seeds both cohort rows: cohort A's stored cap
        # (2) MISmatches its declared literal (1) — the genuine mismatch —
        # and cohort B's stored cap (1) matches its declared literal.
        for actor, cap in (("wf-policy-cap-a", 2), ("wf-policy-cap-b", 1)):
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config '
                "(actor, queue, max_concurrent, metadata) "
                "VALUES ($1, 'default', $2, '{\"workflow_actor\": true}'::jsonb) "
                "ON CONFLICT (actor) DO UPDATE SET max_concurrent = $2",
                actor,
                cap,
            )

        # The release gate: cohort A's saturation body holds its attempt —
        # the worker's slot, the row's lock and the cohort's single reserved
        # slot all genuinely occupied — until THIS test opens the gate.
        # The test, not a timer, decides the window.
        await conn.execute(e2e_release_gate_ddl(schema))
        await seed_e2e_release_gate(conn, schema, "cohort-a-release")

        flow_id = await _run(pool, schema, "policy_two_cohort_fleet")

        # PHASED, like the late-priority phase. Worker A first; its boot runs
        # the capacity sync whose mismatch diagnostic the logs must carry,
        # through the diagnostic entry (the same app import + the production
        # worker_main_async's setup_logging), because the shared fleet
        # entry's pre-setup chain filters INFO by design.
        fleet["cohort-a"] = await spawn_joined_worker(
            conn,
            pg_dsn,
            schema,
            tag="cohort-a",
            extra_env={"TASKQ_QUEUES": "default", "TASKQ_MAX_CONCURRENCY": "1"},
            log_sink=f"{log_sink}-cohort-a",
            entry=WF_DIAG_ENTRY,
        )
        awork_ids = [
            str(row["id"])
            for row in await conn.fetch(
                f"""SELECT id FROM "{schema}".jobs
                    WHERE metadata->>'flow_id' = $1 AND step_key = 'awork' ORDER BY id""",
                flow_id,
            )
        ]
        assert len(awork_ids) == 2, awork_ids
        ca_worker_id = await conn.fetchval(
            f'SELECT id FROM "{schema}".workers WHERE pid = $1',
            fleet["cohort-a"].proc.pid,
        )
        assert ca_worker_id is not None
        # The CLAIM HANDSHAKE, on this independent connection: the exact
        # cohort-A row worker A claimed, via the row's durable claim state
        # (running + locked_by_worker = A's registered id — the same
        # race-free shape the late-priority phase observes; the gate holds
        # it running indefinitely, so this has no duration to race). The
        # attempts' WINDOWS below are the job_attempts facts.
        async with asyncio.timeout(90):
            while True:
                a1_row = await conn.fetchrow(
                    f"""SELECT id::text AS job_id, started_at
                        FROM "{schema}".jobs
                        WHERE id = ANY($1::uuid[]) AND status = 'running'
                          AND locked_by_worker = $2::uuid""",
                    awork_ids,
                    ca_worker_id,
                )
                if a1_row is not None:
                    break
                await asyncio.sleep(0.02)
        a1_id = str(a1_row["job_id"])
        a2_id = next(cand for cand in awork_ids if cand != a1_id)

        # Worker B second — its process does not exist until A1's claim is a
        # durable fact, so no legal schedule can put B's attempts ahead of
        # A1's claim.
        fleet["cohort-b"] = await spawn_joined_worker(
            conn,
            pg_dsn,
            schema,
            tag="cohort-b",
            extra_env={"TASKQ_QUEUES": "default", "TASKQ_MAX_CONCURRENCY": "1"},
            log_sink=f"{log_sink}-cohort-b",
            entry=WF_DIAG_ENTRY,
        )
        cb_worker_id = await conn.fetchval(
            f'SELECT id FROM "{schema}".workers WHERE pid = $1',
            fleet["cohort-b"].proc.pid,
        )
        assert cb_worker_id is not None

        # Cohort B claims and completes inside A1's open window — the free
        # cohort is not throttled by A's saturation.
        async with asyncio.timeout(120):
            while True:
                b_done = await conn.fetchval(
                    f"""SELECT count(*) FROM "{schema}".jobs
                        WHERE metadata->>'flow_id' = $1 AND step_key = 'bwork'
                          AND status = 'succeeded'""",
                    flow_id,
                )
                if int(b_done or 0) == 2:
                    break
                await asyncio.sleep(0.05)

        # THE FENCE, witnessed across A1's whole open window — after cohort
        # B's completions, i.e. after the free worker has had dispatch
        # opportunity aplenty: A1 is STILL running (the gate holds its
        # claim), and A2 is still an unclaimed pending row — never started,
        # never locked. The reservation-headroom fence refused the free
        # worker.
        a1_open = await conn.fetchrow(
            f"""SELECT status::text AS status, locked_by_worker
                FROM "{schema}".jobs WHERE id = $1::uuid""",
            a1_id,
        )
        assert a1_open is not None and a1_open["status"] == "running", (
            f"A1 must still be running while the gate holds its claim: "
            f"a1={a1_id}, got {None if a1_open is None else dict(a1_open)!r}"
        )
        a2_state = await conn.fetchrow(
            f"""SELECT status::text AS status, started_at, locked_by_worker
                FROM "{schema}".jobs WHERE id = $1::uuid""",
            a2_id,
        )
        assert a2_state is not None, a2_id
        a2_live = dict(a2_state)
        assert (
            a2_live["status"] == "pending"
            and a2_live["started_at"] is None
            and a2_live["locked_by_worker"] is None
        ), (
            "cohort A's second row must stay an unclaimed pending row while "
            "A1's window is open (the fence refuses the free worker): "
            f"a2={a2_live!r}, b_done={b_done}"
        )
        print(
            "[cohorts] A1's claim open (gate-held), B1+B2 succeeded inside "
            "A's window, A2 unclaimed — the cohorts enforce independently"
        )

        # THE RELEASE — the test's own decision, after the observations
        # above: A1 finishes, the slot and reservation free, A2 proceeds.
        await open_e2e_release_gate(conn, schema, "cohort-a-release")
        async with asyncio.timeout(120):
            while True:
                done = await conn.fetchval(
                    f"""SELECT count(*) FROM "{schema}".jobs
                        WHERE metadata->>'flow_id' = $1
                          AND step_key IN ('awork', 'bwork')
                          AND status = 'succeeded'""",
                    flow_id,
                )
                if int(done or 0) == 4:
                    break
                await asyncio.sleep(0.05)

        # The DURABLE causal order, from job_attempts on the exact rows
        # (an attempt row materializes at its attempt's END; its started_at
        # is the claim instant copied from the job row): A1 is exactly one
        # gate-held attempt; every B attempt sits strictly INSIDE A1's
        # attempt window (B claimed after A1's claim — the phased spawn —
        # and finished before A1's release-gated completion); A2's first
        # attempt starts only AFTER A1's attempt finished. A delayed-A
        # schedule (B done before A1 even claims) violates the containment;
        # a broken fence (A2 claimed during the hold) violates A2's order.
        a1_window = await conn.fetchrow(
            f"""SELECT count(*) AS n, min(a.started_at) AS started,
                       max(a.finished_at) AS finished
                FROM "{schema}".job_attempts AS a WHERE a.job_id = $1::uuid""",
            a1_id,
        )
        assert a1_window is not None
        a1w = dict(a1_window)
        assert int(a1w["n"]) == 1 and a1w["started"] is not None and a1w["finished"] is not None, (
            f"A1 must be exactly one (gate-held) attempt: {a1w}"
        )
        b_windows = await conn.fetch(
            f"""SELECT a.job_id::text AS job_id, a.started_at, a.finished_at,
                       a.worker_id::text AS worker_id
                FROM "{schema}".job_attempts AS a
                WHERE a.job_id IN (SELECT id FROM "{schema}".jobs
                                   WHERE metadata->>'flow_id' = $1 AND step_key = 'bwork')
                ORDER BY a.started_at""",
            flow_id,
        )
        assert len(b_windows) == 2, f"both B rows must carry their own attempts: {b_windows!r}"
        for window in b_windows:
            assert (
                window["started_at"] > a1w["started"] and window["finished_at"] < a1w["finished"]
            ), (
                "every B attempt must sit strictly inside A1's attempt window "
                "(B progressed inside A's held slot, never before or after it): "
                f"a1_window=[{a1w['started']}, {a1w['finished']}], b={dict(window)!r}"
            )
            assert window["worker_id"] == str(cb_worker_id), (
                "B's work must belong to the second real process inside A's "
                f"window: {dict(window)!r}"
            )
        a2_first = await conn.fetchrow(
            f"""SELECT min(a.started_at) AS started FROM "{schema}".job_attempts AS a
                WHERE a.job_id = $1::uuid""",
            a2_id,
        )
        assert a2_first is not None and a2_first["started"] is not None, (
            "A2 must have completed (its release-gated claim happened)"
        )
        assert a2_first["started"] > a1w["finished"], (
            "A2's first attempt must start only after A1's attempt finished — "
            f"the fence held across A1's whole window: a2_first={dict(a2_first)!r}, "
            f"a1_finished={a1w['finished']}"
        )
        print(
            "[cohorts] durable attempt order: B1,B2 strictly inside A1's "
            "attempt window; A2's first attempt only after A1 finished"
        )

        # Both cohorts' actor_config rows, as the declarations projected:
        # the operator-owned cohort's stored cap was NOT overwritten (the
        # mismatch's durable half), the code-owned cohort's row carries the
        # code's literal.
        a_config = await conn.fetchrow(
            f"""SELECT max_concurrent FROM "{schema}".actor_config
                WHERE actor = 'wf-policy-cap-a'"""
        )
        b_config = await conn.fetchrow(
            f"""SELECT max_concurrent FROM "{schema}".actor_config
                WHERE actor = 'wf-policy-cap-b'"""
        )
        assert a_config is not None
        a_values = dict(a_config)
        assert a_values["max_concurrent"] == 2, a_values
        assert b_config is not None
        b_values = dict(b_config)
        assert b_values["max_concurrent"] == 1, b_values

        # The operator-owned mismatch diagnostic actually fires, on both real
        # workers' boots; the matching code-owned cohort fires nothing.
        for name in fleet:
            assert _log_has_event(
                log_sink + f"-{name}",
                "actor-config-capacity-override",
                "wf-policy-cap-a",
            ), f"worker {name}'s boot must log the operator-owned mismatch for cohort A"
            assert not _log_has_event(
                log_sink + f"-{name}",
                "actor-config-capacity-code-change",
                "wf-policy-cap-b",
            ), f"worker {name}'s boot must stay quiet on the matching code-owned cohort B"
        print(
            "[cohorts] the operator mismatch diagnostic fired on both real boots; the matching cohort stayed quiet"
        )
    finally:
        try:
            # Release any gate-held body (a failed assertion must not leave a
            # worker inside it through reap's grace) and remove the row (the
            # shared module schema's other tests seed their own). Never
            # masks the test's own verdict.
            await open_e2e_release_gate(conn, schema, "cohort-a-release")
            await clear_e2e_release_gate(conn, schema, "cohort-a-release")
        except Exception as exc:
            print(f"[cohorts] gate cleanup warning: {exc!r}")
        for pod in fleet.values():
            reap(pod)
        await conn.close()
        await pool.close()
