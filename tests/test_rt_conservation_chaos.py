"""Conservation chaos: composed failure paths never drop a job or lose state.

The individually-pinned failure families (claim-loss reconcile, the
prune-to-archive drain, phase-2 forced cancel, leader handover, a dead
progress broker, a SIGKILL on the terminal write, the disowned re-pend) are
composed two at a time here, against real Postgres and real Dragonfly, and
every pin carries a CONSERVATION assertion: a counter that must balance.

jobs-in == terminal-out + archived + in-flight-under-a-live-lease +
awaiting-reclaim, never a row in unreachable limbo (no lease, no pending
state, no sweep owns it); and whatever the lossy fanout drops, the durable
PG surface must observe at least as much (the poll backfill), with the
explicit-drop counter firing for what the fanout lost.

The scenarios, composed:

1. the crash-reclaim sweep DURING the archive prune: the reclaim's snapshot
   and the archiver's move run as concurrent transactions on one table; a
   re-claim of a row the archiver just moved would resurrect it (double
   existence) or strand its ledger;
2. slot death (a phase-2 forced cancel kills the job, not the slot's
   consumer loop) DURING a cap-refusal storm on the SAME actor: the death
   frees a refusaled cap slot; the storm's next attempt must take it, and
   the consumer loop must keep serving;
3. leader handover DURING a cancel request: the requesting worker is
   SIGKILLed with the cancel armed; the new leader's Sweep 1 must honour
   the request (terminal 'cancelled', audit kept) and the request must
   never age out unowned;
4. a paused (all-commands-blocked) Dragonfly DURING the progress fanout:
   every publish attempt times out and drops; the durable PG progress_seq
   must still carry every consumed seq, and the fanout must resume after
   the pause;
5. a worker SIGKILLed between the terminal DB commit and the result /
   progress fanout: the committed row must be terminal-and-whole (result
   present, attempt ledger row present, event trail present), never half;
6. the disowned re-pend under pg_terminate_backend chaos: a re-pended job
   may run again under a NEW attempt, but never twice against one attempt
   (the claim-intent window) and never without a claim row behind it.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend._sweeps import sweep_expired_locks
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client import JobsClient
from taskq.constants import progress_channel
from taskq.context import JobContext
from taskq.exceptions import MaxPendingExceededError
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import (
    ModulePgSchema,
    _open_pg_backend_on_schema,  # pyright: ignore[reportPrivateUsage]  # Why: the driver's own backend on the module schema, the surface tests/test_postgres_max_pending.py reaches through clean_jobs_app.
    redis_url_for,
)
from taskq.testing.health import unique_health_sock_path
from taskq.testing.otel import counter_value, setup_meter
from taskq.worker._leader_shared import prune_terminal_jobs
from taskq.worker.run import _main

pytestmark = [pytest.mark.integration]

_QUEUE = "consv_q"
_TAG = "consv"
_PROGRESS_CALLS = 6

#: The body-run ledger actors connect to PG with this DSN (set by the s6
#: test before its worker starts; the actors close over the module state
#: because an actor body's surface is (payload, ctx) by contract).
_RUN_STATE: dict[str, str] = {}


class ConsvPayload(BaseModel):
    calls: int = _PROGRESS_CALLS


@actor(name="consv_slow", queue=_QUEUE, max_pending=2)
async def consv_slow(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> None:
    _ = payload
    await asyncio.sleep(8.0)
    _ = ctx


@actor(name="consv_fast", queue=_QUEUE)
async def consv_fast(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> None:
    _ = payload, ctx


@actor(name="consv_progress", queue=_QUEUE)
async def consv_progress(
    payload: ConsvPayload, ctx: JobContext[ConsvPayload]
) -> dict[str, str | int]:
    for step in range(payload.calls):
        await ctx.progress(step=step)
        await asyncio.sleep(0.2)
    return {"marker": "landed", "calls": payload.calls}


async def _record_body_run(ctx: JobContext[ConsvPayload]) -> None:
    """Write the body-run ledger row: one per (job_id, attempt) run."""
    run_dsn = _RUN_STATE.get("dsn")
    run_schema = _RUN_STATE.get("schema")
    assert run_dsn is not None and run_schema is not None
    conn = await asyncpg.connect(run_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{run_schema}".consv_body_runs (job_id, attempt, run_token) '
            "VALUES ($1, $2, $3)",
            ctx.job_id,
            ctx.attempt,
            new_uuid(),
        )
    finally:
        await conn.close()


@actor(name="consv_ledger_fast", queue=_QUEUE)
async def consv_ledger_fast(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> None:
    await _record_body_run(ctx)
    _ = payload


@actor(name="consv_ledger_slow", queue=_QUEUE)
async def consv_ledger_slow(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> None:
    await _record_body_run(ctx)
    await asyncio.sleep(10.0)


_CONSV_REGISTRY: dict[str, object] = {
    "consv_slow": consv_slow,
    "consv_fast": consv_fast,
    "consv_progress": consv_progress,
}

_LEDGER_REGISTRY: dict[str, object] = {
    "consv_ledger_fast": consv_ledger_fast,
    "consv_ledger_slow": consv_ledger_slow,
}


def _scoped_dsn(pg_dsn: str, schema: str) -> str:
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(pg_dsn)
    query = (
        f"application_name={schema}"
        if not parsed.query
        else f"{parsed.query}&application_name={schema}"
    )
    return urlunparse(parsed._replace(query=query))


def _worker_settings(pg_dsn: str, schema: str, **extra: object) -> WorkerSettings:
    base: dict[str, object] = {
        "pg_dsn": pg_dsn,
        "schema_name": schema,
        "heartbeat_interval": "0.5",
        "lock_lease": "5",
        "sweep_interval": "1",
        "poll_interval": "0.05",
        "cancellation_grace_period": "1",
        "cleanup_grace_period": "1",
        "heartbeat_command_timeout": "0.1",
        "watchdog_loop_lag_budget": "4.0",
        "watchdog_loop_lag_warn_budget": "0.5",
        "max_concurrency": "2",
        "queues": [_QUEUE],
        "health_socket_path": unique_health_sock_path("consv"),
        "progress_coalesce_interval": "0.1",
    }
    base.update(extra)
    return WorkerSettings.load_from_dict(base)


async def _seed_running_expired(
    conn: asyncpg.Connection, schema: str, job_id: object, tag: str
) -> None:
    """A running row whose holder is gone: lease lapsed an hour ago."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
        "scheduled_at, started_at, locked_by_worker, lock_expires_at, tags) VALUES "
        "($1, 'consv_slow', $2, '{}'::jsonb, 'running', 1, 5, 'transient', "
        "clock_timestamp(), clock_timestamp(), $3::uuid, "
        "clock_timestamp() - interval '1 hour', ARRAY[$4::text])",
        job_id,
        _QUEUE,
        new_uuid(),  # a stand-in worker id; locality is irrelevant here
        tag,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".job_attempts (job_id, attempt, started_at) '
        "VALUES ($1, 1, clock_timestamp())",
        job_id,
    )


async def _seed_terminal_past_retention(
    conn: asyncpg.Connection, schema: str, job_id: object, tag: str
) -> None:
    """A succeeded row an hour past a zero retention: archiver food."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
        "scheduled_at, started_at, finished_at, result, tags) VALUES "
        "($1, 'consv_slow', $2, '{}'::jsonb, 'succeeded', 1, 5, 'transient', "
        "clock_timestamp() - interval '2 hours', clock_timestamp() - interval '2 hours', "
        "clock_timestamp() - interval '1 hour', '{}'::jsonb, ARRAY[$3::text])",
        job_id,
        _QUEUE,
        tag,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".job_attempts '
        "(job_id, attempt, started_at, finished_at, outcome) VALUES "
        "($1, 1, clock_timestamp() - interval '2 hours', "
        "clock_timestamp() - interval '1 hour', 'succeeded')",
        job_id,
    )


async def _move_to_archive(conn: asyncpg.Connection, schema: str, job_id: object) -> None:
    """Mirror the archiver's move the way _ARCHIVE_CTE_SQL does: insert the
    archive rows from the live rows, then delete the live rows."""
    await conn.execute(
        f"""
        INSERT INTO "{schema}".jobs_archive (
            id, actor, queue, payload, status, attempt, max_attempts,
            retry_kind, created_at, scheduled_at, started_at, finished_at,
            locked_by_worker, lock_expires_at, cancel_requested_at,
            cancel_phase, error_class, error_message, error_traceback,
            progress_state, progress_seq, result, result_size_bytes,
            result_expires_at, idempotency_key, trace_id, span_id,
            metadata, tags, expire_at
        )
        SELECT id, actor, queue, payload, status, attempt, max_attempts,
               retry_kind, created_at, scheduled_at, started_at, finished_at,
               locked_by_worker, lock_expires_at, cancel_requested_at,
               cancel_phase, error_class, error_message, error_traceback,
               progress_state, progress_seq, result, result_size_bytes,
               result_expires_at, idempotency_key, trace_id, span_id,
               metadata, tags,
               clock_timestamp() + interval '1 year'
        FROM "{schema}".jobs WHERE id = $1
        """,
        job_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".job_attempts_archive '
        f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1',
        job_id,
    )
    await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', job_id)


async def _conservation_violations(conn: asyncpg.Connection, schema: str, tag: str) -> list[str]:
    """The conservation counter, as a list of named violations.

    For every seeded job (tag-scoped), across BOTH the live table and the
    archive: exactly one row total (double existence is a resurrected job;
    zero is a dropped one); an archived row is terminal (the archiver moved
    a live row otherwise); a terminal row has finished_at; no row is
    running with a lapsed lease and no live holder (unreachable limbo); a
    non-terminal non-running row is claimable (scheduled_at set); and the
    attempt ledger reconciles, one attempt row per attempt counter tick,
    live or archived.
    """
    rows = await conn.fetch(
        f"""
        WITH pop AS (
            SELECT id, status::text AS status, lock_expires_at, attempt,
                   finished_at, scheduled_at, 'jobs'::text AS src
            FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]
            UNION ALL
            SELECT id, status::text AS status, lock_expires_at, attempt,
                   finished_at, scheduled_at, 'archive'::text AS src
            FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]
        )
        SELECT id,
               count(*)::int AS copies,
               (count(*) FILTER (WHERE src = 'jobs'))::int AS in_jobs,
               (count(*) FILTER (WHERE src = 'archive'))::int AS in_archive,
               max(status::text) AS status,
               max(lock_expires_at) AS lock_expires_at,
               max(attempt)::int AS attempt,
               max(finished_at) AS finished_at,
               max(scheduled_at) AS scheduled_at
        FROM pop GROUP BY id
        """,
        tag,
    )
    violations: list[str] = []
    for row in rows:
        jid = row["id"]
        if row["copies"] != 1:
            violations.append(
                f"job {jid}: {row['copies']} rows exist (jobs={row['in_jobs']}, "
                f"archive={row['in_archive']}) - dropped or resurrected"
            )
            continue
        if row["in_archive"] and row["status"] not in TERMINAL_STATUSES:
            violations.append(
                f"job {jid}: archived while status={row['status']} - the archiver moved a live row"
            )
        if row["status"] in TERMINAL_STATUSES and row["finished_at"] is None:
            violations.append(f"job {jid}: terminal with finished_at NULL - half state")
        if (
            row["status"] == "running"
            and row["lock_expires_at"] is not None
            and row["lock_expires_at"] < datetime.now(UTC)
        ):
            violations.append(f"job {jid}: running with a lapsed lease and no live holder - limbo")
        if (
            row["status"] not in TERMINAL_STATUSES
            and row["status"] != "running"
            and row["scheduled_at"] is None
        ):
            violations.append(
                f"job {jid}: status={row['status']} with scheduled_at NULL - "
                "awaiting-reclaim rows must be claimable"
            )
        if row["in_jobs"]:
            attempts = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".job_attempts WHERE job_id = $1',
                jid,
            )
        else:
            attempts = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".job_attempts_archive WHERE job_id = $1',
                jid,
            )
        if attempts != row["attempt"]:
            violations.append(
                f"job {jid}: attempt counter {row['attempt']} vs {attempts} "
                "attempt rows - a claim was lost or doubled"
            )
    return violations


async def _drain_reclaims(conn: asyncpg.Connection, schema: str, max_calls: int = 25) -> int:
    """Repeated Sweep 1 calls until a call reclaims nothing (drained)."""
    total = 0
    for _ in range(max_calls):
        n = await sweep_expired_locks(
            conn,
            timedelta(seconds=1),
            timedelta(seconds=1),
            schema=schema,
        )
        total += n
        if n == 0:
            break
    return total


async def _settle_terminal(
    conn: asyncpg.Connection, schema: str, tag: str, cap_secs: float
) -> dict[str, int]:
    """Wait until every tagged job is terminal (quiescence backstop)."""
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        row = await conn.fetchrow(
            f'SELECT count(*)::int AS n FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status NOT IN "
            "('succeeded', 'failed', 'crashed', 'cancelled', 'abandoned')",
            tag,
        )
        assert row is not None
        if row["n"] == 0:
            counts = await conn.fetch(
                f"SELECT status::text AS status, count(*)::int AS n "
                f'FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] GROUP BY status',
                tag,
            )
            return {r["status"]: r["n"] for r in counts}
        await asyncio.sleep(0.25)
    raise AssertionError(
        f"NOT SETTLED within {cap_secs}s: tagged jobs never reached terminal - "
        "a dropped or livelocked job"
    )


# ── Scenario 1: the reclaim sweep DURING the archive prune ──────────────


@pytest.mark.parametrize("trial", range(3))
async def test_reclaim_during_archive_prune_conserves_every_row(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """The crash-reclaim reconcile and the prune-to-archive drain race on
    one table for ~3 seconds; the composition runs three concurrent
    writers (the reclaim sweep, the archiver, and a terminal writer
    standing in for workers finishing mid-race) and then asserts the
    conservation counter for every seeded row: exactly one row per job
    across jobs and jobs_archive, terminal rows whole, no lapsed-lease
    limbo, and an attempt ledger that neither lost nor doubled a claim."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s1-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    try:
        for _ in range(6):
            await _seed_running_expired(conn, schema, new_uuid(), tag)
        for _ in range(6):
            await _seed_terminal_past_retention(conn, schema, new_uuid(), tag)

        stop = asyncio.Event()

        async def reclaim_actor() -> None:
            rconn = await asyncpg.connect(pg_dsn)
            try:
                while not stop.is_set():
                    await sweep_expired_locks(
                        rconn,
                        timedelta(seconds=1),
                        timedelta(seconds=1),
                        schema=schema,
                    )
                    await asyncio.sleep(0.01)
            finally:
                await rconn.close()

        async def prune_actor() -> None:
            pconn = await asyncpg.connect(pg_dsn)
            try:
                while not stop.is_set():
                    await prune_terminal_jobs(
                        pconn,
                        retention_per_status={
                            "succeeded": timedelta(0),
                            **{
                                s: timedelta(days=3650)
                                for s in TERMINAL_STATUSES
                                if s != "succeeded"
                            },
                        },
                        archive_retention=timedelta(days=3650),
                        batch_size=2,
                        schema=schema,
                    )
                    await asyncio.sleep(0.01)
            finally:
                await pconn.close()

        async def terminal_writer_actor() -> None:
            wconn = await asyncpg.connect(pg_dsn)
            try:
                flip = True
                while not stop.is_set():
                    if flip:
                        # Stand-in for a worker's terminal write landing
                        # mid-race: flips one expired-lease running row to
                        # succeeded, exactly the shape the reclaim's
                        # snapshot may have already locked.
                        await wconn.execute(
                            f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                            "finished_at = clock_timestamp() "
                            f'WHERE id = (SELECT id FROM "{schema}".jobs '
                            "WHERE tags @> ARRAY[$1::text] AND status = 'running' "
                            "AND lock_expires_at < clock_timestamp() "
                            "ORDER BY lock_expires_at LIMIT 1 FOR UPDATE SKIP LOCKED)",
                            tag,
                        )
                    flip = not flip
                    await asyncio.sleep(0.02)
            finally:
                await wconn.close()

        actors = [
            asyncio.create_task(reclaim_actor(), name="reclaim"),
            asyncio.create_task(prune_actor(), name="prune"),
            asyncio.create_task(terminal_writer_actor(), name="writer"),
        ]
        await asyncio.sleep(3.0)
        stop.set()
        for task in actors:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # Drain: every lapsed-lease row must end owned by a sweep outcome
        # (terminal or awaiting-reclaim), never running with no holder.
        await _drain_reclaims(conn, schema)

        violations = await _conservation_violations(conn, schema, tag)
        assert not violations, (
            f"trial {trial}: conservation violated under the reclaim/prune "
            "composition:\n" + "\n".join(violations)
        )

        # The composition actually fired both directions: rows were
        # archived AND rows were reclaimed, else the pin is vacuous.
        archived = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]',
            tag,
        )
        assert archived > 0, "the archiver never moved a row: composition did not fire"
        ledger = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_events e '
            f'JOIN "{schema}".jobs j ON j.id = e.job_id '
            "WHERE j.tags @> ARRAY[$1::text] AND e.kind = 'state_change' "
            "AND e.detail->>'reason' = 'lock_expired'",
            tag,
        )
        assert ledger > 0, "the reclaim never fired: composition did not fire"
    finally:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(
            f'DELETE FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]', tag
        )
        await conn.close()


async def test_conservation_predicate_has_teeth(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The teeth proof for the conservation counter: a reclaim that
    re-pends from a stale snapshot (the defect class the race could
    produce) produces a resurrection the predicate must name.

    The mutation stands in for the buggy sweep: it snapshots a job id, the
    archiver moves the row (copy to jobs_archive, delete from jobs), and
    then the snapshot's holder re-pends BY BARE ID, inserting the row back
    into jobs. The predicate must red with the double-existence verdict,
    and a green control population must not red (never vacuous)."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-teeth"
    conn = await asyncpg.connect(pg_dsn)
    try:
        # Green control: a properly archived row and a properly pending
        # row, both with reconciled ledgers.
        done_id = new_uuid()
        await _seed_terminal_past_retention(conn, schema, done_id, tag)
        await _move_to_archive(conn, schema, done_id)

        pend_id = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, "
            "scheduled_at, tags) VALUES "
            "($1, 'consv_slow', $2, '{}'::jsonb, 'pending', 5, 'transient', "
            "clock_timestamp(), ARRAY[$3::text])",
            pend_id,
            _QUEUE,
            tag,
        )
        control = await _conservation_violations(conn, schema, tag)
        assert control == [], (
            f"green control red: the predicate fires on a healthy population: {control}"
        )

        # The mutation: the archiver moves a row, then a stale-snapshot
        # reconcile re-pends it by bare id. Resurrection.
        moved_id = new_uuid()
        await _seed_terminal_past_retention(conn, schema, moved_id, tag)
        await _move_to_archive(conn, schema, moved_id)
        await conn.execute(
            f"""
            INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, status, attempt, max_attempts,
                retry_kind, scheduled_at, tags
            )
            SELECT id, actor, queue, payload, 'pending', attempt, max_attempts,
                   retry_kind, clock_timestamp(), tags
            FROM "{schema}".jobs_archive WHERE id = $1
            """,
            moved_id,
        )
        violations = await _conservation_violations(conn, schema, tag)
        assert violations, (
            "the mutation (stale-snapshot re-pend over an archived row) "
            "left no conservation violation: the predicate is vacuous"
        )
        assert any("rows exist" in v for v in violations), violations
    finally:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(
            f'DELETE FROM "{schema}".jobs_archive WHERE tags @> ARRAY[$1::text]', tag
        )
        await conn.close()


# ── Scenario 2: slot death DURING a cap-refusal storm ───────────────────


@pytest.mark.parametrize("trial", range(2))
async def test_forced_cancel_slot_death_during_cap_refusal_storm(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """Phase-2 forced cancels (slot death) land while the producer is
    refusing enqueues of the SAME actor at its max_pending cap. The refusal
    storm must observe each freed slot (its next attempt is admitted), the
    consumer loop must survive the phase-2 kills and keep serving (the
    #420 property), and the counter must balance: enqueue attempts ==
    admitted + explicitly-refused, every admitted job terminal, and one
    attempt ledger row per attempt tick."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s2-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    dsn = _scoped_dsn(pg_dsn, schema)
    settings = _worker_settings(dsn, schema, max_concurrency="2")

    async def _runner() -> int:
        with contextlib.suppress(asyncio.CancelledError):
            return await _main(settings, actor_registry=_CONSV_REGISTRY)
        return 0

    worker_task = asyncio.create_task(_runner(), name=f"consv-s2-{trial}")
    try:
        await asyncio.sleep(2.0)  # bootstrap: sync_actor_config persists max_pending=2

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            for _ in range(2):
                await client.enqueue(consv_slow, ConsvPayload(), tags=[tag])

            running: int | None = None
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                running = await conn.fetchval(
                    f'SELECT count(*)::int FROM "{schema}".jobs '
                    "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
                    tag,
                )
                if running == 2:
                    break
                await asyncio.sleep(0.05)
            assert running == 2, f"the slow jobs never filled both slots: {running}"

            # The composition: a canceller keeps phase-2 killing every
            # running tagged job (slot death), while the storm keeps
            # enqueueing the capped actor (cap refusals). The refusals
            # must end the moment a death frees a slot.
            stop_chaos = asyncio.Event()

            async def canceller() -> int:
                kills = 0
                kconn = await asyncpg.connect(pg_dsn)
                try:
                    while not stop_chaos.is_set():
                        result = await kconn.fetchval(
                            f'UPDATE "{schema}".jobs SET cancel_phase = 2, '
                            "cancel_requested_at = clock_timestamp() "
                            "WHERE tags @> ARRAY[$1::text] AND status = 'running' "
                            "RETURNING 1",
                            tag,
                        )
                        if result is not None:
                            kills += 1
                        await asyncio.sleep(1.0)
                finally:
                    await kconn.close()
                return kills

            cancel_task = asyncio.create_task(canceller(), name="consv-s2-canceller")

            admitted = 0
            refused = 0
            for _attempt_no in range(80):
                if admitted >= 4:
                    break
                try:
                    await client.enqueue(consv_slow, ConsvPayload(), tags=[tag])
                    admitted += 1
                except MaxPendingExceededError:
                    refused += 1
                await asyncio.sleep(0.2)

            stop_chaos.set()
            kills = await cancel_task

            assert refused > 0, (
                "the cap never refused: the composition did not fire "
                "(the cap must be full when the storm starts)"
            )
            assert admitted >= 4, (
                f"only {admitted} of 4 storm enqueues were admitted across the "
                "slot deaths: the refusal queue's next actor never took the "
                "freed slot"
            )
            assert kills >= 2, f"the canceller only killed {kills} slots"

            # The consumer loop survived the phase-2 kills (the #420
            # property): a job enqueued and completed AFTER the last slot
            # death runs to success on the same worker.
            await client.enqueue(consv_fast, ConsvPayload(), tags=[tag])

            counts = await _settle_terminal(conn, schema, tag, cap_secs=90.0)

            # Conservation: enqueue attempts == admitted + refused.
            total_jobs = sum(counts.values())
            assert total_jobs == 3 + admitted, (
                f"jobs-in {total_jobs} != admitted {3 + admitted}: an enqueue attempt vanished"
            )
            assert counts.get("abandoned", 0) >= 2, (
                f"the phase-2 deaths did not land as abandoned (the forced "
                f"cancel's terminal label): {counts}"
            )
            # The consumer loop survived the phase-2 kills (the #420
            # property): the post-cancel fast job completed.
            after = counts.get("succeeded", 0)
            assert after >= 1, (
                f"the post-cancel fast job never completed: {counts} - the "
                "slot's consumer loop died with the job"
            )
            # Ledger: one attempt row per attempt counter tick.
            bad = await conn.fetchval(
                f"""
                SELECT count(*)::int FROM "{schema}".jobs j
                WHERE tags @> ARRAY[$1::text]
                  AND (SELECT count(*)::int FROM "{schema}".job_attempts a
                       WHERE a.job_id = j.id) <> j.attempt
                """,
                tag,
            )
            assert bad == 0, f"{bad} jobs carry a ledger that does not match their counter"
        finally:
            await stack.aclose()
    finally:
        if not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await asyncio.wait_for(worker_task, timeout=60.0)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.close()


# ── Scenario 3: leader handover DURING a cancel request ─────────────────


def _spawn_consv_worker(
    pg_dsn: str,
    schema: str,
    socket_path: str,
    *,
    kill_on_terminal: bool = False,
    redis_url: str | None = None,
) -> subprocess.Popen[bytes]:
    env = {**os.environ}
    env.update(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_QUEUES": _QUEUE,
            "TASKQ_POLL_INTERVAL": "0.05",
            "TASKQ_SWEEP_INTERVAL": "1",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "3.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
            "TASKQ_TERMINATION_GRACE_PERIOD": "15.0",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_HEALTH_SOCKET_PATH": socket_path,
            "TASKQ_WATCHDOG_ENABLED": "false",
        }
    )
    if redis_url is not None:
        env["TASKQ_REDIS_URL"] = redis_url
    if kill_on_terminal:
        env["TASKQ_CONSV_KILL_ON_TERMINAL"] = "1"
    return subprocess.Popen(  # Why: fixed argv, project-owned module.
        [sys.executable, "-m", "tests._worker_harness_consv"],
        env=env,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )


def _wait_for_socket(socket_path: str, proc: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _stdout, stderr = proc.communicate(timeout=1)
            raise RuntimeError(f"worker exited rc={proc.returncode} stderr={stderr.decode()!r}")
        try:
            with socket.socket(socket.AF_UNIX) as sock:
                sock.settimeout(0.1)
                sock.connect(socket_path)
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"socket {socket_path!r} did not appear within 15s")


@pytest.mark.timeout(300)
async def test_cancel_request_survives_leader_handover(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The operator's cancel request arms on a running job; the holding
    worker is SIGKILLed before it can honour it. The NEW leader's Sweep 1
    owns the row next: its cancel arm must terminalise the row 'cancelled'
    (never re-pend it, never leave it to age out unowned) within lease +
    graces + the flat 60s cooperative headroom + one sweep interval, the
    cancel audit columns must survive as the trail of the honoured
    request, and at every observation while the row is running the request
    must still be armed (cancel_requested_at never silently cleared)."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s3"
    conn = await asyncpg.connect(pg_dsn)
    dsn = _scoped_dsn(pg_dsn, schema)
    sock = unique_health_sock_path("consv-s3")
    proc = _spawn_consv_worker(dsn, schema, sock)
    worker_task: asyncio.Task[object] | None = None
    try:
        _wait_for_socket(sock, proc)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            handle = await client.enqueue(consv_slow, ConsvPayload(), tags=[tag])
            job_id = handle.job_id

            status: str | None = None
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                status = await conn.fetchval(
                    f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
                )
                if status == "running":
                    break
                await asyncio.sleep(0.05)
            assert status == "running", f"the job never claimed: status={status}"

            # Arm the operator's request, then SIGKILL the holder before
            # the cooperative ladder can honour it (grace 1.0s: the kill
            # lands ~0.2s after the request commits).
            await conn.execute(
                f'UPDATE "{schema}".jobs SET cancel_phase = 1, '
                "cancel_requested_at = clock_timestamp() WHERE id = $1",
                job_id,
            )
            await asyncio.sleep(0.2)
            proc.kill()
            proc.wait(timeout=10)
            assert proc.returncode == -9

            # Leader handover: an in-process worker takes the fleet over.
            settings = _worker_settings(dsn, schema)

            async def _runner() -> int:
                with contextlib.suppress(asyncio.CancelledError):
                    return await _main(settings, actor_registry=_CONSV_REGISTRY)
                return 0

            worker_task = asyncio.create_task(_runner(), name="consv-s3-new-leader")

            # Bound: lease 3 + cancel grace 1 + cleanup grace 1 + the flat
            # 60s cooperative headroom + one sweep interval, with margin.
            bound = time.monotonic() + 100.0
            row: asyncpg.Record | None = None
            while time.monotonic() < bound:
                row = await conn.fetchrow(
                    f"SELECT status::text AS status, cancel_phase AS phase, "
                    "cancel_requested_at AS requested_at, finished_at AS finished_at "
                    f'FROM "{schema}".jobs WHERE id = $1',
                    job_id,
                )
                assert row is not None
                if row["status"] == "cancelled":
                    break
                # Mid-flight invariant: a running row's request never ages
                # out unowned (the columns survive until the arm honours).
                if row["status"] == "running" and row["requested_at"] is None:
                    raise AssertionError(
                        "cancel_requested_at was cleared while the row was "
                        "still running: the request aged out unowned"
                    )
                await asyncio.sleep(1.0)
            assert row is not None and row["status"] == "cancelled", (
                f"the new leader's sweep never honoured the cancel request: "
                f"status={row['status'] if row is not None else 'no row'}"
            )
            assert row["phase"] != 0 and row["requested_at"] is not None, (
                "the honoured cancel lost its audit trail (cancel columns must "
                "survive the arm that honoured them)"
            )
            assert row["finished_at"] is not None
        finally:
            await stack.aclose()

        # Ledger: the reclaim wrote the attempt row and the event trail.
        attempt = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        assert attempt >= 1
        event = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_events '
            "WHERE job_id = $1 AND kind = 'state_change' "
            "AND detail->>'to_state' = 'cancelled'",
            job_id,
        )
        assert event >= 1, "the honoured cancel wrote no state_change event"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
        if worker_task is not None and not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await asyncio.wait_for(worker_task, timeout=60.0)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.close()


# ── Scenario 4: a paused Dragonfly DURING the progress fanout ───────────


@pytest.mark.parametrize("trial", range(2))
async def test_paused_redis_during_progress_fanout_poll_backfills(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    killable_redis_container: object,
    monkeypatch: pytest.MonkeyPatch,
    trial: int,
) -> None:
    """The progress broker stops serving commands (CLIENT PAUSE ALL, the
    paused-server shape: every publish round trip exceeds the bounded
    publish timeout and drops) before the job's fanout runs, so every
    publish attempt of the job expires inside the pause window. The
    conservation: every consumed seq survives on the durable PG surface
    (the poll-state read), strictly more than any subscriber saw, the
    explicit-drop counter fires for the lost publishes, and the fanout
    resumes for the next job after the pause lifts."""
    reader = setup_meter(monkeypatch)
    from taskq.obs import _otel as otel_mod

    monkeypatch.setattr(
        otel_mod,
        "_progress_publish_failures",
        otel_mod.get_meter().create_counter("taskq.progress.publish_failures"),
    )

    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s4-{trial}"
    redis_url = redis_url_for(killable_redis_container)
    conn = await asyncpg.connect(pg_dsn)
    dsn = _scoped_dsn(pg_dsn, schema)
    settings = _worker_settings(dsn, schema, redis_url=redis_url)

    admin = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=10)
    subscriber = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=10)
    worker_task: asyncio.Task[object] | None = None
    psub = subscriber.pubsub()
    try:
        await admin.ping()

        async def _runner() -> int:
            with contextlib.suppress(asyncio.CancelledError):
                return await _main(settings, actor_registry=_CONSV_REGISTRY)
            return 0

        worker_task = asyncio.create_task(_runner(), name=f"consv-s4-{trial}")
        await asyncio.sleep(2.0)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)

            # Enqueue first (the id is needed to subscribe), subscribe on a
            # healthy broker, THEN pause: every publish of this job's
            # fanout is now attempted inside the pause window (each
            # publish's 1s bounded round trip expires inside the 15s pause
            # and drops).
            handle = await client.enqueue(consv_progress, ConsvPayload(), tags=[tag])
            job_id = handle.job_id
            await psub.subscribe(progress_channel(schema, job_id))
            await admin.execute_command("CLIENT", "PAUSE", "15000", "ALL")

            # The job's whole fanout (6 progress calls at 0.2s + the
            # terminal write) runs and drops inside the pause.
            await asyncio.sleep(9.0)

            row = await conn.fetchrow(
                f"SELECT status::text AS status, progress_seq AS seq "
                f'FROM "{schema}".jobs WHERE id = $1 AND tags @> ARRAY[$2::text]',
                job_id,
                tag,
            )
            assert row is not None, "the fanout-loss job vanished from the jobs table"

            received: list[int] = []
            msg = await psub.get_message(timeout=0.1)
            while msg is not None:
                payload = msg.get("data")
                if isinstance(payload, bytes) and b'"seq"' in payload:
                    received.append(int(json.loads(payload)["seq"]))
                msg = await psub.get_message(timeout=0.1)

            # The poll backfill: the durable surface carries every consumed
            # seq (6 progress calls + the terminal event's consumed seq),
            # strictly more than any subscriber saw.
            assert row["status"] == "succeeded", (
                f"the job did not reach terminal under the broker pause: {row}"
            )
            assert row["seq"] >= _PROGRESS_CALLS + 1, (
                f"durable progress_seq {row['seq']} lost consumed seqs "
                f"(expected >= {_PROGRESS_CALLS + 1})"
            )
            for seq in received:
                assert seq <= row["seq"], (
                    f"subscriber saw seq {seq} the durable surface never carried"
                )
            assert len(received) < _PROGRESS_CALLS + 1, (
                "the subscriber saw the whole fanout: the pause dropped nothing "
                "and the loss assertion is vacuous"
            )

            # The explicit drop: every dropped publish fired the counter.
            dropped = counter_value(reader, "taskq.progress.publish_failures")
            assert dropped >= 1, (
                "the paused broker dropped publishes without the explicit-drop counter firing"
            )
        finally:
            await stack.aclose()

        # Let the pause expire fully (the admin connection is inside the
        # paused population too), then lift it defensively.
        await asyncio.sleep(7.0)
        await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")

        # Fanout resumes: the next job's events flow again.
        stack2, _deps2, backend2 = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client2 = JobsClient(backend2)
            handle_b = await client2.enqueue(consv_progress, ConsvPayload(), tags=[tag])
            job_b = handle_b.job_id
            await psub.subscribe(progress_channel(schema, job_b))
            deadline = time.monotonic() + 30.0
            got_b = 0
            row_b: asyncpg.Record | None = None
            while time.monotonic() < deadline:
                msg = await psub.get_message(timeout=0.2)
                if msg is not None and isinstance(msg.get("data"), bytes):
                    got_b += 1
                row_b = await conn.fetchrow(
                    f"SELECT status::text AS status, progress_seq AS seq "
                    f'FROM "{schema}".jobs WHERE id = $1',
                    job_b,
                )
                if row_b is not None and row_b["status"] == "succeeded":
                    break
            assert row_b is not None and row_b["status"] == "succeeded", (
                f"the post-pause job never went terminal: {row_b}"
            )
            assert row_b["seq"] >= _PROGRESS_CALLS + 1
            assert got_b >= 1, (
                "the fanout never resumed after the pause: the loss was not "
                "bounded to the outage window"
            )
        finally:
            await stack2.aclose()
            await psub.unsubscribe()
            await psub.aclose()
    finally:
        with contextlib.suppress(Exception):
            await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")
        with contextlib.suppress(Exception):
            await admin.aclose()
        with contextlib.suppress(Exception):
            await subscriber.aclose()
        if worker_task is not None and not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await asyncio.wait_for(worker_task, timeout=60.0)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.close()


# ── Scenario 5: SIGKILL between the terminal write and the fanout ───────


@pytest.mark.parametrize("trial", range(2))
async def test_sigkill_between_terminal_write_and_fanout_leaves_no_half_state(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    killable_redis_container: object,
    trial: int,
) -> None:
    """The worker dies by SIGKILL the instant the terminal DB write
    commits (the harness patches mark_succeeded to kill after the await),
    before the terminal state-change publish and the result fanout. The
    committed row must be terminal-AND-WHOLE: succeeded with the result
    present, finished_at set, error fields clean, the attempt ledger row
    written, the event trail written, and the poll-state surface (the SQL
    the progress endpoint serves) answering with the terminal state the
    fanout never announced."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s5-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    dsn = _scoped_dsn(pg_dsn, schema)
    redis_url = redis_url_for(killable_redis_container)
    sock = unique_health_sock_path("consv-s5")
    proc = _spawn_consv_worker(dsn, schema, sock, kill_on_terminal=True, redis_url=redis_url)
    try:
        _wait_for_socket(sock, proc)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            handle = await client.enqueue(consv_progress, ConsvPayload(), tags=[tag])
            job_id = handle.job_id

            deadline = time.monotonic() + 30.0
            row: asyncpg.Record | None = None
            while time.monotonic() < deadline:
                row = await conn.fetchrow(
                    f"SELECT status::text AS status, result AS result, "
                    "finished_at AS finished_at, error_class AS error_class, "
                    f'progress_seq AS seq FROM "{schema}".jobs WHERE id = $1',
                    job_id,
                )
                if row is not None and row["status"] == "succeeded":
                    break
                if proc.poll() is not None:
                    # Died this round; one more fetch so the read cannot
                    # precede the kill's commit by microseconds.
                    row = await conn.fetchrow(
                        f"SELECT status::text AS status, result AS result, "
                        "finished_at AS finished_at, error_class AS error_class, "
                        f'progress_seq AS seq FROM "{schema}".jobs WHERE id = $1',
                        job_id,
                    )
                    break
                await asyncio.sleep(0.1)

            # The kill lands microseconds after the commit the row read
            # observed; reap it before reading the exit status.
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=10)
            assert proc.returncode == -9, (
                f"the harness exited {proc.returncode} instead of dying by "
                "SIGKILL on the terminal write: the injection never fired"
            )
            assert row is not None and row["status"] == "succeeded", (
                f"the terminal write never committed before the kill: {row}"
            )
            # Terminal-AND-WHOLE: result present, no error fields, the
            # finished stamp, the ledger row, the event trail.
            assert row["result"] is not None, (
                "succeeded with a NULL result: the terminal write was half"
            )
            result_marker = await conn.fetchval(
                f"SELECT result->>'marker' FROM \"{schema}\".jobs WHERE id = $1",
                job_id,
            )
            assert result_marker == "landed", f"result payload lost: {result_marker!r}"
            assert row["finished_at"] is not None
            assert row["error_class"] is None
            assert row["seq"] >= _PROGRESS_CALLS + 1, (
                f"progress_seq {row['seq']} lost consumed seqs across the kill"
            )
            attempt = await conn.fetchval(
                f'SELECT outcome FROM "{schema}".job_attempts WHERE job_id = $1 AND attempt = 1',
                job_id,
            )
            assert attempt == "succeeded", f"attempt ledger row missing or mislabelled: {attempt!r}"
            event = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".job_events '
                "WHERE job_id = $1 AND kind = 'state_change' "
                "AND detail->>'to_state' = 'succeeded'",
                job_id,
            )
            assert event >= 1, "the terminal event trail was lost with the process"
        finally:
            await stack.aclose()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.close()


# ── Scenario 6: the disowned re-pend never double-runs the body ──────────


@pytest.mark.parametrize("trial", range(2))
async def test_disowned_repend_never_double_runs_the_body(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """pg_terminate_backend chaos (the failover shape) against a live
    worker running a mixed workload whose bodies record a run row each
    time they execute. The reclaim re-pends disowned rows and the fleet
    re-claims them under NEW attempt numbers; the body-run ledger must
    reconcile with the attempt ledger: never two body runs against one
    attempt (the claim-intent window double-run), never a body run with no
    claim row behind it."""
    global _RUN_DSN, _RUN_SCHEMA

    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s6-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    await conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{schema}".consv_body_runs ('
        "job_id uuid NOT NULL, attempt int NOT NULL, run_token uuid NOT NULL, "
        "run_at timestamptz NOT NULL DEFAULT clock_timestamp())"
    )
    dsn = _scoped_dsn(pg_dsn, schema)
    settings = _worker_settings(dsn, schema, max_concurrency="4")
    _RUN_STATE["dsn"] = pg_dsn
    _RUN_STATE["schema"] = schema

    async def _runner() -> int:
        with contextlib.suppress(asyncio.CancelledError):
            return await _main(settings, actor_registry=_LEDGER_REGISTRY)
        return 0

    worker_task = asyncio.create_task(_runner(), name=f"consv-s6-{trial}")
    try:
        await asyncio.sleep(2.0)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            for i in range(8):
                if i % 2 == 0:
                    await client.enqueue(consv_ledger_fast, ConsvPayload(), tags=[tag])
                else:
                    await client.enqueue(consv_ledger_slow, ConsvPayload(), tags=[tag])
                await asyncio.sleep(0.3)

            # Chaos: failover-shaped connection kills on the worker's
            # sessions, mid-flight, three rounds.
            killer = await asyncpg.connect(pg_dsn)
            try:
                for _ in range(3):
                    await killer.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                        "AND application_name = $1",
                        schema,
                    )
                    await asyncio.sleep(0.7)
            finally:
                await killer.close()

            # The slow bodies sleep 30s: phase-2 cancel whatever still
            # runs so settle is bounded (the chaos may already have
            # killed some mid-flight; those the reclaim re-pends and the
            # fleet re-runs under new attempts).
            await conn.execute(
                f'UPDATE "{schema}".jobs SET cancel_phase = 2, '
                "cancel_requested_at = clock_timestamp() "
                "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
                tag,
            )
            counts = await _settle_terminal(conn, schema, tag, cap_secs=120.0)
        finally:
            await stack.aclose()

        # The body-run ledger reconciles with the attempt ledger.
        doubles = await conn.fetchval(
            f"SELECT count(*)::int FROM (SELECT job_id, attempt FROM "
            f'"{schema}".consv_body_runs GROUP BY job_id, attempt '
            "HAVING count(*) > 1) d",
        )
        assert doubles == 0, (
            f"{doubles} (job, attempt) pairs ran the body twice: the re-pend "
            "double-ran against the local claim"
        )
        orphans = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".consv_body_runs r '
            f'WHERE NOT EXISTS (SELECT 1 FROM "{schema}".job_attempts a '
            "WHERE a.job_id = r.job_id AND a.attempt = r.attempt)",
        )
        assert orphans == 0, (
            f"{orphans} body runs have no claim row behind them: a run without a claim"
        )
        _ = counts
    finally:
        if not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await asyncio.wait_for(worker_task, timeout=60.0)
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}".consv_body_runs')
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.close()
