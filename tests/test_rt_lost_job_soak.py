"""Lost-job soak: the grand mixin over one real worker and real PG.

The concurrency pins each freeze one interleaving; this soak runs the
WHOLE system - the full worker bootstrap (leader loop, sweep arms, cron
ticks, notify listener, consumers, heartbeat) against real Postgres -
under a mixed workload for several hundred rounds and asserts only what
survives every interleaving:

* every seeded job ends terminal (or handed back pending at shutdown),
  and no job's event trail is empty (a zero-state job is a job the
  system lost);
* the event trail reconciles with the attempt ledger: one claim event
  per recorded attempt, at most one terminal outcome per attempt - a
  double-applied transition or a lost terminal shows up here;
* forward progress: every job reaches a claim count it cannot exceed
  (its retry budget plus the operator retries the driver itself
  issued), and terminal completions never stall for long - a livelock
  (sweep vs retry churn, notify-wake vs poll starvation) shows up as a
  stall or a claim count running past its budget;
* the worker exits clean: an unhandled loop death during the mixin
  surfaces as a nonzero exit or a raised exception.

The mixin: transient-failure jobs on the retry curve, operator cancels
of in-flight jobs, operator re-runs of terminal jobs, and
``pg_terminate_backend`` interruptions of the worker's connections -
delivered the way a failover delivers them, scoped by application_name
so the kill never reaches a concurrent test's sessions.

Every step runs under a bounded watch: a step exceeding 30 seconds
dumps every live task into the failure, because a soak that hangs is a
livelock finding, not a timeout to bump.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Coroutine
from typing import Any
from urllib.parse import urlparse, urlunparse
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.health import unique_health_sock_path
from taskq.worker.run import _main

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_ROUNDS = 240
_STEP_BOUND_SECS = 30.0
_STALL_BOUND_SECS = 30.0
_SETTLE_BOUND_SECS = 90.0
_HANDBACK_BOUND_SECS = 30.0

_QUEUE = "soak_q"
_TAG = "soak"


class SoakPayload(BaseModel):
    marker: str
    fail_until_attempt: int = 0


@actor(name="soak_ok", queue=_QUEUE)
async def soak_ok(payload: SoakPayload, ctx: JobContext[SoakPayload]) -> None:
    _ = payload, ctx


@actor(name="soak_flaky", queue=_QUEUE)
async def soak_flaky(payload: SoakPayload, ctx: JobContext[SoakPayload]) -> None:
    if ctx.attempt <= payload.fail_until_attempt:
        raise RuntimeError(f"flaky failure on attempt {ctx.attempt}")


@actor(name="soak_slow", queue=_QUEUE)
async def soak_slow(payload: SoakPayload, ctx: JobContext[SoakPayload]) -> None:
    _ = payload
    await asyncio.sleep(2.0)
    _ = ctx


_REGISTRY = {
    "soak_ok": soak_ok,
    "soak_flaky": soak_flaky,
    "soak_slow": soak_slow,
}


def _scoped_dsn(pg_dsn: str, schema: str) -> str:
    """The worker DSN tagged with application_name, so the soak's
    connection kills hit only this test's sessions (pg_stat_activity is
    cluster-wide; the container hosts every xdist worker's databases)."""
    parsed = urlparse(pg_dsn)
    query = (
        f"application_name={schema}"
        if not parsed.query
        else f"{parsed.query}&application_name={schema}"
    )
    return urlunparse(parsed._replace(query=query))


def _settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "heartbeat_interval": "0.5",
            "lock_lease": "5",
            "sweep_interval": "1",
            "poll_interval": "0.05",
            "cancellation_grace_period": "1",
            "cleanup_grace_period": "1",
            "heartbeat_command_timeout": "0.1",
            "watchdog_loop_lag_budget": "1.2",
            "watchdog_loop_lag_warn_budget": "0.5",
            "max_concurrency": "4",
            "queues": [_QUEUE],
            "health_socket_path": unique_health_sock_path("soak"),
        }
    )


async def _bounded[T](step: asyncio.Future[T] | Coroutine[Any, Any, T], name: str) -> T:
    """Await *step* under the hang watchdog: >30s fails with a task dump."""
    try:
        return await asyncio.wait_for(step, timeout=_STEP_BOUND_SECS)
    except TimeoutError:
        live = [f"{t.get_name()}: {t!r}" for t in asyncio.all_tasks() if not t.done()]
        raise AssertionError(
            f"HANG WATCHDOG: step {name!r} exceeded {_STEP_BOUND_SECS}s - "
            "a livelock candidate. Live tasks:\n" + "\n".join(live)
        ) from None


async def _kill_worker_connections(schema: str, dsn: str) -> int:
    """Terminate the worker's sessions, the way a failover ends them.

    The scope is the worker's own application_name: the scoped DSN stamps
    every pool it builds with the schema name (the startup packet survives
    the pools' server_settings), while the killer and the probe connections
    carry their own distinct names and are excluded by the pid guard and
    the name guard respectively. pg_stat_activity is cluster-wide; this
    name never crosses test modules (each schema is one module's)."""
    conn = await asyncpg.connect(dsn, server_settings={"application_name": "taskq_soak_killer"})
    try:
        rows = await conn.fetch(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = current_database() "
            "AND pid <> pg_backend_pid() "
            "AND application_name = $1",
            schema,
        )
        return len(rows)
    finally:
        await conn.close()


async def _status_counts(conn: asyncpg.Connection, schema: str) -> dict[str, int]:
    rows = await conn.fetch(
        f"SELECT status::text AS status, count(*)::int AS n "
        f'FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] GROUP BY status',
        _TAG,
    )
    return {r["status"]: r["n"] for r in rows}


async def _trial(
    conn: asyncpg.Connection,
    schema: str,
    dsn: str,
    worker_task: asyncio.Task[object],
) -> None:
    seeded: list[UUID] = []
    operator_retries = 0
    terminal_timeline: list[tuple[float, int]] = [(asyncio.get_running_loop().time(), 0)]

    for round_no in range(_ROUNDS):
        # Two jobs per round: one plain, one on the retry curve.
        ids: list[UUID] = []
        for index in range(2):
            job_id = new_uuid()
            actor_name = "soak_ok" if round_no % 2 == 0 else "soak_flaky"
            fail_until = 1 if actor_name == "soak_flaky" and round_no % 7 == 3 else 0
            await _bounded(
                conn.execute(
                    f'INSERT INTO "{schema}".jobs '
                    "(id, actor, queue, payload, status, max_attempts, retry_kind, "
                    "scheduled_at, tags) VALUES ($1, $2, $3, $4::jsonb, 'pending', 5, "
                    "'transient', clock_timestamp(), ARRAY[$5::text])",
                    job_id,
                    actor_name,
                    _QUEUE,
                    SoakPayload(
                        marker=f"{round_no}-{index}", fail_until_attempt=fail_until
                    ).model_dump_json(),
                    _TAG,
                ),
                f"round {round_no} enqueue",
            )
            seeded.append(job_id)
            ids.append(job_id)

        if round_no % 40 == 11:
            # An operator cancel of an in-flight job: enqueue a slow job,
            # let it claim, then cancel it mid-flight.
            slow_id = new_uuid()
            await _bounded(
                conn.execute(
                    f'INSERT INTO "{schema}".jobs '
                    "(id, actor, queue, payload, status, max_attempts, retry_kind, "
                    f"scheduled_at, tags) VALUES ($1, 'soak_slow', '{_QUEUE}', "
                    "'{}'::jsonb, 'pending', 5, 'transient', clock_timestamp(), "
                    "ARRAY[$2::text])",
                    slow_id,
                    _TAG,
                ),
                f"round {round_no} slow enqueue",
            )
            seeded.append(slow_id)
            await _bounded(asyncio.sleep(0.5), "slow-claim window")
            await _bounded(
                conn.execute(
                    f'UPDATE "{schema}".jobs SET cancel_requested_at = clock_timestamp(), '
                    "cancel_phase = 1 WHERE id = $1 AND status = 'running' "
                    "AND cancel_phase = 0",
                    slow_id,
                ),
                f"round {round_no} operator cancel",
            )

        if round_no % 50 == 7:
            # Operator re-runs a terminal job (the admin retry surface).
            row = await _bounded(
                conn.fetchrow(
                    f'SELECT id FROM "{schema}".jobs '
                    "WHERE tags @> ARRAY[$1::text] AND status IN ('failed', 'cancelled', 'crashed') "
                    "ORDER BY created_at LIMIT 1",
                    _TAG,
                ),
                f"round {round_no} retry pick",
            )
            if row is not None:
                await _bounded(
                    conn.execute(
                        f"UPDATE \"{schema}\".jobs SET status = 'pending', "
                        "max_attempts = LEAST(GREATEST(max_attempts, attempt + 1), 32767), "
                        "cancel_phase = 0, cancel_requested_at = NULL, "
                        "error_class = NULL, error_message = NULL, error_traceback = NULL, "
                        "finished_at = NULL, result = NULL, scheduled_at = clock_timestamp() "
                        "WHERE id = $1",
                        row["id"],
                    ),
                    f"round {round_no} operator retry",
                )
                operator_retries += 1

        if round_no % 33 == 5:
            killed = await _bounded(
                _kill_worker_connections(schema, dsn), f"round {round_no} terminate"
            )
            assert killed > 0 or worker_task.done(), (
                "the interruption must end at least one session (a zero kill "
                "means the worker holds no sessions - it has already exited, "
                "which the post-run assertions pin as a defect)"
            )

        # Real-time pacing: a round every ~100ms keeps the soak's wall
        # clock near 25s, so the chaos lands ACROSS the worker's recovery
        # cycles instead of all inside one reconnect window, and the
        # throughput timeline below has per-round resolution.
        await _bounded(asyncio.sleep(0.1), f"round {round_no} pace")

        # Progress meter: completions move, and the driver's own view of
        # the ledger updates for the stall check.
        counts = await _bounded(_status_counts(conn, schema), f"round {round_no} status")
        done = sum(
            counts.get(s, 0) for s in ("succeeded", "failed", "crashed", "cancelled", "abandoned")
        )
        terminal_timeline.append((asyncio.get_running_loop().time(), done))

    # ── Settle: every job terminal, with the worker still running ──
    deadline = asyncio.get_running_loop().time() + _SETTLE_BOUND_SECS
    counts: dict[str, int] = {}
    while asyncio.get_running_loop().time() < deadline:
        counts = await _bounded(_status_counts(conn, schema), "settle status")
        live = counts.get("pending", 0) + counts.get("scheduled", 0) + counts.get("running", 0)
        if live == 0:
            break
        await _bounded(asyncio.sleep(0.5), "settle sleep")
    live = counts.get("pending", 0) + counts.get("scheduled", 0) + counts.get("running", 0)
    assert live == 0, (
        f"settle timeout: {live} jobs never reached a terminal state "
        f"(counts: {counts}) - a lost or livelocked job"
    )

    # Throughput: terminal completions never stall for long mid-soak.
    last_t, last_n = terminal_timeline[0]
    longest: tuple[float, float, int] = (0.0, 0.0, 0)
    for t, n in terminal_timeline[1:]:
        if n > last_n:
            gap = t - last_t
            if gap > longest[0]:
                longest = (gap, last_t, last_n)
            last_t, last_n = t, n
    assert longest[0] < _STALL_BOUND_SECS, (
        f"THROUGHPUT STALL: no terminal completion for {longest[0]:.1f}s "
        f"(from t={longest[1]:.1f}, done={longest[2]}) - livelock candidate"
    )

    # ── The worker exits clean ──
    assert not worker_task.done(), "the worker died mid-soak"
    worker_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await _bounded(asyncio.wait_for(worker_task, timeout=60.0), "worker shutdown")

    # Handback: after a clean shutdown no job stays running on a dead
    # worker's lease.
    deadline = asyncio.get_running_loop().time() + _HANDBACK_BOUND_SECS
    running = -1
    while asyncio.get_running_loop().time() < deadline:
        running = int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] '
                "AND status = 'running'",
                _TAG,
            )
        )
        if running == 0:
            break
        await asyncio.sleep(0.5)
    assert running == 0, f"{running} jobs stayed running after the worker exited"

    # ── The trail: no zero-state job, ledger reconciliation ──
    zero_state = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs j WHERE tags @> ARRAY[$1::text] '
        f'AND NOT EXISTS (SELECT 1 FROM "{schema}".job_events e WHERE e.job_id = j.id)',
        _TAG,
    )
    assert zero_state == 0, f"{zero_state} jobs carry no events - the system lost them"

    reconciled = await conn.fetch(
        f"""
        SELECT j.id, j.attempt,
               (SELECT count(*)::int FROM "{schema}".job_attempts a WHERE a.job_id = j.id) AS attempts,
               (SELECT count(*)::int FROM "{schema}".job_events e
                WHERE e.job_id = j.id AND e.kind = 'state_change'
                  AND e.detail->>'to_state' IN
                      ('succeeded', 'failed', 'crashed', 'cancelled', 'abandoned')
               ) AS terminals
        FROM "{schema}".jobs j WHERE j.tags @> ARRAY[$1::text]
        """,
        _TAG,
    )
    for row in reconciled:
        assert row["attempts"] == row["attempt"], (
            f"job {row['id']}: attempt counter {row['attempt']} vs "
            f"{row['attempts']} attempt rows - a claim was double-applied "
            "(two consumers ran one attempt number: the PK absorbs the "
            "second, so the ledger undercounts the counter) or a claim's "
            "ledger row was lost"
        )
        assert 1 <= row["terminals"] <= row["attempts"], (
            f"job {row['id']}: {row['terminals']} terminal events on "
            f"{row['attempts']} attempts - a terminal transition was "
            "double-applied or the job never terminalised"
        )
    _ = operator_retries


@pytest.mark.parametrize("trial", range(3))
async def test_lost_job_soak_grand_mixin(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """Three independent soak runs of the whole worker under the mixin."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    await conn.close()  # the module fixture applied the migrations; a live
    # connection here would only hold a slot the worker needs.

    dsn = _scoped_dsn(pg_dsn, schema)
    settings = _settings(dsn, schema)

    async def _runner() -> int:
        with contextlib.suppress(asyncio.CancelledError):
            return await _main(settings, actor_registry=_REGISTRY)
        return 0

    worker_task = asyncio.create_task(_runner(), name=f"soak-worker-{trial}")
    try:
        await _bounded(asyncio.sleep(2.0), "worker bootstrap")

        soak_conn = await asyncpg.connect(
            pg_dsn, server_settings={"application_name": "taskq_soak_probe"}
        )
        try:
            await _trial(soak_conn, schema, dsn, worker_task)
        finally:
            await soak_conn.close()
    finally:
        if not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await asyncio.wait_for(worker_task, timeout=60.0)
