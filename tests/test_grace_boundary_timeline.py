"""The grace boundary: conservation at every point of the k8s SIGTERM timeline.

The scenario: k8s sends SIGTERM; the worker stops taking new work and drains
in-flight jobs within the grace period; at ``terminationGracePeriodSeconds``
the pod is KILLED with no further notice. The invariant under attack:
CONSERVATION at every point of that timeline. No lost job, no double run, a
whole attempt ledger, and a requeue that lands within a DERIVED bound after
the hard exit (the lease lapse plus the reclaim's cadence: the bound is
derived below, then measured).

The timeline shapes, one pin each:

1. the drain's own bound: SIGTERM with jobs running that finish WITHIN the
   grace: every job is conserved - terminal ``succeeded`` (whole ledger,
   requeue count zero) or the drain's documented hand-back to the fleet
   (a row parked in local_queue at the signal goes back pending, its
   claim refunded; the graces-expired unwind goes back ``scheduled`` on
   the interrupt release) - the either-signal form, every arm toothed;
2. the grace EXPIRY: a job still running at the hard deadline (it defies
   cancellation, the stand-in for work that cannot observe cancellation):
   the process dies by its own watchdog's deadline trip, the row leaves
   RELEASING released-with-hold, and the successor's pickup is measured
   against the derived hold-expiry bound; the ledger shows the first
   attempt interrupted and the second run: exactly 2 runs, never 0 or 3;
3. the mid-terminal-write kill, both deterministic sides of the window:
   the kill AFTER the terminal write commits (committed: terminal, done
   once) and the kill BEFORE it is attempted (uncommitted: running with a
   live lease, reclaimed, re-run). BOTH conserve; never both-effect-nor-
   none;
4. the SIGTERM mid-DRAIN: the kill lands during the drain's cancel ladder
   walk with an operator cancel armed on one job: the partially walked
   ladder state meets the successor's reclaim; no stranded cancel, no lost
   cancel, the requested job terminalises ``cancelled`` exactly once;
5. the grace period = 0 shape (the kernel kill with no notice at all): the
   pure lease-lapse path: conservation over the whole population and the
   pickup bound measured against the derived lease + takeover + sweep +
   reclaim-backoff + claim sum.

Every pickup bound is asserted against a DERIVED constant (named terms plus
one named margin), never a lottery sleep. Real Postgres, subprocess workers,
the body-run ledger, deterministic timing.
"""

# ruff: noqa: S608  # Why: schema and event predicates are fixture identifiers validated by the backend; every value is $-bound, the conservation module's convention.

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.client import JobsClient
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import (
    ModulePgSchema,
    _open_pg_backend_on_schema,  # pyright: ignore[reportPrivateUsage]  # Why: the driver's own backend on the module schema, the surface every conservation pin drives enqueues through.
)
from taskq.testing.health import unique_health_sock_path
from taskq.worker.run import _main

pytestmark = pytest.mark.integration

# The queue MUST match the harness module's declaration exactly: the actor
# name is the sync key, and a released/re-pended row routes by the actor's
# CURRENT assignment (actor_config.queue, whatever the worker's sync wrote
# last). Two registries declaring different queues for one name leaves the
# re-pended row on a queue no worker of this test consumes.
_QUEUE = "consv_q"
_TAG = "grace"

# ── The derived pickup bounds ────────────────────────────────────────────
#
# Every term is a named quantity of the test configuration; the bound is
# their sum plus ONE named margin. A pin asserts pickup <= bound, so a
# recovery mechanism that stops running (a wedged sweep, a lost promote)
# fails the pin on its own deadline instead of passing by luck.

_HB = 0.5  # heartbeat_interval: the dead worker's lease/ping cadence.
_LEASE = 3.0  # lock_lease: the running row's lease, renewed every _HB; at
#   the death instant it holds at most the full lease.
_LEADER_LEASE = 2.0  # leader_lease, pinned to the four-beat floor: the
#   takeover term collapses to max(lease, 4*_HB) + one election cadence.
_SWEEP = 1.0  # sweep_interval: one tick of the leader sweep loop (reclaim)
#   and of the scheduled-wake loop (promote), both at or under this.
_BACKOFF = 1.0  # the reclaim's re-pend delay: the row's own curve, base 1s
#   at attempt 1 with jitter 0 (the grace actors' policy), floored at the
#   1s MIN_DEFERRAL_INTERVAL: exactly 1.0s.
_TAIL = 4.0  # release_exit_tail_seconds: watchdog_dump_interval (1.0) +
#   WATCHDOG_METRICS_FLUSH_TIMEOUT_SECS (2.0) + RELEASE_EXIT_TAIL_SLACK_SECS
#   (1.0). The hold a RELEASING release carries always expires at deadline
#   + tail, so the pickup after the hard exit waits out the tail, then the
#   promote tick, then the claim.
_CLAIM = 0.5  # the wake (NOTIFY or the 0.05 poll) + the claim cooldown +
#   the dispatch round: bounded well under this term.
_MARGIN = 2.0  # loop jitter, pool acquires, subprocess reap skew: one
#   margin for the whole sum, never per-term.

# The pure lease-lapse path (shapes 4 and 5, and the uncommitted side of 3):
# lease lapse, leader takeover (the dead holder's ping goes stale after
# 4 beats, plus one election cadence), the reclaim tick, the reclaim's own
# re-pend delay, the claim.
BOUND_LEASE_LAPSE = (
    _LEASE + max(_LEADER_LEASE, 4 * _HB) + _HB + _SWEEP + _BACKOFF + _CLAIM + _MARGIN
)

# The hold-expiry path (shape 2): the row leaves RELEASING 'scheduled' until
# deadline + tail; from the hard exit the pickup waits out the tail, then
# one promote tick, then the claim.
BOUND_HOLD_EXPIRY = _TAIL + _SWEEP + _CLAIM + _MARGIN


class ConsvPayload(BaseModel):
    """Mirrors the harness module's payload exactly (the worker side
    validates against ITS model; the two must accept the same fields)."""

    calls: int = 6
    hold: float = 0.0


_GRACE_RETRY = RetryPolicy(max_attempts=5, base=timedelta(seconds=1), jitter=0.0)

#: The body-run ledger's DSN/schema, set by each test for the in-process
#: successor's actors (the subprocess harness reads its own env instead).
_RUN_STATE: dict[str, str] = {}


async def _record_body_run(ctx: JobContext[ConsvPayload]) -> None:
    """One body-run ledger row per (job_id, attempt) execution."""
    conn = await asyncpg.connect(_RUN_STATE["dsn"])
    try:
        await conn.execute(
            f'INSERT INTO "{_RUN_STATE["schema"]}".consv_body_runs '
            "(job_id, attempt, run_token) VALUES ($1, $2, $3)",
            ctx.job_id,
            ctx.attempt,
            new_uuid(),
        )
    finally:
        await conn.close()


@actor(name="consv_grace_tail", queue=_QUEUE, retry=_GRACE_RETRY)
async def grace_tail(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> dict[str, str | int]:
    """Bounded stepped work (~1.5s): a job that finishes within the grace."""
    await _record_body_run(ctx)
    for step in range(6):
        await ctx.progress(step=step)
        await asyncio.sleep(0.25)
    return {"marker": "landed", "calls": payload.calls}


@actor(name="consv_grace_holder", queue=_QUEUE, retry=_GRACE_RETRY)
async def grace_holder(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> dict[str, str]:
    """Hold for ``payload.hold`` on attempt 1, briefly on any later attempt."""
    await _record_body_run(ctx)
    hold = payload.hold if ctx.attempt == 1 else 0.2
    await asyncio.sleep(hold)
    _ = ctx
    return {"marker": "landed"}


@actor(name="consv_grace_defier", queue=_QUEUE, retry=_GRACE_RETRY)
async def grace_defier(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> dict[str, str]:
    """Hold past every grace by defying cancellation."""
    await _record_body_run(ctx)
    hold = payload.hold if ctx.attempt == 1 else 0.2
    held_until = time.monotonic() + hold
    while time.monotonic() < held_until:
        try:
            await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            continue
    return {"marker": "landed"}


_GRACE_REGISTRY: dict[str, object] = {
    "consv_grace_tail": grace_tail,
    "consv_grace_holder": grace_holder,
    "consv_grace_defier": grace_defier,
}


def _spawn_grace_worker(
    pg_dsn: str,
    schema: str,
    sock_path: str,
    stderr_log: Path,
    *,
    lock_lease: str,
    cancellation_grace: str,
    cleanup_grace: str,
    termination_grace: str,
    kill_on_terminal: bool = False,
    kill_before_terminal: bool = False,
) -> subprocess.Popen[bytes]:
    """A real worker subprocess on the module schema, watchdog armed."""
    env = {**os.environ}
    env.update(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_QUEUES": _QUEUE,
            "TASKQ_POLL_INTERVAL": "0.05",
            "TASKQ_SWEEP_INTERVAL": "1.0",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": lock_lease,
            "TASKQ_LEADER_LEASE": "2.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": cancellation_grace,
            "TASKQ_CLEANUP_GRACE_PERIOD": cleanup_grace,
            "TASKQ_TERMINATION_GRACE_PERIOD": termination_grace,
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_HEALTH_SOCKET_PATH": sock_path,
            "TASKQ_WATCHDOG_ENABLED": "true",
            "TASKQ_WATCHDOG_DUMP_INTERVAL": "1.0",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "2.0",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_MAX_CONCURRENCY": "2",
        }
    )
    if kill_on_terminal:
        env["TASKQ_CONSV_KILL_ON_TERMINAL"] = "1"
    if kill_before_terminal:
        env["TASKQ_CONSV_KILL_BEFORE_TERMINAL"] = "1"
    # stderr to a file, not a pipe: the watchdog's straggler and trip dumps
    # would fill a 64K pipe and block the dying process's stderr writes
    # past the deadline the pin measures. The file is read when the boot
    # itself fails.
    with stderr_log.open("wb") as err_fh:
        # The child dups the fd; the parent-side handle closes here.
        return subprocess.Popen(  # Why: fixed argv, project-owned module.
            [sys.executable, "-m", "tests._worker_harness_consv"],
            env=env,
            stderr=err_fh,
            stdout=subprocess.DEVNULL,
        )


def _wait_for_socket(sock_path: str, proc: subprocess.Popen[bytes], stderr_log: Path) -> None:
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"worker exited rc={proc.returncode} "
                f"stderr={stderr_log.read_text(errors='replace')[-4000:]!r}"
            )
        try:
            with socket.socket(socket.AF_UNIX) as sock:
                sock.settimeout(0.1)
                sock.connect(sock_path)
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"socket {sock_path!r} did not appear within 20s")


async def _wait_exit(proc: subprocess.Popen[bytes], cap_secs: float) -> float:
    """Wait for the subprocess to exit without blocking the loop; return
    the monotonic instant the exit was observed."""
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return time.monotonic()
        await asyncio.sleep(0.02)
    proc.kill()
    raise AssertionError(f"the worker did not exit within {cap_secs}s")


def _successor_settings(
    pg_dsn: str,
    schema: str,
    sock_path: str,
    *,
    lock_lease: str,
    cancellation_grace: str,
    cleanup_grace: str,
    termination_grace: str,
) -> WorkerSettings:
    """The in-process successor's settings: the same derived numbers the
    subprocess runs with, so the bound's terms are the same on both."""
    base: dict[str, object] = {
        "pg_dsn": pg_dsn,
        "schema_name": schema,
        "heartbeat_interval": "0.5",
        "lock_lease": lock_lease,
        "leader_lease": "2.0",
        "sweep_interval": "1.0",
        "poll_interval": "0.05",
        "cancellation_grace_period": cancellation_grace,
        "cleanup_grace_period": cleanup_grace,
        "termination_grace_period": termination_grace,
        "heartbeat_command_timeout": "0.1",
        "watchdog_loop_lag_budget": "2.0",
        "watchdog_loop_lag_warn_budget": "0.5",
        "watchdog_dump_interval": "1.0",
        "max_concurrency": "2",
        "queues": [_QUEUE],
        "health_socket_path": sock_path,
        "progress_coalesce_interval": "0.1",
    }
    return WorkerSettings.load_from_dict(base)


async def _start_successor(
    pg_dsn: str, schema: str, settings: WorkerSettings
) -> asyncio.Task[object]:
    """Start the in-process successor and point the ledger writers at the
    module schema (the successor's actors run in THIS process)."""
    _RUN_STATE["dsn"] = pg_dsn
    _RUN_STATE["schema"] = schema

    async def _runner() -> object:
        with contextlib.suppress(asyncio.CancelledError):
            return await _main(settings, actor_registry=_GRACE_REGISTRY)
        return 0

    task = asyncio.create_task(_runner(), name="grace-successor")
    # Boot: the successor registers, syncs configs, binds its health socket.
    await asyncio.sleep(2.0)
    return task


async def _stop_successor(worker_task: asyncio.Task[object] | None) -> None:
    if worker_task is not None and not worker_task.done():
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, BaseException):
            await asyncio.wait_for(worker_task, timeout=60.0)


async def _create_body_runs(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{schema}".consv_body_runs ('
        "job_id uuid NOT NULL, attempt int NOT NULL, run_token uuid NOT NULL, "
        "run_at timestamptz NOT NULL DEFAULT clock_timestamp())"
    )


async def _wait_running(
    conn: asyncpg.Connection, schema: str, tag: str, want: int, cap_secs: float = 20.0
) -> None:
    deadline = time.monotonic() + cap_secs
    n = 0
    while time.monotonic() < deadline:
        n = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
            tag,
        )
        if n == want:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"only {n} of {want} tagged jobs reached running within {cap_secs}s")


async def _wait_attempt_run(
    conn: asyncpg.Connection, schema: str, job_id: object, attempt: int, cap_secs: float
) -> float:
    """Poll for the attempt's body-run row; return the elapsed seconds.

    The poll interval is 0.05s, two orders under the bound's margin: the
    measurement noise is the poll granularity, not a sleep lottery.
    """
    start = time.monotonic()
    while time.monotonic() - start < cap_secs:
        n = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".consv_body_runs '
            "WHERE job_id = $1 AND attempt = $2",
            job_id,
            attempt,
        )
        if n:
            return time.monotonic() - start
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"the attempt {attempt} body run never landed within {cap_secs}s "
        "(the pickup bound: a lost or stalled requeue)"
    )


async def _wait_terminal(
    conn: asyncpg.Connection, schema: str, tag: str, status: str, want: int, cap_secs: float
) -> None:
    deadline = time.monotonic() + cap_secs
    n = 0
    while time.monotonic() < deadline:
        n = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status = $2",
            tag,
            status,
        )
        if n >= want:
            return
        await asyncio.sleep(0.05)
    rows = await conn.fetch(
        f'SELECT id, status::text AS status FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]',
        tag,
    )
    raise AssertionError(
        f"only {n} of {want} tagged jobs reached {status!r} within {cap_secs}s: {rows}"
    )


async def _job_row(conn: asyncpg.Connection, schema: str, job_id: object) -> asyncpg.Record:
    row = await conn.fetchrow(
        f"SELECT status::text AS status, attempt AS attempt, "
        "interrupt_count AS interrupt_count, cancel_phase AS cancel_phase, "
        "cancel_requested_at AS cancel_requested_at, finished_at AS finished_at, "
        "locked_by_worker AS locked_by_worker, lock_expires_at AS lock_expires_at, "
        f"result->>'marker' AS marker FROM \"{schema}\".jobs WHERE id = $1",
        job_id,
    )
    assert row is not None, f"job {job_id} vanished from the jobs table"
    return row


async def _attempt_rows(
    conn: asyncpg.Connection, schema: str, job_id: object
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"SELECT attempt AS attempt, outcome::text AS outcome, "
        f'error_class AS error_class FROM "{schema}".job_attempts '
        "WHERE job_id = $1 ORDER BY attempt",
        job_id,
    )


async def _event_count(conn: asyncpg.Connection, schema: str, job_id: object, where: str) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_events WHERE job_id = $1 AND {where}',
            job_id,
        )
    )


async def _body_run_count(
    conn: asyncpg.Connection, schema: str, job_id: object, attempt: int | None = None
) -> int:
    if attempt is None:
        return int(
            await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".consv_body_runs WHERE job_id = $1',
                job_id,
            )
        )
    return int(
        await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".consv_body_runs '
            "WHERE job_id = $1 AND attempt = $2",
            job_id,
            attempt,
        )
    )


async def _assert_drained_job_conserved(
    conn: asyncpg.Connection, schema: str, job_id: object
) -> None:
    """The drain-to-terminal conservation, per job, in the either-signal form.

    The worker exited 0 (both promptness pins above already held), so the
    row's state is final: every write the drain owed was awaited before the
    process died. What the drain owes is NOT one fixed verdict - the
    observable contract ("a deploy never terminalises a job", conservation:
    no lost job, no double run, a whole attempt ledger) admits exactly the
    arms below, each with its own teeth:

    1. ``succeeded`` - the job finished within the graces and terminalised:
       the original single-form pin, byte-identical.
    2. ``pending`` - the DRAINING hand-back: SIGTERM landed while the row
       was claimed but still parked in local_queue (the claim-to-take
       window: the claim commits before the consumer takes, and
       ``_wait_running`` fires the signal the instant the DB says
       running). The hand-back deliberately re-pends parked rows
       (``held_ids`` covers registered consumers and claim intents, not
       queued ones), the consumer's post-take stop-check declines the
       stale copy, and the row goes back to the fleet with the claim's
       attempt refunded. The teeth pin that premise HARD: zero body runs
       (a run under a refunded attempt with no successor is a lost
       verdict, the defect class the tooth hunts), no attempt row, the
       lock cleared, no interrupt/reclaim/succeeded event anywhere.
    3. ``scheduled`` - the RELEASING interrupt-release: the unwind
       outlasted the cancellation and cleanup graces (wall-clock graces
       on a runner whose loop was starved - the #523 lesson: every
       wall-clock premise gets eaten eventually). The release hands the
       row back with the spent attempt standing, held until this process
       is provably gone. Teeth: exactly one body run, interrupt_count 1,
       the lock cleared, no succeeded verdict, no reclaim, the release's
       own ``interrupted`` audit present.

    Any other state - running, failed, cancelled, abandoned - is a lost
    or double-manufactured job and fails the pin.
    """
    row = await _job_row(conn, schema, job_id)
    status = str(row["status"])

    # Every arm: the reclaim sweep never touched a clean drain's rows.
    assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'lock_expired'") == 0, (
        f"a clean drain's job was reclaimed (status {status!r}): the lease "
        "lapsed and Sweep 1 owns the row - the drain lost it"
    )

    if status == "succeeded":
        assert row["marker"] == "landed", "the terminal result was lost"
        assert row["attempt"] == 1, f"the job requeued: attempt {row['attempt']}"
        assert row["interrupt_count"] == 0, "the job was interrupted: a requeue"
        assert row["finished_at"] is not None

        # The ledger, whole: exactly one run, one attempt row, its
        # outcome succeeded.
        assert await _body_run_count(conn, schema, job_id) == 1, (
            "the body ran more than once: a double run inside the drain"
        )
        attempts = await _attempt_rows(conn, schema, job_id)
        assert len(attempts) == 1 and attempts[0]["outcome"] == "succeeded", (
            f"the attempt ledger is not whole: {attempts}"
        )
        # Requeue count zero: no interruption event, no reclaim event.
        assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'interrupted'") == 0
        assert (
            await _event_count(
                conn,
                schema,
                job_id,
                "kind = 'state_change' AND detail->>'to_state' = 'succeeded'",
            )
            >= 1
        )
        return

    if status == "pending":
        # The hand-back's premise, in data: the claim never reached an
        # actor. A body run here is an executed attempt whose verdict
        # went nowhere - the run is lost to the ledger and a successor
        # would run the body a second time.
        assert await _body_run_count(conn, schema, job_id) == 0, (
            "the hand-back re-pended a row whose body EXECUTED: the run's "
            "verdict was discarded (fenced or never written) and the refund "
            "erases it - a lost verdict, a double run across the fleet"
        )
        assert row["attempt"] == 0, (
            f"the hand-back did not refund the claim: attempt {row['attempt']}"
        )
        attempts = await _attempt_rows(conn, schema, job_id)
        assert len(attempts) == 0, f"an unexecuted claim grew an attempt ledger: {attempts}"
        assert row["locked_by_worker"] is None, "the hand-back left the row locked"
        assert row["lock_expires_at"] is None, "the hand-back left a lease on the row"
        assert row["interrupt_count"] == 0, "the hand-back interrupted nothing: the body never ran"
        assert row["finished_at"] is None, "the hand-back manufactured a finish stamp"
        assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'interrupted'") == 0, (
            "the release ladder touched a row the hand-back owns"
        )
        assert (
            await _event_count(
                conn,
                schema,
                job_id,
                "kind = 'state_change' AND detail->>'to_state' = 'succeeded'",
            )
            == 0
        ), "the hand-back's row carries a succeeded verdict it never earned"
        return

    if status == "scheduled":
        # The interrupt-release: the body ran (exactly once), the release
        # handed the row back claimable, the spent attempt stands.
        assert await _body_run_count(conn, schema, job_id) == 1, (
            "the interrupted job's body did not run exactly once"
        )
        assert row["interrupt_count"] == 1, (
            f"the release's interrupt stamp is wrong: {row['interrupt_count']}"
        )
        assert row["locked_by_worker"] is None, "the release left the row locked"
        assert row["lock_expires_at"] is None, "the release left a lease on the row"
        assert row["finished_at"] is None, "the release manufactured a finish stamp"
        assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'interrupted'") >= 1, (
            "the release wrote no interrupted audit"
        )
        assert (
            await _event_count(
                conn,
                schema,
                job_id,
                "kind = 'state_change' AND detail->>'to_state' = 'succeeded'",
            )
            == 0
        ), "the released row also carries a succeeded verdict: a double verdict"
        return

    raise AssertionError(
        f"a drained job was left {status!r}: neither terminal nor a documented "
        "hand-back (pending: the claimed-but-unstarted DRAINING hand-back; "
        "scheduled: the graces-expired interrupt release) - a lost job"
    )


# ── Shape 1: the drain's own bound ───────────────────────────────────────


@pytest.mark.timeout(180)
@pytest.mark.parametrize("trial", range(2))
async def test_sigterm_with_jobs_finishing_within_grace_drains_to_terminal(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
    trial: int,
) -> None:
    """SIGTERM with jobs mid-flight that finish WITHIN the grace: the worker
    exits CLEANLY inside the termination deadline, and every job is conserved
    through the drain - terminal ``succeeded`` with a whole ledger, or the
    documented hand-back (``_assert_drained_job_conserved``'s arms), never a
    lost job, a double run, or a manufactured verdict."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s1-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    stderr_log = tmp_path / f"s1-{trial}-worker.err"
    sock = unique_health_sock_path(f"grace-s1-{trial}")
    proc = _spawn_grace_worker(
        pg_dsn,
        schema,
        sock,
        stderr_log,
        # The clean drain is not crash-recovery-bound (no hard exit, no
        # lease-lapse pickup); the lease only has to carry the graces the
        # settings validation requires it to cover.
        lock_lease="5.0",
        cancellation_grace="3.0",
        cleanup_grace="1.0",
        termination_grace="10.0",
    )
    try:
        await _create_body_runs(conn, schema)
        _wait_for_socket(sock, proc, stderr_log)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        job_ids: list[object] = []
        try:
            client = JobsClient(backend)
            for _ in range(2):
                handle = await client.enqueue(grace_tail, ConsvPayload(calls=6), tags=[tag])
                job_ids.append(handle.job_id)

            await _wait_running(conn, schema, tag, want=2)

            t0 = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            t_exit = await _wait_exit(proc, cap_secs=30.0)
        finally:
            await stack.aclose()

        # The drain's own bound: a clean exit inside the termination
        # deadline. The watchdog never trips (rc 0, not its exit code 2).
        assert proc.returncode == 0, (
            f"the drain exited {proc.returncode}: a clean drain must reach "
            f"exit 0 inside the grace; stderr={stderr_log.read_text(errors='replace')[-2000:]!r}"
        )
        assert t_exit - t0 <= 10.0, (
            f"the drain took {t_exit - t0:.1f}s, past the 10s termination deadline it is bounded by"
        )

        for job_id in job_ids:
            await _assert_drained_job_conserved(conn, schema, job_id)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}".consv_body_runs')
        await conn.close()


# ── Shape 2: the grace expiry (the hard deadline) ────────────────────────


@pytest.mark.timeout(240)
@pytest.mark.parametrize("trial", range(2))
async def test_grace_expiry_hard_exit_requeues_within_derived_hold_bound(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
    trial: int,
) -> None:
    """A job still running at the hard deadline (it defies both cancels):
    the worker's own watchdog kills the process at the deadline with no
    further notice. The row leaves RELEASING released-with-hold, scheduled
    until deadline + the exit tail; the successor's pickup is measured from
    the death instant against the derived hold-expiry bound, and the ledger
    shows the first attempt interrupted and the second run: exactly 2 runs."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s2-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    stderr_log = tmp_path / f"s2-{trial}-worker.err"
    sock = unique_health_sock_path(f"grace-s2-{trial}")
    succ_sock = unique_health_sock_path(f"grace-s2-succ-{trial}")
    proc = _spawn_grace_worker(
        pg_dsn,
        schema,
        sock,
        stderr_log,
        lock_lease="3.0",
        cancellation_grace="1.0",
        cleanup_grace="1.0",
        termination_grace="8.0",
    )
    successor: asyncio.Task[object] | None = None
    try:
        await _create_body_runs(conn, schema)
        _wait_for_socket(sock, proc, stderr_log)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            handle = await client.enqueue(grace_defier, ConsvPayload(hold=30.0), tags=[tag])
            job_id = handle.job_id

            await _wait_running(conn, schema, tag, want=1)

            # The rolling-deploy shape: the replacement is already up before
            # the hard exit, so the measured pickup is the recovery
            # mechanism's bound, not a boot time.
            successor = await _start_successor(
                pg_dsn,
                schema,
                _successor_settings(
                    pg_dsn,
                    schema,
                    succ_sock,
                    lock_lease="3.0",
                    cancellation_grace="1.0",
                    cleanup_grace="1.0",
                    termination_grace="8.0",
                ),
            )

            t0 = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            t_death = await _wait_exit(proc, cap_secs=30.0)
        finally:
            await stack.aclose()

        # The hard exit: the watchdog's deadline trip at the termination
        # grace, its own exit code 2, no further notice. The shutdown
        # cannot finish (the defying consumer never unwinds), the deadline
        # is the bound that ends it, and the death lands between the
        # deadline and the deadline + the exit tail.
        death = t_death - t0
        assert proc.returncode == 2, (
            f"the worker exited {proc.returncode}: the grace expiry must end "
            f"in the watchdog's deadline trip (exit 2); "
            f"stderr={stderr_log.read_text(errors='replace')[-2000:]!r}"
        )
        assert 7.0 <= death <= 8.0 + _TAIL + 2.0, (
            f"the hard exit landed {death:.1f}s after SIGTERM: outside "
            f"[the 8s deadline, deadline + exit tail + margin]"
        )

        # The pickup bound: from the death instant, the held row becomes
        # claimable at deadline + tail, one promote tick, one claim.
        pickup = await _wait_attempt_run(
            conn, schema, job_id, attempt=2, cap_secs=BOUND_HOLD_EXPIRY
        )
        assert pickup <= BOUND_HOLD_EXPIRY, (
            f"the successor picked the job up {pickup:.2f}s after the hard "
            f"exit, past the derived bound {BOUND_HOLD_EXPIRY:.1f}s "
            f"(exit tail {_TAIL} + sweep {_SWEEP} + claim {_CLAIM} + margin {_MARGIN})"
        )

        await _wait_terminal(
            conn, schema, tag, "succeeded", want=1, cap_secs=BOUND_HOLD_EXPIRY + 5.0
        )

        # The ledger: the FIRST attempt interrupted (released, not
        # terminalised, no attempt row: an interruption is not an execution
        # outcome), the SECOND run and succeeded. Exactly 2 runs.
        row = await _job_row(conn, schema, job_id)
        assert row["status"] == "succeeded" and row["attempt"] == 2, f"row: {row}"
        assert row["interrupt_count"] == 1, f"the interruption was not counted: {row}"
        assert row["marker"] == "landed", "the effect landed twice or never"
        assert await _body_run_count(conn, schema, job_id) == 2, (
            "the body ran fewer or more than exactly twice across the hard exit"
        )
        assert await _body_run_count(conn, schema, job_id, attempt=1) == 1
        assert await _body_run_count(conn, schema, job_id, attempt=2) == 1
        attempts = await _attempt_rows(conn, schema, job_id)
        assert len(attempts) == 1 and attempts[0]["attempt"] == 2, (
            f"the interrupted arm must write no attempt row for attempt 1 "
            f"and the second run's terminal write must land: {attempts}"
        )
        assert attempts[0]["outcome"] == "succeeded"
        assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'interrupted'") == 1
        assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'lock_expired'") == 0
        assert (
            await _event_count(
                conn, schema, job_id, "kind = 'state_change' AND detail->>'to_state' = 'succeeded'"
            )
            == 1
        ), "the job terminalised 'succeeded' more than once"
    finally:
        await _stop_successor(successor)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
            os.unlink(succ_sock)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}".consv_body_runs')
        await conn.close()


# ── Shape 3: the mid-terminal-write kill, both sides of the window ───────


@pytest.mark.timeout(180)
@pytest.mark.parametrize("trial", range(2))
async def test_kill_after_terminal_commit_is_terminal_and_done_once(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
    trial: int,
) -> None:
    """The kill lands the instant the terminal write COMMITS (the committed
    side of the window): the row is terminal-and-whole, the effect landed
    exactly once, and nothing requeued it."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s3a-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    stderr_log = tmp_path / f"s3a-{trial}-worker.err"
    sock = unique_health_sock_path(f"grace-s3a-{trial}")
    proc = _spawn_grace_worker(
        pg_dsn,
        schema,
        sock,
        stderr_log,
        lock_lease="3.0",
        cancellation_grace="1.0",
        cleanup_grace="1.0",
        termination_grace="8.0",
        kill_on_terminal=True,
    )
    try:
        await _create_body_runs(conn, schema)
        _wait_for_socket(sock, proc, stderr_log)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            handle = await client.enqueue(grace_tail, ConsvPayload(calls=6), tags=[tag])
            job_id = handle.job_id

            # The injection kills the process at the terminal write, about
            # 1.5s after the claim.
            await _wait_exit(proc, cap_secs=30.0)
        finally:
            await stack.aclose()

        assert proc.returncode == -9, (
            f"the harness exited {proc.returncode}: the terminal-write injection never fired"
        )
        row = await _job_row(conn, schema, job_id)

        # The committed side: terminal AND whole, done once.
        assert row["status"] == "succeeded", (
            f"the committed zombie left {row['status']!r}: the commit did not hold"
        )
        assert row["marker"] == "landed" and row["finished_at"] is not None
        assert row["attempt"] == 1 and row["interrupt_count"] == 0
        assert await _body_run_count(conn, schema, job_id) == 1, (
            "the committed shape must be done ONCE: the body-run ledger disagrees"
        )
        attempts = await _attempt_rows(conn, schema, job_id)
        assert len(attempts) == 1 and attempts[0]["outcome"] == "succeeded", (
            f"the attempt ledger is not whole: {attempts}"
        )
        # Never requeued: no reclaim fired, no second claim followed.
        assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'lock_expired'") == 0
        assert (
            await _event_count(
                conn, schema, job_id, "kind = 'state_change' AND detail->>'to_state' = 'succeeded'"
            )
            == 1
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}".consv_body_runs')
        await conn.close()


@pytest.mark.timeout(240)
@pytest.mark.parametrize("trial", range(2))
async def test_kill_before_terminal_write_requeues_within_derived_lease_bound(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
    trial: int,
) -> None:
    """The kill lands BEFORE the terminal write is attempted (the
    uncommitted side of the window): the row is left running with a live
    lease, the lease lapses, the reclaim re-pends, and the successor's
    second attempt conserves: exactly 2 runs, one succeeded terminal, the
    pickup inside the derived lease-lapse bound."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s3b-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    stderr_log = tmp_path / f"s3b-{trial}-worker.err"
    sock = unique_health_sock_path(f"grace-s3b-{trial}")
    succ_sock = unique_health_sock_path(f"grace-s3b-succ-{trial}")
    proc = _spawn_grace_worker(
        pg_dsn,
        schema,
        sock,
        stderr_log,
        lock_lease="3.0",
        cancellation_grace="1.0",
        cleanup_grace="1.0",
        termination_grace="8.0",
        kill_before_terminal=True,
    )
    successor: asyncio.Task[object] | None = None
    try:
        await _create_body_runs(conn, schema)
        _wait_for_socket(sock, proc, stderr_log)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            handle = await client.enqueue(grace_tail, ConsvPayload(calls=6), tags=[tag])
            job_id = handle.job_id

            await _wait_running(conn, schema, tag, want=1)

            successor = await _start_successor(
                pg_dsn,
                schema,
                _successor_settings(
                    pg_dsn,
                    schema,
                    succ_sock,
                    lock_lease="3.0",
                    cancellation_grace="1.0",
                    cleanup_grace="1.0",
                    termination_grace="8.0",
                ),
            )

            await _wait_exit(proc, cap_secs=30.0)
        finally:
            await stack.aclose()

        assert proc.returncode == -9, (
            f"the harness exited {proc.returncode}: the pre-terminal injection never fired"
        )
        # The pickup measurement anchors at the poll helper's own clock.

        # The uncommitted side: the row is NOT terminal; the recovery is
        # the lease lapse, the reclaim, the requeue, the second attempt.
        pickup = await _wait_attempt_run(
            conn, schema, job_id, attempt=2, cap_secs=BOUND_LEASE_LAPSE
        )
        assert pickup <= BOUND_LEASE_LAPSE, (
            f"the successor picked the job up {pickup:.2f}s after the kill, "
            f"past the derived bound {BOUND_LEASE_LAPSE:.1f}s (lease {_LEASE} "
            f"+ takeover {max(_LEADER_LEASE, 4 * _HB) + _HB} + sweep {_SWEEP} "
            f"+ backoff {_BACKOFF} + claim {_CLAIM} + margin {_MARGIN})"
        )

        await _wait_terminal(
            conn, schema, tag, "succeeded", want=1, cap_secs=BOUND_LEASE_LAPSE + 5.0
        )

        row = await _job_row(conn, schema, job_id)
        assert row["status"] == "succeeded" and row["attempt"] == 2, f"row: {row}"
        assert row["marker"] == "landed", "the effect landed twice or never"
        assert await _body_run_count(conn, schema, job_id) == 2, (
            "the uncommitted shape must requeue to exactly one second run"
        )
        assert await _body_run_count(conn, schema, job_id, attempt=1) == 1
        assert await _body_run_count(conn, schema, job_id, attempt=2) == 1
        attempts = await _attempt_rows(conn, schema, job_id)
        assert len(attempts) == 2, f"the attempt ledger lost or doubled a claim: {attempts}"
        assert attempts[0]["attempt"] == 1 and attempts[0]["outcome"] == "crashed", (
            f"the first attempt must stand as crashed (the reclaim's honest label): {attempts}"
        )
        assert attempts[0]["error_class"] == "WorkerCrashed"
        assert attempts[1]["attempt"] == 2 and attempts[1]["outcome"] == "succeeded"
        assert await _event_count(conn, schema, job_id, "detail->>'reason' = 'lock_expired'") >= 1
        assert (
            await _event_count(
                conn, schema, job_id, "kind = 'state_change' AND detail->>'to_state' = 'succeeded'"
            )
            == 1
        ), "the job terminalised 'succeeded' more than once"
    finally:
        await _stop_successor(successor)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
            os.unlink(succ_sock)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}".consv_body_runs')
        await conn.close()


# ── Shape 4: the SIGTERM mid-drain (the cancel ladder walk killed) ───────


@pytest.mark.timeout(240)
@pytest.mark.parametrize("trial", range(2))
async def test_kill_mid_cancel_ladder_strands_nothing_loses_no_cancel(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
    trial: int,
) -> None:
    """SIGTERM starts the drain; the kill lands mid-cancel-ladder with an
    operator cancel armed on one job. The partially walked ladder state
    meets the successor's reclaim: the requested job terminalises
    'cancelled' exactly once with its audit intact (no lost cancel), the
    unrequested one requeues and re-runs (no stranded row), both inside the
    derived lease-lapse bound."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s4-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    stderr_log = tmp_path / f"s4-{trial}-worker.err"
    sock = unique_health_sock_path(f"grace-s4-{trial}")
    succ_sock = unique_health_sock_path(f"grace-s4-succ-{trial}")
    proc = _spawn_grace_worker(
        pg_dsn,
        schema,
        sock,
        stderr_log,
        lock_lease="3.0",
        cancellation_grace="1.5",
        cleanup_grace="1.0",
        termination_grace="8.0",
    )
    successor: asyncio.Task[object] | None = None
    try:
        await _create_body_runs(conn, schema)
        _wait_for_socket(sock, proc, stderr_log)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        requested_id: object = None
        plain_id: object = None
        try:
            client = JobsClient(backend)
            handle = await client.enqueue(grace_holder, ConsvPayload(hold=30.0), tags=[tag])
            requested_id = handle.job_id
            handle_b = await client.enqueue(grace_holder, ConsvPayload(hold=30.0), tags=[tag])
            plain_id = handle_b.job_id

            await _wait_running(conn, schema, tag, want=2)

            successor = await _start_successor(
                pg_dsn,
                schema,
                _successor_settings(
                    pg_dsn,
                    schema,
                    succ_sock,
                    lock_lease="3.0",
                    cancellation_grace="1.5",
                    cleanup_grace="1.0",
                    termination_grace="8.0",
                ),
            )

            # The operator's request arms on the running row, THEN the
            # drain starts: the ladder walk owns both jobs when the hard
            # exit lands.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET cancel_phase = 1, '
                "cancel_requested_at = clock_timestamp() WHERE id = $1",
                requested_id,
            )

            proc.send_signal(signal.SIGTERM)
            # Kill mid-walk: CANCELLING's grace runs 1.5s from the signal,
            # the SIGKILL at +0.5s lands inside the ladder's walk.
            await asyncio.sleep(0.5)
            proc.kill()
            await _wait_exit(proc, cap_secs=10.0)
        finally:
            await stack.aclose()

        assert proc.returncode == -9, (
            f"the worker exited {proc.returncode} instead of dying by the mid-drain SIGKILL"
        )

        # The requested job's derived bound. The sweep's cancel carve-out:
        # a cancel-carrying running row is crash-reclaim eligible only past
        # its lease lapse PLUS cancel_grace + cleanup_grace + the flat 60s
        # cooperative headroom (the protocol's own completion window, the
        # margin that keeps a merely-slow cancellation from being mistaken
        # for a crash); one sweep tick then owns it, then the terminal
        # write. The takeover term is long inside the headroom and adds
        # nothing here.
        cancel_bound = _LEASE + 1.5 + 1.0 + 60.0 + _SWEEP + _MARGIN

        # No lost cancel: the requested job terminalises 'cancelled', the
        # request's audit survives, and the body never ran again.
        await _wait_terminal(conn, schema, tag, "cancelled", want=1, cap_secs=cancel_bound)
        requested = await _job_row(conn, schema, requested_id)
        assert requested["status"] == "cancelled", (
            f"the operator's cancel was lost: the job landed "
            f"{requested['status']!r} instead of cancelled"
        )
        assert requested["cancel_requested_at"] is not None, (
            "the honoured cancel lost its audit trail"
        )
        assert requested["cancel_phase"] != 0
        assert requested["finished_at"] is not None
        assert await _body_run_count(conn, schema, requested_id) == 1, (
            "the cancelled job's body ran again: the cancel was lost and the row requeued"
        )
        requested_attempts = await _attempt_rows(conn, schema, requested_id)
        assert len(requested_attempts) == 1, (
            f"the cancelled job's attempt ledger is not one row: {requested_attempts}"
        )
        # The attempt row is the reclaim's crash row (the kill beat the
        # ladder) or the ladder's own cancelled row (the ladder beat the
        # kill); both are the honest label of which arm owned it.
        assert requested_attempts[0]["outcome"] in ("cancelled", "crashed"), (
            f"unexpected attempt outcome: {requested_attempts}"
        )
        assert (
            await _event_count(
                conn,
                schema,
                requested_id,
                "kind = 'state_change' AND detail->>'to_state' = 'cancelled'",
            )
            >= 1
        )

        # No stranded row: the unrequested job requeues within the bound
        # and re-runs to terminal.
        pickup = await _wait_attempt_run(
            conn, schema, plain_id, attempt=2, cap_secs=BOUND_LEASE_LAPSE
        )
        assert pickup <= BOUND_LEASE_LAPSE, (
            f"the unrequested job's requeue took {pickup:.2f}s past the "
            f"derived bound {BOUND_LEASE_LAPSE:.1f}s"
        )
        await _wait_terminal(
            conn, schema, tag, "succeeded", want=1, cap_secs=BOUND_LEASE_LAPSE + 5.0
        )
        plain = await _job_row(conn, schema, plain_id)
        assert plain["status"] == "succeeded" and plain["attempt"] == 2
        assert await _body_run_count(conn, schema, plain_id) == 2

        # The population, whole: no row still running, no row locked to the
        # dead worker, no stranded phase-2 row.
        stranded = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND (status = 'running' OR cancel_phase = 2)",
            tag,
        )
        assert stranded == 0, f"{stranded} tagged rows stranded mid-ladder at settle"
    finally:
        await _stop_successor(successor)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
            os.unlink(succ_sock)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}".consv_body_runs')
        await conn.close()


# ── Shape 5: the grace period = 0 shape (the immediate kill) ─────────────


@pytest.mark.timeout(240)
@pytest.mark.parametrize("trial", range(2))
async def test_immediate_kill_pure_lease_lapse_conserves_and_requeues_in_bound(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
    trial: int,
) -> None:
    """No notice at all: the SIGKILL lands mid-body (a zero grace; the
    5s settings floor makes the real zero unrepresentable, the kernel kill
    is the shape it names). The pure lease-lapse path: the rows are left
    running with live leases, the takeover plus the reclaim cadence hands
    them back, the successor re-runs them, and the whole population
    conserves with the pickup inside the derived bound."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-s5-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    stderr_log = tmp_path / f"s5-{trial}-worker.err"
    sock = unique_health_sock_path(f"grace-s5-{trial}")
    succ_sock = unique_health_sock_path(f"grace-s5-succ-{trial}")
    proc = _spawn_grace_worker(
        pg_dsn,
        schema,
        sock,
        stderr_log,
        lock_lease="3.0",
        cancellation_grace="1.0",
        cleanup_grace="1.0",
        termination_grace="8.0",
    )
    successor: asyncio.Task[object] | None = None
    try:
        await _create_body_runs(conn, schema)
        _wait_for_socket(sock, proc, stderr_log)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        job_ids: list[object] = []
        try:
            client = JobsClient(backend)
            for _ in range(2):
                handle = await client.enqueue(grace_holder, ConsvPayload(hold=30.0), tags=[tag])
                job_ids.append(handle.job_id)

            await _wait_running(conn, schema, tag, want=2)

            successor = await _start_successor(
                pg_dsn,
                schema,
                _successor_settings(
                    pg_dsn,
                    schema,
                    succ_sock,
                    lock_lease="3.0",
                    cancellation_grace="1.0",
                    cleanup_grace="1.0",
                    termination_grace="8.0",
                ),
            )
            await asyncio.sleep(0.5)

            proc.kill()
            await _wait_exit(proc, cap_secs=10.0)
        finally:
            await stack.aclose()

        assert proc.returncode == -9

        # Pickup bound for BOTH rows, measured from the death instant.
        for job_id in job_ids:
            pickup = await _wait_attempt_run(
                conn, schema, job_id, attempt=2, cap_secs=BOUND_LEASE_LAPSE
            )
            assert pickup <= BOUND_LEASE_LAPSE, (
                f"the successor picked job {job_id} up {pickup:.2f}s after "
                f"the kill, past the derived bound {BOUND_LEASE_LAPSE:.1f}s"
            )

        await _wait_terminal(
            conn, schema, tag, "succeeded", want=2, cap_secs=BOUND_LEASE_LAPSE + 5.0
        )
        for job_id in job_ids:
            row = await _job_row(conn, schema, job_id)
            assert row["status"] == "succeeded" and row["attempt"] == 2, f"row: {row}"
            assert row["marker"] == "landed"
            assert await _body_run_count(conn, schema, job_id) == 2, (
                "exactly 2 runs across the hard kill: never 0, never 3"
            )
            attempts = await _attempt_rows(conn, schema, job_id)
            assert len(attempts) == 2, f"the attempt ledger lost or doubled a claim: {attempts}"
            assert (
                attempts[0]["outcome"] == "crashed"
                and attempts[0]["error_class"] == "WorkerCrashed"
            )
            assert attempts[1]["outcome"] == "succeeded"
            assert (
                await _event_count(conn, schema, job_id, "detail->>'reason' = 'lock_expired'") >= 1
            )
            assert (
                await _event_count(
                    conn,
                    schema,
                    job_id,
                    "kind = 'state_change' AND detail->>'to_state' = 'succeeded'",
                )
                == 1
            )

        # Conservation over the whole population: no limbo row (running
        # with a lapsed lease and no live holder), no row locked to the
        # dead worker, every row terminal.
        rows = await conn.fetch(
            f"SELECT id, status::text AS status, lock_expires_at AS lock_expires_at, "
            f'locked_by_worker AS locked_by FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text]",
            tag,
        )
        assert len(rows) == 2, f"the population is not whole: {rows}"
        for r in rows:
            assert r["status"] == "succeeded", f"a row never reached terminal: {r}"
            assert r["locked_by"] is None, f"a row is still locked to a dead worker: {r}"
            assert r["lock_expires_at"] is None, f"a terminal row still carries a lease: {r}"
    finally:
        await _stop_successor(successor)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
            os.unlink(succ_sock)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}".consv_body_runs')
        await conn.close()
