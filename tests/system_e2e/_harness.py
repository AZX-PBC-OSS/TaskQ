"""The worker subprocess harness: spawn, readiness, signal, reap.

This is the subprocess harness pattern from the conservation chaos suite
(tests/test_rt_conservation_chaos.py's ``_spawn_consv_worker`` /
``_wait_for_socket`` pair), lifted into the system tier's shared module so
every lifecycle scenario drives workers through ONE spawn surface: real
OS processes running the real bootstrap, env-configured like a pod, with
a health socket as the readiness signal.

Worker defaults mirror the e2e fleet's timing knobs (lease >= 4 beats, a
command budget with cascade headroom, graces under the lease) so the
invariants are exercised at production-shaped cadences, not at fixtures
that make the sweeps invisible.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import time
from typing import TYPE_CHECKING, NamedTuple
from urllib.parse import urlparse, urlunparse

from taskq.testing.health import unique_health_sock_path

if TYPE_CHECKING:
    import asyncpg

_WORKER_ENTRY = "tests.system_e2e._worker_entry"

#: Fleet constants: the SINGLE SOURCE the tier's scenarios import when a
#: bound must scale with the fleet's cadence (``test_rolling_release_fleet.py``'s
#: drain/lease bounds, the reaping-window budget below). ``_BASE_ENV`` derives
#: from these same names, so a scenario can never drift from the cadence its
#: workers actually boot with.
#: Fleet defaults: the same cascade the e2e conftest validates (command
#: budget floor: max(0.5, 0.5) + 4 * (0.5 + 0.5) = 4.5 <= lease 8.0; the
#: lease is the loop-stall budget for in-flight jobs, so it carries the
#: e2e fleet's 8s margin against co-tenant load stalls).
HEARTBEAT_INTERVAL_S = 0.5
LOCK_LEASE_S = 8.0
CANCELLATION_GRACE_S = 1.0
CLEANUP_GRACE_S = 1.0
TERMINATION_GRACE_S = 15.0
SWEEP_INTERVAL_S = 1.0
#: The harness's own readiness bound (``wait_for_socket``): a worker whose
#: bootstrap has not answered its health socket within this window is a
#: failed spawn, and every scenario's "booted and registered" cap derives
#: from the same number.
BOOT_READY_BOUND_S = 30.0

_BASE_ENV: dict[str, str] = {
    "TASKQ_QUEUES": "system_e2e",
    "TASKQ_POLL_INTERVAL": "0.05",
    "TASKQ_SWEEP_INTERVAL": str(SWEEP_INTERVAL_S),
    "TASKQ_HEARTBEAT_INTERVAL": str(HEARTBEAT_INTERVAL_S),
    "TASKQ_LOCK_LEASE": str(LOCK_LEASE_S),
    "TASKQ_CANCELLATION_GRACE_PERIOD": str(CANCELLATION_GRACE_S),
    "TASKQ_CLEANUP_GRACE_PERIOD": str(CLEANUP_GRACE_S),
    "TASKQ_TERMINATION_GRACE_PERIOD": str(TERMINATION_GRACE_S),
    "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.5",
    "TASKQ_WATCHDOG_ENABLED": "false",
    "TASKQ_MAX_CONCURRENCY": "4",
    "TASKQ_MIGRATE_ON_START": "false",
    "TASKQ_ENVIRONMENT": "dev",
}


def stale_worker_reap_window_s(
    heartbeat_interval: float = HEARTBEAT_INTERVAL_S, max_heartbeat_failures: int = 3
) -> float:
    """The stale-worker cleanup's liveness window, in seconds.

    The leader's sweep deletes a worker row whose ``last_seen_at`` is older
    than ``heartbeat_interval * (max_heartbeat_failures + 3)`` — the same
    derivation ``_leader_sweeps.py``'s ``stale_workers_grace`` runs (the
    ``max_heartbeat_failures`` default here is the settings default). At the
    tier's own cadence that is 0.5 * (3 + 3) = 3s: a healthy replica whose
    beats stall past this window on a loaded host has its row reaped
    mid-scenario. Every bound that reads FLEET MEMBERSHIP (the workers
    table, the leader row, imbalance's ``live_workers``) must budget this
    window — bounds that only read job rows need not.
    """
    return heartbeat_interval * (max_heartbeat_failures + 3)


#: The co-tenancy stretch between a fleet-progress bound's arithmetic and
#: its observation on a shared runner (dd4572ff's stall band, the same
#: measured runner weather test_cancel_storm's storm deadline derives
#: from). A bound that budgets PROGRESS — a settle, a reclaim sweep, a
#: re-pickup after a requeue — multiplies its derived arithmetic by this,
#: so a loaded runner stretches the bound instead of voiding the
#: scenario; a bound that catches a LEAK keeps its bite either way, the
#: failure mode is "never", and no finite stretch tolerates it.
TIER_LOAD_STRETCH = 2.0

#: The deployment-shaped cancel margins: the escalation-to-abandoned the
#: graces' expiry produces must not fire on a healthy holder whose loop
#: stalls a few seconds under co-tenant load (the harness's 1s+1s pair is
#: the chaos tier's precision - a body must hit the forced ladder within
#: seconds). These are the pair the operational-loop family runs (the
#: shipped settings defaults are 20s+20s): a stalled beat still honours
#: the cooperative phases. The lease is the family's cascade-legal
#: companion (cancellation + cleanup < lease; the command budget
#: 0.5 + 4 * (2.0 + 0.5) = 10.5 <= 24), and the shutdown budget the
#: graces imply.
DEPLOYMENT_CANCELLATION_GRACE_S = 10.0
DEPLOYMENT_CLEANUP_GRACE_S = 10.0
DEPLOYMENT_LOCK_LEASE_S = 24.0
DEPLOYMENT_HEARTBEAT_INTERVAL_S = 2.0
DEPLOYMENT_TERMINATION_GRACE_S = 40.0


def scoped_dsn(pg_dsn: str, schema: str) -> str:
    """The DSN with ``application_name`` pinned to the schema, so a
    scenario that must kill connections (failover shapes) can target its
    own workers without touching sibling xdist workers."""
    parsed = urlparse(pg_dsn)
    query = (
        f"application_name={schema}"
        if not parsed.query
        else f"{parsed.query}&application_name={schema}"
    )
    return urlunparse(
        parsed._replace(query=query)
    )  # Why: namedtuple._replace is the sanctioned API.


class WorkerProc(NamedTuple):
    """A spawned worker subprocess plus the readiness socket it was given."""

    proc: subprocess.Popen[bytes]
    sock_path: str

    @property
    def returncode(self) -> int | None:
        return self.proc.returncode

    def poll(self) -> int | None:
        return self.proc.poll()


def spawn_worker(
    pg_dsn: str,
    schema: str,
    *,
    redis_url: str | None = None,
    tag: str = "sys",
    extra_env: dict[str, str] | None = None,
) -> WorkerProc:
    """One worker subprocess: a real OS process running the real bootstrap.

    The DSN is application_name-scoped; the health socket is unique per
    spawn (the tag plus the pid), so concurrent scenarios never gate on
    each other's readiness.

    The worker's stderr is teed to a file next to its health socket (a
    daemon reader thread drains the pipe for the process's whole life):
    a scenario that needs the worker's own log lines - a wedged-loop
    forensic, a failed-tick cascade - reads the file instead of the
    pipe, and an undrained 64K pipe can never block a chatty worker
    mid-write.
    """
    sock_path = unique_health_sock_path(f"syse2e-{tag}")
    env = {**os.environ, **_BASE_ENV}
    env.update(
        {
            "TASKQ_PG_DSN": scoped_dsn(pg_dsn, schema),
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_HEALTH_SOCKET_PATH": sock_path,
        }
    )
    if redis_url is not None:
        env["TASKQ_REDIS_URL"] = redis_url
    if extra_env is not None:
        env.update(extra_env)
    proc = subprocess.Popen(  # noqa: S603  # Why: fixed argv, no shell, binary is this interpreter, module is project-owned; no untrusted input.
        [sys.executable, "-m", _WORKER_ENTRY],
        env=env,
        cwd=os.environ.get("TASKQ_REPO_ROOT", os.getcwd()),
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    )
    _tee_stderr(proc, f"{sock_path}.worker.log")
    return WorkerProc(proc=proc, sock_path=sock_path)


def _tee_stderr(proc: subprocess.Popen[bytes], log_path: str) -> None:
    """Drain *proc*'s stderr into *log_path* for the process's whole life."""

    def _pump() -> None:
        with open(log_path, "wb") as log:
            stream = proc.stderr
            assert stream is not None
            while True:
                chunk = stream.read(4096)
                if not chunk:
                    return
                log.write(chunk)
                log.flush()

    import threading

    threading.Thread(target=_pump, daemon=True).start()


async def spawn_joined_worker(
    conn: asyncpg.Connection,
    dsn: str,
    schema: str,
    tag: str,
    extra_env: dict[str, str] | None = None,
) -> WorkerProc:
    """One fleet replica, held to the operator's own deployment standard.

    Not just "the process booted and its health socket answers": the
    replica must JOIN the fleet - a registered row whose ``last_seen_at``
    the heartbeat actually advances. The distinction is not decorative:
    on a cold, contended container a replica can take its boot steps
    slowly enough - or stall them long enough - that it is registered and
    health-green while its first beats are still minutes away from
    landing, and on a loaded host the stale-worker cleanup's reaping
    window (``stale_worker_reap_window_s``: 3s at the tier's own 0.5s
    beat) can reap a healthy replica's row mid-scenario, leaving a
    GHOST: a live process heartbeating a deleted row, which no wait can
    repair. What the operator's orchestrator sees on a starved host is
    the same thing this helper sees: a pod that never went Ready. The
    remedy is the restart, and this helper does exactly that: verify the
    heartbeat advances, else reap and respawn, and fail only when no
    spawn joins.
    """
    import asyncio

    last_error = ""
    for _attempt in range(3):
        worker = spawn_worker(dsn, schema, tag=tag, extra_env=extra_env)
        wait_worker_ready(worker)
        pid = worker.proc.pid

        async def _registered(pid: int = pid) -> bool:
            row = await conn.fetchval(
                f'SELECT last_seen_at FROM "{schema}".workers WHERE pid = $1',  # noqa: S608  # Why: every query's schema identifier comes from the settings boundary the caller validated, and every value is $-bound.
                pid,
            )
            return row is not None

        try:
            await asyncio.wait_for(_registered(), timeout=BOOT_READY_BOUND_S)
            seen_1 = await conn.fetchval(
                f'SELECT last_seen_at FROM "{schema}".workers WHERE pid = $1',  # noqa: S608  # Why: every query's schema identifier comes from the settings boundary the caller validated, and every value is $-bound.
                pid,
            )
            await asyncio.sleep(5.0)  # two and a half beats at the fleet's cadence
            seen_2 = await conn.fetchval(
                f'SELECT last_seen_at FROM "{schema}".workers WHERE pid = $1',  # noqa: S608  # Why: every query's schema identifier comes from the settings boundary the caller validated, and every value is $-bound.
                pid,
            )
            if seen_2 is not None and seen_1 is not None and seen_2 > seen_1:
                return worker
            last_error = (
                f"replica {tag}'s heartbeat never advanced its registered row "
                f"(booted green, never joined the fleet); restarted"
            )
        except TimeoutError as exc:
            last_error = f"replica {tag} never registered its fleet row: {exc!r}"
        reap(worker)
    raise AssertionError(f"the replica never joined the fleet in 3 spawns: {last_error}")


def wait_for_socket(socket_path: str, proc: subprocess.Popen[bytes]) -> None:
    """Block until the worker's health socket accepts, or fail loudly with
    the worker's stderr (a bootstrap crash must not surface as a bare
    socket timeout)."""
    deadline = time.monotonic() + BOOT_READY_BOUND_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _stdout, stderr = proc.communicate(timeout=5)
            raise RuntimeError(
                f"worker exited rc={proc.returncode} before readiness: {stderr.decode()!r}"
            )
        try:
            with socket.socket(socket.AF_UNIX) as sock:
                sock.settimeout(0.1)
                sock.connect(socket_path)
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"health socket {socket_path!r} did not appear within 30s")


def wait_worker_ready(worker: WorkerProc) -> None:
    """Readiness gate: the worker's own health socket answers."""
    wait_for_socket(worker.sock_path, worker.proc)


def graceful_stop(worker: WorkerProc, timeout: float = 60.0) -> int:
    """SIGTERM, then wait; a worker past its termination grace is SIGKILLed
    and the exit code reports that (the caller's invariant decides whether
    a -9 is acceptable)."""
    proc = worker.proc
    proc.terminate()
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        return proc.returncode if proc.returncode is not None else -9


def reap(worker: WorkerProc) -> None:
    """Teardown: make sure nothing survives the test, however the body ended."""
    proc = worker.proc
    if proc.poll() is None:
        proc.kill()
    with contextlib.suppress(subprocess.TimeoutExpired):  # reap of last resort
        proc.wait(timeout=10)
