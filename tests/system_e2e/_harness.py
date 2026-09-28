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
from typing import IO, NamedTuple
from urllib.parse import urlparse, urlunparse

from taskq.testing.health import unique_health_sock_path

_WORKER_ENTRY = "tests.system_e2e._worker_entry"

#: Fleet defaults: the same cascade the e2e conftest validates (command
#: budget floor: max(0.5, 0.5) + 4 * (0.5 + 0.5) = 4.5 <= lease 8.0; the
#: lease is the loop-stall budget for in-flight jobs, so it carries the
#: e2e fleet's 8s margin against co-tenant load stalls).
_BASE_ENV: dict[str, str] = {
    "TASKQ_QUEUES": "system_e2e",
    "TASKQ_POLL_INTERVAL": "0.05",
    "TASKQ_SWEEP_INTERVAL": "1.0",
    "TASKQ_HEARTBEAT_INTERVAL": "0.5",
    "TASKQ_LOCK_LEASE": "8.0",
    "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
    "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
    "TASKQ_TERMINATION_GRACE_PERIOD": "15.0",
    "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.5",
    "TASKQ_WATCHDOG_ENABLED": "false",
    "TASKQ_MAX_CONCURRENCY": "4",
    "TASKQ_MIGRATE_ON_START": "false",
    "TASKQ_ENVIRONMENT": "dev",
}


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
    """A spawned worker subprocess plus the readiness socket it was given.

    ``log_path``: when the spawn was given a ``log_sink``, the file BOTH
    streams write to - the diagnostic surface a partitioned worker's own
    failure record needs, and the reason partition scenarios use the sink
    at all: a worker whose PG path is cut logs every failed tick, and a
    64KB PIPE nobody drains fills in seconds, blocking the worker's
    logging write - a WEDGE THE HARNESS ITSELF would manufacture, exactly
    the "hang" the partition contracts say must not happen."""

    proc: subprocess.Popen[bytes]
    sock_path: str
    log_path: str | None = None

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
    log_sink: str | None = None,
) -> WorkerProc:
    """One worker subprocess: a real OS process running the real bootstrap.

    The DSN is application_name-scoped; the health socket is unique per
    spawn (the tag plus the pid), so concurrent scenarios never gate on
    each other's readiness.

    ``log_sink``: a file path to redirect BOTH streams into. Scenarios
    whose workers log continuously (chaos windows, failing heartbeats)
    MUST pass one: the default PIPE pair is sized for short-lived
    readiness-spawned workers, and a full pipe blocks the child's first
    logging write - a harness-manufactured wedge.
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
    stdout: IO[str] | int = subprocess.PIPE
    stderr: IO[str] | int = subprocess.PIPE
    log_file: IO[str] | None = None
    if log_sink is not None:
        log_file = open(log_sink, "w")  # noqa: SIM115  # Why: closed by reap/graceful_stop's communicate, the same lifetime the PIPEs' reader has.
        stdout = log_file
        stderr = log_file
    proc = subprocess.Popen(  # noqa: S603  # Why: fixed argv, no shell, binary is this interpreter, module is project-owned; no untrusted input.
        [sys.executable, "-m", _WORKER_ENTRY],
        env=env,
        cwd=os.environ.get("TASKQ_REPO_ROOT", os.getcwd()),
        stderr=stderr,
        stdout=stdout,
    )
    if log_file is not None:
        # The child owns the dup'ed descriptors now.
        log_file.close()
    return WorkerProc(proc=proc, sock_path=sock_path, log_path=log_sink)


def wait_for_socket(socket_path: str, proc: subprocess.Popen[bytes]) -> None:
    """Block until the worker's health socket accepts, or fail loudly with
    the worker's stderr (a bootstrap crash must not surface as a bare
    socket timeout)."""
    deadline = time.monotonic() + 30.0
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
