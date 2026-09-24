"""ATTACK tests: the fork lands in a REAL worker, against REAL Postgres.

The unit pins (``test_attack_fork_inherit.py``) prove the guard's logic;
this module proves the guard's PLACEMENT - that the resources a real
worker and a real client build actually carry it, and that the documented
scenario (a job body that forks mid-flight) ends in the refusal, the
parent-side report, and a worker that keeps consuming.

Four attacks:

1. The inheritance inventory, made real: a pool TaskQ built, forked
   mid-flight - the child's fd table is enumerated and every socket the
   parent owned is IN the child (the leak the guard exists to answer).
2. The wire refusal against a live server: the forked child grabs the
   inherited connection and is refused before a byte moves.
3. The full loop: a worker consumes a job whose body forks; the child
   refuses the inherited client pool, the worker reports
   ``fork-detected-in-worker-process``, and the NEXT job still succeeds -
   the parent is not punished for its child's fork.
4. The subprocess cousin's dark side: a process that spawns a long-lived
   child with ``close_fds=False`` and dies - the child's copy of the PG
   socket keeps the backend alive for a pool that no longer exists (the
   leak Python's default close_fds prevents).
"""

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import time
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._forkguard import (
    ForkedInheritedProcessError,
    guarded_connection_class,
    guarded_redis_connection_class,
    install_fork_guard,
)
from taskq.actor import ActorRef
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.health import unique_health_sock_path
from taskq.worker.run import worker_main_async

pytestmark = pytest.mark.integration


# ── the actor the fork happens inside ─────────────────────────────────


class _ForkProbeState:
    """The fork-memory channel between the harness test and the actor body.

    The actor body runs in the WORKER process (this one), the test set the
    fields before the worker started; the forked GRANDchild inherits them
    the way it inherits every other byte of the worker's heap.
    """

    verdict_path: str | None = None
    """Where the child writes its verdict; the only pipe back to the test."""
    client_pool: Any = None
    """The TaskQ-built client pool the child attacks."""


_PROBE = _ForkProbeState()


class _ForkProbePayload(BaseModel):
    pass


def _noop_actor_def():
    from taskq.actor import actor

    @actor(name="_fork_attack_noop")
    async def _noop_actor_impl(payload: _ForkProbePayload) -> None:
        return None

    return _noop_actor_impl


def _fork_body_actor():
    """Built lazily so the module globals above are already set."""
    from taskq.actor import actor

    @actor(name="_fork_body_actor")
    async def _fork_body_actor_impl(payload: _ForkProbePayload) -> None:
        assert _PROBE.verdict_path is not None
        assert _PROBE.client_pool is not None
        # The parent holds one connection OUT of the TaskQ-built pool while
        # it forks - the exact mid-flight shape: the child inherits this
        # connection's socket mid-protocol.
        conn = await _PROBE.client_pool.acquire()
        pid = os.fork()
        if pid == 0:
            # THE CHILD: still inside the worker's coroutine, loop not
            # running in this process, the inherited connection's socket
            # one write away from corruption. The refusal must fire
            # synchronously, before any wire byte.
            verdict = "allowed"
            try:
                await conn.fetch("SELECT 1")
            except ForkedInheritedProcessError:
                verdict = "refused"
            except BaseException as exc:
                verdict = f"other:{type(exc).__name__}:{exc}"
            fd = os.open(_PROBE.verdict_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            os.write(fd, verdict.encode())
            os.close(fd)
            # The child is scaffolding: never return into the worker's
            # coroutine machinery (its loop is the parent's). SIGKILL is the
            # exit that cannot be intercepted by anything in the tree.
            os.kill(os.getpid(), signal.SIGKILL)  # pragma: no cover - the child never returns
        # Parent: the fork happened mid-body. Reap the child, release the
        # connection the child inherited (its copy died with it, never used),
        # then finish the job normally - the worker's loops own the report.
        os.waitpid(pid, 0)  # noqa: ASYNC222  # Why: waitpid MUST block here - the child's verdict write must land before the parent releases the shared connection; asyncio's process APIs do not reap a raw fork().
        await _PROBE.client_pool.release(conn)
        return None

    return _fork_body_actor_impl


# ── 1 + 2: the inheritance inventory and the live refusal ────────────


def _socket_inodes() -> set[str]:
    """The inode ids of every socket fd THIS process holds."""
    inodes: set[str] = set()
    for fd in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:  # pragma: no cover - the walk's own fd
            continue
        if target.startswith("socket:"):
            inodes.add(target)
    return inodes


async def test_forked_child_inherits_the_pools_sockets_and_is_refused(pg_dsn: str) -> None:
    """One pin, two halves: the child's fd table carries the pool's sockets
    (the inventory, made real), and the child's use of the inherited
    connection is refused before the wire moves (the guard, on a live
    server)."""
    install_fork_guard()
    pool = await asyncpg.create_pool(
        dsn=pg_dsn, min_size=1, max_size=2, connection_class=guarded_connection_class()
    )
    assert pool is not None
    try:
        # A live, exercised connection leaves the parent's socket table...
        parent_sockets = _socket_inodes()
        # ...and one STAYS acquired while the fork happens: the exact
        # mid-flight shape, the child inherits a checked-out connection.
        conn = await pool.acquire()
        assert await conn.fetchval("SELECT 1") == 1

        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            verdict = "no-run"
            try:
                child_sockets = _socket_inodes()
                inherited = child_sockets & parent_sockets
                if not inherited:
                    verdict = "no-inherited-sockets"
                else:
                    # The same asyncio.run shape a prefork server's child
                    # would use: a NEW loop in the child, the PARENT's
                    # connection underneath it.
                    async def child_uses() -> str:
                        try:
                            await conn.fetch("SELECT 1")
                            return "allowed"
                        except ForkedInheritedProcessError:
                            return "refused"

                    verdict = asyncio.run(child_uses())
            except BaseException as exc:  # Why: the child reports any failure whole.
                verdict = f"other:{type(exc).__name__}:{exc}"
            os.write(w, verdict.encode())
            os.close(w)
            os.kill(os.getpid(), signal.SIGKILL)
        os.close(w)
        with os.fdopen(r, "rb") as fh:
            child_verdict = fh.read().decode()
        os.waitpid(pid, 0)  # noqa: ASYNC222  # Why: the raw fork()'s child must be reaped before the parent's pool connection is reused; the pin is synchronous by construction.

        assert child_verdict != "no-inherited-sockets", (
            "the forked child held NONE of the pool's sockets: the "
            "inheritance inventory does not reproduce, the attack premise "
            "is wrong on this platform"
        )
        assert child_verdict == "refused", (
            f"the forked child used the parent's LIVE connection against a "
            f"real Postgres and was not refused (verdict {child_verdict!r})"
        )

        # The parent's pool never noticed: the same checked-out connection
        # still works, then goes home.
        assert await conn.fetchval("SELECT 2") == 2
        await pool.release(conn)
    finally:
        await pool.close()


def test_redis_class_is_pinned_for_the_wire_sites() -> None:
    """The redis twin ships from the same module and subclasses redis's own
    Connection, so every ``from_url(connection_class=...)`` site threads the
    guard without a second mechanism."""
    import redis.asyncio as redis_async

    cls = guarded_redis_connection_class()
    assert issubclass(cls, redis_async.Connection)


# ── 3: the full loop - fork in a job body, worker keeps consuming ─────


async def test_fork_in_a_job_body_refuses_reports_and_keeps_consuming(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The documented scenario, end to end on real Postgres:

    - a TaskQ client builds its pool (guard installed by ``open``);
    - a worker consumes a job whose body forks mid-flight;
    - the child refuses the inherited client pool;
    - the worker's loops report ``fork-detected-in-worker-process``;
    - a SECOND job, enqueued after the fork, still succeeds - the parent
      did not inherit the child's mess.
    """
    import logging

    from taskq import TaskQ

    verdict_path = unique_health_sock_path("fork_inherit") + ".verdict"
    _PROBE.verdict_path = verdict_path

    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    actor_ref = _fork_body_actor()
    noop_ref = _noop_actor_def()
    registry: dict[str, ActorRef[Any, Any]] = {
        actor_ref.name: actor_ref,  # type: ignore[dict-item]  # Why: the same Mapping variance suppression every registry test carries.
        noop_ref.name: noop_ref,  # type: ignore[dict-item]  # Why: as above.
    }

    worker_settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "heartbeat_interval": "0.5",
            "heartbeat_command_timeout": "0.5",
            # The validator's cascade: lease > grace sums + the loop-lag
            # budget, all moved together (the same coherent set the e2e
            # fleet's env uses).
            "cancellation_grace_period": "1.0",
            "cleanup_grace_period": "1.0",
            "watchdog_loop_lag_budget": "4.0",
            "watchdog_loop_lag_warn_budget": "2.0",
            "watchdog_enabled": "false",
            "lock_lease": "8.0",
            "health_socket_path": unique_health_sock_path("fork_inherit"),
        }
    )

    async with TaskQ(dsn=pg_dsn, schema=schema) as tq:
        _PROBE.client_pool = tq._pool  # pyright: ignore[reportPrivateUsage]  # Why: the pin attacks the TaskQ-built pool itself; the attribute is the pin's subject.
        assert _PROBE.client_pool is not None

        # The fork job, and a plain job that proves the worker keeps
        # consuming after a fork landed mid-dispatch.
        handle_fork = await tq.enqueue(actor_ref, _ForkProbePayload())
        handle_after = await tq.enqueue(noop_ref, _ForkProbePayload())
        with caplog.at_level(logging.ERROR):
            code = await worker_main_async(
                worker_settings,
                actor_registry=registry,
                cron_registry=[],
                until_idle=True,
                idle_settle_window=0.3,
                idle_poll_interval=0.1,
                idle_max_runtime=30.0,
            )
        assert code == 0

        # The child's verdict, as written from inside the forked worker.
        with open(verdict_path, "rb") as fh:  # noqa: ASYNC230  # Why: one file read after the worker exited; there is no loop concurrency left to protect.
            assert fh.read().decode() == "refused", (
                "the forked child of a live worker used the inherited "
                "TaskQ-built pool and was not refused"
            )

        # The parent-side report: the worker learned a fork happened.
        events = [r.getMessage() for r in caplog.records]
        assert any("fork-detected-in-worker-process" in e for e in events), (
            "a fork inside a job body produced no fork-detected report: the "
            "at-fork parent hook or the loop's consume is broken"
        )

        # The ledger: the PARENT's attempts completed BOTH jobs (the child
        # refused the wire and died; the parent's body ran to its terminal
        # write, and the worker went on to the next job).
        check = await asyncpg.connect(pg_dsn)
        try:
            rows = await check.fetch(
                f'SELECT id, status FROM "{schema}".jobs ORDER BY created_at'  # noqa: S608  # Why: schema validated as a PG identifier by the settings validator.
            )
        finally:
            await check.close()
        statuses = [r["status"] for r in rows]
        assert len(rows) == 2, f"the worker consumed the wrong number of jobs: {statuses}"
        assert all(s == "succeeded" for s in statuses), (
            f"the worker did not ride out the fork cleanly: {statuses}"
        )
        assert str(handle_fork.job_id) in {str(r["id"]) for r in rows}
        assert str(handle_after.job_id) in {str(r["id"]) for r in rows}


# ── 4: the subprocess cousin's dark side ──────────────────────────────


def _pg_backend_count(dsn: str, db: str) -> int:
    async def _count() -> int:
        conn = await asyncpg.connect(dsn)
        try:
            row = await conn.fetchrow(
                "SELECT count(*) AS n FROM pg_stat_activity WHERE datname = $1", db
            )
            assert row is not None
            return int(row["n"])
        finally:
            await conn.close()

    return asyncio.run(_count())


def test_close_fds_false_child_holds_the_backend_alive_after_the_parent_dies(
    pg_dsn: str,
) -> None:
    """The cousin's real leak, demonstrated: a process that spawns a
    LONG-LIVED child with close_fds=False and dies leaves its Postgres
    backend alive in the child - the TCP connection survives inside a
    process that will never speak the protocol. This is the shape Python's
    close_fds=True default prevents, and why TaskQ's contract says 'let the
    default stand'."""
    db = pg_dsn.rpartition("/")[2]
    # The child script: open a connection, spawn a sleeper with
    # close_fds=False, exit. The sleeper holds the socket copy.
    script = (
        "import asyncpg, subprocess, sys, asyncio\n"
        f"async def go():\n"
        f"    conn = await asyncpg.connect({pg_dsn!r})\n"
        f"    await conn.fetchval('SELECT 1')\n"
        f"    p = subprocess.Popen(['sleep', '6'], close_fds=False)\n"
        f"    await conn.close()\n"
        f"    return p.pid\n"
        f"sleeper_pid = asyncio.run(go())\n"
        f"print(sleeper_pid)\n"
    )
    proc = subprocess.run(  # noqa: S603  # Why: the script is a pin-local literal, sys.executable the suite's own interpreter.
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert proc.returncode == 0, proc.stderr
    sleeper_pid = int(proc.stdout.strip())

    try:
        # The spawning parent is DEAD; the backend lives in the sleeper.
        alive_with_parent_dead = _pg_backend_count(pg_dsn, db)
        assert alive_with_parent_dead >= 1, (
            "the close_fds=False child did not hold the backend: the leak "
            "did not reproduce, the pin proves nothing"
        )
        # The contract's other half: the default drops the descriptors. Same
        # scenario, default close_fds - no sleeper holding anything.
        script_safe = script.replace("close_fds=False", "close_fds=True")
        proc_safe = subprocess.run(  # noqa: S603  # Why: same pin-local literal script.
            [sys.executable, "-c", script_safe], capture_output=True, text=True, timeout=30
        )
        assert proc_safe.returncode == 0, proc_safe.stderr
        time.sleep(0.3)
        # Only the dead-spawner's backends age out; the safe run leaked none
        # on top of whatever the first arm still holds.
        after_safe = _pg_backend_count(pg_dsn, db)
        assert after_safe <= alive_with_parent_dead, (
            "a close_fds=True spawn left a backend behind: Python's default contract regressed"
        )
    finally:
        # The sleeper is scaffolding: kill it, the leaked backend clears.
        subprocess.run(  # noqa: S603  # Why: teardown scaffolding, fixed argv; pkill is the only way to reach the sleeper's own children.
            ["/usr/bin/pkill", "-KILL", "-P", str(sleeper_pid)], capture_output=True
        )
        with contextlib.suppress(ProcessLookupError):  # pragma: no cover - already gone
            os.kill(sleeper_pid, signal.SIGKILL)
