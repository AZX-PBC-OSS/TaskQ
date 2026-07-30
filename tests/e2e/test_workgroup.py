"""Workgroup supervisor e2e - real supervised worker processes on the host.

Scenario:
a ``taskq workgroup start`` supervisor subprocess on the host manages two
``taskq worker`` child processes, both consuming the module's ``e2e`` queue
under the module schema. The test proves the three supervision semantics
that define the feature:

1. Spawn + register: both children register in ``{schema}.workers`` with
   the config's ``name`` as ``worker_label`` and the supervisor-generated
   UUIDv7 as ``workgroup_instance`` (run.py ``register_worker``), then
   heartbeat (fresh ``last_seen_at`` after ``started_at``).
2. Process: the supervised fleet dispatches and completes enqueued jobs
   (ground truth: ``e2e_effects`` rows plus ``JobHandle.wait``).
3. Respawn: SIGKILLing one child's pid (read from its ``workers`` row) is
   detected by the supervisor's liveness monitor (0.5 s tick), which
   restarts the child after ``supervisor.backoff_initial``; the fleet
   keeps processing afterwards.

Why host subprocesses instead of worker containers: the supervisor itself
spawning and reaping children IS the thing under test; it launches them
via ``sys.executable -m taskq worker ...`` (workgroup.py ``_spawn_child``)
on the same machine. Supervisor and children therefore use the host
endpoints (``e2e_schema.host_dsn``, Dragonfly host URL + module logical
DB), not the in-network aliases the container fixtures use. The config
format has no per-worker env support; children inherit the supervisor's
environment, so the fixture exports the standard e2e timing knobs (same
dict as conftest ``worker_env``) with the two endpoints retargeted.

KNOWN BLOCKER (pre-existing defect, not introduced by this module):
``src/taskq/__main__.py`` does not exist, so the supervisor's hardcoded
``python -m taskq worker`` child spawn fails with "No module named
taskq.__main__" (proven 2026-07-28 by running the console script against
a probe config: every child exits 1 and the supervisor retries with
backoff until the burst limit stops it). The unit suite cannot catch this
because ``tests/test_workgroup.py`` stubs ``asyncio.create_subprocess_exec``.
This module is skipped until ``src/taskq/__main__.py`` exists (a 3-line
module delegating to ``taskq.cli:main``); the skip self-clears once added.

Actors and DI: the CLI worker path (``taskq worker --actors module:attr``,
cli.py) passes no ``di_registry`` to ``worker_main``; the bootstrap
registers the worker's own pool as the default LOOP-scope ``asyncpg.Pool``
provider (_bootstrap.py) and validates actor dependencies against that
registry. Pool-only actors work unchanged; actors with custom providers
(``enrich_order`` -> ``FakeHttpClient``) would fail DI ``validate()`` and
are excluded from ``tests/e2e/host_actors.py``. Supervision semantics, not
DI, is the purpose of this module (test_di.py owns DI via the container).

Shutdown semantics (verified against workgroups.md and a live probe): on
SIGTERM the supervisor forwards SIGTERM to every child, waits up to
``supervisor.shutdown_grace`` (5 s in this config), then SIGKILLs any
survivors, so terminating the supervisor reaps the whole fleet. Teardown
still escalates to SIGKILL on the supervisor itself after 15 s as a
backstop, and cancels the stdout drain task after the process exits.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import os
import signal
import sys
from collections import deque
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import pytest
import pytest_asyncio

from ._assertions import fetch_effects, poll_until, wait_all
from .actors import WelcomeEmailPayload, send_welcome_email

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2EDragonfly, E2ESchema

# Pre-existing defect gate: workgroup._spawn_child runs
# `sys.executable -m taskq worker`, which requires taskq/__main__.py.
_HAS_TASKQ_MAIN = importlib.util.find_spec("taskq.__main__") is not None

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.timeout(900),
    pytest.mark.skipif(
        not _HAS_TASKQ_MAIN,
        reason=(
            "taskq.__main__ missing: the workgroup supervisor spawns children via "
            "`python -m taskq worker` (workgroup.py _spawn_child), which exits 1 until "
            "src/taskq/__main__.py delegates to taskq.cli:main"
        ),
    ),
]

_REPO_ROOT = Path(__file__).resolve().parents[2]

_WORKER_LABELS = ("wg-alpha", "wg-beta")
_READINESS_TIMEOUT_S = 45.0
_RESPAWN_TIMEOUT_S = 45.0
_SUPERVISOR_STOP_TIMEOUT_S = 15.0
_LOG_TAIL_LINES = 80

_WORKGROUP_TOML = f"""\
actors = "tests.e2e.host_actors:ACTORS"

[supervisor]
shutdown_grace = 5.0
backoff_initial = 0.2
backoff_max = 1.0
backoff_factor = 2.0
burst_limit = 8
burst_window = 30.0

[[workers]]
name = "{_WORKER_LABELS[0]}"
queues = ["e2e"]
poll_interval = 0.2
max_concurrency = 4

[[workers]]
name = "{_WORKER_LABELS[1]}"
queues = ["e2e"]
poll_interval = 0.2
max_concurrency = 4
"""


class WorkgroupSupervisor(NamedTuple):
    """Running supervisor subprocess plus its captured combined output."""

    process: asyncio.subprocess.Process
    log_lines: deque[str]


async def _drain_stdout(stream: asyncio.StreamReader | None, sink: deque[str]) -> None:
    """Forward subprocess output lines into *sink* until EOF.

    Draining is load-bearing: an undrained PIPE buffer would eventually
    block the supervisor's writes. The deque is bounded so a chatty child
    fleet cannot grow memory unboundedly.
    """
    if stream is None:
        return
    while True:
        line = await stream.readline()
        if not line:
            return
        sink.append(line.decode("utf-8", errors="replace").rstrip())


def _tail(lines: deque[str], count: int = _LOG_TAIL_LINES) -> str:
    """Last *count* captured supervisor output lines for failure messages."""
    tail = list(lines)[-count:]
    return "\n".join(tail) if tail else "<no supervisor output captured>"


def _supervisor_env(e2e_schema: E2ESchema, e2e_dragonfly: E2EDragonfly) -> dict[str, str]:
    """Child-inherited environment for the supervisor subprocess.

    Starts from the module's standard worker env (identical timing knobs
    to the container workers) and retargets the two endpoints to the host:
    host PG DSN and host Dragonfly URL with the module's logical DB, since
    supervisor and children run outside the Docker network. PYTHONPATH
    gains the repo root so children can import ``tests.e2e.host_actors``.
    """
    env = dict(os.environ)
    env.update(e2e_schema.worker_env)
    env["TASKQ_PG_DSN"] = e2e_schema.host_dsn
    env["TASKQ_REDIS_URL"] = f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}"
    pythonpath = [str(_REPO_ROOT)]
    existing = env.get("PYTHONPATH")
    if existing:
        pythonpath.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath)
    return env


@pytest_asyncio.fixture
async def workgroup_supervisor(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    e2e_schema: E2ESchema,
    e2e_dragonfly: E2EDragonfly,
    e2e_pg_pool: asyncpg.Pool,
) -> AsyncIterator[WorkgroupSupervisor]:
    """Function-scoped supervisor subprocess managing two child workers.

    Writes the workgroup TOML to ``tmp_path``, spawns ``taskq workgroup
    start`` (the console script next to ``sys.executable``; there is no
    ``taskq.__main__``), and gates readiness on BOTH labels showing a
    fresh post-register heartbeat (``last_seen_at > started_at``) with a
    non-NULL ``workgroup_instance`` - the same post-register proof as the
    container fixtures, so a child stuck between register and the first
    heartbeat can never satisfy the gate. A prematurely exited supervisor
    fails fast; on timeout the captured output is dumped into the failure.

    Teardown SIGTERMs the supervisor, which forwards SIGTERM to all
    children and reaps them within ``shutdown_grace`` (verified against
    the supervisor's own shutdown path); after 15 s it escalates to
    SIGKILL on the supervisor as a backstop.
    """
    taskq_bin = Path(sys.executable).with_name("taskq")
    if not taskq_bin.exists():
        msg = f"taskq console script not found next to sys.executable: {taskq_bin}"
        raise RuntimeError(msg)

    config_path = tmp_path / "workgroup.toml"
    config_path.write_text(_WORKGROUP_TOML, encoding="utf-8")

    process = await asyncio.create_subprocess_exec(
        str(taskq_bin),
        "workgroup",
        "start",
        str(config_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=_supervisor_env(e2e_schema, e2e_dragonfly),
        cwd=str(_REPO_ROOT),
    )
    log_lines: deque[str] = deque(maxlen=2000)
    drain_task = asyncio.create_task(_drain_stdout(process.stdout, log_lines))

    async def _fleet_ready() -> bool:
        if process.returncode is not None:
            msg = (
                f"workgroup supervisor exited early (rc={process.returncode}) "
                f"during readiness\n{_tail(log_lines)}"
            )
            raise RuntimeError(msg)
        count = await e2e_pg_pool.fetchval(
            f"""
            SELECT count(DISTINCT worker_label) FROM "{e2e_schema.schema_name}".workers
            WHERE worker_label = ANY($1::text[])
              AND workgroup_instance IS NOT NULL
              AND last_seen_at > now() - interval '10 seconds'
              AND last_seen_at > started_at
            """,
            list(_WORKER_LABELS),
        )
        return count == len(_WORKER_LABELS)

    try:
        try:
            await poll_until(
                _fleet_ready,
                timeout=_READINESS_TIMEOUT_S,
                description=(
                    f"{len(_WORKER_LABELS)} workgroup child heartbeats in "
                    f"{e2e_schema.schema_name}.workers"
                ),
            )
        except TimeoutError:
            msg = (
                "workgroup supervisor failed readiness gate: children did not "
                f"register+heartbeat within {_READINESS_TIMEOUT_S}s\n{_tail(log_lines)}"
            )
            raise RuntimeError(msg) from None
        yield WorkgroupSupervisor(process=process, log_lines=log_lines)
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=_SUPERVISOR_STOP_TIMEOUT_S)
            except TimeoutError:
                process.kill()
                await process.wait()
        drain_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await drain_task
        if request.config.option.verbose >= 2:
            print(f"--- workgroup supervisor output ---\n{_tail(log_lines, 2000)}")


async def test_workgroup_supervisor_spawns_and_restarts_workers(
    e2e_client: TaskQ,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    workgroup_supervisor: WorkgroupSupervisor,
    run_id: str,
) -> None:
    """Spawn, process, respawn: the three supervision semantics in one flow.

    (a) Process: four jobs all succeed through the supervised fleet; four
    "send" effects over exactly the enqueued job_ids prove exactly-once
    dispatch across the two children (a duplicate execution would surface
    as a fifth row or a repeated job_id).

    (b) Respawn: SIGKILL the current ``wg-alpha`` child (pid read from its
    workers row, so the signal provably hits the supervised process); the
    supervisor's liveness monitor must respawn it, observable as a NEW
    workers row (same label, different worker id) with a fresh
    post-register heartbeat. The SIGKILLed child's stale row can never
    satisfy the freshness window.

    (c) Fleet survives: a follow-up job enqueued after the respawn still
    succeeds end to end.
    """
    handles = [
        await e2e_client.enqueue(
            send_welcome_email,
            WelcomeEmailPayload(
                run_id=run_id,
                user_id=f"wg-{i:02d}",
                email=f"wg-{i:02d}@example.com",
            ),
        )
        for i in range(4)
    ]
    await wait_all(handles, timeout=90)

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="send")
    assert len(rows) == 4
    assert {row["job_id"] for row in rows} == {handle.job_id for handle in handles}

    victim = await e2e_pg_pool.fetchrow(
        f"""
        SELECT id, pid FROM "{e2e_schema.schema_name}".workers
        WHERE worker_label = $1
          AND workgroup_instance IS NOT NULL
          AND last_seen_at > now() - interval '10 seconds'
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        _WORKER_LABELS[0],
    )
    assert victim is not None, f"no fresh workers row for label {_WORKER_LABELS[0]!r}"
    victim_id = victim["id"]
    victim_pid = int(victim["pid"])
    assert 0 < victim_pid != os.getpid()
    with contextlib.suppress(ProcessLookupError):
        # An already-exited child is equivalent for the respawn proof below.
        os.kill(victim_pid, signal.SIGKILL)

    async def _respawned() -> bool:
        count = await e2e_pg_pool.fetchval(
            f"""
            SELECT count(*) FROM "{e2e_schema.schema_name}".workers
            WHERE worker_label = $1
              AND id <> $2
              AND last_seen_at > now() - interval '10 seconds'
              AND last_seen_at > started_at
            """,
            _WORKER_LABELS[0],
            victim_id,
        )
        return count >= 1

    await poll_until(
        _respawned,
        timeout=_RESPAWN_TIMEOUT_S,
        description=(
            f"respawn of {_WORKER_LABELS[0]!r} (new worker id, fresh heartbeat) "
            f"in {e2e_schema.schema_name}.workers"
        ),
    )

    follow_up = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(
            run_id=run_id,
            user_id="wg-followup",
            email="wg-followup@example.com",
        ),
    )
    await follow_up.wait(timeout=60)

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="send")
    assert follow_up.job_id in {row["job_id"] for row in rows}
