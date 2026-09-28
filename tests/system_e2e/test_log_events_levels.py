"""The event-stream verbosity knob, end to end: real worker subprocesses.

The unit pins (``tests/test_obs_log_events_level.py``) prove the filter's
classification; these scenarios prove the DEPLOYMENT facts a level
setting makes:

- ``warning``: a real failing job's ``job-failed`` line survives on the
  worker's stream while the per-job ``state-change`` lines are gone;
- ``off``: the state-change lines are gone entirely AND the
  ``job_events`` ledger still received every event (the DB ledger is the
  audit trail - the knob suppresses the streaming duplicate only, it
  never touches the ledger);
- ``debug``: the per-tick internals (``poll-cadence``) appear.

Workers are the tier's real subprocesses (``_harness.spawn_worker``),
env-configured like a pod, with a ``log_sink`` so the assertions read
the worker's own JSON stream.
"""

# ruff: noqa: S608  # Why: every query's schema identifier is the fixture-validated module schema; every value is $-bound.

from __future__ import annotations

import asyncio
import json
import pathlib
from typing import TYPE_CHECKING

import asyncpg
import pytest

from tests.system_e2e._harness import (
    _BASE_ENV,  # pyright: ignore[reportPrivateUsage]  # Why: the fleet env is the tier's shared constant; the production entry must run the same pod shape.
    WorkerProc,
    reap,
    wait_for_socket,
)
from tests.system_e2e.actors import SysPayload, sys_always_fails, sys_fast

if TYPE_CHECKING:
    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [
    pytest.mark.integration,
    pytest.mark.system,
    pytest.mark.timeout(300),
]

_TAG = "loglevel"


def spawn_production_worker(
    pg_dsn: str, schema: str, *, tag: str, events_level: str, log_sink: str
) -> WorkerProc:
    """One worker subprocess via the PRODUCTION entry: ``taskq worker``.

    The tier's ``_worker_entry`` is pure glue that deliberately skips
    logging setup (its docstring: the PIPE-buffered default); these
    scenarios assert on the worker's own log STREAM, so the worker must
    be the production entry whose bootstrap wires
    ``settings.log_events_level`` through ``setup_logging`` — the same
    path the ``taskq worker`` CLI command runs, env-configured like a
    pod. The log_sink is a FILE (never a PIPE), so a chatty level cannot
    wedge the worker's logging write.
    """
    import os
    import subprocess
    import sys

    from taskq.testing.health import unique_health_sock_path

    sock_path = unique_health_sock_path(f"logev-{tag}")
    env = {**os.environ, **_BASE_ENV}
    env.update(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_LOG_EVENTS_LEVEL": events_level,
            "TASKQ_HEALTH_SOCKET_PATH": sock_path,
        }
    )
    proc = subprocess.Popen(  # Why: fixed argv, no shell, binary is this interpreter, module is project-owned; no untrusted input.
        [
            sys.executable,
            "-m",
            "taskq",
            "worker",
            "--actors",
            "tests.system_e2e.actors:ACTORS",
        ],
        env=env,
        cwd=os.environ.get("TASKQ_REPO_ROOT", os.getcwd()),
        stdout=open(log_sink, "wb"),  # noqa: SIM115  # Why: the child owns the descriptor; reaped at process exit.
        stderr=subprocess.STDOUT,
    )
    return WorkerProc(proc=proc, sock_path=sock_path, log_path=log_sink)


async def _wait_terminal(conn: asyncpg.Connection, schema: str, tag: str) -> None:
    """Block until every job tagged ``tag`` reached a terminal status."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 60.0
    while True:
        n = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE tags @> $1::text[] '
            "AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')",
            [tag],
        )
        if n == 0:
            return
        if loop.time() > deadline:
            raise TimeoutError(f"tagged jobs never settled: {n} non-terminal")
        await asyncio.sleep(0.1)


def _log_lines(log_path: str) -> list[dict[str, object]]:
    """Parse the worker's JSON log stream (tolerating non-JSON teardown noise)."""
    out: list[dict[str, object]] = []
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _event_names(log_path: str) -> set[str]:
    return {str(e.get("event")) for e in _log_lines(log_path)}


async def test_warning_level_keeps_failures_drops_state_changes(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    tmp_path: pathlib.Path,
) -> None:
    """A real failure through a real worker: the failed-job line survives,
    the state-change lines are gone."""
    schema = module_pg_schema.schema_name
    log_sink = str(tmp_path / "worker-warning.log")
    worker: WorkerProc | None = None
    try:
        worker = spawn_production_worker(
            pg_dsn,
            schema,
            tag=f"{_TAG}-warn",
            events_level="warning",
            log_sink=log_sink,
        )
        wait_for_socket(worker.sock_path, worker.proc)

        await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])
        await sys_client.enqueue(sys_always_fails, SysPayload(), tags=[_TAG])

        conn = await asyncpg.connect(pg_dsn)
        try:
            await _wait_terminal(conn, schema, _TAG)
        finally:
            await conn.close()

        names = _event_names(log_sink)
        # The anomaly stream survives: the real failure's ERROR line is on
        # the worker's stream.
        assert "job-failed" in names, (
            f"the failure stream was suppressed at warning: {sorted(names)[:30]}"
        )
        # The happy path's per-job duplicate is gone.
        assert "state-change" not in names, (
            f"state-change lines survived at warning: {sorted(names)[:30]}"
        )
    finally:
        if worker is not None:
            reap(worker)


async def test_off_level_keeps_the_ledger_complete(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    tmp_path: pathlib.Path,
) -> None:
    """With off, the log stream drops the state-change lines but the
    job_events ledger still received every event - the durable audit
    trail is untouchable, the knob suppresses the duplicate only."""
    schema = module_pg_schema.schema_name
    log_sink = str(tmp_path / "worker-off.log")
    worker: WorkerProc | None = None
    conn = await asyncpg.connect(pg_dsn)
    try:
        worker = spawn_production_worker(
            pg_dsn,
            schema,
            tag=f"{_TAG}-off",
            events_level="off",
            log_sink=log_sink,
        )
        wait_for_socket(worker.sock_path, worker.proc)

        ok = await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])
        bad = await sys_client.enqueue(sys_always_fails, SysPayload(), tags=[_TAG])

        await _wait_terminal(conn, schema, _TAG)

        names = _event_names(log_sink)
        assert "state-change" not in names, (
            f"state-change lines survived at off: {sorted(names)[:30]}"
        )
        # off never blinds the operator to failures.
        assert "job-failed" in names, (
            f"WARNING-and-above anomalies must survive off: {sorted(names)[:30]}"
        )

        # THE LEDGER PIN: every job event is in job_events - the
        # state_change rows the suppressed log lines duplicated are all
        # there, for the succeeded job and the failed one alike.
        for handle in (ok, bad):
            kinds = {
                r["kind"]
                for r in await conn.fetch(
                    f'SELECT kind FROM "{schema}".job_events WHERE job_id = $1', handle.job_id
                )
            }
            assert "state_change" in kinds, (
                f"the ledger lost the state_change events for {handle.job_id}: {sorted(kinds)}"
            )
    finally:
        await conn.close()
        if worker is not None:
            reap(worker)


async def test_debug_level_adds_the_internals(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    tmp_path: pathlib.Path,
) -> None:
    """debug ADDS the per-tick internals: the producer loop's
    poll-cadence trace appears (it exists at no other level)."""
    schema = module_pg_schema.schema_name
    log_sink = str(tmp_path / "worker-debug.log")
    worker: WorkerProc | None = None
    try:
        worker = spawn_production_worker(
            pg_dsn,
            schema,
            tag=f"{_TAG}-debug",
            events_level="debug",
            log_sink=log_sink,
        )
        wait_for_socket(worker.sock_path, worker.proc)

        await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _wait_terminal(conn, schema, _TAG)
        finally:
            await conn.close()

        names = _event_names(log_sink)
        assert "poll-cadence" in names, (
            f"the debug-only internals were absent at debug: {sorted(names)[:30]}"
        )
    finally:
        if worker is not None:
            reap(worker)
