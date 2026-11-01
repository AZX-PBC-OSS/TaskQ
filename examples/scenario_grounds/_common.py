"""Shared wiring for the seven scenario grounds.

Every scenario follows the same shape: it points at a real Postgres (and
a real Redis when a worker joins), builds its own schema from scratch,
runs the story, and prints its evidence. The evidence lines are the
machine-readable half (``EVIDENCE {…}`` — one JSON object per check);
the surrounding lines are the same facts in plain English. The companion
test (``tests/test_scenario_grounds.py``) runs each script and asserts
the row-truth of the printed evidence.

Configuration, all from the environment:

- ``TASKQ_PG_DSN`` — the Postgres the scenarios build their schemas in
  (default: the repository's compose stack on localhost:5432).
- ``TASKQ_REDIS_URL`` — the Redis the worker scenarios use (default:
  the compose stack's redis://localhost:6379/0).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import asyncpg
from pydantic import BaseModel

import taskq.migrate

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The defaults match the repository's own compose stack (docker-compose.yml
#: at the repo root): Postgres on 5432, Redis on 6379.
DEFAULT_PG_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"


def pg_dsn() -> str:
    """The Postgres every scenario builds in (env ``TASKQ_PG_DSN``)."""
    return os.environ.get("TASKQ_PG_DSN", DEFAULT_PG_DSN)


def redis_url() -> str:
    """The Redis the worker scenarios use (env ``TASKQ_REDIS_URL``)."""
    return os.environ.get("TASKQ_REDIS_URL", DEFAULT_REDIS_URL)


def ev(**fields: object) -> None:
    """Print one machine-readable evidence line (the companion test
    parses these). The same facts sit in the plain-English lines around
    it; this line is the one the rows must answer for."""
    print(f"EVIDENCE {json.dumps(fields, default=str, sort_keys=True)}", flush=True)


def say(line: str) -> None:
    """One plain-English line of the story."""
    print(line, flush=True)


async def fresh_schema(dsn: str, schema: str) -> None:
    """Drop and rebuild one scenario schema: empty, migrated, ours.

    Each scenario owns its schema name (``grounds_s1`` … ``grounds_s7``),
    so the scenarios re-run cleanly and never see each other's rows.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
        )  # Why: the schema is the scenario's own constant identifier; nothing user-supplied reaches this string.
        await conn.execute(
            f'CREATE SCHEMA "{schema}"'
        )  # Why: same — the scenario's own constant schema name.
    finally:
        await conn.close()
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")


async def open_pool(dsn: str) -> asyncpg.Pool:
    """A small pool for the scenario's own reads and writes. asyncpg's
    create_pool returns a pool or raises — no None face to guard."""
    return await asyncpg.create_pool(dsn, min_size=1, max_size=8)


def scratch_dir(name: str) -> Path:
    """A scratch directory for the scenario's worker logs (created fresh
    per run; nothing in the repository tree is touched)."""
    base = Path(tempfile.gettempdir()) / f"scenario-grounds-{name}"
    base.mkdir(parents=True, exist_ok=True)
    return base


def worker_base_env(schema: str) -> dict[str, str]:
    """The environment a scenario's worker boots with: the same Postgres
    and Redis the scenario drives, the scenario's schema, and a scratch
    dotenv directory so no stray repository .env leaks in."""
    dotenv = scratch_dir("dotenv")
    dotenv.mkdir(parents=True, exist_ok=True)
    return {
        **os.environ,
        "TASKQ_PG_DSN": pg_dsn(),
        "TASKQ_REDIS_URL": redis_url(),
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_ENVIRONMENT": "development",
        "DOTENV_DIR": str(dotenv),
    }


def worker_argv(actors_ref: str) -> list[str]:
    """The worker command: this interpreter, the package's own entry
    point, the definitions module the worker should carry."""
    return [sys.executable, "-m", "taskq", "worker", "--actors", actors_ref]


class WorkerRun:
    """One worker process a scenario started, with its log file.

    The scenario starts it, waits for the boot to register the worker
    row, drives the story, then stops it (the graceful stop is the
    production shutdown path: drain, then exit)."""

    def __init__(self, proc: asyncio.subprocess.Process, log_path: Path) -> None:
        self.proc = proc
        self.log_path = log_path

    async def stop(self) -> int:
        """The graceful stop (SIGTERM, the production deploy's signal)."""
        if self.proc.returncode is not None:
            return self.proc.returncode
        self.proc.terminate()
        try:
            return await asyncio.wait_for(self.proc.wait(), timeout=20)
        except TimeoutError:
            self.proc.kill()
            await self.proc.wait()
            return -1

    async def wait_for_exit(self, timeout: float) -> int:  # noqa: ASYNC109  # Why: a `timeout` parameter is the point — the repo's established wait-helper shape.
        """Wait for a worker that is expected to exit on its own."""
        try:
            return await asyncio.wait_for(self.proc.wait(), timeout=timeout)
        except TimeoutError:
            self.proc.kill()
            await self.proc.wait()
            raise

    def log_contains(self, needle: str) -> bool:
        """Has the worker's log printed this line yet?"""
        try:
            return needle in self.log_path.read_text(errors="replace")
        except FileNotFoundError:
            return False

    def log_lines_with(self, needle: str) -> list[str]:
        """The log's lines carrying this fragment (the sweep records)."""
        try:
            return [
                line.strip()
                for line in self.log_path.read_text(errors="replace").splitlines()
                if needle in line
            ]
        except FileNotFoundError:
            return []

    def log_text(self) -> str:
        """The whole log, as one string (the refusal text's search)."""
        try:
            return self.log_path.read_text(errors="replace")
        except FileNotFoundError:
            return ""


async def start_worker(
    *,
    schema: str,
    actors_ref: str,
    queues: list[str],
    log_path: Path,
    max_concurrency: int | None = None,
    until_idle: bool = False,
    extra_env: dict[str, str] | None = None,
) -> WorkerRun:
    """Start one worker process against the scenario's schema.

    ``queues`` is the consumed set; the flags are the CLI's own public
    surface (``--max-concurrency``, ``--poll-interval``, ``--until-idle``).
    """
    argv = worker_argv(actors_ref)
    for queue in queues:
        argv.extend(["--queues", queue])
    argv.extend(["--poll-interval", "0.1"])
    if max_concurrency is not None:
        argv.extend(["--max-concurrency", str(max_concurrency)])
    if until_idle:
        argv.extend(["--until-idle", "--idle-settle-window", "1.0"])
    env = worker_base_env(schema)
    if extra_env:
        env.update(extra_env)
    log_file = log_path.open("wb")
    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=str(REPO_ROOT),
        env=env,
        stdout=log_file,
        stderr=log_file,
    )
    return WorkerRun(proc, log_path)


async def wait_for_worker_ready(pool: asyncpg.Pool, schema: str, worker: WorkerRun) -> None:
    """Wait until the worker's boot registered its row (the readiness
    the fleet's own dispatch waits for)."""

    async def registered() -> bool:
        count = await pool.fetchval(f'SELECT count(*) FROM "{schema}".workers')  # noqa: S608  # Why: the schema is the scenario's own constant identifier.
        return int(count) > 0

    await wait_until(registered, timeout=45, what="the worker's boot registration")


async def wait_until(fn: Any, *, timeout: float, what: str, interval: float = 0.2) -> Any:  # noqa: ASYNC109  # Why: a `timeout` parameter is the point — the scenario passes a per-wait budget, not an enclosing asyncio.timeout scope; the repo's established wait-helper shape.
    """Poll until the check answers truthy, or fail with the named
    expectation. Every scenario's wait goes through here so the failure
    message says what never happened."""
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        result = await fn()
        if result:
            return result
        if _time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {what}")
        await asyncio.sleep(interval)


async def flow_root_status(pool: asyncpg.Pool, schema: str, flow_id: str) -> str | None:
    """The run's own status (the root row's)."""
    return await pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema is the scenario's own constant identifier; the id is bound.
        flow_id,
    )


async def wait_for_root(
    pool: asyncpg.Pool,
    schema: str,
    flow_id: str,
    *,
    want: tuple[str, ...],
    timeout: float,  # noqa: ASYNC109  # Why: a `timeout` parameter is the point — the repo's established wait-helper shape.
) -> str:
    """Wait until the run's root row reads one of the wanted states."""

    async def there() -> str | None:
        status = await flow_root_status(pool, schema, flow_id)
        return status if status in want else None

    final = await wait_until(there, timeout=timeout, what=f"the run to reach {want}")
    return str(final)


class TerminalCensus(BaseModel):
    """How a group of runs ended, counted off the root rows."""

    total: int
    succeeded: int
    failed: int
    cancelled: int


async def root_census(pool: asyncpg.Pool, schema: str, flow_ids: list[str]) -> TerminalCensus:
    """Count the runs' root rows by their terminal verdict."""
    statuses = await pool.fetch(
        f'SELECT status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: the schema is the scenario's own constant identifier; the ids are bound.
        flow_ids,
    )
    succeeded = sum(1 for row in statuses if row["status"] == "succeeded")
    failed = sum(1 for row in statuses if row["status"] == "failed")
    cancelled = sum(1 for row in statuses if row["status"] == "cancelled")
    return TerminalCensus(
        total=len(flow_ids), succeeded=succeeded, failed=failed, cancelled=cancelled
    )


async def wait_all_runs_terminal(
    pool: asyncpg.Pool,
    schema: str,
    flow_ids: list[str],
    *,
    worker: WorkerRun | None = None,
    timeout: float = 240,  # noqa: ASYNC109  # Why: a `timeout` parameter is the point — the repo's established wait-helper shape.
) -> TerminalCensus:
    """Wait until every run's root row is terminal (the worker's leader
    derives each root's verdict from its rows every sweep tick)."""
    import time as _time

    deadline = _time.monotonic() + timeout
    while True:
        census = await root_census(pool, schema, flow_ids)
        if census.succeeded + census.failed + census.cancelled == census.total:
            return census
        if _time.monotonic() > deadline:
            raise TimeoutError(f"only {census.succeeded}/{census.total} runs finished in time")
        if worker is not None and worker.proc.returncode is not None:
            raise RuntimeError("the worker exited before the runs finished")
        await asyncio.sleep(0.4)
