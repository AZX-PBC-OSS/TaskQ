"""DocShelf's drill harness.

Every drill runs against REAL Postgres (the adoption sim's own instance,
default ``postgresql://postgres:taskq@localhost:5804/taskq``) with REAL
worker PROCESSES (``taskq worker`` subprocesses — the crash drills
SIGKILL them). The TaskQ machinery is real; only the EXTERNAL systems
are mocked (they live inside the app).

The fixtures:

* ``pg_dsn`` — the module-scoped DSN (the consumer contract: named
  ``pg_dsn``, module scope, exclusive cluster).
* ``schema`` — a per-module schema, migrated pre+post, the app's own
  tables created, dropped at teardown.
* ``pool`` — the module's asyncpg pool on that schema.
* ``workers`` — a factory fixture: spawn/kill/wait worker subprocesses.
* ``admin_app`` — the admin sidecar (the operator faces' capture target).

Drill receipts land in ``.measurements/docshelf/`` when
``DOCSHELF_CAPTURE_DIR`` is set (the repo's evidence convention).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import asyncpg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

#: The adoption sim's PG (NOT testcontainers — the drill's own instance).
DOCSHELF_DSN = os.environ.get("DOCSHELF_PG_DSN", "postgresql://postgres:taskq@localhost:5804/taskq")

CAPTURE_DIR = Path(
    os.environ.get("DOCSHELF_CAPTURE_DIR", str(REPO_ROOT / ".measurements" / "docshelf"))
)

#: The fast-recovery settings the drills run workers under (the crash
#: drills' reclaim latency must fit the test budget; the defaults'
#: 60 s lease would pace every drill at a minute).
FAST_WORKER_ENV: dict[str, str] = {
    "TASKQ_LOCK_LEASE": "20",
    "TASKQ_SWEEP_INTERVAL": "1",
    "TASKQ_HEARTBEAT_INTERVAL": "1",
    "TASKQ_MAX_HEARTBEAT_FAILURES": "1",
    "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.5",
    "TASKQ_CANCELLATION_GRACE_PERIOD": "5",
    "TASKQ_CLEANUP_GRACE_PERIOD": "2",
    "TASKQ_WATCHDOG_ENABLED": "false",
}


def capture(name: str, payload: dict[str, Any]) -> Path:
    """Write one drill's receipt into the evidence directory."""
    path = CAPTURE_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return path


def _module_schema(request: pytest.FixtureRequest) -> str:
    digest = hashlib.sha256(request.module.__name__.encode()).hexdigest()[:10]
    stem = request.module.__name__.replace(".", "_")
    return f"docshelf_{stem[-30:]}_{digest}"


@pytest.fixture(scope="module")
def pg_dsn() -> str:
    """The adoption sim's cluster DSN (module-scoped per the consumer contract)."""
    return DOCSHELF_DSN


@pytest.fixture(scope="module")
async def schema(request: pytest.FixtureRequest) -> AsyncIterator[str]:
    name = _module_schema(request)
    conn = await asyncpg.connect(DOCSHELF_DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{name}"')
    finally:
        await conn.close()
    yield name
    if os.environ.get("DOCSHELF_KEEP_SCHEMA"):
        return  # keep the evidence for the post-mortem
    conn = await asyncpg.connect(DOCSHELF_DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    finally:
        await conn.close()


@pytest.fixture(scope="module")
async def migrated(schema: str) -> str:
    """Apply the library's migrations (pre + post) to the module schema."""
    import taskq.migrate

    await taskq.migrate.apply_pending_locked(DOCSHELF_DSN, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(DOCSHELF_DSN, schema=schema, phase="post")
    return schema


@pytest.fixture(scope="module")
async def pool(migrated: str, schema: str) -> AsyncIterator[asyncpg.Pool]:
    from examples.docshelf import store

    the_pool = await asyncpg.create_pool(DOCSHELF_DSN, min_size=1, max_size=8)
    assert the_pool is not None
    await store.ensure_tables(the_pool, schema)
    yield the_pool
    await the_pool.close()


class WorkerProc:
    """One real ``taskq worker`` subprocess."""

    def __init__(self, proc: subprocess.Popen[bytes], worker_tag: str) -> None:
        self.proc = proc
        self.tag = worker_tag

    def kill9(self) -> None:
        """THE CRASH: SIGKILL — no drain, no shutdown, a corpse on the rows."""
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def alive(self) -> bool:
        return self.proc.poll() is None


class WorkerFactory:
    """Spawns real worker subprocesses against one schema."""

    def __init__(self, schema: str, log_dir: Path) -> None:
        self.schema = schema
        self._log_dir = log_dir
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._spawned: list[WorkerProc] = []

    def spawn(
        self,
        *,
        tag: str | None = None,
        max_concurrency: int = 8,
        queues: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> WorkerProc:
        from examples.docshelf.config import DOCSHELF_QUEUES

        tag = tag or f"w-{secrets.token_hex(4)}"
        env = {
            **os.environ,
            "TASKQ_PG_DSN": DOCSHELF_DSN,
            "TASKQ_SCHEMA_NAME": self.schema,
            "TASKQ_QUEUES": ",".join(queues or DOCSHELF_QUEUES),
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
            # S108 Why: the drill's per-worker health socket; the path is
            # the worker's own tag, unique per spawn.
            # S108 Why: the drill's per-worker health socket (the
            # worker's own tag, unique per spawn).
            "TASKQ_HEALTH_SOCKET_PATH": f"/tmp/docshelf-{tag}.sock",  # noqa: S108
            # S108 Why: an EMPTY dotenv dir hermetically sealing the
            # worker boot from the developer's .env.
            "DOTENV_DIR": "/tmp/docshelf-empty-dotenv",  # noqa: S108
            "PYTHONPATH": str(REPO_ROOT),
            **FAST_WORKER_ENV,
            **(extra_env or {}),
        }
        Path("/tmp/docshelf-empty-dotenv").mkdir(exist_ok=True)  # noqa: S108  # Why: see DOTENV_DIR above — the empty dir, not a file.
        cli = _worker_cli()
        log_path = self._log_dir / f"{tag}.log"
        with log_path.open("wb") as log_file:
            proc = subprocess.Popen(  # noqa: S603  # Why: the CLI path is the repo's own console script, not free input.
                [*cli, "worker", "--actors", "examples.docshelf.worker_entry:ACTORS"],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
                env=env,
            )
        worker = WorkerProc(proc, tag)
        self._spawned.append(worker)
        return worker

    async def spawn_ready(
        self,
        pool: asyncpg.Pool,
        *,
        tag: str | None = None,
        max_concurrency: int = 8,
        queues: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> WorkerProc:
        """Spawn + wait for the boot registration (the workers row live)."""
        worker = self.spawn(
            tag=tag, max_concurrency=max_concurrency, queues=queues, extra_env=extra_env
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            n = await pool.fetchval(
                f'SELECT count(*) FROM "{self.schema}".workers '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
                "WHERE last_seen_at > now() - interval '5 seconds'"
            )
            if int(n or 0) >= 1:
                return worker
            if not worker.alive():
                raise RuntimeError(f"worker {worker.tag} died at boot (see the log dir)")
            await asyncio.sleep(0.2)
        raise RuntimeError("the worker never registered (30 s)")

    def stop_all(self) -> None:
        for worker in self._spawned:
            if worker.alive():
                worker.proc.terminate()
        for worker in self._spawned:
            try:
                worker.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                worker.kill9()


def _worker_cli() -> list[str]:
    """The ``taskq`` console script of the environment running the drills."""
    override = os.environ.get("DOCSHELF_WORKER_BIN")
    if override:
        return [override]
    candidate = Path(sys.executable).parent / "taskq"
    if candidate.exists():
        return [str(candidate)]
    return ["taskq"]


@pytest.fixture(scope="module")
def workers(schema: str, request: pytest.FixtureRequest) -> WorkerFactory:
    # S108 Why: the drill's own log sink (outside the repo; the
    # adoption sim's scratch space).
    factory = WorkerFactory(schema, log_dir=Path("/tmp/opencode/docshelf-worker-logs"))  # noqa: S108  # Why: the drill's own log sink (outside the repo; the adoption sim's scratch space).
    request.addfinalizer(factory.stop_all)
    return factory


# ── the drill helpers ────────────────────────────────────────────────────


async def await_terminal(
    pool: asyncpg.Pool,
    schema: str,
    flow_id: str,
    *,
    timeout_s: float = 240.0,
    poll_s: float = 0.25,
) -> str:
    """Wait for a flow run's TERMINAL (the root row's status; the rows
    are the truth). Raises with the run's stuck-node story on timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        row = await pool.fetchrow(
            f'SELECT status, error_class, error_message FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            uuid.UUID(flow_id),
        )
        if row and row["status"] in ("succeeded", "failed", "cancelled", "crashed", "abandoned"):
            return str(row["status"])
        await asyncio.sleep(poll_s)
    stuck = await pool.fetch(
        f'SELECT step_key, status, attempt FROM "{schema}".jobs '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE metadata->>'flow_id' = $1 AND status NOT IN ('succeeded','failed','cancelled') "
        "ORDER BY step_key LIMIT 20",
        uuid.UUID(flow_id),
    )
    raise RuntimeError(
        f"run {flow_id} never reached terminal in {timeout_s}s; stuck nodes: "
        + ", ".join(f"{r['step_key']}={r['status']}(a{r['attempt']})" for r in stuck)
    )


async def wait_until(
    predicate: Callable[[], Awaitable[object | None]],
    *,
    timeout_s: float = 60.0,
    poll_s: float = 0.2,
    what: str = "condition",
) -> object:
    """Poll until the predicate returns a non-None value."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = await predicate()
        if result is not None:
            return result
        await asyncio.sleep(poll_s)
    raise RuntimeError(f"timed out waiting for {what} ({timeout_s}s)")


def make_docs(n: int, prefix: str = "doc", poison: set[str] | None = None) -> list[Any]:
    """*n* deterministic documents (the mock corpus)."""
    from examples.docshelf.models import DocPayload

    poison = poison or set()
    return [
        DocPayload(
            doc_id=f"{prefix}-{i:04}" if f"{prefix}-{i:04}" not in poison else f"poison-{i:04}",
            content=(
                f"document {i} body text " * 6 + f"unique tail {i} " + "word " * (i % 17 + 4)
            ).strip(),
        )
        for i in range(n)
    ]


@pytest.fixture(scope="module")
def delivery_counter() -> list[int]:
    return [0]


@pytest.fixture
def run_receipt(request: pytest.FixtureRequest):
    """Capture the drill's receipt at the end (call the returned fn)."""
    entries: list[tuple[str, dict[str, Any]]] = []

    def _record(name: str, payload: dict[str, Any]) -> None:
        entries.append((f"{request.node.name}-{name}", payload))

    yield _record
    for name, payload in entries:
        capture(f"{request.module.__name__}/{name}.json", payload)
