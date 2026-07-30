"""E2E suite fixtures: real worker containers against real PG + Dragonfly.

Topology:

- Session: one Docker network, one PG container (alias ``pg``), one Dragonfly
  container (alias ``dragonfly``), one worker image built from the fresh wheel.
- Module: a unique PG schema (migrated + ``e2e_effects`` scratch table), a
  unique Dragonfly logical DB, an asyncpg pool on the host DSN, one worker
  container, and an open ``TaskQ`` client on the host DSN.
- Function: ``run_id`` correlation id and the autouse ``clean_e2e_state``
  isolation reset (idle gate → FK-safe DELETEs → FLUSHDB).

The test process is a pure client: all dispatch happens inside the worker
containers; assertions go through handles, the module pool, and effects rows.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import re
import shutil
import subprocess
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple
from uuid import uuid4

import pytest
import pytest_asyncio

from ._assertions import poll_until, wait_for_worker_ready

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    from taskq import TaskQ

_REPO_ROOT = Path(__file__).resolve().parents[2]

_PG_IMAGE = "postgres:18-alpine"
_PG_USER = "taskq"
_PG_PASSWORD = "taskq"
_PG_DB = "taskq"
_DRAGONFLY_IMAGE = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"
_DRAGONFLY_DBNUM = 128
_WORKER_IMAGE_NAME = "taskq-e2e-worker"
_IN_NETWORK_PG_DSN = f"postgresql://{_PG_USER}:{_PG_PASSWORD}@pg:5432/{_PG_DB}"
_IN_NETWORK_DRAGONFLY_URL = "redis://dragonfly:6379"

_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Scratch table asserted on as ground truth for invocation, fan-out,
# rate-limit spread, and double-execution absence. Created per module schema
# by the e2e_schema fixture — NOT by TaskQ migrations.
_E2E_EFFECTS_DDL = """
CREATE TABLE "{schema}".e2e_effects (
    seq      BIGSERIAL PRIMARY KEY,
    at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor    TEXT NOT NULL,
    job_id   UUID NOT NULL,
    attempt  INT NOT NULL,
    kind     TEXT NOT NULL,
    detail   JSONB NOT NULL DEFAULT '{{}}'
);
"""

# FK-safe DELETE order for the per-test reset, mirroring truncate_schema's
# list. DELETE (ROW EXCLUSIVE) does not deadlock with the worker's
# FOR UPDATE SKIP LOCKED dispatch the way TRUNCATE (ACCESS EXCLUSIVE) would.
# job_events and job_attempts cascade from jobs; job_attempts_archive cascades
# from jobs_archive. workers / actor_config / queues stay — they hold worker
# registration/config, not per-test job state.
_DELETE_ORDER: tuple[str, ...] = (
    "reservation_slots",
    "rate_limit_window_entries",
    "rate_limit_buckets",
    "cron_schedules",
    "jobs_archive",
    "jobs",
    "batches",
    "e2e_effects",
)


class E2EPg(NamedTuple):
    """Session PG endpoints: host-mapped (test process) and in-network (workers)."""

    host_dsn: str
    network_dsn: str


class E2EDragonfly(NamedTuple):
    """Session Dragonfly endpoints: base URLs without a logical-DB suffix."""

    host_url: str
    network_url: str


class E2ESchema(NamedTuple):
    """Per-module isolation unit: PG schema + Dragonfly logical DB + worker env."""

    schema_name: str
    host_dsn: str
    worker_env: dict[str, str]
    redis_db: int


class E2EWorker(NamedTuple):
    """Running worker container bound to a module schema."""

    container: DockerContainer
    schema: str


# ── Session: network ──────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def e2e_network() -> Iterator[Network]:
    """One Docker network per test process (pid-suffixed so parallel
    validations never collide), shared by the PG, Dragonfly, and worker
    containers."""
    from testcontainers.core.network import Network

    network = Network()
    network.name = f"taskq-e2e-net-{os.getpid()}"
    with network:
        yield network


# ── Session: Postgres ─────────────────────────────────────────────────────


async def _probe_pg(dsn: str, *, attempts: int = 30, interval: float = 0.5) -> None:
    """Readiness probe: retry ``SELECT 1`` over asyncpg until PG accepts
    connections (belt-and-braces on top of the testcontainers wait strategy)."""
    import asyncpg

    last_exc: Exception | None = None
    for _ in range(attempts):
        try:
            conn = await asyncpg.connect(dsn)
        except (OSError, asyncpg.PostgresError) as exc:
            last_exc = exc
            await asyncio.sleep(interval)
            continue
        try:
            await conn.fetchval("SELECT 1")
        finally:
            await conn.close()
        return
    msg = f"PG probe failed after {attempts} attempts ({dsn})"
    raise RuntimeError(msg) from last_exc


@pytest.fixture(scope="session")
def e2e_pg(e2e_network: Network) -> Iterator[E2EPg]:
    """Session PG container on the shared network (alias ``pg`` for workers),
    probed with asyncpg before yielding both the host and in-network DSNs."""
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(
        image=_PG_IMAGE,
        username=_PG_USER,
        password=_PG_PASSWORD,
        dbname=_PG_DB,
        # Same headroom as the main suite's pg_container (tests/conftest.py):
        # per-module worker containers plus their pools and the test-side
        # asyncpg pools would otherwise approach PG's default of 100.
        command="-c max_connections=1000",
    )
    container.with_network(e2e_network).with_network_aliases("pg")
    with container:
        host_dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        # Invariant: asyncio.run is only safe here because this sync session
        # fixture is resolved before any event loop exists. No async fixture
        # may lazily request this fixture via request.getfixturevalue — that
        # would execute this body inside an already-running loop and raise.
        asyncio.run(_probe_pg(host_dsn))
        yield E2EPg(host_dsn=host_dsn, network_dsn=_IN_NETWORK_PG_DSN)


# ── Session: Dragonfly ────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def e2e_dragonfly(e2e_network: Network) -> Iterator[E2EDragonfly]:
    """Session Dragonfly container (Redis-compatible) on the shared network
    (alias ``dragonfly``), started with enough logical DBs for one per test
    module, PING-probed before yielding host and in-network base URLs."""
    import warnings

    from testcontainers.redis import RedisContainer

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*wait_container_is_ready.*",
            category=DeprecationWarning,
            module="testcontainers.redis",
        )
        container = RedisContainer(image=_DRAGONFLY_IMAGE).with_command(
            f"--dbnum {_DRAGONFLY_DBNUM}"
        )
        container.with_network(e2e_network).with_network_aliases("dragonfly")
        with container:
            client = container.get_client()
            try:
                if not client.ping():
                    msg = "Dragonfly PING probe returned falsy"
                    raise RuntimeError(msg)
            finally:
                client.close()
            host = container.get_container_host_ip()
            port = container.get_exposed_port(6379)
            yield E2EDragonfly(
                host_url=f"redis://{host}:{port}",
                network_url=_IN_NETWORK_DRAGONFLY_URL,
            )


# ── Session: worker image ─────────────────────────────────────────────────


def _build_wheel() -> Path:
    """Build the taskq wheel from current source into a per-process out dir
    and return the freshest ``taskq_py-*-py3-none-any.whl`` in it. Always
    rebuilt so the suite tests the current source as a packaged artifact.

    The out dir is pid-unique (``dist-e2e-<pid>``, gitignored) so two
    concurrent e2e sessions never race on a shared ``dist/`` wheel: one
    process's rebuild could otherwise swap the file under another's image
    build. The ``e2e_worker_image`` session finalizer removes the dir after
    the run; it stays gitignored so a crashed session's leftovers never
    dirty the tree. ``uv build`` also drops a catch-all ``.gitignore`` into
    it.
    """
    uv_bin = shutil.which("uv")
    if uv_bin is None:
        msg = "uv executable not found on PATH (required to build the worker wheel)"
        raise RuntimeError(msg)
    out_dir = _REPO_ROOT / f"dist-e2e-{os.getpid()}"
    try:
        subprocess.run(  # noqa: S603  # Why: static argv, no shell, binary resolved via shutil.which; inputs are not user-controlled.
            [uv_bin, "build", "--wheel", "--out-dir", str(out_dir)],
            cwd=_REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        msg = f"uv build --wheel failed (exit {exc.returncode}):\n{exc.stderr}"
        raise RuntimeError(msg) from exc
    wheels = sorted(
        out_dir.glob("taskq_py-*-py3-none-any.whl"),
        key=lambda p: p.stat().st_mtime,
    )
    if not wheels:
        msg = (
            f"uv build --wheel succeeded but no wheel matched {out_dir}/taskq_py-*-py3-none-any.whl"
        )
        raise RuntimeError(msg)
    return wheels[-1]


@pytest.fixture(scope="session")
def e2e_worker_image() -> Iterator[BuiltImage]:
    """Build the worker image once per session via containerspec.

    Wheel install validates the shipped artifact (packaging, entry points,
    dependency metadata). Content-hash caching makes repeat runs ~0s when
    wheel + actors are unchanged. ``tests/e2e`` is copied LAST so actor
    edits bust only the cheap final layer.

    Teardown removes this process's ``dist-e2e-<pid>/`` wheel dir so runs
    don't accumulate one wheel each (F12). Best-effort: an already-removed
    or unreadable dir never fails the session.
    """
    from containerspec import DockerTarget, ImageSpec

    wheel = _build_wheel()
    spec = (
        # pin_digest=False is deliberate: the suite tracks the floating
        # python:3.12-slim tag so the worker runs on the minimum supported
        # Python; upstream republishes (digest drift) are accepted.
        ImageSpec.from_registry("python:3.12-slim", pin_digest=False)
        .copy(str(wheel), f"/app/dist/{wheel.name}")
        .run_commands(f"pip install --no-cache-dir '/app/dist/{wheel.name}[redis]'")
        .copy(str(_REPO_ROOT / "tests" / "e2e"), "/app/e2e")
        .env({"PYTHONPATH": "/app"})
        .workdir("/app")
        .entrypoint(["python", "-m", "e2e.worker_entry"])
    )
    # Invariant: asyncio.run is only safe here because this sync session
    # fixture is resolved before any event loop exists. No async fixture may
    # lazily request this fixture via request.getfixturevalue — that would
    # execute this body inside an already-running loop and raise.
    built: BuiltImage = asyncio.run(spec.build(DockerTarget(_WORKER_IMAGE_NAME)))
    yield built
    shutil.rmtree(wheel.parent, ignore_errors=True)


# ── Module: schema + Dragonfly DB ─────────────────────────────────────────

# One logical DB per test module; the container runs --dbnum 128 and DB 0 is
# reserved for ad-hoc use. Never reused within a session: sharing a DB would
# let one module's FLUSHDB wipe another's mid-run state.
_redis_db_index = 0


def _next_redis_db() -> int:
    global _redis_db_index
    db = _redis_db_index + 1
    if db >= _DRAGONFLY_DBNUM:
        msg = f"exhausted Dragonfly logical DBs ({_DRAGONFLY_DBNUM})"
        raise RuntimeError(msg)
    _redis_db_index += 1
    return db


def _e2e_schema_name(request: pytest.FixtureRequest) -> str:
    """Unique, lowercase per-module schema name (``te_`` + 10-char hash of
    xdist worker id + module name), mirroring
    ``taskq.testing.fixtures._schema_name_from_module``."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    module = request.module.__name__.replace(".", "_").replace("/", "_").lower()
    name = "te_" + hashlib.md5(f"{worker}_{module}".encode()).hexdigest()[:10]  # noqa: S324  # Why: non-cryptographic hash for test-schema naming; mirrors the existing module-schema pattern.
    if not _SCHEMA_NAME_RE.fullmatch(name):
        msg = f"derived e2e schema name {name!r} is not a valid PG identifier"
        raise RuntimeError(msg)
    return name


def _flushdb(url: str) -> None:
    """FLUSHDB via the synchronous redis client (reliable in any context)."""
    import redis as redis_sync

    with redis_sync.from_url(url, decode_responses=False) as client:
        client.flushdb()


@pytest_asyncio.fixture(scope="module")
async def e2e_schema(
    request: pytest.FixtureRequest,
    e2e_pg: E2EPg,
    e2e_dragonfly: E2EDragonfly,
) -> AsyncIterator[E2ESchema]:
    """Module-scoped PG schema + Dragonfly logical DB.

    Crash-safe setup: DROP SCHEMA IF EXISTS ... CASCADE before migrating, and
    FLUSHDB the module's DB, so stale state from a crashed prior run can never
    leak in. Teardown drops the schema CASCADE.
    """
    import asyncpg

    from taskq.migrate import apply_pending_locked

    schema = _e2e_schema_name(request)
    redis_db = _next_redis_db()

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()

    await apply_pending_locked(e2e_pg.host_dsn, schema=schema)

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(_E2E_EFFECTS_DDL.format(schema=schema))
    finally:
        await conn.close()

    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{redis_db}")

    # Shortened timing knobs so tests stay fast while preserving the
    # validated invariants: lock_lease (3.0) >= 4 * heartbeat_interval (2.0);
    # cancellation (1.0) + cleanup (1.0) grace < min(lock_lease,
    # termination_grace - 5).
    worker_env = {
        "TASKQ_PG_DSN": e2e_pg.network_dsn,
        "TASKQ_REDIS_URL": f"{e2e_dragonfly.network_url}/{redis_db}",
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_QUEUES": "e2e",
        # worker_main never migrates — only the CLI consumes
        # TASKQ_MIGRATE_ON_START, so it is inert on this entry path. The
        # conftest migrates before container start (e2e_schema above); the
        # env var is set only defensively.
        "TASKQ_MIGRATE_ON_START": "false",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "3.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "15.0",
        "TASKQ_SWEEP_INTERVAL": "2.0",
        "TASKQ_QUEUE_DEPTH_INTERVAL": "2.0",
        "TASKQ_RESERVATION_SLOTS_INTERVAL": "2.0",
        "TASKQ_STRANDED_JOBS_INTERVAL": "2.0",
    }

    yield E2ESchema(
        schema_name=schema,
        host_dsn=e2e_pg.host_dsn,
        worker_env=worker_env,
        redis_db=redis_db,
    )

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


# ── Module: asyncpg pool ──────────────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def e2e_pg_pool(e2e_schema: E2ESchema) -> AsyncIterator[asyncpg.Pool]:
    """Module-scoped asyncpg pool on the host DSN. All direct-SQL assertions
    (jobs, job_events, progress columns, effects, workers, batch status) go
    through this pool — the TaskQ client exposes no SQL access."""
    import asyncpg

    pool = await asyncpg.create_pool(
        e2e_schema.host_dsn,
        min_size=1,
        max_size=4,
    )
    assert pool is not None  # asyncpg returns None only for record_class paths
    try:
        yield pool
    finally:
        await pool.close()


# ── Module: worker container ──────────────────────────────────────────────


def _container_logs(container: DockerContainer) -> str:
    """Best-effort stdout/stderr dump for failure messages."""
    import docker.errors
    from testcontainers.core.exceptions import ContainerStartException

    try:
        stdout, stderr = container.get_logs()
    except (ContainerStartException, docker.errors.DockerException) as exc:
        return f"<worker container logs unavailable: {exc!r}>"
    out = stdout.decode(encoding="utf-8", errors="replace")
    err = stderr.decode(encoding="utf-8", errors="replace")
    return f"--- worker stdout ---\n{out}\n--- worker stderr ---\n{err}"


def _stop_container(container: DockerContainer) -> None:
    """Stop/remove the container; tolerate an already-removed container."""
    import docker.errors

    with contextlib.suppress(docker.errors.NotFound):
        container.stop()


def _raise_if_worker_crashed(worker: E2EWorker) -> None:
    """Best-effort liveness check backing ``clean_e2e_state``'s idle gate.

    A container that crashed mid-module makes the gate vacuous: zero
    ``running`` rows is trivially true when no consumer exists, so the reset
    would DELETE underneath the leader's unreaped orphans and the crash
    would surface as a confusing stale-row assertion in the next test. Fail
    loudly at the gate instead. Docker-API errors never block cleanup.
    """
    import docker.errors

    try:
        wrapped = worker.container.get_wrapped_container()
        wrapped.reload()
        status = str(wrapped.status)
    except docker.errors.DockerException:
        return  # best-effort: don't block cleanup on Docker API hiccups
    if status != "running":
        msg = "worker container crashed mid-module; idle gate is vacuous"
        raise RuntimeError(msg)


@pytest_asyncio.fixture(scope="module")
async def e2e_worker(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_pg_pool: asyncpg.Pool,
    e2e_worker_image: BuiltImage,
) -> AsyncIterator[E2EWorker]:
    """Module-scoped worker container on the shared network, gated on a real
    end-to-end readiness signal (fresh heartbeat row in ``{schema}.workers``).

    On readiness timeout the container logs are dumped into the failure
    message. Teardown stops/removes the container even when tests fail.
    """
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_network(e2e_network).with_network_aliases(f"worker-{e2e_schema.schema_name}")
    for key, value in e2e_schema.worker_env.items():
        container.with_env(key, value)

    await asyncio.to_thread(container.start)
    try:
        try:
            await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)
        except TimeoutError:
            logs = _container_logs(container)
            msg = (
                "e2e worker failed readiness gate: no fresh heartbeat in "
                f"{e2e_schema.schema_name}.workers within 30s\n{logs}"
            )
            raise RuntimeError(msg) from None
        yield E2EWorker(container=container, schema=e2e_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


@pytest_asyncio.fixture(scope="module")
async def e2e_worker_serial(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_pg_pool: asyncpg.Pool,
    e2e_worker_image: BuiltImage,
) -> AsyncIterator[E2EWorker]:
    """Module-scoped worker container with ``TASKQ_MAX_CONCURRENCY=1`` for
    serialized dispatch — needed for deterministic abort testing where the
    exact dispatch ordering must be controlled.

    Identical to :func:`e2e_worker` except the worker env is overridden with
    a single concurrency slot so jobs dispatch one at a time.
    """
    from testcontainers.core.container import DockerContainer

    serial_env = {**e2e_schema.worker_env, "TASKQ_MAX_CONCURRENCY": "1"}

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_network(e2e_network).with_network_aliases(
        f"worker-serial-{e2e_schema.schema_name}"
    )
    for key, value in serial_env.items():
        container.with_env(key, value)

    await asyncio.to_thread(container.start)
    try:
        try:
            await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)
        except TimeoutError:
            logs = _container_logs(container)
            msg = (
                "e2e serial worker failed readiness gate: no fresh heartbeat in "
                f"{e2e_schema.schema_name}.workers within 30s\n{logs}"
            )
            raise RuntimeError(msg) from None
        yield E2EWorker(container=container, schema=e2e_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


@pytest_asyncio.fixture(scope="module")
async def e2e_client(e2e_schema: E2ESchema) -> AsyncIterator[TaskQ]:
    """Module-scoped open ``TaskQ`` client on the host DSN + module schema.
    Handles (``JobHandle``/``BatchHandle``) are minted per test; nothing
    module-scoped holds job state."""
    from taskq import TaskQ

    async with TaskQ(dsn=e2e_schema.host_dsn, schema=e2e_schema.schema_name) as client:
        yield client


# ── Function: run id + per-test isolation ─────────────────────────────────


@pytest.fixture
def run_id() -> str:
    """Fresh uuid4 hex correlation id per test. Actors copy it into
    ``e2e_effects.detail->>'run_id'`` so assertions can never
    cross-contaminate even if a reset is missed."""
    return uuid4().hex


@pytest.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """Autouse per-test state reset, in strict order (spec: Isolation contract):

    1. Idle gate — wait for zero ``running`` jobs so the reset never races the
       worker's ``FOR UPDATE SKIP LOCKED`` dispatch.
    2. FK-safe DELETEs (not TRUNCATE) of per-test job state.
    3. FLUSHDB the module's Dragonfly DB — safe only after (1) and (2),
       because the worker holds no in-flight rate-limit/progress state then.
    """
    # Why: a test that requests no e2e infrastructure (e.g. the marker-wiring
    # smoke test) must not boot the container stack through this autouse
    # fixture — skip the reset when its fixture closure is infra-free. Infra
    # fixtures are fetched lazily so the guard runs before any container work.
    if not {
        "e2e_client",
        "e2e_pg_pool",
        "e2e_worker",
        "e2e_worker_serial",
        "e2e_schema",
    }.intersection(request.fixturenames):
        yield
        return

    e2e_schema: E2ESchema = request.getfixturevalue("e2e_schema")
    e2e_pg_pool: asyncpg.Pool = request.getfixturevalue("e2e_pg_pool")
    e2e_dragonfly: E2EDragonfly = request.getfixturevalue("e2e_dragonfly")
    schema = e2e_schema.schema_name

    async def _no_running_jobs() -> bool:
        count = await e2e_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE status = $1',
            "running",
        )
        return count == 0

    await poll_until(
        _no_running_jobs,
        timeout=10.0,
        description=f"idle gate: zero running jobs in {schema}.jobs",
    )

    # F1: the idle gate cannot distinguish "drained" from "dead worker" —
    # verify the container is still alive before the DELETEs. The lazy
    # getfixturevalue runs ONLY when the test requested e2e_worker itself:
    # booting the module worker lazily from inside this running async
    # fixture would drive the sync e2e_worker_image body (asyncio.run)
    # inside an already-running loop and raise. Tests with their own
    # dedicated containers (cron, crash-recovery) skip this check.
    if "e2e_worker" in request.fixturenames:
        worker: E2EWorker = request.getfixturevalue("e2e_worker")
        await asyncio.to_thread(_raise_if_worker_crashed, worker)
    elif "e2e_worker_serial" in request.fixturenames:
        worker: E2EWorker = request.getfixturevalue("e2e_worker_serial")
        await asyncio.to_thread(_raise_if_worker_crashed, worker)

    async with e2e_pg_pool.acquire() as conn:
        for table in _DELETE_ORDER:
            await conn.execute(f'DELETE FROM "{schema}"."{table}"')

    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}")

    yield
