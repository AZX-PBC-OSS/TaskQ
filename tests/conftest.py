"""Shared test fixtures.

Container topology: ONE Postgres + ONE Dragonfly shared across ALL xdist
workers of a run (see :mod:`taskq.testing._shared_containers` — the first
session fixture to take the state-dir file lock starts the pair; every other
worker reuses it; it comes down when the last LIVE holder — of this run or of
any concurrent invocation sharing the pair — releases).
Under the old per-worker-session design a ``-n 4`` run booted four Postgres
plus four Dragonfly containers, and that contention is what made heavy PG
tests trip internal timeouts intermittently. Per-worker isolation on the
shared containers is preserved: every module gets its own PG database
(``pg_dsn``) and every Redis consumer its own logical DB.

Tests that need PG are marked ``integration`` so non-integration runs (e.g.
``pytest -m 'not integration'``) skip them entirely.

Module-scoped fixtures (``module_pg_schema``, ``module_redis_url``) provide
per-file isolation — each test file gets its own PG schema and Redis DB.
Function-scoped cleanup fixtures (``clean_pg_conn``, ``clean_jobs_app``,
``clean_redis_url``, ``clean_redis_client``) truncate/drop state before
each test for within-file isolation.

Pytest discovers fixtures imported into a conftest.py.
The fixtures are imported from :mod:`taskq.testing.fixtures`
and re-registered here so they are available to all test modules.
"""

import contextlib
import glob
import os
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

import asyncpg
import pytest
import pytest_asyncio

from taskq.settings import OIDCSettings, SAMLSettings, TaskQSettings
from taskq.testing._shared_containers import shared_service_pair
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    StubActorConfig,
    as_backend,
    default_actor_config,
)
from taskq.testing.assertions import (
    assert_attempt,
    assert_has_event,
    assert_has_otel_event,
    assert_has_span,
    assert_job_status,
    assert_job_terminal,
    assert_transition_sequence,
    wait_for,
    wait_for_job_status,
    wait_for_leader,
)
from taskq.testing.fixtures import (
    RUN_TOKEN_ENV_VAR,
    JobsApp,
    ModulePgSchema,
    actor_runner,
    backend_pair,
    clean_jobs_app,
    clean_pg_conn,
    clean_redis_client,
    clean_redis_url,
    jobs_app,
    killable_redis_container,
    memory_jobs,
    module_jobs_app,
    module_pg_pool,
    module_pg_schema,
    module_redis_url,
    redis_container,
    redis_url,
    run_isolation_token,
    worker_with_running_job,
)
from taskq.testing.health import unique_health_sock_path
from taskq.testing.jobs import (
    error_info,
    make_enqueue_args,
    make_job_row,
)
from taskq.testing.otel import _logging_configured_guard, _otel_enabled_guard
from taskq.testing.pg import (
    DEFAULT_ACTORS,
    create_pending_job,
    create_running_job,
    create_worker,
    create_workered_running_job,
    get_job_triple,
    parse_detail,
    reset_schema,
    seed_actors,
    setup_running_job,
    truncate_schema,
)
from taskq.testing.settings import (
    make_integration_settings,
    make_integration_settings_dict,
)
from taskq.worker.deps import WorkerDeps
from taskq.worker.health import HealthServer

# ── Health-socket isolation ──────────────────────────────────────────────
# WorkerSettings.health_socket_path defaults to the shared production path
# /tmp/taskq_health.sock, and _main starts a real HealthServer. Under xdist,
# two workers inside _main concurrently race on that one filesystem path —
# the loser gets EADDRINUSE (TOCTOU window in create_unix_server's stale-file
# removal), or silently steals the socket from the live winner.
# unique_health_sock_path() mints per-test unique module-scoped paths for
# settings factories; the autouse fixture below redirects any
# HealthServer.start still targeting the shared default, so no test can ever
# bind it regardless of how its settings were built.

_SHARED_DEFAULT_HEALTH_SOCK = "/tmp/taskq_health.sock"  # noqa: S108  # Why: must match the WorkerSettings.health_socket_path default in settings.py; pinned by tests/test_health_socket_isolation.py.


def free_host_port() -> int:
    """An unused localhost TCP port, for pinning a container's host binding.

    Required by any test that stops and restarts a container and keeps
    using its host-mapped DSN: Docker does NOT preserve a Docker-assigned
    ephemeral host port across stop/start — it allocates a fresh one on
    start (measured on Linux: 34252 → 34253), silently invalidating every
    host DSN derived before the restart. An explicitly published port is
    part of the container's declared config and is restored verbatim.

    Bind-and-release: a port free now is almost certainly still free when
    Docker publishes it moments later.
    """
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# unique_health_sock_path is published as taskq.testing.health.unique_health_sock_path
# (imported above for the autouse redirect below). Test modules import it from
# the published path directly; only the redirect shim stays repo-specific.


@pytest.fixture(autouse=True)
def _isolate_health_server_socket(  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """Redirect HealthServer.start away from any path another server may hold.

    Rewrites the shared default path (the WorkerSettings default no test
    should bind) AND any path this fixture already minted — the latter so a
    settings object reused across two start() calls (e.g. a worker-restart
    test) still yields two distinct sockets instead of the second start
    stealing the first server's live socket.
    """
    # Why: e2e runs workers in containers — no in-process HealthServer exists to isolate.
    if "e2e" in request.node.keywords:
        return
    original_start = HealthServer.start
    module = getattr(request.module, "__name__", "unknown")
    module = module.removeprefix("tests.test_").removeprefix("tests.")
    minted: set[str] = set()

    async def _start_isolated(self: HealthServer, deps: WorkerDeps) -> None:
        path = deps.settings.health_socket_path
        if path == _SHARED_DEFAULT_HEALTH_SOCK or path in minted:
            path = unique_health_sock_path(module)
            minted.add(path)
            deps.settings.health_socket_path = path
        await original_start(self, deps)

    monkeypatch.setattr(HealthServer, "start", _start_isolated)


@pytest.fixture(autouse=True)
def _reset_oidc_saml_cached() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner.
    """Reset dotenvmodel cached() singletons for OIDC/SAML settings after each test.

    ``settings.oidc`` and ``settings.saml`` use dotenvmodel's
    ``OIDCSettings.cached()`` / ``SAMLSettings.cached()``, which returns a process-wide
    singleton — the environment is read on first access and the same instance
    returned thereafter. Without this reset, a test that sets
    ``TASKQ_OIDC_*`` / ``TASKQ_SAML_*`` env vars and accesses the property
    would leak the cached instance into subsequent tests, silently giving them
    stale values. ``reset_cached()`` is a no-op when the cache is cold.
    """
    yield
    OIDCSettings.reset_cached()
    SAMLSettings.reset_cached()


@pytest.fixture(scope="session", autouse=True)
def _no_developer_dotfiles(  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[None]:
    """Make the whole suite hermetic w.r.t. the developer's gitignored dotfiles.

    :meth:`TaskQSettings.load` defaults ``override=True``: dotenvmodel's
    cascade (``.env`` → ``.env.local`` → ``.env.{env}`` → ``.env.{env}.local``,
    rooted at ``DOTENV_DIR`` or the CWD) is written INTO ``os.environ``,
    overriding even ``monkeypatch.setenv`` values set earlier in the same
    test. ``.env.example`` tells developers to create exactly such a file with
    ``TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq`` — so on a
    dev machine the ``settings`` fixture below would silently load the
    developer's own DSN and ``pg_conn`` would then run
    ``DROP SCHEMA … CASCADE`` against the developer's database.

    Pointing ``DOTENV_DIR`` at an empty directory makes every ``.load()`` see
    zero dotfiles without touching, moving, or reading the real files. The
    directory must exist: dotenvmodel's ``load_env_files`` raises
    ``FileNotFoundError`` for a missing ``DOTENV_DIR``
    (``dotenvmodel/loading.py``).

    A raw ``pytest.MonkeyPatch()`` instance is used, not the function-scoped
    ``monkeypatch`` fixture (which has no session scope): ``MonkeyPatch`` is
    the sanctioned env seam with correct undo semantics. ``DOTENV_DIR`` is the
    one variable that CANNOT flow through ``TaskQSettings`` — it is the input
    that tells dotenvmodel where to look for dotfiles BEFORE any settings
    object exists. Setting it in the test process also makes subprocess
    children spawned with ``env={**os.environ, ...}`` (e.g. the
    ``taskq ui serve`` and e2e entry scripts, which call
    ``WorkerSettings.load()`` themselves) hermetic for the same reason.
    """
    empty_dir = tmp_path_factory.mktemp("no-dotfiles")
    mp = pytest.MonkeyPatch()
    mp.setenv("DOTENV_DIR", str(empty_dir))
    try:
        yield
    finally:
        mp.undo()


@pytest.fixture(scope="session", autouse=True)
def _sweep_health_sock_files() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Sweep this process's own tq-*-<pid>-*.sock files after the session.

    HealthServer.stop() unlinks under normal completion; this is a backstop
    for abnormal termination. Scoped to this PID only, so it can never touch
    another worker's or session's socket files.
    """
    yield
    for path in glob.glob(f"/tmp/tq-*-{os.getpid()}-*.sock"):  # noqa: S108  # Why: matches unique_health_sock_path's own prefix.
        with contextlib.suppress(OSError):
            os.unlink(path)


@pytest.fixture(scope="session", autouse=True)
def _publish_run_isolation_token(  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[None]:
    """Publish the per-invocation isolation token for this run, once, before
    any naming helper runs (autouse session fixtures instantiate before every
    module/function fixture of the tests they apply to).

    The token feeds every hashed per-module/per-test database and schema name
    (``_module_db_name`` here, ``_schema_name_from_test`` /
    ``_schema_name_from_module`` in :mod:`taskq.testing.fixtures`) via
    :func:`taskq.testing.fixtures.run_isolation_token`. Without it, two
    concurrent bare-``pytest`` invocations of the same module hashed to the
    SAME database on the SAME shared pair — serial runs share
    ``/tmp/pytest-of-<user>`` (the shared-pair state dir) across all
    invocations and the ``worker-or-"master"`` fallback is
    invocation-invariant — and each run's ``DROP DATABASE ... WITH (FORCE)``
    killed the other's live pools mid-test (redteam-reproduced: both runs
    rc=1 on one ``tq_db_*`` name).

    Under xdist the worker id is already invocation-unique (each worker's
    state dir is ``<basetemp>/popen-gwK`` with a per-invocation basetemp), so
    it IS the token and per-worker names are unchanged. Serial runs use the
    invocation-unique basetemp dir name (``pytest-N``) — pytest allocates it
    under a lock, so two overlapping runs can never hold the same one.
    """
    token = os.environ.get("PYTEST_XDIST_WORKER") or tmp_path_factory.getbasetemp().name
    previous = os.environ.get(RUN_TOKEN_ENV_VAR)
    os.environ[RUN_TOKEN_ENV_VAR] = token
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(RUN_TOKEN_ENV_VAR, None)
        else:
            os.environ[RUN_TOKEN_ENV_VAR] = previous


class _FakePool:
    """Stub asyncpg.Pool for unit tests that need WorkerDeps without real I/O."""

    def __init__(self) -> None:
        self._conn = _FakeConn()

    def acquire(self, timeout: float | None = None) -> "_FakeConnCtx":
        return _FakeConnCtx(self._conn)


class _FakeConn:
    """Stub asyncpg.Connection with no-op execute/fetch/transaction."""

    async def execute(self, *args: object, **kwargs: object) -> str:
        return "OK"

    async def fetch(self, *args: object, **kwargs: object) -> list[object]:
        return []

    def transaction(self) -> "_FakeConnCtx":
        return _FakeConnCtx(self)


class _FakeConnCtx:
    """Async context manager for _FakeConn."""

    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


@pytest.fixture(autouse=True)
def _clean_rate_limit_registry(request: pytest.FixtureRequest) -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: autouse fixture called by pytest; pyright cannot detect autouse fixtures.
    """Isolate the global rate-limit singleton per test.

    Actor decorators register rate limits into the module-level
    ``RateLimitRegistry`` singleton as import-time side effects, and pytest
    imports every collected module before running any test — so by the
    first test, the registry already holds ALL modules' entries.

    The registry has four module-level dicts plus two float timestamps that
    can carry state across tests: ``_rate_limits``, ``_reservations``,
    ``_keyed_reservation_last_used``, and ``_keyed_rate_limit_last_used``.
    The latter two are populated by lazy keyed-ref materialization
    (``_resolve_reservation_name`` / ``_resolve_rate_limit_name``) and
    would leak across tests if not isolated — a test that materializes a
    keyed ref against the real singleton would leave tracking entries
    that a subsequent test's cap-check or eviction logic could observe.
    The two opportunistic-eviction scan timestamps
    (``_keyed_*_last_eviction_scan``) are likewise reset so a prior
    test's cap-hit can't suppress a later test's expected scan.

    * Unit tests: cleared outright via the public
      :meth:`RateLimitRegistry.clear` (which resets all six state fields,
      including the opportunistic-eviction scan timestamps) —
      ``sync_rate_limit_buckets`` / ``sync_slots`` (called from ``_main``)
      would otherwise attempt pool I/O on stub-pool objects.
    * Integration tests: snapshot-and-restore — entries a test adds (or
      removes) are reverted afterwards so nothing leaks FORWARD into
      later tests. The worker additionally filters the registry by its
      own schema at bootstrap (see ``worker/_bootstrap.py``), so leftover
      foreign-schema entries are inert.
    """
    # Why: rate limiting runs inside the worker container; the in-process registry is irrelevant.
    if "e2e" in request.node.keywords:
        yield
        return
    from taskq.ratelimit.registry import registry as _rl

    if "integration" in request.node.keywords:
        snapshot_limits = dict(_rl._rate_limits)  # pyright: ignore[reportPrivateUsage]
        snapshot_reservations = dict(_rl._reservations)  # pyright: ignore[reportPrivateUsage]
        snapshot_keyed_res = dict(_rl._keyed_reservation_last_used)  # pyright: ignore[reportPrivateUsage]
        snapshot_keyed_rl = dict(_rl._keyed_rate_limit_last_used)  # pyright: ignore[reportPrivateUsage]
        snapshot_scan_res = _rl._keyed_reservation_last_eviction_scan  # pyright: ignore[reportPrivateUsage]
        snapshot_scan_rl = _rl._keyed_rate_limit_last_eviction_scan  # pyright: ignore[reportPrivateUsage]
        yield
        _rl._rate_limits.clear()  # pyright: ignore[reportPrivateUsage]
        _rl._rate_limits.update(snapshot_limits)  # pyright: ignore[reportPrivateUsage]
        _rl._reservations.clear()  # pyright: ignore[reportPrivateUsage]
        _rl._reservations.update(snapshot_reservations)  # pyright: ignore[reportPrivateUsage]
        _rl._keyed_reservation_last_used.clear()  # pyright: ignore[reportPrivateUsage]
        _rl._keyed_reservation_last_used.update(snapshot_keyed_res)  # pyright: ignore[reportPrivateUsage]
        _rl._keyed_rate_limit_last_used.clear()  # pyright: ignore[reportPrivateUsage]
        _rl._keyed_rate_limit_last_used.update(snapshot_keyed_rl)  # pyright: ignore[reportPrivateUsage]
        _rl._keyed_reservation_last_eviction_scan = snapshot_scan_res  # pyright: ignore[reportPrivateUsage]
        _rl._keyed_rate_limit_last_eviction_scan = snapshot_scan_rl  # pyright: ignore[reportPrivateUsage]
    else:
        _rl.clear()
        yield


__all__ = [
    "DEFAULT_ACTORS",
    "EmptyPayload",
    "FakeBackend",
    "JobsApp",
    "ModulePgSchema",
    "StubActorConfig",
    "_FakePool",
    "_logging_configured_guard",
    "_otel_enabled_guard",
    "actor_runner",
    "as_backend",
    "assert_attempt",
    "assert_has_event",
    "assert_has_otel_event",
    "assert_has_span",
    "assert_job_status",
    "assert_job_terminal",
    "assert_transition_sequence",
    "backend_pair",
    "clean_jobs_app",
    "clean_pg_conn",
    "clean_redis_client",
    "clean_redis_url",
    "create_pending_job",
    "create_running_job",
    "create_worker",
    "create_workered_running_job",
    "default_actor_config",
    "error_info",
    "get_job_triple",
    "jobs_app",
    "killable_redis_container",
    "make_enqueue_args",
    "make_integration_settings",
    "make_integration_settings_dict",
    "make_job_row",
    "memory_jobs",
    "module_jobs_app",
    "module_pg_pool",
    "module_pg_schema",
    "module_redis_url",
    "parse_detail",
    "redis_container",
    "redis_url",
    "reset_schema",
    "seed_actors",
    "setup_running_job",
    "truncate_schema",
    "wait_for",
    "wait_for_job_status",
    "wait_for_leader",
    "worker_with_running_job",
]


@dataclass(frozen=True, slots=True)
class _PgContainerShim:
    """Minimal shim matching the ``PostgresContainer`` surface TaskQ's tests use:
    ``get_connection_url()``. Wraps the SHARED container pair's DSN so tests that
    take ``pg_container`` as a fixture parameter work without starting a
    per-worker container. The DSN is the asyncpg-friendly ``postgresql://`` form
    (the real container returns ``postgresql+psycopg2://``; every consumer
    here replaces that prefix, so the shim returns the replaced form directly).
    """

    dsn: str

    def get_connection_url(self) -> str:
        return self.dsn


@pytest.fixture(scope="session")
def pg_container(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[_PgContainerShim]:
    """One shared Postgres 18 container for the whole run — every xdist worker.

    Backs onto :func:`taskq.testing._shared_containers.shared_service_pair`
    (state-dir file lock + cross-run holder registry): the first worker to take
    the lock boots the tuned container (``max_connections=1000`` serving ALL
    workers on ONE container, plus checkpoint tuning — see that module for the
    measured rationale), later workers reuse it, and the last worker to finish
    removes it. Tests that must STOP/PAUSE/MUTATE a Postgres (chaos, ALTER USER)
    get their own disposable container instead — never this one.
    """
    with shared_service_pair(tmp_path_factory.getbasetemp().parent) as services:
        delta = _pg_clock_delta(services.pg_dsn)
        if abs(delta) > 0.25:
            print(
                f"[TaskQ] application and database clocks diverge by {delta:+.3f}s "
                "(positive = app ahead). While this persists, wall-clock comparisons "
                "across the two domains are unreliable — VM pause/resume and NTP drift "
                "are common causes. Same-statement single-domain comparisons (this "
                "suite's timing tests) are unaffected."
            )
        yield _PgContainerShim(dsn=services.pg_dsn)


def _module_db_name(request: pytest.FixtureRequest) -> str:
    """Derive a unique, lowercase database name from the test module path.

    Mirrors the schema-name hashing in ``taskq.testing.fixtures`` (the run
    isolation token from :func:`run_isolation_token` is included so the same
    module gets distinct databases across parallel xdist workers AND across
    concurrent serial invocations sharing one container pair), sized well
    under PostgreSQL's 63-char identifier limit.
    """
    import hashlib

    token = run_isolation_token()
    full = request.module.__name__.replace(".", "_").replace("/", "_").lower()
    return "tq_db_" + hashlib.md5(f"{token}_{full}".encode()).hexdigest()[:12]  # noqa: S324 # Why: non-cryptographic hash for test database naming; collisions across ~100 modules are negligible.


def _pg_admin(base_dsn: str, *statements: str) -> None:
    """Run admin statements (CREATE/DROP DATABASE) against the container.

    Uses a private event loop on a private thread: sync fixtures may be
    requested from inside an already-running loop (pytest-asyncio drives
    async fixtures/tests via ``asyncio.Runner`` in the main thread), so
    creating a loop in the calling thread is not safe — a fresh thread
    has no such constraint. asyncpg is the only PG driver installed.
    """
    import asyncio
    import threading

    error: list[BaseException] = []

    def _target() -> None:
        async def _go() -> None:
            conn = await asyncpg.connect(base_dsn)
            try:
                for stmt in statements:
                    await conn.execute(stmt)
            finally:
                await conn.close()

        try:
            asyncio.run(_go())
        except BaseException as exc:  # Why: re-raised in the calling thread below.
            error.append(exc)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=30)
    if t.is_alive():
        raise TimeoutError(f"database admin timed out: {statements!r}")
    if error:
        raise error[0]


def _pg_clock_delta(base_dsn: str) -> float:
    """Seconds between the application and database clocks (app minus DB).

    Read once at container startup so divergence is attributed loudly
    instead of silently corrupting wall-clock comparisons: the two clocks
    are independent and can diverge or step (VM pause/resume and NTP drift
    are common causes). Same private-thread pattern as ``_pg_admin``.

    One-shot diagnostic, not a guard: it cannot see divergence that begins
    AFTER session start (a mid-run VM pause steps the clocks with no
    follow-up probe) — same-statement single-domain comparisons (this
    suite's timing tests) are unaffected either way.
    """
    import asyncio
    import threading
    import time

    result: list[float] = []
    error: list[BaseException] = []

    def _target() -> None:
        async def _go() -> None:
            conn = await asyncpg.connect(base_dsn)
            try:
                pg_epoch = await conn.fetchval("SELECT EXTRACT(EPOCH FROM clock_timestamp())")
            finally:
                await conn.close()
            result.append(float(pg_epoch) - time.time())

        try:
            asyncio.run(_go())
        except BaseException as exc:  # Why: re-raised in the calling thread below.
            error.append(exc)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=30)
    if t.is_alive():
        raise TimeoutError("clock-divergence probe timed out")
    if error:
        raise error[0]
    return result[0]


@pytest.fixture(scope="module")
def pg_dsn(pg_container: _PgContainerShim, request: pytest.FixtureRequest) -> Iterator[str]:
    """Module-scoped database on the shared container; DSN pointing at it.

    Every test module gets its OWN database on the ONE shared container —
    schema-level isolation in a shared database still shares cluster-wide
    state (advisory locks, pg_stat_activity, connection pressure), which let
    modules clobber each other. The database name is hashed with the run
    isolation token (see ``_publish_run_isolation_token``), so the same
    module gets distinct databases on parallel xdist workers AND in
    concurrent serial runs sharing the pair. The database is dropped
    (FORCE) on module teardown (cheap on the tuned shared cluster — see
    ``taskq.testing._shared_containers``); a drop-if-exists at setup clears
    stale state from crashed runs.
    """
    base_dsn = pg_container.get_connection_url()
    db_name = _module_db_name(request)

    _pg_admin(
        base_dsn,
        f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)',
        f'CREATE DATABASE "{db_name}"',
    )

    prefix, _, _db = base_dsn.rpartition("/")
    module_dsn = f"{prefix}/{db_name}"
    try:
        yield module_dsn
    finally:
        _pg_admin(base_dsn, f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')


@pytest.fixture
def settings(
    pg_dsn: str, module_pg_schema: ModulePgSchema, monkeypatch: pytest.MonkeyPatch
) -> TaskQSettings:
    """Per-test settings via :meth:`TaskQSettings.load`.

    Env vars are set with ``monkeypatch`` so they're scoped to one test, then
    ``TaskQSettings.load()`` reads them through the standard cascade. The
    schema name is derived from :func:`module_pg_schema` (a hash of the test
    module's own name) rather than the xdist worker id, so distinct test
    modules never collide on the same schema within a worker.
    """
    monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)
    return TaskQSettings.load()


@pytest_asyncio.fixture
async def pg_conn(settings: TaskQSettings) -> AsyncIterator[asyncpg.Connection]:
    """A clean asyncpg connection on the module's PG schema (see
    :func:`module_pg_schema`).  Drops the schema before each test — for
    isolation within a truncate/reseed cycle prefer ``clean_pg_conn``
    instead, which reuses the already-migrated module schema.
    """
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        yield conn
    finally:
        await conn.close()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Group ``integration`` and ``e2e`` tests by module for ``--dist=loadgroup``.

    ``--dist=loadgroup`` (set in ``pyproject.toml``) schedules every test
    that shares an ``xdist_group`` marker onto the same worker, and
    schedules everything else (ungrouped items) individually via the
    default load-balancing strategy.

    Grouping is defense-in-depth and efficiency, NOT a correctness
    requirement: every module-scoped name is already worker-qualified
    (``module_pg_schema`` / ``module_pg_pool`` / ``module_jobs_app`` hash the
    xdist worker id into the schema name, ``_module_db_name`` does the same
    for the per-module database, and ``module_redis_url`` allocates from a
    per-process counter), so a module accidentally split across workers
    would get DISTINCT schemas/databases/Redis DBs rather than clobbering.
    What grouping prevents is the waste and noise of that split: duplicated
    create/migrate/drop work per worker, doubled pool pressure against the
    session container, and e2e modules paying for a second worker container
    (``e2e_schema``, ``e2e_pg_pool``, ``e2e_worker``). This hook assigns
    ``xdist_group(name=<module basename>)`` to every ``integration`` or
    ``e2e`` test that doesn't already carry an explicit ``xdist_group``
    marker, so chaos-style tests keep whatever group they already declared
    (e.g. ``xdist_group(name="chaos")``) while everything else gets a safe,
    per-file default. The e2e namespace prefix keeps an e2e module from
    ever sharing a group with a same-stem integration module.
    """
    for item in items:
        is_e2e = "e2e" in item.keywords
        if "integration" not in item.keywords and not is_e2e:
            continue
        if item.get_closest_marker("xdist_group") is not None:
            continue
        group = f"e2e-{item.path.stem}" if is_e2e else item.path.stem
        item.add_marker(pytest.mark.xdist_group(name=group))
