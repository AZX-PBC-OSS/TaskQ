"""Shared test fixtures.

The ``pg_container`` fixture is session-scoped — Postgres takes a few seconds
to come up and we don't want to repeat that per test. Each test using
``pg_conn`` gets a fresh connection on the shared container, and the
``settings`` fixture sets ``TASKQ_*`` env vars so :meth:`TaskQSettings.load`
sees the per-test values.

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

from collections.abc import AsyncIterator, Iterator

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from taskq.settings import TaskQSettings
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
    JobsApp,
    ModulePgSchema,
    actor_runner,
    backend_pair,
    clean_jobs_app,
    clean_pg_conn,
    clean_redis_client,
    clean_redis_url,
    jobs_app,
    memory_jobs,
    module_jobs_app,
    module_pg_pool,
    module_pg_schema,
    module_redis_url,
    redis_container,
    redis_url,
    worker_with_running_job,
)
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
def _clean_rate_limit_registry(request: pytest.FixtureRequest) -> None:  # pyright: ignore[reportUnusedFunction] # Why: autouse fixture called by pytest; pyright cannot detect autouse fixtures.
    """Reset the global rate-limit singleton between test modules.

    Actor decorators register rate limits into the module-level
    ``RateLimitRegistry`` singleton as import-time side effects. That
    state leaks across test modules and causes ``sync_rate_limit_buckets``
    ``sync_slots`` (called from ``_main``) to attempt pool I/O on
    stub-pool objects in unit tests.

    We skip this cleanup for integration tests (they need the real
    registry populated from their own actor imports).
    """
    if "integration" in request.node.keywords:
        return
    from taskq.ratelimit.registry import registry as _rl

    _rl._rate_limits.clear()  # pyright: ignore[reportPrivateUsage]
    _rl._reservations.clear()  # pyright: ignore[reportPrivateUsage]


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


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    """Boot a Postgres 18 container for the test session.

    ``max_connections=500`` accommodates parallel test workers (``-n auto``
    on 32-core machines opens 32 x ~22 connections = ~700, which exceeds
    PostgreSQL's default of 100).
    """
    with PostgresContainer(
        image="postgres:18-alpine",
        username="taskq",
        password="taskq",
        dbname="taskq",
        command="-c max_connections=1000",
    ) as container:
        yield container


@pytest.fixture(scope="session")
def pg_dsn(pg_container: PostgresContainer) -> str:
    """Asyncpg-friendly DSN (``postgresql://`` scheme, no driver suffix)."""
    return pg_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


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
    """Group ``integration`` tests by module for ``--dist=loadgroup``.

    ``--dist=loadgroup`` (set in ``pyproject.toml``) schedules every test
    that shares an ``xdist_group`` marker onto the same worker, and
    schedules everything else (ungrouped items) individually via the
    default load-balancing strategy. Module-scoped PG fixtures
    (``module_pg_schema``, ``module_pg_pool``, ``module_jobs_app``) are
    only safe when every test in a module lands on the same worker —
    otherwise two workers would each try to create/migrate/drop the same
    hashed schema name concurrently. This hook assigns
    ``xdist_group(name=<module basename>)`` to every ``integration`` test
    that doesn't already carry an explicit ``xdist_group`` marker, so
    chaos-style tests keep whatever group they already declared (e.g.
    ``xdist_group(name="chaos")``) while everything else gets a safe,
    per-file default.
    """
    for item in items:
        if "integration" not in item.keywords:
            continue
        if item.get_closest_marker("xdist_group") is not None:
            continue
        item.add_marker(pytest.mark.xdist_group(name=item.path.stem))
