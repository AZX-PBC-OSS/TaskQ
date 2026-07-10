from __future__ import annotations

"""Pytest fixtures for TaskQ test suites.

Blueprint anchor:  Defines fixtures that downstream tests
consume:

- **memory_jobs** — function-scoped, yields a fresh ``InMemoryBackend``.
- **jobs_app** — function-scoped, opens ``WorkerDeps`` + ``PostgresBackend``
  against the session-scoped ``pg_container``.
- **actor_runner** — function-scoped, yields a callable that runs an actor
  function with a synthetic ``JobContext``.
- **backend_pair** — function-scoped, parametrised ``["memory", "pg"]``,
  yields a single ``Backend`` instance per param id.
- **module_pg_schema** — module-scoped, creates a per-file PG schema,
  applies migrations, seeds default data.  Reuses the session-scoped
  ``pg_container``.  Drops the schema on module teardown.
- **module_redis_url** — module-scoped, assigns a unique Redis DB per
  test module via atomic counter.  FLUSHDB on module teardown.
- **clean_pg_conn** — function-scoped, truncates all tables (FK-safe
  CASCADE) then re-seeds default data within the module's PG schema.
  Returns a clean ``asyncpg.Connection``.
- **clean_jobs_app** — function-scoped, same truncate+seed as
  ``clean_pg_conn``, then opens ``WorkerDeps`` + ``PostgresBackend``
  against the module's schema.
- **clean_redis_url** — function-scoped, ``FLUSHDB`` on the module's
  Redis DB, returns the clean URL.
- **clean_redis_client** — function-scoped, returns a fresh
  ``redis.asyncio.Redis`` client connected to the module's DB.

These fixtures test at the Backend level (``write_cancel_request``), not
the ``JobsClient`` public API level.  ``JobsClient.cancel()`` is a thin
wrapper that calls ``write_cancel_request`` and returns a ``CancelResult``.
The ``memory_jobs`` fixture exposes ``InMemoryBackend`` directly so tests
can call ``write_cancel_request`` + ``tick_cancel_polling`` without
depending on ``JobsClient``.

This is the only file in ``taskq.testing`` that may import asyncpg,
testcontainers, and pytest — and only inside the fixture definitions.
``InMemoryBackend`` and ``FakeClock`` modules remain stdlib-only.
"""

import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, NamedTuple, Protocol, cast
from uuid import UUID

import pytest
import pytest_asyncio
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import JobId
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.obs import bind_job_context
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload
from taskq.testing.job_context import JobContext
from taskq.testing.pg import (
    DEFAULT_ACTORS,
    _create_worker,  # pyright: ignore[reportPrivateUsage]  # Why: _create_worker is a shared test helper published by the testing module; private prefix scopes it within the testing package.
    create_workered_running_job,
    reset_schema,
    seed_actors,
)

if TYPE_CHECKING:
    import asyncpg
    from testcontainers.redis import RedisContainer

    from taskq.backend import Backend
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = Backend = object


if TYPE_CHECKING:

    class JobsApp(NamedTuple):
        deps: WorkerDeps  # type: ignore[valid-type]  # Why: WorkerDeps is only imported under TYPE_CHECKING; pyright cannot resolve the forward reference for the NamedTuple field annotation at runtime.
        backend: PostgresBackend  # type: ignore[valid-type]  # Why: PostgresBackend is only imported under TYPE_CHECKING; pyright cannot resolve the forward reference for the NamedTuple field annotation at runtime.

    class ModulePgSchema(NamedTuple):
        """Module-scoped PG schema reference returned by :func:`module_pg_schema`."""

        schema_name: str
        pg_dsn: str

else:

    class JobsApp(NamedTuple):
        deps: object
        backend: object

    class ModulePgSchema(NamedTuple):
        schema_name: str
        pg_dsn: str


__all__ = [
    "ActorRunnerCallable",
    "JobsApp",
    "ModulePgSchema",
    "_create_worker",
    "actor_runner",
    "backend_pair",
    "clean_jobs_app",
    "clean_pg_conn",
    "clean_redis_client",
    "clean_redis_url",
    "jobs_app",
    "memory_jobs",
    "module_jobs_app",
    "module_pg_pool",
    "module_pg_schema",
    "module_redis_url",
    "redis_container",
    "redis_url",
    "worker_with_running_job",
]


# ── ActorRunnerCallable protocol ───────────────────────────────────────


class ActorRunnerCallable(Protocol):
    """Protocol for the callable yielded by the ``actor_runner`` fixture.

    ``payload`` is permissively typed: a :class:`pydantic.BaseModel`
    (typed actor payload) or a ``dict[str, object]`` / ``object`` that
    the runner wraps in a :class:`PassthroughPayload` model. The
    ``JobContext.payload`` handed to the actor is always a
    :class:`pydantic.BaseModel` per the locked architecture; the
    coercion happens here so test authors don't have to declare a model
    for ad-hoc payloads.
    """

    async def __call__(
        self,
        actor_fn: Callable[..., object],
        payload: BaseModel | dict[str, object] | object,
        *,
        backend: InMemoryBackend,
        job_id: JobId | UUID | None = ...,
        attempt: int = ...,
        cancel_event: asyncio.Event | None = ...,
        actor: str = ...,
        queue: str = ...,
        **deps: object,
    ) -> object: ...


# ── memory_jobs ─────────────────────────────────────────────────────────


@pytest.fixture
async def memory_jobs() -> AsyncIterator[InMemoryBackend]:
    """Yield a fresh ``InMemoryBackend`` with a ``FakeClock`` starting at
    ``datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)``.  Default cancellation
    and cleanup grace from ``InMemoryBackend.__init__`` (30s each).
    No teardown beyond GC (fully isolated per ).
    """
    clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    for actor in DEFAULT_ACTORS:
        backend.register_actor_config(actor=actor)
    yield backend


# ── actor_runner ────────────────────────────────────────────────────────


@pytest.fixture
def actor_runner() -> ActorRunnerCallable:
    """Yield a callable that constructs a synthetic ``JobContext`` and
    calls ``actor_fn(payload, ctx)``.

    Accepts ``cancel_event`` to test cancellation paths and ``**deps``
    to forward ad-hoc keyword-injected collaborators (e.g. stub HTTP
    clients or database sessions) directly to ``actor_fn`` without
    wiring the full DI scope hierarchy.
    """

    async def run_actor(
        actor_fn: Callable[..., object],
        payload: BaseModel | dict[str, object] | object,
        *,
        backend: InMemoryBackend,
        job_id: JobId | UUID | None = None,
        attempt: int = 1,
        cancel_event: asyncio.Event | None = None,
        actor: str = "test_actor",
        queue: str = "default",
        **deps: object,
    ) -> object:
        jid: JobId = JobId(job_id) if job_id is not None else new_job_id()
        evt = cancel_event or asyncio.Event()
        backend.register_cancel_event(jid, evt)

        # Coerce arbitrary payload shapes into BaseModel — the production
        # JobContext requires P: BaseModel. Real test payloads (BaseModel
        # instances) pass through; raw dicts and other shapes get wrapped
        # in the permissive PassthroughPayload (extra="allow").
        ctx_payload: BaseModel
        if isinstance(payload, BaseModel):
            ctx_payload = payload
        elif isinstance(payload, dict):
            ctx_payload = PassthroughPayload.model_validate(payload)
        else:
            ctx_payload = PassthroughPayload.model_validate({"value": payload})

        ctx: JobContext[BaseModel] = JobContext(
            job_id=jid,
            actor=actor,
            queue=queue,
            attempt=attempt,
            payload=ctx_payload,
            cancel_event=evt,
            worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage]  # Why: fixture is an owned helper; _worker_id is private to InMemoryBackend but readable here for JobContext construction
            jobs=SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=None,
                backend=backend,
            ),
            log=bind_job_context(
                structlog.get_logger("taskq.testing.actor_runner"),
                job_id=jid,
                actor=actor,
                queue=queue,
                attempt=attempt,
                identity_key=None,
                trace_id="",
            ),
            deps=deps if deps else None,
        )
        result: object = actor_fn(payload, ctx)
        if isinstance(result, Awaitable):
            result = await cast(Awaitable[object], result)
        return result

    return run_actor


# ── PG helper (shared by jobs_app, clean_jobs_app, and backend_pair) ───


async def _open_pg_backend(
    pg_dsn: str,
    schema_name: str,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend]:
    """Open WorkerDeps + PostgresBackend against the PG container.

    Returns ``(stack, deps, backend)`` where *stack* is an
    :class:`AsyncExitStack` that the caller must close (LIFO teardown
    closes pools), *deps* is a ``WorkerDeps`` instance, and *backend*
    is a ``PostgresBackend`` instance.

    Performs: settings construction → schema drop/recreate → migrations →
    pool open → backend construction.

    Heavy imports (asyncpg, WorkerSettings, PostgresBackend, etc.) are
    done inside this function to keep the module-level import surface
    stdlib-only.
    """
    import asyncpg

    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.migrate import apply_pending
    from taskq.settings import WorkerSettings

    # 1. Build settings with fast integration-test defaults (blitz heartbeat,
    #    minimal grace periods) so test timeouts are bounded.
    from taskq.testing.settings import make_integration_settings_dict
    from taskq.worker.deps import open_worker_deps

    settings = WorkerSettings.load_from_dict(make_integration_settings_dict(pg_dsn))
    # Override schema name — make_integration_settings_dict defaults to
    # "taskq_test", but callers may pass custom schema names.
    settings.schema_name = schema_name

    # 2. Open connection, drop schema, apply migrations
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
        await seed_actors(conn, settings.schema_name)
    finally:
        await conn.close()

    # 3. DSN narrowing — direct DSN is guaranteed set after _post_load
    assert settings.pg_dsn_direct is not None  # post-load guarantee

    # 4. Open WorkerDeps via AsyncExitStack for proper LIFO teardown.
    #    If PostgresBackend construction fails after entering the context,
    #    we close the stack before re-raising so asyncpg pools are not leaked.
    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))

    try:
        # 5. Coerce float seconds to timedelta (research)
        cancellation_grace = timedelta(seconds=deps.settings.cancellation_grace_period)
        cleanup_grace = timedelta(seconds=deps.settings.cleanup_grace_period)

        # 6. Construct backend
        backend: PostgresBackend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=cancellation_grace,
            cleanup_grace_period=cleanup_grace,
        )
    except BaseException:
        await stack.aclose()
        raise

    return stack, deps, backend


# ── _open_two_pg_workers ────────────────────────────────────────────────


@asynccontextmanager
async def _open_two_pg_workers(  # type: ignore[reportUnusedFunction]  # Why: module-private helper consumed by downstream test modules (two-pod integration and chaos tests)
    pg_dsn: str,
    *,
    schema: str,
) -> AsyncGenerator[
    tuple[
        tuple[AsyncExitStack, WorkerDeps, PostgresBackend, UUID],
        tuple[AsyncExitStack, WorkerDeps, PostgresBackend, UUID],
    ],
    None,
]:
    """Open two WorkerDeps instances against the same PG container for
    two-pod election and chaos scenarios.

    Each pod gets an independent ``WorkerDeps`` + ``PostgresBackend``
    with its own pools, ``leader_conn``, ``is_leader`` event, and
    ``worker_id``. Both pods share the same migrated schema. The helper
    does NOT start the leader loops — callers construct
    ``MaintenanceLeader`` themselves with different initial states.

    Teardown is LIFO: per-pod ``AsyncExitStack`` pools close first,
    then the shared stack drops the schema.

    Used by integration tests for two-pod races and chaos kill scenarios.
    """
    import asyncpg

    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.migrate import apply_pending
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import open_worker_deps

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
        }
    )

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None

    worker_id_a = new_uuid()
    worker_id_b = new_uuid()

    # Pod A
    stack_a = AsyncExitStack()
    deps_a: WorkerDeps = await stack_a.enter_async_context(open_worker_deps(settings))
    try:
        cancellation_grace = timedelta(seconds=deps_a.settings.cancellation_grace_period)
        cleanup_grace = timedelta(seconds=deps_a.settings.cleanup_grace_period)
        backend_a: PostgresBackend = PostgresBackend(
            deps_a,
            clock=SystemClock(),
            cancellation_grace_period=cancellation_grace,
            cleanup_grace_period=cleanup_grace,
        )
    except BaseException:
        await stack_a.aclose()
        raise

    # Pod B
    stack_b = AsyncExitStack()
    deps_b: WorkerDeps = await stack_b.enter_async_context(open_worker_deps(settings))
    try:
        cancellation_grace = timedelta(seconds=deps_b.settings.cancellation_grace_period)
        cleanup_grace = timedelta(seconds=deps_b.settings.cleanup_grace_period)
        backend_b: PostgresBackend = PostgresBackend(
            deps_b,
            clock=SystemClock(),
            cancellation_grace_period=cancellation_grace,
            cleanup_grace_period=cleanup_grace,
        )
    except BaseException:
        await stack_b.aclose()
        await stack_a.aclose()
        raise

    try:
        # Create worker rows (both must exist before leader UPSERT — FK constraint)
        async with deps_a.dispatcher_pool.acquire() as conn_a:
            await _create_worker(conn_a, settings.schema_name, worker_id_a)
        async with deps_b.dispatcher_pool.acquire() as conn_b:
            await _create_worker(conn_b, settings.schema_name, worker_id_b)

        # Shared schema-cleanup stack closes last (LIFO after per-pod stacks)
        shared_stack = AsyncExitStack()

        async def _drop_schema() -> None:
            c = await asyncpg.connect(str(settings.pg_dsn))
            try:
                await c.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
            finally:
                await c.close()

        shared_stack.push_async_callback(_drop_schema)

        try:
            yield (
                (stack_a, deps_a, backend_a, worker_id_a),
                (stack_b, deps_b, backend_b, worker_id_b),
            )
        finally:
            await shared_stack.aclose()
    finally:
        await stack_b.aclose()
        await stack_a.aclose()


# ── jobs_app ───────────────────────────────────────────────────────────


@pytest.fixture
async def jobs_app(pg_dsn: str, request: pytest.FixtureRequest) -> AsyncIterator[JobsApp]:
    """Yield a :class:`JobsApp` named tuple ``(deps, backend)`` against the
    session-scoped PG container.

    Access the fields as ``jobs_app.deps`` and ``jobs_app.backend`` instead
    of unpacking — the named-tuple interface is clearer and type-safe.

    Per-test isolation: drops the schema CASCADE before each test (schema
    name is hashed from the test's own node id via
    :func:`_schema_name_from_test`, so distinct tests never share a schema),
    applies migrations, opens pools, constructs the backend.  Teardown via
    ``AsyncExitStack`` unwind closes pools; the schema is dropped at the
    next invocation's setup for the same test — same pattern as ``pg_conn``.
    """
    stack, deps, backend = await _open_pg_backend(
        pg_dsn, schema_name=_schema_name_from_test(request)
    )
    try:
        yield JobsApp(deps=deps, backend=backend)
    finally:
        await stack.aclose()


# ── backend_pair ───────────────────────────────────────────────────────


@pytest.fixture(params=["memory", "pg"], ids=["memory", "pg"])
async def backend_pair(request: pytest.FixtureRequest) -> AsyncIterator[Backend]:
    """Yield a single ``Backend`` instance per parametrize id.

    - ``memory``: same construction as ``memory_jobs``.
    - ``pg``: requires the ``jobs_app`` infrastructure (testcontainers +
      migrations); reuses the same ``pg_container`` / settings / migration
      sequence via :func:`_open_pg_backend`.  Returns the
      ``PostgresBackend`` instance.

    Tests using this fixture must be marked ``@pytest.mark.integration``
    so the PG branch does not run in the unit tier.  The guard below
    enforces this: the ``pg`` param is automatically skipped when the
    test lacks ``@pytest.mark.integration``, preventing testcontainers
    from booting during unit-only runs.

    ``pg_dsn`` is resolved lazily via ``request.getfixturevalue`` only
    inside the ``pg`` branch (after the integration-marker check) so
    the ``memory`` variant never triggers the ``pg_container`` fixture
    chain and stays container-free.
    """
    if request.param == "memory":
        clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
        backend: Backend = InMemoryBackend(clock=clock)
        for actor in DEFAULT_ACTORS:
            backend.register_actor_config(actor=actor)
        yield backend
    else:
        if not request.node.get_closest_marker("integration"):
            pytest.skip("PG backend requires @pytest.mark.integration")
        pg_dsn: str = request.getfixturevalue("pg_dsn")
        stack, _deps, pg_backend = await _open_pg_backend(
            pg_dsn, schema_name=_schema_name_from_test(request)
        )
        try:
            yield pg_backend
        finally:
            await stack.aclose()


# ── Redis container fixtures ───────────────────────────────────────────────


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
    """Boot a Redis 7.4 container for the test session.

    Image pinned to ``redis:7.4-alpine`` to match the ``redis-py >= 7.4.0``
    client expectations and Redis 7.0+ features (ACL, RESP3, streamed
    pub/sub).
    """
    import warnings

    from testcontainers.redis import RedisContainer

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*wait_container_is_ready.*",
            category=DeprecationWarning,
            module="testcontainers.redis",
        )
        with RedisContainer(image="redis:7.4-alpine") as rc:
            yield rc


@pytest.fixture
def redis_url(redis_container: RedisContainer) -> str:
    """Per-test Redis URL against the session-scoped container.

    Database ``/0`` is pinned explicitly — redis-py defaults to db=0 when
    no path is given, but spelling it out avoids surprises across redis-py
    versions and macOS Docker Desktop port-mapping quirks (where
    ``get_exposed_port`` may return a host-mapped port distinct from 6379).
    """
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


# ── Redis DB counter (module-level) ─────────────────────────────────────

_REDIS_DB_IDS: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
_redis_db_index: int = 0


def _next_redis_db() -> int:
    """Return the next Redis DB id (1-15), wrapping deterministically."""
    global _redis_db_index
    db = _REDIS_DB_IDS[_redis_db_index % len(_REDIS_DB_IDS)]
    _redis_db_index += 1
    return db


def _schema_name_from_test(request: pytest.FixtureRequest) -> str:
    """Derive a unique, lowercase schema name from the test's node id.

    Used by function-scoped PG fixtures (``jobs_app``, ``backend_pair``)
    that must not share a schema across different tests within the same
    xdist worker — the worker id alone is not a valid isolation key since
    many test functions run sequentially within one worker process.  Hashed
    for the same 13-char PostgreSQL identifier / NOTIFY-channel budget as
    :func:`_schema_name_from_module`.
    """
    import hashlib

    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    nodeid: str = request.node.nodeid.replace(".", "_").replace("/", "_").lower()  # pyright: ignore[reportUnknownVariableType]  # Why: pytest.FixtureRequest types are incomplete; return value is always a str.
    return "tq_" + hashlib.md5(f"{worker}_{nodeid}".encode()).hexdigest()[:10]  # noqa: S324 # Why: non-cryptographic hash for test schema naming; collisions across test nodeids are negligible with 10 hex chars.


def _schema_name_from_module(request: pytest.FixtureRequest) -> str:
    """Derive a unique, lowercase schema name from the test module path.

    Long module names are hashed to stay within PostgreSQL's 63-char
    identifier limit when combined with NOTIFY channel prefixes
    (``taskq_worker_{schema}_{uuid}`` = 13 + len(schema) + 1 + 36).
    The schema portion must be ≤ 13 chars, so we use ``tq_`` + 10-char
    hash suffix for modules whose full name would exceed the budget.

    The xdist worker ID is incorporated into the hash input so that the
    same module split across parallel workers does not collide on the
    same schema.
    """
    import hashlib

    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    full = request.module.__name__.replace(".", "_").replace("/", "_").lower()  # pyright: ignore[reportUnknownVariableType]  # Why: pytest.FixtureRequest types are incomplete; return value is always a str.
    # Always hash (with worker) — the non-hash branch would exceed the 13-char
    # budget once the worker suffix is appended, and hashing guarantees both
    # uniqueness across workers and the length constraint.
    return "tq_" + hashlib.md5(f"{worker}_{full}".encode()).hexdigest()[:10]  # noqa: S324 # Why: non-cryptographic hash for test schema naming; collisions across ~100 test modules are negligible with 10 hex chars.


# ── Module-scoped PG schema ─────────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def module_pg_schema(
    request: pytest.FixtureRequest,
    pg_dsn: str,
) -> AsyncIterator[ModulePgSchema]:
    """Module-scoped PG schema: create once per test file, truncate per test.

    Derives a unique schema name from the test module, applies migrations,
    seeds default actor_config rows.  Drops the schema CASCADE on module
    teardown.

    All tests in the module share the same schema; per-function isolation
    is provided by :func:`clean_pg_conn` and :func:`clean_jobs_app` which
    truncate every table before each test.
    """
    import asyncpg

    from taskq.migrate import apply_pending

    schema_name = _schema_name_from_module(request)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await apply_pending(conn, schema=schema_name)
        await seed_actors(conn, schema_name)
    finally:
        await conn.close()

    yield ModulePgSchema(schema_name=schema_name, pg_dsn=pg_dsn)

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
    finally:
        await conn.close()


# ── Module-scoped Redis DB ──────────────────────────────────────────────


@pytest.fixture(scope="module")
def module_redis_url(
    request: pytest.FixtureRequest,
    redis_container: RedisContainer,
) -> Iterator[str]:
    """Module-scoped Redis URL with a unique DB per test file.

    Assigns a unique Redis DB id (1-15) via atomic counter, returns
    ``redis://host:port/{db}``.  FLUSHDB on module teardown so clean
    for the next file.  DB 0 is reserved for the ``redis_url``
    fixture and manual development.
    """
    import redis as redis_sync

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    db = _next_redis_db()
    url = f"redis://{host}:{port}/{db}"
    yield url

    # Teardown: FLUSHDB the module's DB via sync client (safe in any context).
    client = redis_sync.from_url(url, decode_responses=False)
    try:
        client.flushdb()
    finally:
        client.close()


# ── Module-scoped PG pool ────────────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def module_pg_pool(module_pg_schema: ModulePgSchema) -> AsyncIterator[asyncpg.Pool]:
    """Module-scoped asyncpg pool for the module's PG schema.

    Pool is shared by all tests in a module. Per-test isolation is handled
    by clean_pg_conn/clean_jobs_app which truncate tables between tests.
    """
    import asyncpg

    pool = await asyncpg.create_pool(
        module_pg_schema.pg_dsn,
        min_size=1,
        max_size=4,
    )
    try:
        yield pool
    finally:
        await pool.close()


# ── Module-scoped JobsApp ────────────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def module_jobs_app(module_pg_schema: ModulePgSchema) -> AsyncIterator[JobsApp]:
    """Module-scoped JobsApp (WorkerDeps + PostgresBackend) on the module's PG schema.

    Pools are opened once per test module. Per-test isolation is handled by
    clean_jobs_app which truncates tables between tests against the same schema.
    The backend instance is shared — callers should NOT cache or mutate backend
    state across test boundaries.
    """
    stack, deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        yield JobsApp(deps=deps, backend=backend)
    finally:
        await stack.aclose()


# ── Function-scoped cleanup (per-test isolation) ────────────────────────


@pytest.fixture
async def clean_pg_conn(module_pg_schema: ModulePgSchema) -> AsyncIterator[asyncpg.Connection]:
    """Per-test clean asyncpg connection on the module's PG schema.

    Truncates every dynamic table (FK-safe CASCADE) then re-seeds default
    actor_config rows, guaranteeing a blank slate for each test while
    keeping migration state intact.
    """
    import asyncpg as _asyncpg

    conn = await _asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await reset_schema(conn, module_pg_schema.schema_name)
        yield conn
    finally:
        await conn.close()


@pytest.fixture
async def worker_with_running_job(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> AsyncIterator[tuple[UUID, UUID, asyncpg.Connection]]:
    """Per-test clean connection with a pre-created worker + running job.

    Yields ``(worker_id, job_id, conn)`` where *conn* is the same
    ``asyncpg.Connection`` provided by :func:`clean_pg_conn`.  The
    connection lifecycle is managed by ``clean_pg_conn`` — this fixture
    only inserts the worker and job rows.
    """
    wid, jid = await create_workered_running_job(clean_pg_conn, module_pg_schema.schema_name)
    yield wid, jid, clean_pg_conn


async def _open_pg_backend_on_schema(
    pg_dsn: str,
    schema_name: str,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend]:
    """Open WorkerDeps + PostgresBackend on an already-migrated schema.

    Unlike :func:`_open_pg_backend`, this does NOT drop or recreate the
    schema — the caller must handle truncation/reset via :func:`reset_schema`.
    """
    from taskq.backend.clock import SystemClock
    from taskq.backend.postgres import PostgresBackend
    from taskq.settings import WorkerSettings
    from taskq.testing.settings import make_integration_settings_dict
    from taskq.worker.deps import open_worker_deps

    settings = WorkerSettings.load_from_dict(make_integration_settings_dict(pg_dsn))
    settings.schema_name = schema_name
    assert settings.pg_dsn_direct is not None

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        cancellation_grace = timedelta(seconds=deps.settings.cancellation_grace_period)
        cleanup_grace = timedelta(seconds=deps.settings.cleanup_grace_period)
        backend: PostgresBackend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=cancellation_grace,
            cleanup_grace_period=cleanup_grace,
        )
    except BaseException:
        await stack.aclose()
        raise

    return stack, deps, backend


@pytest.fixture
async def clean_jobs_app(module_pg_schema: ModulePgSchema) -> AsyncIterator[JobsApp]:
    """Per-test clean ``JobsApp`` (WorkerDeps + PostgresBackend) on the
    module's PG schema.

    Truncates + re-seeds before each test, then opens WorkerDeps and
    constructs a PostgresBackend.  Faster than the ``jobs_app`` fixture
    which drops/recreates the schema every test.
    """
    import asyncpg

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await reset_schema(conn, module_pg_schema.schema_name)
    finally:
        await conn.close()

    stack, deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        yield JobsApp(deps=deps, backend=backend)
    finally:
        await stack.aclose()


# ── Function-scoped Redis cleanup ───────────────────────────────────────


@pytest.fixture
def clean_redis_url(module_redis_url: str) -> str:
    """Per-test clean Redis URL.

    FLUSHDB on the module's Redis DB before each test, guaranteeing
    no cross-test state within the module.  Uses the synchronous
    ``redis.Redis`` client so the flush is reliable regardless of
    whether tests are sync or async.
    """
    import redis as redis_sync

    client = redis_sync.from_url(module_redis_url, decode_responses=False)
    try:
        client.flushdb()
    finally:
        client.close()

    return module_redis_url


@pytest.fixture
async def clean_redis_client(clean_redis_url: str) -> AsyncIterator[object]:
    """Per-test clean Redis async client against the module's DB.

    Returns a fresh ``redis.asyncio.Redis`` client with ``decode_responses=False``.
    """
    from redis.asyncio import from_url as redis_from_url

    client = redis_from_url(clean_redis_url, decode_responses=False)
    try:
        yield client
    finally:
        await client.aclose()
