"""Queue-mode resolution cache pins: TTL pickup and explicit invalidation.

Every ``dispatch_batch`` opened its transaction with a queue-mode resolve
round trip (``SELECT name, mode FROM queues WHERE name = any($1)``) for
data that changes only through queue ops. The worker-side
:class:`~taskq.backend._dispatch.QueueModeCache` collapses that per-round
statement to a per-TTL resolve. These pins hold the two halves that make
the collapse safe:

* TTL - a mode change made out of band (another process, raw SQL) is
  picked up once the entry expires: the next dispatch re-resolves through
  the existing query, never through a cached stale mode.
* invalidation - a mode change made in THIS process through the queue-ops
  seam (``set_queue_mode``) is picked up by the very next dispatch: the
  seam clears every live cache, so no TTL window applies to the process
  that made the change.
* the miss path - unknown queues still resolve through the query with the
  ``strict_fifo`` fallback, and an absent row is cached with the same TTL
  bound (a queue configured mid-flight is picked up within the TTL, the
  same propagation bound a mode change gets).

These pins CONVERTED to the real fixture (the mock-subtraction cure): the
statements are the REAL pool's own - a RECORDING PROXY over
``module_pg_pool`` counts the resolve/dispatch statements the real
``_dispatch_batch`` issues (the house pattern:
``tests/test_cancel_where_bounded.py::_StatementRecordingPool``) - the
assertion counts real statements, it never answers for them. The only
double left is the CACHE's clock: the TTL arithmetic is the unit under
test's own domain, and a FakeClock's monotonic advances it without
sleeping.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch import (
    QUEUE_MODE_CACHE_TTL_SECONDS,
    QueueModeCache,
    _dispatch_batch,
)
from taskq.backend._sql_templates import render
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.queue_ops import set_queue_mode

pytestmark = pytest.mark.integration

_CLOCK_START = datetime(2025, 1, 1, tzinfo=UTC)


def _fake_clock() -> FakeClock:
    return FakeClock(_CLOCK_START)


class _RecordingDispatchPool:
    """RECORDING PROXY over the real pool (COUNT, DON'T ANSWER).

    Every call delegates to the real connection untouched; the proxy only
    COUNTS: the queue-mode resolves (the resolve reads the queues *table*,
    ``.queues WHERE`` - the dispatch CTE's ``queues`` is a params column,
    so qualified references like ``p.queues`` in the idle-actor prefilter
    never match) and the dispatch-CTE fetches (the claim, whose rendered
    body names the mode variant)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self.resolve_fetches = 0
        self.dispatch_fetches = 0
        self.dispatch_sqls: list[str] = []

    @asynccontextmanager
    async def acquire(
        self,
        **kwargs: object,  # Why: asyncpg.Pool.acquire's own kwargs forwarded verbatim.
    ) -> AsyncGenerator[asyncpg.Connection]:
        outer = self
        async with self._pool.acquire(**kwargs) as conn:  # type: ignore[arg-type]

            class _RecordingConn:
                async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
                    if ".queues WHERE" in sql:
                        outer.resolve_fetches += 1
                    elif "WITH RECURSIVE params AS (" in sql:
                        outer.dispatch_fetches += 1
                        outer.dispatch_sqls.append(sql)
                    return await conn.fetch(sql, *args)

                def __getattr__(self, name: str) -> object:
                    return getattr(conn, name)

            yield _RecordingConn()  # type: ignore[return-value]


async def _seed_queue(
    pool: asyncpg.Pool, schema: str, name: str, mode: str = "strict_fifo"
) -> None:
    await pool.execute(
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '  # noqa: S608  # Why: schema is the fixture-derived test identifier; name/mode are $-bound.
        "ON CONFLICT (name) DO UPDATE SET mode = EXCLUDED.mode",
        name,
        mode,
    )


async def _dispatch(
    pool: _RecordingDispatchPool,
    cache: QueueModeCache | None,
    *,
    queues: list[str],
    schema: str = "taskq",
) -> None:
    """One real _dispatch_batch round through the recording proxy."""
    await _dispatch_batch(
        pool,  # type: ignore[arg-type]  # Why: the proxy is the pool's exact surface; only acquire() is used.
        render(schema),
        2,
        5.0,
        schema,
        new_uuid(),
        queues,
        10,
        timedelta(seconds=30),
        queue_mode_cache=cache,
    )


@pytest.fixture
def dispatch_schema(module_pg_schema: ModulePgSchema) -> ModulePgSchema:
    return module_pg_schema


# ── The collapse: one resolve per TTL, not one per round ────────────────


async def test_resolve_runs_once_per_ttl_not_once_per_round(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """Within the TTL every dispatch round skips the resolve statement;
    the round still dispatches (the real CTE fetch happens each time)."""
    schema = module_pg_schema.schema_name
    await _seed_queue(module_pg_pool, schema, "qm_default")
    clock = _fake_clock()
    pool = _RecordingDispatchPool(module_pg_pool)
    cache = QueueModeCache(clock=clock.monotonic)

    for _ in range(5):
        await _dispatch(pool, cache, queues=["qm_default"], schema=schema)

    assert pool.resolve_fetches == 1, (
        f"expected ONE queue-mode resolve for 5 dispatch rounds inside the "
        f"TTL, got {pool.resolve_fetches} - the per-round statement is back"
    )
    assert pool.dispatch_fetches == 5, "every round must still dispatch (real claims)"
    assert len(pool.dispatch_sqls) == 5


async def test_mode_change_is_picked_up_within_the_ttl(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """An out-of-band mode change (raw SQL / another process) is picked up
    once the TTL expires: the entry goes stale, the next dispatch
    re-resolves, and the newly-read mode selects the SQL variant."""
    schema = module_pg_schema.schema_name
    await _seed_queue(module_pg_pool, schema, "qm_ttl_flip", mode="strict_fifo")
    clock = _fake_clock()
    pool = _RecordingDispatchPool(module_pg_pool)
    cache = QueueModeCache(clock=clock.monotonic)
    tmpl = render(schema)

    await _dispatch(pool, cache, queues=["qm_ttl_flip"], schema=schema)
    assert pool.dispatch_sqls[-1] == tmpl.dispatch_strict_fifo

    # Out-of-band flip on the real row - invisible to this process until
    # the TTL expires.
    await _seed_queue(module_pg_pool, schema, "qm_ttl_flip", mode="round_robin")
    clock.advance(timedelta(seconds=QUEUE_MODE_CACHE_TTL_SECONDS + 0.1))
    await _dispatch(pool, cache, queues=["qm_ttl_flip"], schema=schema)

    assert pool.resolve_fetches == 2, "an expired entry must re-resolve"
    assert pool.dispatch_sqls[-1] == tmpl.dispatch_round_robin, (
        "the re-resolved mode must select the round-robin SQL variant - a "
        "stale cached mode after TTL expiry silently changes dispatch "
        "ordering"
    )


# ── Explicit invalidation at the queue-ops seam ─────────────────────────


async def test_mode_change_is_picked_up_immediately_after_queue_ops_invalidation(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """A mode change made in this process through the queue-ops seam is
    visible to the very next dispatch - no TTL window applies to the
    process that made the change, because set_queue_mode clears every
    live cache."""
    schema = module_pg_schema.schema_name
    await _seed_queue(module_pg_pool, schema, "qm_seam", mode="strict_fifo")
    clock = _fake_clock()
    pool = _RecordingDispatchPool(module_pg_pool)
    cache = QueueModeCache(clock=clock.monotonic)
    tmpl = render(schema)

    await _dispatch(pool, cache, queues=["qm_seam"], schema=schema)
    assert pool.dispatch_sqls[-1] == tmpl.dispatch_strict_fifo

    async with module_pg_pool.acquire() as ops_conn:
        row = await set_queue_mode(ops_conn, "qm_seam", "round_robin", schema=schema)
    assert row.mode == "round_robin"

    # Clock not advanced: only the seam's invalidation can make this miss.
    await _dispatch(pool, cache, queues=["qm_seam"], schema=schema)
    assert pool.resolve_fetches == 2, (
        "set_queue_mode must clear the live cache - a same-process mode "
        "change served stale for a full TTL is a self-inflicted "
        "propagation delay"
    )
    assert pool.dispatch_sqls[-1] == tmpl.dispatch_round_robin


# ── The miss path and the fallback ─────────────────────────────────────


async def test_unknown_queue_resolves_via_the_query_and_caches_the_fallback(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """A queue with no row resolves through the existing query with the
    strict_fifo fallback (first round), and the absent row is cached with
    the same TTL bound (second round issues no resolve)."""
    schema = module_pg_schema.schema_name
    clock = _fake_clock()
    pool = _RecordingDispatchPool(module_pg_pool)
    cache = QueueModeCache(clock=clock.monotonic)
    tmpl = render(schema)

    missing = f"qm_missing_{new_base62()}"
    await _dispatch(pool, cache, queues=[missing], schema=schema)
    assert pool.resolve_fetches == 1
    assert pool.dispatch_sqls[-1] == tmpl.dispatch_strict_fifo

    await _dispatch(pool, cache, queues=[missing], schema=schema)
    assert pool.resolve_fetches == 1, "an absent row is cached, not re-read"


async def test_without_a_cache_every_round_resolves(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """queue_mode_cache=None preserves the helper's standalone contract:
    resolve per call, exactly the pre-cache behaviour."""
    schema = module_pg_schema.schema_name
    await _seed_queue(module_pg_pool, schema, "qm_nocache")
    pool = _RecordingDispatchPool(module_pg_pool)

    for _ in range(3):
        await _dispatch(pool, None, queues=["qm_nocache"], schema=schema)

    assert pool.resolve_fetches == 3


# ── Per-backend-instance ownership ─────────────────────────────────────


async def test_backend_instances_cache_independently(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """Two PostgresBackend instances never share mode state: a resolve
    that filled one backend's cache does nothing for the other backend's
    next dispatch - each pays its own one resolve per TTL."""
    from taskq.backend.postgres import PostgresBackend

    schema = module_pg_schema.schema_name
    await _seed_queue(module_pg_pool, schema, "qm_two_backends")
    mock_clock = FakeClock(_CLOCK_START)

    pool_a = _RecordingDispatchPool(module_pg_pool)
    pool_b = _RecordingDispatchPool(module_pg_pool)

    def _settings_for(s: ModulePgSchema) -> object:
        from taskq.settings import WorkerSettings

        return WorkerSettings.load_from_dict(
            {"pg_dsn": s.pg_dsn, "schema_name": s.schema_name},
        )

    def _deps(proxy: _RecordingDispatchPool) -> object:
        class _Deps:
            dispatcher_pool = proxy

        deps = _Deps()
        deps.settings = _settings_for(module_pg_schema)  # type: ignore[reportAttributeAccessIssue]  # Why: the stub deps satisfies BackendDeps structurally at runtime.
        return deps

    backend_a = PostgresBackend(  # type: ignore[arg-type]  # Why: the stub deps satisfies BackendDeps structurally at runtime.
        deps=_deps(pool_a),
        clock=mock_clock,
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )
    backend_b = PostgresBackend(  # type: ignore[arg-type]  # Why: same as above.
        deps=_deps(pool_b),
        clock=mock_clock,
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )

    for backend in (backend_a, backend_b):
        for _ in range(2):
            await backend.dispatch_batch(
                worker_id=new_uuid(),
                queues=["qm_two_backends"],
                limit=10,
                lock_lease=timedelta(seconds=30),
            )

    assert pool_a.resolve_fetches == 1
    assert pool_b.resolve_fetches == 1, (
        "backend B must pay its own resolve - a cache shared across "
        "backend instances couples workers that share a process (two "
        "workers, two schemas) into one mode view"
    )
