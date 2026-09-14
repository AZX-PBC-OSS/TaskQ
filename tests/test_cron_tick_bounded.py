"""``tick_cron`` must bound the work one tick does, and must not lose it all on a deadline.

The tick's driving SELECT (``taskq/worker/cron_loop.py``, the ``_cron_tick_sql``
f-string) is ``LIMIT``-ed and ordered::

    SELECT ... FROM "<schema>".cron_schedules
    WHERE enabled = true AND next_fire_at <= statement_timestamp()
    ORDER BY next_fire_at
    LIMIT $1

so one tick fires at most that many schedules; the fire path's enqueue
and ``UPDATE cron_schedules`` writes are batched, and the leader's loop
drains a catch-up burst across several committed ticks rather than
attempting it as one oversized transaction.

Why the bound matters
---------------------
``MaintenanceLeader._cron_loop`` wraps a tick in one deadline and one
transaction::

    async with asyncio.timeout(self._deps.settings.dispatcher_command_timeout):
        async with conn.transaction():
            await tick_cron(...)

That is all-or-nothing: if a tick's statement count grew with the number
of due schedules, the deadline would eventually trip mid-tick, roll the
whole thing back, leave every ``next_fire_at`` in the past, and hand the
next tick a backlog at least as large — the livelock shape.  A catch-up
burst after a leader outage is exactly the N that would trip it; the cap
plus the batched writes foreclose it.  This is the same shape as the
sweep-3 contract pinned in
``test_sweep_scheduled_to_pending_batching.py``.

There is a second cost even when the deadline is not hit.  A tick writes
``jobs`` rows, so its open-transaction duration must not be proportional
to the due backlog: ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``
(2 seconds) documents that the ``job_events`` watermark guarantee holds
only while no writer sits longer than that margin between INSERT and
COMMIT.  A cron tick is not itself a ``job_events`` writer, but a long
tick transaction against ``jobs`` on the leader's dedicated connection
makes every other writer's commit queue behind whatever lock contention
it creates.

The wall-clock threshold is RTT-dependent, so the Layer 1 assertions below
count awaited statements rather than seconds -- deterministic in any
environment -- and assert directly that a deadline-tripped tick leaves
*committed* progress behind rather than nothing at all.

Layer 1 (``TestTickIsBounded``) pins the boundedness contract.  Layer 2
(``TestTickBehaviourPinned``) pins the observable behaviour a rewrite of
the tick's SQL must not change.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

# Large enough that the per-row statement count is unmistakable against any
# plausible constant, small enough that the Layer 2 tests stay quick.
_DUE = 40

# Hourly, so a fired schedule's recomputed next_fire_at lands comfortably in
# the future and a second tick in the same test finds nothing due.
_HOURLY = "0 * * * *"

_CRON_ACTOR = "test_actor"
_CRON_QUEUE = "cron_queue"


# ── Harness ────────────────────────────────────────────────────────────


class _CountingConn:
    """Delegates to a real connection, counting every awaited round trip.

    The statement count IS the property under test: correctness never differed
    between the per-schedule loop and a batched form, only the number of
    awaited round trips taken inside the tick's transaction while the leader's
    ``asyncio.timeout`` clock runs.

    Every awaited protocol method is counted -- ``execute``, ``executemany``,
    ``fetch``, ``fetchrow``, ``fetchval`` -- so the assertion pins the
    invariant (a tick's round trips do not grow linearly in the number of due
    schedules) rather than the spelling of any particular fix. A fix that
    moves the per-schedule UPDATE into one ``unnest`` statement, or that
    batches the enqueues, or that simply adds a ``LIMIT`` and lets the leader
    drain across ticks, all satisfy it; a loop reintroduced under any other
    spelling -- ``enumerate``, a comprehension of awaits, a helper -- does not,
    and a source-grep for ``for row in rows:`` would not have caught those.

    ``transaction()`` is passed straight through, so a fix that opens
    per-batch subtransactions still works under this wrapper.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.statements: list[str] = []

    def _record(self, sql: str) -> None:
        self.statements.append(" ".join(sql.split())[:200])

    @property
    def count(self) -> int:
        return len(self.statements)

    def matching(self, needle: str) -> int:
        """Number of recorded statements containing *needle* (case-sensitive)."""
        return sum(1 for s in self.statements if needle in s)

    async def execute(self, sql: str, *args: object, **kwargs: object) -> Any:
        self._record(sql)
        return await self._conn.execute(sql, *args, **kwargs)

    async def executemany(self, sql: str, args: object, **kwargs: object) -> Any:
        self._record(sql)
        return await self._conn.executemany(sql, args, **kwargs)

    async def fetch(self, sql: str, *args: object, **kwargs: object) -> Any:
        self._record(sql)
        return await self._conn.fetch(sql, *args, **kwargs)

    async def fetchrow(self, sql: str, *args: object, **kwargs: object) -> Any:
        self._record(sql)
        return await self._conn.fetchrow(sql, *args, **kwargs)

    async def fetchval(self, sql: str, *args: object, **kwargs: object) -> Any:
        self._record(sql)
        return await self._conn.fetchval(sql, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _StubBackendDeps:
    """Minimal ``BackendDeps`` for ``PostgresBackend``.

    ``PostgresBackend.__init__`` reads only ``deps.settings.schema_name``; the
    pools are resolved lazily through properties, and the only backend method
    the cron tick calls is ``enqueue_with_conn``, which runs entirely on the
    caller-supplied connection and never touches a pool.
    """

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool = None
        self.heartbeat_pool = None
        self.dispatcher_pool = None


def _cron_settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_CRON_AUTO_DISABLE_THRESHOLD": "3",
        },
        validate=False,
    )


def _make_backend(schema: str) -> tuple[PostgresBackend, WorkerSettings]:
    settings = _cron_settings(schema)
    backend = PostgresBackend(
        _StubBackendDeps(settings),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; __init__ reads only settings.schema_name and pools are never acquired on this path.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=0),
        cleanup_grace_period=timedelta(seconds=0),
    )
    return backend, settings


async def _seed_actor_config(
    conn: asyncpg.Connection,
    schema: str,
    actor: str = _CRON_ACTOR,
    *,
    queue: str = _CRON_QUEUE,
    max_attempts: int = 5,
    retry_kind: str = "transient",
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_attempts, retry_kind) '  # noqa: S608  # Why: schema is a test-fixture identifier derived from the module name.
        "VALUES ($1, $2, $3, $4) "
        "ON CONFLICT (actor) DO UPDATE SET queue = EXCLUDED.queue, "
        "max_attempts = EXCLUDED.max_attempts, retry_kind = EXCLUDED.retry_kind",
        actor,
        queue,
        max_attempts,
        retry_kind,
    )


async def _seed_due_schedules(
    conn: asyncpg.Connection,
    schema: str,
    count: int,
    *,
    actor: str = _CRON_ACTOR,
    identity_prefix: str | None = None,
) -> list[UUID]:
    """Seed *count* enabled, already-due schedules in ONE round trip.

    ``cron_schedules`` is UNIQUE on ``(actor, name)`` (migration
    ``01.00.01_01_pre_per_property_cron.sql`` drops the original
    UNIQUE(actor) and adds the composite), so many schedules share one actor
    as long as ``name`` differs -- which is also what exercises the
    ``actor_config_cache`` hit path rather than N cache misses.

    ``next_fire_at`` is seeded ON the cron grid, at the current hour
    boundary (``date_trunc('hour', clock_timestamp())``) -- due (the
    boundary is <= now for every instant inside the hour), exactly one
    slot behind, and inside ``cron_catch_up_window`` (1 hour by default),
    so none of these rows takes the missed-slot recompute branch. The grid
    alignment is load-bearing: an off-grid due time (the previous
    ``now - 5 minutes`` seed) makes the recomputed ``next_fire_at`` depend
    on where the wall clock sits inside the hour -- run the suite in the
    first ~5 minutes of an hour and the next hourly boundary after
    ``now - 5m`` is already in the past, so the fired schedule stays due
    by design (sequential catch-up fires each missed slot on its own tick,
    per ``WorkerSettings.cron_catch_up_window``) and the advance/second-
    tick pins below fail purely on time-of-day. On the grid, the next
    boundary after the seed is the NEXT hour, strictly in the future for
    every possible run instant.
    """
    ids = [new_uuid() for _ in range(count)]
    names = [f"s{i:05d}" for i in range(count)]
    keys = (
        [f"{identity_prefix}{i:05d}" for i in range(count)]
        if identity_prefix is not None
        else [None] * count
    )
    await conn.execute(
        f'INSERT INTO "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier derived from the module name.
        "(id, actor, name, cron_expr, timezone, dst_strategy, payload_factory, "
        " enabled, next_fire_at, metadata, identity_key, consecutive_failures) "
        "SELECT t.id, $4, t.name, $5, 'UTC', 'skip', NULL, true, "
        "       date_trunc('hour', clock_timestamp()), '{}'::jsonb, t.ikey, 0 "
        "FROM unnest($1::uuid[], $2::text[], $3::text[]) AS t(id, name, ikey)",
        ids,
        names,
        keys,
        actor,
        _HOURLY,
    )
    return ids


@pytest.fixture(autouse=True)
def _quiet_cron_logs() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Keep structlog's per-fire INFO lines out of a 40-schedule test's output."""
    import logging

    logger = logging.getLogger("taskq.worker.cron_loop")
    previous = logger.level
    logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        logger.setLevel(previous)


# ── Layer 1 — the boundedness contract these tests enforce ─────────────


class TestTickIsBounded:
    """Layer 1: the boundedness contract. A regression to an unbounded,
    per-schedule-loop tick fails every test here."""

    async def test_tick_round_trips_do_not_grow_with_the_number_of_due_schedules(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One tick over N due schedules must not take O(N) awaited round trips.

        A bounded tick spends a constant handful of statements regardless
        of N: the driving ``fetch`` plus the advisory-lock and
        ``clock_timestamp()`` ``fetchval``s, a per-actor
        ``actor_config`` ``fetchrow``, and batched writes — never three
        round trips per due schedule.

        The threshold below is deliberately loose: anything genuinely bounded
        (one batched enqueue plus one batched UPDATE, or a ``LIMIT`` that caps
        the tick) lands in the low tens regardless of N. The point of the
        assertion is the *shape* -- constant vs linear -- not an exact
        number, so the implementation is free to spend a handful of extra
        statements.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        await _seed_due_schedules(clean_pg_conn, schema, _DUE)

        counting = _CountingConn(clean_pg_conn)
        async with clean_pg_conn.transaction():
            await tick_cron(
                counting,  # type: ignore[arg-type]  # Why: duck-typed connection; only execute/fetch/fetchrow/fetchval/transaction are used.
                settings,
                backend,
                schema,
                new_uuid(),
            )

        budget = 20
        assert counting.count <= budget, (
            f"one tick over {_DUE} due schedules issued {counting.count} awaited "
            f"round trips (budget {budget}); the per-schedule loop is still there, "
            f"so a tick costs O(N) statements inside the single transaction the "
            f"leader's dispatcher_command_timeout deadline rolls back in full. "
            f"Statement kinds seen: "
            f"{counting.matching('cron_schedules')} against cron_schedules, "
            f"{counting.matching('pg_notify')} pg_notify."
        )

    async def test_per_schedule_update_is_not_one_statement_per_schedule(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The ``UPDATE cron_schedules`` advance must be batched, not per row.

        The schedule advance is one batched ``UPDATE`` (an ``unnest``
        over ``(id, next_fire_at)`` -- the same shape dispatch uses for
        its event writes), never N single-row ``UPDATE ... WHERE id =
        $1`` statements each taking a row lock held until the tick's
        COMMIT.  Both the round-trip count and the lock-hold window are
        therefore independent of the backlog.

        Asserted separately from the round-trip total above because it is
        the one write the tick can trivially batch, and because a
        ``LIMIT`` alone would satisfy this one too (the cap bounds the
        count).
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        await _seed_due_schedules(clean_pg_conn, schema, _DUE)

        counting = _CountingConn(clean_pg_conn)
        async with clean_pg_conn.transaction():
            await tick_cron(
                counting,  # type: ignore[arg-type]  # Why: duck-typed connection.
                settings,
                backend,
                schema,
                new_uuid(),
            )

        updates = counting.matching('UPDATE "')
        assert updates <= 4, (
            f"{updates} awaited UPDATE statements for {_DUE} due schedules — the "
            "per-schedule `UPDATE cron_schedules ... WHERE id = $1` in "
            "fire_schedule is still one round trip and one row lock per row, "
            "both held until the tick's COMMIT"
        )

    async def test_tick_accepts_and_honours_a_cap_on_schedules_fired(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """``tick_cron`` must expose a cap and fire at most that many schedules.

        ``tick_cron``'s driving SELECT carries a ``LIMIT``, so one tick
        fires at most that many schedules whatever the size of the due
        backlog.

        A cap is what makes the work per tick bounded *independently* of how
        cheap the individual statements become: with it, a catch-up burst
        drains across several committed ticks (each one second apart, or
        immediately if the caller drains in a loop) instead of being attempted
        and rolled back as one oversized transaction. The parameter name is not
        pinned -- any of ``limit``/``max_fires``/``batch_size``/``cap`` is
        accepted -- only that one exists and is respected.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        await _seed_due_schedules(clean_pg_conn, schema, _DUE)

        params = inspect.signature(tick_cron).parameters
        cap_names = ("limit", "max_fires", "max_schedules", "batch_size", "cap")
        cap_param = next((n for n in cap_names if n in params), None)
        assert cap_param is not None, (
            "tick_cron takes no cap parameter — its SELECT over cron_schedules has "
            f"no LIMIT, so one tick fires the whole due backlog. Tried {cap_names}; "
            f"signature is {inspect.signature(tick_cron)}"
        )

        cap = 5
        async with clean_pg_conn.transaction():
            await tick_cron(
                clean_pg_conn,
                settings,
                backend,
                schema,
                new_uuid(),
                **{cap_param: cap},
            )

        enqueued: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier.
            _CRON_ACTOR,
        )
        assert enqueued == cap, (
            f"tick_cron({cap_param}={cap}) enqueued {enqueued} jobs; a capped tick "
            f"must fire at most {cap} schedules and leave the rest for the next tick"
        )

        still_due: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "WHERE enabled = true AND next_fire_at <= clock_timestamp()"
        )
        assert still_due == _DUE - cap, (
            f"{still_due} schedules still due after a capped tick; expected "
            f"{_DUE - cap} — the uncapped remainder must be left untouched, not "
            "silently advanced"
        )

    async def test_deadline_tripped_tick_commits_partial_progress(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A tick cut short by the leader's deadline must not lose ALL its work.

        The leader runs::

            async with asyncio.timeout(settings.dispatcher_command_timeout):
                async with conn.transaction():
                    await tick_cron(...)

        A regression to a per-schedule loop under that nesting is the
        livelock itself: when the deadline fires mid-loop the single
        enclosing transaction rolls back — zero jobs enqueued, zero
        ``next_fire_at`` advanced, every schedule still due. The next
        tick starts from a backlog no smaller than the one that just
        failed, and cron never fires again until the backlog somehow
        shrinks — which, without a commit, it cannot.

        This test reproduces the caller's exact nesting and asserts the
        recovery property: after a deadline trip, *some* schedules must
        have fired and been advanced. Batching alone satisfies it (the
        whole tick finishes inside the deadline); a ``LIMIT`` plus a
        per-batch commit satisfies it (each committed batch is durable
        progress); a single-commit-point tick cannot, because its one
        commit is never reached.

        The deadline is deliberately tiny rather than the 5-second production
        default: the assertion is about all-or-nothing rollback, not about the
        wall-clock value at which it starts happening, and a tight deadline
        makes the trip deterministic on fast and slow hardware alike.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        await _seed_due_schedules(clean_pg_conn, schema, _DUE)

        worker_id = new_uuid()
        # Small enough that a per-schedule round-trip regression (4 + 3N
        # awaited statements, N=40 → 124) would trip it at any round-trip
        # latency above ~1.7 ms, i.e. under any parallel-suite load — and
        # large enough that the bounded tick (≈7 round trips + in-memory
        # planning) completes with real headroom. Calibrated by
        # measurement, not guesswork: one bounded tick over _DUE
        # schedules measures ~25-31 ms idle and ~2x that under ``-n 4``
        # suite load, so 0.05 s (an earlier value) left only a ~1.7x
        # margin and flaked under load — a loaded tick overran the
        # deadline, the single tick transaction rolled back, and the test
        # failed on machine timing rather than the property it pins. The
        # deterministic regression pins for per-schedule round trips live
        # in the sibling tests (statement count, UPDATE count); this
        # test's own property is the all-or-nothing rollback/livelock
        # behaviour, which a 0.2 s deadline still exercises.
        # Calibrate the deadline from a measured healthy tick on this
        # machine, per this test's own stated philosophy (measurement, not
        # guesswork). A constant deadline flakes in exactly one direction:
        # on a slow or loaded runner a healthy bounded tick overruns it,
        # commits nothing, and the test fails on machine timing instead of
        # the property it pins — this happened in CI at 0.2 s. The
        # property needs only that the deadline is far above a healthy
        # tick's duration and far below the per-schedule regression's
        # (4 + 3N awaited statements is >10x the bounded tick's ~7); 10x
        # the measured tick satisfies both at any runner speed. The
        # livelock regression is caught by the assertions below
        # regardless of where the deadline lands: their failure mode is
        # ZERO committed fires, which no deadline size produces for a
        # healthy or batch-committed tick. The schedules were seeded once
        # above; the calibration tick consumes them, and the reset below
        # hands the timed tick the same due state again.
        calibration_start = time.perf_counter()
        async with clean_pg_conn.transaction():
            await tick_cron(
                clean_pg_conn,
                settings,
                backend,
                schema,
                worker_id,
            )
        healthy_s = time.perf_counter() - calibration_start

        # Restore the due state the calibration tick consumed: drop its
        # fires and clear the fire stamps so the timed tick below faces
        # the same backlog shape the production regression would.
        await clean_pg_conn.execute(
            f'DELETE FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier.
            _CRON_ACTOR,
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "SET last_fired_at = NULL, next_fire_at = clock_timestamp() "
            "WHERE enabled = true"
        )

        deadline = max(0.2, 10 * healthy_s)

        with contextlib.suppress(TimeoutError, asyncpg.QueryCanceledError, asyncpg.InterfaceError):
            async with asyncio.timeout(deadline):
                async with clean_pg_conn.transaction():
                    await tick_cron(
                        clean_pg_conn,
                        settings,
                        backend,
                        schema,
                        worker_id,
                    )

        # Read the outcome on a SEPARATE connection: the question is what is
        # COMMITTED and visible fleet-wide, not what the (possibly aborted,
        # possibly cancel-wounded) tick connection can still see. Anything
        # uncommitted is invisible here by MVCC, which is exactly the
        # distinction the assertions below are about.
        observer = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            enqueued: int = await observer.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier.
                _CRON_ACTOR,
            )
            fired: int = await observer.fetchval(
                f'SELECT count(*) FROM "{schema}".cron_schedules WHERE last_fired_at IS NOT NULL'  # noqa: S608  # Why: schema is a test-fixture identifier.
            )
        finally:
            await observer.close()

        assert enqueued > 0, (
            f"a tick over {_DUE} due schedules cut off after {deadline}s committed "
            "ZERO jobs — the whole-iteration deadline rolled back every fire. The "
            "backlog is unchanged, the next tick faces the same or a larger one, "
            "and cron never commits again: livelock"
        )
        assert fired > 0, (
            "zero schedules had last_fired_at stamped after a deadline-tripped "
            "tick — no fire survived the rollback, so next_fire_at never advances "
            "and the due set can only grow"
        )


# ── Layer 2 — PASSES TODAY; must still pass after the fix ──────────────


class TestTickBehaviourPinned:
    """Layer 2: pins existing behaviour — must pass before AND after the fix.

    The fixes rewrite the SQL that selects due schedules, enqueues their jobs
    and advances ``next_fire_at``. These tests pin the observable result of a
    tick so a rewrite cannot quietly change it: which schedules fire, what the
    enqueued job looks like, what the schedule row looks like afterwards, and
    which rows the predicate must leave alone.
    """

    async def test_one_job_per_due_schedule_with_actor_queue_and_identity_key(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins existing behaviour — must pass before AND after the fix.

        Exactly one job per due schedule, carrying the schedule's actor, the
        queue from ``actor_config`` (not the schedule row), ``max_attempts``
        and ``retry_kind`` from ``actor_config``, and the schedule's own
        ``identity_key`` propagated one-to-one. Status is ``pending``, because
        ``fire_schedule`` passes ``scheduled_at=None`` and the enqueue SQL
        stamps the server clock and decides status in the same statement.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(
            clean_pg_conn, schema, queue=_CRON_QUEUE, max_attempts=7, retry_kind="indefinite"
        )
        count = 6
        await _seed_due_schedules(clean_pg_conn, schema, count, identity_prefix="tenant-")

        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        rows = await clean_pg_conn.fetch(
            f"SELECT actor, queue, status::text AS status, max_attempts, retry_kind, "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f"identity_key, payload, payload_schema_ver, attempt "
            f'FROM "{schema}".jobs ORDER BY identity_key'
        )
        assert len(rows) == count, f"expected exactly one job per due schedule ({count})"

        for i, row in enumerate(rows):
            assert row["actor"] == _CRON_ACTOR
            assert row["queue"] == _CRON_QUEUE, "queue comes from actor_config, not the schedule"
            assert row["status"] == "pending"
            assert row["max_attempts"] == 7
            assert row["retry_kind"] == "indefinite"
            assert row["identity_key"] == f"tenant-{i:05d}", (
                "the schedule's identity_key must reach the job unchanged"
            )
            assert row["payload_schema_ver"] == 1
            assert row["attempt"] == 0

        distinct_ids: int = await clean_pg_conn.fetchval(
            f'SELECT count(DISTINCT id) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert distinct_ids == count, "each fire must mint its own job id"

    async def test_fired_schedule_row_is_advanced_stamped_and_cleared(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins existing behaviour — must pass before AND after the fix.

        A successful fire writes four things on ``cron_schedules``:
        ``next_fire_at`` moved strictly into the future, ``last_fired_at``
        stamped with the server clock, ``last_fire_error`` cleared to NULL, and
        ``consecutive_failures`` reset to 0. ``enabled`` must be untouched.

        ``consecutive_failures`` is seeded non-zero first so "reset to 0" is a
        real observation rather than the column default.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        ids = await _seed_due_schedules(clean_pg_conn, schema, 5)
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "SET consecutive_failures = 2, last_fire_error = 'stale failure'"
        )

        before = datetime.now(UTC)
        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        rows = await clean_pg_conn.fetch(
            f"SELECT id, enabled, next_fire_at, last_fired_at, last_fire_error, "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f'consecutive_failures FROM "{schema}".cron_schedules'
        )
        assert {r["id"] for r in rows} == set(ids)
        now = datetime.now(UTC)
        for row in rows:
            assert row["enabled"] is True, "a successful fire must not touch enabled"
            assert row["next_fire_at"] > now, (
                "next_fire_at must be recomputed strictly into the future, or the "
                "schedule stays due and fires again on the next tick"
            )
            assert row["last_fired_at"] is not None, "last_fired_at must be stamped"
            assert before <= row["last_fired_at"] <= now, (
                "last_fired_at must come from the server clock taken during the tick"
            )
            assert row["last_fire_error"] is None, "a success must clear last_fire_error"
            assert row["consecutive_failures"] == 0, "a success must reset the failure count"

    async def test_disabled_and_future_schedules_are_never_fired(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins existing behaviour — must pass before AND after the fix.

        The tick's predicate is ``enabled = true AND next_fire_at <=
        clock_timestamp()``. Rows failing either half must be left bit-for-bit
        alone: no job, no ``last_fired_at``, no ``next_fire_at`` movement. This
        is the pin most at risk from a rewrite that moves the predicate into a
        CTE or adds a ``LIMIT`` with a different ``ORDER BY``.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)

        due_ids = await _seed_due_schedules(clean_pg_conn, schema, 3)

        disabled_id = new_uuid()
        future_id = new_uuid()
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "(id, actor, name, cron_expr, timezone, dst_strategy, enabled, next_fire_at) "
            "VALUES ($1, $3, 'disabled-but-due', $4, 'UTC', 'skip', false, "
            "        clock_timestamp() - interval '5 minutes'), "
            "       ($2, $3, 'enabled-but-future', $4, 'UTC', 'skip', true, "
            "        clock_timestamp() + interval '1 hour')",
            disabled_id,
            future_id,
            _CRON_ACTOR,
            _HOURLY,
        )
        untouched = await clean_pg_conn.fetch(
            f"SELECT id, next_fire_at, last_fired_at, consecutive_failures, enabled "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f'FROM "{schema}".cron_schedules WHERE id = ANY($1::uuid[]) ORDER BY id',
            [disabled_id, future_id],
        )

        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        enqueued: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert enqueued == len(due_ids), (
            f"only the {len(due_ids)} enabled+due schedules may fire; got {enqueued} "
            "jobs — a disabled schedule or a future-dated one was fired"
        )

        after = await clean_pg_conn.fetch(
            f"SELECT id, next_fire_at, last_fired_at, consecutive_failures, enabled "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f'FROM "{schema}".cron_schedules WHERE id = ANY($1::uuid[]) ORDER BY id',
            [disabled_id, future_id],
        )
        assert [dict(r) for r in after] == [dict(r) for r in untouched], (
            "rows not matching `enabled = true AND next_fire_at <= clock_timestamp()` "
            "must be untouched by the tick"
        )

    async def test_second_tick_fires_nothing_more(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins existing behaviour — must pass before AND after the fix.

        Idempotency of the advance: once a schedule has fired, its recomputed
        ``next_fire_at`` is in the future, so an immediately following tick
        finds nothing due and enqueues nothing extra. A rewrite that recomputed
        ``next_fire_at`` from the wrong seed (the Python clock instead of the
        server ``clock_timestamp()``, or the old ``next_fire_at`` instead of
        ``fire_at``) could land it in the past and produce a fire loop; this
        catches that.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        count = 5
        await _seed_due_schedules(clean_pg_conn, schema, count)

        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())
        first: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert first == count

        fired_at = await clean_pg_conn.fetch(
            f'SELECT id, last_fired_at, next_fire_at FROM "{schema}".cron_schedules ORDER BY id'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )

        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        second: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert second == count, (
            f"a second tick enqueued {second - count} extra jobs — a fired schedule "
            "must not still be due"
        )

        again = await clean_pg_conn.fetch(
            f'SELECT id, last_fired_at, next_fire_at FROM "{schema}".cron_schedules ORDER BY id'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert [dict(r) for r in again] == [dict(r) for r in fired_at], (
            "the second tick must not re-stamp any schedule row"
        )

    async def test_advisory_lock_contention_returns_without_firing(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins existing behaviour — must pass before AND after the fix.

        ``tick_cron`` opens with ``pg_try_advisory_xact_lock``; when another
        session holds it the function returns cleanly — no exception, no fire,
        no job — after recording the contention metric. This is the
        leader-handover double-fire guard, and it must stay the FIRST thing a
        tick does: a fix that moved the driving SELECT ahead of the lock, or
        that batched work before checking it, would let two leaders fire the
        same schedules concurrently.

        A second real connection holds the lock inside its own open
        transaction, which is the only way to make ``pg_try_advisory_xact_lock``
        return false.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        await _seed_due_schedules(clean_pg_conn, schema, 4)

        # Re-pointed with the schema-qualified lock names: the holder must
        # take the lock the tick actually probes,
        # schema_lock_name("cron", schema) — the pin now holds the real
        # lock, not a name that drifted from the implementation.
        from taskq.constants import schema_lock_name

        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                held: bool = await holder.fetchval(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                    schema_lock_name("cron", schema),
                )
                assert held is True, "the holder connection must take the lock first"

                counting = _CountingConn(clean_pg_conn)
                async with clean_pg_conn.transaction():
                    await tick_cron(
                        counting,  # type: ignore[arg-type]  # Why: duck-typed connection.
                        settings,
                        backend,
                        schema,
                        new_uuid(),
                    )

                assert counting.count == 1, (
                    "a contended tick must issue exactly the advisory-lock probe and "
                    f"nothing else; it issued {counting.count} statements: "
                    f"{counting.statements}"
                )
        finally:
            await holder.close()

        enqueued: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert enqueued == 0, "a tick that lost the advisory lock must fire nothing"

        unfired: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".cron_schedules WHERE last_fired_at IS NULL'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert unfired == 4, "no schedule row may be advanced by a contended tick"

    async def test_tick_writes_no_job_events_and_no_job_attempts(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins existing behaviour — must pass before AND after the fix.

        A cron tick enqueues jobs; it does not write ``job_events`` or
        ``job_attempts``. That matters for
        ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``: the 2-second
        watermark margin is violated by any ``job_events`` writer that holds a
        transaction open longer than the margin between INSERT and COMMIT, and
        a tick over a large backlog is exactly such a long transaction. Today
        it is safe only because it inserts no events — so if a fix ever adds
        per-fire event writes inside the same unbounded transaction, the
        watermark assumption breaks with a silently missed event as the
        consequence, and this test is where that shows up.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        count = 8
        await _seed_due_schedules(clean_pg_conn, schema, count)

        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        jobs: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert jobs == count

        events = await clean_pg_conn.fetch(
            f'SELECT id, job_id, occurred_at, kind, detail FROM "{schema}".job_events ORDER BY id'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert list(events) == [], (
            f"a cron tick wrote {len(events)} job_events rows "
            f"({sorted({e['kind'] for e in events})}); enqueue does not write events "
            "today, and adding per-fire event writes inside the tick's single "
            "unbounded transaction would put it over the 2-second "
            "RECLAIM_EVENT_VISIBILITY_DELAY margin"
        )

        attempts: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert attempts == 0, "a cron tick must not write job_attempts rows"

    async def test_any_job_events_written_stay_co_monotonic_and_distinct(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins existing behaviour — must pass before AND after the fix.

        ``job_events.occurred_at`` is stamped per row by ``clock_timestamp()``
        and must stay co-monotonic with the bigserial ``id`` -- the assumption
        ``RECLAIM_EVENT_VISIBILITY_DELAY``'s trailing watermark rests on -- and
        distinct per row, which is what proves the stamp really is per-row
        ``clock_timestamp()`` rather than a statement-wide ``now()`` shared
        across a batch.

        A cron tick writes no events today (pinned above), so this test states
        the invariant unconditionally over whatever the tick leaves behind: it
        passes vacuously now, and becomes a real assertion the moment a fix
        batches event writes into the tick. ``now()`` is transaction-scoped in
        Postgres, so a batched ``INSERT ... SELECT FROM unnest(...)`` that used
        it would give every row an identical ``occurred_at`` and fail the
        distinctness half here rather than silently eroding the watermark in
        production.
        """
        schema = module_pg_schema.schema_name
        backend, settings = _make_backend(schema)
        await _seed_actor_config(clean_pg_conn, schema)
        await _seed_due_schedules(clean_pg_conn, schema, 8)

        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        events = await clean_pg_conn.fetch(
            f'SELECT id, occurred_at FROM "{schema}".job_events ORDER BY id'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )

        stamps = [e["occurred_at"] for e in events]
        inversions = [
            (events[i]["id"], events[i + 1]["id"])
            for i in range(len(events) - 1)
            if stamps[i] > stamps[i + 1]
        ]
        assert inversions == [], (
            f"occurred_at inverted against ascending id at {inversions}; the "
            "poll_reclaim_events watermark assumes the two are co-monotonic"
        )
        assert len(set(stamps)) == len(stamps), (
            "occurred_at values repeat across rows — the per-row clock_timestamp() "
            "stamp was replaced by a transaction-scoped now(), which collapses the "
            "ordering the reclaim watermark reads"
        )
