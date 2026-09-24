from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple
from uuid import UUID

from taskq._ids import new_uuid
from taskq._json import dumps_str
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.testing.assertions import parse_detail

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy

__all__ = [
    "DEFAULT_ACTORS",
    "JobTriple",
    "RowVisitCounter",
    "create_pending_job",
    "create_running_job",
    "create_worker",
    "create_workered_running_job",
    "get_job_triple",
    "install_row_visit_counter",
    "parse_detail",
    "read_row_visits",
    "reset_schema",
    "seed_actors",
    "setup_running_job",
    "truncate_schema",
]

# Role the row-visit counter runs statements as. Row-level security is
# bypassed for a table's owner and for superusers, so a counting policy
# only fires for an ordinary role. Cluster-wide (roles are not
# schema-scoped) and reused across tests; the policy and sequence that
# actually isolate one test from another are per schema.
#
# The role is created, never dropped. A teardown that dropped it would
# break any concurrent worker sitting between a ``SET ROLE`` and the
# matching ``RESET ROLE``, and the suite's xdist workers share one
# cluster. Creation itself is safe under that concurrency:
# :func:`install_row_visit_counter` serialises it with a
# transaction-scoped advisory lock, because a check-then-create inside a
# single DO block is atomic per statement but not across connections.
ROW_VISIT_COUNTER_ROLE = "taskq_row_visit_probe"


async def _create_worker(
    conn: _Conn,
    schema: str,
    worker_id: UUID,
) -> None:
    """Insert a worker row used by integration tests that exercise leader
    election or per-attempt history (both still FK to workers(id)).
    ``jobs.locked_by_worker`` is not an FK, so
    tests that only dispatch/lock jobs do not strictly need this, it is
    kept for tests that also write ``job_attempts`` or ``maintenance_leader``.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608
        worker_id,
        "test-host",
        12345,
        ["default"],
    )


create_worker = _create_worker

# Default actor_config rows seeded into every test schema.
# The dispatch CTE requires explicit rows in actor_config, even uncapped
# actors need an entry.  Test authors can override via seed_actors() or
# pass their own list to reset_schema().
DEFAULT_ACTORS: tuple[str, ...] = (
    "actor_a",
    "actor_b",
    "actor_c",
    "A",
    "C",
    "X",
    "test_actor",
    "_progress_redis_hundred",
    "_progress_redis_single",
    "_progress_redis_three",
)

# Tables truncated by truncate_schema() in FK-safe cascade order.
# job_attempts_archive is listed BEFORE jobs_archive: on vanilla Postgres
# the CASCADE from jobs_archive would reach it anyway, but the optional
# hypertable mode drops that foreign key (no table may reference a
# hypertable), so the explicit entry keeps the reset complete in both
# modes. schema_migrations is excluded, migration metadata is not test data.
_TRUNCATE_TABLES: tuple[str, ...] = (
    "reservation_slots",
    "rate_limit_window_entries",
    "rate_limit_buckets",
    "cron_schedules",
    "job_attempts_archive",
    "jobs_archive",
    "jobs",
    "workers",
    "actor_config",
    "queues",
    # The admin audit trail is test data like any other: a suite that
    # asserts "exactly one row" after one mutation must not see the
    # previous test's rows. Truncating it is also the per-test story an
    # audit-asserting suite needs; nothing else reads it.
    "admin_audit",
    # The event-prune watermark is mutable test state too: a retention-gap
    # pin asserts a cursor reads 0 before its own deleter ran, so the
    # previous test's advanced bound must not leak. The migration seeds the
    # singleton row; after a truncate the read path answers 0 (NULL row)
    # and the next deleter re-seeds it through its UPSERT.
    "job_events_prune_state",
)


_migrated_triggers: dict[str, frozenset[tuple[str, str]]] = {}
"""Per-schema trigger set as the migrations left it, captured on first reset.

A test that installs a trigger, a commit-time constraint trigger to make
a COMMIT fail, say, changes the schema's DDL, which no TRUNCATE undoes.
The next test on the module's shared schema then meets a rule it never
asked for, so its failure reads as a defect in the code under test.  The
snapshot is what lets a reset put the DDL back.
"""


async def _reset_triggers(conn: _Conn, schema: str) -> None:
    """Drop triggers on the dynamic tables that the migrations did not
    install, restoring the schema's DDL to its migrated state.

    The first call for a schema records the migrated set instead, it runs
    in per-test setup, before any test body can add one.

    The trigger and table names come from the catalog and are interpolated
    into the DROP, so they pass the project's identifier validation first
    (the same rule every user-sourced identifier follows): a name the rule
    cannot admit fails loudly here rather than being interpolated raw ,
    a quote inside it would break out of the quoted identifier.
    """
    rows = await conn.fetch(
        "SELECT c.relname AS table_name, t.tgname AS trigger_name "
        "FROM pg_trigger t "
        "JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND NOT t.tgisinternal AND c.relname = ANY($2::text[])",
        schema,
        list(_TRUNCATE_TABLES),
    )
    present = {(str(row["table_name"]), str(row["trigger_name"])) for row in rows}
    migrated = _migrated_triggers.get(schema)
    if migrated is None:
        _migrated_triggers[schema] = frozenset(present)
        return
    extra = sorted(present - migrated)
    # Validate the whole drop list before issuing any DROP: a reset that
    # fails halfway leaves the schema in neither state.
    for table, trigger in extra:
        if not _IDENT_RE.match(table) or not _IDENT_RE.match(trigger):
            raise ValueError(f"invalid trigger identifier {trigger!r} on table {table!r}")
    for table, trigger in extra:
        await conn.execute(f'DROP TRIGGER "{trigger}" ON "{schema}"."{table}"')


async def truncate_schema(conn: _Conn, schema: str) -> None:
    """Truncate all dynamic tables in FK-safe order using CASCADE, and drop
    triggers a previous test added to them.

    Leaves ``schema_migrations`` intact.  Safe to call repeatedly.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    for table in _TRUNCATE_TABLES:
        await conn.execute(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
    await _reset_triggers(conn, schema)


async def seed_actors(
    conn: _Conn,
    schema: str,
    *,
    actors: Sequence[str] | None = None,
) -> None:
    """Insert actor_config rows for the given actors (or DEFAULT_ACTORS).

    ``ON CONFLICT (actor) DO NOTHING`` makes this safe to call
    alongside custom seed data, it never overwrites existing rows.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    target = actors if actors is not None else DEFAULT_ACTORS
    await conn.executemany(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',  # noqa: S608
        [(actor, "default") for actor in target],
    )


async def reset_schema(
    conn: _Conn,
    schema: str,
    *,
    actors: Sequence[str] | None = None,
) -> None:
    """Truncate all dynamic tables then seed default actor_config rows.

    Tests needing a custom actor set can pass ``actors=[...]``;
    tests that need an empty actor_config can pass ``actors=[]``.
    """
    await truncate_schema(conn, schema)
    await seed_actors(conn, schema, actors=actors)


class JobTriple(NamedTuple):
    row: asyncpg.Record
    attempts: list[asyncpg.Record]
    events: list[asyncpg.Record]


async def create_running_job(
    conn: _Conn,
    schema: str,
    worker_id: UUID,
    job_id: UUID | None = None,
    *,
    cancel_phase: int = 0,
    max_attempts: int = 3,
    retry_kind: str = "transient",
    attempt: int = 1,
    cancel_requested_at: datetime | None = None,
    lock_expires_at: datetime | None = None,
    schedule_to_close: datetime | None = None,
    with_events: bool = True,
) -> UUID:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    job_id = job_id or new_uuid()
    expires_at = lock_expires_at or (datetime.now(UTC) + timedelta(seconds=60))
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, attempt, claim_epoch, scheduled_at,
            locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,
            cancel_phase, cancel_requested_at, schedule_to_close
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            'running', 0, $7, $13, clock_timestamp(),
            $8, $9, clock_timestamp(), clock_timestamp(),
            $10, $11, $12
        )""",  # noqa: S608
        job_id,
        "test_actor",
        "default",
        '{"key": "value"}',
        max_attempts,
        retry_kind,
        attempt,
        worker_id,
        expires_at,
        cancel_phase,
        cancel_requested_at,
        schedule_to_close,
        # The epoch seed models the refund-free history this helper
        # represents: a row that reached attempt N through N claims, each
        # of which stamped attempt and claim_epoch together, so the row's
        # epoch is N and the fences under test are satisfiable by
        # presenting the row's own values. attempt and claim_epoch are
        # distinct counters that DO diverge in production (the attempt
        # saturates at the smallint ceiling and non-terminal releases
        # refund it; the epoch only ever increments, so the row's epoch is
        # always >= its attempt); the equality fences treat a diverged row
        # identically, and a test that needs a diverged shape seeds the
        # two columns separately.
        attempt,
    )
    if with_events:
        detail = dumps_str(
            {"from_state": "pending", "to_state": "running", "worker_id": str(worker_id)}
        )
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail) '  # noqa: S608
            "VALUES ($1, $2, 'state_change', $3::jsonb)",
            job_id,
            now,
            detail,
        )
    return job_id


async def create_pending_job(
    conn: _Conn,
    schema: str,
    job_id: UUID | None = None,
    *,
    schedule_to_close: datetime | None = None,
    status: str = "pending",
    scheduled_at: datetime | None = None,
) -> UUID:
    """Seed one job row directly. ``scheduled_at`` defaults to the
    application clock's ``now()``, a stamp a claim CTE comparing against
    the database's ``statement_timestamp()`` reads as not-yet-due whenever
    the database clock lags the application clock (Docker VM pause and NTP
    drift both cause it). Seed a past margin or an explicit ``scheduled_at``
    whenever the test then asserts the row is claimable.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    job_id = job_id or new_uuid()
    stc = schedule_to_close or (datetime.now(UTC) + timedelta(seconds=60))
    sa = scheduled_at or datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            $7, 0, $8, $9
        )""",  # noqa: S608
        job_id,
        "test_actor",
        "default",
        '{"key": "value"}',
        3,
        "transient",
        status,
        sa,
        stc,
    )
    return job_id


async def get_job_triple(conn: _Conn, schema: str, job_id: UUID) -> JobTriple:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    row = await conn.fetchrow(
        f'SELECT * FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
        job_id,
    )
    assert row is not None
    attempts = await conn.fetch(
        f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1',  # noqa: S608
        job_id,
    )
    events = await conn.fetch(
        f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',  # noqa: S608
        job_id,
    )
    return JobTriple(row=row, attempts=list(attempts), events=list(events))


async def setup_running_job(
    conn: _Conn,
    schema: str,
    *,
    worker_id: UUID | None = None,
    job_id: UUID | None = None,
    attempt: int = 1,
    max_attempts: int = 3,
    retry_kind: str = "transient",
    cancel_phase: int = 0,
    cancel_requested_at: datetime | None = None,
    lock_expires_at: datetime | None = None,
    schedule_to_close: datetime | None = None,
    with_events: bool = True,
) -> tuple[UUID, UUID]:
    """Create a worker row and a running job row in one call.

    Returns ``(worker_id, job_id)``.  Delegates to
    :func:`create_workered_running_job`.
    """
    return await create_workered_running_job(
        conn,
        schema,
        worker_id=worker_id,
        job_id=job_id,
        cancel_phase=cancel_phase,
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        attempt=attempt,
        cancel_requested_at=cancel_requested_at,
        lock_expires_at=lock_expires_at,
        schedule_to_close=schedule_to_close,
        with_events=with_events,
    )


async def create_workered_running_job(
    conn: _Conn,
    schema: str,
    *,
    worker_id: UUID | None = None,
    **job_kwargs: Any,
) -> tuple[UUID, UUID]:
    """Create a worker row and a running job row, returning ``(worker_id, job_id)``.

    Passthrough wrapper: creates a worker (generating a UUID if none provided),
    then creates a running job belonging to that worker.  All extra keyword
    arguments are forwarded to :func:`create_running_job`.
    """
    wid = worker_id or new_uuid()
    await _create_worker(conn, schema, wid)
    jid = await create_running_job(conn, schema, wid, **job_kwargs)
    return wid, jid


# ── Row-visit counting (cost oracles) ────────────────────────────────────


def _create_role_or_reuse_do_block(role: str) -> str:
    """A DO block that creates *role* if absent, or reuses the winner's.

    The guard exists because role creation is cluster-wide while the
    advisory lock that serialises installs is per-database: two xdist
    workers on different databases of one cluster can both see the role
    as absent, and the loser of the concurrent CREATE ROLE dies on
    pg_authid_rolname_index with UniqueViolationError (23505) or
    DuplicateObjectError (42710). Both mean the winner's CREATE ROLE
    committed, so the guard re-checks the role and proceeds with the
    winner's; it re-raises when the role genuinely is absent, so real
    breakage still surfaces.
    """
    # Why: CREATE ROLE takes no parameters, so the role name must be interpolated; the caller passes this module's own constant or a pin test's scratch name, never external input.
    return (
        "DO $$ BEGIN "  # noqa: S608
        f"  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') "
        "  THEN BEGIN "
        f"    CREATE ROLE {role} NOLOGIN; "
        "    EXCEPTION "
        "    WHEN duplicate_object OR unique_violation THEN "
        f"    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') "
        "    THEN RAISE; END IF; "
        "  END; END IF; "
        "END $$"
    )


async def install_row_visit_counter(conn: _Conn, schema: str, table: str = "jobs") -> None:
    """Make *table* count every row the engine actually visits.

    Installs a permissive row-level-security policy whose USING expression
    bumps a sequence. PostgreSQL evaluates that expression once per row it
    reads from the table, so the sequence advances by exactly the number
    of rows a statement looked at -- including the rows it looked at and
    then discarded, which is the number a cost oracle wants.

    This exists so a test can assert "this operation's cost is bounded by
    its batch, not by the table" as **observable behaviour**, without any
    of the usual brittleness:

    * **Survives engine upgrades.** It reads no ``EXPLAIN`` output, so no
      plan node name, plan shape, or counter spelling can change under
      it. Plan text is not a stable interface across PostgreSQL major
      versions; "rows the engine read" is.
    * **Tests behaviour, not implementation.** It needs no knowledge of
      the SQL under test -- it is not parsed, re-spelled, re-bound or
      re-run -- so it follows any rewrite that keeps cost bounded and
      fails any that does not.
    * **Deterministic.** An exact count: no clock, no buffer-cache state,
      no machine-speed dependence, nothing to make it flaky.

    Use it for any bounded-cost contract where a drain, sweep or scan
    must not re-read what it already processed -- the failure mode where
    nothing errors and no count is wrong, but the work grows with the
    backlog until the operation stops finishing.

    The policy is permissive and always true, so it changes visibility not
    at all. Statements must run as :data:`ROW_VISIT_COUNTER_ROLE` for it
    to fire; :class:`RowVisitCounter` arranges that.

    Safe to call from concurrent processes against one cluster (the
    suite's xdist workers do exactly that): role creation survives two
    concurrent first-installs. The transaction-scoped advisory lock
    serialises callers in the same database. Across databases the lock
    cannot help: advisory locks are per-database while CREATE ROLE is
    cluster-wide, so the create is additionally guarded: the loser of
    the cluster-level duplicate catches the duplicate and reuses the
    winner's role.
    """
    if not _IDENT_RE.match(schema):  # pragma: no cover - guards a test-only helper
        raise ValueError(f"invalid schema identifier: {schema!r}")
    if not _IDENT_RE.match(table):  # pragma: no cover - guards a test-only helper
        raise ValueError(f"invalid table identifier: {table!r}")
    await conn.execute(f'CREATE SEQUENCE IF NOT EXISTS "{schema}".taskq_rows_visited')
    # VOLATILE with a high COST so the planner never folds it away,
    # caches it, or hoists it above the scan it is counting.
    await conn.execute(
        f'CREATE OR REPLACE FUNCTION "{schema}".taskq_count_row_visit() RETURNS boolean '
        f"AS $$ SELECT nextval('\"{schema}\".taskq_rows_visited') IS NOT NULL $$ "
        "LANGUAGE sql VOLATILE COST 10000"
    )
    await conn.execute(f'ALTER TABLE "{schema}".{table} ENABLE ROW LEVEL SECURITY')
    await conn.execute(f'DROP POLICY IF EXISTS taskq_count_row_visits ON "{schema}".{table}')
    await conn.execute(
        f'CREATE POLICY taskq_count_row_visits ON "{schema}".{table} '
        f'USING ("{schema}".taskq_count_row_visit())'
    )
    # Creating the role is the one cluster-level statement here, and the
    # suite's xdist workers share one Postgres cluster across per-module
    # databases. Two guards make the check-then-create safe. First, the
    # transaction-scoped advisory lock serialises same-database callers,
    # so a loser that shares the winner's database waits, re-reads the
    # role after the winner commits, and reuses it. Second, the guard is
    # needed because advisory locks are per-database while CREATE ROLE is
    # cluster-wide: two workers on different databases of one cluster are
    # never serialised by the lock, both see the role as absent, and the
    # loser of the index insert dies on pg_authid_rolname_index with
    # UniqueViolationError. The guard in _create_role_or_reuse_do_block
    # catches that duplicate, re-checks the role, and reuses the winner's
    # role; it re-raises when the role genuinely is absent, so real
    # breakage still surfaces.
    async with conn.transaction():
        await conn.execute(
            # Why: the lock key derives from this module's own role-name constant, never caller input.
            f"SELECT pg_advisory_xact_lock(hashtext('{ROW_VISIT_COUNTER_ROLE}'), 0)"
        )
        await conn.execute(_create_role_or_reuse_do_block(ROW_VISIT_COUNTER_ROLE))
    await conn.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO {ROW_VISIT_COUNTER_ROLE}')
    await conn.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA "{schema}" TO {ROW_VISIT_COUNTER_ROLE}')
    await conn.execute(
        f'GRANT ALL ON ALL SEQUENCES IN SCHEMA "{schema}" TO {ROW_VISIT_COUNTER_ROLE}'
    )


async def read_row_visits(conn: _Conn, schema: str) -> int:
    """Rows visited so far, per :func:`install_row_visit_counter`."""
    return int(
        await conn.fetchval(
            f'SELECT last_value FROM "{schema}".taskq_rows_visited'  # noqa: S608  # Why: a sequence name cannot be $N-bound; `schema` is validated against _IDENT_RE in install_row_visit_counter before any of this runs.
        )
        or 0
    )


class RowVisitCounter:
    """Pool stand-in recording how many rows each statement visited.

    Wraps a real pool and hands out real pooled connections, so the code
    under test runs untouched against the real engine; this only samples
    the counting sequence either side of each statement it issues.

    ``per_statement`` ends up with one entry per driving statement, in
    order -- so a bounded drain shows a flat list and an unbounded one
    shows a list that climbs.

    Requires :func:`install_row_visit_counter` to have been called for
    *schema* first.
    """

    def __init__(self, pool: Any, schema: str, *, method: str = "fetchrow") -> None:
        self._pool = pool
        self._schema = schema
        self._method = method
        self.per_statement: list[int] = []
        # Row visits grouped by the statement text that caused them, so a
        # caller that issues more than one distinct driving statement (the
        # bulk cancel runs a terminal arm and then a cooperative-cancel
        # arm) can assert each one's cost against its own contract instead
        # of against a concatenation of both.
        self.by_statement: dict[str, list[int]] = {}

    def acquire(self, **kwargs: object) -> Any:
        outer = self

        class _Acquire:
            async def __aenter__(self) -> Any:
                self._ctx = outer._pool.acquire(**kwargs)
                self._conn = await self._ctx.__aenter__()
                # RLS is bypassed for the table owner and for superusers,
                # so the counting policy only fires for an ordinary role.
                await self._conn.execute(f"SET ROLE {ROW_VISIT_COUNTER_ROLE}")
                return _CountingConnection(self._conn, outer)

            async def __aexit__(self, *exc: object) -> Any:
                # Hand the pooled connection back exactly as it was found.
                await self._conn.execute("RESET ROLE")
                return await self._ctx.__aexit__(*exc)

        return _Acquire()


class _CountingConnection:
    """Delegates everything to a real connection, counting row visits
    around the one statement-issuing method under measurement."""

    def __init__(self, inner: Any, counter: RowVisitCounter) -> None:
        self._inner = inner
        self._counter = counter

    def __getattr__(self, name: str) -> Any:
        inner_attr = getattr(self._inner, name)
        if name != self._counter._method:
            return inner_attr

        async def _measured(*args: object, **kwargs: object) -> Any:
            counter = self._counter
            schema = counter._schema
            before = await read_row_visits(self._inner, schema)
            result = await inner_attr(*args, **kwargs)
            after = await read_row_visits(self._inner, schema)
            visits = after - before
            counter.per_statement.append(visits)
            sql = str(args[0]) if args else ""
            counter.by_statement.setdefault(sql, []).append(visits)
            return result

        return _measured
