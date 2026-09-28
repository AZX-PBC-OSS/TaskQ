# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""The per-attempt due-time stamp (migration 01.00.20_04): every
``job_attempts`` row carries ``due_at`` - the ``jobs.scheduled_at`` the
attempt's claim took the row against - so a retry chain's wait spans
reconstruct from the ledger alone.

Pins, matching the gates this column ships under:

- the chain-reconstruction pin: a job with two retries carries, on each
  attempt row, that attempt's OWN due time (not the next retry's
  reschedule, not the first attempt's);
- the NULL-legacy pin: pre-migration attempt rows read NULL, documented,
  never backfilled;
- the migration's idempotence (apply twice) and the archive COPY carrying
  the column (the prune sweep's column-explicit INSERT);
- the crash-reclaim, deadline and isolate paths stamping the pre-reschedule
  due time;
- both Postgres modes: the chain on vanilla and on a TimescaleDB
  hypertable conversion (the archive twin's ``started_at`` chunk key is
  untouched, so the conversion keeps working with the extra column).

NULL semantics: a due time of a historical attempt is unrecoverable -
``jobs.scheduled_at`` is overwritten by every subsequent reschedule - so
NULL means "pre-migration attempt (or a writer that could not know it)"
and no backfill is attempted. That is the documented contract the
operational-insights readers key on.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker._leader_shared import _JOB_ATTEMPTS_COLUMNS
from taskq.worker.leader import prune_terminal_jobs

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=30)

_ERROR = ErrorInfo(error_class="BoomError", error_message="boom", error_traceback=None)

# A clearly non-now due time for the first attempt: no retry reschedule
# (clock + delay, near now) can collide with it.
_FIRST_DUE = datetime.now(UTC) - timedelta(hours=1)

_RETRY_DELAY = timedelta(seconds=5)


def _enqueue_args(scheduled_at: datetime) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_uuid(),
        actor="due_at_probe",
        queue="default",
        payload={"x": 1},
        max_attempts=10,
        retry_kind="transient",
        scheduled_at=scheduled_at,
    )


async def _create_worker(conn: asyncpg.Connection, schema: str) -> Any:
    """The test's worker row plus the probe actor's registry row: dispatch
    routes by the actor's ``actor_config`` assignment, an actor without a
    row is the documented ``stranded/no_actor_config`` shape and is never
    claimable."""
    from taskq.testing.pg import create_worker as _pg_create_worker

    worker_id = new_uuid()
    await _pg_create_worker(conn, schema, worker_id)
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ('due_at_probe', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    return worker_id


async def _scheduled_at(conn: asyncpg.Connection, schema: str, job_id: JobId) -> datetime:
    row = await conn.fetchrow(f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    return row["scheduled_at"]


# ── The chain-reconstruction pin ─────────────────────────────────────


async def test_retry_chain_reconstructs_each_attempt_own_due_time(
    clean_jobs_app: JobsApp,
) -> None:
    """A job with two retries: attempts 1/2/3's due_at values are each
    attempt's own due time - attempt k's due_at is the scheduled_at
    attempt k's claim ran against, which is also attempt k+1's wait
    START, so ``due_at(k) -> started_at(k) -> due_at(k+1)`` reconstructs
    the chain. The reschedule arm (mark_retry) must stamp the PRE-reschedule
    value: the same statement's new scheduled_at belongs to the NEXT
    attempt."""
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name

    job_id = (await backend.enqueue(_enqueue_args(_FIRST_DUE))).id
    worker_id = await _create_worker(deps.worker_pool, schema)

    # Attempt 1: claimed against the enqueue's scheduled_at, retried.
    claimed = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [j.attempt for j in claimed] == [1]
    due_1 = await _scheduled_at(deps.worker_pool, schema, job_id)
    assert due_1 == _FIRST_DUE, "the claim must not touch scheduled_at"
    await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        _ERROR,
        retry_delay=_RETRY_DELAY,
        attempt=1,
        claim_epoch=1,
    )
    rescheduled_1 = await _scheduled_at(deps.worker_pool, schema, job_id)
    assert rescheduled_1 > due_1, "the retry rescheduled the row for the next attempt"

    # Make the rescheduled row due and claimable: a retried row lands
    # 'scheduled' (the delay is > 0), so the promotion sweep (the same
    # one test_full_lifecycle rides) moves it back to pending once due.
    # The backdate happens server-side and is read back, so the value
    # pinned as attempt 2's claim-time due time stays in the server's
    # clock domain.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() '
            "- interval '1 hour' WHERE id = $1",
            job_id,
        )
    await backend.scheduled_to_pending()
    # Attempt 2: claimed against the first retry's reschedule, retried.
    claimed = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [j.attempt for j in claimed] == [2]
    due_2 = await _scheduled_at(deps.worker_pool, schema, job_id)
    await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        _ERROR,
        retry_delay=_RETRY_DELAY,
        attempt=2,
        claim_epoch=2,
    )
    rescheduled_2 = await _scheduled_at(deps.worker_pool, schema, job_id)
    assert rescheduled_2 > due_2

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() '
            "- interval '1 hour' WHERE id = $1",
            job_id,
        )
    await backend.scheduled_to_pending()
    # Attempt 3: claimed against the second retry's reschedule, terminal.
    claimed = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [j.attempt for j in claimed] == [3]
    due_3 = await _scheduled_at(deps.worker_pool, schema, job_id)
    await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        _ERROR,
        retry_delay=None,  # terminal
        attempt=3,
        claim_epoch=3,
    )

    rows = {r.attempt: r for r in await backend.get_attempts(job_id)}
    assert set(rows) == {1, 2, 3}
    # THE PIN: each attempt's due_at is its OWN claim-time due time -
    # attempt 1's is the enqueue's; the retried attempts' are the values
    # their claims ran against (the pre-reschedule reads), never the
    # reschedule the retry arm wrote in the same statement.
    assert rows[1].due_at == due_1
    assert rows[2].due_at == due_2
    assert rows[2].due_at != rescheduled_1
    assert rows[3].due_at == due_3
    assert rows[3].due_at != rescheduled_2
    # And the chain reconstructs: every attempt started at/after its own
    # due time.
    for k in (1, 2, 3):
        assert rows[k].started_at >= (rows[k].due_at or _FIRST_DUE)


async def test_retry_after_consume_true_snoozed_arm_stamps_pre_reschedule_due_time(
    clean_jobs_app: JobsApp,
) -> None:
    """The OTHER reschedule arm that also writes an attempt row
    (``mark_retry_after_consume_true``'s snoozed arm) stamps the
    pre-reschedule due time, not the snooze it just scheduled."""
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name

    job_id = (await backend.enqueue(_enqueue_args(_FIRST_DUE))).id
    worker_id = await _create_worker(deps.worker_pool, schema)

    claimed = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [j.attempt for j in claimed] == [1]
    await backend.mark_retry_after(
        job_id,
        worker_id,
        delay=_RETRY_DELAY,
        consume_budget=True,
        attempt=1,
        claim_epoch=1,
    )
    next_due = await _scheduled_at(deps.worker_pool, schema, job_id)
    assert next_due > _FIRST_DUE

    rows = await backend.get_attempts(job_id)
    assert len(rows) == 1
    assert rows[0].outcome == "snoozed"
    assert rows[0].due_at == _FIRST_DUE


# ── The reclaim / deadline / isolate writers ─────────────────────────


async def test_crash_reclaim_stamps_the_pre_reschedule_due_time(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """Sweep 1's crashed attempt rows carry the claim-time due time, read
    BEFORE the re-pend arm reschedules the row."""
    from taskq.backend._sweeps import sweep_expired_locks
    from taskq.testing.pg import create_running_job

    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = new_uuid()
    await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        job_id=job_id,
        max_attempts=3,
        attempt=1,
        retry_kind="transient",
        cancel_phase=0,
    )
    # The reclaim deadlines are server-clock comparisons
    # (statement_timestamp()), so both stamps are set server-side: the
    # lock expired, and the claim's due time is a fixed, capturable
    # server value (the sweep's re-pend arm will overwrite it - that
    # overwrite is exactly what the due_at stamp must NOT read).
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET scheduled_at = statement_timestamp() '
        "- interval '2 minutes', lock_expires_at = statement_timestamp() "
        "- interval '10 seconds' WHERE id = $1",
        job_id,
    )
    due = await clean_pg_conn.fetchval(
        f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
    )

    await sweep_expired_locks(
        clean_pg_conn,
        schema=schema,
        cancel_grace=timedelta(0),
        cleanup_grace=timedelta(0),
        batch_size=10,
        max_retry_backoff=timedelta(seconds=5),
    )

    row = await clean_pg_conn.fetchrow(
        f'SELECT outcome, due_at, started_at FROM "{schema}".job_attempts '
        "WHERE job_id = $1 AND attempt = 1",
        job_id,
    )
    assert row is not None and row["outcome"] == "crashed"
    assert row["due_at"] == due, "the pre-reschedule due time, never the re-pend's"
    assert row["started_at"] is not None


async def test_deadline_sweep_stamps_the_rows_own_due_time(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """Sweep 2's synthetic attempt rows (a schedule_to_close that lapsed
    on a never-dispatched row) carry the row's own scheduled_at - the due
    time the attempt record waited against."""
    from taskq.backend._sweeps import sweep_deadline_exceeded
    from taskq.testing.pg import create_pending_job

    schema = module_pg_schema.schema_name
    job_id = new_uuid()
    await create_pending_job(clean_pg_conn, schema, job_id=job_id)
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET scheduled_at = statement_timestamp() '
        "- interval '5 minutes', schedule_to_close = statement_timestamp() "
        "- interval '1 second' WHERE id = $1",
        job_id,
    )
    due = await clean_pg_conn.fetchval(
        f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
    )

    await sweep_deadline_exceeded(clean_pg_conn, schema=schema, batch_size=10)

    row = await clean_pg_conn.fetchrow(
        f'SELECT outcome, due_at FROM "{schema}".job_attempts WHERE job_id = $1',
        job_id,
    )
    assert row is not None and row["outcome"] == "failed"
    assert row["due_at"] == due


async def test_isolate_self_stamps_the_claim_time_due_time(pg_dsn: str) -> None:
    """The isolate path's HeartbeatLost attempt row carries the claim-time
    due time: the snapshot SELECT reads scheduled_at before the arbiter's
    re-pend reschedules the row (a blocked loop has no other writer)."""
    import asyncio

    from taskq.testing.fixtures import _open_pg_backend
    from taskq.worker.heartbeat import isolate_self

    schema = "iso_due_" + new_uuid().hex[:10]
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema)
    try:
        async with deps.worker_pool.acquire() as conn:
            worker_id = await _create_worker(conn, schema)

        job_id = (await backend.enqueue(_enqueue_args(_FIRST_DUE))).id
        claimed = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert [j.attempt for j in claimed] == [1]

        shutdown = asyncio.Event()
        await isolate_self(deps, worker_id, shutdown)

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT outcome, due_at FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
        assert row is not None and row["outcome"] == "crashed"
        assert row["due_at"] == _FIRST_DUE
    finally:
        await stack.aclose()


# ── Migration: NULL legacy, idempotence, the archive COPY ────────────


async def test_pre_migration_attempts_read_null_and_stay_null(pg_dsn: str) -> None:
    """The NULL-legacy pin: an attempt row written by the PRE-vmigration
    schema reads NULL after the migration lands, and the writers leave it
    NULL (no backfill - the due time of history is unrecoverable)."""
    from taskq.migrate import discover

    schema = "legacy_due_" + new_uuid().hex[:10]
    conn = await asyncpg.connect(pg_dsn)
    try:
        # The world BEFORE 01.00.20_04: every migration up to (but
        # excluding) it, applied raw with their ledger rows recorded so
        # the runner's later apply_pending starts at the new file. One
        # attempt written by the old writers (which named no due_at
        # column) is already in the ledger.
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        migrations = discover()
        index = next(i for i, m in enumerate(migrations) if m.version == "01.00.20_04")
        for migration in migrations[:index]:
            await conn.execute(migration.sql_template.format(schema=schema))
            await conn.execute(
                f'INSERT INTO "{schema}".schema_migrations (version, checksum) VALUES ($1, $2)',
                migration.key,
                migration.checksum(schema),
            )
        worker_id = new_uuid()
        job_id = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, 'test-host', 1, '{default}')",
            worker_id,
        )
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs
            (id, actor, queue, payload, status, attempt, max_attempts, retry_kind,
             scheduled_at, started_at, finished_at)
            VALUES ($1, 'due_at_probe', 'default', '{{}}'::jsonb, 'failed', 1, 3,
                    'transient', statement_timestamp(), statement_timestamp(),
                    statement_timestamp())""",
            job_id,
        )
        await conn.execute(
            f"""INSERT INTO "{schema}".job_attempts
            (job_id, attempt, started_at, finished_at, outcome)
            VALUES ($1, 1, statement_timestamp(), statement_timestamp(), 'failed')""",
            job_id,
        )

        # The migration lands (with every later one - the runner's own
        # order), and the legacy row reads NULL.
        await apply_pending(conn, schema=schema)
        row = await conn.fetchrow(
            f'SELECT due_at FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        assert row is not None and row["due_at"] is None

        # A POST-migration attempt of the same job stamps its own due_at;
        # the legacy row stays NULL beside it (no backfill).
        await conn.execute(
            f"""INSERT INTO "{schema}".job_attempts
            (job_id, attempt, started_at, finished_at, outcome, due_at)
            VALUES ($1, 2, statement_timestamp(), statement_timestamp(), 'failed',
                    statement_timestamp())""",
            job_id,
        )
        rows = await conn.fetch(
            f'SELECT attempt, due_at FROM "{schema}".job_attempts '
            "WHERE job_id = $1 ORDER BY attempt",
            job_id,
        )
        assert [r["due_at"] is None for r in rows] == [True, False]
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_due_at_migration_applies_twice(pg_dsn: str) -> None:
    """The migration's own idempotence: running its statements a second
    time on an already-migrated schema is a no-op (``IF NOT EXISTS``)."""
    from taskq.migrate import discover

    migration = next(m for m in discover() if m.version == "01.00.20_04")
    schema = "idem_due_" + new_uuid().hex[:10]
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        # The due_at migration's statements are additive on the initial
        # schema: apply that first, then the new file twice.
        initial = next(m for m in discover() if m.version == "01.00.00_01")
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(initial.sql_template.format(schema=schema))
        await conn.execute(migration.sql_template.format(schema=schema))
        # Twice: the whole pre-phase (both tables + comments) re-runs.
        await conn.execute(migration.sql_template.format(schema=schema))
        columns = await conn.fetch(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema = $1 AND table_name = 'job_attempts'
               AND column_name = 'due_at'""",
            schema,
        )
        assert len(columns) == 1, "no duplicate column, no error"
        archive_columns = await conn.fetch(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema = $1 AND table_name = 'job_attempts_archive'
               AND column_name = 'due_at'""",
            schema,
        )
        assert len(archive_columns) == 1
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_archive_copy_carries_due_at(clean_jobs_app: JobsApp) -> None:
    """The prune sweep's column-explicit archive INSERT mirrors due_at:
    the archived attempt keeps its due time and the mirror tuple names it
    (never a bare source.*) - the twin-column discipline the archive pair
    runs under."""
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name

    assert "due_at" in _JOB_ATTEMPTS_COLUMNS, (
        "the archive mirror's column list must name the new column"
    )

    job_id = (await backend.enqueue(_enqueue_args(_FIRST_DUE))).id
    worker_id = await _create_worker(deps.worker_pool, schema)
    claimed = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [j.attempt for j in claimed] == [1]
    await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        _ERROR,
        retry_delay=None,  # terminal
        attempt=1,
        claim_epoch=1,
    )
    await deps.worker_pool.execute(
        f'UPDATE "{schema}".jobs SET finished_at = clock_timestamp() - '
        "interval '31 days' WHERE id = $1",
        job_id,
    )

    async with deps.worker_pool.acquire() as conn:
        result = await prune_terminal_jobs(
            conn,
            retention_per_status={"failed": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            schema=schema,
            batch_size=10,
        )
    assert result.archived >= 1

    async with deps.worker_pool.acquire() as conn:
        archived = await conn.fetchrow(
            f'SELECT due_at, started_at FROM "{schema}".job_attempts_archive WHERE job_id = $1',
            job_id,
        )
    assert archived is not None
    assert archived["due_at"] == _FIRST_DUE
    # No live row survived the prune: the attempt history lives only in
    # the archive tier.
    async with deps.worker_pool.acquire() as conn:
        live = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
    assert live == 0


# ── Both modes: the chain on the hypertable conversion ───────────────


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    """One timescaledb container per module; skips without Docker."""
    import os

    from testcontainers.community.postgres import PostgresContainer

    from taskq.testing._shared_containers import creator_labels, skip_test_without_docker

    skip_test_without_docker()
    image = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE", "timescale/timescaledb:2.30.1-pg18")
    with PostgresContainer(
        image=image,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def timescale_dsn(timescale_container: Any) -> str:
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


@pytest.fixture
def ts_schema() -> str:
    return "ts_due_" + new_uuid().hex[:12]


async def test_retry_chain_and_archive_on_hypertables(
    timescale_dsn: str,
    ts_schema: str,
) -> None:
    """Both-modes proof: on a TimescaleDB deployment the chain stamps
    identically, and the archive twin's hypertable conversion (chunk key
    ``started_at``) still works with the extra column - the pruned
    attempt keeps its due time in the converted archive."""

    import asyncpg as _asyncpg

    from taskq.migrate import apply_pending
    from taskq.testing.fixtures import _open_pg_backend_on_schema
    from taskq.timescale import enable_hypertables

    conn = await _asyncpg.connect(timescale_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await apply_pending(conn, schema=ts_schema)
        report = await enable_hypertables(
            conn,
            schema=ts_schema,
            settings=WorkerSettings.load_from_dict(
                {
                    "TASKQ_PG_DSN": timescale_dsn,
                    "TASKQ_SCHEMA_NAME": ts_schema,
                    "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
                    "TASKQ_ARCHIVE_RETENTION_PERIOD": "172800s",
                    "TASKQ_EVENT_RETENTION_PERIOD": "86400s",
                }
            ),
        )
    finally:
        await conn.close()
    assert "job_attempts_archive" in report.converted

    stack, deps, backend = await _open_pg_backend_on_schema(timescale_dsn, ts_schema)
    try:
        async with deps.worker_pool.acquire() as c:
            worker_id = await _create_worker(c, ts_schema)

        job_id = (await backend.enqueue(_enqueue_args(_FIRST_DUE))).id
        claimed = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert [j.attempt for j in claimed] == [1]
        await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            _ERROR,
            retry_delay=_RETRY_DELAY,
            attempt=1,
            claim_epoch=1,
        )
        due_2 = await _scheduled_at(deps.worker_pool, ts_schema, job_id)
        # Backdate out of the retry's server-clock delay and promote, the
        # same treatment the vanilla chain pin applies.
        async with deps.worker_pool.acquire() as c:
            await c.execute(
                f'UPDATE "{ts_schema}".jobs SET scheduled_at = clock_timestamp() '
                "- interval '1 hour' WHERE id = $1",
                job_id,
            )
        await backend.scheduled_to_pending()
        claimed = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        assert [j.attempt for j in claimed] == [2]
        due_2 = await _scheduled_at(deps.worker_pool, ts_schema, job_id)
        await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            _ERROR,
            retry_delay=None,
            attempt=2,
            claim_epoch=2,
        )

        rows = {r.attempt: r for r in await backend.get_attempts(job_id)}
        assert rows[1].due_at == _FIRST_DUE
        assert rows[2].due_at == due_2

        # Archive through the conversion: the column-explicit COPY keeps
        # the due time on the hypertable.
        async with deps.worker_pool.acquire() as c:
            await c.execute(
                f'UPDATE "{ts_schema}".jobs SET finished_at = clock_timestamp() - '
                "interval '31 days' WHERE id = $1",
                job_id,
            )
            result = await prune_terminal_jobs(
                c,
                retention_per_status={"failed": timedelta(days=30)},
                archive_retention=timedelta(days=365),
                schema=ts_schema,
                batch_size=10,
            )
        assert result.archived >= 1
        async with deps.worker_pool.acquire() as c:
            archived = await c.fetchrow(
                f'SELECT due_at FROM "{ts_schema}".job_attempts_archive WHERE job_id = $1',
                job_id,
            )
        assert archived is not None and archived["due_at"] == _FIRST_DUE
    finally:
        await stack.aclose()
