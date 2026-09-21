"""An archived job must answer through the client API, not vanish.

Issue #314: after a real prune moved a terminal job to ``jobs_archive``,
``backend.get`` read the hot ``jobs`` table only, so the Python API
reported the job as if it never existed: ``JobHandle.status()`` /
``refresh()`` / ``wait()`` raised a bare ``KeyError`` and
``JobsClient.get()`` returned ``None``, while the row sat in the archive
tier and ``taskq job show`` (the CLI) answered correctly from it.

The contract pinned here, mirroring the CLI's jobs-then-archive fallback:
a ``get`` for an id that lives only in ``jobs_archive`` returns that row
with ``archived=True`` on the row (and on the handle), and a
never-existed id keeps the documented missing-job behavior (``None``
from ``get`` / ``get_row``, ``KeyError`` from the handle read-backs).
"""

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from pydantic import TypeAdapter

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import JobId, JobRow
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client._handle import JobHandle
from taskq.client._jobs import JobsClient
from taskq.settings import TaskQSettings
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.settings import make_integration_settings_dict
from taskq.worker._leader_shared import prune_terminal_jobs

pytestmark = pytest.mark.integration

_NONE_ADAPTER = TypeAdapter(type(None))


async def _seed_terminal_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str = "succeeded",
    job_id: JobId | None = None,
    attempt: int = 0,
) -> JobId:
    """One terminal job already past any retention, ready for the prune."""
    jid = job_id if job_id is not None else new_job_id()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close,
            finished_at, metadata, payload_schema_ver, attempt
        ) VALUES (
            $1, 'test_actor', 'default', '{{"v": 1}}'::jsonb, 3, 'transient',
            $2::{schema}.job_status, 0, $3, $4,
            $5, '{{}}'::jsonb, 1, $6
        )""",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        jid,
        status,
        now,
        now + timedelta(hours=1),
        now - timedelta(minutes=5),
        attempt,
    )
    return jid


async def _prune_into_archive(
    conn: asyncpg.Connection,
    schema: str,
    job_id: JobId,
) -> None:
    """A real prune (not a hand INSERT into jobs_archive): the issue's
    table state is 'the row the prune left behind', so the test produces
    it the way production does."""
    result = await prune_terminal_jobs(
        conn,
        retention_per_status={status: timedelta(0) for status in TERMINAL_STATUSES},
        archive_retention=timedelta(days=365),
        schema=schema,
    )
    assert result.archived >= 1, (
        f"the prune archived nothing ({result!r}); the scenario requires the "
        "seeded job to have been moved to jobs_archive"
    )
    remaining = await conn.fetchval(
        f"SELECT count(*) FROM {schema}.jobs WHERE id = $1",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        job_id,
    )
    assert remaining == 0, "the seeded job must have left the hot table"
    stored = await conn.fetchval(
        f"SELECT count(*) FROM {schema}.jobs_archive WHERE id = $1",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        job_id,
    )
    assert stored == 1


@pytest.fixture
def client_settings(module_pg_schema: ModulePgSchema) -> TaskQSettings:
    """Settings pointing at the module's migrated schema (for JobsClient)."""
    settings = TaskQSettings.load_from_dict(make_integration_settings_dict(module_pg_schema.pg_dsn))
    settings.schema_name = module_pg_schema.schema_name
    return settings


@pytest.mark.integration
async def test_backend_get_returns_the_archived_row(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
    client_settings: TaskQSettings,
) -> None:
    """``backend.get`` answers an archived id from jobs_archive, marked."""
    _deps, backend = clean_jobs_app
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await _seed_terminal_job(conn, module_pg_schema.schema_name)
        await _prune_into_archive(conn, module_pg_schema.schema_name, job_id)
    finally:
        await conn.close()

    row = await backend.get(job_id)
    assert row is not None, (
        "the job was archived, not destroyed: get() reported an existing job "
        "as if it never existed (issue #314)"
    )
    assert row.id == job_id
    assert row.status == "succeeded"
    assert row.archived is True


@pytest.mark.integration
async def test_client_get_and_handle_status_answer_from_the_archive_tier(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
    client_settings: TaskQSettings,
) -> None:
    """``JobsClient.get`` returns the archived job's handle, flagged.

    Before the fix this returned ``None`` and the handle read-backs below
    raised a bare ``KeyError`` -- the archive tier was invisible to the
    Python API while the CLI answered from it.
    """
    _deps, backend = clean_jobs_app
    client = JobsClient(backend, settings=client_settings)
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await _seed_terminal_job(conn, module_pg_schema.schema_name)
        await _prune_into_archive(conn, module_pg_schema.schema_name, job_id)
    finally:
        await conn.close()

    handle = await client.get(job_id)
    assert handle is not None, (
        "client.get() returned None for an archived job: callers holding an "
        "id across a prune lost the job entirely (issue #314)"
    )
    assert handle.archived is True
    assert await handle.status() == "succeeded"
    row = await handle.refresh()
    assert row.archived is True
    assert handle.row.archived is True


@pytest.mark.integration
async def test_archived_handle_wait_returns_without_key_error(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
    client_settings: TaskQSettings,
) -> None:
    """``wait()`` on an archived succeeded job resolves instead of raising.

    The polling loop reads through the same backend ``get``; an archived
    terminal row is a perfectly good terminal answer.
    """
    _deps, backend = clean_jobs_app
    client = JobsClient(backend, settings=client_settings)
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await _seed_terminal_job(conn, module_pg_schema.schema_name)
        await _prune_into_archive(conn, module_pg_schema.schema_name, job_id)
    finally:
        await conn.close()

    handle = await client.get(job_id)
    assert handle is not None
    assert await handle.wait() is None


@pytest.mark.integration
async def test_a_live_job_is_not_marked_archived(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The flag marks the archive tier, not age: a hot row reads False."""
    _deps, backend = clean_jobs_app
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await _seed_terminal_job(conn, module_pg_schema.schema_name)
    finally:
        await conn.close()

    row = await backend.get(job_id)
    assert row is not None
    assert row.archived is False


@pytest.mark.integration
async def test_a_never_existed_id_keeps_the_documented_missing_behavior(
    clean_jobs_app: JobsApp,
    client_settings: TaskQSettings,
) -> None:
    """The fallback must not turn 'no such job' into a finding: an id in
    neither tier is still ``None`` from the reads and ``KeyError`` from
    the handle read-backs, exactly as documented."""
    _deps, backend = clean_jobs_app
    client = JobsClient(backend, settings=client_settings)
    missing = new_job_id()

    assert await backend.get(missing) is None
    assert await client.get(missing) is None
    assert await client.get_row(missing) is None

    phantom = JobRow(
        id=missing,
        actor="test_actor",
        queue="default",
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime.now(UTC),
        scheduled_at=datetime.now(UTC),
    )
    handle = JobHandle(
        client=client,
        row=phantom,
        result_adapter=_NONE_ADAPTER,
        was_existing=False,
    )
    with pytest.raises(KeyError):
        await handle.status()
    with pytest.raises(KeyError):
        await handle.refresh()


async def _seed_attempt(
    conn: asyncpg.Connection,
    schema: str,
    job_id: JobId,
    *,
    attempt: int = 1,
) -> None:
    """One succeeded attempt row for *job_id*, the history a prune moves
    to ``job_attempts_archive`` alongside the job row."""
    started = datetime.now(UTC)
    worker_id = new_uuid()
    await conn.execute(
        f"""INSERT INTO {schema}.workers (id, hostname, pid, queues)
        VALUES ($1, 'rt-archived-reads', 1, '{{default}}')""",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        worker_id,
    )
    await conn.execute(
        f"""INSERT INTO {schema}.job_attempts (
            job_id, attempt, started_at, finished_at, outcome,
            worker_id, metadata
        ) VALUES (
            $1, $2, $3, $4, 'succeeded', $5, '{{}}'::jsonb
        )""",  # noqa: S608  # Why: schema is fixture-derived; every value is $N-bound
        job_id,
        attempt,
        started,
        started + timedelta(seconds=1),
        worker_id,
    )


@pytest.mark.integration
async def test_get_attempts_answers_an_archived_jobs_attempt_history(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
    client_settings: TaskQSettings,
) -> None:
    """``get_attempts`` for an archived job answers the moved attempt
    history, not an empty list.

    The prune moves the job's attempt rows to ``job_attempts_archive``
    in the same statement that moves the job row to ``jobs_archive``.
    The get fallback (issue #314) made the ROW honest - ``handle.row``
    reports ``attempt=1`` - while ``get_attempts`` kept reading the hot
    ``job_attempts`` table only, so the same handle reported a job with
    an attempt count and NO attempt history: the row said it ran, the
    history said it never did.
    """
    _deps, backend = clean_jobs_app
    client = JobsClient(backend, settings=client_settings)
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await _seed_terminal_job(conn, module_pg_schema.schema_name, attempt=1)
        await _seed_attempt(conn, module_pg_schema.schema_name, job_id)
        await _prune_into_archive(conn, module_pg_schema.schema_name, job_id)
    finally:
        await conn.close()

    row = await backend.get(job_id)
    assert row is not None
    assert row.archived is True
    assert row.attempt >= 1, "the archived row carries the attempt count"

    attempts = await backend.get_attempts(job_id)
    assert attempts, (
        "the archived job's attempt history was moved to "
        "job_attempts_archive by the prune, but get_attempts answered "
        "empty: the row claims an attempt happened, the history claims "
        "nothing did"
    )
    assert attempts[0].attempt == 1
    assert attempts[0].outcome == "succeeded"
    assert all(a.job_id == job_id for a in attempts)

    handle = await client.get(job_id)
    assert handle is not None
    assert await handle.attempts(), (
        "JobHandle.attempts() must see the archived attempt history the same handle's row counts"
    )
