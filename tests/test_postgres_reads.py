"""Integration tests for PostgresBackend read methods against real PG.

Covers ``get``, ``list_jobs``, and ``get_attempts``.

Test IDs.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._cursor import encode_cursor
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, JobFilter
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


async def _enqueue_job(
    backend: object,
    *,
    actor: str = "test_actor",
    queue: str = "default",
    idempotency_key: str | None = None,
    payload: dict[str, object] | None = None,
    scheduled_at: datetime | None = None,
) -> object:
    """Enqueue a job via the backend and return the row."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload=payload or {"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=scheduled_at or datetime.now(UTC),
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key is not None else None,
    )
    from taskq.backend._protocol import Backend

    b: Backend = backend  # type: ignore[assignment] # Why: backend is typed as object in clean_jobs_app; actual type satisfies Backend protocol
    return await b.enqueue(args)


# ── get ────────────────────────────────────────────────────────────────


class TestGet:
    """``get`` returns the row when it exists; returns ``None`` when it
    doesn't."""

    async def test_get_existing_job(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        row = await _enqueue_job(backend)
        result = await backend.get(row.id)  # type: ignore[union-attr] # Why: row is JobRow returned from enqueue
        assert result is not None
        assert result.id == row.id  # type: ignore[union-attr]
        assert result.actor == "test_actor"  # type: ignore[union-attr]

    async def test_get_missing_job_returns_none(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        result = await backend.get(new_job_id())
        assert result is None


# ── list_jobs ──────────────────────────────────────────────────────────


class TestListJobs:
    """``list_jobs`` honours the ``JobFilter`` (status, queue, pagination)."""

    async def test_list_jobs_returns_all_by_default(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        await _enqueue_job(backend, queue="q1")
        await _enqueue_job(backend, queue="q2")
        await _enqueue_job(backend, queue="q1")

        rows = await backend.list_jobs(JobFilter())
        assert len(rows) == 3

    async def test_filter_by_queue(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        await _enqueue_job(backend, queue="alpha")
        await _enqueue_job(backend, queue="beta")
        await _enqueue_job(backend, queue="alpha")

        rows = await backend.list_jobs(JobFilter(queue="alpha"))
        assert len(rows) == 2
        assert all(r.queue == "alpha" for r in rows)

    async def test_filter_by_status(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        # Enqueue a pending job
        await _enqueue_job(backend)

        # Manually insert a running job to have a different status
        worker_id = new_uuid()
        job_id = new_job_id()
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',
                worker_id,
                "test-host",
                12345,
                ["default"],
            )
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, "
                "status, priority, attempt, scheduled_at, "
                "locked_by_worker, lock_expires_at, started_at, last_heartbeat_at) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                "'running', 0, 1, now(), "
                "$7, now() + interval '60 seconds', now(), now())",
                job_id,
                "running_actor",
                "default",
                "{}",
                3,
                "transient",
                worker_id,
            )

        rows = await backend.list_jobs(JobFilter(status="running"))
        assert len(rows) >= 1
        assert all(r.status == "running" for r in rows)

    async def test_filter_by_actor(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        await _enqueue_job(backend, actor="actor_a")
        await _enqueue_job(backend, actor="actor_b")

        rows = await backend.list_jobs(JobFilter(actor="actor_a"))
        assert len(rows) == 1
        assert rows[0].actor == "actor_a"

    async def test_filter_by_batch_id(self, clean_jobs_app: JobsApp) -> None:
        """batch_id filter matches jobs whose metadata contains the key."""

        backend = clean_jobs_app.backend

        # Enqueue with batch_id in metadata
        args_a = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            metadata={"batch_id": "batch-1"},
        )
        args_b = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            metadata={"batch_id": "batch-2"},
        )
        args_c = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            metadata={},
        )
        from taskq.backend._protocol import Backend

        b: Backend = backend  # type: ignore[assignment]
        await b.enqueue(args_a)
        await b.enqueue(args_b)
        await b.enqueue(args_c)

        rows = await backend.list_jobs(JobFilter(batch_id="batch-1"))  # type: ignore[arg-type] # Why: test uses string batch_id for metadata-based filtering; JobFilter.batch_id typed as UUID|None for protocol but string values are tested here
        assert len(rows) == 1
        assert rows[0].metadata.get("batch_id") == "batch-1"

    async def test_pagination_with_limit(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        for _ in range(5):
            await _enqueue_job(backend, queue="pag")

        rows = await backend.list_jobs(JobFilter(queue="pag", limit=2))
        assert len(rows) == 2

    async def test_cursor_pagination(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        for _ in range(5):
            await _enqueue_job(backend, queue="cursor_q")

        # Page 1
        page1 = await backend.list_jobs(JobFilter(queue="cursor_q", limit=3))
        assert len(page1) == 3

        # Build cursor from last row of page 1
        last = page1[-1]
        cursor = encode_cursor(last.priority, last.scheduled_at, last.id)

        # Page 2
        page2 = await backend.list_jobs(JobFilter(queue="cursor_q", limit=3, cursor=cursor))
        assert len(page2) == 2  # remaining 2 rows
        assert all(r.id not in {r2.id for r2 in page1} for r in page2)

    async def test_cursor_pagination_without_field_filters(self, clean_jobs_app: JobsApp) -> None:
        """Cursor-only list_jobs (no queue/status/actor filters) must
        produce valid SQL — regression test for the AND-without-WHERE bug."""

        backend = clean_jobs_app.backend

        # Enqueue jobs across different queues so they can't be
        # accidentally filtered by a queue parameter
        await _enqueue_job(backend, queue="q_a")
        await _enqueue_job(backend, queue="q_b")
        await _enqueue_job(backend, queue="q_a")

        # Page 1: no filters, just limit
        page1 = await backend.list_jobs(JobFilter(limit=2))
        assert len(page1) == 2

        # Build cursor from last row of page 1
        last = page1[-1]
        cursor = encode_cursor(last.priority, last.scheduled_at, last.id)

        # Page 2: cursor only, no queue/status/actor filter
        page2 = await backend.list_jobs(JobFilter(limit=2, cursor=cursor))
        assert len(page2) == 1  # one remaining row
        assert all(r.id not in {r2.id for r2 in page1} for r in page2)

    async def test_sort_order(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        # Enqueue jobs with different priorities
        await _enqueue_job(backend)
        # Set priority via a separate enqueue with higher priority
        high_args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            priority=10,
        )
        from taskq.backend._protocol import Backend

        b: Backend = backend  # type: ignore[assignment]
        await b.enqueue(high_args)

        rows = await backend.list_jobs(JobFilter(queue="default"))
        # Higher priority first
        assert rows[0].priority >= rows[1].priority


# ── get_attempts ───────────────────────────────────────────────────────


class TestGetAttempts:
    """``get_attempts`` returns rows in ``attempt`` order."""

    async def test_no_attempts_returns_empty(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        row = await _enqueue_job(backend)
        attempts = await backend.get_attempts(row.id)  # type: ignore[union-attr]
        assert attempts == []

    async def test_attempts_after_terminal_write(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',
                worker_id,
                "test-host",
                12345,
                ["default"],
            )
            job_id = new_job_id()
            await conn.execute(
                f"""INSERT INTO \"{schema}\".jobs (
                    id, actor, queue, payload, max_attempts, retry_kind,
                    status, priority, attempt, scheduled_at,
                    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at
                ) VALUES (
                    $1, $2, $3, $4::jsonb, $5, $6,
                    'running', 0, 1, now(),
                    $7, now() + interval '60 seconds', now(), now()
                )""",
                job_id,
                "test_actor",
                "default",
                '{"key": "value"}',
                3,
                "transient",
                worker_id,
            )

        await backend.mark_succeeded(job_id, worker_id, {"ok": True})

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "succeeded"
        assert attempts[0].attempt == 1

    async def test_attempts_ordered_by_attempt(self, clean_jobs_app: JobsApp) -> None:
        """Multiple attempts are returned in ascending attempt order."""

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        # Insert a job and then manually write attempts
        job_id = new_job_id()
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, attempt) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, 2)",
                job_id,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                datetime.now(UTC),
            )
            # Insert two attempts out of order
            await conn.execute(
                f'INSERT INTO "{schema}".job_attempts '
                "(job_id, attempt, started_at, finished_at, outcome, metadata) "
                "VALUES ($1, 2, $2, $3, 'failed', $4::jsonb)",
                job_id,
                datetime.now(UTC) - timedelta(seconds=10),
                datetime.now(UTC) - timedelta(seconds=5),
                "{}",
            )
            await conn.execute(
                f'INSERT INTO "{schema}".job_attempts '
                "(job_id, attempt, started_at, finished_at, outcome, metadata) "
                "VALUES ($1, 1, $2, $3, 'failed', $4::jsonb)",
                job_id,
                datetime.now(UTC) - timedelta(seconds=30),
                datetime.now(UTC) - timedelta(seconds=20),
                "{}",
            )

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 2
        assert attempts[0].attempt == 1
        assert attempts[1].attempt == 2
