# ruff: noqa: S608
"""Integration tests for PostgresBackend methods with zero PG coverage.

Covers ``count_pending_jobs``, ``extend_reservation_leases``,
``list_jobs`` with ``identity_key`` filter, and ``get_events``.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import (
    EnqueueArgs,
    IdempotencyKey,
    IdentityKey,
    JobFilter,
)
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


# ── Helpers ────────────────────────────────────────────────────────────────


async def _enqueue_job(
    backend: object,
    *,
    actor: str = "test_actor",
    queue: str = "default",
    identity_key: IdentityKey | None = None,
    payload: dict[str, object] | None = None,
    scheduled_at: datetime | None = None,
    idempotency_key: IdempotencyKey | None = None,
) -> object:
    """Enqueue a job via the backend and return the JobRow."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload=payload or {"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=scheduled_at or datetime.now(UTC),
        identity_key=identity_key,
        idempotency_key=idempotency_key,
    )
    from taskq.backend._protocol import Backend

    b: Backend = backend  # type: ignore[assignment]
    return await b.enqueue(args)


# ── count_pending_jobs ─────────────────────────────────────────────────────


class TestCountPendingJobs:
    """``count_pending_jobs`` returns a dict of actor -> pending+scheduled count."""

    async def test_returns_correct_counts_for_multiple_actors(
        self, clean_jobs_app: JobsApp
    ) -> None:
        backend = clean_jobs_app.backend

        # Enqueue 3 jobs for actor_a, 2 for actor_b, none for actor_c
        await _enqueue_job(backend, actor="actor_a")
        await _enqueue_job(backend, actor="actor_a")
        await _enqueue_job(backend, actor="actor_a")
        await _enqueue_job(backend, actor="actor_b")
        await _enqueue_job(backend, actor="actor_b")

        counts = await backend.count_pending_jobs(["actor_a", "actor_b", "actor_c"])
        assert counts == {"actor_a": 3, "actor_b": 2}

    async def test_actors_with_no_pending_absent_from_result(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        await _enqueue_job(backend, actor="actor_a")

        counts = await backend.count_pending_jobs(["actor_a", "actor_b"])
        assert "actor_a" in counts
        assert "actor_b" not in counts

    async def test_empty_actors_list_returns_empty_dict(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        counts = await backend.count_pending_jobs([])
        assert counts == {}

    async def test_only_counts_pending_and_scheduled(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        # Enqueue a pending job
        await _enqueue_job(backend, actor="actor_a")

        # Manually insert a running job and a succeeded job
        running_id = new_job_id()
        succeeded_id = new_job_id()

        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, $2, $3, $4)",
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
                running_id,
                "actor_a",
                "default",
                "{}",
                3,
                "transient",
                worker_id,
            )
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, "
                "status, priority, scheduled_at) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                "'succeeded', 0, now())",
                succeeded_id,
                "actor_a",
                "default",
                "{}",
                3,
                "transient",
            )

        counts = await backend.count_pending_jobs(["actor_a"])
        # Only the pending job should be counted; running+succeeded are excluded
        assert counts == {"actor_a": 1}

    async def test_scheduled_jobs_are_counted(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        # Enqueue a job with future scheduled_at
        await _enqueue_job(
            backend,
            actor="actor_a",
            scheduled_at=datetime.now(UTC) + timedelta(hours=1),
        )

        counts = await backend.count_pending_jobs(["actor_a"])
        assert counts == {"actor_a": 1}


# ── extend_reservation_leases ──────────────────────────────────────────────


class TestExtendReservationLeases:
    """``extend_reservation_leases`` extends the lease on reservation slots
    belonging to the specified worker's running jobs."""

    async def test_extends_lease_on_reservation_slot(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()
        job_id = new_job_id()

        # Create worker, running job, and a reservation_slot row
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, $2, $3, $4)",
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
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                worker_id,
            )

            old_lease = datetime.now(UTC) + timedelta(seconds=5)
            await conn.execute(
                f'INSERT INTO "{schema}".reservation_slots '
                "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                "test_bucket",
                0,
                job_id,
                worker_id,
                datetime.now(UTC),
                old_lease,
            )

        lease_duration = timedelta(seconds=120)
        count = await backend.extend_reservation_leases(worker_id, lease_duration)
        assert count == 1

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT lease_expires_at FROM "{schema}".reservation_slots '
                "WHERE bucket_name = $1 AND slot_index = $2",
                "test_bucket",
                0,
            )

        assert row is not None
        new_lease: datetime = row["lease_expires_at"]
        # The new lease should be approximately now + lease_duration
        now = datetime.now(UTC)
        expected_min = now + timedelta(seconds=110)
        expected_max = now + timedelta(seconds=130)
        assert expected_min <= new_lease <= expected_max, (
            f"lease_expires_at {new_lease} not in expected range [{expected_min}, {expected_max}]"
        )

    async def test_multiple_slots_for_same_worker_all_extended(
        self, clean_jobs_app: JobsApp
    ) -> None:
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, $2, $3, $4)",
                worker_id,
                "test-host",
                12345,
                ["default"],
            )

            # Create two running jobs
            for i in range(2):
                jid = new_job_id()
                await conn.execute(
                    f'INSERT INTO "{schema}".jobs '
                    "(id, actor, queue, payload, max_attempts, retry_kind, "
                    "status, priority, attempt, scheduled_at, "
                    "locked_by_worker, lock_expires_at, started_at, last_heartbeat_at) "
                    "VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                    "'running', 0, 1, now(), "
                    "$7, now() + interval '60 seconds', now(), now())",
                    jid,
                    "test_actor",
                    "default",
                    "{}",
                    3,
                    "transient",
                    worker_id,
                )
                old_lease = datetime.now(UTC) + timedelta(seconds=5)
                await conn.execute(
                    f'INSERT INTO "{schema}".reservation_slots '
                    "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    "test_bucket",
                    i,
                    jid,
                    worker_id,
                    datetime.now(UTC),
                    old_lease,
                )

        count = await backend.extend_reservation_leases(worker_id, timedelta(seconds=120))
        assert count == 2

    async def test_slots_for_different_worker_not_affected(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        worker_a = new_uuid()
        worker_b = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, $2, $3, $4)",
                worker_a,
                "test-host",
                12345,
                ["default"],
            )
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, $2, $3, $4)",
                worker_b,
                "test-host",
                12346,
                ["default"],
            )

            # Worker A's running job + slot
            job_a = new_job_id()
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, "
                "status, priority, attempt, scheduled_at, "
                "locked_by_worker, lock_expires_at, started_at, last_heartbeat_at) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                "'running', 0, 1, now(), "
                "$7, now() + interval '60 seconds', now(), now())",
                job_a,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                worker_a,
            )
            old_lease_a = datetime.now(UTC) + timedelta(seconds=5)
            await conn.execute(
                f'INSERT INTO "{schema}".reservation_slots '
                "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                "test_bucket",
                0,
                job_a,
                worker_a,
                datetime.now(UTC),
                old_lease_a,
            )

            # Worker B's running job + slot
            job_b = new_job_id()
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, "
                "status, priority, attempt, scheduled_at, "
                "locked_by_worker, lock_expires_at, started_at, last_heartbeat_at) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                "'running', 0, 1, now(), "
                "$7, now() + interval '60 seconds', now(), now())",
                job_b,
                "test_actor",
                "default",
                "{}",
                3,
                "transient",
                worker_b,
            )
            old_lease_b = datetime.now(UTC) + timedelta(seconds=5)
            await conn.execute(
                f'INSERT INTO "{schema}".reservation_slots '
                "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                "other_bucket",
                0,
                job_b,
                worker_b,
                datetime.now(UTC),
                old_lease_b,
            )

        # Extend only worker A's leases
        count = await backend.extend_reservation_leases(worker_a, timedelta(seconds=120))
        assert count == 1

        # Verify worker B's lease was NOT extended (still near old_lease_b)
        async with deps.worker_pool.acquire() as conn:
            row_b = await conn.fetchrow(
                f'SELECT lease_expires_at FROM "{schema}".reservation_slots '
                "WHERE bucket_name = $1 AND slot_index = $2",
                "other_bucket",
                0,
            )

        assert row_b is not None
        lease_b: datetime = row_b["lease_expires_at"]
        # Worker B's lease should still be close to old_lease_b (within ~10s tolerance)
        assert abs((lease_b - old_lease_b).total_seconds()) < 10, (
            f"Worker B's lease was unexpectedly extended: old={old_lease_b}, new={lease_b}"
        )

    async def test_no_running_jobs_returns_zero(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        count = await backend.extend_reservation_leases(new_uuid(), timedelta(seconds=60))
        assert count == 0


# ── list_jobs with identity_key filter ─────────────────────────────────────


class TestListJobsIdentityKey:
    """``list_jobs`` with ``identity_key`` filter returns only matching jobs."""

    async def test_filter_by_identity_key(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        await _enqueue_job(
            backend,
            actor="actor_a",
            identity_key=IdentityKey("ABC"),
        )
        await _enqueue_job(
            backend,
            actor="actor_b",
            identity_key=IdentityKey("DEF"),
        )

        rows = await backend.list_jobs(JobFilter(identity_key=IdentityKey("ABC")))
        assert len(rows) == 1
        assert rows[0].identity_key == IdentityKey("ABC")

    async def test_filter_by_identity_key_and_status(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        # Enqueue a pending job with identity_key "ABC"
        await _enqueue_job(
            backend,
            actor="actor_a",
            identity_key=IdentityKey("ABC"),
        )

        # Insert a succeeded job with the same identity_key
        succeeded_id = new_job_id()
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, "
                "status, priority, scheduled_at, identity_key) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, "
                "'succeeded', 0, now(), $7)",
                succeeded_id,
                "actor_a",
                "default",
                "{}",
                3,
                "transient",
                "ABC",
            )

        # Filter by identity_key + pending status
        rows = await backend.list_jobs(JobFilter(identity_key=IdentityKey("ABC"), status="pending"))
        assert len(rows) == 1
        assert rows[0].status == "pending"
        assert rows[0].identity_key == IdentityKey("ABC")

        # Filter by identity_key + succeeded status
        rows = await backend.list_jobs(
            JobFilter(identity_key=IdentityKey("ABC"), status="succeeded")
        )
        assert len(rows) == 1
        assert rows[0].status == "succeeded"


# ── get_events standalone ──────────────────────────────────────────────────


class TestGetEvents:
    """``get_events`` returns event rows for a job sorted by ``occurred_at``."""

    async def test_get_events_returns_rows_in_order(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        job_id = new_job_id()

        # Create a job row first
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)",
                job_id,
                "test_actor",
                "default",
                '{"key": "value"}',
                3,
                "transient",
                datetime.now(UTC),
            )

            # Write two events via raw SQL
            now = datetime.now(UTC)
            await conn.execute(
                f'INSERT INTO "{schema}".job_events '
                "(job_id, occurred_at, kind, detail) "
                "VALUES ($1, $2, 'state_change', $3::jsonb)",
                job_id,
                now - timedelta(seconds=10),
                '{"from_state": "pending", "to_state": "running"}',
            )
            await conn.execute(
                f'INSERT INTO "{schema}".job_events '
                "(job_id, occurred_at, kind, detail) "
                "VALUES ($1, $2, 'state_change', $3::jsonb)",
                job_id,
                now - timedelta(seconds=5),
                '{"from_state": "running", "to_state": "succeeded"}',
            )

        events = await backend.get_events(job_id)
        assert len(events) == 2
        assert events[0].kind == "state_change"
        assert events[0].detail == {"from_state": "pending", "to_state": "running"}
        assert events[1].kind == "state_change"
        assert events[1].detail == {"from_state": "running", "to_state": "succeeded"}

    async def test_get_events_no_events_returns_empty(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        job_id = new_job_id()

        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)",
                job_id,
                "test_actor",
                "default",
                '{"key": "value"}',
                3,
                "transient",
                datetime.now(UTC),
            )

        events = await backend.get_events(job_id)
        assert events == []

    async def test_get_events_nonexistent_job_returns_empty(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        events = await backend.get_events(new_job_id())
        assert events == []
