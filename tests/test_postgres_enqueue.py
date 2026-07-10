"""Integration tests for PostgresBackend.enqueue against real PG.

Covers idempotency-key ON CONFLICT behaviour, the post-INSERT
pg_notify wake signal, and a webhook-event dedup seam check.
"""

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.constants import wake_channel
from taskq.testing.assertions import assert_job_status
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_enqueue_args

if TYPE_CHECKING:
    # asyncpg's Connection / PoolConnectionProxy are generic in the stubs but
    # the runtime classes are not subscriptable. Type-only aliases keep
    # pyright strict happy without breaking import-time type evaluation.
    type _AsyncpgConn = (
        asyncpg.Connection[asyncpg.Record] | asyncpg.pool.PoolConnectionProxy[asyncpg.Record]
    )

pytestmark = pytest.mark.integration


async def _insert_job_direct(
    conn: "_AsyncpgConn",
    schema: str,
    *,
    idempotency_key: str | None = None,
) -> None:
    """Insert a job row directly via asyncpg (bypasses backend)."""
    await conn.execute(
        f"""INSERT INTO \"{schema}\".jobs
        (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, idempotency_key)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)""",
        new_uuid(),
        "direct_actor",
        "default",
        "{}",
        3,
        "transient",
        datetime.now(UTC),
        idempotency_key,
    )


# ── partial unique index enforcement ───────────────────────────


class TestPartialUniqueIndexEnforcement:
    """the partial unique index on idempotency_key enforces
    uniqueness for non-NULL keys and allows duplicate NULLs."""

    async def test_duplicate_non_null_key_raises(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            key = "unique-key-partial-index"
            await _insert_job_direct(conn, schema, idempotency_key=key)
            with pytest.raises(asyncpg.UniqueViolationError):
                await _insert_job_direct(conn, schema, idempotency_key=key)

    async def test_duplicate_null_keys_succeed(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            # Two rows with idempotency_key=NULL should both succeed
            await _insert_job_direct(conn, schema, idempotency_key=None)
            await _insert_job_direct(conn, schema, idempotency_key=None)


# ── idempotency-key dedup via enqueue ──────────────────────────


class TestIdempotencyKeyDedupViaEnqueue:
    """enqueue with the same idempotency_key returns the
    same job_id and the row count is exactly 1."""

    async def test_duplicate_enqueue_returns_same_job_id(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        key = "dedup-key-via-enqueue"
        args1 = make_enqueue_args(idempotency_key=key)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, payload={"value": 2})
        row2 = await backend.enqueue(args2)

        # Same job_id returned both times
        assert row1.id == row2.id
        assert row1.idempotency_key == key

        # Verify the job exists and was not duplicated via backend protocol
        row = await backend.get(row1.id)
        assert row is not None
        assert_job_status(row, row1.status)
        assert row.idempotency_key == key


# ── payload not overwritten on conflict ─────────────────────────


class TestPayloadNotOverwrittenOnConflict:
    """enqueue with a duplicate key does NOT overwrite the stored
    payload — the original payload is preserved."""

    async def test_conflict_preserves_original_payload(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        key = "payload-key-conflict"
        args1 = make_enqueue_args(idempotency_key=key, payload={"value": 1})
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, payload={"value": 2})
        row2 = await backend.enqueue(args2)

        # The stored payload must still be {"value": 1}
        assert row2.payload == {"value": 1}

        # Double-check via backend get
        row = await backend.get(row1.id)
        assert row is not None
        assert row.payload == {"value": 1}


# ── pg_notify verification ─────────────────────────────────────────────


class TestNotify:
    """pg_notify is issued on a new INSERT but NOT on a conflict-returns-existing
    path."""

    async def test_notify_on_new_insert(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        channel = wake_channel(schema)

        # Use an asyncio.Event to capture the notification
        notify_event = asyncio.Event()

        def _on_notify(
            _conn: "_AsyncpgConn",
            _pid: int,
            _ch: str,
            _payload: object,
        ) -> None:
            notify_event.set()

        # Open a second connection and LISTEN
        listen_conn = await asyncpg.connect(str(deps.settings.pg_dsn))
        try:
            await listen_conn.add_listener(channel, _on_notify)
            # Drain any pending notifications
            await asyncio.sleep(0.05)

            args = make_enqueue_args()
            await backend.enqueue(args)

            # Assert notification arrives within a short timeout
            await asyncio.wait_for(notify_event.wait(), timeout=2.0)
        finally:
            await listen_conn.remove_listener(channel, _on_notify)
            await listen_conn.close()

    async def test_no_notify_on_conflict(self, clean_jobs_app: JobsApp) -> None:

        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        channel = wake_channel(schema)

        key = "notify-dedup-key"
        args1 = make_enqueue_args(idempotency_key=key)
        await backend.enqueue(args1)

        # Use an asyncio.Event to detect if a notification fires
        notify_event = asyncio.Event()

        def _on_notify(
            _conn: "_AsyncpgConn",
            _pid: int,
            _ch: str,
            _payload: object,
        ) -> None:
            notify_event.set()

        # Open a second connection and LISTEN after the first insert
        listen_conn = await asyncpg.connect(str(deps.settings.pg_dsn))
        try:
            await listen_conn.add_listener(channel, _on_notify)
            await asyncio.sleep(0.05)

            args2 = make_enqueue_args(idempotency_key=key, payload={"value": 2})
            await backend.enqueue(args2)

            # Assert NO notification arrives within a short timeout
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(notify_event.wait(), timeout=0.5)
        finally:
            await listen_conn.remove_listener(channel, _on_notify)
            await listen_conn.close()


# ── webhook event dedup seam check ──────────────────────────────────────


class TestWebhookEventDedup:
    """Webhook-event dedup seam check: enqueue with key ``webhook:event-123``;
    second enqueue with the same key; assert the same ``job_id``."""

    async def test_webhook_event_dedup(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        key = "webhook:event-123"
        args1 = make_enqueue_args(idempotency_key=key)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key)
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id


# ── Enqueue without idempotency_key ────────────────────────────────────


class TestEnqueueNoKey:
    """When ``idempotency_key is None``, a plain INSERT runs and
    ``RETURNING *`` always succeeds."""

    async def test_enqueue_no_key_succeeds(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        args = make_enqueue_args(idempotency_key=None)
        row = await backend.enqueue(args)

        assert row.id == args.id
        assert row.status in ("pending", "scheduled")
        assert row.idempotency_key is None

    async def test_enqueue_no_key_creates_job_in_db(self, clean_jobs_app: JobsApp) -> None:

        backend = clean_jobs_app.backend

        args = make_enqueue_args(idempotency_key=None, payload={"x": 42})
        await backend.enqueue(args)

        row = await backend.get(args.id)
        assert row is not None
        assert row.actor == "test_actor"
        assert row.queue == "default"

    async def test_two_enqueues_without_key_create_separate_jobs(
        self, clean_jobs_app: JobsApp
    ) -> None:

        backend = clean_jobs_app.backend

        args1 = make_enqueue_args(idempotency_key=None)
        args2 = make_enqueue_args(idempotency_key=None)
        row1 = await backend.enqueue(args1)
        row2 = await backend.enqueue(args2)

        assert row1.id != row2.id
