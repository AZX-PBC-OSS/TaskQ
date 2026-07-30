"""Integration tests for PostgresBackend.cancel_where."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import asyncpg
import pytest

from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import Backend, JobFilter
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_enqueue_args

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.integration
class TestCancelWherePostgres:
    async def test_pg_cancel_where_pending(self, backend_pair: Backend) -> None:
        """PostgresBackend.cancel_where cancels pending jobs by tag."""
        for _ in range(3):
            await backend_pair.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))
        await backend_pair.enqueue(make_enqueue_args(tags=("other",), scheduled_at=_NOW))

        result = await backend_pair.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason="offboard",
        )

        assert result.cancelled_directly == 3
        assert result.cancel_requested == 0

        remaining = await backend_pair.list_jobs(JobFilter(tags=("other",)))
        assert len(remaining) == 1
        assert remaining[0].status in ("pending", "scheduled")

    async def test_pg_cancel_where_events_inserted(self, backend_pair: Backend) -> None:
        """cancel_where inserts job_events for cancelled jobs."""
        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend_pair.enqueue(args)

        await backend_pair.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason="test",
        )

        events = await backend_pair.get_events(row.id)
        kinds = [e.kind for e in events]
        assert "state_change" in kinds
        assert "cancel_request" in kinds
        sc = [e for e in events if e.kind == "state_change"]
        assert sc[0].detail.get("from_state") in ("pending", "scheduled")
        assert sc[0].detail.get("to_state") == "cancelled"

    async def test_pg_cancel_where_reason_with_quotes(self, backend_pair: Backend) -> None:
        """Reason containing double-quotes does not cause DataError."""
        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend_pair.enqueue(args)

        result = await backend_pair.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason='offboard "tenant-acme" \\ done',
        )

        assert result.cancelled_directly == 1
        events = await backend_pair.get_events(row.id)
        cr = [e for e in events if e.kind == "cancel_request"]
        assert cr[0].detail.get("reason") == 'offboard "tenant-acme" \\ done'

    async def test_pg_cancel_where_batch_id_filter(self, backend_pair: Backend) -> None:
        """cancel_where works with batch_id filter on Postgres."""
        bid = uuid4()
        for _ in range(3):
            await backend_pair.enqueue(
                make_enqueue_args(
                    scheduled_at=_NOW,
                    metadata={"batch_id": str(bid)},
                )
            )

        result = await backend_pair.cancel_where(
            JobFilter(batch_id=bid),
            reason="batch abort",
        )

        assert result.cancelled_directly == 3

    async def test_pg_cancel_where_running_cooperative(
        self,
        clean_jobs_app: JobsApp,
    ) -> None:
        """cancel_where sets cancel_phase=1 for running jobs on Postgres."""
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend.enqueue(args)

        worker_id = uuid4()
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs '
                f"SET status = 'running', locked_by_worker = $1 WHERE id = $2",
                worker_id,
                row.id,
            )

        result = await backend.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason="offboard",
        )

        assert result.cancelled_directly == 0
        assert result.cancel_requested == 1

        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "running"
        assert updated.cancel_phase == 1

        events = await backend.get_events(row.id)
        kinds = [e.kind for e in events]
        assert "cancel_request" in kinds
        assert "state_change" not in kinds

    async def test_pg_cancel_where_does_not_clobber_concurrent_claim(
        self,
        clean_jobs_app: JobsApp,
    ) -> None:
        """EPQ regression: a job claimed while cancel_where executes must
        NOT be overwritten to terminal 'cancelled'."""
        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend.enqueue(args)
        worker_id = uuid4()

        claim_conn = await deps.worker_pool.acquire()
        try:
            claim_tx = claim_conn.transaction()
            await claim_tx.start()
            await claim_conn.execute(
                f'UPDATE "{schema}".jobs '
                f"SET status = 'running', locked_by_worker = $1 WHERE id = $2",
                worker_id,
                row.id,
            )

            cancel_task = asyncio.create_task(
                backend.cancel_where(JobFilter(tags=("tenant-acme",)), reason="offboard")
            )
            await asyncio.sleep(0.2)
            await claim_tx.commit()
            result = await cancel_task

            assert result.cancelled_directly == 0
            updated = await backend.get(row.id)
            assert updated is not None
            assert updated.status == "running"
        finally:
            await deps.worker_pool.release(claim_conn)

    async def test_pg_cancel_where_notify_sent_for_running(
        self,
        clean_jobs_app: JobsApp,
        pg_dsn: str,
    ) -> None:
        """Batched NOTIFY fires on the fleet and per-worker channels."""
        import asyncpg

        from taskq.constants import events_channel, worker_channel

        backend = clean_jobs_app.backend
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name

        args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
        row = await backend.enqueue(args)
        worker_id = uuid4()
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs '
                f"SET status = 'running', locked_by_worker = $1 WHERE id = $2",
                worker_id,
                row.id,
            )

        received: list[str] = []
        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            await listen_conn.add_listener(
                events_channel(schema),
                lambda _c, _p, _ch, payload: received.append(payload),
            )
            await listen_conn.add_listener(
                worker_channel(schema, str(worker_id)),
                lambda _c, _p, _ch, payload: received.append(payload),
            )
            result = await backend.cancel_where(JobFilter(tags=("tenant-acme",)), reason="offboard")
            assert result.cancel_requested == 1
            await asyncio.sleep(0.3)
        finally:
            await listen_conn.close()

        assert len(received) == 2


class TestDeadlockRetry:
    """Unit tests for the deadlock retry loop in _cancel_where.

    These don't need a real Postgres — they mock the pool to simulate
    deadlock-then-success and deadlock-then-exhaustion scenarios.
    """

    @staticmethod
    def _mock_pool_and_conn(
        fetch_row: dict | None = None,
        fetch_side_effects: list[Exception] | None = None,
    ) -> tuple[MagicMock, MagicMock]:
        conn = MagicMock()

        if fetch_side_effects is not None:
            fetch_mock = AsyncMock(side_effect=fetch_side_effects)
            if fetch_row is not None:
                fetch_mock.side_effect = [
                    *fetch_side_effects,
                    fetch_row,
                ]
            conn.fetchrow = fetch_mock
        else:
            conn.fetchrow = AsyncMock(return_value=fetch_row)

        conn.executemany = AsyncMock(return_value=None)

        tx = AsyncMock()
        tx.__aenter__ = AsyncMock(return_value=tx)
        tx.__aexit__ = AsyncMock(return_value=False)
        conn.transaction = MagicMock(return_value=tx)

        conn.__aenter__ = AsyncMock(return_value=conn)
        conn.__aexit__ = AsyncMock(return_value=False)

        pool = MagicMock()
        pool.acquire = MagicMock(return_value=conn)
        return pool, conn

    @staticmethod
    def _success_row() -> dict:
        return {
            "cancelled_directly": 1,
            "cancel_requested": 0,
            "cancelled_ids": [uuid4()],
            "cancelled_prev_statuses": ["pending"],
            "cancel_requested_ids": [],
            "cancel_requested_workers": [],
        }

    async def test_deadlock_retry_succeeds_on_second_attempt(self) -> None:
        """_cancel_where retries on DeadlockDetectedError and succeeds."""
        row = self._success_row()
        pool, _ = self._mock_pool_and_conn(
            fetch_row=row,
            fetch_side_effects=[asyncpg.DeadlockDetectedError()],
        )

        sql = MagicMock()
        sql.insert_event = "INSERT INTO job_events VALUES ($1, $2, $3, $4)"

        result, _notify = await _cancel_where(pool, "taskq", sql, JobFilter(tags=("x",)), "test")

        assert result.cancelled_directly == 1
        assert result.cancel_requested == 0
        assert len(result.cancelled_ids) == 1

    async def test_deadlock_retry_exhausted_raises(self) -> None:
        """_cancel_where raises after 3 failed attempts."""
        pool, _ = self._mock_pool_and_conn(
            fetch_side_effects=[asyncpg.DeadlockDetectedError()] * 3,
        )

        sql = MagicMock()

        with pytest.raises(asyncpg.DeadlockDetectedError):
            await _cancel_where(pool, "taskq", sql, JobFilter(tags=("x",)), "test")

    async def test_no_deadlock_no_retry(self) -> None:
        """_cancel_where succeeds immediately without retry."""
        row = self._success_row()
        pool, conn = self._mock_pool_and_conn(fetch_row=row)

        sql = MagicMock()
        sql.insert_event = "INSERT INTO job_events VALUES ($1, $2, $3, $4)"

        await _cancel_where(pool, "taskq", sql, JobFilter(tags=("x",)), "test")

        assert conn.fetchrow.call_count == 1
