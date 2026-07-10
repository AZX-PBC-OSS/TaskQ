"""Tests for Backend.enqueue_with_conn and supports_transactional_simulation.

Covers:
- In-memory: enqueue_with_conn delegates to enqueue and returns structurally
  identical results.
- In-memory: supports_transactional_simulation is True on InMemoryBackend.
- PostgresBackend: supports_transactional_simulation is False.
- Backend Protocol: enqueue_with_conn is part of the protocol surface.
- PG integration tests (smoke-level) are marked with pytest.mark.integration.
"""

from datetime import UTC, datetime

import pytest

from taskq.backend._protocol import JobRow
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema, _open_pg_backend_on_schema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)


# ── supports_transactional_simulation ────────────────────────────────────


class TestSupportsTransactionalSimulation:
    """Verify the ClassVar value on each backend class."""

    def test_in_memory_is_true(self) -> None:
        assert InMemoryBackend.supports_transactional_simulation is True

    def test_pg_is_false(self) -> None:
        from taskq.backend.postgres import PostgresBackend

        assert PostgresBackend.supports_transactional_simulation is False

    def test_backend_protocol_default_is_false(self) -> None:
        from taskq.backend._protocol import Backend

        assert Backend.supports_transactional_simulation is False


# ── In-memory: enqueue_with_conn delegates to enqueue ────────────────────


class TestInMemoryEnqueueWithConn:
    """InMemoryBackend.enqueue_with_conn delegates to enqueue."""

    async def test_enqueue_with_conn_inserts_row(self) -> None:
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        args = make_enqueue_args(scheduled_at=_START)

        row = await backend.enqueue_with_conn(None, args)

        assert isinstance(row, JobRow)
        assert row.id == args.id
        stored = await backend.get(args.id)
        assert stored is not None
        assert stored.id == args.id

    async def test_enqueue_with_conn_matches_enqueue(self) -> None:
        clock = FakeClock(_START)
        backend1 = InMemoryBackend(clock=clock)
        backend2 = InMemoryBackend(clock=clock)
        args1 = make_enqueue_args(scheduled_at=_START)
        args2 = make_enqueue_args(
            actor=args1.actor,
            queue=args1.queue,
            scheduled_at=_START,
        )

        row_via_conn = await backend1.enqueue_with_conn(None, args1)
        row_via_enq = await backend2.enqueue(args2)

        assert row_via_conn.actor == row_via_enq.actor
        assert row_via_conn.queue == row_via_enq.queue
        assert row_via_conn.status == row_via_enq.status
        assert row_via_conn.payload == row_via_enq.payload

    async def test_enqueue_with_conn_idempotency_dedup(self) -> None:
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        args1 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        args2 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)

        row1 = await backend.enqueue_with_conn(None, args1)
        row2 = await backend.enqueue_with_conn(None, args2)

        assert row2.id == row1.id

    async def test_enqueue_autonomous_still_works(self) -> None:
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        args = make_enqueue_args(scheduled_at=_START)

        row = await backend.enqueue(args)

        assert isinstance(row, JobRow)
        assert row.id == args.id


# ── PG integration: enqueue_with_conn uses supplied conn ──────────────────


class TestPGEnqueueWithConn:
    """PostgresBackend.enqueue_with_conn integration tests (smoke-level)."""

    pytestmark = pytest.mark.integration

    async def test_enqueue_with_conn_uses_supplied_conn(
        self,
        clean_pg_conn: object,
        module_pg_schema: object,
    ) -> None:
        """enqueue_with_conn on a real PG connection inserts a row and it persists."""
        pg_schema: ModulePgSchema = module_pg_schema  # type: ignore[assignment]
        stack, _deps, backend = await _open_pg_backend_on_schema(
            pg_schema.pg_dsn,
            pg_schema.schema_name,
        )
        try:
            args = make_enqueue_args()
            async with clean_pg_conn.transaction():  # type: ignore[union-attr]
                row = await backend.enqueue_with_conn(clean_pg_conn, args)
            assert row.id == args.id
            # Verify the row persisted after commit
            stored = await backend.get(args.id)
            assert stored is not None
            assert stored.id == args.id
        finally:
            await stack.aclose()

    async def test_enqueue_with_conn_rolls_back_on_caller_rollback(
        self,
        clean_pg_conn: object,
        module_pg_schema: object,
    ) -> None:
        """enqueue_with_conn inside a rolled-back transaction leaves no row."""
        pg_schema: ModulePgSchema = module_pg_schema  # type: ignore[assignment]
        stack, _deps, backend = await _open_pg_backend_on_schema(
            pg_schema.pg_dsn,
            pg_schema.schema_name,
        )
        try:
            args = make_enqueue_args()
            try:
                async with clean_pg_conn.transaction():  # type: ignore[union-attr]
                    row = await backend.enqueue_with_conn(clean_pg_conn, args)
                    assert row.id == args.id
                    raise RuntimeError("simulated rollback")
            except RuntimeError:
                pass
            # Verify the row was NOT persisted (rolled back)
            stored = await backend.get(args.id)
            assert stored is None
        finally:
            await stack.aclose()
