# ruff: noqa: S608
"""Integration tests for PostgresBackend schedule CRUD methods.

Covers ``create_schedule``, ``list_schedules``, ``update_schedule``,
and ``delete_schedule`` at the backend level against real PG.
"""

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from asyncpg.exceptions import UniqueViolationError

from taskq.backend._protocol import ScheduleCreateArgs, ScheduleUpdateArgs
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


# ── Helpers ────────────────────────────────────────────────────────────────


async def _fetch_schedule_raw(
    conn: asyncpg.Connection, schema: str, schedule_id: str
) -> asyncpg.Record | None:
    """Return the raw asyncpg Record for a cron_schedules row, or None."""
    return await conn.fetchrow(
        f'SELECT * FROM "{schema}".cron_schedules WHERE id = $1',
        schedule_id,
    )


# ── create_schedule ────────────────────────────────────────────────────────


class TestCreateSchedule:
    """``create_schedule`` inserts a row and returns a ScheduleRecord."""

    async def test_create_schedule_inserts_row(
        self, clean_jobs_app: JobsApp, clean_pg_conn: asyncpg.Connection
    ) -> None:
        backend = clean_jobs_app.backend
        schema = clean_jobs_app.deps.settings.schema_name

        fire_at = datetime.now(UTC) + timedelta(minutes=5)
        args = ScheduleCreateArgs(
            actor="actor_a",
            cron_expr="0 0 * * *",
            timezone="UTC",
            next_fire_at=fire_at,
        )
        record = await backend.create_schedule(args)

        # Verify the returned record
        assert record.actor == "actor_a"
        assert record.cron_expr == "0 0 * * *"
        assert record.timezone == "UTC"
        assert record.enabled is True
        assert record.consecutive_failures == 0

        # Verify the row exists in PG
        row = await _fetch_schedule_raw(clean_pg_conn, schema, record.id)
        assert row is not None
        assert row["actor"] == "actor_a"
        assert row["cron_expr"] == "0 0 * * *"
        assert row["enabled"] is True

    async def test_create_schedule_with_disabled_and_payload_factory(
        self, clean_jobs_app: JobsApp, clean_pg_conn: asyncpg.Connection
    ) -> None:
        backend = clean_jobs_app.backend
        schema = clean_jobs_app.deps.settings.schema_name

        fire_at = datetime.now(UTC) + timedelta(minutes=5)
        args = ScheduleCreateArgs(
            actor="actor_b",
            cron_expr="*/15 * * * *",
            timezone="America/New_York",
            next_fire_at=fire_at,
            enabled=False,
            payload_factory="my.module:factory_func",
            dst_strategy="firstof",
            metadata={"owner": "test"},
        )
        record = await backend.create_schedule(args)

        assert record.enabled is False
        assert record.payload_factory == "my.module:factory_func"
        assert record.dst_strategy == "firstof"
        assert record.metadata == {"owner": "test"}

        row = await _fetch_schedule_raw(clean_pg_conn, schema, record.id)
        assert row is not None
        assert row["enabled"] is False
        assert row["payload_factory"] == "my.module:factory_func"


# ── list_schedules ─────────────────────────────────────────────────────────


class TestListSchedules:
    """``list_schedules`` returns schedules with optional filters."""

    async def test_list_all_schedules(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=True,
            )
        )
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_b",
                cron_expr="0 12 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=True,
            )
        )
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_c",
                cron_expr="*/5 * * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=False,
            )
        )

        all_schedules = await backend.list_schedules()
        assert len(all_schedules) == 3

    async def test_list_schedules_filter_enabled(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=True,
            )
        )
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_b",
                cron_expr="0 12 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=True,
            )
        )
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_c",
                cron_expr="*/5 * * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=False,
            )
        )

        enabled = await backend.list_schedules(enabled=True)
        assert len(enabled) == 2
        assert all(s.enabled for s in enabled)

        disabled = await backend.list_schedules(enabled=False)
        assert len(disabled) == 1
        assert all(not s.enabled for s in disabled)

    async def test_list_schedules_filter_actor(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
            )
        )
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_b",
                cron_expr="0 12 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
            )
        )

        actor_a_schedules = await backend.list_schedules(actor="actor_a")
        assert len(actor_a_schedules) == 1
        assert actor_a_schedules[0].actor == "actor_a"

    async def test_list_schedules_filter_actor_and_enabled(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=True,
            )
        )
        # actor_b: enabled=True, actor_c: enabled=False — crossed filter
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_b",
                cron_expr="0 12 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=True,
            )
        )
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_c",
                cron_expr="*/5 * * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=False,
            )
        )

        # actor_b + enabled=True → 1 match
        result = await backend.list_schedules(actor="actor_b", enabled=True)
        assert len(result) == 1
        assert result[0].actor == "actor_b"
        assert result[0].enabled is True

        # actor_c + enabled=True → 0 matches (it's disabled)
        result = await backend.list_schedules(actor="actor_c", enabled=True)
        assert len(result) == 0


# ── update_schedule ────────────────────────────────────────────────────────


class TestUpdateSchedule:
    """``update_schedule`` modifies a schedule row and returns the updated record."""

    async def test_update_cron_expr_and_next_fire_at(
        self, clean_jobs_app: JobsApp, clean_pg_conn: asyncpg.Connection
    ) -> None:
        backend = clean_jobs_app.backend
        schema = clean_jobs_app.deps.settings.schema_name
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        record = await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
            )
        )

        new_fire = datetime.now(UTC) + timedelta(hours=2)
        update_args = ScheduleUpdateArgs(
            cron_expr="0 6 * * *",
            next_fire_at=new_fire,
        )
        updated = await backend.update_schedule(record.id, update_args)
        assert updated.cron_expr == "0 6 * * *"
        assert updated.next_fire_at == new_fire

        row = await _fetch_schedule_raw(clean_pg_conn, schema, record.id)
        assert row is not None
        assert row["cron_expr"] == "0 6 * * *"

    async def test_update_enabled_resets_consecutive_failures(
        self, clean_jobs_app: JobsApp, clean_pg_conn: asyncpg.Connection
    ) -> None:
        backend = clean_jobs_app.backend
        schema = clean_jobs_app.deps.settings.schema_name
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        # Create disabled schedule first
        record = await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                enabled=False,
            )
        )

        # Manually set consecutive_failures and last_fire_error via raw SQL
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules '
            "SET consecutive_failures = 5, last_fire_error = 'some error' "
            "WHERE id = $1",
            record.id,
        )

        # Now enable — should reset failures
        updated = await backend.update_schedule(record.id, ScheduleUpdateArgs(enabled=True))
        assert updated.enabled is True
        assert updated.consecutive_failures == 0
        assert updated.last_fire_error is None

        row = await _fetch_schedule_raw(clean_pg_conn, schema, record.id)
        assert row is not None
        assert row["enabled"] is True
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None

    async def test_update_payload_factory(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        record = await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
            )
        )
        assert record.payload_factory is None

        updated = await backend.update_schedule(
            record.id,
            ScheduleUpdateArgs(payload_factory="my.module:new_factory"),
        )
        assert updated.payload_factory == "my.module:new_factory"

    async def test_update_metadata(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        record = await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                metadata={"original": True},
            )
        )

        updated = await backend.update_schedule(
            record.id,
            ScheduleUpdateArgs(metadata={"updated": True, "version": 2}),
        )
        assert updated.metadata == {"updated": True, "version": 2}

    async def test_update_no_fields_returns_current_row(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        record = await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
            )
        )

        # Empty update — should return current row unchanged
        updated = await backend.update_schedule(record.id, ScheduleUpdateArgs())
        assert updated.id == record.id
        assert updated.actor == record.actor
        assert updated.cron_expr == record.cron_expr

    async def test_update_nonexistent_raises_keyerror(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        from uuid import uuid4

        with pytest.raises(KeyError, match="not found"):
            await backend.update_schedule(uuid4(), ScheduleUpdateArgs(enabled=True))


# ── delete_schedule ────────────────────────────────────────────────────────


class TestDeleteSchedule:
    """``delete_schedule`` removes a cron schedule row."""

    async def test_delete_existing_schedule(
        self, clean_jobs_app: JobsApp, clean_pg_conn: asyncpg.Connection
    ) -> None:
        backend = clean_jobs_app.backend
        schema = clean_jobs_app.deps.settings.schema_name
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        record = await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
            )
        )

        # Verify it exists
        row = await _fetch_schedule_raw(clean_pg_conn, schema, record.id)
        assert row is not None

        await backend.delete_schedule(record.id)

        # Verify it's gone
        row = await _fetch_schedule_raw(clean_pg_conn, schema, record.id)
        assert row is None

    async def test_delete_nonexistent_schedule_is_idempotent(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        from uuid import uuid4

        # Should not raise
        await backend.delete_schedule(uuid4())


# ── update_schedule clear_payload_factory ──────────────────────────────────


class TestClearPayloadFactory:
    """``update_schedule`` with ``clear_payload_factory=True`` sets the
    column to NULL."""

    async def test_clear_payload_factory_sets_null(
        self, clean_jobs_app: JobsApp, clean_pg_conn: asyncpg.Connection
    ) -> None:
        backend = clean_jobs_app.backend
        schema = clean_jobs_app.deps.settings.schema_name
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        record = await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
                payload_factory="my.module:factory_func",
            )
        )
        assert record.payload_factory == "my.module:factory_func"

        updated = await backend.update_schedule(
            record.id, ScheduleUpdateArgs(clear_payload_factory=True)
        )
        assert updated.payload_factory is None

        row = await _fetch_schedule_raw(clean_pg_conn, schema, record.id)
        assert row is not None
        assert row["payload_factory"] is None


# ── create_schedule duplicate actor ────────────────────────────────────────


class TestCreateScheduleDuplicateActor:
    """``create_schedule`` with a duplicate actor raises UniqueViolationError."""

    async def test_duplicate_actor_raises_unique_violation(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend
        fire_at = datetime.now(UTC) + timedelta(minutes=5)

        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="actor_a",
                cron_expr="0 0 * * *",
                timezone="UTC",
                next_fire_at=fire_at,
            )
        )

        with pytest.raises(UniqueViolationError):
            await backend.create_schedule(
                ScheduleCreateArgs(
                    actor="actor_a",
                    cron_expr="0 12 * * *",
                    timezone="UTC",
                    next_fire_at=fire_at,
                )
            )
