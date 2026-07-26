"""Integration tests for PostgresBackend ``enqueue_batch()`` with idempotency_scope.

Extends tests/test_postgres_enqueue_batch_collision.py style:
- Same idempotency_key across different idempotency_scope values within a
  single enqueue_batch call all insert as distinct jobs.
- Same key+scope pair within a batch collides correctly against a pre-existing row.
"""

from datetime import UTC, datetime

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


def _make_args(
    *,
    actor: str = "test_actor",
    queue: str = "default",
    payload: dict[str, object] | None = None,
    idempotency_key: str | None = None,
    idempotency_scope: str = "",
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload=payload or {"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC),
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key is not None else None,
        idempotency_scope=idempotency_scope,
    )


# ── Same key, different scopes within a batch → all distinct ───


class TestSameKeyDifferentScopesInBatch:
    """Same idempotency_key across different idempotency_scope values
    within a single enqueue_batch call should all insert as distinct jobs."""

    async def test_two_scopes_same_key_all_new(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        args_list = [
            _make_args(idempotency_key="shared-key", idempotency_scope="run-A"),
            _make_args(idempotency_key="shared-key", idempotency_scope="run-B"),
        ]

        result = await backend.enqueue_batch(args_list)
        assert len(result) == 2

        assert result[0].id == args_list[0].id
        assert result[1].id == args_list[1].id
        assert result[0].id != result[1].id

    async def test_three_scopes_same_key_all_new(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        args_list = [
            _make_args(idempotency_key="shared-key", idempotency_scope="run-A"),
            _make_args(idempotency_key="shared-key", idempotency_scope="run-B"),
            _make_args(idempotency_key="shared-key", idempotency_scope="run-C"),
        ]

        result = await backend.enqueue_batch(args_list)
        assert len(result) == 3

        ids = {str(r.id) for r in result}
        assert len(ids) == 3

    async def test_scoped_and_unscoped_same_key_distinct(self, clean_jobs_app: JobsApp) -> None:
        """A scoped key and an unscoped (default "") key in the same batch
        with the same idempotency_key are distinct jobs."""
        backend = clean_jobs_app.backend

        args_list = [
            _make_args(idempotency_key="shared-key", idempotency_scope="run-A"),
            _make_args(idempotency_key="shared-key", idempotency_scope=""),
        ]

        result = await backend.enqueue_batch(args_list)
        assert len(result) == 2

        assert result[0].id != result[1].id


# ── Same key+scope collides against pre-existing row ───────────


class TestSameKeySameScopeCollidesInBatch:
    """Same key+scope pair within a batch collides correctly against a
    pre-existing row — returns the original row's data."""

    async def test_collision_returns_pre_existing_row(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        # Pre-populate with a scoped key
        pre_args = _make_args(
            actor="pre_actor",
            idempotency_key="batch-scope-collision",
            idempotency_scope="run-A",
            payload={"pre": True},
        )
        pre_batch = await backend.enqueue_batch([pre_args])
        pre_row = pre_batch[0]

        # Batch with same key+scope → collision
        collision_args = _make_args(
            actor="collision_actor",
            idempotency_key="batch-scope-collision",
            idempotency_scope="run-A",
            payload={"should_be_ignored": True},
        )
        result = await backend.enqueue_batch([collision_args])
        assert len(result) == 1

        # Should return the pre-existing row, not the new args
        assert result[0].id == pre_row.id
        assert result[0].actor == "pre_actor"
        assert result[0].payload == {"pre": True}

    async def test_collision_same_scope_but_different_key_is_new(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Same scope but different key → new row, not a collision."""
        backend = clean_jobs_app.backend

        pre_args = _make_args(
            idempotency_key="key-1",
            idempotency_scope="run-A",
        )
        await backend.enqueue_batch([pre_args])

        new_args = _make_args(
            idempotency_key="key-2",
            idempotency_scope="run-A",
        )
        result = await backend.enqueue_batch([new_args])
        assert len(result) == 1
        assert result[0].id == new_args.id


# ── Mixed: collision in one scope, new in another ─────────────


class TestMixedScopeBatch:
    """Batch with a collision in one scope and a new row in another scope."""

    async def test_mixed_collision_and_new_across_scopes(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        # Pre-populate scope-A
        pre_args = _make_args(
            actor="pre_actor",
            idempotency_key="mixed-key",
            idempotency_scope="scope-A",
            payload={"pre": True},
        )
        pre_batch = await backend.enqueue_batch([pre_args])
        pre_row = pre_batch[0]

        # Batch: [collision in scope-A, new in scope-B with same key]
        args_collision = _make_args(
            actor="collision_actor",
            idempotency_key="mixed-key",
            idempotency_scope="scope-A",
            payload={"should_be_ignored": True},
        )
        args_new = _make_args(
            actor="new_actor",
            idempotency_key="mixed-key",
            idempotency_scope="scope-B",
            payload={"new": True},
        )

        result = await backend.enqueue_batch([args_collision, args_new])
        assert len(result) == 2

        # First: collision → original row
        assert result[0].id == pre_row.id
        assert result[0].actor == "pre_actor"
        assert result[0].payload == {"pre": True}

        # Second: new row in scope-B
        assert result[1].id == args_new.id
        assert result[1].actor == "new_actor"
        assert result[1].payload == {"new": True}
