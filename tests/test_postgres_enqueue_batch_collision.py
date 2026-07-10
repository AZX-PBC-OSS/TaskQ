"""Integration tests for PostgresBackend ``enqueue_batch()`` collision resolution.

Tests the RETURNING + follow-up SELECT path when idempotency keys collide
during batch insertion.
"""

from datetime import UTC, datetime

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


_IDEMP_KEY_1 = "idemp-key-collision-1"
_IDEMP_KEY_2 = "idemp-key-collision-2"
_IDEMP_KEY_3 = "idemp-key-collision-3"


def _make_args(
    *,
    actor: str = "test_actor",
    queue: str = "default",
    payload: dict[str, object] | None = None,
    idempotency_key: str | None = None,
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
    )


# ── First enqueue wins, second returns existing rows ───────────────────────


class TestFirstEnqueueWins:
    """When an idempotency key already exists, the second batch returns the
    previously-inserted row data."""

    async def test_second_batch_returns_original_row_with_existing_key(
        self, clean_jobs_app: JobsApp
    ) -> None:
        backend = clean_jobs_app.backend

        # First enqueue — inserts a new row
        args_a = _make_args(idempotency_key=_IDEMP_KEY_1)
        batch1 = await backend.enqueue_batch([args_a])
        assert len(batch1) == 1
        original = batch1[0]

        # Second enqueue with same key — should return existing row
        args_b = _make_args(idempotency_key=_IDEMP_KEY_1)
        batch2 = await backend.enqueue_batch([args_b])
        assert len(batch2) == 1
        collision = batch2[0]

        # The collision result should have the ORIGINAL job's id, not args_b's
        assert collision.id == original.id
        assert collision.idempotency_key == IdempotencyKey(_IDEMP_KEY_1)

    async def test_collision_row_has_original_data_not_second_requests(
        self, clean_jobs_app: JobsApp
    ) -> None:
        backend = clean_jobs_app.backend

        # First enqueue
        args_a = _make_args(
            actor="actor_a",
            idempotency_key=_IDEMP_KEY_2,
            payload={"first": True},
        )
        batch1 = await backend.enqueue_batch([args_a])
        original = batch1[0]

        # Second enqueue with different actor and payload, same idempotency key
        args_b = _make_args(
            actor="actor_b",
            idempotency_key=_IDEMP_KEY_2,
            payload={"second": True},
        )
        batch2 = await backend.enqueue_batch([args_b])
        collision = batch2[0]

        # The collision should return the ORIGINAL data
        assert collision.id == original.id
        assert collision.actor == "actor_a"  # NOT actor_b
        assert collision.payload == {"first": True}  # NOT {"second": True}


# ── Mixed new and collision ────────────────────────────────────────────────


class TestMixedNewAndCollision:
    """Batch with both new items and items whose keys already exist."""

    async def test_mixed_batch_returns_correct_order(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        # Pre-populate one key
        pre_args = _make_args(
            actor="actor_a",
            idempotency_key=_IDEMP_KEY_1,
            payload={"pre": True},
        )
        pre_batch = await backend.enqueue_batch([pre_args])
        pre_row = pre_batch[0]

        # Now enqueue a batch: [collision, new, new]
        args_collision = _make_args(
            actor="actor_collision",
            idempotency_key=_IDEMP_KEY_1,  # same key as pre
            payload={"should_be_ignored": True},
        )
        args_new_1 = _make_args(actor="actor_b", payload={"new_1": True})
        args_new_2 = _make_args(actor="actor_c", payload={"new_2": True})

        batch = await backend.enqueue_batch([args_collision, args_new_1, args_new_2])
        assert len(batch) == 3

        # First result: collision → original row's data
        assert batch[0].id == pre_row.id
        assert batch[0].actor == "actor_a"
        assert batch[0].payload == {"pre": True}

        # Second and third: new rows
        assert batch[1].id == args_new_1.id
        assert batch[1].actor == "actor_b"
        assert batch[1].payload == {"new_1": True}

        assert batch[2].id == args_new_2.id
        assert batch[2].actor == "actor_c"
        assert batch[2].payload == {"new_2": True}


# ── Multiple collisions in one batch ───────────────────────────────────────


class TestMultipleCollisions:
    """Batch where multiple items match existing idempotency keys."""

    async def test_two_collisions_three_new_in_original_order(
        self, clean_jobs_app: JobsApp
    ) -> None:
        backend = clean_jobs_app.backend

        # Pre-populate two keys
        pre_1 = _make_args(
            actor="pre_actor_1",
            idempotency_key=_IDEMP_KEY_1,
            payload={"pre": 1},
        )
        pre_2 = _make_args(
            actor="pre_actor_2",
            idempotency_key=_IDEMP_KEY_2,
            payload={"pre": 2},
        )
        pre_rows = await backend.enqueue_batch([pre_1, pre_2])
        pre_row_1 = pre_rows[0]
        pre_row_2 = pre_rows[1]

        # Batch of 5: [new_A, collision_1, new_B, collision_2, new_C]
        args = [
            _make_args(actor="new_A", payload={"idx": 0}),
            _make_args(
                actor="collision_1",
                idempotency_key=_IDEMP_KEY_1,
                payload={"should_ignore": True},
            ),
            _make_args(actor="new_B", payload={"idx": 2}),
            _make_args(
                actor="collision_2",
                idempotency_key=_IDEMP_KEY_2,
                payload={"should_ignore": True},
            ),
            _make_args(actor="new_C", payload={"idx": 4}),
        ]

        result = await backend.enqueue_batch(args)
        assert len(result) == 5

        # Index 0: new_A
        assert result[0].id == args[0].id
        assert result[0].actor == "new_A"

        # Index 1: collision_1 → returns pre_row_1 data
        assert result[1].id == pre_row_1.id
        assert result[1].actor == "pre_actor_1"
        assert result[1].payload == {"pre": 1}

        # Index 2: new_B
        assert result[2].id == args[2].id
        assert result[2].actor == "new_B"

        # Index 3: collision_2 → returns pre_row_2 data
        assert result[3].id == pre_row_2.id
        assert result[3].actor == "pre_actor_2"
        assert result[3].payload == {"pre": 2}

        # Index 4: new_C
        assert result[4].id == args[4].id
        assert result[4].actor == "new_C"

    async def test_collision_items_without_idempotency_key_not_treated_as_collision(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Items without an idempotency key should always be inserted fresh,
        even if another item in the same batch has a matching actor."""
        backend = clean_jobs_app.backend

        # First batch: create a row with an idempotency key
        args_with_key = _make_args(
            actor="actor_a",
            idempotency_key=_IDEMP_KEY_1,
            payload={"with_key": True},
        )
        pre_rows = await backend.enqueue_batch([args_with_key])
        pre_row = pre_rows[0]

        # Second batch: mixed — same key (collision) and a new item without key
        args_collision = _make_args(
            actor="actor_collision",
            idempotency_key=_IDEMP_KEY_1,
        )
        args_no_key = _make_args(
            actor="actor_no_key",
            idempotency_key=None,  # explicitly no key
        )

        batch = await backend.enqueue_batch([args_collision, args_no_key])
        assert len(batch) == 2

        # Index 0: collision → original row
        assert batch[0].id == pre_row.id
        assert batch[0].payload == {"with_key": True}

        # Index 1: new row (no key, so always new)
        assert batch[1].id == args_no_key.id
        assert batch[1].actor == "actor_no_key"


# ── No collisions ──────────────────────────────────────────────────────────


class TestNoCollisions:
    """When all items have unique (or absent) idempotency keys, all get new rows."""

    async def test_all_unique_keys_all_new_rows(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        args_list = [
            _make_args(
                actor="actor_a",
                idempotency_key="unique-key-a",
                payload={"a": 1},
            ),
            _make_args(
                actor="actor_b",
                idempotency_key="unique-key-b",
                payload={"b": 2},
            ),
            _make_args(
                actor="actor_c",
                idempotency_key="unique-key-c",
                payload={"c": 3},
            ),
        ]

        result = await backend.enqueue_batch(args_list)
        assert len(result) == 3

        # Each result should have its own args.id
        for i, (res, args) in enumerate(zip(result, args_list, strict=True)):
            assert res.id == args.id, f"item {i}: result.id {res.id} != args.id {args.id}"
            assert res.actor == args.actor
            assert res.payload == args.payload

    async def test_no_idempotency_keys_all_new_rows(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        args_list = [
            _make_args(actor="actor_a", payload={"a": 1}),
            _make_args(actor="actor_b", payload={"b": 2}),
            _make_args(actor="actor_c", payload={"c": 3}),
        ]

        result = await backend.enqueue_batch(args_list)
        assert len(result) == 3

        for i, (res, args) in enumerate(zip(result, args_list, strict=True)):
            assert res.id == args.id, f"item {i}: result.id {res.id} != args.id {args.id}"
            assert res.actor == args.actor

    async def test_batch_repeated_actor_no_conflict(self, clean_jobs_app: JobsApp) -> None:
        """Same actor multiple times in a batch with different idempotency
        keys — each should get a new row."""
        backend = clean_jobs_app.backend

        args_list = [
            _make_args(
                actor="actor_a",
                idempotency_key="same-actor-key-1",
                payload={"idx": 0},
            ),
            _make_args(
                actor="actor_a",
                idempotency_key="same-actor-key-2",
                payload={"idx": 1},
            ),
            _make_args(
                actor="actor_a",
                idempotency_key="same-actor-key-3",
                payload={"idx": 2},
            ),
        ]

        result = await backend.enqueue_batch(args_list)
        assert len(result) == 3

        for i, (res, args) in enumerate(zip(result, args_list, strict=True)):
            assert res.id == args.id, f"item {i}"
            assert res.actor == "actor_a"
            assert res.payload == {"idx": i}
