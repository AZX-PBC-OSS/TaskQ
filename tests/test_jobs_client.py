"""Tests for JobsClient public API (unit tests).

Covers:
- JobsClient.cancel on a pending job and on a terminal job.
- JobsClient.enqueue returns JobHandle whose job_id matches inserted row.
- JobsClient.enqueue with duplicate idempotency_key returns JobHandle
  whose job_id matches the first call.
- JobsClient.list paginates correctly against the in-memory backend.
- JobsClient.get(<unknown_id>) returns None (not raise).
- Singleton actor enqueue injects singleton metadata.
- Non-singleton actor enqueue leaves metadata untouched.
- Singleton actor + custom metadata preserves user keys.
- Singleton actor + caller-supplied singleton=False is overridden.
- Non-mutation regression: caller metadata dict is not mutated.
- Schedule CRUD: create_schedule, list_schedules, update_schedule, delete_schedule.

Integration tests live in ``test_jobs_client_integration.py``.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_job_id
from taskq.actor import actor
from taskq.backend._protocol import EnqueueArgs, JobFilter, ScheduleRecord
from taskq.client import CancelResult, JobHandle, JobsClient
from taskq.client._args import build_enqueue_args
from taskq.cron import ScheduleHandle
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_RA: TypeAdapter[None] = TypeAdapter(type(None))


def _make_client() -> tuple[InMemoryBackend, JobsClient]:
    backend = InMemoryBackend(clock=FakeClock(_START))
    client = JobsClient(backend)
    return backend, client


async def _enqueue(
    backend: InMemoryBackend, client: JobsClient, args: EnqueueArgs
) -> JobHandle[None]:
    """Enqueue via the backend and return a JobHandle wrapping the result."""
    row = await backend.enqueue(args)
    return JobHandle(client=client, row=row, result_adapter=_RA, was_existing=False)


# ── cancel ────────────────────────────────────────────────────────────


class TestCancel:
    """JobsClient.cancel on pending and terminal jobs."""

    async def test_cancel_pending_job(self) -> None:
        """Cancel a pending job: CancelResult with
        previous_status='pending', new_status='cancelled',
        cancellation_initiated=True.
        """
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        result = await client.cancel(handle.job_id)

        assert isinstance(result, CancelResult)
        assert result.previous_status == "pending"
        assert result.new_status == "cancelled"
        assert result.cancellation_initiated is True
        assert result.job_id == handle.job_id

    async def test_cancel_succeeded_job(self) -> None:
        """Cancel a succeeded (terminal) job: CancelResult with
        cancellation_initiated=False, previous_status='succeeded',
        new_status='succeeded'.
        """
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        await _enqueue(backend, client, args)

        # Run the job to completion
        backend.register_stub(
            "test_actor",
            lambda payload, ctx: {"ok": True},
        )
        await backend.run_until_drained()

        # Now cancel the succeeded job
        row = await backend.get(args.id)
        assert row is not None
        result = await client.cancel(row.id)

        assert isinstance(result, CancelResult)
        assert result.cancellation_initiated is False
        assert result.previous_status == "succeeded"
        assert result.new_status == "succeeded"

    async def test_cancel_unknown_job_raises_key_error(self) -> None:
        """Cancel a non-existent job raises KeyError with the job_id."""
        _backend, client = _make_client()

        missing_id = new_job_id()
        with pytest.raises(KeyError, match=str(missing_id)):
            await client.cancel(missing_id)


# ── enqueue ────────────────────────────────────────────────────────────


class TestEnqueue:
    """JobsClient.enqueue returns JobHandle whose job_id matches
    the inserted row.
    """

    async def test_enqueue_returns_handle(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        assert isinstance(handle, JobHandle)
        assert handle.job_id == args.id
        assert handle.actor_name == "test_actor"
        assert handle.queue == "default"

    async def test_enqueue_row_matches(self) -> None:
        """The JobHandle.job_id matches the row stored in the backend."""
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.id == handle.job_id

    async def test_enqueue_duplicate_idempotency_key_returns_same_job(self) -> None:
        """Enqueue with duplicate idempotency_key returns a JobHandle
        whose job_id matches the first call.
        """
        backend, client = _make_client()

        args1 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        handle1 = await _enqueue(backend, client, args1)

        args2 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        handle2 = await _enqueue(backend, client, args2)

        assert handle2.job_id == handle1.job_id


# ── get ────────────────────────────────────────────────────────────────


class TestGet:
    """JobsClient.get returns None for unknown job_id."""

    async def test_get_unknown_returns_none(self) -> None:
        _backend, client = _make_client()

        result = await client.get(new_job_id(), result_adapter=_RA)
        assert result is None

    async def test_get_existing_returns_handle(self) -> None:
        backend, client = _make_client()

        args = make_enqueue_args(scheduled_at=_START)
        handle = await _enqueue(backend, client, args)

        found = await client.get(handle.job_id, result_adapter=_RA)
        assert found is not None
        assert found.job_id == handle.job_id


# ── list ───────────────────────────────────────────────────────────────


class TestList:
    """JobsClient.list paginates correctly."""

    async def test_list_returns_page(self) -> None:
        backend, client = _make_client()

        # Enqueue 3 jobs
        for _ in range(3):
            args = make_enqueue_args(scheduled_at=_START)
            await backend.enqueue(args)

        page = await client.list(JobFilter())
        assert len(page.jobs) == 3
        assert page.next_cursor is None

    async def test_list_with_limit_produces_cursor(self) -> None:
        backend, client = _make_client()

        # Enqueue 5 jobs
        for _ in range(5):
            args = make_enqueue_args(scheduled_at=_START)
            await backend.enqueue(args)

        page = await client.list(JobFilter(limit=3))
        assert len(page.jobs) == 3
        assert page.next_cursor is not None

    async def test_list_cursor_pagination(self) -> None:
        backend, client = _make_client()

        # Enqueue 5 jobs
        for _ in range(5):
            args = make_enqueue_args(scheduled_at=_START)
            await backend.enqueue(args)

        page1 = await client.list(JobFilter(limit=3))
        assert len(page1.jobs) == 3
        assert page1.next_cursor is not None

        page2 = await client.list(JobFilter(limit=3, cursor=page1.next_cursor))
        assert len(page2.jobs) == 2
        assert page2.next_cursor is None

    async def test_list_filters_by_queue(self) -> None:
        backend, client = _make_client()

        await backend.enqueue(make_enqueue_args(queue="alpha", scheduled_at=_START))
        await backend.enqueue(make_enqueue_args(queue="beta", scheduled_at=_START))
        await backend.enqueue(make_enqueue_args(queue="alpha", scheduled_at=_START))

        page = await client.list(JobFilter(queue="alpha"))
        assert len(page.jobs) == 2
        assert all(j.queue == "alpha" for j in page.jobs)


# ── Singleton metadata injection helpers ─────────────────────────────────


class _SingletonPayload(BaseModel):
    value: int = 1


@actor(name="_singleton_actor", singleton=True)
async def _singleton_actor(payload: _SingletonPayload) -> None:
    pass


@actor(name="_non_singleton_actor", singleton=False)
async def _non_singleton_actor(payload: _SingletonPayload) -> None:
    pass


# ── singleton metadata injection ────────────────────────────────────────


class TestSingletonMetadata:
    """metadata.singleton injection for singleton and non-singleton actors."""

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend)
        return backend, client

    async def test_singleton_actor_injects_metadata(self) -> None:
        """@actor(singleton=True) enqueue results in metadata containing {"singleton": True}."""
        backend, client = self._make_client()
        payload = _SingletonPayload(value=42)

        handle = await client.enqueue(_singleton_actor, payload)
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.metadata == {"singleton": True}

    async def test_non_singleton_actor_leaves_metadata_untouched(self) -> None:
        """@actor(singleton=False) enqueue results in metadata with no singleton key."""
        backend, client = self._make_client()
        payload = _SingletonPayload(value=42)

        handle = await client.enqueue(_non_singleton_actor, payload)
        row = await backend.get(handle.job_id)
        assert row is not None
        assert "singleton" not in row.metadata

    async def test_non_singleton_actor_preserves_caller_metadata(self) -> None:
        """@actor(singleton=False) with caller metadata preserves it unmodified."""
        backend, client = self._make_client()
        payload = _SingletonPayload(value=42)
        caller_meta: dict[str, object] = {"custom": "data", "user_id": 123}

        handle = await client.enqueue(_non_singleton_actor, payload, metadata=caller_meta)
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.metadata == {"custom": "data", "user_id": 123}
        assert "singleton" not in row.metadata

    async def test_singleton_actor_preserves_custom_keys(self) -> None:
        """Singleton actor + custom metadata preserves user keys and adds singleton."""
        backend, client = self._make_client()
        payload = _SingletonPayload(value=42)
        caller_meta: dict[str, object] = {"custom_key": "value"}

        handle = await client.enqueue(_singleton_actor, payload, metadata=caller_meta)
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.metadata == {"singleton": True, "custom_key": "value"}

    async def test_singleton_actor_overrides_caller_singleton_false(self) -> None:
        """Singleton actor + metadata={"singleton": False} produces {"singleton": True}."""
        backend, client = self._make_client()
        payload = _SingletonPayload(value=42)
        caller_meta: dict[str, object] = {"singleton": False}

        handle = await client.enqueue(_singleton_actor, payload, metadata=caller_meta)
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.metadata == {"singleton": True}

    async def test_caller_dict_not_mutated_after_enqueue(self) -> None:
        """Non-mutation regression: caller metadata dict is not mutated after enqueue."""
        _, client = self._make_client()
        payload = _SingletonPayload(value=42)
        caller_meta: dict[str, object] = {"correlation_id": "abc-123"}

        await client.enqueue(_singleton_actor, payload, metadata=caller_meta)
        assert caller_meta == {"correlation_id": "abc-123"}
        assert "singleton" not in caller_meta


# ── unique_for / unique_states: EnqueueArgs round-trip ──────────────────


def test_enqueue_args_unique_for_unique_states_round_trip() -> None:
    """EnqueueArgs(unique_for=..., unique_states=(...)) round-trips attribute access."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        unique_for=timedelta(minutes=15),
        unique_states=("pending", "scheduled"),
    )
    assert args.unique_for == timedelta(minutes=15)
    assert args.unique_states == ("pending", "scheduled")


def test_enqueue_args_unique_for_none_by_default() -> None:
    """EnqueueArgs() omitting unique_for leaves it as None."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    assert args.unique_for is None


def test_enqueue_args_unique_states_default() -> None:
    """EnqueueArgs() omitting unique_states defaults to ('pending', 'scheduled', 'running')."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    assert args.unique_states == ("pending", "scheduled", "running")


# ── unique_for / unique_states: JobsClient.enqueue wiring ───────────────


async def test_enqueue_passes_unique_for_and_unique_states_to_enqueue_args() -> None:
    """JobsClient.enqueue passes ref.unique_for and ref.unique_states to EnqueueArgs."""
    backend, client = _make_client()

    @actor(
        name="_unique_actor",
        unique_for=timedelta(minutes=30),
        unique_states=("pending",),
    )
    async def _unique_actor(payload: _SingletonPayload) -> None:
        pass

    captured: list[EnqueueArgs] = []
    original_enqueue = backend.enqueue

    async def capture_enqueue(args: EnqueueArgs):  # type: ignore[no-untyped-def]  # Why: monkeypatch for test capture; type matches Backend.enqueue.
        captured.append(args)
        return await original_enqueue(args)

    object.__setattr__(backend, "enqueue", capture_enqueue)

    payload = _SingletonPayload(value=1)
    await client.enqueue(_unique_actor, payload)

    assert len(captured) == 1
    assert captured[0].unique_for == timedelta(minutes=30)
    assert captured[0].unique_states == ("pending",)


# ── idempotency_key input validation ────────────────────────────────


class TestIdempotencyKeyValidation:
    """idempotency_key rejects empty, whitespace-only, and over-length values."""

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend)
        return backend, client

    async def test_empty_string_raises_value_error(self) -> None:
        _, client = self._make_client()
        payload = _SingletonPayload(value=1)
        with pytest.raises(ValueError, match="idempotency_key must not be empty"):
            await client.enqueue(_singleton_actor, payload, idempotency_key="")

    async def test_whitespace_only_raises_value_error(self) -> None:
        _, client = self._make_client()
        payload = _SingletonPayload(value=1)
        with pytest.raises(ValueError, match="idempotency_key must not be whitespace-only"):
            await client.enqueue(_singleton_actor, payload, idempotency_key="   ")

    async def test_tabs_and_newlines_are_whitespace(self) -> None:
        _, client = self._make_client()
        payload = _SingletonPayload(value=1)
        with pytest.raises(ValueError, match="idempotency_key must not be whitespace-only"):
            await client.enqueue(_singleton_actor, payload, idempotency_key="\t")
        with pytest.raises(ValueError, match="idempotency_key must not be whitespace-only"):
            await client.enqueue(_singleton_actor, payload, idempotency_key="\n")

    async def test_over_length_raises_value_error(self) -> None:
        _, client = self._make_client()
        payload = _SingletonPayload(value=1)
        too_long = "x" * 257
        with pytest.raises(
            ValueError, match="idempotency_key must be at most 256 characters, got 257"
        ):
            await client.enqueue(_singleton_actor, payload, idempotency_key=too_long)

    async def test_none_is_accepted(self) -> None:
        _, client = self._make_client()
        payload = _SingletonPayload(value=1)
        handle = await client.enqueue(_singleton_actor, payload, idempotency_key=None)
        assert handle.job_id is not None

    async def test_boundary_256_chars_is_accepted(self) -> None:
        _, client = self._make_client()
        payload = _SingletonPayload(value=1)
        key = "x" * 256
        handle = await client.enqueue(_singleton_actor, payload, idempotency_key=key)
        assert handle.job_id is not None

    async def test_valid_key_is_accepted(self) -> None:
        _, client = self._make_client()
        payload = _SingletonPayload(value=1)
        handle = await client.enqueue(_singleton_actor, payload, idempotency_key="webhook:123")
        assert handle.job_id is not None

    async def test_validation_runs_before_backend_call(self) -> None:
        backend, client = self._make_client()
        reached: list[bool] = [False]

        async def raise_immediately(_args: EnqueueArgs):
            reached[0] = True
            raise RuntimeError("backend should not be reached")

        object.__setattr__(backend, "enqueue", raise_immediately)

        payload = _SingletonPayload(value=1)
        with pytest.raises(ValueError, match="idempotency_key must not be empty"):
            await client.enqueue(_singleton_actor, payload, idempotency_key="")
        assert not reached[0], "backend was reached before validation fired"


# ── was_existing in JobsClient.enqueue ─────────────────────────────────


class _DedupPayload(BaseModel):
    value: int = 1


@actor(name="_fresh_actor")
async def _fresh_actor(payload: _DedupPayload) -> None:
    pass


@actor(
    name="_unique_for_dedup_actor",
    unique_for=timedelta(minutes=15),
    unique_states=("pending",),
)
async def _unique_for_dedup_actor(payload: _DedupPayload) -> None:
    pass


class TestEnqueueWasExisting:
    """was_existing detection in JobsClient.enqueue."""

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        clock = FakeClock(_START)
        backend = InMemoryBackend(clock=clock)
        client = JobsClient(backend, clock=clock)
        return backend, client

    async def test_enqueue_fresh_insert_was_existing_false(self) -> None:
        """Vanilla enqueue (no idempotency_key, no unique_for) returns was_existing == False."""
        backend, client = self._make_client()

        handle = await client.enqueue(_fresh_actor, _DedupPayload(value=42))

        assert handle.was_existing is False
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.id == handle.job_id

    async def test_enqueue_idempotency_dedup_was_existing_true(self) -> None:
        """Second enqueue with same idempotency_key returns was_existing == True."""
        _backend, client = self._make_client()

        handle1 = await client.enqueue(
            _fresh_actor, _DedupPayload(value=42), idempotency_key="dedup-key-1"
        )
        handle2 = await client.enqueue(
            _fresh_actor, _DedupPayload(value=99), idempotency_key="dedup-key-1"
        )

        assert handle1.was_existing is False
        assert handle2.was_existing is True
        assert handle2.job_id == handle1.job_id

    async def test_enqueue_unique_for_dedup_was_existing_true(self) -> None:
        """Second enqueue within the unique_for window returns was_existing == True."""
        backend, client = self._make_client()
        clock = backend._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock for time control

        identity = "account:42"
        handle1 = await client.enqueue(
            _unique_for_dedup_actor,
            _DedupPayload(value=1),
            identity_key=identity,
        )
        # Same identity, within the unique_for window
        clock.advance(timedelta(minutes=5))  # type: ignore[reportAttributeAccessIssue]  # Why: FakeClock.advance is not on the Clock Protocol; test-only cast through _clock attr
        handle2 = await client.enqueue(
            _unique_for_dedup_actor,
            _DedupPayload(value=2),
            identity_key=identity,
        )

        assert handle1.was_existing is False
        assert handle2.was_existing is True
        assert handle2.job_id == handle1.job_id


# ── singleton + unique_for composition ──────────────────────────────────


class _SingletonDedupPayload(BaseModel):
    value: int = 1


@actor(
    name="_singleton_dedup_actor",
    singleton=True,
    unique_for=timedelta(minutes=15),
    unique_states=("pending",),
)
async def _singleton_dedup_actor(payload: _SingletonDedupPayload) -> None:
    pass


@actor(
    name="_nodup_no_identity_actor",
    unique_for=timedelta(minutes=15),
    unique_states=("pending",),
)
async def _nodup_no_identity_actor(payload: _SingletonDedupPayload) -> None:
    pass


async def test_singleton_plus_unique_for_unique_for_fires_first() -> None:
    """Actor with singleton=True AND unique_for; second enqueue within
    window returns was_existing=True via unique_for; no SingletonCollisionError."""
    clock = FakeClock(_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend, clock=clock)

    identity = "account:42"
    handle1 = await client.enqueue(
        _singleton_dedup_actor,
        _SingletonDedupPayload(value=1),
        identity_key=identity,
    )
    assert handle1.was_existing is False

    handle2 = await client.enqueue(
        _singleton_dedup_actor,
        _SingletonDedupPayload(value=2),
        identity_key=identity,
    )

    assert handle2.was_existing is True
    assert handle2.job_id == handle1.job_id


# ── unique_for with no identity → no dedup ──────────────────────────────


async def test_unique_for_no_identity_no_dedup_fresh_jobs() -> None:
    """Actor with unique_for but no identity_key passed at enqueue;
    no dedup occurs; fresh job created on every call."""
    clock = FakeClock(_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend, clock=clock)

    handle1 = await client.enqueue(
        _nodup_no_identity_actor,
        _SingletonDedupPayload(value=1),
    )
    handle2 = await client.enqueue(
        _nodup_no_identity_actor,
        _SingletonDedupPayload(value=2),
    )

    assert handle1.was_existing is False
    assert handle2.was_existing is False
    assert handle1.job_id != handle2.job_id


# ── constraint-name disambiguation (SKIPPED) ────────────────────────────


@pytest.mark.skip(
    reason=(
        "InMemoryBackend has no Layer 2 unique constraint equivalent for singleton; "
        "monkeypatching the singleton preflight to return None results in both "
        "inserts succeeding with no error raised. Integration test covers "
        "this against PG."
    )
)
async def test_singleton_constraint_disambiguation() -> None:
    """Constraint-name disambiguation — a singleton actor's preflight
    is monkey-patched to None; in the PG backend the second INSERT raises
    UniqueViolationError on jobs_singleton_uniq, caught as
    SingletonCollisionError (not a dedup return). InMemoryBackend lacks this
    Layer 2 and is skipped. Equivalent integration test covers this
    against PG."""


# ── max_pending: EnqueueArgs round-trip ─────────────────────────────────


def test_enqueue_args_max_pending_explicit_value() -> None:
    """EnqueueArgs(max_pending=5) round-trips attribute access."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        max_pending=5,
    )
    assert args.max_pending == 5


def test_enqueue_args_max_pending_none_by_default() -> None:
    """EnqueueArgs() omitting max_pending leaves it as None."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    assert args.max_pending is None


# ── max_pending: JobsClient.enqueue wiring ──────────────────────────────


async def test_enqueue_passes_max_pending_to_enqueue_args() -> None:
    """JobsClient.enqueue passes ref.max_pending to EnqueueArgs."""
    backend, client = _make_client()

    @actor(
        name="_max_pending_actor_5",
        max_pending=5,
    )
    async def _max_pending_actor_5(payload: _SingletonPayload) -> None:
        pass

    captured: list[EnqueueArgs] = []
    original_enqueue = backend.enqueue

    async def capture_enqueue(args: EnqueueArgs):  # type: ignore[no-untyped-def]  # Why: monkeypatch for test capture; type matches Backend.enqueue.
        captured.append(args)
        return await original_enqueue(args)

    object.__setattr__(backend, "enqueue", capture_enqueue)

    payload = _SingletonPayload(value=1)
    await client.enqueue(_max_pending_actor_5, payload)

    assert len(captured) == 1
    assert captured[0].max_pending == 5


async def test_enqueue_passes_max_pending_none_when_unset() -> None:
    """JobsClient.enqueue with actor having no max_pending defaults EnqueueArgs.max_pending to None."""
    backend, client = _make_client()

    @actor(name="_max_pending_default_actor")
    async def _max_pending_default_actor(payload: _SingletonPayload) -> None:
        pass

    captured: list[EnqueueArgs] = []
    original_enqueue = backend.enqueue

    async def capture_enqueue(args: EnqueueArgs):  # type: ignore[no-untyped-def]  # Why: monkeypatch for test capture; type matches Backend.enqueue.
        captured.append(args)
        return await original_enqueue(args)

    object.__setattr__(backend, "enqueue", capture_enqueue)

    payload = _SingletonPayload(value=1)
    await client.enqueue(_max_pending_default_actor, payload)

    assert len(captured) == 1
    assert captured[0].max_pending is None


# ── regression: JobsClient.enqueue handle wait works ──────────


class TestEnqueueHandleWaitRegression:
    """Regression guard for the JobHandle constructor signature change
    JobsClient.enqueue must still return a handle whose
    wait() works after the client/backend decoupling.
    """

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend)
        return backend, client

    async def test_enqueue_handle_wait_works(self) -> None:
        """JobsClient.enqueue → handle.wait() returns after the job
        completes (regression guard for constructor signature change).
        """
        backend, client = self._make_client()

        backend.register_stub(
            "test_actor",
            lambda payload, ctx: None,
        )

        args = make_enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        handle = JobHandle(client=client, row=row, result_adapter=_RA, was_existing=False)

        await backend.run_until_drained()
        await handle.wait(timeout=2.0)

        refreshed = await handle.refresh()
        assert refreshed.status == "succeeded"


# ── build_enqueue_args extraction regression ──────────────────────────


class TestBuildEnqueueArgs:
    """Regression guard: build_enqueue_args produces EnqueueArgs matching
    the structure that JobsClient.enqueue would have built inline.
    """

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend, clock=backend._clock)  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock for deterministic comparison
        return backend, client

    async def test_args_match_enqueue_output(self) -> None:
        """build_enqueue_args produces EnqueueArgs whose fields match what
        JobsClient.enqueue would have produced inline.
        """
        backend, client = self._make_client()
        clock = backend._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock for deterministic comparison

        args = build_enqueue_args(
            _fresh_actor,
            _DedupPayload(value=42),
            clock=clock,
        )

        handle = await client.enqueue(_fresh_actor, _DedupPayload(value=42))

        assert args.actor == handle.actor_name
        assert args.queue == handle.queue
        assert args.max_attempts == _fresh_actor.retry.max_attempts
        assert args.retry_kind == _fresh_actor.retry.kind
        assert args.priority == 0
        assert args.max_pending is None
        assert args.identity_key is None
        assert args.idempotency_key is None
        assert args.unique_for is None
        assert args.unique_states == _fresh_actor.unique_states
        assert args.metadata == {}

    async def test_singleton_actor_args(self) -> None:
        """build_enqueue_args for a singleton actor injects metadata."""
        _, client = self._make_client()
        clock = client._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock

        args = build_enqueue_args(
            _singleton_actor,
            _SingletonPayload(value=1),
            clock=clock,
        )

        assert args.metadata == {"singleton": True}

    async def test_idempotency_key_validation_in_helper(self) -> None:
        """build_enqueue_args raises ValueError on invalid idempotency_key."""
        _, client = self._make_client()
        clock = client._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock

        with pytest.raises(ValueError, match="idempotency_key must not be empty"):
            build_enqueue_args(
                _fresh_actor,
                _DedupPayload(value=1),
                idempotency_key="",
                clock=clock,
            )

    async def test_unique_for_from_ref(self) -> None:
        """build_enqueue_args uses ref.unique_for when not overridden."""
        _, client = self._make_client()
        clock = client._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock

        args = build_enqueue_args(
            _unique_for_dedup_actor,
            _DedupPayload(value=1),
            clock=clock,
        )

        assert args.unique_for == timedelta(minutes=15)
        assert args.unique_states == ("pending",)

    async def test_max_pending_from_ref(self) -> None:
        """build_enqueue_args uses ref.max_pending when not overridden."""
        _, client = self._make_client()
        clock = client._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock

        @actor(name="_bargs_max_pending", max_pending=5)
        async def _bargs_max_pending(payload: _SingletonPayload) -> None:
            pass

        args = build_enqueue_args(
            _bargs_max_pending,
            _SingletonPayload(value=1),
            clock=clock,
        )

        assert args.max_pending == 5

    async def test_start_to_close_per_call_wins_over_actor_default(self) -> None:
        """build_enqueue_args resolves per-call start_to_close over the
        actor's declared default when both are given."""
        _, client = self._make_client()
        clock = client._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock

        @actor(name="_bargs_stc_both", start_to_close=timedelta(minutes=1))
        async def _bargs_stc_both(payload: _SingletonPayload) -> None:
            pass

        args = build_enqueue_args(
            _bargs_stc_both,
            _SingletonPayload(value=1),
            start_to_close=timedelta(seconds=30),
            clock=clock,
        )

        assert args.start_to_close == timedelta(seconds=30)

    async def test_start_to_close_falls_back_to_actor_default(self) -> None:
        """build_enqueue_args uses ref.start_to_close when the per-call
        override is None."""
        _, client = self._make_client()
        clock = client._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock

        @actor(name="_bargs_stc_actor_default", start_to_close=timedelta(minutes=2))
        async def _bargs_stc_actor_default(payload: _SingletonPayload) -> None:
            pass

        args = build_enqueue_args(
            _bargs_stc_actor_default,
            _SingletonPayload(value=1),
            clock=clock,
        )

        assert args.start_to_close == timedelta(minutes=2)

    async def test_start_to_close_none_when_unset_everywhere(self) -> None:
        """build_enqueue_args yields start_to_close=None when neither the
        per-call override nor the actor default is set."""
        _, client = self._make_client()
        clock = client._clock  # type: ignore[reportPrivateUsage]  # Why: test-only access to FakeClock

        args = build_enqueue_args(
            _fresh_actor,
            _DedupPayload(value=1),
            clock=clock,
        )

        assert args.start_to_close is None


# ── Schedule CRUD ──────────────────────────────────────────────────────


class TestCreateSchedule:
    """JobsClient.create_schedule validation and creation."""

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend)
        return backend, client

    async def test_invalid_cron_expr_raises_value_error(self) -> None:
        """create_schedule with invalid cron_expr raises ValueError
        before any DB call.
        """
        backend, client = self._make_client()
        reached: list[bool] = [False]

        async def fail_create(_args: object) -> None:
            reached[0] = True
            raise RuntimeError("backend should not be reached")

        object.__setattr__(backend, "create_schedule", fail_create)

        with pytest.raises(ValueError, match="Invalid cron expression"):
            await client.create_schedule("my_actor", "not-valid-cron")
        assert not reached[0], "backend was reached before validation fired"

    async def test_both_payload_factory_and_static_payload_raises(self) -> None:
        """create_schedule with both payload_factory and static_payload
        raises ValueError before any DB call.
        """
        backend, client = self._make_client()
        reached: list[bool] = [False]

        async def fail_create(_args: object) -> None:
            reached[0] = True
            raise RuntimeError("backend should not be reached")

        object.__setattr__(backend, "create_schedule", fail_create)

        with pytest.raises(ValueError, match="mutually exclusive"):
            await client.create_schedule(
                "my_actor",
                "0 * * * *",
                payload_factory="mymod.fn",
                static_payload={"key": "val"},
            )
        assert not reached[0], "backend was reached before validation fired"

    async def test_create_returns_schedule_handle(self) -> None:
        """create_schedule returns a ScheduleHandle with correct fields."""
        _backend, client = self._make_client()

        handle = await client.create_schedule(
            "ticker",
            "*/5 * * * *",
            timezone="UTC",
            name="ticker-schedule",
        )

        assert isinstance(handle, ScheduleHandle)
        assert handle.actor == "ticker"
        assert handle.cron_expr == "*/5 * * * *"
        assert handle.timezone == "UTC"
        assert handle.enabled is True
        assert isinstance(handle.schedule_id, UUID)
        assert handle.next_fire_at > _START

    async def test_create_with_actor_ref_extracts_name(self) -> None:
        """create_schedule with an ActorRef extracts the actor name."""
        _backend, client = self._make_client()

        handle = await client.create_schedule(
            _fresh_actor,
            "*/5 * * * *",
        )

        assert handle.actor == "_fresh_actor"

    async def test_create_with_static_payload_stores_in_metadata(self) -> None:
        """create_schedule with static_payload stores it in metadata."""
        _backend, client = self._make_client()

        _handle = await client.create_schedule(
            "my_actor",
            "0 * * * *",
            static_payload={"key": "val"},
        )

        record = await client.list_schedules(actor="my_actor")
        assert len(record) == 1
        assert record[0].metadata.get("static_payload") == {"key": "val"}

    async def test_create_with_name_stores_in_name_column(self) -> None:
        """create_schedule with name stores it in the schedule's name column."""
        _backend, client = self._make_client()

        _handle = await client.create_schedule(
            "my_actor",
            "0 * * * *",
            name="my-schedule",
        )

        record = await client.list_schedules(actor="my_actor")
        assert len(record) == 1
        assert record[0].name == "my-schedule"

    async def test_create_with_identity_key_stores_on_schedule(self) -> None:
        """create_schedule with identity_key stores it on the schedule so
        cron-fired jobs dedup against on-demand jobs for that business key."""
        from taskq.backend._protocol import IdentityKey

        _backend, client = self._make_client()

        handle = await client.create_schedule(
            "entity_sync",
            "0 * * * *",
            name="prop-123",
            identity_key=IdentityKey("sync:entity:123"),
        )

        assert handle.identity_key == IdentityKey("sync:entity:123")
        record = await client.list_schedules(actor="entity_sync")
        assert len(record) == 1
        assert record[0].identity_key == "sync:entity:123"
        assert record[0].name == "prop-123"

    async def test_create_same_actor_different_names_succeeds(self) -> None:
        """Per-property cron: the same actor may have multiple schedules
        distinguished by name under UNIQUE(actor, name)."""
        _backend, client = self._make_client()

        await client.create_schedule("entity_sync", "0 * * * *", name="prop-123")
        await client.create_schedule("entity_sync", "0 * * * *", name="prop-456")

        records = await client.list_schedules(actor="entity_sync")
        names = sorted(r.name for r in records)
        assert names == ["prop-123", "prop-456"]

    async def test_create_same_actor_same_name_raises(self) -> None:
        """Duplicate (actor, name) is rejected by the backend uniqueness gate."""
        _backend, client = self._make_client()

        await client.create_schedule("entity_sync", "0 * * * *", name="prop-123")
        with pytest.raises(ValueError, match="already exists"):
            await client.create_schedule("entity_sync", "0 * * * *", name="prop-123")

    async def test_create_same_actor_empty_name_backward_compat_raises(self) -> None:
        """A second schedule for the same actor with the default empty name
        is rejected — preserves the pre-migration actor-only uniqueness."""
        _backend, client = self._make_client()

        await client.create_schedule("solo_actor", "0 * * * *")
        with pytest.raises(ValueError, match="already exists"):
            await client.create_schedule("solo_actor", "0 * * * *")

    async def test_create_compute_next_fire_at(self) -> None:
        """create_schedule computes next_fire_at from cron_expr and timezone."""
        _backend, client = self._make_client()

        handle = await client.create_schedule(
            "my_actor",
            "0 3 * * *",
            timezone="UTC",
        )

        assert handle.next_fire_at.hour == 3
        assert handle.next_fire_at.minute == 0


class TestListSchedules:
    """JobsClient.list_schedules filtering."""

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend)
        return backend, client

    async def test_list_returns_all(self) -> None:
        """list_schedules with no filters returns all schedules."""
        _backend, client = self._make_client()

        await client.create_schedule("actor_a", "0 * * * *")
        await client.create_schedule("actor_b", "0 */2 * * *")

        records = await client.list_schedules()
        assert len(records) == 2

    async def test_list_filters_by_actor(self) -> None:
        """list_schedules filters by actor."""
        _backend, client = self._make_client()

        await client.create_schedule("actor_a", "0 * * * *")
        await client.create_schedule("actor_b", "0 */2 * * *")

        records = await client.list_schedules(actor="actor_a")
        assert len(records) == 1
        assert records[0].actor == "actor_a"

    async def test_list_filters_by_enabled(self) -> None:
        """list_schedules filters by enabled status."""
        _backend, client = self._make_client()

        await client.create_schedule("actor_a", "0 * * * *", enabled=True)
        await client.create_schedule("actor_b", "0 */2 * * *", enabled=False)

        records = await client.list_schedules(enabled=True)
        assert len(records) == 1
        assert records[0].actor == "actor_a"

    async def test_list_returns_schedule_records(self) -> None:
        """list_schedules returns ScheduleRecord instances."""
        _backend, client = self._make_client()

        await client.create_schedule("actor_a", "0 * * * *")

        records = await client.list_schedules()
        assert len(records) == 1
        assert isinstance(records[0], ScheduleRecord)
        assert records[0].consecutive_failures == 0
        assert records[0].last_fired_at is None


class TestUpdateSchedule:
    """JobsClient.update_schedule validation and updates."""

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend)
        return backend, client

    async def test_invalid_cron_expr_raises_value_error(self) -> None:
        """update_schedule with invalid cron_expr raises ValueError."""
        backend, client = self._make_client()
        reached: list[bool] = [False]

        async def fail_update(_id: object, _args: object) -> None:
            reached[0] = True
            raise RuntimeError("backend should not be reached")

        object.__setattr__(backend, "update_schedule", fail_update)

        with pytest.raises(ValueError, match="Invalid cron expression"):
            await client.update_schedule(
                UUID("00000000-0000-0000-0000-000000000001"),
                cron_expr="not-valid",
            )
        assert not reached[0], "backend was reached before validation fired"

    async def test_both_payload_factory_and_static_payload_raises(self) -> None:
        """update_schedule with both payload_factory and static_payload
        raises ValueError.
        """
        _backend, client = self._make_client()

        with pytest.raises(ValueError, match="mutually exclusive"):
            await client.update_schedule(
                UUID("00000000-0000-0000-0000-000000000001"),
                payload_factory="mymod.fn",
                static_payload={"key": "val"},
            )

    async def test_enabled_true_resets_consecutive_failures(self) -> None:
        """update_schedule with enabled=True resets consecutive_failures
        and clears last_fire_error.
        """
        _backend, client = self._make_client()

        handle = await client.create_schedule("my_actor", "0 * * * *")

        # Simulate failures by directly updating the backend store
        schedule_id = handle.schedule_id
        records = await client.list_schedules(actor="my_actor")
        assert len(records) == 1
        record = records[0].model_copy(
            update={"consecutive_failures": 3, "last_fire_error": "boom", "enabled": False}
        )
        _backend._schedules[schedule_id] = record

        updated = await client.update_schedule(schedule_id, enabled=True)

        assert updated.consecutive_failures == 0
        assert updated.last_fire_error is None
        assert updated.enabled is True

    async def test_update_cron_expr_recomputes_next_fire_at(self) -> None:
        """update_schedule with cron_expr recomputes next_fire_at."""
        _backend, client = self._make_client()

        handle = await client.create_schedule("my_actor", "0 * * * *")
        schedule_id = handle.schedule_id

        updated = await client.update_schedule(schedule_id, cron_expr="0 3 * * *")

        assert updated.cron_expr == "0 3 * * *"
        assert updated.next_fire_at.hour == 3

    async def test_clear_payload_factory_sets_none(self) -> None:
        """update_schedule with clear_payload_factory=True sets payload_factory to None."""
        _backend, client = self._make_client()

        handle = await client.create_schedule(
            "my_actor", "0 * * * *", payload_factory="some.module.fn"
        )

        updated = await client.update_schedule(handle.schedule_id, clear_payload_factory=True)

        assert updated.payload_factory is None

    async def test_update_returns_schedule_record(self) -> None:
        """update_schedule returns the updated ScheduleRecord."""
        _backend, client = self._make_client()

        handle = await client.create_schedule("my_actor", "0 * * * *")
        updated = await client.update_schedule(handle.schedule_id, enabled=False)

        assert isinstance(updated, ScheduleRecord)
        assert updated.enabled is False


class TestDeleteSchedule:
    """JobsClient.delete_schedule idempotent deletion."""

    @staticmethod
    def _make_client() -> tuple[InMemoryBackend, JobsClient]:
        backend = InMemoryBackend(clock=FakeClock(_START))
        client = JobsClient(backend)
        return backend, client

    async def test_delete_removes_schedule(self) -> None:
        """delete_schedule removes the schedule from the backend."""
        _backend, client = self._make_client()

        handle = await client.create_schedule("my_actor", "0 * * * *")
        await client.delete_schedule(handle.schedule_id)

        records = await client.list_schedules()
        assert len(records) == 0

    async def test_delete_idempotent(self) -> None:
        """delete_schedule does not raise if the schedule does not exist."""
        _backend, client = self._make_client()

        await client.delete_schedule(UUID("00000000-0000-0000-0000-000000000001"))

    async def test_delete_twice_no_error(self) -> None:
        """Deleting the same schedule twice does not raise."""
        _backend, client = self._make_client()

        handle = await client.create_schedule("my_actor", "0 * * * *")
        await client.delete_schedule(handle.schedule_id)
        await client.delete_schedule(handle.schedule_id)
