"""Unit tests for SubJobEnqueuer.enqueue tags parameter and parent-tag inheritance."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.backend._protocol import JobId
from taskq.client._enqueuer import SubJobEnqueuer, _parent_tags_var, set_parent_tags
from taskq.retry import RetryPolicy
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(name: str = "child") -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


def _make_enqueuer(backend: InMemoryBackend | None = None) -> SubJobEnqueuer:
    if backend is None:
        backend = InMemoryBackend(clock=FakeClock(_NOW))
    return SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),
        backend=backend,
        clock=FakeClock(_NOW),
    )


class TestSubJobExplicitTags:
    async def test_explicit_tags_no_inheritance(self) -> None:
        """tags= with inherit_tags=False sets only explicit tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            tags=["alpha", "beta"],
            inherit_tags=False,
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("alpha", "beta")

    async def test_tags_validated(self) -> None:
        """Invalid tags raise ValueError."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        with pytest.raises(ValueError, match="invalid tag"):
            await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                tags=["ab"],
            )

    async def test_tags_deduplicated(self) -> None:
        """Duplicate tags are deduplicated."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            tags=["alpha", "alpha", "beta"],
            inherit_tags=False,
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("alpha", "beta")


class TestSubJobTagInheritance:
    async def test_inherit_parent_tags_default(self) -> None:
        """With no explicit tags and default inherit_tags=True, sub-job inherits parent tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001", "tenant-acme"))
        try:
            handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("run-001", "tenant-acme")

    async def test_inherit_and_merge_tags(self) -> None:
        """Explicit tags merge with parent tags (parent first, deduped)."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001", "tenant-acme"))
        try:
            handle = await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                tags=["stage-2", "tenant-acme"],
            )
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("run-001", "tenant-acme", "stage-2")

    async def test_no_parent_tags_no_explicit_tags(self) -> None:
        """With no parent tags and no explicit tags, sub-job has empty tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ()

    async def test_inherit_false_no_parent_tags(self) -> None:
        """inherit_tags=False with no explicit tags -> empty tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001",))
        try:
            handle = await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                inherit_tags=False,
            )
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ()

    async def test_inherit_false_with_explicit_tags(self) -> None:
        """inherit_tags=False with explicit tags -> only explicit tags."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001",))
        try:
            handle = await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                tags=["custom-tag"],
                inherit_tags=False,
            )
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("custom-tag",)

    async def test_empty_list_tags_with_inheritance(self) -> None:
        """tags=[] with inherit_tags=True and parent tags -> inherits parent tags only.

        An empty list means "no additional tags to add" -- parent tags
        are still inherited. This is the intuitive behavior: the caller
        didn't add any new tags, so the sub-job carries what the parent
        carried.
        """
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        token = set_parent_tags(("run-001", "tenant-acme"))
        try:
            handle = await enqueuer.enqueue(
                _make_actor_ref(),
                _Payload(),
                tags=[],
            )
        finally:
            _parent_tags_var.reset(token)

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("run-001", "tenant-acme")


class TestSubJobMissingFields:
    async def test_schedule_to_close(self) -> None:
        """schedule_to_close is accepted and stored on the row."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        deadline = _NOW + timedelta(hours=1)
        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            schedule_to_close=deadline,
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.schedule_to_close == deadline

    async def test_start_to_close(self) -> None:
        """start_to_close is accepted and stored on the row."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            start_to_close=timedelta(minutes=30),
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.start_to_close == timedelta(minutes=30)

    async def test_heartbeat_timeout(self) -> None:
        """heartbeat_timeout is accepted and stored on the row."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            heartbeat_timeout=timedelta(seconds=10),
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.heartbeat_timeout == timedelta(seconds=10)


class TestContextVarIsolation:
    async def test_concurrent_jobs_separate_parent_tags(self) -> None:
        """ContextVar ensures concurrent consumers don't share parent tags."""
        import asyncio

        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        async def enqueue_with_parent(parent_tags: tuple[str, ...]) -> UUID:
            token = set_parent_tags(parent_tags)
            try:
                handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
                return handle.job_id
            finally:
                _parent_tags_var.reset(token)

        id1, id2 = await asyncio.gather(
            enqueue_with_parent(("run-a",)),
            enqueue_with_parent(("run-b",)),
        )

        row1 = await backend.get(JobId(id1))
        row2 = await backend.get(JobId(id2))
        assert row1 is not None
        assert row2 is not None
        assert row1.tags == ("run-a",)
        assert row2.tags == ("run-b",)
