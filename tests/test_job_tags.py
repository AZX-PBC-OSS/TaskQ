"""Tests for job tags support.

Covers:
- Tag validation (regex, length, dedup)
- Enqueue with tags (single and batch)
- list_jobs filtering by tags
- Admin API tag filtering
- Both InMemoryBackend and PostgresBackend via backend_pair
"""

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.backend._protocol import (
    JobFilter,
)
from taskq.batch import EnqueueItem
from taskq.client._args import _TAG_RE, _validate_and_dedup_tags, build_enqueue_args
from taskq.client._jobs import JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

if TYPE_CHECKING:
    from taskq.backend._protocol import Backend

_START = datetime(2025, 1, 1, tzinfo=UTC)


# ── Test actors ──────────────────────────────────────────────────────────


class _TagPayload(BaseModel):
    value: str = "test"


@actor(name="tag_test_actor")
async def _tag_actor(_payload: _TagPayload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_client(backend: InMemoryBackend) -> JobsClient:
    return JobsClient(backend=backend, clock=FakeClock(start=_START))


# ── Tag validation tests ─────────────────────────────────────────────────


class TestTagValidation:
    """Tag validation — regex, length, dedup."""

    def test_valid_tags_pass_regex(self) -> None:
        """Valid tags match the regex pattern."""
        valid_tags = [
            "abc",
            "high-priority",
            "tenant_acme",
            "cost-center_marketing",
            "a1b2c3",
            "my-tag-123",
            "research",
        ]
        for tag in valid_tags:
            assert _TAG_RE.match(tag), f"tag {tag!r} should be valid"

    def test_invalid_tags_fail_regex(self) -> None:
        """Invalid tags are rejected by the regex."""
        invalid_tags = [
            "ab",  # too short (< 3 chars)
            "-starts-hyphen",
            "ends-hyphen-",
            "has spaces",
            "has,comma",
            "has/slash",
            "has@at",
            "with.dot",
        ]
        for tag in invalid_tags:
            assert not _TAG_RE.match(tag), f"tag {tag!r} should be invalid"

    def test_tag_exceeds_max_length_raises(self) -> None:
        """Tags over 255 chars raise ValueError."""
        long_tag = "a" * 256
        with pytest.raises(ValueError, match="exceeds 255 characters"):
            _validate_and_dedup_tags([long_tag])

    def test_empty_tag_raises(self) -> None:
        """Empty string tags raise ValueError."""
        with pytest.raises(ValueError, match="tag must not be empty"):
            _validate_and_dedup_tags([""])

    def test_invalid_tag_raises(self) -> None:
        """Tags failing regex raise ValueError."""
        with pytest.raises(ValueError, match="invalid tag"):
            _validate_and_dedup_tags(["ab"])

    def test_duplicate_tags_deduplicated(self) -> None:
        """Duplicate tags are silently deduplicated, preserving first-occurrence order."""
        result = _validate_and_dedup_tags(["alpha", "beta", "alpha", "gamma", "beta"])
        assert result == ("alpha", "beta", "gamma")

    def test_none_returns_empty_tuple(self) -> None:
        """None input returns empty tuple."""
        assert _validate_and_dedup_tags(None) == ()

    def test_empty_list_returns_empty_tuple(self) -> None:
        """Empty list returns empty tuple."""
        assert _validate_and_dedup_tags([]) == ()

    def test_tag_starts_with_underscore_valid(self) -> None:
        """Tags starting with underscore are valid (underscore IS a word char)."""
        result = _validate_and_dedup_tags(["_internal"])
        assert result == ("_internal",)


# ── Enqueue with tags tests ──────────────────────────────────────────────


class TestEnqueueWithTags:
    """Enqueue with tags via JobsClient."""

    async def test_enqueue_single_job_with_tags(self) -> None:
        """Enqueuing a job with tags stores them on the row."""
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue(
            _tag_actor,
            _TagPayload(value="test"),
            tags=["research", "high-priority"],
        )

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("research", "high-priority")

    async def test_enqueue_without_tags_defaults_empty(self) -> None:
        """Enqueuing without tags produces empty tags tuple."""
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue(_tag_actor, _TagPayload(value="test"))
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ()

    async def test_enqueue_with_none_tags(self) -> None:
        """Explicit None tags produces empty tuple."""
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue(_tag_actor, _TagPayload(value="test"), tags=None)
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ()

    async def test_enqueue_validates_tags(self) -> None:
        """Invalid tags raise ValueError during enqueue."""
        backend = _make_backend()
        client = _make_client(backend)

        with pytest.raises(ValueError, match="invalid tag"):
            await client.enqueue(
                _tag_actor,
                _TagPayload(value="test"),
                tags=["ab"],  # too short
            )

    async def test_enqueue_deduplicates_tags(self) -> None:
        """Duplicate tags are deduplicated."""
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue(
            _tag_actor,
            _TagPayload(value="test"),
            tags=["alpha", "alpha", "beta", "alpha"],
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("alpha", "beta")

    async def test_enqueue_preserves_tag_order(self) -> None:
        """Tag order is preserved (first occurrence)."""
        backend = _make_backend()
        client = _make_client(backend)

        handle = await client.enqueue(
            _tag_actor,
            _TagPayload(value="test"),
            tags=["zulu", "alpha", "mike"],
        )
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.tags == ("zulu", "alpha", "mike")


# ── Batch enqueue with tags tests ────────────────────────────────────────


class TestBatchEnqueueWithTags:
    """Batch enqueue with tags."""

    async def test_batch_enqueue_with_tags(self) -> None:
        """Each item in a batch can carry its own tags."""
        backend = _make_backend()
        client = _make_client(backend)

        items = [
            EnqueueItem(
                actor_ref=_tag_actor,
                payload=_TagPayload(value="a"),
                tags=["batch-abc", "chunk-1"],
            ),
            EnqueueItem(
                actor_ref=_tag_actor,
                payload=_TagPayload(value="b"),
                tags=["batch-abc", "chunk-2"],
            ),
            EnqueueItem(
                actor_ref=_tag_actor,
                payload=_TagPayload(value="c"),
            ),
        ]
        batch = await client.enqueue_batch(items)

        rows = []
        for h in batch.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            rows.append(row)

        assert rows[0].tags == ("batch-abc", "chunk-1")
        assert rows[1].tags == ("batch-abc", "chunk-2")
        assert rows[2].tags == ()

    async def test_batch_enqueue_items_without_tags(self) -> None:
        """Items without tags default to empty tuple."""
        backend = _make_backend()
        client = _make_client(backend)

        items = [
            EnqueueItem(actor_ref=_tag_actor, payload=_TagPayload(value="a")),
            EnqueueItem(actor_ref=_tag_actor, payload=_TagPayload(value="b")),
        ]
        batch = await client.enqueue_batch(items)

        for h in batch.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.tags == ()


# ── List/filter jobs by tags tests ───────────────────────────────────────


class TestListJobsByTags:
    """list_jobs filtering by tags."""

    async def test_list_jobs_filter_by_single_tag(self) -> None:
        """Filter jobs by a single tag returns matching jobs."""
        backend = _make_backend()
        client = _make_client(backend)

        # Enqueue jobs with different tags
        await client.enqueue(_tag_actor, _TagPayload(value="a"), tags=["research", "high-priority"])
        await client.enqueue(_tag_actor, _TagPayload(value="b"), tags=["research"])
        await client.enqueue(_tag_actor, _TagPayload(value="c"), tags=["low-priority"])

        result = await client.list(JobFilter(tags=("research",)))
        assert len(result.jobs) == 2

    async def test_list_jobs_filter_by_multiple_tags(self) -> None:
        """Filter by multiple tags uses OR semantics (array overlap)."""
        backend = _make_backend()
        client = _make_client(backend)

        await client.enqueue(_tag_actor, _TagPayload(value="a"), tags=["research"])
        await client.enqueue(_tag_actor, _TagPayload(value="b"), tags=["high-priority"])
        await client.enqueue(_tag_actor, _TagPayload(value="c"), tags=["low-priority"])

        result = await client.list(JobFilter(tags=("research", "high-priority")))
        assert len(result.jobs) == 2

    async def test_list_jobs_filter_no_match(self) -> None:
        """Filter with non-matching tags returns empty list."""
        backend = _make_backend()
        client = _make_client(backend)

        await client.enqueue(_tag_actor, _TagPayload(value="a"), tags=["research"])

        result = await client.list(JobFilter(tags=("nonexistent",)))
        assert len(result.jobs) == 0

    async def test_list_jobs_filter_none_tags_includes_all(self) -> None:
        """When tags filter is None, all jobs are returned (no tag filter)."""
        backend = _make_backend()
        client = _make_client(backend)

        await client.enqueue(_tag_actor, _TagPayload(value="a"), tags=["research"])
        await client.enqueue(_tag_actor, _TagPayload(value="b"))

        result = await client.list(JobFilter())
        assert len(result.jobs) == 2

    async def test_list_jobs_combine_tags_with_other_filters(self) -> None:
        """Tags filter can be combined with actor and queue filters."""
        backend = _make_backend()
        client = _make_client(backend)

        await client.enqueue(_tag_actor, _TagPayload(value="a"), tags=["alpha"])
        await client.enqueue(_tag_actor, _TagPayload(value="b"), tags=["beta"])

        # Combine actor filter with tags
        result = await client.list(JobFilter(actor="tag_test_actor", tags=("beta",)))
        assert len(result.jobs) == 1
        assert result.jobs[0].tags == ("beta",)

        # Tags alone finds both
        result = await client.list(JobFilter(tags=("alpha", "beta")))
        assert len(result.jobs) == 2


# ── EnqueueArgs and JobRow tags ──────────────────────────────────────────


class TestDataCarriers:
    """EnqueueArgs and JobRow carry tags correctly."""

    def test_enqueue_args_default_tags_empty(self) -> None:
        """EnqueueArgs defaults to empty tags tuple."""
        args = make_enqueue_args()
        assert args.tags == ()

    def test_enqueue_args_with_tags(self) -> None:
        """EnqueueArgs stores provided tags."""
        args = make_enqueue_args(tags=("alpha", "beta"))
        assert args.tags == ("alpha", "beta")

    def test_job_row_default_tags_empty(self) -> None:
        """JobRow without tags defaults to empty tuple."""
        from taskq.testing.jobs import make_job_row

        row = make_job_row()
        assert row.tags == ()


# ── build_enqueue_args tags propagation ──────────────────────────────────


class TestBuildEnqueueArgs:
    """build_enqueue_args propagates tags to EnqueueArgs."""

    def test_build_enqueue_args_with_tags(self) -> None:
        """build_enqueue_args includes tags in the resulting EnqueueArgs."""
        clock = FakeClock(start=_START)
        args = build_enqueue_args(
            _tag_actor,
            _TagPayload(value="test"),
            tags=["alpha", "beta"],
            clock=clock,
        )
        assert args.tags == ("alpha", "beta")

    def test_build_enqueue_args_without_tags(self) -> None:
        """build_enqueue_args defaults to empty tags."""
        clock = FakeClock(start=_START)
        args = build_enqueue_args(
            _tag_actor,
            _TagPayload(value="test"),
            clock=clock,
        )
        assert args.tags == ()


# ── backend_pair tests (integration) ─────────────────────────────────────


@pytest.mark.integration
class TestBackendPairTags:
    """through Tags work across both backends."""

    async def test_enqueue_with_tags_both_backends(self, backend_pair: "Backend") -> None:
        """Enqueue with tags works on both memory and PG backends."""

        args = make_enqueue_args(
            tags=("tenant-acme", "priority-high"),
            scheduled_at=_START,
        )
        row = await backend_pair.enqueue(args)
        assert row.tags == ("tenant-acme", "priority-high")

    async def test_list_jobs_filter_by_tags_both_backends(self, backend_pair: "Backend") -> None:
        """List jobs by tags filter works on both backends."""

        # Enqueue jobs with and without tags
        args1 = make_enqueue_args(
            actor="tag_filter_a",
            tags=("group-a", "shared"),
            scheduled_at=_START,
        )
        args2 = make_enqueue_args(
            actor="tag_filter_b",
            tags=("group-b", "shared"),
            scheduled_at=_START,
        )
        args3 = make_enqueue_args(
            actor="tag_filter_c",
            scheduled_at=_START,
        )

        await backend_pair.enqueue(args1)
        await backend_pair.enqueue(args2)
        await backend_pair.enqueue(args3)

        # Filter by shared tag
        rows = await backend_pair.list_jobs(JobFilter(tags=("shared",)))
        assert len(rows) == 2
        actor_names = {r.actor for r in rows}
        assert actor_names == {"tag_filter_a", "tag_filter_b"}

        # Filter by group-a tag
        rows = await backend_pair.list_jobs(JobFilter(tags=("group-a",)))
        assert len(rows) == 1
        assert rows[0].actor == "tag_filter_a"

        # Filter by nonexistent tag
        rows = await backend_pair.list_jobs(JobFilter(tags=("nonexistent",)))
        assert len(rows) == 0

    async def test_enqueue_batch_tags_both_backends(self, backend_pair: "Backend") -> None:
        """Batch enqueue with tags works on both backends."""

        args1 = make_enqueue_args(tags=("batch-tag", "item-1"), scheduled_at=_START)
        args2 = make_enqueue_args(tags=("batch-tag", "item-2"), scheduled_at=_START)

        rows = await backend_pair.enqueue_batch([args1, args2])
        assert len(rows) == 2
        assert rows[0].tags == ("batch-tag", "item-1")
        assert rows[1].tags == ("batch-tag", "item-2")
