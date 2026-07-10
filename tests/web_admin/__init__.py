"""Shared test utilities for the taskq.web.admin test suite.

Stub classes for duck-typing asyncpg primitives in unit tests.
Pytest fixtures that use these classes live in the adjacent ``conftest.py``.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from taskq.backend._protocol import (
    EnqueueArgs,
    JobRow,
)

__all__ = [
    "StubAcquireContext",
    "StubBackend",
    "StubConnection",
    "StubPool",
    "StubRecord",
    "_stub_job_row",
]


class StubRecord(dict[str, object]):
    """Minimal asyncpg.Record duck type for testing."""


class StubConnection:
    """Minimal asyncpg.Connection duck type that returns empty results."""

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return None

    async def fetchval(self, query: str, *args: object) -> object:
        return False

    async def execute(self, query: str, *args: object) -> str:
        return ""


class StubAcquireContext:
    """Async context manager yielding a StubConnection."""

    async def __aenter__(self) -> StubConnection:
        return StubConnection()

    async def __aexit__(self, *args: object) -> None:
        pass


class StubPool:
    """Minimal asyncpg.Pool duck type for testing."""

    def acquire(self) -> StubAcquireContext:
        return StubAcquireContext()


_StubPool = StubPool  # backward-compatible alias used by test modules

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")


def _stub_job_row(
    job_id: UUID,
    *,
    status: str = "pending",
) -> JobRow:
    """Build a minimal JobRow for testing."""
    return JobRow(
        id=job_id,  # pyright: ignore[reportArgumentType]
        actor="test_actor",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=1,
        status=status,  # pyright: ignore[reportArgumentType]
        priority=0,
        attempt=0,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime.now(UTC),
        scheduled_at=datetime.now(UTC),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=0,  # pyright: ignore[reportArgumentType]
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )


class StubBackend:
    """Minimal Backend stub that records method calls for assertion."""

    def __init__(self, *, job_row: JobRow | None = None) -> None:
        self._job_row = job_row
        self.cancel_calls: list[tuple[UUID, str | None]] = []
        self.retry_calls: list[UUID] = []
        self.enqueue_calls: list[EnqueueArgs] = []

    async def get(self, job_id: Any) -> JobRow | None:
        return self._job_row

    async def write_cancel_request(self, job_id: Any, reason: str | None) -> bool:
        self.cancel_calls.append((job_id, reason))
        return True

    async def retry_job(self, job_id: Any) -> bool:
        self.retry_calls.append(job_id)
        return True

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        self.enqueue_calls.append(args)
        assert self._job_row is not None
        return self._job_row
