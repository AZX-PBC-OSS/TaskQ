"""Shared test utilities for the taskq.web.admin test suite.

Stub classes for duck-typing asyncpg and Redis primitives in unit tests.
Pytest fixtures that use these classes live in the adjacent ``conftest.py``.
"""

from __future__ import annotations

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
    "StubPipelinedRedis",
    "StubPool",
    "StubRecord",
    "StubRedisPipeline",
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
        if "clock_timestamp()" in query:
            # Postgres answers a server-clock read with a timestamp
            # unconditionally. Returning the generic False here handed the
            # admin factory's clock-offset probe a bool where the real
            # database hands it a datetime.
            return datetime.now(UTC)
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
        idempotency_scope="",
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


# ── Redis pipeline transport double ───────────────────────────────────────
#
# The admin rate-limits fetch (taskq.web.admin.ops._fetch_redis_rl_state)
# reads every bucket through ONE redis-py pipeline: N commands queued, a
# single execute() round trip. These doubles speak that transport, mirroring
# the recording pattern in tests/test_sweepaudit_admin_redis_batch.py.


class StubRedisPipeline:
    """redis-py pipeline double: queues reads, resolves them at execute().

    Commands are recorded (not executed) as they are queued; ONE
    ``execute()`` call resolves them in order by replaying each read
    against the owning client's direct readers. A reader that raises
    therefore surfaces at the round trip — exactly where a live Redis
    failure would — which is the failure seam the fetch's degrade-to-None
    guard protects.
    """

    def __init__(self, client: StubPipelinedRedis) -> None:
        self._client = client
        self.commands: list[tuple[str, str]] = []

    def hgetall(self, key: str) -> StubRedisPipeline:
        self.commands.append(("hgetall", key))
        return self

    def get(self, key: str) -> StubRedisPipeline:
        self.commands.append(("get", key))
        return self

    def zcard(self, key: str) -> StubRedisPipeline:
        self.commands.append(("zcard", key))
        return self

    async def execute(self) -> list[object]:
        results: list[object] = []
        for command, key in self.commands:
            if command == "hgetall":
                results.append(await self._client.hgetall(key))
            elif command == "get":
                results.append(await self._client.get(key))
            else:
                results.append(await self._client.zcard(key))
        return results


class StubPipelinedRedis:
    """Redis client double for the admin fetch's pipeline transport.

    ``pipeline()`` hands back a :class:`StubRedisPipeline` whose
    ``execute()`` resolves queued reads through these direct readers, so a
    key read via the pipeline is exactly a key the direct transport would
    have read. Subclasses script per-key data by overriding the readers;
    the defaults are a Redis holding no state: empty hash, missing key,
    empty zset.
    """

    def pipeline(self) -> StubRedisPipeline:
        return StubRedisPipeline(self)

    async def hgetall(self, key: str) -> dict[str, str] | list[tuple[bytes, bytes]]:
        return {}

    async def get(self, key: str) -> bytes | str | None:
        return None

    async def zcard(self, key: str) -> int:
        return 0
