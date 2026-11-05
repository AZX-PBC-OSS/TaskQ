"""Shared test utilities for the taskq.web.admin test suite.

Stub classes for duck-typing asyncpg and Redis primitives in unit tests.
Pytest fixtures that use these classes live in ``_fixtures.py``, which
``tests/conftest.py`` imports and registers so fixture resolution never
depends on conftest adjacency (see that module's docstring).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

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


class StubTransaction:
    """Minimal asyncpg transaction duck type: enters and exits, rolls nothing back."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class StubConnection:
    """FAIL-CLOSED admin connection double (tests/test_suite_hygiene.py's
    rule): the blind defaults the old stub answered — ``fetch`` → ``[]``,
    ``fetchval`` → ``False``, ``execute`` → ``""`` — died there. A
    statement reaching this double now needs a NAMED script: *scripts*
    maps a distinctive SQL fragment to the answer (a list for ``fetch``,
    a record or ``None`` for ``fetchrow``, a scalar for ``fetchval``, a
    tag string for ``execute``); an unscripted statement raises
    AssertionError naming the fragment. The server-clock probe stays a
    named answer (Postgres answers it unconditionally).

    The render-only surface (no execution) is a stub with no scripts: a
    render that needs a row must script it, a render that queries
    unscripted statements is not render-only - it is a route test whose
    happy path belongs to the real-PG packs
    (tests/test_web_admin_integration.py, tests/test_web_admin_actors.py,
    tests/test_insights.py, tests/web_admin/test_batch_detail_pg.py).
    """

    def __init__(self, *, scripts: dict[str, object] | None = None) -> None:
        # The render tier's page scripts are the DEFAULT: a directly
        # constructed stub renders the pages (empty) through the same
        # named scripts the fixture uses - never blind defaults.
        self._scripts: dict[str, object] = dict(RENDER_ONLY_SCRIPTS) if scripts is None else scripts
        self.executed_calls: list[tuple[str, tuple[object, ...]]] = []

    def _answer(self, query: str) -> object | None:
        for fragment, answer in self._scripts.items():
            if fragment in query:
                return answer
        raise AssertionError(
            f"StubConnection: no script answers statement {query!r} - the "
            "fail-closed rule (tests/test_suite_hygiene.py) forbids a blind "
            "default. Script the statement (scripts={fragment: answer}), or "
            "the happy path belongs to the real-PG admin packs."
        )

    def transaction(self) -> StubTransaction:
        # The admin mutation routes wrap their writes in
        # conn.transaction() (the audit row commits with the mutation);
        # the real connection's block is where atomicity lives, the stub
        # only has to be enterable.
        return StubTransaction()

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        answer = self._answer(query)
        if answer is None:
            return []
        if not isinstance(answer, list):
            raise AssertionError(
                f"StubConnection: the script for {query!r} is a scalar - "
                "scalar scripts answer fetchval only; script a list for fetch"
            )
        return [row if isinstance(row, StubRecord) else StubRecord(dict(row)) for row in answer]  # type: ignore[arg-type]

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        answer = self._answer(query)
        if answer is None:
            return None
        if isinstance(answer, list):
            return StubRecord(dict(answer[0])) if answer else None
        if isinstance(answer, StubRecord):
            return answer
        raise AssertionError(
            f"StubConnection: the script for {query!r} is neither a record "
            "nor a list - script a record (or None) for fetchrow"
        )

    async def fetchval(self, query: str, *args: object) -> object:
        if query.strip() == "SELECT clock_timestamp()":
            # Postgres answers a server-clock read with a timestamp
            # unconditionally. Returning the generic False here handed the
            # admin factory's clock-offset probe a bool where the real
            # database hands it a datetime.
            return datetime.now(UTC)
        answer = self._answer(query)
        if isinstance(answer, list | StubRecord):
            raise AssertionError(
                f"StubConnection: the script for {query!r} is a row shape - "
                "row scripts answer fetch/fetchrow; script a scalar for fetchval"
            )
        return answer

    async def execute(self, query: str, *args: object) -> str:
        self.executed_calls.append((query, args))
        answer = self._answer(query)
        return answer if isinstance(answer, str) else "UPDATE 0"


class StubAcquireContext:
    """Async context manager yielding a StubConnection."""

    async def __aenter__(self) -> StubConnection:
        return StubConnection(scripts=self._scripts)

    async def __aexit__(self, *args: object) -> None:
        pass

    def __init__(self, scripts: dict[str, object] | None = None) -> None:
        self._scripts = scripts if scripts is not None else {}


# ── The render tier's named page scripts ────────────────────────────────
#
# The render-only tests (nav structure, table headers, pagination bars,
# styling) drive pages that legitimately query: the RENDER fixture scripts
# each page's own list/count statement ONCE, centrally, with the empty-page
# answer — a named script per page, never a blind default (the fail-closed
# rule). A render test needing rows scripts the page's fragment with them;
# the page's happy-path DATA belongs to the real-PG packs
# (tests/test_web_admin_integration.py, tests/test_web_admin_actors.py,
# tests/test_insights.py, tests/web_admin/test_batch_detail_pg.py).

RENDER_ONLY_SCRIPTS: dict[str, object] = {
    # queues page: the per-queue census + the overview aggregates
    "SELECT queue, count(*) FILTER (WHERE status": [],
    "u.actor,": [],  # the per-actor/queue census join (the u.* aggregate)
    "count(*) FILTER (WHERE job_id IS NOT NULL)": [],  # the reservations summary
    # the pages' queue-filter dropdown: the distinct live-queue probe
    "SELECT DISTINCT j.queue FROM": [],
    # the queues page's stranded-queue census (the unserved-queue warning)
    "SELECT r.routing_queue AS queue, count(*) AS stranded_count": [],
    # the workers-per-queue census (the queues page's worker counts)
    "SELECT q AS queue, count(*) AS worker_count": [],
    # jobs page: the page-list union
    "SELECT id, actor, queue, status, finished_at, created_at, started_at": [],
    # history page: the page-list union
    "SELECT id, actor, queue, status, created_at, scheduled_at": [],
    # history page: the per-status summary (a row per status)
    "SELECT status, count(*) AS cnt": [],
    # batch detail: the member listing
    "SELECT id, queue, actor, status, scheduled_at, attempt, max_attempts": [],
    # reservations page: the slot listing
    "SELECT bucket_name, slot_index, job_id, held_by_worker_id": [],
    # job detail: no row scripted - the render tests script their own
    # workers page: the fleet listing
    "SELECT w.*, (ml.worker_id IS NOT NULL) AS is_leader": [],
    # leader page: the lease row join
    "SELECT ml.*, w.hostname, w.pid": [],
    # schedules page: the schedule listing
    "SELECT id, actor, cron_expr, timezone, enabled": [],
    # rate-limits page: the bucket listing
    'FROM "taskq".rate_limit_buckets ORDER BY bucket_name': [],
    # batches page: the batch listing
    "SELECT id, queue, status, expected_size": [],
    # actors page: the actor census
    "SELECT ac.actor, ac.max_concurrent": [],
    # the workers EXISTS probe (the polling badge's live-worker check)
    "SELECT EXISTS (": False,
    # the exact status-count scalar probe (the templates' badge counters)
    'SELECT COUNT(*) FROM "taskq".jobs WHERE status = ANY($1)': 0,
    'SELECT COUNT(*) FROM "taskq".jobs_archive WHERE status = ANY($1)': 0,
    # job detail's lookup: the render tier's default row set is EMPTY -
    # the detail page's 404 shape; a render test scripting its own row
    # overrides this fragment with the record.
    'SELECT * FROM "taskq".jobs WHERE id = $1': None,
    'SELECT * FROM "taskq".jobs_archive WHERE id = $1': None,
}


class StubPool:
    """Minimal asyncpg.Pool duck type for testing.

    *scripts* is the fail-closed stub's named-answer map (fragment →
    answer), handed to every connection this pool yields. The ``stub_pool``
    fixture builds the pool with the render tier's own page scripts
    (``RENDER_ONLY_SCRIPTS``); a test needing its own rows mutates the
    pool's ``scripts`` dict directly."""

    def __init__(self, *, scripts: dict[str, object] | None = None) -> None:
        # The render tier's page scripts are the DEFAULT: a directly
        # constructed pool renders the pages (empty) through the same
        # named scripts the fixture uses - never blind defaults.
        self._scripts: dict[str, object] = dict(RENDER_ONLY_SCRIPTS) if scripts is None else scripts

    @property
    def scripts(self) -> dict[str, object]:
        """The pool's named-answer map (mutate to script a statement)."""
        return self._scripts

    def acquire(self, *, timeout: float | None = None) -> StubAcquireContext:
        return StubAcquireContext(scripts=self._scripts)


_StubPool = StubPool  # backward-compatible alias used by test modules


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

    def __init__(
        self,
        *,
        job_row: JobRow | None = None,
        retry_result: bool = True,
        cancel_result: bool = True,
    ) -> None:
        self._job_row = job_row
        self._retry_result = retry_result
        self._cancel_result = cancel_result
        self.cancel_calls: list[tuple[UUID, str | None]] = []
        self.retry_calls: list[UUID] = []
        self.enqueue_calls: list[EnqueueArgs] = []

    async def get(self, job_id: Any) -> JobRow | None:
        return self._job_row

    async def write_cancel_request(self, job_id: Any, reason: str | None) -> bool:
        self.cancel_calls.append((job_id, reason))
        return self._cancel_result

    async def retry_job(self, job_id: Any) -> bool:
        self.retry_calls.append(job_id)
        return self._retry_result

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
    therefore surfaces at the round trip - exactly where a live Redis
    failure would - which is the failure seam the fetch's degrade-to-None
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
