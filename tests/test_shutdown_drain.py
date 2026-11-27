"""Unit tests for drain_local_queue_to_pending and ShutdownPhase enum."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import asyncpg
import pytest
import structlog
import structlog.testing

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobId, JobRow
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload
from taskq.testing.jobs import make_job_row
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import producer_loop
from taskq.worker.shutdown import ShutdownPhase, drain_local_queue_to_pending


def _make_ctx(job_id: JobId, claim_epoch: int) -> JobContext[EmptyPayload]:
    """A real context carrying the claim identity a registry entry holds."""
    return JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        claim_epoch=claim_epoch,
        worker_id=new_uuid(),
        payload=EmptyPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None),
        log=structlog.get_logger("taskq.test"),
    )


# ── Minimal fakes ──────────────────────────────────────────────────────────


class FakeConn:
    """Lightweight asyncpg.Connection stand-in for drain tests."""

    def __init__(
        self, *, fail_execute_with: BaseException | None = None, tag: str = "UPDATE 3"
    ) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._fail_execute_with = fail_execute_with
        self._tag = tag

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._fail_execute_with is not None:
            raise self._fail_execute_with
        return self._tag


class FakePool:
    """Lightweight asyncpg.Pool stand-in for drain tests."""

    def __init__(
        self,
        *,
        fail_acquire_with: BaseException | None = None,
        conn_fail_execute_with: BaseException | None = None,
        tag: str = "UPDATE 3",
    ) -> None:
        self._fail_acquire_with = fail_acquire_with
        self._conn_fail_execute_with = conn_fail_execute_with
        self._tag = tag
        self.acquire_count = 0
        self.last_acquire_timeout: float | None = None
        self._last_conn: FakeConn | None = None

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109 # Why: asyncpg.Pool.acquire signature takes timeout; FakePool mirrors it.
        self.acquire_count += 1
        self.last_acquire_timeout = timeout
        if self._fail_acquire_with is not None:
            raise self._fail_acquire_with
        conn = FakeConn(fail_execute_with=self._conn_fail_execute_with, tag=self._tag)
        self._last_conn = conn
        yield conn

    @property
    def execute_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        if self._last_conn is None:
            return []
        return self._last_conn.execute_calls


# ── Helper to build deps ───────────────────────────────────────────────────


def _worker_settings(schema_name: str = "taskq") -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": schema_name},
    )


def _worker_settings_no_validate(schema_name: str = "taskq") -> WorkerSettings:
    """Settings carrying an arbitrary schema name, bypassing field validation.

    `schema_name` is validated by a `validator=` hook, which dotenvmodel runs on
    every load path INCLUDING `validate=False` -- deliberately, because it is
    the one setting that reaches raw SQL as an interpolated identifier. So an
    invalid name can no longer be loaded, and the object is mutated after
    construction instead.

    That is the point of these tests: they assert the shutdown path performs its
    own `_IDENT_RE` check rather than trusting settings. That defence in depth
    is what keeps the settings-level guard from being a single point of failure,
    so it still needs proving.
    """
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": "taskq"},
        validate=False,
    )
    settings.schema_name = schema_name
    return settings


# ── Enum-value sanity test ────────────────────────────


def test_shutdown_phase_values() -> None:
    """Assert ShutdownPhase enum values match the ordering."""
    assert ShutdownPhase.NONE == 0
    assert ShutdownPhase.DRAINING == 1
    assert ShutdownPhase.CANCELLING == 2
    assert ShutdownPhase.FORCING == 3
    assert ShutdownPhase.RELEASING == 4


# ── SQL shape ──────────────────────────────────────────────────────


async def test_drain_sql_shape() -> None:
    """drain_local_queue_to_pending drains running jobs via the pool and
    returns the correct count. Verifies the configured schema name is used."""
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type] # Why: heartbeat_pool not used by drain but WorkerDeps requires it.
        worker_pool=pool,  # type: ignore[arg-type] # Why: worker_pool not used by drain but WorkerDeps requires it.
        notify_conn=None,
        leader_conn=None,
    )

    rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 3
    assert pool.acquire_count == 1
    assert pool.last_acquire_timeout == 2.0
    assert len(pool.execute_calls) == 1

    sql, args = pool.execute_calls[0]
    assert args == (worker_id,)
    assert "UPDATE" in sql
    assert "jobs" in sql
    assert settings.schema_name in sql


async def test_drain_pool_exhaustion_returns_zero() -> None:
    """asyncio.TimeoutError on acquire → returns 0, logs warning, no raise."""
    worker_id = new_uuid()
    pool = FakePool(fail_acquire_with=TimeoutError("timed out"))
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 0


# ── PG connection error ────────────────────────────────────────


async def test_drain_pg_connection_error_returns_zero() -> None:
    """asyncpg.PostgresConnectionError on execute → returns 0, logs warning, no raise."""
    worker_id = new_uuid()
    pool = FakePool(conn_fail_execute_with=asyncpg.PostgresConnectionError("gone"))
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 0


# ── schema validation ──────────────────────────────────────────


async def test_drain_invalid_schema_raises() -> None:
    """Invalid schema raises ValueError before any pool acquisition."""
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings_no_validate("foo;DROP TABLE")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    with pytest.raises(ValueError) as exc_info:
        await drain_local_queue_to_pending(deps, worker_id)

    assert "invalid schema identifier" in str(exc_info.value)
    assert "foo;DROP TABLE" in str(exc_info.value)
    assert pool.acquire_count == 0


async def test_drain_local_queue_uses_transient_pg_errors_not_handrolled() -> None:
    """asyncpg.InterfaceError (e.g. a closed pool) on execute must be handled
    as transient - logged and rowcount 0 - not propagated as fatal.

    InterfaceError is in TRANSIENT_PG_ERRORS but was missing from an earlier
    hand-rolled (TimeoutError, PostgresConnectionError) tuple; this drives an
    actual InterfaceError through the drain path to pin the fix. The failure
    record also names its class, call site and stage, so an operator can
    classify it without reconstructing which pass ran."""
    worker_id = new_uuid()
    pool = FakePool(conn_fail_execute_with=asyncpg.InterfaceError("pool is closed"))
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    with structlog.testing.capture_logs() as captured:
        rowcount = await drain_local_queue_to_pending(deps, worker_id)

    assert rowcount == 0, "InterfaceError must be treated as transient, not fatal"
    failures = [entry for entry in captured if entry.get("event") == "drain-local-queue-failed"]
    assert len(failures) == 1, f"expected one drain-local-queue-failed record, got: {captured}"
    record = failures[0]
    assert record["log_level"] == "warning", (
        "an ordinary transient (a closed pool) stays a warning; only a "
        "statement-shape refusal escalates. "
        f"Got: {record!r}"
    )
    assert record["error_class"] == "InterfaceError", (
        f"the failure record must name the error class. Got: {record!r}"
    )
    assert record["call_site"] == "shutdown", (
        f"the default call site must be named on the record. Got: {record!r}"
    )
    assert record["stage"] == "execute", (
        f"the record must say the statement itself failed. Got: {record!r}"
    )
    assert "statement_contract_error" not in record, (
        f"a closed pool is not a statement-shape refusal. Got: {record!r}"
    )


async def test_a_statement_shape_interface_error_is_escalated_with_its_contract_marker() -> None:
    """A parameter-count mismatch is a permanently refused statement, not weather.

    ``asyncpg.InterfaceError`` covers both a dead connection and asyncpg's
    own "the server expects N arguments for this query" raise. The second is
    the shape of the statement - permanent by construction - and the drain's
    bind surface is the one widened to paired arrays. The record must
    therefore carry ``statement_contract_error=True`` at ERROR (with the
    class, call site and stage), while the helper still returns 0 so
    shutdown never deadlocks on it; the lease-expiry sweep is the backstop.
    """
    worker_id = new_uuid()
    pool = FakePool(
        conn_fail_execute_with=asyncpg.InterfaceError(
            "the server expects 3 arguments for this query, 2 were passed"
        )
    )
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    with structlog.testing.capture_logs() as captured:
        rowcount = await drain_local_queue_to_pending(deps, worker_id, call_site="producer_exit")

    assert rowcount == 0, (
        "the helper must still degrade to 0: a permanent statement refusal "
        "may not deadlock shutdown"
    )
    failures = [entry for entry in captured if entry.get("event") == "drain-local-queue-failed"]
    assert len(failures) == 1, f"expected one hand-back failure record, got: {captured}"
    record = failures[0]
    assert record["log_level"] == "error", (
        f"a permanently refused statement must be loud. Got: {record!r}"
    )
    assert record["statement_contract_error"] is True, (
        f"the record must mark the statement-shape refusal. Got: {record!r}"
    )
    assert record["error_class"] == "InterfaceError"
    assert record["call_site"] == "producer_exit"
    assert record["stage"] == "execute"


async def test_a_pool_acquire_failure_names_its_stage_and_the_acquire_bound() -> None:
    """A drain that never reached its statement says so, with the 2s bound."""
    worker_id = new_uuid()
    pool = FakePool(fail_acquire_with=TimeoutError("pool acquire timed out"))
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    with structlog.testing.capture_logs() as captured:
        rowcount = await drain_local_queue_to_pending(deps, worker_id, call_site="draining_phase")

    assert rowcount == 0
    failures = [entry for entry in captured if entry.get("event") == "drain-local-queue-failed"]
    assert len(failures) == 1, f"expected one hand-back failure record, got: {captured}"
    record = failures[0]
    assert record["stage"] == "pool_acquire", (
        f"the record must say the pool acquire failed, not the statement. Got: {record!r}"
    )
    assert record["acquire_timeout_s"] == 2.0, (
        f"the acquire's two-second bound belongs on the record. Got: {record!r}"
    )
    assert record["error_class"] == "TimeoutError"
    assert record["call_site"] == "draining_phase"
    assert record["log_level"] == "warning"


async def test_drain_active_jobs_excluded_counts_distinct_rows() -> None:
    """The completed record counts spared rows and claim pairs separately.

    ``active_jobs_excluded`` is the number of jobs the hand-back deliberately
    spared, so it must count distinct rows even when the exclusion binds two
    claim pairs for one row (a stale attempt still registered while a newer
    take holds its intent). ``active_claims_excluded`` carries the pair count
    so the difference is legible rather than hidden.
    """
    worker_id = new_uuid()
    pool = FakePool(tag="UPDATE 1")
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    stale_id = JobId(new_uuid())
    # A stale attempt still registered at epoch 1, while the row has already
    # been re-claimed and a newer take recorded its intent at epoch 2.
    await deps.active_jobs.register(
        stale_id,
        cast("asyncio.Task[object]", MagicMock()),
        _make_ctx(stale_id, 1),
    )
    deps.active_jobs.mark_claimed(stale_id, 2)
    assert len(deps.active_jobs.held_claims()) == 2, "premise: two claim pairs"
    assert len({job_id for job_id, _epoch in deps.active_jobs.held_claims()}) == 1, (
        "premise: one distinct row held"
    )

    with structlog.testing.capture_logs() as captured:
        await drain_local_queue_to_pending(deps, worker_id)

    completed = [entry for entry in captured if entry.get("event") == "drain-local-queue-completed"]
    assert len(completed) == 1, f"expected one completed record, got: {captured}"
    record = completed[0]
    assert record["active_jobs_excluded"] == 1, (
        "active_jobs_excluded must count the rows the hand-back spared, not the "
        f"claim pairs the exclusion binds. Got: {record!r}"
    )
    assert record["active_claims_excluded"] == 2, (
        f"the claim-pair count must stay legible beside the row count. Got: {record!r}"
    )


# ── Hand-back excludes jobs a consumer is already executing ────────


async def test_drain_excludes_jobs_a_consumer_is_already_executing() -> None:
    """A job with a live consumer on this worker is never handed back to pending.

    The hand-back exists for rows this worker claimed but never started: the
    local_queue backlog. A row whose consumer is already running is the exact
    opposite case - clearing its lock would publish it to the fleet while this
    worker's consumer is still inside the actor body, so the job body runs a
    second time on the claimer while the first run is still in flight. The
    cancelling / forcing / abandoning phases own those rows and drive them to
    a terminal state; DRAINING must leave them alone.

    The discriminator is this process's own in-flight registry, because the
    row itself carries no "a consumer took it" mark - the dispatch claim
    stamps started_at at claim time for every claimed row alike.
    """
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    running_ids = [JobId(new_uuid()) for _ in range(2)]
    for epoch, jid in enumerate(running_ids, start=1):
        await deps.active_jobs.register(
            jid,
            cast("asyncio.Task[object]", MagicMock()),
            _make_ctx(jid, epoch),
        )

    await drain_local_queue_to_pending(deps, worker_id)

    assert len(pool.execute_calls) == 1
    sql, args = pool.execute_calls[0]
    assert args[0] == worker_id
    assert len(args) == 3, (
        "the hand-back UPDATE must bind the in-flight (job id, claim epoch) "
        f"pairs so rows with a live consumer are excluded from the re-pend; "
        f"bound args were {args!r}"
    )
    assert set(zip(cast("list[object]", args[1]), cast("list[object]", args[2]), strict=True)) == {
        (jid, epoch) for epoch, jid in enumerate(running_ids, start=1)
    }, (
        "every job registered as in-flight on this worker must appear in the "
        f"exclusion list with the epoch it is held at; got {args[1:]!r} for "
        f"registry {running_ids!r}"
    )
    assert "<>" in sql or "NOT" in sql.upper(), (
        f"the bound in-flight ids must be used as an exclusion predicate: {sql!r}"
    )


async def test_drain_hands_back_at_most_once_across_repeated_calls() -> None:
    """A second hand-back pass re-pends nothing that the first already released.

    The hand-back predicate is scoped to rows still locked by this worker, so a
    retried or re-entered DRAINING phase cannot release a row twice - the first
    pass cleared the lock, and the second matches nothing. This is what makes
    "handed back exactly once" a property of the statement rather than of
    shutdown being called exactly once: a drain-monitor trigger racing a
    SIGTERM, or a retry after a transient failure, must not reopen a row that
    another worker has since re-claimed and started.
    """
    worker_id = new_uuid()
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    await drain_local_queue_to_pending(deps, worker_id)

    sql, args = pool.execute_calls[0]
    assert args[0] == worker_id
    normalised = " ".join(sql.split())
    assert "locked_by_worker=$1" in normalised.replace(" = ", "=")
    assert "status='running'" in normalised.replace(" = ", "=").replace('"', "'"), (
        "the hand-back must be scoped to rows this worker still holds "
        f"(locked_by_worker + status='running'): {normalised!r}"
    )
    assert "locked_by_worker=NULL" in normalised.replace(" = ", "="), (
        f"the hand-back must clear the lock, so a second pass matches nothing: {normalised!r}"
    )
    assert "status='pending'" in normalised.replace(" = ", "="), (
        f"the hand-back must set the row back to pending: {normalised!r}"
    )


async def test_drain_exclusion_captures_a_take_that_lands_during_the_pool_acquire() -> None:
    """A claim intent marked while the drain waits for its connection is excluded.

    The exclusion arrays are the row's classification ("a consumer of this
    process holds this row"), and the UPDATE is the writer that acts on it.
    The registry's maps are in-memory; the pool acquire is the one await
    between the helper's entry and its statement, and on a contended pool it
    parks the drain for as long as the contention lasts - exactly the window
    a consumer's take (mark_claimed) or registration lands in. A capture
    taken before the acquire binds a stale exclusion: the UPDATE re-pends the
    row under its live consumer, the consumer's terminal write fences out
    (the fence requires status='running' and locked_by_worker, both cleared
    by the re-pend), and the row is stranded 'pending' with its body
    mid-flight. The capture must therefore happen after the acquire,
    immediately before the bind: the bound exclusion carries the intent the
    acquire's park let land.

    The fake pool marks the take inside ``acquire`` - the take's exact
    timing, deterministically. RED on a capture-before-acquire tree: the
    exclusion binds empty and the UPDATE runs with no claim-pair predicate.
    """
    worker_id = new_uuid()
    taken_id = JobId(new_uuid())
    pool = FakePool()
    settings = _worker_settings("taskq")
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    real_acquire = pool.acquire

    @asynccontextmanager
    async def acquire_marking_the_take(
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's signature, the same convention FakePool.acquires uses.
    ) -> AsyncGenerator[FakeConn, None]:
        # The consumer's take: the row is claimed (running, locked) and this
        # loop's take records the claim intent - the registry's own fast
        # path, no awaits, exactly what mark_claimed does at a real take.
        deps.active_jobs.mark_claimed(taken_id, 1)
        async with real_acquire(timeout=timeout) as conn:
            yield conn

    cast("Any", deps.dispatcher_pool).acquire = acquire_marking_the_take

    await drain_local_queue_to_pending(deps, worker_id)

    sql, args = pool.execute_calls[0]
    assert len(args) == 3, (
        "the drain captured its exclusion BEFORE the pool acquire: the take "
        f"that landed during the acquire's park is missing from the bound "
        f"exclusion (bound args {args!r}), and the UPDATE re-pends a row its "
        "consumer is executing - the terminal write fences out and the row "
        "strands 'pending'"
    )
    assert set(cast("list[object]", args[1])) == {taken_id}, (
        "the take that landed during the pool acquire must appear in the "
        f"exclusion list; got {args[1]!r}"
    )
    assert cast("list[object]", args[2]) == [1], (
        f"the exclusion must carry the claim epoch the take landed at; got {args[2]!r}"
    )
    assert "<>" in sql or "NOT" in sql.upper(), (
        f"the exclusion must be a bound predicate of the UPDATE: {sql!r}"
    )


# ── the producer's exit hand-back ─────────────────────────────────────────

_ROWS_IN_ROUND = 3


class _ClaimingBackend:
    """Claims one full round, then sets the draining stop as the round commits.

    The stop landing inside the claim is exactly the post-stop case: the round
    commits after ``producer_stop_event`` was set, so the producer breaks
    straight to its exit hand-back.
    """

    def __init__(self, stop_event: asyncio.Event) -> None:
        self._stop_event = stop_event
        self.rounds = 0

    async def dispatch_batch(self, **_kwargs: Any) -> list[JobRow]:
        self.rounds += 1
        rows = [
            replace(
                make_job_row(actor="test_actor", status="running"),
                id=JobId(new_uuid()),
                claim_epoch=1,
            )
            for _ in range(_ROWS_IN_ROUND)
        ]
        self._stop_event.set()
        return rows


class _CountingLiveness:
    """Liveness stub that records which named loop forgot its registration."""

    def __init__(self) -> None:
        self.forgotten: list[str] = []

    def tick(self, *_args: object, **_kwargs: object) -> None:
        return None

    def forget(self, name: str) -> None:
        self.forgotten.append(name)


def _producer_deps(pool: object) -> SimpleNamespace:
    """A WorkerDeps stand-in carrying only what ``producer_loop`` reads."""
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=0.05,
        notify_poll_interval=0.05,
        max_concurrency=_ROWS_IN_ROUND,
        schema_name="taskq",
        pg_is_pooled=False,
    )
    return SimpleNamespace(
        settings=settings,
        liveness=_CountingLiveness(),
        active_jobs=SimpleNamespace(
            all=list,
            count=lambda: 0,
            intent_count=lambda: 0,
            held_claims=list,
        ),
        disowned_jobs=set(),
        dispatcher_pool=pool,
    )


async def _drive_producer(
    deps: SimpleNamespace,
    backend: object,
    local_queue: asyncio.Queue[JobRow],
    stop_event: asyncio.Event,
) -> BaseException | None:
    """Run the real loop to its exit; return the escaped error, if any."""
    try:
        await producer_loop(
            deps,  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in for WorkerDeps, the established producer-loop unit pattern.
            local_queue,
            asyncio.Event(),  # shutdown_event: clear, the draining path
            stop_event,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
        )
    except BaseException as exc:  # Why: the caller asserts on anything that escaped.
        return exc
    return None


async def test_the_post_stop_round_is_recorded_even_when_nothing_is_re_pended() -> None:
    """A skipped post-stop round is named, and sized, even at zero re-pends.

    On the draining path a claim round that commits after the stop is skipped
    by the ``break`` and handed back by the producer's exit pass. That round
    is the only claim of the shutdown that no in-process structure knows
    about (it is in neither ``local_queue`` nor the registry), so the exit
    pass is its sole writer.

    Both records must exist: ``producer-post-stop-round-skipped`` for the
    round the break declined to enqueue, and ``producer-exit-handback``
    carrying ``rows_skipped`` beside ``rows_re_pended`` even when the
    hand-back re-pends nothing. Otherwise "the exit pass ran and released
    nothing" and "the exit pass never ran" look identical, while the rows sit
    on lease expiry, each spending a charged attempt.
    """
    stop_event = asyncio.Event()
    backend = _ClaimingBackend(stop_event)
    deps = _producer_deps(FakePool(tag="UPDATE 0"))
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=_ROWS_IN_ROUND)

    with structlog.testing.capture_logs() as captured:
        escaped = await _drive_producer(deps, backend, local_queue, stop_event)

    assert escaped is None, f"the clean draining exit must not raise; got {escaped!r}"
    assert backend.rounds == 1, f"premise: one claim round; got {backend.rounds}"
    assert local_queue.qsize() == 0, "premise: the post-stop round was never queued"

    skipped_records = [
        entry for entry in captured if entry.get("event") == "producer-post-stop-round-skipped"
    ]
    assert len(skipped_records) == 1, (
        f"the skipped post-stop round must be recorded once. Captured: {captured!r}"
    )
    assert skipped_records[0]["rows_skipped"] == _ROWS_IN_ROUND, (
        f"the skipped round's size must be on its record. Got: {skipped_records[0]!r}"
    )

    hand_back_records = [
        entry for entry in captured if entry.get("event") == "producer-exit-handback"
    ]
    assert len(hand_back_records) == 1, (
        "the producer's exit hand-back ran but was not recorded: with a "
        f"zero-row re-pend the pass is invisible. Captured: {captured!r}"
    )
    record = hand_back_records[0]
    assert record["rows_re_pended"] == 0, f"premise: nothing was re-pended. Got: {record!r}"
    assert record["rows_skipped"] == _ROWS_IN_ROUND, (
        "the exit pass's record must quantify the round it was solely "
        f"responsible for. Got: {record!r}"
    )


async def test_a_non_transient_exit_hand_back_failure_is_contained_and_recorded() -> None:
    """A permanent failure in the exit hand-back must not crash the producer.

    The helper contains only ``TRANSIENT_PG_ERRORS``; a non-transient error
    (here an ``UndefinedColumnError`` from a migration-skewed deploy) escapes
    it. Uncontained, that exception leaves ``producer_loop`` into ``_guarded``,
    which counts a sibling crash and re-raises - aborting the worker's
    TaskGroup and cancelling the shutdown orchestrator's remaining phases -
    and the producer's own ``producer-loop-exit`` record and
    ``liveness.forget`` never run.

    The exit pass must therefore be contained like the DRAINING phase's
    identical call: the failure gets its own record (class + rows at stake)
    and the loop still exits cleanly.
    """
    stop_event = asyncio.Event()
    backend = _ClaimingBackend(stop_event)
    deps = _producer_deps(
        FakePool(
            fail_acquire_with=asyncpg.UndefinedColumnError("column j.claim_epoch does not exist")
        )
    )
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=_ROWS_IN_ROUND)

    with structlog.testing.capture_logs() as captured:
        escaped = await _drive_producer(deps, backend, local_queue, stop_event)

    liveness = cast("_CountingLiveness", deps.liveness)
    assert escaped is None, (
        "a non-transient failure in the exit hand-back escaped producer_loop "
        f"({escaped!r}): escaping aborts the worker's TaskGroup and cancels "
        "the shutdown orchestrator's remaining phases, and the rows the pass "
        "owed the fleet are stranded with no record naming them"
    )
    assert liveness.forgotten == ["producer"], (
        f"the producer's exit bookkeeping must still run; got {liveness.forgotten!r}"
    )
    assert any(entry.get("event") == "producer-loop-exit" for entry in captured), (
        f"the producer must still record its own exit. Captured: {captured!r}"
    )
    failures = [
        entry for entry in captured if entry.get("event") == "producer-exit-handback-failed"
    ]
    assert len(failures) == 1, (
        f"the failed exit hand-back must leave a record. Captured: {captured!r}"
    )
    record = failures[0]
    assert record["error_class"] == "UndefinedColumnError", (
        f"the record must name the failure class. Got: {record!r}"
    )
    assert record["rows_skipped"] == _ROWS_IN_ROUND, (
        f"the record must name the rows at stake. Got: {record!r}"
    )
