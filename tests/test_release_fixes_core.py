"""Regression tests for the release-hardening findings (see task ticket).

Each test class/section maps to one numbered finding. Tests that need a
live Postgres are marked ``integration`` and reuse the shared fixtures
from :mod:`taskq.testing.fixtures` via ``tests/conftest.py``.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from taskq._ids import new_base62, new_uuid
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._flush import _flush_buffer
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.testing.fixtures import JobsApp, ModulePgSchema, _open_pg_backend
from taskq.testing.jobs import make_enqueue_args

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_WORKER_ID = UUID("11111111-2222-3333-4444-555555555555")


# ── Finding 1: progress lost-update race ────────────────────────────────


def _make_gated_pool(
    *, returning_row: dict[str, object] | None, release_event: asyncio.Event
) -> MagicMock:
    """A pool mock whose fetchrow blocks on `release_event` before returning.

    Lets the test mutate the buffer while `_flush_buffer` is suspended
    inside the `await conn.fetchrow(...)` call, simulating a ctx.progress()
    call landing mid-flight.
    """
    conn = AsyncMock()

    async def _fetchrow(*_args: object, **_kwargs: object) -> dict[str, object] | None:
        await release_event.wait()
        return returning_row

    conn.fetchrow.side_effect = _fetchrow

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire
    return pool


@pytest.mark.asyncio
async def test_flush_buffer_survives_late_update_mid_flight() -> None:
    """A ctx.progress() call landing during the DB await is not discarded.

    Regression for the lost-update race: the old `_flush_buffer` snapshot
    the buffer, awaited the DB write, then wholesale-reset
    (base_seq=returned, delta=0, dirty=False, pending_state={}) — silently
    dropping any mutation that happened during the await.
    """
    release_event = asyncio.Event()
    pool = _make_gated_pool(returning_row={"progress_seq": 5}, release_event=release_event)

    progress_buffers: dict[UUID, _ProgressBuffer] = {}
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buf.pending_seq_delta = 5
    buf.pending_state["step"] = "phase-1"
    buf.dirty = True
    progress_buffers[_JOB_ID] = buf

    flush_task = asyncio.create_task(
        _flush_buffer(pool, "taskq", _JOB_ID, _WORKER_ID, buf, progress_buffers)
    )
    await asyncio.sleep(0)  # let the flush task reach the gated fetchrow

    # Late update lands while the flush is suspended awaiting the DB.
    buf.pending_seq_delta += 1
    buf.pending_state["step"] = "phase-2"

    release_event.set()
    await flush_task

    # The DB write reflects the snapshot (delta=5); base_seq is now 5.
    assert buf.base_seq == 5
    # The late delta (+1) survives — not clobbered by a delta=0 reset.
    assert buf.pending_seq_delta == 1
    # The late state mutation survives.
    assert buf.pending_state == {"step": "phase-2"}
    # Buffer remains dirty so the next flush picks up the late update.
    assert buf.dirty is True


@pytest.mark.asyncio
async def test_flush_buffer_clean_when_no_late_update() -> None:
    """Sanity check: with no concurrent mutation, flush still fully clears."""
    release_event = asyncio.Event()
    release_event.set()
    pool = _make_gated_pool(returning_row={"progress_seq": 3}, release_event=release_event)

    progress_buffers: dict[UUID, _ProgressBuffer] = {}
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buf.pending_seq_delta = 3
    buf.pending_state["step"] = "done"
    buf.dirty = True
    progress_buffers[_JOB_ID] = buf

    await _flush_buffer(pool, "taskq", _JOB_ID, _WORKER_ID, buf, progress_buffers)

    assert buf.base_seq == 3
    assert buf.pending_seq_delta == 0
    assert buf.pending_state == {}
    assert buf.dirty is False


# ── Finding 4: registry.register() idempotent-by-value ─────────────────


def test_register_identical_token_bucket_is_noop() -> None:
    registry = RateLimitRegistry()
    a = TokenBucket(name="dup", capacity=10, refill_per_second=1, backend="memory")
    b = TokenBucket(name="dup", capacity=10, refill_per_second=1, backend="memory")

    registry.register(a)
    registry.register(b)  # identical config — no-op, no raise

    assert registry.get_rate_limit("dup") is a


def test_register_conflicting_token_bucket_raises_naming_both() -> None:
    registry = RateLimitRegistry()
    a = TokenBucket(name="dup", capacity=10, refill_per_second=1, backend="memory")
    b = TokenBucket(name="dup", capacity=20, refill_per_second=1, backend="memory")

    registry.register(a)
    with pytest.raises(ValueError) as excinfo:
        registry.register(b)

    message = str(excinfo.value)
    assert "dup" in message
    assert repr(a) in message
    assert repr(b) in message


# ── Finding 6: terminal-write failure must not mislabel the actor error ──


class _RaisingBackend:
    """Backend stub whose mark_failed_or_retry raises once, then would succeed.

    Only the raising behaviour is exercised — the test checks that the
    infra exception propagates out of ``_handle_generic_exception`` (so
    the caller — ``_run_terminal_path`` or ``dispatch.py`` — can catch it
    consistently), rather than being swallowed internally (which caused a
    false terminal Redis publish via ``_run_terminal_path``).
    """

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.calls = 0

    async def mark_failed_or_retry(self, **_kwargs: object) -> None:
        self.calls += 1
        raise self._exc


@pytest.mark.asyncio
async def test_handle_generic_exception_propagates_infra_write_failure() -> None:
    """After the fix, _handle_generic_exception propagates infra exceptions
    from the terminal write so that _run_terminal_path's outer guard (or the
    dispatch.py direct-call guard) catches them consistently — instead of
    swallowing them and causing a false terminal Redis publish."""
    from datetime import UTC, datetime

    import asyncpg
    import structlog

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_generic_exception

    infra_exc = asyncpg.PostgresConnectionError("connection lost")
    backend = _RaisingBackend(infra_exc)
    job = make_job_row(attempt=1, max_attempts=3)
    actor_config = StubActorConfig(retry=RetryPolicy(max_attempts=3))
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None:
            return None

    actor_exc = RuntimeError("actor blew up")

    # The infra exception now propagates — the caller is responsible for
    # catching it (see _run_terminal_path and dispatch.py).
    with pytest.raises(asyncpg.PostgresConnectionError):
        await _handle_generic_exception(
            backend,  # type: ignore[arg-type]
            job,
            _WORKER_ID,
            actor_exc,
            actor_config,
            clock,
            timedelta(hours=24),
            _FakeSpan(),  # type: ignore[arg-type]
            log,
        )

    assert backend.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["timeout", "snooze", "generic"])
async def test_dispatch_exception_swallows_infra_failure_all_handlers(
    path: str,
) -> None:
    """Infra failures during ANY terminal handler's write are caught in
    _run_terminal_path — not just _handle_generic_exception's — so they can
    never be re-dispatched into generic handling as the actor's failure."""
    from datetime import UTC, datetime

    import asyncpg
    import structlog

    from taskq.exceptions import Snooze
    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _dispatch_exception

    infra_exc = asyncpg.PostgresConnectionError("connection lost")

    class _RaisingTerminalBackend:
        def __init__(self) -> None:
            self.calls = 0

        async def mark_failed_or_retry(self, *_args: object, **_kwargs: object) -> object:
            self.calls += 1
            raise infra_exc

        async def mark_snoozed(self, *_args: object, **_kwargs: object) -> str:
            self.calls += 1
            raise infra_exc

    backend = _RaisingTerminalBackend()
    job = make_job_row(attempt=1, max_attempts=3)
    actor_config = StubActorConfig(retry=RetryPolicy(max_attempts=3))
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None:
            return None

    if path == "timeout":
        exc: BaseException = TimeoutError("actor timed out")
    elif path == "snooze":
        exc = Snooze(timedelta(seconds=30))
    else:
        exc = RuntimeError("actor blew up")

    # Must not raise — the infra exception is caught in _run_terminal_path.
    outcome = await _dispatch_exception(
        exc,
        backend=backend,  # type: ignore[arg-type]
        job=job,
        worker_id=_WORKER_ID,
        actor_config=actor_config,
        clock=clock,
        max_retry_backoff=timedelta(hours=24),
        consumer_span=_FakeSpan(),  # type: ignore[arg-type]
        log=log,
        progress_buffers=None,
        worker_pool=None,
        settings=None,
        redis_client=None,
    )

    assert backend.calls == 1
    assert outcome in ("failed", "scheduled")


# ── Finding 9: enqueue SQL uses clock_timestamp() consistently ──────────


def test_enqueue_sql_does_not_mix_now_and_clock_timestamp() -> None:
    from taskq.backend._sql_templates import render

    sql = render("taskq")
    assert "now()" not in sql.enqueue, (
        "enqueue SQL should use clock_timestamp() exclusively, matching enqueue_with_interval"
    )


# ── Finding 12: SubJobEnqueuer.enqueue_batch(batch_id=...) passthrough ──


@pytest.mark.asyncio
async def test_sub_job_enqueuer_enqueue_batch_uses_supplied_batch_id() -> None:
    import dataclasses

    from examples.actors.basic import CounterPayload, counter

    from taskq.backend._protocol import EnqueueArgs, JobRow
    from taskq.batch import EnqueueItem
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.testing.jobs import make_job_row

    class _EnqueueOnlyBackend:
        supports_transactional_simulation = False

        async def enqueue(self, args: EnqueueArgs) -> JobRow:
            base = make_job_row(actor=args.actor, queue=args.queue, payload=args.payload)
            return dataclasses.replace(base, id=args.id, metadata=dict(args.metadata))

    backend = _EnqueueOnlyBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=MagicMock(),
        backend=backend,  # type: ignore[arg-type]
    )

    supplied_batch_id = new_uuid()

    items = [EnqueueItem(actor_ref=counter, payload=CounterPayload(n=1))]

    handles = await enqueuer.enqueue_batch(items, batch_id=supplied_batch_id)

    assert len(handles) == 1
    row = handles[0]._row
    assert row.metadata is not None
    assert row.metadata.get("batch_id") == str(supplied_batch_id)


# ── Finding 3: token-bucket PG cold-start over-admission ────────────────


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_token_bucket_cold_start_single_admission(pg_dsn: str) -> None:
    """Two concurrent first acquires on a capacity=1 bucket admit exactly 1.

    Regression for the cold-start race: SELECT ... FOR UPDATE cannot lock a
    row that doesn't exist yet, so before the fix both concurrent first
    acquires would read `row is None`, each computing tokens=capacity
    independently and both being admitted.
    """
    from datetime import UTC, datetime

    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock

    schema = f"tbcs_{new_base62()}".lower()
    stack, deps, _backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        settings = WorkerSettings.load_from_dict(
            {"pg_dsn": pg_dsn, "schema_name": deps.settings.schema_name}
        )
        clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
        bucket = TokenBucket(
            name=f"cold-{new_base62()}",
            capacity=1.0,
            refill_per_second=0.0,
            backend="postgres",
        )

        barrier = asyncio.Barrier(2)

        async def _acquire() -> bool:
            await barrier.wait()
            decision = await bucket.acquire(
                1.0, pg_pool=deps.worker_pool, clock=clock, settings=settings
            )
            return decision.allowed

        results = await asyncio.gather(_acquire(), _acquire())
        assert sum(1 for r in results if r) == 1, (
            f"expected exactly 1 admission on cold-start, got {sum(1 for r in results if r)}"
        )
    finally:
        await stack.aclose()


# ── Finding 5 helper reused by test_dispatch_fairness_pg.py ─────────────

__all__ = ["JobsApp", "ModulePgSchema", "make_enqueue_args"]


# ── Traceback regression: format the explicit exception, not the ambient ──


class _RecordingTerminalBackend:
    """Backend stub recording mark_failed_or_retry kwargs; never raises.

    Used by direct-call handler tests that take the retry path, where the
    handler only needs the write to succeed (the returned row is unused).
    """

    def __init__(self) -> None:
        self.mark_failed_or_retry_calls: list[dict[str, object]] = []

    async def mark_failed_or_retry(self, **kwargs: object) -> None:
        self.mark_failed_or_retry_calls.append(kwargs)


class _FakeSpan:
    def add_event(self, *_args: object, **_kwargs: object) -> None:
        return None


@pytest.mark.asyncio
async def test_generic_exception_traceback_without_active_exception() -> None:
    """Direct call outside an except block: error_traceback must come from
    the explicit exception, not the ambient (empty) one."""
    from datetime import UTC, datetime

    import structlog

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_generic_exception

    backend = _RecordingTerminalBackend()
    job = make_job_row(attempt=1, max_attempts=3)
    actor_config = StubActorConfig(retry=RetryPolicy(max_attempts=3))
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    log = MagicMock(spec=structlog.stdlib.BoundLogger)
    log.bind.return_value = log

    actor_exc = RuntimeError("actor blew up")  # constructed, never raised

    await _handle_generic_exception(
        backend,  # type: ignore[arg-type]
        job,
        _WORKER_ID,
        actor_exc,
        actor_config,
        clock,
        timedelta(hours=24),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
    )

    # Level-agnostic: the level pins live in test_consumer.py; this test
    # pins the traceback regression only.
    job_exception_calls = [
        c
        for c in (*log.warning.call_args_list, *log.error.call_args_list)
        if c.args and c.args[0] == "job_exception"
    ]
    assert len(job_exception_calls) == 1, (
        f"expected 1 job_exception log, got {len(job_exception_calls)}"
    )
    tb = job_exception_calls[0].kwargs["error_traceback"]
    assert "RuntimeError: actor blew up" in tb
    assert "NoneType" not in tb


@pytest.mark.asyncio
async def test_timeout_traceback_without_active_exception() -> None:
    """Same regression pin for _handle_timeout."""
    from datetime import UTC, datetime

    import structlog

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_timeout

    backend = _RecordingTerminalBackend()
    job = make_job_row(attempt=1, max_attempts=3)
    actor_config = StubActorConfig(retry=RetryPolicy(max_attempts=3))
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    log = MagicMock(spec=structlog.stdlib.BoundLogger)
    log.bind.return_value = log

    timeout_exc = TimeoutError("db slow")  # constructed, never raised

    await _handle_timeout(
        backend,  # type: ignore[arg-type]
        job,
        _WORKER_ID,
        timeout_exc,
        actor_config,
        clock,
        timedelta(hours=24),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
    )

    job_timeout_calls = [
        c
        for c in (*log.warning.call_args_list, *log.error.call_args_list)
        if c.args and c.args[0] == "job_timeout"
    ]
    assert len(job_timeout_calls) == 1, f"expected 1 job_timeout log, got {len(job_timeout_calls)}"
    tb = job_timeout_calls[0].kwargs["error_traceback"]
    assert "TimeoutError: db slow" in tb
    assert "NoneType" not in tb


# ── terminal-write-failed: job/infra traceback fields ────────────────────


def test_log_terminal_write_failed_includes_tracebacks() -> None:
    """Error-path terminal write failure logs one ERROR event carrying real
    tracebacks for both the actor exception and the infra exception."""
    import structlog

    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _log_terminal_write_failed

    try:
        raise RuntimeError("actor blew up")
    except RuntimeError as exc:
        job_exc = exc

    try:
        raise OSError("db socket closed")
    except OSError as exc:
        infra_exc = exc

    job = make_job_row()
    log = MagicMock(spec=structlog.stdlib.BoundLogger)
    log.bind.return_value = log

    _log_terminal_write_failed(log, job, job_exc, infra_exc)

    log.error.assert_called_once()
    call = log.error.call_args
    assert call.args[0] == "terminal-write-failed"
    kwargs = call.kwargs
    assert kwargs["kind"] == "terminal-write-failed"
    assert kwargs["actor_succeeded"] is False
    assert kwargs["job_error_class"] == "RuntimeError"
    assert "RuntimeError: actor blew up" in kwargs["job_error_traceback"]
    assert kwargs["infra_error_class"] == "OSError"
    assert "OSError: db socket closed" in kwargs["infra_error_traceback"]


def test_log_terminal_write_failed_success_path_omits_job_fields() -> None:
    """Success-path terminal write failure (job_exc=None): actor_succeeded
    is True and the job_* fields are None; infra fields stay populated."""
    import structlog

    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _log_terminal_write_failed

    try:
        raise OSError("db socket closed")
    except OSError as exc:
        infra_exc = exc

    job = make_job_row()
    log = MagicMock(spec=structlog.stdlib.BoundLogger)
    log.bind.return_value = log

    _log_terminal_write_failed(log, job, None, infra_exc)

    log.error.assert_called_once()
    call = log.error.call_args
    assert call.args[0] == "terminal-write-failed"
    kwargs = call.kwargs
    assert kwargs["actor_succeeded"] is True
    assert kwargs["job_error_class"] is None
    assert kwargs["job_error_message"] is None
    assert kwargs["job_error_traceback"] is None
    assert kwargs["infra_error_class"] == "OSError"
    assert "OSError: db socket closed" in kwargs["infra_error_traceback"]
