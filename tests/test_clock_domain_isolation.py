"""Clock-domain isolation tests — the C3/C4/C5/C6/C11 skew series.

TaskQ runs two independent clocks: the injectable Python ``Clock`` and
the PG server clock (``clock_timestamp()``).  Every predicate below is
exercised with the Python clock deliberately skewed relative to the
server (via :class:`tests._clock_skew.SkewedClock`) and must behave as
if no skew existed — the server is the single arbiter for every
skew-sensitive decision.  No assertion in this module may depend on
cross-domain clock alignment.
"""

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.actor import actor
from taskq.backend._protocol import EnqueueArgs, IdentityKey
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import (
    create_pending_job,
    create_worker,
    seed_actors,
    setup_running_job,
)
from tests._clock_skew import SkewedClock

_integration = pytest.mark.integration

_GRACE = timedelta(seconds=30)


async def server_now(conn: asyncpg.Connection) -> datetime:
    """Read the PG server clock — the domain tests skew *against*."""
    return await conn.fetchval("SELECT clock_timestamp()")


@asynccontextmanager
async def _connect(pg_dsn: str) -> AsyncGenerator[asyncpg.Connection]:
    """Open-and-close an asyncpg connection (await-then-context idiom)."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        yield conn
    finally:
        await conn.close()


async def _mk_backend(
    pg_dsn: str,
    skew: timedelta,
) -> tuple[AsyncExitStack, PostgresBackend, str]:
    """Clean migrated schema + WorkerDeps + PostgresBackend on a skewed clock.

    Same shape as ``_setup`` in ``tests/test_cancel_notify_integration.py``
    with one substitution: the backend is wired with
    ``SkewedClock(SystemClock(), skew)`` so the Python clock domain is
    deliberately offset from the PG server clock.  Returns
    ``(stack, backend, schema)``; caller MUST ``await stack.aclose()``.
    """
    from taskq.migrate import apply_pending
    from taskq.testing.settings import make_integration_settings
    from taskq.worker.deps import open_worker_deps

    settings = make_integration_settings(pg_dsn)
    schema = settings.schema_name

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    stack = AsyncExitStack()
    deps: Any = await stack.enter_async_context(open_worker_deps(settings))
    backend = PostgresBackend(deps, SkewedClock(SystemClock(), skew), _GRACE, _GRACE)
    return stack, backend, schema


async def _seed_dispatch_targets(pg_dsn: str, schema: str, *, actors: list[str]) -> Any:
    """Seed actor_config + a worker row so dispatch_batch can pick jobs up."""
    worker_id = new_uuid()
    async with _connect(pg_dsn) as conn:
        await seed_actors(conn, schema, actors=actors)
        await create_worker(conn, schema, worker_id)
    return worker_id


# ── Module-level actor for client-path tests ─────────────────────────────


class _SkewPayload(BaseModel):
    value: int = 0


@actor(name="clock_domain_skew_actor")
async def _skew_actor(_payload: _SkewPayload) -> None:
    pass


# ── C3: schedule_to_close anchored to the server clock on every arm ──────


@_integration
async def test_schedule_to_close_interval_anchored_to_server_clock(pg_dsn: str) -> None:
    """An interval-form schedule_to_close must be anchored to the SERVER
    clock on every arm.  The caller's stamp simulates a client clock 120 s
    ahead of PG; pre-fix, the batch arm computes ``args.scheduled_at +
    interval`` in Python and stores a server_now+720 s absolute (drift
    +120 s).  The single arm already computes server-side — it is the
    regression guard; the batch arm is the failing case.  The COPY arm is
    pinned by the C5/C6 tests below."""
    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=120))
    try:
        stamp = datetime.now(UTC) + timedelta(seconds=120)  # client-domain "now"
        jobs: dict[str, object] = {}
        for via, jid in (("single", new_job_id()), ("batch", new_job_id())):
            args = EnqueueArgs(
                id=jid,
                actor="a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=stamp,
                schedule_to_close_interval=timedelta(minutes=10),
            )
            if via == "single":
                await backend.enqueue(args)
            else:
                await backend.enqueue_batch([args])
            jobs[via] = jid

        async with _connect(pg_dsn) as conn:
            expected = await server_now(conn) + timedelta(minutes=10)
            for via, jid in jobs.items():
                row = await conn.fetchrow(
                    f'SELECT schedule_to_close FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                    jid,
                )
                assert row is not None, f"{via} arm: row missing"
                drift = (row["schedule_to_close"] - expected).total_seconds()
                assert abs(drift) < 1.0, f"{via} arm drifted {drift:+.1f}s off the server anchor"
    finally:
        await stack.aclose()


# ── C6: COPY-path created_at is server-stamped so unique_for holds ───────


@_integration
async def test_copy_path_created_at_server_stamped_dedup_window_holds(pg_dsn: str) -> None:
    """Rows written by ``enqueue_batch_fast`` must carry a SERVER ``created_at``
    so the ``unique_for`` preflight (``created_at > now() - interval``,
    server-side) measures the true age.  The Python clock is skewed -120 s:
    pre-fix the COPY stamps ``created_at`` in the Python domain, the first
    row looks 120 s older than it is, and the duplicate arriving inside the
    30 s window escapes dedup — a duplicate side effect."""
    from dataclasses import replace

    stack, backend, _schema = await _mk_backend(pg_dsn, timedelta(seconds=-120))
    try:
        first = EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=120),  # client clock behind
            identity_key=IdentityKey("vendor-order-1"),
            unique_for=timedelta(seconds=30),
        )
        await backend.enqueue_batch_fast([first])

        second = replace(first, id=new_job_id())
        dup = await backend.enqueue(second)

        assert dup.id == first.id  # deduplicated onto the first row; pre-fix: a NEW row is inserted
    finally:
        await stack.aclose()


# ── C5: COPY-path status/schedule decided by the fixup UPDATE's CASE ─────


@_integration
async def test_copy_path_status_decided_server_side_explicit_future(pg_dsn: str) -> None:
    """The COPY path's status comes from the fixup UPDATE's server CASE.
    Pinned here for the explicit-future row; the immediate-row case (status
    under positive skew) is pinned by
    ``test_immediate_enqueue_dispatchable_under_positive_skew``."""
    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=-120))
    try:
        args = EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) + timedelta(hours=1),
        )
        await backend.enqueue_batch_fast([args])
        async with _connect(pg_dsn) as conn:
            status = await conn.fetchval(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                args.id,
            )
        assert status == "scheduled"
    finally:
        await stack.aclose()


# ── C4: enqueue status has a single arbiter (the server) ─────────────────


@_integration
async def test_immediate_enqueue_dispatchable_under_positive_skew(pg_dsn: str) -> None:
    """With the *producer host's* clock skewed +5 s ahead of the backend
    host (and the PG server), an immediate enqueue must still land
    ``status='pending'`` and be dispatchable NOW — the server is the only
    arbiter.  Pre-fix: the client stamped ``scheduled_at`` from its own
    skewed Python clock (``client/_args.py``), the backend's Python
    pre-decision kept it (it reads as future to the unskewed backend
    clock), and the server CASE (``COALESCE($14, clock_timestamp()) >
    clock_timestamp()``) read it as future → ``'scheduled'``, invisible to
    dispatch for 5 s."""
    from taskq.client._jobs import JobsClient

    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=0))
    try:
        client = JobsClient(backend=backend, clock=SkewedClock(SystemClock(), timedelta(seconds=5)))
        handle = await client.enqueue(_skew_actor, _SkewPayload())
        job_row = await backend.get(handle.job_id)
        assert job_row is not None
        assert job_row.status == "pending"

        # The same contract stated directly against the backend protocol:
        # scheduled_at=None IS the immediate form (old backends fail loudly
        # on it — TypeError, not silent misbehavior).
        direct = await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
            )
        )
        assert direct.status == "pending"

        worker_id = await _seed_dispatch_targets(pg_dsn, schema, actors=[_skew_actor.name, "a"])
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=2, lock_lease=timedelta(seconds=60)
        )
        assert sorted(str(d.id) for d in dispatched) == sorted((str(handle.job_id), str(direct.id)))
    finally:
        await stack.aclose()


@_integration
async def test_future_enqueue_still_deferred_under_skew(pg_dsn: str) -> None:
    """Regression guard: a caller-explicit future scheduled_at (an absolute
    datetime is the caller's own cross-domain intent) still lands
    ``'scheduled'``."""
    stack, backend, _schema = await _mk_backend(pg_dsn, timedelta(seconds=5))
    try:
        job = await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC) + timedelta(seconds=60),
            )
        )
        assert job.status == "scheduled"
    finally:
        await stack.aclose()


@_integration
async def test_copy_path_immediate_status_pending_under_positive_skew(pg_dsn: str) -> None:
    """The COPY path's immediate row is decided by the fixup UPDATE's server
    CASE: ``scheduled_at=None`` lands ``'pending'`` with a server-stamped
    ``scheduled_at`` even with the Python clock +5 s ahead.  Pre-fix the
    Python pre-decision (``args.scheduled_at > batch_now``) failed loudly
    on None (TypeError)."""
    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=5))
    try:
        args = EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        )
        await backend.enqueue_batch_fast([args])
        async with _connect(pg_dsn) as conn:
            rec = await conn.fetchrow(
                f'SELECT status, scheduled_at FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                args.id,
            )
            assert rec is not None
            assert rec["status"] == "pending"
            drift = (rec["scheduled_at"] - await server_now(conn)).total_seconds()
            assert abs(drift) < 1.0, (
                f"COPY scheduled_at drifted {drift:+.1f}s off the server anchor"
            )
    finally:
        await stack.aclose()


async def test_in_memory_parity_immediate_and_future() -> None:
    """InMemoryBackend mirrors the server semantics from its own injected
    clock (single domain): None → pending + clock-stamped; future →
    scheduled."""
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock)
    immediate = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    assert immediate.status == "pending"
    assert immediate.scheduled_at == clock.now()

    future = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now() + timedelta(hours=1),
        )
    )
    assert future.status == "scheduled"


# ── C1: mark_failed_or_retry delay applied by the server clock ───────────


@_integration
async def test_retry_backoff_not_voided_by_negative_skew(pg_dsn: str) -> None:
    """C1 pin: fail a running job with ``retry_delay=30s`` while the worker's
    Python clock is 120s BEHIND the server.  Pre-fix: the caller computed
    ``next_scheduled_at = python_now + 30s`` and the SQL stored/compared that
    Python-domain stamp — it lands 90s in the server's past, the
    ``$3 > clock_timestamp()`` CASE yields ``'pending'`` and the job is
    immediately re-dispatchable: exponential backoff voided.  Post-fix the
    server computes ``now() + 30s`` → ``'scheduled'``, not due, not
    dispatchable."""
    from taskq.backend._protocol import ErrorInfo
    from taskq.testing.pg import create_running_job

    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=-120))
    try:
        worker_id, job_id = new_uuid(), new_job_id()
        async with _connect(pg_dsn) as conn:
            await seed_actors(conn, schema, actors=["a"])
            await create_worker(conn, schema, worker_id)
            await create_running_job(conn, schema, worker_id, job_id)

        row = await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None),
            timedelta(seconds=30),
        )
        assert row.status == "scheduled"

        async with _connect(pg_dsn) as conn:
            due = await conn.fetchval(
                f'SELECT scheduled_at <= clock_timestamp() FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                job_id,
            )
        assert due is False
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert dispatched == []
    finally:
        await stack.aclose()


@_integration
async def test_retry_deadline_arbitrated_server_side(pg_dsn: str) -> None:
    """C1/C2 pin: ``schedule_to_close`` = server_now + 10s, ``retry_delay`` =
    30s, worker Python clock skewed -120s (a caller-side deadline pre-check
    computed from that clock would still say Retry).  The SQL deadline guard
    is the single arbiter: ``clock_timestamp() + 30s > schedule_to_close`` →
    the row lands ``'failed'`` with ``error_class='DeadlineExceeded'``
    instead of being retried past its deadline."""
    from taskq.backend._protocol import ErrorInfo
    from taskq.testing.pg import create_running_job

    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=-120))
    try:
        worker_id, job_id = new_uuid(), new_job_id()
        async with _connect(pg_dsn) as conn:
            await seed_actors(conn, schema, actors=["a"])
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                job_id,
                schedule_to_close=await server_now(conn) + timedelta(seconds=10),
            )

        row = await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None),
            timedelta(seconds=30),
        )
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"
    finally:
        await stack.aclose()


# ── C2: the retry classifier is not a deadline arbiter ───────────────────


def test_classifier_has_no_deadline_opinion() -> None:
    """C2 pin: attempts remain and the kind is retryable → Retry, regardless
    of ``schedule_to_close`` — the classifier takes NO deadline and NO
    clock input at all.  The SQL deadline guard in ``mark_failed_or_retry``
    (pinned above) is the only deadline arbiter; a Python-side pre-check
    computed from the worker's clock disagrees with it under skew and
    kills jobs early.  Pre-change: a long-past schedule_to_close returned
    ``Fail(DeadlineExceeded)``."""
    from taskq.retry import Retry, RetryClassifier, RetryPolicy

    decision = RetryClassifier.classify(
        RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        non_retryable_exceptions=(ValueError,),
        exception=RuntimeError("boom"),
        attempt=1,
        max_retry_backoff=timedelta(hours=24),
    )
    assert isinstance(decision, Retry)
    assert decision.retry_delay == timedelta(seconds=5)  # base backoff, jitter 0

    # The indefinite tier had its own Python deadline pre-check
    # (now >= schedule_to_close → Fail); it is gone for the same reason.
    indefinite = RetryClassifier.classify(
        RetryPolicy(kind="indefinite", max_attempts=3, jitter=0.0),
        non_retryable_exceptions=(ValueError,),
        exception=RuntimeError("boom"),
        attempt=9,  # far past max_attempts — irrelevant for the indefinite tier
        max_retry_backoff=timedelta(hours=24),
    )
    assert isinstance(indefinite, Retry)


@_integration
async def test_retry_survives_worker_clock_skew_deadline_disagreement(pg_dsn: str) -> None:
    """C2 pin (end-to-end): a worker whose Python clock is +300 s ahead of
    the server, with a LIVE server deadline (server_now + 200 s) and a
    10 s retry delay.  Pre-C2 the classifier's Python pre-check
    (python_now + 10 s >= schedule_to_close, i.e. 310 s >= 200 s) flipped
    the decision to Fail(DeadlineExceeded) — the row died early even
    though the server would have retried it (server_now + 10 s <=
    server_now + 200 s).  Post-C2 the classifier has no deadline opinion
    and the SQL guard is the only arbiter → the retry lands ``scheduled``.
    A genuinely-expired deadline still fails server-side."""
    from taskq.backend._protocol import ErrorInfo
    from taskq.retry import JobRetryState, Retry, RetryPolicy, decide_after_failure
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.pg import create_running_job

    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=300))
    try:
        actor_config = StubActorConfig(
            retry=RetryPolicy(
                kind="transient", max_attempts=3, jitter=0.0, base=timedelta(seconds=10)
            ),
            non_retryable_exceptions=(ValueError,),
        )

        # Live deadline: server_now + 200s (the classifier's skewed Python
        # opinion would call now+10s >= deadline and fail the job).
        worker_id, live_job = new_uuid(), new_job_id()
        # Genuinely-expired deadline: server_now + 5s with a 10s delay.
        dead_worker, dead_job = new_uuid(), new_job_id()
        async with _connect(pg_dsn) as conn:
            await seed_actors(conn, schema, actors=["a"])
            await create_worker(conn, schema, worker_id)
            await create_worker(conn, schema, dead_worker)
            await create_running_job(
                conn,
                schema,
                worker_id,
                live_job,
                schedule_to_close=await server_now(conn) + timedelta(seconds=200),
            )
            await create_running_job(
                conn,
                schema,
                dead_worker,
                dead_job,
                schedule_to_close=await server_now(conn) + timedelta(seconds=5),
            )

        for job_id, worker in ((live_job, worker_id), (dead_job, dead_worker)):
            row = await backend.get(job_id)
            assert row is not None
            job_state = JobRetryState(
                attempt=row.attempt,
                max_attempts=row.max_attempts,
                retry_kind=row.retry_kind,
                schedule_to_close=row.schedule_to_close,
                start_to_close=row.start_to_close,
            )
            decision = decide_after_failure(
                actor_config,
                RuntimeError("boom"),
                job_state,
                max_retry_backoff=timedelta(hours=24),
            )
            assert isinstance(decision, Retry), (
                f"job {job_id}: the classifier must not arbitrate the deadline"
            )
            assert decision.retry_delay == timedelta(seconds=10)  # base 10s, jitter 0

            updated = await backend.mark_failed_or_retry(
                job_id,
                worker,
                ErrorInfo(error_class="RuntimeError", error_message="boom", error_traceback=None),
                decision.retry_delay,
            )
            if job_id is live_job:
                assert updated.status == "scheduled", (
                    "live deadline: the server must retry (skew must not kill it)"
                )
            else:
                assert updated.status == "failed"
                assert updated.error_class == "DeadlineExceeded"
    finally:
        await stack.aclose()


# ── Sweep seam: no caller-supplied now — the backend's clock is the domain ─


async def test_in_memory_sweeps_use_injected_clock_no_caller_now() -> None:
    """Seam-removal pin: the sweep methods take NO caller-supplied ``now``.
    Pre-change the parameter was live on InMemoryBackend (the caller's
    clock decided promotion) and silently ignored on PostgresBackend (the
    server clock decided) — the two backends were not exercising the same
    contract.  Post-change InMemoryBackend consults its injected clock,
    which for InMemory IS the right single domain (the mirror of PG's
    server clock)."""
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock)
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now() + timedelta(seconds=60),
        )
    )

    # Not due by the backend's own clock → nothing promoted; there is no
    # caller `now` through which a different domain could be smuggled in.
    assert await backend.scheduled_to_pending() == 0
    clock.advance(timedelta(seconds=61))
    assert await backend.scheduled_to_pending() == 1


async def test_in_memory_deadline_sweep_uses_injected_clock() -> None:
    """Same seam contract for deadline_sweep: the injected clock arbitrates."""
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock)
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=clock.now() + timedelta(hours=1),
            schedule_to_close_interval=timedelta(seconds=60),
        )
    )

    assert await backend.deadline_sweep() == 0
    clock.advance(timedelta(seconds=61))
    assert await backend.deadline_sweep() == 1


# ── D4: batch INSERT immediate stamp is statement time, not txn start ────


@_integration
async def test_batch_immediate_stamp_is_statement_time_not_txn_start(pg_dsn: str) -> None:
    """``enqueue_batch`` on a caller-owned transaction that has been open
    for 0.6 s must stamp an immediate (``scheduled_at=None``) row with the
    STATEMENT-time server clock (``clock_timestamp()``) — matching the
    single-enqueue template and the COPY fixup.  Pre-fix the batch template
    used the transaction-start ``now()``, pinning every immediate batch
    item to when the caller's transaction began (0.6 s in the past here)."""
    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=0))
    try:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute("BEGIN")
            await conn.execute("SELECT pg_sleep(0.6)")
            job_id = new_job_id()
            await backend.enqueue_batch(
                [
                    EnqueueArgs(
                        id=job_id,
                        actor="a",
                        queue="default",
                        payload={},
                        max_attempts=3,
                        retry_kind="transient",
                        scheduled_at=None,
                    )
                ],
                connection=conn,
            )
            rec = await conn.fetchrow(
                f'SELECT scheduled_at, clock_timestamp() AS stmt_now FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                job_id,
            )
        finally:
            await conn.execute("ROLLBACK")
            await conn.close()
    finally:
        await stack.aclose()
    assert rec is not None
    drift = (rec["scheduled_at"] - rec["stmt_now"]).total_seconds()
    assert abs(drift) < 0.3, (
        f"immediate batch scheduled_at drifted {drift:+.2f}s off statement time "
        f"(pre-fix: pinned to the 0.6s-old transaction start)"
    )


# ── Batch/COPY stc anchoring: the budget runs from ENQUEUE time ──────────


@_integration
async def test_batch_stc_anchored_to_enqueue_time_future_item_fails_pre_dispatch(
    pg_dsn: str,
) -> None:
    """D9 pin: on EVERY arm the ``schedule_to_close`` budget runs from
    ENQUEUE time (``clock_timestamp() + interval``) — previously the
    batch/COPY arms anchored it to ``scheduled_at + interval``.  A
    future-scheduled batch item with a short interval therefore fails
    DeadlineExceeded BEFORE it is ever dispatched (sweep 2) — the unified
    single-arm contract, behavior-changing for batch users who relied on
    the old scheduled_at anchoring."""
    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=0))
    try:
        job_id = new_job_id()
        await backend.enqueue_batch(
            [
                EnqueueArgs(
                    id=job_id,
                    actor="a",
                    queue="default",
                    payload={},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=datetime.now(UTC) + timedelta(hours=1),
                    schedule_to_close_interval=timedelta(seconds=0),
                )
            ]
        )

        async with _connect(pg_dsn) as conn:
            rec = await conn.fetchrow(
                f"SELECT status, schedule_to_close, clock_timestamp() AS srv_now "  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert rec is not None
            # Future scheduled_at → deferred, not dispatchable yet...
            assert rec["status"] == "scheduled"
            # ...but the deadline is anchored to ENQUEUE time, not to the
            # future scheduled_at (the old batch arm stored srv+1h).
            stc_drift = (rec["schedule_to_close"] - rec["srv_now"]).total_seconds()
            assert abs(stc_drift) < 1.0, (
                f"batch stc anchored {stc_drift:+.0f}s off enqueue time "
                f"(old scheduled_at anchoring would read ~+3600s)"
            )

        # Sweep 2 fails the job before any dispatch can occur.
        swept = await backend.deadline_sweep()
        assert swept == 1
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_class == "DeadlineExceeded"

        worker_id = await _seed_dispatch_targets(pg_dsn, schema, actors=["a"])
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert dispatched == []
    finally:
        await stack.aclose()


# ── C11: enqueue-time result TTL stamped by the server clock ─────────────


@_integration
async def test_result_ttl_anchored_to_server_clock(pg_dsn: str) -> None:
    """Enqueue with ``result_ttl=60 s`` while the Python clock is +300 s
    ahead.  Pre-fix: ``result_expires_at = python_now + 60 s`` → results
    live 300 s longer than configured (and the server-side TTL sweep
    honours that wrong stamp).  Post-fix: ``clock_timestamp() + 60 s`` on
    the single and batch arms; the COPY arm is already server-stamped by
    the fixup UPDATE (pinned here too)."""
    stack, backend, schema = await _mk_backend(pg_dsn, timedelta(seconds=300))
    try:
        stamp = datetime.now(UTC) + timedelta(seconds=300)
        single = await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=stamp,
                result_ttl=timedelta(seconds=60),
            )
        )
        batch_rows = await backend.enqueue_batch(
            [
                EnqueueArgs(
                    id=new_job_id(),
                    actor="a",
                    queue="default",
                    payload={},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=stamp,
                    result_ttl=timedelta(seconds=60),
                )
            ]
        )
        copy_id = new_job_id()
        await backend.enqueue_batch_fast(
            [
                EnqueueArgs(
                    id=copy_id,
                    actor="a",
                    queue="default",
                    payload={},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=stamp,
                    result_ttl=timedelta(seconds=60),
                )
            ]
        )

        async with _connect(pg_dsn) as conn:
            expected = await server_now(conn) + timedelta(seconds=60)
            for via, expires_at in (
                ("single", single.result_expires_at),
                ("batch", batch_rows[0].result_expires_at),
            ):
                assert expires_at is not None, f"{via} arm: result_expires_at unexpectedly None"
                drift = (expires_at - expected).total_seconds()
                assert abs(drift) < 1.0, (
                    f"{via} arm result TTL drifted {drift:+.1f}s off the server anchor"
                )
            copy_expires = await conn.fetchval(
                f'SELECT result_expires_at FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                copy_id,
            )
            assert copy_expires is not None
            drift = (copy_expires - expected).total_seconds()
            assert abs(drift) < 1.0, (
                f"copy arm result TTL drifted {drift:+.1f}s off the server anchor"
            )
    finally:
        await stack.aclose()


# ── D12: the sweeps judge cut-offs at STATEMENT time, not transaction start ──


@_integration
async def test_sweeps_judge_cutoffs_at_statement_time_not_transaction_start(
    clean_jobs_app: JobsApp,
) -> None:
    """Every sweep predicate must read ``clock_timestamp()``, never ``now()``.

    The sweeps are a public surface: ``PostgresBackend.sweep_*`` take a
    caller-supplied ``ConnLike`` and open a *nested* transaction on it, so a
    caller (or an embedder running maintenance alongside its own work) can
    have the transaction already open and several statements old.  ``now()``
    is fixed at transaction START, so under it every cut-off below is judged
    against a stale instant and the rows that fell due *during* the
    transaction are silently skipped: leases stay unreclaimed, deadlines
    stay unenforced, and scheduled jobs never promote to pending.

    All three rows are made due 2 s after the transaction opened and the
    sweeps run 3 s later on that same connection — so ``clock_timestamp()``
    sees them due and ``now()`` (pinned to BEGIN) does not.  This is the
    statement-vs-transaction axis; the rest of this module covers the
    orthogonal server-vs-skewed-app-clock axis, and neither substitutes for
    the other.
    """
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn, conn.transaction():
        _worker_id, locked_job = await setup_running_job(conn, schema)
        deadline_job = await create_pending_job(conn, schema)
        promote_job = await create_pending_job(conn, schema, status="scheduled")
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
            "SET lock_expires_at = CASE WHEN id = $1 THEN clock_timestamp() + interval '2 seconds' END, "
            "    schedule_to_close = CASE WHEN id = $2 THEN clock_timestamp() + interval '2 seconds' END, "
            "    scheduled_at = CASE WHEN id = $3 THEN clock_timestamp() + interval '2 seconds' "
            "                        ELSE scheduled_at END "
            "WHERE id = ANY($4::uuid[])",
            locked_job,
            deadline_job,
            promote_job,
            [locked_job, deadline_job, promote_job],
        )
        await conn.execute("SELECT pg_sleep(3)")

        reclaimed = await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)
        deadlined = await PostgresBackend.sweep_deadline_exceeded(conn, schema=schema)
        promoted = await PostgresBackend.sweep_scheduled_to_pending(conn, schema=schema)

        statuses = {
            rec["id"]: rec["status"]
            for rec in await conn.fetch(
                f'SELECT id, status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema derived from settings validated against _IDENT_RE.
                [locked_job, deadline_job, promote_job],
            )
        }

    assert reclaimed == 1, "expired lease not reclaimed — sweep 1 judged the lease at txn start"
    assert deadlined == 1, "passed deadline not failed — sweep 2 judged the deadline at txn start"
    assert promoted == 1, "due job not promoted — sweep 3 judged scheduled_at at txn start"
    assert statuses[locked_job] == "pending"
    assert statuses[deadline_job] == "failed"
    assert statuses[promote_job] == "pending"
