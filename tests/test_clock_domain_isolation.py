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
from taskq.testing.pg import create_worker, seed_actors
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
