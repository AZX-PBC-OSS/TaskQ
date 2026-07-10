"""Tests for the jobs_app fixture (PG integration).

enqueue to dispatch to terminal round-trip.

Sanity: jobs_app constructs WorkerDeps successfully against the
PG container; the schema is created and migrations applied; the
backend is type-compatible with Backend.

DSN narrowing: assert deps.settings.pg_dsn_direct is not None.
"""

from datetime import timedelta

import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import _open_pg_backend
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration


# ── Sanity: fixture reaches yield cleanly ─────────────────────────────


async def test_jobs_app_yields_deps_and_backend(
    jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Sanity: jobs_app constructs WorkerDeps and PostgresBackend
    against the PG container; schema is created and migrations applied.
    """
    deps, backend = jobs_app
    assert deps is not None
    assert isinstance(backend, PostgresBackend)


async def test_jobs_app_backend_satisfies_protocol(
    jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Sanity: PostgresBackend is type-compatible with Backend."""
    _deps, backend = jobs_app
    assert isinstance(backend, Backend)


async def test_jobs_app_dsn_narrowing(
    jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """DSN narrowing pattern: deps.settings.pg_dsn_direct is not None
    after the fixture's explicit assertion.
    """
    deps, _backend = jobs_app
    assert deps.settings.pg_dsn_direct is not None


# ── Regression: _open_pg_backend cleans up pools on constructor failure ──


async def test_open_pg_backend_cleans_up_on_constructor_failure(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ``PostgresBackend.__init__`` raises after ``WorkerDeps`` has
    been entered into the ``AsyncExitStack``, the stack must be closed
    so asyncpg pools are not leaked.

    We monkeypatch PostgresBackend.__init__ to raise, and wrap
    ``open_worker_deps`` (via its source module) with a tracked version
    so we can observe that ``__aexit__`` is called (proving pool cleanup).
    """
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager

    from taskq.worker.deps import open_worker_deps

    aexit_called = False
    real_open = open_worker_deps

    @asynccontextmanager
    async def _tracked_open(
        settings: WorkerSettings,
    ) -> AsyncGenerator[WorkerDeps, None]:
        """Wraps the real ``open_worker_deps``; sets aexit_called on exit."""
        nonlocal aexit_called
        async with real_open(settings) as deps:
            yield deps
        aexit_called = True

    def _failing_init(self: object, *args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated constructor failure")

    # Patch the *source* module so the ``from … import`` inside
    # _open_pg_backend picks up the tracked version each call.
    monkeypatch.setattr("taskq.worker.deps.open_worker_deps", _tracked_open)
    monkeypatch.setattr(PostgresBackend, "__init__", _failing_init)

    with pytest.raises(RuntimeError, match="simulated constructor failure"):
        await _open_pg_backend(pg_dsn, schema_name=f"tja_{new_base62()}".lower())

    assert aexit_called, "WorkerDeps.__aexit__ was not called — pool leak!"


# ── round-trip ────────────────────────────────────────────────


async def test_jobs_app_enqueue_dispatch_round_trip(
    jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """enqueue → dispatch → terminal round-trip."""
    from datetime import UTC, datetime

    _deps, backend = jobs_app
    job_id = new_job_id()
    worker_id = new_uuid()
    now = datetime(2025, 1, 1, tzinfo=UTC)
    schema = _deps.settings.schema_name

    # Register a worker row so FK constraints on job_attempts are satisfied.
    async with backend._worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608 # Why: schema_name is validated against _IDENT_RE during settings load; asyncpg has no parameter binding for identifiers.
            worker_id,
            "test-host",
            1234,
            ["default"],
        )

    # Enqueue
    row = await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={"x": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=now,
        )
    )
    assert row.status == "pending"

    # Dispatch
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    assert dispatched[0].status == "running"

    # Mark succeeded — reuse the same worker_id that acquired the lock
    ok = await backend.mark_succeeded(job_id, worker_id, {"result": True})
    assert ok is True

    # Get attempts
    attempts = await backend.get_attempts(job_id)
    assert len(attempts) >= 1
