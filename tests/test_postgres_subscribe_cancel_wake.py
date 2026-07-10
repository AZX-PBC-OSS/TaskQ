"""Integration tests for PostgresBackend subscribe_cancel_wake and subscribe_wake.

Happy-path behavior tests for the asyncio.Event-based cancel wake and wake
subscribe/unsubscribe mechanisms.

Covers:
- subscribe_cancel_wake context manager mechanics (fresh event, enter/exit)
- Cancel wake event firing via notify callback (exact production code path)
- Event cleanup on exit (no stale subscribers)
- Multiple cancel subscribers receive cancel wake (fan-out)
- No cancel wake for pending-job cancels (only running jobs trigger NOTIFY)
- subscribe_wake context manager mechanics (fresh event, enter/exit)
- Wake event firing on enqueue (full PG enqueue → NOTIFY → callback round-trip)
- Event cleanup on exit for wake
- Multiple wake subscribers receive wake (fan-out)

note::

    Cancel-wake behavioral tests call the notify callbacks directly
    (``_make_events_callback`` / ``_make_worker_events_callback``) rather
    than going through ``write_cancel_request`` → ``pg_notify`` on the
    per-worker channel. The per-worker channel name exceeds PostgreSQL's
    63-char identifier limit when using the ``module_pg_schema`` fixture
    (schema name is derived from the module path). The callbacks are the
    exact same production code path — the only skipped step is asyncpg's
    NOTIFY delivery, which is exercised by the dedicated
    ``test_cancel_notify_integration.py`` under a short schema.
"""

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import asyncpg
import orjson
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.constants import events_channel, wake_channel, worker_channel
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_pending_job
from taskq.worker.notify import (
    _make_callback,
    _make_events_callback,
    _make_worker_events_callback,
)

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend

pytestmark = pytest.mark.integration


# ── Helpers ────────────────────────────────────────────────────────────


def _cancel_payload(job_id: str, worker_id: str) -> str:
    """Build a JSON cancel payload matching the production ``pg_notify`` format."""
    return orjson.dumps({"type": "cancel", "job_id": job_id, "worker_id": worker_id}).decode()


def _make_enqueue_args(
    *,
    job_id: object | None = None,
    actor: str = "test_actor",
    queue: str = "default",
    scheduled_at: datetime | None = None,
) -> EnqueueArgs:
    """Create an :class:`EnqueueArgs` with sensible defaults."""
    return EnqueueArgs(
        id=job_id or new_job_id(),
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=scheduled_at or datetime.now(UTC),
    )


async def _setup_wake_listener(
    deps: object,
    backend: "PostgresBackend",
) -> tuple[str, object]:
    """Register the wake callback on ``deps.notify_conn``.

    ``deps.notify_conn`` already has ``LISTEN`` on the wake channel
    (issued by ``open_worker_deps``). Returns ``(channel, callback)``
    for cleanup via :func:`_teardown_listener`.
    """
    schema: str = deps.settings.schema_name  # type: ignore[union-attr]
    conn: asyncpg.Connection = deps.notify_conn  # type: ignore[union-attr]
    assert conn is not None, "notify_conn must be open"

    w_ch = wake_channel(schema)
    w_cb = _make_callback(backend)
    await conn.add_listener(w_ch, w_cb)
    return (w_ch, w_cb)


async def _teardown_listener(deps: object, channel: str, callback: object) -> None:
    """Remove a listener callback from ``deps.notify_conn`` (best-effort)."""
    conn = deps.notify_conn  # type: ignore[union-attr]
    if conn is None:
        return
    with contextlib.suppress(Exception):
        await conn.remove_listener(channel, callback)


# ── subscribe_cancel_wake tests ────────────────────────────────────────


class TestSubscribeCancelWake:
    """Happy-path tests for :meth:`PostgresBackend.subscribe_cancel_wake`."""

    async def test_context_manager_yields_fresh_unset_event(self, clean_jobs_app: JobsApp) -> None:
        """Enter subscribe_cancel_wake; verify the yielded event is a fresh,
        unset :class:`asyncio.Event`.
        """
        backend = clean_jobs_app.backend

        async with backend.subscribe_cancel_wake() as event:
            assert not event.is_set(), "fresh event must not be set on enter"

    async def test_cancel_wake_fires_via_events_callback_on_running_job(
        self,
        module_pg_schema: ModulePgSchema,
        clean_jobs_app: JobsApp,
    ) -> None:
        """Call ``_make_events_callback`` with a matching cancel payload.
        The callback (same production code path) sets all cancel subscriber
        events.
        """
        backend = clean_jobs_app.backend
        worker_id = new_uuid()

        # The events callback filters on worker_id — create one for our worker.
        events_cb = _make_events_callback(backend, worker_id)

        async with backend.subscribe_cancel_wake() as cancel_event:
            payload = _cancel_payload(str(new_job_id()), str(worker_id))
            events_cb(
                _mock_asyncpg_conn(),
                0,
                events_channel(module_pg_schema.schema_name),
                payload,
            )
            await asyncio.wait_for(cancel_event.wait(), timeout=2.0)

        assert cancel_event.is_set(), "cancel wake event must be set by events callback"

    async def test_cancel_wake_fires_via_worker_callback(
        self,
        module_pg_schema: ModulePgSchema,
        clean_jobs_app: JobsApp,
    ) -> None:
        """Call ``_make_worker_events_callback`` with a cancel payload.
        The per-worker callback fires cancel subscribers unconditionally for
        any cancel-type payload (no worker_id filtering).
        """
        backend = clean_jobs_app.backend

        worker_cb = _make_worker_events_callback(backend)

        async with backend.subscribe_cancel_wake() as cancel_event:
            payload = _cancel_payload(str(new_job_id()), str(new_uuid()))
            worker_cb(
                _mock_asyncpg_conn(),
                0,
                worker_channel(module_pg_schema.schema_name, str(new_uuid())),
                payload,
            )
            await asyncio.wait_for(cancel_event.wait(), timeout=2.0)

        assert cancel_event.is_set(), "cancel wake event must be set by worker callback"

    async def test_event_removed_from_subscribers_on_exit(self, clean_jobs_app: JobsApp) -> None:
        """Enter, then exit subscribe_cancel_wake. The event must be removed
        from the cancel subscriber set.
        """
        backend = clean_jobs_app.backend

        async with backend.subscribe_cancel_wake() as event:
            pass

        assert event not in backend._cancel_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only assertion verifying cleanup

    async def test_multiple_subscribers_all_receive_cancel_wake(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Two concurrent subscribe_cancel_wake contexts: both events must be
        set when the worker callback fires (fan-out).
        """
        backend = clean_jobs_app.backend

        worker_cb = _make_worker_events_callback(backend)

        async with (
            backend.subscribe_cancel_wake() as event_a,
            backend.subscribe_cancel_wake() as event_b,
        ):
            payload = _cancel_payload(str(new_job_id()), str(new_uuid()))
            worker_cb(_mock_asyncpg_conn(), 0, "ch", payload)
            await asyncio.wait_for(asyncio.gather(event_a.wait(), event_b.wait()), timeout=2.0)
            assert event_a.is_set(), "first cancel subscriber must be set"
            assert event_b.is_set(), "second cancel subscriber must be set"

    async def test_cancel_wake_not_set_for_pending_job_cancel(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Cancelling a pending job (case 1 — immediate terminal) does NOT
        fire a cancel notify. The cancel wake event must remain unset as
        there is no callback invocation.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema: str = deps.settings.schema_name  # type: ignore[union-attr]

        async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
            job_id = await create_pending_job(conn, schema)

        async with backend.subscribe_cancel_wake() as cancel_event:
            result = await backend.write_cancel_request(job_id, "pending cancel")
            assert result is True, "pending job cancel must return True"
            # No PG NOTIFY is sent for pending jobs, so the event stays unset.
            await asyncio.sleep(0.1)
            assert not cancel_event.is_set(), "cancel wake must NOT fire for pending-job cancel"


# ── subscribe_wake tests ───────────────────────────────────────────────


class TestSubscribeWake:
    """Happy-path tests for :meth:`PostgresBackend.subscribe_wake`."""

    async def test_context_manager_yields_fresh_unset_event(self, clean_jobs_app: JobsApp) -> None:
        """Enter subscribe_wake; verify the yielded event is a fresh, unset
        class:`asyncio.Event`.
        """
        backend = clean_jobs_app.backend

        async with backend.subscribe_wake() as event:
            assert not event.is_set(), "fresh wake event must not be set on enter"

    async def test_wake_fires_after_enqueue(self, clean_jobs_app: JobsApp) -> None:
        """Subscribe to wake, enqueue a job. The enqueue NOTIFY triggers the
        wake callback which sets the event. Full round-trip test.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend

        ch_cb = await _setup_wake_listener(deps, backend)
        try:
            async with backend.subscribe_wake() as wake_event:
                await backend.enqueue(_make_enqueue_args())
                await asyncio.wait_for(wake_event.wait(), timeout=2.0)
            assert wake_event.is_set(), "wake event must be set after enqueue"
        finally:
            await _teardown_listener(deps, *ch_cb)

    async def test_event_removed_from_subscribers_on_exit(self, clean_jobs_app: JobsApp) -> None:
        """Enter, then exit subscribe_wake. The event must be removed from
        the wake subscriber set.
        """
        backend = clean_jobs_app.backend

        async with backend.subscribe_wake() as event:
            pass

        assert event not in backend._wake_subscribers  # type: ignore[reportPrivateUsage] # Why: test-only assertion verifying cleanup

    async def test_multiple_subscribers_all_receive_wake(self, clean_jobs_app: JobsApp) -> None:
        """Two concurrent subscribe_wake contexts: both events must be set
        when enqueue fires (fan-out). Full round-trip test.
        """
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend

        ch_cb = await _setup_wake_listener(deps, backend)
        try:
            async with (
                backend.subscribe_wake() as event_a,
                backend.subscribe_wake() as event_b,
            ):
                await backend.enqueue(_make_enqueue_args())
                await asyncio.wait_for(asyncio.gather(event_a.wait(), event_b.wait()), timeout=2.0)
                assert event_a.is_set(), "first wake subscriber must be set"
                assert event_b.is_set(), "second wake subscriber must be set"
        finally:
            await _teardown_listener(deps, *ch_cb)


# ── Mock asyncpg connection (for callback invocation) ──────────────────


def _mock_asyncpg_conn() -> asyncpg.Connection:
    """Return a lightweight stand-in for ``asyncpg.Connection``.

    The notify callbacks receive a connection as their first argument but
    do not use it — they only iterate subscriber sets. Returning a
    ``Mock`` avoids constructing a real asyncpg connection for callback-
    only tests.
    """
    from unittest.mock import Mock

    return Mock(spec=asyncpg.Connection)
