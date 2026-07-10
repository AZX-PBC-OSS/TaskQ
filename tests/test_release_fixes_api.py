"""Regression tests for the API/web/worker-startup release-audit fixes.

Covers (see task audit items, API-layer subset):

1. ``TaskQ.create_schedule`` forwards ``dst_strategy`` to ``JobsClient``.
3. ``JobsClient`` translates ``asyncpg.exceptions.UndefinedTableError`` into
   ``SchemaNotMigratedError`` on enqueue/get/list/cancel.
4. ``ActorConfigDriftError``/``ActorConfigDriftList`` fold the remedy hint
   into ``__str__``.
6. Admin ``_listen`` NOTIFY queue is bounded and drops the oldest payload
   on overflow (logging once).
9. Worker DI startup validation logs a warning when a sync actor declares
   a LOOP-scoped dependency.
12. ``HealthServer`` only unlinks its own socket at shutdown (inode-guarded),
    and binds past a stale dead socket.
14. ``TaskQSettings.load()`` suppresses dotenvmodel's "No .env files found"
    WARNING and restores the previous logger level afterward.

Unit-level only: no Postgres required. Uses ``InMemoryBackend`` /
``ProviderRegistry`` / direct exception construction as appropriate.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import TaskQ
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq.actor import actor
from taskq.backend.clock import SystemClock
from taskq.client._jobs import JobsClient
from taskq.context import JobContext
from taskq.exceptions import ActorConfigDriftError, ActorConfigDriftList, SchemaNotMigratedError
from taskq.testing.in_memory import InMemoryBackend

# ---------------------------------------------------------------------------
# 1. TaskQ.create_schedule forwards dst_strategy
# ---------------------------------------------------------------------------


class _Payload(BaseModel):
    value: int = 1


@pytest.mark.asyncio
async def test_taskq_create_schedule_forwards_dst_strategy() -> None:
    """TaskQ facade must pass dst_strategy through to JobsClient.create_schedule.

    Regression for: TaskQ.create_schedule silently dropped dst_strategy,
    so callers using the facade always got the "skip" default regardless
    of what they passed, with no error.
    """
    backend = InMemoryBackend(clock=SystemClock())
    client = JobsClient(backend)

    tq = TaskQ(dsn="postgresql://unused/unused")
    tq._client = client  # bypass open(): no real Postgres needed for this unit test

    handle = await tq.create_schedule(
        "some_actor",
        "*/5 * * * *",
        dst_strategy="firstof",
    )
    assert handle.dst_strategy == "firstof"

    records = await tq.list_schedules(actor="some_actor")
    assert len(records) == 1
    assert records[0].dst_strategy == "firstof"


# ---------------------------------------------------------------------------
# 3. SchemaNotMigratedError translation
# ---------------------------------------------------------------------------


def _undefined_table_error() -> asyncpg.exceptions.UndefinedTableError:
    return asyncpg.exceptions.UndefinedTableError('relation "taskq.jobs" does not exist')


@pytest.mark.asyncio
async def test_jobs_client_get_translates_undefined_table_error() -> None:
    backend = AsyncMock()
    backend.get.side_effect = _undefined_table_error()
    client = JobsClient(backend)

    with pytest.raises(SchemaNotMigratedError) as excinfo:
        await client.get(object())  # type: ignore[arg-type]  # Why: job_id value is irrelevant; backend.get is stubbed to raise unconditionally.

    assert "taskq migrate up" in str(excinfo.value)
    assert "TASKQ_MIGRATE_ON_START" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, asyncpg.exceptions.UndefinedTableError)


@pytest.mark.asyncio
async def test_jobs_client_list_translates_undefined_table_error() -> None:
    from taskq.backend._protocol import JobFilter

    backend = AsyncMock()
    backend.list_jobs.side_effect = _undefined_table_error()
    client = JobsClient(backend)

    with pytest.raises(SchemaNotMigratedError):
        await client.list(JobFilter(limit=10))


@pytest.mark.asyncio
async def test_jobs_client_cancel_translates_undefined_table_error() -> None:
    backend = AsyncMock()
    backend.get.side_effect = _undefined_table_error()
    client = JobsClient(backend)

    with pytest.raises(SchemaNotMigratedError):
        await client.cancel(object())  # type: ignore[arg-type]  # Why: job_id value is irrelevant; backend.get is stubbed to raise unconditionally.


@pytest.mark.asyncio
async def test_jobs_client_enqueue_translates_undefined_table_error() -> None:
    backend = AsyncMock()
    backend.enqueue.side_effect = _undefined_table_error()
    client = JobsClient(backend)

    @actor(name="schema_error_actor")
    async def _act(_payload: _Payload) -> None:
        pass

    with pytest.raises(SchemaNotMigratedError):
        await client.enqueue(_act, _Payload())


# ---------------------------------------------------------------------------
# 4. ActorConfigDriftError / ActorConfigDriftList hint folding
# ---------------------------------------------------------------------------


def test_actor_config_drift_error_exposes_remedy_hint() -> None:
    """ActorConfigDriftError is only ever raised wrapped in
    ActorConfigDriftList (see worker/startup.py) — its own __str__ stays a
    plain per-actor diff line so ActorConfigDriftList doesn't double-print
    the hint once per drift. It still exposes the hint via .hint so
    standalone callers/tests can access it directly."""
    err = ActorConfigDriftError("my_actor", "max_concurrent", 4, 8)
    assert "force-update-actor-config" in err.hint
    assert "TASKQ_FORCE_UPDATE_ACTOR_CONFIG" in err.hint


def test_actor_config_drift_list_str_includes_remedy_hint_once() -> None:
    drifts = (ActorConfigDriftError("a", "max_concurrent", 1, 2),)
    err = ActorConfigDriftList(drifts)
    text = str(err)
    assert text.count("force-update-actor-config") == 1


# ---------------------------------------------------------------------------
# 6. Admin _listen bounded queue with drop-oldest + single warning
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_listen_notify_callback_drops_oldest_on_overflow(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Why capsys, not caplog: taskq's structlog logger renders to stdout via
    # PrintLogger unless taskq.obs.setup_logging() has bridged it into
    # stdlib logging, which this unit test does not (and should not) set up.
    from taskq.web.admin._listen import _QUEUE_MAXSIZE, _make_notify_callback

    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=3)
    cb = _make_notify_callback(queue, "test_channel")

    for i in range(5):
        cb(None, 0, "test_channel", f"payload-{i}")  # type: ignore[arg-type]  # Why: connection arg unused by the callback body.

    assert queue.qsize() == 3
    # Oldest two payloads (0, 1) were dropped; the three most recent remain.
    remaining = [queue.get_nowait() for _ in range(3)]
    assert remaining == ["payload-2", "payload-3", "payload-4"]

    out = capsys.readouterr().out
    assert out.count("listen-queue-overflow-drop-oldest") == 1  # logged once, not once per drop
    assert _QUEUE_MAXSIZE == 1000  # documented default cap


# ---------------------------------------------------------------------------
# 9. Sync actor + LOOP-scoped dependency startup warning
# ---------------------------------------------------------------------------


class _LoopDep:
    pass


@pytest.mark.asyncio
async def test_sync_actor_with_loop_scoped_dep_logs_warning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    registry = ProviderRegistry()
    registry.register_value(_LoopDep, Scope.LOOP, _LoopDep())

    # Why plain `def` (no `async def`): ActorRef.is_sync is auto-detected
    # from `not inspect.iscoroutinefunction(fn)` — there is no explicit
    # is_sync kwarg on the @actor decorator.
    @actor(name="sync_actor_with_loop_dep")
    def _sync_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: _LoopDep,
    ) -> None: ...

    assert _sync_actor.is_sync is True

    registry.validate(actors=[_sync_actor])  # type: ignore[list-item]  # Why: ActorRef[Any, Any] erasure boundary, same as production call sites.

    out = capsys.readouterr().out
    assert "sync_actor_loop_scoped_dependency" in out


# ---------------------------------------------------------------------------
# 12. HealthServer inode-guarded unlink
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_server_stop_skips_unlink_when_socket_replaced(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A slow-shutting-down worker must not delete a replacement worker's
    freshly bound socket at the same path (the shutdown-race bug)."""
    from taskq.worker.health import HealthServer

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "taskq_health.sock")

        srv = HealthServer()
        srv._socket_path = sock_path  # pyright: ignore[reportPrivateUsage]  # Why: test seam — exercising stop()'s unlink guard without a full asyncio.start_unix_server bind.

        # Simulate: server bound, captured inode 111 at bind time...
        srv._socket_inode = 111  # pyright: ignore[reportPrivateUsage]

        # ...but by the time stop() runs, a replacement worker has already
        # unlinked and rebound the same path (different inode).
        replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        replacement.bind(sock_path)
        try:
            await srv.stop()
            assert os.path.exists(sock_path), (  # noqa: ASYNC240  # Why: test assertion on already-completed I/O; not a blocking call in an async hot path.
                "stop() must not unlink a path it no longer owns"
            )
            out = capsys.readouterr().out
            assert "health-server-stop-skipped-unlink" in out
        finally:
            replacement.close()


@pytest.mark.asyncio
async def test_health_server_stop_unlinks_own_socket() -> None:
    from taskq.worker.health import HealthServer

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "taskq_health.sock")

        srv = HealthServer()
        srv._socket_path = sock_path  # pyright: ignore[reportPrivateUsage]

        owned = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        owned.bind(sock_path)
        try:
            srv._socket_inode = os.stat(sock_path).st_ino  # pyright: ignore[reportPrivateUsage]
        finally:
            owned.close()

        await srv.stop()
        assert not os.path.exists(sock_path)  # noqa: ASYNC240  # Why: test assertion, not a blocking call in an async hot path.


def test_unlink_stale_socket_removes_dead_socket() -> None:
    from taskq.worker.health import _unlink_stale_socket

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "dead.sock")
        dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        dead.bind(sock_path)
        dead.close()  # bound then closed without unlinking -> stale path on disk

        assert os.path.exists(sock_path)
        _unlink_stale_socket(sock_path)
        assert not os.path.exists(sock_path)


def test_unlink_stale_socket_leaves_live_socket_alone() -> None:
    from taskq.worker.health import _unlink_stale_socket

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "live.sock")
        live = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        live.bind(sock_path)
        live.listen(1)
        try:
            _unlink_stale_socket(sock_path)
            assert os.path.exists(sock_path)
        finally:
            live.close()


# ---------------------------------------------------------------------------
# 14. dotenvmodel ".env not found" warning suppression
# ---------------------------------------------------------------------------


def test_taskq_settings_load_suppresses_dotenv_warning(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from taskq.settings import TaskQSettings

    monkeypatch.chdir(tmp_path)  # empty dir: guarantees "No .env files found"
    monkeypatch.setenv("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5432/taskq")

    dotenv_logger = logging.getLogger("dotenvmodel")
    original_level = dotenv_logger.level
    try:
        with caplog.at_level(logging.WARNING, logger="dotenvmodel"):
            TaskQSettings.load()
        assert not any("No .env files found" in r.message for r in caplog.records)
    finally:
        assert dotenv_logger.level == original_level, "logger level must be restored"
