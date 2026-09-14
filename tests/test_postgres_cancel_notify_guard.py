"""Unit tests for PostgresBackend.write_cancel_request's post-commit NOTIFY guard.

The cancel flag commits in the transaction block; the pg_notify fan-out
fires afterwards on a second pooled connection. A NOTIFY failure there
must NOT surface to the caller as a failed cancel — the request IS
recorded — so it is swallowed with a warning, the same contract as
``cancel_where``'s notify block. These tests pin that contract at the
connection-mock level (no real PG needed); the happy-path SQL behaviour
lives in the integration tests (test_postgres_cancel_paths.py).
"""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, Mock

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend

_GRACE = timedelta(seconds=30)

_NOTIFY_SQL_FRAGMENT = "pg_notify"


def _running_record() -> dict[str, object]:
    return {"prev_status": None, "locked_by_worker": new_uuid()}


def _mock_pool(conn: Mock) -> Mock:
    """A pool stub whose acquire() hands back *conn* on every checkout."""

    def _acm() -> MagicMock:
        acm = MagicMock()
        acm.__aenter__ = AsyncMock(return_value=conn)
        acm.__aexit__ = AsyncMock(return_value=False)
        return acm

    pool = Mock()
    pool.acquire = Mock(side_effect=_acm)
    return pool


def _scripted_conn(notify_error: Exception) -> Mock:
    """A conn stub for write_cancel_request's running-job path.

    fetchrow: no pending/scheduled row, then a running row (drives the
    cancel_phase=1 branch). execute: succeeds for the in-transaction
    cancel_request event INSERT, raises *notify_error* for the
    post-commit pg_notify round trip.
    """
    conn = Mock()
    conn.fetchrow = AsyncMock(side_effect=[None, _running_record()])

    async def scripted_execute(sql: str, *args: object) -> object:
        if _NOTIFY_SQL_FRAGMENT in sql:
            raise notify_error
        return "OK"

    conn.execute = AsyncMock(side_effect=scripted_execute)
    conn.transaction = MagicMock()
    return conn


def _make_backend(worker_pool: Mock) -> PostgresBackend:
    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.worker_pool = worker_pool
    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0
    return PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


class TestWriteCancelRequestNotifyGuard:
    async def test_notify_failure_does_not_fail_a_committed_cancel(self) -> None:
        """A pg_notify failure after the cancel flag committed is swallowed
        with a warning: write_cancel_request still returns True.
        """
        conn = _scripted_conn(asyncpg.PostgresConnectionError("notify channel down"))
        backend = _make_backend(_mock_pool(conn))
        job_id = new_job_id()

        result = await backend.write_cancel_request(job_id, "user request")

        assert result is True

    @pytest.mark.parametrize(
        "notify_error",
        [
            asyncpg.PostgresConnectionError("notify channel down"),
            OSError("socket gone"),
            RuntimeError("pool closed"),
        ],
    )
    async def test_notify_failure_is_arbitrary_exception(self, notify_error: Exception) -> None:
        """The guard is except-Exception (credential/provider errors are not
        asyncpg-shaped), mirroring cancel_where's notify guard.
        """
        conn = _scripted_conn(notify_error)
        backend = _make_backend(_mock_pool(conn))

        assert await backend.write_cancel_request(new_job_id(), None) is True

    async def test_notify_failure_logs_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The swallow is a WARN (not silent) with the job id on the event —
        the heartbeat poll remains authoritative for signal delivery.
        """
        import taskq.backend.postgres as postgres_mod

        mock_logger = Mock()
        monkeypatch.setattr(postgres_mod, "logger", mock_logger)

        conn = _scripted_conn(RuntimeError("pool closed"))
        backend = _make_backend(_mock_pool(conn))
        job_id = new_job_id()

        result = await backend.write_cancel_request(job_id, "user request")

        assert result is True
        mock_logger.warning.assert_called_once()
        call = mock_logger.warning.call_args
        assert call.args[0] == "cancel-request-notify-failed"
        assert call.kwargs["job_id"] == str(job_id)

    async def test_notify_success_path_unguarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On success no warning is logged (guard is failure-only)."""
        import taskq.backend.postgres as postgres_mod

        mock_logger = Mock()
        monkeypatch.setattr(postgres_mod, "logger", mock_logger)

        conn = Mock()
        conn.fetchrow = AsyncMock(side_effect=[None, _running_record()])
        conn.execute = AsyncMock()
        conn.transaction = MagicMock()
        backend = _make_backend(_mock_pool(conn))

        result = await backend.write_cancel_request(new_job_id(), "user request")

        assert result is True
        mock_logger.warning.assert_not_called()
