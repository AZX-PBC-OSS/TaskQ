"""Tests for the PostgresBackend stub.

Covers:
- Importing PostgresBackend succeeds (version guard does not fire)
- Static type compatibility: takes_backend(PostgresBackend(...)) type-checks
- BACKEND_PROTOCOL_VERSION accessible from the postgres module equals 2
- Stub can be constructed with Mock deps without a live PG
"""

from contextlib import AbstractAsyncContextManager as AsyncContextManager
from datetime import timedelta
from unittest.mock import Mock

from taskq.backend import Backend
from taskq.backend.clock import Clock
from taskq.backend.postgres import BACKEND_PROTOCOL_VERSION, PostgresBackend

# ── Helpers ────────────────────────────────────────────────────────────

_GRACE = timedelta(seconds=30)


def _make_backend() -> PostgresBackend:
    """Construct a PostgresBackend with mock deps (no live PG needed).

    The mock deps must provide ``settings.schema_name`` as a valid string
    and ``worker_pool`` as a Mock (typed as asyncpg.Pool at the annotation
    level; runtime isinstance checks are not enforced so Mocks work).
    """
    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.worker_pool = Mock()
    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0
    return PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


# ── Import / version guard ─────────────────────────────────────────────


class TestImportSucceeds:
    def test_import_does_not_fire_version_guard(self) -> None:
        """If the version guard fired, the import would have raised RuntimeError."""
        from taskq.backend.postgres import PostgresBackend as _

        assert _ is PostgresBackend

    def test_backend_protocol_version_is_two(self) -> None:
        assert BACKEND_PROTOCOL_VERSION == 2

    def test_backend_protocol_version_is_int(self) -> None:
        assert isinstance(BACKEND_PROTOCOL_VERSION, int)


# ── Construction ───────────────────────────────────────────────────────


class TestConstruction:
    def test_constructs_with_mock_deps(self) -> None:
        backend = _make_backend()
        assert isinstance(backend, PostgresBackend)

    def test_stores_grace_periods(self) -> None:
        cancel_grace = timedelta(seconds=10)
        cleanup_grace = timedelta(seconds=20)
        mock_deps = Mock()
        mock_deps.settings.schema_name = "taskq_test"
        mock_deps.worker_pool = Mock()
        backend = PostgresBackend(
            deps=mock_deps,
            clock=Mock(spec=Clock),
            cancellation_grace_period=cancel_grace,
            cleanup_grace_period=cleanup_grace,
        )
        assert backend._cancellation_grace_period == cancel_grace  # type: ignore[reportPrivateUsage] # Why: test-only private access
        assert backend._cleanup_grace_period == cleanup_grace  # type: ignore[reportPrivateUsage] # Why: test-only private access

    def test_stores_clock(self) -> None:
        mock_clock = Mock(spec=Clock)
        mock_deps = Mock()
        mock_deps.settings.schema_name = "taskq_test"
        mock_deps.worker_pool = Mock()
        backend = PostgresBackend(
            deps=mock_deps,
            clock=mock_clock,
            cancellation_grace_period=_GRACE,
            cleanup_grace_period=_GRACE,
        )
        assert backend._clock is mock_clock  # type: ignore[reportPrivateUsage] # Why: test-only private access


# ── Static type compatibility ──────────────────────────────────────────


class TestStaticTypeCompatibility:
    def test_takes_backend_accepts_postgres_backend(self) -> None:
        """PostgresBackend satisfies the Backend protocol structurally.

        pyright verifies this call site — no ``# type: ignore`` needed.
        """

        def takes_backend(b: Backend) -> None:
            pass

        takes_backend(_make_backend())

    def test_isinstance_backend_is_true(self) -> None:
        """Backend is @runtime_checkable, so isinstance should work
        on a concrete implementation that matches the protocol."""
        backend = _make_backend()
        assert isinstance(backend, Backend)


# ── Snooze / retry status CASE determinism ──────────────────────────────


class TestSnoozeRetryStatusCase:
    """The pending-vs-scheduled CASE must compare the delay parameter
    directly against ``interval '0'`` rather than calling
    ``clock_timestamp()`` twice — two separate calls are non-deterministic
    for ``delay=0`` and could misclassify a zero-delay snooze as
    ``scheduled`` instead of ``pending``.
    """

    def _sqls(self) -> tuple[str, str, str]:
        backend = _make_backend()
        sql = backend._sql  # type: ignore[reportPrivateUsage] # Why: test-only private access to pre-rendered SQL
        return (
            sql.mark_snoozed,
            sql.mark_retry_after_consume_true,
            sql.mark_retry_after_consume_false,
        )

    def test_status_case_compares_delay_to_zero_interval(self) -> None:
        for sql in self._sqls():
            assert "CASE WHEN $3::interval > interval '0'" in sql, (
                f"status CASE must use $3::interval > interval '0', got: {sql[:200]}"
            )

    def test_status_case_has_no_double_clock_timestamp(self) -> None:
        nondeterministic = "clock_timestamp() + (SELECT delay FROM params) > clock_timestamp()"
        for sql in self._sqls():
            assert nondeterministic not in sql, (
                "status CASE must not compare two clock_timestamp() calls"
            )


# ── subscribe_wake is sync ─────────────────────────────────────────────


class TestSubscribeWakeIsSync:
    def test_is_not_coroutine_function(self) -> None:
        import inspect

        assert not inspect.iscoroutinefunction(PostgresBackend.subscribe_wake)

    def test_return_annotation(self) -> None:
        from typing import get_type_hints

        hints = get_type_hints(PostgresBackend.subscribe_wake)
        ret = hints.get("return")
        assert ret is not None
        origin = getattr(ret, "__origin__", None)
        assert origin is AsyncContextManager, (
            f"subscribe_wake return origin should be AsyncContextManager, got {origin}"
        )


# ── SQL discipline ──────────────────────────────────────────────────────

_SQL_KEYWORDS = ("SELECT", "INSERT", "UPDATE", "DELETE", "FROM")


class TestSqlDiscipline:
    def test_no_f_string_sql_with_user_data(self) -> None:
        """No f-string interpolation of user-supplied values in SQL strings.

        The schema identifier IS interpolated via f-strings (validated
        against _IDENT_RE at construction time), but
        user data must use ``$N`` parameter binding only. This test
        checks that no f-string in the source contains both an f-string
        prefix and a SQL keyword, which would indicate runtime value
        interpolation.
        """
        import taskq.backend.postgres as pg_mod

        source_file = pg_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            content = f.read()

        # Grep for f-string SQL: lines with f" or f''' containing SQL keywords
        # This is a heuristic — the definitive check is the schema-validated
        # f-string approach documented in the module header.
        for line in content.splitlines():
            # Only check f-strings (not regular strings which are pre-rendered templates)
            if 'f"' in line or "f'" in line:
                for kw in _SQL_KEYWORDS:
                    if f"{kw} " in line.upper() or f"{kw}\n" in line.upper():
                        # f-string with SQL keyword — only allowed for schema
                        # interpolation, not user data
                        assert (
                            "{s}" in line or "{self._schema_name}" in line or "{schema}" in line
                        ), f"f-string with SQL keyword but no schema variable: {line.strip()}"

    def test_no_future_annotations(self) -> None:
        """no ``from __future__ import annotations``."""
        import taskq.backend.postgres as pg_mod

        source_file = pg_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            for line in f:
                assert "from __future__ import annotations" not in line
