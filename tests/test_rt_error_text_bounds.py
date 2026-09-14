"""Red-team: job error text is unbounded on the failure terminal-write paths.

Hypothesis: ``error_message`` / ``error_traceback`` produced when a job
fails are stored unbounded. Evidence for the gap: the ``jobs`` /
``job_attempts`` ``error_class`` / ``error_message`` / ``error_traceback``
columns are unbounded ``text``
(``src/taskq/migrations/01.00.00_01_pre_initial.sql:99-101``); neither
``src/taskq/worker/_handlers.py`` (``ErrorInfo`` construction only
sanitizes NUL, never truncates) nor ``src/taskq/backend/_terminal.py``
(``_mark_failed`` / ``_mark_retry`` bind ``ErrorInfo`` fields verbatim,
unlike the ``MAX_RESULT_BYTES`` / ``ResultTooLarge`` cap on the success
path) bounds them. Upstream precedent: Que truncates errors to 500/10k
chars in SQL with CHECK constraints, and GoodJob hit
``PG::ProgramLimitExceeded`` from large error payloads.

Attack: fail a job with a 1MB exception message + 1MB traceback through
the real terminal-write path (``PostgresBackend.mark_failed_or_retry``
on real PG, the same call ``_handle_generic_exception`` funnels into)
and assert the stored text is bounded (<= 100_000 chars — the point is
boundedness; the failure output shows the unbounded reality).
"""

# ruff: noqa: S608 Why: schema name is validated by WorkerSettings.post_load and _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches existing integration test pattern

from __future__ import annotations

from datetime import timedelta

import asyncpg
import pytest

from taskq.backend._protocol import ErrorInfo
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_workered_running_job

from .test_rt_cron_harness import cron_settings, pool_backend, seed_actor_config

_HUGE_CHARS = 1_000_000
"""1MB of hostile error text — well within PG's 1GB ``text`` limit, so the
write lands and the stored length proves boundedness instead of
erroring at the server."""


def _huge_error() -> ErrorInfo:
    """An ``ErrorInfo`` shaped like a hostile failure: 1MB of class,
    message, and traceback each — every field over its bound."""
    return ErrorInfo(
        error_class="C" * _HUGE_CHARS,
        error_message="M" * _HUGE_CHARS,
        error_traceback="T" * _HUGE_CHARS,
    )


@pytest.mark.integration
async def test_mark_failed_truncates_1mb_error_text(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Attacks ``_mark_failed``: a 1MB failure must not land verbatim in ``jobs`` + ``job_attempts``."""
    schema = module_pg_schema.schema_name
    settings = cron_settings(schema)
    await seed_actor_config(clean_pg_conn, schema, "test_actor")
    worker_id, job_id = await create_workered_running_job(clean_pg_conn, schema)
    backend = pool_backend(settings, module_pg_pool)

    row = await backend.mark_failed_or_retry(job_id, worker_id, _huge_error(), None)
    assert row.status == "failed"

    stored = await clean_pg_conn.fetchrow(
        f'SELECT error_class, error_message, error_traceback FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert stored is not None
    assert len(str(stored["error_class"])) == 500, (
        f"error_class truncates to its 500-char bound; got len={len(str(stored['error_class']))}"
    )
    assert len(str(stored["error_message"])) == 10_000, (
        "error_message truncates to its 10_000-char bound; "
        f"got len={len(str(stored['error_message']))}"
    )
    assert len(str(stored["error_traceback"])) == 100_000, (
        "error_traceback truncates to its 100_000-char bound; "
        f"got len={len(str(stored['error_traceback']))}"
    )

    attempt = await clean_pg_conn.fetchrow(
        f'SELECT error_message, error_traceback FROM "{schema}".job_attempts WHERE job_id = $1',
        job_id,
    )
    assert attempt is not None
    assert len(str(attempt["error_message"])) == 10_000
    assert len(str(attempt["error_traceback"])) == 100_000


@pytest.mark.integration
async def test_mark_retry_truncates_1mb_error_text(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Attacks ``_mark_retry``: the retry branch must not store 1MB error text verbatim either."""
    schema = module_pg_schema.schema_name
    settings = cron_settings(schema)
    await seed_actor_config(clean_pg_conn, schema, "test_actor")
    worker_id, job_id = await create_workered_running_job(clean_pg_conn, schema)
    backend = pool_backend(settings, module_pg_pool)

    row = await backend.mark_failed_or_retry(job_id, worker_id, _huge_error(), timedelta(seconds=1))
    assert row.status == "scheduled"

    stored = await clean_pg_conn.fetchrow(
        f'SELECT error_message, error_traceback FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert stored is not None
    assert len(str(stored["error_message"])) == 10_000
    assert len(str(stored["error_traceback"])) == 100_000

    attempt = await clean_pg_conn.fetchrow(
        f'SELECT error_message, error_traceback FROM "{schema}".job_attempts WHERE job_id = $1',
        job_id,
    )
    assert attempt is not None
    assert len(str(attempt["error_message"])) == 10_000
    assert len(str(attempt["error_traceback"])) == 100_000


def test_error_info_truncates_oversized_values_at_construction() -> None:
    """``ErrorInfo`` truncates every oversized field at construction."""
    error = _huge_error()
    assert len(error.error_class) == 500, (
        f"ErrorInfo.error_class must truncate to 500 chars; got len={len(error.error_class)}"
    )
    assert len(error.error_message) == 10_000, (
        f"ErrorInfo.error_message must truncate to 10_000 chars; got len={len(error.error_message)}"
    )
    assert error.error_traceback is not None
    assert len(error.error_traceback) == 100_000, (
        f"ErrorInfo.error_traceback must truncate to 100_000 chars; got len={len(error.error_traceback)}"
    )


def test_error_info_rejects_nul_pin() -> None:
    """Green pin: the existing NUL guard raises — proves this harness exercises the real ``ErrorInfo``."""
    with pytest.raises(ValueError):
        ErrorInfo(
            error_class="ValueError",
            error_message="bad\x00value",
            error_traceback=None,
        )
