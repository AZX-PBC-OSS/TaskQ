# ruff: noqa: S608  # Why: schema names are generated/validated identifiers, every value is $-bound.
"""ATTACK pins (real PG): the result size gate is a loud boundary, never a
silent truncation.

The data-integrity question: a terminal write whose serialised result
overflows ``result_max_bytes`` must be REJECTED at the boundary - a silent
truncation here would let the terminal write SUCCEED with a corrupted
result, the worst kind of loss (the row says succeeded, the value is
wrong). Pinned at the exact boundary, on a real Postgres, with a 64-byte
cap so every case is reachable:

* just under (cap-1): stored, ``result_size_bytes`` records the full size;
* exactly at (cap): stored, full size - the comparison is strict ``>``, one
  off-by-one to ``>=`` would strand every result at the cap;
* just over (cap+1): :class:`ResultTooLarge` raised, the row untouched
  (still running, ``result`` NULL, no terminal write, no attempt row) -
  the job then dies LOUD, not silently corrupted: the retry classifier
  maps ``ResultTooLarge`` to a non-retryable ``Fail`` whose terminal write
  records ``error_class='ResultTooLarge'``.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import MAX_RESULT_BYTES
from taskq.exceptions import ResultTooLarge
from taskq.retry import Fail, RetryClassifier, RetryPolicy
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_workered_running_job

pytestmark = pytest.mark.integration

_CAP = 64


class _TestBackendSettings:
    schema_name: str
    dispatch_oversample: int = 2
    dispatcher_command_timeout: float = 5.0
    result_max_bytes: int = _CAP
    event_writer_batch_size: int = 100
    event_writer_statement_timeout_ms: float = 5000.0
    event_writer_reduced_batch_divisor: int = 4
    sweep_breaker_failure_threshold: int = 3
    sweep_breaker_window_secs: float = 600.0
    max_pending_lock_timeout_ms: float = 5000.0
    unique_for_lock_timeout_ms: float = 5000.0
    idempotency_lock_timeout_ms: float = 5000.0
    max_retry_backoff: timedelta = timedelta(seconds=60)

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name


class _TestBackendDeps:
    settings: _TestBackendSettings
    worker_pool: asyncpg.Pool
    heartbeat_pool: asyncpg.Pool
    dispatcher_pool: asyncpg.Pool | None = None

    def __init__(self, schema: str, pool: asyncpg.Pool) -> None:
        self.settings = _TestBackendSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


@pytest.fixture()
def make_backend(module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema) -> Any:
    def _make() -> PostgresBackend:
        return PostgresBackend(
            _TestBackendDeps(module_pg_schema.schema_name, module_pg_pool),  # pyright: ignore[reportArgumentType]  # Why: the protocol double mirrors test_admin_audit_trail.py.
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=5),
            cleanup_grace_period=timedelta(seconds=5),
        )

    return _make


def _sized_result(size_target: int) -> bytes:
    """orjson output measuring exactly *size_target* bytes: the blob key
    pads to the byte, asserted below so a format drift fails loudly here
    and not as a confusing off-by-one downstream."""
    import orjson

    data = orjson.dumps({"blob": "x" * (size_target - 11)})
    assert len(data) == size_target, (len(data), size_target)
    return data


class TestResultSizeGateBoundary:
    async def test_just_under_cap_is_stored_in_full(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
        make_backend: Any,
    ) -> None:
        schema = module_pg_schema.schema_name
        worker_id, job_id = await create_workered_running_job(clean_pg_conn, schema)
        backend = make_backend()
        data = _sized_result(_CAP - 1)

        ok = await backend.mark_succeeded(
            job_id, worker_id, result_bytes=data, attempt=1, claim_epoch=1
        )
        assert ok is True
        row = await clean_pg_conn.fetchrow(
            f'SELECT status, result, result_size_bytes FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None
        assert row["status"] == "succeeded"
        assert row["result_size_bytes"] == _CAP - 1
        # The FULL value survived: jsonb normalises the text rendering
        # (PG prints `{"blob": "..."}` with a space, one byte more than
        # the orjson input), so the round-trip proof is dict equality
        # against the exact input, and result_size_bytes is the INPUT
        # byte length - the same number the gate measured at the
        # boundary, recorded without truncation.
        import orjson

        assert row["result"] is not None
        assert orjson.loads(str(row["result"])) == orjson.loads(data)

    async def test_exactly_at_cap_is_stored_in_full(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
        make_backend: Any,
    ) -> None:
        schema = module_pg_schema.schema_name
        worker_id, job_id = await create_workered_running_job(clean_pg_conn, schema)
        backend = make_backend()
        data = _sized_result(_CAP)

        ok = await backend.mark_succeeded(
            job_id, worker_id, result_bytes=data, attempt=1, claim_epoch=1
        )
        assert ok is True
        row = await clean_pg_conn.fetchrow(
            f'SELECT status, result_size_bytes FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None
        assert row["status"] == "succeeded"
        assert row["result_size_bytes"] == _CAP

    async def test_just_over_cap_is_rejected_loud_and_row_untouched(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
        make_backend: Any,
    ) -> None:
        schema = module_pg_schema.schema_name
        worker_id, job_id = await create_workered_running_job(clean_pg_conn, schema)
        backend = make_backend()
        data = _sized_result(_CAP + 1)

        with pytest.raises(ResultTooLarge, match="bytes exceeds"):
            await backend.mark_succeeded(
                job_id, worker_id, result_bytes=data, attempt=1, claim_epoch=1
            )

        # Nothing partial landed: the row is still running, result NULL,
        # no terminal write, no corrupted value, no attempt row.
        row = await clean_pg_conn.fetchrow(
            f'SELECT status, result, result_size_bytes FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None
        assert row["status"] == "running"
        assert row["result"] is None
        assert row["result_size_bytes"] is None
        attempts = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        assert attempts == 0

        # And the failure is loud, not silent: the retry classifier maps
        # ResultTooLarge to a non-retryable Fail (the terminal write that
        # follows records error_class='ResultTooLarge'), so the job ends
        # 'failed' with the cause on the row, never succeeded with a
        # truncated value.
        decision = RetryClassifier.classify(
            RetryPolicy(),
            (),
            ResultTooLarge(f"result size {_CAP + 1} bytes exceeds {_CAP} byte cap"),
            1,
        )
        assert isinstance(decision, Fail)
        assert decision.retryable is False
        assert decision.error_class == "ResultTooLarge"

    async def test_default_cap_constant_unchanged(
        self,
        module_pg_schema: ModulePgSchema,  # Why: keeps the module's fixture shape uniform
    ) -> None:
        """The shipped default is 64 KiB; the boundary pins above shrink
        the cap through settings, this pins the default itself so a
        silent change of the shipped constant shows up here."""
        assert MAX_RESULT_BYTES == 65536
