# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: the serialization/payload/result boundary against REAL jsonb.

The load-bearing tier for the values where asyncpg's client codecs and
PG's ``jsonb_in`` decide -- not orjson alone. Hunts:

* an actor exception whose message/traceback carries a lone surrogate
  (``os.fsdecode`` of a non-UTF-8 byte) reaching the terminal write's
  ``text`` params: asyncpg raises DataError -- a PostgresError subclass
  -- so ``_TERMINAL_WRITE_INFRA_EXCEPTIONS`` misreads a PERMANENT data
  defect as transient infra and the job strands running in the reclaim
  loop (the exact failure mode the NUL guard exists to prevent, one
  layer up).
* NaN/Infinity results through real jsonb (orjson emits null -- standard
  JSON, accepted; pinned green).
* the enqueue boundary's typed refusals (NUL ValueError parity with the
  in-memory mirror, surrogate TypeError, over-deep TypeError) and its
  deliberate content-agnosticism (malformed payloads land verbatim;
  dispatch-time ``validate_actor_payload`` owns payload validation).
* the progress_state COALESCE-merge with adversarial type changes
  (jsonb ``||`` last-writer-wins per top-level key).
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import NamedTuple
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq._json import NUL_JSONB_ERROR, dumps, loads, sanitize_nul_str
from taskq.backend._protocol import ErrorInfo
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.pg import create_workered_running_job
from taskq.worker._handlers import (
    _TERMINAL_WRITE_INFRA_EXCEPTIONS,  # pyright: ignore[reportPrivateUsage]  # Why: the infra-classification tuple IS the defect under test; importing it is the only way to pin the misclassification.
)

pytestmark = pytest.mark.integration

_SURROGATE = "\udcff"
"""A lone low surrogate: legal Python str, impossible to UTF-8 encode --
exactly what ``os.fsdecode(b'\\xff')`` yields on a surrogateescape fs."""


class _PgEnv(NamedTuple):
    schema: str
    pool: asyncpg.Pool
    backend: PostgresBackend


class _PgDeps:
    """Duck-typed BackendDeps: settings + the shared pool on every pool
    slot (the cron-harness pattern -- PostgresBackend resolves pools
    lazily through these attributes)."""

    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool


@pytest.fixture(scope="module")
async def pg_env(pg_dsn: str) -> AsyncIterator[_PgEnv]:
    """A migrated ``tpb_*`` schema on the module's dedicated database,
    with a pool-backed PostgresBackend; dropped CASCADE on teardown."""
    schema = f"tpb_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    try:
        settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema}
        )
        backend = PostgresBackend(
            _PgDeps(settings, pool),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; PostgresBackend reads only settings + the three pool attributes set here.
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
        yield _PgEnv(schema=schema, pool=pool, backend=backend)
    finally:
        await pool.close()
        sweep = await asyncpg.connect(pg_dsn)
        try:
            await sweep.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await sweep.close()


async def _seed_actor(env: _PgEnv) -> None:
    async with env.pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{env.schema}".actor_config (actor, queue, max_attempts, retry_kind) '
            "VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (actor) DO UPDATE SET queue = EXCLUDED.queue",
            "test_actor",
            "default",
            5,
            "transient",
        )


async def _seed_running_job(env: _PgEnv) -> tuple[UUID, UUID]:
    """A worker row + a running job row owned by it: the exact state the
    terminal writes fence on."""
    async with env.pool.acquire() as conn:
        return await create_workered_running_job(conn, env.schema)


# ── Defect: surrogate exception text strands the terminal write ─────────


def test_asyncpg_data_error_is_classified_as_terminal_write_infra() -> None:
    """Unit pin of the misclassification mechanism (no PG needed): the
    error asyncpg raises for an unencodable text argument IS a
    PostgresError subclass, so the terminal-write infra tuple catches it
    and a permanent data defect is treated as transient -- the job is
    left running for the reclaim loop."""
    probe = asyncpg.exceptions.DataError("invalid input for query argument $1")
    assert isinstance(probe, asyncpg.PostgresError), (
        "fact: asyncpg raises DataError (a PostgresError) for unencodable text params"
    )
    assert isinstance(probe, _TERMINAL_WRITE_INFRA_EXCEPTIONS), (
        "defect: a permanent unencodable-value error matches the transient-infra "
        "classification tuple, so the terminal write is retried forever instead of failing"
    )


async def test_error_message_with_lone_surrogate_must_not_strand(pg_env: _PgEnv) -> None:
    """RED: the terminal failure write for an actor whose exception
    message/traceback carries a lone surrogate (built EXACTLY as
    ``_handle_generic_exception`` builds it: sanitize_nul_str(str(exc)))
    must LAND -- job failed, defect visible -- because asyncpg instead
    raises DataError, which the infra classification swallows: the job
    strands 'running' and every lease-sweep reclaim re-runs the actor's
    committed side effects against the same unencodable text forever."""
    worker_id, job_id = await _seed_running_job(pg_env)
    await _seed_actor(pg_env)
    error = ErrorInfo(
        error_class="ValueError",
        error_message=sanitize_nul_str(f"cannot open file {_SURROGATE}"),
        error_traceback=sanitize_nul_str(
            f"Traceback (most recent call last):\nValueError: cannot open file {_SURROGATE}"
        ),
    )

    row = await pg_env.backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=1)
    assert row.status == "failed", (
        "contract: an unencodable exception message must fail the job with the defect "
        "visible (escaped/sanitized), never strand it running"
    )

    async with pg_env.pool.acquire() as conn:
        stored = await conn.fetchrow(
            f'SELECT status, error_message FROM "{pg_env.schema}".jobs WHERE id = $1',
            job_id,
        )
    assert stored is not None
    assert stored["status"] == "failed"
    assert str(stored["error_message"]), "contract: the stored message keeps the defect diagnosable"


async def test_error_message_with_nul_sanitized_lands_failed(pg_env: _PgEnv) -> None:
    """GREEN pin isolating the gap: the NUL half of the derived-text
    guard DOES land on PG (sanitize_nul_str replaces NUL with the visible
    escape), so the surrogate half is the missing sibling, not a broken
    guard family."""
    worker_id, job_id = await _seed_running_job(pg_env)
    await _seed_actor(pg_env)
    error = ErrorInfo(
        error_class="ValueError",
        error_message=sanitize_nul_str("bad\x00value"),
        error_traceback=sanitize_nul_str("bad\x00trace"),
    )

    row = await pg_env.backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=1)
    assert row.status == "failed"

    async with pg_env.pool.acquire() as conn:
        stored = await conn.fetchrow(
            f'SELECT error_message FROM "{pg_env.schema}".jobs WHERE id = $1', job_id
        )
    assert stored is not None
    assert "\\x00" in str(stored["error_message"]), (
        f"contract: NUL is stored as the visible \\x00 escape; got {stored['error_message']!r}"
    )


# ── NaN / Infinity through real jsonb ───────────────────────────────────


async def test_nan_result_stores_null_and_succeeds(pg_env: _PgEnv) -> None:
    """GREEN: an actor result containing NaN/Infinity serializes to
    STANDARD JSON null (orjson), which real jsonb accepts -- the job
    succeeds and the stored result shows null. No non-standard token
    ever reaches PG, so no 22P02/22P03 rejection is possible."""
    worker_id, job_id = await _seed_running_job(pg_env)
    await _seed_actor(pg_env)

    ok = await pg_env.backend.mark_succeeded(
        job_id,
        worker_id,
        result={"ratio": float("nan"), "inf": float("inf"), "ok": True},
        attempt=1,
    )
    assert ok, "contract: a NaN-bearing result is storable (as null), not a failure"

    async with pg_env.pool.acquire() as conn:
        stored = await conn.fetchrow(
            f'SELECT status, result, result_size_bytes FROM "{pg_env.schema}".jobs WHERE id = $1',
            job_id,
        )
    assert stored is not None
    assert stored["status"] == "succeeded"
    decoded = loads(str(stored["result"]).encode())
    assert decoded == {"ratio": None, "inf": None, "ok": True}, (
        f"contract: NaN/Infinity store as jsonb null; got {decoded!r}"
    )
    assert stored["result_size_bytes"] == len(dumps({"ratio": None, "inf": None, "ok": True})), (
        "contract: result_size_bytes is the stored document's byte length"
    )


# ── The enqueue boundary: typed refusals, content-agnostic storage ──────


async def _enqueue_job_count(env: _PgEnv, args_id: UUID) -> int:
    async with env.pool.acquire() as conn:
        return (
            await conn.fetchval(f'SELECT count(*) FROM "{env.schema}".jobs WHERE id = $1', args_id)
            or 0
        )


async def test_enqueue_nul_payload_raises_canonical_value_error(pg_env: _PgEnv) -> None:
    """GREEN parity pin: a NUL in the payload is refused by the PG-side
    jsonb guard with the byte-identical ValueError the in-memory mirror
    raises -- nothing reaches the database."""
    await _seed_actor(pg_env)
    args = make_enqueue_args(payload={"note": "a\x00b"})
    with pytest.raises(ValueError, match=re.escape(NUL_JSONB_ERROR)):
        await pg_env.backend.enqueue(args)
    assert await _enqueue_job_count(pg_env, args.id) == 0, (
        "contract: a NUL-refused enqueue leaves no row"
    )


async def test_enqueue_surrogate_payload_raises_type_error(pg_env: _PgEnv) -> None:
    """GREEN: a lone surrogate in the payload is refused at the enqueue
    boundary with orjson's TypeError (the same fail-fast contract as the
    #134 non-str-keys break) -- never a DataError mid-INSERT."""
    await _seed_actor(pg_env)
    args = make_enqueue_args(payload={"m": _SURROGATE})
    with pytest.raises(TypeError, match="surrogates not allowed"):
        await pg_env.backend.enqueue(args)
    assert await _enqueue_job_count(pg_env, args.id) == 0, (
        "contract: a surrogate-refused enqueue leaves no row"
    )


async def test_enqueue_overdeep_payload_raises_type_error(pg_env: _PgEnv) -> None:
    """GREEN: over-deep nesting is refused at the enqueue boundary with
    a typed TypeError (orjson's recursion limit), never a server-side
    stack overflow or a half-written batch."""
    await _seed_actor(pg_env)
    node: object = None
    for _ in range(5000):
        node = {"a": node}
    payload: dict[str, object] = {"deep": node}  # pyright: ignore[reportUnknownVariableType,reportAssignmentType]  # Why: the loop deliberately builds object-typed nesting; the wrapper is dict[str, object] by construction.
    args = make_enqueue_args(payload=payload)
    with pytest.raises(TypeError, match="Recursion limit reached"):
        await pg_env.backend.enqueue(args)
    assert await _enqueue_job_count(pg_env, args.id) == 0, (
        "contract: an over-deep refused enqueue leaves no row"
    )


async def test_enqueue_stores_malformed_payload_verbatim(pg_env: _PgEnv) -> None:
    """GREEN architecture pin: PG-side enqueue performs NO payload
    content validation -- wrong types and extra keys land verbatim, and
    dispatch-time ``validate_actor_payload`` (PayloadValidationError,
    non-retryable -- pinned in the runner suite) owns the payload
    boundary. This pins WHERE the boundary lives, not that it is missing."""
    await _seed_actor(pg_env)
    hostile = {"value": "not-an-int", "extra_unexpected": True}
    args = make_enqueue_args(payload=hostile)
    row = await pg_env.backend.enqueue(args)
    assert row.payload == hostile, (
        f"contract: enqueue is content-agnostic; dispatch-time validation owns the boundary; stored {row.payload!r}"
    )


# ── progress_state COALESCE-merge with adversarial type changes ─────────


async def test_progress_state_merge_last_writer_wins_on_type_change(pg_env: _PgEnv) -> None:
    """GREEN: jsonb ``||`` merge is last-writer-wins per top-level key
    with NO coercion -- ``{"step": 1}`` then ``{"step": "one"}`` stores
    the string, and the in-memory mirror's ``dict |`` semantics agree
    exactly."""
    worker_id, job_id = await _seed_running_job(pg_env)
    await _seed_actor(pg_env)
    async with pg_env.pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{pg_env.schema}".jobs SET progress_state = \'{{"step": 1, "keep": true}}\'::jsonb '
            "WHERE id = $1",
            job_id,
        )

    ok = await pg_env.backend.mark_succeeded(
        job_id,
        worker_id,
        result={"done": True},
        progress_seq=1,
        progress_state={"step": "one"},
        attempt=1,
    )
    assert ok

    async with pg_env.pool.acquire() as conn:
        merged = await conn.fetchval(
            f'SELECT progress_state FROM "{pg_env.schema}".jobs WHERE id = $1', job_id
        )
    decoded = loads(str(merged).encode())
    assert decoded == {"keep": True, "step": "one"}, (
        f"contract: jsonb || replaces the value (and its type) wholesale per key; got {decoded!r}"
    )
    assert decoded == {"step": 1, "keep": True} | {"step": "one"}, (
        "contract: the in-memory mirror's dict-merge semantics match jsonb || exactly"
    )
