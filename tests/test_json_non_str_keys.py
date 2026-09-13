"""Non-``str`` dict keys and empty ``result_bytes`` boundary contracts.

``OPT_NON_STR_KEYS`` was dropped from :func:`taskq._json.dumps` for speed on
str-keyed input; these tests pin the resulting fail-fast contract at every
boundary that serializes a RAW (unvalidated) caller dict, plus the
empty-``result_bytes`` ValueError both terminal implementations must raise
(decoded empty bytes bind as ``''``, which ``jsonb`` rejects with a
``PostgresError`` — a permanent data defect the terminal-write
classification would otherwise read as transient infrastructure failure).

Pydantic-validated payloads are unaffected: ``dict[str, ...]`` model fields
reject non-str keys at validation, so they never reach these boundaries.
"""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel, ValidationError

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, IdentityKey
from taskq.backend._sql_templates import render as render_sql
from taskq.backend._terminal import _mark_succeeded_on_conn
from taskq.context import JobContext
from taskq.progress._buffer import _ProgressBuffer
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import _encode_result

_SQL = render_sql("taskq")
_START = datetime(2025, 1, 1, tzinfo=UTC)
_LEASE = timedelta(seconds=60)


class _FakeConn:
    """Minimal ConnLike stand-in; never reached when the guard fires."""

    async def fetchrow(self, query: object, *args: object) -> dict[str, object]:
        raise AssertionError("terminal write must reject before touching the connection")

    async def execute(self, query: str, *args: object) -> str:
        raise AssertionError("terminal write must reject before touching the connection")


class _FakeSettings:
    progress_data_max_bytes = 10_000


def _make_ctx(job_id: object, buffer: _ProgressBuffer) -> JobContext[BaseModel]:
    return JobContext(
        job_id=job_id,
        actor="a",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=None,  # type: ignore[arg-type]
        jobs=None,  # type: ignore[arg-type]
        log=None,  # type: ignore[arg-type]
        _progress_buffers={job_id: buffer},  # type: ignore[dict-item]
        _redis_client=None,
        _worker_settings=_FakeSettings(),  # type: ignore[arg-type]
        _pending_publish_tasks=None,
    )


# ── dumps: the serialization primitive ─────────────────────────────────


def test_dumps_rejects_top_level_int_key() -> None:
    from taskq import _json

    with pytest.raises(TypeError):
        _json.dumps({1: "x"})


def test_dumps_rejects_nested_int_key() -> None:
    from taskq import _json

    with pytest.raises(TypeError):
        _json.dumps({"a": {2: "x"}})


def test_dumps_str_keys_still_serialize() -> None:
    from taskq import _json

    assert _json.loads(_json.dumps({"1": "x"})) == {"1": "x"}


# ── consumer result boundary ───────────────────────────────────────────


def test_encode_result_rejects_int_keyed_result() -> None:
    """An actor returning {1: ...} fails the attempt with TypeError where
    OPT_NON_STR_KEYS previously coerced to {'1': ...}."""
    with pytest.raises(TypeError):
        _encode_result({1: "x"})


def test_encode_result_rejects_nested_int_key() -> None:
    with pytest.raises(TypeError):
        _encode_result({"a": {2: "x"}})


# ── ctx.progress boundary (size check dumps the raw dict) ──────────────


async def test_ctx_progress_rejects_int_keyed_data() -> None:
    job_id = new_job_id()
    buffer = _ProgressBuffer(job_id=job_id, base_seq=0)
    ctx = _make_ctx(job_id, buffer)
    with pytest.raises(TypeError):
        await ctx.progress(data={1: "x"})
    assert "data" not in buffer.pending_state, "rejected data must not enter the buffer"


async def test_ctx_progress_rejects_nested_int_key() -> None:
    job_id = new_job_id()
    buffer = _ProgressBuffer(job_id=job_id, base_seq=0)
    ctx = _make_ctx(job_id, buffer)
    with pytest.raises(TypeError):
        await ctx.progress(data={"a": {2: "x"}})


# ── empty result_bytes: same ValueError class from both terminals ──────


async def test_pg_terminal_rejects_empty_result_bytes() -> None:
    with pytest.raises(ValueError, match="result_bytes must be non-empty"):
        await _mark_succeeded_on_conn(
            _FakeConn(),
            _SQL,
            new_job_id(),
            new_uuid(),
            result_bytes=b"",
        )


async def test_testing_terminal_rejects_empty_result_bytes() -> None:
    backend = InMemoryBackend(clock=FakeClock(_START))
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={},
        identity_key=IdentityKey("empty-bytes-guard"),
    )
    await backend.enqueue(args)
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, _LEASE)
    assert len(claimed) == 1
    with pytest.raises(ValueError, match="result_bytes must be non-empty"):
        await backend.mark_succeeded(claimed[0].id, worker_id, result_bytes=b"")


# ── the pydantic-validated payload path stays unreachable ──────────────


def test_pydantic_dict_field_rejects_int_keys() -> None:
    """Documents WHY payload/metadata routed through models are safe under
    the flag removal: pydantic rejects non-str keys at validation instead
    of coercing them."""

    class P(BaseModel):
        d: dict[str, object]

    with pytest.raises(ValidationError):
        P.model_validate({"d": {1: "x"}})
