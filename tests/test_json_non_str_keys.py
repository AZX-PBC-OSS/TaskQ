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
from uuid import UUID

import pytest
from pydantic import BaseModel, ValidationError

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import (
    AttemptRow,
    EnqueueArgs,
    IdentityKey,
    ScheduleCreateArgs,
    ScheduleUpdateArgs,
)
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
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")
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


# ── mirror value normalization: PG's jsonb read-back shapes ────────────
#
# The rejection contracts above pin what both backends refuse; these pin
# what they READ BACK. PG persists every caller dict through jsonb (the
# orjson text at bind time, loads at read), so a value whose orjson
# encoding differs from the Python object comes back morphed — NaN and
# Infinity as null, UUIDs as their string form, tuples as arrays. The
# mirror must read back the same shapes or a test author sees different
# results under the two backends for the same call.


@pytest.mark.parametrize(
    ("stored_value", "pg_read_back"),
    [
        (float("nan"), None),
        (float("inf"), None),
        (
            UUID("12345678-1234-5678-1234-567812345678"),
            "12345678-1234-5678-1234-567812345678",
        ),
        ((1, 2), [1, 2]),
    ],
    ids=["nan-nulls", "inf-nulls", "uuid-stringifies", "tuple-becomes-array"],
)
async def test_in_memory_enqueue_stores_pg_jsonb_round_trip_values(
    stored_value: object,
    pg_read_back: object,
) -> None:
    """The mirror's enqueue stores the round-trip of the same
    serialization its bind-time guard already runs (the one PG binds), so
    payload and metadata read back exactly as PG's jsonb column reads
    them."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"v": stored_value},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"m": stored_value},
    )

    await backend.enqueue(args)

    row = await backend.get(args.id)
    assert row is not None
    assert row.payload == {"v": pg_read_back}, (
        "payload must read back PG's jsonb round-trip shape "
        f"(expected {{'v': {pg_read_back!r}}}, got {row.payload!r})"
    )
    assert row.metadata == {"m": pg_read_back}


async def test_in_memory_terminal_progress_merge_stores_pg_jsonb_round_trip_values() -> None:
    """The terminal writes' progress merge stores the round-trip of the
    same serialization PG's ``COALESCE(progress_state,'{}') || new``
    binds — actor-supplied progress values read back exactly as PG reads
    them."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={},
    )
    await backend.enqueue(args)
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, _LEASE)
    assert len(claimed) == 1

    ok = await backend.mark_succeeded(
        claimed[0].id,
        worker_id,
        result={"done": True},
        progress_state={
            "v": UUID("12345678-1234-5678-1234-567812345678"),
            "n": float("nan"),
        },
        attempt=1,
    )
    assert ok is True

    row = await backend.get(claimed[0].id)
    assert row is not None
    assert row.progress_state == {"v": "12345678-1234-5678-1234-567812345678", "n": None}


async def test_in_memory_snooze_metadata_update_stores_pg_jsonb_round_trip_values() -> None:
    """``mark_snoozed``'s ``metadata_update`` binds as ``jsonb`` on PG
    (``metadata = j.metadata || COALESCE(metadata_update, ...)``), so its
    values read back JSON-round-tripped — UUID → string, NaN/Infinity →
    null, tuple → array — exactly like every other metadata write path
    above. The merge must store that same round-trip, not the caller's
    Python objects, or the one snooze path reads back differently from
    production."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={},
    )
    await backend.enqueue(args)
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, _LEASE)
    assert len(claimed) == 1

    ok = await backend.mark_snoozed(
        claimed[0].id,
        worker_id,
        timedelta(seconds=5),
        metadata_update={
            "v": UUID("12345678-1234-5678-1234-567812345678"),
            "n": float("nan"),
            "t": (1, 2),
        },
        progress_state={
            "v": UUID("12345678-1234-5678-1234-567812345678"),
            "n": float("nan"),
        },
        attempt=1,
    )
    assert ok == "scheduled"

    row = await backend.get(claimed[0].id)
    assert row is not None
    assert row.metadata == {
        "v": "12345678-1234-5678-1234-567812345678",
        "n": None,
        "t": [1, 2],
    }, (
        "the snooze metadata merge must read back PG's jsonb round-trip "
        f"shape (got {row.metadata!r})"
    )
    assert row.progress_state == {"v": "12345678-1234-5678-1234-567812345678", "n": None}, (
        "the snooze progress merge must read back PG's jsonb round-trip "
        f"shape (got {row.progress_state!r})"
    )


async def test_in_memory_snooze_metadata_update_with_nul_raises_value_error() -> None:
    """The round-trip the snooze merge now runs is also its NUL guard:
    PG's jsonb_param rejects a NUL value before the UPDATE fires, and the
    mirror must reject at the same boundary with the row untouched.
    Before the round-trip landed here, a NUL value was stored silently —
    a result PG never could hold."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={},
    )
    await backend.enqueue(args)
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, _LEASE)
    assert len(claimed) == 1

    with pytest.raises(ValueError, match="NUL"):
        await backend.mark_snoozed(
            claimed[0].id,
            worker_id,
            timedelta(seconds=5),
            metadata_update={"k": "a\x00b"},
            attempt=1,
        )

    row = await backend.get(claimed[0].id)
    assert row is not None
    assert row.status == "running"


async def test_in_memory_schedule_metadata_with_nul_raises_value_error() -> None:
    """Schedule metadata routes through the same guarded serialization on
    both backends: PG binds it via jsonb_param (NUL → ValueError before
    the INSERT/UPDATE fires), so the mirror's round-trip must reject at
    the same boundary, create and update alike, leaving storage
    untouched."""
    backend = InMemoryBackend(clock=FakeClock(_START))

    with pytest.raises(ValueError, match="NUL"):
        await backend.create_schedule(
            ScheduleCreateArgs(
                actor="test_actor",
                cron_expr="* * * * *",
                timezone="UTC",
                next_fire_at=_START,
                metadata={"k": "a\x00b"},
            )
        )
    assert await backend.list_schedules() == []

    record = await backend.create_schedule(
        ScheduleCreateArgs(
            actor="test_actor",
            cron_expr="* * * * *",
            timezone="UTC",
            next_fire_at=_START,
            metadata={"k": "clean"},
        )
    )
    with pytest.raises(ValueError, match="NUL"):
        await backend.update_schedule(record.id, ScheduleUpdateArgs(metadata={"k": "a\x00b"}))

    listed = await backend.list_schedules()
    assert len(listed) == 1
    assert listed[0].metadata == {"k": "clean"}


async def test_in_memory_attempt_metadata_with_nul_raises_value_error() -> None:
    """Attempt metadata binds through jsonb_param on PG (NUL → ValueError
    before the INSERT), so the mirror's round-trip must reject at the
    same boundary and store nothing."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={},
    )
    await backend.enqueue(args)
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, _LEASE)
    assert len(claimed) == 1

    attempt = AttemptRow(
        job_id=claimed[0].id,
        attempt=1,
        started_at=_START,
        finished_at=_START,
        outcome="succeeded",
        error_class=None,
        error_message=None,
        error_traceback=None,
        duration_ms=1,
        worker_id=worker_id,
        metadata={"k": "a\x00b"},
    )
    with pytest.raises(ValueError, match="NUL"):
        await backend.write_attempt(attempt)

    assert await backend.get_attempts(claimed[0].id) == []


async def test_in_memory_schedule_metadata_stores_pg_jsonb_round_trip_values() -> None:
    """Schedule metadata persists through the same jsonb round-trip on
    both backends — create and update alike."""
    backend = InMemoryBackend(clock=FakeClock(_START))

    record = await backend.create_schedule(
        ScheduleCreateArgs(
            actor="test_actor",
            cron_expr="* * * * *",
            timezone="UTC",
            next_fire_at=_START,
            metadata={"v": UUID("12345678-1234-5678-1234-567812345678")},
        )
    )
    assert record.metadata == {"v": "12345678-1234-5678-1234-567812345678"}

    updated = await backend.update_schedule(
        record.id,
        ScheduleUpdateArgs(metadata={"w": (1, 2)}),
    )
    assert updated.metadata == {"w": [1, 2]}


async def test_in_memory_attempt_metadata_stores_pg_jsonb_round_trip_values() -> None:
    """``write_attempt`` is a Backend-protocol method a direct caller can
    reach with any metadata dict; PG binds it through the NUL-guarded
    serialization and reads it back round-tripped, so the mirror must
    store the same round-trip — not the caller's Python objects."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id = new_job_id()

    await backend.write_attempt(
        AttemptRow(
            job_id=job_id,
            attempt=1,
            started_at=_START,
            finished_at=_START + timedelta(seconds=5),
            outcome="failed",
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
            duration_ms=5000,
            worker_id=new_uuid(),
            metadata={"v": UUID("12345678-1234-5678-1234-567812345678"), "t": (1, 2)},
        )
    )

    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].metadata == {
        "v": "12345678-1234-5678-1234-567812345678",
        "t": [1, 2],
    }
