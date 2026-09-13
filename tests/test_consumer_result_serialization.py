"""Consumer-level single-serialization contract for the result payload.

``_encode_result`` serializes the actor's result ONCE and both consumer
paths (autonomous and transactional) pass those bytes into
``mark_succeeded`` / ``mark_succeeded_with_conn`` via the explicit
``result_bytes`` parameter so the terminal write never re-serializes.
"""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import dumps as _json_dumps
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import ResultTooLarge
from taskq.testing.actor import EmptyPayload, FakeBackend, as_backend, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import _encode_result, consume_one_job

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


class _CountingDumps:
    """Counts the consumer's own result serializations."""

    def __init__(self) -> None:
        self.calls: list[object] = []

    def __call__(self, value: object) -> bytes:
        self.calls.append(value)
        return _json_dumps(value)


class _FakeConnection:
    """Minimal asyncpg.Connection stand-in with a transaction() context manager."""

    class _Transaction:
        async def __aenter__(self) -> "_FakeConnection._Transaction":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    def transaction(self) -> "_FakeConnection._Transaction":
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _TxBackend(FakeBackend):
    """FakeBackend tracking mark_succeeded_with_conn calls."""

    def __init__(self) -> None:
        super().__init__()
        self.mark_succeeded_with_conn_calls: list[
            tuple[object, UUID, UUID, dict[str, object] | None, bytes | None]
        ] = []

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: UUID,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: object = None,
        *,
        result_bytes: bytes | None = None,
    ) -> bool:
        self.mark_succeeded_with_conn_calls.append((conn, job_id, worker_id, result, result_bytes))
        return await self.mark_succeeded(
            job_id, worker_id, result, progress_seq, progress_state, result_bytes=result_bytes
        )


# ── Autonomous path ────────────────────────────────────────────────────


async def test_autonomous_success_serializes_result_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One ``dumps`` call in the consumer per successful completion, and its
    bytes — not a dict — are what reach ``mark_succeeded``."""
    counter = _CountingDumps()
    monkeypatch.setattr("taskq.worker._consumer._json_dumps", counter)
    backend = FakeBackend()

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"value": 42}

    await consume_one_job(
        as_backend(backend),
        make_job_row(),
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
    )

    assert len(counter.calls) == 1
    assert len(backend.mark_succeeded_calls) == 1
    _job_id, _worker_id, recorded_result, recorded_bytes = backend.mark_succeeded_calls[0]
    assert recorded_result is None
    assert recorded_bytes == _json_dumps({"value": 42})


async def test_autonomous_none_result_no_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-dict actor return stores no result and serializes nothing."""
    counter = _CountingDumps()
    monkeypatch.setattr("taskq.worker._consumer._json_dumps", counter)
    backend = FakeBackend()

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        return "not a result dict"

    await consume_one_job(
        as_backend(backend),
        make_job_row(),
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
    )

    assert counter.calls == []
    assert len(backend.mark_succeeded_calls) == 1
    assert backend.mark_succeeded_calls[0][2] is None
    assert backend.mark_succeeded_calls[0][3] is None


async def test_autonomous_nul_result_still_reaches_backend_unraised() -> None:
    """The NUL guard stays at the terminal write (``dumps_jsonb_str`` /
    the byte-level scan), not in the consumer: with a recording backend the
    NUL payload passes through as bytes, exactly as a plain dict did before."""
    backend = FakeBackend()

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"k": "a\x00b"}

    await consume_one_job(
        as_backend(backend),
        make_job_row(),
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
    )

    recorded_bytes = backend.mark_succeeded_calls[0][3]
    assert recorded_bytes is not None
    assert b"\\u0000" in recorded_bytes


async def test_autonomous_cap_exceeded_raises_result_too_large_in_encode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ResultTooLarge`` is still raised from the consumer's encode helper
    (same site), before ``mark_succeeded`` — routed to failure, not success."""
    monkeypatch.setattr("taskq.worker._consumer._json_dumps", _CountingDumps())
    backend = FakeBackend()
    raised_from: list[str] = []
    original = _encode_result

    def _spy(result: object, max_bytes: int = 65536) -> bytes | None:
        try:
            return original(result, max_bytes)
        except ResultTooLarge:
            raised_from.append("_encode_result")
            raise

    monkeypatch.setattr("taskq.worker._consumer._encode_result", _spy)

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"blob": "x" * 66560}

    outcome = await consume_one_job(
        as_backend(backend),
        make_job_row(),
        _WORKER_ID,
        run_actor=actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
    )

    assert outcome == "failed"
    assert raised_from == ["_encode_result"]
    assert len(backend.mark_succeeded_calls) == 0


# ── Transactional path ─────────────────────────────────────────────────


async def test_transactional_success_serializes_result_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same single-serialization contract on the transactional path."""
    counter = _CountingDumps()
    monkeypatch.setattr("taskq.worker._consumer._json_dumps", counter)
    backend = _TxBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"value": 7}

    await asyncio.shield(
        consume_one_job(
            as_backend(backend),
            make_job_row(),
            _WORKER_ID,
            run_actor=actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            enqueuer=enqueuer,
            transaction_conn=_FakeConnection(),
        )
    )

    assert len(counter.calls) == 1
    assert len(backend.mark_succeeded_with_conn_calls) == 1
    _conn, _job_id, _worker_id, recorded_result, recorded_bytes = (
        backend.mark_succeeded_with_conn_calls[0]
    )
    assert recorded_result is None
    assert recorded_bytes == _json_dumps({"value": 7})


# ── Unit: _encode_result returns the single serialization ──────────────


def test_encode_result_returns_bytes() -> None:
    """The helper serializes once; size semantics unchanged: len(bytes)."""
    payload: dict[str, object] = {"a": 1}
    data = _encode_result(payload)
    assert data == _json_dumps(payload)
    assert len(data) == len(_json_dumps(payload))


def test_encode_result_base_model_dumps_model_json() -> None:
    class _Result(BaseModel):
        ok: bool = True

    data = _encode_result(_Result())
    assert data == _json_dumps({"ok": True})


def test_encode_result_non_storable_returns_none() -> None:
    assert _encode_result(None) is None
    assert _encode_result("not a result dict") is None


def test_encode_result_over_cap_raises() -> None:
    with pytest.raises(ResultTooLarge, match="bytes exceeds"):
        _encode_result({"blob": "x" * 66560})


def test_encode_result_custom_cap() -> None:
    with pytest.raises(ResultTooLarge, match="bytes exceeds 10 byte cap"):
        _encode_result({"a": "bbbbbbbbbbbbbb"}, 10)
