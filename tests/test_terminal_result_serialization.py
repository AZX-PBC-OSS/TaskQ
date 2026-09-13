"""Single-serialization contract for the success-path result payload.

The consumer serializes an actor's result dict exactly once (orjson →
bytes) and hands the terminal write those bytes through the explicit
``result_bytes`` parameter of ``mark_succeeded`` /
``mark_succeeded_with_conn``.  ``_mark_succeeded_on_conn`` must reuse the
bytes (one NUL scan + one decode) instead of re-serializing a dict and
re-encoding the bound str just to measure ``result_size_bytes``.
"""

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq._json import dumps as _json_dumps
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import JobId
from taskq.backend._records import jsonb_to_dict
from taskq.backend._sql_templates import render as render_sql
from taskq.backend._terminal import _mark_succeeded_on_conn
from taskq.exceptions import ResultTooLarge
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import setup_running_job

_SQL = render_sql("taskq")


class _FakeConn:
    """Minimal ConnLike stand-in recording bound parameters."""

    def __init__(self) -> None:
        self.fetchrow_args: tuple[object, ...] | None = None
        self.executes: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: object, *args: object) -> dict[str, object]:
        self.fetchrow_args = (query, *args)
        return {"attempt": 1, "started_at": None, "finished_at": None}

    async def execute(self, query: str, *args: object) -> str:
        self.executes.append((query, args))
        return ""


class _CountingDumps:
    """Monkeypatch stand-in counting ``taskq._json.dumps`` calls."""

    def __init__(self) -> None:
        self.calls: list[object] = []

    def __call__(self, value: object) -> bytes:
        self.calls.append(value)
        return _json_dumps(value)


# ── Pre-serialized path (consumer-supplied bytes) ──────────────────────


async def test_result_bytes_binds_decoded_str_and_exact_size() -> None:
    """The terminal reuses the held bytes: decoded once for the jsonb
    binding, size taken from len(bytes) — no serialization, no re-encode."""
    payload = {"value": 42, "nested": {"a": [1, 2, 3]}}
    data = _json_dumps(payload)
    conn = _FakeConn()

    ok = await _mark_succeeded_on_conn(
        conn, _SQL, JobId(new_job_id()), new_uuid(), result_bytes=data
    )

    assert ok is True
    assert conn.fetchrow_args is not None
    bound_result = conn.fetchrow_args[3]
    bound_size = conn.fetchrow_args[4]
    assert bound_result == data.decode("utf-8")
    assert bound_size == len(data)


async def test_result_bytes_does_not_serialize_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero ``taskq._json.dumps`` calls inside the terminal write when the
    result arrives pre-serialized.  The fused statement builds the event
    detail server-side (jsonb_build_object), so nothing on this path
    serializes client-side: the result bytes are reused, and a NULL
    progress_state contributes no serialization either."""
    counter = _CountingDumps()
    monkeypatch.setattr("taskq._json.dumps", counter)
    payload = {"ok": True}
    data = _json_dumps(payload)
    conn = _FakeConn()

    await _mark_succeeded_on_conn(conn, _SQL, JobId(new_job_id()), new_uuid(), result_bytes=data)

    assert len(counter.calls) == 0
    assert all(call is not payload for call in counter.calls)


async def test_result_bytes_size_matches_legacy_size_computation() -> None:
    """``result_size_bytes`` semantics preserved: the pre-serialized size
    equals what the plain-dict path computes for the same content."""
    payload = {"k": "v" * 100}
    data = _json_dumps(payload)
    legacy_conn = _FakeConn()
    pre_conn = _FakeConn()

    await _mark_succeeded_on_conn(legacy_conn, _SQL, JobId(new_job_id()), new_uuid(), dict(payload))
    await _mark_succeeded_on_conn(
        pre_conn, _SQL, JobId(new_job_id()), new_uuid(), result_bytes=data
    )

    assert legacy_conn.fetchrow_args is not None
    assert pre_conn.fetchrow_args is not None
    assert pre_conn.fetchrow_args[4] == legacy_conn.fetchrow_args[4]
    assert pre_conn.fetchrow_args[3] == legacy_conn.fetchrow_args[3]


async def test_result_bytes_with_nul_raises_value_error() -> None:
    """The NUL guard still fires on the pre-serialized bytes, with the
    exact message ``dumps_jsonb_str`` raises for the same content."""
    nul_payload = {"k": "a\x00b"}
    with pytest.raises(ValueError) as legacy_exc:
        dumps_jsonb_str(nul_payload)
    conn = _FakeConn()

    with pytest.raises(ValueError) as pre_exc:
        await _mark_succeeded_on_conn(
            conn, _SQL, JobId(new_job_id()), new_uuid(), result_bytes=_json_dumps(nul_payload)
        )

    assert str(pre_exc.value) == str(legacy_exc.value)
    assert conn.fetchrow_args is None


async def test_result_and_result_bytes_are_mutually_exclusive() -> None:
    """Passing both the dict and its encoding is a caller bug — rejected
    loudly before anything is bound."""
    conn = _FakeConn()

    with pytest.raises(ValueError, match="mutually exclusive"):
        await _mark_succeeded_on_conn(
            conn,
            _SQL,
            JobId(new_job_id()),
            new_uuid(),
            {"ok": True},
            result_bytes=_json_dumps({"ok": True}),
        )

    assert conn.fetchrow_args is None


# ── Plain-dict path — unchanged for direct backend callers ─────────────


async def test_plain_dict_result_serializes_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A plain dict (direct ``Backend.mark_succeeded`` callers) still works:
    exactly one serialization OF THE RESULT inside the terminal, exact size
    (the event-detail dump is unrelated and filtered out)."""
    counter = _CountingDumps()
    monkeypatch.setattr("taskq._json.dumps", counter)
    conn = _FakeConn()
    payload = {"ok": True}

    ok = await _mark_succeeded_on_conn(conn, _SQL, JobId(new_job_id()), new_uuid(), payload)

    assert ok is True
    assert [call for call in counter.calls if call is payload] == [payload]
    assert conn.fetchrow_args is not None
    assert conn.fetchrow_args[3] == dumps_jsonb_str(payload)
    assert conn.fetchrow_args[4] == len(_json_dumps(payload))


async def test_plain_dict_result_with_nul_still_raises() -> None:
    """Plain-dict path NUL guard (dumps_jsonb_str) untouched."""
    conn = _FakeConn()

    with pytest.raises(ValueError, match="NUL"):
        await _mark_succeeded_on_conn(conn, _SQL, JobId(new_job_id()), new_uuid(), {"k": "a\x00b"})


async def test_none_result_stores_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``result=None`` stores NULL/NULL regardless of path."""
    counter = _CountingDumps()
    monkeypatch.setattr("taskq._json.dumps", counter)
    conn = _FakeConn()

    ok = await _mark_succeeded_on_conn(conn, _SQL, JobId(new_job_id()), new_uuid(), None)

    assert ok is True
    # None result: no result serialization, and the fused statement builds
    # the event detail server-side, so zero client-side dumps calls total.
    assert len(counter.calls) == 0
    assert conn.fetchrow_args is not None
    assert conn.fetchrow_args[3] is None
    assert conn.fetchrow_args[4] is None


async def test_result_bytes_over_cap_still_raises_result_too_large() -> None:
    """The storage-boundary cap guard still applies to pre-serialized
    bytes (defense in depth — a caller-supplied bytes blob over the cap
    is rejected at the same site)."""
    big_payload = {"blob": "x" * 65536 + "overflow"}
    data = _json_dumps(big_payload)
    assert len(data) > 65536
    conn = _FakeConn()

    with pytest.raises(ResultTooLarge):
        await _mark_succeeded_on_conn(
            conn, _SQL, JobId(new_job_id()), new_uuid(), result_bytes=data
        )


# ── Integration: the decoded str binds for ::jsonb on real PG ──────────


@pytest.mark.integration
class TestResultBytesAgainstPostgres:
    """The pre-serialized bytes survive the real ``Backend`` →
    ``_mark_succeeded_on_conn`` → asyncpg ``::jsonb`` binding path."""

    async def test_result_bytes_stored_in_pg(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        payload: dict[str, object] = {"ok": True, "nested": {"a": [1, 2, 3]}}
        data = _json_dumps(payload)
        ok = await backend.mark_succeeded(job_id, worker_id, result_bytes=data)
        assert ok is True

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT result, result_size_bytes FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
                job_id,
            )
        assert row is not None
        assert jsonb_to_dict(row["result"]) == payload
        assert row["result_size_bytes"] == len(data)

    async def test_size_identical_to_plain_dict_call(self, clean_jobs_app: JobsApp) -> None:
        """result_bytes and plain-dict calls store the SAME result_size_bytes
        for the same content — semantics fully preserved."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        payload: dict[str, object] = {"k": "v" * 500}

        async with deps.worker_pool.acquire() as conn:
            bytes_worker, bytes_job = await setup_running_job(conn, schema)
        async with deps.worker_pool.acquire() as conn:
            plain_worker, plain_job = await setup_running_job(conn, schema)

        assert (
            await backend.mark_succeeded(bytes_job, bytes_worker, result_bytes=_json_dumps(payload))
            is True
        )
        assert await backend.mark_succeeded(plain_job, plain_worker, payload) is True

        async with deps.worker_pool.acquire() as conn:
            rows = await conn.fetch(
                f'SELECT id, result_size_bytes FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
                [bytes_job, plain_job],
            )
        sizes = {row["id"]: row["result_size_bytes"] for row in rows}
        assert sizes[bytes_job] == sizes[plain_job] == len(_json_dumps(payload))
