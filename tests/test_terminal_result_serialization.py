"""Single-serialization contract for the success-path result payload.

The consumer serializes an actor's result dict exactly once (orjson →
bytes) and hands the terminal write those bytes through the explicit
``result_bytes`` parameter of ``mark_succeeded`` /
``mark_succeeded_with_conn``.  ``_mark_succeeded_on_conn`` must reuse the
bytes (one NUL scan + one decode) instead of re-serializing a dict and
re-encoding the bound str just to measure ``result_size_bytes``.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq._json import dumps as _json_dumps
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.backend._records import jsonb_to_dict
from taskq.backend._sql_templates import render as render_sql
from taskq.backend._terminal import _mark_succeeded_on_conn
from taskq.exceptions import ResultTooLarge
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend
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
    binding, size taken from len(bytes) - no serialization, no re-encode."""
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
    """Passing both the dict and its encoding is a caller bug - rejected
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


# ── Plain-dict path - unchanged for direct backend callers ─────────────


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
    bytes (defense in depth - a caller-supplied bytes blob over the cap
    is rejected at the same site)."""
    big_payload = {"blob": "x" * 65536 + "overflow"}
    data = _json_dumps(big_payload)
    assert len(data) > 65536
    conn = _FakeConn()

    with pytest.raises(ResultTooLarge):
        await _mark_succeeded_on_conn(
            conn, _SQL, JobId(new_job_id()), new_uuid(), result_bytes=data
        )


# ── In-memory backend - the same contract ────────────────────────────
#
# The consumer passes result_bytes on every success, whichever backend is
# configured, so the testing backend must be observable-equivalent to PG
# on this path or tests run under it diverge from production. The PG-side
# guards above each name their in-memory parity obligation; these are them.

_START = datetime(2026, 1, 1, tzinfo=UTC)


async def _in_memory_running_job(
    backend: InMemoryBackend,
) -> tuple[JobId, UUID]:
    """Enqueue and claim one job; return (job_id, worker_id) of the
    running row the terminal write expects."""
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement - candidates come FROM the registry).
    if "test_actor" not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]  # Why: test-only private access; the established fixture pattern.
        backend.register_actor_config(actor="test_actor")
    await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            metadata={},
        )
    )
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(claimed) == 1
    return claimed[0].id, worker_id


async def test_in_memory_result_bytes_round_trip_stores_decoded_result_and_exact_size() -> None:
    """The bytes form reads back exactly as PG: the decoded result dict and
    the byte length of what PG would store. A testing backend that stored
    the raw bytes (or re-serialized) would make green tests diverge from
    production."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)
    payload = {"value": 42, "nested": {"a": [1, 2, 3]}}
    data = _json_dumps(payload)

    ok = await backend.mark_succeeded(
        job_id, worker_id, result_bytes=data, attempt=1, claim_epoch=1
    )

    assert ok is True
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.result == payload
    assert row.result_size_bytes == len(data)


async def test_in_memory_result_and_result_bytes_are_mutually_exclusive() -> None:
    """Both forms at once is a caller bug - rejected loudly before any
    state change, same as the PG terminal (the job must stay running)."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    with pytest.raises(ValueError, match="mutually exclusive"):
        await backend.mark_succeeded(
            job_id,
            worker_id,
            {"ok": True},
            result_bytes=_json_dumps({"ok": True}),
        )

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "running"


async def test_in_memory_result_bytes_with_nul_raises_value_error() -> None:
    """Caller-supplied bytes carrying a NUL escape would bind as invalid
    jsonb on PG - the testing backend rejects them with the same
    ValueError, at the same boundary."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    with pytest.raises(ValueError, match="NUL"):
        await backend.mark_succeeded(job_id, worker_id, result_bytes=_json_dumps({"k": "a\x00b"}))


async def test_in_memory_plain_dict_result_with_nul_raises_value_error() -> None:
    """The dict form is NUL-guarded too - PG's jsonb binding rejects the
    value, so the testing backend must fail identically rather than store
    a result PG never could."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    with pytest.raises(ValueError, match="NUL"):
        await backend.mark_succeeded(job_id, worker_id, {"k": "a\x00b"})


async def test_in_memory_progress_state_with_nul_raises_value_error() -> None:
    """PG guards ``progress_state`` at bind time too (``jsonb_param`` →
    ``dumps_jsonb_str``), so a direct ``mark_succeeded(progress_state=...)``
    carrying a NUL raises ValueError before the fencing write. The mirror's
    result NUL guard is pinned above; the progress path must match or a
    test stores a state PG never could, the exact parity the mirror
    rewrite exists to hold."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    with pytest.raises(ValueError, match="NUL"):
        await backend.mark_succeeded(
            job_id,
            worker_id,
            {"done": True},
            progress_state={"detail": "bad\x00value"},
            attempt=1,
            claim_epoch=1,
        )

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "running"


async def test_in_memory_result_bytes_over_cap_raises_result_too_large() -> None:
    """The result cap applies to the bytes form at its measured length -
    the same storage-boundary guard the PG terminal applies."""
    backend = InMemoryBackend(clock=FakeClock(_START), result_max_bytes=64)
    job_id, worker_id = await _in_memory_running_job(backend)
    data = _json_dumps({"blob": "x" * 128})
    assert len(data) > 64

    with pytest.raises(ResultTooLarge, match="bytes exceeds"):
        await backend.mark_succeeded(job_id, worker_id, result_bytes=data)


async def test_in_memory_result_bytes_exactly_at_cap_is_stored() -> None:
    """The cap is a strict comparison on both backends: bytes measuring
    exactly result_max_bytes are stored, size recorded in full. One
    off-by-one to `>=` here would strand every result at the cap."""
    backend = InMemoryBackend(clock=FakeClock(_START), result_max_bytes=64)
    job_id, worker_id = await _in_memory_running_job(backend)
    data = _json_dumps({"blob": "x" * 53})
    assert len(data) == 64

    ok = await backend.mark_succeeded(
        job_id, worker_id, result_bytes=data, attempt=1, claim_epoch=1
    )

    assert ok is True
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.result_size_bytes == 64


async def test_in_memory_empty_result_bytes_raises_value_error_and_job_stays_running() -> None:
    """Empty bytes are never valid orjson output. The PG terminal rejects
    them with a dedicated ValueError whose rationale names this mirror
    ("same ValueError class the in-memory/testing mirrors raise for the
    same input") - the mirror must keep that true: a ValueError-family
    rejection, before any state change, the row still running."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    with pytest.raises(ValueError):
        await backend.mark_succeeded(job_id, worker_id, result_bytes=b"")

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "running"


async def test_in_memory_result_bytes_invalid_json_raises_value_error() -> None:
    """Bytes that are not valid JSON are rejected with the ValueError
    family - the reference behavior the PG terminal's guard matches
    (bound as text and cast server-side, the jsonb rejection would arrive
    as a PostgresError the terminal-write classification reads as
    transient infra). The mirror must keep raising here or the two
    backends diverge on the exception family a caller observes; the
    message is the shared helper's, so both backends reject with the
    same words the NUL and empty guards already share."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    with pytest.raises(ValueError, match="valid orjson output"):
        await backend.mark_succeeded(job_id, worker_id, result_bytes=b"not json")

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "running"


async def test_in_memory_non_object_valid_json_result_reads_back_like_pg() -> None:
    """Any valid JSON the guard accepts must be readable back on the
    mirror exactly as PG reads it back. PG accepts any valid JSON in the
    result column and reads it verbatim through ``jsonb_to_value`` ->
    ``loads``, so a JSON array stores and reads back as ``[1, 2]``. The
    mirror's guard accepts the same bytes (they are valid JSON), but its
    read path assumes a dict and must not crash on the stored value."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    ok = await backend.mark_succeeded(
        job_id, worker_id, result_bytes=b"[1, 2]", attempt=1, claim_epoch=1
    )

    assert ok is True
    row = await backend.get(job_id)
    assert row is not None
    assert row.result == [1, 2]


async def test_in_memory_result_bytes_invalid_json_rejected_even_when_the_job_is_not_running() -> (
    None
):
    """Content validation, like the mutual-exclusivity check, runs before
    the state fence - the mirror matches the PG terminal's order (its
    guards precede the fencing UPDATE). A non-running job must not turn a
    permanently-unstorable value into a silent False indistinguishable
    from an innocent fencing mismatch."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)
    await backend.mark_succeeded(job_id, worker_id, {"done": True})

    with pytest.raises(ValueError):
        await backend.mark_succeeded(job_id, worker_id, result_bytes=b"not json")


async def test_in_memory_both_result_forms_rejected_even_when_the_job_is_not_running() -> None:
    """PG validates the two-form misuse at the function top, before any
    state lookup - a caller passing both forms always gets the loud
    ValueError. The mirror gates the same check behind the running/lock
    fence, so the identical misuse on a job that is not running (already
    terminal, wrong worker, never existed) returns False - the same
    outcome as an innocent fencing mismatch - and the caller bug hides.
    Observable-equivalence with PG on this path is the mirror's contract."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)
    await backend.mark_succeeded(job_id, worker_id, {"done": True})

    with pytest.raises(ValueError, match="mutually exclusive"):
        await backend.mark_succeeded(
            job_id,
            worker_id,
            {"again": True},
            result_bytes=_json_dumps({"again": True}),
        )


async def test_in_memory_over_cap_result_rejected_even_when_the_job_is_not_running() -> None:
    """The cap check is validation too: the backend contract
    (docs/architecture.md) is that ALL validation precedes the fencing
    write, so a permanently-unstorable result raises ResultTooLarge
    whatever the job's state - never returns the False of an innocent
    fencing mismatch. The ValueError-family guards are pinned against a
    non-running job above; this pins the one other exception family,
    raised from a different site."""
    backend = InMemoryBackend(clock=FakeClock(_START), result_max_bytes=64)
    job_id, worker_id = await _in_memory_running_job(backend)
    await backend.mark_succeeded(job_id, worker_id, {"done": True})

    with pytest.raises(ResultTooLarge):
        await backend.mark_succeeded(job_id, worker_id, {"blob": "x" * 128})


@pytest.mark.parametrize(
    ("dict_result", "pg_read_back"),
    [
        ({"v": float("nan")}, {"v": None}),
        ({"v": float("inf")}, {"v": None}),
        (
            {"v": UUID("12345678-1234-5678-1234-567812345678")},
            {"v": "12345678-1234-5678-1234-567812345678"},
        ),
        ({"v": (1, 2)}, {"v": [1, 2]}),
    ],
    ids=["nan-nulls", "inf-nulls", "uuid-stringifies", "tuple-becomes-array"],
)
async def test_in_memory_dict_form_result_normalizes_to_pg_observable_state(
    dict_result: dict[str, object],
    pg_read_back: dict[str, object],
) -> None:
    """The mirror's dict-form write claims to "normalize to the same
    observable state as PG" - but it stores the caller's Python objects
    verbatim (``dict(result)``), while PG stores what orjson emitted and
    reads the JSON back. Every value whose orjson encoding differs from
    the object diverges on read-back: NaN and Infinity become null, UUIDs
    become their string form, tuples become arrays. The consumer's bytes
    form round-trips through JSON on both backends and cannot diverge -
    this is the dict form's parity hole."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    job_id, worker_id = await _in_memory_running_job(backend)

    ok = await backend.mark_succeeded(job_id, worker_id, dict_result, attempt=1, claim_epoch=1)
    assert ok is True

    row = await backend.get(job_id)
    assert row is not None
    assert row.result == pg_read_back, (
        "the in-memory dict form must read back exactly what PG's jsonb "
        f"round-trip reads back (expected {pg_read_back!r}, got {row.result!r})"
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
        ok = await backend.mark_succeeded(
            job_id, worker_id, result_bytes=data, attempt=1, claim_epoch=1
        )
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
        for the same content - semantics fully preserved."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name
        payload: dict[str, object] = {"k": "v" * 500}

        async with deps.worker_pool.acquire() as conn:
            bytes_worker, bytes_job = await setup_running_job(conn, schema)
        async with deps.worker_pool.acquire() as conn:
            plain_worker, plain_job = await setup_running_job(conn, schema)

        assert (
            await backend.mark_succeeded(
                bytes_job, bytes_worker, result_bytes=_json_dumps(payload), attempt=1, claim_epoch=1
            )
            is True
        )
        assert (
            await backend.mark_succeeded(plain_job, plain_worker, payload, attempt=1, claim_epoch=1)
            is True
        )

        async with deps.worker_pool.acquire() as conn:
            rows = await conn.fetch(
                f'SELECT id, result_size_bytes FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
                [bytes_job, plain_job],
            )
        sizes = {row["id"]: row["result_size_bytes"] for row in rows}
        assert sizes[bytes_job] == sizes[plain_job] == len(_json_dumps(payload))

    async def test_result_bytes_invalid_json_rejected_at_the_boundary(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """Bytes that are not valid JSON must be rejected client-side with
        the ValueError family - the same classification its sibling guards
        (empty bytes, NUL) already apply, for the same reason: bound as
        text and cast server-side, the jsonb rejection arrives as a
        PostgresError, which the terminal-write classification reads as
        TRANSIENT INFRASTRUCTURE failure - the job never reaches a terminal
        state, the lease sweep reclaims it, a re-run produces the same
        bytes, and the write loops. A permanent data defect must not wear
        an infra failure's costume; the in-memory mirror raises ValueError
        for the identical input (see the in-memory section), so this is
        also the parity seam between the two backends."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        with pytest.raises(ValueError):
            await backend.mark_succeeded(job_id, worker_id, result_bytes=b"not json")

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
                job_id,
            )
        assert row is not None
        assert row["status"] == "running"

    async def test_result_bytes_undecodable_utf8_rejected_by_the_parse_guard(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """The JSON-parse guard's claim, pinned on its distinct input
        family: orjson rejects bytes that are not decodable UTF-8 with the
        same ValueError family as malformed ASCII, so the boundary's own
        message ("the bytes are not decodable JSON") carries the cause and
        the decode below the guard cannot fail. Binary garbage must never
        reach the server as a text binding - the same transient-infra
        misclassification the malformed-JSON test above guards."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        with pytest.raises(ValueError, match="not decodable JSON"):
            await backend.mark_succeeded(job_id, worker_id, result_bytes=b"\xff\xfe")

        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
                job_id,
            )
        assert row is not None
        assert row["status"] == "running"

    async def test_non_object_json_result_round_trips_on_pg(self, clean_jobs_app: JobsApp) -> None:
        """The mirror's non-object round-trip test asserts parity with a PG
        behavior it only reads in code - anchor it: a JSON array result
        stores and reads back verbatim on the reference implementation
        (``jsonb_to_value`` -> ``loads`` passes the list through), which is
        what licenses the mirror to do the same."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)

        ok = await backend.mark_succeeded(
            job_id, worker_id, result_bytes=b"[1, 2]", attempt=1, claim_epoch=1
        )
        assert ok is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.result == [1, 2]
        assert row.result_size_bytes == len(b"[1, 2]")

    async def test_invalid_result_bytes_rejected_before_the_fence_on_a_terminal_job(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """The contract the in-memory mirror's fence-ordering tests pin
        parity AGAINST: on the reference implementation, invalid
        ``result_bytes`` on a job that is already terminal raises the
        ValueError family - never returns the False of an innocent
        fencing mismatch. Without this anchor, the mirror's ordering
        tests pin parity with a documented behavior nothing asserts PG
        itself keeps."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(conn, schema)
        await backend.mark_succeeded(job_id, worker_id, {"done": True}, attempt=1, claim_epoch=1)

        with pytest.raises(ValueError):
            await backend.mark_succeeded(job_id, worker_id, result_bytes=b"not json")

    async def test_dict_form_result_reads_back_json_round_tripped_on_pg(
        self, clean_jobs_app: JobsApp
    ) -> None:
        """The reference behavior the mirror's dict-form test pins parity
        against: PG stores what orjson emitted, so a UUID result reads
        back its string form and a NaN result reads back null - the
        JSON round-trip is the normalization the in-memory dict form
        skips."""
        deps = clean_jobs_app.deps
        backend = clean_jobs_app.backend
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            uuid_worker, uuid_job = await setup_running_job(conn, schema)
        async with deps.worker_pool.acquire() as conn:
            nan_worker, nan_job = await setup_running_job(conn, schema)

        uuid_value = UUID("12345678-1234-5678-1234-567812345678")
        assert (
            await backend.mark_succeeded(
                uuid_job, uuid_worker, {"v": uuid_value}, attempt=1, claim_epoch=1
            )
            is True
        )
        assert (
            await backend.mark_succeeded(
                nan_job, nan_worker, {"v": float("nan")}, attempt=1, claim_epoch=1
            )
            is True
        )

        async with deps.worker_pool.acquire() as conn:
            rows = await conn.fetch(
                f'SELECT id, result FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
                [uuid_job, nan_job],
            )
        results = {str(row["id"]): jsonb_to_dict(row["result"]) for row in rows}
        assert results[str(uuid_job)] == {"v": "12345678-1234-5678-1234-567812345678"}
        assert results[str(nan_job)] == {"v": None}
