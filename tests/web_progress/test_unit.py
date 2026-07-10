"""Unit tests for taskq.web.progress SSE bridge and poll-state endpoint (§3.7, §10.3).

Uses stub PG and Redis mocks — no testcontainers required.

Strategy
--------
Most tests exercise the generator directly via ``async for`` to avoid blocking
the TestClient with an infinite SSE keepalive loop.  HTTP-layer tests use only
terminal scenarios where the generator exits naturally, or read until a
specific marker and then close the connection.
"""

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sse_starlette.event import ServerSentEvent

from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import progress_channel
from taskq.progress._events import ProgressEvent
from taskq.web.progress import (
    _event_generator,  # pyright: ignore[reportPrivateUsage]  # Why: unit tests import private symbols to exercise them directly.
    _resolve_last_event_id,  # pyright: ignore[reportPrivateUsage]  # Why: unit tests import private symbols to exercise them directly.
    _serialize_progress_state,  # pyright: ignore[reportPrivateUsage]  # Why: unit tests import private symbols to exercise them directly.
    create_router,
)

_SCHEMA_LABEL = "taskq"
_JOB_ID = UUID("00000000-0000-0000-0000-000000000001")
_HEARTBEAT = timedelta(milliseconds=10)


class _FakeConn:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        return self._row


class _AcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
        return _AcquireCtx(_FakeConn(self._row))


def _pg_row(
    *,
    status: str = "running",
    progress_seq: int = 5,
    progress_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "progress_seq": progress_seq,
        "progress_state": progress_state or {"step": 1},
    }


def _make_event(
    *,
    seq: int,
    terminal: bool = False,
    step: int = 1,
) -> ProgressEvent:
    return ProgressEvent(
        v=1,
        kind="progress" if not terminal else "state_change",
        job_id=_JOB_ID,
        actor="test_actor",
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        seq=seq,
        status="succeeded" if terminal else "running",
        step=step,
        percent=float(step * 10),
        terminal=terminal,
    )


def _redis_msg(event: ProgressEvent) -> dict[str, Any]:
    return {
        "type": "message",
        "channel": b"taskq:taskq:progress:" + str(_JOB_ID).encode(),
        "data": event.model_dump_json(exclude_none=True).encode(),
    }


_EXHAUST = object()


class _StubPubSub:
    """Minimal redis PubSub duck-type for unit tests.

    ``messages`` is a list of items:
      - ``dict`` — a Redis message returned by get_message
      - ``None`` — simulate a timeout (keepalive emitted)
      - ``_EXHAUST`` — signals end-of-stream: subsequent calls return None
    """

    def __init__(self, messages: list[dict[str, Any] | None | object]) -> None:
        self._messages = list(messages)
        self._pos = 0
        self.subscribed_channels: list[str | bytes] = []
        self.unsubscribed = False
        self.closed = False

    async def subscribe(self, channel: str | bytes) -> None:
        self.subscribed_channels.append(channel)

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message signature; not a missing asyncio.timeout pattern.
    ) -> dict[str, Any] | None:
        if self._pos < len(self._messages):
            item = self._messages[self._pos]
            self._pos += 1
            if item is _EXHAUST:
                return None
            return item  # type: ignore[return-value]  # Why: item is dict|None here; _EXHAUST was already handled.
        return None

    async def unsubscribe(self, channel: str | bytes) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.closed = True


class _StubRedis:
    def __init__(self, pubsub: _StubPubSub) -> None:
        self._pubsub = pubsub

    def pubsub(self) -> _StubPubSub:
        return self._pubsub


def _make_app(
    pg_row: dict[str, Any] | None,
    pubsub: _StubPubSub | None,
    *,
    schema: str = _SCHEMA_LABEL,
    heartbeat: timedelta = _HEARTBEAT,
    auth_dependency: Any | None = None,
) -> tuple[FastAPI, TestClient]:
    redis_client: _StubRedis | None = _StubRedis(pubsub) if pubsub is not None else None
    pool = _StubPool(pg_row)
    router = create_router(
        pool,  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub satisfies the erased Any boundary at redis_client; pyright cannot verify structural compatibility across Any.
        redis_client,  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub satisfies the erased Any boundary at redis_client; pyright cannot verify structural compatibility across Any.
        schema=schema,
        auth_dependency=auth_dependency,
        sse_heartbeat_interval=heartbeat,
    )
    app = FastAPI()
    app.include_router(router, prefix="/jobs")
    client = TestClient(app, raise_server_exceptions=False)
    return app, client


async def _drive_generator(
    pg_row: dict[str, Any] | None,
    pubsub: _StubPubSub,
    *,
    schema: str = _SCHEMA_LABEL,
    last_event_id_header: str | None = None,
    last_event_id_param: int | None = None,
    heartbeat: timedelta = _HEARTBEAT,
    max_events: int = 20,
) -> list[ServerSentEvent]:
    from unittest.mock import MagicMock

    mock_request = MagicMock()
    mock_request.headers.get.return_value = last_event_id_header

    resolved_lei = _resolve_last_event_id(mock_request, last_event_id_param)
    channel = progress_channel(schema, _JOB_ID)
    heartbeat_secs = heartbeat.total_seconds()
    pool = _StubPool(pg_row)

    await pubsub.subscribe(channel)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("", _JOB_ID)

    if row is None:
        raise HTTPException(status_code=404, detail="job not found")

    raw_progress_state: Any = row["progress_state"]
    progress_seq: int = row["progress_seq"]
    status: str = row["status"]
    is_terminal = status in TERMINAL_STATUSES
    progress_data = _serialize_progress_state(raw_progress_state)

    results: list[ServerSentEvent] = []
    gen = _event_generator(
        pubsub=pubsub,
        channel=channel,
        job_id=_JOB_ID,
        is_terminal=is_terminal,
        progress_seq=progress_seq,
        progress_data=progress_data,
        resolved_last_event_id=resolved_lei,
        heartbeat_secs=heartbeat_secs,
    )
    try:
        async for sse_event in gen:
            results.append(sse_event)
            if len(results) >= max_events:
                break
    finally:
        await gen.aclose()

    return results


def _encode(sse: ServerSentEvent) -> str:
    return sse.encode().decode("utf-8")


# ── SSE wire format correctness ──────────────────────────────────


@pytest.mark.asyncio
async def test_sse_wire_format() -> None:
    """SSE events must include id:, event:, data:, and blank-line separator."""
    event = _make_event(seq=5)
    pubsub = _StubPubSub([_redis_msg(event), _EXHAUST])
    pg_row = _pg_row(status="running", progress_seq=3)

    results = await _drive_generator(pg_row, pubsub)

    assert len(results) >= 2

    first_raw = _encode(results[0])
    assert "id: 3\n" in first_raw
    assert "event: progress\n" in first_raw
    assert "data: " in first_raw
    assert first_raw.endswith("\n\n")

    data_line = next(line for line in first_raw.splitlines() if line.startswith("data: "))
    json.loads(data_line[len("data: ") :])


# ── Last-Event-ID header takes precedence over query param ───────


@pytest.mark.asyncio
async def test_header_takes_precedence_over_query_param() -> None:
    """Last-Event-ID header wins over ?last_event_id= query param."""
    pg_row = _pg_row(status="running", progress_seq=7, progress_state={"step": 7})
    pubsub = _StubPubSub([_EXHAUST])

    results = await _drive_generator(
        pg_row,
        pubsub,
        last_event_id_header="5",
        last_event_id_param=3,
    )

    assert len(results) >= 1
    first_raw = _encode(results[0])
    assert "id: 7\n" in first_raw
    assert "event: progress\n" in first_raw


# ── Query param used when no header ─────────────────────────────


@pytest.mark.asyncio
async def test_query_param_used_when_no_header() -> None:
    """When no Last-Event-ID header, ?last_event_id= query param is used."""
    pg_row = _pg_row(status="running", progress_seq=7, progress_state={"step": 7})
    pubsub = _StubPubSub([_EXHAUST])

    results = await _drive_generator(
        pg_row,
        pubsub,
        last_event_id_header=None,
        last_event_id_param=3,
    )

    assert len(results) >= 1
    first_raw = _encode(results[0])
    assert "id: 7\n" in first_raw


# ── No catch-up when progress_seq <= last_event_id ──────────────


@pytest.mark.asyncio
async def test_no_catchup_when_seq_not_advanced() -> None:
    """No catch-up event when progress_seq <= last_event_id."""
    live_event = _make_event(seq=6)
    pubsub = _StubPubSub([_redis_msg(live_event), _EXHAUST])
    pg_row = _pg_row(status="running", progress_seq=3)

    results = await _drive_generator(
        pg_row,
        pubsub,
        last_event_id_param=5,
    )

    assert len(results) >= 1
    first_raw = _encode(results[0])
    assert "id: 6\n" in first_raw

    raw_all = "".join(_encode(r) for r in results)
    assert "id: 3\n" not in raw_all


# ── Duplicate filter (seq <= last_emitted_seq suppressed) ───────


@pytest.mark.asyncio
async def test_duplicate_filter() -> None:
    """Events with seq <= last_emitted_seq are suppressed."""
    pg_row = _pg_row(status="running", progress_seq=5)
    dup_event = _make_event(seq=5)
    new_event = _make_event(seq=6)
    pubsub = _StubPubSub([_redis_msg(dup_event), _redis_msg(new_event), _EXHAUST])

    results = await _drive_generator(pg_row, pubsub)

    raw_all = "".join(_encode(r) for r in results)
    assert raw_all.count("id: 5\n") == 1
    assert "id: 6\n" in raw_all


# ── Terminal event closes stream ────────────────────────────────


@pytest.mark.asyncio
async def test_terminal_event_closes_stream() -> None:
    """Terminal Redis event emits event:terminal + event:done, then generator exits."""
    pg_row = _pg_row(status="running", progress_seq=3)
    terminal_event = _make_event(seq=8, terminal=True)
    pubsub = _StubPubSub([_redis_msg(terminal_event)])

    results = await _drive_generator(pg_row, pubsub)

    raw_all = "".join(_encode(r) for r in results)
    assert "event: terminal\n" in raw_all
    assert "event: done\n" in raw_all

    event_types = [r.event for r in results]
    assert "terminal" in event_types
    assert "done" in event_types
    terminal_idx = event_types.index("terminal")
    done_idx = event_types.index("done")
    assert terminal_idx < done_idx

    done_ev = results[done_idx]
    assert done_ev.id is None
    assert done_ev.data is None


# ── Already-terminal at connect time ─────────────────────────────


@pytest.mark.asyncio
async def test_already_terminal_at_connect_time() -> None:
    """If PG status is terminal: emit event:terminal then event:done then exit."""
    pg_row = _pg_row(status="succeeded", progress_seq=5)
    pubsub = _StubPubSub([])

    results = await _drive_generator(pg_row, pubsub)

    assert len(results) == 2
    assert results[0].event == "terminal"
    assert results[0].id == "5"
    assert results[1].event == "done"
    assert results[1].id is None


# ── Heartbeat comment emitted on real timeout ─────────────────────


@pytest.mark.asyncio
async def test_heartbeat_comment_emitted() -> None:
    """Heartbeat comment emitted when no Redis message arrives within interval."""
    pg_row = _pg_row(status="running", progress_seq=3)
    heartbeat = timedelta(milliseconds=50)
    # Two Nones = two timeouts = two keepalive opportunities, then exhaust
    pubsub = _StubPubSub([None, None, _EXHAUST])

    results = await _drive_generator(pg_row, pubsub, heartbeat=heartbeat)

    keepalive_events = [r for r in results if r.comment == "keepalive"]
    assert len(keepalive_events) >= 1
    ka = keepalive_events[0]
    assert ka.id is None
    assert ka.data is None

    ka_raw = _encode(ka)
    assert ": keepalive" in ka_raw


# ── Job not found → HTTP 404 ─────────────────────────────────────


def test_job_not_found_returns_404() -> None:
    """PG returning no row must produce HTTP 404, not an SSE stream."""
    pubsub = _StubPubSub([])
    _, client = _make_app(None, pubsub)

    resp = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert resp.status_code == 404


# ── Redis unavailable → HTTP 503 with Retry-After: 2 ───────────


def test_redis_unavailable_returns_503() -> None:
    """When redis_client is None, endpoint must return HTTP 503 with Retry-After."""
    pg_row = _pg_row()
    _, client = _make_app(pg_row, None)

    resp = client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream")
    assert resp.status_code == 503
    assert resp.headers.get("retry-after") == "2"
    body = resp.json()
    assert body == {"error": "redis_not_configured"}


# ── Subscribe-before-query ordering ─────────────────────────────


@pytest.mark.asyncio
async def test_subscribe_before_query_ordering() -> None:
    """Subscribe-before-query ensures events published during PG read are not lost."""
    # PG returns progress_seq=5.  Redis delivers seq=6 live event.
    # Because subscribe happens before query, seq=6 arrives via pubsub.
    # The dedup filter: snapshot emits seq=5; live seq=6 > 5 → forwarded.
    pg_row = _pg_row(status="running", progress_seq=5)
    live_event = _make_event(seq=6)
    pubsub = _StubPubSub([_redis_msg(live_event), _EXHAUST])

    results = await _drive_generator(pg_row, pubsub)

    raw_all = "".join(_encode(r) for r in results)
    # Snapshot at seq=5 is emitted
    assert "id: 5\n" in raw_all
    # Live event seq=6 is forwarded (not lost)
    assert "id: 6\n" in raw_all

    # Verify subscribe was called (subscribe-before-query contract)
    assert len(pubsub.subscribed_channels) >= 1


# ── Response headers ────────────────────────────────────────────


def test_response_headers() -> None:
    """SSE response must include X-Accel-Buffering: no and Cache-Control: no-cache."""
    pg_row = _pg_row(status="succeeded", progress_seq=1)
    pubsub = _StubPubSub([])
    _, client = _make_app(pg_row, pubsub)

    with client.stream("GET", f"/jobs/api/job/{_JOB_ID}/progress/stream") as resp:
        assert resp.status_code == 200
        assert resp.headers.get("x-accel-buffering") == "no"
        assert resp.headers.get("cache-control") == "no-cache"
        assert "text/event-stream" in resp.headers.get("content-type", "")


# ── Poll-state endpoint ─────────────────────────────────────────────────


def test_poll_state_returns_json() -> None:
    """Poll-state endpoint returns JSON with status, progress_state, progress_seq."""
    pg_row = _pg_row(status="running", progress_seq=3, progress_state={"rows": 100})
    _, client = _make_app(pg_row, None)

    resp = client.get(f"/jobs/api/job/{_JOB_ID}/state")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "running"
    assert body["progress_seq"] == 3
    assert body["progress_state"] == {"rows": 100}


def test_poll_state_404_when_not_found() -> None:
    """Poll-state endpoint returns 404 when job not found."""
    _, client = _make_app(None, None)

    resp = client.get(f"/jobs/api/job/{_JOB_ID}/state")
    assert resp.status_code == 404


# ── Auth dependency ──────────────────────────────────────────────────────


def test_auth_dependency_enforced() -> None:
    """When auth_dependency raises HTTPException(401), all routes return 401."""

    def _reject() -> None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    pg_row = _pg_row(status="succeeded", progress_seq=1)
    pubsub = _StubPubSub([])
    _, client = _make_app(pg_row, pubsub, auth_dependency=_reject)

    assert client.get(f"/jobs/api/job/{_JOB_ID}/progress/stream").status_code == 401
    assert client.get(f"/jobs/api/job/{_JOB_ID}/state").status_code == 401


# ── Invalid job_id ──────────────────────────────────────────────────────


def test_invalid_job_id_returns_422() -> None:
    """Non-UUID job_id path parameter must produce HTTP 422 (FastAPI validation)."""
    _, client = _make_app(_pg_row(), _StubPubSub([]))

    assert client.get("/jobs/api/job/not-a-uuid/progress/stream").status_code == 422
    assert client.get("/jobs/api/job/not-a-uuid/state").status_code == 422


# ── Invalid schema ───────────────────────────────────────────────────────


def test_invalid_schema_raises_value_error() -> None:
    """create_router must raise ValueError for schema names that fail _IDENT_RE."""
    pool = _StubPool(_pg_row())
    with pytest.raises(ValueError, match="invalid schema identifier"):
        create_router(pool, None, schema="bad schema; DROP TABLE jobs;--")


# ── Cleanup on disconnect ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cleanup_called_on_generator_exit() -> None:
    """pubsub.unsubscribe and aclose are called when the generator exits."""
    pg_row = _pg_row(status="succeeded", progress_seq=5)
    pubsub = _StubPubSub([])

    await _drive_generator(pg_row, pubsub)

    assert pubsub.unsubscribed is True
    assert pubsub.closed is True


# ── Reconnect: no catch-up, resume live from cursor ─────────────────────


@pytest.mark.asyncio
async def test_reconnect_no_catchup_resumes_from_cursor() -> None:
    """On reconnect with progress_seq <= last_event_id: no catch-up; live resumes."""
    pg_row = _pg_row(status="running", progress_seq=4)
    live_event = _make_event(seq=6)
    pubsub = _StubPubSub([_redis_msg(live_event), _EXHAUST])

    results = await _drive_generator(pg_row, pubsub, last_event_id_param=5)

    raw_all = "".join(_encode(r) for r in results)
    assert "id: 4\n" not in raw_all
    assert "id: 6\n" in raw_all


# ── Reconnect terminal catch-up closes immediately ───────────────────────


@pytest.mark.asyncio
async def test_reconnect_terminal_catchup_closes_immediately() -> None:
    """On reconnect where PG is terminal and seq advanced: emit terminal+done, close."""
    pg_row = _pg_row(status="succeeded", progress_seq=10)
    pubsub = _StubPubSub([])

    results = await _drive_generator(pg_row, pubsub, last_event_id_param=5)

    assert len(results) == 2
    assert results[0].event == "terminal"
    assert results[0].id == "10"
    assert results[1].event == "done"


# ── _serialize_progress_state branches ───────────────────────────────────


def test_serialize_progress_state_branches() -> None:
    """_serialize_progress_state handles None, str, bytes, and dict inputs."""
    assert _serialize_progress_state(None) == "{}"
    assert _serialize_progress_state("{}") == "{}"
    assert _serialize_progress_state(b'{"k":1}') == '{"k":1}'
    result = _serialize_progress_state({"key": "val"})
    assert json.loads(result) == {"key": "val"}


# ── _resolve_last_event_id header/query priority ──────────────────────────


def test_resolve_last_event_id_priority() -> None:
    """_resolve_last_event_id returns header value over query param."""
    from unittest.mock import MagicMock

    req_with_header = MagicMock()
    req_with_header.headers.get.return_value = "7"
    assert _resolve_last_event_id(req_with_header, 3) == 7

    req_no_header = MagicMock()
    req_no_header.headers.get.return_value = None
    assert _resolve_last_event_id(req_no_header, 3) == 3
    assert _resolve_last_event_id(req_no_header, None) is None
