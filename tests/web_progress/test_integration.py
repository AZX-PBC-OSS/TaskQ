"""Integration and negative tests for the SSE progress bridge (§3.7, §10.3).

Runs against real Postgres (testcontainers) and real
Redis (testcontainers).  SSE streams are consumed via ``httpx.AsyncClient``
with ASGI transport and ``aiter_lines()``.

Pattern notes
-------------
- Session-scoped PG container from ``tests/conftest.py`` (``pg_container``,
  ``pg_dsn``).
- Session-scoped Redis container from ``tests/conftest.py`` (``redis_container``,
  ``redis_url``).
- Per-test ``pool`` fixture: drops schema CASCADE, applies migrations, yields
  an asyncpg pool.
- ``httpx.AsyncClient(transport=httpx.ASGITransport(app=app))`` for async
  HTTP requests.
- ``client.stream("GET", url)`` + ``async for line in resp.aiter_lines()`` to
  read SSE lines to exhaustion (never ``break`` - an abandoned ``aiter_lines``
  chain finalizes as loop tasks one wave per generator after the test ends;
  under ``ASGITransport`` the body is buffered, so terminal streams reach EOF
  on their own and non-terminating ones are bounded by ``overall_timeout``).
- A ``_make_app(pool, redis_client)`` factory that calls
  ``create_router(pool, redis_client, schema=SCHEMA_LABEL)``, then mounts the
  router at prefix ``/jobs``, yielding a FastAPI ``app`` object.
"""

import asyncio
import gc
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest
import pytest_asyncio

pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.constants import progress_channel
from taskq.migrate import apply_pending
from taskq.progress._events import ProgressEvent
from taskq.web.progress import create_router

pytestmark = [pytest.mark.fastapi]
pytestmark = [pytest.mark.integration, pytest.mark.redis]

SCHEMA_LABEL = f"twp_{new_base62()}".lower()


# ── App factory ────────────────────────────────────────────────────────────


def _make_app(
    pool: asyncpg.Pool,
    redis_client: Any | None,
    *,
    auth_dependency: Any | None = None,
    sse_heartbeat_interval: timedelta = timedelta(seconds=15),
) -> FastAPI:
    router = create_router(
        pool,
        redis_client,
        schema=SCHEMA_LABEL,
        auth_dependency=auth_dependency,
        sse_heartbeat_interval=sse_heartbeat_interval,
    )
    app = FastAPI()
    app.include_router(router, prefix="/jobs")
    return app


# ── Per-test pool fixture ──────────────────────────────────────────────────


@pytest_asyncio.fixture
async def pool(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    setup_conn = await asyncpg.connect(pg_dsn)
    try:
        await setup_conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA_LABEL}" CASCADE')
        await apply_pending(setup_conn, schema=SCHEMA_LABEL)
    finally:
        await setup_conn.close()

    pg_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    assert pg_pool is not None
    try:
        yield pg_pool
    finally:
        await pg_pool.close()


# ── Per-test redis_client fixture ──────────────────────────────────────────


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[aioredis.Redis]:
    client = aioredis.from_url(redis_url, socket_timeout=None)
    try:
        yield client
    finally:
        await client.aclose()


# ── Seed helpers ───────────────────────────────────────────────────────────


async def _seed_running_job(
    pool: asyncpg.Pool,
    *,
    progress_seq: int = 0,
    progress_state: dict[str, Any] | None = None,
    status: str = "running",
) -> uuid.UUID:
    job_id = new_job_id()
    expires_at = datetime.now(UTC) + timedelta(seconds=300)
    ps = progress_state if progress_state is not None else {}
    async with pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO {SCHEMA_LABEL}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, attempt, scheduled_at, schedule_to_close,
                locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,
                progress_state, progress_seq
            ) VALUES (
                $1, $2, $3, $4::jsonb, $5, $6,
                $7, 0, 1, now(), now() + interval '300 seconds',
                $8, $9, now(), now(),
                $10::jsonb, $11
            )""",
            job_id,
            "test_actor",
            "default",
            "{}",
            3,
            "transient",
            status,
            new_uuid(),
            expires_at,
            _json_dumps(ps),
            progress_seq,
        )
    return job_id


def _json_dumps(obj: Any) -> str:
    from taskq._json import dumps_str

    return dumps_str(obj)


def _progress_event(
    *,
    job_id: uuid.UUID,
    seq: int,
    terminal: bool = False,
    status: str = "running",
    step: int | None = None,
) -> ProgressEvent:
    return ProgressEvent(
        v=1,
        kind="state_change" if terminal else "progress",
        job_id=job_id,
        actor="test_actor",
        ts=datetime.now(UTC),
        seq=seq,
        status="succeeded" if terminal else status,
        step=step,
        terminal=terminal,
    )


# ── SSE stream consumer helpers ────────────────────────────────────────────


async def _reap_stream_teardown_tasks(
    baseline: frozenset[asyncio.Task[object]],
    *,
    reap_timeout: float = 5.0,
) -> None:
    """Reap every task the SSE interaction minted, INSIDE the call phase.

    Streaming one SSE response over ``httpx.ASGITransport`` leaves loop
    tasks the test cannot hold a reference to:

    - sse-starlette's ``_shutdown_watcher``: minted by raw
      ``loop.create_task`` the first time an ``EventSourceResponse``
      streams and dropped (no reference survives), it parks until a
      uvicorn shutdown flag that no in-process test ever sets. It only
      finishes when cancelled. The autouse join in
      ``tests/web_progress/conftest.py`` reaps it at FIXTURE teardown -
      the module-loop guard's call-window snapshot correctly scores that
      as too late, so the reap belongs in the call.
    - the abandoned-``aiter_lines`` finalization cascade: ``break`` out
      of ``resp.aiter_lines()`` leaves the httpx generator chain
      (aiter_lines → aiter_text → aiter_bytes → aiter_raw → the response
      stream) suspended; CPython finalizes each abandoned generator
      through the loop's asyncgen hook, which mints an
      ``async_generator_athrow`` task per generator. Closing one
      generator releases the next, so the cascade lands as consecutive
      waves - measured here: one wave per loop turn for four waves, and
      a wave minted only after the previous one's unwind ran.

    Neither kind can be awaited by reference, so the reap is a set diff
    against *baseline* (everything pending when the interaction began:
    fixture-owned tasks, the caller's TaskGroup workers) driven to
    quiescence: ``gc.collect()`` forces each wave's finalization
    deterministically, and every outcome is retrieved - a finalizer that
    died with a real exception is a finding, never a silent give-up.

    The two kinds are reaped DIFFERENTLY, and the difference is the
    whole mechanism: a finalizer task is AWAITED, never cancelled. Its
    coroutine throws ``GeneratorExit`` into the suspended generator and
    the unwind is await-free, so the task completes within a loop turn -
    but cancelling it aborts the unwind mid-way, the released generator
    stays suspended, and the cascade re-mints one wave later (measured:
    the cancelled variant left the fourth wave pending at pytest
    teardown, exactly the residue the guard names). The watcher, by
    contrast, parks forever and is cancelled by name - the same
    identification the conftest join uses.
    """
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    deadline = loop.time() + reap_timeout

    def _is_shutdown_watcher(task: asyncio.Task[object]) -> bool:
        return getattr(task.get_coro(), "__name__", None) == "_shutdown_watcher"

    async def _reap_one_wave(wave: list[asyncio.Task[object]]) -> None:
        """Await one cascade wave to completion, outcomes retrieved.

        Lives one frame below the driver loop ON PURPOSE: every task
        reference this function holds (including the completed finalizer
        tasks, whose coroutine pins the generator it just closed) dies
        when this frame returns, so the driver's next ``gc.collect()``
        sees the chain's NEXT generator actually garbage.
        """
        # The shutdown watcher parks forever; it only finishes cancelled.
        # The finalizer tasks are awaited, never cancelled (see above).
        for task in wave:
            if _is_shutdown_watcher(task):
                task.cancel()
        done, pending = await asyncio.wait(wave, timeout=max(deadline - loop.time(), 0.001))
        crashes: list[str] = []
        for task in done:
            if task.cancelled():
                continue  # the watcher's expected cancellation delivery
            exc = task.exception()
            if exc is not None:
                crashes.append(f"  - task {task.get_name()!r} died with: {exc!r}")
        if pending or crashes:
            lines = [
                f"SSE stream teardown left {len(pending)} task(s) pending after "
                f"{reap_timeout:.0f}s - the module loop is not clean at the end of "
                "the stream interaction. Live tasks:"
            ]
            lines.extend(f"  - task {t.get_name()!r} still pending: {t!r}" for t in pending)
            lines.extend(crashes)
            raise AssertionError("\n".join(lines))

    while True:
        # Release the PREVIOUS wave's task references first: a completed
        # finalizer task still pins the generator it closed, and a
        # generator still reachable cannot be finalized, so a collect run
        # under it mints nothing and the loop would report a quiescence
        # one wave short of the real cascade.
        wave: list[asyncio.Task[object]] = []
        # Deterministic asyncgen finalization for this wave: without it the
        # abandoned generators are finalized whenever the next collection
        # happens, which must be inside the call, not at some later test's
        # gc pause.
        gc.collect()
        wave = [
            task
            for task in asyncio.all_tasks(loop) - baseline
            if not task.done() and task is not current
        ]
        if not wave:
            return
        await _reap_one_wave(wave)


async def _collect_sse_lines(
    app: FastAPI,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    max_lines: int = 200,
    read_timeout: float = 10.0,
    overall_timeout: float = 30.0,
) -> list[str]:
    lines: list[str] = []
    # Everything pending before the interaction is not this interaction's
    # leak (the reap's own task is excluded by identity, not by the baseline).
    baseline: frozenset[asyncio.Task[object]] = frozenset(asyncio.all_tasks())

    async def _read() -> None:
        nonlocal lines
        async with (
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client,
            client.stream("GET", path, headers=headers, timeout=read_timeout) as resp,
        ):
            # Read to EXHAUSTION, never ``break``: breaking out of
            # ``aiter_lines()`` abandons the suspended httpx generator chain
            # (aiter_lines → aiter_text → aiter_bytes → aiter_raw → the
            # stream), which CPython then finalizes through the loop's
            # asyncgen hook one wave per generator - a cascade of
            # ``async_generator_athrow`` tasks that lands AFTER this helper
            # returns and cannot be fully reaped from inside the call (the
            # last waves mint only once the test's own frame dies). Consumed
            # to exhaustion the chain closes ITSELF: aiter_raw's tail
            # ``await self.aclose()`` runs, every generator ends closed, no
            # finalization task is ever minted. Under ``ASGITransport`` the
            # body is fully buffered before the response is returned, so a
            # terminal stream always reaches EOF (immediately after
            # ``event: done``) and a non-terminating one never does - that
            # case is this function's ``overall_timeout``, whose
            # cancellation IS the simulated client disconnect.
            async for line in resp.aiter_lines():
                lines.append(line)
                if len(lines) >= max_lines:
                    break

    try:
        await asyncio.wait_for(_read(), timeout=overall_timeout)
    finally:
        # Every exit path owes the module loop a clean teardown. The
        # exhausted-consumption path above mints nothing; the timeout path
        # (cancellation mid-stream) leaves the watcher plus the abandoned
        # chain's finalization cascade, reaped here - bounded, outcomes
        # retrieved. See the helper.
        await _reap_stream_teardown_tasks(baseline)
    return lines


def _parse_sse_frames(lines: list[str]) -> list[dict[str, str]]:
    frames: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in lines:
        if line == "":
            if current:
                frames.append(current)
                current = {}
        elif line.startswith(": "):
            current["comment"] = line[2:]
        elif ":" in line:
            key, _, value = line.partition(":")
            current[key.strip()] = value.strip()
        else:
            pass
    if current:
        frames.append(current)
    return frames


async def _get_json(app: FastAPI, path: str) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        return await client.get(path)


# ── Full round-trip per acceptance definition ───────────────────────


@pytest.mark.asyncio
async def test_full_round_trip(pool: asyncpg.Pool, redis_client: aioredis.Redis) -> None:
    """Full round-trip - connect mid-stream with last_event_id, verify catch-up + duplicate filter + terminal."""
    job_id = await _seed_running_job(pool, progress_seq=5, progress_state={"step": 5})

    app = _make_app(pool, redis_client, sse_heartbeat_interval=timedelta(seconds=2))

    channel = progress_channel(SCHEMA_LABEL, job_id)

    async def _publish_events() -> None:
        await asyncio.sleep(0.5)
        # Duplicate events (seq <= 5) should be filtered by last_emitted_seq
        await redis_client.publish(
            channel,
            _progress_event(job_id=job_id, seq=4, step=4).model_dump_json(exclude_none=True),
        )
        await redis_client.publish(
            channel,
            _progress_event(job_id=job_id, seq=5, step=5).model_dump_json(exclude_none=True),
        )
        # Terminal event (seq=6) should pass through
        await _update_progress(
            pool, job_id, progress_seq=6, progress_state={"step": 6}, status="succeeded"
        )
        await redis_client.publish(
            channel,
            _progress_event(job_id=job_id, seq=6, terminal=True).model_dump_json(exclude_none=True),
        )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_publish_events())
        lines = await _collect_sse_lines(
            app,
            f"/jobs/api/job/{job_id}/progress/stream?last_event_id=2",
            overall_timeout=15.0,
        )

    frames = _parse_sse_frames(lines)

    catch_up = [f for f in frames if f.get("id") == "5" and f.get("event") == "progress"]
    assert len(catch_up) == 1, (
        f"Expected exactly one catch-up event id=5, got {len(catch_up)}: {frames}"
    )

    # Duplicate events seq=4,5 must be filtered out
    seq_4_frames = [f for f in frames if f.get("id") == "4"]
    assert len(seq_4_frames) == 0, (
        f"Duplicate seq=4 should be filtered, got {len(seq_4_frames)} frames"
    )

    terminal_frames = [f for f in frames if f.get("event") == "terminal"]
    done_frames = [f for f in frames if f.get("event") == "done"]
    assert len(terminal_frames) == 1, (
        f"Expected exactly one terminal event, got {len(terminal_frames)}: {frames}"
    )
    assert len(done_frames) == 1, (
        f"Expected exactly one done event, got {len(done_frames)}: {frames}"
    )


# ── Reconnect replay via Last-Event-ID header ──────────────────────


@pytest.mark.asyncio
async def test_reconnect_replay_via_header(
    pool: asyncpg.Pool, redis_client: aioredis.Redis
) -> None:
    """Reconnect with Last-Event-ID header receives catch-up before live events.

    Seeds a terminal job (progress_seq=6) and connects with
    ``Last-Event-ID: 5``.  The SSE bridge emits a terminal catch-up event
    (id=6) then done, verifying that the ``Last-Event-ID`` header triggers
    catch-up from the PG snapshot.  A non-running job is used so the SSE
    stream terminates naturally (httpx.ASGITransport collects the full
    response before returning).
    """
    job_id = await _seed_running_job(
        pool, progress_seq=6, progress_state={"step": 6}, status="succeeded"
    )

    app = _make_app(pool, redis_client, sse_heartbeat_interval=timedelta(seconds=2))

    # Reconnect with Last-Event-ID header - progress_seq=6 > 5, so catch-up fires
    lines = await _collect_sse_lines(
        app,
        f"/jobs/api/job/{job_id}/progress/stream",
        headers={"Last-Event-ID": "5"},
        overall_timeout=10.0,
    )
    frames = _parse_sse_frames(lines)

    catch_up = [f for f in frames if f.get("id") == "6"]
    assert len(catch_up) == 1, (
        f"Expected exactly one catch-up event id=6 after reconnect, got {len(catch_up)}: {frames}"
    )


# ── Client disconnect cancels Redis subscription ────────────────────


@pytest.mark.asyncio
async def test_client_disconnect_cancels_subscription(
    pool: asyncpg.Pool, redis_client: aioredis.Redis
) -> None:
    """After client disconnect, PUBSUB NUMSUB returns 0 on the channel.

    Opens an SSE stream with a short ``asyncio.wait_for`` deadline that
    cancels the read coroutine mid-stream, simulating a client disconnect.
    The ``CancelledError`` propagates into the ``_event_generator``'s
    ``finally`` block, which unsubscribes and closes the pubsub.
    After cleanup, the Redis channel should have 0 subscribers.
    """
    job_id = await _seed_running_job(pool, progress_seq=1)

    app = _make_app(pool, redis_client, sse_heartbeat_interval=timedelta(seconds=2))
    channel = progress_channel(SCHEMA_LABEL, job_id)

    # Open the SSE stream and let it be cancelled mid-stream.
    # httpx.ASGITransport buffers the full response, so we cannot read
    # individual lines from a non-terminating stream.  Instead we rely on
    # the overall_timeout to cancel the read coroutine, which triggers
    # cleanup via the generator's finally block.
    with pytest.raises((TimeoutError, asyncio.CancelledError)):
        await _collect_sse_lines(
            app,
            f"/jobs/api/job/{job_id}/progress/stream",
            overall_timeout=2.0,
        )

    await asyncio.sleep(0.5)

    result = await redis_client.execute_command("PUBSUB", "NUMSUB", channel)
    numsub = result[1] if isinstance(result, (list, tuple)) and len(result) >= 2 else result
    assert numsub == 0, f"Expected 0 subscribers after disconnect, got {numsub}"


# ── Terminal stream closes ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_terminal_stream_closes(pool: asyncpg.Pool, redis_client: aioredis.Redis) -> None:
    """Terminal job emits event:terminal then event:done; stream ends cleanly."""
    job_id = await _seed_running_job(pool, progress_seq=1, progress_state={"step": 1})

    app = _make_app(pool, redis_client, sse_heartbeat_interval=timedelta(seconds=2))

    channel = progress_channel(SCHEMA_LABEL, job_id)

    async def _publish_terminal() -> None:
        await asyncio.sleep(0.3)
        await _update_progress(
            pool, job_id, progress_seq=2, progress_state={"step": 2}, status="succeeded"
        )
        await redis_client.publish(
            channel,
            _progress_event(job_id=job_id, seq=2, terminal=True).model_dump_json(exclude_none=True),
        )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_publish_terminal())
        lines = await _collect_sse_lines(
            app,
            f"/jobs/api/job/{job_id}/progress/stream",
            overall_timeout=15.0,
        )

    frames = _parse_sse_frames(lines)

    event_types = [f.get("event") for f in frames]
    assert "terminal" in event_types, f"Expected terminal event, got: {event_types}"
    assert "done" in event_types, f"Expected done event, got: {event_types}"

    done_idx = event_types.index("done")
    remaining = event_types[done_idx + 1 :]
    assert remaining == [], f"No events after done, got: {remaining}"


# ── 404 on unknown job_id ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_404_on_unknown_job_id(pool: asyncpg.Pool, redis_client: aioredis.Redis) -> None:
    """GET /jobs/api/job/<random-uuid>/progress/stream returns HTTP 404."""
    app = _make_app(pool, redis_client)
    unknown = new_uuid()
    resp = await _get_json(app, f"/jobs/api/job/{unknown}/progress/stream")
    assert resp.status_code == 404
    assert "application/json" in resp.headers.get("content-type", "")


# ── 503 when Redis unavailable ────────────────────────────────────


@pytest.mark.asyncio
async def test_503_when_redis_unavailable(pool: asyncpg.Pool) -> None:
    """When redis_client=None, endpoint returns HTTP 503 with Retry-After: 2."""
    app = _make_app(pool, None)
    job_id = await _seed_running_job(pool, progress_seq=1)
    resp = await _get_json(app, f"/jobs/api/job/{job_id}/progress/stream")
    assert resp.status_code == 503
    assert resp.headers.get("retry-after") == "2"
    body = resp.json()
    assert body == {"error": "redis_not_configured"}


# ── Auth dependency enforced ────────────────────────────────────────


@pytest.mark.asyncio
async def test_auth_dependency_enforced(pool: asyncpg.Pool, redis_client: aioredis.Redis) -> None:
    """Auth dependency that raises HTTPException(401) blocks all routes."""

    def _reject() -> None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    app = _make_app(pool, redis_client, auth_dependency=_reject)
    job_id = await _seed_running_job(pool, progress_seq=1)

    sse_resp = await _get_json(app, f"/jobs/api/job/{job_id}/progress/stream")
    assert sse_resp.status_code == 401

    state_resp = await _get_json(app, f"/jobs/api/job/{job_id}/state")
    assert state_resp.status_code == 401


# ── Invalid job_id format (non-UUID) ────────────────────────────────


@pytest.mark.asyncio
async def test_invalid_job_id_format(pool: asyncpg.Pool, redis_client: aioredis.Redis) -> None:
    """Non-UUID job_id returns HTTP 422 from FastAPI path validation."""
    app = _make_app(pool, redis_client)
    resp = await _get_json(app, "/jobs/api/job/not-a-uuid/progress/stream")
    assert resp.status_code == 422


# ── Missing job_id segment ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_missing_job_id_segment(pool: asyncpg.Pool, redis_client: aioredis.Redis) -> None:
    """GET /jobs/api/job/ (missing job_id) returns HTTP 404 from routing."""
    app = _make_app(pool, redis_client)
    resp = await _get_json(app, "/jobs/api/job/")
    assert resp.status_code == 404


# ── Negative last_event_id ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_negative_last_event_id_is_rejected_with_400(
    pool: asyncpg.Pool,
    redis_client: aioredis.Redis,
) -> None:
    """``?last_event_id=-1`` gets the same 400 the Last-Event-ID header gets.

    Every id this stream issues is a non-negative integer sequence number,
    so a negative cursor cannot have come from it - it is rejected at the
    boundary (naming the query parameter) instead of being read as "no
    cursor" or silently clamped: a cursor the stream never issued would
    poison the generator's dedup and strand the connection as a blackhole.
    """
    job_id = await _seed_running_job(
        pool, progress_seq=3, progress_state={"step": 3}, status="succeeded"
    )

    app = _make_app(pool, redis_client, sse_heartbeat_interval=timedelta(seconds=2))

    resp = await _get_json(app, f"/jobs/api/job/{job_id}/progress/stream?last_event_id=-1")
    assert resp.status_code == 400
    assert "last_event_id" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_in_domain_last_event_id_query_param_catches_up(
    pool: asyncpg.Pool,
    redis_client: aioredis.Redis,
) -> None:
    """A well-formed query cursor still catches up on the real wire.

    Seeds a terminal job (progress_seq=3) so the SSE stream terminates
    naturally after the catch-up + done events.  ``last_event_id=1`` is
    inside the int4 domain, so ``progress_seq=3 > 1`` triggers the
    catch-up event.
    """
    job_id = await _seed_running_job(
        pool, progress_seq=3, progress_state={"step": 3}, status="succeeded"
    )

    app = _make_app(pool, redis_client, sse_heartbeat_interval=timedelta(seconds=2))

    lines = await _collect_sse_lines(
        app,
        f"/jobs/api/job/{job_id}/progress/stream?last_event_id=1",
        overall_timeout=10.0,
    )
    frames = _parse_sse_frames(lines)

    catch_up = [f for f in frames if f.get("id") == "3"]
    assert len(catch_up) == 1, (
        f"Expected exactly one catch-up event id=3 (progress_seq > 1), got {len(catch_up)}: {frames}"
    )


# ── Internal helpers ────────────────────────────────────────────────────────


async def _update_progress(
    pool: asyncpg.Pool,
    job_id: uuid.UUID,
    *,
    progress_seq: int,
    progress_state: dict[str, Any] | None = None,
    status: str = "running",
) -> None:
    ps = progress_state if progress_state is not None else {}
    async with pool.acquire() as conn:
        if status != "running":
            await conn.execute(
                f'UPDATE "{SCHEMA_LABEL}".jobs SET progress_seq = $1, progress_state = $2::jsonb, '
                "status = $3, finished_at = now() WHERE id = $4",
                progress_seq,
                _json_dumps(ps),
                status,
                job_id,
            )
        else:
            await conn.execute(
                f'UPDATE "{SCHEMA_LABEL}".jobs SET progress_seq = $1, progress_state = $2::jsonb '
                f"WHERE id = $3",
                progress_seq,
                _json_dumps(ps),
                job_id,
            )
