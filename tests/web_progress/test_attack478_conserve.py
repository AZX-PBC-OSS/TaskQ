# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound.

"""Conservation attacks on the #478 recovery surface: when recovery SKIPS,
is every job's progress state still exactly-once and whole?

The surface is PR 478's diff (reverted): the client recovery skip plus the
stream's reconnect catch-up. The question is conservation - a balancing
counter per scenario: every sequence the durable row carried above a
recovery cursor is delivered to the subscriber EXACTLY ONCE (no duplicate
delivery, no rewind, no blackhole), and a skip leaves no committed side
effect.

The scenarios run against real Postgres, real Dragonfly, and a real
uvicorn socket (in-process ASGI transports buffer the body, so no
mid-stream frame - a disconnect, a mid-stream death - is observable
there; a real socket is the honest transport for this class):

1. reconnect double-delivery: the catch-up snapshot, live deltas, AND a
   replayed frame on one stream - the delivered multiset must have no
   duplicate sequence;
2. a paused (all-commands-blocked) Dragonfly DURING the stream: the read
   deadline ends the stream fail-visible on its own clock, the publish
   inside the pause drops (bounded), the flush lands after the lift, and
   the reconnect delivers every durable sequence above the cursor exactly
   once;
3. the terminal-trailing recovery: the durable row sits BELOW the client's
   cursor (the deltas were seen on the wire but never flushed, the attempt
   died, the reclaim wrote its own seq) - the recovery must deliver
   nothing spurious, never rewind, and not blackhole the live events that
   follow. This is the server-side fact that makes the corrected client's
   poll path the only truthful restorer of the terminal state.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Any

import asyncpg
import httpx
import pytest
import pytest_asyncio

# Why: the guards live at this file's one seam, before the imports that
# need the stack (uvicorn here, fastapi transitively through the
# test_integration scaffolding) - a leg without the fastapi extra skips
# the module instead of failing collection with ModuleNotFoundError.
pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")
pytest.importorskip("uvicorn")

import redis.asyncio as aioredis
import uvicorn

from taskq.constants import progress_channel
from taskq.migrate import apply_pending
from tests.web_progress.test_integration import (  # pyright: ignore[reportPrivateUsage]  # Why: the conservation attacks reuse the integration suite's app/seed scaffolding verbatim rather than forking it.
    SCHEMA_LABEL,  # pyright: ignore[reportPrivateUsage]
    _make_app,  # pyright: ignore[reportPrivateUsage]
    _progress_event,  # pyright: ignore[reportPrivateUsage]
    _reap_stream_teardown_tasks,  # pyright: ignore[reportPrivateUsage]
    _seed_running_job,  # pyright: ignore[reportPrivateUsage]
    _update_progress,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = [pytest.mark.integration, pytest.mark.redis]


# ── Real-server fixture: one uvicorn socket per test ──────────────────────


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


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[aioredis.Redis]:
    client = aioredis.from_url(redis_url)
    try:
        yield client
    finally:
        await client.aclose()


# ── Real-server fixture: one uvicorn socket per test ──────────────────────


@pytest_asyncio.fixture
async def server(pool: asyncpg.Pool, redis_client: aioredis.Redis) -> AsyncIterator[str]:
    app = _make_app(pool, redis_client, sse_heartbeat_interval=timedelta(seconds=1))
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    # The test loop owns the signal discipline; uvicorn must not re-arm it.
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]  # Why: the instance attribute shadows the method; serve() calls it unconditionally.
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.05)
    assert server.started, "uvicorn never came up"
    port = server.servers[0].sockets[0].getsockname()[1]  # pyright: ignore[reportOptionalSubscript]  # Why: started implies a bound socket.
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


# ── Stream reader: real socket, frames kept even on mid-stream death ──────


async def _stream_frames(
    base_url: str,
    path: str,
    *,
    until: Callable[[list[dict[str, str]]], bool],
    overall_timeout: float = 15.0,
) -> tuple[list[dict[str, str]], float, BaseException | None]:
    """Read one SSE stream to a stop condition, EOF, or death.

    The return carries the frames read, the elapsed time, and the
    exception that ended the read (``None`` = clean EOF or the ``until``
    predicate firing, both a controlled close). Every exit reaps the
    abandoned-``aiter_lines`` finalization cascade and sse-starlette's
    parked watcher INSIDE the call (the same discipline the integration
    suite's collector applies): a ``until`` break suspends the httpx
    generator chain, and CPython finalizes it one wave per generator -
    the module loop must be clean at test end.
    """
    frames: list[dict[str, str]] = []
    failure: BaseException | None = None
    loop = asyncio.get_running_loop()
    started = loop.time()
    baseline: frozenset[asyncio.Task[object]] = frozenset(asyncio.all_tasks())

    async def _read() -> None:
        nonlocal failure
        current: dict[str, str] = {}
        async with httpx.AsyncClient(base_url=base_url) as client:
            try:
                async with client.stream("GET", path, timeout=httpx.Timeout(5.0, read=8.0)) as resp:
                    async for line in resp.aiter_lines():
                        if line == "":
                            if current:
                                frames.append(current)
                                current = {}
                                if until(frames):
                                    return
                        elif line.startswith(": "):
                            pass  # keepalive comment
                        elif ":" in line:
                            key, _, value = line.partition(":")
                            current[key.strip()] = value.strip()
            except Exception as exc:
                failure = exc

    try:
        await asyncio.wait_for(_read(), timeout=overall_timeout)
    except (TimeoutError, ExceptionGroup) as exc:
        failure = exc
    finally:
        await _reap_stream_teardown_tasks(baseline)
    return frames, loop.time() - started, failure


def _delivered_seq_multiset(frames: list[dict[str, str]]) -> Counter[int]:
    """The balancing counter: how many times each sequence was delivered."""
    return Counter(
        int(frame["id"]) for frame in frames if frame.get("event") in ("progress", "terminal")
    )


def _pub(job_id: uuid.UUID, seq: int, **kw: Any) -> str:
    return _progress_event(job_id=job_id, seq=seq, **kw).model_dump_json(exclude_none=True)


@pytest.mark.asyncio
async def test_reconnect_delivers_every_sequence_exactly_once(
    pool: asyncpg.Pool, redis_client: aioredis.Redis, server: str
) -> None:
    """Catch-up + live + a replayed frame: no sequence delivered twice."""
    job_id = await _seed_running_job(pool, progress_seq=3, progress_state={"step": 3})
    channel = progress_channel(SCHEMA_LABEL, job_id)

    async def _wire() -> None:
        await asyncio.sleep(0.3)
        # The flush lands the seq-4 delta, the seq-5 delta is on the wire
        # only, and a transport replay of seq 4 arrives a second time.
        await _update_progress(pool, job_id, progress_seq=4, progress_state={"step": 4})
        await redis_client.publish(channel, _pub(job_id, 5, step=5))
        await redis_client.publish(channel, _pub(job_id, 4, step=4))

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_wire())
        frames, _elapsed, _failure = await _stream_frames(
            server,
            f"/jobs/api/job/{job_id}/progress/stream?last_event_id=1",
            until=lambda fs: _delivered_seq_multiset(fs).total() >= 2,
        )

    # The catch-up (3), the live delta (5); the replayed 4 is dropped.
    assert _delivered_seq_multiset(frames) == Counter({3: 1, 5: 1}), frames
    row = await pool.fetchrow(
        f'SELECT progress_seq FROM "{SCHEMA_LABEL}".jobs WHERE id = $1', job_id
    )
    assert row is not None and row["progress_seq"] == 4


@pytest.mark.asyncio
async def test_paused_broker_drops_the_fanout_and_recovery_restores_exactly_once(
    pool: asyncpg.Pool, redis_client: aioredis.Redis, redis_url: str, server: str
) -> None:
    """Dragonfly paused mid-stream: the fanout drops, recovery heals exactly.

    On this broker a CLIENT PAUSE ALL starves the WRITE side - every
    publish inside the window blocks past its bound and drops - while the
    subscribed connection's read keeps its keepalive cadence, so the
    stream survives. The conservation question is then: what does the
    survivor miss, and does the recovery restore it exactly once? The
    deltas whose publishes dropped exist only in PG (the flush is a PG
    write); the reconnect at the pre-pause cursor must deliver the
    durable state exactly once, and the surviving connection must never
    deliver a sequence twice.
    """
    job_id = await _seed_running_job(pool, progress_seq=3, progress_state={"step": 3})
    channel = progress_channel(SCHEMA_LABEL, job_id)
    # The admin connection rides the SAME broker the stream and the
    # publisher use; the pause must reach every client of it.
    admin = aioredis.from_url(redis_url)
    try:
        # Healthy start: the snapshot (seq 3), then one delta whose flush
        # lands. ONE SECOND IN, the broker stops serving writes.
        pause_fired = asyncio.Event()

        async def _wire() -> None:
            await asyncio.sleep(0.3)
            await redis_client.publish(channel, _pub(job_id, 4, step=4))
            await _update_progress(pool, job_id, progress_seq=4, progress_state={"step": 4})
            await asyncio.sleep(0.7)
            await admin.execute_command("CLIENT", "PAUSE", "6000", "ALL")
            pause_fired.set()
            # The fanout inside the pause: the publish cannot complete and
            # the drop is bounded (never an unbounded publisher wait).
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(2.0):
                    await redis_client.publish(channel, _pub(job_id, 5, step=5))

        wire_task = asyncio.create_task(_wire())

        # The surviving connection: delivered 3 and 4 before the pause,
        # alive through it (keepalives); the "dropped" publish of seq 5
        # is a DELAY, not a loss - the broker executes the queued command
        # the moment the pause lifts - and seq 7 resumes after it.
        reader = asyncio.create_task(
            _stream_frames(
                server,
                f"/jobs/api/job/{job_id}/progress/stream",
                until=lambda fs: _delivered_seq_multiset(fs).total() >= 4,
                overall_timeout=15.0,
            )
        )
        # Let the survivor connect and receive the snapshot and the seq-4
        # delta before anything else moves.
        await asyncio.sleep(0.8)
        # The flushes ride PG, not the broker: seq 6 lands durably INSIDE
        # the pause window (its wire publish never happens).
        await _update_progress(pool, job_id, progress_seq=6, progress_state={"step": 6})
        await wire_task
        assert pause_fired.is_set()

        # The pause has expired (6 s from ~1.0 s; the wire task's bounded
        # publish ended at ~3 s); the fanout resumes. The flush that
        # outlived the pause is at seq 7, published to the wire.
        await asyncio.sleep(4.5)
        await _update_progress(pool, job_id, progress_seq=7, progress_state={"step": 7})
        await redis_client.publish(channel, _pub(job_id, 7, step=7))

        frames, _elapsed, _failure = await reader

        # The pause delayed the fanout but lost nothing: the survivor
        # delivered every published sequence - 3, 4, the delayed 5, the
        # post-pause 7 - each EXACTLY ONCE, never a duplicate.
        assert _delivered_seq_multiset(frames) == Counter({3: 1, 4: 1, 5: 1, 7: 1}), frames

        reconnect, _elapsed, _failure = await _stream_frames(
            server,
            f"/jobs/api/job/{job_id}/progress/stream?last_event_id=4",
            until=lambda fs: _delivered_seq_multiset(fs).total() >= 1,
            overall_timeout=10.0,
        )
        # The recovery at the pre-pause cursor restores the durable state
        # (seq 7 via the catch-up) exactly once: no duplicate delivery.
        assert _delivered_seq_multiset(reconnect) == Counter({7: 1}), reconnect
        row = await pool.fetchrow(
            f'SELECT progress_seq FROM "{SCHEMA_LABEL}".jobs WHERE id = $1', job_id
        )
        assert row is not None and row["progress_seq"] == 7
    finally:
        await admin.aclose()


@pytest.mark.asyncio
async def test_recovery_below_the_cursor_delivers_nothing_spurious_and_does_not_blackhole(
    pool: asyncpg.Pool, redis_client: aioredis.Redis, server: str
) -> None:
    """A durable row BELOW the recovery cursor: silent above it, live after.

    The subscriber saw deltas 5 and 6 on the wire but the flushes were
    lost with the paused broker; the attempt died and the reclaim wrote
    its own seq 4. The reconnect at cursor 6 must neither replay nor
    rewind - and must keep the stream alive for the events that follow.
    """
    job_id = await _seed_running_job(pool, progress_seq=4, progress_state={"step": 4})
    await _update_progress(
        pool,
        job_id,
        progress_seq=4,
        progress_state={"percent": 90, "detail": "reclaimed"},
        status="crashed",
    )
    channel = progress_channel(SCHEMA_LABEL, job_id)

    async def _wire() -> None:
        await asyncio.sleep(0.3)
        # A replay of what the subscriber already saw below the cursor...
        await redis_client.publish(channel, _pub(job_id, 5, step=5))
        await redis_client.publish(channel, _pub(job_id, 6, step=6))
        # ...then the live stream continues above it, and the retry
        # attempt's terminal lands durably.
        await _update_progress(
            pool,
            job_id,
            progress_seq=8,
            progress_state={"percent": 100},
            status="succeeded",
        )
        await redis_client.publish(channel, _pub(job_id, 7, step=7))
        await redis_client.publish(channel, _pub(job_id, 8, terminal=True, status="succeeded"))

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_wire())
        # Read to EXHAUSTION: the terminal stream EOFs by itself right
        # after `done`, so the chain closes itself and nothing is left
        # for the finalizer cascade.
        frames, _elapsed, _failure = await _stream_frames(
            server,
            f"/jobs/api/job/{job_id}/progress/stream?last_event_id=6",
            until=lambda _fs: False,
        )

    # No replay of 5/6, no rewind to the durable 4; 7 and the terminal 8
    # flow - the recovery is silent below the cursor, never a blackhole.
    assert _delivered_seq_multiset(frames) == Counter({7: 1, 8: 1}), frames
    events = [f["event"] for f in frames if "event" in f]
    assert events[-2:] == ["terminal", "done"], events
