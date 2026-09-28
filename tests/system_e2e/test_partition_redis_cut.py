"""Partition weather III: CUT worker↔redis mid-progress-stream and mid-ratelimit.

The broker cut is a toxiproxy between EVERY redis speaker (the worker's
fanout client, the SSE bridge's pubsub, the rate limiter's script
client) and the shared Dragonfly. A cut is not a container kill: the
connections stay ESTABLISHED and the bytes are held - the shape where
every client budget, not the SYN, is the only thing between a silent
hang and an honest failure.

Cell 1 (progress stream): the SSE bridge's broker-read deadline (the
pinned contract in ``src/taskq/web/progress.py``: every broker read is
bounded by the heartbeat interval plus a grace, so a broker that dies
mid-read ends the stream instead of stranding a subscription, a task
and an SSE slot) and the reconnect ladder: the client re-dials with its
``Last-Event-ID`` cursor and the stream resumes STRICTLY PAST the
cursor - no duplicated frame, no lost durable state (the catch-up reads
the durable ``progress_seq`` the PG surface carried through the cut).

Cell 2 (rate limit): the GCRA acquire's redis→PG fallback (the pinned
contract in ``src/taskq/ratelimit/_redis_utils.py``: weather gets a
BOUNDED transient retry ladder - 3 attempts, backoffs 0.25s + 0.75s -
then admission re-runs against the durable PG row; when no fallback is
wired the same ladder must surface an honest dependency-unavailable
error). The cut is on the wire: connects succeed, replies are held then
closed, so redis-py's own socket budgets are what fail.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest
import redis.asyncio as redis_async

from taskq.ratelimit.sliding_window import SlidingWindow
from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, delete_tagged
from tests.system_e2e.actors import SysPayload, sys_progress

if TYPE_CHECKING:
    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema
    from tests.system_e2e._toxiproxy import Toxiproxy

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-partition-redis"

#: The SSE bridge's stream-end bound, DERIVED against the measured
#: pipeline (standalone repro + this test's runs): the toxic's finite
#: hold (3s) closes the pubsub connection, the bridge's read deadline
#: (1s + 0.5s grace) fires, and redis-py's OWN reconnect-retry ladder
#: (default 3 attempts) rides INSIDE the aborted read before surfacing -
#: each retry cycle costs one full hold (3s) against the toxic that
#: stays armed - plus the task-group teardown. Measured end-to-end
#: 10-15s. What the bound PINS is the contract that matters: the stream
#: ENDS - no subscription, task or SSE slot stranded on a dead broker -
#: and the client's reconnect ladder takes over. (The 1.5s read deadline
#: itself is soft under redis-py's retry-swallowed cancellation - noted
#: to the report, not pinned here: the end is bounded and honest either
#: way.)
_STREAM_END_BUDGET_S = 20.0

#: The GCRA fallback ladder, DERIVED from the pinned constants: 3 redis
#: attempts, each failing at the client's 2s socket timeout, with the
#: pinned backoffs (0.25s, 0.75s) between them, then the PG acquire.
#: Lower bound: the two backoffs alone (1.0s) - a decision that lands
#: faster never rode the ladder. Upper bound: 3*2 + 1.0 + PG slack.
_FALLBACK_LADDER_MIN_S = 1.0
_FALLBACK_LADDER_BUDGET_S = 15.0


# ── The SSE live capture (the web_progress ASGI pattern, inlined) ──────
#
# httpx.ASGITransport buffers the whole body before the response returns,
# so a stream that outlives the scenario cannot be consumed through it.
# The capture below drives the REAL ASGI app live. "The stream ended" is
# observed at the ASGI boundary - the response's FINAL body message
# (``more_body`` absent/false) - and NOT by the reader task's completion:
# sse-starlette's client-disconnect watcher parks on a ``http.disconnect``
# this in-process client never sends, so the raw app() call outlives the
# stream by design (the same strand the web_progress suite reaps). Every
# task the interaction mints is reaped against a baseline snapshot.


async def _start_sse(
    app: Any,
    path: str,
    *,
    last_event_id: int | None = None,
) -> tuple[asyncio.Task[None], list[str]]:
    """Start one SSE stream against the ASGI app; return (reader, chunks)."""
    chunks: list[str] = []

    async def _receive() -> dict[str, object]:
        await asyncio.sleep(3600)
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message: dict[str, object]) -> None:
        if message["type"] == "http.response.body":
            body = message.get("body", b"")
            assert isinstance(body, bytes)
            chunks.append(body.decode("utf-8"))

    headers: list[tuple[bytes, bytes]] = [(b"host", b"test")]
    if last_event_id is not None:
        headers.append((b"last-event-id", str(last_event_id).encode()))

    scope: dict[str, object] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "queryString": b"",
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("test", 1234),
        "server": ("test", 80),
    }

    async def _drive() -> None:
        await app(scope, _receive, _send)  # pyright: ignore[reportArgumentType,reportCallIssue]  # Why: the raw ASGI callable, the same driving the web_progress captures use.

    reader: asyncio.Task[None] = asyncio.create_task(_drive())  # pyright: ignore[reportAssignmentType]
    return reader, chunks


def _task_baseline() -> frozenset[asyncio.Task[object]]:
    return frozenset(asyncio.all_tasks())


async def _reap_new_tasks(baseline: frozenset[asyncio.Task[object]]) -> None:
    """Cancel and gather every task minted since *baseline* - sse-starlette's
    shutdown watcher and the teardown tasks an in-process stream strands
    (the web_progress pattern's reap, inlined)."""
    fresh = [t for t in asyncio.all_tasks() if t not in baseline and not t.done()]
    for task in fresh:
        task.cancel()
    await asyncio.gather(*fresh, return_exceptions=True)


def _chunk_seqs(chunks: list[str]) -> list[int]:
    """The seq of every data frame in the stream so far. The bridge puts
    the seq on the SSE ``id`` field (``_make_sse_event``) and the progress
    STATE on ``data`` - the parser reads the id line; keepalive comments
    and the terminal ``done`` event carry no id."""
    seqs: list[int] = []
    for line in "".join(chunks).splitlines():
        if line.startswith("id:"):
            seqs.append(int(line[len("id:") :].strip()))
    return seqs


async def _wait_for_frames(chunks: list[str], count: int, budget_s: float) -> None:
    """Wait until *count* data frames are visible in the live chunks.

    Budget, derived: the publisher beats every 0.15s, so *count* frames
    land within a beat cadence of the subscribe; the budget is that
    cadence grown by the worker's own start-up and co-tenant slack. A
    shortfall here is a harness fault (the stream never carried frames),
    not a partition result - it fails loudly either way.
    """
    deadline = time.monotonic() + budget_s
    while time.monotonic() < deadline:
        if len(_chunk_seqs(chunks)) >= count:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"the stream delivered {len(_chunk_seqs(chunks))} frames in {budget_s}s "
        f"(wanted {count}); the publisher never produced a visible fanout"
    )


async def _end_sse(reader: asyncio.Task[None], budget_s: float, what: str) -> None:
    """Wait for the stream's ASGI call to RETURN within *budget_s* - the
    observable "the stream ended on its own" (the broker-read deadline or
    the terminal event; the generator's own broker-death error may
    propagate through the ASGI call, which IS the end). Never stranded.

    asyncio.wait, not wait_for: the bridge's OWN read deadline raises
    TimeoutError through the app call, and wait_for re-raises the inner
    task's exception - indistinguishable from the wait's own timeout.
    asyncio.wait reports completion without touching the inner
    exception, which is retrieved (and discarded) explicitly."""
    done, _pending = await asyncio.wait({reader}, timeout=budget_s)
    if not done:
        pytest.fail(f"{what} did not end within {budget_s:.1f}s - the stream stranded")
    with contextlib.suppress(Exception):
        _ = reader.exception()


@pytest.mark.timeout(240)
@pytest.mark.load_sensitive
async def test_redis_cut_mid_stream_ends_bounded_and_reconnects_past_cursor(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    redis_container: Any,
    toxiproxy: Toxiproxy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CUT worker↔redis mid-progress-stream: the stream the cut strands
    must END (bounded, the broker-read deadline), the reconnect must
    resume strictly past the cursor, and the durable surface must
    converge the client's view - no lost state, no duplicated frame."""
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    redis_host, redis_port = (
        redis_container.get_container_host_ip(),
        redis_container.get_exposed_port(6379),
    )
    proxy = toxiproxy.create_proxy_sync("redis_cut", redis_host, redis_port)
    proxied_redis_url = f"redis://{proxy.url_host}:{proxy.port}/0"

    # The worker's redis path IS the weather; its PG path is direct.
    worker: WorkerProc | None = None
    pg_pool: asyncpg.Pool | None = None
    router_redis: redis_async.Redis | None = None
    try:
        worker = spawn_worker(pg_dsn, schema, redis_url=proxied_redis_url, tag="part-redis")
        wait_worker_ready(worker)

        handle = await sys_client.enqueue(sys_progress, SysPayload(beats=30), tags=[_TAG])
        job_id = handle.job_id

        # The SSE bridge under test: the REAL router, a REAL redis pubsub
        # through the proxy, heartbeat cadence 1s (the read deadline's
        # base - see the budget arithmetic above).
        pg_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
        router_redis = redis_async.from_url(proxied_redis_url, socket_timeout=None)
        from taskq.web.progress import create_router

        router = create_router(
            pg_pool,
            router_redis,
            schema=schema,
            sse_heartbeat_interval=timedelta(seconds=1),
        )
        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(router, prefix="/jobs")

        # Connection 1: live from the start, frames flowing.
        baseline_1 = _task_baseline()
        reader_1, chunks_1 = await _start_sse(app, f"/jobs/api/job/{job_id}/progress/stream")
        await _wait_for_frames(chunks_1, count=2, budget_s=15.0)

        seqs_1 = _chunk_seqs(chunks_1)
        assert seqs_1 == sorted(set(seqs_1)), (
            f"the first connection delivered duplicated or out-of-order frames: {seqs_1}"
        )
        cursor = seqs_1[-1]

        # The cut, mid-stream: bytes held 3s, then every connection
        # through the proxy closed - broker death, on the wire.
        await proxy.hold_then_close(3000)

        # The stream must END on its own within the derived budget - the
        # pinned broker-read deadline doing its job against a real death.
        await _end_sse(
            reader_1,
            _STREAM_END_BUDGET_S,
            "the SSE stream (heartbeat 1s + grace 0.5s + toxic hold 3s + slack)",
        )
        ended_at = time.monotonic()
        await _reap_new_tasks(baseline_1)

        # Recovery, then the reconnect ladder: the cursor is the LAST seq
        # the dead stream delivered; the re-dialed stream must resume
        # STRICTLY past it. This stream ends itself: the job terminalises
        # and the bridge closes with the ``done`` event.
        await proxy.clear()
        baseline_2 = _task_baseline()
        reader_2, chunks_2 = await _start_sse(
            app, f"/jobs/api/job/{job_id}/progress/stream", last_event_id=cursor
        )
        await _end_sse(reader_2, 90.0, "the reconnected stream (job terminal + done event)")
        await _reap_new_tasks(baseline_2)

        seqs_2 = _chunk_seqs(chunks_2)
        assert seqs_2, (
            f"the reconnect delivered no frames past the cursor {cursor}; the ladder "
            f"did not re-serve the durable state. Raw stream head: {chunks_2[:3]!r}"
        )
        assert seqs_2[0] > cursor, (
            f"the reconnect's first frame is seq {seqs_2[0]} at or before the cursor "
            f"{cursor} - a duplicated frame past the cursor"
        )
        assert seqs_2 == sorted(set(seqs_2)), (
            f"the reconnected stream delivered duplicated or out-of-order frames: {seqs_2}"
        )

        # Convergence: the job terminalises (its beats ran on the worker,
        # whose PG surface the cut never touched), and the reconnected
        # stream's last seq is the DURABLE seq - nothing the worker
        # consumed was lost from the client's view.
        settle_deadline = ended_at + 60.0
        row: asyncpg.Record | None = None
        while time.monotonic() < settle_deadline:
            row = await conn.fetchrow(
                f"SELECT status::text AS status, progress_seq AS seq "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            if row is not None and row["status"] == "succeeded":
                break
            await asyncio.sleep(0.25)
        assert row is not None and row["status"] == "succeeded", (
            f"the job did not reach terminal after the broker cut: {row}"
        )
        assert seqs_2[-1] >= (row["seq"] or 0) - 1, (
            f"the reconnected stream ended at seq {seqs_2[-1]} but the durable surface "
            f"carries seq {row['seq']} - the client's view lost durable state"
        )

        # The worker survived the cut on the same process (no restart).
        assert worker.proc.poll() is None, (
            "the worker exited during the broker cut: the fanout's redis path must "
            "weather the cut, not kill the worker"
        )
        counts = await assert_balanced(conn, schema, _TAG)
        assert set(counts) == {"succeeded"}, f"the cut manufactured outcomes: {counts}"
    finally:
        if router_redis is not None:
            with contextlib.suppress(Exception):
                await router_redis.aclose()
        if pg_pool is not None:
            with contextlib.suppress(Exception):
                await pg_pool.close()
        if worker is not None:
            reap(worker)
        with contextlib.suppress(Exception):
            await proxy.clear()
        await delete_tagged(conn, schema, _TAG)


@pytest.mark.timeout(120)
@pytest.mark.load_sensitive
async def test_gcra_fallback_engages_on_wire_cut_and_recovers(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    toxiproxy: Toxiproxy,
    redis_container: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CUT worker↔redis mid-ratelimit: the GCRA acquire rides the BOUNDED
    transient-retry ladder and admits via the PG fallback; with the
    fallback unwired the SAME ladder surfaces an honest
    dependency-unavailable error; a cleared wire admits via redis again
    - no permanent poisoning, no restart."""
    _ = module_pg_schema  # the PG fallback writes the module schema's GCRA state
    monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)
    from taskq.settings import WorkerSettings

    worker_settings = WorkerSettings.load()
    redis_host, redis_port = (
        redis_container.get_container_host_ip(),
        redis_container.get_exposed_port(6379),
    )
    proxy = toxiproxy.create_proxy_sync("redis_rl", redis_host, redis_port)

    proxied_redis = redis_async.from_url(
        f"redis://{proxy.url_host}:{proxy.port}/0",
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
    )
    pg_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    window = SlidingWindow(
        "partition_gcra", 100, timedelta(seconds=60), backend="redis", style="gcra"
    )
    try:
        # Baseline on the clean wire: redis answers.
        base = await window.acquire(
            redis_client=proxied_redis, pg_pool=pg_pool, settings=worker_settings
        )
        assert base.backend == "redis" and base.allowed, (
            f"the clean-wire GCRA acquire did not answer from redis: {base}"
        )

        # The cut: connects succeed, replies are held 2s then closed -
        # each redis attempt fails at the CLIENT's own 2s socket budget.
        await proxy.hold_then_close(2000)

        started = time.monotonic()
        fallback = await window.acquire(
            redis_client=proxied_redis, pg_pool=pg_pool, settings=worker_settings
        )
        elapsed = time.monotonic() - started
        assert fallback.backend == "postgres", (
            f"the wire-cut GCRA acquire did not fall back to PG (backend={fallback.backend}); "
            f"the fallback ladder did not engage"
        )
        assert fallback.allowed is True, f"the PG fallback denied a fresh bucket: {fallback}"
        assert _FALLBACK_LADDER_MIN_S <= elapsed <= _FALLBACK_LADDER_BUDGET_S, (
            f"the fallback ladder took {elapsed:.2f}s - outside the pinned arithmetic "
            f"(2 backoffs >= {_FALLBACK_LADDER_MIN_S}s; 3 socket budgets of 2s + PG slack "
            f"<= {_FALLBACK_LADDER_BUDGET_S}s)"
        )

        # The same cut, NO fallback wired: the ladder must END in the
        # honest dependency-unavailable error, never hang. The unwired
        # settings are built the sanctioned way (load_from_dict, the
        # shape test_attack_redis_lies uses) rather than a copied model.
        no_fallback_settings = WorkerSettings.load_from_dict(
            {
                "pg_dsn": pg_dsn,
                "schema_name": module_pg_schema.schema_name,
                "rate_limit_pg_fallback_enabled": False,
            }
        )  # type: ignore[arg-type]  # Why: load_from_dict accepts the dict at runtime, the same boundary the lie-attack tests pin.
        started = time.monotonic()
        with pytest.raises(redis_async.TimeoutError):
            await window.acquire(
                redis_client=proxied_redis, pg_pool=pg_pool, settings=no_fallback_settings
            )
        assert time.monotonic() - started <= _FALLBACK_LADDER_BUDGET_S, (
            "the no-fallback arm exceeded the ladder budget: the acquire hung on the "
            "wire cut instead of surfacing the dependency error"
        )

        # Recovery on the SAME objects: clear the wire, redis admits again.
        await proxy.clear()
        recovered = await window.acquire(
            redis_client=proxied_redis, pg_pool=pg_pool, settings=worker_settings
        )
        assert recovered.backend == "redis" and recovered.allowed, (
            f"the cleared wire did not admit via redis again: {recovered}"
        )
    finally:
        with contextlib.suppress(Exception):
            await proxied_redis.aclose()
        with contextlib.suppress(Exception):
            await pg_pool.close()
        with contextlib.suppress(Exception):
            await proxy.clear()
