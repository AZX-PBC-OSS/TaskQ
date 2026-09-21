"""Progress e2e - progress observable by resuming and subscribing consumers.

Scenario:
``generate_report`` 4 stages → progress reaches 100% and is observable
through the library's public streaming surfaces; pub/sub verified by
subscribing to the **global** progress channel
(``progress_global_channel(schema)``) **before** enqueueing - the
per-job channel is unknowable pre-enqueue and pub/sub drops late subscribers -
then filtering events by ``job_id``.

Asserted behavior is read from the library's public surfaces, not guessed:

- Resume contract (persistence): a consumer that reconnects with a cursor
  behind the durable snapshot receives exactly one catch-up terminal event
  carrying the job's full accumulated state - the snapshot the endpoint
  reads from the ``jobs`` row's ``progress_state`` jsonb - and the stream
  closes with ``done`` (``web/progress.py``; docs/guides/progress.md,
  "Reconnect semantics").
- Fanout: every subscriber on the global channel receives the identical
  event sequence; within a worker's lifetime no event's ``seq`` repeats and
  the terminal event strictly follows every event before it (the seq total
  order, docs/guides/progress.md; pinned at unit level in
  tests/test_progress_seq_total_order.py).
- Wire payload: ``taskq.progress.ProgressEvent`` serialised with
  ``exclude_none=True`` (``progress/_publish.py``); the global fanout channel
  name comes from ``taskq.constants.progress_global_channel`` and fanout is on
  by default (``WorkerSettings.progress_publish_global``).

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import socket
import subprocess
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pytest

from taskq.constants import progress_global_channel
from taskq.progress import ProgressEvent

from ._assertions import poll_until
from .actors import GenerateReportPayload, generate_report

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2EDragonfly, E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


def _report_payload(run_id: str) -> GenerateReportPayload:
    """4 stages x 300 ms - long enough for mid-run observation, short for e2e."""
    return GenerateReportPayload(
        run_id=run_id,
        report_id=f"r-{run_id[:8]}",
        stages=4,
        stage_latency_ms=300,
    )


async def test_progress_persisted_to_pg(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_schema: E2ESchema,
    e2e_dragonfly: E2EDragonfly,
    run_id: str,
) -> None:
    """A consumer resuming after completion observes the durable snapshot.

    Persistence is proven through the documented resume contract
    (docs/guides/progress.md, "Reconnect semantics"): a client that
    reconnects with a cursor behind the durable snapshot receives exactly
    one catch-up terminal event - the accumulated state snapshot the
    endpoint reads from the ``jobs`` row - and the stream closes with
    ``done``. The catch-up payload is therefore the persisted state
    observed through the public SSE API; no internal column is read.

    ``handle.wait()`` returns only after the terminal write commits, so the
    post-wait resume is deterministic - no polling required.
    """
    proc, client, _port = await _spawn_admin_sse(e2e_schema, e2e_dragonfly)
    try:
        handle = await e2e_client.enqueue(generate_report, _report_payload(run_id))
        await handle.wait(timeout=60)

        # Arrangement: the resuming consumer's cursor. 0 = the client holds
        # no events, so the durable snapshot is strictly ahead of it and the
        # documented catch-up must fire instead of the stream hanging.
        resume_cursor = "0"

        sse_url = f"/admin/jobs/api/job/{handle.job_id}/progress/stream"
        blocks: list[dict[str, str]] = []
        current: dict[str, str] = {}
        async with asyncio.timeout(30):
            async with client.stream(
                "GET", sse_url, headers={"Last-Event-ID": resume_cursor}
            ) as resp:
                assert resp.status_code == 200, f"SSE endpoint returned {resp.status_code}"
                assert resp.headers.get("content-type", "").startswith("text/event-stream"), (
                    f"expected text/event-stream, got {resp.headers.get('content-type')}"
                )
                async for line in resp.aiter_lines():
                    for prefix, key in (
                        ("event: ", "event"),
                        ("id: ", "id"),
                        ("data: ", "data"),
                    ):
                        if line.startswith(prefix):
                            current[key] = line[len(prefix) :]
                            break
                    if current.get("event") == "done":
                        blocks.append(current)
                        current = {}
                        break
                    if line == "" and current:
                        blocks.append(current)
                        current = {}

        # The catch-up fired: exactly one terminal event, then the close - a
        # hung stream (no catch-up for a cursor behind the snapshot) or a
        # dribble of extra events is a resume-contract regression.
        assert [block["event"] for block in blocks] == ["terminal", "done"], (
            f"a resuming consumer must receive one catch-up terminal then done; got {blocks}"
        )

        # The snapshot carries the job's full accumulated state - the
        # 4-stage report's final stage - not a partial intermediate.
        terminal = blocks[0]
        assert json.loads(terminal["data"]) == {
            "step": 4,
            "percent": 100.0,
            "detail": "stage 4 store",
        }

        # The catch-up never rewinds the consumer: its event id is strictly
        # ahead of the cursor it resumed from.
        assert int(terminal["id"]) > int(resume_cursor)

    finally:
        await client.aclose()
        await asyncio.to_thread(_shutdown_admin, proc)


async def test_progress_fanout_pubsub(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_schema: E2ESchema,
    e2e_dragonfly: E2EDragonfly,
    run_id: str,
) -> None:
    """Every subscriber on the global channel receives the same ordered events.

    SUBSCRIBE-first is mandatory: Dragonfly pub/sub drops messages published
    before the subscription registers server-side. Both subscribers' subscribe
    acks are read back explicitly so the enqueue cannot race registration.
    Events are filtered by ``job_id`` (not ``run_id``): the wire payload is a
    ``ProgressEvent``, which carries no ``run_id``.

    Two independent subscribers register before the enqueue and each collects
    this job's events. Asserted behavior (docs/guides/progress.md):

    - fanout equivalence: both subscribers receive the identical event
      sequence - same payloads, same order;
    - no duplicates and no rewinds: within a worker's lifetime no event's
      ``seq`` repeats, and the terminal event's ``seq`` is strictly greater
      than every event before it, so a seq-cursor consumer (the
      ``Last-Event-ID`` discipline) neither drops the terminal nor sees
      state go backwards;
    - the terminal event is a ``succeeded`` state_change carrying the job's
      full accumulated 4-stage state - the observable record of what the
      terminal write merged.

    Resilience (F3): a dropped pub/sub socket under container resource
    pressure is retried with a fresh SUBSCRIBE inside an overall 90 s
    deadline - pub/sub events missed during the reconnect window are lost
    (fire-and-forget), but the fanout must still deliver the terminal event
    afterwards. A stalled listen (``TimeoutError``) FAILS the test: a fanout
    that stops delivering is a regression signal, not a skip-shaped pass.
    """
    import redis.asyncio as redis_async

    channel = progress_global_channel(e2e_schema.schema_name)
    url = f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}"
    redis_clients = [redis_async.from_url(url, decode_responses=False) for _ in range(2)]
    received: list[list[ProgressEvent]] = [[], []]
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 90.0

        # SUBSCRIBE-first is mandatory: both pubsubs register (acks read back
        # explicitly) before the enqueue can race them server-side.
        pubsubs = [client.pubsub() for client in redis_clients]
        for pubsub in pubsubs:
            await pubsub.subscribe(channel)
        for pubsub in pubsubs:
            async with asyncio.timeout(10):
                while True:
                    ack = await pubsub.get_message(ignore_subscribe_messages=False, timeout=1.0)
                    if ack is not None and ack["type"] == "subscribe":
                        break

        handle = await e2e_client.enqueue(generate_report, _report_payload(run_id))

        async def drain(index: int, pubsub: Any) -> None:
            """Collect this job's events for subscriber *index* until terminal.

            A dropped socket (F3) re-subscribes on a fresh connection and
            keeps listening within the overall deadline; a stalled listen
            raises ``TimeoutError`` and fails the test. The first pass
            listens on the caller's already-subscribed pubsub.
            """
            terminal_seen = False
            while not terminal_seen:
                try:
                    async with asyncio.timeout(max(0.0, deadline - loop.time())):
                        async for msg in pubsub.listen():
                            if msg.get("type") != "message":
                                continue
                            event = ProgressEvent.model_validate_json(msg["data"])
                            if event.job_id != handle.job_id:
                                continue
                            received[index].append(event)
                            if event.terminal:
                                terminal_seen = True
                                break
                except redis_async.ConnectionError:
                    # F3 transport flake: dropped pub/sub socket - resubscribe
                    # and keep listening within the overall deadline.
                    pass
                finally:
                    await pubsub.aclose()
                if not terminal_seen:
                    pubsub = redis_clients[index].pubsub()
                    await pubsub.subscribe(channel)

        try:
            async with asyncio.TaskGroup() as task_group:
                for index, pubsub in enumerate(pubsubs):
                    task_group.create_task(drain(index, pubsub))
        finally:
            for pubsub in pubsubs:
                await pubsub.aclose()

        await handle.wait(timeout=60)
    finally:
        for redis_client in redis_clients:
            await redis_client.aclose()

    events_a, events_b = received

    # Fanout proof - unconditional: each drain loop only exits with the
    # terminal event in hand (a stall raises TimeoutError and fails above).
    progress_events = [event for event in events_a if event.kind == "progress"]
    assert progress_events, (
        f"expected >= 1 progress event for job {handle.job_id} on {channel!r}; "
        f"received {[(event.kind, event.seq) for event in events_a]}"
    )
    assert all(event.actor == "generate_report" for event in events_a)

    # Fanout equivalence: both subscribers received the identical sequence -
    # same payloads, same order.
    assert [event.model_dump_json(exclude_none=True) for event in events_a] == [
        event.model_dump_json(exclude_none=True) for event in events_b
    ], (
        f"subscribers on {channel!r} must receive the same events in the same "
        f"order; got {[(e.kind, e.seq) for e in events_a]} vs "
        f"{[(e.kind, e.seq) for e in events_b]}"
    )

    # No duplicates and no rewinds: within a worker's lifetime every event's
    # seq is unique and strictly increasing, so a seq-cursor consumer sees
    # every event exactly once and never state going backwards.
    seqs = [event.seq for event in events_a]
    assert seqs == sorted(set(seqs)), f"fanout seq duplicates or rewinds: {seqs}"

    # The terminal strictly follows everything before it: a consumer that
    # dedupes by seq can never drop it or mistake it for a duplicate.
    terminal = events_a[-1]
    assert all(terminal.seq > seq for seq in seqs[:-1]), f"terminal seq rewinds: {seqs}"
    assert terminal.kind == "state_change"
    assert terminal.terminal is True
    assert terminal.status == "succeeded"


# ── Progress SSE stream via admin server ──────────────────────────────────

_SSE_REPO_ROOT = Path(__file__).resolve().parents[2]
_SSE_ADMIN_ENTRY = _SSE_REPO_ROOT / "tests" / "e2e" / "admin_entry.py"
# Bind-and-release port selection loses a TOCTOU race now and then under
# parallel load; a child that exits early with "address already in use" gets
# this many total attempts, each on a fresh port.
_SSE_MAX_BIND_ATTEMPTS = 3


def _sse_free_port() -> int:
    """Ephemeral host port for the SSE admin server subprocess.

    Bind-and-release: a lost race is detected by the caller (child exits
    early with "address already in use") and retried on a fresh port.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _shutdown_admin(proc: subprocess.Popen[str]) -> str:
    """SIGTERM, escalate to SIGKILL on timeout, then drain captured output.

    ``communicate`` waits for process exit and returns everything buffered in
    the stdout pipe (stderr is merged into it at spawn). Repeated calls after
    process death are safe: CPython caches the drained buffers.
    """
    if proc.poll() is None:
        proc.terminate()
    try:
        out, _ = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    return out.strip()


def _sse_admin_readiness(
    proc: subprocess.Popen[str], client: httpx.AsyncClient
) -> Callable[[], Awaitable[bool]]:
    """Readiness predicate for one spawned SSE admin server.

    Fail-fast with captured logs when the child exits during startup (the
    caller classifies "address already in use" for a fresh-port retry);
    otherwise probe ``/admin/jobs/count`` with the bearer token.
    """

    async def _ready() -> bool:
        if proc.poll() is not None:
            logs = await asyncio.to_thread(_shutdown_admin, proc)
            msg = f"admin server exited during startup (rc={proc.returncode})\n{logs}"
            raise RuntimeError(msg)
        try:
            resp = await client.get("/admin/jobs/count")
        except httpx.HTTPError:
            return False
        return resp.status_code == 200

    return _ready


async def _spawn_admin_sse(
    e2e_schema: E2ESchema,
    e2e_dragonfly: E2EDragonfly,
) -> tuple[subprocess.Popen[str], httpx.AsyncClient, int]:
    """Spawn one admin server subprocess with the Redis progress bridge.

    The port is chosen bind-and-release (TOCTOU window: another process can
    win it before the child binds), so a child that exits early with
    "address already in use" is retried on a fresh port, up to
    ``_SSE_MAX_BIND_ATTEMPTS`` attempts. Returns the child process, an
    authorized client bound to it, and the chosen port; the caller owns
    both (``_shutdown_admin`` / ``aclose``).
    """
    token = secrets.token_hex(16)
    python_path = os.environ.get("PYTHONPATH")
    redis_url = f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}"

    for attempt in range(1, _SSE_MAX_BIND_ATTEMPTS + 1):
        port = _sse_free_port()
        env = {
            **os.environ,
            "TASKQ_PG_DSN": e2e_schema.host_dsn,
            "TASKQ_SCHEMA_NAME": e2e_schema.schema_name,
            "TASKQ_E2E_ADMIN_TOKEN": token,
            "TASKQ_E2E_ADMIN_PORT": str(port),
            "TASKQ_E2E_REDIS_URL": redis_url,
            "PYTHONPATH": (
                str(_SSE_REPO_ROOT)
                if not python_path
                else f"{_SSE_REPO_ROOT}{os.pathsep}{python_path}"
            ),
        }
        proc = await asyncio.to_thread(
            lambda env=env: subprocess.Popen(  # noqa: S603
                [sys.executable, str(_SSE_ADMIN_ENTRY)],
                cwd=str(_SSE_REPO_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        )
        client = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=httpx.Timeout(connect=10.0, read=90.0, write=10.0, pool=10.0),
        )

        try:
            try:
                await poll_until(
                    _sse_admin_readiness(proc, client),
                    timeout=30.0,
                    description=f"admin server readiness at {client.base_url}",
                )
            except TimeoutError:
                logs = await asyncio.to_thread(_shutdown_admin, proc)
                msg = f"admin server not ready within 30s\n{logs}"
                raise RuntimeError(msg) from None
        except RuntimeError as exc:
            await client.aclose()
            if "address already in use" in str(exc).lower() and attempt < _SSE_MAX_BIND_ATTEMPTS:
                continue  # lost the bind race - fresh port, fresh child
            raise
        return proc, client, port

    raise AssertionError("unreachable: the bind-retry loop always returns or raises")


async def test_progress_sse_stream(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    e2e_dragonfly: E2EDragonfly,
    run_id: str,
) -> None:
    """Subscribe to the progress SSE endpoint and verify progress events
    arrive in real-time.

    The admin server (``admin_entry.py``) mounts the progress router at
    ``/jobs`` which exposes ``GET /jobs/api/job/{job_id}/progress/stream``
    (``web/progress.py``). The SSE endpoint subscribes to the per-job
    Redis pub/sub channel BEFORE reading the PG snapshot, so no events
    are lost between the snapshot and the subscription.

    The test spawns the admin server with Redis configured (via
    ``TASKQ_E2E_REDIS_URL``), enqueues a ``generate_report`` job, and
    opens an SSE connection to the job's progress stream. Progress
    events (``event: progress``) and a terminal event (``event: terminal``)
    must arrive before the stream closes with ``event: done``.

    Subscribe-before-enqueue is used for the SSE endpoint (unlike the
    pub/sub test above which subscribes to the global channel before
    enqueue): the SSE endpoint subscribes to the per-job channel, which
    requires the job_id. So the test enqueues first, then opens the SSE
    stream. The SSE handler subscribes Redis BEFORE reading the PG
    snapshot, so events published between the enqueue and the subscribe
    are captured by the initial PG snapshot read.
    """
    proc, client, port = await _spawn_admin_sse(e2e_schema, e2e_dragonfly)
    try:
        handle = await e2e_client.enqueue(generate_report, _report_payload(run_id))
        await handle.wait(timeout=60)

        sse_url = f"/admin/jobs/api/job/{handle.job_id}/progress/stream"
        events: list[str] = []
        terminal_seen = False

        # The job has already completed, so the SSE handler's initial PG
        # snapshot will show a terminal state. The handler emits a
        # terminal event followed by done, then closes the stream.
        sse_client = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": client.headers["Authorization"]},
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
        )
        try:
            async with asyncio.timeout(30):
                async with sse_client.stream("GET", sse_url) as resp:
                    assert resp.status_code == 200, f"SSE endpoint returned {resp.status_code}"
                    assert resp.headers.get("content-type", "").startswith("text/event-stream"), (
                        f"expected text/event-stream, got {resp.headers.get('content-type')}"
                    )
                    async for line in resp.aiter_lines():
                        if line.startswith("event: "):
                            events.append(line[len("event: ") :])
                        if line == "event: done":
                            terminal_seen = True
                            break
        finally:
            await sse_client.aclose()

        assert terminal_seen, f"SSE stream closed without 'done' event; events seen: {events}"
        assert "terminal" in events, f"expected a 'terminal' SSE event; events seen: {events}"

    finally:
        await client.aclose()
        await asyncio.to_thread(_shutdown_admin, proc)
