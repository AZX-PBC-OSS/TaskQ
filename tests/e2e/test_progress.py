"""Progress e2e — progress persisted to PG and fanned out over Redis pub/sub.

Scenario:
``generate_report`` 4 stages → ``progress_state``/``progress_seq`` reach 100%
via ``e2e_pg_pool``; pub/sub verified by subscribing to the **global** progress
channel (``progress_global_channel(schema)``) **before** enqueueing — the
per-job channel is unknowable pre-enqueue and pub/sub drops late subscribers —
then filtering events by ``job_id``.

Asserted values are read from the library, not guessed:

- Progress columns: ``progress_state jsonb`` / ``progress_seq int`` on
  ``{schema}.jobs`` (migrations/01.00.00_01_pre_initial.sql).
- Final persisted values: the consumer success path flushes the coalesce
  buffer immediately and the terminal write SETs ``progress_seq`` while
  merging the accumulated state (``worker/_consumer.py`` →
  ``_seq_and_state_after_flush_attempt`` → ``mark_succeeded``), so a 4-stage
  report lands at seq 4 with step=4 / percent=100.0 / detail="stage 4 store".
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
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from taskq.constants import progress_global_channel
from taskq.progress import ProgressEvent

from .actors import GenerateReportPayload, ReportResult, generate_report

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.client import JobHandle

    from .conftest import E2EDragonfly, E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


def _report_payload(run_id: str) -> GenerateReportPayload:
    """4 stages x 300 ms — long enough for mid-run observation, short for e2e."""
    return GenerateReportPayload(
        run_id=run_id,
        report_id=f"r-{run_id[:8]}",
        stages=4,
        stage_latency_ms=300,
    )


async def test_progress_persisted_to_pg(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """4-stage report → ``jobs.progress_state`` at 100% with ``progress_seq == 4``.

    ``handle.wait()`` returns only after the terminal write commits, so the
    post-wait read is deterministic — no polling required. asyncpg returns
    JSONB as ``str``, so ``progress_state`` is parsed before asserting.
    """
    handle = await e2e_client.enqueue(generate_report, _report_payload(run_id))
    await handle.wait(timeout=60)

    row = await e2e_pg_pool.fetchrow(
        f"""
        SELECT progress_state, progress_seq
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )

    assert row is not None
    assert row["progress_seq"] == 4
    state: dict[str, object] = json.loads(row["progress_state"])
    assert state == {"step": 4, "percent": 100.0, "detail": "stage 4 store"}


async def test_progress_fanout_pubsub(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    e2e_dragonfly: E2EDragonfly,
    run_id: str,
) -> None:
    """A pre-enqueue subscriber on the global channel receives this job's events.

    SUBSCRIBE-first is mandatory: Dragonfly pub/sub drops messages published
    before the subscription registers server-side. The subscribe ack is read
    back explicitly so the enqueue cannot race registration. Events are
    filtered by ``job_id`` (not ``run_id``): the wire payload is a
    ``ProgressEvent``, which carries no ``run_id``.

    Resilience (F3): a dropped pub/sub socket under container resource
    pressure is retried with a fresh SUBSCRIBE inside an overall 90 s
    deadline — pub/sub events missed during the reconnect window are lost
    (fire-and-forget), but the fanout must still deliver the terminal event
    afterwards. A stalled listen (``TimeoutError``) FAILS the test: a fanout
    that stops delivering is a regression signal, not a skip-shaped pass.
    The PG ground-truth assertion (``progress_state``/``progress_seq``)
    always runs as the authority on progress persistence.
    """
    import redis.asyncio as redis_async

    channel = progress_global_channel(e2e_schema.schema_name)
    url = f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}"
    redis_client = redis_async.from_url(url, decode_responses=False)
    received: list[ProgressEvent] = []
    handle: JobHandle[ReportResult] | None = None
    terminal_seen = False
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 90.0
        while not terminal_seen:
            pubsub = redis_client.pubsub()
            try:
                await pubsub.subscribe(channel)
                # SUBSCRIBE-first is mandatory: read the ack back explicitly
                # so the enqueue cannot race registration server-side.
                async with asyncio.timeout(10):
                    while True:
                        ack = await pubsub.get_message(ignore_subscribe_messages=False, timeout=1.0)
                        if ack is not None and ack["type"] == "subscribe":
                            break

                if handle is None:
                    handle = await e2e_client.enqueue(generate_report, _report_payload(run_id))

                async with asyncio.timeout(max(0.0, deadline - loop.time())):
                    async for msg in pubsub.listen():
                        if msg.get("type") != "message":
                            continue
                        event = ProgressEvent.model_validate_json(msg["data"])
                        if event.job_id != handle.job_id:
                            continue
                        received.append(event)
                        if event.terminal:
                            terminal_seen = True
                            break
            except redis_async.ConnectionError:
                # F3 transport flake: dropped pub/sub socket — resubscribe
                # and keep listening within the overall deadline.
                continue
            finally:
                await pubsub.aclose()
    finally:
        await redis_client.aclose()

    assert handle is not None

    await handle.wait(timeout=60)

    # PG ground truth (authority): the terminal progress write landed.
    row = await e2e_pg_pool.fetchrow(
        f"""
        SELECT progress_state, progress_seq
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert row is not None
    assert row["progress_seq"] == 4
    state: dict[str, object] = json.loads(row["progress_state"])
    assert state == {"step": 4, "percent": 100.0, "detail": "stage 4 store"}

    # Fanout proof — unconditional: the listen loop only exits with the
    # terminal event in hand (a stall raises TimeoutError and fails above).
    progress_events = [event for event in received if event.kind == "progress"]
    assert progress_events, (
        f"expected >= 1 progress event for job {handle.job_id} on {channel!r}; "
        f"received {[(event.kind, event.seq) for event in received]}"
    )
    assert all(event.actor == "generate_report" for event in received)
    assert received[-1].kind == "state_change"
    assert received[-1].terminal is True
    assert received[-1].status == "succeeded"


# ── Progress SSE stream via admin server ──────────────────────────────────

_SSE_REPO_ROOT = Path(__file__).resolve().parents[2]
_SSE_ADMIN_ENTRY = _SSE_REPO_ROOT / "tests" / "e2e" / "admin_entry.py"


def _sse_free_port() -> int:
    """Ephemeral host port for the SSE admin server subprocess."""
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
    import os
    import secrets
    import subprocess
    import sys

    import httpx

    from ._assertions import poll_until

    port = _sse_free_port()
    token = secrets.token_hex(16)
    python_path = os.environ.get("PYTHONPATH")
    redis_url = f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}"
    env = {
        **os.environ,
        "TASKQ_PG_DSN": e2e_schema.host_dsn,
        "TASKQ_SCHEMA_NAME": e2e_schema.schema_name,
        "TASKQ_E2E_ADMIN_TOKEN": token,
        "TASKQ_E2E_ADMIN_PORT": str(port),
        "TASKQ_E2E_REDIS_URL": redis_url,
        "PYTHONPATH": (
            str(_SSE_REPO_ROOT) if not python_path else f"{_SSE_REPO_ROOT}{os.pathsep}{python_path}"
        ),
    }
    proc = await asyncio.to_thread(
        lambda: subprocess.Popen(  # noqa: S603
            [sys.executable, str(_SSE_ADMIN_ENTRY)],
            cwd=str(_SSE_REPO_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    )

    def _shutdown(proc: subprocess.Popen[str]) -> str:
        if proc.poll() is None:
            proc.terminate()
        try:
            out, _ = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
        return out.strip()

    client = httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=httpx.Timeout(connect=10.0, read=90.0, write=10.0, pool=10.0),
    )

    async def _ready() -> bool:
        if proc.poll() is not None:
            logs = await asyncio.to_thread(_shutdown, proc)
            msg = f"admin server exited during startup (rc={proc.returncode})\n{logs}"
            raise RuntimeError(msg)
        try:
            resp = await client.get("/admin/jobs/count")
        except httpx.HTTPError:
            return False
        return resp.status_code == 200

    try:
        try:
            await poll_until(
                _ready,
                timeout=30.0,
                description=f"admin server readiness at {client.base_url}",
            )
        except TimeoutError:
            logs = await asyncio.to_thread(_shutdown, proc)
            msg = f"admin server not ready within 30s\n{logs}"
            raise RuntimeError(msg) from None

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
            headers={"Authorization": f"Bearer {token}"},
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
        await asyncio.to_thread(_shutdown, proc)
