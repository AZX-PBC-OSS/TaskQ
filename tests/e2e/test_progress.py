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
