"""Core lifecycle e2e — enqueue → cross-container dispatch → typed result round-trip.

Scenario:
enqueue → ``handle.wait()`` → succeeded; typed result retrievable after
completion; transition sequence pending→running→succeeded; effects row present.
Ground truth: ``jobs``, ``job_events``, ``e2e_effects``.

``job_events`` kind values asserted here are read from the library, not
guessed: the only ``insert_event`` writers are ``backend/_dispatch.py``
(``state_change`` pending→running on dispatch) and ``backend/_terminal.py``
(``state_change`` running→succeeded via ``_insert_state_change_event``). The
``kind="enqueue"`` / ``kind="dispatch"`` values in ``_enqueue.py`` /
``_dispatch_sql.py`` are structlog log keys, not ``job_events`` rows.

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from taskq import JobFilter

from ._assertions import fetch_effects, poll_until
from .actors import (
    LongRunningPayload,
    WelcomeEmailPayload,
    WelcomeEmailResult,
    long_running_job,
    send_welcome_email,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.backend._protocol import JobId

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


def _welcome_payload(run_id: str) -> WelcomeEmailPayload:
    """Deterministic payload shared by all four scenarios (each test mints its
    own ``run_id`` via fixture, so payloads never correlate across tests)."""
    return WelcomeEmailPayload(run_id=run_id, user_id="u-1", email="a@example.com")


async def test_enqueue_dispatch_succeeds(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    run_id: str,
) -> None:
    """Enqueue → real cross-container dispatch + invocation → typed result round-trip.

    The test process is a pure client: dispatch happens only inside the worker
    container (``FOR UPDATE SKIP LOCKED``), and ``handle.wait()`` validates the
    stored JSONB result back into ``WelcomeEmailResult``.
    """
    handle = await e2e_client.enqueue(send_welcome_email, _welcome_payload(run_id))

    result = await handle.wait(timeout=60)

    assert isinstance(result, WelcomeEmailResult)
    assert result.sent is True
    assert result.message_id == "msg-u-1"


async def test_job_transitions_recorded(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """The job's lifecycle transitions land in ``{schema}.job_events``.

    A one-shot success records exactly two ``state_change`` rows: dispatch's
    pending→running and the terminal write's running→succeeded. ``detail`` is
    JSONB; asyncpg returns it as ``str``, so it is parsed before asserting.
    """
    handle = await e2e_client.enqueue(send_welcome_email, _welcome_payload(run_id))
    await handle.wait(timeout=60)

    rows = await e2e_pg_pool.fetch(
        f"""
        SELECT kind, detail
        FROM "{e2e_schema.schema_name}".job_events
        WHERE job_id = $1
        ORDER BY id
        """,
        handle.job_id,
    )

    assert len(rows) == 2
    assert {row["kind"] for row in rows} == {"state_change"}
    details: list[dict[str, str]] = [json.loads(row["detail"]) for row in rows]
    transitions = [(detail["from_state"], detail["to_state"]) for detail in details]
    assert transitions == [("pending", "running"), ("running", "succeeded")]


async def test_effects_written_by_actor(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """The actor's ``send`` effect row is the ground truth for real invocation.

    The INSERT happens inside the actor before it returns, so it is committed
    before the terminal write that ``handle.wait()`` observes — a one-shot
    ``fetch_effects`` (no polling) is deterministic here.
    """
    handle = await e2e_client.enqueue(send_welcome_email, _welcome_payload(run_id))
    await handle.wait(timeout=60)

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="send")

    assert len(rows) == 1
    effect = rows[0]
    assert effect["actor"] == "send_welcome_email"
    assert effect["job_id"] == handle.job_id
    assert effect["attempt"] == 1
    detail: dict[str, str] = json.loads(effect["detail"])
    assert detail["run_id"] == run_id
    assert detail["email"] == "a@example.com"
    assert detail["message_id"] == "msg-u-1"


async def test_result_retrievable_after_completion(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    run_id: str,
) -> None:
    """Post-completion, the stored result is re-readable without ``wait()``.

    ``JobHandle`` exposes the stored result after completion through
    ``refresh()`` (``JobRow.result``, the parsed ``jobs.result`` JSONB column);
    it must validate back to the same typed result ``wait()`` returned. The
    actor's ``result_ttl`` (1h) keeps the row's result live for this read.
    """
    handle = await e2e_client.enqueue(send_welcome_email, _welcome_payload(run_id))
    waited = await handle.wait(timeout=60)

    assert await handle.status() == "succeeded"
    row = await handle.refresh()
    assert row.status == "succeeded"
    assert row.result is not None
    stored = WelcomeEmailResult.model_validate(row.result)
    assert stored == waited
    assert stored.sent is True
    assert stored.message_id == "msg-u-1"


async def test_job_filter_status_and_active(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    run_id: str,
) -> None:
    """``JobFilter`` multi-status + ``active`` meta-filter against a live fleet.

    In flight the job is visible under ``status=["pending", "running"]`` and
    ``active=True``; after a forced cancel it leaves ``active`` and appears
    under ``status=["cancelled"]``. ``status=[]`` matches nothing (documented
    semantics — an empty sequence is not 'no filter'). The 30 s
    ``long_running_job`` actor keeps the in-flight arm deterministic; the
    cancel ends the test without waiting out the actor.
    """

    async def _ids(filter: JobFilter) -> set[JobId]:
        page = await e2e_client.list(filter)
        return {job.id for job in page.jobs}

    handle = await e2e_client.enqueue(long_running_job, LongRunningPayload(run_id=run_id))

    async def _visible_in_flight() -> bool:
        return handle.job_id in await _ids(JobFilter(status=["pending", "running"]))

    await poll_until(
        _visible_in_flight,
        timeout=30.0,
        description="job visible via JobFilter(status=[pending, running])",
    )
    assert handle.job_id in await _ids(JobFilter(active=True))
    assert await _ids(JobFilter(status=[])) == set()

    cancel_result = await handle.cancel()
    assert cancel_result.cancellation_initiated

    # Forced cancel lands ~1 s after the worker observes the request
    # (TASKQ_CANCELLATION_GRACE_PERIOD=1 in e2e); poll to the terminal row.
    async def _visible_cancelled() -> bool:
        return handle.job_id in await _ids(JobFilter(status=["cancelled"]))

    await poll_until(
        _visible_cancelled,
        timeout=30.0,
        description="job visible via JobFilter(status=[cancelled]) after cancel",
    )
    assert handle.job_id not in await _ids(JobFilter(active=True))
