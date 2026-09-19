"""RED-team: progress-publish failure emission and task lifetime under Redis death.

The bounded-never-raise contract of the publish helpers is pinned elsewhere
(tests/test_progress_publish.py). This file hunts the two unpinned sides of
sustained Redis death: (1) the failure-emission VOLUME - one WARNING per
publish attempt forever is the storm-of-logs class; (2) the fire-and-forget
publish task's lifetime - every round trip is bounded by
``_PUBLISH_TIMEOUT_S`` even against a black-holed Redis, so the pending
task set drains instead of parking.
"""

import asyncio
from datetime import UTC, datetime
from time import monotonic
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
import structlog
import structlog.testing

import taskq.progress._publish as publish_mod
from taskq._ids import new_job_id, new_uuid
from taskq.constants import progress_channel, progress_global_channel
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._publish import _publish_event, _publish_event_dual
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from tests._progress_context import make_progress_context

_JOB_ID = new_job_id()
_ATTEMPTS = 25


class _DeadPipeline:
    """Pipeline double whose execute round trip fails like a dead Redis."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    def publish(self, channel: str, payload: str) -> None:
        return None

    async def execute(self) -> list[int]:
        raise self._error

    async def __aenter__(self) -> "_DeadPipeline":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _DeadRedisClient:
    """Client double whose every publish round trip raises immediately -
    the sustained-outage shape with no timeout wait to slow the test."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    async def publish(self, channel: str, payload: str) -> int:
        raise self._error

    def pipeline(self, **kwargs: object) -> _DeadPipeline:
        return _DeadPipeline(self._error)


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_SCHEMA_NAME": "taskq_rt", "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false"}
    )


async def _tick_publishes(client: object) -> None:
    """One sustained-outage window: every progress tick attempts a publish
    and every round trip fails."""
    channel = progress_channel("taskq_rt", _JOB_ID)
    for seq in range(_ATTEMPTS):
        await _publish_event(
            client,  # type: ignore[arg-type]  # Why: duck-typed double standing in for redis.asyncio.Redis at the publish seam
            channel,
            '{"v": 1}',
            job_id=_JOB_ID,
            actor="rt_actor",
            seq=seq,
            channel_label="per_job",
        )


@pytest.mark.parametrize("path", ["direct", "dual"])
async def test_publish_failure_log_emission_is_bounded_under_sustained_outage(
    path: str,
) -> None:
    """Under sustained Redis death the failure WARNING must be rate-limited:
    the first failure reports the outage, subsequent failures inside a
    window are counted (the OTel counter aggregates every attempt) but must
    not emit one warning line per publish attempt forever - the same
    window-gated emission the registry already applies to heal failures
    (``_keyed_reservation_heal_failure_logged``).

    Verdict asserted: BOUNDED EMISSION. Current posture is fail-open-RED
    (storm of logs): every failed attempt emits its own warning line.
    """
    error = ConnectionError("redis dead")
    client = _DeadRedisClient(error)

    with structlog.testing.capture_logs() as captured:
        if path == "direct":
            await _tick_publishes(client)
        else:
            for seq in range(_ATTEMPTS):
                await _publish_event_dual(
                    client,  # type: ignore[arg-type]  # Why: duck-typed double standing in for redis.asyncio.Redis at the pipeline seam
                    progress_channel("taskq_rt", _JOB_ID),
                    progress_global_channel("taskq_rt"),
                    '{"v": 1}',
                    job_id=_JOB_ID,
                    actor="rt_actor",
                    seq=seq,
                )

    failure_warnings = [
        e
        for e in captured
        if e.get("event") == "progress-publish-failure" and e.get("log_level") == "warning"
    ]
    assert len(failure_warnings) <= 2, (
        f"DEPENDENCY-FAILURE contract (bounded failure emission): sustained Redis "
        f"death must not emit one progress-publish-failure warning per publish "
        f"attempt forever ({_ATTEMPTS} back-to-back failed attempts emitted "
        f"{len(failure_warnings)} warning lines). The outage must be reported once "
        "and remain observable through the counter, mirroring the registry's "
        "window-gated heal-failure emission. Verdict: FAIL-OPEN-RED (storm of logs)."
    )


async def test_publish_task_lifetime_bounded_under_hanging_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A black-holed Redis (publish accepted, no reply, no FIN) must not
    park the fire-and-forget progress publish task: every round trip is
    bounded by ``_PUBLISH_TIMEOUT_S``, so the spawned task completes and
    drains out of ``_pending_publish_tasks`` within the bound.

    Verdict asserted: BOUNDED TASK LIFETIME (fail-closed never-park).
    """

    async def _hanging_publish(channel: str, payload: str) -> int:
        await asyncio.sleep(3600)
        return 1

    client = AsyncMock()
    client.publish.side_effect = _hanging_publish
    monkeypatch.setattr(publish_mod, "_PUBLISH_TIMEOUT_S", 0.2)

    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: _ProgressBuffer(job_id=_JOB_ID, base_seq=0)}
    pending: set[asyncio.Task[None]] = set()

    ctx = make_progress_context(
        buffers,
        _JOB_ID,
        worker_id=new_uuid(),
        backend=backend,
        settings=_settings(),
        redis_client=client,
        pending_publish_tasks=pending,
    )

    t0 = monotonic()
    await ctx.progress(step=1)
    assert len(pending) == 1, "ctx.progress must spawn the publish task fire-and-forget"

    await wait_for_condition(
        lambda: not pending,
        timeout=5.0,
        description=(
            "the black-holed publish task never drained from pending_publish_tasks "
            "within the round-trip bound"
        ),
    )
    elapsed = monotonic() - t0
    assert not pending, (
        "DEPENDENCY-FAILURE contract (bounded task lifetime): a black-holed Redis "
        "must not park the fire-and-forget progress publish task - the round trip "
        f"is bounded by _PUBLISH_TIMEOUT_S and the task must drain from "
        f"pending_publish_tasks within that bound; still pending after "
        f"{elapsed:.2f}s. Verdict: FAIL-CLOSED never-park (bounded)."
    )
    assert elapsed < 1.0, (
        "the publish task must complete within the bound plus scheduling slack, "
        "not anywhere near the unbounded hang it was given"
    )
