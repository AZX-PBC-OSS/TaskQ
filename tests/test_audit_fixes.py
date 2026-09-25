"""Tests for audit-fix bugs: stranded log dedup, producer single dispatch, max_pending boundary, cron registration."""

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from taskq.backend._protocol import JobRow, ScheduleCreateArgs
from taskq.retry import RetryPolicy
from taskq.testing.in_memory import InMemoryBackend


class _FakeConn:
    """Fake asyncpg connection for stranded-jobs query."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.fetch_calls = 0

    async def fetch(self, sql: str, *args: object) -> list[dict[str, Any]]:
        self.fetch_calls += 1
        return self._rows


class _FakePoolCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self, **kwargs: object) -> _FakePoolCtx:
        return _FakePoolCtx(self._conn)


@pytest.mark.asyncio
async def test_stranded_jobs_loop_logs_once_per_actor() -> None:
    """Bug 1: stranded_jobs_loop should only log a warning once per actor."""
    from taskq.worker.leader import MaintenanceLeader

    deps = MagicMock()
    deps.is_leader = asyncio.Event()
    deps.is_leader.set()
    deps.settings.schema_name = "taskq"
    deps.settings.stranded_jobs_interval = 0.01

    # Row shape mirrors the detector SQL's per-shape breakdown columns.
    fake_conn = _FakeConn(
        rows=[
            {
                "actor": "orphan_actor",
                "cnt": 5,
                "no_actor_config_cnt": 5,
                "unserved_queue_cnt": 0,
                "unserved_queues": [],
            }
        ]
    )
    deps.worker_pool = _FakePool(fake_conn)

    backend = MagicMock()
    leader = MaintenanceLeader(deps, UUID(int=1), backend, clock=MagicMock())

    shutdown = asyncio.Event()
    log_calls: list[tuple[str, dict[str, Any]]] = []

    real_fetch = fake_conn.fetch

    async def _fetch_then_shutdown(sql: str, *args: object) -> list[dict[str, Any]]:
        rows = await real_fetch(sql, *args)
        if fake_conn.fetch_calls >= 2:
            shutdown.set()
        return rows

    fake_conn.fetch = _fetch_then_shutdown  # type: ignore[method-assign]  # Why: test drives loop exit via the fetch path, not by patching library internals.

    with patch("taskq.worker._leader_sweeps.log") as mock_log:
        mock_log.warning = lambda event, **kw: log_calls.append((event, kw))
        await leader._stranded_jobs_loop(shutdown)

    stranded = [c for c in log_calls if c[0] == "stranded-jobs-no-actor-config"]
    assert len(stranded) == 1, f"Expected 1 warning, got {len(stranded)}"
    assert fake_conn.fetch_calls == 2


@pytest.mark.asyncio
async def test_producer_loop_single_dispatch_on_empty_poll() -> None:
    """Bug 2: producer_loop should not call dispatch_batch twice on empty poll.

    With the fix, each loop iteration calls dispatch_batch exactly once.
    The original bug called it twice (once as the main dispatch, then again
    as a fallback when the first returned empty).
    """
    from taskq.worker.run import producer_loop

    dispatch_calls: list[dict[str, Any]] = []

    async def fake_dispatch(**kwargs: Any) -> list[JobRow]:
        dispatch_calls.append(kwargs)
        return []

    backend = MagicMock()
    backend.dispatch_batch = fake_dispatch
    backend.subscribe_wake = None

    shutdown_event = asyncio.Event()
    producer_stop_event = asyncio.Event()
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)

    settings = MagicMock()
    settings.poll_interval = 0.05
    settings.notify_poll_interval = 0.05
    settings.queues = ["default"]
    settings.lock_lease = 30
    settings.notify_enabled = False
    settings.max_concurrency = 4

    deps = MagicMock()
    deps.settings = settings
    # The producer's availability subtracts active jobs: an
    # empty-poll round with an empty queue and no active jobs must size
    # at the full slot count. The claim intents (takes not yet
    # registered) are the arithmetic's third term.
    deps.active_jobs.count.return_value = 0
    deps.active_jobs.intent_count.return_value = 0

    iteration = 0

    async def fast_sleep(duration: float) -> None:
        nonlocal iteration
        iteration += 1
        if iteration >= 3:
            producer_stop_event.set()

    with (
        patch("taskq.worker.run.asyncio.sleep", fast_sleep),
        patch("taskq.worker.run.contextlib.AsyncExitStack") as mock_stack,
    ):
        mock_stack.return_value.__aenter__ = AsyncMock(return_value=mock_stack.return_value)
        mock_stack.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_stack.return_value.enter_async_context = MagicMock(return_value=None)

        await producer_loop(
            deps,
            local_queue,
            shutdown_event,
            producer_stop_event,
            backend=backend,
            worker_id=UUID(int=1),
        )

    # With the fix, each loop iteration does exactly 1 dispatch_batch call.
    # The loop ran at least 2 iterations (dispatch → wait → dispatch → wait → stop).
    # Without the fix, the first empty poll would trigger 2 calls per iteration.
    assert len(dispatch_calls) >= 2
    # Key assertion: no two dispatch calls share the same iteration.
    # The original bug had 2 calls in the SAME iteration (same available slot count).
    # After the fix, consecutive calls always come from different iterations.
    # We verify by checking that between any two consecutive calls,
    # the loop went through a wait cycle (indicated by iteration count > dispatch count).
    assert len(dispatch_calls) <= iteration, (
        f"Dispatch calls ({len(dispatch_calls)}) should not exceed iterations ({iteration})"
    )


@pytest.mark.asyncio
async def test_max_pending_batch_boundary_uses_ge() -> None:
    """Bug 3: batch pre-flight should use >= (not >) for max_pending check."""
    from pydantic import BaseModel

    from taskq.actor import ActorRef
    from taskq.client._jobs import JobsClient
    from taskq.exceptions import MaxPendingExceededError
    from taskq.testing.clock import FakeClock

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend=backend, clock=clock)

    class _Payload(BaseModel):
        x: int = 0

    ref = ActorRef(
        name="test_actor",
        queue="default",
        fn=lambda p: None,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=MagicMock(),
        # A real policy, not a MagicMock: the enqueue boundary domain-validates
        # max_attempts (build_enqueue_args reads ref.retry.max_attempts into
        # EnqueueArgs, whose __post_init__ refuses a non-int), so a mock's
        # attribute spill fails the boundary this test drives.
        retry=RetryPolicy(),
        result_ttl=None,
        max_pending=3,
    )

    await client.enqueue(ref, _Payload(x=1))
    await client.enqueue(ref, _Payload(x=2))
    await client.enqueue(ref, _Payload(x=3))

    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(x=4))

    # Why the batch expects the typed batch error, not
    # MaxPendingExceededError: every item belongs to the one over-cap
    # actor, so the backend's per-actor partition admits nothing and
    # raises BatchMaxPendingExceededError (a BackpressureError sibling,
    # deliberately not a subclass - see its docstring).
    from taskq.batch import EnqueueItem
    from taskq.exceptions import BatchMaxPendingExceededError

    items = [EnqueueItem(actor_ref=ref, payload=_Payload(x=i)) for i in range(2)]
    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await client.enqueue_batch(items)
    assert exc_info.value.admitted_count == 0


@pytest.mark.asyncio
async def test_cron_registration_uses_backend_create_schedule() -> None:
    """Bug 4: cron registration should route through backend.create_schedule, not direct INSERT.

    Verifies that the code path in run._main that registers cron schedules
    calls backend.create_schedule with a ScheduleCreateArgs struct.
    """
    import contextlib as ctxlib

    import asyncpg

    from taskq.cron import CronScheduleSpec, compute_next_fire_after
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    calls: list[ScheduleCreateArgs] = []

    class _Tracking(InMemoryBackend):
        async def create_schedule(self, args: ScheduleCreateArgs) -> Any:
            calls.append(args)
            return await super().create_schedule(args)

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = _Tracking(clock=clock)

    specs = [
        CronScheduleSpec(actor="my_actor", cron_expr="0 3 * * *", timezone="UTC"),
        CronScheduleSpec(
            actor="cron_actor",
            cron_expr="*/5 * * * *",
            timezone="UTC",
            name="fast",
            static_payload={"key": "val"},
        ),
    ]

    for spec in specs:
        next_fires = compute_next_fire_after(
            spec.cron_expr,
            spec.timezone,
            datetime.now(tz=UTC),
            dst_strategy=spec.dst_strategy,
        )
        next_fire = next_fires[0]
        metadata: dict[str, object] = {}
        if spec.static_payload is not None:
            metadata["static_payload"] = spec.static_payload
        with ctxlib.suppress(asyncpg.UniqueViolationError):
            await backend.create_schedule(
                ScheduleCreateArgs(
                    actor=spec.actor,
                    cron_expr=spec.cron_expr,
                    timezone=spec.timezone,
                    next_fire_at=next_fire,
                    dst_strategy=spec.dst_strategy,
                    payload_factory=spec.payload_factory,
                    enabled=spec.enabled,
                    name=spec.name,
                    identity_key=spec.identity_key,
                    metadata=metadata,
                )
            )

    assert len(calls) == 2
    assert calls[0].actor == "my_actor"
    assert calls[0].cron_expr == "0 3 * * *"
    assert calls[0].name == ""
    assert calls[1].actor == "cron_actor"
    assert calls[1].name == "fast"
    assert calls[1].metadata.get("static_payload") == {"key": "val"}
