"""Attack: NOTIFY wake delivery under loss, storms, and mid-round arrival.

Three behaviour-level attacks on the wake transport, all holding ONE
contract: the wake is an OPTIMIZATION. A job enqueued to an idle worker
must be claimed by a deadline whether or not any single NOTIFY survives
-- the LISTEN path delivers it promptly, the poll fallback delivers it
late but surely, and no schedule may exist where neither path carries
the row (a stall).

* Mid-round arrival: a wake that lands DURING a claim round (the enqueue
  committing after the round's snapshot) must be answered by exactly one
  follow-up round promptly -- never lost at the
  clear-before-round/wait-after-round seam, never delayed by the full
  poll interval.
* Reconnect storm: the listener's backend killed repeatedly while
  notifications keep firing -- after every cycle, delivery still lands
  by deadline (the reconnect's synthetic wake or the poll fallback
  carries it).
* Total wake loss: NO listener running at all -- the poll fallback alone
  must still claim within the notify poll cadence's bound. A construction
  where neither the wake nor the poll delivers is the stall this asserts
  cannot exist.
"""

import asyncio
import contextlib
from contextlib import AsyncExitStack, asynccontextmanager
from typing import cast

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import Backend, JobRow
from taskq.testing.assertions import wait_for_condition
from taskq.testing.pg import _create_worker
from taskq.worker.notify import notify_listener_loop
from taskq.worker.run import producer_loop

pytestmark = [pytest.mark.integration]

_CLAIM_BOUND_S = 12.0  # notify_poll_interval (5.0) + reconnect budget + slack


async def _raw_pg_conn(pg_dsn: str) -> asyncpg.Connection:
    return await asyncpg.connect(pg_dsn)


async def _pg_terminate_backend(pg_dsn: str, pid: int) -> None:
    conn = await _raw_pg_conn(pg_dsn)
    try:
        await conn.fetchval("SELECT pg_terminate_backend($1)", pid)
    finally:
        await conn.close()


async def _enqueue_raw(pg_dsn: str, schema: str, count: int = 1) -> None:
    """INSERT jobs the way a client does - the INSERT trigger is the sole
    wake source, so each row fires a NOTIFY on commit."""
    conn = await _raw_pg_conn(pg_dsn)
    try:
        await conn.execute(
            f"SET search_path TO {schema}"
        )  # Why: schema validated by WorkerSettings.post_load.
        for _ in range(count):
            await conn.execute(
                f'INSERT INTO "{schema}".jobs'  # noqa: S608  # Why: as above.
                " (id, actor, queue, payload, max_attempts, retry_kind)"
                " VALUES ($1, 'test_actor', 'default', '{}', 3, 'transient')",
                new_uuid(),
            )
    finally:
        await conn.close()


# ── Attack 1: the mid-round wake must survive the round seam ────────────


class _WakeBackend:
    """dispatch_batch with a hook that runs mid-round, before answering."""

    def __init__(self) -> None:
        self.wake_event = asyncio.Event()
        self.dispatch_calls: list[float] = []
        self.jobs: list[JobRow] = []
        self.on_round: object = None

    async def dispatch_batch(
        self,
        *,
        worker_id: object,
        queues: object,
        limit: int,
        lock_lease: object,
    ) -> list[JobRow]:
        self.dispatch_calls.append(asyncio.get_running_loop().time())
        if self.on_round is not None:
            self.on_round()  # pyright: ignore[reportCallIssue]  # Why: test-set callable.
        if self.jobs and limit > 0:
            return [self.jobs.pop(0)]
        return []

    @asynccontextmanager
    async def subscribe_wake(self, queues: object = None):
        yield self.wake_event


async def test_wake_landing_during_a_claim_round_is_claimed_by_a_prompt_followup_round(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wake set DURING round N (the enqueue committed after the round's
    snapshot) must not be lost at the round seam: the very next round runs
    promptly - the cleared event is answered by that round's own claim -
    and claims the job. Lost here, the claim waits out the whole poll
    interval: the latency the LISTEN path exists to remove."""
    from taskq.testing.jobs import make_job_row
    from tests.test_worker_producer_wake import _NoopPool, _producer_deps

    backend = _WakeBackend()
    deps = _producer_deps(poll_interval=5.0, maxsize=2)
    deps.settings.notify_enabled = True
    deps.dispatcher_pool = _NoopPool()

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=2)
    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()

    task = asyncio.create_task(
        producer_loop(
            deps,  # type: ignore[arg-type]  # Why: SimpleNamespace stand-in, the established producer-loop unit pattern.
            local_queue,
            shutdown_event,
            stop_event,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
        )
    )
    try:
        await wait_for_condition(
            lambda: len(backend.dispatch_calls) >= 1,
            description="first round started",
            timeout=2.0,
        )
        round1_at = backend.dispatch_calls[0]

        # The enqueue commits while round 1 runs: its NOTIFY fires this
        # hook, which sets the wake event mid-round.
        first_wake: dict[str, float] = {}

        def _mid_round() -> None:
            if "at" not in first_wake:
                first_wake["at"] = asyncio.get_running_loop().time()
                backend.wake_event.set()
                # The row the wake announces, claimable only by a LATER
                # round: round 1 is already past its snapshot.
                backend.jobs.append(make_job_row(status="pending"))

        backend.on_round = _mid_round

        await wait_for_condition(
            lambda: local_queue.qsize() >= 1,
            description="the mid-round wake's job claimed",
            timeout=_CLAIM_BOUND_S,
        )
        claimed_at = asyncio.get_running_loop().time()
        assert len(backend.dispatch_calls) >= 2, "a follow-up round must run"
        round2_at = backend.dispatch_calls[1]
        assert first_wake["at"] >= round1_at, "the hook ran mid-round"
        assert round2_at - first_wake["at"] < 2.0, (
            f"the follow-up round took {round2_at - first_wake['at']:.2f}s after "
            "the mid-round wake - the wake was lost at the round seam and the "
            "claim waited out the poll interval"
        )
        assert claimed_at - first_wake["at"] < 2.0
    finally:
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ── Attack 2: reconnect storm, notifications throughout ─────────────────


async def test_reconnect_storm_delivers_by_deadline_every_cycle(pg_dsn: str) -> None:
    """pg_terminate_backend lands repeatedly while notifications keep
    firing. After EVERY cycle - kill, enqueue during the outage, reconnect
    - the wake is delivered by deadline: the synthetic wake on the
    reconnected connection or the poll fallback carries it. Zero missed
    wakes across the storm."""
    from taskq.testing.fixtures import _open_pg_backend

    stack, deps, backend = await _open_pg_backend(
        pg_dsn, schema_name=f"storm_{new_base62()}".lower()
    )
    deps.settings.notify_health_check_interval = 0.5
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    raw_conn = await _raw_pg_conn(pg_dsn)
    try:
        await _create_worker(raw_conn, schema, worker_id)
    finally:
        await raw_conn.close()

    shutdown = asyncio.Event()
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, worker_id),
                name="notify.storm",
            )

            async with backend.subscribe_wake() as sentinel:
                for cycle in range(3):
                    notify_conn = deps.notify_conn
                    assert notify_conn is not None, f"cycle {cycle}: no connection"
                    pid = notify_conn.get_server_pid()
                    await _pg_terminate_backend(pg_dsn, pid)
                    await asyncio.sleep(0.05)
                    # Notifications keep firing during the outage: this row's
                    # wake can only reach the subscriber through the
                    # reconnect's synthetic wake or the poll fallback.
                    await _enqueue_raw(pg_dsn, schema)
                    try:
                        await asyncio.wait_for(sentinel.wait(), timeout=_CLAIM_BOUND_S)
                    except TimeoutError:
                        pytest.fail(
                            f"cycle {cycle}: wake not delivered within "
                            f"{_CLAIM_BOUND_S}s of the kill - missed wake"
                        )
                    sentinel.clear()

                    # The next cycle needs the reconnect DONE: wait for the
                    # connection to be replaced before killing again. Default
                    # args bind THIS cycle's pid (B023: the closure outlives
                    # the loop iteration it was defined in).
                    async def _reconnected(_pid: int = pid) -> bool:
                        conn = deps.notify_conn
                        return (
                            conn is not None
                            and conn.get_server_pid() != _pid
                            and not conn.is_closed()
                        )

                    await wait_for_condition(
                        _reconnected,
                        description=f"cycle {cycle}: listener reconnected",
                        timeout=_CLAIM_BOUND_S,
                    )

            shutdown.set()
    finally:
        shutdown.set()
        await stack.aclose()


# ── Attack 3: total wake loss - the poll fallback must carry delivery ───


async def test_claim_by_deadline_when_every_wake_is_lost(pg_dsn: str) -> None:
    """The stall construction: NO listener loop at all - no LISTEN is ever
    registered, so every wake is lost by construction. A job enqueued to
    an idle producer must still be claimed within the notify poll cadence's
    bound. If neither the wake (absent) nor the poll (must run) delivers,
    the enqueue-to-claim path has a stall and this fails on the deadline."""
    from taskq.testing.fixtures import _open_pg_backend

    stack, deps, backend = await _open_pg_backend(
        pg_dsn, schema_name=f"lost_{new_base62()}".lower()
    )
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    raw_conn = await _raw_pg_conn(pg_dsn)
    try:
        await _create_worker(raw_conn, schema, worker_id)
    finally:
        await raw_conn.close()

    # Deliberately NO notify_listener_loop: the backend's wake subscribers
    # (if any subscribed) would never be set - every wake is lost.
    shutdown_event = asyncio.Event()
    stop_event = asyncio.Event()
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)

    async with AsyncExitStack() as producer_stack:
        producer_stack.push_async_callback(stack.aclose)
        task = asyncio.create_task(
            producer_loop(
                deps,
                local_queue,
                shutdown_event,
                stop_event,
                backend=cast(Backend, backend),
                worker_id=worker_id,
            ),
            name="producer.total-wake-loss",
        )
        try:
            await asyncio.sleep(0.2)  # the producer's first round finds nothing
            enqueued_at = asyncio.get_running_loop().time()
            await _enqueue_raw(pg_dsn, schema)

            def _claimed() -> bool:
                return local_queue.qsize() >= 1

            await wait_for_condition(
                _claimed,
                description="job claimed with every wake lost (poll fallback)",
                timeout=_CLAIM_BOUND_S,
            )
            claimed_at = asyncio.get_running_loop().time()
            assert claimed_at - enqueued_at <= _CLAIM_BOUND_S
        finally:
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
