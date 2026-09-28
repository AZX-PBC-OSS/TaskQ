"""The kill9 SIGKILL campaign, part 2: the redis broker dies mid-flight.

The wave-2 outage work pinned the TIMEOUT case (``CLIENT PAUSE``: every
command stalls past its budget and drops). This family attacks the other
death: the broker process is SIGKILLed (``docker kill``), so every live
socket breaks at once and new connections are REFUSED - the
killed-broker case, where the failure is immediate, not slow.

Two windows:

* mid-ratelimit-acquire: an admission is in flight (and then attempted)
  while the broker is dead. The contract is fail-CLOSED-or-fallback,
  never fail-open: with the PG fallback wired (the default) the killed
  broker's ConnectionError weathers its bounded transient retries and
  lands on the durable PG row's decision; with the fallback disabled the
  error PROPAGATES (no admission is granted by a store that could not
  answer). After the broker restarts (same pinned host port - the
  container's declared config survives kill/start), clients recover: the
  next acquire is answered by redis again.
* mid-progress-stream: a body is publishing beats while the broker dies.
  The durable PG surface (``progress_seq``) must carry what the body
  consumed, the job must still terminate (the terminal write is PG, not
  broker), the worker must survive the kill, and after the restart the
  fanout must resume for the next job - the loss bounded to the outage
  window.

The container idiom is the killable one (function-scoped, ownership
labels, never the session-shared broker) with ONE campaign addition: the
host port is PINNED (``free_host_port``), because only an explicitly
published port is part of the container's declared config and is
restored verbatim across ``docker kill`` / ``docker start`` - a
Docker-assigned ephemeral port is reallocated on start and would
silently invalidate the URL every client holds.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import time
from typing import TYPE_CHECKING

import pytest
import redis.asyncio as redis_async

from taskq.backend.clock import SystemClock
from taskq.constants import progress_channel
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import (
    DRAGONFLY_IMAGE,
    DRAGONFLY_RESOURCE_FLAGS,
    creator_labels,
    skip_test_without_docker,
)
from tests.conftest import free_host_port
from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, delete_tagged
from tests.system_e2e._kill_actors import Kill9Payload, kill9_progress

if TYPE_CHECKING:
    from collections.abc import Iterator

    import asyncpg
    from testcontainers.community.redis import RedisContainer

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [
    pytest.mark.integration,
    pytest.mark.system,
    pytest.mark.timeout(300),
]

_TAG = "kill9-redis"

#: The transient-retry budget a killed broker weathers before the
#: decision: RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS attempts with the
#: (0.25, 0.75) backoffs - the fallback cannot legitimately land sooner
#: than the backoffs sum.
_RETRY_BACKOFFS_S = (0.25, 0.75)
_FALLBACK_FLOOR_S = sum(_RETRY_BACKOFFS_S) - 0.05

#: A killed Dragonfly restarts in well under a second (measured: ~0.7s);
#: the probe budget is 60s so a co-tenanted daemon cannot flake the cap.
_BROKER_RESTART_PROBE_CAP_S = 60.0

_BEATS = 20  # 3s of streaming at the 0.15s cadence


@pytest.fixture
def kill9_redis() -> Iterator[RedisContainer]:
    """The killable broker idiom (function-scoped, ownership labels) plus
    the campaign's pinned host port: an explicitly published port is part
    of the container's declared config, so ``docker kill`` / ``docker
    start`` restores the exact mapping and every client's URL survives."""
    skip_test_without_docker()
    from testcontainers.community.redis import RedisContainer

    port = free_host_port()
    with (
        RedisContainer(image=DRAGONFLY_IMAGE)
        .with_command(DRAGONFLY_RESOURCE_FLAGS)
        .with_kwargs(labels=creator_labels())
        .with_bind_ports(6379, port)
    ) as rc:
        yield rc


def _docker_path() -> str:
    import shutil

    docker = shutil.which("docker")
    assert docker is not None, "docker CLI not on PATH - the kill idiom requires it"
    return docker


def _docker_kill(container_id: str) -> None:
    """SIGKILL the container's pid 1 - the dirty death, no graceful stop."""
    subprocess.run(  # noqa: S603  # Why: fixed argv, no shell, the id is the fixture's own container.
        [_docker_path(), "kill", container_id],
        check=True,
        capture_output=True,
        timeout=30,
    )


def _docker_start(container_id: str) -> None:
    subprocess.run(  # noqa: S603  # Why: fixed argv, no shell, the id is the fixture's own container.
        [_docker_path(), "start", container_id],
        check=True,
        capture_output=True,
        timeout=30,
    )


async def _redis_url(container: RedisContainer) -> str:
    from taskq.testing.fixtures import redis_url_for

    return redis_url_for(container)


async def _wait_redis_answers(url: str, cap_s: float) -> None:
    """Poll the broker back to answering PINGs (the restart's readiness
    signal, never a blind sleep)."""
    deadline = time.monotonic() + cap_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            probe = redis_async.from_url(url, decode_responses=False, socket_timeout=2)
            try:
                await probe.ping()
            finally:
                await probe.aclose()
            return
        except (
            Exception
        ) as exc:  # Why: the probe's failure modes are redis's whole error family across the boot.
            last = exc
            await asyncio.sleep(0.25)
    raise AssertionError(f"the broker never answered PINGs within {cap_s}s: {last!r}")


async def test_sigkill_broker_mid_ratelimit_acquire_fails_closed_then_recovers(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    kill9_redis: RedisContainer,
) -> None:
    """A killed broker mid-acquire: the admission never opens. With the
    fallback wired, the connection family's bounded weather lands on the
    PG decision; with the fallback disabled, the error propagates (no
    decision at all - the closed shape). After the broker restarts on its
    pinned port, the next acquire is redis's again."""
    from redis.exceptions import ConnectionError as RedisConnectionError

    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema}
    )
    closed_settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "schema_name": schema,
            "rate_limit_pg_fallback_enabled": False,
        }
    )
    clock = SystemClock()
    url = await _redis_url(kill9_redis)
    client = redis_async.from_url(url, decode_responses=False, socket_timeout=None)
    try:
        # The healthy path first: the acquire's redis-backend proof cannot
        # be vacuous.
        tb = TokenBucket(
            name="kill9-acquire",
            capacity=10.0,
            refill_per_second=10.0,
            backend="redis",
        )
        healthy = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert healthy.backend == "redis" and healthy.allowed, (
            f"the healthy acquire did not ride redis: {healthy}"
        )

        # The dirty death: SIGKILL the broker mid-stream of acquires.
        await asyncio.to_thread(_docker_kill, kill9_redis.get_container_id())

        # KILLED-broker acquire with the fallback wired: the immediate
        # ConnectionError (not the timeout case) weathers the bounded
        # transient retries, then lands on the PG decision - a DECISION,
        # never an exception, never an unbacked admission.
        t0 = time.monotonic()
        fallback = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        elapsed = time.monotonic() - t0
        assert fallback.backend == "postgres", (
            f"the killed broker did not fall back to PG: {fallback}"
        )
        assert fallback.allowed is True, f"the PG fallback denied a fresh bucket: {fallback}"
        assert elapsed >= _FALLBACK_FLOOR_S, (
            f"the fallback landed in {elapsed:.2f}s - the bounded transient "
            "retries never ran (the weather budget was skipped)"
        )

        # The closed shape: no fallback wired, the error propagates - a
        # store that cannot answer grants nothing.
        tb_closed = TokenBucket(
            name="kill9-closed",
            capacity=10.0,
            refill_per_second=10.0,
            backend="redis",
        )
        with pytest.raises(RedisConnectionError):
            await tb_closed.acquire(
                redis_client=client,
                pg_pool=module_pg_pool,
                clock=clock,
                settings=closed_settings,
            )

        # Recovery: the broker restarts on the SAME pinned port; the next
        # acquire is answered by redis again (redis-py reconnects on the
        # next command; the stale script SHA re-loads through NOSCRIPT).
        await asyncio.to_thread(_docker_start, kill9_redis.get_container_id())
        await _wait_redis_answers(url, _BROKER_RESTART_PROBE_CAP_S)
        deadline = time.monotonic() + _BROKER_RESTART_PROBE_CAP_S
        recovered = None
        while time.monotonic() < deadline:
            recovered = await tb.acquire(
                redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
            )
            if recovered.backend == "redis":
                break
            await asyncio.sleep(0.5)  # the booting broker can still refuse briefly
        assert recovered is not None and recovered.backend == "redis", (
            f"the client never recovered onto the restarted broker: {recovered}"
        )
    finally:
        await client.aclose()


async def test_sigkill_broker_mid_progress_stream_loses_only_the_fanout(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    kill9_redis: RedisContainer,
) -> None:
    """A body streams beats and the broker is SIGKILLed mid-stream (the
    kill lands after the first fanout message was received - observed
    readiness, not a blind sleep). The durable PG surface must still
    carry the consumed seqs, the job must still terminate (the terminal
    write is PG), the worker must survive, and after the restart the
    fanout resumes for the next job."""
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    url = await _redis_url(kill9_redis)
    worker: WorkerProc | None = None
    subscriber = redis_async.from_url(url, decode_responses=False, socket_timeout=10)
    psub = subscriber.pubsub()
    try:
        worker = spawn_worker(pg_dsn, schema, redis_url=url, tag="kill9-redis")
        wait_worker_ready(worker)

        handle = await sys_client.enqueue(kill9_progress, Kill9Payload(beats=_BEATS), tags=[_TAG])
        await psub.subscribe(progress_channel(schema, handle.job_id))

        # Observed readiness: the kill lands once the fanout is PROVEN
        # alive (>= 1 message received), never on a guessed delay.
        async def _first_message() -> bool:
            msg = await psub.get_message(timeout=0.2)
            data = msg.get("data") if msg is not None else None
            return isinstance(data, bytes) and b'"seq"' in data

        deadline = time.monotonic() + 30.0
        got_any = False
        while time.monotonic() < deadline:
            if await _first_message():
                got_any = True
                break
            row = await conn.fetchrow(
                f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = $1',
                handle.job_id,
            )
            if row is not None and row["status"] == "succeeded":
                break  # too fast to catch - the kill would be vacuous; re-enqueue not needed, the assertion below fails loudly
        assert got_any, (
            "the fanout never delivered a message - the mid-stream kill would be vacuous"
        )

        await asyncio.to_thread(_docker_kill, kill9_redis.get_container_id())

        # The job still terminates: the terminal write is PG, not broker.
        deadline = time.monotonic() + 60.0
        row: asyncpg.Record | None = None
        while time.monotonic() < deadline:
            row = await conn.fetchrow(
                f"SELECT status::text AS status, progress_seq AS seq "
                f'FROM "{schema}".jobs WHERE id = $1',
                handle.job_id,
            )
            if row is not None and row["status"] == "succeeded":
                break
            await asyncio.sleep(0.25)
        assert row is not None and row["status"] == "succeeded", (
            f"the job never reached terminal across the broker kill: {row}"
        )
        assert row["seq"] >= 1, (
            f"durable progress_seq {row['seq']} lost the consumed seqs - the "
            "durable surface is the broker's victim too"
        )
        # The worker survived the kill (redis errors are the dependency
        # family, absorbed - the process is not).
        assert worker.proc.poll() is None, "the worker died with the broker"
        await psub.unsubscribe(progress_channel(schema, handle.job_id))

        # Recovery: the broker restarts on the pinned port; the SAME
        # worker's fanout resumes for the next job.
        await asyncio.to_thread(_docker_start, kill9_redis.get_container_id())
        await _wait_redis_answers(url, _BROKER_RESTART_PROBE_CAP_S)

        # The OLD subscriber's socket died with the kill (a pubsub
        # connection does not transparently re-subscribe after a severed
        # transport), so the recovery proof rides a FRESH connection - the
        # same fresh-connect path any recovered consumer takes.
        with contextlib.suppress(Exception):
            await psub.aclose()
        with contextlib.suppress(Exception):
            await subscriber.aclose()
        subscriber = redis_async.from_url(url, decode_responses=False, socket_timeout=10)
        psub = subscriber.pubsub()

        handle_b = await sys_client.enqueue(kill9_progress, Kill9Payload(beats=4), tags=[_TAG])
        await psub.subscribe(progress_channel(schema, handle_b.job_id))
        deadline = time.monotonic() + 60.0
        got_after = 0
        row_b: asyncpg.Record | None = None
        while time.monotonic() < deadline:
            msg = await psub.get_message(timeout=0.2)
            data = msg.get("data") if msg is not None else None
            if isinstance(data, bytes) and b'"seq"' in data:
                got_after += 1
            row_b = await conn.fetchrow(
                f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = $1',
                handle_b.job_id,
            )
            if row_b is not None and row_b["status"] == "succeeded" and got_after >= 1:
                break
        assert row_b is not None and row_b["status"] == "succeeded", (
            f"the post-restart job never went terminal: {row_b}"
        )
        assert got_after >= 1, (
            "the fanout never resumed after the broker restart - the loss was "
            "not bounded to the outage window"
        )

        # The whole tagged population balances.
        counts = await assert_balanced(conn, schema, _TAG)
        assert set(counts) <= {"succeeded"}, f"the kill manufactured outcomes: {counts}"
    finally:
        with contextlib.suppress(Exception):
            await subscriber.aclose()
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)
