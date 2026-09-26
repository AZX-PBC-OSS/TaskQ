"""The cancel-is-not-weather pins: a DELIVERED cancellation never re-arms the
redis transient-retry loop.

The gap this pins: ``with_pg_fallback``'s weather arm treats every
``redis.ConnectionError``/``redis.TimeoutError`` as a blip worth a bounded
retry. redis's own read machinery sits between the caller's cancellation and
that arm: the read runs inside a timeout context (``asyncio.timeout`` on the
stacks this repo ships: Python >= 3.12, redis-py 8.x), and a cancellation that
arrives DURING a read can surface at the retry loop as the read's very own
``TimeoutError`` (when a cancellation is already counted when the read's
timeout context is entered, the deadline's expiry converts the unwind to
``TimeoutError`` and leaves ``Task.cancelling()`` nonzero). Without the guard,
the retry loop re-arms against a task the caller already gave up on: the
budget burns, the mislabelled weather WARNING flies, and the PG fallback
answers a request whose caller is gone.

The contract pinned here:

* a cancellation delivered MID-READ propagates: the task ends cancelled, the
  retry never re-arms past it, no verdict, no fallback call;
* even when a cancellation was absorbed upstream and the read times out
  honestly (the conversion shape: ``TimeoutError`` with ``cancelling()`` > 0
  riding into the weather arm), the guard re-raises on sight: one read, no
  re-arm, no weather WARNING, no verdict.

The injection is the zombie-proxy pattern of ``tests/test_attack_zombie_tx.py``
minus the forwarding: a TCP blackhole that ACCEPTS and never responds, so the
client's read parks until ``socket_timeout`` expires. The client is a REAL
``redis.asyncio.Redis`` whose config is proven against the REAL container
first (a ``TIME`` round trip must answer), then pointed at the blackhole:
``driver_info=None`` and ``protocol=2`` remove the connect-phase round trips
(no CLIENT SETINFO, no HELLO) so the FIRST read is the command's own, and
``retries=0`` disables redis-py's client-level retry so one read is exactly
one ``socket_timeout`` window: the timeline is the mandate's: read (0 to 0.5s)
-> backoff (0.25s) -> second read (0.75 to 1.25s).
"""

import asyncio
import contextlib

import pytest
import redis.asyncio as redis_async
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from taskq.constants import RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS
from taskq.ratelimit._redis_utils import with_pg_fallback
from taskq.ratelimit.decision import RateLimitDecision
from taskq.settings import WorkerSettings

pytestmark = [pytest.mark.integration, pytest.mark.redis]

_READ_TIMEOUT_S = 0.5
"""The victim client's socket_timeout: one blackholed read dies here."""

_SECOND_READ_SETTLE_S = 0.15
"""How long after the second read STARTS the cancel is delivered: well
inside its 0.5s window, well after the first read's backoff."""

_CONNECT_TIMEOUT_S = 2.0
_BUCKET = "cancel-not-weather-pin"

_PG_VERDICT = RateLimitDecision(
    allowed=True,
    remaining=1.0,
    retry_after=None,
    bucket_name=_BUCKET,
    backend="postgres",
)


def _settings() -> WorkerSettings:
    """The fallback-WIRED settings: the eat's verdict (the PG decision) is
    observable. The settings' own redis_url is never dialed: the pin drives
    ``with_pg_fallback`` through its seams directly."""
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",  # the deliberate redis-only shape
            "schema_name": "cancel_not_weather_pin",
            "redis_url": "redis://blackhole.invalid/0",
            "rate_limit_pg_fallback_enabled": True,
        }
    )  # type: ignore[arg-type]  # Why: WorkerSettings.load_from_dict accepts the dict at runtime, the same shape the transient-retry pins use.


def _client(url: str) -> redis_async.Redis:
    """A REAL client, tuned so the blackhole timeline is deterministic.

    ``driver_info=None`` / ``protocol=2``: the connect phase performs ZERO
    round trips (no CLIENT SETINFO, no HELLO), so the first read the client
    ever makes is the command's own. ``retries=0``: redis-py's client-level
    retry is disabled, one read is exactly one ``socket_timeout`` window.
    The SAME config is proven against the real container in each pin (the
    sanity ``TIME`` round trip) before the blackhole arms.
    """
    return redis_async.from_url(
        url,
        decode_responses=False,
        socket_timeout=_READ_TIMEOUT_S,
        socket_connect_timeout=_CONNECT_TIMEOUT_S,
        driver_info=None,
        protocol=2,
        retry=Retry(NoBackoff(), retries=0),
    )


class _BlackholeProxy:
    """The zombie proxy's relay shape minus the forwarding.

    Accepts every connection and NEVER responds: the handler parks forever,
    no byte is read, no byte is written. The client's write lands in the
    socket buffer and its read parks until ``socket_timeout`` expires: the
    read that never returns.
    """

    def __init__(self) -> None:
        self._server: asyncio.Server | None = None
        self._handlers: set[asyncio.Task[None]] = set()

    async def start(self) -> str:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        return f"redis://127.0.0.1:{port}/0"

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        for task in list(self._handlers):
            task.cancel()
        with contextlib.suppress(asyncio.TimeoutError, Exception):
            await asyncio.wait_for(self._server.wait_closed(), timeout=2.0)
        self._server = None

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handler = asyncio.current_task()
        assert handler is not None
        self._handlers.add(handler)
        try:
            # The blackhole: accept, then never read and never respond.
            await asyncio.Event().wait()
        finally:
            self._handlers.discard(handler)
            writer.close()


async def _sane_client_proves_itself(redis_url: str) -> None:
    """The injection's self-check on the CLIENT config: the same knobs that
    shape the blackhole timeline must speak real redis first. A config this
    pin broke (a bad protocol knob, a broken retry seam) would otherwise
    red the pins for the wrong reason.

    The sanity client keeps every config knob EXCEPT the 0.5s read budget:
    that budget is the BLACKHOLE timeline's parameter (one parked read dies
    there by construction), not the sanity round trip's - and the sanity
    read targets the SHARED co-tenanted broker, whose stall band under
    ``-n 2`` runner load measurably exceeds 0.5s (the observed red was the
    sanity ``TIME`` dying with ``Timeout reading from localhost:<port>``).
    ``socket_timeout=None`` here: a wedged broker is the suite-wide
    pytest-timeout's job, not the sanity check's; the 0.5s window's teeth
    are proven by the blackhole phase itself, where the read actually dies
    on schedule.
    """
    sane = redis_async.from_url(
        redis_url,
        decode_responses=False,
        socket_timeout=None,
        socket_connect_timeout=_CONNECT_TIMEOUT_S,
        driver_info=None,
        protocol=2,
        retry=Retry(NoBackoff(), retries=0),
    )
    try:
        await asyncio.wait_for(sane.time(), timeout=10.0)
    finally:
        await sane.aclose()


async def _wait_read_started(read_started: list[asyncio.Event], n: int) -> None:
    """Park until the *n*-th read has begun (the drive's ``redis_call`` sets
    its own flag the moment the read is entered): the cancel is placed
    relative to the READS' own progress (event-driven), never to a wall
    clock, so a slow runner cannot push the delivery outside the window it
    must land in."""
    await asyncio.wait_for(read_started[n - 1].wait(), timeout=10.0)


# ── Pin 1: the mandated drive: the cancel delivered mid-read ──────────


async def test_cancel_delivered_mid_read_ends_the_task_never_re_arms(
    redis_url: str,
) -> None:
    """The REAL read that never returns; the cancel delivered inside the
    SECOND read's window (after the first retry's backoff). The task ends
    CANCELLED: the cancellation propagates, the retry NEVER re-arms past a
    delivered cancellation (the read count stops at 2: the first read's
    genuine timeout was weathered, the second was the cancellation), no
    verdict, no fallback call."""
    await _sane_client_proves_itself(redis_url)

    proxy = _BlackholeProxy()
    victim_url = await proxy.start()
    client = _client(victim_url)
    reads: list[dict[str, object]] = []
    # One flag per read the budget could ever run (plus the drive's own
    # reads): the drive's redis_call raises the flag of ITS ordinal.
    read_started = [asyncio.Event() for _ in range(RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS + 1)]
    pg_calls: list[RateLimitDecision] = []
    try:

        async def redis_call() -> RateLimitDecision:
            read: dict[str, object] = {"error": None}
            reads.append(read)
            read_started[len(reads) - 1].set()
            try:
                await client.time()
                raise AssertionError("the blackhole answered: the injection failed")
            except BaseException as exc:
                read["error"] = type(exc).__name__
                reader_task = asyncio.current_task()
                assert reader_task is not None
                read["cancelling"] = reader_task.cancelling()
                raise

        async def pg_call() -> RateLimitDecision:
            pg_calls.append(_PG_VERDICT)
            return _PG_VERDICT

        task = asyncio.create_task(
            with_pg_fallback(redis_call, pg_call, bucket_name=_BUCKET, settings=None)
        )

        # The timeline: the first read times out at 0.5s, the retry's backoff
        # runs, the second read begins; the cancel lands INSIDE its window.
        await _wait_read_started(read_started, 2)
        await asyncio.sleep(_SECOND_READ_SETTLE_S)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10.0)

        assert len(reads) == 2, (
            f"the retry re-armed past a delivered cancellation: {len(reads)} "
            "reads ran, the budget is a re-attack on a task the caller gave "
            "up on"
        )
        assert reads[0]["error"] == "TimeoutError", (
            f"the first read must die of the genuine socket timeout, got {reads[0]['error']}"
        )
        first_cancelling = reads[0]["cancelling"]
        assert first_cancelling == 0, (
            f"the first read's timeout is weather, no cancellation was "
            f"delivered yet: cancelling()={first_cancelling}"
        )
        assert reads[1]["error"] == "CancelledError", (
            f"the second read must surface the DELIVERED cancellation, got "
            f"{reads[1]['error']}: a TimeoutError here would mean redis "
            "converted the cancellation into weather"
        )
        second_cancelling = reads[1]["cancelling"]
        assert second_cancelling == 1, (
            f"the cancellation was delivered to the task mid-read: cancelling()={second_cancelling}"
        )
        assert not pg_calls, (
            f"the fallback fired for a request whose caller was already "
            f"cancelled: {len(pg_calls)} verdict(s)"
        )
    finally:
        await client.aclose()
        await proxy.stop()


# ── Pin 2: the conversion seam: TimeoutError with cancelling() > 0 ────


async def test_absorbed_cancellation_surfacing_as_timeout_is_never_weathered(
    redis_url: str,
) -> None:
    """The eat's exact shape, isolated: a cancellation the caller DELIVERED
    is absorbed upstream (redis's read machinery converts a mid-read
    cancellation into its own ``TimeoutError`` whenever the cancellation is
    already counted when the read's timeout context is entered), and the
    read that reaches the weather arm is a REAL blackholed read that times
    out honestly, carrying ``cancelling()`` > 0.

    The guard must re-raise on SIGHT: one read, no re-arm, no verdict. Red
    under the pre-guard code: the TimeoutError was weathered, the budget
    burned (exactly RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS reads against
    a task whose caller gave up), and the PG fallback answered the dead
    request: the verdict the caller never saw."""
    await _sane_client_proves_itself(redis_url)

    proxy = _BlackholeProxy()
    victim_url = await proxy.start()
    client = _client(victim_url)
    reads: list[dict[str, object]] = []
    read_started = [asyncio.Event() for _ in range(RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS + 1)]
    pg_calls: list[RateLimitDecision] = []
    parked = asyncio.Event()
    try:

        async def redis_call() -> RateLimitDecision:
            read: dict[str, object] = {"error": None}
            reads.append(read)
            read_started[len(reads) - 1].set()
            try:
                await client.time()
                raise AssertionError("the blackhole answered: the injection failed")
            except BaseException as exc:
                read["error"] = type(exc).__name__
                reader_task = asyncio.current_task()
                assert reader_task is not None
                read["cancelling"] = reader_task.cancelling()
                raise

        async def pg_call() -> RateLimitDecision:
            pg_calls.append(_PG_VERDICT)
            return _PG_VERDICT

        async def drive() -> RateLimitDecision:
            # The modelled absorption: the caller's cancellation is delivered
            # while this task is parked, and the layer that catches it does
            # not re-raise (the shape redis's own read machinery produces
            # when the cancellation arrives mid-read). The task runs on with
            # cancelling() > 0: the cancellation is IN the task, unhandled.
            parked.set()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(30.0)
            return await with_pg_fallback(
                redis_call, pg_call, bucket_name=_BUCKET, settings=_settings()
            )

        task = asyncio.create_task(drive())
        await asyncio.wait_for(parked.wait(), timeout=10.0)
        task.cancel()
        await _wait_read_started(read_started, 1)

        with pytest.raises(redis_async.TimeoutError):
            await asyncio.wait_for(task, timeout=10.0)

        assert len(reads) == 1, (
            f"the weather arm re-armed past a delivered cancellation: "
            f"{len(reads)} reads ran "
            f"(the budget is {RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS}): "
            "a TimeoutError carrying cancelling() > 0 is the caller's "
            "cancellation in the read's clothes, never weather"
        )
        assert reads[0]["error"] == "TimeoutError", (
            f"the real blackholed read must die of its socket timeout, got {reads[0]['error']}"
        )
        first_cancelling = reads[0]["cancelling"]
        assert first_cancelling == 1, (
            f"the read must reach the seam with the cancellation already "
            f"counted: cancelling()={first_cancelling}"
        )
        assert not pg_calls, (
            f"the fallback answered a request whose caller was cancelled: "
            f"{len(pg_calls)} verdict(s)"
        )
        assert task.cancelling() >= 1, (
            "the guard's re-raise must keep the caller's cancellation count "
            f"intact, got cancelling()={task.cancelling()}"
        )
    finally:
        await client.aclose()
        await proxy.stop()
