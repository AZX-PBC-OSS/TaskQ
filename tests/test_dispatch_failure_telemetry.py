"""Pins failure-path telemetry for the dispatch loop.

Dispatch is the only failure-prone loop in the worker that currently emits
telemetry solely on the success path. A producer that fails every round
(a statement timeout on a slow plan, a lock timeout, a connection reset
mid-query) leaves both `taskq.dispatch.duration` and the fleet's metric
stream completely silent, so a pod that is draining nothing looks
identical, from the metrics, to an idle queue with no work. Two distinct
raise sites are covered: the dispatch query itself, and the queue-mode
resolution step that runs before it and today never even reaches the
dispatch helper.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch import _dispatch_batch
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._sql_templates import render
from taskq.testing.otel import (
    collect_metrics,
    counter_data_points,
    counter_value,
    histogram_points,
    setup_meter,
    setup_tracer,
)

pytestmark = pytest.mark.asyncio


class _FailingConn:
    """A connection whose fetch always raises -- stands in for a statement
    timeout, lock timeout, or connection reset mid dispatch query."""

    async def fetch(self, sql: str, *args: object) -> list[dict[str, int]]:
        raise ConnectionResetError("simulated connection reset mid-query")


async def test_dispatch_query_failure_still_emits_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dispatch round whose SQL execution raises still records a duration
    sample, so a pod failing every round is visible as elevated/failing
    dispatch activity rather than silence indistinguishable from idle."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    conn = _FailingConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    with pytest.raises(ConnectionResetError):
        await dispatch_batch(
            conn,  # type: ignore[arg-type] # Why: duck-typed fake satisfies fetch protocol but not asyncpg.Connection full type
            sql=rendered,
            queues=["default"],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )

    metrics = collect_metrics(reader)
    assert metrics, (
        "a dispatch round that raised produced an empty metric stream -- "
        "indistinguishable from an idle queue with no pending work"
    )


async def test_dispatch_query_failure_emits_failure_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed dispatch round bumps a dedicated failure counter naming the
    loop, so a dispatch outage is distinguishable from an idle queue purely
    from the metric stream -- no log scraping required."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    conn = _FailingConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    with pytest.raises(ConnectionResetError):
        await dispatch_batch(
            conn,  # type: ignore[arg-type] # Why: duck-typed fake
            sql=rendered,
            queues=["default"],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )

    metrics = collect_metrics(reader)
    failure_metric_names = [m.name for m in metrics if "dispatch" in m.name and "fail" in m.name]
    assert failure_metric_names, (
        "no dispatch failure counter was emitted; a repeatedly-failing "
        "dispatch round is invisible in the metric stream"
    )
    assert counter_value(reader, failure_metric_names[0]) >= 1


# ── the pre-dispatch queue-mode resolution raise site ────────────────────


class _RaisingResolveConn:
    """Fails the queue-mode resolve statement -- the raise site that never
    even reaches ``dispatch_batch``'s own span/telemetry: a lock timeout
    or connection reset while resolving ``queues.mode`` before the
    dispatch CTE is ever issued."""

    async def fetch(self, sql: str, *args: object) -> list[dict[str, str]]:
        if ".queues WHERE" in sql:
            raise TimeoutError("simulated lock timeout resolving queue modes")
        return []

    async def execute(self, sql: str, *args: object) -> str:
        return "INSERT 0 1"

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakeResolvePool:
    def __init__(self, conn: _RaisingResolveConn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire, which _dispatch_batch calls with timeout=.
    ) -> AsyncGenerator[_RaisingResolveConn]:
        yield self._conn


async def test_queue_mode_resolution_failure_still_emits_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A round whose queue-mode resolution raises before the dispatch CTE
    is ever issued still leaves some dispatch-loop telemetry behind. This
    failure never reaches ``dispatch_batch``'s span or histogram at all,
    so without the round-level record the whole metric stream is empty
    for this entire class of failure."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    conn = _RaisingResolveConn()

    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            _FakeResolvePool(conn),  # type: ignore[arg-type] # Why: duck-typed pool; only acquire() is used.
            render("taskq"),
            2,
            5.0,
            "taskq",
            new_uuid(),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )

    metrics = collect_metrics(reader)
    assert metrics, (
        "queue-mode resolution failure before the dispatch CTE produced an "
        "empty metric stream -- this failure class is invisible even "
        "though a producer retries it forever"
    )


# ── the claimable-rows probe raise site (window-expansion gate) ─────────


class _ProbeFailingConn:
    """The claim returns empty; the claimable-rows probe raises -- a
    statement timeout or connection reset on the window-expansion gate,
    the third raise site a dispatch round can take."""

    async def fetch(self, sql: str, *args: object) -> list[dict[str, str]]:
        if "ac.queue = ANY($1::text[])" in sql:
            raise TimeoutError("simulated lock timeout on the claimable-rows probe")
        return []

    async def execute(self, sql: str, *args: object) -> str:
        return "INSERT 0 1"

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()


async def test_claimable_probe_failure_still_emits_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty round whose claimable-rows probe raises must fail loudly
    with the round's failure telemetry recorded -- a producer whose probe
    times out every round is failing, not idle, and the metric stream must
    say so."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    conn = _ProbeFailingConn()

    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            _FakeResolvePool(conn),  # type: ignore[arg-type] # Why: duck-typed pool; only acquire() is used.
            render("taskq"),
            2,
            5.0,
            "taskq",
            new_uuid(),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )

    metrics = collect_metrics(reader)
    failure_metric_names = [m.name for m in metrics if "dispatch" in m.name and "fail" in m.name]
    assert failure_metric_names, (
        "a dispatch round that died on the claimable-rows probe emitted no "
        "failure counter -- indistinguishable from an idle queue"
    )
    assert counter_value(reader, failure_metric_names[0]) >= 1


# ── the pool-acquire raise site (the round's first stage) ────────────────


class _ExpiringAcquirePool:
    """A pool whose acquire wait always times out.

    ``acquire()`` models asyncpg.Pool.acquire: it returns the async
    context manager, and the WAIT (the part that can raise) happens on
    ``__aenter__``. An exhausted dispatcher pool on a saturated PG
    raises TimeoutError here, the round's very first stage, before the
    resolve, the probe, and the claim can even run.
    """

    class _Ctx:
        async def __aenter__(self) -> object:
            raise TimeoutError("simulated dispatcher pool acquire timeout")

        async def __aexit__(self, *args: object) -> None:
            return None

    def acquire(self, *, timeout: float | None = None) -> "_ExpiringAcquirePool._Ctx":
        return self._Ctx()


async def test_pool_acquire_failure_emits_failure_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A round that dies waiting for a dispatcher connection must record
    the failure counter, the same record every sibling stage of the same
    round (resolve, probe, claim) makes. The acquire stage sits OUTSIDE
    the recording tries of those stages, so before the fix a pod whose
    every round died on pool exhaustion or acquire timeout was, in the
    metric stream, indistinguishable from one polling an idle queue:
    silent on taskq.dispatch.failures, retries forever, /ready green."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            _ExpiringAcquirePool(),  # type: ignore[arg-type] # Why: duck-typed pool; only acquire() is used.
            render("taskq"),
            2,
            5.0,
            "taskq",
            new_uuid(),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )

    assert counter_value(reader, "taskq.dispatch.failures") >= 1, (
        "a dispatch round that failed at the pool acquire emitted no "
        "failure counter -- indistinguishable from an idle queue"
    )
    assert counter_data_points(reader, "taskq.dispatch.failures")[0].attributes, (
        "the acquire-stage failure point must carry the stage labels"
    )


async def test_pool_wait_stays_out_of_the_query_duration_histogram(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pool wait is a different quantity from the SQL latency
    ``taskq.dispatch.duration`` exists to measure (its contract is SQL
    execution only): a pool-exhausted pod waiting seconds per round for a
    connection runs zero SQL, so feeding the wait into that histogram
    corrupts the dispatch p99 with zero-query samples. The wait lands in
    its own histogram instead."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            _ExpiringAcquirePool(),  # type: ignore[arg-type] # Why: duck-typed pool; only acquire() is used.
            render("taskq"),
            2,
            5.0,
            "taskq",
            new_uuid(),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )

    assert histogram_points(reader, "taskq.dispatch.duration") == [], (
        "pool wait was recorded on the SQL-latency histogram: the dispatch "
        "p99 now mixes zero-query waits with query times"
    )
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert len(wait_points) == 1, (
        "the pool wait must be observable on its own histogram, not dropped"
    )
    assert wait_points[0].count == 1
    assert wait_points[0].sum > 0


class _CancelledAcquirePool:
    """A pool whose acquire wait is interrupted by cancellation: a
    shutdown or a loop teardown landing while the round waits for a
    connection."""

    class _Ctx:
        async def __aenter__(self) -> object:
            raise asyncio.CancelledError()

        async def __aexit__(self, *args: object) -> None:
            return None

    def acquire(self, *, timeout: float | None = None) -> "_CancelledAcquirePool._Ctx":
        return self._Ctx()


async def test_cancelled_acquire_records_wait_not_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancelled round's wait still happened, so it records on the wait
    histogram, but a cancellation is a shutdown, not a dispatch failure:
    the failure counter stays silent, matching every sibling stage's
    ``except Exception`` arms."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    with pytest.raises(asyncio.CancelledError):
        await _dispatch_batch(
            _CancelledAcquirePool(),  # type: ignore[arg-type] # Why: duck-typed pool; only acquire() is used.
            render("taskq"),
            2,
            5.0,
            "taskq",
            new_uuid(),
            ["default"],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )

    assert counter_value(reader, "taskq.dispatch.failures") == 0, (
        "a cancelled round must not count as a dispatch failure"
    )
    wait_points = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert len(wait_points) == 1 and wait_points[0].count == 1, (
        "the interrupted wait must still record its duration"
    )


# ── red-team: does the failure counter name the failure class? ──────────


async def test_dispatch_failure_counter_names_the_failure_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatch failure counter must carry an ``error_type`` label, the
    idiom already established elsewhere in this module (for example
    ``record_reservation_reclaim_drain_failure``), so a transient error
    (connection reset, lock timeout -- a producer that will recover on its
    own) is distinguishable from a permanent one (auth failure, schema
    drift -- a producer that never will) purely from the metric stream.
    Today ``record_dispatch_failure`` derives the class from the caught
    exception when the call site names none. Without the label a
    connection reset and a permanent misconfiguration were recorded
    identically, so an alert built on this counter could not tell a
    self-healing blip from an outage that needs a human."""
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    conn = _FailingConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    with pytest.raises(ConnectionResetError):
        await dispatch_batch(
            conn,  # type: ignore[arg-type] # Why: duck-typed fake
            sql=rendered,
            queues=["default"],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )

    metrics = collect_metrics(reader)
    failure_metrics = [m for m in metrics if "dispatch" in m.name and "fail" in m.name]
    assert failure_metrics, "expected a dispatch failure metric"

    for metric in failure_metrics:
        for data_point in metric.data.data_points:
            attrs = dict(data_point.attributes or {})
            assert "error_type" in attrs, (
                f"dispatch failure counter has no error_type label (attrs={attrs}); "
                "a lock-timeout retry storm and a permanent auth failure are "
                "indistinguishable in the metric stream, defeating the alerting "
                "use case this counter exists to serve"
            )
            assert attrs["error_type"] == "ConnectionResetError", (
                f"error_type must name the failure class, got {attrs!r} - a "
                "constant or empty value would leave a transient reset "
                "indistinguishable from a permanent auth failure"
            )
