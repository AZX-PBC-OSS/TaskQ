# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Attack tests for the operational-insights recording commit
(a0f7245e): the parked classifier's verdicts, the windowed stall delta,
the due_at chain across a rate-limit denial, the migration discipline,
and the idle-fraction histogram's endpoint samples.

Every attack that could be driven empirically is driven on a REAL event
loop or a REAL Postgres, not only on synthetic frame tuples:

- the classifier's verdicts under an idle loop, a loop blocked inside a
  callback, a hot ``call_soon`` spinner (the zero-timeout select window),
  a nested-loop attempt on the loop thread, and uvloop;
- the windowed delta across a FAILED heartbeat tick (the anchor advance
  and the idle drain both land before the write that rolls back);
- eviction mid-window with re-attribution;
- two concurrent ``metadata || $2::jsonb`` writers on one workers row;
- the due_at chain across the full confusion: claim -> fail-retry ->
  claim -> rate-limit denial (no attempt row, attempt refunded) ->
  claim -> snooze -> claim -> succeed;
- the migration's checksum honesty on an already-migrated database;
- the ``taskq.worker.loop_idle_fraction`` histogram recording its 0.0
  and 1.0 endpoint samples (bounded buckets, nothing clipped away).
"""

import asyncio
import json
import threading
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import JobsApp
from taskq.worker._stall_tally import StallAttributionTally
from taskq.worker._watchdog import (
    _TIMEOUT_UNREAD,  # pyright: ignore[reportPrivateUsage]  # Why: the attack pins the classifier's own degradation sentinel.
    LoopLagWatchdog,
    LoopLiveness,
    _classify_loop_parked,  # pyright: ignore[reportPrivateUsage]
)

# ── Part 1: the parked classifier's verdicts (real loops) ────────────


class _RealLoopProbe:
    """Sample the current thread's loop through the real watchdog path.

    The samples run through :meth:`LoopLagWatchdog._loop_thread_parked` -
    the exact production read (one ``sys._current_frames`` walk serving
    both the shape sample and the ``_run_once`` timeout local), not a
    test-only reimplementation.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.verdicts: list[bool | None] = []
        self._watcher = LoopLagWatchdog(
            loop,
            LoopLiveness(),
            budget=60.0,
            warn_budget=30.0,
            startup_grace=0.0,
            poll_interval=0.5,
            enabled=False,
        )
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sampler, daemon=True)

    def _sampler(self) -> None:
        while not self._stop.is_set():
            self.verdicts.append(self._watcher._loop_thread_parked())  # pyright: ignore[reportPrivateUsage]
            time.sleep(0.002)

    def __enter__(self) -> "_RealLoopProbe":
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=5.0)

    def tally(self) -> tuple[int, int, int]:
        v = self.verdicts
        return (
            sum(x is True for x in v),
            sum(x is False for x in v),
            sum(x is None for x in v),
        )


async def test_idle_loop_reads_parked_through_the_real_watchdog_path() -> None:
    """A genuinely idle loop reads parked through the production read:
    the shape pair matches and the ``_run_once`` timeout local is positive
    or None (the timer-backed wait)."""
    loop = asyncio.get_running_loop()
    with _RealLoopProbe(loop) as probe:
        await asyncio.sleep(0.4)
    parked, busy, unreadable = probe.tally()
    assert parked > 0 and parked > busy * 10, (
        f"an idle loop must read overwhelmingly parked, got "
        f"parked={parked} busy={busy} unreadable={unreadable}"
    )
    assert unreadable == 0


async def test_hot_call_soon_churn_reads_busy_through_the_real_watchdog_path() -> None:
    """THE RED PROOF (red on the shape-only classifier): a loop saturated
    by ``call_soon`` churn is 100% busy scheduling callbacks, but its
    thread sits inside the ZERO-TIMEOUT select ``_run_once`` makes when
    ready callbacks are queued. Measured on the pre-fix code: 657/658
    samples read parked (~100% reported idle for a maximally busy loop),
    which falsified the shipped "biases toward busy, never toward idle"
    claim and the docstring's "microseconds wide ... at most one sample"
    estimate - the churn window is the loop's WHOLE wall time. The fix
    reads the ``_run_once`` frame's ``timeout`` local (0 exactly when
    ready callbacks are queued) through the same off-loop frame walk;
    this pin holds the busy verdict so the discriminator cannot silently
    regress to the shape-only read."""
    loop = asyncio.get_running_loop()
    # pytest-asyncio runs this module at loop scope MODULE - one shared
    # loop for every test in the file - so the spinner MUST be stopped by
    # the test's own finally (the iteration cap alone would keep it
    # spinning for seconds into the following tests' idle windows).
    spin_stop = threading.Event()
    try:
        with _RealLoopProbe(loop) as probe:
            await asyncio.sleep(0.05)
            iterations = 0

            def spinner() -> None:
                nonlocal iterations
                iterations += 1
                if not spin_stop.is_set() and iterations < 10_000_000:
                    loop.call_soon(spinner)

            loop.call_soon(spinner)
            await asyncio.sleep(0.4)
    finally:
        # Post the stop BEFORE any later scheduling: the shared loop runs
        # the next test's body only after this unwinds.
        spin_stop.set()
    parked, busy, _unreadable = probe.tally()
    assert iterations > 1000, "the spinner must have actually saturated the loop"
    assert busy > parked * 2, (
        f"a 100%-busy call_soon spinner must read mostly busy, got "
        f"parked={parked} busy={busy} - the zero-timeout select window is "
        "reading as idle again (the timeout-local discriminator regressed)"
    )


async def test_loop_blocked_inside_a_callback_reads_busy() -> None:
    """A loop blocked INSIDE a callback (the frame is the callback, not
    select) reads busy for the whole block."""
    loop = asyncio.get_running_loop()
    block_started = asyncio.Event()
    with _RealLoopProbe(loop) as probe:
        await asyncio.sleep(0.05)

        def blocking_callback() -> None:
            block_started.set()
            time.sleep(0.4)  # holds the loop thread inside a plain callback

        loop.call_soon(blocking_callback)
        await block_started.wait()
        # The loop thread is inside time.sleep right now; the sampler is
        # reading it off-thread. Await past the block.
        await asyncio.sleep(0.2)
    _parked, busy, _unreadable = probe.tally()
    # The block is 0.4s wide (~200 samples at the 2ms cadence); all of it
    # must read busy - the frame is the callback, never the selector.
    assert busy >= 100, (
        f"a loop blocked inside a callback must read busy through the "
        f"block, got only {busy} busy samples"
    )


async def test_nested_loop_on_the_loop_thread_is_a_cpython_runtime_error() -> None:
    """The one false-idle shape the frame read cannot see - a nested loop
    parked in ITS idle selector while the OUTER loop is wedged - is
    unreachable on CPython: the runtime's own re-entrancy guard raises
    before the nested loop ever parks. Pinned so a future CPython that
    permits re-entrancy re-opens the attack knowingly (the classifier's
    timeout-local read would then need the outer/inner distinction)."""
    loop = asyncio.get_running_loop()
    raised = asyncio.Event()

    def nested_callback() -> None:
        coro = _never_started()
        try:
            asyncio.run(coro)
        except RuntimeError:
            raised.set()
        finally:
            coro.close()  # never started: close it, avert the warning

    async def _never_started() -> None:
        raise AssertionError("the nested coroutine must never start")

    loop.call_soon(nested_callback)
    await asyncio.wait_for(raised.wait(), timeout=5.0)


def test_classifier_timeout_local_discriminator_pins() -> None:
    """The classifier's timeout-local arm: 0 (ready callbacks queued) is
    NOT parked; a positive timer wait and None (nothing scheduled) ARE;
    an unreadable local falls back to the shape-only verdict."""
    import os

    base = os.path.dirname(os.__file__)

    def shape() -> tuple[tuple[str, int, str, int], tuple[str, int, str, int]]:
        sel = os.path.join(base, "selectors.py")
        be = os.path.join(base, "base_events.py")
        # (file, lineno, funcname, code-object id) - the id is unused here.
        return (sel, 452, "select", 1), (be, 1200, "_run_once", 2)

    s = shape()
    assert _classify_loop_parked(s, run_once_timeout=0) is False
    assert _classify_loop_parked(s, run_once_timeout=0.25) is True
    assert _classify_loop_parked(s, run_once_timeout=None) is True
    assert _classify_loop_parked(s, run_once_timeout=_TIMEOUT_UNREAD) is True, (
        "an unreadable timeout local degrades to the shape-only verdict"
    )
    # Any other readable shape is busy regardless of the local.
    other = ((s[1][0], 1, "run_forever", 3), (s[1][0], 2, "run", 4))
    assert _classify_loop_parked(other, run_once_timeout=0) is False


def _has_uvloop() -> bool:
    """Whether the uvloop verdict probe can run (it is not a dependency)."""
    try:
        import uvloop  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: the probe skips cleanly without it.

        return True
    except ImportError:  # pragma: no cover - the shipped env has no uvloop
        return False


@pytest.mark.skipif(not _has_uvloop(), reason="uvloop is not installed")
def test_under_uvloop_the_signal_reads_honestly_busy() -> None:
    """Under uvloop (NOT a CPython selector loop - the documented
    boundary) the classifier never reads garbage and never reads parked:
    the loop thread's Python frames sit in the runner, the shape pair
    never matches, and every verdict is the honest-busy False. An
    operator running under uvloop sees idle_fraction pinned at 0 - an
    under-report, never a fabricated idle."""
    import uvloop

    async def body() -> None:
        loop = asyncio.get_running_loop()
        with _RealLoopProbe(loop) as probe:
            await asyncio.sleep(0.3)
        assert probe.verdicts, "the sampler must have taken samples"
        assert all(v is False for v in probe.verdicts), (
            f"under uvloop every verdict must be the honest-busy False, got {set(probe.verdicts)}"
        )

    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
        runner.run(body())


# ── Part 2: the windowed stall delta and the idle window ─────────────


def _window(publish: dict[str, object]) -> dict[str, object]:
    """The ``loop_stalls_window`` member, typed for the pins below."""
    return cast(dict[str, object], publish["loop_stalls_window"])


def _stalls(publish: dict[str, object]) -> dict[str, dict[str, int]]:
    """The window's per-actor delta map."""
    return cast(dict[str, dict[str, int]], _window(publish)["stalls"])


def _cumulative(publish: dict[str, object]) -> dict[str, dict[str, int]]:
    """The publish's cumulative ``loop_stalls`` map."""
    return cast(dict[str, dict[str, int]], publish["loop_stalls"])


def test_windowed_delta_across_a_skipped_publish_reports_no_double_window() -> None:
    """A FAILED heartbeat tick between two publishes still calls
    ``metadata_value()`` (the anchor advances BEFORE the write that then
    rolls back). The stalls of the rolled-back window are lost from the
    PUBLISHED windows - a coverage gap - but the delta never reports a
    double window and never fabricates: the next publish's span and its
    delta cover exactly the post-anchor segment, and the cumulative view
    keeps every stall."""
    tally = StallAttributionTally()

    # Window 1: published (the tick's write commits).
    tally.record("actor_a", kind="blocking_call")
    publish_1 = tally.metadata_value()
    assert _cumulative(publish_1) == {"actor_a": {"blocking_call": 1}}
    assert _stalls(publish_1) == {"actor_a": {"blocking_call": 1}}

    # Window 2: a stall lands, the tick calls metadata_value() (anchor
    # advances), and the write FAILS - the payload dies with the rollback.
    tally.record("actor_a", kind="blocking_call")
    lost_payload = tally.metadata_value()  # never published
    assert _stalls(lost_payload) == {"actor_a": {"blocking_call": 1}}

    # Window 3: the recovery beat publishes. The delta must be THIS
    # window's stalls only - not a double window (which would re-report
    # the lost segment), not a fabricated count.
    tally.record("actor_b", kind="gil_held")
    publish_3 = tally.metadata_value()
    assert _cumulative(publish_3) == {
        "actor_a": {"blocking_call": 2},
        "actor_b": {"gil_held": 1},
    }, "the cumulative view keeps every stall, including the lost window's"
    window_3_stalls = _stalls(publish_3)
    assert window_3_stalls == {"actor_b": {"gil_held": 1}}, (
        f"the post-anchor window reports only its own delta, got "
        f"{window_3_stalls} - a double window (re-reporting the "
        "rolled-back segment) would fabricate a rate spike"
    )
    assert "actor_a" not in window_3_stalls, (
        "the anchor advanced at the failed tick: actor_a's lost segment is "
        "a coverage gap, never a re-reported delta"
    )


def test_eviction_mid_window_with_reattribution_never_fabricates() -> None:
    """The bounded cap evicts mid-window. Observed contract, pinned
    against fabrication: an evicted actor's re-attribution re-evicts it
    (count 1 is always the new minimum), the window delta carries no
    entry for it - never a NEGATIVE count, never an inflated one - and
    the cumulative view never resurrects a phantom."""
    tally = StallAttributionTally(max_actors=3)

    # Publish 1: b is the smallest (count 1, and the name tie-break's min).
    tally.record("b", kind="blocking_call")
    tally.record("a", kind="blocking_call")
    tally.record("a", kind="blocking_call")
    tally.record("c", kind="blocking_call")
    tally.record("c", kind="blocking_call")
    publish_1 = tally.metadata_value()
    assert _cumulative(publish_1)["b"] == {"blocking_call": 1}

    # Mid-window eviction: a new attribution pushes past the cap and the
    # tie-break evicts 'b'; its re-attribution re-evicts it (count 1 is
    # again the minimum), so the final snapshot does not carry it.
    tally.record("d", kind="blocking_call")
    assert "b" not in tally.snapshot(), "the eviction must have run"
    tally.record("b", kind="blocking_call")  # the mid-window re-attribution

    publish_2 = tally.metadata_value()
    window_stalls = _stalls(publish_2)
    assert "b" not in window_stalls, (
        f"the re-attributed-then-re-evicted actor must not appear in the "
        f"window delta, got {window_stalls.get('b')}"
    )
    assert all(count >= 0 for actor in window_stalls.values() for count in actor.values()), (
        "no fabricated negative counts"
    )
    assert window_stalls == {"d": {"blocking_call": 1}}, (
        "the untouched new actor's delta is its honest count"
    )
    # The cumulative view: exactly the live keys, no phantom 'b'.
    assert _cumulative(publish_2) == {
        "a": {"blocking_call": 2},
        "c": {"blocking_call": 2},
        "d": {"blocking_call": 1},
    }


class _FailOnNthLivenessConn:
    """A asyncpg.Connection stand-in raising a transient error on the
    n-th liveness write (the tick-2 failure), recording every liveness
    call's bound payload."""

    def __init__(self, fail_on: int) -> None:
        self._fail_on = fail_on
        self._inner = _RecordingExecConn()
        self.liveness_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        if "last_seen_at = clock_timestamp()" in sql:
            self.liveness_calls.append((sql, args))
            if len(self.liveness_calls) == self._fail_on:
                raise asyncpg.QueryCanceledError("budget")
        return await self._inner.execute(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []

    def close(self) -> None:  # pragma: no cover - mirrors FakeConn's surface
        pass

    def terminate(self) -> None:  # pragma: no cover
        pass

    def transaction(self) -> Any:
        return self._inner.transaction()


class _RecordingExecConn:
    """The statement executor under the failure-injecting wrapper."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 1"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()


class _FakeTransaction:
    """Explicit-API transaction stand-in (start/commit/rollback)."""

    def __init__(self) -> None:
        self.started = False
        self.committed = False
        self.rolled_back = False

    async def start(self) -> None:
        self.started = True

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class _SharedPool:
    """Pool stand-in yielding one shared conn (the heartbeat loop's only
    pool surface in these unit ticks)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[Any, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's signature, as FakePool does.
        yield self._conn


async def test_failed_tick_keeps_the_idle_drain_metrics_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failed tick's idle drain is METRICS-ONLY: the histogram point
    stands, the row's payload dies with the rolled-back transaction, and
    the next tick's window starts fresh (no key, no second histogram
    point for the same samples)."""
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import HistogramDataPoint, InMemoryMetricReader

    import taskq.worker.heartbeat as hb_mod
    from tests.test_heartbeat import _make_deps  # pyright: ignore[reportPrivateUsage]

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("test", "0")
    idle_hist = meter.create_histogram(
        "taskq.worker.loop_idle_fraction",
        explicit_bucket_boundaries_advisory=(0.0, 0.5, 1.0),
    )
    saved = hb_mod._loop_idle_fraction
    monkeypatch.setattr(hb_mod, "_loop_idle_fraction", idle_hist)

    conn = _FailOnNthLivenessConn(fail_on=2)  # tick 1 commits, tick 2 fails
    deps = _make_deps(
        heartbeat_pool=_SharedPool(conn),  # type: ignore[arg-type]  # Why: the stand-in mirrors the asyncpg.Pool surface the loop uses.
        heartbeat_interval=0.5,
        max_heartbeat_failures=5,
    )
    # Samples in tick 1's window (recorded by the watchdog thread in
    # production; seeded here before the loop starts).
    for parked in (True, True, False):
        deps.loop_idle.record_sample(parked=parked)

    shutdown = asyncio.Event()
    task = asyncio.create_task(hb_mod.heartbeat_loop(deps, new_uuid(), shutdown))

    async def _liveness_reached(n: int) -> None:
        await wait_for_condition(
            lambda: len(conn.liveness_calls) >= n,
            description=f"liveness call {n}",
            timeout=10.0,
        )

    await _liveness_reached(1)  # tick 1's write landed
    # Samples recorded INSIDE tick 2's window: the tick that will fail
    # (its drain happens before the failing write).
    for parked in (True, False):
        deps.loop_idle.record_sample(parked=parked)
    await _liveness_reached(3)  # tick 2 attempted (failed), tick 3 committed
    shutdown.set()
    await task

    def _idle_points() -> list[HistogramDataPoint]:
        md = reader.get_metrics_data()
        assert md is not None
        return [
            p
            for rm in md.resource_metrics
            for sm in rm.scope_metrics
            for m in sm.metrics
            if m.name == "taskq.worker.loop_idle_fraction"
            for p in m.data.data_points
            if isinstance(p, HistogramDataPoint)
        ]

    # The failed tick's drain recorded its point (metrics-only), tick 1's
    # window recorded its own, and tick 3's EMPTY window recorded nothing:
    # exactly two points, the drained windows never double-counted.
    points = _idle_points()
    assert sum(p.count for p in points) == 2, (
        f"expected 2 histogram samples (tick 1's window + the failed "
        f"tick's metrics-only drain), got {sum(p.count for p in points)}"
    )
    # The recovery tick's write carried NO loop_idle key (its window was
    # empty) and no fabricated counts.
    _sql3, args3 = conn.liveness_calls[2]
    payload = json.loads(str(args3[1]))
    assert "loop_idle" not in payload
    # And the loop recovered: the failure was counted exactly once, then
    # reset by the successful tick.
    assert deps.heartbeat_failures == 0
    _ = saved  # monkeypatch restores the module global


async def test_concurrent_metadata_merges_on_one_workers_row_no_lost_update(
    pg_dsn: str,
) -> None:
    """Two writers racing the SAME workers row's
    ``metadata = metadata || $2::jsonb`` merge must not lose either
    side's key: READ COMMITTED re-evaluates the blocked UPDATE against
    the committed row version, so the second merge concats onto the
    first's result (the shape two concurrent publishes would need to
    corrupt to fabricate a stall count)."""
    from taskq.migrate import apply_pending

    schema = "merge_due_" + new_uuid().hex[:10]
    conn_a = await asyncpg.connect(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn)
    try:
        await conn_a.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn_a, schema=schema)
        worker_id = new_uuid()
        await conn_a.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
            "VALUES ($1, 'merge-probe', 1, '{default}', '{}')",
            worker_id,
        )

        async def merge(conn: asyncpg.Connection, key: str) -> None:
            await conn.execute(
                f'UPDATE "{schema}".workers SET metadata = metadata || $2::jsonb WHERE id = $1',
                worker_id,
                json.dumps({key: {"blocking_call": 1}}),
            )

        # Two sessions, same row, one blocked behind the other's lock.
        await asyncio.gather(
            merge(conn_a, "loop_stalls"),
            merge(conn_b, "loop_idle"),
        )

        row = await conn_a.fetchrow(
            f'SELECT metadata FROM "{schema}".workers WHERE id = $1', worker_id
        )
        assert row is not None
        metadata = json.loads(json.dumps(row["metadata"]))
        assert "loop_stalls" in metadata and "loop_idle" in metadata, (
            f"both merges must survive the race, got {metadata} - a lost "
            "update would fabricate a healed worker"
        )
    finally:
        await conn_a.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn_a.close()
        await conn_b.close()


# ── Part 3: the due_at chain across the full confusion ───────────────


_RETRY_DELAY = timedelta(seconds=5)


def _enqueue_args(scheduled_at: datetime) -> Any:
    from taskq.backend._protocol import EnqueueArgs

    return EnqueueArgs(
        id=new_uuid(),
        actor="due_at_probe",
        queue="default",
        payload={"x": 1},
        max_attempts=10,
        retry_kind="transient",
        scheduled_at=scheduled_at,
    )


async def _create_worker_with_actor(conn: Any, schema: str) -> Any:
    from taskq.testing.pg import create_worker

    worker_id = new_uuid()
    await create_worker(conn, schema, worker_id)
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ('due_at_probe', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    return worker_id


async def test_full_confusion_chain_rate_limit_denial_does_not_skew(
    clean_jobs_app: JobsApp,
) -> None:
    """The full confusion: claim -> fail-retry -> claim -> RATE-LIMIT
    DENIAL (no attempt row, the attempt increment refunded) -> claim ->
    snooze (an attempt-row reschedule arm) -> claim -> succeed.

    Pins, per attempt: due_at(k) is THAT attempt's own claim-time
    scheduled_at; the denial writes NO attempt row and its reschedule
    becomes the NEXT claim's honest due time; the snoozed arm stamps the
    pre-reschedule value via its same-snapshot CTE; and the chain
    reconstructs due_at(k) -> started_at(k) -> due_at(k+1)."""
    from taskq.backend._protocol import ErrorInfo

    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name

    first_due = datetime.now(UTC) - timedelta(hours=1)
    job_id = (await backend.enqueue(_enqueue_args(first_due))).id
    worker_id = await _create_worker_with_actor(deps.worker_pool, schema)

    async def _scheduled_at() -> datetime:
        async with deps.worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
        assert row is not None
        return row["scheduled_at"]

    async def _backdate_and_promote() -> None:
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() '
                "- interval '1 hour' WHERE id = $1",
                job_id,
            )
        await backend.scheduled_to_pending()

    # Attempt 1: claimed against the enqueue's scheduled_at, FAILS-RETRY.
    claimed = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert [j.attempt for j in claimed] == [1]
    due_1 = await _scheduled_at()
    assert due_1 == first_due
    await backend.mark_failed_or_retry(
        job_id,
        worker_id,
        ErrorInfo(error_class="BoomError", error_message="boom", error_traceback=None),
        retry_delay=_RETRY_DELAY,
        attempt=1,
        claim_epoch=claimed[0].claim_epoch,
    )
    rescheduled_1 = await _scheduled_at()
    assert rescheduled_1 > due_1

    # Attempt 2 (first mint): claimed against rescheduled_1, then the
    # RATE-LIMIT DENIAL - claimed, refunded, rescheduled, NO attempt row.
    # The denial's delay rides the 1s MIN_DEFERRAL floor (no backdate
    # here: the denial's own reschedule must be the value the NEXT claim
    # takes the row against, which is the skew being probed).
    await _backdate_and_promote()
    claimed = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert [j.attempt for j in claimed] == [2]
    denial_claim_time = await _scheduled_at()
    tri = await backend.mark_snoozed(
        job_id,
        worker_id,
        timedelta(seconds=1),
        outcome="rate_limit_denied",
        attempt=2,
        claim_epoch=claimed[0].claim_epoch,
    )
    assert tri == "scheduled"
    denial_reschedule = await _scheduled_at()
    assert denial_reschedule > denial_claim_time
    async with deps.worker_pool.acquire() as conn:
        n_rows = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        row_attempt = await conn.fetchval(
            f'SELECT attempt FROM "{schema}".jobs WHERE id = $1', job_id
        )
    assert n_rows == 1, "the denial writes NO attempt row"
    assert row_attempt == 1, "the denial refunds the claim's attempt increment"

    # Attempt 2 (RE-minted over the refunded number): claimed against the
    # DENIAL's reschedule - the row is left exactly as the denial left
    # it (the 1s deferral elapses, the promotion sweep moves it to
    # pending, no backdate touches the denial's reschedule).
    await asyncio.sleep(1.1)
    await backend.scheduled_to_pending()
    claimed = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert [j.attempt for j in claimed] == [2], (
        "the refunded attempt number is re-minted: the denial consumed nothing"
    )
    due_2 = await _scheduled_at()
    assert due_2 == denial_reschedule, (
        "attempt 2's claim-time due time is the denial's reschedule - the "
        "non-row did not skew the chain"
    )
    await backend.mark_retry_after(
        job_id,
        worker_id,
        delay=_RETRY_DELAY,
        consume_budget=True,
        attempt=2,
        claim_epoch=claimed[0].claim_epoch,
    )
    rescheduled_2 = await _scheduled_at()
    assert rescheduled_2 > due_2

    # Attempt 3: claimed against rescheduled_2, SUCCEEDS.
    await _backdate_and_promote()
    claimed = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert [j.attempt for j in claimed] == [3]
    due_3 = await _scheduled_at()
    await backend.mark_succeeded(
        job_id,
        worker_id,
        result={"ok": True},
        attempt=3,
        claim_epoch=claimed[0].claim_epoch,
    )

    rows = {r.attempt: r for r in await backend.get_attempts(job_id)}
    assert set(rows) == {1, 2, 3}, "the denial's non-row must not appear"
    assert rows[1].due_at == due_1
    assert rows[2].due_at == due_2
    assert rows[2].due_at != rescheduled_1, "never the pre-denial reschedule"
    assert rows[2].due_at != rescheduled_2, "never the snooze's own reschedule"
    assert rows[3].due_at == due_3
    # The chain reconstructs: every attempt started at/after its own due
    # time (attempt 2's due time is the denial's reschedule, which its
    # 1s deferral-spanning start honours without any scaffolding).
    for k in (1, 2, 3):
        assert rows[k].started_at >= (rows[k].due_at or first_due)
    assert rows[2].started_at >= denial_reschedule


# ── Part 4: the migration's checksum honesty ─────────────────────────


async def test_due_at_migration_checksum_honest_on_an_already_migrated_db(
    pg_dsn: str,
) -> None:
    """An ALREADY-migrated dev database (pre-01.00.20_04) upgrades: the
    runner applies exactly the PENDING TAIL of the bundled migrations —
    01.00.20_04 and every migration bundled after it (01.00.21_01
    today; the expectation is derived from ``discover()`` itself, so the
    next bundled migration extends this pin instead of breaking it) —
    records their rendered checksums, reports NO drift (the ledger is
    honest), and a second apply_pending is a clean no-op."""
    from taskq.migrate import apply_pending, checksum_drifts, discover

    schema = "ck_due_" + new_uuid().hex[:10]
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        # The world BEFORE the new file: every earlier migration, applied
        # raw with their ledger rows recorded (the dev DB's state).
        migrations = discover()
        index = next(i for i, m in enumerate(migrations) if m.version == "01.00.20_04")
        for earlier in migrations[:index]:
            await conn.execute(earlier.sql_template.format(schema=schema))
            await conn.execute(
                f'INSERT INTO "{schema}".schema_migrations (version, checksum) VALUES ($1, $2)',
                earlier.key,
                earlier.checksum(schema),
            )
        # The upgrade: EXACTLY the pending tail applies — everything from
        # the 01.00.20_04 cutoff onward (01.00.20_04 itself through the
        # bundled set's end: main's 01.00.21_01 archive keyset index
        # today). Derived from discover()'s own ordering, never a
        # hardcoded literal, so the next bundled migration extends the
        # expectation instead of re-breaking it.
        applied = await apply_pending(conn, schema=schema)
        assert [m.key for m in applied] == [m.key for m in migrations[index:]]

        # Every recorded checksum is the file's honest rendered checksum.
        for m in applied:
            recorded = await conn.fetchval(
                f'SELECT checksum FROM "{schema}".schema_migrations WHERE version = $1',
                m.key,
            )
            assert recorded == m.checksum(schema), f"{m.key}'s ledger row drifted"
        assert await checksum_drifts(conn, schema=schema) == {}

        # Idempotence at the runner level: nothing pending, no drift error.
        assert await apply_pending(conn, schema=schema) == []
        assert await checksum_drifts(conn, schema=schema) == {}
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── Part 5: the histogram's endpoint samples ─────────────────────────


def test_loop_idle_histogram_records_both_endpoints() -> None:
    """The 1.0 sample (never parked) and the 0.0 sample (never busy) both
    land in the bounded bucket set - neither clipped away, no unbounded
    tail, no dropped boundary value."""
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import HistogramDataPoint, InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("test", "0")
    hist = meter.create_histogram(
        "taskq.worker.loop_idle_fraction",
        explicit_bucket_boundaries_advisory=(
            0.0,
            0.05,
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
        ),
    )
    hist.record(1.0)  # a window where every poll read busy
    hist.record(0.0)  # a window where every poll read parked

    md = reader.get_metrics_data()
    assert md is not None
    points = [
        p
        for rm in md.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "taskq.worker.loop_idle_fraction"
        for p in m.data.data_points
        if isinstance(p, HistogramDataPoint)
    ]
    assert len(points) == 1
    point = points[0]
    assert sum(point.bucket_counts) == 2, (
        f"both endpoint samples recorded, got {point.bucket_counts}"
    )
    # The Python SDK's buckets are upper-inclusive: 0.0 lands in the FIRST
    # bucket (upper edge 0.0) and 1.0 in the bucket whose upper edge is
    # 1.0 - both inside the explicit set, the (+1.0, inf) tail EMPTY
    # (nothing spills past the bounded range).
    assert point.bucket_counts[0] == 1, "the never-busy 0.0 sample clipped away"
    assert point.bucket_counts[len(point.explicit_bounds) - 1] == 1, (
        "the never-parked 1.0 sample clipped away"
    )
    assert point.bucket_counts[-1] == 0, "a bounded fraction must never reach the tail"
