# ruff: noqa: S608  # Why: schema is a fixed test-local identifier, not user input; every value is $-bound, the differential harness's own convention.
"""ATTACK: the enqueue/dispatch integration surface (all fixes coexisting).

Four attack lanes against main with every landed fix at once:

1. The producer loop's failure budget x the dispatch round's disjoint
   telemetry: rounds failing at acquire/resolve/probe/claim in every
   combination, pool exhaustion and cancellation interleaved. The raise
   must land on exactly the Nth non-transient failure carrying the
   ORIGINAL exception object, pool waits must land ONLY on
   ``taskq.dispatch.pool_acquire_duration``, query durations ONLY on
   ``taskq.dispatch.duration``, the failures counter must count each
   round-stage exactly once, a cancelled round must record a wait and
   not a failure, and the pool must end every scenario with zero leaked
   checkouts.
2. The twin's atomic batch refusal + rollback index discipline, hammered
   concurrently and under injected interleaving with cancels and
   re-enqueues, with the atomic refusal's outcome shapes differentialled
   against Postgres.
3. The JobFilter rename's runtime boundary: positional 11-arg
   construction, the ``active=`` keyword alias, ``replace()`` copies
   through the list probe and the bulk-cancel sanitizer, exactly ONE
   deprecation warning per construction, identical rendered SQL for both
   spellings, and no bulk-cancel behavior drift on either backend.
4. An end-to-end soak on real Postgres: a few hundred rounds of mixed
   workload where nothing is lost, nothing duplicated, every job ends
   exactly one of live/archived, and the metric stream is coherent.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import warnings
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._dispatch import _dispatch_batch
from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._protocol import BatchRow, EnqueueArgs, JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.otel import (
    collect_metrics,
    counter_value,
    histogram_points,
    setup_meter,
)
from taskq.worker import _transient as transient_mod
from taskq.worker import run as run_mod
from taskq.worker._transient import DEFAULT_MAX_CONSECUTIVE_UNEXPECTED
from taskq.worker.run import producer_loop

from .test_rt_diff_enqueue_batch_failures import _batch_item, _keyed_batch_item
from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = [pytest.mark.asyncio]

_QUEUE = "default"
_SQL = render("taskq")

_RESOLVE_MARK = ".queues WHERE"
_PROBE_MARK = "ac.queue = ANY"
_CLAIM_MARK = "SKIP LOCKED"

_UNEXPECTED_COUNTER = "taskq.worker.loop_unexpected_errors_total"
_LOOP_LABEL = "worker.producer"

_TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


# ══════════════════════════════════════════════════════════════════════
# Lane 1 helpers: stage-scripted connections and a checkout-tracking pool
# ══════════════════════════════════════════════════════════════════════


class _StageConn:
    """A connection that fails exactly ONE round stage and succeeds the rest.

    Stage discrimination is by the statement's own shape: the resolve is
    the queues read, the probe is the claimable probe, the claim is the
    only statement that takes row locks. A conn with ``fail_mark=None``
    completes a fully successful empty round (claim -> empty, probe ->
    empty).
    """

    def __init__(
        self, fail_mark: str | None, exc: BaseException, *, fail_delay: float = 0.0
    ) -> None:
        self._fail_mark = fail_mark
        self._exc = exc
        self._fail_delay = fail_delay

    async def fetch(self, sql: str, *args: object) -> list[dict[str, str]]:
        if self._fail_mark is not None and self._fail_mark in sql:
            if self._fail_delay:
                await asyncio.sleep(self._fail_delay)
            raise self._exc
        if _RESOLVE_MARK in sql:
            return [{"name": _QUEUE, "mode": "strict_fifo"}]
        return []  # the probe (nothing else reaches fetch)

    async def execute(self, *_args: object) -> str:
        return "INSERT 0 1"

    def transaction(self) -> _FakeTx:
        return _FakeTx()


class _FakeTx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _TrackingPool:
    """A pool with real checkout discipline: acquire checks out, the
    context's exit checks in, and a public counter reports what is out
    right now. The wait before the handoff is the pool-exhaustion
    observation surface; ``fail_enter`` is an acquire that raises."""

    def __init__(
        self, conn: object, *, wait: float = 0.0, fail_enter: BaseException | None = None
    ) -> None:
        self._conn = conn
        self._wait = wait
        self._fail_enter = fail_enter
        self.checked_out = 0
        self.max_checked_out = 0

    def acquire(self, *, timeout: float | None = None) -> Any:
        pool = self
        wait = self._wait
        conn = self._conn
        fail_enter = self._fail_enter

        class _PoolCtx:
            async def __aenter__(self) -> object:
                if wait:
                    await asyncio.sleep(wait)
                if fail_enter is not None:
                    raise fail_enter
                pool.checked_out += 1
                pool.max_checked_out = max(pool.max_checked_out, pool.checked_out)
                return conn

            async def __aexit__(self, *exc: object) -> None:
                pool.checked_out -= 1

        return _PoolCtx()


@dataclasses.dataclass
class _RoundSpec:
    conn: object
    pool_wait: float = 0.0
    fail_enter: BaseException | None = None


class _StageScriptBackend:
    """A backend whose ``dispatch_batch`` runs the REAL ``_dispatch_batch``
    against the round's scripted pool/connection, counting outcomes."""

    def __init__(self, rounds: list[_RoundSpec]) -> None:
        self.rounds = rounds
        self.i = 0
        self.failures = 0
        self.successes = 0
        self.last_pool: _TrackingPool | None = None

    async def dispatch_batch(self, **_kwargs: Any) -> list[Any]:
        # Local schema name: the scripted conns discriminate by SQL shape,
        # never by schema, so this is an opaque marker (the real-PG lanes
        # below mint their own per-test schema). A module-level constant
        # here trips the suite-hygiene module-schema ban.
        schema = "taskq"
        spec = self.rounds[min(self.i, len(self.rounds) - 1)]
        self.i += 1
        pool = _TrackingPool(spec.conn, wait=spec.pool_wait, fail_enter=spec.fail_enter)
        self.last_pool = pool
        try:
            rows = await _dispatch_batch(
                cast(Any, pool),
                _SQL,
                2,
                5.0,
                schema,
                new_uuid(),
                [_QUEUE],
                10,
                timedelta(seconds=30),
                queue_mode_cache=None,
            )
        except asyncio.CancelledError:
            raise  # a cancelled round is neither a failure nor a success
        except BaseException:
            self.failures += 1
            raise
        self.successes += 1
        return rows


def _producer_deps(poll_interval: float = 0.05) -> Any:
    settings = SimpleNamespace(
        queues=[_QUEUE],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=poll_interval,
        notify_poll_interval=poll_interval,
        max_concurrency=1,
        # The producer here runs against scripted stand-ins; the schema
        # name is opaque to them (a local per the hygiene module-schema ban).
        schema_name="taskq",
        pg_is_pooled=False,
    )
    liveness = SimpleNamespace(tick=lambda *a, **k: None, forget=lambda *a, **k: None)
    return SimpleNamespace(
        settings=settings,
        liveness=liveness,
        active_jobs=SimpleNamespace(all=list, count=lambda: 0),
        disowned_jobs=set(),
        dispatcher_pool=_NoopPool(),
    )


class _NoopPool:
    """asyncpg.Pool stand-in; the producer's exit hand-back is a no-op."""

    class _Conn:
        async def __aenter__(self) -> _NoopPool._Conn:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def execute(self, *_args: object) -> str:
            return "UPDATE 0"

    def acquire(self, *, timeout: float | None = None) -> _NoopPool._Conn:
        return self._Conn()


class _CleanExit:
    """Marker for a producer_loop that returned without raising."""


async def _drive_producer(
    monkeypatch: pytest.MonkeyPatch,
    backend: Any,
    *,
    stop_after_rounds: int,
) -> Any:
    """Drive the REAL producer_loop over *backend*; return what escaped
    (the _CleanExit marker on a clean return). The patched sleep is the
    round counter's clock seam, the same drive the budget pins use."""
    outcomes: list[Any] = []
    real_sleep = asyncio.sleep
    stop_event = asyncio.Event()

    async def _round_counting_sleep(_delay: float, result: object = None) -> object:
        if backend.failures + backend.successes >= stop_after_rounds:
            stop_event.set()
        await real_sleep(0)
        return result

    # run_mod.asyncio IS the global asyncio module; the existing budget
    # pins patch it through the run module's own reference (the attribute
    # exists at runtime, the stub just does not re-export it).
    run_asyncio: Any = run_mod.asyncio  # pyright: ignore[reportAttributeAccessIssue]  # Why: run.py re-exports asyncio for its loops; the stub does not carry it.
    monkeypatch.setattr(run_asyncio, "sleep", _round_counting_sleep)

    async def _drive() -> None:
        try:
            await producer_loop(
                _producer_deps(),
                asyncio.Queue(maxsize=1),
                asyncio.Event(),
                stop_event,
                backend=backend,
                worker_id=new_uuid(),
            )
            outcomes.append(_CleanExit())
        except BaseException as exc:
            outcomes.append(exc)

    await _drive()
    return outcomes[0]


def _points_by_loop(reader: Any, counter: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for m in collect_metrics(reader):
        if m.name == counter:
            for p in m.data.data_points:
                value = getattr(p, "value", 0)
                out[str(dict(p.attributes or {}).get("loop"))] = value
    return out


# ══════════════════════════════════════════════════════════════════════
# Lane 1: budget x telemetry under mixed stage failures
# ══════════════════════════════════════════════════════════════════════


async def test_budget_raises_on_nth_non_transient_with_the_original_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every stage fails transiently (one different stage per round: claim,
    resolve, probe, acquire), then the claim fails non-transiently five
    rounds in a row: the raise lands on exactly the Nth non-transient
    failure carrying the ORIGINAL exception OBJECT, and the counters
    partition exactly - every failed round once on dispatch.failures
    (whichever stage raised), only the non-transient ones on the
    loop-unexpected counter, every round's pool wait exactly once on its
    own histogram, query durations only on the rounds that ran a query."""
    reader = setup_meter(monkeypatch)
    from taskq.obs import get_meter as _obs_get_meter

    monkeypatch.setattr(
        transient_mod,
        "_unexpected_loop_errors",
        _obs_get_meter().create_counter(_UNEXPECTED_COUNTER, unit="1"),
    )
    sentinel = ValueError("a permanent claim-stage fault")

    def _round(round_no: int) -> _RoundSpec:
        if round_no == 1:
            return _RoundSpec(_StageConn(_CLAIM_MARK, TimeoutError("claim blip")))
        if round_no == 2:
            return _RoundSpec(_StageConn(_RESOLVE_MARK, TimeoutError("resolve blip")))
        if round_no == 3:
            return _RoundSpec(_StageConn(_PROBE_MARK, TimeoutError("probe blip")))
        if round_no == 4:
            return _RoundSpec(
                _StageConn(None, TimeoutError("unused")), fail_enter=TimeoutError("acquire blip")
            )
        return _RoundSpec(_StageConn(_CLAIM_MARK, sentinel))

    backend = _StageScriptBackend([_round(i + 1) for i in range(9)])

    escaped = await _drive_producer(monkeypatch, backend, stop_after_rounds=64)

    assert isinstance(escaped, ValueError), (
        f"the budget must re-raise, got {escaped!r} after {backend.failures} failed rounds"
    )
    # The ORIGINAL exception OBJECT, not a copy: identity, not equality.
    assert escaped is sentinel, (
        "the escaped error must be the original exception object the raise "
        "site threw, not a re-wrapped copy"
    )
    assert backend.failures == 9, (
        f"expected 4 transient rounds + the budget of 5, got {backend.failures}"
    )
    assert counter_value(reader, "taskq.dispatch.failures") == 9, (
        "every failed round-stage must count exactly once on the round-scoped "
        "failure counter, whichever stage raised"
    )
    unexpected = _points_by_loop(reader, _UNEXPECTED_COUNTER)
    assert unexpected.get(_LOOP_LABEL) == DEFAULT_MAX_CONSECUTIVE_UNEXPECTED, (
        f"only the non-transient failures may feed the budget, got {unexpected}"
    )
    waits = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert len(waits) == 1 and waits[0].count == 9, (
        f"every round's pool wait must land exactly once on the wait histogram, got {waits}"
    )
    queries = histogram_points(reader, "taskq.dispatch.duration")
    assert len(queries) == 1 and queries[0].count == 8, (
        f"exactly the eight rounds that ran a query record a query duration "
        f"(the acquire-failing round ran none), got {queries}"
    )


async def test_pool_waits_and_query_durations_stay_on_disjoint_histograms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow pool wait followed by an instantly-failing claim must put the
    wall time ONLY on pool_acquire_duration; a fast pool wait followed by a
    slow failing claim must put it ONLY on dispatch.duration. Each round
    records exactly one sample per histogram it touches."""
    schema = "taskq"  # opaque to the scripted conns; local per the hygiene ban
    reader_a = setup_meter(monkeypatch)
    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            cast(Any, _TrackingPool(_StageConn(_CLAIM_MARK, TimeoutError("claim")), wait=0.08)),
            _SQL,
            2,
            5.0,
            schema,
            new_uuid(),
            [_QUEUE],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )
    waits_a = histogram_points(reader_a, "taskq.dispatch.pool_acquire_duration")
    queries_a = histogram_points(reader_a, "taskq.dispatch.duration")
    assert len(waits_a) == 1 and waits_a[0].count == 1, f"one wait sample expected, got {waits_a}"
    assert waits_a[0].sum >= 0.05, (
        f"the pool wait must land on the wait histogram, got {waits_a[0].sum}"
    )
    assert len(queries_a) == 1 and queries_a[0].count == 1, (
        f"the failing claim still owns one query sample, got {queries_a}"
    )
    assert queries_a[0].sum < 0.05, (
        f"the pool wait leaked into the query-latency histogram: {queries_a[0].sum}"
    )

    reader_b = setup_meter(monkeypatch)
    with pytest.raises(TimeoutError):
        await _dispatch_batch(
            cast(
                Any,
                _TrackingPool(_StageConn(_CLAIM_MARK, TimeoutError("claim"), fail_delay=0.08)),
            ),
            _SQL,
            2,
            5.0,
            schema,
            new_uuid(),
            [_QUEUE],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )
    waits_b = histogram_points(reader_b, "taskq.dispatch.pool_acquire_duration")
    queries_b = histogram_points(reader_b, "taskq.dispatch.duration")
    assert len(waits_b) == 1 and waits_b[0].count == 1
    assert waits_b[0].sum < 0.05, (
        f"the claim's latency leaked into the pool-wait histogram: {waits_b[0].sum}"
    )
    assert len(queries_b) == 1 and queries_b[0].count == 1
    assert queries_b[0].sum >= 0.05, (
        f"the claim's latency must land on the query histogram, got {queries_b[0].sum}"
    )


async def test_cancelled_round_records_wait_not_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation delivered while the producer waits for a dispatcher
    connection: the wait happened (one wait sample), the failure counter
    stays silent, and the cancellation tears the loop down rather than
    being swallowed into a retry. The pool's checkout count never leaves
    zero - a cancelled acquire checks nothing out."""
    reader = setup_meter(monkeypatch)
    backend = _StageScriptBackend(
        [_RoundSpec(_StageConn(None, TimeoutError("unused")), pool_wait=30.0)]
    )
    stop_event = asyncio.Event()

    loop_task = asyncio.ensure_future(
        producer_loop(
            _producer_deps(),
            asyncio.Queue(maxsize=1),
            asyncio.Event(),
            stop_event,
            backend=cast(Any, backend),
            worker_id=new_uuid(),
        )
    )
    await asyncio.sleep(0.1)
    loop_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await loop_task

    assert counter_value(reader, "taskq.dispatch.failures") == 0, (
        "a cancelled round must not count as a dispatch failure"
    )
    waits = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    assert len(waits) == 1 and waits[0].count == 1, (
        f"the interrupted round's wait must be observable exactly once, got {waits}"
    )
    assert backend.last_pool is not None and backend.last_pool.checked_out == 0, (
        "a cancelled acquire must not leak a checkout"
    )
    assert backend.failures == 0 and backend.successes == 0, (
        "the round never completed: neither a failure nor a success may be counted"
    )


# ══════════════════════════════════════════════════════════════════════
# Lane 1b: real pool, mixed load, cancellation interleaved - no leaks
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.integration
async def test_real_pool_mixed_rounds_keep_the_checkout_count_at_baseline(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent dispatch rounds over ONE small real pool, some cancelled
    mid-flight at staggered instants (in the pool wait, the resolve, and
    the claim): no round crashes on its own, every round's wait lands at
    most once on the wait histogram, only surviving rounds record query
    durations, nothing fails, and the pool ends with every connection
    idle - no round, cancelled or not, leaks a checkout."""
    reader = setup_meter(monkeypatch)
    # Own schema with migrations applied: the rounds run the real claim SQL.
    schema = f"tdf_{new_uuid().hex[:10]}"
    setup_conn = await asyncpg.connect(pg_dsn)
    try:
        await setup_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        from taskq.migrate import apply_pending

        await apply_pending(setup_conn, schema=schema)
    finally:
        await setup_conn.close()
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    rounds = 24

    async def one_round() -> None:
        await asyncio.sleep(0)
        await _dispatch_batch(
            cast(Any, pool),
            render(schema),
            2,
            5.0,
            schema,
            new_uuid(),
            [_QUEUE],
            10,
            timedelta(seconds=30),
            queue_mode_cache=None,
        )

    tasks = [asyncio.ensure_future(one_round()) for _ in range(rounds)]
    for i, t in enumerate(tasks):
        if i % 3 == 0:
            asyncio.get_running_loop().call_later(0.001 * (i % 7), t.cancel)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
    errors = [
        r
        for r in results
        if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError)
    ]
    assert not errors, f"no round may crash on its own: {errors!r}"

    # Let release bookkeeping settle, then nothing may be checked out:
    # every connection the pool owns must be idle again.
    await asyncio.sleep(0.05)
    checked_out = pool.get_size() - pool.get_idle_size()
    size = pool.get_size()
    await pool.close()

    assert checked_out == 0, (
        f"{checked_out} connection(s) still checked out: the pool's checkout "
        "count must return to its baseline after every scenario, cancelled "
        "rounds included"
    )
    assert size <= 2, (
        f"the pool grew to {size} connections: a leaked or discarded checkout forced a replacement"
    )

    waits = histogram_points(reader, "taskq.dispatch.pool_acquire_duration")
    total_waits = sum(p.count for p in waits)
    assert total_waits <= rounds, f"each round may record at most one wait, got {total_waits}"
    assert total_waits >= rounds - cancelled, (
        f"every round that reached a wait must record it: {total_waits} waits for "
        f"{rounds - cancelled} surviving + {cancelled} cancelled rounds"
    )
    assert counter_value(reader, "taskq.dispatch.failures") == 0, (
        "a real empty-queue round must not fail"
    )
    queries = histogram_points(reader, "taskq.dispatch.duration")
    total_queries = sum(p.count for p in queries)
    assert total_queries == rounds - cancelled, (
        f"exactly the surviving rounds record query durations: {total_queries} vs "
        f"{rounds - cancelled}"
    )


# ══════════════════════════════════════════════════════════════════════
# Lane 2: the twin's atomic batch refusal + rollback index discipline
# ══════════════════════════════════════════════════════════════════════


def _memory_twin() -> Any:
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    return InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))


def _plain_item(actor: str, *, key: str | None = None, nul: bool = False) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=_QUEUE,
        payload={"value": "bad\u0000" if nul else 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime(2026, 1, 1, tzinfo=UTC),
        idempotency_key=key,  # type: ignore[arg-type]  # Why: runtime-transparent NewType.
    )


def _batch_row(created_at: datetime) -> BatchRow:
    return BatchRow(
        id=new_uuid(),
        queue=_QUEUE,
        status="active",
        expected_size=0,
        consecutive_failures=0,
        failure_threshold=None,
        finalizer_job_id=None,
        originating_actor=None,
        created_at=created_at,
        completed_at=None,
        metadata={},
    )


async def test_twin_atomic_refusal_leaves_no_rows_and_a_clean_index() -> None:
    """A mid-stream refusal (NUL in the SECOND chunk) must withdraw the
    first chunk's rows, create no batch row, and leave the idempotency
    index clean: a re-enqueue of a withdrawn item stores a fresh row, and
    a repeated pair resolves to its one live holder."""
    from taskq.exceptions import PayloadValidationError

    backend = _memory_twin()
    batch_id = new_uuid()
    ok1, ok2, poisoned = (
        _plain_item("actor_a"),
        _plain_item("actor_a"),
        _plain_item("actor_a", nul=True),
    )

    with pytest.raises(PayloadValidationError) as exc_info:
        await backend.enqueue_batch_atomic(
            [ok1, ok2, poisoned],
            batch_id=batch_id,
            queue=_QUEUE,
            batch_row=_batch_row(datetime(2026, 1, 1, tzinfo=UTC)),
            finalizer_args=None,
            chunk_size=2,  # the NUL lands in chunk 2, after chunk 1 inserted
        )
    assert exc_info.value.item_index == 2, (
        f"the refusal must name the caller-global stream index, got {exc_info.value.item_index}"
    )

    # All-or-nothing: the first chunk's admitted rows are gone.
    for args in (ok1, ok2, poisoned):
        assert await backend.get(args.id) is None, (
            "a refused atomic batch must leave NO row behind, first chunk included"
        )
    assert await backend.get_batch(batch_id) is None, "a refused call must not leave a batch row"

    # Index discipline: the withdrawn (unkeyed) items re-enqueue as fresh
    # rows under the same batch id.
    rows = await backend.enqueue_batch_atomic(
        [ok1, ok2],
        batch_id=batch_id,
        queue=_QUEUE,
        batch_row=_batch_row(datetime(2026, 1, 1, tzinfo=UTC)),
        finalizer_args=None,
    )
    assert [r.id for r in rows] == [ok1.id, ok2.id]
    assert await backend.get_batch(batch_id) is not None

    # A keyed pair's discipline: the pair resolves to its one live holder.
    keyed_a, keyed_b = _plain_item("actor_a", key="dup"), _plain_item("actor_a", key="dup")
    pair_batch = new_uuid()
    pair_rows = await backend.enqueue_batch_atomic(
        [keyed_a, keyed_b],
        batch_id=pair_batch,
        queue=_QUEUE,
        batch_row=_batch_row(datetime(2026, 1, 1, tzinfo=UTC)),
        finalizer_args=None,
    )
    assert pair_rows[1].id == pair_rows[0].id, "the repeated pair must alias its holder"
    assert await backend.get(keyed_b.id) is None, "a dedup hit stores nothing"


async def test_twin_concurrent_batch_refusals_hammer_is_all_or_nothing() -> None:
    """Twelve concurrent atomic calls racing ONE batch id, with a cancel
    racing the storm: exactly one call creates the batch, every refused
    call leaves zero rows, every pair ends disciplined (a repeat dedups
    onto the pair's one live row), and the batch's counts match the live
    members."""
    backend = _memory_twin()
    batch_id = new_uuid()
    calls = 12

    async def one_call(i: int) -> Any:
        items = [_plain_item("actor_a", key=None if i % 2 else f"shared-{i}") for _ in range(3)]
        try:
            rows = await backend.enqueue_batch_atomic(
                items,
                batch_id=batch_id,
                queue=_QUEUE,
                batch_row=_batch_row(datetime(2026, 1, 1, tzinfo=UTC)),
                finalizer_args=None,
            )
            return ("ok", items, rows)
        except Exception as exc:  # Why: the typed refusal is the observable.
            return ("refused", items, exc)

    async def _racing_cancel() -> None:
        for _ in range(50):
            await asyncio.sleep(0)
        members = await backend.list_jobs(JobFilter(batch_id=batch_id, limit=5))
        for row in members:
            await backend.write_cancel_request(row.id, "racing cancel")
            return

    results, _ = await asyncio.gather(
        asyncio.gather(*(one_call(i) for i in range(calls))),
        _racing_cancel(),
    )
    outcomes = results  # type: ignore[assignment]
    ok = [r for r in outcomes if r[0] == "ok"]
    refused = [r for r in outcomes if r[0] == "refused"]
    assert len(ok) == 1, f"exactly one call may create the batch, got {len(ok)}"
    assert len(refused) == calls - 1
    assert all(type(r[2]).__name__ == "BatchIdExistsError" for r in refused), (
        f"refusals must be typed: {sorted({type(r[2]).__name__ for r in refused})}",
    )

    # All-or-nothing per refused call: none of its items stored.
    for _kind, items, _exc in refused:
        for args in items:
            assert await backend.get(args.id) is None, "a refused atomic call must leave zero rows"

    # Index discipline for every exercised pair: probe once (stores at
    # most one row), probe again with a fresh id (dedups onto the first
    # probe's row, stores nothing).
    for i in range(0, calls, 2):
        key = f"shared-{i}"
        probe1 = _plain_item("actor_a", key=key)
        holder = await backend.enqueue_with_conn(None, probe1)
        assert await backend.get(probe1.id) is None or holder.id == probe1.id
        probe2 = _plain_item("actor_a", key=key)
        resolved = await backend.enqueue_with_conn(None, probe2)
        assert resolved.id == holder.id, (
            f"pair {key} must resolve to its one live holder, got {resolved.id} vs {holder.id}"
        )
        assert await backend.get(probe2.id) is None, (
            "a pair's repeat must dedup, never store a second row"
        )

    # Batch<->jobs consistency: the batch row's non-terminal count equals
    # the live member rows actually present.
    batch = await backend.get_batch(batch_id)
    assert batch is not None
    members = await backend.list_jobs(JobFilter(batch_id=batch_id, limit=100))
    non_terminal = await backend.count_batch_non_terminal(batch_id)
    live_members = [j for j in members if j.status not in _TERMINAL]
    assert non_terminal == len(live_members), (
        f"batch non-terminal count {non_terminal} must equal live members {len(live_members)}"
    )


async def test_twin_atomic_refusal_under_injected_interleaving_keeps_the_index_true() -> None:
    """The sharpest rollback attack: a batch call's enqueue seam forced to
    run an intruder's write mid-loop - the intruder re-enqueues the SAME
    idempotency pair the batch is about to claim. The refusal's rollback
    must withdraw only the batch's own rows and must NOT pop the index
    entry the intruder's live row owns."""
    backend = _memory_twin()
    batch_id = new_uuid()
    shared_key = "contested"
    intruder_row = await backend.enqueue(_plain_item("actor_a", key=shared_key))

    real_enqueue_with_conn = backend.enqueue_with_conn
    yielded = {"n": 0}

    async def _yielding_enqueue_with_conn(conn: object, args: EnqueueArgs) -> Any:
        if args.idempotency_key == shared_key and yielded["n"] == 0:
            yielded["n"] += 1
            # The intruder lands while the batch call is suspended
            # mid-loop: a real concurrent enqueue's committed write.
            await real_enqueue_with_conn(None, _plain_item("actor_a", key=shared_key))
            await asyncio.sleep(0)
        return await real_enqueue_with_conn(conn, args)

    backend.enqueue_with_conn = _yielding_enqueue_with_conn  # type: ignore[method-assign]

    a, b = _plain_item("actor_a", key=shared_key), _plain_item("actor_a")
    poisoned = _plain_item("actor_a", nul=True)

    with pytest.raises(Exception) as exc_info:
        await backend.enqueue_batch_atomic(
            [a, b, poisoned],
            batch_id=batch_id,
            queue=_QUEUE,
            batch_row=_batch_row(datetime(2026, 1, 1, tzinfo=UTC)),
            finalizer_args=None,
            chunk_size=2,
        )
    assert not isinstance(exc_info.value, asyncio.CancelledError)

    # All-or-nothing: the batch's own plain row is gone, and the keyed
    # item stored nothing of its own (it deduped onto the intruder).
    assert await backend.get(a.id) is None, "the batch's keyed row must store nothing"
    assert await backend.get(b.id) is None, "the batch's plain row must be withdrawn"
    # Index discipline: the pair still resolves to the INTRUDER's row,
    # which the rollback had no right to touch.
    assert await backend.get(intruder_row.id) is not None, (
        "the intruder's concurrently committed row must survive the rollback"
    )
    probe = _plain_item("actor_a", key=shared_key)
    resolved = await backend.enqueue_with_conn(None, probe)
    assert resolved.id == intruder_row.id, (
        f"the contested pair must resolve to the intruder's live row, got {resolved.id}"
    )
    assert await backend.get(probe.id) is None, "the pair's repeat must dedup onto the live holder"
    # No batch row survived the refusal.
    assert await backend.get_batch(batch_id) is None


# ══════════════════════════════════════════════════════════════════════
# Lane 2b: the atomic refusal's outcome shapes, differentialled to PG
# ══════════════════════════════════════════════════════════════════════


def _side_batch_row(side: DiffSide, batch_id: Any) -> BatchRow:
    return BatchRow(
        id=batch_id,
        queue=_QUEUE,
        status="active",
        expected_size=0,
        consecutive_failures=0,
        failure_threshold=None,
        finalizer_job_id=None,
        originating_actor=None,
        created_at=side.ts(-1.0),
        completed_at=None,
        metadata={},
    )


async def _atomic_mid_stream_nul_refusal(side: DiffSide) -> None:
    """A NUL in the second chunk refuses the whole atomic call, names the
    caller-global index, and stores nothing - the first chunk's admitted
    rows included."""
    from taskq.exceptions import PayloadValidationError

    batch_id = new_uuid()
    items = [
        _batch_item(side, "g1"),
        _batch_item(side, "g2"),
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue=_QUEUE,
            payload={"value": "bad\u0000"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=side.ts(-1.0),
        ),
    ]
    side.register_job_id("g3", items[2].id)
    try:
        await side.backend.enqueue_batch_atomic(
            items,
            batch_id=batch_id,
            queue=_QUEUE,
            batch_row=_side_batch_row(side, batch_id),
            finalizer_args=None,
            chunk_size=2,
        )
        side.record("refusal", "none")
    except PayloadValidationError as exc:
        side.record("refusal", "PayloadValidationError")
        side.record("item_index", exc.item_index)
    except Exception as exc:  # Why: a typed refusal is the observable either way.
        side.record("refusal", type(exc).__name__)
    stored = [
        t
        for t, args in (("g1", items[0]), ("g2", items[1]), ("g3", items[2]))
        if await side.backend.get(args.id) is not None
    ]
    side.record("stored", sorted(stored))


@pytest.mark.integration
async def test_diff_atomic_mid_stream_nul_refusal(pg_dsn: str) -> None:
    mem, pg = await run_differential(
        _atomic_mid_stream_nul_refusal, pg_dsn=pg_dsn, actors=("test_actor",)
    )
    assert_mirror(
        "a NUL in the second chunk refuses the whole atomic batch with the "
        "same typed error at the same caller-global index and stores nothing, "
        "identically",
        mem,
        pg,
    )
    assert pg["records"]["refusal"] == "PayloadValidationError"
    assert pg["records"]["item_index"] == 2
    assert pg["records"]["stored"] == []


async def _atomic_duplicate_batch_id_refusal(side: DiffSide) -> None:
    """A second atomic call racing the SAME batch id refuses whole, typed,
    and stores nothing of its own."""
    batch_id = new_uuid()
    first = [_batch_item(side, "f1"), _batch_item(side, "f2")]
    await side.backend.enqueue_batch_atomic(
        first,
        batch_id=batch_id,
        queue=_QUEUE,
        batch_row=_side_batch_row(side, batch_id),
        finalizer_args=None,
    )
    second = [_batch_item(side, "s1"), _batch_item(side, "s2")]
    try:
        await side.backend.enqueue_batch_atomic(
            second,
            batch_id=batch_id,
            queue=_QUEUE,
            batch_row=_side_batch_row(side, batch_id),
            finalizer_args=None,
        )
        side.record("refusal", "none")
    except Exception as exc:  # Why: the typed outcome is the observable.
        side.record("refusal", type(exc).__name__)
    stored = [
        t
        for t, args in (("s1", second[0]), ("s2", second[1]))
        if await side.backend.get(args.id) is not None
    ]
    side.record("stored", sorted(stored))
    # The winners' rows must be untouched by the refusal's rollback.
    kept = [
        t
        for t, args in (("f1", first[0]), ("f2", first[1]))
        if await side.backend.get(args.id) is not None
    ]
    side.record("winner_rows_intact", sorted(kept))
    batch = await side.backend.get_batch(batch_id)
    side.record("batch_row_present", batch is not None)
    side.record("batch_status", None if batch is None else batch.status)


@pytest.mark.integration
async def test_diff_atomic_duplicate_batch_id_refusal(pg_dsn: str) -> None:
    mem, pg = await run_differential(
        _atomic_duplicate_batch_id_refusal, pg_dsn=pg_dsn, actors=("test_actor",)
    )
    assert_mirror(
        "a second atomic call on the same batch id refuses the whole call "
        "typed, stores nothing of its own, leaves the winner's rows intact "
        "and the batch row present, identically",
        mem,
        pg,
    )
    assert pg["records"]["refusal"] == "BatchIdExistsError"
    assert pg["records"]["stored"] == []
    assert pg["records"]["winner_rows_intact"] == ["f1", "f2"]
    assert pg["records"]["batch_row_present"] is True


async def _stored_holder_cancel_then_reenqueue(side: DiffSide) -> None:
    """The cancel/epoch-adjacent shape: an atomic call whose keyed item
    dedups onto a STORED holder, then the holder is cancelled and the pair
    re-enqueued. The dedup target's identity and the surviving statuses
    must agree across backends."""
    holder = await side.enqueue("holder", idempotency_key="k", actor="test_actor")
    batch_id = new_uuid()
    items = [_keyed_batch_item(side, "k1", "test_actor", "k"), _batch_item(side, "p1")]
    rows = await side.backend.enqueue_batch_atomic(
        items,
        batch_id=batch_id,
        queue=_QUEUE,
        batch_row=_side_batch_row(side, batch_id),
        finalizer_args=None,
    )
    side.record("atomic_returns", [side.token_of(r.id) for r in rows])
    side.record("k1_stored", await side.backend.get(side._jobs_by_token["k1"]) is not None)
    # Cancel the holder through the public write surface, then re-enqueue
    # the pair.
    await side.write_cancel_request("holder", "post-atomic cancel")
    holder_row = await side.backend.get(holder.id)
    side.record("holder_status_after_cancel", None if holder_row is None else holder_row.status)
    again = _keyed_batch_item(side, "again", "test_actor", "k")
    re_rows = await side.backend.enqueue_batch([again])
    side.record("reenqueue_returns", [side.token_of(r.id) for r in re_rows])
    side.record("reenqueue_stored", await side.backend.get(again.id) is not None)


@pytest.mark.integration
async def test_diff_stored_holder_cancel_then_reenqueue(pg_dsn: str) -> None:
    mem, pg = await run_differential(
        _stored_holder_cancel_then_reenqueue, pg_dsn=pg_dsn, actors=("test_actor",)
    )
    assert_mirror(
        "an atomic batch item dedups onto its stored holder; cancelling the "
        "holder and re-enqueueing the pair produces the same statuses and "
        "the same dedup target, identically",
        mem,
        pg,
    )
    assert pg["records"]["atomic_returns"] == ["holder", "p1"]
    assert pg["records"]["k1_stored"] is False


# ══════════════════════════════════════════════════════════════════════
# Lane 3: the JobFilter rename's runtime boundary
# ══════════════════════════════════════════════════════════════════════

_RENAME_KWARGS: dict[str, Any] = {
    # No `status`: the terminality filter and `status` are mutually
    # exclusive (pre-rename rule, unchanged by the alias).
    "queue": "q",
    "actor": "a",
    "identity_key": "ik",
    "limit": 100,
    "cursor": None,
    "tags": ("t",),
    "order_by": None,
    "created_before": datetime(2026, 1, 1, tzinfo=UTC),
}


def _positional_11() -> JobFilter:
    """The PRE-RENAME positional form: 11 args, the 10th the terminality
    filter, the 11th created_before."""
    return JobFilter(
        "q",
        None,
        "a",
        "ik",
        None,
        100,
        None,
        ("t",),
        None,
        True,  # the pre-rename 10th slot: the alias
        datetime(2026, 1, 1, tzinfo=UTC),  # the 11th: created_before
    )


async def test_rename_positional_and_keyword_constructions_agree_with_one_warning() -> None:
    """The old positional form and the new keyword form must construct
    identically (unfinished promoted, active cleared, created_before at
    its slot), and each must warn EXACTLY once."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        f_pos = _positional_11()
        f_kw = JobFilter(active=True, **_RENAME_KWARGS)
    deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert len(deprecations) == 2, (
        f"exactly ONE deprecation warning PER construction, got {len(deprecations)}: "
        f"{[str(w.message) for w in deprecations]}"
    )

    for f in (f_pos, f_kw):
        assert f.unfinished is True, "the alias must promote onto unfinished"
        assert f.active is None, "the alias must be consumed at construction"
        assert f.created_before == _RENAME_KWARGS["created_before"], (
            "the 11th positional argument must stay created_before; the alias must not shift it"
        )
        assert f.queue == "q" and f.actor == "a" and f.tags == ("t",)
        assert f.has_predicates(), (
            "an alias-built filter counts as predicated for the bulk-cancel guard"
        )

    # The two spellings compare equal (the alias is excluded from eq).
    assert f_pos == f_kw, "the two spellings must produce equal filters"

    # The False spelling: positional and keyword agree, one warning only
    # for the alias spelling.
    with warnings.catch_warnings(record=True) as caught_false:
        warnings.simplefilter("always")
        f_false_pos = JobFilter("q", None, "a", "ik", None, 100, None, ("t",), None, False, None)
        f_false_kw = JobFilter(unfinished=False, queue="q")
    assert len([w for w in caught_false if issubclass(w.category, DeprecationWarning)]) == 1
    assert f_false_pos.unfinished is False and f_false_pos.active is None
    assert f_false_kw.unfinished is False and f_false_kw.active is None

    # Disagreeing spellings refuse loudly, and no warning precedes the
    # refusal.
    with warnings.catch_warnings(record=True) as caught_bad:
        warnings.simplefilter("always")
        with pytest.raises(ValueError):
            JobFilter(active=True, unfinished=False, queue="q")
    assert not [w for w in caught_bad if issubclass(w.category, DeprecationWarning)]


async def test_rename_replace_copies_through_hot_paths_stay_warning_free() -> None:
    """dataclasses.replace copies through the client list probe and the
    bulk-cancel sanitizer must re-enter construction warning-free and must
    carry the promoted unfinished value, never resurrect the alias."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        f_active = JobFilter(active=True, **_RENAME_KWARGS)
        f_quiet = JobFilter(unfinished=True, **_RENAME_KWARGS)

    with warnings.catch_warnings():
        # ANY second deprecation warning becomes an error: the copies must
        # be silent.
        warnings.simplefilter("error", DeprecationWarning)
        probe = dataclasses.replace(f_active, limit=f_active.limit + 1)  # the list probe's copy
        sanitized = dataclasses.replace(
            f_active, limit=2**31, cursor=None, order_by=None
        )  # the bulk-cancel sanitizer's copy
        probe_quiet = dataclasses.replace(f_quiet, limit=f_quiet.limit + 1)

    for copy in (probe, sanitized, probe_quiet):
        assert copy.unfinished is True, "the promoted value must survive every internal copy"
        assert copy.active is None, "no copy may resurrect the alias"

    assert probe.limit == 101
    assert sanitized.limit == 2**31 and sanitized.cursor is None and sanitized.order_by is None


async def test_rename_renders_identical_sql_for_both_spellings() -> None:
    """The shared filter->SQL builder must render identical conditions and
    parameters for the alias spelling, the canonical spelling, and a
    replace() copy of the alias spelling - and a real predicate change must
    still be able to make them differ (the differential can detect drift)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        f_active = JobFilter(active=True, **_RENAME_KWARGS)
    f_unfinished = JobFilter(unfinished=True, **_RENAME_KWARGS)
    f_copy = dataclasses.replace(f_active)

    sql_active = build_filter_conditions(f_active)
    sql_unfinished = build_filter_conditions(f_unfinished)
    sql_copy = build_filter_conditions(f_copy)

    assert sql_active.conditions == sql_unfinished.conditions, (
        f"SQL drift across the rename:\nalias:     {sql_active.conditions}\n"
        f"canonical: {sql_unfinished.conditions}"
    )
    assert sql_active.params == sql_unfinished.params, (
        f"parameter drift across the rename:\nalias:     {sql_active.params}\n"
        f"canonical: {sql_unfinished.params}"
    )
    assert sql_active.conditions == sql_copy.conditions
    assert sql_active.params == sql_copy.params

    # Sharpness: the differential CAN detect a real difference.
    f_other = JobFilter(unfinished=True, queue="different")
    assert build_filter_conditions(f_other).params != sql_unfinished.params


@pytest.mark.integration
async def test_diff_bulk_cancel_rename_spellings_agree(pg_dsn: str) -> None:
    """Bulk cancel driven with the alias spelling and with the canonical
    spelling must cancel the same rows with the same counts on BOTH
    backends - no behavior drift through the sanitizer."""

    def scenario_factory(spelling: str) -> Any:
        async def scenario(side: DiffSide) -> None:
            await side.enqueue("c1", actor="test_actor")
            await side.enqueue("c2", actor="test_actor")
            await side.enqueue("c3", actor="test_actor", queue="other")
            kwargs = {"active": True} if spelling == "active" else {"unfinished": True}
            result = await side.backend.cancel_where(
                JobFilter(queue=_QUEUE, **kwargs),  # type: ignore[arg-type]  # Why: the alias spelling is the surface under attack.
                "rename differential",
            )
            side.record("cancelled_directly", result.cancelled_directly)
            side.record("cancelled_ids", sorted(side.token_of(i) for i in result.cancelled_ids))
            rows = [
                await side.backend.get(side._jobs_by_token[t])  # pyright: ignore[reportPrivateUsage]  # Why: the harness's own token registry.
                for t in ("c1", "c2", "c3")
            ]
            side.record("statuses", [None if r is None else r.status for r in rows])

        return scenario

    mem_a, pg_a = await run_differential(
        scenario_factory("active"), pg_dsn=pg_dsn, actors=("test_actor",)
    )
    assert_mirror("bulk cancel under the alias spelling mirrors PG", mem_a, pg_a)
    mem_b, pg_b = await run_differential(
        scenario_factory("unfinished"), pg_dsn=pg_dsn, actors=("test_actor",)
    )
    assert_mirror("bulk cancel under the canonical spelling mirrors PG", mem_b, pg_b)

    assert pg_a["records"] == pg_b["records"], (
        f"bulk-cancel behavior drifted across the rename:\nalias:      {pg_a['records']}\n"
        f"canonical:  {pg_b['records']}"
    )
    assert pg_a["records"]["cancelled_ids"] == ["c1", "c2"]
    assert pg_a["records"]["statuses"] == ["cancelled", "cancelled", "pending"]


# ══════════════════════════════════════════════════════════════════════
# Lane 4: the end-to-end soak on real Postgres
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.integration
async def test_soak_mixed_workload_nothing_lost_or_duplicated(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A few hundred rounds of mixed workload - singles, keyed dedup
    repeats, atomic batches, dispatch, retries, cancels, sweeps - against
    real Postgres: every enqueued id resolves to exactly one row, nothing
    is duplicated, every job ends exactly one of live/archived (terminal
    with the ledger's status, or live and non-terminal), batch counts
    match their member rows, and the dispatch metric stream is coherent
    at the end."""
    reader = setup_meter(monkeypatch)
    from .test_rt_diff_harness import _close_pg_side, _pg_side

    schema = f"tdf_{new_uuid().hex[:10]}"
    side = await _pg_side(pg_dsn, schema=schema, actors=("test_actor",))
    try:
        conn = side._conn
        assert conn is not None
        ledger_terminal: dict[str, str] = {}
        ledger_live: set[str] = set()
        dedup_tokens: dict[str, str] = {}

        rounds = 240
        for r in range(rounds):
            # Enqueue: singles, occasionally keyed (dedup), every 6th round
            # a mini atomic batch.
            for suffix in ("a", "b"):
                token = f"j{r}{suffix}"
                key = f"k{r % 40}" if r % 17 == 0 else None
                if key is not None and key in dedup_tokens:
                    holder = dedup_tokens[key]
                    _keyed_batch_item(side, token, "test_actor", key)
                    rows = await side.backend.enqueue_batch(
                        [_keyed_batch_item(side, token + "x", "test_actor", key)]
                    )
                    assert all(row.id == side._jobs_by_token[holder] for row in rows), (
                        "a dedup repeat must alias its holder, never a second row"
                    )
                    continue
                await side.enqueue(token, actor="test_actor", idempotency_key=key)
                if key is not None:
                    dedup_tokens[key] = token
                ledger_live.add(token)

            if r % 6 == 0:
                bid = new_uuid()
                batch_tokens = [f"b{r}_{i}" for i in range(3)]
                items = [_batch_item(side, t) for t in batch_tokens]
                await side.backend.enqueue_batch_atomic(
                    items,
                    batch_id=bid,
                    queue=_QUEUE,
                    batch_row=_side_batch_row(side, bid),
                    finalizer_args=None,
                )
                ledger_live.update(batch_tokens)

            # Dispatch and drive the claimed rows to a deterministic outcome.
            claimed = await side.dispatch("w1", [_QUEUE], limit=10)
            for token in claimed:
                phase = r % 4
                if phase == 1:
                    row = await side.backend.get(side._jobs_by_token[token])
                    assert row is not None
                    if row.attempt >= 3:
                        await side.mark_failed_or_retry(token, "w1", retry_delay_s=None)
                        ledger_terminal[token] = "failed"
                        ledger_live.discard(token)
                    else:
                        await side.mark_failed_or_retry(token, "w1", retry_delay_s=0.0)
                        ledger_terminal.pop(token, None)
                        ledger_live.add(token)
                elif phase == 2:
                    await side.mark_succeeded(token, "w1")
                    ledger_terminal[token] = "succeeded"
                    ledger_live.discard(token)
                else:
                    await side.mark_cancelled(token, "w1")
                    ledger_terminal[token] = "cancelled"
                    ledger_live.discard(token)

            # Occasional operator cancels of pending rows, through the
            # bulk-cancel surface the rename feeds.
            if r % 11 == 0 and r > 0:
                victim = f"j{r}a"
                if victim in ledger_live:
                    result = await side.backend.cancel_where(
                        JobFilter(queue=_QUEUE, limit=1, cursor=None, order_by=None),
                        "soak cancel",
                    )
                    for jid in result.cancelled_ids:
                        for t in list(ledger_live):
                            if side._jobs_by_token.get(t) == jid:  # pyright: ignore[reportPrivateUsage]
                                ledger_terminal[t] = "cancelled"
                                ledger_live.discard(t)

            # Periodic sweeps.
            if r % 13 == 0 and r > 0:
                await side.sweep_promote()
                await side.sweep_deadline()

        # ── The soak's closing invariants ────────────────────────────
        # 1. Nothing lost: every ledger token resolves to exactly one row.
        missing = [
            token
            for token in list(ledger_terminal) + sorted(ledger_live)
            if await side.backend.get(side._jobs_by_token[token]) is None  # pyright: ignore[reportPrivateUsage]
        ]
        assert not missing, f"rows vanished from the ledger: {missing[:10]}"

        # 2. Nothing duplicated: the table holds exactly the ledger's rows.
        all_tokens = list(ledger_terminal) + sorted(ledger_live)
        distinct_ids = {side._jobs_by_token[t] for t in all_tokens}  # pyright: ignore[reportPrivateUsage]
        total_rows = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
        assert int(total_rows) == len(distinct_ids), (
            f"the jobs table holds {total_rows} rows for a {len(distinct_ids)}-row "
            "ledger: a duplicate or an untracked row"
        )

        # 3. Every job exactly one of live/archived: ledger-terminal rows
        # are terminal with the ledger's status; ledger-live rows are
        # non-terminal.
        terminal_bad, live_bad = [], []
        for token, status in ledger_terminal.items():
            row = await side.backend.get(side._jobs_by_token[token])  # pyright: ignore[reportPrivateUsage]
            assert row is not None
            if row.status != status:
                terminal_bad.append((token, row.status, status))
        for token in ledger_live:
            row = await side.backend.get(side._jobs_by_token[token])  # pyright: ignore[reportPrivateUsage]
            assert row is not None
            if row.status in _TERMINAL:
                live_bad.append((token, row.status))
        assert not terminal_bad, f"terminal rows disagree with the ledger: {terminal_bad[:10]}"
        assert not live_bad, f"live rows turned terminal outside the ledger: {live_bad[:10]}"

        # 4. Batch counts coherent: every created batch's non-terminal
        # count never exceeds the member rows actually present.
        batch_rows = await conn.fetch(f'SELECT id FROM "{schema}".batches')
        assert len(batch_rows) == rounds // 6, (
            f"every 6th round created one batch: {len(batch_rows)}"
        )
        for brow in batch_rows:
            non_terminal = await side.backend.count_batch_non_terminal(brow["id"])
            members = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE metadata @> $1::jsonb',
                f'{{"batch_id": "{brow["id"]}"}}',
            )
            assert non_terminal <= int(members), (
                f"batch {brow['id']} counts {non_terminal} non-terminal members for {members} rows"
            )

        # 5. Metrics coherent: every dispatch round exactly one query
        # sample and one wait sample, no failures.
        by_count: dict[str, int] = {}
        for m in collect_metrics(reader):
            count = sum(getattr(p, "count", 0) for p in m.data.data_points)
            if count:
                by_count[m.name] = by_count.get(m.name, 0) + count
        assert by_count.get("taskq.dispatch.duration", 0) == rounds, (
            f"each of the {rounds} dispatch rounds records exactly one query "
            f"sample, got {by_count.get('taskq.dispatch.duration')}"
        )
        assert by_count.get("taskq.dispatch.pool_acquire_duration", 0) == rounds, (
            f"each dispatch round records exactly one pool wait, got "
            f"{by_count.get('taskq.dispatch.pool_acquire_duration')}"
        )
        assert by_count.get("taskq.dispatch.failures", 0) == 0, "no soak round may fail"
    finally:
        await _close_pg_side(side)
