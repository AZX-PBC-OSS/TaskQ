"""Audit pins: the clock-domain and timeout-ordering contracts.

Two bug classes were audited across every ``time.monotonic()`` /
``perf_counter()`` / ``loop.time()`` site and every (Python bounded wait,
PG timeout) pair in ``src/``:

1. A Python monotonic VALUE (meaningless outside its process and after a
   suspend) crossing a boundary: persisted into a column/jsonb, emitted
   into a log field used for alerting, a payload, an event detail. The
   sweep-stall fix (the monotonic ledger beside the wall gauge) is the
   pattern; these pins freeze the boundary contracts that keep every
   other ledger process-local.

2. The ordering between a Python-side bounded wait and the PG-side
   timeout it waits on. The conservative contract on EVERY path: the
   Python bound is strictly greater than the PG bound (so the server's
   typed refusal -- and its counters -- land first), or the PG-timeout-
   fired outcome (``QueryCanceledError`` from a cancelled query, the
   client ``TimeoutError``) is handled as a first-class, infra-retryable
   result. A path where the Python budget is shorter than the PG timeout
   it wraps makes the PG bound the one that cannot fire.

The pins are behavioural wherever a fake at the driver boundary can drive
the real production code (the advisory acquire, the drain monitor), and
constant-relation pins where the ordering lives in shipped defaults (the
event-writer ``statement_timeout`` inside its margins).

If a fix moves a seam, update the driver -- the assertions are the
contract, not the spelling.
"""

import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import cast

import asyncpg
import pytest
import structlog
import structlog.testing

from taskq._advisory import (
    DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
    acquire_advisory_xact_lock_bounded,
)
from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.connections import (
    _LOCK_BUDGET_COMMAND_TIMEOUT_SHARE,  # pyright: ignore[reportPrivateUsage]  # Why: the ordering contract IS this share; the pin asserts the clamped budget sits strictly under the pool's per-query bound because of it.
    bounded_lock_budget_ms,
    lock_budget_command_timeout_secs,
)
from taskq.constants import (
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    RECLAIM_EVENT_VISIBILITY_DELAY,
)
from taskq.settings import WorkerSettings
from taskq.worker._leader_sweeps import (
    _is_deadline_family,  # pyright: ignore[reportPrivateUsage]  # Why: the deadline-family predicate is the first-class outcome classifier the ordering contract leans on; the pin freezes its exact-type discipline.
)
from taskq.worker.drain import drain_monitor_loop
from taskq.worker.shutdown import ShutdownPhase

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

_LOCK_KEY = "pin:some-lock"


# ── Part 1: the monotonic value never crosses the emit boundary ────────


def test_sweep_success_wall_ledger_stamps_absolute_unix_time() -> None:
    """The exported ``sweep_last_success_seconds`` gauge's contract is an
    ABSOLUTE Unix timestamp (``time() - value`` is the operator's PromQL
    staleness subtraction). The ledger feeding it must therefore be
    stamped in the wall domain: a monotonic uptime value (tiny, meaning-
    less across processes, reset at boot) exported under that name sends
    every consumer's ``time() - value`` to hours-of-staleness fiction and
    fires the promotion-stalled alert on every healthy worker. The
    in-process staleness reader is a separate concern (the monotonic
    ledger the sweep-stall fix adds beside this one); this pin holds only
    the wall ledger's exported contract."""
    import taskq.obs._otel as otel_mod

    try:
        before = time.time()
        from taskq.obs._otel import record_sweep_success

        record_sweep_success("pin_sweep")
        after = time.time()
        stamp = otel_mod._sweep_success_cache["pin_sweep"]
        assert before - 1.0 <= stamp <= after + 1.0, (
            "the sweep-success ledger must carry a wall-domain Unix "
            f"timestamp (got {stamp!r}, wall window [{before}, {after}]): "
            "a monotonic uptime value here is the persisted-monotonic "
            "leak this audit hunts"
        )
    finally:
        otel_mod._sweep_success_cache = {}  # pyright: ignore[reportPrivateUsage]  # Why: the pin stamped the process-singleton ledger; leaving the stamp behind would leak into the gauge tests' sample populations.


def test_cancel_phase_change_line_carries_no_clock_value() -> None:
    """The cancel ladder's canonical ``cancel_phase_change`` record is the
    line alerts and dashboards key on. Its schema must stay clock-free:
    the ladder's stamps (``_ActiveJob.cancel_observed_at``, a
    ``loop.time()`` monotonic value) are process-local by contract, and
    the one place they could leak is this record's kwargs. A monotonic
    float in the schema would be the emitted-monotonic leak: meaningless
    across workers and restarts, yet stable enough to be compared against
    a wall timestamp by a downstream rule."""
    from taskq.obs import log_cancel_phase_change

    with structlog.testing.capture_logs() as logs:
        log_cancel_phase_change(
            structlog.get_logger("pin"),
            from_phase=0,
            to_phase=1,
            job_id="00000000-0000-0000-0000-000000000001",
            worker_id="00000000-0000-0000-0000-000000000002",
        )
    assert len(logs) == 1
    record = logs[0]
    assert record["event"] == "cancel_phase_change"
    assert record["kind"] == "cancel_phase_change"
    clock_free = {
        key
        for key in record
        if key
        not in {
            "event",
            "kind",
            "from_phase",
            "to_phase",
            "job_id",
            "worker_id",
            "level",
            "log_level",
        }
    }
    assert clock_free == set(), (
        f"the canonical cancel_phase_change record grew non-canonical keys "
        f"{clock_free!r}: the ladder's loop.time() stamps (cancel_observed_at "
        "and friends) are process-local monotonic values and must never be "
        "emitted into the alertable record"
    )


# ── Part 2: the (Python bound, PG bound) ordering contracts ────────────


class _HeldLockConn:
    """ConnLike stand-in whose contended-tier blocking acquire never
    grants: the try-lock fast path refuses, the GUC read answers "0", and
    the ``pg_advisory_xact_lock`` statement hangs until the CLIENT-side
    backstop cancels it -- the network-black-hole shape the server-side
    ``lock_timeout`` cannot see."""

    async def fetchval(self, sql: str, *args: object) -> object:
        del args
        if "pg_try_advisory_xact_lock" in sql:
            return False  # contended: the fast path refuses
        if "current_setting" in sql:
            return "0"  # no prior lock_timeout to restore
        return None

    async def execute(self, sql: str, *args: object) -> str:
        del args
        if "pg_try_advisory_xact_lock" not in sql and "pg_advisory_xact_lock" in sql:
            await asyncio.sleep(3600.0)  # the black-holed holder
        return "OK"

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield


async def test_advisory_backstop_fires_strictly_behind_the_server_budget() -> None:
    """The bounded advisory acquire's client-side backstop must be the
    server budget PLUS slack, strictly: the server's ``lock_timeout`` is
    the bound that should fire on a healthy network (it produces the
    typed refusal path), and the backstop exists only for the black hole
    the server cannot see. A backstop at or under the budget converts
    every legitimate near-budget grant into a client ``TimeoutError``
    before the server's refusal (and its counter) can land -- the
    "bound that cannot fire" inversion, on the client side."""
    conn = _HeldLockConn()
    budget_ms = 300.0
    started = time.monotonic()
    acquired = await acquire_advisory_xact_lock_bounded(
        conn,  # type: ignore[arg-type]  # Why: ConnLike is the structural alias this fake satisfies; the strict runtime check lives in the production call sites.
        _LOCK_KEY,
        timeout_ms=budget_ms,
    )
    elapsed = time.monotonic() - started
    assert acquired is False, (
        "a never-granting holder must exhaust to False, the caller's typed-refusal anchor"
    )
    assert elapsed >= budget_ms / 1000.0 + DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S * 0.9, (
        f"the backstop fired at {elapsed:.3f}s, before the server budget "
        f"({budget_ms}ms) plus its slack could elapse: the client bound is "
        "no longer strictly behind the server bound, so the server's "
        "lock_timeout is the bound that cannot fire"
    )


async def test_advisory_server_refusal_is_a_first_class_outcome() -> None:
    """When the SERVER's ``lock_timeout`` fires (SQLSTATE 55P03), the
    acquire must return False -- the caller's typed-exhaustion anchor --
    not raise. The PG-timeout-fired outcome being first-class is the
    other half of the ordering contract: whichever bound wins, the
    caller sees one settled shape."""
    conn = _HeldLockConn()

    async def _server_refuses(sql: str, *args: object) -> str:
        del args
        if "pg_try_advisory_xact_lock" not in sql and "pg_advisory_xact_lock" in sql:
            raise asyncpg.LockNotAvailableError("55P03: pin")
        return "OK"

    conn.execute = _server_refuses  # type: ignore[method-assign]  # Why: the pin swaps only the blocking arm for the server-refusal shape.
    acquired = await acquire_advisory_xact_lock_bounded(
        conn,  # type: ignore[arg-type]  # Why: same fake-conn rationale as the backstop pin.
        _LOCK_KEY,
        timeout_ms=300.0,
    )
    assert acquired is False


def test_enqueue_lock_budget_clamp_keeps_the_server_bound_first() -> None:
    """An enqueue lock budget at or above its connection's per-query
    ``command_timeout`` can never deliver the typed refusal: asyncpg
    starts its client-side timer before the ``SET LOCAL lock_timeout``
    executes, so the client ``TimeoutError`` always wins and the typed
    refusal (``MaxPendingLockTimeoutError`` and friends, with their
    backpressure counters) is unreachable code. The clamp must hold every
    budget strictly under the pool's bound, at the share that leaves the
    server's refusal room to be raised, unwound and written back."""
    command_timeout_secs = 5.0
    huge_budget_ms = 60_000.0
    clamped = bounded_lock_budget_ms(huge_budget_ms, command_timeout_secs)
    assert clamped <= command_timeout_secs * 1000.0 * _LOCK_BUDGET_COMMAND_TIMEOUT_SHARE
    assert clamped < command_timeout_secs * 1000.0, (
        "the clamped budget must sit STRICTLY under the connection's "
        "per-query bound: at or above it, the client timer fires first "
        "and the server-side lock_timeout is the bound that cannot fire"
    )


@pytest.mark.parametrize(
    ("budget_ms", "command_timeout_secs"),
    [
        pytest.param(1000.0, 5.0, id="budget-under-share-passes-through"),
        pytest.param(5000.0, None, id="unbounded-connection-keeps-budget"),
        pytest.param(5000.0, 0.0, id="zero-command-timeout-keeps-budget"),
        pytest.param(0.0, 5.0, id="zero-budget-is-the-indefinite-convention"),
        pytest.param(-1.0, 5.0, id="negative-budget-is-the-indefinite-convention"),
    ],
)
def test_enqueue_lock_budget_clamp_identity_cases(
    budget_ms: float, command_timeout_secs: float | None
) -> None:
    """The clamp only exists to keep a budget under its connection's
    bound; every case where the budget already fits (or the operator
    asked for the unbounded convention) must pass through unchanged --
    a clamp that bit those cases would silently shorten every shipped
    default wait."""
    assert bounded_lock_budget_ms(budget_ms, command_timeout_secs) == budget_ms


def test_widened_budget_rederives_the_pool_bound_above_it() -> None:
    """An operator budget widened past its shipped default must be
    honored END TO END: the TaskQ-built pool's per-query bound is
    re-derived (budget / share) so the budget still fits under it and
    the server-side ``lock_timeout`` still fires before the client-side
    timer. A derivation that returned a bound at or under the budget
    re-creates the truncation the knob exists to remove."""
    default_ms = 5000.0
    widened_ms = 30_000.0
    bound = lock_budget_command_timeout_secs(
        [(widened_ms, default_ms)],
        floor_secs=5.0,
    )
    assert bound * 1000.0 * _LOCK_BUDGET_COMMAND_TIMEOUT_SHARE >= widened_ms, (
        "the re-derived pool bound must give the widened budget its configured share of headroom"
    )
    assert bound > widened_ms / 1000.0, (
        "the pool's per-query bound must be STRICTLY greater than the "
        "lock budget it wraps: equality is the race with no deterministic "
        "winner, and the client timer must never be the bound that fires"
    )
    # A budget at or under its default never moves the bound off the floor.
    assert lock_budget_command_timeout_secs([(default_ms, default_ms)], floor_secs=5.0) == 5.0, (
        "the shipped-default fleet must keep the pre-knob pool bound exactly"
    )
    # An unbounded (non-positive) budget cannot fit inside any finite
    # bound and must not lower the floor either.
    assert lock_budget_command_timeout_secs([(0.0, default_ms)], floor_secs=5.0) == 5.0, (
        "an unbounded lock wait must not strip the pool's black-hole guard"
    )


def test_deadline_family_is_the_exact_type_discipline() -> None:
    """The deadline family -- the two shapes a bounded statement's
    deadline can arrive as -- is ``TimeoutError`` EXACTLY (an
    ``asyncio.timeout``/command-timeout firing or a cancelled await) or
    the server's ``QueryCanceledError``. The exact-type check exists so
    the classifier never mislabels a raw ``OSError`` (socket death,
    reachable through ``TimeoutError``'s class hierarchy in neither
    direction) as a deadline: the first-class-outcome contract depends
    on this precision."""
    assert _is_deadline_family(TimeoutError()) is True
    assert _is_deadline_family(asyncpg.QueryCanceledError("pin")) is True
    assert _is_deadline_family(OSError("socket died")) is False, (
        "a socket death is a different failure family with different "
        "remediation; the exact-type discipline keeps it out of the "
        "deadline family"
    )
    assert _is_deadline_family(asyncpg.InterfaceError("pool closed")) is False


class _DeadlineFamilyBackend:
    """count_active_jobs that fails with each PG-deadline shape in turn,
    then recovers to a busy count: the degraded-PG sequence the drain
    monitor must ride out."""

    def __init__(self, failures: list[BaseException]) -> None:
        self._failures = list(failures)
        self.calls = 0

    async def count_active_jobs(self, queues: object) -> int:
        del queues
        self.calls += 1
        if self._failures:
            raise self._failures.pop(0)
        return 1  # busy: the monitor keeps polling, never triggers


def _drain_deps(backend: object) -> SimpleNamespace:
    return SimpleNamespace(
        liveness=SimpleNamespace(tick=lambda *a, **k: None),
        active_jobs=SimpleNamespace(count=lambda: 0),
        drain_failures=0,
        shutdown_phase=ShutdownPhase.NONE,
    )


async def test_drain_monitor_survives_both_pg_deadline_shapes() -> None:
    """The drain monitor's outer ``wait_for`` bound and the pool's inner
    per-query bound are independent knobs, so EITHER deadline shape can
    fire first: the server cancelling at its own ``statement_timeout``
    (``QueryCanceledError``) or the client timer (``TimeoutError``). The
    ordering contract for this path is the second permitted form: the
    PG-timeout-fired outcome is handled as infra-retryable, the monitor
    counts the round unknown and keeps polling. A tuple that drops one
    half lets a degraded-but-recovering PG tear the monitor (and its
    TaskGroup sibling) down mid-drain."""
    backend = _DeadlineFamilyBackend(
        [
            asyncpg.QueryCanceledError("server statement_timeout fired"),
            TimeoutError("client command timeout fired"),
        ]
    )
    shutdown_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []
    monitor = asyncio.create_task(
        drain_monitor_loop(
            deps=_drain_deps(backend),  # type: ignore[arg-type]  # Why: the SimpleNamespace stand-in satisfies the names drain_monitor_loop reads; WorkerDeps' full shape is irrelevant to this path.
            settings=WorkerSettings.load_from_dict(  # type: ignore[arg-type]  # Why: the monitor reads .queues off it; the settings object's full contract is not this path's.
                {"TASKQ_PG_DSN": _DSN, "TASKQ_QUEUES": "default"}
            ),
            worker_id=new_uuid(),
            shutdown_event=shutdown_event,
            escalate_event=asyncio.Event(),
            orchestrator_holder=orchestrator_holder,
            backend=cast(Backend, backend),
            idle_settle_window=3600.0,
            idle_poll_interval=0.05,
            max_runtime=None,
        )
    )
    try:
        # Both deadline shapes fired, then the backend recovered busy.
        for _ in range(60):
            if backend.calls >= 3:
                break
            await asyncio.sleep(0.05)
        assert backend.calls >= 3, (
            "the monitor stopped polling after the deadline shapes: the "
            f"PG-timeout-fired outcome was not handled as infra-retryable "
            f"(calls={backend.calls}, done={monitor.done()})"
        )
        assert not monitor.done() or monitor.exception() is None, (
            f"the monitor task died on a deadline-family error: {monitor.exception()!r}"
        )
        assert orchestrator_holder == [], "a degraded count must never trigger the drain shutdown"
    finally:
        shutdown_event.set()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError, Exception):
            await asyncio.wait_for(monitor, timeout=5.0)


def test_event_writer_statement_timeout_sits_inside_its_margins() -> None:
    """The event-writer batch's server-side ``statement_timeout`` is the
    ENFORCED half of the reclaim-margin invariant: 7/8 of the 2 s
    ``RECLAIM_EVENT_VISIBILITY_DELAY`` (the margin an open batch
    transaction may hold between INSERT and COMMIT), and strictly under
    every pool bound that wraps it (the dispatcher pool's default 5 s
    ``command_timeout``), so the SERVER bound is the one that fires and
    the abort surfaces as ``QueryCanceledError`` -- the deadline family
    the sweep breaker and ``sweep_timeouts`` counter are built on.
    Raising the constant past either margin inverts the ordering: the
    batch would hold its transaction past the reclaim watermark, or the
    client timer would fire first and the typed counter plane goes
    silent."""
    margin_ms = RECLAIM_EVENT_VISIBILITY_DELAY.total_seconds() * 1000.0
    assert margin_ms * 7 / 8 == DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS, (
        "the event-writer statement_timeout must stay at 7/8 of the "
        "reclaim visibility margin: it is the enforcement of that margin"
    )
    settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})
    assert (
        settings.dispatcher_command_timeout * 1000.0 > DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS
    ), (  # Why: the assertion message is the contract; the line break would hide the comparison.
        "the server-side statement_timeout must sit strictly under the "
        "pool's per-query command_timeout, or the client timer fires "
        "first and the server-cancelled outcome (sweep_timeouts, the "
        "breaker) is the bound that cannot fire"
    )
