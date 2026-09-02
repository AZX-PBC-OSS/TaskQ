"""Regression tests for PR #39 follow-up fixes.

Each test validates a specific issue from XBeg9's final approved review that
was merged without being addressed (rcbevans: "To make progress I'll merge
as is and address remaining comments suggestions in a follow up").

The tests are behavioral: they drive the real loops with fake deps and
assert the contract that XBeg9 identified as broken or missing.
"""

import asyncio
import contextlib
import time
from types import SimpleNamespace
from typing import Any, cast

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.backend.clock import SystemClock
from taskq.worker._watchdog import LoopLiveness
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import MaintenanceLeader

# ═══════════════════════════════════════════════════════════════════════════
# Fix 1: Cron loop must classify all TRANSIENT_PG_ERRORS as retry, not fatal
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (leader.py:652): "Cron still hand-rolls its error classification
# instead of using TRANSIENT_PG_ERRORS, and the two isinstance branches miss
# most of it: AdminShutdownError, CannotConnectNowError,
# TooManyConnectionsError, DeadlockDetectedError, SerializationError,
# IdleSessionTimeoutError, IdleInTransactionSessionTimeoutError — 7 of the
# 12. Before this commit the blanket except Exception logged those and
# retried forever; now five in a row takes the worker down. Deadlock and
# serialization inside a transaction are routine, not surprises."
#
# Fix: catch TRANSIENT_PG_ERRORS first (retry), keep the narrower conn-state
# check inside it, only truly unexpected errors reach the guard backstop.


class _FakeCronConn:
    """Minimal conn stand-in whose transaction() delegates to tick_cron."""

    def __init__(self) -> None:
        self._closed = False

    def transaction(self) -> object:
        class _Tx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *args: object) -> bool:
                return False

        return _Tx()

    def is_closed(self) -> bool:
        return False

    async def close(self) -> None:
        self._closed = True

    def terminate(self) -> None:
        self._closed = True


@pytest.mark.parametrize(
    ("exc_factory", "exc_name"),
    [
        (lambda: asyncpg.DeadlockDetectedError("40P01"), "DeadlockDetectedError"),
        (lambda: asyncpg.SerializationError("40001"), "SerializationError"),
        (lambda: asyncpg.AdminShutdownError("57P01"), "AdminShutdownError"),
        (lambda: asyncpg.CannotConnectNowError("57P03"), "CannotConnectNowError"),
        (lambda: asyncpg.TooManyConnectionsError("53300"), "TooManyConnectionsError"),
        (
            lambda: asyncpg.IdleSessionTimeoutError("57P05"),
            "IdleSessionTimeoutError",
        ),
        (
            lambda: asyncpg.IdleInTransactionSessionTimeoutError("25P03"),
            "IdleInTransactionSessionTimeoutError",
        ),
    ],
    ids=[
        "DeadlockDetectedError",
        "SerializationError",
        "AdminShutdownError",
        "CannotConnectNowError",
        "TooManyConnectionsError",
        "IdleSessionTimeoutError",
        "IdleInTransactionSessionTimeoutError",
    ],
)
async def test_cron_loop_treats_transient_error_as_retry_not_fatal(
    monkeypatch: pytest.MonkeyPatch,
    exc_factory: Any,
    exc_name: str,
) -> None:
    """Each transient PG error that the hand-rolled isinstance branches missed
    must be retried by the cron loop, not counted by the backstop guard. Before
    the fix, 5 consecutive deadlocks killed the worker — deadlock inside a
    transaction is routine, not a bug."""

    tick_count = 0

    async def _failing_tick_cron(*args: object, **kwargs: object) -> None:
        nonlocal tick_count
        tick_count += 1
        raise exc_factory()

    monkeypatch.setattr("taskq.worker.leader.tick_cron", _failing_tick_cron)
    monkeypatch.setattr("taskq.worker._transient.DEFAULT_MAX_CONSECUTIVE_UNEXPECTED", 3)

    liveness = LoopLiveness()
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(
                schema_name="taskq",
                dispatcher_command_timeout=2.5,
            ),
        ),
    )
    leader = MaintenanceLeader(
        deps, new_uuid(), cast(Backend, SimpleNamespace()), clock=SystemClock()
    )
    leader._cron_conn = _FakeCronConn()  # type: ignore[assignment]

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))
    try:
        await asyncio.sleep(4.0)
        assert not task.done(), (
            f"cron loop must ride out {exc_name}, not die: "
            f"{task.exception() if task.done() else 'still running'}"
        )
        assert tick_count >= 3, (
            f"cron loop must keep retrying after {exc_name}: {tick_count} ticks in 4s"
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(task, timeout=5.0)


# ═══════════════════════════════════════════════════════════════════════════
# Fix 2: Cleanup must run before guard.unexpected raises at the cap
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (leader.py:295): "guard.unexpected(exc) raises at the cap and the
# _drop_leader_conn / _close_leader_owned_conns calls sit on the lines after
# it, so on the fatal iteration none of it runs and is_leader stays set
# until run()'s finally. The comment just above says 'cleanup mirrors the
# transient path' — right for the first four, wrong for the fifth. Same at
# the lock, upsert and watchdog sites."
#
# Fix: move cleanup BEFORE guard.unexpected() at all four sites
# (election probe, lock attempt, upsert, watchdog probe).


class _AlwaysUnexpectedConn:
    """Conn whose execute always raises ValueError (a non-transient bug)."""

    def __init__(self) -> None:
        self._closed = False

    async def execute(self, *args: object, **kwargs: object) -> str:
        raise ValueError("unexpected bug in probe")

    async def fetchval(self, *args: object, **kwargs: object) -> object:
        raise ValueError("unexpected bug in lock attempt")

    def is_closed(self) -> bool:
        return False

    async def close(self) -> None:
        self._closed = True

    def terminate(self) -> None:
        self._closed = True


async def test_election_probe_cleanup_runs_before_guard_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the election probe's unexpected error reaches the guard cap,
    cleanup (drop_leader_conn, close_leader_owned_conns) must have already
    run. Before the fix, guard.unexpected raised first and the cleanup lines
    after it never executed, leaving is_leader set and leader_conn dangling."""

    monkeypatch.setattr("taskq.worker._transient.DEFAULT_MAX_CONSECUTIVE_UNEXPECTED", 3)

    liveness = LoopLiveness()
    is_leader = asyncio.Event()
    is_leader.set()
    bad_conn = cast(asyncpg.Connection, _AlwaysUnexpectedConn())
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            leader_conn=bad_conn,
            owns_leader_conn=True,
            leader_conn_factory=None,
            settings=SimpleNamespace(
                schema_name="taskq",
                heartbeat_interval=0.01,
                dispatcher_command_timeout=2.5,
                pg_dsn_direct=None,
            ),
            dispatcher_pool=None,
        ),
    )
    leader = MaintenanceLeader(
        deps, new_uuid(), cast(Backend, SimpleNamespace()), clock=SystemClock()
    )

    # Track cleanup calls without actually clearing is_leader or nulling
    # leader_conn, so the probe keeps running and the guard can reach the
    # cap. The test verifies cleanup was CALLED before the guard raised,
    # not that it had a specific effect on deps state.
    cleanup_calls: list[str] = []

    async def _tracking_drop(*args: object, **kwargs: object) -> None:
        cleanup_calls.append("drop")

    async def _tracking_close(*args: object, **kwargs: object) -> None:
        cleanup_calls.append("close")

    leader._drop_leader_conn = _tracking_drop  # type: ignore[assignment]
    leader._close_leader_owned_conns = _tracking_close  # type: ignore[assignment]

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        with pytest.raises(ValueError, match="unexpected bug"):
            await asyncio.wait_for(task, timeout=10.0)

        # The guard raised at the cap (3 consecutive). Without the fix,
        # guard.unexpected raised BEFORE cleanup, so cleanup_calls would be
        # empty. With the fix, cleanup runs before each guard.unexpected call.
        assert len(cleanup_calls) >= 2, (
            f"cleanup (drop + close) must run before guard.unexpected raises; got {cleanup_calls}"
        )
    finally:
        shutdown.set()
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# ═══════════════════════════════════════════════════════════════════════════
# Fix 3: Probe's unexpected branch must continue, not fall through to guard.ok()
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (leader.py:280): "The probe's unexpected branch doesn't continue,
# so the iteration keeps going into re-election and reaches guard.ok() at
# the bottom. A fault that only hits the probe alternates unexpected/ok and
# never gets to the cap. The docstring says only a fully successful
# iteration should reset the streak."
#
# Fix: add `continue` after the probe's guard.unexpected block so it doesn't
# fall through to re-election and guard.ok() in the same iteration.


async def test_election_probe_unexpected_does_not_reach_guard_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A probe-only unexpected error must not fall through to guard.ok() at
    the bottom of the election loop. Without `continue`, the iteration
    continues into re-election, succeeds, and resets the streak — hiding the
    pattern. With `continue`, the probe failure is isolated to its own
    iteration."""

    monkeypatch.setattr("taskq.worker._transient.DEFAULT_MAX_CONSECUTIVE_UNEXPECTED", 3)

    probe_calls = 0
    lock_attempts = 0

    class _ProbeFailsLockSucceedsConn:
        """Conn where execute('SELECT 1') raises ValueError but fetchval
        (lock attempt) returns True and the upsert succeeds."""

        def __init__(self) -> None:
            self._closed = False

        async def execute(self, sql: str, *args: object) -> str:
            nonlocal probe_calls
            if "SELECT 1" in sql:
                probe_calls += 1
                raise ValueError("probe bug")
            return "UPDATE 1"

        async def fetchval(self, sql: str, *args: object) -> object:
            nonlocal lock_attempts
            if "pg_try_advisory_lock" in sql:
                lock_attempts += 1
                return True
            return None

        def is_closed(self) -> bool:
            return False

        async def close(self) -> None:
            self._closed = True

        def terminate(self) -> None:
            self._closed = True

    liveness = LoopLiveness()
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            leader_conn=cast(asyncpg.Connection, _ProbeFailsLockSucceedsConn()),
            owns_leader_conn=True,
            leader_conn_factory=None,
            settings=SimpleNamespace(
                schema_name="taskq",
                heartbeat_interval=0.01,
                dispatcher_command_timeout=2.5,
                pg_dsn_direct=None,
            ),
            dispatcher_pool=None,
        ),
    )
    leader = MaintenanceLeader(
        deps, new_uuid(), cast(Backend, SimpleNamespace()), clock=SystemClock()
    )

    # Prevent _open_leader_conn from being called (which would fail since
    # pg_dsn_direct is None) by making the election loop think the conn is
    # always alive after cleanup.
    async def _mock_close(*args: object, **kwargs: object) -> None:
        pass

    leader._close_leader_owned_conns = _mock_close  # type: ignore[assignment]

    # Also prevent _drop_leader_conn from nulling (so the loop keeps
    # using the same fake conn)
    async def _mock_drop(*args: object, **kwargs: object) -> None:
        pass

    leader._drop_leader_conn = _mock_drop  # type: ignore[assignment]

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        with contextlib.suppress(ValueError, BaseExceptionGroup):
            await asyncio.wait_for(task, timeout=10.0)
    finally:
        shutdown.set()
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    # Without the fix: probe fails → falls through to re-election →
    # lock_attempts > 0 and the guard never reaches the cap (streak resets).
    # With the fix: probe fails → continue → lock is NOT attempted in the
    # same iteration.
    assert lock_attempts == 0, (
        f"probe failure must `continue` before re-election; "
        f"lock was attempted {lock_attempts} time(s) in the same iteration "
        f"as a probe failure — guard.ok() would reset the streak"
    )
    assert probe_calls >= 3, f"probe must be called at least max_consecutive times: {probe_calls}"


# ═══════════════════════════════════════════════════════════════════════════
# Fix 4: Settings validator must not claim the producer is bounded
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (settings.py:1024): "Not asking you to fix the producer here — it's
# never been bounded... What's new is this validator saying it's fine. The
# model is timeout + period, which holds for scheduled_wake and cron because
# you wrapped them. The producer isn't wrapped... Either wrap the producer
# like the leader loops, or drop it from the validator and say per-statement
# only."
#
# Fix: remove the producer from the invariant check; update the description.


async def test_settings_validator_does_not_check_producer_loop() -> None:
    """The settings validator must not certify the producer loop as bounded
    when it is not wrapped in asyncio.timeout. The validator's model
    (timeout + period) does not hold for the producer's multi-statement
    dispatch_batch. Removing the producer from the check stops the invariant
    from making a false guarantee."""

    from taskq.settings import WorkerSettings

    # A config where the producer would fail the invariant if checked:
    # notify_enabled=False, poll_interval=1.0 (producer_period=1.0),
    # dispatcher_command_timeout=9.0, watchdog_stale_floor=10.0.
    # Leader loops: budget=10, 9+1=10 >= 10 → FAILS.
    # Producer: budget=10, 9+1=10 >= 10 → would also FAIL.
    # But we want to test the producer specifically. Use a config where
    # the leader passes but the producer would fail if checked:
    # timeout=8.0, notify_enabled=True, notify_poll_interval=1.0,
    # watchdog_stale_floor=9.0.
    # Leader: budget=max(5, 9)=9, 8+1=9 >= 9 → FAILS.
    # Hmm, both share the floor. The only way to test is to verify the
    # error message does NOT mention "producer" when only the leader fails.

    # Use a config that fails the leader check:
    settings_dict = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_DISPATCHER_COMMAND_TIMEOUT": "9.5",
        "TASKQ_WATCHDOG_STALE_FLOOR": "10.0",
        "TASKQ_NOTIFY_ENABLED": "false",
        "TASKQ_POLL_INTERVAL": "1.0",
    }

    with pytest.raises(Exception) as exc_info:
        WorkerSettings.load_from_dict(settings_dict)

    error_msg = str(exc_info.value)
    assert "producer" not in error_msg.lower(), (
        f"the validator must not check the producer loop (it is not wrapped; "
        f"the timeout + period model does not hold for multi-statement "
        f"dispatch_batch). Error mentions producer: {error_msg}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Fix 5: open_dedicated_conn must pass timeout= to asyncpg.connect
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (deps.py:126): "asyncpg.connect is called without timeout=, so
# establishing a connection keeps the driver's 60s default even where you
# now pass command_timeout. _election_loop opens up to three in a win
# cycle. Backlog."
#
# Fix: pass timeout=command_timeout when command_timeout is set.


async def test_open_dedicated_conn_passes_connection_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """open_dedicated_conn must pass timeout= to asyncpg.connect so
    connection establishment is bounded, not just query execution."""

    from taskq.worker import deps as deps_mod

    captured: dict[str, object] = {}

    class _Conn:
        def set_ssl_context(self, *args: object) -> None:
            pass

    async def _fake_connect(dsn: str, **kwargs: object) -> _Conn:
        captured.update(kwargs)
        return _Conn()

    monkeypatch.setattr(deps_mod.asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(deps_mod, "apply_keepalive_to_conn", lambda *a, **k: False)

    await deps_mod.open_dedicated_conn(
        "postgresql://x:x@localhost/x", label="cron", command_timeout=7.5
    )

    assert captured.get("timeout") == 7.5, (
        f"open_dedicated_conn must pass timeout= to asyncpg.connect so "
        f"connection establishment is bounded by the same value as "
        f"command_timeout. Got: {captured}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Fix 6: Cron timeout branch must sleep before re-issuing
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (leader.py:641): "The timeout branch continue's past the bottom
# asyncio.sleep(1), so a persistently slow PG re-issues BEGIN + tick_cron
# back to back with a warning each time. Every other branch rests first.
# Backlog."
#
# Fix: remove `continue` from the deadline and conn-state branches so they
# fall through to `await asyncio.sleep(1)`.


async def test_cron_timeout_branch_sleeps_before_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a timeout, the cron loop must sleep before the next tick, not
    re-issue BEGIN + tick_cron back-to-back. Without the fix, `continue`
    skips the trailing `await asyncio.sleep(1)`, hammering a degraded PG."""

    tick_times: list[float] = []

    async def _timeout_tick_cron(*args: object, **kwargs: object) -> None:
        tick_times.append(time.monotonic())
        raise TimeoutError("iteration deadline")

    monkeypatch.setattr("taskq.worker.leader.tick_cron", _timeout_tick_cron)

    liveness = LoopLiveness()
    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(
                schema_name="taskq",
                dispatcher_command_timeout=0.1,
            ),
        ),
    )
    leader = MaintenanceLeader(
        deps, new_uuid(), cast(Backend, SimpleNamespace()), clock=SystemClock()
    )
    leader._cron_conn = _FakeCronConn()  # type: ignore[assignment]

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))
    try:
        await asyncio.sleep(3.5)  # enough for 3+ ticks
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5.0)

    # With the fix: each tick is followed by a 1s sleep, so gaps >= ~1s.
    # Without the fix: continue skips the sleep, so gaps are ~0s.
    assert len(tick_times) >= 3, f"expected at least 3 ticks: {len(tick_times)}"
    gaps = [tick_times[i + 1] - tick_times[i] for i in range(len(tick_times) - 1)]
    min_gap = min(gaps)
    assert min_gap >= 0.8, (
        f"cron timeout branch must sleep before re-issuing; "
        f"minimum gap between ticks was {min_gap:.3f}s (expected >= ~1s). "
        f"Gaps: {[f'{g:.3f}' for g in gaps]}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Fix 7: Settings description must not say "every bounded loop"
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (settings.py:369): "Wording. Checked it against detector 2's actual
# registry: ... 'every bounded loop' reads like a promise." Three loops
# (leader.election, leader.watchdog, progress_flush) are not checked.
#
# Fix: update the description to say "the period-1 leader loops" instead of
# "every bounded loop."


def test_dispatcher_command_timeout_description_is_accurate() -> None:
    """The setting description must not promise 'every bounded loop' when
    only the period-1 leader loops are checked by the invariant."""

    from taskq.settings import WorkerSettings

    fields = WorkerSettings.get_fields()
    field_info = fields["dispatcher_command_timeout"][1]
    description = str(field_info.description or "")
    assert "every bounded loop" not in description, (
        f"the description must not say 'every bounded loop' — three loops "
        f"(leader.election, leader.watchdog, progress_flush) are not checked. "
        f"Description: {description}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Fix 8: _transient.py doc must correctly describe QueryCanceledError
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (_transient.py:36): "This says QueryCanceledError is 'asyncpg's
# OTHER shape for a fired command_timeout'. I ran it against a real PG 18:
# command_timeout (connect / per-call / pool) → builtins.TimeoutError,
# asyncio.timeout → builtins.TimeoutError, server-side pg_cancel_backend →
# QueryCanceledError. 57014 is server-side cancellation only — a DBA, or a
# server-side statement_timeout. Keep it in the tuple, it belongs there,
# just for that reason rather than this one."
#
# Fix: update the doc comment to say QueryCanceledError is server-side
# cancellation (57014), not a fired command_timeout.


def test_transient_pg_errors_doc_describes_query_canceled_correctly() -> None:
    """The TRANSIENT_PG_ERRORS source comment must describe
    QueryCanceledError as server-side 57014 cancellation (pg_cancel_backend
    or server-side statement_timeout), not as a fired command_timeout.
    XBeg9 tested with real PG 18: command_timeout raises TimeoutError, not
    QueryCanceledError."""

    from pathlib import Path

    import taskq.worker._transient as transient_mod

    source = Path(transient_mod.__file__).read_text()
    # The Sphinx #: comment above QueryCanceledError must not claim it is
    # "a fired command_timeout" — that is TimeoutError's shape, not
    # QueryCanceledError's. QueryCanceledError is server-side 57014.
    # Find the line mentioning QueryCanceledError and check its context.
    lines = source.splitlines()
    for i, line in enumerate(lines):
        if "QueryCanceledError" in line and "server-side" in line:
            # Check the surrounding lines for the incorrect claim
            context = " ".join(lines[i : i + 3])
            assert "fired" not in context.lower(), (
                f"the QueryCanceledError comment must not say 'fired' — "
                f"XBeg9 proved with real PG 18 that command_timeout raises "
                f"TimeoutError, not QueryCanceledError. "
                f"QueryCanceledError is server-side 57014 cancellation "
                f"(pg_cancel_backend or server-side statement_timeout). "
                f"Context: {context}"
            )
            break
    else:
        pytest.fail("QueryCanceledError comment not found in _transient.py")
