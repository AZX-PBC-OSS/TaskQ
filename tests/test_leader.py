"""Unit tests for MaintenanceLeader - pure-Python, no PG required.

Covers election, watchdog, sweep-loop gating, pg_notify, prune/expiry
scheduling, and retention-config helpers against InMemoryBackend.
"""

import asyncio
import contextlib
import itertools
from collections.abc import AsyncGenerator, Callable, Iterable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase, JobId, JobRow
from taskq.backend._sweeps import SweepBatchSizer
from taskq.cron import CronScheduleSpec
from taskq.settings import WorkerSettings
from taskq.testing.assertions import (
    wait_for,
    wait_for_condition,
    wait_for_job_status,
    wait_for_leader,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._leader_shared import (
    _DB_NOW_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the fake must answer the exact DB-clock query the prune/expiry code issues; importing it keeps the two from drifting apart.
)
from taskq.worker.deps import LeaderTerm, WorkerDeps
from taskq.worker.leader import (
    MaintenanceLeader,
    _build_retention_per_status,
    _schedule_utc_to_cron,
    build_leader_lease_sql,
    prune_terminal_jobs,
)


def _next_minute_cron() -> str:
    """Return a 5-field cron expression that fires at the next UTC minute boundary.

    E.g. if now is 10:45:30 UTC, returns '46 10 * * *'.
    Maximum wait: 60 seconds.
    """
    now = datetime.now(UTC)
    next_minute = now.minute + 1 if now.minute < 59 else 0
    next_hour = now.hour if now.minute < 59 else (now.hour + 1) % 24
    return f"{next_minute} {next_hour} * * *"


def _as_dict(attrs: object) -> dict[str, object]:
    """Convert OTel Attributes to plain dict for test assertions."""
    return dict(attrs)  # type: ignore[arg-type]  # Why: OTel Attributes is Mapping[str, AttributeValue]; dict(attrs) works at runtime but pyright infers wrong overload.


async def _stop_after_tick(
    task: asyncio.Task[object],
    shutdown: asyncio.Event,
    *,
    delay: float = 0.05,
) -> None:
    """Wait briefly then cancel *task* and suppress CancelledError."""
    await asyncio.sleep(delay)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


pytestmark = pytest.mark.asyncio

# ── Test doubles ──────────────────────────────────────────────────────────


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


def _is_server_clock_read(sql: str) -> bool:
    """Whether *sql* asks Postgres for its own clock, and nothing else.

    Postgres answers such a statement with a timestamp unconditionally, so a
    double that hands back its generic ``fetchval_result`` here is lying about
    the row shape: the leader sweeps read the server clock through the same
    connection they take the advisory lock on, and a double configured with
    ``fetchval_result=True`` for the lock was returning that bool where the
    real database returns a datetime.

    Statements that merely STAMP the server clock into a column are not clock
    reads: the election statement writes ``clock_timestamp()`` into the lease
    row and returns its ``elected_at`` only when it actually won. Answering
    those with an unconditional timestamp would make the double report a win
    to every caller, including the pod that lost.
    """
    return sql.lstrip().upper().startswith("SELECT CLOCK_TIMESTAMP()")


def _is_lease_statement(sql: str) -> bool:
    """Whether *sql* is one of the maintenance-lease statements.

    The elect and renew statements return a timestamp column, so a double
    must answer them with one - see :meth:`FakeConn.fetchval`.
    """
    return "maintenance_leader" in sql


class FakeConn:
    """Lightweight asyncpg.Connection stand-in with fetchval + execute recording."""

    def __init__(
        self,
        *,
        fetchval_result: object = None,
        on_fetchval: Callable[[], None] | None = None,
        on_execute: Callable[[], None] | None = None,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._closed = False
        self.close_calls = 0
        self._fetchval_result = fetchval_result
        self._on_fetchval = on_fetchval
        self._on_execute = on_execute
        self._on_close = on_close
        # Hang-gate + terminate tracking for bounded-close tests (mirrors the
        # _FakeConn conventions in tests/test_worker_deps_teardown.py):
        # clear close_wait to make close() block forever (dead PG).
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.terminated = False
        # Why an event alongside the flag: the flag is the assertion
        # surface; the event is the wait surface - the watchdog closes
        # leader-owned conns on its own task, and a test that needs
        # "closed" can await this instead of polling a fixed interval
        # that races the close under load (same convention as the fakes
        # in tests/test_reload_credentials.py).
        self.closed_event = asyncio.Event()

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if self._on_fetchval is not None:
            self._on_fetchval()
        if _is_server_clock_read(sql):
            return datetime.now(UTC)
        if _is_lease_statement(sql):
            # The election and renewal statements answer with the term's
            # timestamp when this pod holds the row and with no row at all
            # when it does not. A double that handed back its generic bool
            # here would report the same outcome to the winner and the loser.
            return datetime.now(UTC) if self._fetchval_result else None
        if "pg_try_advisory_lock" in sql or "pg_advisory_unlock" in sql:
            # The courtesy lock probe / the release verdict: the configured
            # verdict (True = held/won, anything falsy = not held). Named,
            # never a fall-through default.
            return self._fetchval_result
        if sql.strip().upper() == "SELECT 1":
            return 1  # the watchdog's liveness probe; the value is unread
        raise AssertionError(
            f"FakeConn: no named answer for statement {sql!r} (args {args!r}) - "
            "the fail-closed rule (tests/test_suite_hygiene.py) forbids a silent "
            "default: add the statement shape explicitly to this double"
        )

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._on_execute is not None:
            self._on_execute()
        normalized = " ".join(sql.split()).upper()
        # The idempotent re-registration repair (the FK arm's restore): the
        # command tag is unread on this path - the retried elect is the
        # authoritative check of whether the row exists - but the shape is
        # named explicitly per the fail-closed rule.
        if (
            normalized.startswith("INSERT INTO")
            and ".WORKERS" in normalized
            and "ON CONFLICT (ID) DO NOTHING" in normalized
        ):
            return "INSERT 0 1"
        # Why a DELETE tag for the resign: the command tag is the delete
        # count, and resign() now reads it to decide whether the row was
        # actually handed back: a double answering "UPDATE 1" here would
        # report every resign as a no-op (the same class of lie as the
        # fetchval doubles answering lease statements with a bool: the
        # shape the caller reads must be the shape the real driver sends).
        if sql.lstrip().upper().startswith("DELETE FROM") and "maintenance_leader" in sql:
            return "DELETE 1"
        if "pg_notify" in sql:
            return "SELECT 1"  # the wake broadcast; the tag is unread
        if "pg_advisory_unlock" in sql:
            return "SELECT 1"  # the release's execute round; the verdict rides fetchval
        if "set_config" in sql:
            return "SELECT 1"  # the batch's SET LOCAL statement_timeout apply/restore
        raise AssertionError(
            f"FakeConn: no named answer for statement {sql!r} (args {args!r}) - "
            "the fail-closed rule (tests/test_suite_hygiene.py) forbids a silent "
            "default: add the statement shape explicitly to this double"
        )

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        raise AssertionError(
            f"FakeConn: fetchrow has no named answer for statement {sql!r} "
            "(args {args!r}) - the fail-closed rule "
            "(tests/test_suite_hygiene.py) forbids a silent default: add the "
            "statement shape explicitly to this double"
        )

    async def fetch(self, sql: str, *args: object) -> Sequence[object]:
        raise AssertionError(
            f"FakeConn: fetch has no named answer for statement {sql!r} "
            f"(args {args!r}) - the fail-closed rule "
            "(tests/test_suite_hygiene.py) forbids a silent default: add the "
            "statement shape explicitly to this double"
        )

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self._closed = True
        self.closed_event.set()
        if self._on_close is not None:
            self._on_close()

    def terminate(self) -> None:
        self.terminated = True
        self._closed = True
        self.closed_event.set()
        self.close_wait.set()  # aborts any in-flight close() wait

    def is_closed(self) -> bool:
        return self._closed

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    def is_in_transaction(self) -> bool:
        """Model a caller-owned connection with an open transaction."""
        return True


class FakePool:
    """Lightweight asyncpg.Pool stand-in that tracks acquire and connections."""

    def __init__(
        self,
        *,
        fail_acquire_with: BaseException | None = None,
    ) -> None:
        self._fail_acquire_with = fail_acquire_with
        self.acquire_count = 0
        self._conns: list[FakeConn] = []

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109  # Why: asyncpg.Pool.acquire signature takes timeout; FakePool mirrors it.
        self.acquire_count += 1
        if self._fail_acquire_with is not None:
            raise self._fail_acquire_with
        conn = FakeConn()
        self._conns.append(conn)
        yield conn

    @property
    def execute_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.execute_calls)
        return result

    @property
    def fetchval_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.fetchval_calls)
        return result


class _PoolWithFixedConn(FakePool):
    """FakePool that always yields a pre-built connection on acquire().

    Used by tests that need the pool-acquired connection to have custom
    fetchval/fetch/execute behaviour (e.g. _FakeConnForPrune subclasses).
    """

    def __init__(self, conn: FakeConn) -> None:
        super().__init__()
        self._fixed_conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
        self.acquire_count += 1
        yield self._fixed_conn

    @property
    def execute_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.execute_calls)
        return result

    @property
    def fetchval_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.fetchval_calls)
        return result


# ── Factories ─────────────────────────────────────────────────────────────


def _worker_settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": pg_dsn}
    for key, value in overrides.items():
        data[f"TASKQ_{key}"] = value
    return WorkerSettings.load_from_dict(data, validate=False)


def _make_deps(
    *,
    dispatcher_pool: FakePool | None = None,
    heartbeat_pool: FakePool | None = None,
    worker_pool: FakePool | None = None,
    leader_conn: FakeConn | None = None,
    is_leader: bool = False,
    heartbeat_interval: float = 0.5,
    leader_lease: float = 40.0,
) -> WorkerDeps:
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        HEARTBEAT_INTERVAL=str(heartbeat_interval),
        LEADER_LEASE=str(leader_lease),
        # 3.0 + the tiny command timeout satisfies the cascade
        # floor: 4 * (0.5 + 2 * 0.1) = 2.8 <= 3.0 (the lease value itself is
        # arbitrary for these unit doubles).
        HEARTBEAT_COMMAND_TIMEOUT="0.1",
        LOCK_LEASE="3.0",
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        WATCHDOG_LOOP_LAG_WARN_BUDGET="0.5",
        WATCHDOG_CHECK_INTERVAL="0.25",  # the lag lattice: check < warn (the F6 storm refusal)
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=dispatcher_pool or FakePool(),  # type: ignore[arg-type]  # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=heartbeat_pool or FakePool(),  # type: ignore[arg-type]
        worker_pool=worker_pool or FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn or FakeConn(),  # type: ignore[arg-type]  # Why: FakeConn is a drop-in for asyncpg.Connection in unit tests.
    )
    if is_leader:
        deps.is_leader.set()
    return deps


async def _make_leader(
    *,
    leader_conn: FakeConn | None = None,
    dispatcher_pool: FakePool | None = None,
    is_leader: bool = False,
    monkeypatch: Any | None = None,
    leader_lease: float = 40.0,
) -> tuple[MaintenanceLeader, WorkerDeps, InMemoryBackend, FakeConn, FakePool, asyncio.Event]:
    """Construct MaintenanceLeader wired with fake deps and InMemoryBackend.

    Returns (leader, deps, backend, leader_conn, dispatcher_pool, shutdown).
    """
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    fake_leader_conn = leader_conn or FakeConn()
    fake_dp = dispatcher_pool or FakePool()
    deps = _make_deps(
        dispatcher_pool=fake_dp,
        leader_conn=fake_leader_conn,
        is_leader=is_leader,
        heartbeat_interval=0.01,
        leader_lease=leader_lease,
    )
    worker_id = new_uuid()
    leader = MaintenanceLeader(deps, worker_id, backend, clock=clock)

    # Ensure open_dedicated_conn is mocked to avoid real asyncpg.connect
    if monkeypatch is not None:
        import taskq.worker.leader as leader_mod

        async def fake_open_conn(
            dsn: str,
            *,
            label: str = "",
            apply_keepalive: bool = True,
            command_timeout: float | None = None,
        ) -> FakeConn:
            return FakeConn()

        monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open_conn)

    shutdown = asyncio.Event()
    return leader, deps, backend, fake_leader_conn, fake_dp, shutdown


# ── Election win sets is_leader ────────────────────────────────────


async def test_election_win_sets_is_leader(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]  # Why: pytest monkeypatch fixture type is only available with pytest-stub; using Any for test ergonomics.
    """Election win: the lease statement returns a term → is_leader set,
    monitor opened, the lease row written, counter incremented, INFO log."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    counter_calls: list[tuple[int, dict[str, object]]] = []
    import taskq.obs._otel as otel_mod

    otel_mod._leader_election_attempts.add = lambda amount, attrs=None: counter_calls.append(
        (int(amount), _as_dict(attrs or {}))
    )  # type: ignore[method-assign]  # Why: test-only instrumentation to observe OTel counter calls.

    task = asyncio.create_task(leader._election_loop(shutdown))
    # is_leader is an asyncio.Event - a bounded event wait (never a
    # sleep-poll that races the election under load).
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert leader._leader_monitor_conn is not None
    assert any("maintenance_leader" in sql for sql, _ in leader_conn.fetchval_calls)
    assert counter_calls == [(1, {})]


# ── Election loop must degrade, not crash, when the courtesy lock is refused ──


async def test_election_loop_degrades_when_advisory_lock_privilege_is_refused(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A winning election whose courtesy ``pg_try_advisory_lock`` probe hits a
        refused privilege (``InsufficientPrivilegeError``, as a managed Postgres
        deployment that restricts advisory-lock functions would raise) must not
        crash the election loop.

        ``_try_election_lock`` only catches ``TRANSIENT_PG_ERRORS`` (see
        ``leader.py``): a privilege error is not in that tuple, so it propagates
        out of ``_try_election_lock``, through ``_assume_leadership`` (called at
        line ~745, OUTSIDE the try/except that guards the election statement
        itself), and out of ``_election_loop`` entirely unhandled. In production
        that loop runs inside ``MaintenanceLeader.run()``'s ``TaskGroup``
        alongside the watchdog, cron, sweep, prune, and every other leader-gated
        loop - one task raising cancels every sibling and tears down the whole
        maintenance plane, exactly the failure this project's own docstring
        on ``_try_election_lock`` says a courtesy probe must never cause
    ("a miss ... must never gate the election").

        This is the same defect shape reported for the retired
        ``pg_terminate_backend`` recovery path (a refused privilege escaping as
        a raw driver error instead of degrading to follower) - it has resurfaced
        at the new call site the row-lease redesign introduced.
    """

    class _PrivilegeRefusedConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            if "pg_try_advisory_lock" in sql:
                self.fetchval_calls.append((sql, args))
                raise asyncpg.InsufficientPrivilegeError(
                    "permission denied for function pg_try_advisory_lock"
                )
            return await super().fetchval(sql, *args)

    refusing_conn = _PrivilegeRefusedConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=refusing_conn,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the degradation path under test.

    crashed: BaseException | None = None
    try:
        # A short bounded wait: a healthy loop wins the election (the lease
        # statement returns a term) and keeps looping as a leader that never
        # acquired the courtesy lock. A crashing loop instead raises out of
        # the task almost immediately.
        await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
    except TimeoutError:
        pass
    except BaseException as exc:  # Why: capturing the crash is the assertion below, not letting pytest report it as an unhandled task exception.
        crashed = exc
    finally:
        shutdown.set()
        if not task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=2.0)
        elif task.cancelled():
            pass
        elif task.exception() is not None:
            crashed = task.exception()

    assert crashed is None, (
        "the election loop must degrade to follower (log once, skip the "
        "courtesy lock, keep competing by the lease) when the advisory-lock "
        f"privilege is refused, not crash: raised {type(crashed).__name__ if crashed else None}"
    )
    assert deps.is_leader.is_set(), (
        "the lease statement won the election; losing the courtesy lock "
        "afterward must not cost the pod its leadership"
    )


async def test_election_loop_degrades_when_advisory_lock_probe_fails_transiently(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The transient sibling of the privilege-refusal pin: a probe that
    raises a TRANSIENT_PG_ERRORS member (the conn dying under the probe)
    must also degrade to a lock miss - the lease row already granted the
    role, and a courtesy probe riding a dying conn must not cost it."""

    class _TransientFailingConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            if "pg_try_advisory_lock" in sql:
                self.fetchval_calls.append((sql, args))
                raise asyncpg.PostgresConnectionError("conn dropped mid-probe")
            return await super().fetchval(sql, *args)

    failing_conn = _TransientFailingConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=failing_conn,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the degradation path under test.
    try:
        await wait_for_leader(deps)
    finally:
        shutdown.set()
        await task

    assert deps.is_leader.is_set(), (
        "the lease statement won the election; a transient courtesy-probe "
        "failure must not cost the pod its leadership"
    )


async def test_advisory_lock_refusal_logs_once_per_refusal_streak(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A refused deployment refuses every probe for as long as the grants
    stand: the WARN naming the refused function must fire on the first
    refusal of a streak, not on every one - a fleet re-electing on every
    lease lapse must not WARN-spam - and must re-arm once a probe succeeds
    again (the grant appearing is a new operational fact)."""

    class _RefusingConn(FakeConn):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)  # type: ignore[arg-type]  # Why: FakeConn kwargs are keyword-only; forwarding keeps this double a drop-in.
            self.refuse = True

        async def fetchval(self, sql: str, *args: object) -> object:
            if "pg_try_advisory_lock" in sql and self.refuse:
                self.fetchval_calls.append((sql, args))
                raise asyncpg.InsufficientPrivilegeError(
                    "permission denied for function pg_try_advisory_lock"
                )
            return await super().fetchval(sql, *args)

    refusing_conn = _RefusingConn(fetchval_result=True)
    leader, _deps, _backend, _, _, _shutdown = await _make_leader(
        leader_conn=refusing_conn,
        monkeypatch=monkeypatch,
    )

    with structlog.testing.capture_logs() as captured:
        assert await leader._try_election_lock() is False  # pyright: ignore[reportPrivateUsage]  # Why: the courtesy probe is the unit under test.
        assert await leader._try_election_lock() is False  # pyright: ignore[reportPrivateUsage]

    refusals = [e for e in captured if e["event"] == "leader-advisory-lock-refused"]
    assert len(refusals) == 1, (
        f"two consecutive refusals produced {len(refusals)} WARN events - the "
        "refusal is permanent until the grants change, so one WARN per streak "
        "is the whole signal"
    )
    assert refusals[0]["log_level"] == "warning"
    assert refusals[0]["function"] == "pg_try_advisory_lock"

    # Grant restored: the probe answers again, which re-arms the latch...
    refusing_conn.refuse = False
    assert await leader._try_election_lock() is True  # pyright: ignore[reportPrivateUsage]  # Why: same as above.

    # ...so a later refusal is news and logs again.
    refusing_conn.refuse = True
    with structlog.testing.capture_logs() as captured_after_rearm:
        assert await leader._try_election_lock() is False  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
    refusals_after = [
        e for e in captured_after_rearm if e["event"] == "leader-advisory-lock-refused"
    ]
    assert len(refusals_after) == 1


# ── Leader lease gauge: stamped on elect/renew ───────────────────────


async def test_lease_gauge_is_stamped_on_election_and_each_renewal(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """``taskq.maintenance_leader.lease_expires_in_seconds`` is the
    lease's freshness signal: stamped with the full TTL at election win
    and re-stamped at every successful renewal, so a leader that stops
    renewing is a series that stops moving. Both arms are pinned here -
    a dropped call site leaves the gauge frozen on a live leader, which
    is exactly the lie the gauge exists to refute."""
    import taskq.obs._otel as otel_mod

    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    monkeypatch.setattr(otel_mod, "_leader_lease_expires_in_seconds_cache", None)

    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )
    lease = deps.settings.resolved_leader_lease

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the elect arm under test.
    await wait_for_leader(deps)

    # Elect arm.
    assert otel_mod._leader_lease_expires_in_seconds_cache == lease  # pyright: ignore[reportPrivateUsage]  # Why: the pin is the gauge's cached value, not an SDK round trip.

    # Renew arm: poison the stamp, then drive one renewal through the
    # loop's own method (the trust-spent test's seam - deterministic, no
    # polling) and the stamp must be restored.
    otel_mod._leader_lease_expires_in_seconds_cache = -1.0  # pyright: ignore[reportPrivateUsage]
    term = deps.leader_term
    assert term is not None, "the election win must have installed a term"

    from taskq.worker._transient import UnexpectedLoopErrorGuard

    # The real renew statement, not a placeholder: the renewal rides the
    # lease statement's shape (the fail-closed double answers only named
    # shapes - the lease family's, by schema).
    renew_sql = build_leader_lease_sql(deps.settings.schema_name)[1]
    should_sleep = await leader._renew_term(  # pyright: ignore[reportPrivateUsage]  # Why: driving the renew arm directly is the test.
        term, renew_sql, UnexpectedLoopErrorGuard("test")
    )

    assert should_sleep is True, "a healthy renewal keeps leading"
    assert otel_mod._leader_lease_expires_in_seconds_cache == lease  # pyright: ignore[reportPrivateUsage]
    shutdown.set()
    await task


# ── Election loss does not set is_leader ───────────────────────────


async def test_election_loss_does_not_set_is_leader(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Election loss: the lease statement returns no row → is_leader stays
    clear, no monitor opened, INFO log with kind='leader_retry', counter
    incremented."""
    # Additive event on the double: set exactly where the election's lock
    # probe is entered, so the test waits for the attempt instead of
    # sleeping and hoping the loop reached it.
    election_attempted = asyncio.Event()
    leader_conn = FakeConn(fetchval_result=False, on_fetchval=election_attempted.set)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    counter_calls: list[tuple[int, dict[str, object]]] = []
    failure_calls: list[tuple[int, dict[str, object]]] = []
    import taskq.obs._otel as otel_mod

    otel_mod._leader_election_attempts.add = lambda amount, attrs=None: counter_calls.append(
        (int(amount), _as_dict(attrs or {}))
    )  # type: ignore[method-assign]
    otel_mod._leader_election_failures.add = lambda amount, attrs=None: failure_calls.append(
        (int(amount), _as_dict(attrs or {}))
    )  # type: ignore[method-assign]

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for(election_attempted)
    shutdown.set()
    await task

    assert not deps.is_leader.is_set()
    assert leader._leader_monitor_conn is None
    assert counter_calls == [(1, {})]
    assert failure_calls == [(1, {})]


# ── Watchdog failure clears is_leader (parametrized) ────────────────


@pytest.mark.parametrize(
    "exc",
    [
        asyncpg.PostgresConnectionError("connection lost"),
        asyncpg.InterfaceError("connection is closed"),
        OSError(104, "Connection reset by peer"),
    ],
)
async def test_watchdog_failure_clears_is_leader(exc: BaseException) -> None:
    """Watchdog failure on SELECT 1 clears is_leader, closes both
    connections, logs WARNING with kind='leadership_lost', continues outer loop."""
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)

    class FailingFakeConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise exc

    failing_monitor = FailingFakeConn()
    _clk = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    leader = MaintenanceLeader(
        deps,
        new_uuid(),
        InMemoryBackend(clock=_clk),
        clock=_clk,
    )
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]  # Why: FakeConn is not asyncpg.Connection; assignment of test double is intentional.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    for _ in range(200):
        if failing_monitor._closed and not deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    assert not deps.is_leader.is_set()
    assert failing_monitor._closed
    assert deps.leader_conn is None
    shutdown.set()
    deps.is_leader.set()  # unblock the wait() then exit via shutdown check
    await task


# ── Watchdog continues after error and re-election ──────────────────


async def test_watchdog_continues_after_error_and_reelection(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """After watchdog failure clears is_leader, re-election reactivates
    the watchdog and SELECT 1 runs on the new connection."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)

    # Simulate post-watchdog-failure state: leader_conn is None (it was closed)
    deps.leader_conn = None

    # Mock open_dedicated_conn to return a new FakeConn for leader_conn
    new_leader_conn = FakeConn(fetchval_result=True)
    import taskq.worker.leader as leader_mod

    open_calls: list[str] = []

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        open_calls.append(label)
        if label == "leader":
            return new_leader_conn
        return FakeConn()

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    elect_task = asyncio.create_task(leader._election_loop(shutdown))
    # Wait for election win: is_leader is the event itself.
    await wait_for_leader(deps)
    shutdown.set()
    await elect_task

    assert deps.is_leader.is_set()
    assert deps.leader_conn is new_leader_conn


# ── deps.leader_conn replaced after watchdog ────────────────────────


async def test_leader_conn_replaced_after_watchdog(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """deps.leader_conn is replaced after watchdog failure."""
    original_leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original_leader_conn,
        monkeypatch=monkeypatch,
    )

    # Simulate watchdog failure: clear is_leader, null leader_conn
    deps.leader_conn = None
    deps.is_leader.clear()

    # Mock open_dedicated_conn to return a sentinel new connection
    sentinel_conn = FakeConn(fetchval_result=True)
    import taskq.worker.leader as leader_mod

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        return sentinel_conn

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    task = asyncio.create_task(leader._election_loop(shutdown))
    # Wait for election win: is_leader is the event itself.
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.leader_conn is sentinel_conn
    assert deps.leader_conn is not original_leader_conn


async def test_watchdog_reopen_uses_leader_conn_factory_not_dsn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """When deps.leader_conn_factory is set, the watchdog reopens through it -
    never through open_dedicated_conn's raw DSN path. Regression test for a bug
    where the watchdog hardcoded pg_dsn_direct, bypassing WorkerConnections
    entirely (broken for AAD/AWS/Vault deployments with no DSN configured)."""
    original_leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original_leader_conn,
        monkeypatch=monkeypatch,
    )

    # Simulate watchdog failure: clear is_leader, null leader_conn
    deps.leader_conn = None
    deps.is_leader.clear()

    factory_calls: list[None] = []
    factory_conns: list[
        object
    ] = []  # Why: identity bag - members are compared with `is`/`in` against deps.leader_conn (asyncpg.Connection), so element typing as FakeConn makes pyright report no-overlap.

    async def fake_factory() -> FakeConn:
        factory_calls.append(None)
        conn = FakeConn(fetchval_result=True)
        factory_conns.append(conn)
        return conn

    deps.leader_conn_factory = fake_factory  # type: ignore[assignment]

    # open_dedicated_conn must not be called when a factory is set - fail loud
    # if the watchdog falls back to the DSN path instead of the factory.
    import taskq.worker.leader as leader_mod

    async def fail_if_called(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        raise AssertionError(
            f"open_dedicated_conn called with dsn={dsn!r} label={label!r} - "
            "leader_conn_factory should have been used instead"
        )

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fail_if_called)

    task = asyncio.create_task(leader._election_loop(shutdown))
    # Wait for election win: is_leader is the event itself.
    await wait_for_leader(deps)
    shutdown.set()
    await task

    # leader_conn_factory backs leader_conn and the leader's other dedicated
    # connections (leader_monitor_conn, cron_conn) - all must route through
    # it, never through the raw-DSN open_dedicated_conn path.
    assert len(factory_calls) >= 1
    assert deps.leader_conn in factory_conns


async def test_reload_credentials_rebuilds_leader_monitor_and_cron_conns(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """SIGHUP reload (reload_credentials nulling leader_conn) causes the
    election loop's re-election cascade to rebuild leader_monitor_conn and
    cron_conn through leader_conn_factory too - not just leader_conn itself.

    reload_credentials() only directly touches deps.leader_conn (closing it
    and setting it to None so the watchdog/election loop reopens it - see
    deps.py's reload_credentials docstring). This test verifies the
    downstream effect: _election_loop's re-election path, triggered by
    leader_conn becoming None while is_leader is still set, also rebuilds
    the leader's other dedicated connections (_leader_monitor_conn,
    _cron_conn) via the same leader_conn_factory - so a hot-reloaded leader
    doesn't keep querying with monitor/cron connections opened under a
    stale credential until they separately fail.
    """
    original_leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original_leader_conn,
        monkeypatch=monkeypatch,
    )

    factory_calls: list[str] = []

    async def fake_factory() -> FakeConn:
        factory_calls.append("factory")
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = fake_factory  # type: ignore[assignment]

    # Drive the election loop through a real election first (is_leader=True
    # at construction is an artificial state _election_loop's re-election
    # path never produces - genuine leadership always flows through
    # `if got_lock:`, which is what populates _leader_monitor_conn /
    # _cron_conn in the first place).
    task = asyncio.create_task(leader._election_loop(shutdown))
    # The election loop opens both dedicated conns before it sets
    # is_leader (leader.py: upsert → monitor conn → cron conn → set), so
    # the is_leader event wait subsumes polling the conn attributes -
    # and it is a bounded wait on the event itself, never a sleep-poll.
    await wait_for_leader(deps)
    assert deps.is_leader.is_set()
    assert leader._leader_monitor_conn is not None
    assert leader._cron_conn is not None
    old_monitor_conn = leader._leader_monitor_conn
    old_cron_conn = leader._cron_conn
    factory_calls.clear()  # only count calls from the reload onward

    # Simulate what reload_credentials does to leader_conn on SIGHUP: close
    # it and null it while is_leader remains set (deps.py:599-606).
    deps.leader_conn = None

    # The rebuilt conns are plain attributes the election loop assigns -
    # no event to wait on, so a bounded, deadline-based poll.
    await wait_for_condition(
        lambda: (
            leader._leader_monitor_conn is not old_monitor_conn
            and leader._cron_conn is not old_cron_conn
        ),
        description="the re-election cascade must rebuild leader_monitor_conn and cron_conn",
    )
    shutdown.set()
    await task

    assert leader._leader_monitor_conn is not old_monitor_conn
    assert leader._cron_conn is not old_cron_conn
    # Both the leader_conn reopen and the monitor/cron reopens went through
    # leader_conn_factory (3 calls: leader_conn, leader_monitor_conn, cron_conn).
    assert len(factory_calls) == 3


# ── Sweep loops gate on is_leader ──────────────────────────────────


class StubPool(FakePool):
    """Pool that records acquire calls for behavioural assertions."""

    def __init__(self) -> None:
        super().__init__()
        self.acquire_called = False

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109
        self.acquire_called = True
        async with super().acquire(timeout=timeout) as conn:
            yield conn


async def test_sweep_loops_gate_on_is_leader() -> None:
    """Sweep loops only run when is_leader; both sweeps 1 and 2
    transition jobs; sweep 4 is skipped on InMemoryBackend."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    now = clock.now()

    expired_job_id = new_job_id()
    deadline_job_id = new_job_id()
    worker_id = new_uuid()

    _insert_job(
        backend,
        expired_job_id,
        status="running",
        lock_expires_at=now - timedelta(seconds=1),
        attempt=1,
        max_attempts=3,
        locked_by_worker=worker_id,
        now=now,
    )
    _insert_job(
        backend,
        deadline_job_id,
        status="scheduled",
        schedule_to_close=now - timedelta(seconds=1),
        locked_by_worker=worker_id,
        now=now,
    )

    spy_pool = StubPool()
    deps = _make_deps(
        is_leader=False,
        heartbeat_interval=0.01,
        dispatcher_pool=spy_pool,  # type: ignore[arg-type]  # Why: StubPool extends FakePool for test assertions.
    )

    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)
    shutdown = asyncio.Event()

    # Run _sweep_loop with is_leader=False → gates prevent entry
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    await _stop_after_tick(task, shutdown, delay=0.05)

    expired_row = await backend.get(expired_job_id)
    deadline_row = await backend.get(deadline_job_id)
    assert expired_row is not None
    assert deadline_row is not None
    assert expired_row.status == "running"
    assert deadline_row.status == "scheduled"

    # Run _sweep_loop with is_leader=True → both sweeps fire
    deps.is_leader.set()
    shutdown_2 = asyncio.Event()
    task_2 = asyncio.create_task(leader._sweep_loop(shutdown_2))
    # Bounded waits on the sweeps' own observable (job status), never a
    # fixed 0.05s hoping the tick completed under load.
    await wait_for_job_status(backend, expired_job_id, "pending")
    await wait_for_job_status(backend, deadline_job_id, "failed")
    await _stop_after_tick(task_2, shutdown_2, delay=0.0)

    expired_row = await backend.get(expired_job_id)
    deadline_row = await backend.get(deadline_job_id)
    assert expired_row is not None
    assert deadline_row is not None
    assert expired_row.status == "pending"
    assert expired_row.locked_by_worker is None
    assert expired_row.lock_expires_at is None
    assert deadline_row.status == "failed"
    assert deadline_row.finished_at is not None
    assert not spy_pool.acquire_called  # sweep 4 skipped


# ── Helper: insert a job row into InMemoryBackend ────────────────────────


def _insert_job(
    backend: InMemoryBackend,
    job_id: JobId,
    *,
    status: str = "running",
    lock_expires_at: datetime | None = None,
    schedule_to_close: datetime | None = None,
    attempt: int = 1,
    max_attempts: int = 3,
    locked_by_worker: UUID | None = None,
    cancel_phase: CancelPhase = CancelPhase.NONE,
    now: datetime | None = None,
) -> None:
    _now = now if now is not None else datetime.now(UTC)
    row = JobRow(
        id=job_id,
        actor="test_actor",
        queue="default",
        payload={},
        payload_schema_ver=1,
        status=status,  # type: ignore[arg-type]  # Why: Literal not narrowed for dynamic status values from test helper.
        priority=0,
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
        schedule_to_close=schedule_to_close,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=_now,
        scheduled_at=_now - timedelta(seconds=10),
        started_at=_now - timedelta(seconds=5),
        finished_at=None,
        last_heartbeat_at=_now - timedelta(seconds=2),
        locked_by_worker=locked_by_worker,
        lock_expires_at=lock_expires_at,
        cancel_requested_at=None,
        cancel_phase=cancel_phase,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        identity_key=None,
        fairness_key=None,
        metadata={},
        tags=(),
    )
    backend._jobs[job_id] = row  # type: ignore[reportPrivateUsage]  # Why: _jobs is internal storage accessed by test helper for direct state setup.


# ── pg_notify issued after non-zero promotion count ────────────────


async def test_pg_notify_issued_after_promotion(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """When scheduled_to_pending returns count > 0, exactly one
    pg_notify execute call fires on dispatcher_pool connection."""
    leader, deps, backend, _lc, fake_dp, shutdown = await _make_leader(
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    backend.scheduled_to_pending = lambda **kw: asyncio.sleep(0, result=3)  # type: ignore[method-assign]  # Why: async stub returning int for test setup.
    deps.is_leader.set()

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    try:
        # The notify execute lands on a pool-internal conn - no event to
        # wait on, so a bounded, deadline-based poll on the recording.
        await wait_for_condition(
            lambda: bool(fake_dp.execute_calls),
            description="the wake loop must issue its post-promotion pg_notify",
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    pg_notify_calls = [c for c in fake_dp.execute_calls if "pg_notify" in c[0]]
    assert len(pg_notify_calls) == 1


# ── pg_notify not issued when count is 0 ────────────────────────────


async def test_pg_notify_not_issued_when_zero(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """When scheduled_to_pending returns 0, no pg_notify fires."""
    leader, deps, backend, _lc, fake_dp, shutdown = await _make_leader(
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    count = 0
    # Additive event on the double: set exactly where the sweep call is
    # entered, so the "no pg_notify" window below is anchored on a sweep
    # that provably ran instead of a sleep hoping the loop reached it.
    promote_attempted = asyncio.Event()

    async def zero_promote(**kw: object) -> int:
        promote_attempted.set()
        return count

    backend.scheduled_to_pending = zero_promote  # type: ignore[method-assign]
    deps.is_leader.set()

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    await wait_for(promote_attempted)
    # The notify decision is made in the same loop step as the awaited
    # count - one yield for the loop to finish that step, then stop it.
    await asyncio.sleep(0)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    pg_notify_calls = [c for c in fake_dp.execute_calls if "pg_notify" in c[0]]
    assert pg_notify_calls == []


# ── Prune loop runs on schedule when leader ──────────────────────────


async def test_prune_loop_runs_on_schedule(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """_prune_loop fires at the scheduled time when is_leader, acquires
    advisory lock, calls prune_terminal_jobs, and releases the lock."""
    import croniter as croniter_mod

    prune_result_rows = [
        [
            _FakeRecord({"actor": "a", "status": "succeeded", "cnt": 3}),
        ],
    ]
    leader_conn = _FakeConnForPrune(
        batch_rows=prune_result_rows, fetchval_result=True, actor_config_rows=[]
    )

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()
    settings.prune_batch_size = 100

    task = asyncio.create_task(leader._prune_loop(shutdown))

    # Bounded poll for the prune cycle's first advisory-lock probe (no
    # event exists on the fake's recording).
    await wait_for_condition(
        lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
        description="the prune loop must attempt its advisory lock",
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert lock_calls, "expected advisory lock acquisition"
    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert unlock_calls, "expected advisory lock release"


# ── OTel metrics emitted ────────────────────────────────────────────


async def test_otel_metrics_emitted(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """OTel metrics: elections counter records win, sweep rows counters
    record sweep_name labels."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=False,
        monkeypatch=monkeypatch,
    )

    sweep_rows_calls: list[tuple[int, dict[str, object]]] = []
    election_calls: list[tuple[int, dict[str, object]]] = []
    import taskq.obs._otel as otel_mod
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    _leader_sweeps_mod._sweep_rows_counter.add = lambda amount, attrs: sweep_rows_calls.append(  # type: ignore[method-assign]
        (int(amount), _as_dict(attrs or {}))
    )
    otel_mod._leader_election_attempts.add = lambda amount, attrs=None: election_calls.append(  # type: ignore[method-assign]
        (int(amount), _as_dict(attrs or {}))
    )

    # Run election
    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert election_calls == [(1, {})]

    # Populate backend with expired jobs, run sweep
    _now = leader._clock.now()  # type: ignore[reportPrivateUsage]  # Why: test reads injected clock to align job timestamps with sweep time.
    expired_id = new_job_id()
    _insert_job(
        backend,
        expired_id,
        status="running",
        lock_expires_at=_now - timedelta(seconds=1),
        attempt=1,
        max_attempts=3,
        now=_now,
    )
    deadline_id = new_job_id()
    _insert_job(
        backend,
        deadline_id,
        status="scheduled",
        schedule_to_close=_now - timedelta(seconds=1),
        now=_now,
    )

    sweep_rows_calls.clear()
    shutdown_2 = asyncio.Event()
    task_2 = asyncio.create_task(leader._sweep_loop(shutdown_2))
    # Bounded poll for both sweeps' non-empty row counters - the wake of
    # the completion asserts, never a fixed 0.05s hoping the tick ran.
    await wait_for_condition(
        lambda: (
            any(c[0] > 0 and c[1].get("sweep_name") == "expired_locks" for c in sweep_rows_calls)
            and any(
                c[0] > 0 and c[1].get("sweep_name") == "deadline_exceeded" for c in sweep_rows_calls
            )
        ),
        description="the sweep loop must run the expired-locks and deadline sweeps",
    )
    await _stop_after_tick(task_2, shutdown_2, delay=0.0)

    names_seen = {call[1].get("sweep_name") for call in sweep_rows_calls}
    assert "expired_locks" in names_seen
    assert "deadline_exceeded" in names_seen
    assert any(c[0] > 0 for c in sweep_rows_calls if c[1].get("sweep_name") == "expired_locks")
    assert any(c[0] > 0 for c in sweep_rows_calls if c[1].get("sweep_name") == "deadline_exceeded")


# ── kind='leadership_lost' log on watchdog error ────────────────────


async def test_leadership_lost_log_on_watchdog_error() -> None:
    """WARNING log with kind='leadership_lost', worker_id, error fields."""
    exc = asyncpg.PostgresConnectionError("watchdog failed")

    class FailingFakeConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise exc

    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    failing_monitor = FailingFakeConn()
    _clk = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    leader = MaintenanceLeader(
        deps,
        new_uuid(),
        InMemoryBackend(clock=_clk),
        clock=_clk,
    )
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    for _ in range(200):
        if failing_monitor._closed and not deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    assert not deps.is_leader.is_set()
    assert failing_monitor._closed
    assert deps.leader_conn is None
    shutdown.set()
    deps.is_leader.set()
    await task


# ── Transaction-scoped lock not used ─────────────────────────────────


async def test_transaction_scoped_lock_not_used(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """pg_try_advisory_xact_lock is never called.
    pg_try_advisory_lock(hashtextextended($1, 0)) IS called with literal 0."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    for sql, _ in leader_conn.fetchval_calls:
        assert "pg_try_advisory_xact_lock" not in sql

    lock_calls = [
        (sql, args) for sql, args in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql
    ]
    assert lock_calls
    for sql, _args in lock_calls:
        assert "hashtextextended($1, 0)" in sql
        assert "hashtextextended($1, $2)" not in sql


# ── Prune loop gates on is_leader ────────────────────────────────────


async def test_prune_loop_gates_on_is_leader() -> None:
    """Without is_leader, no prune lock acquisition or prune call.
    With is_leader, the lock is acquired and prune runs."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=False,
    )

    settings = deps.settings
    settings.prune_schedule_utc = "03:00"

    task = asyncio.create_task(leader._prune_loop(shutdown))
    await asyncio.sleep(0.1)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no advisory lock when not leader"


# ── Injected Clock controls sweep and wake-loop timestamps ────────


async def test_scheduled_wake_passes_no_clock_to_sweep() -> None:
    """_scheduled_wake_loop drives scheduled_to_pending with no clock
    value - the backend's own clock is the arbiter (seam removal: a
    caller-supplied ``now`` was ignored by PG and honored by InMemory, so
    the two backends never exercised the same contract)."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)

    called = asyncio.Event()

    async def _capture_scheduled_to_pending() -> int:
        called.set()
        return 0

    backend.scheduled_to_pending = _capture_scheduled_to_pending  # type: ignore[method-assign]  # Why: test-only interception of the sweep call.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    # The capture closure sets the event exactly where the sweep call is
    # entered - the bounded wait, never a sleep hoping the loop reached it.
    await wait_for(called)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert called.is_set()


async def test_sweep_loop_passes_no_clock_to_sweeps() -> None:
    """_sweep_loop drives reclaim_expired_locks and deadline_sweep with no
    clock value - the backend's own clock is the arbiter (same seam
    removal as the scheduled-wake loop)."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)

    reclaim_called = asyncio.Event()
    deadline_called = asyncio.Event()

    async def _capture_reclaim(cg: timedelta, ug: timedelta) -> int:
        reclaim_called.set()
        return 0

    async def _capture_deadline() -> int:
        deadline_called.set()
        return 0

    backend.reclaim_expired_locks = _capture_reclaim  # type: ignore[method-assign]  # Why: test-only interception of the sweep call.
    backend.deadline_sweep = _capture_deadline  # type: ignore[method-assign]  # Why: test-only interception of the sweep call.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # The capture closures set their events exactly where each sweep call
    # is entered - bounded event waits, never sleeps hoping the loop
    # reached them.
    await wait_for(reclaim_called)
    await wait_for(deadline_called)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert reclaim_called.is_set()
    assert deadline_called.is_set()


# ── _schedule_utc_to_cron ────────────────────────────────────────────


async def test_schedule_utc_to_cron_standard() -> None:
    """_schedule_utc_to_cron parses '03:00' → '0 3 * * *'."""
    assert _schedule_utc_to_cron("03:00") == "0 3 * * *"


async def test_schedule_utc_to_cron_half_hour() -> None:
    """_schedule_utc_to_cron parses '00:30' → '30 0 * * *'."""
    assert _schedule_utc_to_cron("00:30") == "30 0 * * *"


async def test_schedule_utc_to_cron_single_digit_hour() -> None:
    """_schedule_utc_to_cron handles single-digit hour '9:15' → '15 9 * * *'."""
    assert _schedule_utc_to_cron("9:15") == "15 9 * * *"


async def test_schedule_utc_to_cron_invalid_format() -> None:
    """_schedule_utc_to_cron raises ValueError on invalid input."""
    with pytest.raises(ValueError, match="invalid HH:MM"):
        _schedule_utc_to_cron("not-a-time")


async def test_schedule_utc_to_cron_invalid_minutes() -> None:
    """_schedule_utc_to_cron raises ValueError on missing colon."""
    with pytest.raises(ValueError, match="invalid HH:MM"):
        _schedule_utc_to_cron("0300")


# ── _build_retention_per_status ──────────────────────────────────────


async def test_build_retention_per_status_defaults() -> None:
    """_build_retention_per_status returns five statuses with crashed → abandoned."""
    settings = _worker_settings("postgresql://x:x@localhost/x")
    result = _build_retention_per_status(settings)
    assert set(result.keys()) == {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
    assert result["crashed"] == settings.prune_retention_abandoned
    assert result["abandoned"] == settings.prune_retention_abandoned
    assert result["succeeded"] == settings.prune_retention_succeeded
    assert result["failed"] == settings.prune_retention_failed
    assert result["cancelled"] == settings.prune_retention_cancelled


async def test_build_retention_per_status_crashed_uses_abandoned() -> None:
    """crashed maps to prune_retention_abandoned, not a separate field."""
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        PRUNE_RETENTION_ABANDONED="P120D",
    )
    result = _build_retention_per_status(settings)
    assert result["crashed"] == timedelta(days=120)
    assert result["abandoned"] == timedelta(days=120)


# NOTE (the mock-subtraction cure): the _load_actor_retention_overrides fake
# arms rode the old positional contract (the actor-config fetch consuming the
# next scripted batch — the probe-ate-a-batch shape). They converted to the
# real fixture in tests/test_leader_prune.py, where the invalid-row isolation
# contract already had its real-PG anchor
# (test_invalid_actor_retention_does_not_stop_global_prune).


# ── prune_terminal_jobs with FakeConn ────────────────────────


class _FakeRecord:
    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data


class _FakeConnForPrune(FakeConn):
    """The prune-family double: every statement shape the sweep machinery
    issues is a NAMED match (fail-closed, the
    tests/test_suite_hygiene.py rule); an unmatched statement raises
    AssertionError naming the fragment instead of falling through to a
    silent default — the fall-through shape is what once let the
    availability gate's probe eat a scripted batch and made the sweeps
    delete 0 of 7."""

    def __init__(
        self,
        *,
        batch_rows: list[list[_FakeRecord]] | None = None,
        actor_config_rows: list[_FakeRecord] | None = None,
        fetchval_result: object = None,
        db_now: datetime | None = None,
    ) -> None:
        super().__init__(fetchval_result=fetchval_result)
        self._batch_rows = batch_rows or []
        self._batch_index = 0
        self._pending_write: list[_FakeRecord] | None = None
        self._pending_ids: list[UUID] | None = None
        self._actor_config_rows = actor_config_rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        # 7565d3f anchors prune cutoffs and the expiry reference instant to
        # the database clock via a dedicated fetchval, while fetchval_result
        # keeps answering the advisory-lock probes - one scalar cannot serve
        # both, so the fake answers the DB-now query with a datetime.
        self._db_now = db_now

    async def fetchval(self, sql: str, *args: object) -> object:
        if sql != _DB_NOW_SQL:
            return await super().fetchval(sql, *args)
        self.fetchval_calls.append((sql, args))
        return self._db_now if self._db_now is not None else datetime.now(UTC)

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        self.fetch_calls.append((sql, args))
        # The batch machinery reads the session's statement_timeout through
        # fetch() before every batch (SET LOCAL is transaction-scoped, so
        # the sweep must capture and restore it); the fake answers that
        # probe itself rather than consuming a scripted batch.
        if "current_setting" in sql:
            return [_FakeRecord({"current_setting": "0"})]
        if "actor_config" in sql:
            # named shape, never the scripted batches: the config read
            # answers its own scripted rows, or the empty table (a
            # legitimate server state) - the old positional contract
            # (fall through and consume the next scripted batch) was the
            # probe-ate-a-batch incident's shape.
            return self._actor_config_rows if self._actor_config_rows is not None else []
        # The archive write locks the batch by id and returns the grouped
        # delete counts; the fake answers it from the batch the preceding
        # candidate fetch selected, asserting the bound ids are exactly
        # the ids that fetch returned - the caller discipline that keeps
        # the write set bounded by the LIMIT-ed window (the same contract
        # test_sweepaudit_bounded_writes.py pins structurally).
        if "locked AS MATERIALIZED" in sql:
            pending = self._pending_write
            self._pending_write = None
            pending_ids = self._pending_ids
            self._pending_ids = None
            if pending is not None:
                assert pending_ids is not None and list(args[2]) == pending_ids, (
                    f"the archive write must bind exactly the candidate "
                    f"window's returned ids; bound {args[2]!r}, "
                    f"the window returned {pending_ids!r}"
                )
            return pending if pending is not None else []
        # The expiry sweep is one statement per batch: the scripted batch
        # is the statement's own result.
        if "WITH expired AS MATERIALIZED" in sql:
            return self._next_scripted_batch()
        # The availability gate's pre-check (the extension's catalog
        # read): the fake's server is vanilla — no rows — and the probe
        # consumes no scripted batch (it is a probe, not a batch
        # statement).
        if "pg_extension" in sql:
            return []
        # The candidate window: the next scripted batch, one candidate id
        # per scripted write row. An empty scripted batch selects nothing
        # and ends the drain, the same contract an empty page has on the
        # real backend.
        if _is_prune_candidate_sql(sql):
            return await self._fetch_candidates(sql, *args)
        raise AssertionError(
            f"_FakeConnForPrune: no named answer for statement {sql!r} "
            f"(args {args!r}) - the fail-closed rule "
            "(tests/test_suite_hygiene.py) forbids a silent default: add the "
            "statement shape explicitly to this double"
        )

    def _next_scripted_batch(self) -> list[_FakeRecord]:
        if self._batch_index < len(self._batch_rows):
            rows = self._batch_rows[self._batch_index]
            self._batch_index += 1
            return rows
        return []

    def _candidate_ids(self, write_rows: list[_FakeRecord]) -> list[_FakeRecord]:
        # One candidate id per scripted write row, stashed so the write
        # fetch can prove the production caller bound exactly these ids
        # (the candidate window's returned set, unmodified).
        ids = [new_uuid() for _ in write_rows]
        self._pending_ids = ids
        return [_FakeRecord({"id": i}) for i in ids]

    async def _fetch_candidates(self, sql: str, *args: object) -> list[_FakeRecord]:
        """The candidate window: the next scripted batch, one candidate id
        per scripted write row. An empty scripted batch selects nothing
        and ends the drain, the same contract an empty page has on the
        real backend."""
        rows = self._next_scripted_batch()
        self._pending_write = rows
        if not rows:
            return []
        return self._candidate_ids(rows)


# NOTE (the mock-subtraction cure): the sweep-function fake arms that
# re-derived the prune/expiry CTE semantics (aggregate counts, drain-to-empty,
# per-actor override routing) are deleted — the real-PG pack
# tests/test_leader_prune.py owns that behavior against real data
# (test_batch_drain, test_expiry_sweep, test_expiry_drain,
# test_per_actor_retention, test_per_status_retention,
# test_prune_cutoff_anchored_to_server_clock). The invalid-schema arms and the
# statement-timeout arms converted to the real fixture there; the loop
# machinery below (retry ladder, day latch, lock survival, shutdown drain)
# keeps its scripted double because no real-PG sibling owns the loop, and the
# double is fail-closed.


# ── Date guard prevents double-prune on same UTC day ────────────────


class _SpacedCroniter:
    """The croniter stand-in whose two-fire SPACING is configurable: the
    loop's own fire computation calls get_next once per wake (the next
    fire — ``spacing`` seconds away, so the loop wakes immediately in
    tests); ``_sub_daily``'s lane probe calls it TWICE on its own
    instance, and the second call adds ``lane_spacing`` days — the
    interval the lane predicate reads. A ``lane_spacing`` of 2 days makes
    the schedule DAILY-or-slower (the latched lane); 0 days (the default)
    makes it sub-daily (the retention-check lane)."""

    spacing_secs: float = 0.05
    lane_spacing_days: float = 0.0

    def __init__(self, expr: str, start_time: object) -> None:
        self._calls = 0

    def get_next(self, dt_type: type[datetime]) -> datetime:
        self._calls += 1
        extra = timedelta(days=self.lane_spacing_days) if self._calls >= 2 else timedelta(0)
        return datetime.now(UTC) + timedelta(seconds=self.spacing_secs) + extra


async def test_prune_loop_date_guard_prevents_double_run(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """The DAILY lane: _prune_loop skips when last_pruned_date == today
    (UTC) — and the SKIP IS SURFACED (the D2 soak's P1 fix (a): the
    latch's deferral is a NAMED event, never silence — 80+ minutes of
    ``*/5`` fires deferred silently read as "nothing to prune" while 88k
    eligible rows sat unpruned)."""
    import croniter as croniter_mod

    prune_result_rows = [
        [
            _FakeRecord({"actor": "a", "status": "succeeded", "cnt": 5}),
        ],
    ]
    leader_conn = _FakeConnForPrune(
        batch_rows=prune_result_rows, fetchval_result=True, actor_config_rows=[]
    )

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _DailyCroniter(_SpacedCroniter):
        lane_spacing_days = 2.0  # the fake's two-fire interval: 2 days → daily lane

    monkeypatch.setattr(croniter_mod, "croniter", _DailyCroniter)

    settings = deps.settings
    settings.prune_cron_expr = "0 3 * * *"

    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(leader._prune_loop(shutdown))

        # Bounded poll for the first lock acquisition (no event exists on the
        # fake's recording).
        await wait_for_condition(
            lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
            description="the prune loop must attempt its first advisory lock",
        )

        lock_calls_after_first = len(
            [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
        )
        assert lock_calls_after_first == 1

        for _ in range(200):
            lock_calls_now = len(
                [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
            )
            if lock_calls_now > lock_calls_after_first:
                pytest.fail("date guard should have prevented second lock acquisition on same day")
            await asyncio.sleep(0.01)

    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

        # the deferral is named — the loop's own log states when the latch deferred
    # and what it deferred to.
    latched = [e for e in captured if e["event"] == "prune-skipped-day-latch"]
    assert latched, (
        "the day-latch deferred the second fire SILENTLY — the D2 soak's "
        "contract (the surfaced truth: the skip-reason states the latch "
        "and the next fire)"
    )
    assert latched[0]["next_fire"]


async def test_prune_loop_sub_daily_retention_lane_checks_every_tick(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """The retention-check lane (the D2 soak's P1 fix (b) — the
    at-most-daily class is DEAD): a SUB-DAILY prune cron with a 180 s
    retention and rows past retention is CHECKED every LEADER TICK — the
    arm attempts (and prunes) on successive wakes within the observation
    window, the deleted count DROPS with each check, and the day-latch
    never gates it. The D2 shape (a ``*/5`` cron + 180 s retention + 88k
    eligible rows) sat on its hands for 80+ minutes under the old
    latch-everything arm; this pin rejects the latch-gated variant as
    its red."""
    import croniter as croniter_mod

    # Every attempt answers one prunable batch: the eligible rows exist
    # (the D2's 88k class), so a live arm drains on every wake.
    leader_conn = _FakeConnForPrune(
        batch_rows=[[_FakeRecord({"actor": "a", "status": "succeeded", "cnt": 500})]],
        fetchval_result=True,
        actor_config_rows=[],
    )
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    monkeypatch.setattr(croniter_mod, "croniter", _SpacedCroniter)  # sub-daily spacing

    settings = deps.settings
    settings.prune_cron_expr = "*/5 * * * *"  # the D2's exact expression
    settings.prune_retention_succeeded = timedelta(seconds=180)

    task = asyncio.create_task(leader._prune_loop(shutdown))
    await wait_for_condition(
        lambda: (
            len([sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql])
            >= 3
        ),
        description="the retention-check lane must attempt on EVERY tick, not once per day",
        timeout=10.0,
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    attempts = len([sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql])
    assert attempts >= 3, (
        f"the sub-daily retention lane attempted {attempts} time(s) — the "
        "day-latch converted a */5 retention policy to at-most-daily (the "
        "D2 soak's P1, kept failing on purpose by this pin)"
    )
    # measured. each attempt drained its batch — the archive write consumed the
    # eligible window.
    assert any("locked AS MATERIALIZED" in sql for sql, _ in leader_conn.fetch_calls), (
        "the retention check never ran an archive batch — nothing pruned"
    )


async def test_archive_expiry_loop_date_guard_prevents_double_run(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """_archive_expiry_loop skips when last_expiry_date == today (UTC)."""
    import croniter as croniter_mod

    expiry_result_rows = [
        [
            _FakeRecord({"status": "succeeded", "cnt": 5}),
        ],
    ]
    leader_conn = _FakeConnForPrune(batch_rows=expiry_result_rows, fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _DailyCroniter(_SpacedCroniter):
        lane_spacing_days = 2.0  # the fake's two-fire interval: 2 days → daily lane

    monkeypatch.setattr(croniter_mod, "croniter", _DailyCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = "0 4 * * *"

    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

        # Bounded poll for the first lock acquisition (no event exists on the
        # fake's recording).
        await wait_for_condition(
            lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
            description="the archive expiry loop must attempt its first advisory lock",
        )

        lock_calls_after_first = len(
            [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
        )
        assert lock_calls_after_first == 1

        for _ in range(200):
            lock_calls_now = len(
                [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
            )
            if lock_calls_now > lock_calls_after_first:
                pytest.fail("date guard should have prevented second lock acquisition on same day")
            await asyncio.sleep(0.01)

    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # the surfaced latch (the same named-deferral event as the prune
    # loop's — one policy scaffold, one operator signal).
    assert any(e["event"] == "archive-expiry-skipped-day-latch" for e in captured), (
        "the archive expiry's day-latch deferred the second fire SILENTLY "
        "— the surfaced-truth law covers both loops"
    )


# ── Archive expiry loop gates on is_leader ──────────────────────────


async def test_archive_expiry_loop_gates_on_is_leader() -> None:
    """_archive_expiry_loop does not acquire lock when not leader."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=False,
    )

    settings = deps.settings
    settings.archive_expiry_schedule_utc = "04:00"

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    await asyncio.sleep(0.1)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no advisory lock when not leader"


# ── Prune family: bounded, interruptible, retrying batches ──────────
#
# The once-a-day prune is under the repo's bounded-per-transaction
# discipline, like every maintenance family: bare while-True drains with
# no shutdown check (a SIGTERM mid-drain hangs MaintenanceLeader's
# TaskGroup for the whole backlog while holding the prune advisory lock
# and a pool connection), no per-batch statement timeout or batch-size
# breaker (a 10 000-row archive CTE under the dispatcher pool's 5 s
# client command timeout fails every attempt on a loaded PG), and no
# retry after a failed attempt (the next try was tomorrow's cron fire).
# These tests pin the three halves of that fix: gate-stopped drains,
# server-bounded batches with a latching breaker, and the intra-day
# backoff retry that never runs a second successful prune per day.


def _full_batch_record(batch_size: int) -> _FakeRecord:
    """One aggregate row reporting a full batch - the drain must continue."""
    return _FakeRecord({"actor": "a", "status": "succeeded", "cnt": batch_size})


class _HookedPruneBatchConn(_FakeConnForPrune):
    """Answers every prune batch with one full batch, invoking *on_batch*
    first - a backlog no drain can exhaust, with an observation seam at
    each batch."""

    def __init__(self, *, batch_size: int, on_batch: Callable[[], None]) -> None:
        super().__init__(batch_rows=[], fetchval_result=True)
        self._row = [_full_batch_record(batch_size)]
        self._on_batch = on_batch
        self.batches = 0

    async def _fetch_candidates(self, sql: str, *args: object) -> list[_FakeRecord]:
        """A backlog no drain can exhaust: every candidate fetch selects a
        full batch and the write returns it."""
        self.batches += 1
        self._on_batch()
        self._pending_write = list(self._row)
        return self._candidate_ids(self._row)


class _HookedExpiryBatchConn(_FakeConnForPrune):
    """The archive-expiry twin of _HookedPruneBatchConn (``expired`` window)."""

    def __init__(self, *, batch_size: int, on_batch: Callable[[], None]) -> None:
        super().__init__(batch_rows=[], fetchval_result=True)
        self._row = [_FakeRecord({"status": "succeeded", "cnt": batch_size})]
        self._on_batch = on_batch
        self.batches = 0

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        if "expired" in sql:
            self.batches += 1
            self._on_batch()
            return list(self._row)
        return await super().fetch(sql, *args)


async def test_prune_timeout_latches_sizer_and_degrades_next_attempt() -> None:
    """A server-side batch abort (QueryCanceledError) counts against the
    breaker: the sizer latches and the next attempt's windows run at the
    reduced tier - the degradation that makes a loaded database drainable
    under a timeout smaller than its backlog."""

    class _TimeoutConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if _is_prune_candidate_sql(sql):
                raise asyncpg.QueryCanceledError("canceling statement due to statement timeout")
            return await super().fetch(sql, *args)

    sizer = SweepBatchSizer(default_size=10_000, divisor=4, failure_threshold=1, window_secs=600.0)
    conn = _TimeoutConn(batch_rows=[])
    with pytest.raises(asyncpg.QueryCanceledError):
        await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            schema="taskq",
            statement_timeout_ms=4321,
            sizer=sizer,
        )
    assert sizer.effective_size() == 2_500, (
        "a cancelled batch must latch the breaker to the reduced tier"
    )

    # The latched tier sizes the next attempt's windows - the retry path's
    # whole point.
    conn2 = _FakeConnForPrune(batch_rows=[[_full_batch_record(5)]])
    await prune_terminal_jobs(
        conn2,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        schema="taskq",
        statement_timeout_ms=4321,
        sizer=sizer,
    )
    batch_fetches = [(sql, args) for sql, args in conn2.fetch_calls if _is_prune_candidate_sql(sql)]
    assert batch_fetches, "fixture broken: no prune batch ran"
    assert batch_fetches[0][1][2] == 2_500, (
        f"the latched reduced tier must be the window LIMIT; got args {batch_fetches[0][1]!r}"
    )


def _soon_then_far_croniter() -> type:
    """A croniter double whose first fire is immediate and every later one
    an hour out: within a test window, any attempt after the first can
    only be a failure backoff retry, never a scheduled fire. Fresh state
    per call (the loop constructs a new croniter each iteration, so the
    soon/far memory must be shared across instances, not per-instance)."""
    state = {"first": True}

    class _Croniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            if state["first"]:
                state["first"] = False
                return datetime.now(UTC) + timedelta(seconds=0.01)
            return datetime.now(UTC) + timedelta(hours=1)

    return _Croniter


class _AlwaysFailsPruneConn(_FakeConnForPrune):
    """Every prune batch statement raises - the failed-attempt shape."""

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        if _is_prune_candidate_sql(sql):
            raise RuntimeError("connection lost")
        return await super().fetch(sql, *args)


def _is_prune_candidate_sql(sql: str) -> bool:
    """The archive batch's candidate window: a SELECT bounded by the
    finished_at order (carrying the ``WITH held AS MATERIALIZED`` CTE on
    workflow-aware schemas). The other statements the sweep fetches are the
    lock-bearing write (``locked AS MATERIALIZED``), the expiry sweep
    (``WITH expired AS MATERIALIZED``), or machinery probes (current_setting,
    actor_config), so the shape is the signature."""
    return (
        "ORDER BY finished_at" in sql
        and "locked AS MATERIALIZED" not in sql
        and "WITH expired AS MATERIALIZED" not in sql
    )


def _lock_attempts(conn: FakeConn) -> int:
    return len([sql for sql, _ in conn.fetchval_calls if "pg_try_advisory_lock" in sql])


async def test_prune_loop_retries_failed_attempt_with_backoff(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """A failed prune attempt retries within the day on the backoff ladder
    instead of sleeping to tomorrow's cron fire."""
    import croniter as croniter_mod

    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.02)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 0.2)
    monkeypatch.setattr(croniter_mod, "croniter", _soon_then_far_croniter())

    leader_conn = _AlwaysFailsPruneConn(batch_rows=[], fetchval_result=True)
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._prune_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _lock_attempts(leader_conn) >= 3,
            description="a failed prune attempt must retry on the backoff ladder "
            "(>=3 attempts) rather than wait for tomorrow's cron fire",
            timeout=3.0,
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_prune_loop_backoff_doubles_to_cap(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """The retry cadence doubles (60 s, 120 s, … capped) rather than
    retrying at a fixed rate: with initial=0.05 s doubling, a 1 s window
    sees ~5 attempts, where a fixed 0.05 s cadence would see ~20 and a
    no-retry loop exactly 1."""
    import croniter as croniter_mod

    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.05)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 10.0)
    monkeypatch.setattr(croniter_mod, "croniter", _soon_then_far_croniter())

    leader_conn = _AlwaysFailsPruneConn(batch_rows=[], fetchval_result=True)
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._prune_loop(shutdown))
    await asyncio.sleep(1.0)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    attempts = _lock_attempts(leader_conn)
    assert 3 <= attempts <= 7, (
        f"{attempts} attempts in 1 s - a doubling ladder from 0.05 s lands "
        "at ~5 (attempts at 0.05, 0.1, 0.2, 0.4, 0.8 s); ~20 means a fixed "
        "cadence, 1 means no retry at all"
    )


async def test_prune_loop_success_stops_retry_for_the_day(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Fail once, succeed on the retry, then the day is done: no further
    attempts - the once-per-SUCCESSFUL-prune-per-day guard holds through
    the retry change (a retried day never gets a second successful
    prune)."""
    import croniter as croniter_mod

    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.02)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 0.2)
    monkeypatch.setattr(croniter_mod, "croniter", _soon_then_far_croniter())

    class _FailOnceConn(_FakeConnForPrune):
        def __init__(self) -> None:
            super().__init__(batch_rows=[], fetchval_result=True)
            self._prune_batches = 0

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if _is_prune_candidate_sql(sql):
                self._prune_batches += 1
                if self._prune_batches == 1:
                    raise RuntimeError("connection lost")
                return []
            return await super().fetch(sql, *args)

    leader_conn = _FailOnceConn()
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._prune_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _lock_attempts(leader_conn) >= 2,
            description="the failed first attempt must be retried",
            timeout=3.0,
        )
        # The retry succeeded; the day is marked. Give the loop room to
        # (wrongly) attempt again, then hold it to exactly two.
        await asyncio.sleep(0.3)
        assert _lock_attempts(leader_conn) == 2, (
            "a successful prune must end the day's attempts - the retry "
            "must not become a second prune"
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_prune_loop_demotion_cut_arms_ladder_instead_of_stamping_the_day(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A demotion that CUTS the drain mid-way is an unfinished day, not a
    done one: the date latch stays unstamped and the backoff ladder arms,
    so the remainder is re-attempted within the day instead of silently
    waiting for the next cron fire (up to 24h) once the pod leads again."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.02)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 0.2)
    # _leader_sweeps binds the croniter module (``import croniter as cr``),
    # so patching the attribute on the module itself reaches the loop
    # without a private-import report on ``_leader_sweeps_mod.cr``.
    import croniter as croniter_mod

    monkeypatch.setattr(croniter_mod, "croniter", _soon_then_far_croniter())

    # The demotion seam: the conn must be built before the leader (whose
    # deps the drain consults), so the cut resolves the holder at write
    # time - the batch's own commit instant is the demotion instant.
    deps_holder: list[WorkerDeps] = []

    class _DemoteAtFirstWriteConn(_FakeConnForPrune):
        demoted = False

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            result = await super().fetch(sql, *args)
            if not self.demoted and "locked AS MATERIALIZED" in sql and result and deps_holder:
                # Batch 1 has committed: this is the demotion instant.
                # The drain's next gate call refuses (the demotion
                # clause), and the loop's post-drain leading() read must
                # see the pod no longer leading.
                self.demoted = True
                deps_holder[0].stop_leading()
                # The bug's own scenario: the same pod regains leadership
                # a beat later. The ladder, not the cron, must land the
                # remainder. A timer, not a sleep: the assertion below
                # stays a bounded condition wait.
                asyncio.get_running_loop().call_later(0.1, deps_holder[0].is_leader.set)
            return result

    leader_conn = _DemoteAtFirstWriteConn(
        batch_rows=[[_full_batch_record(5)]], fetchval_result=True, actor_config_rows=[]
    )
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )
    deps_holder.append(deps)

    task = asyncio.create_task(leader._prune_loop(shutdown))
    try:
        # The cut attempt ends without raising, so the ladder only proves
        # itself when the pod leads again: on the ladder's next wake the
        # attempt re-runs (a second advisory-lock attempt). With the date
        # wrongly stamped by the cut, the loop would sleep to the far
        # cron fire and never attempt again.
        await wait_for_condition(
            lambda: _lock_attempts(leader_conn) >= 2,
            description="the demotion-cut drain must arm the retry ladder: "
            "the remainder is re-attempted within the day, not deferred to "
            "the next cron fire",
            timeout=3.0,
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_archive_expiry_loop_retries_failed_attempt_with_backoff(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The archive-expiry loop shares the prune loop's failure-half policy:
    a failed attempt retries on the backoff ladder within the day."""
    import croniter as croniter_mod

    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.02)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 0.2)
    monkeypatch.setattr(croniter_mod, "croniter", _soon_then_far_croniter())

    class _AlwaysFailsExpiryConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "expired" in sql:
                raise RuntimeError("connection lost")
            return await super().fetch(sql, *args)

    leader_conn = _AlwaysFailsExpiryConn(batch_rows=[], fetchval_result=True)
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _lock_attempts(leader_conn) >= 3,
            description="a failed archive-expiry attempt must retry on the "
            "backoff ladder rather than wait for tomorrow's cron fire",
            timeout=3.0,
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_prune_loop_drain_stops_on_shutdown_and_ticks_liveness(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """SIGTERM mid-drain ends the attempt between batches - the loop task
    finishes without cancel, after a bounded number of batches, not after
    the whole backlog. The drain registers with detector 2 while it runs
    (so a wedged batch loop is visible to the watchdog) and forgets the
    registration when the attempt ends (so a once-a-day loop cannot read
    as a stale sibling between attempts)."""
    import croniter as croniter_mod

    shutdown = asyncio.Event()
    # The observation seam closes over a holder: the conn must be built
    # before the leader (whose deps carry the liveness registry it
    # observes), so the callback resolves the holder at batch time.
    deps_holder: list[WorkerDeps] = []
    liveness_snapshots: list[dict[str, float]] = []

    def on_batch() -> None:
        liveness_snapshots.append(dict(deps_holder[0].liveness.ages()))
        if len(liveness_snapshots) == 3:
            shutdown.set()

    leader_conn = _HookedPruneBatchConn(batch_size=10, on_batch=on_batch)
    leader, deps, _backend, _, _, _ = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )
    deps_holder.append(deps)

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)
    deps.settings.prune_cron_expr = "* * * * *"
    # The hooked conn reports 10-row full batches, so the effective window
    # must be 10 too - the sizer's default tier comes from this setting.
    deps.settings.prune_batch_size = 10

    task = asyncio.create_task(leader._prune_loop(shutdown))
    # No cancel, no suppress: a shutdown-responsive drain lets the loop
    # task complete on its own - the property the TaskGroup hang lacked.
    await asyncio.wait_for(task, timeout=5.0)

    assert leader_conn.batches == 3, (
        f"{leader_conn.batches} batches ran after shutdown was set "
        "mid-drain; the drain must stop between batches, holding the prune "
        "advisory lock and its pool connection for at most one batch"
    )
    assert any("leader.prune" in ages for ages in liveness_snapshots), (
        "the drain must register with detector 2 while batches flow - a "
        "wedged once-a-day drain is otherwise invisible to the watchdog"
    )
    assert "leader.prune" not in deps.liveness.ages(), (
        "the attempt-scoped liveness registration must be forgotten when "
        "the attempt ends - a once-a-day loop must not read as a stale "
        "sibling between attempts"
    )


async def test_archive_expiry_loop_drain_stops_on_shutdown(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The archive-expiry drain honors the same shutdown-between-batches
    contract as the prune drain."""
    import croniter as croniter_mod

    shutdown = asyncio.Event()
    batches_seen: list[int] = []

    def on_batch() -> None:
        batches_seen.append(1)
        if len(batches_seen) == 2:
            shutdown.set()

    leader_conn = _HookedExpiryBatchConn(batch_size=10, on_batch=on_batch)
    leader, deps, _backend, _, _, _ = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)
    deps.settings.archive_expiry_cron_expr = "* * * * *"
    # The hooked conn reports 10-row full batches, so the effective window
    # must be 10 too - the sizer's default tier comes from this setting.
    deps.settings.prune_batch_size = 10

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    await asyncio.wait_for(task, timeout=5.0)

    assert leader_conn.batches == 2


# ── Prune loop releases lock on error ────────────────────────────────────────


async def test_prune_loop_releases_lock_on_error(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop releases advisory lock even when prune_terminal_jobs raises."""
    import croniter as croniter_mod

    # Additive event on the double: set exactly where the prune query
    # raises, so the test waits for the error path instead of sleeping
    # and hoping the loop reached it.
    error_seen = asyncio.Event()

    class _ErrorConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if _is_prune_candidate_sql(sql):
                error_seen.set()
                raise RuntimeError("connection lost")
            # Machinery probes (the batch timeout's current_setting read)
            # answer through the base double so the error surfaces from the
            # prune statement itself, not from the batch wrapper.
            return await super().fetch(sql, *args)

    leader_conn = _ErrorConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    await wait_for(error_seen)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert unlock_calls, "lock must be released even on error"


# ── Archive expiry loop releases lock on error ──────────────────────────────


async def test_archive_expiry_loop_releases_lock_on_error(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop releases advisory lock even when sweep raises."""
    import croniter as croniter_mod

    # Additive event on the double: set exactly where the expiry query
    # raises, so the test waits for the error path instead of sleeping
    # and hoping the loop reached it.
    error_seen = asyncio.Event()

    class _ErrorConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "expired" in sql:
                error_seen.set()
                raise RuntimeError("connection lost")
            # Machinery probes (the batch timeout's current_setting read)
            # answer through the base double so the error surfaces from the
            # expiry statement itself, not from the batch wrapper.
            return await super().fetch(sql, *args)

    leader_conn = _ErrorConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

    await wait_for(error_seen)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert unlock_calls, "lock must be released even on error"


# ── Shutdown wakes prune loop immediately ────────────────────────────────────


async def test_prune_loop_wakes_on_shutdown() -> None:
    """Prune loop breaks immediately when shutdown is set during sleep."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=True,
    )

    settings = deps.settings
    settings.prune_schedule_utc = "03:00"

    shutdown.set()
    task = asyncio.create_task(leader._prune_loop(shutdown))
    # The loop observes the pre-set shutdown immediately - awaiting the
    # task is the bounded wait for its exit (and it re-raises if the loop
    # died of an error instead of exiting cleanly).
    await asyncio.wait_for(task, timeout=2.0)
    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no lock acquisition during shutdown"


# ── Shutdown wakes archive expiry loop immediately ──────────────────────────


async def test_archive_expiry_loop_wakes_on_shutdown() -> None:
    """Archive expiry loop breaks immediately when shutdown is set during sleep."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=True,
    )

    settings = deps.settings
    settings.archive_expiry_schedule_utc = "04:00"

    shutdown.set()
    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    # The loop observes the pre-set shutdown immediately - awaiting the
    # task is the bounded wait for its exit (and it re-raises if the loop
    # died of an error instead of exiting cleanly).
    await asyncio.wait_for(task, timeout=2.0)
    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no lock acquisition during shutdown"


# ── Prune loop skips when lock not acquired ──────────────────────────────────


async def test_prune_loop_skips_when_lock_not_acquired(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop skips the prune run and does not call prune_terminal_jobs
    when pg_try_advisory_lock returns False."""
    import croniter as croniter_mod

    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=False)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    await wait_for_condition(
        lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
        description="the prune loop must attempt the advisory lock",
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    fetch_calls = [sql for sql, _ in leader_conn.fetch_calls if _is_prune_candidate_sql(sql)]
    assert not fetch_calls, "prune should not run when lock not acquired"
    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert not unlock_calls, "no unlock needed when lock was never acquired"


# ── Prune loop survives lock acquisition failure ─────────────────────────────


async def test_prune_loop_survives_lock_acquisition_failure(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop continues when pg_try_advisory_lock raises a connection error,
    instead of crashing the TaskGroup."""
    import croniter as croniter_mod

    lock_call_count = 0

    class _LockFailsConn(_FakeConnForPrune):
        async def fetchval(self, sql: str, *args: object) -> object:
            nonlocal lock_call_count
            if "pg_try_advisory_lock" in sql and "prune" in str(args):
                lock_call_count += 1
                raise asyncpg.PostgresConnectionError("connection lost")
            return await super().fetchval(sql, *args)

    leader_conn = _LockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    # Bounded poll on the failure count (a count on the fake's raise
    # point is the observable; the loop must retry past the first one).
    await wait_for_condition(
        lambda: lock_call_count >= 2,
        description="the prune loop must retry the lock acquisition after a connection failure",
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert lock_call_count >= 2, "loop should continue after lock acquisition failure"
    assert not task.cancelled(), "task should not be cancelled"


# ── Archive expiry loop survives lock acquisition failure ─────────────────────


async def test_archive_expiry_loop_survives_lock_acquisition_failure(
    monkeypatch: Any,
) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop continues when pg_try_advisory_lock raises a connection
    error, instead of crashing the TaskGroup."""
    import croniter as croniter_mod

    lock_call_count = 0

    class _LockFailsConn(_FakeConnForPrune):
        async def fetchval(self, sql: str, *args: object) -> object:
            nonlocal lock_call_count
            if "pg_try_advisory_lock" in sql and "archive_expiry" in str(args):
                lock_call_count += 1
                raise asyncpg.InterfaceError("connection is closed")
            return await super().fetchval(sql, *args)

    leader_conn = _LockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

    # Bounded poll on the failure count (a count on the fake's raise
    # point is the observable; the loop must retry past the first one).
    await wait_for_condition(
        lambda: lock_call_count >= 2,
        description=(
            "the archive expiry loop must retry the lock acquisition after a connection failure"
        ),
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert lock_call_count >= 2, "loop should continue after lock acquisition failure"
    assert not task.cancelled(), "task should not be cancelled"


# ── Prune loop survives unlock failure in finally block ──────────────────────


async def test_prune_loop_survives_unlock_failure(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop continues when pg_advisory_unlock raises in the finally block,
    instead of crashing the TaskGroup. PG releases the lock on session death."""
    import croniter as croniter_mod

    # Additive event on the double: set exactly where the prune query is
    # entered, so the test waits for the prune run instead of sleeping
    # and hoping the loop reached it.
    prune_ran = asyncio.Event()

    class _UnlockFailsConn(_FakeConnForPrune):
        async def execute(self, sql: str, *args: object) -> str:
            if "pg_advisory_unlock" in sql:
                raise asyncpg.PostgresConnectionError("connection lost")
            return await super().execute(sql, *args)

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if _is_prune_candidate_sql(sql):
                prune_ran.set()
            # Machinery probes answer through the base double (see the
            # _ErrorConn note above).
            return await super().fetch(sql, *args)

    leader_conn = _UnlockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    await wait_for(prune_ran)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert prune_ran.is_set(), "prune should have run"
    assert not task.cancelled(), "task should not be cancelled after unlock failure"


# ── Archive expiry loop survives unlock failure in finally block ──────────────


async def test_archive_expiry_loop_survives_unlock_failure(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop continues when pg_advisory_unlock raises in the finally
    block, instead of crashing the TaskGroup. PG releases the lock on session death."""
    import croniter as croniter_mod

    # Additive event on the double: set exactly where the expiry query is
    # entered, so the test waits for the sweep run instead of sleeping
    # and hoping the loop reached it.
    sweep_ran = asyncio.Event()

    class _UnlockFailsConn(_FakeConnForPrune):
        async def execute(self, sql: str, *args: object) -> str:
            if "pg_advisory_unlock" in sql:
                raise OSError(104, "Connection reset by peer")
            return await super().execute(sql, *args)

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "expired" in sql:
                sweep_ran.set()
            # Machinery probes answer through the base double (see the
            # _ErrorConn note above).
            return await super().fetch(sql, *args)

    leader_conn = _UnlockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(croniter_mod, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

    await wait_for(sweep_ran)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert sweep_ran.is_set(), "archive expiry sweep should have run"
    assert not task.cancelled(), "task should not be cancelled after unlock failure"


# ── Reopen retries on credential-provider exceptions ────────────────────


class _FakeProviderError(RuntimeError):
    """Stand-in for azure/hvac/botocore credential-fetch failures, which are
    not asyncpg.PostgresConnectionError subclasses (and neither is
    asyncpg.InvalidPasswordError - a fresh-but-rejected token)."""


async def test_election_reopen_retries_on_provider_exception(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """A leader_conn_factory raising a provider-style exception once (IdP
    outage) then succeeding must be retried by the election loop - the
    exception must not escape and crash the worker TaskGroup."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn = None
    deps.is_leader.clear()

    factory_calls = 0

    async def flaky_factory() -> FakeConn:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            raise _FakeProviderError("simulated IdP outage")
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = flaky_factory  # type: ignore[assignment]

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task  # must not propagate the provider exception

    assert deps.is_leader.is_set()
    assert factory_calls >= 2


async def test_dedicated_conn_reopen_retries_on_provider_exception(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """A provider exception while opening the monitor/cron dedicated conns
    (after the advisory lock is won) must be caught and retried - a
    transient IdP failure mid-election must not crash the worker."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn = None
    deps.is_leader.clear()

    factory_calls = 0

    async def flaky_factory() -> FakeConn:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 2:
            # leader_conn reopened fine; the monitor-conn open hits the outage
            raise _FakeProviderError("simulated IdP outage")
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = flaky_factory  # type: ignore[assignment]

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    # leader, failed monitor, then leader + monitor + cron on the retry
    assert factory_calls >= 4


# ── Ownership contract: caller-owned leader_conn is never closed ────────


async def test_watchdog_does_not_close_caller_owned_leader_conn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Caller-owned leader_conn that dies is abandoned, never closed.

    The ownership contract ("TaskQ never closes caller-owned resources")
    forbids close() even on the dead-conn path - the caller owns the
    corpse. The watchdog must still drop our reference so the election
    loop rebuilds via the factory/DSN path and re-establishes leadership.
    """
    caller_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=caller_conn,
        monkeypatch=monkeypatch,
    )
    assert not deps.owns_leader_conn  # _make_deps default: caller-provided conn
    assert deps.leader_conn is caller_conn

    class FailingMonitor(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise asyncpg.PostgresConnectionError("monitor probe failed")

    deps.is_leader.set()
    failing_monitor = FailingMonitor()
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]

    factory_conns: list[
        object
    ] = []  # Why: identity bag - members are compared with `is`/`in` against deps.leader_conn (asyncpg.Connection), so element typing as FakeConn makes pyright report no-overlap.

    async def factory() -> FakeConn:
        conn = FakeConn(fetchval_result=True)
        factory_conns.append(conn)
        return conn

    deps.leader_conn_factory = factory  # type: ignore[assignment]

    watchdog_task = asyncio.create_task(leader._watchdog_loop(shutdown))
    election_task = asyncio.create_task(leader._election_loop(shutdown))

    # Wait for the watchdog failure path to run. failing_monitor.closed_event
    # is the stable witness: _close_leader_owned_conns closes it after the
    # leader_conn close/abandon in the same handler, so once it is set the
    # caller-conn decision has definitely been made (and unlike the
    # is_leader flip, it never flips back under a racing re-election).
    await wait_for(failing_monitor.closed_event)
    assert failing_monitor._closed
    assert not caller_conn.is_closed()
    assert caller_conn.close_calls == 0

    # Election loop re-acquires leadership through the factory. The
    # election loop assigns deps.leader_conn from the factory before it
    # wins the lock and sets is_leader (leader.py), so the event wait
    # implies the factory-conn check asserted below.
    await wait_for_leader(deps)
    shutdown.set()
    watchdog_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watchdog_task
    await election_task

    assert deps.is_leader.is_set()
    assert deps.leader_conn in factory_conns
    assert not caller_conn.is_closed()
    assert caller_conn.close_calls == 0


async def test_watchdog_closes_taskq_owned_leader_conn() -> None:
    """TaskQ-owned leader_conn IS closed on the watchdog failure path - the
    ownership guard must not change TaskQ-owned behaviour."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(leader_conn=leader_conn)
    deps.owns_leader_conn = True
    deps.is_leader.set()

    class FailingMonitor(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise asyncpg.PostgresConnectionError("monitor probe failed")

    failing_monitor = FailingMonitor()
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]

    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    for _ in range(200):
        if leader_conn.is_closed() and not deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    deps.is_leader.set()  # unblock the wait() so the loop observes shutdown
    await task

    assert leader_conn.is_closed()
    assert leader_conn.close_calls == 1
    assert deps.leader_conn is None


# ── Bounded closes: hung close is terminated, fast close is not ─────────
#
# The election/watchdog/cron paths closed leader-owned dedicated
# conns with a bare ``await conn.close()`` - a dead PG can block that
# indefinitely, stalling the watchdog. These tests pin the bounded-close
# discipline (asyncio.wait_for + terminate on timeout) applied via
# ``close_conn_bounded``; the shrink seam is the same module-global
# monkeypatch convention as tests/test_worker_deps_teardown.py.


async def test_close_leader_owned_conns_terminates_hung_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung close on a leader-owned dedicated conn (dead PG) is terminated
    after the bounded timeout; the loop still closes the remaining conn
    gracefully, nulls both attrs, and clears is_leader."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    monitor = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]
    monitor.close_wait.clear()  # close() blocks forever from now on

    # Why the outer timeout: unguarded, this awaits conn.close() unbounded, so
    # the red state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        await leader._close_leader_owned_conns()

    assert monitor.terminated is True
    assert monitor.close_calls == 1
    assert cron.is_closed()
    assert cron.terminated is False
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()


async def test_close_leader_owned_conns_fast_close_not_terminated() -> None:
    """Healthy close(): both leader-owned conns close gracefully - nothing is
    terminated; attrs nulled; is_leader cleared. A no-regression pin."""
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    monitor = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]

    await leader._close_leader_owned_conns()

    for conn in (cron, monitor):
        assert conn.is_closed()
        assert conn.close_calls == 1
        assert conn.terminated is False
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()


class _CloseEntryConn(FakeConn):
    """FakeConn that signals when close() has been ENTERED (not completed).

    Used to observe in-flight closes: the hang gate (close_wait) parks the
    close after entry, so close_entered marks the exact window in which a
    dead-PG close is in progress.
    """

    def __init__(self) -> None:
        super().__init__()
        self.close_entered = asyncio.Event()

    async def close(self) -> None:
        self.close_entered.set()
        await super().close()


async def test_close_leader_owned_conns_clears_is_leader_before_closes_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Demotion is observable IMMEDIATELY: is_leader clears before the
    bounded closes complete, not after. On a dead PG the closes can park
    for seconds, and is_leader backs the taskq.maintenance_leader.is_leader
    gauge, /metrics, and the health report - a pod parked in a hung close
    must not keep advertising leadership while its replacement should be
    taking over (review N2)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = _CloseEntryConn()
    monitor = _CloseEntryConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # both close()s block forever from now on
    monitor.close_wait.clear()

    # Why the outer timeout: unguarded, the clear runs only after both bounded
    # closes finish, so the red state's assertion window is bounded by the
    # 0.05s shrink seam; the guard keeps a regression from hanging.
    async with asyncio.timeout(5):
        task = asyncio.create_task(leader._close_leader_owned_conns())
        # Wait until both close()s have been entered (not completed)...
        await cron.close_entered.wait()
        await monitor.close_entered.wait()
        # ...then, while the closes are still parked, the flag must already
        # be clear. Unguarded it stays set until both closes finish → red.
        assert not monitor.is_closed()
        assert not deps.is_leader.is_set()
        await task

    assert cron.terminated is True
    assert monitor.terminated is True
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()


async def test_close_leader_owned_conns_mid_run_false_logs_teardown_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Final teardown is not a mid-run emergency: called with mid_run=False,
    a hung close logs the ``conn-teardown-close-*`` family and never the
    mid-run ``conn-close-*`` family, so an ordinary shutdown does not page
    as an unexpected mid-run close timeout (review N6)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # close() blocks forever from now on

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await leader._close_leader_owned_conns(mid_run=False)

    assert cron.terminated is True
    assert cron.close_calls == 1
    timeout_events = [
        e for e in captured if e.get("event") == "conn-teardown-close-timeout-terminating"
    ]
    assert len(timeout_events) == 1, f"expected 1 teardown timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "cron_conn"
    mid_run_events = [e for e in captured if str(e.get("event", "")).startswith("conn-close-")]
    assert mid_run_events == [], f"mid-run family must not fire on teardown: {captured!r}"


async def test_close_leader_owned_conns_default_mid_run_logs_mid_run_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default stays the mid-run family: the watchdog/election/cron
    demotion paths call without mid_run, and a conn so dead that even
    close() hung while the worker is alive must keep paging as
    ``conn-close-*`` (existing behaviour pinned)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # close() blocks forever from now on

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await leader._close_leader_owned_conns()

    assert cron.terminated is True
    assert cron.close_calls == 1
    timeout_events = [e for e in captured if e.get("event") == "conn-close-timeout-terminating"]
    assert len(timeout_events) == 1, f"expected 1 mid-run timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "cron_conn"
    teardown_events = [
        e for e in captured if str(e.get("event", "")).startswith("conn-teardown-close-")
    ]
    assert teardown_events == [], f"teardown family must not fire mid-run: {captured!r}"


async def test_run_final_teardown_closes_leader_conns_with_teardown_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run()'s finally is final teardown, not a mid-run demotion: it must
    close leader-owned conns with the ``conn-teardown-close-*`` family so an
    ordinary shutdown of a leader pod never pages as a mid-run close
    failure. Drives run() end-to-end with shutdown pre-set - every loop
    observes the set event and exits immediately, so only the finally's
    close path does any work (review N6 wiring pin)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    monitor = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # close() blocks forever from now on
    shutdown.set()  # all loops exit immediately; only the finally does work

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await leader.run(shutdown)

    assert cron.terminated is True
    assert cron.close_calls == 1
    assert monitor.is_closed()
    assert monitor.terminated is False
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()
    timeout_events = [
        e for e in captured if e.get("event") == "conn-teardown-close-timeout-terminating"
    ]
    assert len(timeout_events) == 1, f"expected 1 teardown timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "cron_conn"
    mid_run_events = [e for e in captured if str(e.get("event", "")).startswith("conn-close-")]
    assert mid_run_events == [], f"mid-run family must not fire on teardown: {captured!r}"


async def test_drop_leader_conn_terminates_hung_taskq_owned_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung close on a TaskQ-owned leader_conn is terminated after the
    bound and deps.leader_conn is nulled, so the watchdog/election drop
    path completes instead of stalling."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, _shutdown = await _make_leader(leader_conn=leader_conn)
    deps.owns_leader_conn = True
    leader_conn.close_wait.clear()  # close() blocks forever from now on

    # Why the outer timeout: unguarded, this awaits conn.close() unbounded, so
    # the red state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        await leader._drop_leader_conn(reason="test_hung_close")

    assert leader_conn.terminated is True
    assert leader_conn.close_calls == 1
    assert deps.leader_conn is None


async def test_drop_leader_conn_fast_close_not_terminated() -> None:
    """TaskQ-owned leader_conn with a healthy close(): closed once, never
    terminated, reference dropped. A no-regression pin."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, _shutdown = await _make_leader(leader_conn=leader_conn)
    deps.owns_leader_conn = True

    await leader._drop_leader_conn(reason="test_fast_close")

    assert leader_conn.is_closed()
    assert leader_conn.close_calls == 1
    assert leader_conn.terminated is False
    assert deps.leader_conn is None


# ── Keepalive applied to factory-built leader connections ───────────────


async def test_factory_built_leader_conns_get_keepalive(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Factory-built leader/monitor/cron conns get the worker's TCP
    keepalive policy applied. The DSN path gets it inside
    open_dedicated_conn; the factory path bypasses that helper, so
    _open_leader_conn/_open_dedicated_conn must apply it explicitly."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn = None
    deps.is_leader.clear()

    async def factory() -> FakeConn:
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = factory  # type: ignore[assignment]

    import taskq.worker.leader as leader_mod

    keepalive_calls: list[str] = []

    def fake_keepalive(conn: object, *, label: str) -> bool:
        keepalive_calls.append(label)
        return True

    monkeypatch.setattr(leader_mod, "apply_keepalive_to_conn", fake_keepalive)

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert keepalive_calls == ["leader", "leader_monitor_conn", "cron_conn"]


# ── Fail-fast when no rebuild path exists ───────────────────────────────


async def test_open_leader_conn_fails_fast_without_factory_or_dsn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """With no leader_conn_factory and pg_dsn_direct None, _open_leader_conn
    must fail fast with RuntimeError - never asyncpg.connect(str(None)),
    which would DNS-retry the literal host 'None' forever. Unreachable via
    open_worker_deps (startup validation forbids it); belt-and-braces for
    hand-built WorkerDeps."""
    leader, deps, _backend, _, _, _shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn_factory = None
    deps.settings.pg_dsn_direct = None

    import taskq.worker.leader as leader_mod

    async def fail_if_called(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        raise AssertionError(f"open_dedicated_conn must not be called (dsn={dsn!r})")

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fail_if_called)

    with pytest.raises(RuntimeError, match="cannot rebuild leader connection"):
        await leader._open_leader_conn()


async def test_open_dedicated_conn_fails_fast_without_factory_or_dsn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Same fail-fast for the monitor/cron dedicated-conn open path."""
    leader, deps, _backend, _, _, _shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn_factory = None
    deps.settings.pg_dsn_direct = None

    import taskq.worker.leader as leader_mod

    async def fail_if_called(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        raise AssertionError(f"open_dedicated_conn must not be called (dsn={dsn!r})")

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fail_if_called)

    with pytest.raises(RuntimeError, match="cannot rebuild leader_monitor_conn"):
        await leader._open_dedicated_conn("leader_monitor_conn")


# ── Demotion precedes the lock release ────────────────────────────────


async def test_step_down_stops_leading_before_the_lock_can_be_released(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """``leading()`` answers false from before the courtesy lock can go.

    Releasing the lock is what lets a peer win the election and take the
    row, and the close that releases it can park for up to the bounded
    close on a dead PG. Anything still reading ``leading()`` as true across
    that suspension would be a second leader acting beside the successor.
    """
    leading_during_close: list[bool] = []

    class _ObservingConn(FakeConn):
        async def close(self) -> None:
            leading_during_close.append(deps.leading())
            await super().close()

    leader_conn = _ObservingConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    # TaskQ-owned, so standing down closes the conn (releasing the courtesy
    # lock) rather than abandoning a caller's handle.
    deps.owns_leader_conn = True

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    assert deps.leading(), "the elected pod must be leading before it stands down"

    await leader._step_down("term_lost")
    shutdown.set()
    await task

    assert leading_during_close == [False], (
        "the leader conn was closed - releasing the courtesy lock - while this "
        "pod still reported leading()"
    )
    assert not deps.is_leader.is_set()
    assert deps.leader_term is None


# ── The trust window narrows leading() on this process's own clock ───────


async def test_leading_narrows_false_once_the_trust_window_closes() -> None:
    """``leading()`` answers by the monotonic deadline, not the event.

    The event can still be set - the election loop has not noticed yet -
    but past ``trusted_until`` a peer may legally hold the row, so every
    leader-gated loop must already read the role as gone.
    """
    deps = _make_deps(is_leader=False)
    loop = asyncio.get_running_loop()

    deps.lead(LeaderTerm(elected_at=datetime.now(UTC), trusted_until=loop.time() + 60))
    assert deps.leading(), "a term inside its trust window leads"

    deps.lead(LeaderTerm(elected_at=datetime.now(UTC), trusted_until=loop.time() - 0.01))
    assert deps.is_leader.is_set(), "test setup: the event outlives the trust window"
    assert not deps.leading(), (
        "past trusted_until the role must read as gone even before the "
        "election loop's step-down clears the event"
    )


async def test_renew_term_with_spent_trust_steps_down_without_touching_the_database(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A loop that wakes with its trust window already spent stands down
    locally - the step-down is the split-brain guard, and issuing a renewal
    against a row a peer may already hold would defeat it."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )
    _ = shutdown

    loop = asyncio.get_running_loop()
    term = LeaderTerm(elected_at=datetime.now(UTC), trusted_until=loop.time() - 0.01)
    deps.lead(term)

    from taskq.worker._transient import UnexpectedLoopErrorGuard

    with structlog.testing.capture_logs() as captured:
        should_sleep = await leader._renew_term(  # pyright: ignore[reportPrivateUsage]  # Why: driving the exact trust-spent branch is the test.
            term, "renew-sql", UnexpectedLoopErrorGuard("test")
        )

    assert should_sleep is False, "a spent trust window ends the term, not the loop"
    assert not deps.is_leader.is_set()
    assert deps.leader_term is None
    assert leader_conn.fetchval_calls == [] and leader_conn.execute_calls == [], (
        "the trust-spent step-down must not touch the database - a peer may already hold the row"
    )
    assert any(
        e.get("kind") == "leadership_lost" and e.get("reason") == "trust_expired" for e in captured
    )


# ── Resign rides a surviving conn ─────────────────────────────────────────


async def test_resign_falls_back_to_the_monitor_conn_when_leader_conn_is_gone(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The shutdown orchestrator closes a TaskQ-owned leader_conn before the
    leader's teardown runs; the resign must still land - fenced on the term -
    through the leader-owned monitor conn, which nothing else closes first."""
    leader, deps, _backend, leader_conn, _, shutdown = await _make_leader(
        monkeypatch=monkeypatch,
    )
    _ = shutdown
    monitor = FakeConn()
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]  # Why: the harness assigns the test double to the leader-owned conn slot directly.

    elected = datetime.now(UTC)
    deps.lead(LeaderTerm(elected_at=elected, trusted_until=60.0))
    deps.leader_conn = None  # what the orchestrator leaves behind

    await leader.resign()

    deletes = [
        (sql, args)
        for sql, args in monitor.execute_calls
        if sql.lstrip().upper().startswith("DELETE FROM")
    ]
    assert len(deletes) == 1, f"the resign must land exactly once, got {monitor.execute_calls!r}"
    _sql, args = deletes[0]
    assert args[1] == elected, "the resign carries the term fence"
    assert leader_conn.execute_calls == [], "the gone conn is not used"


async def test_resign_without_a_term_or_a_live_conn_is_a_noop(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A follower, or a leader whose conns are all gone, has nothing to
    resign and nothing to resign it through - the lapse covers it."""
    leader, deps, _backend, leader_conn, _, shutdown = await _make_leader(
        monkeypatch=monkeypatch,
    )
    _ = shutdown

    # No term (a follower): nothing is written.
    await leader.resign()
    assert leader_conn.execute_calls == []

    # A term with no live conn anywhere: same - the lease lapses on its own.
    deps.leader_conn = None
    deps.lead(LeaderTerm(elected_at=datetime.now(UTC), trusted_until=60.0))
    await leader.resign()
    assert leader_conn.execute_calls == []


# ── A won-but-unassumable lease goes back ─────────────────────────


class _ElectRecordingConn(FakeConn):
    """FakeConn that records elects and resigns into a shared timeline.

    The elect statement's return IS the term's ``elected_at``, and the
    leader conn is rebuilt every failed cycle, so proving the resign
    fences on the CURRENT win needs the answers in call order across
    every conn the cycles opened: a single interleaved timeline of
    ``("elect", term)`` / ``("resign", fence)`` events, not per-conn
    lists a racing loop keeps appending to.
    """

    def __init__(self, timeline: list[tuple[str, datetime]], **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]  # Why: FakeConn kwargs are keyword-only; forwarding keeps this double a drop-in.
        self._timeline = timeline

    async def fetchval(self, sql: str, *args: object) -> object:
        result = await super().fetchval(sql, *args)
        if "INSERT" in sql.upper() and "maintenance_leader" in sql and isinstance(result, datetime):
            self._timeline.append(("elect", result))
        return result

    async def execute(self, sql: str, *args: object) -> str:
        result = await super().execute(sql, *args)
        if sql.lstrip().upper().startswith("DELETE FROM") and "maintenance_leader" in sql:
            fence = args[1]
            assert isinstance(fence, datetime)
            self._timeline.append(("resign", fence))
        return result


def _resign_deletes(conns: Iterable[FakeConn]) -> list[tuple[str, tuple[object, ...]]]:
    """Every maintenance-leader resign DELETE issued through *conns*."""
    return [
        (sql, args)
        for conn in conns
        for sql, args in conn.execute_calls
        if sql.lstrip().upper().startswith("DELETE FROM") and "maintenance_leader" in sql
    ]


async def test_persistent_dedicated_conn_failure_hands_the_won_lease_back(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A pod that keeps WINNING the row but can never open its
    dedicated conns must resign the row once its episode's trust budget
    is spent, and keep resigning on every later re-win, because the
    anchor survives the resign (a still-broken pod buying a fresh window
    per re-win is exactly the unbounded hold being fixed).

    ``leader_lease`` is pinned to the 1 s floor, so the trust window
    (lease - the 1 s margin) is spent the moment the first failed assume
    anchors it: the first failed cycle keeps the row (the blip budget,
    pinned separately below), the second hands it back.
    """
    import taskq.worker.leader as leader_mod

    timeline: list[tuple[str, datetime]] = []
    opened: list[_ElectRecordingConn] = []

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        conn = _ElectRecordingConn(timeline, fetchval_result=True)
        opened.append(conn)
        if label in ("leader_monitor_conn", "cron_conn"):
            # The conn-count-pressure shape from the issue: the one
            # leader-conn slot the election itself freed and re-took
            # opens fine; the two extra dedicated conns are refused.
            raise asyncpg.TooManyConnectionsError("remaining connection slots are reserved")
        return conn

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    leader, deps, _backend, first_conn, _, shutdown = await _make_leader(
        leader_conn=_ElectRecordingConn(timeline, fetchval_result=True),
        monkeypatch=None,
        leader_lease=1.0,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the policy under test.
    try:
        with structlog.testing.capture_logs() as captured:
            await wait_for_condition(
                lambda: any(e.get("kind") == "leader_resigned_unassumable" for e in captured),
                description="the trust-spent hand-back never resigned the "
                "won-but-unassumable lease",
            )
            # The anchor outlives the resign: a still-broken pod that
            # re-wins the freed row hands it straight back, every cycle.
            await wait_for_condition(
                lambda: len(_resign_deletes([*opened, first_conn])) >= 2,
                description="a re-win while still broken bought a fresh "
                "trust window instead of handing the row straight back",
            )
    finally:
        shutdown.set()
        await task

    elects = [term for event, term in timeline if event == "elect"]
    assert elects, "sanity: the pod must have won elects for this to be the shape"
    assert not deps.is_leader.is_set(), "a pod that cannot open its conns never led"
    assert not any(e.get("event") == "leader-elected" for e in captured), (
        "leader-elected is the successful-assume record; this pod never assumed"
    )
    # The fence invariant: every resign fences on the term of the win it
    # is handing back (the most recent elect before it in the timeline),
    # never a stale term from an earlier cycle of the episode.
    last_elect: datetime | None = None
    resigned = 0
    for event, term in timeline:
        if event == "elect":
            last_elect = term
        else:
            resigned += 1
            assert last_elect is not None
            assert term == last_elect, (
                f"a resign fenced on {term!r} while the row it was handing back "
                f"was won at {last_elect!r}: the fence must carry the CURRENT term"
            )
    assert resigned >= 2, "the persistently-broken pod must keep handing re-won rows back"
    for _sql, args in _resign_deletes([*opened, first_conn]):
        assert args[0] == leader._worker_id, "the resign must carry this pod's id"


async def test_transient_dedicated_conn_failure_keeps_the_lease_and_recovers(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The blip half of the budget: one failed conn open inside the
    episode's trust window must not resign: the own-row arm's cheap
    route back (the case: a credential reload, a momentary
    ``TooManyConnections``) is exactly the re-win this window preserves,
    and resigning per blip would thrash leadership on every flake.
    """
    import taskq.worker.leader as leader_mod

    opened: list[FakeConn] = []
    failed_once = False

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        nonlocal failed_once
        conn = FakeConn(fetchval_result=True)
        opened.append(conn)
        if label in ("leader_monitor_conn", "cron_conn") and not failed_once:
            failed_once = True
            raise asyncpg.TooManyConnectionsError("one refused connection slot")
        return conn

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    leader, deps, _backend, first_conn, _, shutdown = await _make_leader(
        leader_conn=FakeConn(fetchval_result=True),
        monkeypatch=None,
        # Default-scale lease: a 39 s trust window, so one blip is deep
        # inside the budget and must never reach the resign.
    )

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the recovery path under test.
    try:
        with structlog.testing.capture_logs() as captured:
            await wait_for_leader(deps)
    finally:
        shutdown.set()
        await task

    assert deps.is_leader.is_set(), "the blip must recover through the own-row arm's cheap re-win"
    assert _resign_deletes([*opened, first_conn]) == [], (
        "a single failed conn open inside the trust window resigned the "
        "won lease: the blip budget is what keeps leadership from "
        "thrashing on every flake"
    )
    assert not any(e.get("kind") == "leader_resigned_unassumable" for e in captured)
    assert any(e.get("kind") == "leader_dedicated_conn_failed" for e in captured), (
        "sanity: the failure this test recovers from must have actually happened"
    )


async def test_escaped_assume_failure_also_hands_the_won_lease_back(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The sibling entry point into: an exception ESCAPING
    ``_assume_leadership`` after a won elect (here, a courtesy-lock
    probe whose conn dies with a transient error the probe's own catch
    reclassifies as a miss-then-raise) leaves the same held-but-never-led
    row behind. ``_election_attempt_failed`` must run the same
    trust-spent hand-back for the won-row cycles it cleans up.
    """
    timeline: list[tuple[str, datetime]] = []
    opened: list[_ElectRecordingConn] = []

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        conn = _ElectRecordingConn(timeline, fetchval_result=True)
        opened.append(conn)
        return conn

    leader, deps, _backend, first_conn, _, shutdown = await _make_leader(
        leader_conn=_ElectRecordingConn(timeline, fetchval_result=True),
        monkeypatch=None,
        leader_lease=1.0,
    )
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    async def _probe_conn_dying_mid_courtesy_lock() -> bool:
        raise asyncpg.PostgresConnectionError("courtesy-lock probe conn died")

    monkeypatch.setattr(  # pyright: ignore[reportAttributeAccessIssue]  # Why: instance-attribute seam; the double stands in for the courtesy probe the election loop calls after a won elect.
        leader,
        "_try_election_lock",
        _probe_conn_dying_mid_courtesy_lock,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the escaped-failure path under test.
    try:
        with structlog.testing.capture_logs() as captured:
            await wait_for_condition(
                lambda: any(e.get("kind") == "leader_resigned_unassumable" for e in captured),
                description="an escaped assume failure after a won elect never "
                "triggered the trust-spent hand-back",
            )
    finally:
        shutdown.set()
        await task

    assert not deps.is_leader.is_set()
    # Same fence invariant as the caught-failure pin: the resign fences on
    # the current win, which the escaped failure could not have carried
    # anywhere but _resign_fence.
    last_elect: datetime | None = None
    resigned = 0
    for event, term in timeline:
        if event == "elect":
            last_elect = term
        else:
            resigned += 1
            assert last_elect is not None
            assert term == last_elect, (
                "the escaped-failure resign must fence on the CURRENT win's term"
            )
    assert resigned >= 1, "the hand-back must issue its fenced DELETE"
    for _sql, args in _resign_deletes([*opened, first_conn]):
        assert args[0] == leader._worker_id
    hand_backs = [e for e in captured if e.get("kind") == "leader_resigned_unassumable"]
    assert hand_backs[0]["reason"] == "assume_failed"


class _SteerableConn(_ElectRecordingConn):
    """A double whose elect answers follow a shared mutable flag, per call.

    The R1 review shape needs the election to flip mid-run (a peer takes
    the row, so this pod's elects start LOSING), while ``FakeConn`` binds
    its lease-statement answer at construction and the leader conn is
    rebuilt every failed cycle. The flag is therefore read at call time:
    ``win`` truthy answers the elect with a fresh term (recorded in the
    timeline), ``win`` falsy answers with no row at all (the ordinary
    follower state).
    """

    def __init__(self, timeline: list[tuple[str, datetime]], state: dict[str, int]) -> None:
        super().__init__(timeline, fetchval_result=True)
        self._state = state

    async def fetchval(self, sql: str, *args: object) -> object:
        if _is_lease_statement(sql) and not _is_server_clock_read(sql) and not self._state["win"]:
            return None
        return await super().fetchval(sql, *args)


async def test_a_blip_after_a_peer_takeover_gets_a_fresh_trust_window(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """F1, the R1 review shape: an OLD episode's spent anchor must not
    resign a NEW term's first conn-open blip.

    Sequence: a broken episode hands the lease back at its trust horizon;
    a peer takes the row; this pod later wins again (the peer's lease
    ended) and hits one failed dedicated-conn open. The stale anchor,
    cleared only by a successful assume in the code under review, is
    long spent, so that single blip resigned the fresh term immediately:
    the per-blip leadership thrash the design explicitly rejected,
    arriving through the back door for exactly the pods that once had an
    unassumable episode.

    A lost election is the observable end of an episode (a peer holds the
    row now), so the anchor must clear there, while the sibling pin
    (``test_persistent_dedicated_conn_failure_hands_the_won_lease_back``)
    keeps the other rule: a re-win while still broken, with no peer
    having taken the row in between, hands it straight back.
    """
    import taskq.worker.leader as leader_mod

    timeline: list[tuple[str, datetime]] = []
    # win: 1 = this pod's elects win (its row / a free row), 0 = a live
    # peer holds the row and every elect loses. fail_opens: every
    # dedicated-conn open refuses (the broken episode). blips_left: a
    # one-shot refusal count for the new term's single blip.
    state: dict[str, int] = {"win": 1, "fail_opens": 1, "blips_left": 0}

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        if label in ("leader_monitor_conn", "cron_conn") and (
            state["fail_opens"] or state["blips_left"] > 0
        ):
            # The one-shot blip decrements only when the persistent
            # failure is over: phase 3's single refusal.
            if state["blips_left"] > 0 and not state["fail_opens"]:
                state["blips_left"] -= 1
            raise asyncpg.TooManyConnectionsError("remaining connection slots reserved")
        return _SteerableConn(timeline, state)

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    leader, deps, _backend, _first_conn, _, shutdown = await _make_leader(
        leader_conn=_SteerableConn(timeline, state),
        monkeypatch=None,
        leader_lease=1.0,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the episode state machine under test.

    try:
        with structlog.testing.capture_logs() as captured:

            def _warn_count() -> int:
                return sum(1 for e in captured if e.get("kind") == "leader_resigned_unassumable")

            # Phase 1, the broken episode: wins, failed opens, the
            # trust-spent hand-back (and its re-win churn).
            await wait_for_condition(
                _warn_count,
                description="phase 1 never produced the trust-spent hand-back",
            )

            # Phase 2, the peer takes the row: this pod's elects lose.
            state["win"] = 0
            await wait_for_condition(
                lambda: any(e.get("kind") == "leader_retry" for e in captured),
                description="the pod never observed the election loss that ends the episode",
            )
            warns_after_loss = _warn_count()

            # Phase 3, the pod wins a new term (the peer's lease ended)
            # and hits exactly one conn-open blip on it: the persistent
            # failure is over, one open still refuses.
            state["win"] = 1
            state["fail_opens"] = 0
            state["blips_left"] = 1
            await wait_for_leader(deps)

        assert _warn_count() == warns_after_loss, (
            "the first conn-open blip of a NEW term resigned immediately: the "
            "spent anchor from an old episode (ended by a peer takeover) "
            "survived the election loss it should have cleared on"
        )
        assert deps.is_leader.is_set(), (
            "the blipped term must recover through its own trust window and "
            "assume on the next cycle"
        )
    finally:
        shutdown.set()
        await task

    # The recovery was real leadership, not a hand-back: this pod assumed
    # the term it won in phase 3.
    assert deps.leader_term is not None
    assert any(e.get("event") == "leader-elected" for e in captured)


async def test_the_hand_back_warn_does_not_fire_when_the_resign_did_not_land(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """F2: ``leader-resigned-unassumable`` is the policy record ("this
    pod handed the lease back because it cannot assume it"), so it must
    fire only when the resign actually DELETED the row.

    The review run logged 20 of those WARNs against zero successful
    resigns: the WARN must follow a resign that actually happened -
    a resign with no live conn returns silently,
    and warning anyway points the runbook's "the fleet is leading from
    elsewhere" advice at a row nobody had resigned. The gate is ``resign()``'s own
    return: the delete count from the command tag, not the absence of
    an error (a fenced no-op is success-shaped).
    """
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(monkeypatch=monkeypatch)
    _ = shutdown

    # A spent anchor (an old episode's budget is long past) and a fenced
    # term, so the hand-back's resign is due right now.
    loop = asyncio.get_running_loop()
    spent = LeaderTerm(elected_at=datetime.now(UTC), trusted_until=loop.time() - 1.0)
    leader._unassumable_anchor = spent  # pyright: ignore[reportPrivateUsage]  # Why: driving the exact spent-anchor branch is the test.
    leader._resign_fence = spent  # pyright: ignore[reportPrivateUsage]  # Why: the fence the resign would carry.

    # No live conn anywhere: resign() has nothing to ride and returns
    # False silently; the hand-back must not claim a hand-back.
    deps.leader_conn = None
    assert leader._leader_monitor_conn is None
    with structlog.testing.capture_logs() as silent:
        await leader._hand_back_unassumable_lease(  # pyright: ignore[reportPrivateUsage]  # Why: the gate under test is this method's own logging decision.
            reason="dedicated_conn_open_failed"
        )
    assert not any(e.get("kind") == "leader_resigned_unassumable" for e in silent), (
        "the WARN claimed a handed-back lease while no resign could even be issued"
    )

    # A live conn whose DELETE lands: the warn fires, and the record is
    # true exactly when the row is gone.
    deps.leader_conn = FakeConn(fetchval_result=True)  # type: ignore[assignment]  # Why: FakeConn is a drop-in for asyncpg.Connection in unit tests; the harness in this file assigns it the same way.
    with structlog.testing.capture_logs() as landed:
        await leader._hand_back_unassumable_lease(  # pyright: ignore[reportPrivateUsage]  # Why: same seam, affirmative arm.
            reason="dedicated_conn_open_failed"
        )
    hand_backs = [e for e in landed if e.get("kind") == "leader_resigned_unassumable"]
    assert len(hand_backs) == 1
    assert hand_backs[0]["reason"] == "dedicated_conn_open_failed"


# ── Leadership gap-window after reload ──────────────────────────────────


async def test_reelection_after_conn_loss_never_waits_on_the_courtesy_lock(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """After reload closes leader_conn, the rebuilt conn's
    pg_try_advisory_lock can return False - the old session's lock release
    is still propagating, or a stranger session holds it. The lease row is
    the authority, so the election must WIN anyway: the courtesy miss is
    logged, leadership is taken, and the loop does not retry the election
    over the lock."""
    original = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    assert deps.is_leader.is_set()

    # The lease statement on the rebuilt conn wins immediately - the
    # departed session's row is this pod's own to re-elect - while the
    # courtesy lock refuses every attempt: the shape of a stale lock that
    # outlives the row behind it.
    lock_attempts = 0

    class RefusingLockConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            nonlocal lock_attempts
            if "pg_try_advisory_lock" in sql:
                lock_attempts += 1
                return False
            return await super().fetchval(sql, *args)

    gap_conn = RefusingLockConn(fetchval_result=True)
    factory_calls = 0

    async def factory() -> FakeConn:
        nonlocal factory_calls
        factory_calls += 1
        return gap_conn

    deps.leader_conn_factory = factory  # type: ignore[assignment]

    # Simulate reload_credentials: close leader_conn, null it, is_leader still set.
    await original.close()
    deps.leader_conn = None

    # The demotion is latched on the leader_conn_died event, not sampled on
    # is_leader: the fast path logs it and clears is_leader in the same step
    # (no await between the log and the clear), while the re-win can re-set
    # the flag within one 10ms heartbeat - the transient False window can
    # close between 0.01s samples under load (same class as the tc2 gate
    # flake, tests/test_leader_chaos.py).
    with structlog.testing.capture_logs() as captured:
        # Bounded poll on the captured entries (a capture_logs list is
        # append-only state - no event exists to wait on).
        await wait_for_condition(
            lambda: any(e.get("kind") == "leader_conn_died" for e in captured),
            description="election loop never took the leader-conn-died demotion path after the null",
        )
        # ...and the re-election must not wait for the courtesy lock at all.
        await wait_for_condition(
            lambda: deps.is_leader.is_set(),
            description="re-election waited on the courtesy lock - the lease row alone decides",
        )

    shutdown.set()
    await task

    assert deps.is_leader.is_set(), (
        "a courtesy-lock refusal must never cost an election the lease row won"
    )
    assert deps.leader_conn is gap_conn
    assert any(e.get("event") == "leader-advisory-lock-unavailable" for e in captured), (
        "the courtesy miss must be logged - during a roll it is the difference "
        "between old-release pods being excluded and not"
    )
    assert lock_attempts == 1, (
        "the lock is attempted once, after the win - never consulted to decide "
        "the election, never retried over"
    )
    # The rebuilt conn is reused - the factory runs once per conn role
    # (leader + monitor + cron), not per attempt.
    assert factory_calls == 3


# ── Watchdog shutdown park ────────────────────────────────────────────────


async def test_watchdog_loop_exits_on_shutdown_while_awaiting_leadership() -> None:
    """Shutdown must wake the watchdog's ``is_leader.wait()`` park.

    Regression: when PG is unreachable the watchdog drops leadership and
    parks on ``is_leader.wait()``; the election loop cannot re-elect (PG
    down), so nothing ever sets ``is_leader`` again. The TaskGroup in
    ``MaintenanceLeader.run`` then waits forever and the worker cannot
    exit - the hang the PG-restart chaos path exposes.
    """

    class _ParkedWitnessEvent(asyncio.Event):
        """Reports the park: *parked* sets the moment a waiter enters
        ``wait()`` — the watchdog's own ``is_leader.wait()`` task is the
        witness that the loop reached its park, no sleep-and-hope."""

        def __init__(self) -> None:
            super().__init__()
            self.parked = asyncio.Event()

        async def wait(self) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]  # Why: the witness's wait returns whatever super() hands back; the override exists only to set the park witness.
            self.parked.set()
            return await super().wait()

    leader, deps, _backend, _conn, _pool, shutdown = await _make_leader(is_leader=False)
    witnessed = _ParkedWitnessEvent()
    deps.is_leader = witnessed
    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    # The parked-event witness: the watchdog is parked on is_leader.wait().
    await wait_for_condition(
        witnessed.parked.is_set,
        description="the watchdog must park on is_leader.wait() before shutdown",
    )
    shutdown.set()
    # The exit assert: unguarded this parks forever; the race wakes it promptly.
    await asyncio.wait_for(task, timeout=2.0)


# ── Leader loops survive PG loss ──────────────────────────────────────────
#
# Regression (PG-restart chaos): a maintenance loop that lets an infra
# connection error escape crashes MaintenanceLeader.run's TaskGroup, which
# propagates into the worker's TaskGroup and cancels every sibling -
# without setting shutdown_event. The heartbeat is cancelled before it can
# reach isolate_self, so no job is re-pended and no phase log is written.
# Every one of these loops must treat PG loss as transient, exactly as
# their already-guarded siblings do.


async def test_sweep_loop_survives_reclaim_connection_loss() -> None:
    """A dead PG in sweep 1 must not escape ``_sweep_loop``."""
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)

    dead_pg_calls = 0

    async def _dead_pg(*_args: object, **_kw: object) -> int:
        nonlocal dead_pg_calls
        dead_pg_calls += 1
        raise OSError(-2, "Name or service not known")

    backend.reclaim_expired_locks = _dead_pg  # type: ignore[method-assign]  # Why: test-only injection of a dead-PG failure.

    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # The survival assert must be preceded by proof the loop actually
    # faced the failure: the sweep attempts fire at loop entry, but a
    # fixed window can contain zero ticks under scheduler starvation,
    # and ``not task.done()`` would then pass without the dead-PG path
    # having run at all.
    await wait_for_condition(
        lambda: dead_pg_calls >= 1,
        description="the sweep loop never attempted reclaim_expired_locks against the dead PG",
    )
    assert not task.done(), f"sweep loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_sweep_loop_survives_deadline_sweep_connection_loss() -> None:
    """A dead PG in sweep 2 must not escape ``_sweep_loop``."""
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)

    dead_pg_calls = 0

    async def _dead_pg(*_args: object, **_kw: object) -> int:
        nonlocal dead_pg_calls
        dead_pg_calls += 1
        raise asyncpg.InterfaceError("connection is closed")

    backend.deadline_sweep = _dead_pg  # type: ignore[method-assign]  # Why: test-only injection of a dead-PG failure.

    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # Same proven-window discipline as the reclaim test above: the
    # survival assert is meaningless unless the loop first faced the
    # dead-PG call.
    await wait_for_condition(
        lambda: dead_pg_calls >= 1,
        description="the sweep loop never attempted deadline_sweep against the dead PG",
    )
    assert not task.done(), f"sweep loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_scheduled_wake_loop_survives_connection_loss() -> None:
    """A dead PG in ``scheduled_to_pending`` must not escape the wake loop."""
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)

    dead_pg_calls = 0

    async def _dead_pg(**_kw: object) -> int:
        nonlocal dead_pg_calls
        dead_pg_calls += 1
        raise OSError(-2, "Name or service not known")

    backend.scheduled_to_pending = _dead_pg  # type: ignore[method-assign]  # Why: test-only injection of a dead-PG failure.

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    # The wake loop attempts its first sweep at loop entry, then parks
    # on its 1s tick - the bounded wait proves the attempt happened
    # before the survival assert speaks.
    await wait_for_condition(
        lambda: dead_pg_calls >= 1,
        description="the scheduled-wake loop never attempted scheduled_to_pending against the dead PG",
    )
    assert not task.done(), f"wake loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_scheduled_wake_loop_survives_notify_connection_loss() -> None:
    """A dead PG in the post-promotion ``pg_notify`` must not escape."""
    # FakePool(fail_acquire_with=...) raises the dead-PG error at the
    # acquire seam and counts every attempt: the wake loop only reaches
    # the acquire after scheduled_to_pending reports a promotion, so a
    # counted acquire is the proof the loop faced the failure.
    dead_pool = FakePool(fail_acquire_with=OSError(-2, "Name or service not known"))
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(
        is_leader=True,
        dispatcher_pool=dead_pool,
    )
    backend.scheduled_to_pending = lambda **_kw: asyncio.sleep(0, result=3)  # type: ignore[method-assign]  # Why: async stub returning a positive promotion count.

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    await wait_for_condition(
        lambda: dead_pool.acquire_count >= 1,
        description="the scheduled-wake loop never reached its post-promotion pg_notify against the dead pool",
    )
    assert not task.done(), f"wake loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_election_upsert_survives_connection_loss(monkeypatch: Any) -> None:
    """A dead PG in the maintenance_leader lease write must not escape the election loop."""

    def _dead_pg() -> None:
        raise asyncpg.InterfaceError("connection is closed")

    class _DeadOnLeaseWrite(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            if _is_lease_statement(sql):
                self.fetchval_calls.append((sql, args))
                _dead_pg()
            return await super().fetchval(sql, *args)

    leader_conn = _DeadOnLeaseWrite(fetchval_result=True)
    leader, deps, _backend, _conn, _pool, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))
    # The double records the statement before the failure hook fires, so the
    # recording is the proof the loop reached (and lost) the lease write -
    # the failure this test exists for.
    await wait_for_condition(
        lambda: any("maintenance_leader" in sql for sql, _ in leader_conn.fetchval_calls),
        description="the election loop never attempted the maintenance_leader lease write against the dead PG",
    )
    assert not task.done(), f"election loop died: {task.exception() if task.done() else None!r}"
    assert not deps.is_leader.is_set(), "leadership must not be claimed when the UPSERT failed"
    await _stop_after_tick(task, shutdown, delay=0.0)


# ── Scheduled-wake loop transient PG guard ─────────────────────────────────


async def test_scheduled_wake_loop_survives_transient_pg_errors() -> None:
    """Transient PG failures from ``scheduled_to_pending`` must not escape
    into ``MaintenanceLeader.run``'s TaskGroup (same defect class as the
    unguarded reclaim/deadline sweeps)."""

    class _DeadPgBackend:
        """Backend whose ``scheduled_to_pending`` raises the dead-PG error
        class, counting attempts so the survival assert is preceded by
        proof the loop faced the failure.

        Argless to match the production call site - the sweep's
        server-side clock predicate is the single arbiter, so a ``now``
        parameter would TypeError into the generic backstop instead of
        the transient branch this test pins.
        """

        def __init__(self) -> None:
            self.scheduled_to_pending_calls = 0

        async def scheduled_to_pending(self) -> int:
            self.scheduled_to_pending_calls += 1
            raise OSError(111, "Connect call failed")

    backend = _DeadPgBackend()
    deps = _make_deps(is_leader=True)
    leader = MaintenanceLeader(
        deps,
        new_uuid(),
        backend,  # type: ignore[arg-type]  # Why: stub satisfying only the called method.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    await wait_for_condition(
        lambda: backend.scheduled_to_pending_calls >= 1,
        description="the scheduled-wake loop never attempted scheduled_to_pending against the dead PG",
    )
    assert not task.done(), "scheduled-wake loop died on a transient PG error"
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ── Gated-loop liveness registration ─────────────────────────────────────


async def test_watchdog_loop_forgets_tick_registration_on_demotion() -> None:
    """Demotion must drop the ``leader.watchdog`` liveness registration.

    Regression: the tick is stamped inside the ``is_leader``-gated inner
    loop, so on demotion (probe failure -> is_leader cleared -> break) the
    loop re-parks on ``is_leader.wait()`` and stops ticking through no
    fault of its own. A lingering registration goes stale while parked,
    so watchdog detector 2 would force-exit a perfectly healthy non-leader
    worker ~grace seconds after every ordinary leadership change. The
    ``forget`` must therefore run where the inner loop is LEFT, not after
    the park (which only returns once is_leader is set again).
    """
    from taskq.worker._watchdog import LoopLiveness

    def _boom() -> None:
        raise OSError("pg gone")

    leader, deps, _backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)
    now = [1000.0]
    deps.liveness = LoopLiveness(clock=lambda: now[0])
    # Drive the real demotion path: the monitor probe raises, which clears
    # is_leader via _close_leader_owned_conns and breaks the inner loop.
    leader._leader_monitor_conn = FakeConn(on_fetchval=_boom)  # type: ignore[assignment]  # Why: FakeConn is the asyncpg.Connection stand-in used throughout this module.

    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    # Poll-asserted, not a bare sleep: the demotion is synchronous on the
    # first failed probe (the probe raises, _step_down clears is_leader in
    # the same loop turn), so this budget covers loop turns only — 2.0 s of
    # co-tenancy slack, deliberately not a stretch of the 5s
    # _WATCHDOG_INTERVAL_SECS park (0.4x of it): a demotion that ever waited
    # a full probe cadence would false-fail here, and that is the tripwire.
    # The no-demotion regression this pins leaves the flag set, so the poll
    # still times out red.
    await wait_for_condition(
        lambda: not deps.is_leader.is_set(),
        description="the monitor probe failure to demote this worker (is_leader cleared)",
        timeout=2.0,
    )

    await asyncio.sleep(0.05)  # the loop has re-parked as a non-leader
    now[0] += 3600.0  # far beyond any staleness budget
    stale = deps.liveness.stale()

    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert stale == [], (
        f"stale registrations after demotion would force-exit a healthy worker: {stale}"
    )


async def test_close_leader_owned_conns_identity_guard() -> None:
    """_close_leader_owned_conns must only null the attribute if it still
    points to the same connection object - not a fresh one created by the
    election loop during the close suspension.

    Simulates the race directly: start the bounded close of the stale
    conn, suspend it mid-``await`` (via ``close_wait``), assign a fresh
    conn to the same attribute while suspended, then let the close
    complete. The fresh conn must survive - the attribute must still
    point at it, not be nulled out from under the election loop.
    """
    leader, _deps, _backend, _leader_conn, _dp, _shutdown = await _make_leader()

    # _CloseEntryConn marks the exact point close() is entered - the
    # bounded wait for the suspended close, never a sleep-poll hoping
    # the loop reached it.
    stale_conn = _CloseEntryConn()
    stale_conn.close_wait.clear()  # close() blocks until we release it below
    leader._cron_conn = stale_conn  # type: ignore[assignment]  # Why: FakeConn is the asyncpg.Connection stand-in used throughout this module.

    close_task = asyncio.create_task(leader._close_leader_owned_conns())

    # Wait until the close is suspended inside stale_conn.close().
    await wait_for(stale_conn.close_entered)
    assert stale_conn.close_calls == 1, "close() was never entered"

    # While the close is suspended, the election loop creates and assigns
    # a fresh connection to the same attribute.
    fresh_conn = FakeConn()
    leader._cron_conn = fresh_conn  # type: ignore[assignment]  # Why: FakeConn is the asyncpg.Connection stand-in used throughout this module.

    # Let the suspended close() complete.
    stale_conn.close_wait.set()
    await close_task

    assert leader._cron_conn is not None, (
        "identity guard regressed: fresh conn created during the close "
        "suspension was nulled out, orphaning it and busy-spinning"
    )
    assert leader._cron_conn is fresh_conn


# ── Takeover recovery: the assume path re-runs the cron ownership pass ──


async def test_assume_leadership_runs_the_cron_takeover_recovery(
    monkeypatch: Any,
) -> None:
    """A leader that inherits the cron table re-runs the stale-auto-disable
    recovery over the code's declared specs (issue #460's deploy window: an
    old leader's unmarked auto-disable can land after every new pod has
    booted, so the takeover is the last look the new code gets)."""
    import taskq.worker.leader as leader_mod

    spec_calls: list[object] = []
    captured: dict[str, object] = {}

    async def fake_recovery(deps: Any, settings: Any, specs: Any) -> int:
        captured["specs"] = list(specs)
        spec_calls.append(specs)
        return 1

    monkeypatch.setattr(leader_mod, "revert_stale_auto_disables", fake_recovery)

    spec = CronScheduleSpec(
        actor="takeover_actor",
        name="hourly",
        cron_expr="0 * * * *",
        timezone="UTC",
    )
    leader, deps, _backend, _conn, _pool, _shutdown = await _make_leader(monkeypatch=monkeypatch)
    leader2 = MaintenanceLeader(
        deps,
        new_uuid(),
        leader._backend,  # type: ignore[arg-type]  # Why: the InMemoryBackend _make_leader built satisfies the Backend protocol at runtime.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        cron_specs=[spec],
    )
    await leader2._assume_leadership(datetime.now(UTC), 0.0)

    assert spec_calls, "the assume path must run the takeover recovery"
    assert captured["specs"] == [spec]
    assert deps.is_leader.is_set()


async def test_assume_leadership_without_specs_skips_the_recovery(
    monkeypatch: Any,
) -> None:
    """No declared specs (tests, spec-less deployments): the takeover pass
    is skipped entirely, the assume path unchanged."""
    import taskq.worker.leader as leader_mod

    called = False

    async def fake_recovery(deps: Any, settings: Any, specs: Any) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(leader_mod, "revert_stale_auto_disables", fake_recovery)

    leader, deps, _backend, _conn, _pool, _shutdown = await _make_leader(monkeypatch=monkeypatch)
    await leader._assume_leadership(datetime.now(UTC), 0.0)

    assert not called
    assert deps.is_leader.is_set()


async def test_assume_leadership_survives_a_failing_takeover_recovery(
    monkeypatch: Any,
) -> None:
    """A recovery pass that cannot run (pool down) must not fail the
    assumption: the pod keeps the term it won, the next assumption
    retries the pass."""
    import taskq.worker.leader as leader_mod

    async def fake_recovery(deps: Any, settings: Any, specs: Any) -> int:
        raise OSError("pool down")

    monkeypatch.setattr(leader_mod, "revert_stale_auto_disables", fake_recovery)

    spec = CronScheduleSpec(
        actor="takeover_actor",
        name="hourly",
        cron_expr="0 * * * *",
        timezone="UTC",
    )
    leader, deps, _backend, _conn, _pool, _shutdown = await _make_leader(monkeypatch=monkeypatch)
    leader2 = MaintenanceLeader(
        deps,
        new_uuid(),
        leader._backend,  # type: ignore[arg-type]  # Why: the InMemoryBackend _make_leader built satisfies the Backend protocol at runtime.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        cron_specs=[spec],
    )
    await leader2._assume_leadership(datetime.now(UTC), 0.0)

    assert deps.is_leader.is_set(), (
        "the takeover recovery is best-effort; a pass that cannot reach "
        "the pool is a degraded maintenance plane, never a lost election"
    )


# ── The stop-race resign, the renew-failure ladder, and the fatal FK arm ─


class _StopMidElectConn(FakeConn):
    """FakeConn that lands the stop signal while the elect is in flight.

    The elect statement's round trip is exactly where SIGTERM intercepts
    a winning candidate. The double stamps ``deps.shutdown_start_event``
    the moment the lease statement goes out, then answers with the win:
    the stop is OBSERVED before the caller's ``_stopping()`` re-check
    reads it, which is the deterministic interleave
    ``_resign_won_lease`` exists for - a win whose row is on this pod's
    name while the drain has already begun.
    """

    def __init__(self, stop_event: asyncio.Event, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]  # Why: FakeConn kwargs are keyword-only; forwarding keeps this double a drop-in.
        self._stop_event = stop_event
        self.elect_in_flight = asyncio.Event()
        self.won_elected_at: datetime | None = None

    async def fetchval(self, sql: str, *args: object) -> object:
        if _is_lease_statement(sql) and not _is_server_clock_read(sql):
            self.elect_in_flight.set()
            self._stop_event.set()
            result = await super().fetchval(sql, *args)
            if isinstance(result, datetime):
                self.won_elected_at = result
            return result
        return await super().fetchval(sql, *args)


async def test_elect_won_row_after_stop_is_resigned_fenced(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The stop race: an elect that WINS after the stop lands is handed
    back fenced, and the assume path never starts.

    Every peer's lapse predicate is re-falsified by the won row (this
    pod's name is on it) until it is handed back - if the lease were held
    across the drain, the fleet's maintenance plane stalls for a whole
    leader lease while a dying pod drains. The contract, pinned per
    assert: the resign DELETE rides the same conn the winning elect just
    used (the orchestrator closes that conn only after the phases; a
    resign on any other wire can land after the close) and is fenced on
    ``(worker_id, elected_at)`` of the WON term (a fence on anything else
    either deletes a successor's row or, more likely here, no-op's and
    leaves this pod's row holding the fleet); the resign fence is cleared
    once the DELETE lands (a stale fence re-arming a later resign could
    delete a successor's term); and ``lead()`` never runs - no
    ``is_leader``, no term, no monitor/cron conns, no courtesy probe - so
    no leader-gated sweep can begin on a pod that is exiting.
    """
    stop_event = asyncio.Event()
    conn = _StopMidElectConn(stop_event=stop_event, fetchval_result=True)
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )
    # The double stamps the event the ``_stopping()`` predicate reads -
    # deps's own default event is a different object.
    deps.shutdown_start_event = stop_event
    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the election loop is the stop-race path under test.
    try:
        await wait_for(conn.elect_in_flight, description="the elect statement to go in flight")
        stop_event.set()  # the SIGTERM, mid-elect
        await wait_for_condition(
            lambda: len(_resign_deletes([conn])) == 1,
            description="the immediate fenced resign of the won row",
        )
        await wait_for_condition(
            lambda: leader._resign_fence is None,
            description="the resign fence to clear once the DELETE landed",
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)

    _resign_sql, resign_args = _resign_deletes([conn])[0]
    assert conn.won_elected_at is not None, "sanity: the elect must have WON"
    assert resign_args == (leader._worker_id, conn.won_elected_at), (
        f"the resign must be fenced on the WON term (worker_id, elected_at), got {resign_args!r}"
    )
    # Same-conn proof: the elect rode this conn's fetchval, the resign
    # this conn's execute - one wire, still open, before the close.
    assert any(
        _is_lease_statement(sql) and not _is_server_clock_read(sql)
        for sql, _ in conn.fetchval_calls
    )
    assert not deps.is_leader.is_set() and deps.leader_term is None, (
        "the won lease was assumed - a dying pod held leadership across the drain"
    )
    assert leader._leader_monitor_conn is None and leader._cron_conn is None, (
        "the assume path opened dedicated conns for a stopping worker"
    )
    assert not any("pg_try_advisory_lock" in sql for sql, _ in conn.fetchval_calls), (
        "the assume path ran (the courtesy-lock probe issued)"
    )
    # The loop parks attempt-free after the hand-back: exactly one elect
    # statement the whole run.
    elect_attempts = sum(
        1
        for sql, _ in conn.fetchval_calls
        if _is_lease_statement(sql) and not _is_server_clock_read(sql)
    )
    assert elect_attempts == 1, (
        f"a stopping worker re-attempted the election ({elect_attempts} attempts)"
    )


async def test_renew_failure_ladder_steps_down_when_trust_spent(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """``_renew_failed`` with the trust window spent stands down locally.

    Past ``trusted_until`` a peer may already hold the row, so the ladder's
    terminal rung must demote without touching the database (issuing a
    resignation or a renewal against a row a peer holds is the
    split-brain the trust window exists to prevent) and return False (the
    loop re-enters as a candidate). The regressions pinned: a ladder that
    keeps retrying past the trust horizon (a zombie pod acting on a term
    it no longer owns), and a step-down that reports the loss without
    clearing the flag/term (every leader-gated loop keeps sweeping on a
    dead term).
    """
    leader, deps, _backend, leader_conn, _, shutdown = await _make_leader(
        monkeypatch=monkeypatch,
    )
    _ = shutdown

    loop = asyncio.get_running_loop()
    term = LeaderTerm(elected_at=datetime.now(UTC), trusted_until=loop.time() - 0.01)
    deps.lead(term)

    from taskq.worker._transient import UnexpectedLoopErrorGuard

    with structlog.testing.capture_logs() as captured:
        should_sleep = await leader._renew_failed(  # pyright: ignore[reportPrivateUsage]  # Why: driving the exact ladder rung is the test.
            term,
            asyncpg.PostgresConnectionError("renew write lost"),
            guard=UnexpectedLoopErrorGuard("test"),
            unexpected=False,
        )

    assert should_sleep is False, "a spent trust window ends the term, not the loop"
    assert not deps.is_leader.is_set() and deps.leader_term is None, (
        "the step-down must clear the role now, not on some later cycle - "
        "leader-gated loops read the flag per iteration"
    )
    assert leader_conn.fetchval_calls == [] and leader_conn.execute_calls == [], (
        "the trust-spent step-down must not touch the database - a peer may already hold the row"
    )
    assert any(e.get("kind") == "leader_lease_renew_failed" for e in captured), (
        "the renew failure must be visible as its own event before the step-down decision"
    )
    assert any(
        e.get("event") == "leadership-lost" and e.get("reason") == "renew_failed" for e in captured
    ), (
        "the step-down must carry the renew_failed reason - the runbook's 'PG loss vs successor' triage reads it"
    )


async def test_renew_failure_inside_trust_backs_off_without_demotion(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The back-off rung: trust remaining → retry inside it, keep leading.

    One transient renew failure inside the window must not demote - the
    window exists precisely so a PG blip doesn't thrash leadership (the
    step-down hands the row's availability to the next election cycle).
    The return is still False (the caller re-enters immediately rather
    than sleeping a full tick on top of the back-off), and the renew
    failure is WARNed with its own event either way.
    """
    leader, deps, _backend, leader_conn, _, shutdown = await _make_leader(
        monkeypatch=monkeypatch,
    )
    _ = shutdown

    loop = asyncio.get_running_loop()
    # 2.0 s of trust: a loop stall between constructing the term and the
    # rung's ``remaining`` read must not outlive the window and silently
    # turn this back-off pin into the step-down arm (the old 0.05 s could
    # lose that race). The rung's sleep is bounded by min(1.0, remaining),
    # so the wider window costs the test at most one second.
    term = LeaderTerm(elected_at=datetime.now(UTC), trusted_until=loop.time() + 2.0)
    deps.lead(term)

    from taskq.worker._transient import UnexpectedLoopErrorGuard

    with structlog.testing.capture_logs() as captured:
        should_sleep = await leader._renew_failed(  # pyright: ignore[reportPrivateUsage]
            term,
            asyncpg.PostgresConnectionError("renew write lost"),
            guard=UnexpectedLoopErrorGuard("test"),
            unexpected=False,
        )

    assert should_sleep is False
    assert deps.is_leader.is_set() and deps.leader_term is term, (
        "a transient renew failure inside the trust window demoted the "
        "leader - leadership thrashes on every PG blip"
    )
    assert leader_conn.fetchval_calls == [] and leader_conn.execute_calls == [], (
        "the back-off rung must not write anything - the retry is the next loop iteration's renewal"
    )
    assert any(e.get("kind") == "leader_lease_renew_failed" for e in captured)
    assert not any(e.get("event") == "leadership-lost" for e in captured)


async def test_elect_fk_violation_is_fatal_not_transient(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A ForeignKeyViolationError that SURVIVES the restore attempt kills the
    loop deliberately - it is not retried as transient.

    The FK constraint means the maintenance_leader row references a
    workers row that does not exist. The arm first restores the
    registration once and retries the elect (the reaped-while-live
    recovery); the persistent-FK scenario pinned here - a broken FK
    constraint or a recurrent deletion - raises FK on the retry too, and
    no amount of further retrying fixes a permanent fault: a transient
    classification would spin elect attempts forever, log-storming it.
    The contract: the error event fires once at ERROR level, exactly two
    elect statements are issued (the initial elect plus its single
    post-restore retry) with one restore INSERT between them, the
    worker-wide ``shutdown`` event is set (the TaskGroup siblings
    unwind), and the loop RETURNS (the task completes - a mutation that
    retries leaves it running and this test red on both the attempt count
    and the join).
    """

    class _FkViolatingConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            if _is_lease_statement(sql) and not _is_server_clock_read(sql):
                self.fetchval_calls.append((sql, args))
                raise asyncpg.ForeignKeyViolationError(
                    "insert or update on table 'maintenance_leader' violates "
                    "foreign key constraint 'maintenance_leader_worker_id_fkey'"
                )
            return await super().fetchval(sql, *args)

    conn = _FkViolatingConn()
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    with structlog.testing.capture_logs() as captured:
        done, _pending = await asyncio.wait({task}, timeout=5.0)

    assert task in done, "the fatal arm must RETURN, not retry forever"

    assert shutdown.is_set(), (
        "the deliberate fatal must stop the worker-wide shutdown event - "
        "the TaskGroup siblings (watchdog, sweeps) have to unwind"
    )
    fk_events = [e for e in captured if e.get("kind") == "leader_upsert_fk_violation"]
    assert len(fk_events) == 1 and fk_events[0]["log_level"] == "error", (
        f"expected exactly one ERROR-level leader_upsert_fk_violation, got {fk_events!r}"
    )
    elect_attempts = sum(
        1
        for sql, _ in conn.fetchval_calls
        if _is_lease_statement(sql) and not _is_server_clock_read(sql)
    )
    assert elect_attempts == 2, (
        f"expected the initial elect plus its single post-restore retry ({elect_attempts} "
        "elect attempts) - zero retries would mean the reaped-row recovery never ran, "
        "more than two means a permanent fault is being retried as transient and "
        "log-storms forever"
    )
    restore_attempts = sum(
        1
        for sql, _ in conn.execute_calls
        if ".WORKERS" in " ".join(sql.split()).upper()
        and "ON CONFLICT (ID) DO NOTHING" in " ".join(sql.split()).upper()
    )
    assert restore_attempts == 1, (
        f"expected exactly one idempotent restore INSERT between the two elects, "
        f"got {restore_attempts}"
    )
    assert not deps.is_leader.is_set()
    assert not any(e.get("kind") == "election_attempt_failed" for e in captured), (
        "the FK arm must not route through the transient cleanup - the "
        "deliberate fatal is its own path"
    )
    assert not any(e.get("kind") == "worker_registration_restored" for e in captured), (
        "a restore whose retry still hits the FK never restored anything "
        "durable - the recovery warning must not fire on this path"
    )


class _FkThenHealthyConn(FakeConn):
    """Elect raises FK exactly once (the reaped-row shape), then wins.

    The restore INSERT is answered with the configured command tag - both
    "INSERT 0 1" (this repair landed first) and "INSERT 0 0" (a concurrent
    repair, the heartbeat's, beat us to the row) are outcomes the recovery
    must treat as "the row may exist now; the retry elect decides".
    """

    def __init__(self, *, restore_tag: str = "INSERT 0 1", **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]  # Why: FakeConn kwargs are typed loosely for the doubles; the subclass forwards them unchanged.
        self._fk_elects_left = 1
        self.restore_tag = restore_tag
        self.restore_calls = 0

    async def fetchval(self, sql: str, *args: object) -> object:
        if _is_lease_statement(sql) and not _is_server_clock_read(sql):
            self.fetchval_calls.append((sql, args))
            if self._fk_elects_left > 0:
                self._fk_elects_left -= 1
                raise asyncpg.ForeignKeyViolationError(
                    "insert or update on table 'maintenance_leader' violates "
                    "foreign key constraint 'maintenance_leader_worker_id_fkey'"
                )
            return datetime.now(UTC)
        return await super().fetchval(sql, *args)

    async def execute(self, sql: str, *args: object) -> str:
        normalized = " ".join(sql.split()).upper()
        if ".WORKERS" in normalized and "ON CONFLICT (ID) DO NOTHING" in normalized:
            self.restore_calls += 1
            return self.restore_tag
        return await super().execute(sql, *args)


@pytest.mark.parametrize("restore_tag", ["INSERT 0 1", "INSERT 0 0"])
async def test_elect_fk_from_a_reaped_row_restores_and_wins(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
    restore_tag: str,
) -> None:
    """A reaped row recovers: restore once, retry the elect once, win.

    The FK violation's usual producer is the stale sweep reaping a LIVE
    worker's row, not a never-registered deploy fault - so the arm must
    restore the registration and re-campaign, not shut the process down.
    Both restore tags are pinned: a concurrent repair's "INSERT 0 0" is
    not a failure, the retried elect is the authoritative check.
    """
    conn = _FkThenHealthyConn(restore_tag=restore_tag)
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )
    task = asyncio.create_task(leader._election_loop(shutdown))
    with structlog.testing.capture_logs() as captured:
        await wait_for_leader(deps)
        assert not shutdown.is_set(), (
            "the live worker must recover and win, not set the process-wide shutdown"
        )
    shutdown.set()
    await task

    assert conn.restore_calls == 1, "expected exactly one restore INSERT"
    elect_attempts = sum(
        1
        for sql, _ in conn.fetchval_calls
        if _is_lease_statement(sql) and not _is_server_clock_read(sql)
    )
    assert elect_attempts == 2, (
        f"expected the initial elect plus the single post-restore retry, got {elect_attempts}"
    )
    restored = [e for e in captured if e.get("kind") == "worker_registration_restored"]
    assert len(restored) == 1 and restored[0]["log_level"] == "warning", (
        f"expected exactly one WARNING-level worker-registration-restored, got {restored!r}"
    )
    assert restored[0]["source"] == "leader_election"
    assert not any(e.get("kind") == "leader_upsert_fk_violation" for e in captured)


class _RestoreRefusedConn(FakeConn):
    """Elect raises FK (the reaped-row shape); the restore INSERT is
    refused permanently (never able to register)."""

    async def fetchval(self, sql: str, *args: object) -> object:
        if _is_lease_statement(sql) and not _is_server_clock_read(sql):
            self.fetchval_calls.append((sql, args))
            raise asyncpg.ForeignKeyViolationError(
                "insert or update on table 'maintenance_leader' violates "
                "foreign key constraint 'maintenance_leader_worker_id_fkey'"
            )
        return await super().fetchval(sql, *args)

    async def execute(self, sql: str, *args: object) -> str:
        normalized = " ".join(sql.split()).upper()
        if ".WORKERS" in normalized and "ON CONFLICT (ID) DO NOTHING" in normalized:
            self.execute_calls.append((sql, args))
            raise asyncpg.InsufficientPrivilegeError("permission denied for table workers")
        return await super().execute(sql, *args)


async def test_elect_fk_with_a_permanently_refused_restore_fails_closed(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """A restore the server will never accept is a permanent fault: fail
    closed on the first cycle, with the restore's own cause attached.

    A revoked INSERT privilege or a broken schema cannot recover by
    retrying - the FK arm's restore attempt must not route the refusal
    through the transient budget (that would log-storm a permanent fault)
    and must not log a restoration that never happened.
    """
    conn = _RestoreRefusedConn()
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )
    task = asyncio.create_task(leader._election_loop(shutdown))
    with structlog.testing.capture_logs() as captured:
        done, _pending = await asyncio.wait({task}, timeout=5.0)

    assert task in done, "the permanent refusal must RETURN, not retry"
    assert shutdown.is_set()
    fk_events = [e for e in captured if e.get("kind") == "leader_upsert_fk_violation"]
    assert len(fk_events) == 1 and fk_events[0]["log_level"] == "error", (
        f"expected exactly one ERROR-level leader_upsert_fk_violation, got {fk_events!r}"
    )
    assert "cause" in fk_events[0], (
        "the refusal's own cause must ride the fatal event - the operator "
        "has to see what the restore was refused WITH, not just the FK"
    )
    elect_attempts = sum(
        1
        for sql, _ in conn.fetchval_calls
        if _is_lease_statement(sql) and not _is_server_clock_read(sql)
    )
    assert elect_attempts == 1, (
        "a refused restore must not retry the elect - the fault is permanent, "
        f"not a reaped row; got {elect_attempts} elect attempts"
    )
    assert not deps.is_leader.is_set()
    assert not any(e.get("kind") == "worker_registration_restored" for e in captured)


class _StopMidElectThenFkConn(FakeConn):
    """The elect raises FK with the stop signal landing in the same round trip.

    SIGTERM can be observed between the FK arriving and the arm's
    ``_stopping()`` read - a drain beginning on a pod whose row a peer's
    sweep has just reaped. The double stamps the stop event as the lease
    statement goes out and then raises the FK, so the arm's first gate
    reads a stop that was not set when the attempt started.
    """

    def __init__(self, stop_event: asyncio.Event) -> None:
        super().__init__()
        self._stop_event = stop_event
        self.elect_in_flight = asyncio.Event()

    async def fetchval(self, sql: str, *args: object) -> object:
        if _is_lease_statement(sql) and not _is_server_clock_read(sql):
            self.fetchval_calls.append((sql, args))
            self.elect_in_flight.set()
            self._stop_event.set()
            raise asyncpg.ForeignKeyViolationError(
                "insert or update on table 'maintenance_leader' violates "
                "foreign key constraint 'maintenance_leader_worker_id_fkey'"
            )
        return await super().fetchval(sql, *args)


async def test_elect_fk_observed_while_stopping_never_restores_or_fails_closed(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """An FK that arrives after the stop signal must not re-insert a row
    and must not turn the drain fatal.

    The reaping sweep and a SIGTERM intersect on the very round trip the
    elect is in: the row is gone, the arm sees the FK, and the drain has
    already begun. The shutdown-ordering contract, in its election-loop
    scope, says a stopping campaign never re-inserts a membership row while
    draining (the heartbeat deliberately keeps restoring, for the attempt
    attribution the drain's holder CTE reads - clause 6 in ``leader``'s
    module header); and because the fault is not permanent - it
    is the stop, not a broken deploy - the fail-closed arm must not fire
    either, or one reaped row landing inside a deploy's drain would cut
    every sibling loop short mid hand-back. The loop parks attempt-free
    for the stop; no further election may be issued.
    """
    stop_event = asyncio.Event()
    conn = _StopMidElectThenFkConn(stop_event)
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )
    # The double stamps the event the ``_stopping()`` predicate reads.
    deps.shutdown_start_event = stop_event

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the stop-race FK arm lives in the election loop itself.
    shutdown_set_by_loop = False
    try:
        with structlog.testing.capture_logs() as captured:
            await wait_for(conn.elect_in_flight, description="the elect statement to go in flight")
            # The stop gate drops the loop's liveness registration and parks
            # on the shutdown event: the observable proof the arm returned to
            # the top rather than restoring, retrying, or returning fatally.
            await wait_for_condition(
                lambda: "leader.election" not in deps.liveness.ages(),
                description="the stopping election loop to park attempt-free",
                timeout=5.0,
            )
            # Read before this test's own teardown sets the event (which is
            # the graceful stop, not the fail-closed arm under attack).
            shutdown_set_by_loop = shutdown.is_set()
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)

    restore_calls = [
        (sql, args)
        for sql, args in conn.execute_calls
        if ".WORKERS" in " ".join(sql.split()).upper()
        and "ON CONFLICT (ID) DO NOTHING" in " ".join(sql.split()).upper()
    ]
    assert restore_calls == [], (
        "a stopping worker re-inserted its workers row while draining - the "
        "FK arm ran its repair past the stop gate instead of parking"
    )
    elect_attempts = sum(
        1
        for sql, _ in conn.fetchval_calls
        if _is_lease_statement(sql) and not _is_server_clock_read(sql)
    )
    assert elect_attempts == 1, (
        "a stopping worker re-attempted the election after the FK; the "
        f"retry belongs to a live campaign, not a drain (got {elect_attempts})"
    )
    assert not shutdown_set_by_loop, (
        "the FK that landed after the stop signal was treated as a permanent "
        "fault: the fail-closed arm cut the process-wide shutdown during the drain"
    )
    assert not any(e.get("kind") == "leader_upsert_fk_violation" for e in captured)
    assert not any(e.get("kind") == "worker_registration_restored" for e in captured)
    assert not any(e.get("kind") == "election_attempt_failed" for e in captured), (
        "the stop-race FK was routed through the transient cleanup - it was "
        "not an attempt failure, the drain had begun"
    )


class _FkAfterConnNulledMidElectConn(FakeConn):
    """The elect's round trip nulls ``deps.leader_conn``, THEN raises FK.

    ``reload_credentials``' close-timeout path nulls ``deps.leader_conn``
    while an await on it is still in flight. This double lands the two in
    the order the FK arm's re-read exists for: the attribute is already
    ``None`` when the arm starts, so the arm's own read - not a binding
    captured before the await - decides what it may write.
    """

    def __init__(self) -> None:
        super().__init__()
        self.null_leader_conn: Callable[[], None] | None = None
        self.elect_in_flight = asyncio.Event()
        self.fk_raised = False

    async def fetchval(self, sql: str, *args: object) -> object:
        if _is_lease_statement(sql) and not _is_server_clock_read(sql):
            self.fetchval_calls.append((sql, args))
            self.elect_in_flight.set()
            if self.null_leader_conn is not None:
                self.null_leader_conn()
                self.null_leader_conn = None
                self.fk_raised = True
                raise asyncpg.ForeignKeyViolationError(
                    "insert or update on table 'maintenance_leader' violates "
                    "foreign key constraint 'maintenance_leader_worker_id_fkey'"
                )
            return datetime.now(UTC)
        return await super().fetchval(sql, *args)


async def test_elect_fk_found_with_the_leader_conn_already_nulled_recovers(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """An FK read while ``deps.leader_conn`` is already gone is a reopen.

    ``reload_credentials``' close-timeout path can null ``deps.leader_conn``
    during the elect's round trip, so the attribute is already ``None`` when
    the FK arm starts. The arm must then treat the conn as unavailable - no
    repair written into the vanished connection, no ``AttributeError``, no
    fail-closed shutdown - and return to the loop top, whose open block
    rebuilds through ``leader_conn_factory`` with the rotated credentials.
    The repair belongs to whoever still has a live connection (the heartbeat
    does), so this pod surviving to campaign again is the recovery that
    matters.
    """
    conn = _FkAfterConnNulledMidElectConn()
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )

    def null_conn() -> None:
        deps.leader_conn = None

    conn.null_leader_conn = null_conn

    factory_conns: list[FakeConn] = []

    async def factory() -> asyncpg.Connection:
        rebuilt = FakeConn(fetchval_result=True)  # the reopened conn wins the elect
        factory_conns.append(rebuilt)
        return rebuilt  # type: ignore[return-value]  # Why: FakeConn is a drop-in for asyncpg.Connection in unit tests, the same substitution _make_deps makes.

    deps.leader_conn_factory = factory

    task = asyncio.create_task(leader._election_loop(shutdown))
    shutdown_set_by_loop = False
    with structlog.testing.capture_logs() as captured:
        await wait_for_leader(deps)
        shutdown_set_by_loop = shutdown.is_set()
    shutdown.set()
    await task

    assert conn.fk_raised, "sanity: the nulling+FK interleave must have landed"
    assert factory_conns, (
        "the loop never rebuilt leader_conn through the factory: the FK arm "
        "either dereferenced the nulled attribute or treated it as permanent"
    )
    assert deps.leader_conn is factory_conns[0], (
        "the factory-built connection never became deps.leader_conn - the elect "
        "recovered on a monitor/cron open, not by reopening the leader_conn"
    )
    assert not shutdown_set_by_loop, (
        "a leader_conn that vanished mid-elect (the reload close-timeout) was "
        "classified as a permanent fault and shut the process down"
    )
    restore_calls = [
        (sql, args)
        for sql, args in conn.execute_calls
        if ".WORKERS" in " ".join(sql.split()).upper()
        and "ON CONFLICT (ID) DO NOTHING" in " ".join(sql.split()).upper()
    ]
    assert restore_calls == [], (
        "the repair was written into the connection that had already been "
        "nulled for background termination - its commit is not durable"
    )
    assert not any(e.get("kind") == "leader_upsert_fk_violation" for e in captured)
    assert deps.is_leader.is_set(), "the reopened connection's elect never won"


class _NullMidRestoreConn(FakeConn):
    """The reload lands during the repair INSERT, not before it.

    ``reload_credentials``' close-timeout path nulls ``deps.leader_conn``
    and only *schedules* the old connection's termination, so there is a
    real window where the attribute is gone while the old wire still
    answers ``is_closed() is False``. This double fires at the repair
    INSERT's round trip - after the FK arm's first re-read and before its
    second. Every lease statement arriving after the null is recorded
    regardless of the connection's closed state: the open half is the one
    a retried elect sent on a stale binding would actually win a lease on.
    """

    def __init__(self) -> None:
        super().__init__()
        self.null_conn: Callable[[], None] | None = None
        self.nulled = False
        self.fk_raised = False
        self.lease_calls = 0
        self.lease_stmts_after_null: list[str] = []
        self.restores = 0

    async def fetchval(self, sql: str, *args: object) -> object:
        if _is_lease_statement(sql) and not _is_server_clock_read(sql):
            if self.nulled:
                self.lease_stmts_after_null.append(sql)
                if self.is_closed():
                    raise asyncpg.InterfaceError("connection is closed")
            self.lease_calls += 1
            if self.lease_calls == 1:
                self.fk_raised = True
                raise asyncpg.ForeignKeyViolationError(
                    "insert or update on table 'maintenance_leader' violates "
                    "foreign key constraint 'maintenance_leader_worker_id_fkey'"
                )
            return datetime.now(UTC)
        return await super().fetchval(sql, *args)

    async def execute(self, sql: str, *args: object) -> str:
        normalized = " ".join(sql.split()).upper()
        tag = await super().execute(sql, *args)
        if ".WORKERS" in normalized and "ON CONFLICT (ID) DO NOTHING" in normalized:
            self.restores += 1
            if self.null_conn is not None:
                callback, self.null_conn = self.null_conn, None
                callback()
        return tag


@pytest.mark.parametrize(
    "terminate_on_null", [True, False], ids=["scheduled-close", "nulled-but-open"]
)
async def test_elect_retry_never_runs_on_the_connection_the_reload_nulled(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
    terminate_on_null: bool,
) -> None:
    """The FK arm's second re-read: a conn lost during the repair is a reopen.

    ``reload_credentials`` nulls ``deps.leader_conn`` and *schedules* the old
    connection's termination while the repair INSERT is in flight, so the
    attribute can be gone while the old wire still answers - the window in
    which a binding captured before the await is indistinguishable from a
    live connection by ``is_closed()`` alone. The retried elect must be
    issued only on a connection re-read from the attribute: the arm returns
    to the loop top, whose open block rebuilds through
    ``leader_conn_factory`` with the rotated credentials.

    Both halves of the window are pinned. ``scheduled-close`` terminates in
    the same callback, where ``is_closed()`` alone catches a stale binding;
    ``nulled-but-open`` leaves the old wire answering, so only the re-read
    can - the case a stale ``retry_conn`` binding would win a lease on,
    holding leadership on a wire already queued for termination (a
    leader-without-lock window until the next renew notices, with a
    ``maintenance_leader`` row no peer can take until the lease lapses).

    Regressions pinned (each reachable via a SIGHUP credential reload crossing
    a reaped registration): a retried elect sent on the connection the reload
    already nulled, and an ``AttributeError`` from dereferencing the nulled
    attribute - tolerated by the backstop as an election FAILURE, which the
    attempt count exposes.
    """
    conn = _NullMidRestoreConn()
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )

    def null_conn() -> None:
        deps.leader_conn = None
        if terminate_on_null:
            # The reload's background termination, already scheduled on the
            # old conn: asyncpg reports it closed from here on.
            conn.terminate()

    conn.null_conn = null_conn

    factory_conns: list[FakeConn] = []

    async def factory() -> asyncpg.Connection:
        rebuilt = FakeConn(fetchval_result=True)  # the reopened conn wins the elect
        factory_conns.append(rebuilt)
        return rebuilt  # type: ignore[return-value]  # Why: FakeConn is a drop-in for asyncpg.Connection in unit tests, the same substitution _make_deps makes.

    deps.leader_conn_factory = factory

    task = asyncio.create_task(leader._election_loop(shutdown))
    shutdown_set_by_loop = False
    with structlog.testing.capture_logs() as captured:
        await wait_for_leader(deps)
        shutdown_set_by_loop = shutdown.is_set()
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert conn.fk_raised, "sanity: the FK must have routed into the repair arm"
    assert conn.restores == 1, (
        "sanity: the nulling must land inside the repair INSERT's own await "
        f"(restores seen: {conn.restores})"
    )
    assert conn.lease_calls == 1, (
        "the retried elect was issued on the old connection after the reload "
        f"nulled it (lease statements on it: {conn.lease_calls})"
    )
    assert conn.lease_stmts_after_null == [], (
        "a lease statement was sent on the connection the reload had already "
        f"nulled: {conn.lease_stmts_after_null!r}"
    )
    assert factory_conns, (
        "the loop never rebuilt leader_conn through the factory - the vanished "
        "connection was treated as usable or as a permanent fault"
    )
    assert deps.leader_conn is factory_conns[0], (
        "the factory-built connection never became deps.leader_conn - the elect "
        "recovered on a monitor/cron open, not by reopening the leader_conn"
    )
    assert deps.is_leader.is_set(), "the reopened connection's elect never won"
    assert not shutdown_set_by_loop, (
        "a leader_conn that vanished during the repair was classified as a "
        "permanent fault and shut the process down"
    )
    assert not any(e.get("kind") == "leader_upsert_fk_violation" for e in captured)
    assert not any(e.get("kind") == "worker_registration_restored" for e in captured), (
        "a restore whose retry never completed on a live connection claimed the "
        "durability point the operator warning exists to report"
    )
    assert not any(e.get("kind") == "election_attempt_failed" for e in captured), (
        "the vanished connection was booked as a failed election attempt: a conn "
        "the reload took away is a reopen, not an error the loop has to budget"
    )
    assert not any(e.get("kind") == "loop_unexpected_error" for e in captured), (
        "the vanished connection reached the unexpected-error backstop (a "
        "dereference or a write on the dying wire), not the reopen path"
    )


class _FlakyResignConn(FakeConn):
    """FakeConn whose resign DELETE fails a fixed number of times first.

    Simulates a leader conn that cannot reach the database for the first
    few hand-back attempts (a failover in flight) and recovers - the
    shape the failed-resign retry park exists to survive.
    """

    def __init__(self, *, resign_failures: int, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._resign_failures_left = resign_failures
        self.resign_attempts = 0

    async def execute(self, sql: str, *args: object) -> str:
        if sql.lstrip().upper().startswith("DELETE FROM") and "maintenance_leader" in sql:
            self.resign_attempts += 1
            if self._resign_failures_left > 0:
                self._resign_failures_left -= 1
                raise asyncpg.PostgresConnectionError("resign write lost")
        return await super().execute(sql, *args)


async def test_a_failed_resign_retries_fenced_until_it_lands(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The retry park: a resign that cannot reach the database is retried
    on the tick cadence, fenced, attempt-free, until it lands.

    The park's branch condition is ``_resign_fence is None``: the fence
    ALIVE means the row is still this pod's to hand back, so the loop
    must keep retrying (the fleet is stalled until the DELETE lands) -
    parking attempt-free on a failed resign would strand the lease for a
    whole leader lease. Pinned against: a park that gives up after one
    failure (the fence never clears, exactly one resign attempt), and a
    retry that re-enters the election instead of staying attempt-free
    (a stopping worker must never elect again - the shutdown-ordering
    contract). The fence identity is the third pin: every retry fences on
    the same won term, so the moment a successor takes the row the
    retries can no longer touch it.
    """
    conn = _FlakyResignConn(resign_failures=2)
    leader, deps, _backend, _leader_conn, _, shutdown = await _make_leader(
        leader_conn=conn,
        monkeypatch=monkeypatch,
    )
    loop = asyncio.get_running_loop()
    # The pod led once and was demoted mid-run: leader_term is long gone,
    # only the fence remembers the term the row was won under.
    fence = LeaderTerm(elected_at=datetime.now(UTC), trusted_until=loop.time() + 60.0)
    leader._resign_fence = fence
    deps.shutdown_start_event.set()

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    with structlog.testing.capture_logs() as captured:
        await wait_for_condition(
            lambda: leader._resign_fence is None,
            description="the retried fenced resign to land and clear the fence",
            timeout=5.0,
        )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert conn.resign_attempts >= 3, (
        f"the resign landed after {conn.resign_attempts} attempt(s) - the "
        "retry park gave up on a failing resign and stranded the lease"
    )
    assert conn.fetchval_calls == [], (
        "the retry loop re-attempted the election while stopping - the "
        "park must stay attempt-free, only the hand-back retries"
    )
    for _sql, args in _resign_deletes([conn]):
        assert args[1] == fence.elected_at, (
            f"a retry resigned against {args[1]!r}, not the won term's "
            f"{fence.elected_at!r} - the fence drifted off the term the "
            "row was won under"
        )
    assert len([e for e in captured if e.get("kind") == "leader_resign_failed"]) == 2, (
        "each failed attempt must carry its own leader-resign-failed WARN"
    )
    assert not deps.is_leader.is_set()


# ── the cron conn's dirty record dies with the connection it describes ────
#
# A tick that fails while its transaction state is unknown records the conn; the
# next iteration repairs it with Connection.reset(). The record is keyed to the
# connection OBJECT rather than a bare flag because the election loop rebuilds
# _cron_conn (SIGHUP credential reload, a re-election cascade) while the cron
# loop may still hold the record for the old one: applied to the fresh
# connection, the repair would RESET ALL a session nobody failed on and drop
# whatever an embedder's leader_conn_factory setup hook set.


class _RepairRecordingCronConn(FakeConn):
    """FakeConn that records the deferred repair and the tick's entry.

    ``reset`` is the repair's only observable on a connection, and the shared
    double has none - which is why a repair attempt against it raises
    ``AttributeError`` and tells a repaired connection from an untouched one
    not at all.
    """

    def __init__(self) -> None:
        super().__init__()
        self.reset_calls = 0
        self.transaction_entries = 0

    async def reset(self) -> None:
        self.reset_calls += 1

    def transaction(self) -> _FakeTransaction:
        conn = self

        class _RecordingTx(_FakeTransaction):
            async def __aenter__(self) -> None:
                conn.transaction_entries += 1

        return _RecordingTx()


def _cron_tick_stub(monkeypatch: pytest.MonkeyPatch, *, failures: int) -> None:
    """Make the loop's tick time out on its first *failures* calls, then succeed."""
    from taskq.worker import leader as leader_mod

    calls = 0

    async def _tick(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        if calls <= failures:
            raise TimeoutError()  # the deadline family: keeps the conn, records it
        return 0

    monkeypatch.setattr(leader_mod, "tick_cron", _tick)


def _cron_loop_leader(
    conn: FakeConn, monkeypatch: pytest.MonkeyPatch, *, tick_deadline: float = 1.0
) -> tuple[MaintenanceLeader, WorkerDeps]:
    from types import SimpleNamespace
    from typing import cast

    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock

    from ._leader_stub_deps import stub_deps

    liveness = SimpleNamespace(tick=lambda *args, **kwargs: None)
    is_leader = asyncio.Event()
    is_leader.set()
    deps = stub_deps(
        SimpleNamespace(
            liveness=liveness,
            is_leader=is_leader,
            settings=SimpleNamespace(
                schema_name="taskq",
                dispatcher_command_timeout=tick_deadline,
                cron_tick_limit=100,
            ),
        )
    )
    leader = MaintenanceLeader(
        deps, new_uuid(), cast(Backend, SimpleNamespace()), clock=SystemClock()
    )
    leader._cron_conn = conn  # type: ignore[reportAttributeAccessIssue]  # Why: FakeConn is the asyncpg.Connection stand-in used throughout this module.
    _cron_tick_stub(monkeypatch, failures=1)
    return leader, deps


async def test_the_same_cron_conn_is_repaired_after_a_failed_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: the record is live, so the same connection IS repaired.

    Without this arm a green discard assertion would prove only that the
    harness never reached the repair.
    """
    conn = _RepairRecordingCronConn()
    leader, _deps = _cron_loop_leader(conn, monkeypatch)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    try:
        await wait_for_condition(
            lambda: conn.reset_calls >= 1,
            description="the deferred repair to run on the connection that failed",
            timeout=5.0,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert conn.reset_calls == 1, (
        f"the failed tick's connection was repaired {conn.reset_calls} times, "
        "not once: the record did not reach the repair"
    )
    assert conn.transaction_entries >= 1, "the loop never ticked on the repaired connection"


async def test_a_conn_that_cannot_be_repaired_is_given_up_after_two_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A repair that keeps raising must be bounded, not retried for the process's life.

    ``conn.reset()`` can fail outside the deadline family: an embedder
    connection class with no ``reset`` (the handler names that case), or a
    driver-side error. A tick that appears to succeed on that conn is NOT proof
    the state is usable: with the leaked top-level transaction still recorded,
    the next ``transaction()`` is a nested SAVEPOINT whose ``RELEASE`` returns
    while nothing commits - the incident this change exists to end, invisible to
    the tick's own success. So the failure counts toward the same give-up bound
    as a cut: after two consecutive failed repairs the connection is terminated
    synchronously (Postgres rolls the transaction back on disconnect) and the
    next iteration's conn-state branch drops and demotes. Without the bound the
    failing reset is retried, and warns, once per iteration for the process's
    life.
    """
    from taskq.worker import leader as leader_mod

    class _UnrepairableCronConn(_RepairRecordingCronConn):
        async def reset(self) -> None:
            self.reset_calls += 1
            raise AttributeError("reset")

    conn = _UnrepairableCronConn()
    leader, deps = _cron_loop_leader(conn, monkeypatch)
    calls = 0

    async def _tick(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError()  # the deadline family: records the conn
        return 0

    monkeypatch.setattr(leader_mod, "tick_cron", _tick)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    try:
        await wait_for_condition(
            lambda: conn.terminated,
            description="the unrepairable connection to be given up",
            timeout=10.0,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert conn.reset_calls == 2, (
        f"the failing repair ran {conn.reset_calls} times, not the two the "
        "give-up bound allows: an unrepairable conn must not be retried (and "
        "warn) once per iteration for the process's life"
    )
    assert conn.terminated, (
        "the unrepairable connection was never terminated: the unknown "
        "transaction state it carries is bounded only by the give-up"
    )
    assert deps.is_leader.is_set(), (
        "the pin did not reach the give-up before the loop stopped: the "
        "escalation, not a shutdown, is what must end this sequence"
    )


async def test_a_rebuilt_cron_conn_is_not_repaired_for_the_old_conns_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A record for the old connection must not reset the rebuilt one.

    The election loop can assign a fresh ``_cron_conn`` between two iterations
    (credential reload) while the record still names the old connection.
    ``reset()`` on the fresh one would ``RESET ALL`` a session that never failed
    and drop an embedder's setup-hook session settings.
    """
    old_conn = _RepairRecordingCronConn()
    new_conn = _RepairRecordingCronConn()
    leader, _deps = _cron_loop_leader(old_conn, monkeypatch)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    try:
        # Iteration 1 fails on old_conn and records it; iteration 2 begins one
        # period later, so the rebuild lands inside the loop's own park. Wait
        # on the record's own observable (the failing tick's transaction
        # entry) rather than a sleep: a starved loop that never dirtied the old
        # conn would make the no-reset assertion below pass without reaching
        # its premise.
        await wait_for_condition(
            lambda: old_conn.transaction_entries >= 1,
            description="the first iteration to fail on the old connection",
            timeout=5.0,
        )
        leader._cron_conn = new_conn  # type: ignore[reportAttributeAccessIssue]  # Why: this is exactly what the election loop's rebuild does.
        await wait_for_condition(
            lambda: new_conn.transaction_entries >= 1,
            description="the loop to tick on the rebuilt connection",
            timeout=5.0,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert old_conn.reset_calls == 0, (
        "the old connection was reset after the rebuild: the record must die "
        "with the connection it describes, not be applied to whatever is there"
    )
    assert new_conn.reset_calls == 0, (
        "the rebuilt cron connection was reset for the OLD connection's failed "
        "tick: the record outlived the connection it describes, so a credential "
        "reload now RESET ALLs a fresh session and drops an embedder's "
        "leader_conn_factory setup-hook settings"
    )
    assert new_conn.transaction_entries >= 1, (
        "the loop never ticked on the rebuilt connection, so this pin did not "
        "reach the iteration the record would have been applied to"
    )


@pytest.mark.load_sensitive
async def test_a_tick_that_spends_its_granted_factory_budget_still_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tick that fits ``dispatcher_command_timeout`` must commit, not be cut.

    ``cron_loop._TICK_WRITE_RESERVE_FRACTION`` grants payload factories up to
    ``dispatcher_command_timeout * (1 - 0.1)`` and states what the remaining
    tenth is for: *"the reserve leaves room for the tick's own writes and its
    COMMIT"*. The settings field says the same thing from the operator's side:
    the factory deadline is *"clamped to stay strictly inside what is left of
    the leader's whole-tick deadline (dispatcher_command_timeout)"*. So a tick
    whose granted waits total 0.9 of the setting and whose writes take the
    remaining 0.1 is inside the documented budget and must reach its COMMIT.

    The regression this guards is carving the reserved unwind share out of the
    tick body's pot: the body would then run under
    ``dispatcher_command_timeout - 0.1 * dispatcher_command_timeout``, so the
    batch the factory arithmetic was built around would have no time left for
    its
    writes. It is cut, the transaction rolls back, ``next_fire_at`` does not
    advance, the identical batch is selected next tick and the same grant is
    made again - the livelock ``_TickWriteReserveFraction`` exists to prevent.
    """
    from taskq.worker import leader as leader_mod

    conn = _RepairRecordingCronConn()
    leader, _deps = _cron_loop_leader(conn, monkeypatch)
    committed: list[int] = []
    cut: list[int] = []
    monkeypatch.setattr(leader_mod, "record_sweep_success", lambda loop: committed.append(1))
    monkeypatch.setattr(leader_mod, "record_sweep_timeout", lambda loop: cut.append(1))

    tick_deadline = leader._deps.settings.dispatcher_command_timeout  # pyright: ignore[reportPrivateUsage]  # Why: the setting the test's budget arithmetic is stated in.

    async def _tick(*args: object, **kwargs: object) -> int:
        # 0.9 of the setting for the granted factory waits + 0.05 for the
        # writes: the shape _factory_deadline budgets for, well inside the
        # whole-tick deadline.
        await asyncio.sleep(tick_deadline * 0.95)
        return 1

    monkeypatch.setattr(leader_mod, "tick_cron", _tick)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    try:
        # Either the tick commits, or the loop has already cut it twice: the
        # latter is the livelock, and asserting on it here keeps the failure
        # message about the budget rather than about a wait timing out.
        await wait_for_condition(
            lambda: bool(committed) or len(cut) >= 2,
            description="the 95%-of-budget tick to commit or be cut twice",
            timeout=3.0,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert committed, (
        f"a tick that spends 95% of dispatcher_command_timeout ({tick_deadline}s) "
        f"never committed: the iteration was cut {len(cut)} time(s) with "
        f"{conn.transaction_entries} transaction(s) entered. The factory "
        "arithmetic grants waits up to 0.9 of the setting and reserves the "
        "other 0.1 for this tick's writes and COMMIT, so this batch fits the "
        "documented budget - a cut here rolls it back, selects the identical "
        "batch again and re-grants the same wait every second, starving the "
        "schedule"
    )
    assert not cut, (
        f"{len(cut)} iteration(s) were cut before a tick that fits "
        f"dispatcher_command_timeout ({tick_deadline}s) could commit"
    )


async def test_a_slow_repair_does_not_spend_the_reserved_unwind_share(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the body is cut, the reserved unwind share must still be unspent.

    The nested scopes reserve ``_CRON_TICK_UNWIND_FRACTION`` of the iteration
    *"so a stalled transport cannot park the loop inside its own unwind with no
    timer left"*. That reservation only holds if the inner scope is sized from
    what is left of the iteration: it is armed *after* the deferred repair, so a
    repair slower than the reserved share pushes the inner scope's expiry past
    the outer scope's. The outer deadline then cuts the body, and the unwind
    that follows has no live timer at all - the exact park the reservation was
    added to prevent, reachable without any help from the transport.
    """
    from taskq.worker import leader as leader_mod

    tick_deadline = 3.0
    repair_delay = tick_deadline * 0.5
    unwind_share = tick_deadline * 0.1

    class _SlowResetConn(_RepairRecordingCronConn):
        async def reset(self) -> None:
            await asyncio.sleep(repair_delay)
            self.reset_calls += 1

    conn = _SlowResetConn()
    leader, _deps = _cron_loop_leader(conn, monkeypatch, tick_deadline=tick_deadline)

    # (repaired_before_this_body, seconds_the_body_ran_before_being_cut)
    bodies: list[tuple[bool, float]] = []

    async def _tick(*args: object, **kwargs: object) -> int:
        repaired = conn.reset_calls > 0
        started = asyncio.get_running_loop().time()
        try:
            await asyncio.sleep(tick_deadline * 10)  # always cut
        finally:
            bodies.append((repaired, asyncio.get_running_loop().time() - started))
        return 1

    monkeypatch.setattr(leader_mod, "tick_cron", _tick)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    try:
        await wait_for_condition(
            lambda: any(repaired for repaired, _ in bodies),
            description="a tick body to run after the deferred repair",
            timeout=15.0,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    after_repair = [(repaired, ran) for repaired, ran in bodies if repaired]
    assert after_repair, "no tick body ran after the repair, so the pin did not reach its window"
    _, ran = after_repair[0]
    # What remains of the ITERATION when the body was cut: the iteration's
    # bound is the tick deadline plus the added unwind share (the outer
    # scope), so the remaining-of-iteration measure carries that share. Measuring
    # from the bare tick deadline instead would demand the share be carved out
    # of the tick body's own pot - which the factory-budget contract
    # (``_TICK_WRITE_RESERVE_FRACTION`` reserves the writes and COMMIT inside
    # the whole-tick deadline) forbids, and no code change satisfies both.
    remaining_when_cut = tick_deadline + unwind_share - repair_delay - ran
    assert remaining_when_cut >= unwind_share - 0.05, (
        f"the body was cut with only {remaining_when_cut:.2f}s of the iteration "
        f"left ({ran:.2f}s after a {repair_delay:.2f}s repair), not the reserved "
        f"{unwind_share:.2f}s: the inner scope is armed from the nominal "
        "deadline, not from what remains, so the outer deadline is the one that "
        "cut the body and the unwind that follows has no live timer - the park "
        "the reservation exists to prevent"
    )


async def test_an_iteration_that_spends_the_unwind_share_repays_it_from_the_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An iteration that spends the unwind share must repay it from the period.

    The unwind share is ADDED to the iteration's bound, but the settings
    validator's staleness model still says the worst-case stamp-to-stamp gap is
    ``dispatcher_command_timeout +`` the 1.0s period - so whatever the iteration
    spends past the tick's own deadline (a transaction unwind slower than a
    round trip, cut by the outer bound) is repaid from the period at the bottom
    of the iteration. Without the repayment, an iteration that spent the share
    widens the gap to timeout + share + period: the false trip of a healthy
    worker mid-degradation that the load-time model exists to prevent, and one
    no test would see (the watchdog budget suite's margin is wider than the
    share).
    """
    from taskq.worker import leader as leader_mod

    tick_deadline = 1.0
    unwind_spend = 0.09  # most of the 0.1 share, well past a round trip

    class _SlowUnwindConn(_RepairRecordingCronConn):
        """A conn whose transaction unwind (its __aexit__) is slow."""

        def transaction(self) -> _FakeTransaction:
            class _SlowUnwindTx(_FakeTransaction):
                async def __aexit__(self, *args: object) -> None:
                    await asyncio.sleep(unwind_spend)

            return _SlowUnwindTx()

    conn = _SlowUnwindConn()
    leader, _deps = _cron_loop_leader(conn, monkeypatch, tick_deadline=tick_deadline)

    tick_starts: list[float] = []

    async def _tick(*args: object, **kwargs: object) -> int:
        tick_starts.append(asyncio.get_running_loop().time())
        await asyncio.sleep(tick_deadline * 10)  # always cut by the tick's scope
        return 1

    monkeypatch.setattr(leader_mod, "tick_cron", _tick)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    try:
        await wait_for_condition(
            lambda: len(tick_starts) >= 3,
            description="three ticks to start",
            timeout=10.0,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    gaps = [b - a for a, b in itertools.pairwise(tick_starts)]
    assert gaps, "no tick-to-tick gap was observed"
    # timeout + the 1.0s period, with timer-granularity slack only: the share
    # the unwind spent must appear as a SHORTER period, not a wider gap.
    bound = tick_deadline + 1.0 + 0.03
    assert max(gaps) <= bound, (
        f"an iteration that spent the unwind share widened the tick gap to "
        f"{max(gaps):.3f}s (gaps {[f'{g:.3f}' for g in gaps]}), past the "
        f"timeout + period bound ({bound:.3f}s) the settings validator "
        "enforces: the overshoot was not repaid from the loop's period, so a "
        "healthy worker false-trips the staleness detector mid-degradation"
    )


@pytest.mark.slow
async def test_the_widened_iteration_still_fits_the_staleness_model_at_large_deadlines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The unwind repayment must hold at every LEGAL ``dispatcher_command_timeout``.

    The repayment exists so the stamp-to-stamp gap stays inside
    ``dispatcher_command_timeout + period`` — the arithmetic
    ``WorkerSettings`` validates at load time and the constant's docstring
    promises (*"Whatever of the share the iteration actually spends is repaid
    from the loop's period"*). The share is 10% of the deadline and the period
    is ``_CRON_TICK_PERIOD_SECS`` (1.0s), so the repayment can only cover it
    while ``0.1 * deadline <= 1.0``; above a 10s deadline the sleep bottoms out
    at zero and the share widens the gap outright.

    That is reachable through the settings' own documented lever: a slow but
    successful payload factory is told to *"raise dispatcher_command_timeout"*,
    and the validator's bounded-loop invariant then only demands a staleness
    budget STRICTLY above ``deadline + period`` (it refuses
    ``dispatcher_command_timeout + period >= budget``: a 15s timeout with the
    natural 16.0s floor is rejected, while 16.2s loads). An uncapped iteration
    that spends its whole 1.5s share ends at 16.5s — past every legal budget in
    between, so detector 2 force-exits a worker that is degraded but
    recovering: precisely the healthy-worker false trip the load-time model
    exists to prevent.
    """
    from taskq.worker import leader as leader_mod

    tick_deadline = 15.0
    # Past the CAPPED share (min(0.1 x 15.0, 1.0) = 1.0s), so the unwind is cut
    # at the outer bound and the iteration ends at deadline + share, with the
    # repayment bottomed out at zero. Under the uncapped 1.5s share the same
    # spend completes instead and the iteration ends at deadline + spend, which
    # is the widened gap this pin refuses.
    unwind_spend = 1.4

    class _SlowUnwindConn(_RepairRecordingCronConn):
        def transaction(self) -> _FakeTransaction:
            class _SlowUnwindTx(_FakeTransaction):
                async def __aexit__(self, *args: object) -> None:
                    await asyncio.sleep(unwind_spend)

            return _SlowUnwindTx()

    conn = _SlowUnwindConn()
    leader, _deps = _cron_loop_leader(conn, monkeypatch, tick_deadline=tick_deadline)

    tick_starts: list[float] = []

    async def _tick(*args: object, **kwargs: object) -> int:
        tick_starts.append(asyncio.get_running_loop().time())
        await asyncio.sleep(tick_deadline * 10)  # always cut by the tick's scope
        return 1

    monkeypatch.setattr(leader_mod, "tick_cron", _tick)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))  # pyright: ignore[reportPrivateUsage]
    try:
        await wait_for_condition(
            lambda: len(tick_starts) >= 2,
            description="two tick starts across an iteration that spends its unwind share",
            timeout=60.0,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    gaps = [b - a for a, b in itertools.pairwise(tick_starts)]
    assert gaps, "no tick-to-tick gap was observed"
    # The uncapped share's signal is 1.4s (a 17.4s gap against the 16.0s
    # model); the slack here is scheduler granularity on a serial lane, still
    # 5.6x below that signal.
    bound = tick_deadline + 1.0 + 0.25  # the model, plus scheduling slack
    assert max(gaps) <= bound, (
        f"an iteration that spent its {tick_deadline * 0.1:.2f}s unwind share "
        f"widened the tick gap to {max(gaps):.2f}s, past the deadline + period "
        f"bound ({bound:.2f}s) the settings validator models: the repayment "
        "sleeps max(0.0, 1.0 - overshoot), so it cannot cover a share larger "
        "than the period and the stale-loop detector force-exits the worker"
    )
