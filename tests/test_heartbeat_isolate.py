"""Unit tests for isolate_self - pure-Python, no PG required."""

import asyncio
from typing import cast

import pytest

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import isolate_self
from tests.conftest import _FakePool

# ── Test helpers ─────────────────────────────────────────────────────────


class FakeConn:
    """Lightweight asyncpg.Connection stand-in for isolate_self tests."""

    def __init__(
        self,
        *,
        fetch_rows: list[dict[str, object]] | None = None,
        fail_execute_with: BaseException | None = None,
    ) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_calls = 0
        self._fetch_rows = fetch_rows or []
        self._fail_execute_with = fail_execute_with
        self._execute_count = 0
        # Hang-gate + terminate tracking for bounded-close tests (mirrors the
        # _FakeConn conventions in tests/test_worker_deps_teardown.py):
        # clear close_wait to make close() block forever (dead PG).
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.terminated = False

    async def execute(self, sql: str, *args: object) -> str:
        self._execute_count += 1
        if self._fail_execute_with is not None:
            raise self._fail_execute_with
        self.execute_calls.append((sql, args))
        pieces = sql.rsplit(" ", 1)
        return f"{pieces[0]} 1"

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
        """The guarded arbiter UPDATE rides fetchrow (its RETURNING is the
        standing-claim fence's source of truth): the row's own attempt and
        stamp for a job this fake still holds, None when it does not - the
        lost-race shape the rowcount-0 path serves."""
        self._execute_count += 1
        if self._fail_execute_with is not None:
            raise self._fail_execute_with
        self.execute_calls.append((sql, args))
        if "RETURNING j.attempt" in sql:
            job_id = args[0]
            for row in self._fetch_rows:
                if row["id"] == job_id:
                    # The RETURNING now carries j.status too - the
                    # arbiter's CASE outcome is the classification's
                    # source of truth (the snapshot's attempt can be
                    # stale behind a refund that committed in the
                    # window). The fake emulates the CASE faithfully:
                    # operator cancel first, then the budget arms.
                    if row["cancel_phase"] != 0:
                        status = "cancelled"
                    elif row["retry_kind"] != "non_retryable" and (
                        row["retry_kind"] == "indefinite"
                        or int(row["attempt"]) < int(row["max_attempts"])  # type: ignore[operator]  # Why: FakeConn's rows are untyped dicts (pyright: object); the values are ints by construction.
                    ):
                        status = "pending"
                    else:
                        status = "crashed"
                    return {
                        "attempt": row["attempt"],
                        "started_at": row["started_at"],
                        "status": status,
                    }
            return None
        return None

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls = getattr(self, "fetch_calls", [])
        self.fetch_calls.append((sql, args))
        return list(self._fetch_rows)

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()

    def terminate(self) -> None:
        self.terminated = True
        self.close_wait.set()  # aborts any in-flight close() wait

    def transaction(self) -> "_FakeTransaction":
        self.transaction_calls += 1
        return _FakeTransaction()


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


def _worker_settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": pg_dsn}
    for key, value in overrides.items():
        if not key.startswith("TASKQ_"):
            data[f"TASKQ_{key}"] = value
        else:
            data[key] = value
    return WorkerSettings.load_from_dict(data)


def _make_deps(
    *,
    lock_lease: float = 60.0,
    heartbeat_interval: float = 10.0,
) -> WorkerDeps:
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        LOCK_LEASE=str(lock_lease),
        HEARTBEAT_INTERVAL=str(heartbeat_interval),
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    return deps


# ── Test: isolate_self opens a fresh asyncpg.connect ─────────────────────


async def test_isolate_self_writes_reclaim_event_rows() -> None:
    """Every job isolate_self transitions rides the crash-reclaim outbox
    channel: ONE batched job_events insert (the sweep's event writer
    shape) whose rows carry reason='lock_expired' (the channel key
    poll_reclaim_events tails) and cause='isolate_self' (which origin
    fired), with the arm's to_state. Without it, an isolate-reclaimed
    job is invisible to poll_reclaim_events and watch_reclaims while
    the row and the attempt history both say it is long gone."""

    job_rows: list[dict[str, object]] = [
        {
            "id": new_uuid(),
            "attempt": 1,
            "started_at": "2025-01-01T00:00:00Z",
            "max_attempts": 3,
            "retry_kind": "transient",
            "cancel_phase": 0,
        },
        {
            "id": new_uuid(),
            "attempt": 2,
            "started_at": "2025-01-01T00:00:01Z",
            "max_attempts": 2,
            "retry_kind": "non_retryable",
            "cancel_phase": 0,
        },
    ]

    conn = FakeConn(fetch_rows=job_rows)

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        return conn

    import json

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign] # Why: patching asyncpg.connect for unit test; restored in finally.
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)

        assert shutdown.is_set()
        event_calls = [(sql, args) for sql, args in conn.execute_calls if "job_events" in sql]
        assert len(event_calls) == 1, (
            f"isolate_self must batch its reclaim events into one insert, got {len(event_calls)}"
        )
        _sql, args = event_calls[0]
        job_ids, details, kind = args[0], args[1], args[2]
        assert kind == "state_change"
        assert list(job_ids) == [row["id"] for row in job_rows]  # type: ignore[arg-type] # Why: FakeConn records untyped tuples; the insert's $1 is the uuid[] binding.
        detail_json = cast("tuple[object, ...]", details)
        parsed = [json.loads(str(d)) for d in detail_json]
        # Arm classification mirrors the UPDATE's CASE order: the
        # transient job with budget re-pends, the non_retryable one
        # crashes.
        assert parsed[0]["to_state"] == "pending"
        assert parsed[1]["to_state"] == "crashed"
        for detail in parsed:
            assert detail["from_state"] == "running"
            assert detail["reason"] == "lock_expired"
            assert detail["cause"] == "isolate_self"
            assert detail["worker_id"]
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign] # Why: patching asyncpg.connect for unit test; restored in finally.


async def test_isolate_self_classifies_from_the_arbiter_not_the_snapshot() -> None:
    """The classification reads the ARBITER's RETURNING, never the SELECT
    snapshot: a refund that committed in the SELECT->arbiter window
    de-charges and un-stamps the row before the arbiter takes its lock,
    and the arbiter's own budget CASE then re-pends it - a classification
    from the snapshot would call that row 'crashed' (its snapshot attempt
    was spent) and fabricate a verdict the ledger does not carry: the
    reclaim event's to_state would say crashed above a jobs row that says
    pending, and watch_reclaims consumers would trust the lie. The
    arbiter is the trustworthy read - the same doctrine the attempt
    row's fence (the NULL-stamp -> no attempt row) runs on; here the
    pin holds the CLASSIFICATION to it."""

    snapshot_row: dict[str, object] = {
        "id": new_uuid(),
        # The SNAPSHOT the SELECT hands out: the attempt budget spent -
        # the stale view the old classification crashed the row from.
        "attempt": 2,
        "started_at": "2025-01-01T00:00:00Z",
        "max_attempts": 2,
        "retry_kind": "transient",
        "cancel_phase": 0,
    }
    # The ARBITER's RETURNING: the refund committed in the window (the
    # charge de-charged: attempt 2 -> 1; the stamp un-stamped), the
    # arbiter re-evaluated the budget on the live row and re-pended it.
    arbiter_returning: dict[str, object] = {
        "attempt": 1,
        "started_at": None,
        "status": "pending",
    }

    class RefundedUnderneathConn(FakeConn):
        async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
            if "RETURNING j.attempt" in sql:
                return dict(arbiter_returning)
            return await super().fetchrow(sql, *args)

    conn = RefundedUnderneathConn(fetch_rows=[snapshot_row])

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object] | None = None,
    ) -> RefundedUnderneathConn:
        return conn

    import json

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)

        assert shutdown.is_set()
        # The event carries the ARBITER's verdict: to_state pending - not
        # the snapshot's 'crashed'.
        event_calls = [(sql, args) for sql, args in conn.execute_calls if "job_events" in sql]
        assert len(event_calls) == 1, (
            f"the reclaim event must land exactly once, got {len(event_calls)}"
        )
        _sql, args = event_calls[0]
        details = cast("tuple[object, ...]", args[1])
        detail = json.loads(str(details[0]))
        assert detail["to_state"] == "pending", (
            f"the classification must read the arbiter's RETURNING "
            f"(status='pending' behind the refund), got to_state="
            f"{detail['to_state']!r} - a fabricated verdict the ledger "
            "does not carry"
        )
        # The fence's other half stands with it: the re-pended row's
        # un-stamped charge writes NO attempt row (nothing was executed
        # for the isolate to charge).
        attempt_inserts = [(sql, args) for sql, args in conn.execute_calls if "job_attempts" in sql]
        assert attempt_inserts == [], (
            f"the refunded row's isolate must not mint an attempt row, got {len(attempt_inserts)}"
        )
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


async def test_isolate_self_opens_fresh_connect() -> None:
    """isolate_self opens a fresh asyncpg.connect() - NOT the heartbeat pool."""

    connect_calls: list[tuple[str, float]] = []

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        connect_calls.append((dsn, timeout))
        return FakeConn()

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign] # Why: patching asyncpg.connect for unit test; restored in finally.
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)
        assert len(connect_calls) == 1
        assert connect_calls[0][1] == 5.0
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: shutdown.set() called even when connect fails ──────────────────


async def test_isolate_self_shutdown_even_on_connect_failure() -> None:
    """isolate_self calls shutdown.set() even when asyncpg.connect() raises."""

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        raise OSError("connection refused")

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)
        assert shutdown.is_set()
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: isolate_self writes one AttemptRow per running job ─────────────


async def test_isolate_self_writes_attempt_row_per_job() -> None:
    """isolate_self INSERTs one AttemptRow per running job with correct fields."""

    job_rows: list[dict[str, object]] = [
        {
            "id": new_uuid(),
            "attempt": 1,
            "started_at": "2025-01-01T00:00:00Z",
            "max_attempts": 3,
            "retry_kind": "transient",
            "cancel_phase": 0,
        },
        {
            "id": new_uuid(),
            "attempt": 2,
            "started_at": "2025-01-01T00:00:01Z",
            "max_attempts": 2,
            "retry_kind": "non_retryable",
            "cancel_phase": 0,
        },
    ]

    conn = FakeConn(fetch_rows=job_rows)

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        return conn

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        worker_id = new_uuid()
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, worker_id, shutdown)

        assert shutdown.is_set()
        # Match on the target table, not the statement's first word: the
        # attempt INSERT is now a WITH holder ... INSERT ... statement.
        insert_calls = [(sql, args) for sql, args in conn.execute_calls if "job_attempts" in sql]
        assert len(insert_calls) == 2

        for i, (_sql, args) in enumerate(insert_calls):
            assert args[0] == job_rows[i]["id"]
            assert args[1] == job_rows[i]["attempt"]
            assert args[3] == "crashed"
            assert args[4] == "HeartbeatLost"
            assert args[8] == worker_id
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: isolate_self honours CASE shape ──────────────────────────


async def test_isolate_self_honours_fr12_case_shape() -> None:
    """isolate_self sends the exact disposition UPDATE SQL."""
    runner = None

    class StubConn(FakeConn):
        async def execute(self, sql: str, *args: object) -> str:
            nonlocal runner
            if "SET status = CASE" in sql:
                runner = sql
            return "UPDATE 1"

        async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
            nonlocal runner
            if "SET status = CASE" in sql:
                # The arbiter rides fetchrow (its RETURNING is the
                # standing-claim fence's source of truth): the row the
                # stub's fetch hands out, as the winning transition's
                # RETURNING view.
                runner = sql
                return {
                    "attempt": 0,
                    "started_at": "2025-01-01T00:00:00Z",
                    "status": "pending",
                }
            return await super().fetchrow(sql, *args)

        async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
            return [
                {
                    "id": new_uuid(),
                    "attempt": 0,
                    "started_at": "2025-01-01T00:00:00Z",
                    "max_attempts": 3,
                    "retry_kind": "transient",
                    "cancel_phase": 0,
                }
            ]

    conn = StubConn()

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> StubConn:
        return conn

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        deps = _make_deps()
        shutdown = asyncio.Event()
        await isolate_self(deps, new_uuid(), shutdown)
        assert runner is not None
        # The has-budget predicate: 'indefinite' has no attempt ceiling
        # (its schedule_to_close deadline is its budget), every other
        # kind is bounded by max_attempts, and 'non_retryable' has no
        # second attempt at all - see _sweeps._RECLAIM_HAS_BUDGET_SQL,
        # which this statement shares verbatim.
        assert "j.retry_kind = 'indefinite'" in runner
        assert "j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'" in runner
        # The reclaim delay is derived from the row's own stamped
        # RetryPolicy curve (retry_base_seconds/retry_cap_seconds/
        # retry_backoff/retry_jitter), not a hardcoded flat interval:
        # a crash/heartbeat reclaim reschedules on the same curve an
        # application-level failure would.
        assert "clock_timestamp() +" in runner
        assert "j.retry_base_seconds" in runner
        assert "j.retry_cap_seconds" in runner
        assert "j.retry_backoff" in runner
        assert "j.retry_jitter" in runner
        assert "interval '5 seconds'" not in runner, (
            "isolate_self still stamps a hardcoded flat reclaim delay "
            "instead of deriving it from the job's own RetryPolicy curve"
        )
        assert "WHERE j.id = $1" in runner
        assert "j.locked_by_worker = $2" in runner
        # pins: operator intent outranks retry budget. The cancel
        # arm is evaluated BEFORE the budget arm; the cancel columns are
        # preserved on the arm that honours them; the crashed arm
        # self-describes on the job row (shape-mirror of the sweep's
        # WorkerCrashed stamp, with the intentionally distinct class).
        assert "WHEN j.cancel_phase != 0" in runner
        assert runner.index("WHEN j.cancel_phase != 0") < runner.index(
            "WHEN (j.retry_kind = 'indefinite'"
        ), (
            "the cancel arm must be evaluated before the budget arm: "
            "budget-first re-pends a cancel-in-flight row and wipes the "
            "operator's cancel"
        )
        assert (
            "cancel_phase = CASE WHEN j.cancel_phase != 0 THEN j.cancel_phase ELSE 0 END" in runner
        ), "the cancel arm must preserve the phase as the audit trail"
        assert "THEN 'HeartbeatLost'" in runner, (
            "the isolate's crashed arm must self-describe on the job row, "
            "mirroring the sweep's crashed-arm stamp in shape"
        )
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]


# ── Test: asyncio.shield on terminal writes ───────────────────────────────


async def test_isolate_self_shields_terminal_writes() -> None:
    """Terminal writes in isolate_self survive task cancellation via asyncio.shield.

    Verifies that the transaction block (fetch + update + insert) passes
    through asyncio.shield, so mid-flight cancellation cannot strand
    PG in an inconsistent state.
    """
    shield_calls: list[object] = []
    _real_shield = asyncio.shield

    async def _tracking_shield(coro: object) -> object:
        shield_calls.append(coro)
        return await _real_shield(coro)  # type: ignore[arg-type] # Why: coro wraps _inner; passthrough to real shield.

    import taskq.worker.heartbeat as hb_mod

    hb_mod.asyncio.shield = _tracking_shield  # type: ignore[method-assign]
    try:
        conn = FakeConn(
            fetch_rows=[
                {
                    "id": new_uuid(),
                    "attempt": 0,
                    "started_at": "2025-01-01T00:00:00Z",
                    "max_attempts": 3,
                    "retry_kind": "transient",
                }
            ]
        )

        async def fake_connect(
            dsn: str,
            *,
            timeout: float,
            command_timeout: float | None = None,
            connection_class: type[object]
            | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
        ) -> FakeConn:
            return conn

        import asyncpg as apg

        orig_pg_connect = apg.connect
        apg.connect = fake_connect  # type: ignore[method-assign]
        try:
            deps = _make_deps()
            shutdown = asyncio.Event()
            await isolate_self(deps, new_uuid(), shutdown)
            assert len(shield_calls) == 1
            assert shutdown.is_set()
        finally:
            apg.connect = orig_pg_connect  # type: ignore[method-assign]
    finally:
        hb_mod.asyncio.shield = _real_shield  # type: ignore[method-assign]


# ── Bounded conn close in the finally path ──────────────────────────────
#
# isolate_self only runs when PG is already suspected dead (heartbeat
# failures exceeded); its ``finally: await conn.close()`` could then block
# indefinitely on exactly the dead PG that triggered it, wedging the
# worker's shutdown signalling. These tests pin the bounded-close
# discipline (asyncio.wait_for + terminate on timeout) applied via
# ``close_conn_bounded``; the shrink seam is the same module-global
# monkeypatch convention as tests/test_worker_deps_teardown.py.


async def test_isolate_self_terminates_hung_conn_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung conn.close() in isolate_self's finally is terminated after the
    bounded timeout; isolate_self still completes and signals shutdown."""
    import taskq.worker.heartbeat as hb_mod

    monkeypatch.setattr(hb_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    conn = FakeConn()
    conn.close_wait.clear()  # close() blocks forever from now on

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        return conn

    import asyncpg as apg

    monkeypatch.setattr(apg, "connect", fake_connect)  # type: ignore[method-assign]
    deps = _make_deps()
    shutdown = asyncio.Event()
    # Why the outer timeout: pre-fix the finally awaited conn.close()
    # unbounded, so the RED state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        await isolate_self(deps, new_uuid(), shutdown)

    assert conn.terminated is True
    assert conn.close_calls == 1
    assert shutdown.is_set()


async def test_isolate_self_fast_close_not_terminated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Healthy close(): the isolate-self conn is closed once and never
    terminated. Pins the no-regression behaviour (passes pre/post-fix)."""
    conn = FakeConn()

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        return conn

    import asyncpg as apg

    monkeypatch.setattr(apg, "connect", fake_connect)  # type: ignore[method-assign]
    deps = _make_deps()
    shutdown = asyncio.Event()
    await isolate_self(deps, new_uuid(), shutdown)

    assert conn.close_calls == 1
    assert conn.terminated is False
    assert shutdown.is_set()


# ── Test: isolate excludes still-owned rows and routes them to the interrupt arm ──


async def test_isolate_self_cancels_live_actors_and_excludes_their_rows() -> None:
    """A still-executing actor's row must never enter the re-pend.

    Isolate used to re-pend every running row at the reclaim backoff while
    the local actor kept executing: a peer claimed the row when its
    scheduled_at arrived and ran it concurrently with the body still in
    flight, a double run. Now isolate cancels the local actors the same way
    the orchestrator's CANCELLING phase does (origin stamp, cancel event,
    task cancel) so each consumer routes through its SHUTDOWN-origin
    mark_interrupted arm, waits for that bounded, and excludes the rows
    from the re-pend selection: their release belongs to the interrupt
    arms (hold earned, attempt spent).
    """
    from types import SimpleNamespace

    from taskq.backend._protocol import CancelPhase
    from taskq.context import CancelOrigin

    job_id = new_uuid()
    interrupt_writes: list[object] = []
    origin_stamps: list[CancelOrigin] = []
    cancel_event = asyncio.Event()

    async def _fake_consumer() -> None:
        # Park the way a real consumer does between flag polls; on cancel,
        # the SHUTDOWN-origin interrupt arm runs its release write during
        # unwinding.
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            interrupt_writes.append(job_id)
            raise

    entry_task = asyncio.create_task(_fake_consumer())
    # Let the consumer reach its park: a real consumer task is long
    # started by the time a heartbeat failure isolates the worker, and a
    # cancel delivered before a task's first run never executes its body.
    await asyncio.sleep(0)
    entry = SimpleNamespace(
        job_id=job_id,
        task=entry_task,
        ctx=SimpleNamespace(
            cancel_event=cancel_event,
            _set_cancel_origin=origin_stamps.append,
        ),
        cancel_phase=CancelPhase.NONE,
        cancel_observed_at=None,
        cancel_origin=CancelOrigin.NONE,
    )
    # _ActiveJob is the registry's entry type; the fake carries the exact
    # attribute surface isolate_self reads.
    deps = _make_deps()
    deps.active_jobs._by_id[job_id] = entry  # type: ignore[reportAttributeAccessUsage, index-assign]  # Why: unit test injects a minimal entry; the registry's real register() needs a full JobContext the fake replaces.

    conn = FakeConn()

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        return conn

    import asyncpg as apg

    monkeypatch_orig = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]
    try:
        shutdown = asyncio.Event()
        await asyncio.wait_for(isolate_self(deps, new_uuid(), shutdown), timeout=10.0)
    finally:
        apg.connect = monkeypatch_orig  # type: ignore[method-assign]

    # The actor got the CANCELLING-phase treatment: cooperative event, the
    # SHUTDOWN origin stamp (the interrupt arm's routing key), and the
    # task cancellation that delivered it into unwinding.
    assert cancel_event.is_set()
    assert origin_stamps == [CancelOrigin.SHUTDOWN]
    assert interrupt_writes == [job_id], (
        "the consumer's interrupt arm must run: the row's release with the "
        "hold and the spent attempt belongs to it, not to the re-pend"
    )
    assert entry_task.done()

    # The re-pend's SELECT excluded the owned row: the exclusion array is
    # the fetch's second bound parameter.
    assert len(conn.fetch_calls) == 1
    _sql, fetch_args = conn.fetch_calls[0]
    assert len(fetch_args) == 2
    assert job_id in fetch_args[1], (
        "the re-pend selection must exclude the row the local actor owns"
    )
    assert shutdown.is_set()


# ── Test: isolate excludes claim-intent rows (the take-to-register window) ──


async def test_isolate_self_excludes_claim_intent_rows() -> None:
    """A consumer parked in the take-to-register window must be excluded too.

    The registry's claim intents (marked at queue take, resolved at
    register) carry no _ActiveJob entry, so a snapshot of ``all()`` alone
    misses them: the isolate re-pended a row this process had already
    claimed (running, locked, this worker), a peer claimed the re-pended
    row and ran it concurrently with the local body, a double run, and
    the local terminal write then lost the attempt-epoch fence. The
    exclusion array must come from held_ids(), which covers both maps.
    """

    job_id = new_uuid()
    deps = _make_deps()
    # ONLY a claim intent, no registered entry: the exact window shape.
    deps.active_jobs.mark_claimed(job_id)

    row: dict[str, object] = {
        "id": job_id,
        "attempt": 1,
        "started_at": "2025-01-01T00:00:00Z",
        "max_attempts": 3,
        "retry_kind": "transient",
        "cancel_phase": 0,
    }

    class _ExclusionAwareConn(FakeConn):
        async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
            self.fetch_calls = getattr(self, "fetch_calls", [])
            self.fetch_calls.append((sql, args))
            # Mirror the statement's exclusion predicate: rows whose id
            # sits in the second bound parameter (the uuid[] exclusion
            # array) never come back, so a missed exclusion shows up
            # here as a re-pended row rather than only as a bad bind.
            excluded = set(args[1]) if len(args) > 1 else set()
            return [candidate for candidate in self._fetch_rows if candidate["id"] not in excluded]

    conn = _ExclusionAwareConn(fetch_rows=[row])

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> _ExclusionAwareConn:
        return conn

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign] # Why: patching asyncpg.connect for unit test; restored in finally.
    try:
        shutdown = asyncio.Event()
        await asyncio.wait_for(isolate_self(deps, new_uuid(), shutdown), timeout=10.0)
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]

    # The exclusion array (the fetch's second bound parameter) carries
    # the intent's id even though the registry holds no entry for it.
    assert len(conn.fetch_calls) == 1
    _sql, fetch_args = conn.fetch_calls[0]
    assert len(fetch_args) == 2
    assert job_id in fetch_args[1], (
        "the re-pend selection must exclude the claimed-but-unregistered row"
    )
    # The row was NOT re-pended: no disposition UPDATE, no crashed
    # attempt INSERT, the local body's epoch keeps the row.
    assert conn.execute_calls == [], "the claim intent's row must never enter the isolate's re-pend"
    assert shutdown.is_set()


# ── Test: isolate does not re-stamp an in-flight cancel ──────────────────


async def test_isolate_self_preserves_an_already_cancelled_entry() -> None:
    """An entry the CANCELLING orchestrator already routed (origin stamped,
    phase escalated, task unwound) must pass through isolate untouched.

    isolate's per-entry arms are guarded on the entry's current state
    exactly so a shutdown racing an operator cancel cannot overwrite the
    cancel origin the interrupt arm routes on, re-escalate a phase the
    ladder already advanced, or cancel() a task that already exited. The
    observable contract: the origin stamp list stays empty, the phase and
    its observation time are preserved, and the join does not wait on a
    done task (no join-timeout warning).
    """
    from types import SimpleNamespace

    import structlog.testing

    from taskq.backend._protocol import CancelPhase
    from taskq.context import CancelOrigin

    job_id = new_uuid()
    origin_stamps: list[CancelOrigin] = []

    async def _already_exiting() -> None:
        return None

    entry_task = asyncio.create_task(_already_exiting())
    await entry_task  # the task is done before isolate runs

    entry = SimpleNamespace(
        job_id=job_id,
        task=entry_task,
        ctx=SimpleNamespace(
            cancel_event=asyncio.Event(),
            _set_cancel_origin=origin_stamps.append,
        ),
        cancel_phase=CancelPhase.FORCED,
        cancel_observed_at=None,
        cancel_origin=CancelOrigin.OPERATOR,
    )
    deps = _make_deps()
    deps.active_jobs._by_id[job_id] = entry  # type: ignore[reportAttributeAccessUsage, index-assign]  # Why: unit test injects a minimal entry; same convention as test_isolate_self_cancels_live_actors_and_excludes_their_rows.

    conn = FakeConn()

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        return conn

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]  # Why: patching asyncpg.connect for unit test; restored in finally.
    try:
        with structlog.testing.capture_logs() as logs:
            shutdown = asyncio.Event()
            await asyncio.wait_for(isolate_self(deps, new_uuid(), shutdown), timeout=10.0)
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]

    assert origin_stamps == [], (
        "isolate must not overwrite the entry's cancel origin: the "
        "interrupt arm routes on that stamp, and a SHUTDOWN overwrite "
        "would reclassify an operator cancel"
    )
    assert entry.cancel_phase == CancelPhase.FORCED, (
        "isolate must not de-escalate (or re-stamp) a phase the cancel ladder already advanced"
    )
    assert entry.cancel_observed_at is None
    assert not any(e["event"] == "isolate-self-actor-join-timeout" for e in logs), (
        "a done task must not enter the join: waiting on it would bound "
        "every isolate by the grace periods for no reason"
    )
    assert shutdown.is_set()


async def test_isolate_self_warns_and_proceeds_when_an_actor_ignores_cancellation() -> None:
    """An actor body that swallows its cancellation outlives the join
    bound: isolate must log the stragglers (job ids included) and proceed
    with the re-pend, the bounded-wait contract — never hang the shutdown
    on an uncooperative handler."""
    import contextlib
    from types import SimpleNamespace

    import structlog.testing

    from taskq.backend._protocol import CancelPhase
    from taskq.context import CancelOrigin

    job_id = new_uuid()
    park = asyncio.Event()

    async def _stubborn_consumer() -> None:
        try:
            await park.wait()
        except asyncio.CancelledError:
            await park.wait()  # swallows the first cancel, like a handler
            # that defers cleanup past the grace periods.

    entry_task = asyncio.create_task(_stubborn_consumer())
    await asyncio.sleep(0)  # let the task reach its park

    entry = SimpleNamespace(
        job_id=job_id,
        task=entry_task,
        ctx=SimpleNamespace(
            cancel_event=asyncio.Event(),
            _set_cancel_origin=lambda origin: None,
        ),
        cancel_phase=CancelPhase.NONE,
        cancel_observed_at=None,
        cancel_origin=CancelOrigin.NONE,
    )
    deps = _make_deps()  # grace periods 0.0; the join bound is CLOSE_TIMEOUT_SECS alone
    deps.active_jobs._by_id[job_id] = entry  # type: ignore[reportAttributeAccessUsage, index-assign]  # Why: same minimal-entry convention as the other isolate tests.

    conn = FakeConn()

    async def fake_connect(
        dsn: str,
        *,
        timeout: float,
        command_timeout: float | None = None,
        connection_class: type[object]
        | None = None,  # Why: the isolate connect now threads the fork guard's connection class; the fakes accept and ignore it, the connection object they return is what the assertions see.
    ) -> FakeConn:
        return conn

    import asyncpg as apg

    orig_connect = apg.connect
    apg.connect = fake_connect  # type: ignore[method-assign]  # Why: patching asyncpg.connect for unit test; restored in finally.
    try:
        with structlog.testing.capture_logs() as logs:
            shutdown = asyncio.Event()
            await asyncio.wait_for(isolate_self(deps, new_uuid(), shutdown), timeout=10.0)
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]
        # The stubborn task outlived the join; retire it so the leaked-task
        # guard sees a clean loop.
        park.set()
        entry_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await entry_task

    timeouts = [e for e in logs if e["event"] == "isolate-self-actor-join-timeout"]
    assert len(timeouts) == 1, (
        f"an actor that ignores cancellation must surface exactly one "
        f"join-timeout warning, got {[e['event'] for e in logs]}"
    )
    assert str(job_id) in timeouts[0]["still_running"], (
        "the warning must name the straggling job so the operator can "
        "find which handler defied the shutdown"
    )
    assert shutdown.is_set(), (
        "the bounded join must proceed with the re-pend and the shutdown "
        "when the grace periods lapse, not hang on the straggler"
    )


async def test_isolate_self_refuses_a_schema_that_is_not_an_identifier() -> None:
    """The isolate interpolates the schema into its SQL templates, so a
    schema value that is not a plain identifier must raise before any
    connection is opened. WorkerSettings validates schema_name at load;
    this pin holds the defense's own contract should a caller ever
    bypass that (the same fail-loud contract cleanup_stale_workers_sql
    and complete_stale_batches_sql carry)."""
    deps = _make_deps()
    # Plain attribute assignment (the settings object does not run
    # validators on mutation): the pin is the function's guard, not the
    # settings layer's.
    deps.settings.schema_name = 'bad"; DROP schema'  # type: ignore[reportAttributeAssignmentIssue]

    import asyncpg as apg

    async def _no_connect(*args: object, **kwargs: object) -> None:
        raise AssertionError("isolate must not open a connection for an invalid schema")

    orig_connect = apg.connect
    apg.connect = _no_connect  # type: ignore[method-assign]  # Why: patching asyncpg.connect for unit test; restored in finally.
    try:
        with pytest.raises(ValueError, match="invalid schema identifier"):
            await isolate_self(deps, new_uuid(), asyncio.Event())
    finally:
        apg.connect = orig_connect  # type: ignore[method-assign]
