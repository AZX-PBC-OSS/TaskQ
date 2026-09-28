"""Audit-coverage pins: the cron tick's attribution and telemetry arms.

Every test here was written red-first against a specific uncovered line
(the audit-cov census, branch feat/audit-coverage); each documents the
arm it closes, and each was mutation-proven (flipping the target line
makes the test fail).

Closed arms:
  - ``_attributable_violation``: the bounded cause-chain walk answers None
    past depth 8 (cron_loop.py:1039).
  - ``_attribute_violation``: the detail's value that is not a UUID
    (cron_loop.py:1090-1091) and the detail's column list naming a column
    TaskQ's own indexes never key (cron_loop.py:1103) are unattributable
    on purpose — a strike follows only a verified colliding plan.
  - ``tick_cron``: the schema identifier guard (cron_loop.py:1312).
  - ``_actor_failure_totals``: the absent-row and non-dict-aggregate
    decode arms answer the reconcile's "true zero" contract
    (cron_loop.py:1792, 1795).
  - ``_emit_on_commit``: a connection that cannot NOTIFY (a
    transaction-pooling proxy) retires exactly its own armed emission and
    falls back to the inline emit (cron_loop.py:579).

AUDIT CLASSIFICATION (c) — defensive-unreachable, left documented in
code (each carries its own Why comment at the site):
  - cron_loop.py:949-950 — the max-pending DST overlap's non-scheduled
    defer arm: "Unreachable today: only the DST ``allof`` pair produces
    more than one args and its extras are always future-scheduled."
  - cron_loop.py:1908, 2001 — the monotonicity belt breaks: the
    computation is pinned to answer strictly after its seed; the break is
    the regression backstop that must not turn a hop into a loop.
  - cron_loop.py:1245 — the per-plan landing's transient re-raise: the
    transient-classification predicate it re-raises under is pinned
    exhaustively at the loop boundary by tests/test_attack_loop_liveness
    (every member of TRANSIENT_PG_ERRORS drives the real loop); hitting
    THIS arm requires a PG-injected transient mid-landing inside a live
    tick transaction — chaos-tier reach.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from asyncpg.exceptions import UniqueViolationError

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.settings import WorkerSettings
from taskq.worker.cron_loop import (  # pyright: ignore[reportPrivateUsage]  # Why: the attribution and telemetry arms under test
    _actor_failure_totals,
    _attributable_violation,
    _attribute_violation,
    _emit_on_commit,
    tick_cron,
)


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "schema_name": "taskq_test"}
    )


class _FakeRecord(dict[str, Any]):
    def __getitem__(self, key: str) -> Any:
        return dict.__getitem__(self, key)


def _violation(constraint: str, detail: str) -> UniqueViolationError:
    exc = UniqueViolationError("duplicate key value violates unique constraint")
    exc.constraint_name = constraint  # type: ignore[attr-defined]  # Why: asyncpg errors carry the server's fields as attributes
    exc.detail = detail  # type: ignore[attr-defined]
    return exc


def _plan(schedule_id: Any = None, *, singleton: bool = False) -> Any:
    """A minimal _FireSuccess-shaped plan for the attribution matcher."""
    from taskq.worker.cron_loop import _FireSuccess

    row = _FakeRecord({"id": schedule_id or new_uuid(), "cron_expr": "* * * * *"})
    args = EnqueueArgs(
        id=new_uuid(),
        actor="test_actor",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={"singleton": True} if singleton else {},
    )
    return _FireSuccess(
        schedule_id=row["id"],
        row=row,
        enqueue_args=[args],
        next_fire_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=1),
        actor="test_actor",
        queue="default",
        prev_consecutive=0,
        skipped_slots=0,
    )


# ── the cause-chain walk's depth cap ────────────────────────────────────


def test_attributable_violation_bounds_the_cause_chain() -> None:
    """A pathological cause chain deeper than the walk's bound answers
    None — an unconverted violation at depth zero, a chain that never
    loops the tick. The chain carries a real violation PAST the bound: the
    bounded walk must stop at 8 and answer None, not find it."""
    tail = _violation("jobs_pkey", "Key (id)=(x) already exists.")
    exc: BaseException = tail
    for _ in range(11):
        wrapper = RuntimeError("wrapper")
        wrapper.__cause__ = exc  # type: ignore[attr-defined]
        exc = wrapper

    assert _attributable_violation(exc) is None, (
        "a violation buried deeper than the walk's bound must be "
        "unattributable: the bounded walk stops at depth 8"
    )
    assert _attributable_violation(tail) is tail


def test_attributable_violation_finds_the_raw_violation() -> None:
    """The honest shapes: the violation raw at depth zero and converted
    one cause down both answer the violation."""
    raw = _violation("jobs_pkey", "Key (id)=(x) already exists.")
    assert _attributable_violation(raw) is raw
    wrapper = RuntimeError("converted")
    wrapper.__cause__ = raw  # type: ignore[attr-defined]
    assert _attributable_violation(wrapper) is raw


# ── the detail attribution's unattributable shapes ──────────────────────


def test_attribute_violation_rejects_a_non_uuid_id_value() -> None:
    """PG truncates long detail values: a truncated (non-UUID) id value
    verifies against nothing pending — unattributable, on purpose."""
    plan = _plan()
    exc = _violation("jobs_pkey", "Key (id)=(not-a-uuid) already exists.")
    assert _attribute_violation([plan], exc) is None


def test_attribute_violation_rejects_an_unkeyed_column_detail() -> None:
    """An operator-added unique index on another column raises the same
    detail shape under a TaskQ constraint name only if TaskQ shipped it —
    a detail naming a column TaskQ's own indexes never key attributes
    nothing (an unstamped row the operator's index rejected must not
    strike a singleton-stamped plan)."""
    plan = _plan(singleton=True)
    exc = _violation("jobs_pkey", "Key (tenant)=(acme) already exists.")
    assert _attribute_violation([plan], exc) is None


def test_attribute_violation_rejects_an_unknown_constraint_name() -> None:
    """A violation under a name TaskQ does not ship is never attributable."""
    plan = _plan()
    exc = _violation("operator_index", "Key (id)=(x) already exists.")
    assert _attribute_violation([plan], exc) is None


def test_attribute_violation_strikes_the_verified_colliding_plan() -> None:
    """The honest shape: the detail's id names a pending plan's arg — the
    violator is struck, the rest survive."""
    plan = _plan()
    colliding_id = plan.enqueue_args[0].id
    exc = _violation("jobs_pkey", f"Key (id)=({colliding_id}) already exists.")
    result = _attribute_violation([plan], exc)
    assert result is not None
    violation, offenders, surviving = result
    assert violation is exc
    assert offenders == [plan]
    assert surviving == []


# ── the tick's schema guard ─────────────────────────────────────────────


async def test_tick_cron_refuses_an_invalid_schema_identifier() -> None:
    """The schema is interpolated into every statement the tick runs; the
    guard fires before any database touch."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        await tick_cron(
            None,  # type: ignore[arg-type]  # Why: the guard precedes any connection use
            _settings(),
            None,  # type: ignore[arg-type]
            "bad schema; drop table",
            new_uuid(),
        )


# ── the failure-totals decode arms ──────────────────────────────────────


class _StubConn:
    def __init__(self, row: Any) -> None:
        self._row = row

    async def fetchrow(self, *args: Any, **kwargs: Any) -> Any:
        return self._row


async def test_actor_failure_totals_absent_row_is_a_true_zero() -> None:
    """A conn that answers no row (a race with the table's creation) reads
    as 'no failing schedule anywhere' — the reconcile's true zero."""
    assert await _actor_failure_totals(_StubConn(None), "taskq_test") == {}


async def test_actor_failure_totals_non_dict_aggregate_is_a_true_zero() -> None:
    """A totals column that decodes to something other than an object is
    not a shape the reconcile can read — the true zero, never a crash."""
    assert (
        await _actor_failure_totals(_StubConn(_FakeRecord({"totals": "[1,2]"})), "taskq_test") == {}
    )


# ── the commit gate's notify-failure fallback ───────────────────────────


class _NoNotifyConn:
    """A transaction-pooling proxy: LISTEN works, NOTIFY dies."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def get_server_pid(self) -> int:
        return 4242

    async def remove_listener(self, channel: str, callback: Any) -> None:
        return None

    async def add_listener(self, channel: str, callback: Any) -> None:
        return None

    def add_termination_listener(self, callback: Any) -> None:
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append(query)
        raise asyncpg.PostgresError("NOTIFY is not supported on this proxy")


async def test_emit_on_commit_falls_back_to_inline_emit_when_notify_dies() -> None:
    """A connection that cannot carry the NOTIFY retires exactly its own
    armed emission and emits inline — losing the gate costs precision,
    losing the telemetry would cost the whole failure trail."""
    from taskq.worker import cron_loop as cron_mod

    conn = _NoNotifyConn()
    emitted: list[str] = []

    cron_mod._confirmed_listening.discard(4242)
    cron_mod._armed_commit_emits.pop(4242, None)
    await _emit_on_commit(conn, lambda: emitted.append("fired"), schema="taskq_test")

    assert emitted == ["fired"], "the degraded path must still emit"
    assert 4242 not in cron_mod._armed_commit_emits, (
        "the armed entry for THIS session must be retired, a recycled pid must not inherit it"
    )
    assert 4242 not in cron_mod._confirmed_listening
