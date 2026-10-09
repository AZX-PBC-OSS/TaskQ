"""Differential audit: the ``_SweepSpec`` registry vs the hand-unrolled blocks.

The refactor claim (b56a0f39) is that the spec-driven runner in
``taskq.worker._leader_sweeps`` is behavior-identical to the eight
hand-unrolled try/except/finally blocks it replaced — tolerances, tick
order, metric emissions, and drain discipline carried verbatim. This
module does not re-read the claim, it EXECUTES both shapes against the
same scripted faults and diffs the observable event streams:

* the vendored pre-refactor module (``tests/_audit_vendor/``, extracted
  verbatim from ``origin/main``) — the OLD implementation;
* the working-tree module — the NEW implementation.

Each scenario runs one tick of each under identical instrumentation
(backend call order, ``record_sweep_*``, ``_metric_duration``/
``_metric_rows``, ``_dbg``/``_err``, and every log event) and asserts
the two event streams are EQUAL. A tolerance the old block had that the
spec dropped, a reordered sweep, a moved metric, or a changed drain
discipline shows up as a stream diff here, red.
"""

import asyncio
import contextlib
import importlib.util
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import (
    uuid4,  # noqa: TID251  # Why: never persisted — a loop-harness worker_id for log fields only.
)

import asyncpg
import pytest

from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.worker import _leader_sweeps as new_sweeps
from taskq.worker._leader_shared import SweepContext
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.asyncio

_VENDOR_DIR = Path(__file__).parent / "_audit_vendor"


def _load_old_module() -> Any:
    """The verbatim pre-refactor ``_leader_sweeps`` (vendored from main)."""
    spec = importlib.util.spec_from_file_location(
        "taskq_audit_old_leader_sweeps", _VENDOR_DIR / "_old_leader_sweeps.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── Instrumentation ───────────────────────────────────────────────────────

_TIMING_KEYS = frozenset({"sweep_duration_ms", "duration_ms"})


def _clean_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Drop wall-clock-derived fields and the run's random worker_id; keep
    every structural field."""
    return {
        k: ("<worker_id>" if k == "worker_id" else v)
        for k, v in kwargs.items()
        if k not in _TIMING_KEYS
    }


class _LogCap:
    """Stand-in for the module logger recording every structured event."""

    def __init__(self, sink: Callable[[Any], None]) -> None:
        self._sink = sink

    def debug(self, event: str, **kw: Any) -> None:
        self._sink(("debug", event, _clean_kwargs(kw)))

    def info(self, event: str, **kw: Any) -> None:
        self._sink(("info", event, _clean_kwargs(kw)))

    def warning(self, event: str, **kw: Any) -> None:
        self._sink(("warn", event, _clean_kwargs(kw)))

    def error(self, event: str, **kw: Any) -> None:
        self._sink(("error", event, _clean_kwargs(kw)))


class ScriptedBackend:
    """Backend double whose per-sweep scripts are popped per call.

    A script entry is an ``int`` (the batch's row count) or a
    ``BaseException`` instance to raise. The LAST entry repeats, so a
    drained sweep's ``[7, 0]`` script yields 7 rows to the first call
    and 0 to the drain's single follow-up.
    """

    def __init__(
        self,
        scripts: dict[str, deque[int | BaseException]],
        stream: list[Any] | None = None,
    ) -> None:
        self.scripts = scripts
        self.stream = stream if stream is not None else []

    def _next(self, name: str) -> int:
        self.stream.append(("call", name))
        script = self.scripts[name]
        item = script[0] if len(script) == 1 else script.popleft()
        if isinstance(item, BaseException):
            raise item
        return item

    async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
        return self._next("expired_locks")

    async def deadline_sweep(self) -> int:
        return self._next("deadline_exceeded")

    async def sweep_leaked_reservation_slots(
        self, conn: Any, *, schema: str, batch_size: int
    ) -> int:
        return self._next("leaked_slots")

    async def sweep_expired_results(self, conn: Any, *, schema: str, batch_size: int) -> int:
        return self._next("expired_results")

    async def sweep_expired_events(
        self, conn: Any, *, schema: str, retention: timedelta, batch_size: int
    ) -> int:
        return self._next("job_events_retention")

    async def sweep_idle_keyed_rows(
        self, conn: Any, *, schema: str, horizon: timedelta, batch_size: int
    ) -> int:
        return self._next("keyed_row_reclaim")


class ScriptConn:
    """Conn double: scripted ``execute`` tags and ``fetchval`` verdicts.

    ``stale_workers`` and ``stale_batches`` ride the shared SQL helpers
    on a pool conn (not backend methods), so their row counts — and
    their faults — are scripted here: the ``execute`` tag drives
    stale_workers, the ``fetchval`` int drives stale_batches.
    """

    def __init__(
        self,
        execute_script: deque[str | BaseException] | None = None,
        fetchval_script: deque[int | BaseException] | None = None,
        stream: list[Any] | None = None,
    ) -> None:
        self._execute = execute_script
        self._fetchval = fetchval_script
        self.stream = stream if stream is not None else []

    async def execute(self, sql: str, *args: object) -> str:
        # The workflow statements' calls carry the wf_ prefix (the
        # composition pin strips them with the wf events; a legacy call
        # never matches — the strip stays conservative). The jobs-table
        # workflow statements (the nodeless-root reap) carry no wf_ table
        # — the flow-root marker they filter on IS the classifier.
        if "wf_" in sql or "__flow__" in sql:
            self.stream.append(("call", "wf_stmt"))
            return "UPDATE 0"
        self.stream.append(("call", "stale_workers"))
        if self._execute is None:
            return "DELETE 0"
        item = self._execute[0] if len(self._execute) == 1 else self._execute.popleft()
        if isinstance(item, BaseException):
            raise item
        return item

    async def fetchval(self, sql: str, *args: object) -> int:
        # The SAME wf_ classification the execute path carries (the ring
        # prune's owner count is the first wf statement on this channel —
        # an unclassified fetchval would ride the stale_batches script's
        # deque and shift a 7 into the wrong arm's verdict). The
        # jobs-table workflow statements (the nodeless-root reap) carry
        # no wf_ table — the flow-root marker IS the classifier.
        if "wf_" in sql or "__flow__" in sql:
            self.stream.append(("call", "wf_stmt"))
            return 0
        self.stream.append(("call", "stale_batches"))
        if self._fetchval is None:
            return 0
        item = self._fetchval[0] if len(self._fetchval) == 1 else self._fetchval.popleft()
        if isinstance(item, BaseException):
            raise item
        return item

    def transaction(self) -> "_PoolCtx":
        """The workflow arms' transaction context (asyncpg's
        ``conn.transaction()`` is called UNAWAITED — it returns the
        context manager; the arms ``async with`` it). No legacy sweep
        enters a transaction on the conn — adding the context manager
        cannot disturb the legacy scenarios."""
        return _PoolCtx(self)

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
        """The workflow rederive arm's summary read. The ZERO summary: no
        join-wait rows in the double's world — the fire arm never engages,
        the pass is one benign read per tick. No legacy sweep calls
        ``fetchrow`` — this cannot disturb the legacy scenarios."""
        return {
            "blocked": 0,
            "blocked_required": 0,
            "flow_fenced": 0,
            "reconciled": 0,
            "firable": 0,
        }

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []


class _PoolCtx:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def __aenter__(self) -> Any:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        return None


class ConnPool:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> _PoolCtx:
        return _PoolCtx(self._conn)


class _NoKeyedRL:
    """Rate-limit registry stand-in with nothing keyed and nothing pending."""

    has_keyed_reservations = False
    has_keyed_rate_limits = False
    has_pending_reservation_reclaims = False


# ── The tick table both implementations must walk identically ─────────────

SWEEPS = (
    "expired_locks",
    "deadline_exceeded",
    "leaked_slots",
    "expired_results",
    "job_events_retention",
    "keyed_row_reclaim",
    "stale_workers",
    "stale_batches",
)
# Sweeps whose drain is a backend-method repeat. stale_workers /
# stale_batches also drain, but their repeat rides the conn (below).
DRAINED_BACKEND = frozenset(
    {"expired_locks", "deadline_exceeded", "leaked_slots", "expired_results"}
)


def _worker_settings(sweep_interval: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}
    for key, value in {"SWEEP_INTERVAL": sweep_interval, **overrides}.items():
        data[f"TASKQ_{key}"] = value
    return WorkerSettings.load_from_dict(data, validate=False)


def _make_deps(pool: Any, sweep_interval: str = "5.0", **overrides: str) -> WorkerDeps:
    settings = _worker_settings(
        sweep_interval,
        HEARTBEAT_INTERVAL="0.5",
        HEARTBEAT_COMMAND_TIMEOUT="0.1",
        LOCK_LEASE="3.0",
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        WATCHDOG_LOOP_LAG_WARN_BUDGET="0.5",
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
        **overrides,
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    deps.is_leader.set()  # the sweeps are leader-gated
    return deps


def _success_scripts() -> dict[str, deque[int | BaseException]]:
    """Every sweep returns rows once; drained sweeps' drains see 0."""
    scripts: dict[str, deque[int | BaseException]] = {}
    for name in SWEEPS:
        scripts[name] = deque([7, 0]) if name in DRAINED_BACKEND else deque([7])
    return scripts


def _fault_scripts(faulted: str, exc: BaseException) -> dict[str, deque[int | BaseException]]:
    """The faulted sweep raises on its first call; every sibling is benign.

    Benign siblings return 0, so NO drain engages in fault scenarios:
    every sweep — drained or not — makes exactly ONE call per tick.
    """
    scripts: dict[str, deque[int | BaseException]] = {name: deque([0]) for name in SWEEPS}
    scripts[faulted] = deque([exc])
    return scripts


def _success_conn() -> ScriptConn:
    """A conn whose stale_workers/stale_batches batches return 7 then 0."""
    return ScriptConn(
        execute_script=deque(["DELETE 7", "DELETE 0"]),
        fetchval_script=deque([7, 0]),
    )


def _all_success_calls() -> int:
    """Calls a full benign tick issues: 2 per drained backend sweep, 1 per
    single-batch backend sweep, 2 per conn-driven sweep."""
    return 2 * len(DRAINED_BACKEND) + 2 + 4


async def _run_one_tick(
    mod: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    backend: Any,
    conn: ScriptConn,
    until: Callable[[list[Any]], bool],
    sweep_interval: str = "5.0",
    **period_overrides: str,
) -> list[Any]:
    """Run one tick (at least) of *mod*'s ``_sweep_loop`` and return the
    event stream observed so far.

    Both implementations are instrumented identically (module-global
    rebinding: the sweep code resolves these names in its own module
    namespace at call time), so the returned streams are comparable.
    """
    events: list[Any] = []

    def sink(entry: Any) -> None:
        events.append(entry)

    # Backend/conn call events land in the SAME stream, in call order.
    if isinstance(backend, ScriptedBackend):
        backend.stream = events
    conn.stream = events

    monkeypatch.setattr(mod, "record_sweep_success", lambda n: sink(("success", n)))
    monkeypatch.setattr(mod, "record_sweep_timeout", lambda n: sink(("timeout", n)))
    monkeypatch.setattr(mod, "_metric_duration", lambda n, s: sink(("metric_duration", n)))
    monkeypatch.setattr(mod, "_metric_rows", lambda n, c: sink(("metric_rows", n, c)))
    monkeypatch.setattr(mod, "_dbg", lambda ev, ki, co, st: sink(("dbg", ev, ki, co)))
    monkeypatch.setattr(mod, "_err", lambda ev, ki, wi, ex: sink(("err", ev, ki)))
    monkeypatch.setattr(mod, "log", _LogCap(sink))

    deps = _make_deps(ConnPool(conn), sweep_interval=sweep_interval, **period_overrides)
    ctx = SweepContext(
        deps=deps,
        backend=cast(Any, backend),
        clock=cast(Any, FakeClock(datetime(2025, 1, 1, tzinfo=UTC))),
        worker_id=uuid4(),
        rate_limit_registry=cast(Any, _NoKeyedRL()),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(mod._sweep_loop(ctx, shutdown))
    try:
        await wait_for_condition(
            lambda: until(events),
            description="the tick must reach its scripted observable",
        )
        # Let the tick's tail (finally-metrics, drain bookkeeping) flush
        # before the shutdown cut, identically for both implementations.
        await asyncio.sleep(0.05)
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return events


def _calls(events: list[Any]) -> list[str]:
    return [e[1] for e in events if e[0] == "call"]


def _after_calls(n: int) -> Callable[[list[Any]], bool]:
    return lambda events: len(_calls(events)) >= n


# ── Scenarios ─────────────────────────────────────────────────────────────


async def test_audit_success_tick_event_streams_identical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The all-success tick: call order, metrics, dbg events, drains."""
    old_events = await _run_one_tick(
        _load_old_module(),
        monkeypatch,
        backend=ScriptedBackend(_success_scripts()),
        conn=_success_conn(),
        until=_after_calls(_all_success_calls()),
    )
    new_events = await _run_one_tick(
        new_sweeps,
        monkeypatch,
        backend=ScriptedBackend(_success_scripts()),
        conn=_success_conn(),
        until=_after_calls(_all_success_calls()),
    )
    assert old_events == new_events
    # Non-vacuity: the tick really swept everything, in order, and the
    # drains engaged (a second call per drained sweep).
    assert _calls(new_events) == [
        "expired_locks",
        "expired_locks",
        "deadline_exceeded",
        "deadline_exceeded",
        "leaked_slots",
        "leaked_slots",
        "expired_results",
        "expired_results",
        "job_events_retention",
        "keyed_row_reclaim",
        "stale_workers",
        "stale_workers",
        "stale_batches",
        "stale_batches",
    ]
    # The sample discipline is IN the stream: duration always; rows +
    # success only when the call returned (non-empty on this path).
    for name in SWEEPS:
        assert ("metric_duration", name) in new_events
        assert ("metric_rows", name, 7) in new_events
        assert ("success", name) in new_events


@pytest.mark.parametrize(
    ("faulted", "exc", "tolerated"),
    [
        # The deadline family: timeout record + warn, tick continues.
        ("expired_locks", TimeoutError(), True),
        ("deadline_exceeded", asyncpg.QueryCanceledError("q"), True),
        ("leaked_slots", TimeoutError(), True),
        ("expired_results", asyncpg.QueryCanceledError("q"), True),
        ("job_events_retention", TimeoutError(), True),
        ("keyed_row_reclaim", asyncpg.QueryCanceledError("q"), True),
        ("stale_workers", asyncpg.DeadlockDetectedError("d"), True),
        ("stale_batches", asyncpg.DeadlockDetectedError("d"), True),
        # The pre-migration tolerances are per-sweep: the spec's
        # extra_except must cover EXACTLY the old blocks' extras ...
        ("keyed_row_reclaim", asyncpg.exceptions.UndefinedColumnError("col"), True),
        ("stale_batches", asyncpg.exceptions.UndefinedTableError("tbl"), True),
        # ... and must NOT leak into the sibling sweeps: these propagate
        # into the unexpected-error backstop, aborting the tick.
        ("expired_locks", asyncpg.exceptions.UndefinedColumnError("col"), False),
        ("leaked_slots", asyncpg.exceptions.UndefinedTableError("tbl"), False),
        ("expired_results", asyncpg.exceptions.UndefinedColumnError("col"), False),
        ("job_events_retention", asyncpg.exceptions.UndefinedTableError("tbl"), False),
        ("stale_workers", asyncpg.exceptions.UndefinedColumnError("col"), False),
        # NotImplementedError: the warn-once arm for sweeps 1/2 only.
        ("expired_locks", NotImplementedError("ni"), True),
        ("deadline_exceeded", NotImplementedError("ni"), True),
        ("leaked_slots", NotImplementedError("ni"), False),
        # A raw bug: never tolerated, by old blocks or specs alike.
        ("expired_results", ValueError("bug"), False),
        ("stale_batches", ValueError("bug"), False),
    ],
)
async def test_audit_fault_event_streams_identical(
    monkeypatch: pytest.MonkeyPatch,
    faulted: str,
    exc: BaseException,
    tolerated: bool,
) -> None:
    """Each scripted fault produces the IDENTICAL old-vs-new stream.

    ``tolerated=False`` scenarios are the mutation hunt's teeth: if the
    runner had NORMALIZED the tolerance sets (swallowing a fault the old
    block let propagate, or propagating one it tolerated), the streams'
    call prefixes diverge — the aborted tick stops issuing calls.
    """
    if faulted == "stale_workers":
        conn_factory: Callable[[], ScriptConn] = lambda: ScriptConn(  # noqa: E731
            execute_script=deque([exc])
        )
    elif faulted == "stale_batches":
        conn_factory = lambda: ScriptConn(fetchval_script=deque([exc]))  # noqa: E731
    else:
        conn_factory = ScriptConn

    def backend_factory() -> ScriptedBackend:
        return ScriptedBackend(_fault_scripts(faulted, exc))

    until = _after_calls(len(SWEEPS)) if tolerated else _after_calls(SWEEPS.index(faulted) + 1)

    old_events = await _run_one_tick(
        _load_old_module(),
        monkeypatch,
        backend=backend_factory(),
        conn=conn_factory(),
        until=until,
    )
    new_events = await _run_one_tick(
        new_sweeps,
        monkeypatch,
        backend=backend_factory(),
        conn=conn_factory(),
        until=until,
    )
    assert old_events == new_events
    # Non-vacuity: the fault really surfaced (tolerated arms warn
    # in-stream) or really aborted the tick (the faulted call is last).
    if tolerated:
        is_deadline_family = isinstance(exc, (TimeoutError, asyncpg.QueryCanceledError))
        if isinstance(exc, NotImplementedError):
            errs = [e for e in new_events if e[0] == "err"]
            assert errs, f"the unimplemented {faulted} arm never surfaced in the stream"
        else:
            warns = [e for e in new_events if e[0] == "warn" and e[2].get("error") == repr(exc)]
            assert warns, f"the tolerated {faulted} fault never surfaced in the stream"
            if is_deadline_family:
                assert ("timeout", faulted) in new_events
            else:
                assert ("timeout", faulted) not in new_events
    else:
        assert _calls(new_events)[-1] == faulted


async def test_audit_warn_once_unimplemented_arm_holds_across_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sweeps 1/2's NotImplementedError arm warns ONCE per loop, both."""

    def scripts() -> dict[str, deque[int | BaseException]]:
        out: dict[str, deque[int | BaseException]] = {name: deque([0]) for name in SWEEPS}
        out["expired_locks"] = deque([NotImplementedError("ni")])
        out["deadline_exceeded"] = deque([NotImplementedError("ni")])
        return out

    errs: dict[str, list[Any]] = {}
    for side, mod in (("old", _load_old_module()), ("new", new_sweeps)):
        # A small interval so the loop ticks repeatedly inside the wait
        # window; the err cap (one per sweep per loop) is what's compared.
        events = await _run_one_tick(
            mod,
            monkeypatch,
            backend=ScriptedBackend(scripts()),
            conn=ScriptConn(),
            until=_after_calls(2 * len(SWEEPS)),
            sweep_interval="0.05",
        )
        errs[side] = [e for e in events if e[0] == "err"]
    assert errs["old"] == errs["new"]
    assert len(errs["new"]) == 2, "the unimplemented arm must warn exactly once per sweep"


async def test_audit_period_gate_zero_disables_identically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timedelta(0) sentinel keeps retention/keyed off in both."""
    old_events = await _run_one_tick(
        _load_old_module(),
        monkeypatch,
        backend=ScriptedBackend(_success_scripts()),
        conn=_success_conn(),
        until=_after_calls(_all_success_calls() - 2),
        EVENT_RETENTION_PERIOD="0",
        KEYED_ROW_RECLAIM_PERIOD="0",
    )
    new_events = await _run_one_tick(
        new_sweeps,
        monkeypatch,
        backend=ScriptedBackend(_success_scripts()),
        conn=_success_conn(),
        until=_after_calls(_all_success_calls() - 2),
        EVENT_RETENTION_PERIOD="0",
        KEYED_ROW_RECLAIM_PERIOD="0",
    )
    assert old_events == new_events
    calls = _calls(new_events)
    assert "job_events_retention" not in calls
    assert "keyed_row_reclaim" not in calls


async def test_audit_hasattr_gate_keeps_sweeps_off_in_memory_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backend without the maintenance sweeps: only sweeps 1/2 run."""

    class MemLikeBackend:
        async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
            return 0

        async def deadline_sweep(self) -> int:
            return 0

    def until(events: list[Any]) -> bool:
        return len([e for e in events if e[0] == "metric_duration"]) >= 2

    old_events = await _run_one_tick(
        _load_old_module(),
        monkeypatch,
        backend=MemLikeBackend(),
        conn=ScriptConn(),
        until=until,
    )
    new_events = await _run_one_tick(
        new_sweeps,
        monkeypatch,
        backend=MemLikeBackend(),
        conn=ScriptConn(),
        until=until,
    )
    assert old_events == new_events
    names = [e[1] for e in new_events if e[0] == "metric_duration"]
    assert set(names) == {"expired_locks", "deadline_exceeded"}


# ── The workflow arms' own expectations (the capability-gated appends) ────
#
# The differential above audits the REFACTOR equivalence: the vendored
# eight-sweep module vs the spec-driven one, on doubles that implement the
# LEGACY maintenance surface. The workflow arms (T04) are NOT part of that
# claim — they registered later, admitted through the same hasattr seam on
# their OWN capability marker (``workflow_sweeps_capable`` — the
# backend's declaration; the arms never borrow another sweep's method
# name). These pins hold the registration's COMPOSITION SHAPE, so the
# arms are load-bearing here too: without the capability the arms
# contribute NOTHING (the legacy loop the differential audits is
# unchanged); with it, the arms are pure APPENDS after the legacy block —
# never a reorder, a rename, or a legacy-event mutation.

_WF_ARMS = (
    "wf_join_rederive",
    "wf_outbox_drain",
    "wf_signal_sweep",
    # The D2 soak's cure: the hold-stamp reconcile (the SIGKILL-during-
    # hold wedge's fleet arm — the hold's state decides). Pure append,
    # same as the arms before it.
    "wf_hold_stamp_reconcile",
    "wf_loop_budget",
    "wf_phantom_reap",
    "wf_nodeless_root_reap",
    "wf_progress_ring_prune",
)


def _is_wf_event(entry: Any) -> bool:
    return (
        isinstance(entry, tuple)
        and len(entry) > 1
        and isinstance(entry[1], str)
        and entry[1].startswith("wf_")
    )


def _until_wf_arms_sampled(legacy_calls: int) -> Callable[[list[Any]], bool]:
    """The legacy block's calls are in AND all three arms have sampled
    their duration (the tick reached the table's tail)."""
    return lambda events: (
        len(_calls(events)) >= legacy_calls
        and (len([e for e in events if e[0] == "metric_duration" and e[1].startswith("wf_")]) >= 3)
    )


async def test_audit_wf_arms_stay_off_without_the_capability_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The admission gate: a backend implementing the LEGACY maintenance
    surface only (every differential double above is exactly that) runs
    no wf arm — the legacy loop's observable behavior is unchanged by the
    arms' registration."""
    events = await _run_one_tick(
        new_sweeps,
        monkeypatch,
        backend=ScriptedBackend(_success_scripts()),
        conn=_success_conn(),
        until=_after_calls(_all_success_calls()),
    )
    assert not [e for e in events if _is_wf_event(e)], (
        "the workflow arms ran on a backend that does not declare the "
        "workflow capability — the arms must be admitted through their own "
        "marker (the same hasattr seam the legacy sweeps use), never by "
        "borrowing another sweep's"
    )


async def test_audit_wf_arms_are_pure_appends_after_the_legacy_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On a capability-declaring backend the arms run IN TABLE ORDER after
    the legacy block — and stripping the wf events yields the
    legacy-only stream byte-for-byte (composition, not mutation: the
    differential's legacy contract holds on the capable backend too)."""
    legacy_events = await _run_one_tick(
        new_sweeps,
        monkeypatch,
        backend=ScriptedBackend(_success_scripts()),
        conn=_success_conn(),
        until=_after_calls(_all_success_calls()),
    )
    capable_backend = ScriptedBackend(_success_scripts())
    capable_backend.workflow_sweeps_capable = True  # type: ignore[reportAttributeAccessIssue]  # Why: the capability probe is a plain hasattr — the double declares the marker the real backend's class carries.
    capable_events = await _run_one_tick(
        new_sweeps,
        monkeypatch,
        backend=capable_backend,
        conn=_success_conn(),
        until=_until_wf_arms_sampled(_all_success_calls()),
    )
    assert [e for e in capable_events if not _is_wf_event(e)] == legacy_events
    # The arms really ran, in table order, under the SAME sample
    # discipline as every legacy sweep (duration always; rows + success
    # when the call returned — 0 rows here, the double's world is empty).
    wf_names = [
        e[1] for e in capable_events if e[0] == "metric_duration" and e[1].startswith("wf_")
    ]
    assert wf_names == list(_WF_ARMS), wf_names
    for name in _WF_ARMS:
        assert ("metric_rows", name, 0) in capable_events
        assert ("success", name) in capable_events
