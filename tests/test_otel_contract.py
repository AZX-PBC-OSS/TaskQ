"""Pins the OpenTelemetry surface TaskQ's floors are chosen against.

The floors in pyproject.toml (`opentelemetry-api>=1.42.0` and the matching
`otel` extra) are a measured boundary, not a guess: 1.42.0 is the lowest
version the suite ran green on. These tests fail loudly if a version inside
that range stops providing what TaskQ relies on, instead of letting it surface
as an ImportError or an AttributeError somewhere deep in a metrics assertion.

They exist mainly because of one seam. `src/taskq/testing/otel.py` used to
import `HistogramDataPoint` and `NumberDataPoint` from
`opentelemetry.sdk.metrics._internal.point`, which carries no stability
guarantee at all: a private module can be renamed in any release, including a
patch. Both names turn out to be re-exported from the public
`opentelemetry.sdk.metrics.export` (verified identical objects, and listed in
its `__all__`, on 1.42.0, 1.43.0 and 1.44.0), so the import moved there and the
private dependency is gone. What is left to defend is the shape of the two
dataclasses, which the public path does not promise field-by-field.
"""

import contextlib
import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("opentelemetry.sdk")

# Import follows the importorskip guard above deliberately.
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid
from taskq.testing.otel import counter_data_points, histogram_points

# The fields TaskQ actually reads, not the full dataclass. A future release may
# add fields freely; removing or renaming one of these is what breaks us.
pytestmark = [pytest.mark.otel]

#: A fixed wall-clock anchor for the prune/expiry doubles' DB-clock reads.
_START = datetime(2026, 1, 1, tzinfo=UTC)

_NUMBER_FIELDS_USED = frozenset({"attributes", "value"})
_HISTOGRAM_FIELDS_USED = frozenset(
    {"attributes", "count", "sum", "bucket_counts", "explicit_bounds"}
)


def test_data_points_are_importable_from_the_public_module() -> None:
    """Neither name may drift back to a private module.

    `opentelemetry.sdk.metrics.export` is the supported path. If a future
    release drops these from it, the fix is a narrow shim with a clear message,
    not a quiet reach back into `_internal`.
    """
    from opentelemetry.sdk.metrics import export

    assert "NumberDataPoint" in export.__all__
    assert "HistogramDataPoint" in export.__all__


def test_number_data_point_keeps_the_fields_taskq_reads() -> None:
    """`counter_value` and `counter_data_points` read `.value` and `.attributes`."""
    present = {f.name for f in dataclasses.fields(NumberDataPoint)}
    missing = _NUMBER_FIELDS_USED - present
    assert not missing, (
        f"NumberDataPoint no longer provides {sorted(missing)}. "
        "src/taskq/testing/otel.py reads these; adjust it and the floor together."
    )


def test_histogram_data_point_keeps_the_fields_taskq_reads() -> None:
    """`histogram_points` hands these straight to callers asserting on them."""
    present = {f.name for f in dataclasses.fields(HistogramDataPoint)}
    missing = _HISTOGRAM_FIELDS_USED - present
    assert not missing, (
        f"HistogramDataPoint no longer provides {sorted(missing)}. "
        "src/taskq/testing/otel.py returns these; adjust it and the floor together."
    )


def test_prometheus_reader_accepts_the_registry_kwarg() -> None:
    """The reason the floor is 1.42.0 and not lower.

    `opentelemetry-exporter-prometheus` 0.62b0 hard-codes the global
    REGISTRY, so isolated metric scrapes are impossible.
    0.63b0 added the public `registry=` kwarg and requires
    `opentelemetry-sdk~=1.42.0`, which is what pins the whole floor set to
    1.42.0. Measured: on 0.62b0 this suite produces 7 TypeErrors in
    tests/test_prometheus_metrics.py.
    """
    import inspect

    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    assert "registry" in inspect.signature(PrometheusMetricReader.__init__).parameters


# ── The memoized-tracer contract ───────────────────────────────────────
#
# ``get_tracer()`` used to call ``importlib.metadata.version`` per span
# (~320µs, benchmarks/ab_otel_hotspots.py); it now resolves the tracer
# once per process. The pins below hold the two halves of that design:
# the memo actually memoizes, and - the part that makes memoization
# legal - a tracer resolved BEFORE an SDK registers still rebinds to the
# real one afterwards (the API's ProxyTracer re-checks the global
# provider on every span start, opentelemetry/trace/__init__.py
# ``ProxyTracer._tracer``). Memoization must never pin the proxy/no-op
# behavior into a worker that configures its SDK later.


def test_get_tracer_resolves_once_and_memoizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two ``get_tracer()`` calls hit ``trace.get_tracer`` exactly once and
    return the same object - the memo is the whole optimization."""
    monkeypatch.setattr(otel_mod, "_library_tracer", None)  # pyright: ignore[reportPrivateUsage]  # Why: reset the memo so this test observes its own resolution, not one an earlier test warmed.

    import opentelemetry.trace as trace_api

    calls: list[tuple[str, str]] = []
    real_get_tracer = trace_api.get_tracer

    def counting_get_tracer(name: str, version: str | None = None, **kwargs: object):  # type: ignore[no-untyped-def]
        calls.append((name, version or ""))
        return real_get_tracer(name, version, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(trace_api, "get_tracer", counting_get_tracer)

    first = otel_mod.get_tracer()
    second = otel_mod.get_tracer()

    assert first is second
    assert calls == [(otel_mod.INSTRUMENTATION_NAME, otel_mod._version())]


def test_memoized_tracer_rebinds_after_sdk_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transition contract: a tracer memoized with NO provider yields
    non-recording spans, and the SAME memoized object yields recording
    spans once a real provider registers. If memoization ever swaps the
    ProxyTracer for something that pins the no-op behavior, a worker that
    configures its SDK after first span goes silently dark - this pin is
    what makes the memoization safe to keep."""
    import opentelemetry.trace as trace_api

    # Force the no-provider path for the resolution (and restore whatever
    # the process had afterwards - never set the global for real).
    monkeypatch.setattr(trace_api, "_TRACER_PROVIDER", None)  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(otel_mod, "_library_tracer", None)  # pyright: ignore[reportPrivateUsage]

    tracer = otel_mod.get_tracer()
    pre = tracer.start_span("pre-registration")
    assert not pre.is_recording()  # Why: a method on the OTel Span API, not a property.
    pre.end()

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace_api, "_TRACER_PROVIDER", provider)  # pyright: ignore[reportPrivateUsage]

    # The SAME memoized object, now backed by the real provider.
    assert otel_mod.get_tracer() is tracer
    post = tracer.start_span("post-registration")
    assert post.is_recording()
    post.end()
    assert {s.name for s in exporter.get_finished_spans()} == {"post-registration"}


# ── Emit-site coverage (hardening wave) ──────────────────────────────────
#
# 15 of the module's ~65 emit sites had no assertion anywhere: the calls
# compiled, so their label dicts could drift (a renamed label key, a new
# unbounded label value, a swapped attribute) without any test going
# red. This section drives the TOP sites through their REAL paths (the
# terminal write, the leader prune, the shared dedup reporter, the error
# reporter, the progress flush) and pins each instrument's datapoint
# with its EXACT documented label set - cardinality is part of the
# contract, so the attribute DICT is compared, not membership.


_INSTRUMENTS: tuple[tuple[str, str, str, str], ...] = (
    # (module attr, metric name, kind, unit) - the module-level singletons
    # the emit sites record on; the lazy instruments need only get_meter.
    ("_pool_acquire_duration", "taskq.dispatch.pool_acquire_duration", "histogram", "s"),
    ("_corrupt_dispatch_rows", "taskq.dispatch.corrupt_rows", "counter", "1"),
    ("_pruned_jobs", "taskq.pruned.jobs", "counter", "1"),
    ("_archived_jobs", "taskq.archived.jobs", "counter", "1"),
    ("_expired_archive_jobs", "taskq.expired_archive.jobs", "counter", "1"),
    ("_error_reporter_failures", "taskq.error_reporter.failures", "counter", "1"),
)


@pytest.fixture
def meter_reader(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Per-test meter isolation for the emit sites under test.

    Replaces the named module singletons with instruments on a fresh
    MeterProvider, re-points ``get_meter`` so the LAZY instruments
    resolve onto the same provider, and flips ``_otel_enabled`` on (the
    root conftest's autouse guards restore both ends).
    """
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    import taskq.obs as obs_mod

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())

    for attr, name, kind, unit in _INSTRUMENTS:
        instrument: object
        if kind == "counter":
            instrument = meter.create_counter(name, unit=unit)
        else:
            instrument = meter.create_histogram(name, unit=unit)
        monkeypatch.setattr(otel_mod, attr, instrument)

    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter)
    otel_mod.set_otel_enabled(True)
    return reader


# ── Real path: the terminal write's interruption counters ───────────────


class _TerminalConn:
    def __init__(self, row: dict[str, object] | None) -> None:
        self._row = row

    async def fetchrow(self, _sql: str, *_args: object) -> dict[str, object] | None:
        return self._row


class _TerminalPool:
    def __init__(self, row: dict[str, object] | None) -> None:
        self._row = row

    def acquire(self, *, timeout: float | None = None) -> Any:
        conn = _TerminalConn(self._row)

        class _Ctx:
            async def __aenter__(self) -> _TerminalConn:
                return conn

            async def __aexit__(self, *exc_info: object) -> None:
                return None

        return _Ctx()


@pytest.mark.parametrize(
    ("row_status", "expected_hold"),
    [
        ("pending", "0"),
        ("scheduled", ">0"),
    ],
)
async def test_mark_interrupted_released_emits_the_documented_label_pair(
    meter_reader: Any,
    row_status: str,
    expected_hold: str,
) -> None:
    """``taskq.jobs.interrupted`` rides the REAL ``_mark_interrupted``
    release arm: one datapoint, labels exactly ``{actor, hold}`` with
    hold in ``{'0', '>0'}`` - the split an operator reads to see whether
    deploys are interrupting responsive or unresponsive actors.

    Regression caught: a label rename (``held=`` vs ``hold=``) or a third
    hold value silently breaks every dashboard built on the documented
    pair, and the counter is the ONLY record of the interruption class.
    """
    from taskq.backend._sql_templates import render
    from taskq.backend._terminal import (
        _mark_interrupted,  # pyright: ignore[reportPrivateUsage]  # Why: the shared terminal-write body is the real path under test.
    )

    rec: dict[str, object] = {
        "outcome_branch": "released",
        "status": row_status,
        "actor": "alpha",
        "attempt": 1,
    }
    job_id, worker_id = new_uuid(), new_uuid()

    outcome = await _mark_interrupted(
        _TerminalPool(rec),  # pyright: ignore[reportArgumentType]  # Why: duck-typed pool double, the dispatcher's real input shape.
        render("taskq"),
        job_id,
        worker_id,
        attempt=1,
        hold=timedelta(seconds=10),
    )

    assert outcome == row_status
    points = counter_data_points(meter_reader, "taskq.jobs.interrupted")
    assert len(points) == 1
    assert points[0].attributes == {"actor": "alpha", "hold": expected_hold}


async def test_mark_interrupted_fenced_out_emits_the_noop_counter(
    meter_reader: Any,
) -> None:
    """The fenced-out arm (the row moved: a reclaim, a terminal write, an
    operator cancel in flight) emits ``taskq.jobs.interrupted_noop`` with
    the empty-actor label - a silent no-op on a release path is the
    failure mode the project rule names.

    Regression caught: dropping the noop instrumentation (or attributing
    a foreign actor to it) turns "the interruption looked released but
    never landed" back into an undetectable incident.
    """
    from taskq.backend._sql_templates import render
    from taskq.backend._terminal import _mark_interrupted  # pyright: ignore[reportPrivateUsage]

    job_id, worker_id = new_uuid(), new_uuid()

    outcome = await _mark_interrupted(
        _TerminalPool(None),  # pyright: ignore[reportArgumentType]
        render("taskq"),
        job_id,
        worker_id,
        attempt=1,
        hold=timedelta(seconds=10),
    )

    assert outcome == "noop"
    points = counter_data_points(meter_reader, "taskq.jobs.interrupted_noop")
    assert len(points) == 1
    assert points[0].attributes == {"actor": ""}


# ── Real path: the leader prune's archive counters ───────────────────────


class _PruneScriptedConn:
    """Conn double for the leader's prune/expiry sweeps.

    Answers the two infrastructure reads every sweep makes (the GUC
    probe, the DB-clock read) and routes the batch statements by
    (arg-shape, status): the archive candidate is
    ``(status, retention, size)``, the archive write ``(status,
    archive_interval, ids, retention)``, the expiry batch ``(size,)``.
    Each scripted answer is returned ONCE (the first matching call);
    every further matching call answers empty, which ends the drain.
    """

    def __init__(
        self,
        db_now: datetime,
        *,
        candidate: list[dict[str, object]] | None = None,
        write: list[dict[str, object]] | None = None,
        expiry: list[dict[str, object]] | None = None,
    ) -> None:
        self._db_now = db_now
        self._candidate = list(candidate or [])
        self._write = list(write or [])
        self._expiry = list(expiry or [])

    def _answer_once(self, script: list[dict[str, object]]) -> list[dict[str, object]]:
        if script:
            rows = list(script)
            script.clear()
            return rows
        return []

    def transaction(self) -> Any:
        return contextlib.nullcontext()

    async def fetchval(self, _sql: str) -> datetime:
        return self._db_now

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        if "current_setting" in sql:
            return [{"current_setting": "30s"}]
        if len(args) == 3 and args[0] == "succeeded":
            return self._answer_once(self._candidate)
        if len(args) == 4 and args[0] == "succeeded":
            return self._answer_once(self._write)
        if len(args) == 1:
            return self._answer_once(self._expiry)
        return []

    async def execute(self, _sql: str, *_args: object) -> str:
        return "SELECT 1"


async def test_prune_terminal_jobs_emits_paired_prune_and_archive_counters(
    meter_reader: Any,
) -> None:
    """The fleet-wide prune arm records ``taskq.pruned.jobs`` AND
    ``taskq.archived.jobs`` together, per (actor, status) group, from the
    write statement's RETURNING - the real ``prune_terminal_jobs`` loop,
    batch statement by batch statement.

    Regression caught: the two counters ride the same row loop; a drift
    that records one without the other (a refactor adding an early
    ``continue``) makes the prune total and the archive total disagree -
    exactly the dashboard anomaly the paired emit exists to prevent.
    """
    from taskq.worker._leader_shared import prune_terminal_jobs

    conn = _PruneScriptedConn(
        _START + timedelta(days=40),
        candidate=[{"id": new_uuid()}],
        write=[{"actor": "alpha", "status": "succeeded", "cnt": 2}],
    )

    result = await prune_terminal_jobs(
        conn,  # pyright: ignore[reportArgumentType]
        retention_per_status={"succeeded": timedelta(days=1)},
        archive_retention=timedelta(days=30),
        schema="taskq",
    )

    assert result.total_deleted == 2
    archived = counter_data_points(meter_reader, "taskq.archived.jobs")
    assert len(archived) == 1
    assert archived[0].attributes == {"status": "succeeded"}
    assert archived[0].value == 2
    pruned = counter_data_points(meter_reader, "taskq.pruned.jobs")
    assert len(pruned) == 1
    assert pruned[0].attributes == {"actor": "alpha", "status": "succeeded"}
    assert pruned[0].value == 2


async def test_archive_expiry_sweep_emits_the_expiry_counter(
    meter_reader: Any,
) -> None:
    """The archive expiry sweep records ``taskq.expired_archive.jobs`` per
    status group from the hard-delete's RETURNING, through the real
    ``archive_expiry_sweep`` drain.

    Regression caught: this is the sweep that hard-deletes expired
    archive rows; its counter disappearing (a refactor renaming the
    instrument the loop closes over) would make archive growth look
    unbounded while the rows were actually being dropped.
    """
    from taskq.worker._leader_shared import archive_expiry_sweep

    conn = _PruneScriptedConn(
        _START + timedelta(days=90),
        expiry=[{"actor": "alpha", "status": "failed", "cnt": 3}],
    )

    result = await archive_expiry_sweep(conn, schema="taskq")  # pyright: ignore[reportArgumentType]

    assert result.total_deleted == 3
    points = counter_data_points(meter_reader, "taskq.expired_archive.jobs")
    assert len(points) == 1
    assert points[0].attributes == {"status": "failed"}
    assert points[0].value == 3


# ── Real path: the shared dedup reporter and the error reporter ─────────


async def test_enqueue_dedup_reporter_emits_the_documented_reason_label(
    meter_reader: Any,
) -> None:
    """``taskq.enqueue.dedups`` rides the shared seam
    (``_log_enqueue_dedup``) every dedup site reports through, labelled
    with the bounded reason enum - the rate signal that survives the
    per-hit WARNING budget at batch scale.

    Regression caught: a site reporting through a private counter (or a
    reason string outside the enum) splits the dedup rate across
    instruments the budget-bounded log arm was supposed to replace.
    """
    from taskq.backend._enqueue import _log_enqueue_dedup  # pyright: ignore[reportPrivateUsage]
    from taskq.testing.jobs import make_job_row

    _log_enqueue_dedup(make_job_row(status="running"), dedup_reason="idempotency_key")

    points = counter_data_points(meter_reader, "taskq.enqueue.dedups")
    assert len(points) == 1
    assert points[0].attributes == {"dedup_reason": "idempotency_key"}


async def test_failing_error_reporter_emits_its_type_label(
    meter_reader: Any,
) -> None:
    """A reporter whose ``report()`` raises is counted on
    ``taskq.error_reporter.failures`` with the reporter CLASS NAME - the
    real ``invoke_error_reporter`` defensive wrap.

    Regression caught: the counter is the only signal that a vendor hook
    (Sentry forwarder, DLQ writer) is silently eating every terminal
    failure; a label rename orphans the alert that watches for it.
    """
    from taskq.obs.error_reporter import invoke_error_reporter
    from taskq.testing.jobs import make_job_row

    class _BoomReporter:
        async def report(self, job: object, exception: BaseException) -> None:
            raise RuntimeError("boom")

    await invoke_error_reporter(_BoomReporter(), make_job_row(status="failed"), RuntimeError("x"))

    points = counter_data_points(meter_reader, "taskq.error_reporter.failures")
    assert len(points) == 1
    assert points[0].attributes == {"reporter_type": "_BoomReporter"}


# ── Real path: the progress flush's two failure stages ───────────────────


class _FlushPool:
    """Pool double for ``_flush_buffer``: the acquire itself can fail (the
    pool-stage arm) or hand out a conn whose statement fails (the
    per-job arm)."""

    def __init__(self, conn: object | None, acquire_error: Exception | None = None) -> None:
        self._conn = conn
        self._acquire_error = acquire_error

    def acquire(self) -> Any:
        if self._acquire_error is not None:
            raise self._acquire_error
        conn = self._conn

        class _Ctx:
            async def __aenter__(self) -> object:
                return conn

            async def __aexit__(self, *exc_info: object) -> None:
                return None

        return _Ctx()


@pytest.mark.parametrize(
    ("scenario", "expected_stage", "expected_error_type"),
    [
        ("acquire_fails", "pool", "RuntimeError"),
        ("statement_fails", "per_job", "ValueError"),
    ],
)
async def test_progress_flush_failures_carry_the_stage_split(
    meter_reader: Any,
    scenario: str,
    expected_stage: str,
    expected_error_type: str,
) -> None:
    """``taskq.progress.flush_failures`` keeps the pool/per_job stages
    distinguishable - the two are materially different incidents (every
    job's progress lost vs one job's delta lost) and must never fold
    into one label - through the REAL ``_flush_buffer`` failure arms.

    Regression caught: an alert rule keys on ``stage='pool'`` for the
    page-now condition; folding the stages (or renaming the label) makes
    a pool outage read as per-job noise.
    """
    from taskq.progress._buffer import _ProgressBuffer  # pyright: ignore[reportPrivateUsage]
    from taskq.progress._flush import _flush_buffer  # pyright: ignore[reportPrivateUsage]

    job_id, worker_id = new_uuid(), new_uuid()
    buffer = _ProgressBuffer(job_id=job_id, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {}

    if scenario == "acquire_fails":
        pool: Any = _FlushPool(conn=None, acquire_error=RuntimeError("pool exhausted"))
    else:

        class _FailingConn:
            async def fetchrow(self, _sql: str, *_args: object) -> object:
                raise ValueError("value too long for column")

        pool = _FlushPool(conn=_FailingConn())

    await _flush_buffer(pool, "taskq", job_id, worker_id, buffer, buffers)

    points = counter_data_points(meter_reader, "taskq.progress.flush_failures")
    assert len(points) == 1
    assert points[0].attributes == {"stage": expected_stage, "error_type": expected_error_type}


# ── Label pins: the remaining unasserted emit sites ──────────────────────


def test_remaining_emit_sites_record_their_exact_documented_labels(
    meter_reader: Any,
) -> None:
    """The single-instrument emit sites whose labels had no pin: the
    pool-acquire histogram, the corrupt-dispatch-row counter, the
    queue-wait histogram, the ratelimit dependency-failure counter, and
    the keyed-reclaim drain trio (failure / duration / rows).

    Regression caught: each attribute dict is compared EXACTLY - a new
    unbounded label value (a queue name skipping ``_bounded_queue``), a
    swapped label key, or a spill of caller-controlled strings into a
    label set breaks the pin before it breaks the metric store's
    cardinality budget.
    """
    otel_mod.record_pool_acquire_duration("invoices", 0.25)
    otel_mod.record_corrupt_dispatch_row("invoices", "payload")
    otel_mod.record_queue_wait("alpha", "invoices", 1.5)
    otel_mod.record_ratelimit_acquire_dependency_failure("ConnectionError")
    otel_mod.record_reservation_reclaim_drain_failure("TimeoutError")
    otel_mod.record_reservation_reclaim_drain_duration(0.75)
    otel_mod.record_reservation_reclaim_drain_rows(7)

    acquire_points = histogram_points(meter_reader, "taskq.dispatch.pool_acquire_duration")
    assert len(acquire_points) == 1
    assert acquire_points[0].attributes == {"queue": "invoices"}

    corrupt = counter_data_points(meter_reader, "taskq.dispatch.corrupt_rows")
    assert len(corrupt) == 1
    assert corrupt[0].attributes == {"queue": "invoices", "column": "payload"}

    wait_points = histogram_points(meter_reader, "taskq.jobs.queue_wait_seconds")
    assert len(wait_points) == 1
    assert wait_points[0].attributes == {"actor": "alpha", "queue": "invoices"}

    dependency = counter_data_points(meter_reader, "taskq.ratelimit.acquire_dependency_failures")
    assert len(dependency) == 1
    assert dependency[0].attributes == {"error_type": "ConnectionError"}

    drain_failure = counter_data_points(meter_reader, "taskq.ratelimit.reclaim_drain_failures")
    assert len(drain_failure) == 1
    assert drain_failure[0].attributes == {"error_type": "TimeoutError"}

    drain_duration = histogram_points(meter_reader, "taskq.ratelimit.reclaim_drain_duration")
    assert len(drain_duration) == 1
    assert drain_duration[0].attributes == {}, "the duration histogram carries no labels"
    assert drain_duration[0].sum == pytest.approx(0.75)

    drain_rows = counter_data_points(meter_reader, "taskq.ratelimit.reclaim_drain_rows")
    assert len(drain_rows) == 1
    assert drain_rows[0].attributes == {}, "the rows counter carries no labels"
    assert drain_rows[0].value == 7
