"""THE WF-OTEL PINS — the workflow spans' contract, red-first.

THE DEFECT (the consumers' report): the vanilla lifecycle emits spans at
every messaging point — ``enqueue {actor}`` (PRODUCER), ``process
{actor}`` (CONSUMER), ``attempt.{N}`` (INTERNAL), ``dispatch`` (INTERNAL),
``cron fire`` (PRODUCER) — and the workflows package emitted ZERO: the
fleet intercept bypasses ``consume_one_job`` (the rows never ride the
vanilla consumer path) and the dev drive never enters it, so a consumer
running otel (TAStack's Azure Monitor, SustainabilityAI's ddtrace) sees
its flows as silence.

THE CURE'S LAW (mirroring the vanilla exactly — no new otel machinery):

* the SAME ``safe_start_span`` at the wf lifecycle's own points; the
  SAME no-op law (telemetry off → no spans, the flow runs, nothing
  fails); the SAME scrubbed-error recording (``render_exception`` /
  ``record_exception_text`` — exception text is scrubbed; the
  caller-supplied ATTRIBUTE values — ``taskq.run_key``, the workflow
  and step names — ride as-declared, the vanilla ``identity_key``'s
  own posture); the SAME naming grammar (lowercase phrases — ``run
  {workflow}``, ``step {step_key}`` — beside the vanilla's ``process
  {actor}``, ``cron fire``); the SAME ``taskq.*`` attribute namespace.
* the RUN span (``run {workflow}``): a ROOT (``new_root=True`` — the
  producer-link pattern), created at the flow's first claim (the
  run-key claim the packaged :func:`taskq.workflows.run` performs),
  ended at the run's terminal. Linked (not parented) to the ambient
  trace.
* the STEP span (``step {step_key}``): the claim tail's attempt
  (``_execute_claimed`` — the ONE shared tail both doors land in). The
  parent is the run span WHEN both execute in one process (the dev
  drive — otel's in-process context propagation, for free); on a fleet
  worker there is no run span in that process, so the step span is a
  per-process root — the vanilla's own cross-process law (the enqueue
  span stamps ids; the consumer LINKS) has no wf producer span to
  stamp them, so the flow correlation across processes is the
  ``taskq.flow_id`` attribute. Pinned here as the contract.
* the step's outcomes ride the span: ``taskq.outcome`` echoes the
  claim tail's own label (the ROW is the truth, the span is the
  record's echo); the status maps like the vanilla consumer's
  (succeeded-class OK, laddered ERROR, the mid-states unset); the
  ladder's failure records SCRUBBED (the redaction law); the
  fork/join/hold/defer events are span EVENTS (``span.add_event``),
  not separate spans.
* no metrics: the vanilla's one wf-relevant instrument is the leader's
  fleet gauge (``taskq.wf_progress_nodes_total``) — no per-run wf
  metric exists in the vanilla to mirror, so none is added.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from opentelemetry import trace
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.exceptions import Snooze
from taskq.obs._otel import set_otel_enabled
from taskq.testing.otel import setup_tracer
from taskq.workflows import (
    FlowRunner,
    GateDecl,
    Promise,
    StepContext,
    WorkflowApp,
    WorkflowRunError,
    build,
    map_source,
    run,
    step,
)
from taskq.workflows._worker_execution import (
    execute_flow_job,
    project_workflow_actor_configs,
)

pytestmark = pytest.mark.integration


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Approval(BaseModel):
    verdict: str


async def _plain_body(ctx: StepContext, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def _list_source(ctx: StepContext, params: Ingest) -> list[str]:
    return ["a", "b"]


async def _item_body(ctx: StepContext, item: str) -> str:
    return item


async def _hold_body(ctx: StepContext, params: Ingest) -> Any:
    return await ctx.wait_signal(Approval, timeout_s=120.0)


# ── 1. the span tree — the run span over its step children ──────────────


async def test_a_small_flow_emits_the_run_span_over_its_step_children(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE TREE: one run of a one-step flow exports the RUN span (a root —
    ``new_root=True``, the producer-link pattern) and the STEP span, and
    the step span's parent IS the run span (the dev drive executes both
    in one process — otel's context propagation parents them for free).
    The vanilla messaging spans (enqueue/process/attempt/dispatch) appear
    NOWHERE — the wf rows never ride the vanilla consumer path (the
    honest boundary; the vanilla battery pins those spans where they
    belong)."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-solo-flow")
    def otel_solo_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    result = await run(app.get("otel-solo-flow"), wf_pool, wf_schema, input={"doc_id": "d"})
    assert result.outcome == "terminal", result.outcome

    run_span = exporter.span_named("run otel-solo-flow")
    assert run_span is not None, [s.name for s in exporter.spans]
    assert run_span.parent is None, "the run span is a root (the producer-link pattern)"
    assert run_span.kind == trace.SpanKind.INTERNAL

    step_span = exporter.span_named("step solo")
    assert step_span is not None, [s.name for s in exporter.spans]
    assert step_span.parent is not None
    assert step_span.parent.span_id == run_span.get_span_context().span_id  # pyright: ignore[reportOptionalMemberAccess]  # Why: the assert above narrows for humans; pyright cannot narrow ReadableSpan.parent across the assertion boundary.
    step_ctx = step_span.get_span_context()
    run_ctx = run_span.get_span_context()
    assert step_ctx is not None and run_ctx is not None
    assert step_ctx.trace_id == run_ctx.trace_id, (
        "one trace: the step rides the run's trace in-process"
    )

    # THE HONEST BOUNDARY: no vanilla messaging span exists for the wf rows.
    names = [s.name for s in exporter.spans]
    assert not [
        n for n in names if n.startswith(("enqueue ", "process ", "dispatch", "attempt."))
    ], names


# ── 2. the run span's attribute contract ─────────────────────────────────


async def test_the_run_span_carries_the_declared_attribute_contract(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE RUN SPAN'S ATTRIBUTES (the ``taskq.*`` namespace): the
    workflow's name, the run's flow id, the root's queue, the caller's
    run key, and the per-call priority — plus the vanilla lifecycle
    events' grammar (``lifecycle.running`` at the create, the terminal
    event at the end) and the status (OK on a succeeded run)."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-attrs-flow")
    def otel_attrs_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    result = await run(
        app.get("otel-attrs-flow"),
        wf_pool,
        wf_schema,
        input={"doc_id": "d"},
        key="nightly-slot-1",
        priority=5,
    )
    assert result.outcome == "terminal"

    run_span = exporter.span_named("run otel-attrs-flow")
    assert run_span is not None
    attrs = run_span.attributes
    assert attrs is not None
    assert attrs.get("taskq.workflow") == "otel-attrs-flow"
    assert attrs.get("taskq.flow_id") == str(result.flow_id)
    assert attrs.get("taskq.run_key") == "nightly-slot-1"
    assert attrs.get("taskq.queue") == "default", "the root row's queue (the shim's law)"
    assert attrs.get("taskq.priority") == 5
    assert attrs.get("taskq.outcome") == "terminal"

    # THE LIFECYCLE EVENTS (the vanilla consumer span's own grammar):
    running = exporter.events_on("run otel-attrs-flow", "lifecycle.running")
    assert running, "the run's start is the root row's pending → running"
    running_attrs = running[0].attributes
    assert running_attrs is not None
    assert running_attrs.get("from_state") == "pending"
    assert running_attrs.get("to_state") == "running"
    succeeded = exporter.events_on("run otel-attrs-flow", "lifecycle.succeeded")
    assert succeeded, "the run's terminal is an event, the vanilla's own shape"
    succeeded_attrs = succeeded[0].attributes
    assert succeeded_attrs is not None
    assert succeeded_attrs.get("to_state") == "succeeded"

    assert run_span.status is not None
    assert run_span.status.status_code == trace.StatusCode.OK


async def test_the_run_span_omits_the_unset_optionals(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE ABSENCE FACE: no key and no priority → ``taskq.run_key`` and
    ``taskq.priority`` are absent (the minted ``flow:{uuid}`` key is the
    arbiter's business, never a span attribute; the priority's ``when
    set`` law)."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-plain-flow")
    def otel_plain_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    result = await run(app.get("otel-plain-flow"), wf_pool, wf_schema, input={"doc_id": "d"})
    assert result.outcome == "terminal"

    run_span = exporter.span_named("run otel-plain-flow")
    assert run_span is not None
    attrs = run_span.attributes
    assert attrs is not None
    assert attrs.get("taskq.run_key") is None
    assert attrs.get("taskq.priority") is None


# ── 3. the step span's attribute contract ────────────────────────────────


async def test_the_step_span_carries_the_attribute_contract(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE STEP SPAN'S ATTRIBUTES: the flow link, the workflow name, the
    step key, the attempt ordinal, the node's queue, and the OUTCOME
    (the claim tail's own label — the record's echo) with the status
    (OK on the succeeded class). A plain step carries no map_index."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-step-flow")
    def otel_step_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    result = await run(app.get("otel-step-flow"), wf_pool, wf_schema, input={"doc_id": "d"})
    assert result.outcome == "terminal"

    step_span = exporter.span_named("step solo")
    assert step_span is not None
    attrs = step_span.attributes
    assert attrs is not None
    assert attrs.get("taskq.flow_id") == str(result.flow_id)
    assert attrs.get("taskq.workflow") == "otel-step-flow"
    assert attrs.get("taskq.step") == "solo"
    assert attrs.get("taskq.attempt") == 1
    assert attrs.get("taskq.queue") == "default"
    assert attrs.get("taskq.outcome") == "succeeded"
    assert attrs.get("taskq.map_index") is None, "a plain step carries no map_index"

    assert step_span.status is not None
    assert step_span.status.status_code == trace.StatusCode.OK


# ── 4. the failure faces — the ladder, scrubbed ──────────────────────────


async def test_a_failing_step_records_the_ladder_scrubbed(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE LADDER'S TWO FACES ON THE SPAN: attempt 1's span records the
    failure SCRUBBED (the exception event's text is the redaction pass's
    — the DSN password never reaches the exporter) with the
    ``wf.laddered`` event naming the route (retried); the last attempt's
    span names the terminal route (retried false) and dies ERROR; the
    run span carries ``lifecycle.failed`` and ends ERROR. The packaged
    run re-raises the typed failure verdict (the door's own law)."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    async def _doomed_body(ctx: StepContext, params: Ingest) -> Report:
        raise RuntimeError(
            f"boom for {params.doc_id} over postgresql://taskq:hunter2@db.internal:5432/taskq"
        )

    @app.workflow("otel-doomed-flow")
    def otel_doomed_flow() -> Promise[object]:
        return build(step(_doomed_body, Ingest(doc_id="d"), key="doomed", max_attempts=2))

    with pytest.raises(WorkflowRunError):
        await run(app.get("otel-doomed-flow"), wf_pool, wf_schema, input={"doc_id": "d"})

    attempts = exporter.spans_named("step doomed")
    assert len(attempts) == 2, [s.name for s in exporter.spans]

    first, last = attempts[0], attempts[1]
    first_attrs = first.attributes
    last_attrs = last.attributes
    assert first_attrs is not None and last_attrs is not None
    assert first_attrs.get("taskq.attempt") == 1
    assert last_attrs.get("taskq.attempt") == 2
    assert first_attrs.get("taskq.outcome") == "laddered"
    assert last_attrs.get("taskq.outcome") == "laddered"

    first_ladder = [ev for ev in first.events if ev.name == "wf.laddered"]
    last_ladder = [ev for ev in last.events if ev.name == "wf.laddered"]
    assert first_ladder and last_ladder, [ev.name for ev in first.events]
    first_ladder_attrs = first_ladder[0].attributes
    assert first_ladder_attrs is not None
    assert first_ladder_attrs.get("retried") is True, "the ladder re-pended attempt 1"
    last_ladder_attrs = last_ladder[0].attributes
    assert last_ladder_attrs is not None
    assert last_ladder_attrs.get("retried") is False, "the budget exhausted"

    # THE REDACTION LAW: the exception event is the scrubbed render — the
    # canary password is gone, the diagnostic class is kept.
    for span in (first, last):
        exc_events = [ev for ev in span.events if ev.name == "exception"]
        assert exc_events, [ev.name for ev in span.events]
        rendered = str(exc_events[0].attributes)
        assert "hunter2" not in rendered, rendered
        assert "RuntimeError" in rendered, rendered

    assert last.status is not None
    assert last.status.status_code == trace.StatusCode.ERROR

    run_span = exporter.span_named("run otel-doomed-flow")
    assert run_span is not None
    failed = exporter.events_on("run otel-doomed-flow", "lifecycle.failed")
    assert failed, "the failed run's terminal event"
    assert run_span.status is not None
    assert run_span.status.status_code == trace.StatusCode.ERROR


# ── 5. the deferral and the hold — events on the span ────────────────────


async def test_a_deferred_step_records_the_deferral_event(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE DEFERRAL (the body's typed control flow): one tick of the
    runner drives the body's ``Snooze`` — the step span's outcome is the
    tail's own ``deferred`` label, the ``wf.deferred`` event names the
    signal and the delay, and the status stays UNSET (a mid-state, the
    vanilla consumer's own law). The span is a per-process ROOT here —
    the driver's face without a run span around it, exactly the fleet
    worker's shape."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    async def _deferring_body(ctx: StepContext, params: Ingest) -> Report:
        raise Snooze(timedelta(seconds=30))

    @app.workflow("otel-defer-flow")
    def otel_defer_flow() -> Promise[object]:
        return build(step(_deferring_body, Ingest(doc_id="d"), key="deferred"))

    runner = FlowRunner(app.get("otel-defer-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow(input={"doc_id": "d"})).flow_id
    await runner.tick(flow_id)

    step_span = exporter.span_named("step deferred")
    assert step_span is not None, [s.name for s in exporter.spans]
    assert step_span.parent is None, "no run span in this frame — the per-process root"
    step_attrs = step_span.attributes
    assert step_attrs is not None
    assert step_attrs.get("taskq.outcome") == "deferred"

    deferred = [ev for ev in step_span.events if ev.name == "wf.deferred"]
    assert deferred, [ev.name for ev in step_span.events]
    deferred_attrs = deferred[0].attributes
    assert deferred_attrs is not None
    assert deferred_attrs.get("signal") == "Snooze"
    assert deferred_attrs.get("delay_s") == 30.0

    assert step_span.status is None or step_span.status.status_code is trace.StatusCode.UNSET


async def test_a_held_step_records_the_hold_event(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE HOLD (T10): a body at its gate — the step span's outcome is
    ``held``, the ``wf.held`` event names the hold id and the signals,
    the status stays UNSET; the run span (``until='held'``) records the
    drive's stop and NO terminal lifecycle event (the run is not
    terminal — the envelope must not lie)."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-hold-flow")
    def otel_hold_flow() -> Promise[object]:
        return build(
            step(
                _hold_body,
                Ingest(doc_id="d"),
                key="review",
                gates=(GateDecl(name="_Approval", payload_models=(Approval,), timeout_s=120.0),),
            )
        )

    result = await run(
        app.get("otel-hold-flow"), wf_pool, wf_schema, input={"doc_id": "d"}, until="held"
    )
    assert result.outcome == "held"

    step_span = exporter.span_named("step review")
    assert step_span is not None, [s.name for s in exporter.spans]
    step_attrs = step_span.attributes
    assert step_attrs is not None
    assert step_attrs.get("taskq.outcome") == "held"

    held = [ev for ev in step_span.events if ev.name == "wf.held"]
    assert held, [ev.name for ev in step_span.events]
    held_attrs = held[0].attributes
    assert held_attrs is not None
    assert held_attrs.get("signals") is not None

    assert step_span.status is None or step_span.status.status_code is trace.StatusCode.UNSET

    run_span = exporter.span_named("run otel-hold-flow")
    assert run_span is not None
    run_attrs = run_span.attributes
    assert run_attrs is not None
    assert run_attrs.get("taskq.outcome") == "held"
    terminal_events = [
        ev for ev in run_span.events if ev.name.startswith("lifecycle.succeeded")
    ] + [ev for ev in run_span.events if ev.name.startswith("lifecycle.failed")]
    assert not terminal_events, "a held run is not terminal — no terminal event"


# ── 6. the fork and the join fire — events, not spans ────────────────────


async def test_the_fork_and_join_fires_are_events_on_the_step_spans(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE FAN-OUT'S SHAPE (the brief's law — events on the spans, never
    separate spans): the map source's step span carries ``wf.forked``
    (the children's count), and the item's finalize that FIRES the join
    carries ``wf.join_fired``. The join node itself is still just a step
    span — the tree stays flat at the lifecycle points."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-map-flow")
    def otel_map_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src")
        mapped = map_source(source, _item_body)
        return build(mapped)

    result = await run(app.get("otel-map-flow"), wf_pool, wf_schema, input={"doc_id": "d"})
    assert result.outcome == "terminal"

    src_span = exporter.span_named("step src")
    assert src_span is not None, [s.name for s in exporter.spans]
    forked = [ev for ev in src_span.events if ev.name == "wf.forked"]
    assert forked, [ev.name for ev in src_span.events]
    forked_attrs = forked[0].attributes
    assert forked_attrs is not None
    assert forked_attrs.get("children") == 2

    join_fired = [
        (span, ev) for span in exporter.spans for ev in span.events if ev.name == "wf.join_fired"
    ]
    assert join_fired, [s.name for s in exporter.spans]
    fired_span, fired_ev = join_fired[0]
    fired_attrs = fired_span.attributes
    assert fired_attrs is not None
    assert str(fired_attrs.get("taskq.flow_id")) == str(result.flow_id)
    assert fired_attrs.get("taskq.outcome") == "succeeded"
    assert fired_ev.attributes is not None


# ── 7. the fleet door — the per-process root, correlated by flow_id ──────


async def test_the_fleet_door_emits_a_root_step_span(
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE FLEET DOOR (production's own path): the claimed row executes
    through ``execute_flow_job`` — the step span emits THERE, as a
    per-process ROOT (no run span exists in the worker's process — the
    vanilla's cross-process law has no wf producer span to stamp ids
    from) correlated by the ``taskq.flow_id`` attribute. The fork's
    children still ride the span as the ``wf.forked`` event."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-fleet-flow")
    def otel_fleet_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src")
        mapped = map_source(source, _item_body)
        return build(mapped)

    runner = FlowRunner(app.get("otel-fleet-flow"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow(input={"doc_id": "d"})).flow_id

    for config in project_workflow_actor_configs():
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '  # noqa: S608
        "VALUES ($1, 'otel-fleet-pin', 1, '{default}', $2::jsonb)",
        worker_id,
        json.dumps({"workflow_execution": True}),
    )
    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert len(dispatched) == 1, dispatched

    from taskq.backend._records import _job_row_from_record

    execution = await execute_flow_job(
        pool=module_pg_pool,
        schema=wf_schema,
        worker_id=JobId(worker_id),
        job=_job_row_from_record(dispatched[0]),
    )
    assert execution.outcome == "succeeded", execution.outcome

    step_span = exporter.span_named("step src")
    assert step_span is not None, [s.name for s in exporter.spans]
    assert step_span.parent is None, "the fleet step span is a per-process root"
    step_attrs = step_span.attributes
    assert step_attrs is not None
    assert step_attrs.get("taskq.flow_id") == str(flow_id)
    assert step_attrs.get("taskq.workflow") == "otel-fleet-flow"
    assert step_attrs.get("taskq.outcome") == "succeeded"
    forked = [ev for ev in step_span.events if ev.name == "wf.forked"]
    assert forked, [ev.name for ev in step_span.events]


# ── 7b. the ambient link — the run span is linked, never parented ────────


async def test_the_run_span_links_to_the_ambient_trace(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE PRODUCER-LINK PATTERN (the cron fire span's own law): a run()
    called inside an AMBIENT span — a caller's own trace — starts the run
    span as a ROOT that LINKS to the ambient context (``new_root=True`` +
    ``links=``), never parents to it: the caller's trace is not held open
    for the run's lifetime, the link is the correlation."""
    provider, exporter = setup_tracer(monkeypatch)
    ambient_tracer = provider.get_tracer("the-pin's-ambient")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: the test provider's tracer is typed loosely by the SDK.

    app = WorkflowApp()

    @app.workflow("otel-link-flow")
    def otel_link_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    with ambient_tracer.start_as_current_span("the-caller's-trace"):
        result = await run(app.get("otel-link-flow"), wf_pool, wf_schema, input={"doc_id": "d"})
    assert result.outcome == "terminal"

    run_span = exporter.span_named("run otel-link-flow")
    assert run_span is not None
    assert run_span.parent is None, "linked, never parented"
    ambient_ctx = None
    for s in exporter.spans:
        if s.name == "the-caller's-trace":
            ambient_ctx = s.get_span_context()
    assert ambient_ctx is not None
    assert any(link.context.span_id == ambient_ctx.span_id for link in run_span.links), [
        list(run_span.links)
    ]


# ── 7c. the crashed face — the machinery boundary's re-raise ─────────────


async def test_an_escaped_machinery_fault_records_the_crashed_outcome(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE MACHINERY BOUNDARY's re-raise (the reclaim class): an infra
    fault escaping the claim tail's own statements re-raises (the driver
    counts it) — the step span ends ``crashed`` (the ledger's own
    vocabulary) with the scrubbed error the fallback recorded, ERROR
    status, and the row re-pended (the reclaim's own law, untouched)."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-crash-flow")
    def otel_crash_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    runner = FlowRunner(app.get("otel-crash-flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow(input={"doc_id": "d"})).flow_id

    real_finalize = runner._finalize_success

    async def _blowing_finalize(*args: Any, **kwargs: Any) -> Any:
        raise ConnectionError("the finalize's transport blipped")

    runner._finalize_success = _blowing_finalize  # type: ignore[method-assign]  # Why: the pin injects the infra fault at the finalize's own seam — the escape-point contract's face.
    try:
        with pytest.raises(ConnectionError):
            await runner.tick(flow_id)
    finally:
        runner._finalize_success = real_finalize  # type: ignore[method-assign]  # Why: restore the seam (the runner object is this test's own).

    step_span = exporter.span_named("step solo")
    assert step_span is not None, [s.name for s in exporter.spans]
    step_attrs = step_span.attributes
    assert step_attrs is not None
    assert step_attrs.get("taskq.outcome") == "crashed"
    exc_events = [ev for ev in step_span.events if ev.name == "exception"]
    assert exc_events, [ev.name for ev in step_span.events]
    assert step_span.status is not None
    assert step_span.status.status_code == trace.StatusCode.ERROR

    # The row's truth is untouched: the reclaim re-pended the node (the
    # spans never steer the machinery) — the row is back in the pool at
    # the re-pend's own backoff stamp.
    node_status = await wf_pool.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs '  # noqa: S608
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'solo'",
        flow_id,
    )
    assert node_status == "scheduled", node_status


# ── 8. the no-op law — telemetry off / provider absent ───────────────────


async def test_otel_disabled_flow_runs_and_exports_nothing(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE NO-OP LAW (the vanilla's own): telemetry off → the flow runs
    to terminal, ZERO spans export, nothing raises."""
    _, exporter = setup_tracer(monkeypatch)
    set_otel_enabled(False)
    app = WorkflowApp()

    @app.workflow("otel-off-flow")
    def otel_off_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    result = await run(app.get("otel-off-flow"), wf_pool, wf_schema, input={"doc_id": "d"})
    assert result.outcome == "terminal", result.outcome
    assert exporter.spans == [], [s.name for s in exporter.spans]


async def test_otel_provider_absent_flow_still_runs(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """THE ABSENT-PROVIDER FACE: no SDK configured (the default no-op
    tracer) → the flow runs to terminal, no import fails, no raise —
    the library never installs a provider itself (the deployment
    default)."""
    app = WorkflowApp()

    @app.workflow("otel-noprovider-flow")
    def otel_noprovider_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="solo"))

    result = await run(app.get("otel-noprovider-flow"), wf_pool, wf_schema, input={"doc_id": "d"})
    assert result.outcome == "terminal", result.outcome


# ── 9. the no-behavior-change law: the rows are byte-identical ───────────


async def test_the_drive_outcome_and_rows_are_byte_identical_under_spans(
    wf_schema: str, wf_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE NO-BEHAVIOR-CHANGE LAW, pinned: the same flow driven under the
    spans ends at the same terminal, with the same rows (the root's
    status, the node's statuses, the map's children) — the spans wrap
    the lifecycle points, they never steer them."""
    _, exporter = setup_tracer(monkeypatch)
    app = WorkflowApp()

    @app.workflow("otel-parity-flow")
    def otel_parity_flow() -> Promise[object]:
        source = step(_list_source, Ingest(doc_id="d"), key="src")
        mapped = map_source(source, _item_body)
        return build(mapped)

    compiled = app.get("otel-parity-flow")

    # The span run.
    result = await run(compiled, wf_pool, wf_schema)
    assert result.outcome == "terminal"
    spanned_rows = await wf_pool.fetch(
        f'SELECT step_key, status, attempt FROM "{wf_schema}".jobs '  # noqa: S608
        f"WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY step_key, map_index",
        result.flow_id,
    )

    # The bare run (telemetry off — the flag off is the cheaper gate; the
    # autouse guard restores the flag at teardown).
    set_otel_enabled(False)
    bare = await run(compiled, wf_pool, wf_schema)
    set_otel_enabled(True)
    assert bare.outcome == "terminal"
    bare_rows = await wf_pool.fetch(
        f'SELECT step_key, status, attempt FROM "{wf_schema}".jobs '  # noqa: S608
        f"WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY step_key, map_index",
        bare.flow_id,
    )
    assert [tuple(r) for r in spanned_rows] == [tuple(r) for r in bare_rows]
    assert exporter.span_named("run otel-parity-flow") is not None
