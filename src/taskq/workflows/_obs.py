"""The workflow span contract — the wf faces of the obs layer's otel law.

WHY THIS MODULE EXISTS: the vanilla lifecycle emits spans at every
messaging point (``enqueue {actor}``, ``process {actor}``,
``attempt.{N}``, ``dispatch``, ``cron fire`` — see ``worker/dispatch.py``,
``worker/_consumer.py``, ``worker/cron_loop.py``) and the workflows
package emitted ZERO: the fleet intercept bypasses ``consume_one_job``
(the wf rows never ride the vanilla consumer path) and the dev drive
never enters it, so a consumer running otel (an Azure Monitor or a
ddtrace backend) sees its flows as silence.

THE CURE reuses the vanilla machinery — no new otel machinery, no new
metrics (the vanilla's one wf-relevant instrument is the maintenance
leader's fleet gauge ``taskq.wf_progress_nodes_total``; no per-run wf
metric exists in the vanilla to mirror):

* the SAME :func:`taskq.obs.safe_start_span` (the no-op law: telemetry
  off → a non-recording span, the flow runs, nothing fails);
* the SAME scrubbed-error recording (``render_exception`` /
  ``record_exception_text`` — caller-supplied values never leak
  unredacted);
* the SAME naming grammar (lowercase phrases beside the vanilla's
  ``process {actor}`` / ``cron fire``): the RUN span is ``run
  {workflow}``, the STEP span is ``step {step_key}``;
* the SAME ``taskq.*`` attribute namespace (the vanilla's
  ``taskq.actor`` / ``taskq.attempt`` / ``taskq.queue`` family).

THE SPAN TREE
-------------

``run {workflow}`` — the RUN span: a ROOT (``new_root=True``, the
producer-link pattern — linked, never parented, to the ambient trace),
created at the flow's first claim (the run-key claim the packaged
:func:`taskq.workflows.run` performs) and ended at the run's terminal
(the drive's stop). One per ``run()`` call — the ROOT run's; child
flows (T28) spawn through ``create_flow`` and carry no run span of
their own (their steps ride the tree below them).

``step {step_key}`` — the STEP span: ONE attempt of one node through
the claim tail (``FlowRunner._execute_claimed`` — the ONE tail both
doors land in). The parent is the run span WHEN both execute in one
process (the dev drive — otel's in-process context propagation). On a
fleet worker there is no run span in that process, so the step span is
a per-process ROOT: the vanilla's own cross-process law is
producer-stamped ids on the job row + a CONSUMER-side link (the
``enqueue`` span → the ``process`` span's link), and the wf rows have
no producer span to stamp otel ids from (the row's ``trace_id`` column
carries the wf per-record lineage id — a different concept, §18.2), so
the cross-process correlation is the ``taskq.flow_id`` attribute. No
traceparent crosses processes — adding one would be new machinery
(propagator injection into the node INSERT), deliberately out of
scope.

THE OUTCOMES
------------

``taskq.outcome`` echoes the claim tail's own label (the ROW is the
truth, the span is the record's echo — the tail's docstring's law):
``succeeded`` / ``skipped`` / ``loop`` / ``held`` / ``deferred`` /
``fenced`` / ``laddered`` / ``cancelled``, and ``crashed`` for the
machinery boundary's re-raise (the ledger's own vocabulary). The
status maps like the vanilla consumer span's (``process {actor}``:
OK on the succeeded class, ERROR on the failed class, the mid-states
UNSET); the ladder's failure records SCRUBBED; the fork / join-fire /
hold / deferral are span EVENTS (``span.add_event``), not separate
spans — cheaper, honest.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from opentelemetry.trace import Span, StatusCode

from taskq.exceptions import RetryAfter, Snooze
from taskq.obs import otel_enabled, record_exception_text, render_exception

__all__ = [
    "finish_step_span",
    "record_run_started",
    "record_run_terminal",
    "record_step_deferred",
    "record_step_forked",
    "record_step_held",
    "record_step_join_fired",
    "record_step_laddered",
    "run_span_attributes",
    "run_span_name",
    "step_span_attributes",
    "step_span_name",
]

#: The run span's ``taskq.queue``: the ROOT row's queue. The run-key
#: claim inserts the root through :class:`~taskq.workflows.api.
#: _runner_codec.FlowEntryShim`, whose queue default is the constant —
#: the root's placement is a constant by construction (the STEPS carry
#: their own queues; the step span's ``taskq.queue`` is the node's).
ROOT_QUEUE: Final[str] = "default"

#: The step span's status mapping — the tail's outcome labels onto the
#: vanilla consumer span's own law (``dispatch.py``: OK on the
#: succeeded class, ERROR on the failed class, the mid-states UNSET).
#: The mirror of the fleet door's ``_FLOW_OUTCOME_TO_CONSUMED`` (the
#: consumed vocabulary), kept as its own table so the span contract
#: names the labels it means without importing the worker seam.
_OK_OUTCOMES: Final[frozenset[str]] = frozenset({"succeeded", "skipped", "loop"})


def run_span_name(workflow: str) -> str:
    """The RUN span's name — the vanilla grammar's verb-first phrase."""
    return f"run {workflow}"


def step_span_name(step_key: str) -> str:
    """The STEP span's name — the vanilla grammar's verb-first phrase.
    The map's children keep the node's own step key (``src.item``); the
    per-item ordinal rides the ``taskq.map_index`` attribute, never the
    name (step keys themselves carry dots)."""
    return f"step {step_key}"


def run_span_attributes(
    *,
    workflow: str,
    run_key: str | None,
    priority: int | None,
) -> dict[str, str | int] | None:
    """The RUN span's attribute document, or ``None`` when telemetry is
    off — the vanilla's gated-allocation law (``dispatch.py``'s
    ``consumer_attrs``): these entries' only reader is the span, so with
    telemetry off nothing is built. ``taskq.run_key`` and
    ``taskq.priority`` ride only when the caller declared them (the
    minted ``flow:{uuid}`` key is the arbiter's business; the
    priority's own law is ``when set``)."""
    if not otel_enabled():
        return None
    attrs: dict[str, str | int] = {
        "taskq.workflow": workflow,
        "taskq.queue": ROOT_QUEUE,
    }
    if run_key is not None:
        attrs["taskq.run_key"] = run_key
    if priority is not None:
        attrs["taskq.priority"] = priority
    return attrs


def step_span_attributes(
    *,
    flow_id: str,
    workflow: str,
    step_key: str,
    attempt: int,
    queue: str | None = None,
    map_index: int | None = None,
) -> dict[str, str | int] | None:
    """The STEP span's attribute document, ``None`` when telemetry is
    off (the same gated-allocation law). ``taskq.queue`` rides when the
    node's placement is known (a compiled node's declaration — the
    fork-spawned map children have no decl of their own);
    ``taskq.map_index`` rides on the map's children."""
    if not otel_enabled():
        return None
    attrs: dict[str, str | int] = {
        "taskq.flow_id": flow_id,
        "taskq.workflow": workflow,
        "taskq.step": step_key,
        "taskq.attempt": attempt,
    }
    if queue is not None:
        attrs["taskq.queue"] = queue
    if map_index is not None:
        attrs["taskq.map_index"] = map_index
    return attrs


def record_run_started(span: Span, flow_id: str) -> None:
    """Stamp the run's identity + the start event on the RUN span.

    Called by the packaged run the moment the run-key claim returned:
    the span opened before the claim (the claim rides the span), so the
    flow id — unknown until the arbiter answered — lands as a
    mid-flight attribute, and the vanilla lifecycle event's grammar
    (``lifecycle.running``, ``from_state``/``to_state``) marks the
    root row's pending → running."""
    span.set_attribute("taskq.flow_id", flow_id)
    span.add_event(
        "lifecycle.running",
        attributes={"from_state": "pending", "to_state": "running"},
    )


def record_run_terminal(span: Span, state: str) -> None:
    """Stamp the run's terminal on the RUN span: the lifecycle event
    (the vanilla's grammar) and the status — OK on a succeeded run,
    ERROR on a failed one, UNSET on a cancelled one (the vanilla
    consumer span's own law: only the failure is an error). A state
    outside the root row's terminal set records nothing (the
    envelope must not lie about a state it did not see)."""
    if not span.is_recording():
        return
    if state not in ("succeeded", "failed", "cancelled"):
        return
    span.add_event(
        f"lifecycle.{state}",
        attributes={"from_state": "running", "to_state": state},
    )
    if state == "succeeded":
        span.set_status(StatusCode.OK)
    elif state == "failed":
        span.set_status(StatusCode.ERROR)


def finish_step_span(span: Span, outcome: str) -> None:
    """Stamp the claim tail's outcome on the STEP span.

    ``taskq.outcome`` is the tail's own label (the record's echo); the
    status follows the vanilla consumer span's law via
    :data:`_OK_OUTCOMES`. The ladder's failure face is NOT set here —
    the ladder arm (the site with the exception) already recorded the
    scrubbed text and the ERROR status;
    :func:`record_step_laddered`'s ERROR set survives this function's
    UNSET-or-OK logic because an ERROR status is never lowered."""
    span.set_attribute("taskq.outcome", outcome)
    if not span.is_recording():
        return
    if outcome in _OK_OUTCOMES:
        status_code = getattr(getattr(span, "status", None), "status_code", None)
        if status_code is not StatusCode.ERROR:
            span.set_status(StatusCode.OK)
    elif outcome == "laddered":
        status_code = getattr(getattr(span, "status", None), "status_code", None)
        if status_code is not StatusCode.ERROR:
            span.set_status(StatusCode.ERROR)


def record_step_laddered(span: Span, exc: Exception, *, retried: bool) -> None:
    """Record the ladder's verdict on the STEP span — SCRUBBED.

    THE REDACTION LAW (the vanilla consumer's own idiom,
    ``_consumer.py``'s attempt span): the exception is rendered through
    :func:`taskq.obs.render_exception` once — the pass that strips the
    Postgres DETAIL row values and masks the URI credentials — and
    ``record_exception_text`` sets the ERROR status described by the
    scrubbed message plus the ``exception`` event. The ``wf.laddered``
    event names the route the ladder took (``retried`` — the node
    re-pends; ``False`` — the budget exhausted, the terminal landed),
    the distinction the row's ledger carries and the span echoes.

    A non-recording span takes nothing: rendering is the dominant cost
    of a failed attempt (``_record_scrubbed_error``'s own law)."""
    if not span.is_recording():
        return
    span.add_event(
        "wf.laddered",
        attributes={"error_class": type(exc).__qualname__, "retried": retried},
    )
    record_exception_text(span, render_exception(exc))


def record_step_held(span: Span, hold_id: str, signals: Sequence[str]) -> None:
    """Record the hold (T10) on the STEP span as the ``wf.held`` event.

    The hold is not a terminal and not a failure — the node rests in
    the held representation and the resume re-executes the body; the
    event names the hold id and the signals the body waits on, the
    same truth the runner's log line carries."""
    if not span.is_recording():
        return
    span.add_event("wf.held", attributes={"hold_id": hold_id, "signals": list(signals)})


def record_step_deferred(span: Span, exc: Snooze | RetryAfter) -> None:
    """Record the deferral (the body's typed control flow) on the STEP
    span as the ``wf.deferred`` event: the signal's class and the delay
    it asked for. The deferral is not a failure — the attempt is
    refunded, the row re-schedules — so the status stays untouched."""
    if not span.is_recording():
        return
    span.add_event(
        "wf.deferred",
        attributes={"signal": type(exc).__name__, "delay_s": exc.delay.total_seconds()},
    )


def record_step_forked(span: Span, children: int) -> None:
    """Record the success-finalize's fork on the STEP span as the
    ``wf.forked`` event — the map's children (the fan-out) are rows the
    finalize materialized atomically with the terminal mark, never
    spans of their own (the tree stays flat at the lifecycle points;
    the per-child truth is the rows and the run page)."""
    if not span.is_recording():
        return
    span.add_event("wf.forked", attributes={"children": children})


def record_step_join_fired(span: Span, step_keys: Sequence[str]) -> None:
    """Record the finalize's fired joins on the STEP span as the
    ``wf.join_fired`` event — the fan-in's decrement reached zero and
    the join row fired in the same tx. The event names the join step
    keys that fired; the join's own execution is (like every node) a
    step span, not a special one."""
    if not span.is_recording():
        return
    span.add_event("wf.join_fired", attributes={"joins": list(step_keys)})
