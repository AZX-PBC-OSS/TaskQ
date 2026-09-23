"""Behavioural tests for the job-detail progress driver in ``static/realtime.js``.

Driven under Node with the browser surface stubbed (``document``, ``fetch``,
``EventSource``, timers), the same shape as the admin.js harness: assertions
are on which requests the module issues and how the mode badge reacts, not on
source text. Node is the only JS runtime the repository has; the tests skip
where it is absent.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = [pytest.mark.fastapi]

pytest.importorskip("fastapi")

REALTIME_JS = (
    Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "realtime.js"
)

requires_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="realtime.js behaviour is driven under Node"
)

# A minimal browser: the badge and progress section are the two elements the
# module touches; fetch answers /sse/mode with the scripted verdict and the
# poll with scenario-specific snapshots; timers are virtual.
_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 2], "utf8");
const scenario = process.argv[process.argv.length - 1];

const log = [];
let now = 0;
let timers = [];
let nextTimer = 1;
let wantRealtime = [
    "redis-returns",
    "realtime-empty-progress",
    "realtime-progress",
    "initial-progress",
    "transient-sse-error",
    "terminal-poll-recovery",
    "repeated-progress",
    "timestamped-progress",
    "terminal-trailing-seq",
    "empty-recovery-wipe",
    "catchup-rendered-once",
    "cursor-advance",
    "replayed-delta",
].includes(scenario);

global.window = { TASKQ_BASE_PATH: "/taskq" };
global.POLL_INTERVAL_MS = 1000;

const badge = {
    attrs: {
        "data-mode": scenario === "redis-returns"
            ? "polling-degraded"
            : scenario.startsWith("polling-")
                ? "polling"
                : "realtime"
    },
    textContent: "",
    setAttribute(k, v) { this.attrs[k] = v; },
    getAttribute(k) { return this.attrs[k]; },
};
const initialProgress = scenario === "initial-progress"
    ? { seq: 2, state: { percent: 50 } }
    : { seq: 1, state: {} };
const section = {
    attrs: {
        "data-job-id": "j1",
        "data-progress-seq": String(initialProgress.seq),
        "data-progress-state": JSON.stringify(initialProgress.state),
    },
    getAttribute(k) { return this.attrs[k]; },
};
const timeline = {
    appendChild(entry) {
        log.push("append-progress");
        const meta = entry.children.find((child) => child.className === "progress-meta");
        if (meta) log.push("progress-meta:" + meta.textContent);
    },
};
const elements = {
    "progress-section": section,
    "progress-timeline": timeline,
};

global.document = {
    addEventListener(name, fn) { if (name === "DOMContentLoaded") fn(); },
    getElementById(id) { return elements[id] ?? null; },
    querySelector(sel) { return sel === ".taskq-badge" ? badge : null; },
    createElement() {
        return {
            children: [],
            className: "",
            textContent: "",
            style: {},
            appendChild(child) { this.children.push(child); },
            scrollIntoView() {},
        };
    },
};

global.EventSource = class {
    constructor(url) {
        this.url = url;
        this.handlers = {};
        global.lastEventSource = this;
        log.push("sse-open:" + url);
    }
    addEventListener(name, fn) { this.handlers[name] = fn; }
    close() { log.push("sse-close"); }
    emit(name, data, lastEventId) {
        this.handlers[name]({ data: JSON.stringify(data), lastEventId });
    }
    emitOpen() { this.handlers.open({}); }
    emitError() { this.handlers.error({}); }
};

global.setInterval = (fn, ms) => { const id = nextTimer++; timers.push({ id, fn, ms, due: now + ms }); return id; };
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };

let pollCount = 0;
const routes = [
    {
        condition: ({ url }) => url.endsWith("/sse/mode"),
        handler: () => ({ realtime: wantRealtime }),
    },
    {
        condition: ({ scenario }) => scenario === "polling-empty-progress",
        handler: () => ({
            status: "succeeded",
            progress_state: {},
            progress_seq: 2,
        }),
    },
    {
        condition: ({ scenario }) => scenario === "polling-progress",
        handler: () => {
            pollCount += 1;
            return pollCount === 1
                ? { status: "running", progress_state: { percent: 50 }, progress_seq: 2 }
                : { status: "succeeded", progress_state: { percent: 50 }, progress_seq: 3 };
        },
    },
    {
        condition: ({ scenario }) => scenario === "sse-to-polling",
        handler: () => {
            pollCount += 1;
            return {
                status: "succeeded",
                progress_state: { detail: "phase", percent: 50, data: { a: 1, b: 2 } },
                progress_seq: 4,
            };
        },
    },
    {
        condition: ({ scenario }) => scenario === "terminal-poll-recovery",
        handler: () => ({
            status: "succeeded",
            progress_state: { percent: 100 },
            progress_seq: 2,
        }),
    },
    {
        // The durable row trails the fanout: the deltas this page merged
        // were never flushed (paused broker), the attempt died, the
        // reclaim consumed its seq without carrying the dead attempt's
        // unflushed work. The poll's terminal snapshot arrives with a
        // sequence BELOW the deltas already rendered.
        condition: ({ scenario }) => scenario === "terminal-trailing-seq",
        handler: () => ({
            status: "crashed",
            progress_state: { percent: 90, detail: "reclaimed" },
            progress_seq: 4,
        }),
    },
];

function resolveBody(context) {
    const route = routes.find(({ condition }) => condition(context));
    return route?.handler(context) ?? {};
}

global.fetch = (url) => ({
    then(f1) {
        log.push("fetch:" + url.split("?")[0]);
        const body = resolveBody({ url, scenario });
        return { then(f2) { f2(f1({ json: () => body })); return { catch() {} }; } };
    },
});

function advance(ms) {
    const target = now + ms;
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due; due.due += due.ms; due.fn();
    }
    now = target;
}

new Function(src)();
if (scenario === "realtime-empty-progress") {
    global.lastEventSource.emit("terminal", {}, "2");
} else if (scenario === "realtime-progress") {
    global.lastEventSource.emit("progress", { kind: "progress", detail: "retained" }, "2");
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "3");
    global.lastEventSource.emit(
        "terminal",
        { kind: "state_change", percent: 50, terminal: true },
        "4",
    );
} else if (scenario === "sse-to-polling") {
    global.lastEventSource.emit("progress", { kind: "progress", detail: "phase" }, "2");
    global.lastEventSource.emit(
        "progress",
        { kind: "progress", data: { b: 2, a: 1 }, percent: 50 },
        "3",
    );
} else if (scenario === "initial-progress") {
    global.lastEventSource.emit("progress", { percent: 50 }, "2");
    global.lastEventSource.emit("terminal", { percent: 50, terminal: true }, "3");
} else if (scenario === "transient-sse-error") {
    global.lastEventSource.emitError();
    advance(1000);
    global.lastEventSource.emitOpen();
} else if (scenario === "terminal-poll-recovery") {
    global.lastEventSource.emitError();
    advance(1000);
} else if (scenario === "repeated-progress") {
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "2");
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "3");
} else if (scenario === "timestamped-progress") {
    global.lastEventSource.emit(
        "progress",
        { kind: "progress", percent: 50, ts: "2026-01-01T00:00:00Z" },
        "2",
    );
} else if (scenario === "terminal-trailing-seq") {
    global.lastEventSource.emit("progress", { kind: "progress", percent: 90 }, "5");
    global.lastEventSource.emit("progress", { kind: "progress", detail: "step" }, "6");
    global.lastEventSource.emitError();
    advance(1000);
} else if (scenario === "empty-recovery-wipe") {
    global.lastEventSource.emit("progress", { kind: "progress", percent: 90 }, "2");
    // The reconnect catch-up: the durable row had nothing (the deltas were
    // never flushed), the server answers the recovery with an empty
    // absolute snapshot.
    global.lastEventSource.emit("progress", {}, "6");
    global.lastEventSource.emit("progress", { kind: "progress", detail: "x" }, "7");
    global.lastEventSource.emit("progress", { percent: 90, detail: "x" }, "8");
} else if (scenario === "catchup-rendered-once") {
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "2");
    // Reconnect catch-up: one absolute snapshot above the cursor...
    global.lastEventSource.emit("progress", { percent: 75, detail: "catchup" }, "4");
    // ...then the poll observes the same durable state by sequence.
    advance(1000);
} else if (scenario === "cursor-advance") {
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "2");
    // A deduped absolute snapshot ABOVE the cursor still moves the cursor.
    global.lastEventSource.emit("progress", { percent: 50 }, "5");
    // A stale transport replay from below it must stay dropped.
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "3");
} else if (scenario === "replayed-delta") {
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "2");
    // The same sequence delivered a second time (transport replay).
    global.lastEventSource.emit("progress", { kind: "progress", percent: 50 }, "2");
}
advance(32000);
log.push("mode:" + badge.attrs["data-mode"]);
process.stdout.write(JSON.stringify(log));
"""


def _drive(scenario: str) -> list[str]:
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and scenario names are this file's own constants.
        [node, "-e", _HARNESS, "--", str(REALTIME_JS), scenario],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout)


@requires_node
def test_mode_probe_polls_the_sse_mode_endpoint_not_the_worker_health_route() -> None:
    """The 30 s probe asks the admin router's own /sse/mode. It previously
    fetched the worker health router's /jobs/health/ready, which a mounted
    admin UI never serves, so the probe 404ed forever and a degraded page
    never recovered."""
    log = _drive("realtime-stays")
    assert "fetch:/taskq/sse/mode" in log
    assert not any("health/ready" in entry for entry in log)


@requires_node
def test_redis_returning_upgrades_a_degraded_page_to_realtime() -> None:
    """A page that rendered polling-degraded upgrades itself when the probe
    reports Redis back: badge flips to real-time mode and the SSE stream for
    the job opens without a manual reload."""
    log = _drive("redis-returns")
    assert "fetch:/taskq/sse/mode" in log
    assert "sse-open:/taskq/jobs/api/job/j1/progress/stream" in log
    assert log[-1] == "mode:realtime"


@requires_node
def test_redis_failing_degrades_a_realtime_page_to_polling() -> None:
    """The inverse transition still works: the stream closes and the poll
    takes over when the probe reports Redis gone."""
    log = _drive("realtime-stays")
    assert "sse-close" in log
    assert log[-1] == "mode:polling-degraded"


@requires_node
def test_polling_does_not_render_the_empty_initial_progress_state() -> None:
    """A terminal job that never reported progress keeps the empty-state UI.

    Lifecycle transitions consume sequences even when no actor progress was
    reported. The polling driver must not turn that empty state into a
    synthetic 0% timeline entry.
    """
    log = _drive("polling-empty-progress")
    assert "fetch:/taskq/jobs/api/job/j1/state" in log
    assert "append-progress" not in log


@requires_node
def test_polling_renders_a_later_progress_update_once() -> None:
    """A terminal sequence does not duplicate the last polled progress."""
    log = _drive("polling-progress")
    assert log.count("append-progress") == 1


@requires_node
def test_realtime_does_not_render_the_empty_initial_progress_state() -> None:
    """A nonzero terminal lifecycle event without progress stays empty."""
    log = _drive("realtime-empty-progress")
    assert "sse-open:/taskq/jobs/api/job/j1/progress/stream" in log
    assert "append-progress" not in log
    assert "sse-close" in log
    assert "fetch:/taskq/jobs/api/job/j1/state" not in log


@requires_node
def test_realtime_renders_a_later_progress_update_once() -> None:
    """An accumulated terminal state does not duplicate a progress delta."""
    log = _drive("realtime-progress")
    assert log.count("append-progress") == 2


@requires_node
def test_sse_to_polling_deduplicates_the_last_realtime_progress_event() -> None:
    """Polling deduplicates an accumulated snapshot after SSE deltas."""
    log = _drive("sse-to-polling")
    assert log.count("append-progress") == 2


@requires_node
def test_initial_sse_snapshot_does_not_duplicate_server_rendered_progress() -> None:
    """The persisted sequence and state initialize the browser cursor."""
    log = _drive("initial-progress")
    assert log.count("append-progress") == 0


@requires_node
def test_transient_sse_error_keeps_native_eventsource_reconnect() -> None:
    """A disconnect keeps native reconnect while polling durable state."""
    log = _drive("transient-sse-error")
    assert "sse-close" not in log
    assert log.count("fetch:/taskq/jobs/api/job/j1/state") == 1
    assert log[-1] == "mode:realtime"


@requires_node
def test_terminal_poll_closes_recovering_eventsource() -> None:
    """Durable terminal state stops both polling and native SSE recovery."""
    log = _drive("terminal-poll-recovery")
    assert log.count("fetch:/taskq/jobs/api/job/j1/state") == 1
    assert log.count("sse-close") == 1


@requires_node
def test_repeated_actor_progress_calls_render_at_distinct_sequences() -> None:
    """Equal actor updates are events, unlike duplicate lifecycle snapshots."""
    log = _drive("repeated-progress")
    assert log.count("append-progress") == 2


@requires_node
def test_realtime_progress_retains_its_timestamp() -> None:
    """The progress renderer receives timestamps from Redis envelopes."""
    log = _drive("timestamped-progress")
    assert any(entry.startswith("progress-meta:50% · ") for entry in log)


# ── Conservation attacks on the recovery skip (the #478 surface) ──────────


@requires_node
def test_terminal_snapshot_with_a_trailing_sequence_is_never_suppressed() -> None:
    """A terminal durable snapshot below the merged delta cursor still renders.

    The deltas this page merged ride Redis ahead of the coalesced flush; a
    reclaim that ends the attempt writes its seq without carrying the dead
    attempt's unflushed deltas, so the poll's terminal snapshot arrives with
    a progress_seq BELOW the cursor the deltas moved. Gating absolute
    snapshots on that cursor loses the terminal state to the UI: the poll
    then stops and closes the stream, so no later event can restore it.
    """
    log = _drive("terminal-trailing-seq")
    # One entry per merged delta, then the terminal durable truth.
    assert log.count("append-progress") == 3
    # The terminal poll still stops both drivers.
    assert log.count("sse-close") == 1


@requires_node
def test_empty_recovery_snapshot_leaves_the_accumulator_whole() -> None:
    """An empty recovery snapshot skips wholly: no accumulator wipe.

    The recovery's empty absolute snapshot must carry no side effects. With
    the wipe, the accumulated percent is dropped and the later full
    snapshot re-renders a state the page had already shown; with the
    accumulator left whole, that snapshot dedupes against the accumulated
    fingerprint - no lost percent, no duplicate entry.
    """
    log = _drive("empty-recovery-wipe")
    # delta(90%) + delta(detail); the seq-8 snapshot dedupes against the
    # accumulated fingerprint instead of re-rendering the combined state.
    assert log.count("append-progress") == 2


@requires_node
def test_reconnect_catchup_snapshot_is_delivered_exactly_once() -> None:
    """The catch-up snapshot and the poll's equal-seq state render once.

    The double-delivery shape: the recovery's absolute snapshot at seq N,
    then the poll's full path reading the same durable seq N. The content
    dedupe (not the sequence gate) collapses the pair - the sequence gate
    alone would let a BELOW-cursor terminal through instead.
    """
    log = _drive("catchup-rendered-once")
    assert log.count("append-progress") == 2


@requires_node
def test_transport_replay_of_a_delivered_sequence_is_dropped() -> None:
    """A delta sequence delivered twice renders once.

    The exactly-once discipline of the delta fast path: the same seq can
    reach the page twice (a pub/sub replay after reconnect, a duplicated
    frame); the cursor drops the second copy.
    """
    log = _drive("replayed-delta")
    assert log.count("append-progress") == 1


@requires_node
def test_deduped_snapshot_still_advances_the_cursor() -> None:
    """A content-deduped snapshot above the cursor still moves the cursor.

    Dedupe suppresses only the render; the cursor must still move, or a
    stale transport replay from below the snapshot's sequence renders a
    state the page had already passed.
    """
    log = _drive("cursor-advance")
    assert log.count("append-progress") == 1
