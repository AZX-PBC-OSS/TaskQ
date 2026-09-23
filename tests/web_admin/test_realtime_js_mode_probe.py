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
    "redispatch-terminal-envelope",
    "reconnect-terminal-snapshot",
    "terminal-poll-behind-cursor",
    "empty-terminal-behind-cursor",
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
    : scenario === "redispatch-terminal-envelope"
        || scenario === "reconnect-terminal-snapshot"
        || scenario === "terminal-poll-behind-cursor"
        || scenario === "empty-terminal-behind-cursor"
        ? { seq: 5, state: { percent: 90 } }
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
        condition: ({ scenario }) => scenario === "terminal-poll-behind-cursor",
        handler: () => ({
            status: "failed",
            progress_state: { percent: 25, detail: "attempt 2 failed" },
            progress_seq: 2,
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
} else if (scenario === "redispatch-terminal-envelope") {
    // The redispatch shape: the page consumed attempt 1's wire seqs (the
    // hydrated cursor sits at 5), the crash ate the flush, and attempt 2's
    // terminal envelope carries a re-seeded seq BELOW that cursor. The
    // stream's terminal exemption exists to deliver exactly this frame.
    global.lastEventSource.emit(
        "terminal",
        { kind: "state_change", percent: 25, detail: "attempt 2 failed", terminal: true },
        "2",
    );
} else if (scenario === "reconnect-terminal-snapshot") {
    // The reconnect shape: the durable row is terminal at a seq below the
    // cursor, so the snapshot replayed from the row (no kind, the PG state
    // shape) arrives behind it and the stream closes on delivery.
    global.lastEventSource.emit(
        "terminal",
        { percent: 25, detail: "attempt 2 failed" },
        "2",
    );
} else if (scenario === "terminal-poll-behind-cursor") {
    global.lastEventSource.emitError();
    advance(1000);
} else if (scenario === "empty-terminal-behind-cursor") {
    // The lifecycle-only terminal (no progress content) delivered behind
    // the cursor: the exemption must not turn it into a synthetic entry.
    global.lastEventSource.emit("terminal", { kind: "state_change", terminal: true }, "2");
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


# ── The terminal exemption: a terminal behind the client cursor lands ──


@requires_node
def test_redispatched_attempt_terminal_envelope_renders_behind_the_cursor() -> None:
    """A terminal envelope below the client cursor is rendered, never dropped.

    The cursor can legitimately sit ABOVE a terminal's seq: publishes ride
    Redis out as progress calls land while the coalesced flush lands the
    seq up to half a second later, so a crash in between leaves the durable
    row (and the redispatched attempt's re-seeded seqs) behind what the page
    already consumed. The hydrated cursor here is 5 (attempt 1's wire seqs);
    attempt 2's terminal envelope carries the re-seeded seq 2 with attempt
    2's own state. Dropping it freezes the page on attempt 1's stale 90%
    forever - the job is durably over, no later seq recovers the page.
    """
    log = _drive("redispatch-terminal-envelope")
    assert log.count("append-progress") == 1
    assert "progress-meta:25%" in log
    assert "sse-close" in log


@requires_node
def test_reconnect_terminal_snapshot_renders_behind_the_cursor() -> None:
    """A terminal snapshot replayed from the durable row lands behind the cursor.

    The reconnect delivers the row's state (no kind - the PG snapshot shape)
    with the stream's close signal. The cursor must not discard it: this is
    the delivery the stream exists to make when the row went terminal while
    the page's cursor was inflated by unflushed wire seqs.
    """
    log = _drive("reconnect-terminal-snapshot")
    assert log.count("append-progress") == 1
    assert "progress-meta:25%" in log
    assert "sse-close" in log


@requires_node
def test_poll_delivers_the_durable_terminal_row_behind_the_cursor() -> None:
    """A poll body whose durable terminal seq sits below the cursor still lands.

    The polling driver is the recovery surface for exactly the cut that
    inflates the cursor: Redis delivered the pre-crash seqs, the flush was
    lost, the redispatched attempt's terminal write landed the row at a seq
    below them. The old poll-local cursor (-1) rendered this body; the
    shared cursor must not un-render it.
    """
    log = _drive("terminal-poll-behind-cursor")
    assert "fetch:/taskq/jobs/api/job/j1/state" in log
    assert log.count("append-progress") == 1
    assert "progress-meta:25%" in log
    assert log.count("sse-close") == 1


@requires_node
def test_empty_terminal_behind_the_cursor_stays_unrendered() -> None:
    """The terminal exemption does not resurrect the synthetic empty entry.

    A lifecycle-only terminal (no progress content) delivered behind the
    cursor renders nothing - the empty-snapshot skip this module exists for
    holds on the exemption path - and the stream still closes on it.
    """
    log = _drive("empty-terminal-behind-cursor")
    assert "append-progress" not in log
    assert "sse-close" in log
