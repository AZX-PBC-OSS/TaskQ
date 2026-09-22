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
const timeline = { appendChild() { log.push("append-progress"); } };
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
            style: {},
            appendChild() {},
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
        { kind: "state_change", percent: 50, detail: "retained", terminal: true },
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
    assert "fetch:/taskq/jobs/api/job/j1/state" in log
    assert log[-1] == "mode:realtime"
