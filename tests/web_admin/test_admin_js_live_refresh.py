"""Behavioural tests for the jobs page component in ``static/admin.js``.

The component is driven under Node with the browser surface it touches
stubbed (``document``, ``Alpine.data``, ``EventSource``, timers, ``fetch``),
so the assertions are on what the page does — which requests it issues and
when — not on the source text. Node is the only JS runtime the repository
has; the tests skip where it is absent.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

ADMIN_JS = Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "admin.js"

requires_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="admin.js behaviour is driven under Node"
)

# A minimal browser: Alpine registers the component factory; EventSource,
# setInterval and fetch record what the component does with them; the
# clock is virtual and advanced by the scenario.
_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 2], "utf8");
const scenario = process.argv[process.argv.length - 1];

const log = [];
let components = {};
let listeners = {};
let now = 0;
let timers = [];
let nextTimer = 1;

global.window = { __taskqJobConfig: { tab: "live", liveOn: true, pollIntervalMs: 1000, basePath: "/admin" } };
global.document = {
    addEventListener(name, fn) { listeners[name] = fn; },
    getElementById() { return { requestSubmit() { log.push("submit"); } }; },
    querySelector() { return null; },
    createElement() { return { querySelector() { return null; } }; },
};
global.Alpine = { data(name, factory) { components[name] = factory; } };
// The stub form has no fields: the request the component issues is what
// matters, not the filter values it carries.
global.FormData = class { *[Symbol.iterator]() {} };
global.EventSource = class {
    constructor(url) { this.url = url; this.handlers = {}; log.push("sse-open:" + url); global.lastEventSource = this; }
    addEventListener(name, fn) { this.handlers[name] = fn; }
    close() { log.push("sse-close"); }
    emit(name, data) { this.handlers[name]({ data: JSON.stringify(data) }); }
};
global.setInterval = (fn, ms) => { const id = nextTimer++; timers.push({ id, fn, ms, due: now + ms }); return id; };
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };
global.fetch = (url) => { log.push("fetch:" + url.split("?")[0]); return { then() { return { then() { return { catch() {} }; } }; } }; };

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
listeners["alpine:init"]();
const page = components.jobsPage();
page.init();

if (scenario === "poll-while-sse-connected") {
    advance(3000);
} else if (scenario === "sse-payload-without-status") {
    global.lastEventSource.emit("state_change", { type: "cancel", job_id: "j1", worker_id: "w1" });
} else if (scenario === "sse-terminal-status-for-unlisted-row") {
    global.lastEventSource.emit("state_change", { job_id: "j1", status: "succeeded" });
}
process.stdout.write(JSON.stringify(log));
"""


def _drive(scenario: str) -> list[str]:
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and scenario names are this file's own constants.
        [node, "-e", _HARNESS, "--", str(ADMIN_JS), scenario],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout)


@requires_node
def test_live_mode_keeps_polling_while_sse_is_connected() -> None:
    """The SSE channel only carries the cancel fast-path today; terminal
    writes and dispatch never NOTIFY it. Live mode must therefore keep the
    poll timer running so the table stays current, with SSE events only
    accelerating a refresh on top."""
    log = _drive("poll-while-sse-connected")
    assert "sse-open:/admin/sse/jobs" in log
    assert log.count("fetch:/admin/jobs") == 3, log


@requires_node
def test_sse_payload_without_status_refreshes_the_table() -> None:
    """A payload that names a job but carries no ``status`` (the cancel
    NOTIFY) means the row changed in a way the client cannot apply locally:
    the truth is on the server, so the table is refreshed."""
    log = _drive("sse-payload-without-status")
    assert log.count("fetch:/admin/jobs") == 1, log


@requires_node
def test_sse_terminal_status_for_unlisted_row_refreshes_the_table() -> None:
    """A terminal transition for a job not in the table means the listing's
    membership changed (a filter on active statuses no longer matches it, or
    it now belongs on a terminal filter), so the table is refreshed rather
    than the event being dropped."""
    log = _drive("sse-terminal-status-for-unlisted-row")
    assert log.count("fetch:/admin/jobs") == 1, log
