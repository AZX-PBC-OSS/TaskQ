"""Behavioural tests for the jobs page component in ``static/admin.js``.

The component is driven under Node with the browser surface it touches
stubbed (``document``, ``Alpine.data``, ``EventSource``, timers, ``fetch``),
so the assertions are on what the page does - which requests it issues and
when - not on the source text. Node is the only JS runtime the repository
has; the tests skip where it is absent on a developer machine, and fail
in CI, where the workflow installs Node so that a skip there would be
silent coverage loss.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import parse_qs

import pytest

pytestmark = [pytest.mark.fastapi]

pytest.importorskip("fastapi")

ADMIN_JS = Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "admin.js"


def _node_or_skip() -> str:
    node = shutil.which("node")
    if node is None:
        if os.environ.get("CI"):
            pytest.fail(
                "node is not on PATH in CI: the admin.js tests would skip silently; "
                "the workflow's setup-node step is missing or broken"
            )
        pytest.skip("admin.js behaviour is driven under Node")
    return node


requires_node = pytest.mark.skipif(
    shutil.which("node") is None and not os.environ.get("CI"),
    reason="admin.js behaviour is driven under Node",
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

global.window = { __taskqJobConfig: { tab: "live", liveOn: true, pollIntervalMs: 1000, basePath: "/admin" }, htmx: {} };
global.document = {
    addEventListener(name, fn) { listeners[name] = fn; },
    body: {
        addEventListener(name, fn) { listeners[name] = fn; },
        removeEventListener(name, fn) { if (listeners[name] === fn) delete listeners[name]; },
    },
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
global.fetch = (url) => {
    log.push("fetch:" + url.split("?")[0]);
    log.push("qs:" + (url.split("?")[1] || ""));
    return { then() { return { then() { return { catch() {} }; } }; } };
};

// What htmx fires before issuing a request: the path is what the anchor or
// form declared (pagination links carry their cursor in the query string,
// form submits never do). Emits only if the component registered a listener.
function htmxRequest(params) {
    const qs = new URLSearchParams(params).toString();
    const fn = listeners["htmx:beforeRequest"];
    if (fn) fn({ detail: { requestConfig: { path: "/admin/jobs" + (qs ? "?" + qs : "") } } });
}

function advance(ms) {
    const target = now + ms;
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due; due.due += due.ms; due.fn();
    }
    now = target;
}

if (scenario === "starts-paused") global.window.__taskqJobConfig.liveOn = false;
new Function(src)();
listeners["alpine:init"]();
const page = components.jobsPage();
page.init();

if (scenario === "poll-while-sse-connected") {
    advance(3000);
} else if (scenario === "poll-preserves-cursor") {
    htmxRequest({ cursor_at: "2026-09-16T12:00:00", cursor_id: "j9", cursor_dir: "next" });
    advance(3000);
} else if (scenario === "filter-submit-repages") {
    htmxRequest({ cursor_at: "2026-09-16T12:00:00", cursor_id: "j9", cursor_dir: "next" });
    htmxRequest({});
    advance(3000);
} else if (scenario === "sse-forward-on-cursor-page") {
    htmxRequest({ cursor_at: "2026-09-16T12:00:00", cursor_id: "j9", cursor_dir: "next" });
    global.lastEventSource.emit("state_change", { type: "cancel", job_id: "j1", worker_id: "w1" });
} else if (scenario === "sse-payload-without-status") {
    global.lastEventSource.emit("state_change", { type: "cancel", job_id: "j1", worker_id: "w1" });
} else if (scenario === "sse-terminal-status-for-unlisted-row") {
    global.lastEventSource.emit("state_change", { job_id: "j1", status: "succeeded" });
} else if (scenario === "sse-error-keeps-polling") {
    global.lastEventSource.handlers["error"]();
    advance(2000);
} else if (scenario === "paused-freezes-the-table") {
    page.toggleLive();
    advance(3000);
    global.lastEventSource.emit("state_change", { type: "cancel", job_id: "j1", worker_id: "w1" });
} else if (scenario === "resume-reloads-and-restarts") {
    page.toggleLive();
    advance(3000);
    page.toggleLive();
    advance(2000);
} else if (scenario === "starts-paused") {
    // handled below: the page was initialised with liveOn=false
}
log.push("polling:" + (page.pollTimer !== null) + " sse:" + (page.eventSource !== null));
process.stdout.write(JSON.stringify(log));
"""


def _drive(scenario: str) -> list[str]:
    node = _node_or_skip()
    # No per-spawn wall-clock deadline on purpose. The harness is fully
    # virtual (scripted fetches, stubbed timers, a synchronous log), so the
    # child's exit is the only event worth waiting for, and a fixed deadline
    # is a delay that races child startup, not a behaviour gate: under
    # co-tenant load (-n 4 plus a CPU/IO stressor) a starved Node startup
    # blew a 30s deadline and turned a behaviourally-correct pin red (the
    # same child runs in ~60ms of CPU once scheduled). A wedged child is
    # the suite-wide pytest-timeout budget's job (--timeout=300 in
    # addopts, every lane): it fails the hung test by name with a stack
    # dump instead of guessing a threshold no load condition can justify.
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and scenario names are this file's own constants.
        [node, "-e", _HARNESS, "--", str(ADMIN_JS), scenario],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Surface the child's stderr: a harness crash (a stub drift, a
        # scenario typo) must name the JS error, not exit status 1.
        pytest.fail(
            f"the harness Node process exited {result.returncode} "
            f"(its stderr follows)\n{result.stderr}"
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
def test_poll_without_a_cursor_refreshes_page_one() -> None:
    """Without a cursor the poll refetches the unpaged first page: live mode
    keeps the table current from the operator's own vantage point."""
    log = _drive("poll-while-sse-connected")
    qs_entries = [e for e in log if e.startswith("qs:")]
    assert len(qs_entries) == 3, log
    assert all("cursor_at" not in q for q in qs_entries)


@requires_node
def test_poll_while_on_a_cursor_page_fetches_that_page() -> None:
    """The poll runs unconditionally in live mode, so it must refetch the
    page the operator is on: the cursor synced from the last pagination
    click rides along on every poll fetch, and a reader on page two is
    never yanked back to page one."""
    log = _drive("poll-preserves-cursor")
    assert log.count("fetch:/admin/jobs") == 3, log
    for q in (e for e in log if e.startswith("qs:")):
        parsed = parse_qs(q[3:])
        assert parsed["cursor_at"] == ["2026-09-16T12:00:00"], log
        assert parsed["cursor_id"] == ["j9"], log
        assert parsed["cursor_dir"] == ["next"], log


@requires_node
def test_a_request_without_a_cursor_is_the_explicit_repage() -> None:
    """A submit that carries no cursor (a filter change, a tab switch, the
    live toggle) clears the cursor: re-paginating is the operator's own
    action, so the poll goes back to fetching page one."""
    log = _drive("filter-submit-repages")
    qs_entries = [e for e in log if e.startswith("qs:")]
    assert len(qs_entries) == 3, log
    assert all("cursor_at" not in q for q in qs_entries)


@requires_node
def test_sse_refresh_forward_does_not_reset_the_cursor_page() -> None:
    """A state change the client cannot apply locally refreshes forward only
    when no cursor is active: on a cursor page the poll already refreshes
    the operator's page in place, and a cursor-less refresh from here would
    swap page one under the reader."""
    log = _drive("sse-forward-on-cursor-page")
    assert log.count("fetch:/admin/jobs") == 0, log


@requires_node
def test_sse_payload_without_status_refreshes_the_table() -> None:
    """A payload that names a job but carries no ``status`` (the cancel
    NOTIFY) means the row changed in a way the client cannot apply locally:
    the truth is on the server, so the table is refreshed."""
    log = _drive("sse-payload-without-status")
    assert log.count("fetch:/admin/jobs") == 1, log
    # The one refresh is a page-one fetch: with no cursor active (the
    # operator never paginated), the SSE-driven refresh must not
    # manufacture one - a cursor-less refresh is the page's own vantage
    # point, and this is the exact complement of the cursor-page pin
    # (where the refresh is left to the poll that carries the cursor).
    qs_entries = [e for e in log if e.startswith("qs:")]
    assert len(qs_entries) == 1, log
    assert all("cursor_at" not in q and "cursor_id" not in q for q in qs_entries), log


@requires_node
def test_sse_terminal_status_for_unlisted_row_refreshes_the_table() -> None:
    """A terminal transition for a job not in the table means the listing's
    membership changed (a filter on active statuses no longer matches it, or
    it now belongs on a terminal filter), so the table is refreshed rather
    than the event being dropped."""
    log = _drive("sse-terminal-status-for-unlisted-row")
    assert log.count("fetch:/admin/jobs") == 1, log


@requires_node
def test_sse_error_leaves_the_stream_to_reconnect_and_keeps_polling() -> None:
    """An EventSource error is not the end of the stream: the browser's own
    reconnect is left to run (closing it here made a proxy idle timeout or a
    server restart permanent), and the poll that was running all along
    carries the page meanwhile."""
    log = _drive("sse-error-keeps-polling")
    assert "sse-close" not in log
    assert log.count("sse-open:/admin/sse/jobs") == 1
    assert log.count("fetch:/admin/jobs") == 2, log
    assert log[-1] == "polling:true sse:true"


@requires_node
def test_paused_freezes_the_table() -> None:
    """Paused means the table stays as the operator left it: the poll
    stops, SSE is closed, and nothing - not even a late event on the old
    stream - fetches the table."""
    log = _drive("paused-freezes-the-table")
    assert "sse-close" in log
    assert log.count("fetch:/admin/jobs") == 0, log
    assert log[-1] == "polling:false sse:false"


@requires_node
def test_resuming_reloads_the_table_and_restarts_live_refresh() -> None:
    """Resuming reloads the table immediately (whatever changed while it
    was frozen), reconnects SSE and restarts the poll."""
    log = _drive("resume-reloads-and-restarts")
    assert log.count("sse-open:/admin/sse/jobs") == 2
    assert "submit" in log
    # The two fetches are both after the resume: none while paused, and
    # both downstream of the reload (the submit) - the frozen window
    # fetched nothing and the resumed page's cadence starts from the
    # reload, not alongside or before it.
    assert log.count("fetch:/admin/jobs") == 2, log
    submit_at = log.index("submit")
    assert log[submit_at + 1 :].count("fetch:/admin/jobs") == 2, log
    assert log[-1] == "polling:true sse:true"


@requires_node
def test_a_page_opened_paused_starts_frozen() -> None:
    """``live=off`` in the URL (the form's own state) opens the page with
    neither the poll nor SSE running."""
    log = _drive("starts-paused")
    assert "sse-open:/admin/sse/jobs" not in log
    assert log[-1] == "polling:false sse:false"
