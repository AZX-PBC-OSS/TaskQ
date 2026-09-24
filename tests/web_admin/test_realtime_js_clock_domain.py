"""Clock-domain pins for the job-detail progress driver (``static/realtime.js``).

The failure class: a browser whose clock is wrong (a laptop minutes off, or a
tab the OS suspended for minutes and then resumed) must not be able to flip a
rendered sign or break the poll cadence. The driver's contract:

1. the render never does relative-time math against the browser's clock: the
   meta line prints the snapshot's own timestamp (the server's instant), so a
   skewed client clock cannot produce a negative duration, an "in -3s", or a
   mis-ordering;
2. a suspended tab's overdue poll is ONE catch-up request, not a burst: the
   browser coalesces a throttled ``setInterval`` to a single fire on resume,
   and one fire issues exactly one conditional GET that renders the fresh
   state exactly once (the stale-DOM discard);
3. a poll request still outstanding never stacks a second one: the tick skips
   itself and the next fire catches up (a slow server or a resumed tab
   therefore cannot overlap conditional GETs);
4. a replayed or stale-sequence snapshot (an SSE reconnect replaying across
   the gap) writes nothing: the seq cursor and the fingerprint gate drop it
   before any DOM work.

Driven under Node with a DOM stub that logs every mutation, the same shape as
the render harness. Unlike that harness's synchronous thenables, this one
uses real Promises so a response can be HELD across ticks and released late.
Node is the only JS runtime the repository has; the tests skip where it is
absent.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

pytestmark = [pytest.mark.fastapi]

pytest.importorskip("fastapi")

REALTIME_JS = (
    Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "realtime.js"
)

requires_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="realtime.js behaviour is driven under Node"
)

# The same stub browser as the render harness (every DOM write is logged),
# plus real-Promise fetches: a scripted poll can be held in flight and
# released after later ticks, which is the suspended-tab / slow-server shape.
# Segments are cut by ``seg:<name>`` markers, so every pin asserts exactly
# which driver event wrote what.
_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 2], "utf8");
const scenario = process.argv[process.argv.length - 1];

const dom = [];
const net = [];
const held = [];

function mark(name) {
    dom.push(`seg:${name}`);
    net.push(`seg:${name}`);
}

let nextTimer = 1;
const timers = [];

global.window = { TASKQ_BASE_PATH: "" };
global.POLL_INTERVAL_MS = 1000;

const sseScenario = scenario === "sse-replay-and-stale";

const badge = {
    attrs: { "data-mode": sseScenario ? "realtime" : "polling" },
    textContent: "",
    setAttribute(k, v) { this.attrs[k] = v; },
    getAttribute(k) { return this.attrs[k]; },
};

const section = {
    attrs: {
        "data-job-id": "j1",
        "data-progress-seq": "0",
        "data-progress-state": "{}",
    },
    getAttribute(k) { return this.attrs[k]; },
};

let nodeCounter = 0;

function makeStyle(name) {
    const style = {};
    let width = "";
    Object.defineProperty(style, "width", {
        get() { return width; },
        set(v) { width = v; dom.push(`width:${name}=${v}`); },
    });
    return style;
}

function makeNode(tag) {
    nodeCounter += 1;
    const name = `${tag}${nodeCounter}`;
    const node = {
        _name: name,
        children: [],
        className: "",
        style: makeStyle(name),
        appendChild(child) {
            dom.push(`append:${name}<${child._name}`);
            this.children.push(child);
        },
        remove() { dom.push(`remove:${name}`); },
        scrollIntoView() { dom.push(`scroll:${name}`); },
    };
    let text = "";
    Object.defineProperty(node, "textContent", {
        get() { return text; },
        set(v) { text = v; dom.push(`text:${name}=${v}`); },
    });
    return node;
}

const timeline = {
    children: [],
    appendChild(child) {
        dom.push(`timeline-append:${child._name}`);
        this.children.push(child);
    },
};

global.document = {
    addEventListener(name, fn) { if (name === "DOMContentLoaded") fn(); },
    getElementById(id) {
        if (id === "progress-section") return section;
        if (id === "progress-timeline") return timeline;
        return null;
    },
    querySelector(sel) { return sel === ".taskq-badge" ? badge : null; },
    createElement(tag) { return makeNode(tag); },
};

const POLL_SCRIPT = {
    // tick 1 renders seq 40; the coalesced overdue fire after the
    // suspension renders seq 41 exactly once; the next fire is 304.
    "suspend-resume-catch-up": [
        { status: "running", progress_seq: 40, progress_state: { percent: 90, step: "almost" } },
        { status: "running", progress_seq: 41, progress_state: { percent: 95, step: "final" } },
        { notModified: true },
    ],
    // tick 1's response is held; tick 2 must not stack a second fetch; the
    // late release renders once; tick 3's conditional GET is 304.
    "in-flight-overlap": [
        {
            hold: true,
            body: { status: "running", progress_seq: 40, progress_state: { percent: 90, step: "almost" } },
        },
        { notModified: true },
        { notModified: true },
    ],
};

let pollCount = 0;

global.fetch = function (url, opts) {
    net.push(`fetch:${url}`);
    if (opts && opts.headers && opts.headers["If-None-Match"] !== undefined) {
        net.push(`inm:${opts.headers["If-None-Match"]}`);
    }
    const script = POLL_SCRIPT[scenario];
    const scripted = script[pollCount] ?? script[script.length - 1];
    pollCount += 1;
    if (scripted.hold) {
        const body = scripted.body;
        return new Promise(function (resolve) {
            held.push(function () {
                resolve({ status: 200, json: function () { return body; } });
            });
        });
    }
    const status = scripted.notModified ? 304 : 200;
    const body = scripted.notModified ? {} : scripted;
    return Promise.resolve({ status: status, json: function () { return body; } });
};

global.EventSource = class {
    constructor(url) {
        this.url = url;
        this.handlers = {};
        global.lastEventSource = this;
        net.push(`sse-open:${url}`);
    }
    addEventListener(name, fn) { this.handlers[name] = fn; }
};

global.setInterval = function (fn, ms) {
    const id = nextTimer++;
    timers.push({ id: id, fn: fn, ms: ms });
    return id;
};
global.clearInterval = function (id) {
    for (let i = timers.length - 1; i >= 0; i -= 1) {
        if (timers[i].id === id) timers.splice(i, 1);
    }
};

async function settle() {
    // Two turns so the fetch's whole .then/.catch chain drains before the
    // next segment is cut.
    await null;
    await null;
}

async function firePoll() {
    for (const t of timers.slice()) {
        if (t.ms === POLL_INTERVAL_MS) {
            t.fn();
            await settle();
        }
    }
}

function fireSse(evt) {
    global.lastEventSource.handlers.progress({
        data: JSON.stringify(evt),
        lastEventId: String(evt.seq),
    });
}

(async function main() {
    new Function(src)();

    const out = { dom: dom, net: net, localTime: "" };

    if (sseScenario) {
        const evt41 = {
            job_id: "j1",
            actor: "w",
            ts: "2026-01-01T00:00:01Z",
            seq: 41,
            status: "running",
            step: "stage",
            percent: 50,
            detail: "half",
        };
        const evt40 = { ...evt41, seq: 40 };
        const evt42 = { ...evt41, seq: 42, percent: 75 };
        mark("replay1");
        fireSse(evt41);
        await settle();
        mark("replay2");
        fireSse(evt41);
        await settle();
        mark("stale");
        fireSse(evt40);
        await settle();
        mark("fresh");
        fireSse(evt42);
        await settle();
        out.localTime = new Date(evt41.ts).toLocaleTimeString();
    } else if (scenario === "in-flight-overlap") {
        mark("tick1");
        await firePoll();
        mark("tick2");
        await firePoll();
        mark("release");
        for (const release of held) release();
        await settle();
        mark("tick3");
        await firePoll();
    } else {
        mark("tick1");
        await firePoll();
        mark("resume");
        await firePoll();
        mark("resume-next");
        await firePoll();
    }

    process.stdout.write(JSON.stringify(out));
})().catch(function (err) {
    console.error(err);
    process.exit(1);
});
"""


def _drive(scenario: str) -> dict[str, Any]:
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and scenario names are this file's own constants.
        [node, "-e", _HARNESS, "--", str(REALTIME_JS), scenario],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout)  # pyright: ignore[reportAny, reportUnknownMemberType]


def _segments(entries: list[str]) -> dict[str, list[str]]:
    """Split a log into per-segment lists (between ``seg:`` markers)."""
    segments: dict[str, list[str]] = {}
    current = ""
    for entry in entries:
        if entry.startswith("seg:"):
            current = entry.split(":", 1)[1]
            segments[current] = []
        elif current:
            segments[current].append(entry)
    return segments


@requires_node
def test_a_suspended_tab_resumes_with_one_catchup_poll_and_one_render() -> None:
    """A tab the OS suspended (timers frozen for minutes) resumes with the
    browser's single coalesced interval fire: exactly ONE conditional GET,
    carrying the pre-suspension sequence, that renders the fresh state
    exactly once - never a burst of overlapping requests, never a double
    render of the same snapshot - and the following fire is a 304 that
    writes nothing."""
    log = _drive("suspend-resume-catch-up")
    dom, net = _segments(log["dom"]), _segments(log["net"])

    # The overdue fire: one request, the stale-seq's own catch-up.
    assert net["resume"] == [
        "fetch:/jobs/api/job/j1/state",
        'inm:"40"',
    ], f"the resumed tab must issue exactly one conditional catch-up GET: {net['resume']}"

    # It patches the rendered entry in place, exactly the nodes the fresh
    # snapshot feeds (bar width, detail, meta line), never rebuilding and
    # never stealing the viewport.
    appends = [e for e in dom["resume"] if e.startswith("timeline-append:")]
    assert appends == [], f"the catch-up render must patch in place, never rebuild: {dom['resume']}"
    assert dom["resume"] == [
        "width:div3=95%",
        "text:div4=final",
        "text:div5=95% · final",
    ], f"the catch-up render must patch exactly the fresh snapshot's nodes: {dom['resume']}"
    assert not any(e.startswith("scroll:") for e in dom["resume"]), (
        f"the catch-up render must not jump the scroll position: {dom['resume']}"
    )

    # The next fire after the catch-up is a 304 that writes nothing.
    assert net["resume-next"] == [
        "fetch:/jobs/api/job/j1/state",
        'inm:"41"',
    ], f"the catch-up must advance the conditional-GET cursor: {net['resume-next']}"
    assert dom["resume-next"] == [], (
        f"a 304 after the catch-up must write nothing: {dom['resume-next']}"
    )


@requires_node
def test_an_outstanding_poll_never_stacks_a_second_one() -> None:
    """A response still in flight (a slow server, a tab resumed mid-request)
    must not let the next tick stack an overlapping conditional GET: the
    tick skips itself; when the response lands late it renders exactly
    once; and the following tick issues the next request carrying the
    sequence the late render learned."""
    log = _drive("in-flight-overlap")
    dom, net = _segments(log["dom"]), _segments(log["net"])

    # Tick 1 issued the (held) request and rendered nothing yet.
    assert net["tick1"] == ["fetch:/jobs/api/job/j1/state"], net["tick1"]

    # Tick 2: the request is still outstanding - no second fetch.
    assert net["tick2"] == [], (
        f"a tick whose poll is still in flight must skip itself, it fetched: {net['tick2']}"
    )

    # The late response renders exactly once, building the entry (nothing
    # was rendered while the request was held).
    appends = [e for e in dom["release"] if e.startswith("timeline-append:")]
    assert len(appends) == 1, f"the late response must render exactly once: {dom['release']}"

    # The next tick is conditional on the sequence the late render learned,
    # and a 304 writes nothing.
    assert net["tick3"] == [
        "fetch:/jobs/api/job/j1/state",
        'inm:"40"',
    ], f"the late render must advance the conditional-GET cursor: {net['tick3']}"
    assert dom["tick3"] == [], f"a 304 after the late render must write nothing: {dom['tick3']}"


@requires_node
def test_sse_replays_and_stale_seqs_write_nothing_and_meta_has_no_relative_time() -> None:
    """An SSE reconnect replay (the same snapshot twice, then an older
    sequence arriving after a newer one - the resumed-stream shape) must
    write nothing: the seq cursor and the fingerprint gate drop it before
    any DOM work. And the meta line renders the snapshot's own timestamp -
    never a browser-clock-derived duration - so a skewed client clock
    cannot produce a negative relative render ('in -3s', 'Xs ago')."""
    log = _drive("sse-replay-and-stale")
    dom = _segments(log["dom"])

    # The first delivery renders once.
    assert any(e.startswith("timeline-append:") for e in dom["replay1"]), dom["replay1"]

    # The identical replay, a stale (lower) sequence, and the fresh event's
    # unaffected nodes: all zero-write.
    assert dom["replay2"] == [], f"an identical replay must write nothing: {dom['replay2']}"
    assert dom["stale"] == [], f"a stale sequence must write nothing: {dom['stale']}"

    # The fresh event patches exactly the two nodes percent feeds.
    assert len(dom["fresh"]) == 2 and all(
        e.startswith(("width:", "text:")) for e in dom["fresh"]
    ), f"a changed field must patch exactly its own nodes: {dom['fresh']}"

    # Every meta line is percent · step · the snapshot's own instant:
    # no 'ago', no 'in ', no negative anything - nothing the browser's
    # clock could have computed.
    meta_lines = [e.split("=", 1)[1] for e in log["dom"] if e.startswith("text:div") and "·" in e]
    assert meta_lines, f"the replay must have rendered meta lines: {log['dom']}"
    for line in meta_lines:
        assert line == f"50% · stage · {log['localTime']}" or line == (
            f"75% · stage · {log['localTime']}"
        ), f"the meta line must be the snapshot's own instant, got: {line}"
        assert "ago" not in line and " in " not in line and "-" not in line, (
            f"the meta line must carry no browser-clock-derived relative time: {line}"
        )
