"""UX pins for the job-detail progress driver's poll path (the flicker fix).

A polling-degraded page used to repaint the progress section on every poll
tick: the render rebuilt the entry's DOM per accepted snapshot, so an idle
job whose worker re-flushed its (unchanged) snapshot with a bumped seq and
a fresh ts flickered forever. Polling is a first-class mode of the portal,
so the contract these pins hold is what a polling page owes its operator:

1. a poll tick that changes nothing touches NOTHING (zero DOM writes, and
   therefore zero layout) - the acceptProgress fingerprint gate drops a
   snapshot the timeline already renders before any DOM work;
2. a changed field patches exactly its own nodes in place; the entry is
   built once and never rebuilt;
3. the driver never steals the viewport: no scrollIntoView anywhere, so an
   append or patch never jumps the scroll position;
4. the poll cadence does not re-download unchanged data: the poll carries
   ``If-None-Match`` with the progress sequence the page already rendered
   and an unchanged tick is answered 304 with no body at all;
5. the badge states the mode calmly: a dropped stream does not flip it to
   ``polling-degraded`` (EventSource reconnects on its own; polling keeps
   the data fresh meanwhile; the periodic probe owns badge transitions).

Driven under Node with a DOM stub that logs every mutation (node append,
``textContent`` write, bar ``width`` write, node remove, scroll), the same
shape as the mode-probe harness. The log is cut per poll tick, so the pins
assert exactly which ticks mutated what. Node is the only JS runtime the
repository has; the tests skip where it is absent.
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

# A minimal browser whose DOM logs every mutation. The badge and the
# progress section are the elements the module touches; the poll answers
# scenario-scripted snapshots (304 when the script says so); timers are
# virtual. Between ticks the harness cuts the mutation log with a
# ``poll:N`` marker, so a tick that writes nothing shows up as an empty
# segment.
_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 2], "utf8");
const scenario = process.argv[process.argv.length - 1];

const SCENARIOS = {
    // One changed snapshot, then the same snapshot re-flushed with a
    // bumped seq and a fresh ts: the repeated identical polls must write
    // nothing.
    "identical-polls": {
        seed: { seq: 0, state: {} },
        polls: [
            { status: "running", progress_seq: 2, progress_state: { percent: 50, step: "stage", detail: "half", ts: "2026-01-01T00:00:01Z" } },
            { status: "running", progress_seq: 3, progress_state: { percent: 50, step: "stage", detail: "half", ts: "2026-01-01T00:00:02Z" } },
            { status: "running", progress_seq: 4, progress_state: { percent: 50, step: "stage", detail: "half", ts: "2026-01-01T00:00:03Z" } },
            { status: "running", progress_seq: 5, progress_state: { percent: 50, step: "stage", detail: "half", ts: "2026-01-01T00:00:04Z" } },
        ],
    },
    // The state the server already rendered: even the FIRST poll must
    // write nothing (the boot seeds the fingerprint from the section's
    // data attributes).
    "server-rendered-identical": {
        seed: { seq: 2, state: { percent: 50, step: "stage", detail: "half" } },
        polls: [
            { status: "running", progress_seq: 3, progress_state: { percent: 50, step: "stage", detail: "half", ts: "2026-01-01T00:00:01Z" } },
            { status: "running", progress_seq: 4, progress_state: { percent: 50, step: "stage", detail: "half", ts: "2026-01-01T00:00:02Z" } },
        ],
    },
    // One field changes (percent 50 -> 75): the render must patch exactly
    // the nodes percent feeds (the bar width and the meta line) and touch
    // nothing else - no rebuild, no append, the detail node untouched.
    "changed-field": {
        seed: { seq: 0, state: {} },
        polls: [
            { status: "running", progress_seq: 2, progress_state: { percent: 50, step: "stage", detail: "half" } },
            { status: "running", progress_seq: 3, progress_state: { percent: 75, step: "stage", detail: "half" } },
        ],
    },
    // After the first render the page has a progress sequence, so every
    // later tick polls conditionally: the unchanged ticks are answered
    // 304 (no body to parse, nothing to render).
    "unchanged-cadence": {
        seed: { seq: 0, state: {} },
        polls: [
            { status: "running", progress_seq: 2, progress_state: { percent: 50, step: "stage", detail: "half" }, etag: false },
            { notModified: true },
            { notModified: true },
        ],
    },
};

const config = SCENARIOS[scenario];
const sseScenario = scenario === "transient-sse-error";

// Two logs, cut by the same ``poll:N`` markers: ``dom`` records every DOM
// mutation (the flicker contract lives here), ``net`` records the
// requests the module issues (the no-re-download contract lives here).
const dom = [];
const net = [];

function mark(label) {
    dom.push(label);
    net.push(label);
}
let now = 0;
let timers = [];
let nextTimer = 1;

global.window = { TASKQ_BASE_PATH: "" };
global.POLL_INTERVAL_MS = 1000;

const badge = {
    attrs: { "data-mode": sseScenario ? "realtime" : "polling" },
    textContent: "",
    setAttribute(k, v) { this.attrs[k] = v; },
    getAttribute(k) { return this.attrs[k]; },
};

const section = {
    attrs: {
        "data-job-id": "j1",
        "data-progress-seq": String(config ? config.seed.seq : 0),
        "data-progress-state": config ? JSON.stringify(config.seed.state) : "{}",
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

let pollCount = 0;

global.fetch = (url, opts) => ({
    then(f1) {
        net.push(`fetch:${url.split("?")[0]}`);
        if (opts && opts.headers && opts.headers["If-None-Match"] !== undefined) {
            net.push(`inm:${opts.headers["If-None-Match"]}`);
        }
        let status = 200;
        let body = { status: "running", progress_seq: 0, progress_state: {} };
        if (config && url.endsWith("/state")) {
            const scripted = config.polls[pollCount] ?? config.polls[config.polls.length - 1];
            pollCount += 1;
            if (scripted.notModified) {
                status = 304;
            } else {
                body = scripted;
            }
        } else if (url.endsWith("/sse/mode")) {
            body = { realtime: sseScenario };
        }
        const response = { status, json: () => body };
        return { then(f2) { f2(f1(response)); return { catch() {} }; } };
    },
});

global.EventSource = class {
    constructor(url) {
        this.url = url;
        this.handlers = {};
        global.lastEventSource = this;
        net.push("sse-open:" + url);
    }
    addEventListener(name, fn) { this.handlers[name] = fn; }
    close() { net.push("sse-close"); }
    emitOpen() { this.handlers.open && this.handlers.open({}); }
    emitError() { this.handlers.error({}); }
};

global.setInterval = (fn, ms) => { const id = nextTimer++; timers.push({ id, fn, ms, due: now + ms }); return id; };
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };

function advanceOneTick() {
    const target = now + POLL_INTERVAL_MS;
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due; due.due += due.ms; due.fn();
    }
    now = target;
}

new Function(src)();
if (sseScenario) {
    // A realtime page whose stream drops mid-flight (a proxy blip): the
    // badge must stay calm while polling bridges the gap, and the badge
    // only moves when the periodic probe - the authority - says so.
    mark("poll:1");
    advanceOneTick();
    global.lastEventSource.emitError();
    mark("poll:2");
    advanceOneTick();
    // The probe still reports Redis healthy: no mode transition.
    global.lastEventSource.emitOpen();
    mark("poll:3");
    advanceOneTick();
} else {
    for (let i = 0; i < config.polls.length; i += 1) {
        mark(`poll:${i + 1}`);
        advanceOneTick();
    }
}
mark(`mode:${badge.attrs["data-mode"]}`);
process.stdout.write(JSON.stringify({ dom, net }));
"""


def _drive(scenario: str) -> dict[str, list[str]]:
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


def _segments(entries: list[str]) -> dict[int, list[str]]:
    """Split a log into per-tick segments (between ``poll:N`` markers).

    The trailing ``mode:`` verdict the driver emits last is not a segment
    entry."""
    segments: dict[int, list[str]] = {}
    current = 0
    for entry in entries:
        if entry.startswith("mode:"):
            continue
        if entry.startswith("poll:"):
            current = int(entry.split(":", 1)[1])
            segments[current] = []
        elif current:
            segments[current].append(entry)
    return segments


def _no_scroll(dom: list[str]) -> None:
    """The driver never steals the viewport: no scrollIntoView call, ever."""
    scrolls = [entry for entry in dom if entry.startswith("scroll:")]
    assert scrolls == [], f"the driver must never jump the scroll position, it scrolled: {scrolls}"


@requires_node
def test_repeated_identical_polls_produce_zero_dom_mutations_and_zero_layout() -> None:
    """A snapshot the timeline already renders must write NOTHING, however
    often the worker re-flushes it. The worker re-flushes unchanged
    snapshots with a bumped seq and a fresh ts; the old render keyed on
    the seq alone, so every re-flush repainted the section: the flicker.
    The first poll renders (one entry, built once); polls 2 through 4
    carry the identical state and must leave the DOM untouched - and a
    tick that writes nothing lays out nothing."""
    log = _drive("identical-polls")
    dom, segments = log["dom"], _segments(log["dom"])

    first = segments[1]
    assert any(entry.startswith("timeline-append:") for entry in first), (
        f"the first poll renders the entry once: {first}"
    )
    for tick in (2, 3, 4):
        assert segments[tick] == [], (
            f"a repeated identical poll must perform zero DOM mutations "
            f"(and therefore zero layout), tick {tick} wrote {segments[tick]}"
        )
    _no_scroll(dom)


@requires_node
def test_poll_matching_the_server_rendered_state_writes_nothing_at_all() -> None:
    """The section's data attributes seed the fingerprint with the
    snapshot the template already rendered, so a poll that returns that
    same state writes nothing at all: not even the first poll appends an
    entry (the server-rendered one IS the current state)."""
    dom = _drive("server-rendered-identical")["dom"]
    assert not any(
        entry.startswith(("timeline-append:", "append:", "text:", "width:", "remove:", "scroll:"))
        for entry in dom
    ), f"every poll matched the server-rendered state and had to write nothing: {dom}"


@requires_node
def test_a_changed_field_patches_exactly_its_own_nodes() -> None:
    """A changed field must patch the nodes that display it and touch
    nothing else: no rebuild, no fresh entry, the unchanged detail node
    unwritten. Percent feeds exactly two nodes (the bar's width and the
    meta line); the second poll's mutation log must be exactly those two
    writes."""
    out = _drive("changed-field")
    dom, segments = out["dom"], _segments(out["dom"])

    first = segments[1]
    assert any(entry.startswith("timeline-append:") for entry in first), (
        f"the first poll renders the entry: {first}"
    )

    # The changed tick: percent 50 -> 75. The bar width and the meta line
    # (which renders the percentage) change; nothing else does.
    assert segments[2] == ["width:div3=75%", "text:div5=75% · stage"], (
        f"a changed field must patch exactly its own nodes: {segments[2]}"
    )
    _no_scroll(dom)


@requires_node
def test_unchanged_poll_ticks_download_nothing_and_render_nothing() -> None:
    """The poll cadence must not re-download or re-render data the page
    already has: once the page has rendered a progress sequence, every
    poll carries ``If-None-Match`` with that sequence, an unchanged tick
    is answered 304 (no state bytes to parse), and the tick performs zero
    DOM mutations."""
    out = _drive("unchanged-cadence")
    dom, net = out["dom"], out["net"]
    dom_segments, net_segments = _segments(dom), _segments(net)

    first = dom_segments[1]
    assert any(entry.startswith("timeline-append:") for entry in first), (
        f"the first poll renders the entry and learns the sequence: {first}"
    )
    # The page sends its cursor from the second poll on.
    assert net_segments[2] == ["fetch:/jobs/api/job/j1/state", 'inm:"2"'], (
        f"the poll must carry the rendered sequence as If-None-Match: {net_segments[2]}"
    )
    assert net_segments[3] == ["fetch:/jobs/api/job/j1/state", 'inm:"2"'], (
        f"the poll must carry the rendered sequence as If-None-Match: {net_segments[3]}"
    )
    for tick in (2, 3):
        assert dom_segments[tick] == [], (
            f"an unchanged tick downloads no state and writes no DOM: "
            f"dom={dom_segments[tick]} net={net_segments[tick]}"
        )
    _no_scroll(dom)


@requires_node
def test_a_dropped_stream_does_not_flip_the_badge_to_degraded() -> None:
    """The badge states the mode calmly: a transient stream error is not
    an outage announcement. The badge stays ``realtime`` (the operator
    must never read the page as broken when it is not), polling bridges
    the gap, and when the stream reconnects the poll stands down. Only
    the periodic probe - the authority on Redis health - transitions the
    badge."""
    out = _drive("transient-sse-error")
    net = out["net"]

    assert "sse-open:/jobs/api/job/j1/progress/stream" in net
    assert not any(
        entry.startswith(("timeline-append:", "append:", "text:", "width:", "remove:", "scroll:"))
        for entry in out["dom"]
    ), f"no poll rendered anything on a stream-backed page with no progress changes: {out['dom']}"
    assert net[-1] == "mode:realtime", (
        f"a dropped stream must not flip the badge; the probe still says Redis is healthy: {net}"
    )
    # Polling bridged the gap while the stream was down...
    assert "fetch:/jobs/api/job/j1/state" in net
    # ...and stood down once the stream was live again.
    after_reopen = net[net.index("poll:3") + 1 :]
    assert not any(entry.startswith("fetch:/jobs/api/job") for entry in after_reopen), (
        f"the bridging poll must stand down when the stream reconnects: {after_reopen}"
    )
