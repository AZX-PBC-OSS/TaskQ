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

// A state object nested *depth* levels deep ({"a": {"a": ... 1}}), the
// shape the deep-data monster ships.
function deepState(depth) {
    let v = 1;
    for (let i = 0; i < depth; i++) v = { a: v };
    return v;
}

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
            { status: "running", progress_seq: 2, progress_state: { percent: 50, step: "stage", detail: "half" } },
            { notModified: true },
            { notModified: true },
        ],
    },
    // The seq monster at the double-precision boundary: the poll body's
    // progress_seq arrives as a JS number, and 2^53 + 1 parses back as
    // exactly 2^53 - the value the cursor already holds. The cursor must
    // come from the ETag (the exact decimal), so the tick RENDERS instead
    // of being dropped as a duplicate of the state before it. Both polls'
    // bodies carry the mangled double (the first the plain 2^53 value the
    // second's body would parse to); the second's ETag carries the exact
    // 2^53 + 1 the server compared.
    "bigint-seq-collision": {
        seed: { seq: 0, state: {} },
        polls: [
            { status: "running", progress_seq: 9007199254740992, progress_state: { percent: 50, step: "stage", detail: "half" } },
            { status: "running", progress_seq: 9007199254740992, etag: "9007199254740993", progress_state: { percent: 75, step: "stage", detail: "more" } },
        ],
    },
    // The data monster: a progress_state whose data is nested 10000 deep.
    // The fingerprint machinery (canonicalize + JSON.stringify) used to
    // recurse unbounded and RangeError on exactly this shape, killing the
    // poll's then-chain: the tick wrote nothing and every later tick was
    // swallowed by the same throw. Depth-capped, the tick renders and the
    // machine stands.
    "deeply-nested-data": {
        seed: { seq: 0, state: {} },
        polls: [
            { status: "running", progress_seq: 2, progress_state: { step: "deep", data: deepState(10000) } },
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

// A real promise chain: the module's poll path awaits ``res.json()``
// (the browser contract), so the stub answers with one and the async
// timer pump below drains the microtasks between ticks.
global.fetch = async (url, opts) => {
    net.push(`fetch:${url.split("?")[0]}`);
    if (opts && opts.headers && opts.headers["If-None-Match"] !== undefined) {
        net.push(`inm:${opts.headers["If-None-Match"]}`);
    }
    let status = 200;
    let body = { status: "running", progress_seq: 0, progress_state: {} };
    let scriptedEtag = null;
    if (config && url.endsWith("/state")) {
        const scripted = config.polls[pollCount] ?? config.polls[config.polls.length - 1];
        pollCount += 1;
        if (scripted.notModified) {
            status = 304;
        } else {
            body = scripted;
            scriptedEtag = scripted.etag ?? null;
        }
    } else if (url.endsWith("/sse/mode")) {
        body = { realtime: sseScenario };
    }
    // The state endpoint sets an ETag on every answer: the exact decimal
    // progress_seq, the cursor's precision-safe source. A scenario can
    // override it (``etag``) to stage the double-precision collision -
    // body.progress_seq mangled by JSON parsing, ETag exact.
    const responseEtag = status === 304 || !body || body.progress_seq === undefined
        ? null
        : `"${body.progress_seq}"`;
    return {
        status,
        json: () => Promise.resolve(body),
        headers: {
            get(name) {
                if (name.toLowerCase() !== "etag") return null;
                return scriptedEtag !== null ? `"${scriptedEtag}"` : responseEtag;
            },
        },
    };
};

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

// One macrotask hop per fired timer: the poll's promise callbacks are
// microtasks, so the pump yields once after each firing to let them land
// BEFORE the next tick - the per-tick log segmentation depends on it.
async function advanceOneTick() {
    const target = now + POLL_INTERVAL_MS;
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due; due.due += due.ms; due.fn();
        await new Promise((resolve) => setImmediate(resolve));
    }
    now = target;
    await new Promise((resolve) => setImmediate(resolve));
}

async function main() {
    new Function(src)();
    if (sseScenario) {
        // A realtime page whose stream drops mid-flight (a proxy blip): the
        // badge must stay calm while polling bridges the gap, and the badge
        // only moves when the periodic probe - the authority - says so.
        mark("poll:1");
        await advanceOneTick();
        global.lastEventSource.emitError();
        mark("poll:2");
        await advanceOneTick();
        // The probe still reports Redis healthy: no mode transition.
        global.lastEventSource.emitOpen();
        mark("poll:3");
        await advanceOneTick();
    } else {
        for (let i = 0; i < config.polls.length; i += 1) {
            mark(`poll:${i + 1}`);
            await advanceOneTick();
        }
    }
    mark(`mode:${badge.attrs["data-mode"]}`);
    process.stdout.write(JSON.stringify({ dom, net }));
}

main();
"""


def _drive(scenario: str) -> dict[str, list[str]]:
    node = shutil.which("node")
    assert node is not None
    # No per-spawn wall-clock deadline on purpose: the harness is fully
    # virtual (scripted fetches, virtual timers, a synchronous log), so
    # the child's exit is the only event worth waiting for, and a fixed
    # deadline is a delay that races child startup, not a behaviour gate.
    # Under co-tenant load (-n 4 plus a CPU/IO stressor) a starved Node
    # startup blew a 30s deadline and turned a behaviourally-correct pin
    # red (the same child runs in ~60ms of CPU once scheduled). A wedged
    # child is the suite-wide pytest-timeout budget's job (--timeout=300
    # in addopts, every lane): it fails the hung test by name with a
    # stack dump instead of guessing a threshold no load condition can
    # justify.
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and scenario names are this file's own constants.
        [node, "-e", _HARNESS, "--", str(REALTIME_JS), scenario],
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
    net_segments = _segments(log["net"])

    first = segments[1]
    assert any(entry.startswith("timeline-append:") for entry in first), (
        f"the first poll renders the entry once: {first}"
    )
    for tick, expected_inm in ((2, '"2"'), (3, '"3"'), (4, '"4"')):
        assert segments[tick] == [], (
            f"a repeated identical poll must perform zero DOM mutations "
            f"(and therefore zero layout), tick {tick} wrote {segments[tick]}"
        )
        # The zero-write is not vacuous: the tick polled (the cadence
        # kept running) and it polled the sequence the previous tick's
        # gate advanced - the fingerprint gate drops the render but the
        # conditional-GET cursor moves with every sequence, dropped or
        # not, so the next tick asks about the NEW state, not a stale one.
        assert net_segments[tick] == [
            "fetch:/jobs/api/job/j1/state",
            f"inm:{expected_inm}",
        ], (
            f"tick {tick} must poll the advanced conditional-GET cursor "
            f"while writing nothing: {net_segments[tick]}"
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


@requires_node
def test_a_seq_at_the_double_precision_boundary_still_renders() -> None:
    """The seq monster at 2^53: the poll body's ``progress_seq`` arrives as
    a JS double, and 9007199254740993 parses back as exactly 9007199254740992
    - the value the cursor already holds - so under the Number-based gate
    the tick compared equal to the cursor and was dropped as a duplicate:
    the timeline froze on the state before it, forever. The cursor must
    come from the ETag header (the exact decimal the server compares), so
    the changed state at seq 2^53 + 1 RENDERS."""
    out = _drive("bigint-seq-collision")
    segments = _segments(out["dom"])

    first = segments[1]
    assert any(entry.startswith("timeline-append:") for entry in first), (
        f"the first poll (seq 2^53) renders the entry: {first}"
    )
    assert segments[2] == ["width:div3=75%", "text:div4=more", "text:div5=75% · stage"], (
        f"the tick at seq 2^53 + 1 must advance the exact cursor and patch "
        f"its nodes, not be dropped as a duplicate of 2^53: {segments[2]}"
    )
    _no_scroll(out["dom"])


@requires_node
def test_a_deeply_nested_data_monster_cannot_crash_the_fingerprint() -> None:
    """The data monster: a progress state nested 10000 deep used to
    RangeError the fingerprint's recursive canonicalize (and the
    JSON.stringify beneath it) - the poll's then-chain died mid-tick, the
    tick wrote nothing, and every later tick died the same way: the
    timeline froze with stale content while Postgres held new state. The
    depth-capped canonicalize degrades gracefully: the tick renders, the
    data node degrades to its notice, and the machine stands."""
    out = _drive("deeply-nested-data")
    segments = _segments(out["dom"])

    first = segments[1]
    assert any(entry.startswith("timeline-append:") for entry in first), (
        f"the deep-data tick must render the entry, not throw: {first}"
    )
    assert any(entry.startswith("text:") and entry.endswith("=deep") for entry in first), (
        f"the deep-data tick must render the state's own fields: {first}"
    )
    _no_scroll(out["dom"])
