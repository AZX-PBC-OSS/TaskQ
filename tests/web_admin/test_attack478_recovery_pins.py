"""Livelock/starvation pins for the #478 progress-recovery driver.

The #478 diff rewrote the client recovery loop in ``static/realtime.js``:
``acceptProgress`` now SKIPS empty snapshots (the headline fix), the
client keeps a cross-source seq cursor, and stream recovery changed from
close-on-error to browser-reconnect-with-polling-standby. Four candidate
loop shapes come with that territory, and each gets a deterministic pin
driven under Node with virtual timers (same harness family as
``test_realtime_js_mode_probe.py``; assertions are on issued requests,
renders and teardown, never on source text):

1. SPIN: a skip must not change the loop's cadence. The recovery loop
   wakes on the poll interval (or on a pushed frame); under sustained
   empty snapshots the wake rate stays exactly one per interval, the skip
   never arms the poller hotter, and no interval leaks.
2. STARVATION: an event arriving inside (or right after) a skip's window
   must still render at push latency. The skip's early-continue consumes
   only the skipped snapshot; it must not consume the next event's wake,
   and the reconnect catch-up plus the poll reconcile must leave the next
   live delta rendering.
3. LATCH: repeated empties must never exit the recovery machinery. No
   empty-counter give-up: after minutes of empties and error flaps the
   stream is still live (or the poller re-armed), so an idle broker
   cannot kill the page's progress path.
4. TEARDOWN (the bounded-close family): terminal discovery stops the
   poller and closes the stream exactly once, and the skip's
   early-continue must not sit between the terminal observation and the
   close.

Mutations (literal patches applied to a scratch copy of realtime.js)
prove each pin is sharp: the hot-spin re-poll, the skip that swallows the
terminal check, the empty-counter give-up, and the catch-up drop each
turn at least one pin red.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")

pytestmark = [pytest.mark.fastapi]

REALTIME_JS = (
    Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "realtime.js"
)

requires_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="realtime.js behaviour is driven under Node"
)

# Virtual-clock harness. Everything the module can observe is scripted:
# the badge mode, the section attributes, fetch bodies per URL, a manually
# driven EventSource whose handlers the scenario fires, and an advance()
# that runs due timers in order. stats is the pin's liveness counter set.
_HARNESS = r"""
const fs = require("fs");
const srcPath = process.argv[process.argv.length - 2];
const scenarioArg = process.argv[process.argv.length - 1];
const src = fs.readFileSync(srcPath, "utf8");
// the scenario travels as the argv JSON itself: a fixed temp file would
// be clobbered by concurrent xdist workers driving this harness
const scenario = JSON.parse(scenarioArg);

const stats = {
    pollTicks: 0,
    stateFetches: [],   // virtual ms of each /state fetch
    modeFetches: 0,
    renders: [],        // {at, percent, meta}
    sseOpens: 0,
    sseCloses: 0,
    intervalsCreated: 0,
    liveIntervals: 0,
};

let now = 0;
let timers = [];
let nextTimer = 1;
let es = null;

global.window = { TASKQ_BASE_PATH: "/taskq" };
global.POLL_INTERVAL_MS = scenario.pollIntervalMs || 1000;

const badge = {
    attrs: { "data-mode": scenario.badgeMode },
    textContent: "",
    setAttribute(k, v) { this.attrs[k] = v; },
    getAttribute(k) { return this.attrs[k]; },
};
const section = {
    attrs: scenario.sectionAttrs || {},
    getAttribute(k) { return this.attrs[k]; },
};

function makeEl() {
    return {
        className: "", style: {}, textContent: "", children: [],
        appendChild(c) { this.children.push(c); },
        scrollIntoView() {},
    };
}
const timeline = {
    children: [],
    appendChild(entry) {
        const barWrap = entry.children.find((c) => c.className === "progress-bar-wrap");
        const detail = entry.children.find((c) => c.className === "progress-detail");
        const meta = entry.children.find((c) => c.className === "progress-meta");
        const bar = barWrap ? barWrap.children.find((c) => c.className === "progress-bar") : null;
        stats.renders.push({
            at: now,
            percent: bar ? bar.style.width : null,
            meta: meta ? meta.textContent : "",
            detail: detail ? detail.textContent : "",
        });
        this.children.push(entry);
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
    createElement() { return makeEl(); },
};

// EventSource: constructed once per openEventSource; the SCENARIO fires
// events at it. A real browser reconnect reuses the same instance and its
// listeners, modelled by the "reopen" step (fire open + catch-up on the
// live instance). close() only records; after close nothing is delivered.
global.EventSource = class {
    constructor(url) {
        this.url = url;
        this.handlers = {};
        this.closed = false;
        stats.sseOpens += 1;
        es = this;
    }
    addEventListener(name, fn) { this.handlers[name] = fn; }
    close() { if (!this.closed) stats.sseCloses += 1; this.closed = true; }
};

function fireSSE(ev) {
    if (!es || es.closed) return;
    const handler = es.handlers[ev.type];
    if (!handler) return;
    handler({
        data: typeof ev.data === "string" ? ev.data : JSON.stringify(ev.data),
        lastEventId: ev.id === undefined ? "" : String(ev.id),
    });
}

global.setInterval = (fn, ms) => {
    if (ms === POLL_INTERVAL_MS) stats.intervalsCreated += 1;
    const id = nextTimer++;
    timers.push({ id, fn, ms, due: now + ms, kind: "interval" });
    return id;
};
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };
global.setTimeout = (fn, ms) => {
    const id = nextTimer++;
    timers.push({ id, fn, ms, due: now + ms, kind: "timeout" });
    return id;
};
global.clearTimeout = (id) => { timers = timers.filter((t) => t.id !== id); };

function bodyFor(url) {
    for (const rule of scenario.fetchRules || []) {
        if (url.endsWith(rule.suffix)) return rule.body;
    }
    return {};
}

// Promise-like chain: the first .then receives the response object
// ({json}), every later .then receives the previous callback's result;
// catch is a no-op anywhere (the module's poll chain swallows errors).
const valChain = (getV) => {
    const p = {
        then(f) {
            let v;
            try { v = f(getV()); } catch (e) { v = undefined; }
            return valChain(() => v);
        },
        catch() { return p; },
    };
    return p;
};
const respChain = (getBody) => {
    const p = {
        then(f) {
            let v;
            try { v = f({ json: () => getBody() }); } catch (e) { v = undefined; }
            return valChain(() => v);
        },
        catch() { return p; },
    };
    return p;
};

global.fetch = (url) => {
    if (url.endsWith("/sse/mode")) stats.modeFetches += 1;
    else stats.stateFetches.push(now);
    return respChain(() => bodyFor(url));
};

function advance(ms) {
    const target = now + ms;
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due;
        if (due.kind === "interval") due.due += due.ms;
        else timers = timers.filter((t) => t.id !== due.id);
        due.fn();
    }
    now = target;
}

new Function(src)();

const steps = (scenario.steps || []).slice().sort((a, b) => a.at - b.at);
for (const step of steps) {
    if (step.at > now) advance(step.at - now);
    if (step.do === "fire") fireSSE(step);
    else if (step.do === "error") {
        if (es && !es.closed && es.handlers.error) es.handlers.error({});
    } else if (step.do === "reopen") {
        if (es && !es.closed) {
            if (es.handlers.open) es.handlers.open({});
            for (const ev of step.fire || []) fireSSE(ev);
        }
    }
}
if (scenario.horizonMs > now) advance(scenario.horizonMs - now);

// live poller intervals only: the 30 s health probe is a separate,
// legitimately-lifelong interval and must not pollute the poller's count
stats.liveIntervals = timers.filter(
    (t) => t.kind === "interval" && t.ms === POLL_INTERVAL_MS
).length;
stats.mode = badge.attrs["data-mode"];
stats.esLive = es !== null && !es.closed;
process.stdout.write(JSON.stringify(stats));
"""


def _drive(scenario: dict[str, Any], *, js_path: Path | None = None) -> dict[str, Any]:
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; paths are this file's constants or a scratch copy.
        [node, "-e", _HARNESS, "--", str(js_path or REALTIME_JS), json.dumps(scenario)],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout)


# A running job whose progress_state jsonb is still NULL (the actor never
# called progress(); the state transitions consumed seqs 1..3). This is
# the "empty snapshot" the #478 skip exists for.
_EMPTY_RUNNING = {"status": "running", "progress_state": None, "progress_seq": 3}

# The realtime scenarios must keep the health probe convinced Redis is up,
# or the 30 s probe degrades the page and closes the stream itself.
_REDIS_UP = [{"suffix": "/sse/mode", "body": {"realtime": True}}]


@requires_node
def test_pin_sustained_empty_polls_wake_at_exactly_the_poll_interval() -> None:
    """SPIN (poll path): 120 s of empty snapshots must cost exactly one
    wake per poll interval, render nothing, and leave exactly the one
    interval alive. A skip that re-arms hotter (a 0-timeout re-poll) or
    leaks intervals breaks the count."""
    stats = _drive(
        {
            "badgeMode": "polling-degraded",
            "sectionAttrs": {"data-job-id": "j1"},
            "fetchRules": [{"suffix": "/state", "body": _EMPTY_RUNNING}],
            "horizonMs": 120_000,
        }
    )
    assert stats["mode"] == "polling-degraded"
    assert len(stats["stateFetches"]) == 120  # 120 s at the 1 s cadence: no hotter wake
    assert stats["intervalsCreated"] == 1  # no re-arm, no leak
    assert stats["liveIntervals"] == 1
    assert stats["renders"] == []  # every empty snapshot skipped, no render churn


@requires_node
def test_pin_sustained_empty_sse_frames_stay_on_the_stream() -> None:
    """SPIN (stream path): empty progress frames pushed once a second must
    render nothing, never wake the poller, and never tear down or reopen
    the stream. A skip that bounces the connection per empty or arms the
    poller breaks this."""
    frames = [
        {"at": i * 1000, "do": "fire", "type": "progress", "id": 3 + i, "data": "{}"}
        for i in range(1, 61)
    ]
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": _REDIS_UP,
            "steps": frames,
            "horizonMs": 61_000,
        }
    )
    assert stats["renders"] == []  # every empty frame skipped
    assert stats["stateFetches"] == []  # the skip never woke the poller
    assert stats["sseOpens"] == 1  # no close/reopen churn on the skip path
    assert stats["sseCloses"] == 0
    assert stats["esLive"] is True  # the recovery machinery stayed armed


@requires_node
def test_pin_event_after_an_empty_skip_renders_at_push_latency() -> None:
    """STARVATION: the first real delta arriving after the skipped empty
    snapshot must render at push latency (t=500 ms), not wait for the next
    poll period. The skip's early-continue must not consume the next
    event's wake."""
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": _REDIS_UP,
            "steps": [
                # initial PG snapshot: empty, seq 3, cursor-deduped
                {"at": 0, "do": "fire", "type": "progress", "id": 3, "data": "{}"},
                # the first real event, a call-level delta
                {
                    "at": 500,
                    "do": "fire",
                    "type": "progress",
                    "id": 4,
                    "data": {"kind": "progress", "percent": 10},
                },
            ],
            "horizonMs": 5_000,
        }
    )
    assert len(stats["renders"]) == 1
    assert stats["renders"][0]["at"] == 500  # push latency, not a poll period
    assert stats["renders"][0]["percent"] == "10%"
    assert stats["stateFetches"] == []


@requires_node
def test_pin_reconnect_catch_up_is_not_dropped_and_the_wake_is_not_lost() -> None:
    """STARVATION (recovery): deltas render, the stream drops, the poll
    reconciles the outage gap (seq 9), the browser reconnects, the
    server's catch-up snapshot (seq 9) is cursor-deduped, and the NEXT
    live delta (seq 10) still renders. Neither the skip nor the dedup may
    swallow the post-recovery wake, and the poller must stand down once
    the stream is back."""
    full_at_9 = {"status": "running", "progress_state": {"percent": 90}, "progress_seq": 9}
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": [*_REDIS_UP, {"suffix": "/state", "body": full_at_9}],
            "steps": [
                {
                    "at": 100,
                    "do": "fire",
                    "type": "progress",
                    "id": 4,
                    "data": {"kind": "progress", "percent": 20},
                },
                {
                    "at": 200,
                    "do": "fire",
                    "type": "progress",
                    "id": 5,
                    "data": {"kind": "progress", "percent": 30},
                },
                # stream drops; the browser retries on its own
                {"at": 300, "do": "error"},
                # the poll reconciles to 90 within its first tick; the
                # reconnect completes at t=4000 with catch-up seq 9
                # (cursor already there -> deduped) and live delta seq 10.
                {
                    "at": 4000,
                    "do": "reopen",
                    "fire": [
                        {"type": "progress", "id": 9, "data": '{"percent": 90}'},
                        {"type": "progress", "id": 10, "data": {"kind": "progress", "percent": 95}},
                    ],
                },
            ],
            "horizonMs": 10_000,
        }
    )
    percents = [r["percent"] for r in stats["renders"]]
    assert "20%" in percents and "30%" in percents  # the pre-drop deltas
    assert "90%" in percents  # the poll reconciled the outage gap
    assert percents[-1] == "95%"  # the post-recovery wake was NOT lost
    # the poller stood down at the reconnect (t=4000): no fetch after it
    assert stats["stateFetches"] and max(stats["stateFetches"]) < 4000
    assert stats["esLive"] is True


@requires_node
def test_pin_minutes_of_empties_and_flaps_never_exit_the_recovery_machinery() -> None:
    """LATCH: ten minutes of empty frames plus an error/reopen flap every
    5 s must end with the machinery still armed (stream live, no stray
    interval), no give-up exit. An idle broker must not kill the page's
    progress path."""
    steps: list[dict[str, Any]] = []
    for i in range(1, 300):  # an empty frame every 2 s for ~10 min
        steps.append({"at": i * 2000, "do": "fire", "type": "progress", "id": 3 + i, "data": "{}"})
    for i in range(1, 120):  # a flap every 5 s, reconnect 100 ms later
        steps.append({"at": i * 5000, "do": "error"})
        steps.append({"at": i * 5000 + 100, "do": "reopen"})
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": _REDIS_UP,
            "steps": steps,
            "horizonMs": 600_000,
        }
    )
    assert stats["esLive"] is True  # never gave up on the stream
    assert stats["liveIntervals"] == 0  # the standby poller is reaped
    assert stats["renders"] == []  # nothing rendered, nothing spun


@requires_node
def test_pin_terminal_discovered_by_the_poll_tears_everything_down_once() -> None:
    """TEARDOWN (poll path): the poll's terminal observation stops the
    poller after exactly one tick. The skip's early-continue must not sit
    between the terminal observation and the close."""
    stats = _drive(
        {
            "badgeMode": "polling-degraded",
            "sectionAttrs": {"data-job-id": "j1"},
            "fetchRules": [
                {
                    "suffix": "/state",
                    "body": {"status": "succeeded", "progress_state": None, "progress_seq": 4},
                },
            ],
            "horizonMs": 5_000,
        }
    )
    assert len(stats["stateFetches"]) == 1  # first tick saw terminal, stopped
    assert stats["liveIntervals"] == 0  # poller reaped
    assert stats["sseCloses"] == 0  # nothing was open on a degraded page


@requires_node
def test_pin_terminal_sse_frame_tears_down_and_late_frames_are_inert() -> None:
    """TEARDOWN (stream path): the terminal frame closes the stream and
    stands down the standby poller exactly once; nothing resurrects after
    the close."""
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": _REDIS_UP,
            "steps": [
                {"at": 100, "do": "error"},  # stream drops, poller arms
                {
                    "at": 1100,
                    "do": "fire",
                    "type": "progress",
                    "id": 4,
                    "data": {"kind": "progress", "percent": 50},
                },
                {
                    "at": 1200,
                    "do": "reopen",
                    "fire": [
                        {
                            "type": "terminal",
                            "id": 9,
                            "data": {"kind": "state_change", "status": "failed", "terminal": True},
                        },
                    ],
                },
            ],
            "horizonMs": 5_000,
        }
    )
    assert stats["sseCloses"] == 1  # exactly one teardown
    assert stats["esLive"] is False
    assert stats["liveIntervals"] == 0  # the standby poller stood down
    percents = [r["percent"] for r in stats["renders"]]
    assert "50%" in percents  # the frame before the terminal rendered


@requires_node
def test_pin_error_flap_creates_no_more_than_one_interval_per_flap() -> None:
    """TEARDOWN (bounded-close family): repeated error/reopen flaps must
    create at most one live interval at any time - startPolling's guard
    plus stopPolling's reap, no interval leak under churn."""
    steps: list[dict[str, Any]] = []
    for i in range(1, 21):
        steps.append({"at": i * 1000, "do": "error"})
        steps.append({"at": i * 1000 + 100, "do": "reopen"})
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": _REDIS_UP,
            "steps": steps,
            "horizonMs": 25_000,
        }
    )
    assert stats["esLive"] is True
    assert stats["liveIntervals"] == 0  # stream is up; no standby left ticking
    assert stats["intervalsCreated"] <= 20  # one arm per flap, guard the rest


# ---------------------------------------------------------------------------
# Mutations: each pin must be sharp. Patches are applied to a scratch copy
# of realtime.js; a pin that stays green under a mutation is not a pin.
# ---------------------------------------------------------------------------

_SCRATCH = Path(tempfile.mkdtemp(prefix="taskq-attack478-scratch-"))


def _mutated(name: str, *patches: tuple[str, str]) -> Path:
    _SCRATCH.mkdir(parents=True, exist_ok=True)
    path = _SCRATCH / f"realtime_{name}.js"
    src = REALTIME_JS.read_text(encoding="utf-8")
    for old, new in patches:
        assert old in src, f"mutation {name}: anchor text not found"
        src = src.replace(old, new, 1)
    path.write_text(src, encoding="utf-8")
    return path


def _mutated_poll_hot_spin() -> Path:
    """The hot-spin regression: the poller re-arms a 0 ms self-re-poll on
    every tick instead of waiting for the interval."""
    return _mutated(
        "hot_spin",
        (
            "pollingInterval = setInterval(function () {",
            "pollingInterval = setInterval(function () {"
            " setTimeout(function () { if (pollingActive)"
            " fetch(`${BASE}/jobs/api/job/${jobId}/state`)"
            ".then(function () {}).catch(function () {}); }, 0);",
        ),
    )


def _mutated_skip_swallows_terminal() -> Path:
    """The lost-wake regression: the empty-snapshot skip's early-continue
    also skips the tick's terminal check - a skipped snapshot consumes the
    observation, the poller runs past the terminal forever."""
    return _mutated(
        "skip_terminal",
        (
            "if (fingerprint === null || (fingerprint === lastRenderedProgress"
            " && !actorProgress)) return;",
            "if (fingerprint === null || (fingerprint === lastRenderedProgress"
            " && !actorProgress)) return false;",
        ),
        (
            "lastRenderedProgress = fingerprint;\n        renderProgressEvent(state);",
            "lastRenderedProgress = fingerprint;\n        renderProgressEvent(state);\n"
            "        return true;",
        ),
        (
            "if (!Number.isInteger(seq) || seq <= lastSeenSeq) return;",
            "if (!Number.isInteger(seq) || seq <= lastSeenSeq) return false;",
        ),
        (
            "acceptProgress(body.progress_seq, body.progress_state ?? {}, false);",
            "if (acceptProgress(body.progress_seq, body.progress_state ?? {}, false)"
            " === false) return;",
        ),
    )


def _mutated_empty_giveup() -> Path:
    """The premature give-up: three consecutive empty snapshots close the
    stream and stand everything down - the stream dies on an idle broker."""
    return _mutated(
        "empty_giveup",
        (
            "function acceptProgress(seq, rawState, merge) {\n"
            "        if (!Number.isInteger(seq) || seq <= lastSeenSeq) return;",
            "let emptySkips = 0;\n"
            "    function acceptProgress(seq, rawState, merge) {\n"
            "        if (!Number.isInteger(seq) || seq <= lastSeenSeq) return;\n"
            "        const isEmpty = rawState && rawState.step == null"
            " && rawState.percent == null && rawState.detail == null"
            " && rawState.data == null;\n"
            "        if (isEmpty) {\n"
            "            emptySkips += 1;\n"
            "            if (emptySkips >= 3 && eventSource) { eventSource.close();"
            " eventSource = null; stopPolling(); }\n"
            "        } else {\n"
            "            emptySkips = 0;\n"
            "        }",
        ),
    )


def _mutated_catchup_drop() -> Path:
    """The catch-up drop: a reconnect snapshot that jumps the cursor by
    more than one seq is discarded without rendering - the reconciled
    state never reaches the timeline."""
    return _mutated(
        "catchup_drop",
        (
            "if (!Number.isInteger(seq) || seq <= lastSeenSeq) return;\n        lastSeenSeq = seq;",
            "if (!Number.isInteger(seq) || seq <= lastSeenSeq) return;\n"
            "        if (!merge && seq > lastSeenSeq + 1) { lastSeenSeq = seq; return; }\n"
            "        lastSeenSeq = seq;",
        ),
    )


@requires_node
def test_mutation_hot_spin_is_caught_by_the_poll_cadence_pin() -> None:
    stats = _drive(
        {
            "badgeMode": "polling-degraded",
            "sectionAttrs": {"data-job-id": "j1"},
            "fetchRules": [{"suffix": "/state", "body": _EMPTY_RUNNING}],
            "horizonMs": 10_000,
        },
        js_path=_mutated_poll_hot_spin(),
    )
    # the honest 1 s cadence issues 10 fetches in 10 s; the self-re-arm
    # doubles (or worse) the wake rate
    assert len(stats["stateFetches"]) > 10


@requires_node
def test_mutation_skip_swallows_terminal_is_caught_by_the_teardown_pin() -> None:
    stats = _drive(
        {
            "badgeMode": "polling-degraded",
            "sectionAttrs": {"data-job-id": "j1"},
            "fetchRules": [
                {
                    "suffix": "/state",
                    "body": {"status": "succeeded", "progress_state": None, "progress_seq": 4},
                },
            ],
            "horizonMs": 5_000,
        },
        js_path=_mutated_skip_swallows_terminal(),
    )
    assert len(stats["stateFetches"]) > 1  # the poller ran past the terminal
    assert stats["liveIntervals"] == 1  # ...and is still running


@requires_node
def test_mutation_empty_giveup_is_caught_by_the_latch_pin() -> None:
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": _REDIS_UP,
            "steps": [
                {"at": 1000, "do": "fire", "type": "progress", "id": 4, "data": "{}"},
                {"at": 2000, "do": "fire", "type": "progress", "id": 5, "data": "{}"},
                {"at": 3000, "do": "fire", "type": "progress", "id": 6, "data": "{}"},
            ],
            "horizonMs": 10_000,
        },
        js_path=_mutated_empty_giveup(),
    )
    assert stats["esLive"] is False  # the give-up killed the stream


@requires_node
def test_mutation_catchup_drop_is_caught_by_the_starvation_pin() -> None:
    full_at_9 = {"status": "running", "progress_state": {"percent": 90}, "progress_seq": 9}
    stats = _drive(
        {
            "badgeMode": "realtime",
            "sectionAttrs": {"data-job-id": "j1", "data-progress-seq": "3"},
            "fetchRules": [*_REDIS_UP, {"suffix": "/state", "body": full_at_9}],
            "steps": [
                {
                    "at": 100,
                    "do": "fire",
                    "type": "progress",
                    "id": 4,
                    "data": {"kind": "progress", "percent": 20},
                },
                {"at": 300, "do": "error"},
                {
                    "at": 4000,
                    "do": "reopen",
                    "fire": [
                        {"type": "progress", "id": 9, "data": '{"percent": 90}'},
                    ],
                },
            ],
            "horizonMs": 10_000,
        },
        js_path=_mutated_catchup_drop(),
    )
    # green code renders the reconciled 90 (poll path, first tick at 1300);
    # the drop mutant discards both the poll's snapshot and the catch-up
    percents = [r["percent"] for r in stats["renders"]]
    assert "90%" not in percents
