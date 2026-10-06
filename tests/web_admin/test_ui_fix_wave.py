"""The consolidated admin-UI fix wave's pins (B1-B5, U1-U7, D1/D2/D4).

Every pin is the mutation-checked teeth of one finding from the admin-UI
audit: revert the fix and the pin reds. The findings live in the review
ledger; this file holds the UI-side half, ``tests/test_api_fix_wave.py``
the API-side half.

B1 - the shipped deployments never passed a backend to ``create_router``,
     so every backend-mediated mutation button (cancel / retry / run-now)
     posted to a route that answered 503. The server-side half: a
     deployment with no backend must not RENDER the dead buttons at all
     (the templates read the bundle off the render context).
B2 - ``TASKQ_ADMIN_ACTIONS_ENABLED=false`` (the default) still rendered
     every action form, and the POST refusal came out as a CSRF error
     (the CSRF dependency ran before the enabled-check) - a config state
     reported as a token problem. The enabled-check now runs first with
     an actionable message, and disabled deployments render a banner and
     no forms.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from taskq._ids import new_uuid

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state

from . import StubPool

pytestmark = [pytest.mark.fastapi]

requires_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="the JS behaviour is driven under Node"
)

_ACTIONS_BANNER = "Actions are disabled on this deployment (TASKQ_ADMIN_ACTIONS_ENABLED=false)"

_JOB_ID = new_uuid()


class _JobDetailPool(StubPool):
    """A stub pool whose connection answers the job-detail page's reads.

    The job fetch returns a pending live row (so the cancel form's
    condition is met); every other read is empty, like :class:`StubPool`.
    """

    def __init__(self) -> None:
        self.job: dict[str, Any] | None = {
            "id": _JOB_ID,
            "actor": "test_actor",
            "queue": "default",
            "status": "pending",
            "created_at": "2026-01-01T00:00:00+00:00",
            "scheduled_at": "2026-01-01T00:00:00+00:00",
            "started_at": None,
            "finished_at": None,
            "attempt": 0,
            "max_attempts": 3,
            "retry_kind": "transient",
            "priority": 0,
            "identity_key": None,
            "fairness_key": None,
            "locked_by_worker": None,
            "lock_expires_at": None,
            "cancel_requested_at": None,
            "progress_state": None,
            "progress_seq": 0,
            "payload": None,
            "metadata": None,
            "result": None,
            "error_class": None,
            "error_message": None,
            "error_traceback": None,
            "trace_id": None,
            "span_id": None,
            "tags": [],
            "schedule_to_close": None,
            "start_to_close": None,
            "heartbeat_timeout": None,
            "cancel_phase": 0,
            "result_size_bytes": None,
        }

    async def _maybe_job(self, query: str, *args: object) -> dict[str, Any] | None:
        if 'FROM "taskq".jobs WHERE id = $1' in query:
            # The fetch binds the requested id: only THAT id resolves to
            # the canned row, so a foreign uuid is a genuine miss.
            return self.job if str(args[0]) == str(_JOB_ID) else None
        return None


class _JobDetailConnection:
    """Connection stub routing each admin read to the right canned answer."""

    def __init__(self, pool: _JobDetailPool) -> None:
        self._pool = pool

    def transaction(self) -> Any:
        from . import StubTransaction

        return StubTransaction()

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        return []

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        return await self._pool._maybe_job(query, *args)

    async def fetchval(self, query: str, *args: object) -> Any:
        from datetime import UTC, datetime

        if "clock_timestamp()" in query:
            # The clock-offset probe must see a datetime, the shape the
            # real database answers with (see StubConnection.fetchval).
            return datetime.now(UTC)
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _JobDetailAcquire:
    def __init__(self, pool: _JobDetailPool) -> None:
        self._pool = pool

    async def __aenter__(self) -> _JobDetailConnection:
        return _JobDetailConnection(self._pool)

    async def __aexit__(self, *args: object) -> None:
        pass


def _make_job_detail_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    backend: object | None = None,
    actions_enabled: bool | None = None,
) -> TestClient:
    """A TestClient serving the job-detail page from stubbed reads.

    ``actions_enabled=None`` keeps the deployment default (the setting is
    absent from the env) - the exact configuration every fresh deployment
    ships with.
    """
    if actions_enabled is None:
        monkeypatch.delenv("TASKQ_ADMIN_ACTIONS_ENABLED", raising=False)
    else:
        monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", str(actions_enabled).lower())
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")

    pool = _JobDetailPool()
    pool.acquire = lambda **kwargs: _JobDetailAcquire(pool)  # type: ignore[method-assign]

    bundle = create_router(pool, backend=backend)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool; the admin router duck-types asyncpg.Pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


# ── B1: no backend on the bundle → the backend-mediated buttons must not render ─


def test_job_detail_without_backend_renders_no_backend_buttons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """backend=None (the shipped default): no Cancel/Retry forms at all.

    Every backend-mediated route answers 503 from that deployment, so a
    rendered button is a dead button - the templates must consult the
    bundle the render context carries and hide them.
    """
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.get(f"/jobs/{_JOB_ID}")
    assert resp.status_code == 200
    html = resp.text
    assert "Cancel Job" not in html, "a deployment with no backend rendered a dead Cancel button"
    assert "Retry Job" not in html, "a deployment with no backend rendered a dead Retry button"


def test_job_detail_with_backend_renders_the_buttons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: with a backend (and actions enabled) the buttons render."""
    from . import StubBackend

    client = _make_job_detail_app(monkeypatch, backend=StubBackend(), actions_enabled=True)
    resp = client.get(f"/jobs/{_JOB_ID}")
    assert resp.status_code == 200
    assert "Cancel Job" in resp.text


# ── B2: disabled actions → no forms, one banner, an actionable POST refusal ─


def test_job_detail_with_actions_disabled_renders_no_forms_and_one_banner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default settings: the deployment cannot act, so no action form
    renders and one banner says why (with the setting's name, so the
    operator knows the knob)."""
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.get(f"/jobs/{_JOB_ID}")
    assert resp.status_code == 200
    html = resp.text
    assert "Cancel Job" not in html
    assert "Retry Job" not in html
    assert _ACTIONS_BANNER in html
    assert html.count(_ACTIONS_BANNER) == 1, "the disabled-actions banner must render once"


def test_disabled_actions_post_is_refused_actionably_before_csrf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST to a mutation route on a disabled deployment: the refusal names
    the setting, and it wins over the CSRF check (a config state is safe
    to answer before any token validation; the token still guards when
    enabled)."""
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.post(f"/jobs/{_JOB_ID}/cancel", data={"reason": "nope"})
    assert resp.status_code == 403
    assert "TASKQ_ADMIN_ACTIONS_ENABLED" in resp.text, (
        f"the refusal must be the actionable config message, got: {resp.text[:200]}"
    )
    assert "CSRF" not in resp.text, "the enabled-check must run BEFORE the CSRF check"


def test_enabled_actions_post_still_requires_csrf(monkeypatch: pytest.MonkeyPatch) -> None:
    """The reorder must not weaken the token: with actions enabled, a POST
    without a token is still refused by the CSRF guard."""
    from . import StubBackend

    client = _make_job_detail_app(monkeypatch, backend=StubBackend(), actions_enabled=True)
    resp = client.post(f"/jobs/{_JOB_ID}/cancel", data={"reason": "nope"})
    assert resp.status_code == 403
    assert "CSRF" in resp.text


# ── B1: the shipped example deployments pass a backend ────────────────────


_EXAMPLES = (
    ("examples/app.py", "the embedded-shape trigger app"),
    ("examples/admin_app.py", "the sidecar-shape admin app"),
)


@pytest.mark.parametrize(("path", "role"), _EXAMPLES)
def test_example_deployments_pass_a_backend(path: str, role: str) -> None:
    """B1's wiring half: both shipped example deployments hand the admin
    router a Backend, so their action buttons WORK (the compose-stack e2e
    drives a live cancel through them)."""
    import inspect

    import examples.admin_app
    import examples.app

    module = examples.app if path.endswith("app.py") else examples.admin_app
    source = inspect.getsource(module)
    assert re.search(r"create_router\([^)]*backend=", source, re.DOTALL), (
        f"{role} ({path}) mounts the admin router without backend=: every "
        "backend-mediated action button on that deployment posts to a 503"
    )


# ── B3: the live progress nodes carry the server template's classes ──────

_REALTIME_JS = (
    Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "realtime.js"
)

# Drives realtime.js under Node. The timeline is pre-seeded with ONE
# server-rendered snapshot entry (the class string job_detail.html renders)
# and the poll answers a CHANGED snapshot, so the first live event must
# replace the snapshot entry (no stale twin) with nodes carrying the SAME
# Tailwind classes the template renders.
_B3_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 1], "utf8");

const dom = [];
const created = [];

global.window = { TASKQ_BASE_PATH: "" };
global.POLL_INTERVAL_MS = 1000;

const serverEntry = {
    _name: "server-entry",
    className: "progress-event bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded p-3",
    remove() {
        dom.push("remove:server-entry");
        timeline.children = timeline.children.filter((c) => c !== serverEntry);
    },
    style: { width: "40%" },
    textContent: "",
};

const timeline = {
    children: [serverEntry],
    get firstChild() { return this.children[0] ?? null; },
    removeChild(child) {
        this.children = this.children.filter((c) => c !== child);
        child.remove();
    },
    querySelectorAll(sel) {
        if (sel === ".progress-event") return this.children.filter((c) => String(c.className).includes("progress-event"));
        return [];
    },
    appendChild(child) { dom.push(`timeline-append:${child._name}`); this.children.push(child); },
};

function makeNode(tag) {
    const node = {
        _name: `${tag}${created.length}`,
        className: "",
        children: [],
        style: { width: "" },
        appendChild(child) { this.children.push(child); },
        remove() { dom.push(`remove:${this._name}`); },
    };
    let text = "";
    Object.defineProperty(node, "textContent", {
        get() { return text; },
        set(v) { text = v; dom.push(`text:${node._name}=${v}`); },
    });
    created.push(node);
    return node;
}

const section = {
    attrs: { "data-job-id": "j1", "data-progress-seq": "1", "data-progress-state": JSON.stringify({ percent: 40, step: "old" }) },
    getAttribute(k) { return this.attrs[k]; },
};

global.document = {
    addEventListener(name, fn) { if (name === "DOMContentLoaded") fn(); },
    getElementById(id) { return id === "progress-timeline" ? timeline : id === "progress-section" ? section : null; },
    querySelector(sel) { return sel === ".taskq-badge" ? { getAttribute() { return "polling"; }, setAttribute() {}, textContent: "" } : null; },
    createElement(tag) { return makeNode(tag); },
};

// The poll's first tick: a changed snapshot (percent 40 -> 70).
let polled = false;
global.fetch = async (url) => {
    if (String(url).endsWith("/state")) polled = true;
    return {
        status: 200,
        json: () => Promise.resolve({ status: "running", progress_seq: 2, progress_state: { percent: 70, step: "new", detail: "d", ts: "2026-01-01T00:00:00Z" } }),
        headers: { get() { return '"2"'; } },
    };
};
global.EventSource = class { addEventListener() {} close() {} };
// Virtual timers: the pump fires a due timer on setImmediate, so the
// poll's promise chain drains between fires - and the bootstrap's own
// setInterval(startPolling) registration order is honored (pollingActive
// is set BEFORE the first fire, the production order).
let now = 0;
let timers = [];
let nextTimer = 1;
global.setInterval = (fn, ms) => { const id = nextTimer++; timers.push({ id, fn, ms, due: now + ms }); return id; };
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };

async function drain(target) {
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due;
        due.due += due.ms;
        due.fn();
        await new Promise((r) => setImmediate(r));
    }
    now = target;
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
}

async function main() {
    new Function(src)();
    await drain(now + POLL_INTERVAL_MS * 2);
    const live = created.filter((n) => String(n.className).includes("progress-event"));
    process.stdout.write(JSON.stringify({
        dom,
        liveClasses: live.map((n) => n.className),
        created: created.map((n) => n.className),
        timelineChildren: timeline.children.map((c) => c._name),
    }));
}

main();
"""


@requires_node
def test_live_progress_entry_uses_the_server_templates_classes() -> None:
    """B3's styling half: the first live progress event renders with the
    SAME Tailwind classes the server template renders the snapshot entry
    with - the visible bar (track + fill), not an unstyled skeleton."""
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and path are this file's own constants.
        [node, "-e", _B3_HARNESS, "--", str(_REALTIME_JS)],
        capture_output=True,
        text=True,
        check=True,
    )
    out = json.loads(result.stdout)
    classes = " ".join(out["created"])
    assert "bg-slate-50 dark:bg-slate-800" in classes, out["created"]
    assert "flex-1 h-2 bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden" in classes
    assert "h-full bg-blue-500 rounded-full" in classes


@requires_node
def test_first_live_event_replaces_the_server_rendered_snapshot() -> None:
    """B3's twin half: the first live event REPLACES the server-rendered
    snapshot entry - appending next to it froze the boot snapshot above
    the live bar forever (the stale twin)."""
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and path are this file's own constants.
        [node, "-e", _B3_HARNESS, "--", str(_REALTIME_JS)],
        capture_output=True,
        text=True,
        check=True,
    )
    out = json.loads(result.stdout)
    assert "remove:server-entry" in out["dom"], out["dom"]
    assert out["timelineChildren"].count("server-entry") == 0
    assert len(out["liveClasses"]) == 1, out["liveClasses"]


# ── B5: the Alpine tab race ───────────────────────────────────────────────

_ADMIN_JS = Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "admin.js"

_B5_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 1], "utf8");

const log = [];
const components = {};

const tabInput = { name: "tab", value: "live" };
const liveInput = { name: "live", value: "on" };
const form = {
    requestSubmit() { log.push(`submit:tab=${tabInput.value}:live=${liveInput.value}`); },
    querySelector(sel) {
        if (sel === 'input[name="tab"]') return tabInput;
        if (sel === 'input[name="live"]') return liveInput;
        return null;
    },
};

global.window = { __taskqJobConfig: { tab: "live", liveOn: true, pollIntervalMs: 1000, basePath: "/admin" }, htmx: {} };
global.document = {
    addEventListener(name, fn) { if (name === "alpine:init") fn(); },
    body: { addEventListener() {}, removeEventListener() {} },
    getElementById(id) { return id === "job-filters" ? form : null; },
    querySelector() { return null; },
    createElement() { return { querySelector() { return null; } }; },
};
global.Alpine = { data(name, factory) { components[name] = factory; } };
global.FormData = class { *[Symbol.iterator]() {} };
global.EventSource = class { addEventListener() {} close() {} };
global.setInterval = () => 1;
global.clearInterval = () => {};
global.fetch = () => ({ then() { return { then() { return { catch() {} }; } }; } });

new Function(src)();
const page = components["jobsPage"]();
page.init();
// The operator clicks the Archived tab: the SUBMITTED tab parameter must
// be the tab they clicked, not the one they were on.
page.switchTab("archived");
process.stdout.write(JSON.stringify(log));
"""


@requires_node
def test_switch_tab_submits_the_clicked_tab_not_the_previous_one() -> None:
    """B5: switchTab sets the hidden input's value DIRECTLY before
    requestSubmit - Alpine's :value binding flushes asynchronously, so the
    submitted form carried the PREVIOUS tab and the server rendered (and
    the poll kept re-serving) the tab the operator just left."""
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603
        [node, "-e", _B5_HARNESS, "--", str(_ADMIN_JS)],
        capture_output=True,
        text=True,
        check=True,
    )
    log = json.loads(result.stdout)
    assert log == ["submit:tab=archived:live=on"], log


# ── D2: ONE source for the status colors ──────────────────────────────────


def test_status_classes_blob_is_served_from_the_single_python_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The chrome emits the status→class maps as JSON, from the same dict
    the status_badge macro renders badges from."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    from . import StubPool

    bundle = create_router(StubPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    html = bundle.templates.get_template("_base.html").render(
        realtime_mode="polling", mode_label="polling mode", suppress_refresh=True
    )
    assert "__taskqStatusClasses" in html
    assert "bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300" in html
    macro = bundle.templates.from_string(
        "{% from '_partials/job_card.html' import status_badge %}{{ status_badge('failed') }}"
    ).render()
    assert "bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300" in macro


_B5_D2_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 2], "utf8");
const mode = process.argv[process.argv.length - 1];

const components = {};
global.window = { __taskqJobConfig: { tab: "live", liveOn: true, pollIntervalMs: 1000, basePath: "/admin" }, htmx: {} };
if (mode === "with-blob") {
    // A status the JS has never heard of, from the server's own map.
    global.window.__taskqStatusClasses = {
        chip: { weird: "bg-teal-100 text-teal-700 dark:bg-teal-900 dark:text-teal-300" },
        text: { weird: "text-teal-600 dark:text-teal-400" },
        badgeBase: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ",
    };
}
global.document = {
    addEventListener(name, fn) { if (name === "alpine:init") fn(); },
    body: { addEventListener() {}, removeEventListener() {} },
    getElementById() { return { requestSubmit() {}, querySelector() { return null; } }; },
    querySelector() { return null; },
    createElement() { return { querySelector() { return null; } }; },
};
global.Alpine = { data(name, factory) { components[name] = factory; } };

new Function(src)();
const combobox = components["statusCombobox"]();
const page = components["jobsPage"]();
// The live-badge path: a state_change event's status renders through
// BADGE_CLASSES.
const badge = { textContent: "", className: "" };
const row = {
    setAttribute() {},
    querySelector(sel) { return sel === "[data-status-badge]" ? badge : null; },
};
global.document.querySelector = function (sel) {
    return sel === 'tr[data-job-id="j9"]' ? row : null;
};
page.liveOn = true;
page.handleStateChange({ job_id: "j9", status: "weird" });
process.stdout.write(JSON.stringify({
    chip: combobox.chipColor("weird"),
    text: combobox.statusColor("weird"),
    badge: badge.className,
}));
"""


@requires_node
def test_admin_js_derives_its_status_maps_from_the_server_blob() -> None:
    """D2: the three JS maps are DERIVED views of the server's blob - a
    status that exists only in the server map still renders correctly."""
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603
        [node, "-e", _B5_D2_HARNESS, "--", str(_ADMIN_JS), "with-blob"],
        capture_output=True,
        text=True,
        check=True,
    )
    out = json.loads(result.stdout)
    assert out["chip"] == "bg-teal-100 text-teal-700 dark:bg-teal-900 dark:text-teal-300", out
    assert out["text"] == "text-teal-600 dark:text-teal-400", out
    assert out["badge"] == (
        "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium "
        "bg-teal-100 text-teal-700 dark:bg-teal-900 dark:text-teal-300"
    ), out


@requires_node
def test_admin_js_has_no_hand_maintained_status_map_fallback() -> None:
    """D2's mutation tooth: with the blob absent, the maps answer NOTHING
    (there is no hardcoded copy left to fall back to - the four maps died)."""
    node = shutil.which("node")
    assert node is not None
    result = subprocess.run(  # noqa: S603
        [node, "-e", _B5_D2_HARNESS, "--", str(_ADMIN_JS), "no-blob"],
        capture_output=True,
        text=True,
        check=True,
    )
    out = json.loads(result.stdout)
    assert out["chip"] == "", out
    assert out["text"] == "", out
    assert out["badge"] == "", out


# ── D4: the a11y attributes ───────────────────────────────────────────────


def test_status_combobox_carries_a11y_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The status combobox announces its state: aria-expanded tracks the
    Alpine `open` flag and the popup role is declared."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    from . import StubPool

    bundle = create_router(StubPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    source = bundle.templates.loader.get_source(bundle.templates, "jobs.html")[0]  # pyright: ignore[reportOptionalMemberAccess]  # Why: PackageLoader.get_source is not None for a bundled template.
    assert ':aria-expanded="open"' in source
    assert 'aria-haspopup="listbox"' in source


def test_row_view_action_carries_an_accessible_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The jobs table's row-view action is an icon-only link: without an
    aria-label it reads as an empty link to a screen reader."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    from . import StubPool

    bundle = create_router(StubPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    source = bundle.templates.loader.get_source(bundle.templates, "_partials/job_table.html")[0]  # pyright: ignore[reportOptionalMemberAccess]  # Why: PackageLoader.get_source is not None for a bundled template.
    assert 'aria-label="View job {{ job.id }}"' in source


# ── U1: the Postgres-unreachable handler ─────────────────────────────────


class _DeadPoolConnection:
    """Connection whose every checkout dies with an asyncpg connection error."""

    async def __aenter__(self) -> None:
        import asyncpg.exceptions

        raise asyncpg.exceptions.ConnectionDoesNotExistError("server closed the connection")

    async def __aexit__(self, *args: object) -> None:
        pass


class _DeadPgAppPool:
    """A pool whose acquire raises, the docker-stop-PG shape."""

    def acquire(self, **kwargs: object) -> _DeadPoolConnection:
        return _DeadPoolConnection()


def _make_broken_pg_app(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """An admin app whose PG is unreachable, the deployment U1's proof runs."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    pool = _DeadPgAppPool()
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app, raise_server_exceptions=False)


def test_pg_down_html_routes_render_the_branded_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """U1: with Postgres unreachable, an HTML page request renders the
    branded error page (a 503 that says the queue database is unreachable
    and retries itself), never a bare wall."""
    client = _make_broken_pg_app(monkeypatch)
    resp = client.get("/queues", headers={"Accept": "text/html"})
    assert resp.status_code == 503
    assert "unreachable" in resp.text
    assert '<meta http-equiv="refresh"' in resp.text, "the branded page must retry itself"
    assert "TaskQ Admin" in resp.text, "the branded page must be the portal's own chrome"


def test_pg_down_json_routes_get_the_503_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """U1: a JSON route answers the machine envelope + Retry-After, so
    monitors and scripts get a status to react to."""
    client = _make_broken_pg_app(monkeypatch)
    resp = client.get("/jobs/count")
    assert resp.status_code == 503
    assert resp.json() == {"detail": "postgres_unavailable"}
    assert resp.headers.get("retry-after") is not None


# ── U2: HTML-wanting requests get rendered error pages ───────────────────


def test_missing_job_renders_a_404_page_not_a_json_wall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """U2: a job id that does not exist renders the error page for a
    browser; the JSON shape stays for the API-ish routes."""
    from datetime import UTC, datetime

    client = _make_job_detail_app(monkeypatch, backend=None)
    # _JobDetailPool answers no job for a DIFFERENT id (the fetch keys on
    # the canned row's id): request a foreign uuid.
    missing = new_uuid()
    assert missing != _JOB_ID
    resp = client.get(f"/jobs/{missing}", headers={"Accept": "text/html"})
    assert resp.status_code == 404
    assert "TaskQ Admin" in resp.text
    assert "Job not found" in resp.text
    # The JSON contract is intact for non-HTML wants:
    resp_json = client.get(f"/jobs/{missing}")
    assert resp_json.status_code == 404
    assert resp_json.json() == {"detail": "Job not found"}


def test_bad_filter_renders_a_400_page_naming_the_accepted_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """U2: a bogus status filter renders the 400 page whose body carries
    the refusal's detail (which already names the accepted set)."""
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.get("/history", params={"status": "bogus"}, headers={"Accept": "text/html"})
    assert resp.status_code == 400
    assert resp.headers.get("content-type", "").startswith("text/html"), (
        "a browser's bad filter must render the error page, not a JSON wall"
    )
    assert "bogus" in resp.text
    assert "succeeded" in resp.text, "the accepted status list must ride the page"


def test_malformed_uuid_renders_a_422_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """U2: a path parameter FastAPI cannot parse (a non-UUID job id) is a
    rendered 422 page for a browser, not the raw JSON validation dump."""
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.get("/jobs/not-a-uuid", headers={"Accept": "text/html"})
    assert resp.status_code == 422
    assert "TaskQ Admin" in resp.text


def test_api_ish_routes_stay_json_on_validation_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """U2's boundary: the machine routes (/jobs/count, /jobs/api/*) never
    render HTML - a script's 422 stays JSON."""
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.get("/jobs/count", params={"tab": "bogus-tab-value"})
    # tab is a free str... hit the 422 through a non-int last_event_id on
    # the progress state route instead: it is under /api/.
    resp = client.get("/jobs/api/job/not-a-uuid/state")
    assert resp.status_code == 422
    assert resp.headers.get("content-type", "").startswith("application/json")
