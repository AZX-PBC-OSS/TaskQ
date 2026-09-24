# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound.

"""The client cursor must not outlive the durable row it recovers from.

The lost-flush cut, end to end on real Postgres and real Redis: a worker
publishes each progress event to the per-job channel as the call lands,
the coalesced flush carries the seq to the row up to half a second later,
and a crash in between leaves the row (and every seq the redispatched
attempt consumes) behind what a browser already saw on the wire.

This module drives that cut through the REAL paths - the real router's SSE
bridge (subscribe-before-query, the seq-discard loop, real Redis pub/sub),
the real poll-state endpoint reading the real row, and the real
``realtime.js`` client cursor driven under Node on the captured frames -
and pins the terminal delivery: the redispatched attempt's durable
terminal write lands the row at a seq BELOW the wire-inflated cursor, and
that row is the delivery. A client cursor that skips it freezes the page
on the previous attempt's stale progress for a job that is durably over;
no later seq ever recovers it. The poll-side terminal exemption is the
client mirror of the stream's own terminal-delivery rule.

The client driving follows the assembled (#487) client's shapes: the poll
is conditional (``If-None-Match`` carries the rendered sequence, a 304
tick has no body to parse), the render is update-in-place (the entry is
appended once and patched, so the render log is the log of DOM writes),
and the poll's terminal observation tears the poller and the stream down.
"""

import asyncio
import contextlib
import json
import shutil
import subprocess
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
import pytest_asyncio

pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")

import redis.asyncio as aioredis
from fastapi import FastAPI

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.constants import progress_channel
from taskq.migrate import apply_pending
from taskq.progress._events import ProgressEvent
from taskq.web.progress import create_router

pytestmark = [pytest.mark.integration, pytest.mark.redis]

SCHEMA_LABEL = f"tcr_{new_base62()}".lower()

REALTIME_JS = (
    Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "realtime.js"
)

requires_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="realtime.js behaviour is driven under Node"
)


# ── Fixtures (the test_integration.py shapes) ─────────────────────────


@pytest_asyncio.fixture
async def pool(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    setup_conn = await asyncpg.connect(pg_dsn)
    try:
        await setup_conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA_LABEL}" CASCADE')
        await apply_pending(setup_conn, schema=SCHEMA_LABEL)
    finally:
        await setup_conn.close()

    pg_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    assert pg_pool is not None
    try:
        yield pg_pool
    finally:
        await pg_pool.close()


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[aioredis.Redis]:
    client = aioredis.from_url(redis_url)
    try:
        yield client
    finally:
        await client.aclose()


def _make_app(pool: asyncpg.Pool, redis_client: Any) -> FastAPI:
    router = create_router(
        pool,
        redis_client,
        schema=SCHEMA_LABEL,
        sse_heartbeat_interval=timedelta(seconds=1),
    )
    app = FastAPI()
    app.include_router(router, prefix="/jobs")
    return app


async def _seed_running_job(pool: asyncpg.Pool) -> Any:
    job_id = new_job_id()
    expires_at = datetime.now(UTC) + timedelta(seconds=300)
    async with pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO {SCHEMA_LABEL}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, attempt, scheduled_at, schedule_to_close,
                locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,
                progress_state, progress_seq
            ) VALUES (
                $1, $2, $3, $4::jsonb, $5, $6,
                $7, 0, 1, now(), now() + interval '300 seconds',
                $8, $9, now(), now(),
                $10::jsonb, $11
            )""",
            job_id,
            "test_actor",
            "default",
            "{}",
            3,
            "transient",
            "running",
            new_uuid(),
            expires_at,
            "{}",
            0,
        )
    return job_id


async def _land_redispatched_terminal(pool: asyncpg.Pool, job_id: Any) -> None:
    """Land the absolute terminal write the redispatched attempt's mark_* runs.

    The attempt re-seeded its buffer from the row the crash left behind
    (progress_seq = 0), so its terminal write consumes a small seq and SETs
    it ABSOLUTELY - the row ends at 2 while the wire already carried 1..5.
    """
    async with pool.acquire() as conn:
        await conn.execute(
            f"""UPDATE {SCHEMA_LABEL}.jobs
                SET status = 'failed',
                    progress_seq = 2,
                    progress_state = $2::jsonb
                WHERE id = $1""",
            job_id,
            json.dumps({"percent": 25, "detail": "attempt 2 failed"}),
        )


# ── SSE capture ────────────────────────────────────────────────────────
#
# ``httpx.ASGITransport`` buffers the whole body before the response is
# returned, so a stream that strands on keepalives (this cut's shape: no
# terminal envelope ends it on a behind-cursor row) yields nothing at all.
# The capture below consumes the REAL ASGI app directly - the same app, the
# same generator, the same Redis pub/sub - and cancels the reader the way a
# client disconnect would, so the frames are captured LIVE off the stranded
# stream.


async def _collect_sse_frames(
    app: FastAPI,
    path: str,
    *,
    stop: asyncio.Event,
    overall_timeout: float = 10.0,
) -> list[dict[str, str]]:
    from tests.web_progress.test_integration import _reap_stream_teardown_tasks

    baseline: frozenset[asyncio.Task[object]] = frozenset(asyncio.all_tasks())
    chunks: list[str] = []

    async def _receive() -> dict[str, object]:
        # The request body never arrives; the stream is a GET.
        await asyncio.sleep(3600)
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message: dict[str, object]) -> None:
        if message["type"] == "http.response.body":
            body = message.get("body", b"")
            assert isinstance(body, bytes)
            chunks.append(body.decode("utf-8"))

    scope: dict[str, object] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "queryString": b"",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"test")],
        "client": ("test", 1234),
        "server": ("test", 80),
    }

    async def _drive() -> None:
        # The cancelled variant is the point: the CancelledError thrown into
        # the app is the client disconnect, and the generator's finally
        # releases the SSE slot and the Redis subscription for it.
        await app(scope, _receive, _send)  # pyright: ignore[reportArgumentType,reportCallIssue]

    reader = asyncio.create_task(_drive())
    try:
        await asyncio.wait_for(stop.wait(), timeout=overall_timeout)
    except TimeoutError:
        pass
    finally:
        reader.cancel()
        with _suppress_cancel():
            await reader
        await _reap_stream_teardown_tasks(baseline)

    lines = "".join(chunks).splitlines()
    frames: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in lines:
        if line == "":
            if current:
                frames.append(current)
                current = {}
        elif line.startswith(": "):
            current["comment"] = line[2:]
        elif ":" in line:
            key, _, value = line.partition(":")
            current[key.strip()] = value.strip()
        else:
            pass
    if current:
        frames.append(current)
    return frames


def _suppress_cancel() -> "contextlib.AbstractContextManager[object]":
    return contextlib.suppress(asyncio.CancelledError)


def _wire_event(job_id: Any, seq: int, percent: float) -> ProgressEvent:
    return ProgressEvent(
        v=1,
        kind="progress",
        job_id=job_id,
        actor="test_actor",
        ts=datetime.now(UTC),
        seq=seq,
        status="running",
        percent=percent,
        terminal=False,
    )


# ── The client harness (the test_realtime_js_mode_probe.py shape) ─────
#
# Driving the ASSEMBLED client: the poll is conditional (the request
# carries ``If-None-Match`` once the page has a sequence; a 304 answer has
# no body), the render is update-in-place (the entry is appended once,
# every later tick patches nodes in place), and the DOM-write log is the
# render record: a ``width`` write is a percent render, a ``text`` write
# on the meta line starts with the rendered percentage.

_CLIENT_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 2], "utf8");
const script = JSON.parse(fs.readFileSync(process.argv[process.argv.length - 1], "utf8"));

const log = [];
const writes = [];  // every DOM write: {kind: "append"|"width"|"text", value}
let now = 0;
let timers = [];
let nextTimer = 1;

global.window = { TASKQ_BASE_PATH: "" };
global.POLL_INTERVAL_MS = 1000;

const badge = {
    attrs: { "data-mode": "realtime" },
    textContent: "",
    setAttribute(k, v) { this.attrs[k] = v; },
    getAttribute(k) { return this.attrs[k]; },
};
const section = {
    attrs: {
        "data-job-id": "j1",
        "data-progress-seq": String(script.initial.seq),
        "data-progress-state": JSON.stringify(script.initial.state),
    },
    getAttribute(k) { return this.attrs[k]; },
};

let nodeCounter = 0;
function makeNode(tag) {
    nodeCounter += 1;
    const name = `${tag}${nodeCounter}`;
    const node = {
        _name: name,
        children: [],
        className: "",
        appendChild(child) { this.children.push(child); },
        remove() {},
    };
    let width = "";
    Object.defineProperty(node, "style", {
        value: {},
    });
    let text = "";
    Object.defineProperty(node, "textContent", {
        get() { return text; },
        set(v) {
            text = v;
            writes.push({ kind: "text", value: v });
        },
    });
    return node;
}

const timeline = {
    appendChild(entry) {
        log.push("append-progress");
        writes.push({ kind: "append", value: entry._name });
        // Hand the entry's live nodes back so the patch writes land in
        // THIS object (update-in-place: the entry is built once).
        const barWrap = entry.children.find((c) => c.className === "progress-bar-wrap");
        const bar = barWrap.children[0];
        Object.defineProperty(bar.style, "width", {
            set(v) { writes.push({ kind: "width", value: v }); },
            get() { return ""; },
        });
    },
};
const elements = { "progress-section": section, "progress-timeline": timeline };

global.document = {
    addEventListener(name, fn) { if (name === "DOMContentLoaded") fn(); },
    getElementById(id) { return elements[id] ?? null; },
    querySelector(sel) { return sel === ".taskq-badge" ? badge : null; },
    createElement(tag) { return makeNode(tag); },
};

global.EventSource = class {
    constructor(url) { this.url = url; this.handlers = {}; global.lastEventSource = this; log.push("sse-open"); }
    addEventListener(name, fn) { this.handlers[name] = fn; }
    close() { log.push("sse-close"); this.closed = true; }
};

global.setInterval = (fn, ms) => { const id = nextTimer++; timers.push({ id, fn, ms, due: now + ms }); return id; };
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };

let polled = false;
global.fetch = (url, opts) => ({
    then(f1) {
        log.push("fetch:" + url.split("?")[0]);
        if (opts && opts.headers && opts.headers["If-None-Match"] !== undefined) {
            log.push("inm:" + opts.headers["If-None-Match"]);
        }
        const body = polled ? { status: "running", progress_state: null, progress_seq: 0 } : script.poll;
        polled = true;
        return { then(f2) { f2(f1({ status: 200, json: () => body })); return { catch() {} }; } };
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
for (const frame of script.sse) {
    if (global.lastEventSource.closed) break;
    global.lastEventSource.handlers[frame.event]({
        data: typeof frame.data === "string" ? frame.data : JSON.stringify(frame.data),
        lastEventId: frame.id,
    });
    if (log.includes("sse-close")) break;
}
global.lastEventSource.handlers.error({});
advance(1000);
advance(32000);
process.stdout.write(JSON.stringify({ log, writes }));
"""


def _drive_client(script: dict[str, object], script_path: str) -> dict[str, Any]:
    node = shutil.which("node")
    assert node is not None
    Path(script_path).write_text(json.dumps(script), encoding="utf-8")
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and script paths are this file's own constants.
        [node, "-e", _CLIENT_HARNESS, "--", str(REALTIME_JS), script_path],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout)


# ── The pin ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
@requires_node
async def test_the_client_cursor_renders_the_durable_terminal_behind_the_wire_inflated_seq(
    pool: asyncpg.Pool, redis_client: aioredis.Redis, tmp_path: Path
) -> None:
    """End to end on real PG and real Redis: the lost-flush cut strands the
    durable terminal behind the client cursor, and the page still lands it.

    Attempt 1 publishes five progress envelopes (wire seqs 1..5) that the
    coalesced flush never carries to the row - the crash ate it. The
    redispatched attempt re-seeds from the behind row and its terminal
    write lands the row at seq 2 with attempt 2's own state. The real
    poll-state endpoint then serves exactly that row. The client, hydrated
    at seq 0 and inflated to seq 5 by the captured wire frames, must render
    the durable terminal from the poll body even though 2 <= 5: the job is
    durably over, the row is the delivery, and no later seq recovers the
    page. On a client that gates the poll body on the cursor alone, the
    poll body is skipped, both drivers stop, and the page freezes on
    attempt 1's stale 50%.
    """
    job_id = await _seed_running_job(pool)
    app = _make_app(pool, redis_client)
    channel = progress_channel(SCHEMA_LABEL, job_id)
    captured = asyncio.Event()

    async def _publish_attempt_one() -> None:
        # The flush-lost cut: the publishes ride out, the row stays at 0.
        await asyncio.sleep(0.3)
        for seq in range(1, 6):
            await redis_client.publish(
                channel,
                _wire_event(job_id, seq, percent=seq * 10).model_dump_json(exclude_none=True),
            )
        await asyncio.sleep(0.5)
        await _land_redispatched_terminal(pool, job_id)
        captured.set()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_publish_attempt_one())
        frames = await _collect_sse_frames(
            app, f"/jobs/api/job/{job_id}/progress/stream", stop=captured
        )

    wire_frames = [f for f in frames if f.get("event") == "progress"]
    wire_ids = [f.get("id") for f in wire_frames]
    assert wire_ids == ["0", "1", "2", "3", "4", "5"], (
        f"the wire must have inflated the cursor to 5, got {wire_ids}: {frames}"
    )

    poll_resp = await httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ).get(f"/jobs/api/job/{job_id}/state")
    poll_body = poll_resp.json()
    assert poll_body["status"] == "failed"
    assert poll_body["progress_seq"] == 2, (
        f"the redispatched attempt's terminal write landed seq 2: {poll_body}"
    )

    script = {
        "initial": {"seq": 0, "state": {}},
        "sse": wire_frames,
        "poll": poll_body,
    }
    outcome = _drive_client(script, str(tmp_path / "cursor_recovery_script.json"))

    log = outcome["log"]
    assert log.count("sse-open") == 1
    # The update-in-place render's record is the DOM-write log: a width
    # write carries the rendered percentage.
    percents = [w["value"] for w in outcome["writes"] if w["kind"] == "width"]
    assert percents[:5] == ["10%", "20%", "30%", "40%", "50%"], (
        f"the five attempt-1 deltas rendered live: {percents}"
    )
    assert percents[-1] == "25%", (
        f"the page must end on the durable terminal's state, not stale attempt-1 progress: {percents}"
    )
    assert log.count("sse-close") == 1, (
        f"terminal discovery tears the poller and the stream down: {log}"
    )
