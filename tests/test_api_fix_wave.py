"""The API contract fixes' pins (F3/F4/F6).

F3 - the trigger app answered THREE success shapes (200 {"redirect",
     "job_id", "has_progress"}, 202 {"result_url", "job_id"}) and raw
     text on some 4xxs (a bare-text 404 "unknown actor: x"). ONE success
     envelope now: 201/202 {"job_id", "url"}; every 4xx is JSON with the
     FastAPI {"detail": ...} shape; validation errors render as one
     compact line, not pydantic's multi-line dump.
F4 - the progress SSE's reconnect arm blackholed: a row that went
     terminal while the client was away with nothing to replay
     (progress_seq <= Last-Event-ID) fell into the pub/sub loop and
     never told the client - the reconnecting EventSource sat on a
     finished job forever. The arm now emits ``event: done`` immediately.
F6 - FastAPI's APIRoute does not add HEAD to GET routes (Starlette's own
     Route does), so every monitor's HEAD check answered 405. The admin
     router, the progress router, the ui-serve health router, and the
     example trigger app now serve HEAD via the shared route class.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

pytestmark = [pytest.mark.fastapi]

_REPO_ROOT = Path(__file__).resolve().parents[1]


# ── F3: one envelope on every trigger route ──────────────────────────────


class _FakeHandle:
    def __init__(self) -> None:
        from taskq._ids import new_uuid

        self.job_id = new_uuid()


class _FakeTQ:
    """TaskQ stub answering enqueue with a fresh handle, get with None."""

    def __init__(self) -> None:
        self.handles: list[_FakeHandle] = []

    async def enqueue(self, ref: Any, payload: Any, **kwargs: Any) -> _FakeHandle:
        handle = _FakeHandle()
        self.handles.append(handle)
        return handle

    async def get(self, job_id: Any, **kwargs: Any) -> None:
        return None

    async def cancel(self, job_id: Any, **kwargs: Any) -> Any:
        raise KeyError(job_id)


def _trigger_client() -> TestClient:
    import examples.app as trigger_app

    client = TestClient(trigger_app.app, raise_server_exceptions=False)
    # The lifespan owns the real TaskQ; the HTTP contract tests stub it.
    trigger_app.app.state.tq = _FakeTQ()  # type: ignore[assignment]
    return client


def test_enqueue_counter_answers_the_one_success_envelope() -> None:
    """A plain enqueue: 201 {"job_id", "url"} — nothing else on the wire."""
    client = _trigger_client()
    resp = client.post("/enqueue/counter", data={"n": "3"})
    assert resp.status_code == 201
    body = resp.json()
    assert set(body) == {"job_id", "url"}, body
    assert body["url"] == f"/taskq/jobs/{body['job_id']}"


def test_enqueue_result_actor_answers_202_with_the_result_url() -> None:
    """A result-bearing actor: 202 with the result page as the url."""
    client = _trigger_client()
    resp = client.post("/enqueue/summer", data={"values": "1,2"})
    assert resp.status_code == 202
    body = resp.json()
    assert set(body) == {"job_id", "url"}, body
    assert body["url"].startswith("/result/")


def test_unknown_actor_is_a_json_404_not_raw_text() -> None:
    """The bare-text 404 wall: a 404 a script can parse."""
    client = _trigger_client()
    resp = client.post("/enqueue/definitely_not_an_actor", data={})
    assert resp.status_code == 404
    assert resp.json()["detail"].startswith("unknown actor")


def test_validation_errors_render_as_one_compact_line() -> None:
    """A bad payload: 400 with the compact field: message summary — not
    pydantic's multi-line dump, not an unparseable wall."""
    client = _trigger_client()
    resp = client.post("/enqueue/counter", data={"n": "not-a-number"})
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert "\n" not in detail, detail
    assert detail.startswith("invalid payload for 'counter':")
    assert "n:" in detail, detail


def test_the_conflict_routes_carry_the_detail_shape() -> None:
    """The 409s (singleton collision, max-pending) speak {"detail": ...}
    like every other refusal on the app."""
    import examples.app as trigger_app

    from taskq import MaxPendingExceededError, SingletonCollisionError

    class _CollidingTQ(_FakeTQ):
        async def enqueue(self, ref: Any, payload: Any, **kwargs: Any) -> Any:
            raise SingletonCollisionError(actor="counter", blocking_job_id="x")

    class _CappedTQ(_FakeTQ):
        async def enqueue(self, ref: Any, payload: Any, **kwargs: Any) -> Any:
            raise MaxPendingExceededError(actor="counter", current_count=7, max_pending=5)

    original = getattr(trigger_app.app.state, "tq", None)
    client = _trigger_client()
    try:
        trigger_app.app.state.tq = _CollidingTQ()  # type: ignore[assignment]
        resp = client.post("/enqueue/counter", data={"n": "1"})
        assert resp.status_code == 409
        assert "detail" in resp.json()

        trigger_app.app.state.tq = _CappedTQ()  # type: ignore[assignment]
        resp = client.post("/enqueue/counter", data={"n": "1"})
        assert resp.status_code == 409
        assert "detail" in resp.json()
    finally:
        trigger_app.app.state.tq = original


def test_cancel_and_result_404s_are_json() -> None:
    """The remaining trigger routes' 4xxs: JSON, never raw text."""
    import uuid as uuid_mod

    import examples.app as trigger_app

    client = TestClient(trigger_app.app, raise_server_exceptions=False)
    original = getattr(trigger_app.app.state, "tq", None)
    try:
        trigger_app.app.state.tq = _FakeTQ()  # type: ignore[assignment]
        missing = uuid_mod.uuid5(uuid_mod.NAMESPACE_URL, "missing")
        resp = client.post(f"/cancel/{missing}")
        assert resp.status_code == 404
        assert resp.json() == {"detail": "job not found"}

        resp = client.get(f"/progress/{missing}")
        assert resp.status_code == 404
        assert resp.json() == {"detail": "job not found"}

        resp = client.get(f"/result/{missing}")
        assert resp.status_code == 404
        assert resp.json() == {"detail": "job not found"}
    finally:
        trigger_app.app.state.tq = original


# ── F4: the reconnect blackhole answers done ─────────────────────────────


def test_sse_reconnect_on_a_terminal_row_emits_done_immediately() -> None:
    """The reconnect arm: a terminal row + nothing to replay
    (progress_seq <= Last-Event-ID) → ``event: done`` arrives at once and
    the generator ENDS. Before the fix the stream sat in the pub/sub loop
    emitting keepalives forever - the client never learned the job was
    over."""
    from taskq._ids import new_uuid
    from taskq.web.progress import _event_generator

    class _NeverMessagePubsub:
        """A pub/sub that never delivers (the finished job's channel)."""

        async def get_message(self, **kwargs: Any) -> None:
            await asyncio.sleep(3600)

        async def unsubscribe(self, channel: str) -> None: ...

    async def _run() -> list[Any]:
        gen = _event_generator(
            pubsub=_NeverMessagePubsub(),
            channel="c",
            job_id=new_uuid(),
            is_terminal=True,
            progress_seq=5,
            progress_data="{}",
            resolved_last_event_id=10,  # the client is AT or past the row
            heartbeat_secs=3600,  # a keepalive would take an hour
        )
        return [event async for event in gen]

    events = asyncio.run(asyncio.wait_for(_run(), timeout=5))
    assert len(events) == 1, f"the reconnect must answer done and stop, got {events}"
    assert events[0].event == "done"


def test_sse_reconnect_with_replayable_progress_still_replays() -> None:
    """The control: a terminal row whose seq is PAST the client's cursor
    still replays the terminal payload before the done."""
    from taskq._ids import new_uuid
    from taskq.web.progress import _event_generator

    class _NeverMessagePubsub:
        async def get_message(self, **kwargs: Any) -> None:
            await asyncio.sleep(3600)

        async def unsubscribe(self, channel: str) -> None: ...

    async def _run() -> list[Any]:
        gen = _event_generator(
            pubsub=_NeverMessagePubsub(),
            channel="c",
            job_id=new_uuid(),
            is_terminal=True,
            progress_seq=12,
            progress_data='{"percent": 100}',
            resolved_last_event_id=10,
            heartbeat_secs=3600,
        )
        return [event async for event in gen]

    events = asyncio.run(asyncio.wait_for(_run(), timeout=5))
    assert [e.event for e in events] == ["terminal", "done"], events


# ── F6: HEAD answers on the GET routes ───────────────────────────────────


def test_admin_pages_answer_head() -> None:
    """The monitor's check: HEAD /admin/queues must not 405."""
    os.environ["TASKQ_ENVIRONMENT"] = "dev"
    from taskq.web.admin import create_router, setup_admin_state
    from tests.web_admin import StubPool

    pool = StubPool()
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    client = TestClient(app)
    resp = client.head("/queues")
    assert resp.status_code == 200, "HEAD on a serving page must not read as down (405)"


def test_trigger_app_answers_head() -> None:
    """F6 on the shipped trigger app: HEAD / and HEAD /rate-limits."""
    import examples.app as trigger_app

    client = TestClient(trigger_app.app, raise_server_exceptions=False)
    assert client.head("/").status_code == 200
    assert client.head("/rate-limits").status_code == 200
