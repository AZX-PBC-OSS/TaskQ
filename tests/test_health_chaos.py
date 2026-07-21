"""Chaos and property tests for health endpoints.

Unit-tier file (no ``pytestmark = integration``). Contains:
- Wedged event loop reports 503 on liveness
- PG-ping timeout chaos (saturated pool)
- Hypothesis property test on the /ready JSON schema
- redis_configured=False does not flip readiness
"""

import asyncio
import json
import subprocess
import sys
import time
from types import SimpleNamespace

import asyncpg
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from taskq.worker.deps import WorkerDeps
from taskq.worker.health import (
    HealthReport,
    build_ready_body,
    compute_health,
)
from taskq.worker.shutdown import ShutdownPhase

# -- No module-level pytestmark -- tests individually marked below.


# ── Stubs ──────────────────────────────────────────────────────────


class _FakeConn:
    async def execute(self, query: str, *args: object) -> str:
        return "SELECT 1"


class _AcquireCtx:
    def __init__(
        self,
        conn: _FakeConn | None = None,
        error: BaseException | None = None,
        delay: float = 0.0,
    ) -> None:
        self._conn = conn
        self._error = error
        self._delay = delay

    async def __aenter__(self) -> _FakeConn:
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        if self._error is not None:
            raise self._error
        assert self._conn is not None
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self, error: BaseException | None = None, delay: float = 0.0) -> None:
        self._error = error
        self._delay = delay
        self.acquire_calls = 0

    def acquire(self, timeout: float = 30.0) -> _AcquireCtx:
        self.acquire_calls += 1
        return _AcquireCtx(
            conn=_FakeConn() if self._error is None and self._delay == 0 else None,
            error=self._error,
            delay=self._delay,
        )


def _make_deps(**overrides: object) -> WorkerDeps:  # pyright: ignore[reportReturnType] # Why: test helper returns a SimpleNamespace duck-type of WorkerDeps; constructing a real WorkerDeps requires real asyncpg pools.
    defaults: dict[str, object] = {
        "shutdown_phase": ShutdownPhase.NONE,
        "dispatcher_pool": _StubPool(),
        "heartbeat_pool": _StubPool(),
        "settings": SimpleNamespace(
            health_pg_ping_timeout=0.2,
            max_heartbeat_failures=3,
            redis_url=None,
            health_socket_path="",
        ),
        "is_leader": SimpleNamespace(is_set=lambda: False),
        "active_jobs": SimpleNamespace(count=lambda: 2),
        "heartbeat_failures": 0,
        # WorkerDeps.redis_client (default None) — health reads it for redis_configured.
        "redis_client": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)  # type: ignore[return-value] # Why: same underlying constraint as above; pyright flags the return statement separately.


# ── Wedged event loop ───────────────────────────────────────


@pytest.mark.slow  # subprocess-timed chaos test; wall-clock bound under parallel load
def test_tc1_wedged_loop_unresponsive() -> None:
    """Wedged event loop causes ``_check_live`` to hang.

    Spawns a subprocess whose event loop is wedged by a synchronous
    ``time.sleep(2)`` callback scheduled via ``call_later``.
    The subprocess calls ``_check_live()`` while the loop is wedged;
    both the probe callback and the 1 s timeout callback are queued
    behind the wedge, so ``_check_live`` cannot return until the
    wedge releases. The test verifies the subprocess wall-clock
    elapsed time exceeds 1.5 s — the signal that the loop was
    unresponsive.

    The return value of ``_check_live`` is NOT gated because
    ``call_later(0.01,...)`` fires before the 1 s timeout callback
    when the loop unblocks (earlier heapq deadline), producing a
    false positive. Only wall-clock timing is reliable from within
    the same process.
    """
    code = """\
import asyncio, time

async def _check_live():
    loop = asyncio.get_running_loop()
    responded = asyncio.Event()
    def _on_fired():
        responded.set()
    loop.call_later(0.01, _on_fired)
    try:
        await asyncio.wait_for(responded.wait(), timeout=1.0)
        return True, "ok"
    except TimeoutError:
        return False, "event loop unresponsive (timeout after 1.0s)"

async def main():
    loop = asyncio.get_running_loop()
    loop.call_later(0.01, lambda: time.sleep(2))
    await _check_live()

asyncio.run(main())
"""
    t0 = time.perf_counter()
    subprocess.run(  # noqa: S603 # Why: test harness passes a fixed in-memory code string to the project's own Python; no untrusted input.
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
    )
    elapsed = time.perf_counter() - t0
    assert elapsed > 1.5, (
        f"subprocess completed in {elapsed:.3f}s; expected >1.5 s blocking delay from wedged loop."
    )


# ── PG-ping timeout chaos (saturated pool) ──────────────────


@pytest.mark.slow  # perf sanity check, not correctness; relaxed bound under parallel load
async def test_tc2_saturated_pool_returns_timeout() -> None:
    """PG-ping timeout chaos when pool is saturated.

    Constructs a fake pool whose acquire sleeps for
    ``timeout + 0.05`` then raises ``TimeoutError``, simulating
    an asyncpg pool saturation. Configures
    ``health_pg_ping_timeout=0.05`` so the test runs fast.
    Asserts ``pg_ping_ok=False``, ``"pg_ping_timeout" in reasons``,
    total ``compute_health`` duration < 500 ms.
    (.)
    """

    class _SatAcquireCtx:
        def __init__(self, timeout: float) -> None:
            self._timeout = timeout

        async def __aenter__(self) -> object:
            await asyncio.sleep(self._timeout + 0.05)
            raise TimeoutError

        async def __aexit__(self, *_: object) -> None:
            pass

    class SaturatedPool:
        def acquire(self, *, timeout: float) -> _SatAcquireCtx:
            return _SatAcquireCtx(timeout)

    deps = _make_deps(
        dispatcher_pool=SaturatedPool(),
        settings=SimpleNamespace(
            health_pg_ping_timeout=0.05,
            max_heartbeat_failures=3,
            redis_url=None,
            health_socket_path="",
        ),
    )

    t0 = time.perf_counter()
    report = await compute_health(deps)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    assert report.pg_ping_ok is False
    assert "pg_ping_timeout" in report.reasons
    # Functional oracle is pg_ping_ok/reasons above; this is a loose perf
    # sanity check widened from 500ms to survive parallel-test load.
    assert elapsed_ms < 2000.0, f"compute_health took {elapsed_ms:.0f} ms"


# ── Hypothesis property test on /ready JSON schema ──────────


_phase_strategy = st.sampled_from(
    [
        ShutdownPhase.NONE,
        ShutdownPhase.DRAINING,
        ShutdownPhase.CANCELLING,
        ShutdownPhase.FORCING,
        ShutdownPhase.ABANDONING,
    ]
)

_ready_body_keys = ["ready", "redis_configured", "active_jobs", "is_leader", "shutdown_phase"]


@given(
    phase=_phase_strategy,
    active_jobs=st.integers(min_value=0, max_value=100),
    is_leader=st.booleans(),
    redis_url=st.one_of(st.none(), st.just("redis://localhost:6379/0")),
    pg_ping_ok=st.booleans(),
)
@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@pytest.mark.asyncio
async def test_tp1_ready_body_schema_invariant(
    phase: ShutdownPhase,
    active_jobs: int,
    is_leader: bool,
    redis_url: str | None,
    pg_ping_ok: bool,
) -> None:
    """ready response body always satisfies the schema.

    For every generated ``WorkerDeps`` state, constructs a
    ``HealthReport`` directly and calls ``build_ready_body(report, deps)``.
    Then asserts:
    (a) every body has exactly the 5 keys from
    (b) ``shutdown_phase`` is ``None`` iff the deps phase is NONE,
        otherwise an int matching ``phase.value``,
    (c) ``ready=True`` iff ``shutdown_phase == NONE AND pg_ping_ok=True``,
    (d) the keys appear in the order specified by.
    (.)
    """
    report = HealthReport(
        live=True,
        ready=(phase == ShutdownPhase.NONE and pg_ping_ok),
        reasons=[],
        shutdown_phase=phase,
        heartbeat_failures=0,
        max_heartbeat_failures=3,
        is_leader=is_leader,
        redis_configured=bool(redis_url),
        pg_ping_ok=pg_ping_ok,
        pg_ping_latency_ms=0.0,
        active_jobs=active_jobs,
    )

    deps = _make_deps(
        shutdown_phase=phase,
        active_jobs=SimpleNamespace(count=lambda: active_jobs),
        is_leader=SimpleNamespace(is_set=lambda: is_leader),
        settings=SimpleNamespace(
            health_pg_ping_timeout=0.2,
            max_heartbeat_failures=3,
            redis_url=redis_url,
            health_socket_path="",
        ),
    )

    body_bytes = build_ready_body(report, deps)
    body = json.loads(body_bytes)

    assert list(body.keys()) == _ready_body_keys, f"key order mismatch: {list(body.keys())}"

    assert set(body.keys()) == set(_ready_body_keys)

    if phase == ShutdownPhase.NONE:
        assert body["shutdown_phase"] is None, f"NONE phase rendered as {body['shutdown_phase']!r}"
    else:
        assert body["shutdown_phase"] == phase.value, (
            f"{phase.name} rendered as {body['shutdown_phase']} (expected {phase.value})"
        )

    expected_ready = phase == ShutdownPhase.NONE and pg_ping_ok
    assert body["ready"] is expected_ready, (
        f"ready={body['ready']}, phase={phase.name}, pg_ping_ok={pg_ping_ok}"
    )

    assert body["redis_configured"] is bool(redis_url)
    assert body["active_jobs"] == active_jobs
    assert body["is_leader"] is is_leader


# ── cross-transport parity ───────────────────────────────────


@pytest.mark.asyncio
async def test_tp1_cross_transport_parity() -> None:
    """cross-transport parity.

    Mounts ``create_health_router(deps)`` on a FastAPI app and uses
    ``TestClient`` to GET ``/jobs/health/ready``. Compares the
    response body bytes to ``build_ready_body(await compute_health(deps), deps)``
    for the SAME deps. Asserts byte-equality across three parametrised
    states (NONE+healthy, DRAINING+healthy, NONE+pg_failure).
    (cross-transport invariant.)
    """
    pytest.importorskip("fastapi")

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.health import create_health_router

    scenarios: list[dict[str, object]] = [
        {
            "label": "NONE+healthy",
            "shutdown_phase": ShutdownPhase.NONE,
            "pg_error": None,
        },
        {
            "label": "DRAINING+healthy",
            "shutdown_phase": ShutdownPhase.DRAINING,
            "pg_error": None,
        },
        {
            "label": "NONE+pg_failure",
            "shutdown_phase": ShutdownPhase.NONE,
            "pg_error": asyncpg.PostgresConnectionError("connection refused"),
        },
    ]

    for scenario in scenarios:
        pg_pool = _StubPool(error=scenario["pg_error"])  # type: ignore[arg-type] # Why: None means no error — healthy pool; BaseException means raise on acquire.
        deps = _make_deps(
            shutdown_phase=scenario["shutdown_phase"],
            dispatcher_pool=pg_pool,
            active_jobs=SimpleNamespace(count=lambda: 0),
        )

        app = FastAPI()
        router = create_health_router(deps)
        app.include_router(router)
        client = TestClient(app)

        http_resp = client.get("/jobs/health/ready")

        report = await compute_health(deps)
        helper_bytes = build_ready_body(report, deps)

        assert http_resp.content == helper_bytes, (
            f"scenario {scenario['label']}: HTTP body != helper body\n"
            f"  HTTP:   {http_resp.content.decode()!r}\n"
            f"  helper: {helper_bytes.decode()!r}"
        )


# ── redis_configured=False does not flip readiness ──────────


async def test_tn3_redis_unconfigured_still_ready() -> None:
    """``redis_configured=False`` does not flip readiness.

    Configures deps with ``redis_url=None`` and otherwise healthy PG;
    asserts ``compute_health`` returns ``ready=True`` while
    ``redis_configured`` is ``False``. (.)
    """
    deps = _make_deps()
    report = await compute_health(deps)
    assert report.ready is True
    assert report.redis_configured is False
