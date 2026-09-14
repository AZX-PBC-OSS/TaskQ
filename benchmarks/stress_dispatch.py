"""Stress the per-job dispatch CPU path (no Postgres, no network).

Simulates, per dispatched job, the pure-Python work the TaskQ worker does
around each job:

  1. ``solve_dependencies`` — uncached ``get_type_hints`` +
     ``inspect.signature`` per dispatched job (stub registry/containers
     return cached values, mimicking ``tests/test_di_solver.py`` stubs;
     actor has 2 DI params).
  2. ``dumps_jsonb_str`` on a ~4KB payload — orjson serialize → decode →
     NUL prefilter.
  3. ``_job_row_from_record`` on a fake wide asyncpg record (dict
     subclass with ``__getitem__``) — 4 JSON parses per row.
  4. Every 10th job: ``compute_next_fire_after`` — 1-6 croniter
     instances per call, pure Python.

Runs ~STRESS_SECONDS (default 20) and prints jobs/sec.

Usage: .venv/bin/python benchmarks/stress_dispatch.py
"""

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from uuid import (
    uuid4,  # noqa: TID251  # Why: throwaway ids for scratch benchmark rows; PK B-tree locality is not the variable under test.
)

import structlog

from taskq._di.scope import Scope
from taskq._di.solver import solve_dependencies
from taskq._di.types import FactoryShape, ProviderEntry
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import JobRow
from taskq.backend._records import _job_row_from_record
from taskq.cron import compute_next_fire_after
from taskq.exceptions import MissingProvider

STRESS_SECONDS = float(os.environ.get("STRESS_SECONDS", "20"))
CRON_EVERY_N = 10
CRON_EXPR = "*/5 * * * *"
CRON_TZ = "America/New_York"


# ── DI test doubles (mimics tests/test_di_solver.py stubs) ────────────


class _StubRegistry:
    """Minimal ProviderRegistry test double."""

    def __init__(self, entries: dict[type, ProviderEntry[object]]) -> None:
        self._entries = entries

    @property
    def providers(self) -> dict[type, ProviderEntry[object]]:
        return dict(self._entries)

    def get(self, type_: type[object]) -> ProviderEntry[object]:
        entry = self._entries.get(type_)
        if entry is None:
            raise MissingProvider(type_name=type_.__qualname__, required_by="stress_dispatch")
        return entry


class _CachedStubContainer:
    """Stub ScopeContainer whose lookups are always cache hits after the
    first (VALUE-shaped providers), isolating the solver's own cost."""

    def __init__(self, scope: Scope) -> None:
        self._scope = scope
        self._cache: dict[type[object], object] = {}
        self.last_cache_hit: bool = False

    async def get_or_create(self, type_: type[object], entry: ProviderEntry[object]) -> object:
        if self._scope is not Scope.TRANSIENT and type_ in self._cache:
            self.last_cache_hit = True
            return self._cache[type_]
        self.last_cache_hit = False
        value = entry.impl  # FactoryShape.VALUE
        if self._scope is not Scope.TRANSIENT:
            self._cache[type_] = value
        return value

    async def aclose(self) -> None:
        await asyncio.sleep(0)


class _DBConn:
    pass


class _Settings:
    pass


def _make_di_stack() -> tuple[
    object,
    _StubRegistry,
    dict[Scope, _CachedStubContainer],
]:
    """Registry + containers with both actor deps pre-cached."""
    registry = _StubRegistry(
        {
            _DBConn: ProviderEntry(
                type_=_DBConn,
                scope=Scope.LOOP,
                kind="value",
                impl=_DBConn(),
                factory_shape=FactoryShape.VALUE,
            ),
            _Settings: ProviderEntry(
                type_=_Settings,
                scope=Scope.PROCESS,
                kind="value",
                impl=_Settings(),
                factory_shape=FactoryShape.VALUE,
            ),
        }
    )
    containers: dict[Scope, _CachedStubContainer] = {}
    for scope in Scope:
        containers[scope] = _CachedStubContainer(scope)
    return registry, registry, containers


async def _bench_actor(db: _DBConn, settings: _Settings) -> None:
    return None


# ── Fake record (37-column asyncpg.Record stand-in) ───────────────────


class _FakeRecord(dict):
    """dict subclass with ``__getitem__`` — matches asyncpg.Record access."""

    def __getitem__(self, key):
        return dict.__getitem__(self, key)


def _jsonb(text_b64ish: str, pad_kb: int) -> str:
    blob = (text_b64ish * (pad_kb * 1024 // len(text_b64ish) + 1))[: pad_kb * 1024]
    return json.dumps({"blob": blob, "n": len(blob)})


def _make_record() -> _FakeRecord:
    now = datetime(2026, 9, 12, 12, 0, 0, tzinfo=UTC)
    return _FakeRecord(
        id=uuid4(),
        actor="bench.actor",
        queue="default",
        identity_key="identity-bench-0001",
        fairness_key="fairness-bench",
        payload=_jsonb("payloadblock0123456789", 2),
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=timedelta(seconds=300),
        heartbeat_timeout=timedelta(seconds=30),
        created_at=now,
        scheduled_at=now,
        started_at=now,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=uuid4(),
        lock_expires_at=now + timedelta(seconds=60),
        cancel_requested_at=None,
        cancel_phase=0,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state=_jsonb("progressblock0123456789", 1),
        progress_seq=0,
        result=_jsonb("resultblock0123456789", 1),
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key="idempotency-bench-0001",
        idempotency_scope="",
        trace_id="trace-bench-0001",
        span_id="span-bench-0001",
        metadata=_jsonb("metadatablock0123456789", 1),
        tags=["bench", "stress"],
    )


def _make_payload_4kb() -> dict[str, object]:
    filler = "x" * 3840
    return {
        "order_id": str(uuid4()),
        "attempt": 1,
        "items": [{"sku": f"SKU-{i:04d}", "qty": i} for i in range(8)],
        "notes": filler,
        "nested": {"a": [1, 2, 3], "b": {"c": True, "d": None}},
    }


# ── Main loop ─────────────────────────────────────────────────────────


async def main() -> None:
    # A configured worker runs at INFO/WARNING: the solver's per-dep
    # logger.debug call still happens but records below the level are
    # dropped before rendering. Mirror that so terminal-rendering I/O
    # doesn't dominate the profile.
    logging.root.setLevel(logging.WARNING)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
        cache_logger_on_first_use=True,
    )

    _, registry, containers = _make_di_stack()
    record = _make_record()
    payload = _make_payload_4kb()
    payload_size = len(dumps_jsonb_str(payload))
    assert 3500 <= payload_size <= 4600, f"payload is {payload_size}B, expected ~4KB"

    now = datetime.now(UTC)
    jobs = 0
    cron_calls = 0
    deadline = time.perf_counter() + STRESS_SECONDS

    while time.perf_counter() < deadline:
        await solve_dependencies(
            func=_bench_actor,
            registry=registry,
            scope_containers=containers,
        )
        dumps_jsonb_str(payload)
        row = _job_row_from_record(record)
        assert isinstance(row, JobRow)
        if jobs % CRON_EVERY_N == 0:
            compute_next_fire_after(CRON_EXPR, CRON_TZ, now)
            cron_calls += 1
        jobs += 1

    elapsed = time.perf_counter() - (deadline - STRESS_SECONDS)
    print(f"jobs: {jobs} in {elapsed:.2f}s -> {jobs / elapsed:,.0f} jobs/sec")
    print(f"cron calls: {cron_calls} (1 per {CRON_EVERY_N} jobs)")


if __name__ == "__main__":
    asyncio.run(main())
